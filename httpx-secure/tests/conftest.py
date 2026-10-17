from __future__ import annotations

import ssl
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import anyio
import h2.config
import h2.connection
import h2.events
import pytest
import trustme
from anyio.abc import SocketAttribute
from anyio.streams.tls import TLSAttribute, TLSStream

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator


@pytest.fixture(params=['asyncio', 'trio'])
def anyio_backend(request):
    return request.param


@pytest.fixture(scope='session')
def certificate():
    ca = trustme.CA()
    return ca, ca.issue_cert('localhost')


@pytest.fixture
def tls_contexts(certificate):
    ca, cert = certificate
    server = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    cert.configure_cert(server)
    client = ssl.create_default_context()
    ca.configure_trust(client)
    return server, client


@dataclass
class ReceivedRequest:
    connection: int
    path: str
    headers: dict[str, str]
    body: bytes
    sni: str | None


@dataclass
class HTTPServer:
    port: int
    responses: dict[str, tuple[int, list[tuple[str, str]], bytes]]
    tls_context: ssl.SSLContext | None = None
    requests: list[ReceivedRequest] = field(default_factory=list)
    closed: dict[int, anyio.Event] = field(default_factory=dict)
    snis: dict[int, str | None] = field(default_factory=dict)
    accepted: int = 0

    def reply(self, path):
        return self.responses.get(path, (200, [], b'OK'))

    async def handle(self, raw):
        self.accepted += 1
        ident = self.accepted
        self.closed[ident] = anyio.Event()
        try:
            await self._handle(raw, ident)
        except anyio.EndOfStream:
            pass
        finally:
            with anyio.CancelScope(shield=True):
                await raw.aclose()
            self.closed[ident].set()

    async def _handle(self, stream, ident):
        if self.tls_context is not None:
            try:
                stream = await TLSStream.wrap(
                    stream,
                    server_side=True,
                    ssl_context=self.tls_context,
                    standard_compatible=False,
                )
            except (anyio.EndOfStream, anyio.BrokenResourceError, ssl.SSLError):
                return  # A plain TCP selection probe or rejected certificate.
            obj = stream.extra(TLSAttribute.ssl_object)
            sni = self.snis.get(id(obj))
            if obj.selected_alpn_protocol() == 'h2':
                await self._http2(stream, ident, sni)
                return
        else:
            sni = None
        await self._http1(stream, ident, sni)

    async def _http1(self, stream, ident, sni):
        pending = b''
        while True:
            while b'\r\n\r\n' not in pending:
                pending += await stream.receive(65536)
            header, pending = pending.split(b'\r\n\r\n', 1)
            first, *lines = header.decode('ascii').split('\r\n')
            path = first.split()[1]
            headers = dict(line.split(': ', 1) for line in lines)
            headers = {key.lower(): value for key, value in headers.items()}
            size = int(headers.get('content-length', '0'))
            while len(pending) < size:
                pending += await stream.receive(65536)
            body, pending = pending[:size], pending[size:]
            self.requests.append(ReceivedRequest(ident, path, headers, body, sni))
            status, extra, content = self.reply(path)
            response = (
                f'HTTP/1.1 {status} Response\r\nContent-Length: {len(content)}\r\n'
                + ''.join(f'{key}: {value}\r\n' for key, value in extra)
                + '\r\n'
            ).encode() + content
            await stream.send(response)
            if headers.get('connection') == 'close' or ('Connection', 'close') in extra:
                return

    async def _http2(self, stream, ident, sni):
        connection = h2.connection.H2Connection(
            config=h2.config.H2Configuration(client_side=False, header_encoding='ascii')
        )
        requests = {}
        connection.initiate_connection()
        await stream.send(connection.data_to_send())
        while True:
            data = await stream.receive(65536)
            for event in connection.receive_data(data):
                if isinstance(event, h2.events.RequestReceived):
                    requests[event.stream_id] = (dict(event.headers), b'')
                elif isinstance(event, h2.events.DataReceived):
                    headers, body = requests[event.stream_id]
                    requests[event.stream_id] = headers, body + event.data
                    connection.acknowledge_received_data(
                        event.flow_controlled_length, event.stream_id
                    )
                elif isinstance(event, h2.events.StreamEnded):
                    headers, body = requests.pop(event.stream_id)
                    path = headers[':path']
                    self.requests.append(
                        ReceivedRequest(ident, path, headers, body, sni)
                    )
                    status, extra, content = self.reply(path)
                    connection.send_headers(
                        event.stream_id,
                        [
                            (':status', str(status)),
                            ('content-length', str(len(content))),
                            *[(k.lower(), v) for k, v in extra],
                        ],
                    )
                    connection.send_data(event.stream_id, content, end_stream=True)
            await stream.send(connection.data_to_send())


@pytest.fixture
def server_factory():
    @asynccontextmanager
    async def serve(
        *, host='127.0.0.1', port=0, tls=None, http2=False, responses=None
    ) -> AsyncGenerator[HTTPServer, None]:
        listener = await anyio.create_tcp_listener(local_host=host, local_port=port)
        async with listener:
            server = HTTPServer(
                listener.extra(SocketAttribute.local_port),
                responses or {},
                tls_context=tls,
            )
            if tls is not None:
                tls.set_servername_callback(
                    lambda obj, name, _ctx: server.snis.__setitem__(id(obj), name)
                )
                tls.set_alpn_protocols(['h2'] if http2 else ['http/1.1'])
            async with anyio.create_task_group() as group:
                group.start_soon(listener.serve, server.handle)
                try:
                    yield server
                finally:
                    group.cancel_scope.cancel()

    return serve
