"""Finite, checked HTTP exchanges; H2 connections retain HPACK and stream state."""

from __future__ import annotations

import hashlib
import http.client
import socket
import struct
import time
from contextlib import ExitStack, nullcontext
from dataclasses import dataclass, field
from pathlib import Path
from threading import Lock
from typing import TYPE_CHECKING, cast

import h2.config
import h2.connection
import h2.events
import h2.settings
import h11

if TYPE_CHECKING:
    import ssl


@dataclass(frozen=True)
class Exchange:
    path: str
    body: bytes
    expected: bytes
    headers: tuple[tuple[str, str], ...] = ()
    status: int = 200
    grpc: bool = False
    slow: bool = False
    early_hints: tuple[bytes, ...] = ()
    trailers: tuple[tuple[bytes, bytes], ...] = ()


@dataclass
class Pending:
    exchange: Exchange
    sent: int = 0
    headers: dict = field(default_factory=dict)
    trailers: dict = field(default_factory=dict)
    received: bytearray = field(default_factory=bytearray)
    early_hints: list[bytes] = field(default_factory=list)


@dataclass
class TlsState:
    session: ssl.SSLSession | None = None
    full: int = 0
    resumed: int = 0


class Connections:
    """Keep completed connections idle until the server's graceful drain."""

    def __init__(self):
        self.stack = ExitStack()
        self.lock = Lock()

    def __enter__(self):
        return self

    def __exit__(self, *exception):
        return self.stack.__exit__(*exception)

    def retain(self, connection):
        with self.lock:
            self.stack.callback(connection.close)
        return connection


def connect(
    port: int | Path,
    context: ssl.SSLContext | None,
    protocol: str,
    proxy: str = '',
    tls_state: TlsState | None = None,
) -> socket.socket:
    if isinstance(port, Path):
        sock = socket.socket(socket.AF_UNIX)
        sock.settimeout(60)
        sock.connect(str(port))
    else:
        sock = socket.create_connection(('127.0.0.1', port), timeout=60)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    if proxy == 'v1':
        sock.sendall(b'PROXY TCP4 192.0.2.10 127.0.0.1 43210 443\r\n')
    elif proxy == 'v2':
        sock.sendall(
            b'\r\n\r\n\0\r\nQUIT\n'
            + bytes([0x21, 0x11])
            + struct.pack(
                '!H4s4sHH',
                12,
                socket.inet_aton('192.0.2.10'),
                socket.inet_aton('127.0.0.1'),
                43210,
                443,
            )
        )
    if context is None:
        return sock
    try:
        secure = context.wrap_socket(
            sock,
            server_hostname='localhost',
            session=tls_state.session if tls_state is not None else None,
        )
    except BaseException:
        sock.close()
        raise
    if secure.selected_alpn_protocol() != ('h2' if protocol == 'h2' else 'http/1.1'):
        secure.close()
        raise RuntimeError('TLS ALPN selected the wrong protocol')
    if tls_state is not None:
        if secure.session_reused:
            tls_state.resumed += 1
        else:
            tls_state.full += 1
    return secure


def http1(
    port: int | Path,
    exchanges: list[Exchange],
    context: ssl.SSLContext | None,
    connections: Connections,
    *,
    proxy: str = '',
    tls_state: TlsState | None = None,
) -> dict[int, int]:
    counts = {}
    known_pid = None
    connection = connections.retain(http.client.HTTPConnection('localhost', timeout=60))
    connection.sock = connect(port, context, 'h1', proxy, tls_state)
    try:
        for exchange in exchanges:
            connection.request(
                'POST' if exchange.body else 'GET',
                exchange.path,
                exchange.body,
                dict(exchange.headers),
            )
            response = connection.getresponse()
            data = bytearray()
            while chunk := response.read(16384):
                data.extend(chunk)
                if exchange.slow:
                    time.sleep(0.005)
            if response.status != exchange.status or data != exchange.expected:
                raise RuntimeError(
                    f'H1 response mismatch: {exchange.path}, status {response.status}'
                )
            worker = response.getheader('x-worker', known_pid)
            if worker is None:
                raise RuntimeError('H1 response is missing its worker identity')
            pid = int(worker)
            known_pid = pid
            counts[pid] = counts.get(pid, 0) + 1
        if tls_state is not None:
            tls_state.session = cast('ssl.SSLSocket', connection.sock).session
    except BaseException:
        connection.close()
        raise
    return counts


def http2(
    port: int | Path,
    exchanges: list[Exchange],
    context: ssl.SSLContext | None,
    connections: Connections,
    *,
    slots: int = 16,
    proxy: str = '',
    tls_state: TlsState | None = None,
) -> dict[int, int]:
    counts = {}
    known_pid = None
    with nullcontext(
        connections.retain(connect(port, context, 'h2', proxy, tls_state))
    ) as sock:
        connection = h2.connection.H2Connection(
            config=h2.config.H2Configuration(client_side=True)
        )
        connection.initiate_connection()
        connection.update_settings({
            h2.settings.SettingCodes.INITIAL_WINDOW_SIZE: 65536
        })
        active: dict[int, Pending] = {}
        remaining = iter(exchanges)
        exhausted = False
        while active or not exhausted:
            while not exhausted and len(active) < slots:
                exchange = next(remaining, None)
                if exchange is None:
                    exhausted = True
                    break
                stream = connection.get_next_available_stream_id()
                active[stream] = Pending(exchange)
                connection.send_headers(
                    stream,
                    [
                        (':method', 'POST' if exchange.body else 'GET'),
                        (':path', exchange.path),
                        (':scheme', 'https' if context else 'http'),
                        (':authority', 'localhost'),
                        ('content-length', str(len(exchange.body))),
                        *exchange.headers,
                    ],
                    end_stream=not exchange.body,
                )
            for stream, pending in active.items():
                body = pending.exchange.body
                while pending.sent < len(body):
                    size = min(
                        connection.local_flow_control_window(stream),
                        connection.max_outbound_frame_size,
                        len(body) - pending.sent,
                    )
                    if size <= 0:
                        break
                    end = pending.sent + size
                    connection.send_data(
                        stream, body[pending.sent : end], end_stream=end == len(body)
                    )
                    pending.sent = end
            sock.sendall(connection.data_to_send())
            if not active:
                break
            data = sock.recv(65536)
            if not data:
                raise RuntimeError('H2 peer closed before all requests completed')
            for event in connection.receive_data(data):
                if isinstance(event, h2.events.ResponseReceived):
                    active[event.stream_id].headers = dict(event.headers)
                elif isinstance(event, h2.events.InformationalResponseReceived):
                    if dict(event.headers).get(b':status') != b'103':
                        raise RuntimeError(
                            'H2 informational response is not an early hint'
                        )
                    active[event.stream_id].early_hints.extend(
                        value for key, value in event.headers if key == b'link'
                    )
                elif isinstance(event, h2.events.TrailersReceived):
                    active[event.stream_id].trailers = dict(event.headers)
                elif isinstance(event, h2.events.DataReceived):
                    pending = active[event.stream_id]
                    pending.received.extend(event.data)
                    if pending.exchange.slow:
                        time.sleep(0.005)
                    connection.acknowledge_received_data(
                        event.flow_controlled_length, event.stream_id
                    )
                elif isinstance(event, h2.events.StreamEnded):
                    pending = active.pop(event.stream_id)
                    exchange = pending.exchange
                    if (
                        pending.headers.get(b':status') != str(exchange.status).encode()
                        or pending.received != exchange.expected
                        or tuple(pending.early_hints) != exchange.early_hints
                        or any(
                            pending.trailers.get(key) != value
                            for key, value in exchange.trailers
                        )
                        or (
                            exchange.grpc
                            and pending.trailers.get(b'grpc-status') != b'0'
                        )
                    ):
                        raise RuntimeError(f'H2 response mismatch: {exchange.path}')
                    pid = int(pending.headers.get(b'x-worker', known_pid))
                    known_pid = pid
                    counts[pid] = counts.get(pid, 0) + 1
                elif isinstance(
                    event, (h2.events.StreamReset, h2.events.ConnectionTerminated)
                ):
                    raise OSError(f'H2 training peer interrupted work: {event}')
        if tls_state is not None:
            tls_state.session = cast('ssl.SSLSocket', sock).session
        sock.sendall(connection.data_to_send())
    return counts


def http1_trailers(
    port: int | Path,
    exchanges: list[Exchange],
    context: ssl.SSLContext | None,
    connections: Connections,
    *,
    proxy: str = '',
    tls_state: TlsState | None = None,
) -> dict[int, int]:
    counts = {}
    sock = connections.retain(connect(port, context, 'h1', proxy, tls_state))
    connection = h11.Connection(h11.CLIENT)
    for exchange in exchanges:
        sock.sendall(
            connection.send(
                h11.Request(
                    method='GET',
                    target=exchange.path,
                    headers=[('host', 'localhost'), *exchange.headers],
                )
            )
        )
        sock.sendall(connection.send(h11.EndOfMessage()))
        response = None
        body = bytearray()
        while True:
            event = connection.next_event()
            if event is h11.NEED_DATA:
                data = sock.recv(65536)
                if not data:
                    raise RuntimeError('H1 peer closed before its trailers')
                connection.receive_data(data)
            elif isinstance(event, h11.Response):
                response = event
            elif isinstance(event, h11.Data):
                body.extend(event.data)
            elif isinstance(event, h11.EndOfMessage):
                if (
                    response is None
                    or response.status_code != exchange.status
                    or body != exchange.expected
                    or tuple(event.headers) != exchange.trailers
                ):
                    raise RuntimeError(f'H1 trailer response mismatch: {exchange.path}')
                pid = int(dict(response.headers)[b'x-worker'])
                counts[pid] = counts.get(pid, 0) + 1
                connection.start_next_cycle()
                break
            else:
                raise RuntimeError(f'unexpected H1 trailer response event: {event}')
    if tls_state is not None:
        tls_state.session = cast('ssl.SSLSocket', sock).session
    return counts


def digest(data: bytes) -> bytes:
    return hashlib.sha256(data).hexdigest().encode()
