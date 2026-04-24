import asyncio
import socket
from contextlib import asynccontextmanager
from pathlib import Path

import h2.config
import h2.connection
import h2.events
from h2corn import Config, Server
from h2corn._config import TcpBindSpec, UnixBindSpec, _parse_bind_spec


async def open_h2_connection(
    *,
    host: str = '127.0.0.1',
    port: int | None = None,
    uds: Path | None = None,
    prefix: bytes = b'',
) -> tuple[
    asyncio.StreamReader,
    asyncio.StreamWriter,
    h2.connection.H2Connection,
    bytes,
]:
    if uds is not None:
        reader, writer = await asyncio.open_unix_connection(uds)
        authority = b'localhost'
    else:
        assert port is not None
        reader, writer = await asyncio.open_connection(host, port)
        authority = f'{host}:{port}'.encode()

    conn = h2.connection.H2Connection(
        config=h2.config.H2Configuration(client_side=True, header_encoding=None)
    )
    conn.initiate_connection()
    writer.write(prefix + conn.data_to_send())
    await writer.drain()
    return reader, writer, conn, authority


async def read_h2_response(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    conn: h2.connection.H2Connection,
    stream_id: int,
) -> tuple[int, bytes, list[tuple[bytes, bytes]]]:
    status = None
    response_body = bytearray()
    response_trailers: list[tuple[bytes, bytes]] = []

    while True:
        data = await reader.read(65535)
        if not data:
            break
        for event in conn.receive_data(data):
            if isinstance(event, h2.events.ResponseReceived):
                status = int(dict(event.headers)[b':status'])
            elif isinstance(event, h2.events.TrailersReceived):
                response_trailers.extend(event.headers)
            elif isinstance(event, h2.events.DataReceived):
                response_body.extend(event.data)
                conn.acknowledge_received_data(
                    event.flow_controlled_length,
                    stream_id,
                )
            elif isinstance(event, h2.events.StreamEnded):
                pending = conn.data_to_send()
                if pending:
                    writer.write(pending)
                    await writer.drain()
                assert status is not None
                return status, bytes(response_body), response_trailers
        pending = conn.data_to_send()
        if pending:
            writer.write(pending)
            await writer.drain()

    raise RuntimeError('response stream ended unexpectedly')


def find_free_port() -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]
    finally:
        sock.close()


def proxy_v1_prefix(
    *,
    transport: str = 'TCP4',
    client_host: str,
    server_host: str,
    client_port: int,
    server_port: int,
) -> bytes:
    return f'PROXY {transport} {client_host} {server_host} {client_port} {server_port}\r\n'.encode()


def proxy_v2_prefix(
    *,
    client_host: str,
    server_host: str,
    client_port: int,
    server_port: int,
    tlvs: bytes = b'',
) -> bytes:
    payload = (
        socket.inet_aton(client_host)
        + socket.inet_aton(server_host)
        + client_port.to_bytes(2, 'big')
        + server_port.to_bytes(2, 'big')
        + tlvs
    )
    return (
        b'\r\n\r\n\x00\r\nQUIT\n'
        + bytes([0x21])
        + bytes([0x11])
        + len(payload).to_bytes(2, 'big')
        + payload
    )


async def h2_request_details(
    *,
    host: str = '127.0.0.1',
    port: int | None = None,
    uds: Path | None = None,
    method: str = 'GET',
    path: str = '/',
    body: bytes = b'',
    extra_headers: list[tuple[bytes, bytes]] | None = None,
    prefix: bytes = b'',
) -> tuple[int, bytes, list[tuple[bytes, bytes]]]:
    reader, writer, conn, authority = await open_h2_connection(
        host=host,
        port=port,
        uds=uds,
        prefix=prefix,
    )
    try:
        stream_id = conn.get_next_available_stream_id()
        headers = [
            (b':method', method.encode()),
            (b':scheme', b'http'),
            (b':authority', authority),
            (b':path', path.encode()),
        ]
        if extra_headers is not None:
            headers.extend(extra_headers)
        conn.send_headers(stream_id, headers, end_stream=not body)
        if body:
            conn.send_data(stream_id, body, end_stream=True)
        writer.write(conn.data_to_send())
        await writer.drain()
        return await read_h2_response(reader, writer, conn, stream_id)
    finally:
        writer.close()
        await writer.wait_closed()


async def h2_request(
    *,
    host: str = '127.0.0.1',
    port: int | None = None,
    uds: Path | None = None,
    method: str = 'GET',
    path: str = '/',
    body: bytes = b'',
    extra_headers: list[tuple[bytes, bytes]] | None = None,
    prefix: bytes = b'',
) -> tuple[int, bytes]:
    status, response_body, _ = await h2_request_details(
        host=host,
        port=port,
        uds=uds,
        method=method,
        path=path,
        body=body,
        extra_headers=extra_headers,
        prefix=prefix,
    )
    return status, response_body


async def read_http_request_body(receive) -> bytes:
    chunks = bytearray()
    while True:
        message = await receive()
        assert message['type'] == 'http.request'
        chunks.extend(message.get('body', b''))
        if not message.get('more_body', False):
            return bytes(chunks)


async def http1_request(
    *,
    port: int,
    request: bytes,
    head_only: bool = False,
) -> tuple[int, dict[bytes, bytes], bytes, list[tuple[bytes, bytes]]]:
    reader, writer = await asyncio.open_connection('127.0.0.1', port)
    writer.write(request)
    await writer.drain()
    try:
        return await read_http1_response(reader, head_only=head_only)
    finally:
        writer.close()
        await writer.wait_closed()


async def read_http1_response(
    reader: asyncio.StreamReader,
    *,
    head_only: bool = False,
) -> tuple[int, dict[bytes, bytes], bytes, list[tuple[bytes, bytes]]]:
    head = await asyncio.wait_for(reader.readuntil(b'\r\n\r\n'), timeout=5)
    lines = head[:-4].split(b'\r\n')
    status = int(lines[0].split(b' ', 2)[1])
    headers = {}
    for line in lines[1:]:
        name, value = line.split(b':', 1)
        headers[name.lower()] = value.strip()

    body = bytearray()
    trailers: list[tuple[bytes, bytes]] = []
    if head_only:
        return status, headers, bytes(body), trailers
    if headers.get(b'transfer-encoding') == b'chunked':
        while True:
            line = await asyncio.wait_for(reader.readuntil(b'\r\n'), timeout=5)
            size = int(line[:-2].split(b';', 1)[0], 16)
            if size == 0:
                while True:
                    line = await asyncio.wait_for(reader.readuntil(b'\r\n'), timeout=5)
                    if line == b'\r\n':
                        break
                    name, value = line[:-2].split(b':', 1)
                    trailers.append((name.lower(), value.strip()))
                break
            body.extend(await asyncio.wait_for(reader.readexactly(size), timeout=5))
            assert await asyncio.wait_for(reader.readexactly(2), timeout=5) == b'\r\n'
    elif (content_length := headers.get(b'content-length')) is not None:
        body.extend(
            await asyncio.wait_for(
                reader.readexactly(int(content_length)),
                timeout=5,
            )
        )
    return status, headers, bytes(body), trailers


@asynccontextmanager
async def running_server(app, config: Config):
    server = Server(app, config)
    task = asyncio.create_task(server.serve())
    await wait_for_config_binds(config)
    try:
        yield server
    finally:
        server.shutdown()
        try:
            await asyncio.wait_for(task, timeout=2)
        except TimeoutError:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass


async def wait_for_config_binds(config: Config, timeout: float = 5.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        pending_unix = []
        pending_ports = []
        for bind in config.bind:
            spec = _parse_bind_spec(bind)
            if isinstance(spec, TcpBindSpec):
                if spec.port == 0:
                    pending_ports = None
                    break
                pending_ports.append((spec.host, spec.port))
            elif isinstance(spec, UnixBindSpec):
                pending_unix.append(spec.path)
        if pending_ports is not None and all(
            _port_is_open(host, port) for host, port in pending_ports
        ) and all(path.exists() for path in pending_unix):
            return
        if loop.time() >= deadline:
            raise TimeoutError(f'timed out waiting for listeners {config.bind}')
        await asyncio.sleep(0.01)


def _port_is_open(host: str, port: int) -> bool:
    family = socket.AF_INET6 if ':' in host else socket.AF_INET
    with socket.socket(family, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.1)
        if family == socket.AF_INET6:
            return sock.connect_ex((host, port, 0, 0)) == 0
        return sock.connect_ex((host, port)) == 0


async def wait_for_port(port: int, timeout: float = 5.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(0.1)
            if sock.connect_ex(('127.0.0.1', port)) == 0:
                return
        if loop.time() >= deadline:
            raise TimeoutError(f'timed out waiting for port {port}')
        await asyncio.sleep(0.01)
