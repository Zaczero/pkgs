import asyncio
import socket
from contextlib import asynccontextmanager
from pathlib import Path

import h2.config
import h2.connection
import h2.events
import h2.settings
from h2corn import Config, Server
from h2corn._config import TcpBindSpec, parse_bind_spec


async def open_h2_connection(
    *,
    host: str = '127.0.0.1',
    port: int | None = None,
    uds: Path | None = None,
    prefix: bytes = b'',
    max_header_list_size: int | None = None,
    initial_window_size: int | None = None,
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
        # An IPv6 literal must be bracketed in an authority (RFC 3986 §3.2.2);
        # unbracketed, it is malformed and the server is right to refuse it.
        literal = f'[{host}]' if ':' in host else host
        authority = f'{literal}:{port}'.encode()

    conn = h2.connection.H2Connection(
        config=h2.config.H2Configuration(client_side=True, header_encoding=None)
    )
    if max_header_list_size is not None:
        conn.local_settings[h2.settings.SettingCodes.MAX_HEADER_LIST_SIZE] = (
            max_header_list_size
        )
        # h2 applies an advertised setting to its decoder on the peer's ACK.
        # A peer may answer the opening request in the same read as that ACK,
        # so accept the advertised limit from the first response frame too.
        conn.decoder.max_header_list_size = max_header_list_size
    if initial_window_size is not None:
        # Advertised in the opening SETTINGS, so the server never has stream
        # credit to write DATA into. Only the connection's own outbound byte
        # budget can bound what it queues.
        conn.local_settings[h2.settings.SettingCodes.INITIAL_WINDOW_SIZE] = (
            initial_window_size
        )
    conn.initiate_connection()
    writer.write(prefix + conn.data_to_send())
    await writer.drain()
    return reader, writer, conn, authority


async def read_raw_h2_frames(
    reader: asyncio.StreamReader,
    *,
    timeout: float = 5.0,
    stop_at_goaway: bool = True,
) -> list[tuple[int, int, int, bytes]]:
    """Read raw HTTP/2 frames as ``(type, flags, stream_id, payload)`` tuples
    until GOAWAY (optional), peer close, or ``timeout`` of inactivity.
    """
    frames = []
    try:
        while True:
            header = await asyncio.wait_for(reader.readexactly(9), timeout=timeout)
            length = int.from_bytes(header[:3], 'big')
            frame_type = header[3]
            flags = header[4]
            stream_id = int.from_bytes(header[5:9], 'big') & 0x7FFF_FFFF
            payload = await asyncio.wait_for(
                reader.readexactly(length), timeout=timeout
            )
            frames.append((frame_type, flags, stream_id, payload))
            if stop_at_goaway and frame_type == 0x07:
                return frames
    except (asyncio.IncompleteReadError, TimeoutError):
        return frames


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

    # The peer closed before END_STREAM. That is a transport outcome, not a
    # helper defect, and callers that tolerate a server going away mid-request
    # must be able to catch it as one -- a bare RuntimeError forces them to
    # swallow genuine test bugs to do so.
    raise ConnectionAbortedError('response stream ended unexpectedly')


def find_free_port() -> int:
    """Allocate a port for SUBPROCESS-spawned servers only.

    Allocate-close-rebind is inherently racy; in-process tests must bind
    port 0 and read the kernel-assigned port back via `server_port`.
    """
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
        assert message['type'] == 'http.request', message
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
    # RFC 9112 section 6.3: a response to HEAD and any 1xx, 204 or 304 is
    # terminated by the blank line whatever length fields it carries, so a
    # `Content-Length` describing the representation a GET would have returned
    # must not be read as a body. 205 is deliberately absent from that list.
    if head_only or status < 200 or status in {204, 304}:
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
    await wait_for_server(server, task)
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


def server_port(server: Server, index: int = 0) -> int:
    """Kernel-assigned port of the server's `index`-th TCP listener."""
    ports = [
        spec.port
        for address in server.addresses
        if isinstance(spec := parse_bind_spec(address), TcpBindSpec)
    ]
    if index >= len(ports):
        raise AssertionError(f'no TCP listener {index}: {server.addresses!r}')
    return ports[index]


async def wait_for_server(
    server: Server,
    task: asyncio.Task,
    timeout: float = 5.0,
) -> None:
    """Wait until `server` is accepting connections.

    `Server.wait_started()` is the server's own readiness signal, so there
    is nothing to poll; racing it against the serve task turns a startup
    failure into that failure rather than a timeout.
    """
    waiting = asyncio.ensure_future(server.wait_started())
    done, _pending = await asyncio.wait(
        (waiting, task), timeout=timeout, return_when=asyncio.FIRST_COMPLETED
    )
    if not done:
        waiting.cancel()
        raise TimeoutError('timed out waiting for the server to start serving')
    if task in done:
        # Whatever ended the server is the real answer, even when readiness
        # resolved in the same pass.
        waiting.cancel()
        task.result()
        raise AssertionError('server task finished before it started serving')
    await waiting


async def assert_serve_reusable(server: Server, timeout: float = 2) -> None:
    """The server accepts a fresh serve() lifecycle after the previous ended."""
    task = asyncio.create_task(server.serve())
    await wait_for_server(server, task, timeout=timeout)
    server.shutdown()
    await asyncio.wait_for(task, timeout=timeout)


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
