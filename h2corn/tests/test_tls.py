import asyncio
import ssl
from contextlib import suppress
from pathlib import Path

import h2.config
import h2.connection
import pytest
import trustme
from h2corn import Config

from tests._support import (
    proxy_v1_prefix,
    proxy_v2_prefix,
    read_h2_response,
    read_http1_response,
    running_server,
    server_port,
)

pytestmark = pytest.mark.asyncio


def write_self_signed_cert(tmp_path: Path) -> tuple[Path, Path]:
    # Generated in-process with trustme so the certs are RFC-clean and identical
    # on every OS (the host openssl/LibreSSL emits extensions rustls rejects).
    ca = trustme.CA()
    cert = ca.issue_cert('localhost', '127.0.0.1')
    certfile = tmp_path / 'server.crt'
    keyfile = tmp_path / 'server.key'
    # The file carries the leaf plus the CA: the server presents the chain and
    # the same file doubles as the client's trust anchor.
    certfile.write_bytes(cert.cert_chain_pems[0].bytes() + ca.cert_pem.bytes())
    cert.private_key_pem.write_to_path(str(keyfile))
    return certfile, keyfile


def write_mutual_tls_certs(tmp_path: Path) -> tuple[Path, Path, Path, Path, Path]:
    # One trustme CA signs the server and client leaves. Each side trusts the CA;
    # generation is in-process so it is RFC-clean and identical on every OS.
    ca = trustme.CA()
    server = ca.issue_cert('localhost', '127.0.0.1')
    client = ca.issue_cert('h2corn-client@example.com')

    ca_cert = tmp_path / 'ca.crt'
    server_cert = tmp_path / 'server.crt'
    server_key = tmp_path / 'server.key'
    client_cert = tmp_path / 'client.crt'
    client_key = tmp_path / 'client.key'

    ca.cert_pem.write_to_path(str(ca_cert))
    server.cert_chain_pems[0].write_to_path(str(server_cert))
    server.private_key_pem.write_to_path(str(server_key))
    client.cert_chain_pems[0].write_to_path(str(client_cert))
    client.private_key_pem.write_to_path(str(client_key))
    return ca_cert, server_cert, server_key, client_cert, client_key


def client_context(
    cafile: Path,
    *,
    alpn: list[str] | None = None,
    certfile: Path | None = None,
    keyfile: Path | None = None,
) -> ssl.SSLContext:
    context = ssl.create_default_context(cafile=str(cafile))
    if certfile is not None and keyfile is not None:
        context.load_cert_chain(certfile=str(certfile), keyfile=str(keyfile))
    if alpn is not None:
        context.set_alpn_protocols(alpn)
    return context


async def tls_http1_request(
    port: int,
    context: ssl.SSLContext,
    *,
    request: bytes = b'GET / HTTP/1.1\r\nHost: localhost\r\n\r\n',
) -> bytes:
    reader, writer = await asyncio.open_connection(
        '127.0.0.1',
        port,
        ssl=context,
        server_hostname='localhost',
    )
    try:
        writer.write(request)
        await writer.drain()
        status, _, body, _ = await read_http1_response(reader)
    finally:
        writer.close()
        with suppress(OSError, ssl.SSLError):
            await writer.wait_closed()
    assert status == 200
    return body


async def open_prefixed_tls_connection(
    port: int,
    context: ssl.SSLContext,
    *,
    prefix: bytes = b'',
) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    reader, writer = await asyncio.open_connection('127.0.0.1', port)
    try:
        if prefix:
            writer.write(prefix)
            await writer.drain()
        await writer.start_tls(context, server_hostname='localhost')
    except BaseException:
        # Close the plain-TCP transport on handshake failure so its pending
        # futures don't surface "exception was never retrieved" at GC time.
        writer.close()
        with suppress(OSError, ssl.SSLError):
            await writer.wait_closed()
        raise
    return reader, writer


async def tls_h2_request(
    port: int,
    context: ssl.SSLContext,
    *,
    prefix: bytes = b'',
    path: str = '/',
    scheme: bytes = b'https',
    extra_headers: list[tuple[bytes, bytes]] | None = None,
) -> tuple[int, bytes]:
    reader, writer = await open_prefixed_tls_connection(
        port,
        context,
        prefix=prefix,
    )
    assert writer.get_extra_info('ssl_object').selected_alpn_protocol() == 'h2'
    conn = h2.connection.H2Connection(
        config=h2.config.H2Configuration(
            client_side=True,
            header_encoding=None,
        )
    )
    conn.initiate_connection()
    stream_id = conn.get_next_available_stream_id()
    headers = [
        (b':method', b'GET'),
        (b':scheme', scheme),
        (b':authority', f'127.0.0.1:{port}'.encode()),
        (b':path', path.encode()),
    ]
    if extra_headers is not None:
        headers.extend(extra_headers)
    conn.send_headers(stream_id, headers, end_stream=True)
    writer.write(conn.data_to_send())
    await writer.drain()
    try:
        status, body, _ = await read_h2_response(reader, writer, conn, stream_id)
        return status, body
    finally:
        writer.close()
        with suppress(ConnectionResetError, ssl.SSLError):
            await writer.wait_closed()


async def test_tls_http2_alpn_round_trip(tmp_path: Path) -> None:
    certfile, keyfile = write_self_signed_cert(tmp_path)

    async def app(scope, receive, send):
        assert scope['type'] == 'http'
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': scope['scheme'].encode()})

    config = Config(
        port=0,
        certfile=certfile,
        keyfile=keyfile,
    )
    context = client_context(certfile, alpn=['h2'])
    async with running_server(app, config) as server:
        reader, writer = await asyncio.open_connection(
            '127.0.0.1',
            server_port(server),
            ssl=context,
            server_hostname='localhost',
        )
        assert writer.get_extra_info('ssl_object').selected_alpn_protocol() == 'h2'
        conn = h2.connection.H2Connection(
            config=h2.config.H2Configuration(
                client_side=True,
                header_encoding=None,
            )
        )
        conn.initiate_connection()
        stream_id = conn.get_next_available_stream_id()
        conn.send_headers(
            stream_id,
            [
                (b':method', b'GET'),
                (b':scheme', b'https'),
                (b':authority', f'127.0.0.1:{server_port(server)}'.encode()),
                (b':path', b'/'),
            ],
            end_stream=True,
        )
        writer.write(conn.data_to_send())
        await writer.drain()
        status, body, _ = await read_h2_response(reader, writer, conn, stream_id)
        writer.close()
        with suppress(ssl.SSLError):
            await writer.wait_closed()

    assert status == 200
    assert body == b'https'


async def test_tls_http2_overrides_spoofed_scheme(tmp_path: Path) -> None:
    certfile, keyfile = write_self_signed_cert(tmp_path)

    async def app(scope, receive, send):
        assert scope['type'] == 'http'
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': scope['scheme'].encode()})

    config = Config(
        port=0,
        certfile=certfile,
        keyfile=keyfile,
    )
    context = client_context(certfile, alpn=['h2'])
    async with running_server(app, config) as server:
        status, body = await asyncio.wait_for(
            tls_h2_request(server_port(server), context, scheme=b'http'),
            timeout=5,
        )

    assert status == 200
    assert body == b'https'


async def test_tls_http2_trusted_forwarded_proto_overrides_tls_scheme(
    tmp_path: Path,
) -> None:
    certfile, keyfile = write_self_signed_cert(tmp_path)

    async def app(scope, receive, send):
        assert scope['type'] == 'http'
        payload = f'{scope["scheme"]}|{scope["server"][0]}|{scope["server"][1]}'
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': payload.encode()})

    config = Config(
        port=0,
        certfile=certfile,
        keyfile=keyfile,
        proxy_headers=True,
        forwarded_allow_ips=('127.0.0.1',),
    )
    context = client_context(certfile, alpn=['h2'])
    async with running_server(app, config) as server:
        status, body = await asyncio.wait_for(
            tls_h2_request(
                server_port(server),
                context,
                extra_headers=[(b'forwarded', b'proto=http;host=example.com')],
            ),
            timeout=5,
        )

    assert status == 200
    assert body == b'http|example.com|80'


async def test_tls_http1_absolute_form_preserves_tls_scheme(tmp_path: Path) -> None:
    certfile, keyfile = write_self_signed_cert(tmp_path)

    async def app(scope, receive, send):
        assert scope['type'] == 'http'
        body = (
            f'{scope["scheme"]}|{scope["path"]}|{scope["query_string"].decode()}'
        ).encode()
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': body})

    config = Config(
        port=0,
        certfile=certfile,
        keyfile=keyfile,
    )
    context = client_context(certfile, alpn=['http/1.1'])
    async with running_server(app, config) as server:
        body = await asyncio.wait_for(
            tls_http1_request(
                server_port(server),
                context,
                request=b'GET http://localhost/absolute?x=1 HTTP/1.1\r\n\r\n',
            ),
            timeout=5,
        )

    assert body == b'https|/absolute|x=1'


@pytest.mark.parametrize('proxy_protocol', ['v1', 'v2'])
async def test_tls_proxy_protocol_rewrites_h2_scope(
    tmp_path: Path,
    proxy_protocol: str,
) -> None:
    certfile, keyfile = write_self_signed_cert(tmp_path)

    async def app(scope, receive, send):
        assert scope['type'] == 'http'
        payload = (
            f'{scope["scheme"]}|{scope["client"][0]}|{scope["client"][1]}|'
            f'{scope["server"][0]}|{scope["server"][1]}'
        ).encode()
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': payload})

    config = Config(
        port=0,
        certfile=certfile,
        keyfile=keyfile,
        proxy_protocol=proxy_protocol,
        forwarded_allow_ips=('127.0.0.1',),
    )
    prefix_args = {
        'client_host': '203.0.113.10',
        'server_host': '198.51.100.20',
        'client_port': 41234,
        'server_port': 8443,
    }
    prefix = (
        proxy_v1_prefix(**prefix_args)
        if proxy_protocol == 'v1'
        else proxy_v2_prefix(**prefix_args)
    )
    context = client_context(certfile, alpn=['h2'])
    async with running_server(app, config) as server:
        status, body = await asyncio.wait_for(
            tls_h2_request(server_port(server), context, prefix=prefix),
            timeout=5,
        )

    assert status == 200
    assert body == b'https|203.0.113.10|41234|198.51.100.20|8443'


async def test_tls_without_http1_rejects_http1_alpn_client(tmp_path: Path) -> None:
    certfile, keyfile = write_self_signed_cert(tmp_path)

    async def app(scope, receive, send):
        raise AssertionError('request should not reach the ASGI app')

    config = Config(
        port=0,
        http1=False,
        certfile=certfile,
        keyfile=keyfile,
    )
    context = client_context(certfile, alpn=['http/1.1'])
    async with running_server(app, config) as server:
        try:
            reader, writer = await open_prefixed_tls_connection(
                server_port(server), context
            )
        except (ConnectionResetError, ssl.SSLError):
            return
        assert writer.get_extra_info('ssl_object').selected_alpn_protocol() is None
        writer.write(b'GET / HTTP/1.1\r\nHost: localhost\r\n\r\n')
        await writer.drain()
        with suppress(ConnectionResetError):
            assert await asyncio.wait_for(reader.read(1), timeout=5) == b''
        writer.close()
        with suppress(ConnectionResetError, ssl.SSLError):
            await writer.wait_closed()


async def test_tls_without_http1_accepts_h2_when_client_also_offers_http1(
    tmp_path: Path,
) -> None:
    certfile, keyfile = write_self_signed_cert(tmp_path)

    async def app(scope, receive, send):
        assert scope['type'] == 'http'
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': scope['scheme'].encode()})

    config = Config(
        port=0,
        http1=False,
        certfile=certfile,
        keyfile=keyfile,
    )
    context = client_context(certfile, alpn=['http/1.1', 'h2'])
    async with running_server(app, config) as server:
        status, body = await asyncio.wait_for(
            tls_h2_request(server_port(server), context),
            timeout=5,
        )

    assert status == 200
    assert body == b'https'


async def test_tls_http1_websocket_scope_uses_wss(tmp_path: Path) -> None:
    certfile, keyfile = write_self_signed_cert(tmp_path)
    state = {}

    async def app(scope, receive, send):
        assert scope['type'] == 'websocket'
        state['scheme'] = scope['scheme']
        assert await receive() == {'type': 'websocket.connect'}
        await send({'type': 'websocket.close'})

    config = Config(
        port=0,
        certfile=certfile,
        keyfile=keyfile,
    )
    context = client_context(certfile, alpn=['http/1.1'])
    async with running_server(app, config) as server:
        reader, writer = await open_prefixed_tls_connection(
            server_port(server), context
        )
        writer.write(
            b'GET /ws HTTP/1.1\r\n'
            b'Host: localhost\r\n'
            b'Connection: Upgrade\r\n'
            b'Upgrade: websocket\r\n'
            b'Sec-WebSocket-Version: 13\r\n'
            b'Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n'
            b'\r\n'
        )
        await writer.drain()
        status, _, body, _ = await read_http1_response(reader)
        writer.close()
        with suppress(ConnectionResetError, ssl.SSLError):
            await writer.wait_closed()

    assert state == {'scheme': 'wss'}
    assert status == 403
    assert body == b''


@pytest.mark.parametrize('scheme', [b'https', b'http'])
async def test_tls_http2_websocket_scope_uses_wss(
    tmp_path: Path,
    scheme: bytes,
) -> None:
    certfile, keyfile = write_self_signed_cert(tmp_path)
    state = {}

    async def app(scope, receive, send):
        assert scope['type'] == 'websocket'
        state['scheme'] = scope['scheme']
        state['http_version'] = scope['http_version']
        assert await receive() == {'type': 'websocket.connect'}
        await send({'type': 'websocket.close'})

    config = Config(
        port=0,
        certfile=certfile,
        keyfile=keyfile,
    )
    context = client_context(certfile, alpn=['h2'])
    async with running_server(app, config) as server:
        reader, writer = await open_prefixed_tls_connection(
            server_port(server), context
        )
        assert writer.get_extra_info('ssl_object').selected_alpn_protocol() == 'h2'
        conn = h2.connection.H2Connection(
            config=h2.config.H2Configuration(
                client_side=True,
                header_encoding=None,
            )
        )
        conn.initiate_connection()
        stream_id = conn.get_next_available_stream_id()
        conn.send_headers(
            stream_id,
            [
                (b':method', b'CONNECT'),
                (b':protocol', b'websocket'),
                (b':scheme', scheme),
                (b':authority', b'localhost'),
                (b':path', b'/ws'),
                (b'sec-websocket-version', b'13'),
            ],
            end_stream=False,
        )
        writer.write(conn.data_to_send())
        await writer.drain()
        status, body, _ = await read_h2_response(reader, writer, conn, stream_id)
        writer.close()
        with suppress(ConnectionResetError, ssl.SSLError):
            await writer.wait_closed()

    assert state == {'scheme': 'wss', 'http_version': '2'}
    assert status == 403
    assert body == b''


async def test_tls_http2_pathsend_streams_file(tmp_path: Path) -> None:
    certfile, keyfile = write_self_signed_cert(tmp_path)
    file_path = tmp_path / 'payload.bin'
    payload = (b'tls-h2-pathsend-' * 3000)[:30000]
    file_path.write_bytes(payload)

    async def app(scope, receive, send):
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [
                (b'content-type', b'application/octet-stream'),
                (b'content-length', str(len(payload)).encode()),
            ],
        })
        await send({'type': 'http.response.pathsend', 'path': str(file_path)})

    config = Config(
        port=0,
        certfile=certfile,
        keyfile=keyfile,
    )
    context = client_context(certfile, alpn=['h2'])
    async with running_server(app, config) as server:
        status, body = await asyncio.wait_for(
            tls_h2_request(server_port(server), context, path='/download'),
            timeout=5,
        )

    assert status == 200
    assert body == payload


async def test_tls_http1_fallback_without_alpn(tmp_path: Path) -> None:
    certfile, keyfile = write_self_signed_cert(tmp_path)

    async def app(scope, receive, send):
        assert scope['type'] == 'http'
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': scope['scheme'].encode()})

    config = Config(
        port=0,
        certfile=certfile,
        keyfile=keyfile,
    )
    context = client_context(certfile)
    async with running_server(app, config) as server:
        reader, writer = await asyncio.open_connection(
            '127.0.0.1',
            server_port(server),
            ssl=context,
            server_hostname='localhost',
        )
        assert writer.get_extra_info('ssl_object').selected_alpn_protocol() is None
        writer.write(b'GET / HTTP/1.1\r\nHost: localhost\r\n\r\n')
        await writer.drain()
        status, _, body, _ = await read_http1_response(reader)
        writer.close()
        await writer.wait_closed()

    assert status == 200
    assert body == b'https'


async def test_tls_without_http1_rejects_no_alpn_client(tmp_path: Path) -> None:
    certfile, keyfile = write_self_signed_cert(tmp_path)

    async def app(scope, receive, send):
        raise AssertionError('request should not reach the ASGI app')

    config = Config(
        port=0,
        http1=False,
        certfile=certfile,
        keyfile=keyfile,
    )
    context = client_context(certfile)
    async with running_server(app, config) as server:
        reader, writer = await asyncio.open_connection(
            '127.0.0.1',
            server_port(server),
            ssl=context,
            server_hostname='localhost',
        )
        writer.write(b'GET / HTTP/1.1\r\nHost: localhost\r\n\r\n')
        await writer.drain()
        with suppress(ConnectionResetError):
            assert await asyncio.wait_for(reader.read(1), timeout=5) == b''
        writer.close()
        with suppress(ConnectionResetError, ssl.SSLError):
            await writer.wait_closed()


async def test_required_client_certificate_accepts_trusted_client(
    tmp_path: Path,
) -> None:
    ca_cert, server_cert, server_key, client_cert, client_key = write_mutual_tls_certs(
        tmp_path
    )

    async def app(scope, receive, send):
        assert scope['type'] == 'http'
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'mtls'})

    config = Config(
        port=0,
        certfile=server_cert,
        keyfile=server_key,
        ca_certs=ca_cert,
        cert_reqs='required',
    )
    context = client_context(ca_cert, certfile=client_cert, keyfile=client_key)
    async with running_server(app, config) as server:
        body = await tls_http1_request(server_port(server), context)

    assert body == b'mtls'


async def test_required_client_certificate_rejects_missing_client_cert(
    tmp_path: Path,
) -> None:
    ca_cert, server_cert, server_key, _, _ = write_mutual_tls_certs(tmp_path)

    async def app(scope, receive, send):
        raise AssertionError('request should not reach the ASGI app')

    config = Config(
        port=0,
        certfile=server_cert,
        keyfile=server_key,
        ca_certs=ca_cert,
        cert_reqs='required',
    )
    context = client_context(ca_cert)
    async with running_server(app, config) as server:
        with pytest.raises((ConnectionResetError, ssl.SSLError, TimeoutError)):
            await asyncio.wait_for(
                tls_http1_request(server_port(server), context), timeout=5
            )


async def test_optional_client_certificate_allows_missing_client_cert(
    tmp_path: Path,
) -> None:
    ca_cert, server_cert, server_key, _, _ = write_mutual_tls_certs(tmp_path)

    async def app(scope, receive, send):
        assert scope['type'] == 'http'
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'optional'})

    config = Config(
        port=0,
        certfile=server_cert,
        keyfile=server_key,
        ca_certs=ca_cert,
        cert_reqs='optional',
    )
    context = client_context(ca_cert)
    async with running_server(app, config) as server:
        body = await tls_http1_request(server_port(server), context)

    assert body == b'optional'
