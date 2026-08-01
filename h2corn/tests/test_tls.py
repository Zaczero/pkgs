import asyncio
import os
import signal
import ssl
import subprocess
import sys
import textwrap
from contextlib import suppress
from pathlib import Path
from typing import TypedDict

import h2.config
import h2.connection
import pytest
import trustme
from cryptography import x509
from h2corn import Config, TLSExtension, _server

from tests._support import (
    find_free_port,
    http1_request,
    proxy_v1_prefix,
    proxy_v2_prefix,
    read_h2_response,
    read_http1_response,
    running_server,
    server_port,
    wait_for_port,
)

pytestmark = pytest.mark.asyncio


class _NegotiatedTLS(TypedDict):
    version: str
    cipher: tuple[str, str, int]


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
    negotiated: list[_NegotiatedTLS] | None = None,
) -> bytes:
    """Make one TLS request, optionally reporting what the client negotiated.

    `negotiated` receives the client's own view of the connection, which is
    what lets a caller cross-check the values h2corn reports rather than
    comparing them against a constant and guessing when they disagree.
    """
    reader, writer = await asyncio.open_connection(
        '127.0.0.1',
        port,
        ssl=context,
        server_hostname='localhost',
    )
    try:
        if negotiated is not None:
            ssl_object = writer.get_extra_info('ssl_object')
            version = ssl_object.version()
            cipher = ssl_object.cipher()
            assert version is not None and cipher is not None
            negotiated.append({'version': version, 'cipher': cipher})
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
        with suppress(ConnectionResetError, ConnectionAbortedError, ssl.SSLError):
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
        # A rejected connection surfaces as a clean EOF, a reset (POSIX), or an
        # abort (Windows, WinError 10053).
        with suppress(ConnectionResetError, ConnectionAbortedError):
            assert await asyncio.wait_for(reader.read(1), timeout=5) == b''
        writer.close()
        with suppress(ConnectionResetError, ConnectionAbortedError, ssl.SSLError):
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
        with suppress(ConnectionResetError, ConnectionAbortedError, ssl.SSLError):
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
        with suppress(ConnectionResetError, ConnectionAbortedError, ssl.SSLError):
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
        # A rejected connection surfaces as a clean EOF, a reset (POSIX), or an
        # abort (Windows, WinError 10053).
        with suppress(ConnectionResetError, ConnectionAbortedError):
            assert await asyncio.wait_for(reader.read(1), timeout=5) == b''
        writer.close()
        with suppress(ConnectionResetError, ConnectionAbortedError, ssl.SSLError):
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
        with pytest.raises((
            ConnectionResetError,
            ConnectionAbortedError,
            ssl.SSLError,
            TimeoutError,
        )):
            await asyncio.wait_for(
                tls_http1_request(server_port(server), context), timeout=5
            )


async def test_optional_client_certificate_allows_missing_client_cert(
    tmp_path: Path,
) -> None:
    ca_cert, server_cert, server_key, _, _ = write_mutual_tls_certs(tmp_path)
    captured: list[TLSExtension] = []

    async def app(scope, receive, send):
        assert scope['type'] == 'http'
        captured.append(scope['extensions']['tls'])
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
    assert captured
    assert captured[0]['client_cert_chain'] == ()


async def test_tls_material_is_read_before_privileges_drop(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    A TLS key is routinely readable only by the user the server starts as,
    so the files must be read before `setuid`, not after. Standing in for the
    privilege drop, this makes the key unreadable at exactly that moment: a
    server that still completes a handshake was holding the bytes already.
    """
    certfile, keyfile = write_self_signed_cert(tmp_path)

    async def app(scope, receive, send):
        assert scope['type'] == 'http'
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'served'})

    dropped = False

    def drop_and_seal_the_key(identity) -> None:
        nonlocal dropped
        keyfile.chmod(0o000)
        certfile.chmod(0o000)
        dropped = True

    monkeypatch.setattr(_server, 'drop_process_privileges', drop_and_seal_the_key)

    config = Config(port=0, certfile=certfile, keyfile=keyfile)
    context = client_context(certfile, alpn=['http/1.1'])
    try:
        async with running_server(app, config) as server:
            body = await tls_http1_request(server_port(server), context)
    finally:
        keyfile.chmod(0o600)
        certfile.chmod(0o644)

    assert dropped
    assert body == b'served'


async def test_tls_extension_reports_the_verified_client_certificate(
    tmp_path: Path,
) -> None:
    """
    The ASGI TLS extension is what turns authentication into authorization:
    h2corn proves who the client is during the handshake, and this is where
    the application gets to read it.
    """
    ca_cert, server_cert, server_key, client_cert, client_key = write_mutual_tls_certs(
        tmp_path
    )
    captured: list[TLSExtension] = []

    async def app(scope, receive, send):
        assert scope['type'] == 'http'
        captured.append(scope['extensions']['tls'])
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
        assert await tls_http1_request(server_port(server), context) == b'mtls'

    assert captured
    tls = captured[0]
    # TLS 1.3 and one of its three cipher suites, as hexadecimal wire values.
    assert tls['tls_version'] == 0x0304
    assert tls['cipher_suite'] in {0x1301, 0x1302, 0x1303}

    # The leaf comes first, so `chain[0]` is the identity that connected.
    chain = tls['client_cert_chain']
    assert chain
    leaf = x509.load_pem_x509_certificate(chain[0].encode())
    expected = x509.load_pem_x509_certificate(client_cert.read_bytes())
    assert leaf.subject == expected.subject
    # Subject is rendered once as RFC 4514 — exact match against cryptography.
    assert tls['client_cert_name'] == expected.subject.rfc4514_string()

    # The configured leaf, re-encoded from what rustls parsed.
    assert tls['server_cert'] is not None
    assert x509.load_pem_x509_certificate(
        tls['server_cert'].encode()
    ) == x509.load_pem_x509_certificate(server_cert.read_bytes())

    # An unverifiable client certificate fails the handshake, so anything that
    # reaches an application verified.
    assert tls['client_cert_error'] is None


async def test_client_certificate_chain_exact_der_order(tmp_path: Path) -> None:
    """The TLS scope preserves the wire's leaf-first client chain exactly."""
    from cryptography.hazmat.primitives import serialization

    root = trustme.CA()
    intermediate = root.create_child_ca()
    server = root.issue_cert('localhost', '127.0.0.1')
    client = intermediate.issue_cert('chain-client.example')
    root_path = tmp_path / 'root.crt'
    server_cert = tmp_path / 'server.crt'
    server_key = tmp_path / 'server.key'
    client_cert = tmp_path / 'client.crt'
    client_key = tmp_path / 'client.key'
    root.cert_pem.write_to_path(str(root_path))
    server.cert_chain_pems[0].write_to_path(str(server_cert))
    server.private_key_pem.write_to_path(str(server_key))
    client_cert.write_bytes(
        client.cert_chain_pems[0].bytes() + intermediate.cert_pem.bytes()
    )
    client.private_key_pem.write_to_path(str(client_key))
    captured: list[TLSExtension] = []

    async def app(scope, receive, send):
        assert scope['type'] == 'http'
        captured.append(scope['extensions']['tls'])
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'chain'})

    config = Config(
        port=0,
        certfile=server_cert,
        keyfile=server_key,
        ca_certs=root_path,
        cert_reqs='required',
    )
    context = client_context(root_path, certfile=client_cert, keyfile=client_key)
    async with running_server(app, config) as server_instance:
        assert (
            await tls_http1_request(server_port(server_instance), context) == b'chain'
        )

    assert captured
    tls = captured[0]
    chain = tls['client_cert_chain']
    actual = tuple(
        x509.load_pem_x509_certificate(pem.encode()).public_bytes(
            serialization.Encoding.DER
        )
        for pem in chain
    )
    expected = tuple(
        x509.load_pem_x509_certificate(pem).public_bytes(serialization.Encoding.DER)
        for pem in (client.cert_chain_pems[0].bytes(), intermediate.cert_pem.bytes())
    )
    assert actual == expected
    leaf = x509.load_der_x509_certificate(expected[0])
    assert tls['client_cert_name'] == leaf.subject.rfc4514_string()


async def test_tls_extension_reports_an_empty_chain_without_client_certs(
    tmp_path: Path,
) -> None:
    """Ordinary TLS still describes itself; there is just no client identity."""
    certfile, keyfile = write_self_signed_cert(tmp_path)
    captured: list[TLSExtension] = []

    async def app(scope, receive, send):
        captured.append(scope['extensions']['tls'])
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'plain-tls'})

    config = Config(port=0, certfile=certfile, keyfile=keyfile)
    context = client_context(certfile, alpn=['http/1.1'])
    async with running_server(app, config) as server:
        assert await tls_http1_request(server_port(server), context) == b'plain-tls'

    assert captured
    tls = captured[0]
    assert tls['client_cert_chain'] == ()
    assert tls['client_cert_name'] is None
    assert tls['tls_version'] == 0x0304


async def test_tls_extension_is_absent_on_a_plaintext_connection() -> None:
    """The extension requires the key to be missing, not empty, without TLS."""
    captured_extensions: list[dict[str, object]] = []

    async def app(scope, receive, send):
        if scope['type'] == 'lifespan':
            message = await receive()
            await send({'type': f'{message["type"]}.complete'})
            return
        captured_extensions.append(dict(scope['extensions']))
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b''})

    async with running_server(app, Config(port=0)) as server:
        _status, _headers, _body, _trailers = await http1_request(
            port=server_port(server),
            request=b'GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n',
        )

    assert captured_extensions
    assert 'tls' not in captured_extensions[0]


async def test_tls_extension_reaches_an_http2_request(tmp_path: Path) -> None:
    """The extension is a property of the connection, not of HTTP/1."""
    certfile, keyfile = write_self_signed_cert(tmp_path)
    captured: list[TLSExtension] = []

    async def app(scope, receive, send):
        assert scope['type'] == 'http'
        captured.append(scope['extensions']['tls'])
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'h2'})

    config = Config(port=0, certfile=certfile, keyfile=keyfile)
    context = client_context(certfile, alpn=['h2'])
    async with running_server(app, config) as server:
        status, body = await tls_h2_request(server_port(server), context)

    assert (status, body) == (200, b'h2')
    assert captured
    tls = captured[0]
    assert tls['tls_version'] == 0x0304
    assert tls['server_cert'] is not None
    assert tls['server_cert'].startswith('-----BEGIN CERTIFICATE-----')


async def test_tls_extension_is_built_once_per_connection(tmp_path: Path) -> None:
    """
    Every request on a connection reports the same handshake, so it is the
    same object — which is also why the enclosing `extensions` stays
    per-request, since that is the dict applications write to.
    """
    certfile, keyfile = write_self_signed_cert(tmp_path)
    seen: list[int] = []

    async def app(scope, receive, send):
        seen.append(id(scope['extensions']['tls']))
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'ok'})

    config = Config(port=0, certfile=certfile, keyfile=keyfile)
    context = client_context(certfile, alpn=['http/1.1'])
    async with running_server(app, config) as server:
        port = server_port(server)
        reader, writer = await open_prefixed_tls_connection(port, context)
        try:
            for _ in range(3):
                writer.write(b'GET / HTTP/1.1\r\nHost: x\r\n\r\n')
                await writer.drain()
                await reader.readuntil(b'ok')
        finally:
            writer.close()
            with suppress(OSError, ssl.SSLError):
                await writer.wait_closed()
        # A second connection is a second handshake, and gets its own.
        await tls_http1_request(port, context)

    assert len(seen) == 4
    assert len(set(seen[:3])) == 1
    assert seen[3] not in seen[:3]


async def test_tls_extension_reaches_a_websocket_scope(tmp_path: Path) -> None:
    """
    A WebSocket scope is built by its own path, so it needs its own check —
    and a connection-authenticated client is exactly the case where a
    WebSocket handler wants to know who connected.
    """
    certfile, keyfile = write_self_signed_cert(tmp_path)
    captured: list[TLSExtension] = []

    async def app(scope, receive, send):
        assert scope['type'] == 'websocket'
        captured.append(scope['extensions']['tls'])
        assert await receive() == {'type': 'websocket.connect'}
        await send({'type': 'websocket.close'})

    config = Config(port=0, certfile=certfile, keyfile=keyfile)
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
        await read_http1_response(reader)
        writer.close()
        with suppress(ConnectionResetError, ConnectionAbortedError, ssl.SSLError):
            await writer.wait_closed()

    assert captured
    tls = captured[0]
    assert tls['tls_version'] == 0x0304
    assert tls['server_cert'] is not None
    assert tls['server_cert'].startswith('-----BEGIN CERTIFICATE-----')
    assert tls['client_cert_chain'] == ()


async def test_prepare_tls_rejects_empty_and_malformed_pem(tmp_path: Path) -> None:
    """``prepare_tls`` is the same ingress ``--check-config`` uses for PEM bytes."""
    from h2corn._lib import prepare_tls
    from h2corn._server import TlsMaterial

    ok_dir = tmp_path / 'ok'
    ok_dir.mkdir()
    certfile, keyfile = write_self_signed_cert(ok_dir)
    good_cert = certfile.read_bytes()
    good_key = keyfile.read_bytes()
    junk = b'not-a-pem'

    with pytest.raises((ValueError, OSError), match=r'certfile|no certificates'):
        prepare_tls(
            Config(certfile=certfile, keyfile=keyfile),
            TlsMaterial(certificate=b'', private_key=good_key, client_ca=None),
        )

    with pytest.raises(
        (ValueError, OSError), match=r'certfile|could not be parsed|no certificates'
    ):
        prepare_tls(
            Config(certfile=certfile, keyfile=keyfile),
            TlsMaterial(certificate=junk, private_key=good_key, client_ca=None),
        )
    with pytest.raises((ValueError, OSError), match='keyfile'):
        prepare_tls(
            Config(certfile=certfile, keyfile=keyfile),
            TlsMaterial(certificate=good_cert, private_key=b'', client_ca=None),
        )
    with pytest.raises((ValueError, OSError), match='keyfile'):
        prepare_tls(
            Config(certfile=certfile, keyfile=keyfile),
            TlsMaterial(certificate=good_cert, private_key=junk, client_ca=None),
        )
    ca_path = tmp_path / 'ca.pem'
    ca_path.write_bytes(b'')
    with pytest.raises((ValueError, OSError), match='ca_certs'):
        prepare_tls(
            Config(
                certfile=certfile,
                keyfile=keyfile,
                ca_certs=ca_path,
                cert_reqs='required',
            ),
            TlsMaterial(
                certificate=good_cert,
                private_key=good_key,
                client_ca=b'',
            ),
        )


async def test_check_config_validates_tls_in_subprocess(tmp_path: Path) -> None:
    """``--check-config`` must enter the same PEM preparation path as serving."""
    valid_dir = tmp_path / 'valid'
    mismatched_dir = tmp_path / 'mismatched'
    valid_dir.mkdir()
    mismatched_dir.mkdir()
    certfile, keyfile = write_self_signed_cert(valid_dir)
    _other_cert, other_key = write_self_signed_cert(mismatched_dir)
    empty = tmp_path / 'empty.pem'
    malformed = tmp_path / 'malformed.pem'
    empty.write_bytes(b'')
    malformed.write_bytes(b'not a PEM')

    def check(*args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, '-m', 'h2corn', '--check-config', *args],
            check=False,
            capture_output=True,
            text=True,
        )

    valid = check('--certfile', str(certfile), '--keyfile', str(keyfile))
    assert valid.returncode == 0, valid.stderr

    invalid_commands = [
        ('--certfile', str(tmp_path / 'missing.crt'), '--keyfile', str(keyfile)),
        ('--certfile', str(empty), '--keyfile', str(keyfile)),
        ('--certfile', str(malformed), '--keyfile', str(keyfile)),
        ('--certfile', str(certfile), '--keyfile', str(tmp_path / 'missing.key')),
        ('--certfile', str(certfile), '--keyfile', str(empty)),
        ('--certfile', str(certfile), '--keyfile', str(malformed)),
        ('--certfile', str(certfile), '--keyfile', str(other_key)),
        (
            '--certfile',
            str(certfile),
            '--keyfile',
            str(keyfile),
            '--ca-certs',
            str(tmp_path / 'missing-ca.pem'),
            '--cert-reqs',
            'required',
        ),
        (
            '--certfile',
            str(certfile),
            '--keyfile',
            str(keyfile),
            '--ca-certs',
            str(empty),
            '--cert-reqs',
            'required',
        ),
    ]
    for args in invalid_commands:
        result = check(*args)
        assert result.returncode != 0, (args, result.stdout, result.stderr)


async def test_prepared_tls_survives_source_file_removal(tmp_path: Path) -> None:
    """Workers reuse the prepared acceptor; PEM paths need not remain readable."""
    certfile, keyfile = write_self_signed_cert(tmp_path)

    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'after-unlink'})

    config = Config(port=0, certfile=certfile, keyfile=keyfile)
    ca_bytes = certfile.read_bytes()
    async with running_server(app, config) as server:
        certfile.unlink()
        keyfile.unlink()
        trust = tmp_path / 'trust.crt'
        trust.write_bytes(ca_bytes)
        body = await tls_http1_request(
            server_port(server),
            client_context(trust, alpn=['http/1.1']),
        )
    assert body == b'after-unlink'


@pytest.mark.skipif(sys.platform == 'win32', reason='POSIX worker supervisor')
async def test_supervisor_replacement_uses_inherited_tls_material(
    tmp_path: Path,
) -> None:
    """A replacement handshakes after all PEM source paths have vanished."""
    ca_cert, server_cert, server_key, client_cert, client_key = write_mutual_tls_certs(
        tmp_path
    )
    release = tmp_path / 'release-first-request'
    first_entered = tmp_path / 'first-request-entered'
    module = tmp_path / 'tls_replacement_app.py'
    module.write_text(
        textwrap.dedent(
            f"""
            import asyncio
            import os
            from pathlib import Path

            release = Path({os.fspath(release)!r})
            first_entered = Path({os.fspath(first_entered)!r})

            async def app(scope, receive, send):
                if not release.exists():
                    first_entered.write_text('ready')
                    while not release.exists():
                        await asyncio.sleep(0.01)
                await send({{'type': 'http.response.start', 'status': 200, 'headers': []}})
                await send({{'type': 'http.response.body', 'body': str(os.getpid()).encode()}})
            """
        ).strip()
        + '\n'
    )
    port = find_free_port()
    env = os.environ.copy()
    env['PYTHONPATH'] = (
        f'{tmp_path}:{env["PYTHONPATH"]}' if 'PYTHONPATH' in env else str(tmp_path)
    )
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        '-m',
        'h2corn._server',
        'tls_replacement_app:app',
        '--workers',
        '1',
        '--port',
        str(port),
        '--lifespan',
        'off',
        '--max-requests',
        '1',
        '--certfile',
        str(server_cert),
        '--keyfile',
        str(server_key),
        '--ca-certs',
        str(ca_cert),
        '--cert-reqs',
        'required',
        env=env,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
        start_new_session=True,
    )
    context = client_context(ca_cert, certfile=client_cert, keyfile=client_key)
    try:
        deadline = asyncio.get_running_loop().time() + 10
        await wait_for_port(port, timeout=10)
        first = asyncio.create_task(tls_http1_request(port, context))
        while not first_entered.exists():
            assert process.returncode is None, (
                'supervisor exited before first handshake'
            )
            assert asyncio.get_running_loop().time() < deadline
            if first.done():
                await first
            await asyncio.sleep(0.01)
        server_cert.unlink()
        server_key.unlink()
        ca_cert.unlink()
        release.write_text('go')
        first_pid = await asyncio.wait_for(first, timeout=5)

        while True:
            assert asyncio.get_running_loop().time() < deadline
            try:
                replacement_pid = await tls_http1_request(port, context)
            except (ConnectionError, OSError, ssl.SSLError, TimeoutError):
                await asyncio.sleep(0.02)
                continue
            if replacement_pid != first_pid:
                break
            await asyncio.sleep(0.02)
        assert process.returncode is None
    finally:
        if process.returncode is None:
            os.killpg(process.pid, signal.SIGTERM)
        with suppress(TimeoutError):
            await asyncio.wait_for(process.wait(), timeout=5)
        if process.returncode is None:
            os.killpg(process.pid, signal.SIGKILL)
            await asyncio.wait_for(process.wait(), timeout=5)


async def test_client_cert_name_matches_leaf_rfc4514(tmp_path: Path) -> None:
    """Nonempty client chain yields an exact RFC 4514 leaf name matching cryptography."""
    ca_cert, server_cert, server_key, client_cert, client_key = write_mutual_tls_certs(
        tmp_path
    )
    captured: list[TLSExtension] = []

    async def app(scope, receive, send):
        captured.append(scope['extensions']['tls'])
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'ok'})

    config = Config(
        port=0,
        certfile=server_cert,
        keyfile=server_key,
        ca_certs=ca_cert,
        cert_reqs='required',
    )
    context = client_context(ca_cert, certfile=client_cert, keyfile=client_key)
    async with running_server(app, config) as server:
        assert await tls_http1_request(server_port(server), context) == b'ok'

    leaf = x509.load_pem_x509_certificate(client_cert.read_bytes())
    assert captured
    tls = captured[0]
    assert tls['client_cert_name'] == leaf.subject.rfc4514_string()


def _rfc4514_subjects():
    from cryptography.x509.oid import NameOID

    return [
        pytest.param(
            x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, 'simple')]),
            id='simple',
        ),
        pytest.param(
            x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, 'has,comma')]),
            id='comma',
        ),
        pytest.param(
            x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, ' lead space')]),
            id='leading-space',
        ),
        pytest.param(
            x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, 'trail space ')]),
            id='trailing-space',
        ),
        pytest.param(
            x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, 'quote"slash\\')]),
            id='escaping',
        ),
        pytest.param(
            x509.Name([
                x509.NameAttribute(NameOID.COUNTRY_NAME, 'US'),
                x509.NameAttribute(NameOID.ORGANIZATION_NAME, 'Acme'),
                x509.NameAttribute(NameOID.COMMON_NAME, 'multi'),
            ]),
            id='multi-attribute',
        ),
        pytest.param(
            x509.Name([
                x509.RelativeDistinguishedName([
                    x509.NameAttribute(NameOID.ORGANIZATIONAL_UNIT_NAME, 'Sales'),
                    x509.NameAttribute(NameOID.COMMON_NAME, 'Bob'),
                ])
            ]),
            id='multivalued-rdn',
        ),
        pytest.param(
            x509.Name([
                x509.NameAttribute(x509.ObjectIdentifier('1.2.3.4.5'), 'unknown')
            ]),
            id='unknown-oid',
            # RFC 4514 §2.4 requires `#` + hex DER whenever the AttributeType
            # is rendered in dotted-decimal form, so an unknown OID is
            # asserted against the RFC directly rather than against
            # cryptography, which prints the string form instead.
        ),
    ]


@pytest.mark.parametrize('subject', _rfc4514_subjects())
async def test_rfc4514_client_name_matches_cryptography(
    tmp_path: Path,
    subject: x509.Name,
    request: pytest.FixtureRequest,
) -> None:
    """DN rendering matches cryptography.x509.Name.rfc4514_string() for handshake subjects."""
    import datetime

    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import NameOID

    ca_key = ec.generate_private_key(ec.SECP256R1())
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, 'test-ca')])
    now = datetime.datetime.now(datetime.UTC)
    ca_cert = (
        x509
        .CertificateBuilder()
        .subject_name(ca_name)
        .issuer_name(ca_name)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now)
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(ca_key, hashes.SHA256())
    )
    client_key = ec.generate_private_key(ec.SECP256R1())
    client_cert = (
        x509
        .CertificateBuilder()
        .subject_name(subject)
        .issuer_name(ca_name)
        .public_key(client_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now)
        .not_valid_after(now + datetime.timedelta(days=1))
        .sign(ca_key, hashes.SHA256())
    )
    srv_dir = tmp_path / 'srv'
    srv_dir.mkdir()
    srv_cert, srv_key = write_self_signed_cert(srv_dir)
    ca_path = tmp_path / 'ca.crt'
    ca_path.write_bytes(ca_cert.public_bytes(serialization.Encoding.PEM))
    c_cert = tmp_path / 'client.crt'
    c_key = tmp_path / 'client.key'
    c_cert.write_bytes(client_cert.public_bytes(serialization.Encoding.PEM))
    c_key.write_bytes(
        client_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        )
    )
    captured: list[TLSExtension] = []

    async def app(scope, receive, send):
        captured.append(scope['extensions']['tls'])
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'n'})

    config = Config(
        port=0,
        certfile=srv_cert,
        keyfile=srv_key,
        ca_certs=ca_path,
        cert_reqs='required',
    )
    ctx = client_context(srv_cert, certfile=c_cert, keyfile=c_key)
    async with running_server(app, config) as server:
        assert await tls_http1_request(server_port(server), ctx) == b'n'

    # Prefer the leaf as rustls saw it (may reorder multi-valued RDN SETs).
    leaf = x509.load_pem_x509_certificate(c_cert.read_bytes())
    expected = leaf.subject.rfc4514_string()
    if 'unknown-oid' in request.node.callspec.id:
        # The value is the BER encoding of a UTF8String "unknown", hex-encoded.
        assert (
            captured
            and captured[0]['client_cert_name'] == '1.2.3.4.5=#0c07756e6b6e6f776e'
        )
    else:
        assert captured and captured[0]['client_cert_name'] == expected


async def test_negotiated_cipher_suite_matches_scope(tmp_path: Path) -> None:
    certfile, keyfile = write_self_signed_cert(tmp_path)
    captured: list[TLSExtension] = []

    async def app(scope, receive, send):
        captured.append(scope['extensions']['tls'])
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'v'})

    config = Config(port=0, certfile=certfile, keyfile=keyfile)

    for maximum, expected in (
        (ssl.TLSVersion.TLSv1_2, 0x0303),
        (ssl.TLSVersion.TLSv1_3, 0x0304),
    ):
        captured.clear()
        context = client_context(certfile, alpn=['http/1.1'])
        context.maximum_version = maximum
        context.minimum_version = maximum
        negotiated: list[_NegotiatedTLS] = []
        async with running_server(app, config) as server:
            port = server_port(server)
            body = await tls_http1_request(port, context, negotiated=negotiated)
            assert body == b'v'

        # Cross-checked against the client's own view rather than a bare
        # constant: if these ever disagree the failure says which side is
        # wrong, instead of leaving two numbers to guess between.
        assert negotiated
        client = negotiated[0]
        client_version = {'TLSv1.2': 0x0303, 'TLSv1.3': 0x0304}[client['version']]
        assert captured
        tls = captured[0]
        context_summary = (
            f'pinned={maximum!r} port={port} client={client!r} server_extension={tls!r}'
        )
        assert client_version == expected, (
            f'the client did not negotiate the pinned version; {context_summary}'
        )
        assert tls['tls_version'] == client_version, (
            f'h2corn reported a different version than the client '
            f'negotiated; {context_summary}'
        )
        client_cipher = client['cipher']
        cipher_id = next(
            entry['id']
            for entry in context.get_ciphers()
            if entry['name'] == client_cipher[0]
        )
        assert tls['cipher_suite'] == cipher_id & 0xFFFF, context_summary


async def test_http1_plaintext_has_http_scheme_and_no_tls_extension() -> None:
    captured_scheme: list[str] = []
    captured_extensions: list[dict[str, object]] = []

    async def app(scope, receive, send):
        captured_scheme.append(scope['scheme'])
        captured_extensions.append(dict(scope['extensions']))
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b''})

    async with running_server(app, Config(port=0)) as server:
        await http1_request(
            port=server_port(server),
            request=b'GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n',
        )
    assert captured_scheme == ['http']
    assert captured_extensions
    assert 'tls' not in captured_extensions[0]


async def test_http1_tls_has_https_scheme_and_tls_extension(tmp_path: Path) -> None:
    certfile, keyfile = write_self_signed_cert(tmp_path)
    captured: dict[str, object] = {}

    async def app(scope, receive, send):
        captured['scheme'] = scope['scheme']
        captured['has_tls'] = 'tls' in scope['extensions']
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b''})

    config = Config(port=0, certfile=certfile, keyfile=keyfile)
    context = client_context(certfile, alpn=['http/1.1'])
    async with running_server(app, config) as server:
        await tls_http1_request(server_port(server), context)
    assert captured == {'scheme': 'https', 'has_tls': True}


async def test_http2_plaintext_preserves_scheme_without_tls_extension() -> None:
    from tests._support import h2_request

    captured_scheme: list[str] = []
    captured_extensions: list[dict[str, object]] = []

    async def app(scope, receive, send):
        captured_scheme.append(scope['scheme'])
        captured_extensions.append(dict(scope['extensions']))
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b''})

    async with running_server(app, Config(port=0)) as server:
        await h2_request(port=server_port(server))
    assert captured_scheme == ['http']
    assert captured_extensions
    assert 'tls' not in captured_extensions[0]


async def test_http2_tls_forces_https_and_has_tls_extension(tmp_path: Path) -> None:
    certfile, keyfile = write_self_signed_cert(tmp_path)
    captured: dict[str, object] = {}

    async def app(scope, receive, send):
        captured['scheme'] = scope['scheme']
        captured['has_tls'] = 'tls' in scope['extensions']
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b''})

    config = Config(port=0, certfile=certfile, keyfile=keyfile)
    context = client_context(certfile, alpn=['h2'])
    async with running_server(app, config) as server:
        status, _body = await tls_h2_request(
            server_port(server), context, scheme=b'http'
        )
    assert status == 200
    assert captured == {'scheme': 'https', 'has_tls': True}
