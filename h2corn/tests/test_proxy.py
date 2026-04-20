import asyncio
import socket

import pytest
from h2corn import Config

from tests._support import find_free_port, h2_request, running_server

pytestmark = pytest.mark.asyncio


def _proxy_v1_prefix(
    *,
    transport: str = 'TCP4',
    client_host: str,
    server_host: str,
    client_port: int,
    server_port: int,
) -> bytes:
    return f'PROXY {transport} {client_host} {server_host} {client_port} {server_port}\r\n'.encode()


def _proxy_v2_prefix(
    *,
    client_host: str,
    server_host: str,
    client_port: int,
    server_port: int,
    tlvs: bytes = b'',
) -> bytes:
    signature = b'\r\n\r\n\x00\r\nQUIT\n'
    version_command = bytes([0x21])
    family_protocol = bytes([0x11])
    payload = (
        socket.inet_aton(client_host)
        + socket.inet_aton(server_host)
        + client_port.to_bytes(2, 'big')
        + server_port.to_bytes(2, 'big')
        + tlvs
    )
    return (
        signature
        + version_command
        + family_protocol
        + len(payload).to_bytes(2, 'big')
        + payload
    )


async def test_proxy_headers_rewrite_scope_from_trusted_peer() -> None:
    async def app(scope, receive, send):
        payload = (
            f'{scope["scheme"]}|{scope["client"][0]}|{scope["server"][0]}|'
            f'{scope["server"][1]}|{scope.get("root_path", "")}'
        ).encode()
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [(b'content-type', b'text/plain')],
        })
        await send({'type': 'http.response.body', 'body': payload})

    config = Config(
        port=find_free_port(),
        proxy_headers=True,
        forwarded_allow_ips=('127.0.0.1',),
    )
    async with running_server(app, config):
        status, body = await asyncio.wait_for(
            h2_request(
                port=config.port,
                extra_headers=[
                    (
                        b'forwarded',
                        b'for=203.0.113.10;proto=https;host=example.com:8443',
                    ),
                    (b'x-forwarded-prefix', b'/api'),
                ],
            ),
            timeout=5,
        )

    assert status == 200
    assert body == b'https|203.0.113.10|example.com|8443|/api'


async def test_proxy_headers_are_ignored_from_untrusted_peer() -> None:
    async def app(scope, receive, send):
        payload = (
            f'{scope["scheme"]}|{scope["client"][0]}|{scope["server"][0]}|'
            f'{scope["server"][1]}|{scope.get("root_path", "")}'
        ).encode()
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [(b'content-type', b'text/plain')],
        })
        await send({'type': 'http.response.body', 'body': payload})

    config = Config(
        port=find_free_port(),
        proxy_headers=True,
        forwarded_allow_ips=('10.0.0.0/8',),
    )
    async with running_server(app, config):
        status, body = await asyncio.wait_for(
            h2_request(
                port=config.port,
                extra_headers=[
                    (
                        b'forwarded',
                        b'for=203.0.113.10;proto=https;host=example.com:8443',
                    ),
                    (b'x-forwarded-prefix', b'/api'),
                ],
            ),
            timeout=5,
        )

    assert status == 200
    assert body == f'http|127.0.0.1|127.0.0.1|{config.port}|'.encode()


async def test_proxy_headers_infer_default_port_from_forwarded_scheme() -> None:
    async def app(scope, receive, send):
        payload = (
            f'{scope["scheme"]}|{scope["server"][0]}|{scope["server"][1]}'.encode()
        )
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [(b'content-type', b'text/plain')],
        })
        await send({'type': 'http.response.body', 'body': payload})

    config = Config(
        port=find_free_port(),
        proxy_headers=True,
        forwarded_allow_ips=('127.0.0.1',),
    )
    async with running_server(app, config):
        status, body = await asyncio.wait_for(
            h2_request(
                port=config.port,
                extra_headers=[
                    (b'x-forwarded-proto', b'https'),
                    (b'x-forwarded-host', b'example.com'),
                ],
            ),
            timeout=5,
        )

    assert status == 200
    assert body == b'https|example.com|443'


@pytest.mark.parametrize(
    ('root_path', 'prefix', 'expected'),
    [
        ('', '/api', '/api'),
        ('/', '/api', '/api'),
        ('/root', '/api', '/api/root'),
        ('/root', '/api/', '/api/root'),
        ('/root', '/', '/root'),
    ],
)
async def test_proxy_headers_join_forwarded_prefix_and_root_path(
    root_path: str,
    prefix: str,
    expected: str,
) -> None:
    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({
            'type': 'http.response.body',
            'body': scope.get('root_path', '').encode(),
        })

    config = Config(
        port=find_free_port(),
        root_path=root_path,
        proxy_headers=True,
        forwarded_allow_ips=('127.0.0.1',),
    )
    async with running_server(app, config):
        status, body = await asyncio.wait_for(
            h2_request(
                port=config.port,
                extra_headers=[(b'x-forwarded-prefix', prefix.encode())],
            ),
            timeout=5,
        )

    assert status == 200
    assert body == expected.encode()


async def test_proxy_headers_support_bracketed_ipv6_forwarded_values() -> None:
    async def app(scope, receive, send):
        payload = (
            f'{scope["scheme"]}|{scope["client"][0]}|{scope["server"][0]}|'
            f'{scope["server"][1]}'
        ).encode()
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [(b'content-type', b'text/plain')],
        })
        await send({'type': 'http.response.body', 'body': payload})

    config = Config(
        port=find_free_port(),
        proxy_headers=True,
        forwarded_allow_ips=('127.0.0.1',),
    )
    async with running_server(app, config):
        status, body = await asyncio.wait_for(
            h2_request(
                port=config.port,
                extra_headers=[
                    (
                        b'forwarded',
                        b'for="[2001:db8::1]:1234";proto=https;host="[2001:db8::2]:8443"',
                    ),
                ],
            ),
            timeout=5,
        )

    assert status == 200
    assert body == b'https|2001:db8::1|2001:db8::2|8443'


async def test_proxy_headers_support_mixed_case_forwarded_parameters() -> None:
    async def app(scope, receive, send):
        payload = (
            f'{scope["scheme"]}|{scope["client"][0]}|{scope["server"][0]}|'
            f'{scope["server"][1]}'
        ).encode()
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [(b'content-type', b'text/plain')],
        })
        await send({'type': 'http.response.body', 'body': payload})

    config = Config(
        port=find_free_port(),
        proxy_headers=True,
        forwarded_allow_ips=('127.0.0.1',),
    )
    async with running_server(app, config):
        status, body = await asyncio.wait_for(
            h2_request(
                port=config.port,
                extra_headers=[
                    (
                        b'forwarded',
                        b'For="[2001:db8::1]:1234";Proto=HTTPS;Host=example.com:8443',
                    ),
                ],
            ),
            timeout=5,
        )

    assert status == 200
    assert body == b'https|2001:db8::1|example.com|8443'


async def test_proxy_headers_use_backend_facing_forwarded_hop() -> None:
    async def app(scope, receive, send):
        payload = (
            f'{scope["scheme"]}|{scope["client"][0]}|{scope["server"][0]}|'
            f'{scope["server"][1]}'
        ).encode()
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [(b'content-type', b'text/plain')],
        })
        await send({'type': 'http.response.body', 'body': payload})

    config = Config(
        port=find_free_port(),
        proxy_headers=True,
        forwarded_allow_ips=('127.0.0.1',),
    )
    async with running_server(app, config):
        status, body = await asyncio.wait_for(
            h2_request(
                port=config.port,
                extra_headers=[
                    (
                        b'forwarded',
                        b'for=203.0.113.10;proto=http;host=attacker.example, '
                        b'for=198.51.100.7;proto=https;host=example.com:8443',
                    ),
                ],
            ),
            timeout=5,
        )

    assert status == 200
    assert body == b'https|198.51.100.7|example.com|8443'


async def test_proxy_headers_use_last_forwarded_header_field() -> None:
    async def app(scope, receive, send):
        payload = (
            f'{scope["scheme"]}|{scope["client"][0]}|{scope["server"][0]}|'
            f'{scope["server"][1]}'
        ).encode()
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [(b'content-type', b'text/plain')],
        })
        await send({'type': 'http.response.body', 'body': payload})

    config = Config(
        port=find_free_port(),
        proxy_headers=True,
        forwarded_allow_ips=('127.0.0.1',),
    )
    async with running_server(app, config):
        status, body = await asyncio.wait_for(
            h2_request(
                port=config.port,
                extra_headers=[
                    (
                        b'forwarded',
                        b'for=203.0.113.10;proto=http;host=attacker.example',
                    ),
                    (
                        b'forwarded',
                        b'for=198.51.100.7;proto=https;host=example.com:8443',
                    ),
                ],
            ),
            timeout=5,
        )

    assert status == 200
    assert body == b'https|198.51.100.7|example.com|8443'


async def test_proxy_headers_walk_x_forwarded_for_from_backend_facing_end() -> None:
    async def app(scope, receive, send):
        payload = (
            f'{scope["scheme"]}|{scope["client"][0]}|{scope["server"][0]}|'
            f'{scope["server"][1]}'
        ).encode()
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [(b'content-type', b'text/plain')],
        })
        await send({'type': 'http.response.body', 'body': payload})

    config = Config(
        port=find_free_port(),
        proxy_headers=True,
        forwarded_allow_ips=('127.0.0.1',),
    )
    async with running_server(app, config):
        status, body = await asyncio.wait_for(
            h2_request(
                port=config.port,
                extra_headers=[
                    (b'x-forwarded-for', b'203.0.113.10, 198.51.100.7'),
                    (b'x-forwarded-proto', b'https'),
                    (b'x-forwarded-host', b'example.com:8443'),
                ],
            ),
            timeout=5,
        )

    assert status == 200
    assert body == b'https|198.51.100.7|example.com|8443'


async def test_proxy_headers_use_backend_facing_proto_and_host_tokens() -> None:
    async def app(scope, receive, send):
        payload = (
            f'{scope["scheme"]}|{scope["server"][0]}|{scope["server"][1]}'.encode()
        )
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [(b'content-type', b'text/plain')],
        })
        await send({'type': 'http.response.body', 'body': payload})

    config = Config(
        port=find_free_port(),
        proxy_headers=True,
        forwarded_allow_ips=('127.0.0.1',),
    )
    async with running_server(app, config):
        status, body = await asyncio.wait_for(
            h2_request(
                port=config.port,
                extra_headers=[
                    (b'x-forwarded-proto', b'http, https'),
                    (b'x-forwarded-host', b'attacker.example, example.com:8443'),
                ],
            ),
            timeout=5,
        )

    assert status == 200
    assert body == b'https|example.com|8443'


async def test_proxy_headers_use_backend_facing_port_and_prefix_tokens() -> None:
    async def app(scope, receive, send):
        payload = (
            f'{scope["scheme"]}|{scope["server"][0]}|{scope["server"][1]}|'
            f'{scope.get("root_path", "")}'
        ).encode()
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [(b'content-type', b'text/plain')],
        })
        await send({'type': 'http.response.body', 'body': payload})

    config = Config(
        port=find_free_port(),
        root_path='/root',
        proxy_headers=True,
        forwarded_allow_ips=('127.0.0.1',),
    )
    async with running_server(app, config):
        status, body = await asyncio.wait_for(
            h2_request(
                port=config.port,
                extra_headers=[
                    (b'x-forwarded-proto', b'http, https'),
                    (b'x-forwarded-host', b'attacker.example, example.com'),
                    (b'x-forwarded-port', b'8080, 9443'),
                    (b'x-forwarded-prefix', b'/ignored, /api'),
                ],
            ),
            timeout=5,
        )

    assert status == 200
    assert body == b'https|example.com|9443|/api/root'


async def test_http1_requires_proxy_header_when_proxy_protocol_is_configured() -> None:
    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'optional proxy'})

    config = Config(
        port=find_free_port(),
        forwarded_allow_ips=('127.0.0.1',),
        proxy_protocol='v1',
    )
    async with running_server(app, config):
        reader, writer = await asyncio.open_connection('127.0.0.1', config.port)
        writer.write(
            f'GET / HTTP/1.1\r\nHost: 127.0.0.1:{config.port}\r\n\r\n'.encode()
        )
        await writer.drain()
        data = await asyncio.wait_for(reader.read(1024), timeout=5)
        writer.close()
        await writer.wait_closed()

    assert data == b''


async def test_proxy_protocol_v1_rewrites_scope_from_trusted_peer() -> None:
    async def app(scope, receive, send):
        payload = (
            f'{scope["client"][0]}|{scope["client"][1]}|'
            f'{scope["server"][0]}|{scope["server"][1]}'
        ).encode()
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [(b'content-type', b'text/plain')],
        })
        await send({'type': 'http.response.body', 'body': payload})

    config = Config(
        port=find_free_port(),
        forwarded_allow_ips=('127.0.0.1',),
        proxy_protocol='v1',
    )
    async with running_server(app, config):
        status, body = await asyncio.wait_for(
            h2_request(
                port=config.port,
                prefix=_proxy_v1_prefix(
                    client_host='203.0.113.10',
                    server_host='198.51.100.20',
                    client_port=41234,
                    server_port=8080,
                ),
            ),
            timeout=5,
        )

    assert status == 200
    assert body == b'203.0.113.10|41234|198.51.100.20|8080'


async def test_proxy_protocol_v1_rejects_overlong_header_line() -> None:
    async def app(scope, receive, send):
        raise AssertionError('overlong proxy header should fail before request dispatch')

    config = Config(
        port=find_free_port(),
        forwarded_allow_ips=('127.0.0.1',),
        proxy_protocol='v1',
    )
    async with running_server(app, config):
        with pytest.raises((
            ConnectionResetError,
            BrokenPipeError,
            RuntimeError,
            OSError,
        )):
            await asyncio.wait_for(
                h2_request(
                    port=config.port,
                    prefix=b'PROXY UNKNOWN ' + (b'x' * 128) + b'\r\n',
                ),
                timeout=5,
            )


async def test_proxy_protocol_v2_and_forwarded_headers_stack_cleanly() -> None:
    async def app(scope, receive, send):
        payload = (
            f'{scope["scheme"]}|{scope["client"][0]}|{scope["client"][1]}|'
            f'{scope["server"][0]}|{scope["server"][1]}|{scope.get("root_path", "")}'
        ).encode()
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [(b'content-type', b'text/plain')],
        })
        await send({'type': 'http.response.body', 'body': payload})

    config = Config(
        port=find_free_port(),
        proxy_headers=True,
        forwarded_allow_ips=('127.0.0.1/32',),
        proxy_protocol='v2',
    )
    async with running_server(app, config):
        status, body = await asyncio.wait_for(
            h2_request(
                port=config.port,
                prefix=_proxy_v2_prefix(
                    client_host='203.0.113.10',
                    server_host='198.51.100.20',
                    client_port=41234,
                    server_port=8080,
                ),
                extra_headers=[
                    (b'forwarded', b'proto=https;host=example.com:8443'),
                    (b'x-forwarded-prefix', b'/api'),
                ],
            ),
            timeout=5,
        )

    assert status == 200
    assert body == b'https|203.0.113.10|41234|example.com|8443|/api'


async def test_proxy_protocol_v2_zero_destination_keeps_bind_server_tuple() -> None:
    async def app(scope, receive, send):
        payload = (
            f'{scope["client"][0]}|{scope["client"][1]}|'
            f'{scope["server"][0]}|{scope["server"][1]}'
        ).encode()
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [(b'content-type', b'text/plain')],
        })
        await send({'type': 'http.response.body', 'body': payload})

    config = Config(
        port=find_free_port(),
        forwarded_allow_ips=('127.0.0.1',),
        proxy_protocol='v2',
    )
    async with running_server(app, config):
        status, body = await asyncio.wait_for(
            h2_request(
                port=config.port,
                prefix=_proxy_v2_prefix(
                    client_host='203.0.113.10',
                    server_host='0.0.0.0',  # noqa: S104 - intentional wildcard destination tuple
                    client_port=0,
                    server_port=0,
                ),
            ),
            timeout=5,
        )

    assert status == 200
    assert body == f'203.0.113.10|0|127.0.0.1|{config.port}'.encode()


async def test_proxy_protocol_v2_zero_destination_uses_actual_multi_bind_listener() -> None:
    async def app(scope, receive, send):
        payload = (
            f'{scope["client"][0]}|{scope["client"][1]}|'
            f'{scope["server"][0]}|{scope["server"][1]}'
        ).encode()
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [(b'content-type', b'text/plain')],
        })
        await send({'type': 'http.response.body', 'body': payload})

    ports = (find_free_port(), find_free_port())
    config = Config(
        bind=tuple(f'127.0.0.1:{port}' for port in ports),
        forwarded_allow_ips=('127.0.0.1',),
        proxy_protocol='v2',
    )
    async with running_server(app, config):
        status, body = await asyncio.wait_for(
            h2_request(
                port=ports[1],
                prefix=_proxy_v2_prefix(
                    client_host='203.0.113.10',
                    server_host='0.0.0.0',  # noqa: S104 - intentional wildcard destination tuple
                    client_port=0,
                    server_port=0,
                ),
            ),
            timeout=5,
        )

    assert status == 200
    assert body == f'203.0.113.10|0|127.0.0.1|{ports[1]}'.encode()


async def test_proxy_protocol_v2_ignores_trailing_tlvs() -> None:
    async def app(scope, receive, send):
        payload = (
            f'{scope["client"][0]}|{scope["client"][1]}|'
            f'{scope["server"][0]}|{scope["server"][1]}'
        ).encode()
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [(b'content-type', b'text/plain')],
        })
        await send({'type': 'http.response.body', 'body': payload})

    config = Config(
        port=find_free_port(),
        forwarded_allow_ips=('127.0.0.1',),
        proxy_protocol='v2',
    )
    async with running_server(app, config):
        status, body = await asyncio.wait_for(
            h2_request(
                port=config.port,
                prefix=_proxy_v2_prefix(
                    client_host='203.0.113.10',
                    server_host='198.51.100.20',
                    client_port=41234,
                    server_port=8080,
                    tlvs=b'\x01\x00\x03abc',
                ),
            ),
            timeout=5,
        )

    assert status == 200
    assert body == b'203.0.113.10|41234|198.51.100.20|8080'


async def test_proxy_protocol_v1_requires_header_in_strict_mode() -> None:
    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'ok'})

    config = Config(
        port=find_free_port(),
        forwarded_allow_ips=('127.0.0.1',),
        proxy_protocol='v1',
    )
    async with running_server(app, config):
        with pytest.raises((
            ConnectionResetError,
            BrokenPipeError,
            RuntimeError,
            OSError,
        )):
            await asyncio.wait_for(h2_request(port=config.port), timeout=5)


async def test_proxy_protocol_v1_rejects_address_family_mismatch() -> None:
    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'ok'})

    config = Config(
        port=find_free_port(),
        forwarded_allow_ips=('127.0.0.1',),
        proxy_protocol='v1',
    )
    async with running_server(app, config):
        with pytest.raises((
            ConnectionResetError,
            BrokenPipeError,
            RuntimeError,
            OSError,
        )):
            await asyncio.wait_for(
                h2_request(
                    port=config.port,
                    prefix=_proxy_v1_prefix(
                        transport='TCP4',
                        client_host='2001:db8::1',
                        server_host='198.51.100.20',
                        client_port=41234,
                        server_port=8080,
                    ),
                ),
                timeout=5,
            )


async def test_untrusted_proxy_protocol_header_is_rejected() -> None:
    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'ok'})

    config = Config(
        port=find_free_port(),
        forwarded_allow_ips=('10.0.0.0/8',),
        proxy_protocol='v1',
    )
    async with running_server(app, config):
        with pytest.raises((
            ConnectionResetError,
            BrokenPipeError,
            RuntimeError,
            OSError,
        )):
            await asyncio.wait_for(
                h2_request(
                    port=config.port,
                    prefix=_proxy_v1_prefix(
                        client_host='203.0.113.10',
                        server_host='198.51.100.20',
                        client_port=41234,
                        server_port=8080,
                    ),
                ),
                timeout=5,
            )
