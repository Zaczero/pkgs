import asyncio
import socket

import pytest
from h2corn import Config

from tests._support import (
    find_free_port,
    h2_request,
    proxy_v1_prefix,
    proxy_v2_prefix,
    read_http1_response,
    running_server,
    server_port,
)

pytestmark = pytest.mark.asyncio


_PROXY_V2_SIGNATURE = b'\r\n\r\n\x00\r\nQUIT\n'


def _proxy_v2_prefix(
    version_command: int,
    family_transport: int,
    payload: bytes = b'',
) -> bytes:
    return (
        _PROXY_V2_SIGNATURE
        + bytes((version_command, family_transport))
        + len(payload).to_bytes(2, 'big')
        + payload
    )


def _proxy_v2_ipv4_payload(
    client_host: str = '203.0.113.10',
    server_host: str = '198.51.100.20',
    client_port: int = 41234,
    server_port: int = 8080,
) -> bytes:
    return (
        socket.inet_aton(client_host)
        + socket.inet_aton(server_host)
        + client_port.to_bytes(2, 'big')
        + server_port.to_bytes(2, 'big')
    )


def _ipv6_loopback_is_bindable() -> bool:
    if not socket.has_ipv6:
        return False
    try:
        with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as probe:
            probe.bind(('::1', 0))
    except OSError:
        return False
    return True


async def _http1_after_split_proxy_prefix(
    port: int,
    prefix: bytes,
    split_at: int,
) -> tuple[int, dict[bytes, bytes], bytes, list[tuple[bytes, bytes]]]:
    reader, writer = await asyncio.open_connection('127.0.0.1', port)
    try:
        writer.write(prefix[:split_at])
        await writer.drain()
        writer.write(
            prefix[split_at:]
            + b'GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n'
        )
        await writer.drain()
        return await read_http1_response(reader)
    finally:
        writer.close()
        await writer.wait_closed()


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
        port=0,
        proxy_headers=True,
        forwarded_allow_ips=('10.0.0.0/8',),
    )
    async with running_server(app, config) as server:
        port = server_port(server)
        status, body = await asyncio.wait_for(
            h2_request(
                port=port,
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
    assert body == f'http|127.0.0.1|127.0.0.1|{port}|'.encode()


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
        port=0,
        proxy_headers=True,
        forwarded_allow_ips=('127.0.0.1',),
    )
    async with running_server(app, config) as server:
        status, body = await asyncio.wait_for(
            h2_request(
                port=server_port(server),
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
        port=0,
        root_path=root_path,
        proxy_headers=True,
        forwarded_allow_ips=('127.0.0.1',),
    )
    async with running_server(app, config) as server:
        status, body = await asyncio.wait_for(
            h2_request(
                port=server_port(server),
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
        port=0,
        proxy_headers=True,
        forwarded_allow_ips=('127.0.0.1',),
    )
    async with running_server(app, config) as server:
        status, body = await asyncio.wait_for(
            h2_request(
                port=server_port(server),
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
        port=0,
        proxy_headers=True,
        forwarded_allow_ips=('127.0.0.1',),
    )
    async with running_server(app, config) as server:
        status, body = await asyncio.wait_for(
            h2_request(
                port=server_port(server),
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


async def test_proxy_headers_walk_forwarded_for_through_trusted_hops() -> None:
    """`for` names the client, so trusted hops are skipped as in X-Forwarded-For.

    `proto` and `host` describe the hop that handed us the request and stay with
    the last element, so this asserts both halves at once: taking the last
    element's `for` too would report the intermediate proxy as the client.
    """

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
        port=0,
        proxy_headers=True,
        # The edge proxy is trusted too, so the walk has to pass through it.
        forwarded_allow_ips=('127.0.0.1', '198.51.100.7'),
    )
    async with running_server(app, config) as server:
        status, body = await asyncio.wait_for(
            h2_request(
                port=server_port(server),
                extra_headers=[
                    (
                        b'forwarded',
                        # The edge saw the client; the next hop saw the edge.
                        (
                            b'for=203.0.113.10;proto=http;host=attacker.example, '
                            b'for=198.51.100.7;proto=https;host=example.com:8443'
                        ),
                    ),
                ],
            ),
            timeout=5,
        )

    assert status == 200
    assert body == b'https|203.0.113.10|example.com|8443'


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
        port=0,
        proxy_headers=True,
        forwarded_allow_ips=('127.0.0.1',),
    )
    async with running_server(app, config) as server:
        status, body = await asyncio.wait_for(
            h2_request(
                port=server_port(server),
                extra_headers=[
                    (
                        b'forwarded',
                        (
                            b'for=203.0.113.10;proto=http;host=attacker.example, '
                            b'for=198.51.100.7;proto=https;host=example.com:8443'
                        ),
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
        port=0,
        proxy_headers=True,
        forwarded_allow_ips=('127.0.0.1',),
    )
    async with running_server(app, config) as server:
        status, body = await asyncio.wait_for(
            h2_request(
                port=server_port(server),
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
        port=0,
        proxy_headers=True,
        forwarded_allow_ips=('127.0.0.1',),
    )
    async with running_server(app, config) as server:
        status, body = await asyncio.wait_for(
            h2_request(
                port=server_port(server),
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
        port=0,
        proxy_headers=True,
        forwarded_allow_ips=('127.0.0.1',),
    )
    async with running_server(app, config) as server:
        status, body = await asyncio.wait_for(
            h2_request(
                port=server_port(server),
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
        port=0,
        root_path='/root',
        proxy_headers=True,
        forwarded_allow_ips=('127.0.0.1',),
    )
    async with running_server(app, config) as server:
        status, body = await asyncio.wait_for(
            h2_request(
                port=server_port(server),
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
        port=0,
        forwarded_allow_ips=('127.0.0.1',),
        proxy_protocol='v1',
    )
    async with running_server(app, config) as server:
        reader, writer = await asyncio.open_connection('127.0.0.1', server_port(server))
        writer.write(
            f'GET / HTTP/1.1\r\nHost: 127.0.0.1:{server_port(server)}\r\n\r\n'.encode()
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
        port=0,
        forwarded_allow_ips=('127.0.0.1',),
        proxy_protocol='v1',
    )
    async with running_server(app, config) as server:
        status, body = await asyncio.wait_for(
            h2_request(
                port=server_port(server),
                prefix=proxy_v1_prefix(
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
        raise AssertionError(
            'overlong proxy header should fail before request dispatch'
        )

    config = Config(
        port=0,
        forwarded_allow_ips=('127.0.0.1',),
        proxy_protocol='v1',
    )
    async with running_server(app, config) as server:
        with pytest.raises((
            ConnectionResetError,
            BrokenPipeError,
            RuntimeError,
            OSError,
        )):
            await asyncio.wait_for(
                h2_request(
                    port=server_port(server),
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
        port=0,
        proxy_headers=True,
        forwarded_allow_ips=('127.0.0.1/32',),
        proxy_protocol='v2',
    )
    async with running_server(app, config) as server:
        status, body = await asyncio.wait_for(
            h2_request(
                port=server_port(server),
                prefix=proxy_v2_prefix(
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
        port=0,
        forwarded_allow_ips=('127.0.0.1',),
        proxy_protocol='v2',
    )
    async with running_server(app, config) as server:
        port = server_port(server)
        status, body = await asyncio.wait_for(
            h2_request(
                port=port,
                prefix=proxy_v2_prefix(
                    client_host='203.0.113.10',
                    server_host='0.0.0.0',  # noqa: S104 - intentional wildcard destination tuple
                    client_port=0,
                    server_port=0,
                ),
            ),
            timeout=5,
        )

    assert status == 200
    assert body == f'203.0.113.10|0|127.0.0.1|{port}'.encode()


async def test_proxy_protocol_v2_zero_destination_uses_actual_multi_bind_listener() -> (
    None
):
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

    # Two listeners on one host need distinct ports; multiple port-0 binds
    # deliberately share one ephemeral port (for 0.0.0.0 + [::] pairs), so
    # this is one of the few in-process cases that must pre-allocate.
    ports = (find_free_port(), find_free_port())
    config = Config(
        bind=tuple(f'127.0.0.1:{port}' for port in ports),
        forwarded_allow_ips=('127.0.0.1',),
        proxy_protocol='v2',
    )
    async with running_server(app, config):
        second_port = ports[1]
        status, body = await asyncio.wait_for(
            h2_request(
                port=second_port,
                prefix=proxy_v2_prefix(
                    client_host='203.0.113.10',
                    server_host='0.0.0.0',  # noqa: S104 - intentional wildcard destination tuple
                    client_port=0,
                    server_port=0,
                ),
            ),
            timeout=5,
        )

    assert status == 200
    assert body == f'203.0.113.10|0|127.0.0.1|{second_port}'.encode()


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
        port=0,
        forwarded_allow_ips=('127.0.0.1',),
        proxy_protocol='v2',
    )
    async with running_server(app, config) as server:
        status, body = await asyncio.wait_for(
            h2_request(
                port=server_port(server),
                prefix=proxy_v2_prefix(
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
        port=0,
        forwarded_allow_ips=('127.0.0.1',),
        proxy_protocol='v1',
    )
    async with running_server(app, config) as server:
        with pytest.raises((
            ConnectionResetError,
            BrokenPipeError,
            RuntimeError,
            OSError,
        )):
            await asyncio.wait_for(h2_request(port=server_port(server)), timeout=5)


async def test_proxy_protocol_v1_rejects_address_family_mismatch() -> None:
    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'ok'})

    config = Config(
        port=0,
        forwarded_allow_ips=('127.0.0.1',),
        proxy_protocol='v1',
    )
    async with running_server(app, config) as server:
        with pytest.raises((
            ConnectionResetError,
            BrokenPipeError,
            RuntimeError,
            OSError,
        )):
            await asyncio.wait_for(
                h2_request(
                    port=server_port(server),
                    prefix=proxy_v1_prefix(
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
        port=0,
        forwarded_allow_ips=('10.0.0.0/8',),
        proxy_protocol='v1',
    )
    async with running_server(app, config) as server:
        with pytest.raises((
            ConnectionResetError,
            BrokenPipeError,
            RuntimeError,
            OSError,
        )):
            await asyncio.wait_for(
                h2_request(
                    port=server_port(server),
                    prefix=proxy_v1_prefix(
                        client_host='203.0.113.10',
                        server_host='198.51.100.20',
                        client_port=41234,
                        server_port=8080,
                    ),
                ),
                timeout=5,
            )


async def test_untrusted_proxy_protocol_configuration_fault_is_reported(
    captured_stderr,
) -> None:
    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'ok'})

    config = Config(
        port=0,
        forwarded_allow_ips=('10.0.0.0/8',),
        proxy_protocol='v1',
    )
    async with running_server(app, config) as server:
        reader, writer = await asyncio.open_connection('127.0.0.1', server_port(server))
        try:
            writer.write(
                proxy_v1_prefix(
                    client_host='203.0.113.10',
                    server_host='198.51.100.20',
                    client_port=41234,
                    server_port=8080,
                )
            )
            await writer.drain()
            try:
                await asyncio.wait_for(reader.read(), timeout=5)
            except ConnectionResetError:
                pass
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except ConnectionResetError:
                pass

    stderr = captured_stderr.readouterr().err
    assert stderr
    assert (
        'connection failed: PROXY protocol requires the connection peer to be trusted'
        in stderr
    )


async def test_proxy_v2_maximum_payload_followed_by_http() -> None:
    """A maximum-length PROXY prelude consumes every TLV byte before HTTP."""
    payload = _proxy_v2_ipv4_payload() + (b'x' * 65_523)
    assert len(payload) == 2**16 - 1
    prefix = _proxy_v2_prefix(0x21, 0x11, payload)

    async def app(scope, receive, send):
        if scope['type'] != 'http':
            return
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({
            'type': 'http.response.body',
            'body': f'{scope["client"][0]}|{scope["client"][1]}'.encode(),
        })

    config = Config(
        port=0,
        forwarded_allow_ips=('127.0.0.1',),
        proxy_protocol='v2',
    )
    async with running_server(app, config) as server:
        status, body = await asyncio.wait_for(
            h2_request(port=server_port(server), prefix=prefix), timeout=10
        )

    assert (status, body) == (200, b'203.0.113.10|41234')


async def test_proxy_v2_maximum_payload_truncated() -> None:
    """EOF one byte before the declared prelude is never dispatched as HTTP."""
    calls = 0
    payload = _proxy_v2_ipv4_payload() + (b'x' * 65_523)
    prefix = _proxy_v2_prefix(0x21, 0x11, payload)

    async def app(scope, receive, send):
        nonlocal calls
        if scope['type'] != 'http':
            return
        calls += 1
        await send({'type': 'http.response.start', 'status': 204, 'headers': []})
        await send({'type': 'http.response.body', 'body': b''})

    config = Config(
        port=0,
        forwarded_allow_ips=('127.0.0.1',),
        proxy_protocol='v2',
    )
    async with running_server(app, config) as server:
        _reader, writer = await asyncio.open_connection(
            '127.0.0.1', server_port(server)
        )
        writer.write(prefix[:-1])
        await writer.drain()
        writer.close()
        await writer.wait_closed()

        # A valid later connection is the completion barrier for the EOF path:
        # if the truncated prelude had reached the application, `calls` is 2.
        status, body = await asyncio.wait_for(
            h2_request(
                port=server_port(server),
                prefix=_proxy_v2_prefix(0x21, 0x11, _proxy_v2_ipv4_payload()),
            ),
            timeout=5,
        )

    assert (status, body, calls) == (204, b'', 1)


async def test_proxy_v2_local_and_proxy_minimums() -> None:
    """LOCAL preserves the peer tuple; both concrete minimum tuples map exactly."""
    observed: list[tuple[object, object]] = []

    async def app(scope, receive, send):
        if scope['type'] != 'http':
            return
        observed.append((scope['client'], scope['server']))
        await send({'type': 'http.response.start', 'status': 204, 'headers': []})
        await send({'type': 'http.response.body', 'body': b''})

    config = Config(
        port=0,
        forwarded_allow_ips=('127.0.0.1', '::1'),
        proxy_protocol='v2',
    )
    async with running_server(app, config) as server:
        port = server_port(server)
        local_status, _ = await h2_request(
            port=port,
            prefix=_proxy_v2_prefix(0x20, 0x00),
        )
        ipv4_status, _ = await h2_request(
            port=port,
            prefix=_proxy_v2_prefix(0x21, 0x11, _proxy_v2_ipv4_payload()),
        )

    assert (local_status, ipv4_status) == (204, 204)
    local_client, local_server = observed[0]
    assert isinstance(local_client, tuple) and local_client[0] == '127.0.0.1'
    assert isinstance(local_server, tuple) and local_server[1] == port
    assert observed[1] == (('203.0.113.10', 41234), ('198.51.100.20', 8080))


@pytest.mark.skipif(
    not _ipv6_loopback_is_bindable(),
    reason='IPv6 loopback is disabled in this kernel namespace',
)
async def test_proxy_v2_ipv6_proxy_minimum_maps_exactly() -> None:
    observed: list[tuple[object, object]] = []
    payload = (
        socket.inet_pton(socket.AF_INET6, '2001:db8::10')
        + socket.inet_pton(socket.AF_INET6, '2001:db8::20')
        + (41234).to_bytes(2, 'big')
        + (8080).to_bytes(2, 'big')
    )
    assert len(payload) == 36

    async def app(scope, receive, send):
        if scope['type'] != 'http':
            return
        observed.append((scope['client'], scope['server']))
        await send({'type': 'http.response.start', 'status': 204, 'headers': []})
        await send({'type': 'http.response.body', 'body': b''})

    config = Config(
        bind=('[::1]:0',),
        forwarded_allow_ips=('::1',),
        proxy_protocol='v2',
    )
    async with running_server(app, config) as server:
        status, body = await h2_request(
            host='::1',
            port=server_port(server),
            prefix=_proxy_v2_prefix(0x21, 0x21, payload),
        )

    assert (status, body) == (204, b'')
    assert observed == [(('2001:db8::10', 41234), ('2001:db8::20', 8080))]


@pytest.mark.parametrize(
    ('version_command', 'family_transport'),
    [
        (0x11, 0x11),  # unsupported version
        (0x22, 0x11),  # invalid command nibble
        (0x21, 0x31),  # unsupported address family
        (0x21, 0x12),  # unsupported transport
    ],
)
async def test_proxy_v2_invalid_version_family_transport(
    version_command: int,
    family_transport: int,
) -> None:
    """Every rejected v2 nibble combination closes before application dispatch."""
    calls = 0

    async def app(scope, receive, send):
        nonlocal calls
        if scope['type'] != 'http':
            return
        calls += 1
        await send({'type': 'http.response.start', 'status': 204, 'headers': []})
        await send({'type': 'http.response.body', 'body': b''})

    config = Config(
        port=0,
        forwarded_allow_ips=('127.0.0.1',),
        proxy_protocol='v2',
    )
    async with running_server(app, config) as server:
        with pytest.raises((
            ConnectionResetError,
            BrokenPipeError,
            RuntimeError,
            OSError,
        )):
            await asyncio.wait_for(
                h2_request(
                    port=server_port(server),
                    prefix=_proxy_v2_prefix(
                        version_command,
                        family_transport,
                        _proxy_v2_ipv4_payload(),
                    ),
                ),
                timeout=5,
            )
        status, body = await asyncio.wait_for(
            h2_request(
                port=server_port(server),
                prefix=_proxy_v2_prefix(0x21, 0x11, _proxy_v2_ipv4_payload()),
            ),
            timeout=5,
        )

    assert (status, body, calls) == (204, b'', 1)


@pytest.mark.parametrize('split_at', range(1, 29))
async def test_proxy_v2_prefix_split_at_every_boundary(split_at: int) -> None:
    """Exact-read accumulation is independent of every prelude segmentation."""
    prefix = _proxy_v2_prefix(0x21, 0x11, _proxy_v2_ipv4_payload())
    assert len(prefix) == 28

    async def app(scope, receive, send):
        if scope['type'] != 'http':
            return
        await send({'type': 'http.response.start', 'status': 204, 'headers': []})
        await send({'type': 'http.response.body', 'body': b''})

    config = Config(
        port=0,
        forwarded_allow_ips=('127.0.0.1',),
        proxy_protocol='v2',
    )
    async with running_server(app, config) as server:
        status, _headers, body, trailers = await asyncio.wait_for(
            _http1_after_split_proxy_prefix(server_port(server), prefix, split_at),
            timeout=5,
        )

    assert (status, body, trailers) == (204, b'', [])
