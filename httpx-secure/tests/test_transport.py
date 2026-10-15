from __future__ import annotations

import socket
from ipaddress import ip_address

import anyio
import httpx
import pytest

import httpx_secure
from httpx_secure import SSRFProtectionError, httpx_ssrf_protection

pytestmark = pytest.mark.anyio


def protect(client, **kwargs):
    return httpx_ssrf_protection(
        client,
        check_globally_reachable=False,
        custom_validator=lambda _host, ip, _port: ip.is_loopback,
        **kwargs,
    )


async def test_hostname_denial_sends_no_application_bytes(server_factory):
    async with server_factory() as server:
        async with httpx_ssrf_protection(httpx.AsyncClient(trust_env=False)) as client:
            with pytest.raises(SSRFProtectionError) as error:
                await client.post(
                    f'http://localhost:{server.port}/denied', content=b'must-not-send'
                )
        assert error.value.hostname == 'localhost'
        assert error.value.ip_addr.is_loopback
        assert server.requests == []


@pytest.mark.parametrize('http2', [False, True], ids=['http1', 'http2'])
async def test_tls_relative_redirect_preserves_authority_and_pool(
    server_factory, tls_contexts, http2
):
    server_tls, client_tls = tls_contexts
    responses = {
        '/start': (
            302,
            [('Location', '/next'), ('Set-Cookie', 'sid=local; Path=/')],
            b'',
        )
    }
    async with server_factory(
        tls=server_tls, http2=http2, responses=responses
    ) as server:
        client = httpx.AsyncClient(
            verify=client_tls, http2=http2, trust_env=False, follow_redirects=True
        )
        transport, pool = client._transport, client._transport._pool
        assert protect(client) is client
        async with client:
            response = await client.get(
                f'https://localhost:{server.port}/start',
                headers={'Authorization': 'Bearer origin-token'},
            )
        assert response.content == b'OK'
        assert str(response.url) == f'https://localhost:{server.port}/next'
        assert response.http_version == ('HTTP/2' if http2 else 'HTTP/1.1')
        assert client._transport is transport
        assert transport._pool is pool
        first, second = server.requests
        authority = ':authority' if http2 else 'host'
        assert first.headers[authority] == f'localhost:{server.port}'
        assert second.headers[authority] == f'localhost:{server.port}'
        assert first.sni == second.sni == 'localhost'
        assert first.connection == second.connection
        assert second.headers['cookie'] == 'sid=local'
        assert second.headers['authorization'] == 'Bearer origin-token'


@pytest.mark.parametrize('http2', [False, True], ids=['http1', 'http2'])
async def test_same_ip_different_origin_requires_its_own_verified_tls(
    server_factory, tls_contexts, http2
):
    server_tls, client_tls = tls_contexts
    async with server_factory(tls=server_tls, http2=http2) as server:
        async with protect(
            httpx.AsyncClient(verify=client_tls, http2=http2, trust_env=False)
        ) as client:
            await client.get(f'https://localhost:{server.port}/first')
            with pytest.raises(httpx.ConnectError):
                await client.post(
                    f'https://127.0.0.1:{server.port}/second',
                    headers={'Authorization': 'Bearer must-not-send'},
                    content=b'meaningful-secret',
                )
        assert [request.path for request in server.requests] == ['/first']


async def test_absolute_redirect_rechecks_destination_and_strips_auth(server_factory):
    async with server_factory() as destination:
        responses = {
            '/start': (
                302,
                [('Location', f'http://127.0.0.1:{destination.port}/next')],
                b'',
            )
        }
        async with server_factory(responses=responses) as origin:
            admitted = []

            def allow(host, ip, port):
                admitted.append((host, ip, port))
                return ip.is_loopback

            async with httpx_ssrf_protection(
                httpx.AsyncClient(trust_env=False, follow_redirects=True),
                check_globally_reachable=False,
                custom_validator=allow,
            ) as client:
                await client.get(
                    f'http://localhost:{origin.port}/start',
                    headers={'Authorization': 'Bearer origin-only'},
                )
            assert 'authorization' not in destination.requests[0].headers
            assert admitted == [
                ('localhost', ip_address('127.0.0.1'), origin.port),
                ('127.0.0.1', ip_address('127.0.0.1'), destination.port),
            ]


async def test_absolute_redirect_to_denied_endpoint_sends_no_payload(server_factory):
    async with server_factory() as destination:
        responses = {
            '/start': (
                307,
                [('Location', f'http://127.0.0.1:{destination.port}/denied')],
                b'',
            )
        }
        async with server_factory(responses=responses) as origin:
            async with httpx_ssrf_protection(
                httpx.AsyncClient(trust_env=False, follow_redirects=True),
                check_globally_reachable=False,
                custom_validator=lambda _host, _ip, port: port == origin.port,
            ) as client:
                with pytest.raises(SSRFProtectionError) as error:
                    await client.post(
                        f'http://localhost:{origin.port}/start', content=b'origin-body'
                    )
            assert error.value.port == destination.port
            assert origin.requests[0].body == b'origin-body'
            assert destination.requests == []


async def test_connection_lifetime_admission_and_new_connection_ttl(
    server_factory, monkeypatch
):
    clock = [1.0]
    monkeypatch.setattr(httpx_secure, 'monotonic', lambda: clock[0])
    allowed = [True]
    calls = []

    def validate(host, ip, port):
        calls.append(allowed[0])
        return allowed[0]

    async with server_factory() as server:
        async with httpx_ssrf_protection(
            httpx.AsyncClient(trust_env=False),
            check_globally_reachable=False,
            custom_validator=validate,
            dns_cache_ttl=5,
        ) as client:
            url = f'http://localhost:{server.port}'
            await client.get(f'{url}/first')
            allowed[0] = False
            clock[0] = 20
            assert (await client.get(f'{url}/pooled')).status_code == 200
            await client.get(f'{url}/close', headers={'Connection': 'close'})
            with pytest.raises(SSRFProtectionError):
                await client.get(f'{url}/new')
        assert calls == [True, False]
        assert [request.path for request in server.requests] == [
            '/first',
            '/pooled',
            '/close',
        ]


async def test_new_connections_share_cached_admission_until_expiry(
    server_factory, monkeypatch
):
    clock = [1.0]
    monkeypatch.setattr(httpx_secure, 'monotonic', lambda: clock[0])
    calls = []

    def validate(host, ip, port):
        calls.append(port)
        return True

    async with server_factory() as server:
        async with httpx_ssrf_protection(
            httpx.AsyncClient(trust_env=False),
            check_globally_reachable=False,
            custom_validator=validate,
            dns_cache_ttl=5,
        ) as client:
            url = f'http://localhost:{server.port}'
            await client.get(f'{url}/first', headers={'Connection': 'close'})
            await client.get(f'{url}/cached', headers={'Connection': 'close'})
            clock[0] = 10
            await client.get(f'{url}/expired', headers={'Connection': 'close'})
        assert calls == [server.port, server.port]


async def test_current_request_connect_timeout_bounds_async_validation(server_factory):
    entered = anyio.Event()

    async def stalled(host, ip, port):
        entered.set()
        await anyio.sleep_forever()

    async with server_factory() as server:
        async with httpx_ssrf_protection(
            httpx.AsyncClient(trust_env=False, timeout=None),  # noqa: S113 - The per-request deadline must override an unbounded client default.
            check_globally_reachable=False,
            custom_validator=stalled,
        ) as client:
            with pytest.raises(httpx.ConnectTimeout):
                await client.get(f'http://127.0.0.1:{server.port}/', timeout=0.02)
        assert entered.is_set()
        assert server.requests == []


async def test_cancelled_validator_releases_admission_without_caching_denial(
    server_factory,
):
    entered = anyio.Event()
    calls = []

    async def validate(host, ip, port):
        calls.append(port)
        if len(calls) == 1:
            entered.set()
            await anyio.sleep_forever()
        return True

    async with server_factory() as server:
        async with httpx_ssrf_protection(
            httpx.AsyncClient(trust_env=False),
            check_globally_reachable=False,
            custom_validator=validate,
        ) as client:

            async def first(*, task_status):
                with anyio.CancelScope() as scope:
                    task_status.started(scope)
                    await client.get(f'http://localhost:{server.port}/cancel')

            async with anyio.create_task_group() as group:
                scope = await group.start(first)
                await entered.wait()
                scope.cancel()
            assert (
                await client.get(f'http://localhost:{server.port}/next')
            ).content == b'OK'
        assert calls == [server.port, server.port]
        assert [request.path for request in server.requests] == ['/next']


async def test_cancelled_waiter_does_not_cancel_shared_admission(server_factory):
    validating = anyio.Event()
    release = anyio.Event()
    waiter_connecting = anyio.Event()
    calls = []

    async def validate(host, ip, port):
        calls.append(port)
        validating.set()
        await release.wait()
        return True

    async def trace(name, info):
        if name == 'connection.connect_tcp.started':
            waiter_connecting.set()

    async with server_factory() as server:
        async with httpx_ssrf_protection(
            httpx.AsyncClient(trust_env=False),
            check_globally_reachable=False,
            custom_validator=validate,
        ) as client:

            async def first():
                await client.get(f'http://localhost:{server.port}/first')

            async def waiter(*, task_status):
                with anyio.CancelScope() as scope:
                    task_status.started(scope)
                    await client.get(
                        f'http://localhost:{server.port}/waiter',
                        extensions={'trace': trace},
                    )

            async with anyio.create_task_group() as group:
                group.start_soon(first)
                await validating.wait()
                scope = await group.start(waiter)
                await waiter_connecting.wait()
                scope.cancel()
                release.set()
            await client.get(f'http://localhost:{server.port}/cached')
        assert calls == [server.port]
        assert [request.path for request in server.requests] == ['/first', '/cached']


async def test_cancelled_tls_setup_closes_the_owned_stream():
    hello = anyio.Event()
    closed = anyio.Event()

    async def stall(stream):
        async with stream:
            await stream.receive(65536)
            hello.set()
            try:
                while True:
                    await stream.receive(65536)
            except anyio.EndOfStream:
                closed.set()

    listener = await anyio.create_tcp_listener(local_host='127.0.0.1')
    port = listener.extra(anyio.abc.SocketAttribute.local_port)
    async with listener, anyio.create_task_group() as group:
        group.start_soon(listener.serve, stall)
        async with protect(httpx.AsyncClient(trust_env=False, timeout=30)) as client:

            async def request(*, task_status):
                with anyio.CancelScope() as scope:
                    task_status.started(scope)
                    await client.get(f'https://127.0.0.1:{port}/')

            scope = await group.start(request)
            await hello.wait()
            scope.cancel()
            with anyio.fail_after(5):
                await closed.wait()
        group.cancel_scope.cancel()


async def test_later_request_hook_cannot_bypass_actual_destination_policy(
    server_factory,
):
    async with server_factory() as server:
        async with httpx_ssrf_protection(httpx.AsyncClient(trust_env=False)) as client:

            async def change_destination(request):
                request.url = request.url.copy_with(host='127.0.0.1', port=server.port)

            client.event_hooks['request'].append(change_destination)
            with pytest.raises(SSRFProtectionError):
                await client.post('http://8.8.8.8/', content=b'never-send')
        assert server.requests == []


async def test_data_connection_is_pinned_to_the_selected_validated_ip(
    server_factory, monkeypatch
):
    real_resolution = socket.getaddrinfo
    resolutions = []

    def resolve(host, port, family=0, type=0, proto=0, flags=0):
        name = host.decode() if isinstance(host, bytes) else host
        if name == 'destination.test':
            if flags & socket.AI_NUMERICHOST:
                raise socket.gaierror(socket.EAI_NONAME, 'A hostname, not an IP')
            resolutions.append(name)
            host = '127.0.0.1' if len(resolutions) == 1 else '127.0.0.2'
        return real_resolution(host, port, family, type, proto, flags)

    async with (
        server_factory(host='127.0.0.1') as allowed,
        server_factory(host='127.0.0.2', port=allowed.port) as denied,
    ):
        monkeypatch.setattr(socket, 'getaddrinfo', resolve)
        async with httpx_ssrf_protection(
            httpx.AsyncClient(trust_env=False),
            check_globally_reachable=False,
            custom_validator=lambda _host, ip, _port: str(ip) == '127.0.0.1',
        ) as client:
            response = await client.post(
                f'http://destination.test:{allowed.port}/pinned',
                content=b'only-approved',
            )
        assert response.content == b'OK'
        assert allowed.requests[0].body == b'only-approved'
        assert denied.requests == []
        assert resolutions == ['destination.test']


async def test_ipv6_literal_keeps_bracketed_authority(server_factory):
    async with server_factory(host='::1') as server:
        async with protect(httpx.AsyncClient(trust_env=False)) as client:
            response = await client.get(f'http://[::1]:{server.port}/ipv6')
        assert response.content == b'OK'
        assert server.requests[0].headers['host'] == f'[::1]:{server.port}'


async def test_cache_eviction_revalidates_new_connections(server_factory):
    calls = []

    def allow(host, ip, port):
        calls.append(host)
        return True

    async with server_factory() as server:
        async with httpx_ssrf_protection(
            httpx.AsyncClient(trust_env=False),
            check_globally_reachable=False,
            custom_validator=allow,
            dns_cache_size=1,
        ) as client:
            for host in ['localhost', '127.0.0.1', 'localhost']:
                await client.get(
                    f'http://{host}:{server.port}/', headers={'Connection': 'close'}
                )
        assert calls == ['localhost', '127.0.0.1', 'localhost']


async def test_expired_denial_can_admit_a_new_connection(server_factory, monkeypatch):
    clock = [1.0]
    monkeypatch.setattr(httpx_secure, 'monotonic', lambda: clock[0])
    allowed = [False]

    async with server_factory() as server:
        async with httpx_ssrf_protection(
            httpx.AsyncClient(trust_env=False),
            check_globally_reachable=False,
            custom_validator=lambda _host, _ip, _port: allowed[0],
            dns_cache_ttl=5,
        ) as client:
            url = f'http://127.0.0.1:{server.port}/'
            with pytest.raises(SSRFProtectionError):
                await client.get(url)
            allowed[0] = True
            clock[0] = 10
            assert (await client.get(url)).content == b'OK'
        assert len(server.requests) == 1


async def test_dns_failures_are_request_bound_and_not_cached_as_denials(monkeypatch):
    calls = []

    def fail_resolution(host, *args, **kwargs):
        calls.append(host)
        raise socket.gaierror(socket.EAI_NONAME, 'No such hostname')

    monkeypatch.setattr(socket, 'getaddrinfo', fail_resolution)
    async with httpx_ssrf_protection(httpx.AsyncClient(trust_env=False)) as client:
        with pytest.raises(httpx.ConnectError) as first:
            await client.get('http://nonexistent.invalid/first')
        first_calls = len(calls)
        with pytest.raises(httpx.ConnectError) as second:
            await client.get('http://nonexistent.invalid/second')
    assert first_calls > 0
    assert len(calls) > first_calls
    assert first.value.request.url.path == '/first'
    assert second.value.request.url.path == '/second'


@pytest.mark.parametrize('error_type', [ValueError, TimeoutError])
async def test_validator_exceptions_propagate_without_poisoning_admission(
    server_factory, error_type
):
    calls = []

    async def validate(host, ip, port):
        calls.append(port)
        if len(calls) <= 2:
            raise error_type('Custom policy provider failed')
        return True

    async with server_factory() as server:
        async with httpx_ssrf_protection(
            httpx.AsyncClient(trust_env=False),
            check_globally_reachable=False,
            custom_validator=validate,
        ) as client:
            url = f'http://127.0.0.1:{server.port}/'
            with pytest.raises(error_type):
                await client.get(url)
            with pytest.raises(error_type):
                await client.get(url)
            assert (await client.get(url)).content == b'OK'
        assert calls == [server.port, server.port, server.port]
