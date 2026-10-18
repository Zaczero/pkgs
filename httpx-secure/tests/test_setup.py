from __future__ import annotations

import os

import anyio
import httpx
import pytest

from httpx_secure import SSRFProtectionError, httpx_ssrf_protection

pytestmark = pytest.mark.anyio


@pytest.mark.parametrize('http2', [False, True], ids=['http1', 'http2'])
@pytest.mark.parametrize('request_after_setup', [False, True], ids=['close', 'request'])
async def test_idle_connections_retire_without_losing_close_ownership(
    server_factory, tls_contexts, http2, request_after_setup
):
    server_tls, client_tls = tls_contexts
    async with server_factory(tls=server_tls, http2=http2) as server:
        async with httpx.AsyncClient(
            verify=client_tls, http2=http2, trust_env=False
        ) as client:
            url = f'https://localhost:{server.port}'
            await client.get(f'{url}/before')
            transport = client._transport
            pool = transport._pool
            connection = server.requests[0].connection
            assert httpx_ssrf_protection(client) is client
            assert client._transport is transport
            assert transport._pool is pool
            if request_after_setup:
                with pytest.raises(SSRFProtectionError):
                    await client.post(f'{url}/after', content=b'never-send')
                with anyio.fail_after(5):
                    await server.closed[connection].wait()
        with anyio.fail_after(5):
            await server.closed[connection].wait()
        assert [request.path for request in server.requests] == ['/before']


async def test_active_mounted_response_rejects_setup_without_expiring_idle_route(
    server_factory,
):
    mounted = httpx.AsyncHTTPTransport()
    async with (
        server_factory() as server,
        httpx.AsyncClient(
            trust_env=False, mounts={'http://localhost': mounted}
        ) as client,
    ):
        await client.get(f'http://127.0.0.1:{server.port}/idle')
        first = server.requests[-1].connection
        response = await client.send(
            client.build_request('GET', f'http://localhost:{server.port}/active'),
            stream=True,
        )
        try:
            with pytest.raises(RuntimeError):
                httpx_ssrf_protection(client)
            await client.get(f'http://127.0.0.1:{server.port}/still-idle')
            assert server.requests[-1].connection == first
        finally:
            await response.aclose()


async def test_direct_mounts_and_duplicate_transport_routes_are_protected(
    server_factory,
):
    mounted = httpx.AsyncHTTPTransport(local_address='127.0.0.1')
    calls = []

    def allow(host, ip, port):
        calls.append(host)
        return True

    async with server_factory() as server:
        async with httpx.AsyncClient(
            trust_env=False,
            mounts={'http://localhost': mounted, 'https://localhost': mounted},
        ) as client:
            httpx_ssrf_protection(
                client, check_globally_reachable=False, custom_validator=allow
            )
            await client.get(f'http://127.0.0.1:{server.port}/default')
            await client.get(f'http://localhost:{server.port}/mounted')
        assert calls == ['127.0.0.1', 'localhost']
        assert [request.path for request in server.requests] == ['/default', '/mounted']


@pytest.mark.parametrize(
    'transport, reason',
    [
        pytest.param(
            lambda: httpx.AsyncHTTPTransport(proxy='http://proxy.invalid'),
            'proxy transports are not supported',
            id='proxy',
        ),
        pytest.param(
            lambda: httpx.AsyncHTTPTransport(uds='/unused.sock'),
            'Unix-domain socket transports are not supported',
            id='unix',
        ),
        pytest.param(
            lambda: httpx.MockTransport(lambda _request: httpx.Response(200)),
            'unsupported transport: MockTransport',
            id='custom',
        ),
    ],
)
async def test_unsupported_mount_rejects_setup_without_changing_usable_route(
    server_factory, transport, reason
):
    async with server_factory() as server:
        async with httpx.AsyncClient(
            trust_env=False, mounts={'http://unsupported.invalid': transport()}
        ) as client:
            await client.get(f'http://127.0.0.1:{server.port}/before')
            first = server.requests[-1].connection
            with pytest.raises(ValueError, match=reason):
                httpx_ssrf_protection(client)
            await client.get(f'http://127.0.0.1:{server.port}/after')
        assert server.requests[-1].connection == first


async def test_actual_environment_proxy_is_rejected(monkeypatch):
    monkeypatch.setenv('HTTPS_PROXY', 'http://proxy.invalid')
    async with httpx.AsyncClient(trust_env=True) as client:
        with pytest.raises(ValueError, match='proxy transports are not supported'):
            httpx_ssrf_protection(client)


async def test_trust_env_is_retained_without_actual_proxy_routes(
    monkeypatch, server_factory
):
    for key in list(os.environ):
        if key.lower().endswith('_proxy'):
            monkeypatch.delenv(key)
    async with server_factory() as server:
        async with httpx_ssrf_protection(httpx.AsyncClient(trust_env=True)) as client:
            assert client.trust_env
            with pytest.raises(SSRFProtectionError):
                await client.get(f'http://127.0.0.1:{server.port}/')
        assert server.requests == []


async def test_closed_client_and_duplicate_installation_are_rejected():
    async with httpx.AsyncClient(trust_env=False) as client:
        httpx_ssrf_protection(client)
        with pytest.raises(RuntimeError):
            httpx_ssrf_protection(client)
    with pytest.raises(RuntimeError):
        httpx_ssrf_protection(client)


@pytest.mark.parametrize(
    ('client', 'client_type'),
    [
        pytest.param(None, 'NoneType', id='none'),
        pytest.param(object(), 'object', id='other-object'),
    ],
)
def test_non_async_client_is_rejected_before_transport_inspection(client, client_type):
    with pytest.raises(
        TypeError,
        match=f'client must be an HTTPX AsyncClient, got {client_type}',
    ):
        httpx_ssrf_protection(client)


def test_sync_httpx_client_is_rejected_before_transport_inspection():
    with (
        httpx.Client(trust_env=False) as client,
        pytest.raises(
            TypeError,
            match='client must be an HTTPX AsyncClient, got Client',
        ),
    ):
        httpx_ssrf_protection(client)


@pytest.mark.parametrize(
    ('validator', 'validator_type'),
    [
        pytest.param(False, 'bool', id='false'),
        pytest.param(0, 'int', id='zero'),
        pytest.param(object(), 'object', id='object'),
    ],
)
async def test_noncallable_validator_is_rejected_before_transport_inspection(
    validator, validator_type
):
    transport = httpx.MockTransport(lambda _request: httpx.Response(200))
    async with httpx.AsyncClient(transport=transport, trust_env=False) as client:
        with pytest.raises(
            TypeError,
            match=(f'custom_validator must be callable or None, got {validator_type}'),
        ):
            httpx_ssrf_protection(
                client,
                check_globally_reachable=False,
                custom_validator=validator,
            )
        assert client._transport is transport
