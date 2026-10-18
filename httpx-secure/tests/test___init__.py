from __future__ import annotations

import doctest
from ipaddress import ip_address
from pathlib import Path

import httpx
import pytest

from httpx_secure import (
    SSRFProtectionError,
    _AddressPolicy,
    _DNSCache,
    httpx_ssrf_protection,
)

pytestmark = pytest.mark.anyio


def test_readme_examples():
    result = doctest.testfile(
        str(Path(__file__).parents[1] / 'README.md'),
        module_relative=False,
        encoding='utf-8',
    )
    assert result.failed == 0


@pytest.mark.parametrize(
    'address',
    [
        '127.0.0.1',
        '::1',
        '192.168.1.1',
        '10.0.0.1',
        '172.16.0.1',
        '169.254.1.1',
        'fe80::1',
        'fc00::1',
        '64:ff9b::7f00:1',
        '64:ff9b::c0a8:101',
        '64:ff9b::6440:1',
    ],
)
async def test_blocks_non_global_literals(address):
    host = f'[{address}]' if ':' in address else address
    async with httpx_ssrf_protection(httpx.AsyncClient(trust_env=False)) as client:
        with pytest.raises(SSRFProtectionError) as caught:
            await client.get(f'http://{host}/blocked')
    assert str(caught.value) == (
        f'access denied: IP address {address!r} is not globally reachable'
    )
    assert caught.value.ip_addr == ip_address(address)
    assert caught.value.hostname == address
    assert caught.value.port == 80
    assert caught.value.request.url.path == '/blocked'


def test_invalid_ip_error_names_its_parameter_and_value():
    error = SSRFProtectionError.invalid_ip(
        'bad address',
        hostname='bad address',
        port=80,
    )

    assert str(error) == "ip_str must be a valid IP address, got 'bad address'"


@pytest.mark.parametrize(
    'address',
    ['8.8.8.8', '1.1.1.1', '2001:4860:4860::8888', '64:ff9b::808:808'],
)
async def test_global_literal_policy_needs_no_resolution(address):
    policy = _AddressPolicy(
        cache=_DNSCache(10, 60),
        check_globally_reachable=True,
        custom_validator=None,
        happy_eyeballs_delay=0.25,
    )
    assert await policy.admit(address, 443, None) == address


async def test_cached_denials_are_fresh_and_bound_to_current_request():
    calls = []

    async def deny(hostname, address, port):
        calls.append((hostname, address, port))
        return False

    async with httpx_ssrf_protection(
        httpx.AsyncClient(trust_env=False), custom_validator=deny
    ) as client:
        with pytest.raises(SSRFProtectionError) as first:
            await client.get('https://8.8.8.8/first')
        with pytest.raises(SSRFProtectionError) as second:
            await client.get('https://8.8.8.8/second')
    assert str(first.value) == (
        "access denied: IP address '8.8.8.8' failed custom validation"
    )
    assert calls == [('8.8.8.8', ip_address('8.8.8.8'), 443)]
    assert first.value is not second.value
    assert first.value.request.url.path == '/first'
    assert second.value.request.url.path == '/second'


async def test_global_check_can_be_disabled_with_custom_policy(server_factory):
    seen = []

    def allow(hostname, address, port):
        seen.append((hostname, address, port))
        return address.is_loopback

    async with server_factory() as server:
        async with httpx_ssrf_protection(
            httpx.AsyncClient(trust_env=False),
            check_globally_reachable=False,
            custom_validator=allow,
        ) as client:
            response = await client.get(f'http://127.0.0.1:{server.port}/allowed')
        assert response.content == b'OK'
        assert seen == [('127.0.0.1', ip_address('127.0.0.1'), server.port)]


async def test_disabling_global_check_requires_custom_validator():
    async with httpx.AsyncClient(trust_env=False) as client:
        with pytest.raises(ValueError, match='custom_validator must be provided'):
            httpx_ssrf_protection(client, check_globally_reachable=False)
