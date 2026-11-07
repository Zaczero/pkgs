"""Expose the nested package to the native query-test extractor."""

from httpx_secure._network import _ProtectedBackend
from httpx_secure import _AddressPolicy, ApplicationConnector


async def exercise(backend, policy, host, port, mode):
    protected = _ProtectedBackend(backend, policy)
    return await protected.connect_tcp(host, port, mode)


async def exercise_probe(host, port, mode):
    return await _AddressPolicy()._resolve(host, port, None, mode)


async def exercise_application(host, port):
    return await ApplicationConnector()._resolve(host, port)
