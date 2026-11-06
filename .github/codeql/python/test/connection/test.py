"""Expose the nested package to the native query-test extractor."""

from httpx_secure._network import _ProtectedBackend


async def exercise(backend, policy, host, port, mode):
    protected = _ProtectedBackend(backend, policy)
    return await protected.connect_tcp(host, port, mode)
