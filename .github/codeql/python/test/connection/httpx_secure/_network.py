"""Explicit good and bad data-bearing connections for the contract query."""

from anyio import connect_tcp as probe


class UnrelatedPolicy:
    async def admit(self, host, port, local_address):
        return host


unrelated = UnrelatedPolicy()


async def dial(backend, address, port):
    return await backend.connect_tcp(address, port)


async def admit(policy, host, port):
    return await policy.admit(host, port, None)


class _ProtectedBackend:
    def __init__(self, backend, policy):
        self._backend = backend
        self._policy = policy

    async def connect_tcp(self, host, port, mode):
        if mode == 'direct':
            return await self._backend.connect_tcp(host, port)
        if mode == 'alias':
            address = host
            connect = self._backend.connect_tcp
            return await connect(host=address, port=port)
        if mode == 'helper':
            return await dial(self._backend, host, port)
        if mode == 'normalized':
            return await self._backend.connect_tcp(host.lower(), port)
        if mode == 'ignored-admission':
            await self._policy.admit(host, port, None)
            return await self._backend.connect_tcp(host, port)
        if mode == 'fallback':
            try:
                address = await self._policy.admit(host, port, None)
            except ValueError:
                address = host
            return await self._backend.connect_tcp(address, port)
        if mode == 'admitted':
            address = await self._policy.admit(host, port, None)
            return await self._backend.connect_tcp(address, port)
        if mode == 'admitted-alias':
            policy = self._policy
            selected = await policy.admit(host, port, None)
            address = selected
            return await dial(self._backend, address, port)
        if mode == 'admitted-helper':
            address = await admit(self._policy, host, port)
            return await dial(self._backend, address, port)
        if mode == 'probe-then-admitted':
            stream = await probe(host, port)
            await stream.aclose()
            address = await self._policy.admit(host, port, None)
            return await self._backend.connect_tcp(address, port)
        if mode == 'unrelated-admission':
            address = await unrelated.admit(host, port, None)
            return await self._backend.connect_tcp(address, port)
        if mode == 'closure':

            async def bad_connect():
                return await self._backend.connect_tcp(host, port)

            return await bad_connect()
        if mode == 'admitted-closure':
            address = await self._policy.admit(host, port, None)

            async def good_connect():
                return await self._backend.connect_tcp(address, port)

            return await good_connect()
        return None
