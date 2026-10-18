from __future__ import annotations

# This module owns the version-bounded private HTTPX/HTTPcore integration.
# pyright: reportPrivateUsage=false
from typing import TYPE_CHECKING, cast

import httpcore
import httpx
from anyio import CancelScope, current_time, move_on_after
from httpcore._backends.auto import AutoBackend

if TYPE_CHECKING:
    from collections.abc import Iterable
    from ssl import SSLContext

    from httpx_secure import _AddressPolicy


class _ProtectedStream(httpcore.AsyncNetworkStream):
    __slots__ = ('_stream',)

    def __init__(self, stream: httpcore.AsyncNetworkStream) -> None:
        self._stream = stream

    async def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
        return await self._stream.read(max_bytes, timeout)

    async def write(self, buffer: bytes, timeout: float | None = None) -> None:
        await self._stream.write(buffer, timeout)

    async def aclose(self) -> None:
        await self._stream.aclose()

    async def start_tls(
        self,
        ssl_context: SSLContext,
        server_hostname: str | None = None,
        timeout: float | None = None,
    ) -> httpcore.AsyncNetworkStream:
        try:
            stream = await self._stream.start_tls(
                ssl_context, server_hostname=server_hostname, timeout=timeout
            )
        except BaseException:
            # Stock HTTPcore streams close on Exception, which excludes cancellation.
            with CancelScope(shield=True):
                await self._stream.aclose()
            raise
        return _ProtectedStream(stream)

    def get_extra_info(self, info: str) -> object:
        return self._stream.get_extra_info(info)


class _ProtectedBackend(httpcore.AsyncNetworkBackend):
    __slots__ = ('_backend', '_policy')

    def __init__(
        self, backend: httpcore.AsyncNetworkBackend, policy: _AddressPolicy
    ) -> None:
        self._backend = backend
        self._policy = policy

    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,
        local_address: str | None = None,
        socket_options: Iterable[httpcore.SOCKET_OPTION] | None = None,
    ) -> httpcore.AsyncNetworkStream:
        with move_on_after(timeout) as scope:
            address = await self._policy.admit(host, port, local_address)
            remaining = (
                None if timeout is None else max(0.0, scope.deadline - current_time())
            )
            stream = await self._backend.connect_tcp(
                address,
                port,
                timeout=remaining,
                local_address=local_address,
                socket_options=socket_options,
            )
            return _ProtectedStream(stream)
        raise httpcore.ConnectTimeout('connection establishment timed out')

    async def sleep(self, seconds: float) -> None:
        await self._backend.sleep(seconds)


def protect_client(client: httpx.AsyncClient, policy: _AddressPolicy) -> None:
    if client.is_closed:
        raise RuntimeError('cannot protect a closed AsyncClient')
    if type(client)._transport_for_url is not httpx.AsyncClient._transport_for_url:
        raise ValueError('custom AsyncClient routing is not supported')

    pools: dict[int, httpcore.AsyncConnectionPool] = {}
    expired: list[httpcore.AsyncHTTP11Connection | httpcore.AsyncHTTP2Connection] = []
    transports = [client._transport, *client._mounts.values()]
    try:
        for transport in transports:
            if transport is None:
                continue
            if type(transport) is not httpx.AsyncHTTPTransport:
                raise ValueError(
                    f'unsupported transport: {type(transport).__name__}; '
                    'standard direct AsyncHTTPTransport is required'
                )
            pool = transport._pool
            if (
                type(pool) is not httpcore.AsyncConnectionPool
                or pool._proxy is not None
            ):
                raise ValueError('proxy transports are not supported')
            if pool._uds is not None:
                raise ValueError('Unix-domain socket transports are not supported')
            if id(pool) in pools:
                continue
            backend = pool._network_backend
            if isinstance(backend, _ProtectedBackend):
                raise RuntimeError('SSRF protection is already configured')  # ruff: ignore[type-check-without-type-error] - Installation state, not an invalid caller type.
            stock_backends: tuple[type[object], ...] = (
                httpcore.AnyIOBackend,
                httpcore.TrioBackend,
            )
            if type(backend) is not AutoBackend:
                raise ValueError('custom network backends are not supported')
            initialized: object = getattr(backend, '_backend', None)
            if initialized is not None and type(initialized) not in stock_backends:
                raise ValueError('custom network backends are not supported')
            if pool._requests:
                raise RuntimeError(
                    'SSRF protection requires quiescent transports; '
                    'finish or close outstanding responses first'
                )
            for connection in pool._connections:
                if type(connection) is not httpcore.AsyncHTTPConnection:
                    raise ValueError('unsupported HTTPcore connection layout')
                if connection.is_closed():
                    continue
                if not connection.is_idle():
                    raise RuntimeError('cannot protect an active connection')
                protocol = connection._connection
                if type(protocol) not in (
                    httpcore.AsyncHTTP11Connection,
                    httpcore.AsyncHTTP2Connection,
                ) or not hasattr(protocol, '_expire_at'):
                    raise ValueError('unsupported HTTPcore protocol layout')
                expired.append(
                    cast(
                        'httpcore.AsyncHTTP11Connection | httpcore.AsyncHTTP2Connection',
                        protocol,
                    )
                )
            pools[id(pool)] = pool
    except AttributeError as error:
        raise ValueError('unsupported HTTPX/HTTPcore transport layout') from error

    backends = [
        (pool, _ProtectedBackend(pool._network_backend, policy))
        for pool in pools.values()
    ]
    # Keep idle sockets in their pool: its next assignment or close retires them.
    # Neither active streams nor any route change until every route passes admission.
    for protocol in expired:
        protocol._expire_at = float('-inf')
    for pool, backend in backends:
        pool._network_backend = backend
