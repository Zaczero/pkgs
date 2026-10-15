from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from inspect import isawaitable
from ipaddress import IPv4Address, IPv6Address, IPv6Network, ip_address
from time import monotonic
from weakref import WeakValueDictionary

from anyio import CancelScope, Lock, connect_tcp
from anyio.abc import SocketAttribute
from httpcore import ConnectError
from httpx import RequestError

from httpx_secure._network import protect_client as _protect_client

TYPE_CHECKING = False
if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from typing import TypeVar

    from httpx import AsyncClient, Request

    _IPAddress = IPv4Address | IPv6Address

    _CacheKey = tuple[str, int, str | None]  # hostname, port, local address

    _AsyncClientT = TypeVar('_AsyncClientT', bound=AsyncClient)

__version__ = '1.3.0'

_NAT64_WELL_KNOWN_PREFIX = IPv6Network('64:ff9b::/96')


class SSRFProtectionError(RequestError):
    """Raised when a request would access restricted network resources."""

    def __init__(
        self,
        message: str,
        *,
        request: Request | None = None,
        hostname: str,
        ip_addr: _IPAddress | None,
        port: int,
    ) -> None:
        super().__init__(message, request=request)
        self.hostname = hostname
        self.ip_addr = ip_addr
        self.port = port

    @classmethod
    def invalid_ip(
        cls,
        ip_str: str,
        *,
        hostname: str,
        port: int,
    ) -> SSRFProtectionError:
        return cls(
            f'Invalid IP address format: {ip_str!r}',
            hostname=hostname,
            ip_addr=None,
            port=port,
        )

    @classmethod
    def non_global(
        cls,
        ip_str: str,
        *,
        hostname: str,
        ip_addr: _IPAddress,
        port: int,
    ) -> SSRFProtectionError:
        return cls(
            f'Access denied: IP address {ip_str!r} is not globally reachable',
            hostname=hostname,
            ip_addr=ip_addr,
            port=port,
        )

    @classmethod
    def custom_validation_failed(
        cls,
        ip_str: str,
        *,
        hostname: str,
        ip_addr: _IPAddress,
        port: int,
    ) -> SSRFProtectionError:
        return cls(
            f'Access denied: IP address {ip_str!r} failed custom validation',
            hostname=hostname,
            ip_addr=ip_addr,
            port=port,
        )


@dataclass(frozen=True, slots=True)
class _DeniedAddress:
    message: str
    hostname: str
    ip_addr: _IPAddress | None
    port: int

    @classmethod
    def from_exception(cls, error: SSRFProtectionError) -> _DeniedAddress:
        return cls(
            message=str(error),
            hostname=error.hostname,
            ip_addr=error.ip_addr,
            port=error.port,
        )

    def exception(self) -> SSRFProtectionError:
        return SSRFProtectionError(
            self.message,
            hostname=self.hostname,
            ip_addr=self.ip_addr,
            port=self.port,
        )


class _DNSCache:
    __slots__ = ('_cache', '_max_size', '_ttl')

    def __init__(self, max_size: int, ttl: float) -> None:
        self._cache: OrderedDict[
            _CacheKey,
            tuple[float, str | _DeniedAddress],
        ] = OrderedDict()
        self._max_size = max_size
        self._ttl = ttl

    def get(
        self,
        key: _CacheKey,
        /,
    ) -> str | _DeniedAddress | None:
        cached_value = self._cache.get(key)
        if cached_value is None:
            return None

        timestamp, value = cached_value

        current_time = monotonic()
        if current_time - timestamp > self._ttl:
            del self._cache[key]
            return None

        self._cache.move_to_end(key)
        return value

    def put(
        self,
        key: _CacheKey,
        value: str | _DeniedAddress,
        /,
    ) -> None:
        current_time = monotonic()
        cache_hit = key in self._cache
        self._cache[key] = (current_time, value)

        if cache_hit:
            self._cache.move_to_end(key)
        elif len(self._cache) > self._max_size:
            self._cache.popitem(last=False)


class _AddressPolicy:
    __slots__ = (
        '_cache',
        '_locks',
        'check_globally_reachable',
        'custom_validator',
        'happy_eyeballs_delay',
    )

    def __init__(
        self,
        *,
        cache: _DNSCache,
        check_globally_reachable: bool,
        custom_validator: (
            Callable[
                [str, _IPAddress, int],
                bool | Awaitable[bool],
            ]
            | None
        ),
        happy_eyeballs_delay: float,
    ) -> None:
        self._cache = cache
        self._locks: WeakValueDictionary[_CacheKey, Lock] = WeakValueDictionary()
        self.check_globally_reachable = check_globally_reachable
        self.custom_validator = custom_validator
        self.happy_eyeballs_delay = happy_eyeballs_delay

    async def _resolve(
        self,
        hostname: str,
        port: int,
        local_address: str | None,
    ) -> str:
        try:
            return str(ip_address(hostname))
        except ValueError:
            pass

        try:
            stream = await connect_tcp(
                hostname,
                port,
                local_host=local_address,
                happy_eyeballs_delay=self.happy_eyeballs_delay,
            )
            try:
                remote_address = stream.extra(SocketAttribute.remote_address)
                if isinstance(remote_address, str):
                    return remote_address
                return remote_address[0]
            finally:
                with CancelScope(shield=True):
                    await stream.aclose()
        except OSError as e:
            raise ConnectError(str(e)) from e

    async def _validate(
        self,
        ip_str: str,
        port: int,
        hostname: str,
    ) -> None:
        try:
            ip_addr = ip_address(ip_str)
        except ValueError as e:
            raise SSRFProtectionError.invalid_ip(
                ip_str,
                hostname=hostname,
                port=port,
            ) from e

        if self.check_globally_reachable and (
            not ip_addr.is_global
            or (
                isinstance(ip_addr, IPv6Address)
                and ip_addr in _NAT64_WELL_KNOWN_PREFIX
                and not IPv4Address(ip_addr.packed[-4:]).is_global
            )
        ):
            raise SSRFProtectionError.non_global(
                ip_str,
                hostname=hostname,
                ip_addr=ip_addr,
                port=port,
            )

        if self.custom_validator:
            result = self.custom_validator(hostname, ip_addr, port)
            if isawaitable(result):
                result = await result
            if not result:
                raise SSRFProtectionError.custom_validation_failed(
                    ip_str,
                    hostname=hostname,
                    ip_addr=ip_addr,
                    port=port,
                )

    async def admit(
        self,
        hostname: str,
        port: int,
        local_address: str | None,
    ) -> str:
        cache_key = (hostname, port, local_address)
        result = self._cache.get(cache_key)
        if result is not None:
            if isinstance(result, _DeniedAddress):
                raise result.exception()
            return result

        lock = self._locks.get(cache_key)
        if lock is None:
            lock = self._locks[cache_key] = Lock()

        async with lock:
            result = self._cache.get(cache_key)
            if result is not None:
                if isinstance(result, _DeniedAddress):
                    raise result.exception()
                return result

            ip_str = await self._resolve(hostname, port, local_address)

            try:
                await self._validate(ip_str, port, hostname)
            except SSRFProtectionError as e:
                denial = _DeniedAddress.from_exception(e)
                self._cache.put(cache_key, denial)
                raise denial.exception() from None

            self._cache.put(cache_key, ip_str)
            return ip_str


def httpx_ssrf_protection(
    client: _AsyncClientT,
    *,
    check_globally_reachable: bool = True,
    custom_validator: (
        Callable[
            [str, _IPAddress, int],
            bool | Awaitable[bool],
        ]
        | None
    ) = None,
    dns_cache_size: int = 1000,
    dns_cache_ttl: float = 600,
    happy_eyeballs_delay: float = 0.25,
) -> _AsyncClientT:
    """
    Protect new connections of an existing, quiescent HTTPX AsyncClient.

    Standard direct transports and direct mounts are supported. Existing idle
    connections are retired before reuse. URLs, Host headers, TLS names, cookies
    and redirects retain HTTPX's normal origin semantics. Configure the client
    exclusively on its owning async runtime, before sharing it with other tasks.

    Hostname selection may open and close a TCP probe, including to a denied
    address, without sending HTTP or application data. The data-bearing connection
    uses only the selected, validated numeric IP and port. Authorization lasts
    for that connection's lifetime; cache expiry affects new connections only.
    Close and recreate the client to apply changed custom policy immediately.

    Args:
        client: Existing client, with no pending requests or open response streams.
        check_globally_reachable: Block non-global IPs, including non-global IPv4
            destinations embedded in the well-known NAT64 prefix. Defaults to True.
        custom_validator: Optional sync or async function(hostname, ip, port)
            returning bool. It runs after the global check. Required when
            check_globally_reachable is False.
        dns_cache_size: Maximum cached address selections or denials; defaults
            to 1000. Entries distinguish hostname, port and local bind address.
        dns_cache_ttl: Cache lifetime in seconds; defaults to 600. Expiry does
            not revoke established connections.
        happy_eyeballs_delay: Probe connection-attempt delay in seconds;
            defaults to 0.25.
    Returns:
        The same client instance with SSRF protection configured.

    Raises:
        ValueError: No policy is enabled, or a proxy, Unix socket, custom routing,
            custom transport/backend, or unsupported transport layout is present.
        RuntimeError: The client is closed, already protected, or has active or
            pending requests. Finish or close outstanding responses before retrying.

    Requests can raise SSRFProtectionError with the current request, hostname,
    selected IP and port. DNS/connect failures and connect deadlines retain HTTPX's
    ConnectError/ConnectTimeout behavior. Custom-validator exceptions propagate.
    """
    if not check_globally_reachable and custom_validator is None:
        raise ValueError(
            'When check_globally_reachable is False, custom_validator must be provided '
            'to ensure SSRF protection is not completely disabled',
        )

    _protect_client(
        client,
        _AddressPolicy(
            cache=_DNSCache(dns_cache_size, dns_cache_ttl),
            check_globally_reachable=check_globally_reachable,
            custom_validator=custom_validator,
            happy_eyeballs_delay=happy_eyeballs_delay,
        ),
    )
    return client
