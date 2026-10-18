# httpx-secure

<a id="why-use-this"></a>

Block requests to private and non-global IP addresses in an existing HTTPX
`AsyncClient`. Add your own destination rules without changing HTTPX's request
API. Python 3.11+, with asyncio or Trio.

## Installation

```sh
pip install httpx-secure
```

HTTP/2 remains optional: install `httpx[http2]` when configuring an HTTP/2 client.

## Quick Start

Configure a direct client before making requests:

```python
import asyncio
import httpx
from httpx_secure import SSRFProtectionError, httpx_ssrf_protection

async def main():
    async with httpx.AsyncClient(trust_env=False) as client:
        httpx_ssrf_protection(client)
        try:
            await client.get("http://127.0.0.1/private")
        except SSRFProtectionError as error:
            print(f"Blocked: {error.hostname}")

asyncio.run(main())  # Blocked: 127.0.0.1
```

The returned object is the same client. Keep using its ordinary `get`, `post`,
`send`, `stream` and async context-manager interfaces. HTTPX continues to own
timeouts, authentication, TLS configuration and response streaming.
See [connection authorization](#connection-authorization) for the policy boundary.

## Custom Validation

A validator receives the logical hostname, selected `IPv4Address` or
`IPv6Address`, and effective destination port. It may return a bool directly or
asynchronously. The global-address check runs first and cannot be overridden by
an accepting custom validator.

For example, restrict connections to port 443:

```python
import asyncio
import httpx
from httpx_secure import httpx_ssrf_protection

def https_port_only(hostname, address, port):
    return port == 443

async def main():
    async with httpx.AsyncClient(trust_env=False) as client:
        httpx_ssrf_protection(client, custom_validator=https_port_only)
        # Make requests with client here.

asyncio.run(main())
```

Set `check_globally_reachable=False` only when your custom validator owns the
entire destination policy, such as explicitly permitting a particular internal
service. A custom validator is required in that configuration.

## Supported Clients

Call the helper while exclusively configuring the client on its owning async
runtime. A newly created client or a client inside its context manager is
supported. Already-used clients are supported when every response has finished
or been closed and no requests remain active or queued.

The helper retains the existing client, standard `AsyncHTTPTransport` instances,
connection pools, SSL contexts, HTTP-version settings, limits and direct mounts.
Existing idle HTTP/1.1 and HTTP/2 connections expire before reuse. Their pools close
them on the next request or during client closure.

Setup fails before changing any route when the client has:

- HTTP, HTTPS or SOCKS proxy routes, including environment-created proxy mounts;
- Unix-domain sockets, custom transports or custom network backends;
- custom client routing instead of HTTPX's normal transport selection;
- active requests, pending requests or open streamed responses; or
- unsupported transport/connection state.

For an intended direct client, `trust_env=False` avoids environment-created proxy
routes. `trust_env=True` is supported when no proxy route is actually configured;
existing environment-derived SSL trust is retained. The helper does not remove
proxies or silently fall back to weaker protection for unsupported routes.

Sharing a transport between clients also shares its connector policy. Configure
such transports exclusively; do not mutate routing or replace transports after
installation. Async task sharing after setup follows HTTPX's normal client
lifecycle. Cross-thread or cross-event-loop configuration is not supported.

<a id="how-it-works"></a>

## Connection Authorization

For a new connection, the connector selects an IP, applies the global-address and
custom checks, and opens the data-bearing connection to that exact numeric IP
and port. HTTPX and HTTPcore retain the logical origin for pooling, TLS certificate
verification/SNI, redirects, cookies and Host authority. Absolute redirects that
need another connection receive that destination's policy check.

**Hostname selection can open and close plain TCP probes, including to a denied
address. Probes send no HTTP or application data.** This package does not promise
zero TCP contact with denied addresses. Literal IPs do not need a selection probe.
A failed validated-IP connection does not fall back to an unvalidated hostname.

Authorization lasts for the established connection's lifetime. The bounded
address-selection cache stores approved selections and denials; expiry affects
new connections, not requests on an established socket. To apply a changed custom
policy immediately, close the client and create/configure another client.

The default check uses Python's `ipaddress.is_global` classification and also
blocks non-global IPv4 addresses embedded in the well-known NAT64 prefix. It is
an endpoint-IP policy, not a network firewall or a policy for what an authorized
remote application may do. Caller-supplied Host/SNI overrides and the client's
trusted routing configuration remain the caller's responsibility.

## API Reference

### httpx_ssrf_protection

Accepts an existing `httpx.AsyncClient` and returns that **same instance**.
See [the function reference](https://github.com/Zaczero/pkgs/blob/main/httpx-secure/httpx_secure/__init__.py)
or `help(httpx_ssrf_protection)` for its signature and defaults.

| Parameter | Meaning |
| --- | --- |
| `client` | Quiescent existing client with supported direct routes. |
| `check_globally_reachable` | Apply the global-IP and NAT64 checks. |
| `custom_validator` | Optional sync/async `(hostname, address, port) -> bool` policy; required when the global check is disabled. |
| `dns_cache_size` | Maximum cached selections/denials; zero disables retention. Keys distinguish hostname, port and local bind address. |
| `dns_cache_ttl` | Cache lifetime in seconds, applied to new connections. |
| `happy_eyeballs_delay` | Seconds between hostname-selection probe attempts. |

The current request's connect timeout bounds cache-lock waiting, probe,
asynchronous validation and final TCP connection together. TLS, pool, read and
write timeouts retain HTTPX/HTTPcore's separate phase semantics. Custom-validator
exceptions propagate; temporary connection failures and cancellation are not
cached as denials.

`TypeError` reports a non-`AsyncClient` or non-callable validator. `ValueError`
reports unsupported routing/configuration or a completely disabled policy.
`RuntimeError` reports a closed/already-protected client or setup attempted during
active use. Finish or close outstanding responses before retrying setup.

### SSRFProtectionError

An HTTPX `RequestError` raised when the selected destination is denied. Each
failure is a fresh exception with the current `request`, logical `hostname`,
selected `ip_addr` (`IPv4Address`, `IPv6Address`, or None for an invalid address)
and effective `port`.

Its `invalid_ip`, `non_global` and `custom_validation_failed` classmethods construct
the corresponding unbound errors; requests through HTTPX bind them to the current
request. DNS/connection failures use HTTPX `ConnectError`, and connect deadlines
use `ConnectTimeout`. Catch those separately when deciding whether to retry.

## Support

[Issues and questions](https://github.com/Zaczero/pkgs/issues) ·
[Private security reports](https://github.com/Zaczero/pkgs/security/advisories/new) ·
[Premium support](https://monicz.dev/#get-in-touch)
