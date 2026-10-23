---
description: Host Connect-generated ASGI applications with native gRPC and gRPC-Web on h2corn's HTTP/2 server.
---

# gRPC and Connect

h2corn hosts Connect-generated ASGI applications for the Connect protocol,
native gRPC, and gRPC-Web. h2corn owns HTTP/2 transport; the ASGI application
owns RPC routing, codecs, handlers, and status behavior.

## Host a generated Connect application

Requires CPython 3.11 or newer; see
[Python compatibility and wheels](compatibility.md) for platform support.

Install h2corn and the
[published Connect Python package](https://pypi.org/project/connectrpc/):

```console
pip install h2corn connectrpc "protobuf-py<0.7"
```

ConnectRPC currently requires the pre-0.7 `protobuf-py` module layout.

Generate a Connect ASGI application with the
[Connect Python guide](https://connectrpc.com/docs/python/getting-started/).
Serve that application directly or mount it in an ASGI framework such as
Starlette or FastAPI.

## Enable native gRPC

Native gRPC admission is opt-in. For the CLI, pass
[`--grpc`](configuration.md#option-grpc):

```console title="template"
h2corn myservice.asgi:app --grpc
```

Replace `myservice.asgi:app` with your ASGI application's `module:object`
import target. For embedded servers, set `Config(grpc=True)` on `Server` or
`serve`. The [`grpc` configuration reference](configuration.md#option-grpc)
lists its accepted media types and the other supported input forms.

The setting controls whether native gRPC requests reach the ASGI application;
it does not authenticate clients or restrict network access. Use a dedicated
listener and network policy when native gRPC should be isolated. Connect and
gRPC-Web requests use the ordinary HTTP path and do not need this admission
setting.

## Try a native gRPC round trip

The repository's
[Notebook service](https://github.com/Zaczero/pkgs/blob/main/h2corn/examples/grpc/app.py)
uses generated Connect service code and includes a real native gRPC client
round trip in
[roundtrip.py](https://github.com/Zaczero/pkgs/blob/main/h2corn/examples/grpc/roundtrip.py).
From the `h2corn/` package directory in a source checkout, install the example's
dependencies and run it:

```console
pip install h2corn connectrpc "protobuf-py<0.7" starlette asgi-lifespan
python -m examples.grpc.roundtrip
```
The script binds an ephemeral loopback port, calls the generated service with
Connect's released client configured for native gRPC, checks the returned note,
and shuts the server down. Its output is:

```text
native gRPC response: Welcome
```

## HTTP/2, TLS, and proxies

Native gRPC uses HTTP/2. Use HTTP/2 prior knowledge (`h2c`) for cleartext
native gRPC connections.
For direct TLS, ALPN negotiates `h2`; any HTTP/1.1 fallback is separate and
does not carry native gRPC. For production, terminate TLS in h2corn or at a
reverse proxy. With a proxy, keep HTTP/2 on the proxy-to-h2corn hop over a
private `h2c` connection. The
[proxy guide](deployment/proxy.md) covers that trust boundary and forwarding
headers; [Direct TLS](deployment/tls.md) covers TLS and ALPN in h2corn.

Forwarded client identity is not trusted just because a request contains
forwarding headers. Enable
[`proxy-headers`](configuration.md#option-proxy_headers) only when
[`forwarded-allow-ips`](configuration.md#option-forwarded_allow_ips) limits
trust to known proxy peers.
When a proxy terminates TLS, h2corn sees the cleartext backend connection and
does not expose that edge connection through its ASGI TLS extension.

## Request policies and deadlines

With native gRPC enabled, unmatched gRPC requests bypass the ordinary global
request-body size and body-idle limits. The global HTTP/2 response-stall and
transport write limits remain active. A matching
[`RequestPolicy`](configuration.md#option-request_policies) replaces all three
of its values and can set explicit body-size, body-idle, and response-stall
limits for a service path.

Connect and gRPC-Web requests remain ordinary HTTP and use the global body-size
and body-idle limits unless a request policy matches. A body-idle timeout is
not an RPC deadline; it bounds time between request-body data. The HTTP/2
response-stall timeout bounds flow-control stalls, while the transport write
timeout bounds pending frame output and flushing. See the canonical entries for
[`max-request-body-size`](configuration.md#option-max_request_body_size),
[`timeout-request-body-idle`](configuration.md#option-timeout_request_body_idle),
[`h2-timeout-response-stall`](configuration.md#option-h2_timeout_response_stall),
and [`h2-timeout-write`](configuration.md#option-h2_timeout_write).
