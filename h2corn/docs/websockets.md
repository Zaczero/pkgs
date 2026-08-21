---
description: WebSockets over HTTP/1.1 and over HTTP/2 (RFC 8441), the limits that apply, and what a proxy has to do.
---

# WebSockets

`h2corn` implements WebSockets on both transports the ASGI ecosystem
expects:

- Classic [RFC 6455](https://datatracker.ietf.org/doc/html/rfc6455)
  WebSockets over HTTP/1.1 with the `Upgrade` handshake.
- [RFC 8441](https://datatracker.ietf.org/doc/html/rfc8441)
  "Bootstrapping WebSockets with HTTP/2", using the extended `CONNECT`
  method on a single HTTP/2 stream.

To the application, both look identical: a `websocket` ASGI scope, a
`receive` callable, and a `send` callable. The transport is negotiated
between client and server with no application code changes.

## Why the HTTP/2 transport matters

On HTTP/2, each WebSocket is a stream on the shared connection rather
than a hijacked socket. Two practical consequences:

- A client can multiplex many WebSockets and ordinary HTTP requests on
  one TCP connection, instead of opening a fresh socket per stream.
- The proxy → app hop stays on HTTP/2 the whole time — no
  `Upgrade`/`Connection: keep-alive` interaction with HTTP/1.1.

When a reverse proxy translates the browser's `Upgrade` into RFC 8441 extended
`CONNECT`, WebSocket traffic can ride the same `h2c` connection as the rest of
the application. See [Behind a reverse proxy](deployment/proxy.md) for the
HTTP/1.1 split route and the pure HTTP/2 requirements.

## Limits and keep-alives

The relevant configuration knobs share a `websocket_*` prefix; full
descriptions, defaults, and CLI flags live in the
[Configuration reference](configuration.md):

- `websocket_max_message_size` caps a reassembled message, not one
  frame: fragments are accounted as they arrive, and a compressed
  message is measured after decompression. The default is 16 MiB; set it
  to `0` to remove the cap entirely.
- `websocket_per_message_deflate` controls whether the server accepts
  the [permessage-deflate](https://datatracker.ietf.org/doc/html/rfc7692)
  compression extension when a client offers it.
- `websocket_ping_interval` and `websocket_ping_timeout` keep idle
  connections alive and detect dead peers. Set `websocket_ping_interval`
  to `0` to turn keep-alive off entirely — that is the single off switch;
  no pings are sent, and the timeout is irrelevant while keep-alive is
  unset.

## Echo application

```python title="ws.py"
from fastapi import FastAPI, WebSocket
from starlette.websockets import WebSocketDisconnect

app = FastAPI()


@app.websocket('/ws')
async def echo(ws: WebSocket):
    await ws.accept()
    try:
        while True:
            message = await ws.receive_text()
            await ws.send_text(f'echo: {message}')
    except WebSocketDisconnect:
        pass
```

Once the peer is gone, `send()` raises `OSError` — see
[What `send()` raises](asgi.md#what-send-raises). Starlette surfaces that as
`WebSocketDisconnect`.

## HTTP/2-only deployment

```bash title="development command"
h2corn ws:app --no-http1
```

With `--no-http1`, the server only accepts the HTTP/2 WebSocket
bootstrap — the [RFC 8441](https://datatracker.ietf.org/doc/html/rfc8441)
extended `CONNECT`. Browser WebSocket APIs and the reverse proxy need support
for the upgrade translation. If the proxy does not provide it, route WebSocket
upgrades over HTTP/1.1 and ordinary requests over `h2c`, and keep HTTP/1
enabled there. See [Behind a reverse
proxy](deployment/proxy.md) before dropping HTTP/1.
