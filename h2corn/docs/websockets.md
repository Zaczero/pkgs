# WebSockets

`h2corn` implements WebSockets on both transports the ASGI ecosystem
expects:

- Classic [RFC 6455](https://datatracker.ietf.org/doc/html/rfc6455)
  WebSockets over HTTP/1.1 with the `Upgrade` handshake.
- [RFC 8441](https://datatracker.ietf.org/doc/html/rfc8441)
  "Bootstrapping WebSockets with HTTP/2", using the extended `CONNECT`
  method on a single HTTP/2 stream.

For the application, both look identical: a `websocket` ASGI scope, a
`receive` callable, and a `send` callable. The transport choice is
negotiated by the client and the server with no application code
changes.

## Why HTTP/2 WebSockets matter

On HTTP/2, each WebSocket is a single stream on the shared connection
instead of a hijacked socket. The practical consequences:

- A client can multiplex many WebSockets and ordinary HTTP requests on
  one TCP connection, instead of opening fresh sockets per stream.
- The proxy → app hop stays HTTP/2 the whole time — no
  `Upgrade`/`Connection: keep-alive` interaction with HTTP/1.1.

If your reverse proxy speaks HTTP/2 to `h2corn` (Caddy and HAProxy do —
see [Behind a proxy](deployment/proxy.md)), WebSocket traffic rides the
same `h2c` connection as the rest of the app.

## Limits and keep-alives

The relevant configuration knobs all share a `websocket_*` prefix. Their
full descriptions, defaults, and CLI flags live in the
[Configuration reference](configuration.md):

- `websocket_max_message_size` caps individual frames. The default is
  16 MiB; set it to the literal string `inherit` to follow
  `max_request_body_size`, or `0` to disable the cap entirely.
- `websocket_per_message_deflate` controls whether the server accepts
  the [permessage-deflate](https://datatracker.ietf.org/doc/html/rfc7692)
  compression extension when offered.
- `websocket_ping_interval` and `websocket_ping_timeout` keep idle
  connections alive and detect dead peers. Setting `ping_interval` to
  `0` disables both.

## Example

```python
from fastapi import FastAPI, WebSocket

app = FastAPI()


@app.websocket('/ws')
async def echo(ws: WebSocket):
    await ws.accept()
    try:
        while True:
            message = await ws.receive_text()
            await ws.send_text(f'echo: {message}')
    except Exception:
        await ws.close()
```

```bash
h2corn ws:app --no-http1
```

With `--no-http1`, the server only accepts the HTTP/2 WebSocket
bootstrap. Most browser-side WebSocket clients can use either transport
transparently as long as the proxy advertises HTTP/2.
