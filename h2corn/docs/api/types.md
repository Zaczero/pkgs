# Types

::: h2corn.ASGIApp
    options:
      show_signature: false

::: h2corn.FrameworkASGIApp
    options:
      show_signature: false

[`ASGIApp`][h2corn.ASGIApp] uses h2corn's precise, discriminated scope and
message unions. [`FrameworkASGIApp`][h2corn.FrameworkASGIApp] matches the
broader mutable-mapping annotations used by FastAPI, Starlette, Django, and
similar frameworks; both are accepted anywhere h2corn accepts an application.

::: h2corn.Scope
    options:
      show_signature: false

::: h2corn.ReceiveMessage
    options:
      show_signature: false

::: h2corn.SendMessage
    options:
      show_signature: false

| Type | Contents |
| ---- | -------- |
| `Scope` | `HTTPScope`, `WebSocketScope`, or `LifespanScope` |
| `ASGIVersions` | HTTP/WebSocket 2.5 or lifespan 2.0 ASGI metadata |
| `ReceiveMessage` | HTTP request/disconnect, WebSocket connect/receive/disconnect, and lifespan startup/shutdown events |
| `SendMessage` | HTTP response, WebSocket response/send/close, and lifespan completion/failure events |
| `Message` | Any receive or send event |
| `Receive` | Precisely typed ASGI receive callable |
| `Send` | Precisely typed ASGI send callable |
| `Headers`, `State` | Reusable aliases for iterable headers and lifespan/request state |
| `HTTPExtensions`, `WebSocketExtensions` | Advertised h2corn capabilities, with open-ended `ExtensionParameters` payloads |

The individual event types are exported as well. Their names follow their ASGI
events: `HTTPRequest`, `HTTPDisconnect`, `HTTPResponseStart`,
`HTTPResponseBody`, `HTTPResponseTrailers`, `HTTPResponsePathsend`,
`WebSocketConnect`, `WebSocketReceiveBytes`, `WebSocketReceiveText`,
`WebSocketDisconnect`, `WebSocketAccept`, `WebSocketSendBytes`,
`WebSocketSendText`, `WebSocketClose`, `WebSocketHTTPResponseStart`,
`WebSocketHTTPResponseBody`, and the `LifespanStartup*`/`LifespanShutdown*`
types. This makes event construction and pattern matching visible to editor and
type-checker tooling without forcing framework internals into narrower
annotations.

```python
from h2corn import ASGIApp, Receive, ReceiveMessage, Scope, Send


def request_body(message: ReceiveMessage) -> bytes:
    if message["type"] == "http.request":
        return message.get("body", b"")
    return b""


async def app(scope: Scope, receive: Receive, send: Send) -> None:
    ...


typed_app: ASGIApp = app
```

The wire contracts follow the ASGI
[HTTP and WebSocket](https://asgi.readthedocs.io/en/latest/specs/www.html) and
[lifespan](https://asgi.readthedocs.io/en/latest/specs/lifespan.html)
specifications.
`h2corn` accepts callables matching either `ASGIApp` or `FrameworkASGIApp`,
including
[FastAPI](https://fastapi.tiangolo.com/),
[Starlette](https://www.starlette.io/),
[Django](https://docs.djangoproject.com/en/stable/howto/deployment/asgi/) (`asgi.application`),
[Litestar](https://litestar.dev/), and
[Quart](https://quart.palletsprojects.com/).
