---
description: Public Python API reference for h2corn.
---

# API reference

Use [`serve()`][h2corn.serve] with a [`Config`][h2corn.Config] for the standard
server entrypoint, or [`Server`][h2corn.Server] to embed one worker in an
existing event loop.

::: h2corn
    options:
      show_root_heading: false
      show_root_toc_entry: false
      members: false

| Symbol                          | What it is                                                                  |
| ------------------------------- | --------------------------------------------------------------------------- |
| [`serve`][h2corn.serve]         | Start the server through the multi-worker supervisor (or in-process on Windows). |
| [`Server`][h2corn.Server]       | Embed a single-worker server in your own event loop.                        |
| [`Config`][h2corn.Config]       | Frozen dataclass holding every server option.                               |
| [`ProxyProtocolMode`][h2corn.ProxyProtocolMode] | Literal type for the `proxy_protocol` option.               |
| [`LifespanMode`][h2corn.LifespanMode] | Literal type for the `lifespan` option.                               |
| [`CertReqsMode`][h2corn.CertReqsMode] | Literal type for the `cert_reqs` option.                              |
| [`LoopImpl`][h2corn.LoopImpl]   | Literal type for the `loop` option.                                          |
| [`ServerHeaderMode`][h2corn.ServerHeaderMode] | Literal type for the `server_header` option.                   |
| [`ASGIApp`][h2corn.ASGIApp]     | Precisely typed callable alias for an ASGI 3 application.                    |
| [`FrameworkASGIApp`][h2corn.FrameworkASGIApp] | Compatibility alias for broadly typed framework applications. |
| [`Scope`][h2corn.Scope]         | Discriminated union of h2corn's HTTP, WebSocket, and lifespan scopes.        |
| [`ReceiveMessage`][h2corn.ReceiveMessage] | Discriminated union of ASGI events received by an application.    |
| [`SendMessage`][h2corn.SendMessage] | Discriminated union of ASGI events sent by an application.              |

For per-option descriptions, defaults, environment variables, and CLI
flags, see the [Configuration reference](../configuration.md).
