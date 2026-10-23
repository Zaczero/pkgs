---
description: Public Python API reference for h2corn.
---

# API reference

Use [`serve()`][h2corn.serve] with a [`Config`][h2corn.Config] for the standard
server entrypoint, or [`Server`][h2corn.Server] to embed one worker in an
existing event loop.

| Page | Use it for |
| --- | --- |
| [Server entrypoints](server.md) | Start a standalone server or embed one in an event loop. |
| [Configuration types](config.md) | Construct `Config`, `RequestPolicy`, and option values. |
| [ASGI applications and aliases](applications.md) | Annotate applications, factories, and send/receive callables. |
| [Scopes and extensions](scopes.md) | Work with headers, state, protocol versions, and extension metadata. |
| [HTTP scope and messages](http.md) | Handle requests, responses, trailers, and file sending. |
| [WebSocket scope and messages](websockets.md) | Accept connections, exchange messages, and close sessions. |
| [Lifespan scope and messages](lifespan.md) | Handle application startup and shutdown. |

For per-option descriptions, defaults, environment variables, and CLI
flags, see the [Configuration reference](../configuration.md#option-index).
