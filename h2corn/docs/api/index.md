---
description: Public Python API reference for h2corn.
---

# API reference

Use [`serve()`][h2corn.serve] with a [`Config`][h2corn.Config] for the standard
server entrypoint, or [`Server`][h2corn.Server] to embed one worker in an
existing event loop. The reference is grouped by the ASGI concept a symbol
describes; each page contains the complete signature and docstring.

::: h2corn
    options:
      show_root_heading: false
      show_root_toc_entry: false
      members: false

| Page | Public symbols |
| --- | --- |
| [Server entrypoints](server.md) | [`serve`][h2corn.serve], [`Server`][h2corn.Server] |
| [Configuration types](config.md) | [`Config`][h2corn.Config], [`ProxyProtocolMode`][h2corn.ProxyProtocolMode], [`LifespanMode`][h2corn.LifespanMode], [`CertReqsMode`][h2corn.CertReqsMode], [`LoopImpl`][h2corn.LoopImpl], [`ServerHeaderMode`][h2corn.ServerHeaderMode], [`LogFormat`][h2corn.LogFormat] |
| [ASGI applications and aliases](applications.md) | [`ASGIApp`][h2corn.ASGIApp], [`FrameworkASGIApp`][h2corn.FrameworkASGIApp], [`Application`][h2corn.Application], [`Receive`][h2corn.Receive], [`Send`][h2corn.Send], [`Scope`][h2corn.Scope], [`ReceiveMessage`][h2corn.ReceiveMessage], [`SendMessage`][h2corn.SendMessage], [`Message`][h2corn.Message] |
| [Scopes and extensions](scopes.md) | [`ASGIVersions`][h2corn.ASGIVersions], [`HTTPASGIVersions`][h2corn.HTTPASGIVersions], [`LifespanASGIVersions`][h2corn.LifespanASGIVersions], [`Headers`][h2corn.Headers], [`ScopeHeaders`][h2corn.ScopeHeaders], [`State`][h2corn.State], [`Extensions`][h2corn.Extensions], [`HTTPExtensions`][h2corn.HTTPExtensions], [`WebSocketExtensions`][h2corn.WebSocketExtensions], [`ExtensionParameters`][h2corn.ExtensionParameters], [`TLSExtension`][h2corn.TLSExtension] |
| [HTTP scope and messages](http.md) | [`HTTPScope`][h2corn.HTTPScope], [`HTTPRequest`][h2corn.HTTPRequest], [`HTTPDisconnect`][h2corn.HTTPDisconnect], [`HTTPResponseStart`][h2corn.HTTPResponseStart], [`HTTPResponseBody`][h2corn.HTTPResponseBody], [`HTTPResponseTrailers`][h2corn.HTTPResponseTrailers], [`HTTPResponsePathsend`][h2corn.HTTPResponsePathsend], [`HTTPResponseZeroCopySend`][h2corn.HTTPResponseZeroCopySend], [`HTTPResponseEarlyHint`][h2corn.HTTPResponseEarlyHint] |
| [WebSocket scope and messages](websockets.md) | [`WebSocketScope`][h2corn.WebSocketScope], [`WebSocketConnect`][h2corn.WebSocketConnect], [`WebSocketReceiveBytes`][h2corn.WebSocketReceiveBytes], [`WebSocketReceiveText`][h2corn.WebSocketReceiveText], [`WebSocketDisconnect`][h2corn.WebSocketDisconnect], [`WebSocketAccept`][h2corn.WebSocketAccept], [`WebSocketSendBytes`][h2corn.WebSocketSendBytes], [`WebSocketSendText`][h2corn.WebSocketSendText], [`WebSocketClose`][h2corn.WebSocketClose], [`WebSocketHTTPResponseStart`][h2corn.WebSocketHTTPResponseStart], [`WebSocketHTTPResponseBody`][h2corn.WebSocketHTTPResponseBody] |
| [Lifespan scope and messages](lifespan.md) | [`LifespanScope`][h2corn.LifespanScope], [`LifespanStartup`][h2corn.LifespanStartup], [`LifespanShutdown`][h2corn.LifespanShutdown], [`LifespanStartupComplete`][h2corn.LifespanStartupComplete], [`LifespanStartupFailed`][h2corn.LifespanStartupFailed], [`LifespanShutdownComplete`][h2corn.LifespanShutdownComplete], [`LifespanShutdownFailed`][h2corn.LifespanShutdownFailed] |

For per-option descriptions, defaults, environment variables, and CLI
flags, see the [Configuration reference](../configuration.md#option-index).
