"""
High-performance HTTP/2 ASGI server for FastAPI, Starlette, and similar apps.

`h2corn` is optimized for `h2c` behind a trusted reverse proxy, with optional
direct TLS support for TCP deployments. Application traffic stays on HTTP/2
end-to-end instead of being downgraded to HTTP/1.1 inside the trust boundary.

The two entrypoints are:

- [`serve`][h2corn.serve] — start the server through the multi-worker supervisor
  (Unix) or in-process (Windows). This matches the behavior of the `h2corn` CLI.
- [`Server`][h2corn.Server] — embed a single-worker server in your own event loop.

Configuration is provided through [`Config`][h2corn.Config], which is also
constructable from environment variables or a TOML file.
"""

from typing import TYPE_CHECKING as _TYPE_CHECKING
from typing import Any as _Any

from ._config import CertReqsMode, Config, LifespanMode, LoopImpl, ProxyProtocolMode

if _TYPE_CHECKING:
    from ._server import Server, serve
    from ._types import (
        ASGIApp,
        ASGIVersions,
        ExtensionParameters,
        Extensions,
        FrameworkASGIApp,
        Headers,
        HTTPASGIVersions,
        HTTPDisconnect,
        HTTPExtensions,
        HTTPRequest,
        HTTPResponseBody,
        HTTPResponsePathsend,
        HTTPResponseStart,
        HTTPResponseTrailers,
        HTTPScope,
        LifespanASGIVersions,
        LifespanScope,
        LifespanShutdown,
        LifespanShutdownComplete,
        LifespanShutdownFailed,
        LifespanStartup,
        LifespanStartupComplete,
        LifespanStartupFailed,
        Message,
        Receive,
        ReceiveMessage,
        Scope,
        Send,
        SendMessage,
        State,
        WebSocketAccept,
        WebSocketClose,
        WebSocketConnect,
        WebSocketDisconnect,
        WebSocketExtensions,
        WebSocketHTTPResponseBody,
        WebSocketHTTPResponseStart,
        WebSocketReceiveBytes,
        WebSocketReceiveText,
        WebSocketScope,
        WebSocketSendBytes,
        WebSocketSendText,
    )

__all__ = (
    'ASGIApp',
    'ASGIVersions',
    'CertReqsMode',
    'Config',
    'ExtensionParameters',
    'Extensions',
    'FrameworkASGIApp',
    'HTTPASGIVersions',
    'HTTPDisconnect',
    'HTTPExtensions',
    'HTTPRequest',
    'HTTPResponseBody',
    'HTTPResponsePathsend',
    'HTTPResponseStart',
    'HTTPResponseTrailers',
    'HTTPScope',
    'Headers',
    'LifespanASGIVersions',
    'LifespanMode',
    'LifespanScope',
    'LifespanShutdown',
    'LifespanShutdownComplete',
    'LifespanShutdownFailed',
    'LifespanStartup',
    'LifespanStartupComplete',
    'LifespanStartupFailed',
    'LoopImpl',
    'Message',
    'ProxyProtocolMode',
    'Receive',
    'ReceiveMessage',
    'Scope',
    'Send',
    'SendMessage',
    'Server',
    'State',
    'WebSocketAccept',
    'WebSocketClose',
    'WebSocketConnect',
    'WebSocketDisconnect',
    'WebSocketExtensions',
    'WebSocketHTTPResponseBody',
    'WebSocketHTTPResponseStart',
    'WebSocketReceiveBytes',
    'WebSocketReceiveText',
    'WebSocketScope',
    'WebSocketSendBytes',
    'WebSocketSendText',
    'serve',
)


def __getattr__(name: str) -> _Any:
    if name in {'Server', 'serve'}:
        from . import _server as module
    elif name in __all__:
        from . import _types as module
    else:
        raise AttributeError(f'module {__name__!r} has no attribute {name!r}')

    value = getattr(module, name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted({*globals(), *__all__})
