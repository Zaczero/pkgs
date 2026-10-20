"""
Blazing-fast HTTP/2 ASGI server for Python, written in Rust.

`h2corn` accepts HTTP/1.1 and cleartext HTTP/2 (`h2c`) and can terminate TLS
directly. Keep cleartext `h2c` connections within a private network.

The two entrypoints are:

- [`serve`][h2corn.serve] — start the server through the multi-worker supervisor
  (Unix) or in-process (Windows). This matches the behavior of the `h2corn` CLI.
- [`Server`][h2corn.Server] — embed a single-worker server in your own event loop.

Configuration is provided through [`Config`][h2corn.Config], which is also
constructable from environment variables or a TOML file.
"""

from typing import TYPE_CHECKING as _TYPE_CHECKING
from typing import Any as _Any

from h2corn._config import (
    CertReqsMode,
    Config,
    LifespanMode,
    LoopImpl,
    ProxyProtocolMode,
    RequestPolicy,
    ServerHeaderMode,
)
from h2corn._log import LogFormat

if _TYPE_CHECKING:
    from h2corn._server import Server, serve
    from h2corn._types import (
        Application,
        ApplicationFactory,
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
        HTTPResponseEarlyHint,
        HTTPResponsePathsend,
        HTTPResponseStart,
        HTTPResponseTrailers,
        HTTPResponseZeroCopySend,
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
        ScopeHeaders,
        Send,
        SendMessage,
        State,
        TLSExtension,
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
    'Application',
    'ApplicationFactory',
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
    'HTTPResponseEarlyHint',
    'HTTPResponsePathsend',
    'HTTPResponseStart',
    'HTTPResponseTrailers',
    'HTTPResponseZeroCopySend',
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
    'LogFormat',
    'LoopImpl',
    'Message',
    'ProxyProtocolMode',
    'Receive',
    'ReceiveMessage',
    'RequestPolicy',
    'Scope',
    'ScopeHeaders',
    'Send',
    'SendMessage',
    'Server',
    'ServerHeaderMode',
    'State',
    'TLSExtension',
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
        import h2corn._server as server_module

        value = getattr(server_module, name)
    elif name in __all__:
        import h2corn._types as types_module

        value = getattr(types_module, name)
    else:
        raise AttributeError(f'module {__name__!r} has no attribute {name!r}')

    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted({*globals(), *__all__})
