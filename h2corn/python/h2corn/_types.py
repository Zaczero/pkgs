from collections.abc import Awaitable, Callable, Iterable, Mapping, MutableMapping
from typing import Any, Literal, NotRequired, Protocol, TypedDict

HeaderPair = tuple[bytes, bytes]
Headers = Iterable[HeaderPair]
ScopeHeaders = list[HeaderPair]
State = dict[str, Any]
#: An extension's parameters, as the server publishes them.
#:
#: `Mapping`, not `dict`, and deliberately: a parameterless extension's value is
#: one object shared by every scope, so writing to it would be visible to every
#: later request. Nothing writes to capability metadata, so the runtime value
#: stays a plain `dict` -- a read-only view would cost an indirection on every
#: read to defend against something no framework does. Saying `Mapping` here
#: costs nothing and lets a type checker reject the write instead.
ExtensionParameters = Mapping[str, Any]


class TLSExtension(TypedDict):
    """
    The negotiated TLS parameters, under `scope["extensions"]["tls"]`.

    Present only on connections h2corn terminated itself; a connection
    arriving through a TLS-terminating proxy has no `tls` key at all.

    `client_cert_chain` holds the certificates the peer presented, in PEM,
    leaf first — empty unless `cert_reqs` asked for one. When the chain is
    nonempty, `client_cert_name` is the leaf subject as an RFC 4514 string.
    `client_cert_error` is always `None`: a certificate that fails verification
    fails the handshake and never reaches an application.
    """

    server_cert: str | None
    client_cert_chain: tuple[str, ...]
    client_cert_name: str | None
    client_cert_error: str | None
    tls_version: int | None
    cipher_suite: int | None


# Functional syntax preserves the ASGI extension names verbatim; dotted keys
# cannot be declared with class-syntax TypedDict fields.
HTTPExtensions = TypedDict(
    'HTTPExtensions',
    {
        'http.response.pathsend': ExtensionParameters,
        # Unix only: the descriptor handling it needs has no Windows
        # equivalent, so the key is absent there rather than advertising a
        # capability the server was not built with.
        'http.response.zerocopysend': NotRequired[ExtensionParameters],
        'http.response.trailers': NotRequired[ExtensionParameters],
        # HTTP/2 only -- RFC 8297 interim responses are not offered on HTTP/1.
        'http.response.early_hint': NotRequired[ExtensionParameters],
        'tls': NotRequired[TLSExtension],
    },
)
WebSocketExtensions = TypedDict(
    'WebSocketExtensions',
    {
        'websocket.http.response': ExtensionParameters,
        'tls': NotRequired[TLSExtension],
    },
)
Extensions = HTTPExtensions | WebSocketExtensions


class HTTPASGIVersions(TypedDict):
    version: Literal['3.0']
    spec_version: Literal['2.5']


class LifespanASGIVersions(TypedDict):
    version: Literal['3.0']
    spec_version: Literal['2.0']


ASGIVersions = HTTPASGIVersions | LifespanASGIVersions


class HTTPScope(TypedDict):
    """An HTTP request scope in [`Scope`][h2corn.Scope]."""

    type: Literal['http']
    asgi: HTTPASGIVersions
    http_version: Literal['1.1', '2']
    method: str
    scheme: str
    path: str
    raw_path: bytes
    query_string: bytes
    root_path: NotRequired[str]
    headers: ScopeHeaders
    client: NotRequired[tuple[str, int]]
    server: tuple[str, int | None]
    state: NotRequired[State]
    extensions: HTTPExtensions


class WebSocketScope(TypedDict):
    """A WebSocket scope in [`Scope`][h2corn.Scope]."""

    type: Literal['websocket']
    asgi: HTTPASGIVersions
    http_version: Literal['1.1', '2']
    scheme: str
    path: str
    raw_path: bytes
    query_string: bytes
    root_path: NotRequired[str]
    headers: ScopeHeaders
    client: NotRequired[tuple[str, int]]
    server: tuple[str, int | None]
    subprotocols: list[str]
    state: NotRequired[State]
    extensions: WebSocketExtensions


class LifespanScope(TypedDict):
    """A lifespan scope in [`Scope`][h2corn.Scope]."""

    type: Literal['lifespan']
    asgi: LifespanASGIVersions
    state: State


#: A request scope, one of [`HTTPScope`][h2corn.HTTPScope],
#: [`WebSocketScope`][h2corn.WebSocketScope], or
#: [`LifespanScope`][h2corn.LifespanScope].
Scope = HTTPScope | WebSocketScope | LifespanScope


class HTTPRequest(TypedDict):
    """An HTTP request event in [`ReceiveMessage`][h2corn.ReceiveMessage]."""

    type: Literal['http.request']
    body: NotRequired[bytes]
    more_body: NotRequired[bool]


class HTTPDisconnect(TypedDict):
    """An HTTP disconnect event in [`ReceiveMessage`][h2corn.ReceiveMessage]."""

    type: Literal['http.disconnect']


class WebSocketConnect(TypedDict):
    """A WebSocket connect event in [`ReceiveMessage`][h2corn.ReceiveMessage]."""

    type: Literal['websocket.connect']


class WebSocketReceiveBytes(TypedDict):
    """A binary WebSocket event in [`ReceiveMessage`][h2corn.ReceiveMessage]."""

    type: Literal['websocket.receive']
    bytes: bytes
    text: NotRequired[None]


class WebSocketReceiveText(TypedDict):
    """A text WebSocket event in [`ReceiveMessage`][h2corn.ReceiveMessage]."""

    type: Literal['websocket.receive']
    text: str
    bytes: NotRequired[None]


class WebSocketDisconnect(TypedDict):
    """A WebSocket disconnect event in [`ReceiveMessage`][h2corn.ReceiveMessage]."""

    type: Literal['websocket.disconnect']
    code: int
    reason: NotRequired[str]


class LifespanStartup(TypedDict):
    """A lifespan startup event in [`ReceiveMessage`][h2corn.ReceiveMessage]."""

    type: Literal['lifespan.startup']


class LifespanShutdown(TypedDict):
    """A lifespan shutdown event in [`ReceiveMessage`][h2corn.ReceiveMessage]."""

    type: Literal['lifespan.shutdown']


#: An incoming ASGI message, one of [`HTTPRequest`][h2corn.HTTPRequest],
#: [`HTTPDisconnect`][h2corn.HTTPDisconnect],
#: [`WebSocketConnect`][h2corn.WebSocketConnect],
#: [`WebSocketReceiveBytes`][h2corn.WebSocketReceiveBytes],
#: [`WebSocketReceiveText`][h2corn.WebSocketReceiveText],
#: [`WebSocketDisconnect`][h2corn.WebSocketDisconnect],
#: [`LifespanStartup`][h2corn.LifespanStartup], or
#: [`LifespanShutdown`][h2corn.LifespanShutdown]. [`Message`][h2corn.Message]
#: combines these with outgoing messages.
ReceiveMessage = (
    HTTPRequest
    | HTTPDisconnect
    | WebSocketConnect
    | WebSocketReceiveBytes
    | WebSocketReceiveText
    | WebSocketDisconnect
    | LifespanStartup
    | LifespanShutdown
)


class HTTPResponseStart(TypedDict):
    """An HTTP response-start event in [`SendMessage`][h2corn.SendMessage]."""

    type: Literal['http.response.start']
    status: int
    headers: NotRequired[Headers]
    trailers: NotRequired[bool]


class HTTPResponseBody(TypedDict):
    """An HTTP response-body event in [`SendMessage`][h2corn.SendMessage]."""

    type: Literal['http.response.body']
    body: NotRequired[bytes]
    more_body: NotRequired[bool]


class HTTPResponseTrailers(TypedDict):
    """An HTTP response-trailers event in [`SendMessage`][h2corn.SendMessage]."""

    type: Literal['http.response.trailers']
    headers: Headers
    more_trailers: NotRequired[bool]


class SupportsFileno(Protocol):
    """An object naming an OS descriptor, per the ASGI specification's
    "opened file descriptor object".

    Structural on purpose, so an open file and an application's own wrapper
    both qualify without inheriting anything. Note that satisfying this
    protocol is necessary but not sufficient: at runtime the descriptor must
    name a *readable regular file*, so a socket -- which has `fileno()` -- is
    rejected.
    """

    def fileno(self) -> int: ...


class HTTPResponsePathsend(TypedDict):
    """An HTTP path-send event in [`SendMessage`][h2corn.SendMessage]."""

    type: Literal['http.response.pathsend']
    path: str


class HTTPResponseZeroCopySend(TypedDict):
    """A response body segment in [`SendMessage`][h2corn.SendMessage], read straight from an open file.

    Sent after `http.response.start`, as many times as wanted, and freely
    interleaved with `http.response.body`. `offset` defaults to the
    descriptor's current position and `count` to the rest of the file.

    The descriptor stays yours: this server duplicates it and closes only the
    duplicate, so closing it as soon as `send()` returns is safe -- and, per the
    ASGI specification, your responsibility.
    """

    type: Literal['http.response.zerocopysend']
    file: SupportsFileno
    offset: NotRequired[int]
    count: NotRequired[int]
    more_body: NotRequired[bool]


class HTTPResponseEarlyHint(TypedDict):
    """An RFC 8297 `103 Early Hints` interim response in [`SendMessage`][h2corn.SendMessage].

    Sent after `http.response.start` and before the final body, as many times
    as wanted. Each value becomes one `link` field; ordering and duplicates are
    preserved. Offered only on HTTP/2 -- check `scope["extensions"]`.
    """

    type: Literal['http.response.early_hint']
    links: Iterable[bytes]


class WebSocketAccept(TypedDict):
    """A WebSocket accept event in [`SendMessage`][h2corn.SendMessage]."""

    type: Literal['websocket.accept']
    subprotocol: NotRequired[str | None]
    headers: NotRequired[Headers]


class WebSocketSendBytes(TypedDict):
    """A binary WebSocket event in [`SendMessage`][h2corn.SendMessage]."""

    type: Literal['websocket.send']
    bytes: bytes
    text: NotRequired[None]


class WebSocketSendText(TypedDict):
    """A text WebSocket event in [`SendMessage`][h2corn.SendMessage]."""

    type: Literal['websocket.send']
    text: str
    bytes: NotRequired[None]


class WebSocketClose(TypedDict):
    """A WebSocket close event in [`SendMessage`][h2corn.SendMessage]."""

    type: Literal['websocket.close']
    code: NotRequired[int]
    reason: NotRequired[str | None]


class WebSocketHTTPResponseStart(TypedDict):
    """A WebSocket HTTP response-start event in [`SendMessage`][h2corn.SendMessage]."""

    type: Literal['websocket.http.response.start']
    status: int
    headers: NotRequired[Headers]


class WebSocketHTTPResponseBody(TypedDict):
    """A WebSocket HTTP response-body event in [`SendMessage`][h2corn.SendMessage]."""

    type: Literal['websocket.http.response.body']
    body: NotRequired[bytes]
    more_body: NotRequired[bool]


class LifespanStartupComplete(TypedDict):
    """A lifespan startup-complete event in [`SendMessage`][h2corn.SendMessage]."""

    type: Literal['lifespan.startup.complete']


class LifespanStartupFailed(TypedDict):
    """A lifespan startup-failed event in [`SendMessage`][h2corn.SendMessage]."""

    type: Literal['lifespan.startup.failed']
    message: NotRequired[str]


class LifespanShutdownComplete(TypedDict):
    """A lifespan shutdown-complete event in [`SendMessage`][h2corn.SendMessage]."""

    type: Literal['lifespan.shutdown.complete']


class LifespanShutdownFailed(TypedDict):
    """A lifespan shutdown-failed event in [`SendMessage`][h2corn.SendMessage]."""

    type: Literal['lifespan.shutdown.failed']
    message: NotRequired[str]


#: An outgoing ASGI message, one of [`HTTPResponseStart`][h2corn.HTTPResponseStart],
#: [`HTTPResponseBody`][h2corn.HTTPResponseBody],
#: [`HTTPResponseTrailers`][h2corn.HTTPResponseTrailers],
#: [`HTTPResponsePathsend`][h2corn.HTTPResponsePathsend],
#: [`HTTPResponseZeroCopySend`][h2corn.HTTPResponseZeroCopySend],
#: [`HTTPResponseEarlyHint`][h2corn.HTTPResponseEarlyHint],
#: [`WebSocketAccept`][h2corn.WebSocketAccept],
#: [`WebSocketSendBytes`][h2corn.WebSocketSendBytes],
#: [`WebSocketSendText`][h2corn.WebSocketSendText],
#: [`WebSocketClose`][h2corn.WebSocketClose],
#: [`WebSocketHTTPResponseStart`][h2corn.WebSocketHTTPResponseStart],
#: [`WebSocketHTTPResponseBody`][h2corn.WebSocketHTTPResponseBody],
#: [`LifespanStartupComplete`][h2corn.LifespanStartupComplete],
#: [`LifespanStartupFailed`][h2corn.LifespanStartupFailed],
#: [`LifespanShutdownComplete`][h2corn.LifespanShutdownComplete], or
#: [`LifespanShutdownFailed`][h2corn.LifespanShutdownFailed].
#: [`Message`][h2corn.Message] combines these with incoming messages.
SendMessage = (
    HTTPResponseStart
    | HTTPResponseBody
    | HTTPResponseTrailers
    | HTTPResponsePathsend
    | HTTPResponseZeroCopySend
    | HTTPResponseEarlyHint
    | WebSocketAccept
    | WebSocketSendBytes
    | WebSocketSendText
    | WebSocketClose
    | WebSocketHTTPResponseStart
    | WebSocketHTTPResponseBody
    | LifespanStartupComplete
    | LifespanStartupFailed
    | LifespanShutdownComplete
    | LifespanShutdownFailed
)
#: Any incoming or outgoing ASGI message: [`ReceiveMessage`][h2corn.ReceiveMessage]
#: or [`SendMessage`][h2corn.SendMessage].
Message = ReceiveMessage | SendMessage
Receive = Callable[[], Awaitable[ReceiveMessage]]
Send = Callable[[SendMessage], Awaitable[None]]
ASGIApp = Callable[[Scope, Receive, Send], Awaitable[None]]

# Third-party frameworks intentionally annotate their ASGI boundary more
# broadly than the wire contract. Keep this compatibility alias alongside the
# precise `ASGIApp` so both framework instances and fully typed applications
# remain first-class inputs to h2corn.
FrameworkMessage = MutableMapping[str, Any]
FrameworkReceive = Callable[[], Awaitable[FrameworkMessage]]
FrameworkSend = Callable[[FrameworkMessage], Awaitable[None]]
FrameworkASGIApp = Callable[
    [MutableMapping[str, Any], FrameworkReceive, FrameworkSend], Awaitable[None]
]
Application = ASGIApp | FrameworkASGIApp
