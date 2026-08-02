from collections.abc import Awaitable, Callable, Iterable, Mapping, MutableMapping
from typing import Any, Literal, NotRequired, TypedDict

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
    type: Literal['lifespan']
    asgi: LifespanASGIVersions
    state: State


Scope = HTTPScope | WebSocketScope | LifespanScope


class HTTPRequest(TypedDict):
    type: Literal['http.request']
    body: NotRequired[bytes]
    more_body: NotRequired[bool]


class HTTPDisconnect(TypedDict):
    type: Literal['http.disconnect']


class WebSocketConnect(TypedDict):
    type: Literal['websocket.connect']


class WebSocketReceiveBytes(TypedDict):
    type: Literal['websocket.receive']
    bytes: bytes
    text: NotRequired[None]


class WebSocketReceiveText(TypedDict):
    type: Literal['websocket.receive']
    text: str
    bytes: NotRequired[None]


class WebSocketDisconnect(TypedDict):
    type: Literal['websocket.disconnect']
    code: int
    reason: NotRequired[str]


class LifespanStartup(TypedDict):
    type: Literal['lifespan.startup']


class LifespanShutdown(TypedDict):
    type: Literal['lifespan.shutdown']


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
    type: Literal['http.response.start']
    status: int
    headers: NotRequired[Headers]
    trailers: NotRequired[bool]


class HTTPResponseBody(TypedDict):
    type: Literal['http.response.body']
    body: NotRequired[bytes]
    more_body: NotRequired[bool]


class HTTPResponseTrailers(TypedDict):
    type: Literal['http.response.trailers']
    headers: Headers
    more_trailers: NotRequired[bool]


class HTTPResponsePathsend(TypedDict):
    type: Literal['http.response.pathsend']
    path: str


class HTTPResponseEarlyHint(TypedDict):
    """An RFC 8297 `103 Early Hints` interim response.

    Sent after `http.response.start` and before the final body, as many times
    as wanted. Each value becomes one `link` field; ordering and duplicates are
    preserved. Offered only on HTTP/2 -- check `scope["extensions"]`.
    """

    type: Literal['http.response.early_hint']
    links: Iterable[bytes]


class WebSocketAccept(TypedDict):
    type: Literal['websocket.accept']
    subprotocol: NotRequired[str | None]
    headers: NotRequired[Headers]


class WebSocketSendBytes(TypedDict):
    type: Literal['websocket.send']
    bytes: bytes
    text: NotRequired[None]


class WebSocketSendText(TypedDict):
    type: Literal['websocket.send']
    text: str
    bytes: NotRequired[None]


class WebSocketClose(TypedDict):
    type: Literal['websocket.close']
    code: NotRequired[int]
    reason: NotRequired[str | None]


class WebSocketHTTPResponseStart(TypedDict):
    type: Literal['websocket.http.response.start']
    status: int
    headers: NotRequired[Headers]


class WebSocketHTTPResponseBody(TypedDict):
    type: Literal['websocket.http.response.body']
    body: NotRequired[bytes]
    more_body: NotRequired[bool]


class LifespanStartupComplete(TypedDict):
    type: Literal['lifespan.startup.complete']


class LifespanStartupFailed(TypedDict):
    type: Literal['lifespan.startup.failed']
    message: NotRequired[str]


class LifespanShutdownComplete(TypedDict):
    type: Literal['lifespan.shutdown.complete']


class LifespanShutdownFailed(TypedDict):
    type: Literal['lifespan.shutdown.failed']
    message: NotRequired[str]


SendMessage = (
    HTTPResponseStart
    | HTTPResponseBody
    | HTTPResponseTrailers
    | HTTPResponsePathsend
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
