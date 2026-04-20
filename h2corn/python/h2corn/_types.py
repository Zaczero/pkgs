from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from typing import Any, Literal, NotRequired, TypedDict

HeaderPair = tuple[bytes, bytes]
Headers = Sequence[HeaderPair]
State = dict[str, Any]
Extensions = dict[str, dict[str, Any]]


class ASGIVersions(TypedDict):
    version: str
    spec_version: str


class HTTPScope(TypedDict):
    type: Literal['http']
    asgi: ASGIVersions
    http_version: str
    method: str
    scheme: str
    path: str
    raw_path: bytes
    query_string: bytes
    root_path: str
    headers: Headers
    client: NotRequired[tuple[str, int] | None]
    server: NotRequired[tuple[str, int | None] | None]
    state: NotRequired[State]
    extensions: NotRequired[Extensions]


class WebSocketScope(TypedDict):
    type: Literal['websocket']
    asgi: ASGIVersions
    http_version: str
    scheme: str
    path: str
    raw_path: bytes
    query_string: bytes
    root_path: str
    headers: Headers
    client: NotRequired[tuple[str, int] | None]
    server: NotRequired[tuple[str, int | None] | None]
    subprotocols: list[str]
    state: NotRequired[State]
    extensions: NotRequired[Extensions]


class LifespanScope(TypedDict):
    type: Literal['lifespan']
    asgi: ASGIVersions
    state: NotRequired[State]


Scope = HTTPScope | WebSocketScope | LifespanScope
Message = dict[str, Any]
Receive = Callable[[], Awaitable[Message]]
Send = Callable[[Message], Awaitable[None]]
ASGIApp = Callable[[Scope, Receive, Send], Awaitable[None]]
