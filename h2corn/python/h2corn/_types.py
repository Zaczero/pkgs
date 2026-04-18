from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from typing import Any, TypedDict

HeaderPair = tuple[bytes, bytes]
Headers = Sequence[HeaderPair]
State = dict[str, Any]
Extensions = dict[str, dict[str, Any]]


class ASGIVersions(TypedDict):
    version: str
    spec_version: str


class HTTPScope(TypedDict, total=False):
    type: str
    asgi: ASGIVersions
    http_version: str
    method: str
    scheme: str
    path: str
    raw_path: bytes
    query_string: bytes
    root_path: str
    headers: Headers
    client: tuple[str, int] | None
    server: tuple[str, int | None] | None
    state: State
    extensions: Extensions


class WebSocketScope(TypedDict, total=False):
    type: str
    asgi: ASGIVersions
    http_version: str
    scheme: str
    path: str
    raw_path: bytes
    query_string: bytes
    root_path: str
    headers: Headers
    client: tuple[str, int] | None
    server: tuple[str, int | None] | None
    subprotocols: list[str]
    state: State
    extensions: Extensions


class LifespanScope(TypedDict, total=False):
    type: str
    asgi: ASGIVersions
    state: State


Scope = HTTPScope | WebSocketScope | LifespanScope
Message = dict[str, Any]
Receive = Callable[[], Awaitable[Message]]
Send = Callable[[Message], Awaitable[None]]
ASGIApp = Callable[[Scope, Receive, Send], Awaitable[None]]
