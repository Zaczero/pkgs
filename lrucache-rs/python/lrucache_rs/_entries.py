"""Python-owned payloads and error transport for native metadata operations."""

from __future__ import annotations

from typing import Generic, TypeVar, cast

_K = TypeVar('_K')
_V = TypeVar('_V')


class Entry(Generic[_K, _V]):
    __slots__ = ('key', 'value')
    __hash__ = None  # type: ignore[assignment]

    def __init__(self, key: _K, value: _V) -> None:
        self.key = key
        self.value = value

    def __eq__(self, other: object) -> bool:
        request = cast('Request[_K, _V]', other)
        try:
            return self.key is request.key or bool(self.key == request.key)
        except BaseException as error:
            # Even exception arguments can contain recursive tuples. Keep the
            # original error in Python until the native caller has returned.
            request.error = error
            request.failed = True
            return False


class Request(Generic[_K, _V]):
    __slots__ = ('error', 'failed', 'key', 'value')

    def __init__(self, key: _K, value: _V) -> None:
        self.key = key
        self.value = value
        self.failed = False
        self.error: BaseException | None = None

    def __hash__(self) -> int:
        try:
            return hash(self.key)
        except BaseException as error:
            self.error = error
            self.failed = True
            return 0

    def build_entry(self, stored: Entry[_K, _V] | None) -> Entry[_K, _V] | None:
        try:
            if stored is not None and stored.value is self.value:
                return stored
            return Entry(self.key if stored is None else stored.key, self.value)
        except BaseException as error:
            self.error = error
            self.failed = True
            return None

    def finish(self) -> None:
        if self.failed:
            error = self.error
            self.error = None
            raise cast('BaseException', error)
