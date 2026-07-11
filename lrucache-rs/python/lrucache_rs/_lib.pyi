from collections.abc import Hashable, Iterator, MutableMapping
from typing import TypeVar, final, overload

__all__ = ['LRUCache']

_K = TypeVar('_K', bound=Hashable)
_V = TypeVar('_V')
_D = TypeVar('_D')

@final
class LRUCache(MutableMapping[_K, _V]):
    """A thread-safe LRU cache with a fixed maximum size.

    A ``MutableMapping``: reads and writes mark the key as most recently
    used, and inserting past ``maxsize`` evicts the least recently used
    entry. Construct with ``LRUCache(maxsize)``; ``LRUCache[K, V]`` works
    for type hints.
    """
    def __class_getitem__(cls, _item: object) -> type[LRUCache[_K, _V]]:
        """Subscriptable type hints: `LRUCache[K, V]` is just `LRUCache` at
        runtime.
        """

    def __new__(cls, maxsize: int) -> LRUCache[_K, _V]:
        """Create and return a new object.  See help(type) for accurate signature."""

    @property
    def maxsize(self) -> int:
        """The configured maximum number of entries."""

    def __len__(self) -> int:
        """Return len(self)."""

    def __contains__(self, key: object, /) -> bool:
        """Return bool(key in self)."""

    def __iter__(self) -> Iterator[_K]:
        """Implement iter(self)."""

    def __setitem__(self, key: _K, value: _V, /) -> None:
        """Set self[key] to value."""

    def __getitem__(self, key: _K, /) -> _V:
        """Return self[key]."""

    def __delitem__(self, key: _K, /) -> None:
        """Delete self[key]."""

    @overload
    def get(self, key: _K, /) -> _V | None: ...
    @overload
    def get(self, key: _K, /, default: _D) -> _V | _D: ...
    @overload
    def get(self, key: _K, /, default: _D | None = None) -> _V | _D | None:
        """Retrieve the value for ``key``, marking it most recently used;
        ``default`` (``None`` if omitted) when the key is absent.
        """

    @overload
    def peek(self, key: _K, /) -> _V | None: ...
    @overload
    def peek(self, key: _K, /, default: _D) -> _V | _D: ...
    @overload
    def peek(self, key: _K, /, default: _D | None = None) -> _V | _D | None:
        """Read without bumping recency. Useful for instrumentation that should not
        perturb the LRU order; the convention is borrowed from `cachetools`.
        """

    def clear(self) -> None:
        """Remove all entries from the cache."""

    def popitem(self) -> tuple[_K, _V]:
        """Remove and return the least-recently-used `(key, value)` pair, raising
        `KeyError` if empty.
        """
