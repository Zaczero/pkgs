"""A bounded, thread-safe mutable mapping with least-recently-used eviction."""

from collections.abc import (
    Hashable,
    ItemsView,
    Iterator,
    KeysView,
    MutableMapping,
    ValuesView,
)
from operator import index
from typing import TypeVar, final, overload

from lrucache_rs._lib import Cache as _Cache
from lrucache_rs._views import CacheItemsView, CacheKeysView, CacheValuesView

__all__ = ('LRUCache',)

_K = TypeVar('_K', bound=Hashable)
_V = TypeVar('_V')
_D = TypeVar('_D')


@final
class LRUCache(_Cache, MutableMapping[_K, _V]):
    """A mutable mapping with a fixed positive maximum number of entries.

    Item lookup, get, assignment and setdefault mark a key most recently
    used. Peek, containment, iteration, views and equality do not change
    recency. Inserting beyond maxsize evicts the least-recently-used entry.
    Iterators snapshot entries in least-to-most-recently-used order.

    Individual native operations are thread-safe, including pop and
    setdefault. Update processes its input incrementally; it is not an
    atomic transaction. Key equality and object finalizers may reenter the
    cache. Equality that continually changes membership may prevent a lookup
    from stabilizing.

    Invalid capacities raise ValueError. Values without Python's integer
    index protocol raise TypeError.
    """

    __slots__ = ()
    __hash__ = None  # type: ignore[assignment]

    def __new__(cls, maxsize: int) -> 'LRUCache[_K, _V]':
        return super().__new__(cls, index(maxsize))

    @classmethod
    def __class_getitem__(cls, _item: object) -> 'type[LRUCache[_K, _V]]':
        return cls

    def __contains__(self, key: object, /) -> bool:
        return self._peek(key) is not None

    def __getitem__(self, key: _K, /) -> _V:
        entry: tuple[_K, _V] | None = self._get(key)
        if entry is None:
            raise KeyError(key)
        return entry[1]

    def __setitem__(self, key: _K, value: _V, /) -> None:
        self._set(key, value)

    def __delitem__(self, key: _K, /) -> None:
        entry = self._remove(key)
        if entry is None:
            raise KeyError(key)

    @overload
    def get(self, key: _K, /, default: None = None) -> _V | None: ...
    @overload
    def get(self, key: _K, /, default: _V) -> _V: ...
    @overload
    def get(self, key: _K, /, default: _D) -> _V | _D: ...
    def get(self, key: _K, /, default: _D | None = None) -> _V | _D | None:
        """Retrieve a value and mark a present key most recently used."""
        entry: tuple[_K, _V] | None = self._get(key)
        return default if entry is None else entry[1]

    @overload
    def peek(self, key: _K, /) -> _V | None: ...
    @overload
    def peek(self, key: _K, /, default: _D) -> _V | _D: ...
    def peek(self, key: _K, /, default: _D | None = None) -> _V | _D | None:
        """Retrieve a value without changing recency."""
        entry: tuple[_K, _V] | None = self._peek(key)
        return default if entry is None else entry[1]

    @overload
    def pop(self, key: _K, /) -> _V: ...
    @overload
    def pop(self, key: _K, default: _V, /) -> _V: ...
    @overload
    def pop(self, key: _K, default: _D, /) -> _V | _D: ...
    def pop(self, key: _K, /, *default: _D) -> _V | _D:
        """Remove a key atomically, returning its value or the supplied default."""
        if len(default) > 1:
            raise TypeError('pop accepts at most one default')
        entry: tuple[_K, _V] | None = self._remove(key)
        if entry is not None:
            return entry[1]
        if default:
            return default[0]
        raise KeyError(key)

    @overload
    def setdefault(self, key: _K, /, default: None = None) -> _V | None: ...
    @overload
    def setdefault(self, key: _K, /, default: _V) -> _V: ...
    def setdefault(self, key: _K, /, default: _V | None = None) -> _V | None:
        """Return and touch an existing value, or atomically insert the default."""
        entry: tuple[_K, _V | None] = self._setdefault(key, default)
        return entry[1]

    def items(self) -> ItemsView[_K, _V]:
        """Return a live items view with non-touching snapshot iteration."""
        return CacheItemsView(self)

    def __iter__(self) -> Iterator[_K]:
        """Snapshot keys from least to most recently used."""
        return iter([entry[0] for entry in self._snapshot_entries()])

    def keys(self) -> KeysView[_K]:
        """Return a live keys view with non-touching snapshot iteration."""
        return CacheKeysView(self)

    def popitem(self) -> tuple[_K, _V]:
        """Remove and return the least-recently-used pair, or raise KeyError."""
        entry = self._popitem()
        if entry is None:
            raise KeyError('popitem(): cache is empty')
        return entry

    def values(self) -> ValuesView[_V]:
        """Return a live values view with non-touching snapshot iteration."""
        return CacheValuesView(self)
