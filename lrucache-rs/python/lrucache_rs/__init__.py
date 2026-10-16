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
from typing import TypeVar, cast, final, overload

from lrucache_rs._entries import Entry as _Entry
from lrucache_rs._entries import Request as _Request
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
    """

    __slots__ = ('_references',)
    _references: list[_Entry[_K, _V] | None]
    __hash__ = None  # type: ignore[assignment]

    def __new__(cls, maxsize: int) -> 'LRUCache[_K, _V]':
        instance: LRUCache[_K, _V] = super().__new__(cls, index(maxsize))
        # A Python slot keeps this edge visible to PyPy, whose collector does
        # not traverse Python references stored in a native pyclass.
        instance._references = [None] * instance.maxsize
        return instance

    @classmethod
    def __class_getitem__(cls, _item: object) -> 'type[LRUCache[_K, _V]]':
        return cls

    def __contains__(self, key: object, /) -> bool:
        return self._find(key) is not None

    def __getitem__(self, key: _K, /) -> _V:
        request = _Request(key, None)
        entry: _Entry[_K, _V] | None = self._get(request)
        request.finish()
        if entry is None:
            raise KeyError(key)
        return entry.value

    def __setitem__(self, key: _K, value: _V, /) -> None:
        request = _Request(key, value)
        self._set(request)
        request.finish()

    def __delitem__(self, key: _K, /) -> None:
        request = _Request(key, None)
        entry = self._remove(request)
        request.finish()
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
        request = _Request(key, None)
        entry: _Entry[_K, _V] | None = self._get(request)
        request.finish()
        return default if entry is None else entry.value

    def _find(self, key: object) -> _Entry[_K, _V] | None:
        request = _Request(key, None)
        entry: _Entry[_K, _V] | None = self._peek(request)
        request.finish()
        return entry

    @overload
    def peek(self, key: _K, /) -> _V | None: ...
    @overload
    def peek(self, key: _K, /, default: _D) -> _V | _D: ...
    def peek(self, key: _K, /, default: _D | None = None) -> _V | _D | None:
        """Retrieve a value without changing recency."""
        entry = self._find(key)
        return default if entry is None else entry.value

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
        request = _Request(key, None)
        entry: _Entry[_K, _V] | None = self._remove(request)
        request.finish()
        if entry is not None:
            return entry.value
        if default:
            return default[0]
        raise KeyError(key)

    @overload
    def setdefault(self, key: _K, /, default: None = None) -> _V | None: ...
    @overload
    def setdefault(self, key: _K, /, default: _V) -> _V: ...
    def setdefault(self, key: _K, /, default: _V | None = None) -> _V | None:
        """Return and touch an existing value, or atomically insert the default."""
        request = _Request(key, default)
        entry = self._setdefault(request)
        request.finish()
        return cast('_Entry[_K, _V | None]', entry).value

    def _capture(self, indices: list[int]) -> list[_Entry[_K, _V] | None]:
        return [self._references[index] for index in indices]

    def items(self) -> ItemsView[_K, _V]:
        """Return a live items view with non-touching snapshot iteration."""
        return CacheItemsView(self)

    def __iter__(self) -> Iterator[_K]:
        """Snapshot keys from least to most recently used."""
        return iter([entry.key for entry in self._snapshot_entries()])

    def keys(self) -> KeysView[_K]:
        """Return a live keys view with non-touching snapshot iteration."""
        return CacheKeysView(self)

    def popitem(self) -> tuple[_K, _V]:
        """Remove and return the least-recently-used pair, or raise KeyError."""
        entry = self._popitem()
        if entry is None:
            raise KeyError('popitem(): cache is empty')
        return entry.key, entry.value

    def values(self) -> ValuesView[_V]:
        """Return a live values view with non-touching snapshot iteration."""
        return CacheValuesView(self)
