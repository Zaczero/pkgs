"""A bounded, thread-safe mutable mapping with least-recently-used eviction."""

from collections.abc import (
    Hashable,
    ItemsView,
    Iterator,
    KeysView,
    MutableMapping,
    ValuesView,
)
from typing import TypeVar, cast, final

from lrucache_rs._lib import Cache as _Cache
from lrucache_rs._views import CacheItemsView, CacheKeysView, CacheValuesView

__all__ = ('LRUCache',)

_K = TypeVar('_K', bound=Hashable)
_V = TypeVar('_V')


@final
class LRUCache(_Cache[_K, _V], MutableMapping[_K, _V]):
    """A mutable mapping with a fixed positive maximum number of entries.

    Item lookup, get, assignment and setdefault mark a key most recently
    used. Peek, containment, iteration, views and equality do not change
    recency. Inserting beyond maxsize evicts the least-recently-used entry.
    Iterators snapshot entries in least-to-most-recently-used order.

    Individual native operations are thread-safe, including pop and
    setdefault. Update processes its input incrementally; it is not an
    atomic transaction. Key equality and object finalizers may reenter the
    cache. Equality that continually changes membership may prevent a lookup
    from stabilizing. Python owns the cache reference graph; PyPy may still
    retain tuple cycles after tuples cross its C-extension boundary.
    """

    __slots__ = ('_references',)
    _references: list[object]
    __hash__ = None  # type: ignore[assignment]

    def __new__(cls, maxsize: int) -> 'LRUCache[_K, _V]':
        instance = cast('LRUCache[_K, _V]', super().__new__(cls, maxsize))
        # A Python slot keeps this edge visible to PyPy, whose collector does
        # not traverse Python references stored in a native pyclass.
        instance._references = [None] * (2 * instance.maxsize)
        return instance

    def items(self) -> ItemsView[_K, _V]:
        """Return a live items view with non-touching snapshot iteration."""
        return CacheItemsView(self)

    def __iter__(self) -> Iterator[_K]:
        """Snapshot keys from least to most recently used."""
        # Python-created snapshots remain visible to PyPy's collector even
        # when an iterator is stored back into the cache.
        return iter(list(super().__iter__()))

    def keys(self) -> KeysView[_K]:
        """Return a live keys view with non-touching snapshot iteration."""
        return CacheKeysView(self)

    def popitem(self) -> tuple[_K, _V]:
        """Remove and return the least-recently-used pair, or raise KeyError."""
        key, value = super().popitem()
        return key, value

    def values(self) -> ValuesView[_V]:
        """Return a live values view with non-touching snapshot iteration."""
        return CacheValuesView(self)
