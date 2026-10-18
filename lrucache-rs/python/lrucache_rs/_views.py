"""Live mapping views with non-touching snapshot iteration."""

from __future__ import annotations

from collections.abc import Hashable, ItemsView, Iterator, KeysView, ValuesView
from typing import TYPE_CHECKING, TypeVar, cast

if TYPE_CHECKING:
    from lrucache_rs import LRUCache

_K = TypeVar('_K', bound=Hashable)
_V = TypeVar('_V')
_MISSING = object()


class CacheKeysView(KeysView[_K]):
    _mapping: LRUCache[_K, object]

    def __iter__(self) -> Iterator[_K]:
        return iter(self._mapping)


class CacheItemsView(ItemsView[_K, _V]):
    _mapping: LRUCache[_K, _V]

    def __iter__(self) -> Iterator[tuple[_K, _V]]:
        return iter(self._mapping._snapshot_entries())

    def __contains__(self, item: object) -> bool:
        key, expected = cast('tuple[_K, _V]', item)
        value = self._mapping.peek(key, _MISSING)
        return value is not _MISSING and (value is expected or value == expected)


class CacheValuesView(ValuesView[_V]):
    _mapping: LRUCache[Hashable, _V]

    def __iter__(self) -> Iterator[_V]:
        return iter([entry[1] for entry in self._mapping._snapshot_entries()])

    def __contains__(self, expected: object) -> bool:
        return any(value is expected or value == expected for value in self)
