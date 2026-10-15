from collections.abc import Hashable, Iterator
from typing import Generic, TypeVar, overload

from typing_extensions import disjoint_base

__all__ = ['Cache']

_K = TypeVar('_K', bound=Hashable)
_V = TypeVar('_V')
_D = TypeVar('_D')

@disjoint_base
class Cache(Generic[_K, _V]):
    """Native storage for the public Python-owned LRU mapping."""
    def __class_getitem__(cls, _item: object) -> type[Cache[_K, _V]]:
        """Subscriptable type hints: `LRUCache[K, V]` is just `LRUCache` at
        runtime.
        """

    def __new__(cls, maxsize: int) -> Cache[_K, _V]:
        """Create native metadata for the given positive cache capacity.

        Parameters
        ----------
        maxsize : int
            Positive maximum entry count within the native index range.

        Returns
        -------
        Cache
            Initialized native metadata for the public Python mapping.
        """

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
    def get(self, key: _K, /, default: None = None) -> _V | None: ...
    @overload
    def get(self, key: _K, /, default: _V) -> _V: ...
    @overload
    def get(self, key: _K, /, default: _D) -> _V | _D:
        """Retrieve a value and mark a present key most recently used.

        Parameters
        ----------
        key : Hashable
            Key to look up.
        default : object, optional
            Value returned when the key is absent.

        Returns
        -------
        object
            The identical stored value, or the default when absent.
        """

    @overload
    def peek(self, key: _K, /) -> _V | None: ...
    @overload
    def peek(self, key: _K, /, default: _D) -> _V | _D: ...
    @overload
    def peek(self, key: _K, /, default: _D | None = None) -> _V | _D | None:
        """Retrieve a value without changing recency.

        Parameters
        ----------
        key : Hashable
            Key to look up.
        default : object, optional
            Value returned when the key is absent.

        Returns
        -------
        object
            The identical stored value, or the default when absent.
        """

    def clear(self) -> None:
        """Remove all entries before running any retired key or value finalizers.

        Returns
        -------
        None
            Finalizers may insert new entries after the clear commits.
        """

    def popitem(self) -> tuple[_K, _V]:
        """Remove and return the least-recently-used pair, or raise KeyError.

        Returns
        -------
        tuple
            The removed key and value with their identities preserved.
        """

    @overload
    def pop(self, key: _K, /) -> _V: ...
    @overload
    def pop(self, key: _K, default: _V, /) -> _V: ...
    @overload
    def pop(self, key: _K, default: _D, /) -> _V | _D:
        """Remove a key atomically, returning its value or the supplied default.

        Parameters
        ----------
        key : Hashable
            Key to remove.
        default : object, optional
            Value returned when absent. If omitted, absence raises KeyError.

        Returns
        -------
        object
            The removed value, or the supplied default when absent.
        """
    @overload
    def setdefault(self, key: _K, /, default: None = None) -> _V | None: ...
    @overload
    def setdefault(self, key: _K, /, default: _V) -> _V:
        """Return and touch an existing value, or atomically insert the default.

        Parameters
        ----------
        key : Hashable
            Key to look up or insert.
        default : object, optional
            Value inserted when absent; None when omitted.

        Returns
        -------
        object
            The identical existing value or inserted default.
        """

    def _snapshot_items(self) -> list[tuple[_K, _V]]: ...
