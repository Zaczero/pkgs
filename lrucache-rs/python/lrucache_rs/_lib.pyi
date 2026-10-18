import sys
from typing import Any, Self

if sys.version_info >= (3, 15):
    from typing import disjoint_base
else:
    from typing_extensions import disjoint_base

__all__ = ['Cache']

@disjoint_base
class Cache:
    """Native storage for the public LRU mapping."""

    def __new__(cls, maxsize: int) -> Self:
        """Create native storage for the given positive cache capacity.

        Parameters
        ----------
        maxsize : int
            Positive maximum entry count within the native index range.

        Returns
        -------
        Cache
            Initialized native storage for the public Python mapping.
        """

    @property
    def maxsize(self) -> int:
        """The configured maximum number of entries.

        Returns
        -------
        int
            The positive entry capacity.
        """

    def __len__(self) -> int:
        """Return len(self)."""

    def _get(self, key: Any) -> tuple[Any, Any] | None: ...
    def _peek(self, key: Any) -> tuple[Any, Any] | None: ...
    def _set(self, key: Any, value: Any) -> None: ...
    def _setdefault(self, key: Any, value: Any) -> tuple[Any, Any]: ...
    def _remove(self, key: Any) -> tuple[Any, Any] | None: ...
    def _popitem(self) -> tuple[Any, Any] | None: ...
    def _snapshot_entries(self) -> list[tuple[Any, Any]]: ...
    def clear(self) -> None:
        """Remove all entries before running any retired key or value finalizers.

        Returns
        -------
        None
            Finalizers may insert new entries after the clear commits.
        """
