"""A class whose only public callable is its constructor."""

from typing import Self

__all__ = ['Widget']


class Widget:
    """A widget."""

    def __new__(cls, size: int = 3) -> Self:
        del size
        return super().__new__(cls)
