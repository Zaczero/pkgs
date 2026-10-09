"""A class whose only public member is a property."""

__all__ = ['Shape']


class Shape:
    """A shape."""

    @property
    def area(self) -> float:
        """Area."""
        return 1.0
