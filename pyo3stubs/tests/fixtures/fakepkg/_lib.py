"""Fake compiled module: pure-Python stand-in exercising every gate.

Each member exists to trip (or satisfy) exactly one check — see
``tests/test_checks.py`` for the seeded-violation twins.
"""

__all__ = ['Open', 'Sealed', 'documented', 'overloaded']


class Sealed:
    """A runtime-unsubclassable class (PyO3 default)."""

    __match_args__ = ('x', 'y')

    def __init_subclass__(cls, **kwargs: object) -> None:
        raise TypeError('Sealed cannot be subclassed')

    @property
    def maybe(self) -> int | None:
        """Optional payload (fixture for the Option gate)."""
        return None

    def method(self, value: int, flag: bool = True) -> int:
        """A documented method."""
        return value if flag else -value


class Open:
    """A subclassable class (``#[pyclass(subclass)]`` analogue)."""


def documented(a: int, b: str = 'x') -> str:
    """A documented function."""
    return b * a


def overloaded(value: object) -> object:
    """Scalar-or-batch fixture for overload checks."""
    return value


class _Uninspectable:
    """Callable whose signature cannot be introspected."""

    @property
    def __signature__(self) -> None:
        raise ValueError('no signature')

    def __call__(self) -> None:
        return None


hidden_callable = _Uninspectable()
