from typing import Final, final, overload

__all__ = ['Open', 'Sealed', 'documented', 'overloaded']

@final
class Sealed:
    """A runtime-unsubclassable class (PyO3 default)."""

    __match_args__: Final = ('x', 'y')
    @property
    def maybe(self) -> int | None:
        """Optional payload (fixture for the Option gate)."""
    def method(self, value: int, flag: bool = True) -> int:
        """A documented method."""

class Open:
    """A subclassable class (``#[pyclass(subclass)]`` analogue)."""

class _Uninspectable:
    @property
    def __signature__(self) -> None: ...
    def __call__(self) -> None: ...

hidden_callable: _Uninspectable

def documented(a: int, b: str = 'x') -> str:
    """A documented function."""

@overload
def overloaded(value: int) -> int: ...
@overload
def overloaded(value: str) -> str:
    """Distinct real variants narrow independently."""
