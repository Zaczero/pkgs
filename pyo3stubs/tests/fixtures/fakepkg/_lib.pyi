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
        """A documented method.

        Parameters
        ----------
        value : int
            The number to return.
        flag : bool, default True
            Whether to keep the sign.

        Returns
        -------
        int
            ``value``, negated when ``flag`` is false.
        """

class Open:
    """A subclassable class (``#[pyclass(subclass)]`` analogue)."""

    def method(self, value: int, flag: bool = True) -> int:
        """The same operation on a second surface (cross-surface parity).

        Parameters
        ----------
        value : int
            The number to return.
        flag : bool, default True
            Whether to flip the sign.

        Returns
        -------
        int
            ``value``, negated when ``flag`` is true.
        """

class _Uninspectable:
    """Callable whose signature cannot be introspected."""
    @property
    def __signature__(self) -> None: ...
    def __call__(self) -> None: ...

hidden_callable: _Uninspectable

def _token_vocabulary() -> list[tuple[str, str | None, tuple[str, ...]]]:
    """``(enum, stub alias, tokens)`` for every ``token_enum!`` surface."""

def documented(a: int, b: str = 'x') -> str:
    """A documented function.

    Parameters
    ----------
    a : int
        How many times to repeat.
    b : str, default 'x'
        The text to repeat.

    Returns
    -------
    str
        ``b`` repeated ``a`` times.
    """

@overload
def overloaded(value: int) -> int: ...
@overload
def overloaded(value: str) -> str:
    """Scalar-or-batch fixture for overload checks.

    Parameters
    ----------
    value : object
        Anything at all.

    Returns
    -------
    object
        The value unchanged.
    """
