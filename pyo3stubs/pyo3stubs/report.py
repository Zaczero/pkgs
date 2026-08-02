"""How a violation is phrased, in one place.

Every violation starts with a navigable location when the gate has one, so an
editor can jump straight to it — several gates used to emit a bare symbol name
and leave the reader to grep. Every allowlist is checked for rot the same way,
because an allowlist nobody prunes is how a real violation goes quiet.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import ast
    from collections.abc import Iterable
    from pathlib import Path

    from pyo3stubs.config import Reasons


class Findings(list[str]):
    """A gate's violations, and how many subjects it examined finding them.

    A ``list`` subclass because the violations *are* the result; the count is
    an annotation on it, produced by the same walk, so the two cannot drift.

    Without it, a gate that finds no subjects at all is indistinguishable from
    one that checked everything and was happy — `text-signature` printed
    "matches signature attributes" on two packages that have no
    ``text_signature`` attribute anywhere.
    """

    __slots__ = ('examined',)

    def __init__(
        self, violations: Iterable[str] = (), *, examined: int | None = None
    ) -> None:
        super().__init__(violations)
        #: Subjects looked at; None where the notion does not apply (mypy
        #: checks the stub as a whole, not a countable set of things).
        self.examined = examined


def loc(path: Path | str, node: ast.stmt | ast.expr) -> str:
    """``path:line`` prefix for a violation anchored at ``node``."""
    return f'{path}:{node.lineno}'


def unused_allowlist_errors(
    label: str, allowlist: Reasons, used: Iterable[str], *, why: str = 'is unused'
) -> list[str]:
    """One violation per allowlist entry nothing needed this run.

    ``why`` names what the gate learned — an entry can be stale because the
    surface it named is gone, or because it came back inside the rule.
    """
    return [
        f'{label} allowlist entry {name!r} {why} — drop it'
        for name in sorted(set(allowlist) - set(used))
    ]
