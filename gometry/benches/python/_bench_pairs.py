"""Shared declared gometry-to-competitor pairing."""

from __future__ import annotations

import sys
from pathlib import Path

_SUPPORT = Path(__file__).resolve().parents[1] / 'support'
if str(_SUPPORT) not in sys.path:
    sys.path.insert(0, str(_SUPPORT))

from _bench_registry import ALL_PUBLIC_OPERATIONS, operation_for_row


def find_competitor(gometry_name: str, available: set[str]) -> str | None:
    """Resolve a competitor for a declared public row among *available* names."""
    operation = operation_for_row(gometry_name)
    if (
        operation is not None
        and operation.gometry == gometry_name
        and operation.competitor in available
    ):
        return operation.competitor
    return None


def pair_units(rows: tuple[str, ...], *, suite: str) -> tuple[tuple[str, ...], ...]:
    """Group declared rows into atomic pair units for pair-aware chunking."""
    available = set(rows)
    used: set[str] = set()
    units: list[tuple[str, ...]] = []

    for operation in ALL_PUBLIC_OPERATIONS:
        if operation.suite != suite:
            continue
        members = tuple(name for name in operation.rows if name in available)
        if not members or any(name in used for name in members):
            continue
        units.append(members)
        used.update(members)

    for row in rows:
        if row not in used:
            competitor = (
                find_competitor(row, available - used)
                if suite == 'competitors'
                else None
            )
            if competitor is not None:
                units.append((row, competitor))
                used.update((row, competitor))
            else:
                units.append((row,))
                used.add(row)
    return tuple(units)


def pair_orderings(
    rows: tuple[str, ...], *, suite: str
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Return balanced A/B and B/A row orderings without splitting pairs."""
    units = pair_units(rows, suite=suite)
    ab = tuple(row for unit in units for row in unit)
    ba = tuple(
        row for unit in units for row in (unit[::-1] if len(unit) == 2 else unit)
    )
    return ab, ba
