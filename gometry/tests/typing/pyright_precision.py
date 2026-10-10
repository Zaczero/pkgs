"""Pyright-primary assertions for precision Mypy cannot currently retain.

The public stub remains checker-neutral. This fixture isolates exact subtype
relationships whose safe Mypy approximation would otherwise weaken the
contract shared by Pyright users and generated reference documentation.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, assert_type

if TYPE_CHECKING:
    import gometry as gm

    def mixed_missing_removal(
        values: gm.GeometryArray[gm.Point | gm.LineString | None],
    ) -> None:
        assert_type(
            values.drop_missing(),
            gm.GeometryArray[gm.Point | gm.LineString],
        )

    def nullable_cell_groups(
        cells: gm.CellArray[gm.H3Cell | None],
    ) -> None:
        assert_type(
            cells.children(),
            gm.Groups[gm.CellArray[gm.H3Cell] | None],
        )
