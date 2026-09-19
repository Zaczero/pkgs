"""Degenerate input: empties of an accepted kind, rings that collapse under a
transform, zero plateaus, and amplification parameters past every budget.

Each case answers or raises the operation's typed error; none may panic,
hang, or surface an internal size overflow.
"""

from __future__ import annotations

import gometry as gm

W = gm.from_wkt


def test_affine_transforms_keep_mixed_axes_rows() -> None:
    rows = gm.GeometryArray([W('POINT (1 2)'), W('POINT Z (1 2 3)')])
    assert rows.translate(1, 1).to_wkt() == ['POINT (2 3)', 'POINT Z (2 3 3)']
    assert rows.affine_transform([2, 0, 0, 2, 0, 0]).to_wkt() == [
        'POINT (2 4)',
        'POINT Z (2 4 3)',
    ]
