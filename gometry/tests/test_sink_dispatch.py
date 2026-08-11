"""R5-L2: Shape-native array sinks + no eager per-row ShapeData on pure kernels.

Locks the two round-5 P0 fixes:
  P0-D — fill_missing / parts / rings terminate in array-owned Shape storage
  P0-E — pure topology (intersection, simplify) does not grow input retained size
"""

from __future__ import annotations

import gometry as gm
import numpy as np


def test_fill_missing_packed_points_element_identity_and_dense() -> None:
    n = 10_000
    x = np.arange(n, dtype=np.float64)
    dense = gm.points(x, x)
    mask = np.zeros(n, dtype=bool)
    mask[::100] = True
    sparse = dense._with_missing(mask)
    fill = gm.Point(-1.0, -1.0)
    filled = sparse.fill_missing(fill)

    assert not any(filled.is_missing)
    assert len(filled) == n
    # Missing slots get the fill; present slots keep source coordinates.
    assert filled[0] == fill
    assert filled[1] == gm.Point(1.0, 1.0)
    assert filled[100] == fill
    assert filled[101] == gm.Point(101.0, 101.0)
    # Dense packed point output: sizeof is column-scale, not scalar-wrapper-scale.
    # Pre-fix PyGeometry staging retained ~1 KiB/row (~10 MiB here); packed is ~320 KB.
    assert filled.__sizeof__() < 1_000_000


def test_fill_missing_array_fill_only_consumes_missing_rows() -> None:
    arr = gm.GeometryArray([gm.Point(0.0, 0.0), None, gm.Point(2.0, 2.0)])
    fill = gm.GeometryArray([None, gm.Point(9.0, 9.0), None])
    out = arr.fill_missing(fill)
    assert out.to_wkt() == ['POINT (0 0)', 'POINT (9 9)', 'POINT (2 2)']


def test_parts_and_rings_order_and_typing() -> None:
    multi = gm.MultiPoint([(0.0, 0.0), (1.0, 1.0), (2.0, 2.0)])
    parts = gm.parts(multi)
    assert len(parts) == 3
    assert all(isinstance(p, gm.Point) for p in parts)
    assert [p.to_wkt() for p in parts] == [
        'POINT (0 0)',
        'POINT (1 1)',
        'POINT (2 2)',
    ]

    donut = gm.Polygon(
        [(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0)],
        holes=[[(1.0, 1.0), (2.0, 1.0), (2.0, 2.0), (1.0, 2.0)]],
    )
    rings = gm.rings(donut)
    assert len(rings) == 2
    assert all(isinstance(r, gm.LineString) for r in rings)
    assert rings[0].to_wkt().startswith('LINESTRING (0 0')
    assert rings[1].to_wkt().startswith('LINESTRING (1 1')


def test_intersection_and_simplify_do_not_retain_input_sidecars() -> None:
    n = 5_000
    x = np.arange(n, dtype=np.float64)
    left = gm.points(x, x)
    right = gm.points(x + 0.5, x + 0.5)
    size_before = left.__sizeof__() + right.__sizeof__()
    result = gm.intersection(left, right)
    del result
    size_after = left.__sizeof__() + right.__sizeof__()
    # No prepared-slot / frame-cache / forced bounds-cache growth on inputs.
    assert size_after == size_before

    coords = [(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.5, 0.5), (0.0, 1.0), (0.0, 0.0)]
    mixed = gm.GeometryArray([gm.Polygon(coords), gm.Point(0.0, 0.0)] * 200)
    before = mixed.__sizeof__()
    simplified = mixed.simplify(0.1)
    del simplified
    assert mixed.__sizeof__() == before
