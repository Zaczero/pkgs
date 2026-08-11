"""Lane R4-L2: storage-direct Arrow export + direct-to-SoA NxD ingest.

Deterministic round-trips and selection sharing contracts for the zero-copy
export / direct Arc-column paths. Not a perf test — numbers land in scratch.
"""

from __future__ import annotations

import gometry as gm
import numpy as np


def test_to_arrow_from_arrow_packed_points_roundtrip() -> None:
    arr = gm.points(
        np.arange(1000, dtype=np.float64), np.arange(1000, 2000, dtype=np.float64)
    )
    back = gm.from_arrow(arr.to_arrow())
    assert list(back.to_wkt()) == list(arr.to_wkt())
    assert np.allclose(back.coords.x, arr.coords.x)
    assert np.allclose(back.coords.y, arr.coords.y)


def test_to_arrow_from_arrow_packed_lines_roundtrip() -> None:
    lines = [
        gm.LineString(np.column_stack([np.arange(5) + i, np.arange(5) * 0.5 + i]))
        for i in range(50)
    ]
    arr = gm.GeometryArray(lines)
    back = gm.from_arrow(arr.to_arrow())
    assert list(back.to_wkt()) == list(arr.to_wkt())


def test_to_arrow_from_arrow_polygons_with_holes_roundtrip() -> None:
    shell = np.array([[0.0, 0.0], [4.0, 0.0], [4.0, 4.0], [0.0, 4.0], [0.0, 0.0]])
    hole = np.array([[1.0, 1.0], [2.0, 1.0], [2.0, 2.0], [1.0, 2.0], [1.0, 1.0]])
    poly = gm.Polygon(shell, holes=[hole])
    arr = gm.GeometryArray([poly, gm.Polygon(shell)])
    back = gm.from_arrow(arr.to_arrow())
    assert list(back.to_wkt()) == list(arr.to_wkt())


def test_to_arrow_from_arrow_mixed_roundtrip() -> None:
    arr = gm.GeometryArray([
        gm.Point(1, 2),
        gm.LineString([(0, 0), (1, 1)]),
        gm.Polygon([(0, 0), (1, 0), (1, 1), (0, 0)]),
    ])
    back = gm.from_arrow(arr.to_arrow())
    assert list(back.to_wkt()) == list(arr.to_wkt())


def test_to_arrow_from_arrow_missing_rows_null_bitmap() -> None:
    base = gm.points([0.0, 1.0, 2.0], [0.0, 1.0, 2.0])
    # Build a nullable column via apply_missing on the dense export.
    from gometry._arrow import apply_missing

    dense = base.to_arrow()
    # bit0 present, bit1 missing, bit2 present → mask bytes with bit1 clear
    validity = bytes([(1 << 0) | (1 << 2)])
    nullable = apply_missing(dense, validity)
    restored = gm.from_arrow(nullable)
    assert restored.is_missing.tolist() == [False, True, False]
    present = [g for g, m in zip(restored, restored.is_missing, strict=True) if not m]
    assert [p.to_wkt() for p in present] == ['POINT (0 0)', 'POINT (2 2)']
    # Round-trip identity on the nullable array itself
    again = gm.from_arrow(restored.to_arrow())
    assert again.is_missing.tolist() == restored.is_missing.tolist()
    assert list(again.to_wkt()) == list(restored.to_wkt())


def test_window_slice_to_arrow_roundtrip_no_full_parent() -> None:
    arr = gm.points(
        np.arange(10000, dtype=np.float64), np.arange(10000, dtype=np.float64) + 0.5
    )
    windowed = arr[1000:9000]
    back = gm.from_arrow(windowed.to_arrow())
    assert len(back) == 8000
    assert list(back.to_wkt()[:3]) == list(windowed.to_wkt()[:3])
    assert list(back.to_wkt()[-3:]) == list(windowed.to_wkt()[-3:])


def test_gather_selection_to_arrow_roundtrip() -> None:
    arr = gm.points(np.arange(100, dtype=np.float64), np.arange(100, dtype=np.float64))
    gathered = arr[[5, 1, 99, 0, 50]]
    back = gm.from_arrow(gathered.to_arrow())
    assert list(back.to_wkt()) == list(gathered.to_wkt())


def test_linestring_numpy_nx2_direct() -> None:
    coords = np.ascontiguousarray(
        np.column_stack([np.linspace(0, 1, 1000), np.linspace(2, 3, 1000)])
    )
    line = gm.LineString(coords)
    assert line.coords.x.shape == (1000,)
    assert np.allclose(line.coords.x, coords[:, 0])
    assert np.allclose(line.coords.y, coords[:, 1])


def test_multipoint_numpy_nx2_direct() -> None:
    coords = np.column_stack([
        np.arange(500, dtype=np.float64),
        np.arange(500, dtype=np.float64),
    ])
    mp = gm.MultiPoint(coords)
    assert len(mp) == 500
    assert np.allclose(mp.coords.x, coords[:, 0])


def test_polygon_numpy_ring_direct() -> None:
    # Closed ring
    ring = np.array(
        [[0.0, 0.0], [10.0, 0.0], [10.0, 10.0], [0.0, 10.0], [0.0, 0.0]],
        dtype=np.float64,
    )
    poly = gm.Polygon(ring)
    assert poly.area > 0
    assert np.allclose(poly.exterior.coords.x[:4], ring[:4, 0])


def test_strided_numpy_linestring() -> None:
    base = np.arange(2000, dtype=np.float64).reshape(1000, 2)
    # Non-contiguous rows: every other vertex from a wider parent
    wide = np.zeros((2000, 2), dtype=np.float64)
    wide[::2] = base
    strided = wide[::2]
    assert not strided.flags['C_CONTIGUOUS']
    line = gm.LineString(strided)
    assert np.allclose(line.coords.x, base[:, 0])
    assert np.allclose(line.coords.y, base[:, 1])


def test_points_contiguous_arc_path() -> None:
    x = np.linspace(0, 1, 1000)
    y = np.linspace(1, 2, 1000)
    arr = gm.points(x, y)
    assert len(arr) == 1000
    assert np.allclose(arr.coords.x, x)
    assert np.allclose(arr.coords.y, y)


def test_empty_nd_buffer_axes() -> None:
    empty = np.empty((0, 2), dtype=np.float64)
    line = gm.LineString(empty)
    assert line.is_empty
    empty3 = np.empty((0, 3), dtype=np.float64)
    line3 = gm.LineString(empty3)
    assert line3.is_empty
    assert line3.has_z


class _CapsuleArrayOnly:
    """Carrier that only exposes `__arrow_c_array__` (forces capsule import)."""

    def __init__(self, schema: object, array: object) -> None:
        self._schema = schema
        self._array = array

    def __arrow_c_array__(self, requested_schema: object | None = None) -> object:
        del requested_schema
        return (self._schema, self._array)


def test_arrow_c_array_capsule_roundtrip_packed_points() -> None:
    arr = gm.points([1.0, 2.0, 3.0], [4.0, 5.0, 6.0])
    # __arrow_c_array__ is the capsule path (storage-direct after R4-L2).
    schema_cap, array_cap = arr.__arrow_c_array__()
    assert schema_cap is not None
    assert array_cap is not None
    back = gm.from_arrow(_CapsuleArrayOnly(schema_cap, array_cap))
    assert list(back.to_wkt()) == list(arr.to_wkt())


def _nullable_array(dense: gm.GeometryArray, missing: list[bool]) -> gm.GeometryArray:
    """Build a GeometryArray with an explicit missing mask via apply_missing."""
    from gometry._arrow import apply_missing

    bits = 0
    for i, is_missing in enumerate(missing):
        if not is_missing:
            bits |= 1 << i
    nbytes = (len(missing) + 7) // 8
    validity = bits.to_bytes(nbytes, 'little')
    return gm.from_arrow(apply_missing(dense.to_arrow(), validity))


def test_capsule_window_missing_packed_lines_roundtrip() -> None:
    """Window + null bitmap must align when capsule export uses Arrow offset.

    Item 1 regression: list_array_windowed sets offset=start; validity bits are
    physical (offset+i), not length-only at bit 0.
    """
    import pyarrow as pa

    lines = gm.GeometryArray([
        gm.LineString([(float(i), 0.0), (float(i + 1), 1.0)]) for i in range(7)
    ])
    # missing on rows 0, 3, 5
    missing = [True, False, False, True, False, True, False]
    parent = _nullable_array(lines, missing)
    assert parent.is_missing.tolist() == missing
    windowed = parent[1:6]
    expected_missing = missing[1:6]
    assert windowed.is_missing.tolist() == expected_missing

    # Native capsule import (does not require pyarrow type registration).
    schema_c, array_c = windowed.__arrow_c_array__()
    back_native = gm.from_arrow(_CapsuleArrayOnly(schema_c, array_c))
    assert back_native.is_missing.tolist() == expected_missing
    assert list(back_native.to_wkt()) == list(windowed.to_wkt())

    # Fresh capsules for pyarrow: observes offset-relative validity.
    schema_c2, array_c2 = windowed.__arrow_c_array__()
    arr = pa.Array._import_from_c_capsule(schema_c2, array_c2)
    assert len(arr) == 5
    assert arr.offset == 1  # parent share via Window offset
    assert arr.null_count == sum(expected_missing)
    # The C Data Interface bitmap remains parent-physical: logical row i is
    # bit offset+i, not bit i in a rebased five-row bitmap.
    validity = memoryview(arr.buffers()[0]).cast('B')
    for logical, is_missing in enumerate(expected_missing):
        bit = arr.offset + logical
        assert bool(validity[bit // 8] & (1 << (bit % 8))) is not is_missing
    assert arr.is_null().to_pylist() == expected_missing
    back_pa = gm.from_arrow(arr)
    assert back_pa.is_missing.tolist() == expected_missing
    assert list(back_pa.to_wkt()) == list(windowed.to_wkt())


def test_capsule_window_missing_packed_polygons_roundtrip() -> None:
    """Same offset-relative validity contract for Windowed packed polygons."""
    import pyarrow as pa

    def box(i: float) -> gm.Polygon:
        return gm.Polygon([
            (i, i),
            (i + 1, i),
            (i + 1, i + 1),
            (i, i + 1),
            (i, i),
        ])

    polys = gm.GeometryArray([box(float(i)) for i in range(6)])
    missing = [False, True, False, False, True, False]
    parent = _nullable_array(polys, missing)
    windowed = parent[1:5]
    expected_missing = missing[1:5]
    assert windowed.is_missing.tolist() == expected_missing

    schema_c, array_c = windowed.__arrow_c_array__()
    arr = pa.Array._import_from_c_capsule(schema_c, array_c)
    assert arr.offset == 1
    assert arr.is_null().to_pylist() == expected_missing
    schema_c2, array_c2 = windowed.__arrow_c_array__()
    back = gm.from_arrow(_CapsuleArrayOnly(schema_c2, array_c2))
    assert back.is_missing.tolist() == expected_missing
    assert list(back.to_wkt()) == list(windowed.to_wkt())


def test_capsule_missing_packed_lines_identity_roundtrip() -> None:
    """Identity packed lines with missing still export a correct null bitmap."""
    import pyarrow as pa

    lines = gm.GeometryArray([
        gm.LineString([(0.0, 0.0), (1.0, 1.0)]),
        gm.LineString([(2.0, 0.0), (3.0, 1.0)]),
        gm.LineString([(4.0, 0.0), (5.0, 1.0)]),
    ])
    missing = [False, True, False]
    parent = _nullable_array(lines, missing)
    schema_c, array_c = parent.__arrow_c_array__()
    arr = pa.Array._import_from_c_capsule(schema_c, array_c)
    assert arr.offset == 0
    assert arr.is_null().to_pylist() == missing
    schema_c2, array_c2 = parent.__arrow_c_array__()
    back = gm.from_arrow(_CapsuleArrayOnly(schema_c2, array_c2))
    assert back.is_missing.tolist() == missing
    assert list(back.to_wkt()) == list(parent.to_wkt())


def test_scalar_linestring_to_arrow_roundtrip() -> None:
    line = gm.LineString([(0.0, 0.0), (1.0, 1.0), (2.0, 0.0)])
    back = gm.from_arrow(line.to_arrow())
    # from_arrow of scalar export yields a GeometryArray of length 1
    assert len(back) == 1
    assert back[0].to_wkt() == line.to_wkt()
