"""Bulk-import streaming lanes: builder demotion, chunked WKB, and WKT rings."""

import pickle

import gometry as gm
import pytest


def test_mixed_array_scalar_extract_reuses_prepared_shape_data() -> None:
    """arr[i] on mixed storage materializes ShapeData once and reuses it.

    Arc identity of the prepared handle is pinned in the Rust unit test
    ``mixed_geometry_at_shares_prepared_shape_data_arc`` (array prepared
    cache). This Python seam checks the public consequences: distinct
    wrapper objects, value equality, and that a warmed scalar cache
    (``is_valid`` / length) remains consistent across repeated extract.
    """
    arr = gm.from_wkt([
        'POINT (1 2)',
        'LINESTRING (0 0, 1 1)',
        'POLYGON ((0 0, 2 0, 2 2, 0 0))',
    ])
    a = arr[1]
    # Warm prepared verdicts on the shared ShapeData.
    assert a.is_valid is True
    length = a.length
    b = arr[1]
    # Distinct Python wrappers (new Typed leaf each extract) ...
    assert a is not b
    # ... wrapping equal geometry with the same prepared results.
    assert a == b
    assert b.is_valid is True
    assert b.length == length
    assert arr[0] == gm.Point(1, 2)
    assert arr[2].area > 0


def test_masked_mixed_array_pickle_round_trip() -> None:
    """Missing rows on mixed storage pickle with null ≠ empty preserved."""
    arr = gm.GeometryArray([
        gm.Point(1, 2),
        None,
        gm.LineString([(0, 0), (1, 1)]),
        gm.from_wkt('POINT EMPTY'),
    ])
    assert arr.is_missing.tolist() == [False, True, False, False]
    assert arr[1] is None
    assert arr[3].is_empty
    restored = pickle.loads(pickle.dumps(arr))
    assert restored == arr
    assert restored.is_missing.tolist() == [False, True, False, False]
    assert restored[1] is None
    assert restored[3].is_empty
    assert restored[0] == gm.Point(1, 2)


def test_bulk_import_mixed_rows_survive_streaming_demote() -> None:
    """The streaming import lane packs uniform point prefixes; a mid-stream
    kind change must re-expand them without losing or reordering rows.
    """
    arr = gm.from_wkt([
        'POINT (1 2)',
        'POINT (3 4)',
        'LINESTRING (0 0, 1 1)',
        'POINT (5 6)',
    ])
    assert arr.to_wkt() == [
        'POINT (1 2)',
        'POINT (3 4)',
        'LINESTRING (0 0, 1 1)',
        'POINT (5 6)',
    ]
    wkb = arr.to_wkb()
    assert gm.from_wkb(wkb).to_wkt() == arr.to_wkt()
    assert gm.from_wkt([
        'LINESTRING (0 0, 1 1)',
        'LINESTRING EMPTY',
    ]).is_valid.tolist() == [True, True]


def test_bulk_streaming_lines_and_polygons_preserve_rows_on_demote() -> None:
    lines = gm.from_wkt([
        'LINESTRING (0 0, 1 1)',
        'LINESTRING Z (2 2 3, 3 3 4)',
        'POINT (9 9)',
    ])
    assert lines.to_wkt() == [
        'LINESTRING (0 0, 1 1)',
        'LINESTRING Z (2 2 3, 3 3 4)',
        'POINT (9 9)',
    ]
    polygons = gm.from_wkt([
        'POLYGON ((0 0, 2 0, 2 2, 0 0))',
        'POLYGON ((3 3, 5 3, 5 5, 3 3))',
        'LINESTRING (7 7, 8 8)',
    ])
    assert polygons.to_wkt() == [
        'POLYGON ((0 0, 2 0, 2 2, 0 0))',
        'POLYGON ((3 3, 5 3, 5 5, 3 3))',
        'LINESTRING (7 7, 8 8)',
    ]


def test_polygon_streaming_mixed_ring_axes_rejects_without_panicking() -> None:
    """Mixed XY shell + XYZ hole rejects at construction (writer parity; A3/G2)."""
    with pytest.raises(gm.InvalidGeometryError, match=r'share one coordinate axes'):
        gm.Polygon(
            [(0, 0), (4, 0), (4, 4), (0, 0)],
            [[(1, 1, 5), (2, 1, 5), (2, 2, 5), (1, 1, 5)]],
        )


def test_array_to_wkb_preserves_order_across_internal_chunks() -> None:
    values = gm.from_wkt([f'POINT ({i} {-i})' for i in range(4_101)], crs=None)
    encoded = values.to_wkb()
    assert len(encoded) == len(values)
    assert gm.from_wkb(encoded, crs=None).to_wkt() == values.to_wkt()


@pytest.mark.parametrize(
    'value',
    [
        'POLYGON ((0 0, 1 0, 1 1, 0 0)',
        'MULTILINESTRING ((0 0, 1 1), (2 2, 3 3)',
    ],
)
def test_wkt_streaming_ring_scanner_preserves_unclosed_error(value: str) -> None:
    with pytest.raises(gm.ParseError, match=r'missing closing|unclosed'):
        gm.from_wkt(value)


# --- LANE A4: WKB/WKT serialization correctness (mixed-axis, nested SRID, EWKT) ---
