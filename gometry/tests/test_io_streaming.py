"""Bulk-import streaming lanes: builder demotion, chunked WKB, and WKT rings."""

import gometry as gm
import pytest


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


def test_polygon_streaming_demotes_mixed_ring_axes_without_panicking() -> None:
    mixed_ring_axes = {
        'type': 'Polygon',
        'coordinates': [
            [[0, 0], [4, 0], [4, 4], [0, 0]],
            [[1, 1, 5], [2, 1, 5], [2, 2, 5], [1, 1, 5]],
        ],
    }
    scalar = gm.from_geojson(mixed_ring_axes)
    array = gm.from_geojson([mixed_ring_axes, mixed_ring_axes])
    assert array.to_wkt() == [scalar.to_wkt(), scalar.to_wkt()]


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
