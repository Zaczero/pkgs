from collections import Counter
from itertools import pairwise

import gometry as gm
import pytest


def _dense_geographic_rectangle(
    x0: float,
    y0: float,
    x1: float,
    y1: float,
    *,
    crs: int,
) -> gm.Polygon:
    steps = 32
    points = [
        *((x0 + (x1 - x0) * index / steps, y0) for index in range(steps)),
        *((x1, y0 + (y1 - y0) * index / steps) for index in range(steps)),
        *((x1 - (x1 - x0) * index / steps, y1) for index in range(steps)),
        *((x0, y1 - (y1 - y0) * index / steps) for index in range(steps)),
    ]
    points.append(points[0])
    return gm.Polygon(points, crs=crs)


def test_boundary_of_invalid_polygonal_support_uses_occupied_cells() -> None:
    cases = [
        (
            'outward retraced spur',
            'POLYGON ((0 0,4 0,4 4,2 4,2 6,2 4,0 4,0 0))',
            'LINESTRING (0 0, 4 0, 4 4, 0 4, 0 0)',
        ),
        (
            'inward retraced slit',
            'POLYGON ((0 0,4 0,4 4,2 4,2 2,2 4,0 4,0 0))',
            'LINESTRING (0 0, 4 0, 4 4, 0 4, 0 0)',
        ),
        (
            'external hole',
            'POLYGON ((0 0,4 0,4 4,0 4,0 0),(5 5,6 5,6 6,5 6,5 5))',
            'LINESTRING (0 0, 4 0, 4 4, 0 4, 0 0)',
        ),
        (
            'adjacent multipolygon members',
            'MULTIPOLYGON (((0 0,2 0,2 2,0 2,0 0)),((2 0,4 0,4 2,2 2,2 0)))',
            'LINESTRING (0 0, 4 0, 4 2, 0 2, 0 0)',
        ),
        (
            'overlapping multipolygon members',
            'MULTIPOLYGON (((0 0,4 0,4 2,0 2,0 0)),((2 0,6 0,6 2,2 2,2 0)))',
            'LINESTRING (0 0, 6 0, 6 2, 0 2, 0 0)',
        ),
        (
            'duplicate multipolygon members',
            'MULTIPOLYGON (((0 0,4 0,4 4,0 4,0 0)),((0 0,4 0,4 4,0 4,0 0)))',
            'LINESTRING (0 0, 4 0, 4 4, 0 4, 0 0)',
        ),
        (
            'twice-wound ring',
            'POLYGON ((0 0,4 0,4 4,0 4,0 0,4 0,4 4,0 4,0 0))',
            'LINESTRING (0 0, 4 0, 4 4, 0 4, 0 0)',
        ),
        (
            'hole touching the shell',
            'POLYGON ((0 0,4 0,4 4,0 4,0 0),(0 1,2 1,2 2,0 2,0 1))',
            'LINESTRING (0 0, 4 0, 4 4, 0 4, 0 2, 2 2, 2 1, 0 1, 0 0)',
        ),
    ]
    for name, wkt, expected in cases:
        shape = gm.from_wkt(wkt)
        assert not shape.is_valid, name
        assert shape.boundary().to_wkt() == expected, name


def test_boundary_of_regular_polygon_keeps_its_hole_cycle() -> None:
    polygon = gm.from_wkt('POLYGON ((0 0,4 0,4 4,0 4,0 0),(1 1,3 1,3 3,1 3,1 1))')

    assert polygon.is_valid
    assert polygon.boundary().to_wkt() == (
        'MULTILINESTRING ((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 3 1, 3 3, 1 3, 1 1))'
    )


def test_boundary_of_ordinary_geographic_area_preserves_crs_and_ring() -> None:
    polygon = gm.box(-2, -1, 2, 1, crs=4326)

    boundary = polygon.boundary()
    assert boundary.crs == polygon.crs
    assert boundary.to_wkt() == 'LINESTRING (-2 -1, 2 -1, 2 1, -2 1, -2 -1)'


def test_packed_geographic_array_boundary_preserves_open_chart_rings_and_gathers() -> (
    None
):
    polygons = [
        gm.box(-2, -1, 2, 1, crs=4326),
        gm.box(10, 20, 12, 22, crs=4326),
    ]
    rows = gm.GeometryArray(polygons, crs=4326)
    expected = [
        'LINESTRING (-2 -1, 2 -1, 2 1, -2 1, -2 -1)',
        'LINESTRING (10 20, 12 20, 12 22, 10 22, 10 20)',
    ]

    boundaries = rows.boundary()
    assert boundaries.crs == rows.crs
    assert [shape.to_wkt() for shape in boundaries] == expected

    gathered = rows[[1, 0]].boundary()
    assert [shape.to_wkt() for shape in gathered] == expected[::-1]


def test_packed_geographic_array_boundary_routes_seam_rows_through_quotient() -> None:
    ordinary = gm.box(-2, -1, 2, 1, crs=4326)
    seam = gm.Polygon(
        [(179, 0), (181, 0), (181, 2), (179, 2), (179, 0)],
        crs=4326,
    )

    boundaries = gm.GeometryArray([ordinary, seam], crs=4326).boundary()

    assert boundaries[0].to_wkt() == 'LINESTRING (-2 -1, 2 -1, 2 1, -2 1, -2 -1)'
    assert boundaries[1].to_wkt() == (
        'LINESTRING (179 2, 179 0, -180 0, -179 0, -179 2, -180 2, 179 2)'
    )


def test_packed_geographic_array_boundary_refines_invalid_support() -> None:
    ordinary = gm.box(0, 0, 4, 4, crs=4326)
    spur = gm.Polygon(
        [(0, 0), (4, 0), (4, 4), (0, 4), (0, 0), (-1, 0), (0, 0)],
        crs=4326,
    )
    assert not spur.is_valid

    boundaries = gm.GeometryArray([ordinary, spur], crs=4326).boundary()

    assert boundaries[0].to_wkt() == 'LINESTRING (0 0, 4 0, 4 4, 0 4, 0 0)'
    assert boundaries[1].to_wkt() == 'LINESTRING (0 0, 4 0, 4 4, 0 4, 0 0)'


def test_packed_geographic_array_boundary_keeps_invalid_coordinate_error() -> None:
    ordinary = gm.box(-2, -1, 2, 1, crs=4326)
    invalid = gm.Polygon(
        [(0, 89), (1, 89), (1, 91), (0, 91), (0, 89)],
        crs=4326,
    )

    with pytest.raises(gm.InvalidGeometryError, match='invalid longitude/latitude'):
        gm.GeometryArray([ordinary, invalid], crs=4326).boundary()


def test_packed_geographic_boundary_rechecks_admission_after_frame_retag() -> None:
    ordinary = _dense_geographic_rectangle(-2, -1, 2, 1, crs=4807)
    seam = _dense_geographic_rectangle(179, 0, 181, 2, crs=4807)
    rows = gm.GeometryArray([ordinary] * 64 + [seam], crs=4807)

    warm = rows.boundary()
    assert '-180' not in warm[64].to_wkt()

    retagged = rows.set_crs(4326, overwrite=True)
    boundary = retagged.boundary()
    assert '-180' in boundary[64].to_wkt()
    assert '-180' not in boundary[0].to_wkt()
    assert gm.relate(retagged[64], boundary[64]) == 'FF21FFFF2'
    assert gm.relate(retagged[0], boundary[0]) == 'FF21FFFF2'


def test_packed_geographic_gather_uses_cached_products_for_physical_row_64() -> None:
    ordinary = _dense_geographic_rectangle(-2, -1, 2, 1, crs=4326)
    spur = gm.Polygon(
        [(0, 0), (4, 0), (4, 4), (0, 4), (0, 0), (-1, 0), (0, 0)],
        crs=4326,
    )
    rows = gm.GeometryArray([ordinary] * 64 + [spur], crs=4326)

    prefix = rows[:64]
    prefix_boundary = prefix.boundary()
    assert all(
        gm.relate(source, boundary) == 'FF21FFFF2'
        for source, boundary in zip(prefix, prefix_boundary, strict=True)
    )

    gathered = rows[[64, 0, 64, 1]]
    boundary = gathered.boundary()
    assert [
        gm.relate(source, result)
        for source, result in zip(gathered, boundary, strict=True)
    ] == [
        'FF21FFFF2',
        'FF21FFFF2',
        'FF21FFFF2',
        'FF21FFFF2',
    ]
    assert boundary[0].to_wkb() == boundary[2].to_wkb()
    assert boundary[1].to_wkb() == boundary[3].to_wkb()


def test_degenerate_polygon_boundary_follows_effective_dimension() -> None:
    cases = [
        (
            'collapsed line support',
            'POLYGON ((0 0,2 2,4 4,0 0))',
            'MULTIPOINT ((0 0), (4 4))',
        ),
        (
            'collapsed point support',
            'POLYGON ((1 2,1 2,1 2,1 2))',
            'GEOMETRYCOLLECTION EMPTY',
        ),
        (
            'collapsed ring inside occupied area',
            'MULTIPOLYGON (((0 0,4 0,4 4,0 4,0 0)),((1 1,2 2,3 3,1 1)))',
            'LINESTRING (0 0, 4 0, 4 4, 0 4, 0 0)',
        ),
    ]
    for name, wkt, expected in cases:
        assert gm.from_wkt(wkt).boundary().to_wkt() == expected, name


def test_bowtie_boundary_publishes_both_occupied_area_cycles() -> None:
    bowtie = gm.from_wkt('POLYGON ((0 0,4 4,0 4,4 0,0 0))')

    assert bowtie.boundary().to_wkt() == (
        'MULTILINESTRING ((0 0, 4 0, 2 2, 0 0), (0 4, 2 2, 4 4, 0 4))'
    )


def test_boundary_can_combine_area_cycles_and_residual_points() -> None:
    shape = gm.from_wkt(
        'MULTIPOLYGON (((0 0,4 0,4 4,0 4,0 0)),((10 0,12 0,14 0,10 0)))'
    )

    assert shape.boundary().to_wkt() == (
        'GEOMETRYCOLLECTION (LINESTRING (0 0, 4 0, 4 4, 0 4, 0 0), '
        'MULTIPOINT ((10 0), (14 0)))'
    )


def test_array_boundary_rejects_the_literal_shell_path_for_invalid_multipolygons() -> (
    None
):
    adjacent = gm.from_wkt(
        'MULTIPOLYGON (((0 0,2 0,2 2,0 2,0 0)),((2 0,4 0,4 2,2 2,2 0)))'
    )
    rows = gm.GeometryArray([gm.box(-2, 0, 0, 2), adjacent])

    boundaries = rows.boundary()

    assert boundaries[0].to_wkt() == 'LINESTRING (-2 0, 0 0, 0 2, -2 2, -2 0)'
    assert boundaries[1].to_wkt() == 'LINESTRING (0 0, 4 0, 4 2, 0 2, 0 0)'


def test_boundary_preserves_source_zm_on_area_and_residual_cells() -> None:
    shape = gm.from_wkt(
        'MULTIPOLYGON ZM ('
        '((0 0 1 11,4 0 2 22,4 4 3 33,2 4 4 44,2 6 5 55,'
        '2 4 4 44,0 4 6 66,0 0 1 11)),'
        '((10 0 5 55,12 0 6 66,14 0 7 77,10 0 5 55)))'
    )

    assert shape.boundary().to_wkt() == (
        'GEOMETRYCOLLECTION (LINESTRING ZM '
        '(0 0 1 11, 4 0 2 22, 4 4 3 33, 2 4 4 44, 0 4 6 66, 0 0 1 11), '
        'MULTIPOINT ZM ((10 0 5 55), (14 0 7 77)))'
    )


def test_geometry_collection_boundary_preserves_member_occurrences() -> None:
    polygon = gm.box(0, 0, 2, 2)
    collection = gm.GeometryCollection([polygon, polygon])
    ring = 'LINESTRING (0 0, 2 0, 2 2, 0 2, 0 0)'

    assert collection.boundary().to_wkt() == f'GEOMETRYCOLLECTION ({ring}, {ring})'


def _edge(left: tuple[float, float], right: tuple[float, float]):
    def point(value: tuple[float, float]) -> tuple[float, float]:
        x, y = value
        x = (x + 180.0) % 360.0 - 180.0
        return (0.0 if x == 0.0 else x, y)

    left = point(left)
    right = point(right)
    return (left, right) if left <= right else (right, left)


def _line_edges(shape: gm.Geometry) -> Counter:
    lines = [shape] if shape.geometry_type == 'LineString' else list(shape.parts)
    edges = Counter()
    for line in lines:
        coordinates = list(line.coords.select('XY'))
        for left, right in pairwise(coordinates):
            edges[_edge(left, right)] += 1
    return edges


THREE_BOX_WKT = (
    'MULTIPOLYGON ('
    '((20 20,40 20,40 30,20 30,20 20)),'
    '((0 10,20 10,20 20,0 20,0 10)),'
    '((170 20,190 20,190 40,170 40,170 20)))'
)
THREE_BOX_EDGES = [
    ((0, 10), (20, 10)),
    ((20, 10), (20, 20)),
    ((20, 20), (0, 20)),
    ((0, 20), (0, 10)),
    ((20, 20), (40, 20)),
    ((40, 20), (40, 30)),
    ((40, 30), (20, 30)),
    ((20, 30), (20, 20)),
    ((170, 40), (170, 20)),
    ((170, 20), (-180, 20)),
    ((-180, 20), (-170, 20)),
    ((-170, 20), (-170, 40)),
    ((-170, 40), (-180, 40)),
    ((-180, 40), (170, 40)),
]
PENTAGON_WKT = (
    'MULTIPOLYGON ('
    '((0 10,20 10,20 20,40 20,0 30,0 10)),'
    '((170 20,190 20,190 40,170 40,170 20)))'
)
PENTAGON_EDGES = [
    ((0, 30), (0, 10)),
    ((0, 10), (20, 10)),
    ((20, 10), (20, 20)),
    ((20, 20), (40, 20)),
    ((40, 20), (0, 30)),
    ((170, 40), (170, 20)),
    ((170, 20), (-180, 20)),
    ((-180, 20), (-170, 20)),
    ((-170, 20), (-170, 40)),
    ((-170, 40), (-180, 40)),
    ((-180, 40), (170, 40)),
]
SEAM_BOUNDARY_CASES = [
    pytest.param(THREE_BOX_WKT, THREE_BOX_EDGES, id='three-box seam union'),
    pytest.param(PENTAGON_WKT, PENTAGON_EDGES, id='pentagon with seam box'),
]


@pytest.mark.parametrize(('wkt', 'expected_edges'), SEAM_BOUNDARY_CASES)
def test_q_boundary_keeps_seam_members_without_artificial_cut_edges(
    wkt: str, expected_edges: list[tuple[tuple[int, int], tuple[int, int]]]
) -> None:
    shape = gm.from_wkt(wkt, crs=4326)
    boundary = shape.boundary()
    expected = Counter(_edge(left, right) for left, right in expected_edges)

    assert _line_edges(boundary) == expected
    assert _edge((180, 20), (180, 40)) not in _line_edges(boundary)


def test_q_boundary_retains_a_single_sided_true_seam_edge() -> None:
    polygon = gm.Polygon(
        [(170, 40), (180, 40), (180, 50), (170, 50)],
        crs=4326,
    )
    expected = Counter(
        _edge(left, right)
        for left, right in [
            ((170, 40), (180, 40)),
            ((180, 40), (180, 50)),
            ((180, 50), (170, 50)),
            ((170, 50), (170, 40)),
        ]
    )

    actual = _line_edges(polygon.boundary())

    assert actual == expected
    assert _edge((180, 40), (180, 50)) in actual


def test_q_boundary_keeps_0_to_360_edge_cells_without_meridian_pairs() -> None:
    polygon = gm.Polygon(
        [(190, 10), (200, 10), (200, 20), (190, 20)],
        crs=4326,
    )
    expected = Counter(
        _edge(left, right)
        for left, right in [
            ((190, 20), (190, 10)),
            ((190, 10), (200, 10)),
            ((200, 10), (200, 20)),
            ((200, 20), (190, 20)),
        ]
    )

    assert _line_edges(polygon.boundary()) == expected


def test_q_boundary_unions_longitude_translated_multipolygon_members() -> None:
    polygons = gm.from_wkt(
        'MULTIPOLYGON ('
        '((0 0,10 0,10 10,0 10,0 0)),'
        '((360 0,370 0,370 10,360 10,360 0)))',
        crs=4326,
    )
    expected = Counter(
        _edge(left, right)
        for left, right in [
            ((0, 10), (0, 0)),
            ((0, 0), (10, 0)),
            ((10, 0), (10, 10)),
            ((10, 10), (0, 10)),
        ]
    )

    assert _line_edges(polygons.boundary()) == expected


def test_geographic_boundary_array_preserves_q_results_through_gathers_and_nulls() -> (
    None
):
    first = gm.from_wkt(THREE_BOX_WKT, crs=4326)
    second = gm.from_wkt(PENTAGON_WKT, crs=4326)
    gathered = gm.GeometryArray([first, None, second], crs=4326)[[2, 1, 0]].boundary()

    assert gathered.is_missing.tolist() == [False, True, False]
    assert gathered[1] is None
    assert _line_edges(gathered[0]) == Counter(
        _edge(left, right) for left, right in PENTAGON_EDGES
    )
    assert _line_edges(gathered[2]) == Counter(
        _edge(left, right) for left, right in THREE_BOX_EDGES
    )


def test_q_boundary_polar_ring_does_not_add_a_cut_or_pole_edge() -> None:
    cap = gm.Polygon(
        [(-179, 80), (-120, 80), (-60, 80), (0, 80), (60, 80), (120, 80), (179, 80)],
        crs=4326,
    )
    expected = Counter(
        _edge(left, right)
        for left, right in [
            ((-60, 80), (0, 80)),
            ((0, 80), (60, 80)),
            ((60, 80), (120, 80)),
            ((120, 80), (179, 80)),
            ((179, 80), (-180, 80)),
            ((-180, 80), (-179, 80)),
            ((-179, 80), (-120, 80)),
            ((-120, 80), (-60, 80)),
        ]
    )

    assert _line_edges(cap.boundary()) == expected


def test_seam_only_polygon_boundary_preserves_odd_endpoint_aliases() -> None:
    shape = gm.from_wkt(
        'POLYGON ((-180 -85,180 -85,180 85,-180 85,-180 -85))',
        crs=4326,
    )

    assert shape.boundary().to_wkt() == 'MULTIPOINT ((180 -85), (-180 85))'


def test_q_boundary_interpolates_source_zm_at_emitted_seam_vertices() -> None:
    polygon = gm.from_wkt(
        'POLYGON ZM ('
        '(170 20 0 100,190 20 20 200,190 40 40 400,'
        '170 40 60 600,170 20 0 100))',
        crs=4326,
    )

    assert polygon.boundary().to_wkt() == (
        'LINESTRING ZM (170 40 60 600, 170 20 0 100, -180 20 10 150, '
        '-170 20 20 200, -170 40 40 400, -180 40 50 500, 170 40 60 600)'
    )


@pytest.mark.parametrize(
    'wkt',
    [
        'LINESTRING (-180 0, 180 0)',
        'LINESTRING (-120 90, 40 90)',
    ],
    ids=['seam aliases', 'pole aliases'],
)
def test_geographic_line_boundary_uses_quotient_endpoint_identity(
    wkt: str,
) -> None:
    line = gm.from_wkt(wkt, crs=4326)
    assert line.boundary().to_wkt() == 'MULTIPOINT EMPTY'
