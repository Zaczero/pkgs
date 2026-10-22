"""Validation and repair — reports with location/path, self-intersection
repair, non-finite rejection, and quantize.
"""

import math
import struct
from typing import Any, cast

import gometry as gm
import numpy as np
import pytest


def _canon_geometries(value: gm.GeometryArray) -> list[str]:
    return [g.to_wkt() for g in value]


def test_single_vertex_lines_are_rejected_at_every_boundary() -> None:
    message = 'line string requires at least two vertices'
    with pytest.raises(ValueError, match=message):
        gm.LineString([(0, 0)])
    with pytest.raises(ValueError, match=message):
        gm.MultiLineString([[(0, 0)], [(1, 1), (2, 2)]])
    with pytest.raises(ValueError, match=message):
        gm.from_wkt('LINESTRING (1 2)')
    with pytest.raises(ValueError, match=message):
        gm.from_geojson('{"type": "LineString", "coordinates": [[1.0, 2.0]]}')
    with pytest.raises(ValueError, match=message):
        gm.from_wkb(
            b'\x01'
            + struct.pack('<I', 2)
            + struct.pack('<I', 1)
            + struct.pack('<2d', 1.0, 2.0)
        )
    singleton = gm.from_polyline('_p~iF~ps|U')
    assert singleton.geometry_type == 'Point'
    assert singleton.to_wkt() == 'POINT (-120.2 38.5)'
    assert singleton.to_polyline() == '_p~iF~ps|U'
    assert gm.LineString([]).is_empty
    assert gm.LineString([(0, 0), (1, 1)]).length > 0


def test_collapsed_line_is_invalid_and_repairs_to_its_first_coordinate() -> None:
    collapsed = gm.from_wkt(
        'LINESTRING ZM (3 4 8 9, 3 4 10 11)',
        crs=3857,
    )
    report = collapsed.validate()

    assert not collapsed.is_valid
    assert (report.code, report.detail, report.location, report.path) == (
        'line_collapsed',
        'line string collapses to fewer than two distinct XY positions',
        (3.0, 4.0),
        '$',
    )
    with pytest.raises(gm.InvalidGeometryError) as invalid:
        gm.require(collapsed)
    assert (invalid.value.code, invalid.value.location, invalid.value.path) == (
        'line_collapsed',
        (3.0, 4.0),
        '$',
    )
    repaired = collapsed.repair()
    assert repaired.to_wkt() == 'POINT ZM (3 4 8 9)'
    assert repaired.crs == gm.CRS('EPSG:3857')
    assert repaired.is_valid

    values = gm.GeometryArray([
        collapsed,
        gm.from_wkt('LINESTRING (0 0, 0 0, 1 0)', crs=3857),
    ])
    np.testing.assert_array_equal(values.is_valid, [False, True])
    assert [report.code for report in values.validate()] == ['line_collapsed', None]
    assert [geometry.to_wkt() for geometry in values.repair()] == [
        'POINT ZM (3 4 8 9)',
        'LINESTRING (0 0, 0 0, 1 0)',
    ]


@pytest.mark.parametrize(
    ('source', 'expected'),
    [
        pytest.param(
            'MULTILINESTRING ((0 0, 0 0), EMPTY, EMPTY)',
            'POINT (0 0)',
            id='one-collapsed-member-and-empties',
        ),
        pytest.param(
            'MULTILINESTRING ((0 0, 0 0), (0 0, 0 0))',
            'MULTIPOINT ((0 0), (0 0))',
            id='duplicate-collapsed-members',
        ),
        pytest.param(
            'MULTILINESTRING ((0 0, 0 0), (1 1, 2 2))',
            'GEOMETRYCOLLECTION (POINT (0 0), LINESTRING (1 1, 2 2))',
            id='one-surviving-line',
        ),
        pytest.param(
            'MULTILINESTRING ((0 0, 0 0), (0 0, 0 0), (1 1, 2 2), (3 3, 4 4))',
            'GEOMETRYCOLLECTION (MULTIPOINT ((0 0), (0 0)), MULTILINESTRING ((1 1, 2 2), (3 3, 4 4)))',
            id='multiple-points-and-lines',
        ),
    ],
)
def test_collapsed_multiline_members_repair_to_geos_geometry_types(
    source: str,
    expected: str,
) -> None:
    geometry = gm.from_wkt(source)

    assert not geometry.is_valid
    repaired = geometry.repair()
    assert repaired.to_wkt() == expected
    assert repaired.is_valid


def test_geometry_collection_repair_keeps_its_outer_type() -> None:
    geometry = gm.from_wkt('GEOMETRYCOLLECTION (LINESTRING (0 0, 0 0), POINT (4 5))')

    repaired = geometry.repair()

    assert repaired.to_wkt() == ('GEOMETRYCOLLECTION (POINT (0 0), POINT (4 5))')
    assert repaired.is_valid


def test_geojson_rejects_dimensionally_mixed_coordinate_sequences() -> None:
    message = 'dimensionally uniform'
    with pytest.raises(ValueError, match=message):
        gm.from_geojson(
            '{"type": "LineString", "coordinates": [[0.0, 0.0], [1.0, 1.0, 5.0]]}'
        )
    with pytest.raises(ValueError, match=message):
        gm.from_geojson(
            '{"type": "MultiPoint", "coordinates": [[0.0, 0.0, 1.0], [1.0, 1.0]]}'
        )
    with pytest.raises(ValueError, match=message):
        gm.from_geojson(
            '{"type": "Polygon", "coordinates": [[[0, 0], [1, 0, 3], [1, 1], [0, 0]]]}'
        )
    line = gm.from_geojson(
        '{"type": "LineString", "coordinates": [[0.0, 0.0, 2.0], [1.0, 1.0, 5.0]]}'
    )
    assert line.has_z


def test_validation_reports_wkb_boundary_ring_location_and_path() -> None:
    """Sub-open WKB rings reject at parse; ≥3 open corners silent-close (A1).

    Rings with fewer than MIN_VERTICES_OPEN coordinates raise ParseError.
    XY-open rings with ≥3 corners silent-close under shared admission and are
    valid after admit. Topology diagnosis still runs for self-cross content.
    """
    # 2 vertices — below MIN_VERTICES_OPEN.
    short = (
        b'\x01'
        + struct.pack('<I', 3)
        + struct.pack('<I', 1)
        + struct.pack('<I', 2)
        + struct.pack('<4d', 0, 0, 1, 0)
    )
    with pytest.raises(gm.ParseError, match=r'ring|coordinates|vertices') as raised:
        gm.from_wkb(short)
    assert type(raised.value).__name__ != 'PanicException'

    # 3 open corners — silent-close (uniform with WKT).
    open3 = (
        b'\x01'
        + struct.pack('<I', 3)
        + struct.pack('<I', 1)
        + struct.pack('<I', 3)
        + struct.pack('<6d', 0, 0, 1, 0, 0, 1)
    )
    tri = gm.from_wkb(open3)
    assert tri.to_wkt() == 'POLYGON ((0 0, 1 0, 0 1, 0 0))'
    assert tri.is_valid

    # XY-open ≥4-vertex rings silent-close (A1) so they are valid after admit.
    unclosed = (
        b'\x01'
        + struct.pack('<I', 3)
        + struct.pack('<I', 1)
        + struct.pack('<I', 4)
        + struct.pack('<8d', 0, 0, 1, 0, 1, 1, 0.5, 1)
    )
    closed = gm.from_wkb(unclosed)
    assert closed.is_valid
    assert closed.validate().valid
    # Bowtie shell: content-level self-intersection after a closed ring.
    bowtie_wkb = gm.Polygon([(0, 0), (2, 2), (0, 2), (2, 0), (0, 0)]).to_wkb()
    report = gm.from_wkb(bowtie_wkb).validate()
    assert not report
    assert report.reason is not None


def test_multipolygon_validation_preserves_collapsed_shell_context_at_member_three() -> (
    None
):
    shell = [(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)]
    collapsed = [(0, 0), (1, 1), (1, 1), (0, 0)]
    geometry = gm.MultiPolygon([
        gm.Polygon(shell),
        gm.Polygon(shell),
        gm.Polygon(shell),
        gm.Polygon(collapsed),
    ])

    report = geometry.validate()

    assert (report.code, report.detail, report.path) == (
        'ring_collapsed',
        'multi polygon member shell collapses to fewer than three distinct vertices',
        '$[3].shell',
    )


def test_validation_reports_and_repairs_self_intersecting_polygons() -> None:
    planar_bowtie = gm.Polygon([(0, 0), (2, 2), (0, 2), (2, 0), (0, 0)])
    report = planar_bowtie.validate()
    assert not report
    assert report.reason is not None
    assert 'self-intersection' in report.reason.lower()
    planar_repaired = planar_bowtie.repair()
    assert planar_repaired.geometry_type == 'MultiPolygon'
    assert planar_repaired.validate().valid
    assert planar_repaired.area == pytest.approx(2)
    geographic_bowtie = gm.Polygon([(0, 0), (2, 2), (0, 2), (2, 0), (0, 0)], crs=4326)
    geographic_repaired = geographic_bowtie.repair()
    assert geographic_repaired.crs == gm.CRS('EPSG:4326')
    assert geographic_repaired.validate().valid


def test_repair_accepts_geometry_directly_and_preserves_crs() -> None:
    bowtie = gm.Polygon([(0, 0), (2, 2), (0, 2), (2, 0), (0, 0)], crs=3857)
    repaired = bowtie.repair()
    assert repaired.crs == gm.CRS('EPSG:3857')
    assert repaired.geometry_type == 'MultiPolygon'
    assert repaired.validate().valid
    assert repaired.area == pytest.approx(2)
    with pytest.raises(gm.GeometryError, match='unknown repair method'):
        bowtie.repair(method=cast('Any', 'unknown'))


def test_rejects_non_finite_coordinates() -> None:
    with pytest.raises(gm.InvalidGeometryError, match='x must be finite'):
        gm.Point(math.nan, 0)


def test_quantize_preserves_crs_and_rounds_coordinates() -> None:
    point = gm.Point(1.23456, 2.34567, crs=4326).quantize(3)
    values = gm.points([1.23456, 9.87654], [2.34567, 8.76543], crs=4326).quantize(2)
    assert point.crs == gm.CRS('EPSG:4326')
    assert point.coords.to_nested() == [1.235, 2.346]
    assert values[0].coords.to_nested() == [1.23, 2.35]
    assert [geometry.coords.to_nested() for geometry in values] == [
        [1.23, 2.35],
        [9.88, 8.77],
    ]
    assert point.quantize(16).coords.to_nested() == point.coords.to_nested()
    huge = gm.Point(1e300, 2e300).quantize(15)
    assert huge.coords.to_nested() == [1e300, 2e300]


@pytest.mark.parametrize(
    ('value', 'precision'),
    [
        (2.675, 2),
        (1.25, 1),
        (1.75, 1),
        (-2.5, 0),
        (149.9, -2),
        (150.0, -2),
        (250.0, -2),
        (-350.0, -2),
        (1.7976931348623157e308, 300),
        (1.7976931348623157e308, -303),
        (1.234567e-20, 22),
        (5e-324, 323),
        (5e-324, 324),
        (1.0, 10**100),
        (-1.0, -(10**100)),
    ],
)
def test_quantize_matches_python_decimal_rounding(value: float, precision: int) -> None:
    expected = round(value, precision)
    point = gm.Point(value, -value, z=value, m=-value)
    assert point.quantize(precision).coords.to_nested() == [
        expected,
        -expected,
        expected,
        -expected,
    ]
    packed = gm.GeometryArray([point, point])
    assert (
        packed.quantize(precision).to_wkt() == [point.quantize(precision).to_wkt()] * 2
    )


def test_quantize_rejects_actual_result_overflow() -> None:
    with pytest.raises(gm.GeometryError, match='finite coordinate range'):
        gm.Point(1.7976931348623157e308, 0).quantize(-308)


@pytest.mark.parametrize('precision', [-2, -1, 0, 2, 6, 7, 22])
def test_quantize_columns_mix_certified_values_ties_and_extremes(
    precision: int,
) -> None:
    values = np.array([
        2.675,
        -2.675,
        1.25,
        1.75,
        12.34567891,
        -87.65432109,
        0.0,
        -0.0,
        1e300,
        1.234567e-20,
        5e-324,
        -5e-324,
        37.123456789,
        52.765432198,
        0.125,
        0.375,
        123.456789,
    ])
    result = gm.points(values, -values, z=values, m=-values).quantize(precision)
    expected = [round(float(value), precision) for value in values]
    np.testing.assert_array_equal([point.x for point in result], expected)
    np.testing.assert_array_equal(
        [point.y for point in result], [-value for value in expected]
    )
    np.testing.assert_array_equal([point.z for point in result], expected)
    np.testing.assert_array_equal(
        [point.m for point in result], [-value for value in expected]
    )


@pytest.mark.parametrize('kind', ['point', 'line', 'polygon'])
@pytest.mark.parametrize('precision', [2, 6, -1, 22, -308])
def test_quantize_packed_kinds_keep_selected_missing_and_empty_rows(
    kind: str,
    precision: int,
) -> None:
    shape = {
        'point': gm.Point(2.675, 9.87654, z=3.45678, m=4.56789),
        'line': gm.LineString(
            [(2.675, 9.87654), (12.34567, 8.76543), (23.45678, 7.65432)],
            z=[3.45678, 4.56789, 5.67891],
            m=[6.78912, 7.89123, 8.91234],
        ),
        'polygon': gm.Polygon(
            [
                (2.675, 9.87654),
                (12.34567, 8.76543),
                (23.45678, 17.65432),
                (2.675, 9.87654),
            ],
            z=[3.45678, 4.56789, 5.67891, 3.45678],
            m=[6.78912, 7.89123, 8.91234, 6.78912],
        ),
    }[kind]
    empty = gm.from_wkt(
        {
            'point': 'POINT ZM EMPTY',
            'line': 'LINESTRING ZM EMPTY',
            'polygon': 'POLYGON ZM EMPTY',
        }[kind]
    )
    dense = gm.GeometryArray([shape] * 17)
    nullable = gm.GeometryArray([shape, None, shape, None])
    with_empty = gm.GeometryArray([empty, shape, None, empty, shape])
    for rows in (
        dense,
        dense[2:13],
        dense[[12, 1, 12, 0]],
        nullable,
        nullable[[3, 0, 1, 2]],
        nullable[[1, 3]],
        with_empty,
        with_empty[[3, 2, 1, 0, 4]],
    ):
        expected = [
            None if row is None else row.quantize(precision).to_wkt() for row in rows
        ]
        assert rows.quantize(precision).to_wkt() == expected


@pytest.mark.parametrize('kind', ['point', 'line', 'polygon'])
def test_quantize_does_not_visit_excluded_overflowing_coordinates(kind: str) -> None:
    maximum = 1.7976931348623157e308
    bad, good = {
        'point': (gm.Point(maximum, 0), gm.Point(1, 2)),
        'line': (
            gm.LineString([(0, 0), (maximum, 1)]),
            gm.LineString([(1, 2), (3, 4)]),
        ),
        'polygon': (
            gm.Polygon([(0, 0), (maximum, 0), (0, 1), (0, 0)]),
            gm.Polygon([(1, 2), (3, 4), (0, 5), (1, 2)]),
        ),
    }[kind]
    rows = gm.GeometryArray([bad, good])
    with pytest.raises(gm.GeometryError, match='finite coordinate range'):
        rows.quantize(-308)
    expected = good.quantize(-308).to_wkt()
    assert rows[1:].quantize(-308).to_wkt() == [expected]
    assert rows[[1, 1]].quantize(-308).to_wkt() == [expected, expected]


def test_is_valid_packed_arrays_match_mixed() -> None:
    """Packed point/line/polygon columns agree with mixed-row arrays."""
    packed_pts = gm.points([0.0, 1.0], [0.0, 1.0], crs=4326)
    mixed_pts = gm.GeometryArray([gm.Point(0, 0), gm.Point(1, 1)], crs=4326)
    assert packed_pts.to_arrow().type.extension_name == 'geoarrow.point'
    np.testing.assert_array_equal(packed_pts.is_valid, [True, True])
    np.testing.assert_array_equal(mixed_pts.is_valid, [True, True])
    np.testing.assert_array_equal(packed_pts.is_valid, mixed_pts.is_valid)
    packed_lines = gm.GeometryArray([
        gm.LineString([(0, 0), (1, 1)]),
        gm.from_wkt('LINESTRING EMPTY'),
    ])
    mixed_lines = gm.from_wkt(['LINESTRING (0 0, 1 1)', 'LINESTRING EMPTY'])
    assert packed_lines.to_arrow().type.extension_name == 'geoarrow.linestring'
    np.testing.assert_array_equal(packed_lines.is_valid, [True, True])
    np.testing.assert_array_equal(mixed_lines.is_valid, [True, True])
    np.testing.assert_array_equal(packed_lines.is_valid, mixed_lines.is_valid)
    square = gm.box(0, 0, 1, 1)
    bowtie = gm.from_wkt('POLYGON ((0 0, 2 2, 2 0, 0 2, 0 0))')
    packed_polys = gm.GeometryArray([square, bowtie], crs=4326)
    mixed_polys = gm.from_wkt(
        ['POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))', 'POLYGON ((0 0, 2 2, 2 0, 0 2, 0 0))'],
        crs=4326,
    )
    assert packed_polys.to_arrow().type.extension_name == 'geoarrow.polygon'
    np.testing.assert_array_equal(packed_polys.is_valid, [True, False])
    np.testing.assert_array_equal(mixed_polys.is_valid, [True, False])
    np.testing.assert_array_equal(packed_polys.is_valid, [True, False])


def test_is_valid_preserves_nullable_packed_and_empty_polygon_rows() -> None:
    empty = gm.from_wkt('POLYGON EMPTY')
    valid = gm.box(0, 0, 1, 1)
    invalid = gm.from_wkt('POLYGON ((2 0, 4 2, 4 0, 2 2, 2 0))')
    values = gm.GeometryArray([empty, None, valid, invalid], crs=4326)

    # This object-constructor fixture keeps the zero-ring row in Mixed storage;
    # the packable nullable sibling below proves the packed polygon bool path.
    np.testing.assert_array_equal(values.is_valid, [True, False, True, False])

    packed = gm.GeometryArray([valid, None, invalid], crs=4326)
    packed_unpickle, _ = packed.drop_missing().__reduce_ex__(5)
    assert packed_unpickle.__name__ == '_unpickle_packed_array_v1'
    np.testing.assert_array_equal(packed.is_valid, [True, False, False])


def test_is_valid_packed_polygons_reject_topology_counterexamples() -> None:
    """The packed validity route must not accept crossing or external holes."""
    valid = gm.from_wkt(
        'POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 1 2, 2 2, 2 1, 1 1))'
    )
    bowtie = gm.from_wkt('POLYGON ((10 0, 12 2, 12 0, 10 2, 10 0))')
    outside_hole = gm.from_wkt(
        'POLYGON ((20 0, 24 0, 24 4, 20 4, 20 0), (30 0, 32 0, 32 2, 30 2, 30 0))'
    )
    values = gm.GeometryArray([valid, bowtie, outside_hole])
    np.testing.assert_array_equal(values.is_valid, [True, False, False])


@pytest.mark.parametrize(
    ('holes', 'expected'),
    [
        (
            '(0 0,4 0,4 4,0 4,0 0),(0 0,2 1,1 2,0 0)',
            False,
        ),
        (
            '(0 0,2 1,1 2,0 0),(0 0,4 0,4 4,0 4,0 0)',
            False,
        ),
        (
            '(2 2,0 2,0 0,2 0,2 2),(2 2,4 3,3 4,2 2)',
            True,
        ),
    ],
    ids=['nested', 'nested-reversed', 'exterior-point-touch'],
)
def test_hole_nesting_is_not_decided_at_a_shared_first_vertex(
    holes: str, expected: bool
) -> None:
    wkt = f'POLYGON((-5 -5,5 -5,5 5,-5 5,-5 -5),{holes})'
    polygon = gm.from_wkt(wkt)
    assert polygon.is_valid is expected
    assert polygon.validate().code == (None if expected else 'hole_overlap')
    rows = gm.GeometryArray([gm.from_wkt(wkt), None])
    np.testing.assert_array_equal(rows.is_valid, [expected, False])


def test_is_valid_is_uniform_across_scalar_and_array() -> None:
    bowtie = gm.from_wkt('POLYGON ((0 0, 1 1, 1 0, 0 1, 0 0))', crs=4326)
    square = gm.box(0, 0, 1, 1)
    assert square.is_valid is True
    assert bowtie.is_valid is False
    array = gm.GeometryArray([square, bowtie], crs=4326)
    np.testing.assert_array_equal(array.is_valid, [True, False])
    reports = array.validate()
    np.testing.assert_array_equal([bool(report) for report in reports], [True, False])
    assert reports[1].reason
    repaired = bowtie.repair()
    assert repaired.is_valid and repaired.crs == gm.CRS('EPSG:4326')


@pytest.mark.parametrize('crs', [4326, 'OGC:CRS84'], ids=['epsg4326', 'crs84'])
def test_geographic_seam_bowtie_is_invalid_with_exact_q_witness(crs: int | str) -> None:
    points = [(170, -10), (-170, 10), (170, 10), (-170, -10), (170, -10)]
    bowtie = gm.Polygon(points, crs=crs)

    report = bowtie.validate()

    assert not bowtie.is_valid
    assert not bowtie.is_simple
    assert (report.code, report.reason) == (
        'self_intersection',
        'exterior ring has a self-intersection',
    )
    # The lifted diagonals meet at (180, 0), canonically spelled (-180, 0).
    assert report.location == (-180.0, 0.0)
    respelled = gm.Polygon([(x + 360, y) for x, y in points], crs=crs)
    assert respelled.validate().location == (-180.0, 0.0)

    planar_unwrap = gm.Polygon([
        (170, -10),
        (190, 10),
        (170, 10),
        (190, -10),
        (170, -10),
    ])
    planar_report = planar_unwrap.validate()
    assert not planar_unwrap.is_valid and not planar_unwrap.is_simple
    assert planar_report.code == report.code
    assert planar_report.location == (180.0, 0.0)

    rows = gm.GeometryArray(
        [
            bowtie,
            gm.Polygon([(170, -10), (-170, -10), (-170, 10), (170, 10), (170, -10)]),
        ],
        crs=crs,
    )
    np.testing.assert_array_equal(rows.is_valid, [False, True])
    np.testing.assert_array_equal(rows.is_simple, [False, True])
    reports = rows.validate()
    assert reports[0] is not None and reports[0].location == report.location


def test_geographic_latitude_domain_is_a_validity_issue_and_operations_still_raise() -> (
    None
):
    polygon = gm.Polygon([(0, 0), (10, 0), (10, 100), (0, 0)], crs=4326)

    report = polygon.validate()

    assert (report.code, report.path, report.location) == (
        'latitude_out_of_range',
        '$.shell',
        (10.0, 100.0),
    )
    assert not polygon.is_valid
    rows = gm.GeometryArray([polygon, gm.box(0, 0, 1, 1, crs=4326)], crs=4326)
    np.testing.assert_array_equal(rows.is_valid, [False, True])
    assert rows.validate()[0].code == 'latitude_out_of_range'
    with pytest.raises(
        gm.InvalidGeometryError,
        match=r'invalid longitude/latitude \(10, 100\)',
    ):
        _ = polygon.area
    for geometry in (polygon, rows):
        with pytest.raises(
            gm.InvalidGeometryError,
            match=r'invalid longitude/latitude \(10, 100\)',
        ):
            _ = geometry.is_simple
    with pytest.raises(
        gm.InvalidGeometryError,
        match=r'invalid longitude/latitude \(10, 100\)',
    ):
        _ = polygon.is_ring
    with pytest.raises(
        gm.InvalidGeometryError,
        match=r'invalid longitude/latitude \(10, 100\)',
    ):
        _ = rows.is_ring
    with pytest.raises(gm.InvalidGeometryError, match='geometry repair failed'):
        polygon.repair()


def test_ambiguous_full_turn_ring_is_reported_and_rejected_at_admission() -> None:
    polygon = gm.Polygon([(0, -10), (360, -10), (300, 10), (0, -10)], crs=4326)

    report = polygon.validate()

    assert (report.code, report.path, report.location) == (
        'ambiguous_full_turn_ring',
        '$.shell',
        (0.0, -10.0),
    )
    assert 'ring winds around the globe and spans the equator' in report.detail
    assert not polygon.is_valid
    rows = gm.GeometryArray([polygon], crs=4326)
    np.testing.assert_array_equal(rows.is_valid, [False])
    assert rows.validate()[0].code == 'ambiguous_full_turn_ring'

    message = 'ring winds around the globe and spans the equator'
    with pytest.raises(gm.InvalidGeometryError, match=message):
        _ = polygon.area
    with pytest.raises(gm.InvalidGeometryError, match=message):
        _ = rows.area
    with pytest.raises(gm.InvalidGeometryError, match=message):
        gm.SpatialIndex([polygon])
    with pytest.raises(gm.InvalidGeometryError, match='geometry repair failed'):
        polygon.repair()


@pytest.mark.parametrize('crs', [4326, 'OGC:CRS84'], ids=['epsg4326', 'crs84'])
@pytest.mark.parametrize('latitude', [90, -90], ids=['north', 'south'])
def test_geographic_pole_backtrack_is_simple_in_scalar_and_array_lanes(
    crs: int | str,
    latitude: int,
) -> None:
    # Every vertex names one pole, so the apparent planar backtrack is null.
    line = gm.LineString([(0, latitude), (20, latitude), (10, latitude)], crs=crs)
    assert line.is_simple
    rows = gm.GeometryArray([line, None], crs=crs)
    np.testing.assert_array_equal(rows.is_simple, [True, False])
    gathered = rows[[0, 1, 0]]
    np.testing.assert_array_equal(gathered.is_simple, [True, False, True])
    assert len(line.self_intersections()) == 0


@pytest.mark.parametrize('crs', [4326, 'OGC:CRS84'], ids=['epsg4326', 'crs84'])
def test_geographic_seam_hole_touches_keep_planar_ogc_validity(crs: int | str) -> None:
    shell = [(170, -10), (-170, -10), (-170, 10), (170, 10), (170, -10)]
    point_touch = [(180, -10), (175, -5), (180, 0), (-175, -5), (180, -10)]
    shared_segment = [(178, -10), (-178, -10), (-179, -5), (179, -5), (178, -10)]
    point_touch_polygon = gm.Polygon(shell, [point_touch], crs=crs)
    shared_segment_polygon = gm.Polygon(shell, [shared_segment], crs=crs)

    assert point_touch_polygon.is_valid and point_touch_polygon.is_simple
    assert point_touch_polygon.validate().valid
    assert not shared_segment_polygon.is_valid
    assert not shared_segment_polygon.is_simple
    report = shared_segment_polygon.validate()
    assert report.code == 'shared_linework'
    # At y=-10 the overlap lifts to [178, 182]; its canonical minimum is -180.
    assert report.location == (-180.0, -10.0)
    overlap = gm.LineString([(178, -10), (-178, -10)], crs=crs)
    assert gm.covers(overlap, gm.Point(*report.location, crs=crs))
    respelled = gm.Polygon(
        [(x + 360, y) for x, y in shell],
        [[(x + 360, y) for x, y in shared_segment]],
        crs=crs,
    )
    assert respelled.validate().location == (-180.0, -10.0)

    planar_point_touch = gm.Polygon(
        [(170, -10), (190, -10), (190, 10), (170, 10), (170, -10)],
        [[(180, -10), (175, -5), (180, 0), (185, -5), (180, -10)]],
    )
    planar_shared_segment = gm.Polygon(
        [(170, -10), (190, -10), (190, 10), (170, 10), (170, -10)],
        [[(178, -10), (182, -10), (181, -5), (179, -5), (178, -10)]],
    )
    assert planar_point_touch.is_valid == point_touch_polygon.is_valid
    assert planar_shared_segment.is_valid == shared_segment_polygon.is_valid

    rows = gm.GeometryArray([point_touch_polygon, shared_segment_polygon], crs=crs)
    np.testing.assert_array_equal(rows.is_valid, [True, False])
    np.testing.assert_array_equal(rows.is_simple, [True, False])
    assert [item.valid for item in rows.validate()] == [True, False]


def test_geographic_ring_self_touch_and_line_crossing_are_not_simple_at_seam() -> None:
    ring_touch = gm.Polygon(
        [
            (170, -10),
            (-180, 0),
            (170, 10),
            (-170, 10),
            (180, 0),
            (-170, -10),
            (170, -10),
        ],
        crs=4326,
    )
    crossing_points = [(170, -10), (-170, 10), (170, 10), (-170, -10)]
    crossing = gm.LineString(crossing_points, crs=4326)
    seam_crossing = gm.LineString([(170, 0), (-170, 1)], crs=4326)

    report = ring_touch.validate()
    assert not ring_touch.is_valid and not ring_touch.is_simple
    assert report.code == 'self_intersection'
    assert not crossing.is_simple
    assert seam_crossing.is_simple
    # The two non-adjacent diagonals meet only at the canonical seam point.
    assert [(point.x, point.y) for point in crossing.self_intersections()] == [
        (-180.0, 0.0)
    ]
    respelled = gm.LineString([(x + 360, y) for x, y in crossing_points], crs=4326)
    assert [(point.x, point.y) for point in respelled.self_intersections()] == [
        (-180.0, 0.0)
    ]

    rows = gm.GeometryArray([crossing, seam_crossing], crs=4326)
    np.testing.assert_array_equal(rows.is_simple, [False, True])


def test_valid_geographic_seam_crossing_polygon_still_splits() -> None:
    box = gm.Polygon(
        [(170, -10), (-170, -10), (-170, 10), (170, 10), (170, -10)],
        crs=4326,
    )

    assert box.is_valid and box.is_simple
    split = box.split_antimeridian()
    assert split.is_valid
    assert split.to_wkt() == (
        'MULTIPOLYGON (((-180 -10, -170 -10, -170 10, -180 10, -180 -10)), '
        '((180 10, 170 10, 170 -10, 180 -10, 180 10)))'
    )


def test_caller_multipolygon_matching_split_output_is_valid() -> None:
    geometry = gm.from_wkt(
        'MULTIPOLYGON (((-180 -10, -170 -10, -170 10, -180 10, -180 -10)), '
        '((180 10, 170 10, 170 -10, 180 -10, 180 10)))',
        crs=4326,
    )

    assert geometry.is_valid and geometry.is_simple


def test_multipolygon_interiors_overlapping_across_seam_are_invalid() -> None:
    geometry = gm.from_wkt(
        'MULTIPOLYGON ('
        '((170 -10, -170 -10, -170 10, 170 10, 170 -10)), '
        '((175 -5, -175 -5, -175 5, 175 5, 175 -5)))',
        crs=4326,
    )

    assert not geometry.is_valid and not geometry.is_simple


def test_multipolygon_edges_crossing_at_the_seam_are_invalid() -> None:
    geometry = gm.from_wkt(
        'MULTIPOLYGON ('
        '((179 -1, -179 1, 179 3, 179 -1)), '
        '((179 1, -179 -1, -179 4, 179 1)))',
        crs=4326,
    )

    assert not geometry.is_valid and not geometry.is_simple


def test_multipolygon_members_touching_at_one_seam_point_are_valid() -> None:
    geometry = gm.from_wkt(
        'MULTIPOLYGON ('
        '((170 -10, 180 -10, 180 0, 170 0, 170 -10)), '
        '((-180 0, -170 0, -170 10, -180 10, -180 0)))',
        crs=4326,
    )

    assert geometry.is_valid and geometry.is_simple


def test_multipolygon_members_sharing_a_seam_edge_from_the_same_side_are_invalid() -> (
    None
):
    geometry = gm.from_wkt(
        'MULTIPOLYGON ('
        '((-180 -10, -170 -10, -170 10, -180 10, -180 -10)), '
        '((180 0, -170 0, -170 15, 180 15, 180 0)))',
        crs=4326,
    )

    assert not geometry.is_valid and not geometry.is_simple


def test_partial_opposite_side_seam_edge_overlap_is_a_cut() -> None:
    geometry = gm.from_wkt(
        'MULTIPOLYGON ('
        '((-180 -10, -170 -10, -170 10, -180 10, -180 -10)), '
        '((180 0, 170 0, 170 15, 180 15, 180 0)))',
        crs=4326,
    )

    assert geometry.is_valid and geometry.is_simple


def test_one_ulp_distinct_touching_holes_are_valid_in_every_row_order() -> None:
    below_one = math.nextafter(1.0, 0.0)
    shell = [(-20, -20), (20, -20), (20, 20), (-20, 20), (-20, -20)]
    first = [(0, 0), (1, 1), (0, 10), (-1, -1), (0, 0)]
    second = [(0, 0), (1, -1), (10, 0), (1, below_one), (0, 0)]
    hole_sets = [
        [first, second],
        [second, first],
        [list(reversed(first)), list(reversed(second))],
        [list(reversed(second)), list(reversed(first))],
    ]
    polygons = [gm.Polygon(shell, holes) for holes in hole_sets]

    assert all(polygon.is_valid for polygon in polygons)
    assert all(polygon.validate().valid for polygon in polygons)

    values = gm.GeometryArray(polygons)
    np.testing.assert_array_equal(values.is_valid, [True] * len(polygons))
    assert all(report.valid for report in values.validate())


BOWTIE_WKT = 'POLYGON ((0 0, 2 2, 2 0, 0 2, 0 0))'


def test_repair_is_deterministic() -> None:
    bowtie = gm.from_wkt(BOWTIE_WKT, crs=4326)
    lines = gm.GeometryArray([
        gm.LineString([(0, 0), (1, 0)]),
        gm.LineString([(1, 0), (1, 1)]),
        gm.LineString([(1, 1), (0, 0)]),
    ])
    assert len({bowtie.repair().to_wkt() for _ in range(20)}) == 1
    assert len({bowtie.repair(method='structure').to_wkt() for _ in range(20)}) == 1
    assert (
        len({'|'.join(gm.polygonize_all(list(lines)).to_wkt()) for _ in range(20)}) == 1
    )


def test_repair_methods_union_distinct_polygon_members() -> None:
    overlap = gm.from_wkt(
        'MULTIPOLYGON (((0 0, 4 0, 4 4, 0 4, 0 0)), ((2 2, 6 2, 6 6, 2 6, 2 2)))'
    )
    linework = overlap.repair()
    structure = overlap.repair(method='structure')
    assert linework.geometry_type == 'Polygon'
    assert linework.area == pytest.approx(28)
    assert structure.geometry_type == 'Polygon'
    assert structure.area == pytest.approx(28)
    square = '((0 0, 1 0, 1 1, 0 1, 0 0))'
    duplicated = gm.from_wkt(f'MULTIPOLYGON ({square}, {square})')
    assert duplicated.repair().area == pytest.approx(1)
    assert duplicated.repair(method='structure').area == pytest.approx(1)


def test_repair_structure_fills_wound_shells() -> None:
    import math

    n = 600
    ring = []
    for k in range(n):
        a = k * 7 * 2 * math.pi / n % (2 * math.pi)
        r = 1.0 + 0.4 * math.sin(k * 60 * math.pi / n)
        ring.append((math.cos(a) * r * 50 + 50, math.sin(a) * r * 30 + 50))
    poly = gm.Polygon(ring)
    linework = poly.repair()
    structure = poly.repair(method='structure')
    assert linework.is_valid and structure.is_valid
    assert structure.area > linework.area * 1.3
    assert gm.covers(structure, linework)


def test_repair_parity_and_valid_fast_path() -> None:
    bowtie = gm.from_wkt(BOWTIE_WKT, crs=4326)
    square = gm.box(0, 0, 1, 1, crs=4326)
    expected = bowtie.repair().to_wkt()
    assert bowtie.repair().to_wkt() == expected
    assert bowtie.repair().to_wkt() == expected
    array = gm.GeometryArray([bowtie, square], crs=4326)
    repaired = array.repair()
    assert repaired[0].to_wkt() == expected
    assert all(repaired.is_valid)
    assert array.repair().to_wkt() == repaired.to_wkt()
    assert square.repair() == square
    valid = gm.GeometryArray([square, gm.box(2, 2, 3, 3)], crs=4326)
    assert list(valid.repair()) == list(valid)


def test_linework_repair_keeps_bowtie_and_nested_hole_regions() -> None:
    bowtie = gm.from_wkt(BOWTIE_WKT)
    nested_holes = gm.from_wkt(
        'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), '
        '(2 2, 8 2, 8 8, 2 8, 2 2), '
        '(4 4, 6 4, 6 6, 4 6, 4 4))'
    )

    repaired = gm.GeometryArray([bowtie, nested_holes]).repair()

    assert all(repaired.is_valid)
    assert repaired[0].geometry_type == 'MultiPolygon'
    assert repaired[0].area == pytest.approx(2)
    assert repaired[1].area == pytest.approx(64)
    assert not gm.covers(repaired[1], gm.Point(3, 3))
    assert not gm.covers(repaired[1], gm.Point(5, 5))


def test_repair_drops_conflicting_z_from_the_rebuild() -> None:
    bowtie_z = gm.from_wkt('POLYGON Z ((0 0 1, 2 2 5, 2 0 3, 0 2 7, 0 0 1))')
    repaired = bowtie_z.repair()
    assert repaired.coordinate_axes == 'XY'


def test_polygonize_assembles_one_universe_across_array_rows() -> None:
    lines = gm.GeometryArray([
        gm.LineString([(0, 0), (1, 0)]),
        gm.LineString([(1, 0), (1, 1)]),
        gm.LineString([(1, 1), (0, 0)]),
    ])
    # The free `polygonize_all` pools ALL rows' edges into one planar graph, so a ring
    # closes from edges spread across rows.
    polygons = gm.polygonize_all(lines)
    assert gm.polygonize_all(lines[0]) == lines[0].polygonize()
    assert (
        gm.polygonize_all(lines[0], diagnostics=True).polygons
        == gm.polygonize_all([lines[0]], diagnostics=True).polygons
    )
    assert len(polygons) == 1
    assert polygons[0].area == pytest.approx(0.5)
    # The per-row array method polygonizes each edge alone — a lone segment
    # closes nothing, so every group is empty (contrast with the aggregate).
    per_row = lines.polygonize()
    assert isinstance(per_row, gm.Groups)
    assert [len(per_row[i]) for i in range(len(per_row))] == [0, 0, 0]
    # Diagnostics preserve the pooled polygons and add source-row linework.
    result = gm.polygonize_all(lines, diagnostics=True)
    full_polygons, cuts, dangles, invalid = result
    assert result.polygons == polygons
    assert len(full_polygons) == 1
    assert [sum(len(row) for row in groups) for groups in (cuts, dangles, invalid)] == [
        0,
        0,
        0,
    ]
    assert not hasattr(lines, 'polygonize_full')

    masked = gm.GeometryArray([lines[0], None, lines[1], lines[2]])
    assert gm.polygonize_all(masked) == polygons
    assert gm.polygonize_all(masked, diagnostics=True).polygons == full_polygons


def test_polygonize_auto_nodes_crossing_linework() -> None:
    lines = gm.MultiLineString([
        [(0, 0), (2, 2)],
        [(0, 2), (2, 0)],
        [(0, 0), (2, 0)],
        [(2, 0), (2, 2)],
        [(2, 2), (0, 2)],
        [(0, 2), (0, 0)],
    ])

    polygons = lines.polygonize()

    assert len(polygons) == 4
    assert [polygon.area for polygon in polygons] == [1.0] * 4


def test_polygonize_full_groups_noded_diagnostics_by_source_row() -> None:
    square = gm.LineString([(0, 0), (2, 0), (2, 2), (0, 2), (0, 0)])
    crossing = gm.LineString([(-1, 1), (3, 1)])

    result = gm.polygonize_all(gm.GeometryArray([square, crossing]), diagnostics=True)

    assert len(result.dangles) == 2
    assert len(result.dangles[0]) == 0
    assert [line.to_wkt() for line in result.dangles[1]] == [
        'LINESTRING (2 1, 3 1)',
        'LINESTRING (-1 1, 0 1)',
    ]
    assert [
        sum(len(row) for row in groups)
        for groups in (
            result.cut_edges,
            result.invalid_rings,
        )
    ] == [0, 0]


def test_geographic_polygonize_full_keeps_seam_faces_and_source_diagnostics() -> None:
    crs = '+proj=longlat +a=7000000 +rf=150 +type=crs'
    lines = gm.GeometryArray([
        gm.LineString([(170, 0), (-170, 0)], crs=crs),
        None,
        gm.LineString([(-170, 0), (-170, 10)], crs=crs),
        gm.LineString([(-170, 10), (170, 10)], crs=crs),
        gm.LineString([(170, 10), (170, 0)], crs=crs),
        gm.LineString([(-170, 5, 7, 70), (-160, 5, 17, 170)], crs=crs),
    ])

    result = gm.polygonize_all(lines, diagnostics=True)

    assert len(result.polygons) == 1
    polygon = result.polygons[0]
    assert polygon.crs == gm.CRS(crs)
    assert gm.covers(polygon, gm.Point(179, 5, crs=crs))
    assert gm.covers(polygon, gm.Point(-179, 5, crs=crs))
    assert not gm.covers(polygon, gm.Point(0, 5, crs=crs))
    assert len(result.dangles) == 6
    assert result.dangles[1] is None
    assert [len(result.dangles[index]) for index in (0, 2, 3, 4)] == [0, 0, 0, 0]
    assert len(result.dangles[5]) == 1
    diagnostic = result.dangles[5][0]
    assert diagnostic.geometry_type == 'LineString'
    assert diagnostic.coordinate_axes == 'XYZM'
    assert diagnostic.crs == gm.CRS(crs)
    assert diagnostic.coords.to_nested() == [
        (-170.0, 5.0, 7.0, 70.0),
        (-160.0, 5.0, 17.0, 170.0),
    ]
    assert all(row is None or len(row) == 0 for row in result.cut_edges)
    assert all(row is None or len(row) == 0 for row in result.invalid_rings)


def test_geographic_polar_polygonize_full_keeps_cap_face() -> None:
    crs = '+proj=longlat +a=7000000 +rf=150 +type=crs'
    lines = gm.GeometryArray([
        gm.LineString([(0, 80), (90, 80)], crs=crs),
        gm.LineString([(90, 80), (180, 80)], crs=crs),
        gm.LineString([(180, 80), (270, 80)], crs=crs),
        gm.LineString([(270, 80), (0, 80)], crs=crs),
    ])

    result = gm.polygonize_all(lines, diagnostics=True)

    assert len(result.polygons) == 1
    cap = result.polygons[0]
    assert cap.crs == gm.CRS(crs)
    assert gm.covers(cap, gm.Point(45, 85, crs=crs))
    assert gm.covers(cap, gm.Point(-135, 85, crs=crs))
    assert not gm.covers(cap, gm.Point(45, 75, crs=crs))
    assert all(len(row) == 0 for row in result.cut_edges)
    assert all(len(row) == 0 for row in result.dangles)
    assert all(len(row) == 0 for row in result.invalid_rings)


def test_polygonize_full_diagnostic_rows_scope_conflicting_z_and_empty_rows() -> None:
    first = gm.from_wkt('LINESTRING Z (0 0 1, 1 0 2)')
    missing = None
    second = gm.from_wkt('LINESTRING Z (0 0 99, -1 0 100)')

    result = gm.polygonize_all(
        gm.GeometryArray([first, missing, second]), diagnostics=True
    )

    assert len(result.dangles) == 3
    assert len(result.dangles[0]) == 1
    assert result.dangles[0][0].coordinate_axes == 'XYZ'
    assert sorted(result.dangles[0][0].coords.to_nested()) == [
        (0.0, 0.0, 1.0),
        (1.0, 0.0, 2.0),
    ]
    assert result.dangles[1] is None
    assert len(result.dangles[2]) == 1
    assert result.dangles[2][0].coordinate_axes == 'XYZ'
    assert sorted(result.dangles[2][0].coords.to_nested()) == [
        (-1.0, 0.0, 100.0),
        (0.0, 0.0, 99.0),
    ]


def test_polygonize_full_diagnostic_rows_scope_mixed_axes() -> None:
    z_line = gm.from_wkt('LINESTRING Z (0 0 1, 1 0 2)')
    m_line = gm.from_wkt('LINESTRING M (0 0 10, -1 0 20)')

    result = gm.polygonize_all(
        gm.GeometryArray([z_line, None, m_line]), diagnostics=True
    )

    assert len(result.dangles) == 3
    assert result.dangles[0][0].coordinate_axes == 'XYZ'
    assert sorted(result.dangles[0][0].coords.to_nested()) == [
        (0.0, 0.0, 1.0),
        (1.0, 0.0, 2.0),
    ]
    assert result.dangles[1] is None
    assert result.dangles[2][0].coordinate_axes == 'XYM'
    assert sorted(result.dangles[2][0].coords.to_nested()) == [
        (-1.0, 0.0, 20.0),
        (0.0, 0.0, 10.0),
    ]


def test_pickle_and_copy_round_trip_all_data_types() -> None:
    """Data types pickle (multiprocessing/caching just work); Z/M, leaf type,
    CRS, and epoch all survive. copy/deepcopy ride the same protocol.
    """
    import copy
    import pickle

    point = gm.Point(1, 2, crs=4326, epoch=2020.5)
    restored = pickle.loads(pickle.dumps(point))
    assert type(restored) is gm.Point
    assert restored == point and restored.epoch == 2020.5
    assert copy.copy(point) == point and copy.deepcopy(point) == point
    zm_line = gm.from_wkt('LINESTRING ZM (0 0 1 10, 1 1 2 20)', crs=3857)
    assert pickle.loads(pickle.dumps(zm_line)) == zm_line
    assert pickle.loads(pickle.dumps(gm.from_wkt('POLYGON EMPTY'))).is_empty
    packed = gm.points([1.0, 2.0], [3.0, 4.0], crs=4326, epoch=2020.5)
    restored_packed = pickle.loads(pickle.dumps(packed))
    assert list(restored_packed) == list(packed)
    assert (restored_packed.crs, restored_packed.epoch) == (gm.CRS('EPSG:4326'), 2020.5)
    assert restored_packed.to_arrow().type.extension_name == 'geoarrow.point'
    lines = gm.GeometryArray(
        [
            gm.LineString([(0, 0, 5), (1, 1, 6), (2, 0, 7)]),
            gm.LineString([(3, 3, 8), (4, 4, 9)]),
        ],
        crs=4326,
    )
    restored_lines = pickle.loads(pickle.dumps(lines))
    assert list(restored_lines) == list(lines)
    assert restored_lines.crs == gm.CRS('EPSG:4326')
    assert restored_lines.to_arrow().type.extension_name == 'geoarrow.linestring'
    polygons = gm.GeometryArray(
        [
            gm.from_wkt(
                'POLYGON Z ((0 0 1, 4 0 1, 4 4 1, 0 4 1, 0 0 1), (1 1 1, 2 1 1, 2 2 1, 1 2 1, 1 1 1))'
            ),
            gm.from_wkt('POLYGON Z ((10 10 2, 14 10 2, 14 14 2, 10 14 2, 10 10 2))'),
        ],
        crs=4326,
        epoch=2020.5,
    )
    restored_polygons = pickle.loads(pickle.dumps(polygons))
    assert [str(item) for item in restored_polygons] == [str(item) for item in polygons]
    assert (restored_polygons.crs, restored_polygons.epoch) == (
        gm.CRS('EPSG:4326'),
        2020.5,
    )
    assert restored_polygons.to_arrow().type.extension_name == 'geoarrow.polygon'
    mixed = gm.GeometryArray([gm.box(0, 0, 1, 1, crs=4326), gm.Point(2, 3, crs=4326)])
    assert list(pickle.loads(pickle.dumps(mixed))) == list(mixed)
    crs = gm.CRS(32633)
    assert pickle.loads(pickle.dumps(crs)) == crs
    h3_cell = gm.H3Cell(13.4, 52.5, resolution=9)
    assert pickle.loads(pickle.dumps(h3_cell)) == h3_cell
    s2_cell = gm.S2Cell(13.4, 52.5, level=12)
    assert pickle.loads(pickle.dumps(s2_cell)).token == s2_cell.token


def test_self_intersections_reports_every_nonsimple_node() -> None:
    bowtie = gm.LineString([(0, 0), (2, 0), (1, 1), (1, -1)])
    assert [(p.x, p.y) for p in bowtie.self_intersections()] == [(1.0, 0.0)]
    touch = gm.LineString([(0, 0), (2, 0), (2, 1), (1, 0)])
    assert [(p.x, p.y) for p in touch.self_intersections()] == [(1.0, 0.0)]
    spike = gm.LineString([(0, 0), (2, 0), (1, 0)])
    assert {(p.x, p.y) for p in spike.self_intersections()} == {(2.0, 0.0), (1.0, 0.0)}
    repeated = gm.LineString([(0, 0), (1, 1), (1, 1), (2, 0)])
    assert repeated.is_simple
    assert [(p.x, p.y) for p in repeated.self_intersections()] == []
    assert gm.from_wkt('POLYGON ((0 0, 0 0, 4 0, 4 4, 0 4, 0 0))').is_valid
    assert not gm.from_wkt('POLYGON ((0 0, 0 0, 0 0, 0 0))').is_valid
    assert not gm.from_wkt('POLYGON ((0 0, 4 0, 4 0, 0 0))').is_valid
    joined = gm.from_wkt('MULTILINESTRING ((0 0, 1 1), (1 1, 2 0))')
    assert joined.is_simple
    assert len(joined.self_intersections()) == 0
    tee = gm.from_wkt('MULTILINESTRING ((0 0, 2 0), (1 0, 1 5))')
    assert not tee.is_simple
    assert [(p.x, p.y) for p in tee.self_intersections()] == [(1.0, 0.0)]
    ringed = gm.from_wkt('MULTILINESTRING ((0 0, 1 0, 1 1, 0 0), (2 2, 0 0))')
    assert not ringed.is_simple
    assert {(p.x, p.y) for p in ringed.self_intersections()} == {(0.0, 0.0), (1.0, 1.0)}
    dupes = gm.from_wkt('MULTIPOINT ((1 1), (0 0), (1 1), (1 1))')
    assert [(p.x, p.y) for p in dupes.self_intersections()] == [(1.0, 1.0)]
    pinched = gm.from_wkt('POLYGON ((0 0, 2 2, 2 0, 0 2, 0 0))')
    assert [(p.x, p.y) for p in pinched.self_intersections()] == [(1.0, 1.0)]
    ring = gm.LineString([(0, 0), (1, 0), (1, 1), (0, 0)], crs=4326)
    assert len(ring.self_intersections()) == 0
    assert ring.self_intersections().crs == gm.CRS('EPSG:4326')
    assert len(gm.Point(1, 1).self_intersections()) == 0
    assert len(gm.from_wkt('LINESTRING EMPTY').self_intersections()) == 0
    battery = [
        'POINT (0 0)',
        'MULTIPOINT ((0 0), (1 1))',
        'MULTIPOINT ((0 0), (0 0))',
        'LINESTRING (0 0, 1 0, 1 1)',
        'LINESTRING (0 0, 2 0, 1 1, 1 -1)',
        'LINESTRING (0 0, 1 1, 1 1, 2 0)',
        'MULTILINESTRING ((0 0, 1 0), (0 1, 1 1))',
        'MULTILINESTRING ((0 0, 1 1), (1 1, 2 0))',
        'GEOMETRYCOLLECTION (LINESTRING (0 0, 1 0), POINT (5 5))',
    ]
    for wkt in battery:
        geometry = gm.from_wkt(wkt)
        if geometry.geometry_type == 'GeometryCollection':
            continue
        assert bool(len(geometry.self_intersections())) == (not geometry.is_simple), wkt
    titan = gm.LineString([
        (1e308, 1e308),
        (-1e308, -1e308),
        (1e308, -1e308),
        (-1e308, 1e308),
    ])
    assert not titan.is_simple
    assert len(titan.self_intersections()) == 1
    nested = gm.from_wkt('GEOMETRYCOLLECTION (MULTIPOINT ((0 0), (0 0)))')
    assert [(p.x, p.y) for p in nested.self_intersections()] == [(0.0, 0.0)]
    twins = gm.from_wkt('GEOMETRYCOLLECTION (POINT (3 3), POINT (3 3))')
    assert [(p.x, p.y) for p in twins.self_intersections()] == [(3.0, 3.0)]
    rows = gm.GeometryArray([bowtie, gm.LineString([(0, 0), (1, 0), (1, 1), (0, 0)])])
    per_row = rows.self_intersections()
    assert [(p.x, p.y) for p in per_row[0]] == [(1.0, 0.0)]
    assert len(per_row[1]) == 0
    assert [(p.x, p.y) for p in bowtie.self_intersections()] == [(1.0, 0.0)]
    assert len(rows.self_intersections()[1]) == 0


def test_repeated_consecutive_vertices_are_removable_redundancy() -> None:
    """A duplicate consecutive vertex is stutter, not topology: valid/simple
    and behaviourally identical to the deduped geometry across every
    operation, while a ring that stutters to nothing stays invalid.
    """
    shapely = pytest.importorskip('shapely')
    dup = gm.from_wkt('POLYGON ((0 0, 0 0, 4 0, 4 4, 0 4, 0 0))')
    clean = gm.from_wkt('POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))')
    other = gm.from_wkt('POLYGON ((2 2, 6 2, 6 6, 2 6, 2 2))')
    point_in = gm.from_wkt('POINT (1 1)')
    assert dup.is_valid and dup.is_simple
    assert shapely.from_wkt(dup.to_wkt()).is_valid
    assert gm.equals(dup, clean)
    assert dup.area == clean.area == 16
    for op in ('union', 'intersection', 'difference', 'symmetric_difference'):
        assert gm.equals(getattr(gm, op)(dup, other), getattr(gm, op)(clean, other))
    for pred in ('intersects', 'touches', 'covers', 'contains', 'disjoint'):
        assert getattr(gm, pred)(dup, point_in) == getattr(gm, pred)(clean, point_in)
        assert getattr(gm, pred)(dup, other) == getattr(gm, pred)(clean, other)
    assert gm.relate(dup, other) == gm.relate(clean, other)
    assert dup.self_intersections().__len__() == 0
    assert gm.coverage_is_valid([dup, other]) == gm.coverage_is_valid([clean, other])
    assert gm.coverage_invalid_edges([dup, other]) == gm.coverage_invalid_edges([
        clean,
        other,
    ])
    arr = gm.GeometryArray([dup, other])
    # These rows overlap, so the public check-and-do simplifier must reject
    # both presentations consistently rather than silently simplifying an
    # invalid coverage.
    with pytest.raises(gm.InvalidGeometryError, match='valid polygonal coverage'):
        gm.coverage_simplify(arr, 0.1)
    with pytest.raises(gm.InvalidGeometryError, match='valid polygonal coverage'):
        gm.coverage_simplify([clean, other], 0.1)
    assert gm.from_wkt('LINESTRING (0 0, 1 1, 1 1, 2 0)').is_simple
    assert not gm.from_wkt('POLYGON ((0 0, 0 0, 0 0, 0 0))').is_valid
    assert not gm.from_wkt('POLYGON ((0 0, 4 0, 4 0, 0 0))').is_valid


def test_validity_reports_indexed_witnesses() -> None:
    """The indexed validity kernel reports each issue with a witness location,
    including disconnected interiors.
    """
    disconnected = gm.from_wkt(
        'POLYGON ((0 0, 8 0, 8 8, 0 8, 0 0), (0 4, 4 2, 8 4, 4 6, 0 4))'
    )
    report = disconnected.validate()
    assert not report.valid
    assert report.reason == 'interior is disconnected'
    assert report.location is not None
    crossing = gm.from_wkt(
        'POLYGON ((0 0, 8 0, 8 8, 0 8, 0 0), (4 4, 2 6, 2 2, 4 4), (4 4, 6 2, 6 6, 4 4))'
    )
    from shapely import from_wkt as swkt

    assert crossing.is_valid == bool(swkt(crossing.to_wkt()).is_valid)
    fan = gm.from_wkt(
        'POLYGON ((0 0, 9 0, 9 9, 0 9, 0 0), (4 4, 3 2, 5 2, 4 4), (4 4, 2 5, 2 3, 4 4), (4 4, 6 5, 5 6, 4 4))'
    )
    assert fan.is_valid
    for wkt in (
        'POLYGON ((0 0, 4 4, 4 0, 0 4, 0 0))',
        'POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0), (5 5, 6 5, 6 6, 5 6, 5 5))',
        'MULTIPOLYGON (((0 0, 4 0, 4 4, 0 4, 0 0)), ((2 2, 6 2, 6 6, 2 6, 2 2)))',
    ):
        issue = gm.from_wkt(wkt).validate()
        assert not issue.valid
        assert issue.location is not None, wkt
