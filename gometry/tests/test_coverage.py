"""Polygonal-coverage suite — validation, topology-preserving simplification,
and cleaning (the GEOS/PostGIS coverage model over a GeometryArray).
"""

import math

import gometry as gm
import numpy as np
import pytest

from tests._support import oblique_chart_arclength_fraction


def grid_rows() -> list[gm.Polygon]:
    return [
        gm.box(0, 0, 1, 1),
        gm.box(1, 0, 2, 1),
        gm.box(0, 1, 1, 2),
        gm.box(1, 1, 2, 2),
    ]


def test_coverage_is_valid_verdicts() -> None:
    grid = gm.GeometryArray(grid_rows())
    assert gm.coverage_is_valid(grid)
    assert all(g.is_empty for g in gm.coverage_invalid_edges(grid))
    overlap = gm.GeometryArray([gm.box(0, 0, 1.1, 1), gm.box(1, 0, 2, 1)])
    assert not gm.coverage_is_valid(overlap)
    assert not any(g.is_empty for g in gm.coverage_invalid_edges(overlap))
    tjoin = gm.GeometryArray([
        gm.box(0, 0, 1, 2),
        gm.from_wkt('POLYGON ((1 0, 2 0, 2 1, 1 1, 1 0))'),
        gm.from_wkt('POLYGON ((1 1, 2 1, 2 2, 1 2, 1 1))'),
    ])
    assert not gm.coverage_is_valid(tjoin)
    gap = gm.GeometryArray([gm.box(0, 0, 0.999, 1), gm.box(1, 0, 2, 1)])
    assert gm.coverage_is_valid(gap)
    assert not gm.coverage_is_valid(gap, gap_width=0.01)
    assert gm.coverage_is_valid(grid_rows())
    assert gm.coverage_is_valid(gm.box(0, 0, 1, 1))
    assert len(gm.coverage_invalid_edges(gm.box(0, 0, 1, 1))) == 1
    assert len(gm.coverage_clean(gm.box(0, 0, 1, 1))) == 1
    assert len(gm.coverage_simplify(gm.box(0, 0, 1, 1), 0.0)) == 1
    assert gm.coverage_union_all(gm.box(0, 0, 1, 1)).geometry_type == 'Polygon'
    edges = gm.coverage_invalid_edges([
        gm.box(0, 0, 1.1, 1, crs=3857),
        gm.box(1, 0, 2, 1, crs=3857),
    ])
    assert edges.crs == gm.CRS('EPSG:3857')
    with pytest.raises(gm.GeometryTypeError, match='Polygon or MultiPolygon'):
        gm.coverage_is_valid([gm.Point(0, 0)])
    with pytest.raises(gm.GeometryError, match='gap_width'):
        gm.coverage_is_valid(grid, gap_width=-1)


def test_geographic_disjoint_rows_are_valid_without_invalid_edges() -> None:
    seam = gm.Polygon(
        [(170, 0), (-170, 0), (-170, 10), (170, 10)],
        crs='OGC:CRS84',
    )
    greenwich = gm.Polygon(
        [(-5, 2), (5, 2), (5, 8), (-5, 8)],
        crs='OGC:CRS84',
    )
    rows = [seam, greenwich]

    assert gm.coverage_is_valid(rows)
    assert all(edge.is_empty for edge in gm.coverage_invalid_edges(rows))


def test_geographic_overlap_is_invalid_and_reports_source_edges() -> None:
    rows = [
        gm.box(0, 0, 2, 2, crs='OGC:CRS84'),
        gm.box(1, 0.5, 3, 1.5, crs='OGC:CRS84'),
    ]

    assert not gm.coverage_is_valid(rows)
    assert any(not edge.is_empty for edge in gm.coverage_invalid_edges(rows))


def test_geographic_max_area_overlap_uses_ellipsoidal_area() -> None:
    rows = [
        gm.box(0, 60, 10, 85, crs='OGC:CRS84'),
        gm.box(0, 50, 10, 70, crs='OGC:CRS84'),
    ]

    cleaned = gm.coverage_clean(rows, grid_size=0, overlap_rule='max_area')
    overlap = gm.Point(5, 65, crs='OGC:CRS84')

    assert not gm.covers(cleaned[0], overlap)
    assert gm.covers(cleaned[1], overlap)


def test_geographic_min_area_overlap_uses_ellipsoidal_area() -> None:
    rows = [
        gm.box(0, 60, 10, 85, crs='OGC:CRS84'),
        gm.box(0, 50, 10, 70, crs='OGC:CRS84'),
    ]

    cleaned = gm.coverage_clean(rows, grid_size=0, overlap_rule='min_area')
    overlap = gm.Point(5, 65, crs='OGC:CRS84')

    assert gm.covers(cleaned[0], overlap)
    assert not gm.covers(cleaned[1], overlap)


def test_geographic_longest_border_uses_ellipsoidal_chart_edge_length() -> None:
    rows = [
        gm.box(0, 60, 10, 80, crs='OGC:CRS84'),
        gm.box(0, 70, 10, 90, crs='OGC:CRS84'),
    ]

    cleaned = gm.coverage_clean(rows, grid_size=0, overlap_rule='longest_border')
    overlap = gm.Point(5, 75, crs='OGC:CRS84')

    # The rows share equal chart-length borders with the overlap, but the
    # border at 70 degrees is longer on the ellipsoid than the one at 80.
    assert not gm.covers(cleaned[0], overlap)
    assert gm.covers(cleaned[1], overlap)


def test_geographic_t_junction_is_invalid_and_reports_source_edges() -> None:
    rows = [
        gm.box(0, 0, 1, 2, crs='OGC:CRS84'),
        gm.from_wkt(
            'POLYGON ((1 0, 2 0, 2 1, 1 1, 1 0))',
            crs='OGC:CRS84',
        ),
        gm.from_wkt(
            'POLYGON ((1 1, 2 1, 2 2, 1 2, 1 1))',
            crs='OGC:CRS84',
        ),
    ]

    assert not gm.coverage_is_valid(rows)
    assert any(not edge.is_empty for edge in gm.coverage_invalid_edges(rows))


def test_geographic_gap_width_uses_native_coordinate_units() -> None:
    rows = [
        gm.box(0, 0, 1, 1, crs='OGC:CRS84'),
        gm.box(1.005, 0, 2, 1, crs='OGC:CRS84'),
    ]

    assert gm.coverage_is_valid(rows)
    assert not gm.coverage_is_valid(rows, gap_width=0.01)
    assert any(
        not edge.is_empty for edge in gm.coverage_invalid_edges(rows, gap_width=0.01)
    )


def test_geographic_gap_merges_into_ellipsoidally_longest_border_neighbor() -> None:
    rows = [
        gm.box(0, 78, 100, 79, crs='OGC:CRS84'),
        gm.box(-0.1, 79, 0, 79.1, crs='OGC:CRS84'),
        gm.box(100, 79, 100.1, 79.1, crs='OGC:CRS84'),
        gm.box(0, 79.1, 100, 80, crs='OGC:CRS84'),
    ]

    cleaned = gm.coverage_clean(
        rows,
        grid_size=0,
        gap_width=0.2,
        overlap_rule='min_index',
    )
    gap = gm.Point(50, 79.05, crs='OGC:CRS84')

    assert gm.covers(cleaned[0], gap)
    assert not gm.covers(cleaned[3], gap)


def test_geographic_disjoint_coverage_preserves_nullable_rows() -> None:
    seam = gm.Polygon(
        [(170, 0), (-170, 0), (-170, 10), (170, 10)],
        crs='OGC:CRS84',
    )
    greenwich = gm.Polygon(
        [(-5, 2), (5, 2), (5, 8), (-5, 8)],
        crs='OGC:CRS84',
    )
    rows = gm.GeometryArray([seam, None, greenwich])

    assert gm.coverage_is_valid(rows)
    edges = gm.coverage_invalid_edges(rows)
    assert edges.is_missing.tolist() == [False, True, False]
    assert edges[0].is_empty
    assert edges[2].is_empty


def test_geographic_clean_keeps_valid_disjoint_rows_as_identity() -> None:
    seam = gm.Polygon(
        [(170, 0), (-170, 0), (-170, 10), (170, 10)],
        crs='OGC:CRS84',
    )
    greenwich = gm.Polygon(
        [(-5, 2), (5, 2), (5, 8), (-5, 8)],
        crs='OGC:CRS84',
    )
    rows = gm.GeometryArray([seam, greenwich])

    cleaned = gm.coverage_clean(rows)
    assert cleaned.to_wkt() == rows.to_wkt()
    assert gm.coverage_is_valid(cleaned)


def test_geographic_nonidentity_clean_assigns_seam_overlap_to_input_owner() -> None:
    crs = '+proj=longlat +a=7000000 +rf=150 +type=crs'
    first = gm.Polygon(
        [
            (170, 0, 1, 10),
            (-165, 0, 11, 60),
            (-165, 10, 21, 110),
            (170, 10, 31, 160),
            (170, 0, 1, 10),
        ],
        crs=crs,
    )
    second = gm.Polygon(
        [(175, 0), (-160, 0), (-160, 10), (175, 10), (175, 0)],
        crs=crs,
    )

    cleaned = gm.coverage_clean(
        [first, second],
        grid_size=0,
        overlap_rule='min_index',
    )

    # In the continuous longitude chart, the source rows cover [170, 195]
    # and [175, 200]. The overlap belongs to row zero, leaving row one only
    # [195, 200].
    probes = [
        gm.Point(172.5, 5, crs=crs),
        gm.Point(177, 5, crs=crs),
        gm.Point(-172, 5, crs=crs),
        gm.Point(-162.5, 5, crs=crs),
        gm.Point(0, 5, crs=crs),
    ]
    assert [[gm.covers(row, point) for row in cleaned] for point in probes] == [
        [True, False],
        [True, False],
        [True, False],
        [False, True],
        [False, False],
    ]
    assert gm.coverage_is_valid(cleaned)
    assert cleaned[0].coordinate_axes == 'XYZM'

    reversed_rows = gm.coverage_clean(
        [second, first],
        grid_size=0,
        overlap_rule='min_index',
    )
    assert [[gm.covers(row, point) for row in reversed_rows] for point in probes] == [
        [False, True],
        [True, False],
        [True, False],
        [True, False],
        [False, False],
    ]
    assert gm.coverage_is_valid(reversed_rows)


def test_geographic_nonidentity_clean_scatters_nullable_rows_after_publication() -> (
    None
):
    crs = '+proj=longlat +a=7000000 +rf=150 +type=crs'
    first = gm.Polygon(
        [
            (170, 0, 1, 10),
            (-165, 0, 11, 60),
            (-165, 10, 21, 110),
            (170, 10, 31, 160),
            (170, 0, 1, 10),
        ],
        crs=crs,
    )
    second = gm.Polygon(
        [(175, 0), (-160, 0), (-160, 10), (175, 10), (175, 0)],
        crs=crs,
    )
    rows = gm.GeometryArray([first, None, second])

    cleaned = gm.coverage_clean(rows, grid_size=0, overlap_rule='min_index')

    assert cleaned.is_missing.tolist() == [False, True, False]
    assert cleaned.crs == gm.CRS(crs)
    assert cleaned[1] is None
    assert gm.covers(cleaned[0], gm.Point(177, 5, crs=crs))
    assert not gm.covers(cleaned[2], gm.Point(177, 5, crs=crs))
    assert gm.covers(cleaned[2], gm.Point(-162.5, 5, crs=crs))
    assert gm.coverage_is_valid(cleaned)


def test_geographic_nonidentity_clean_assigns_polar_overlap_to_input_owner() -> None:
    crs = '+proj=longlat +a=7000000 +rf=150 +type=crs'

    def polar_cap(latitude: float, offset: float) -> gm.Polygon:
        return gm.Polygon(
            [
                (0, latitude, offset, 100 + offset),
                (90, latitude, offset + 10, 200 + offset),
                (180, latitude, offset + 20, 300 + offset),
                (270, latitude, offset + 30, 400 + offset),
                (0, latitude, offset, 100 + offset),
            ],
            crs=crs,
        )

    outer = polar_cap(80, 1)
    inner = polar_cap(85, 10)
    polar_probes = [
        gm.Point(0, 89, crs=crs),
        gm.Point(45, 82, crs=crs),
        gm.Point(45, 79, crs=crs),
    ]
    polar_cleaned = gm.coverage_clean(
        [outer, inner],
        grid_size=0,
        overlap_rule='min_index',
    )
    assert [
        [gm.covers(row, point) for row in polar_cleaned] for point in polar_probes
    ] == [
        [True, False],
        [True, False],
        [False, False],
    ]
    reversed_polar = gm.coverage_clean(
        [inner, outer],
        grid_size=0,
        overlap_rule='min_index',
    )
    assert [
        [gm.covers(row, point) for row in reversed_polar] for point in polar_probes
    ] == [
        [True, False],
        [False, True],
        [False, False],
    ]


def test_geographic_coverage_simplify_rebuilds_an_unshared_polar_cap() -> None:
    polar = gm.Polygon(
        [(0, 80), (90, 80), (180, 80), (270, 80)],
        crs='OGC:CRS84',
    )
    equator = gm.box(-5, -5, 5, 5, crs='OGC:CRS84')
    rows = gm.GeometryArray([polar, equator])
    assert gm.coverage_is_valid(rows)

    simplified = gm.coverage_simplify(rows, 1.0, method='dp')

    assert gm.coverage_is_valid(simplified)
    assert gm.covers(simplified[0], gm.Point(0, 89, crs='OGC:CRS84'))
    assert not gm.covers(simplified[0], gm.Point(45, 70, crs='OGC:CRS84'))
    assert gm.covers(simplified[1], gm.Point(0, 0, crs='OGC:CRS84'))


def test_geographic_zero_tolerance_simplify_keeps_valid_disjoint_rows_as_identity() -> (
    None
):
    seam = gm.Polygon(
        [(170, 0), (-170, 0), (-170, 10), (170, 10)],
        crs='OGC:CRS84',
    )
    greenwich = gm.Polygon(
        [(-5, 2), (5, 2), (5, 8), (-5, 8)],
        crs='OGC:CRS84',
    )
    rows = gm.GeometryArray([seam, greenwich])

    simplified = gm.coverage_simplify(rows, 0.0)
    assert simplified.to_wkt() == rows.to_wkt()
    assert gm.coverage_is_valid(simplified)


def test_nonzero_geographic_simplify_keeps_disjoint_chart_support() -> None:
    seam = gm.Polygon(
        [(170, 0), (-170, 0), (-170, 10), (170, 10)],
        crs='OGC:CRS84',
    )
    greenwich = gm.Polygon(
        [(-5, -1), (5, -1), (5, 1), (-5, 1)],
        crs='OGC:CRS84',
    )

    simplified = gm.coverage_simplify([seam, greenwich], 0.5)

    assert gm.coverage_is_valid(simplified)
    assert gm.covers(simplified[0], gm.Point(175, 5, crs='OGC:CRS84'))
    assert not gm.covers(simplified[0], gm.Point(0, 0, crs='OGC:CRS84'))
    assert gm.covers(simplified[1], gm.Point(0, 0, crs='OGC:CRS84'))
    assert not gm.covers(simplified[1], gm.Point(175, 5, crs='OGC:CRS84'))


def test_nonzero_geographic_simplify_reduces_disjoint_row_boundary() -> None:
    seam = gm.Polygon(
        [(170, 0), (-170, 0), (-170, 10), (170, 10)],
        crs='OGC:CRS84',
    )
    central = gm.Polygon(
        [(-5, -1), (5, -1), (5, 1), (0, 1.2), (-5, 1)],
        crs='OGC:CRS84',
    )

    simplified = gm.coverage_simplify([seam, central], 0.5, method='dp')

    assert len(simplified[1].exterior.coords) < len(central.exterior.coords)
    assert gm.coverage_is_valid(simplified)


def test_nonzero_geographic_simplify_preserves_measured_disjoint_rows() -> None:
    crs = '+proj=longlat +a=7000000 +rf=150 +type=crs'
    ring = [
        (0.0, 45.0, 35.25, 551.0),
        (90.0, 54.0, 40.0, 600.0),
        (180.0, 63.0, 45.0, 675.0),
        (275.0, 72.5, 49.5, 726.0),
        (275.0, 82.5, 60.0, 800.0),
        (180.0, 73.0, 55.0, 775.0),
        (90.0, 64.0, 50.0, 650.0),
        (0.0, 55.0, 45.0, 600.0),
        (0.0, 45.0, 35.25, 551.0),
    ]
    source = gm.Polygon(ring, crs=crs)
    separate = gm.box(-5, -80, -1, -75, crs=crs)

    simplified = gm.coverage_simplify([source, separate], 0.1, method='vw')

    assert gm.coverage_is_valid(simplified)
    assert simplified[0].has_z
    assert simplified[0].has_m
    source_ordinates = {(x, y): (z, m) for x, y, z, m in ring}
    for x, y, z, m in simplified[0].exterior.coords:
        if (x, y) in source_ordinates:
            assert (z, m) == source_ordinates[(x, y)]
        else:
            assert math.isfinite(z)
            assert math.isfinite(m)


def test_nonzero_geographic_shared_seam_simplify_keeps_the_interface() -> None:
    west = gm.Polygon(
        [(170, 0), (180, 0), (180, 5), (180, 10), (170, 10)],
        crs='OGC:CRS84',
    )
    east = gm.Polygon(
        [(-180, 0), (-170, 0), (-170, 10), (-180, 10), (-180, 6)],
        crs='OGC:CRS84',
    )

    simplified = gm.coverage_simplify([west, east], 2.0, method='dp')

    assert gm.coverage_is_valid(simplified)
    assert gm.covers(
        simplified[0].boundary(),
        gm.Point(180, 5, crs='OGC:CRS84'),
    )
    assert gm.covers(
        simplified[1].boundary(),
        gm.Point(-180, 5, crs='OGC:CRS84'),
    )
    assert gm.covers(simplified[0], gm.Point(175, 5, crs='OGC:CRS84'))
    assert gm.covers(simplified[1], gm.Point(-175, 5, crs='OGC:CRS84'))


def test_nonzero_geographic_shared_seam_simplify_replays_reversed_zm_owners() -> None:
    crs = '+proj=longlat +a=7000000 +rf=150 +type=crs'
    west = gm.Polygon(
        [
            (170, 0, 1, 10),
            (180, 0, 2, 20),
            (180, 4, 3, 30),
            (180, 6, 4, 40),
            (180, 10, 5, 50),
            (170, 10, 6, 60),
            (170, 0, 1, 10),
        ],
        crs=crs,
    )
    east = gm.Polygon(
        [
            (-180, 0, 2, 20),
            (-170, 0, 7, 70),
            (-170, 10, 8, 80),
            (-180, 10, 5, 50),
            (-180, 7, 4.5, 45),
            (-180, 6, 4, 40),
            (-180, 4, 3, 30),
            (-180, 0, 2, 20),
        ],
        crs=crs,
    )

    simplified = gm.coverage_simplify([west, east], 2.0, method='dp')

    probes = [
        gm.Point(175, 5, crs=crs),
        gm.Point(-175, 5, crs=crs),
        gm.Point(180, 5, crs=crs),
        gm.Point(-180, 5, crs=crs),
        gm.Point(0, 5, crs=crs),
    ]
    assert [[gm.covers(row, point) for row in simplified] for point in probes] == [
        [True, False],
        [False, True],
        [True, True],
        [True, True],
        [False, False],
    ]
    assert [len(row.exterior.coords) for row in simplified] == [5, 5]
    assert [row.coordinate_axes for row in simplified] == ['XYZM', 'XYZM']
    assert gm.coverage_is_valid(simplified)


def test_geographic_disjoint_coverage_union_keeps_its_output_bytes() -> None:
    seam = gm.Polygon(
        [(170, 0), (-170, 0), (-170, 10), (170, 10)],
        crs='OGC:CRS84',
    )
    greenwich = gm.Polygon(
        [(-5, 2), (5, 2), (5, 8), (-5, 8)],
        crs='OGC:CRS84',
    )

    assert gm.coverage_union_all([seam, greenwich]).to_wkt() == (
        'MULTIPOLYGON (((-180 0, -170 0, -170 10, -180 10, -180 0)), '
        '((180 10, 170 10, 170 0, 180 0, 180 10)), '
        '((-5 2, 5 2, 5 8, -5 8, -5 2)))'
    )


def test_geographic_seam_coverage_union_keeps_its_output_bytes() -> None:
    west = gm.Polygon(
        [(170, 0), (180, 0), (180, 10), (170, 10)],
        crs='OGC:CRS84',
    )
    east = gm.Polygon(
        [(-180, 0), (-170, 0), (-170, 10), (-180, 10)],
        crs='OGC:CRS84',
    )

    assert gm.coverage_union_all([west, east]).to_wkt() == (
        'MULTIPOLYGON (((170 0, 180 0, 180 10, 170 10, 170 0)), '
        '((-180 0, -170 0, -170 10, -180 10, -180 0)))'
    )


def test_geographic_coverage_union_keeps_presplit_original_zm_arclength() -> None:
    from math import cos, hypot, radians, sin, sqrt

    crs = '+proj=longlat +a=7000000 +rf=150 +type=crs'
    crossing = gm.Polygon(
        [
            (165, -40, 10, 100),
            (-175, 20, 70, 700),
            (-170, 20, 80, 800),
            (-170, -30, 90, 900),
            (165, -40, 10, 100),
        ],
        crs=crs,
    )
    separate = gm.box(20, 0, 21, 1, crs=crs).with_coordinates(z=200, m=2000)
    assert gm.coverage_is_valid([crossing, separate])

    semi_major = 7_000_000.0
    flattening = 1.0 / 150.0
    eccentricity_squared = flattening * (2.0 - flattening)
    latitude_rate = radians(60.0)
    longitude_rate = radians(20.0)
    intervals = 4096

    def chart_edge_length(start: float, end: float) -> float:
        step = (end - start) / intervals
        weighted_speeds = []
        for index in range(intervals + 1):
            parameter = start + index * step
            latitude = radians(-40.0 + 60.0 * parameter)
            denominator = 1.0 - eccentricity_squared * sin(latitude) ** 2
            meridian_radius = (
                semi_major * (1.0 - eccentricity_squared) / denominator**1.5
            )
            prime_vertical_radius = semi_major / sqrt(denominator)
            speed = hypot(
                meridian_radius * latitude_rate,
                prime_vertical_radius * cos(latitude) * longitude_rate,
            )
            weight = 1 if index in (0, intervals) else 4 if index % 2 else 2
            weighted_speeds.append(weight * speed)
        return step * math.fsum(weighted_speeds) / 3.0

    source_fraction = chart_edge_length(0.0, 0.75) / chart_edge_length(0.0, 1.0)
    expected_z = 10.0 + 60.0 * source_fraction
    expected_m = 100.0 + 600.0 * source_fraction

    for rows in ([crossing, separate], [separate, crossing]):
        union = gm.coverage_union_all(rows)
        seam_records = {
            tuple(float(value) for value in coordinate)
            for polygon in union.parts
            for coordinate in polygon.exterior.coords
            if abs(float(coordinate[0])) == 180.0 and float(coordinate[1]) == 5.0
        }
        assert seam_records
        assert all(abs(record[0]) == 180.0 for record in seam_records)
        assert all(record[1] == 5.0 for record in seam_records)
        assert all(
            record[2:] == pytest.approx((expected_z, expected_m), abs=2e-9)
            for record in seam_records
        )
        assert union.coordinate_axes == 'XYZM'
        assert gm.covers(union, gm.Point(180, 0, crs=crs))
        assert gm.covers(union, gm.Point(20.5, 0.5, crs=crs))
        assert not gm.covers(union, gm.Point(0, 0, crs=crs))


def test_geographic_coverage_union_keeps_original_roots_after_seam_dissolve() -> None:
    crs = '+proj=longlat +a=7000000 +rf=150 +type=crs'
    first_start = (165, -20, 1, 10)
    first_lower_end = (-165, -10, 11, 110)
    shared_end = (-165, 30, 21, 210)
    second_upper_end = (165, 20, 31, 310)
    first = gm.Polygon(
        [first_start, first_lower_end, shared_end, first_start],
        crs=crs,
    )
    second = gm.Polygon(
        [first_start, shared_end, second_upper_end, first_start],
        crs=crs,
    )
    assert gm.coverage_is_valid([first, second])

    semi_major = 7_000_000.0
    flattening = 1.0 / 150.0
    eccentricity_squared = flattening * (2.0 - flattening)
    latitude_rate = math.radians(10.0)
    longitude_rate = math.radians(30.0)
    intervals = 4096

    def chart_edge_length(start: float, end: float) -> float:
        step = (end - start) / intervals
        weighted_speeds = []
        for index in range(intervals + 1):
            parameter = start + index * step
            latitude = math.radians(-20.0 + 10.0 * parameter)
            denominator = 1.0 - eccentricity_squared * math.sin(latitude) ** 2
            meridian_radius = (
                semi_major * (1.0 - eccentricity_squared) / denominator**1.5
            )
            prime_vertical_radius = semi_major / math.sqrt(denominator)
            speed = math.hypot(
                meridian_radius * latitude_rate,
                prime_vertical_radius * math.cos(latitude) * longitude_rate,
            )
            weight = 1 if index in (0, intervals) else 4 if index % 2 else 2
            weighted_speeds.append(weight * speed)
        return step * math.fsum(weighted_speeds) / 3.0

    source_fraction = chart_edge_length(0.0, 0.5) / chart_edge_length(0.0, 1.0)
    expected_z = 1.0 + 10.0 * source_fraction
    expected_m = 10.0 + 100.0 * source_fraction

    for rows in ([first, second], [second, first]):
        union = gm.coverage_union_all(rows)
        assert union.coordinate_axes == 'XYZM'
        seam_records = {
            tuple(float(value) for value in coordinate)
            for polygon in (
                union.parts if union.geometry_type == 'MultiPolygon' else [union]
            )
            for coordinate in polygon.exterior.coords
            if abs(float(coordinate[0])) == 180.0 and float(coordinate[1]) == -15.0
        }
        assert seam_records
        assert all(abs(record[0]) == 180.0 for record in seam_records)
        assert all(
            record[2:] == pytest.approx((expected_z, expected_m), abs=2e-9)
            for record in seam_records
        )
        assert gm.covers(union, gm.Point(179, 0, crs=crs))
        assert gm.covers(union, gm.Point(-179, 0, crs=crs))
        assert not gm.covers(union, gm.Point(0, 0, crs=crs))


def test_geographic_coverage_union_keeps_oblique_source_ordinates() -> None:
    crs = '+proj=longlat +a=7000000 +rf=150 +type=crs'
    fraction = oblique_chart_arclength_fraction((0.0, 10.0), (60.0, 40.0), 0.5)
    expected_z = 10.0 + 200.0 * fraction
    assert fraction - 0.5 > 0.01
    left = gm.Polygon(
        [
            (0.0, 10.0, 10.0),
            (30.0, 25.0, expected_z),
            (60.0, 40.0, 210.0),
            (60.0, 70.0, 220.0),
            (0.0, 40.0, 20.0),
            (0.0, 10.0, 10.0),
        ],
        crs=crs,
    )
    right = gm.Polygon(
        [
            (60.0, 40.0, 210.0),
            (90.0, 40.0, 30.0),
            (90.0, 70.0, 40.0),
            (60.0, 70.0, 220.0),
            (60.0, 40.0, 210.0),
        ],
        crs=crs,
    )
    assert gm.coverage_is_valid([left, right])

    union = gm.coverage_union_all([left, right])

    assert union.coordinate_axes == 'XYZ'
    polygons = union.parts if union.geometry_type == 'MultiPolygon' else [union]
    samples = [
        coordinate
        for polygon in polygons
        for coordinate in polygon.exterior.coords
        if coordinate[:2] == (30.0, 25.0)
    ]
    assert samples
    assert all(
        sample[2] == pytest.approx(expected_z, rel=0.0, abs=2e-9) for sample in samples
    )
    assert abs(samples[0][2] - 110.0) > 1.0


def test_geographic_coverage_union_keeps_oblique_xyz_at_inserted_seam_vertices() -> (
    None
):
    crs = '+proj=longlat +a=7000000 +rf=150 +type=crs'
    source = gm.Polygon(
        [
            (120.0, 10.0, 10.0),
            (-120.0, 70.0, 210.0),
            (-120.0, 80.0, 220.0),
            (120.0, 20.0, 20.0),
            (120.0, 10.0, 10.0),
        ],
        crs=crs,
    )
    fraction = oblique_chart_arclength_fraction((120.0, 10.0), (240.0, 70.0), 0.5)
    expected_z = 10.0 + 200.0 * fraction
    assert fraction - 0.5 > 0.07
    assert abs(expected_z - 110.0) > 14.0

    union = gm.coverage_union_all(source)

    assert union.coordinate_axes == 'XYZ'
    polygons = union.parts if union.geometry_type == 'MultiPolygon' else [union]
    seam_samples = [
        coordinate
        for polygon in polygons
        for coordinate in polygon.exterior.coords
        if abs(coordinate[0]) == 180.0 and coordinate[1] == 40.0
    ]
    assert seam_samples
    assert all(
        coordinate[2] == pytest.approx(expected_z, rel=0.0, abs=2e-8)
        for coordinate in seam_samples
    )


def test_coverage_is_valid_keeps_subnormal_gap_width_in_distance_space() -> None:
    scale = 1.0e-162
    rows = gm.GeometryArray([
        gm.box(0.0, 0.0, scale, scale),
        gm.box(2.0 * scale, 0.0, 3.0 * scale, scale),
    ])

    assert gm.coverage_is_valid(rows)
    assert not gm.coverage_is_valid(rows, gap_width=1.1 * scale)


def test_coverage_free_functions_accept_geometry_array() -> None:
    grid = gm.GeometryArray(grid_rows())
    assert gm.coverage_is_valid(grid)
    assert gm.coverage_invalid_edges(grid)[0].is_empty
    assert gm.coverage_is_valid(gm.coverage_simplify(grid, 0.0))
    assert gm.coverage_is_valid(gm.coverage_clean(grid, grid_size=0.0))
    assert gm.coverage_union_all(grid).geometry_type == 'Polygon'


def test_coverage_union_normalize_is_stable() -> None:
    """Canonical presentation lives on normalize(); raw ring start may vary."""
    tiles = [gm.box(0, 0, 1, 1), gm.box(1, 0, 2, 1)]
    union = gm.coverage_union_all(tiles)
    assert union.area == pytest.approx(2.0)
    canonical = {union.normalize().to_wkt() for _ in range(20)}
    assert len(canonical) == 1
    assert canonical.pop() == 'POLYGON ((0 0, 1 0, 2 0, 2 1, 1 1, 0 1, 0 0))'


def test_coverage_union_dissolves_shared_edges() -> None:
    grid = [gm.box(i, j, i + 1, j + 1) for i in range(3) for j in range(3)]
    union = gm.coverage_union_all(grid)
    assert union.geometry_type == 'Polygon'
    assert union.area == pytest.approx(9.0)
    ring = [
        gm.box(i, j, i + 1, j + 1)
        for i in range(3)
        for j in range(3)
        if not (i == 1 and j == 1)
    ]
    holed = gm.coverage_union_all(ring)
    assert holed.area == pytest.approx(8.0)
    disjoint = gm.coverage_union_all([gm.box(0, 0, 1, 1), gm.box(5, 5, 6, 6)])
    assert disjoint.geometry_type == 'MultiPolygon'
    assert disjoint.area == pytest.approx(2.0)
    assert gm.coverage_union_all([gm.box(0, 0, 1, 1, crs=3857)]).crs == gm.CRS(
        'EPSG:3857'
    )
    with pytest.raises(gm.GeometryTypeError, match='Polygon or MultiPolygon'):
        gm.coverage_union_all([gm.Point(0, 0)])
    with pytest.raises(gm.InvalidGeometryError):
        gm.coverage_union_all([])


def test_coverage_union_keeps_point_touching_parts_separate() -> None:
    """An all-exterior edge inventory may bypass arrangement without merging a touch."""
    touching = [gm.box(0, 0, 1, 1), gm.box(1, 1, 2, 2)]
    union = gm.coverage_union_all(touching)
    assert union.geometry_type == 'MultiPolygon'
    assert union.area == pytest.approx(2.0)


def test_coverage_union_mixed_axes_stays_homogeneous_in_sparse_path() -> None:
    xy = gm.box(0, 0, 1, 1)
    xyz = gm.box(3, 0, 4, 1).with_coordinates(z=7.0)
    xym = gm.box(6, 0, 7, 1).with_coordinates(m=9.0)
    xyzm = gm.box(9, 0, 10, 1).with_coordinates(z=11.0, m=12.0)
    xyz_second = gm.box(12, 0, 13, 1).with_coordinates(z=8.0)
    explicit_empty_xy = gm.from_wkt('MULTIPOLYGON (EMPTY, ((0 0, 1 0, 1 1, 0 1, 0 0)))')
    explicit_empty_xyz = gm.from_wkt(
        'MULTIPOLYGON Z (EMPTY, ((3 0 7, 4 0 8, 4 1 9, 3 1 10, 3 0 7)))'
    )

    cases = [
        (
            'xy_xyz',
            xy,
            xyz,
            'XY',
            [
                (0.0, 0.0),
                (1.0, 0.0),
                (1.0, 1.0),
                (0.0, 1.0),
                (0.0, 0.0),
                (3.0, 0.0),
                (4.0, 0.0),
                (4.0, 1.0),
                (3.0, 1.0),
                (3.0, 0.0),
            ],
        ),
        (
            'xyz_xym',
            xyz,
            xym,
            'XY',
            [
                (3.0, 0.0),
                (4.0, 0.0),
                (4.0, 1.0),
                (3.0, 1.0),
                (3.0, 0.0),
                (6.0, 0.0),
                (7.0, 0.0),
                (7.0, 1.0),
                (6.0, 1.0),
                (6.0, 0.0),
            ],
        ),
        (
            'xyz_xyzm',
            xyz,
            xyzm,
            'XYZ',
            [
                (3.0, 0.0, 7.0),
                (4.0, 0.0, 7.0),
                (4.0, 1.0, 7.0),
                (3.0, 1.0, 7.0),
                (3.0, 0.0, 7.0),
                (9.0, 0.0, 11.0),
                (10.0, 0.0, 11.0),
                (10.0, 1.0, 11.0),
                (9.0, 1.0, 11.0),
                (9.0, 0.0, 11.0),
            ],
        ),
        (
            'xyz_xyz',
            xyz,
            xyz_second,
            'XYZ',
            [
                (3.0, 0.0, 7.0),
                (4.0, 0.0, 7.0),
                (4.0, 1.0, 7.0),
                (3.0, 1.0, 7.0),
                (3.0, 0.0, 7.0),
                (12.0, 0.0, 8.0),
                (13.0, 0.0, 8.0),
                (13.0, 1.0, 8.0),
                (12.0, 1.0, 8.0),
                (12.0, 0.0, 8.0),
            ],
        ),
        (
            'explicit_empty_xy_xyz',
            explicit_empty_xy,
            explicit_empty_xyz,
            'XY',
            [
                (0.0, 0.0),
                (1.0, 0.0),
                (1.0, 1.0),
                (0.0, 1.0),
                (0.0, 0.0),
                (3.0, 0.0),
                (4.0, 0.0),
                (4.0, 1.0),
                (3.0, 1.0),
                (3.0, 0.0),
            ],
        ),
    ]
    for name, left, right, axes, expected in cases:
        for order, rows in (('forward', (left, right)), ('reverse', (right, left))):
            label = f'{name}/{order}'
            result = gm.coverage_union_all(list(rows))
            assert result.geometry_type == 'MultiPolygon', label
            assert result.coordinate_axes == axes, label
            assert result.area == pytest.approx(2.0), label
            assert all(part.coordinate_axes == axes for part in result.parts), label
            actual = [
                coordinate
                for polygon in result.coords.to_nested()
                for ring in polygon
                for coordinate in ring
            ]
            assert sorted(actual) == sorted(expected), label


def test_coverage_union_all_empty_members_use_declared_union_axes() -> None:
    cases = [
        (
            gm.from_wkt('MULTIPOLYGON (EMPTY)'),
            gm.from_wkt('MULTIPOLYGON Z (EMPTY)'),
            'XYZ',
        ),
        (
            gm.from_wkt('MULTIPOLYGON Z (EMPTY)'),
            gm.from_wkt('MULTIPOLYGON M (EMPTY)'),
            'XYZM',
        ),
    ]
    for left, right, axes in cases:
        for rows in ((left, right), (right, left)):
            result = gm.coverage_union_all(list(rows))
            assert result.geometry_type == 'Polygon'
            assert result.is_empty
            assert result.coordinate_axes == axes
            assert result.area == 0.0


def test_coverage_union_uses_exact_closed_support_for_invalid_polygon_rows() -> None:
    """Exact coverage dissolve uses a polygon's closed shell support."""
    bowtie = gm.from_wkt('POLYGON ((0 0, 2 2, 0 2, 2 0, 0 0))')
    disjoint = gm.box(5, 0, 6, 1)
    assert gm.coverage_is_valid([bowtie, disjoint])
    result = gm.coverage_union_all([bowtie, disjoint])
    assert result.geometry_type == 'MultiPolygon'
    assert result.area == pytest.approx(3.0)


def test_coverage_union_matches_shapely_oracle() -> None:
    import shapely

    cases = [
        [gm.box(i, j, i + 1, j + 1) for i in range(4) for j in range(4)],
        [
            gm.box(i, j, i + 1, j + 1)
            for i in range(3)
            for j in range(3)
            if not (i == 1 and j == 1)
        ],
        [gm.box(0, 0, 1, 1), gm.box(2, 2, 3, 3)],
    ]
    for rows in cases:
        ours = gm.coverage_union_all(rows)
        theirs = shapely.coverage_union_all(
            shapely.from_wkt([g.to_wkt() for g in rows])
        )
        assert ours.area == pytest.approx(shapely.area(theirs)), [
            g.to_wkt() for g in rows
        ]


def test_coverage_validity_matches_shapely_oracle() -> None:
    import shapely

    cases = [
        grid_rows(),
        [gm.box(0, 0, 1.05, 1), gm.box(1, 0, 2, 1)],
        [
            gm.box(0, 0, 1, 2),
            gm.from_wkt('POLYGON ((1 0, 2 0, 2 1, 1 1, 1 0))'),
            gm.from_wkt('POLYGON ((1 1, 2 1, 2 2, 1 2, 1 1))'),
        ],
        [gm.box(0, 0, 1, 1), gm.box(2, 2, 3, 3)],
    ]
    for rows in cases:
        ours = gm.coverage_is_valid(rows)
        theirs = bool(
            shapely.coverage_is_valid([shapely.from_wkt(g.to_wkt()) for g in rows])
        )
        assert ours == theirs, [g.to_wkt() for g in rows]


def test_three_polygons_sharing_one_edge_is_invalid() -> None:
    """An interface must be matched by exactly two rows; a third claimant is invalid."""
    triple = gm.GeometryArray([
        gm.box(0, 0, 1, 1),
        gm.box(1, 0, 2, 1),
        gm.box(1, 0, 1.5, 1),
    ])
    assert not gm.coverage_is_valid(triple)
    assert not any(g.is_empty for g in gm.coverage_invalid_edges(triple))


@pytest.mark.parametrize('copies', [2, 3, 5])
def test_coincident_rows_are_invalid_coverage(copies: int) -> None:
    """Repeated boundaries enclose overlap, not a shared interface."""
    first = gm.box(0, 0, 1, 1)
    # Equivalent presentation with a different ring start and orientation.
    second = gm.from_wkt('POLYGON ((1 1, 1 0, 0 0, 0 1, 1 1))')
    rows = gm.GeometryArray([first, *[second] * (copies - 1)])
    assert not gm.coverage_is_valid(rows)
    assert all(not edge.is_empty for edge in gm.coverage_invalid_edges(rows))
    for operation in (
        lambda: gm.coverage_union_all(rows),
        lambda: gm.coverage_simplify(rows, 0.0),
    ):
        with pytest.raises(gm.InvalidGeometryError, match='valid polygonal coverage'):
            operation()


def test_adjacent_coverage_rows_can_have_identical_bounds() -> None:
    rows = gm.from_wkt([
        'POLYGON ((0 0, 1 0, 1 1, 0 0))',
        'POLYGON ((0 0, 1 1, 0 1, 0 0))',
    ])
    assert gm.coverage_is_valid(rows)
    assert all(edge.is_empty for edge in gm.coverage_invalid_edges(rows))
    assert gm.equals(gm.coverage_union_all(rows), gm.box(0, 0, 1, 1))


def test_check_and_do_coverage_operations_reject_invalid_input() -> None:
    overlap = gm.GeometryArray([gm.box(0, 0, 1.2, 1), gm.box(1, 0, 2, 1)])
    for operation in (
        lambda: gm.coverage_union_all(overlap),
        lambda: gm.coverage_simplify(overlap, 0.1),
        lambda: gm.coverage_union_all(list(overlap)),
        lambda: gm.coverage_simplify(list(overlap), 0.1),
    ):
        with pytest.raises(
            gm.InvalidGeometryError,
            match=r'valid polygonal coverage.*coverage_invalid_edges.*coverage_clean',
        ) as exc_info:
            operation()
        assert type(exc_info.value) is gm.InvalidGeometryError
        assert exc_info.value.operation in {'coverage_union_all', 'coverage_simplify'}


def test_coverage_union_keeps_validity_gate_distinct_from_union_all() -> None:
    """The coverage fast path must not silently become a general dissolve."""
    overlap = [gm.box(0, 0, 1.2, 1), gm.box(1, 0, 2, 1)]
    assert gm.union_all(overlap).area == pytest.approx(2.0)
    with pytest.raises(gm.InvalidGeometryError, match='valid polygonal coverage'):
        gm.coverage_union_all(overlap)


def test_coverage_union_rejects_tjunction() -> None:
    """T-junction coverages fail the precondition through coverage_union_all too."""
    tjoin = gm.GeometryArray([
        gm.box(0, 0, 1, 2),
        gm.from_wkt('POLYGON ((1 0, 2 0, 2 1, 1 1, 1 0))'),
        gm.from_wkt('POLYGON ((1 1, 2 1, 2 2, 1 2, 1 1))'),
    ])
    assert not gm.coverage_is_valid(tjoin)
    for operation in (
        lambda: gm.coverage_union_all(tjoin),
        lambda: gm.coverage_union_all(list(tjoin)),
    ):
        with pytest.raises(
            gm.InvalidGeometryError,
            match=r'valid polygonal coverage.*coverage_invalid_edges.*coverage_clean',
        ) as exc_info:
            operation()
        assert exc_info.value.operation == 'coverage_union_all'


def test_coverage_union_rejects_gap_under_width() -> None:
    """gap_width is a coverage_is_valid concern; union always uses exact match.

    A narrow gap is still a *valid* exact coverage (no shared edge required
    across a gap), so coverage_union_all dissolves the disjoint parts. Document
    that union does not apply gap_width — rejection for gap is only via
    coverage_is_valid(gap_width=...).
    """
    gap = gm.GeometryArray([gm.box(0, 0, 0.999, 1), gm.box(1, 0, 2, 1)])
    assert gm.coverage_is_valid(gap)
    assert not gm.coverage_is_valid(gap, gap_width=0.01)
    # Exact-match union still accepts (two components); not a rejection class.
    union = gm.coverage_union_all(gap)
    assert union.geometry_type == 'MultiPolygon'
    assert union.area == pytest.approx(0.999 + 1.0)


@pytest.mark.parametrize(
    'simplify_boundary',
    [
        pytest.param(True, id='simplify-exterior'),
        pytest.param(False, id='preserve-exterior'),
    ],
)
def test_coverage_simplify_walks_open_exterior_chains(
    simplify_boundary: bool,
) -> None:
    """A removable exterior kink must not make an open shared chain look cyclic."""
    left = gm.from_wkt(
        'POLYGON ((0 0, 0.5 0.035, 1 0, 1 1, 0 1, 0 0))',
        crs=3857,
    )
    right = gm.from_wkt(
        'POLYGON ((1 0, 2 0, 2 1, 1 1, 1 0))',
        crs=3857,
    )
    rows = gm.GeometryArray([left, right], crs=3857)

    assert [geometry.is_valid for geometry in rows] == [True, True]
    assert gm.coverage_is_valid(rows)

    simplified = gm.coverage_simplify(
        rows,
        0.2,
        method='vw',
        simplify_boundary=simplify_boundary,
    )

    assert len(simplified) == 2
    assert simplified.crs == gm.CRS('EPSG:3857')
    assert gm.coverage_is_valid(simplified)
    expected = (
        [gm.box(0, 0, 1, 1, crs=3857), gm.box(1, 0, 2, 1, crs=3857)]
        if simplify_boundary
        else [left, right]
    )
    assert [
        gm.equals(got, want) for got, want in zip(simplified, expected, strict=True)
    ] == [
        True,
        True,
    ]


def test_geographic_coverage_simplifier_carries_inserted_zm_by_ellipsoidal_arclength() -> (
    None
):
    crs = '+proj=longlat +a=7000000 +rf=150 +type=crs'
    ring = [
        (0.0, 45.0, 35.25, 551.0),
        (90.0, 54.0, 40.0, 600.0),
        (180.0, 63.0, 45.0, 675.0),
        (275.0, 72.5, 49.5, 726.0),
        (275.0, 82.5, 60.0, 800.0),
        (180.0, 73.0, 55.0, 775.0),
        (90.0, 64.0, 50.0, 650.0),
        (0.0, 55.0, 45.0, 600.0),
        (0.0, 45.0, 35.25, 551.0),
    ]
    source = gm.Polygon(ring, crs=crs)

    simplified = gm.coverage_simplify(source, 0.1, method='vw')
    coordinates = list(simplified[0].exterior.coords)
    sample = next(
        coordinate for coordinate in coordinates if coordinate[:2] == (137.5, 58.75)
    )
    fraction = oblique_chart_arclength_fraction((0.0, 45.0), (275.0, 72.5), 0.5)
    assert fraction - 0.5 > 0.09
    assert math.isclose(sample[2], 35.25 + 14.25 * fraction, abs_tol=2.0e-8)
    assert math.isclose(sample[3], 551.0 + 175.0 * fraction, abs_tol=2.0e-7)
    assert not math.isclose(sample[2], 42.375, abs_tol=1.0e-6)
    assert not math.isclose(sample[3], 638.5, abs_tol=1.0e-6)
    assert coordinates[0] == (0.0, 45.0, 35.25, 551.0)
    assert coordinates[-1] == coordinates[0]

    xy_source = gm.Polygon([(x, y) for x, y, *_ in ring], crs=crs)
    xy_simplified = gm.coverage_simplify(xy_source, 0.1, method='vw')
    assert [(x, y) for x, y, *_ in coordinates] == list(
        xy_simplified[0].exterior.coords
    )


def test_geographic_coverage_simplifier_keeps_oblique_xyz_chord_ordinates() -> None:
    crs = '+proj=longlat +a=7000000 +rf=150 +type=crs'
    ring = [
        (0.0, 45.0, 35.25),
        (90.0, 54.0, 40.0),
        (180.0, 63.0, 45.0),
        (275.0, 72.5, 49.5),
        (275.0, 82.5, 60.0),
        (180.0, 73.0, 55.0),
        (90.0, 64.0, 50.0),
        (0.0, 55.0, 45.0),
        (0.0, 45.0, 35.25),
    ]
    simplified = gm.coverage_simplify(gm.Polygon(ring, crs=crs), 0.1, method='vw')
    coordinates = list(simplified[0].exterior.coords)
    sample = next(
        coordinate for coordinate in coordinates if coordinate[:2] == (137.5, 58.75)
    )
    fraction = oblique_chart_arclength_fraction((0.0, 45.0), (275.0, 72.5), 0.5)

    assert simplified[0].coordinate_axes == 'XYZ'
    assert sample[2] == pytest.approx(35.25 + 14.25 * fraction, rel=0.0, abs=2e-8)
    assert abs(sample[2] - 42.375) > 1.0


def test_coverage_simplify_preserves_topology() -> None:
    left = gm.from_wkt(
        'POLYGON ((0 0, 1 0, 1.05 0.25, 0.95 0.5, 1.05 0.75, 1 1, 0 1, 0 0))'
    )
    right = gm.from_wkt(
        'POLYGON ((1 0, 2 0, 2 1, 1 1, 1.05 0.75, 0.95 0.5, 1.05 0.25, 1 0))'
    )
    arr = gm.GeometryArray([left, right], crs=3857)
    assert gm.coverage_is_valid(arr)
    out = gm.coverage_simplify(arr, 0.5)
    assert gm.coverage_is_valid(out)
    assert [g.to_wkt() for g in out] == [
        'POLYGON ((1 0, 1 1, 0 1, 0 0, 1 0))',
        'POLYGON ((1 0, 2 0, 2 1, 1 1, 1 0))',
    ]
    assert out.crs == gm.CRS('EPSG:3857')
    assert sum(g.area for g in out) == pytest.approx(sum(g.area for g in arr), rel=0.1)
    pinned = gm.coverage_simplify(arr, 0.5, simplify_boundary=False)
    assert gm.coverage_is_valid(pinned)
    measured = gm.GeometryArray([g.with_coordinates(z=1.0) for g in grid_rows()])
    carried = gm.coverage_simplify(measured, 0.1)
    assert all(g.has_z for g in carried)
    assert all(z == 1.0 for geom in carried for z in geom.coords.z)


def test_coverage_simplify_remains_valid_across_aggressive_tolerances() -> None:
    """One exact chain decision stays shared across multipart rows."""
    left = gm.from_wkt(
        'POLYGON ((0 0, 1 0, 1.05 0.25, 0.95 0.5, 1.05 0.75, 1 1, 0 1, 0 0))'
    )
    right = gm.from_wkt(
        'POLYGON ((1 0, 2 0, 2 1, 1 1, 1.05 0.75, 0.95 0.5, 1.05 0.25, 1 0))'
    )
    rows = [
        gm.MultiPolygon([left, gm.box(10, 0, 11, 1)]),
        right,
        gm.box(11, 0, 12, 1),
    ]
    assert gm.coverage_is_valid(rows)
    for tolerance in (0.0, 0.01, 0.1, 0.5, 1.0, 1.1, 2.0, 10.0, 100.0):
        simplified = gm.coverage_simplify(rows, tolerance)
        assert gm.coverage_is_valid(simplified), (
            tolerance,
            [geometry.to_wkt() for geometry in simplified],
        )


def test_coverage_dp_keeps_vertex_when_shortcut_projection_clamps() -> None:
    """DP measures a removable vertex to the finite shortcut segment."""
    polygon = gm.from_wkt('POLYGON ((0 0, 6 3, 5 0, 25 -5, 0 -5, 0 0))')
    vertex = gm.Point(6, 3)
    tolerance = 3.1

    # The projection of (6, 3) onto the supporting line of (0, 0)-(5, 0)
    # is (6, 0), past the shortcut's endpoint. Its finite-segment distance
    # is therefore the witnessed endpoint distance sqrt(10), above tolerance.
    assert math.sqrt(10.0) > tolerance
    simplified = gm.coverage_simplify([polygon], tolerance, method='dp')[0]

    assert gm.covers(simplified.boundary(), vertex)


def test_coverage_simplify_matches_shapely_oracle() -> None:
    import shapely

    left = gm.from_wkt(
        'POLYGON ((0 0, 1 0, 1.05 0.25, 0.95 0.5, 1.05 0.75, 1 1, 0 1, 0 0))'
    )
    right = gm.from_wkt(
        'POLYGON ((1 0, 2 0, 2 1, 1 1, 1.05 0.75, 0.95 0.5, 1.05 0.25, 1 0))'
    )
    ours = gm.coverage_simplify([left, right], 0.5)
    theirs = shapely.coverage_simplify(
        [shapely.from_wkt(g.to_wkt()) for g in (left, right)], 0.5
    )
    for got, want in zip(ours, theirs, strict=True):
        assert shapely.equals(shapely.from_wkt(got.to_wkt()), want), (
            got.to_wkt(),
            want.wkt,
        )


def test_coverage_clean_overlap_rules_and_gaps() -> None:
    overlap = gm.GeometryArray([gm.box(0, 0, 1.2, 1), gm.box(1, 0, 2, 1)])
    assert not gm.coverage_is_valid(overlap)
    expected: dict[str, list[float]] = {
        'longest_border': [1.2, 0.8],
        'max_area': [1.2, 0.8],
        'min_area': [1.0, 1.0],
        'min_index': [1.2, 0.8],
    }
    for rule, areas in expected.items():
        out = gm.coverage_clean(overlap, grid_size=0, overlap_rule=rule)
        assert [g.area for g in out] == pytest.approx(areas)
        assert gm.coverage_is_valid(out)
    reversed_overlap = gm.GeometryArray(list(reversed(overlap)))
    reversed_cleaned = gm.coverage_clean(
        reversed_overlap,
        grid_size=0,
        overlap_rule='longest_border',
    )
    assert [g.area for g in reversed_cleaned] == pytest.approx([1.0, 1.0])
    outer = gm.from_wkt(
        'POLYGON ((0 0, 3 0, 3 3, 0 3, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1))'
    )
    inner = gm.box(1.005, 1.005, 1.995, 1.995)
    ring = gm.GeometryArray([outer, inner])
    merged = gm.coverage_clean(ring, grid_size=0, gap_width=0.05)
    assert sum(g.area for g in merged) == pytest.approx(9.0)
    assert gm.coverage_is_valid(merged)
    kept = gm.coverage_clean(ring, grid_size=0)
    assert sum(g.area for g in kept) == pytest.approx(8.9801)
    near = gm.GeometryArray([gm.box(0, 0, 1.0000001, 1), gm.box(1, 0, 2, 1)])
    assert not gm.coverage_is_valid(near)
    unchanged_default = gm.coverage_clean(near)
    assert gm.coverage_is_valid(unchanged_default)
    coordinates = np.asarray(unchanged_default.coords, dtype=np.float64)
    assert sorted(set(coordinates[:, 0])) == [
        0.0,
        1.0,
        1.0000001,
        2.0,
    ]
    healed = gm.coverage_clean(near, grid_size=1e-6)
    assert gm.coverage_is_valid(healed)
    grid = gm.GeometryArray(grid_rows())
    identity = gm.coverage_clean(grid, grid_size=0)
    assert gm.coverage_is_valid(identity)
    assert [g.area for g in identity] == pytest.approx([1.0] * 4)
    assert identity.to_wkb() == grid.to_wkb()
    twice = gm.coverage_clean(identity)
    assert twice.to_wkb() == identity.to_wkb()


def test_coverage_clean_longest_border_orders_individual_length_overflow() -> None:
    rows = gm.GeometryArray([
        gm.box(-1.6e308, -1.7e308, 1.6e308, 1.7e308),
        gm.box(-1.7e308, 0.0, 1.7e308, 1.0e308),
    ])
    overlap = gm.Point(0.0, 0.5e308)
    output = gm.coverage_clean(rows, grid_size=0, overlap_rule='longest_border')
    assert not gm.contains(output[0], overlap)
    assert gm.contains(output[1], overlap)

    reversed_output = gm.coverage_clean(
        gm.GeometryArray(list(reversed(rows))),
        grid_size=0,
        overlap_rule='longest_border',
    )
    assert gm.contains(reversed_output[0], overlap)
    assert not gm.contains(reversed_output[1], overlap)

    scaled_rows = gm.GeometryArray([
        gm.box(-1.6, -1.7, 1.6, 1.7),
        gm.box(-1.7, 0.0, 1.7, 1.0),
    ])
    scaled_output = gm.coverage_clean(
        scaled_rows,
        grid_size=0,
        overlap_rule='longest_border',
    )
    assert not gm.contains(scaled_output[0], gm.Point(0.0, 0.5))
    assert gm.contains(scaled_output[1], gm.Point(0.0, 0.5))


def test_coverage_clean_longest_border_orders_overflowed_sums_with_finite_edges() -> (
    None
):
    rows = gm.GeometryArray([
        gm.box(0.0, -1.7e308, 1.5e308, 1.7e308),
        gm.box(-1.7e308, 0.0, 1.7e308, 1.0e308),
    ])
    output = gm.coverage_clean(rows, grid_size=0, overlap_rule='longest_border')
    assert not gm.contains(output[0], gm.Point(0.75e308, 0.5e308))
    assert gm.contains(output[1], gm.Point(0.75e308, 0.5e308))

    scaled_rows = gm.GeometryArray([
        gm.box(0.0, -0.017, 0.015, 0.017),
        gm.box(-0.017, 0.0, 0.017, 0.01),
    ])
    scaled_output = gm.coverage_clean(
        scaled_rows,
        grid_size=0,
        overlap_rule='longest_border',
    )
    assert not gm.contains(scaled_output[0], gm.Point(0.0075, 0.005))
    assert gm.contains(scaled_output[1], gm.Point(0.0075, 0.005))


def test_coverage_clean_longest_border_keeps_overflowed_tie_order() -> None:
    rows = gm.GeometryArray([
        gm.box(-1.7e308, 0.0, 1.7e308, 1.5e308),
        gm.box(0.0, -1.7e308, 1.5e308, 1.7e308),
    ])
    overlap = gm.Point(0.75e308, 0.75e308)
    output = gm.coverage_clean(rows, grid_size=0, overlap_rule='longest_border')
    assert gm.contains(output[0], overlap)
    assert not gm.contains(output[1], overlap)

    reversed_output = gm.coverage_clean(
        gm.GeometryArray(list(reversed(rows))),
        grid_size=0,
        overlap_rule='longest_border',
    )
    assert gm.contains(reversed_output[0], overlap)
    assert not gm.contains(reversed_output[1], overlap)


def test_coverage_clean_publishes_shared_bits_and_source_ordinates() -> None:
    import numpy as np

    rows = gm.GeometryArray([
        gm.box(0, 0, 1.2, 1).with_coordinates(z=7.0, m=11.0),
        gm.box(1, 0, 2, 1).with_coordinates(z=7.0, m=11.0),
    ])
    out = gm.coverage_clean(rows)
    assert all(geometry.coordinate_axes == 'XYZM' for geometry in out)
    assert all(z == 7.0 for geometry in out for z in geometry.coords.z)
    assert all(m == 11.0 for geometry in out for m in geometry.coords.m)

    expected = np.asarray([[1.2, 0.0], [1.2, 1.0]], dtype=np.float64)
    expected_bits = {tuple(row) for row in expected.view(np.uint64)}
    shared_bits = []
    for geometry in out:
        coordinates = np.asarray(geometry.force_2d().coords, dtype=np.float64)
        selected = coordinates[
            (coordinates[:, 0] == 1.2)
            & ((coordinates[:, 1] == 0.0) | (coordinates[:, 1] == 1.0))
        ]
        shared_bits.append({tuple(row) for row in selected.view(np.uint64)})
    assert shared_bits == [expected_bits, expected_bits]

    mixed = gm.coverage_clean(
        gm.GeometryArray([
            gm.box(0, 0, 1.2, 1).with_coordinates(z=5.0),
            gm.box(1, 0, 2, 1),
        ])
    )
    assert [geometry.coordinate_axes for geometry in mixed] == ['XYZ', 'XY']


def test_coverage_clean_deterministic_jitter_corpus() -> None:
    """Pinned near-coverages stay valid and conserve their union area."""
    jitters = (
        (
            (0.012, -0.009),
            (-0.018, 0.021),
            (0.0, 0.015),
            (0.027, -0.024),
            (-0.006, 0.003),
            (0.019, -0.011),
        ),
        (
            (-0.03, 0.03),
            (0.03, -0.03),
            (-0.02, 0.01),
            (0.01, -0.02),
            (0.025, 0.005),
            (-0.005, -0.025),
        ),
        (
            (0.0, 0.0),
            (1e-9, -1e-9),
            (-1e-7, 1e-7),
            (0.02, 0.02),
            (-0.02, -0.02),
            (0.01, -0.01),
        ),
    )
    for case in jitters:
        rows = []
        for index, (x_jitter, y_jitter) in enumerate(case):
            i, j = divmod(index, 2)
            rows.append(gm.box(i, j, i + 1 + x_jitter, j + 1 + y_jitter))
        arr = gm.GeometryArray(rows)
        out = gm.coverage_clean(arr, grid_size=0)
        assert gm.coverage_is_valid(out)
        union_in = gm.union_all(list(arr)).area
        union_out = gm.union_all([g for g in out if not g.is_empty]).area
        assert union_out == pytest.approx(union_in, rel=1e-06)
