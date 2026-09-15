"""Correctness/contract guards for the nearest/distance family.

`shortest_line`, `nearest_points`, and `distance` accept any geometry pair in
planar and projected modes and compute the true closest approach — the witness
may land on an edge interior, not only a vertex. Geographic geodesic modes
support point and linework operands; filled polygon interiors are unsupported.
These tests lock that down across rotated/oblique inputs and cross-type pairs,
and pin the two deliberate contracts:

* empty operand -> output-type EMPTY sentinel (never raises, never aborts a
  batch): ``distance``->``inf``, ``dwithin``->``False``, ``nearest_points``->
  ``(POINT EMPTY, POINT EMPTY)``, ``shortest_line``->``LINESTRING EMPTY``;
* mixed dimensionality -> common axes (an XYZM line vs an XY point yields XY
  witnesses), so ``nearest_points`` matches ``shortest_line``'s endpoints
  exactly.
"""

from __future__ import annotations

import math
from fractions import Fraction

import gometry as gm
import pytest


def _endpoints(line: gm.Geometry) -> list[tuple[float, float]]:
    coords = line.coords
    xs = [float(v) for v in coords.x]
    ys = [float(v) for v in coords.y]
    return list(zip(xs, ys, strict=True))


def _xy(point: gm.Geometry) -> tuple[float, float]:
    return (float(point.coords.x[0]), float(point.coords.y[0]))


_ANTIMERIDIAN_LONGITUDES = frozenset({180.0, -180.0})


def _assert_lonlat(
    got: tuple[float, float], expected: tuple[float, float]
) -> None:
    gx, gy = got
    ex, ey = expected
    assert gy == pytest.approx(ey, rel=0.0, abs=0.0)
    if ex in _ANTIMERIDIAN_LONGITUDES:
        assert gx in _ANTIMERIDIAN_LONGITUDES
    else:
        assert gx == pytest.approx(ex, rel=0.0, abs=0.0)


def _assert_shortest_line_row(
    line: gm.Geometry,
    want: tuple[tuple[float, float], tuple[float, float]],
    left: gm.Geometry,
    right: gm.Geometry,
) -> None:
    ends = _endpoints(line)
    _assert_lonlat(ends[0], want[0])
    _assert_lonlat(ends[1], want[1])
    scalar_ends = _endpoints(gm.shortest_line(left, right))
    first, second = gm.nearest_points(left, right)
    _assert_lonlat(ends[0], scalar_ends[0])
    _assert_lonlat(ends[1], scalar_ends[1])
    _assert_lonlat(ends[0], _xy(first))
    _assert_lonlat(ends[1], _xy(second))
    assert line.crs == left.crs
    assert line.epoch == left.epoch
    assert line.coordinate_axes == 'XY'
    assert not line.has_z
    assert not line.has_m


_CROSS_TYPES = [
    ('point', gm.Point(-1, -1)),
    ('line', gm.from_wkt('LINESTRING (-3 -1, -3 1)')),
    ('polygon', gm.from_wkt('POLYGON ((-5 -1, -4 -1, -4 1, -5 1, -5 -1))')),
    ('multipoint', gm.from_wkt('MULTIPOINT ((-1 4), (-1 6))')),
    ('multiline', gm.from_wkt('MULTILINESTRING ((-3 4, -3 6), (-2 4, -2 6))')),
    ('multipolygon', gm.from_wkt('MULTIPOLYGON (((-6 4, -5 4, -5 6, -6 6, -6 4)))')),
    (
        'collection',
        gm.from_wkt('GEOMETRYCOLLECTION (POINT (-8 0), LINESTRING (-8 2, -8 4))'),
    ),
]


class TestTriInvariant:
    """shortest_line endpoints == nearest_points pair == distance, for every
    cross-type pair (the core consistency contract).
    """

    @pytest.mark.parametrize('right_name,right', _CROSS_TYPES)
    @pytest.mark.parametrize('left_name,left', _CROSS_TYPES)
    def test_cross_type_matrix(self, left_name, left, right_name, right) -> None:
        line = gm.shortest_line(left, right)
        a, b = gm.nearest_points(left, right)
        dist = gm.distance(left, right)
        ends = _endpoints(line)
        assert len(ends) == 2
        assert ends[0] == pytest.approx(_xy(a))
        assert ends[1] == pytest.approx(_xy(b))
        manhattan = math.dist(ends[0], ends[1])
        assert dist == pytest.approx(manhattan)
        assert gm.distance(right, left) == pytest.approx(dist)


class TestVertexToEdge:
    """The witness can fall on an edge interior, not just a vertex — the case
    that motivated this review (a 45 deg square next to another square).
    """

    def test_diamond_to_square_is_vertex_to_edge(self) -> None:
        square = gm.from_wkt('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))')
        diamond = gm.from_wkt('POLYGON ((4 1, 5 0, 6 1, 5 2, 4 1))')
        assert (gm.shortest_line(square, diamond)).to_wkt() == 'LINESTRING (2 1, 4 1)'
        a, b = gm.nearest_points(square, diamond)
        assert _xy(a) == (2.0, 1.0)
        assert (2.0, 1.0) not in _endpoints(square)
        assert _xy(b) == (4.0, 1.0)
        assert gm.distance(square, diamond) == pytest.approx(2.0)

    def test_rotated_square_witness(self) -> None:
        square = gm.from_wkt('POLYGON ((-1 -1, 1 -1, 1 1, -1 1, -1 -1))')
        diamond = (square).rotate(45, origin=(0, 0))
        line = gm.shortest_line(diamond, gm.Point(5, 0))
        ends = _endpoints(line)
        assert ends[0] == pytest.approx((math.sqrt(2), 0.0), abs=1e-09)
        assert ends[1] == pytest.approx((5.0, 0.0))

    def test_edge_to_edge_parallel(self) -> None:
        s1 = gm.from_wkt('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))')
        s2 = gm.from_wkt('POLYGON ((4 0.5, 6 0.5, 6 1.5, 4 1.5, 4 0.5))')
        line = gm.shortest_line(s1, s2)
        ends = _endpoints(line)
        assert ends[0][1] == pytest.approx(ends[1][1])
        assert math.dist(ends[0], ends[1]) == pytest.approx(2.0)
        assert gm.distance(s1, s2) == pytest.approx(2.0)

    def test_vertex_to_vertex_control(self) -> None:
        s1 = gm.from_wkt('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))')
        s2 = gm.from_wkt('POLYGON ((3 3, 5 3, 5 5, 3 5, 3 3))')
        assert (gm.shortest_line(s1, s2)).to_wkt() == 'LINESTRING (2 2, 3 3)'


class TestObliqueFoot:
    """Foot-of-perpendicular onto oblique segments (interior, not an endpoint)."""

    def test_point_to_oblique_line(self) -> None:
        line = gm.from_wkt('LINESTRING (0 0, 10 10)')
        point = gm.Point(5, 6)
        a, b = gm.nearest_points(line, point)
        assert _xy(a) == pytest.approx((5.5, 5.5))
        assert _xy(b) == pytest.approx((5.0, 6.0))
        assert gm.distance(line, point) == pytest.approx(1.0 / math.sqrt(2))

    def test_two_parallel_oblique_segments(self) -> None:
        s1 = gm.from_wkt('LINESTRING (0 0, 4 4)')
        s2 = gm.from_wkt('LINESTRING (5 1, 9 5)')
        assert gm.distance(s1, s2) == pytest.approx(4.0 / math.sqrt(2))


class TestDegenerate:
    """Touching/intersecting/contained operands -> distance 0, a degenerate
    (zero-length) connecting line, and identical witnesses.
    """

    def _assert_zero_contact(self, left, right) -> None:
        assert gm.distance(left, right) == 0.0
        a, b = gm.nearest_points(left, right)
        assert (a).to_wkt() == (b).to_wkt()
        ends = _endpoints(gm.shortest_line(left, right))
        assert ends[0] == ends[1]
        assert ends[0] == pytest.approx(_xy(a))

    def test_crossing_lines(self) -> None:
        l1 = gm.from_wkt('LINESTRING (0 0, 2 2)')
        l2 = gm.from_wkt('LINESTRING (0 2, 2 0)')
        self._assert_zero_contact(l1, l2)
        assert (gm.shortest_line(l1, l2)).to_wkt() == 'LINESTRING (1 1, 1 1)'

    def test_point_on_line(self) -> None:
        line = gm.from_wkt('LINESTRING (0 0, 4 0)')
        self._assert_zero_contact(line, gm.Point(2, 0))

    def test_point_in_polygon(self) -> None:
        poly = gm.from_wkt('POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))')
        self._assert_zero_contact(poly, gm.Point(2, 2))

    def test_overlapping_polygons(self) -> None:
        a = gm.from_wkt('POLYGON ((0 0, 3 0, 3 3, 0 3, 0 0))')
        b = gm.from_wkt('POLYGON ((1 1, 4 1, 4 4, 1 4, 1 1))')
        self._assert_zero_contact(a, b)

    def test_identical(self) -> None:
        poly = gm.from_wkt('POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))')
        self._assert_zero_contact(poly, poly)


class TestCommonDims:
    """Mixed dimensionality drops to the operands' common axes; shared axes are
    preserved and interpolated.
    """

    def test_xyzm_line_vs_xy_point_drops_to_xy(self) -> None:
        line = gm.LineString([(0, 0), (4, 0)], z=[0, 8], m=[10, 18])
        point = gm.Point(1, 3)
        assert (gm.shortest_line(line, point)).to_wkt() == 'LINESTRING (1 0, 1 3)'
        a, b = gm.nearest_points(line, point)
        assert (a).to_wkt() == 'POINT (1 0)'
        assert (b).to_wkt() == 'POINT (1 3)'

    def test_z_line_vs_xy_point_drops_z(self) -> None:
        line = gm.from_wkt('LINESTRING Z (0 0 0, 4 0 8)')
        a, b = gm.nearest_points(line, gm.Point(1, 3))
        assert (a).to_wkt() == 'POINT (1 0)'
        assert (b).to_wkt() == 'POINT (1 3)'

    def test_both_z_preserves_and_interpolates_z(self) -> None:
        line = gm.from_wkt('LINESTRING Z (0 0 0, 4 0 8)')
        point = gm.from_wkt('POINT Z (1 3 99)')
        a, b = gm.nearest_points(line, point)
        assert (a).to_wkt() == 'POINT Z (1 0 2)'
        assert (b).to_wkt() == 'POINT Z (1 3 99)'
        assert (
            gm.shortest_line(line, point)
        ).to_wkt() == 'LINESTRING Z (1 0 2, 1 3 99)'


class TestEmptyContract:
    """Empty operand -> output-type EMPTY sentinel, total across the family."""

    @pytest.mark.parametrize(
        'empty',
        [
            gm.from_wkt('POINT EMPTY'),
            gm.from_wkt('LINESTRING EMPTY'),
            gm.from_wkt('POLYGON EMPTY'),
            gm.from_wkt('GEOMETRYCOLLECTION EMPTY'),
        ],
    )
    def test_scalar_sentinels(self, empty) -> None:
        point = gm.Point(1, 1)
        for left, right in ((empty, point), (point, empty)):
            a, b = gm.nearest_points(left, right)
            assert (a).to_wkt() == 'POINT EMPTY'
            assert (b).to_wkt() == 'POINT EMPTY'
            assert (gm.shortest_line(left, right)).to_wkt() == 'LINESTRING EMPTY'
            assert gm.distance(left, right) == math.inf
            assert gm.dwithin(left, right, 5.0) is False

    def test_array_empty_row_does_not_abort_batch(self) -> None:
        arr = gm.GeometryArray([
            gm.Point(0, 0),
            gm.from_wkt('POINT EMPTY'),
            gm.Point(3, 0),
        ])
        probe = gm.Point(0, 5)
        pair_left, pair_right = gm.nearest_points(arr, probe)
        assert [(g).to_wkt() for g in pair_left] == [
            'POINT (0 0)',
            'POINT EMPTY',
            'POINT (3 0)',
        ]
        assert [(g).to_wkt() for g in pair_right] == [
            'POINT (0 5)',
            'POINT EMPTY',
            'POINT (0 5)',
        ]
        assert [(g).to_wkt() for g in gm.shortest_line(arr, probe)] == [
            'LINESTRING (0 0, 0 5)',
            'LINESTRING EMPTY',
            'LINESTRING (3 0, 0 5)',
        ]
        dists = gm.distance(arr, probe)
        assert dists[0] == pytest.approx(5.0)
        assert dists[1] == math.inf
        assert dists[2] == pytest.approx(math.hypot(3.0, 5.0))

    def test_empty_sentinels_carry_crs(self) -> None:
        empty = gm.from_wkt('POLYGON EMPTY', crs=3857)
        point = gm.Point(1, 1, crs=3857)
        a, b = gm.nearest_points(empty, point)
        assert a.crs == 'EPSG:3857'
        assert b.crs == 'EPSG:3857'
        assert gm.shortest_line(empty, point).crs == 'EPSG:3857'


class TestSurfaceConsistency:
    """Scalar method == free function == array form."""

    def test_shortest_line_and_nearest_points_agree(self) -> None:
        left = gm.from_wkt('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))')
        right = gm.Point(5, 1)
        method = (gm.shortest_line(left, right)).to_wkt()
        free = (gm.shortest_line(left, right)).to_wkt()
        array = gm.shortest_line(gm.GeometryArray([left]), right)
        assert method == free
        assert [(g).to_wkt() for g in array] == [free]
        m_a, m_b = gm.nearest_points(left, right)
        f_a, f_b = gm.nearest_points(left, right)
        assert ((m_a).to_wkt(), (m_b).to_wkt()) == ((f_a).to_wkt(), (f_b).to_wkt())
        arr_left, arr_right = gm.nearest_points(gm.GeometryArray([left]), right)
        assert [(g).to_wkt() for g in arr_left] == [(m_a).to_wkt()]
        assert [(g).to_wkt() for g in arr_right] == [(m_b).to_wkt()]

    @pytest.mark.parametrize('fixed_name,fixed', _CROSS_TYPES)
    def test_fixed_array_executor_matches_scalar_oracle_for_every_geometry_kind(
        self,
        fixed_name: str,
        fixed: gm.Geometry,
    ) -> None:
        del fixed_name
        values = [value for _, value in _CROSS_TYPES]
        array = gm.GeometryArray([*values, None])

        for fixed_is_left in (True, False):
            operands = (fixed, array) if fixed_is_left else (array, fixed)
            actual_left, actual_right = gm.nearest_points(*operands)
            expected = [
                gm.nearest_points(fixed, value)
                if fixed_is_left
                else gm.nearest_points(value, fixed)
                for value in values
            ]
            assert actual_left.to_wkt() == [
                *[left.to_wkt() for left, _ in expected],
                None,
            ]
            assert actual_right.to_wkt() == [
                *[right.to_wkt() for _, right in expected],
                None,
            ]

    def test_fixed_array_executor_preserves_zm_and_antimeridian_orientation(
        self,
    ) -> None:
        fixed = gm.LineString(
            [(0, 0), (4, 0)],
            z=[0, 8],
            m=[10, 18],
        )
        values = [gm.Point(1, 3, z=99, m=30), None, gm.Point(3, -2, z=7, m=40)]
        points = gm.GeometryArray(values)
        left, right = gm.nearest_points(fixed, points, unit='planar')
        assert left.to_wkt() == ['POINT ZM (1 0 2 12)', None, 'POINT ZM (3 0 6 16)']
        assert right.to_wkt() == ['POINT ZM (1 3 99 30)', None, 'POINT ZM (3 -2 7 40)']

        crossing = gm.LineString([(179, 0), (-179, 0)], crs=4326)
        probes = gm.GeometryArray(
            [gm.Point(180, 1, crs=4326), None, gm.Point(170, 1, crs=4326)],
        )
        for operands in ((crossing, probes), (probes, crossing)):
            actual = gm.nearest_points(*operands)
            expected = [
                gm.nearest_points(operands[0], probes[index])
                if operands[0] is crossing
                else gm.nearest_points(probes[index], crossing)
                for index in (0, 2)
            ]
            assert actual[0].to_wkt() == [
                expected[0][0].to_wkt(),
                None,
                expected[1][0].to_wkt(),
            ]
            assert actual[1].to_wkt() == [
                expected[0][1].to_wkt(),
                None,
                expected[1][1].to_wkt(),
            ]


class TestGeodesic:
    """Geographic CRS produces geodesic witnesses; planar override escapes."""

    @pytest.mark.parametrize(
        'longitude',
        [0.0, 5.0, 2.5, 1.0, 3.7],
        ids=[
            'start-endpoint',
            'end-endpoint',
            'mid-interior',
            'early-interior',
            'late-interior',
        ],
    )
    def test_exact_geodesic_segment_contacts_survive_endpoint_guard(
        self, longitude: float
    ) -> None:
        line = gm.LineString([(0.0, 0.0), (5.0, 0.0)], crs=4326)
        query = gm.Point(longitude, 0.0, crs=4326)

        assert gm.distance(query, line) == 0.0
        assert gm.dwithin(query, line, 0.0)
        assert gm.nearest_points(query, line)[1].to_wkt() == query.to_wkt()

    def test_point_to_line_geodesic_clamps_to_endpoint_without_tie_drift(self) -> None:
        query = gm.Point(6.0, 3.0, crs=4326)
        line = gm.LineString([(0.0, 0.0), (5.0, 0.0)], crs=4326)
        endpoint = gm.Point(5.0, 0.0, crs=4326)

        # The equator is a geodesic, and the perpendicular meridian through
        # (6, 3) meets its supporting line at longitude 6, beyond the finite
        # [0, 5] segment. The endpoint is consequently the exact nearest
        # feature, not a point just inside the endpoint.
        first, second = gm.nearest_points(query, line)
        assert first.to_wkt() == query.to_wkt()
        assert second.to_wkt() == endpoint.to_wkt()
        assert gm.distance(query, line) == gm.distance(query, endpoint)
        assert gm.shortest_line(query, line).to_wkt() == 'LINESTRING (6 3, 5 0)'
        assert line.line_locate(query) == line.length

    def test_point_near_geodesic_endpoint_is_not_fabricated_contact(self) -> None:
        line = gm.LineString([(0.0, 0.0), (5.0, 0.0)], crs=4326)
        endpoint = gm.Point(5.0, 0.0, crs=4326)
        query = gm.Point(5.0, 1.0e-12, crs=4326)

        # The query is just north of the equator, not on it. Its distance to
        # the endpoint is positive, so a contact tolerance must not return the
        # query itself as a point on the finite segment.
        endpoint_distance = gm.distance(query, endpoint)
        assert endpoint_distance > 0.0
        assert gm.distance(query, line) == endpoint_distance
        assert not gm.dwithin(query, line, 0.0)
        assert gm.nearest_points(query, line)[1].to_wkt() == endpoint.to_wkt()

    @pytest.mark.parametrize(
        'delta',
        [1.0e-12, 1.0e-11, 1.0e-9],
        ids=['one-trillionth-degree', 'ten-trillionths-degree', 'one-billionth-degree'],
    )
    def test_positive_geodesic_dwithin_refines_near_endpoint(self, delta: float) -> None:
        line = gm.LineString([(0.0, 0.0), (5.0, 0.0)], crs=4326)
        endpoint = gm.Point(5.0, 0.0, crs=4326)
        query = gm.Point(5.0 + delta, 0.0, crs=4326)
        endpoint_distance = gm.distance(query, endpoint)

        assert endpoint_distance > 0.0
        assert not gm.dwithin(query, line, 0.0)
        assert not gm.dwithin(query, line, 1.0e-9)
        assert not gm.dwithin(query, line, min(1.0e-7, endpoint_distance / 2.0))
        assert not gm.dwithin(query, line, math.nextafter(endpoint_distance, 0.0))
        assert gm.dwithin(query, line, endpoint_distance)

    def test_geodesic_dwithin_rejects_a_point_1000_001_metres_beyond_radius(self) -> None:
        line = gm.LineString([(0.0, 0.0), (5.0, 0.0)], crs=4326)
        endpoint = gm.Point(5.0, 0.0, crs=4326)
        query = endpoint.destination(90.0, 1000.001)
        endpoint_distance = gm.distance(query, endpoint)

        assert endpoint_distance > 1000.0
        assert gm.distance(query, line) == endpoint_distance
        assert not gm.dwithin(query, line, 1000.0)
        assert gm.dwithin(query, line, endpoint_distance)

    @pytest.mark.parametrize(
        'segment_length',
        [0.25, 1.0, 5.0, 10.0, 30.0],
        ids=['quarter-degree', 'one-degree', 'five-degrees', 'ten-degrees', 'thirty-degrees'],
    )
    def test_geodesic_point_segment_sweep_is_attained(
        self, segment_length: float
    ) -> None:
        line = gm.LineString(
            [(0.0, 0.0), (segment_length, 0.0)], crs=4326
        )
        exhibited = gm.Point(segment_length / 2.0, 0.0, crs=4326)

        for latitude in (1.0e-9, 1.0e-7, 1.0e-5, 1.0e-4, 1.0e-2, 1.0, 20.0):
            query = gm.Point(segment_length / 2.0, latitude, crs=4326)
            distance = gm.distance(query, line)
            exhibited_distance = gm.distance(query, exhibited)
            _, witness = gm.nearest_points(query, line)
            witness_distance = gm.distance(query, witness)
            rounding_floor = 32.0 * math.ulp(
                max(distance, exhibited_distance, witness_distance, 1.0)
            )

            assert distance <= exhibited_distance + rounding_floor
            assert abs(distance - witness_distance) <= rounding_floor

    @pytest.mark.parametrize(
        ('first_target', 'second_target', 'argmax'),
        [(1.0, 8.0, 4.5), (0.3, 7.1, 3.7), (1.7, 9.3, 5.5)],
        ids=['midpoint-4_5', 'midpoint-3_7', 'midpoint-5_5'],
    )
    def test_geodesic_hausdorff_extremum_is_bounded_and_attained(
        self, first_target: float, second_target: float, argmax: float
    ) -> None:
        source = gm.LineString([(0.0, 0.0), (10.0, 0.0)], crs=4326)
        target = gm.MultiPoint(
            [(first_target, 0.0), (second_target, 0.0)], crs=4326
        )

        reported = gm.hausdorff_distance(source, target)
        exhibited = gm.distance(gm.Point(argmax, 0.0, crs=4326), target)
        rounding_floor = 32.0 * math.ulp(max(reported, exhibited, 1.0))

        assert reported <= exhibited + rounding_floor
        assert exhibited - reported <= 1.0e-2 + rounding_floor

    def test_point_to_line_geodesic(self) -> None:
        point = gm.Point(0.5, 0.5, crs=4326)
        line = gm.LineString([(0, 0), (10, 0)], crs=4326)
        line_geom = gm.shortest_line(point, line)
        ends = _endpoints(line_geom)
        assert ends[0] == pytest.approx((0.5, 0.5))
        # The equator is a geodesic, and the query's perpendicular meridian
        # meets it at the same longitude as the query.
        assert ends[1][0] == pytest.approx(0.5, abs=1e-12)
        assert ends[1][1] == pytest.approx(0.0, abs=1e-06)
        assert gm.distance(point, line) == pytest.approx(55287.15, rel=0.0001)

    def test_planar_override(self) -> None:
        point = gm.Point(0.5, 0.5, crs=4326)
        line = gm.LineString([(0, 0), (10, 0)], crs=4326)
        assert (
            gm.shortest_line(point, line, unit='planar')
        ).to_wkt() == 'LINESTRING (0.5 0.5, 0.5 0)'

    @pytest.mark.parametrize(
        ('layout', 'fixed_is_left', 'expected'),
        [
            pytest.param(
                'dense',
                True,
                [((180.0, 1.0), (180.0, 0.0)), ((180.0, 1.0), (170.0, 1.0))],
                id='dense-scalar-left',
            ),
            pytest.param(
                'dense',
                False,
                [((180.0, 0.0), (180.0, 1.0)), ((170.0, 1.0), (180.0, 1.0))],
                id='dense-array-left',
            ),
            pytest.param(
                'nullable',
                False,
                [((180.0, 0.0), (180.0, 1.0)), ((170.0, 1.0), (180.0, 1.0))],
                id='nullable-array-left',
            ),
        ],
    )
    def test_seam_shortest_line_endpoints_are_antimeridian_foot_and_inputs(
        self,
        layout: str,
        fixed_is_left: bool,
        expected: list[tuple[tuple[float, float], tuple[float, float]]],
    ) -> None:
        # Equatorial 179→-179 is the 2° seam segment. (180, 1) sits on that
        # meridian, so the foot is (180, 0), the same meridian as (-180, 0).
        # The second row is point-point, so the vertices are the inputs.
        crossing = gm.LineString(
            [(179.0, 0.0), (-179.0, 0.0)], crs=4326, epoch=2020.0
        )
        plain = gm.Point(170.0, 1.0, crs=4326, epoch=2020.0)
        probe = gm.Point(180.0, 1.0, crs=4326, epoch=2020.0)
        if layout == 'dense':
            array = gm.GeometryArray([crossing, plain])
            missing = [False, False]
            pairs = [(crossing, expected[0]), (plain, expected[1])]
        else:
            array = gm.GeometryArray([crossing, None, plain])
            missing = [False, True, False]
            pairs = [(crossing, expected[0]), (None, None), (plain, expected[1])]
        result = (
            gm.shortest_line(probe, array)
            if fixed_is_left
            else gm.shortest_line(array, probe)
        )
        left_frame = probe if fixed_is_left else array
        assert result.is_missing.tolist() == missing
        assert result.crs == left_frame.crs
        assert result.epoch == left_frame.epoch
        for index, (value, want) in enumerate(pairs):
            line = result[index]
            if value is None:
                assert line is None
                continue
            left, right = (probe, value) if fixed_is_left else (value, probe)
            _assert_shortest_line_row(line, want, left, right)

    @pytest.mark.parametrize(
        ('layout', 'fixed_is_left', 'expected'),
        [
            pytest.param(
                'dense',
                True,
                [((0.0, 89.0), (0.0, 90.0)), ((0.0, 89.0), (180.0, 89.0))],
                id='dense-scalar-left',
            ),
            pytest.param(
                'dense',
                False,
                [((0.0, 90.0), (0.0, 89.0)), ((180.0, 89.0), (0.0, 89.0))],
                id='dense-array-left',
            ),
            pytest.param(
                'nullable',
                False,
                [((0.0, 90.0), (0.0, 89.0)), ((180.0, 89.0), (0.0, 89.0))],
                id='nullable-array-left',
            ),
        ],
    )
    def test_pole_shortest_line_endpoints_are_input_points(
        self,
        layout: str,
        fixed_is_left: bool,
        expected: list[tuple[tuple[float, float], tuple[float, float]]],
    ) -> None:
        pole = gm.Point(0.0, 90.0, crs=4326, epoch=2020.0)
        high = gm.Point(180.0, 89.0, crs=4326, epoch=2020.0)
        probe = gm.Point(0.0, 89.0, crs=4326, epoch=2020.0)
        if layout == 'dense':
            array = gm.GeometryArray([pole, high])
            missing = [False, False]
            pairs = [(pole, expected[0]), (high, expected[1])]
        else:
            array = gm.GeometryArray([pole, None, high])
            missing = [False, True, False]
            pairs = [(pole, expected[0]), (None, None), (high, expected[1])]
        result = (
            gm.shortest_line(probe, array)
            if fixed_is_left
            else gm.shortest_line(array, probe)
        )
        left_frame = probe if fixed_is_left else array
        assert result.is_missing.tolist() == missing
        assert result.crs == left_frame.crs
        assert result.epoch == left_frame.epoch
        for index, (value, want) in enumerate(pairs):
            line = result[index]
            if value is None:
                assert line is None
                continue
            left, right = (probe, value) if fixed_is_left else (value, probe)
            _assert_shortest_line_row(line, want, left, right)


class TestCrossingWitnessIsCertified:
    """A crossing witness must be a real shared point, never a fabricated one.

    ``nearest_points``/``shortest_line`` answer a crossing pair from one
    segment-contact primitive whose existence AND placement come from the exact
    predicates. Its predecessor returned ``left.start`` when the parametric
    denominator vanished and had no finiteness guard, so a caller could get a
    point lying on neither operand — indistinguishable from a real witness.
    """

    @pytest.mark.parametrize(
        ('name', 'left', 'right'),
        [
            ('areal transversal', gm.box(0, 0, 2, 2), gm.box(1, 1, 3, 3)),
            (
                'lineal X',
                gm.LineString([(0, 0), (2, 2)]),
                gm.LineString([(0, 2), (2, 0)]),
            ),
            (
                'lineal T touch',
                gm.LineString([(0, 0), (2, 0)]),
                gm.LineString([(1, 0), (1, 2)]),
            ),
            (
                'collinear overlap',
                gm.LineString([(0, 0), (2, 0)]),
                gm.LineString([(1, 0), (3, 0)]),
            ),
            (
                'huge coordinates',
                gm.LineString([(0, 0), (1e300, 1e300)]),
                gm.LineString([(0, 1e300), (1e300, 0)]),
            ),
            (
                'line crosses ring',
                gm.LineString([(-1, 1), (3, 1)]),
                gm.box(0, 0, 2, 2),
            ),
            (
                'multipart',
                gm.MultiLineString([[(0, 0), (2, 2)], [(9, 9), (9, 10)]]),
                gm.MultiLineString([[(0, 2), (2, 0)], [(-9, -9), (-9, -10)]]),
            ),
        ],
    )
    def test_witness_lies_on_both_operands(
        self, name: str, left: gm.Geometry, right: gm.Geometry
    ) -> None:
        assert gm.intersects(left, right), name
        for a, b in ((left, right), (right, left)):
            first, second = gm.nearest_points(a, b)
            assert gm.intersects(a, first), f'{name}: witness not on the left operand'
            assert gm.intersects(b, second), f'{name}: witness not on the right operand'
            assert gm.distance(a, b) == 0.0
            ends = _endpoints(gm.shortest_line(a, b))
            assert ends[0] == pytest.approx((first.x, first.y))
            assert ends[1] == pytest.approx((second.x, second.y))

    def test_mixed_scale_diagonal_witness_is_ulp_accurate(self) -> None:
        """The crossing of a mixed-scale diagonal pair is not representable.

        Only a DIAGONAL at mixed magnitudes reaches the shared orientation
        predicate — an axis-aligned probe takes specialized paths and proves
        nothing here. The two segments cross at ``x = mu*L/(L + mu)``, which
        rounds to the smallest subnormal, so no double lies on both segments
        and an exact on-segment oracle would be wrong. What must hold is that
        the witness is within one ULP of the true crossing rather than an
        arbitrary operand endpoint.
        """
        scale = 2.0**-20
        mu = 2.0**-1074
        left = gm.LineString([(0.0, 0.0), (scale, scale)])
        right = gm.LineString([(0.0, mu), (scale, 0.0)])
        assert gm.intersects(left, right)
        assert gm.distance(left, right) == 0.0

        exact_x = Fraction(mu) * Fraction(scale) / (Fraction(scale) + Fraction(mu))
        first, _ = gm.nearest_points(left, right)
        # `float(exact_x)` is the correctly rounded crossing; the witness must
        # be that value or its immediate neighbour, never a far endpoint.
        target = float(exact_x)
        assert abs(first.x - target) <= math.ulp(target)
        assert abs(first.y - target) <= math.ulp(target)


class TestFrechetMatchesTheFamilyDegradePolicy:
    """Hausdorff rejects an empty support while the older metrics degrade.

    `distance` retains its documented ``inf`` result for an empty operand.
    Fréchet retains its existing sentinel for compatibility; Hausdorff is
    undefined for an empty support and raises a typed geometry error.
    """

    EMPTY = gm.from_wkt('LINESTRING EMPTY')
    LINE = gm.LineString([(0, 0), (1, 1)])

    @pytest.mark.parametrize(
        ('name', 'left', 'right'),
        [
            (
                'empty, line',
                gm.from_wkt('LINESTRING EMPTY'),
                gm.LineString([(0, 0), (1, 1)]),
            ),
            (
                'line, empty',
                gm.LineString([(0, 0), (1, 1)]),
                gm.from_wkt('LINESTRING EMPTY'),
            ),
            # The identical-empty pair took an identity fast path whose kind
            # check reported `EmptyLinework` before the sentinel was reached.
            (
                'empty, empty',
                gm.from_wkt('LINESTRING EMPTY'),
                gm.from_wkt('LINESTRING EMPTY'),
            ),
            (
                'point empty, line empty',
                gm.from_wkt('POINT EMPTY'),
                gm.from_wkt('LINESTRING EMPTY'),
            ),
            (
                'polygon empty pair',
                gm.from_wkt('POLYGON EMPTY'),
                gm.from_wkt('POLYGON EMPTY'),
            ),
        ],
    )
    def test_empty_operand_is_typed_for_hausdorff_and_infinite_for_distance(
        self, name: str, left: gm.Geometry, right: gm.Geometry
    ) -> None:
        with pytest.raises(gm.InvalidGeometryError, match='non-empty'):
            gm.hausdorff_distance(left, right, max_error=1e-12)
        assert gm.frechet_distance(left, right) == math.inf, name
        assert gm.distance(left, right) == math.inf, name

    def test_one_empty_row_is_typed_for_hausdorff_and_total_for_siblings(self) -> None:
        left = gm.GeometryArray([self.LINE, self.EMPTY, self.LINE])
        right = gm.GeometryArray([self.LINE, self.LINE, self.EMPTY])
        with pytest.raises(gm.InvalidGeometryError, match='non-empty'):
            gm.hausdorff_distance(left, right, max_error=1e-12)
        assert gm.frechet_distance(left, right).tolist() == [0.0, math.inf, math.inf]
        assert gm.distance(left, right).tolist() == [0.0, math.inf, math.inf]

    def test_missing_rows_keep_the_array_missing_result(self) -> None:
        masked = gm.GeometryArray([self.LINE, None])
        result = gm.hausdorff_distance(masked, self.LINE, max_error=1e-12)
        assert result[0] == 0.0
        assert math.isnan(result[1])

    def test_identical_non_empty_is_zero_and_wrong_kind_still_raises(self) -> None:
        """Only the DATA condition degrades — a wrong kind is still an error."""
        assert gm.frechet_distance(self.LINE, self.LINE) == 0.0
        for left, right in (
            (gm.box(0, 0, 1, 1), gm.box(0, 0, 1, 1)),  # identical polygons
            (gm.Point(0, 0), self.LINE),
        ):
            with pytest.raises(gm.GeometryTypeError):
                gm.frechet_distance(left, right)
            with pytest.raises(gm.GeometryTypeError):
                gm.frechet_distance(gm.GeometryArray([left]), gm.GeometryArray([right]))
