"""Correctness/contract guards for the nearest/distance family.

`shortest_line`, `nearest_points`, and `distance` accept ANY geometry pair and
compute the true closest approach — the witness may land on an edge interior,
not only a vertex. These tests lock that down across rotated/oblique inputs and
cross-type pairs, and pin the two deliberate contracts:

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

    def test_point_to_line_geodesic(self) -> None:
        point = gm.Point(0.5, 0.5, crs=4326)
        line = gm.LineString([(0, 0), (10, 0)], crs=4326)
        line_geom = gm.shortest_line(point, line)
        ends = _endpoints(line_geom)
        assert ends[0] == pytest.approx((0.5, 0.5))
        assert ends[1][0] == pytest.approx(0.5, abs=1e-06)
        assert ends[1][1] == pytest.approx(0.0, abs=1e-06)
        assert gm.distance(point, line) == pytest.approx(55287.15, rel=0.0001)

    def test_planar_override(self) -> None:
        point = gm.Point(0.5, 0.5, crs=4326)
        line = gm.LineString([(0, 0), (10, 0)], crs=4326)
        assert (
            gm.shortest_line(point, line, unit='planar')
        ).to_wkt() == 'LINESTRING (0.5 0.5, 0.5 0)'


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
    """`frechet_distance` degrades on an empty operand like its siblings.

    An empty operand is *total*, not an error: `distance` and
    `hausdorff_distance` both answer `inf`, and the columnar contract is that a
    single bad row never fails the rest of a batch. `frechet_distance` raised
    instead — on the scalar surface it disagreed with both siblings, and on the
    array surface it aborted the whole call on `EmptyLinework`, the exact
    condition the degrade policy names.
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
    def test_empty_operand_is_infinite_for_all_three(
        self, name: str, left: gm.Geometry, right: gm.Geometry
    ) -> None:
        assert gm.frechet_distance(left, right) == math.inf, name
        assert gm.hausdorff_distance(left, right) == math.inf, name
        assert gm.distance(left, right) == math.inf, name

    def test_one_empty_row_never_aborts_the_batch(self) -> None:
        left = gm.GeometryArray([self.LINE, self.EMPTY, self.LINE])
        right = gm.GeometryArray([self.LINE, self.LINE, self.EMPTY])
        for measure in (gm.frechet_distance, gm.hausdorff_distance, gm.distance):
            assert measure(left, right).tolist() == [0.0, math.inf, math.inf]

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
