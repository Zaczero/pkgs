"""Metamorphic invariants over a randomized geometry corpus.

Where the characterization tests pin exact values on hand-picked inputs, these
tests assert the *relationships* that must hold for every input — dualities
between predicates, DE-9IM transposition, overlay set algebra, and idempotent
transforms. A corpus bug shows up here even when no hand-picked case covers it.
"""

import gometry as gm
import numpy as np
import pytest


def _corpus() -> list[gm.Geometry]:
    # Frozen corpus (seed 99 jagged + last point); no runtime RNG.
    jagged = [
        (5.0, 2.0),
        (5.273182304862493, 3.824053733358194),
        (3.140912698618656, 4.192781937575378),
        (1.6895642024565203, 4.114710278886845),
        (-0.28322220010363086, 4.170449782458377),
        (-1.5311016866080438, 3.0585053284050616),
        (-0.22585615507894063, 0.941494671594939),
        (0.3540577964319209, -0.17044978245837727),
        (1.456546767903769, -0.11471027888684482),
        (3.351577379392662, -0.19278193757537831),
        (3.774338892124594, 0.1759462666418068),
    ]
    return [
        gm.Point(2, 2),
        gm.Point(0, 0),
        gm.Point(50, 50),
        gm.from_wkt('MULTIPOINT ((1 1), (3 3))'),
        gm.LineString([(0, 0), (4, 4)]),
        gm.LineString([(-2, 2), (6, 2)]),
        gm.from_wkt('MULTILINESTRING ((0 0, 1 1), (2 0, 1 1), (1 2, 1 1))'),
        gm.box(0, 0, 4, 4),
        gm.box(2, 2, 6, 6),
        gm.box(10, 10, 12, 12),
        gm.Polygon(jagged),
        gm.from_wkt(
            'MULTIPOLYGON (((0 0, 1 0, 1 1, 0 1, 0 0)), ((3 3, 4 3, 4 4, 3 4, 3 3)))'
        ),
        gm.from_wkt('GEOMETRYCOLLECTION (POINT (2 2), LINESTRING (0 4, 4 0))'),
        gm.from_wkt('POINT EMPTY'),
        gm.from_wkt('POLYGON EMPTY'),
        gm.Point(0.2517149742676619, -2.3990192205257097),
    ]


def test_predicate_dualities_hold_for_every_corpus_pair() -> None:
    corpus = _corpus()
    for a in corpus:
        for b in corpus:
            assert gm.contains(a, b) == gm.within(b, a), (a, b)
            assert gm.covers(a, b) == gm.covered_by(b, a), (a, b)
            assert gm.disjoint(a, b) == (not gm.intersects(a, b)), (a, b)
            for symmetric in (
                'intersects',
                'disjoint',
                'touches',
                'crosses',
                'overlaps',
                'equals',
            ):
                forward = getattr(gm, symmetric)(a, b)
                backward = getattr(gm, symmetric)(b, a)
                assert forward == backward, (symmetric, a, b)
            if gm.contains_properly(a, b):
                assert gm.contains(a, b), (a, b)
            if gm.contains(a, b) and (not b.is_empty):
                assert gm.covers(a, b), (a, b)


def test_relate_matrix_transposes_between_operand_orders() -> None:

    def transpose(matrix: str) -> str:
        return ''.join(matrix[3 * (i % 3) + i // 3] for i in range(9))

    corpus = [g for g in _corpus() if not g.is_empty]
    for a in corpus:
        for b in corpus:
            assert gm.relate(a, b) == transpose(gm.relate(b, a)), (a, b)


def test_overlay_set_algebra_on_rectangle_relation_classes() -> None:
    cases = {
        'identical': ((0, 0, 4, 4), (0, 0, 4, 4)),
        'nested': ((0, 0, 4, 4), (1, 1, 2, 2)),
        'partial_area_overlap': ((0, 0, 4, 4), (2, 1, 6, 3)),
        'disjoint': ((0, 0, 4, 4), (5, 0, 6, 1)),
        'shared_edge': ((0, 0, 4, 4), (4, 0, 6, 4)),
        'shared_corner': ((0, 0, 4, 4), (4, 4, 6, 6)),
    }
    for name, (a_bounds, b_bounds) in cases.items():
        a = gm.box(*a_bounds)
        b = gm.box(*b_bounds)
        union = gm.union(a, b)
        inter = gm.intersection(a, b)
        assert gm.difference(a, union).area <= 1e-07 * a.area
        assert gm.difference(b, union).area <= 1e-07 * b.area
        if not inter.is_empty:
            assert gm.difference(inter, a).area <= 1e-07 * inter.area
            assert gm.difference(inter, b).area <= 1e-07 * inter.area
        assert union.area + inter.area == pytest.approx(a.area + b.area, rel=1e-07)
        sym = gm.symmetric_difference(a, b)
        assert sym.area == pytest.approx(union.area - inter.area, rel=1e-07)
        leak = gm.intersection(gm.difference(a, b), b).area
        assert leak <= 1e-07 * b.area, (name, leak)


def test_buffer_covers_its_input_for_positive_distances() -> None:
    for geom in (
        gm.Point(3, 4),
        gm.LineString([(0, 0), (5, 2), (7, -1)]),
        gm.box(0, 0, 4, 4),
    ):
        for distance in (0.1, 1.0, 10.0):
            assert gm.covers(geom.buffer(distance), geom), (geom, distance)


def test_normalize_reverse_and_quantize_are_self_consistent() -> None:
    for geom in _corpus():
        normalized = geom.normalize()
        assert gm.equals_exact(normalized, normalized.normalize()), geom
        assert gm.equals(geom, normalized), geom
        reversed_twice = geom.reverse().reverse()
        assert gm.equals_exact(geom, reversed_twice), geom
        quantized = geom.quantize(3)
        assert gm.equals_exact(quantized, quantized.quantize(3)), geom


def test_linear_referencing_edge_positions() -> None:
    line = gm.LineString([(0, 0), (10, 0)])
    assert gm.equals(line.line_interpolate(0.0), gm.Point(0, 0))
    assert gm.equals(line.line_interpolate(10.0), gm.Point(10, 0))
    assert gm.equals(line.line_interpolate(99.0), gm.Point(10, 0))
    assert gm.equals(line.line_interpolate(-5.0), gm.Point(5, 0))
    assert gm.equals(line.line_interpolate(-99.0), gm.Point(0, 0))
    assert gm.equals(line.line_substring(4.0, 4.0), gm.Point(4, 0))
    assert 0.0 <= line.line_locate(gm.Point(3, 7)) <= 10.0
    assert line.line_locate(gm.Point(-100, 0)) == 0.0
    assert line.line_locate(gm.Point(100, 0)) == 10.0


def test_geodesic_point_operations_at_poles_and_antimeridian() -> None:
    pole = gm.Point(0.0, 90.0, crs=4326)
    south_a = gm.destination(pole, 0.0, 1000.0)
    south_b = gm.destination(pole, 90.0, 1000.0)
    assert south_a.y == pytest.approx(south_b.y, abs=1e-09)
    start = gm.Point(170.0, 10.0, crs=4326)
    out = gm.destination(start, 90.0, 2000000.0)
    bearing_back = gm.bearing(out, start)
    home = gm.destination(out, bearing_back, gm.distance(out, start))
    assert gm.distance(home, start) < 0.001
    a = gm.Point(179.0, 5.0, crs=4326)
    b = gm.Point(-179.0, 5.0, crs=4326)
    mid = gm.point_between(a, b, 0.5, normalized=True)
    assert gm.distance(mid, a) == pytest.approx(gm.distance(mid, b), rel=1e-09)


def test_epoch_survives_geometry_rebuilding_operations() -> None:
    a = gm.box(0, 0, 2, 2, crs=4326, epoch=2020.0)
    b = gm.box(1, 1, 3, 3, crs=4326, epoch=2020.0)
    rebuilt = [
        a.buffer(0.5),
        a.simplify(0.01),
        a.quantize(3),
        a.reverse(),
        a.normalize(),
        a.centroid(),
        a.boundary(),
        a.convex_hull(),
        gm.union(a, b),
        gm.intersection(a, b),
        gm.difference(a, b),
        gm.symmetric_difference(a, b),
    ]
    for result in rebuilt:
        assert result.crs == 'EPSG:4326', result
        assert result.epoch == 2020.0, result


def test_dwithin_zero_is_exactly_the_intersects_relation() -> None:
    corpus = [g for g in _corpus() if not g.is_empty]
    for a in corpus:
        for b in corpus:
            assert gm.dwithin(a, b, 0.0) == gm.intersects(a, b), (a, b)
            if gm.intersects(a, b):
                assert gm.distance(a, b) == 0.0, (a, b)
            assert gm.dwithin(a, b, 1.0) == gm.dwithin(b, a, 1.0), (a, b)


def test_predicates_and_overlay_agree_on_near_parallel_crossings() -> None:
    """Regression: a 1e-13-slope crossing must not be dropped by the noder.

    The predicates decide crossings with robust orientation; the constructive
    intersection used an *absolute* denominator cutoff that silently discarded
    the cut, leaving `intersects(a, b) == True` with an empty intersection.
    The cutoff is now relative, so both layers agree.
    """
    a = gm.LineString([(0.0, 0.0), (1.0, 1e-13)])
    b = gm.LineString([(0.0, 1e-13), (1.0, 0.0)])
    assert gm.intersects(a, b)
    crossing = gm.intersection(a, b)
    assert isinstance(crossing, gm.Point)
    assert crossing.x == pytest.approx(0.5, abs=1e-09)


@pytest.mark.parametrize('scale', [1e155, 1e-180])
def test_predicates_and_overlay_agree_at_extreme_crossing_scales(scale: float) -> None:
    """Existence and placement share one overflow/underflow-safe frame."""
    a = gm.LineString([(scale, scale), (2.0 * scale, 2.0 * scale)])
    b = gm.LineString([(scale, 2.0 * scale), (2.0 * scale, scale)])
    expected = 1.5 * scale
    variants = (
        (a, b),
        (b, a),
        (a.reverse(), b),
        (a, b.reverse()),
        (b.reverse(), a.reverse()),
    )
    crossings = []
    for left, right in variants:
        assert gm.intersects(left, right)
        crossing = gm.intersection(left, right)
        assert isinstance(crossing, gm.Point)
        assert crossing.x == pytest.approx(expected, rel=2e-15, abs=0.0)
        assert crossing.y == pytest.approx(expected, rel=2e-15, abs=0.0)
        crossings.append((crossing.x, crossing.y))
    assert len(set(crossings)) == 1


@pytest.mark.parametrize('scale', [2.0**600, 2.0**-600])
def test_extreme_crossing_point_lies_exactly_on_both_segments(scale: float) -> None:
    """Binary-exact inputs admit one exact float witness on both segments."""
    a = gm.LineString([(scale, scale), (2.0 * scale, 2.0 * scale)])
    b = gm.LineString([(scale, 2.0 * scale), (2.0 * scale, scale)])
    crossing = gm.intersection(a, b)
    assert isinstance(crossing, gm.Point)
    assert gm.covers(a, crossing)
    assert gm.covers(b, crossing)


def test_extreme_crossing_preserves_mixed_axis_exponents() -> None:
    horizontal = gm.LineString([(-1e300, 0.0), (1e300, 0.0)])
    vertical = gm.LineString([(1e-300, -1e-300), (1e-300, 1e-300)])
    assert gm.intersects(horizontal, vertical)
    crossing = gm.intersection(horizontal, vertical)
    assert isinstance(crossing, gm.Point)
    assert crossing.x == 1e-300
    assert crossing.y == 0.0
    assert gm.covers(horizontal, crossing)
    assert gm.covers(vertical, crossing)


def test_extreme_crossing_preserves_mixed_exponents_within_axes() -> None:
    huge = 2.0**1000
    tiny = 2.0**-999
    long = gm.LineString([(0.0, 0.0), (huge, huge)])
    short = gm.LineString([(0.0, tiny), (tiny, 0.0)])
    expected = 2.0**-1000
    assert not gm.covers(long, gm.Point(0.0, tiny))
    assert not gm.covers(long, gm.Point(tiny, 0.0))
    crossing = gm.intersection(long, short)
    assert isinstance(crossing, gm.Point)
    assert (crossing.x, crossing.y) == (expected, expected)
    assert gm.covers(long, crossing)
    assert gm.covers(short, crossing)


def test_extreme_coordinates_stay_finite_and_exact() -> None:
    line = gm.LineString([(-1e150, 0.0), (1e150, 0.0)])
    probe = gm.Point(0.0, 1.0)
    assert gm.distance(line, probe) == 1.0
    nearest_on_line, _ = gm.nearest_points(line, probe)
    assert gm.equals(nearest_on_line, gm.Point(0, 0))
    assert line.line_locate(probe, normalized=True) == 0.5


def test_overlay_dimension_collapse_at_shared_edges_and_corners() -> None:
    edge = gm.intersection(gm.box(0, 0, 1, 1), gm.box(1, 0, 2, 1))
    assert edge.geometry_type == 'LineString'
    assert edge.length == pytest.approx(1.0)
    corner = gm.intersection(gm.box(0, 0, 1, 1), gm.box(1, 1, 2, 2))
    assert corner.geometry_type == 'Point'
    assert gm.equals(corner, gm.Point(1, 1))


def test_point_probe_lanes_agree_with_scalar_engine_on_area_containment() -> None:
    nested = gm.GeometryCollection([gm.box(0, 0, 4, 4)])
    inside = gm.Point(2, 2)
    probes = gm.points([2.0, 10.0], [2.0, 10.0])
    assert gm.distance(inside, nested) == 0.0
    assert gm.distance(probes, nested)[0] == 0.0
    np.testing.assert_array_equal(gm.dwithin(probes, nested, 0.5), [True, False])
    line = gm.LineString([(0, 0), (10, 0)])
    on_line = gm.points([3.0, 3.0], [0.0, 5.0])
    assert gm.distance(on_line, line)[0] == 0.0
    np.testing.assert_array_equal(gm.dwithin(on_line, line, 1e-12), [True, False])


def test_distance_hausdorff_extreme_coords_match_scalar_oracle() -> None:
    scale = 1e150
    segment_count = 64
    coords = [
        (-scale + i / segment_count * 2 * scale, 0.0) for i in range(segment_count + 1)
    ]
    line = gm.LineString(coords)
    probe = gm.Point(0.0, 1.0)
    assert gm.distance(line, probe) == 1.0
    shifted = gm.LineString([(x + 1e149, y) for x, y in coords])
    hd = gm.hausdorff_distance(line, shifted)
    assert hd > 0.0
    assert hd == pytest.approx(gm.hausdorff_distance(shifted, line))
    bulk = gm.distance(
        gm.GeometryArray([line, shifted]), gm.GeometryArray([probe, probe])
    )
    assert bulk.tolist() == pytest.approx([1.0, 1.0])


def test_nearest_points_survive_extreme_coordinates() -> None:
    left = gm.LineString([(-1e154, 0.0), (1e154, 0.0)])
    right = gm.LineString([(-1e154, 1e155), (1e154, 1e155)])
    a, b = gm.nearest_points(left, right)
    assert a.y == 0.0
    assert b.y == 1e155
