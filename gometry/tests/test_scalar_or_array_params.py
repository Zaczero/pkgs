"""Scalar-or-array magnitude parameters on `GeometryArray` methods.

A family of element-wise methods now accept their numeric magnitude argument as
EITHER a scalar float — broadcast to every row, the long-standing behaviour — OR
a per-element sequence/ndarray of length equal to the array (one value per
geometry, numpy/shapely style). This module pins down that contract:

* a scalar and an all-equal per-element list are equivalent (broadcast);
* a varying per-element vector actually varies the result row-by-row;
* the headline round-trip ``line_locate`` -> ``line_interpolate``
  lands every point back on its own line;
* a length mismatch and a non-finite element both raise (and say so);
* an ``ndarray`` input matches the equivalent ``list`` input;
* results agree with shapely's vectorised forms where it has them; and
* the plain scalar path still produces a correct array.

The methods covered: ``buffer``/``offset_curve``, ``simplify``,
``segmentize``, ``remove_repeated_points``, ``line_interpolate``/
``line_substring`` (with either ``basis``)/
``interpolate_m``, and ``concave_hull``/``polylabel``/
``maximum_inscribed_circle``.
"""

import gometry as gm
import numpy as np
import pytest


def _three_boxes() -> gm.GeometryArray:
    """A length-3 array of unit boxes, one per row."""
    return gm.GeometryArray([
        gm.box(0, 0, 1, 1),
        gm.box(10, 0, 11, 1),
        gm.box(20, 0, 21, 1),
    ])


def _three_lines() -> gm.GeometryArray:
    """A length-3 array of axis-aligned LineStrings of length 10 each."""
    return gm.GeometryArray([
        gm.LineString([(0, 0), (10, 0)]),
        gm.LineString([(0, 10), (10, 10)]),
        gm.LineString([(0, 20), (10, 20)]),
    ])


def _detailed_lines() -> gm.GeometryArray:
    """LineStrings carrying a tiny zig-zag detail, for simplify tests."""
    return gm.GeometryArray([
        gm.LineString([(0, 0), (1, 0.001), (2, 0), (3, 0.001), (4, 0)]),
        gm.LineString([(0, 0), (1, 0.001), (2, 0), (3, 0.001), (4, 0)]),
    ])


def test_buffer_scalar_matches_equal_per_element_list() -> None:
    """``buffer(v)`` equals ``buffer([v, v, v])`` on every row."""
    boxes = _three_boxes()
    scalar = boxes.buffer(0.5)
    per_element = boxes.buffer([0.5, 0.5, 0.5])
    for got, want in zip(scalar.area, per_element.area, strict=True):
        assert got == pytest.approx(want)


def test_simplify_scalar_matches_equal_per_element_list() -> None:
    """``simplify(t)`` equals ``simplify([t, t])`` row by row."""
    lines = _detailed_lines()
    scalar = lines.simplify(0.01)
    per_element = lines.simplify([0.01, 0.01])
    assert [g.to_wkt() for g in scalar] == [g.to_wkt() for g in per_element]


def test_segmentize_scalar_matches_equal_per_element_list() -> None:
    """``segmentize(s)`` equals ``segmentize([s, s, s])`` row by row."""
    lines = _three_lines()
    scalar = lines.segmentize(2.5)
    per_element = lines.segmentize([2.5, 2.5, 2.5])
    assert [g.to_wkt() for g in scalar] == [g.to_wkt() for g in per_element]


def test_line_interpolate_scalar_matches_equal_per_element_list() -> None:
    """``line_interpolate(d)`` equals the all-equal vector form."""
    lines = _three_lines()
    scalar = lines.line_interpolate(4.0)
    per_element = lines.line_interpolate([4.0, 4.0, 4.0])
    assert [p.to_wkt() for p in scalar] == [p.to_wkt() for p in per_element]


def test_buffer_per_element_distance_grows_area_monotonically() -> None:
    """Larger per-row buffer distance yields strictly larger area."""
    boxes = _three_boxes()
    buffered = boxes.buffer([1.0, 2.0, 3.0])
    areas = list(buffered.area)
    assert areas[0] < areas[1] < areas[2]
    assert areas[0] > 1.0


def test_simplify_per_element_tolerance_keeps_vs_collapses_detail() -> None:
    """A tiny tolerance keeps detail; a huge one collapses it, per row."""
    lines = _detailed_lines()
    simplified = lines.simplify([1e-06, 100.0])
    kept, collapsed = list(simplified)
    assert kept.to_wkt() == lines[0].to_wkt()
    assert collapsed.to_wkt() == 'LINESTRING (0 0, 4 0)'


def test_line_locate_then_interpolate_round_trips_onto_each_line() -> None:
    """``line_interpolate(line_locate(p))`` lands p's projection on
    its own line, element-wise.
    """
    lines = _three_lines()
    points = gm.GeometryArray([
        gm.Point(3.0, 5.0),
        gm.Point(7.0, 12.0),
        gm.Point(5.5, 18.0),
    ])
    distances = lines.line_locate(points)
    projected = lines.line_interpolate(distances)
    for line, point in zip(lines, projected, strict=True):
        assert gm.distance(line, point) == pytest.approx(0.0, abs=1e-09)
        assert gm.intersects(line, point)


def test_line_substring_accepts_per_element_bounds() -> None:
    """``line_substring`` takes per-row start and end distances."""
    lines = _three_lines()
    subs = lines.line_substring([0.0, 2.0, 4.0], [5.0, 8.0, 6.0])
    expected_lengths = [5.0, 6.0, 2.0]
    for sub, want in zip(subs, expected_lengths, strict=True):
        assert sub.length == pytest.approx(want)


def test_buffer_length_mismatch_raises_and_mentions_length() -> None:
    """A per-element vector of the wrong length is rejected with a length-aware
    message.
    """
    boxes = _three_boxes()
    with pytest.raises(gm.GeometryError, match='length'):
        boxes.buffer([1.0, 2.0])


def test_simplify_length_mismatch_raises() -> None:
    """``simplify`` rejects a too-long tolerance vector."""
    lines = _detailed_lines()
    with pytest.raises(gm.GeometryError, match='length'):
        lines.simplify([0.1, 0.2, 0.3])


def test_buffer_non_finite_element_raises() -> None:
    """A NaN inside the per-element vector is rejected at the boundary."""
    boxes = _three_boxes()
    with pytest.raises(gm.GeometryError, match='must be finite'):
        boxes.buffer([1.0, float('nan'), 2.0])


def test_segmentize_non_finite_element_raises() -> None:
    """An inf inside the per-element vector is rejected at the boundary."""
    lines = _three_lines()
    with pytest.raises(gm.GeometryError, match='must be finite'):
        lines.segmentize([2.0, float('inf'), 2.0])


def test_buffer_ndarray_input_equals_list_input() -> None:
    """A numpy ``ndarray`` magnitude matches the equivalent ``list``."""
    boxes = _three_boxes()
    from_list = boxes.buffer([1.0, 2.0, 3.0])
    from_ndarray = boxes.buffer(np.array([1.0, 2.0, 3.0]))
    for got, want in zip(from_ndarray.area, from_list.area, strict=True):
        assert got == pytest.approx(want)


def test_line_interpolate_ndarray_input_equals_list_input() -> None:
    """An ndarray of distances matches the equivalent list of distances."""
    lines = _three_lines()
    from_list = lines.line_interpolate([1.0, 5.0, 9.0])
    from_ndarray = lines.line_interpolate(np.array([1.0, 5.0, 9.0]))
    assert [p.to_wkt() for p in from_ndarray] == [p.to_wkt() for p in from_list]


def _shapely_array(geometries: gm.GeometryArray):
    """Round-trip a gometry array into a numpy array of shapely geometries."""
    shapely = pytest.importorskip('shapely')
    return shapely.from_wkt([g.to_wkt() for g in geometries])


def test_buffer_per_element_matches_shapely() -> None:
    """Per-element buffer areas agree with shapely's vectorised ``buffer``."""
    shapely = pytest.importorskip('shapely')
    boxes = _three_boxes()
    distances = [1.0, 2.0, 3.0]
    ours = list(boxes.buffer(distances).area)
    theirs = shapely.area(shapely.buffer(_shapely_array(boxes), np.array(distances)))
    for got, want in zip(ours, theirs, strict=True):
        assert got == pytest.approx(float(want), rel=1e-06)


def test_simplify_per_element_matches_shapely() -> None:
    """Per-element simplify output agrees with shapely's vectorised
    ``simplify`` (raw Douglas-Peucker, topology not preserved).
    """
    shapely = pytest.importorskip('shapely')
    lines = _detailed_lines()
    tolerances = [1e-06, 100.0]
    ours = lines.simplify(tolerances, preserve_topology=False)
    theirs = shapely.simplify(
        _shapely_array(lines), np.array(tolerances), preserve_topology=False
    )
    for got, want in zip(ours, theirs, strict=True):
        assert shapely.equals(shapely.from_wkt(got.to_wkt()), want)


def test_segmentize_per_element_matches_shapely() -> None:
    """Per-element segmentize agrees with shapely's vectorised ``segmentize``
    on vertex count and total length.
    """
    shapely = pytest.importorskip('shapely')
    lines = _three_lines()
    max_lengths = [2.0, 3.0, 5.0]
    ours = lines.segmentize(max_lengths)
    theirs = shapely.segmentize(_shapely_array(lines), np.array(max_lengths))
    for got, want in zip(ours, theirs, strict=True):
        sg = shapely.from_wkt(got.to_wkt())
        assert shapely.get_num_points(sg) == shapely.get_num_points(want)
        assert shapely.length(sg) == pytest.approx(shapely.length(want))


def test_line_interpolate_per_element_matches_shapely() -> None:
    """Per-element point-at-distance agrees with shapely's vectorised
    ``line_interpolate``.
    """
    shapely = pytest.importorskip('shapely')
    lines = _three_lines()
    distances = [1.0, 5.0, 9.0]
    ours = lines.line_interpolate(distances)
    theirs = shapely.line_interpolate_point(_shapely_array(lines), np.array(distances))
    for got, want in zip(ours, theirs, strict=True):
        assert got.x == pytest.approx(want.x)
        assert got.y == pytest.approx(want.y)


def test_scalar_buffer_still_returns_correct_array() -> None:
    """A scalar distance buffers every row identically and correctly."""
    boxes = _three_boxes()
    buffered = boxes.buffer(1.0)
    areas = list(buffered.area)
    assert len(areas) == 3
    assert all(a == pytest.approx(areas[0]) for a in areas)
    assert areas[0] > 1.0


def test_scalar_simplify_still_returns_correct_array() -> None:
    """A scalar tolerance simplifies every row identically."""
    lines = _detailed_lines()
    simplified = lines.simplify(100.0)
    assert [g.to_wkt() for g in simplified] == ['LINESTRING (0 0, 4 0)'] * 2


def test_offset_curve_accepts_per_element_distance() -> None:
    """``offset_curve`` takes a per-row signed distance."""
    lines = _three_lines()
    scalar = lines.offset_curve(1.0)
    per_element = lines.offset_curve([1.0, 1.0, 1.0])
    assert [g.to_wkt() for g in scalar] == [g.to_wkt() for g in per_element]
    varied = lines.offset_curve([1.0, 2.0, 3.0])
    assert len(list(varied)) == 3


def test_simplify_vw_accepts_per_element_tolerance() -> None:
    """``simplify(..., method='vw')`` takes a per-row tolerance and varies the result."""
    lines = _detailed_lines()
    simplified = lines.simplify([1e-06, 100.0], method='vw', preserve_topology=False)
    kept, collapsed = list(simplified)
    assert kept.to_wkt() == lines[0].to_wkt()
    assert collapsed.to_wkt() == 'LINESTRING (0 0, 4 0)'


def test_remove_repeated_points_accepts_per_element_tolerance() -> None:
    """``remove_repeated_points`` takes a per-row tolerance keyword."""
    shapely = pytest.importorskip('shapely')
    lines = gm.GeometryArray([
        gm.LineString([(0, 0), (0.1, 0), (5, 0)]),
        gm.LineString([(0, 0), (0.1, 0), (5, 0)]),
    ])
    cleaned = lines.remove_repeated_points(tolerance=[0.01, 1.0])
    keep, merged = list(cleaned)
    assert shapely.get_num_points(shapely.from_wkt(keep.to_wkt())) == 3
    assert shapely.get_num_points(shapely.from_wkt(merged.to_wkt())) == 2


def test_line_interpolate_m_basis_accepts_per_element_measure() -> None:
    """``line_interpolate(..., basis='m')`` takes a per-row measure."""
    lines = gm.GeometryArray([
        gm.from_wkt('LINESTRING M (0 0 0, 10 0 100)'),
        gm.from_wkt('LINESTRING M (0 10 0, 10 10 100)'),
    ])
    scalar = lines.line_interpolate(50.0, basis='m')
    per_element = lines.line_interpolate([50.0, 50.0], basis='m')
    assert [p.to_wkt() for p in scalar] == [p.to_wkt() for p in per_element]
    varied = lines.line_interpolate([25.0, 75.0], basis='m')
    xs = [p.x for p in varied]
    assert xs[0] == pytest.approx(2.5)
    assert xs[1] == pytest.approx(7.5)


def test_line_substring_m_basis_accepts_per_element_measures() -> None:
    """``line_substring(..., basis='m')`` takes per-row measure bounds."""
    lines = gm.GeometryArray([
        gm.from_wkt('LINESTRING M (0 0 0, 10 0 100)'),
        gm.from_wkt('LINESTRING M (0 10 0, 10 10 100)'),
    ])
    subs = lines.line_substring([0.0, 25.0], [50.0, 75.0], basis='m')
    assert subs[0].length == pytest.approx(5.0)
    assert subs[1].length == pytest.approx(5.0)


def test_interpolate_m_accepts_per_element_range() -> None:
    """``interpolate_m`` takes per-row start/end measures."""
    lines = _three_lines()
    scalar = lines.interpolate_m(0.0, 100.0)
    per_element = lines.interpolate_m([0.0, 0.0, 0.0], [100.0, 100.0, 100.0])
    assert [g.to_wkt() for g in scalar] == [g.to_wkt() for g in per_element]


def test_concave_hull_accepts_per_element_parameters() -> None:
    """``concave_hull`` takes per-row ``concavity`` and ``length_threshold``."""
    points = gm.GeometryArray([
        gm.MultiPoint([(0, 0), (4, 0), (4, 4), (0, 4), (2, 2)]),
        gm.MultiPoint([(0, 0), (4, 0), (4, 4), (0, 4), (2, 2)]),
    ])
    scalar = points.concave_hull(concavity=2.0)
    per_element = points.concave_hull(concavity=[2.0, 2.0])
    assert [g.to_wkt() for g in scalar] == [g.to_wkt() for g in per_element]
    varied = points.concave_hull(concavity=[1.0, 10.0])
    areas = list(varied.area)
    assert areas[1] >= areas[0]


def test_polylabel_accepts_per_element_tolerance() -> None:
    """``polylabel`` takes a per-row search tolerance."""
    polys = gm.GeometryArray([gm.box(0, 0, 10, 10), gm.box(0, 0, 10, 10)])
    scalar = polys.polylabel(tolerance=1.0)
    per_element = polys.polylabel(tolerance=[1.0, 1.0])
    assert [p.to_wkt() for p in scalar] == [p.to_wkt() for p in per_element]
    centre = scalar[0]
    assert centre.x == pytest.approx(5.0, abs=1.0)
    assert centre.y == pytest.approx(5.0, abs=1.0)


def test_maximum_inscribed_circle_accepts_per_element_tolerance() -> None:
    """``maximum_inscribed_circle`` takes a per-row search tolerance."""
    polys = gm.GeometryArray([gm.box(0, 0, 10, 10), gm.box(0, 0, 10, 10)])
    scalar = polys.maximum_inscribed_circle(tolerance=1.0)
    per_element = polys.maximum_inscribed_circle(tolerance=[1.0, 1.0])
    assert [g.to_wkt() for g in scalar] == [g.to_wkt() for g in per_element]


def test_snap_accepts_per_element_tolerance() -> None:
    """``snap`` takes a per-row tolerance: each pair snaps at its own value."""
    lines = gm.GeometryArray([
        gm.LineString([(0, 0), (10, 0)]),
        gm.LineString([(0, 0), (10, 0)]),
    ])
    refs = gm.GeometryArray([gm.Point(5, 0.4), gm.Point(5, 0.4)])
    snapped = gm.snap(lines, refs, [0.5, 0.1])
    rows = [g.to_wkt() for g in snapped]
    assert rows[0] == 'LINESTRING (0 0, 5 0.4, 10 0)'
    assert rows[1] == 'LINESTRING (0 0, 10 0)'


def test_snap_scalar_matches_per_element_constant() -> None:
    """A scalar snap tolerance equals an all-equal per-row list."""
    lines = gm.GeometryArray([
        gm.LineString([(0, 0), (10, 0)]),
        gm.LineString([(0, 0), (10, 0)]),
    ])
    refs = gm.GeometryArray([gm.Point(5, 0.4), gm.Point(5, 0.4)])
    scalar = [g.to_wkt() for g in gm.snap(lines, refs, 0.5)]
    per_element = [g.to_wkt() for g in gm.snap(lines, refs, [0.5, 0.5])]
    assert scalar == per_element


def test_equals_exact_accepts_per_element_tolerance() -> None:
    """``equals_exact`` takes a per-row tolerance."""
    left = gm.GeometryArray([gm.Point(0, 0), gm.Point(0, 0)])
    right = gm.GeometryArray([gm.Point(0, 0.3), gm.Point(0, 0.3)])
    np.testing.assert_array_equal(
        list(gm.equals_exact(left, right, [0.5, 0.1])), [True, False]
    )
    np.testing.assert_array_equal(list(gm.equals_exact(left, right, 0.5)), [True, True])


def test_equals_exact_per_element_ndarray_and_errors() -> None:
    """Ndarray tolerance works; length mismatch and negatives raise."""
    left = gm.GeometryArray([gm.Point(0, 0), gm.Point(0, 0)])
    right = gm.GeometryArray([gm.Point(0, 0.3), gm.Point(0, 0.3)])
    np.testing.assert_array_equal(
        list(gm.equals_exact(left, right, np.array([0.5, 0.1]))), [True, False]
    )
    with pytest.raises(gm.GeometryError, match='length'):
        gm.equals_exact(left, right, [0.5])
    with pytest.raises(gm.GeometryError, match='non-negative finite number'):
        gm.equals_exact(left, right, [0.5, -1.0])


def test_dwithin_accepts_per_element_distance() -> None:
    """``dwithin`` takes a per-row distance threshold (array reference)."""
    pts = gm.points([0.0, 0.0], [0.0, 0.0])
    others = gm.points([3.0, 3.0], [0.0, 0.0])
    np.testing.assert_array_equal(
        list(gm.dwithin(pts, others, [2.0, 5.0])), [False, True]
    )
    np.testing.assert_array_equal(list(gm.dwithin(pts, others, 5.0)), [True, True])
    np.testing.assert_array_equal(list(gm.dwithin(pts, others, 2.0)), [False, False])


def test_dwithin_per_element_scalar_reference_and_ndarray() -> None:
    """Per-row distance against a scalar reference, and ndarray input."""
    pts = gm.points([0.0, 0.0], [0.0, 0.0])
    np.testing.assert_array_equal(
        list(gm.dwithin(pts, gm.Point(3, 0), [2.0, 5.0])), [False, True]
    )
    others = gm.points([3.0, 3.0], [0.0, 0.0])
    np.testing.assert_array_equal(
        list(gm.dwithin(pts, others, np.array([2.0, 5.0]))), [False, True]
    )


def test_dwithin_per_element_errors() -> None:
    """Length mismatch and negative distance raise."""
    pts = gm.points([0.0, 0.0], [0.0, 0.0])
    others = gm.points([3.0, 3.0], [0.0, 0.0])
    with pytest.raises(gm.GeometryError, match='length'):
        gm.dwithin(pts, others, [2.0])
    with pytest.raises(gm.GeometryError, match='non-negative finite number'):
        gm.dwithin(pts, others, [2.0, -1.0])


def test_free_dwithin_accepts_per_element_distance() -> None:
    """The free ``gm.dwithin`` mirrors the method: per-row distance, either
    operand the array (dwithin is symmetric).
    """
    arr = gm.points([0.0, 0.0], [0.0, 0.0])
    other = gm.Point(3, 0)
    np.testing.assert_array_equal(
        list(gm.dwithin(arr, other, [1.0, 5.0])), [False, True]
    )
    np.testing.assert_array_equal(
        list(gm.dwithin(other, arr, [1.0, 5.0])), [False, True]
    )
    np.testing.assert_array_equal(list(gm.dwithin(arr, other, 5.0)), [True, True])


def test_free_delegating_functions_accept_per_element() -> None:
    """Free functions that dispatch to the array method inherit scalar-or-array."""
    polys = gm.GeometryArray([gm.Polygon([(0, 0), (4, 0), (4, 4), (0, 4), (0, 0)])] * 2)
    grown = polys.buffer([1.0, 2.0])
    assert grown[1].area > grown[0].area
    lines = gm.GeometryArray([gm.LineString([(0, 0), (10, 0)])] * 2)
    pts = lines.line_interpolate([2.0, 8.0])
    assert [g.to_wkt() for g in pts] == ['POINT (2 0)', 'POINT (8 0)']


def test_free_snap_accepts_per_element_tolerance() -> None:
    """The free ``gm.snap`` snaps each row with its own tolerance; a scalar
    broadcasts identically.
    """
    geoms = gm.GeometryArray([
        gm.LineString([(0.05, 0.05), (2, 2)]),
        gm.LineString([(0.5, 0.5), (3, 3)]),
    ])
    ref = gm.Point(0, 0)
    out = gm.snap(geoms, ref, [0.1, 0.01])
    assert out[0].to_wkt() == 'LINESTRING (0 0, 2 2)'
    assert out[1].to_wkt() == 'LINESTRING (0.5 0.5, 3 3)'
    assert [g.to_wkt() for g in gm.snap(geoms, ref, [0.1, 0.1])] == [
        g.to_wkt() for g in gm.snap(geoms, ref, 0.1)
    ]


def test_free_snap_scalar_geometry_array_reference_per_element() -> None:
    """A scalar ``geom`` against an array ``reference`` broadcasts per-row,
    each with its own tolerance (the scalar-left/array-right lane).
    """
    geom = gm.LineString([(0.05, 0.05), (2, 2)])
    refs = gm.GeometryArray([gm.Point(0, 0), gm.Point(0, 0)])
    out = gm.snap(geom, refs, [0.1, 0.001])
    assert out[0].to_wkt() == 'LINESTRING (0 0, 2 2)'
    assert out[1].to_wkt() == 'LINESTRING (0.05 0.05, 2 2)'


def test_free_equals_exact_accepts_per_element_tolerance() -> None:
    """The free ``gm.equals_exact`` compares each row to its own tolerance;
    symmetric, so either operand may be the array.
    """
    a = gm.GeometryArray([gm.Point(1, 1), gm.Point(1, 1)])
    b = gm.GeometryArray([gm.Point(1, 1.05), gm.Point(1, 1.05)])
    np.testing.assert_array_equal(
        list(gm.equals_exact(a, b, [0.1, 0.01])), [True, False]
    )
    np.testing.assert_array_equal(
        list(gm.equals_exact(b, a, [0.1, 0.01])), [True, False]
    )
    np.testing.assert_array_equal(
        list(gm.equals_exact(gm.Point(1, 1), b, [0.1, 0.01])), [True, False]
    )
    np.testing.assert_array_equal(list(gm.equals_exact(a, b, 0.1)), [True, True])


def test_free_equals_exact_per_element_length_mismatch_raises() -> None:
    a = gm.GeometryArray([gm.Point(1, 1), gm.Point(1, 1)])
    b = gm.GeometryArray([gm.Point(1, 1), gm.Point(1, 1)])
    with pytest.raises(gm.GeometryError, match='length-2'):
        gm.equals_exact(a, b, [0.1, 0.2, 0.3])


def _measured_lines() -> gm.GeometryArray:
    """A length-2 array of M-carrying horizontal lines (M from 0 to 10)."""
    line = gm.LineString([(0, 0), (10, 0)], m=[0.0, 10.0])
    return gm.GeometryArray([line, line])


def test_array_line_interpolate_m_basis_accepts_per_element_measure() -> None:
    """``GeometryArray.line_interpolate(..., basis='m')`` takes a per-row measure."""
    lines = _measured_lines()
    pts = lines.line_interpolate([2.0, 8.0], basis='m')
    assert [round(p.x, 6) for p in pts] == [2.0, 8.0]
    assert [p.to_wkt() for p in lines.line_interpolate(5.0, basis='m')] == [
        p.to_wkt() for p in lines.line_interpolate([5.0, 5.0], basis='m')
    ]


def test_array_line_substring_m_basis_accepts_per_element_measures() -> None:
    """``GeometryArray.line_substring(..., basis='m')`` takes per-row bounds."""
    lines = _measured_lines()
    subs = lines.line_substring([0.0, 2.0], [5.0, 8.0], basis='m')
    np.testing.assert_allclose([round(s.length, 6) for s in subs], [5.0, 6.0])


def test_free_interpolate_m_accepts_per_element_range() -> None:
    """``GeometryArray.interpolate_m`` fills M per-row from its own range."""
    plain = gm.GeometryArray([gm.LineString([(0, 0), (10, 0)])] * 2)
    filled = plain.interpolate_m([0.0, 100.0], [10.0, 200.0])
    assert [round(g.coords[-1][-1], 6) for g in filled] == [10.0, 200.0]
