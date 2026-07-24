"""Regression guards: array batch paths must match scalar results.

These pin behaviors that an optimization could silently break:

* the array ``envelope`` all-rectangular fast path must build INDEPENDENT
  prepared handles for its output boxes (it carries the input's per-row bounds,
  which are correct, but must NOT alias the input's lazily-filled prepared
  cache — the output rectangles are different geometry);
* batch geodesic CRS/ellipsoid resolution (resolve once per array op) must give
  results identical to the per-element scalar path.
"""

import gometry as gm
import numpy as np
import pytest

WGS84 = 4326


def test_envelope_prepared_cache_is_not_aliased_to_input():
    tri = gm.from_wkt([
        'POLYGON ((0 0, 10 0, 0 10, 0 0))',
        'POLYGON ((20 20, 30 20, 20 30, 20 20))',
    ])
    probe = gm.Point(8, 8)
    env = (tri).envelope()
    assert bool(gm.contains(env, probe)[0]) is True
    assert bool(gm.contains(tri, probe)[0]) is False


def test_envelope_prepared_independent_when_input_cache_filled_first():
    tri = gm.from_wkt(['POLYGON ((0 0, 10 0, 0 10, 0 0))'])
    probe = gm.Point(8, 8)
    env = (tri).envelope()
    assert bool(gm.contains(tri, probe)[0]) is False
    assert bool(gm.contains(env, probe)[0]) is True


def _scalar(values, crs):
    return [gm.from_wkt(text, crs=crs) for text in values]


def test_geodesic_array_distance_matches_scalar():
    left_wkt = ['POINT (0 0)', 'POINT (10 10)', 'POINT (-170 5)']
    right_wkt = ['POINT (1 1)', 'POINT (11 9)', 'POINT (170 5)']
    left = gm.from_wkt(left_wkt, crs=WGS84)
    right = gm.from_wkt(right_wkt, crs=WGS84)
    array = gm.distance(left, right)
    scalar = np.array([
        gm.distance(a, b)
        for a, b in zip(
            _scalar(left_wkt, WGS84), _scalar(right_wkt, WGS84), strict=True
        )
    ])
    assert np.allclose(array, scalar, rtol=1e-12)


def test_geodesic_array_dwithin_matches_scalar():
    left_wkt = ['POINT (0 0)', 'POINT (10 10)', 'POINT (-170 5)']
    right_wkt = ['POINT (1 1)', 'POINT (11 9)', 'POINT (170 5)']
    left = gm.from_wkt(left_wkt, crs=WGS84)
    right = gm.from_wkt(right_wkt, crs=WGS84)
    array = gm.dwithin(left, right, 200000.0)
    scalar = np.array([
        gm.dwithin(a, b, 200000.0)
        for a, b in zip(
            _scalar(left_wkt, WGS84), _scalar(right_wkt, WGS84), strict=True
        )
    ])
    assert np.array_equal(array, scalar)


def test_free_area_length_accept_raw_iterables():
    boxes = [gm.box(0, 0, 2, 2), gm.box(2, 0, 4, 2)]
    lines = [gm.LineString([(0, 0), (3, 4)]), gm.LineString([(0, 0), (0, 2)])]

    np.testing.assert_array_equal(
        gm.area(boxes), gm.area(gm.GeometryArray(boxes))
    )
    np.testing.assert_array_equal(
        gm.area(box for box in boxes), gm.area(gm.GeometryArray(boxes))
    )
    np.testing.assert_array_equal(
        gm.length(lines), gm.length(gm.GeometryArray(lines))
    )
    np.testing.assert_array_equal(
        gm.length(line for line in lines), gm.length(gm.GeometryArray(lines))
    )
    assert isinstance(gm.area(boxes[0], unit='planar'), float)


@pytest.mark.parametrize('metric', ['area', 'length'])
def test_geodesic_array_area_length_match_scalar(metric):
    wkt = [
        'POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))',
        'POLYGON ((10 10, 12 10, 12 12, 10 12, 10 10), (10.4 10.4, 10.8 10.4, 10.8 10.8, 10.4 10.8, 10.4 10.4))',
        # Multiple holes of unequal size: the packed shell-minus-holes / perimeter
        # accumulation must mirror the scalar expression order so the parity below
        # holds BIT-EXACTLY, not just to a tolerance.
        'POLYGON ((0 0, 6 0, 6 6, 0 6, 0 0), (0.2 0.2, 0.5 0.2, 0.5 0.5, 0.2 0.5, 0.2 0.2), '
        '(4 4, 5.7 4, 5.7 5.7, 4 5.7, 4 4), (1 4, 1.3 4, 1.3 4.3, 1 4.3, 1 4))',
        'POLYGON ((179.5 -0.5, -179.5 -0.5, -179.5 0.5, 179.5 0.5, 179.5 -0.5))',
        'POLYGON EMPTY',
    ]
    polys = gm.from_wkt(wkt, crs=WGS84)
    array = getattr(polys, metric)
    scalar = np.array([getattr(g, metric) for g in _scalar(wkt, WGS84)])
    # The packed geodesic kernel reuses the scalar per-ring math in the scalar
    # combine order, so array and scalar agree to the last bit.
    assert np.array_equal(array, scalar)
    if metric == 'length':
        line_wkt = [
            'LINESTRING (0 0, 1 0, 1 1)',
            'LINESTRING (179.5 0, -179.5 0, -179.5 1)',
            'LINESTRING (10 10, 10.5 10.2, 11 10.1, 12 11)',
            'LINESTRING EMPTY',
        ]
        lines = gm.from_wkt(line_wkt, crs=WGS84)
        np.testing.assert_allclose(
            lines.length,
            [g.length for g in _scalar(line_wkt, WGS84)],
            rtol=1e-12,
            atol=1e-09,
        )


def test_to_geojson_always_rejects_m():
    xym = gm.from_wkt('POINT M (1 2 3)', crs=4326)
    with pytest.raises(gm.InvalidGeometryError, match='GeoJSON has no M ordinate'):
        (xym).to_geojson()
    with pytest.raises(gm.InvalidGeometryError, match='GeoJSON has no M ordinate'):
        (gm.from_wkt(['POINT M (1 2 3)'], crs=4326)).to_geojson()
    assert xym.set_m(None).to_geojson() == '{"type":"Point","coordinates":[1.0,2.0]}'
    assert '9' in (gm.from_wkt('POINT Z (1 2 9)', crs=4326)).to_geojson()


def test_array_topological_dimension_matches_scalar():
    wkt = [
        'POINT (0 0)',
        'LINESTRING (0 0, 1 1)',
        'POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))',
    ]
    array = gm.from_wkt(wkt)
    assert list(array.topological_dimension) == [0, 1, 2]
    assert list(array.topological_dimension) == [
        gm.from_wkt(w).topological_dimension for w in wkt
    ]
    mixed = gm.GeometryArray([
        gm.Point(0, 0),
        gm.from_wkt('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))'),
    ])
    assert list(mixed.topological_dimension) == [0, 2]


def test_geodesic_array_nearest_points_match_scalar():
    left_wkt = ['POINT (0 0)', 'LINESTRING (10 10, 11 11)']
    right_wkt = ['POINT (1 1)', 'POINT (12 12)']
    left = gm.from_wkt(left_wkt, crs=WGS84)
    right = gm.from_wkt(right_wkt, crs=WGS84)
    array = gm.shortest_line(left, right)
    scalar = [
        gm.shortest_line(a, b)
        for a, b in zip(
            _scalar(left_wkt, WGS84), _scalar(right_wkt, WGS84), strict=True
        )
    ]
    for row, expected in zip(array, scalar, strict=True):
        assert gm.equals_exact(row, expected, 1e-09)
