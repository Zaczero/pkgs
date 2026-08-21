"""Container-input contracts exercised through representative real calls."""

from __future__ import annotations

import array
import types

import gometry as gm
import numpy as np
import pytest


def test_float_lanes_accept_iterators_and_buffers() -> None:
    expected = 'LINESTRING (0 0, 1 1)'
    xs = (0.0, 1.0)
    assert gm.points(iter(xs), iter(xs))[1] == gm.Point(1.0, 1.0)
    assert gm.LineString(x=iter(xs), y=iter(xs)).to_wkt() == expected
    assert (
        gm.LineString(x=array.array('d', xs), y=array.array('d', xs)).to_wkt()
        == expected
    )
    box = gm.box(0, 0, 2, 2)
    np.testing.assert_array_equal(
        gm.contains_xy(box, iter([0.5, 9.0]), iter([0.5, 9.0])), [True, False]
    )
    out = gm.crs_transform(4326, 4326, iter([1.0]), iter([2.0]))
    assert out[:, 0].tolist() == [1.0]
    assert gm.CRS(4326).geodesic_inverse(
        iter([0.0]), iter([0.0]), iter([1.0]), iter([0.0])
    )['distance'][0] == pytest.approx(111319.49, rel=0.0001)


def test_lanes_and_batches_accept_generators() -> None:
    pts = [gm.Point(0, 0), gm.Point(1, 1)]
    assert len(gm.GeometryArray(g for g in pts)) == 2
    assert gm.union_all(g for g in pts).geometry_type == 'MultiPoint'
    arr = gm.GeometryArray([*pts, gm.Point(2, 2)])
    assert list(arr[[0, 2]]) == [pts[0], gm.Point(2, 2)]
    assert arr[[True, False, True]][1] == gm.Point(2, 2)
    wkts = ['POINT (0 0)', 'POINT (1 1)']
    assert len(gm.from_wkt(w for w in wkts)) == 2
    wkbs = [g.to_wkb() for g in pts]
    assert len(gm.from_wkb(b for b in wkbs)) == 2
    cells = gm.h3_cover(gm.box(0, 0, 1, 1, crs=4326), resolution=4)
    assert gm.CellArray((c for c in cells), type=gm.H3Cell).compact() == cells.compact()
    assert gm.CellArray([str(cells[0])], type=gm.H3Cell)[0] == cells[0]


def test_geometry_iterables_propagate_iterator_failures_without_partial_results() -> (
    None
):
    class IteratorError(RuntimeError):
        pass

    def broken():
        yield gm.Point(0, 0)
        raise IteratorError('source failed after its first geometry')

    for consume in (gm.GeometryArray, gm.union_all, gm.SpatialIndex):
        with pytest.raises(IteratorError, match='source failed'):
            consume(broken())


def test_wkb_accepts_any_buffer_exporter() -> None:
    wkb = gm.Point(1, 2).to_wkb()
    for payload in (bytearray(wkb), memoryview(wkb), array.array('B', wkb)):
        assert gm.from_wkb(payload) == gm.Point(1, 2)


def test_mappings_accepted_everywhere_dicts_are() -> None:
    geojson = types.MappingProxyType({'type': 'Point', 'coordinates': [1.0, 2.0]})
    assert gm.from_geojson(geojson, crs=None) == gm.Point(1, 2)
    projjson = types.MappingProxyType(gm.CRS(4326).to_projjson_dict())
    assert gm.CRS(projjson) == gm.CRS(4326)
    assert gm.CRS(types.MappingProxyType(gm.CRS(32633).to_cf())) == gm.CRS(32633)
    assert gm.Point(0, 0, crs=projjson).crs == 'EPSG:4326'
    area = types.MappingProxyType({
        'west': 9.0,
        'south': 49.0,
        'east': 11.0,
        'north': 51.0,
    })
    moved = gm.Point(10, 50, crs=4326).to_crs(3857, area_of_interest=area)
    assert moved.crs == 'EPSG:3857'
    assert gm.from_geojson(geojson).crs == 'OGC:CRS84'


def test_transform_bounds_accepts_any_float_sequence() -> None:
    out = gm.crs_transform_bounds(4326, 3857, iter([0.0, 0.0, 1.0, 1.0]))
    assert len(out) == 4


def test_crs_accepts_authority_list_pair() -> None:
    assert gm.Point(0, 0, crs=['EPSG', 4326]).crs == 'EPSG:4326'
