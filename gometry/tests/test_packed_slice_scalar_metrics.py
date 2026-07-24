"""Packed Lines/Polygons slice + scalar x array metric parity."""

from __future__ import annotations

import gometry as gm
import pytest
from conftest import canon, floats, ids, line_storage_twins, polygon_storage_twins

_SLICES = [slice(None), slice(None, None, -1), slice(0, 1), slice(1, None)]


@pytest.mark.parametrize('slc', _SLICES)
def test_packed_lines_slice_matches_mixed_and_stays_packed(slc: slice) -> None:
    packed, mixed = line_storage_twins()
    assert packed.to_arrow().type.extension_name == 'geoarrow.linestring'
    got = packed[slc]
    expected = mixed[slc]
    assert canon(list(got)) == canon(list(expected))
    assert got.to_arrow().type.extension_name == 'geoarrow.linestring'


@pytest.mark.parametrize('slc', _SLICES)
def test_packed_polygons_slice_matches_mixed_and_stays_packed(slc: slice) -> None:
    packed, mixed = polygon_storage_twins()
    assert packed.to_arrow().type.extension_name == 'geoarrow.polygon'
    got = packed[slc]
    expected = mixed[slc]
    assert canon(list(got)) == canon(list(expected))
    assert got.to_arrow().type.extension_name == 'geoarrow.polygon'


def test_packed_lines_scalar_distance_broadcast_matches_per_row() -> None:
    packed, mixed = line_storage_twins()
    probe = gm.LineString([(0.0, 0.0), (1.0, 1.0)], crs=3857)
    expected = [gm.distance(geom, probe) for geom in mixed]
    assert floats(gm.distance(packed, probe)) == pytest.approx(expected)
    assert floats(gm.distance(probe, packed)) == pytest.approx(expected)


def test_packed_polygons_scalar_distance_broadcast_matches_per_row() -> None:
    packed, mixed = polygon_storage_twins()
    probe = gm.Point(0.5, 0.5, crs=3857)
    expected = [gm.distance(geom, probe) for geom in mixed]
    assert floats(gm.distance(packed, probe)) == pytest.approx(expected)
    assert floats(gm.distance(probe, packed)) == pytest.approx(expected)


def test_packed_lines_length_matches_per_row_scalar() -> None:
    packed, mixed = line_storage_twins()
    expected = [geom.length for geom in mixed]
    assert floats(packed.length) == pytest.approx(expected)


def test_packed_lines_contiguous_slice_window_matches_oracle() -> None:
    import pickle

    packed, mixed = line_storage_twins()
    got = packed[1:]
    expected = mixed[1:]
    assert canon(list(got)) == canon(list(expected))
    assert got.total_bounds == expected.total_bounds
    assert got.coords.x.tolist() == expected.coords.x.tolist()
    assert pickle.loads(pickle.dumps(got)).to_arrow() == expected.to_arrow()


def test_packed_lines_contiguous_take_window_matches_oracle() -> None:
    packed = gm.GeometryArray([
        gm.LineString([(0.0, 0.0), (1.0, 0.0)], crs=3857),
        gm.LineString([(10.0, 0.0), (11.0, 0.0)], crs=3857),
        gm.LineString([(20.0, 0.0), (21.0, 0.0)], crs=3857),
    ])
    mixed = gm.from_wkt([(geom).to_wkt() for geom in packed], crs=packed.crs)
    got = packed[[1, 2]]
    expected = mixed[[1, 2]]
    assert canon(list(got)) == canon(list(expected))
    assert got.total_bounds == expected.total_bounds
    assert got.coords.x.tolist() == expected.coords.x.tolist()


def test_packed_lines_window_spatial_index_uses_logical_rows() -> None:
    packed, _ = line_storage_twins()
    selected = packed[1:]
    idx = gm.SpatialIndex(selected)
    assert ids(idx.query(selected[0])) == [0]


def test_packed_polygons_contiguous_slice_window_matches_oracle() -> None:
    import pickle

    packed, mixed = polygon_storage_twins()
    got = packed[1:]
    expected = mixed[1:]
    assert canon(list(got)) == canon(list(expected))
    assert got.total_bounds == expected.total_bounds
    assert got.coords.x.tolist() == expected.coords.x.tolist()
    assert pickle.loads(pickle.dumps(got)).to_arrow() == expected.to_arrow()


def test_packed_polygons_contiguous_take_window_matches_oracle() -> None:
    packed = gm.GeometryArray([
        gm.box(0.0, 0.0, 1.0, 1.0, crs=3857),
        gm.box(10.0, 0.0, 11.0, 1.0, crs=3857),
        gm.box(20.0, 0.0, 21.0, 1.0, crs=3857),
    ])
    mixed = gm.from_wkt([(geom).to_wkt() for geom in packed], crs=packed.crs)
    got = packed[[1, 2]]
    expected = mixed[[1, 2]]
    assert canon(list(got)) == canon(list(expected))
    assert got.total_bounds == expected.total_bounds
    assert got.coords.x.tolist() == expected.coords.x.tolist()


def test_packed_polygons_window_spatial_index_uses_logical_rows() -> None:
    packed, _ = polygon_storage_twins()
    selected = packed[1:]
    idx = gm.SpatialIndex(selected)
    assert ids(idx.query(selected[0])) == [0]
