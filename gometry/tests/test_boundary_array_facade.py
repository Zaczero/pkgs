"""Deterministic regression tests for R14-E boundary/array/facade findings."""

import sys

import gometry as gm
import numpy as np
import pytest

# --- A1: SpatialIndex returns typed leaves -----------------------------------


def test_spatial_index_getitem_and_values_are_typed_leaves() -> None:
    pts = [gm.Point(i, i) for i in range(3)]
    poly = gm.box(0, 0, 1, 1)
    idx = gm.SpatialIndex([*pts, poly])
    assert isinstance(idx[0], gm.Point)
    assert isinstance(idx[3], gm.Polygon)
    values = list(idx.values())
    assert isinstance(values[0], gm.Point)
    assert isinstance(values[3], gm.Polygon)
    got = idx.get(1)
    assert isinstance(got, gm.Point)


def test_public_geometry_returns_are_typed_leaves_surface_sample() -> None:
    """Recurrence guard: a sample of public geometry-yielding paths return leaves."""
    p = gm.Point(1, 2)
    line = gm.LineString([(0, 0), (1, 1)])
    poly = gm.box(0, 0, 2, 2)
    samples = [
        p.centroid(),
        p.buffer(1),
        line.centroid(),
        poly.exterior,
        gm.from_wkt('POINT (0 0)'),
        gm.GeometryArray([p])[0],
        gm.SpatialIndex([p])[0],
        next(iter(gm.SpatialIndex([p]).values())),
    ]
    for geom in samples:
        assert type(geom) is not gm.Geometry, f'bare Geometry from {geom!r}'
        assert isinstance(geom, gm.Geometry)


# --- B1: total_bounds invalidated by mask change -----------------------------


def test_with_missing_invalidates_stale_total_bounds() -> None:
    arr = gm.GeometryArray([gm.Point(0, 0), gm.Point(100, 100), gm.Point(50, 50)])
    warm = arr.total_bounds
    assert warm == (0.0, 0.0, 100.0, 100.0)
    masked = arr._with_missing(np.array([False, True, False]))
    # Present extent is only (0,0) and (50,50) — must not retain the warm cache.
    assert masked.total_bounds == (0.0, 0.0, 50.0, 50.0)


# --- B2: all-missing index stays frame-free ----------------------------------


def test_all_missing_spatial_index_adopts_frame_on_first_insert() -> None:
    arr = gm.GeometryArray([None, None, None])
    idx = gm.SpatialIndex(arr)
    # Frame-free: a CRS-tagged insert must be accepted and lock the frame.
    handle = idx.insert(gm.Point(0, 0, crs=4326))
    assert handle >= 0
    assert isinstance(idx[handle], gm.Point)
    with pytest.raises(gm.CRSMismatchError):
        idx.insert(gm.Point(1, 1, crs=3857))


# --- B4: tiny views report logical sizeof, not full parent -------------------


def test_tiny_view_sizeof_much_smaller_than_parent() -> None:
    arr = gm.GeometryArray([gm.Point(i, i) for i in range(10_000)])
    full = sys.getsizeof(arr)
    view = arr[100:110]
    assert isinstance(view, gm.GeometryArray)
    # Logical view of 10 rows must not charge the full parent slot table.
    assert sys.getsizeof(view) < full // 4
    assert sys.getsizeof(view) < 50_000


# --- B5: drop_missing carries frame caches -----------------------------------


def test_drop_missing_preserves_frame_on_present_rows() -> None:
    arr = gm.GeometryArray([
        gm.Point(0, 0, crs=4326),
        None,
        gm.Point(1, 1, crs=4326),
    ])
    dense = arr.drop_missing()
    assert len(dense) == 2
    assert dense.crs is not None
    assert str(dense.crs).startswith('EPSG:4326') or dense.crs.to_epsg() == 4326


# --- C1: lying __len__ must not MemoryError before first yield ---------------


def test_collect_hint_lying_len_does_not_preallocate_wall() -> None:
    class HugeLen:
        def __len__(self) -> int:
            return 10**15

        def __iter__(self):
            yield gm.Point(0, 0)
            yield gm.Point(1, 1)

    # Construction must succeed: advisory length is a clamped initial chunk.
    arr = gm.GeometryArray(HugeLen())
    assert len(arr) == 2


# --- C5: typed empty coordinates retain axes ---------------------------------


def test_empty_point_z_coords_retain_axes() -> None:
    empty = gm.from_wkt('POINT Z EMPTY')
    assert empty.coords.coordinate_axes == 'XYZ'
    assert len(empty.coords) == 0


# --- D1: select layout agrees for membership/count/index/iteration -----------


def test_coordinates_select_protocols_share_visible_layout() -> None:
    c = gm.Point(1, 2, z=3).coords.select('XY')
    assert list(c) == [(1.0, 2.0)]
    assert (1.0, 2.0) in c
    assert (1.0, 2.0, 3.0) not in c
    assert c.index((1.0, 2.0)) == 0
    assert c.count((1.0, 2.0)) == 1

    forced = gm.Point(1, 2).coords.select('XYZ')
    assert list(forced) == [(1.0, 2.0, None)]
    assert (1.0, 2.0, None) in forced
    assert forced.count((1.0, 2.0, None)) == 1


# --- D2: __eq__ propagates provider errors -----------------------------------


def test_coordinates_eq_propagates_provider_errors() -> None:
    c = gm.LineString([(0, 0), (1, 1)]).coords

    class Boom:
        def __iter__(self):
            raise RuntimeError('provider boom')

    with pytest.raises(RuntimeError, match='provider boom'):
        _ = c == Boom()


# --- E2: np.floating NaN is missing ------------------------------------------


def test_pandas_missing_scalar_accepts_numpy_floating_nan() -> None:
    from gometry._pandas import _is_missing_scalar

    assert _is_missing_scalar(np.float64('nan'))
    assert _is_missing_scalar(np.float32('nan'))
    assert _is_missing_scalar(float('nan'))
    assert not _is_missing_scalar(0.0)


# --- E3: GeoParquet row_groups accept numpy integers -------------------------


def test_geoparquet_row_groups_accept_numpy_integral() -> None:
    from gometry._geoparquet import _materialize_row_groups

    groups = _materialize_row_groups([np.int64(0), np.int32(1)])
    assert groups == [0, 1]


# --- E5: structured param/value on integer parameter errors ------------------


def test_integer_parameter_errors_expose_param_and_value() -> None:
    with pytest.raises(gm.GeometryError) as excinfo:
        gm.h3_cover(gm.box(0, 0, 1, 1), resolution=20)
    err = excinfo.value
    assert err.param == 'resolution'
    assert err.value == 20


# --- F3: explain reports live rows, not allocated slots ----------------------


def test_spatial_index_explain_live_rows_not_slots() -> None:
    idx = gm.SpatialIndex([gm.Point(0, 0), None, gm.Point(1, 1)])
    lines = idx.explain()
    assert lines[0] == 'loaded 2 geometries'


# --- F1: affine origin propagates provider exceptions ------------------------


def test_affine_origin_propagates_iter_errors() -> None:
    class Boom:
        def __iter__(self):
            raise RuntimeError('origin boom')

    with pytest.raises(RuntimeError, match='origin boom'):
        gm.Point(0, 0).rotate(90, origin=Boom())


# --- Y1: tile_cover rejects out-of-domain latitude ---------------------------


def test_tile_cover_rejects_out_of_domain_latitude() -> None:
    in_domain = gm.box(-10, 84, 10, 85, crs=4326)
    out_domain = gm.box(-10, 84, 10, 89.9, crs=4326)
    cov = gm.tile_cover(in_domain, zoom=3)
    assert len(cov) >= 1
    with pytest.raises(gm.InvalidGeometryError, match='Web Mercator'):
        gm.tile_cover(out_domain, zoom=3)
