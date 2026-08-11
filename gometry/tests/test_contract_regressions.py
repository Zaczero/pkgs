"""P0 correctness/safety regressions (Arrow-C, pickle, replace, coords, grids,
defaulted params, null Feature, CrsInfo). Fail-before / pass-after for the
their corresponding fixes.
"""

from __future__ import annotations

import collections.abc
import pickle
import struct

import gometry as gm
import numpy as np
import pyarrow as pa
import pytest
from gometry import _lib

# ---------------------------------------------------------------------------
# P0.3 typed-empty Point.__replace__ axes
# ---------------------------------------------------------------------------


def test_point_replace_preserves_typed_empty_axes() -> None:
    empty_z = gm.from_wkt('POINT Z EMPTY')
    assert empty_z.to_wkt() == 'POINT Z EMPTY'
    assert empty_z.__replace__().to_wkt() == 'POINT Z EMPTY'
    assert empty_z.__replace__(crs=4326).to_wkt() == 'POINT Z EMPTY'
    assert empty_z.__replace__(crs=4326).crs == 'EPSG:4326'

    empty_m = gm.from_wkt('POINT M EMPTY')
    assert empty_m.__replace__().to_wkt() == 'POINT M EMPTY'
    empty_zm = gm.from_wkt('POINT ZM EMPTY')
    assert empty_zm.__replace__().to_wkt() == 'POINT ZM EMPTY'

    # Axis-aware Z/M edits on empties.
    cleared = empty_z.__replace__(z=None)
    assert cleared.to_wkt() == 'POINT EMPTY'
    promoted = gm.from_wkt('POINT EMPTY').__replace__(z=1.0)
    assert promoted.to_wkt() == 'POINT Z EMPTY'

    # Materialization that lacks required ordinates raises rather than flattens.
    with pytest.raises(gm.InvalidGeometryError, match='requires z'):
        empty_z.__replace__(x=1.0, y=2.0)
    material = empty_z.__replace__(x=1.0, y=2.0, z=3.0)
    assert material.to_wkt() == 'POINT Z (1 2 3)'


# ---------------------------------------------------------------------------
# P0.4 Coordinates.z/.m ndarray doctrine
# ---------------------------------------------------------------------------


def test_coords_z_m_always_readonly_float64_nan_when_absent() -> None:
    coords = gm.Point(1, 2, crs=4326).coords
    for col in (coords.z, coords.m):
        assert isinstance(col, np.ndarray)
        assert col.dtype == np.float64
        assert col.flags.writeable is False
        assert col.shape == (1,)
        assert np.isnan(col).all()


# ---------------------------------------------------------------------------
# P0.5 bounding_cell skips missing rows
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    'factory',
    [
        gm.h3_bounding_cell,
        gm.s2_bounding_cell,
        gm.geohash_bounding_cell,
        gm.tile_bounding_cell,
    ],
    ids=['h3', 's2', 'geohash', 'tile'],
)
def test_bounding_cell_skips_missing_rows(factory) -> None:
    arr = gm.GeometryArray([gm.Point(13.4, 52.5, crs=4326), None])
    cell = factory(arr)
    assert cell is not None
    # Present nonempty row folds; all-missing still errors.
    with pytest.raises(gm.InvalidGeometryError, match='non-empty'):
        factory(gm.GeometryArray([None, None], crs=4326))


# ---------------------------------------------------------------------------
# Part-1 §2 broadcast-default params reject explicit None
# ---------------------------------------------------------------------------


def test_defaulted_params_reject_explicit_none_and_honor_omission() -> None:
    poly = gm.Polygon([(0, 0), (3, 0), (3, 3), (0, 3), (0, 0)])
    # Omission uses documented defaults.
    assert isinstance(poly.concave_hull(), gm.Polygon)
    assert gm.equals(poly.remove_repeated_points(), poly)
    assert gm.equals_exact(gm.Point(1, 1), gm.Point(1, 1))
    assert poly.smooth() is not None
    snapped = poly.snap_to_grid(1.0)
    assert isinstance(snapped, (gm.Polygon, gm.MultiPolygon))
    rotated = poly.rotate(0.0)
    assert isinstance(rotated, (gm.Polygon, gm.MultiPolygon))
    voronoi = gm.MultiPoint([(0, 0), (1, 0), (0, 1)]).voronoi_polygons()
    assert len(voronoi) >= 1

    # Explicit None raises (TypeError-ish).
    with pytest.raises(TypeError):
        poly.concave_hull(concavity=None)
    with pytest.raises(TypeError):
        poly.concave_hull(length_threshold=None)
    with pytest.raises(TypeError):
        poly.remove_repeated_points(tolerance=None)
    with pytest.raises(TypeError):
        gm.equals_exact(gm.Point(0, 0), gm.Point(0, 0), None)
    with pytest.raises(TypeError):
        poly.smooth(iterations=None)
    with pytest.raises(TypeError):
        poly.snap_to_grid(1.0, origin=None)
    with pytest.raises(TypeError):
        poly.rotate(10.0, origin=None)
    with pytest.raises(TypeError):
        poly.scale(2.0, origin=None)
    with pytest.raises(TypeError):
        poly.skew(origin=None)
    with pytest.raises(TypeError):
        gm.MultiPoint([(0, 0), (1, 0), (0, 1)]).voronoi_polygons(clip=None)


# ---------------------------------------------------------------------------
# P1.1 scalar null Feature rejected
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# P1.5 CrsInfo keeps only deprecated (not is_deprecated)
# ---------------------------------------------------------------------------


def test_crs_info_has_deprecated_not_is_deprecated() -> None:
    info = gm.CRS(4326).info
    assert info['deprecated'] is False
    assert 'is_deprecated' not in info
    assert gm.CRS(4326).is_deprecated is False


# ---------------------------------------------------------------------------
# P0.2 packed GeometryArray pickle representation safety
# ---------------------------------------------------------------------------


def test_pickle_empty_packed_selection_and_permutation() -> None:
    points = gm.points([0, 1, 2], [0, 1, 2])
    empty = points[[]]
    restored_empty = pickle.loads(pickle.dumps(empty))
    assert len(restored_empty) == 0
    assert restored_empty.to_wkt() == []

    permuted = points[[2, 0, 1]]
    restored = pickle.loads(pickle.dumps(permuted))
    assert restored.to_wkt() == permuted.to_wkt()

    sub = points[1:3]
    assert pickle.loads(pickle.dumps(sub)).to_wkt() == sub.to_wkt()


def test_pickle_malformed_mask_and_ring_payloads_rejected() -> None:
    # Bad mask bytes (value 2) at the untrusted mixed unpickler boundary.
    with pytest.raises(
        (gm.GeometryError, ValueError, TypeError), match=r'mask|malformed'
    ):
        _lib._unpickle_geometry_array([b'\x01', b'\x01'], None, None, bytes([2, 0]))

    poly_offsets = struct.pack('<2i', 0, 1)

    # Too short: 2 coords — below MIN_VERTICES_OPEN (still reject).
    xs2 = struct.pack('<2d', 0.0, 1.0)
    ys2 = struct.pack('<2d', 0.0, 0.0)
    with pytest.raises(
        (gm.GeometryError, ValueError, TypeError), match=r'ring|closed|short|three'
    ):
        _lib._unpickle_polygon_array(
            xs2,
            ys2,
            None,
            None,
            struct.pack('<2i', 0, 2),
            poly_offsets,
            None,
            None,
            None,
            None,
        )

    # Open 3-corner and open 4-vertex rings silent-close (A1 = WKT/WKB policy).
    xs3 = struct.pack('<3d', 0.0, 1.0, 0.0)
    ys3 = struct.pack('<3d', 0.0, 0.0, 1.0)
    open3 = _lib._unpickle_polygon_array(
        xs3,
        ys3,
        None,
        None,
        struct.pack('<2i', 0, 3),
        poly_offsets,
        None,
        None,
        None,
        None,
    )
    assert open3.to_wkt() == ['POLYGON ((0 0, 1 0, 0 1, 0 0))']

    xs4 = struct.pack('<4d', 0.0, 1.0, 1.0, 0.5)
    ys4 = struct.pack('<4d', 0.0, 0.0, 1.0, 1.0)
    open4 = _lib._unpickle_polygon_array(
        xs4,
        ys4,
        None,
        None,
        struct.pack('<2i', 0, 4),
        poly_offsets,
        None,
        None,
        None,
        None,
    )
    assert open4.to_wkt() == ['POLYGON ((0 0, 1 0, 1 1, 0.5 1, 0 0))']

    # Invalid CSR endpoint past coordinates.
    xs_ok = struct.pack('<4d', 0.0, 1.0, 1.0, 0.0)
    ys_ok = struct.pack('<4d', 0.0, 0.0, 1.0, 0.0)
    bad_ring = struct.pack('<2i', 0, 99)
    with pytest.raises(
        (gm.GeometryError, ValueError, TypeError), match=r'CSR|offset|ring'
    ):
        _lib._unpickle_polygon_array(
            xs_ok, ys_ok, None, None, bad_ring, poly_offsets, None, None, None, None
        )

    # Empty-ring payload: zero coords, ring window [0,0].
    empty_xs = b''
    empty_ys = b''
    empty_ring = struct.pack('<2i', 0, 0)
    empty_poly = struct.pack('<2i', 0, 1)
    with pytest.raises(
        (gm.GeometryError, ValueError, TypeError), match=r'ring|short|four'
    ):
        _lib._unpickle_polygon_array(
            empty_xs,
            empty_ys,
            None,
            None,
            empty_ring,
            empty_poly,
            None,
            None,
            None,
            None,
        )

    # No-shell polygon: ring_offsets=[0], polygon_offsets=[0,0] (panicked
    # in polygon_view before the shell cardinality gate).
    no_shell_ring = struct.pack('<i', 0)
    no_shell_poly = struct.pack('<2i', 0, 0)
    with pytest.raises(
        (gm.GeometryError, ValueError, TypeError), match=r'shell|ring|CSR'
    ):
        _lib._unpickle_polygon_array(
            empty_xs,
            empty_ys,
            None,
            None,
            no_shell_ring,
            no_shell_poly,
            None,
            None,
            None,
            None,
        )

    # Zero-row packed polygon remains valid: [0]/[0] offsets, empty columns.
    zero_ring = struct.pack('<i', 0)
    zero_poly = struct.pack('<i', 0)
    empty_arr = _lib._unpickle_polygon_array(
        empty_xs, empty_ys, None, None, zero_ring, zero_poly, None, None, None, None
    )
    assert len(empty_arr) == 0


def test_d05_polygon_pickle_rejects_active_ordinate_unclosed_rings() -> None:
    """D05: pack-admission ring closure must cover every active ordinate.

    XY-closed rings whose Z and/or M first!=last previously entered trusted
    packed storage because pickle validated with XY-only ``same_point``.
    Admission must match ``ring_seq_is_packable`` / ``same_active_position``.
    """
    f = lambda xs: b''.join(struct.pack('<d', x) for x in xs)
    i = lambda xs: b''.join(struct.pack('<i', x) for x in xs)
    # Triangle closed in XY: (0,0)→(1,0)→(0,1)→(0,0).
    xs = f([0.0, 1.0, 0.0, 0.0])
    ys = f([0.0, 0.0, 1.0, 0.0])
    ring = i([0, 4])
    poly = i([0, 1])

    # Z closes in XY but not Z (1 != 2) — the audit repro.
    with pytest.raises(gm.GeometryError, match=r'closed|ring') as excinfo:
        _lib._unpickle_polygon_array(
            xs, ys, f([1.0, 1.0, 1.0, 2.0]), None, ring, poly, None, None, None, None
        )
    assert type(excinfo.value).__name__ != 'PanicException'

    # M-only unclosed (first M != last M).
    with pytest.raises(gm.GeometryError, match=r'closed|ring') as excinfo:
        _lib._unpickle_polygon_array(
            xs, ys, None, f([1.0, 1.0, 1.0, 2.0]), ring, poly, None, None, None, None
        )
    assert type(excinfo.value).__name__ != 'PanicException'

    # ZM unclosed on both extra ordinates.
    with pytest.raises(gm.GeometryError, match=r'closed|ring') as excinfo:
        _lib._unpickle_polygon_array(
            xs,
            ys,
            f([1.0, 1.0, 1.0, 2.0]),
            f([3.0, 3.0, 3.0, 4.0]),
            ring,
            poly,
            None,
            None,
            None,
            None,
        )
    assert type(excinfo.value).__name__ != 'PanicException'

    # Genuinely closed 3D ring still unpickles into packed storage.
    zs_closed = f([1.0, 1.0, 1.0, 1.0])
    ok_z = _lib._unpickle_polygon_array(
        xs, ys, zs_closed, None, ring, poly, None, None, None, None
    )
    assert len(ok_z) == 1
    assert list(ok_z[0].exterior.coords.z) == [1.0, 1.0, 1.0, 1.0]
    restored_z = pickle.loads(pickle.dumps(ok_z))
    assert list(restored_z[0].exterior.coords.z) == [1.0, 1.0, 1.0, 1.0]

    # Native 2D closed ring still accepts.
    ok_2d = _lib._unpickle_polygon_array(
        xs, ys, None, None, ring, poly, None, None, None, None
    )
    assert len(ok_2d) == 1
    assert ok_2d[0].to_wkt().startswith('POLYGON')


def test_pickle_drop_missing_and_present_only_gather_round_trip() -> None:
    """drop_missing / fancy gather / bool-filter leave orphan NaNs in
    physical columns but clear the mask — must still round-trip.
    """
    base = gm.GeometryArray([gm.Point(0, 0), None, gm.Point(1, 1)])

    dropped = base.drop_missing()
    restored = pickle.loads(pickle.dumps(dropped))
    assert restored.to_wkt() == ['POINT (0 0)', 'POINT (1 1)']
    assert not restored.is_missing.any()

    gathered = base[[2, 0]]  # present-only fancy gather
    restored_g = pickle.loads(pickle.dumps(gathered))
    assert restored_g.to_wkt() == ['POINT (1 1)', 'POINT (0 0)']

    filtered = base[np.array([True, False, True])]
    restored_f = pickle.loads(pickle.dumps(filtered))
    assert restored_f.to_wkt() == ['POINT (0 0)', 'POINT (1 1)']


def _assert_packed_mask_pickle_round_trips(
    base: gm.GeometryArray, expected_dropped: list[str]
) -> None:
    """drop_missing / present-only gather / bool-filter / empty-selection pickle."""
    dropped = base.drop_missing()
    restored = pickle.loads(pickle.dumps(dropped))
    assert restored.to_wkt() == expected_dropped
    assert not restored.is_missing.any()

    # Present-only fancy gather (physical NaN orphans from mask scatter cleared).
    present_ids = [i for i, m in enumerate(base.is_missing.tolist()) if not m]
    gathered = base[present_ids[::-1]]
    restored_g = pickle.loads(pickle.dumps(gathered))
    assert restored_g.to_wkt() == list(reversed(expected_dropped))

    mask = np.array([not m for m in base.is_missing.tolist()])
    filtered = base[mask]
    restored_f = pickle.loads(pickle.dumps(filtered))
    assert restored_f.to_wkt() == expected_dropped

    # Transformed-mask path (to_crs keeps packed lines/polys + missing mask).
    if base.crs is not None:
        transformed = base.to_crs(3857).drop_missing()
        restored_t = pickle.loads(pickle.dumps(transformed))
        assert len(restored_t) == len(expected_dropped)
        assert not restored_t.is_missing.any()

    empty_sel = base[[]]
    restored_e = pickle.loads(pickle.dumps(empty_sel))
    assert len(restored_e) == 0


def _nullable_packed_linestring_arrow() -> gm.GeometryArray:
    """Nullable packed LineString array via GeoArrow import (true Lines storage,
    not Mixed). Middle row is missing with NaN placeholders under the mask.
    """
    xy = pa.struct([('x', pa.float64()), ('y', pa.float64())])
    points = pa.array([(0.0, 0.0), (1.0, 1.0), (2.0, 2.0), (3.0, 3.0)], type=xy)
    offsets = pa.array([0, 2, 2, 4], type=pa.int32())
    storage = pa.ListArray.from_arrays(
        offsets, points, mask=pa.array([False, True, False])
    )
    field = pa.field(
        'geometry',
        storage.type,
        metadata={
            b'ARROW:extension:name': b'geoarrow.linestring',
            b'ARROW:extension:metadata': b'{}',
        },
    )
    batch = pa.RecordBatch.from_arrays([storage], schema=pa.schema([field]))
    arr = gm.from_arrow(batch)
    # Prove packed (not Mixed): drop_missing densifies to the line unpickler.
    assert arr.drop_missing().__reduce_ex__(5)[0].__name__ == '_unpickle_line_array'
    return arr


def _nullable_packed_polygon_arrow() -> gm.GeometryArray:
    """Nullable packed Polygon array via GeoArrow import (true Polygons storage).

    The null row carries a closed 4-vertex NaN placeholder under the mask
    (empty ring spans are rejected by polygon import); present rows are real.
    """
    xy = pa.struct([('x', pa.float64()), ('y', pa.float64())])
    nan = float('nan')
    points = pa.array(
        [
            (0.0, 0.0),
            (1.0, 0.0),
            (1.0, 1.0),
            (0.0, 0.0),
            (nan, nan),
            (nan, nan),
            (nan, nan),
            (nan, nan),
            (2.0, 2.0),
            (3.0, 2.0),
            (3.0, 3.0),
            (2.0, 2.0),
        ],
        type=xy,
    )
    ring_offsets = pa.array([0, 4, 8, 12], type=pa.int32())
    rings = pa.ListArray.from_arrays(ring_offsets, points)
    poly_offsets = pa.array([0, 1, 2, 3], type=pa.int32())
    storage = pa.ListArray.from_arrays(
        poly_offsets, rings, mask=pa.array([False, True, False])
    )
    field = pa.field(
        'geometry',
        storage.type,
        metadata={
            b'ARROW:extension:name': b'geoarrow.polygon',
            b'ARROW:extension:metadata': b'{}',
        },
    )
    batch = pa.RecordBatch.from_arrays([storage], schema=pa.schema([field]))
    arr = gm.from_arrow(batch)
    assert arr.drop_missing().__reduce_ex__(5)[0].__name__ == '_unpickle_polygon_array'
    return arr


def test_nullable_packed_unary_lanes_skip_placeholders_and_scatter() -> None:
    """A true GeoArrow mask must keep packed unary work off NaN placeholders."""
    lines = _nullable_packed_linestring_arrow()
    dense_lines = lines.drop_missing()
    np.testing.assert_equal(
        lines.length,
        np.array([dense_lines.length[0], np.nan, dense_lines.length[1]]),
    )
    np.testing.assert_equal(
        lines.bounds,
        np.vstack((dense_lines.bounds[0], [np.nan] * 4, dense_lines.bounds[1])),
    )

    polygons = _nullable_packed_polygon_arrow()
    dense_polygons = polygons.drop_missing()
    boundary = polygons.boundary()
    assert boundary.is_missing.tolist() == [False, True, False]
    assert boundary.to_wkb() == [
        dense_polygons.boundary().to_wkb()[0],
        None,
        dense_polygons.boundary().to_wkb()[1],
    ]


def test_pickle_packed_line_drop_missing_gather_filter_round_trip() -> None:
    """Packed LINES (not Mixed) must densify NaN placeholders on pickle."""
    base = _nullable_packed_linestring_arrow()
    assert base.is_missing.tolist() == [False, True, False]
    _assert_packed_mask_pickle_round_trips(
        base,
        ['LINESTRING (0 0, 1 1)', 'LINESTRING (2 2, 3 3)'],
    )


def test_pickle_packed_polygon_drop_missing_gather_filter_round_trip() -> None:
    """Packed POLYGONS (not Mixed) must densify NaN placeholders on pickle."""
    base = _nullable_packed_polygon_arrow()
    assert base.is_missing.tolist() == [False, True, False]
    _assert_packed_mask_pickle_round_trips(
        base,
        [
            'POLYGON ((0 0, 1 0, 1 1, 0 0))',
            'POLYGON ((2 2, 3 2, 3 3, 2 2))',
        ],
    )


def test_public_constructor_malformed_wkb_polygon_never_panics() -> None:
    """Empty/too-short WKB rings reject; XY-open ≥3 corners silent-close (A1).

    Empty and < MIN_VERTICES_OPEN rings are structurally illegal —
    ``from_wkb`` raises a typed ParseError, never PanicException. Open rings
    with ≥3 corners silent-close under the shared untrusted ring admission
    (same policy as WKT and pickle).
    """
    reject_cases = {
        'zero_ring_coords': (
            # Release-blocking repro: hex 01030000000100000000000000
            bytes.fromhex('01030000000100000000000000')
        ),
        'two_vertex_ring': (
            # 2 vertices — below MIN_VERTICES_OPEN
            b'\x01\x03\x00\x00\x00\x01\x00\x00\x00\x02\x00\x00\x00'
            + struct.pack('<4d', 0.0, 0.0, 1.0, 0.0)
        ),
        'closed_3_vertex_ring': (
            # 3 coordinates, closed (first==last) — only 2 corners
            b'\x01\x03\x00\x00\x00\x01\x00\x00\x00\x03\x00\x00\x00'
            + struct.pack('<6d', 0.0, 0.0, 1.0, 0.0, 0.0, 0.0)
        ),
    }
    for name, wkb in reject_cases.items():
        with pytest.raises(gm.ParseError, match=r'ring|coordinates|vertices') as raised:
            gm.from_wkb(wkb)
        assert type(raised.value).__name__ != 'PanicException', name
        with pytest.raises(gm.ParseError):
            gm.from_wkb([wkb])

    # 3 open corners — silent-close (was WKB-only reject before A1 uniformity).
    open3 = b'\x01\x03\x00\x00\x00\x01\x00\x00\x00\x03\x00\x00\x00' + struct.pack(
        '<6d', 0.0, 0.0, 1.0, 0.0, 0.0, 1.0
    )
    assert gm.from_wkb(open3).to_wkt() == 'POLYGON ((0 0, 1 0, 0 1, 0 0))'
    assert gm.from_wkb(open3).is_valid

    # 4 vertices, first != last — silent-close to a valid ring (A1).
    unclosed = b'\x01\x03\x00\x00\x00\x01\x00\x00\x00\x04\x00\x00\x00' + struct.pack(
        '<8d', 0.0, 0.0, 1.0, 0.0, 1.0, 1.0, 0.5, 1.0
    )
    arr = gm.from_wkb([unclosed])
    assert len(arr) == 1
    assert arr.to_wkt() == [
        'POLYGON ((0 0, 1 0, 1 1, 0.5 1, 0 0))',
    ]
    assert bool(arr.is_valid[0])
    geom = gm.from_wkb(unclosed)
    assert geom.is_valid
    assert geom.area > 0.0


def test_pickle_crafted_masked_row_map_nan_payload_rejected() -> None:
    """A logical mask applied to physical rows must not admit orphan NaNs.
    Packed-point lane no longer accepts a missing mask; non-finite columns
    are rejected by from_owned_columns.
    """
    import math

    xs = struct.pack('<2d', math.nan, 1.0)
    ys = struct.pack('<2d', math.nan, 1.0)
    row_map = struct.pack('<2Q', 1, 0)
    with pytest.raises(
        (gm.InvalidGeometryError, gm.GeometryError), match=r'finite|coordinate'
    ):
        _lib._unpickle_point_array(xs, ys, None, None, None, None, row_map, None)


def test_pickle_malformed_wkb_polygon_stays_mixed_not_packed() -> None:
    """The mixed WKB unpickler must not opportunistically pack.

    Uses a *valid, pack-admissible* polygon WKB so swapping
    ``mixed`` for ``pack_or_mixed`` would produce packed storage and fail
    the reduce-name check (malformed WKB demotes under pack_admission and
    would not detect that swap).
    """
    # Closed 4-vertex polygon — would pack under pack_or_mixed.
    wkb = gm.Polygon([(0, 0), (1, 0), (1, 1), (0, 0)]).to_wkb()
    arr = _lib._unpickle_geometry_array([wkb], None, None, None)
    assert len(arr) == 1
    assert arr[0] is not None
    assert arr[0].is_valid
    # Unpickler must force Mixed: reduce stays on the mixed WKB lane.
    assert arr.__reduce_ex__(5)[0].__name__ == '_unpickle_geometry_array'
    restored = pickle.loads(pickle.dumps(arr))
    assert restored.__reduce_ex__(5)[0].__name__ == '_unpickle_geometry_array'
    assert restored[0].is_valid


def test_d06_mixed_pickle_rejects_srid_plus_crs_free_rows() -> None:
    """D06: mixed unpickle must not stamp an EWKB SRID onto CRS-free rows.

    Bulk ``from_wkb`` rejects an SRID-tagged row mixed with a plain WKB row
    when no explicit ``crs=`` is given. The pickle reconstructor must reuse
    the same ``SridFrameAdmission`` path — never silently assign the SRID frame
    to plain rows.
    """
    ewkb = struct.pack('<BII2d', 1, 0x20000001, 4326, 1.0, 2.0)  # POINT SRID=4326
    plain = struct.pack('<BI2d', 1, 1, 3.0, 4.0)  # POINT no SRID

    with pytest.raises(gm.CRSMismatchError) as bulk_exc:
        gm.from_wkb([ewkb, plain])
    assert type(bulk_exc.value).__name__ != 'PanicException'

    with pytest.raises(gm.CRSMismatchError) as pickle_exc:
        _lib._unpickle_geometry_array([ewkb, plain], None, None, None)
    assert type(pickle_exc.value).__name__ != 'PanicException'

    # Payload crs= provides the frame for plain rows (normal pickle path).
    framed = _lib._unpickle_geometry_array([plain, plain], 'EPSG:4326', None, None)
    assert framed.crs is not None
    assert framed.crs.to_authority() == ('EPSG', '4326')
    assert framed.to_wkt() == ['POINT (3 4)', 'POINT (3 4)']

    # Real array pickle round-trip still restores exactly (plain WKB + frame).
    arr = gm.GeometryArray([gm.Point(1, 2, crs=4326), gm.Point(3, 4, crs=4326)])
    restored = pickle.loads(pickle.dumps(arr))
    assert restored.crs == arr.crs
    assert restored.to_wkt() == arr.to_wkt()
    assert restored.to_wkt() == ['POINT (1 2)', 'POINT (3 4)']

    # Masked CRS-free placeholder next to an EWKB row still admits (mask skip).
    masked = _lib._unpickle_geometry_array([ewkb, plain], None, None, bytes([0, 1]))
    assert masked.crs is not None
    assert masked.crs.to_authority() == ('EPSG', '4326')
    assert masked.is_missing[1]


def test_r16_mixed_unpickle_epoch_after_ewkb_crs() -> None:
    """R16: mixed unpickle discovers EWKB CRS before validating epoch.

    Exact audit repro: ``from_wkb([ewkb], epoch=2020.0)`` accepts while
    ``_unpickle_geometry_array([ewkb], None, 2020.0, None)`` previously raised
    ``CRSError: coordinate epoch requires a CRS`` because epoch ran before the
    embedded-SRID scan.
    """
    w = gm.Point(1, 2, crs=4326).to_wkb(include_srid=True)

    from_wkb = gm.from_wkb([w], epoch=2020.0)
    assert from_wkb.crs is not None
    assert from_wkb.crs.to_authority() == ('EPSG', '4326')
    assert from_wkb.epoch == 2020.0
    assert [str(value) for value in from_wkb] == ['POINT (1 2)']

    static = gm.Point(1, 2, crs=2180).to_wkb(include_srid=True)
    with pytest.raises(
        ValueError,
        match=r'^a coordinate epoch requires a dynamic CRS; EPSG:2180 is static\. Remove epoch= or transform to a dynamic CRS first$',
    ):
        gm.from_wkb([static], epoch=2020.0)

    unpickled = _lib._unpickle_geometry_array([w], None, 2020.0, None)
    assert unpickled.crs is not None
    assert unpickled.crs.to_authority() == ('EPSG', '4326')
    assert unpickled.epoch == 2020.0
    assert [str(value) for value in unpickled] == ['POINT (1 2)']

    # Honest pickle round-trip still works with CRS+epoch on the array frame.
    framed = gm.GeometryArray([gm.Point(1, 2, crs=4326)], epoch=2020.0)
    restored = pickle.loads(pickle.dumps(framed))
    assert restored.crs == framed.crs
    assert restored.epoch == 2020.0
    assert [str(value) for value in restored] == ['POINT (1 2)']

    # Epoch without any CRS (payload or EWKB) still errors.
    plain = gm.Point(1, 2).to_wkb()  # no SRID
    with pytest.raises(
        gm.CRSError,
        match=r'^a coordinate epoch requires a CRS; attach one with crs= \(or set_crs\(\.\.\.\)\) before tagging an epoch$',
    ):
        _lib._unpickle_geometry_array([plain], None, 2020.0, None)
    with pytest.raises(
        gm.CRSError,
        match=r'^a coordinate epoch requires a CRS; attach one with crs= \(or set_crs\(\.\.\.\)\) before tagging an epoch$',
    ):
        gm.from_wkb([plain], epoch=2020.0)


# ---------------------------------------------------------------------------
# P0.1 Arrow-C layout validation
# ---------------------------------------------------------------------------


def test_arrow_c_multirow_binary_and_large_binary_round_trip() -> None:
    wkbs = [gm.Point(i, i).to_wkb() for i in range(5)]
    for arrow_type in (pa.binary(), pa.large_binary()):
        arrow = pa.array(wkbs, type=arrow_type)
        restored = gm.from_arrow(arrow)
        assert restored.to_wkt() == [f'POINT ({i} {i})' for i in range(5)]


def test_arrow_c_nonzero_offset_and_sliced_geoarrow_struct() -> None:
    values = gm.points([0, 1, 2, 3], [0, 1, 2, 3], crs=4326)
    arrow = values.to_arrow()
    sliced = arrow.slice(1, 2)
    restored = gm.from_arrow(sliced)
    assert restored.to_wkt() == ['POINT (1 1)', 'POINT (2 2)']
    # Coordinate child fields of a sliced GeoArrow struct inherit parent offset
    # so the visible span matches the slice, not the full parent allocation.
    assert len(gm.from_arrow(sliced)) == 2
    assert restored.crs == values.crs


def test_true_nonzero_offset_c_array_capsule_binary() -> None:
    """True nonzero-offset C array capsule (PyArrow slice retains offset;
    gometry re-export always writes offset=0 so must not be the only test).
    """
    wkbs = [gm.Point(i, i).to_wkb() for i in range(5)]
    full = pa.array(wkbs, type=pa.binary())
    sliced = full.slice(1, 3)  # offset=1, length=3 in the C ABI
    # Capsule-only forces owned native Arrow admission with a real offset.
    restored = gm.from_arrow(_capsule_only_from_pyarrow(sliced))
    assert restored.to_wkt() == ['POINT (1 1)', 'POINT (2 2)', 'POINT (3 3)']
    # LargeBinary sibling.
    sliced_l = pa.array(wkbs, type=pa.large_binary()).slice(2, 2)
    restored_l = gm.from_arrow(_capsule_only_from_pyarrow(sliced_l))
    assert restored_l.to_wkt() == ['POINT (2 2)', 'POINT (3 3)']


def test_arrow_c_null_count_unknown_and_excessive() -> None:
    # Known nulls still import as missing rows through pyarrow.
    # Layout edge cases (null_count=-1 / <-1 / known>length, overflow, struct
    # child bounds, Binary vs LargeBinary widths) are exercised by the pure
    # Rust battery `layout_validation_tests` in src/py/arrow_c/native.rs —
    # pyarrow normalizes those C-ABI fields before they reach from_arrow.
    binary = pa.array(
        [gm.Point(0, 0).to_wkb(), None, gm.Point(1, 1).to_wkb()],
        type=pa.binary(),
    )
    restored = gm.from_arrow(binary)
    assert restored.is_missing.tolist() == [False, True, False]


def test_arrow_c_malformed_payload_rejected() -> None:
    # Truncated WKB payload must fail as a parse error, never UB.
    # Schema-driven offset/overflow rejection is covered by the Rust
    # layout_validation_tests battery (native_buffer_len / checked_byte_span /
    # binary terminal-offset width; fallible format parse rejects Other).
    with pytest.raises((gm.ParseError, TypeError, ValueError)):
        gm.from_arrow(pa.array([b'\x01'], type=pa.binary()))


def test_arrow_c_empty_array_with_unknown_null_count_imports() -> None:
    """Empty arrays with null_count=-1 must not require a validity bitmap."""
    empty = pa.array([], type=pa.binary())
    restored = gm.from_arrow(empty)
    assert len(restored) == 0


def test_arrow_c_sliced_struct_child_null_count_raw_length() -> None:
    """Struct-child null_count is validated against the raw child length, not
    the projected parent-visible length.
    """
    values = gm.points([0, 1, 2, 3, 4, 5, 6, 7, 8, 9], [0] * 10, crs=4326)
    arrow = values.to_arrow()
    sliced = arrow.slice(2, 3)
    restored = gm.from_arrow(sliced)
    assert restored.to_wkt() == ['POINT (2 0)', 'POINT (3 0)', 'POINT (4 0)']


def test_arrow_c_binary_view_wkb_round_trip() -> None:
    """BinaryView storage (geoarrow.wkb / pa.binary_view) through the real
    import path: schema tree + buffer iteration + final copy.
    """
    if not hasattr(pa, 'binary_view'):
        pytest.skip('pyarrow binary_view unavailable')
    wkbs = [gm.Point(i, i).to_wkb() for i in range(3)]
    arrow = pa.array(wkbs, type=pa.binary_view())
    restored = gm.from_arrow(arrow)
    assert restored.to_wkt() == [f'POINT ({i} {i})' for i in range(3)]


def test_zero_chunk_binary_view_accepted() -> None:
    """B: zero-chunk BinaryView must share non-empty acceptance (empty GeometryArray)."""
    if not hasattr(pa, 'binary_view'):
        pytest.skip('pyarrow binary_view unavailable')
    empty = gm.from_arrow(pa.chunked_array([], type=pa.binary_view()))
    assert len(empty) == 0
    # Capsule-only zero-chunk is not a ChunkedArray; empty BinaryView array
    # still exercises the same type frame acceptance.
    empty2 = gm.from_arrow(pa.array([], type=pa.binary_view()))
    assert len(empty2) == 0


def test_zero_chunk_large_list_geoarrow_extension_admits_empty() -> None:
    """LargeList-backed GeoArrow accepts zero chunks as an empty column."""
    if not hasattr(pa, 'large_list'):
        pytest.skip('pyarrow large_list unavailable')

    xy = pa.struct([('x', pa.float64()), ('y', pa.float64())])
    storage_type = pa.large_list(xy)

    class _GeoarrowLinestring(pa.ExtensionType):
        def __arrow_ext_serialize__(self) -> bytes:
            return b'{}'

        @classmethod
        def __arrow_ext_deserialize__(cls, storage_type, serialized):
            return cls(storage_type)

    ext = _GeoarrowLinestring(storage_type, 'geoarrow.linestring')
    # Zero-chunk extension type (the C zero-chunk frame path).
    zero = pa.chunked_array([], type=ext)
    empty = gm.from_arrow(zero)
    assert len(empty) == 0

    # Field-metadata LargeList + zero batches (Table) also admits empty.
    field = pa.field(
        'geometry',
        storage_type,
        metadata={
            b'ARROW:extension:name': b'geoarrow.linestring',
            b'ARROW:extension:metadata': b'{}',
        },
    )
    table = pa.Table.from_batches([], schema=pa.schema([field]))
    empty_t = gm.from_arrow(table)
    assert len(empty_t) == 0


def test_binary_view_multi_buffer_external_wkb_import() -> None:
    """Pin true multi-data-buffer BinaryView import (not a single external buffer).

    PyArrow 24 packs one array into one data buffer; concatenating two
    BinaryView arrays yields ≥2 external data buffers end-to-end.
    """
    if not hasattr(pa, 'binary_view'):
        pytest.skip('pyarrow binary_view unavailable')
    # Long WKB forces external (non-inline) BinaryView descriptors on each side.
    left = [
        gm.LineString([(float(j), float(j)) for j in range(30)]).to_wkb()
        for _ in range(4)
    ]
    right = [
        gm.LineString([(float(j + 10), float(j)) for j in range(30)]).to_wkb()
        for _ in range(4)
    ]
    assert all(len(w) > 12 for w in left + right)
    arrow = pa.concat_arrays([
        pa.array(left, type=pa.binary_view()),
        pa.array(right, type=pa.binary_view()),
    ])
    # ≥4 Arrow buffers: validity, views, data0, data1 (true multi-buffer).
    assert len(arrow.buffers()) >= 4, (
        f'expected multi-buffer BinaryView, got {len(arrow.buffers())}'
    )
    restored = gm.from_arrow(arrow)
    assert len(restored) == 8
    assert (
        restored[0].to_wkt()
        == gm.LineString([(float(j), float(j)) for j in range(30)]).to_wkt()
    )
    assert (
        restored[4].to_wkt()
        == gm.LineString([(float(j + 10), float(j)) for j in range(30)]).to_wkt()
    )
    # Capsule-only forces native BinaryView materialization over multi buffers.
    restored_c = gm.from_arrow(_capsule_only_from_pyarrow(arrow))
    assert restored_c.to_wkt() == restored.to_wkt()


def test_geoarrow_closed_3_coord_ring_rejects_like_wkt_wkb() -> None:
    """G1/F2: closed 3-coordinate GeoArrow ring is rejected at admission.

    Shared ``admit_closed_ring`` policy: XY-closed rings need ≥4 vertices
    (MIN_VERTICES_CLOSED). GeoArrow must not admit a short ring WKT/WKB refuse.
    A legal 4-vertex ring still imports and pickles cleanly.
    """
    xy = pa.struct([('x', pa.float64()), ('y', pa.float64())])
    # Degenerate closed 3-vertex ring: (0,0), (1,0), (0,0).
    points3 = pa.array([(0.0, 0.0), (1.0, 0.0), (0.0, 0.0)], type=xy)
    rings3 = pa.ListArray.from_arrays(pa.array([0, 3], type=pa.int32()), points3)
    polygons3 = pa.ListArray.from_arrays(pa.array([0, 1], type=pa.int32()), rings3)
    field = pa.field(
        'geometry',
        polygons3.type,
        metadata={
            b'ARROW:extension:name': b'geoarrow.polygon',
            b'ARROW:extension:metadata': b'{}',
        },
    )
    batch3 = pa.RecordBatch.from_arrays([polygons3], schema=pa.schema([field]))
    with pytest.raises(
        (gm.ParseError, gm.InvalidGeometryError, TypeError, ValueError),
        match=r'three coordinates|RingTooShort|require at least',
    ):
        gm.from_arrow(batch3)

    # Legal triangle (4 verts closed) still round-trips through pickle.
    points4 = pa.array([(0.0, 0.0), (1.0, 0.0), (0.0, 1.0), (0.0, 0.0)], type=xy)
    rings4 = pa.ListArray.from_arrays(pa.array([0, 4], type=pa.int32()), points4)
    polygons4 = pa.ListArray.from_arrays(pa.array([0, 1], type=pa.int32()), rings4)
    field4 = pa.field(
        'geometry',
        polygons4.type,
        metadata={
            b'ARROW:extension:name': b'geoarrow.polygon',
            b'ARROW:extension:metadata': b'{}',
        },
    )
    arr4 = gm.from_arrow(
        pa.RecordBatch.from_arrays([polygons4], schema=pa.schema([field4]))
    )
    restored = pickle.loads(pickle.dumps(arr4))
    assert len(restored) == 1
    _ = restored.is_valid
    _ = restored.area


# ---------------------------------------------------------------------------
# Capsule-only Arrow path (forces ImportedCapsules → native.rs, NOT pyarrow)
# ---------------------------------------------------------------------------


class _CapsuleOnly:
    """Arrow C provider without a ``.type`` attribute so from_arrow cannot
    dispatch to the PyArrow branch (mod.rs:344). Exercises native.rs end-to-end.
    """

    __slots__ = ('_capsules',)

    def __init__(self, schema_capsule: object, array_capsule: object) -> None:
        self._capsules = (schema_capsule, array_capsule)

    def __arrow_c_array__(self, requested_schema=None):
        return self._capsules


def _capsule_only_from_pyarrow(arrow: pa.Array) -> _CapsuleOnly:
    schema, array = arrow.__arrow_c_array__()
    return _CapsuleOnly(schema, array)


def _capsule_only_from_gometry(arr: gm.GeometryArray) -> _CapsuleOnly:
    schema, array = arr.__arrow_c_array__()
    return _CapsuleOnly(schema, array)


def test_capsule_only_binary_large_binary_binary_view_and_empty() -> None:
    """Capsule-only imports for z / Z / vz / empty — must hit native.rs."""
    wkbs = [gm.Point(i, i).to_wkb() for i in range(3)]
    for arrow_type in (pa.binary(), pa.large_binary()):
        restored = gm.from_arrow(
            _capsule_only_from_pyarrow(pa.array(wkbs, type=arrow_type))
        )
        assert restored.to_wkt() == [f'POINT ({i} {i})' for i in range(3)]

    if hasattr(pa, 'binary_view'):
        restored = gm.from_arrow(
            _capsule_only_from_pyarrow(pa.array(wkbs, type=pa.binary_view()))
        )
        assert restored.to_wkt() == [f'POINT ({i} {i})' for i in range(3)]

    # Empty Binary / LargeBinary / BinaryView — zero-sized buffers may be null.
    for arrow_type in (pa.binary(), pa.large_binary()):
        empty = gm.from_arrow(_capsule_only_from_pyarrow(pa.array([], type=arrow_type)))
        assert len(empty) == 0
    if hasattr(pa, 'binary_view'):
        empty_v = gm.from_arrow(
            _capsule_only_from_pyarrow(pa.array([], type=pa.binary_view()))
        )
        assert len(empty_v) == 0


def test_capsule_only_geoarrow_struct_slice_nested_list() -> None:
    """Capsule-only +s (struct point) and nested +l (linestring) + slices."""
    points = gm.points([0, 1, 2, 3], [0, 1, 2, 3], crs=4326)
    restored = gm.from_arrow(_capsule_only_from_gometry(points))
    assert restored.to_wkt() == points.to_wkt()
    assert restored.crs == points.crs

    # Empty WKB capsule path (zero-sized buffers may be null).
    empty_wkb = gm.from_arrow(
        _capsule_only_from_pyarrow(pa.array([], type=pa.binary()))
    )
    assert len(empty_wkb) == 0

    sliced = points[1:3]
    restored_s = gm.from_arrow(_capsule_only_from_gometry(sliced))
    assert restored_s.to_wkt() == ['POINT (1 1)', 'POINT (2 2)']

    lines = gm.GeometryArray(
        [
            gm.LineString([(0, 0), (1, 1)]),
            gm.LineString([(2, 2), (3, 3), (4, 4)]),
        ],
        crs=4326,
    )
    restored_l = gm.from_arrow(_capsule_only_from_gometry(lines))
    assert restored_l.to_wkt() == lines.to_wkt()


def test_capsule_only_large_list_geometry_extension_admits() -> None:
    """Native capsule and PyArrow lanes agree on checked-i64 LargeList input.

    Bare LargeList without a geometry extension remains non-geometry storage.
    """
    if not hasattr(pa, 'large_list'):
        pytest.skip('pyarrow large_list unavailable')
    coords = pa.array([0.0, 0.0, 1.0, 1.0], type=pa.float64())
    large = pa.LargeListArray.from_arrays(pa.array([0, 2], type=pa.int64()), coords)
    # Bare LargeList (no geometry encoding) is not a geometry storage type.
    with pytest.raises((TypeError, gm.ParseError, ValueError)):
        gm.from_arrow(large)

    # Both GeoArrow paths read checked i64 offsets; they must not reinterpret
    # [0, 2] as i32 cells and produce EMPTY/a short line.
    xy = pa.struct([('x', pa.float64()), ('y', pa.float64())])
    points = pa.array([(0.0, 0.0), (1.0, 1.0)], type=xy)
    storage = pa.LargeListArray.from_arrays(pa.array([0, 2], type=pa.int64()), points)
    field = pa.field(
        'geometry',
        storage.type,
        metadata={
            b'ARROW:extension:name': b'geoarrow.linestring',
            b'ARROW:extension:metadata': b'{}',
        },
    )
    batch = pa.RecordBatch.from_arrays([storage], schema=pa.schema([field]))
    admitted = gm.from_arrow(batch)
    assert admitted.to_wkt() == ['LINESTRING (0 0, 1 1)']

    class _GeoArrowLinestringType(pa.ExtensionType):
        def __init__(self) -> None:
            super().__init__(storage.type, 'geoarrow.linestring')

        def __arrow_ext_serialize__(self) -> bytes:
            return b'{}'

        @classmethod
        def __arrow_ext_deserialize__(
            cls, storage_type: pa.DataType, serialized: bytes
        ) -> _GeoArrowLinestringType:
            del storage_type, serialized
            return cls()

    extension = _GeoArrowLinestringType()
    native = gm.from_arrow(
        _capsule_only_from_pyarrow(pa.ExtensionArray.from_storage(extension, storage))
    )
    assert native.to_wkt() == admitted.to_wkt()


def test_pyarrow_large_list_geometry_extension_admits() -> None:
    """Geoarrow linestring stored as LargeList decodes with checked i64 offsets.

    Offsets [0, 2] as i64 must NOT be misread as i32 cells (which would yield
    EMPTY or a short line). Admission is the GeoArrow SHOULD for LargeList.
    """
    if not hasattr(pa, 'large_list'):
        pytest.skip('pyarrow large_list unavailable')
    xy = pa.struct([('x', pa.float64()), ('y', pa.float64())])
    points = pa.array([(0.0, 0.0), (1.0, 1.0)], type=xy)
    offsets = pa.array([0, 2], type=pa.int64())
    storage = pa.LargeListArray.from_arrays(offsets, points)
    field = pa.field(
        'geometry',
        storage.type,
        metadata={
            b'ARROW:extension:name': b'geoarrow.linestring',
            b'ARROW:extension:metadata': b'{}',
        },
    )
    batch = pa.RecordBatch.from_arrays([storage], schema=pa.schema([field]))
    arr = gm.from_arrow(batch)
    assert arr.to_wkt() == ['LINESTRING (0 0, 1 1)']


def test_stub_snap_to_grid_origin_is_numeric_pair_only() -> None:
    """Runtime GridOrigin rejects affine tokens; stub must match."""
    pt = gm.Point(1.2, 3.4)
    assert isinstance(pt.snap_to_grid(1.0, origin=(0.0, 0.0)), gm.Point)
    with pytest.raises((TypeError, gm.GeometryError, ValueError)):
        pt.snap_to_grid(1.0, origin='centroid')  # type: ignore[arg-type]


def test_coords_select_nan_vs_none_doctrine() -> None:
    """Ndarray columns use NaN for absent axes; nested/tuple iteration uses None."""
    coords = gm.Point(1, 2).coords.select('XYZ')
    assert np.isnan(coords.z).all()
    # Iteration / tuple rows (not the flat Point to_nested list) carry None.
    assert list(coords) == [(1.0, 2.0, None)]
    line = gm.LineString([(0, 0), (1, 1)]).coords.select('XYZ')
    assert np.isnan(line.z).all()
    assert list(line) == [(0.0, 0.0, None), (1.0, 1.0, None)]


# ---------------------------------------------------------------------------
# Ship-gate: five remaining Arrow-C / WKB / packed-storage defects
# ---------------------------------------------------------------------------


def test_with_missing_cannot_clear_mask_on_nullable_packed_arrow() -> None:
    """_with_missing must OR with the existing mask — never expose NaN placeholders.

    Nullable packed Arrow import puts NaN placeholder vertices under the mask;
    clearing those bits would yield trusted ``LINESTRING (NaN NaN, …)``.
    """
    arr = _nullable_packed_linestring_arrow()
    assert arr.is_missing.tolist() == [False, True, False]
    # Attempt to clear every bit (the defect: replace-mask with all-False).
    cleared = arr._with_missing([False] * len(arr))
    assert cleared.is_missing.tolist() == [False, True, False]
    assert cleared[1] is None
    # Present rows still usable; no NaN geometry trusted.
    assert 'NaN' not in cleared[0].to_wkt()
    assert np.isfinite(cleared[0].length)
    # fill_missing remains the valid unmask path.
    filled = arr.fill_missing(gm.LineString([(9, 9), (10, 10)]))
    assert filled.is_missing.tolist() == [False, False, False]
    assert filled.to_wkt() == [
        'LINESTRING (0 0, 1 1)',
        'LINESTRING (9 9, 10 10)',
        'LINESTRING (2 2, 3 3)',
    ]
    # Same on nullable packed polygons.
    polys = _nullable_packed_polygon_arrow()
    still_masked = polys._with_missing([False] * len(polys))
    assert still_masked.is_missing.tolist() == [False, True, False]
    assert still_masked[1] is None
    assert 'NaN' not in still_masked[0].to_wkt()
    assert np.isfinite(still_masked[0].area)


def test_empty_arrow_c_stream_large_list_geoarrow_admits() -> None:
    """Zero-batch native streams accept GeoArrow LargeList like the direct lane."""
    if not hasattr(pa, 'large_list'):
        pytest.skip('pyarrow large_list unavailable')
    xy = pa.struct([('x', pa.float64()), ('y', pa.float64())])
    storage_type = pa.large_list(xy)
    field = pa.field(
        'geometry',
        storage_type,
        metadata={
            b'ARROW:extension:name': b'geoarrow.linestring',
            b'ARROW:extension:metadata': b'{}',
        },
    )
    table = pa.Table.from_batches([], schema=pa.schema([field]))

    class _StreamOnly:
        """Force the native empty-stream path (no .type / pyarrow dispatch)."""

        __slots__ = ('_obj',)

        def __init__(self, obj: object) -> None:
            self._obj = obj

        def __arrow_c_stream__(self, requested_schema=None):
            return self._obj.__arrow_c_stream__(requested_schema)

    assert len(gm.from_arrow(_StreamOnly(table))) == 0
    # i32 List remains equivalent.
    ok_field = pa.field(
        'geometry',
        pa.list_(xy),
        metadata={
            b'ARROW:extension:name': b'geoarrow.linestring',
            b'ARROW:extension:metadata': b'{}',
        },
    )
    ok_table = pa.Table.from_batches([], schema=pa.schema([ok_field]))
    empty = gm.from_arrow(_StreamOnly(ok_table))
    assert len(empty) == 0
    empty_wkb = gm.from_arrow(
        _StreamOnly(
            pa.Table.from_batches(
                [],
                schema=pa.schema([
                    pa.field(
                        'geometry',
                        pa.binary(),
                        metadata={
                            b'ARROW:extension:name': b'geoarrow.wkb',
                            b'ARROW:extension:metadata': b'{}',
                        },
                    )
                ]),
            )
        )
    )
    assert len(empty_wkb) == 0


def test_from_arrow_untrusted_length_raises_clean_not_panic() -> None:
    """Arrow length=i64::MAX must not PanicException (checked-reservation keystone).

    Crafts a binary (format ``z``) capsule with length=i64::MAX and three null
    buffers so from_arrow hits the mixed-import capacity path.
    """
    import ctypes

    # Build a released-owned schema/array pair via pyarrow, then mutate length
    # is not possible after export. Instead construct minimal C ABI capsules
    # with the review's exact state: format z, length=i64::MAX, n_buffers=3.
    # Use ctypes to allocate ArrowSchema + ArrowArray matching the ABI.
    class ArrowSchema(ctypes.Structure):
        _fields_ = [
            ('format', ctypes.c_char_p),
            ('name', ctypes.c_char_p),
            ('metadata', ctypes.c_char_p),
            ('flags', ctypes.c_int64),
            ('n_children', ctypes.c_int64),
            ('children', ctypes.POINTER(ctypes.c_void_p)),
            ('dictionary', ctypes.c_void_p),
            ('release', ctypes.c_void_p),
            ('private_data', ctypes.c_void_p),
        ]

    class ArrowArray(ctypes.Structure):
        _fields_ = [
            ('length', ctypes.c_int64),
            ('null_count', ctypes.c_int64),
            ('offset', ctypes.c_int64),
            ('n_buffers', ctypes.c_int64),
            ('n_children', ctypes.c_int64),
            ('buffers', ctypes.POINTER(ctypes.c_void_p)),
            ('children', ctypes.POINTER(ctypes.c_void_p)),
            ('dictionary', ctypes.c_void_p),
            ('release', ctypes.c_void_p),
            ('private_data', ctypes.c_void_p),
        ]

    # Keep buffer table alive for the capsule lifetime.
    buffer_table = (ctypes.c_void_p * 3)(0, 0, 0)

    # Dummy non-null release so the importer accepts the capsules.
    @ctypes.CFUNCTYPE(None, ctypes.c_void_p)
    def _noop_release(_ptr):
        pass

    schema = ArrowSchema(
        format=b'z',
        name=b'',
        metadata=None,
        flags=0,
        n_children=0,
        children=None,
        dictionary=None,
        release=ctypes.cast(_noop_release, ctypes.c_void_p),
        private_data=None,
    )
    array = ArrowArray(
        length=2**63 - 1,  # i64::MAX
        null_count=0,
        offset=0,
        n_buffers=3,
        n_children=0,
        buffers=ctypes.cast(buffer_table, ctypes.POINTER(ctypes.c_void_p)),
        children=None,
        dictionary=None,
        release=ctypes.cast(_noop_release, ctypes.c_void_p),
        private_data=None,
    )
    # Pin structures.
    schema_box = ctypes.pointer(schema)
    array_box = ctypes.pointer(array)

    # PyCapsule from ctypes pointers.
    PyCapsule_New = ctypes.pythonapi.PyCapsule_New
    PyCapsule_New.restype = ctypes.py_object
    PyCapsule_New.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_void_p]
    schema_cap = PyCapsule_New(schema_box, b'arrow_schema', None)
    array_cap = PyCapsule_New(array_box, b'arrow_array', None)
    provider = _CapsuleOnly(schema_cap, array_cap)

    # Must raise a clean error — never pyo3_runtime.PanicException.
    with pytest.raises(BaseException) as exc_info:
        gm.from_arrow(provider)
    err = exc_info.value
    assert type(err).__name__ != 'PanicException', f'panicked: {err!r}'
    assert 'capacity overflow' not in str(err).lower()
    # Accept TypeError/ParseError/MemoryError/ValueError/Overflow from the path.
    assert isinstance(
        err,
        (
            TypeError,
            ValueError,
            MemoryError,
            gm.ParseError,
            gm.GeometryError,
            OverflowError,
        ),
    ), type(err)


def test_geometry_array_untrusted_len_raises_clean_not_panic() -> None:
    """GeometryArray(values) must not panic on ``__len__ = sys.maxsize``.

    Untrusted ``__len__`` hints are clamped (never a product validity cap);
    growth is fallible. Accept either a clean error or an empty array from the
    empty iterator — never PanicException / capacity overflow.
    """
    import sys

    class HugeLenEmptyIter:
        def __len__(self) -> int:
            return sys.maxsize

        def __iter__(self):
            return iter(())

    try:
        out = gm.GeometryArray(HugeLenEmptyIter())
    except (MemoryError, ValueError, OverflowError, gm.GeometryError, TypeError):
        # Clean domain error path — never PanicException (that would not match).
        return
    # Clamp path: reserve was bounded and the empty iterator produced empty.
    assert len(out) == 0


def test_wkb_structure_budget_rejects_empty_ring_amplification() -> None:
    """200k empty rings fail structurally (min ring vertices / encoding bound).

    No magic input-relative structure ratio — empty rings are illegal because a
    closed ring needs ≥4 coordinates. Rejection must not allocate the rings.
    """
    n = 200_000
    wkb = b'\x01\x03\x00\x00\x00' + struct.pack('<I', n) + b'\x00\x00\x00\x00' * n
    assert len(wkb) == 9 + 4 * n
    with pytest.raises(
        (gm.ParseError, gm.InvalidGeometryError),
        match=r'ring|vertices|too short|count|coordinates',
    ):
        gm.from_wkb(wkb)
    # Same structural rejection on the mixed unpickler path.
    with pytest.raises(
        (gm.ParseError, gm.InvalidGeometryError),
        match=r'ring|vertices|too short|count|coordinates',
    ):
        _lib._unpickle_geometry_array([wkb], None, None, None)
    # Legitimate multi-ring polygon still parses.
    ok = gm.Polygon(
        [(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)],
        [[(2, 2), (3, 2), (3, 3), (2, 2)]],
    )
    assert gm.from_wkb(ok.to_wkb()).area > 0


# ---------------------------------------------------------------------------
# Ingress gate: undo over-rejection + empty/stream parity + trust-boundary
# sequences (2026-07 review)
# ---------------------------------------------------------------------------


class _StreamOnly:
    """Force the Arrow C stream path (no .type / pyarrow dispatch)."""

    __slots__ = ('_obj',)

    def __init__(self, obj: object) -> None:
        self._obj = obj

    def __arrow_c_stream__(self, requested_schema=None):
        return self._obj.__arrow_c_stream__(requested_schema)


class _LyingLenSequence(collections.abc.Sequence):
    """Sequence whose ``__len__`` is a deliberate lie (sys.maxsize).

    Registers as ``collections.abc.Sequence`` so PyO3 ``PySequence`` extract
    would allocate from the lying length via generic ``Vec<T>: FromPyObject``;
    the real payload is tiny and only reachable via ``__getitem__``.
    """

    __slots__ = ('_items', '_reported')

    def __init__(self, items: list, reported: int | None = None) -> None:
        self._items = items
        self._reported = reported if reported is not None else (1 << 62)

    def __len__(self) -> int:
        return self._reported

    def __getitem__(self, index):
        if isinstance(index, slice):
            return [self[i] for i in range(*index.indices(len(self._items)))]
        if index < 0 or index >= len(self._items):
            raise IndexError(index)
        return self._items[index]


def test_multichunk_point_import_matches_single_chunk() -> None:
    """Multi-chunk packed import must equal single-chunk (no chunking-dependent cap)."""
    n = 2_500
    xs = list(range(n))
    ys = [0.0] * n
    single = gm.points(xs, ys).to_arrow()
    mid = n // 2
    multi = pa.chunked_array([single.slice(0, mid), single.slice(mid, n - mid)])
    a = gm.from_arrow(single)
    b = gm.from_arrow(multi)
    assert a.to_wkt() == b.to_wkt()
    assert len(a) == n


def test_malformed_geoarrow_polygon_offsets_raise_not_panic() -> None:
    """Nested ring offsets that escape the loaded span must raise cleanly.

    polygon offsets [0,2], ring offsets [0,100,4] against 100 coords previously
    panicked in ArrowOrdinateValues::value (index out of bounds).
    """
    from gometry._arrow import GEOARROW_POLYGON, _extension_type_from_storage

    # 100 XY coordinate structs; visible polygon claims 2 rings with a runaway
    # first ring window [0, 100] then [100, 4] (non-monotonic / out of span).
    coords = pa.StructArray.from_arrays(
        [pa.array([float(i) for i in range(100)]), pa.array([0.0] * 100)],
        names=['x', 'y'],
    )
    # Ring offsets: [0, 100, 4] — first ring claims 100 vertices, second is inverted.
    ring_offsets = pa.array([0, 100, 4], type=pa.int32())
    rings = pa.ListArray.from_arrays(ring_offsets, coords)
    poly_offsets = pa.array([0, 2], type=pa.int32())
    polys = pa.ListArray.from_arrays(poly_offsets, rings)
    ext = _extension_type_from_storage(pa, GEOARROW_POLYGON, polys.type, None, None)
    arrow = pa.ExtensionArray.from_storage(ext, polys)
    with pytest.raises(
        (gm.ParseError, TypeError, ValueError, gm.GeometryError),
        match=r'offset|span|range|ordered|vertex|coordinate|buffer',
    ) as exc_info:
        gm.from_arrow(arrow)
    # Must not be a PanicException / index out of bounds.
    assert 'PanicException' not in type(exc_info.value).__name__
    assert 'index out of bounds' not in str(exc_info.value).lower()


def test_empty_stream_rejects_bare_non_geometry_types() -> None:
    """Zero-chunk stream must reject the same bare types non-empty rejects."""
    for typ in [
        pa.int64(),
        pa.float64(),
        pa.string(),
        pa.list_(pa.float64()),
        pa.struct([('x', pa.float64()), ('y', pa.float64())]),
    ]:
        table = pa.Table.from_batches([], schema=pa.schema([pa.field('col', typ)]))
        with pytest.raises((TypeError, gm.ParseError, ValueError)):
            gm.from_arrow(_StreamOnly(table))


def test_r14_zero_batch_binary_with_illegal_child_rejected() -> None:
    """R14: schema-only/zero-batch must reject Binary/`z` with n_children=1.

    Nonempty arrays already reject impossible child cardinality via layout
    validation; a zero-batch stream must reject the same impossible binary
    schema without dereferencing its declared child table.
    """
    import ctypes

    class ArrowSchema(ctypes.Structure):
        pass

    ArrowSchema._fields_ = [
        ('format', ctypes.c_char_p),
        ('name', ctypes.c_char_p),
        ('metadata', ctypes.c_void_p),
        ('flags', ctypes.c_int64),
        ('n_children', ctypes.c_int64),
        ('children', ctypes.POINTER(ctypes.POINTER(ArrowSchema))),
        ('dictionary', ctypes.c_void_p),
        ('release', ctypes.c_void_p),
        ('private_data', ctypes.c_void_p),
    ]

    class ArrowArray(ctypes.Structure):
        _fields_ = [
            ('length', ctypes.c_int64),
            ('null_count', ctypes.c_int64),
            ('offset', ctypes.c_int64),
            ('n_buffers', ctypes.c_int64),
            ('n_children', ctypes.c_int64),
            ('buffers', ctypes.c_void_p),
            ('children', ctypes.c_void_p),
            ('dictionary', ctypes.c_void_p),
            ('release', ctypes.c_void_p),
            ('private_data', ctypes.c_void_p),
        ]

    class ArrowArrayStream(ctypes.Structure):
        _fields_ = [
            ('get_schema', ctypes.c_void_p),
            ('get_next', ctypes.c_void_p),
            ('get_last_error', ctypes.c_void_p),
            ('release', ctypes.c_void_p),
            ('private_data', ctypes.c_void_p),
        ]

    @ctypes.CFUNCTYPE(None, ctypes.c_void_p)
    def _noop_release(_ptr: int) -> None:
        pass

    release_fn = ctypes.cast(_noop_release, ctypes.c_void_p)

    # Child: real float64 leaf (format "g").
    child = ArrowSchema(
        format=b'g',
        name=b'x',
        metadata=None,
        flags=0,
        n_children=0,
        children=None,
        dictionary=None,
        release=release_fn,
        private_data=None,
    )
    child_ptr = ctypes.pointer(child)
    children_table = (ctypes.POINTER(ArrowSchema) * 1)(child_ptr)

    # Root: format "z" (Binary) with illegal n_children=1 + float64 child.
    root = ArrowSchema(
        format=b'z',
        name=b'geometry',
        metadata=None,
        flags=0,
        n_children=1,
        children=ctypes.cast(
            children_table, ctypes.POINTER(ctypes.POINTER(ArrowSchema))
        ),
        dictionary=None,
        release=release_fn,
        private_data=None,
    )

    # Zero-batch stream: schema is the impossible Binary tree; get_next ends.
    stream = ArrowArrayStream()

    @ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_void_p, ctypes.c_void_p)
    def get_schema(_stream: int, out_schema: int) -> int:
        out = ArrowSchema.from_address(out_schema)
        out.format = root.format
        out.name = root.name
        out.metadata = root.metadata
        out.flags = root.flags
        out.n_children = root.n_children
        out.children = root.children
        out.dictionary = root.dictionary
        out.release = release_fn
        out.private_data = None
        return 0

    @ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_void_p, ctypes.c_void_p)
    def get_next(_stream: int, out_array: int) -> int:
        # End of stream: leave release=None (Arrow C convention).
        out = ArrowArray.from_address(out_array)
        out.length = 0
        out.null_count = 0
        out.offset = 0
        out.n_buffers = 0
        out.n_children = 0
        out.buffers = None
        out.children = None
        out.dictionary = None
        out.release = None
        out.private_data = None
        return 0

    @ctypes.CFUNCTYPE(ctypes.c_char_p, ctypes.c_void_p)
    def get_last_error(_stream: int) -> None:
        return None

    stream.get_schema = ctypes.cast(get_schema, ctypes.c_void_p)
    stream.get_next = ctypes.cast(get_next, ctypes.c_void_p)
    stream.get_last_error = ctypes.cast(get_last_error, ctypes.c_void_p)
    stream.release = release_fn
    stream.private_data = None

    # Keep live refs so GC cannot free the CFUNCTYPE objects mid-call.
    stream._keep = (
        get_schema,
        get_next,
        get_last_error,
        _noop_release,
        child,
        children_table,
        root,
    )  # type: ignore[attr-defined]

    PyCapsule_New = ctypes.pythonapi.PyCapsule_New
    PyCapsule_New.restype = ctypes.py_object
    PyCapsule_New.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_void_p]
    stream_box = ctypes.pointer(stream)
    stream_cap = PyCapsule_New(stream_box, b'arrow_array_stream', None)

    class _StreamCapsuleOnly:
        def __arrow_c_stream__(self, requested_schema=None):
            return stream_cap

    with pytest.raises((
        TypeError,
        gm.ParseError,
        gm.GeometryError,
        ValueError,
    )) as exc_info:
        gm.from_arrow(_StreamCapsuleOnly())
    assert type(exc_info.value).__name__ != 'PanicException'
    msg = str(exc_info.value).lower()
    assert 'child' in msg or 'children' in msg or 'binary' in msg or '0' in msg

    # Positive: legal zero-batch Binary WKB schema still accepts.
    ok = pa.Table.from_batches(
        [],
        schema=pa.schema([
            pa.field(
                'geometry',
                pa.binary(),
                metadata={b'ARROW:extension:name': b'geoarrow.wkb'},
            )
        ]),
    )
    empty = gm.from_arrow(_StreamOnly(ok))
    assert len(empty) == 0

    # Nonempty array with the same illegal layout is still rejected (parity).
    class _ArrayCapsuleOnly:
        def __init__(self, schema_cap: object, array_cap: object) -> None:
            self._schema = schema_cap
            self._array = array_cap

        def __arrow_c_array__(self, requested_schema=None):
            return (self._schema, self._array)

    # Reuse the illegal schema with a length=0 array (layout still sees n_children).
    buffer_table = (ctypes.c_void_p * 3)(0, 0, 0)
    arr = ArrowArray(
        length=0,
        null_count=0,
        offset=0,
        n_buffers=3,
        n_children=1,
        buffers=ctypes.cast(buffer_table, ctypes.c_void_p),
        children=ctypes.cast(children_table, ctypes.c_void_p),
        dictionary=None,
        release=release_fn,
        private_data=None,
    )
    schema_cap = PyCapsule_New(ctypes.pointer(root), b'arrow_schema', None)
    array_cap = PyCapsule_New(ctypes.pointer(arr), b'arrow_array', None)
    with pytest.raises((
        TypeError,
        gm.ParseError,
        gm.GeometryError,
        ValueError,
    )) as exc2:
        gm.from_arrow(_ArrayCapsuleOnly(schema_cap, array_cap))
    assert type(exc2.value).__name__ != 'PanicException'


def test_empty_stream_preserves_geometry_field_crs_epoch() -> None:
    """Empty stream CRS/epoch come from the geometry field, not the struct root."""
    meta = (
        gm
        .GeometryArray([gm.Point(0, 0, crs=4326)], epoch=2020.0)
        .to_arrow()
        .type.__arrow_ext_serialize__()
    )
    field = pa.field(
        'geometry',
        pa.binary(),
        metadata={
            b'ARROW:extension:name': b'geoarrow.wkb',
            b'ARROW:extension:metadata': meta,
        },
    )
    table = pa.Table.from_batches([], schema=pa.schema([field]))
    empty = gm.from_arrow(_StreamOnly(table))
    assert len(empty) == 0
    assert empty.crs is not None
    assert empty.crs.to_authority() == ('EPSG', '4326')
    assert empty.epoch == 2020.0


def test_stream_table_with_large_list_sibling_imports() -> None:
    """Unrelated large_list sibling must not block geometry column import."""
    if not hasattr(pa, 'large_list'):
        pytest.skip('pyarrow large_list unavailable')
    pts = [gm.Point(0, 0).to_wkb(), gm.Point(1, 1).to_wkb()]
    table = pa.table({
        'geometry': pa.array(pts, type=pa.binary()),
        'extra': pa.array([[1], [2]], type=pa.large_list(pa.int32())),
    })
    direct = gm.from_arrow(table)
    stream = gm.from_arrow(_StreamOnly(table))
    assert direct.to_wkt() == stream.to_wkt() == ['POINT (0 0)', 'POINT (1 1)']


def test_stream_wrapper_nonempty_wkb_table_imports() -> None:
    """Valid non-empty one-column WKB table must import through stream-only wrapper."""
    pts = [gm.Point(i, i).to_wkb() for i in range(3)]
    table = pa.table({'geometry': pa.array(pts, type=pa.binary())})
    assert gm.from_arrow(table).to_wkt() == gm.from_arrow(_StreamOnly(table)).to_wkt()


def test_zero_chunk_tagged_int64_geoarrow_rejected() -> None:
    """geoarrow.linestring with int64 storage must reject empty and non-empty."""

    class _Ext(pa.ExtensionType):
        def __arrow_ext_serialize__(self) -> bytes:
            return b'{}'

        @classmethod
        def __arrow_ext_deserialize__(cls, storage_type, serialized):
            return cls(storage_type)

    ext = _Ext(pa.int64(), 'geoarrow.linestring')
    with pytest.raises((TypeError, gm.ParseError, ValueError, AttributeError)):
        gm.from_arrow(pa.chunked_array([], type=ext))
    with pytest.raises((TypeError, gm.ParseError, ValueError, AttributeError)):
        gm.from_arrow(pa.array([1, 2, 3], type=ext))


def test_lying_len_with_missing_raises_cleanly() -> None:
    """``_with_missing`` must not allocator-abort on a lying ``__len__`` mask."""
    arr = gm.points([0.0, 1.0], [0.0, 1.0])
    lying = _LyingLenSequence([True, False], reported=(1 << 62))
    # Length is captured from the mask once and compared to the array — never
    # used as ``Vec::with_capacity`` via FromPyObject.
    with pytest.raises((ValueError, MemoryError, TypeError, OverflowError)):
        arr._with_missing(lying)


def test_lying_len_fancy_index_raises_cleanly() -> None:
    """Fancy index must not abort when classification/extraction lengths diverge."""
    arr = gm.points([0.0, 1.0, 2.0], [0.0, 0.0, 0.0])
    # Report maxsize but only one getitem works. One-pass collect uses try_iter
    # (getitem until IndexError) or rejects absurd len — never FromPyObject abort.
    lying = _LyingLenSequence([0], reported=(1 << 62))
    try:
        out = arr[lying]
    except (ValueError, MemoryError, TypeError, OverflowError, IndexError):
        return
    assert len(out) == 1
    assert out[0].to_wkt() == 'POINT (0 0)'


def test_lying_len_mixed_pickle_rows_raises_cleanly() -> None:
    """Mixed unpickler must not abort on a lying outer sequence of rows."""
    wkb = gm.Point(1, 2).to_wkb()
    lying = _LyingLenSequence([wkb], reported=(1 << 62))
    try:
        out = _lib._unpickle_geometry_array(lying, None, None, None)
    except (ValueError, MemoryError, TypeError, OverflowError):
        out = None
    if out is not None:
        # Safe success: iterator path ignored lying ``__len__``.
        assert len(out) == 1
        assert out[0].to_wkt() == 'POINT (1 2)'
    # Positive control: concrete list of bytes still works.
    ok = _lib._unpickle_geometry_array([wkb], None, None, None)
    assert len(ok) == 1
    assert ok[0].to_wkt() == 'POINT (1 2)'


# ---------------------------------------------------------------------------
# R01/R02: reservation keystone on ordinary sequences / iterators
# (lying collections.abc.Sequence.__len__ + unbounded-iterator bounds)
# ---------------------------------------------------------------------------


def _assert_no_capacity_panic(exc: BaseException) -> None:
    assert type(exc).__name__ != 'PanicException', f'Rust panic: {exc!r}'
    assert 'capacity overflow' not in str(exc).lower(), f'capacity panic: {exc!r}'


def test_r01_lying_len_coordinate_lanes_no_panic() -> None:
    """R01: coordinate/string lanes must not PanicException on lying ``__len__``.

    Generic ``Vec<T>: FromPyObject`` allocated from ``__len__`` before any
    length check. Real short payloads succeed (iterator walks until IndexError)
    or raise a typed error — never capacity overflow.
    """
    xs = _LyingLenSequence([0.0, 1.0])
    ys = [0.0, 1.0]
    pts = gm.points(xs, ys)
    assert len(pts) == 2
    assert pts[0].to_wkt() == 'POINT (0 0)'
    assert pts[1].to_wkt() == 'POINT (1 1)'

    moved = gm.Point(0, 0).set_coordinates(x=_LyingLenSequence([1.0]), y=[2.0])
    assert moved.to_wkt() == 'POINT (1 2)'

    out = gm.crs_transform(4326, 3857, xs, ys)
    assert len(out) == 2

    cells = gm.h3_cells(xs, ys, resolution=1)
    assert len(cells) == 2

    polys = gm.pluscode_polygon(_LyingLenSequence(['8FVC9G8F+6X']))
    assert len(polys) == 1
    assert polys[0].geometry_type == 'Polygon'


def test_r01_lying_len_rejects_without_panic_on_type_mismatch() -> None:
    """Lying-len sequence of wrong element type still raises typed, not panic."""
    bad = _LyingLenSequence(['not-a-float', 'still-not'])
    with pytest.raises((
        TypeError,
        ValueError,
        gm.GeometryError,
        MemoryError,
    )) as exc_info:
        gm.points(bad, [0.0, 1.0])
    _assert_no_capacity_panic(exc_info.value)


def test_r02_geometry_array_and_from_wkt_fallible_collect() -> None:
    """R02: GeometryArray / from_wkt use fallible collect (no abort on huge hint)."""
    # Honest finite iterators.
    arr = gm.GeometryArray(iter([gm.Point(0, 0), gm.Point(1, 1)]))
    assert len(arr) == 2
    wkt_arr = gm.from_wkt(iter(['POINT (0 0)', 'POINT (1 1)']))
    assert len(wkt_arr) == 2

    # Lying-len Sequence of WKT strings succeeds on real elements.
    lying_wkt = _LyingLenSequence(['POINT (0 0)', 'POINT (1 1)'])
    try:
        out = gm.from_wkt(lying_wkt)
    except (
        TypeError,
        ValueError,
        MemoryError,
        OverflowError,
        gm.GeometryError,
        gm.ParseError,
    ) as exc:
        _assert_no_capacity_panic(exc)
    else:
        assert len(out) == 2

    # Huge empty __len__ hint: clamp + empty result or clean MemoryError.
    class HugeEmpty:
        def __len__(self) -> int:
            return 1 << 62

        def __iter__(self):
            return iter(())

    try:
        empty = gm.GeometryArray(HugeEmpty())
    except (MemoryError, ValueError, OverflowError, TypeError) as exc:
        _assert_no_capacity_panic(exc)
    else:
        assert len(empty) == 0


def test_r02_dissolve_stops_after_expected_plus_one() -> None:
    """R02: dissolve keys stop after expected+1 (unbounded iterators bound)."""
    import itertools

    arr = gm.GeometryArray([gm.Point(0, 0), gm.Point(1, 1)])
    geoms, keys = arr.dissolve(by=[0, 0])
    assert keys == [0]
    assert len(geoms) == 1

    with pytest.raises(gm.GeometryError, match='one key per geometry'):
        arr.dissolve(by=itertools.repeat(0))

    with pytest.raises(gm.GeometryError, match='one key per geometry'):
        arr.dissolve(by=['a'])


# ---------------------------------------------------------------------------
# P0 gate: shared geoarrow ordinate classifier + WKB multi empty/axes/budget
# (2026-07 release blockers — revert-sensitive)
# ---------------------------------------------------------------------------


class _ArrayOnly:
    """Force the Arrow-C array capsule path (no .type / pyarrow dispatch)."""

    __slots__ = ('_obj',)

    def __init__(self, obj: object) -> None:
        self._obj = obj

    def __arrow_c_array__(self, requested_schema=None):
        return self._obj.__arrow_c_array__(requested_schema)


def _geoarrow_point_field_array(struct_array: pa.StructArray) -> pa.Table:
    field = pa.field(
        'geometry',
        struct_array.type,
        metadata={b'ARROW:extension:name': b'geoarrow.point'},
    )
    return pa.Table.from_arrays([struct_array], schema=pa.schema([field]))


def _geoarrow_point_extension(struct_array: pa.StructArray) -> pa.ExtensionArray:
    from gometry._arrow import GEOARROW_POINT, _extension_type_from_storage

    ext = _extension_type_from_storage(
        pa, GEOARROW_POINT, struct_array.type, None, None
    )
    return pa.ExtensionArray.from_storage(ext, struct_array)


@pytest.mark.parametrize('frontend', ['direct', 'stream', 'array_capsule'])
@pytest.mark.parametrize('empty', [False, True])
@pytest.mark.parametrize('via', ['extension', 'field_metadata'])
def test_geoarrow_int64_point_storage_rejected_every_frontend(
    frontend: str, empty: bool, via: str
) -> None:
    """int64 geoarrow.point must never reinterpret bytes as f64.

    Exact kill: struct<x:int64,y:int64> row (1,2) must REJECT — not import as
    ~(5e-324, 1e-323).
    """
    if empty:
        typ = pa.struct([('x', pa.int64()), ('y', pa.int64())])
        storage = pa.array([], type=typ)
    else:
        storage = pa.StructArray.from_arrays(
            [pa.array([1], type=pa.int64()), pa.array([2], type=pa.int64())],
            names=['x', 'y'],
        )
    if via == 'extension':
        try:
            arrow = _geoarrow_point_extension(storage)
        except Exception:
            # ExtensionType construction may reject non-float storage types.
            return
        table = pa.table({'geometry': arrow})
    else:
        table = _geoarrow_point_field_array(storage)

    if frontend == 'direct':
        target: object = table if via == 'field_metadata' else table.column(0)
    elif frontend == 'stream':
        target = _StreamOnly(table)
    else:
        # Direct capsule on the extension/column array when available.
        col = table.column(0)
        if hasattr(col, 'combine_chunks'):
            col = col.combine_chunks()
        if not hasattr(col, '__arrow_c_array__'):
            pytest.skip('no __arrow_c_array__ on column')
        target = _ArrayOnly(col)

    with pytest.raises((TypeError, gm.ParseError, ValueError, AttributeError)):
        gm.from_arrow(target)


@pytest.mark.parametrize('frontend', ['direct', 'stream', 'array_capsule'])
@pytest.mark.parametrize('empty', [False, True])
def test_geoarrow_duplicate_x_point_storage_rejected_every_frontend(
    frontend: str, empty: bool
) -> None:
    """Duplicate ``x`` field must reject — never silently drop the second x.

    Exact kill: struct<x:f64,x:f64,y:f64> row (1,999,2) must not become POINT (1 2).
    """
    if empty:
        typ = pa.struct([('x', pa.float64()), ('x', pa.float64()), ('y', pa.float64())])
        storage = pa.array([], type=typ)
    else:
        storage = pa.StructArray.from_arrays(
            [pa.array([1.0]), pa.array([999.0]), pa.array([2.0])],
            names=['x', 'x', 'y'],
        )
    # Field-metadata path (ExtensionType may collapse duplicate names).
    table = _geoarrow_point_field_array(storage)

    if frontend == 'direct':
        target: object = table
    elif frontend == 'stream':
        target = _StreamOnly(table)
    else:
        col = table.column(0)
        if hasattr(col, 'combine_chunks'):
            col = col.combine_chunks()
        if not hasattr(col, '__arrow_c_array__'):
            pytest.skip('no __arrow_c_array__ on column')
        target = _ArrayOnly(col)

    with pytest.raises((
        TypeError,
        gm.ParseError,
        ValueError,
        KeyError,
        AttributeError,
    )):
        gm.from_arrow(target)


@pytest.mark.parametrize('frontend', ['direct', 'stream'])
@pytest.mark.parametrize('empty', [False, True])
def test_unrecognized_extension_name_errors_every_frontend(
    frontend: str, empty: bool
) -> None:
    """If an extension NAME is present it must classify or ERROR — never fall through."""
    field = pa.field(
        'geometry',
        pa.binary(),
        metadata={b'ARROW:extension:name': b'not.geometry'},
    )
    if empty:
        table = pa.Table.from_batches([], schema=pa.schema([field]))
    else:
        wkb = gm.Point(1, 2).to_wkb()
        table = pa.Table.from_arrays(
            [pa.array([wkb], type=pa.binary())], schema=pa.schema([field])
        )
    target: object = table if frontend == 'direct' else _StreamOnly(table)
    with pytest.raises(
        (TypeError, gm.ParseError, ValueError), match=r'geoarrow|extension'
    ):
        gm.from_arrow(target)


@pytest.mark.parametrize('frontend', ['direct', 'stream'])
@pytest.mark.parametrize('empty', [False, True])
def test_junk_extension_metadata_without_name_imports_as_bare_wkb(
    frontend: str, empty: bool
) -> None:
    """Bare binary WKB with junk ext-metadata but no name imports everywhere."""
    field = pa.field(
        'geometry',
        pa.binary(),
        metadata={b'ARROW:extension:metadata': b'not-json'},
    )
    if empty:
        table = pa.Table.from_batches([], schema=pa.schema([field]))
        target: object = table if frontend == 'direct' else _StreamOnly(table)
        out = gm.from_arrow(target)
        assert len(out) == 0
        return
    wkb = gm.Point(1, 2).to_wkb()
    table = pa.Table.from_arrays(
        [pa.array([wkb], type=pa.binary())], schema=pa.schema([field])
    )
    target = table if frontend == 'direct' else _StreamOnly(table)
    assert gm.from_arrow(target).to_wkt() == ['POINT (1 2)']


def test_wkb_empty_multipoint_member_matches_wkt() -> None:
    """Matching typed empty MultiPoint members normalize like WKT.

    Exact LE payload: MULTIPOINT with NaN empty Point then POINT (1 2).
    """
    nan = float('nan')
    wkb = (
        struct.pack('<BII', 1, 4, 2)
        + struct.pack('<BI2d', 1, 1, nan, nan)
        + struct.pack('<BI2d', 1, 1, 1.0, 2.0)
    )
    wkb_geom = gm.from_wkb(wkb)
    wkt_geom = gm.from_wkt('MULTIPOINT (EMPTY, (1 2))')
    assert wkb_geom.to_wkt() == wkt_geom.to_wkt() == 'MULTIPOINT ((1 2))'
    assert len(wkb_geom.coords) == 1
    # bulk / Arrow-WKB / pickle
    bulk = gm.from_wkb([wkb])
    assert bulk[0].to_wkt() == 'MULTIPOINT ((1 2))'
    arrow = gm.from_arrow(pa.array([wkb], type=pa.binary()))
    assert arrow[0].to_wkt() == 'MULTIPOINT ((1 2))'
    assert pickle.loads(pickle.dumps(wkb_geom)).to_wkt() == 'MULTIPOINT ((1 2))'


def test_wkb_empty_multipolygon_member_matches_wkt() -> None:
    """Matching typed empty MultiPolygon members normalize like WKT."""
    empty_poly = struct.pack('<BI', 1, 3) + struct.pack('<I', 0)
    shell = (
        struct.pack('<BI', 1, 3)
        + struct.pack('<I', 1)
        + struct.pack('<I', 5)
        + struct.pack('<10d', 0.0, 0.0, 1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 0.0, 0.0)
    )
    wkb = struct.pack('<BII', 1, 6, 2) + empty_poly + shell
    wkb_geom = gm.from_wkb(wkb)
    wkt_geom = gm.from_wkt('MULTIPOLYGON (EMPTY, ((0 0, 1 0, 1 1, 0 1, 0 0)))')
    assert wkb_geom.to_wkt() == wkt_geom.to_wkt()
    assert len(wkb_geom.parts) == 1
    assert pickle.loads(pickle.dumps(wkb_geom)).to_wkt() == wkb_geom.to_wkt()


@pytest.mark.parametrize(
    'wkb',
    [
        # MULTIPOINT Z containing XY Point
        struct.pack('<BII', 1, 1004, 1) + struct.pack('<BI2d', 1, 1, 1.0, 2.0),
        # MULTIPOINT (XY) containing Point Z
        struct.pack('<BII', 1, 4, 1) + struct.pack('<BI3d', 1, 1001, 1.0, 2.0, 3.0),
    ],
    ids=['outer_z_member_xy', 'outer_xy_member_z'],
)
def test_wkb_homogeneous_outer_member_axes_must_agree(wkb: bytes) -> None:
    """Outer/member axes equality for MultiPoint — no silent promote/demote."""
    with pytest.raises(gm.ParseError, match=r'axes|member'):
        gm.from_wkb(wkb)


def test_geoarrow_nested_polygon_int64_coords_rejected() -> None:
    """Nested polygon geoarrow with int64 ordinate leaves must reject."""
    from gometry._arrow import GEOARROW_POLYGON, _extension_type_from_storage

    coords = pa.StructArray.from_arrays(
        [
            pa.array([0, 1, 1, 0, 0], type=pa.int64()),
            pa.array([0, 0, 1, 1, 0], type=pa.int64()),
        ],
        names=['x', 'y'],
    )
    ring_offsets = pa.array([0, 5], type=pa.int32())
    rings = pa.ListArray.from_arrays(ring_offsets, coords)
    poly_offsets = pa.array([0, 1], type=pa.int32())
    polys = pa.ListArray.from_arrays(poly_offsets, rings)
    try:
        ext = _extension_type_from_storage(pa, GEOARROW_POLYGON, polys.type, None, None)
        arrow = pa.ExtensionArray.from_storage(ext, polys)
    except Exception:
        # Construction rejected — also a pass for this invariant.
        return
    with pytest.raises((TypeError, gm.ParseError, ValueError, AttributeError)):
        gm.from_arrow(arrow)
    with pytest.raises((TypeError, gm.ParseError, ValueError, AttributeError)):
        gm.from_arrow(_StreamOnly(pa.table({'geometry': arrow})))


# ---------------------------------------------------------------------------
# F4 — GeoArrow polygon ring closure on all active ordinates
# ---------------------------------------------------------------------------


def _geoarrow_polygon_array(
    points: list[tuple[float, ...]],
    *,
    axes: str = 'xyz',
) -> pa.Array:
    """Build a single-row geoarrow.polygon ListArray over a coordinate struct."""
    if axes == 'xyz':
        coord_type = pa.struct([
            ('x', pa.float64()),
            ('y', pa.float64()),
            ('z', pa.float64()),
        ])
    elif axes == 'xym':
        coord_type = pa.struct([
            ('x', pa.float64()),
            ('y', pa.float64()),
            ('m', pa.float64()),
        ])
    elif axes == 'xyzm':
        coord_type = pa.struct([
            ('x', pa.float64()),
            ('y', pa.float64()),
            ('z', pa.float64()),
            ('m', pa.float64()),
        ])
    else:
        coord_type = pa.struct([('x', pa.float64()), ('y', pa.float64())])
    pts = pa.array(points, type=coord_type)
    rings = pa.ListArray.from_arrays(pa.array([0, len(points)], type=pa.int32()), pts)
    polys = pa.ListArray.from_arrays(pa.array([0, 1], type=pa.int32()), rings)
    field = pa.field(
        'geometry',
        polys.type,
        metadata={
            b'ARROW:extension:name': b'geoarrow.polygon',
            b'ARROW:extension:metadata': b'{}',
        },
    )
    return pa.RecordBatch.from_arrays([polys], schema=pa.schema([field]))


@pytest.mark.parametrize(
    ('points', 'axes'),
    [
        # Z-unclosed: XY closes (1 vs 9 on Z).
        (
            [(0.0, 0.0, 1.0), (1.0, 0.0, 2.0), (0.0, 1.0, 3.0), (0.0, 0.0, 9.0)],
            'xyz',
        ),
        # M-unclosed.
        (
            [(0.0, 0.0, 1.0), (1.0, 0.0, 2.0), (0.0, 1.0, 3.0), (0.0, 0.0, 9.0)],
            'xym',
        ),
        # ZM-unclosed on Z.
        (
            [
                (0.0, 0.0, 1.0, 4.0),
                (1.0, 0.0, 2.0, 4.0),
                (0.0, 1.0, 3.0, 4.0),
                (0.0, 0.0, 9.0, 4.0),
            ],
            'xyzm',
        ),
        # ZM-unclosed on M.
        (
            [
                (0.0, 0.0, 1.0, 4.0),
                (1.0, 0.0, 2.0, 4.0),
                (0.0, 1.0, 3.0, 4.0),
                (0.0, 0.0, 1.0, 9.0),
            ],
            'xyzm',
        ),
    ],
)
def test_f4_geoarrow_polygon_rejects_active_ordinate_unclosed_rings(
    points: list[tuple[float, ...]],
    axes: str,
) -> None:
    """F4: Z/M/ZM-open GeoArrow rings must reject at import (D05 sister).

    Packed import used to check XY-only closure, admitting trusted state that
    pickle later rejected via same_active_position.
    """
    batch = _geoarrow_polygon_array(points, axes=axes)
    with pytest.raises(gm.ParseError, match=r'closed|active ordinate|ring'):
        gm.from_arrow(batch)
    # Capsule frontend (native Arrow-C path).
    storage = batch.column(0)
    field = batch.schema.field(0)
    # Re-wrap as ExtensionArray-like via RecordBatch so metadata sticks;
    # capsule-only uses the same field metadata through table export.
    table = pa.Table.from_batches([batch])
    with pytest.raises(gm.ParseError, match=r'closed|active ordinate|ring'):
        gm.from_arrow(_StreamOnly(table))
    del storage, field


def test_f4_geoarrow_closed_3d_polygon_imports_and_pickles() -> None:
    """Genuinely closed 3D GeoArrow polygon still imports and pickle-round-trips."""
    closed = [
        (0.0, 0.0, 1.0),
        (1.0, 0.0, 2.0),
        (0.0, 1.0, 3.0),
        (0.0, 0.0, 1.0),
    ]
    arr = gm.from_arrow(_geoarrow_polygon_array(closed, axes='xyz'))
    assert arr.to_wkt() == ['POLYGON Z ((0 0 1, 1 0 2, 0 1 3, 0 0 1))']
    restored = pickle.loads(pickle.dumps(arr))
    assert restored.to_wkt() == arr.to_wkt()
    assert list(restored[0].exterior.coords.z) == [1.0, 2.0, 3.0, 1.0]


def test_f4_geoarrow_xy_closed_2d_polygon_unaffected() -> None:
    """XY 2D closed rings still import on the packed path."""
    closed_2d = [
        (0.0, 0.0),
        (1.0, 0.0),
        (0.0, 1.0),
        (0.0, 0.0),
    ]
    arr = gm.from_arrow(_geoarrow_polygon_array(closed_2d, axes='xy'))
    assert arr.to_wkt() == ['POLYGON ((0 0, 1 0, 0 1, 0 0))']
    assert pickle.loads(pickle.dumps(arr)).to_wkt() == arr.to_wkt()


# ---------------------------------------------------------------------------
# F5 — Arrow-C child index bounds-checked against n_children
# ---------------------------------------------------------------------------


def _f5_arrow_c_structs():
    """Ctypes ArrowSchema/ArrowArray/Stream types for F5 harnesses."""
    import ctypes

    class ArrowSchema(ctypes.Structure):
        pass

    ArrowSchema._fields_ = [
        ('format', ctypes.c_char_p),
        ('name', ctypes.c_char_p),
        ('metadata', ctypes.c_void_p),
        ('flags', ctypes.c_int64),
        ('n_children', ctypes.c_int64),
        ('children', ctypes.POINTER(ctypes.POINTER(ArrowSchema))),
        ('dictionary', ctypes.c_void_p),
        ('release', ctypes.c_void_p),
        ('private_data', ctypes.c_void_p),
    ]

    class ArrowArray(ctypes.Structure):
        _fields_ = [
            ('length', ctypes.c_int64),
            ('null_count', ctypes.c_int64),
            ('offset', ctypes.c_int64),
            ('n_buffers', ctypes.c_int64),
            ('n_children', ctypes.c_int64),
            ('buffers', ctypes.c_void_p),
            ('children', ctypes.c_void_p),
            ('dictionary', ctypes.c_void_p),
            ('release', ctypes.c_void_p),
            ('private_data', ctypes.c_void_p),
        ]

    class ArrowArrayStream(ctypes.Structure):
        _fields_ = [
            ('get_schema', ctypes.c_void_p),
            ('get_next', ctypes.c_void_p),
            ('get_last_error', ctypes.c_void_p),
            ('release', ctypes.c_void_p),
            ('private_data', ctypes.c_void_p),
        ]

    return ctypes, ArrowSchema, ArrowArray, ArrowArrayStream


def _f5_build_id_geometry_schema(ctypes, ArrowSchema, release_fn):
    """Struct schema with child0=id, child1=geoarrow.point geometry."""

    def _meta(pairs):
        body = b''
        for key, value in pairs:
            body += struct.pack('<i', len(key)) + key
            body += struct.pack('<i', len(value)) + value
        blob = struct.pack('<i', len(pairs)) + body
        return (ctypes.c_uint8 * len(blob))(*blob)

    child0 = ArrowSchema(
        format=b'l',
        name=b'id',
        metadata=None,
        flags=0,
        n_children=0,
        children=None,
        dictionary=None,
        release=release_fn,
        private_data=None,
    )
    x_schema = ArrowSchema(
        format=b'g',
        name=b'x',
        metadata=None,
        flags=0,
        n_children=0,
        children=None,
        dictionary=None,
        release=release_fn,
        private_data=None,
    )
    y_schema = ArrowSchema(
        format=b'g',
        name=b'y',
        metadata=None,
        flags=0,
        n_children=0,
        children=None,
        dictionary=None,
        release=release_fn,
        private_data=None,
    )
    xy_ptrs = (ctypes.POINTER(ArrowSchema) * 2)(
        ctypes.pointer(x_schema), ctypes.pointer(y_schema)
    )
    geom_meta = _meta([
        (b'ARROW:extension:name', b'geoarrow.point'),
        (b'ARROW:extension:metadata', b'{}'),
    ])
    child1 = ArrowSchema(
        format=b'+s',
        name=b'geometry',
        metadata=ctypes.cast(geom_meta, ctypes.c_void_p),
        flags=0,
        n_children=2,
        children=ctypes.cast(xy_ptrs, ctypes.POINTER(ctypes.POINTER(ArrowSchema))),
        dictionary=None,
        release=release_fn,
        private_data=None,
    )
    schema_children = (ctypes.POINTER(ArrowSchema) * 2)(
        ctypes.pointer(child0), ctypes.pointer(child1)
    )
    root_schema = ArrowSchema(
        format=b'+s',
        name=b'',
        metadata=None,
        flags=0,
        n_children=2,
        children=ctypes.cast(
            schema_children, ctypes.POINTER(ctypes.POINTER(ArrowSchema))
        ),
        dictionary=None,
        release=release_fn,
        private_data=None,
    )
    keep = (
        geom_meta,
        x_schema,
        y_schema,
        xy_ptrs,
        child0,
        child1,
        schema_children,
        root_schema,
    )
    return root_schema, keep


@pytest.mark.parametrize(
    ('status', 'expected'),
    [(12, MemoryError), (5, OSError), (22, OSError)],
    ids=['enomem', 'eio', 'errno_fallback'],
)
def test_arrow_c_stream_callback_errno_maps_to_python_error(
    status: int, expected: type[Exception]
) -> None:
    """Callback return codes remain observable; no failure becomes TypeError."""
    ctypes, _ArrowSchema, _ArrowArray, ArrowArrayStream = _f5_arrow_c_structs()

    @ctypes.CFUNCTYPE(None, ctypes.c_void_p)
    def release(_ptr: int) -> None:
        pass

    @ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_void_p, ctypes.c_void_p)
    def get_schema(_stream: int, _out_schema: int) -> int:
        return status

    @ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_void_p, ctypes.c_void_p)
    def get_next(_stream: int, _out_array: int) -> int:
        return 0

    error_detail = ctypes.create_string_buffer(b'producer callback failed')

    @ctypes.CFUNCTYPE(ctypes.c_void_p, ctypes.c_void_p)
    def get_last_error(_stream: int) -> int:
        return ctypes.addressof(error_detail)

    stream = ArrowArrayStream(
        ctypes.cast(get_schema, ctypes.c_void_p),
        ctypes.cast(get_next, ctypes.c_void_p),
        ctypes.cast(get_last_error, ctypes.c_void_p),
        ctypes.cast(release, ctypes.c_void_p),
        None,
    )
    PyCapsule_New = ctypes.pythonapi.PyCapsule_New
    PyCapsule_New.restype = ctypes.py_object
    PyCapsule_New.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_void_p]
    capsule = PyCapsule_New(ctypes.pointer(stream), b'arrow_array_stream', None)

    class _ErrorStream:
        def __arrow_c_stream__(self, requested_schema=None):
            del requested_schema
            return capsule

    _ErrorStream._keep = (  # type: ignore[attr-defined]
        stream,
        get_schema,
        get_next,
        get_last_error,
        release,
        error_detail,
        capsule,
    )
    with pytest.raises(expected, match='producer callback failed') as error:
        gm.from_arrow(_ErrorStream())
    if expected is OSError:
        assert error.value.errno == status


def test_f5_arrow_c_child_index_out_of_range_rejects() -> None:
    """F5: schema geometry child index >= array.n_children must ParseError.

    Stream zero-row path: schema has geometry at index 1, batch declares
    n_children=1 — children.add(1) was OOB before the bounds gate. Point
    zero-row validation returned immediately after the bad add.
    """
    ctypes, ArrowSchema, ArrowArray, ArrowArrayStream = _f5_arrow_c_structs()

    @ctypes.CFUNCTYPE(None, ctypes.c_void_p)
    def _noop_release(_ptr: int) -> None:
        pass

    release_fn = ctypes.cast(_noop_release, ctypes.c_void_p)
    root_schema, schema_keep = _f5_build_id_geometry_schema(
        ctypes, ArrowSchema, release_fn
    )

    id_child_arr = ArrowArray(
        length=0,
        null_count=0,
        offset=0,
        n_buffers=2,
        n_children=0,
        buffers=None,
        children=None,
        dictionary=None,
        release=release_fn,
        private_data=None,
    )
    only_one = (ctypes.POINTER(ArrowArray) * 1)(ctypes.pointer(id_child_arr))

    stream = ArrowArrayStream()
    state = {'done': False}

    @ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_void_p, ctypes.c_void_p)
    def get_schema(_stream: int, out_schema: int) -> int:
        out = ArrowSchema.from_address(out_schema)
        out.format = root_schema.format
        out.name = root_schema.name
        out.metadata = root_schema.metadata
        out.flags = root_schema.flags
        out.n_children = root_schema.n_children
        out.children = root_schema.children
        out.dictionary = root_schema.dictionary
        out.release = release_fn
        out.private_data = None
        return 0

    @ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_void_p, ctypes.c_void_p)
    def get_next(_stream: int, out_array: int) -> int:
        out = ArrowArray.from_address(out_array)
        if state['done']:
            out.length = 0
            out.null_count = 0
            out.offset = 0
            out.n_buffers = 0
            out.n_children = 0
            out.buffers = None
            out.children = None
            out.dictionary = None
            out.release = None
            out.private_data = None
            return 0
        state['done'] = True
        # Zero-row batch: n_children=1 while schema geometry is at index 1.
        out.length = 0
        out.null_count = 0
        out.offset = 0
        out.n_buffers = 1
        out.n_children = 1
        out.buffers = None
        out.children = ctypes.cast(only_one, ctypes.c_void_p)
        out.dictionary = None
        out.release = release_fn
        out.private_data = None
        return 0

    @ctypes.CFUNCTYPE(ctypes.c_char_p, ctypes.c_void_p)
    def get_last_error(_stream: int) -> None:
        return None

    stream.get_schema = ctypes.cast(get_schema, ctypes.c_void_p)
    stream.get_next = ctypes.cast(get_next, ctypes.c_void_p)
    stream.get_last_error = ctypes.cast(get_last_error, ctypes.c_void_p)
    stream.release = release_fn
    stream.private_data = None
    stream._keep = (  # type: ignore[attr-defined]
        get_schema,
        get_next,
        get_last_error,
        _noop_release,
        schema_keep,
        id_child_arr,
        only_one,
        state,
    )

    PyCapsule_New = ctypes.pythonapi.PyCapsule_New
    PyCapsule_New.restype = ctypes.py_object
    PyCapsule_New.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_void_p]
    stream_cap = PyCapsule_New(ctypes.pointer(stream), b'arrow_array_stream', None)

    class _StreamCapsuleOnly:
        def __arrow_c_stream__(self, requested_schema=None):
            return stream_cap

    with pytest.raises(
        gm.ParseError, match=r'child index|n_children|out of range'
    ) as exc:
        gm.from_arrow(_StreamCapsuleOnly())
    assert type(exc.value).__name__ != 'PanicException'
    assert exc.value.format == 'geoarrow'

    # Positive: conforming multi-child table still imports (array + stream).
    ok = gm.GeometryArray([gm.Point(1.0, 2.0)])
    table = pa.table({
        'id': pa.array([1], type=pa.int64()),
        'geometry': ok.to_arrow(),
    })
    batch = table.to_batches()[0]
    restored = gm.from_arrow(_CapsuleOnly(*batch.__arrow_c_array__()))
    assert restored.to_wkt() == ['POINT (1 2)']
    restored_s = gm.from_arrow(_StreamOnly(table))
    assert restored_s.to_wkt() == ['POINT (1 2)']


def test_f5_arrow_c_null_child_pointer_rejects() -> None:
    """F5: null child pointer at a valid index must ParseError on stream path."""
    ctypes, ArrowSchema, ArrowArray, ArrowArrayStream = _f5_arrow_c_structs()

    @ctypes.CFUNCTYPE(None, ctypes.c_void_p)
    def _noop_release(_ptr: int) -> None:
        pass

    release_fn = ctypes.cast(_noop_release, ctypes.c_void_p)
    root_schema, schema_keep = _f5_build_id_geometry_schema(
        ctypes, ArrowSchema, release_fn
    )

    id_arr = ArrowArray(
        length=0,
        null_count=0,
        offset=0,
        n_buffers=2,
        n_children=0,
        buffers=None,
        children=None,
        dictionary=None,
        release=release_fn,
        private_data=None,
    )
    # n_children=2 matches schema; child[1] (geometry) is NULL.
    child_table = (ctypes.POINTER(ArrowArray) * 2)(ctypes.pointer(id_arr), None)

    stream = ArrowArrayStream()
    state = {'done': False}

    @ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_void_p, ctypes.c_void_p)
    def get_schema(_stream: int, out_schema: int) -> int:
        out = ArrowSchema.from_address(out_schema)
        out.format = root_schema.format
        out.name = root_schema.name
        out.metadata = root_schema.metadata
        out.flags = root_schema.flags
        out.n_children = root_schema.n_children
        out.children = root_schema.children
        out.dictionary = root_schema.dictionary
        out.release = release_fn
        out.private_data = None
        return 0

    @ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_void_p, ctypes.c_void_p)
    def get_next(_stream: int, out_array: int) -> int:
        out = ArrowArray.from_address(out_array)
        if state['done']:
            out.length = 0
            out.null_count = 0
            out.offset = 0
            out.n_buffers = 0
            out.n_children = 0
            out.buffers = None
            out.children = None
            out.dictionary = None
            out.release = None
            out.private_data = None
            return 0
        state['done'] = True
        out.length = 0
        out.null_count = 0
        out.offset = 0
        out.n_buffers = 1
        out.n_children = 2
        out.buffers = None
        out.children = ctypes.cast(child_table, ctypes.c_void_p)
        out.dictionary = None
        out.release = release_fn
        out.private_data = None
        return 0

    @ctypes.CFUNCTYPE(ctypes.c_char_p, ctypes.c_void_p)
    def get_last_error(_stream: int) -> None:
        return None

    stream.get_schema = ctypes.cast(get_schema, ctypes.c_void_p)
    stream.get_next = ctypes.cast(get_next, ctypes.c_void_p)
    stream.get_last_error = ctypes.cast(get_last_error, ctypes.c_void_p)
    stream.release = release_fn
    stream.private_data = None
    stream._keep = (  # type: ignore[attr-defined]
        get_schema,
        get_next,
        get_last_error,
        _noop_release,
        schema_keep,
        id_arr,
        child_table,
        state,
    )

    PyCapsule_New = ctypes.pythonapi.PyCapsule_New
    PyCapsule_New.restype = ctypes.py_object
    PyCapsule_New.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_void_p]
    stream_cap = PyCapsule_New(ctypes.pointer(stream), b'arrow_array_stream', None)

    class _StreamCapsuleOnly:
        def __arrow_c_stream__(self, requested_schema=None):
            return stream_cap

    with pytest.raises(gm.ParseError, match=r'child.*null|null.*child') as exc:
        gm.from_arrow(_StreamCapsuleOnly())
    assert type(exc.value).__name__ != 'PanicException'
    assert exc.value.format == 'geoarrow'
