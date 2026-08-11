"""R14-G consolidation: GeoArrow ring admission, multipart axes, LargeList /
interleaved FixedSizeList, and related I/O stop-and-report guards.

Deterministic fixtures only — no timing, no generative tests.
"""

from __future__ import annotations

import gometry as gm
import pytest

pa = pytest.importorskip('pyarrow')


class _CapsuleArrayOnly:
    """Force the native Arrow-C array importer rather than PyArrow dispatch."""

    def __init__(self, schema: object, array: object) -> None:
        self._capsules = schema, array

    def __arrow_c_array__(self, requested_schema: object | None = None) -> object:
        return self._capsules


class _CapsuleStreamOnly:
    """Force the native Arrow-C stream importer rather than PyArrow dispatch."""

    def __init__(self, table: object) -> None:
        self._table = table

    def __arrow_c_stream__(self, requested_schema: object | None = None) -> object:
        return self._table.__arrow_c_stream__(requested_schema)


def _geoarrow_polygon_table(
    rings: list[list[tuple[float, ...]]], *, with_z: bool = False
):
    """Build a one-row geoarrow.polygon RecordBatch from ring vertex lists."""
    xs: list[float] = []
    ys: list[float] = []
    zs: list[float] = []
    ring_offs = [0]
    for ring in rings:
        for pt in ring:
            xs.append(float(pt[0]))
            ys.append(float(pt[1]))
            if with_z:
                zs.append(float(pt[2]))
        ring_offs.append(len(xs))
    poly_offs = [0, len(ring_offs) - 1]
    names = ['x', 'y']
    arrs: list[pa.Array] = [pa.array(xs), pa.array(ys)]
    if with_z:
        names.append('z')
        arrs.append(pa.array(zs))
    coords = pa.StructArray.from_arrays(arrs, names=names)
    rings_a = pa.ListArray.from_arrays(pa.array(ring_offs, type=pa.int32()), coords)
    polys = pa.ListArray.from_arrays(pa.array(poly_offs, type=pa.int32()), rings_a)
    field = pa.field(
        'geometry',
        polys.type,
        metadata={
            b'ARROW:extension:name': b'geoarrow.polygon',
            b'ARROW:extension:metadata': b'{}',
        },
    )
    return pa.RecordBatch.from_arrays([polys], schema=pa.schema([field]))


# ---------------------------------------------------------------------------
# G1 — GeoArrow ring admission == admit_closed_ring (WKT/WKB/pickle)
# ---------------------------------------------------------------------------


def test_g1_from_arrow_admits_3_corner_open_like_wkt() -> None:
    """XY-open with ≥3 corners: silent-close on Arrow, WKT, WKB."""
    open_wkt = 'POLYGON ((0 0, 1 0, 0 1))'
    expected = 'POLYGON ((0 0, 1 0, 0 1, 0 0))'
    assert gm.from_wkt(open_wkt).to_wkt() == expected
    wkb = gm.from_wkt(expected).to_wkb()
    # Re-parse open via WKB of closed form is closed; open form via arrow:
    arr = gm.from_arrow(_geoarrow_polygon_table([[(0.0, 0.0), (1.0, 0.0), (0.0, 1.0)]]))
    assert arr.to_wkt() == [expected]
    assert gm.from_wkb(wkb).to_wkt() == expected


def test_g1_from_arrow_admits_4_corner_unclosed_like_wkt() -> None:
    """4-corner XY-open ring silent-closes on all four ingresses."""
    open_wkt = 'POLYGON ((0 0, 1 0, 1 1, 0 1))'
    expected = 'POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))'
    assert gm.from_wkt(open_wkt).to_wkt() == expected
    arr = gm.from_arrow(
        _geoarrow_polygon_table([[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0)]])
    )
    assert arr.to_wkt() == [expected]


def test_g1_from_arrow_rejects_xy_closed_z_open_like_wkt() -> None:
    """XY-closed but Z-open is rejected (never invent closing Z)."""
    wkt = 'POLYGON Z ((0 0 1, 1 0 2, 1 1 3, 0 0 9))'
    with pytest.raises(gm.ParseError, match='closed on all active ordinates'):
        gm.from_wkt(wkt)
    with pytest.raises(
        (gm.ParseError, TypeError, ValueError),
        match='closed on all active ordinates',
    ):
        gm.from_arrow(
            _geoarrow_polygon_table(
                [[(0.0, 0.0, 1.0), (1.0, 0.0, 2.0), (1.0, 1.0, 3.0), (0.0, 0.0, 9.0)]],
                with_z=True,
            )
        )


def test_g1_from_arrow_rejects_short_closed_ring_like_wkt() -> None:
    """Closed 3-coordinate ring (2 corners) rejected like WKT/WKB."""
    with pytest.raises(gm.ParseError, match='at least three coordinates'):
        gm.from_wkt('POLYGON ((0 0, 1 0, 0 0))')
    with pytest.raises(
        (gm.ParseError, TypeError, ValueError),
        match=r'three coordinates|at least',
    ):
        gm.from_arrow(_geoarrow_polygon_table([[(0.0, 0.0), (1.0, 0.0), (0.0, 0.0)]]))


# ---------------------------------------------------------------------------
# G2 — MultiLineString / MultiPolygon construction axes == writers
# ---------------------------------------------------------------------------


def test_g2_multilinestring_mixed_axes_rejected_at_construction() -> None:
    """Heterogeneous XY/XYZ members raise at construct (same message as writers)."""
    ls_xy = gm.LineString([(0, 0), (1, 1)])
    ls_xyz = gm.LineString([(0, 0, 1), (1, 1, 2)])
    with pytest.raises(
        gm.InvalidGeometryError,
        match=r'MultiLineString members must share one coordinate axes',
    ):
        gm.MultiLineString([ls_xy, ls_xyz])
    # Explicit promotion then succeeds and serializes without inventing Z.
    promoted = gm.MultiLineString([ls_xy.force_3d(0.0), ls_xyz])
    assert promoted.coordinate_axes == 'XYZ'
    assert 'LINESTRING Z' in promoted.to_wkt() or promoted.to_wkt().startswith(
        'MULTILINESTRING Z'
    )
    _ = promoted.to_wkb()  # must not raise


def test_g2_multipolygon_mixed_axes_rejected_at_construction() -> None:
    p_xy = gm.Polygon([(0, 0), (1, 0), (0, 1)])
    p_xyz = gm.Polygon([(0, 0, 1), (1, 0, 2), (0, 1, 3), (0, 0, 1)])
    with pytest.raises(
        gm.InvalidGeometryError,
        match=r'MultiPolygon members must share one coordinate axes',
    ):
        gm.MultiPolygon([p_xy, p_xyz])


def test_g2_polygon_mixed_ring_axes_rejected_at_construction() -> None:
    with pytest.raises(
        gm.InvalidGeometryError,
        match=r'Polygon rings must share one coordinate axes',
    ):
        gm.Polygon(
            [(0, 0), (3, 0), (0, 3)],
            holes=[[(1, 1, 5), (2, 1, 5), (1, 2, 5), (1, 1, 5)]],
        )


# ---------------------------------------------------------------------------
# G3 — LargeList + interleaved FixedSizeList (PyArrow path)
# ---------------------------------------------------------------------------


def test_g3_large_list_linestring_decodes() -> None:
    if not hasattr(pa, 'large_list'):
        pytest.skip('pyarrow large_list unavailable')
    xy = pa.struct([('x', pa.float64()), ('y', pa.float64())])
    points = pa.array([(0.0, 0.0), (1.0, 1.0), (2.0, 0.0)], type=xy)
    storage = pa.LargeListArray.from_arrays(pa.array([0, 3], type=pa.int64()), points)
    field = pa.field(
        'geometry',
        storage.type,
        metadata={
            b'ARROW:extension:name': b'geoarrow.linestring',
            b'ARROW:extension:metadata': b'{}',
        },
    )
    arr = gm.from_arrow(
        pa.RecordBatch.from_arrays([storage], schema=pa.schema([field]))
    )
    assert arr.to_wkt() == ['LINESTRING (0 0, 1 1, 2 0)']


def test_g3_interleaved_fixed_size_list_point() -> None:
    """GeoArrow interleaved point: FixedSizeList<float64>[2]."""
    values = pa.array([1.0, 2.0, 3.0, 4.0], type=pa.float64())
    fsl = pa.FixedSizeListArray.from_arrays(values, 2)
    field = pa.field(
        'geometry',
        fsl.type,
        metadata={
            b'ARROW:extension:name': b'geoarrow.point',
            b'ARROW:extension:metadata': b'{}',
        },
    )
    arr = gm.from_arrow(pa.RecordBatch.from_arrays([fsl], schema=pa.schema([field])))
    assert arr.to_wkt() == ['POINT (1 2)', 'POINT (3 4)']


def test_g3_interleaved_fixed_size_list_linestring() -> None:
    """List of FixedSizeList interleaved coordinates for a linestring."""
    flat = pa.array([0.0, 0.0, 1.0, 1.0, 2.0, 0.0], type=pa.float64())
    verts = pa.FixedSizeListArray.from_arrays(flat, 2)
    storage = pa.ListArray.from_arrays(pa.array([0, 3], type=pa.int32()), verts)
    field = pa.field(
        'geometry',
        storage.type,
        metadata={
            b'ARROW:extension:name': b'geoarrow.linestring',
            b'ARROW:extension:metadata': b'{}',
        },
    )
    arr = gm.from_arrow(
        pa.RecordBatch.from_arrays([storage], schema=pa.schema([field]))
    )
    assert arr.to_wkt() == ['LINESTRING (0 0, 1 1, 2 0)']


def test_g3_interleaved_fixed_size_list_linestring_decodes_native_array_and_stream() -> (
    None
):
    """`+w:2` is a schema-directed native child window, never PyArrow-only."""
    flat = pa.array([0.0, 0.0, 1.0, 1.0, 2.0, 0.0], type=pa.float64())
    vertices = pa.FixedSizeListArray.from_arrays(flat, 2)
    storage = pa.ListArray.from_arrays(pa.array([0, 3], type=pa.int32()), vertices)
    field = pa.field(
        'geometry',
        storage.type,
        metadata={
            b'ARROW:extension:name': b'geoarrow.linestring',
            b'ARROW:extension:metadata': b'{}',
        },
    )
    batch = pa.RecordBatch.from_arrays([storage], schema=pa.schema([field]))
    schema, array = batch.__arrow_c_array__()
    native_array = gm.from_arrow(_CapsuleArrayOnly(schema, array))
    assert native_array.to_wkt() == ['LINESTRING (0 0, 1 1, 2 0)']

    # Array capsules are one-shot; the stream gets a fresh producer tree.
    table = pa.Table.from_batches([batch])
    native_stream = gm.from_arrow(_CapsuleStreamOnly(table))
    assert native_stream.to_wkt() == ['LINESTRING (0 0, 1 1, 2 0)']


def test_g3_interleaved_xyz_point() -> None:
    values = pa.array([1.0, 2.0, 3.0, 4.0, 5.0, 6.0], type=pa.float64())
    # size-3 FixedSizeList defaults to XYZ (GeoArrow interleaved convention).
    fsl = pa.FixedSizeListArray.from_arrays(values, 3)
    field = pa.field(
        'geometry',
        fsl.type,
        metadata={
            b'ARROW:extension:name': b'geoarrow.point',
            b'ARROW:extension:metadata': b'{}',
        },
    )
    arr = gm.from_arrow(pa.RecordBatch.from_arrays([fsl], schema=pa.schema([field])))
    assert arr.to_wkt() == ['POINT Z (1 2 3)', 'POINT Z (4 5 6)']


def _interleaved_point_batch(flat: list[float], list_size: int, value_name: str):
    """Named FixedSizeList geoarrow.point batch (field name encodes dimensions)."""
    fsl_type = pa.list_(pa.field(value_name, pa.float64()), list_size)
    fsl = pa.FixedSizeListArray.from_arrays(
        pa.array(flat, type=pa.float64()), type=fsl_type
    )
    field = pa.field(
        'geometry',
        fsl.type,
        metadata={
            b'ARROW:extension:name': b'geoarrow.point',
            b'ARROW:extension:metadata': b'{}',
        },
    )
    return pa.RecordBatch.from_arrays([fsl], schema=pa.schema([field]))


def test_g3_interleaved_size3_rejects_noncanonical_field_name() -> None:
    """Size-3 FixedSizeList field names must be xyz/xym (or default item).

    Regression: the reject arm was gated by ``list_size != 3``, so names like
    'garbage'/'xy'/'xyzm' were silently admitted as XYZ.
    """
    flat = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]
    for bad in ('garbage', 'xy', 'xyzm', 'z'):
        with pytest.raises(
            (TypeError, gm.ParseError, ValueError), match=r'field name|interleaved'
        ):
            gm.from_arrow(_interleaved_point_batch(flat, 3, bad))
    # Canonical size-3 names still admit.
    assert gm.from_arrow(_interleaved_point_batch(flat, 3, 'xyz')).to_wkt() == [
        'POINT Z (1 2 3)',
        'POINT Z (4 5 6)',
    ]
    assert gm.from_arrow(_interleaved_point_batch(flat, 3, 'xym')).to_wkt() == [
        'POINT M (1 2 3)',
        'POINT M (4 5 6)',
    ]
    # Default pyarrow name 'item' still admits as XYZ.
    assert gm.from_arrow(_interleaved_point_batch(flat, 3, 'item')).to_wkt() == [
        'POINT Z (1 2 3)',
        'POINT Z (4 5 6)',
    ]


# ---------------------------------------------------------------------------
# G4 — EMPTY members: model documents drop-at-ingress (not a construction bug)
# ---------------------------------------------------------------------------


def test_g4_wkt_empty_members_drop_not_model_restructure() -> None:
    """EMPTY MultiPoint/MultiPolygon members normalize by dropping (WKB parity).

    Shape::MultiPoint is a CoordSeq (no Shape::Empty slots); preserving empty
    members would require a geometry-model change. Bounded behavior is drop.
    """
    g = gm.from_wkt('MULTIPOINT Z ((1 2 3), EMPTY)')
    assert g.to_wkt() == 'MULTIPOINT Z ((1 2 3))'
    g2 = gm.from_wkt('MULTIPOLYGON (((0 0, 1 0, 1 1, 0 0)), EMPTY)')
    assert g2.to_wkt() == 'MULTIPOLYGON (((0 0, 1 0, 1 1, 0 0)))'
    # Construction does not accept an empty Point as a multipoint member.
    with pytest.raises(TypeError):
        gm.MultiPoint([gm.Point(1, 2), gm.Point()])


# ---------------------------------------------------------------------------
# G5 — include_srid docs already describe CRSError (null if prose accurate)
# ---------------------------------------------------------------------------


def test_g5_include_srid_docs_and_runtime_raise() -> None:
    doc = gm.Point.to_wkb.__doc__ or ''
    assert 'CRSError' in doc
    assert 'include_srid' in doc
    assert 'no EPSG' in doc or 'EPSG-authority' in doc or 'EPSG' in doc
    with pytest.raises(gm.CRSError):
        gm.Point(1, 2).to_wkb(include_srid=True)
    with pytest.raises(gm.CRSError):
        gm.Point(1, 2, crs='ESRI:102003').to_wkb(include_srid=True)


# ---------------------------------------------------------------------------
# G6 — Feature / require: properties discarded by design on geometry return
# ---------------------------------------------------------------------------


def test_g6_require_feature_returns_geometry_only() -> None:
    """gm.require(Feature) returns Geometry; properties are not a projection leak.

    from_features keeps properties; require/from_geojson return geometry only.
    Confirmed null for "parses properties that projection discards" as a bug:
    properties are intentionally out of the Geometry return contract.
    """
    feature = {
        'type': 'Feature',
        'properties': {'name': 'A', 'count': 99},
        'geometry': {'type': 'Point', 'coordinates': [1.0, 2.0]},
        'id': 7,
    }
    g = gm.require(feature, crs=4326)
    assert isinstance(g, gm.Point)
    assert g.to_wkt() == 'POINT (1 2)'
    feats = gm.from_features(feature)
    assert feats.properties == ({'name': 'A', 'count': 99},)
    assert feats.ids == (7,)
