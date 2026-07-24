"""Revert-sensitive regressions for trusted-deserialization + allocation-safety.

Covers audit defects D01, D02, D25-D30, D32 from the systematic ingress matrix.
Each negative test drives the audit's exact repro; each positive test proves
honest pickles/imports still round-trip.
"""

from __future__ import annotations

import pickle

import gometry as gm
import numpy as np
import pytest
from gometry import _lib


class Lying:
    """Sequence that lies about its length (D01)."""

    def __len__(self):
        return 1 << 62

    def __getitem__(self, i):
        raise IndexError


def _assert_not_panic(exc: BaseException) -> None:
    assert type(exc).__name__ != "PanicException", f"Rust panic: {exc}"
    assert "capacity overflow" not in str(exc).lower()


def _call_no_panic(call):
    """Run *call*; allow a clean result or re-raise a non-panic error."""
    try:
        return call(), None
    except BaseException as exc:
        _assert_not_panic(exc)
        return None, exc


# ---------------------------------------------------------------------------
# D01 — lying __len__ must not PanicException
# ---------------------------------------------------------------------------


def test_d01_lying_len_cell_array_unpickle_no_panic():
    # Lying __len__ must not force Vec::with_capacity panic. Iteration via
    # getitem that immediately IndexErrors yields an empty cell array (safe).
    out, err = _call_no_panic(lambda: _lib._unpickle_cell_array(Lying(), "h3"))
    if err is None:
        assert len(out) == 0


def test_d01_lying_len_coverage_and_groups_no_panic():
    calls = [
        lambda: _lib._unpickle_h3_coverage(
            gm.box(0, 0, 1, 1, crs=4326), Lying(), "overlap", 1, None, 1_000_000
        ),
        lambda: _lib._unpickle_s2_coverage(
            gm.box(0, 0, 1, 1, crs=4326),
            Lying(),
            "overlap",
            1,
            1,
            1,
            10000,
            8,
        ),
        lambda: _lib._unpickle_int64_groups(Lying(), [0]),
        lambda: _lib._unpickle_spatial_index(
            gm.GeometryArray([gm.Point(0, 0, crs=4326)]), Lying()
        ),
        lambda: _lib._unpickle_h3_vertex_array(Lying()),
        lambda: _lib._unpickle_h3_edge_array(Lying()),
    ]
    for call in calls:
        _call_no_panic(call)


def test_d01_positive_cell_array_roundtrip():
    cells = gm.h3_cover(gm.box(0, 0, 1, 1, crs=4326), resolution=2).cells
    arr = gm.CellArray(list(cells))
    out = pickle.loads(pickle.dumps(arr))
    assert [c.id for c in out] == [c.id for c in arr]


# ---------------------------------------------------------------------------
# D02 — honest Arrow multilinestring amplification fails cleanly or bounds
# ---------------------------------------------------------------------------


def test_d02_arrow_multilinestring_empty_parts_budget(pyarrow_or_skip):
    """Empty line members are legal; decode is fallible-reserve only (no panic).

    Empty multiparts are linear in the declared offset length, not an
    amplification attack. A moderate empty-heavy MultiLineString must parse;
    genuine OOM surfaces as MemoryError (never PanicException).
    """
    pa = pyarrow_or_skip
    from gometry._arrow import GEOARROW_MULTILINESTRING, _extension_type_from_storage

    # Moderate empty-heavy case (was over the old magic structure ratio).
    n = 2_048
    coord = pa.struct([pa.field("x", pa.float64()), pa.field("y", pa.float64())])
    inner = pa.ListArray.from_arrays(
        pa.array(np.zeros(n + 1, dtype=np.int32)),
        pa.array([], type=coord),
    )
    outer = pa.ListArray.from_arrays(pa.array([0, n], type=pa.int32()), inner)
    typ = _extension_type_from_storage(
        pa, GEOARROW_MULTILINESTRING, outer.type, None, None
    )
    ext = pa.ExtensionArray.from_storage(typ, outer)
    arr = gm.from_arrow(ext)
    assert len(arr) == 1
    assert arr[0].geometry_type == "MultiLineString"


def test_d02_positive_dense_multilinestring_imports(pyarrow_or_skip):
    _ = pyarrow_or_skip
    # Small legitimate multi-line must still import.
    g = gm.MultiLineString(
        [
            gm.LineString([(0, 0), (1, 1)]),
            gm.LineString([(2, 2), (3, 3)]),
        ]
    )
    arr = gm.GeometryArray([g])
    arrow = arr.to_arrow()
    back = gm.from_arrow(arrow)
    assert back.to_wkt() == arr.to_wkt()


# ---------------------------------------------------------------------------
# D25 — geometry pickle CRS / EWKB SRID
# ---------------------------------------------------------------------------


def test_d25_ewkb_srid_conflict_rejected():
    # Exact audit repro: embedded 4326 + payload EPSG:3857 must not overwrite.
    w = gm.Point(1, 2, crs=4326).to_wkb(include_srid=True)
    with pytest.raises(gm.CRSMismatchError) as excinfo:
        _lib._unpickle_geometry(w, "EPSG:3857", None)
    assert "3857" in str(excinfo.value)
    assert "4326" in str(excinfo.value)


def test_d25_ewkb_srid_kept_when_frame_absent():
    # Exact audit repro: array unpickle with absent frame keeps embedded 4326.
    w = gm.Point(1, 2, crs=4326).to_wkb(include_srid=True)
    g = _lib._unpickle_geometry_array([w], None, None, None)
    assert g[0].crs is not None
    assert "4326" in str(g[0].crs)
    # Scalar path is consistent: no silent discard either.
    scalar = _lib._unpickle_geometry(w, None, None)
    assert scalar.crs is not None
    assert "4326" in str(scalar.crs)


def test_d25_invalid_frame_rejected_at_unpickle():
    # Exact audit repro: "NOT_A_CRS" fails AT unpickle (typed CRSError).
    with pytest.raises(gm.CRSError):
        _lib._unpickle_geometry(gm.Point(1, 2).to_wkb(), "NOT_A_CRS", None)


def test_d25_positive_geometry_pickle_roundtrip():
    g = gm.Point(1, 2, crs=4326)
    out = pickle.loads(pickle.dumps(g))
    assert out == g
    assert out.to_wkt() == g.to_wkt()
    assert str(out.crs) == str(g.crs)

    arr = gm.GeometryArray([gm.Point(1, 2, crs=4326), gm.Point(3, 4, crs=4326)])
    out_arr = pickle.loads(pickle.dumps(arr))
    assert out_arr.to_wkt() == arr.to_wkt()
    assert str(out_arr.crs) == str(arr.crs)

    # Epoch round-trip when present (no over-rejection).
    g_epoch = gm.Point(1, 2, crs=4326).set_epoch(2010.0)
    out_e = pickle.loads(pickle.dumps(g_epoch))
    assert out_e.epoch == g_epoch.epoch
    assert str(out_e.crs) == str(g_epoch.crs)


# ---------------------------------------------------------------------------
# D29 — SpatialIndex pickle CRS gate
# ---------------------------------------------------------------------------


def test_d29_forged_has_metadata_false_still_enforces_crs():
    # Exact audit repro: forged has_metadata=False must not disable the CRS gate.
    # Reconstructors re-derive metadata from row storage; the flag is gone.
    arr = gm.GeometryArray([gm.Point(0, 0, crs=4326)])
    with pytest.raises(TypeError):
        _lib._unpickle_spatial_index(arr, [0], False)  # type: ignore[call-arg]
    idx = _lib._unpickle_spatial_index(arr, [0])
    with pytest.raises(gm.CRSMismatchError):
        idx.query(gm.Point(0, 0, crs=3857))


def test_d29_positive_spatial_index_roundtrip():
    arr = gm.GeometryArray([gm.Point(0, 0, crs=4326), gm.Point(1, 1, crs=4326)])
    idx = gm.SpatialIndex(arr)
    out = pickle.loads(pickle.dumps(idx))
    hits = out.query(gm.Point(0, 0, crs=4326))
    assert 0 in hits.tolist()
    with pytest.raises(gm.CRSMismatchError):
        out.query(gm.Point(0, 0, crs=3857))


# ---------------------------------------------------------------------------
# D30 — ValidationReport recompute
# ---------------------------------------------------------------------------


def test_d30_forged_valid_recomputed_false():
    # Exact audit repro: forged valid=True via unpickle payload is impossible;
    # unpickle takes geometry only and re-runs validate().
    bad = gm.from_wkt("POLYGON ((0 0,1 1,1 0,0 1,0 0))")
    assert bad.is_valid is False
    with pytest.raises(TypeError):
        _lib._unpickle_validation_report(bad, None)  # type: ignore[call-arg]
    report = _lib._unpickle_validation_report(bad)
    assert report.valid is False
    assert report.valid == bad.is_valid


def test_d30_forged_nonfinite_diagnostic_not_accepted():
    # Exact audit repro: forged issue tuple with nonfinite location is rejected
    # at the signature — payload never carries diagnostics.
    with pytest.raises(TypeError):
        _lib._unpickle_validation_report(  # type: ignore[call-arg]
            gm.Point(0, 0),
            ("forged", (float("nan"), float("inf")), "not-a-path"),
        )
    report = _lib._unpickle_validation_report(gm.Point(0, 0))
    assert report.valid is True
    assert report.location is None


def test_d30_positive_validation_report_roundtrip():
    good = gm.Point(0, 0)
    bad = gm.from_wkt("POLYGON ((0 0,1 1,1 0,0 1,0 0))")
    for geom in (good, bad):
        report = geom.validate()
        out = pickle.loads(pickle.dumps(report))
        assert out.valid == report.valid == geom.is_valid
        assert out.reason == report.reason


# ---------------------------------------------------------------------------
# D26 — coverage source + partitions
# ---------------------------------------------------------------------------


def test_d26_source_normalization_matches_factory():
    """Unpickle applies the same lon/lat/domain/nonempty rules as h3_cover."""
    base = gm.h3_cover(gm.box(0, 0, 1, 1, crs=4326), resolution=1)
    args = list(base.__reduce__()[1])
    # Empty geometry: factory rejects; unpickle must too.
    empty = gm.from_wkt("POLYGON EMPTY").set_crs(4326)
    args[0] = empty
    with pytest.raises(gm.InvalidGeometryError) as excinfo:
        _lib._unpickle_h3_coverage(*args)
    _assert_not_panic(excinfo.value)
    # Out-of-domain lon/lat: factory rejects; unpickle must too.
    args[0] = gm.box(1e10, 1e10, 1e10 + 1, 1e10 + 1)
    with pytest.raises(Exception) as excinfo:
        _lib._unpickle_h3_coverage(*args)
    _assert_not_panic(excinfo.value)
    # Exact audit (a): projected source is reprojected (factory parity), never
    # trusted as raw lon/lat coordinates.
    projected = gm.box(0, 0, 1, 1, crs=3857)
    factory = gm.h3_cover(projected, resolution=1)
    args[0] = projected
    unpickled = _lib._unpickle_h3_coverage(*args)
    assert len(unpickled) == len(factory)
    assert list(unpickled.cells) == list(factory.cells)


def _assert_coverage_pickle_identity(cov) -> None:
    out = pickle.loads(pickle.dumps(cov))
    assert len(out) == len(cov)
    assert list(out.cells) == list(cov.cells)
    assert out.cell_rule == cov.cell_rule
    if len(cov) == 0:
        return
    assert out.to_polygon().bounds == cov.to_polygon().bounds


def test_d26_positive_coverage_roundtrips():
    src = gm.box(0, 0, 1, 1, crs=4326)
    covers = (
        gm.h3_cover(src, resolution=4),
        gm.s2_cover(src, level=6),
        gm.geohash_cover(src, precision=5),
        gm.tile_cover(src, zoom=8),
    )
    for cov in covers:
        _assert_coverage_pickle_identity(cov)
        compacted = cov.compact()
        _assert_coverage_pickle_identity(compacted)
        # Uncompact back to the cover depth when uniform metadata is present.
        depth = (
            getattr(cov, "resolution", None)
            or getattr(cov, "level", None)
            or getattr(cov, "precision", None)
            or getattr(cov, "zoom", None)
        )
        if depth is not None:
            expanded = compacted.uncompact(depth)
            _assert_coverage_pickle_identity(expanded)
        # with_parents is also a legitimate multiresolution pickle form.
        _assert_coverage_pickle_identity(cov.with_parents())


def test_r12_positive_empty_and_projected_roundtrips():
    """Empty coverings and projected sources (D26a false positive) must round-trip."""
    # Empty: a cover of an empty multipolygon is rejected at the factory; pickle
    # of a nonempty covering with empty visible is not factory-reachable.
    # Positive empty = no cells only when the covering itself is empty
    # is not factory-reachable; assert projected identity instead.
    wgs = gm.box(13.3, 52.4, 13.5, 52.6, crs=4326)  # Berlin
    proj = wgs.to_crs(3857)
    for factory in (
        lambda g: gm.h3_cover(g, resolution=5),
        lambda g: gm.s2_cover(g, level=8),
        lambda g: gm.geohash_cover(g, precision=5),
        lambda g: gm.tile_cover(g, zoom=10),
    ):
        a = factory(wgs)
        b = factory(proj)
        # Projected source yields the same cells as its WGS84 twin (factory).
        assert list(a.cells) == list(b.cells)
        _assert_coverage_pickle_identity(a)
        _assert_coverage_pickle_identity(b)
        _assert_coverage_pickle_identity(a.compact())
        _assert_coverage_pickle_identity(b.compact())


def test_r12_positive_cell_rule_variants_roundtrip():
    src = gm.box(0, 0, 1, 1, crs=4326)
    for rule in ("overlap", "within", "center", "bbox"):
        for cov in (
            gm.h3_cover(src, resolution=4, cell_rule=rule),
            gm.tile_cover(src, zoom=6, cell_rule=rule),
        ):
            _assert_coverage_pickle_identity(cov)
            if len(cov) > 0:
                _assert_coverage_pickle_identity(cov.compact())


# ---------------------------------------------------------------------------
# D27 — unpickle does NOT re-cap (factory max_cells is the budget)
# ---------------------------------------------------------------------------


def test_d27_unpickle_does_not_reenforce_cell_cap():
    """Transformed coverings may exceed the factory default; pickle restores them."""
    # Small covering uncompacted past any factory size is still a legitimate
    # payload — unpickle must not apply a hard 1M wall.
    src = gm.box(0, 0, 1, 1, crs=4326)
    for cov in (
        gm.h3_cover(src, resolution=3).uncompact(5),
        gm.s2_cover(src, level=5, max_cells=10000).uncompact(7),
        gm.geohash_cover(src, precision=4).uncompact(6),
        gm.tile_cover(src, zoom=5).uncompact(7),
    ):
        out = pickle.loads(pickle.dumps(cov))
        assert len(out) == len(cov)
        assert list(out.cells) == list(cov.cells)


def test_d27_positive_coverage_roundtrips():
    src = gm.box(0, 0, 1, 1, crs=4326)
    for cov in (
        gm.h3_cover(src, resolution=2),
        gm.s2_cover(src, level=6),
        gm.geohash_cover(src, precision=4),
        gm.tile_cover(src, zoom=6),
    ):
        out = pickle.loads(pickle.dumps(cov))
        assert len(out) == len(cov)
        assert list(out.cells) == list(cov.cells)
        assert out.to_polygon().bounds == cov.to_polygon().bounds


# ---------------------------------------------------------------------------
# D28 — impossible coverage params rejected
# ---------------------------------------------------------------------------


def test_d28_s2_impossible_params_rejected():
    cov = gm.s2_cover(gm.box(0, 0, 1, 1, crs=4326), level=1)
    args = list(cov.__reduce__()[1])
    # Payload: (geom, cells, rule, min_level, max_level, level_mod, max_cells, target_cells)
    bad = list(args)
    bad[3:7] = [31, 1, 0, 0]
    with pytest.raises(gm.GeometryError) as excinfo:
        _lib._unpickle_s2_coverage(*bad)
    _assert_not_panic(excinfo.value)
    # Individual public-parser cases (same validators as s2_cover).
    cases = [
        ([31, 1, 1, 10000], "level"),
        ([5, 1, 1, 10000], "min_level"),
        ([1, 1, 0, 10000], "level_mod"),
        ([1, 1, 1, 0], "max_cells"),
    ]
    for patch, _needle in cases:
        bad = list(args)
        bad[3:7] = patch
        with pytest.raises(gm.GeometryError) as excinfo:
            _lib._unpickle_s2_coverage(*bad)
        _assert_not_panic(excinfo.value)


def test_d28_geohash_impossible_precision_rejected():
    cov = gm.geohash_cover(gm.box(0, 0, 1, 1, crs=4326), precision=4)
    args = list(cov.__reduce__()[1])
    # (geom, cells, rule, factory_precision, visible_depth, max_cells)
    factory_idx = -3
    for bad in (0, 13, 255):
        bad_args = list(args)
        bad_args[factory_idx] = bad
        with pytest.raises(gm.GeometryError, match="precision") as excinfo:
            _lib._unpickle_geohash_coverage(*bad_args)
        _assert_not_panic(excinfo.value)


def test_d28_tile_impossible_zoom_rejected():
    cov = gm.tile_cover(gm.box(0, 0, 1, 1, crs=4326), zoom=6)
    args = list(cov.__reduce__()[1])
    factory_idx = -3
    for bad in (30, 255):
        bad_args = list(args)
        bad_args[factory_idx] = bad
        with pytest.raises(gm.GeometryError, match="zoom") as excinfo:
            _lib._unpickle_tile_coverage(*bad_args)
        _assert_not_panic(excinfo.value)


def test_d28_h3_impossible_resolution_rejected():
    cov = gm.h3_cover(gm.box(0, 0, 1, 1, crs=4326), resolution=1)
    args = list(cov.__reduce__()[1])
    # (geom, cells, rule, factory_resolution, visible_depth, max_cells)
    factory_idx = -3
    for bad in (16, 255):
        bad_args = list(args)
        bad_args[factory_idx] = bad
        with pytest.raises(gm.GeometryError, match="resolution") as excinfo:
            _lib._unpickle_h3_coverage(*bad_args)
        _assert_not_panic(excinfo.value)


def test_d20_empty_coverage_rejects_impossible_visible_depth():
    """D20: empty-cell unpickle must still validate visible_depth.

    When cells are empty, depth falls back to serialized visible_depth; that
    value must pass the same public range gate as factory depth (H3 0-15,
    geohash 1-12, tile 0-29). S2 has no separate visible_depth — min/max
    levels are always range-checked.
    """
    src = gm.Point(0, 0, crs=4326)

    # H3 audit repro: empty cells + visible_depth=255.
    with pytest.raises(gm.GeometryError, match="resolution") as excinfo:
        _lib._unpickle_h3_coverage(src, [], "within", 0, 255, 1_000_000)
    _assert_not_panic(excinfo.value)
    with pytest.raises(gm.GeometryError, match="resolution") as excinfo:
        _lib._unpickle_h3_coverage(src, [], "within", 0, 16, 1_000_000)
    _assert_not_panic(excinfo.value)

    # Geohash / tile empty path (visible_depth is second-to-last; max_cells last).
    with pytest.raises(gm.GeometryError, match="precision") as excinfo:
        _lib._unpickle_geohash_coverage(src, [], "within", 1, 255, 1_000_000)
    _assert_not_panic(excinfo.value)
    with pytest.raises(gm.GeometryError, match="precision") as excinfo:
        _lib._unpickle_geohash_coverage(src, [], "within", 1, 13, 1_000_000)
    _assert_not_panic(excinfo.value)
    with pytest.raises(gm.GeometryError, match="zoom") as excinfo:
        _lib._unpickle_tile_coverage(src, [], "within", 0, 255, 1_000_000)
    _assert_not_panic(excinfo.value)
    with pytest.raises(gm.GeometryError, match="zoom") as excinfo:
        _lib._unpickle_tile_coverage(src, [], "within", 0, 30, 1_000_000)
    _assert_not_panic(excinfo.value)

    # S2: impossible max_level on empty visible cells (levels always gated).
    with pytest.raises(gm.GeometryError, match="level") as excinfo:
        _lib._unpickle_s2_coverage(src, [], "within", 0, 31, 1, 10000, 8)
    _assert_not_panic(excinfo.value)

    # Legitimate empty coverages at valid depths still restore.
    empty_h3 = _lib._unpickle_h3_coverage(src, [], "within", 0, 0, 1_000_000)
    assert len(empty_h3) == 0
    assert empty_h3.resolution == 0
    round_h3 = pickle.loads(pickle.dumps(gm.h3_cover(src, resolution=0, cell_rule="within")))
    assert len(round_h3) == 0
    assert round_h3.resolution == 0

    empty_gh = _lib._unpickle_geohash_coverage(src, [], "within", 1, 1, 1_000_000)
    assert len(empty_gh) == 0
    assert empty_gh.precision == 1

    empty_tile = _lib._unpickle_tile_coverage(src, [], "within", 0, 0, 1_000_000)
    assert len(empty_tile) == 0
    assert empty_tile.zoom == 0

    empty_s2 = gm.s2_cover(src, level=1, cell_rule="within")
    assert len(empty_s2) == 0
    round_s2 = pickle.loads(pickle.dumps(empty_s2))
    assert len(round_s2) == 0


def test_d28_positive_params_roundtrip():
    src = gm.box(0, 0, 1, 1, crs=4326)
    for cov in (
        gm.h3_cover(src, resolution=3),
        gm.s2_cover(src, level=5),
        gm.geohash_cover(src, precision=5),
        gm.tile_cover(src, zoom=7),
    ):
        out = pickle.loads(pickle.dumps(cov))
        assert len(out) == len(cov)
        assert out.cell_rule == cov.cell_rule


# ---------------------------------------------------------------------------
# D32 — empty Groups offsets
# ---------------------------------------------------------------------------


def test_d32_empty_geometry_groups_unpickle():
    # Empty offsets is invalid CSR (must start with 0). Pre-fix release raised
    # BufferError / debug panic on len()-1; require a clean GeometryError.
    with pytest.raises(gm.GeometryError, match="invalid CSR offsets") as excinfo:
        _lib._unpickle_geometry_groups(gm.GeometryArray([]), [])
    assert type(excinfo.value).__name__ != "BufferError"
    assert type(excinfo.value).__name__ != "PanicException"

    # Valid empty Groups: offsets=[0].
    g = _lib._unpickle_geometry_groups(gm.GeometryArray([]), [0])
    assert len(g) == 0


def test_d32_empty_cell_groups_unpickle():
    with pytest.raises(gm.GeometryError, match="invalid CSR offsets") as excinfo:
        _lib._unpickle_cell_groups(gm.CellArray([], type=gm.H3Cell), [])
    assert type(excinfo.value).__name__ != "BufferError"
    assert type(excinfo.value).__name__ != "PanicException"

    g = _lib._unpickle_cell_groups(gm.CellArray([], type=gm.H3Cell), [0])
    assert len(g) == 0


def test_d32_positive_groups_roundtrip():
    arr = gm.GeometryArray([gm.Point(0, 0), gm.Point(1, 1)])
    # Build groups via a real op when available; else unpickle a valid CSR.
    groups = _lib._unpickle_geometry_groups(arr, [0, 1, 2])
    out = pickle.loads(pickle.dumps(groups))
    assert len(out) == 2


# ---------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def pyarrow_or_skip():
    return pytest.importorskip("pyarrow")
