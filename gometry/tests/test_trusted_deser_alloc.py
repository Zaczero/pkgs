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
    assert type(exc).__name__ != 'PanicException', f'Rust panic: {exc}'
    assert 'capacity overflow' not in str(exc).lower()


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
    out, err = _call_no_panic(lambda: _lib._unpickle_cell_array(Lying(), 'h3', None))
    if err is None:
        assert len(out) == 0


def test_d01_lying_len_groups_no_panic():
    calls = [
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
    cells = gm.h3_cover(gm.box(0, 0, 1, 1, crs=4326), resolution=2)
    arr = gm.CellArray(list(cells))
    out = pickle.loads(pickle.dumps(arr))
    assert [c.id for c in out] == [c.id for c in arr]


def test_cell_array_pickle_mask_is_zero_present_one_missing():
    cell = gm.H3Cell(13.4, 52.5, resolution=7)
    mask = bytes([0, 1, 0])
    identities = [cell.id, cell.id]
    arr = _lib._unpickle_cell_array(identities, 'h3', mask)
    _callable, args = arr.__reduce__()
    assert args == (identities, 'h3', mask)
    assert pickle.loads(pickle.dumps(arr)).is_missing.tolist() == [False, True, False]


def test_cell_array_pickle_mask_rejects_non_bytes():
    cell = gm.H3Cell(13.4, 52.5, resolution=7)
    with pytest.raises(ValueError, match='bytes or None'):
        _lib._unpickle_cell_array([cell.id], 'h3', [0])


def test_cell_array_pickle_all_missing_roundtrip():
    arr = _lib._unpickle_cell_array([], 'h3', bytes([1, 1, 1]))
    _callable, args = arr.__reduce__()

    assert len(arr) == 3
    assert arr.is_missing.tolist() == [True, True, True]
    assert list(arr) == [None, None, None]
    assert args[0] == []
    assert args[1:] == ('h3', bytes([1, 1, 1]))

    out = pickle.loads(pickle.dumps(arr))
    assert len(out) == len(arr)
    assert out.is_missing.tolist() == [True, True, True]
    assert list(out) == [None, None, None]
    assert out == arr
    assert hash(out) == hash(arr)


@pytest.mark.parametrize('mask', [bytes([0, 2]), bytes([0, 0]), bytes([0, 0, 1])])
def test_cell_array_pickle_mask_rejects_bad_values_and_counts(mask):
    cell = gm.H3Cell(13.4, 52.5, resolution=7)
    with pytest.raises(ValueError, match='mask'):
        _lib._unpickle_cell_array([cell.token], 'h3', mask)


def test_cell_array_pickle_invalid_huge_mask_rejected_without_panic():
    """A large invalid mask is rejected cleanly.

    Python-level testing can observe rejection and panic-freedom, but not the
    allocator ordering inside the native parser.
    """
    cell = gm.H3Cell(13.4, 52.5, resolution=7)
    with pytest.raises(ValueError, match='present count'):
        _lib._unpickle_cell_array(
            [cell.token], 'h3', bytes([0, 0]) + bytes([1]) * 10_000_000
        )


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
    coord = pa.struct([pa.field('x', pa.float64()), pa.field('y', pa.float64())])
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
    assert arr[0].geometry_type == 'MultiLineString'


def test_d02_positive_dense_multilinestring_imports(pyarrow_or_skip):
    _ = pyarrow_or_skip
    # Small legitimate multi-line must still import.
    g = gm.MultiLineString([
        gm.LineString([(0, 0), (1, 1)]),
        gm.LineString([(2, 2), (3, 3)]),
    ])
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
        _lib._unpickle_geometry(w, 'EPSG:3857', None)
    assert '3857' in str(excinfo.value)
    assert '4326' in str(excinfo.value)


def test_d25_ewkb_srid_kept_when_frame_absent():
    # Exact audit repro: array unpickle with absent frame keeps embedded 4326.
    w = gm.Point(1, 2, crs=4326).to_wkb(include_srid=True)
    g = _lib._unpickle_geometry_array([w], None, None, None)
    assert g[0].crs is not None
    assert '4326' in str(g[0].crs)
    # Scalar path is consistent: no silent discard either.
    scalar = _lib._unpickle_geometry(w, None, None)
    assert scalar.crs is not None
    assert '4326' in str(scalar.crs)


def test_d25_invalid_frame_rejected_at_unpickle():
    # Exact audit repro: "NOT_A_CRS" fails AT unpickle (typed CRSError).
    with pytest.raises(gm.CRSError):
        _lib._unpickle_geometry(gm.Point(1, 2).to_wkb(), 'NOT_A_CRS', None)


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
    bad = gm.from_wkt('POLYGON ((0 0,1 1,1 0,0 1,0 0))')
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
            ('forged', (float('nan'), float('inf')), 'not-a-path'),
        )
    report = _lib._unpickle_validation_report(gm.Point(0, 0))
    assert report.valid is True
    assert report.location is None


def test_d30_positive_validation_report_roundtrip():
    good = gm.Point(0, 0)
    bad = gm.from_wkt('POLYGON ((0 0,1 1,1 0,0 1,0 0))')
    for geom in (good, bad):
        report = geom.validate()
        out = pickle.loads(pickle.dumps(report))
        assert out.valid == report.valid == geom.is_valid
        assert out.reason == report.reason


# D32 — empty Groups offsets
# ---------------------------------------------------------------------------


def test_d32_empty_geometry_groups_unpickle():
    # Empty offsets is invalid CSR (must start with 0). Pre-fix release raised
    # BufferError / debug panic on len()-1; require a clean GeometryError.
    with pytest.raises(gm.GeometryError, match='invalid CSR offsets') as excinfo:
        _lib._unpickle_geometry_groups(gm.GeometryArray([]), [])
    assert type(excinfo.value).__name__ != 'BufferError'
    assert type(excinfo.value).__name__ != 'PanicException'

    # Valid empty Groups: offsets=[0].
    g = _lib._unpickle_geometry_groups(gm.GeometryArray([]), [0])
    assert len(g) == 0


def test_d32_empty_cell_groups_unpickle():
    with pytest.raises(gm.GeometryError, match='invalid CSR offsets') as excinfo:
        _lib._unpickle_cell_groups(gm.CellArray([], type=gm.H3Cell), [])
    assert type(excinfo.value).__name__ != 'BufferError'
    assert type(excinfo.value).__name__ != 'PanicException'

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
    return pytest.importorskip('pyarrow')
