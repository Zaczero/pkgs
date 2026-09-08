use std::sync::Arc;

use pyo3::IntoPyObjectExt as _;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::array::{
    GeometryArrayStorage, PackedStorage, PointRowLayout, RowSelection, RowSelectionRef,
    ValidityMask,
};
use crate::py::wire_crs::{SridFrameAdmission, guard_embedded_crs_conflict};
use crate::{CoordSeq, Crs, Frame, PyGeometry, PyGeometryArray, ShapeData, Typed};

type EncodedPointColumns = (Vec<u8>, Vec<u8>, Option<Vec<u8>>, Option<Vec<u8>>);

fn pickle_error(message: impl Into<Box<str>>) -> crate::error::Error {
    crate::geometry::GeometryErrorKind::pickle_payload(message)
}

fn pickle_size_overflow(message: &'static str) -> crate::error::Error {
    crate::error::ResourceError::SizeOverflowContext(message).into()
}

// --- Pickle support ----------------------------------------------------------
//
// `__reduce__` payloads round-trip geometry rows through plain WKB (Z/M
// preserved; the frame travels separately as the canonical CRS string plus
// epoch) and packed point/line/polygon arrays through their raw f64 columns
// (plus CSR offsets for lineal storage). Data types
// pickle; derived engines (spatial indexes, prepared geometries, coverages)
// are rebuilt instead, like their ecosystem counterparts.

/// One selected point coordinate column in logical row order.
fn selected_f64_column_le_bytes(
    values: &[f64],
    selection: RowSelectionRef<'_>,
) -> crate::error::Result<Vec<u8>> {
    let len = selection.len(values.len());
    let byte_len = len
        .checked_mul(size_of::<f64>())
        .ok_or_else(|| pickle_size_overflow("coordinate byte length overflows usize"))?;
    let mut out = Vec::new();
    crate::boundary::try_reserve_vec(&mut out, byte_len)?;
    for logical in 0..len {
        out.extend_from_slice(&values[selection.physical(logical)].to_le_bytes());
    }
    Ok(out)
}

fn selected_f64_windows_le_bytes(
    values: &[f64],
    windows: impl Iterator<Item = std::ops::Range<usize>>,
    value_count: usize,
) -> crate::error::Result<Vec<u8>> {
    let byte_len = value_count
        .checked_mul(size_of::<f64>())
        .ok_or_else(|| pickle_size_overflow("coordinate byte length overflows usize"))?;
    let mut out = Vec::new();
    crate::boundary::try_reserve_vec(&mut out, byte_len)?;
    for window in windows {
        for &value in &values[window] {
            out.extend_from_slice(&value.to_le_bytes());
        }
    }
    Ok(out)
}

fn rebased_i32_offsets_le_bytes(
    lengths: impl Iterator<Item = usize>,
    row_count: usize,
) -> crate::error::Result<Vec<u8>> {
    let offset_count = row_count
        .checked_add(1)
        .ok_or_else(|| pickle_size_overflow("offset count overflows usize"))?;
    let byte_len = offset_count
        .checked_mul(size_of::<i32>())
        .ok_or_else(|| pickle_size_overflow("offset byte length overflows usize"))?;
    let mut out = Vec::new();
    crate::boundary::try_reserve_vec(&mut out, byte_len)?;
    out.extend_from_slice(&0_i32.to_le_bytes());
    let mut total = 0_usize;
    for length in lengths {
        total = total
            .checked_add(length)
            .ok_or_else(|| pickle_size_overflow("offset value overflows usize"))?;
        let offset = i32::try_from(total)
            .map_err(|_| pickle_size_overflow("offset value does not fit in i32"))?;
        out.extend_from_slice(&offset.to_le_bytes());
    }
    Ok(out)
}

/// Canonicalize a CRS string arriving from a pickle payload through the same
/// PROJ-backed parser public constructors use. Rejects `"NOT_A_CRS"` at unpickle
/// rather than deferring failure to a later CRS operation.
fn deserialized_crs(crs: Option<String>) -> crate::error::Result<Option<Crs>> {
    match crs {
        None => Ok(None),
        Some(text) => {
            let canonical = crate::crs::canonicalize(&text).map_err(|err| {
                crate::crs::CrsError::message(format!(
                    "invalid CRS in geometry pickle payload: {err}"
                ))
            })?;
            Ok(Some(canonical))
        },
    }
}

fn deserialized_epoch(epoch: Option<f64>, crs: Option<&str>) -> crate::error::Result<Option<f64>> {
    let epoch = match epoch {
        None => None,
        Some(value) if value.is_finite() => Some(if value == 0.0 { 0.0 } else { value }),
        Some(_) => {
            return Err(pickle_error(
                "coordinate epoch must be a finite decimal year",
            ));
        },
    };
    let crs = crs.map(Crs::from);
    Frame::new(crs, epoch).map(|_| epoch)
}

fn snapshot_bytes(bytes: &[u8]) -> PyResult<Vec<u8>> {
    let mut snapshot = Vec::new();
    crate::boundary::try_reserve_vec(&mut snapshot, bytes.len()).map_err(PyErr::from)?;
    snapshot.extend_from_slice(bytes);
    Ok(snapshot)
}

fn snapshot_optional_bytes(bytes: Option<&[u8]>) -> PyResult<Option<Vec<u8>>> {
    bytes.map(snapshot_bytes).transpose()
}

#[expect(
    clippy::used_underscore_items,
    reason = "the leading underscore is the Python convention for a module-private helper, and pickle stores these names in its stream, so the name is a wire contract rather than a Rust style choice"
)]
/// Rebuild a pickled `Geometry` (internal; see ``Geometry.__reduce__``).
///
/// Frame strings are canonicalized through PROJ. Embedded EWKB SRIDs reuse the
/// public `from_wkb` reconciliation (conflict with the payload CRS is rejected;
/// an embedded SRID is kept when the payload CRS is absent).
#[pyfunction]
pub(crate) fn _unpickle_geometry(
    py: Python<'_>,
    wkb: &[u8],
    crs: Option<String>,
    epoch: Option<f64>,
) -> PyResult<Typed> {
    let wkb = snapshot_bytes(wkb)?;
    let geometry = py
        .detach(move || unpickle_geometry_detached(&wkb, crs, epoch))
        .map_err(PyErr::from)?;
    Ok(Typed(geometry))
}

#[expect(
    clippy::used_underscore_items,
    reason = "the leading underscore is the Python convention for a module-private helper, and pickle stores these names in its stream, so the name is a wire contract rather than a Rust style choice"
)]
/// Rebuild a pickled mixed-storage `GeometryArray` (internal; see
/// ``GeometryArray.__reduce__``).
///
/// Always builds Mixed storage: WKB intentionally preserves malformed rings
/// for `validate()`, and must never enter trusted packed columns.
///
/// Frame strings are canonicalized; per-row EWKB SRIDs reconcile against the
/// array frame (same rules as bulk `from_wkb`).
#[pyfunction]
pub(crate) fn _unpickle_geometry_array(
    py: Python<'_>,
    rows: &Bound<'_, PyAny>,
    crs: Option<String>,
    epoch: Option<f64>,
    missing: Option<&[u8]>,
) -> PyResult<PyGeometryArray> {
    // Collect bytes rows manually: generic `Vec<Vec<u8>>` extraction allocates
    // from a lying `__len__` before any Rust check (allocator abort).
    let rows = crate::collect_bytes_rows(rows)?;
    let missing = snapshot_optional_bytes(missing)?;
    py.detach(move || unpickle_geometry_array_detached(rows, crs, epoch, missing))
        .map_err(PyErr::from)
}

#[expect(
    clippy::used_underscore_items,
    reason = "the leading underscore is the Python convention for a module-private helper, and pickle stores these names in its stream, so the name is a wire contract rather than a Rust style choice"
)]
/// Rebuild a pickled packed-point `GeometryArray` from raw f64 columns
/// (internal; see ``GeometryArray.__reduce__``).
///
/// Packed-point payloads carry only finite present rows; an optional missing
/// mask restores their original logical row positions.
#[pyfunction]
#[pyo3(signature = (xs, ys, zs, ms, crs, epoch, row_map, missing))]
pub(crate) fn _unpickle_point_array(
    py: Python<'_>,
    xs: &[u8],
    ys: &[u8],
    zs: Option<&[u8]>,
    ms: Option<&[u8]>,
    crs: Option<String>,
    epoch: Option<f64>,
    row_map: Option<&[u8]>,
    missing: Option<&[u8]>,
) -> PyResult<PyGeometryArray> {
    let xs = snapshot_bytes(xs)?;
    let ys = snapshot_bytes(ys)?;
    let zs = snapshot_optional_bytes(zs)?;
    let ms = snapshot_optional_bytes(ms)?;
    let row_map = snapshot_optional_bytes(row_map)?;
    let missing = snapshot_optional_bytes(missing)?;
    py.detach(move || unpickle_point_array_detached(xs, ys, zs, ms, crs, epoch, row_map, missing))
        .map_err(PyErr::from)
}

#[expect(
    clippy::used_underscore_items,
    reason = "the leading underscore is the Python convention for a module-private helper, and pickle stores these names in its stream, so the name is a wire contract rather than a Rust style choice"
)]
/// Rebuild a pickled packed-line `GeometryArray` from raw f64 columns plus
/// little-endian `i32` CSR offsets (internal; see
/// ``GeometryArray.__reduce__``).
#[pyfunction]
pub(crate) fn _unpickle_line_array(
    py: Python<'_>,
    xs: &[u8],
    ys: &[u8],
    zs: Option<&[u8]>,
    ms: Option<&[u8]>,
    offsets: &[u8],
    crs: Option<String>,
    epoch: Option<f64>,
    row_map: Option<&[u8]>,
    missing: Option<&[u8]>,
) -> PyResult<PyGeometryArray> {
    let xs = snapshot_bytes(xs)?;
    let ys = snapshot_bytes(ys)?;
    let zs = snapshot_optional_bytes(zs)?;
    let ms = snapshot_optional_bytes(ms)?;
    let offsets = snapshot_bytes(offsets)?;
    let row_map = snapshot_optional_bytes(row_map)?;
    let missing = snapshot_optional_bytes(missing)?;
    py.detach(move || {
        unpickle_line_array_detached(xs, ys, zs, ms, offsets, crs, epoch, row_map, missing)
    })
    .map_err(PyErr::from)
}

#[expect(
    clippy::used_underscore_items,
    reason = "the leading underscore is the Python convention for a module-private helper, and pickle stores these names in its stream, so the name is a wire contract rather than a Rust style choice"
)]
/// Rebuild a pickled packed-polygon `GeometryArray` from raw f64 columns plus
/// little-endian `i32` two-level CSR offsets (internal; see
/// ``GeometryArray.__reduce__``).
#[pyfunction]
pub(crate) fn _unpickle_polygon_array(
    py: Python<'_>,
    xs: &[u8],
    ys: &[u8],
    zs: Option<&[u8]>,
    ms: Option<&[u8]>,
    ring_offsets: &[u8],
    polygon_offsets: &[u8],
    crs: Option<String>,
    epoch: Option<f64>,
    row_map: Option<&[u8]>,
    missing: Option<&[u8]>,
) -> PyResult<PyGeometryArray> {
    let xs = snapshot_bytes(xs)?;
    let ys = snapshot_bytes(ys)?;
    let zs = snapshot_optional_bytes(zs)?;
    let ms = snapshot_optional_bytes(ms)?;
    let ring_offsets = snapshot_bytes(ring_offsets)?;
    let polygon_offsets = snapshot_bytes(polygon_offsets)?;
    let row_map = snapshot_optional_bytes(row_map)?;
    let missing = snapshot_optional_bytes(missing)?;
    py.detach(move || {
        unpickle_polygon_array_detached(
            xs,
            ys,
            zs,
            ms,
            ring_offsets,
            polygon_offsets,
            crs,
            epoch,
            row_map,
            missing,
        )
    })
    .map_err(PyErr::from)
}

/// Untrusted pickle ring boundary — same policy as WKT/WKB
/// ([`crate::io::admit_closed_ring`]):
/// silent-close XY-open rings (≥3 corners), reject Z/M-open and too-short.
/// When any ring is open the coordinate columns and ring CSR are rebuilt so
/// packed storage still holds closed rings only.
fn admit_pickled_polygon_rings(
    seq: crate::geometry::CoordSeq,
    ring_offsets: crate::geometry::CsrOffsetColumn<crate::geometry::RingLevel>,
) -> crate::error::Result<(
    crate::geometry::CoordSeq,
    crate::geometry::CsrOffsetColumn<crate::geometry::RingLevel>,
)> {
    use crate::geometry::CoordSeqBuilder;

    let mut windows = Vec::new();
    crate::boundary::try_reserve_vec(&mut windows, ring_offsets.len().saturating_sub(1))?;
    for [start, end] in ring_offsets.array_windows::<2>() {
        crate::boundary::try_push_vec(&mut windows, (*start as usize, *end as usize))?;
    }

    // Fast path: every ring already fully closed on active ordinates and long
    // enough — no rebuild (the common gometry-written pickle case).
    let already_closed = windows
        .iter()
        .all(|&(start, end)| crate::array::CheckedRingWindow::checked(&seq, start..end).is_ok());
    if already_closed {
        return Ok((seq, ring_offsets));
    }

    // Rebuild through the shared admitter (silent-close / reject).
    let coord_capacity = seq
        .len()
        .checked_add(windows.len())
        .ok_or_else(|| pickle_error("polygon-array pickle coordinate count overflows usize"))?;
    let mut builder = CoordSeqBuilder::try_with_capacity(seq.axes(), coord_capacity)?;
    let offset_capacity = windows
        .len()
        .checked_add(1)
        .ok_or_else(|| pickle_error("polygon-array pickle offset count overflows usize"))?;
    let mut new_offsets = Vec::new();
    crate::boundary::try_reserve_vec(&mut new_offsets, offset_capacity)?;
    crate::boundary::try_push_vec(&mut new_offsets, 0)?;
    for (start, end) in windows {
        let n = end - start;
        let ring_capacity = n
            .checked_add(1)
            .ok_or_else(|| pickle_error("polygon-array pickle ring size overflows usize"))?;
        let mut ring_builder = CoordSeqBuilder::try_with_capacity(seq.axes(), ring_capacity)?;
        for index in start..end {
            ring_builder.push_at(&seq, index)?;
        }
        let ring_seq = ring_builder.finish()?;
        let admitted = crate::io::admit_closed_ring(ring_seq)
            .map_err(|error| pickle_error(error.to_string()))?;
        let coords = admitted.coords();
        for index in 0..coords.len() {
            builder.push_at(coords, index)?;
        }
        let next = i32::try_from(builder.len())
            .map_err(|_| pickle_error("polygon-array pickle exceeds the offset domain"))?;
        crate::boundary::try_push_vec(&mut new_offsets, next)?;
    }
    let seq = builder.finish()?;
    let ring_offsets = crate::geometry::CsrOffsetColumn::try_from_shared_i32(
        crate::geometry::SharedSlice::try_from_vec(new_offsets)?,
        seq.len(),
    )
    .map_err(|_| pickle_error("polygon-array pickle ring offsets are not a valid CSR"))?;
    Ok((seq, ring_offsets))
}

/// Every polygon CSR window must cover at least one ring (`end > start`).
/// Zero-row arrays keep `[0]` / `[0]` (no windows) as the only empty case.
fn validate_pickled_polygon_shells(
    polygon_offsets: &crate::geometry::CsrOffsetColumn<crate::geometry::PolygonLevel>,
) -> crate::error::Result<()> {
    for [start, end] in polygon_offsets.array_windows::<2>() {
        if end <= start {
            return Err(pickle_error(
                "polygon-array pickle polygon has no shell ring",
            ));
        }
    }
    Ok(())
}

/// Decode an optional little-endian `u64` logical-row map from a pickle
/// payload. Empty bytes are a valid empty gather (zero logical rows).
fn pickled_usize_row_map(
    bytes: Option<&[u8]>,
    physical_rows: usize,
    label: &str,
) -> crate::error::Result<RowSelection> {
    let Some(bytes) = bytes else {
        return Ok(RowSelection::Identity);
    };
    if !bytes.len().is_multiple_of(size_of::<u64>()) {
        return Err(pickle_error(format!("malformed {label} pickle row_map")));
    }
    let mut map = Vec::new();
    crate::boundary::try_reserve_vec(&mut map, bytes.len() / size_of::<u64>())?;
    for chunk in bytes.as_chunks::<8>().0 {
        let value = u64::from_le_bytes(*chunk);
        let value = usize::try_from(value)
            .map_err(|_| pickle_error(format!("{label} pickle row_map index out of range")))?;
        crate::boundary::try_push_vec(&mut map, value)?;
    }
    RowSelection::gather_checked_native(map, physical_rows)?.ok_or_else(|| {
        pickle_error(format!(
            "{label} pickle row_map references a nonexistent physical row"
        ))
    })
}

/// Decode little-endian `i32` CSR offsets from a packed-array pickle payload.
fn pickled_i32_csr_offsets(bytes: &[u8], label: &str) -> crate::error::Result<Vec<i32>> {
    if !bytes.len().is_multiple_of(size_of::<i32>()) || bytes.len() < size_of::<i32>() {
        return Err(pickle_error(format!("malformed {label} pickle offsets")));
    }
    let mut offsets = Vec::new();
    crate::boundary::try_reserve_vec(&mut offsets, bytes.len() / size_of::<i32>())?;
    for chunk in bytes.as_chunks::<4>().0 {
        crate::boundary::try_push_vec(&mut offsets, i32::from_le_bytes(*chunk))?;
    }
    Ok(offsets)
}

fn pickled_missing_mask(
    len: usize,
    missing: Option<&[u8]>,
    label: &str,
    present_count: Option<usize>,
) -> crate::error::Result<Option<ValidityMask>> {
    let Some(bytes) = missing else {
        return Ok(None);
    };
    if bytes.len() != len {
        return Err(pickle_error(format!(
            "malformed {label} pickle missing mask"
        )));
    }
    // Untrusted boundary: only exact 0/1 bytes are valid mask entries.
    if bytes.iter().any(|&byte| byte > 1) {
        return Err(pickle_error(format!(
            "malformed {label} pickle missing mask"
        )));
    }
    let mut values = Vec::new();
    crate::boundary::try_reserve_vec(&mut values, len)?;
    for &byte in bytes {
        crate::boundary::try_push_vec(&mut values, byte != 0)?;
    }
    let mask = ValidityMask::from_vec(len, values)?;
    let decoded_present_count = mask.as_ref().map_or(len, ValidityMask::present_count);
    if present_count.is_some_and(|present_count| decoded_present_count != present_count) {
        return Err(pickle_error(format!(
            "malformed {label} pickle missing mask"
        )));
    }
    Ok(mask)
}

/// Decode the shared column payload of the packed-array picklers.
/// Every physical ordinate must be finite — the reducer never emits orphan
/// NaN placeholders on packed lanes.
///
/// Columns fill exact final `Arc`s (no `Vec` intermediate). Element count is
/// **derived from each column's byte length** (`len / 8`), never from a
/// separate trusted `size_hint` — same class of fix as the WKB exact-Arc
/// decoder. Validation messages and reconstruction semantics are unchanged.
fn pickled_coordseq(
    xs: &[u8],
    ys: &[u8],
    zs: Option<&[u8]>,
    ms: Option<&[u8]>,
) -> crate::error::Result<CoordSeq> {
    let column = |bytes: &[u8]| -> crate::error::Result<Vec<f64>> {
        if !bytes.len().is_multiple_of(size_of::<f64>()) {
            unreachable!("column lengths are validated before allocation");
        }
        // Proven count from the byte span (not a caller-supplied length).
        let chunks = bytes.as_chunks::<8>().0;
        let len = chunks.len();
        let mut values = Vec::new();
        crate::boundary::try_reserve_vec(&mut values, len)?;
        for chunk in chunks {
            crate::boundary::try_push_vec(&mut values, f64::from_le_bytes(*chunk))?;
        }
        Ok(values)
    };
    for bytes in [Some(xs), Some(ys), zs, ms].into_iter().flatten() {
        if !bytes.len().is_multiple_of(size_of::<f64>()) {
            return Err(pickle_error("malformed packed-array pickle column"));
        }
    }
    let xs = column(xs)?;
    let ys = column(ys)?;
    let zs = zs.map(column).transpose()?;
    let ms = ms.map(column).transpose()?;
    if xs.len() != ys.len()
        || zs.as_ref().is_some_and(|column| column.len() != xs.len())
        || ms.as_ref().is_some_and(|column| column.len() != xs.len())
    {
        return Err(pickle_error("packed-array pickle columns differ in length"));
    }
    CoordSeq::from_owned_columns(xs, ys, zs, ms)
}

struct GeometryPicklePayload {
    wkb: Vec<u8>,
    crs: Option<String>,
    epoch: Option<f64>,
}

fn geometry_pickle_payload_detached(
    shape: Arc<ShapeData>,
    crs: Option<String>,
    epoch: Option<f64>,
) -> crate::error::Result<GeometryPicklePayload> {
    Ok(GeometryPicklePayload {
        wkb: crate::io::to_wkb(shape.shape(), None, false)?,
        crs,
        epoch,
    })
}

enum ArrayPicklePayload {
    Mixed {
        rows: Vec<Vec<u8>>,
        crs: Option<String>,
        epoch: Option<f64>,
        missing: Option<Vec<u8>>,
    },
    Points {
        xs: Vec<u8>,
        ys: Vec<u8>,
        zs: Option<Vec<u8>>,
        ms: Option<Vec<u8>>,
        crs: Option<String>,
        epoch: Option<f64>,
        row_map: Option<Vec<u8>>,
        missing: Option<Vec<u8>>,
    },
    Lines {
        xs: Vec<u8>,
        ys: Vec<u8>,
        zs: Option<Vec<u8>>,
        ms: Option<Vec<u8>>,
        offsets: Vec<u8>,
        crs: Option<String>,
        epoch: Option<f64>,
        row_map: Option<Vec<u8>>,
        missing: Option<Vec<u8>>,
    },
    Polygons {
        xs: Vec<u8>,
        ys: Vec<u8>,
        zs: Option<Vec<u8>>,
        ms: Option<Vec<u8>>,
        ring_offsets: Vec<u8>,
        polygon_offsets: Vec<u8>,
        crs: Option<String>,
        epoch: Option<f64>,
        row_map: Option<Vec<u8>>,
        missing: Option<Vec<u8>>,
    },
}

/// Detached payload planning for `Geometry.__reduce__`.
pub(crate) fn reduce_geometry(
    py: Python<'_>,
    geometry: &PyGeometry,
) -> PyResult<(Py<PyAny>, Py<PyAny>)> {
    let shape = Arc::clone(&geometry.shape);
    let crs = geometry.crs_str().map(str::to_owned);
    let epoch = geometry.epoch();
    let payload = py
        .detach(move || geometry_pickle_payload_detached(shape, crs, epoch))
        .map_err(PyErr::from)?;
    let callable = crate::gometry_lib_module(py)?
        .getattr(pyo3::intern!(py, "_unpickle_geometry"))?
        .unbind();
    let args = (
        PyBytes::new(py, &payload.wkb),
        payload.crs.as_deref(),
        payload.epoch,
    )
        .into_py_any(py)?;
    Ok((callable, args))
}

/// Detached payload planning for `GeometryArray.__reduce__`.
pub(crate) fn reduce_geometry_array(
    py: Python<'_>,
    array: &PyGeometryArray,
) -> PyResult<(Py<PyAny>, Py<PyAny>)> {
    let storage = Arc::clone(array.storage_arc());
    let missing = array.missing().cloned();
    let point_layout = array.point_row_layout();
    let crs = array.crs_str().map(str::to_owned);
    let epoch = array.epoch();
    let payload = py
        .detach(move || {
            array_pickle_payload_detached(
                storage.as_ref(),
                missing.as_ref(),
                point_layout,
                crs,
                epoch,
            )
        })
        .map_err(PyErr::from)?;
    materialize_array_pickle_payload(py, payload)
}

#[expect(
    clippy::too_many_lines,
    reason = "one payload planner owns the storage and missing-row encoding matrix"
)]
fn array_pickle_payload_detached(
    storage: &GeometryArrayStorage,
    missing: Option<&ValidityMask>,
    point_layout: PointRowLayout,
    crs: Option<String>,
    epoch: Option<f64>,
) -> crate::error::Result<ArrayPicklePayload> {
    let missing_bytes = missing.map(try_missing_bytes).transpose()?;
    match (storage, missing) {
        (GeometryArrayStorage::Mixed(_), missing) => Ok(ArrayPicklePayload::Mixed {
            rows: wkb_rows_for_mask(storage, missing, point_layout)?,
            crs,
            epoch,
            missing: missing_bytes,
        }),
        (GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map }), None) => {
            let selection = row_map.as_deref();
            if points_are_finite(coords, selection) {
                let (xs, ys, zs, ms) = encode_points_selection(coords, selection)?;
                Ok(ArrayPicklePayload::Points {
                    xs,
                    ys,
                    zs,
                    ms,
                    crs,
                    epoch,
                    row_map: None,
                    missing: None,
                })
            } else {
                Ok(ArrayPicklePayload::Mixed {
                    rows: wkb_rows_for_mask(storage, None, point_layout)?,
                    crs,
                    epoch,
                    missing: None,
                })
            }
        },
        (GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map }), Some(mask)) => {
            let selection = row_map.as_deref();
            let row_aligned =
                !point_layout.is_compact() && mask.len() == selection.len(coords.len());
            let encoded = if row_aligned {
                let indices = selected_storage_indices(selection, coords.len(), Some(mask))?;
                if !points_are_finite(coords, RowSelectionRef::Gather(&indices)) {
                    return Ok(ArrayPicklePayload::Mixed {
                        rows: wkb_rows_for_mask(storage, Some(mask), point_layout)?,
                        crs,
                        epoch,
                        missing: missing_bytes,
                    });
                }
                encode_points(coords, &indices)?
            } else {
                if !points_are_finite(coords, selection) {
                    return Ok(ArrayPicklePayload::Mixed {
                        rows: wkb_rows_for_mask(storage, Some(mask), point_layout)?,
                        crs,
                        epoch,
                        missing: missing_bytes,
                    });
                }
                encode_points_selection(coords, selection)?
            };
            let (xs, ys, zs, ms) = encoded;
            Ok(ArrayPicklePayload::Points {
                xs,
                ys,
                zs,
                ms,
                crs,
                epoch,
                row_map: None,
                missing: missing_bytes,
            })
        },
        (
            GeometryArrayStorage::Packed(PackedStorage::Lines {
                coords,
                offsets,
                row_map,
            }),
            mask,
        ) => {
            let windows = selected_line_windows(offsets, row_map.as_deref(), mask)?;
            if columns_are_finite(coords, &windows) {
                let (xs, ys, zs, ms) = encode_windows(coords, &windows)?;
                Ok(ArrayPicklePayload::Lines {
                    xs,
                    ys,
                    zs,
                    ms,
                    offsets: rebased_i32_offsets_le_bytes(
                        windows.iter().map(std::ops::Range::len),
                        windows.len(),
                    )?,
                    crs,
                    epoch,
                    row_map: None,
                    missing: missing_bytes,
                })
            } else {
                Ok(ArrayPicklePayload::Mixed {
                    rows: wkb_rows_for_mask(storage, mask, point_layout)?,
                    crs,
                    epoch,
                    missing: missing_bytes,
                })
            }
        },
        (
            GeometryArrayStorage::Packed(PackedStorage::Polygons {
                coords,
                ring_offsets,
                polygon_offsets,
                row_map,
            }),
            mask,
        ) => {
            let rows = selected_storage_rows(
                row_map.as_deref(),
                polygon_offsets.len().saturating_sub(1),
                mask,
            )?;
            let mut windows = Vec::new();
            let mut polygon_ring_counts = Vec::new();
            crate::boundary::try_reserve_vec(&mut windows, rows.len())?;
            crate::boundary::try_reserve_vec(&mut polygon_ring_counts, rows.len())?;
            for &row in &rows {
                let rings = row_map.as_deref().csr_window(polygon_offsets, row);
                crate::boundary::try_push_vec(&mut polygon_ring_counts, rings.len())?;
                for ring in rings {
                    crate::boundary::try_push_vec(
                        &mut windows,
                        ring_offsets[ring] as usize..ring_offsets[ring + 1] as usize,
                    )?;
                }
            }
            if columns_are_finite(coords, &windows) {
                let (xs, ys, zs, ms) = encode_windows(coords, &windows)?;
                Ok(ArrayPicklePayload::Polygons {
                    xs,
                    ys,
                    zs,
                    ms,
                    ring_offsets: rebased_i32_offsets_le_bytes(
                        windows.iter().map(std::ops::Range::len),
                        windows.len(),
                    )?,
                    polygon_offsets: rebased_i32_offsets_le_bytes(
                        polygon_ring_counts.into_iter(),
                        rows.len(),
                    )?,
                    crs,
                    epoch,
                    row_map: None,
                    missing: missing_bytes,
                })
            } else {
                Ok(ArrayPicklePayload::Mixed {
                    rows: wkb_rows_for_mask(storage, mask, point_layout)?,
                    crs,
                    epoch,
                    missing: missing_bytes,
                })
            }
        },
    }
}

fn try_missing_bytes(mask: &ValidityMask) -> crate::error::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    crate::boundary::try_reserve_vec(&mut bytes, mask.len())?;
    for missing in mask.iter() {
        crate::boundary::try_push_vec(&mut bytes, u8::from(missing))?;
    }
    Ok(bytes)
}

fn selected_storage_rows(
    row_map: RowSelectionRef<'_>,
    physical_rows: usize,
    missing: Option<&ValidityMask>,
) -> crate::error::Result<Vec<usize>> {
    let logical_rows = row_map.len(physical_rows);
    let mut rows = Vec::new();
    crate::boundary::try_reserve_vec(&mut rows, logical_rows)?;
    for logical in 0..logical_rows {
        if missing.is_some_and(|mask| mask.len() == logical_rows && mask.is_missing(logical)) {
            continue;
        }
        crate::boundary::try_push_vec(&mut rows, logical)?;
    }
    Ok(rows)
}

fn selected_line_windows(
    offsets: &[i32],
    row_map: RowSelectionRef<'_>,
    missing: Option<&ValidityMask>,
) -> crate::error::Result<Vec<std::ops::Range<usize>>> {
    let logical_rows = row_map.len(offsets.len().saturating_sub(1));
    let mut windows = Vec::new();
    crate::boundary::try_reserve_vec(&mut windows, logical_rows)?;
    for logical in 0..logical_rows {
        if missing.is_some_and(|mask| mask.len() == logical_rows && mask.is_missing(logical)) {
            continue;
        }
        crate::boundary::try_push_vec(&mut windows, row_map.csr_window(offsets, logical))?;
    }
    Ok(windows)
}

fn selected_storage_indices(
    row_map: RowSelectionRef<'_>,
    physical_rows: usize,
    missing: Option<&ValidityMask>,
) -> crate::error::Result<Vec<usize>> {
    let mut indices = Vec::new();
    let logical_rows = row_map.len(physical_rows);
    crate::boundary::try_reserve_vec(&mut indices, logical_rows)?;
    for logical in 0..logical_rows {
        if missing.is_some_and(|mask| mask.len() == logical_rows && mask.is_missing(logical)) {
            continue;
        }
        crate::boundary::try_push_vec(&mut indices, row_map.physical(logical))?;
    }
    Ok(indices)
}

fn points_are_finite(coords: &CoordSeq, selection: RowSelectionRef<'_>) -> bool {
    (0..selection.len(coords.len())).all(|logical| {
        let index = selection.physical(logical);
        coords.xs()[index].is_finite()
            && coords.ys()[index].is_finite()
            && coords.zs().is_none_or(|values| values[index].is_finite())
            && coords.ms().is_none_or(|values| values[index].is_finite())
    })
}

fn columns_are_finite(coords: &CoordSeq, windows: &[std::ops::Range<usize>]) -> bool {
    windows.iter().all(|window| {
        coords.xs()[window.clone()]
            .iter()
            .all(|value| value.is_finite())
            && coords.ys()[window.clone()]
                .iter()
                .all(|value| value.is_finite())
            && coords
                .zs()
                .is_none_or(|values| values[window.clone()].iter().all(|value| value.is_finite()))
            && coords
                .ms()
                .is_none_or(|values| values[window.clone()].iter().all(|value| value.is_finite()))
    })
}

fn encode_points(
    coords: &CoordSeq,
    indices: &[usize],
) -> crate::error::Result<EncodedPointColumns> {
    let selection = RowSelectionRef::Gather(indices);
    let xs = selected_f64_column_le_bytes(coords.xs(), selection)?;
    let ys = selected_f64_column_le_bytes(coords.ys(), selection)?;
    let zs = coords
        .zs()
        .map(|values| selected_f64_column_le_bytes(values, selection))
        .transpose()?;
    let ms = coords
        .ms()
        .map(|values| selected_f64_column_le_bytes(values, selection))
        .transpose()?;
    Ok((xs, ys, zs, ms))
}

fn encode_points_selection(
    coords: &CoordSeq,
    selection: RowSelectionRef<'_>,
) -> crate::error::Result<EncodedPointColumns> {
    let xs = selected_f64_column_le_bytes(coords.xs(), selection)?;
    let ys = selected_f64_column_le_bytes(coords.ys(), selection)?;
    let zs = coords
        .zs()
        .map(|values| selected_f64_column_le_bytes(values, selection))
        .transpose()?;
    let ms = coords
        .ms()
        .map(|values| selected_f64_column_le_bytes(values, selection))
        .transpose()?;
    Ok((xs, ys, zs, ms))
}

fn encode_windows(
    coords: &CoordSeq,
    windows: &[std::ops::Range<usize>],
) -> crate::error::Result<EncodedPointColumns> {
    let value_count = windows.iter().try_fold(0_usize, |total, window| {
        total
            .checked_add(window.len())
            .ok_or_else(|| pickle_size_overflow("coordinate count overflows usize"))
    })?;
    let xs = selected_f64_windows_le_bytes(coords.xs(), windows.iter().cloned(), value_count)?;
    let ys = selected_f64_windows_le_bytes(coords.ys(), windows.iter().cloned(), value_count)?;
    let zs = coords
        .zs()
        .map(|values| selected_f64_windows_le_bytes(values, windows.iter().cloned(), value_count))
        .transpose()?;
    let ms = coords
        .ms()
        .map(|values| selected_f64_windows_le_bytes(values, windows.iter().cloned(), value_count))
        .transpose()?;
    Ok((xs, ys, zs, ms))
}

fn wkb_rows_for_mask(
    storage: &GeometryArrayStorage,
    missing: Option<&ValidityMask>,
    point_layout: PointRowLayout,
) -> crate::error::Result<Vec<Vec<u8>>> {
    let row_count = missing.map_or(storage.len(), ValidityMask::len);
    let compact = missing.is_some_and(|mask| match storage {
        GeometryArrayStorage::Packed(PackedStorage::Points { .. }) => point_layout.is_compact(),
        GeometryArrayStorage::Packed(
            PackedStorage::Lines { .. } | PackedStorage::Polygons { .. },
        ) => storage.len() != mask.len(),
        GeometryArrayStorage::Mixed(_) => false,
    });
    let mut rows = Vec::new();
    crate::boundary::try_reserve_vec(&mut rows, row_count)?;
    let mut present = 0;
    for logical in 0..row_count {
        if missing.is_some_and(|mask| mask.is_missing(logical)) {
            crate::boundary::try_push_vec(&mut rows, Vec::new())?;
            continue;
        }
        let payload_row = if compact { present } else { logical };
        present += 1;
        let wkb = storage
            .row(payload_row)
            .try_with_shape(|shape| crate::io::to_wkb(shape, None, false))?;
        crate::boundary::try_push_vec(&mut rows, wkb)?;
    }
    Ok(rows)
}

fn materialize_array_pickle_payload(
    py: Python<'_>,
    payload: ArrayPicklePayload,
) -> PyResult<(Py<PyAny>, Py<PyAny>)> {
    let (name, args) = match payload {
        ArrayPicklePayload::Mixed {
            rows,
            crs,
            epoch,
            missing,
        } => {
            let mut py_rows = Vec::new();
            crate::boundary::try_reserve_vec(&mut py_rows, rows.len()).map_err(PyErr::from)?;
            for row in rows {
                crate::boundary::try_push_vec(&mut py_rows, PyBytes::new(py, &row))
                    .map_err(PyErr::from)?;
            }
            (
                "_unpickle_geometry_array",
                (py_rows, crs.as_deref(), epoch, py_bytes_opt(py, missing)).into_py_any(py)?,
            )
        },
        ArrayPicklePayload::Points {
            xs,
            ys,
            zs,
            ms,
            crs,
            epoch,
            row_map,
            missing,
        } => (
            "_unpickle_point_array",
            (
                PyBytes::new(py, &xs),
                PyBytes::new(py, &ys),
                py_bytes_opt(py, zs),
                py_bytes_opt(py, ms),
                crs.as_deref(),
                epoch,
                py_bytes_opt(py, row_map),
                py_bytes_opt(py, missing),
            )
                .into_py_any(py)?,
        ),
        ArrayPicklePayload::Lines {
            xs,
            ys,
            zs,
            ms,
            offsets,
            crs,
            epoch,
            row_map,
            missing,
        } => (
            "_unpickle_line_array",
            (
                PyBytes::new(py, &xs),
                PyBytes::new(py, &ys),
                py_bytes_opt(py, zs),
                py_bytes_opt(py, ms),
                PyBytes::new(py, &offsets),
                crs.as_deref(),
                epoch,
                py_bytes_opt(py, row_map),
                py_bytes_opt(py, missing),
            )
                .into_py_any(py)?,
        ),
        ArrayPicklePayload::Polygons {
            xs,
            ys,
            zs,
            ms,
            ring_offsets,
            polygon_offsets,
            crs,
            epoch,
            row_map,
            missing,
        } => (
            "_unpickle_polygon_array",
            (
                PyBytes::new(py, &xs),
                PyBytes::new(py, &ys),
                py_bytes_opt(py, zs),
                py_bytes_opt(py, ms),
                PyBytes::new(py, &ring_offsets),
                PyBytes::new(py, &polygon_offsets),
                crs.as_deref(),
                epoch,
                py_bytes_opt(py, row_map),
                py_bytes_opt(py, missing),
            )
                .into_py_any(py)?,
        ),
    };
    let callable = crate::gometry_lib_module(py)?.getattr(name)?.unbind();
    Ok((callable, args))
}

#[expect(
    clippy::single_option_map,
    reason = "this boundary owns optional bytes before constructing a Python object"
)]
fn py_bytes_opt(py: Python<'_>, bytes: Option<Vec<u8>>) -> Option<Bound<'_, PyBytes>> {
    bytes.map(|bytes| PyBytes::new(py, &bytes))
}

fn unpickle_geometry_detached(
    wkb: &[u8],
    crs: Option<String>,
    epoch: Option<f64>,
) -> crate::error::Result<PyGeometry> {
    let parsed = crate::io::parse_wkb(wkb)?;
    let embedded = crate::io::crs_from_optional_srid(parsed.srid)?;
    let fallback = deserialized_crs(crs)?;
    guard_embedded_crs_conflict(embedded.as_deref(), fallback.as_deref(), "EWKB SRID")?;
    let crs = embedded.or(fallback);
    let epoch = deserialized_epoch(epoch, crs.as_deref())?;
    PyGeometry::with_frame(parsed.shape, Frame::new(crs, epoch)?)
}

fn unpickle_geometry_array_detached(
    rows: Vec<Vec<u8>>,
    crs: Option<String>,
    epoch: Option<f64>,
    missing: Option<Vec<u8>>,
) -> crate::error::Result<PyGeometryArray> {
    let fallback = deserialized_crs(crs)?;
    let mask = pickled_missing_mask(rows.len(), missing.as_deref(), "mixed-array", None)?;
    let mut parsed_rows = Vec::new();
    crate::boundary::try_reserve_vec(&mut parsed_rows, rows.len())?;
    let mut frame_admit = SridFrameAdmission::new(fallback, None);
    for (row, wkb) in rows.iter().enumerate() {
        if mask.as_ref().is_some_and(|mask| mask.is_missing(row)) {
            crate::boundary::try_push_vec(&mut parsed_rows, None)?;
            continue;
        }
        let parsed = crate::io::parse_wkb(wkb)?;
        frame_admit.admit_srid(parsed.srid, row, "geometry array pickle", "EWKB SRID")?;
        crate::boundary::try_push_vec(&mut parsed_rows, Some(parsed.shape))?;
    }
    let shared = frame_admit.finish()?;
    let epoch = deserialized_epoch(epoch, shared.as_deref())?;
    let frame = Frame::new(shared, epoch)?;
    let mut shapes = Vec::new();
    crate::boundary::try_reserve_vec(&mut shapes, parsed_rows.len())?;
    for parsed in parsed_rows {
        crate::boundary::try_push_vec(
            &mut shapes,
            parsed.unwrap_or_else(PyGeometryArray::missing_placeholder),
        )?;
    }
    PyGeometryArray::mixed_shapes(shapes, frame)?.with_missing_mask(mask)
}

fn unpickle_point_array_detached(
    xs: Vec<u8>,
    ys: Vec<u8>,
    zs: Option<Vec<u8>>,
    ms: Option<Vec<u8>>,
    crs: Option<String>,
    epoch: Option<f64>,
    row_map: Option<Vec<u8>>,
    missing: Option<Vec<u8>>,
) -> crate::error::Result<PyGeometryArray> {
    let seq = pickled_coordseq(&xs, &ys, zs.as_deref(), ms.as_deref())?;
    let crs = deserialized_crs(crs)?;
    let epoch = deserialized_epoch(epoch, crs.as_deref())?;
    let frame = Frame::new(crs, epoch)?;
    let physical_rows = seq.len();
    let row_map = pickled_usize_row_map(row_map.as_deref(), physical_rows, "point-array")?;
    let present = PyGeometryArray::packed_points_mapped(seq, frame, row_map)?;
    let present_count = present.len();
    let mask_len = missing.as_ref().map_or(present_count, Vec::len);
    if let Some(mask) = pickled_missing_mask(
        mask_len,
        missing.as_deref(),
        "point-array",
        Some(present_count),
    )? {
        Ok(PyGeometryArray::scatter_present_rows(&present, mask)?)
    } else {
        Ok(present)
    }
}

fn unpickle_line_array_detached(
    xs: Vec<u8>,
    ys: Vec<u8>,
    zs: Option<Vec<u8>>,
    ms: Option<Vec<u8>>,
    offsets: Vec<u8>,
    crs: Option<String>,
    epoch: Option<f64>,
    row_map: Option<Vec<u8>>,
    missing: Option<Vec<u8>>,
) -> crate::error::Result<PyGeometryArray> {
    let seq = pickled_coordseq(&xs, &ys, zs.as_deref(), ms.as_deref())?;
    let offsets = pickled_i32_csr_offsets(&offsets, "line-array")?;
    let total = i32::try_from(seq.len())
        .map_err(|_| pickle_error("line-array pickle exceeds the offset domain"))?;
    let valid_csr = offsets[0] == 0
        && *offsets.last().expect("non-empty offsets") == total
        && offsets.is_sorted()
        && offsets
            .array_windows::<2>()
            .all(|[left, right]| right - left != 1);
    if !valid_csr {
        return Err(pickle_error(
            "line-array pickle offsets are not a valid line CSR",
        ));
    }
    let crs = deserialized_crs(crs)?;
    let epoch = deserialized_epoch(epoch, crs.as_deref())?;
    let frame = Frame::new(crs, epoch)?;
    let offsets = crate::geometry::CsrOffsetColumn::try_from_shared_i32(
        crate::geometry::SharedSlice::try_from_vec(offsets)?,
        seq.len(),
    )
    .map_err(|_| pickle_error("line-array pickle offsets are not a valid line CSR"))?;
    let physical_rows = offsets.len().saturating_sub(1);
    let row_map = pickled_usize_row_map(row_map.as_deref(), physical_rows, "line-array")?;
    let present = PyGeometryArray::packed_lines_mapped(seq, offsets, frame, row_map)?;
    let present_count = present.len();
    let mask_len = missing.as_ref().map_or(present_count, Vec::len);
    if let Some(mask) = pickled_missing_mask(
        mask_len,
        missing.as_deref(),
        "line-array",
        Some(present_count),
    )? {
        Ok(PyGeometryArray::scatter_present_rows(&present, mask)?)
    } else {
        Ok(present)
    }
}

fn unpickle_polygon_array_detached(
    xs: Vec<u8>,
    ys: Vec<u8>,
    zs: Option<Vec<u8>>,
    ms: Option<Vec<u8>>,
    ring_offsets: Vec<u8>,
    polygon_offsets: Vec<u8>,
    crs: Option<String>,
    epoch: Option<f64>,
    row_map: Option<Vec<u8>>,
    missing: Option<Vec<u8>>,
) -> crate::error::Result<PyGeometryArray> {
    let seq = pickled_coordseq(&xs, &ys, zs.as_deref(), ms.as_deref())?;
    let ring_offsets = pickled_i32_csr_offsets(&ring_offsets, "polygon-array ring")?;
    let total = i32::try_from(seq.len())
        .map_err(|_| pickle_error("polygon-array pickle exceeds the offset domain"))?;
    let valid_ring_csr = ring_offsets[0] == 0
        && *ring_offsets.last().expect("non-empty offsets") == total
        && ring_offsets.is_sorted();
    if !valid_ring_csr {
        return Err(pickle_error(
            "polygon-array pickle ring offsets are not a valid CSR",
        ));
    }
    let ring_offsets = crate::geometry::CsrOffsetColumn::try_from_shared_i32(
        crate::geometry::SharedSlice::try_from_vec(ring_offsets)?,
        seq.len(),
    )
    .map_err(|_| pickle_error("polygon-array pickle ring offsets are not a valid CSR"))?;
    let ring_count = ring_offsets.len().saturating_sub(1);
    let polygon_offsets = pickled_i32_csr_offsets(&polygon_offsets, "polygon-array polygon")?;
    let ring_count_i32 = i32::try_from(ring_count)
        .map_err(|_| pickle_error("polygon-array pickle exceeds the offset domain"))?;
    let valid_polygon_csr = polygon_offsets[0] == 0
        && *polygon_offsets.last().expect("non-empty offsets") == ring_count_i32
        && polygon_offsets.is_sorted();
    if !valid_polygon_csr {
        return Err(pickle_error(
            "polygon-array pickle polygon offsets are not a valid CSR",
        ));
    }
    let polygon_offsets = crate::geometry::CsrOffsetColumn::try_from_shared_i32(
        crate::geometry::SharedSlice::try_from_vec(polygon_offsets)?,
        ring_count,
    )
    .map_err(|_| pickle_error("polygon-array pickle polygon offsets are not a valid CSR"))?;
    let (seq, ring_offsets) = admit_pickled_polygon_rings(seq, ring_offsets)?;
    validate_pickled_polygon_shells(&polygon_offsets)?;
    let crs = deserialized_crs(crs)?;
    let epoch = deserialized_epoch(epoch, crs.as_deref())?;
    let frame = Frame::new(crs, epoch)?;
    let physical_rows = polygon_offsets.len().saturating_sub(1);
    let row_map = pickled_usize_row_map(row_map.as_deref(), physical_rows, "polygon-array")?;
    let present = PyGeometryArray::packed_polygons_mapped(
        seq,
        ring_offsets,
        polygon_offsets,
        frame,
        row_map,
    )?;
    let present_count = present.len();
    let mask_len = missing.as_ref().map_or(present_count, Vec::len);
    if let Some(mask) = pickled_missing_mask(
        mask_len,
        missing.as_deref(),
        "polygon-array",
        Some(present_count),
    )? {
        Ok(PyGeometryArray::scatter_present_rows(&present, mask)?)
    } else {
        Ok(present)
    }
}
