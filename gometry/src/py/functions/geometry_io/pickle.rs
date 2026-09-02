use std::sync::Arc;

use pyo3::exceptions::PyOverflowError;
use pyo3::prelude::*;

use crate::array::{RowSelection, RowSelectionRef, ValidityMask};
use crate::py::errors::{CRSError, ParseFormat, parse_error};
use crate::py::wire_crs::{SridFrameAdmission, guard_embedded_crs_conflict};
use crate::{CoordSeq, Crs, Frame, PyGeometry, PyGeometryArray, Typed, parse_wkb_geometry};

/// Pickle is trusted-code persistence, but malformed payloads still must not
/// enter packed storage. Keep the existing reconstruction checks and classify
/// their failures as serialized-input parse errors.
struct GeometryError;

impl GeometryError {
    fn new_err(message: impl Into<String>) -> PyErr {
        parse_error(message, ParseFormat::Pickle)
    }
}

// --- Pickle support ----------------------------------------------------------
//
// `__reduce__` payloads round-trip geometry rows through plain WKB (Z/M
// preserved; the frame travels separately as the canonical CRS string plus
// epoch) and packed point/line/polygon arrays through their raw f64 columns
// (plus CSR offsets for lineal storage). Data types
// pickle; derived engines (spatial indexes, prepared geometries, coverages)
// are rebuilt instead, like their ecosystem counterparts.

/// One packed coordinate column as little-endian f64 bytes (the
/// architecture-independent `__reduce__` payload; a memcpy on LE targets).
pub(crate) fn f64_column_le_bytes(values: &[f64]) -> PyResult<Vec<u8>> {
    let byte_len = values
        .len()
        .checked_mul(size_of::<f64>())
        .ok_or_else(|| PyOverflowError::new_err("coordinate byte length overflows usize"))?;
    let mut out = crate::try_vec_with_capacity::<u8>(byte_len)?;
    for &value in values {
        out.extend_from_slice(&value.to_le_bytes());
    }
    Ok(out)
}

/// One selected point coordinate column in logical row order.
pub(crate) fn selected_f64_column_le_bytes(
    values: &[f64],
    selection: RowSelectionRef<'_>,
) -> PyResult<Vec<u8>> {
    let len = selection.len(values.len());
    let byte_len = len
        .checked_mul(size_of::<f64>())
        .ok_or_else(|| PyOverflowError::new_err("coordinate byte length overflows usize"))?;
    let mut out = crate::try_vec_with_capacity::<u8>(byte_len)?;
    for logical in 0..len {
        out.extend_from_slice(&values[selection.physical(logical)].to_le_bytes());
    }
    Ok(out)
}

pub(crate) fn selected_f64_windows_le_bytes(
    values: &[f64],
    windows: impl Iterator<Item = std::ops::Range<usize>>,
    value_count: usize,
) -> PyResult<Vec<u8>> {
    let byte_len = value_count
        .checked_mul(size_of::<f64>())
        .ok_or_else(|| PyOverflowError::new_err("coordinate byte length overflows usize"))?;
    let mut out = crate::try_vec_with_capacity::<u8>(byte_len)?;
    for window in windows {
        for &value in &values[window] {
            out.extend_from_slice(&value.to_le_bytes());
        }
    }
    Ok(out)
}

pub(crate) fn rebased_i32_offsets_le_bytes(
    lengths: impl Iterator<Item = usize>,
    row_count: usize,
) -> PyResult<Vec<u8>> {
    let offset_count = row_count
        .checked_add(1)
        .ok_or_else(|| PyOverflowError::new_err("offset count overflows usize"))?;
    let byte_len = offset_count
        .checked_mul(size_of::<i32>())
        .ok_or_else(|| PyOverflowError::new_err("offset byte length overflows usize"))?;
    let mut out = crate::try_vec_with_capacity::<u8>(byte_len)?;
    out.extend_from_slice(&0_i32.to_le_bytes());
    let mut total = 0_usize;
    for length in lengths {
        total = total
            .checked_add(length)
            .ok_or_else(|| PyOverflowError::new_err("offset value overflows usize"))?;
        let offset = i32::try_from(total)
            .map_err(|_| PyOverflowError::new_err("offset value does not fit in i32"))?;
        out.extend_from_slice(&offset.to_le_bytes());
    }
    Ok(out)
}

pub(crate) fn i32_column_le_bytes(values: &[i32]) -> PyResult<Vec<u8>> {
    let byte_len = values
        .len()
        .checked_mul(size_of::<i32>())
        .ok_or_else(|| PyOverflowError::new_err("offset byte length overflows usize"))?;
    let mut out = crate::try_vec_with_capacity::<u8>(byte_len)?;
    for &value in values {
        out.extend_from_slice(&value.to_le_bytes());
    }
    Ok(out)
}

/// Canonicalize a CRS string arriving from a pickle payload through the same
/// PROJ-backed parser public constructors use. Rejects `"NOT_A_CRS"` at unpickle
/// rather than deferring failure to a later CRS operation.
fn deserialized_crs(crs: Option<String>) -> PyResult<Option<Crs>> {
    match crs {
        None => Ok(None),
        Some(text) => {
            let canonical = crate::crs::canonicalize(&text).map_err(|err| {
                CRSError::new_err(format!("invalid CRS in geometry pickle payload: {err}"))
            })?;
            Ok(Some(canonical))
        },
    }
}

/// Rebuild a pickled `Geometry` (internal; see ``Geometry.__reduce__``).
///
/// Frame strings are canonicalized through PROJ. Embedded EWKB SRIDs reuse the
/// public `from_wkb` reconciliation (conflict with the payload CRS is rejected;
/// an embedded SRID is kept when the payload CRS is absent).
#[pyfunction]
pub(crate) fn _unpickle_geometry(
    wkb: &[u8],
    crs: Option<String>,
    epoch: Option<f64>,
) -> PyResult<Typed> {
    let mut geometry = parse_wkb_geometry(wkb)?;
    let fallback = deserialized_crs(crs)?;
    guard_embedded_crs_conflict(geometry.crs_str(), fallback.as_deref(), "EWKB SRID")?;
    if geometry.crs_ref().is_none() {
        geometry.set_crs_keep_epoch(fallback)?;
    }
    let epoch = crate::deserialized_epoch(epoch, geometry.crs_str())?;
    geometry.set_epoch_keep_crs(epoch)?;
    Ok(Typed(geometry))
}

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
    let _ = py;
    let rows = crate::collect_bytes_rows(rows)?;
    let fallback = deserialized_crs(crs)?;
    let mask = pickled_missing_mask(rows.len(), missing, "mixed-array", None)?;
    let mut parsed_rows = crate::try_vec_with_capacity(rows.len())?;
    // Numeric SRID admission (same as bulk from_wkb): resolve each distinct
    // code once, first-conflict row preserved, explicit payload crs= covers
    // plain rows. Masked rows establish no CRS.
    let mut frame_admit = SridFrameAdmission::new(fallback, None);
    for (row, wkb) in rows.iter().enumerate() {
        if mask.as_ref().is_some_and(|mask| mask.is_missing(row)) {
            crate::try_push(&mut parsed_rows, None)?;
            continue;
        }
        let parsed = crate::io::parse_wkb(wkb)?;
        frame_admit.admit_srid(parsed.srid, row, "geometry array pickle", "EWKB SRID")?;
        crate::try_push(&mut parsed_rows, Some(parsed))?;
    }
    let shared = frame_admit.finish()?;
    let epoch = crate::deserialized_epoch(epoch, shared.as_deref())?;
    let frame = Frame::new(shared, epoch)?;
    let mut items = crate::try_vec_with_capacity(rows.len())?;
    for parsed in parsed_rows {
        if let Some(geometry) = parsed {
            crate::try_push(
                &mut items,
                PyGeometry::with_frame(geometry.shape, frame.clone()),
            )?;
        } else {
            crate::try_push(
                &mut items,
                PyGeometry::with_frame(PyGeometryArray::missing_placeholder(), frame.clone()),
            )?;
        }
    }
    Ok(PyGeometryArray::mixed(items, frame).with_missing_mask(mask))
}

/// Rebuild a pickled packed-point `GeometryArray` from raw f64 columns
/// (internal; see ``GeometryArray.__reduce__``).
///
/// Packed-point payloads carry only finite present rows; an optional missing
/// mask restores their original logical row positions.
#[pyfunction]
#[pyo3(signature = (xs, ys, zs, ms, crs, epoch, row_map, missing))]
pub(crate) fn _unpickle_point_array(
    xs: &[u8],
    ys: &[u8],
    zs: Option<&[u8]>,
    ms: Option<&[u8]>,
    crs: Option<String>,
    epoch: Option<f64>,
    row_map: Option<&[u8]>,
    missing: Option<&[u8]>,
) -> PyResult<PyGeometryArray> {
    let seq = pickled_coordseq(xs, ys, zs, ms)?;
    let crs = deserialized_crs(crs)?;
    let epoch = crate::deserialized_epoch(epoch, crs.as_deref())?;
    let frame = Frame::new(crs, epoch)?;
    let physical_rows = seq.len();
    let row_map = pickled_usize_row_map(row_map, physical_rows, "point-array")?;
    let present = PyGeometryArray::packed_points_mapped(seq, frame, row_map);
    let present_count = present.len();
    let mask_len = missing.map_or(present_count, <[u8]>::len);
    if let Some(mask) = pickled_missing_mask(mask_len, missing, "point-array", Some(present_count))?
    {
        Ok(PyGeometryArray::scatter_present_rows(&present, mask))
    } else {
        Ok(present)
    }
}

/// Rebuild a pickled packed-line `GeometryArray` from raw f64 columns plus
/// little-endian `i32` CSR offsets (internal; see
/// ``GeometryArray.__reduce__``).
#[pyfunction]
pub(crate) fn _unpickle_line_array(
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
    let seq = pickled_coordseq(xs, ys, zs, ms)?;
    let offsets = pickled_i32_csr_offsets(offsets, "line-array")?;
    // Pickle payloads are an untrusted boundary: the offsets must form a
    // CSR over exactly the pickled coordinates, with no 1-vertex rows
    // (the LineString invariant every constructor enforces).
    let total = i32::try_from(seq.len())
        .map_err(|_| GeometryError::new_err("line-array pickle exceeds the offset domain"))?;
    let valid_csr = offsets[0] == 0
        && *offsets.last().expect("non-empty offsets") == total
        && offsets.is_sorted()
        && offsets
            .array_windows::<2>()
            .all(|[left, right]| right - left != 1);
    if !valid_csr {
        return Err(GeometryError::new_err(
            "line-array pickle offsets are not a valid line CSR",
        ));
    }
    let crs = deserialized_crs(crs)?;
    let epoch = crate::deserialized_epoch(epoch, crs.as_deref())?;
    let frame = Frame::new(crs, epoch)?;
    let offsets = crate::geometry::CsrOffsetColumn::try_from_arc_i32(offsets.into(), seq.len())
        .map_err(|_| {
            GeometryError::new_err("line-array pickle offsets are not a valid line CSR")
        })?;
    let physical_rows = offsets.len().saturating_sub(1);
    let row_map = pickled_usize_row_map(row_map, physical_rows, "line-array")?;
    let present = PyGeometryArray::packed_lines_mapped(seq, offsets, frame, row_map);
    let present_count = present.len();
    let mask_len = missing.map_or(present_count, <[u8]>::len);
    if let Some(mask) = pickled_missing_mask(mask_len, missing, "line-array", Some(present_count))?
    {
        Ok(PyGeometryArray::scatter_present_rows(&present, mask))
    } else {
        Ok(present)
    }
}

/// Rebuild a pickled packed-polygon `GeometryArray` from raw f64 columns plus
/// little-endian `i32` two-level CSR offsets (internal; see
/// ``GeometryArray.__reduce__``).
#[pyfunction]
pub(crate) fn _unpickle_polygon_array(
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
    let seq = pickled_coordseq(xs, ys, zs, ms)?;
    let ring_offsets = pickled_i32_csr_offsets(ring_offsets, "polygon-array ring")?;
    let total = i32::try_from(seq.len())
        .map_err(|_| GeometryError::new_err("polygon-array pickle exceeds the offset domain"))?;
    let valid_ring_csr = ring_offsets[0] == 0
        && *ring_offsets.last().expect("non-empty offsets") == total
        && ring_offsets.is_sorted();
    if !valid_ring_csr {
        return Err(GeometryError::new_err(
            "polygon-array pickle ring offsets are not a valid CSR",
        ));
    }
    let ring_offsets =
        crate::geometry::CsrOffsetColumn::try_from_arc_i32(ring_offsets.into(), seq.len())
            .map_err(|_| {
                GeometryError::new_err("polygon-array pickle ring offsets are not a valid CSR")
            })?;
    let ring_count = ring_offsets.len().saturating_sub(1);
    let polygon_offsets = pickled_i32_csr_offsets(polygon_offsets, "polygon-array polygon")?;
    let ring_count_i32 = i32::try_from(ring_count)
        .map_err(|_| GeometryError::new_err("polygon-array pickle exceeds the offset domain"))?;
    let valid_polygon_csr = polygon_offsets[0] == 0
        && *polygon_offsets.last().expect("non-empty offsets") == ring_count_i32
        && polygon_offsets.is_sorted();
    if !valid_polygon_csr {
        return Err(GeometryError::new_err(
            "polygon-array pickle polygon offsets are not a valid CSR",
        ));
    }
    let polygon_offsets =
        crate::geometry::CsrOffsetColumn::try_from_arc_i32(polygon_offsets.into(), ring_count)
            .map_err(|_| {
                GeometryError::new_err("polygon-array pickle polygon offsets are not a valid CSR")
            })?;
    // Shared untrusted ring policy with WKT/WKB: silent-close XY-open rings
    // (may grow the coordinate columns) or reject Z/M-open / too-short.
    let (seq, ring_offsets) = admit_pickled_polygon_rings(seq, ring_offsets)?;
    validate_pickled_polygon_shells(&polygon_offsets)?;
    let crs = deserialized_crs(crs)?;
    let epoch = crate::deserialized_epoch(epoch, crs.as_deref())?;
    let frame = Frame::new(crs, epoch)?;
    let physical_rows = polygon_offsets.len().saturating_sub(1);
    let row_map = pickled_usize_row_map(row_map, physical_rows, "polygon-array")?;
    let present =
        PyGeometryArray::packed_polygons_mapped(seq, ring_offsets, polygon_offsets, frame, row_map);
    let present_count = present.len();
    let mask_len = missing.map_or(present_count, <[u8]>::len);
    if let Some(mask) =
        pickled_missing_mask(mask_len, missing, "polygon-array", Some(present_count))?
    {
        Ok(PyGeometryArray::scatter_present_rows(&present, mask))
    } else {
        Ok(present)
    }
}

/// Untrusted pickle ring boundary — same policy as WKT/WKB
/// ([`crate::io::admit_closed_ring`]):
/// silent-close XY-open rings (≥3 corners), reject Z/M-open and too-short.
/// When any ring is open the coordinate columns and ring CSR are rebuilt so
/// packed storage still holds closed rings only.
fn admit_pickled_polygon_rings(
    seq: crate::geometry::CoordSeq,
    ring_offsets: crate::geometry::CsrOffsetColumn<crate::geometry::RingLevel>,
) -> PyResult<(
    crate::geometry::CoordSeq,
    crate::geometry::CsrOffsetColumn<crate::geometry::RingLevel>,
)> {
    use crate::geometry::CoordSeqBuilder;

    let windows: Vec<(usize, usize)> = ring_offsets
        .array_windows::<2>()
        .map(|[start, end]| (*start as usize, *end as usize))
        .collect();

    // Fast path: every ring already fully closed on active ordinates and long
    // enough — no rebuild (the common gometry-written pickle case).
    let already_closed = windows
        .iter()
        .all(|&(start, end)| crate::array::CheckedRingWindow::checked(&seq, start..end).is_ok());
    if already_closed {
        return Ok((seq, ring_offsets));
    }

    // Rebuild through the shared admitter (silent-close / reject).
    let coord_capacity = seq.len().checked_add(windows.len()).ok_or_else(|| {
        GeometryError::new_err("polygon-array pickle coordinate count overflows usize")
    })?;
    let mut builder =
        CoordSeqBuilder::try_with_capacity(seq.axes(), coord_capacity).map_err(PyErr::from)?;
    let offset_capacity = windows.len().checked_add(1).ok_or_else(|| {
        GeometryError::new_err("polygon-array pickle offset count overflows usize")
    })?;
    let mut new_offsets = Vec::new();
    if let Err(error) = crate::try_reserve_vec(&mut new_offsets, offset_capacity) {
        drop(builder);
        return Err(PyErr::from(error));
    }
    if let Err(error) = crate::try_push_vec(&mut new_offsets, 0) {
        drop(builder);
        return Err(PyErr::from(error));
    }
    for (start, end) in windows {
        let n = end - start;
        let ring_capacity = n.checked_add(1).ok_or_else(|| {
            GeometryError::new_err("polygon-array pickle ring size overflows usize")
        })?;
        let mut ring_builder =
            CoordSeqBuilder::try_with_capacity(seq.axes(), ring_capacity).map_err(PyErr::from)?;
        for index in start..end {
            ring_builder.push_at(&seq, index);
        }
        let ring_seq = ring_builder.finish().map_err(PyErr::from)?;
        let admitted = crate::io::admit_closed_ring(ring_seq).map_err(|error| {
            // Prefer the domain message (closed / short) over a bare GeometryError.
            GeometryError::new_err(error.to_string())
        })?;
        let coords = admitted.coords();
        for index in 0..coords.len() {
            builder.push_at(coords, index);
        }
        let next = i32::try_from(builder.len()).map_err(|_| {
            GeometryError::new_err("polygon-array pickle exceeds the offset domain")
        })?;
        if let Err(error) = crate::try_push_vec(&mut new_offsets, next) {
            drop(builder);
            return Err(PyErr::from(error));
        }
    }
    let seq = builder.finish().map_err(PyErr::from)?;
    let ring_offsets = crate::geometry::CsrOffsetColumn::try_from_arc_i32(
        std::sync::Arc::<[i32]>::from(new_offsets),
        seq.len(),
    )
    .map_err(|_| GeometryError::new_err("polygon-array pickle ring offsets are not a valid CSR"))?;
    Ok((seq, ring_offsets))
}

/// Every polygon CSR window must cover at least one ring (`end > start`).
/// Zero-row arrays keep `[0]` / `[0]` (no windows) as the only empty case.
fn validate_pickled_polygon_shells(
    polygon_offsets: &crate::geometry::CsrOffsetColumn<crate::geometry::PolygonLevel>,
) -> PyResult<()> {
    for [start, end] in polygon_offsets.array_windows::<2>() {
        if end <= start {
            return Err(GeometryError::new_err(
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
) -> PyResult<RowSelection> {
    let Some(bytes) = bytes else {
        return Ok(RowSelection::Identity);
    };
    if !bytes.len().is_multiple_of(size_of::<u64>()) {
        return Err(GeometryError::new_err(format!(
            "malformed {label} pickle row_map"
        )));
    }
    if bytes.is_empty() {
        return RowSelection::gather_checked(
            Arc::<[usize]>::from([]),
            physical_rows,
            &format!("{label} pickle"),
        );
    }
    let map = crate::try_collect_results(bytes.as_chunks::<8>().0.iter().map(|chunk| {
        let value = u64::from_le_bytes(*chunk);
        usize::try_from(value).map_err(|_| {
            GeometryError::new_err(format!("{label} pickle row_map index out of range"))
        })
    }))?;
    RowSelection::gather_checked(map.into(), physical_rows, &format!("{label} pickle"))
}

/// Decode little-endian `i32` CSR offsets from a packed-array pickle payload.
fn pickled_i32_csr_offsets(bytes: &[u8], label: &str) -> PyResult<Vec<i32>> {
    if !bytes.len().is_multiple_of(size_of::<i32>()) || bytes.len() < size_of::<i32>() {
        return Err(GeometryError::new_err(format!(
            "malformed {label} pickle offsets"
        )));
    }
    let mut offsets = crate::try_vec_with_capacity(bytes.len() / size_of::<i32>())?;
    for chunk in bytes.as_chunks::<4>().0 {
        crate::try_push(&mut offsets, i32::from_le_bytes(*chunk))?;
    }
    Ok(offsets)
}

fn pickled_missing_mask(
    len: usize,
    missing: Option<&[u8]>,
    label: &str,
    present_count: Option<usize>,
) -> PyResult<Option<ValidityMask>> {
    let Some(bytes) = missing else {
        return Ok(None);
    };
    if bytes.len() != len {
        return Err(GeometryError::new_err(format!(
            "malformed {label} pickle missing mask"
        )));
    }
    // Untrusted boundary: only exact 0/1 bytes are valid mask entries.
    if bytes.iter().any(|&byte| byte > 1) {
        return Err(GeometryError::new_err(format!(
            "malformed {label} pickle missing mask"
        )));
    }
    let mut values = crate::try_vec_with_capacity(len)?;
    for &byte in bytes {
        crate::try_push(&mut values, byte != 0)?;
    }
    let mask = ValidityMask::from_vec(len, values);
    let decoded_present_count = mask.as_ref().map_or(len, ValidityMask::present_count);
    if present_count.is_some_and(|present_count| decoded_present_count != present_count) {
        return Err(GeometryError::new_err(format!(
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
) -> PyResult<CoordSeq> {
    use std::sync::Arc;

    let column = |bytes: &[u8]| -> crate::error::Result<Arc<[f64]>> {
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
        Ok(values.into())
    };
    for bytes in [Some(xs), Some(ys), zs, ms].into_iter().flatten() {
        if !bytes.len().is_multiple_of(size_of::<f64>()) {
            return Err(GeometryError::new_err(
                "malformed packed-array pickle column",
            ));
        }
    }
    let xs = column(xs).map_err(PyErr::from)?;
    let ys = match column(ys) {
        Ok(values) => values,
        Err(error) => {
            drop(xs);
            return Err(PyErr::from(error));
        },
    };
    let zs = match zs.map(column).transpose() {
        Ok(values) => values,
        Err(error) => {
            drop((xs, ys));
            return Err(PyErr::from(error));
        },
    };
    let ms = match ms.map(column).transpose() {
        Ok(values) => values,
        Err(error) => {
            drop((xs, ys, zs));
            return Err(PyErr::from(error));
        },
    };
    if xs.len() != ys.len()
        || zs.as_ref().is_some_and(|column| column.len() != xs.len())
        || ms.as_ref().is_some_and(|column| column.len() != xs.len())
    {
        return Err(GeometryError::new_err(
            "packed-array pickle columns differ in length",
        ));
    }
    Ok(CoordSeq::from_arc_columns(xs, ys, zs, ms)?)
}
