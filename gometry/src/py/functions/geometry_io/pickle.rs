use std::sync::Arc;

use pyo3::prelude::*;

use super::guard_embedded_crs_conflict;
use crate::array::{MissingMask, RowSelection};
use crate::py::errors::{CRSError, GeometryError};
use crate::*;

// --- Pickle support ----------------------------------------------------------
//
// `__reduce__` payloads round-trip geometry rows through plain WKB (Z/M
// preserved; the frame travels separately as the canonical CRS string plus
// epoch) and packed point/line/polygon arrays through their raw f64 columns
// (plus CSR offsets for lineal storage). Data types
// pickle; derived engines (spatial indexes, prepared geometries, coverages)
// are rebuilt instead, like their ecosystem counterparts.

/// One logical-row map as little-endian `u64` indices (the
/// architecture-independent `__reduce__` payload for permuted packed arrays).
pub(crate) fn usize_row_map_le_bytes(values: &[usize]) -> Vec<u8> {
    let widened: Vec<u64> = values
        .iter()
        .map(|&value| u64::try_from(value).expect("row_map index fits u64"))
        .collect();
    if cfg!(target_endian = "little") {
        bytemuck::cast_slice(&widened).to_vec()
    } else {
        let mut out = Vec::with_capacity(widened.len().saturating_mul(size_of::<u64>()));
        for value in widened {
            out.extend_from_slice(&value.to_le_bytes());
        }
        out
    }
}

/// One packed coordinate column as little-endian f64 bytes (the
/// architecture-independent `__reduce__` payload; a memcpy on LE targets).
pub(crate) fn f64_column_le_bytes(values: &[f64]) -> Vec<u8> {
    if cfg!(target_endian = "little") {
        bytemuck::cast_slice(values).to_vec()
    } else {
        let mut out = Vec::with_capacity(std::mem::size_of_val(values));
        for value in values {
            out.extend_from_slice(&value.to_le_bytes());
        }
        out
    }
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
        geometry.set_crs_keep_epoch(fallback);
    }
    let epoch = crate::deserialized_epoch(epoch, geometry.crs_str())?;
    geometry.set_epoch_keep_crs(epoch);
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
    // Resolve one shared array frame FIRST through the same SharedRowCrs
    // admission bulk `from_wkb` uses: an SRID-established frame mixed with a
    // CRS-free row is CRSMismatchError (no silent stamp). Explicit payload
    // `crs=` still covers plain rows (normal pickle round-trip). Epoch
    // validation runs after this scan so EWKB-only CRS + epoch matches bulk
    // `from_wkb(..., epoch=...)` (R16). Masked rows are skipped.
    let mut shared_rows = super::SharedRowCrs::Unseen;
    for (row, wkb) in rows.iter().enumerate() {
        if mask.as_ref().is_some_and(|mask| mask[row]) {
            crate::try_push(&mut parsed_rows, None)?;
            continue;
        }
        let parsed = crate::io::parse_wkb(wkb)?;
        guard_embedded_crs_conflict(parsed.crs.as_deref(), fallback.as_deref(), "EWKB SRID")?;
        let row_crs = match parsed.crs.as_deref() {
            Some(embedded) => {
                let embedded = crate::crs::canonicalize(embedded).map_err(|err| {
                    CRSError::new_err(format!("invalid EWKB SRID in geometry array pickle: {err}"))
                })?;
                Some(embedded)
            },
            None => None,
        };
        shared_rows.admit(row_crs, fallback.as_ref(), row, "geometry array pickle")?;
        crate::try_push(&mut parsed_rows, Some(parsed))?;
    }
    let shared = shared_rows.into_crs(fallback);
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
    let present_count = present.storage().len();
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
    let present_count = present.storage().len();
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
    validate_pickled_polygon_rings(&seq, &ring_offsets)?;
    validate_pickled_polygon_shells(&polygon_offsets)?;
    let crs = deserialized_crs(crs)?;
    let epoch = crate::deserialized_epoch(epoch, crs.as_deref())?;
    let frame = Frame::new(crs, epoch)?;
    let physical_rows = polygon_offsets.len().saturating_sub(1);
    let row_map = pickled_usize_row_map(row_map, physical_rows, "polygon-array")?;
    let present =
        PyGeometryArray::packed_polygons_mapped(seq, ring_offsets, polygon_offsets, frame, row_map);
    let present_count = present.storage().len();
    let mask_len = missing.map_or(present_count, <[u8]>::len);
    if let Some(mask) =
        pickled_missing_mask(mask_len, missing, "polygon-array", Some(present_count))?
    {
        Ok(PyGeometryArray::scatter_present_rows(&present, mask))
    } else {
        Ok(present)
    }
}

/// Untrusted pickle boundary: every ring span must have at least four
/// coordinates and be closed on **every active ordinate** before the trusted
/// packed constructor — the same admission as [`crate::array::ring_seq_is_packable`]
/// / [`same_active_position`] (XY-only closure is not enough for Z/M rings).
/// Zero-coordinate rings are rejected (no `n == 0` exemption).
fn validate_pickled_polygon_rings(
    seq: &crate::geometry::CoordSeq,
    ring_offsets: &crate::geometry::CsrOffsetColumn<crate::geometry::RingLevel>,
) -> PyResult<()> {
    use crate::geometry::{Ring, same_active_position};
    let width = seq.len();
    for [start, end] in ring_offsets.array_windows::<2>() {
        let start = usize::try_from(*start).map_err(|_| {
            GeometryError::new_err("polygon-array pickle ring offsets are not a valid CSR")
        })?;
        let end = usize::try_from(*end).map_err(|_| {
            GeometryError::new_err("polygon-array pickle ring offsets are not a valid CSR")
        })?;
        if end < start || end > width {
            return Err(GeometryError::new_err(
                "polygon-array pickle ring offsets are not a valid CSR",
            ));
        }
        let n = end - start;
        if n < Ring::MIN_VERTICES_CLOSED {
            return Err(GeometryError::new_err(
                "polygon-array pickle ring is shorter than four coordinates",
            ));
        }
        // Full active Point (X/Y/Z/M) — matches pack_admission::ring_seq_is_packable.
        let first = seq.point_at(start);
        let last = seq.point_at(end - 1);
        if !same_active_position(first, last) {
            return Err(GeometryError::new_err(
                "polygon-array pickle ring must be closed",
            ));
        }
    }
    Ok(())
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
    let map = bytes
        .as_chunks::<8>()
        .0
        .iter()
        .map(|chunk| {
            let value = u64::from_le_bytes(*chunk);
            usize::try_from(value).map_err(|_| {
                GeometryError::new_err(format!("{label} pickle row_map index out of range"))
            })
        })
        .collect::<PyResult<Vec<_>>>()?;
    RowSelection::gather_checked(map.into(), physical_rows, &format!("{label} pickle"))
}

/// Decode little-endian `i32` CSR offsets from a packed-array pickle payload.
fn pickled_i32_csr_offsets(bytes: &[u8], label: &str) -> PyResult<Vec<i32>> {
    if !bytes.len().is_multiple_of(size_of::<i32>()) || bytes.len() < size_of::<i32>() {
        return Err(GeometryError::new_err(format!(
            "malformed {label} pickle offsets"
        )));
    }
    Ok(bytes
        .as_chunks::<4>()
        .0
        .iter()
        .map(|chunk| i32::from_le_bytes(*chunk))
        .collect())
}

fn pickled_missing_mask(
    len: usize,
    missing: Option<&[u8]>,
    label: &str,
    present_count: Option<usize>,
) -> PyResult<Option<MissingMask>> {
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
    let mask = MissingMask::from_vec(len, bytes.iter().map(|&byte| byte != 0).collect());
    let decoded_present_count = mask.as_ref().map_or(len, MissingMask::present_count);
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
fn pickled_coordseq(
    xs: &[u8],
    ys: &[u8],
    zs: Option<&[u8]>,
    ms: Option<&[u8]>,
) -> PyResult<CoordSeq> {
    let column = |bytes: &[u8]| -> PyResult<Vec<f64>> {
        if !bytes.len().is_multiple_of(size_of::<f64>()) {
            return Err(GeometryError::new_err(
                "malformed packed-array pickle column",
            ));
        }
        Ok(bytes
            .as_chunks::<8>()
            .0
            .iter()
            .map(|chunk| f64::from_le_bytes(*chunk))
            .collect())
    };
    let xs = column(xs)?;
    let ys = column(ys)?;
    let zs = zs.map(column).transpose()?;
    let ms = ms.map(column).transpose()?;
    if xs.len() != ys.len()
        || zs.as_ref().is_some_and(|column| column.len() != xs.len())
        || ms.as_ref().is_some_and(|column| column.len() != xs.len())
    {
        return Err(GeometryError::new_err(
            "packed-array pickle columns differ in length",
        ));
    }
    Ok(CoordSeq::from_owned_columns(xs, ys, zs, ms)?)
}
