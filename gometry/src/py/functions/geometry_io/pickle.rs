use std::sync::Arc;

use pyo3::IntoPyObjectExt as _;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyTuple};

use crate::array::{
    CheckedRingWindow, GeometryArrayStorage, PackedStorage, PointRowLayout, RowSelectionRef,
    ValidityMask,
};
use crate::geometry::{CsrOffsetColumn, SharedSlice};
use crate::{CoordSeq, Crs, Frame, PyGeometry, PyGeometryArray, ShapeData, Typed};

type EncodedPointColumns<T = Vec<u8>> = (T, T, Option<T>, Option<T>);

fn pickle_error(message: impl Into<Box<str>>) -> crate::error::Error {
    crate::geometry::GeometryErrorKind::pickle_payload(message)
}

fn pickle_size_overflow(message: &'static str) -> crate::error::Error {
    crate::error::ResourceError::SizeOverflowContext(message).into()
}

// --- Pickle support ----------------------------------------------------------
//
// `__reduce__` payloads round-trip geometry rows through plain WKB (Z/M
// preserved; the frame travels separately as the stored CRS definition plus
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
    let mut out = Vec::with_capacity(byte_len);
    for logical in 0..len {
        let value = values
            .get(selection.physical(logical))
            .expect("pickle row selection is admitted against the coordinate column");
        out.extend_from_slice(&value.to_le_bytes());
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
    let mut out = Vec::with_capacity(byte_len);
    for window in windows {
        let selected = values
            .get(window)
            .expect("pickle CSR windows are admitted against the coordinate column");
        for &value in selected {
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
    let mut out = Vec::with_capacity(byte_len);
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

/// The frame a reducer wrote. Unpickling already runs arbitrary code, so a
/// forged frame is not a boundary this reader defends.
fn pickled_frame(crs: Option<String>, epoch: Option<f64>) -> crate::error::Result<Frame> {
    Frame::new(crs.map(Crs::from), epoch)
}

#[expect(
    clippy::used_underscore_items,
    reason = "the leading underscore is the Python convention for a module-private helper, and pickle stores these names in its stream, so the name is a wire contract rather than a Rust style choice"
)]
/// Rebuild a pickled `Geometry` from WKB (internal; see
/// ``Geometry.__reduce__``).
#[pyfunction]
pub(crate) fn _unpickle_geometry_v1(
    py: Python<'_>,
    wkb: &[u8],
    crs: Option<String>,
    epoch: Option<f64>,
) -> PyResult<Typed> {
    // PyO3 extracts &[u8] only from immutable PyBytes; the call retains its owner.
    let geometry = py
        .detach(move || {
            Ok::<_, crate::error::Error>(PyGeometry::with_frame(
                crate::io::parse_wkb(wkb)?.shape,
                pickled_frame(crs, epoch)?,
            ))
        })
        .map_err(PyErr::from)?;
    Ok(Typed(geometry))
}

#[expect(
    clippy::used_underscore_items,
    reason = "the leading underscore is the Python convention for a module-private helper, and pickle stores these names in its stream, so the name is a wire contract rather than a Rust style choice"
)]
/// Rebuild a pickled mixed-storage `GeometryArray` from WKB rows (internal;
/// see ``GeometryArray.__reduce__``).
///
/// Always builds Mixed storage: WKB intentionally preserves malformed rings
/// for `validate()`, and must never enter trusted packed columns.
#[pyfunction]
pub(crate) fn _unpickle_geometry_array_v1(
    py: Python<'_>,
    rows: &Bound<'_, PyAny>,
    crs: Option<String>,
    epoch: Option<f64>,
    missing: Option<&[u8]>,
) -> PyResult<PyGeometryArray> {
    let rows = crate::collect_bytes_rows(rows)?;
    py.detach(move || unpickle_geometry_array_detached(rows, crs, epoch, missing))
        .map_err(PyErr::from)
}

#[expect(
    clippy::used_underscore_items,
    reason = "the leading underscore is the Python convention for a module-private helper, and pickle stores these names in its stream, so the name is a wire contract rather than a Rust style choice"
)]
/// Rebuild a pickled packed `GeometryArray` from little-endian `f64` columns
/// and the `i32` CSR offset levels of its kind: none for points, one for
/// lines, ring then polygon offsets for polygons (internal; see
/// ``GeometryArray.__reduce__``).
///
/// The payload carries only present rows; an optional missing mask restores
/// their logical positions.
#[pyfunction]
pub(crate) fn _unpickle_packed_array_v1(
    py: Python<'_>,
    xs: &[u8],
    ys: &[u8],
    zs: Option<&[u8]>,
    ms: Option<&[u8]>,
    offsets: &Bound<'_, PyTuple>,
    crs: Option<String>,
    epoch: Option<f64>,
    missing: Option<&[u8]>,
) -> PyResult<PyGeometryArray> {
    let level = |index: usize| -> PyResult<Bound<'_, PyBytes>> {
        Ok(offsets.get_item(index)?.cast_into::<PyBytes>()?)
    };
    let owners = match offsets.len() {
        0 => OffsetLevels::Points,
        1 => OffsetLevels::Lines(level(0)?),
        2 => OffsetLevels::Polygons {
            rings: level(0)?,
            polygons: level(1)?,
        },
        _ => return Err(pickle_error("packed pickle has at most two offset levels").into()),
    };
    let levels = match &owners {
        OffsetLevels::Points => OffsetLevels::Points,
        OffsetLevels::Lines(bytes) => OffsetLevels::Lines(bytes.as_bytes()),
        OffsetLevels::Polygons { rings, polygons } => OffsetLevels::Polygons {
            rings: rings.as_bytes(),
            polygons: polygons.as_bytes(),
        },
    };
    let columns = (xs, ys, zs, ms);
    py.detach(move || unpickle_packed_array_detached(columns, levels, crs, epoch, missing))
        .map_err(PyErr::from)
}

/// The CSR offset levels of one packed geometry kind.
enum OffsetLevels<T = Vec<u8>> {
    Points,
    Lines(T),
    Polygons { rings: T, polygons: T },
}

/// Decode a missing mask; any nonzero byte marks a missing row.
fn pickled_missing_mask(bytes: &[u8]) -> Option<ValidityMask> {
    ValidityMask::from_missing_iter(bytes.len(), bytes.iter().map(|&byte| byte != 0))
}

/// Decode little-endian `i32` CSR offsets through the owning validator, which
/// keeps every row window inside `end_cap`.
fn pickled_csr_offsets<Level>(
    bytes: &[u8],
    end_cap: usize,
) -> crate::error::Result<CsrOffsetColumn<Level>> {
    let chunks = bytes.as_chunks::<4>().0;
    let mut offsets = Vec::with_capacity(chunks.len());
    offsets.extend(chunks.iter().map(|chunk| i32::from_le_bytes(*chunk)));
    CsrOffsetColumn::try_from_shared_i32(SharedSlice::from_vec(offsets), end_cap)
}

/// Decode the little-endian `f64` columns of a packed payload.
fn pickled_coordseq(
    xs: &[u8],
    ys: &[u8],
    zs: Option<&[u8]>,
    ms: Option<&[u8]>,
) -> crate::error::Result<CoordSeq> {
    let column = |bytes: &[u8]| {
        let chunks = bytes.as_chunks::<8>().0;
        let mut values = Vec::with_capacity(chunks.len());
        values.extend(chunks.iter().map(|chunk| f64::from_le_bytes(*chunk)));
        values
    };
    let xs = column(xs);
    let ys = column(ys);
    let zs = zs.map(column);
    let ms = ms.map(column);
    // `from_owned_columns` leaves equal column lengths to its caller.
    if xs.len() != ys.len()
        || zs.as_ref().is_some_and(|column| column.len() != xs.len())
        || ms.as_ref().is_some_and(|column| column.len() != xs.len())
    {
        return Err(pickle_error("packed-array pickle columns differ in length"));
    }
    CoordSeq::from_owned_columns(xs, ys, zs, ms)
}

fn unpickle_geometry_array_detached(
    rows: Vec<crate::OwnedBytes>,
    crs: Option<String>,
    epoch: Option<f64>,
    missing: Option<&[u8]>,
) -> crate::error::Result<PyGeometryArray> {
    let mask = match missing {
        Some(bytes) if bytes.len() != rows.len() => {
            return Err(pickle_error(
                "mixed-array pickle missing mask length differs from its rows",
            ));
        },
        Some(bytes) => pickled_missing_mask(bytes),
        None => None,
    };
    let mut shapes = Vec::with_capacity(rows.len());
    for (row, wkb) in rows.iter().enumerate() {
        let shape = if mask.as_ref().is_some_and(|mask| mask.is_missing(row)) {
            PyGeometryArray::missing_placeholder()
        } else {
            crate::io::parse_wkb(wkb.as_slice())?.shape
        };
        shapes.push(shape);
    }
    PyGeometryArray::mixed_shapes(shapes, pickled_frame(crs, epoch)?)?.with_missing_mask(mask)
}

/// Packed rows are read through constructors that assert their shape, so
/// the structural checks here stop a panic on a corrupt payload.
fn unpickle_packed_array_detached(
    (xs, ys, zs, ms): EncodedPointColumns<&[u8]>,
    levels: OffsetLevels<&[u8]>,
    crs: Option<String>,
    epoch: Option<f64>,
    missing: Option<&[u8]>,
) -> crate::error::Result<PyGeometryArray> {
    let seq = pickled_coordseq(xs, ys, zs, ms)?;
    let frame = pickled_frame(crs, epoch)?;
    let present = match levels {
        OffsetLevels::Points => PyGeometryArray::packed_points(seq, frame)?,
        OffsetLevels::Lines(offsets) => {
            let offsets = pickled_csr_offsets(offsets, seq.len())?;
            if offsets
                .array_windows::<2>()
                .any(|[start, end]| (*end).wrapping_sub(*start) == 1)
            {
                return Err(pickle_error("line-array pickle row has a single vertex"));
            }
            PyGeometryArray::packed_lines(seq, offsets, frame)?
        },
        OffsetLevels::Polygons { rings, polygons } => {
            let rings = pickled_csr_offsets(rings, seq.len())?;
            for [start, end] in rings.array_windows::<2>() {
                CheckedRingWindow::checked(&seq, *start as usize..*end as usize)
                    .map_err(|error| pickle_error(format!("polygon-array pickle {error}")))?;
            }
            let polygons = pickled_csr_offsets(polygons, rings.len().wrapping_sub(1))?;
            if polygons
                .array_windows::<2>()
                .any(|[start, end]| end <= start)
            {
                return Err(pickle_error(
                    "polygon-array pickle polygon has no shell ring",
                ));
            }
            PyGeometryArray::packed_polygons(seq, rings, polygons, frame)?
        },
    };
    match missing.and_then(pickled_missing_mask) {
        Some(mask) => PyGeometryArray::scatter_present_rows(&present, mask),
        None => Ok(present),
    }
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

/// A `GeometryArray.__reduce__` row payload: WKB rows for mixed storage, or
/// typed columns and offsets for packed storage.
enum PickleRows {
    Mixed(Vec<Vec<u8>>),
    Packed {
        columns: EncodedPointColumns,
        levels: OffsetLevels,
    },
}

struct ArrayPicklePayload {
    rows: PickleRows,
    crs: Option<String>,
    epoch: Option<f64>,
    missing: Option<Vec<u8>>,
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
        .getattr(pyo3::intern!(py, "_unpickle_geometry_v1"))?
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
        .detach(move || -> crate::error::Result<ArrayPicklePayload> {
            Ok(ArrayPicklePayload {
                rows: pickle_rows(storage.as_ref(), missing.as_ref(), point_layout)?,
                crs,
                epoch,
                missing: missing.as_ref().map(missing_bytes),
            })
        })
        .map_err(PyErr::from)?;
    materialize_array_pickle_payload(py, payload)
}

/// Packed payloads hold finite present rows only; storage with a nonfinite
/// present ordinate falls back to WKB rows.
fn pickle_rows(
    storage: &GeometryArrayStorage,
    missing: Option<&ValidityMask>,
    point_layout: PointRowLayout,
) -> crate::error::Result<PickleRows> {
    let mixed = || -> crate::error::Result<PickleRows> {
        Ok(PickleRows::Mixed(wkb_rows_for_mask(
            storage,
            missing,
            point_layout,
        )?))
    };
    match (storage, missing) {
        (GeometryArrayStorage::Mixed(_), _) => mixed(),
        (GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map }), None) => {
            let selection = row_map.as_deref();
            if !points_are_finite(coords, selection) {
                return mixed();
            }
            Ok(PickleRows::Packed {
                columns: encode_points_selection(coords, selection)?,
                levels: OffsetLevels::Points,
            })
        },
        (GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map }), Some(mask)) => {
            let selection = row_map.as_deref();
            let row_aligned =
                !point_layout.is_compact() && mask.len() == selection.len(coords.len());
            let columns = if row_aligned {
                let indices = selected_storage_indices(selection, coords.len(), Some(mask));
                if !points_are_finite(coords, RowSelectionRef::Gather(&indices)) {
                    return mixed();
                }
                encode_points(coords, &indices)?
            } else {
                if !points_are_finite(coords, selection) {
                    return mixed();
                }
                encode_points_selection(coords, selection)?
            };
            Ok(PickleRows::Packed {
                columns,
                levels: OffsetLevels::Points,
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
            let windows = selected_line_windows(offsets, row_map.as_deref(), mask);
            if !columns_are_finite(coords, &windows) {
                return mixed();
            }
            Ok(PickleRows::Packed {
                columns: encode_windows(coords, &windows)?,
                levels: OffsetLevels::Lines(rebased_i32_offsets_le_bytes(
                    windows.iter().map(std::ops::Range::len),
                    windows.len(),
                )?),
            })
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
            );
            let mut windows = Vec::with_capacity(rows.len());
            let mut polygon_ring_counts = Vec::with_capacity(rows.len());
            for &row in &rows {
                let ring_indices = row_map.as_deref().csr_window(polygon_offsets, row);
                polygon_ring_counts.push(ring_indices.len());
                let ring_ends = ring_offsets
                    .as_slice()
                    .get(ring_indices.start..=ring_indices.end)
                    .expect("admitted polygon offsets select admitted ring offsets");
                for [start, end] in ring_ends.array_windows::<2>() {
                    windows.push(*start as usize..*end as usize);
                }
            }
            if !columns_are_finite(coords, &windows) {
                return mixed();
            }
            Ok(PickleRows::Packed {
                columns: encode_windows(coords, &windows)?,
                levels: OffsetLevels::Polygons {
                    rings: rebased_i32_offsets_le_bytes(
                        windows.iter().map(std::ops::Range::len),
                        windows.len(),
                    )?,
                    polygons: rebased_i32_offsets_le_bytes(
                        polygon_ring_counts.into_iter(),
                        rows.len(),
                    )?,
                },
            })
        },
    }
}

fn missing_bytes(mask: &ValidityMask) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(mask.len());
    for missing in mask.iter() {
        bytes.push(u8::from(missing));
    }
    bytes
}

fn selected_storage_rows(
    row_map: RowSelectionRef<'_>,
    physical_rows: usize,
    missing: Option<&ValidityMask>,
) -> Vec<usize> {
    let logical_rows = row_map.len(physical_rows);
    let mut rows = Vec::with_capacity(logical_rows);
    for logical in 0..logical_rows {
        if missing.is_some_and(|mask| mask.len() == logical_rows && mask.is_missing(logical)) {
            continue;
        }
        rows.push(logical);
    }
    rows
}

fn selected_line_windows(
    offsets: &[i32],
    row_map: RowSelectionRef<'_>,
    missing: Option<&ValidityMask>,
) -> Vec<std::ops::Range<usize>> {
    let logical_rows = row_map.len(offsets.len().saturating_sub(1));
    let mut windows = Vec::with_capacity(logical_rows);
    for logical in 0..logical_rows {
        if missing.is_some_and(|mask| mask.len() == logical_rows && mask.is_missing(logical)) {
            continue;
        }
        windows.push(row_map.csr_window(offsets, logical));
    }
    windows
}

fn selected_storage_indices(
    row_map: RowSelectionRef<'_>,
    physical_rows: usize,
    missing: Option<&ValidityMask>,
) -> Vec<usize> {
    let logical_rows = row_map.len(physical_rows);
    let mut indices = Vec::with_capacity(logical_rows);
    for logical in 0..logical_rows {
        if missing.is_some_and(|mask| mask.len() == logical_rows && mask.is_missing(logical)) {
            continue;
        }
        indices.push(row_map.physical(logical));
    }
    indices
}

fn points_are_finite(coords: &CoordSeq, selection: RowSelectionRef<'_>) -> bool {
    (0..selection.len(coords.len())).all(|logical| {
        let index = selection.physical(logical);
        coords
            .xs()
            .get(index)
            .expect("pickle row selection is admitted against the X column")
            .is_finite()
            && coords
                .ys()
                .get(index)
                .expect("pickle row selection is admitted against the Y column")
                .is_finite()
            && coords.zs().is_none_or(|values| {
                values
                    .get(index)
                    .expect("coordinate axes keep the Z column aligned")
                    .is_finite()
            })
            && coords.ms().is_none_or(|values| {
                values
                    .get(index)
                    .expect("coordinate axes keep the M column aligned")
                    .is_finite()
            })
    })
}

fn columns_are_finite(coords: &CoordSeq, windows: &[std::ops::Range<usize>]) -> bool {
    windows.iter().all(|window| {
        coords
            .xs()
            .get(window.clone())
            .expect("pickle CSR windows are admitted against the X column")
            .iter()
            .all(|value| value.is_finite())
            && coords
                .ys()
                .get(window.clone())
                .expect("pickle CSR windows are admitted against the Y column")
                .iter()
                .all(|value| value.is_finite())
            && coords.zs().is_none_or(|values| {
                values
                    .get(window.clone())
                    .expect("coordinate axes keep the Z column aligned")
                    .iter()
                    .all(|value| value.is_finite())
            })
            && coords.ms().is_none_or(|values| {
                values
                    .get(window.clone())
                    .expect("coordinate axes keep the M column aligned")
                    .iter()
                    .all(|value| value.is_finite())
            })
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
    let mut rows = Vec::with_capacity(row_count);
    let mut present = 0;
    for logical in 0..row_count {
        if missing.is_some_and(|mask| mask.is_missing(logical)) {
            rows.push(Vec::new());
            continue;
        }
        let payload_row = if compact { present } else { logical };
        present = present.wrapping_add(1);
        let wkb = storage
            .row(payload_row)
            .try_with_shape(|shape| crate::io::to_wkb(shape, None, false))?;
        rows.push(wkb);
    }
    Ok(rows)
}

fn materialize_array_pickle_payload(
    py: Python<'_>,
    payload: ArrayPicklePayload,
) -> PyResult<(Py<PyAny>, Py<PyAny>)> {
    let ArrayPicklePayload {
        rows,
        crs,
        epoch,
        missing,
    } = payload;
    let bytes = |bytes: &[u8]| PyBytes::new(py, bytes);
    let missing = missing.as_deref().map(bytes);
    let (name, args) = match rows {
        PickleRows::Mixed(rows) => {
            let mut py_rows = Vec::with_capacity(rows.len());
            for row in rows {
                py_rows.push(bytes(&row));
            }
            (
                "_unpickle_geometry_array_v1",
                (py_rows, crs.as_deref(), epoch, missing).into_py_any(py)?,
            )
        },
        PickleRows::Packed {
            columns: (xs, ys, zs, ms),
            levels,
        } => {
            let offsets = match &levels {
                OffsetLevels::Points => PyTuple::empty(py),
                OffsetLevels::Lines(offsets) => PyTuple::new(py, [bytes(offsets)])?,
                OffsetLevels::Polygons { rings, polygons } => {
                    PyTuple::new(py, [bytes(rings), bytes(polygons)])?
                },
            };
            (
                "_unpickle_packed_array_v1",
                (
                    bytes(&xs),
                    bytes(&ys),
                    zs.as_deref().map(bytes),
                    ms.as_deref().map(bytes),
                    offsets,
                    crs.as_deref(),
                    epoch,
                    missing,
                )
                    .into_py_any(py)?,
            )
        },
    };
    let callable = crate::gometry_lib_module(py)?.getattr(name)?.unbind();
    Ok((callable, args))
}
