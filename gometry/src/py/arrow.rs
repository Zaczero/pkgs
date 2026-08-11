//! `PyArrow` interchange — encode/decode gometry geometries to/from Arrow
//! arrays.
//!
//! Extracted from `lib.rs`; uses crate-root helpers directly (a child module
//! may access its ancestors' private items).
#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]

use std::sync::Arc;

pub(crate) use pyo3::exceptions::{PyModuleNotFoundError, PyTypeError};
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyAny, PyBytes, PyModule};
use serde_json::Value;

use crate::boundary::metadata::Frame;
use crate::geometry::{
    CoordSeq, CoordinateAxes, Coordinates, EmptyKind, HasM, HasZ, LineSeq, Point, Polygon, Shape,
    same_active_position,
};
use crate::py::errors::{GeometryError, InvalidGeometryError, ParseFormat, parse_error};
use crate::py::geoarrow::GeometryEncoding;
use crate::py::wire_crs::guard_embedded_crs_conflict;
use crate::{
    Crs, PyGeometry, PyGeometryArray, common_crs_required, crs_arc, crs_arc_str, exact_geometry,
    exact_geometry_array, io, parse_crs,
};

crate::tokens::token_enum! {
    /// Arrow export encoding.
    pub enum ArrowEncoding("encoding", param = "encoding") {
        Auto = "auto",
        Wkb = "wkb",
    }
}
crate::tokens::token_from_pyobject!(ArrowEncoding);

pub(crate) struct ArrowStorage {
    pub(crate) storage: Py<PyAny>,
    pub(crate) crs: Option<String>,
    pub(crate) epoch: Option<f64>,
    pub(crate) encoding: crate::py::geoarrow::GeometryEncoding,
    pub(crate) wkb_offset_width: WkbOffsetWidth,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WkbOffsetWidth {
    Int32,
    Int64,
    View,
}

pub(crate) struct ArrowValidity {
    pub(crate) bitmap: Option<Vec<u8>>,
    pub(crate) offset: usize,
}

pub(crate) struct ArrowCoordinateValues {
    pub(crate) x: ArrowOrdinateValues,
    pub(crate) y: ArrowOrdinateValues,
    pub(crate) z: Option<ArrowOrdinateValues>,
    pub(crate) m: Option<ArrowOrdinateValues>,
    pub(crate) value_validity: ArrowValidity,
    /// Logical coordinate index represented by validity slot zero.
    pub(crate) value_base: usize,
    pub(crate) full: std::cell::OnceCell<crate::geometry::CoordSeq>,
}

pub(crate) struct ArrowOrdinateValues {
    pub(crate) values: Arc<[f64]>,
    pub(crate) base: usize,
    pub(crate) validity: ArrowValidity,
    pub(crate) field: &'static str,
}

pub(crate) fn pyarrow_module(py: Python<'_>) -> PyResult<&Bound<'_, PyModule>> {
    static PYARROW: PyOnceLock<Py<PyModule>> = PyOnceLock::new();
    PYARROW
        .get_or_try_init(py, || {
            py.import("pyarrow").map(Bound::unbind).map_err(|_| {
                PyModuleNotFoundError::new_err(
                    "Arrow interop requires pyarrow; install gometry with pyarrow available",
                )
            })
        })
        .map(|module| module.bind(py))
}

pub(crate) fn gometry_arrow_module(py: Python<'_>) -> PyResult<&Bound<'_, PyModule>> {
    static GOMETRY_ARROW: PyOnceLock<Py<PyModule>> = PyOnceLock::new();
    GOMETRY_ARROW
        .get_or_try_init(py, || py.import("gometry._arrow").map(Bound::unbind))
        .map(|module| module.bind(py))
}

pub(crate) fn reject_requested_schema(requested_schema: Option<&Bound<'_, PyAny>>) -> PyResult<()> {
    if requested_schema.is_some_and(|schema| !schema.is_none()) {
        return Err(PyTypeError::new_err(
            "requested_schema is not supported; export the default GeoArrow schema",
        ));
    }
    Ok(())
}

pub(crate) fn reconcile_arrow_crs(
    embedded: Option<&str>,
    crs: Option<&Bound<'_, PyAny>>,
) -> PyResult<Option<String>> {
    let explicit = parse_crs(crs)?;
    guard_embedded_crs_conflict(embedded, explicit.as_ref().map(Crs::as_str), "GeoArrow CRS")?;
    Ok(embedded
        .map(str::to_owned)
        .or_else(|| explicit.map(|value| value.as_str().to_owned())))
}

pub(crate) fn reconcile_arrow_epoch(
    embedded: Option<f64>,
    epoch: Option<&Bound<'_, PyAny>>,
) -> PyResult<Option<f64>> {
    let explicit = crate::coordinate_epoch_option("epoch", epoch)?;
    match (embedded, explicit) {
        (Some(embedded), Some(explicit))
            if !crate::boundary::metadata::epochs_equal(embedded, explicit) =>
        {
            Err(crate::py::errors::epoch_mismatch_error(
                "GeoArrow epoch metadata conflicts with epoch=",
                Some(explicit),
                Some(embedded),
                None,
            ))
        },
        (Some(embedded), None) => Ok(Some(embedded)),
        (_, Some(explicit)) => Ok(Some(explicit)),
        (None, None) => Ok(None),
    }
}

pub(crate) fn geometries_from_native_capsules(
    py: Python<'_>,
    schema_capsule: &Bound<'_, PyAny>,
    array_capsule: &Bound<'_, PyAny>,
    crs: Option<&Bound<'_, PyAny>>,
    epoch: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyAny>> {
    // Same mandatory schema classifier as Arrow-C streams — no buffer read
    // until ordinate/storage admission succeeds.
    crate::py::arrow_c::geometries_from_native_capsules_classified(
        py,
        schema_capsule,
        array_capsule,
        crs,
        epoch,
    )
}

fn is_pure_offset_wkb(storages: &[ArrowStorage]) -> bool {
    storages.iter().all(|storage| {
        matches!(storage.encoding, GeometryEncoding::Wkb)
            && !matches!(storage.wkb_offset_width, WkbOffsetWidth::View)
    })
}

fn reconcile_storages_frame(
    storages: &[ArrowStorage],
    crs: Option<&Bound<'_, PyAny>>,
    epoch: Option<&Bound<'_, PyAny>>,
) -> PyResult<(Option<String>, Option<f64>, Frame)> {
    let mut frame = None;
    for storage in storages {
        match &frame {
            Some(frame) if *frame != (storage.crs.clone(), storage.epoch) => {
                return Err(geoarrow_parse_error(
                    "Arrow chunks have mixed CRS/epoch metadata",
                ));
            },
            Some(_) => {},
            None => frame = Some((storage.crs.clone(), storage.epoch)),
        }
    }
    let (embedded_crs, embedded_epoch) = frame.unwrap_or((None, None));
    let crs = reconcile_arrow_crs(embedded_crs.as_deref(), crs)?;
    let epoch = reconcile_arrow_epoch(embedded_epoch, epoch)?;
    let frame = Frame::new(crs.clone().map(Into::into), epoch)?;
    Ok((crs, epoch, frame))
}

fn storages_may_have_nulls(py: Python<'_>, storages: &[ArrowStorage]) -> PyResult<bool> {
    for storage in storages {
        let null_count = storage
            .storage
            .bind(py)
            .getattr("null_count")?
            .extract::<i64>()?;
        if null_count != 0 {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(crate) fn geometries_from_arrow_storages(
    py: Python<'_>,
    storages: Vec<ArrowStorage>,
    crs: Option<&Bound<'_, PyAny>>,
    epoch: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyAny>> {
    // Pure offset-based WKB: `AdmittedWkbPlan::admit` is the single layout
    // pass (offsets + data). Skip the general ensure walk so offsets are not
    // fetched twice. BinaryView and non-WKB encodings still use ensure.
    let pure_offset_wkb = is_pure_offset_wkb(&storages);
    // A successful packed line/polygon import admits every selected list
    // window before it reads coordinates.  Let that one admission establish
    // the offset invariant instead of snapshotting the same outer and inner
    // offsets once here and again in the packed path.  If packing declines,
    // the complete preflight still runs before the boxed decoder touches the
    // provider's coordinate payload.
    let defer_packed_list_preflight = is_homogeneous_packed_list(&storages);
    if !pure_offset_wkb && !defer_packed_list_preflight {
        for storage in &storages {
            ensure_pyarrow_storage_offsets_monotonic(py, storage)?;
        }
    }
    let (crs, epoch, frame) = reconcile_storages_frame(&storages, crs, epoch)?;

    // Homogeneous WKB (offset Binary/LargeBinary and BinaryView) BEFORE any
    // pyarrow null strip/scatter: native admission already validates validity
    // and inserts final-order missing placeholders. Nullable non-WKB and
    // heterogeneous encodings fall through to the masked path below.
    if storages
        .iter()
        .all(|storage| matches!(storage.encoding, GeometryEncoding::Wkb))
    {
        let explicit = crs.map(crs_arc);
        return Ok(import_arrow_wkb_shapes(py, &storages, explicit, epoch)?
            .into_pyobject(py)?
            .unbind()
            .into());
    }

    let may_have_nulls = storages_may_have_nulls(py, &storages)?;
    let all_pyarrow = storages
        .iter()
        .all(|storage| storage.storage.bind(py).hasattr("is_null").unwrap_or(false));
    if may_have_nulls && all_pyarrow {
        // Non-WKB only (homogeneous WKB returned above). Offsets already
        // ensured when not pure_offset_wkb, except the packed fast path whose
        // no-null precondition did not hold.
        if defer_packed_list_preflight {
            for storage in &storages {
                ensure_pyarrow_storage_offsets_monotonic(py, storage)?;
            }
        }
        debug_assert!(!pure_offset_wkb);
        return masked_pyarrow_import(py, storages, frame);
    }
    if storages
        .iter()
        .all(|storage| matches!(storage.encoding, GeometryEncoding::LineString))
        && let Some(array) = packed_linestrings_import(py, &storages, frame.clone())?
    {
        return Ok(array.into_pyobject(py)?.unbind().into());
    }
    if storages
        .iter()
        .all(|storage| matches!(storage.encoding, GeometryEncoding::Point))
        && let Some(array) = packed_points_import(py, &storages, frame.clone())?
    {
        return Ok(array.into_pyobject(py)?.unbind().into());
    }
    if storages
        .iter()
        .all(|storage| matches!(storage.encoding, GeometryEncoding::Polygon))
        && let Some(array) = packed_polygons_import(py, &storages, frame)?
    {
        return Ok(array.into_pyobject(py)?.unbind().into());
    }

    // A packed candidate which did not materialize must not reach the boxed
    // decoder on the strength of a partial admission.
    if defer_packed_list_preflight {
        for storage in &storages {
            ensure_pyarrow_storage_offsets_monotonic(py, storage)?;
        }
    }

    // Untrusted Arrow lengths: checked sum + fallible reserve (never panic).
    let capacity = crate::checked_length_sum(
        storages
            .iter()
            .map(|storage| storage.storage.bind(py).len()),
    )?;
    let mut geometries = crate::try_vec_with_capacity(capacity)?;
    let mut row = 0_usize;
    let mut missing_rows: Vec<usize> = Vec::new();
    for storage in &storages {
        let storage_array = storage.storage.bind(py);
        append_arrow_storage(
            py,
            storage_array,
            storage,
            &mut geometries,
            &mut row,
            &mut missing_rows,
        )?;
    }
    let crs = match crs {
        Some(crs) => Some(crs_arc(crs)),
        None => arrow_import_common_crs(&geometries, &missing_rows)?,
    };
    apply_arrow_batch_frame(&mut geometries, crs.as_ref(), epoch);
    let mask = crate::array::sparse_missing_mask(geometries.len(), &missing_rows);
    Ok(
        PyGeometryArray::pack_or_mixed(geometries, Frame::new(crs, epoch)?)
            .with_missing_mask(mask)
            .into_pyobject(py)?
            .unbind()
            .into(),
    )
}

/// Only these homogeneous encodings have a packed importer that validates the
/// complete selected list hierarchy before reading coordinate values.
fn is_homogeneous_packed_list(storages: &[ArrowStorage]) -> bool {
    storages.iter().all(|storage| {
        matches!(
            storage.encoding,
            GeometryEncoding::LineString | GeometryEncoding::Polygon
        )
    })
}

/// Stamp the resolved batch CRS/epoch onto imported rows only when they
/// differ. Append paths already attach storage CRS (or embedded EWKB SRID)
/// with epoch `None`; the common path therefore does no per-row
/// `FrameDependentCaches` rebuild (the measured mixed-import churn).
fn apply_arrow_batch_frame(geometries: &mut [PyGeometry], crs: Option<&Crs>, epoch: Option<f64>) {
    for geometry in geometries {
        if geometry.crs_ref().is_none()
            && let Some(crs) = crs
        {
            geometry.set_crs_keep_epoch(Some(crs.clone()));
        }
        let epoch_differs = match (geometry.epoch(), epoch) {
            (None, None) => false,
            (Some(left), Some(right)) => !crate::epochs_equal(left, right),
            _ => true,
        };
        if epoch_differs {
            geometry.set_epoch_keep_crs(epoch);
        }
    }
}

fn arrow_import_common_crs(
    geometries: &[PyGeometry],
    missing_rows: &[usize],
) -> PyResult<Option<Crs>> {
    if missing_rows.is_empty() {
        return common_crs_required(geometries, "Arrow import");
    }
    // Placeholder rows carry no frame; the shared CRS is decided by the
    // present rows alone. `missing_rows` is produced in row order.
    let mut missing_iter = missing_rows.iter().copied();
    let mut next_missing = missing_iter.next();
    common_crs_required(
        geometries
            .iter()
            .enumerate()
            .filter_map(move |(index, geometry)| {
                if next_missing == Some(index) {
                    next_missing = missing_iter.next();
                    None
                } else {
                    Some(geometry)
                }
            }),
        "Arrow import",
    )
}

/// Validate list/binary offset structure on the **pre-strip** array (m01/D17/D18).
///
/// * m01 — full visible window including null slots must be ordered.
/// * D17 — nested list levels validate the descendant spans selected by the
///   visible parent rows, including selected null slots. Sliced-away physical
///   children are neither decoded nor retained by Arrow admission.
/// * D18 — length-0 arrays still have one start offset that must be
///   non-negative; empty early-returns must not skip this check.
///
/// `strip_missing` rebuilds a dense present-only view whose offsets are
/// always ordered, so the null-hidden non-monotonic defect would otherwise
/// only be caught on the capsule/stream path when this runs pre-strip.
pub(crate) fn ensure_pyarrow_storage_offsets_monotonic(
    py: Python<'_>,
    storage: &ArrowStorage,
) -> PyResult<()> {
    let array = storage.storage.bind(py);
    let len = array.len()?;
    let offset = arrow_array_offset(array)?;
    match storage.encoding {
        GeometryEncoding::Wkb => match storage.wkb_offset_width {
            WkbOffsetWidth::View => Ok(()),
            WkbOffsetWidth::Int64 => {
                let slots = len
                    .checked_add(1)
                    .ok_or_else(|| geoarrow_parse_error("Arrow offset count overflows"))?;
                let offsets = arrow_i64_offsets_window(py, array, offset, slots)?;
                // Offset chain first (D18 non-negative / ordered). Data-buffer
                // length may be schema-derived from the terminal on the native
                // path and must not be read before offsets are proven valid.
                ensure_i64_offsets_monotonic(&offsets, 0, len, usize::MAX)?;
                let data_len = arrow_binary_data_buffer_len(array)?;
                let terminal = i64_offset_to_usize(offsets[len])?;
                ensure_offset_terminal_within_child(terminal, data_len)
            },
            WkbOffsetWidth::Int32 => {
                let slots = len
                    .checked_add(1)
                    .ok_or_else(|| geoarrow_parse_error("Arrow offset count overflows"))?;
                let offsets = arrow_i32_offsets_window(py, array, offset, slots)?;
                ensure_i32_offsets_monotonic(&offsets, 0, len, usize::MAX)?;
                let data_len = arrow_binary_data_buffer_len(array)?;
                let terminal = i32_offset_to_usize(offsets[len])?;
                ensure_offset_terminal_within_child(terminal, data_len)
            },
        },
        GeometryEncoding::Point => Ok(()),
        GeometryEncoding::MultiPoint
        | GeometryEncoding::LineString
        | GeometryEncoding::MultiLineString
        | GeometryEncoding::Polygon
        | GeometryEncoding::MultiPolygon => {
            // Outer list offsets — full visible window including nulls and the
            // empty-array start slot (count may be 0).
            let level = ArrowListLevel::read(py, array)?;
            level.ensure(0, len)?;
            // Nested list encodings: walk every descendant span selected by
            // the visible parent rows. A selected null row is structural input
            // and stays checked; a sliced-away physical child is outside this
            // import's visible-span contract.
            ensure_nested_list_offsets_monotonic(py, array, storage.encoding, &level)
        },
    }
}

fn ensure_nested_list_offsets_monotonic(
    py: Python<'_>,
    array: &Bound<'_, PyAny>,
    encoding: GeometryEncoding,
    outer: &ArrowListLevel,
) -> PyResult<()> {
    // Depth of list nesting past the outer geometry list (0 = multipoint/line).
    let inner_lists = match encoding {
        GeometryEncoding::MultiPoint | GeometryEncoding::LineString => 0,
        GeometryEncoding::MultiLineString | GeometryEncoding::Polygon => 1,
        GeometryEncoding::MultiPolygon => 2,
        GeometryEncoding::Point | GeometryEncoding::Wkb => return Ok(()),
    };
    // The outer level was checked by the caller over its full visible window.
    // Reuse that admitted window while propagating its selected child span;
    // native Arrow-C does the same rather than taking a second outer snapshot.
    let len = array.len()?;
    let mut start = outer.endpoint(0)?;
    let mut end = outer.endpoint(len)?;
    let mut current = array.getattr("values")?;
    for _ in 0..inner_lists {
        let count = end
            .checked_sub(start)
            .ok_or_else(|| geoarrow_parse_error("Arrow offsets must be ordered"))?;
        let level = ArrowListLevel::read_selected(py, &current, start, count)?;
        level.ensure(start, count)?;
        start = level.endpoint(start)?;
        end = level.endpoint(end)?;
        current = current.getattr("values")?;
    }
    Ok(())
}

/// Nulled pyarrow chunks: strip the null rows Python-side, import densely,
/// then scatter the present rows back under the collected mask. (Native
/// capsule arrays take the WKB row lane instead, which masks per row.)
fn masked_pyarrow_import(
    py: Python<'_>,
    storages: Vec<ArrowStorage>,
    frame: Frame,
) -> PyResult<Py<PyAny>> {
    let module = gometry_arrow_module(py)?;
    let mut mask: Vec<bool> = Vec::new();
    let mut dense_storages = Vec::with_capacity(storages.len());
    for storage in storages {
        // Offsets already validated in geometries_from_arrow_storages (pre-strip).
        let array = storage.storage.bind(py);
        let null_count = array.getattr("null_count")?.extract::<i64>()?;
        let chunk_mask: Vec<u8> = module.call_method1("missing_mask", (array,))?.extract()?;
        let chunk_bools: Vec<bool> = chunk_mask.into_iter().map(|byte| byte != 0).collect();
        // Known null_count must agree with the visible validity bitmap (P02).
        ensure_null_count_matches_mask(null_count, chunk_bools.iter().copied())?;
        mask.extend(chunk_bools);
        let dense = module.call_method1("strip_missing", (array,))?;
        dense_storages.push(ArrowStorage {
            storage: dense.unbind(),
            crs: storage.crs,
            epoch: storage.epoch,
            encoding: storage.encoding,
            wkb_offset_width: storage.wkb_offset_width,
        });
    }
    let dense = geometries_from_arrow_storages(py, dense_storages, None, None)?;
    let dense = dense
        .bind(py)
        .cast::<PyGeometryArray>()?
        .borrow()
        .retag_frame(frame);
    // Unknown null_count (-1) may resolve to zero actual nulls after the
    // bitmap is inspected; return the dense import without scattering.
    let Some(mask) = crate::array::MissingMask::from_vec(mask.len(), mask) else {
        return Ok(dense.into_pyobject(py)?.unbind().into());
    };
    Ok(PyGeometryArray::scatter_present_rows(&dense, mask)
        .into_pyobject(py)?
        .unbind()
        .into())
}

pub(crate) fn geometries_from_arrow(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    crs: Option<&Bound<'_, PyAny>>,
    epoch: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyAny>> {
    if exact_geometry(value).is_some() || exact_geometry_array(value).is_some() {
        return Err(PyTypeError::new_err(
            "from_arrow expects foreign Arrow data; a Geometry or GeometryArray is already decoded",
        ));
    }
    if value.getattr("type").is_ok()
        || value.getattr("chunks").is_ok()
        || value.getattr("column_names").is_ok()
    {
        let pa = pyarrow_module(py)?;
        return geometries_from_pyarrow(py, pa, value, crs, epoch);
    }
    // RecordBatchReader / stream-like: materialize via PyArrow so ExtensionType
    // and field metadata remain dual-visible for the reconciliation keystone
    // (pure C export collapses them and can drop a CRS on conflict).
    // Propagate `read_all()` errors when pyarrow is available — never fall
    // through to the Arrow-C stream path after a failed/partial consume
    // (that yields a silent empty array). Fall through only when pyarrow is
    // genuinely unavailable.
    if value.hasattr("read_all")?
        && let Ok(pa) = pyarrow_module(py)
    {
        let table = value.call_method0("read_all")?;
        return geometries_from_pyarrow(py, pa, &table, crs, epoch);
    }
    if let Some(output) = crate::py::arrow_c::geometries_from_arrow_c(py, value, crs, epoch)? {
        return Ok(output);
    }
    Err(PyTypeError::new_err(
        "expected a GeoArrow-encoded Arrow array, ChunkedArray, Table, RecordBatch, or Arrow C provider",
    ))
}

mod axes;
mod buffers;
mod coordinates;
mod export;
mod export_buffers;
mod import;
mod import_nested;
mod import_shapes;
mod metadata;
mod packed_import;

pub(crate) use axes::accumulate_geometry_axes;
pub(crate) use buffers::{
    AdmittedBuffer, arrow_array_offset, arrow_binary_data_buffer_len,
    arrow_binary_data_span_admitted, arrow_buffer_span_admitted, arrow_content_error,
    arrow_f64_values_span, arrow_i32_offsets_window, arrow_i64_offsets_window, arrow_null_error,
    arrow_validity, arrow_validity_window, coordinate_span, ensure_arrow_range,
    ensure_i32_offsets_monotonic, ensure_i64_offsets_monotonic, ensure_null_count_matches_mask,
    ensure_offset_terminal_within_child, ensure_usize_offsets_monotonic, i32_offset_to_usize,
    i64_offset_to_usize, mixed_axes_error, push_geometry_level_missing, push_i32_le,
    reject_inner_nulls_in_range, usize_offset_at,
};
pub(crate) use coordinates::{
    ArrowListLevel, ArrowPolygonLevels, arrow_coordinate_values, arrow_polygon_from_ring_range,
    arrow_ring_needs_closure, ensure_vertex_range_in_span,
};
pub(crate) use export::{
    packed_lines_to_arrow, packed_points_to_arrow, packed_polygons_to_arrow, shapes_to_arrow,
    shapes_to_wkb_arrow, storage_to_wkb_arrow, validity_bitmap_from_missing,
};
pub(crate) use export_buffers::{
    ArrowCoordinateBuffers, ExactArrowCoordinateFill, columns_to_pybytes,
};
pub(crate) use import::geometries_from_pyarrow;
pub(crate) use import_nested::{
    append_arrow_multilinestrings, append_arrow_multipolygons, append_arrow_polygons,
    append_arrow_wkb, import_arrow_wkb_shapes,
};
pub(crate) use import_shapes::{append_arrow_storage, arrow_geometry_input};
pub(crate) use metadata::{
    arrow_storage_array, arrow_storage_from_native_geometry, arrow_value_frame,
    decode_extension_name, geoarrow_parse_error, parse_geoarrow_extension_metadata,
};
pub(crate) use packed_import::{
    packed_linestrings_import, packed_points_import, packed_polygons_import,
};

/// Build geometries from an Arrow array (``GeoArrow`` interchange).
///
/// Parameters
/// ----------
/// data : Arrow array
///     A GeoArrow-encoded array (anything exposing the Arrow C stream).
///
/// crs : str or int, optional
///     CRS for arrays without embedded `GeoArrow` metadata. When metadata
///     carries a CRS, ``crs=`` must agree or `CRSMismatchError` is raised.
///
/// epoch : float, optional
///     Coordinate epoch (decimal year) for time-dependent frames.
///
/// Returns
/// -------
/// GeometryArray
///     Arrow containers return a `GeometryArray`, even when they hold one row.
///     CRS and coordinate epoch come from the `GeoArrow` metadata.
///
/// Raises
/// ------
/// TypeError
///     If ``data`` is not an Arrow container or Arrow C provider.
///
/// ParseError
///     If ``data`` is an Arrow object but not a supported `GeoArrow` layout.
/// CRSMismatchError
///     If embedded `GeoArrow` CRS or epoch metadata conflicts with ``crs`` /
///     ``epoch``.
/// CRSError
///     If ``epoch`` is set without ``crs``, or ``crs`` is not recognized.
/// GeometryError
///     If ``epoch`` is not a finite decimal year.
///
/// Notes
/// -----
/// Import copies the selected geometry schema and every span it validates or
/// decodes (validity, offsets, views, referenced BinaryView size entries,
/// coordinates, WKB payload) into owned storage, then validates and decodes
/// only that snapshot. Native
/// Arrow-C providers must not modify exported structs, pointer tables, schema
/// memory, or buffers before gometry invokes their release callback; direct
/// PyArrow objects must not be mutated while ``from_arrow`` is running. Arrow C
/// capsule producers (``__arrow_c_array__`` / ``__arrow_c_stream__``) are trusted
/// to be ABI-conforming — a deliberately hostile duck-typed producer that forges
/// its own buffers is out of the threat model (same line as pyarrow; the C Data
/// Interface carries no buffer capacity except BinaryView's mandatory
/// variadic-sizes buffer, which is enforced). See ``docs/ecosystem/arrow.md``.
///
/// See Also
/// --------
/// GeometryArray.to_arrow : Encode geometries as a GeoArrow array.
#[pyfunction]
#[pyo3(signature = (data, *, crs = None, epoch = None))]
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> arr = gm.GeometryArray([gm.Point(1, 2)])
/// >>> gm.from_arrow(arr.to_arrow()).to_wkt()
/// ['POINT (1 2)']
fn from_arrow(
    py: Python<'_>,
    data: &Bound<'_, PyAny>,
    crs: Option<&Bound<'_, PyAny>>,
    epoch: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyAny>> {
    geometries_from_arrow(py, data, crs, epoch)
}

/// Register the Arrow interchange free functions on the module.
///
/// Encoding an existing geometry/array to Arrow is the `to_arrow` method on
/// `Geometry`/`GeometryArray` (a serializer follows the "what you hold" rule —
/// you already hold the object). Only `from_arrow` is a free function, because
/// decoding has no geometry receiver yet.
pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    crate::py::arrow_c::register_native_arrow_classes(m)?;
    crate::add_functions!(
        m;
        from_arrow,
        metadata::py_parse_geoarrow_extension_metadata,
        metadata::py_parse_geoparquet_column_frame,
        metadata::py_admit_geoparquet_geometry_storage,
    );
    Ok(())
}
