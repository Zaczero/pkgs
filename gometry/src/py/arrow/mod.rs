#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! `PyArrow` interchange — encode/decode gometry geometries to/from Arrow
//! arrays.
//!
//! Extracted from `lib.rs`; uses crate-root helpers directly (a child module
//! may access its ancestors' private items).

use std::sync::Arc;

pub(crate) use pyo3::exceptions::{PyModuleNotFoundError, PyTypeError};
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyAny, PyBytes, PyModule};
use serde_json::Value;

use crate::boundary::metadata::Frame;
use crate::geometry::{
    CoordSeq, CoordinateAxes, Coordinates, EmptyKind, HasM, HasZ, LineSeq, Point, Polygon, Ring,
    Shape, same_active_position, same_point,
};
use crate::py::errors::{GeometryError, InvalidGeometryError, ParseFormat, parse_error};
use crate::py::functions::geometry_io::guard_embedded_crs_conflict;
use crate::py::geoarrow::GeometryEncoding;
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

pub(crate) fn geometries_from_arrow_storages(
    py: Python<'_>,
    storages: Vec<ArrowStorage>,
    crs: Option<&Bound<'_, PyAny>>,
    epoch: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyAny>> {
    // Structural offset validation on every storage before any decode path
    // (masked pyarrow strip, packed import, or per-row append). Covers native
    // capsule/stream arrays that lack `is_null` and would otherwise skip the
    // pre-strip m01/D17/D18 checks that live only in the masked lane.
    for storage in &storages {
        ensure_pyarrow_storage_offsets_monotonic(py, storage)?;
    }
    let mut frame = None;
    for storage in &storages {
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
    // Geometry-level Arrow nulls become missing rows: import the null-free
    // storage densely, then scatter the present rows under the mask.
    let mut may_have_nulls = false;
    for storage in &storages {
        let null_count = storage
            .storage
            .bind(py)
            .getattr("null_count")?
            .extract::<i64>()?;
        // Known positive or unknown (-1) means a mask may be present.
        if null_count != 0 {
            may_have_nulls = true;
        }
    }
    let all_pyarrow = storages
        .iter()
        .all(|storage| storage.storage.bind(py).hasattr("is_null").unwrap_or(false));
    if may_have_nulls && all_pyarrow {
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
    for geometry in &mut geometries {
        if geometry.crs_ref().is_none() {
            geometry.set_crs_keep_epoch(crs.clone());
        }
        geometry.set_epoch_keep_crs(epoch);
    }
    let mask = crate::array::sparse_missing_mask(geometries.len(), &missing_rows);
    Ok(
        PyGeometryArray::pack_or_mixed(geometries, Frame::new(crs, epoch)?)
            .with_missing_mask(mask)
            .into_pyobject(py)?
            .unbind()
            .into(),
    )
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
/// * D17 — nested list levels validate the **entire physical child** window
///   (not only logical post-slice non-null rows), matching
///   ``array.validate(full=True)``.
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
                let offsets = arrow_i64_offsets(py, array)?;
                // Offset chain first (D18 non-negative / ordered). Data-buffer
                // length may be schema-derived from the terminal on the native
                // path and must not be read before offsets are proven valid.
                ensure_i64_offsets_monotonic(&offsets, offset, len, usize::MAX)?;
                let data_len = arrow_binary_data_buffer_len(array)?;
                let terminal = i64_offset_to_usize(offsets[offset + len])?;
                ensure_offset_terminal_within_child(terminal, data_len)
            },
            WkbOffsetWidth::Int32 => {
                let offsets = arrow_i32_offsets(py, array)?;
                ensure_i32_offsets_monotonic(&offsets, offset, len, usize::MAX)?;
                let data_len = arrow_binary_data_buffer_len(array)?;
                let terminal = i32_offset_to_usize(offsets[offset + len])?;
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
            // Nested list encodings: walk every nesting level's full physical
            // child window so a null-hidden or sliced-away non-mono ring/line
            // offset also fails (D17).
            ensure_nested_list_offsets_monotonic(py, array, storage.encoding)
        },
    }
}

fn ensure_nested_list_offsets_monotonic(
    py: Python<'_>,
    array: &Bound<'_, PyAny>,
    encoding: GeometryEncoding,
) -> PyResult<()> {
    // Depth of list nesting past the outer geometry list (0 = multipoint/line).
    let inner_lists = match encoding {
        GeometryEncoding::MultiPoint | GeometryEncoding::LineString => 0,
        GeometryEncoding::MultiLineString | GeometryEncoding::Polygon => 1,
        GeometryEncoding::MultiPolygon => 2,
        GeometryEncoding::Point | GeometryEncoding::Wkb => return Ok(()),
    };
    let mut current = array.getattr("values")?;
    for _ in 0..inner_lists {
        // Full child length (including rows only referenced by null/sliced-away
        // parent slots). `ensure` with count=0 still checks the start offset.
        let len = current.len()?;
        let level = ArrowListLevel::read(py, &current)?;
        level.ensure(0, len)?;
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

pub(crate) use axes::*;
pub(crate) use buffers::*;
pub(crate) use coordinates::*;
pub(crate) use export::*;
pub(crate) use export_buffers::*;
pub(crate) use import::*;
pub(crate) use import_nested::*;
pub(crate) use import_shapes::*;
pub(crate) use metadata::*;
pub(crate) use packed_import::*;

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
/// Schema, offsets, and encoding are validated defensively. Arrow C capsule
/// producers (``__arrow_c_array__`` / ``__arrow_c_stream__``) are trusted to be
/// ABI-conforming — a deliberately hostile duck-typed producer that forges its
/// own buffers is out of the threat model (same line as pyarrow; the C Data
/// Interface carries no buffer capacity except BinaryView's mandatory
/// variadic-sizes buffer, which is enforced. See ``docs/ecosystem/arrow.md``.
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
    );
    Ok(())
}
