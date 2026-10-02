//! Arrow interchange: attached protocol/metadata handling and the native
//! owned batch decoder entry point.

use std::num::NonZeroU32;
use std::sync::Arc;

use pyo3::exceptions::{PyModuleNotFoundError, PyTypeError};
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyAny, PyModule, PyTuple};

use crate::array::{GeometryArrayStorage, PackedStorage};
use crate::crs::frame::{
    Frame, epochs_equal, optional_crs_labels_operationally_equal, optional_epochs_equal,
};
use crate::geometry::{CoordSeqBuilder, CoordinateAxes, CsrOffsetBuilder};
use crate::py::arrow_c::{
    NativeArrowBatch, NativeArrowClassification, NativeArrowDecodeError, NativeDecodedBatch,
    NativeDecodedStorage,
};
use crate::py::geoarrow::GeometryEncoding;
use crate::py::wire_crs::SridFrameAdmission;
use crate::py::{arrow_c, errors, wire_crs};
use crate::{PyGeometryArray, crs_arc, parse_crs};

crate::tokens::token_enum! {
    /// Arrow export encoding.
    pub enum ArrowEncoding("encoding", param = "encoding") {
        Auto = "auto",
        Wkb = "wkb",
    }
}
crate::tokens::token_from_pyobject!(ArrowEncoding);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WkbOffsetWidth {
    Int32,
    Int64,
    View,
}

pub(crate) fn pyarrow_module(py: Python<'_>) -> PyResult<&Bound<'_, PyModule>> {
    static PYARROW: PyOnceLock<Py<PyModule>> = PyOnceLock::new();
    PYARROW
        .get_or_try_init(py, || {
            py.import("pyarrow")
                .map(Bound::unbind)
                .map_err(|error| map_pyarrow_import_error(py, error))
        })
        .map(|module| module.bind(py))
}

fn map_pyarrow_import_error(py: Python<'_>, error: PyErr) -> PyErr {
    let missing_pyarrow = error.is_instance_of::<PyModuleNotFoundError>(py)
        && error
            .value(py)
            .getattr("name")
            .ok()
            .and_then(|name| name.extract::<Option<String>>().ok().flatten())
            .as_deref()
            == Some("pyarrow");
    if missing_pyarrow {
        PyModuleNotFoundError::new_err(
            "Arrow interop requires pyarrow; install gometry with pyarrow available",
        )
    } else {
        error
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PyArrowInputKind {
    Container,
    Reader,
}

/// A genuine PyArrow object necessarily keeps its top-level module loaded.
/// Looking there avoids importing an optional dependency merely to inspect a
/// foreign Arrow-C provider.
fn loaded_pyarrow_module(py: Python<'_>) -> PyResult<Option<&Bound<'_, PyModule>>> {
    let modules = py.import("sys")?.getattr("modules")?;
    let module = modules.call_method1("get", ("pyarrow", py.None()))?;
    if module.is_none() {
        return Ok(None);
    }
    Ok(Some(pyarrow_module(py)?))
}

fn pyarrow_input_kind(
    pa: &Bound<'_, PyModule>,
    value: &Bound<'_, PyAny>,
) -> PyResult<Option<PyArrowInputKind>> {
    let reader = pa.getattr("RecordBatchReader")?;
    if value.is_instance(&reader)? {
        return Ok(Some(PyArrowInputKind::Reader));
    }
    for class_name in ["Array", "ChunkedArray", "Table", "RecordBatch"] {
        let class = pa.getattr(class_name)?;
        if value.is_instance(&class)? {
            return Ok(Some(PyArrowInputKind::Container));
        }
    }
    Ok(None)
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
    let explicit = crate::parse_crs(crs)?;
    wire_crs::guard_embedded_crs_conflict(
        embedded,
        explicit.as_ref().map(crate::Crs::as_str),
        "GeoArrow CRS",
    )?;
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
        (Some(embedded), Some(explicit)) if !epochs_equal(embedded, explicit) => {
            Err(errors::epoch_mismatch_error(
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

/// Accumulate producer-independent Arrow payloads and row metadata. Raw Arrow
/// shells and buffers are never stored here. Homogeneous primitive batches
/// remain packed until the final canonical array is published; unsupported or
/// structurally non-packable batches use the explicit mixed lane.
pub(crate) struct NativeArrowAccumulator {
    encoding: Option<GeometryEncoding>,
    wkb_offset_width: Option<WkbOffsetWidth>,
    #[expect(
        clippy::rc_buffer,
        reason = "the sharing handle must remain Arc<String> so capacity stays part of heap accounting"
    )]
    crs: Option<Arc<String>>,
    epoch: Option<f64>,
    storage: Vec<NativeDecodedStorage>,
    srids: Vec<Option<NonZeroU32>>,
    missing: Vec<usize>,
    length: usize,
}

impl NativeArrowAccumulator {
    pub(crate) const fn new() -> Self {
        Self {
            encoding: None,
            wkb_offset_width: None,
            crs: None,
            epoch: None,
            storage: Vec::new(),
            srids: Vec::new(),
            missing: Vec::new(),
            length: 0,
        }
    }

    pub(crate) fn admit_classification(
        &mut self,
        classification: &NativeArrowClassification,
    ) -> PyResult<()> {
        if let Some(encoding) = self.encoding
            && (encoding != classification.encoding
                || self.wkb_offset_width != Some(classification.wkb_offset_width))
        {
            return Err(geoarrow_parse_error(
                "Arrow batches have conflicting geometry storage classifications",
            ));
        }
        if let Some(existing) = self.crs.as_deref().map(String::as_str)
            && (!optional_crs_labels_operationally_equal(
                Some(existing),
                classification.crs.as_deref().map(String::as_str),
            )? || !optional_epochs_equal(self.epoch, classification.epoch))
        {
            return Err(geoarrow_parse_error(
                "Arrow chunks have mixed CRS/epoch metadata",
            ));
        }
        if self.encoding.is_none() {
            self.encoding = Some(classification.encoding);
            self.wkb_offset_width = Some(classification.wkb_offset_width);
            self.crs.clone_from(&classification.crs);
            self.epoch = classification.epoch;
        }
        Ok(())
    }

    pub(crate) fn push_decoded(&mut self, decoded: NativeDecodedBatch) -> PyResult<()> {
        let payload_len = decoded.storage.len();
        if (!decoded.srids.is_empty() && payload_len != decoded.srids.len())
            || payload_len.checked_add(decoded.missing.len()) != Some(decoded.length)
        {
            return Err(geoarrow_parse_error(
                "native Arrow decoder returned inconsistent row counts",
            ));
        }
        if decoded.length == 0 {
            // Admission and decoding above still validate every batch,
            // including offset-bearing empties. A genuinely zero-row batch
            // carries no row metadata; drop it before retaining an empty part.
            // Check the decoded length rather than storage length so a
            // non-empty all-missing batch still retains its row metadata.
            return Ok(());
        }
        let row_base = self.length;
        let new_length = row_base
            .checked_add(decoded.length)
            .ok_or_else(|| PyTypeError::new_err("Arrow row count overflows"))?;
        let NativeDecodedBatch {
            storage,
            mut srids,
            missing,
            length: decoded_length,
        } = decoded;
        for &local in &missing {
            if local >= decoded_length {
                self.release_owned_rows();
                drop((storage, srids, missing));
                return Err(geoarrow_parse_error(
                    "native Arrow decoder returned an out-of-range missing row",
                ));
            }
        }
        self.storage.reserve(1);
        self.srids.reserve(srids.len());
        self.missing.reserve(missing.len());
        for local in missing {
            self.missing.push(row_base.wrapping_add(local));
        }
        self.storage.push(storage);
        self.srids.append(&mut srids);
        self.length = new_length;
        Ok(())
    }

    fn release_owned_rows(&mut self) {
        self.storage = Vec::new();
        self.srids = Vec::new();
        self.missing = Vec::new();
    }

    pub(crate) fn finish(
        self,
        py: Python<'_>,
        crs: Option<&Bound<'_, PyAny>>,
        epoch: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let Self {
            encoding,
            crs: metadata_crs,
            epoch: metadata_epoch,
            storage,
            srids,
            missing: missing_rows,
            length,
            ..
        } = self;
        let explicit = parse_crs(crs)?;
        // Resolve explicit-vs-metadata precedence before row-level EWKB SRIDs.
        let _resolved_metadata_crs =
            reconcile_arrow_crs(metadata_crs.as_deref().map(String::as_str), crs)?;
        let mut frame = SridFrameAdmission::new(
            explicit,
            metadata_crs.as_deref().map(String::as_str).map(crs_arc),
        );
        let mut present = 0_usize;
        let has_srids = encoding == Some(GeometryEncoding::Wkb);
        let mut missing = missing_rows.iter().copied().peekable();
        for row in 0..length {
            if missing.peek() == Some(&row) {
                missing.next();
                continue;
            }
            let srid = if has_srids {
                srids.get(present).copied().ok_or_else(|| {
                    geoarrow_parse_error("native Arrow decoder returned inconsistent row counts")
                })?
            } else {
                None
            };
            present = present.wrapping_add(1);
            frame
                .admit_srid(srid, row, "Arrow import", "EWKB SRID")
                .map_err(PyErr::from)?;
            if let Some(storage_crs) = metadata_crs.as_deref().map(String::as_str) {
                frame
                    .guard_storage_srid(srid, Some(storage_crs), row)
                    .map_err(PyErr::from)?;
            }
        }
        if missing.peek().is_some() || (has_srids && present != srids.len()) {
            return Err(geoarrow_parse_error(
                "native Arrow decoder returned inconsistent row counts",
            ));
        }
        let crs = frame.finish().map_err(PyErr::from)?;
        let epoch = reconcile_arrow_epoch(metadata_epoch, epoch)?;
        let frame = Frame::new(crs, epoch)?;
        let storage = combine_native_storage(storage)?;
        let missing = crate::array::ValidityMask::from_sparse(length, &missing_rows);
        let output = PyGeometryArray::from_result_storage(
            Arc::new(storage),
            frame,
            missing,
            crate::array::StorageLayout::PresentOnly,
            crate::array::ComputedCaches::fresh(),
        )?;
        Ok(output.into_pyobject(py)?.into_any().unbind())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NativePackedFamily {
    Points(CoordinateAxes),
    Lines(CoordinateAxes),
    Polygons(CoordinateAxes),
}

fn native_packed_family(storage: &NativeDecodedStorage) -> Option<NativePackedFamily> {
    let NativeDecodedStorage::Packed(GeometryArrayStorage::Packed(storage)) = storage else {
        return None;
    };
    Some(match storage {
        PackedStorage::Points { coords, row_map } if row_map.is_identity() => {
            NativePackedFamily::Points(coords.axes())
        },
        PackedStorage::Lines {
            coords, row_map, ..
        } if row_map.is_identity() => NativePackedFamily::Lines(coords.axes()),
        PackedStorage::Polygons {
            coords, row_map, ..
        } if row_map.is_identity() => NativePackedFamily::Polygons(coords.axes()),
        PackedStorage::Points { .. }
        | PackedStorage::Lines { .. }
        | PackedStorage::Polygons { .. } => return None,
    })
}

/// Join direct packed batches once, after stream/chunk admission has finished.
/// A kind/axes mismatch or the packed i32 domain is a structural fallback to
/// mixed shape storage; allocation and size errors remain native errors.
fn combine_native_storage(
    parts: Vec<NativeDecodedStorage>,
) -> crate::error::Result<GeometryArrayStorage> {
    let Some(first) = parts.first() else {
        return Ok(GeometryArrayStorage::Mixed(Vec::new()));
    };
    if parts.len() == 1 {
        return match parts.into_iter().next().expect("one part exists") {
            NativeDecodedStorage::Packed(storage) => Ok(storage),
            NativeDecodedStorage::Mixed(shapes) => Ok(GeometryArrayStorage::Mixed(shapes)),
        };
    }
    let family = native_packed_family(first);
    let all_same_packed = family.is_some()
        && parts
            .iter()
            .all(|part| native_packed_family(part) == family);
    if all_same_packed && let Some(storage) = combine_packed_parts(&parts)? {
        return Ok(storage);
    }
    // The packed i32 domain is a structural limit. Re-open the parts as mixed
    // rows below; no allocation failure is hidden by this branch.

    let total = parts.iter().try_fold(0_usize, |total, part| {
        total
            .checked_add(part.len())
            .ok_or(crate::error::ResourceError::SizeOverflow)
    })?;
    let mut shapes = Vec::with_capacity(total);
    for part in parts {
        let mut part = part.into_shapes().map_err(|error| match error {
            NativeArrowDecodeError::Core(error) => error,
            NativeArrowDecodeError::GeoArrow(message) => {
                crate::geometry::GeometryErrorKind::message(message)
            },
        })?;
        shapes.append(&mut part);
    }
    Ok(GeometryArrayStorage::Mixed(shapes))
}

fn combine_packed_parts(
    parts: &[NativeDecodedStorage],
) -> crate::error::Result<Option<GeometryArrayStorage>> {
    let family = native_packed_family(parts.first().expect("non-empty parts"))
        .expect("packed parts have one family");
    let mut total_rows = 0_usize;
    let mut total_vertices = 0_usize;
    let mut total_rings = 0_usize;
    for part in parts {
        let NativeDecodedStorage::Packed(GeometryArrayStorage::Packed(storage)) = part else {
            unreachable!("packed parts have one family")
        };
        match storage {
            PackedStorage::Points { coords, .. } => {
                total_rows = total_rows
                    .checked_add(coords.len())
                    .ok_or(crate::error::ResourceError::SizeOverflow)?;
                total_vertices = total_rows;
            },
            PackedStorage::Lines {
                coords, offsets, ..
            } => {
                total_rows = total_rows
                    .checked_add(offsets.len().saturating_sub(1))
                    .ok_or(crate::error::ResourceError::SizeOverflow)?;
                total_vertices = total_vertices
                    .checked_add(coords.len())
                    .ok_or(crate::error::ResourceError::SizeOverflow)?;
            },
            PackedStorage::Polygons {
                coords,
                ring_offsets,
                polygon_offsets,
                ..
            } => {
                total_rows = total_rows
                    .checked_add(polygon_offsets.len().saturating_sub(1))
                    .ok_or(crate::error::ResourceError::SizeOverflow)?;
                total_rings = total_rings
                    .checked_add(ring_offsets.len().saturating_sub(1))
                    .ok_or(crate::error::ResourceError::SizeOverflow)?;
                total_vertices = total_vertices
                    .checked_add(coords.len())
                    .ok_or(crate::error::ResourceError::SizeOverflow)?;
            },
        }
    }
    if total_vertices > i32::MAX as usize || total_rings > i32::MAX as usize {
        return Ok(None);
    }

    match family {
        NativePackedFamily::Points(axes) => {
            let mut builder = CoordSeqBuilder::with_capacity(axes, total_vertices);
            for part in parts {
                let NativeDecodedStorage::Packed(GeometryArrayStorage::Packed(
                    PackedStorage::Points { coords, .. },
                )) = part
                else {
                    unreachable!("packed parts have one point family")
                };
                builder.extend_window(coords, 0..coords.len());
            }
            let coords = Arc::new(builder.finish()?);
            Ok(Some(GeometryArrayStorage::Packed(PackedStorage::Points {
                coords,
                row_map: crate::array::RowSelection::Identity,
            })))
        },
        NativePackedFamily::Lines(axes) => {
            let mut builder = CoordSeqBuilder::with_capacity(axes, total_vertices);
            let mut offsets = CsrOffsetBuilder::with_capacity(total_rows);
            for part in parts {
                let NativeDecodedStorage::Packed(GeometryArrayStorage::Packed(
                    PackedStorage::Lines {
                        coords: source,
                        offsets: source_offsets,
                        ..
                    },
                )) = part
                else {
                    unreachable!("packed parts have one line family")
                };
                for source_window in source_offsets.as_slice().windows(2) {
                    let [start, end] = source_window else {
                        unreachable!("offset windows contain two endpoints")
                    };
                    let window = *start as usize..*end as usize;
                    builder.extend_window(source, window);
                    offsets.push_end(builder.len(), i32::MAX as usize)?;
                }
            }
            let coords = builder.finish()?;
            let coord_len = coords.len();
            Ok(Some(GeometryArrayStorage::Packed(PackedStorage::Lines {
                coords: Arc::new(coords),
                offsets: offsets.finish(coord_len)?,
                row_map: crate::array::RowSelection::Identity,
            })))
        },
        NativePackedFamily::Polygons(axes) => {
            let mut builder = CoordSeqBuilder::with_capacity(axes, total_vertices);
            let mut ring_offsets = CsrOffsetBuilder::with_capacity(total_rings);
            let mut polygon_offsets = CsrOffsetBuilder::with_capacity(total_rows);
            let mut rings = 0_usize;
            for part in parts {
                let NativeDecodedStorage::Packed(GeometryArrayStorage::Packed(
                    PackedStorage::Polygons {
                        coords: source,
                        ring_offsets: source_ring_offsets,
                        polygon_offsets: source_polygon_offsets,
                        ..
                    },
                )) = part
                else {
                    unreachable!("packed parts have one polygon family")
                };
                let mut source_rings = source_ring_offsets.as_slice().windows(2);
                for source_polygon in source_polygon_offsets.as_slice().windows(2) {
                    let [ring_start, ring_end] = source_polygon else {
                        unreachable!("offset windows contain two endpoints")
                    };
                    let ring_count = (*ring_end as usize).wrapping_sub(*ring_start as usize);
                    for _ in 0..ring_count {
                        let source_window = source_rings
                            .next()
                            .expect("admitted polygon offsets cover every ring");
                        let [start, end] = source_window else {
                            unreachable!("offset windows contain two endpoints")
                        };
                        let window = *start as usize..*end as usize;
                        builder.extend_window(source, window);
                        ring_offsets.push_end(builder.len(), i32::MAX as usize)?;
                        rings = rings.wrapping_add(1);
                    }
                    polygon_offsets.push_end(rings, i32::MAX as usize)?;
                }
            }
            let coords = builder.finish()?;
            let coord_len = coords.len();
            Ok(Some(GeometryArrayStorage::Packed(
                PackedStorage::Polygons {
                    coords: Arc::new(coords),
                    ring_offsets: ring_offsets.finish(coord_len)?.cast_level(),
                    polygon_offsets: polygon_offsets.finish(rings)?.cast_level(),
                    row_map: crate::array::RowSelection::Identity,
                },
            )))
        },
    }
}

/// The selected PyArrow value and its optional table field. These Python
/// handles stay attached and are consumed only to enumerate/protocol-export
/// batches; no handle enters native decoding.
pub(crate) struct ArrowGeometryInput {
    pub(crate) value: Py<PyAny>,
    pub(crate) field: Option<Py<PyAny>>,
}

pub(crate) fn arrow_geometry_input(
    _pa: &Bound<'_, PyModule>,
    value: &Bound<'_, PyAny>,
) -> PyResult<ArrowGeometryInput> {
    if value
        .getattr_opt(pyo3::intern!(value.py(), "type"))?
        .is_some()
        || value
            .getattr_opt(pyo3::intern!(value.py(), "chunks"))?
            .is_some()
    {
        return Ok(ArrowGeometryInput {
            value: value.clone().unbind(),
            field: None,
        });
    }
    let names = value
        .getattr_opt(pyo3::intern!(value.py(), "column_names"))?
        .ok_or_else(|| {
            PyTypeError::new_err(
                "expected a GeoArrow-encoded Arrow array, ChunkedArray, Table, or RecordBatch",
            )
        })?;
    let names = crate::collect_py_strings(&names, "Arrow column names")?;
    if names.is_empty() {
        return Err(PyTypeError::new_err(
            "Arrow table or record batch has no columns",
        ));
    }
    let schema = value.getattr_opt(pyo3::intern!(value.py(), "schema"))?;
    let mut geometry_name_index = None;
    for (index, name) in names.iter().enumerate() {
        if name == "geometry" && geometry_name_index.replace(index).is_some() {
            return Err(PyTypeError::new_err(
                "Arrow table or record batch has multiple columns named 'geometry'; select one column explicitly",
            ));
        }
    }
    if let Some(index) = geometry_name_index {
        return Ok(ArrowGeometryInput {
            value: value.call_method1("column", (index,))?.unbind(),
            field: schema
                .as_ref()
                .map(|schema| schema.call_method1("field", (index,)))
                .transpose()?
                .map(Bound::unbind),
        });
    }
    let mut geometry_column = None;
    for index in 0..names.len() {
        let column = value.call_method1("column", (index,))?;
        let field = schema
            .as_ref()
            .map(|schema| schema.call_method1("field", (index,)))
            .transpose()?;
        if arrow_geometry_column_supported(&column, field.as_ref())? {
            if geometry_column.is_some() {
                return Err(PyTypeError::new_err(
                    "Arrow table or record batch has multiple geometry-like columns; use table['geometry'] or select one column explicitly",
                ));
            }
            geometry_column = Some(ArrowGeometryInput {
                value: column.unbind(),
                field: field.map(Bound::unbind),
            });
        }
    }
    geometry_column.ok_or_else(|| {
        PyTypeError::new_err(
            "expected an Arrow geometry array, chunked array, or table/record batch with a 'geometry' column or exactly one geometry-like column",
        )
    })
}

fn arrow_geometry_column_supported(
    value: &Bound<'_, PyAny>,
    field: Option<&Bound<'_, PyAny>>,
) -> PyResult<bool> {
    arrow_probe_type(
        metadata::arrow_classification_for_value(value, field),
        value.py(),
    )
}

/// Table autodetection treats an ordinary unsupported column as a negative
/// classification, but provider failures and malformed metadata remain visible.
fn arrow_probe_type<T>(result: PyResult<T>, py: Python<'_>) -> PyResult<bool> {
    match result {
        Ok(_) => Ok(true),
        Err(error) if error.is_instance_of::<PyTypeError>(py) => Ok(false),
        Err(error) => Err(error),
    }
}

fn pyarrow_batch(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    classification: NativeArrowClassification,
) -> PyResult<NativeArrowBatch> {
    let capsules = value.call_method1("__arrow_c_array__", (py.None(),))?;
    let tuple = capsules.cast::<PyTuple>()?;
    if tuple.len() != 2 {
        return Err(PyTypeError::new_err(
            "__arrow_c_array__ must return (schema_capsule, array_capsule)",
        ));
    }
    let schema = tuple.get_item(0)?;
    let array = tuple.get_item(1)?;
    let batch = arrow_c::native_arrow_batch_from_capsules_with_classification(
        &schema,
        &array,
        classification,
    )?;
    drop(capsules);
    Ok(batch)
}

fn decode_pyarrow_selected(
    py: Python<'_>,
    selected: &Bound<'_, PyAny>,
    classification: NativeArrowClassification,
    crs: Option<&Bound<'_, PyAny>>,
    epoch: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyAny>> {
    let mut accumulator = NativeArrowAccumulator::new();
    accumulator.admit_classification(&classification)?;

    if let Some(chunks) = selected.getattr_opt(pyo3::intern!(py, "chunks"))? {
        for (index, chunk) in chunks.try_iter()?.enumerate() {
            let chunk = chunk?;
            crate::check_python_signals(py, index)?;
            let batch = pyarrow_batch(py, &chunk, classification.clone())?;
            let decoded = py
                .detach(move || batch.decode())
                .map_err(NativeArrowDecodeError::into_py_err)?;
            accumulator.push_decoded(decoded)?;
        }
    } else {
        let batch = pyarrow_batch(py, selected, classification)?;
        let decoded = py
            .detach(move || batch.decode())
            .map_err(NativeArrowDecodeError::into_py_err)?;
        accumulator.push_decoded(decoded)?;
    }
    accumulator.finish(py, crs, epoch)
}

pub(crate) fn geometries_from_pyarrow_with_classification(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    classification: NativeArrowClassification,
    crs: Option<&Bound<'_, PyAny>>,
    epoch: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyAny>> {
    decode_pyarrow_selected(py, value, classification, crs, epoch)
}

pub(crate) fn geometries_from_pyarrow(
    py: Python<'_>,
    pa: &Bound<'_, PyModule>,
    value: &Bound<'_, PyAny>,
    crs: Option<&Bound<'_, PyAny>>,
    epoch: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyAny>> {
    let input = arrow_geometry_input(pa, value)?;
    let selected = input.value.bind(py);
    let field = input.field.as_ref().map(|field| field.bind(py));
    // Resolve both ExtensionType and field metadata before consuming any
    // selected value's Arrow-C export.
    let classification = metadata::arrow_classification_for_value(selected, field)?;
    decode_pyarrow_selected(py, selected, classification, crs, epoch)
}

#[pyfunction]
#[pyo3(
    name = "_from_geoparquet_arrow",
    signature = (data, layout, *, crs = None, epoch = None)
)]
fn py_from_geoparquet_arrow(
    py: Python<'_>,
    data: &Bound<'_, PyAny>,
    layout: &Bound<'_, PyAny>,
    crs: Option<&Bound<'_, PyAny>>,
    epoch: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyAny>> {
    let layout = layout.cast::<metadata::GeoParquetPhysicalLayout>()?.get();
    let embedded_crs = crate::parse_crs(crs)?.map(|value| value.as_str().to_owned());
    let embedded_epoch = crate::coordinate_epoch_option("epoch", epoch)?;
    let classification = layout.classification(embedded_crs, embedded_epoch);
    geometries_from_pyarrow_with_classification(py, data, classification, crs, epoch)
}

pub(crate) use metadata::{
    decode_extension_name, geoarrow_parse_error, parse_geoarrow_extension_metadata_for,
};

mod axes;
mod export;
mod metadata;

pub(crate) use axes::accumulate_geometry_axes;
pub(crate) use export::{array_to_arrow, shapes_to_arrow, shapes_to_wkb_arrow};

/// Build geometries from an Arrow array, table, record batch, reader, or
/// Arrow-C provider. Protocol callbacks and PyArrow object inspection remain
/// attached; selected buffers are owned before the decoder is detached.
/// Arrow null slots become missing rows; typed empty geometries remain typed
/// empty values. Empty arrays and streams return an empty array while retaining
/// compatible CRS and epoch metadata when the input provides it.
///
/// Parameters
/// ----------
/// data : object
///     A foreign PyArrow ``Array``, ``ChunkedArray``, ``Table``,
///     ``RecordBatch``, or ``RecordBatchReader``, or an object implementing
///     ``__arrow_c_array__`` or ``__arrow_c_stream__``. The input must use a
///     supported GeoArrow encoding, including GeoArrow WKB.
///
/// crs : str, int, or CRS-like object, optional
///     CRS to attach when the input has no embedded CRS. If embedded metadata
///     is present, it must be operationally compatible with this value and the
///     explicit value is retained as the result's CRS. Passing ``None`` does
///     not strip compatible embedded metadata.
///
/// epoch : float, optional
///     Coordinate epoch (decimal year) to attach as frame metadata. A supplied
///     epoch must be finite and have a compatible embedded or explicit CRS; an
///     embedded epoch is preserved when this argument is omitted.
///
/// Returns
/// -------
/// GeometryArray[Geometry | None]
///     Decoded geometry rows. Arrow null slots become missing ``None`` rows;
///     typed empty geometries remain typed empty values. Empty input produces
///     an empty array, retaining compatible CRS and epoch metadata when
///     available.
///
/// Raises
/// ------
/// CRSError
///     If ``crs`` or an embedded EWKB SRID cannot be resolved, or ``epoch`` is
///     supplied without a CRS or with a static CRS.
/// CRSMismatchError
///     If explicit CRS or epoch metadata conflicts with embedded metadata, or
///     EWKB row SRIDs disagree.
/// GeometryError
///     If ``epoch`` is not finite or admitted geometry data violates a geometry
///     invariant.
/// ParseError
///     If GeoArrow metadata, offsets, validity buffers, coordinate buffers, or
///     WKB data are malformed, or a selected table geometry child is absent
///     or out of range.
/// TypeError
///     If ``data`` is not a supported Arrow object or provider, its schema or
///     encoding cannot be classified, it has no geometry column, a C array
///     length or offset is negative, its ``null_count`` is below -1 (``-1``
///     means unknown) or a known count exceeds the logical length, or its
///     format-specific buffer/child layout conflicts with the schema.
/// OSError
///     If an Arrow C stream schema or batch callback returns a nonzero status
///     other than ``ENOMEM``; the callback status is exposed as ``errno``.
/// MemoryError
///     If an Arrow C stream callback returns ``ENOMEM``. Native Rust allocator
///     exhaustion is process-fatal and is not translated into a Python error.
/// OverflowError
///     If a nonnegative Arrow buffer byte span cannot fit ``usize`` or the
///     maximum size of a Rust slice.
///
/// See Also
/// --------
/// GeometryArray.to_arrow : Export an array as a GeoArrow array.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> source = gm.points([1.0], [2.0])
/// >>> restored = gm.from_arrow(source.to_arrow())
/// >>> restored.to_wkt()
/// ['POINT (1 2)']
#[pyfunction]
#[pyo3(signature = (data, *, crs = None, epoch = None))]
fn from_arrow(
    py: Python<'_>,
    data: &Bound<'_, PyAny>,
    crs: Option<&Bound<'_, PyAny>>,
    epoch: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyAny>> {
    if crate::exact_geometry(data).is_some() || crate::exact_geometry_array(data).is_some() {
        return Err(PyTypeError::new_err(
            "from_arrow expects foreign Arrow data; a Geometry or GeometryArray is already decoded",
        ));
    }
    if let Some(pa) = loaded_pyarrow_module(py)? {
        match pyarrow_input_kind(pa, data)? {
            Some(PyArrowInputKind::Container) => {
                return geometries_from_pyarrow(py, pa, data, crs, epoch);
            },
            Some(PyArrowInputKind::Reader) => {
                // Reader consumption and table construction are attached; the
                // selected table column still enters the same C-export decoder.
                let table = data.call_method0("read_all")?;
                return geometries_from_pyarrow(py, pa, &table, crs, epoch);
            },
            None => {},
        }
    }
    if let Some(output) = arrow_c::geometries_from_arrow_c(py, data, crs, epoch)? {
        return Ok(output);
    }
    Err(PyTypeError::new_err(
        "expected a GeoArrow-encoded Arrow array, ChunkedArray, Table, RecordBatch, RecordBatchReader, or Arrow C provider",
    ))
}

/// Register the Arrow interchange free functions on the module.
pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    crate::add_functions!(
        m;
        from_arrow,
        py_from_geoparquet_arrow,
        metadata::py_parse_geoarrow_extension_metadata,
        metadata::py_parse_geoparquet_column_frame,
        metadata::py_admit_geoparquet_geometry_storage,
    );
    m.add_class::<metadata::GeoParquetColumnFrame>()?;
    m.add_class::<metadata::GeoParquetPhysicalLayout>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use pyo3::exceptions::{PyImportError, PyModuleNotFoundError, PyRuntimeError};
    use pyo3::ffi::c_str;
    use pyo3::prelude::*;
    use pyo3::types::PyDict;

    use super::{NativeArrowAccumulator, NativeDecodedBatch, NativeDecodedStorage, pyarrow_module};

    #[test]
    fn empty_decoded_batch_is_not_retained() {
        let decoded = NativeDecodedBatch {
            length: 0,
            storage: NativeDecodedStorage::Mixed(Vec::new()),
            srids: Vec::new(),
            missing: Vec::new(),
        };
        let mut accumulator = NativeArrowAccumulator::new();
        let ((), allocations) = crate::test_support::allocation_count(|| {
            accumulator
                .push_decoded(decoded)
                .expect("empty decoded batch should be accepted");
        });
        assert_eq!(allocations, 0);
        assert!(accumulator.storage.is_empty());
        assert_eq!(accumulator.storage.capacity(), 0);
        assert_eq!(accumulator.length, 0);
    }

    #[test]
    fn pyarrow_import_reclassifies_only_missing_root_module() {
        Python::initialize();
        Python::attach(|py| -> PyResult<()> {
            let locals = PyDict::new(py);
            py.run(
                c_str!(
                    r#"import sys
class FailureFinder:
    mode = 'missing'
    def find_spec(self, fullname, path=None, target=None):
        if fullname != 'pyarrow': return None
        if self.mode == 'missing':
            raise ModuleNotFoundError("No module named 'pyarrow'", name='pyarrow')
        if self.mode == 'broken':
            raise RuntimeError('broken pyarrow initialization')
        if self.mode == 'abi':
            raise ImportError('pyarrow ABI mismatch')
        raise ModuleNotFoundError("No module named 'numpy'", name='numpy')
finder = FailureFinder()
sys.modules.pop('pyarrow', None)
sys.meta_path.insert(0, finder)"#
                ),
                Some(&locals),
                None,
            )?;

            let Err(missing) = pyarrow_module(py) else {
                panic!("missing pyarrow should fail");
            };
            assert!(missing.is_instance_of::<PyModuleNotFoundError>(py));
            assert!(missing.value(py).to_string().contains("install gometry"));

            let finder = locals
                .get_item("finder")?
                .expect("finder is defined")
                .unbind();
            finder.bind(py).setattr("mode", "broken")?;
            let Err(broken) = pyarrow_module(py) else {
                panic!("broken pyarrow should fail");
            };
            assert!(broken.is_instance_of::<PyRuntimeError>(py));
            assert!(
                broken
                    .value(py)
                    .to_string()
                    .contains("broken pyarrow initialization")
            );

            finder.bind(py).setattr("mode", "abi")?;
            let Err(abi) = pyarrow_module(py) else {
                panic!("ABI failure should fail");
            };
            assert!(abi.is_instance_of::<PyImportError>(py));
            assert!(abi.value(py).to_string().contains("pyarrow ABI mismatch"));

            finder.bind(py).setattr("mode", "nested")?;
            let Err(nested) = pyarrow_module(py) else {
                panic!("nested dependency failure should fail");
            };
            assert!(nested.is_instance_of::<PyModuleNotFoundError>(py));
            assert!(
                nested
                    .value(py)
                    .to_string()
                    .contains("No module named 'numpy'")
            );

            py.run(
                c_str!("sys.meta_path.remove(finder)\nsys.modules.pop('pyarrow', None)"),
                Some(&locals),
                None,
            )?;
            Ok(())
        })
        .expect("Python import-failure probe should complete");
    }
}
