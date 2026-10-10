//! Core `GeometryArray` methods: construction, metadata, properties, iteration.

use numpy::PyArrayMethods as _;
use pyo3::PyErr;
use pyo3::types::{PyInt, PyList};

use crate::array::{
    ArrayRow, Bound, Bounds, CRSError, Crs, FrameAdoption, GeometryKind, Py, PyAny,
    PyAnyMethods as _, PyCoordinates, PyCrs, PyDict, PyDictMethods as _, PyGeometry,
    PyGeometryArray, PyListMethods as _, PyRef, PyResult, PyTuple, PyTypeError, PyTypeMethods as _,
    Python, Result, SVG_ARRAY_PREVIEW, Shape, bool_array, coerce_present_geometry_items,
    coordinate_epoch_option, crs, crs_arc, exact_geometry, geometry,
    geometry_array_svg_grid_html_masked, line_is_ccw, line_is_simple, line_is_valid,
    map_coordinates_callback, parse_coordinate_replacement, parse_crs, polygon_is_valid, pymethods,
    replace_shape_coordinates, require_geojson_crs, slice_replacement_for_shape,
};
use crate::boundary::coordinates::checked_vector_len;
use crate::boundary::metadata;
use crate::crs::frame;
use crate::crs::frame::FrameError;
#[cfg(test)]
use crate::geometry::CoordSeq;
use crate::geometry::storage::kernels::{
    geographic_total_bounds_from_columns, raw_total_bounds_from_columns,
};
use crate::geometry::storage::packed_columns::validate_replacement_ring;
use crate::geometry::storage::replacement::CoordinateReplacement;
use crate::geometry::storage::{GeometryArrayStorage, PackedStorage, physical_row};
use crate::geometry::{
    ShapeCountQuery, TopologyDomain, line_is_closed_in_domain, shape_is_closed_in_domain, storage,
};
use crate::predicates::unary;
use crate::py::functions::bulk_rows::{StreamedShape, stream_bulk_owned};
use crate::py::functions::geometry_io;
use crate::py::numpy::int64_array;
use crate::py::support::{geojson_domain_metric, own_geometry_item, parse_owned_geometry_item};
use crate::py::vectors::Groups;
use crate::py::viz;

fn type_module(value: &Bound<'_, PyAny>) -> PyResult<String> {
    value.get_type().getattr("__module__")?.extract()
}

fn polygon_row_is_valid(polygon: Option<&crate::geometry::Polygon>) -> Result<bool> {
    polygon.map_or(Ok(true), polygon_is_valid)
}

fn normalize_streamed_geometry(
    mut geometry: PyGeometry,
    explicit_crs: Option<&Crs>,
    explicit_epoch: Option<f64>,
    row: usize,
) -> Result<PyGeometry> {
    if let Some(expected) = explicit_crs {
        if let Some(actual) = geometry.crs_ref()
            && !frame::crs_operationally_equal(actual, expected)?
        {
            return Err(FrameError::SharedCrs {
                context: "GeometryArray".into(),
                index: row,
                first: Some(expected.clone()),
                other: Some(actual.clone()),
            }
            .into());
        }
        geometry.set_crs_keep_epoch(Some(expected.clone()))?;
    }
    if let Some(expected) = explicit_epoch {
        if let Some(actual) = geometry.epoch()
            && !frame::epochs_equal(actual, expected)
        {
            return Err(FrameError::SharedEpoch {
                context: "GeometryArray".into(),
                index: row,
                first: Some(expected),
                other: Some(actual),
            }
            .into());
        }
        geometry.set_epoch_keep_crs(Some(expected))?;
    }
    Ok(geometry)
}

fn is_shapely_geometry(value: &Bound<'_, PyAny>) -> PyResult<bool> {
    Ok(type_module(value)?.starts_with("shapely.geometry."))
}

/// Vectorize homogeneous Shapely list/object-ndarray ingestion through one
/// lazy ``shapely.to_wkb`` call. The first item is the cheap discriminator;
/// the full scan prevents a mixed Shapely/gometry batch from changing its
/// established per-object coercion semantics.
fn shapely_wkb_batch<'py>(
    values: &Bound<'py, PyAny>,
    items: &[Bound<'py, PyAny>],
) -> PyResult<Option<Bound<'py, PyAny>>> {
    let is_list = values.cast::<PyList>().is_ok();
    let value_type = values.get_type();
    let is_ndarray = value_type.name()? == "ndarray"
        && value_type.getattr("__module__")?.extract::<&str>()? == "numpy";
    let Some(first) = items.first() else {
        return Ok(None);
    };
    if !(is_list || is_ndarray) || !is_shapely_geometry(first)? {
        return Ok(None);
    }
    for item in items {
        if !item.is_none() && !is_shapely_geometry(item)? {
            return Ok(None);
        }
    }
    Ok(Some(
        values
            .py()
            .import("shapely")?
            .getattr("to_wkb")?
            .call1((values,))?,
    ))
}

/// Adopt dense identity-packed replacement columns directly. Selected and
/// masked arrays need logical-row gathering/scattering and stay on the generic
/// packed dispatcher below.
fn replace_dense_packed_coordinates(
    array: &PyGeometryArray,
    replacement: CoordinateReplacement,
) -> PyResult<Result<PyGeometryArray, CoordinateReplacement>> {
    if array.has_missing() {
        return Ok(Err(replacement));
    }
    match array.storage() {
        GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map })
            if row_map.is_identity() =>
        {
            debug_assert_eq!(coords.len(), replacement.len);
            let coords = replacement.apply_to_seq(coords)?;
            Ok(Ok(PyGeometryArray::packed_points(
                coords,
                array.frame.clone(),
            )?))
        },
        GeometryArrayStorage::Packed(PackedStorage::Lines {
            coords,
            offsets,
            row_map,
        }) if row_map.is_identity() => {
            debug_assert_eq!(coords.len(), replacement.len);
            let coords = replacement.apply_to_seq(coords)?;
            Ok(Ok(PyGeometryArray::packed_lines(
                coords,
                offsets.clone(),
                array.frame.clone(),
            )?))
        },
        GeometryArrayStorage::Packed(PackedStorage::Polygons {
            coords,
            ring_offsets,
            polygon_offsets,
            row_map,
        }) if row_map.is_identity() => {
            debug_assert_eq!(coords.len(), replacement.len);
            let coords = replacement.apply_to_seq(coords)?;
            for [start, end] in ring_offsets.array_windows::<2>() {
                validate_replacement_ring(&coords, *start as usize..*end as usize)?;
            }
            Ok(Ok(PyGeometryArray::packed_polygons(
                coords,
                ring_offsets.clone(),
                polygon_offsets.clone(),
                array.frame.clone(),
            )?))
        },
        GeometryArrayStorage::Packed(
            PackedStorage::Points { .. }
            | PackedStorage::Lines { .. }
            | PackedStorage::Polygons { .. },
        )
        | GeometryArrayStorage::Mixed(_) => Ok(Err(replacement)),
    }
}

fn replace_array_coordinates(
    array: &PyGeometryArray,
    py: Python<'_>,
    replacement: CoordinateReplacement,
) -> PyResult<PyGeometryArray> {
    let replacement = match replace_dense_packed_coordinates(array, replacement)? {
        Ok(output) => return Ok(output),
        Err(replacement) => replacement,
    };
    if !matches!(array.storage(), GeometryArrayStorage::Mixed(_)) {
        return Ok(array
            .replace_packed_coords_detached(py, array.frame.clone(), replacement)?
            .expect("non-Mixed storage has packed columns"));
    }
    let mut present_cursor = 0_usize;
    let mut counts = ShapeCountQuery::default();
    let mut shapes = Vec::with_capacity(
        array
            .missing()
            .map_or(array.len(), storage::ValidityMask::present_count),
    );
    for row in array.row_operation().present_rows() {
        let logical = row.logical();
        let count = row.row().coordinates(&mut counts).map_err(PyErr::from)?;
        let next_present_cursor = present_cursor
            .checked_add(count)
            .ok_or_else(|| crate::error::Error::from(crate::error::ResourceError::SizeOverflow))
            .map_err(PyErr::from)?;
        let sub = slice_replacement_for_shape(&replacement, present_cursor, count);
        present_cursor = next_present_cursor;
        shapes.push(
            row.row()
                .try_with_shape(|shape| replace_shape_coordinates(shape, &sub))
                .map_err(|error| crate::note_array_row(error, logical))?,
        );
    }
    debug_assert_eq!(present_cursor, replacement.len);
    Ok(array.scatter_present_result(PyGeometryArray::from_shapes(shapes, array.frame.clone())?)?)
}

#[expect(
    clippy::used_underscore_items,
    reason = "the leading underscore is the Python convention for a module-private helper, and pickle stores these names in its stream, so the name is a wire contract rather than a Rust style choice"
)]
#[pymethods]
impl PyGeometryArray {
    /// Estimate one conformal metric CRS for all present rows.
    ///
    /// Missing rows are ignored. The complete present extent is evaluated
    /// against a fixed 0.1% linear scale-error ceiling; empty/all-missing,
    /// CRS-free, or geographically unsafe arrays raise ``CRSError``.
    ///
    /// Returns
    /// -------
    /// CRS
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> arr = gm.GeometryArray([gm.Point(-122.4, 37.8, crs=4326)])
    /// >>> arr.estimate_local_crs()
    /// CRS("EPSG:32610")
    pub fn estimate_local_crs(&self) -> PyResult<PyCrs> {
        let source = self.crs_str().ok_or_else(|| {
            CRSError::new_err("estimate_local_crs requires a CRS-tagged geometry array")
        })?;
        let shapes = self
            .present_shape_rows()
            .map(|result| result.map(|(_, shape)| shape).map_err(PyErr::from))
            .filter(|result| !matches!(result, Ok(shape) if shape.is_empty()))
            .collect::<pyo3::PyResult<Vec<_>>>()?;
        if shapes.is_empty() {
            return Err(CRSError::new_err(
                "estimate_local_crs requires at least one present, non-empty geometry",
            ));
        }
        let collection = Shape::geometry_collection(shapes)?;
        Ok(PyCrs::from_definition(crs_arc(
            crs::local::estimate_local_crs(&collection, source)?,
        )))
    }

    // NEP 13: opt out of numpy ufunc dispatch (we have our own & | - ^ /
    // predicates)
    #[classattr]
    #[expect(non_upper_case_globals, reason = "Python dunder name")]
    const __array_ufunc__: Option<Py<PyAny>> = None;

    /// Construct an immutable array from geometry-like rows.
    ///
    /// Iterable values are consumed once in input order. ``None`` creates a
    /// missing row. A single ``Geometry`` is not a row iterable; wrap it in a
    /// one-item sequence. Rows may be native geometries, WKB byte buffers,
    /// GeoJSON mappings, or objects exposing ``__geo_interface__``. ``crs=``
    /// attaches shared metadata and never transforms coordinates.
    ///
    /// Parameters
    /// ----------
    /// values : iterable of Geometry, Buffer, Mapping, or None
    ///     One native geometry, WKB buffer, GeoJSON mapping, or object exposing
    ///     ``__geo_interface__`` per input position; ``None`` marks a missing
    ///     row.
    /// crs : CrsInput or None, optional, default None
    ///     Shared CRS label. CRS-free rows adopt it; existing CRS values must
    ///     be equivalent to it.
    /// epoch : float or None, optional, default None
    ///     Shared coordinate epoch for a CRS that admits geometry epochs.
    ///     Existing epochs must agree with it.
    ///
    /// Returns
    /// -------
    /// GeometryArray
    ///     An immutable, row-aligned array with missing positions preserved.
    ///
    /// Raises
    /// ------
    /// TypeError
    ///     If ``values`` is a single geometry or contains an unsupported value.
    /// CRSError
    ///     If ``crs`` cannot be resolved or an epoch is invalid for its CRS.
    /// CRSMismatchError
    ///     If input CRS or epoch metadata conflicts with the shared frame.
    /// GeometryError
    ///     If ``epoch`` is non-finite.
    /// ParseError
    ///     If a WKB or GeoJSON row is malformed.
    #[new]
    #[pyo3(signature = (values, *, crs = None, epoch = None))]
    pub fn new(
        values: &Bound<'_, PyAny>,
        crs: Option<&Bound<'_, PyAny>>,
        epoch: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        if exact_geometry(values).is_some() {
            return Err(PyTypeError::new_err(
                "GeometryArray requires an iterable; use GeometryArray([geom]) for one row or GeometryArray(geom.parts) to explode parts explicitly",
            ));
        }
        let value_type = values.get_type();
        let is_ndarray = value_type.name()? == "ndarray"
            && value_type.getattr("__module__")?.extract::<&str>()? == "numpy";
        if values.cast::<PyList>().is_err() && !is_ndarray {
            let explicit_crs = parse_crs(crs)?;
            let explicit_epoch = coordinate_epoch_option("epoch", epoch)?;
            let policy_crs = explicit_crs.clone();
            let parse_crs = explicit_crs.clone();
            let iter = values.try_iter()?;
            return stream_bulk_owned(
                values,
                iter,
                explicit_crs,
                explicit_epoch,
                Some("GeometryArray"),
                move |item, _row| {
                    own_geometry_item(
                        item,
                        crate::io::LegacyGeoJsonCrsPolicy::Adopt(policy_crs.as_deref()),
                    )
                },
                move |item, row| {
                    let geometry = parse_owned_geometry_item(
                        item,
                        crate::io::LegacyGeoJsonCrsPolicy::Adopt(parse_crs.as_deref()),
                    )?;
                    let geometry = normalize_streamed_geometry(
                        geometry,
                        parse_crs.as_ref(),
                        explicit_epoch,
                        row,
                    )?;
                    Ok(StreamedShape::Present {
                        shape: geometry.shape.shape().clone(),
                        crs: geometry.crs_ref().cloned(),
                        epoch: geometry.epoch(),
                    })
                },
            );
        }
        // Concrete list/ndarray batches retain their existing Shapely fast path
        // and batch coercion; arbitrary iterators take the row-at-a-time lane.
        let raw_items = crate::collect_py_iter(values, Ok)?;
        if let Some(wkb) = shapely_wkb_batch(values, &raw_items)? {
            let parsed = geometry_io::from_wkb(values.py(), &wkb, crs, epoch)?;
            return Ok(parsed
                .bind(values.py())
                .extract::<PyRef<'_, Self>>()?
                .clone());
        }
        // First-class missing rows: `None` items mark the validity lane; only
        // present items participate in coercion and frame resolution. The
        // native row builder restores their logical positions on publication.
        let mut mask = Vec::with_capacity(raw_items.len());
        mask.extend(raw_items.iter().map(|item| item.is_none()));
        let explicit_crs = parse_crs(crs)?;
        let explicit_epoch = coordinate_epoch_option("epoch", epoch)?;
        let coerced = coerce_present_geometry_items(
            raw_items.iter().filter(|item| !item.is_none()),
            false,
            crate::io::LegacyGeoJsonCrsPolicy::Adopt(explicit_crs.as_deref()),
        )?;
        drop(raw_items);
        let py = values.py();
        let result = py.detach(move || -> Result<Self> {
            let mut items = coerced;
            let frame = metadata::resolve_geometry_frames(
                &mut items,
                FrameAdoption {
                    crs: explicit_crs,
                    epoch: explicit_epoch,
                },
                "GeometryArray",
            )?;
            let mut rows = crate::array::StreamingShapes::with_capacity(0, mask.len());
            let mut present = items.into_iter();
            for is_missing in mask {
                if is_missing {
                    rows.try_push_missing()?;
                } else {
                    let geometry = present
                        .next()
                        .expect("mask and present geometry counts agree");
                    let shape = match std::sync::Arc::try_unwrap(geometry.shape) {
                        Ok(data) => data.into_shape(),
                        Err(data) => data.shape().clone(),
                    };
                    rows.try_push(shape)?;
                }
            }
            let extra = present.next();
            debug_assert!(extra.is_none());
            rows.finish(frame)
        });
        let array = match result {
            Ok(array) => array,
            Err(error) => {
                return Err(PyErr::from(error));
            },
        };
        Ok(array)
    }

    pub fn _repr_html_(&self, py: Python<'_>) -> PyResult<String> {
        if let Some(html) = viz::try_array_repr_html(py, self)? {
            return Ok(html);
        }
        Ok(geometry_array_svg_grid_html_masked(
            self.row_operation().rows_with_missing(|| ()),
            self.len(),
            SVG_ARRAY_PREVIEW,
        )?)
    }

    pub fn _repr_html_svg(&self) -> PyResult<String> {
        Ok(geometry_array_svg_grid_html_masked(
            self.row_operation().rows_with_missing(|| ()),
            self.len(),
            SVG_ARRAY_PREVIEW,
        )?)
    }
    /// CRS shared by every geometry in the array, or ``None``.
    ///
    /// Returns
    /// -------
    /// CRS or None
    #[getter]
    pub fn crs(&self) -> Option<PyCrs> {
        self.crs_ref().cloned().map(PyCrs::from_definition)
    }
    /// Coordinate epoch shared by the array, if set.
    ///
    /// Returns
    /// -------
    /// float or None
    #[getter]
    pub const fn epoch(&self) -> Option<f64> {
        self.frame.epoch()
    }
    /// ``__geo_interface__`` for the whole array: a GeoJSON-style
    /// WGS84 longitude/latitude ``FeatureCollection`` mapping, with one
    /// ``Feature`` per row, positional ``id``, and empty ``properties`` — the
    /// shape geopandas and mapping libraries expect from a geometry column.
    #[getter("__geo_interface__")]
    pub fn geo_interface<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, pyo3::types::PyDict>> {
        use pyo3::types::{PyDict, PyList};
        require_geojson_crs(self.crs_str())?;
        if self.has_m() || self.epoch().is_some() {
            return Err(crate::PyTypeError::new_err(
                crate::io::geojson_interface_error(),
            ));
        }
        let array = self.clone();
        let mut metric_scratch = crate::dispatch::MetricScratch::default();
        let (domain, metric) = geojson_domain_metric(self.crs_str(), &mut metric_scratch)?;
        let metric_access = move || {
            metric.geographic_chart_metric()?.ok_or_else(|| {
                crate::crs::CrsError::invalid(
                    "geographic GeoJSON source ordinates require the array CRS metric",
                )
            })
        };
        let geometry_texts = py
            .detach(move || {
                let mut texts: Vec<Option<String>> = Vec::with_capacity(array.len());
                for row in array.row_operation().rows_with_missing(|| ()) {
                    let text = match row {
                        ArrayRow::Missing { .. } => None,
                        ArrayRow::Present(row) => Some(row.row().try_with_shape(|shape| {
                            crate::io::to_geojson_string_with_metric(shape, domain, &metric_access)
                        })?),
                    };
                    texts.push(text);
                }
                Ok::<_, crate::error::Error>(texts)
            })
            .map_err(crate::PyErr::from)?;
        let features = PyList::empty(py);
        for (row, text) in geometry_texts.iter().enumerate() {
            let feature = PyDict::new(py);
            feature.set_item("type", "Feature")?;
            feature.set_item("id", row)?;
            feature.set_item("properties", PyDict::new(py))?;
            let geometry = match text {
                Some(text) => crate::boundary::json_text_to_py(py, text)?.into_bound(py),
                None => py.None().into_bound(py),
            };
            feature.set_item("geometry", geometry)?;
            features.append(feature)?;
        }
        let collection = PyDict::new(py);
        collection.set_item("type", "FeatureCollection")?;
        collection.set_item("features", features)?;
        Ok(collection)
    }
    /// Per-geometry ordinate layout (see `Geometry.coordinate_axes`), with
    /// ``None`` at missing rows.
    ///
    /// Returns
    /// -------
    /// list of str or None
    ///     One ``'XY'``/``'XYZ'``/``'XYM'``/``'XYZM'`` token per row.
    #[getter("coordinate_axes")]
    pub fn coordinate_axes_rows(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let rows = self
            .row_operation()
            .rows_with_missing(|| ())
            .map(|row| match row {
                ArrayRow::Missing { .. } => None,
                ArrayRow::Present(row) => Some(row.row().axes().as_str()),
            });
        Ok(PyList::new(py, rows)?.into_any().unbind())
    }
    /// Ordinate layout shared by every present row, or ``None`` when rows
    /// differ.
    ///
    /// Returns
    /// -------
    /// str or None
    #[getter("common_coordinate_axes")]
    pub fn coordinate_axes(&self) -> Option<&'static str> {
        self.uniform_axes().map(geometry::CoordinateAxes::as_str)
    }
    /// Whether each geometry carries a Z ordinate. Missing rows are false.
    ///
    /// Returns
    /// -------
    /// numpy.ndarray
    #[getter("has_z")]
    pub fn has_z_rows(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        bool_array(
            py,
            self.row_operation()
                .rows_with_missing(|| false)
                .map(|row| match row {
                    ArrayRow::Missing { value, .. } => value,
                    ArrayRow::Present(row) => row.row().axes().has_z(),
                })
                .collect(),
        )
    }
    /// Whether each geometry carries an M ordinate. Missing rows are false.
    ///
    /// Returns
    /// -------
    /// numpy.ndarray
    #[getter("has_m")]
    pub fn has_m_rows(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        bool_array(
            py,
            self.row_operation()
                .rows_with_missing(|| false)
                .map(|row| match row {
                    ArrayRow::Missing { value, .. } => value,
                    ArrayRow::Present(row) => row.row().axes().has_m(),
                })
                .collect(),
        )
    }
    /// Per-geometry bounds ``(minx, miny, maxx, maxy)`` (see
    /// `Geometry.bounds`), as a read-only ``(rows, 4)`` float64 ndarray.
    /// Empty rows are all-``nan`` (intentional: a fixed-width ndarray cannot
    /// hold ``None`` like a scalar geometry); missing rows are also all-``nan``
    /// and are identified by `.is_missing`.
    /// Geographic longitude intervals may cross the seam, with ``minx > maxx``.
    ///
    /// Returns
    /// -------
    /// numpy.ndarray
    ///     One ``minx, miny, maxx, maxy`` row per input geometry.
    #[getter]
    pub fn bounds(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        crate::bounds_array(py, self)
    }
    /// Combined bounds ``(minx, miny, maxx, maxy)`` over all geometries, or
    /// ``None`` if every geometry is empty. Geographic longitude bounds use
    /// the CRS's periodic interval, so ``minx > maxx`` denotes a seam crossing.
    ///
    /// Returns
    /// -------
    /// tuple or None
    #[getter]
    pub fn total_bounds(&self) -> PyResult<Option<(f64, f64, f64, f64)>> {
        if let Some(bounds) = self.total_bounds_cache.get() {
            return Ok(*bounds);
        }
        // Bounds computation can reattach to Python; keep it outside the OnceLock initializer.
        let bounds = self.compute_total_bounds()?;
        self.total_bounds_cache.get_or_init(|| bounds);
        Ok(bounds)
    }
    /// Per-geometry OGC type name (see `Geometry.geometry_type`), with
    /// ``None`` at missing rows.
    ///
    /// Returns
    /// -------
    /// list of str or None
    ///     One name per input geometry, e.g. ``'Point'`` or ``'MultiPolygon'``.
    #[getter]
    pub fn geometry_type(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        // Packed storage is homogeneous, so the type is a constant per row — no
        // per-row Shape materialization.
        let rows = match self.storage() {
            GeometryArrayStorage::Packed(storage) => {
                vec![storage.kind().geometry_type(); self.len()]
            },
            GeometryArrayStorage::Mixed(_) => self
                .storage()
                .iter_shapes()
                .map(|shape| {
                    shape
                        .map(|shape| shape.geometry_type())
                        .map_err(PyErr::from)
                })
                .collect::<pyo3::PyResult<Vec<_>>>()?,
        };
        self.masked_row_list(py, rows)
    }

    /// Sorted unique GeoParquet ``geometry_types`` inventory for present rows.
    ///
    /// Labels match GeoParquet 1.x (``'Point'``, ``'Point Z'``, …). Missing
    /// rows are skipped; an all-missing or empty array yields ``[]``.
    ///
    /// Returns
    /// -------
    /// list of str
    pub fn _geoparquet_geometry_types(&self) -> Vec<String> {
        use std::collections::BTreeSet;

        match self.storage() {
            GeometryArrayStorage::Packed(storage) => {
                if self
                    .missing()
                    .map_or(self.len(), storage::ValidityMask::present_count)
                    == 0
                {
                    return Vec::new();
                }
                vec![format!(
                    "{}{}",
                    storage.kind().geometry_type(),
                    storage.axes().wkt_tag()
                )]
            },
            GeometryArrayStorage::Mixed(shapes) => {
                let mut kinds = BTreeSet::new();
                for (row, shape) in shapes.iter().enumerate() {
                    if !self.is_row_missing(row) {
                        kinds.insert((shape.geometry_type(), shape.axes().wkt_tag()));
                    }
                }
                kinds
                    .into_iter()
                    .map(|(kind, tag)| format!("{kind}{tag}"))
                    .collect()
            },
        }
    }
    /// Top-level parts grouped by source row.
    ///
    /// Simple geometries form one-element groups, multipart and collection
    /// geometries expose their immediate members, and empty or missing rows
    /// form empty groups.
    ///
    /// Returns
    /// -------
    /// Groups
    ///     One `GeometryArray` group per input row.
    #[getter]
    pub fn parts(&self, py: Python<'_>) -> PyResult<Groups> {
        self.flat_map_shapes_groups(py, |shape| {
            let mut parts = Vec::with_capacity(shape.part_count());
            parts.extend(shape.parts());
            Ok(parts)
        })
    }
    /// Per-geometry top-level part count (see `Geometry.num_geometries`):
    /// ``1`` for a single point/line/polygon, including a typed empty; the
    /// member count for a multi/collection, and ``0`` for an empty multipart
    /// or collection. Explicit empty members still count in their multipart's
    /// member count. Missing rows use Shapely's ``None``-geometry sentinel
    /// ``0``; use `.is_missing` to distinguish them from present empty
    /// geometries.
    ///
    /// Returns
    /// -------
    /// numpy.ndarray
    ///     One count per input geometry.
    #[getter]
    pub fn num_geometries(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        // Homogeneous packed storage is one OGC geometry per row — a constant
        // ``1`` from CSR row counts without per-row ``Shape`` materialization.
        // Mixed rows delegate to ``Shape::part_count`` on materialized shapes.
        let values = match self.storage() {
            GeometryArrayStorage::Packed(
                PackedStorage::Points { .. }
                | PackedStorage::Lines { .. }
                | PackedStorage::Polygons { .. },
            ) => (0..self.len())
                .map(|row| i64::from(!self.is_row_missing(row)))
                .collect(),
            GeometryArrayStorage::Mixed(_) => self
                .row_operation()
                .rows_with_missing(|| 0_i64)
                .map(|row| match row {
                    ArrayRow::Missing { value, .. } => Ok(value),
                    ArrayRow::Present(row) => row
                        .row()
                        .with_shape(|shape| shape.part_count() as i64)
                        .map_err(PyErr::from),
                })
                .collect::<pyo3::PyResult<Vec<_>>>()?,
        };
        int64_array(py, values)
    }
    /// Per-geometry coordinate count. Missing rows are ``0``.
    ///
    /// Returns
    /// -------
    /// numpy.ndarray
    ///     One coordinate count per input geometry. The array uses ``int64``
    ///     when every count fits; otherwise it uses ``object`` with exact
    ///     Python integers.
    #[getter]
    pub fn num_coordinates(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let row_count = self.len();
        let int64_capacity = checked_vector_len::<i64>(row_count, "coordinate-count array")?;
        let object_capacity = checked_vector_len::<Py<PyAny>>(row_count, "coordinate-count array")?;
        let mut int64_values = Vec::with_capacity(int64_capacity);
        let mut object_values: Option<Vec<Py<PyAny>>> = None;
        let mut query = crate::boundary::coordinates::CoordinateMeasureQuery::default();
        for row in self.row_operation().rows_with_missing(|| ()) {
            let measure = match row {
                ArrayRow::Missing { .. } => None,
                ArrayRow::Present(row) => Some(query.row(py, row.row())?),
            };
            let narrow = measure
                .as_ref()
                .map_or(Some(0), |measure| measure.count_i64());
            if let Some(value) = narrow {
                if let Some(values) = &mut object_values {
                    values.push(PyInt::new(py, value).into_any().unbind());
                } else {
                    int64_values.push(value);
                }
                continue;
            }
            let measure = measure.expect("only a present row can have a wide coordinate count");
            let values = object_values.get_or_insert_with(|| {
                let mut values = Vec::with_capacity(object_capacity);
                values.extend(
                    std::mem::take(&mut int64_values)
                        .into_iter()
                        .map(|value| PyInt::new(py, value).into_any().unbind()),
                );
                values
            });
            values.push(measure.count_pyint(py).into_any().unbind());
        }
        if let Some(values) = object_values {
            let array = numpy::PyArray1::from_owned_object_array(
                py,
                numpy::ndarray::Array1::from_vec(values),
            );
            array.try_readwrite()?.make_nonwriteable();
            Ok(array.into_any().unbind())
        } else {
            int64_array(py, int64_values)
        }
    }
    /// Return an array with the same topology and replacement coordinates.
    ///
    /// Pass one ``(N, dims)`` matrix (including a `Coordinates` view) or
    /// explicit ``x=`` and ``y=`` columns. Missing rows contribute no input
    /// coordinates and remain missing in the result.
    ///
    /// Parameters
    /// ----------
    /// coordinates : sequence of float, optional
    ///     Replacement ``(N, dims)`` coordinate matrix, including a
    ///     `Coordinates` view.
    /// x, y : sequence of float, optional
    ///     Replacement X and Y columns.
    /// z, m : sequence of float, optional
    ///     Replacement Z and M columns when the array already has those axes.
    ///     Omitted axes are carried unchanged; ``None`` is not a clearing
    ///     sentinel.
    ///
    /// Returns
    /// -------
    /// GeometryArray
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> arr = gm.GeometryArray([gm.LineString([(0, 0), (1, 1)])])
    /// >>> arr.set_coordinates([(5, 5), (6, 6)]).to_wkt()
    /// ['LINESTRING (5 5, 6 6)']
    #[pyo3(signature = (*args, **kwargs), text_signature = "($self, coordinates=None, /, *, x=..., y=..., z=..., m=...)")]
    pub fn set_coordinates(
        &self,
        py: Python<'_>,
        args: &Bound<'_, PyTuple>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Self> {
        // Union axes of the coordinate view (NaN-padded for members that lack
        // Z/M). Non-uniform arrays (mixed-axes rows, heterogeneous GC) parse
        // against that layout; replace_array_coordinates applies per-row via
        // replace_shape_coordinates, which keeps each member's native axes.
        // Uniform packed arrays still take the dense columnar fast path.
        let view = self.coordinate_view();
        let measure = view.exact_measure(py)?;
        let axes = measure.present_axes();
        let len = measure.len_for_python()?;
        let replacement = parse_coordinate_replacement(py, args, kwargs, axes, len)?;
        replace_array_coordinates(self, py, replacement)
    }
    /// Apply a vectorized callback to this array's coordinate matrix.
    ///
    /// The callback receives a read-only ``(N, dims)`` float64 matrix for
    /// present rows and must return a matrix with the same shape.
    /// Non-uniform arrays (mixed-axes rows or a heterogeneous
    /// GeometryCollection) use the view's union layout with NaN padding;
    /// each member keeps its native axes on apply.
    ///
    /// Parameters
    /// ----------
    /// func : callable
    ///     Function called with the read-only coordinate matrix.
    ///
    /// Returns
    /// -------
    /// GeometryArray
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> arr = gm.GeometryArray([gm.LineString([(0, 0), (1, 1)])])
    /// >>> arr.map_coordinates(lambda m: m + 1).to_wkt()
    /// ['LINESTRING (1 1, 2 2)']
    pub fn map_coordinates(&self, py: Python<'_>, func: &Bound<'_, PyAny>) -> PyResult<Self> {
        // Same union-axes + per-row replace path as set_coordinates; no early
        // reject when uniform_axes() is None.
        let coords = PyCoordinates::new(self.coordinate_view());
        let replacement = map_coordinates_callback(py, coords, func)?;
        replace_array_coordinates(self, py, replacement)
    }
    /// Per-geometry topological dimension — ``0`` point, ``1`` curve, ``2``
    /// areal (see `Geometry.topological_dimension`). Missing rows use
    /// Shapely's ``None``-geometry sentinel ``-1``; use `.is_missing` to
    /// distinguish them from present point-like geometries.
    ///
    /// Returns
    /// -------
    /// numpy.ndarray
    ///     One dimension per input geometry.
    #[getter]
    pub fn topological_dimension(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        // Homogeneous packed storage is one constant dimension per row (point
        // ``0`` / line ``1`` / polygon ``2``) — no per-row `Shape` view; mixed
        // rows read each shape's own dimension.
        let values = match self.storage() {
            GeometryArrayStorage::Packed(storage) => {
                let dimension = i64::from(storage.kind().topological_dimension().code());
                (0..self.len())
                    .map(|row| {
                        if self.is_row_missing(row) {
                            -1
                        } else {
                            dimension
                        }
                    })
                    .collect()
            },
            GeometryArrayStorage::Mixed(_) => self
                .row_operation()
                .rows_with_missing(|| -1_i64)
                .map(|row| match row {
                    ArrayRow::Missing { value, .. } => Ok(value),
                    ArrayRow::Present(row) => row
                        .row()
                        .with_shape(|shape| i64::from(shape.topological_dimension().code()))
                        .map_err(PyErr::from),
                })
                .collect::<pyo3::PyResult<Vec<_>>>()?,
        };
        int64_array(py, values)
    }
    /// Whether each geometry is empty (no points, rings, or parts).
    ///
    /// Returns
    /// -------
    /// numpy.ndarray
    ///     One result per input geometry.
    #[getter]
    pub fn is_empty(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        unary::is_empty_array(py, self)
    }
    /// Per-geometry closed test (see `Geometry.is_closed`).
    ///
    /// Returns
    /// -------
    /// numpy.ndarray
    ///     One result per input geometry.
    #[getter]
    pub fn is_closed(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        unary::is_closed_array(py, self)
    }
    /// Per-geometry ring test (see `Geometry.is_ring`).
    ///
    /// Returns
    /// -------
    /// numpy.ndarray
    ///     One result per input geometry.
    #[getter]
    pub fn is_ring(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        unary::is_ring_array(py, self)
    }
    /// Per-geometry counter-clockwise test (see `Geometry.is_ccw`).
    ///
    /// Returns
    /// -------
    /// numpy.ndarray
    ///     One result per input geometry.
    #[getter]
    pub fn is_ccw(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        unary::is_ccw_array(py, self)
    }
    /// Per-geometry simplicity test (see `Geometry.is_simple`).
    ///
    /// Returns
    /// -------
    /// numpy.ndarray
    ///     One result per input geometry.
    #[getter]
    pub fn is_simple(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        // Dense packed path is a separate, small function so the common case
        // does not share an instruction-cache footprint with the full unary
        // fallback (UnaryRowMode / frame-cache / linref monomorphization).
        // Nullable still rides the mask-aware packed lane in the same helper.
        unary::is_simple_array(py, self)
    }
    /// Per-geometry polygon convexity test (see `Geometry.is_convex`).
    ///
    /// Returns
    /// -------
    /// numpy.ndarray
    #[getter]
    pub fn is_convex(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        unary::is_convex_array(py, self)
    }
    /// Element-wise antimeridian-crossing test.
    ///
    /// Returns
    /// -------
    /// numpy.ndarray
    ///     One result per input geometry.
    ///
    /// Raises
    /// ------
    /// CRSError
    ///     If the CRS is projected (a geographic CRS or CRS-free lon/lat is
    ///     required).
    #[getter]
    pub fn crosses_antimeridian(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        unary::crosses_antimeridian_array(py, self)
    }
}

impl PyGeometryArray {
    /// Uncached array-wide bounds fold (geographic antimeridian when needed).
    pub(crate) fn compute_total_bounds(
        &self,
    ) -> crate::error::Result<Option<(f64, f64, f64, f64)>> {
        let domain = crate::geometry::topology_domain(&self.frame).ok();
        let element_bounds = self.existing_element_bounds();
        if let Some(crate::geometry::TopologyDomain::Geographic(frame)) = domain {
            if element_bounds.is_none()
                && let Ok(Some(bounds)) = Python::attach(|py| {
                    self.reduce_packed_columns_detached(py, |columns| {
                        Ok(geographic_total_bounds_from_columns(&columns, frame)?)
                    })
                })
            {
                return Ok(bounds.map(Bounds::into_tuple));
            }
            let row_operation = self.row_operation();
            let mut raw: Option<Bounds> = None;
            let mut ordinary = true;
            for row in row_operation.present_rows() {
                let bounds = element_bounds.as_ref().map_or_else(
                    || row.row().quick_bounds(),
                    |bounds| bounds.get(row.logical()).copied().flatten(),
                );
                let Some(bounds) = bounds else {
                    continue;
                };
                match &mut raw {
                    Some(total) => total.include_bounds(bounds),
                    None => raw = Some(bounds),
                }
                if !raw.is_some_and(|bounds| frame.bounds_certify_ordinary_support(bounds)) {
                    ordinary = false;
                    break;
                }
            }
            if ordinary {
                return Ok(raw.map(Bounds::into_tuple));
            }
            let shapes = row_operation
                .present_rows()
                .map(|row| {
                    use crate::geometry::storage::ShapeRow;
                    Ok(match row.row() {
                        ShapeRow::Handle(plan) => std::borrow::Cow::Borrowed(plan.shape()),
                        ShapeRow::Shape(shape) => std::borrow::Cow::Borrowed(shape),
                        ShapeRow::Point(point) => std::borrow::Cow::Owned(Shape::Point(point)),
                        ShapeRow::Packed(row) => std::borrow::Cow::Owned(row.to_shape()?),
                    })
                })
                .collect::<crate::error::Result<Vec<_>>>()?;
            return Ok(
                crate::geographic::GeographicSupportProducts::public_bounds_for_shapes(
                    frame, &shapes, None,
                )
                .map(Bounds::into_tuple),
            );
        }
        if let Some(bounds) = element_bounds {
            return Ok(bounds
                .iter()
                .flatten()
                .copied()
                .reduce(|mut total, bounds| {
                    total.include_bounds(bounds);
                    total
                })
                .map(Bounds::into_tuple));
        }
        if let Ok(Some(Some(bounds))) = Python::attach(|py| {
            self.reduce_packed_columns_detached(py, |columns| {
                Ok(raw_total_bounds_from_columns(&columns))
            })
        }) {
            return Ok(Some(bounds.into_tuple()));
        }
        // Mixed storage can retain shapes beneath missing rows. The packed
        // column reducer above already skips those rows when it applies.
        let bounds = if self.has_missing() {
            self.row_operation()
                .present_rows()
                .filter_map(|row| row.row().quick_bounds())
                .reduce(|mut total, bounds| {
                    total.include_bounds(bounds);
                    total
                })
        } else {
            self.storage().raw_total_bounds()
        };
        Ok(bounds.map(Bounds::into_tuple))
    }

    pub(crate) fn is_empty_unary_packed(&self) -> PyResult<Vec<bool>> {
        if self.has_missing() {
            return self
                .row_operation()
                .rows_with_missing(|| false)
                .map(|row| match row {
                    ArrayRow::Missing { value, .. } => Ok(value),
                    ArrayRow::Present(row) => {
                        row.row().with_shape(Shape::is_empty).map_err(PyErr::from)
                    },
                })
                .collect();
        }
        let mut output = Vec::with_capacity(self.len());
        match self.storage() {
            GeometryArrayStorage::Packed(PackedStorage::Points { .. }) => {
                output.resize(self.len(), false);
            },
            GeometryArrayStorage::Packed(
                PackedStorage::Lines { .. } | PackedStorage::Polygons { .. },
            ) => {
                for logical in 0..self.len() {
                    output.push(self.storage().row(logical).is_empty());
                }
            },
            GeometryArrayStorage::Mixed(_) => {
                for shape in self.storage().iter_shapes() {
                    output.push(shape.map_err(PyErr::from)?.is_empty());
                }
            },
        }
        Ok(output)
    }

    pub(crate) fn is_closed_unary_packed(&self, domain: TopologyDomain) -> PyResult<Vec<bool>> {
        if let Some(closed) = self
            .storage()
            .lines_bool(|coords| Ok(line_is_closed_in_domain(coords, domain)))
        {
            return Ok(closed?);
        }
        Ok(self
            .storage()
            .const_or_shape_bool(false, |shape| Ok(shape_is_closed_in_domain(shape, domain)))?)
    }

    pub(crate) fn is_ring_unary_packed(&self, domain: TopologyDomain) -> PyResult<Vec<bool>> {
        if let Some(ring) = self.storage().lines_bool(|coords| {
            Ok(line_is_closed_in_domain(coords, domain) && line_is_simple(coords)?)
        }) {
            return Ok(ring?);
        }
        Ok(self.storage().const_or_shape_bool(false, |shape| {
            let data = crate::geometry::PreparedPlan::transient(shape);
            crate::geometry::is_ring_data_in_frame(&data, domain)
        })?)
    }

    pub(crate) fn is_ccw_unary_packed(&self) -> PyResult<Vec<bool>> {
        if let Some(ccw) = self.storage().lines_bool(|coords| Ok(line_is_ccw(coords))) {
            return Ok(ccw?);
        }
        Ok(self
            .storage()
            .const_or_shape_bool(false, |shape| Ok(shape.is_ccw()))?)
    }

    pub(crate) fn is_simple_unary_packed(&self) -> Option<PyResult<Vec<bool>>> {
        let missing = self.missing();
        // Dense kernels avoid a per-row missing-mask branch.
        if missing.is_none() {
            if let Some(simple) = self.storage().lines_bool(line_is_simple) {
                return Some(simple.map_err(PyErr::from));
            }
            if let Some(simple) = self.storage().polygons_bool(polygon_row_is_valid) {
                return Some(simple.map_err(PyErr::from));
            }
            if let GeometryArrayStorage::Packed(PackedStorage::Points { .. }) = self.storage() {
                return Some(Ok(vec![true; self.len()]));
            }
            return None;
        }
        // Nullable kernels skip placeholders and write false for missing rows.
        if let Some(simple) = self.storage().lines_bool_masked(missing, line_is_simple) {
            return Some(simple.map_err(PyErr::from));
        }
        if let Some(simple) = self
            .storage()
            .polygons_bool_masked(missing, polygon_row_is_valid)
        {
            return Some(simple.map_err(PyErr::from));
        }
        if let GeometryArrayStorage::Packed(PackedStorage::Points { .. }) = self.storage() {
            let mask = missing.expect("missing branch");
            return Some(Ok(mask.iter().map(|missing| !missing).collect()));
        }
        None
    }

    pub(crate) fn is_valid_unary_packed(&self) -> Option<PyResult<Vec<bool>>> {
        let missing = self.missing();
        if let GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map }) =
            self.storage()
        {
            let map = row_map.as_deref();
            let len = self.len();
            // Skip masked rows (NaN placeholders); write false for missing so
            // the answer matches the shape lane without a second mask pass
            // being load-bearing for correctness.
            if missing.is_some() {
                let mut values = Vec::with_capacity(len);
                for row in self.row_operation().rows_with_missing(|| false) {
                    let value = match row {
                        crate::array::ArrayRow::Missing { value, .. } => value,
                        crate::array::ArrayRow::Present(row) => match row.row() {
                            storage::ShapeRow::Point(point) => {
                                point.x.is_finite() && point.y.is_finite()
                            },
                            storage::ShapeRow::Handle(_)
                            | storage::ShapeRow::Shape(_)
                            | storage::ShapeRow::Packed(_) => false,
                        },
                    };
                    values.push(value);
                }
                return Some(Ok(values));
            }
            let mut values = Vec::with_capacity(len);
            for logical in 0..len {
                let point = coords.point_at(physical_row(map, logical));
                values.push(point.x.is_finite() && point.y.is_finite());
            }
            return Some(Ok(values));
        }
        if let Some(valid) = self
            .storage()
            .lines_bool_masked(missing, |coords| Ok(line_is_valid(coords)))
        {
            return Some(valid.map_err(PyErr::from));
        }
        if let Some(valid) = self
            .storage()
            .polygons_bool_masked(missing, polygon_row_is_valid)
        {
            return Some(valid.map_err(PyErr::from));
        }
        None
    }

    pub(crate) fn is_convex_unary_packed(&self) -> PyResult<Vec<bool>> {
        let mut output = Vec::with_capacity(self.len());
        match self.storage() {
            GeometryArrayStorage::Packed(
                PackedStorage::Points { .. } | PackedStorage::Lines { .. },
            ) => {
                output.resize(self.len(), false);
            },
            GeometryArrayStorage::Packed(PackedStorage::Polygons { .. })
            | GeometryArrayStorage::Mixed(_) => {
                for (row, shape) in self.storage().iter_shapes().enumerate() {
                    let shape = shape.map_err(PyErr::from)?;
                    let value = if self.is_row_missing(row) {
                        false
                    } else {
                        match shape.as_ref() {
                            Shape::Polygon(_) | Shape::Empty(GeometryKind::Polygon, _) => {
                                shape.is_convex().unwrap_or(false)
                            },
                            Shape::Point(_)
                            | Shape::MultiPoint(_)
                            | Shape::LineString(_)
                            | Shape::MultiLineString(_)
                            | Shape::MultiPolygon(_)
                            | Shape::GeometryCollection(_)
                            | Shape::Empty(..) => false,
                        }
                    };
                    output.push(value);
                }
            },
        }
        Ok(output)
    }

    pub(crate) fn crosses_antimeridian_unary_packed(
        &self,
        frame: crate::geographic::GeographicFrame,
    ) -> Option<PyResult<Vec<bool>>> {
        if let Some(crosses) = self
            .storage()
            .lines_bool(|coords| Ok(crate::geometry::chain_crosses_antimeridian(coords, frame)))
        {
            return Some(crosses.map_err(PyErr::from));
        }
        if let GeometryArrayStorage::Packed(PackedStorage::Points { .. }) = self.storage() {
            return Some(Ok(vec![false; self.len()]));
        }
        None
    }
}

#[cfg(test)]
mod geographic_total_bounds_tests {
    use super::*;
    use crate::crs::frame::{Frame, crs_arc_static};
    use crate::geometry::{Point, Polygon, Ring};

    fn polygon(points: &[(f64, f64)]) -> Shape {
        let coordinates = CoordSeq::try_from_points(
            &points
                .iter()
                .map(|&(x, y)| Point::new_raw_xy(x, y))
                .collect::<Vec<_>>(),
        )
        .expect("polygon coordinates");
        Shape::Polygon(Polygon::new(
            Ring::try_from_closed(coordinates).expect("closed ring"),
            Vec::new(),
        ))
    }

    #[test]
    fn mixed_total_bounds_folds_certified_rows_and_retains_alias_crossing() {
        crate::test_support::initialize_python();
        let frame = Frame::new(Some(crs_arc_static("OGC:CRS84")), None).expect("CRS84");
        let ordinary = polygon(&[
            (-10.0, -5.0),
            (10.0, -5.0),
            (10.0, 5.0),
            (-10.0, 5.0),
            (-10.0, -5.0),
        ]);
        let point = Shape::Point(Point::new_raw_xy(30.0, 2.0));
        let ordinary_array =
            PyGeometryArray::from_shapes(vec![ordinary, point.clone()], frame.clone())
                .expect("mixed geometry array");
        assert!(matches!(
            ordinary_array.storage(),
            GeometryArrayStorage::Mixed(_)
        ));
        assert_eq!(
            ordinary_array.compute_total_bounds().expect("total bounds"),
            Some((-10.0, -5.0, 30.0, 5.0))
        );

        let alias = polygon(&[
            (179.0, -10.0),
            (181.0, -10.0),
            (181.0, 10.0),
            (179.0, 10.0),
            (179.0, -10.0),
        ]);
        let alias_array = PyGeometryArray::from_shapes(vec![alias.clone(), point.clone()], frame)
            .expect("mixed alias array");
        assert_eq!(
            alias_array.compute_total_bounds().expect("total bounds"),
            crate::geographic::GeographicSupportEnvelope::try_from_shapes(
                crate::geographic::GeographicFrame::crs84(),
                &[&alias, &point],
            )
            .ok()
            .and_then(|support| support.bounds(crate::geographic::GeographicFrame::crs84()))
            .map(Bounds::into_tuple)
        );
    }
}

#[cfg(test)]
mod mixed_replacement_row_note_tests {
    use pyo3::prelude::*;

    use super::{PyGeometryArray, replace_array_coordinates};
    use crate::crs::frame::Frame;
    use crate::geometry::storage::ValidityMask;
    use crate::geometry::storage::replacement::{CoordinateReplacement, ReplacementAxis};
    use crate::geometry::{CoordinateAxes, Shape, SharedSlice};

    #[test]
    fn present_empty_mixed_replacement_error_keeps_logical_row_note() {
        crate::test_support::initialize_python();
        Python::attach(|py| {
            let empty = crate::io::parse_wkt("POLYGON M EMPTY").expect("typed empty polygon");
            let polygon = crate::io::parse_wkt("POLYGON M ((0 0 11, 4 0 12, 4 4 13, 0 0 11))")
                .expect("measured polygon");
            let array = PyGeometryArray::mixed_shapes(
                vec![Shape::empty_point(), empty, Shape::empty_point(), polygon],
                Frame::None,
            )
            .expect("mixed geometry array")
            .with_missing_mask(Some(
                ValidityMask::from_vec(4, vec![true, false, true, false])
                    .expect("logical missing mask"),
            ))
            .expect("missing rows attach");
            let replacement = CoordinateReplacement {
                xs: SharedSlice::from_vec(vec![0.0, 4.0, 4.0, 0.5]),
                ys: SharedSlice::from_vec(vec![0.0, 0.0, 4.0, 0.0]),
                zs: ReplacementAxis::Carry,
                ms: ReplacementAxis::Carry,
                len: 4,
                axes: CoordinateAxes::XYM,
                positional: false,
            };

            let error = replace_array_coordinates(&array, py, replacement)
                .expect_err("open mixed polygon replacement fails");
            let notes = error
                .value(py)
                .getattr("__notes__")
                .expect("row error note")
                .extract::<Vec<String>>()
                .expect("row error notes are strings");
            assert_eq!(notes, ["while processing array element 3"]);
        });
    }
}
