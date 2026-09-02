//! Core `GeometryArray` methods: construction, metadata, properties, iteration.
#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
#![allow(
    clippy::needless_pass_by_value,
    reason = "PyO3 special-method receivers must retain their binding-compatible ownership shape"
)]

use pyo3::PyErr;
use pyo3::types::PyList;

use crate::array::{
    Bound, Bounds, CRSError, CoercedCollectedGeometryItems, CoordSeq, CoordinateReplacement,
    CsrOffsetColumn, Frame, FrameAdoption, GeometryArrayStorage, GeometryKind,
    InvalidGeometryError, Py, PyAny, PyAnyMethods as _, PyCoordinates, PyCrs, PyDict,
    PyDictMethods as _, PyGeometry, PyGeometryArray, PyListMethods as _, PyRef, PyResult, PyTuple,
    PyTypeError, PyTypeMethods as _, Python, Result, RingLevel, SVG_ARRAY_PREVIEW, Shape,
    ValidityMask, bool_array, coerce_collected_geometry_items, coordinate_epoch_option, crs,
    crs_arc, exact_geometry, geometry, geometry_array_svg_grid_html_masked,
    line_crosses_antimeridian, line_is_ccw, line_is_closed, line_is_simple, line_is_valid,
    line_logical_len, map_coordinates_callback, parse_coordinate_replacement, parse_crs,
    physical_row, polygon_is_valid, polygon_logical_len, polygon_rings_range, pymethods,
    replace_shape_coordinates, require_geojson_crs, slice_replacement_for_shape,
    total_bounds_from_columns,
};

fn try_collect_values<T>(values: impl IntoIterator<Item = T>, capacity: usize) -> PyResult<Vec<T>> {
    let mut output = crate::try_vec_with_capacity(capacity)?;
    for value in values {
        crate::try_push(&mut output, value)?;
    }
    Ok(output)
}

fn type_module(value: &Bound<'_, PyAny>) -> PyResult<String> {
    value.get_type().getattr("__module__")?.extract()
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

fn validate_packed_replacement_rings(
    coords: &CoordSeq,
    ring_offsets: &CsrOffsetColumn<RingLevel>,
) -> PyResult<()> {
    for [start, end] in ring_offsets.array_windows::<2>() {
        let start = *start as usize;
        let end = *end as usize;
        match crate::array::CheckedRingWindow::checked(coords, start..end) {
            Ok(_) => {},
            Err(crate::array::RingWindowError::TooShort(len)) => {
                let error: crate::error::Error =
                    crate::geometry::GeometryErrorKind::RingTooShort(len).into();
                return Err(error.into());
            },
            Err(crate::array::RingWindowError::Open) => {
                return Err(InvalidGeometryError::new_err("polygon ring must be closed"));
            },
            Err(
                crate::array::RingWindowError::Inverted | crate::array::RingWindowError::OutOfRange,
            ) => {
                return Err(InvalidGeometryError::new_err(
                    "polygon ring range is invalid",
                ));
            },
        }
    }
    Ok(())
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
        GeometryArrayStorage::Points { coords, row_map } if row_map.is_identity() => {
            debug_assert_eq!(coords.len(), replacement.len);
            let coords = replacement.apply_to_seq(coords)?;
            Ok(Ok(PyGeometryArray::packed_points(
                coords,
                array.frame.clone(),
            )))
        },
        GeometryArrayStorage::Lines {
            coords,
            offsets,
            row_map,
        } if row_map.is_identity() => {
            debug_assert_eq!(coords.len(), replacement.len);
            let coords = replacement.apply_to_seq(coords)?;
            Ok(Ok(PyGeometryArray::packed_lines(
                coords,
                offsets.clone(),
                array.frame.clone(),
            )))
        },
        GeometryArrayStorage::Polygons {
            coords,
            ring_offsets,
            polygon_offsets,
            row_map,
        } if row_map.is_identity() => {
            debug_assert_eq!(coords.len(), replacement.len);
            let coords = replacement.apply_to_seq(coords)?;
            validate_packed_replacement_rings(&coords, ring_offsets)?;
            Ok(Ok(PyGeometryArray::packed_polygons(
                coords,
                ring_offsets.clone(),
                polygon_offsets.clone(),
                array.frame.clone(),
            )))
        },
        GeometryArrayStorage::Points { .. }
        | GeometryArrayStorage::Lines { .. }
        | GeometryArrayStorage::Polygons { .. }
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
    if !array.is_compact_nullable() && !matches!(array.storage(), GeometryArrayStorage::Mixed(_)) {
        return Ok(array
            .replace_packed_coords_detached(py, array.frame.clone(), replacement)?
            .expect("non-Mixed storage has packed columns"));
    }
    let mut present_cursor = 0;
    let mut shapes = crate::try_vec_with_capacity(array.len())?;
    for row in array.logical_shape_rows() {
        let crate::array::LogicalShapeRow::Present { shape, .. } = row else {
            continue;
        };
        let count = shape.coord_count();
        let sub = slice_replacement_for_shape(&replacement, present_cursor, count);
        present_cursor += count;
        crate::try_push(&mut shapes, replace_shape_coordinates(&shape, &sub)?)?;
    }
    debug_assert_eq!(present_cursor, replacement.len);
    Ok(array.scatter_present_result(PyGeometryArray::from_shapes(shapes, array.frame.clone())))
}

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
        let shapes = try_collect_values(
            self.present_shape_rows()
                .map(|(_, shape)| shape.into_owned())
                .filter(|shape| !shape.is_empty()),
            self.len(),
        )?;
        if shapes.is_empty() {
            return Err(CRSError::new_err(
                "estimate_local_crs requires at least one present, non-empty geometry",
            ));
        }
        let collection = Shape::GeometryCollection(shapes.into());
        Ok(PyCrs::from_canonical(crs_arc(crs::estimate_local_crs(
            &collection,
            source,
        )?)))
    }

    // NEP 13: opt out of numpy ufunc dispatch (we have our own & | - ^ /
    // predicates)
    #[classattr]
    #[expect(non_upper_case_globals, reason = "Python dunder name")]
    const __array_ufunc__: Option<Py<PyAny>> = None;

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
        // Mandatory keystone: hint-only `__len__` + fallible per-item growth.
        let raw_items = crate::collect_py_iter(values, Ok)?;
        if let Some(wkb) = shapely_wkb_batch(values, &raw_items)? {
            let parsed =
                crate::py::functions::geometry_io::from_wkb(values.py(), &wkb, crs, epoch)?;
            return Ok(parsed
                .bind(values.py())
                .extract::<PyRef<'_, Self>>()?
                .clone());
        }
        // First-class missing rows: `None` items mark the mask; only the
        // present items participate in coercion and frame resolution, then
        // placeholder rows (already frame-tagged) fill the masked slots.
        let mut has_missing = false;
        let mut mask = crate::try_vec_with_capacity(raw_items.len())?;
        let mut present = crate::try_vec_with_capacity(raw_items.len())?;
        for item in &raw_items {
            let missing = item.is_none();
            has_missing |= missing;
            crate::try_push(&mut mask, missing)?;
            if !missing {
                crate::try_push(&mut present, item.clone())?;
            }
        }
        let explicit_crs = parse_crs(crs)?;
        let explicit_epoch = coordinate_epoch_option("epoch", epoch)?;
        let (present_items, frame) = match coerce_collected_geometry_items(
            &present,
            false,
            crate::io::LegacyGeoJsonCrsPolicy::Adopt(explicit_crs.as_deref()),
        )? {
            CoercedCollectedGeometryItems::Items(mut items) => {
                let frame = Frame::resolve_items(
                    &mut items,
                    FrameAdoption {
                        crs: explicit_crs,
                        epoch: explicit_epoch,
                    },
                    "GeometryArray",
                )?;
                (items, frame)
            },
            CoercedCollectedGeometryItems::FramelessShapes(shapes) => {
                let frame = Frame::new(explicit_crs, explicit_epoch)?;
                let mut items = crate::try_vec_with_capacity(shapes.len())?;
                for shape in shapes {
                    crate::try_push(&mut items, PyGeometry::with_frame(shape, frame.clone()))?;
                }
                (items, frame)
            },
        };
        let array = Self::pack_or_mixed(present_items, frame)?;
        Ok(if has_missing {
            Self::scatter_present_rows(
                &array,
                ValidityMask::from_vec(mask.len(), mask)
                    .expect("has_missing guarantees a present missing mask"),
            )
        } else {
            array
        })
    }

    pub fn _repr_html_(&self, py: Python<'_>) -> PyResult<String> {
        if let Some(html) = crate::py::viz::try_array_repr_html(py, self)? {
            return Ok(html);
        }
        Ok(geometry_array_svg_grid_html_masked(
            self.logical_shape_rows(),
            self.len(),
            SVG_ARRAY_PREVIEW,
        )?)
    }

    pub fn _repr_html_svg(&self) -> PyResult<String> {
        Ok(geometry_array_svg_grid_html_masked(
            self.logical_shape_rows(),
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
        self.crs_ref().cloned().map(PyCrs::from_canonical)
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
    /// ``FeatureCollection`` mapping (one ``Feature`` per row, positional
    /// ``id``, empty ``properties``) — the shape geopandas and mapping
    /// libraries expect from a geometry column.
    #[getter("__geo_interface__")]
    pub fn geo_interface<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, pyo3::types::PyDict>> {
        use pyo3::types::{PyDict, PyList};
        require_geojson_crs(self.crs_str())?;
        if self.has_m() || self.epoch().is_some() {
            return Err(crate::PyTypeError::new_err(
                "__geo_interface__ cannot represent M or coordinate epoch; use an explicit serializer or clear the dimension",
            ));
        }
        let features = PyList::empty(py);
        for row in 0..self.len() {
            let feature = PyDict::new(py);
            feature.set_item("type", "Feature")?;
            feature.set_item("id", row)?;
            feature.set_item("properties", PyDict::new(py))?;
            let geometry = if self.is_row_missing(row) {
                py.None().into_bound(py)
            } else {
                let geometry = self.geometry_at(row);
                crate::boundary::convert::geojson_dict_prepared(
                    py,
                    geometry.shape.shape(),
                    self.crs_str().is_some(),
                )?
                .into_any()
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
        let rows = try_collect_values(
            self.logical_shape_rows().map(|row| match row {
                crate::array::LogicalShapeRow::Missing { .. } => "",
                crate::array::LogicalShapeRow::Present { shape, .. } => shape.axes().as_str(),
            }),
            self.len(),
        )?;
        self.masked_row_list(py, rows)
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
            try_collect_values(
                self.logical_shape_rows().map(|row| match row {
                    crate::array::LogicalShapeRow::Missing { .. } => false,
                    crate::array::LogicalShapeRow::Present { shape, .. } => shape.has_z(),
                }),
                self.len(),
            )?,
        )
    }
    /// Whether any present geometry carries a Z ordinate.
    ///
    /// Returns
    /// -------
    /// bool
    #[getter("any_has_z")]
    pub fn has_z(&self) -> bool {
        if self
            .missing()
            .as_ref()
            .is_some_and(|mask| mask.present_count() == 0)
        {
            return false;
        }
        // Packed storage shares ONE coords column with uniform axes, so the
        // whole-array answer is a single O(1) axes read — not a per-row Shape
        // materialization + probe.
        match self.storage() {
            GeometryArrayStorage::Points { coords, .. }
            | GeometryArrayStorage::Lines { coords, .. }
            | GeometryArrayStorage::Polygons { coords, .. } => coords.axes().has_z(),
            GeometryArrayStorage::Mixed(_) => {
                self.present_shape_rows().any(|(_, shape)| shape.has_z())
            },
        }
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
            try_collect_values(
                self.logical_shape_rows().map(|row| match row {
                    crate::array::LogicalShapeRow::Missing { .. } => false,
                    crate::array::LogicalShapeRow::Present { shape, .. } => shape.has_m(),
                }),
                self.len(),
            )?,
        )
    }
    /// Whether any present geometry carries an M ordinate.
    ///
    /// Returns
    /// -------
    /// bool
    #[getter("any_has_m")]
    pub fn has_m(&self) -> bool {
        if self
            .missing()
            .as_ref()
            .is_some_and(|mask| mask.present_count() == 0)
        {
            return false;
        }
        match self.storage() {
            GeometryArrayStorage::Points { coords, .. }
            | GeometryArrayStorage::Lines { coords, .. }
            | GeometryArrayStorage::Polygons { coords, .. } => coords.axes().has_m(),
            GeometryArrayStorage::Mixed(_) => {
                self.present_shape_rows().any(|(_, shape)| shape.has_m())
            },
        }
    }
    /// Per-geometry bounds ``(minx, miny, maxx, maxy)`` (see
    /// `Geometry.bounds`), as a read-only ``(rows, 4)`` float64 ndarray.
    /// Empty rows are all-``nan`` (intentional: a fixed-width ndarray cannot
    /// hold ``None`` like a scalar geometry); missing rows are also all-``nan``
    /// and are identified by `.is_missing`.
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
    /// ``None`` if every geometry is empty.
    ///
    /// Returns
    /// -------
    /// tuple or None
    #[getter]
    pub fn total_bounds(&self) -> Option<(f64, f64, f64, f64)> {
        *self
            .total_bounds_cache
            .get_or_init(|| self.compute_total_bounds())
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
            GeometryArrayStorage::Points { .. } => {
                try_collect_values(std::iter::repeat_n("Point", self.len()), self.len())?
            },
            GeometryArrayStorage::Lines { .. } => {
                try_collect_values(std::iter::repeat_n("LineString", self.len()), self.len())?
            },
            GeometryArrayStorage::Polygons { .. } => {
                try_collect_values(std::iter::repeat_n("Polygon", self.len()), self.len())?
            },
            GeometryArrayStorage::Mixed(_) => try_collect_values(
                self.storage()
                    .iter_shapes()
                    .map(|shape| shape.geometry_type()),
                self.len(),
            )?,
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
    pub fn _geoparquet_geometry_types(&self) -> PyResult<Vec<String>> {
        use std::collections::BTreeSet;

        let mut labels = BTreeSet::new();
        let present = |row: usize| !self.is_row_missing(row);
        match self.uniform_axes() {
            Some(axes) => {
                let tag = axes.wkt_tag();
                match self.storage() {
                    GeometryArrayStorage::Points { .. } => {
                        if (0..self.len()).any(present) {
                            labels.insert(format!("Point{tag}"));
                        }
                    },
                    GeometryArrayStorage::Lines { .. } => {
                        if (0..self.len()).any(present) {
                            labels.insert(format!("LineString{tag}"));
                        }
                    },
                    GeometryArrayStorage::Polygons { .. } => {
                        if (0..self.len()).any(present) {
                            labels.insert(format!("Polygon{tag}"));
                        }
                    },
                    GeometryArrayStorage::Mixed(_) => {
                        for (row, shape) in self.storage().iter_shapes().enumerate() {
                            if present(row) {
                                labels.insert(format!("{}{tag}", shape.geometry_type()));
                            }
                        }
                    },
                }
            },
            None => {
                for (row, shape) in self.storage().iter_shapes().enumerate() {
                    if present(row) {
                        labels.insert(format!(
                            "{}{}",
                            shape.geometry_type(),
                            shape.axes().wkt_tag()
                        ));
                    }
                }
            },
        }
        try_collect_values(labels, 16)
    }
    /// Top-level parts grouped by source row.
    ///
    /// Simple geometries form one-element groups, multipart and collection
    /// geometries expose their immediate members, and empty or missing rows
    /// form empty groups. The free function ``parts`` uses the same grouped
    /// result for `GeometryArray` input and keeps scalar/raw-iterable output flat.
    ///
    /// Returns
    /// -------
    /// Groups
    ///     One `GeometryArray` group per input row.
    #[getter]
    pub fn parts(&self, py: Python<'_>) -> PyResult<crate::py::vectors::Groups> {
        self.flat_map_shapes_groups(py, |shape| {
            let mut parts = Vec::new();
            crate::boundary::try_reserve_vec(&mut parts, shape.part_count())?;
            for part in shape.parts() {
                crate::boundary::try_push_vec(&mut parts, part)?;
            }
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
            GeometryArrayStorage::Points { .. }
            | GeometryArrayStorage::Lines { .. }
            | GeometryArrayStorage::Polygons { .. } => try_collect_values(
                (0..self.len()).map(|row| i64::from(!self.is_row_missing(row))),
                self.len(),
            )?,
            GeometryArrayStorage::Mixed(_) => try_collect_values(
                self.logical_shape_rows().map(|row| match row {
                    crate::array::LogicalShapeRow::Missing { .. } => 0,
                    crate::array::LogicalShapeRow::Present { shape, .. } => {
                        shape.part_count() as i64
                    },
                }),
                self.len(),
            )?,
        };
        crate::py::numpy::int64_array(py, values)
    }
    /// Per-geometry coordinate count. Missing rows are ``0``.
    ///
    /// Returns
    /// -------
    /// numpy.ndarray
    ///     One ``int64`` coordinate count per input geometry.
    #[getter]
    pub fn num_coordinates(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        crate::py::numpy::int64_array(
            py,
            try_collect_values(
                self.logical_shape_rows().map(|row| match row {
                    crate::array::LogicalShapeRow::Missing { .. } => 0,
                    crate::array::LogicalShapeRow::Present { shape, .. } => {
                        shape.coord_count() as i64
                    },
                }),
                self.len(),
            )?,
        )
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
        let replacement = parse_coordinate_replacement(py, args, kwargs, view.axes(), view.len())?;
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
            GeometryArrayStorage::Points { .. } => try_collect_values(
                (0..self.len()).map(|row| {
                    if self.is_row_missing(row) {
                        -1
                    } else {
                        i64::from(GeometryKind::Point.topological_dimension().code())
                    }
                }),
                self.len(),
            )?,
            GeometryArrayStorage::Lines { .. } => try_collect_values(
                (0..self.len()).map(|row| {
                    if self.is_row_missing(row) {
                        -1
                    } else {
                        i64::from(GeometryKind::LineString.topological_dimension().code())
                    }
                }),
                self.len(),
            )?,
            GeometryArrayStorage::Polygons { .. } => try_collect_values(
                (0..self.len()).map(|row| {
                    if self.is_row_missing(row) {
                        -1
                    } else {
                        i64::from(GeometryKind::Polygon.topological_dimension().code())
                    }
                }),
                self.len(),
            )?,
            GeometryArrayStorage::Mixed(_) => try_collect_values(
                self.logical_shape_rows().map(|row| match row {
                    crate::array::LogicalShapeRow::Missing { .. } => -1,
                    crate::array::LogicalShapeRow::Present { shape, .. } => {
                        i64::from(shape.topological_dimension().code())
                    },
                }),
                self.len(),
            )?,
        };
        crate::py::numpy::int64_array(py, values)
    }
    /// Whether each geometry is empty (no points, rings, or parts).
    ///
    /// Returns
    /// -------
    /// numpy.ndarray
    ///     One result per input geometry.
    #[getter]
    pub fn is_empty(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        crate::predicates::unary::is_empty_array(py, self)
    }
    /// Per-geometry closed test (see `Geometry.is_closed`).
    ///
    /// Returns
    /// -------
    /// numpy.ndarray
    ///     One result per input geometry.
    #[getter]
    pub fn is_closed(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        crate::predicates::unary::is_closed_array(py, self)
    }
    /// Per-geometry ring test (see `Geometry.is_ring`).
    ///
    /// Returns
    /// -------
    /// numpy.ndarray
    ///     One result per input geometry.
    #[getter]
    pub fn is_ring(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        crate::predicates::unary::is_ring_array(py, self)
    }
    /// Per-geometry counter-clockwise test (see `Geometry.is_ccw`).
    ///
    /// Returns
    /// -------
    /// numpy.ndarray
    ///     One result per input geometry.
    #[getter]
    pub fn is_ccw(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        crate::predicates::unary::is_ccw_array(py, self)
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
        crate::predicates::unary::is_simple_array(py, self)
    }
    /// Per-geometry polygon convexity test (see `Geometry.is_convex`).
    ///
    /// Returns
    /// -------
    /// numpy.ndarray
    #[getter]
    pub fn is_convex(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        crate::predicates::unary::is_convex_array(py, self)
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
        crate::predicates::unary::crosses_antimeridian_array(py, self)
    }
}

impl PyGeometryArray {
    /// Uncached array-wide bounds fold (geographic antimeridian when needed).
    pub(crate) fn compute_total_bounds(&self) -> Option<(f64, f64, f64, f64)> {
        if self.has_missing() {
            // Aggregates skip missing rows; the placeholder is a NaN point
            // that would poison the column min/max fold.
            return self.drop_missing().total_bounds();
        }
        let geographic = crate::geometry::is_geographic_frame(&self.frame);
        if geographic {
            let mut shapes = self.present_shape_rows();
            if shapes.any(|(_, shape)| shape.crosses_antimeridian()) {
                return crate::geometry::geographic_crossing_bounds_for_shapes(
                    self.present_shape_rows().map(|(_, shape)| shape),
                )
                .map(Bounds::into_tuple);
            }
        }
        if let Some(Some(bounds)) = self.bounds_cache.get() {
            return bounds
                .iter()
                .flatten()
                .copied()
                .reduce(|mut total, bounds| {
                    total.include_bounds(bounds);
                    total
                })
                .map(Bounds::into_tuple);
        }
        if let Ok(Some(Some(bounds))) = Python::attach(|py| {
            self.reduce_packed_columns_detached(py, |columns| {
                Ok(total_bounds_from_columns(&columns))
            })
        }) {
            return Some(bounds.into_tuple());
        }
        self.storage().total_bounds().map(Bounds::into_tuple)
    }

    pub(crate) fn is_empty_unary_packed(&self) -> PyResult<Vec<bool>> {
        if self.has_missing() {
            return self
                .logical_shape_rows()
                .map(|row| match row {
                    crate::array::LogicalShapeRow::Missing { .. } => Ok(false),
                    crate::array::LogicalShapeRow::Present { shape, .. } => Ok(shape.is_empty()),
                })
                .collect();
        }
        let mut output = crate::try_vec_with_capacity(self.len())?;
        match self.storage() {
            GeometryArrayStorage::Points { .. } => {
                for _ in 0..self.len() {
                    crate::try_push(&mut output, false)?;
                }
            },
            GeometryArrayStorage::Lines {
                offsets, row_map, ..
            } => {
                let map = row_map.as_deref();
                for logical in 0..line_logical_len(offsets.as_slice(), map) {
                    crate::try_push(
                        &mut output,
                        map.csr_window(offsets.as_slice(), logical).is_empty(),
                    )?;
                }
            },
            GeometryArrayStorage::Polygons {
                polygon_offsets,
                row_map,
                ..
            } => {
                let map = row_map.as_deref();
                for logical in 0..polygon_logical_len(polygon_offsets.as_slice(), map) {
                    crate::try_push(&mut output, {
                        let rings = polygon_rings_range(polygon_offsets.as_slice(), map, logical);
                        rings.is_empty()
                    })?;
                }
            },
            GeometryArrayStorage::Mixed(_) => {
                for shape in self.storage().iter_shapes() {
                    crate::try_push(&mut output, shape.is_empty())?;
                }
            },
        }
        Ok(output)
    }

    pub(crate) fn is_closed_unary_packed(&self) -> PyResult<Vec<bool>> {
        if let Some(closed) = self.storage().lines_bool(line_is_closed) {
            return Ok(closed?);
        }
        Ok(self
            .storage()
            .const_or_shape_bool(false, Shape::is_closed)?)
    }

    pub(crate) fn is_ring_unary_packed(&self) -> PyResult<Vec<bool>> {
        if let Some(ring) = self
            .storage()
            .lines_bool(|coords| line_is_closed(coords) && line_is_simple(coords))
        {
            return Ok(ring?);
        }
        Ok(self.storage().const_or_shape_bool(false, Shape::is_ring)?)
    }

    pub(crate) fn is_ccw_unary_packed(&self) -> PyResult<Vec<bool>> {
        if let Some(ccw) = self.storage().lines_bool(line_is_ccw) {
            return Ok(ccw?);
        }
        Ok(self.storage().const_or_shape_bool(false, Shape::is_ccw)?)
    }

    pub(crate) fn is_simple_unary_packed(&self) -> Option<PyResult<Vec<bool>>> {
        let missing = self.missing();
        // Dense (no missing mask): use the unmasked kernels so the hot loop
        // has no per-row Option branch — restores the round-4/pre-mask code
        // shape for the overwhelmingly common packed input.
        if missing.is_none() {
            if let Some(simple) = self.storage().lines_bool(line_is_simple) {
                return Some(simple.map_err(PyErr::from));
            }
            if let Some(simple) = self.storage().polygons_bool(polygon_is_valid) {
                return Some(simple.map_err(PyErr::from));
            }
            if let GeometryArrayStorage::Points { .. } = self.storage() {
                return Some((|| {
                    let mut values = crate::try_vec_with_capacity(self.len())?;
                    for _ in 0..self.len() {
                        crate::try_push(&mut values, true)?;
                    }
                    Ok(values)
                })());
            }
            return None;
        }
        // Nullable: mask-aware kernels skip placeholders and write false for
        // missing — keeps the cliff ~1.2× of dense, not the old 592× fallback.
        if let Some(simple) = self.storage().lines_bool_masked(missing, line_is_simple) {
            return Some(simple.map_err(PyErr::from));
        }
        if let Some(simple) = self
            .storage()
            .polygons_bool_masked(missing, polygon_is_valid)
        {
            return Some(simple.map_err(PyErr::from));
        }
        if let GeometryArrayStorage::Points { .. } = self.storage() {
            let mask = missing.expect("missing branch");
            return Some((|| {
                let mut values = crate::try_vec_with_capacity(mask.len())?;
                for is_missing in mask.iter() {
                    crate::try_push(&mut values, !is_missing)?;
                }
                Ok(values)
            })());
        }
        None
    }

    pub(crate) fn is_valid_unary_packed(&self) -> Option<PyResult<Vec<bool>>> {
        let missing = self.missing();
        if let GeometryArrayStorage::Points { coords, row_map } = self.storage() {
            let map = row_map.as_deref();
            let len = self.len();
            // Skip masked rows (NaN placeholders); write false for missing so
            // the answer matches the shape lane without a second mask pass
            // being load-bearing for correctness.
            if let Some(_mask) = missing {
                return Some((|| {
                    let mut values = crate::try_vec_with_capacity(len)?;
                    for row in self.logical_storage_rows() {
                        let value = match row {
                            crate::array::LogicalRow::Missing { .. } => false,
                            crate::array::LogicalRow::Present {
                                row: crate::array::ShapeRow::Point(point),
                                ..
                            } => point.x.is_finite() && point.y.is_finite(),
                            crate::array::LogicalRow::Present { .. } => false,
                        };
                        crate::try_push(&mut values, value)?;
                    }
                    Ok(values)
                })());
            }
            return Some((|| {
                let mut values = crate::try_vec_with_capacity(len)?;
                for logical in 0..len {
                    let point = coords.point_at(physical_row(map, logical));
                    crate::try_push(&mut values, point.x.is_finite() && point.y.is_finite())?;
                }
                Ok(values)
            })());
        }
        if let Some(valid) = self.storage().lines_bool_masked(missing, line_is_valid) {
            return Some(valid.map_err(PyErr::from));
        }
        if let Some(valid) = self
            .storage()
            .polygons_bool_masked(missing, polygon_is_valid)
        {
            return Some(valid.map_err(PyErr::from));
        }
        None
    }

    pub(crate) fn is_convex_unary_packed(&self) -> PyResult<Vec<bool>> {
        let mut output = crate::try_vec_with_capacity(self.len())?;
        match self.storage() {
            GeometryArrayStorage::Points { .. } | GeometryArrayStorage::Lines { .. } => {
                for _ in 0..self.len() {
                    crate::try_push(&mut output, false)?;
                }
            },
            GeometryArrayStorage::Polygons { .. } | GeometryArrayStorage::Mixed(_) => {
                for (row, shape) in self.storage().iter_shapes().enumerate() {
                    let value = if self.is_row_missing(row) {
                        false
                    } else {
                        match shape.as_ref() {
                            Shape::Polygon(_) | Shape::Empty(GeometryKind::Polygon, _) => {
                                shape.is_convex().unwrap_or(false)
                            },
                            _ => false,
                        }
                    };
                    crate::try_push(&mut output, value)?;
                }
            },
        }
        Ok(output)
    }

    pub(crate) fn crosses_antimeridian_unary_packed(&self) -> Option<PyResult<Vec<bool>>> {
        if let Some(crosses) = self.storage().lines_bool(line_crosses_antimeridian) {
            return Some(crosses.map_err(PyErr::from));
        }
        if let GeometryArrayStorage::Points { .. } = self.storage() {
            let mut values = crate::try_vec_with_capacity(self.len());
            if let Ok(ref mut values) = values {
                for _ in 0..self.len() {
                    if let Err(error) = crate::try_push(values, false) {
                        return Some(Err(error));
                    }
                }
            }
            return Some(values);
        }
        None
    }
}
