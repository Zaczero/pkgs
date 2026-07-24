#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! Core `GeometryArray` methods: construction, metadata, properties, iteration.

#![allow(
    clippy::needless_pass_by_value,
    reason = "PyO3 special-method receivers must retain their binding-compatible ownership shape"
)]

use pyo3::types::PyList;

use super::*;
use crate::py::classes::coordinate_methods::ReplacementAxis;

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

fn replacement_coordseq(old: &CoordSeq, replacement: CoordinateReplacement) -> PyResult<CoordSeq> {
    if replacement.positional && old.axes() != replacement.axes {
        return Err(InvalidGeometryError::new_err(
            "coordinates must preserve each coordinate sequence axes",
        ));
    }
    let zs = match replacement.zs {
        ReplacementAxis::Replace(values) => {
            if !old.axes().has_z() {
                return Err(InvalidGeometryError::new_err(
                    "coordinates must preserve each coordinate sequence axes",
                ));
            }
            Some(values)
        },
        ReplacementAxis::Carry => old.carried_zs(),
    };
    let ms = match replacement.ms {
        ReplacementAxis::Replace(values) => {
            if !old.axes().has_m() {
                return Err(InvalidGeometryError::new_err(
                    "coordinates must preserve each coordinate sequence axes",
                ));
            }
            Some(values)
        },
        ReplacementAxis::Carry => old.carried_ms(),
    };
    CoordSeq::from_arc_columns(replacement.xs, replacement.ys, zs, ms).map_err(PyErr::from)
}

fn validate_packed_replacement_rings(
    coords: &CoordSeq,
    ring_offsets: &CsrOffsetColumn<RingLevel>,
) -> PyResult<()> {
    // Same active-ordinate closure as pack_admission::ring_seq_is_packable /
    // pickle admission (D05): XY-only same_point would admit Z/M-open rings
    // into trusted packed storage that the unpickler then rejects.
    use crate::geometry::same_active_position;
    let width = coords.len();
    for [start, end] in ring_offsets.array_windows::<2>() {
        let start = *start as usize;
        let end = *end as usize;
        debug_assert!(end - start >= crate::geometry::Ring::MIN_VERTICES_CLOSED);
        debug_assert!(end <= width);
        let first = coords.point_at(start);
        let last = coords.point_at(end - 1);
        if !same_active_position(first, last) {
            return Err(InvalidGeometryError::new_err("polygon ring must be closed"));
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
            let coords = replacement_coordseq(coords, replacement)?;
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
            let coords = replacement_coordseq(coords, replacement)?;
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
            let coords = replacement_coordseq(coords, replacement)?;
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
    if !matches!(array.storage(), GeometryArrayStorage::Mixed(_)) {
        return Ok(array
            .replace_packed_coords_detached(py, array.frame.clone(), replacement)?
            .expect("non-Mixed storage has packed columns"));
    }
    let mut present_cursor = 0;
    let mut items = Vec::with_capacity(array.storage().len());
    for (missing, shape) in array.masked_shape_rows() {
        let out_shape = if missing {
            PyGeometryArray::missing_placeholder()
        } else {
            let count = shape.coord_count();
            let sub = slice_replacement_for_shape(&replacement, present_cursor, count);
            present_cursor += count;
            replace_shape_coordinates(&shape, &sub)?
        };
        items.push(PyGeometry::with_frame(out_shape, array.frame.clone()));
    }
    debug_assert_eq!(present_cursor, replacement.len);
    Ok(PyGeometryArray::pack_or_mixed(items, array.frame.clone())
        .with_missing_mask(array.missing().cloned()))
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
        let shapes: Vec<Shape> = self
            .present_shape_rows()
            .map(|(_, shape)| shape.into_owned())
            .filter(|shape| !shape.is_empty())
            .collect();
        if shapes.is_empty() {
            return Err(CRSError::new_err(
                "estimate_local_crs requires at least one present, non-empty geometry",
            ));
        }
        let collection = Shape::GeometryCollection(shapes);
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
        let mask: Vec<bool> = raw_items
            .iter()
            .map(|item| {
                let missing = item.is_none();
                has_missing |= missing;
                missing
            })
            .collect();
        let present: Vec<_> = raw_items
            .iter()
            .zip(&mask)
            .filter(|(_, missing)| !**missing)
            .map(|(item, _)| item.clone())
            .collect();
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
                let items = shapes
                    .into_iter()
                    .map(|shape| PyGeometry::with_frame(shape, frame.clone()))
                    .collect();
                (items, frame)
            },
        };
        let array = Self::pack_or_mixed(present_items, frame);
        Ok(if has_missing {
            Self::scatter_present_rows(
                &array,
                MissingMask::from_vec(mask.len(), mask)
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
            self.masked_shape_rows(),
            self.storage().len(),
            SVG_ARRAY_PREVIEW,
        ))
    }

    pub fn _repr_html_svg(&self) -> String {
        geometry_array_svg_grid_html_masked(
            self.masked_shape_rows(),
            self.storage().len(),
            SVG_ARRAY_PREVIEW,
        )
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
        let features = PyList::empty(py);
        for row in 0..self.storage().len() {
            let feature = PyDict::new(py);
            feature.set_item("type", "Feature")?;
            feature.set_item("id", row)?;
            feature.set_item("properties", PyDict::new(py))?;
            let geometry = if self.is_row_missing(row) {
                py.None().into_bound(py)
            } else {
                let geometry =
                    self.storage()
                        .geometry_at(row, self.frame.clone(), self.row_frame_cache(row));
                crate::boundary::convert::geojson_dict(py, geometry.shape.shape())?.into_any()
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
            .storage()
            .iter_shapes()
            .map(|shape| shape.axes().as_str())
            .collect();
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
            self.storage()
                .iter_shapes()
                .enumerate()
                .map(|(row, shape)| !self.is_row_missing(row) && shape.has_z())
                .collect(),
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
            self.storage()
                .iter_shapes()
                .enumerate()
                .map(|(row, shape)| !self.is_row_missing(row) && shape.has_m())
                .collect(),
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
        if let Ok(Some(Some(bounds))) = Python::attach(|py| {
            self.reduce_packed_columns_detached(py, |columns| {
                Ok(total_bounds_from_columns(&columns))
            })
        }) {
            return Some(bounds.into_tuple());
        }
        self.storage().total_bounds().map(Bounds::into_tuple)
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
            GeometryArrayStorage::Points { .. } => vec!["Point"; self.storage().len()],
            GeometryArrayStorage::Lines { .. } => {
                vec!["LineString"; self.storage().len()]
            },
            GeometryArrayStorage::Polygons { .. } => {
                vec!["Polygon"; self.storage().len()]
            },
            GeometryArrayStorage::Mixed(_) => self
                .storage()
                .iter_shapes()
                .map(|shape| shape.geometry_type())
                .collect(),
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

        let mut labels = BTreeSet::new();
        let present = |row: usize| !self.is_row_missing(row);
        match self.uniform_axes() {
            Some(axes) => {
                let tag = axes.wkt_tag();
                match self.storage() {
                    GeometryArrayStorage::Points { .. } => {
                        if (0..self.storage().len()).any(present) {
                            labels.insert(format!("Point{tag}"));
                        }
                    },
                    GeometryArrayStorage::Lines { .. } => {
                        if (0..self.storage().len()).any(present) {
                            labels.insert(format!("LineString{tag}"));
                        }
                    },
                    GeometryArrayStorage::Polygons { .. } => {
                        if (0..self.storage().len()).any(present) {
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
        labels.into_iter().collect()
    }
    /// Top-level parts grouped by source row.
    ///
    /// Simple geometries form one-element groups, multipart and collection
    /// geometries expose their immediate members, and empty or missing rows
    /// form empty groups. Use free function ``parts`` for a flattened materialized
    /// `GeometryArray` instead.
    ///
    /// Returns
    /// -------
    /// Groups
    ///     One `GeometryArray` group per input row.
    #[getter]
    pub fn parts(&self, py: Python<'_>) -> PyResult<crate::py::vectors::Groups> {
        self.flat_map_shapes_groups(py, |shape| Ok(shape.parts().collect()))
    }
    /// Per-geometry top-level part count (see `Geometry.num_geometries`):
    /// ``1`` for a single point/line/polygon, the member count for a
    /// multi/collection, ``0`` for empty. Missing rows use Shapely's
    /// ``None``-geometry sentinel ``0``; use `.is_missing` to distinguish
    /// them from present empty geometries.
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
        crate::py::numpy::int64_array(py, match self.storage() {
            GeometryArrayStorage::Points { .. }
            | GeometryArrayStorage::Lines { .. }
            | GeometryArrayStorage::Polygons { .. } => (0..self.storage().len())
                .map(|row| i64::from(!self.is_row_missing(row)))
                .collect::<Vec<_>>(),
            GeometryArrayStorage::Mixed(_) => self
                .masked_shape_rows()
                .map(|(missing, shape)| {
                    if missing {
                        0
                    } else {
                        shape.part_count() as i64
                    }
                })
                .collect(),
        })
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
            self.masked_shape_rows()
                .map(|(missing, shape)| {
                    if missing {
                        0
                    } else {
                        shape.coord_count() as i64
                    }
                })
                .collect(),
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
        crate::py::numpy::int64_array(py, match self.storage() {
            GeometryArrayStorage::Points { .. } => (0..self.storage().len())
                .map(|row| if self.is_row_missing(row) { -1 } else { 0 })
                .collect::<Vec<_>>(),
            GeometryArrayStorage::Lines { .. } => (0..self.storage().len())
                .map(|row| if self.is_row_missing(row) { -1 } else { 1 })
                .collect::<Vec<_>>(),
            GeometryArrayStorage::Polygons { .. } => (0..self.storage().len())
                .map(|row| if self.is_row_missing(row) { -1 } else { 2 })
                .collect::<Vec<_>>(),
            GeometryArrayStorage::Mixed(_) => self
                .masked_shape_rows()
                .map(|(missing, shape)| {
                    if missing {
                        -1
                    } else {
                        i64::from(shape.topological_dimension().code())
                    }
                })
                .collect(),
        })
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
    pub(crate) fn is_empty_unary_packed(&self) -> Vec<bool> {
        match self.storage() {
            GeometryArrayStorage::Points { .. } => {
                std::iter::repeat_n(false, self.storage().len()).collect()
            },
            GeometryArrayStorage::Lines {
                offsets, row_map, ..
            } => {
                let map = row_map.as_deref();
                (0..line_logical_len(offsets.as_slice(), map))
                    .map(|logical| map.csr_window(offsets.as_slice(), logical).is_empty())
                    .collect()
            },
            GeometryArrayStorage::Polygons {
                polygon_offsets,
                row_map,
                ..
            } => {
                let map = row_map.as_deref();
                (0..polygon_logical_len(polygon_offsets.as_slice(), map))
                    .map(|logical| {
                        let rings = polygon_rings_range(polygon_offsets.as_slice(), map, logical);
                        rings.is_empty()
                    })
                    .collect()
            },
            GeometryArrayStorage::Mixed(_) => self
                .storage()
                .iter_shapes()
                .map(|shape| shape.is_empty())
                .collect(),
        }
    }

    pub(crate) fn is_closed_unary_packed(&self) -> Vec<bool> {
        if let Some(closed) = self.storage().lines_bool(line_is_closed) {
            return closed;
        }
        self.storage().const_or_shape_bool(false, Shape::is_closed)
    }

    pub(crate) fn is_ring_unary_packed(&self) -> Vec<bool> {
        if let Some(ring) = self
            .storage()
            .lines_bool(|coords| line_is_closed(coords) && line_is_simple(coords))
        {
            return ring;
        }
        self.storage().const_or_shape_bool(false, Shape::is_ring)
    }

    pub(crate) fn is_ccw_unary_packed(&self) -> Vec<bool> {
        if let Some(ccw) = self.storage().lines_bool(line_is_ccw) {
            return ccw;
        }
        self.storage().const_or_shape_bool(false, Shape::is_ccw)
    }

    pub(crate) fn is_simple_unary_packed(&self) -> Option<Vec<bool>> {
        if let Some(simple) = self.storage().lines_bool(line_is_simple) {
            return Some(simple);
        }
        if let Some(simple) = self.storage().polygons_bool(polygon_is_valid) {
            return Some(simple);
        }
        if let GeometryArrayStorage::Points { .. } = self.storage() {
            return Some(std::iter::repeat_n(true, self.storage().len()).collect());
        }
        None
    }

    pub(crate) fn is_valid_unary_packed(&self) -> Option<Vec<bool>> {
        if let GeometryArrayStorage::Points { coords, row_map } = self.storage() {
            let map = row_map.as_deref();
            return Some(
                (0..point_logical_len(coords, map))
                    .map(|logical| {
                        let point = coords.point_at(physical_row(map, logical));
                        point.x.is_finite() && point.y.is_finite()
                    })
                    .collect(),
            );
        }
        if let Some(valid) = self.storage().lines_bool(line_is_valid) {
            return Some(valid);
        }
        if let Some(valid) = self.storage().polygons_bool(polygon_is_valid) {
            return Some(valid);
        }
        None
    }

    pub(crate) fn is_convex_unary_packed(&self) -> Vec<bool> {
        match self.storage() {
            GeometryArrayStorage::Points { .. } | GeometryArrayStorage::Lines { .. } => {
                vec![false; self.storage().len()]
            },
            GeometryArrayStorage::Polygons { .. } | GeometryArrayStorage::Mixed(_) => self
                .storage()
                .iter_shapes()
                .map(|shape| match shape.as_ref() {
                    Shape::Polygon(_) | Shape::Empty(EmptyKind::Polygon, _) => {
                        shape.is_convex().unwrap_or(false)
                    },
                    _ => false,
                })
                .collect(),
        }
    }

    pub(crate) fn crosses_antimeridian_unary_packed(&self) -> Option<Vec<bool>> {
        if let Some(crosses) = self.storage().lines_bool(line_crosses_antimeridian) {
            return Some(crosses);
        }
        if let GeometryArrayStorage::Points { .. } = self.storage() {
            return Some(std::iter::repeat_n(false, self.storage().len()).collect());
        }
        None
    }
}
