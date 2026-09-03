#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use pyo3::exceptions::PyOverflowError;
use pyo3::types::PyDict;

use crate::array::packed_columns::XyColumns;
use crate::array::{
    Bound, CsrOffsetColumn, DistanceUnit, GeometryArrayStorage, GeometryError, IntoPyObject as _,
    PackedColumnError, PackedColumns, Py, PyAny, PyAnyMethods as _, PyDictMethods as _, PyErr,
    PyGeometry, PyGeometryArray, PyResult, PyTupleMethods as _, PyTypeError, PyTypeMethods as _,
    Python, Shape, SpatialCurve, Typed, ValidityMask, bounds_3d_values_from_columns,
    bounds_values_from_columns_masked, concat_coord_columns, crs, crs_label,
    ensure_geographic_columns_present, geographic_bounds_values_from_columns, line_measure_masked,
    polygon_measure_masked, pymethods, reduce_lines_or_polygons, resolve_metric,
    segmented_planar_lengths, segmented_planar_lengths_3d,
};
use crate::dispatch::MetricOp;

#[pymethods]
impl PyGeometryArray {
    /// Concatenate one or more arrays sharing this array's CRS and epoch.
    ///
    /// Parameters
    /// ----------
    /// *others : GeometryArray
    ///     Arrays to append, in order; each must share this array's CRS and
    ///     epoch. With no arguments the array itself is returned.
    ///
    /// Returns
    /// -------
    /// GeometryArray
    #[pyo3(signature = (*others))]
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> a = gm.GeometryArray([gm.box(0, 0, 1, 1)])
    /// >>> a.concat(gm.GeometryArray([gm.Point(2, 2)])).to_wkt()
    /// ['POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))', 'POINT (2 2)']
    pub fn concat(&self, others: &Bound<'_, pyo3::types::PyTuple>) -> PyResult<Self> {
        if others.is_empty() {
            return Ok(self.clone());
        }
        let mut borrowed = crate::try_vec_with_capacity(others.len())?;
        for (offset, item) in others.iter().enumerate() {
            let index = offset + 1;
            let other = item.extract::<pyo3::PyRef<'_, Self>>().map_err(|_| {
                pyo3::exceptions::PyTypeError::new_err(format!(
                    "concat arguments must be GeometryArray, got {}",
                    item.get_type()
                        .name()
                        .map_or_else(|_| "<unknown>".to_owned(), |name| name.to_string(),)
                ))
            })?;
            self.ensure_concat_frame(&other, index)?;
            crate::try_push(&mut borrowed, other)?;
        }
        let mut arrays = crate::try_vec_with_capacity(
            borrowed
                .len()
                .checked_add(1)
                .ok_or_else(|| PyOverflowError::new_err("too many arrays to concatenate"))?,
        )?;
        crate::try_push(&mut arrays, self)?;
        for array in &borrowed {
            crate::try_push(&mut arrays, &**array)?;
        }
        if arrays.len() == 2 {
            self.concat_pair(arrays[1])
        } else {
            Self::concat_many(&arrays)
        }
    }

    /// Per-row smallest Z ordinate (``nan`` where a geometry carries no Z).
    ///
    /// Returns
    /// -------
    /// numpy.ndarray
    ///     Read-only ``float64`` ``numpy.ndarray`` of shape ``(n,)``; ``nan``
    ///     where Z is absent or the row is missing.
    #[getter]
    pub fn min_z(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.z_extreme_lane(py, |low, _| low, Shape::min_z)
    }
    /// Per-row largest Z ordinate (``nan`` where a geometry carries no Z).
    ///
    /// Returns
    /// -------
    /// numpy.ndarray
    ///     Read-only ``float64`` ``numpy.ndarray`` of shape ``(n,)``; ``nan``
    ///     where Z is absent or the row is missing.
    #[getter]
    pub fn max_z(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.z_extreme_lane(py, |_, high| high, Shape::max_z)
    }
    /// Per-row Z span (``max_z - min_z``; ``nan`` without Z).
    ///
    /// Returns
    /// -------
    /// numpy.ndarray
    ///     Read-only ``float64`` ``numpy.ndarray`` of shape ``(n,)``; ``nan``
    ///     where Z is absent or the row is missing.
    #[getter]
    pub fn z_range(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.z_extreme_lane(
            py,
            |low, high| high - low,
            |shape| shape.z_extremes().map(|(low, high)| high - low),
        )
    }
    /// Per-row smallest M ordinate (``nan`` where a geometry carries no M).
    ///
    /// Returns
    /// -------
    /// numpy.ndarray
    ///     Read-only ``float64`` ``numpy.ndarray`` of shape ``(n,)``; ``nan``
    ///     where M is absent or the row is missing.
    #[getter]
    pub fn min_m(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.m_extreme_lane(py, |low, _| low, Shape::min_m)
    }
    /// Per-row largest M ordinate (``nan`` where a geometry carries no M).
    ///
    /// Returns
    /// -------
    /// numpy.ndarray
    ///     Read-only ``float64`` ``numpy.ndarray`` of shape ``(n,)``; ``nan``
    ///     where M is absent or the row is missing.
    #[getter]
    pub fn max_m(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.m_extreme_lane(py, |_, high| high, Shape::max_m)
    }
    /// Per-row M span (``max_m - min_m``; ``nan`` without M).
    ///
    /// Returns
    /// -------
    /// numpy.ndarray
    ///     Read-only ``float64`` ``numpy.ndarray`` of shape ``(n,)``; ``nan``
    ///     where M is absent or the row is missing.
    #[getter]
    pub fn m_range(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.m_extreme_lane(
            py,
            |low, high| high - low,
            |shape| shape.m_extremes().map(|(low, high)| high - low),
        )
    }
    /// Per-row 3D bounding box ``(minx, miny, minz, maxx, maxy, maxz)``.
    ///
    /// Returns
    /// -------
    /// numpy.ndarray
    ///     Read-only ``float64`` ``numpy.ndarray`` of shape ``(n, 6)``; ``nan``
    ///     where Z is absent or the row is missing.
    #[getter]
    pub fn bounds_3d(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        if !self.has_missing()
            && self.has_z()
            && let Some(values) = self.reduce_packed_columns_detached(py, |columns| {
                Ok(bounds_3d_values_from_columns(&columns))
            })?
        {
            return crate::py::numpy::bounds3d_array(py, values);
        }
        crate::py::numpy::bounds3d_array(py, self.bounds_3d_rows())
    }
    /// Per-row area, measured for the array's CRS.
    ///
    /// A geographic CRS gives ellipsoidal square meters (geodesic, on the CRS's
    /// own ellipsoid); a projected CRS gives squared native coordinate units;
    /// a CRS-free array gives squared coordinate units. Use ``to_crs`` to
    /// change frame.
    ///
    /// Returns
    /// -------
    /// numpy.ndarray
    ///     Read-only ``float64`` ``numpy.ndarray`` of shape ``(n,)``; ``0`` for
    ///     points and curves; ``nan`` for missing rows.
    ///
    /// Raises
    /// ------
    /// CRSError
    ///     If the CRS lacks linear axis units for a metric result.
    ///
    /// See Also
    /// --------
    /// length : Length/perimeter under the same CRS-aware metric.
    #[getter]
    pub fn area(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        crate::area_natural_array(py, self)
    }
    /// Per-row length (curves) or perimeter (areal), measured for the array's
    /// CRS.
    ///
    /// A geographic CRS gives ellipsoidal meters (geodesic, on the CRS's own
    /// ellipsoid); a projected CRS gives native linear units; a CRS-free array
    /// gives coordinate units. Use ``to_crs`` to change frame.
    ///
    /// Returns
    /// -------
    /// numpy.ndarray
    ///     Read-only ``float64`` ``numpy.ndarray`` of shape ``(n,)``; ``0`` for
    ///     points; ``nan`` for missing rows.
    ///
    /// Raises
    /// ------
    /// CRSError
    ///     If the CRS lacks linear axis units for a metric result.
    ///
    /// See Also
    /// --------
    /// area : Area under the same CRS-aware metric.
    #[getter]
    pub fn length(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        crate::length_natural_array(py, self)
    }
    /// Per-row 3D length of curves with Z, measured for the array's CRS.
    ///
    /// Returns
    /// -------
    /// numpy.ndarray
    ///     Read-only ``float64`` ``numpy.ndarray`` of shape ``(n,)``; ``nan``
    ///     where Z is missing on a vertex or the row is missing.
    ///
    /// Raises
    /// ------
    /// CRSError
    ///     If the CRS lacks linear axis units for a metric result.
    #[getter]
    pub fn length_3d(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        if let Some(mut values) = self.length_3d_unary_packed(py, None, MetricOp::Length3d)? {
            if let Some(mask) = self.missing() {
                for (value, missing) in values.iter_mut().zip(mask.iter()) {
                    if missing {
                        *value = f64::NAN;
                    }
                }
            }
            return crate::py::numpy::float64_array(py, values);
        }
        crate::dispatch::unary_array(
            py,
            self,
            crate::dispatch::Operation::Metric(MetricOp::Length3d),
            None,
            None,
            crate::dispatch::kernels::unary_length_3d,
        )
    }

    /// Return the array reordered along a space-filling curve.
    ///
    /// Parameters
    /// ----------
    /// curve : {'hilbert', 'morton'}, default hilbert
    ///     ``hilbert`` prioritizes locality; ``morton`` uses Z-order.
    ///
    /// level : int, default 16
    ///     Curve depth (``1``-``32``).
    ///
    /// bounds : iterable[float], optional
    ///     ``(minx, miny, maxx, maxy)`` extent for keying; defaults to ``total_bounds``.
    ///
    /// Returns
    /// -------
    /// GeometryArray
    ///     A new array with the same rows in curve-key order; empty and
    ///     missing rows sort last.
    #[pyo3(signature = (*, curve = SpatialCurve::Hilbert, level = 16, bounds = None), text_signature = "($self, *, curve='hilbert', level=16, bounds=None)")]
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> arr = gm.GeometryArray([gm.Point(1, 0), gm.Point(0, 0)])
    /// >>> arr.sort_by_spatial_key().to_wkt()
    /// ['POINT (0 0)', 'POINT (1 0)']
    pub fn sort_by_spatial_key(
        &self,
        curve: SpatialCurve,
        #[pyo3(from_py_with = crate::strict_i64)] level: i64,
        bounds: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let keys = self.curve_keys(level, bounds, "sort_by_spatial_key", curve.into())?;
        Ok(self.taken_by_keys(&keys))
    }

    /// Intersect all present geometries into one geometry.
    ///
    /// The region common to EVERY present row (missing rows are skipped, the
    /// SQL/pandas aggregate convention). The array sibling of
    /// `intersection_all`, which takes raw iterables.
    ///
    /// Returns
    /// -------
    /// Geometry
    ///     A single geometry covered by every present row.
    ///
    /// Raises
    /// ------
    /// InvalidGeometryError
    ///     If the array has no present rows or the overlay cannot be constructed.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> panes = gm.GeometryArray([
    /// ...     gm.box(0, 0, 3, 3), gm.box(1, 1, 4, 4), gm.box(2, 2, 5, 5),
    /// ... ])
    /// >>> panes.intersection_all().to_wkt()
    /// 'POLYGON ((2 2, 3 2, 3 3, 2 3, 2 2))'
    pub fn intersection_all(&self, py: Python<'_>) -> PyResult<crate::Typed> {
        self.apply_overlay_all(py, Shape::intersection_all_topo)
    }

    /// Symmetric difference of all present geometries.
    ///
    /// The region covered by an ODD number of present rows (missing rows are
    /// skipped). The array sibling of `symmetric_difference_all`, which takes
    /// raw iterables.
    ///
    /// Returns
    /// -------
    /// Geometry
    ///     A single geometry covered by an odd number of present rows.
    ///
    /// Raises
    /// ------
    /// InvalidGeometryError
    ///     If the array has no present rows or the overlay cannot be constructed.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> panes = gm.GeometryArray([
    /// ...     gm.box(0, 0, 2, 2), gm.box(0, 0, 2, 2), gm.box(1, 1, 3, 3),
    /// ... ])
    /// >>> panes.symmetric_difference_all().to_wkt()  # the duplicate cancels
    /// 'POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))'
    pub fn symmetric_difference_all(&self, py: Python<'_>) -> PyResult<crate::Typed> {
        self.apply_overlay_all(py, Shape::symmetric_difference_all_topo)
    }

    /// Union all present geometries into one geometry.
    ///
    /// Missing rows are skipped (the SQL/pandas aggregate convention).
    ///
    /// Returns
    /// -------
    /// Geometry
    ///     A single geometry covering every present row.
    ///
    /// Raises
    /// ------
    /// InvalidGeometryError
    ///     If the array has no present rows or the overlay cannot be constructed.
    ///
    /// See Also
    /// --------
    /// coverage_union : Faster dissolve for a valid polygonal coverage.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> panes = gm.GeometryArray([gm.box(0, 0, 2, 2), gm.box(1, 1, 3, 3)])
    /// >>> panes.union_all().to_wkt()
    /// 'POLYGON ((0 0, 2 0, 2 1, 3 1, 3 3, 1 3, 1 2, 0 2, 0 0))'
    pub fn union_all(&self, py: Python<'_>) -> PyResult<crate::Typed> {
        let strictness = crate::geometry::Strictness::Lenient;
        let shapes = if let Some(multipoint) = self.packed_points_as_multipoint()? {
            let mut shapes = crate::try_vec_with_capacity(1)?;
            crate::try_push(&mut shapes, multipoint)?;
            shapes
        } else {
            self.with_borrowed_shapes(|borrowed| {
                let mut shapes = Vec::new();
                crate::boundary::try_reserve_vec(&mut shapes, borrowed.len())?;
                for shape in borrowed {
                    crate::boundary::try_push_vec(&mut shapes, (*shape).clone())?;
                }
                Ok(shapes)
            })?
        };
        let domain = crate::geometry::topology_domain(&self.frame)?;
        let shape = py.detach(move || Shape::union_all_topo(&shapes, domain, strictness))?;
        Ok(Typed(PyGeometry::with_frame(shape, self.frame.clone())))
    }

    /// Dissolve geometries into per-group unions.
    ///
    /// Parameters
    /// ----------
    /// by : iterable
    ///     One grouping key per row (same length as the array).
    ///
    /// Returns
    /// -------
    /// tuple of (GeometryArray, list)
    ///     One union per distinct key plus the parallel keys in first-occurrence order.
    #[pyo3(signature = (by))]
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> arr = gm.GeometryArray([gm.box(0, 0, 1, 1), gm.box(1, 0, 2, 1)])
    /// >>> geoms, keys = arr.dissolve(by=[0, 0])
    /// >>> geoms.to_wkt()
    /// ['POLYGON ((0 0, 2 0, 2 1, 0 1, 0 0))']
    pub fn dissolve(&self, py: Python<'_>, by: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        // Fixed-count alignment via keystone: fallible exact reserve + stop after
        // expected+1 so unbounded `itertools.repeat` cannot grow forever.
        let expected = self.len();
        let keys = crate::collect_py_iter_exact(by, expected, Ok, |got| {
            GeometryError::new_err(format!(
                "by must provide one key per geometry, got {} for {expected}",
                if got > expected {
                    format!(">{expected}")
                } else {
                    got.to_string()
                }
            ))
        })?;
        if let Some(mask) = self.missing() {
            // Missing rows leave the group-by entirely (their keys too) —
            // SQL/pandas aggregation semantics.
            let dense = self.drop_missing();
            let mut kept = crate::try_vec_with_capacity(keys.len())?;
            for (key, missing) in keys.iter().zip(mask.iter()) {
                if !missing {
                    crate::try_push(&mut kept, key.clone())?;
                }
            }
            let kept_list = pyo3::types::PyList::new(py, &kept)?;
            return dense.dissolve(py, kept_list.as_any());
        }
        let strictness = crate::geometry::Strictness::Lenient;
        let mut groups: Vec<Py<PyAny>> = crate::try_vec_with_capacity(keys.len())?;
        let mut group_shapes: Vec<Vec<Shape>> = crate::try_vec_with_capacity(keys.len())?;
        let hash_groups = PyDict::new(py);
        let mut unhashable_groups: Vec<usize> = crate::try_vec_with_capacity(keys.len())?;
        for (key, (_, shape)) in keys.iter().zip(self.present_shape_rows()) {
            let hashable = match key.hash() {
                Ok(_) => true,
                Err(error) if error.is_instance_of::<PyTypeError>(py) => false,
                Err(error) => return Err(error),
            };
            let index = if hashable {
                let hashed = hash_groups
                    .get_item(key)?
                    .map(|index| index.extract::<usize>())
                    .transpose()?;
                if hashed.is_some() {
                    hashed
                } else {
                    let mut equal = None;
                    for &candidate in &unhashable_groups {
                        if groups[candidate].bind(py).eq(key)? {
                            equal = Some(candidate);
                            break;
                        }
                    }
                    equal
                }
            } else {
                let mut equal = None;
                for (candidate, existing) in groups.iter().enumerate() {
                    if existing.bind(py).eq(key)? {
                        equal = Some(candidate);
                        break;
                    }
                }
                equal
            };
            if let Some(index) = index {
                crate::try_push(&mut group_shapes[index], shape.into_owned())?;
            } else {
                let index = groups.len();
                crate::try_push(&mut groups, key.clone().unbind())?;
                let mut values = crate::try_vec_with_capacity(1)?;
                crate::try_push(&mut values, shape.into_owned())?;
                crate::try_push(&mut group_shapes, values)?;
                if hashable {
                    hash_groups.set_item(key, index)?;
                } else {
                    crate::try_push(&mut unhashable_groups, index)?;
                }
            }
        }
        let frame = self.frame.clone();
        let domain = crate::geometry::topology_domain(&frame)?;
        let mut shapes = crate::try_vec_with_capacity(group_shapes.len())?;
        for group in group_shapes {
            let shape = py.detach(move || Shape::union_all_topo(&group, domain, strictness))?;
            crate::try_push(&mut shapes, shape)?;
        }
        Ok((Self::from_shapes(shapes, frame), groups)
            .into_pyobject(py)?
            .into_any()
            .unbind())
    }

    /// Number of rows (including missing rows).
    ///
    /// Returns
    /// -------
    /// int
    pub fn __len__(&self) -> usize {
        self.len()
    }

    /// ``False`` only when the array has zero rows.
    ///
    /// Returns
    /// -------
    /// bool
    pub fn __bool__(&self) -> bool {
        self.len() > 0
    }
}

impl PyGeometryArray {
    pub(crate) fn area_unary_packed(
        &self,
        py: Python<'_>,
        unit: Option<DistanceUnit>,
        op: MetricOp,
    ) -> PyResult<Option<Vec<f64>>> {
        if !matches!(self.storage(), GeometryArrayStorage::Polygons { .. }) {
            return Ok(None);
        }
        let missing = self.missing().cloned();
        match resolve_metric(self.crs_str(), unit, op)? {
            crs::MetricModel::Planar { to_metre } => {
                self.reduce_packed_columns_detached(py, move |columns| {
                    let PackedColumns::Polygons(polygon_columns) = columns else {
                        unreachable!("area packed lane requires polygon storage");
                    };
                    let XyColumns { xs, ys } = polygon_columns.physical_xy();
                    let scale = to_metre.get() * to_metre.get();
                    let ring_offsets = polygon_columns.ring_offsets();
                    let mut values = Vec::new();
                    crate::boundary::try_reserve_vec(&mut values, polygon_columns.rows())
                        .map_err(PackedColumnError::Batch)?;
                    for row in 0..polygon_columns.rows() {
                        if missing.as_ref().is_some_and(|mask| mask.is_missing(row)) {
                            crate::boundary::try_push_vec(&mut values, f64::NAN)
                                .map_err(PackedColumnError::Batch)?;
                            continue;
                        }
                        let rings = polygon_columns.polygon_rings(row);
                        let area = crate::geometry::polygon_area_measure_columns(
                            xs,
                            ys,
                            ring_offsets,
                            rings,
                        );
                        crate::boundary::try_push_vec(&mut values, area * scale)
                            .map_err(PackedColumnError::Batch)?;
                    }
                    Ok(values)
                })
            },
            model @ crs::MetricModel::Geodesic { frame, .. } => {
                let crs::ResolvedMetric::Geodesic(metric) =
                    crs::ResolvedMetric::from_model(&model)?
                else {
                    unreachable!("geodesic model resolves to a geodesic metric");
                };
                let geodesic = metric.geodesic;
                self.reduce_packed_columns_detached(py, move |columns| {
                    ensure_geographic_columns_present(&columns, missing.as_ref(), frame)
                        .map_err(PackedColumnError::Batch)?;
                    let PackedColumns::Polygons(polygon_columns) = columns else {
                        unreachable!("area packed lane requires polygon storage");
                    };
                    let XyColumns { xs, ys } = polygon_columns.physical_xy();
                    let ring_measure = |xs: &[f64], ys: &[f64], _window: std::ops::Range<usize>| {
                        crate::crs::geodesic_ring_measure_columns(&geodesic, xs, ys, frame)
                            .1
                            .abs()
                    };
                    polygon_measure_masked(
                        xs,
                        ys,
                        polygon_columns.ring_offsets(),
                        polygon_columns.polygon_offsets(),
                        polygon_columns.selection(),
                        missing.as_ref(),
                        ring_measure,
                        |rings, ring| {
                            // Mirror the scalar `shell - holes.sum()` expression order
                            // exactly (sum holes, then one subtraction) so packed and
                            // scalar geodesic area stay bit-identical for any hole count.
                            let (start, end) = (rings.start, rings.end);
                            let shell = ring(start);
                            let holes: f64 = (start + 1..end).map(ring).sum();
                            shell - holes
                        },
                    )
                    .map_err(PackedColumnError::Batch)
                })
            },
        }
    }

    pub(crate) fn length_unary_packed(
        &self,
        py: Python<'_>,
        unit: Option<DistanceUnit>,
        op: MetricOp,
    ) -> PyResult<Option<Vec<f64>>> {
        if !matches!(
            self.storage(),
            GeometryArrayStorage::Lines { .. } | GeometryArrayStorage::Polygons { .. }
        ) {
            return Ok(None);
        }
        let missing = self.missing().cloned();
        match resolve_metric(self.crs_str(), unit, op)? {
            crs::MetricModel::Planar { to_metre } => {
                self.reduce_packed_columns_detached(py, move |columns| {
                    let values = reduce_lines_or_polygons(
                        columns,
                        |line_columns| {
                            let XyColumns { xs, ys } = line_columns.physical_xy();
                            // Dual PerRun/ColumnStream over topology-only runs.
                            segmented_planar_lengths(
                                line_columns.segmented_runs(missing.as_ref()),
                                xs,
                                ys,
                                to_metre.get(),
                            )
                        },
                        |polygon_columns| {
                            let XyColumns { xs, ys } = polygon_columns.physical_xy();
                            segmented_planar_lengths(
                                polygon_columns.segmented_runs(missing.as_ref()),
                                xs,
                                ys,
                                to_metre.get(),
                            )
                        },
                    );
                    values.map_err(PackedColumnError::Batch)
                })
            },
            model @ crs::MetricModel::Geodesic { frame, .. } => {
                let crs::ResolvedMetric::Geodesic(metric) =
                    crs::ResolvedMetric::from_model(&model)?
                else {
                    unreachable!("geodesic model resolves to a geodesic metric");
                };
                let geodesic = metric.geodesic;
                self.reduce_packed_columns_detached(py, move |columns| {
                    ensure_geographic_columns_present(&columns, missing.as_ref(), frame)
                        .map_err(PackedColumnError::Batch)?;
                    let values = match columns {
                        PackedColumns::Lines(line_columns) => {
                            let XyColumns { xs, ys } = line_columns.physical_xy();
                            line_measure_masked(
                                xs,
                                ys,
                                line_columns.offsets(),
                                line_columns.selection(),
                                missing.as_ref(),
                                |xs, ys, _window| {
                                    crate::crs::geodesic_line_length_columns(
                                        &geodesic, xs, ys, frame,
                                    )
                                },
                            )
                        },
                        PackedColumns::Polygons(polygon_columns) => {
                            let XyColumns { xs, ys } = polygon_columns.physical_xy();
                            let ring_measure =
                                |xs: &[f64], ys: &[f64], _window: std::ops::Range<usize>| {
                                    crate::crs::geodesic_ring_measure_columns(
                                        &geodesic, xs, ys, frame,
                                    )
                                    .0
                                };
                            polygon_measure_masked(
                                xs,
                                ys,
                                polygon_columns.ring_offsets(),
                                polygon_columns.polygon_offsets(),
                                polygon_columns.selection(),
                                missing.as_ref(),
                                ring_measure,
                                // Mirror the scalar `shell + holes.sum()` order exactly
                                // so packed and scalar geodesic perimeter stay bit-identical.
                                |rings, ring| {
                                    let (start, end) = (rings.start, rings.end);
                                    let shell = ring(start);
                                    let holes: f64 = (start + 1..end).map(ring).sum();
                                    shell + holes
                                },
                            )
                        },
                        PackedColumns::Points(_) => {
                            unreachable!("length packed lane requires line or polygon storage")
                        },
                    };
                    values.map_err(PackedColumnError::Batch)
                })
            },
        }
    }

    pub(crate) fn length_3d_unary_packed(
        &self,
        py: Python<'_>,
        unit: Option<DistanceUnit>,
        op: MetricOp,
    ) -> PyResult<Option<Vec<f64>>> {
        let to_metre =
            crate::broadcast::resolve_metric_3d(self.crs_str(), unit, op)?.coordinate_scale();
        if let GeometryArrayStorage::Points { .. } = self.storage() {
            let mut values = Vec::new();
            crate::boundary::try_reserve_vec(&mut values, self.len()).map_err(PyErr::from)?;
            values.resize(self.len(), 0.0);
            return Ok(Some(values));
        }
        if !matches!(
            self.storage(),
            GeometryArrayStorage::Lines { .. } | GeometryArrayStorage::Polygons { .. }
        ) {
            return Ok(None);
        }
        let missing = self.missing().cloned();
        self.reduce_packed_columns_detached(py, move |columns| {
            let values = reduce_lines_or_polygons(
                columns,
                |line_columns| {
                    let Some(zs) = line_columns.coords().zs() else {
                        let mut values = Vec::new();
                        crate::boundary::try_reserve_vec(&mut values, line_columns.rows())?;
                        values.resize(line_columns.rows(), f64::NAN);
                        return Ok(values);
                    };
                    let XyColumns { xs, ys } = line_columns.physical_xy();
                    segmented_planar_lengths_3d(
                        line_columns.segmented_runs(missing.as_ref()),
                        xs,
                        ys,
                        zs,
                        to_metre,
                    )
                },
                |polygon_columns| {
                    let Some(zs) = polygon_columns.coords().zs() else {
                        let mut values = Vec::new();
                        crate::boundary::try_reserve_vec(&mut values, polygon_columns.rows())?;
                        values.resize(polygon_columns.rows(), f64::NAN);
                        return Ok(values);
                    };
                    let XyColumns { xs, ys } = polygon_columns.physical_xy();
                    segmented_planar_lengths_3d(
                        polygon_columns.segmented_runs(missing.as_ref()),
                        xs,
                        ys,
                        zs,
                        to_metre,
                    )
                },
            );
            values.map_err(PackedColumnError::Batch)
        })
    }

    pub(crate) fn bounds_unary_packed(&self, py: Python<'_>) -> PyResult<Option<Vec<f64>>> {
        let domain = crate::geometry::topology_domain(&self.frame)?;
        let missing = self.missing().cloned();
        if let Some(values) = self.reduce_packed_columns_detached(py, move |columns| {
            let values = if let crate::geometry::TopologyDomain::Geographic(frame) = domain {
                missing.as_ref().map_or_else(
                    || geographic_bounds_values_from_columns(&columns, frame),
                    |missing| {
                        super::packed_column_kernels::geographic_bounds_values_from_columns_masked(
                            &columns,
                            frame,
                            Some(missing),
                        )
                    },
                )
            } else {
                bounds_values_from_columns_masked(&columns, missing.as_ref())
            };
            values.map_err(PackedColumnError::Batch)
        })? {
            return Ok(Some(values));
        }
        Ok(None)
    }
}

impl PyGeometryArray {
    /// The one frame check every `concat` entry point shares: the operands
    /// must name the same frame, and the left array's stored label wins —
    /// matching binary results and `GeometryArray` construction.
    ///
    /// `index` positions the offending operand in the caller's argument list
    /// so the error names which array disagreed.
    fn ensure_concat_frame(&self, other: &Self, index: usize) -> PyResult<()> {
        let agrees = match (self.crs_ref(), other.crs_ref()) {
            (None, None) => true,
            (Some(left), Some(right)) => crate::crs_operationally_equal(left, right)?,
            _ => false,
        };
        if !agrees {
            return Err(crate::py::errors::crs_mismatch_error(
                format!(
                    "concat requires a shared CRS; left is {}, right is {}",
                    crs_label(self.crs_str()),
                    crs_label(other.crs_str()),
                ),
                self.crs_str(),
                other.crs_str(),
                Some(index),
            ));
        }
        if self.epoch() != other.epoch() {
            return Err(crate::py::errors::epoch_mismatch_error(
                "concat requires a shared coordinate epoch",
                self.epoch(),
                other.epoch(),
                Some(index),
            ));
        }
        Ok(())
    }

    fn concat_many(arrays: &[&Self]) -> PyResult<Self> {
        let first = arrays
            .first()
            .expect("concat_many always receives the method receiver");
        for (index, other) in arrays[1..].iter().enumerate() {
            first.ensure_concat_frame(other, index + 1)?;
        }

        let total_rows = arrays.iter().try_fold(0_usize, |total, array| {
            total
                .checked_add(array.len())
                .ok_or_else(|| PyOverflowError::new_err("concatenated row count overflows usize"))
        })?;
        let merged_mask =
            ValidityMask::try_concat(arrays.iter().map(|array| (array.len(), array.missing())))
                .map_err(PyErr::from)?;

        if let Some(packed) = Self::concat_packed_many(arrays)? {
            return Ok(packed.with_missing_mask(merged_mask));
        }

        let mut shapes = crate::try_vec_with_capacity(total_rows)?;
        for array in arrays {
            for (_, shape) in array.present_shape_rows() {
                crate::try_push(&mut shapes, shape.into_owned())?;
            }
        }
        Ok(Self::from_shapes(shapes, first.frame.clone()).with_missing_mask(merged_mask))
    }

    pub(crate) fn concat_pair(&self, other: &Self) -> PyResult<Self> {
        // Missing masks concatenate row-wise (dense sides contribute
        // all-present runs); computed up front, attached to the merged array.
        let merged_mask = ValidityMask::try_concat([
            (self.len(), self.missing()),
            (other.len(), other.missing()),
        ])
        .map_err(PyErr::from)?;
        let merged = self.concat_pair_dense(other)?;
        Ok(merged.with_missing_mask(merged_mask))
    }

    fn concat_pair_dense(&self, other: &Self) -> PyResult<Self> {
        self.ensure_concat_frame(other, 1)?;
        if self.is_compact_nullable() || other.is_compact_nullable() {
            let total_rows = self.len().checked_add(other.len()).ok_or_else(|| {
                PyOverflowError::new_err("concatenated row count overflows usize")
            })?;
            let mut shapes = crate::try_vec_with_capacity(total_rows)?;
            for (_, shape) in self.present_shape_rows() {
                crate::try_push(&mut shapes, shape.into_owned())?;
            }
            for (_, shape) in other.present_shape_rows() {
                crate::try_push(&mut shapes, shape.into_owned())?;
            }
            return Ok(Self::from_shapes(shapes, self.frame.clone()));
        }
        if let Some(joined) = self.try_concat_packed_points(other)? {
            return Ok(joined);
        }
        if let (
            GeometryArrayStorage::Lines {
                coords: left,
                offsets: left_offsets,
                row_map: left_row_map,
            },
            GeometryArrayStorage::Lines {
                coords: right,
                offsets: right_offsets,
                row_map: right_row_map,
            },
        ) = (self.storage(), other.storage())
            && left.axes() == right.axes()
        {
            if left_row_map.reorders() {
                return self.materialize_packed_lines()?.concat_pair(other);
            }
            if right_row_map.reorders() {
                let right_array = other.materialize_packed_lines()?;
                return self.concat_pair(&right_array);
            }
            let out_coords = concat_coord_columns(left, right)?;
            let out_offsets = CsrOffsetColumn::rebase_concat_validated(
                left_offsets,
                right_offsets,
                out_coords.len(),
            )?;
            return Ok(Self::packed_lines(
                out_coords,
                out_offsets,
                self.frame.clone(),
            ));
        }
        if let (
            GeometryArrayStorage::Polygons {
                coords: left,
                ring_offsets: left_rings,
                polygon_offsets: left_polygons,
                row_map: left_row_map,
            },
            GeometryArrayStorage::Polygons {
                coords: right,
                ring_offsets: right_rings,
                polygon_offsets: right_polygons,
                row_map: right_row_map,
            },
        ) = (self.storage(), other.storage())
            && left.axes() == right.axes()
        {
            if left_row_map.reorders() {
                return self.materialize_packed_polygons()?.concat_pair(other);
            }
            if right_row_map.reorders() {
                let right_array = other.materialize_packed_polygons()?;
                return self.concat_pair(&right_array);
            }
            return self.concat_packed_polygons(
                left,
                left_rings,
                left_polygons,
                right,
                right_rings,
                right_polygons,
            );
        }
        let total_rows = self
            .len()
            .checked_add(other.len())
            .ok_or_else(|| PyOverflowError::new_err("concatenated row count overflows usize"))?;
        let mut shapes = crate::try_vec_with_capacity(total_rows)?;
        for (_, shape) in self.present_shape_rows() {
            crate::try_push(&mut shapes, shape.into_owned())?;
        }
        for (_, shape) in other.present_shape_rows() {
            crate::try_push(&mut shapes, shape.into_owned())?;
        }
        Ok(Self::from_shapes(shapes, self.frame.clone()))
    }
}

impl PyGeometryArray {
    /// Shared all-input overlay over the PRESENT rows: the kernel
    /// receives owned shapes (aggregates skip missing rows), detached from
    /// the GIL like `dissolve`.
    fn apply_overlay_all(
        &self,
        py: Python<'_>,
        kernel: impl FnOnce(
            &[Shape],
            crate::geometry::TopologyDomain,
            crate::geometry::Strictness,
        ) -> crate::error::Result<Shape>
        + Send,
    ) -> PyResult<crate::Typed> {
        let strictness = crate::geometry::Strictness::Lenient;
        let shapes = self.present_shapes();
        let domain = crate::geometry::topology_domain(&self.frame)?;
        let shape = py.detach(move || kernel(&shapes, domain, strictness))?;
        Ok(crate::Typed(PyGeometry::with_frame(
            shape,
            self.frame.clone(),
        )))
    }
}

#[cfg(test)]
mod geographic_masked_bounds_tests {
    use super::*;
    use crate::boundary::{Frame, crs_arc_static};
    use crate::geometry::{LineSeq, Point, Polygon, Ring};

    fn line(points: &[(f64, f64)]) -> Shape {
        Shape::LineString(
            LineSeq::try_new(
                points
                    .iter()
                    .map(|&(x, y)| Point::new_raw_xy(x, y))
                    .collect::<Vec<_>>()
                    .into(),
            )
            .expect("line coordinate sequence must contain at least two vertices"),
        )
    }

    fn polygon(points: &[(f64, f64)]) -> Shape {
        Shape::Polygon(Polygon::new(
            Ring::from_closed(
                points
                    .iter()
                    .map(|&(x, y)| Point::new_raw_xy(x, y))
                    .collect::<Vec<_>>(),
            ),
            Vec::new(),
        ))
    }

    #[test]
    fn geographic_missing_rows_keep_the_packed_bounds_lane() {
        crate::test_support::initialize_python();
        let array = PyGeometryArray::from_shapes(
            vec![
                line(&[(170.0, 10.0), (-170.0, 20.0)]),
                line(&[(99.0, 99.0), (100.0, 100.0)]),
                line(&[(1.0, 2.0), (3.0, 4.0)]),
            ],
            Frame::new(Some(crs_arc_static("OGC:CRS84")), None).expect("valid test frame"),
        )
        .with_missing_mask(ValidityMask::from_sparse(3, &[1]));

        let values = Python::attach(|py| array.bounds_unary_packed(py))
            .expect("packed bounds succeeds")
            .expect("geographic missing rows stay on the packed lane");
        assert_eq!(&values[0..4], &[170.0, 10.0, -170.0, 20.0]);
        assert!(values[4..8].iter().all(|value| value.is_nan()));
        assert_eq!(&values[8..12], &[1.0, 2.0, 3.0, 4.0]);
    }

    #[test]
    fn geographic_nary_overlays_ignore_missing_rows_before_graph_assembly() {
        crate::test_support::initialize_python();
        let frame = Frame::new(Some(crs_arc_static("OGC:CRS84")), None).expect("valid test frame");
        let left = polygon(&[
            (170.0, 0.0),
            (-170.0, 0.0),
            (-170.0, 20.0),
            (170.0, 20.0),
            (170.0, 0.0),
        ]);
        let right = polygon(&[
            (175.0, 10.0),
            (-175.0, 10.0),
            (-175.0, 30.0),
            (175.0, 30.0),
            (175.0, 10.0),
        ]);
        let present = PyGeometryArray::from_shapes(vec![left, right], frame);
        let masked = PyGeometryArray::scatter_present_rows(
            &present,
            ValidityMask::from_sparse(3, &[1]).expect("one row is missing"),
        );

        Python::attach(|py| {
            for _ in 0..8 {
                for kernel in [
                    Shape::intersection_all_topo,
                    Shape::symmetric_difference_all_topo,
                ] {
                    let expected = present
                        .apply_overlay_all(py, kernel)
                        .expect("present aggregate succeeds");
                    let actual = masked
                        .apply_overlay_all(py, kernel)
                        .expect("masked aggregate succeeds");
                    assert_eq!(
                        crate::io::to_wkb(expected.0.shape.shape(), None, false)
                            .expect("expected WKB"),
                        crate::io::to_wkb(actual.0.shape.shape(), None, false).expect("actual WKB"),
                    );
                }
            }
        });
    }
}
