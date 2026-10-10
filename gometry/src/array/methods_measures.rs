use pyo3::types::PyDict;

use crate::array::{
    Bound, DistanceUnit, GeometryError, IntoPyObject as _, Py, PyAny, PyAnyMethods as _,
    PyDictMethods as _, PyErr, PyGeometryArray, PyResult, PyTupleMethods as _, PyTypeError,
    PyTypeMethods as _, Python, Shape, SpatialCurve, crs, crs_label, pymethods, resolve_metric,
};
use crate::dispatch::{MetricOp, kernels};
use crate::geometry::storage;
use crate::geometry::storage::kernels::{
    bounds_values_from_columns_masked, geodesic_line_measure, geodesic_polygon_measure,
    geographic_bounds_values_from_columns, geographic_bounds_values_from_columns_masked,
    reduce_lines_or_polygons, segmented_planar_lengths, segmented_planar_lengths_3d,
};
use crate::geometry::storage::packed_columns::{PackedColumnError, PackedColumns, XyColumns};
use crate::geometry::storage::{GeometryArrayStorage, PackedStorage};
use crate::py::errors;
use crate::py::numpy::float64_array;

fn coordinate_payload_len(array: &PyGeometryArray) -> Option<usize> {
    match array.storage() {
        GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map }) => {
            Some(if array.is_compact_nullable() {
                coords.len()
            } else {
                row_map.as_deref().len(coords.len())
            })
        },
        GeometryArrayStorage::Packed(PackedStorage::Lines {
            offsets, row_map, ..
        }) => Some(storage::packed_lines_coord_len(
            offsets.as_slice(),
            row_map.as_deref(),
        )),
        GeometryArrayStorage::Packed(PackedStorage::Polygons {
            ring_offsets,
            polygon_offsets,
            row_map,
            ..
        }) => Some(storage::packed_polygons_coord_len(
            ring_offsets.as_slice(),
            polygon_offsets.as_slice(),
            row_map.as_deref(),
        )),
        GeometryArrayStorage::Mixed(_) => None,
    }
}

fn ring_payload_len(array: &PyGeometryArray) -> Option<usize> {
    match array.storage() {
        GeometryArrayStorage::Packed(PackedStorage::Polygons {
            polygon_offsets,
            row_map,
            ..
        }) => Some(storage::packed_polygons_ring_len(
            polygon_offsets.as_slice(),
            row_map.as_deref(),
        )),
        GeometryArrayStorage::Packed(
            PackedStorage::Points { .. } | PackedStorage::Lines { .. },
        )
        | GeometryArrayStorage::Mixed(_) => None,
    }
}

#[pymethods]
impl PyGeometryArray {
    /// Concatenate one or more arrays sharing this array's CRS and epoch.
    ///
    /// Parameters
    /// ----------
    /// others : GeometryArray
    ///     Arrays to append in order; each must share this array's CRS and
    ///     epoch. The zero-argument form returns this array.
    ///
    /// Returns
    /// -------
    /// GeometryArray
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> a = gm.GeometryArray([gm.box(0, 0, 1, 1)])
    /// >>> a.concat(gm.GeometryArray([gm.Point(2, 2)])).to_wkt()
    /// ['POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))', 'POINT (2 2)']
    #[pyo3(signature = (*others))]
    pub fn concat(&self, others: &Bound<'_, pyo3::types::PyTuple>) -> PyResult<Self> {
        if others.is_empty() {
            return Ok(self.clone());
        }
        let mut borrowed = Vec::with_capacity(others.len());
        for (offset, item) in others.iter().enumerate() {
            let index = offset.wrapping_add(1);
            let other = item.extract::<pyo3::PyRef<'_, Self>>().map_err(|_| {
                pyo3::exceptions::PyTypeError::new_err(format!(
                    "concat arguments must be GeometryArray, got {}",
                    item.get_type()
                        .name()
                        .map_or_else(|_| "<unknown>".to_owned(), |name| name.to_string(),)
                ))
            })?;
            self.ensure_concat_frame(&other, index)?;
            borrowed.push(other);
        }
        self.concat_admitted(borrowed.iter().map(|array| &**array))
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
    /// Per-row area, measured for the array's CRS.
    ///
    /// A geographic CRS gives ellipsoidal square meters for the region bounded
    /// by edges straight in longitude/latitude; a projected CRS gives squared
    /// native linear units; a CRS-free array gives squared raw coordinate units.
    /// Use ``to_crs`` to change frame.
    ///
    /// Each ``GeometryCollection`` row counts overlapping members once, as
    /// the area of their union. Parts of a valid ``MultiPolygon`` cannot
    /// overlap.
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
    /// A geographic CRS gives ellipsoidal meters along edges straight in
    /// longitude/latitude; a projected CRS gives native linear units; a CRS-free
    /// array gives raw coordinate units. Use ``to_crs`` to change frame.
    ///
    /// Geographic polygon perimeter measures straight chart edges and resolves
    /// seam aliases in the CRS-native periodic longitude frame.
    ///
    /// Each ``GeometryCollection`` row sums every member's length or
    /// perimeter, including overlapping members.
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
    /// Geographic CRSs do not support 3D Euclidean length.
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
            return float64_array(py, values);
        }
        crate::dispatch::unary_array(
            py,
            self,
            crate::dispatch::Operation::Metric(MetricOp::Length3d),
            None,
            None,
            kernels::unary_length_3d,
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
    ///     Ordered ``(minx, miny, maxx, maxy)`` rectangle for keying. When
    ///     omitted, uses the present rows' minimum and maximum stored
    ///     coordinates without periodic wrapping. Geographic ``total_bounds``
    ///     may wrap, so it is not necessarily a curve frame.
    ///
    /// Returns
    /// -------
    /// GeometryArray
    ///     A new array with the same rows in curve-key order; empty and
    ///     missing rows sort last.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> arr = gm.GeometryArray([gm.Point(1, 0), gm.Point(0, 0)])
    /// >>> arr.sort_by_spatial_key().to_wkt()
    /// ['POINT (0 0)', 'POINT (1 0)']
    #[pyo3(signature = (*, curve = SpatialCurve::Hilbert, level = 16, bounds = None), text_signature = "($self, *, curve='hilbert', level=16, bounds=None)")]
    pub fn sort_by_spatial_key(
        &self,
        curve: SpatialCurve,
        #[pyo3(from_py_with = crate::strict_i64)] level: i64,
        bounds: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let keys = self.curve_keys(level, bounds, curve.into())?;
        Ok(self.taken_by_keys(&keys)?)
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
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> arr = gm.GeometryArray([gm.box(0, 0, 1, 1), gm.box(1, 0, 2, 1)])
    /// >>> geoms, keys = arr.dissolve(by=[0, 0])
    /// >>> geoms.to_wkt()
    /// ['POLYGON ((0 0, 2 0, 2 1, 0 1, 0 0))']
    #[pyo3(signature = (by))]
    #[expect(
        clippy::indexing_slicing,
        reason = "group indices are created with each group and published only after both parallel vectors are extended"
    )]
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
        let strictness = crate::geometry::Strictness::Lenient;
        let mut groups: Vec<Py<PyAny>> = Vec::new();
        let mut group_shapes: Vec<Vec<Shape>> = Vec::new();
        let hash_groups = PyDict::new(py);
        let mut unhashable_groups: Vec<usize> = Vec::new();
        for (key, row) in keys
            .iter()
            .zip(self.row_operation().rows_with_missing(|| ()))
        {
            let crate::array::ArrayRow::Present(row) = row else {
                continue;
            };
            let shape = row.row().with_shape(Clone::clone).map_err(PyErr::from)?;
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
                group_shapes[index].push(shape);
            } else {
                let index = groups.len();
                groups.push(key.clone().unbind());
                group_shapes.push(vec![shape]);
                if hashable {
                    hash_groups.set_item(key, index)?;
                } else {
                    unhashable_groups.push(index);
                }
            }
        }
        let frame = self.frame.clone();
        let mut metric_scratch = crate::dispatch::MetricScratch::default();
        let (domain, metric) = crate::dispatch::MetricResolver::GeographicCarry
            .resolve_topology_ctx(&frame, &mut metric_scratch)?;
        // One detach around the whole loop: the union of every group needs no
        // Python, and a GIL round-trip per group was the loop's fixed cost.
        let shapes: Vec<Shape> = py.detach(move || {
            let metric_access = || {
                metric.geographic_chart_metric()?.ok_or_else(|| {
                    crate::crs::CrsError::invalid(
                        "geographic dissolve source ordinates require the operation's CRS metric",
                    )
                })
            };
            group_shapes
                .into_iter()
                .map(|group| {
                    crate::geometry::overlay_all_with_metric(
                        &group,
                        domain,
                        crate::geometry::OverlayOp::Union,
                        strictness,
                        &metric_access,
                    )
                })
                .collect::<Result<_, _>>()
        })?;
        drop(metric_scratch);
        Ok((Self::from_shapes(shapes, frame)?, groups)
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
    /// Fused planar polygon reducer: every row is measured as stored, like the
    /// per-row kernel ([`crate::dispatch::kernels::unary_area`]).
    pub(crate) fn area_unary_packed(
        &self,
        py: Python<'_>,
        unit: Option<DistanceUnit>,
    ) -> PyResult<Option<Vec<f64>>> {
        if !matches!(
            self.storage(),
            GeometryArrayStorage::Packed(PackedStorage::Polygons { .. })
        ) || matches!(
            crate::geometry::topology_domain(&self.frame)?,
            crate::geometry::TopologyDomain::Geographic(_)
        ) {
            return Ok(None);
        }
        let crs::MetricModel::Planar { to_metre } =
            resolve_metric(self.crs_str(), unit, MetricOp::Area)?
        else {
            return Ok(None);
        };
        let scale = to_metre.get() * to_metre.get();
        self.reduce_packed_columns_detached(py, move |columns| {
            let PackedColumns::Polygons(polygon_columns) = columns else {
                unreachable!("area packed lane requires polygon storage");
            };
            let XyColumns { xs, ys } = polygon_columns.physical_xy();
            let ring_offsets = polygon_columns.ring_offsets();
            let mut values = Vec::with_capacity(polygon_columns.rows());
            for row in 0..polygon_columns.rows() {
                let area = crate::geometry::polygon_area_measure_columns(
                    xs,
                    ys,
                    ring_offsets,
                    polygon_columns.polygon_rings(row),
                );
                values.push(area * scale);
            }
            Ok(values)
        })
    }

    pub(crate) fn length_unary_packed(
        &self,
        py: Python<'_>,
        unit: Option<DistanceUnit>,
        op: MetricOp,
    ) -> PyResult<Option<Vec<f64>>> {
        if !matches!(
            self.storage(),
            GeometryArrayStorage::Packed(
                PackedStorage::Lines { .. } | PackedStorage::Polygons { .. }
            )
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
                    Ok(values)
                })
            },
            model @ crs::MetricModel::Geodesic { .. } => {
                let crs::ResolvedMetric::Geodesic(metric) =
                    crs::ResolvedMetric::from_model(&model)?
                else {
                    unreachable!("geodesic model resolves to a geodesic metric");
                };
                let frame = metric.frame;
                let cached_bounds = self.existing_element_bounds();
                self.reduce_packed_columns_detached(py, move |columns| {
                    let values = match columns {
                        PackedColumns::Lines(line_columns) => {
                            geodesic_line_measure(&line_columns, frame, |xs, ys| {
                                crate::crs::chart_line_length_columns(&metric, xs, ys)
                            })?
                        },
                        PackedColumns::Polygons(polygon_columns) => geodesic_polygon_measure(
                            &polygon_columns,
                            frame,
                            &metric,
                            cached_bounds.as_deref().map(Vec::as_slice),
                        )?,
                        PackedColumns::Points(_) => {
                            unreachable!("length packed lane requires line or polygon storage")
                        },
                    };
                    Ok(values)
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
        if let GeometryArrayStorage::Packed(PackedStorage::Points { .. }) = self.storage() {
            return Ok(Some(vec![0.0; self.len()]));
        }
        if !matches!(
            self.storage(),
            GeometryArrayStorage::Packed(
                PackedStorage::Lines { .. } | PackedStorage::Polygons { .. }
            )
        ) {
            return Ok(None);
        }
        let missing = self.missing().cloned();
        self.reduce_packed_columns_detached(py, move |columns| {
            let values = reduce_lines_or_polygons(
                columns,
                |line_columns| {
                    let Some(zs) = line_columns.coords().zs() else {
                        return vec![f64::NAN; line_columns.rows()];
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
                        return vec![f64::NAN; polygon_columns.rows()];
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
            Ok(values)
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
                        geographic_bounds_values_from_columns_masked(&columns, frame, Some(missing))
                    },
                )
            } else {
                Ok(bounds_values_from_columns_masked(
                    &columns,
                    missing.as_ref(),
                ))
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
            return Err(errors::crs_mismatch_error(
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
            return Err(errors::epoch_mismatch_error(
                "concat requires a shared coordinate epoch",
                self.epoch(),
                other.epoch(),
                Some(index),
            ));
        }
        Ok(())
    }

    // The public argument loop admits frames in order so an earlier frame
    // mismatch wins over a later argument's type error.
    fn concat_admitted<'a>(
        &'a self,
        others: impl Iterator<Item = &'a Self> + Clone,
    ) -> PyResult<Self> {
        let arrays = || std::iter::once(self).chain(others.clone());
        let total_rows = arrays().map(Self::len).fold(0_usize, usize::wrapping_add);
        let coordinate_capacity = arrays()
            .filter_map(coordinate_payload_len)
            .fold(0_usize, usize::wrapping_add);
        let ring_capacity = arrays()
            .filter_map(ring_payload_len)
            .fold(0_usize, usize::wrapping_add);
        let mut builder = crate::array::StreamingShapes::with_capacity_and_rings(
            coordinate_capacity,
            total_rows,
            ring_capacity,
        );
        for array in std::iter::once(self).chain(others) {
            if builder.try_extend_array(array).map_err(PyErr::from)? {
                continue;
            }
            for row in array.row_operation().rows_with_missing(|| ()) {
                match row {
                    crate::array::ArrayRow::Missing { .. } => {
                        builder.try_push_missing().map_err(PyErr::from)?;
                    },
                    crate::array::ArrayRow::Present(row) => {
                        builder.try_push_row(Some(row.row())).map_err(PyErr::from)?;
                    },
                }
            }
        }
        builder.finish(self.frame.clone()).map_err(PyErr::from)
    }
}

#[cfg(test)]
mod geographic_masked_bounds_tests {
    use super::*;
    use crate::crs::frame::{Frame, crs_arc_static};
    use crate::geometry::storage::ValidityMask;
    use crate::geometry::{CoordSeq, LineSeq, Point, Polygon, Ring};

    fn line(points: &[(f64, f64)]) -> Shape {
        Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_points(
                    &points
                        .iter()
                        .map(|&(x, y)| Point::new_raw_xy(x, y))
                        .collect::<Vec<_>>(),
                )
                .unwrap(),
            )
            .expect("line coordinate sequence must contain at least two vertices"),
        )
    }

    fn polygon(points: &[(f64, f64)]) -> Shape {
        Shape::Polygon(Polygon::new(
            Ring::closed(
                points
                    .iter()
                    .map(|&(x, y)| Point::new_raw_xy(x, y))
                    .collect(),
            )
            .unwrap(),
            Vec::new(),
        ))
    }

    #[test]
    fn packed_geodesic_length_applies_rule_t_and_uses_published_bounds() {
        crate::test_support::initialize_python();
        let ordinary = polygon(&[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0)]);
        let cap = polygon(&[
            (-180.0, 80.0),
            (-90.0, 80.0),
            (0.0, 80.0),
            (90.0, 80.0),
            (180.0, 80.0),
            (180.0, 90.0),
            (-180.0, 90.0),
        ]);
        let array = PyGeometryArray::from_shapes(
            vec![ordinary, cap],
            Frame::new(Some(crs_arc_static("EPSG:4326")), None).expect("valid test frame"),
        )
        .unwrap();
        assert!(matches!(
            array.storage(),
            GeometryArrayStorage::Packed(PackedStorage::Polygons { .. })
        ));

        let cold = Python::attach(|py| {
            crate::length_natural_array(py, &array)
                .and_then(|result| result.bind(py).extract::<Vec<f64>>())
                .expect("packed geographic length succeeds")
        });
        assert_eq!(cold.len(), 2);
        assert!((cold[1] - 6_981_655.0).abs() < 1.0);

        assert!(array.element_bounds().is_some());
        let warm = Python::attach(|py| {
            crate::length_natural_array(py, &array)
                .and_then(|result| result.bind(py).extract::<Vec<f64>>())
                .expect("cached-bound geographic length succeeds")
        });
        assert_eq!(
            warm.iter().map(|value| value.to_bits()).collect::<Vec<_>>(),
            cold.iter().map(|value| value.to_bits()).collect::<Vec<_>>()
        );
    }

    #[test]
    fn packed_geodesic_length_uses_the_shared_ambiguous_pole_gate() {
        crate::test_support::initialize_python();
        let ambiguous = polygon(&[
            (-180.0, 20.0),
            (-90.0, -20.0),
            (0.0, 20.0),
            (90.0, -20.0),
            (180.0, 20.0),
        ]);
        let array = PyGeometryArray::from_shapes(
            vec![ambiguous],
            Frame::new(Some(crs_arc_static("EPSG:4326")), None).expect("valid test frame"),
        )
        .unwrap();
        let error = Python::attach(|py| crate::length_natural_array(py, &array)).unwrap_err();
        assert!(
            error.to_string().to_lowercase().contains("ambiguous"),
            "unexpected packed full-turn error: {error}"
        );
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
        .unwrap()
        .with_missing_mask(ValidityMask::from_sparse(3, &[1]))
        .unwrap();

        let values = Python::attach(|py| array.bounds_unary_packed(py))
            .expect("packed bounds succeeds")
            .expect("geographic missing rows stay on the packed lane");
        assert_eq!(&values[0..4], &[170.0, 10.0, -170.0, 20.0]);
        assert!(values[4..8].iter().all(|value| value.is_nan()));
        assert_eq!(&values[8..12], &[1.0, 2.0, 3.0, 4.0]);
    }

    #[test]
    fn packed_bounds_keep_narrow_noncanonical_aliases_on_the_crossing_path() {
        crate::test_support::initialize_python();
        let shape = polygon(&[
            (179.0, -10.0),
            (181.0, -10.0),
            (181.0, 10.0),
            (179.0, 10.0),
            (179.0, -10.0),
        ]);
        let frame = Frame::new(Some(crs_arc_static("OGC:CRS84")), None).expect("valid test frame");
        let array = PyGeometryArray::from_shapes(vec![shape.clone()], frame).unwrap();
        let values = Python::attach(|py| array.bounds_unary_packed(py))
            .expect("packed bounds succeeds")
            .expect("polygon array stays packed");
        let expected = crate::geometry::geographic_crossing_bounds(&shape)
            .expect("crossing polygon has bounds")
            .into_array();
        assert_eq!(values, expected);
    }

    #[test]
    fn packed_centroid_keeps_narrow_noncanonical_aliases_on_the_crossing_path() {
        crate::test_support::initialize_python();
        let shape = polygon(&[
            (179.0, -10.0),
            (181.0, -10.0),
            (181.0, 10.0),
            (179.0, 10.0),
            (179.0, -10.0),
        ]);
        let frame = Frame::new(Some(crs_arc_static("OGC:CRS84")), None).expect("valid test frame");
        let array = PyGeometryArray::from_shapes(vec![shape.clone()], frame).unwrap();
        let expected =
            crate::geometry::unary_antimeridian_derived(
                &shape,
                crate::geometry::TopologyDomain::Geographic(
                    crate::geographic::GeographicFrame::crs84(),
                ),
                crate::geometry::DerivedPointStrategy::Centroid,
                Shape::centroid,
            )
            .unwrap();

        let result = array
            .centroid_unary_packed()
            .expect("packed centroid succeeds");
        let actual = result
            .storage()
            .iter_shapes()
            .next()
            .expect("one centroid row")
            .expect("centroid row shape");
        assert_eq!(actual.as_ref(), &expected);
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
        let present = PyGeometryArray::from_shapes(vec![left, right], frame).unwrap();
        let masked = PyGeometryArray::scatter_present_rows(
            &present,
            ValidityMask::from_sparse(3, &[1]).expect("one row is missing"),
        )
        .unwrap();

        Python::attach(|_| {
            for _ in 0..8 {
                for kernel in [
                    Shape::intersection_all_topo,
                    Shape::symmetric_difference_all_topo,
                ] {
                    let aggregate = |array: &PyGeometryArray| {
                        let domain = crate::geometry::topology_domain(&array.frame).unwrap();
                        kernel(
                            &array.present_shapes().unwrap(),
                            domain,
                            crate::geometry::Strictness::Lenient,
                        )
                        .expect("aggregate succeeds")
                    };
                    assert_eq!(
                        crate::io::to_wkb(&aggregate(&present), None, false).expect("expected WKB"),
                        crate::io::to_wkb(&aggregate(&masked), None, false).expect("actual WKB"),
                    );
                }
            }
        });
    }
}

#[cfg(test)]
mod concat_bulk_tests {
    use super::*;
    use crate::array::streaming::{
        reset_stream_bulk_extensions, reset_stream_row_advances, stream_bulk_extensions,
        stream_bulk_rows, stream_row_advances,
    };
    use crate::crs::frame::Frame;
    use crate::geometry::storage::ValidityMask;
    use crate::geometry::{CoordSeq, LineSeq, Point, Polygon, Ring};

    fn line(start: f64) -> Shape {
        Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    Point::new_raw_xy(start, start + 0.25),
                    Point::new_raw_xy(start + 1.0, start + 1.25),
                ])
                .unwrap(),
            )
            .unwrap(),
        )
    }

    fn polygon(start: f64) -> Shape {
        let coords = CoordSeq::try_from_points(&[
            Point::new_raw_xy(start, 0.0),
            Point::new_raw_xy(start + 1.0, 0.0),
            Point::new_raw_xy(start + 1.0, 1.0),
            Point::new_raw_xy(start, 1.0),
            Point::new_raw_xy(start, 0.0),
        ])
        .unwrap();
        Shape::Polygon(Polygon::new(Ring::from_closed(coords).unwrap(), Vec::new()))
    }

    fn polygon_with_hole(start: f64) -> Shape {
        let shell = CoordSeq::try_from_points(&[
            Point::new_raw_xy(start, 0.0),
            Point::new_raw_xy(start + 1.0, 0.0),
            Point::new_raw_xy(start + 1.0, 1.0),
            Point::new_raw_xy(start, 1.0),
            Point::new_raw_xy(start, 0.0),
        ])
        .unwrap();
        let hole = CoordSeq::try_from_points(&[
            Point::new_raw_xy(start + 0.25, 0.25),
            Point::new_raw_xy(start + 0.75, 0.25),
            Point::new_raw_xy(start + 0.75, 0.75),
            Point::new_raw_xy(start + 0.25, 0.75),
            Point::new_raw_xy(start + 0.25, 0.25),
        ])
        .unwrap();
        Shape::Polygon(Polygon::new(Ring::from_closed(shell).unwrap(), vec![
            Ring::from_closed(hole).unwrap(),
        ]))
    }

    fn points() -> PyGeometryArray {
        PyGeometryArray::from_shapes(
            vec![
                Shape::Point(Point::new_raw_xy(10.0, 10.25)),
                Shape::Point(Point::new_raw_xy(20.0, 20.25)),
                Shape::Point(Point::new_raw_xy(30.0, 30.25)),
            ],
            Frame::None,
        )
        .unwrap()
    }

    fn lines() -> PyGeometryArray {
        PyGeometryArray::from_shapes(vec![line(0.0), line(10.0), line(20.0)], Frame::None).unwrap()
    }

    fn polygons() -> PyGeometryArray {
        PyGeometryArray::from_shapes(
            vec![polygon(0.0), polygon_with_hole(10.0), polygon(20.0)],
            Frame::None,
        )
        .unwrap()
    }

    fn reset_bulk_observations() {
        reset_stream_bulk_extensions();
        reset_stream_row_advances();
        storage::reset_shape_iter_materializations();
    }

    fn assert_bulk_counts() {
        assert_eq!(stream_bulk_extensions(), 2);
        assert_eq!(stream_bulk_rows(), 5);
        assert_eq!(stream_row_advances(), 0);
        assert_eq!(storage::shape_iter_materializations(), 0);
    }

    #[test]
    fn dense_identity_and_window_concat_use_native_bulk_spans() {
        let points = points();
        let point_window = points.gather_logical_row_range(1..3).unwrap();
        reset_bulk_observations();
        let merged = points
            .concat_admitted(std::iter::once(&point_window))
            .unwrap();
        assert_bulk_counts();
        let GeometryArrayStorage::Packed(PackedStorage::Points { coords, .. }) = merged.storage()
        else {
            panic!("dense point concat must remain packed");
        };
        assert_eq!(coords.xs(), &[10.0, 20.0, 30.0, 20.0, 30.0]);

        let lines = lines();
        let line_window = lines.gather_logical_row_range(1..3).unwrap();
        reset_bulk_observations();
        let merged = lines
            .concat_admitted(std::iter::once(&line_window))
            .unwrap();
        assert_bulk_counts();
        let GeometryArrayStorage::Packed(PackedStorage::Lines {
            coords, offsets, ..
        }) = merged.storage()
        else {
            panic!("dense line concat must remain packed");
        };
        assert_eq!(coords.xs(), &[
            0.0, 1.0, 10.0, 11.0, 20.0, 21.0, 10.0, 11.0, 20.0, 21.0
        ]);
        assert_eq!(offsets.as_slice(), &[0, 2, 4, 6, 8, 10]);

        let polygons = polygons();
        let polygon_window = polygons.gather_logical_row_range(1..3).unwrap();
        reset_bulk_observations();
        let merged = polygons
            .concat_admitted(std::iter::once(&polygon_window))
            .unwrap();
        assert_bulk_counts();
        let GeometryArrayStorage::Packed(PackedStorage::Polygons {
            coords,
            ring_offsets,
            polygon_offsets,
            ..
        }) = merged.storage()
        else {
            panic!("dense polygon concat must remain packed");
        };
        assert_eq!(coords.len(), 35);
        assert_eq!(ring_offsets.as_slice(), &[0, 5, 10, 15, 20, 25, 30, 35]);
        assert_eq!(polygon_offsets.as_slice(), &[0, 1, 3, 4, 6, 7]);
    }

    #[test]
    fn empty_identity_and_window_arrays_do_not_select_a_stream_kind() {
        let empty_points = PyGeometryArray::packed_points(
            CoordSeq::try_from_vecs(Vec::new(), Vec::new(), None, None).unwrap(),
            Frame::None,
        )
        .unwrap();
        let lines = lines();
        let empty_lines = lines.gather_logical_row_range(0..0).unwrap();
        reset_bulk_observations();
        let mut builder = crate::array::StreamingShapes::new();
        assert!(builder.try_extend_array(&empty_points).unwrap());
        assert!(!builder.try_extend_array(&empty_lines).unwrap());
        assert_eq!(empty_lines.logical_rows().count(), 0);
        assert!(builder.try_extend_array(&lines).unwrap());
        let result = builder.finish(Frame::None).unwrap();
        assert!(matches!(
            result.storage(),
            GeometryArrayStorage::Packed(PackedStorage::Lines { .. })
        ));
        assert_eq!(stream_bulk_extensions(), 1);
        assert_eq!(stream_bulk_rows(), 3);
        assert_eq!(stream_row_advances(), 0);
    }

    #[test]
    fn incompatible_bulk_extension_allocates_nothing_before_mixed_fallback() {
        let points = points();
        let lines = lines();
        reset_bulk_observations();
        let mut builder = crate::array::StreamingShapes::with_capacity(0, 2);
        assert!(builder.try_extend_array(&points).unwrap());
        assert!(
            !crate::test_support::assert_no_allocations(|| { builder.try_extend_array(&lines) })
                .unwrap()
        );
        for row in lines.logical_rows() {
            match row {
                crate::array::LogicalRow::Missing { .. } => {
                    builder.try_push_missing().unwrap();
                },
                crate::array::LogicalRow::Present { row, .. } => {
                    builder.try_push_row(Some(row)).unwrap();
                },
            }
        }
        let result = builder.finish(Frame::None).unwrap();
        assert!(matches!(result.storage(), GeometryArrayStorage::Mixed(_)));
        assert_eq!(result.len(), points.len() + lines.len());
        assert_eq!(stream_bulk_extensions(), 1);
        assert_eq!(stream_row_advances(), 3);
        assert_eq!(storage::shape_iter_materializations(), 0);

        let xyz = PyGeometryArray::from_shapes(
            vec![Shape::Point(Point::new_raw_axes(
                40.0,
                40.25,
                crate::geometry::ZOrdinate(Some(1.0)),
                crate::geometry::MOrdinate(None),
            ))],
            Frame::None,
        )
        .unwrap();
        reset_bulk_observations();
        let mut builder = crate::array::StreamingShapes::with_capacity(0, 2);
        assert!(builder.try_extend_array(&points).unwrap());
        assert!(
            !crate::test_support::assert_no_allocations(|| { builder.try_extend_array(&xyz) })
                .unwrap()
        );
        for row in xyz.logical_rows() {
            let crate::array::LogicalRow::Present { row, .. } = row else {
                unreachable!("dense xyz fixture has no missing rows");
            };
            builder.try_push_row(Some(row)).unwrap();
        }
        let result = builder.finish(Frame::None).unwrap();
        assert!(matches!(result.storage(), GeometryArrayStorage::Mixed(_)));
        assert_eq!(result.len(), points.len() + xyz.len());
    }

    #[test]
    fn gathered_duplicate_concat_uses_row_fallback_with_exact_payload_capacity() {
        let lines = lines();
        let gathered = lines.gather_logical_rows(&[2, 0, 2]).unwrap();
        reset_bulk_observations();
        let merged = gathered.concat_admitted(std::iter::empty()).unwrap();
        assert_eq!(stream_bulk_extensions(), 0);
        assert_eq!(stream_bulk_rows(), 0);
        assert_eq!(stream_row_advances(), 3);
        assert_eq!(storage::shape_iter_materializations(), 0);
        let GeometryArrayStorage::Packed(PackedStorage::Lines {
            coords, offsets, ..
        }) = merged.storage()
        else {
            panic!("gathered duplicate lines must remain packed");
        };
        assert_eq!(coords.xs(), &[20.0, 21.0, 0.0, 1.0, 20.0, 21.0]);
        assert_eq!(offsets.as_slice(), &[0, 2, 4, 6]);
        assert!(coords.column_arcs().xs.capacity() >= 6);
    }

    #[test]
    fn nullable_concat_uses_logical_row_fallback() {
        let nullable = points()
            .with_missing_mask(Some(
                ValidityMask::from_vec(3, vec![false, true, false]).unwrap(),
            ))
            .unwrap();
        reset_bulk_observations();
        let merged = nullable.concat_admitted(std::iter::empty()).unwrap();
        assert_eq!(stream_bulk_extensions(), 0);
        assert_eq!(stream_bulk_rows(), 0);
        assert_eq!(stream_row_advances(), 3);
        assert_eq!(merged.missing().unwrap().iter().collect::<Vec<_>>(), [
            false, true, false
        ]);
        let GeometryArrayStorage::Packed(PackedStorage::Points { coords, .. }) = merged.storage()
        else {
            panic!("nullable point concat must remain packed");
        };
        assert_eq!(coords.xs(), &[10.0, 30.0]);

        let dense = points();
        reset_bulk_observations();
        let merged = nullable.concat_admitted(std::iter::once(&dense)).unwrap();
        assert_eq!(stream_bulk_extensions(), 1);
        assert_eq!(stream_bulk_rows(), 3);
        assert_eq!(stream_row_advances(), 3);
        assert_eq!(merged.missing().unwrap().iter().collect::<Vec<_>>(), [
            false, true, false, false, false, false
        ]);
    }
}
