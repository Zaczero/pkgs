use super::adapters::{
    geometry_point, input_len, point_operand, reject_rhumb_unit, require_geographic_model, row_zero,
};
use super::kernels::{
    destination_point, destination_point_on_geodesic, destination_point_planar,
    rhumb_destination_point,
};
use super::rows::{
    finish_point_nav_coords, point_from_point_nav_shape, point_nav_missing_point_with_axes,
    row_missing, uniform_point_axes_dispatch,
};
use super::{
    Bound, CoordSeq, CoordSeqBuilder, CoordinateAxes, CoordinateInput, DistanceUnit, GeometryInput,
    InvalidGeometryError, LogicalRowIndex, MetricOp, NavigationPath, Point, PointRows, Py, PyAny,
    PyErr, PyGeometry, PyGeometryArray, PyResult, Python, Shape, ValidityMask,
    broadcast_coordinate_group, classify_input, classify_required, coordinate_sequence_len_hint,
    crs, expected_geometry_or_array, parameter_input_with_expected, resolve_metric,
};
use crate::IntoPyObject as _;

fn rhumb_destination_scalar_coords(
    metric: &crs::GeodesicMetric,
    point: Point,
    bearings: &[f64],
    distances: &[f64],
    axes: CoordinateAxes,
) -> PyResult<CoordSeq> {
    let mut builder = CoordSeqBuilder::with_capacity(axes, bearings.len());
    for row in destination_rows(bearings, distances, bearings.len()) {
        let shape = rhumb_destination_point(
            &metric.crs,
            metric.frame,
            point,
            row.bearing,
            row.distance,
            &metric.state,
        )
        .map_err(|err| crate::note_array_row(err, row.logical.get()))?;
        builder.push(point_from_point_nav_shape(&shape));
    }
    Ok(builder.finish()?)
}

/// Destinations from POINT EMPTY along many bearings: one empty point each.
fn empty_destinations(py: Python<'_>, geometry: &PyGeometry, len: usize) -> PyResult<Py<PyAny>> {
    let mut shapes = Vec::with_capacity(len);
    shapes.resize_with(len, Shape::empty_point);
    Ok(
        PyGeometryArray::from_shapes(shapes, geometry.frame.clone())?
            .into_pyobject(py)?
            .unbind()
            .into(),
    )
}

fn point_rhumb_destination(geometry: &PyGeometry, bearing: f64, distance: f64) -> PyResult<Shape> {
    const OP: MetricOp = MetricOp::RhumbDestination;
    let from = point_operand(geometry, OP)?;
    let model = require_geographic_model(geometry.crs_str(), OP)?;
    let Some(from) = from else {
        return Ok(Shape::empty_point());
    };
    let kernel = RhumbDestinationKernel {
        bearing: &[bearing],
        distance: &[distance],
    };
    point_unary_geometry_eval(kernel, &model, kernel.first_row(), from)
}

fn point_destination(
    geometry: &PyGeometry,
    bearing: f64,
    distance: f64,
    unit: Option<DistanceUnit>,
) -> PyResult<Shape> {
    let from = point_operand(geometry, MetricOp::Destination)?;
    let model = resolve_metric(geometry.crs_str(), unit, MetricOp::Destination)?;
    let Some(from) = from else {
        return Ok(Shape::empty_point());
    };
    Ok(Shape::Point(destination_point(
        &model, from, bearing, distance,
    )?))
}

fn validate_destination_distances(values: &[f64]) -> PyResult<()> {
    for &distance in values {
        crate::validate_distance(distance)?;
    }
    Ok(())
}

fn broadcast_destination_param(
    input: &mut CoordinateInput,
    len: usize,
    name: &str,
) -> PyResult<()> {
    if input.values.len() == len || input.scalar {
        crate::broadcast_coordinate_input(input, len, name)
    } else {
        Err(InvalidGeometryError::new_err(format!(
            "point, bearing, and distance must have the same length, got {} and {len}",
            input.values.len()
        )))
    }
}

fn destination_parameters(
    py: Python<'_>,
    bearing: &Bound<'_, PyAny>,
    distance: &Bound<'_, PyAny>,
    receiver_len: Option<usize>,
) -> PyResult<(CoordinateInput, CoordinateInput)> {
    let mut expected = coordinate_sequence_len_hint(bearing, "bearing")?;
    if expected.is_none() {
        expected = coordinate_sequence_len_hint(distance, "distance")?;
    }
    if expected.is_none() {
        expected = receiver_len;
    }
    let bearing = parameter_input_with_expected(py, bearing, "bearing", expected, &|| {
        "bearing must be finite".into()
    })?;
    let expected = expected.or_else(|| (!bearing.scalar).then_some(bearing.values.len()));
    let distance = parameter_input_with_expected(py, distance, "distance", expected, &|| {
        "distance must be finite".into()
    })?;
    Ok((bearing, distance))
}

#[derive(Clone, Copy)]
struct DestinationRow {
    logical: LogicalRowIndex,
    bearing: f64,
    distance: f64,
}

fn destination_parameter_slices<'a>(
    bearings: &'a [f64],
    distances: &'a [f64],
    len: usize,
) -> (&'a [f64], &'a [f64]) {
    (
        bearings
            .get(..len)
            .expect("point-navigation bearings cover every logical row"),
        distances
            .get(..len)
            .expect("point-navigation distances cover every logical row"),
    )
}

fn destination_rows<'a>(
    bearings: &'a [f64],
    distances: &'a [f64],
    len: usize,
) -> impl ExactSizeIterator<Item = DestinationRow> + 'a {
    let (bearings, distances) = destination_parameter_slices(bearings, distances, len);
    bearings
        .iter()
        .copied()
        .zip(distances.iter().copied())
        .enumerate()
        .map(|(logical, (bearing, distance))| DestinationRow {
            logical: LogicalRowIndex::new(logical),
            bearing,
            distance,
        })
}

fn destination_row_at(
    bearings: &[f64],
    distances: &[f64],
    logical: LogicalRowIndex,
) -> DestinationRow {
    let row = logical.get();
    DestinationRow {
        logical,
        bearing: *bearings
            .get(row)
            .expect("point-navigation bearings cover every present row"),
        distance: *distances
            .get(row)
            .expect("point-navigation distances cover every present row"),
    }
}

trait PointUnaryGeometryKernel: Copy + Send + Sync + pyo3::marker::Ungil {
    fn parameters(&self) -> (&[f64], &[f64]);

    fn first_row(&self) -> DestinationRow {
        let (bearings, distances) = self.parameters();
        destination_rows(bearings, distances, 1)
            .next()
            .expect("scalar navigation has one parameter row")
    }

    fn eval_planar(self, _to_metre: f64, _row: DestinationRow, _point: Point) -> PyResult<Shape>;

    fn eval_geodesic(
        self,
        metric: &crs::GeodesicMetric,
        row: DestinationRow,
        point: Point,
    ) -> PyResult<Shape>;
}

#[derive(Clone, Copy)]
struct DestinationPointKernel<'a> {
    bearing: &'a [f64],
    distance: &'a [f64],
}

#[derive(Clone, Copy)]
struct RhumbDestinationKernel<'a> {
    bearing: &'a [f64],
    distance: &'a [f64],
}

impl PointUnaryGeometryKernel for RhumbDestinationKernel<'_> {
    fn parameters(&self) -> (&[f64], &[f64]) {
        (self.bearing, self.distance)
    }

    fn eval_planar(self, _to_metre: f64, _row: DestinationRow, _point: Point) -> PyResult<Shape> {
        unreachable!("rhumb setup admits only a geographic CRS")
    }

    fn eval_geodesic(
        self,
        metric: &crs::GeodesicMetric,
        row: DestinationRow,
        point: Point,
    ) -> PyResult<Shape> {
        rhumb_destination_point(
            &metric.crs,
            metric.frame,
            point,
            row.bearing,
            row.distance,
            &metric.state,
        )
    }
}

fn point_unary_geometry_eval<K: PointUnaryGeometryKernel>(
    kernel: K,
    model: &crs::MetricModel,
    row: DestinationRow,
    point: Point,
) -> PyResult<Shape> {
    match model {
        crs::MetricModel::Geodesic { .. } => {
            let metric = crs::GeodesicMetric::from_model(model)?;
            kernel.eval_geodesic(&metric, row, point)
        },
        crs::MetricModel::Planar { to_metre } => kernel.eval_planar(to_metre.get(), row, point),
    }
}

fn point_unary_geometry_coords<const HAS_MISSING: bool, K: PointUnaryGeometryKernel>(
    kernel: K,
    points: &PointRows<'_>,
    len: usize,
    axes: CoordinateAxes,
    missing: Option<&ValidityMask>,
    mut evaluate: impl FnMut(DestinationRow, Point) -> PyResult<Shape>,
) -> PyResult<CoordSeq> {
    let mut builder = CoordSeqBuilder::with_capacity(axes, len);
    let (bearings, distances) = kernel.parameters();
    for row in destination_rows(bearings, distances, len) {
        let logical = row.logical.get();
        if row_missing::<HAS_MISSING>(missing, logical) {
            builder.push(point_nav_missing_point_with_axes(axes));
        } else {
            let shape = evaluate(row, points.get(logical))
                .map_err(|err| crate::note_array_row(err, logical))?;
            builder.push(point_from_point_nav_shape(&shape));
        }
    }
    finish_point_nav_coords(builder, missing)
}

fn point_unary_geometry_coords_dispatch<K: PointUnaryGeometryKernel>(
    kernel: K,
    model: &crs::MetricModel,
    points: &PointRows<'_>,
    len: usize,
    axes: CoordinateAxes,
    missing: Option<&ValidityMask>,
) -> PyResult<CoordSeq> {
    match (model, missing) {
        (crs::MetricModel::Geodesic { .. }, Some(mask)) => {
            let metric = crs::GeodesicMetric::from_model(model)?;
            point_unary_geometry_coords::<true, _>(
                kernel,
                points,
                len,
                axes,
                Some(mask),
                |row, point| kernel.eval_geodesic(&metric, row, point),
            )
        },
        (crs::MetricModel::Geodesic { .. }, None) => {
            let metric = crs::GeodesicMetric::from_model(model)?;
            point_unary_geometry_coords::<false, _>(
                kernel,
                points,
                len,
                axes,
                None,
                |row, point| kernel.eval_geodesic(&metric, row, point),
            )
        },
        (crs::MetricModel::Planar { to_metre }, Some(mask)) => {
            point_unary_geometry_coords::<true, _>(
                kernel,
                points,
                len,
                axes,
                Some(mask),
                |row, point| kernel.eval_planar(to_metre.get(), row, point),
            )
        },
        (crs::MetricModel::Planar { to_metre }, None) => point_unary_geometry_coords::<false, _>(
            kernel,
            points,
            len,
            axes,
            None,
            |row, point| kernel.eval_planar(to_metre.get(), row, point),
        ),
    }
}

fn destination_point_coords<const HAS_MISSING: bool>(
    kernel: DestinationPointKernel<'_>,
    len: usize,
    axes: CoordinateAxes,
    missing: Option<&ValidityMask>,
    mut point_at: impl FnMut(usize) -> Point,
    mut evaluate: impl FnMut(Point, DestinationRow) -> PyResult<Point>,
) -> PyResult<CoordSeq> {
    let mut builder = CoordSeqBuilder::with_capacity(axes, len);
    for row in destination_rows(kernel.bearing, kernel.distance, len) {
        let logical = row.logical.get();
        if row_missing::<HAS_MISSING>(missing, logical) {
            builder.push(point_nav_missing_point_with_axes(axes));
        } else {
            builder.try_push(
                evaluate(point_at(logical), row).map_err(|err| crate::note_array_row(err, logical)),
            )?;
        }
    }
    finish_point_nav_coords(builder, missing)
}

fn destination_coords_dispatch<const HAS_MISSING: bool>(
    kernel: DestinationPointKernel<'_>,
    model: &crs::MetricModel,
    len: usize,
    axes: CoordinateAxes,
    missing: Option<&ValidityMask>,
    point_at: impl FnMut(usize) -> Point,
) -> PyResult<CoordSeq> {
    match model {
        crs::MetricModel::Geodesic { .. } => {
            let metric = crs::GeodesicMetric::from_model(model)?;
            destination_point_coords::<HAS_MISSING>(
                kernel,
                len,
                axes,
                missing,
                point_at,
                |point, row| {
                    destination_point_on_geodesic(
                        &metric.geodesic,
                        metric.frame,
                        point,
                        row.bearing,
                        row.distance,
                    )
                    .map_err(PyErr::from)
                },
            )
        },
        crs::MetricModel::Planar { to_metre } => destination_point_coords::<HAS_MISSING>(
            kernel,
            len,
            axes,
            missing,
            point_at,
            |point, row| destination_point_planar(to_metre.get(), point, row.bearing, row.distance),
        ),
    }
}

fn destination_many_array(
    py: Python<'_>,
    array: &PyGeometryArray,
    bearing: &CoordinateInput,
    distance: &CoordinateInput,
    unit: Option<DistanceUnit>,
) -> PyResult<Py<PyAny>> {
    let len = array.len();
    if len == 0 {
        return Ok(
            PyGeometryArray::from_shapes(Vec::new(), array.frame.clone())?
                .into_pyobject(py)?
                .unbind()
                .into(),
        );
    }
    if let Some(points) = (!array.is_compact_nullable())
        .then(|| array.storage().point_rows())
        .flatten()
    {
        let missing = array.missing().cloned();
        if let Some(axes) = uniform_point_axes_dispatch(&points, len, missing.as_ref()) {
            let model = resolve_metric(array.crs_str(), unit, MetricOp::Destination)
                .map_err(|error| row_zero(error.into()))?;
            let missing_for_rows = missing.clone();
            let kernel = DestinationPointKernel {
                bearing: &bearing.values,
                distance: &distance.values,
            };
            let coords = py.detach(move || {
                missing_for_rows.as_ref().map_or_else(
                    || {
                        destination_coords_dispatch::<false>(
                            kernel,
                            &model,
                            len,
                            axes,
                            None,
                            |row| points.get(row),
                        )
                    },
                    |mask| {
                        destination_coords_dispatch::<true>(
                            kernel,
                            &model,
                            len,
                            axes,
                            Some(mask),
                            |row| points.get(row),
                        )
                    },
                )
            })?;
            return Ok(PyGeometryArray::packed_points(coords, array.frame.clone())?
                .with_missing_mask(missing)?
                .into_pyobject(py)?
                .unbind()
                .into());
        }
    }

    let bearings = bearing.values.as_slice();
    let distances = distance.values.as_slice();
    let frame = array.frame.clone();
    let shapes = py.detach(move || {
        array
            .row_operation()
            .rows_with_missing(|| Option::<Shape>::None)
            .map(|row| match row {
                crate::array::ArrayRow::Missing { value, .. } => Ok(value),
                crate::array::ArrayRow::Present(row) => {
                    let logical = row.logical();
                    row.row().try_with_shape(|shape| {
                        let geometry = PyGeometry::with_frame(shape.clone(), frame.clone());
                        let parameters =
                            destination_row_at(bearings, distances, LogicalRowIndex::new(logical));
                        point_destination(&geometry, parameters.bearing, parameters.distance, unit)
                            .map(Some)
                            .map_err(|err| crate::note_array_row(err, logical))
                    })
                },
            })
            .collect::<PyResult<Vec<_>>>()
    })?;
    let present =
        PyGeometryArray::from_shapes(shapes.into_iter().flatten().collect(), array.frame.clone())?;
    Ok(array
        .scatter_present_result(present)?
        .into_pyobject(py)?
        .unbind()
        .into())
}

fn destination_point_array(
    py: Python<'_>,
    point: &Bound<'_, PyAny>,
    bearing: &Bound<'_, PyAny>,
    distance: &Bound<'_, PyAny>,
    unit: Option<DistanceUnit>,
) -> PyResult<Py<PyAny>> {
    use GeometryInput::{Many, One};

    let point_in = classify_required(point)?;
    let point_len = input_len(point_in);
    let (mut bearing, mut distance) = destination_parameters(py, bearing, distance, point_len)?;
    let param_len = broadcast_coordinate_group(
        [(&mut bearing, "bearing"), (&mut distance, "distance")],
        "bearing and distance",
    )?;
    let len = point_len.unwrap_or(param_len);
    if let Some(point_len) = point_len
        && param_len != point_len
    {
        broadcast_destination_param(&mut bearing, point_len, "bearing")?;
        broadcast_destination_param(&mut distance, point_len, "distance")?;
    }
    validate_destination_distances(&distance.values)?;

    match point_in {
        One(geometry) if len == 1 && bearing.scalar && distance.scalar => {
            let row = destination_rows(&bearing.values, &distance.values, 1)
                .next()
                .expect("scalar destination has one parameter row");
            let shape = point_destination(geometry, row.bearing, row.distance, unit)?;
            Ok(geometry.typed_shape(shape).into_pyobject(py)?.unbind())
        },
        One(geometry) => {
            let point = point_operand(geometry, MetricOp::Destination)?;
            broadcast_destination_param(&mut bearing, len, "bearing")?;
            broadcast_destination_param(&mut distance, len, "distance")?;
            let model = resolve_metric(geometry.crs_str(), unit, MetricOp::Destination)?;
            let Some(point) = point else {
                return empty_destinations(py, geometry, len);
            };
            let axes = CoordinateAxes::from_point(point);
            let kernel = DestinationPointKernel {
                bearing: &bearing.values,
                distance: &distance.values,
            };
            let coords = py.detach(move || {
                destination_coords_dispatch::<false>(kernel, &model, len, axes, None, |_| point)
            })?;
            Ok(
                PyGeometryArray::packed_points(coords, geometry.frame.clone())?
                    .into_pyobject(py)?
                    .unbind()
                    .into(),
            )
        },
        Many(array) => {
            broadcast_destination_param(&mut bearing, len, "bearing")?;
            broadcast_destination_param(&mut distance, len, "distance")?;
            destination_many_array(py, array, &bearing, &distance, unit)
        },
    }
}

/// Point-leaf receiver path. Keeping this separate from the erased geometry
/// parser prevents Point-only API placement from leaking through inheritance.
pub(crate) fn destination_point_receiver(
    py: Python<'_>,
    geometry: &PyGeometry,
    bearing: &Bound<'_, PyAny>,
    distance: &Bound<'_, PyAny>,
    unit: Option<DistanceUnit>,
) -> PyResult<Py<PyAny>> {
    let (mut bearing, mut distance) = destination_parameters(py, bearing, distance, None)?;
    let len = broadcast_coordinate_group(
        [(&mut bearing, "bearing"), (&mut distance, "distance")],
        "bearing and distance",
    )?;
    validate_destination_distances(&distance.values)?;
    if len == 1 && bearing.scalar && distance.scalar {
        let row = destination_rows(&bearing.values, &distance.values, 1)
            .next()
            .expect("scalar destination has one parameter row");
        let shape = point_destination(geometry, row.bearing, row.distance, unit)?;
        return Ok(geometry.typed_shape(shape).into_pyobject(py)?.unbind());
    }
    let point = point_operand(geometry, MetricOp::Destination)?;
    let model = resolve_metric(geometry.crs_str(), unit, MetricOp::Destination)?;
    let Some(point) = point else {
        return empty_destinations(py, geometry, len);
    };
    let axes = CoordinateAxes::from_point(point);
    let kernel = DestinationPointKernel {
        bearing: &bearing.values,
        distance: &distance.values,
    };
    let coords = py.detach(move || {
        destination_coords_dispatch::<false>(kernel, &model, len, axes, None, |_| point)
    })?;
    Ok(
        PyGeometryArray::packed_points(coords, geometry.frame.clone())?
            .into_pyobject(py)?
            .unbind()
            .into(),
    )
}

/// Point-leaf receiver path for the rhumb route.
pub(crate) fn rhumb_destination_point_receiver(
    py: Python<'_>,
    geometry: &PyGeometry,
    bearing: &Bound<'_, PyAny>,
    distance: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let (mut bearing, mut distance) = destination_parameters(py, bearing, distance, None)?;
    let len = broadcast_coordinate_group(
        [(&mut bearing, "bearing"), (&mut distance, "distance")],
        "bearing and distance",
    )?;
    validate_destination_distances(&distance.values)?;
    let point = point_operand(geometry, MetricOp::RhumbDestination)?;
    let model = require_geographic_model(geometry.crs_str(), MetricOp::RhumbDestination)?;
    let metric = crs::GeodesicMetric::from_model(&model)?;
    let Some(point) = point else {
        if len == 1 && bearing.scalar && distance.scalar {
            return Ok(geometry
                .typed_shape(Shape::empty_point())
                .into_pyobject(py)?
                .unbind());
        }
        return empty_destinations(py, geometry, len);
    };
    if len == 1 && bearing.scalar && distance.scalar {
        let row = destination_rows(&bearing.values, &distance.values, 1)
            .next()
            .expect("scalar destination has one parameter row");
        let shape = rhumb_destination_point(
            &metric.crs,
            metric.frame,
            point,
            row.bearing,
            row.distance,
            &metric.state,
        )?;
        return Ok(geometry.typed_shape(shape).into_pyobject(py)?.unbind());
    }
    let axes = CoordinateAxes::from_point(point);
    let coords = py.detach(move || {
        rhumb_destination_scalar_coords(&metric, point, &bearing.values, &distance.values, axes)
    })?;
    Ok(
        PyGeometryArray::packed_points(coords, geometry.frame.clone())?
            .into_pyobject(py)?
            .unbind()
            .into(),
    )
}

fn point_unary_geometry(
    py: Python<'_>,
    geom: &Bound<'_, PyAny>,
    setup: impl Fn(Option<&str>) -> PyResult<crs::MetricModel> + Copy + Send + Sync,
    point_op: impl PointUnaryGeometryKernel,
    fallback: impl Fn(&PyGeometry, DestinationRow) -> PyResult<Shape>
    + Copy
    + Send
    + Sync
    + pyo3::marker::Ungil,
) -> PyResult<Py<PyAny>> {
    use GeometryInput::{Many, One};
    match classify_input(geom) {
        Some(One(geometry)) => {
            if let Some(point) = geometry_point(geometry) {
                let ctx = setup(geometry.crs_str())?;
                return Ok(geometry
                    .typed_shape(point_unary_geometry_eval(
                        point_op,
                        &ctx,
                        point_op.first_row(),
                        point,
                    )?)
                    .into_pyobject(py)?
                    .unbind());
            }
            Ok(geometry
                .typed_shape(fallback(geometry, point_op.first_row())?)
                .into_pyobject(py)?
                .unbind())
        },
        Some(Many(array)) => {
            if !array.is_compact_nullable()
                && let Some(points) = array.storage().point_rows()
            {
                let ctx = setup(array.crs_str()).map_err(row_zero)?;
                if points.is_empty() {
                    return Ok(
                        PyGeometryArray::from_shapes(Vec::new(), array.frame.clone())?
                            .into_pyobject(py)?
                            .unbind()
                            .into(),
                    );
                }
                let missing = array.missing().cloned();
                if let Some(axes) =
                    uniform_point_axes_dispatch(&points, points.len(), missing.as_ref())
                {
                    let missing_for_rows = missing.clone();
                    let coords = py.detach(move || {
                        point_unary_geometry_coords_dispatch(
                            point_op,
                            &ctx,
                            &points,
                            points.len(),
                            axes,
                            missing_for_rows.as_ref(),
                        )
                    })?;
                    return Ok(PyGeometryArray::packed_points(coords, array.frame.clone())?
                        .with_missing_mask(missing)?
                        .into_pyobject(py)?
                        .unbind()
                        .into());
                }
            }
            let output_frame = array.frame.clone();
            let frame = output_frame.clone();
            let (bearings, distances) = point_op.parameters();
            let shapes = py.detach(move || {
                array
                    .row_operation()
                    .rows_with_missing(|| Option::<Shape>::None)
                    .map(|row| match row {
                        crate::array::ArrayRow::Missing { value, .. } => Ok(value),
                        crate::array::ArrayRow::Present(row) => {
                            let logical = row.logical();
                            row.row().try_with_shape(|shape| {
                                let geometry = PyGeometry::with_frame(shape.clone(), frame.clone());
                                let parameters = destination_row_at(
                                    bearings,
                                    distances,
                                    LogicalRowIndex::new(logical),
                                );
                                fallback(&geometry, parameters)
                                    .map(Some)
                                    .map_err(|err| crate::note_array_row(err, logical))
                            })
                        },
                    })
                    .collect::<PyResult<Vec<_>>>()
            })?;
            let present =
                PyGeometryArray::from_shapes(shapes.into_iter().flatten().collect(), output_frame)?;
            Ok(array
                .scatter_present_result(present)?
                .into_pyobject(py)?
                .unbind()
                .into())
        },
        None => Err(expected_geometry_or_array()),
    }
}

/// Return the point reached from ``point`` along ``bearing`` for ``distance``.
///
/// CRS-aware like every metric: geodesic on a geographic CRS, a planar offset
/// in native linear units on a projected CRS, and raw coordinate units when
/// CRS-free.
/// ``path='rhumb'`` instead follows a constant compass course in meters on a
/// geographic CRS. An empty point stays ``POINT EMPTY``.
///
/// Parameters
/// ----------
/// point : Point or GeometryArray
///     Starting point(s).
/// bearing : float or sequence of float
///     Initial azimuth(s) in degrees clockwise from north.
/// distance : float or sequence of float
///     Distance(s) to travel (geodesic meters on a geographic CRS, native
///     linear units on a projected CRS, raw coordinate units when CRS-free).
///     Must be finite
///     and non-negative.
/// path : {'geodesic', 'rhumb'}, default 'geodesic'
///     Route model. Rhumb paths require a geographic CRS and use meters.
#[doc = unit_parameter_doc!()]
///     ``path='rhumb'`` uses meters and does not accept a unit override.
///
/// Returns
/// -------
/// Point or GeometryArray
///     One destination per input point.
///
/// Raises
/// ------
/// GeometryError
///     If ``bearing`` is non-finite or ``distance`` is not finite and
///     non-negative.
/// GeometryTypeError
///     If ``point`` is not a `Point`.
/// CRSError
///     If a coordinate is outside the longitude/latitude domain.
///
/// Examples
/// --------
pub(crate) fn destination(
    py: Python<'_>,
    point: &Bound<'_, PyAny>,
    bearing: &Bound<'_, PyAny>,
    distance: &Bound<'_, PyAny>,
    path: NavigationPath,
    unit: Option<DistanceUnit>,
) -> PyResult<Py<PyAny>> {
    match path {
        NavigationPath::Geodesic => destination_point_array(py, point, bearing, distance, unit),
        NavigationPath::Rhumb => {
            reject_rhumb_unit(unit, MetricOp::Destination)?;
            rhumb_destination_point_array(py, point, bearing, distance)
        },
    }
}

fn rhumb_destination_point_array(
    py: Python<'_>,
    point: &Bound<'_, PyAny>,
    bearing: &Bound<'_, PyAny>,
    distance: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let point_input = classify_required(point)?;
    let point_len = input_len(point_input);
    let (mut bearing, mut distance) = destination_parameters(py, bearing, distance, point_len)?;
    let param_len = broadcast_coordinate_group(
        [(&mut bearing, "bearing"), (&mut distance, "distance")],
        "bearing and distance",
    )?;
    if let Some(point_len) = point_len
        && param_len != point_len
    {
        broadcast_destination_param(&mut bearing, point_len, "bearing")?;
        broadcast_destination_param(&mut distance, point_len, "distance")?;
    }
    validate_destination_distances(&distance.values)?;

    if point_len.is_none() && (!bearing.scalar || !distance.scalar) {
        let GeometryInput::One(geometry) = point_input else {
            unreachable!("point_len is None only for a scalar geometry")
        };
        let point = point_operand(geometry, MetricOp::RhumbDestination)?;
        let model = require_geographic_model(geometry.crs_str(), MetricOp::RhumbDestination)?;
        let Some(point) = point else {
            return empty_destinations(py, geometry, bearing.values.len());
        };
        let metric = crs::GeodesicMetric::from_model(&model)?;
        let axes = CoordinateAxes::from_point(point);
        let coords = py.detach(move || {
            rhumb_destination_scalar_coords(&metric, point, &bearing.values, &distance.values, axes)
        })?;
        return Ok(
            PyGeometryArray::packed_points(coords, geometry.frame.clone())?
                .into_pyobject(py)?
                .unbind()
                .into(),
        );
    }

    let bearings = bearing.values.as_slice();
    let distances = distance.values.as_slice();
    point_unary_geometry(
        py,
        point,
        |crs| require_geographic_model(crs, MetricOp::RhumbDestination),
        RhumbDestinationKernel {
            bearing: bearings,
            distance: distances,
        },
        move |geometry, row| point_rhumb_destination(geometry, row.bearing, row.distance),
    )
}
