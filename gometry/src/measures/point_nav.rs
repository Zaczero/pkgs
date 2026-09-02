//! Point navigation kernels — bearing, destination, interpolate, rhumb.
#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]

use pyo3::IntoPyObjectExt as _;
use pyo3::types::PyAny;

use crate::array::{GeometryArrayStorage, ValidityMask};
use crate::geographic::GeographicFrame;
use crate::geometry::{
    CoordSeq, CoordSeqBuilder, CoordinateAxes, HasM, HasZ, MOrdinate, ZOrdinate,
    same_topological_coordinate,
};
use crate::py::numpy::float64_array;
use crate::{
    Bound, CoordinateInput, Crs, DistanceUnit, Frame, GeometryInput, IntoPyObject as _,
    NavigationPath, Point, PointRows, Py, PyErr, PyGeometry, PyGeometryArray, PyResult, Python,
    Shape, broadcast_coordinate_group, classify_input, classify_required,
    coordinate_sequence_len_hint, crs, ensure_same_len, expected_geometry_or_array,
    parameter_input_with_expected, pyfunction, require_point, resolve_metric,
};

fn geometry_point(geometry: &PyGeometry) -> Option<Point> {
    match geometry.shape.shape() {
        Shape::Point(point) => Some(*point),
        _ => None,
    }
}

fn row_zero(err: PyErr) -> PyErr {
    crate::note_array_row(err, 0)
}

fn to_geodesic_point(frame: GeographicFrame, point: Point) -> PyResult<Point> {
    frame
        .to_geodesic_point(point)
        .map_err(|_| crate::boundary::geographic::invalid_lonlat_error(point.x, point.y).into())
}

fn from_geodesic_point(frame: GeographicFrame, point: Point) -> PyResult<Point> {
    frame
        .from_geodesic_point(point)
        .map_err(|error| crate::py::errors::CRSError::new_err(error.to_string()))
}

fn require_geographic_model(crs: Option<&str>, operation: &str) -> PyResult<crs::MetricModel> {
    match crs::metric_model(crs)? {
        model @ crs::MetricModel::Geodesic { .. } => Ok(model),
        crs::MetricModel::Planar { .. } => Err(crate::py::errors::CRSError::new_err(format!(
            "{operation} requires a geographic CRS; use set_crs(...) or to_crs(...) to attach one"
        ))),
    }
}

pub(crate) fn bearing_degrees(dx: f64, dy: f64) -> f64 {
    let bearing = dx.atan2(dy).to_degrees();
    if bearing < 0.0 {
        bearing + 360.0
    } else {
        bearing
    }
}

fn rhumb_distance_points(
    crs: &str,
    frame: GeographicFrame,
    from: Point,
    to: Point,
    state: &crs::RuntimeState,
) -> PyResult<f64> {
    let from = to_geodesic_point(frame, from)?;
    let to = to_geodesic_point(frame, to)?;
    Ok(crs::rhumb_distance_crs_at(
        crs, from.x, from.y, to.x, to.y, state,
    )?)
}

fn rhumb_bearing_points(
    crs: &str,
    frame: GeographicFrame,
    from: Point,
    to: Point,
    state: &crs::RuntimeState,
) -> PyResult<f64> {
    let from = to_geodesic_point(frame, from)?;
    let to = to_geodesic_point(frame, to)?;
    Ok(crs::rhumb_bearing_crs_at(
        crs, from.x, from.y, to.x, to.y, state,
    )?)
}

fn destination_point<const GEODESIC: bool>(
    model: &crs::MetricModel,
    from: Point,
    bearing: f64,
    distance: f64,
) -> PyResult<Point> {
    dispatch_point_nav::<GEODESIC>(model);
    if same_topological_coordinate(distance, 0.0) {
        if GEODESIC {
            let crs::MetricModel::Geodesic { frame, .. } = model else {
                unreachable!("checked above");
            };
            to_geodesic_point(*frame, from)?;
        }
        return Ok(from);
    }
    let (x, y) = if GEODESIC {
        let crs::MetricModel::Geodesic { crs, frame, state } = model else {
            unreachable!("checked above");
        };
        let from_geodesic = to_geodesic_point(*frame, from)?;
        let (x, y) = crs::geodesic_destination_crs_at(
            crs,
            from_geodesic.x,
            from_geodesic.y,
            bearing,
            distance,
            state,
        )?;
        return from_geodesic_point(
            *frame,
            Point::new_axes(x, y, ZOrdinate(from.z()), MOrdinate(from.m()))?,
        );
    } else {
        let crs::MetricModel::Planar { to_metre } = model else {
            unreachable!("checked above");
        };
        let radians = bearing.to_radians();
        let step = distance / to_metre.get();
        (step * radians.sin() + from.x, step * radians.cos() + from.y)
    };
    Ok(Point::new_axes(
        x,
        y,
        ZOrdinate(from.z()),
        MOrdinate(from.m()),
    )?)
}

fn destination_point_on_geodesic(
    geodesic: &geographiclib_rs::Geodesic,
    frame: GeographicFrame,
    from: Point,
    bearing: f64,
    distance: f64,
) -> crate::error::Result<Point> {
    if same_topological_coordinate(distance, 0.0) {
        frame
            .to_geodesic_point(from)
            .map_err(|_| crate::boundary::geographic::invalid_lonlat_error(from.x, from.y))?;
        return Ok(from);
    }
    let from_geodesic = frame
        .to_geodesic_point(from)
        .map_err(|_| crate::boundary::geographic::invalid_lonlat_error(from.x, from.y))?;
    let (x, y) = crs::geodesic_destination(
        geodesic,
        from_geodesic.x,
        from_geodesic.y,
        bearing,
        distance,
    )?;
    let point = Point::new_axes(x, y, ZOrdinate(from.z()), MOrdinate(from.m()))?;
    frame
        .from_geodesic_point(point)
        .map_err(|error| crate::crs::CrsError::message(error.to_string()).into())
}

fn rhumb_destination_point(
    crs: &str,
    frame: GeographicFrame,
    from: Point,
    bearing: f64,
    distance: f64,
    state: &crs::RuntimeState,
) -> PyResult<Shape> {
    if same_topological_coordinate(distance, 0.0) {
        to_geodesic_point(frame, from)?;
        return Ok(Shape::Point(from));
    }
    let from_geodesic = to_geodesic_point(frame, from)?;
    let Some((x, y)) = crs::rhumb_destination_crs_at(
        crs,
        from_geodesic.x,
        from_geodesic.y,
        bearing,
        distance,
        state,
    )?
    else {
        return Err(crate::py::errors::InvalidGeometryError::new_err(
            "destination(path='rhumb') crosses a pole, where the destination longitude is \
             indeterminate",
        ));
    };
    Ok(Shape::Point(from_geodesic_point(
        frame,
        Point::new_axes(x, y, ZOrdinate(from.z()), MOrdinate(from.m()))?,
    )?))
}

fn interpolate_point_on_geodesic(
    geodesic: &geographiclib_rs::Geodesic,
    frame: GeographicFrame,
    from: Point,
    to: Point,
    distance: f64,
    normalized: bool,
) -> crate::error::Result<Point> {
    let from_geodesic = frame
        .to_geodesic_point(from)
        .map_err(|_| crate::boundary::geographic::invalid_lonlat_error(from.x, from.y))?;
    let to_geodesic = frame
        .to_geodesic_point(to)
        .map_err(|_| crate::boundary::geographic::invalid_lonlat_error(to.x, to.y))?;
    // One inverse (dist+az) + optional direct — total distance is never
    // solved as a separate inverse first.
    let (x, y, ratio) = crs::geodesic_point_between(
        geodesic,
        from_geodesic.x,
        from_geodesic.y,
        to_geodesic.x,
        to_geodesic.y,
        distance,
        normalized,
    )?;
    if same_topological_coordinate(ratio, 0.0) {
        return Ok(from);
    }
    if same_topological_coordinate(ratio, 1.0) {
        return Ok(to);
    }
    let point = Point::new_axes(
        x,
        y,
        ZOrdinate(crate::crs::interpolate_optional_ordinate(
            from.z(),
            to.z(),
            ratio,
        )),
        MOrdinate(crate::crs::interpolate_optional_ordinate(
            from.m(),
            to.m(),
            ratio,
        )),
    )?;
    frame
        .from_geodesic_point(point)
        .map_err(|error| crate::crs::CrsError::message(error.to_string()).into())
}

fn interpolate_point<const GEODESIC: bool>(
    model: &crs::MetricModel,
    from: Point,
    to: Point,
    distance: f64,
    normalized: bool,
) -> PyResult<Shape> {
    dispatch_point_nav::<GEODESIC>(model);
    if GEODESIC {
        let crs::MetricModel::Geodesic { crs, frame, state } = model else {
            unreachable!("checked above");
        };
        let geodesic = crs::geodesic_for_crs_at(crs, state)?;
        return Ok(Shape::Point(interpolate_point_on_geodesic(
            &geodesic, *frame, from, to, distance, normalized,
        )?));
    }
    let crs::MetricModel::Planar { to_metre } = model else {
        unreachable!("checked above");
    };
    let total = (to.x - from.x).hypot(to.y - from.y) * to_metre.get();
    let ratio = if normalized {
        distance
    } else if total == 0.0 {
        0.0
    } else {
        distance / total
    }
    .clamp(0.0, 1.0);
    if same_topological_coordinate(ratio, 0.0) {
        return Ok(Shape::Point(from));
    }
    if same_topological_coordinate(ratio, 1.0) {
        return Ok(Shape::Point(to));
    }
    // Stable lerp: `start*(1-t)+end*t` avoids the `(end-start)*t` overflow
    // class on extreme-but-finite endpoints (shared with geometry segments).
    Ok(Shape::Point(crate::geometry::lerp_point(from, to, ratio)))
}

fn rhumb_interpolate_point(
    crs: &str,
    frame: GeographicFrame,
    from: Point,
    to: Point,
    distance: f64,
    normalized: bool,
    state: &crs::RuntimeState,
) -> PyResult<Shape> {
    let from_geodesic = to_geodesic_point(frame, from)?;
    let to_geodesic = to_geodesic_point(frame, to)?;
    let total = crs::rhumb_distance_crs_at(
        crs,
        from_geodesic.x,
        from_geodesic.y,
        to_geodesic.x,
        to_geodesic.y,
        state,
    )?;
    let ratio = if normalized {
        distance
    } else if total == 0.0 {
        0.0
    } else {
        distance / total
    }
    .clamp(0.0, 1.0);
    if same_topological_coordinate(ratio, 0.0) {
        return Ok(Shape::Point(from));
    }
    if same_topological_coordinate(ratio, 1.0) {
        return Ok(Shape::Point(to));
    }
    let bearing = crs::rhumb_bearing_crs_at(
        crs,
        from_geodesic.x,
        from_geodesic.y,
        to_geodesic.x,
        to_geodesic.y,
        state,
    )?;
    let Some((x, y)) = crs::rhumb_destination_crs_at(
        crs,
        from_geodesic.x,
        from_geodesic.y,
        bearing,
        total * ratio,
        state,
    )?
    else {
        return Err(crate::py::errors::InvalidGeometryError::new_err(
            "point_between(path='rhumb') crosses a pole, where the destination longitude is \
             indeterminate",
        ));
    };
    Ok(Shape::Point(from_geodesic_point(
        frame,
        Point::new_axes(
            x,
            y,
            ZOrdinate(crate::crs::interpolate_optional_ordinate(
                from.z(),
                to.z(),
                ratio,
            )),
            MOrdinate(crate::crs::interpolate_optional_ordinate(
                from.m(),
                to.m(),
                ratio,
            )),
        )?,
    )?))
}

pub(crate) fn point_bearing(geometry: &PyGeometry, other: &PyGeometry) -> PyResult<f64> {
    let from = require_point(geometry, "bearing")?;
    let to = require_point(other, "bearing")?;
    geometry.frame.compatible(&other.frame, "bearing")?;
    let model = resolve_metric(geometry.crs_str(), None, "bearing")?;
    point_binary_f64_eval(BearingKernel, &model, from, to)
}

pub(crate) fn point_cross_track_distance(
    geometry: &PyGeometry,
    start: &PyGeometry,
    end: &PyGeometry,
) -> PyResult<f64> {
    let probe = require_point(geometry, "cross_track_distance")?;
    let from = require_point(start, "cross_track_distance")?;
    let to = require_point(end, "cross_track_distance")?;
    geometry
        .frame
        .compatible(&start.frame, "cross_track_distance")?;
    geometry
        .frame
        .compatible(&end.frame, "cross_track_distance")?;
    let model = resolve_metric(geometry.crs_str(), None, "cross_track_distance")?;
    let crs::MetricModel::Geodesic { crs, frame, state } = model else {
        return Err(crate::py::errors::CRSError::new_err(
            "cross_track_distance requires a geographic CRS; use set_crs(...) or to_crs(...) \
             to attach one",
        ));
    };
    cross_track_points(&crs, frame, probe, from, to, &state)
}

pub(crate) fn point_rhumb_distance(geometry: &PyGeometry, other: &PyGeometry) -> PyResult<f64> {
    let from = require_point(geometry, "rhumb_distance")?;
    let to = require_point(other, "rhumb_distance")?;
    geometry.frame.compatible(&other.frame, "rhumb_distance")?;
    let model = resolve_metric(geometry.crs_str(), None, "rhumb_distance")?;
    let crs::MetricModel::Geodesic { crs, frame, state } = model else {
        return Err(crate::py::errors::CRSError::new_err(
            "rhumb_distance requires a geographic CRS; use set_crs(...) or to_crs(...) to attach one",
        ));
    };
    rhumb_distance_points(&crs, frame, from, to, &state)
}

pub(crate) fn point_rhumb_bearing(geometry: &PyGeometry, other: &PyGeometry) -> PyResult<f64> {
    let from = require_point(geometry, "bearing(path='rhumb')")?;
    let to = require_point(other, "bearing(path='rhumb')")?;
    geometry
        .frame
        .compatible(&other.frame, "bearing(path='rhumb')")?;
    let model = resolve_metric(geometry.crs_str(), None, "bearing(path='rhumb')")?;
    let crs::MetricModel::Geodesic { crs, frame, state } = model else {
        return Err(crate::py::errors::CRSError::new_err(
            "bearing(path='rhumb') requires a geographic CRS; use set_crs(...) or to_crs(...) to attach one",
        ));
    };
    rhumb_bearing_points(&crs, frame, from, to, &state)
}

pub(crate) fn point_rhumb_destination(
    geometry: &PyGeometry,
    bearing: f64,
    distance: f64,
) -> PyResult<Shape> {
    let from = require_point(geometry, "destination(path='rhumb')")?;
    let model = resolve_metric(geometry.crs_str(), None, "destination(path='rhumb')")?;
    let crs::MetricModel::Geodesic { crs, frame, state } = model else {
        return Err(crate::py::errors::CRSError::new_err(
            "destination(path='rhumb') requires a geographic CRS; use set_crs(...) or to_crs(...) to attach one",
        ));
    };
    rhumb_destination_point(&crs, frame, from, bearing, distance, &state)
}

pub(crate) fn point_rhumb_between(
    geometry: &PyGeometry,
    other: &PyGeometry,
    distance: f64,
    normalized: bool,
) -> PyResult<Shape> {
    let from = require_point(geometry, "point_between(path='rhumb')")?;
    let to = require_point(other, "point_between(path='rhumb')")?;
    geometry
        .frame
        .compatible(&other.frame, "point_between(path='rhumb')")?;
    let model = resolve_metric(geometry.crs_str(), None, "point_between(path='rhumb')")?;
    let crs::MetricModel::Geodesic { crs, frame, state } = model else {
        return Err(crate::py::errors::CRSError::new_err(
            "point_between(path='rhumb') requires a geographic CRS; use set_crs(...) or to_crs(...) to attach one",
        ));
    };
    rhumb_interpolate_point(&crs, frame, from, to, distance, normalized, &state)
}

pub(crate) fn point_destination(
    geometry: &PyGeometry,
    bearing: f64,
    distance: f64,
    unit: Option<DistanceUnit>,
) -> PyResult<Shape> {
    let from = require_point(geometry, "destination")?;
    let model = resolve_metric(geometry.crs_str(), unit, "destination")?;
    match model {
        crs::MetricModel::Geodesic { .. } => Ok(Shape::Point(destination_point::<true>(
            &model, from, bearing, distance,
        )?)),
        crs::MetricModel::Planar { .. } => Ok(Shape::Point(destination_point::<false>(
            &model, from, bearing, distance,
        )?)),
    }
}

pub(crate) fn point_interpolate(
    geometry: &PyGeometry,
    other: &PyGeometry,
    distance: f64,
    normalized: bool,
    unit: Option<DistanceUnit>,
) -> PyResult<Shape> {
    let from = require_point(geometry, "interpolate")?;
    let to = require_point(other, "interpolate")?;
    geometry.frame.compatible(&other.frame, "interpolate")?;
    let model = resolve_metric(geometry.crs_str(), unit, "point_between")?;
    match model {
        crs::MetricModel::Geodesic { .. } => {
            interpolate_point::<true>(&model, from, to, distance, normalized)
        },
        crs::MetricModel::Planar { .. } => {
            interpolate_point::<false>(&model, from, to, distance, normalized)
        },
    }
}

fn cross_track_points(
    crs: &str,
    frame: GeographicFrame,
    probe: Point,
    from: Point,
    to: Point,
    state: &crs::RuntimeState,
) -> PyResult<f64> {
    if (from.x, from.y).eq(&(to.x, to.y)) {
        return Err(crate::py::errors::InvalidGeometryError::new_err(
            "cross_track_distance requires distinct start and end points",
        ));
    }
    let probe = to_geodesic_point(frame, probe)?;
    let from = to_geodesic_point(frame, from)?;
    let to = to_geodesic_point(frame, to)?;
    Ok(crs::geodesic_cross_track_crs_at(
        crs,
        (probe.x, probe.y),
        (from.x, from.y),
        (to.x, to.y),
        state,
    )?)
}

fn cross_track_points_with_radius(
    radius: f64,
    frame: GeographicFrame,
    probe: Point,
    from: Point,
    to: Point,
) -> PyResult<f64> {
    if (from.x, from.y).eq(&(to.x, to.y)) {
        return Err(crate::py::errors::InvalidGeometryError::new_err(
            "cross_track_distance requires distinct start and end points",
        ));
    }
    let probe = to_geodesic_point(frame, probe)?;
    let from = to_geodesic_point(frame, from)?;
    let to = to_geodesic_point(frame, to)?;
    Ok(crs::geodesic_cross_track_with_radius(
        radius,
        (probe.x, probe.y),
        (from.x, from.y),
        (to.x, to.y),
    )?)
}

enum PointOperand<'a> {
    One(Point),
    Compact(&'a PyGeometryArray),
    Many {
        points: PointRows<'a>,
        missing: Option<&'a ValidityMask>,
    },
}

impl PointOperand<'_> {
    fn get(&self, row: usize) -> Point {
        match self {
            Self::One(point) => *point,
            Self::Compact(array) => match array.logical_row(row) {
                Some(crate::array::LogicalRow::Present {
                    row: crate::array::ShapeRow::Point(point),
                    ..
                }) => point,
                _ => unreachable!("point kernels read only present compact rows"),
            },
            Self::Many { points, .. } => points.get(row),
        }
    }

    const fn missing(&self) -> Option<&ValidityMask> {
        match self {
            Self::One(_) => None,
            Self::Compact(array) => array.missing(),
            Self::Many { missing, .. } => *missing,
        }
    }
}

trait PointRowSource: Send {
    fn point_at(&self, row: usize) -> Point;
}

impl PointRowSource for Point {
    fn point_at(&self, _row: usize) -> Point {
        *self
    }
}

impl PointRowSource for PointRows<'_> {
    fn point_at(&self, row: usize) -> Point {
        self.get(row)
    }
}

impl PointRowSource for PointOperand<'_> {
    fn point_at(&self, row: usize) -> Point {
        self.get(row)
    }
}

trait PointPairRowsSink {
    type Output;

    fn run<L, R>(self, left: L, right: R) -> Self::Output
    where
        L: PointRowSource,
        R: PointRowSource;
}

fn dispatch_point_pair_rows<S: PointPairRowsSink>(
    left: PointOperand<'_>,
    right: PointOperand<'_>,
    sink: S,
) -> S::Output {
    sink.run(left, right)
}

fn input_len(input: GeometryInput<'_>) -> Option<usize> {
    match input {
        GeometryInput::One(_) => None,
        GeometryInput::Many(array) => Some(array.len()),
    }
}

fn input_point<'a>(input: &'a GeometryInput<'a>) -> Option<PointOperand<'a>> {
    match input {
        GeometryInput::One(geometry) => geometry_point(geometry).map(PointOperand::One),
        GeometryInput::Many(array) if array.is_compact_nullable() => {
            matches!(array.storage(), GeometryArrayStorage::Points { .. })
                .then_some(PointOperand::Compact(array))
        },
        GeometryInput::Many(array) => array
            .masked_point_rows()
            .map(|(points, missing)| PointOperand::Many { points, missing }),
    }
}

const fn input_frame<'a>(input: &'a GeometryInput<'a>) -> (Option<&'a Crs>, Option<f64>) {
    match input {
        GeometryInput::One(geometry) => (geometry.crs_ref(), geometry.epoch()),
        GeometryInput::Many(array) => (array.crs_ref(), array.epoch()),
    }
}

fn input_crs_str<'a>(input: &'a GeometryInput<'a>) -> Option<&'a str> {
    match input {
        GeometryInput::One(geometry) => geometry.crs_str(),
        GeometryInput::Many(array) => array.crs_str(),
    }
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
        Err(crate::py::errors::InvalidGeometryError::new_err(format!(
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

fn point_from_point_nav_shape(shape: &Shape) -> Point {
    match shape {
        Shape::Point(point) => *point,
        _ => unreachable!("point-navigation point fast paths return Point shapes"),
    }
}

fn point_nav_missing_point_with_axes(axes: CoordinateAxes) -> Point {
    Point::new_raw_axes(
        f64::NAN,
        f64::NAN,
        ZOrdinate(axes.has_z().then_some(f64::NAN)),
        MOrdinate(axes.has_m().then_some(f64::NAN)),
    )
}

fn is_missing(mask: Option<&ValidityMask>, row: usize) -> bool {
    mask.is_some_and(|mask| mask.is_missing(row))
}

fn row_missing<const HAS_MISSING: bool>(missing: Option<&ValidityMask>, row: usize) -> bool {
    if HAS_MISSING {
        missing
            .expect("HAS_MISSING rows carry a mask")
            .is_missing(row)
    } else {
        false
    }
}

fn merge_uniform_axes(current: &mut Option<CoordinateAxes>, axes: CoordinateAxes) -> bool {
    if let Some(seen) = *current {
        return seen == axes;
    }
    *current = Some(axes);
    true
}

fn uniform_point_axes<const HAS_MISSING: bool>(
    points: &PointRows<'_>,
    len: usize,
    missing: Option<&ValidityMask>,
) -> Option<CoordinateAxes> {
    let mut axes = None;
    for row in 0..len {
        if row_missing::<HAS_MISSING>(missing, row) {
            continue;
        }
        if !merge_uniform_axes(&mut axes, CoordinateAxes::from_point(points.get(row))) {
            return None;
        }
    }
    Some(axes.unwrap_or(CoordinateAxes::XY))
}

fn uniform_point_axes_dispatch(
    points: &PointRows<'_>,
    len: usize,
    missing: Option<&ValidityMask>,
) -> Option<CoordinateAxes> {
    if let Some(mask) = missing {
        return uniform_point_axes::<true>(points, len, Some(mask));
    }
    uniform_point_axes::<false>(points, len, None)
}

fn dispatch_point_nav<const GEODESIC: bool>(model: &crs::MetricModel) {
    match (GEODESIC, model) {
        (true, crs::MetricModel::Geodesic { .. }) | (false, crs::MetricModel::Planar { .. }) => {},
        _ => unreachable!("point-navigation model dispatch matched a different metric kind"),
    }
}

trait PointBinaryF64Kernel: Copy + Send + Sync {
    fn eval<const GEODESIC: bool>(
        self,
        model: &crs::MetricModel,
        left: Point,
        right: Point,
    ) -> PyResult<f64>;

    /// Geodesic array path: resolve the ellipsoid once, then map rows.
    /// Default keeps per-row `eval` (rhumb kernels share the geographic
    /// model arm but resolve a different TLS cache).
    fn eval_rows_geodesic(
        self,
        model: &crs::MetricModel,
        len: usize,
        missing: Option<&ValidityMask>,
        mut points: impl FnMut(usize) -> (Point, Point),
    ) -> PyResult<Vec<f64>> {
        match missing {
            Some(mask) => (0..len)
                .map(|row| {
                    if mask.is_missing(row) {
                        return Ok(f64::NAN);
                    }
                    let (left, right) = points(row);
                    self.eval::<true>(model, left, right)
                        .map_err(|err| crate::note_array_row(err, row))
                })
                .collect(),
            None => (0..len)
                .map(|row| {
                    let (left, right) = points(row);
                    self.eval::<true>(model, left, right)
                        .map_err(|err| crate::note_array_row(err, row))
                })
                .collect(),
        }
    }
}

#[derive(Clone, Copy)]
struct BearingKernel;

impl PointBinaryF64Kernel for BearingKernel {
    fn eval<const GEODESIC: bool>(
        self,
        model: &crs::MetricModel,
        from: Point,
        to: Point,
    ) -> PyResult<f64> {
        dispatch_point_nav::<GEODESIC>(model);
        if GEODESIC {
            let crs::MetricModel::Geodesic { crs, frame, state } = model else {
                unreachable!("checked above");
            };
            let from = to_geodesic_point(*frame, from)?;
            let to = to_geodesic_point(*frame, to)?;
            Ok(crs::geodesic_bearing_crs_at(
                crs, from.x, from.y, to.x, to.y, state,
            )?)
        } else {
            Ok(bearing_degrees(to.x - from.x, to.y - from.y))
        }
    }

    fn eval_rows_geodesic(
        self,
        model: &crs::MetricModel,
        len: usize,
        missing: Option<&ValidityMask>,
        mut points: impl FnMut(usize) -> (Point, Point),
    ) -> PyResult<Vec<f64>> {
        let crs::MetricModel::Geodesic { crs, frame, state } = model else {
            unreachable!("geodesic rows require a geodesic model");
        };
        // One TLS/LRU resolve for the whole batch (`Geodesic` is Copy).
        let geodesic = crs::geodesic_for_crs_at(crs, state)?;
        match missing {
            Some(mask) => (0..len)
                .map(|row| {
                    if mask.is_missing(row) {
                        return Ok(f64::NAN);
                    }
                    let (from, to) = points(row);
                    let from = to_geodesic_point(*frame, from)
                        .map_err(|err| crate::note_array_row(err, row))?;
                    let to = to_geodesic_point(*frame, to)
                        .map_err(|err| crate::note_array_row(err, row))?;
                    crs::geodesic_bearing(&geodesic, from.x, from.y, to.x, to.y)
                        .map_err(|err| crate::note_array_row(err.into(), row))
                })
                .collect(),
            None => (0..len)
                .map(|row| {
                    let (from, to) = points(row);
                    let from = to_geodesic_point(*frame, from)
                        .map_err(|err| crate::note_array_row(err, row))?;
                    let to = to_geodesic_point(*frame, to)
                        .map_err(|err| crate::note_array_row(err, row))?;
                    crs::geodesic_bearing(&geodesic, from.x, from.y, to.x, to.y)
                        .map_err(|err| crate::note_array_row(err.into(), row))
                })
                .collect(),
        }
    }
}

#[derive(Clone, Copy)]
struct RhumbDistanceKernel;

impl PointBinaryF64Kernel for RhumbDistanceKernel {
    fn eval<const GEODESIC: bool>(
        self,
        model: &crs::MetricModel,
        left: Point,
        right: Point,
    ) -> PyResult<f64> {
        dispatch_point_nav::<GEODESIC>(model);
        let crs::MetricModel::Geodesic { crs, frame, state } = model else {
            unreachable!("require_geographic returns geodesic");
        };
        rhumb_distance_points(crs, *frame, left, right, state)
    }
}

#[derive(Clone, Copy)]
struct RhumbBearingKernel;

impl PointBinaryF64Kernel for RhumbBearingKernel {
    fn eval<const GEODESIC: bool>(
        self,
        model: &crs::MetricModel,
        left: Point,
        right: Point,
    ) -> PyResult<f64> {
        dispatch_point_nav::<GEODESIC>(model);
        let crs::MetricModel::Geodesic { crs, frame, state } = model else {
            unreachable!("require_geographic returns geodesic");
        };
        rhumb_bearing_points(crs, *frame, left, right, state)
    }
}

fn point_binary_f64_eval<K: PointBinaryF64Kernel>(
    kernel: K,
    model: &crs::MetricModel,
    left: Point,
    right: Point,
) -> PyResult<f64> {
    match model {
        crs::MetricModel::Geodesic { .. } => kernel.eval::<true>(model, left, right),
        crs::MetricModel::Planar { .. } => kernel.eval::<false>(model, left, right),
    }
}

fn point_binary_f64_rows_planar<const HAS_MISSING: bool, K: PointBinaryF64Kernel>(
    kernel: K,
    model: &crs::MetricModel,
    len: usize,
    missing: Option<&ValidityMask>,
    mut points: impl FnMut(usize) -> (Point, Point),
) -> PyResult<Vec<f64>> {
    dispatch_point_nav::<false>(model);
    (0..len)
        .map(|row| {
            if row_missing::<HAS_MISSING>(missing, row) {
                return Ok(f64::NAN);
            }
            let (left, right) = points(row);
            kernel
                .eval::<false>(model, left, right)
                .map_err(|err| crate::note_array_row(err, row))
        })
        .collect()
}

fn point_binary_f64_rows_dispatch<K: PointBinaryF64Kernel>(
    kernel: K,
    model: &crs::MetricModel,
    len: usize,
    missing: Option<&ValidityMask>,
    points: impl FnMut(usize) -> (Point, Point),
) -> PyResult<Vec<f64>> {
    match model {
        crs::MetricModel::Geodesic { .. } => {
            dispatch_point_nav::<true>(model);
            kernel.eval_rows_geodesic(model, len, missing, points)
        },
        crs::MetricModel::Planar { .. } => match missing {
            Some(mask) => {
                point_binary_f64_rows_planar::<true, _>(kernel, model, len, Some(mask), points)
            },
            None => point_binary_f64_rows_planar::<false, _>(kernel, model, len, None, points),
        },
    }
}

trait PointBinaryGeometryKernel: Copy + Send + Sync {
    fn eval<const GEODESIC: bool>(
        self,
        model: &crs::MetricModel,
        row: usize,
        left: Point,
        right: Point,
    ) -> PyResult<Shape>;

    fn output_axes(self, left: Point, right: Point) -> CoordinateAxes;

    /// Geodesic array path writing lon/lat (+Z/M) into a `CoordSeq` with the
    /// ellipsoid resolved once. Default keeps per-row `eval` (rhumb).
    fn coords_rows_geodesic(
        self,
        model: &crs::MetricModel,
        len: usize,
        axes: CoordinateAxes,
        missing: Option<&ValidityMask>,
        mut points: impl FnMut(usize) -> (Point, Point),
    ) -> PyResult<CoordSeq> {
        let mut builder = CoordSeqBuilder::with_capacity(axes, len);
        for row in 0..len {
            if missing.is_some_and(|mask| mask.is_missing(row)) {
                builder.push(point_nav_missing_point_with_axes(axes));
                continue;
            }
            let (left, right) = points(row);
            let shape = self
                .eval::<true>(model, row, left, right)
                .map_err(|err| crate::note_array_row(err, row))?;
            builder.push(point_from_point_nav_shape(&shape));
        }
        Ok(match missing {
            Some(_) => builder.finish_with_missing_placeholders(),
            None => builder.finish_infallible(),
        })
    }
}

#[derive(Clone, Copy)]
struct InterpolatePointKernel<'a> {
    distance: &'a [f64],
    normalized: bool,
}

#[derive(Clone, Copy)]
struct RhumbInterpolatePointKernel<'a> {
    distance: &'a [f64],
    normalized: bool,
}

impl PointBinaryGeometryKernel for RhumbInterpolatePointKernel<'_> {
    fn eval<const GEODESIC: bool>(
        self,
        model: &crs::MetricModel,
        row: usize,
        left: Point,
        right: Point,
    ) -> PyResult<Shape> {
        dispatch_point_nav::<GEODESIC>(model);
        let crs::MetricModel::Geodesic { crs, frame, state } = model else {
            unreachable!("require_geographic returns geodesic");
        };
        rhumb_interpolate_point(
            crs,
            *frame,
            left,
            right,
            self.distance[row],
            self.normalized,
            state,
        )
    }

    fn output_axes(self, left: Point, right: Point) -> CoordinateAxes {
        CoordinateAxes::new(
            HasZ(left.axes.has_z() && right.axes.has_z()),
            HasM(left.axes.has_m() && right.axes.has_m()),
        )
    }
}

impl PointBinaryGeometryKernel for InterpolatePointKernel<'_> {
    fn eval<const GEODESIC: bool>(
        self,
        model: &crs::MetricModel,
        row: usize,
        left: Point,
        right: Point,
    ) -> PyResult<Shape> {
        dispatch_point_nav::<GEODESIC>(model);
        interpolate_point::<GEODESIC>(model, left, right, self.distance[row], self.normalized)
    }

    fn output_axes(self, left: Point, right: Point) -> CoordinateAxes {
        CoordinateAxes::new(
            HasZ(left.axes.has_z() && right.axes.has_z()),
            HasM(left.axes.has_m() && right.axes.has_m()),
        )
    }

    fn coords_rows_geodesic(
        self,
        model: &crs::MetricModel,
        len: usize,
        axes: CoordinateAxes,
        missing: Option<&ValidityMask>,
        mut points: impl FnMut(usize) -> (Point, Point),
    ) -> PyResult<CoordSeq> {
        let crs::MetricModel::Geodesic { crs, frame, state } = model else {
            unreachable!("geodesic rows require a geodesic model");
        };
        let geodesic = crs::geodesic_for_crs_at(crs, state)?;
        let mut builder = CoordSeqBuilder::with_capacity(axes, len);
        for row in 0..len {
            if missing.is_some_and(|mask| mask.is_missing(row)) {
                builder.push(point_nav_missing_point_with_axes(axes));
                continue;
            }
            let (left, right) = points(row);
            // Fused inverse+direct into coordinate columns — no Shape::Point stage.
            let point = interpolate_point_on_geodesic(
                &geodesic,
                *frame,
                left,
                right,
                self.distance[row],
                self.normalized,
            )
            .map_err(|err| crate::note_array_row(err.into(), row))?;
            builder.push(point);
        }
        Ok(match missing {
            Some(_) => builder.finish_with_missing_placeholders(),
            None => builder.finish_infallible(),
        })
    }
}

fn point_binary_geometry_eval<K: PointBinaryGeometryKernel>(
    kernel: K,
    model: &crs::MetricModel,
    row: usize,
    left: Point,
    right: Point,
) -> PyResult<Shape> {
    match model {
        crs::MetricModel::Geodesic { .. } => kernel.eval::<true>(model, row, left, right),
        crs::MetricModel::Planar { .. } => kernel.eval::<false>(model, row, left, right),
    }
}

fn point_binary_geometry_coords_planar<const HAS_MISSING: bool, K: PointBinaryGeometryKernel>(
    kernel: K,
    model: &crs::MetricModel,
    len: usize,
    axes: CoordinateAxes,
    missing: Option<&ValidityMask>,
    mut points: impl FnMut(usize) -> (Point, Point),
) -> PyResult<CoordSeq> {
    dispatch_point_nav::<false>(model);
    let mut builder = CoordSeqBuilder::with_capacity(axes, len);
    for row in 0..len {
        if row_missing::<HAS_MISSING>(missing, row) {
            builder.push(point_nav_missing_point_with_axes(axes));
        } else {
            let (left, right) = points(row);
            let shape = kernel
                .eval::<false>(model, row, left, right)
                .map_err(|err| crate::note_array_row(err, row))?;
            builder.push(point_from_point_nav_shape(&shape));
        }
    }
    Ok(builder.finish_infallible())
}

fn point_binary_geometry_coords_dispatch<K: PointBinaryGeometryKernel>(
    kernel: K,
    model: &crs::MetricModel,
    len: usize,
    axes: CoordinateAxes,
    missing: Option<&ValidityMask>,
    points: impl FnMut(usize) -> (Point, Point),
) -> PyResult<CoordSeq> {
    match model {
        crs::MetricModel::Geodesic { .. } => {
            dispatch_point_nav::<true>(model);
            kernel.coords_rows_geodesic(model, len, axes, missing, points)
        },
        crs::MetricModel::Planar { .. } => match missing {
            Some(mask) => point_binary_geometry_coords_planar::<true, _>(
                kernel,
                model,
                len,
                axes,
                Some(mask),
                points,
            ),
            None => point_binary_geometry_coords_planar::<false, _>(
                kernel, model, len, axes, None, points,
            ),
        },
    }
}

fn uniform_point_binary_geometry_axes<const HAS_MISSING: bool, K: PointBinaryGeometryKernel>(
    kernel: K,
    len: usize,
    missing: Option<&ValidityMask>,
    mut points: impl FnMut(usize) -> (Point, Point),
) -> Option<CoordinateAxes> {
    let mut axes = None;
    for row in 0..len {
        if row_missing::<HAS_MISSING>(missing, row) {
            continue;
        }
        let (left, right) = points(row);
        if !merge_uniform_axes(&mut axes, kernel.output_axes(left, right)) {
            return None;
        }
    }
    Some(axes.unwrap_or(CoordinateAxes::XY))
}

fn uniform_point_binary_geometry_axes_dispatch<K: PointBinaryGeometryKernel>(
    kernel: K,
    len: usize,
    missing: Option<&ValidityMask>,
    points: impl FnMut(usize) -> (Point, Point),
) -> Option<CoordinateAxes> {
    match missing {
        Some(mask) => {
            uniform_point_binary_geometry_axes::<true, _>(kernel, len, Some(mask), points)
        },
        None => uniform_point_binary_geometry_axes::<false, _>(kernel, len, None, points),
    }
}

trait PointUnaryGeometryKernel: Copy + Send + Sync {
    fn eval<const GEODESIC: bool>(
        self,
        model: &crs::MetricModel,
        row: usize,
        point: Point,
    ) -> PyResult<Shape>;
}

#[derive(Clone, Copy)]
struct DestinationPointKernel<'a> {
    bearing: &'a [f64],
    distance: &'a [f64],
}

impl DestinationPointKernel<'_> {
    fn eval_row<const GEODESIC: bool>(
        self,
        model: &crs::MetricModel,
        row: usize,
        point: Point,
    ) -> PyResult<Point> {
        dispatch_point_nav::<GEODESIC>(model);
        destination_point::<GEODESIC>(model, point, self.bearing[row], self.distance[row])
    }
}

#[derive(Clone, Copy)]
struct RhumbDestinationKernel<'a> {
    bearing: &'a [f64],
    distance: &'a [f64],
}

impl PointUnaryGeometryKernel for RhumbDestinationKernel<'_> {
    fn eval<const GEODESIC: bool>(
        self,
        model: &crs::MetricModel,
        row: usize,
        point: Point,
    ) -> PyResult<Shape> {
        dispatch_point_nav::<GEODESIC>(model);
        let crs::MetricModel::Geodesic { crs, frame, state } = model else {
            unreachable!("require_geographic returns geodesic");
        };
        rhumb_destination_point(
            crs,
            *frame,
            point,
            self.bearing[row],
            self.distance[row],
            state,
        )
    }
}

fn point_unary_geometry_eval<K: PointUnaryGeometryKernel>(
    kernel: K,
    model: &crs::MetricModel,
    row: usize,
    point: Point,
) -> PyResult<Shape> {
    match model {
        crs::MetricModel::Geodesic { .. } => kernel.eval::<true>(model, row, point),
        crs::MetricModel::Planar { .. } => kernel.eval::<false>(model, row, point),
    }
}

fn point_unary_geometry_coords<
    const GEODESIC: bool,
    const HAS_MISSING: bool,
    K: PointUnaryGeometryKernel,
>(
    kernel: K,
    model: &crs::MetricModel,
    points: &PointRows<'_>,
    len: usize,
    axes: CoordinateAxes,
    missing: Option<&ValidityMask>,
) -> PyResult<CoordSeq> {
    dispatch_point_nav::<GEODESIC>(model);
    let mut builder = CoordSeqBuilder::with_capacity(axes, len);
    for row in 0..len {
        if row_missing::<HAS_MISSING>(missing, row) {
            builder.push(point_nav_missing_point_with_axes(axes));
        } else {
            let shape = kernel
                .eval::<GEODESIC>(model, row, points.get(row))
                .map_err(|err| crate::note_array_row(err, row))?;
            builder.push(point_from_point_nav_shape(&shape));
        }
    }
    Ok(builder.finish_infallible())
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
            point_unary_geometry_coords::<true, true, _>(
                kernel,
                model,
                points,
                len,
                axes,
                Some(mask),
            )
        },
        (crs::MetricModel::Geodesic { .. }, None) => {
            point_unary_geometry_coords::<true, false, _>(kernel, model, points, len, axes, None)
        },
        (crs::MetricModel::Planar { .. }, Some(mask)) => {
            point_unary_geometry_coords::<false, true, _>(
                kernel,
                model,
                points,
                len,
                axes,
                Some(mask),
            )
        },
        (crs::MetricModel::Planar { .. }, None) => {
            point_unary_geometry_coords::<false, false, _>(kernel, model, points, len, axes, None)
        },
    }
}

fn destination_many_coords_geodesic(
    kernel: DestinationPointKernel<'_>,
    model: &crs::MetricModel,
    points: &PointRows<'_>,
    len: usize,
    axes: CoordinateAxes,
    missing: Option<&ValidityMask>,
) -> PyResult<CoordSeq> {
    dispatch_point_nav::<true>(model);
    let crs::MetricModel::Geodesic { crs, frame, state } = model else {
        unreachable!("checked above");
    };
    let geodesic = crs::geodesic_for_crs_at(crs, state)?;
    let mut builder = CoordSeqBuilder::with_capacity(axes, len);
    for row in 0..len {
        if missing.is_some_and(|mask| mask.is_missing(row)) {
            builder.push(point_nav_missing_point_with_axes(axes));
            continue;
        }
        builder.try_push(
            destination_point_on_geodesic(
                &geodesic,
                *frame,
                points.get(row),
                kernel.bearing[row],
                kernel.distance[row],
            )
            .map_err(|err| crate::note_array_row(err.into(), row)),
        )?;
    }
    Ok(builder.finish_infallible())
}

fn destination_many_coords_planar<const HAS_MISSING: bool>(
    kernel: DestinationPointKernel<'_>,
    model: &crs::MetricModel,
    points: &PointRows<'_>,
    len: usize,
    axes: CoordinateAxes,
    missing: Option<&ValidityMask>,
) -> PyResult<CoordSeq> {
    dispatch_point_nav::<false>(model);
    let mut builder = CoordSeqBuilder::with_capacity(axes, len);
    for row in 0..len {
        if row_missing::<HAS_MISSING>(missing, row) {
            builder.push(point_nav_missing_point_with_axes(axes));
        } else {
            builder.try_push(
                kernel
                    .eval_row::<false>(model, row, points.get(row))
                    .map_err(|err| crate::note_array_row(err, row)),
            )?;
        }
    }
    Ok(builder.finish_infallible())
}

fn destination_many_coords_dispatch(
    kernel: DestinationPointKernel<'_>,
    model: &crs::MetricModel,
    points: &PointRows<'_>,
    len: usize,
    axes: CoordinateAxes,
    missing: Option<&ValidityMask>,
) -> PyResult<CoordSeq> {
    match model {
        crs::MetricModel::Geodesic { .. } => {
            destination_many_coords_geodesic(kernel, model, points, len, axes, missing)
        },
        crs::MetricModel::Planar { .. } => missing.map_or_else(
            || destination_many_coords_planar::<false>(kernel, model, points, len, axes, None),
            |mask| {
                destination_many_coords_planar::<true>(kernel, model, points, len, axes, Some(mask))
            },
        ),
    }
}

fn destination_scalar_coords_geodesic(
    model: &crs::MetricModel,
    point: Point,
    bearing: &[f64],
    distance: &[f64],
    len: usize,
    axes: CoordinateAxes,
) -> PyResult<CoordSeq> {
    dispatch_point_nav::<true>(model);
    let crs::MetricModel::Geodesic { crs, frame, state } = model else {
        unreachable!("checked above");
    };
    let geodesic = crs::geodesic_for_crs_at(crs, state)?;
    let mut builder = CoordSeqBuilder::with_capacity(axes, len);
    for row in 0..len {
        builder.try_push(
            destination_point_on_geodesic(&geodesic, *frame, point, bearing[row], distance[row])
                .map_err(|err| crate::note_array_row(err.into(), row)),
        )?;
    }
    Ok(builder.finish_infallible())
}

fn destination_scalar_coords_planar(
    model: &crs::MetricModel,
    point: Point,
    bearing: &[f64],
    distance: &[f64],
    len: usize,
    axes: CoordinateAxes,
) -> PyResult<CoordSeq> {
    dispatch_point_nav::<false>(model);
    let mut builder = CoordSeqBuilder::with_capacity(axes, len);
    for row in 0..len {
        builder.try_push(
            destination_point::<false>(model, point, bearing[row], distance[row])
                .map_err(|err| crate::note_array_row(err, row)),
        )?;
    }
    Ok(builder.finish_infallible())
}

fn destination_scalar_coords_dispatch(
    model: &crs::MetricModel,
    point: Point,
    bearing: &[f64],
    distance: &[f64],
    len: usize,
    axes: CoordinateAxes,
) -> PyResult<CoordSeq> {
    match model {
        crs::MetricModel::Geodesic { .. } => {
            destination_scalar_coords_geodesic(model, point, bearing, distance, len, axes)
        },
        crs::MetricModel::Planar { .. } => {
            destination_scalar_coords_planar(model, point, bearing, distance, len, axes)
        },
    }
}

fn cross_track_point_rows<const HAS_MISSING: bool>(
    radius: f64,
    frame: GeographicFrame,
    points: &PointOperand<'_>,
    starts: &PointOperand<'_>,
    ends: &PointOperand<'_>,
    len: usize,
    missing: Option<&ValidityMask>,
) -> PyResult<Vec<f64>> {
    (0..len)
        .map(|row| {
            if row_missing::<HAS_MISSING>(missing, row) {
                return Ok(f64::NAN);
            }
            let (p, s, e) = (points.get(row), starts.get(row), ends.get(row));
            cross_track_points_with_radius(radius, frame, p, s, e)
                .map_err(|err| crate::note_array_row(err, row))
        })
        .collect()
}

fn cross_track_point_rows_dispatch(
    radius: f64,
    frame: GeographicFrame,
    points: &PointOperand<'_>,
    starts: &PointOperand<'_>,
    ends: &PointOperand<'_>,
    len: usize,
    missing: Option<&ValidityMask>,
) -> PyResult<Vec<f64>> {
    missing.map_or_else(
        || cross_track_point_rows::<false>(radius, frame, points, starts, ends, len, None),
        |mask| cross_track_point_rows::<true>(radius, frame, points, starts, ends, len, Some(mask)),
    )
}

const fn input_missing<'a>(input: &'a GeometryInput<'_>) -> Option<&'a ValidityMask> {
    match input {
        GeometryInput::One(_) => None,
        GeometryInput::Many(array) => array.missing(),
    }
}

fn union_missing_masks(inputs: &[&GeometryInput<'_>]) -> Option<ValidityMask> {
    crate::array::missing::union_many(inputs.iter().map(|input| input_missing(input)))
}

fn union_point_operand_missing(inputs: &[&PointOperand<'_>]) -> Option<ValidityMask> {
    crate::array::missing::union_many(inputs.iter().map(|input| input.missing()))
}

#[derive(Clone, Copy)]
enum PointBinaryInput<'a> {
    One(&'a PyGeometry),
    Many(&'a PyGeometryArray),
}

impl<'a> From<GeometryInput<'a>> for PointBinaryInput<'a> {
    fn from(input: GeometryInput<'a>) -> Self {
        match input {
            GeometryInput::One(geometry) => Self::One(geometry),
            GeometryInput::Many(array) => Self::Many(array),
        }
    }
}

impl<'a> PointBinaryInput<'a> {
    const fn frame(self) -> &'a Frame {
        match self {
            Self::One(geometry) => &geometry.frame,
            Self::Many(array) => &array.frame,
        }
    }

    const fn missing(self) -> Option<&'a ValidityMask> {
        match self {
            Self::One(_) => None,
            Self::Many(array) => array.missing(),
        }
    }

    fn point_operand(self) -> Option<PointOperand<'a>> {
        match self {
            Self::One(geometry) => geometry_point(geometry).map(PointOperand::One),
            Self::Many(array) if array.is_compact_nullable() => {
                matches!(array.storage(), GeometryArrayStorage::Points { .. })
                    .then_some(PointOperand::Compact(array))
            },
            Self::Many(array) => array
                .storage()
                .point_rows()
                .map(|points| PointOperand::Many {
                    points,
                    missing: array.missing(),
                }),
        }
    }

    fn owned_rows(self) -> PointBinaryRowInput {
        match self {
            Self::One(geometry) => PointBinaryRowInput::One(geometry.clone()),
            Self::Many(array) => PointBinaryRowInput::Many(array.clone()),
        }
    }
}

#[derive(Clone, Copy)]
enum PointBinaryExecution<'a> {
    Scalar {
        left: &'a PyGeometry,
        right: &'a PyGeometry,
    },
    Rows(usize),
}

struct PointBinaryPlan<'a> {
    left: PointBinaryInput<'a>,
    right: PointBinaryInput<'a>,
    execution: PointBinaryExecution<'a>,
    output_frame: Frame,
    missing: Option<ValidityMask>,
}

impl<'a> PointBinaryPlan<'a> {
    fn new(
        left: &'a Bound<'_, PyAny>,
        right: &'a Bound<'_, PyAny>,
        operation: &str,
    ) -> PyResult<Self> {
        let (Some(left), Some(right)) = (classify_input(left), classify_input(right)) else {
            return Err(expected_geometry_or_array());
        };
        let left = PointBinaryInput::from(left);
        let right = PointBinaryInput::from(right);
        let execution = match (left, right) {
            (PointBinaryInput::One(left), PointBinaryInput::One(right)) => {
                PointBinaryExecution::Scalar { left, right }
            },
            (PointBinaryInput::One(_), PointBinaryInput::Many(array))
            | (PointBinaryInput::Many(array), PointBinaryInput::One(_)) => {
                PointBinaryExecution::Rows(array.len())
            },
            (PointBinaryInput::Many(left), PointBinaryInput::Many(right)) => {
                // Length wins over frame compatibility for strict array broadcasting.
                ensure_same_len(left.len(), right.len())?;
                PointBinaryExecution::Rows(left.len())
            },
        };
        let output_frame = left.frame().compatible(right.frame(), operation)?;
        let missing = crate::array::missing::union_pair(left.missing(), right.missing());
        Ok(Self {
            left,
            right,
            execution,
            output_frame,
            missing,
        })
    }

    fn crs_str(&self) -> Option<&str> {
        self.left.frame().crs_str()
    }

    fn point_operands(&self) -> Option<(PointOperand<'a>, PointOperand<'a>)> {
        Some((self.left.point_operand()?, self.right.point_operand()?))
    }

    fn owned_rows(&self) -> PointBinaryRows {
        PointBinaryRows {
            left: self.left.owned_rows(),
            right: self.right.owned_rows(),
        }
    }
}

enum PointBinaryRowInput {
    One(PyGeometry),
    Many(PyGeometryArray),
}

struct PointBinaryRows {
    left: PointBinaryRowInput,
    right: PointBinaryRowInput,
}

impl PointBinaryRows {
    fn evaluate<T>(&self, row: usize, operation: impl FnOnce(&PyGeometry, &PyGeometry) -> T) -> T {
        match (&self.left, &self.right) {
            (PointBinaryRowInput::One(left), PointBinaryRowInput::One(right)) => {
                operation(left, right)
            },
            (PointBinaryRowInput::One(left), PointBinaryRowInput::Many(array)) => {
                operation(left, &array.geometry_at(row))
            },
            (PointBinaryRowInput::Many(array), PointBinaryRowInput::One(right)) => {
                operation(&array.geometry_at(row), right)
            },
            (PointBinaryRowInput::Many(left), PointBinaryRowInput::Many(right)) => {
                operation(&left.geometry_at(row), &right.geometry_at(row))
            },
        }
    }
}

fn destination_many_array(
    py: Python<'_>,
    array: &PyGeometryArray,
    bearing: CoordinateInput,
    distance: CoordinateInput,
    unit: Option<DistanceUnit>,
) -> PyResult<Py<PyAny>> {
    let len = array.len();
    if len == 0 {
        return Ok(
            PyGeometryArray::from_shapes(Vec::new(), array.frame.clone())
                .into_pyobject(py)?
                .unbind()
                .into(),
        );
    }
    let Some(points) = (!array.is_compact_nullable())
        .then(|| array.storage().point_rows())
        .flatten()
    else {
        let frame = array.frame.clone();
        let shapes = py.detach(move || {
            array
                .logical_shape_rows()
                .map(|row| match row {
                    crate::array::LogicalShapeRow::Missing { .. } => Ok(None),
                    crate::array::LogicalShapeRow::Present { logical, shape } => {
                        let geometry = PyGeometry::with_frame(shape.into_owned(), frame.clone());
                        point_destination(
                            &geometry,
                            bearing.values[logical],
                            distance.values[logical],
                            unit,
                        )
                        .map(Some)
                        .map_err(|err| crate::note_array_row(err, logical))
                    },
                })
                .collect::<PyResult<Vec<_>>>()
        })?;
        let present = PyGeometryArray::from_shapes(
            shapes.into_iter().flatten().collect(),
            array.frame.clone(),
        );
        return Ok(array
            .scatter_present_result(present)
            .into_pyobject(py)?
            .unbind()
            .into());
    };
    let missing = array.missing().cloned();
    let Some(axes) = uniform_point_axes_dispatch(&points, len, missing.as_ref()) else {
        let frame = array.frame.clone();
        let shapes = py.detach(move || {
            array
                .logical_shape_rows()
                .map(|row| match row {
                    crate::array::LogicalShapeRow::Missing { .. } => Ok(None),
                    crate::array::LogicalShapeRow::Present { logical, shape } => {
                        let geometry = PyGeometry::with_frame(shape.into_owned(), frame.clone());
                        point_destination(
                            &geometry,
                            bearing.values[logical],
                            distance.values[logical],
                            unit,
                        )
                        .map(Some)
                        .map_err(|err| crate::note_array_row(err, logical))
                    },
                })
                .collect::<PyResult<Vec<_>>>()
        })?;
        let present = PyGeometryArray::from_shapes(
            shapes.into_iter().flatten().collect(),
            array.frame.clone(),
        );
        return Ok(array
            .scatter_present_result(present)
            .into_pyobject(py)?
            .unbind()
            .into());
    };
    let model = resolve_metric(array.crs_str(), unit, "destination").map_err(row_zero)?;
    let missing_for_rows = missing.clone();
    let kernel = DestinationPointKernel {
        bearing: &bearing.values,
        distance: &distance.values,
    };
    let coords = py.detach(move || {
        destination_many_coords_dispatch(
            kernel,
            &model,
            &points,
            len,
            axes,
            missing_for_rows.as_ref(),
        )
    })?;
    Ok(PyGeometryArray::packed_points(coords, array.frame.clone())
        .with_missing_mask(missing)
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
            let point = require_point(geometry, "destination")?;
            let model = resolve_metric(geometry.crs_str(), unit, "destination")?;
            let point = match model {
                crs::MetricModel::Geodesic { .. } => {
                    destination_point::<true>(&model, point, bearing.values[0], distance.values[0])?
                },
                crs::MetricModel::Planar { .. } => destination_point::<false>(
                    &model,
                    point,
                    bearing.values[0],
                    distance.values[0],
                )?,
            };
            let shape = Shape::Point(point);
            Ok(geometry.typed_shape(shape).into_pyobject(py)?.unbind())
        },
        One(geometry) => {
            let point = require_point(geometry, "destination")?;
            broadcast_destination_param(&mut bearing, len, "bearing")?;
            broadcast_destination_param(&mut distance, len, "distance")?;
            let model = resolve_metric(geometry.crs_str(), unit, "destination")?;
            let axes = CoordinateAxes::from_point(point);
            let coords = py.detach(move || {
                destination_scalar_coords_dispatch(
                    &model,
                    point,
                    &bearing.values,
                    &distance.values,
                    len,
                    axes,
                )
            })?;
            Ok(
                PyGeometryArray::packed_points(coords, geometry.frame.clone())
                    .into_pyobject(py)?
                    .unbind()
                    .into(),
            )
        },
        Many(array) => {
            broadcast_destination_param(&mut bearing, len, "bearing")?;
            broadcast_destination_param(&mut distance, len, "distance")?;
            destination_many_array(py, array, bearing, distance, unit)
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
    let point = require_point(geometry, "destination")?;
    let model = resolve_metric(geometry.crs_str(), unit, "destination")?;
    if len == 1 && bearing.scalar && distance.scalar {
        let point = match model {
            crs::MetricModel::Geodesic { .. } => {
                destination_point::<true>(&model, point, bearing.values[0], distance.values[0])?
            },
            crs::MetricModel::Planar { .. } => {
                destination_point::<false>(&model, point, bearing.values[0], distance.values[0])?
            },
        };
        return Ok(geometry
            .typed_shape(Shape::Point(point))
            .into_pyobject(py)?
            .unbind());
    }
    let axes = CoordinateAxes::from_point(point);
    let coords = py.detach(move || {
        destination_scalar_coords_dispatch(
            &model,
            point,
            &bearing.values,
            &distance.values,
            len,
            axes,
        )
    })?;
    Ok(
        PyGeometryArray::packed_points(coords, geometry.frame.clone())
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
    let point = require_point(geometry, "destination(path='rhumb')")?;
    let model = require_geographic_model(geometry.crs_str(), "destination(path='rhumb')")?;
    let crs::MetricModel::Geodesic { crs, frame, state } = model else {
        unreachable!("geographic model check succeeded");
    };
    if len == 1 && bearing.scalar && distance.scalar {
        let shape = rhumb_destination_point(
            &crs,
            frame,
            point,
            bearing.values[0],
            distance.values[0],
            &state,
        )?;
        return Ok(geometry.typed_shape(shape).into_pyobject(py)?.unbind());
    }
    let axes = CoordinateAxes::from_point(point);
    let coords = py.detach(move || {
        let mut builder = CoordSeqBuilder::with_capacity(axes, len);
        for row in 0..len {
            let shape = rhumb_destination_point(
                &crs,
                frame,
                point,
                bearing.values[row],
                distance.values[row],
                &state,
            )
            .map_err(|err| crate::note_array_row(err, row))?;
            builder.push(point_from_point_nav_shape(&shape));
        }
        Ok::<_, PyErr>(builder.finish_infallible())
    })?;
    Ok(
        PyGeometryArray::packed_points(coords, geometry.frame.clone())
            .into_pyobject(py)?
            .unbind()
            .into(),
    )
}

struct PointBinaryNumericRowsSink<'py, K> {
    py: Python<'py>,
    model: crs::MetricModel,
    len: usize,
    missing: Option<ValidityMask>,
    kernel: K,
}

impl<K: PointBinaryF64Kernel> PointPairRowsSink for PointBinaryNumericRowsSink<'_, K> {
    type Output = PyResult<Py<PyAny>>;

    fn run<L, R>(self, left: L, right: R) -> Self::Output
    where
        L: PointRowSource,
        R: PointRowSource,
    {
        let Self {
            py,
            model,
            len,
            missing,
            kernel,
        } = self;
        float64_array(
            py,
            py.detach(move || {
                point_binary_f64_rows_dispatch(kernel, &model, len, missing.as_ref(), |row| {
                    (left.point_at(row), right.point_at(row))
                })
            })?,
        )
    }
}

struct PointBinaryGeometryRowsSink<'py, 'plan, 'input, K, F> {
    py: Python<'py>,
    plan: &'plan PointBinaryPlan<'input>,
    model: crs::MetricModel,
    len: usize,
    kernel: K,
    fallback: F,
}

impl<K, F> PointPairRowsSink for PointBinaryGeometryRowsSink<'_, '_, '_, K, F>
where
    K: PointBinaryGeometryKernel,
    F: Fn(&PyGeometry, &PyGeometry, usize) -> PyResult<Shape> + Copy + Send + Sync,
{
    type Output = PyResult<Py<PyAny>>;

    fn run<L, R>(self, left: L, right: R) -> Self::Output
    where
        L: PointRowSource,
        R: PointRowSource,
    {
        let Self {
            py,
            plan,
            model,
            len,
            kernel,
            fallback,
        } = self;
        if len == 0 {
            return Ok(
                PyGeometryArray::from_shapes(Vec::new(), plan.output_frame.clone())
                    .into_pyobject(py)?
                    .unbind()
                    .into(),
            );
        }
        let Some(axes) = uniform_point_binary_geometry_axes_dispatch(
            kernel,
            len,
            plan.missing.as_ref(),
            |row| (left.point_at(row), right.point_at(row)),
        ) else {
            return point_binary_geometry_fallback(py, plan, len, fallback);
        };
        let missing = plan.missing.clone();
        let missing_for_rows = missing.clone();
        let coords = py.detach(move || {
            point_binary_geometry_coords_dispatch(
                kernel,
                &model,
                len,
                axes,
                missing_for_rows.as_ref(),
                |row| (left.point_at(row), right.point_at(row)),
            )
        })?;
        Ok(
            PyGeometryArray::packed_points(coords, plan.output_frame.clone())
                .with_missing_mask(missing)
                .into_pyobject(py)?
                .unbind()
                .into(),
        )
    }
}

fn point_binary_f64(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    op_name: &str,
    setup: impl Fn(Option<&str>) -> PyResult<crs::MetricModel> + Copy + Send + Sync,
    point_op: impl PointBinaryF64Kernel,
    fallback: impl Fn(&PyGeometry, &PyGeometry) -> PyResult<f64> + Copy + Send + Sync,
) -> PyResult<Py<PyAny>> {
    let plan = PointBinaryPlan::new(left, right, op_name)?;
    point_binary_numeric_sink(py, &plan, setup, point_op, fallback)
}

fn point_binary_numeric_sink(
    py: Python<'_>,
    plan: &PointBinaryPlan<'_>,
    setup: impl Fn(Option<&str>) -> PyResult<crs::MetricModel> + Copy + Send + Sync,
    point_op: impl PointBinaryF64Kernel,
    fallback: impl Fn(&PyGeometry, &PyGeometry) -> PyResult<f64> + Copy + Send + Sync,
) -> PyResult<Py<PyAny>> {
    let len = match plan.execution {
        PointBinaryExecution::Scalar { left, right } => {
            return match (geometry_point(left), geometry_point(right)) {
                (Some(left_point), Some(right_point)) => {
                    let model = setup(plan.crs_str())?;
                    point_binary_f64_eval(point_op, &model, left_point, right_point)?
                        .into_py_any(py)
                },
                _ => fallback(left, right)?.into_py_any(py),
            };
        },
        PointBinaryExecution::Rows(len) => len,
    };

    if let Some((left, right)) = plan.point_operands() {
        let model = setup(plan.crs_str()).map_err(row_zero)?;
        return dispatch_point_pair_rows(left, right, PointBinaryNumericRowsSink {
            py,
            model,
            len,
            missing: plan.missing.clone(),
            kernel: point_op,
        });
    }

    let rows = plan.owned_rows();
    let missing = plan.missing.clone();
    float64_array(
        py,
        py.detach(move || {
            (0..len)
                .map(|row| {
                    if is_missing(missing.as_ref(), row) {
                        return Ok(f64::NAN);
                    }
                    rows.evaluate(row, fallback)
                        .map_err(|err| crate::note_array_row(err, row))
                })
                .collect::<PyResult<Vec<_>>>()
        })?,
    )
}

fn point_binary_geometry_sink(
    py: Python<'_>,
    plan: &PointBinaryPlan<'_>,
    setup: impl Fn(Option<&str>) -> PyResult<crs::MetricModel> + Copy + Send + Sync,
    point_op: impl PointBinaryGeometryKernel,
    fallback: impl Fn(&PyGeometry, &PyGeometry, usize) -> PyResult<Shape> + Copy + Send + Sync,
) -> PyResult<Py<PyAny>> {
    let len = match plan.execution {
        PointBinaryExecution::Scalar { left, right } => {
            let shape = if let (Some(left_point), Some(right_point)) =
                (geometry_point(left), geometry_point(right))
            {
                let model = setup(plan.crs_str())?;
                point_binary_geometry_eval(point_op, &model, 0, left_point, right_point)?
            } else {
                fallback(left, right, 0)?
            };
            return Ok(left.typed_shape(shape).into_pyobject(py)?.unbind());
        },
        PointBinaryExecution::Rows(len) => len,
    };

    if let Some((left, right)) = plan.point_operands() {
        let model = setup(plan.crs_str()).map_err(row_zero)?;
        return dispatch_point_pair_rows(left, right, PointBinaryGeometryRowsSink {
            py,
            plan,
            model,
            len,
            kernel: point_op,
            fallback,
        });
    }

    point_binary_geometry_fallback(py, plan, len, fallback)
}

fn point_binary_geometry_fallback(
    py: Python<'_>,
    plan: &PointBinaryPlan<'_>,
    len: usize,
    fallback: impl Fn(&PyGeometry, &PyGeometry, usize) -> PyResult<Shape> + Copy + Send + Sync,
) -> PyResult<Py<PyAny>> {
    let rows = plan.owned_rows();
    let missing = plan.missing.clone();
    let missing_for_rows = missing.clone();
    let shapes = py.detach(move || {
        (0..len)
            .map(|row| {
                if is_missing(missing_for_rows.as_ref(), row) {
                    return Ok(PyGeometryArray::missing_placeholder());
                }
                rows.evaluate(row, |left, right| fallback(left, right, row))
                    .map_err(|err| crate::note_array_row(err, row))
            })
            .collect::<PyResult<Vec<_>>>()
    })?;
    Ok(
        PyGeometryArray::from_shapes(shapes, plan.output_frame.clone())
            .with_missing_mask(missing)
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
    fallback: impl Fn(&PyGeometry, usize) -> PyResult<Shape> + Copy + Send + Sync,
) -> PyResult<Py<PyAny>> {
    use GeometryInput::{Many, One};
    match classify_input(geom) {
        Some(One(geometry)) => {
            if let Some(point) = geometry_point(geometry) {
                let ctx = setup(geometry.crs_str())?;
                return Ok(geometry
                    .typed_shape(point_unary_geometry_eval(point_op, &ctx, 0, point)?)
                    .into_pyobject(py)?
                    .unbind());
            }
            Ok(geometry
                .typed_shape(fallback(geometry, 0)?)
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
                        PyGeometryArray::from_shapes(Vec::new(), array.frame.clone())
                            .into_pyobject(py)?
                            .unbind()
                            .into(),
                    );
                }
                let missing = array.missing().cloned();
                let Some(axes) =
                    uniform_point_axes_dispatch(&points, points.len(), missing.as_ref())
                else {
                    let output_frame = array.frame.clone();
                    let frame = output_frame.clone();
                    let shapes = py.detach(move || {
                        array
                            .logical_shape_rows()
                            .map(|row| match row {
                                crate::array::LogicalShapeRow::Missing { .. } => Ok(None),
                                crate::array::LogicalShapeRow::Present { logical, shape } => {
                                    let geometry =
                                        PyGeometry::with_frame(shape.into_owned(), frame.clone());
                                    fallback(&geometry, logical)
                                        .map(Some)
                                        .map_err(|err| crate::note_array_row(err, logical))
                                },
                            })
                            .collect::<PyResult<Vec<_>>>()
                    })?;
                    let present = PyGeometryArray::from_shapes(
                        shapes.into_iter().flatten().collect(),
                        output_frame,
                    );
                    return Ok(array
                        .scatter_present_result(present)
                        .into_pyobject(py)?
                        .unbind()
                        .into());
                };
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
                return Ok(PyGeometryArray::packed_points(coords, array.frame.clone())
                    .with_missing_mask(missing)
                    .into_pyobject(py)?
                    .unbind()
                    .into());
            }
            let output_frame = array.frame.clone();
            let frame = output_frame.clone();
            let shapes = py.detach(move || {
                array
                    .logical_shape_rows()
                    .map(|row| match row {
                        crate::array::LogicalShapeRow::Missing { .. } => Ok(None),
                        crate::array::LogicalShapeRow::Present { logical, shape } => {
                            let geometry =
                                PyGeometry::with_frame(shape.into_owned(), frame.clone());
                            fallback(&geometry, logical)
                                .map(Some)
                                .map_err(|err| crate::note_array_row(err, logical))
                        },
                    })
                    .collect::<PyResult<Vec<_>>>()
            })?;
            let present =
                PyGeometryArray::from_shapes(shapes.into_iter().flatten().collect(), output_frame);
            Ok(array
                .scatter_present_result(present)
                .into_pyobject(py)?
                .unbind()
                .into())
        },
        None => Err(expected_geometry_or_array()),
    }
}

fn cross_track_broadcast(
    py: Python<'_>,
    point: &Bound<'_, PyAny>,
    start: &Bound<'_, PyAny>,
    end: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    const OP: &str = "cross_track_distance";
    use GeometryInput::{Many, One};
    let op = point_cross_track_distance;
    let point_in = classify_required(point)?;
    let start_in = classify_required(start)?;
    let end_in = classify_required(end)?;

    let mut broadcast_len = None;
    for len in [&point_in, &start_in, &end_in]
        .into_iter()
        .filter_map(|input| input_len(*input))
    {
        if let Some(previous) = broadcast_len {
            ensure_same_len(previous, len)?;
        } else {
            broadcast_len = Some(len);
        }
    }
    let row_at = |input: &GeometryInput<'_>, row: usize| -> PyResult<PyGeometry> {
        Ok(match input {
            One(geometry) => (*geometry).clone(),
            Many(array) => array.geometry_at(row),
        })
    };
    match (&point_in, &start_in, &end_in) {
        (One(p), One(s), One(e)) if broadcast_len.is_none() => {
            p.frame.compatible(&s.frame, OP)?;
            p.frame.compatible(&e.frame, OP)?;
            match (geometry_point(p), geometry_point(s), geometry_point(e)) {
                (Some(probe), Some(start), Some(end)) => {
                    let model = require_geographic_model(p.crs_str(), OP)?;
                    let crs::MetricModel::Geodesic { crs, frame, state } = model else {
                        unreachable!("geographic model check succeeded");
                    };
                    cross_track_points(&crs, frame, probe, start, end, &state)?.into_py_any(py)
                },
                _ => op(p, s, e)?.into_py_any(py),
            }
        },
        _ if broadcast_len.is_some() => {
            let len = broadcast_len.expect("checked above");
            let (point_crs, point_epoch) = input_frame(&point_in);
            let (start_crs, start_epoch) = input_frame(&start_in);
            let (end_crs, end_epoch) = input_frame(&end_in);
            Frame::compatible_parts(point_crs, point_epoch, start_crs, start_epoch, OP)?;
            Frame::compatible_parts(point_crs, point_epoch, end_crs, end_epoch, OP)?;
            let missing = union_missing_masks(&[&point_in, &start_in, &end_in]);
            if let (Some(points), Some(starts), Some(ends)) = (
                input_point(&point_in),
                input_point(&start_in),
                input_point(&end_in),
            ) {
                let model =
                    require_geographic_model(input_crs_str(&point_in), OP).map_err(row_zero)?;
                let crs::MetricModel::Geodesic { crs, frame, state } = model else {
                    unreachable!("geographic model check succeeded");
                };
                let radius = crs::geodesic_cross_track_radius_crs_at(&crs, &state)
                    .map_err(PyErr::from)
                    .map_err(row_zero)?;
                let missing_for_rows = union_point_operand_missing(&[&points, &starts, &ends]);
                return float64_array(
                    py,
                    py.detach(move || {
                        cross_track_point_rows_dispatch(
                            radius,
                            frame,
                            &points,
                            &starts,
                            &ends,
                            len,
                            missing_for_rows.as_ref(),
                        )
                    })?,
                );
            }
            let model = require_geographic_model(point_crs.map(|value| value.as_str()), OP)?;
            let crs::MetricModel::Geodesic { crs, frame, state } = model else {
                unreachable!("geographic model check succeeded");
            };
            float64_array(
                py,
                py.detach(move || {
                    (0..len)
                        .map(|row| {
                            if is_missing(missing.as_ref(), row) {
                                return Ok(f64::NAN);
                            }
                            let (p, s, e) = (
                                row_at(&point_in, row)?,
                                row_at(&start_in, row)?,
                                row_at(&end_in, row)?,
                            );
                            let probe = require_point(&p, OP)?;
                            let start = require_point(&s, OP)?;
                            let end = require_point(&e, OP)?;
                            cross_track_points(&crs, frame, probe, start, end, &state)
                                .map_err(|err| crate::note_array_row(err, row))
                        })
                        .collect::<PyResult<Vec<_>>>()
                })?,
            )
        },
        _ => Err(expected_geometry_or_array()),
    }
}
/// Initial bearing from one point to another, in degrees clockwise from north
/// (``0..360``).
///
/// Geodesic by default on a geographic CRS (on the CRS ellipsoid); grid
/// azimuth on a projected or CRS-free point. ``path='rhumb'`` selects the
/// constant compass course on a geographic CRS.
///
/// Parameters
/// ----------
/// left, right : Point or GeometryArray
///     Origin and destination; must share a CRS.
/// path : {'geodesic', 'rhumb'}, default 'geodesic'
///     Route model. Rhumb bearings require a geographic CRS.
///
/// Returns
/// -------
/// float or numpy.ndarray
///     One bearing per input pair.
///
/// Raises
/// ------
/// GeometryTypeError
///     If either operand is not a `Point`.
/// CRSMismatchError
///     If the operands' CRS or coordinate-epoch metadata differ.
/// CRSError
///     If a coordinate is outside the longitude/latitude domain.
#[pyfunction]
#[pyo3(
    signature = (left, right, *, path = NavigationPath::Geodesic),
    text_signature = "(left, right, *, path='geodesic')"
)]
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.bearing(gm.Point(0, 0), gm.Point(1, 0))
/// 90.0
pub(crate) fn bearing(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    path: NavigationPath,
) -> PyResult<Py<PyAny>> {
    match path {
        NavigationPath::Geodesic => point_binary_f64(
            py,
            left,
            right,
            "bearing",
            |crs| Ok(crs::metric_model(crs)?),
            BearingKernel,
            point_bearing,
        ),
        NavigationPath::Rhumb => point_binary_f64(
            py,
            left,
            right,
            "bearing(path='rhumb')",
            |crs| require_geographic_model(crs, "bearing(path='rhumb')"),
            RhumbBearingKernel,
            point_rhumb_bearing,
        ),
    }
}

/// Signed distance from a point to the great circle through ``start`` and
/// ``end``, in meters.
///
/// Positive when the point lies left of the directed path ``start -> end``,
/// negative right, zero on it. Spherical on the CRS ellipsoid's mean radius.
/// Geographic CRS only.
///
/// Parameters
/// ----------
/// point, start, end : Point or GeometryArray
///     The probe point and two distinct endpoints defining the directed path;
///     all must share a CRS.
///
/// Returns
/// -------
/// float or numpy.ndarray
///     Signed meters off-path.
///
/// Raises
/// ------
/// GeometryTypeError
///     If any operand is not a `Point`.
/// CRSMismatchError
///     If the operands' CRS or coordinate-epoch metadata differ.
/// CRSError
///     If the CRS is missing or not geographic.
/// InvalidGeometryError
///     If ``start`` equals ``end``.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> a, b = gm.Point(0, 0, crs=4326), gm.Point(1, 0, crs=4326)
/// >>> probe = gm.Point(0.5, 0.1, crs=4326)
/// >>> round(gm.cross_track_distance(probe, a, b) / 1000, 1)
/// 11.1
#[pyfunction]
pub(crate) fn cross_track_distance(
    py: Python<'_>,
    point: &Bound<'_, PyAny>,
    start: &Bound<'_, PyAny>,
    end: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    cross_track_broadcast(py, point, start, end)
}

pub(crate) fn reject_rhumb_unit(unit: Option<DistanceUnit>, operation: &str) -> PyResult<()> {
    if unit.is_some() {
        return Err(crate::py::errors::GeometryError::new_err(format!(
            "{operation} does not accept unit when path='rhumb'; rhumb distances are meters"
        )));
    }
    Ok(())
}

/// Return the point reached from ``point`` along ``bearing`` for ``distance``.
///
/// CRS-aware like every metric: geodesic on a geographic CRS, a planar offset
/// in native units on a projected CRS, and coordinate units when CRS-free.
/// ``path='rhumb'`` instead follows a constant compass course in meters on a
/// geographic CRS.
///
/// Parameters
/// ----------
/// point : Point or GeometryArray
///     Starting point(s).
/// bearing : float or sequence of float
///     Initial azimuth(s) in degrees clockwise from north.
/// distance : float or sequence of float
///     Distance(s) to travel (geodesic meters on a geographic CRS, native
///     units on a projected one, coordinate units otherwise).
/// path : {'geodesic', 'rhumb'}, default 'geodesic'
///     Route model. Rhumb paths require a geographic CRS and use meters.
/// unit : {'planar', 'meters'}, default None
///     Omitted follows the CRS: geodesic meters on a geographic CRS, native
///     units on a projected one, coordinate units without a CRS.
///     ``'planar'`` forces raw coordinate units (native angular units as Cartesian
///     geographic CRS — only for deliberate coordinate-space math);
///     ``'meters'`` forces the CRS metric and raises without a CRS.
///
/// Returns
/// -------
/// Point or GeometryArray
///     One destination per input point.
///
/// Raises
/// ------
/// GeometryError
///     If ``bearing`` or ``distance`` is invalid.
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
            reject_rhumb_unit(unit, "destination")?;
            rhumb_destination_point_array(py, point, bearing, distance)
        },
    }
}

/// Return a point interpolated from ``left`` towards ``right``.
///
/// CRS-aware like every metric: geodesic on a geographic CRS, planar on a
/// projected CRS, and coordinate units when CRS-free. ``path='rhumb'`` follows
/// the constant-bearing track on a geographic CRS. Z/M interpolate.
///
/// Parameters
/// ----------
/// left, right : Point or GeometryArray
///     Endpoints; must share a CRS.
/// distance : float or sequence of float
///     Distance(s) from ``left`` (or ``[0, 1]`` fractions if ``normalized``).
/// normalized : bool, default False
///     Treat ``distance`` as a fraction of the total distance.
/// path : {'geodesic', 'rhumb'}, default 'geodesic'
///     Route model. Rhumb paths require a geographic CRS and use meters.
/// unit : {'planar', 'meters'}, default None
///     Omitted follows the CRS: geodesic meters on a geographic CRS, native
///     units on a projected one, coordinate units without a CRS.
///     ``'planar'`` forces raw coordinate units (native angular units as Cartesian
///     geographic CRS — only for deliberate coordinate-space math);
///     ``'meters'`` forces the CRS metric and raises without a CRS.
///
/// Returns
/// -------
/// Point or GeometryArray
///     One interpolated point per input pair.
///
/// Raises
/// ------
/// GeometryTypeError
///     If either operand is not a `Point`.
/// CRSMismatchError
///     If the operands' CRS or coordinate-epoch metadata differ.
/// GeometryError
///     If ``distance`` is not finite.
#[pyfunction]
#[pyo3(
    signature = (left, right, distance, *, normalized = false, path = NavigationPath::Geodesic, unit = None),
    text_signature = "(left, right, distance, *, normalized=False, path='geodesic', unit=None)"
)]
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.point_between(gm.Point(0, 0), gm.Point(2, 0), 0.5).to_wkt()
/// 'POINT (0.5 0)'
pub(crate) fn point_between(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    distance: &Bound<'_, PyAny>,
    normalized: bool,
    path: NavigationPath,
    unit: Option<DistanceUnit>,
) -> PyResult<Py<PyAny>> {
    let mut plan = PointBinaryPlan::new(left, right, "point_between")?;
    let receiver_len = match plan.execution {
        PointBinaryExecution::Scalar { .. } => None,
        PointBinaryExecution::Rows(len) => Some(len),
    };
    let mut distance =
        parameter_input_with_expected(py, distance, "distance", receiver_len, &|| {
            "distance must be finite".into()
        })?;
    match plan.execution {
        PointBinaryExecution::Scalar { .. } if !distance.scalar => {
            plan.execution = PointBinaryExecution::Rows(distance.values.len());
        },
        PointBinaryExecution::Rows(len) => {
            crate::broadcast_coordinate_input(&mut distance, len, "distance")?;
        },
        PointBinaryExecution::Scalar { .. } => {},
    }
    let distances = distance.values.as_slice();
    match path {
        NavigationPath::Geodesic => point_binary_geometry_sink(
            py,
            &plan,
            move |crs| resolve_metric(crs, unit, "point_between"),
            InterpolatePointKernel {
                distance: distances,
                normalized,
            },
            move |left, right, row| {
                point_interpolate(left, right, distances[row], normalized, unit)
            },
        ),
        NavigationPath::Rhumb => {
            reject_rhumb_unit(unit, "point_between")?;
            point_binary_geometry_sink(
                py,
                &plan,
                |crs| require_geographic_model(crs, "point_between(path='rhumb')"),
                RhumbInterpolatePointKernel {
                    distance: distances,
                    normalized,
                },
                move |left, right, row| {
                    point_rhumb_between(left, right, distances[row], normalized)
                },
            )
        },
    }
}

/// Rhumb-line (loxodrome) distance between two points, in meters.
///
/// The length of the constant-bearing track on the CRS ellipsoid — the
/// navigation sibling of the (shorter) geodesic `distance`. Geographic CRS
/// only.
///
/// Parameters
/// ----------
/// left, right : Point or GeometryArray
///     Endpoints; must share a geographic CRS.
///
/// Returns
/// -------
/// float or numpy.ndarray
///     Meters along the rhumb line.
///
/// Raises
/// ------
/// GeometryTypeError
///     If either operand is not a `Point`.
/// CRSMismatchError
///     If the operands' CRS or coordinate-epoch metadata differ.
/// CRSError
///     If the CRS is missing or not geographic.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> jfk, lhr = gm.Point(-73.8, 40.6, crs=4326), gm.Point(-0.5, 51.6, crs=4326)
/// >>> round(gm.rhumb_distance(jfk, lhr) / 1000, 1)
/// 5771.1
#[pyfunction]
pub(crate) fn rhumb_distance(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    point_binary_f64(
        py,
        left,
        right,
        "rhumb_distance",
        |crs| require_geographic_model(crs, "rhumb_distance"),
        RhumbDistanceKernel,
        point_rhumb_distance,
    )
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
    let len = point_len.unwrap_or(param_len);
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
        let point = require_point(geometry, "destination(path='rhumb')")?;
        let model = require_geographic_model(geometry.crs_str(), "destination(path='rhumb')")?;
        let crs::MetricModel::Geodesic { crs, frame, state } = model else {
            unreachable!("geographic model check succeeded");
        };
        let axes = CoordinateAxes::from_point(point);
        let coords = py.detach(move || {
            let mut builder = CoordSeqBuilder::with_capacity(axes, len);
            for row in 0..len {
                let shape = rhumb_destination_point(
                    &crs,
                    frame,
                    point,
                    bearing.values[row],
                    distance.values[row],
                    &state,
                )
                .map_err(|err| crate::note_array_row(err, row))?;
                builder.push(point_from_point_nav_shape(&shape));
            }
            Ok::<_, PyErr>(builder.finish_infallible())
        })?;
        return Ok(
            PyGeometryArray::packed_points(coords, geometry.frame.clone())
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
        |crs| require_geographic_model(crs, "destination(path='rhumb')"),
        RhumbDestinationKernel {
            bearing: bearings,
            distance: distances,
        },
        move |geometry, row| point_rhumb_destination(geometry, bearings[row], distances[row]),
    )
}
