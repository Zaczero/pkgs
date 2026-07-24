#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! Point navigation free functions — bearing, destination, interpolate, rhumb.

use pyo3::prelude::*;
use pyo3::types::PyAny;

use super::super::*;
use crate::array::MissingMask;
use crate::geometry::{
    CoordSeq, CoordSeqBuilder, CoordinateAxes, HasM, HasZ, MOrdinate, ZOrdinate,
    same_topological_coordinate,
};
use crate::py::numpy::float64_array;

fn geometry_point(geometry: &PyGeometry) -> Option<Point> {
    match geometry.shape.shape() {
        Shape::Point(point) => Some(*point),
        _ => None,
    }
}

fn row_zero(err: PyErr) -> PyErr {
    crate::note_array_row(err, 0)
}

fn require_geographic(crs: Option<&str>, operation: &str) -> PyResult<String> {
    match crs::metric_model(crs)? {
        crs::MetricModel::Geodesic(crs) => Ok(crs),
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

fn rhumb_distance_points(crs: &str, from: Point, to: Point) -> PyResult<f64> {
    Ok(crs::rhumb_distance_crs(crs, from.x, from.y, to.x, to.y)?)
}

fn rhumb_bearing_points(crs: &str, from: Point, to: Point) -> PyResult<f64> {
    Ok(crs::rhumb_bearing_crs(crs, from.x, from.y, to.x, to.y)?)
}

fn destination_point<const GEODESIC: bool>(
    model: &crs::MetricModel,
    from: Point,
    bearing: f64,
    distance: f64,
) -> PyResult<Point> {
    dispatch_point_nav::<GEODESIC>(model);
    if same_topological_coordinate(distance, 0.0) {
        return Ok(from);
    }
    let (x, y) = if GEODESIC {
        let crs::MetricModel::Geodesic(crs) = model else {
            unreachable!("checked above");
        };
        crs::geodesic_destination_crs(crs, from.x, from.y, bearing, distance)?
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

fn rhumb_destination_point(crs: &str, from: Point, bearing: f64, distance: f64) -> PyResult<Shape> {
    if same_topological_coordinate(distance, 0.0) {
        return Ok(Shape::Point(from));
    }
    let Some((x, y)) = crs::rhumb_destination_crs(crs, from.x, from.y, bearing, distance)? else {
        return Err(crate::py::errors::InvalidGeometryError::new_err(
            "destination(path='rhumb') crosses a pole, where the destination longitude is \
             indeterminate",
        ));
    };
    Ok(Shape::Point(Point::new_axes(
        x,
        y,
        ZOrdinate(from.z()),
        MOrdinate(from.m()),
    )?))
}

fn interpolate_point<const GEODESIC: bool>(
    model: &crs::MetricModel,
    from: Point,
    to: Point,
    distance: f64,
    normalized: bool,
) -> PyResult<Shape> {
    dispatch_point_nav::<GEODESIC>(model);
    let total = if GEODESIC {
        let crs::MetricModel::Geodesic(crs) = model else {
            unreachable!("checked above");
        };
        crs::geodesic_distance_crs(crs, from.x, from.y, to.x, to.y)?
    } else {
        let crs::MetricModel::Planar { to_metre } = model else {
            unreachable!("checked above");
        };
        (to.x - from.x).hypot(to.y - from.y) * to_metre.get()
    };
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
    let (x, y) = if GEODESIC {
        let crs::MetricModel::Geodesic(crs) = model else {
            unreachable!("checked above");
        };
        crs::geodesic_interpolate_crs(crs, from.x, from.y, to.x, to.y, ratio)?
    } else {
        (
            from.x + (to.x - from.x) * ratio,
            from.y + (to.y - from.y) * ratio,
        )
    };
    Ok(Shape::Point(Point::new_axes(
        x,
        y,
        ZOrdinate(interpolate_optional_axis(from.z(), to.z(), ratio)),
        MOrdinate(interpolate_optional_axis(from.m(), to.m(), ratio)),
    )?))
}

fn rhumb_interpolate_point(
    crs: &str,
    from: Point,
    to: Point,
    distance: f64,
    normalized: bool,
) -> PyResult<Shape> {
    let total = crs::rhumb_distance_crs(crs, from.x, from.y, to.x, to.y)?;
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
    let bearing = crs::rhumb_bearing_crs(crs, from.x, from.y, to.x, to.y)?;
    let Some((x, y)) = crs::rhumb_destination_crs(crs, from.x, from.y, bearing, total * ratio)?
    else {
        return Err(crate::py::errors::InvalidGeometryError::new_err(
            "point_between(path='rhumb') crosses a pole, where the destination longitude is \
             indeterminate",
        ));
    };
    Ok(Shape::Point(Point::new_axes(
        x,
        y,
        ZOrdinate(interpolate_optional_axis(from.z(), to.z(), ratio)),
        MOrdinate(interpolate_optional_axis(from.m(), to.m(), ratio)),
    )?))
}

fn cross_track_points(crs: &str, probe: Point, from: Point, to: Point) -> PyResult<f64> {
    if (from.x, from.y).eq(&(to.x, to.y)) {
        return Err(crate::py::errors::InvalidGeometryError::new_err(
            "cross_track_distance requires distinct start and end points",
        ));
    }
    Ok(crs::geodesic_cross_track_crs(
        crs,
        (probe.x, probe.y),
        (from.x, from.y),
        (to.x, to.y),
    )?)
}

fn cross_track_points_with_radius(
    radius: f64,
    probe: Point,
    from: Point,
    to: Point,
) -> PyResult<f64> {
    if (from.x, from.y).eq(&(to.x, to.y)) {
        return Err(crate::py::errors::InvalidGeometryError::new_err(
            "cross_track_distance requires distinct start and end points",
        ));
    }
    Ok(crs::geodesic_cross_track_with_radius(
        radius,
        (probe.x, probe.y),
        (from.x, from.y),
        (to.x, to.y),
    )?)
}

enum PointOperand<'a> {
    One(Point),
    Many {
        points: PointRows<'a>,
        missing: Option<&'a MissingMask>,
    },
}

impl PointOperand<'_> {
    fn get(&self, row: usize) -> Point {
        match self {
            Self::One(point) => *point,
            Self::Many { points, .. } => points.get(row),
        }
    }

    const fn missing(&self) -> Option<&MissingMask> {
        match self {
            Self::One(_) => None,
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
    match (left, right) {
        (PointOperand::One(left), PointOperand::One(right)) => sink.run(left, right),
        (PointOperand::One(left), PointOperand::Many { points: right, .. }) => {
            sink.run(left, right)
        },
        (PointOperand::Many { points: left, .. }, PointOperand::One(right)) => {
            sink.run(left, right)
        },
        (PointOperand::Many { points: left, .. }, PointOperand::Many { points: right, .. }) => {
            sink.run(left, right)
        },
    }
}

fn input_len(input: &GeometryInput<'_>) -> Option<usize> {
    match input {
        GeometryInput::One(_) => None,
        GeometryInput::Many(array) => Some(array.storage().len()),
    }
}

fn input_point<'a>(input: &'a GeometryInput<'a>) -> Option<PointOperand<'a>> {
    match input {
        GeometryInput::One(geometry) => geometry_point(geometry).map(PointOperand::One),
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

fn point_from_point_nav_shape(shape: &Shape) -> Point {
    match shape {
        Shape::Point(point) => *point,
        _ => unreachable!("point-navigation point fast paths return Point shapes"),
    }
}

fn point_nav_missing_point_with_axes(axes: CoordinateAxes) -> Point {
    Point::new_unchecked_axes(
        f64::NAN,
        f64::NAN,
        ZOrdinate(axes.has_z().then_some(f64::NAN)),
        MOrdinate(axes.has_m().then_some(f64::NAN)),
    )
}

fn is_missing(mask: Option<&MissingMask>, row: usize) -> bool {
    mask.is_some_and(|mask| mask[row])
}

fn row_missing<const HAS_MISSING: bool>(missing: Option<&[bool]>, row: usize) -> bool {
    if HAS_MISSING {
        missing.expect("HAS_MISSING rows carry a mask")[row]
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
    missing: Option<&[bool]>,
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
    missing: Option<&[bool]>,
) -> Option<CoordinateAxes> {
    if let Some(mask) = missing {
        return uniform_point_axes::<true>(points, len, Some(mask));
    }
    uniform_point_axes::<false>(points, len, None)
}

fn dispatch_point_nav<const GEODESIC: bool>(model: &crs::MetricModel) {
    match (GEODESIC, model) {
        (true, crs::MetricModel::Geodesic(_)) | (false, crs::MetricModel::Planar { .. }) => {},
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
            let crs::MetricModel::Geodesic(crs) = model else {
                unreachable!("checked above");
            };
            Ok(crs::geodesic_bearing_crs(crs, from.x, from.y, to.x, to.y)?)
        } else {
            Ok(bearing_degrees(to.x - from.x, to.y - from.y))
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
        let crs::MetricModel::Geodesic(crs) = model else {
            unreachable!("require_geographic returns geodesic");
        };
        rhumb_distance_points(crs, left, right)
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
        let crs::MetricModel::Geodesic(crs) = model else {
            unreachable!("require_geographic returns geodesic");
        };
        rhumb_bearing_points(crs, left, right)
    }
}

fn point_binary_f64_eval<K: PointBinaryF64Kernel>(
    kernel: K,
    model: &crs::MetricModel,
    left: Point,
    right: Point,
) -> PyResult<f64> {
    match model {
        crs::MetricModel::Geodesic(_) => kernel.eval::<true>(model, left, right),
        crs::MetricModel::Planar { .. } => kernel.eval::<false>(model, left, right),
    }
}

fn point_binary_f64_rows<const GEODESIC: bool, const HAS_MISSING: bool, K: PointBinaryF64Kernel>(
    kernel: K,
    model: &crs::MetricModel,
    len: usize,
    missing: Option<&[bool]>,
    mut points: impl FnMut(usize) -> (Point, Point),
) -> PyResult<Vec<f64>> {
    dispatch_point_nav::<GEODESIC>(model);
    (0..len)
        .map(|row| {
            if row_missing::<HAS_MISSING>(missing, row) {
                return Ok(f64::NAN);
            }
            let (left, right) = points(row);
            kernel
                .eval::<GEODESIC>(model, left, right)
                .map_err(|err| crate::note_array_row(err, row))
        })
        .collect()
}

fn point_binary_f64_rows_dispatch<K: PointBinaryF64Kernel>(
    kernel: K,
    model: &crs::MetricModel,
    len: usize,
    missing: Option<&[bool]>,
    points: impl FnMut(usize) -> (Point, Point),
) -> PyResult<Vec<f64>> {
    match (model, missing) {
        (crs::MetricModel::Geodesic(_), Some(mask)) => {
            point_binary_f64_rows::<true, true, _>(kernel, model, len, Some(mask), points)
        },
        (crs::MetricModel::Geodesic(_), None) => {
            point_binary_f64_rows::<true, false, _>(kernel, model, len, None, points)
        },
        (crs::MetricModel::Planar { .. }, Some(mask)) => {
            point_binary_f64_rows::<false, true, _>(kernel, model, len, Some(mask), points)
        },
        (crs::MetricModel::Planar { .. }, None) => {
            point_binary_f64_rows::<false, false, _>(kernel, model, len, None, points)
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
        let crs::MetricModel::Geodesic(crs) = model else {
            unreachable!("require_geographic returns geodesic");
        };
        rhumb_interpolate_point(crs, left, right, self.distance[row], self.normalized)
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
}

fn point_binary_geometry_eval<K: PointBinaryGeometryKernel>(
    kernel: K,
    model: &crs::MetricModel,
    row: usize,
    left: Point,
    right: Point,
) -> PyResult<Shape> {
    match model {
        crs::MetricModel::Geodesic(_) => kernel.eval::<true>(model, row, left, right),
        crs::MetricModel::Planar { .. } => kernel.eval::<false>(model, row, left, right),
    }
}

fn point_binary_geometry_coords<
    const GEODESIC: bool,
    const HAS_MISSING: bool,
    K: PointBinaryGeometryKernel,
>(
    kernel: K,
    model: &crs::MetricModel,
    len: usize,
    axes: CoordinateAxes,
    missing: Option<&[bool]>,
    mut points: impl FnMut(usize) -> (Point, Point),
) -> PyResult<CoordSeq> {
    dispatch_point_nav::<GEODESIC>(model);
    let mut builder = CoordSeqBuilder::with_capacity(axes, len);
    for row in 0..len {
        if row_missing::<HAS_MISSING>(missing, row) {
            builder.push(point_nav_missing_point_with_axes(axes));
        } else {
            let (left, right) = points(row);
            let shape = kernel
                .eval::<GEODESIC>(model, row, left, right)
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
    missing: Option<&[bool]>,
    points: impl FnMut(usize) -> (Point, Point),
) -> PyResult<CoordSeq> {
    match (model, missing) {
        (crs::MetricModel::Geodesic(_), Some(mask)) => {
            point_binary_geometry_coords::<true, true, _>(
                kernel,
                model,
                len,
                axes,
                Some(mask),
                points,
            )
        },
        (crs::MetricModel::Geodesic(_), None) => {
            point_binary_geometry_coords::<true, false, _>(kernel, model, len, axes, None, points)
        },
        (crs::MetricModel::Planar { .. }, Some(mask)) => {
            point_binary_geometry_coords::<false, true, _>(
                kernel,
                model,
                len,
                axes,
                Some(mask),
                points,
            )
        },
        (crs::MetricModel::Planar { .. }, None) => {
            point_binary_geometry_coords::<false, false, _>(kernel, model, len, axes, None, points)
        },
    }
}

fn uniform_point_binary_geometry_axes<const HAS_MISSING: bool, K: PointBinaryGeometryKernel>(
    kernel: K,
    len: usize,
    missing: Option<&[bool]>,
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
    missing: Option<&[bool]>,
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
        let crs::MetricModel::Geodesic(crs) = model else {
            unreachable!("require_geographic returns geodesic");
        };
        rhumb_destination_point(crs, point, self.bearing[row], self.distance[row])
    }
}

fn point_unary_geometry_eval<K: PointUnaryGeometryKernel>(
    kernel: K,
    model: &crs::MetricModel,
    row: usize,
    point: Point,
) -> PyResult<Shape> {
    match model {
        crs::MetricModel::Geodesic(_) => kernel.eval::<true>(model, row, point),
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
    missing: Option<&[bool]>,
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
    missing: Option<&[bool]>,
) -> PyResult<CoordSeq> {
    match (model, missing) {
        (crs::MetricModel::Geodesic(_), Some(mask)) => {
            point_unary_geometry_coords::<true, true, _>(
                kernel,
                model,
                points,
                len,
                axes,
                Some(mask),
            )
        },
        (crs::MetricModel::Geodesic(_), None) => {
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

fn destination_many_coords<const GEODESIC: bool, const HAS_MISSING: bool>(
    kernel: DestinationPointKernel<'_>,
    model: &crs::MetricModel,
    points: &PointRows<'_>,
    len: usize,
    axes: CoordinateAxes,
    missing: Option<&[bool]>,
) -> PyResult<CoordSeq> {
    dispatch_point_nav::<GEODESIC>(model);
    let mut builder = CoordSeqBuilder::with_capacity(axes, len);
    for row in 0..len {
        if row_missing::<HAS_MISSING>(missing, row) {
            builder.push(point_nav_missing_point_with_axes(axes));
        } else {
            builder.try_push(
                kernel
                    .eval_row::<GEODESIC>(model, row, points.get(row))
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
    missing: Option<&[bool]>,
) -> PyResult<CoordSeq> {
    match (model, missing) {
        (crs::MetricModel::Geodesic(_), Some(mask)) => {
            destination_many_coords::<true, true>(kernel, model, points, len, axes, Some(mask))
        },
        (crs::MetricModel::Geodesic(_), None) => {
            destination_many_coords::<true, false>(kernel, model, points, len, axes, None)
        },
        (crs::MetricModel::Planar { .. }, Some(mask)) => {
            destination_many_coords::<false, true>(kernel, model, points, len, axes, Some(mask))
        },
        (crs::MetricModel::Planar { .. }, None) => {
            destination_many_coords::<false, false>(kernel, model, points, len, axes, None)
        },
    }
}

fn destination_scalar_coords<const GEODESIC: bool>(
    model: &crs::MetricModel,
    point: Point,
    bearing: &[f64],
    distance: &[f64],
    len: usize,
    axes: CoordinateAxes,
) -> PyResult<CoordSeq> {
    dispatch_point_nav::<GEODESIC>(model);
    let mut builder = CoordSeqBuilder::with_capacity(axes, len);
    for row in 0..len {
        builder.try_push(
            destination_point::<GEODESIC>(model, point, bearing[row], distance[row])
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
        crs::MetricModel::Geodesic(_) => {
            destination_scalar_coords::<true>(model, point, bearing, distance, len, axes)
        },
        crs::MetricModel::Planar { .. } => {
            destination_scalar_coords::<false>(model, point, bearing, distance, len, axes)
        },
    }
}

fn cross_track_point_rows<const HAS_MISSING: bool>(
    radius: f64,
    points: &PointOperand<'_>,
    starts: &PointOperand<'_>,
    ends: &PointOperand<'_>,
    len: usize,
    missing: Option<&[bool]>,
) -> PyResult<Vec<f64>> {
    (0..len)
        .map(|row| {
            if row_missing::<HAS_MISSING>(missing, row) {
                return Ok(f64::NAN);
            }
            let (p, s, e) = (points.get(row), starts.get(row), ends.get(row));
            cross_track_points_with_radius(radius, p, s, e)
                .map_err(|err| crate::note_array_row(err, row))
        })
        .collect()
}

fn cross_track_point_rows_dispatch(
    radius: f64,
    points: &PointOperand<'_>,
    starts: &PointOperand<'_>,
    ends: &PointOperand<'_>,
    len: usize,
    missing: Option<&[bool]>,
) -> PyResult<Vec<f64>> {
    missing.map_or_else(
        || cross_track_point_rows::<false>(radius, points, starts, ends, len, None),
        |mask| cross_track_point_rows::<true>(radius, points, starts, ends, len, Some(mask)),
    )
}

const fn input_missing<'a>(input: &'a GeometryInput<'_>) -> Option<&'a MissingMask> {
    match input {
        GeometryInput::One(_) => None,
        GeometryInput::Many(array) => array.missing(),
    }
}

fn union_missing_masks(inputs: &[&GeometryInput<'_>]) -> Option<MissingMask> {
    crate::array::missing::union_many(inputs.iter().map(|input| input_missing(input)))
}

fn union_point_operand_missing(inputs: &[&PointOperand<'_>]) -> Option<MissingMask> {
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

    const fn missing(self) -> Option<&'a MissingMask> {
        match self {
            Self::One(_) => None,
            Self::Many(array) => array.missing(),
        }
    }

    fn point_operand(self) -> Option<PointOperand<'a>> {
        match self {
            Self::One(geometry) => geometry_point(geometry).map(PointOperand::One),
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
    missing: Option<MissingMask>,
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
                PointBinaryExecution::Rows(array.storage().len())
            },
            (PointBinaryInput::Many(left), PointBinaryInput::Many(right)) => {
                // Length wins over frame compatibility for strict array broadcasting.
                ensure_same_len(left.storage().len(), right.storage().len())?;
                PointBinaryExecution::Rows(left.storage().len())
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
            (PointBinaryRowInput::One(left), PointBinaryRowInput::Many(array)) => operation(
                left,
                &array
                    .storage()
                    .geometry_at(row, array.frame.clone(), array.row_frame_cache(row)),
            ),
            (PointBinaryRowInput::Many(array), PointBinaryRowInput::One(right)) => operation(
                &array
                    .storage()
                    .geometry_at(row, array.frame.clone(), array.row_frame_cache(row)),
                right,
            ),
            (PointBinaryRowInput::Many(left), PointBinaryRowInput::Many(right)) => operation(
                &left
                    .storage()
                    .geometry_at(row, left.frame.clone(), left.row_frame_cache(row)),
                &right
                    .storage()
                    .geometry_at(row, right.frame.clone(), right.row_frame_cache(row)),
            ),
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
    let len = array.storage().len();
    if len == 0 {
        return Ok(
            PyGeometryArray::from_shapes(Vec::new(), array.frame.clone())
                .into_pyobject(py)?
                .unbind()
                .into(),
        );
    }
    let Some(points) = array.storage().point_rows() else {
        let frame = array.frame.clone();
        let shapes = py.detach(move || {
            array
                .masked_shape_rows()
                .enumerate()
                .map(|(row, (missing, shape))| {
                    if missing {
                        return Ok(PyGeometryArray::missing_placeholder());
                    }
                    let geometry = PyGeometry::with_frame(shape.into_owned(), frame.clone());
                    point_destination(&geometry, bearing.values[row], distance.values[row], unit)
                        .map_err(|err| crate::note_array_row(err, row))
                })
                .collect::<PyResult<Vec<_>>>()
        })?;
        return Ok(PyGeometryArray::from_shapes(shapes, array.frame.clone())
            .with_missing_mask(array.missing().cloned())
            .into_pyobject(py)?
            .unbind()
            .into());
    };
    let missing = array.missing().cloned();
    let Some(axes) = uniform_point_axes_dispatch(&points, len, missing.as_deref()) else {
        let frame = array.frame.clone();
        let shapes = py.detach(move || {
            array
                .masked_shape_rows()
                .enumerate()
                .map(|(row, (missing, shape))| {
                    if missing {
                        return Ok(PyGeometryArray::missing_placeholder());
                    }
                    let geometry = PyGeometry::with_frame(shape.into_owned(), frame.clone());
                    point_destination(&geometry, bearing.values[row], distance.values[row], unit)
                        .map_err(|err| crate::note_array_row(err, row))
                })
                .collect::<PyResult<Vec<_>>>()
        })?;
        return Ok(PyGeometryArray::from_shapes(shapes, array.frame.clone())
            .with_missing_mask(missing)
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
            missing_for_rows.as_deref(),
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
    let mut bearing = coordinate_input(py, bearing, "bearing")?;
    let mut distance = coordinate_input(py, distance, "distance")?;
    let param_len = broadcast_coordinate_group(
        [(&mut bearing, "bearing"), (&mut distance, "distance")],
        "bearing and distance",
    )?;
    let point_len = input_len(&point_in);
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
                crs::MetricModel::Geodesic(_) => {
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

struct PointBinaryNumericRowsSink<'py, K> {
    py: Python<'py>,
    model: crs::MetricModel,
    len: usize,
    missing: Option<MissingMask>,
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
                point_binary_f64_rows_dispatch(kernel, &model, len, missing.as_deref(), |row| {
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
            plan.missing.as_deref(),
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
                missing_for_rows.as_deref(),
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
            if let Some(points) = array.storage().point_rows() {
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
                    uniform_point_axes_dispatch(&points, points.len(), missing.as_deref())
                else {
                    let output_frame = array.frame.clone();
                    let frame = output_frame.clone();
                    let missing_mask = array.missing().cloned();
                    let shapes = py.detach(move || {
                        array
                            .masked_shape_rows()
                            .enumerate()
                            .map(|(row, (missing, shape))| {
                                if missing {
                                    return Ok(PyGeometryArray::missing_placeholder());
                                }
                                let geometry =
                                    PyGeometry::with_frame(shape.into_owned(), frame.clone());
                                fallback(&geometry, row)
                                    .map_err(|err| crate::note_array_row(err, row))
                            })
                            .collect::<PyResult<Vec<_>>>()
                    })?;
                    return Ok(PyGeometryArray::from_shapes(shapes, output_frame)
                        .with_missing_mask(missing_mask)
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
                        missing_for_rows.as_deref(),
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
            let missing_mask = array.missing().cloned();
            let shapes = py.detach(move || {
                array
                    .masked_shape_rows()
                    .enumerate()
                    .map(|(row, (missing, shape))| {
                        if missing {
                            return Ok(PyGeometryArray::missing_placeholder());
                        }
                        let geometry = PyGeometry::with_frame(shape.into_owned(), frame.clone());
                        fallback(&geometry, row).map_err(|err| crate::note_array_row(err, row))
                    })
                    .collect::<PyResult<Vec<_>>>()
            })?;
            Ok(PyGeometryArray::from_shapes(shapes, output_frame)
                .with_missing_mask(missing_mask)
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
        .filter_map(input_len)
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
            Many(array) => {
                array
                    .storage()
                    .geometry_at(row, array.frame.clone(), array.row_frame_cache(row))
            },
        })
    };
    match (&point_in, &start_in, &end_in) {
        (One(p), One(s), One(e)) if broadcast_len.is_none() => {
            p.frame.compatible(&s.frame, OP)?;
            p.frame.compatible(&e.frame, OP)?;
            match (geometry_point(p), geometry_point(s), geometry_point(e)) {
                (Some(probe), Some(start), Some(end)) => {
                    let crs = require_geographic(p.crs_str(), OP)?;
                    cross_track_points(&crs, probe, start, end)?.into_py_any(py)
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
                let crs = require_geographic(input_crs_str(&point_in), OP).map_err(row_zero)?;
                let radius = crs::geodesic_cross_track_radius_crs(&crs)
                    .map_err(PyErr::from)
                    .map_err(row_zero)?;
                let missing_for_rows = union_point_operand_missing(&[&points, &starts, &ends]);
                return float64_array(
                    py,
                    py.detach(move || {
                        cross_track_point_rows_dispatch(
                            radius,
                            &points,
                            &starts,
                            &ends,
                            len,
                            missing_for_rows.as_deref(),
                        )
                    })?,
                );
            }
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
                            op(&p, &s, &e).map_err(|err| crate::note_array_row(err, row))
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
            |crs| require_geographic(crs, "bearing(path='rhumb')").map(crs::MetricModel::Geodesic),
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

fn reject_rhumb_unit(unit: Option<DistanceUnit>, operation: &str) -> PyResult<()> {
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
///     ``'planar'`` forces raw coordinate units (degrees-as-Cartesian on a
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
#[pyfunction]
#[pyo3(
    signature = (point, bearing, distance, *, path = NavigationPath::Geodesic, unit = None),
    text_signature = "(point, bearing, distance, *, path='geodesic', unit=None)"
)]
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.destination(gm.Point(0, 0, crs=4326), distance=1000, bearing=90).to_wkt(precision=5)
/// 'POINT (0.00898 0)'
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
///     ``'planar'`` forces raw coordinate units (degrees-as-Cartesian on a
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
    let mut distance = coordinate_input(py, distance, "distance")?;
    let mut plan = PointBinaryPlan::new(left, right, "point_between")?;
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
                |crs| {
                    require_geographic(crs, "point_between(path='rhumb')")
                        .map(crs::MetricModel::Geodesic)
                },
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
        |crs| require_geographic(crs, "rhumb_distance").map(crs::MetricModel::Geodesic),
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
    let mut bearing = coordinate_input(py, bearing, "bearing")?;
    let mut distance = coordinate_input(py, distance, "distance")?;
    let param_len = broadcast_coordinate_group(
        [(&mut bearing, "bearing"), (&mut distance, "distance")],
        "bearing and distance",
    )?;
    let point_len = input_len(&point_input);
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
        let crs = require_geographic(geometry.crs_str(), "destination(path='rhumb')")?;
        let axes = CoordinateAxes::from_point(point);
        let coords = py.detach(move || {
            let mut builder = CoordSeqBuilder::with_capacity(axes, len);
            for row in 0..len {
                let shape =
                    rhumb_destination_point(&crs, point, bearing.values[row], distance.values[row])
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
        |crs| require_geographic(crs, "destination(path='rhumb')").map(crs::MetricModel::Geodesic),
        RhumbDestinationKernel {
            bearing: bearings,
            distance: distances,
        },
        move |geometry, row| point_rhumb_destination(geometry, bearings[row], distances[row]),
    )
}
