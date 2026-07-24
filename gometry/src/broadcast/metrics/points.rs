#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use super::*;
use crate::geometry::{MOrdinate, ZOrdinate};
use crate::py::errors::InvalidGeometryError;

/// Initial bearing (degrees clockwise from north, `0..360`) from one point to
/// another, geodesic on a geographic CRS or grid azimuth otherwise.
pub(crate) fn point_bearing(geometry: &PyGeometry, other: &PyGeometry) -> PyResult<f64> {
    let from = require_point(geometry, "bearing")?;
    let to = require_point(other, "bearing")?;
    geometry.frame.compatible(&other.frame, "bearing")?;
    Ok(match crs::metric_model(geometry.crs_str())? {
        crs::MetricModel::Planar { .. } => {
            crate::measures::bearing_degrees(to.x - from.x, to.y - from.y)
        },
        crs::MetricModel::Geodesic(crs) => {
            crs::geodesic_bearing_crs(&crs, from.x, from.y, to.x, to.y)?
        },
    })
}

/// Signed spherical cross-track distance (meters) from a point to the
/// great circle through `start -> end`; geographic CRS only.
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
    // Identical start/end leaves the path direction undefined (bit
    // equality; signed zeros compare equal through `==` semantics here).
    if (from.x, from.y).eq(&(to.x, to.y)) {
        return Err(crate::py::errors::InvalidGeometryError::new_err(
            "cross_track_distance requires distinct start and end points",
        ));
    }
    match crs::metric_model(geometry.crs_str())? {
        crs::MetricModel::Geodesic(crs) => Ok(crs::geodesic_cross_track_crs(
            &crs,
            (probe.x, probe.y),
            (from.x, from.y),
            (to.x, to.y),
        )?),
        crs::MetricModel::Planar { .. } => Err(crate::py::errors::CRSError::new_err(
            "cross_track_distance requires a geographic CRS; use set_crs(...) or to_crs(...) \
             to attach one",
        )),
    }
}

/// Resolve the geographic CRS behind a rhumb operation, with the shared
/// "attach a CRS" guidance when the point is projected or CRS-free.
fn require_geographic(geometry: &PyGeometry, operation: &str) -> PyResult<String> {
    match crs::metric_model(geometry.crs_str())? {
        crs::MetricModel::Geodesic(crs) => Ok(crs),
        crs::MetricModel::Planar { .. } => Err(crate::py::errors::CRSError::new_err(format!(
            "{operation} requires a geographic CRS; use set_crs(...) or to_crs(...) to attach one"
        ))),
    }
}

/// Rhumb-line distance (meters) between two points; geographic CRS only.
pub(crate) fn point_rhumb_distance(geometry: &PyGeometry, other: &PyGeometry) -> PyResult<f64> {
    let from = require_point(geometry, "rhumb_distance")?;
    let to = require_point(other, "rhumb_distance")?;
    geometry.frame.compatible(&other.frame, "rhumb_distance")?;
    let crs = require_geographic(geometry, "rhumb_distance")?;
    Ok(crs::rhumb_distance_crs(&crs, from.x, from.y, to.x, to.y)?)
}

/// Constant rhumb bearing (degrees clockwise from north, `0..360`) between
/// two points; geographic CRS only.
pub(crate) fn point_rhumb_bearing(geometry: &PyGeometry, other: &PyGeometry) -> PyResult<f64> {
    let from = require_point(geometry, "bearing(path='rhumb')")?;
    let to = require_point(other, "bearing(path='rhumb')")?;
    geometry
        .frame
        .compatible(&other.frame, "bearing(path='rhumb')")?;
    let crs = require_geographic(geometry, "bearing(path='rhumb')")?;
    Ok(crs::rhumb_bearing_crs(&crs, from.x, from.y, to.x, to.y)?)
}

/// The point reached along a constant `bearing` for `distance` meters;
/// geographic CRS only. Preserves Z/M.
pub(crate) fn point_rhumb_destination(
    geometry: &PyGeometry,
    bearing: f64,
    distance: f64,
) -> PyResult<Shape> {
    let from = require_point(geometry, "destination(path='rhumb')")?;
    let crs = require_geographic(geometry, "destination(path='rhumb')")?;
    let Some((x, y)) = crs::rhumb_destination_crs(&crs, from.x, from.y, bearing, distance)? else {
        return Err(InvalidGeometryError::new_err(
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

/// A point between two endpoints along their rhumb track. Geographic CRS only.
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
    let crs = require_geographic(geometry, "point_between(path='rhumb')")?;
    let total = crs::rhumb_distance_crs(&crs, from.x, from.y, to.x, to.y)?;
    let ratio = if normalized {
        distance
    } else if total == 0.0 {
        0.0
    } else {
        distance / total
    }
    .clamp(0.0, 1.0);
    if crate::geometry::same_topological_coordinate(ratio, 0.0) {
        return Ok(Shape::Point(from));
    }
    if crate::geometry::same_topological_coordinate(ratio, 1.0) {
        return Ok(Shape::Point(to));
    }
    let bearing = crs::rhumb_bearing_crs(&crs, from.x, from.y, to.x, to.y)?;
    let Some((x, y)) = crs::rhumb_destination_crs(&crs, from.x, from.y, bearing, total * ratio)?
    else {
        return Err(InvalidGeometryError::new_err(
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

/// The point reached from `geometry` along `bearing` for `distance` (geodesic
/// meters on a geographic CRS, planar offset otherwise). Preserves Z/M.
pub(crate) fn point_destination(
    geometry: &PyGeometry,
    bearing: f64,
    distance: f64,
    unit: Option<DistanceUnit>,
) -> PyResult<Shape> {
    let from = require_point(geometry, "destination")?;
    let (x, y) = match resolve_metric(geometry.crs_str(), unit, "destination")? {
        crs::MetricModel::Planar { to_metre } => {
            let radians = bearing.to_radians();
            // `distance` is meters; convert to coordinate units for the offset.
            let step = distance / to_metre.get();
            (step * radians.sin() + from.x, step * radians.cos() + from.y)
        },
        crs::MetricModel::Geodesic(crs) => {
            crs::geodesic_destination_crs(&crs, from.x, from.y, bearing, distance)?
        },
    };
    Ok(Shape::Point(Point::new_axes(
        x,
        y,
        ZOrdinate(from.z()),
        MOrdinate(from.m()),
    )?))
}

/// A point interpolated from `geometry` towards `other` by `distance` (or a
/// `[0, 1]` fraction when `normalized`). Geodesic on a geographic CRS, else
/// straight-line. Interpolates Z/M.
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
    // Total length in meters so an absolute meter `distance` is consistent with
    // it (the planar coordinate length is scaled by `to_metre`).
    let total = match &model {
        crs::MetricModel::Planar { to_metre } => {
            (to.x - from.x).hypot(to.y - from.y) * to_metre.get()
        },
        crs::MetricModel::Geodesic(crs) => {
            crs::geodesic_distance_crs(crs, from.x, from.y, to.x, to.y)?
        },
    };
    let ratio = if normalized {
        distance
    } else if total == 0.0 {
        0.0
    } else {
        distance / total
    }
    .clamp(0.0, 1.0);
    let (x, y) = match &model {
        // Plain lerp: scalar `mul_add` is a libm call below x86-64-v3.
        crs::MetricModel::Planar { .. } => (
            from.x + (to.x - from.x) * ratio,
            from.y + (to.y - from.y) * ratio,
        ),
        crs::MetricModel::Geodesic(crs) => {
            crs::geodesic_interpolate_crs(crs, from.x, from.y, to.x, to.y, ratio)?
        },
    };
    let point = Point::new_axes(
        x,
        y,
        ZOrdinate(interpolate_optional_axis(from.z(), to.z(), ratio)),
        MOrdinate(interpolate_optional_axis(from.m(), to.m(), ratio)),
    )?;
    Ok(Shape::Point(point))
}

pub(crate) fn interpolate_optional_axis(
    start: Option<f64>,
    end: Option<f64>,
    ratio: f64,
) -> Option<f64> {
    // Plain lerp: scalar `mul_add` is a libm call below x86-64-v3.
    let start = start?;
    Some(start + (end? - start) * ratio)
}

#[cfg(test)]
mod dwithin_parity_tests {
    use super::*;
    use crate::geometry::{point_distance, point_distance_squared};

    fn point(x: f64, y: f64) -> Point {
        Point::new_unchecked_xy(x, y)
    }

    fn scalar_point_dwithin(a: Point, b: Point, limit: f64) -> bool {
        Shape::Point(a).dwithin(&Shape::Point(b), limit)
    }

    fn packed_planar_dwithin(a: Point, b: Point, limit: f64) -> bool {
        point_distance_squared(a, b) <= limit * limit
    }

    fn shape_data_dwithin(a: Point, b: Point, limit: f64) -> bool {
        ShapeData::new(Shape::Point(a)).dwithin(&ShapeData::new(Shape::Point(b)), limit)
    }

    #[test]
    fn planar_dwithin_scalar_packed_and_shape_data_agree() {
        let coords = [-1e6, -3.0, -1.0, 0.0, 1.0, 3.0, 4.0, 1e6];
        for &ax in &coords {
            for &ay in &coords {
                for &bx in &coords {
                    for &by in &coords {
                        let (a, b) = (point(ax, ay), point(bx, by));
                        let dist = point_distance(a, b);
                        let limits = [
                            0.0,
                            f64::MIN_POSITIVE,
                            dist * 0.5,
                            dist,
                            dist.next_down(),
                            dist.next_up(),
                            dist * 2.0,
                            f64::INFINITY,
                        ];
                        for limit in limits {
                            let scalar = scalar_point_dwithin(a, b, limit);
                            let packed = packed_planar_dwithin(a, b, limit);
                            let data = shape_data_dwithin(a, b, limit);
                            assert_eq!(
                                scalar, packed,
                                "scalar vs packed at ({ax},{ay})-({bx},{by}) limit={limit}"
                            );
                            assert_eq!(
                                scalar, data,
                                "scalar vs ShapeData at ({ax},{ay})-({bx},{by}) limit={limit}"
                            );
                        }
                    }
                }
            }
        }
    }
}
