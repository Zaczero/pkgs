use geographiclib_rs::Geodesic;

use crate::crs::RuntimeState;
use crate::crs::geodesic::{
    CrsError, DirectGeodesic as _, EllipsoidMetric, GeodesicDirectInfo,
    ensure_geographic_domain_in_frame, geographic_metric_frame_at, inverse_distance, normalize,
    with_geodesic_at, with_geographic_metric_erased_at,
};
use crate::error::Result;
use crate::geographic::GeographicFrame;
use crate::geometry::{
    ChartMetric, Coordinates, Point, Shape, canonicalize_zero, same_topological_coordinate,
};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum DistanceMode {
    Absolute,
    Normalized,
}

impl DistanceMode {
    pub(crate) const fn of(normalized: bool) -> Self {
        if normalized {
            Self::Normalized
        } else {
            Self::Absolute
        }
    }

    pub(crate) fn is_normalized(self) -> bool {
        self == Self::Normalized
    }

    pub(crate) fn planar_along(self, distance: f64, to_metre: f64) -> f64 {
        match self {
            Self::Normalized => distance,
            Self::Absolute => distance / to_metre,
        }
    }
}

impl From<bool> for DistanceMode {
    fn from(normalized: bool) -> Self {
        Self::of(normalized)
    }
}

/// Run `f` against the ellipsoid metric for `crs`.
///
/// Centralizes the geographic-CRS check, the lon/lat domain validation of every
/// input `shape`, the thread-cache refresh, CRS normalization, and the cached
/// [`Geodesic`] lookup — the single entry point the geodesic metric ops
/// (`geodesic_hausdorff`/`_nearest`/…) share, so none re-implements the
/// validation and caching boilerplate.
#[cfg(test)]
pub(crate) fn with_ellipsoid_metric<T>(
    crs: &str,
    shapes: &[&Shape],
    f: impl FnOnce(&EllipsoidMetric<'_>) -> Result<T>,
) -> Result<T> {
    let state = crate::crs::runtime_state_snapshot()?;
    with_ellipsoid_metric_at(crs, shapes, &state, f)
}

pub(crate) fn with_ellipsoid_metric_at<T>(
    crs: &str,
    shapes: &[&Shape],
    state: &RuntimeState,
    f: impl FnOnce(&EllipsoidMetric<'_>) -> Result<T>,
) -> Result<T> {
    with_resolved_ellipsoid_metric_at(crs, shapes, state, |_, metric| f(metric))
}

/// Run `f` against the normalized CRS string and resolved ellipsoid metric.
///
/// This is the broadcast-friendly sibling of [`with_ellipsoid_metric`]: the
/// CRS kind/unit checks, input lon/lat validation, PROJ cache refresh,
/// normalization, and geodesic lookup happen once outside the row loop.
pub(crate) fn with_resolved_ellipsoid_metric_at<T>(
    crs: &str,
    shapes: &[&Shape],
    state: &RuntimeState,
    f: impl FnOnce(&str, &EllipsoidMetric<'_>) -> Result<T>,
) -> Result<T> {
    with_geographic_metric_erased_at(crs, shapes, state, |error| error, f)
}

pub(crate) fn chart_shape_area(
    metric: &impl ChartMetric,
    shape: &Shape,
    frame: GeographicFrame,
) -> Result<f64> {
    let area = match shape {
        Shape::Polygon(polygon) => {
            crate::geometry::geographic_polygon_area(metric, polygon, frame)?
        },
        Shape::MultiPolygon(polygons) => compensated_sum(
            polygons
                .iter()
                .map(|polygon| crate::geometry::geographic_polygon_area(metric, polygon, frame)),
        )?,
        Shape::GeometryCollection(geometries) => compensated_sum(
            geometries
                .iter()
                .map(|geometry| chart_shape_area(metric, geometry, frame)),
        )?,
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::Empty(..) => 0.0,
    };
    Ok(canonicalize_zero(area))
}

pub(crate) fn chart_shape_length(
    metric: &impl ChartMetric,
    shape: &Shape,
    cached_bounds: Option<crate::geometry::Bounds>,
) -> Result<f64> {
    let length = match shape {
        Shape::Point(_) | Shape::MultiPoint(_) | Shape::Empty(..) => 0.0,
        Shape::LineString(points) => chart_line_length(metric, points)?,
        Shape::MultiLineString(lines) => {
            compensated_sum(lines.iter().map(|line| chart_line_length(metric, line)))?
        },
        Shape::Polygon(_) | Shape::MultiPolygon(_) => {
            crate::geometry::geographic_polygonal_perimeter(
                metric,
                shape,
                metric.chart_frame(),
                cached_bounds,
            )?
        },
        Shape::GeometryCollection(geometries) => compensated_sum(
            geometries
                .iter()
                .map(|geometry| chart_shape_length(metric, geometry, None)),
        )?,
    };
    Ok(canonicalize_zero(length))
}

fn chart_line_length<C: Coordinates + ?Sized>(
    metric: &impl ChartMetric,
    points: &C,
) -> Result<f64> {
    if let Some((xs, ys)) = points.xy_columns() {
        return chart_line_length_columns(metric, xs, ys);
    }
    compensated_sum(points.segment_pairs().map(|[start, end]| {
        let edge = metric.chart_edge(start, end)?;
        metric.chart_edge_length(&edge)
    }))
}

pub(crate) fn chart_line_length_columns(
    metric: &impl ChartMetric,
    xs: &[f64],
    ys: &[f64],
) -> Result<f64> {
    debug_assert_eq!(xs.len(), ys.len());
    compensated_sum(xs.array_windows::<2>().zip(ys.array_windows::<2>()).map(
        |([x0, x1], [y0, y1])| {
            let edge =
                metric.chart_edge(Point::new_raw_xy(*x0, *y0), Point::new_raw_xy(*x1, *y1))?;
            metric.chart_edge_length(&edge)
        },
    ))
}

fn add_compensated(sum: &mut f64, correction: &mut f64, value: f64) {
    let adjusted = value - *correction;
    let next = *sum + adjusted;
    *correction = (next - *sum) - adjusted;
    *sum = next;
}

fn compensated_sum(values: impl IntoIterator<Item = Result<f64>>) -> Result<f64> {
    let mut sum = 0.0;
    let mut correction = 0.0;
    for value in values {
        add_compensated(&mut sum, &mut correction, value?);
    }
    Ok(sum)
}

pub(crate) fn geodesic_witness_distance_at(
    crs: &str,
    shape: &Shape,
    state: &RuntimeState,
) -> Result<f64> {
    let frame = geographic_metric_frame_at(crs, state)?;
    ensure_geographic_domain_in_frame(frame, shape)?;
    let Shape::LineString(line) = shape else {
        unreachable!("minimum-clearance witness is a two-point line")
    };
    debug_assert_eq!(line.coord_count(), 2);
    let start = frame
        .to_geodesic_point(line.nth_coord(0))
        .expect("geographic coordinates were admitted");
    let end = frame
        .to_geodesic_point(line.nth_coord(1))
        .expect("geographic coordinates were admitted");
    let crs = normalize(crs)?;
    with_geodesic_at(&crs, state, |geodesic| {
        let distance = inverse_distance(geodesic, start.x, start.y, end.x, end.y);
        if distance.is_finite() {
            Ok(distance)
        } else {
            Err(CrsError::invalid(
                "geodesic witness distance is not representable in binary64 metres",
            ))
        }
    })
}

pub(crate) fn geodesic_direct_on_ellipsoid(
    geodesic: &Geodesic,
    longitude: f64,
    latitude: f64,
    azimuth: f64,
    distance: f64,
) -> Result<GeodesicDirectInfo> {
    let (end_latitude, end_longitude, final_azimuth) =
        geodesic.direct(latitude, longitude, azimuth, distance);
    let mut info = GeodesicDirectInfo {
        longitude: end_longitude,
        latitude: end_latitude,
        final_azimuth,
    };
    if same_topological_coordinate(distance, 0.0) {
        info.longitude = longitude;
        info.latitude = latitude;
    }
    if !info.longitude.is_finite() || !info.latitude.is_finite() || !info.final_azimuth.is_finite()
    {
        return Err(CrsError::invalid(
            "geodesic direct calculation failed".to_owned(),
        ));
    }
    Ok(info)
}

/// The point `fraction` of the way along a solved geodesic line, clamped to
/// its endpoints.
pub(crate) fn geodesic_interpolate_on_line(
    geodesic: &Geodesic,
    (longitude, latitude, forward_azimuth, total): (f64, f64, f64, f64),
    fraction: f64,
) -> Result<GeodesicDirectInfo> {
    let target = (fraction * total).clamp(0.0, total);
    geodesic_direct_on_ellipsoid(geodesic, longitude, latitude, forward_azimuth, target)
}

#[cfg(test)]
mod chart_measure_tests {
    use std::sync::Arc;

    use geographiclib_rs::Geodesic;

    use crate::crs::geodesic::{AuthalicSeries, GeodesicMetric};
    use crate::geographic::lonlat::lonlat_rect_area_m2;
    use crate::geographic::{
        AngularFactor, GeodeticPole, GeographicAxisOrientation, GeographicFrame,
    };
    use crate::geometry::{
        Bounds, ChartMetric, Coordinates as _, MultiPolygon, Point, Polygon, Ring, Shape,
        ring_bounded_pole,
    };

    mod area;
    mod perimeter;
    mod support;

    use support::*;
}
