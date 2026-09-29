use geographiclib_rs::{Geodesic, PolygonArea, Winding, geodesic_capability as caps};

use crate::crs::RuntimeState;
use crate::crs::geodesic::{
    CrsError, DirectGeodesic as _, EllipsoidMetric, GeodesicDirectInfo,
    ensure_geographic_domain_in_frame, geographic_metric_frame_at, inverse_distance,
    inverse_distance_azimuths, normalize, with_geodesic_at, with_geodesic_erased_at,
};
use crate::crs::local::normalize_longitude;
use crate::error::Result;
use crate::geographic::GeographicFrame;
use crate::geometry::exact_arith::product_sum::ExactSum;
use crate::geometry::{
    Coordinates, Point, Polygon, Shape, canonicalize_zero, same_topological_coordinate,
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
    with_geodesic_erased_at(crs, shapes, state, |error| error, f)
}

pub(crate) fn geodesic_shape_area(
    geodesic: &Geodesic,
    shape: &Shape,
    frame: GeographicFrame,
) -> f64 {
    canonicalize_zero(match shape {
        Shape::Polygon(polygon) => geodesic_polygon_area(geodesic, polygon, frame),
        Shape::MultiPolygon(polygons) => compensated_sum(
            polygons
                .iter()
                .map(|polygon| geodesic_polygon_area(geodesic, polygon, frame)),
        ),
        Shape::GeometryCollection(geometries) => compensated_sum(
            geometries
                .iter()
                .map(|geometry| geodesic_shape_area(geodesic, geometry, frame)),
        ),
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::Empty(..) => 0.0,
    })
}

pub(crate) fn geodesic_shape_length(
    geodesic: &Geodesic,
    shape: &Shape,
    frame: GeographicFrame,
) -> f64 {
    canonicalize_zero(match shape {
        Shape::Point(_) | Shape::MultiPoint(_) | Shape::Empty(..) => 0.0,
        Shape::LineString(points) => geodesic_line_length(geodesic, points, frame),
        Shape::MultiLineString(lines) => compensated_sum(
            lines
                .iter()
                .map(|line| geodesic_line_length(geodesic, line, frame)),
        ),
        Shape::Polygon(polygon) => geodesic_polygon_perimeter(geodesic, polygon, frame),
        Shape::MultiPolygon(polygons) => compensated_sum(
            polygons
                .iter()
                .map(|polygon| geodesic_polygon_perimeter(geodesic, polygon, frame)),
        ),
        Shape::GeometryCollection(geometries) => compensated_sum(
            geometries
                .iter()
                .map(|geometry| geodesic_shape_length(geodesic, geometry, frame)),
        ),
    })
}

pub(crate) fn geodesic_polygon_area(
    geodesic: &Geodesic,
    polygon: &Polygon,
    frame: GeographicFrame,
) -> f64 {
    // Every ring area selects its interior in the unwrapped longitude/latitude
    // chart, independent of vertex order. Holes subtract that same interior.
    compensated_sum(
        std::iter::once(geodesic_ring_area(geodesic, &polygon.shell, frame)).chain(
            polygon
                .holes
                .iter()
                .map(|hole| -geodesic_ring_area(geodesic, hole, frame)),
        ),
    )
}

pub(crate) fn geodesic_polygon_perimeter(
    geodesic: &Geodesic,
    polygon: &Polygon,
    frame: GeographicFrame,
) -> f64 {
    compensated_sum(
        std::iter::once(geodesic_ring_measure(geodesic, &polygon.shell, frame).0).chain(
            polygon
                .holes
                .iter()
                .map(|hole| geodesic_ring_measure(geodesic, hole, frame).0),
        ),
    )
}

pub(crate) fn geodesic_ring_measure<C: Coordinates + ?Sized>(
    geodesic: &Geodesic,
    ring: &C,
    frame: GeographicFrame,
) -> (f64, f64) {
    let points = || {
        ring.iter_coords().map(|point| {
            let point = frame
                .to_geodesic_point(point)
                .expect("geographic metric received admitted coordinates");
            (point.x, point.y)
        })
    };
    geodesic_ring_measure_lonlat(geodesic, ring.coord_count(), points(), || {
        exact_chart_orientation(points())
    })
}

pub(crate) fn geodesic_ring_measure_columns(
    geodesic: &Geodesic,
    xs: &[f64],
    ys: &[f64],
    frame: GeographicFrame,
) -> (f64, f64) {
    debug_assert_eq!(xs.len(), ys.len());
    let points = || {
        std::iter::zip(xs, ys).map(|(&lon, &lat)| {
            let point = frame
                .to_geodesic_point(Point::new_raw_xy(lon, lat))
                .expect("geographic metric received admitted coordinates");
            (point.x, point.y)
        })
    };
    geodesic_ring_measure_lonlat(geodesic, xs.len(), points(), || {
        exact_chart_orientation(points())
    })
}

/// Compute a ring's chart-interior geodesic area without its perimeter.
///
/// `PolygonArea::add_point` requests both distance and area from every inverse
/// edge, but area measurement discards the distance. The public raw inverse
/// API accepts the narrower `AREA` mask, so keep the same transit/reduction
/// rules while avoiding the unused perimeter work and its accumulator.
fn geodesic_ring_area<C: Coordinates + ?Sized>(
    geodesic: &Geodesic,
    ring: &C,
    frame: GeographicFrame,
) -> f64 {
    if ring.coord_count() < 3 {
        return 0.0;
    }
    let mut points = ring.iter_coords().map(|point| {
        let point = frame
            .to_geodesic_point(point)
            .expect("geographic metric received admitted coordinates");
        (point.x, point.y)
    });
    let Some((initial_lon, initial_lat)) = points.next() else {
        return 0.0;
    };
    let mut chart = ChartRing::new();
    chart.add_point(initial_lon, initial_lat);
    let mut latest_lon = initial_lon;
    let mut latest_lat = initial_lat;
    let mut area = 0.0;
    let mut correction = 0.0;

    for (lon, lat) in points {
        let s12 = geodesic
            ._gen_inverse(latest_lat, latest_lon, lat, lon, caps::AREA)
            .9;
        add_compensated(&mut area, &mut correction, s12);
        chart.add_point(lon, lat);
        latest_lon = lon;
        latest_lat = lat;
    }

    let s12 = geodesic
        ._gen_inverse(latest_lat, latest_lon, initial_lat, initial_lon, caps::AREA)
        .9;
    add_compensated(&mut area, &mut correction, s12);
    chart.close();

    let geoid_area = geodesic.area();
    area %= geoid_area;
    if chart.crossings % 2 != 0 {
        area += if area < 0.0 {
            geoid_area / 2.0
        } else {
            -geoid_area / 2.0
        };
    }

    // PolygonArea is constructed with CounterClockwise winding and computes a
    // signed result in (-half-planet, half-planet].
    area = -area;
    if area > geoid_area / 2.0 {
        area -= geoid_area;
    } else if area <= -geoid_area / 2.0 {
        area += geoid_area;
    }
    let orientation = if chart.crossings % 2 != 0 {
        None
    } else {
        Some(chart.orientation().unwrap_or_else(|| {
            exact_chart_orientation(ring.iter_coords().map(|point| {
                let point = frame
                    .to_geodesic_point(point)
                    .expect("geographic metric received admitted coordinates");
                (point.x, point.y)
            }))
        }))
    };
    chart_interior_area(area, &chart, orientation, geoid_area)
}

fn add_compensated(sum: &mut f64, correction: &mut f64, value: f64) {
    let adjusted = value - *correction;
    let next = *sum + adjusted;
    *correction = (next - *sum) - adjusted;
    *sum = next;
}

fn longitude_transit(lon1: f64, lon2: f64, difference: f64) -> i64 {
    if difference > 0.0 && ((lon1 < 0.0 && lon2 >= 0.0) || (lon1 > 0.0 && lon2 == 0.0)) {
        1
    } else if difference < 0.0 && lon1 >= 0.0 && lon2 < 0.0 {
        -1
    } else {
        0
    }
}

fn longitude_step(lon1: f64, lon2: f64) -> (f64, f64) {
    let difference = lon2 - lon1;
    if difference > 180.0 {
        (difference - 360.0, -360.0)
    } else if difference <= -180.0 {
        (difference + 360.0, 360.0)
    } else {
        (difference, 0.0)
    }
}

struct ChartRing {
    twice_area: f64,
    absolute_products: f64,
    edge_count: usize,
    exact_area: Option<Box<ExactSum>>,
    count: usize,
    initial_lon: f64,
    initial_lat: f64,
    previous_lon: f64,
    previous_lat: f64,
    previous_unwrapped_lon: f64,
    longitude_offset: f64,
    crossings: i64,
}

impl ChartRing {
    fn new() -> Self {
        Self {
            twice_area: 0.0,
            absolute_products: 0.0,
            edge_count: 0,
            exact_area: None,
            count: 0,
            initial_lon: 0.0,
            initial_lat: 0.0,
            previous_lon: 0.0,
            previous_lat: 0.0,
            previous_unwrapped_lon: 0.0,
            longitude_offset: 0.0,
            crossings: 0,
        }
    }

    fn exact() -> Self {
        Self {
            exact_area: Some(Box::new(ExactSum::zero())),
            ..Self::new()
        }
    }

    fn add_point(&mut self, lon: f64, lat: f64) {
        let lon = normalize_longitude(lon);
        if self.count == 0 {
            self.initial_lon = lon;
            self.initial_lat = lat;
            self.previous_lon = lon;
            self.previous_lat = lat;
            self.previous_unwrapped_lon = lon;
            self.count = 1;
            return;
        }

        let (difference, offset_delta) = longitude_step(self.previous_lon, lon);
        self.longitude_offset += offset_delta;
        let unwrapped_lon = lon + self.longitude_offset;
        self.add_edge(
            self.previous_unwrapped_lon,
            self.previous_lat,
            unwrapped_lon,
            lat,
        );
        self.crossings =
            self.crossings
                .wrapping_add(longitude_transit(self.previous_lon, lon, difference));
        self.previous_lon = lon;
        self.previous_lat = lat;
        self.previous_unwrapped_lon = unwrapped_lon;
        self.count += 1;
    }

    fn close(&mut self) {
        if self.count == 0 {
            return;
        }

        let (difference, offset_delta) = longitude_step(self.previous_lon, self.initial_lon);
        let longitude_offset = self.longitude_offset + offset_delta;
        let unwrapped_lon = self.initial_lon + longitude_offset;
        self.add_edge(
            self.previous_unwrapped_lon,
            self.previous_lat,
            unwrapped_lon,
            self.initial_lat,
        );
        self.crossings = self.crossings.wrapping_add(longitude_transit(
            self.previous_lon,
            self.initial_lon,
            difference,
        ));
    }

    fn add_edge(&mut self, x0: f64, y0: f64, x1: f64, y1: f64) {
        let forward = x0 * y1;
        let reverse = x1 * y0;
        self.twice_area += forward - reverse;
        self.absolute_products += forward.abs() + reverse.abs();
        self.edge_count += 1;
        if let Some(exact_area) = &mut self.exact_area {
            exact_area.add_cross(x0, y0, x1, y1);
        }
    }

    fn orientation(&self) -> Option<std::cmp::Ordering> {
        if let Some(exact_area) = &self.exact_area {
            return Some(exact_area.ordering());
        }

        // Admit the rounded sign only outside a bound for product, sum, and
        // subnormal rounding; callers replay uncertain rings with ExactSum.
        let operations = 16.0 * self.edge_count as f64 + 8.0;
        let error = operations * (self.absolute_products * f64::EPSILON + f64::from_bits(1));
        if !self.twice_area.is_finite() || !error.is_finite() || self.twice_area.abs() <= error {
            return None;
        }
        Some(if self.twice_area < 0.0 {
            std::cmp::Ordering::Less
        } else {
            std::cmp::Ordering::Greater
        })
    }
}

fn exact_chart_orientation(points: impl IntoIterator<Item = (f64, f64)>) -> std::cmp::Ordering {
    let mut chart = ChartRing::exact();
    for (lon, lat) in points {
        chart.add_point(lon, lat);
    }
    chart.close();
    chart
        .orientation()
        .expect("an exact chart accumulator always determines orientation")
}

fn chart_interior_area(
    area: f64,
    chart: &ChartRing,
    orientation: Option<std::cmp::Ordering>,
    ellipsoid_area: f64,
) -> f64 {
    if chart.crossings % 2 != 0 {
        // The transit correction already selected the pole-side cap; reverse
        // traversal changes only its sign.
        return area.abs();
    }
    match orientation.expect("non-polar rings resolve chart orientation") {
        std::cmp::Ordering::Equal => 0.0,
        orientation => {
            let oriented = if orientation == std::cmp::Ordering::Less {
                -area
            } else {
                area
            };
            let area = oriented % ellipsoid_area;
            if area < 0.0 {
                area + ellipsoid_area
            } else {
                area
            }
        },
    }
}

fn geodesic_ring_measure_lonlat(
    geodesic: &Geodesic,
    coord_count: usize,
    points: impl IntoIterator<Item = (f64, f64)>,
    exact_orientation: impl FnOnce() -> std::cmp::Ordering,
) -> (f64, f64) {
    if coord_count < 3 {
        return (0.0, 0.0);
    }
    let mut area = PolygonArea::new(geodesic, Winding::CounterClockwise);
    let mut chart = ChartRing::new();
    for (lon, lat) in points {
        area.add_point(lat, lon);
        chart.add_point(lon, lat);
    }
    chart.close();
    let (perimeter, area, _) = area.compute(true);
    let orientation = if chart.crossings % 2 != 0 {
        None
    } else {
        Some(chart.orientation().unwrap_or_else(exact_orientation))
    };
    (
        perimeter,
        chart_interior_area(area, &chart, orientation, geodesic.area()),
    )
}

pub(crate) fn geodesic_length_at(crs: &str, shape: &Shape, state: &RuntimeState) -> Result<f64> {
    let frame = geographic_metric_frame_at(crs, state)?;
    ensure_geographic_domain_in_frame(frame, shape)?;
    let crs = normalize(crs)?;
    with_geodesic_at(&crs, state, |geodesic| {
        Ok(geodesic_shape_length(geodesic, shape, frame))
    })
}

pub(crate) fn geodesic_line_length<C: Coordinates + ?Sized>(
    geodesic: &Geodesic,
    points: &C,
    frame: GeographicFrame,
) -> f64 {
    if let Some((xs, ys)) = points.xy_columns() {
        return geodesic_line_length_columns(geodesic, xs, ys, frame);
    }
    compensated_sum(points.segment_pairs().map(|[start, end]| {
        let start = frame
            .to_geodesic_point(start)
            .expect("geographic metric received admitted coordinates");
        let end = frame
            .to_geodesic_point(end)
            .expect("geographic metric received admitted coordinates");
        geodesic_segment_length(geodesic, start.x, start.y, end.x, end.y)
    }))
}

pub(crate) fn geodesic_line_length_columns(
    geodesic: &Geodesic,
    xs: &[f64],
    ys: &[f64],
    frame: GeographicFrame,
) -> f64 {
    debug_assert_eq!(xs.len(), ys.len());
    compensated_sum(xs.array_windows::<2>().zip(ys.array_windows::<2>()).map(
        |([x0, x1], [y0, y1])| {
            let start = frame
                .to_geodesic_point(Point::new_raw_xy(*x0, *y0))
                .expect("geographic metric received admitted coordinates");
            let end = frame
                .to_geodesic_point(Point::new_raw_xy(*x1, *y1))
                .expect("geographic metric received admitted coordinates");
            geodesic_segment_length(geodesic, start.x, start.y, end.x, end.y)
        },
    ))
}

fn compensated_sum(values: impl IntoIterator<Item = f64>) -> f64 {
    let mut sum = 0.0;
    let mut correction = 0.0;
    for value in values {
        let adjusted = value - correction;
        let next = sum + adjusted;
        correction = (next - sum) - adjusted;
        sum = next;
    }
    sum
}

fn geodesic_segment_length(
    geodesic: &Geodesic,
    start_lon: f64,
    start_lat: f64,
    end_lon: f64,
    end_lat: f64,
) -> f64 {
    inverse_distance(geodesic, start_lon, start_lat, end_lon, end_lat)
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

pub(crate) fn geodesic_line_solution(
    geodesic: &Geodesic,
    lon1: f64,
    lat1: f64,
    lon2: f64,
    lat2: f64,
) -> Result<(f64, f64, f64, f64)> {
    let (total, forward_azimuth, _) = inverse_distance_azimuths(geodesic, lon1, lat1, lon2, lat2);
    if !total.is_finite() || !forward_azimuth.is_finite() {
        return Err(CrsError::invalid(
            "geodesic interpolate calculation failed".to_owned(),
        ));
    }
    Ok((lon1, lat1, forward_azimuth, total))
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
