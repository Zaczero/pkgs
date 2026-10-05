use geographiclib_rs::Geodesic;

use crate::crs::RuntimeState;
use crate::crs::geodesic::{
    CrsError, DirectGeodesic as _, EllipsoidMetric, GeodesicDirectInfo,
    ensure_geographic_domain_in_frame, geographic_metric_frame_at, inverse_distance, normalize,
    with_geodesic_at, with_geographic_metric_erased_at,
};
use crate::error::Result;
use crate::geographic::{GeographicFrame, Pole};
use crate::geometry::{
    ChartMetric, Coordinates, Point, Polygon, Ring, Shape, canonicalize_zero, ring_bounded_pole,
    same_topological_coordinate,
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
        Shape::Polygon(polygon) => chart_polygon_area(metric, polygon, frame)?,
        Shape::MultiPolygon(polygons) => compensated_sum(
            polygons
                .iter()
                .map(|polygon| chart_polygon_area(metric, polygon, frame)),
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

fn chart_polygon_area(
    metric: &impl ChartMetric,
    polygon: &Polygon,
    frame: GeographicFrame,
) -> Result<f64> {
    let mut sum = 0.0;
    let mut correction = 0.0;
    add_compensated(
        &mut sum,
        &mut correction,
        chart_ring_area(metric, &polygon.shell, frame)?,
    );
    for hole in polygon.holes.iter() {
        add_compensated(
            &mut sum,
            &mut correction,
            -chart_ring_area(metric, hole, frame)?,
        );
    }
    Ok(sum)
}

fn chart_ring_area(metric: &impl ChartMetric, ring: &Ring, frame: GeographicFrame) -> Result<f64> {
    let north = match ring_bounded_pole(ring, frame) {
        Some(Pole::North) => true,
        Some(Pole::South) => false,
        None => ring.nth_coord(0).y >= 0.0,
    };

    let mut integral = 0.0;
    let mut correction = 0.0;
    for [start, end] in ring.segment_pairs() {
        let edge = metric.chart_edge(start, end)?;
        add_compensated(
            &mut integral,
            &mut correction,
            metric.chart_area_edge_term(&edge, north),
        );
    }
    Ok(metric.chart_area_factor() * integral.abs())
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

    use super::chart_polygon_area;
    use crate::crs::geodesic::{AuthalicSeries, GeodesicMetric};
    use crate::geographic::{AngularFactor, GeographicFrame};
    use crate::geometry::types::ChartMetric;
    use crate::geometry::{
        Bounds, Coordinates as _, MultiPolygon, Point, Polygon, Ring, Shape, ring_bounded_pole,
    };

    fn metric_with_flattening_and_frame(flattening: f64, frame: GeographicFrame) -> GeodesicMetric {
        let geodesic = Geodesic::new(6_378_137.0, flattening);
        let authalic_series = AuthalicSeries::new(&geodesic);
        GeodesicMetric {
            crs: Arc::new("EPSG:4326".to_owned()),
            geodesic: Box::new(geodesic),
            authalic_series,
            frame,
            state: crate::crs::runtime_state_snapshot().unwrap(),
        }
    }

    fn metric_with_flattening(flattening: f64) -> GeodesicMetric {
        metric_with_flattening_and_frame(flattening, GeographicFrame::crs84())
    }

    fn metric() -> GeodesicMetric {
        metric_with_flattening(1.0 / 298.257_223_563)
    }

    fn ring(points: &[(f64, f64)]) -> Ring {
        Ring::closed(
            points
                .iter()
                .map(|&(x, y)| Point::new_raw_xy(x, y))
                .collect(),
        )
        .unwrap()
    }

    fn polygon(shell: &[(f64, f64)], holes: &[&[(f64, f64)]]) -> Polygon {
        Polygon::new(ring(shell), holes.iter().map(|hole| ring(hole)).collect())
    }

    fn perimeter(metric: &impl ChartMetric, shape: &Shape, frame: GeographicFrame) -> f64 {
        crate::geometry::geographic_polygonal_perimeter(metric, shape, frame, None).unwrap()
    }

    fn analytic_parallel_circumference(metric: &GeodesicMetric, latitude: f64) -> f64 {
        let geodesic = metric.geodesic.as_ref();
        let latitude = latitude * metric.frame.latitude_unit().radians_per_unit();
        let eccentricity_squared = geodesic.f * (2.0 - geodesic.f);
        std::f64::consts::TAU * geodesic.a * latitude.cos()
            / (1.0 - eccentricity_squared * latitude.sin().powi(2)).sqrt()
    }

    fn raw_stored_perimeter(metric: &impl ChartMetric, shape: &Shape) -> f64 {
        let polygons: Vec<&Polygon> = match shape {
            Shape::Polygon(polygon) => vec![polygon],
            Shape::MultiPolygon(polygons) => polygons.iter().collect(),
            Shape::Point(_)
            | Shape::MultiPoint(_)
            | Shape::LineString(_)
            | Shape::MultiLineString(_)
            | Shape::GeometryCollection(_)
            | Shape::Empty(..) => panic!("raw perimeter fixture is polygonal"),
        };
        let mut sum = 0.0;
        let mut correction = 0.0;
        for polygon in polygons {
            for ring in polygon.rings() {
                for [start, end] in ring.segment_pairs() {
                    let edge = metric.chart_edge(start, end).unwrap();
                    let adjusted = metric.chart_edge_length(&edge).unwrap() - correction;
                    let next = sum + adjusted;
                    correction = (next - sum) - adjusted;
                    sum = next;
                }
            }
        }
        sum
    }

    fn multi_polygon(polygons: Vec<Polygon>) -> Shape {
        Shape::MultiPolygon(MultiPolygon::from_dense(polygons).unwrap())
    }

    fn north_closure(longitude: f64, latitude: f64, hop: bool) -> Vec<(f64, f64)> {
        let mut points = (0..=4)
            .map(|index| (longitude + 90.0 * f64::from(index), latitude))
            .collect::<Vec<_>>();
        points.push((longitude + 360.0, 90.0));
        if hop {
            points.push((longitude, 90.0));
        } else {
            points.extend(
                (0..4)
                    .rev()
                    .map(|index| (longitude + 90.0 * f64::from(index), 90.0)),
            );
        }
        points
    }

    fn south_closure(longitude: f64, latitude: f64, hop: bool) -> Vec<(f64, f64)> {
        north_closure(longitude, latitude, hop)
            .into_iter()
            .map(|(longitude, latitude)| (longitude, -latitude))
            .collect()
    }

    fn oblique_north_closure(longitude: f64, latitude: f64, noded: bool) -> Vec<(f64, f64)> {
        let mut points = vec![
            (longitude, latitude),
            (longitude + 90.0, latitude),
            (longitude + 180.0, latitude),
            (longitude + 270.0, latitude),
            (longitude + 360.0, latitude),
            (longitude + 450.0, 90.0),
            (longitude + 360.0, 90.0),
            (longitude + 270.0, 90.0),
            (longitude + 180.0, 90.0),
            (longitude + 90.0, 90.0),
        ];
        if noded {
            points.push((longitude + 45.0, f64::midpoint(latitude, 90.0)));
        }
        points
    }

    fn full_longitude_band(longitude: f64, south: f64, north: f64) -> Vec<(f64, f64)> {
        let mut points = (0..=4)
            .map(|index| (longitude + 90.0 * f64::from(index), south))
            .collect::<Vec<_>>();
        points.extend(
            (0..=4)
                .rev()
                .map(|index| (longitude + 90.0 * f64::from(index), north)),
        );
        points
    }

    fn arbitrary_cut_closure(noded: bool) -> Vec<(f64, f64)> {
        let ascent = [(360.0, 60.0), (400.0, 70.0), (380.0, 80.0), (400.0, 90.0)];
        let mut points = (0..=4)
            .map(|index| (90.0 * f64::from(index), 60.0))
            .collect::<Vec<_>>();
        points.extend(ascent.into_iter().skip(1));
        points.extend([(310.0, 90.0), (220.0, 90.0), (130.0, 90.0), (40.0, 90.0)]);
        points.extend([(20.0, 80.0), (40.0, 70.0)]);
        if noded {
            let insertion = points
                .len()
                .checked_sub(1)
                .expect("the descending cut has a final edge");
            points.insert(insertion, (30.0, 75.0));
        }
        points
    }

    fn box_polygon(x0: f64, y0: f64, x1: f64, y1: f64) -> Polygon {
        polygon(&[(x0, y0), (x1, y0), (x1, y1), (x0, y1)], &[])
    }

    #[test]
    fn chart_and_rectangular_cell_areas_match_high_precision_literals() {
        let metric = metric();
        let frame = metric.frame;
        let zone = polygon(
            &[(-45.0, 70.0), (45.0, 70.0), (45.0, 80.0), (-45.0, 80.0)],
            &[],
        );
        let zone_area = chart_polygon_area(&metric, &zone, frame).unwrap();
        let zone_expected =
            2_899_441_975_904.581_036_205_810_669_933_563_650_514_840_876_784_698_225;
        assert!(
            (zone_area - zone_expected).abs() <= 1.0e-14 * zone_expected,
            "zone area={zone_area:.17}, expected={zone_expected:.17}"
        );
        let rectangular_cell_area = crate::geographic::lonlat::lonlat_rect_area_m2(
            crate::geometry::Bounds::new_raw(-45.0, 70.0, 45.0, 80.0),
        );
        assert!(
            (rectangular_cell_area - zone_expected).abs() <= 1.0e-14 * zone_expected,
            "rectangular cell area={rectangular_cell_area:.17}, expected={zone_expected:.17}"
        );

        let triangle = polygon(&[(0.0, 10.0), (30.0, 10.0), (15.0, 50.0)], &[]);
        let triangle_area = chart_polygon_area(&metric, &triangle, frame).unwrap();
        let triangle_expected =
            6_706_052_330_164.589_974_816_023_206_088_526_299_535_059_741_903_974_644;
        assert!(
            (triangle_area - triangle_expected).abs() <= 1.0e-14 * triangle_expected,
            "triangle area={triangle_area:.17}, expected={triangle_expected:.17}"
        );
    }

    #[test]
    #[expect(
        clippy::excessive_precision,
        reason = "40-digit mpmath oracle literals preserve the independent reference value"
    )]
    fn chart_area_series_matches_spherical_oblate_and_prolate_literals() {
        let frame = GeographicFrame::crs84();
        let zone = polygon(
            &[(-45.0, 70.0), (45.0, 70.0), (45.0, 80.0), (-45.0, 80.0)],
            &[],
        );
        let cases = [
            (
                0.0,
                2_882_901_463_210.015_326_402_293_007_066_545_690_845_072_588_580_157_504_759_345_263_7,
            ),
            (
                0.02,
                2_983_569_749_854.379_597_021_215_698_894_220_798_358_603_884_467_051_635_905_974_172_6,
            ),
            (
                -0.01,
                2_834_347_845_268.142_710_504_611_349_147_843_595_380_138_617_606_293_374_597_410_335_3,
            ),
        ];
        for (flattening, expected) in cases {
            let metric = metric_with_flattening(flattening);
            let actual = chart_polygon_area(&metric, &zone, frame).unwrap();
            assert!(
                (actual - expected).abs() <= 1.0e-14 * expected,
                "flattening={flattening}: actual={actual:.17}, expected={expected:.17}"
            );
        }
    }

    #[test]
    fn chart_area_and_perimeter_are_invariant_between_degree_grad_and_radian_frames() {
        let degree_polygon: [(f64, f64); 4] =
            [(-45.0, 70.0), (45.0, 70.0), (45.0, 80.0), (-45.0, 80.0)];
        let grad_factor = AngularFactor::try_new("grad", std::f64::consts::PI / 200.0).unwrap();
        let grad_frame = GeographicFrame::try_new(grad_factor, grad_factor, 0.0).unwrap();
        let radian_factor = AngularFactor::try_new("radian", 1.0).unwrap();
        let radian_frame = GeographicFrame::try_new(radian_factor, radian_factor, 0.0).unwrap();
        let grad_polygon = degree_polygon
            .map(|(longitude, latitude)| (longitude * 10.0 / 9.0, latitude * 10.0 / 9.0));
        let radian_polygon = degree_polygon
            .map(|(longitude, latitude)| (longitude.to_radians(), latitude.to_radians()));

        let degree_metric = metric_with_flattening(1.0 / 298.257_223_563);
        let grad_metric = metric_with_flattening_and_frame(1.0 / 298.257_223_563, grad_frame);
        let radian_metric = metric_with_flattening_and_frame(1.0 / 298.257_223_563, radian_frame);
        let degree_shape = polygon(&degree_polygon, &[]);
        let grad_shape = polygon(&grad_polygon, &[]);
        let radian_shape = polygon(&radian_polygon, &[]);
        let degree_area =
            chart_polygon_area(&degree_metric, &degree_shape, GeographicFrame::crs84()).unwrap();
        let grad_area = chart_polygon_area(&grad_metric, &grad_shape, grad_frame).unwrap();
        let radian_area = chart_polygon_area(&radian_metric, &radian_shape, radian_frame).unwrap();
        let degree_length = perimeter(
            &degree_metric,
            &Shape::Polygon(degree_shape),
            GeographicFrame::crs84(),
        );
        let grad_length = perimeter(&grad_metric, &Shape::Polygon(grad_shape), grad_frame);
        let radian_length = perimeter(&radian_metric, &Shape::Polygon(radian_shape), radian_frame);

        for actual in [grad_area, radian_area] {
            assert!((actual - degree_area).abs() <= 1.0e-14 * degree_area);
        }
        for actual in [grad_length, radian_length] {
            assert!((actual - degree_length).abs() <= 1.0e-14 * degree_length);
        }
    }

    #[test]
    fn closures_measure_the_same_parallel_in_supported_angular_frames() {
        let degree = GeographicFrame::crs84();
        let grad_unit = AngularFactor::try_new("grad", std::f64::consts::PI / 200.0).unwrap();
        let grad = GeographicFrame::try_new(grad_unit, grad_unit, 0.0).unwrap();
        let radian_unit = AngularFactor::try_new("radian", 1.0).unwrap();
        let radians = GeographicFrame::try_new(radian_unit, radian_unit, 0.0).unwrap();
        let degree_unit = AngularFactor::try_new("degree", std::f64::consts::PI / 180.0).unwrap();
        let paris = GeographicFrame::try_new(
            degree_unit,
            degree_unit,
            2.596_921_3 * std::f64::consts::PI / 180.0,
        )
        .unwrap();

        for (name, frame, scale, longitude) in [
            ("degrees", degree, 1.0, 40.0),
            (
                "radians at zero",
                radians,
                std::f64::consts::PI / 180.0,
                0.0,
            ),
            (
                "radians at the seam",
                radians,
                std::f64::consts::PI / 180.0,
                -180.0,
            ),
            (
                "radians at 90 degrees",
                radians,
                std::f64::consts::PI / 180.0,
                90.0,
            ),
            ("grads at zero", grad, 10.0 / 9.0, 0.0),
            ("grads at the seam", grad, 10.0 / 9.0, -180.0),
            ("Paris prime meridian", paris, 1.0, 40.0),
        ] {
            let metric = metric_with_flattening_and_frame(1.0 / 298.257_223_563, frame);
            let points = north_closure(longitude, 60.0, false)
                .into_iter()
                .map(|(longitude, latitude)| (longitude * scale, latitude * scale))
                .collect::<Vec<_>>();
            let shape = Shape::Polygon(Polygon::new(ring(&points), Vec::new()));
            let actual = perimeter(&metric, &shape, frame);
            let expected = analytic_parallel_circumference(&metric, 60.0 * scale);
            assert!(
                (actual - expected).abs() <= 1.0e-8 * expected,
                "{name}: {actual:.12} != {expected:.12}"
            );
        }
    }

    #[test]
    fn nonidentical_radian_and_grad_aliases_remain_stored_without_a_tolerance() {
        for (name, frame, scale) in [
            (
                "radians",
                GeographicFrame::try_new(
                    AngularFactor::try_new("radian", 1.0).unwrap(),
                    AngularFactor::try_new("radian", 1.0).unwrap(),
                    0.0,
                )
                .unwrap(),
                std::f64::consts::PI / 180.0,
            ),
            (
                "grads",
                GeographicFrame::try_new(
                    AngularFactor::try_new("grad", std::f64::consts::PI / 200.0).unwrap(),
                    AngularFactor::try_new("grad", std::f64::consts::PI / 200.0).unwrap(),
                    0.0,
                )
                .unwrap(),
                10.0 / 9.0,
            ),
        ] {
            let metric = metric_with_flattening_and_frame(1.0 / 298.257_223_563, frame);
            let points = north_closure(40.0, 60.0, false)
                .into_iter()
                .map(|(longitude, latitude)| (longitude * scale, latitude * scale))
                .collect::<Vec<_>>();
            let shape = Shape::Polygon(Polygon::new(ring(&points), Vec::new()));
            let stored = raw_stored_perimeter(&metric, &shape);
            let actual = perimeter(&metric, &shape, frame);
            assert_eq!(
                actual.to_bits(),
                stored.to_bits(),
                "{name} alias spellings are distinct exact positions"
            );
        }
    }

    #[test]
    fn chart_perimeter_cancels_partial_periodic_seam_overlaps() {
        let metric = metric();
        let west = polygon(
            &[(170.0, -10.0), (180.0, -10.0), (180.0, 10.0), (170.0, 10.0)],
            &[],
        );
        let east = polygon(
            &[(-180.0, 0.0), (-170.0, 0.0), (-170.0, 20.0), (-180.0, 20.0)],
            &[],
        );
        let west_perimeter = perimeter(&metric, &Shape::Polygon(west.clone()), metric.frame);
        let east_perimeter = perimeter(&metric, &Shape::Polygon(east.clone()), metric.frame);
        let combined = perimeter(
            &metric,
            &Shape::MultiPolygon(MultiPolygon::from_dense(vec![west, east]).unwrap()),
            metric.frame,
        );
        let shared_edge = metric
            .chart_edge(
                Point::new_raw_xy(-180.0, 0.0),
                Point::new_raw_xy(-180.0, 10.0),
            )
            .unwrap();
        let shared = metric.chart_edge_length(&shared_edge).unwrap();
        let expected = west_perimeter + east_perimeter - 2.0 * shared;
        assert!(
            (combined - expected).abs() <= 1.0e-8,
            "combined={combined:.17}, expected={expected:.17}"
        );
    }

    #[test]
    fn stepped_and_hop_polar_closures_measure_one_parallel() {
        let metric = metric();
        let expected = analytic_parallel_circumference(&metric, 60.0);
        for longitude in [0.0, 40.0, -179.0, 180.0, -180.0] {
            for hop in [false, true] {
                let shape = Shape::Polygon(Polygon::new(
                    ring(&north_closure(longitude, 60.0, hop)),
                    Vec::new(),
                ));
                let actual = perimeter(&metric, &shape, metric.frame);
                assert!(
                    (actual - expected).abs() <= 1.0e-8 * expected,
                    "longitude={longitude}, hop={hop}: {actual:.12} != {expected:.12}"
                );
            }
        }
    }

    #[test]
    fn south_polar_closure_measures_one_parallel() {
        let metric = metric();
        let shape = Shape::Polygon(Polygon::new(
            ring(&south_closure(40.0, 60.0, false)),
            Vec::new(),
        ));
        let expected = analytic_parallel_circumference(&metric, 60.0);
        let actual = perimeter(&metric, &shape, metric.frame);
        assert!(
            (actual - expected).abs() <= 1.0e-8 * expected,
            "south closure={actual:.12}, expected={expected:.12}"
        );
    }

    #[test]
    fn clockwise_polar_closure_measures_one_parallel() {
        let metric = metric();
        let mut points = north_closure(40.0, 60.0, false);
        points.reverse();
        let shape = Shape::Polygon(Polygon::new(ring(&points), Vec::new()));
        let expected = analytic_parallel_circumference(&metric, 60.0);
        let actual = perimeter(&metric, &shape, metric.frame);
        assert!(
            (actual - expected).abs() <= 1.0e-8 * expected,
            "clockwise closure={actual:.12}, expected={expected:.12}"
        );
    }

    #[test]
    fn oblique_and_noded_polar_closures_measure_one_parallel() {
        let metric = metric();
        let expected = analytic_parallel_circumference(&metric, 60.0);
        for noded in [false, true] {
            let shape = Shape::Polygon(Polygon::new(
                ring(&oblique_north_closure(0.0, 60.0, noded)),
                Vec::new(),
            ));
            let actual = perimeter(&metric, &shape, metric.frame);
            assert!(
                (actual - expected).abs() <= 1.0e-8 * expected,
                "noded={noded}: {actual:.12} != {expected:.12}"
            );
        }
    }

    #[test]
    fn arbitrary_slope_lifted_copies_cancel_with_or_without_a_collinear_node() {
        let metric = metric();
        let expected = analytic_parallel_circumference(&metric, 60.0);
        for noded in [false, true] {
            let shape = Shape::Polygon(Polygon::new(
                ring(&arbitrary_cut_closure(noded)),
                Vec::new(),
            ));
            let actual = perimeter(&metric, &shape, metric.frame);
            assert!(
                (actual - expected).abs() <= 1.0e-8 * expected,
                "noded={noded}: {actual:.12} != {expected:.12}"
            );
        }
    }

    #[test]
    fn fractional_collinear_cut_node_keeps_polar_closure_perimeter() {
        let metric = metric();
        let points = [
            (-248.0, -60.0),
            (-158.0, -60.0),
            (-68.0, -60.0),
            (22.0, -60.0),
            (112.0, -60.0),
            (114.0, -63.0),
            (118.0, -76.0),
            (135.0, -82.0),
            (128.0, -87.0),
            (-196.0, -90.0),
            (-106.0, -90.0),
            (-16.0, -90.0),
            (74.0, -90.0),
            (164.0, -90.0),
            (128.0, -87.0),
            (135.0, -82.0),
            (118.0, -76.0),
            (116.0, -69.5),
            (114.0, -63.0),
        ];
        let shape = Shape::Polygon(Polygon::new(ring(&points), Vec::new()));
        let expected = analytic_parallel_circumference(&metric, 60.0);
        let actual = perimeter(&metric, &shape, metric.frame);
        assert!(
            (actual - expected).abs() <= 1.0e-8 * expected,
            "noded polar closure={actual:.12}, expected={expected:.12}"
        );
    }

    #[test]
    fn world_bands_and_explicit_period_cap_keep_their_parallel_boundaries() {
        let metric = metric();
        let one_parallel = analytic_parallel_circumference(&metric, 60.0);
        for longitude in [0.0, 40.0, -180.0] {
            let shape = Shape::Polygon(Polygon::new(
                ring(&full_longitude_band(longitude, -60.0, 60.0)),
                Vec::new(),
            ));
            let actual = perimeter(&metric, &shape, metric.frame);
            assert!(
                (actual - 2.0 * one_parallel).abs() <= 1.0e-8 * one_parallel,
                "band longitude={longitude}: {actual:.12} != {:.12}",
                2.0 * one_parallel
            );
        }

        let cap = Shape::Polygon(Polygon::new(
            ring(&[(0.0, 60.0), (360.0, 60.0), (360.0, 90.0), (0.0, 90.0)]),
            Vec::new(),
        ));
        let cap_length = perimeter(&metric, &cap, metric.frame);
        assert!(
            (cap_length - one_parallel).abs() <= 1.0e-8 * one_parallel,
            "explicit-period cap={cap_length:.12}, expected={one_parallel:.12}"
        );

        let shifted_band = Shape::Polygon(Polygon::new(
            ring(&[(40.0, -60.0), (400.0, -60.0), (400.0, 60.0), (40.0, 60.0)]),
            Vec::new(),
        ));
        let shifted_length = perimeter(&metric, &shifted_band, metric.frame);
        assert!(
            (shifted_length - 2.0 * one_parallel).abs() <= 1.0e-8 * one_parallel,
            "explicit-period band={shifted_length:.12}, expected={:.12}",
            2.0 * one_parallel
        );
    }

    #[test]
    fn split_north_cap_at_eighty_degrees_has_the_analytic_parallel_perimeter() {
        let metric = metric();
        let cap = Shape::Polygon(Polygon::new(
            ring(&[
                (-180.0, 80.0),
                (-90.0, 80.0),
                (0.0, 80.0),
                (90.0, 80.0),
                (180.0, 80.0),
                (180.0, 90.0),
                (-180.0, 90.0),
            ]),
            Vec::new(),
        ));
        let actual = perimeter(&metric, &cap, metric.frame);
        let expected = analytic_parallel_circumference(&metric, 80.0);
        assert!(
            (actual - expected).abs() < 1.0,
            "N cap 80 perimeter={actual:.6} m, analytic={expected:.6} m"
        );
        assert!(
            (actual - 6_981_655.0).abs() < 1.0,
            "N cap 80 perimeter={actual:.6} m, expected about 6,981,655 m"
        );
    }

    #[test]
    fn public_scalar_length_uses_the_polar_chart_cut_perimeter() {
        crate::test_support::initialize_python();
        let metric = metric();
        let cap = Polygon::new(
            ring(&[
                (-180.0, 80.0),
                (-90.0, 80.0),
                (0.0, 80.0),
                (90.0, 80.0),
                (180.0, 80.0),
                (180.0, 90.0),
                (-180.0, 90.0),
            ]),
            Vec::new(),
        );
        let geometry = crate::PyGeometry::wgs84(Shape::Polygon(cap));
        let actual =
            pyo3::Python::attach(|py| crate::length_natural_scalar(py, &geometry)).unwrap();
        let expected = analytic_parallel_circumference(&metric, 80.0);
        assert!(
            (actual - expected).abs() < 1.0,
            "public length={actual:.6} m, expected={expected:.6} m"
        );
    }

    #[test]
    fn seam_split_and_mixed_orientation_keep_only_the_exterior_boundary() {
        let metric = metric();
        let shared = metric
            .chart_edge(
                Point::new_raw_xy(-180.0, -10.0),
                Point::new_raw_xy(-180.0, 10.0),
            )
            .unwrap();
        let shared_length = metric.chart_edge_length(&shared).unwrap();
        let stored_sum = raw_stored_perimeter(
            &metric,
            &multi_polygon(vec![
                box_polygon(170.0, -10.0, 180.0, 10.0),
                box_polygon(-180.0, -10.0, -170.0, 10.0),
            ]),
        );
        let expected = stored_sum - 2.0 * shared_length;
        for reverse_east in [false, true] {
            let orientation_case = if reverse_east {
                "E CW + W CCW"
            } else {
                "E CCW + W CCW"
            };
            let east = box_polygon(170.0, -10.0, 180.0, 10.0);
            let east_ring = if reverse_east {
                east.shell.reversed().unwrap()
            } else {
                east.shell
            };
            let east_member = Polygon::new(east_ring, Vec::new());
            let shape = multi_polygon(vec![east_member, box_polygon(-180.0, -10.0, -170.0, 10.0)]);
            let actual = perimeter(&metric, &shape, metric.frame);
            assert!(
                (actual - expected).abs() < 1.0e-7,
                "{orientation_case}: mixed seam perimeter={actual:.12}, expected={expected:.12}"
            );
        }
    }

    #[test]
    fn partial_seam_overlap_cancels_only_its_shared_latitude_interval() {
        let metric = metric();
        let shape = multi_polygon(vec![
            box_polygon(170.0, -10.0, 180.0, 10.0),
            box_polygon(-180.0, 0.0, -170.0, 20.0),
        ]);
        let shared = metric
            .chart_edge(
                Point::new_raw_xy(-180.0, 0.0),
                Point::new_raw_xy(-180.0, 10.0),
            )
            .unwrap();
        let expected = raw_stored_perimeter(&metric, &shape)
            - 2.0 * metric.chart_edge_length(&shared).unwrap();
        let actual = perimeter(&metric, &shape, metric.frame);
        assert!(
            (actual - expected).abs() < 1.0e-7,
            "partial seam perimeter={actual:.12}, expected={expected:.12}"
        );
    }

    #[test]
    fn seam_duplicates_retain_same_side_copies_and_reduce_east_east_west_to_two_sides() {
        let metric = metric();
        let east_twice = multi_polygon(vec![
            box_polygon(170.0, -10.0, 180.0, 10.0),
            box_polygon(170.0, -10.0, 180.0, 10.0),
        ]);
        let twice_east = perimeter(&metric, &east_twice, metric.frame);
        assert_eq!(
            twice_east.to_bits(),
            raw_stored_perimeter(&metric, &east_twice).to_bits()
        );

        let triple = multi_polygon(vec![
            box_polygon(170.0, -10.0, 180.0, 10.0),
            box_polygon(170.0, -10.0, 180.0, 10.0),
            box_polygon(-180.0, -10.0, -170.0, 10.0),
        ]);
        let shared = metric
            .chart_edge(
                Point::new_raw_xy(-180.0, -10.0),
                Point::new_raw_xy(-180.0, 10.0),
            )
            .unwrap();
        let expected = raw_stored_perimeter(&metric, &triple)
            - 2.0 * metric.chart_edge_length(&shared).unwrap();
        let actual = perimeter(&metric, &triple, metric.frame);
        assert!(
            (actual - expected).abs() < 1.0e-7,
            "E,E,W perimeter={actual:.12}, expected={expected:.12}"
        );
    }

    #[test]
    fn hole_and_multipolygon_perimeters_keep_unshared_boundaries() {
        let metric = metric();
        let hole_coordinates = [(50.0, 70.0), (60.0, 70.0), (60.0, 80.0), (50.0, 80.0)];
        let donut = Shape::Polygon(Polygon::new(ring(&north_closure(40.0, 60.0, false)), vec![
            ring(&hole_coordinates),
        ]));
        let hole_shape = Shape::Polygon(Polygon::new(ring(&hole_coordinates), Vec::new()));
        let expected = analytic_parallel_circumference(&metric, 60.0)
            + raw_stored_perimeter(&metric, &hole_shape);
        let actual = perimeter(&metric, &donut, metric.frame);
        assert!(
            (actual - expected).abs() < 1.0e-7,
            "cap with hole={actual:.12}, expected={expected:.12}"
        );

        let cap = Polygon::new(ring(&north_closure(40.0, 60.0, false)), Vec::new());
        let multi = multi_polygon(vec![cap, box_polygon(100.0, 0.0, 110.0, 10.0)]);
        let expected = analytic_parallel_circumference(&metric, 60.0)
            + raw_stored_perimeter(
                &metric,
                &Shape::Polygon(box_polygon(100.0, 0.0, 110.0, 10.0)),
            );
        let actual = perimeter(&metric, &multi, metric.frame);
        assert!(
            (actual - expected).abs() < 1.0e-7,
            "multipolygon perimeter={actual:.12}, expected={expected:.12}"
        );
    }

    #[test]
    fn ring_rotation_reversal_and_period_respelling_preserve_exact_perimeter() {
        let metric = metric();
        let original = north_closure(40.0, 60.0, false);
        let baseline_shape = Shape::Polygon(Polygon::new(ring(&original), Vec::new()));
        let baseline = perimeter(&metric, &baseline_shape, metric.frame);

        let rotated = original
            .iter()
            .cycle()
            .skip(3)
            .take(original.len())
            .copied()
            .collect::<Vec<_>>();
        let reversed = original.iter().copied().rev().collect::<Vec<_>>();
        let shifted = original
            .iter()
            .map(|&(x, y)| (x + 360.0, y))
            .collect::<Vec<_>>();
        let per_vertex = original
            .iter()
            .enumerate()
            .map(|(index, &(x, y))| {
                let turns = [0.0, 1.0, 0.0, -1.0, 1.0, 1.0, -1.0, 0.0, -1.0, 0.0];
                (x + turns[index] * 360.0, y)
            })
            .collect::<Vec<_>>();
        for (name, points) in [
            ("rotation", rotated),
            ("reversal", reversed),
            ("whole-period lift", shifted),
            ("per-vertex lift", per_vertex),
        ] {
            let shape = Shape::Polygon(Polygon::new(ring(&points), Vec::new()));
            let actual = perimeter(&metric, &shape, metric.frame);
            assert_eq!(
                actual.to_bits(),
                baseline.to_bits(),
                "{name} changed perimeter from {baseline:.17} to {actual:.17}"
            );
        }
    }

    #[test]
    fn near_equal_grads_longitudes_are_not_coalesced_by_a_tolerance() {
        let factor = AngularFactor::try_new("grad", std::f64::consts::PI / 200.0).unwrap();
        let frame = GeographicFrame::try_new(factor, factor, 0.0).unwrap();
        let metric = metric_with_flattening_and_frame(1.0 / 298.257_223_563, frame);
        let shape = Shape::Polygon(Polygon::new(
            ring(&[(40.0, 10.0), (50.0, 20.0), (450.0, 20.0), (440.0, 10.0)]),
            Vec::new(),
        ));
        let stored = raw_stored_perimeter(&metric, &shape);
        let actual = perimeter(&metric, &shape, frame);
        assert_eq!(actual.to_bits(), stored.to_bits());
    }

    #[test]
    fn seam_spelling_at_five_hundred_forty_still_cancels_the_same_ring_pair() {
        let metric = metric();
        let shape = Shape::Polygon(Polygon::new(
            ring(&[
                (530.0, 0.0),
                (540.0, 0.0),
                (540.0, 20.0),
                (530.0, 20.0),
                (530.0, 15.0),
                (540.0, 15.0),
                (540.0, 5.0),
                (530.0, 5.0),
            ]),
            Vec::new(),
        ));
        let shared = metric
            .chart_edge(
                Point::new_raw_xy(-180.0, 5.0),
                Point::new_raw_xy(-180.0, 15.0),
            )
            .unwrap();
        let expected = raw_stored_perimeter(&metric, &shape)
            - 2.0 * metric.chart_edge_length(&shared).unwrap();
        let actual = perimeter(&metric, &shape, metric.frame);
        assert!(
            (actual - expected).abs() < 1.0e-7,
            "seam-spelled perimeter={actual:.12}, expected={expected:.12}"
        );
    }

    #[test]
    fn a_later_multipolygon_ring_with_a_lifted_pair_is_not_screened_away() {
        let metric = metric();
        let first = box_polygon(10.0, -10.0, 20.0, 10.0);
        let closure = Polygon::new(ring(&north_closure(0.0, 50.0, false)), Vec::new());
        let shape = multi_polygon(vec![first.clone(), closure]);
        let expected = analytic_parallel_circumference(&metric, 50.0)
            + raw_stored_perimeter(&metric, &Shape::Polygon(first));
        let actual = perimeter(&metric, &shape, metric.frame);
        assert!(
            (actual - expected).abs() < 1.0e-7,
            "multipolygon perimeter={actual:.12}, expected={expected:.12}"
        );
    }

    #[test]
    fn cached_bounds_and_longitude_fold_take_the_same_slow_perimeter_path() {
        let metric = metric();
        let cases = [
            Shape::Polygon(box_polygon(-10.0, -10.0, 20.0, 30.0)),
            Shape::Polygon(Polygon::new(
                ring(&oblique_north_closure(0.0, 60.0, true)),
                Vec::new(),
            )),
            Shape::Polygon(Polygon::new(
                ring(&full_longitude_band(40.0, -60.0, 60.0)),
                Vec::new(),
            )),
            multi_polygon(vec![
                box_polygon(170.0, -10.0, 180.0, 10.0),
                box_polygon(-180.0, -10.0, -170.0, 10.0),
            ]),
        ];
        let expanded_bounds = Bounds::new_raw(-1000.0, -90.0, 1000.0, 90.0);
        for shape in cases {
            let folded = perimeter(&metric, &shape, metric.frame);
            let cached_bounds = crate::geometry::geographic_polygonal_perimeter(
                &metric,
                &shape,
                metric.frame,
                shape.bounds(),
            )
            .unwrap();
            let forced_slow = crate::geometry::geographic_polygonal_perimeter(
                &metric,
                &shape,
                metric.frame,
                Some(expanded_bounds),
            )
            .unwrap();
            for actual in [cached_bounds, forced_slow] {
                assert!(
                    (actual - folded).abs() <= 1.0e-12 * folded.abs().max(1.0),
                    "screened={folded:.17}, cached/slow={actual:.17}"
                );
            }
        }
    }

    #[test]
    #[expect(
        clippy::excessive_precision,
        reason = "40-digit mpmath oracle literal preserves the independent reference value"
    )]
    fn chart_area_subtracts_holes_against_high_precision_literals() {
        let metric = metric();
        let frame = metric.frame;
        let donut = polygon(&[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0)], &[&[
            (1.0, 1.0),
            (3.0, 1.0),
            (3.0, 3.0),
            (1.0, 3.0),
        ]]);
        let area = chart_polygon_area(&metric, &donut, frame).unwrap();
        let expected =
            147_584_819_008.449_501_378_451_988_766_978_276_231_871_241_968_190_528_449_191_370_78;
        assert!(
            (area - expected).abs() <= 1.0e-13 * expected,
            "donut area={area:.17}, expected={expected:.17}"
        );
    }

    #[test]
    #[expect(
        clippy::excessive_precision,
        reason = "40-digit mpmath oracle literal preserves the independent reference value"
    )]
    fn chart_area_uses_typed_topology_poles_and_rejects_ambiguous_turns() {
        let metric = metric();
        let frame = metric.frame;
        let north_cap = polygon(
            &[
                (-180.0, 80.0),
                (-90.0, 80.0),
                (0.0, 80.0),
                (90.0, 80.0),
                (180.0, 80.0),
            ],
            &[],
        );
        assert_eq!(
            ring_bounded_pole(&north_cap.shell, frame),
            Some(crate::geographic::Pole::North)
        );
        let north_cap_area = chart_polygon_area(&metric, &north_cap, frame).unwrap();
        let north_cap_expected =
            3_908_572_761_836.572_209_792_944_327_893_186_980_909_264_547_994_116_673_544_607_356_9;
        assert!(
            (north_cap_area - north_cap_expected).abs() <= 1.0e-14 * north_cap_expected,
            "north cap area={north_cap_area:.17}, expected={north_cap_expected:.17}"
        );

        let straddling = polygon(
            &[
                (-180.0, 20.0),
                (-90.0, -20.0),
                (0.0, 20.0),
                (90.0, -20.0),
                (180.0, 20.0),
            ],
            &[],
        );
        let shape = Shape::Polygon(straddling);
        assert!(matches!(
            crate::geographic::ScalarGeographicView::try_new(frame, &shape),
            Err(diagnostic)
                if matches!(
                    diagnostic.source,
                    crate::geographic::GeographicError::AmbiguousFullTurnRing { .. }
                )
        ));
    }
}
