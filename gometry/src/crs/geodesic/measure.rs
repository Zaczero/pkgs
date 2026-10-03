use geographiclib_rs::Geodesic;

use crate::crs::RuntimeState;
use crate::crs::geodesic::{
    CrsError, DirectGeodesic as _, EllipsoidMetric, GeodesicDirectInfo,
    ensure_geographic_domain_in_frame, geographic_metric_frame_at, inverse_distance, normalize,
    with_geodesic_at, with_geodesic_erased_at,
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
    with_geodesic_erased_at(crs, shapes, state, |error| error, f)
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

pub(crate) fn chart_shape_length(metric: &impl ChartMetric, shape: &Shape) -> Result<f64> {
    let length = match shape {
        Shape::Point(_) | Shape::MultiPoint(_) | Shape::Empty(..) => 0.0,
        Shape::LineString(points) => chart_line_length(metric, points)?,
        Shape::MultiLineString(lines) => {
            compensated_sum(lines.iter().map(|line| chart_line_length(metric, line)))?
        },
        Shape::Polygon(polygon) => chart_polygonal_perimeter(metric, std::iter::once(polygon))?,
        Shape::MultiPolygon(polygons) => chart_polygonal_perimeter(metric, polygons.iter())?,
        Shape::GeometryCollection(geometries) => compensated_sum(
            geometries
                .iter()
                .map(|geometry| chart_shape_length(metric, geometry)),
        )?,
    };
    Ok(canonicalize_zero(length))
}

fn chart_polygonal_perimeter<'a>(
    metric: &impl ChartMetric,
    polygons: impl IntoIterator<Item = &'a Polygon>,
) -> Result<f64> {
    let mut perimeter = 0.0;
    let mut correction = 0.0;
    let mut seam_edges = SeamEdges::default();
    for polygon in polygons {
        add_ring_perimeter(
            metric,
            &polygon.shell,
            &mut perimeter,
            &mut correction,
            &mut seam_edges,
        )?;
        for hole in polygon.holes.iter() {
            add_ring_perimeter(
                metric,
                hole,
                &mut perimeter,
                &mut correction,
                &mut seam_edges,
            )?;
        }
    }
    seam_edges.cancel(metric, &mut perimeter, &mut correction)?;
    Ok(perimeter)
}

fn add_ring_perimeter(
    metric: &impl ChartMetric,
    ring: &Ring,
    perimeter: &mut f64,
    correction: &mut f64,
    seam_edges: &mut SeamEdges,
) -> Result<()> {
    for [start, end] in ring.segment_pairs() {
        let edge = metric.chart_edge(start, end)?;
        let length = metric.chart_edge_length(&edge)?;
        add_compensated(perimeter, correction, length);
        if let Some([latitude0, latitude1]) = metric.chart_edge_seam_span(&edge) {
            let south = latitude0.min(latitude1);
            let north = latitude0.max(latitude1);
            if south < north {
                seam_edges.add_span(south, north);
            }
        }
    }
    Ok(())
}

#[derive(Default)]
struct SeamEdges {
    starts: Vec<f64>,
    ends: Vec<f64>,
}

impl SeamEdges {
    fn add_span(&mut self, south: f64, north: f64) {
        self.starts.push(canonicalize_zero(south));
        self.ends.push(canonicalize_zero(north));
    }

    #[expect(
        clippy::cast_precision_loss,
        reason = "the occurrence count scales a binary64 perimeter total"
    )]
    fn cancel(
        &mut self,
        metric: &impl ChartMetric,
        perimeter: &mut f64,
        correction: &mut f64,
    ) -> Result<()> {
        self.starts.sort_unstable_by(f64::total_cmp);
        self.ends.sort_unstable_by(f64::total_cmp);
        let mut start_cursor = 0;
        let mut end_cursor = 0;
        let mut previous = None;
        let mut active = 0usize;
        while start_cursor < self.starts.len() || end_cursor < self.ends.len() {
            let latitude = match (
                self.starts.get(start_cursor).copied(),
                self.ends.get(end_cursor).copied(),
            ) {
                (Some(start), Some(end)) => {
                    if start.total_cmp(&end).is_gt() {
                        end
                    } else {
                        start
                    }
                },
                (Some(start), None) => start,
                (None, Some(end)) => end,
                (None, None) => unreachable!("a seam event remains"),
            };

            if let Some(south) = previous
                && south < latitude
                && active > 1
            {
                // Even seam coverage is internal on the longitude quotient.
                let cancelled = active & !1;
                let length = metric.chart_seam_span_length(south, latitude)?;
                add_compensated(perimeter, correction, -(length * cancelled as f64));
            }

            let mut starts = 0usize;
            while self
                .starts
                .get(start_cursor)
                .is_some_and(|start| start.to_bits() == latitude.to_bits())
            {
                starts += 1;
                start_cursor += 1;
            }
            let mut ends = 0usize;
            while self
                .ends
                .get(end_cursor)
                .is_some_and(|end| end.to_bits() == latitude.to_bits())
            {
                ends += 1;
                end_cursor += 1;
            }
            active = active
                .checked_sub(ends)
                .and_then(|active| active.checked_add(starts))
                .expect("periodic seam spans have balanced endpoints");
            previous = Some(latitude);
        }
        debug_assert_eq!(active, 0);
        Ok(())
    }
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

    use super::{chart_polygon_area, chart_polygonal_perimeter};
    use crate::crs::geodesic::{AuthalicSeries, GeodesicMetric};
    use crate::geographic::{AngularFactor, GeographicFrame};
    use crate::geometry::types::ChartMetric;
    use crate::geometry::{Point, Polygon, Ring, Shape, ring_bounded_pole};

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

    #[test]
    fn chart_area_matches_high_precision_zone_and_triangle_literals() {
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
        let degree_length =
            chart_polygonal_perimeter(&degree_metric, std::iter::once(&degree_shape)).unwrap();
        let grad_length =
            chart_polygonal_perimeter(&grad_metric, std::iter::once(&grad_shape)).unwrap();
        let radian_length =
            chart_polygonal_perimeter(&radian_metric, std::iter::once(&radian_shape)).unwrap();

        for actual in [grad_area, radian_area] {
            assert!((actual - degree_area).abs() <= 1.0e-14 * degree_area);
        }
        for actual in [grad_length, radian_length] {
            assert!((actual - degree_length).abs() <= 1.0e-14 * degree_length);
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
        let west_perimeter = chart_polygonal_perimeter(&metric, std::iter::once(&west)).unwrap();
        let east_perimeter = chart_polygonal_perimeter(&metric, std::iter::once(&east)).unwrap();
        let combined = chart_polygonal_perimeter(&metric, [&west, &east]).unwrap();
        let shared = metric.chart_seam_span_length(0.0, 10.0).unwrap();
        let expected = west_perimeter + east_perimeter - 2.0 * shared;
        assert!(
            (combined - expected).abs() <= 1.0e-8,
            "combined={combined:.17}, expected={expected:.17}"
        );
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
