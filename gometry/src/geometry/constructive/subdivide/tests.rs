use super::*;
use crate::crs::{self, GeodesicMetric};
use crate::geometry::overlay::{partition_calls, reset_partition_calls};
use crate::geometry::{CoordSeq, Point, Polygon, Ring, ordinates, overlay};

fn ring_shape(points: &[Point]) -> Shape {
    Shape::Polygon(Polygon::new(
        Ring::from_closed(CoordSeq::try_from_points(points).unwrap()).unwrap(),
        Vec::new(),
    ))
}

fn line_shape(points: &[Point]) -> Shape {
    Shape::LineString(LineSeq::from_proven(
        CoordSeq::try_from_points(points).unwrap(),
    ))
}

fn wgs84_geodesic_metric() -> Result<(GeographicFrame, GeodesicMetric)> {
    let model = crs::metric_model(Some("EPSG:4326"))?;
    let crs::MetricModel::Geodesic { frame, .. } = &model else {
        return Err(crs::CrsError::invalid("WGS84 test CRS is not geographic"));
    };
    Ok((*frame, crs::GeodesicMetric::from_model(&model)?))
}

fn subdivision_output_vertices(parts: &[Shape]) -> usize {
    let mut counts = ShapeCountQuery::default();
    parts
        .iter()
        .map(|part| {
            counts
                .coordinate_count(part)
                .expect("subdivision output has a coordinate count")
        })
        .sum()
}

#[derive(Default)]
struct CountingWork {
    charged: u64,
}

impl Work for CountingWork {
    type Error = crate::error::Error;

    fn charge(&mut self, units: u64) -> Result<()> {
        self.charged = self
            .charged
            .checked_add(units)
            .ok_or(ResourceError::SizeOverflow)?;
        Ok(())
    }

    fn charge_product(&mut self, count: usize, ticks_per_item: usize) -> Result<()> {
        self.charge(crate::work::work_product_ticks(count, ticks_per_item)?)
    }
}

struct RemainingWork {
    remaining: u64,
    refused: Option<u64>,
}

impl Work for RemainingWork {
    type Error = crate::error::Error;

    fn charge(&mut self, units: u64) -> Result<()> {
        if units > self.remaining {
            self.refused = Some(units);
            self.remaining = 0;
            return Err(GeometryErrorKind::Invalid("test work allowance exhausted".into()).into());
        }
        self.remaining = self.remaining.wrapping_sub(units);
        Ok(())
    }

    fn charge_product(&mut self, count: usize, ticks_per_item: usize) -> Result<()> {
        self.charge(crate::work::work_product_ticks(count, ticks_per_item)?)
    }
}

fn all_points(shape: &Shape) -> Vec<Point> {
    let mut points = Vec::new();
    shape.for_each_point(|point| points.push(point));
    points
}

#[test]
fn one_exact_partition_emits_both_children() {
    let shape = ring_shape(&[
        Point::new_raw_xy(0.0, 0.0),
        Point::new_raw_xy(1.0, 0.0),
        Point::new_raw_xy(2.0, 0.0),
        Point::new_raw_xy(2.0, 1.0),
        Point::new_raw_xy(2.0, 2.0),
        Point::new_raw_xy(1.0, 2.0),
        Point::new_raw_xy(0.0, 2.0),
        Point::new_raw_xy(0.0, 1.0),
        Point::new_raw_xy(0.0, 0.0),
    ]);
    reset_partition_calls();
    reset_subdivision_work();
    let parts = shape
        .subdivide_with_context(8, false, &crate::geometry::SourceArcContext::cartesian())
        .unwrap();
    assert_eq!(parts.len(), 2);
    assert_eq!(partition_calls(), 1);
    assert_eq!(subdivision_work(), (3, 1));
    let mut counts = ShapeCountQuery::default();
    assert!(
        parts
            .iter()
            .all(|part| { counts.coordinates(part).is_ok_and(|count| count <= 8) })
    );
}

#[test]
fn geographic_subdivide_splits_a_wide_computed_cut_edge() {
    let shape = ring_shape(&[
        Point::new_raw_xy(-150.0, -500.0),
        Point::new_raw_xy(-50.0, -500.0),
        Point::new_raw_xy(50.0, -500.0),
        Point::new_raw_xy(150.0, -500.0),
        Point::new_raw_xy(150.0, 0.0),
        Point::new_raw_xy(150.0, 500.0),
        Point::new_raw_xy(50.0, 500.0),
        Point::new_raw_xy(-50.0, 500.0),
        Point::new_raw_xy(-150.0, 500.0),
        Point::new_raw_xy(-150.0, 0.0),
        Point::new_raw_xy(-150.0, -500.0),
    ]);
    let longitude =
        crate::geographic::AngularFactor::try_new("longitude", std::f64::consts::PI / 180.0)
            .unwrap();
    let latitude =
        crate::geographic::AngularFactor::try_new("latitude", std::f64::consts::PI / 2000.0)
            .unwrap();
    let frame = crate::geographic::GeographicFrame::try_new(longitude, latitude, 0.0).unwrap();
    let no_metric = || -> Result<&crs::GeodesicMetric> {
        Err(crs::CrsError::invalid(
            "unexpected source-arc metric access",
        ))
    };
    let parts = shape
        .subdivide_with_context(
            8,
            false,
            &crate::geometry::SourceArcContext::new(
                crate::geometry::TopologyDomain::Geographic(frame),
                Some(&no_metric),
            ),
        )
        .unwrap();
    assert!(parts.len() >= 2);
    let mut counts = ShapeCountQuery::default();
    assert!(
        parts
            .iter()
            .all(|part| counts.coordinates(part).is_ok_and(|count| count <= 8))
    );
    assert!(
        parts
            .iter()
            .flat_map(all_points)
            .any(|point| point == Point::new_raw_xy(0.0, -250.0)),
        "the 300-degree generated cut edge needs its canonical midpoint: {parts:?}"
    );
    for point in all_points(&shape) {
        assert!(
            parts
                .iter()
                .flat_map(all_points)
                .any(|output| output == point),
            "subdivision lost source vertex {point:?}: {parts:?}"
        );
    }
}

#[test]
fn geographic_adjacent_binary64_line_keeps_the_no_cut_fallback() {
    let adjacent = f64::from_bits(1.0_f64.to_bits() + 1);
    let points = (0..9)
        .map(|index| Point::new_raw_xy(if index % 2 == 0 { 1.0 } else { adjacent }, 0.0))
        .collect::<Vec<_>>();
    let shape = line_shape(&points);
    let context = SourceArcContext::new(
        crate::geometry::TopologyDomain::Geographic(GeographicFrame::crs84()),
        None,
    );

    reset_partition_calls();
    let parts = shape
        .subdivide_with_context(8, false, &context)
        .expect("adjacent binary64 coordinates use the source-vertex fallback");

    assert_eq!(parts.len(), 2);
    assert_eq!(partition_calls(), 0);
    let mut counts = ShapeCountQuery::default();
    assert!(parts.iter().all(|part| {
        counts
            .coordinate_count(part)
            .is_some_and(|count| count <= 8)
    }));
}

#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "assertions define the test oracle while Result propagates setup errors"
)]
fn ordinary_geographic_xy_polygon_skips_unneeded_root_catalog() -> Result<()> {
    let mut points = (0..127)
        .map(|index| {
            let angle = 2.0 * std::f64::consts::PI * f64::from(index) / 127.0;
            Point::new_raw_xy(8.0 * angle.cos(), 8.0 * angle.sin())
        })
        .collect::<Vec<_>>();
    points.push(points[0]);
    let shape = ring_shape(&points);
    let context: SourceArcContext<'_, '_, crs::GeodesicMetric> = SourceArcContext::new(
        crate::geometry::TopologyDomain::Geographic(GeographicFrame::crs84()),
        None,
    );
    let mut parts = Vec::new();
    let mut budget = ExpansionBudget::new("subdivide", "max_vertices");

    reset_ordinary_root_catalog_builds();
    ordinates::reset_root_range_mapping_counts();
    overlay::reset_subdivision_root_xy_position_calls();
    split(shape, 16, false, &context, &mut parts, &mut budget)?;

    assert_eq!(parts.len(), 16);
    assert_eq!(ordinary_root_catalog_builds(), 0);
    assert_eq!(ordinates::root_range_mapping_counts(), (0, 0));
    assert_eq!(overlay::subdivision_root_xy_position_calls(), 0);
    let mut counts = ShapeCountQuery::default();
    assert!(parts.iter().all(|part| {
        counts
            .coordinate_count(part)
            .is_some_and(|count| count <= 16)
    }));
    Ok(())
}

#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "assertions define the test oracle while Result propagates setup errors"
)]
fn ordinary_geographic_xy_line_without_chart_transit_skips_root_catalog() -> Result<()> {
    let points = (0..128)
        .map(|index| {
            let fraction = f64::from(index) / 127.0;
            Point::new_raw_xy(
                -9.0 + 18.0 * fraction,
                2.0 * (3.0 * std::f64::consts::PI * fraction).sin(),
            )
        })
        .collect::<Vec<_>>();
    let shape = line_shape(&points);
    let context: SourceArcContext<'_, '_, crs::GeodesicMetric> = SourceArcContext::new(
        crate::geometry::TopologyDomain::Geographic(GeographicFrame::crs84()),
        None,
    );
    let mut parts = Vec::new();
    let mut budget = ExpansionBudget::new("subdivide", "max_vertices");

    reset_ordinary_root_catalog_builds();
    ordinates::reset_root_range_mapping_counts();
    overlay::reset_subdivision_root_xy_position_calls();
    split(shape, 16, false, &context, &mut parts, &mut budget)?;

    assert_eq!(parts.len(), 16);
    assert_eq!(ordinary_root_catalog_builds(), 0);
    assert_eq!(ordinates::root_range_mapping_counts(), (0, 0));
    assert_eq!(overlay::subdivision_root_xy_position_calls(), 0);
    assert!(parts.iter().all(|part| {
        ShapeCountQuery::default()
            .coordinate_count(part)
            .is_some_and(|count| count <= 16)
    }));

    let wide_points = (0..128)
        .map(|index| {
            let fraction = f64::from(index) / 127.0;
            Point::new_raw_xy(
                -170.0 + 340.0 * fraction,
                2.0 * (3.0 * std::f64::consts::PI * fraction).sin(),
            )
        })
        .collect::<Vec<_>>();
    let mut wide_parts = Vec::new();
    let mut wide_budget = ExpansionBudget::new("subdivide", "max_vertices");
    reset_ordinary_root_catalog_builds();
    ordinates::reset_root_range_mapping_counts();
    overlay::reset_subdivision_root_xy_position_calls();
    split(
        line_shape(&wide_points),
        16,
        false,
        &context,
        &mut wide_parts,
        &mut wide_budget,
    )?;
    assert_eq!(wide_parts.len(), 16);
    assert_eq!(ordinary_root_catalog_builds(), 0);
    assert_eq!(ordinates::root_range_mapping_counts(), (0, 0));
    assert_eq!(overlay::subdivision_root_xy_position_calls(), 0);
    Ok(())
}

#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "assertions define the test oracle while Result propagates setup errors"
)]
fn ordinary_geographic_xy_line_builds_one_root_catalog_across_cuts() -> Result<()> {
    let mut points = vec![
        Point::new_raw_xy(179.0, 0.0),
        Point::new_raw_xy(-179.0, 30.0),
    ];
    points.extend((0..126).map(|index| {
        let fraction = f64::from(index) / 125.0;
        Point::new_raw_xy(-178.0 + 28.0 * fraction, 31.0 + 4.0 * fraction)
    }));
    let shape = line_shape(&points);
    let context: SourceArcContext<'_, '_, crs::GeodesicMetric> = SourceArcContext::new(
        crate::geometry::TopologyDomain::Geographic(GeographicFrame::crs84()),
        None,
    );
    let mut parts = Vec::new();
    let mut budget = ExpansionBudget::new("subdivide", "max_vertices");

    reset_ordinary_root_catalog_builds();
    ordinates::reset_root_range_mapping_counts();
    overlay::reset_subdivision_root_xy_position_calls();
    reset_subdivision_work();
    split(shape, 16, false, &context, &mut parts, &mut budget)?;

    assert_eq!(ordinary_root_catalog_builds(), 1);
    assert!(subdivision_work().1 > 1);
    assert!(parts.iter().all(|part| {
        ShapeCountQuery::default()
            .coordinate_count(part)
            .is_some_and(|count| count <= 16)
    }));
    let (endpoint_requests, interior_mappings) = ordinates::root_range_mapping_counts();
    assert!(endpoint_requests > 0);
    assert!(
        interior_mappings <= subdivision_output_vertices(&parts).saturating_mul(4),
        "only inserted cut vertices need interior original-root mapping"
    );
    assert!(
        overlay::subdivision_root_xy_position_calls()
            <= subdivision_output_vertices(&parts).saturating_mul(4),
        "root XY correction should only visit emitted cut vertices"
    );
    Ok(())
}

#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "assertions define the test oracle while Result propagates setup errors"
)]
fn ordinary_geographic_xy_polygon_seam_uses_partition_roots_only() -> Result<()> {
    let points = [
        (179.0, -2.0),
        (-179.0, -2.0),
        (-179.0, -1.0),
        (-179.0, 0.0),
        (-179.0, 1.0),
        (-179.0, 2.0),
        (179.0, 2.0),
        (179.0, 1.0),
        (179.0, 0.0),
        (179.0, -1.0),
        (179.0, -2.0),
    ]
    .map(|(x, y)| Point::new_raw_xy(x, y));
    let shape = ring_shape(&points);
    let context: SourceArcContext<'_, '_, crs::GeodesicMetric> = SourceArcContext::new(
        crate::geometry::TopologyDomain::Geographic(GeographicFrame::crs84()),
        None,
    );
    let mut parts = Vec::new();
    let mut budget = ExpansionBudget::new("subdivide", "max_vertices");

    reset_ordinary_root_catalog_builds();
    ordinates::reset_root_range_mapping_counts();
    overlay::reset_subdivision_root_xy_position_calls();
    split(shape, 8, false, &context, &mut parts, &mut budget)?;

    assert_eq!(parts.len(), 2);
    assert_eq!(ordinary_root_catalog_builds(), 0);
    assert_eq!(ordinates::root_range_mapping_counts(), (0, 0));
    assert_eq!(overlay::subdivision_root_xy_position_calls(), 0);
    assert!(parts.iter().all(|part| {
        ShapeCountQuery::default()
            .coordinate_count(part)
            .is_some_and(|count| count <= 8)
    }));
    Ok(())
}

#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "assertions define the test oracle while Result propagates setup errors"
)]
fn ordinary_geographic_zm_polygon_builds_one_root_catalog_and_maps_cut_vertices() -> Result<()> {
    let (frame, metric) = wgs84_geodesic_metric()?;
    let metric_access = || -> Result<&crs::GeodesicMetric> { Ok(&metric) };
    let context = SourceArcContext::new(
        crate::geometry::TopologyDomain::Geographic(frame),
        Some(&metric_access),
    );
    let mut points = (0..127)
        .map(|index| {
            let angle = 2.0 * std::f64::consts::PI * f64::from(index) / 127.0;
            Point::new_raw_axes(
                8.0 * angle.cos(),
                8.0 * angle.sin(),
                crate::geometry::ZOrdinate(Some(f64::from(index) + 1.0)),
                crate::geometry::MOrdinate(Some(1000.0 + f64::from(index))),
            )
        })
        .collect::<Vec<_>>();
    points.push(points[0]);
    let shape = ring_shape(&points);
    let mut parts = Vec::new();
    let mut budget = ExpansionBudget::new("subdivide", "max_vertices");

    reset_ordinary_root_catalog_builds();
    ordinates::reset_root_range_mapping_counts();
    overlay::reset_subdivision_root_xy_position_calls();
    split(shape, 16, false, &context, &mut parts, &mut budget)?;

    assert_eq!(parts.len(), 16);
    assert_eq!(ordinary_root_catalog_builds(), 1);
    let (endpoint_requests, interior_mappings) = ordinates::root_range_mapping_counts();
    assert!(endpoint_requests > 0);
    assert!(
        interior_mappings <= subdivision_output_vertices(&parts).saturating_mul(4),
        "interior original-root mappings should track inserted output vertices"
    );
    assert_eq!(overlay::subdivision_root_xy_position_calls(), 0);
    Ok(())
}

#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "assertions define the test oracle while Result propagates setup errors"
)]
fn ordinary_geographic_zm_line_builds_one_root_catalog_and_maps_cut_vertices() -> Result<()> {
    let (frame, metric) = wgs84_geodesic_metric()?;
    let metric_access = || -> Result<&crs::GeodesicMetric> { Ok(&metric) };
    let context = SourceArcContext::new(
        crate::geometry::TopologyDomain::Geographic(frame),
        Some(&metric_access),
    );
    let points = (0..128)
        .map(|index| {
            let fraction = f64::from(index) / 127.0;
            Point::new_raw_axes(
                -9.0 + 18.0 * fraction,
                2.0 * (3.0 * std::f64::consts::PI * fraction).sin(),
                crate::geometry::ZOrdinate(Some(f64::from(index) + 1.0)),
                crate::geometry::MOrdinate(Some(1000.0 + f64::from(index))),
            )
        })
        .collect::<Vec<_>>();
    let shape = line_shape(&points);
    let mut parts = Vec::new();
    let mut budget = ExpansionBudget::new("subdivide", "max_vertices");

    reset_ordinary_root_catalog_builds();
    ordinates::reset_root_range_mapping_counts();
    overlay::reset_subdivision_root_xy_position_calls();
    split(shape, 16, false, &context, &mut parts, &mut budget)?;

    assert_eq!(parts.len(), 16);
    assert_eq!(ordinary_root_catalog_builds(), 1);
    let (endpoint_requests, interior_mappings) = ordinates::root_range_mapping_counts();
    assert!(endpoint_requests > 0);
    assert!(
        interior_mappings <= subdivision_output_vertices(&parts).saturating_mul(4),
        "interior original-root mappings should track inserted output vertices"
    );
    assert_eq!(overlay::subdivision_root_xy_position_calls(), 0);
    Ok(())
}

#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "assertions define the test oracle while Result propagates setup errors"
)]
fn ordinary_geographic_zm_subdivision_reports_per_cut_root_work() -> Result<()> {
    let (frame, metric) = wgs84_geodesic_metric()?;
    let metric_access = || -> Result<&crs::GeodesicMetric> { Ok(&metric) };
    let context = SourceArcContext::new(
        crate::geometry::TopologyDomain::Geographic(frame),
        Some(&metric_access),
    );
    for kind in ["line", "polygon"] {
        for input_vertices in [32, 64, 128, 256] {
            let mut points = (0..input_vertices)
                .map(|index| {
                    let fraction = f64::from(index) / f64::from(input_vertices - 1);
                    let (x, y) = if kind == "polygon" {
                        let angle =
                            std::f64::consts::TAU * f64::from(index) / f64::from(input_vertices);
                        (8.0 * angle.cos(), 8.0 * angle.sin())
                    } else {
                        (
                            -9.0 + 18.0 * fraction,
                            2.0 * (3.0 * std::f64::consts::PI * fraction).sin(),
                        )
                    };
                    Point::new_raw_axes(
                        x,
                        y,
                        crate::geometry::ZOrdinate(Some(f64::from(index) + 1.0)),
                        crate::geometry::MOrdinate(Some(f64::from(index) + 1000.0)),
                    )
                })
                .collect::<Vec<_>>();
            let shape = if kind == "polygon" {
                points.push(points[0]);
                ring_shape(&points)
            } else {
                line_shape(&points)
            };
            let mut parts = Vec::new();
            let mut budget = ExpansionBudget::new("subdivide", "max_vertices");

            ordinates::reset_root_range_mapping_counts();
            overlay::reset_subdivision_resolution_batches();
            overlay::reset_source_span_view_order_counts();
            drop(ordinates::take_source_arc_observations());
            split(shape, 16, false, &context, &mut parts, &mut budget)?;

            let table_builds = ordinates::root_request_table_builds();
            let local_root_row_sorts = ordinates::root_request_row_sorts();
            let resolution_batches = overlay::take_subdivision_resolution_batches();
            let (
                source_span_sorts,
                ordered_source_span_views,
                source_span_endpoint_parameters,
                source_span_interior_parameters,
            ) = overlay::source_span_view_order_counts();
            let root_observations = ordinates::take_source_arc_observations();
            assert!(
                parts
                    .iter()
                    .all(|part| part.axes().has_z() && part.axes().has_m())
            );
            let emitted_vertices = subdivision_output_vertices(&parts);
            let cuts = parts.len().saturating_sub(1);
            let (resolved_rows, root_requests, unique_root_requests) =
                resolution_batches.iter().copied().fold(
                    (0_usize, 0_usize, 0_usize),
                    |(rows, requests, unique), batch| {
                        (
                            rows.saturating_add(batch.0),
                            requests.saturating_add(batch.1),
                            unique.saturating_add(batch.2),
                        )
                    },
                );
            assert_eq!(table_builds, 0);
            assert_eq!(resolution_batches.len(), cuts);
            assert!(ordered_source_span_views > 0);
            assert!(source_span_sorts <= cuts.saturating_mul(4));
            assert!(source_span_sorts > 0);
            assert!(source_span_endpoint_parameters > 0);
            assert!(source_span_interior_parameters <= cuts.saturating_mul(8));
            assert!(resolution_batches.iter().all(|&(rows, requests, unique)| {
                rows <= 2 && requests <= rows.saturating_mul(2) && unique == rows
            }));
            assert!(resolved_rows <= emitted_vertices);
            assert!(resolved_rows <= cuts.saturating_add(1));
            assert!(local_root_row_sorts <= emitted_vertices);
            assert!(root_requests <= resolved_rows.saturating_mul(2));
            assert!(unique_root_requests <= root_requests);
            assert_eq!(unique_root_requests, resolved_rows);
            assert!(root_observations.len() <= cuts.saturating_add(1) * 2);
        }
    }
    Ok(())
}

#[test]
#[expect(
    clippy::float_cmp,
    reason = "these coordinates assert exact source-root identity; a tolerance could accept a displaced cut"
)]
#[expect(
    clippy::panic_in_result_fn,
    reason = "assertions define the test oracle while Result propagates setup errors"
)]
fn ordinary_geographic_subdivision_resolves_two_cuts_on_the_original_root() -> Result<()> {
    let crs = concat!(
        "GEOGCS[\"Custom grad\",DATUM[\"Custom datum\",",
        "SPHEROID[\"Custom\",7000000,150]],PRIMEM[\"Greenwich\",0],",
        "UNIT[\"grad\",0.015707963267948967],",
        "AXIS[\"Longitude\",EAST],AXIS[\"Latitude\",NORTH]]"
    );
    let model = crs::metric_model(Some(crs))?;
    let crs::MetricModel::Geodesic { frame, .. } = &model else {
        return Err(crs::CrsError::invalid("custom test CRS is not geographic"));
    };
    let metric = crs::GeodesicMetric::from_model(&model)?;
    let metric_access_calls = std::cell::Cell::new(0_usize);
    let metric_access = || -> Result<&crs::GeodesicMetric> {
        metric_access_calls.set(metric_access_calls.get().saturating_add(1));
        Ok(&metric)
    };
    let context = SourceArcContext::new(
        crate::geometry::TopologyDomain::Geographic(*frame),
        Some(&metric_access),
    );
    let degrees = [
        (2.0, 0.0),
        (4.0, 1.0),
        (0.0, 0.0),
        (12.0, 6.0),
        (10.0, 6.1),
        (8.0, 6.2),
        (6.0, 6.3),
        (8.0, 6.4),
        (10.0, 6.5),
        (12.0, 6.6),
        (10.0, 6.7),
        (8.0, 6.8),
    ];
    let points = degrees
        .iter()
        .enumerate()
        .map(|(index, &(longitude, latitude))| {
            let z = match index {
                0 => 20.0,
                1 => 30.0,
                2 => 10.0,
                3 => 210.0,
                _ => 310.0 + 100.0 * (index - 4) as f64,
            };
            let m = match index {
                0 => 200.0,
                1 => 300.0,
                2 => 100.0,
                3 => 1100.0,
                _ => 2100.0 + 1000.0 * (index - 4) as f64,
            };
            Point::new_raw_axes(
                longitude * (10.0 / 9.0),
                latitude * (10.0 / 9.0),
                crate::geometry::ZOrdinate(Some(z)),
                crate::geometry::MOrdinate(Some(m)),
            )
        })
        .collect::<Vec<_>>();
    let shape = line_shape(&points);
    let mut parts = Vec::new();
    let mut budget = ExpansionBudget::new("subdivide", "max_vertices");

    drop(ordinates::take_source_arc_observations());
    split(shape, 8, false, &context, &mut parts, &mut budget)?;
    let observations = ordinates::take_source_arc_observations();
    assert_eq!(metric_access_calls.get(), observations.len());
    let output_points = parts.iter().flat_map(all_points).collect::<Vec<_>>();
    let scale = 10.0 / 9.0;
    assert!(
        output_points
            .iter()
            .any(|point| point.x == 5.0 * scale && point.y == 2.5 * scale)
    );
    assert!(
        output_points
            .iter()
            .any(|point| point.x == 7.0 * scale && point.y == 3.5 * scale)
    );
    let second_cut = output_points
        .iter()
        .find(|point| point.x == 7.0 * scale && point.y == 3.5 * scale)
        .expect("the right child is cut a second time at the original-root site");
    let root_request = observations
        .iter()
        .find(|observation| {
            observation.source[0].x == 0.0
                && observation.source[0].y == 0.0
                && observation.source[1].x == 12.0 * scale
                && observation.source[1].y == 6.0 * scale
                && (observation.parameter - 7.0 / 12.0).abs() <= 1e-15
        })
        .expect("the second cut resolves against the original grad-frame edge");
    assert_eq!(second_cut.z(), root_request.z);
    assert_eq!(second_cut.m(), root_request.m);
    assert!(
        (second_cut.z().unwrap() - 126.777_239_929_426_68).abs() <= 2e-5,
        "the original ellipsoidal arc gives the second cut Z"
    );
    assert!(
        (second_cut.m().unwrap() - 683.886_199_647_133_4).abs() <= 1e-4,
        "the original ellipsoidal arc gives the second cut M"
    );
    let mut counts = ShapeCountQuery::default();
    assert!(parts.iter().all(|part| {
        counts
            .coordinate_count(part)
            .is_some_and(|count| count <= 8)
    }));
    Ok(())
}

#[test]
#[expect(
    clippy::float_cmp,
    reason = "these coordinates assert exact source-root identity; a tolerance could accept a displaced cut"
)]
#[expect(
    clippy::panic_in_result_fn,
    reason = "assertions define the test oracle while Result propagates setup errors"
)]
fn ordinary_geographic_polygon_subdivision_carries_shell_roots_across_cuts() -> Result<()> {
    let crs = concat!(
        "GEOGCS[\"Custom grad\",DATUM[\"Custom datum\",",
        "SPHEROID[\"Custom\",7000000,150]],PRIMEM[\"Greenwich\",0],",
        "UNIT[\"grad\",0.015707963267948967],",
        "AXIS[\"Longitude\",EAST],AXIS[\"Latitude\",NORTH]]"
    );
    let model = crs::metric_model(Some(crs))?;
    let crs::MetricModel::Geodesic { frame, .. } = &model else {
        return Err(crs::CrsError::invalid("custom test CRS is not geographic"));
    };
    let metric = crs::GeodesicMetric::from_model(&model)?;
    let metric_access_calls = std::cell::Cell::new(0_usize);
    let metric_access = || -> Result<&crs::GeodesicMetric> {
        metric_access_calls.set(metric_access_calls.get().saturating_add(1));
        Ok(&metric)
    };
    let context = SourceArcContext::new(
        crate::geometry::TopologyDomain::Geographic(*frame),
        Some(&metric_access),
    );
    let scale = 10.0 / 9.0;
    let xy = [
        (0.0, 0.0),
        (12.0, 6.0),
        (12.0, 6.3),
        (12.0, 6.6),
        (12.0, 7.0),
        (12.0, 7.5),
        (10.0, 7.5),
        (8.0, 7.5),
        (6.0, 7.5),
        (4.0, 7.5),
        (2.0, 7.5),
        (0.0, 7.5),
        (0.0, 0.0),
    ];
    let z = [
        10.0, 210.0, 310.0, 410.0, 510.0, 610.0, 710.0, 810.0, 910.0, 1010.0, 1110.0, 1210.0, 10.0,
    ];
    let m = [
        100.0, 1100.0, 2100.0, 3100.0, 4100.0, 5100.0, 6100.0, 7100.0, 8100.0, 9100.0, 10100.0,
        11100.0, 100.0,
    ];
    let points = xy
        .iter()
        .zip(z)
        .zip(m)
        .map(|((&(x, y), z), m)| {
            Point::new_raw_axes(
                x * scale,
                y * scale,
                crate::geometry::ZOrdinate(Some(z)),
                crate::geometry::MOrdinate(Some(m)),
            )
        })
        .collect::<Vec<_>>();
    let shape = ring_shape(&points);
    let mut parts = Vec::new();
    let mut budget = ExpansionBudget::new("subdivide", "max_vertices");

    drop(ordinates::take_source_arc_observations());
    split(shape, 8, false, &context, &mut parts, &mut budget)?;
    let observations = ordinates::take_source_arc_observations();
    let output_points = parts.iter().flat_map(all_points).collect::<Vec<_>>();
    assert!(
        output_points
            .iter()
            .any(|point| point.x == 5.0 * scale && point.y == 2.5 * scale)
    );
    let second_cut = output_points
        .iter()
        .find(|point| point.x == 7.0 * scale && point.y == 3.5 * scale)
        .expect("the right polygon child is cut at the second source-root site");
    assert!(observations.iter().any(|observation| {
        observation.source[0].x == 0.0
            && observation.source[0].y == 0.0
            && observation.source[1].x == 12.0 * scale
            && observation.source[1].y == 6.0 * scale
            && (observation.parameter - 7.0 / 12.0).abs() <= 1e-15
    }));
    assert!(metric_access_calls.get() > 0);
    assert!((second_cut.z().unwrap() - 126.777_239_929_426_68).abs() <= 2e-5);
    assert!((second_cut.m().unwrap() - 683.886_199_647_133_4).abs() <= 1e-4);
    let mut counts = ShapeCountQuery::default();
    assert!(parts.iter().all(|part| {
        counts
            .coordinate_count(part)
            .is_some_and(|count| count <= 8)
    }));
    Ok(())
}

#[test]
fn preferred_axis_cut_scans_only_the_preferred_axis_and_keeps_x_ties() {
    let x_wide = line_shape(&[
        Point::new_raw_xy(0.0, 0.0),
        Point::new_raw_xy(10.0, 1.0),
        Point::new_raw_xy(20.0, 2.0),
        Point::new_raw_xy(30.0, 3.0),
    ]);
    reset_source_ordinate_axis_scans();
    let mut work = Unmetered;
    assert!(
        preferred_source_cut(&x_wide, x_wide.bounds().unwrap(), 4, false, &mut work)
            .unwrap()
            .is_some_and(|(split_x, cut)| split_x && (0.0..30.0).contains(&cut))
    );
    assert_eq!(source_ordinate_axis_scans(), 1);

    let y_wide = line_shape(&[
        Point::new_raw_xy(0.0, 0.0),
        Point::new_raw_xy(1.0, 10.0),
        Point::new_raw_xy(2.0, 20.0),
        Point::new_raw_xy(3.0, 30.0),
    ]);
    reset_source_ordinate_axis_scans();
    let mut work = Unmetered;
    assert!(
        preferred_source_cut(&y_wide, y_wide.bounds().unwrap(), 4, false, &mut work)
            .unwrap()
            .is_some_and(|(split_x, cut)| !split_x && (0.0..30.0).contains(&cut))
    );
    assert_eq!(source_ordinate_axis_scans(), 1);

    let equal_extents = line_shape(&[
        Point::new_raw_xy(0.0, 0.0),
        Point::new_raw_xy(10.0, 1.0),
        Point::new_raw_xy(1.0, 10.0),
        Point::new_raw_xy(9.0, 9.0),
    ]);
    reset_source_ordinate_axis_scans();
    let mut work = Unmetered;
    assert!(
        preferred_source_cut(
            &equal_extents,
            equal_extents.bounds().unwrap(),
            4,
            false,
            &mut work,
        )
        .unwrap()
        .is_some_and(|(split_x, _)| split_x)
    );
    assert_eq!(source_ordinate_axis_scans(), 1);
}

#[test]
fn preferred_axis_falls_back_only_when_it_has_no_representable_cut() {
    let adjacent = f64::from_bits(1.0_f64.to_bits() + 1);
    let alternate_only = line_shape(&[
        Point::new_raw_xy(1.0, 0.0),
        Point::new_raw_xy(adjacent, 2.5e-17),
        Point::new_raw_xy(1.0, 5.0e-17),
        Point::new_raw_xy(adjacent, 1.0e-16),
    ]);
    reset_source_ordinate_axis_scans();
    let mut work = Unmetered;
    assert!(
        preferred_source_cut(
            &alternate_only,
            alternate_only.bounds().unwrap(),
            4,
            false,
            &mut work,
        )
        .unwrap()
        .is_some_and(|(split_x, cut)| !split_x && cut > 0.0 && cut < 1.0e-16)
    );
    assert_eq!(source_ordinate_axis_scans(), 2);

    let neither_axis = line_shape(&[
        Point::new_raw_xy(1.0, 1.0),
        Point::new_raw_xy(adjacent, adjacent),
        Point::new_raw_xy(1.0, 1.0),
        Point::new_raw_xy(adjacent, adjacent),
    ]);
    reset_source_ordinate_axis_scans();
    let mut work = Unmetered;
    assert_eq!(
        preferred_source_cut(
            &neither_axis,
            neither_axis.bounds().unwrap(),
            4,
            false,
            &mut work,
        )
        .unwrap(),
        None
    );
    assert_eq!(source_ordinate_axis_scans(), 2);
}

#[test]
fn preferred_axis_refusal_precedes_the_alternate_axis_scan() {
    let adjacent = f64::from_bits(1.0_f64.to_bits() + 1);
    let shape = line_shape(&[
        Point::new_raw_xy(1.0, 0.0),
        Point::new_raw_xy(adjacent, 2.5e-17),
        Point::new_raw_xy(1.0, 5.0e-17),
        Point::new_raw_xy(adjacent, 1.0e-16),
    ]);
    let bounds = shape.bounds().unwrap();
    reset_source_ordinate_axis_scans();
    let mut counting = CountingWork::default();
    assert_eq!(
        source_cut(&shape, true, bounds, 4, false, &mut counting).unwrap(),
        None
    );
    assert_eq!(source_ordinate_axis_scans(), 1);

    reset_source_ordinate_axis_scans();
    let mut work = RemainingWork {
        remaining: counting.charged,
        refused: None,
    };
    assert!(
        preferred_source_cut(&shape, bounds, 4, false, &mut work).is_err(),
        "the preferred axis account cannot refuse and then enter the alternate scan"
    );
    assert_eq!(source_ordinate_axis_scans(), 1);
    assert_eq!(work.refused, Some(4));
}

#[test]
fn preferred_axis_vertex_retry_keeps_the_bounds_midpoint_first() {
    let shape = line_shape(&[
        Point::new_raw_xy(0.0, 0.0),
        Point::new_raw_xy(10.0, 1.0),
        Point::new_raw_xy(20.0, 2.0),
        Point::new_raw_xy(30.0, 3.0),
    ]);
    reset_source_ordinate_axis_scans();
    let bounds = shape.bounds().unwrap();
    let mut work = Unmetered;
    assert_eq!(
        preferred_source_cut(&shape, bounds, 4, true, &mut work).unwrap(),
        Some((true, f64::midpoint(bounds.minx(), bounds.maxx())))
    );
    assert_eq!(source_ordinate_axis_scans(), 0);
}

#[test]
fn recursive_subdivision_keeps_each_child_count_with_its_shape() {
    let mut points = (0..32)
        .map(|index| {
            let angle = f64::from(index) * std::f64::consts::TAU / 32.0;
            Point::new_raw_xy(angle.cos(), angle.sin())
        })
        .collect::<Vec<_>>();
    points.push(points[0]);
    let shape = ring_shape(&points);

    reset_subdivision_work();
    let parts = shape
        .subdivide_with_context(8, false, &crate::geometry::SourceArcContext::cartesian())
        .unwrap();

    let (work_items, splits) = subdivision_work();
    assert!(work_items > 3);
    assert!(splits > 1);
    let mut counts = ShapeCountQuery::default();
    assert!(
        parts
            .iter()
            .all(|part| counts.coordinates(part).is_ok_and(|count| count <= 8))
    );
}

#[test]
fn source_gap_search_skips_an_unrepresentable_cluster_gap() {
    let mut points = Vec::new();
    for index in 0..8 {
        points.push(Point::new_raw_xy(
            f64::from_bits(1.0_f64.to_bits() + index),
            0.0,
        ));
    }
    points.push(Point::new_raw_xy(1_000_000.0, 0.0));
    let shape = Shape::multi_point(
        crate::geometry::CoordinateAxes::XY,
        CoordSeq::try_from_points(&points).unwrap(),
    );
    reset_partition_calls();
    let parts = shape
        .subdivide_with_context(8, false, &crate::geometry::SourceArcContext::cartesian())
        .unwrap();
    assert_eq!(parts.len(), 2);
    assert_eq!(partition_calls(), 1);
    assert!(parts.iter().all(|part| !part.is_empty()));
}

#[test]
fn adjacent_sub_ulp_line_uses_coordinate_sequence_cuts() {
    let adjacent = f64::from_bits(1.0_f64.to_bits() + 1);
    let points = (0..9)
        .map(|index| Point::new_raw_xy(if index % 2 == 0 { 1.0 } else { adjacent }, 0.0))
        .collect::<Vec<_>>();
    let shape = line_shape(&points);
    reset_partition_calls();
    let parts = shape
        .subdivide_with_context(8, false, &crate::geometry::SourceArcContext::cartesian())
        .unwrap();
    assert_eq!(parts.len(), 2);
    let mut counts = ShapeCountQuery::default();
    assert!(parts.iter().all(|part| {
        counts
            .coordinate_count(part)
            .is_some_and(|count| count <= 8)
    }));
    assert_eq!(partition_calls(), 0);
}

#[test]
fn source_ordinate_dedup_merges_signed_zero_and_keeps_adjacent_values() {
    let adjacent = f64::from_bits(1.0_f64.to_bits() + 1);
    let shape = line_shape(&[
        Point::new_raw_xy(-0.0, 0.0),
        Point::new_raw_xy(-30.0, 0.0),
        Point::new_raw_xy(0.0, 0.0),
        Point::new_raw_xy(-10.0, 0.0),
        Point::new_raw_xy(1.0, 0.0),
        Point::new_raw_xy(adjacent, 0.0),
    ]);

    let ordinates = source_ordinates(&shape, true, 6, &mut Unmetered).unwrap();

    assert_eq!(ordinates.len(), 5);
    assert_eq!(ordinates[0].to_bits(), (-30.0_f64).to_bits());
    assert_eq!(ordinates[1].to_bits(), (-10.0_f64).to_bits());
    assert_eq!(ordinates[2].abs().to_bits(), 0.0_f64.to_bits());
    assert_eq!(ordinates[3].to_bits(), 1.0_f64.to_bits());
    assert_eq!(ordinates[4].to_bits(), adjacent.to_bits());
}

fn geographic_member_line(identity: f64) -> Shape {
    let coordinates = CoordSeq::try_from_vecs(
        vec![0.0, 0.0],
        vec![0.0, 0.0],
        Some(vec![identity, identity]),
        None,
    )
    .unwrap();
    Shape::LineString(LineSeq::from_proven(coordinates))
}

fn shared_repeated_lines(depth: usize) -> Shape {
    let first = geographic_member_line(10.0);
    let second = geographic_member_line(20.0);
    let mut branch = Shape::geometry_collection(vec![first, second]).unwrap();
    for _ in 0..depth {
        branch = Shape::geometry_collection(vec![branch]).unwrap();
    }
    Shape::geometry_collection(vec![branch.clone(), branch]).unwrap()
}

fn expand_collection_owners(shape: &Shape) -> Shape {
    match shape {
        Shape::GeometryCollection(collection) => Shape::geometry_collection(
            collection
                .as_slice()
                .iter()
                .map(expand_collection_owners)
                .collect(),
        )
        .unwrap(),
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::Polygon(_)
        | Shape::MultiPolygon(_)
        | Shape::Empty(..) => shape.clone(),
    }
}

#[test]
fn geographic_shared_line_subdivision_matches_expanded_occurrences() {
    let longitude =
        crate::geographic::AngularFactor::try_new("longitude", std::f64::consts::PI / 180.0)
            .unwrap();
    let latitude =
        crate::geographic::AngularFactor::try_new("latitude", std::f64::consts::PI / 180.0)
            .unwrap();
    let frame = GeographicFrame::try_new(longitude, latitude, 0.0).unwrap();
    let no_metric = || -> Result<&crs::GeodesicMetric> {
        Err(crs::CrsError::invalid(
            "unexpected source-arc metric access",
        ))
    };
    let context = SourceArcContext::new(
        crate::geometry::TopologyDomain::Geographic(frame),
        Some(&no_metric),
    );

    for depth in [4, 8, 16] {
        let shared = shared_repeated_lines(depth);
        let expanded = expand_collection_owners(&shared);
        let shared_parts = shared
            .subdivide_with_context(2, false, &context)
            .expect("geographic-frame shared subdivision succeeds");
        let expanded_parts = expanded
            .subdivide_with_context(2, false, &context)
            .expect("expanded geographic-frame subdivision succeeds");
        let wkb = |parts: &[Shape]| {
            parts
                .iter()
                .map(|part| crate::io::to_wkb(part, None, false).unwrap())
                .collect::<Vec<_>>()
        };
        assert_eq!(shared_parts.len(), 4, "shared depth {depth}");
        assert_eq!(wkb(&shared_parts), wkb(&expanded_parts), "depth {depth}");
    }
}

#[test]
fn geographic_deep_shared_polygon_subdivision_keeps_support() {
    let base = crate::io::parse_wkt("POLYGON ((0 0,1 0,1 1,0 1,0 0))").unwrap();
    let longitude =
        crate::geographic::AngularFactor::try_new("longitude", std::f64::consts::PI / 180.0)
            .unwrap();
    let latitude =
        crate::geographic::AngularFactor::try_new("latitude", std::f64::consts::PI / 2000.0)
            .unwrap();
    let frame = GeographicFrame::try_new(longitude, latitude, 0.0).unwrap();
    let no_metric = || -> Result<&crs::GeodesicMetric> {
        Err(crs::CrsError::invalid(
            "unexpected source-arc metric access",
        ))
    };
    let context = SourceArcContext::new(
        crate::geometry::TopologyDomain::Geographic(frame),
        Some(&no_metric),
    );

    for depth in [40, 128] {
        let mut shared = base.clone();
        for _ in 0..depth {
            shared = Shape::geometry_collection(vec![shared.clone(), shared])
                .expect("shared collection depth is admitted");
        }
        let parts = shared
            .subdivide_with_context(8, false, &context)
            .expect("deep geographic subdivision succeeds");
        assert!(!parts.is_empty(), "depth {depth}");
        let parts_union = Shape::union_all_topo(
            &parts,
            crate::geometry::TopologyDomain::Geographic(frame),
            crate::geometry::Strictness::Lenient,
        )
        .expect("deep geographic subdivision parts union");
        assert_eq!(parts_union.area(), 1.0, "depth {depth}");

        let source_union = Shape::union_all_topo(
            &[&shared],
            crate::geometry::TopologyDomain::Geographic(frame),
            crate::geometry::Strictness::Lenient,
        )
        .expect("deep geographic source union");
        assert_eq!(source_union.area(), 1.0, "depth {depth}");
    }
}

#[test]
fn over_budget_clipped_multipart_children_are_revisited_by_member() {
    let shape =
        crate::io::parse_wkt("POLYGON((0 0,20 0,20 20,0 20,0 0),(0 0,5 2,2 5,0 0))").unwrap();
    let parts = shape
        .subdivide_with_context(8, false, &crate::geometry::SourceArcContext::cartesian())
        .unwrap();
    let mut counts = ShapeCountQuery::default();
    assert!(parts.iter().all(|part| {
        counts
            .coordinate_count(part)
            .is_some_and(|count| count <= 8)
    }));
}

#[test]
fn over_budget_multipolygon_is_separated_before_spatial_partitioning() {
    let shape = crate::io::parse_wkt(
        "MULTIPOLYGON(((0 0,10 0,10 10,0 10,0 0)),((10 10,20 10,20 20,10 20,10 10)))",
    )
    .unwrap();
    reset_partition_calls();
    let parts = shape
        .subdivide_with_context(8, false, &crate::geometry::SourceArcContext::cartesian())
        .unwrap();
    assert_eq!(parts.len(), 2);
    assert!(parts.iter().all(|part| matches!(part, Shape::Polygon(_))));
    assert_eq!(partition_calls(), 0);
}

#[test]
fn mixed_dimension_subdivision_keeps_each_selected_dimension() {
    let polygon = ring_shape(&[
        Point::new_raw_xy(0.0, 0.0),
        Point::new_raw_xy(2.0, 0.0),
        Point::new_raw_xy(2.0, 2.0),
        Point::new_raw_xy(0.0, 2.0),
        Point::new_raw_xy(0.0, 0.0),
    ]);
    let line = line_shape(&[Point::new_raw_xy(-1.0, 1.0), Point::new_raw_xy(3.0, 1.0)]);
    let shape = Shape::geometry_collection(vec![
        Shape::Point(Point::new_raw_xy(1.0, 1.0)),
        line,
        polygon,
    ])
    .unwrap();
    let parts = shape
        .subdivide_with_context(7, false, &crate::geometry::SourceArcContext::cartesian())
        .unwrap();
    assert!(parts.len() >= 2);
    assert!(parts.iter().all(|part| !part.is_empty()));
    assert!(parts.iter().any(|part| {
        matches!(part, Shape::GeometryCollection(_))
            || matches!(part, Shape::Polygon(_))
            || matches!(part, Shape::LineString(_))
    }));
}

#[test]
fn exact_seam_rounding_and_zm_resolution_are_shared() {
    let coordinates = CoordSeq::try_from_vecs(
        vec![0.0, 1.0, 2.0, 2.0, 2.0, 1.0, 0.0, 0.0, 0.0],
        vec![0.0, 0.0, 0.0, 1.0, 2.0, 2.0, 2.0, 1.0, 0.0],
        Some(vec![0.0, 10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 0.0]),
        Some(vec![
            100.0, 110.0, 120.0, 130.0, 140.0, 150.0, 160.0, 170.0, 100.0,
        ]),
    )
    .unwrap();
    let shape = Shape::Polygon(Polygon::new(
        Ring::from_closed(coordinates).unwrap(),
        Vec::new(),
    ));
    let parts = shape
        .subdivide_with_context(8, false, &crate::geometry::SourceArcContext::cartesian())
        .unwrap();
    assert_eq!(parts.len(), 2);
    let source = all_points(&shape);
    let mut new_shared = 0;
    for left in all_points(&parts[0]) {
        for right in all_points(&parts[1]) {
            if left.x.to_bits() != right.x.to_bits() || left.y.to_bits() != right.y.to_bits() {
                continue;
            }
            assert_eq!(left.z().map(f64::to_bits), right.z().map(f64::to_bits));
            assert_eq!(left.m().map(f64::to_bits), right.m().map(f64::to_bits));
            if !source.iter().any(|point| {
                point.x.to_bits() == left.x.to_bits() && point.y.to_bits() == left.y.to_bits()
            }) {
                new_shared += 1;
            }
        }
    }
    assert!(new_shared > 0);
}

#[test]
fn tangent_holes_and_collapsed_support_follow_exact_selection() {
    for wkt in [
        "POLYGON ((0 0,1 0,2 0,3 0,4 0,4 4,0 4,0 0),(2 1,3 1,3 2,2 2,2 1))",
        "POLYGON ((0 0,4 0,2 0,0 0),(1 1,3 1,3 3,1 3,1 1))",
    ] {
        let shape = crate::io::parse_wkt(wkt).unwrap();
        let parts = shape
            .subdivide_with_context(8, false, &crate::geometry::SourceArcContext::cartesian())
            .unwrap();
        assert!(!parts.is_empty(), "{wkt}");
        assert!(parts.iter().all(|part| !part.is_empty()), "{wkt}");
    }
}

#[test]
fn subdivision_uses_distinct_ordinates_for_mixed_shared_multiplicity() {
    let rectangle = |min_x, max_x| {
        ring_shape(&[
            Point::new_raw_xy(min_x, 0.0),
            Point::new_raw_xy(max_x, 0.0),
            Point::new_raw_xy(max_x, 1.0),
            Point::new_raw_xy(min_x, 1.0),
            Point::new_raw_xy(min_x, 0.0),
        ])
    };
    let first = Shape::geometry_collection(vec![rectangle(0.0, 2.0)]).unwrap();
    let nested = Shape::geometry_collection(vec![first.clone(), rectangle(10.0, 12.0)]).unwrap();
    let shared = Shape::geometry_collection(vec![first, nested]).unwrap();
    let expanded = crate::io::parse_wkb(
        &crate::io::to_wkb(&shared, None, false).expect("shared input serializes"),
    )
    .expect("unshared expansion parses")
    .shape;

    let shared_parts = shared
        .subdivide_with_context(8, false, &crate::geometry::SourceArcContext::cartesian())
        .unwrap();
    let expanded_parts = expanded
        .subdivide_with_context(8, false, &crate::geometry::SourceArcContext::cartesian())
        .unwrap();
    let wkb = |parts: &[Shape]| {
        parts
            .iter()
            .map(|part| crate::io::to_wkb(part, None, false).unwrap())
            .collect::<Vec<_>>()
    };
    let shared_parts_wkb = wkb(&shared_parts);
    assert_eq!(shared_parts_wkb, wkb(&expanded_parts));
    assert_eq!(
        shared_parts_wkb,
        wkb(&shared
            .subdivide_with_context(8, false, &crate::geometry::SourceArcContext::cartesian())
            .unwrap())
    );

    let mut counts = ShapeCountQuery::default();
    assert!(shared_parts.iter().all(|part| {
        counts
            .coordinate_count(part)
            .is_some_and(|count| count <= 8)
    }));
    let source_union = Shape::union_all_topo(
        &[&shared],
        crate::geometry::TopologyDomain::Cartesian,
        crate::geometry::Strictness::Lenient,
    )
    .unwrap();
    let parts_union = Shape::union_all_topo(
        &shared_parts,
        crate::geometry::TopologyDomain::Cartesian,
        crate::geometry::Strictness::Lenient,
    )
    .unwrap();
    assert_eq!(
        crate::io::to_wkb(&source_union, None, false).unwrap(),
        crate::io::to_wkb(&parts_union, None, false).unwrap()
    );
}
