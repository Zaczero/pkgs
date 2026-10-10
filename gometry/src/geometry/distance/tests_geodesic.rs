#![expect(
    clippy::panic_in_result_fn,
    reason = "tests assert, and return Result only to propagate setup errors"
)]
use geographiclib_rs::{Geodesic, InverseGeodesic as _};
use pyo3::prelude::*;

use crate::crs::frame::{Frame, crs_arc_static};
use crate::crs::{PointEdgeScratch, with_ellipsoid_metric};
use crate::error::Result;
use crate::geographic::{AngularFactor, GeographicFrame};
use crate::geometry::distance::frechet::frechet_dp_columns;
use crate::geometry::distance::tests::{line, p, polygon};
use crate::geometry::distance::{
    ChartHausdorffWitness, ChartParts, chart_directed_hausdorff_distance, chart_hausdorff_counters,
    chart_scratch,
};
use crate::geometry::{
    ChartMetric, FrameDependentCaches, GeodesicMetric, GeographicPrepared, MOrdinate, Point,
    PreparedPlan, Shape, ZOrdinate, graph_relate_data_geographic,
};

const MINIMUM_GAP_METERS: f64 = 4e-11 * 6_378_137.0;

fn disconnected_bvh_target() -> Shape {
    Shape::geometry_collection(
        (0..65)
            .map(|index| {
                let longitude = f64::from(index) * 2.0;
                line(vec![p(longitude, 0.0), p(longitude, 1.0)])
            })
            .collect(),
    )
    .expect("disconnected BVH target is valid")
}

fn directed_chart_hausdorff(
    source: &Shape,
    target: &Shape,
    metric: &(impl GeodesicMetric + ChartMetric<PointEdgeScratch = PointEdgeScratch>),
) -> ChartHausdorffWitness {
    let source_parts = ChartParts::build(source, metric).expect("source chart parts");
    let target_parts = ChartParts::build(target, metric).expect("target chart parts");
    let mut scratch = chart_scratch();
    chart_directed_hausdorff_distance(&source_parts, &target_parts, metric, &mut scratch)
        .expect("directed chart Hausdorff")
}

fn parallel_point_distance(geodesic: &Geodesic, colatitude: f64, parameter: f64) -> f64 {
    geodesic.inverse(90.0 - colatitude, 10.0, 85.0, 179.0 * parameter)
}

fn parallel_point_maximum_oracle(geodesic: &Geodesic, colatitude: f64) -> f64 {
    const LAST_SAMPLE: u32 = 4_000;
    let distances = (0..=LAST_SAMPLE)
        .map(|index| {
            parallel_point_distance(
                geodesic,
                colatitude,
                f64::from(index) / f64::from(LAST_SAMPLE),
            )
        })
        .collect::<Vec<_>>();
    let (maximum_index, mut maximum) = distances
        .iter()
        .copied()
        .enumerate()
        .max_by(|left, right| left.1.total_cmp(&right.1))
        .expect("the oracle includes the parallel endpoints");
    let step = 1.0 / f64::from(LAST_SAMPLE);
    let mut lower = (f64::from(maximum_index as u32) * step - step).max(0.0);
    let mut upper = (f64::from(maximum_index as u32) * step + step).min(1.0);
    let golden = (5.0_f64.sqrt() - 1.0) / 2.0;
    let mut left = upper - golden * (upper - lower);
    let mut right = lower + golden * (upper - lower);
    let mut left_value = parallel_point_distance(geodesic, colatitude, left);
    let mut right_value = parallel_point_distance(geodesic, colatitude, right);
    for _ in 0..80 {
        if left_value >= right_value {
            upper = right;
            right = left;
            right_value = left_value;
            left = upper - golden * (upper - lower);
            left_value = parallel_point_distance(geodesic, colatitude, left);
        } else {
            lower = left;
            left = right;
            left_value = right_value;
            right = lower + golden * (upper - lower);
            right_value = parallel_point_distance(geodesic, colatitude, right);
        }
    }
    maximum = maximum.max(left_value).max(right_value);
    maximum.max(
        parallel_point_distance(geodesic, colatitude, 0.0)
            .max(parallel_point_distance(geodesic, colatitude, 1.0)),
    )
}

fn chart_edge_point_distance(
    geodesic: &Geodesic,
    start: Point,
    end: Point,
    target: Point,
    parameter: f64,
) -> f64 {
    let mut longitude_delta = end.x - start.x;
    if longitude_delta > 180.0 {
        longitude_delta -= 360.0;
    } else if longitude_delta <= -180.0 {
        longitude_delta += 360.0;
    }
    let longitude = start.x + longitude_delta * parameter;
    let latitude = start.y + (end.y - start.y) * parameter;
    geodesic.inverse(target.y, target.x, latitude, longitude)
}

fn chart_edge_point_maximum_oracle(start: Point, end: Point, target: Point) -> f64 {
    const LAST_SAMPLE: u32 = 4_000;
    let geodesic = Geodesic::wgs84();
    let distance = |parameter| chart_edge_point_distance(&geodesic, start, end, target, parameter);
    let samples = (0..=LAST_SAMPLE)
        .map(|index| distance(f64::from(index) / f64::from(LAST_SAMPLE)))
        .collect::<Vec<_>>();
    let (index, mut maximum) = samples
        .iter()
        .copied()
        .enumerate()
        .max_by(|left, right| left.1.total_cmp(&right.1))
        .expect("the oracle includes both chart-edge endpoints");
    let step = 1.0 / f64::from(LAST_SAMPLE);
    let mut lower = (index as f64 * step - step).max(0.0);
    let mut upper = (index as f64 * step + step).min(1.0);
    let golden = (5.0_f64.sqrt() - 1.0) / 2.0;
    let mut left = upper - golden * (upper - lower);
    let mut right = lower + golden * (upper - lower);
    let mut left_value = distance(left);
    let mut right_value = distance(right);
    for _ in 0..80 {
        if left_value >= right_value {
            upper = right;
            right = left;
            right_value = left_value;
            left = upper - golden * (upper - lower);
            left_value = distance(left);
        } else {
            lower = left;
            left = right;
            left_value = right_value;
            right = lower + golden * (upper - lower);
            right_value = distance(right);
        }
    }
    maximum = maximum.max(left_value).max(right_value);
    maximum.max(distance(0.0).max(distance(1.0)))
}

fn assert_chart_edge_point_hausdorff(
    name: &str,
    start: Point,
    end: Point,
    target_point: Point,
) -> Result<()> {
    let source = line(vec![start, end]);
    let target = Shape::Point(target_point);
    let expected = chart_edge_point_maximum_oracle(start, end, target_point);
    with_ellipsoid_metric("EPSG:4326", &[&source, &target], |metric| {
        let witness = directed_chart_hausdorff(&source, &target, metric);
        let attained = metric.segment_length(witness.source, witness.target);
        let scale = expected.max(witness.distance).max(1.0);
        let rounding = 32.0 * (scale.next_up() - scale);
        assert!(
            witness.distance <= expected + rounding,
            "{name}: reported {} above the independent maximum {expected}",
            witness.distance,
        );
        assert!(
            expected - witness.distance <= 0.01 + rounding,
            "{name}: reported {} below the independent maximum {expected}",
            witness.distance,
        );
        assert!(
            (attained - witness.distance).abs() <= rounding,
            "{name}: witness distance {attained} does not attain {}",
            witness.distance,
        );
        Ok(())
    })?;
    Ok(())
}

#[test]
fn geographic_hausdorff_parallel_pole_plateaus_close_with_second_order_bounds() -> Result<()> {
    let source = line(vec![p(0.0, 85.0), p(179.0, 85.0)]);
    let geodesic = Geodesic::wgs84();
    let cases = [
        ("colatitude_0_01_degree", 0.01),
        ("colatitude_0_1_degree", 0.1),
        ("colatitude_1_degree", 1.0),
        ("colatitude_3_degrees", 3.0),
    ];
    with_ellipsoid_metric("EPSG:4326", &[&source], |metric| {
        for (name, colatitude) in cases {
            let target = Shape::Point(p(10.0, 90.0 - colatitude));
            chart_hausdorff_counters::reset();
            let witness = directed_chart_hausdorff(&source, &target, metric);
            let evaluations = chart_hausdorff_counters::source_evaluations();
            let expected = parallel_point_maximum_oracle(&geodesic, colatitude);
            let attained = metric.segment_length(witness.source, witness.target);
            let scale = expected.max(witness.distance).max(1.0);
            let rounding = 32.0 * (scale.next_up() - scale);

            assert!(
                witness.distance <= expected + rounding,
                "{name}: reported {} above the independent maximum {expected}",
                witness.distance,
            );
            assert!(
                expected - witness.distance <= 0.01 + rounding,
                "{name}: reported {} below the independent maximum {expected}",
                witness.distance,
            );
            assert!(
                (attained - witness.distance).abs() <= rounding,
                "{name}: source/target witness distance {attained} does not attain {}",
                witness.distance,
            );
            assert!(
                evaluations <= 100,
                "{name}: source frontier used {evaluations} evaluations",
            );
        }
        Ok(())
    })?;
    Ok(())
}

#[test]
fn geographic_hausdorff_endpoint_maximum_is_attained() -> Result<()> {
    assert_chart_edge_point_hausdorff(
        "maximum at the source endpoint",
        p(0.0, 0.0),
        p(1.0, 0.0),
        p(10.0, 5.0),
    )
}

#[test]
fn geographic_hausdorff_short_way_seam_edge_uses_chart_interpolation() -> Result<()> {
    assert_chart_edge_point_hausdorff(
        "short-way seam crossing",
        p(179.5, 45.0),
        p(-179.5, 45.0),
        p(170.0, 43.0),
    )
}

#[test]
fn geographic_hausdorff_near_antipodal_point_target_is_attained() -> Result<()> {
    assert_chart_edge_point_hausdorff(
        "near-antipodal target",
        p(-1.0, 0.0),
        p(1.0, 0.0),
        p(178.0, 5.0),
    )
}

#[test]
fn geographic_hausdorff_closes_a_nearest_point_feature_switch() -> Result<()> {
    let source = line(vec![p(0.0, 0.0), p(10.0, 0.0)]);
    let target =
        Shape::geometry_collection(vec![Shape::Point(p(-1.0, 0.0)), Shape::Point(p(11.0, 0.0))])
            .expect("two-point target is valid");
    with_ellipsoid_metric("EPSG:4326", &[&source, &target], |metric| {
        let witness = directed_chart_hausdorff(&source, &target, metric);
        let expected = metric.segment_length(p(5.0, 0.0), p(-1.0, 0.0));
        assert!((witness.distance - expected).abs() <= 0.01);
        assert!((witness.source.x - 5.0).abs() <= 1.0e-8);
        Ok(())
    })?;
    Ok(())
}

#[test]
fn geographic_hausdorff_keeps_a_farther_reverse_point_minimum() -> Result<()> {
    let source = line(vec![p(0.0, 0.0), p(10.0, 0.0)]);
    let target = Shape::geometry_collection(vec![
        Shape::Point(p(-1.0, 0.0)),
        Shape::Point(p(170.0, 0.0)),
    ])
    .expect("two-point target is valid");
    with_ellipsoid_metric("EPSG:4326", &[&source, &target], |metric| {
        // The 160-degree equatorial arc to the eastern source endpoint
        // dominates every source-to-target minimum.
        let distance = source.chart_hausdorff_distance(&target, metric)?;
        assert!(
            (distance - 17_811_118.526_923_772).abs() <= 0.01,
            "reverse point maximum was {distance}; expected the 160-degree equatorial arc"
        );
        Ok(())
    })?;
    Ok(())
}

#[test]
fn geographic_hausdorff_closes_an_exact_polar_parallel_family() -> Result<()> {
    let source = line(vec![p(-45.0, 85.0), p(45.0, 85.0)]);
    let pole = Shape::Point(p(10.0, 90.0));
    with_ellipsoid_metric("EPSG:4326", &[&source, &pole], |metric| {
        chart_hausdorff_counters::reset();
        let witness = directed_chart_hausdorff(&source, &pole, metric);
        let evaluations = chart_hausdorff_counters::source_evaluations();
        let expected = metric.segment_length(p(0.0, 85.0), p(10.0, 90.0));
        assert!((witness.distance - expected).abs() <= 0.01);
        assert!(
            evaluations <= 3,
            "polar family used {evaluations} evaluations"
        );
        Ok(())
    })?;
    Ok(())
}

#[test]
fn geographic_hausdorff_plateau_uses_native_grad_and_radian_frames() {
    let geodesic = Geodesic::wgs84();
    let expected = 569_423.751_409_724;
    for (name, unit_name, radians_per_unit) in [
        ("grad", "grad", std::f64::consts::PI / 200.0),
        ("radian", "radian", 1.0),
    ] {
        let unit = AngularFactor::try_new(unit_name, radians_per_unit)
            .expect("angular frame unit is valid");
        let frame = GeographicFrame::try_new(unit, unit, 0.0).expect("native frame is valid");
        let to_native = (std::f64::consts::PI / 180.0) / radians_per_unit;
        let source = line(vec![
            p(0.0, 85.0 * to_native),
            p(179.0 * to_native, 85.0 * to_native),
        ]);
        let target = Shape::Point(p(10.0 * to_native, 89.9 * to_native));
        let metric = crate::crs::EllipsoidMetric::for_geodesic_at_frame(&geodesic, 0, frame);

        chart_hausdorff_counters::reset();
        let witness = directed_chart_hausdorff(&source, &target, &metric);
        let evaluations = chart_hausdorff_counters::source_evaluations();
        assert!(
            (witness.distance - expected).abs() <= 0.01,
            "{name}: chart-edge plateau was {}; expected {expected}",
            witness.distance,
        );
        assert!(
            (metric.segment_length(witness.source, witness.target) - witness.distance).abs()
                <= 1.0e-7,
            "{name}: chart-edge witness does not attain the reported distance",
        );
        assert!(
            evaluations <= 100,
            "{name}: {evaluations} source evaluations"
        );
    }
}

#[test]
fn geographic_hausdorff_returns_exact_zero_for_reversed_equal_chart_line() -> Result<()> {
    let original = line(vec![p(-10.0, 30.0), p(15.0, 40.0)]);
    let reversed = line(vec![p(15.0, 40.0), p(-10.0, 30.0)]);
    with_ellipsoid_metric("EPSG:4326", &[&original, &reversed], |metric| {
        assert_eq!(original.chart_hausdorff_distance(&reversed, metric)?, 0.0);
        Ok(())
    })?;
    Ok(())
}

#[test]
fn geographic_hausdorff_pole_equality_folds_exterior_latitude_before_bounds() -> Result<()> {
    let source = line(vec![p(0.0, 90.0), p(180.0, 90.0)]);
    let target = Shape::Point(p(0.0, 90.000_000_000_000_01));
    with_ellipsoid_metric("EPSG:4326", &[&source, &target], |metric| {
        assert_eq!(
            source.chart_hausdorff_distance(&target, metric)?.to_bits(),
            0.0_f64.to_bits(),
        );
        Ok(())
    })?;
    Ok(())
}

#[test]
fn point_to_line_distance_and_nearest_witness_use_the_same_chart_edge() -> Result<()> {
    let point_shape = Shape::Point(p(0.0, 82.9));
    let line_shape = line(vec![p(-45.0, 80.0), p(45.0, 80.0)]);
    let point = PreparedPlan::new(point_shape.clone());
    let line_plan = PreparedPlan::new(line_shape.clone());
    let point_cache = FrameDependentCaches::default();
    let line_cache = FrameDependentCaches::default();
    with_ellipsoid_metric("EPSG:4326", &[&point_shape, &line_shape], |metric| {
        let (semi_major, flattening) = metric.ellipsoid_parameters();
        let distance = point.geodesic_distance_cached(
            &point_cache,
            &line_plan,
            &line_cache,
            "EPSG:4326",
            semi_major,
            flattening,
            metric,
        )?;
        let known_distance = 323_839.508_746_922_83;
        let scale = distance.abs().max(known_distance).max(1.0);
        let rounding_floor = 32.0 * (scale.next_up() - scale);
        assert!(
            (distance - known_distance).abs() <= MINIMUM_GAP_METERS + rounding_floor,
            "point-to-chart-edge distance was {distance}"
        );

        let nearest = point
            .geodesic_nearest_points_cached(
                &point_cache,
                &line_plan,
                &line_cache,
                "EPSG:4326",
                semi_major,
                flattening,
                metric,
            )?
            .expect("nonempty point and line have nearest witnesses");
        let attained = metric.segment_length(nearest.0, nearest.1);
        assert_eq!(distance, attained);
        assert!((-45.0..=45.0).contains(&nearest.1.x));
        assert_eq!(nearest.1.y, 80.0);
        let reference = metric.segment_length(p(0.0, 82.9), p(0.0, 80.0));
        assert!(
            attained <= reference + MINIMUM_GAP_METERS,
            "attained {attained} exceeds the reference {reference} by more than \
             the Q8 gap {MINIMUM_GAP_METERS}"
        );

        assert!(!point.geodesic_dwithin_cached(
            &point_cache,
            &line_plan,
            &line_cache,
            "EPSG:4326",
            semi_major,
            flattening,
            metric,
            distance - 1.0,
        )?);
        assert!(point.geodesic_dwithin_cached(
            &point_cache,
            &line_plan,
            &line_cache,
            "EPSG:4326",
            semi_major,
            flattening,
            metric,
            distance + 1.0,
        )?);

        let touching_shape = Shape::Point(p(0.0, 80.0));
        let touching = PreparedPlan::new(touching_shape);
        let touching_cache = FrameDependentCaches::default();
        assert_eq!(
            touching.geodesic_distance_cached(
                &touching_cache,
                &line_plan,
                &line_cache,
                "EPSG:4326",
                semi_major,
                flattening,
                metric,
            )?,
            0.0
        );
        assert!(touching.geodesic_dwithin_cached(
            &touching_cache,
            &line_plan,
            &line_cache,
            "EPSG:4326",
            semi_major,
            flattening,
            metric,
            0.0,
        )?);
        Ok(())
    })?;
    Ok(())
}

#[test]
fn public_distance_measures_a_full_turn_line_as_its_parallel() {
    crate::test_support::initialize_python();
    Python::attach(|py| {
        let frame = Frame::new(Some(crs_arc_static("EPSG:4326")), None)
            .expect("CRS84 is a valid geographic frame");
        let point = crate::Typed(crate::PyGeometry::with_frame(
            Shape::Point(p(200.0, 50.0)),
            frame.clone(),
        ))
        .into_pyobject(py)
        .expect("point binds as its public typed geometry");
        let line = crate::Typed(crate::PyGeometry::with_frame(
            line(vec![p(10.0, 60.0), p(370.0, 60.0)]),
            frame,
        ))
        .into_pyobject(py)
        .expect("linestring binds as its public typed geometry");

        let distance = crate::distance(py, &point, &line, crate::NavigationPath::Geodesic, None)
            .expect("public distance succeeds")
            .bind(py)
            .extract::<f64>()
            .expect("distance is a scalar");
        let expected: f64 = Geodesic::wgs84().inverse(50.0, 200.0, 60.0, 200.0);
        let rounding_floor = 32.0 * (expected.next_up() - expected);
        assert!(
            (distance - expected).abs() <= MINIMUM_GAP_METERS + rounding_floor,
            "full-turn distance {distance} differs from the 60-degree parallel \
             meridian distance {expected}"
        );
    });
}

#[test]
fn point_to_chart_edge_nearest_witness_interpolates_ordinates_by_arclength() -> Result<()> {
    let point_shape = Shape::Point(p(0.0, 82.9));
    let line_shape = line(vec![
        Point::new_axes(-45.0, 80.0, ZOrdinate(Some(10.0)), MOrdinate(Some(100.0)))
            .expect("finite start ordinates"),
        Point::new_axes(45.0, 80.0, ZOrdinate(Some(30.0)), MOrdinate(Some(300.0)))
            .expect("finite end ordinates"),
    ]);
    with_ellipsoid_metric("EPSG:4326", &[&point_shape, &line_shape], |metric| {
        let point = PreparedPlan::new(point_shape.clone());
        let line_plan = PreparedPlan::new(line_shape.clone());
        let (semi_major, flattening) = metric.ellipsoid_parameters();
        let nearest = point
            .geodesic_nearest_points_cached(
                &FrameDependentCaches::default(),
                &line_plan,
                &FrameDependentCaches::default(),
                "EPSG:4326",
                semi_major,
                flattening,
                metric,
            )?
            .expect("nonempty point and line have nearest witnesses");
        assert!((-45.0..=45.0).contains(&nearest.1.x));
        assert_eq!(nearest.1.y, 80.0);
        let reference = metric.segment_length(p(0.0, 82.9), p(0.0, 80.0));
        let attained = metric.segment_length(nearest.0, nearest.1);
        assert!(
            attained <= reference + MINIMUM_GAP_METERS,
            "attained {attained} exceeds the reference {reference} by more than \
             the Q8 gap {MINIMUM_GAP_METERS}"
        );
        let parameter = (nearest.1.x + 45.0) / 90.0;
        let expected_z = 10.0 + 20.0 * parameter;
        let expected_m = 100.0 + 200.0 * parameter;
        assert!((nearest.1.z().expect("target carries Z") - expected_z).abs() <= 1.0e-12);
        assert!((nearest.1.m().expect("target carries M") - expected_m).abs() <= 1.0e-10);

        let touching = PreparedPlan::new(Shape::Point(p(0.0, 80.0)));
        let touching_witness = touching
            .geodesic_nearest_points_cached(
                &FrameDependentCaches::default(),
                &line_plan,
                &FrameDependentCaches::default(),
                "EPSG:4326",
                semi_major,
                flattening,
                metric,
            )?
            .expect("the point lies on the chart edge");
        assert_eq!((touching_witness.0.x, touching_witness.0.y), (0.0, 80.0));
        assert_eq!((touching_witness.1.x, touching_witness.1.y), (0.0, 80.0));
        assert!((touching_witness.1.z().expect("target carries Z") - 20.0).abs() <= 1.0e-12);
        assert!((touching_witness.1.m().expect("target carries M") - 200.0).abs() <= 1.0e-10);
        Ok(())
    })?;
    Ok(())
}

#[test]
fn cached_point_pair_nearest_validates_both_geographic_operands() -> Result<()> {
    let valid_shape = Shape::Point(p(0.0, 0.0));
    let invalid_shape = Shape::Point(p(1.0, 95.0));
    let valid = PreparedPlan::new(valid_shape);
    let invalid = PreparedPlan::new(invalid_shape);
    with_ellipsoid_metric("EPSG:4326", &[], |metric| {
        let (semi_major, flattening) = metric.ellipsoid_parameters();
        for (left, right) in [(&valid, &invalid), (&invalid, &valid)] {
            let error = left
                .geodesic_nearest_points_cached(
                    &FrameDependentCaches::default(),
                    right,
                    &FrameDependentCaches::default(),
                    "EPSG:4326",
                    semi_major,
                    flattening,
                    metric,
                )
                .expect_err("both point operands require geographic-domain admission");
            assert!(error.to_string().contains("invalid longitude/latitude"));
        }
        Ok(())
    })?;
    Ok(())
}

#[test]
fn scalar_geodesic_dwithin_at_infinity_does_not_allocate() -> Result<()> {
    let left_shape = line(vec![p(-2.0, 0.0), p(2.0, 0.0)]);
    let right_shape = line(vec![p(-2.0, 1.0), p(2.0, 1.0)]);
    with_ellipsoid_metric("EPSG:4326", &[&left_shape, &right_shape], |metric| {
        let left = PreparedPlan::new(left_shape.clone());
        let right = PreparedPlan::new(right_shape.clone());
        let (semi_major, flattening) = metric.ellipsoid_parameters();
        let infinite = || {
            let left_cache = FrameDependentCaches::default();
            let right_cache = FrameDependentCaches::default();
            left.geodesic_dwithin_cached(
                &left_cache,
                &right,
                &right_cache,
                "EPSG:4326",
                semi_major,
                flattening,
                metric,
                f64::INFINITY,
            )
        };
        assert!(crate::test_support::assert_no_allocations(infinite)?);
        Ok(())
    })?;
    Ok(())
}

#[test]
fn fixed_geodesic_point_rows_at_infinity_still_validate_latitudes() -> Result<()> {
    let cases = [
        line(vec![p(-1.0, 0.0), p(1.0, 0.0)]),
        disconnected_bvh_target(),
    ];
    for target in cases {
        with_ellipsoid_metric("EPSG:4326", &[&target], |metric| {
            let data = PreparedPlan::new(target.clone());
            let frame_cache = FrameDependentCaches::default();
            let (semi_major, flattening) = metric.ellipsoid_parameters();
            let xs = [0.0, 10.0];
            let ys = [0.0, 0.0];
            let inf_run = || {
                data.geodesic_dwithin_points(
                    &frame_cache,
                    std::iter::zip(xs.iter().copied(), ys.iter().copied()),
                    "EPSG:4326",
                    semi_major,
                    flattening,
                    metric,
                    f64::INFINITY,
                    metric.frame(),
                )
            };
            assert_eq!(inf_run()?, [true, true]);
            let invalid_ys = [0.0, 95.0];
            let error = data
                .geodesic_dwithin_points(
                    &frame_cache,
                    std::iter::zip(xs[..2].iter().copied(), invalid_ys.iter().copied()),
                    "EPSG:4326",
                    semi_major,
                    flattening,
                    metric,
                    f64::INFINITY,
                    metric.frame(),
                )
                .expect_err("invalid point-row latitude must be rejected");
            assert!(error.to_string().contains("invalid longitude/latitude"));
            Ok(())
        })?;
    }
    Ok(())
}

fn assert_chart_pair_contract<M>(
    left_shape: &Shape,
    right_shape: &Shape,
    expected_intersects: bool,
    metric: &M,
    semi_major: f64,
    flattening: f64,
) -> Result<()>
where
    M: GeodesicMetric + ChartMetric<PointEdgeScratch = PointEdgeScratch>,
{
    let left = PreparedPlan::new(left_shape.clone());
    let right = PreparedPlan::new(right_shape.clone());
    let left_cache = FrameDependentCaches::default();
    let right_cache = FrameDependentCaches::default();
    let frame = metric.chart_frame();
    let left_topology = GeographicPrepared::try_new(&left, &left_cache, frame)?;
    let right_topology = GeographicPrepared::try_new(&right, &right_cache, frame)?;
    let intersects = graph_relate_data_geographic(left_topology, right_topology)?.is_intersects();
    assert_eq!(intersects, expected_intersects);

    let distance = left.geodesic_distance_cached(
        &left_cache,
        &right,
        &right_cache,
        "EPSG:4326",
        semi_major,
        flattening,
        metric,
    )?;
    // Exact topology proves zero for intersections; a near-zero disjoint
    // metric result does not decide exact topology.
    if intersects {
        assert_eq!(distance, 0.0);
    }
    assert_eq!(
        left.geodesic_dwithin_cached(
            &left_cache,
            &right,
            &right_cache,
            "EPSG:4326",
            semi_major,
            flattening,
            metric,
            0.0,
        )?,
        intersects
    );
    let nearest = left
        .geodesic_nearest_points_cached(
            &left_cache,
            &right,
            &right_cache,
            "EPSG:4326",
            semi_major,
            flattening,
            metric,
        )?
        .expect("the fixture operands are non-empty");
    let attained = metric.segment_length(nearest.0, nearest.1);
    let scale = distance.abs().max(attained.abs()).max(1.0);
    let rounding_floor = 32.0 * (scale.next_up() - scale);
    assert!(
        (distance - attained).abs() <= rounding_floor,
        "distance {distance} was not attained by nearest witnesses ({attained})"
    );
    if distance > 0.02 {
        assert!(!left.geodesic_dwithin_cached(
            &left_cache,
            &right,
            &right_cache,
            "EPSG:4326",
            semi_major,
            flattening,
            metric,
            distance - 0.01,
        )?);
        assert!(left.geodesic_dwithin_cached(
            &left_cache,
            &right,
            &right_cache,
            "EPSG:4326",
            semi_major,
            flattening,
            metric,
            distance + 0.01,
        )?);
    }
    Ok(())
}

#[test]
fn chart_distance_property_six_covers_crossings_and_filled_polygons() -> Result<()> {
    let crossing = line(vec![p(-20.0, -10.0), p(30.0, 20.0)]);
    let crossing_other = line(vec![p(-15.0, 20.0), p(25.0, -10.0)]);
    let area = polygon(vec![
        p(-1.0, -1.0),
        p(1.0, -1.0),
        p(1.0, 1.0),
        p(-1.0, 1.0),
        p(-1.0, -1.0),
    ]);
    let inner = Shape::Point(p(0.0, 0.0));
    let outside = Shape::Point(p(2.0, 0.0));
    let overlap = polygon(vec![
        p(0.5, -0.5),
        p(1.5, -0.5),
        p(1.5, 0.5),
        p(0.5, 0.5),
        p(0.5, -0.5),
    ]);
    let separated = polygon(vec![
        p(2.0, -0.5),
        p(3.0, -0.5),
        p(3.0, 0.5),
        p(2.0, 0.5),
        p(2.0, -0.5),
    ]);

    with_ellipsoid_metric("EPSG:4326", &[], |metric| {
        let (semi_major, flattening) = metric.ellipsoid_parameters();
        assert_chart_pair_contract(
            &crossing,
            &crossing_other,
            true,
            metric,
            semi_major,
            flattening,
        )?;
        assert_chart_pair_contract(&inner, &area, true, metric, semi_major, flattening)?;
        assert_chart_pair_contract(&outside, &area, false, metric, semi_major, flattening)?;
        assert_chart_pair_contract(&area, &overlap, true, metric, semi_major, flattening)?;
        assert_chart_pair_contract(&area, &separated, false, metric, semi_major, flattening)?;
        Ok(())
    })?;
    Ok(())
}

#[test]
fn chart_edge_contacts_at_high_latitudes_are_zero_distance() -> Result<()> {
    let cases = [
        (
            "tiny northern contact",
            [p(-0.0001, 89.5), p(0.0001, 89.5)],
            [p(0.0, 79.5), p(0.0, 89.5)],
        ),
        (
            "near north pole contact",
            [p(-0.001, 89.94), p(0.001, 89.94)],
            [p(0.0, 79.94), p(0.0, 89.94)],
        ),
        (
            "near south pole contact",
            [p(-0.001, -89.94), p(0.001, -89.94)],
            [p(0.0, -79.94), p(0.0, -89.94)],
        ),
    ];
    with_ellipsoid_metric("EPSG:4326", &[], |metric| {
        let (semi_major, flattening) = metric.ellipsoid_parameters();
        for (name, parallel_points, meridian_points) in cases {
            let parallel = line(parallel_points.to_vec());
            let meridian = line(meridian_points.to_vec());
            assert_chart_pair_contract(&parallel, &meridian, true, metric, semi_major, flattening)
                .unwrap_or_else(|error| panic!("{name}: {error}"));
        }
        Ok(())
    })?;
    Ok(())
}

#[test]
fn public_dwithin_keeps_an_off_grid_interior_chart_witness() -> Result<()> {
    crate::test_support::initialize_python();
    let geodesic = Geodesic::wgs84();
    let cases = [
        (
            "long oblique",
            p(-70.0, 10.0),
            p(80.0, 55.0),
            0.314_159_265_358_979_3,
            0.0,
            0.0001,
        ),
        (
            "meridian between derivative nodes",
            p(0.0, -72.384_836_123_684_47),
            p(0.0, 72.384_836_123_684_47),
            0.314_159_265_358_979_3,
            0.001,
            0.0,
        ),
        (
            "near-pole parallel",
            p(-45.0, 80.0),
            p(45.0, 80.0),
            0.314_159_265_358_979_3,
            0.0,
            9.999_999_999,
        ),
        (
            "near-antipodal short edge",
            p(0.0, 0.0),
            p(1.0e-10, 0.0),
            0.314_159_265_358_979_3,
            179.999,
            0.001,
        ),
    ];
    let mut observations = Vec::with_capacity(cases.len());
    with_ellipsoid_metric("EPSG:4326", &[], |metric| {
        for (name, start, end, parameter, dx, dy) in cases {
            let witness = p(
                start.x + (end.x - start.x) * parameter,
                start.y + (end.y - start.y) * parameter,
            );
            let query = p(witness.x + dx, witness.y + dy);
            let attained: f64 = geodesic.inverse(query.y, query.x, witness.y, witness.x);
            let scale = attained.max(1.0);
            let rounding = 32.0 * (scale.next_up() - scale);
            let limit = (attained + 4.0e-11 * 6_378_137.0 + rounding).next_up();
            let edge = metric.chart_edge(start, end)?;
            let envelope_lower = metric.point_envelope_bound(query)(edge.bounds());
            let internal = metric.point_edge_query(
                query,
                &edge,
                envelope_lower,
                crate::geometry::PointEdgeQuery::Dwithin { limit },
                &mut PointEdgeScratch::default(),
            )?;
            let classification = match internal {
                crate::geometry::PointEdgeQueryResult::Within { .. } => "within",
                crate::geometry::PointEdgeQueryResult::Outside { lower_bound, .. } => {
                    assert!(
                        lower_bound > limit,
                        "{name}: outside requires lower {lower_bound} > limit {limit}"
                    );
                    "outside"
                },
                crate::geometry::PointEdgeQueryResult::Unresolved { .. } => "unresolved",
                other => panic!("{name}: unexpected threshold result {other:?}"),
            };
            observations.push((name, start, end, query, limit, attained, classification));
        }
        Ok(())
    })?;

    for (name, start, end, query, limit, attained, classification) in observations {
        Python::attach(|py| {
            let frame = Frame::new(Some(crs_arc_static("EPSG:4326")), None)
                .expect("WGS 84 is a valid geographic frame");
            let point = crate::Typed(crate::PyGeometry::with_frame(
                Shape::Point(query),
                frame.clone(),
            ))
            .into_pyobject(py)
            .expect("query point binds as its public typed geometry");
            let line = crate::Typed(crate::PyGeometry::with_frame(line(vec![start, end]), frame))
                .into_pyobject(py)
                .expect("source line binds as its public typed geometry");
            let distance = limit
                .into_pyobject(py)
                .expect("threshold binds as a Python float");
            let result = crate::dwithin(py, point.as_any(), line.as_any(), distance.as_any(), None);
            match classification {
                "within" => assert!(
                    result
                        .expect("a sampled chart witness within the radius returns True")
                        .bind(py)
                        .extract::<bool>()
                        .expect("dwithin result is bool")
                ),
                "unresolved" => assert!(
                    result
                        .expect_err("an unresolved threshold raises instead of returning False")
                        .to_string()
                        .contains(
                            "geodesic dwithin did not converge within 128 point-edge evaluations"
                        )
                ),
                "outside" => {
                    assert!(
                        attained <= limit,
                        "{name}: independent point witness {attained} exceeds {limit}"
                    );
                    assert!(
                        result
                            .expect("independent interior witness proves the result within")
                            .bind(py)
                            .extract::<bool>()
                            .expect("dwithin result is bool")
                    );
                },
                _ => unreachable!("the internal result classified the threshold"),
            }
        });
    }
    Ok(())
}

#[test]
fn fixed_short_parallel_pairs_have_the_meridional_minimum() -> Result<()> {
    let cases = [
        (
            "equatorial overlapping longitude intervals",
            [p(-0.2, 0.0), p(0.2, 0.0)],
            [p(-0.1, 0.02), p(0.3, 0.02)],
            [p(0.0, 0.0), p(0.0, 0.02)],
        ),
        (
            "high latitude overlapping longitude intervals",
            [p(-120.0, 80.0), p(-119.55, 80.0)],
            [p(-119.9, 80.19), p(-119.45, 80.19)],
            [p(-119.8, 80.0), p(-119.8, 80.19)],
        ),
        (
            "seam crossing overlapping longitude intervals",
            [p(179.8, 45.0), p(-179.8, 45.0)],
            [p(179.9, 45.02), p(-179.7, 45.02)],
            [p(180.0, 45.0), p(180.0, 45.02)],
        ),
    ];
    let geodesic = Geodesic::wgs84();
    with_ellipsoid_metric("EPSG:4326", &[], |metric| {
        let (semi_major, flattening) = metric.ellipsoid_parameters();
        for (name, first_points, second_points, reference) in cases {
            let first_shape = line(first_points.to_vec());
            let second_shape = line(second_points.to_vec());
            let first = PreparedPlan::new(first_shape);
            let second = PreparedPlan::new(second_shape);
            let actual = first.geodesic_distance_cached(
                &FrameDependentCaches::default(),
                &second,
                &FrameDependentCaches::default(),
                "EPSG:4326",
                semi_major,
                flattening,
                metric,
            )?;
            let [a, b] = reference;
            let expected: f64 = geodesic.inverse(a.y, a.x, b.y, b.x);
            assert!(
                (actual - expected).abs() <= MINIMUM_GAP_METERS,
                "{name}: distance {actual} differs from meridional minimum {expected}"
            );
        }
        Ok(())
    })?;
    Ok(())
}

/// The textbook FULL `O(n·m)` discrete-Fréchet DP — the oracle the banded
/// [`frechet_dp_columns`] must reproduce bit-for-bit.
fn frechet_full_reference(sx: &[f64], sy: &[f64], lx: &[f64], ly: &[f64]) -> f64 {
    let width = sx.len();
    let mut previous = vec![0.0_f64; width];
    let mut current = vec![0.0_f64; width];
    let mut running = 0.0_f64;
    for index in 0..width {
        let (dx, dy) = (sx[index] - lx[0], sy[index] - ly[0]);
        running = running.max(dx * dx + dy * dy);
        previous[index] = running;
    }
    for row in 1..lx.len() {
        let (dx, dy) = (sx[0] - lx[row], sy[0] - ly[row]);
        let mut left = (dx * dx + dy * dy).max(previous[0]);
        current[0] = left;
        for index in 1..width {
            let (dx, dy) = (sx[index] - lx[row], sy[index] - ly[row]);
            let reach = previous[index]
                .min(previous[index.wrapping_sub(1)])
                .min(left);
            left = (dx * dx + dy * dy).max(reach);
            current[index] = left;
        }
        std::mem::swap(&mut previous, &mut current);
    }
    previous[width.wrapping_sub(1)].sqrt()
}

#[test]
fn banded_frechet_matches_full_dp_bit_for_bit() {
    let base = |n| {
        (0..n)
            .map(|i| {
                let i = f64::from(i);
                (i, 5.0 * i.sin())
            })
            .unzip::<_, _, Vec<_>, Vec<_>>()
    };
    let perturb = |n, magnitude| {
        let (xs, ys) = base(n);
        xs.into_iter()
            .zip(ys)
            .enumerate()
            .map(|(i, (x, y))| {
                let sign = if i % 2 == 0 { 1.0 } else { -1.0 };
                (x + sign * magnitude, y - sign * magnitude)
            })
            .unzip::<_, _, Vec<_>, Vec<_>>()
    };
    let check = |name, (sx, sy): (Vec<f64>, Vec<f64>), (lx, ly): (Vec<f64>, Vec<f64>)| {
        let banded = frechet_dp_columns(&sx, &sy, &lx, &ly);
        let reference = frechet_full_reference(&sx, &sy, &lx, &ly);
        assert_eq!(
            banded.to_bits(),
            reference.to_bits(),
            "{name}: banded {banded} != full {reference}"
        );
    };
    check("1x1_identical", base(1), base(1));
    check("1x5_base", base(1), base(5));
    check("5x1_base", base(5), base(1));
    check("16x16_near", base(16), perturb(16, 1e-9));
    check("7x13_moderate", base(7), perturb(13, 0.5));
    check("13x7_moderate", perturb(13, 0.5), base(7));
    check("40x39_far", base(40), perturb(39, 50.0));
    check("39x40_far", perturb(39, 50.0), base(40));
    let (mut reversed_x, mut reversed_y) = base(5);
    reversed_x.reverse();
    reversed_y.reverse();
    check("5x5_reversed", base(5), (reversed_x, reversed_y));
}
