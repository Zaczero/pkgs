#![expect(
    clippy::panic_in_result_fn,
    reason = "tests assert, and return Result only to propagate setup errors"
)]
use geographiclib_rs::{Geodesic, InverseGeodesic as _};
use pyo3::prelude::*;

use crate::crs::frame::{Frame, crs_arc_static};
use crate::crs::{PointEdgeScratch, with_ellipsoid_metric};
use crate::error::Result;
use crate::geometry::distance::frechet::frechet_dp_columns;
use crate::geometry::distance::geodesic_sweep::geodesic_target_witness;
use crate::geometry::distance::tests::{
    assert_geodesic_close, dense_line, line, multipoint, p, polygon,
};
use crate::geometry::distance::{
    GEODESIC_HAUSDORFF_TOLERANCE, GeodesicHausdorffWitness, GeodesicScratch,
    collect_point_only_into, geodesic_directed_hausdorff_distance, geodesic_hausdorff_counters,
    geodesic_max_min_on_source_segment, geodesic_scratch,
};
use crate::geometry::{
    ChartMetric, FrameDependentCaches, GeodesicMetric, GeodesicSegmentScratch, GeographicPrepared,
    MOrdinate, Point, PreparedPlan, Shape, ZOrdinate, graph_relate_data_geographic,
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

fn brute_geodesic_directed_hausdorff(
    source: &Shape,
    target: &Shape,
    metric: &impl GeodesicMetric,
) -> f64 {
    const SAMPLE_FRACTIONS: [f64; 9] = [
        0.0,
        1.0 / 8.0,
        2.0 / 8.0,
        3.0 / 8.0,
        4.0 / 8.0,
        5.0 / 8.0,
        6.0 / 8.0,
        7.0 / 8.0,
        1.0,
    ];
    let target_parts = target.geodesic_parts(metric).expect("unique test segments");
    let mut scratch = geodesic_scratch();
    let scratch = &mut *scratch;
    let (stack, segment_scratch) = (&mut scratch.stack, &mut scratch.segment);
    let mut cmax = 0.0_f64;
    let mut evaluate = |point: Point| {
        let Some(candidate) =
            geodesic_target_witness(point, &target_parts, metric, stack, segment_scratch)
                .expect("complete target query should succeed")
        else {
            return;
        };
        cmax = cmax.max(candidate.distance);
    };
    source.for_each_point(&mut evaluate);
    source.for_each_vertex_pair(|start, end| {
        for fraction in SAMPLE_FRACTIONS {
            evaluate(metric.interpolate(start, end, fraction));
        }
    });
    cmax
}

fn directed_geodesic_witness(
    source: &Shape,
    target: &Shape,
    metric: &impl GeodesicMetric,
) -> GeodesicHausdorffWitness {
    let source_parts = source.geodesic_parts(metric).unwrap();
    let target_parts = target.geodesic_parts(metric).unwrap();
    let mut scratch = geodesic_scratch();
    geodesic_directed_hausdorff_distance(&source_parts, &target_parts, metric, &mut scratch)
        .unwrap()
}

fn brute_geodesic_target_witness(
    point: Point,
    parts: &crate::geometry::GeodesicParts,
    metric: &impl GeodesicMetric,
) -> Result<Option<(f64, Point)>> {
    let mut best = f64::INFINITY;
    let mut target_witness = None;
    let mut scratch = GeodesicSegmentScratch::default();
    for &segment in &parts.segments {
        let result = metric.point_segment_query(
            point,
            &segment,
            crate::geometry::GeodesicSegmentQuery::Minimum {
                cutoff: best,
                witness: crate::geometry::GeodesicWitnessNeed::Yes,
            },
            &mut scratch,
        )?;
        if let crate::geometry::GeodesicSegmentQueryResult::Attained {
            minimum,
            foot: Some(target),
        } = result
            && minimum.distance < best
        {
            best = minimum.distance;
            target_witness = Some(target);
        }
    }
    for &target in &parts.point_only {
        let distance = metric.segment_length(point, target);
        if distance < best {
            best = distance;
            target_witness = Some(target);
        }
    }
    Ok(target_witness.map(|target| (best, target)))
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

        let distance = crate::distance(py, &point, &line, None)
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

fn dense_directed_geodesic(
    source: &Shape,
    target: &Shape,
    metric: &impl GeodesicMetric,
    steps: usize,
) -> f64 {
    let target_parts = target.geodesic_parts(metric).unwrap();
    let mut scratch = geodesic_scratch();
    let scratch = &mut *scratch;
    let (stack, segment) = (&mut scratch.stack, &mut scratch.segment);
    let mut maximum = 0.0_f64;
    let mut evaluate = |point: Point| {
        let candidate = geodesic_target_witness(point, &target_parts, metric, stack, segment)
            .unwrap()
            .unwrap();
        maximum = maximum.max(candidate.distance);
    };
    source.for_each_point(&mut evaluate);
    source.for_each_vertex_pair(|start, end| {
        for step in 0..=steps {
            evaluate(metric.interpolate(start, end, step as f64 / steps.max(1) as f64));
        }
    });
    maximum
}

fn assert_geodesic_witness_attained(
    witness: &GeodesicHausdorffWitness,
    metric: &impl GeodesicMetric,
) {
    let recomputed = metric.segment_length(witness.source, witness.target);
    let allowance =
        8.0 * metric.geodesic_backend_floor() + 128.0 * f64::EPSILON * witness.distance.max(1.0);
    assert!(
        (witness.distance - recomputed).abs() <= allowance,
        "witness distance {} was not attained by {} within {}",
        witness.distance,
        recomputed,
        allowance
    );
}

#[test]
fn geodesic_point_collection_preserves_nested_source_order() {
    let nested = Shape::geometry_collection(vec![
        multipoint(vec![p(2.0, 0.0), p(3.0, 0.0)]),
        Shape::geometry_collection(vec![Shape::Point(p(4.0, 0.0))])
            .expect("nested point collection is valid"),
    ])
    .expect("point collection is valid");
    let shape = Shape::geometry_collection(vec![
        Shape::Point(p(1.0, 0.0)),
        nested,
        Shape::geometry_collection(Vec::new()).expect("empty collection is valid"),
        line(vec![p(10.0, 0.0), p(11.0, 0.0)]),
    ])
    .expect("mixed collection is valid");
    let mut points = vec![p(-1.0, 0.0)];

    collect_point_only_into(&shape, &mut points);

    assert_eq!(points, vec![
        p(1.0, 0.0),
        p(2.0, 0.0),
        p(3.0, 0.0),
        p(4.0, 0.0)
    ]);
}

#[test]
fn geodesic_parts_retain_degenerate_line_support() -> Result<()> {
    let degenerate = line(vec![p(0.0, 0.0), p(0.0, 0.0)]);
    with_ellipsoid_metric("EPSG:4326", &[&degenerate], |metric| {
        let parts = degenerate.geodesic_parts(metric)?;
        assert!(parts.segments.is_empty());
        assert_eq!(parts.point_only.len(), 1);
        Ok(())
    })?;
    Ok(())
}

#[test]
fn geodesic_parts_coalesce_depth_128_shared_line_owners() -> Result<()> {
    for coordinates in [vec![p(0.0, 0.0), p(1.0, 0.0)], vec![
        p(179.0, 0.0),
        p(-179.0, 0.0),
    ]] {
        let target = line(coordinates.clone());
        let mut shared = line(coordinates);
        for _ in 0..128 {
            shared = Shape::geometry_collection(vec![shared.clone(), shared])
                .expect("shared line DAG stays within the collection depth");
        }

        with_ellipsoid_metric("EPSG:4326", &[&shared, &target], |metric| {
            let parts = shared.geodesic_parts(metric)?;
            assert_eq!(parts.segments.len(), 1);
            assert!(parts.point_only.is_empty());

            let left = PreparedPlan::new(shared.clone());
            let right = PreparedPlan::new(target.clone());
            let left_cache = FrameDependentCaches::default();
            let right_cache = FrameDependentCaches::default();
            let (semi_major, flattening) = metric.ellipsoid_parameters();
            assert_eq!(
                left.geodesic_distance_cached(
                    &left_cache,
                    &right,
                    &right_cache,
                    "EPSG:4326",
                    semi_major,
                    flattening,
                    metric,
                )?,
                0.0
            );
            assert!(left.geodesic_dwithin_cached(
                &left_cache,
                &right,
                &right_cache,
                "EPSG:4326",
                semi_major,
                flattening,
                metric,
                0.0,
            )?);
            Ok(())
        })?;
    }
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
fn geodesic_parts_collapse_periodic_zero_edge_support() -> Result<()> {
    let seam = line(vec![p(-180.0, 0.0), p(180.0, 0.0)]);
    with_ellipsoid_metric("EPSG:4326", &[&seam], |metric| {
        let parts = seam.geodesic_parts(metric)?;
        assert!(parts.segments.is_empty());
        assert_eq!(parts.point_only, [p(-180.0, 0.0)]);
        Ok(())
    })?;
    Ok(())
}

#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "the test returns Result for fallible setup and uses normal assertion macros for its oracle"
)]
fn geodesic_hausdorff_is_segment_aware() -> Result<()> {
    let source = line(vec![p(0.0, 0.0), p(0.5, 0.0)]);
    let target = line(vec![p(0.0, 0.05), p(0.25, 0.4), p(0.5, 0.05)]);
    with_ellipsoid_metric("EPSG:4326", &[&source, &target], |metric| {
        let source_points = source.points_vec()?;
        let target_points = target.points_vec()?;
        let source_parts = source.geodesic_parts(metric)?;
        let target_parts = target.geodesic_parts(metric)?;
        let mut scratch = geodesic_scratch();
        let scratch = &mut *scratch;

        let source_to_target =
            geodesic_directed_hausdorff_distance(&source_parts, &target_parts, metric, scratch)?;
        let source_vertices_only =
            brute_geodesic_directed_hausdorff(&multipoint(source_points), &target, metric);
        assert!(
            source_to_target.distance > source_vertices_only,
            "source interior did not affect the directed maximum: {} <= {source_vertices_only}",
            source_to_target.distance
        );

        let target_vertices_only =
            brute_geodesic_directed_hausdorff(&source, &multipoint(target_points), metric);
        assert!(
            source_to_target.distance < target_vertices_only,
            "target interior did not affect the directed maximum: {} >= {target_vertices_only}",
            source_to_target.distance
        );

        let expected_source_to_target = brute_geodesic_directed_hausdorff(&source, &target, metric);
        assert_geodesic_close(
            "source_to_target",
            source_to_target.distance,
            expected_source_to_target,
        );

        let actual = source.geodesic_hausdorff_distance(&target, metric)?;
        let target_to_source = brute_geodesic_directed_hausdorff(&target, &source, metric);
        assert_geodesic_close(
            "symmetric",
            actual,
            source_to_target.distance.max(target_to_source),
        );
        Ok(())
    })?;
    Ok(())
}

#[test]
fn geodesic_hausdorff_zero_and_polar_plateaus_use_attained_witnesses() -> Result<()> {
    let source = line(vec![p(-10.0, 0.0), p(10.0, 0.0)]);
    let reversed = line(vec![p(10.0, 0.0), p(-10.0, 0.0)]);
    let pole = Shape::Point(p(0.0, 90.0));
    with_ellipsoid_metric("EPSG:4326", &[&source, &reversed, &pole], |metric| {
        geodesic_hausdorff_counters::reset();
        let identical = directed_geodesic_witness(&source, &source, metric);
        assert_eq!(identical.distance, 0.0);
        assert_geodesic_witness_attained(&identical, metric);
        let identical_stats = geodesic_hausdorff_counters::snapshot();
        assert_eq!(identical_stats.outer_source_segments, 1);
        assert!(identical_stats.outer_complete_target_queries >= 3);
        assert!(identical_stats.outer_interval_pops > 0);
        assert_eq!(identical_stats.outer_interval_splits, 0);
        assert!(identical_stats.outer_moving_supports > 0);
        assert!(identical_stats.outer_support_closures > 0);

        geodesic_hausdorff_counters::reset();
        let reversed_witness = directed_geodesic_witness(&source, &reversed, metric);
        assert_eq!(reversed_witness.distance, 0.0);
        assert_geodesic_witness_attained(&reversed_witness, metric);
        assert!(geodesic_hausdorff_counters::snapshot().outer_moving_supports > 0);

        geodesic_hausdorff_counters::reset();
        let polar_witness = directed_geodesic_witness(&source, &pole, metric);
        let polar_distance = metric.segment_length(p(0.0, 0.0), p(0.0, 90.0));
        assert_geodesic_close("polar plateau", polar_witness.distance, polar_distance);
        assert_geodesic_witness_attained(&polar_witness, metric);
        let polar_stats = geodesic_hausdorff_counters::snapshot();
        assert!(polar_stats.outer_family_supports > 0);
        assert!(polar_stats.outer_support_closures > 0);

        let symmetric = source.geodesic_hausdorff_distance(&pole, metric)?;
        assert_geodesic_close("symmetric polar plateau", symmetric, polar_distance);
        Ok(())
    })?;
    Ok(())
}

#[test]
fn geodesic_hausdorff_smooth_and_cusp_extrema_are_attained() -> Result<()> {
    let source = line(vec![p(0.0, 0.0), p(10.0, 0.0)]);
    let smooth_target = line(vec![p(0.0, 1.0), p(5.0, 8.0), p(10.0, 1.0)]);
    let cusp_target = line(vec![p(0.0, 1.0), p(5.0, 0.2), p(10.0, 1.0)]);
    with_ellipsoid_metric(
        "EPSG:4326",
        &[&source, &smooth_target, &cusp_target],
        |metric| {
            for (name, target) in [("smooth", &smooth_target), ("cusp", &cusp_target)] {
                geodesic_hausdorff_counters::reset();
                let witness = directed_geodesic_witness(&source, target, metric);
                let dense = dense_directed_geodesic(&source, target, metric, 512);
                let allowance = GEODESIC_HAUSDORFF_TOLERANCE
                    + 8.0 * metric.geodesic_backend_floor()
                    + 128.0 * f64::EPSILON * dense.max(1.0);
                assert!(
                    dense <= witness.distance + allowance,
                    "{name}: dense {dense} exceeded {} by more than {allowance}",
                    witness.distance
                );
                assert_geodesic_witness_attained(&witness, metric);
                let stats = geodesic_hausdorff_counters::snapshot();
                assert!(stats.outer_interval_pops > 0);
                assert!(stats.outer_complete_target_queries >= 3);
                assert!(
                    stats.outer_interval_splits > 0 || stats.outer_support_closures > 0,
                    "{name}: {stats:?}"
                );
                if name == "smooth" {
                    assert!(stats.outer_regular_supports > 0, "{stats:?}");
                }
                if name == "cusp" {
                    assert!(stats.outer_regular_derivative_declines > 0, "{stats:?}");
                }
            }
            let reverse = directed_geodesic_witness(&smooth_target, &source, metric);
            let reverse_dense = dense_directed_geodesic(&smooth_target, &source, metric, 512);
            assert!(reverse_dense <= reverse.distance + GEODESIC_HAUSDORFF_TOLERANCE);
            assert_geodesic_witness_attained(&reverse, metric);
            let symmetric = source.geodesic_hausdorff_distance(&smooth_target, metric)?;
            assert_geodesic_close(
                "symmetric smooth",
                symmetric,
                directed_geodesic_witness(&source, &smooth_target, metric)
                    .distance
                    .max(reverse.distance),
            );
            Ok(())
        },
    )?;
    Ok(())
}

#[test]
fn geodesic_hausdorff_endpoint_transition_falls_back_safely() -> Result<()> {
    let source = line(vec![p(-5.0, 0.0), p(5.0, 0.0)]);
    let target = line(vec![p(0.0, 0.1), p(0.0, 5.0)]);
    with_ellipsoid_metric("EPSG:4326", &[&source, &target], |metric| {
        let witness = directed_geodesic_witness(&source, &target, metric);
        let dense = dense_directed_geodesic(&source, &target, metric, 512);
        let allowance = GEODESIC_HAUSDORFF_TOLERANCE
            + 8.0 * metric.geodesic_backend_floor()
            + 128.0 * f64::EPSILON * dense.max(1.0);
        assert!(dense <= witness.distance + allowance);
        assert_geodesic_witness_attained(&witness, metric);
        let reverse = directed_geodesic_witness(&target, &source, metric);
        assert_geodesic_witness_attained(&reverse, metric);
        Ok(())
    })?;
    Ok(())
}

#[test]
fn geodesic_hausdorff_regular_support_declines_at_domain_boundary() -> Result<()> {
    let source = line(vec![p(0.0, 0.0), p(1.0, 0.0)]);
    let target = line(vec![p(90.0, -10.0), p(90.0, 10.0)]);
    with_ellipsoid_metric("EPSG:4326", &[&source, &target], |metric| {
        geodesic_hausdorff_counters::reset();
        let witness = directed_geodesic_witness(&source, &target, metric);
        let dense = dense_directed_geodesic(&source, &target, metric, 256);
        assert!(dense <= witness.distance + GEODESIC_HAUSDORFF_TOLERANCE);
        assert_geodesic_witness_attained(&witness, metric);
        let stats = geodesic_hausdorff_counters::snapshot();
        assert!(stats.outer_regular_attempts > 0, "{stats:?}");
        assert!(stats.outer_regular_domain_declines > 0, "{stats:?}");
        assert_eq!(stats.outer_regular_supports, 0, "{stats:?}");
        assert!(stats.outer_interval_splits > 0, "{stats:?}");
        Ok(())
    })?;
    Ok(())
}

#[test]
fn geodesic_hausdorff_near_cut_fallback_is_endpoint_governed() -> Result<()> {
    let source = line(vec![p(-1.0, 0.0), p(1.0, 0.0)]);
    // The short target segment is unambiguous, but its selected near-antipodal
    // connector ends at a target endpoint, so regular interior-foot support
    // must decline and the global fallback must finish the case.
    let target = line(vec![p(179.0, -1.0), p(179.0, 1.0)]);
    with_ellipsoid_metric("EPSG:4326", &[&source, &target], |metric| {
        geodesic_hausdorff_counters::reset();
        let witness = directed_geodesic_witness(&source, &target, metric);
        let dense = dense_directed_geodesic(&source, &target, metric, 256);
        assert!(dense <= witness.distance + GEODESIC_HAUSDORFF_TOLERANCE);
        assert_geodesic_witness_attained(&witness, metric);
        let stats = geodesic_hausdorff_counters::snapshot();
        assert!(stats.outer_regular_attempts > 0, "{stats:?}");
        assert!(stats.outer_regular_foot_endpoint_declines > 0, "{stats:?}");
        assert_eq!(stats.outer_regular_domain_declines, 0, "{stats:?}");
        assert_eq!(stats.outer_regular_supports, 0, "{stats:?}");
        assert!(stats.outer_interval_pops > 0, "{stats:?}");
        assert!(stats.outer_interval_splits > 0, "{stats:?}");
        Ok(())
    })?;
    Ok(())
}

#[test]
fn geodesic_hausdorff_regular_support_declines_at_target_patch_endpoint() -> Result<()> {
    let source = line(vec![p(0.0, 0.0), p(10.0, 0.0)]);
    let target = line(vec![p(5.0, 0.1), p(5.0, 5.0)]);
    with_ellipsoid_metric("EPSG:4326", &[&source, &target], |metric| {
        geodesic_hausdorff_counters::reset();
        let witness = directed_geodesic_witness(&source, &target, metric);
        let dense = dense_directed_geodesic(&source, &target, metric, 256);
        assert!(dense <= witness.distance + GEODESIC_HAUSDORFF_TOLERANCE);
        assert_geodesic_witness_attained(&witness, metric);
        let stats = geodesic_hausdorff_counters::snapshot();
        assert!(stats.outer_regular_attempts > 0, "{stats:?}");
        assert!(stats.outer_regular_foot_endpoint_declines > 0, "{stats:?}");
        assert_eq!(stats.outer_regular_supports, 0, "{stats:?}");
        assert!(stats.outer_interval_splits > 0, "{stats:?}");
        Ok(())
    })?;
    Ok(())
}

#[test]
fn geodesic_hausdorff_regular_support_declines_for_near_zero_distance() -> Result<()> {
    let source = line(vec![p(0.0, 0.0), p(20.0, 0.0)]);
    let target = line(vec![p(0.0, 1e-12), p(20.0, 1e-12)]);
    with_ellipsoid_metric("EPSG:4326", &[&source, &target], |metric| {
        geodesic_hausdorff_counters::reset();
        let witness = directed_geodesic_witness(&source, &target, metric);
        let dense = dense_directed_geodesic(&source, &target, metric, 256);
        assert!(dense <= witness.distance + GEODESIC_HAUSDORFF_TOLERANCE);
        assert_geodesic_witness_attained(&witness, metric);
        let stats = geodesic_hausdorff_counters::snapshot();
        assert!(stats.outer_regular_attempts > 0, "{stats:?}");
        assert!(stats.outer_regular_near_zero_declines > 0, "{stats:?}");
        assert!(stats.outer_moving_supports > 0, "{stats:?}");
        assert_eq!(stats.outer_interval_splits, 0, "{stats:?}");
        Ok(())
    })?;
    Ok(())
}

#[test]
fn geodesic_hausdorff_finite_foot_branch_keeps_shifted_peak() -> Result<()> {
    let source = line(vec![p(0.0, 0.0), p(10.0, 0.0)]);
    let target = line(vec![p(0.0, 0.5), p(7.0, 8.0), p(10.0, 0.5)]);
    with_ellipsoid_metric("EPSG:4326", &[&source, &target], |metric| {
        geodesic_hausdorff_counters::reset();
        let witness = directed_geodesic_witness(&source, &target, metric);
        let dense = dense_directed_geodesic(&source, &target, metric, 1_024);
        let allowance = GEODESIC_HAUSDORFF_TOLERANCE
            + 8.0 * metric.geodesic_backend_floor()
            + 128.0 * f64::EPSILON * dense.max(1.0);
        assert!(dense <= witness.distance + allowance);
        assert_geodesic_witness_attained(&witness, metric);
        let stats = geodesic_hausdorff_counters::snapshot();
        assert!(stats.outer_regular_supports > 0, "{stats:?}");
        assert!(
            witness.source.x > 5.0,
            "shifted peak was not found: {witness:?}"
        );
        Ok(())
    })?;
    Ok(())
}

#[test]
fn spherical_geodesic_hausdorff_closes_focal_plateaus() -> Result<()> {
    let radius = 6_378_137.0;
    let geodesic = geographiclib_rs::Geodesic::new(radius, 0.0);
    let metric = crate::crs::EllipsoidMetric::for_crs84_geometry_test(&geodesic);
    let source = line(vec![p(-30.0, 0.0), p(30.0, 0.0)]);
    let target = Shape::Point(p(0.0, 90.0));
    let witness = directed_geodesic_witness(&source, &target, &metric);
    let expected = metric.segment_length(p(0.0, 0.0), p(0.0, 90.0));
    assert_geodesic_close("spherical polar", witness.distance, expected);
    assert_geodesic_witness_attained(&witness, &metric);
    let parts = source.geodesic_parts(&metric)?;
    let target_parts = target.geodesic_parts(&metric)?;
    let mut scratch = geodesic_scratch();
    let source_witness =
        geodesic_directed_hausdorff_distance(&parts, &target_parts, &metric, &mut scratch)?;
    assert_eq!(source_witness.distance.to_bits(), expected.to_bits());

    let oblique_source = line(vec![p(-20.0, -20.0), p(40.0, 30.0)]);
    let oblique_target = Shape::Point(p(120.0, 10.0));
    geodesic_hausdorff_counters::reset();
    let oblique_witness = directed_geodesic_witness(&oblique_source, &oblique_target, &metric);
    let oblique_dense = dense_directed_geodesic(&oblique_source, &oblique_target, &metric, 512);
    assert!(oblique_dense <= oblique_witness.distance + GEODESIC_HAUSDORFF_TOLERANCE);
    assert_geodesic_witness_attained(&oblique_witness, &metric);
    assert!(geodesic_hausdorff_counters::snapshot().outer_family_supports > 0);
    Ok(())
}

#[test]
fn spherical_geodesic_hausdorff_closes_descendant_fixed_target_phase() {
    let radius = 6_378_137.0;
    let geodesic = geographiclib_rs::Geodesic::new(radius, 0.0);
    let metric = crate::crs::EllipsoidMetric::for_crs84_geometry_test(&geodesic);
    let source = line(vec![p(0.0, 0.0), p(120.0, 0.0)]);
    let target = Shape::Point(p(-105.0, 20.0));

    geodesic_hausdorff_counters::reset();
    let witness = directed_geodesic_witness(&source, &target, &metric);
    let expected = metric.segment_length(p(75.0, 0.0), p(-105.0, 20.0));
    let dense = dense_directed_geodesic(&source, &target, &metric, 1_024);
    assert_geodesic_close("spherical descendant phase", witness.distance, expected);
    assert!(dense <= witness.distance + GEODESIC_HAUSDORFF_TOLERANCE);
    assert!(
        (witness.source.x - 75.0).abs() <= 1e-12,
        "stationary source longitude: {}",
        witness.source.x
    );
    assert_geodesic_witness_attained(&witness, &metric);
    let stats = geodesic_hausdorff_counters::snapshot();
    assert!(stats.outer_interval_splits > 0, "{stats:?}");
    assert!(stats.outer_family_supports > 0, "{stats:?}");
}

#[test]
fn geodesic_hausdorff_equatorial_fixed_point_family_is_not_polar_only() -> Result<()> {
    let source = line(vec![p(-30.0, 0.0), p(30.0, 0.0)]);
    let target = Shape::Point(p(0.0, 89.999));
    let crossing_source = line(vec![p(100.0, 0.0), p(-100.0, 0.0)]);
    let crossing_target = Shape::Point(p(0.0, 10.0));
    with_ellipsoid_metric(
        "EPSG:4326",
        &[&source, &target, &crossing_source, &crossing_target],
        |metric| {
            geodesic_hausdorff_counters::reset();
            let witness = directed_geodesic_witness(&source, &target, metric);
            let center = metric.segment_length(p(0.0, 0.0), p(0.0, 89.999));
            let endpoint = metric.segment_length(p(30.0, 0.0), p(0.0, 89.999));
            let dense = dense_directed_geodesic(&source, &target, metric, 512);
            assert!(endpoint > center);
            assert!(witness.distance > center);
            assert!(dense <= witness.distance + GEODESIC_HAUSDORFF_TOLERANCE);
            assert_geodesic_witness_attained(&witness, metric);
            assert!(geodesic_hausdorff_counters::snapshot().outer_family_supports > 0);

            geodesic_hausdorff_counters::reset();
            let crossing_witness =
                directed_geodesic_witness(&crossing_source, &crossing_target, metric);
            let crossing_dense =
                dense_directed_geodesic(&crossing_source, &crossing_target, metric, 512);
            assert_eq!(
                crossing_witness.source.x.abs().to_bits(),
                180.0_f64.to_bits()
            );
            assert!(crossing_dense <= crossing_witness.distance + GEODESIC_HAUSDORFF_TOLERANCE);
            assert_geodesic_witness_attained(&crossing_witness, metric);
            assert!(geodesic_hausdorff_counters::snapshot().outer_family_supports > 0);
            Ok(())
        },
    )?;
    Ok(())
}

#[test]
fn geodesic_hausdorff_point_only_and_mixed_targets_are_complete() -> Result<()> {
    let source = line(vec![p(-4.0, 0.0), p(4.0, 0.0)]);
    let points = multipoint(vec![p(-2.0, 1.0), p(2.0, 1.0)]);
    let mixed =
        Shape::geometry_collection(vec![points.clone(), line(vec![p(-3.0, 2.0), p(3.0, 2.0)])])
            .expect("mixed target is valid");
    with_ellipsoid_metric("EPSG:4326", &[&source, &points, &mixed], |metric| {
        for target in [&points, &mixed] {
            let witness = directed_geodesic_witness(&source, target, metric);
            let dense = dense_directed_geodesic(&source, target, metric, 256);
            assert!(dense <= witness.distance + GEODESIC_HAUSDORFF_TOLERANCE);
            assert_geodesic_witness_attained(&witness, metric);
        }
        let forward = source.geodesic_hausdorff_distance(&mixed, metric)?;
        let reverse = mixed.geodesic_hausdorff_distance(&source, metric)?;
        assert!(forward.is_finite() && reverse.is_finite());
        assert!(
            Shape::empty_point()
                .geodesic_hausdorff_distance(&source, metric)?
                .is_infinite()
        );
        Ok(())
    })?;
    Ok(())
}

#[test]
fn geodesic_hausdorff_complete_target_uses_bvh_witnesses() -> Result<()> {
    let source = line(vec![p(0.0, 1.0), p(4.0, 1.0)]);
    let target = dense_line(0.0, 0.0, 65);
    with_ellipsoid_metric("EPSG:4326", &[&source, &target], |metric| {
        let target_parts = target.geodesic_parts(metric)?;
        assert!(target_parts.geodesic_bvh(metric).is_some());
        geodesic_hausdorff_counters::reset();
        let witness = directed_geodesic_witness(&source, &target, metric);
        let stats = geodesic_hausdorff_counters::snapshot();
        assert!(stats.outer_bvh_target_queries > 0, "{stats:?}");
        let dense = dense_directed_geodesic(&source, &target, metric, 256);
        assert!(dense <= witness.distance + GEODESIC_HAUSDORFF_TOLERANCE);
        assert_geodesic_witness_attained(&witness, metric);
        Ok(())
    })?;
    Ok(())
}

#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "the test returns Result for metric and part setup and checks retained scratch reuse"
)]
fn geodesic_hausdorff_frontier_reuses_retained_scratch() -> Result<()> {
    use crate::test_support::allocation_count;

    let source = line(vec![p(0.0, 0.0), p(10.0, 0.0), p(20.0, 0.0), p(30.0, 0.0)]);
    let target = line(vec![p(0.0, 1.0), p(15.0, 8.0), p(30.0, 1.0)]);
    with_ellipsoid_metric("EPSG:4326", &[&source, &target], |metric| {
        let source_parts = source.geodesic_parts(metric)?;
        let target_parts = target.geodesic_parts(metric)?;
        let mut target_prepared = Vec::with_capacity(target_parts.segments.len());
        for segment in &target_parts.segments {
            target_prepared.push(metric.prepare_segment(segment)?);
        }
        let mut scratch = GeodesicScratch::default();
        for segment in &source_parts.segments {
            geodesic_max_min_on_source_segment(
                segment,
                &target_parts,
                &target_prepared,
                metric,
                0.0,
                &mut scratch,
            )?;
        }
        assert!(scratch.outer.capacity() > 0);
        let ((), allocations) = allocation_count(|| {
            for segment in &source_parts.segments {
                geodesic_max_min_on_source_segment(
                    segment,
                    &target_parts,
                    &target_prepared,
                    metric,
                    0.0,
                    &mut scratch,
                )
                .expect("warmed frontier must remain reusable");
            }
        });
        assert_eq!(allocations, 0, "warmed outer scratch allocations");
        Ok(())
    })?;
    Ok(())
}

#[test]
fn geodesic_facet_bvh_matches_brute_hausdorff_target_witness() -> Result<()> {
    let target = disconnected_bvh_target();
    let probes = [p(-1.0, 1.5), p(45.0, 1.5), p(128.0, 2.0)];
    with_ellipsoid_metric("EPSG:4326", &[&target], |metric| {
        let target_parts = target.geodesic_parts(metric)?;
        assert!(target_parts.geodesic_bvh(metric).is_some());
        let mut stack = Vec::new();
        let mut scratch = GeodesicSegmentScratch::default();
        for probe in probes {
            let expected = brute_geodesic_target_witness(probe, &target_parts, metric)?
                .expect("target has linework");
            let actual =
                geodesic_target_witness(probe, &target_parts, metric, &mut stack, &mut scratch)?
                    .expect("target has linework");
            assert_eq!((actual.distance, actual.target), expected);
            assert_eq!(
                (actual.distance - metric.segment_length(probe, actual.target)).abs(),
                0.0
            );
        }
        Ok(())
    })?;
    Ok(())
}

fn straight_ordinate_line(z: bool) -> Shape {
    line(
        (0..65)
            .map(|index| {
                let x = -2.0 + f64::from(index) / 16.0;
                if z {
                    Point::new_axes(
                        x,
                        0.0,
                        ZOrdinate(Some(100.0 + f64::from(index))),
                        MOrdinate(None),
                    )
                } else {
                    Point::new_axes(
                        x,
                        0.0,
                        ZOrdinate(None),
                        MOrdinate(Some(200.0 + f64::from(index))),
                    )
                }
                .expect("finite ordinate test point")
            })
            .collect(),
    )
}

#[test]
fn geodesic_bvh_witnesses_preserve_ordinates_across_shared_xy() -> Result<()> {
    let z_line = straight_ordinate_line(true);
    let m_line = straight_ordinate_line(false);
    let probe = p(0.0, 1.0);
    let cases = [
        (
            "xyz_first",
            Shape::geometry_collection(vec![z_line.clone(), m_line.clone()])
                .expect("mixed ordinate collection"),
            Some(132.0),
            None,
        ),
        (
            "xym_first",
            Shape::geometry_collection(vec![m_line, z_line]).expect("mixed ordinate collection"),
            None,
            Some(232.0),
        ),
    ];

    with_ellipsoid_metric("EPSG:4326", &[], |metric| {
        for (name, target, expected_z, expected_m) in cases {
            let target_parts = target.geodesic_parts(metric)?;
            assert!(
                target_parts.geodesic_bvh(metric).is_some(),
                "{name} bvh presence"
            );
            let expected = brute_geodesic_target_witness(probe, &target_parts, metric)?
                .expect("non-empty witness");
            let actual = geodesic_target_witness(
                probe,
                &target_parts,
                metric,
                &mut Vec::new(),
                &mut GeodesicSegmentScratch::default(),
            )?
            .expect("non-empty witness");
            assert_eq!((actual.distance, actual.target), expected, "{name} witness");
            assert_eq!(actual.target.z(), expected_z, "{name} target Z");
            assert_eq!(actual.target.m(), expected_m, "{name} target M");
            assert!(
                (actual.distance - metric.segment_length(probe, actual.target)).abs() <= 1e-3,
                "{name} witness is not attained"
            );
        }
        Ok(())
    })?;
    Ok(())
}

#[test]
fn geodesic_bvh_none_remains_for_empty_and_below_threshold_parts() -> Result<()> {
    let small = line(vec![p(0.0, 0.0), p(1.0, 0.0)]);
    let empty = Shape::empty_point();
    with_ellipsoid_metric("EPSG:4326", &[&small, &empty], |metric| {
        let small_parts = small.geodesic_parts(metric)?;
        let small_result =
            crate::test_support::assert_no_allocations(|| small_parts.geodesic_bvh(metric));
        assert!(small_result.is_none());

        let empty_parts = empty.geodesic_parts(metric)?;
        assert!(empty_parts.geodesic_bvh(metric).is_none());
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
