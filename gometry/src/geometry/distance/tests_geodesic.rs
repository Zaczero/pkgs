#![expect(
    clippy::panic_in_result_fn,
    reason = "tests assert, and return Result only to propagate setup errors"
)]
use crate::crs::{geodesic_counters, with_ellipsoid_metric};
use crate::error::{ErrorKind, ResourceError, Result};
use crate::geometry::distance::frechet::frechet_dp_columns;
use crate::geometry::distance::geodesic_parts::{
    geodesic_cap_from_parts, geodesic_dwithin_sweep_with_parts, geodesic_sweep_with_parts,
};
use crate::geometry::distance::geodesic_sweep::{
    geodesic_capped_witness_sweep_with_parts, geodesic_ordered_rows_into, geodesic_target_witness,
};
use crate::geometry::distance::tests::{
    assert_geodesic_close, assert_same_witness, brute_geodesic_distance,
    brute_geodesic_nearest_points, dense_line, geodesic_bvh_oracle_cases,
    geodesic_distance_oracle_cases, geodesic_nearest_oracle_cases, line, multipoint, p, polygon,
};
use crate::geometry::distance::{
    GEODESIC_HAUSDORFF_TOLERANCE, GeodesicHausdorffWitness, GeodesicScratch,
    collect_point_only_into, geodesic_capped_dwithin_with_parts, geodesic_capped_sweep_with_parts,
    geodesic_directed_hausdorff_distance, geodesic_distance_with_parts,
    geodesic_dwithin_with_parts, geodesic_hausdorff_counters, geodesic_max_min_on_source_segment,
    geodesic_nearest_points_with_parts, geodesic_scratch, geodesic_witness_sweep_with_parts,
};
use crate::geometry::{
    FrameDependentCaches, GeodesicMetric, GeodesicParts, GeodesicSegmentScratch, MOrdinate, Point,
    PreparedPlan, Shape, ZOrdinate,
};

fn fresh_bvh_parts(template: &GeodesicParts) -> GeodesicParts {
    GeodesicParts {
        points: Vec::new(),
        segments: template.segments.clone(),
        point_only: Vec::new(),
        caps: None,
        facet_bvh: std::sync::OnceLock::new(),
    }
}

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

fn live_geodesic_distance(
    left: &Shape,
    right: &Shape,
    metric: &impl GeodesicMetric,
) -> Result<f64> {
    let left_parts = left.geodesic_parts(metric)?;
    let right_parts = right.geodesic_parts(metric)?;
    geodesic_distance_with_parts(left, &left_parts, right, &right_parts, metric)
}

fn live_geodesic_nearest_points(
    left: &Shape,
    right: &Shape,
    metric: &impl GeodesicMetric,
) -> Result<Option<(Point, Point)>> {
    let left_parts = left.geodesic_parts(metric)?;
    let right_parts = right.geodesic_parts(metric)?;
    geodesic_nearest_points_with_parts(left, &left_parts, right, &right_parts, metric)
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

    collect_point_only_into(&shape, &mut points).unwrap();

    assert_eq!(points, vec![
        p(1.0, 0.0),
        p(2.0, 0.0),
        p(3.0, 0.0),
        p(4.0, 0.0)
    ]);
}

#[test]
fn geodesic_parts_retain_all_degenerate_line_support() -> Result<()> {
    let degenerate = line(vec![p(0.0, 0.0), p(0.0, 0.0)]);
    let point = Shape::Point(p(0.0, 0.0));
    with_ellipsoid_metric("EPSG:4326", &[&degenerate, &point], |metric| {
        let parts = degenerate.geodesic_parts(metric)?;
        assert!(parts.segments.is_empty());
        assert_eq!(parts.point_only.len(), 1);
        assert_eq!(
            geodesic_distance_with_parts(
                &degenerate,
                &parts,
                &point,
                &point.geodesic_parts(metric)?,
                metric,
            )?,
            0.0
        );
        Ok(())
    })?;
    Ok(())
}

#[test]
fn geodesic_contact_consumes_prepared_parts_without_reconstruction() -> Result<()> {
    let first_shape = line(vec![p(0.0, 0.0), p(10.0, 0.0)]);
    let second_shape = line(vec![p(5.0, -1.0), p(5.0, 1.0)]);
    with_ellipsoid_metric("EPSG:4326", &[&first_shape, &second_shape], |metric| {
        geodesic_counters::reset();
        let first = first_shape.geodesic_parts(metric)?;
        let second = second_shape.geodesic_parts(metric)?;
        let admissions = geodesic_counters::snapshot().segment_admissions;
        assert_eq!(admissions, 2);
        assert!(matches!(
            metric.segment_contact(&first.segments[0], &second.segments[0])?,
            crate::geometry::SegmentContact::Contact { .. }
        ));
        assert_eq!(geodesic_counters::snapshot().segment_admissions, admissions);
        Ok(())
    })?;
    Ok(())
}

#[test]
fn geodesic_dwithin_short_circuits_before_full_distance_sweep() -> Result<()> {
    let probes = multipoint(vec![
        p(0.0, 0.500_001),
        p(0.0, 0.500_002),
        p(0.0, 0.500_003),
    ]);
    let target = line(vec![p(-1.0, 0.0), p(1.0, 1.0)]);
    with_ellipsoid_metric("EPSG:4326", &[&probes, &target], |metric| {
        let probe_parts = probes.geodesic_parts(metric)?;
        let target_parts = target.geodesic_parts(metric)?;

        geodesic_counters::reset();
        assert!(geodesic_dwithin_with_parts(
            &probes,
            &probe_parts,
            &target,
            &target_parts,
            metric,
            200.0,
        )?);
        let threshold_work = geodesic_counters::snapshot();

        geodesic_counters::reset();
        let _ =
            geodesic_distance_with_parts(&probes, &probe_parts, &target, &target_parts, metric)?;
        let distance_work = geodesic_counters::snapshot();

        let work = |stats: &geodesic_counters::Snapshot| {
            stats.segment_value_evaluations
                + stats.segment_derivative_evaluations
                + stats.segment_interval_pops
                + stats.segment_interval_splits
                + stats.segment_convex_closures
                + stats.segment_derivative_declines
        };
        assert!(work(&threshold_work) < work(&distance_work));
        Ok(())
    })?;
    Ok(())
}

#[test]
#[expect(
    clippy::needless_borrows_for_generic_args,
    reason = "the test retries one closure across every allocator prefix"
)]
fn geodesic_ordered_rows_reservation_is_fallible_and_retryable() -> Result<()> {
    use crate::test_support::{allocation_count, fail_allocations_after};

    let target = line(vec![p(0.0, 0.0), p(1.0, 0.0)]);
    let vertices = [p(2.0, 0.0), p(3.0, 0.0), p(4.0, 0.0)];
    with_ellipsoid_metric("EPSG:4326", &[&target], |metric| {
        let parts = target.geodesic_parts(metric)?;
        let caps = parts
            .caps
            .as_ref()
            .expect("non-empty target has caps")
            .view();
        let run = || -> Result<Vec<_>> {
            let mut rows = Vec::new();
            geodesic_ordered_rows_into(&vertices, caps, metric, false, &mut rows)?;
            Ok(rows)
        };
        let expected = run()?;
        let (_, allocations) = allocation_count(&run);
        assert!(
            allocations > 0,
            "ordered rows did not reserve output storage"
        );
        for allowed in 0..allocations {
            let result = fail_allocations_after(allowed, &run);
            let error = result
                .err()
                .unwrap_or_else(|| panic!("ordered-row prefix {allowed} unexpectedly succeeded"));
            assert!(matches!(
                error.kind(),
                ErrorKind::Resource(ResourceError::Allocation)
            ));
            assert_eq!(run()?, expected, "ordered-row retry at {allowed}");
        }
        Ok(())
    })?;
    Ok(())
}

#[test]
#[expect(
    clippy::needless_borrows_for_generic_args,
    reason = "the test retries one closure across every allocator prefix"
)]
fn scalar_geodesic_dwithin_allocations_are_resource_and_retryable() -> Result<()> {
    use crate::test_support::{allocation_count, fail_allocations_after};

    let left_shape = line(vec![p(-2.0, 0.0), p(2.0, 0.0)]);
    let right_shape = line(vec![p(-2.0, 1.0), p(2.0, 1.0)]);
    with_ellipsoid_metric("EPSG:4326", &[&left_shape, &right_shape], |metric| {
        let left = PreparedPlan::new(left_shape.clone())?;
        let right = PreparedPlan::new(right_shape.clone())?;
        let (semi_major, flattening) = metric.ellipsoid_parameters();
        let run = || {
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
                100_000.0,
            )
        };
        let expected = run()?;
        let (_, allocations) = allocation_count(&run);
        assert!(allocations > 0, "scalar dwithin had no fallible setup");
        for allowed in 0..allocations {
            let result = fail_allocations_after(allowed, &run);
            let error = result
                .err()
                .unwrap_or_else(|| panic!("scalar prefix {allowed} unexpectedly succeeded"));
            assert!(matches!(
                error.kind(),
                ErrorKind::Resource(ResourceError::Allocation)
            ));
            assert_eq!(run()?, expected, "scalar retry at {allowed}");
        }

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
        assert!(infinite()?);
        let (_, infinite_allocations) = allocation_count(&infinite);
        assert_eq!(
            infinite_allocations, 0,
            "scalar positive-infinity dwithin should not allocate"
        );
        Ok(())
    })?;
    Ok(())
}

#[test]
fn geodesic_parts_collapse_periodic_zero_edge_support() -> Result<()> {
    let seam = line(vec![p(-180.0, 0.0), p(180.0, 0.0)]);
    let meridian = line(vec![p(-180.0, -1.0), p(-180.0, 1.0)]);
    with_ellipsoid_metric("EPSG:4326", &[&seam, &meridian], |metric| {
        assert_eq!(live_geodesic_distance(&seam, &meridian, metric)?, 0.0);
        Ok(())
    })?;
    Ok(())
}

#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "the test returns Result for fallible setup and uses normal assertion macros for its oracle"
)]
fn geodesic_parts_returns_resource_on_allocation_failure() -> Result<()> {
    use crate::error::{ErrorKind, ResourceError};
    use crate::test_support::fail_allocations_after;

    let shape = line(vec![p(0.0, 0.0), p(1.0, 0.0)]);
    with_ellipsoid_metric("EPSG:4326", &[&shape], |metric| {
        match fail_allocations_after(0, || shape.geodesic_parts(metric)) {
            Err(error) => assert!(matches!(
                error.kind(),
                ErrorKind::Resource(ResourceError::Allocation)
            )),
            Ok(_) => panic!("expected allocation failure"),
        }
        Ok(())
    })
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
        let source_points = source.points_vec().unwrap();
        let target_points = target.points_vec().unwrap();
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
        assert!(target_parts.geodesic_bvh(metric)?.is_some());
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
    reason = "the test returns Result for metric and part setup and asserts the injected allocation failure"
)]
fn geodesic_hausdorff_outer_target_range_allocation_is_resource() -> Result<()> {
    use crate::test_support::fail_allocations_after;

    let source = line(vec![p(-1.0, 0.0), p(1.0, 0.0)]);
    let target = line(vec![p(-1.0, 1.0), p(1.0, 1.0)]);
    with_ellipsoid_metric("EPSG:4326", &[&source, &target], |metric| {
        let source_parts = source.geodesic_parts(metric)?;
        let target_parts = target.geodesic_parts(metric)?;
        let mut scratch = geodesic_scratch();
        let result = fail_allocations_after(0, || {
            geodesic_directed_hausdorff_distance(&source_parts, &target_parts, metric, &mut scratch)
        });
        let Err(error) = result else {
            panic!("outer target preparation allocation failure was swallowed");
        };
        assert!(matches!(
            error.kind(),
            ErrorKind::Resource(ResourceError::Allocation)
        ));
        Ok(())
    })?;
    Ok(())
}

#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "the test returns Result for metric and part setup and asserts each injected allocation failure"
)]
fn geodesic_hausdorff_frontier_allocations_are_resource() -> Result<()> {
    use crate::test_support::{allocation_count, fail_allocations_after};

    let source = line(vec![p(0.0, 0.0), p(10.0, 0.0)]);
    let target = line(vec![p(0.0, 1.0), p(5.0, 8.0), p(10.0, 1.0)]);
    with_ellipsoid_metric("EPSG:4326", &[&source, &target], |metric| {
        let source_parts = source.geodesic_parts(metric)?;
        let target_parts = target.geodesic_parts(metric)?;
        let mut warm_scratch = GeodesicScratch::default();
        geodesic_directed_hausdorff_distance(
            &source_parts,
            &target_parts,
            metric,
            &mut warm_scratch,
        )?;
        let (_, allocations) = allocation_count(|| {
            let mut scratch = GeodesicScratch::default();
            geodesic_directed_hausdorff_distance(&source_parts, &target_parts, metric, &mut scratch)
        });
        assert!(
            allocations > 1,
            "frontier fixture allocations: {allocations}"
        );
        for allowed in 0..allocations {
            let mut scratch = GeodesicScratch::default();
            let result = fail_allocations_after(allowed, || {
                geodesic_directed_hausdorff_distance(
                    &source_parts,
                    &target_parts,
                    metric,
                    &mut scratch,
                )
            });
            let Err(error) = result else {
                panic!("allocation prefix {allowed} unexpectedly succeeded");
            };
            assert!(matches!(
                error.kind(),
                ErrorKind::Resource(ResourceError::Allocation)
            ));
        }
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
#[expect(
    clippy::panic_in_result_fn,
    reason = "the test returns Result for fallible setup and uses normal assertion macros for its oracle"
)]
fn geodesic_facet_bvh_matches_cap_sweep_oracle() -> Result<()> {
    for (name, probes, target, expect_bvh) in geodesic_bvh_oracle_cases() {
        with_ellipsoid_metric("EPSG:4326", &[&probes, &target], |metric| {
            let target_parts = target.geodesic_parts(metric)?;
            assert_eq!(
                target_parts.geodesic_bvh(metric)?.is_some(),
                expect_bvh,
                "{name} bvh presence"
            );
            let vertices = probes.points_vec().unwrap();
            let mut rows = Vec::new();
            let mut segment_scratch = GeodesicSegmentScratch::default();
            let expected_distance = geodesic_capped_sweep_with_parts(
                &vertices,
                &target_parts,
                metric,
                f64::INFINITY,
                &mut rows,
                &mut segment_scratch,
            )?;
            let actual_distance =
                geodesic_sweep_with_parts(&vertices, &target_parts, metric, f64::INFINITY)?;
            assert_geodesic_close(
                &format!("{name} distance"),
                actual_distance,
                expected_distance,
            );

            for limit in [
                0.0,
                expected_distance * 0.5,
                expected_distance,
                f64::next_up(expected_distance),
            ] {
                let expected = geodesic_capped_dwithin_with_parts(
                    &vertices,
                    &target_parts,
                    metric,
                    limit,
                    &mut rows,
                    &mut segment_scratch,
                )?;
                let actual =
                    geodesic_dwithin_sweep_with_parts(&vertices, &target_parts, metric, limit)?;
                assert_eq!(actual, expected, "{name} dwithin {limit}");
            }

            let expected_witness = geodesic_capped_witness_sweep_with_parts(
                &vertices,
                &target_parts,
                metric,
                None,
                0,
                false,
                &mut rows,
                &mut segment_scratch,
            )?;
            let actual_witness = geodesic_witness_sweep_with_parts(
                &vertices,
                &target_parts,
                metric,
                None,
                0,
                false,
            )?;
            assert_same_witness(name, actual_witness, expected_witness);
            Ok(())
        })?;
    }
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
#[expect(
    clippy::panic_in_result_fn,
    reason = "the test returns Result for fallible setup and uses normal assertion macros for its oracle"
)]
fn geodesic_bvh_coalesces_xy_across_ordinates_without_losing_witnesses() -> Result<()> {
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
                target_parts.geodesic_bvh(metric)?.is_some(),
                "{name} bvh presence"
            );
            let vertices = [probe];
            let mut rows = Vec::new();
            let mut segment_scratch = GeodesicSegmentScratch::default();
            let expected_distance = geodesic_capped_sweep_with_parts(
                &vertices,
                &target_parts,
                metric,
                f64::INFINITY,
                &mut rows,
                &mut segment_scratch,
            )?;
            let actual_distance =
                geodesic_sweep_with_parts(&vertices, &target_parts, metric, f64::INFINITY)?;
            assert_geodesic_close(
                &format!("{name} distance"),
                actual_distance,
                expected_distance,
            );

            for limit in [
                0.0,
                expected_distance * 0.5,
                expected_distance,
                f64::next_up(expected_distance),
            ] {
                let expected = geodesic_capped_dwithin_with_parts(
                    &vertices,
                    &target_parts,
                    metric,
                    limit,
                    &mut rows,
                    &mut segment_scratch,
                )?;
                let actual =
                    geodesic_dwithin_sweep_with_parts(&vertices, &target_parts, metric, limit)?;
                assert_eq!(actual, expected, "{name} dwithin {limit}");
            }

            let expected_witness = geodesic_capped_witness_sweep_with_parts(
                &vertices,
                &target_parts,
                metric,
                None,
                0,
                false,
                &mut rows,
                &mut segment_scratch,
            )?;
            let actual_witness = geodesic_witness_sweep_with_parts(
                &vertices,
                &target_parts,
                metric,
                None,
                0,
                false,
            )?;
            assert_same_witness(name, actual_witness, expected_witness);
            let target = actual_witness.expect("non-empty witness").target;
            assert_eq!(target.z(), expected_z, "{name} target Z");
            assert_eq!(target.m(), expected_m, "{name} target M");
        }
        Ok(())
    })?;
    Ok(())
}

#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "the test returns Result for metric setup and uses assertions for fault outcomes"
)]
fn geodesic_bvh_allocation_failures_are_retryable() -> Result<()> {
    use crate::test_support::{allocation_count, fail_allocations_after};

    let target = disconnected_bvh_target();
    with_ellipsoid_metric("EPSG:4326", &[&target], |metric| {
        let template = target.geodesic_parts(metric)?;
        let warm = fresh_bvh_parts(&template);
        warm.geodesic_bvh(metric)?;
        let counted = fresh_bvh_parts(&template);
        let (_, allocations) = allocation_count(|| counted.geodesic_bvh(metric));
        assert!(allocations > 0);

        for prefix in 0..allocations {
            let parts = fresh_bvh_parts(&template);
            let result = fail_allocations_after(prefix, || parts.geodesic_bvh(metric));
            let error = result
                .err()
                .unwrap_or_else(|| panic!("allocation prefix {prefix} unexpectedly succeeded"));
            assert!(matches!(
                error.kind(),
                ErrorKind::Resource(ResourceError::Allocation)
            ));
            assert!(parts.facet_bvh.get().is_none());
            assert!(parts.geodesic_bvh(metric)?.is_some());
        }
        Ok(())
    })?;
    Ok(())
}

#[test]
fn geodesic_bvh_none_remains_for_empty_and_below_threshold_parts() -> Result<()> {
    use crate::test_support::fail_allocations_after;

    let small = line(vec![p(0.0, 0.0), p(1.0, 0.0)]);
    let empty = Shape::empty_point();
    with_ellipsoid_metric("EPSG:4326", &[&small, &empty], |metric| {
        let small_parts = small.geodesic_parts(metric)?;
        let small_result = fail_allocations_after(0, || small_parts.geodesic_bvh(metric));
        assert!(matches!(small_result, Ok(None)));

        let empty_parts = empty.geodesic_parts(metric)?;
        assert!(matches!(empty_parts.geodesic_bvh(metric), Ok(None)));
        Ok(())
    })?;
    Ok(())
}

#[test]
#[expect(
    clippy::needless_borrows_for_generic_args,
    reason = "the test retries one closure across every allocator prefix"
)]
fn fixed_geodesic_point_rows_propagate_allocation_failures_and_retry() -> Result<()> {
    use crate::test_support::{allocation_count, fail_allocations_after};

    let cases = [
        ("non_bvh", line(vec![p(-1.0, 0.0), p(1.0, 0.0)]), 1.0),
        ("bvh", disconnected_bvh_target(), 1.0),
    ];
    for (name, target, limit) in cases {
        with_ellipsoid_metric("EPSG:4326", &[&target], |metric| {
            let data = PreparedPlan::new(target.clone())?;
            let frame_cache = FrameDependentCaches::default();
            let (semi_major, flattening) = metric.ellipsoid_parameters();
            let xs = [0.0, 10.0];
            let ys = [0.0, 0.0];
            let run = || {
                data.geodesic_dwithin_points(
                    &frame_cache,
                    std::iter::zip(xs.iter().copied(), ys.iter().copied()),
                    "EPSG:4326",
                    semi_major,
                    flattening,
                    metric,
                    limit,
                    metric.frame(),
                )
            };
            let expected = run()?;
            let (_, allocations) = allocation_count(&run);
            assert!(
                allocations > 0,
                "{name}: point-row output was not allocated"
            );
            for allowed in 0..allocations {
                let result = fail_allocations_after(allowed, &run);
                let error = result
                    .err()
                    .unwrap_or_else(|| panic!("{name}: prefix {allowed} unexpectedly succeeded"));
                assert!(matches!(
                    error.kind(),
                    ErrorKind::Resource(ResourceError::Allocation)
                ));
                assert_eq!(run()?, expected, "{name}: retry at {allowed}");
            }

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
            let inf_expected = inf_run()?;
            let (_, inf_allocations) = allocation_count(&inf_run);
            assert!(
                inf_allocations > 0,
                "{name}: infinite point-row output was not allocated"
            );
            for allowed in 0..inf_allocations {
                let result = fail_allocations_after(allowed, &inf_run);
                let error = result.err().unwrap_or_else(|| {
                    panic!("{name}: infinite prefix {allowed} unexpectedly succeeded")
                });
                assert!(matches!(
                    error.kind(),
                    ErrorKind::Resource(ResourceError::Allocation)
                ));
                assert_eq!(
                    inf_run()?,
                    inf_expected,
                    "{name}: infinite retry at {allowed}"
                );
            }
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

#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "the test returns Result for metric setup and uses assertions for fault outcomes"
)]
fn geodesic_sweep_propagates_bvh_allocation_failure() -> Result<()> {
    use crate::test_support::fail_allocations_after;

    let target = disconnected_bvh_target();
    let probe = [p(0.0, 0.0)];
    let small = line(vec![p(-1.0, 0.0), p(1.0, 0.0)]);
    with_ellipsoid_metric("EPSG:4326", &[&target, &small], |metric| {
        let small_parts = small.geodesic_parts(metric)?;
        geodesic_sweep_with_parts(&probe, &small_parts, metric, f64::INFINITY)?;

        let template = target.geodesic_parts(metric)?;
        let parts = fresh_bvh_parts(&template);
        let result = fail_allocations_after(0, || {
            geodesic_sweep_with_parts(&probe, &parts, metric, f64::INFINITY)
        });
        let Err(error) = result else {
            panic!("BVH allocation failure was swallowed by capped fallback");
        };
        assert!(matches!(
            error.kind(),
            ErrorKind::Resource(ResourceError::Allocation)
        ));
        assert!(parts.facet_bvh.get().is_none());
        Ok(())
    })?;
    Ok(())
}

#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "the test returns Result for metric and part setup and asserts the injected stack allocation failure"
)]
fn geodesic_bvh_witness_stack_allocation_is_resource() -> Result<()> {
    use crate::test_support::fail_allocations_after;

    let target = disconnected_bvh_target();
    with_ellipsoid_metric("EPSG:4326", &[&target], |metric| {
        let template = target.geodesic_parts(metric)?;
        let parts = fresh_bvh_parts(&template);
        assert!(parts.geodesic_bvh(metric)?.is_some());
        let mut stack = Vec::new();
        let mut segment_scratch = GeodesicSegmentScratch::default();
        let result = fail_allocations_after(0, || {
            geodesic_target_witness(
                p(0.0, 0.0),
                &parts,
                metric,
                &mut stack,
                &mut segment_scratch,
            )
        });
        let Err(error) = result else {
            panic!("BVH witness stack allocation failure was swallowed");
        };
        assert!(matches!(
            error.kind(),
            ErrorKind::Resource(ResourceError::Allocation)
        ));
        Ok(())
    })?;
    Ok(())
}

#[test]
fn geodesic_distance_matches_brute_scalar_oracle() -> Result<()> {
    for (name, left, right) in geodesic_distance_oracle_cases() {
        with_ellipsoid_metric("EPSG:4326", &[&left, &right], |metric| {
            let expected = brute_geodesic_distance(&left, &right, metric);
            let actual = live_geodesic_distance(&left, &right, metric)?;
            assert_geodesic_close(name, actual, expected);
            Ok(())
        })?;
    }
    Ok(())
}

#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "the test returns Result for fallible setup and uses normal assertion macros for its oracle"
)]
fn cached_geodesic_parts_match_shape_kernels() -> Result<()> {
    let mut cases = geodesic_distance_oracle_cases();
    cases.extend([
        (
            "empty_left",
            Shape::empty_point(),
            Shape::Point(p(1.0, 1.0)),
        ),
        (
            "boundary_inclusive_zero",
            line(vec![p(-1.0, 0.0), p(1.0, 0.0)]),
            Shape::Point(p(0.0, 0.0)),
        ),
    ]);
    for (name, left, right) in cases {
        with_ellipsoid_metric("EPSG:4326", &[&left, &right], |metric| {
            let (semi_major, flattening) = metric.ellipsoid_parameters();
            let left_data = PreparedPlan::new(left.clone()).unwrap();
            let right_data = PreparedPlan::new(right.clone()).unwrap();
            let left_cache = FrameDependentCaches::default();
            let right_cache = FrameDependentCaches::default();
            let expected_distance = live_geodesic_distance(&left, &right, metric)?;
            let actual_distance = left_data.geodesic_distance_cached(
                &left_cache,
                &right_data,
                &right_cache,
                "EPSG:4326",
                semi_major,
                flattening,
                metric,
            )?;
            assert_geodesic_close(
                &format!("{name} distance"),
                actual_distance,
                expected_distance,
            );
            let limits = if expected_distance.is_finite() {
                [
                    0.0,
                    expected_distance,
                    f64::next_up(expected_distance),
                    expected_distance * 0.5,
                ]
            } else {
                [0.0, 1.0, 1_000_000.0, f64::MAX]
            };
            for limit in limits {
                let expected = expected_distance <= limit;
                let actual = left_data.geodesic_dwithin_cached(
                    &left_cache,
                    &right_data,
                    &right_cache,
                    "EPSG:4326",
                    semi_major,
                    flattening,
                    metric,
                    limit,
                )?;
                assert_eq!(actual, expected, "{name} dwithin {limit}");
            }
            let expected_nearest = live_geodesic_nearest_points(&left, &right, metric)?;
            let actual_nearest = left_data.geodesic_nearest_points_cached(
                &left_cache,
                &right_data,
                &right_cache,
                "EPSG:4326",
                semi_major,
                flattening,
                metric,
            )?;
            assert_eq!(actual_nearest, expected_nearest, "{name} nearest");
            Ok(())
        })?;
    }
    Ok(())
}

#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "the test returns Result for fallible setup and uses normal assertion macros for its oracle"
)]
fn geodesic_nearest_points_matches_brute_ordered_witnesses() -> Result<()> {
    for (name, left, right) in geodesic_nearest_oracle_cases() {
        with_ellipsoid_metric("EPSG:4326", &[&left, &right], |metric| {
            let expected = brute_geodesic_nearest_points(&left, &right, metric);
            let actual = live_geodesic_nearest_points(&left, &right, metric)?;
            assert_eq!(actual, expected, "{name}");
            Ok(())
        })?;
    }
    Ok(())
}

#[test]
fn geodesic_nearest_points_returns_crossing_witness_on_ellipsoid() -> Result<()> {
    let left = line(vec![p(-20.0, -10.0), p(30.0, 20.0)]);
    let right = line(vec![p(-15.0, 20.0), p(25.0, -10.0)]);
    with_ellipsoid_metric("EPSG:4326", &[&left, &right], |metric| {
        let (left_point, right_point) =
            live_geodesic_nearest_points(&left, &right, metric)?.expect("non-empty crossing lines");
        let distance = metric.segment_length(left_point, right_point);
        assert!(distance <= 1e-3, "crossing witness distance: {distance}");
        Ok(())
    })?;
    Ok(())
}

#[test]
fn geodesic_filled_area_operations_are_explicitly_unsupported() -> Result<()> {
    let area = polygon(vec![
        p(-1.0, -1.0),
        p(1.0, -1.0),
        p(0.0, 1.0),
        p(-1.0, -1.0),
    ]);
    let point = Shape::Point(p(0.0, 0.0));
    with_ellipsoid_metric("EPSG:4326", &[&area, &point], |metric| {
        area.geodesic_cap(metric).unwrap_err();
        live_geodesic_distance(&area, &point, metric).unwrap_err();
        area.geodesic_hausdorff_distance(&point, metric)
            .unwrap_err();
        live_geodesic_nearest_points(&area, &point, metric).unwrap_err();
        Ok(())
    })?;
    Ok(())
}

#[test]
fn planar_contact_does_not_make_geodesic_distance_zero() -> Result<()> {
    let upper = line(vec![p(-80.0, 80.0), p(80.0, 80.0)]);
    let meridian = line(vec![p(0.0, 70.0), p(0.0, 80.0)]);
    with_ellipsoid_metric("EPSG:4326", &[&upper, &meridian], |metric| {
        let distance = live_geodesic_distance(&upper, &meridian, metric)?;
        assert!(distance > 1.0, "planar contact became geodesic zero");
        Ok(())
    })?;
    Ok(())
}

#[test]
fn planar_false_zero_fixture_b_230_cases_are_positive() -> Result<()> {
    let latitudes = [
        89.50, 89.52, 89.54, 89.56, 89.58, 89.60, 89.62, 89.64, 89.66, 89.68, 89.70, 89.72, 89.74,
        89.76, 89.78, 89.80, 89.82, 89.84, 89.86, 89.88, 89.90, 89.92, 89.94,
    ];
    let longitudes = [
        0.0001, 0.0002, 0.0003, 0.0004, 0.0005, 0.0006, 0.0007, 0.0008, 0.0009, 0.0010,
    ];
    assert_eq!(latitudes.len() * longitudes.len(), 230);
    with_ellipsoid_metric("EPSG:4326", &[], |metric| {
        for &latitude in &latitudes {
            for &longitude in &longitudes {
                let upper = line(vec![p(-longitude, latitude), p(longitude, latitude)]);
                let meridian = line(vec![p(0.0, latitude - 10.0), p(0.0, latitude)]);
                let distance = live_geodesic_distance(&upper, &meridian, metric)?;
                assert!(
                    distance > 0.0,
                    "planar false-zero corpus case lon={longitude} lat={latitude}: {distance}"
                );
            }
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
            let reach = previous[index].min(previous[index - 1]).min(left);
            left = (dx * dx + dy * dy).max(reach);
            current[index] = left;
        }
        std::mem::swap(&mut previous, &mut current);
    }
    previous[width - 1].sqrt()
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

#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "the test returns Result for fallible setup and uses normal assertion macros for its oracle"
)]
fn fused_geodesic_cap_and_parts_agree() -> Result<()> {
    let cases = [
        ("line", line(vec![p(-1.0, 0.0), p(1.0, 0.0)])),
        ("multipoint", multipoint(vec![p(0.0, 0.0), p(1.0, 1.0)])),
    ];
    for (name, shape) in cases {
        with_ellipsoid_metric("EPSG:4326", &[&shape], |metric| {
            let streamed = shape.geodesic_cap(metric)?;
            let parts = shape.geodesic_parts(metric)?;
            assert_eq!(
                streamed,
                geodesic_cap_from_parts(&parts, metric),
                "{name}: streamed cap vs parts cap"
            );
            let (semi_major, flattening) = metric.ellipsoid_parameters();
            let data = PreparedPlan::new(shape.clone()).unwrap();
            let frame_cache = FrameDependentCaches::default();
            let runtime_state = crate::crs::runtime_state_snapshot()?;
            data.prepare_geodesic_parts(&frame_cache, "EPSG:4326", semi_major, flattening, metric)?;
            assert_eq!(
                streamed,
                data.geodesic_cap_cached(
                    &frame_cache,
                    "EPSG:4326",
                    semi_major,
                    flattening,
                    metric,
                    &runtime_state,
                )?,
                "{name}: cached cap"
            );
            Ok(())
        })?;
    }
    Ok(())
}
