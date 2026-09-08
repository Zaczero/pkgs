#![allow(
    clippy::panic_in_result_fn,
    reason = "tests assert, and return Result only to propagate setup errors"
)]
use crate::crs::with_ellipsoid_metric;
use crate::error::{ErrorKind, ResourceError, Result};
use crate::geometry::distance::frechet::frechet_dp_columns;
use crate::geometry::distance::geodesic_parts::{
    geodesic_cap_from_parts, geodesic_dwithin_sweep_with_parts, geodesic_sweep_with_parts,
};
use crate::geometry::distance::geodesic_sweep::{
    geodesic_capped_witness_sweep_with_parts, geodesic_sweep_caps_into,
};
use crate::geometry::distance::tests::{
    assert_geodesic_close, assert_same_witness, brute_geodesic_distance,
    brute_geodesic_nearest_points, geodesic_bvh_oracle_cases, geodesic_distance_oracle_cases,
    geodesic_nearest_oracle_cases, line, multipoint, p, polygon,
};
use crate::geometry::distance::{
    GeodesicSweepCaps, collect_geodesic_segments_into, collect_point_only_into,
    geodesic_capped_dwithin_with_parts, geodesic_capped_sweep_with_parts,
    geodesic_min_distance_to_target, geodesic_witness_sweep_with_parts,
};
use crate::geometry::{
    FrameDependentCaches, GeodesicMetric, GeodesicParts, MOrdinate, Point, PreparedPlan, Shape,
    ZOrdinate,
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
                let longitude = index as f64 * 2.0;
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
    let mut target_edges = Vec::new();
    collect_geodesic_segments_into(target, metric, &mut target_edges)
        .expect("unique test segments");
    let mut target_points = Vec::new();
    target.collect_points_into(&mut target_points).unwrap();
    let mut cmax = 0.0_f64;
    source.for_each_point(|point| {
        let cmin =
            geodesic_min_distance_to_target(point, &target_edges, &target_points, metric, cmax)
                .expect("point-to-segment proof should close");
        if cmin > cmax {
            cmax = cmin;
        }
    });
    source.for_each_vertex_pair(|start, end| {
        for fraction in SAMPLE_FRACTIONS {
            let point = metric.interpolate(start, end, fraction);
            let cmin =
                geodesic_min_distance_to_target(point, &target_edges, &target_points, metric, cmax)
                    .expect("point-to-segment proof should close");
            if cmin > cmax {
                cmax = cmin;
            }
        }
    });
    cmax
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
        let mut source_edges = Vec::new();
        collect_geodesic_segments_into(&source, metric, &mut source_edges)?;
        let source_points = source.points_vec().unwrap();
        let mut target_edges = Vec::new();
        collect_geodesic_segments_into(&target, metric, &mut target_edges)?;
        let target_points = target.points_vec().unwrap();

        let source_to_target = super::shape_impl::geodesic_directed_hausdorff_distance(
            &source_points,
            &source_edges,
            &target_edges,
            &target_points,
            metric,
        )?;
        let source_vertices_only =
            brute_geodesic_directed_hausdorff(&multipoint(source_points), &target, metric);
        assert!(
            source_to_target > source_vertices_only,
            "source interior did not affect the directed maximum: {source_to_target} <= {source_vertices_only}"
        );

        let target_vertices_only =
            brute_geodesic_directed_hausdorff(&source, &multipoint(target_points), metric);
        assert!(
            source_to_target < target_vertices_only,
            "target interior did not affect the directed maximum: {source_to_target} >= {target_vertices_only}"
        );

        let expected_source_to_target = brute_geodesic_directed_hausdorff(&source, &target, metric);
        assert_geodesic_close(
            "source_to_target",
            source_to_target,
            expected_source_to_target,
        );

        let actual = source.geodesic_hausdorff_distance(&target, metric)?;
        let target_to_source = brute_geodesic_directed_hausdorff(&target, &source, metric);
        assert_geodesic_close("symmetric", actual, source_to_target.max(target_to_source));
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
            let expected_distance = geodesic_capped_sweep_with_parts(
                &vertices,
                &target_parts,
                metric,
                f64::INFINITY,
                &mut rows,
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
            let expected_distance = geodesic_capped_sweep_with_parts(
                &vertices,
                &target_parts,
                metric,
                f64::INFINITY,
                &mut rows,
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
        let error = result
            .err()
            .expect("BVH allocation failure was swallowed by capped fallback");
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
fn geodesic_distance_matches_brute_scalar_oracle() -> Result<()> {
    for (name, left, right) in geodesic_distance_oracle_cases() {
        with_ellipsoid_metric("EPSG:4326", &[&left, &right], |metric| {
            let expected = brute_geodesic_distance(&left, &right, metric);
            let actual = left.geodesic_distance(&right, metric)?;
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
            let expected_distance = left.geodesic_distance(&right, metric)?;
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
            let expected_nearest = left.geodesic_nearest_points(&right, metric)?;
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
            let actual = left.geodesic_nearest_points(&right, metric)?;
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
        let (left_point, right_point) = left
            .geodesic_nearest_points(&right, metric)?
            .expect("non-empty crossing lines");
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
        area.geodesic_distance(&point, metric).unwrap_err();
        area.geodesic_hausdorff_distance(&point, metric)
            .unwrap_err();
        area.geodesic_nearest_points(&point, metric).unwrap_err();
        Ok(())
    })?;
    Ok(())
}

#[test]
fn planar_contact_does_not_make_geodesic_distance_zero() -> Result<()> {
    let upper = line(vec![p(-80.0, 80.0), p(80.0, 80.0)]);
    let meridian = line(vec![p(0.0, 70.0), p(0.0, 80.0)]);
    with_ellipsoid_metric("EPSG:4326", &[&upper, &meridian], |metric| {
        let distance = upper.geodesic_distance(&meridian, metric)?;
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
                let distance = upper.geodesic_distance(&meridian, metric)?;
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
            let mut cap_groups = Vec::new();
            let rebuilt = geodesic_sweep_caps_into(
                &parts.segments,
                &parts.point_only,
                metric,
                &mut cap_groups,
            )
            .map(|view| (view.anchor, view.global_reach));
            let rebuilt = rebuilt.map(|(anchor, global_reach)| GeodesicSweepCaps {
                anchor,
                global_reach,
                groups: cap_groups,
            });
            match (&parts.caps, rebuilt) {
                (None, None) => {},
                (Some(built), Some(rebuilt)) => {
                    assert_eq!(built.anchor, rebuilt.anchor, "{name}: cap anchor");
                    assert!(
                        (built.global_reach - rebuilt.global_reach).abs() <= 1e-6,
                        "{name}: cap reach"
                    );
                    assert_eq!(built.groups, rebuilt.groups, "{name}: cap groups");
                },
                _ => panic!("{name}: fused sweep caps presence mismatch"),
            }
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
