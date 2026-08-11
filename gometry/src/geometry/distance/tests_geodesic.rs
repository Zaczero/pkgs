use crate::crs::with_ellipsoid_metric;
use crate::error::Result;
use crate::geometry::distance::frechet::frechet_dp_columns;
use crate::geometry::distance::geodesic_parts::{
    geodesic_cap_from_parts, geodesic_capped_dwithin_with_parts, geodesic_capped_sweep_with_parts,
    geodesic_dwithin_sweep_with_parts, geodesic_sweep_with_parts,
};
use crate::geometry::distance::geodesic_sweep::geodesic_capped_witness_sweep_with_parts;
use crate::geometry::distance::tests::{
    assert_geodesic_close, assert_same_witness, brute_geodesic_distance,
    brute_geodesic_nearest_points, dense_antimeridian_polygon, dense_polygon,
    geodesic_bvh_oracle_cases, geodesic_distance_oracle_cases, geodesic_nearest_oracle_cases, line,
    multipoint, p, polygon,
};
use crate::geometry::distance::{
    GeodesicSweepCaps, collect_geodesic_segments_into, geodesic_min_distance_to_target,
    geodesic_sweep_caps_into, geodesic_witness_sweep_with_parts,
};
use crate::geometry::{FrameDependentCaches, GeodesicMetric, Shape, ShapeData};

fn brute_geodesic_directed_hausdorff(
    source: &Shape,
    target: &Shape,
    metric: &impl GeodesicMetric,
) -> f64 {
    let mut target_edges = Vec::new();
    collect_geodesic_segments_into(target, metric, &mut target_edges);
    let mut target_points = Vec::new();
    target.collect_points_into(&mut target_points);
    let mut cmax = 0.0_f64;
    source.for_each_point(|point| {
        let cmin =
            geodesic_min_distance_to_target(point, &target_edges, &target_points, metric, cmax);
        if cmin > cmax {
            cmax = cmin;
        }
    });
    source.for_each_vertex_pair(|start, end| {
        let steps = 128_usize;
        for step in 0..=steps {
            let point = metric.interpolate(start, end, step as f64 / steps as f64);
            let cmin =
                geodesic_min_distance_to_target(point, &target_edges, &target_points, metric, cmax);
            if cmin > cmax {
                cmax = cmin;
            }
        }
    });
    cmax
}

#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "the test returns Result for fallible setup and uses normal assertion macros for its oracle"
)]
fn geodesic_hausdorff_is_segment_aware() -> Result<()> {
    let peak = line(vec![p(0.0, 0.0), p(10.0, 0.0)]);
    let far = line(vec![p(0.0, 1.0), p(5.0, 8.0), p(10.0, 1.0)]);
    with_ellipsoid_metric("EPSG:4326", &[&peak, &far], |metric| {
        let actual = peak.geodesic_hausdorff_distance(&far, metric);
        let expected = brute_geodesic_directed_hausdorff(&peak, &far, metric)
            .max(brute_geodesic_directed_hausdorff(&far, &peak, metric));
        assert_geodesic_close("interior_peak", actual, expected);
        assert!(actual > 500_000.0);
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
                target_parts.geodesic_bvh(metric).is_some(),
                expect_bvh,
                "{name} bvh presence"
            );
            let vertices = probes.points_vec();
            let mut rows = Vec::new();
            let expected_distance = geodesic_capped_sweep_with_parts(
                &target,
                &vertices,
                &target_parts,
                metric,
                f64::INFINITY,
                &mut rows,
            );
            let actual_distance =
                geodesic_sweep_with_parts(&target, &vertices, &target_parts, metric, f64::INFINITY);
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
                    &target,
                    &vertices,
                    &target_parts,
                    metric,
                    limit,
                    &mut rows,
                );
                let actual = geodesic_dwithin_sweep_with_parts(
                    &target,
                    &vertices,
                    &target_parts,
                    metric,
                    limit,
                );
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
            );
            let actual_witness =
                geodesic_witness_sweep_with_parts(&vertices, &target_parts, metric, None, 0, false);
            assert_same_witness(name, actual_witness, expected_witness);
            Ok(())
        })?;
    }
    Ok(())
}

#[test]
fn geodesic_distance_matches_brute_scalar_oracle() -> Result<()> {
    for (name, left, right) in geodesic_distance_oracle_cases() {
        with_ellipsoid_metric("EPSG:4326", &[&left, &right], |metric| {
            let expected = brute_geodesic_distance(&left, &right, metric);
            let actual = left.geodesic_distance(&right, metric);
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
        (
            "near_pole_polygon_line",
            polygon(vec![
                p(-20.0, 86.0),
                p(20.0, 86.0),
                p(0.0, 89.0),
                p(-20.0, 86.0),
            ]),
            line(vec![p(-40.0, 87.5), p(40.0, 87.5)]),
        ),
    ]);
    for (name, left, right) in cases {
        with_ellipsoid_metric("EPSG:4326", &[&left, &right], |metric| {
            let (semi_major, flattening) = metric.ellipsoid_parameters();
            let left_data = ShapeData::new(left.clone());
            let right_data = ShapeData::new(right.clone());
            let left_cache = FrameDependentCaches::default();
            let right_cache = FrameDependentCaches::default();
            let expected_distance = left.geodesic_distance(&right, metric);
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
            let expected_nearest = left.geodesic_nearest_points(&right, metric);
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
            let actual = left.geodesic_nearest_points(&right, metric);
            assert_eq!(actual, expected, "{name}");
            Ok(())
        })?;
    }
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
        ("polygon", dense_polygon(0.0, 0.0, 5.0, 12)),
        ("antimeridian", dense_antimeridian_polygon()),
    ];
    for (name, shape) in cases {
        with_ellipsoid_metric("EPSG:4326", &[&shape], |metric| {
            let streamed = shape.geodesic_cap(metric);
            let parts = shape.geodesic_parts(metric)?;
            assert_eq!(
                streamed,
                geodesic_cap_from_parts(&parts, metric),
                "{name}: streamed cap vs parts cap"
            );
            let mut cap_lengths = Vec::new();
            let mut cap_groups = Vec::new();
            let rebuilt = geodesic_sweep_caps_into(
                &parts.segments,
                &parts.point_only,
                metric,
                &mut cap_lengths,
                &mut cap_groups,
            )
            .map(|view| (view.anchor, view.global_reach));
            let rebuilt = rebuilt.map(|(anchor, global_reach)| GeodesicSweepCaps {
                anchor,
                global_reach,
                lengths: cap_lengths,
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
                    assert_eq!(built.lengths, rebuilt.lengths, "{name}: cap lengths");
                    assert_eq!(built.groups, rebuilt.groups, "{name}: cap groups");
                },
                _ => panic!("{name}: fused sweep caps presence mismatch"),
            }
            let (semi_major, flattening) = metric.ellipsoid_parameters();
            let data = ShapeData::new(shape.clone());
            let frame_cache = FrameDependentCaches::default();
            data.prepare_geodesic_parts(&frame_cache, "EPSG:4326", semi_major, flattening, metric)?;
            assert_eq!(
                streamed,
                data.geodesic_cap_cached(&frame_cache, "EPSG:4326", semi_major, flattening, metric),
                "{name}: cached cap"
            );
            Ok(())
        })?;
    }
    Ok(())
}
