use crate::geometry::distance::hausdorff::{max_point_to_target_squared_on_segment_culled, stats};
use crate::geometry::distance::*;
use crate::geometry::*;

#[test]
fn hausdorff_pruning_counters_on_wiggly_window() {
    stats::reset();
    let n = 40_usize;
    let mut left_xs = Vec::with_capacity(n);
    let mut left_ys = Vec::with_capacity(n);
    let mut right_xs = Vec::with_capacity(n);
    let mut right_ys = Vec::with_capacity(n);
    for index in 0..n {
        let angle = (index as f64) * std::f64::consts::TAU / (n as f64);
        left_xs.push(angle.cos() * 5.0);
        left_ys.push(angle.sin() * 5.0);
        right_xs.push(angle.cos() * 5.0 + 0.3);
        right_ys.push(angle.sin() * 5.0 + 0.3);
    }
    let _ = hausdorff_distance_squared_line_columns(&left_xs, &left_ys, &right_xs, &right_ys);
    let snapshot = stats::snapshot();
    eprintln!("hausdorff debug counters: {snapshot:?}");
    assert!(snapshot.vertex_probes >= n);
    assert!(
        snapshot.segment_bound_skips + snapshot.coverage_certificate_skips > 0,
        "expected pruning skips, got {snapshot:?}"
    );
    assert_eq!(snapshot.exact_segment_evals, 0);
}

fn point_target(points: &[(f64, f64)]) -> HausdorffTarget {
    HausdorffTarget::from_parts(
        &[],
        points
            .iter()
            .map(|&(x, y)| Point::new_unchecked_xy(x, y))
            .collect(),
        None,
    )
}

#[test]
fn point_target_envelope_route_is_observable() {
    let target = point_target(&[(0.0, 1.0), (1.0, 1.0), (2.0, 1.0)]);
    let source = Segment {
        start: XY::new(0.0, 0.0),
        end: XY::new(2.0, 0.0),
    };
    stats::reset();
    let _ = max_point_to_target_squared_on_segment_culled(source, &target, None);
    let snapshot = stats::snapshot();
    assert_eq!(snapshot.point_envelope_candidates, 3);
    assert_eq!(snapshot.point_envelope_breakpoints, 2);
    assert_eq!(snapshot.point_envelope_samples, 8);
}

#[test]
fn point_target_envelope_clamps_endpoint_switches_before_sampling() {
    // These two affine terms meet at t=0. The lower-envelope route must still
    // sample 0 and its clamped neighbours rather than treating the switch as
    // absent just because it is not strictly interior.
    let target = point_target(&[(0.0, 1.0), (0.6, 0.8)]);
    let source = Segment {
        start: XY::new(0.0, 0.0),
        end: XY::new(1.0, 0.0),
    };
    stats::reset();
    let fast = max_point_to_target_squared_on_segment_culled(source, &target, None);
    let snapshot = stats::snapshot();
    let legacy = max_point_to_target_squared_on_segment_culled_legacy_for_test(source, &target);
    assert_eq!(fast.to_bits(), legacy.to_bits());
    assert_eq!(snapshot.point_envelope_breakpoints, 1);
    assert_eq!(snapshot.point_envelope_samples, 5);
}

#[test]
fn point_target_envelope_hull_uses_adaptive_orientation_at_overflowing_dual_scale() {
    let unit: f64 = 1e200;
    let lines = [
        (3.0 * unit, 3.0 * unit),
        (2.0 * unit, 2.0 * unit),
        (unit, unit.next_up()),
    ];
    // The textbook cross product is inf - inf here, so it cannot classify the
    // turn. The shared adaptive predicate keeps all three active lines.
    let raw = (lines[1].0 - lines[0].0) * (lines[2].1 - lines[0].1)
        - (lines[1].1 - lines[0].1) * (lines[2].0 - lines[0].0);
    assert!(raw.is_nan());
    assert_eq!(
        super::hausdorff::point_envelope_dual_hull_len_for_test(&lines),
        3
    );
}

#[test]
fn point_target_envelope_has_linear_candidates_and_conservative_switch_samples() {
    for count in [100_usize, 200, 400, 800] {
        let source = Segment {
            start: XY::new(0.0, 0.0),
            end: XY::new((count - 1) as f64, 0.0),
        };
        let points: Vec<_> = (0..count).map(|index| (index as f64, 1.0)).collect();
        let target = point_target(&points);

        stats::reset();
        let _ = max_point_to_target_squared_on_segment_culled(source, &target, None);
        let snapshot = stats::snapshot();

        assert_eq!(snapshot.point_envelope_candidates, count, "count={count}");
        assert_eq!(
            snapshot.point_envelope_breakpoints,
            count - 1,
            "count={count}"
        );
        // Endpoints plus down/root/up for every interior hull switch. This
        // pins the conservative breakpoint rule without a wall-clock claim.
        assert_eq!(
            snapshot.point_envelope_samples,
            3 * count - 1,
            "count={count}"
        );
    }
}

#[test]
fn point_target_envelope_is_bitwise_identical_to_the_prior_continuous_kernel() {
    let cases = [
        (
            Segment {
                start: XY::new(0.0, 0.0),
                end: XY::new(8.0, 0.0),
            },
            &[
                (0.0, 1.0),
                (1.5, -2.0),
                (3.25, 0.5),
                (6.5, 3.0),
                (8.0, -1.0),
            ][..],
        ),
        (
            Segment {
                start: XY::new(-3.0, 2.0),
                end: XY::new(7.0, -5.0),
            },
            &[
                (-3.0, 6.0),
                (-1.0, -2.0),
                (2.0, 1.0),
                (5.0, -7.0),
                (8.0, 3.0),
            ][..],
        ),
        (
            Segment {
                start: XY::new(-4.0, -1.0),
                end: XY::new(5.0, 2.0),
            },
            &[
                (-4.0, -1.0),
                (-4.0, -1.0),
                (0.5, 0.5),
                (5.0, 2.0),
                (7.0, 4.0),
            ][..],
        ),
        (
            Segment {
                start: XY::new(0.0, 0.0),
                end: XY::new(1.0, 0.0),
            },
            &[(0.0, 1.0), (0.6, 0.8)][..],
        ),
    ];
    for (source, points) in cases {
        let target = point_target(points);
        let fast = max_point_to_target_squared_on_segment_culled(source, &target, None);
        let legacy = max_point_to_target_squared_on_segment_culled_legacy_for_test(source, &target);
        assert_eq!(
            fast.to_bits(),
            legacy.to_bits(),
            "source={source:?} points={points:?}"
        );
    }
}
