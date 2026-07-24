use super::hausdorff::stats;
use super::*;

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
