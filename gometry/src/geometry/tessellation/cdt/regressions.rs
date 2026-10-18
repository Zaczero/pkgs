use super::*;
use crate::geometry::tessellation::test_support::points;
include!("regressions/support.rs");

#[rstest::rstest]
#[case::exponent_22_i32(22_i32)]
#[case::exponent_154(154)]
#[case::exponent_162(162)]
fn reciprocal_pentagon_swapped_axes_is_three_faces(#[case] exponent: i32) {
    let (large, tiny) = (10_f64.powi(exponent), 10_f64.powi(exponent.wrapping_neg()));
    let base = [
        (-large, 0.0),
        (-0.5 * large, -tiny),
        (0.5 * large, -tiny),
        (large, 0.0),
        (0.0, tiny),
    ];
    let swapped: Vec<(f64, f64)> = base.iter().map(|&(x, y)| (y, x)).collect();
    let points = points(&swapped);
    let cdt = ConstrainedMesh::build(&points, &ring_edges(5)).expect("five corners");
    assert_eq!(inside_count(&cdt), 3, "exponent {exponent}");
}

#[test]
fn sliver_refines_to_the_angle_without_leaving_the_hull() {
    let mut points = points(&[(0.0, 0.0), (20.0, 0.0), (20.0, 0.25), (0.125, 0.0625)]);
    let mut cdt = ConstrainedMesh::build(&points, &ring_edges(4)).unwrap();
    let before = inside_count(&cdt);
    cdt.refine(&mut points, Some(25.0), None, 100_000).unwrap();
    assert!(inside_count(&cdt) > before);
}

#[rstest::rstest]
#[case::exponent_77_i32(77_i32)]
#[case::exponent_159(159)]
#[case::exponent_200(200)]
#[case::exponent_300(300)]
fn reciprocal_rectangle_max_area_splits_into_four(#[case] exponent: i32) {
    for swap in [false, true] {
        let (large, tiny) = (10_f64.powi(exponent), 10_f64.powi(exponent.wrapping_neg()));
        let mut base = vec![(0.0, 0.0), (large, 0.0), (large, tiny), (0.0, tiny)];
        if swap {
            base = base.iter().map(|&(x, y)| (y, x)).collect();
        }
        let mut points = points(&base);
        let mut cdt = ConstrainedMesh::build(&points, &ring_edges(4)).unwrap();
        cdt.refine(&mut points, None, Some(0.4), 100_000).unwrap();
        assert_eq!(inside_count(&cdt), 4, "exponent {exponent} swap {swap}");
    }
}
