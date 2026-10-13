use super::super::HALF_PI_LO;
use super::*;

#[test]
fn ordered_arc_latitude_includes_an_interior_analytic_extremum() {
    // The plane is y=z.  Its north-axis projection is the interior ray
    // (0, 1, 1), above both unit endpoint rays (±1/3, 2/3, 2/3).
    let arc = OrderedMinorArc::from_test_points([1.0 / 3.0, 2.0 / 3.0, 2.0 / 3.0], [
        -1.0 / 3.0,
        2.0 / 3.0,
        2.0 / 3.0,
    ]);
    let endpoints = [
        certified_vector_latitude(arc.start).unwrap(),
        certified_vector_latitude(arc.end).unwrap(),
    ];
    let bounds = certified_arc_latitude_bounds(&arc).unwrap();
    assert!(bounds.hi > endpoints[0].hi.max(endpoints[1].hi));
    assert!(bounds.hi > HALF_PI_LO * 0.5);
}
