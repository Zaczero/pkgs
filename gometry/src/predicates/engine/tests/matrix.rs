/// The pre-topology gates are wired into the array, index and coverer
/// lanes; the scalar lane went straight to the relation, so
/// `gm.intersects(a, b)` on operands that do not overlap still built exact
/// pair topology. The gate can only be an admission filter, never an
/// answer change, so this asserts it against the relation it short-circuits
/// — including the operands it does *not* gate.
#[rstest::rstest]
#[case::predicate_intersects(Predicate::Intersects)]
#[case::predicate_disjoint(Predicate::Disjoint)]
#[case::predicate_contains(Predicate::Contains)]
#[case::predicate_containsproperly(Predicate::ContainsProperly)]
#[case::predicate_within(Predicate::Within)]
#[case::predicate_covers(Predicate::Covers)]
#[case::predicate_coveredby(Predicate::CoveredBy)]
#[case::predicate_equals(Predicate::Equals)]
#[case::predicate_touches(Predicate::Touches)]
#[case::predicate_crosses(Predicate::Crosses)]
#[case::predicate_overlaps(Predicate::Overlaps)]
fn the_scalar_lane_gate_never_changes_an_answer(#[case] predicate: Predicate) {
    let line = |points: Vec<Point>| {
        Shape::LineString(
            LineSeq::try_new(CoordSeq::try_from_points(&points).unwrap()).expect("two vertices"),
        )
    };
    let corpus = [
        // Strictly disjoint envelopes — every spelling is gated.
        (
            line(vec![
                Point::new_raw_xy(0.0, 0.0),
                Point::new_raw_xy(1.0, 1.0),
            ]),
            line(vec![
                Point::new_raw_xy(10.0, 10.0),
                Point::new_raw_xy(11.0, 11.0),
            ]),
        ),
        // Envelopes that merely touch: the gate is strict, so these reach
        // the relation and really do touch.
        (
            line(vec![
                Point::new_raw_xy(0.0, 0.0),
                Point::new_raw_xy(1.0, 0.0),
            ]),
            line(vec![
                Point::new_raw_xy(1.0, 0.0),
                Point::new_raw_xy(2.0, 0.0),
            ]),
        ),
        // Overlapping envelopes, crossing linework.
        (
            line(vec![
                Point::new_raw_xy(0.0, 0.0),
                Point::new_raw_xy(2.0, 2.0),
            ]),
            line(vec![
                Point::new_raw_xy(0.0, 2.0),
                Point::new_raw_xy(2.0, 0.0),
            ]),
        ),
        // Overlapping envelopes, no shared point.
        (
            line(vec![
                Point::new_raw_xy(0.0, 0.0),
                Point::new_raw_xy(2.0, 1.0),
            ]),
            line(vec![
                Point::new_raw_xy(0.0, 2.0),
                Point::new_raw_xy(2.0, 3.0),
            ]),
        ),
        // Areal containment, where the nesting gates apply.
        (
            rectangle(0.0, 0.0, 10.0, 10.0),
            rectangle(2.0, 2.0, 4.0, 4.0),
        ),
    ];

    let spec = predicate.spec();
    for (left, right) in &corpus {
        let left_plan = PreparedPlan::new(left.clone());
        let right_plan = PreparedPlan::new(right.clone());
        let pair = PreparedTopologyPair::Cartesian {
            left: &left_plan,
            right: &right_plan,
        };
        let relation = predicate_matrix_value(&spec, pair.relate().unwrap());
        let pair = PreparedTopologyPair::Cartesian {
            left: &left_plan,
            right: &right_plan,
        };
        assert_eq!(
            topology_scalar_pair(&spec, &pair).unwrap(),
            relation,
            "{predicate:?} disagreed with its own relation",
        );
    }
}

#[test]
fn native_relate_pattern_matches_disjoint_areal_literals() {
    let (a, b) = (rectangle(0.0, 0.0, 1.0, 1.0), rectangle(3.0, 3.0, 4.0, 4.0));
    for (literal, expected) in [
        (b'F', true),
        (b'T', false),
        (b'*', true),
        (b'0', false),
        (b'1', false),
        (b'2', false),
    ] {
        let mut cells = *b"*********";
        cells[0] = literal;
        let pattern = std::str::from_utf8(&cells).unwrap();
        assert_eq!(
            a.relate_pattern(&b, pattern).unwrap(),
            expected,
            "literal {}",
            literal as char
        );
    }
    assert!(a.relate_pattern(&b, "FF2FF1212").unwrap());
    a.relate_pattern(&b, "bad").unwrap_err();
}
