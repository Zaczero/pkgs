#[test]
fn ordinary_and_paid_proximity_builders_keep_their_evaluation_ownership() {
    let chords = [ProximityChord {
        nodes: [0, 1],
        nearest: Segment {
            start: XY::new(0.0, 0.0),
            end: XY::new(0.0, 0.0),
        },
        can_move: [true, true],
    }];
    let incident_chords = [vec![0], vec![0]];
    let seeds = [0];
    let mut ordinary_movable = vec![None; 2];
    let ordinary = build_proximity_classes(
        2,
        &chords,
        &incident_chords,
        &seeds,
        &mut ordinary_movable,
        |_| Ok(true),
        |left, right| Ok(left.cmp(&right)),
        |_, _, _| true,
    )
    .unwrap();

    let mut paid_movable = vec![None; 2];
    let mut budget = EvaluationBudget::new("search_test", 2).unwrap();
    let mut paid_evaluations = 0_u64;
    let paid = build_proximity_classes_with_work(
        2,
        &chords,
        &incident_chords,
        &seeds,
        &mut paid_movable,
        |_| Ok(true),
        |left, right| Ok(left.cmp(&right)),
        |_, _, _| true,
        &mut budget,
        |account, _initial_evaluations, units| {
            account.charge_evaluations(units)?;
            paid_evaluations = paid_evaluations
                .checked_add(units)
                .ok_or(ResourceError::SizeOverflow)?;
            Ok(())
        },
    )
    .unwrap();

    assert_eq!(ordinary.classes, [vec![0, 1]]);
    assert_eq!(ordinary.initial_evaluations, 4);
    assert_eq!(paid.classes, ordinary.classes);
    assert_eq!(paid.initial_evaluations, 0);
    assert_eq!(paid_evaluations, 4);
}
