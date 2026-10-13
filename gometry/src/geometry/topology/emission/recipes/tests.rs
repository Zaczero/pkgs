
#[test]
fn bent_and_backtracking_producers_are_not_flattened() {
    let unsupported = [
        (
            vec![
                pending_source_range(0, 0.0, 1.0, 0.0, 0.0),
                pending_source_range(1, 1.0, 2.0, 0.0, 1.0),
            ],
            2.0,
            1.0,
        ),
        (
            vec![
                pending_source_range(0, 0.0, 2.0, 0.0, 0.0),
                pending_source_range(1, 2.0, 1.0, 0.0, 0.0),
            ],
            1.0,
            0.0,
        ),
    ];
    let mut work = Unmetered;
    for (ranges, longitude, latitude) in unsupported {
        producer_ranges(
            ranges,
            &ExactRational::from_f64(longitude),
            &ExactRational::from_f64(latitude),
            true,
            &mut work,
        )
        .unwrap_err();
    }
}

#[test]
fn endpoint_only_producer_keeps_two_endpoint_requests() {
    let quarter = ExactRational::from_f64(0.25);
    let three_quarters = ExactRational::from_f64(0.75);
    let mut work = Unmetered;
    let mapped = producer_ranges(
        vec![pending_source_range(4, 0.0, 10.0, 90.0, 90.0)],
        &ExactRational::from_f64(10.0),
        &ExactRational::zero(),
        false,
        &mut work,
    )
    .unwrap();
    let range = mapped
        .into_iter()
        .next()
        .ok_or_else(|| GeometryErrorKind::Invalid("point support range is missing".into()))
        .unwrap();
    assert!(range.endpoint_only);
    assert_eq!(range.producer_start, ExactRational::zero());
    assert_eq!(range.producer_end, ExactRational::one());

    let recipe = FragmentRecipe {
        id: FragmentRecipeId(0),
        endpoints: [QPortId(1), QPortId(2)],
        ranges: vec![DirectedSourceRange {
            source_edge: range.source_edge,
            range: ExactSourceRange {
                start: quarter.clone(),
                end: three_quarters.clone(),
            },
            emitted: range.emitted,
            producer_start: range.producer_start,
            producer_end: range.producer_end,
            endpoint_only: true,
        }],
        exact_displacement: ExactRational::from_f64(10.0),
        use_kind: SelectedUse::Curve,
    };
    let recipes = FragmentRecipes {
        values: vec![recipe],
        walks: Vec::new(),
    };
    let trace = recipes
        .source_trace_fragment(FragmentRecipeId(0), &mut work)
        .unwrap();
    assert_eq!(
        trace
            .rows
            .requests_at(0, &ExactRational::zero(), &mut work)
            .unwrap()
            .first()
            .ok_or_else(|| GeometryErrorKind::Invalid("point support lost its start".into()))
            .unwrap()
            .parameter(),
        &quarter
    );
    assert!(
        trace
            .rows
            .requests_at(0, &ExactRational::from_f64(0.5), &mut work)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        trace
            .rows
            .requests_at(0, &ExactRational::one(), &mut work)
            .unwrap()
            .first()
            .ok_or_else(|| GeometryErrorKind::Invalid("point support lost its end".into()))
            .unwrap()
            .parameter(),
        &three_quarters
    );
}
