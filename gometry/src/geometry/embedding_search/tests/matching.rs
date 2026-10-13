#[test]
fn matching_rearranges_an_earlier_cell_to_find_a_perfect_assignment() {
    let problem = FixtureProblem::new(vec![vec![0, 1]], vec![vec![0, 1], vec![0]]);
    let assignment = search_fixture(problem, 2).unwrap();
    let class = assignment.classes.first().unwrap();
    assert_eq!(class.variables, [0, 1]);
    assert_eq!(class.values, [1, 0]);
}

#[test]
fn matching_parent_rewrite_refusal_preserves_the_shared_raw_balance() {
    let class = PreparedOptions {
        class: 0,
        variables: vec![0, 1],
        options: vec![vec![(0_u8, 0_u8), (1, 1)], vec![(0, 0)]],
    };
    let seen_key_capacity = try_hash_set_with_capacity::<u8>(3).unwrap().capacity();
    // The two reservations cost 12 ticks; each root's admitted prefix
    // costs 8 plus the actual seen-key capacity. The remaining tick is
    // insufficient for the second root's two-tick parent/seen rewrite.
    let prefix_ticks = 28 + 2 * u128::try_from(seen_key_capacity).unwrap();
    let available_ticks = prefix_ticks + 1;
    let limit = u64::try_from(available_ticks.div_ceil(WORK_TICKS_PER_EVALUATION)).unwrap();
    let mut budget = EvaluationBudget::new("search_test", 2).unwrap();
    budget.limit = limit;
    let limit_ticks = budget.limit_ticks();
    budget.spent_ticks = limit_ticks - available_ticks;

    let error = has_perfect_matching(&class, &mut budget).unwrap_err();

    assert!(matches!(
        error.kind(),
        ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
            operation: "search_test",
            constructed: 2,
            evaluations
        }) if *evaluations == limit
    ));
    assert_eq!(budget.spent_ticks, limit_ticks - 1);
}

#[test]
fn hall_failure_is_unrepresentable_before_depth_first_search() {
    let problem = FixtureProblem::new(vec![vec![0, 1, 2]], vec![vec![0, 1], vec![0, 1], vec![
        0, 1,
    ]]);
    let local_calls = Rc::clone(&problem.local_calls);
    let error = search_fixture(problem, 3).unwrap_err();

    assert_eq!(local_calls.get(), 0);
    assert!(matches!(
        error.kind(),
        ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingUnrepresentable {
            operation: "search_test",
            constructed: 3
        })
    ));
}

#[test]
fn hall_failure_skips_class_preparation() {
    let problem = FixtureProblem::new(vec![vec![0, 1]], vec![vec![7], vec![7]]);
    let prepare_calls = Rc::clone(&problem.prepare_calls);
    let error = search_fixture(problem, 2).unwrap_err();

    assert!(matches!(
        error.kind(),
        ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingUnrepresentable {
            operation: "search_test",
            constructed: 2
        })
    ));
    assert_eq!(prepare_calls.get(), 0);
}
