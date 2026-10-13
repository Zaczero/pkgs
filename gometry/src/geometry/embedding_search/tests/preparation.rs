#[test]
fn search_sorts_variables_and_candidates_before_returning_the_first_solution() {
    let problem = FixtureProblem::new(vec![vec![3, 1]], vec![
        Vec::new(),
        vec![3, 2],
        Vec::new(),
        vec![8, 6],
    ]);
    let assignment = search_fixture(problem, 2).unwrap();
    let class = assignment.classes.first().unwrap();
    assert_eq!(class.variables, [1, 3]);
    assert_eq!(class.values, [2, 6]);
}

#[test]
fn fixed_nearest_cells_are_removed_before_search() {
    let mut problem = FixtureProblem::new(vec![vec![0]], vec![vec![0, 1]]);
    problem.fixed_keys_by_class[0].push(0);
    let assignment = search_fixture(problem, 1).unwrap();
    assert_eq!(assignment.classes.first().unwrap().values, [1]);
}

#[test]
fn each_class_reserves_nearest_cells_of_other_classes() {
    let mut problem = FixtureProblem::new(vec![vec![0], vec![1]], vec![vec![0, 1], vec![0, 1]]);
    problem.fixed_keys_by_class[0].push(0);
    problem.fixed_keys_by_class[1].push(1);
    let assignment = search_fixture(problem, 2).unwrap();

    let first = assignment.classes.first().unwrap();
    let second = &assignment.classes[1];
    assert_eq!(
        (
            *first.variables.first().unwrap(),
            *first.values.first().unwrap()
        ),
        (0, 1)
    );
    assert_eq!(
        (
            *second.variables.first().unwrap(),
            *second.values.first().unwrap()
        ),
        (1, 0)
    );
}

#[test]
fn empty_variable_class_is_rejected_during_preparation() {
    let problem = FixtureProblem::new(vec![vec![0], Vec::new()], vec![vec![0]]);
    let error = search_fixture(problem, 1).unwrap_err();

    assert!(matches!(
        error.kind(),
        ErrorKind::Geometry(GeometryErrorKind::Invalid(_))
    ));
}

#[test]
fn one_budget_covers_candidate_admission_and_all_classes() {
    let mut problem = FixtureProblem::new(vec![vec![0], vec![1]], vec![vec![7], vec![8]]);
    let limit = BASE_EVALUATION_LIMIT + EVALUATIONS_PER_CONSTRUCTED_NODE;
    problem.initial_evaluations = limit - 2;
    let error = search_fixture(problem, 1).unwrap_err();

    assert!(matches!(
        error.kind(),
        ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
            operation: "search_test",
            constructed: 1,
            evaluations
        }) if *evaluations == limit
    ));
}

#[test]
fn over_budget_closure_reports_its_completed_work() {
    let mut problem = FixtureProblem::new(vec![vec![0]], vec![vec![7]]);
    let limit = BASE_EVALUATION_LIMIT + EVALUATIONS_PER_CONSTRUCTED_NODE;
    problem.initial_evaluations = limit + 7;
    let error = search_fixture(problem, 1).unwrap_err();

    assert!(matches!(
        error.kind(),
        ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
            operation: "search_test",
            constructed: 1,
            evaluations
        }) if *evaluations == limit + 7
    ));
}
