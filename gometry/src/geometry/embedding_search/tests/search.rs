#[test]
fn nested_finite_exhaustion_rejects_outer_candidate_and_search_continues() {
    let mut problem = FixtureProblem::new(vec![vec![0]], vec![vec![0, 1]]);
    problem.nested_completion = true;
    let hall_calls = Rc::clone(&problem.nested_hall_calls);
    let pass_calls = Rc::clone(&problem.nested_pass_calls);
    let global_calls = Rc::clone(&problem.global_calls);

    let assignment = search_fixture(problem, 1).unwrap();

    assert_eq!(
        assignment
            .classes
            .first()
            .expect("outer class is returned")
            .values,
        [1]
    );
    assert_eq!(hall_calls.get(), 1);
    assert_eq!(pass_calls.get(), 1);
    assert_eq!(global_calls.get(), 2);
    assert_eq!(assignment.output.0, [(0, 1)]);
}

#[test]
fn nested_search_exhaustion_spends_the_same_raw_budget() {
    let mut baseline_problem = FixtureProblem::new(vec![vec![0]], vec![vec![0, 1]]);
    baseline_problem.nested_completion = true;
    let mut baseline_budget = EvaluationBudget::new("outer_operation", 1).unwrap();
    assert!(matches!(
        run_with_budget(&mut baseline_problem, &mut baseline_budget).unwrap(),
        SearchOutcome::Pass(_)
    ));
    let outer_prefix_ticks = baseline_problem
        .nested_before_ticks
        .expect("outer G reaches nested completion");
    let nested_ticks = baseline_problem.nested_completion_ticks;
    assert!(nested_ticks > 0);

    let remaining_ticks = outer_prefix_ticks + nested_ticks - 1;
    let mut shared_budget =
        EvaluationBudget::with_remaining_ticks_for_test("outer_operation", 1, remaining_ticks)
            .unwrap();
    let mut nested_problem = FixtureProblem::new(vec![vec![0]], vec![vec![0, 1]]);
    nested_problem.nested_completion = true;
    let error = run_with_budget(&mut nested_problem, &mut shared_budget).unwrap_err();

    assert!(matches!(
        error.kind(),
        ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
            operation: "outer_operation",
            constructed: 1,
            ..
        })
    ));
    assert_eq!(nested_problem.global_calls.get(), 1);
    assert_eq!(nested_problem.nested_hall_calls.get(), 1);
    assert_eq!(nested_problem.nested_pass_calls.get(), 0);
    assert_eq!(shared_budget.spent_ticks, shared_budget.limit_ticks());
}

#[test]
fn adding_constructed_nodes_keeps_prior_work_in_the_same_budget() {
    let mut budget = EvaluationBudget::new("shared_operation", 1).unwrap();
    budget.charge_evaluations(3).unwrap();
    budget.include_constructed(2).unwrap();
    assert_eq!(budget.constructed(), 2);
    assert_eq!(budget.spent_ticks, 48);

    budget.set_constructed(3).unwrap();
    assert_eq!(budget.constructed(), 3);
    assert_eq!(budget.spent_ticks, 48);

    let error = budget.set_constructed(2).unwrap_err();
    assert!(matches!(
        error.kind(),
        ErrorKind::Geometry(GeometryErrorKind::Invalid(_))
    ));
    assert_eq!(budget.constructed(), 3);
    assert_eq!(budget.spent_ticks, 48);
}

#[test]
fn search_forwards_the_shared_work_handle_to_both_comparison_hooks() {
    let phase = Rc::new(Cell::new(0));
    let mut problem = FixtureProblem::new(vec![vec![1, 0]], vec![vec![1, 0]; 2]);
    problem.comparison_phase = Some(Rc::clone(&phase));
    let mut account = ComparisonWorkProbe {
        budget: EvaluationBudget::new("comparison_work_probe", 2).unwrap(),
        phase,
        comparator_ticks: Cell::new([0; 3]),
    };

    assert!(matches!(
        run_with_budget(&mut problem, &mut account).unwrap(),
        SearchOutcome::Pass(_)
    ));
    let comparator_ticks = account.comparator_ticks.get();
    assert!(comparator_ticks[1] >= 37, "node comparison was not charged");
    assert!(
        comparator_ticks[2] >= 37,
        "candidate comparison was not charged"
    );

    let baseline = Search::new(
        FixtureProblem::new(vec![vec![1, 0]], vec![vec![1, 0]; 2]),
        "comparison_work_probe",
        2,
    )
    .unwrap()
    .run()
    .unwrap();
    let ordinary_phase = Rc::new(Cell::new(0));
    let mut ordinary_problem = FixtureProblem::new(vec![vec![1, 0]], vec![vec![1, 0]; 2]);
    ordinary_problem.comparison_phase = Some(ordinary_phase);
    let ordinary = Search::new(ordinary_problem, "comparison_work_probe", 2)
        .unwrap()
        .run()
        .unwrap();
    assert_eq!(ordinary.evaluations, baseline.evaluations);
    assert_eq!(ordinary.output, baseline.output);
    assert_eq!(ordinary.classes[0].variables, baseline.classes[0].variables);
    assert_eq!(ordinary.classes[0].values, baseline.classes[0].values);
}

#[test]
fn local_failure_backtracks_to_the_next_lexicographic_candidate() {
    let mut problem = FixtureProblem::new(vec![vec![1, 0]], vec![vec![1, 0], vec![1, 0]]);
    problem.rejected.push((0, 0, 0));
    let assignment = search_fixture(problem, 2).unwrap();
    let class = assignment.classes.first().unwrap();
    assert_eq!(class.variables, [0, 1]);
    assert_eq!(class.values, [1, 0]);
}

#[test]
fn global_rejections_visit_products_in_deterministic_variable_order() {
    let mut problem = FixtureProblem::new(
        vec![vec![10], vec![0]],
        std::iter::repeat_n(Vec::new(), 11).collect(),
    );
    problem.candidates[0] = vec![1, 0];
    problem.candidates[10] = vec![3, 2];
    problem.global_requirements = Some(vec![vec![(0, 1), (10, 3)]]);
    problem.blamed_classes = vec![0, 1];
    let global_calls = Rc::clone(&problem.global_calls);
    let assignment = search_fixture(problem, 2).unwrap();

    assert_eq!(global_calls.get(), 4);
    let first = assignment.classes.first().unwrap();
    assert_eq!(first.variables, [0]);
    assert_eq!(first.values, [1]);
    let second = &assignment.classes[1];
    assert_eq!(second.variables, [10]);
    assert_eq!(second.values, [3]);
}

#[test]
fn empty_blame_is_unrepresentable_when_no_prepared_assignment_can_repair() {
    let mut problem = FixtureProblem::new(vec![vec![0]], vec![vec![0]]);
    problem.global_requirements = Some(vec![vec![(0, 1)]]);
    let error = search_fixture(problem, 1).unwrap_err();

    assert!(matches!(
        error.kind(),
        ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingUnrepresentable {
            operation: "search_test",
            constructed: 1
        })
    ));
}

#[test]
fn exhausting_every_blamed_class_is_unrepresentable() {
    let mut problem = FixtureProblem::new(vec![vec![0]], vec![vec![0]]);
    problem.global_requirements = Some(vec![vec![(0, 1)]]);
    problem.blamed_classes = vec![0];
    let error = search_fixture(problem, 1).unwrap_err();

    assert!(matches!(
        error.kind(),
        ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingUnrepresentable {
            operation: "search_test",
            constructed: 1
        })
    ));
}

#[test]
fn global_rejection_blame_cannot_skip_a_prepared_product() {
    let mut problem = FixtureProblem::new(vec![vec![0], vec![1]], vec![vec![0, 1], vec![2, 3]]);
    problem.global_requirements = Some(vec![vec![(0, 0), (1, 3)]]);
    problem.blame_by_assignment = vec![
        (vec![(0, 0), (1, 2)], vec![0, 1]),
        (vec![(0, 1), (1, 2)], vec![1]),
        (vec![(0, 1), (1, 3)], vec![0]),
    ];
    let visits = Rc::new(RefCell::new(Vec::new()));
    problem.global_visits = Some(Rc::clone(&visits));
    let assignment = search_fixture(problem, 2);

    assert!(
        assignment.is_ok(),
        "search skipped the passing prepared product (class 0 -> 0, class 1 -> 3): {assignment:?}"
    );
    let assignment = assignment.unwrap();
    assert_eq!(assignment.classes[0].values, [0]);
    assert_eq!(assignment.classes[1].values, [3]);
    assert_eq!(*visits.borrow(), [
        vec![(0, 0), (1, 2)],
        vec![(0, 1), (1, 2)],
        vec![(0, 0), (1, 3)],
    ]);
    assert_eq!(assignment.output.0, [(0, 0), (1, 3)]);
}

#[test]
fn borrowed_and_owned_search_preserve_first_blame_and_move_the_global_output() {
    let mut borrowed_problem =
        FixtureProblem::new(vec![vec![0], vec![1]], vec![vec![0, 1], vec![2, 3]]);
    borrowed_problem.global_requirements = Some(vec![vec![(0, 0), (1, 3)]]);
    borrowed_problem.blamed_classes = vec![0, 1];
    borrowed_problem.blame_by_assignment = vec![
        (vec![(0, 0), (1, 2)], vec![0, 1]),
        (vec![(0, 1), (1, 2)], vec![1]),
        (vec![(0, 1), (1, 3)], vec![0]),
    ];
    let borrowed_visits = Rc::new(RefCell::new(Vec::new()));
    borrowed_problem.global_visits = Some(Rc::clone(&borrowed_visits));
    let mut budget = EvaluationBudget::new("search_test", 2).unwrap();
    let SearchOutcome::Pass(borrowed) =
        run_with_budget(&mut borrowed_problem, &mut budget).unwrap()
    else {
        panic!("the final prepared product satisfies the global certificate");
    };

    let mut owned_problem =
        FixtureProblem::new(vec![vec![0], vec![1]], vec![vec![0, 1], vec![2, 3]]);
    owned_problem.global_requirements = Some(vec![vec![(0, 0), (1, 3)]]);
    owned_problem.blamed_classes = vec![0, 1];
    owned_problem.blame_by_assignment = vec![
        (vec![(0, 0), (1, 2)], vec![0, 1]),
        (vec![(0, 1), (1, 2)], vec![1]),
        (vec![(0, 1), (1, 3)], vec![0]),
    ];
    let owned_visits = Rc::new(RefCell::new(Vec::new()));
    owned_problem.global_visits = Some(Rc::clone(&owned_visits));
    let owned = Search::new(owned_problem, "search_test", 2)
        .unwrap()
        .run()
        .unwrap();

    let expected_visits = [vec![(0, 0), (1, 2)], vec![(0, 1), (1, 2)], vec![
        (0, 0),
        (1, 3),
    ]];
    assert_eq!(*borrowed_visits.borrow(), expected_visits);
    assert_eq!(*owned_visits.borrow(), expected_visits);
    assert_eq!(borrowed.output.0, [(0, 0), (1, 3)]);
    assert_eq!(owned.output.0, borrowed.output.0);
    assert_eq!(owned.evaluations, 17);
    assert!(
        budget.spent_ticks > u128::from(owned.evaluations) * WORK_TICKS_PER_EVALUATION,
        "the borrowed account also pays raw engine work"
    );
}

#[test]
fn borrowed_search_does_not_import_the_problem_legacy_evaluation_tally() {
    let mut problem = FixtureProblem::new(vec![vec![0]], vec![vec![7]]);
    problem.initial_evaluations = BASE_EVALUATION_LIMIT + EVALUATIONS_PER_CONSTRUCTED_NODE + 7;
    let mut budget = EvaluationBudget::new("search_test", 1).unwrap();

    let SearchOutcome::Pass(assignment) = run_with_budget(&mut problem, &mut budget).unwrap()
    else {
        panic!("the fixture has one finite candidate assignment");
    };

    assert_eq!(assignment.output.0, [(0, 7)]);
    assert!(budget.spent_ticks < budget.limit_ticks());
}

#[test]
fn borrowed_search_checks_a_zero_class_problem_and_moves_its_payload() {
    let mut problem = FixtureProblem::new(Vec::new(), Vec::new());
    let global_calls = Rc::clone(&problem.global_calls);
    let mut budget = EvaluationBudget::new("search_test", 0).unwrap();

    let (outcome, allocations) = crate::test_support::allocation_count(|| {
        run_with_budget(&mut problem, &mut budget).unwrap()
    });
    let SearchOutcome::Pass(assignment) = outcome else {
        panic!("an empty assignment passes the fixture's global certificate");
    };

    assert!(assignment.classes.is_empty());
    assert!(assignment.output.0.is_empty());
    assert_eq!(global_calls.get(), 1);
    assert_eq!(
        allocations, 0,
        "empty classes do not allocate candidate scratch"
    );
}

#[test]
fn borrowed_zero_class_empty_blame_is_finite_exhaustion() {
    let mut problem = FixtureProblem::new(Vec::new(), Vec::new());
    problem.global_requirements = Some(Vec::new());
    let global_calls = Rc::clone(&problem.global_calls);
    let mut budget = EvaluationBudget::new("search_test", 0).unwrap();

    let outcome = run_with_budget(&mut problem, &mut budget).unwrap();

    assert!(matches!(outcome, SearchOutcome::Exhausted));
    assert_eq!(global_calls.get(), 1);
}

#[test]
fn borrowed_zero_class_global_error_is_propagated() {
    let mut problem = FixtureProblem::new(Vec::new(), Vec::new());
    problem.global_error = true;
    let global_calls = Rc::clone(&problem.global_calls);
    let mut budget = EvaluationBudget::new("search_test", 0).unwrap();

    let error = run_with_budget(&mut problem, &mut budget).unwrap_err();

    assert!(matches!(
        error.kind(),
        ErrorKind::Geometry(GeometryErrorKind::Invalid(_))
    ));
    assert_eq!(global_calls.get(), 1);
}

#[test]
fn borrowed_zero_class_nonempty_blame_is_invalid() {
    let mut problem = FixtureProblem::new(Vec::new(), Vec::new());
    problem.global_requirements = Some(Vec::new());
    problem.blamed_classes = vec![0];
    let mut budget = EvaluationBudget::new("search_test", 0).unwrap();

    let error = run_with_budget(&mut problem, &mut budget).unwrap_err();

    assert!(matches!(
        error.kind(),
        ErrorKind::Geometry(GeometryErrorKind::Invalid(_))
    ));
}

#[test]
fn global_output_reservation_refusal_precedes_materialization() {
    let variables = [0_usize];
    let values = [7_u8];
    let assignments = [ClassAssignment {
        variables: &variables,
        values: &values,
    }];
    let mut problem = FixtureProblem::new(vec![vec![0]], vec![vec![7]]);
    let global_calls = Rc::clone(&problem.global_calls);
    let mut budget = EvaluationBudget::new("search_test", 1).unwrap();
    budget.limit = 1;

    let Err(error) = Problem::check_global(&mut problem, &assignments, &mut budget) else {
        panic!("the output buffer debit must refuse before materialization");
    };

    assert!(matches!(
        error.kind(),
        ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
            operation: "search_test",
            constructed: 1,
            evaluations: 1
        })
    ));
    assert_eq!(global_calls.get(), 1);
    assert_eq!(budget.spent_ticks, 16);
}

#[test]
fn evaluation_only_outer_pays_raw_work_only_in_the_native_inner_search() {
    let mut ordinary_problem = FixtureProblem::new(vec![vec![0]], vec![vec![0, 1]]);
    ordinary_problem.nested_search = NestedSearch::HallOn(0);
    let ordinary_visits = Rc::new(RefCell::new(Vec::new()));
    ordinary_problem.global_visits = Some(Rc::clone(&ordinary_visits));
    let mut ordinary_budget = EvaluationBudget::new("search_test", 1).unwrap();
    let ordinary_outcome = {
        let mut outer = EvaluationOnly::new(&mut ordinary_budget);
        run_with_budget(&mut ordinary_problem, &mut outer).unwrap()
    };
    let SearchOutcome::Pass(ordinary_assignment) = ordinary_outcome else {
        panic!("the second outer candidate passes after the nested Hall rejection");
    };
    let ordinary_inner_ticks = ordinary_problem.nested_accounted_ticks;

    let mut native_problem = FixtureProblem::new(vec![vec![0]], vec![vec![0, 1]]);
    native_problem.nested_search = NestedSearch::HallOn(0);
    let native_visits = Rc::new(RefCell::new(Vec::new()));
    native_problem.global_visits = Some(Rc::clone(&native_visits));
    let mut native_budget = EvaluationBudget::new("search_test", 1).unwrap();
    let SearchOutcome::Pass(native_assignment) =
        run_with_budget(&mut native_problem, &mut native_budget).unwrap()
    else {
        panic!("the native outer account also reaches the passing candidate");
    };
    let native_inner_ticks = native_problem.nested_accounted_ticks;

    assert_eq!(*ordinary_visits.borrow(), [vec![(0, 0)], vec![(0, 1)]]);
    assert_eq!(*native_visits.borrow(), *ordinary_visits.borrow());
    assert_eq!(ordinary_assignment.classes[0].values, [1]);
    assert_eq!(native_assignment.classes[0].values, [1]);
    // The Hall fixture admits two candidate keys before matching rejects it.
    let inner_raw_ticks = ordinary_inner_ticks.checked_sub(32).unwrap();
    assert!(inner_raw_ticks > 0);
    assert_eq!(native_inner_ticks, ordinary_inner_ticks);
    assert!(native_budget.spent_ticks > ordinary_budget.spent_ticks);
}

#[test]
fn nested_limit_keeps_the_enclosing_operation_and_constructed_count() {
    let mut problem = FixtureProblem::new(Vec::new(), Vec::new());
    problem.nested_search = NestedSearch::HallAlways;
    let mut budget = EvaluationBudget::new("outer_operation", 42).unwrap();
    budget.limit = 1;

    let error = run_with_budget(&mut problem, &mut budget).unwrap_err();

    assert!(matches!(
        error.kind(),
        ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
            operation: "outer_operation",
            constructed: 42,
            evaluations: 1
        })
    ));
    assert_eq!(budget.spent_ticks, 16);
}

#[test]
fn evaluation_only_outer_native_inner_limit_keeps_the_outer_budget() {
    let mut problem = FixtureProblem::new(vec![vec![0]], vec![vec![7]]);
    problem.nested_search = NestedSearch::HallAlways;
    let global_calls = Rc::clone(&problem.global_calls);
    let mut budget = EvaluationBudget::new("mixed_outer", 42).unwrap();
    budget.limit = 4;

    let error = {
        let mut outer = EvaluationOnly::new(&mut budget);
        run_with_budget(&mut problem, &mut outer).unwrap_err()
    };

    assert!(matches!(
        error.kind(),
        ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
            operation: "mixed_outer",
            constructed: 42,
            evaluations: 4
        })
    ));
    assert_eq!(global_calls.get(), 1);
    assert_eq!(budget.spent_ticks, 64);
}

#[test]
fn candidate_cursor_refusal_precedes_dfs_state_mutation() {
    let problem = FixtureProblem::new(vec![vec![0]], vec![vec![7]]);
    let class = PreparedClass {
        class: 0,
        variables: vec![0],
        options: vec![vec![(7, 7)]],
        local: (),
    };
    let mut state = ClassSearchState::new(&class, &mut Unmetered).unwrap();
    let mut account = EvaluationBudget::new("search_test", 1).unwrap();
    account.limit = 1;

    let error = state
        .next_solution(&class, &problem, &mut account)
        .unwrap_err();

    assert!(matches!(
        error.kind(),
        ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
            operation: "search_test",
            constructed: 1,
            evaluations: 1
        })
    ));
    assert_eq!(account.spent_ticks, 16);
    assert_eq!(state.next_candidate, [0]);
    assert_eq!(state.selected, [None]);
    assert!(state.used_keys.is_empty());
    assert_eq!(problem.local_calls.get(), 0);
}

#[test]
fn cell_assignment_refusal_precedes_key_and_value_mutation() {
    let problem = FixtureProblem::new(vec![vec![0]], vec![vec![7]]);
    let class = PreparedClass {
        class: 0,
        variables: vec![0],
        options: vec![vec![(7, 7)]],
        local: (),
    };
    let mut state = ClassSearchState::new(&class, &mut Unmetered).unwrap();
    let mut account = EvaluationBudget::new("search_test", 1).unwrap();
    account.limit = 2;
    account.spent_ticks = 14;

    let error = state
        .next_solution(&class, &problem, &mut account)
        .unwrap_err();

    assert!(matches!(
        error.kind(),
        ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
            operation: "search_test",
            constructed: 1,
            evaluations: 2
        })
    ));
    assert_eq!(account.spent_ticks, 31);
    assert_eq!(state.next_candidate, [1]);
    assert_eq!(state.selected, [None]);
    assert_eq!(state.selected_keys, [None]);
    assert!(state.used_keys.is_empty());
    assert_eq!(problem.local_calls.get(), 0);
}

#[test]
fn option_exhaustion_refusal_precedes_digit_reset() {
    let problem = FixtureProblem::new(vec![vec![0]], vec![vec![7]]);
    let class = PreparedClass {
        class: 0,
        variables: vec![0],
        options: vec![vec![(7, 7)]],
        local: (),
    };
    let mut state = ClassSearchState::new(&class, &mut Unmetered).unwrap();
    state.next_candidate[0] = 1;
    let mut account = EvaluationBudget::new("search_test", 1).unwrap();
    account.limit = 1;
    account.spent_ticks = 16;

    let error = state
        .next_solution(&class, &problem, &mut account)
        .unwrap_err();

    assert!(matches!(
        error.kind(),
        ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
            operation: "search_test",
            constructed: 1,
            evaluations: 1
        })
    ));
    assert_eq!(account.spent_ticks, 16);
    assert_eq!(state.next_candidate, [1]);
    assert!(!state.exhausted);
    assert_eq!(problem.local_calls.get(), 0);
}

#[test]
fn reset_refusal_leaves_the_current_local_solution_intact() {
    let class = PreparedClass {
        class: 0,
        variables: vec![0],
        options: vec![vec![(7, 7)]],
        local: (),
    };
    let mut state = ClassSearchState::new(&class, &mut Unmetered).unwrap();
    state.next_candidate[0] = 1;
    state.selected[0] = Some(7);
    state.selected_keys[0] = Some(7);
    state.used_keys.insert(7);
    state.yielded = true;
    let mut account = EvaluationBudget::new("search_test", 1).unwrap();
    account.limit = 1;
    account.spent_ticks = 14;

    let error = state.reset(&mut account).unwrap_err();

    assert!(matches!(
        error.kind(),
        ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
            operation: "search_test",
            constructed: 1,
            evaluations: 1
        })
    ));
    assert_eq!(account.spent_ticks, 14);
    assert_eq!(state.next_candidate, [1]);
    assert_eq!(state.selected, [Some(7)]);
    assert_eq!(state.selected_keys, [Some(7)]);
    assert!(state.used_keys.contains(&7));
    assert!(state.yielded);
    assert!(!state.exhausted);
}

#[test]
fn solution_copy_refusal_does_not_mark_the_stream_as_yielded() {
    let problem = FixtureProblem::new(vec![vec![0]], vec![vec![7]]);
    let class = PreparedClass {
        class: 0,
        variables: vec![0],
        options: vec![vec![(7, 7)]],
        local: (),
    };
    let mut state = ClassSearchState::new(&class, &mut Unmetered).unwrap();
    let mut account = EvaluationBudget::new("search_test", 1).unwrap();
    account.limit = 3;
    account.spent_ticks = 11;

    let error = state
        .next_solution(&class, &problem, &mut account)
        .unwrap_err();

    assert!(matches!(
        error.kind(),
        ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
            operation: "search_test",
            constructed: 1,
            evaluations: 3
        })
    ));
    assert_eq!(account.spent_ticks, 47);
    assert_eq!(state.next_candidate, [1]);
    assert_eq!(state.selected, [Some(7)]);
    assert_eq!(state.selected_keys, [Some(7)]);
    assert!(state.used_keys.contains(&7));
    assert!(!state.yielded);
    assert_eq!(problem.local_calls.get(), 1);
}

#[test]
fn two_and_three_class_acceptance_tables_follow_first_blame_order() {
    for (streams, class_orders) in [
        (vec![vec![0, 1], vec![2, 3]], vec![vec![0, 1], vec![1, 0]]),
        (vec![vec![0, 1], vec![2, 3], vec![4, 5]], vec![
            vec![0, 1, 2],
            vec![0, 2, 1],
            vec![1, 0, 2],
            vec![1, 2, 0],
            vec![2, 0, 1],
            vec![2, 1, 0],
        ]),
    ] {
        let first_blame = 1;
        let mut digit_order = Vec::with_capacity(streams.len());
        digit_order.push(first_blame);
        digit_order.extend((0..streams.len()).filter(|&node| node != first_blame));
        let products = cartesian_products_with_ordered_fastest_stream(&streams, &digit_order);
        let acceptance_count = 1_usize
            .checked_shl(u32::try_from(products.len()).unwrap())
            .unwrap();
        let accepted_tables = if streams.len() == 2 {
            (0..acceptance_count).collect::<Vec<_>>()
        } else {
            vec![
                0,
                1,
                2,
                4,
                128,
                3,
                7,
                0b0101_0101,
                0b1010_1010,
                0b1110_0000,
                255,
            ]
        };
        let candidates = (0..streams.len())
            .map(|node| {
                let first = u8::try_from(node).unwrap().wrapping_mul(2);
                vec![first, first.wrapping_add(1)]
            })
            .collect::<Vec<_>>();

        for class_order in class_orders {
            for &accepted in &accepted_tables {
                let global_requirements = products
                    .iter()
                    .enumerate()
                    .filter_map(|(index, product)| {
                        let bit = 1_usize.checked_shl(u32::try_from(index).unwrap()).unwrap();
                        (accepted & bit != 0)
                            .then(|| product.iter().copied().enumerate().collect::<Vec<_>>())
                    })
                    .collect();
                let class_for_node = |node| {
                    class_order
                        .iter()
                        .position(|&class_node| class_node == node)
                        .expect("each singleton node has one original class ID")
                };
                let blame_by_assignment = products
                    .iter()
                    .enumerate()
                    .map(|(index, product)| {
                        let assignment = product.iter().copied().enumerate().collect();
                        let blamed_node = match index {
                            1 => 0,
                            2 => streams.len().wrapping_sub(1),
                            _ => first_blame,
                        };
                        (assignment, vec![class_for_node(blamed_node)])
                    })
                    .collect();
                let classes = class_order.iter().copied().map(|node| vec![node]).collect();
                let mut problem = FixtureProblem::new(classes, candidates.clone());
                problem.global_requirements = Some(global_requirements);
                problem.blamed_classes = vec![class_for_node(first_blame)];
                problem.blame_by_assignment = blame_by_assignment;
                let visits = Rc::new(RefCell::new(Vec::new()));
                problem.global_visits = Some(Rc::clone(&visits));
                let result = search_fixture(problem, streams.len());

                let first = products.iter().enumerate().find(|(index, _)| {
                    let bit = 1_usize.checked_shl(u32::try_from(*index).unwrap()).unwrap();
                    accepted & bit != 0
                });
                let expected_visit_count =
                    first.map_or(products.len(), |(index, _)| index.wrapping_add(1));
                let expected_visits = products
                    .iter()
                    .take(expected_visit_count)
                    .map(|product| product.iter().copied().enumerate().collect::<Vec<_>>())
                    .collect::<Vec<_>>();
                assert_eq!(
                    *visits.borrow(),
                    expected_visits,
                    "acceptance mask {accepted:#x}, class order {class_order:?}"
                );

                if let Some((_, expected)) = first {
                    assert!(
                        result.is_ok(),
                        "acceptance mask {accepted:#x}, class order {class_order:?} must return the first passing product: {result:?}"
                    );
                    let assignment = result.unwrap();
                    let actual = assignment
                        .classes
                        .iter()
                        .flat_map(|class| {
                            class
                                .variables
                                .iter()
                                .copied()
                                .zip(class.values.iter().copied())
                        })
                        .collect::<Vec<_>>();
                    assert_eq!(
                        actual,
                        expected.iter().copied().enumerate().collect::<Vec<_>>()
                    );
                } else {
                    let error = result.unwrap_err();
                    assert!(matches!(
                        error.kind(),
                        ErrorKind::Geometry(
                            GeometryErrorKind::ExactEmbeddingUnrepresentable { .. }
                        )
                    ));
                }
            }
        }
    }
}

#[test]
fn product_carry_restarts_unequal_local_streams_and_keeps_fixed_classes() {
    let mut problem = FixtureProblem::new(vec![vec![0, 1], vec![2], vec![3]], vec![
        vec![0, 1, 2],
        vec![0, 1, 2],
        vec![3, 4],
        vec![5],
    ]);
    problem.rejected.push((0, 0, 0));
    problem.global_requirements = Some(vec![vec![(0, 2), (1, 1), (2, 4), (3, 5)]]);
    problem.blamed_classes = vec![0, 1, 2];
    let visits = Rc::new(RefCell::new(Vec::new()));
    problem.global_visits = Some(Rc::clone(&visits));
    let assignment = search_fixture(problem, 3).unwrap();

    let visits_expected = [
        [1, 0, 3, 5],
        [1, 2, 3, 5],
        [2, 0, 3, 5],
        [2, 1, 3, 5],
        [1, 0, 4, 5],
        [1, 2, 4, 5],
        [2, 0, 4, 5],
        [2, 1, 4, 5],
    ]
    .map(|values| values.into_iter().enumerate().collect::<Vec<_>>());
    assert_eq!(*visits.borrow(), visits_expected);
    assert_eq!(
        assignment
            .classes
            .iter()
            .flat_map(|class| {
                class
                    .variables
                    .iter()
                    .copied()
                    .zip(class.values.iter().copied())
            })
            .collect::<Vec<_>>(),
        [(0, 2), (1, 1), (2, 4), (3, 5)]
    );
}

#[test]
fn finite_product_exhaustion_and_search_budget_remain_distinct() {
    let mut no_passing_product =
        FixtureProblem::new(vec![vec![0], vec![1]], vec![vec![0, 1], vec![2, 3]]);
    no_passing_product.global_requirements = Some(Vec::new());
    no_passing_product.blamed_classes = vec![0, 1];
    let error = search_fixture(no_passing_product, 2).unwrap_err();
    assert!(matches!(
        error.kind(),
        ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingUnrepresentable {
            operation: "search_test",
            constructed: 2
        })
    ));

    let mut budgeted = FixtureProblem::new(vec![vec![0], vec![1]], vec![vec![0, 1], vec![2]]);
    budgeted.global_requirements = Some(vec![vec![(0, 1), (1, 2)]]);
    budgeted.blamed_classes = vec![0, 1];
    let error = Search::with_evaluation_limit(budgeted, "search_test", 2, 9)
        .unwrap()
        .run()
        .unwrap_err();
    assert!(matches!(
        error.kind(),
        ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
            operation: "search_test",
            constructed: 2,
            evaluations: 9
        })
    ));
}

#[test]
fn a_single_last_class_repair_fits_the_default_budget() {
    const CLASS_COUNT: usize = 20;

    let classes = (0..CLASS_COUNT).map(|node| vec![node]).collect();
    let candidates = (0..CLASS_COUNT)
        .map(|node| {
            let first = u8::try_from(node).unwrap().wrapping_mul(2);
            vec![first, first.wrapping_add(1)]
        })
        .collect();
    let target = (0..CLASS_COUNT)
        .map(|node| {
            let first = u8::try_from(node).unwrap().wrapping_mul(2);
            (
                node,
                if node == CLASS_COUNT.wrapping_sub(1) {
                    first.wrapping_add(1)
                } else {
                    first
                },
            )
        })
        .collect::<Vec<_>>();
    let mut problem = FixtureProblem::new(classes, candidates);
    problem.global_requirements = Some(vec![vec![(19, 39)]]);
    problem.blamed_classes = vec![CLASS_COUNT.wrapping_sub(1)];
    let global_calls = Rc::clone(&problem.global_calls);
    let assignment = search_fixture(problem, CLASS_COUNT);

    assert!(
        assignment.is_ok(),
        "the single candidate in the only blamed class must be tried within the default budget; completed global checks: {}; result: {assignment:?}",
        global_calls.get()
    );
    let assignment = assignment.unwrap();
    let actual = assignment
        .classes
        .iter()
        .flat_map(|class| {
            class
                .variables
                .iter()
                .copied()
                .zip(class.values.iter().copied())
        })
        .collect::<Vec<_>>();
    assert_eq!(actual, target);
    assert_eq!(
        global_calls.get(),
        2,
        "the nearest product and its one-class repair each receive one global check"
    );
}

#[test]
fn unknown_blame_is_invalid_before_or_after_product_order_initialization() {
    let mut problem = FixtureProblem::new(vec![vec![0]], vec![vec![0, 1]]);
    problem.global_requirements = Some(Vec::new());
    problem.blamed_classes = vec![1];
    let error = search_fixture(problem, 1).unwrap_err();
    assert!(matches!(
        error.kind(),
        ErrorKind::Geometry(GeometryErrorKind::Invalid(_))
    ));

    let mut problem = FixtureProblem::new(vec![vec![0], vec![1]], vec![vec![0, 1], vec![2, 3]]);
    problem.global_requirements = Some(vec![vec![(0, 1), (1, 3)]]);
    problem.blamed_classes = vec![0];
    problem.blame_by_assignment = vec![(vec![(0, 1), (1, 2)], vec![2])];
    let visits = Rc::new(RefCell::new(Vec::new()));
    problem.global_visits = Some(Rc::clone(&visits));
    let error = search_fixture(problem, 2).unwrap_err();

    assert!(matches!(
        error.kind(),
        ErrorKind::Geometry(GeometryErrorKind::Invalid(_))
    ));
    assert_eq!(
        *visits.borrow(),
        [vec![(0, 0), (1, 2)], vec![(0, 1), (1, 2)]],
        "the second rejection's unknown class is checked after traversal order is fixed"
    );
}

#[test]
fn tiny_budget_is_a_search_limit_and_default_budget_accepts_the_same_problem() {
    let error = Search::with_evaluation_limit(
        FixtureProblem::new(vec![vec![0]], vec![vec![7]]),
        "search_test",
        1,
        0,
    )
    .unwrap()
    .run()
    .unwrap_err();
    assert!(matches!(
        error.kind(),
        ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
            operation: "search_test",
            constructed: 1,
            evaluations: 0
        })
    ));

    let assignment = search_fixture(FixtureProblem::new(vec![vec![0]], vec![vec![7]]), 1).unwrap();
    assert_eq!(assignment.classes.first().unwrap().values, [7]);
}

#[test]
fn native_budget_refuses_a_permutation_swap_before_touching_items() {
    let mut items: Vec<_> = (16_u8..32).chain(0..16).map(|key| (key, key)).collect();
    let original = items.clone();
    let mut comparisons = 0;
    let mut budget = EvaluationBudget::new("search_test", 1).unwrap();
    budget.limit = 11;
    budget.spent_ticks = 33;

    let error = try_sort_by_with_work(&mut items, &mut budget, |left, right, _work| {
        comparisons += 1;
        Ok(left.0.cmp(&right.0))
    })
    .unwrap_err();

    assert!(matches!(
        error.kind(),
        ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
            operation: "search_test",
            constructed: 1,
            evaluations: 11
        })
    ));
    assert_eq!(comparisons, 46);
    assert_eq!(items, original);
    assert_eq!(budget.spent_ticks, 176);
}

#[test]
fn native_budget_refusal_precedes_product_carry_solution_replacement() {
    let mut current_solutions = vec![vec![9_u8]];
    let mut budget = EvaluationBudget::new("search_test", 1).unwrap();
    budget.limit = 1;
    budget.spent_ticks = 16;

    let error =
        replace_class_solution(&mut current_solutions, 0, vec![7], &mut budget).unwrap_err();

    assert!(matches!(
        error.kind(),
        ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
            operation: "search_test",
            constructed: 1,
            evaluations: 1
        })
    ));
    assert_eq!(current_solutions, [vec![9]]);
    assert_eq!(budget.spent_ticks, 16);
}
