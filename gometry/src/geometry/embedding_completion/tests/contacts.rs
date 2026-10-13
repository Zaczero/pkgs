#[test]
fn distinct_known_identities_cannot_collapse_to_one_candidate_point() {
    let mut work = Unmetered;
    let graph = admit_known_graph(
        points(&[(0.0, 0.0), (1.0, 0.0)]),
        Vec::new(),
        vec![0, 1],
        &mut work,
    )
    .unwrap();
    let collapsed = points(&[(0.0, 0.0), (0.0, 0.0)]);
    assert!(
        admit_candidate(
            &graph,
            &admit_logical_layout(&graph, Vec::new(), None, &mut work).unwrap(),
            collapsed,
            &mut work
        )
        .unwrap()
        .is_none()
    );
}

#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "the recorder assertions pin point-contact progress and refusal before the next probe"
)]
fn native_point_contact_sweep_pays_no_survivor_probes() -> Result<()> {
    const OPERATION: &str = "native_point_contact_sweep";
    reset_cross_pair_probe_progress();
    let (result, charges) = {
        let mut budget = EvaluationBudget::new(OPERATION, 1)?;
        let mut work = SweepRecordingWork::new(&mut budget);
        let result = isolated_contact_result(&mut work)?;
        (result, work.charges)
    };
    assert!(result);
    assert_eq!(cross_pair_probe_progress(), CrossPairProbeProgress {
        attempted: 8,
        completed: 8,
    });
    let target = charges
        .into_iter()
        .find(|event| {
            event.progress
                == CrossPairProbeProgress {
                    attempted: 2,
                    completed: 1,
                }
        })
        .expect("the paid sweep charges a rejected point-edge probe");

    reset_cross_pair_probe_progress();
    let mut budget = budget_refusing_before(OPERATION, &target);
    let error = {
        let mut work = SweepRecordingWork::new(&mut budget);
        let error = isolated_contact_result(&mut work)
            .expect_err("the no-survivor sweep refuses before its second probe");
        assert_eq!(
            work.refused.map(|event| event.progress),
            Some(target.progress)
        );
        error
    };
    assert_sweep_limit(&error, OPERATION);
    assert_eq!(cross_pair_probe_progress(), target.progress);

    reset_cross_pair_probe_progress();
    assert!(isolated_contact_result(&mut Unmetered)?);
    let mut ordinary_budget = EvaluationBudget::new(OPERATION, 1)?;
    let ordinary_spend = ordinary_budget.spent_ticks_for_test();
    {
        let mut work = EvaluationOnly::new(&mut ordinary_budget);
        assert!(isolated_contact_result(&mut work)?);
    }
    assert_eq!(ordinary_budget.spent_ticks_for_test(), ordinary_spend);
    Ok(())
}

#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "the recorder assertions pin edge-contact progress and refusal before the next probe"
)]
fn native_edge_contact_sweep_pays_no_survivor_probes() -> Result<()> {
    const OPERATION: &str = "native_edge_contact_sweep";
    let coordinates = [(0.0, 0.0), (1.0, 0.0), (10.0, 0.0), (11.0, 0.0)];
    let edges = [[0, 1], [2, 3]];
    reset_cross_pair_probe_progress();
    let (result, charges) = {
        let mut budget = EvaluationBudget::new(OPERATION, 1)?;
        let mut work = SweepRecordingWork::new(&mut budget);
        let result = edge_contact_result(&coordinates, &edges, &mut work)?;
        (result, work.charges)
    };
    assert!(result);
    assert_eq!(cross_pair_probe_progress(), CrossPairProbeProgress {
        attempted: 4,
        completed: 4,
    });
    let target = charges
        .into_iter()
        .find(|event| {
            event.progress
                == CrossPairProbeProgress {
                    attempted: 2,
                    completed: 1,
                }
        })
        .expect("the paid self-sweep charges a rejected edge pair");

    reset_cross_pair_probe_progress();
    let mut budget = budget_refusing_before(OPERATION, &target);
    let error = {
        let mut work = SweepRecordingWork::new(&mut budget);
        let error = edge_contact_result(&coordinates, &edges, &mut work)
            .expect_err("the no-survivor self-sweep refuses before its second probe");
        assert_eq!(
            work.refused.map(|event| event.progress),
            Some(target.progress)
        );
        error
    };
    assert_sweep_limit(&error, OPERATION);
    assert_eq!(cross_pair_probe_progress(), target.progress);

    reset_cross_pair_probe_progress();
    assert!(edge_contact_result(&coordinates, &edges, &mut Unmetered)?);
    let mut ordinary_budget = EvaluationBudget::new(OPERATION, 1)?;
    let ordinary_spend = ordinary_budget.spent_ticks_for_test();
    {
        let mut work = EvaluationOnly::new(&mut ordinary_budget);
        assert!(edge_contact_result(&coordinates, &edges, &mut work)?);
    }
    assert_eq!(ordinary_budget.spent_ticks_for_test(), ordinary_spend);
    Ok(())
}

#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "the recorder assertions pin exact shared-contact work before a later-pair refusal"
)]
fn native_edge_contact_sweep_preserves_exact_charges_before_refusal() -> Result<()> {
    const OPERATION: &str = "native_edge_contact_exact";
    let coordinates = [(0.0, 0.0), (1.0, 1.0), (2.0, 0.0)];
    let edges = [[0, 1], [1, 2]];
    reset_cross_pair_probe_progress();
    let (result, charges) = {
        let mut budget = EvaluationBudget::new(OPERATION, 1)?;
        let mut work = SweepRecordingWork::new(&mut budget);
        let result = edge_contact_result(&coordinates, &edges, &mut work)?;
        (result, work.charges)
    };
    assert!(result);
    assert!(
        charges.iter().any(|event| {
            event.progress
                == CrossPairProbeProgress {
                    attempted: 2,
                    completed: 2,
                }
        }),
        "the shared-endpoint survivor runs its exact contact predicate"
    );
    let target = charges
        .into_iter()
        .find(|event| {
            event.progress
                == CrossPairProbeProgress {
                    attempted: 3,
                    completed: 2,
                }
        })
        .expect("the paid sweep charges the later rejected edge pair");

    reset_cross_pair_probe_progress();
    let mut budget = budget_refusing_before(OPERATION, &target);
    let error = {
        let mut work = SweepRecordingWork::new(&mut budget);
        let error = edge_contact_result(&coordinates, &edges, &mut work)
            .expect_err("later sweep probe refuses after the exact contact");
        assert_eq!(
            work.refused.map(|event| event.progress),
            Some(target.progress)
        );
        error
    };
    assert_sweep_limit(&error, OPERATION);
    assert_eq!(cross_pair_probe_progress(), target.progress);
    Ok(())
}

#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "the recorder assertions pin rational bounds work and refuse before bounds publication"
)]
fn native_contact_bounds_refuse_before_returning_bounds() -> Result<()> {
    const OPERATION: &str = "native_contact_bounds";
    let point = near_one_exact_chart_point();
    super::contacts::reset_contact_check_progress();
    let (expected, charges) = {
        let mut budget = EvaluationBudget::new(OPERATION, 1)?;
        let mut work = SweepRecordingWork::new(&mut budget);
        let expected = super::contacts::outward_bounds(&point, &mut work)?;
        (expected, work.charges)
    };
    assert_eq!(
        super::contacts::contact_check_progress().bounds_attempted,
        super::contacts::contact_check_progress().bounds_completed
    );
    let target = charges
        .into_iter()
        .find(|event| {
            event.contact_progress.bounds_attempted == 1
                && event.contact_progress.bounds_completed == 0
        })
        .expect("contact bounds forward rational rounding work");

    super::contacts::reset_contact_check_progress();
    let mut budget = budget_refusing_before(OPERATION, &target);
    let error = {
        let mut work = SweepRecordingWork::new(&mut budget);
        let error = super::contacts::outward_bounds(&point, &mut work)
            .expect_err("failed exact rounding cannot publish outward bounds");
        assert_eq!(
            work.refused.map(|event| event.contact_progress),
            Some(target.contact_progress)
        );
        error
    };
    assert_sweep_limit(&error, OPERATION);
    assert_eq!(
        super::contacts::contact_check_progress(),
        target.contact_progress
    );

    let expected = [
        expected.minx().to_bits(),
        expected.miny().to_bits(),
        expected.maxx().to_bits(),
        expected.maxy().to_bits(),
    ];
    let ordinary = point.outward_bounds()?;
    assert_eq!(ordinary.map(f64::to_bits), expected);
    let mut ordinary_budget = EvaluationBudget::new(OPERATION, 1)?;
    let ordinary_spend = ordinary_budget.spent_ticks_for_test();
    {
        let mut work = EvaluationOnly::new(&mut ordinary_budget);
        let bounds = super::contacts::outward_bounds(&point, &mut work)?;
        assert_eq!(
            [
                bounds.minx().to_bits(),
                bounds.miny().to_bits(),
                bounds.maxx().to_bits(),
                bounds.maxy().to_bits(),
            ],
            expected
        );
    }
    assert_eq!(ordinary_budget.spent_ticks_for_test(), ordinary_spend);
    Ok(())
}

#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "the recorder assertions pin endpoint identity work before returning the degenerate-edge result"
)]
fn native_contact_endpoint_identity_refuses_inside_exact_equality() -> Result<()> {
    const OPERATION: &str = "native_contact_identity";
    let point = near_one_exact_chart_point();
    let exact = exact_points(&[(point.x.clone(), point.y.clone()), (point.x, point.y)]);
    let edges = [[0, 1]];
    super::contacts::reset_contact_check_progress();
    let (result, charges) = {
        let mut budget = EvaluationBudget::new(OPERATION, 1)?;
        let mut work = SweepRecordingWork::new(&mut budget);
        let result = edges_have_only_vertex_contacts(&exact, &edges, &mut work)?;
        (result, work.charges)
    };
    assert!(!result);
    let target = charges
        .into_iter()
        .find(|event| {
            event.contact_progress.identity_attempted == 1
                && event.contact_progress.identity_completed == 0
        })
        .expect("degenerate endpoint identity uses metered rational equality");

    super::contacts::reset_contact_check_progress();
    let mut budget = budget_refusing_before(OPERATION, &target);
    let error = {
        let mut work = SweepRecordingWork::new(&mut budget);
        let error = edges_have_only_vertex_contacts(&exact, &edges, &mut work)
            .expect_err("endpoint equality refuses before the degenerate-edge result");
        assert_eq!(
            work.refused.map(|event| event.contact_progress),
            Some(target.contact_progress)
        );
        error
    };
    assert_sweep_limit(&error, OPERATION);
    assert_eq!(
        super::contacts::contact_check_progress(),
        target.contact_progress
    );

    assert!(!edges_have_only_vertex_contacts(
        &exact,
        &edges,
        &mut Unmetered
    )?);
    let mut ordinary_budget = EvaluationBudget::new(OPERATION, 1)?;
    let ordinary_spend = ordinary_budget.spent_ticks_for_test();
    {
        let mut work = EvaluationOnly::new(&mut ordinary_budget);
        assert!(!edges_have_only_vertex_contacts(&exact, &edges, &mut work)?);
    }
    assert_eq!(ordinary_budget.spent_ticks_for_test(), ordinary_spend);
    Ok(())
}

#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "assertion is the exact degree-zero point-contact admission regression"
)]
fn candidate_admission_keeps_degree_zero_point_contacts_with_the_reference() -> Result<()> {
    let half_ulp =
        ExactRational::new(ExactDyadic::one(), ExactDyadic::from_i64(1_i64 << 54)).unwrap();
    let reference = exact_points(&[
        (ExactRational::zero(), ExactRational::one()),
        (ExactRational::from_f64(2.0), ExactRational::one()),
        (
            ExactRational::one(),
            ExactRational::one().try_add(&half_ulp).unwrap(),
        ),
    ]);
    let nearest = points(&[(0.0, 1.0), (2.0, 1.0), (1.0, 1.0)]);
    let mut work = Unmetered;
    let graph = admit_known_graph(reference, vec![[0, 1]], vec![0, 1, 2], &mut work)?;
    let layout = admit_logical_layout(&graph, Vec::new(), None, &mut work)?;

    assert!(
        admit_candidate(&graph, &layout, nearest, &mut work)?.is_none(),
        "the nearest image of degree-zero P contacts AB and must not be admitted"
    );
    Ok(())
}

#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "assertions pin query refusal before any later progress"
)]
fn output_point_query_refusal_stops_before_progress() -> Result<()> {
    let mut setup_work = Unmetered;
    let graph = admit_known_graph(
        points(&[(0.0, 1.0), (2.0, 1.0), (1.0, 1.0)]),
        vec![[0, 1]],
        vec![0, 1, 2],
        &mut setup_work,
    )?;
    let budget = EvaluationBudget::with_remaining_ticks_for_test("point contacts", 0, 1)?;
    let mut recorder = EvaluationRecorder {
        budget,
        successful_charges: 0,
        refusals: 0,
        charges_after_refusal: 0,
        refused: false,
    };

    let result = graph.with_output_point_contacts(&[0], &mut recorder);
    assert!(matches!(
        result,
        Err(error)
            if matches!(
                error.kind(),
                crate::error::ErrorKind::Geometry(
                    crate::geometry::GeometryErrorKind::ExactEmbeddingSearchLimit { .. }
                )
            )
    ));
    assert_eq!(recorder.successful_charges, 1);
    assert_eq!(recorder.refusals, 1);
    assert_eq!(recorder.charges_after_refusal, 0);
    Ok(())
}

#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "assertion pins the contact decision under EvaluationOnly work erasure"
)]
fn evaluation_only_erases_point_contact_work() -> Result<()> {
    let half_ulp =
        ExactRational::new(ExactDyadic::one(), ExactDyadic::from_i64(1_i64 << 54)).unwrap();
    let reference = exact_points(&[
        (ExactRational::zero(), ExactRational::one()),
        (ExactRational::from_f64(2.0), ExactRational::one()),
        (
            ExactRational::one(),
            ExactRational::one().try_add(&half_ulp).unwrap(),
        ),
    ]);
    let mut budget =
        EvaluationBudget::with_remaining_ticks_for_test("ordinary point contacts", 0, 0)?;
    let mut ordinary_work = EvaluationOnly::new(&mut budget);
    let graph = admit_known_graph(reference, vec![[0, 1]], vec![0, 1, 2], &mut ordinary_work)?;
    let layout = admit_logical_layout(&graph, Vec::new(), None, &mut ordinary_work)?;

    assert!(
        admit_candidate(
            &graph,
            &layout,
            points(&[(0.0, 1.0), (2.0, 1.0), (1.0, 1.0)]),
            &mut ordinary_work,
        )?
        .is_none(),
        "EvaluationOnly preserves the same geometric decision with zero raw-work allowance"
    );
    Ok(())
}
