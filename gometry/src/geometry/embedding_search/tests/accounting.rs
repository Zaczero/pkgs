#[test]
fn budget_grows_with_the_number_of_constructed_nodes() {
    let mut budget = EvaluationBudget::new("search_test", 17).unwrap();
    let limit = BASE_EVALUATION_LIMIT + 17 * EVALUATIONS_PER_CONSTRUCTED_NODE;
    budget.charge_evaluations(limit).unwrap();
    let error = budget.charge_evaluations(1).unwrap_err();
    assert!(matches!(
        error.kind(),
        ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
            operation: "search_test",
            constructed: 17,
            evaluations
        }) if *evaluations == limit
    ));
}

#[test]
fn published_leaf_budget_has_half_at_and_above_boundary_controls() {
    let mut budget = EvaluationBudget::new("union", 0).unwrap();
    budget.set_published_leaves(5_472).unwrap();
    budget.set_constructed(149).unwrap();
    let limit = 1_775_712;
    assert_eq!(budget.limit, limit);

    budget.record_completed_evaluations(limit / 2).unwrap();
    budget
        .record_completed_evaluations(limit - limit / 2)
        .unwrap();
    let error = budget.record_completed_evaluations(1).unwrap_err();
    assert!(matches!(
        error.kind(),
        ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
            operation: "union",
            constructed: 149,
            evaluations
        }) if *evaluations == limit + 1
    ));
}

#[test]
fn one_evaluation_and_sixteen_raw_ticks_share_the_same_balance() {
    let mut raw = EvaluationBudget::new("search_test", 1).unwrap();
    for _ in 0..16 {
        <EvaluationBudget as Work>::charge(&mut raw, 1).unwrap();
    }

    let mut evaluation = EvaluationBudget::new("search_test", 1).unwrap();
    evaluation.charge_evaluations(1).unwrap();
    assert_eq!(raw.spent_ticks, evaluation.spent_ticks);
    assert_eq!(raw.spent_ticks, 16);

    let mut raw_batch = EvaluationBudget::new("search_test", 1).unwrap();
    <EvaluationBudget as Work>::charge(&mut raw_batch, 256).unwrap();
    let mut evaluation_batch = EvaluationBudget::new("search_test", 1).unwrap();
    for _ in 0..16 {
        evaluation_batch.charge_evaluations(1).unwrap();
    }
    assert_eq!(raw_batch.spent_ticks, evaluation_batch.spent_ticks);
    assert_eq!(raw_batch.spent_ticks, 256);
}

#[test]
fn evaluation_only_account_ignores_raw_ticks_and_billing_products() {
    let mut budget = EvaluationBudget::new("search_test", 1).unwrap();
    {
        let mut account = EvaluationOnly::new(&mut budget);
        let raw: std::result::Result<(), Infallible> = account.charge(u64::MAX);
        raw.unwrap();
        account.charge_into_error(u64::MAX).unwrap();
        account
            .charge_product_into_error(usize::MAX, usize::MAX)
            .unwrap();
        account.charge_evaluations(1).unwrap();
    }

    assert_eq!(budget.spent_ticks, 16);
    let mut unmetered = Unmetered;
    let raw: std::result::Result<(), Infallible> = unmetered.charge_product(usize::MAX, usize::MAX);
    raw.unwrap();
}

#[test]
fn equal_primary_bounds_keep_staging_order_for_every_work_policy() {
    let bounds = equal_primary_sweep_bounds();
    let expected = [(0, 20), (0, 39), (20, 39)];

    let mut ordinary = Vec::new();
    let flow = crate::geometry::for_each_overlapping_bounds_pair(&bounds, |left, right| {
        ordinary.push((left, right));
        ControlFlow::<()>::Continue(())
    })
    .unwrap();
    assert_eq!(flow, ControlFlow::Continue(()));
    assert_eq!(ordinary, expected);

    let mut unmetered = Unmetered;
    assert_eq!(stable_sweep_pairs(&bounds, &mut unmetered), expected);

    let mut erased_budget = EvaluationBudget::new("sweep_test", 1).unwrap();
    let erased = {
        let mut account = EvaluationOnly::new(&mut erased_budget);
        stable_sweep_pairs(&bounds, &mut account)
    };
    assert_eq!(erased, expected);
    assert_eq!(erased_budget.spent_ticks, 0);

    let mut budget = EvaluationBudget::new("sweep_test", 1).unwrap();
    assert_eq!(stable_sweep_pairs(&bounds, &mut budget), expected);
    assert!(budget.spent_ticks > 0);
}

#[test]
fn equal_primary_cross_bounds_keep_left_then_right_staging_order() {
    let left = [Bounds::new_raw(0.0, 0.0, 1.0, 1.0); 3];
    let right = [Bounds::new_raw(0.0, 0.0, 1.0, 1.0); 3];
    let expected = [
        (0, 0),
        (0, 1),
        (0, 2),
        (1, 0),
        (1, 1),
        (1, 2),
        (2, 0),
        (2, 1),
        (2, 2),
    ];

    let mut ordinary = Vec::new();
    let flow = crate::geometry::for_each_overlapping_bounds_cross_pair(&left, &right, |a, b| {
        ordinary.push((a, b));
        ControlFlow::<()>::Continue(())
    })
    .unwrap();
    assert_eq!(flow, ControlFlow::Continue(()));
    assert_eq!(ordinary, expected);

    let mut unmetered = Unmetered;
    assert_eq!(
        stable_cross_sweep_pairs(&left, &right, &mut unmetered),
        expected
    );
    let mut erased_budget = EvaluationBudget::new("sweep_test", 1).unwrap();
    {
        let mut account = EvaluationOnly::new(&mut erased_budget);
        assert_eq!(
            stable_cross_sweep_pairs(&left, &right, &mut account),
            expected
        );
    }
    let mut budget = EvaluationBudget::new("sweep_test", 1).unwrap();
    assert_eq!(
        stable_cross_sweep_pairs(&left, &right, &mut budget),
        expected
    );
}

#[test]
fn stable_sweep_break_and_callback_error_stop_at_the_same_pair_prefix() {
    let bounds = equal_primary_sweep_bounds();

    let mut ordinary_pairs = Vec::new();
    let ordinary_flow =
        crate::geometry::for_each_overlapping_bounds_pair(&bounds, |left, right| {
            ordinary_pairs.push((left, right));
            if ordinary_pairs.len() == 2 {
                ControlFlow::Break("second stable pair".to_owned())
            } else {
                ControlFlow::Continue(())
            }
        })
        .unwrap();
    assert_eq!(
        ordinary_flow,
        ControlFlow::Break("second stable pair".to_owned())
    );
    assert_eq!(ordinary_pairs, [(0, 20), (0, 39)]);

    let mut unmetered = Unmetered;
    assert_stable_break(&bounds, &mut unmetered);
    assert_sweep_error_stops_after_second_pair(&bounds, &mut unmetered);

    let mut erased_budget = EvaluationBudget::new("sweep_test", 1).unwrap();
    {
        let mut account = EvaluationOnly::new(&mut erased_budget);
        assert_stable_break(&bounds, &mut account);
        assert_sweep_error_stops_after_second_pair(&bounds, &mut account);
    }
    assert_eq!(erased_budget.spent_ticks, 0);

    let mut budget = EvaluationBudget::new("sweep_test", 1).unwrap();
    assert_stable_break(&bounds, &mut budget);
    assert_sweep_error_stops_after_second_pair(&bounds, &mut budget);
}

#[test]
fn sweep_callback_reborrow_uses_the_same_exhausted_account() {
    let bounds = equal_primary_sweep_bounds();
    let mut baseline = EvaluationBudget::new("sweep_test", 1).unwrap();
    let mut before_callback = None;
    let flow = sweep_for_each_overlapping_bounds_pair_with_work(
        &bounds,
        &mut baseline,
        |left, right, work| {
            assert_eq!((left, right), (0, 20));
            before_callback = Some(work.spent_ticks);
            Ok(ControlFlow::Break(()))
        },
    )
    .unwrap();
    assert_eq!(flow, ControlFlow::Break(()));
    let before_callback = before_callback.unwrap();

    let allowance_after_sweep = before_callback.wrapping_add(1);
    let mut budget = EvaluationBudget::new("sweep_test", 1).unwrap();
    budget.limit =
        u64::try_from(allowance_after_sweep.div_ceil(WORK_TICKS_PER_EVALUATION)).unwrap();
    budget.spent_ticks = budget.limit_ticks().wrapping_sub(allowance_after_sweep);
    let mut callback_count = 0;
    let mut callback_progress = 0;

    let error =
        sweep_for_each_overlapping_bounds_pair_with_work(&bounds, &mut budget, |_, _, work| {
            callback_count += 1;
            work.charge_into_error(1)?;
            callback_progress += 1;
            work.charge_into_error(1)?;
            callback_progress += 1;
            Ok(ControlFlow::<()>::Continue(()))
        })
        .unwrap_err();

    assert!(matches!(
        error.kind(),
        ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
            operation: "sweep_test",
            constructed: 1,
            ..
        })
    ));
    assert_eq!(callback_count, 1);
    assert_eq!(callback_progress, 1);
    assert_eq!(budget.spent_ticks, budget.limit_ticks());
}

#[test]
fn sweep_refuses_before_the_next_monotone_source_probe() {
    let pool = [
        Segment {
            start: XY::new(0.0, 0.0),
            end: XY::new(1.0, 1.0),
        },
        Segment {
            start: XY::new(3.0, 0.0),
            end: XY::new(4.0, 1.0),
        },
    ];
    let chain_calls = Cell::new(0);
    let candidate_calls = Cell::new(0);
    let mut budget = EvaluationBudget::new("sweep_test", 1).unwrap();
    budget.limit = 0;

    let error = sweep_for_each_candidate_pair_with_work(
        &pool,
        |_| {
            chain_calls.set(chain_calls.get() + 1);
            0
        },
        &mut budget,
        |_, _, _work| {
            candidate_calls.set(candidate_calls.get() + 1);
            Ok(ControlFlow::<()>::Continue(()))
        },
    )
    .unwrap_err();

    assert!(matches!(
        error.kind(),
        ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
            operation: "sweep_test",
            constructed: 1,
            evaluations: 0
        })
    ));
    assert_eq!(chain_calls.get(), 0);
    assert_eq!(candidate_calls.get(), 0);
    assert_eq!(budget.spent_ticks, 0);
}

#[test]
fn bounds_brute_refusal_precedes_filter_and_callback() {
    let left = [Bounds::new_raw(0.0, 0.0, 1.0, 1.0)];
    let right = [Bounds::new_raw(0.0, 0.0, 1.0, 1.0)];
    let mut budget = EvaluationBudget::new("sweep_test", 1).unwrap();
    budget.limit = 1;
    budget.spent_ticks = 16;
    let mut callbacks = 0;

    let error = sweep_for_each_overlapping_bounds_cross_pair_with_work(
        &left,
        &right,
        &mut budget,
        |_, _, _work| {
            callbacks += 1;
            Ok(ControlFlow::<()>::Continue(()))
        },
    )
    .unwrap_err();

    assert!(matches!(
        error.kind(),
        ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
            operation: "sweep_test",
            constructed: 1,
            evaluations: 1
        })
    ));
    assert_eq!(callbacks, 0);
    assert_eq!(budget.spent_ticks, 16);
}

#[test]
fn banded_window_refusal_precedes_the_first_4096_entry_callback() {
    let bounds = vec![Bounds::new_raw(0.0, 0.0, 1.0, 1.0); 4096];
    let mut budget = EvaluationBudget::new("sweep_test", 1).unwrap();
    // The funded entry, stable-sort, two extent-fold, CSR, band-row and
    // left-row phases total 126,736 ticks before the first right window.
    let remaining_ticks = 126_736_u128;
    budget.limit = u64::try_from(remaining_ticks.div_ceil(WORK_TICKS_PER_EVALUATION)).unwrap();
    budget.spent_ticks = budget.limit_ticks().wrapping_sub(remaining_ticks);
    let mut callbacks = 0;

    let error =
        sweep_for_each_overlapping_bounds_pair_with_work(&bounds, &mut budget, |_, _, _work| {
            callbacks += 1;
            Ok(ControlFlow::<()>::Break(()))
        })
        .unwrap_err();

    assert!(matches!(
        error.kind(),
        ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
            operation: "sweep_test",
            constructed: 1,
            evaluations: 7_921
        })
    ));
    assert_eq!(callbacks, 0);
    assert_eq!(budget.spent_ticks, 126_736);
}

#[test]
fn unaffordable_billing_product_is_a_native_search_limit() {
    let mut budget = EvaluationBudget::new("search_test", 1).unwrap();

    let error = <EvaluationBudget as Work>::charge_product(&mut budget, usize::MAX, usize::MAX)
        .unwrap_err();

    assert!(matches!(
        error.kind(),
        ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
            operation: "search_test",
            constructed: 1,
            evaluations: 0
        })
    ));
    assert_eq!(budget.spent_ticks, 0);
}

#[test]
fn exact_product_refuses_before_multilimb_cells_after_reservation() {
    let left = ExactDyadic::from_parts(false, vec![u64::MAX; 9], 0);
    let right = ExactDyadic::from_parts(false, vec![u64::MAX; 10], 0);
    let mut budget = EvaluationBudget::new("exact_product", 1).unwrap();
    budget.limit = 6;

    let error = left.try_product_with_work(&right, &mut budget).unwrap_err();

    assert!(matches!(
        error.kind(),
        ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
            operation: "exact_product",
            constructed: 1,
            evaluations: 2
        })
    ));
    assert_eq!(budget.spent_ticks, 19);
}

#[test]
fn exact_division_refuses_before_the_first_algorithm_d_digit_cells() {
    const TOP: u64 = 1 << 63;
    let dividend = [0, 0, TOP, TOP - 1];
    let divisor = [1, 0, TOP];
    let mut budget = EvaluationBudget::new("exact_division", 1).unwrap();
    budget.limit = 2;
    budget.spent_ticks = 8;

    let error =
        ExactDyadic::div_rem_unsigned_with_work(&dividend, &divisor, &mut budget).unwrap_err();

    assert!(matches!(
        error.kind(),
        ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
            operation: "exact_division",
            constructed: 1,
            evaluations: 2
        })
    ));
    assert_eq!(budget.spent_ticks, 30);
}

#[test]
fn exact_inline_gcd_funds_each_binary_iteration() {
    const PRIME: u64 = 0xFFFF_FFFF_FFFF_FFC5;
    let left = ExactDyadic::try_from_u128(false, u128::from(PRIME).wrapping_mul(3), 5).unwrap();
    let right = ExactDyadic::try_from_u128(false, u128::from(PRIME).wrapping_mul(5), -3).unwrap();
    let mut budget = EvaluationBudget::new("exact_gcd", 1).unwrap();
    budget.limit = 1;
    budget.spent_ticks = 15;

    let error = left.gcd_with_work(&right, &mut budget).unwrap_err();

    assert!(matches!(
        error.kind(),
        ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
            operation: "exact_gcd",
            constructed: 1,
            evaluations: 1
        })
    ));
    assert_eq!(budget.spent_ticks, 16);
}

#[test]
fn exact_ratio_rounding_refuses_before_multilimb_product_cells() {
    let numerator = ExactDyadic::one();
    let denominator = ExactDyadic::from_parts(false, vec![1, 0, 1], 0);
    let mut budget = EvaluationBudget::new("exact_round", 1).unwrap();
    budget.limit = 1;
    budget.spent_ticks = 6;

    let error = round_ratio_with_work(&numerator, &denominator, &mut budget).unwrap_err();

    assert!(matches!(
        error.kind(),
        ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
            operation: "exact_round",
            constructed: 1,
            evaluations: 1
        })
    ));
    assert_eq!(budget.spent_ticks, 14);
}

#[test]
fn exact_primitives_share_native_fuel_and_evaluation_only_erases_work() {
    let left = ExactDyadic::from_parts(false, vec![u64::MAX; 9], 0);
    let right = ExactDyadic::from_parts(false, vec![u64::MAX; 10], 0);
    let expected_product = [
        1,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        u64::MAX,
        u64::MAX - 1,
        u64::MAX,
        u64::MAX,
        u64::MAX,
        u64::MAX,
        u64::MAX,
        u64::MAX,
        u64::MAX,
        u64::MAX,
    ];
    let numerator = ExactDyadic::from_f64(615.0);
    let denominator = ExactDyadic::from_f64(9.0);
    let one = ExactDyadic::one();
    let three = ExactDyadic::from_f64(3.0);

    let mut native = EvaluationBudget::new("exact_primitives", 1).unwrap();
    let native_product = left.try_product_with_work(&right, &mut native).unwrap();
    let native_ratio =
        ExactRational::new_with_work(numerator.clone(), denominator.clone(), &mut native).unwrap();
    let native_round = round_ratio_with_work(&one, &three, &mut native).unwrap();

    assert_eq!(native_product.limbs.as_slice(), expected_product);
    let (normalized_numerator, normalized_denominator) = native_ratio.parts();
    assert_eq!(normalized_numerator.limbs.as_slice(), [205]);
    assert_eq!(normalized_denominator.limbs.as_slice(), [3]);
    assert_eq!(normalized_numerator.exponent, 0);
    assert_eq!(normalized_denominator.exponent, 0);
    assert_eq!(native_round.to_bits(), 0x3FD5_5555_5555_5555);
    assert!(native.spent_ticks > 0);
    assert!(native.spent_ticks <= native.limit_ticks());

    let mut depleted = EvaluationBudget::new("exact_erasure", 1).unwrap();
    depleted.limit = 1;
    depleted.spent_ticks = depleted.limit_ticks();
    let spent_before_erasure = depleted.spent_ticks;
    let (erased_product, erased_ratio, erased_round) = {
        let mut erased = EvaluationOnly::new(&mut depleted);
        let product = left.try_product_with_work(&right, &mut erased).unwrap();
        let ratio = ExactRational::new_with_work(numerator, denominator, &mut erased).unwrap();
        let rounded = round_ratio_with_work(&one, &three, &mut erased).unwrap();
        (product, ratio, rounded)
    };

    assert_eq!(erased_product.limbs.as_slice(), expected_product);
    let (normalized_numerator, normalized_denominator) = erased_ratio.parts();
    assert_eq!(normalized_numerator.limbs.as_slice(), [205]);
    assert_eq!(normalized_denominator.limbs.as_slice(), [3]);
    assert_eq!(erased_round.to_bits(), 0x3FD5_5555_5555_5555);
    assert_eq!(depleted.spent_ticks, spent_before_erasure);
}

#[test]
fn native_budget_refuses_the_first_stable_merge_comparison() {
    let mut items: Vec<_> = (16_u8..32).chain(0..16).map(|key| (key, key)).collect();
    let original = items.clone();
    let mut comparisons = 0;
    let mut budget = EvaluationBudget::new("search_test", 1).unwrap();
    budget.limit = 15;
    budget.spent_ticks = 146;

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
            evaluations: 15
        })
    ));
    assert_eq!(comparisons, 30);
    assert_eq!(items, original);
    assert_eq!(budget.spent_ticks, 240);
}

#[test]
fn stable_sort_comparator_uses_the_same_native_budget_before_its_progress() {
    let mut items = [(2_u8, 'A'), (1, 'B')];
    let original = items;
    let mut comparator_progress = 0;
    let mut budget = EvaluationBudget::new("search_test", 1).unwrap();
    budget.limit = 1;
    budget.spent_ticks = 14;

    let error = try_sort_by_with_work(&mut items, &mut budget, |left, right, work| {
        work.charge_into_error(2)?;
        comparator_progress += 1;
        Ok(left.0.cmp(&right.0))
    })
    .unwrap_err();

    assert!(matches!(
        error.kind(),
        ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
            operation: "search_test",
            constructed: 1,
            evaluations: 1
        })
    ));
    assert_eq!(comparator_progress, 0);
    assert_eq!(items, original);
    assert_eq!(budget.spent_ticks, 15);
}

#[test]
fn merge_comparator_uses_the_same_native_budget_before_its_progress() {
    let mut items: Vec<_> = (16_u8..32).chain(0..16).map(|key| (key, key)).collect();
    let original = items.clone();
    let mut merge_callbacks = 0;
    let mut comparator_progress = 0;
    let mut budget = EvaluationBudget::new("search_test", 1).unwrap();
    budget.limit = 15;
    budget.spent_ticks = 144;

    let error = try_sort_by_with_work(&mut items, &mut budget, |left, right, work| {
        if (*left == (16, 16)) && (*right == (0, 0)) {
            merge_callbacks += 1;
            work.charge_into_error(2)?;
            comparator_progress += 1;
        }
        Ok(left.0.cmp(&right.0))
    })
    .unwrap_err();

    assert!(matches!(
        error.kind(),
        ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
            operation: "search_test",
            constructed: 1,
            evaluations: 15
        })
    ));
    assert_eq!(merge_callbacks, 1);
    assert_eq!(comparator_progress, 0);
    assert_eq!(items, original);
    assert_eq!(budget.spent_ticks, 239);
}

#[test]
fn native_budget_refuses_the_merge_output_slot_after_its_comparison() {
    let mut items: Vec<_> = (16_u8..32).chain(0..16).map(|key| (key, key)).collect();
    let original = items.clone();
    let mut comparisons = 0;
    let mut budget = EvaluationBudget::new("search_test", 1).unwrap();
    budget.limit = 15;
    budget.spent_ticks = 145;

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
            evaluations: 15
        })
    ));
    assert_eq!(comparisons, 31);
    assert_eq!(items, original);
    assert_eq!(budget.spent_ticks, 240);
}

#[test]
fn raw_tick_refusal_leaves_the_shared_balance_unchanged() {
    let mut budget = EvaluationBudget::new("search_test", 1).unwrap();
    budget.limit = 1;
    for _ in 0..15 {
        <EvaluationBudget as Work>::charge(&mut budget, 1).unwrap();
    }
    assert_eq!(budget.spent_ticks, 15);
    <EvaluationBudget as Work>::charge(&mut budget, 1).unwrap();

    let error = <EvaluationBudget as Work>::charge(&mut budget, 1).unwrap_err();

    assert!(matches!(
        error.kind(),
        ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
            operation: "search_test",
            constructed: 1,
            evaluations: 1
        })
    ));
    assert_eq!(budget.spent_ticks, 16);
}

#[test]
fn native_source_arc_quadrature_funds_refinement_before_progress() {
    const OPERATION: &str = "quadrature_source_arc";
    let runtime =
        crate::crs::runtime_state_snapshot().expect("the test runtime configuration is available");
    crate::crs::with_ellipsoid_metric_at("EPSG:4326", &[], &runtime, |metric| {
        let frame = crate::geometry::ChartMetric::chart_frame(metric);
        let source = [
            crate::geometry::Point::new_axes(
                0.0,
                0.0,
                crate::geometry::ZOrdinate(Some(7.0)),
                crate::geometry::MOrdinate(Some(101.0)),
            )?,
            crate::geometry::Point::new_axes(
                179.0,
                89.9,
                crate::geometry::ZOrdinate(Some(47.0)),
                crate::geometry::MOrdinate(Some(701.0)),
            )?,
        ];
        let longitude = frame
            .longitude()
            .chart_connection(source[0].x, source[1].x)
            .map_err(|error| crate::crs::CrsError::invalid(error.to_string()))?
            .retained();
        let parameter = 0.5;
        let axes = crate::geometry::CoordinateAxes::XYZM;

        crate::crs::reset_quadrature_test_progress();
        let mut metered_budget = EvaluationBudget::new(OPERATION, 1)?;
        let (metered, successful_charges) = {
            let mut work = RecordingWork {
                budget: &mut metered_budget,
                successful: Vec::new(),
            };
            let sample = native_source_sample(
                metric, frame, &source, longitude, parameter, axes, &mut work,
            )?;
            (sample, std::mem::take(&mut work.successful))
        };
        let metered_progress = crate::crs::quadrature_test_progress();
        assert!(
            metered_progress.max_refinements >= 2,
            "the real source-arc path refined only {} times in one integration",
            metered_progress.max_refinements
        );
        assert!(metered_budget.spent_ticks > 0);

        let actual_z = metered.z.0.expect("XYZM sampling carries Z");
        let expected_z = 31.528_758_192_036_157;
        assert!(
            (actual_z - expected_z).abs() <= 1.0e-12,
            "Z oracle: actual={actual_z:.17}, expected={expected_z:.17}"
        );
        let actual_m = metered.m.0.expect("XYZM sampling carries M");
        let expected_m = 468.931_372_880_542_37;
        assert!(
            (actual_m - expected_m).abs() <= 1.0e-11,
            "M oracle: actual={actual_m:.17}, expected={expected_m:.17}"
        );

        crate::crs::reset_quadrature_test_progress();
        let mut ordinary_work = Unmetered;
        let ordinary = native_source_sample(
            metric,
            frame,
            &source,
            longitude,
            parameter,
            axes,
            &mut ordinary_work,
        )?;
        assert_eq!(metered.y.to_bits(), ordinary.y.to_bits());
        assert_eq!(
            metered.z.0.map(f64::to_bits),
            ordinary.z.0.map(f64::to_bits)
        );
        assert_eq!(
            metered.m.0.map(f64::to_bits),
            ordinary.m.0.map(f64::to_bits)
        );

        let funded_growths = successful_charges
            .iter()
            .filter(|charge| charge.progress.growth_requests > charge.progress.growths)
            .count();
        assert_eq!(
            u32::try_from(funded_growths).expect("growth count fits the interval cap"),
            metered_progress.growths,
            "each interval growth must follow a successful raw commit debit"
        );
        let scan_charge = successful_charges
            .iter()
            .copied()
            .find(|charge| charge.progress.seeds == 1 && charge.progress.scans == 0)
            .expect("the real source path charges the largest-error scan");
        let commit_charge = successful_charges
            .iter()
            .copied()
            .find(|charge| {
                charge.progress.child_pairs > charge.progress.refinements
                    && charge.progress.growth_requests > charge.progress.growths
            })
            .expect("the real source path charges a refinement that requires growth");

        let removal_charge = successful_charges
            .iter()
            .copied()
            .find(|charge| charge.progress.scans == 1 && charge.progress.removals == 0)
            .expect("the real source path charges before removing the largest interval");
        for (charge, action) in [
            (&scan_charge, "before the interval scan"),
            (&removal_charge, "before interval removal"),
            (&commit_charge, "before the refinement commit"),
        ] {
            assert_raw_budget_refuses_before_progress(OPERATION, charge, action, |budget| {
                let mut work = RecordingWork {
                    budget,
                    successful: Vec::new(),
                };
                native_source_sample(
                    metric, frame, &source, longitude, parameter, axes, &mut work,
                )
            });
        }
        Ok(())
    })
    .expect("EPSG:4326 resolves the source-arc test metric");
}
