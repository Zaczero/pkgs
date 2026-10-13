
#[test]
fn one_materialized_feature_family_counts_once_across_retries() {
    let mut registry = FeatureFamilyBounds {
        bounds: vec![FeatureFamilyBound {
            publication: 1,
            native_cuts: 2,
            native_cut_ranges: Vec::new(),
        }],
        region_families: Vec::new(),
    }
    .into_registry(2);
    let family = FeatureFamily {
        parent: FeatureFamilyParent::Fragment(FragmentRecipeId(0)),
        producer: SampleProducerKind::NativeCut,
        occurrence: 0,
    };

    let mut work = Unmetered;
    for retry in 0..100 {
        assert_eq!(registry.register(family, &mut work).unwrap(), retry == 0);
    }
    assert_eq!(registry.additional_constructed(), 1);

    let mut budget = EvaluationBudget::new("native completion", 2).unwrap();
    budget.charge_into_error(7).unwrap();
    registry.synchronize_budget(&mut budget).unwrap();
    for _ in 0..100 {
        registry.register(family, &mut budget).unwrap();
        registry.synchronize_budget(&mut budget).unwrap();
    }
    assert_eq!(budget.constructed(), 3);
}

#[test]
fn native_cut_occurrences_follow_disjoint_prepared_period_ranges() {
    let registry = FeatureFamilyRegistry {
        bounds: vec![FeatureFamilyBound {
            publication: 0,
            native_cuts: 4,
            native_cut_ranges: vec![NativeCutOccurrenceRange {
                intervals: vec![
                    NativeCutOccurrenceInterval {
                        low_period: 0,
                        high_period: 1,
                        family_base: 0,
                    },
                    NativeCutOccurrenceInterval {
                        low_period: 4,
                        high_period: 5,
                        family_base: 2,
                    },
                ],
            }],
        }],
        region_families: Vec::new(),
        families: FallibleAvl::with_capacity(0),
        source_base: 0,
    };
    let mut work = Unmetered;
    for (period, occurrence) in [(0, 0), (1, 1), (4, 2), (5, 3)] {
        assert_eq!(
            registry
                .native_cut_occurrence(FragmentRecipeId(0), 0, period, &mut work,)
                .unwrap(),
            occurrence,
        );
    }
    assert!(
        registry
            .native_cut_occurrence(FragmentRecipeId(0), 0, 2, &mut work)
            .is_err(),
        "a period not admitted by any prepared endpoint pair has no family identity",
    );
}

#[test]
fn region_feature_registration_is_limited_to_the_frozen_template_universe() {
    let admitted = FeatureFamily {
        parent: FeatureFamilyParent::Region(7),
        producer: SampleProducerKind::WholeSphereCover,
        occurrence: 0,
    };
    let mut registry = FeatureFamilyRegistry {
        bounds: Vec::new(),
        region_families: vec![admitted],
        families: FallibleAvl::with_capacity(0),
        source_base: 0,
    };
    let mut work = Unmetered;
    assert!(registry.register(admitted, &mut work).unwrap());
    assert!(!registry.register(admitted, &mut work).unwrap());
    registry
        .register(
            FeatureFamily {
                parent: FeatureFamilyParent::Region(8),
                producer: SampleProducerKind::WholeSphereCover,
                occurrence: 0,
            },
            &mut work,
        )
        .unwrap_err();
    registry
        .register(
            FeatureFamily {
                occurrence: 10,
                ..admitted
            },
            &mut work,
        )
        .unwrap_err();
}
