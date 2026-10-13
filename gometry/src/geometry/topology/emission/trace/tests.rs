#[test]
fn written_sample_splits_the_original_root_parameter_trace() {
    let one = ExactRational::one();
    let zero = ExactRational::zero();
    let half = ExactRational::from_f64(0.5);
    let quarter = ExactRational::from_f64(0.25);
    let three_quarters = ExactRational::from_f64(0.75);
    let recipe = FragmentRecipe {
        id: FragmentRecipeId(0),
        endpoints: [QPortId(4), QPortId(9)],
        ranges: vec![DirectedSourceRange {
            source_edge: SourceEdgeId(3),
            range: ExactSourceRange {
                start: quarter,
                end: three_quarters,
            },
            emitted: true,
            producer_start: zero,
            producer_end: one,
            endpoint_only: false,
        }],
        exact_displacement: ExactRational::from_f64(10.0),
        use_kind: SelectedUse::Curve,
    };
    let recipes = FragmentRecipes {
        values: vec![recipe],
        walks: Vec::new(),
    };
    let mut work = Unmetered;
    let original = recipes
        .source_trace(
            &RecipeWalk {
                owner: RecipeWalkOwner::Line(0),
                fragments: vec![FragmentRecipeId(0)],
            },
            &mut work,
        )
        .unwrap();
    let split = original
        .split_at_samples(
            &[SourceTraceSample {
                edge_index: 0,
                local_parameter: half.clone(),
                written_vertex: 1,
            }],
            &mut work,
        )
        .unwrap();

    assert_eq!(split.rows.edge_count(), 2);
    assert_eq!(split.rows.owner_count(), 2);
    assert_eq!(split.samples.len(), 1);
    assert_eq!(split.samples[0].written_vertex, 1);
    assert!(split.samples[0].has_source);
    assert!(!split.samples[0].endpoint_only);
    assert_eq!(split.samples[0].request_row.index(), 0);
    let sample_request = split
        .rows
        .sample_requests_at(0, &mut work)
        .unwrap()
        .into_iter()
        .next()
        .ok_or_else(|| GeometryErrorKind::Invalid("sample lost its root request".into()))
        .unwrap();
    assert_eq!(sample_request.parameter(), &half);
    let left_owner = split
        .owners_for_edge(0)
        .unwrap()
        .first()
        .ok_or_else(|| GeometryErrorKind::Invalid("left child lost its root range".into()))
        .unwrap();
    let right_owner = split
        .owners_for_edge(1)
        .unwrap()
        .first()
        .ok_or_else(|| GeometryErrorKind::Invalid("right child lost its root range".into()))
        .unwrap();
    assert!(left_owner.source_end().try_cmp(&half).unwrap().is_eq());
    assert!(right_owner.source_start().try_cmp(&half).unwrap().is_eq());
    assert_eq!(left_owner.producer_start(), &ExactRational::zero());
    assert_eq!(left_owner.producer_end(), &ExactRational::one());
    assert_eq!(right_owner.producer_start(), &ExactRational::zero());
    assert_eq!(right_owner.producer_end(), &ExactRational::one());
    assert_eq!(
        split
            .rows
            .requests_at(0, &half, &mut work)
            .unwrap()
            .first()
            .ok_or_else(|| GeometryErrorKind::Invalid(
                "left child lost its source request".into()
            ))
            .unwrap()
            .parameter(),
        &ExactRational::from_f64(0.375)
    );
    assert_eq!(
        split
            .rows
            .requests_at(1, &half, &mut work)
            .unwrap()
            .first()
            .ok_or_else(|| GeometryErrorKind::Invalid(
                "right child lost its source request".into()
            ))
            .unwrap()
            .parameter(),
        &ExactRational::from_f64(0.625)
    );
}

#[test]
fn recursive_source_trace_split_retains_prior_sample_rows() {
    let zero = ExactRational::zero();
    let one = ExactRational::one();
    let recipe = FragmentRecipe {
        id: FragmentRecipeId(0),
        endpoints: [QPortId(7), QPortId(8)],
        ranges: vec![DirectedSourceRange {
            source_edge: SourceEdgeId(2),
            range: ExactSourceRange {
                start: zero.clone(),
                end: one.clone(),
            },
            emitted: true,
            producer_start: zero,
            producer_end: one,
            endpoint_only: false,
        }],
        exact_displacement: ExactRational::from_f64(16.0),
        use_kind: SelectedUse::Curve,
    };
    let recipes = FragmentRecipes {
        values: vec![recipe],
        walks: Vec::new(),
    };
    let mut work = Unmetered;
    let original = recipes
        .source_trace_fragment(FragmentRecipeId(0), &mut work)
        .unwrap();
    let first = original
        .split_at_samples(
            &[SourceTraceSample {
                edge_index: 0,
                local_parameter: ExactRational::from_f64(0.5),
                written_vertex: 1,
            }],
            &mut work,
        )
        .unwrap();
    let second = first
        .split_at_samples(
            &[SourceTraceSample {
                edge_index: 0,
                local_parameter: ExactRational::from_f64(0.5),
                written_vertex: 1,
            }],
            &mut work,
        )
        .unwrap();

    assert_eq!(second.rows.edge_count(), 3);
    assert_eq!(second.samples.len(), 2);
    assert_eq!(second.samples[0].written_vertex, 1);
    assert_eq!(second.samples[1].written_vertex, 2);
    for (sample, expected) in [
        (&second.samples[0], ExactRational::from_f64(0.25)),
        (&second.samples[1], ExactRational::from_f64(0.5)),
    ] {
        let requests = second
            .rows
            .sample_requests_at(sample.request_row.index(), &mut work)
            .unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].parameter(), &expected);
    }
}

#[test]
fn recursive_source_trace_split_retains_a_prior_child_endpoint() {
    let root = RootId::from_index(12);
    let old_parameter = ExactRational::from_f64(0.75);
    let mut work = Unmetered;
    let mut rows = TraceRowsBuilder::new(&mut work).unwrap();
    rows.push_row([RootRange::whole(root)], &mut work).unwrap();
    rows.push_sample_row(
        [RootRange::point(root, old_parameter.clone(), ExactRational::zero()).unwrap()],
        &mut work,
    )
    .unwrap();
    let trace = SourceTrace {
        rows: rows.finish(&mut work).unwrap(),
        samples: vec![TracedSourceSample {
            written_vertex: 1,
            edge: 0,
            parameter: old_parameter.clone(),
            request_row: RequestRowId::from_index(0).unwrap(),
            has_source: true,
            endpoint_only: true,
        }],
    };
    let split = trace
        .split_at_samples(
            &[SourceTraceSample {
                edge_index: 0,
                local_parameter: ExactRational::from_f64(0.5),
                written_vertex: 1,
            }],
            &mut work,
        )
        .unwrap();

    assert_eq!(split.samples.len(), 2);
    assert_eq!(split.samples[0].written_vertex, 1);
    assert_eq!(split.samples[1].written_vertex, 2);
    let requests = split
        .rows
        .sample_requests_at(split.samples[1].request_row.index(), &mut work)
        .unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].root(), root);
    assert_eq!(requests[0].parameter(), &old_parameter);
}
