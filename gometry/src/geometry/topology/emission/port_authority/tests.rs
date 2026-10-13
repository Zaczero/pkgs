#[test]
fn source_sample_occurrences_reserve_before_copying_requests() {
    let mut setup = Unmetered;
    let requests = [
        RootRequest::new_with_work(
            RootId::from_index(2),
            ExactRational::from_f64(0.25),
            &mut setup,
        )
        .unwrap(),
        RootRequest::new_with_work(
            RootId::from_index(5),
            ExactRational::from_f64(0.75),
            &mut setup,
        )
        .unwrap(),
    ];
    let target = {
        let progress = Rc::new(Cell::new(0));
        let mut budget = EvaluationBudget::new(EMISSION_STORAGE_OPERATION, 0).unwrap();
        let mut work = RecordedWork {
            budget: &mut budget,
            progress: Rc::clone(&progress),
            spent: 0,
            charges: Vec::new(),
        };
        let occurrences = native_source_sample_occurrences(None, &requests, &mut work).unwrap();
        progress.set(1);
        assert_eq!(occurrences.len(), 2);
        assert_eq!(progress.get(), 1);
        let target = *work
            .charges
            .first()
            .expect("source request capacity is paid before occurrence writes");
        assert_eq!(target.ticks, 2);
        assert_eq!(target.progress, 0);
        target
    };

    let progress = Rc::new(Cell::new(0));
    let mut budget = emission_budget_refusing_before(target);
    let mut work = RecordedWork {
        budget: &mut budget,
        progress: Rc::clone(&progress),
        spent: 0,
        charges: Vec::new(),
    };
    let Err(error) = native_source_sample_occurrences(None, &requests, &mut work) else {
        panic!("source occurrence storage must be paid before copying requests");
    };
    assert_emission_work_limit(&error);
    assert_eq!(progress.get(), 0);
    let refusal = work
        .charges
        .last()
        .expect("the recorder observes the refused occurrence reservation");
    assert!(!refusal.accepted);
    assert_eq!(refusal.ticks, target.ticks);
    assert_eq!(refusal.spent_before, target.spent_before);
    assert_eq!(refusal.progress, 0);
}

#[test]
fn native_port_clone_prepays_occurrences_and_root_parameter_limbs() {
    let mut setup = Unmetered;
    let quarter_turn = ExactRational::from_f64(0.75);
    let mut parameter = ExactRational::one();
    for _ in 0..100 {
        parameter = parameter
            .try_product_with_work(&quarter_turn, &mut setup)
            .unwrap();
    }
    let port = NativePortData {
        node: EmissionNodeId(4),
        point: ExactChartPoint::from_rationals(
            ExactRational::from_f64(0.25),
            ExactRational::from_f64(0.5),
        ),
        spelling: PortSpelling::Interior { periods: 0 },
        source_record: None,
        occurrences: vec![OccurrenceSource {
            origin: VertexOrigin::SourceSample,
            source_trace: Some(TraceRequestLocation::Root {
                root: RootId::from_index(3),
                parameter,
            }),
        }],
    };
    let target = {
        let progress = Rc::new(Cell::new(0));
        let mut budget = EvaluationBudget::new(EMISSION_STORAGE_OPERATION, 0).unwrap();
        let mut work = RecordedWork {
            budget: &mut budget,
            progress: Rc::clone(&progress),
            spent: 0,
            charges: Vec::new(),
        };
        let cloned = port.try_clone_with_work(&mut work).unwrap();
        progress.set(1);
        assert_eq!(cloned.node, port.node);
        assert_eq!(cloned.point, port.point);
        let Some(TraceRequestLocation::Root {
            root,
            parameter: cloned_parameter,
        }) = cloned
            .occurrences
            .first()
            .and_then(|occurrence| occurrence.source_trace.as_ref())
        else {
            panic!("the cloned Native occurrence retains its direct source root");
        };
        assert_eq!(*root, RootId::from_index(3));
        let Some(TraceRequestLocation::Root {
            parameter: source_parameter,
            ..
        }) = port
            .occurrences
            .first()
            .and_then(|occurrence| occurrence.source_trace.as_ref())
        else {
            panic!("the source Native occurrence retains its direct source root");
        };
        assert_eq!(cloned_parameter, source_parameter);
        assert_eq!(progress.get(), 1);
        assert_eq!(
            work.charges
                .iter()
                .take(5)
                .map(|charge| charge.ticks)
                .collect::<Vec<_>>(),
            [1, 1, 1, 1, 1]
        );
        let target = *work
            .charges
            .iter()
            .find(|charge| charge.ticks > 1)
            .expect("the wide root parameter copy has a multi-limb Work charge");
        assert!(target.accepted);
        target
    };

    let progress = Rc::new(Cell::new(0));
    let mut budget = emission_budget_refusing_before(target);
    let mut work = RecordedWork {
        budget: &mut budget,
        progress: Rc::clone(&progress),
        spent: 0,
        charges: Vec::new(),
    };
    let Err(error) = port.try_clone_with_work(&mut work) else {
        panic!("Native source-root parameter limbs must be paid before clone completion");
    };
    assert_emission_work_limit(&error);
    assert_eq!(progress.get(), 0);
    let refusal = work
        .charges
        .last()
        .expect("the recorder observes refusal during the root-parameter copy");
    assert!(!refusal.accepted);
    assert_eq!(refusal.ticks, target.ticks);
    assert_eq!(refusal.spent_before, target.spent_before);
    assert_eq!(refusal.progress, 0);
}

#[test]
fn merging_vertex_aliases_keeps_presentation_and_source_rows_separate() {
    let point = ExactChartPoint::from_rationals(
        ExactRational::from_f64(12.0),
        ExactRational::from_f64(34.0),
    );
    let presentation = NativePortData {
        node: EmissionNodeId(3),
        point: point.clone(),
        spelling: PortSpelling::Interior { periods: 0 },
        source_record: None,
        occurrences: vec![OccurrenceSource {
            origin: VertexOrigin::Presentation,
            source_trace: None,
        }],
    };
    let source_sample = NativePortData {
        node: EmissionNodeId(3),
        point,
        spelling: PortSpelling::Interior { periods: 0 },
        source_record: None,
        occurrences: vec![OccurrenceSource {
            origin: VertexOrigin::SourceSample,
            source_trace: Some(TraceRequestLocation::Edge {
                recipe: FragmentRecipeId(5),
                edge: 0,
                parameter: ExactRational::from_f64(0.5),
            }),
        }],
    };
    let mut work = Unmetered;
    let merged = merge_native_port_data(
        GeographicFrame::crs84(),
        &presentation,
        &source_sample,
        &mut work,
    )
    .unwrap();
    assert_eq!(merged.occurrences.len(), 2);
    assert!(matches!(
        merged.occurrences.first(),
        Some(OccurrenceSource {
            origin: VertexOrigin::Presentation,
            source_trace: None,
        })
    ));
    assert!(matches!(
        merged.occurrences.get(1),
        Some(OccurrenceSource {
            origin: VertexOrigin::SourceSample,
            source_trace: Some(_),
        })
    ));
}

#[test]
fn merging_periodic_vertex_aliases_keeps_the_existing_chart_lift() {
    let y = ExactRational::from_f64(20.0);
    let left_point =
        ExactChartPoint::from_rationals(ExactRational::from_f64(-170.0), y.clone());
    let right_point = ExactChartPoint::from_rationals(ExactRational::from_f64(190.0), y);
    let alias = |point, periods| NativePortData {
        node: EmissionNodeId(3),
        point,
        spelling: PortSpelling::Interior { periods },
        source_record: None,
        occurrences: Vec::new(),
    };
    let left = alias(left_point.clone(), 0);
    let right = alias(right_point, 1);
    let mut work = Unmetered;

    let merged =
        merge_native_port_data(GeographicFrame::crs84(), &left, &right, &mut work).unwrap();

    assert_eq!(merged.point, left_point);
    assert!(matches!(merged.spelling, PortSpelling::Interior {
        periods: 0
    }));
}

#[test]
fn native_vertex_merge_rejects_nonperiodic_longitude_aliases() {
    let frame = GeographicFrame::crs84();
    let point = |x, y| {
        ExactChartPoint::from_rationals(ExactRational::from_f64(x), ExactRational::from_f64(y))
    };
    let alias = |point, periods| NativePortData {
        node: EmissionNodeId(3),
        point,
        spelling: PortSpelling::Interior { periods },
        source_record: None,
        occurrences: Vec::new(),
    };
    let left = alias(point(-170.0, 20.0), 0);
    let mut work = Unmetered;

    assert!(
        merge_native_port_data(frame, &left, &alias(point(190.25, 20.0), 1), &mut work)
            .is_err(),
        "equal node IDs do not admit nonperiodic longitude differences"
    );
    assert!(
        merge_native_port_data(frame, &left, &alias(point(190.0, 20.0), 0), &mut work).is_err(),
        "one chart spelling cannot name two exact periodic lifts"
    );
}

#[test]
fn native_vertex_merge_rejects_mismatched_admitted_latitudes() {
    let frame = GeographicFrame::crs84();
    let point = |x, y| {
        ExactChartPoint::from_rationals(ExactRational::from_f64(x), ExactRational::from_f64(y))
    };
    let alias = |point, periods| NativePortData {
        node: EmissionNodeId(3),
        point,
        spelling: PortSpelling::Interior { periods },
        source_record: None,
        occurrences: Vec::new(),
    };
    let left = alias(point(-170.0, 20.0), 0);
    let right = alias(point(190.0, 20.0_f64.next_up()), 1);
    let mut work = Unmetered;

    assert!(
        merge_native_port_data(frame, &left, &right, &mut work).is_err(),
        "one-ULP admitted latitude differences are not one assigned source image"
    );
}

#[test]
fn native_vertex_merge_preserves_one_authenticated_periodic_source_record() {
    let frame = GeographicFrame::crs84();
    let y = ExactRational::from_f64(20.0);
    let left = NativePortData {
        node: EmissionNodeId(3),
        point: ExactChartPoint::from_rationals(ExactRational::from_f64(-170.0), y.clone()),
        spelling: PortSpelling::Interior { periods: 0 },
        source_record: Some(Point::new_raw_axes(
            190.0,
            20.0,
            ZOrdinate(Some(7.0)),
            MOrdinate(None),
        )),
        occurrences: Vec::new(),
    };
    let right = NativePortData {
        node: EmissionNodeId(3),
        point: ExactChartPoint::from_rationals(ExactRational::from_f64(190.0), y),
        spelling: PortSpelling::Interior { periods: 1 },
        source_record: None,
        occurrences: Vec::new(),
    };
    let mut work = Unmetered;

    let merged = merge_native_port_data(frame, &left, &right, &mut work).unwrap();

    assert_eq!(
        merged.source_record.map(|record| record.x.to_bits()),
        Some(190.0_f64.to_bits()),
        "the authenticated raw record is retained even when it uses another exact lift"
    );
    assert_eq!(merged.point, left.point);
}

#[test]
fn native_vertex_merge_selects_the_record_for_its_retained_chart_lift() {
    let frame = GeographicFrame::crs84();
    let make_port = |longitude, periods, source_record| NativePortData {
        node: EmissionNodeId(3),
        point: ExactChartPoint::from_rationals(
            ExactRational::from_f64(longitude),
            ExactRational::from_f64(20.0),
        ),
        spelling: PortSpelling::Interior { periods },
        source_record: Some(Point::new_raw_xy(source_record, 20.0)),
        occurrences: Vec::new(),
    };
    let mut work = Unmetered;
    for (left_record, right_record) in [(190.0, -170.0), (-170.0, 190.0)] {
        let left = make_port(-170.0, 0, left_record);
        let right = make_port(190.0, 1, right_record);
        let merged = merge_native_port_data(frame, &left, &right, &mut work).unwrap();
        assert_eq!(
            merged.source_record.map(|record| record.x.to_bits()),
            Some((-170.0_f64).to_bits()),
            "the retained chart spelling, not contributor arrival order, selects the literal record"
        );
    }
}

#[test]
fn native_vertex_merge_keeps_seam_banks_and_pole_spellings_distinct() {
    let frame = GeographicFrame::crs84();
    let point = |x, y| {
        ExactChartPoint::from_rationals(ExactRational::from_f64(x), ExactRational::from_f64(y))
    };
    let seam = |x, side| NativePortData {
        node: EmissionNodeId(3),
        point: point(x, 20.0),
        spelling: PortSpelling::Seam(side),
        source_record: None,
        occurrences: Vec::new(),
    };
    let mut work = Unmetered;
    assert!(
        merge_native_port_data(
            frame,
            &seam(180.0, ChartSide::East),
            &seam(-180.0, ChartSide::West),
            &mut work,
        )
        .is_err(),
        "opposite seam banks remain distinct ports"
    );

    let pole = |x, longitude| NativePortData {
        node: EmissionNodeId(4),
        point: point(x, 90.0),
        spelling: PortSpelling::Pole {
            native_longitude: longitude,
        },
        source_record: None,
        occurrences: Vec::new(),
    };
    assert!(
        merge_native_port_data(frame, &pole(10.0, 10.0), &pole(370.0, 10.0), &mut work,)
            .is_ok(),
        "same-pole coordinates may differ by an exact period when spelling is retained"
    );
    assert!(
        merge_native_port_data(frame, &pole(10.0, 10.0), &pole(11.0, 11.0), &mut work).is_err(),
        "same-pole identity does not erase the explicit longitude spelling"
    );
    assert!(
        merge_native_port_data(frame, &pole(10.0, 11.0), &pole(10.0, 11.0), &mut work).is_err(),
        "the pole spelling must remain an exact periodic image of the assigned point"
    );
}
