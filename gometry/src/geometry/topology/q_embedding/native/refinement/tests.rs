    #[test]
    fn native_refinement_mismatch_rejects_first_candidate_and_searches_next() -> Result<()> {
        let mut problem = NativeRefinementSearch {
            classes: vec![vec![0]],
            visits: Vec::new(),
        };
        let mut budget = EvaluationBudget::new("native incidence search control", 0)?;

        let outcome = run_with_budget(&mut problem, &mut budget)?;

        let SearchOutcome::Pass(assignment) = outcome else {
            return Err(GeometryErrorKind::Invalid(
                "native incidence search exhausted after a repairable rejection".into(),
            )
            .into());
        };
        assert_eq!(assignment.output, 1);
        assert_eq!(problem.visits, [0, 1]);
        Ok(())
    }
    #[test]
    fn corrupt_native_refinement_source_remains_a_hard_error() -> Result<()> {
        let result: Result<u8> =
            Err(GeometryErrorKind::Invalid("corrupt source-span port map".into()).into());

        match native_reference_global_check(result, || Ok(vec![0])) {
            Err(error) => assert!(matches!(
                error.kind(),
                crate::error::ErrorKind::Geometry(GeometryErrorKind::Invalid(_))
            )),
            Ok(_) => {
                return Err(GeometryErrorKind::Invalid(
                    "corrupt source-span data was treated as a candidate rejection".into(),
                )
                .into());
            },
        }
        Ok(())
    }
    #[test]
    fn native_refinement_aligns_a_reversed_source_lift_to_the_assigned_leaf() -> Result<()> {
        let event = NativePortData {
            node: EmissionNodeId(2),
            point: ExactChartPoint::from_rationals(
                ExactRational::from_f64(-150.0),
                ExactRational::from_f64(-80.0),
            ),
            spelling: PortSpelling::Interior { periods: 0 },
            source_record: None,
            occurrences: Vec::new(),
        };
        let (edge, boundary) = reversed_lift_native_refinement_fixture(event, QPortId(0));
        let refined = common_refine_native_edges(
            vec![edge],
            &[boundary],
            GeographicFrame::crs84(),
            &mut Unmetered,
        )?;

        assert_eq!(refined.len(), 2);
        let split = refined
            .first()
            .and_then(|edge| edge.vertices.get(1))
            .ok_or_else(|| GeometryErrorKind::Invalid("refined split port is missing".into()))?;
        assert_eq!(
            split.point,
            ExactChartPoint::from_rationals(
                ExactRational::from_f64(-150.0),
                ExactRational::from_f64(-80.0),
            )
        );
        assert_eq!(split.node, EmissionNodeId(2));
        let NativeAreaEdgeKind::Producer { known_spans, .. } = &refined
            .get(1)
            .ok_or_else(|| GeometryErrorKind::Invalid("second refined edge is missing".into()))?
            .kind
        else {
            return Err(GeometryErrorKind::Invalid(
                "refinement changed the producer edge kind".into(),
            )
            .into());
        };
        let known = known_spans
            .first()
            .ok_or_else(|| GeometryErrorKind::Invalid("reversed source span was lost".into()))?;
        assert_eq!(known.source_range.start, ExactRational::from_f64(0.5));
        assert_eq!(known.source_range.end, ExactRational::zero());
        assert_eq!(known.source_lift, -1);
        Ok(())
    }
    #[test]
    fn noncoincident_native_refinement_junctions_are_typed_candidate_rejections() -> Result<()> {
        for (longitude, latitude) in [(-149.5, -80.0), (-150.0, (-80.0_f64).next_up())] {
            let event = NativePortData {
                node: EmissionNodeId(2),
                point: ExactChartPoint::from_rationals(
                    ExactRational::from_f64(longitude),
                    ExactRational::from_f64(latitude),
                ),
                spelling: PortSpelling::Interior { periods: 0 },
                source_record: None,
                occurrences: Vec::new(),
            };
            let (edge, boundary) = reversed_lift_native_refinement_fixture(event, QPortId(0));
            let Err(error) = common_refine_native_edges(
                vec![edge],
                &[boundary],
                GeographicFrame::crs84(),
                &mut Unmetered,
            ) else {
                return Err(GeometryErrorKind::Invalid(
                    "noncoincident native refinement event was accepted".into(),
                )
                .into());
            };
            assert!(matches!(
                error.kind(),
                crate::error::ErrorKind::Geometry(GeometryErrorKind::NativeRefinementMismatch)
            ));
        }
        Ok(())
    }
    #[test]
    fn corrupt_native_span_port_map_propagates_through_candidate_classification() -> Result<()> {
        let event = NativePortData {
            node: EmissionNodeId(2),
            point: ExactChartPoint::from_rationals(
                ExactRational::from_f64(-150.0),
                ExactRational::from_f64(-80.0),
            ),
            spelling: PortSpelling::Interior { periods: 0 },
            source_record: None,
            occurrences: Vec::new(),
        };
        let (edge, boundary) = reversed_lift_native_refinement_fixture(event, QPortId(1));
        let result = common_refine_native_edges(
            vec![edge],
            &[boundary],
            GeographicFrame::crs84(),
            &mut Unmetered,
        )
        .map(|_| ());

        match native_reference_global_check(result, || Ok(vec![0])) {
            Err(error) => assert!(matches!(
                error.kind(),
                crate::error::ErrorKind::Geometry(GeometryErrorKind::Invalid(_))
            )),
            Ok(_) => {
                return Err(GeometryErrorKind::Invalid(
                    "corrupt Q-span port identity was treated as a candidate rejection".into(),
                )
                .into());
            },
        }
        Ok(())
    }
