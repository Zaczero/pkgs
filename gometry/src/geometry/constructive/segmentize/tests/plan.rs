use super::*;

#[test]
fn subdivision_admits_exact_generated_limit_without_charging_input_vertices() {
    let at_limit = CoordSeq::try_from_xy(&[
        XY::new(0.0, 0.0),
        XY::new((GENERATED_ITEM_LIMIT + 1) as f64, 0.0),
    ])
    .unwrap();
    let at_limit = plan(&Planar, &at_limit, Target::MaxLength(1.0), 0).unwrap();
    assert_eq!(at_limit.steps, [GENERATED_ITEM_LIMIT + 1]);
    assert_eq!(at_limit.generated, GENERATED_ITEM_LIMIT);

    let mut budget = ExpansionBudget::new("segmentize", "max_length");
    budget.check_additional(at_limit.generated).unwrap();
    budget.add(at_limit.generated).unwrap();
    assert_eq!(budget.used(), GENERATED_ITEM_LIMIT);

    let over_limit = CoordSeq::try_from_xy(&[
        XY::new(0.0, 0.0),
        XY::new((GENERATED_ITEM_LIMIT + 2) as f64, 0.0),
    ])
    .unwrap();
    let over_limit = plan(&Planar, &over_limit, Target::MaxLength(1.0), 0).unwrap();
    assert_eq!(over_limit.generated, GENERATED_ITEM_LIMIT + 1);
    let over_budget = ExpansionBudget::new("segmentize", "max_length");
    let error = over_budget
        .check_additional(over_limit.generated)
        .unwrap_err();
    assert!(matches!(
        error.kind(),
        crate::error::ErrorKind::Geometry(
            crate::geometry::GeometryErrorKind::GeneratedOutputTooLarge {
                operation: "segmentize",
                parameter: "max_length",
                ..
            }
        )
    ));
}
#[test]
fn segmentize_planning_sentinel_is_reserved_for_more_than_the_limit() {
    assert_eq!(
        segmentize_steps((GENERATED_ITEM_LIMIT + 1) as f64, 1.0),
        GENERATED_ITEM_LIMIT + 1
    );
    assert_eq!(
        segmentize_steps((GENERATED_ITEM_LIMIT + 2) as f64, 1.0),
        GENERATED_ITEM_LIMIT + 2
    );
}
#[test]
fn fractional_step_count_does_not_include_prior_budget_usage() {
    let line = CoordSeq::try_from_xy(&[XY::new(0.0, 0.0), XY::new(10.0, 10.0)]).unwrap();
    let mut budget = ExpansionBudget::new("segmentize", "fraction");
    let first =
        densify_points_budgeted_with_placement(&line, 0.5, &SegmentPlacement::Planar, &mut budget)
            .unwrap();
    let second =
        densify_points_budgeted_with_placement(&line, 0.5, &SegmentPlacement::Planar, &mut budget)
            .unwrap();

    assert_eq!(first.len(), 3);
    assert_eq!(second.len(), 3);
    assert_eq!(budget.used(), 2);
}
#[test]
fn planar_segmentize_planner_keeps_the_exact_generated_boundary() {
    let segment = Segment {
        start: XY::new(0.0, 0.0),
        end: XY::new((GENERATED_ITEM_LIMIT + 1) as f64, 0.0),
    };
    assert_eq!(
        planar_segmentize_steps(segment, 1.0),
        GENERATED_ITEM_LIMIT + 1
    );
    let segment = Segment {
        start: XY::new(0.0, 0.0),
        end: XY::new((GENERATED_ITEM_LIMIT + 2) as f64, 0.0),
    };
    assert_eq!(
        planar_segmentize_steps(segment, 1.0),
        GENERATED_ITEM_LIMIT + 2
    );
}
#[test]
fn planar_representability_retry_replans_and_charges_the_accepted_output_once() {
    let points = CoordSeq::try_from_xy(&[XY::new(0.0, 0.0), XY::new(1.0, 0.0)]).unwrap();
    let max_length = 1.0 / 3.0;

    let first = plan(&Planar, &points, Target::MaxLength(max_length), 0).unwrap();
    assert_eq!(first.steps, [3]);
    let budget = ExpansionBudget::new("segmentize", "max_length");
    let mut route = Planar;
    let (first_output, first_generated) =
        materialize(&mut route, &points, &first, &budget).unwrap();
    let first_error =
        verify_segmentize_output(&route, &first_output, &first, first_generated, max_length)
            .unwrap_err();
    assert!(matches!(
        first_error.kind(),
        crate::error::ErrorKind::Geometry(GeometryErrorKind::UnrepresentableOutput {
            operation: "segmentize",
            ..
        })
    ));

    let mut budget = ExpansionBudget::new("segmentize", "max_length");
    let output =
        segmentize_points_budgeted(&points, max_length, &SegmentPlacement::Planar, &mut budget)
            .unwrap();
    assert_eq!(output.len(), 5);
    assert_eq!(output.xs(), &[0.0, 0.25, 0.5, 0.75, 1.0]);
    assert_eq!(budget.used(), output.len() - points.len());
    let accepted = plan(&Planar, &points, Target::MaxLength(max_length), 1).unwrap();
    verify_segmentize_output(&Planar, &output, &accepted, accepted.generated, max_length).unwrap();
}
#[test]
fn unsplit_source_length_keeps_the_full_verification_error_path() {
    struct Underplanned;

    impl Route for Underplanned {
        type Solution = ();

        fn solve(&self, _segment: Segment, _target: Target<'_>) -> Result<(f64, Self::Solution)> {
            Ok((2.0, ()))
        }

        fn pieces(&self, _segment: Segment, _length: f64, _max_length: f64) -> usize {
            1
        }

        fn interior(
            &mut self,
            _segment: Segment,
            (): Self::Solution,
            _pieces: usize,
            _source: &CoordSeq,
            _source_segment: usize,
            _output: &mut OutputColumns,
            _budget: &ExpansionBudget,
            _planned_generated: usize,
            _refined_generated: &mut usize,
        ) -> Result<()> {
            Ok(())
        }

        fn measure(&self, _segment: Segment) -> Result<f64> {
            Ok(2.0)
        }
    }

    let points = CoordSeq::try_from_xy(&[XY::new(0.0, 0.0), XY::new(2.0, 0.0)]).unwrap();
    let mut route = Underplanned;
    let plan = plan(&route, &points, Target::MaxLength(1.0), 0).unwrap();
    assert!(plan.source_exceeds_max_length);
    let budget = ExpansionBudget::new("segmentize", "max_length");
    let (output, generated) = materialize(&mut route, &points, &plan, &budget).unwrap();
    let error = verify_segmentize_output(&route, &output, &plan, generated, 1.0).unwrap_err();
    assert!(matches!(
        error.kind(),
        crate::error::ErrorKind::Geometry(GeometryErrorKind::UnrepresentableOutput {
            operation: "segmentize",
            ..
        })
    ));
}
