use super::*;

fn retained_bytes<T>(values: &Vec<T>) -> usize {
    values.capacity().saturating_mul(std::mem::size_of::<T>())
}

#[test]
fn subdivision_rejects_coordinate_width_before_reserving_columns() {
    let points = CoordSeq::try_from_xy(&[XY::new(0.0, 0.0), XY::new(1.0, 0.0)]).unwrap();
    let plan = Plan {
        steps: vec![1],
        solutions: Vec::<()>::new(),
        generated: i32::MAX as usize,
        source_exceeds_max_length: false,
    };
    let budget = ExpansionBudget::new("segmentize", "max_length");
    let mut route = Planar;
    let error = materialize(&mut route, &points, &plan, &budget)
        .expect_err("unrepresentable coordinate width is rejected");
    assert!(matches!(
        error.kind(),
        crate::error::ErrorKind::Geometry(
            crate::geometry::GeometryErrorKind::OffsetCapacityExceeded
        )
    ));
}
#[test]
fn one_piece_counts_return_the_shared_source_without_allocating() {
    use crate::test_support::allocation_count;

    let points =
        CoordSeq::try_from_xy(&[XY::new(0.0, 0.0), XY::new(0.1, 0.0), XY::new(0.2, 0.0)]).unwrap();
    let counts = [NonZeroUsize::MIN, NonZeroUsize::MIN];
    let geodesic = geographiclib_rs::Geodesic::new(6_378_137.0, 1.0 / 298.257_223_563);
    let mut budget = ExpansionBudget::new("buffer", "quadrant_segments");

    let (refined, allocations) = allocation_count(|| {
        segmentize_points_with_piece_counts_budgeted(
            &points,
            &counts,
            &SegmentPlacement::Chart(&geodesic, GeographicFrame::crs84()),
            &mut budget,
        )
        .unwrap()
    });

    assert_eq!(allocations, 0);
    assert_eq!(refined.xs(), points.xs());
    assert_eq!(refined.ys(), points.ys());
    assert_eq!(budget.used(), 0);
}
#[test]
fn no_op_plans_allocate_only_dense_steps_and_keep_no_route_solutions() {
    use crate::test_support::allocation_count;

    let coordinates: Vec<_> = (0..33)
        .map(|index| XY::new(f64::from(index) / 4.0, 0.0))
        .collect();
    let points = CoordSeq::try_from_xy(&coordinates).unwrap();
    let (planar_plan, allocations) =
        allocation_count(|| plan(&Planar, &points, Target::MaxLength(1.0), 0).unwrap());
    assert_eq!(allocations, 1);
    assert_eq!(planar_plan.steps.len(), points.len() - 1);
    assert_eq!(planar_plan.generated, 0);
    assert!(planar_plan.solutions.is_empty());
    assert_eq!(retained_bytes(&planar_plan.solutions), 0);
}

#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
fn no_op_geographic_plans_keep_no_route_solutions() {
    let geographic = CoordSeq::try_from_xy(&[
        XY::new(0.0, 0.0),
        XY::new(0.000_001, 0.0),
        XY::new(0.000_002, 0.0),
    ])
    .unwrap();
    let geodesic = geographiclib_rs::Geodesic::new(6_378_137.0, 1.0 / 298.257_223_563);
    let frame = GeographicFrame::crs84();
    let chart_plan = plan(
        &Chart {
            geodesic: &geodesic,
            frame,
        },
        &geographic,
        Target::MaxLength(1.0),
        0,
    )
    .unwrap();
    let geodesic_plan = plan(
        &GeodesicRoute {
            geodesic: &geodesic,
            frame,
        },
        &geographic,
        Target::MaxLength(1.0),
        0,
    )
    .unwrap();
    let state = crate::crs::runtime_state_snapshot().unwrap();
    let rhumb_plan = plan(
        &Rhumb {
            crs: "EPSG:4326",
            state: &state,
            frame,
        },
        &geographic,
        Target::MaxLength(1.0),
        0,
    )
    .unwrap();
    assert_eq!(chart_plan.generated, 0);
    assert_eq!(geodesic_plan.generated, 0);
    assert_eq!(rhumb_plan.generated, 0);
    assert_eq!(retained_bytes(&chart_plan.solutions), 0);
    assert_eq!(retained_bytes(&geodesic_plan.solutions), 0);
    assert_eq!(retained_bytes(&rhumb_plan.solutions), 0);
}
#[test]
fn zero_and_one_vertex_plans_share_input_and_generate_nothing() {
    let inputs = [
        CoordSeq::empty(crate::geometry::CoordinateAxes::XY),
        CoordSeq::try_from_xy(&[XY::new(1.0, 2.0)]).unwrap(),
    ];
    for points in &inputs {
        let mut segmentize_budget = ExpansionBudget::new("segmentize", "max_length");
        let segmented = segmentize_points_budgeted(
            points,
            1.0,
            &SegmentPlacement::Planar,
            &mut segmentize_budget,
        )
        .unwrap();
        assert_eq!(&segmented, points);
        assert_eq!(segmentize_budget.used(), 0);

        let mut densify_budget = ExpansionBudget::new("segmentize", "fraction");
        let densified = densify_points_budgeted_with_placement(
            points,
            f64::MIN_POSITIVE,
            &SegmentPlacement::Planar,
            &mut densify_budget,
        )
        .unwrap();
        assert_eq!(&densified, points);
        assert_eq!(densify_budget.used(), 0);
    }
}
