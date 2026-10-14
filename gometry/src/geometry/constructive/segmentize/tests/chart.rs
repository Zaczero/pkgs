use super::*;

#[test]
fn ordinary_chart_stations_keep_common_root_carry_without_remeasurement() {
    let source = CoordSeq::try_from_vecs(
        vec![0.0, 120.0],
        vec![10.0, 60.0],
        Some(vec![7.0, 47.0]),
        Some(vec![101.0, 701.0]),
    )
    .unwrap();
    let geodesic = geographiclib_rs::Geodesic::new(6_378_137.0, 1.0 / 298.257_223_563);
    crate::crs::reset_quadrature_test_progress();
    let _ = crate::geometry::ordinates::take_source_arc_activity();
    let mut budget = ExpansionBudget::new("segmentize", "piece_counts");
    let output = segmentize_points_with_piece_counts_budgeted(
        &source,
        &[NonZeroUsize::new(4).unwrap()],
        &SegmentPlacement::Chart(&geodesic, GeographicFrame::crs84()),
        &mut budget,
    )
    .unwrap();
    let progress = crate::crs::quadrature_test_progress();
    let activity = crate::geometry::ordinates::take_source_arc_activity();
    assert_eq!(output.len(), 5);
    assert!(progress.station_measurements >= 3);
    assert_eq!(progress.fraction_measurements, 0);
    assert_eq!(activity.root_resolutions, 3);
    assert_eq!(output.nth_coord(0), source.nth_coord(0));
    assert_eq!(output.nth_coord(4), source.nth_coord(1));
}

#[test]
fn chart_piece_counts_refine_only_requested_edges_and_preserve_ordinates() {
    let line = CoordSeq::try_from_vecs(
        vec![0.0, 1.0, 1.0, 2.0],
        vec![0.0, 0.0, 1.0, 1.0],
        Some(vec![1.0, 2.0, 32.0, 33.0]),
        Some(vec![10.0, 20.0, 80.0, 90.0]),
    )
    .unwrap();
    let geodesic = geographiclib_rs::Geodesic::new(6_371_000.0, 0.0);
    let counts = [
        NonZeroUsize::new(1).unwrap(),
        NonZeroUsize::new(3).unwrap(),
        NonZeroUsize::new(1).unwrap(),
    ];
    let mut budget = ExpansionBudget::new("buffer", "quadrant_segments");
    budget.add(7).unwrap();
    let refined = segmentize_points_with_piece_counts_budgeted(
        &line,
        &counts,
        &SegmentPlacement::Chart(&geodesic, GeographicFrame::crs84()),
        &mut budget,
    )
    .unwrap();

    assert_eq!(refined.xs(), &[0.0, 1.0, 1.0, 1.0, 1.0, 2.0]);
    assert_eq!(refined.ys(), &[0.0, 0.0, 1.0 / 3.0, 2.0 / 3.0, 1.0, 1.0]);
    for (actual, expected) in refined
        .zs()
        .unwrap()
        .iter()
        .zip([1.0, 2.0, 12.0, 22.0, 32.0, 33.0])
    {
        assert!((*actual - expected).abs() <= 1.0e-12);
    }
    for (actual, expected) in refined
        .ms()
        .unwrap()
        .iter()
        .zip([10.0, 20.0, 40.0, 60.0, 80.0, 90.0])
    {
        assert!((*actual - expected).abs() <= 1.0e-12);
    }
    assert_eq!(
        refined.zs().unwrap().first().unwrap().to_bits(),
        1.0_f64.to_bits()
    );
    assert_eq!(
        refined.zs().unwrap().last().unwrap().to_bits(),
        33.0_f64.to_bits()
    );
    assert_eq!(budget.used(), 9);
}
#[test]
fn chart_support_parameters_compose_exact_partial_ranges_in_reversed_grad_frames() {
    use crate::geographic::{AngularFactor, GeographicAxisOrientation};
    use crate::geometry::ExactRational;
    use crate::geometry::ordinates::{RootId, RootRange};
    use crate::geometry::segments::projection::segment_projection;

    let grad = AngularFactor::try_new("grad", std::f64::consts::TAU / 400.0).unwrap();
    let frame = GeographicFrame::try_new_with_orientation(
        grad,
        grad,
        0.0,
        GeographicAxisOrientation::from_axis_directions(false, false),
    )
    .unwrap();
    let geodesic = geographiclib_rs::Geodesic::new(6_378_137.0, 1.0 / 300.0);
    let source = [
        Point::new_raw_axes(
            -10.0,
            -20.0,
            crate::geometry::ZOrdinate(Some(10.0)),
            crate::geometry::MOrdinate(Some(100.0)),
        ),
        Point::new_raw_axes(
            -60.0,
            -70.0,
            crate::geometry::ZOrdinate(Some(210.0)),
            crate::geometry::MOrdinate(Some(500.0)),
        ),
    ];
    let piece_count = NonZeroUsize::new(4).unwrap();

    for points in [source, [source[1], source[0]]] {
        let coordinates = CoordSeq::try_from_points(&points).unwrap();
        let mut budget = ExpansionBudget::new("minimum_clearance", "input_refinement");
        let (refined, ranges) = segmentize_chart_points_with_piece_counts_budgeted(
            &coordinates,
            &[piece_count],
            &geodesic,
            frame,
            &mut budget,
        )
        .unwrap();

        assert_eq!(ranges.edge_ends, [ranges.parameters.len()]);
        assert_eq!(ranges.parameters.len(), refined.len());
        assert_eq!(
            ranges.parameters.first().unwrap().to_bits(),
            0.0_f64.to_bits()
        );
        assert_eq!(
            ranges.parameters.last().unwrap().to_bits(),
            1.0_f64.to_bits()
        );
        for pair in ranges.parameters.windows(2) {
            assert!(
                pair.first()
                    .zip(pair.get(1))
                    .is_some_and(|(left, right)| left < right)
            );
        }

        let edge = crate::crs::ChartEdge::new(
            Point::new_raw_xy(points[0].x, points[0].y),
            Point::new_raw_xy(points[1].x, points[1].y),
            frame,
        )
        .unwrap();
        let length = edge.length(&geodesic).unwrap();
        let first_equal_arclength_parameter = edge
            .locate_t(length / 4.0, length, &geodesic)
            .unwrap()
            .parameter();
        assert!(
            ranges.parameters.iter().any(|parameter| {
                parameter.to_bits() == first_equal_arclength_parameter.to_bits()
            })
        );
        assert_ne!(first_equal_arclength_parameter, 0.25);

        let interval_index = 1;
        let first_support_interval = ranges.parameters.windows(2).nth(interval_index).unwrap();
        let first_interval = first_support_interval
            .first()
            .zip(first_support_interval.get(1))
            .map(|(start, end)| (*start, *end))
            .unwrap();
        assert!(first_interval.0 > 0.0);
        assert!(first_interval.1 < 1.0);
        let first_segment = Segment {
            start: refined.nth_coord(interval_index).xy(),
            end: refined.nth_coord(interval_index + 1).xy(),
        };
        let selected_xy = XY::new(
            f64::midpoint(first_segment.start.x, first_segment.end.x),
            f64::midpoint(first_segment.start.y, first_segment.end.y),
        );
        let local_parameter = segment_projection(selected_xy, first_segment)
            .unwrap()
            .exact_fraction()
            .unwrap();
        let source_start = ExactRational::from_f64(first_interval.0);
        let source_end = ExactRational::from_f64(first_interval.1);
        let range = RootRange::from_mapped(
            RootId::from_index(0),
            source_start.clone(),
            source_end.clone(),
            ExactRational::zero(),
            ExactRational::one(),
        )
        .unwrap();
        let request = range.request_at(&local_parameter).unwrap().unwrap();
        let expected = source_start
            .try_add(
                &source_end
                    .try_subtract(&source_start)
                    .unwrap()
                    .try_product(&local_parameter)
                    .unwrap(),
            )
            .unwrap();
        assert_eq!(request.parameter(), &expected);
    }
}
