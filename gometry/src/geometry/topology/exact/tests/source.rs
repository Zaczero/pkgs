use std::cmp::Ordering;

use super::super::{
    ExactGeographicChartInventory, RingSummary, SourceEdgeKind, SourceLineId, SourcePolygonId,
    SourceRingId, SourceRowId, local_longitude_edge,
};
use super::support::{chart_step_identity_holds, degrees, inventory, line, ring};
use crate::geographic::{AngularFactor, GeographicFrame, source_admission_counts};
use crate::geometry::numerics::product::exact_power_of_two;
use crate::geometry::tessellation::exact::{ExactDyadic, ExactRational};
use crate::geometry::{
    CoordSeq, ExpansionBudget, LineSeq, MOrdinate, Point, Polygon, Shape, ZOrdinate,
};

#[test]
#[expect(
    clippy::float_cmp,
    reason = "exact half-period equality selects the source seam spelling without rounding"
)]
fn admitted_source_producer_preserves_raw_branch_and_piece_correspondence() {
    let degree = degrees();
    let grad_unit = AngularFactor::try_new("grads", std::f64::consts::TAU / 400.0).unwrap();
    let grad = GeographicFrame::try_new(grad_unit, grad_unit, 0.0).unwrap();
    let rad = GeographicFrame::try_new(
        AngularFactor::try_new("radians", 1.0).unwrap(),
        AngularFactor::try_new("radians", 1.0).unwrap(),
        0.0,
    )
    .unwrap();
    let tiny = f64::from_bits(1);
    let cases = [
        (degree, 0.0, 360.0, 0.0, 0.0, 1),
        (degree, 0.0, -360.0, 0.0, 0.0, -1),
        (degree, 0.0, 180.0, 0.0, -180.0, 1),
        (degree, 0.0, -180.0, 0.0, -180.0, 0),
        (degree, 180.0, 175.0, -180.0, 175.0, -1),
        (degree, 175.0, 180.0, 175.0, -180.0, 1),
        (degree, 530.0, 190.0, 170.0, -170.0, 1),
        (degree, 190.0, 530.0, -170.0, 170.0, -1),
        (
            degree,
            exact_power_of_two(60),
            exact_power_of_two(60),
            136.0,
            136.0,
            0,
        ),
        (degree, -0.0, tiny, 0.0, tiny, 0),
        (degree, tiny, -tiny, tiny, -tiny, 0),
        (degree, 1.0e-20, 360.0, 1.0e-20, 0.0, 0),
        (grad, 195.0, -195.0, 195.0, -195.0, 1),
        (grad, -195.0, 195.0, -195.0, 195.0, -1),
        (rad, 3.0, -3.0, 3.0, -3.0, 1),
        (rad, -3.0, 3.0, -3.0, 3.0, -1),
    ];
    for (frame, from, to, native_from, native_to, periods) in cases {
        let source = [Point::new_raw_xy(from, 1.0), Point::new_raw_xy(to, 2.0)];
        let mut inventory = ExactGeographicChartInventory::new(frame, 1);
        let before = source_admission_counts::snapshot();
        let _ = inventory
            .append_source_edge(
                source[0],
                source[1],
                SourceRowId(0),
                SourceEdgeKind::Line {
                    line: SourceLineId(0),
                },
                0,
            )
            .unwrap();
        let after = source_admission_counts::snapshot();
        assert_eq!(after.0 - before.0, 1, "{from} -> {to}");
        let edge = &inventory.edges[0];
        for (actual, original) in edge.source.iter().zip(source) {
            assert_eq!(actual.x.to_bits(), original.x.to_bits());
            assert_eq!(actual.y.to_bits(), original.y.to_bits());
        }
        let retained = edge.retained_longitude();
        assert_eq!(retained.periods(), periods, "{from} -> {to}");
        assert_eq!(
            retained.endpoints()[0].get().to_bits(),
            native_from.to_bits()
        );
        assert_eq!(retained.endpoints()[1].get().to_bits(), native_to.to_bits());
        let delta = ExactDyadic::from_f64(native_to)
            .try_subtract_ref(&ExactDyadic::from_f64(native_from))
            .unwrap()
            .try_add(
                ExactDyadic::from_f64(frame.longitude().period())
                    .try_product(&ExactDyadic::from_i64(periods))
                    .unwrap(),
            )
            .unwrap();
        let chart_start =
            if native_from == -frame.longitude().half_period() && from.is_sign_positive() {
                frame.longitude().half_period()
            } else {
                native_from
            };
        let alias_adjust = i64::from(chart_start.to_bits() != native_from.to_bits());
        for piece in &inventory.pieces {
            for (point, parameter) in [
                (&piece.start, &piece.source_range.start),
                (&piece.end, &piece.source_range.end),
            ] {
                let original_x = ExactRational::from_f64(chart_start)
                    .try_add(
                        &ExactRational::from_dyadic(delta.clone())
                            .try_product(parameter)
                            .unwrap(),
                    )
                    .unwrap();
                let lifted_x = point
                    .x
                    .try_add(
                        &ExactRational::from_f64(frame.longitude().period())
                            .try_product(&ExactRational::from_dyadic(ExactDyadic::from_i64(
                                piece.lift + alias_adjust,
                            )))
                            .unwrap(),
                    )
                    .unwrap();
                assert_eq!(
                    lifted_x.try_cmp(&original_x).unwrap(),
                    Ordering::Equal,
                    "{from} -> {to}"
                );
                let expected_y = ExactRational::one().try_add(parameter).unwrap();
                assert_eq!(point.y.try_cmp(&expected_y).unwrap(), Ordering::Equal);
            }
        }
        let actual_delta = local_longitude_edge(frame, from, edge.retained_longitude())
            .delta_x(frame.longitude().period())
            .unwrap();
        assert_eq!(actual_delta, delta);
        assert_eq!(source_admission_counts::snapshot(), after);
    }
}

#[test]
fn admitted_ring_producer_classifies_once_beyond_legitimate_summary() {
    let frame = degrees();
    let ring = ring(&[
        (175.0, 10.0),
        (-175.0, 10.0),
        (-175.0, 20.0),
        (175.0, 20.0),
        (175.0, 10.0),
    ]);
    let before = source_admission_counts::snapshot().0;
    RingSummary::try_new(frame, &crate::geographic::GeographicRing::from_ring(&ring)).unwrap();
    let summary_calls = source_admission_counts::snapshot().0 - before;
    let polygon = Polygon::new(ring, Vec::new());
    let mut inventory = ExactGeographicChartInventory::new(frame, 1);
    let before = source_admission_counts::snapshot().0;
    inventory.push_polygon(&polygon, SourceRowId(0)).unwrap();
    let actual = source_admission_counts::snapshot().0 - before;
    assert_eq!(actual, summary_calls + inventory.edges.len());
}
#[test]
fn rounded_period_differences_do_not_create_false_circles() {
    let grad_unit = AngularFactor::try_new("grads", std::f64::consts::TAU / 400.0).unwrap();
    let grads = GeographicFrame::try_new(grad_unit, grad_unit, 0.0).unwrap();
    assert!(chart_step_identity_holds(degrees(), 1.0e-20, 360.0));
    assert!(chart_step_identity_holds(degrees(), 360.0, 1.0e-20));
    assert!(chart_step_identity_holds(grads, 40.5, 440.5));
}
#[test]
fn inventory_retains_full_turn_ring_and_source_ownership() {
    let shell = ring(&[
        (0.0, 10.0),
        (90.0, 20.0),
        (180.0, 30.0),
        (-90.0, 20.0),
        (0.0, 10.0),
    ]);
    let hole = ring(&[(10.0, 2.0), (20.0, 3.0), (15.0, 4.0), (10.0, 2.0)]);
    let polygon = Polygon::new(shell, vec![hole]);
    let shapes = [Shape::Polygon(polygon), line(&[(1.0, 1.0), (2.0, 2.0)])];
    let inventory = inventory(degrees(), &shapes);
    let mut budget = ExpansionBudget::new("test", "exact chart");
    let chart = inventory.node(&mut budget).unwrap();
    assert_eq!(inventory.row_count, 2);
    assert_eq!(inventory.polygons.len(), 1);
    assert_eq!(inventory.polygons[0].row, SourceRowId(0));
    assert_eq!(inventory.rings.len(), 2);
    assert_eq!(inventory.rings[0].id, SourceRingId(0));
    assert_eq!(inventory.rings[0].polygon, SourcePolygonId(0));
    assert!(!inventory.rings[0].hole);
    assert_eq!(
        inventory.rings[0].summary.turn(),
        crate::geographic::RingTurn::Full(crate::geographic::TurnDirection::Positive,)
    );
    assert!(!inventory.rings[0].summary.exact_axis_band);
    assert!(matches!(
        inventory.rings[0].summary.domain,
        crate::geographic::RingDomain::FullLongitude(_)
    ));
    assert_eq!(inventory.rings[0].source_edges, 0..4);
    assert_eq!(inventory.rings[1].id, SourceRingId(1));
    assert!(inventory.rings[1].hole);
    assert_eq!(inventory.rings[1].polygon, SourcePolygonId(0));
    assert_eq!(inventory.rings[1].source_edges, 4..7);
    assert_eq!(inventory.edges[0].row, SourceRowId(0));
    assert!(matches!(inventory.edges[0].kind, SourceEdgeKind::Ring {
        polygon: SourcePolygonId(0),
        ring: SourceRingId(0),
        hole: false,
        ..
    }));
    assert_eq!(inventory.edges[7].row, SourceRowId(1));
    for edge in &inventory.edges[..4] {
        let mut ranges = chart
            .atoms
            .iter()
            .flat_map(|atom| atom.owners.iter())
            .filter(|owner| owner.source_edge == edge.id)
            .map(|owner| owner.source_range.clone())
            .collect::<Vec<_>>();
        ranges.sort_unstable_by(|left, right| {
            left.start
                .try_cmp(&right.start)
                .expect("source parameters compare")
        });
        assert!(!ranges.is_empty());
        assert!(
            ranges[0]
                .start
                .try_cmp(&ExactRational::zero())
                .unwrap()
                .is_eq()
        );
        for pair in ranges.windows(2) {
            let [left, right] = pair else {
                unreachable!("windows(2) yields adjacent source parameter ranges");
            };
            assert!(left.end.try_cmp(&right.start).unwrap().is_eq());
        }
        assert!(
            ranges
                .last()
                .unwrap()
                .end
                .try_cmp(&ExactRational::one())
                .unwrap()
                .is_eq()
        );
    }
}

#[test]
fn inventory_retains_multi_turn_ring_count_beyond_summary_class() {
    let shell = ring(&[
        (0.0, 10.0),
        (120.0, 10.0),
        (240.0, 10.0),
        (360.0, 10.0),
        (480.0, 10.0),
        (600.0, 10.0),
        (720.0, 10.0),
        (0.0, 10.0),
    ]);
    let polygon = Polygon::new(shell, Vec::new());
    let inventory = inventory(degrees(), &[Shape::Polygon(polygon)]);
    assert_eq!(
        inventory.rings[0].summary.turn(),
        crate::geographic::RingTurn::Other
    );
    assert_eq!(inventory.rings[0].summary.chart_turn, 2);
}

#[test]
fn exact_chart_split_stays_finite_at_the_maximum_period() {
    let longitude =
        AngularFactor::try_new("maximum-period longitude", std::f64::consts::TAU / f64::MAX)
            .unwrap();
    let latitude = AngularFactor::try_new("degree latitude", std::f64::consts::PI / 180.0).unwrap();
    let frame = GeographicFrame::try_new(longitude, latitude, 0.0).unwrap();
    let half = frame.longitude().half_period();
    let shape = line(&[(-half, 0.0), (half.next_down(), 1.0)]);
    let inventory = inventory(frame, &[shape]);
    let mut budget = ExpansionBudget::new("test", "exact chart");
    let chart = inventory.node(&mut budget).unwrap();
    assert!(!chart.atoms.is_empty());
    assert!(
        chart
            .ports
            .iter()
            .flat_map(|port| port.point.outward_bounds().unwrap())
            .all(f64::is_finite)
    );
}

#[test]
fn exact_inventory_retains_original_zm_endpoints() {
    let start = Point::new_raw_axes(170.0, 0.0, ZOrdinate(Some(10.0)), MOrdinate(Some(100.0)));
    let end = Point::new_raw_axes(-160.0, 1.0, ZOrdinate(Some(20.0)), MOrdinate(Some(200.0)));
    let shape = Shape::LineString(
        LineSeq::try_new(CoordSeq::try_from_points(&[start, end]).unwrap()).unwrap(),
    );
    let inventory = inventory(degrees(), &[shape]);
    assert_eq!(inventory.edges[0].source, [start, end]);
    assert!(
        inventory.pieces[0]
            .source_range
            .start
            .try_cmp(&ExactRational::zero())
            .unwrap()
            .is_eq()
    );
    assert!(
        inventory.pieces[1]
            .source_range
            .end
            .try_cmp(&ExactRational::one())
            .unwrap()
            .is_eq()
    );
}
