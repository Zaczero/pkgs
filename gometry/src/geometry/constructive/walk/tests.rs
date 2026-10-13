use std::num::NonZeroU32;

use super::*;
use crate::geometry::CoordSeq;
use crate::geometry::constructive::offset::{line_intersection, offset_curve_points};
use crate::geometry::constructive::winding::stroke::raw_stroke_loop;

fn assert_plan_count_matches_materialization(
    points: &[Point],
    cyclic: bool,
    distance: f64,
    rule: WalkJoinRule,
    step_angle: f64,
    closes: bool,
) {
    let plan = WalkPlan::new(points, cyclic, distance, rule)
        .unwrap()
        .unwrap();
    let count_budget = ExpansionBudget::new("test", "count");
    let mut count = WalkCount::budgeted(&count_budget);
    plan.emit_into(step_angle, &mut count).unwrap();
    let expected = count.len();
    assert!(count.all_finite());

    let mut output_budget = ExpansionBudget::new("test", "count");
    let columns = materialize_walk(&plan, step_angle, false, &mut output_budget, true)
        .unwrap()
        .unwrap();
    let (mut xs, mut ys, _) = columns.into_columns();
    assert_eq!(xs.len(), expected);
    assert_eq!(ys.len(), expected);
    assert_eq!(output_budget.used(), expected);
    if closes {
        close_xy_loop(&mut xs, &mut ys, &mut output_budget).unwrap();
        let closed_len = expected
            .checked_add(1)
            .expect("closing a test loop adds one coordinate");
        assert_eq!(xs.len(), closed_len);
        assert_eq!(output_budget.used(), closed_len);
    }
}

#[test]
fn reflex_cross_retains_an_exact_endpoint_local_parameter() {
    let length = 2.0_f64.powi(1023);
    let x = 2.0_f64.powi(-52);
    let incoming = Segment {
        start: XY::new(0.0, 0.0),
        end: XY::new(length, 0.0),
    };
    let outgoing = Segment {
        start: XY::new(x, -1.0),
        end: XY::new(x, 1.0),
    };
    let (point, t_in, t_out) = reflex_cross(incoming, outgoing).unwrap().unwrap();
    assert_eq!(point, XY::new(x, 0.0));
    assert!(t_in.uses_exact_ratio());
    assert!(!t_in.is_start() && !t_in.is_end());
    assert_eq!(t_in.fraction().to_bits(), 0.0_f64.to_bits());
    assert_eq!(t_out.fraction().to_bits(), 0.5_f64.to_bits());
}
#[test]
fn carrier_intersection_scales_opposite_extreme_source_differences() {
    let crossing = line_intersection(
        Segment {
            start: XY::new(-1.5e308, 0.0),
            end: XY::new(1.5e308, 0.0),
        },
        Segment {
            start: XY::new(0.0, -1.0),
            end: XY::new(0.0, 1.0),
        },
    )
    .expect("finite carrier crossing");
    assert_eq!(crossing, XY::new(0.0, 0.0));
}
#[test]
fn miter_limit_keeps_the_equality_tie_at_extreme_scales() {
    let join = |scale: f64, limit: f64| {
        let incoming = Segment {
            start: XY::new(-scale, 0.0),
            end: XY::new(3.0 * scale, 0.0),
        };
        let outgoing = Segment {
            start: XY::new(2.0 * scale, -scale),
            end: XY::new(4.0 * scale, scale),
        };
        convex_miter(
            incoming,
            outgoing,
            Point::new_raw_xy(0.0, 0.0),
            scale,
            limit,
        )
    };

    let below = f64::from_bits(3.0_f64.to_bits() - 1);
    let above = f64::from_bits(3.0_f64.to_bits() + 1);
    assert!(matches!(join(1.0, below), WalkJoin::MiterCut { .. }));
    assert!(matches!(join(1.0, 3.0), WalkJoin::Miter { .. }));
    assert!(matches!(join(1.0, above), WalkJoin::Miter { .. }));
    for scale in [1e-200, 1e200] {
        assert!(matches!(join(scale, 2.9), WalkJoin::MiterCut { .. }));
        assert!(matches!(join(scale, 3.0), WalkJoin::Miter { .. }));
    }
}
#[test]
fn walk_keeps_small_turns_styled_and_sends_near_reversals_to_excursions() {
    let points = [
        Point::new_raw_xy(0.0, 0.0),
        Point::new_raw_xy(1.0, 0.0),
        Point::new_raw_xy(2.0, 1e-6),
    ];
    let round = WalkPlan::new(&points, false, 1.0, WalkJoinRule::Arc)
        .unwrap()
        .unwrap();
    assert!(matches!(round.joins[0], WalkJoin::Arc { .. }));
    let bevel = WalkPlan::new(&points, false, 1.0, WalkJoinRule::Bevel)
        .unwrap()
        .unwrap();
    assert!(matches!(bevel.joins[0], WalkJoin::Straight));

    let reversal = [
        Point::new_raw_xy(0.0, 0.0),
        Point::new_raw_xy(1.0, 0.0),
        Point::new_raw_xy(0.0, -1e-6),
    ];
    let folded = WalkPlan::new(&reversal, false, 1.0, WalkJoinRule::Arc)
        .unwrap()
        .unwrap();
    assert!(matches!(folded.joins[0], WalkJoin::Excursion));
}
#[test]
fn stroke_walk_styles_exact_reversals_and_emits_an_exact_semicircle_midpoint() {
    let points = [
        Point::new_raw_xy(0.0, 0.0),
        Point::new_raw_xy(2.0, 0.0),
        Point::new_raw_xy(1.0, 0.0),
    ];
    let round = WalkPlan::new_stroke(&points, false, 1.0, WalkJoinRule::Arc)
        .unwrap()
        .unwrap();
    assert!(matches!(round.joins[0], WalkJoin::ReversalArc { .. }));

    let mut budget = ExpansionBudget::new("test", "reversal arc");
    let columns = materialize_walk(
        &round,
        std::f64::consts::PI / 16.0,
        false,
        &mut budget,
        true,
    )
    .unwrap()
    .unwrap();
    let (xs, ys, _) = columns.into_columns();
    assert!(
        xs.iter()
            .zip(&ys)
            .any(|(&x, &y)| x.to_bits() == 3.0_f64.to_bits() && y.to_bits() == 0.0_f64.to_bits())
    );

    let bevel = WalkPlan::new_stroke(&points, false, 1.0, WalkJoinRule::Bevel)
        .unwrap()
        .unwrap();
    assert!(matches!(bevel.joins[0], WalkJoin::Straight));
    let miter = WalkPlan::new_stroke(&points, false, 1.0, WalkJoinRule::Miter { limit: 5.0 })
        .unwrap()
        .unwrap();
    assert!(matches!(miter.joins[0], WalkJoin::Straight));
}
#[test]
fn one_immutable_walk_plan_counts_and_materializes_every_join_shape() {
    let step = std::f64::consts::FRAC_PI_2 / 8.0;
    // Round arcs on an open walk.
    assert_plan_count_matches_materialization(
        &[
            Point::new_raw_xy(0.0, 0.0),
            Point::new_raw_xy(1.0, 0.0),
            Point::new_raw_xy(1.0, 1.0),
        ],
        false,
        1.0,
        WalkJoinRule::Arc,
        step,
        false,
    );
    // A closed sharp corner exercises miter cuts and the separately charged
    // public closing coordinate.
    let cut_points = [
        Point::new_raw_xy(0.0, 0.0),
        Point::new_raw_xy(12.0, 1.0),
        Point::new_raw_xy(0.0, 2.0),
    ];
    let cut_plan = WalkPlan::new(&cut_points, true, 0.5, WalkJoinRule::Miter { limit: 2.0 })
        .unwrap()
        .unwrap();
    assert!(
        cut_plan
            .joins
            .iter()
            .any(|join| matches!(join, WalkJoin::MiterCut { .. }))
    );
    assert_plan_count_matches_materialization(
        &[
            Point::new_raw_xy(0.0, 0.0),
            Point::new_raw_xy(12.0, 1.0),
            Point::new_raw_xy(0.0, 2.0),
        ],
        true,
        0.5,
        WalkJoinRule::Miter { limit: 2.0 },
        step,
        true,
    );
    // Near-fold excursions and a scale outside the ordinary squared-length
    // range use the same plan/count/materialize lifecycle.
    assert_plan_count_matches_materialization(
        &[
            Point::new_raw_xy(0.0, 0.0),
            Point::new_raw_xy(1.0, 0.0),
            Point::new_raw_xy(0.0, -1e-6),
        ],
        false,
        1.0,
        WalkJoinRule::Arc,
        step,
        false,
    );
    let excursion_points = [
        Point::new_raw_xy(0.0, 0.0),
        Point::new_raw_xy(1.0, 0.0),
        Point::new_raw_xy(0.0, -1e-6),
    ];
    let excursion_plan = WalkPlan::new(&excursion_points, false, 1.0, WalkJoinRule::Arc)
        .unwrap()
        .unwrap();
    assert!(
        excursion_plan
            .joins
            .iter()
            .any(|join| matches!(join, WalkJoin::Excursion))
    );
    assert_plan_count_matches_materialization(
        &[
            Point::new_raw_xy(0.0, 0.0),
            Point::new_raw_xy(1e155, 0.0),
            Point::new_raw_xy(1e155, 1e155),
        ],
        false,
        1e150,
        WalkJoinRule::Bevel,
        step,
        false,
    );
}
#[test]
fn stroke_reuses_each_validated_walk_plan_for_count_and_output() {
    let chain = [
        Point::new_raw_xy(0.0, 0.0),
        Point::new_raw_xy(1.0, 0.0),
        Point::new_raw_xy(1.0, 1.0),
    ];
    let mut budget = ExpansionBudget::new("buffer", "quadrant_segments");
    reset_walk_plan_construction_count();
    // At distance 1 the inside offset reaches the bend endpoint and is
    // correctly demoted to an excursion; keep both leg crossings interior
    // so this fixture exercises the clean fast-path certificate.
    let (columns, clean) = raw_stroke_loop(
        &chain,
        0.1,
        BufferCapStyle::Round,
        WalkJoinRule::Arc,
        NonZeroU32::new(8).unwrap(),
        &mut budget,
    )
    .unwrap()
    .unwrap();
    assert!(clean);
    assert_eq!(walk_plan_construction_count(), 2);
    assert_eq!(columns.0.len(), columns.1.len());
    // A deliberately doubled charge would fail this equality, while a
    // divergent second plan would fail the exact count/output assertion.
    assert_eq!(budget.used(), columns.0.len());
}
#[test]
fn styled_offset_restores_source_order_and_ordinate_provenance() {
    let line = CoordSeq::try_from_vecs(
        [0.0, 2.0, 2.0].into(),
        [0.0, 0.0, 2.0].into(),
        Some([10.0, 20.0, 30.0].into()),
        Some([100.0, 200.0, 300.0].into()),
    )
    .unwrap();
    let mut budget = ExpansionBudget::new("offset_curve", "quadrant_segments");
    reset_walk_plan_construction_count();
    let output = offset_curve_points(
        &line,
        1.0,
        WalkJoinRule::Miter { limit: 5.0 },
        NonZeroU32::new(8).unwrap(),
        &mut budget,
    )
    .unwrap()
    .unwrap();
    assert_eq!(walk_plan_construction_count(), 1);
    assert_eq!(budget.used(), output.len());
    assert_eq!(
        output
            .iter()
            .map(|point| (point.x, point.y))
            .collect::<Vec<_>>(),
        vec![(0.0, 1.0), (1.0, 1.0), (1.0, 2.0)]
    );
    assert_eq!(
        output.iter().map(|point| point.z()).collect::<Vec<_>>(),
        vec![Some(10.0), Some(20.0), Some(30.0)]
    );
    assert_eq!(
        output.iter().map(|point| point.m()).collect::<Vec<_>>(),
        vec![Some(100.0), Some(200.0), Some(300.0)]
    );
}
