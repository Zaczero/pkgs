use std::num::NonZeroU32;

use crate::Positive;
use crate::geometry::constructive::offset::{line_intersection, offset_curve_points};
use crate::geometry::constructive::walk::{
    WalkCount, WalkJoin, WalkPlan, close_xy_loop, convex_miter, materialize_walk, reflex_cross,
};
use crate::geometry::constructive::winding::raw_stroke_loop;
use crate::geometry::constructive::{
    WalkJoinRule, reset_walk_plan_construction_count, walk_plan_construction_count, winding_buffer,
    winding_erosion, winding_stroke,
};
use crate::geometry::exact_arith::product_sum::ExactSum;
use crate::geometry::overlay::{partition_calls, reset_partition_calls};
use crate::geometry::{
    BufferCapStyle, BufferJoinStyle, CoordSeq, ExpansionBudget, GeometryErrorKind, LineSeq,
    MAX_NESTING, MultiPolygon, Point, Polygon, Ring, Segment, Shape, XY, axis_pow2_scale,
    scaled_residual,
};

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

#[test]
fn buffer_keeps_a_vertex_below_the_removed_input_simplification_tolerance() {
    let line = Shape::LineString(
        LineSeq::try_new(
            CoordSeq::try_from_points(&[
                Point::new_raw_xy(0.0, 0.0),
                Point::new_raw_xy(1.0, 5e-4),
                Point::new_raw_xy(2.0, 0.0),
            ])
            .unwrap(),
        )
        .unwrap(),
    );
    let buffered = line
        .buffer_with_style(
            1.0,
            BufferCapStyle::Flat,
            BufferJoinStyle::Bevel,
            NonZeroU32::new(1).unwrap(),
            Positive::try_new("miter_limit", 5.0).unwrap(),
        )
        .unwrap();
    let bounds = buffered.bounds().unwrap();
    assert!(bounds.maxy() > 1.0004, "vertex was removed: {bounds:?}");
}

#[test]
fn offset_curve_unavailability_stays_empty_while_budget_errors_propagate() {
    let overflow = CoordSeq::try_from_points(&[
        Point::new_raw_xy(1.5e308, 0.0),
        Point::new_raw_xy(1.5e308, 1.0),
    ])
    .unwrap();
    let mut overflow_budget = ExpansionBudget::new("offset_curve", "quadrant_segments");
    assert!(
        offset_curve_points(
            &overflow,
            -1.5e308,
            WalkJoinRule::Arc,
            NonZeroU32::new(8).unwrap(),
            &mut overflow_budget,
        )
        .unwrap()
        .is_none()
    );

    let line =
        CoordSeq::try_from_points(&[Point::new_raw_xy(0.0, 0.0), Point::new_raw_xy(1.0, 0.0)])
            .unwrap();
    let mut budget = ExpansionBudget::new("offset_curve", "quadrant_segments");
    budget.add(crate::geometry::GENERATED_ITEM_LIMIT).unwrap();
    offset_curve_points(
        &line,
        1.0,
        WalkJoinRule::Arc,
        NonZeroU32::new(1).unwrap(),
        &mut budget,
    )
    .unwrap_err();
}

#[test]
fn buffer_accepts_an_edge_whose_raw_delta_overflows_when_offsets_are_finite() {
    // Mutation: restoring the removed raw-delta admission guard rejects this
    // source before the constructive path gets to its representable offsets.
    let start = Point::new_raw_xy(-1.5e308, 0.0);
    let end = Point::new_raw_xy(1.5e308, 0.0);
    assert!(!(end.x - start.x).is_finite());
    let line = Shape::LineString(
        LineSeq::try_new(CoordSeq::try_from_points(&[start, end]).unwrap()).unwrap(),
    );
    let radius = 1.0e300;
    let buffered = line
        .buffer_with_style(
            radius,
            BufferCapStyle::Round,
            BufferJoinStyle::Round,
            NonZeroU32::new(8).unwrap(),
            Positive::try_new("miter_limit", 5.0).unwrap(),
        )
        .expect("finite emitted offsets");

    assert!(!buffered.is_empty());
    let scale = axis_pow2_scale(start.x.abs().max(end.x.abs()).max(radius));
    let end_x = scaled_residual(end.x, start.x, scale);
    let radius = radius * scale;
    buffered
        .try_for_each_point::<()>(|point| {
            assert!(point.x.is_finite() && point.y.is_finite());
            let x = scaled_residual(point.x, start.x, scale);
            let y = scaled_residual(point.y, 0.0, scale);
            let distance = (x - x.clamp(0.0, end_x)).hypot(y);
            assert!(
                distance <= radius + 32.0 * f64::EPSILON,
                "vertex is outside its scaled source buffer: {distance} > {radius}"
            );
            Ok(())
        })
        .expect("all buffered vertices are finite");
}

#[test]
fn buffer_keeps_vertical_diagonal_and_tiny_source_geometry_finite() {
    let cases = [
        (
            Point::new_raw_xy(-1.5e308, -1.5e308),
            Point::new_raw_xy(1.5e308, 1.5e308),
            1.0e300,
        ),
        (
            Point::new_raw_xy(1.5e308, -1.5e308),
            Point::new_raw_xy(1.5e308, 1.5e308),
            1.0e300,
        ),
        (
            Point::new_raw_xy(0.0, 0.0),
            Point::new_raw_xy(1.0e-200, 2.0e-200),
            1.0e-200,
        ),
    ];
    for (start, end, radius) in cases {
        let line = Shape::LineString(
            LineSeq::try_new(CoordSeq::try_from_points(&[start, end]).unwrap()).unwrap(),
        );
        for cap_style in [
            BufferCapStyle::Round,
            BufferCapStyle::Flat,
            BufferCapStyle::Square,
        ] {
            let buffered = line
                .buffer_with_style(
                    radius,
                    cap_style,
                    BufferJoinStyle::Miter,
                    NonZeroU32::new(4).unwrap(),
                    Positive::try_new("miter_limit", 5.0).unwrap(),
                )
                .expect("representable buffer output");
            assert!(!buffered.is_empty());
            buffered
                .try_for_each_point::<()>(|point| {
                    assert!(point.x.is_finite() && point.y.is_finite());
                    Ok(())
                })
                .expect("all emitted coordinates are finite");
        }
    }
}

#[test]
fn buffer_rejects_a_coordinate_that_the_requested_offset_cannot_represent() {
    let line = Shape::LineString(
        LineSeq::try_new(
            CoordSeq::try_from_points(&[
                Point::new_raw_xy(1.5e308, 0.0),
                Point::new_raw_xy(1.5e308, 1.0),
            ])
            .unwrap(),
        )
        .unwrap(),
    );
    let error = line.buffer_with_style(
        1.5e308,
        BufferCapStyle::Flat,
        BufferJoinStyle::Round,
        NonZeroU32::new(1).unwrap(),
        Positive::try_new("miter_limit", 5.0).unwrap(),
    );
    assert!(
        error
            .expect_err("the right-side offset exceeds binary64")
            .to_string()
            .contains("non-finite")
    );
}

#[test]
fn affine_scale_keeps_the_independent_reciprocal_axis() {
    let source = Shape::Point(Point::new(1e308, 1e-300).unwrap());
    let transformed = source.scale(0.5, 1.0, (-1e308, 0.0)).unwrap();
    let Shape::Point(point) = transformed else {
        panic!("point scaling must preserve its kind")
    };
    assert_eq!(point.x.to_bits(), 0.0_f64.to_bits());
    assert_eq!(point.y.to_bits(), 1e-300_f64.to_bits());
}

#[test]
fn affine_origin_row_uses_both_input_pivots() {
    assert_eq!(
        super::edit::affine_about_origin_ordinate(1.0, 0.0, 7.0, 2.0, 7.0, 3.0, 0.0).unwrap(),
        8.0
    );
}

#[test]
fn affine_origin_prepared_rows_match_exact_five_product_rows() {
    let [coefficient_x, coefficient_y, coefficient_z, coefficient_w] = [2.0, -0.5, 0.25, 1.5];
    let (origin_x, origin_y) = (7.0, -5.0);
    let transform = super::edit::AffineLinear::new([
        coefficient_x,
        coefficient_y,
        coefficient_z,
        coefficient_w,
    ])
    .unwrap()
    .at((origin_x, origin_y))
    .unwrap();

    for [value_x, value_y] in [
        [11.0, 13.0],
        [origin_x.next_up(), origin_y.next_down()],
        [2.0_f64.powi(500), -(2.0_f64.powi(499))],
        [f64::from_bits(1), -f64::from_bits(1)],
    ] {
        let expected_x = exact_affine_origin_row([
            coefficient_x,
            coefficient_y,
            origin_x,
            origin_x,
            origin_y,
            value_x,
            value_y,
        ]);
        let expected_y = exact_affine_origin_row([
            coefficient_z,
            coefficient_w,
            origin_y,
            origin_x,
            origin_y,
            value_x,
            value_y,
        ]);
        let (actual_x, actual_y) = transform.map_xy(value_x, value_y).unwrap();
        assert_eq!(actual_x.to_bits(), expected_x.to_bits());
        assert_eq!(actual_y.to_bits(), expected_y.to_bits());
    }

    assert!(
        super::edit::fast_affine_about_origin_ordinate(
            coefficient_x,
            coefficient_y,
            origin_x,
            origin_x,
            origin_y,
            11.0,
            13.0,
        )
        .is_some(),
        "ordinary prepared affine rows should exercise the certified tier"
    );
}

fn exact_affine_origin_row(row: [f64; 7]) -> f64 {
    let [
        coefficient_x,
        coefficient_y,
        output_origin,
        origin_x,
        origin_y,
        value_x,
        value_y,
    ] = row;
    let mut sum = ExactSum::zero();
    sum.add_product(coefficient_x, value_x);
    sum.add_product(coefficient_y, value_y);
    sum.add_product(output_origin, 1.0);
    sum.subtract_product(coefficient_x, origin_x);
    sum.subtract_product(coefficient_y, origin_y);
    sum.round_to_binary64()
}

fn assert_affine_origin_row_matches_exact(row: [f64; 7], case: &str) -> bool {
    let expected = exact_affine_origin_row(row);
    let [
        coefficient_x,
        coefficient_y,
        output_origin,
        origin_x,
        origin_y,
        value_x,
        value_y,
    ] = row;
    let fast = super::edit::fast_affine_about_origin_ordinate(
        coefficient_x,
        coefficient_y,
        output_origin,
        origin_x,
        origin_y,
        value_x,
        value_y,
    );
    if let Some(fast) = fast {
        assert_eq!(
            fast.to_bits(),
            expected.to_bits(),
            "fast affine row misrounded {case}"
        );
    }
    assert_eq!(
        super::edit::affine_about_origin_ordinate(
            coefficient_x,
            coefficient_y,
            output_origin,
            origin_x,
            origin_y,
            value_x,
            value_y,
        )
        .unwrap()
        .to_bits(),
        expected.to_bits(),
        "affine row disagrees with ExactSum for {case}"
    );
    fast.is_some()
}

#[test]
fn affine_origin_fast_filter_matches_exact_five_product_rows() {
    // Captured with seed 0x9e3779b97f4a7c15 at reciprocal exponent strata
    // spanning the normal range. Keep the operands as bits for exact replay.
    const RANDOM_ROWS: [[u64; 7]; 16] = [
        [
            0xFFD8_4FB1_7AF4_FD6F,
            0x8014_805E_6EA0_7EE9,
            0x42BD_990E_0FC1_A065,
            0x801E_78A1_9F40_959C,
            0x7FEE_3CFF_3EE1_F126,
            0x801F_2956_7AC5_9520,
            0x7FEF_E218_C577_1AA3,
        ],
        [
            0x783D_C9B8_883C_11B6,
            0x801B_C685_7C93_F490,
            0x393F_59B4_1424_8874,
            0x87B3_CBCF_732D_E3AA,
            0xFFD0_B4BB_6D4C_DE70,
            0x87B7_0A80_F4E3_B996,
            0xFFD1_FA12_D088_9A08,
        ],
        [
            0xEBB0_53B9_2C61_7FD5,
            0x07B5_6501_48BC_7631,
            0x2D87_7C25_5C40_5BD8,
            0x1437_8443_3201_2B5B,
            0xF838_14EB_797F_DC9D,
            0x143E_1020_B940_19B3,
            0xF833_46C3_F611_669F,
        ],
        [
            0x5F32_48F1_A65B_6958,
            0x1432_8F8F_633B_6AE7,
            0xAE73_E0DE_5253_2BC4,
            0xA0B5_8BBD_1FB5_F7D4,
            0x6BBD_6D2C_30F1_7600,
            0xA0BF_8550_4A76_7ACE,
            0x6BB6_AD30_CB6E_15E6,
        ],
        [
            0xD2BD_B7A6_4E66_6019,
            0x20B9_6C99_53FB_F140,
            0xC7C4_4D31_C55B_A94D,
            0xAD3C_074E_8D32_7690,
            0x5F3D_5714_8149_52F2,
            0xAD39_F615_5717_F3B9,
            0x5F30_1CFB_E813_60AA,
        ],
        [
            0xC639_1999_4E70_62AA,
            0x2D30_B835_169D_E770,
            0x3B9F_DE60_1ADC_9055,
            0x39B3_AA27_6D03_4DD1,
            0xD2BB_1118_E7CC_A720,
            0x39BE_12E6_F0A9_0B42,
            0x52BE_A84E_FCAC_9BC0,
        ],
        [
            0x4001_8ED4_D780_ECA4,
            0x39B7_0D1B_5DCD_9B83,
            0xAF83_7AAB_D82D_57BF,
            0x3FE7_98ED_91F0_4982,
            0xC631_2335_3E18_A87C,
            0x3FE2_EC95_BD65_3821,
            0x4638_2C52_9DC8_0E67,
        ],
        [
            0x3FF9_75D1_7EE6_D2C7,
            0xBFE0_8C2F_CD46_3CE9,
            0xCF12_37FD_DA72_0179,
            0x3FF7_B0CF_F391_59F4,
            0x4009_BD02_DD37_9F19,
            0x3FF1_03AB_D742_084F,
            0xC00D_CDD5_546E_424D,
        ],
        [
            0xBFED_8D85_E94F_580B,
            0x3FF9_49A8_0D8A_D901,
            0xBA53_84AD_7709_AEF4,
            0xC00B_D6E8_0622_7885,
            0xBFFA_D757_5A8D_EDC1,
            0x4004_6C56_3EC0_842B,
            0x3FFA_E2F4_FFDF_C6A4,
        ],
        [
            0x39B8_38FA_7C25_E2CB,
            0x4001_B058_3758_395C,
            0x443A_1E4A_FAE6_D0AA,
            0xC631_7C00_3BA9_0071,
            0x3FE6_4450_8DF9_8F59,
            0x4630_A6AB_8DA1_7FC2,
            0xBFE2_77F0_B91E_9239,
        ],
        [
            0xAD3D_F287_DEC6_9FDB,
            0x4630_8F0C_328C_4163,
            0xB4E9_32FB_F8E0_1189,
            0xD2B0_54EC_1AF7_CB2C,
            0x39B2_0A93_50FA_9579,
            0xD2B5_7C73_A5A4_C2A5,
            0x39B2_E6AE_20B6_9887,
        ],
        [
            0xA0B0_08CE_AB2D_A312,
            0xD2B7_EC25_E3A4_34D3,
            0xC95E_364A_F1A7_178C,
            0xDF31_5D6E_F8C2_344B,
            0xAD30_A662_B53C_C6D8,
            0xDF36_04BF_2CB5_EA7D,
            0xAD30_D238_C473_644C,
        ],
        [
            0x9439_88CF_ED18_8B26,
            0xDF32_A8BD_9FF1_98A2,
            0x421A_FEA0_7BF5_74CD,
            0xEBB4_43D4_5679_2A33,
            0xA0B3_3C8F_913C_3E94,
            0x6BB3_80C3_1CC1_D46E,
            0xA0BD_3F3D_413A_35B3,
        ],
        [
            0x07B6_CAD5_7C9A_564C,
            0xEBBB_34CC_DEB9_B095,
            0x493F_653C_0CD0_1EFD,
            0x783B_0138_BE29_5B63,
            0x9430_16F8_6144_669F,
            0xF83F_A13F_C81B_86C5,
            0x1435_B7B3_3B2F_3B44,
        ],
        [
            0x0017_06A7_7196_42E4,
            0x783F_C2A0_80CD_16D0,
            0xAEBB_774B_E6D7_C4C6,
            0x7FD6_8C9B_CECB_5F79,
            0x07BD_A608_8256_CC4C,
            0x7FD5_8CD6_30FF_8060,
            0x07B4_BFA9_2A35_D698,
        ],
        [
            0x801B_AB09_7775_93B4,
            0xFFDA_6E63_F8FE_B755,
            0x2F79_A59F_C1D0_74FD,
            0x7FE7_2BE5_C939_15B3,
            0x0014_CAD7_5892_7226,
            0xFFE6_C686_B048_52E4,
            0x001D_907D_1EE2_C436,
        ],
    ];
    let ulp_at_one = 1.0_f64.next_up().to_bits();
    let term = 3.0 * 2.0_f64.powi(-109);
    let above_tie_by_lost_residual = [
        1.0,
        2.0_f64.powi(-53),
        -(2.0_f64.powi(-107) + 2.0_f64.powi(-117)),
        -term,
        -3.0 * 2.0_f64.powi(-56),
        1.0,
        1.0,
    ];
    assert_eq!(
        exact_affine_origin_row(above_tie_by_lost_residual).to_bits(),
        ulp_at_one,
        "the exact pivot-relative expression lies just above the halfway value"
    );
    assert_affine_origin_row_matches_exact(above_tie_by_lost_residual, "near-tie pivot");

    let ordinary = [2.0, -0.5, 7.0, 3.0, -5.0, 11.0, 13.0];
    assert_eq!(
        exact_affine_origin_row(ordinary).to_bits(),
        14.0_f64.to_bits()
    );
    assert!(assert_affine_origin_row_matches_exact(
        ordinary,
        "two pivots"
    ));

    let catastrophic_cancellation = [
        1.0 + 2.0_f64.powi(-52),
        -1.0,
        0.0,
        0.0,
        0.0,
        2.0_f64.powi(500),
        2.0_f64.powi(500),
    ];
    assert_eq!(
        exact_affine_origin_row(catastrophic_cancellation).to_bits(),
        2.0_f64.powi(448).to_bits()
    );
    assert_affine_origin_row_matches_exact(catastrophic_cancellation, "cancellation");

    for (case, output_origin, expected) in [
        ("exact tie", 0.0, 1.0),
        (
            "just above tie",
            2.0_f64.powi(-106),
            f64::from_bits(ulp_at_one),
        ),
        ("just below tie", -(2.0_f64.powi(-106)), 1.0),
    ] {
        let row = [1.0, 2.0_f64.powi(-53), output_origin, 0.0, 0.0, 1.0, 1.0];
        assert_eq!(exact_affine_origin_row(row).to_bits(), expected.to_bits());
        assert_affine_origin_row_matches_exact(row, case);
    }

    let minimum_subnormal = f64::from_bits(1);
    let subnormal_row = [1.0, 0.0, 0.0, 0.0, 0.0, minimum_subnormal, 0.0];
    assert_eq!(
        exact_affine_origin_row(subnormal_row).to_bits(),
        minimum_subnormal.to_bits()
    );
    assert_affine_origin_row_matches_exact(subnormal_row, "subnormal input and result");

    let near_overflow = [
        2.0_f64.powi(500),
        0.0,
        0.0,
        0.0,
        0.0,
        2.0_f64.powi(500),
        0.0,
    ];
    assert_eq!(
        exact_affine_origin_row(near_overflow).to_bits(),
        2.0_f64.powi(1000).to_bits()
    );
    assert_affine_origin_row_matches_exact(near_overflow, "near-overflow product");

    let splitter_overflow = [
        2.0_f64.powi(1000),
        0.0,
        0.0,
        0.0,
        0.0,
        2.0_f64.powi(-900),
        0.0,
    ];
    assert_eq!(
        exact_affine_origin_row(splitter_overflow).to_bits(),
        2.0_f64.powi(100).to_bits()
    );
    assert_affine_origin_row_matches_exact(
        splitter_overflow,
        "splitter overflow with a finite product",
    );

    let mut certified = 0_usize;
    for (index, bits) in RANDOM_ROWS.into_iter().enumerate() {
        let row = bits.map(f64::from_bits);
        if assert_affine_origin_row_matches_exact(row, &format!("random full-range row {index}")) {
            certified = certified.saturating_add(1);
        }
    }
    assert!(
        certified > 0,
        "the normal-coordinate corpus exercises the fast tier"
    );
}

#[cfg(test)]
mod winding_buffer_tests {
    use super::*;
    use crate::geometry::Coordinates as _;
    use crate::geometry::constructive::winding::ring_as_open_line;

    fn ring(points: &[(f64, f64)]) -> Ring {
        Ring::from_closed(
            CoordSeq::try_from_points(
                &points
                    .iter()
                    .map(|&(x, y)| Point::new(x, y).unwrap())
                    .collect::<Vec<_>>(),
            )
            .unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn open_degenerate_ring_preserves_ordinates_without_allocating() {
        let source = CoordSeq::try_from_vecs(
            vec![1.0, 2.0, 3.0, 1.0],
            vec![4.0, 5.0, 6.0, 4.0],
            Some(vec![7.0, 8.0, 9.0, 7.0]),
            Some(vec![10.0, 11.0, 12.0, 10.0]),
        )
        .unwrap();
        let (open, allocations) =
            crate::test_support::allocation_count(|| ring_as_open_line(&source));
        assert_eq!(allocations, 0);
        drop(source);
        assert_eq!(open.xs(), &[1.0, 2.0, 3.0]);
        assert_eq!(open.ys(), &[4.0, 5.0, 6.0]);
        assert_eq!(open.zs().unwrap(), &[7.0, 8.0, 9.0]);
        assert_eq!(open.ms().unwrap(), &[10.0, 11.0, 12.0]);
    }

    #[test]
    fn degenerate_buffer_at_collection_limit_returns_an_error() {
        let degenerate = Shape::MultiPolygon(
            MultiPolygon::from_dense(vec![Polygon::new(
                ring(&[(0.0, 0.0), (1.0, 0.0), (2.0, 0.0), (0.0, 0.0)]),
                Vec::new(),
            )])
            .unwrap(),
        );
        let mut nested = degenerate;
        for _ in 0..MAX_NESTING {
            nested = Shape::geometry_collection(vec![nested]).unwrap();
        }
        let error = nested.buffer(1.0).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("geometry collection nesting exceeds the limit of 128")
        );
    }

    /// Full annihilation under erosion must yield the empty polygon even
    /// for deep-notched shells whose inverted inward loop winds the whole
    /// neighborhood positively (the fuzz repro that caught a divergence
    /// between the engine and the reference construction).
    #[test]
    fn erosion_annihilates_deep_notched_shell() {
        let shell = ring(&[
            (-0.514_876_326_055_279_8, 48.442_629_193_004_55),
            (3.661_534_463_294_989_5, 48.442_629_193_004_55),
            (3.861_534_463_294_989_7, 50.911_843_795_191_2),
            (4.061_534_463_294_99, 48.442_629_193_004_55),
            (6.096_036_601_959_777, 48.442_629_193_004_55),
            (6.096_036_601_959_777, 54.615_665_698_471_18),
            (-0.514_876_326_055_279_8, 54.615_665_698_471_18),
            (-0.514_876_326_055_279_8, 48.442_629_193_004_55),
        ]);
        let polygon = Polygon::new(shell, Vec::new());
        let eroded = winding_erosion(
            std::slice::from_ref(&polygon),
            3.938_095_265_107_087_5,
            WalkJoinRule::Arc,
            NonZeroU32::new(8).unwrap(),
        )
        .unwrap();
        assert!(eroded.is_empty(), "expected empty, got {eroded:?}");
    }

    /// The winding-number buffer engine (the routed general positive
    /// round-join path): correctness pins for the cases that killed
    /// earlier designs.
    #[test]
    fn winding_engine_handles_holes_notches_and_inversions() {
        // Surviving square hole: sharp shrunk corners, exact area.
        let with_hole = Polygon::new(
            ring(&[
                (0.0, 0.0),
                (10.0, 0.0),
                (10.0, 10.0),
                (0.0, 10.0),
                (0.0, 0.0),
            ]),
            vec![ring(&[
                (4.0, 4.0),
                (6.0, 4.0),
                (6.0, 6.0),
                (4.0, 6.0),
                (4.0, 4.0),
            ])],
        );
        let buffered = winding_buffer(
            std::slice::from_ref(&with_hole),
            0.5,
            WalkJoinRule::Arc,
            NonZeroU32::new(4).unwrap(),
        )
        .unwrap();
        let expected =
            100.0 + 4.0 * (10.0 * 0.5) + 0.5 * 16.0 * 0.25 * (std::f64::consts::PI / 8.0).sin()
                - 1.0;
        assert!(
            (buffered.area() - expected).abs() < 1e-9,
            "{}",
            buffered.area()
        );

        // Vanished hole: distance beyond the inradius cancels exactly.
        let vanished = winding_buffer(
            std::slice::from_ref(&with_hole),
            1.5,
            WalkJoinRule::Arc,
            NonZeroU32::new(4).unwrap(),
        )
        .unwrap();
        let solid = Polygon::new(
            ring(&[
                (0.0, 0.0),
                (10.0, 0.0),
                (10.0, 10.0),
                (0.0, 10.0),
                (0.0, 0.0),
            ]),
            Vec::new(),
        );
        let solid_buffered = winding_buffer(
            std::slice::from_ref(&solid),
            1.5,
            WalkJoinRule::Arc,
            NonZeroU32::new(4).unwrap(),
        )
        .unwrap();
        assert!((vanished.area() - solid_buffered.area()).abs() < 1e-9);

        // Deep notch seals; the result covers the input.
        let notched = Polygon::new(
            ring(&[
                (0.0, 0.0),
                (4.3, 0.0),
                (4.5, 1.8),
                (4.7, 0.0),
                (9.0, 0.0),
                (9.0, 6.0),
                (0.0, 6.0),
                (0.0, 0.0),
            ]),
            Vec::new(),
        );
        let sealed = winding_buffer(
            std::slice::from_ref(&notched),
            0.3,
            WalkJoinRule::Arc,
            NonZeroU32::new(2).unwrap(),
        )
        .unwrap();
        assert!(sealed.covers(&Shape::Polygon(notched)).unwrap());
    }

    /// Miter and bevel join rules: a square's 90-degree corners are well
    /// within the default limit, so the miter expansion is the exact
    /// larger square and the bevel expansion cuts each corner to a
    /// half-`d²` triangle. Erosion of a convex shape never touches the
    /// outside-join rule (all walk turns are inside turns), so every
    /// style erodes a square to the same smaller square.
    #[test]
    fn miter_and_bevel_rules_are_exact_on_squares() {
        let square = Polygon::new(
            ring(&[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (0.0, 0.0)]),
            Vec::new(),
        );
        let parts = std::slice::from_ref(&square);
        let miter = WalkJoinRule::Miter { limit: 5.0 };
        let mitered = winding_buffer(parts, 1.0, miter, NonZeroU32::new(8).unwrap()).unwrap();
        assert!((mitered.area() - 36.0).abs() < 1e-12, "{}", mitered.area());
        let bevelled =
            winding_buffer(parts, 1.0, WalkJoinRule::Bevel, NonZeroU32::new(8).unwrap()).unwrap();
        assert!(
            (bevelled.area() - 34.0).abs() < 1e-12,
            "{}",
            bevelled.area()
        );
        for rule in [WalkJoinRule::Arc, WalkJoinRule::Bevel, miter] {
            let eroded = winding_erosion(parts, 1.0, rule, NonZeroU32::new(8).unwrap()).unwrap();
            assert!((eroded.area() - 4.0).abs() < 1e-12, "{}", eroded.area());
        }
    }

    /// Over-limit corners clip flat at exactly `limit * distance` along
    /// the corner bisector (continuous in the corner angle), leaving the area
    /// strictly between the bevel and the unlimited miter.
    #[test]
    fn sharp_miter_clips_at_the_limit_reach() {
        // Sharp spike at (12, 1): ratio 1/sin(theta/2) far above 2.
        let spike = Polygon::new(
            ring(&[(0.0, 0.0), (12.0, 1.0), (0.0, 2.0), (0.0, 0.0)]),
            Vec::new(),
        );
        let parts = std::slice::from_ref(&spike);
        let distance = 0.5;
        let limit = 2.0;
        let clipped = winding_buffer(
            parts,
            distance,
            WalkJoinRule::Miter { limit },
            NonZeroU32::new(8).unwrap(),
        )
        .unwrap();
        let bevelled = winding_buffer(
            parts,
            distance,
            WalkJoinRule::Bevel,
            NonZeroU32::new(8).unwrap(),
        )
        .unwrap();
        let unlimited = winding_buffer(
            parts,
            distance,
            WalkJoinRule::Miter { limit: 1e9 },
            NonZeroU32::new(8).unwrap(),
        )
        .unwrap();
        assert!(bevelled.area() < clipped.area() && clipped.area() < unlimited.area());
        // The farthest output vertex from the spike tip sits at the clip:
        // its bisector projection is exactly `limit * distance`.
        let tip = Point::new(12.0, 1.0).unwrap();
        let Shape::Polygon(out) = &clipped else {
            panic!("expected a polygon, got {clipped:?}");
        };
        let reach = out
            .shell
            .coords()
            .iter_coords()
            .map(|point| {
                // Bisector at the tip points along +x (symmetric spike).
                point.x - tip.x
            })
            .fold(f64::NEG_INFINITY, f64::max);
        assert!(
            (reach - limit * distance).abs() < 1e-9,
            "clip reach {reach}"
        );
    }

    /// Convex erosion certificate: accept a simple inward offset of a
    /// convex hole-free shell; reject over-erosion past the inscribed
    /// radius (empty via winding fallback). Concave/styled cases stay
    /// on the winding path (see bevel pin below).
    #[test]
    fn convex_erosion_certificate_accepts_box_rejects_past_inradius() {
        use super::super::convex_buffer;

        let box_poly = Polygon::new(
            ring(&[
                (0.0, 0.0),
                (10.0, 0.0),
                (10.0, 10.0),
                (0.0, 10.0),
                (0.0, 0.0),
            ]),
            Vec::new(),
        );
        let q = NonZeroU32::new(8).unwrap();
        // Accept: distance 1 < inradius 5 → simple eroded square area 64.
        let accepted = convex_buffer(&box_poly, -1.0, WalkJoinRule::Arc, q).unwrap();
        assert!(
            (accepted.area() - 64.0).abs() < 1e-12,
            "accepted area {}",
            accepted.area()
        );
        assert!(!accepted.is_empty());
        // Reject certificate past inradius: convex_buffer returns None and
        // winding_erosion yields empty (the public buffer path composes both).
        assert!(
            convex_buffer(&box_poly, -6.0, WalkJoinRule::Arc, q).is_none(),
            "over-erosion must reject the certificate"
        );
        let empty =
            winding_erosion(std::slice::from_ref(&box_poly), 6.0, WalkJoinRule::Arc, q).unwrap();
        assert!(empty.is_empty(), "expected empty, got {empty:?}");
        // Concave L-shape must never take the certificate (not convex).
        let l_shape = Polygon::new(
            ring(&[
                (0.0, 0.0),
                (5.0, 0.0),
                (5.0, 2.0),
                (2.0, 2.0),
                (2.0, 5.0),
                (0.0, 5.0),
                (0.0, 0.0),
            ]),
            Vec::new(),
        );
        assert!(
            convex_buffer(&l_shape, -1.2, WalkJoinRule::Bevel, q).is_none(),
            "concave input must not enter the certificate path"
        );
    }

    /// SEMANTICS PIN — styled erosion is the exact monotone offset
    /// construction. The bevel allowance at a reflex vertex survives even
    /// as an isolated component. Eroding this L by 1.2 annihilates both
    /// arms; the bevel chord across the grown reflex corner keeps the exact
    /// triangle `{x >= 1.2, y >= 1.2, x + y <= 2.8}` (area 0.08), which
    /// shrinks continuously to nothing at `d = 4/3`.
    #[test]
    fn bevel_erosion_keeps_the_exact_corner_allowance() {
        let l_shape = Polygon::new(
            ring(&[
                (0.0, 0.0),
                (5.0, 0.0),
                (5.0, 2.0),
                (2.0, 2.0),
                (2.0, 5.0),
                (0.0, 5.0),
                (0.0, 0.0),
            ]),
            Vec::new(),
        );
        let parts = std::slice::from_ref(&l_shape);
        let kept =
            winding_erosion(parts, 1.2, WalkJoinRule::Bevel, NonZeroU32::new(8).unwrap()).unwrap();
        assert!((kept.area() - 0.08).abs() < 1e-12, "{}", kept.area());
        let gone = winding_erosion(
            parts,
            4.0 / 3.0 + 1e-9,
            WalkJoinRule::Bevel,
            NonZeroU32::new(8).unwrap(),
        )
        .unwrap();
        assert!(gone.is_empty(), "expected empty, got {gone:?}");
    }

    /// Stroke joins follow the same rules: an L-shaped flat-capped stroke
    /// has exact miter (full corner square) and bevel (corner chord)
    /// areas.
    #[test]
    fn stroke_join_rules_are_exact_on_right_angles() {
        let chain = CoordSeq::try_from_points(&[
            Point::new(0.0, 0.0).unwrap(),
            Point::new(2.0, 0.0).unwrap(),
            Point::new(2.0, 2.0).unwrap(),
        ])
        .unwrap();
        let chains = [&chain];
        let miter = WalkJoinRule::Miter { limit: 5.0 };
        let mitered = winding_stroke(
            &chains,
            0.5,
            BufferCapStyle::Flat,
            miter,
            NonZeroU32::new(8).unwrap(),
        )
        .unwrap();
        assert!((mitered.area() - 4.0).abs() < 1e-12, "{}", mitered.area());
        let bevelled = winding_stroke(
            &chains,
            0.5,
            BufferCapStyle::Flat,
            WalkJoinRule::Bevel,
            NonZeroU32::new(8).unwrap(),
        )
        .unwrap();
        assert!(
            (bevelled.area() - 3.875).abs() < 1e-12,
            "{}",
            bevelled.area()
        );
    }

    /// Puntal and collection routing: a point buffers to the exact
    /// inscribed circle (arc doctrine), a zero-length chain IS that disk
    /// bit-for-bit, negative distances annihilate puntal/lineal input to
    /// the typed empty polygon, and a mixed collection buffers in one
    /// winding pass covering every part.
    #[test]
    fn puntal_and_collection_buffers_route_through_the_engine() {
        let disc = Shape::Point(Point::new(1.0, 1.0).unwrap())
            .buffer(2.0)
            .unwrap();
        // Inscribed 32-gon: area = n/2 * r² * sin(tau/n).
        let expected = 16.0 * 4.0 * (std::f64::consts::TAU / 32.0).sin();
        assert!((disc.area() - expected).abs() < 1e-12, "{}", disc.area());

        let zero_chain = CoordSeq::try_from_points(&[
            Point::new(1.0, 1.0).unwrap(),
            Point::new(1.0, 1.0).unwrap(),
        ])
        .unwrap();
        let stroked = Shape::LineString(LineSeq::try_new(zero_chain).expect("test line is valid"))
            .buffer(2.0)
            .unwrap();
        assert!(stroked.equals_exact(&disc, 0.0, false, false));

        for shape in [
            Shape::Point(Point::new(0.0, 0.0).unwrap()),
            Shape::LineString(
                LineSeq::try_new(
                    CoordSeq::try_from_points(&[
                        Point::new(0.0, 0.0).unwrap(),
                        Point::new(5.0, 0.0).unwrap(),
                    ])
                    .unwrap(),
                )
                .expect("test line is valid"),
            ),
        ] {
            let annihilated = shape.buffer(-1.0).unwrap();
            assert!(annihilated.is_empty(), "{annihilated:?}");
        }

        let collection = Shape::geometry_collection(vec![
            Shape::Point(Point::new(12.0, 3.0).unwrap()),
            Shape::LineString(
                LineSeq::try_new(
                    CoordSeq::try_from_points(&[
                        Point::new(0.0, 0.0).unwrap(),
                        Point::new(4.0, 0.0).unwrap(),
                    ])
                    .unwrap(),
                )
                .expect("test line is valid"),
            ),
            Shape::Polygon(Polygon::new(
                ring(&[(6.0, 0.0), (10.0, 0.0), (10.0, 4.0), (6.0, 4.0), (6.0, 0.0)]),
                Vec::new(),
            )),
        ])
        .expect("test collection is valid");
        let buffered = collection.buffer(0.5).unwrap();
        assert!(
            buffered.validate().unwrap().is_none(),
            "{:?}",
            buffered.validate()
        );
        let Shape::GeometryCollection(parts) = &collection else {
            unreachable!("collection fixture produces a geometry collection");
        };
        for part in parts {
            assert!(buffered.covers(part).unwrap(), "missing {part:?}");
        }
        // Negative distance keeps only the eroded polygon: the exact
        // inner square.
        let eroded = collection.buffer(-0.5).unwrap();
        assert!((eroded.area() - 9.0).abs() < 1e-12, "{}", eroded.area());
    }

    #[test]
    fn touching_multipolygon_erosion_matches_its_union() {
        let rectangle = |min_x: f64, max_x: f64| {
            Polygon::new(
                ring(&[
                    (min_x, 0.0),
                    (max_x, 0.0),
                    (max_x, 4.0),
                    (min_x, 4.0),
                    (min_x, 0.0),
                ]),
                Vec::new(),
            )
        };
        let left = rectangle(0.0, 2.0);
        let right = rectangle(2.0, 4.0);
        let multipart = Shape::MultiPolygon(
            MultiPolygon::from_dense(vec![left.clone(), right.clone()]).unwrap(),
        );
        let union = Shape::Polygon(left)
            .union(&Shape::Polygon(right), crate::geometry::Strictness::Lenient)
            .unwrap();

        let eroded = multipart.buffer(-0.5).unwrap();
        let expected = union.buffer(-0.5).unwrap();

        assert_eq!(expected.area(), 9.0);
        assert_eq!(eroded.area(), expected.area());
        assert!(eroded.equals(&expected).unwrap());
        assert!(multipart.buffer(0.0).unwrap().equals(&union).unwrap());
    }

    #[test]
    fn grouped_zero_support_uses_one_partition_for_nested_signed_sources() {
        let bowtie = Polygon::new(
            ring(&[(0.0, 0.0), (4.0, 4.0), (4.0, 0.0), (0.0, 4.0), (0.0, 0.0)]),
            Vec::new(),
        );
        let collapsed_shell = Polygon::new(
            ring(&[(6.0, 0.0), (8.0, 0.0), (10.0, 0.0), (6.0, 0.0)]),
            Vec::new(),
        );
        let shell = ring(&[
            (10.0, 0.0),
            (14.0, 0.0),
            (14.0, 4.0),
            (10.0, 4.0),
            (10.0, 0.0),
        ]);
        let collapsed_hole = Polygon::new(shell, vec![ring(&[
            (11.0, 2.0),
            (12.0, 2.0),
            (13.0, 2.0),
            (11.0, 2.0),
        ])]);
        let nested = Shape::geometry_collection(vec![
            Shape::Polygon(bowtie),
            Shape::geometry_collection(vec![
                Shape::Polygon(collapsed_shell),
                Shape::Polygon(collapsed_hole),
            ])
            .unwrap(),
        ])
        .unwrap();

        reset_partition_calls();
        let normalized = nested.buffer(0.0).unwrap();

        assert_eq!(partition_calls(), 1);
        assert_eq!(normalized.area(), 20.0);
        assert!(!normalized.is_empty());
    }

    #[test]
    fn grouped_union_fills_another_polygon_hole() {
        let frame = Polygon::new(
            ring(&[
                (0.0, 0.0),
                (10.0, 0.0),
                (10.0, 10.0),
                (0.0, 10.0),
                (0.0, 0.0),
            ]),
            vec![ring(&[
                (3.0, 3.0),
                (7.0, 3.0),
                (7.0, 7.0),
                (3.0, 7.0),
                (3.0, 3.0),
            ])],
        );
        let filler = Polygon::new(
            ring(&[(3.0, 3.0), (7.0, 3.0), (7.0, 7.0), (3.0, 7.0), (3.0, 3.0)]),
            Vec::new(),
        );
        let source = Shape::MultiPolygon(MultiPolygon::from_dense(vec![frame, filler]).unwrap());

        let normalized = source.buffer(0.0).unwrap();

        assert_eq!(normalized.area(), 100.0);
        assert!(
            normalized
                .covers(&Shape::Point(Point::new(5.0, 5.0).unwrap()))
                .unwrap()
        );
    }

    #[test]
    fn overlapping_separated_and_nested_areal_parts_erode_as_one_support() {
        let rectangle = |min_x: f64, max_x: f64| {
            Polygon::new(
                ring(&[
                    (min_x, 0.0),
                    (max_x, 0.0),
                    (max_x, 4.0),
                    (min_x, 4.0),
                    (min_x, 0.0),
                ]),
                Vec::new(),
            )
        };
        let matches_union = |left: Polygon, right: Polygon| {
            let multipart = Shape::MultiPolygon(
                MultiPolygon::from_dense(vec![left.clone(), right.clone()]).unwrap(),
            );
            let union = Shape::Polygon(left)
                .union(&Shape::Polygon(right), crate::geometry::Strictness::Lenient)
                .unwrap();
            let eroded = multipart.buffer(-0.5).unwrap();
            let expected = union.buffer(-0.5).unwrap();
            assert!(eroded.equals(&expected).unwrap());
            eroded
        };

        assert_eq!(
            matches_union(rectangle(0.0, 3.0), rectangle(2.0, 5.0)).area(),
            12.0
        );
        let separated = matches_union(rectangle(0.0, 2.0), rectangle(5.0, 7.0));
        assert_eq!(separated.area(), 6.0);
        assert!(matches!(separated, Shape::MultiPolygon(_)));

        let nested = Shape::geometry_collection(vec![
            Shape::Polygon(rectangle(0.0, 2.0)),
            Shape::geometry_collection(vec![Shape::Polygon(rectangle(2.0, 4.0))]).unwrap(),
        ])
        .unwrap();
        let nested_eroded = nested.buffer(-0.5).unwrap();
        let nested_union = Shape::Polygon(rectangle(0.0, 2.0))
            .union(
                &Shape::Polygon(rectangle(2.0, 4.0)),
                crate::geometry::Strictness::Lenient,
            )
            .unwrap();
        assert!(
            nested_eroded
                .equals(&nested_union.buffer(-0.5).unwrap())
                .unwrap()
        );
    }

    #[test]
    fn erosion_opens_and_closes_polygon_holes_at_the_union_boundary() {
        let holed = Shape::Polygon(Polygon::new(
            ring(&[
                (0.0, 0.0),
                (10.0, 0.0),
                (10.0, 10.0),
                (0.0, 10.0),
                (0.0, 0.0),
            ]),
            vec![ring(&[
                (4.0, 4.0),
                (6.0, 4.0),
                (6.0, 6.0),
                (4.0, 6.0),
                (4.0, 4.0),
            ])],
        ));

        let open = holed.buffer(-1.0).unwrap();
        assert!(
            !open
                .covers(&Shape::Point(Point::new(5.0, 5.0).unwrap()))
                .unwrap()
        );
        assert!(open.area() > 48.0);
        let closed = holed.buffer(-2.5).unwrap();
        assert!(closed.is_empty());
    }

    #[test]
    fn collapsed_areal_support_is_empty_and_collapsed_holes_are_ignored() {
        let collapsed = Shape::Polygon(Polygon::new(
            ring(&[(0.0, 0.0), (2.0, 0.0), (4.0, 0.0), (0.0, 0.0)]),
            Vec::new(),
        ));
        assert!(collapsed.buffer(0.0).unwrap().is_empty());
        assert!(collapsed.buffer(-1.0).unwrap().is_empty());

        let shell = ring(&[
            (0.0, 0.0),
            (10.0, 0.0),
            (10.0, 10.0),
            (0.0, 10.0),
            (0.0, 0.0),
        ]);
        let collapsed_hole = Shape::Polygon(Polygon::new(shell.clone(), vec![ring(&[
            (2.0, 5.0),
            (5.0, 5.0),
            (8.0, 5.0),
            (2.0, 5.0),
        ])]));
        let solid = Shape::Polygon(Polygon::new(shell, Vec::new()));
        assert!(
            collapsed_hole
                .buffer(-1.0)
                .unwrap()
                .equals(&solid.buffer(-1.0).unwrap())
                .unwrap()
        );
    }

    /// `buffer(0)` is the d -> 0 limit through the engine: a valid
    /// polygon is returned as-is (holes included), a self-crossing
    /// bowtie keeps its positively-wound lobe (NOT empty, the old geo
    /// answer), and puntal/lineal input annihilates exactly.
    #[test]
    fn zero_distance_buffer_is_the_winding_limit() {
        let holed = Shape::Polygon(Polygon::new(
            ring(&[
                (0.0, 0.0),
                (10.0, 0.0),
                (10.0, 10.0),
                (0.0, 10.0),
                (0.0, 0.0),
            ]),
            vec![ring(&[
                (4.0, 4.0),
                (6.0, 4.0),
                (6.0, 6.0),
                (4.0, 6.0),
                (4.0, 4.0),
            ])],
        ));
        let same = holed.buffer(0.0).unwrap();
        assert!((same.area() - 96.0).abs() < 1e-12, "{}", same.area());

        let bowtie = Shape::Polygon(Polygon::new(
            ring(&[(0.0, 0.0), (4.0, 4.0), (4.0, 0.0), (0.0, 4.0), (0.0, 0.0)]),
            Vec::new(),
        ));
        let lobe = bowtie.buffer(0.0).unwrap();
        assert!((lobe.area() - 4.0).abs() < 1e-12, "{}", lobe.area());

        let line = Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    Point::new(0.0, 0.0).unwrap(),
                    Point::new(1.0, 1.0).unwrap(),
                ])
                .unwrap(),
            )
            .expect("test line is valid"),
        );
        assert!(line.buffer(0.0).unwrap().is_empty());
    }

    #[test]
    fn collinear_linestring_buffers_to_its_extent_stadium() {
        // Every collinear polyline over the same span — open, doubled back,
        // or closed on itself (`A..A`) — covers exactly that span, so its
        // buffer is the segment stadium, NOT a thin self-retracing region.
        let line = |points: &[(f64, f64)]| {
            Shape::LineString(
                LineSeq::try_new(
                    CoordSeq::try_from_points(
                        &points
                            .iter()
                            .map(|&(x, y)| Point::new(x, y).unwrap())
                            .collect::<Vec<_>>(),
                    )
                    .unwrap(),
                )
                .expect("test line is valid"),
            )
        };
        let open = line(&[(0.0, 0.0), (1.0, 0.0), (2.0, 0.0)])
            .buffer(2.0)
            .unwrap();
        let doubled = line(&[(0.0, 0.0), (2.0, 0.0), (1.0, 0.0)])
            .buffer(2.0)
            .unwrap();
        let closed = line(&[(0.0, 0.0), (1.0, 0.0), (2.0, 0.0), (0.0, 0.0)])
            .buffer(2.0)
            .unwrap();
        assert!(
            (doubled.area() - open.area()).abs() < 1e-9,
            "doubled {} vs open {}",
            doubled.area(),
            open.area()
        );
        assert!(
            (closed.area() - open.area()).abs() < 1e-9,
            "closed {} vs open {}",
            closed.area(),
            open.area()
        );
        // The true stadium of a length-2 segment at radius 2 is `8 + 4*pi`;
        // the inscribed-arc caps undershoot it slightly (never the ~14 the
        // folded-loop bug produced).
        let stadium = 8.0 + 4.0 * std::f64::consts::PI;
        assert!(
            open.area() > stadium - 0.1 && open.area() <= stadium,
            "{}",
            open.area()
        );
    }
}

#[cfg(test)]
mod consecutive_dedup_tests {
    use super::*;
    use crate::geometry::constructive::offset::offset_source;
    use crate::geometry::constructive::strict_cycle;
    use crate::geometry::{CoordinateAxes, GeometryKind};

    fn point(x: f64, y: f64) -> Point {
        Point::new(x, y).unwrap()
    }

    fn closed_ring(points: &[(f64, f64)]) -> CoordSeq {
        let mut coords: Vec<Point> = points.iter().map(|&(x, y)| point(x, y)).collect();
        coords.push(coords[0]);
        CoordSeq::try_from_points(&coords).unwrap()
    }

    #[test]
    fn strict_cycle_collapses_consecutive_duplicates() {
        let ring = closed_ring(&[(0.0, 0.0), (0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0)]);
        let strict = strict_cycle(&ring, false).expect("valid ring");
        assert_eq!(strict.len(), 4);
    }

    #[test]
    fn strict_cycle_rejects_degenerate_after_dedup() {
        let ring = closed_ring(&[(0.0, 0.0), (0.0, 0.0), (1.0, 0.0), (1.0, 0.0), (0.0, 0.0)]);
        assert!(strict_cycle(&ring, false).is_none());
    }

    #[test]
    fn offset_source_dedups_before_close_handling() {
        let line = CoordSeq::try_from_points(&[
            point(0.0, 0.0),
            point(0.0, 0.0),
            point(1.0, 0.0),
            point(1.0, 0.0),
        ])
        .unwrap();
        assert!(offset_source(&line).is_some());
        let mut budget = ExpansionBudget::new("offset_curve", "quadrant_segments");
        assert!(
            offset_curve_points(
                &line,
                1.0,
                WalkJoinRule::Arc,
                NonZeroU32::new(8).unwrap(),
                &mut budget,
            )
            .unwrap()
            .is_some()
        );
        let collapsed = CoordSeq::try_from_points(&[point(0.0, 0.0), point(0.0, 0.0)]).unwrap();
        assert!(offset_source(&collapsed).is_none());
    }

    #[test]
    fn snap_to_grid_recloses_ring_after_dedup() {
        let ring = Ring::from_closed(closed_ring(&[
            (0.0, 0.0),
            (0.0, 0.0),
            (2.0, 0.0),
            (2.0, 2.0),
            (0.0, 2.0),
            (0.0, 0.0),
        ]))
        .unwrap();
        let polygon = Shape::Polygon(Polygon::new(ring, Vec::new()));
        let cleaned = polygon.snap_to_grid((1.0, 1.0), (0.0, 0.0)).unwrap();
        let Shape::Polygon(polygon) = cleaned else {
            panic!("expected polygon");
        };
        assert_eq!(polygon.shell.coords().len(), 5);
        let shell = polygon.shell.coords();
        let first = shell.first().expect("closure");
        let last = shell.last().expect("closure");
        assert_eq!(first.x, last.x);
        assert_eq!(first.y, last.y);
    }

    #[test]
    fn empty_results_keep_source_axes() {
        let collapsed_ring = Ring::from_closed(
            CoordSeq::try_from_vecs(
                vec![0.0, 0.0, 0.0, 0.0],
                vec![0.0, 0.0, 0.0, 0.0],
                Some(vec![1.0, 1.0, 1.0, 1.0]),
                None,
            )
            .unwrap(),
        )
        .unwrap();
        let polygon = Shape::Polygon(Polygon::new(collapsed_ring, Vec::new()));

        assert_eq!(polygon.unique_points().unwrap().axes(), CoordinateAxes::XYZ);
        assert_eq!(
            polygon.snap_to_grid((1.0, 1.0), (0.0, 0.0)).unwrap(),
            Shape::typed_empty(GeometryKind::Polygon, CoordinateAxes::XYZ)
        );
        assert_eq!(
            polygon.simplify_vw(1.0, false).unwrap(),
            Shape::typed_empty(GeometryKind::Polygon, CoordinateAxes::XYZ)
        );
        assert_eq!(
            polygon.simplify_dp_raw(1.0).unwrap(),
            Shape::typed_empty(GeometryKind::Polygon, CoordinateAxes::XYZ)
        );
        assert_eq!(
            polygon.remove_repeated_points(1.0).unwrap(),
            Shape::typed_empty(GeometryKind::Polygon, CoordinateAxes::XYZ)
        );
    }

    #[test]
    fn simplify_vw_raw_keeps_repeated_xy_endpoint_ordinates() {
        let line = Shape::LineString(LineSeq::from_proven(
            CoordSeq::try_from_vecs(
                [0.0, 1.0, 0.0].into(),
                [0.0, 0.0, 0.0].into(),
                Some([1.0, 2.0, 9.0].into()),
                Some([10.0, 20.0, 90.0].into()),
            )
            .unwrap(),
        ));
        let Shape::LineString(out) = line.simplify_vw_raw(1.0).unwrap() else {
            panic!("A-B-A VW keep-mask must remain a line");
        };
        assert_eq!(out.axes(), CoordinateAxes::XYZM);
        assert_eq!(out.xs(), &[0.0, 0.0]);
        assert_eq!(out.ys(), &[0.0, 0.0]);
        assert_eq!(out.zs(), Some(&[1.0, 9.0][..]));
        assert_eq!(out.ms(), Some(&[10.0, 90.0][..]));
    }

    #[test]
    fn dropping_collapsed_shell_keeps_xyzm_empty() {
        let collapsed = Ring::from_closed(
            CoordSeq::try_from_vecs(
                [0.0, 0.0, 0.0, 0.0].into(),
                [0.0, 0.0, 0.0, 0.0].into(),
                Some([1.0, 2.0, 3.0, 1.0].into()),
                Some([10.0, 20.0, 30.0, 10.0].into()),
            )
            .unwrap(),
        )
        .unwrap();
        let polygon = Shape::Polygon(Polygon::new(collapsed, Vec::new()));
        assert_eq!(
            polygon.simplify_vw(0.0, false).unwrap(),
            Shape::typed_empty(GeometryKind::Polygon, CoordinateAxes::XYZM)
        );
    }

    #[test]
    fn dropping_collapsed_hole_keeps_the_shell() {
        let shell = Ring::from_closed(
            CoordSeq::try_from_vecs(
                [0.0, 4.0, 0.0, 0.0].into(),
                [0.0, 0.0, 4.0, 0.0].into(),
                None,
                None,
            )
            .unwrap(),
        )
        .unwrap();
        let hole = Ring::from_closed(
            CoordSeq::try_from_vecs(
                [1.0, 1.0, 1.0, 1.0].into(),
                [1.0, 1.0, 1.0, 1.0].into(),
                None,
                None,
            )
            .unwrap(),
        )
        .unwrap();
        let polygon = Shape::Polygon(Polygon::new(shell, vec![hole]));
        let Shape::Polygon(out) = polygon.simplify_vw(0.0, false).unwrap() else {
            panic!("three-corner shell must remain a polygon");
        };
        assert_eq!(out.holes.len(), 0);
        assert_eq!(out.shell.xs(), &[0.0, 4.0, 0.0, 0.0]);
        assert_eq!(out.shell.ys(), &[0.0, 0.0, 4.0, 0.0]);
    }
}

#[cfg(test)]
mod subdivide_columns_tests {

    use geographiclib_rs::Geodesic;

    use super::*;
    use crate::geographic::GeographicFrame;
    use crate::geometry::constructive::{densify_points_budgeted, segmentize_points_budgeted};

    #[test]
    fn densify_xyzm_interpolates_every_ordinate_lane() {
        let line = CoordSeq::try_from_vecs(
            vec![0.0, 4.0],
            vec![0.0, 0.0],
            Some(vec![10.0, 30.0]),
            Some(vec![100.0, 300.0]),
        )
        .unwrap();
        let mut budget = ExpansionBudget::new("segmentize", "fraction");
        let densified = densify_points_budgeted(&line, 0.25, &mut budget).unwrap();
        assert_eq!(densified.len(), 5);
        assert_eq!(densified.xs(), &[0.0, 1.0, 2.0, 3.0, 4.0]);
        assert_eq!(densified.ys(), &[0.0, 0.0, 0.0, 0.0, 0.0]);
        assert_eq!(densified.zs().unwrap(), &[10.0, 15.0, 20.0, 25.0, 30.0]);
        assert_eq!(densified.ms().unwrap(), &[
            100.0, 150.0, 200.0, 250.0, 300.0
        ]);
    }

    #[test]
    fn segmentize_matches_fraction_dedup_reference() {
        let line = CoordSeq::try_from_points(&[
            Point::new(0.0, 0.0).unwrap(),
            Point::new(3.0, 0.0).unwrap(),
        ])
        .unwrap();
        let mut budget = ExpansionBudget::new("segmentize", "max_length");
        let segmented = segmentize_points_budgeted(
            &line,
            1.0,
            &crate::geometry::SegmentPlacement::Planar,
            &mut budget,
        )
        .unwrap();
        assert_eq!(segmented.len(), 4);
        assert_eq!(segmented.xs(), &[0.0, 1.0, 2.0, 3.0]);
    }

    #[test]
    fn planar_segmentize_scales_opposing_endpoint_distance_before_planning() {
        let line = CoordSeq::try_from_points(&[
            Point::new(-1e308, 0.0).unwrap(),
            Point::new(1e308, 0.0).unwrap(),
        ])
        .unwrap();
        let mut budget = ExpansionBudget::new("segmentize", "max_length");
        let segmented = segmentize_points_budgeted(
            &line,
            1e308,
            &crate::geometry::SegmentPlacement::Planar,
            &mut budget,
        )
        .unwrap();

        assert_eq!(segmented.xs(), &[-1e308, 0.0, 1e308]);
        assert_eq!(segmented.ys(), &[0.0, 0.0, 0.0]);
        assert_eq!(budget.used(), 1);
    }

    #[test]
    fn planar_segmentize_accepts_exact_representable_length_boundaries() {
        let line = CoordSeq::try_from_points(&[
            Point::new(0.0, 0.0).unwrap(),
            Point::new(4.0, 0.0).unwrap(),
        ])
        .unwrap();

        let mut unchanged_budget = ExpansionBudget::new("segmentize", "max_length");
        let unchanged = segmentize_points_budgeted(
            &line,
            4.0,
            &crate::geometry::SegmentPlacement::Planar,
            &mut unchanged_budget,
        )
        .unwrap();
        assert_eq!(unchanged.xs(), &[0.0, 4.0]);
        assert_eq!(unchanged_budget.used(), 0);

        let mut split_budget = ExpansionBudget::new("segmentize", "max_length");
        let split = segmentize_points_budgeted(
            &line,
            2.0,
            &crate::geometry::SegmentPlacement::Planar,
            &mut split_budget,
        )
        .unwrap();
        assert_eq!(split.xs(), &[0.0, 2.0, 4.0]);
        assert_eq!(split_budget.used(), 1);

        let adjacent = f64::from_bits(1.0_f64.to_bits() + 1);
        let adjacent_line = CoordSeq::try_from_points(&[
            Point::new(1.0, 0.0).unwrap(),
            Point::new(adjacent, 0.0).unwrap(),
        ])
        .unwrap();
        let mut adjacent_budget = ExpansionBudget::new("segmentize", "max_length");
        let exact = segmentize_points_budgeted(
            &adjacent_line,
            2.0_f64.powi(-52),
            &crate::geometry::SegmentPlacement::Planar,
            &mut adjacent_budget,
        )
        .unwrap();
        assert_eq!(exact.xs(), &[1.0, adjacent]);
        assert_eq!(adjacent_budget.used(), 0);
    }

    #[test]
    fn planar_segmentize_rejects_an_unrepresentable_binary64_chord() {
        let adjacent = f64::from_bits(1.0_f64.to_bits() + 1);
        let line = CoordSeq::try_from_points(&[
            Point::new(1.0, 0.0).unwrap(),
            Point::new(adjacent, 0.0).unwrap(),
        ])
        .unwrap();
        let error = Shape::LineString(LineSeq::from_proven(line))
            .segmentize(2.0_f64.powi(-54))
            .unwrap_err();

        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(GeometryErrorKind::UnrepresentableOutput {
                operation: "segmentize"
            })
        ));
    }

    #[test]
    fn geographic_segmentize_preserves_curved_xy_and_interpolated_zm() {
        let line = CoordSeq::try_from_vecs(
            [0.0, 10.0].into(),
            [0.0, 10.0].into(),
            Some([10.0, 30.0].into()),
            Some([100.0, 300.0].into()),
        )
        .unwrap();
        let geodesic = Geodesic::wgs84();
        let mut budget = ExpansionBudget::new("segmentize", "max_length");
        let segmented = segmentize_points_budgeted(
            &line,
            1_000_000.0,
            &crate::geometry::SegmentPlacement::Geodesic(&geodesic, GeographicFrame::crs84()),
            &mut budget,
        )
        .unwrap();

        assert_eq!(segmented.len(), 3);
        assert_eq!(segmented.xs()[0].to_bits(), 0.0_f64.to_bits());
        assert_eq!(segmented.ys()[0].to_bits(), 0.0_f64.to_bits());
        assert_eq!(segmented.xs()[2].to_bits(), 10.0_f64.to_bits());
        assert_eq!(segmented.ys()[2].to_bits(), 10.0_f64.to_bits());
        assert!((segmented.xs()[1] - 4.961_882_922_527_124).abs() <= 1e-12);
        assert!((segmented.ys()[1] - 5.019_508_800_894_45).abs() <= 1e-12);
        assert_eq!(segmented.zs().unwrap(), &[10.0, 20.0, 30.0]);
        assert_eq!(segmented.ms().unwrap(), &[100.0, 200.0, 300.0]);
    }

    #[test]
    fn geographic_segmentize_selects_equatorial_interpolation_per_segment() {
        let line = CoordSeq::try_from_points(&[
            Point::new(-10.0, 0.0).unwrap(),
            Point::new(0.0, 0.0).unwrap(),
            Point::new(10.0, 10.0).unwrap(),
        ])
        .unwrap();
        let geodesic = Geodesic::wgs84();
        let mut budget = ExpansionBudget::new("segmentize", "max_length");
        let segmented = segmentize_points_budgeted(
            &line,
            1_000_000.0,
            &crate::geometry::SegmentPlacement::Geodesic(&geodesic, GeographicFrame::crs84()),
            &mut budget,
        )
        .unwrap();

        assert_eq!(segmented.len(), 5);
        assert_eq!(segmented.xs()[1], -5.0);
        assert_eq!(segmented.ys()[1].to_bits(), 0.0_f64.to_bits());
        assert!((segmented.xs()[3] - 4.961_882_922_527_124).abs() <= 1e-12);
        assert!((segmented.ys()[3] - 5.019_508_800_894_45).abs() <= 1e-12);
    }

    #[test]
    fn geographic_segmentize_consumes_sparse_plans_in_source_order() {
        let line = CoordSeq::try_from_vecs(
            [-0.0001, 0.0, 10.0, 10.0001, 20.0001, 20.0002].into(),
            [-0.0001, 0.0, 10.0, 10.0001, 20.0001, 20.0002].into(),
            Some([10.0, 20.0, 30.0, 40.0, 50.0, 60.0].into()),
            Some([100.0, 200.0, 300.0, 400.0, 500.0, 600.0].into()),
        )
        .unwrap();
        let geodesic = Geodesic::wgs84();
        let mut budget = ExpansionBudget::new("segmentize", "max_length");
        let segmented = segmentize_points_budgeted(
            &line,
            1_000_000.0,
            &crate::geometry::SegmentPlacement::Geodesic(&geodesic, GeographicFrame::crs84()),
            &mut budget,
        )
        .unwrap();

        // The inserted coordinates are the stored binary64 midpoints on the
        // WGS84 geodesics; source vertices remain in their original order.
        assert_eq!(segmented.len(), 8);
        assert_eq!(segmented.xs()[0].to_bits(), (-0.0001_f64).to_bits());
        assert_eq!(segmented.ys()[0].to_bits(), (-0.0001_f64).to_bits());
        assert_eq!(segmented.xs()[1].to_bits(), 0.0_f64.to_bits());
        assert_eq!(segmented.ys()[1].to_bits(), 0.0_f64.to_bits());
        assert!((segmented.xs()[2] - 4.961_882_922_527_124).abs() <= 1e-12);
        assert!((segmented.ys()[2] - 5.019_508_800_894_45).abs() <= 1e-12);
        assert_eq!(segmented.xs()[3].to_bits(), 10.0_f64.to_bits());
        assert_eq!(segmented.ys()[3].to_bits(), 10.0_f64.to_bits());
        assert_eq!(segmented.xs()[4].to_bits(), 10.0001_f64.to_bits());
        assert_eq!(segmented.ys()[4].to_bits(), 10.0001_f64.to_bits());
        assert!((segmented.xs()[5] - 14.883_313_082_425_628).abs() <= 1e-12);
        assert!((segmented.ys()[5] - 15.056_212_173_359_553).abs() <= 1e-12);
        assert_eq!(segmented.xs()[6].to_bits(), 20.0001_f64.to_bits());
        assert_eq!(segmented.ys()[6].to_bits(), 20.0001_f64.to_bits());
        assert_eq!(segmented.xs()[7].to_bits(), 20.0002_f64.to_bits());
        assert_eq!(segmented.ys()[7].to_bits(), 20.0002_f64.to_bits());
        assert_eq!(segmented.zs().unwrap(), &[
            10.0, 20.0, 25.0, 30.0, 40.0, 45.0, 50.0, 60.0
        ]);
        assert_eq!(segmented.ms().unwrap(), &[
            100.0, 200.0, 250.0, 300.0, 400.0, 450.0, 500.0, 600.0
        ]);
    }

    #[test]
    fn geodesic_segmentize_rejects_an_ambiguous_antipodal_route() {
        let line = CoordSeq::try_from_points(&[
            Point::new(0.0, 0.0).unwrap(),
            Point::new(180.0, 0.0).unwrap(),
        ])
        .unwrap();
        let geodesic = Geodesic::wgs84();
        let mut budget = ExpansionBudget::new("segmentize", "max_length");
        let error = segmentize_points_budgeted(
            &line,
            11_000_000.0,
            &crate::geometry::SegmentPlacement::Geodesic(&geodesic, GeographicFrame::crs84()),
            &mut budget,
        )
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("geodesic segment endpoints have multiple shortest geodesics")
        );
    }

    #[test]
    fn geographic_segmentize_preserves_coordinates_without_charging() {
        let line = CoordSeq::try_from_points(&[
            Point::new(0.0, 0.0).unwrap(),
            Point::new(1.0, 0.0).unwrap(),
        ])
        .unwrap();
        let geodesic = Geodesic::wgs84();
        let mut budget = ExpansionBudget::new("segmentize", "max_length");
        let segmented = segmentize_points_budgeted(
            &line,
            2_000_000.0,
            &crate::geometry::SegmentPlacement::Geodesic(&geodesic, GeographicFrame::crs84()),
            &mut budget,
        )
        .unwrap();

        assert_eq!(budget.used(), 0);
        assert_eq!(segmented.xs(), line.xs());
        assert_eq!(segmented.ys(), line.ys());
    }

    #[test]
    fn pathological_subdivision_counts_fail_before_allocation() {
        let line = CoordSeq::try_from_points(&[
            Point::new(0.0, 0.0).unwrap(),
            Point::new(1.0, 0.0).unwrap(),
        ])
        .unwrap();
        let mut segmentize_budget = ExpansionBudget::new("segmentize", "max_length");
        let segmentize = segmentize_points_budgeted(
            &line,
            f64::MIN_POSITIVE,
            &crate::geometry::SegmentPlacement::Planar,
            &mut segmentize_budget,
        )
        .unwrap_err();
        assert!(matches!(
            segmentize.kind(),
            crate::error::ErrorKind::Geometry(GeometryErrorKind::GeneratedOutputTooLarge {
                operation: "segmentize",
                parameter: "max_length",
                ..
            })
        ));
        let mut densify_budget = ExpansionBudget::new("segmentize", "fraction");
        let densify =
            densify_points_budgeted(&line, f64::MIN_POSITIVE, &mut densify_budget).unwrap_err();
        assert!(matches!(
            densify.kind(),
            crate::error::ErrorKind::Geometry(GeometryErrorKind::GeneratedOutputTooLarge {
                operation: "segmentize",
                parameter: "fraction",
                ..
            })
        ));
    }
}

#[cfg(test)]
mod expansion_budget_threading_tests {
    use super::*;
    use crate::geometry::{GENERATED_ITEM_LIMIT, SmoothMethod};

    fn exhausted(operation: &'static str, parameter: &'static str) -> ExpansionBudget {
        let mut budget = ExpansionBudget::new(operation, parameter);
        budget.add(GENERATED_ITEM_LIMIT).unwrap();
        budget
    }

    fn bent_line() -> Shape {
        Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    Point::new(0.0, 0.0).unwrap(),
                    Point::new(1.0, 0.0).unwrap(),
                    Point::new(1.0, 1.0).unwrap(),
                ])
                .unwrap(),
            )
            .expect("line coordinate sequence must contain at least two vertices"),
        )
    }

    fn straight_line() -> LineSeq {
        LineSeq::try_new(
            CoordSeq::try_from_points(&[
                Point::new(0.0, 0.0).unwrap(),
                Point::new(1.0, 0.0).unwrap(),
            ])
            .unwrap(),
        )
        .expect("line coordinate sequence must contain at least two vertices")
    }

    fn square_polygon(min_x: f64) -> Shape {
        Shape::Polygon(Polygon::new(
            Ring::from_closed(
                CoordSeq::try_from_points(&[
                    Point::new(min_x, 0.0).unwrap(),
                    Point::new(min_x + 2.0, 0.0).unwrap(),
                    Point::new(min_x + 2.0, 2.0).unwrap(),
                    Point::new(min_x, 2.0).unwrap(),
                    Point::new(min_x, 0.0).unwrap(),
                ])
                .unwrap(),
            )
            .unwrap(),
            Vec::new(),
        ))
    }

    #[test]
    fn buffer_and_offset_collection_children_use_the_callers_budget() {
        let style = Positive::try_new("miter_limit", 5.0).unwrap();
        let mut buffer_budget = exhausted("buffer", "quadrant_segments");
        let buffer = Shape::geometry_collection(vec![Shape::Point(Point::new(0.0, 0.0).unwrap())])
            .expect("test collection is valid");
        buffer
            .buffer_with_style_budgeted(
                1.0,
                BufferCapStyle::Round,
                BufferJoinStyle::Round,
                NonZeroU32::new(1).unwrap(),
                style,
                &mut buffer_budget,
            )
            .unwrap_err();

        let mut offset_budget = exhausted("offset_curve", "quadrant_segments");
        Shape::geometry_collection(vec![bent_line()])
            .expect("test collection is valid")
            .offset_curve_budgeted(
                1.0,
                BufferJoinStyle::Round,
                NonZeroU32::new(1).unwrap(),
                style,
                &mut offset_budget,
            )
            .unwrap_err();
    }

    #[test]
    fn smooth_children_and_triangle_emission_use_the_callers_budget() {
        let mut smooth_budget = exhausted("smooth", "iterations");
        Shape::geometry_collection(vec![bent_line()])
            .expect("test collection is valid")
            .smooth_budgeted(1, SmoothMethod::CatmullRom, true, &mut smooth_budget)
            .unwrap_err();

        let mut triangle_budget = exhausted("triangulate", "method");
        Shape::multi_point(
            crate::geometry::CoordinateAxes::XY,
            CoordSeq::try_from_points(&[
                Point::new(0.0, 0.0).unwrap(),
                Point::new(1.0, 0.0).unwrap(),
                Point::new(0.0, 1.0).unwrap(),
            ])
            .unwrap(),
        )
        .delaunay_triangles_budgeted(&mut triangle_budget)
        .unwrap_err();
    }

    #[test]
    fn segmentize_shares_the_callers_budget_across_parts_and_rings() {
        // One generated point per straight part: the first part lands exactly
        // on the cap and the second must observe that same exhausted owner.
        let mut parts_budget = ExpansionBudget::new("segmentize", "max_length");
        parts_budget.add(GENERATED_ITEM_LIMIT - 1).unwrap();
        Shape::multi_line_string(crate::geometry::CoordinateAxes::XY, vec![
            straight_line(),
            straight_line(),
        ])
        .segmentize_budgeted(
            0.5,
            &crate::geometry::SegmentPlacement::Planar,
            &mut parts_budget,
        )
        .unwrap_err();
        assert_eq!(parts_budget.used(), GENERATED_ITEM_LIMIT);

        // A unit square contributes twelve inserted vertices at this step. The
        // hole must reuse the shell's owner rather than start another budget.
        let square = |min: f64, max: f64| {
            Ring::closed(vec![
                Point::new(min, min).unwrap(),
                Point::new(max, min).unwrap(),
                Point::new(max, max).unwrap(),
                Point::new(min, max).unwrap(),
                Point::new(min, min).unwrap(),
            ])
            .unwrap()
        };
        let mut rings_budget = ExpansionBudget::new("segmentize", "max_length");
        rings_budget.add(GENERATED_ITEM_LIMIT - 12).unwrap();
        Shape::Polygon(Polygon::new(square(0.0, 1.0), vec![square(0.25, 0.75)]))
            .segmentize_budgeted(
                0.25,
                &crate::geometry::SegmentPlacement::Planar,
                &mut rings_budget,
            )
            .unwrap_err();
        assert_eq!(rings_budget.used(), GENERATED_ITEM_LIMIT);
    }

    #[test]
    fn collection_erosion_consumes_one_caller_budget_across_all_parts() {
        let collection = Shape::geometry_collection(vec![square_polygon(0.0), square_polygon(5.0)])
            .expect("test collection is valid");
        let style = Positive::try_new("miter_limit", 5.0).unwrap();
        let mut budget = ExpansionBudget::new("buffer", "quadrant_segments");
        budget.add(GENERATED_ITEM_LIMIT - 5).unwrap();
        let error = collection
            .buffer_with_style_budgeted(
                -0.5,
                BufferCapStyle::Round,
                BufferJoinStyle::Round,
                NonZeroU32::new(1).unwrap(),
                style,
                &mut budget,
            )
            .unwrap_err();
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(GeometryErrorKind::GeneratedOutputTooLarge {
                operation: "buffer",
                parameter: "quadrant_segments",
                ..
            })
        ));
        assert_eq!(budget.used(), GENERATED_ITEM_LIMIT - 1);
    }
}

#[cfg(test)]
mod winding_outcome_tests {
    use super::*;
    use crate::geometry::constructive::convex_buffer_budgeted;

    fn square() -> Polygon {
        Polygon::new(
            Ring::from_closed(
                CoordSeq::try_from_points(&[
                    Point::new_raw_xy(0.0, 0.0),
                    Point::new_raw_xy(4.0, 0.0),
                    Point::new_raw_xy(4.0, 4.0),
                    Point::new_raw_xy(0.0, 4.0),
                    Point::new_raw_xy(0.0, 0.0),
                ])
                .unwrap(),
            )
            .unwrap(),
            Vec::new(),
        )
    }

    #[test]
    fn geometric_declines_and_annihilation_remain_non_errors() {
        let polygon = square();
        let mut budget = ExpansionBudget::new("buffer", "quadrant_segments");
        assert!(
            convex_buffer_budgeted(
                &polygon,
                -3.0,
                WalkJoinRule::Arc,
                NonZeroU32::new(4).unwrap(),
                &mut budget,
            )
            .unwrap()
            .is_none()
        );

        let empty = Shape::Point(Point::new_raw_xy(0.0, 0.0))
            .buffer(-1.0)
            .unwrap();
        assert!(empty.is_empty());
    }

    #[test]
    fn preconstruction_convex_shortcut_does_not_charge_generated_output() {
        let polygon = square();
        let mut budget = ExpansionBudget::new("buffer", "quadrant_segments");
        reset_walk_plan_construction_count();
        assert!(
            convex_buffer_budgeted(
                &polygon,
                0.0,
                WalkJoinRule::Arc,
                NonZeroU32::new(8).unwrap(),
                &mut budget,
            )
            .unwrap()
            .is_none()
        );
        assert_eq!(budget.used(), 0);
        assert_eq!(walk_plan_construction_count(), 0);
    }
}
