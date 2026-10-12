use super::super::test_support::reverse_segment;
use super::*;
use crate::geometry::*;

fn exact_clearance_strip_oracle(
    point: XY,
    normal: [f64; 2],
    low: XY,
    high: XY,
    incumbent: &SquaredDistanceKey,
) -> bool {
    let low_gap = source_dot_difference_exact(normal, low, point).unwrap();
    let high_gap = source_dot_difference_exact(normal, point, high).unwrap();
    let gap = if low_gap.cmp(&high_gap).is_gt() {
        low_gap
    } else {
        high_gap
    };
    let zero = ExactDyadic::zero();
    if !gap.cmp(&zero).is_gt() {
        return false;
    }
    let [nx, ny] = normal;
    let nx = ExactDyadic::from_f64(nx);
    let ny = ExactDyadic::from_f64(ny);
    let norm_squared = nx
        .try_product(&nx)
        .unwrap()
        .try_add(ny.try_product(&ny).unwrap())
        .unwrap();
    if norm_squared.cmp(&zero).is_eq() {
        return false;
    }
    let exact_incumbent = incumbent.witness.squared().unwrap();
    compare_ratios(
        &gap.try_product(&gap).unwrap(),
        &norm_squared,
        &exact_incumbent.numerator,
        &exact_incumbent.denominator,
    )
    .unwrap()
    .is_gt()
}

#[test]
fn segment_nearest_points_returns_edge_interior_foot() {
    let horizontal = Segment {
        start: XY::new(0.0, 0.0),
        end: XY::new(10.0, 0.0),
    };
    let stub = Segment {
        start: XY::new(5.0, 3.0),
        end: XY::new(5.0, 5.0),
    };
    let (on_horizontal, on_stub) = segment_nearest_points(horizontal, stub).unwrap();
    assert_eq!(on_horizontal, XY::new(5.0, 0.0));
    assert_eq!(on_stub, XY::new(5.0, 3.0));
}

/// Crossing segments witness at the exact intersection (a shared point).
#[test]
fn segment_nearest_points_uses_intersection_for_crossing() {
    let a = Segment {
        start: XY::new(0.0, 0.0),
        end: XY::new(2.0, 2.0),
    };
    let b = Segment {
        start: XY::new(0.0, 2.0),
        end: XY::new(2.0, 0.0),
    };
    let (left, right) = segment_nearest_points(a, b).unwrap();
    assert_eq!(left, XY::new(1.0, 1.0));
    assert_eq!(left, right);
}

/// The crossing is the nearest-even rounding of the exact point. Interpolating
/// from a binary64 fraction places this pair at `(-0.333_333_333_333_333_37,
/// 0.833_333_333_333_333_3)`, an ulp off on both axes.

#[test]
fn squared_distance_intervals_contain_the_exact_stored_double_distance() {
    let left = XY::new(
        f64::from_bits(0x3FEC_F685_D0D1_D6E4),
        f64::from_bits(0xC08D_0425_31F4_2A4E),
    );
    let right = XY::new(
        f64::from_bits(0x3F14_D51A_1194_F0F8),
        f64::from_bits(0x40CE_A3AD_0674_290A),
    );
    let point_pair = point_pair_distance_key(left, right);
    assert!(point_pair.is_bounded());
    assert!(
        point_pair.contains_exact(),
        "lo_bits={:#018x} exact_rounded_bits={:#018x} hi_bits={:#018x}",
        point_pair.filter.lo.to_bits(),
        point_pair
            .witness
            .squared()
            .expect("ordinary point-pair exact square")
            .rounded_trustworthy()
            .expect("ordinary point-pair square rounds normally")
            .expect("ordinary point-pair square is normal")
            .to_bits(),
        point_pair.filter.hi.to_bits(),
    );

    // Generated independently with long-double screening, then adjudicated
    // with Python Fraction from these stored doubles. The correctly rounded
    // exact square is ...a57; the certificate must enclose its exact value.
    let mutation_left = XY::new(
        f64::from_bits(0x3FFE_A39A_E95A_135F),
        f64::from_bits(0x3FF6_DB5D_42E1_ECE2),
    );
    let mutation_right = XY::new(
        f64::from_bits(0xBFF0_E338_5449_1D84),
        f64::from_bits(0xBFF3_FF0E_3D7D_76C1),
    );
    let mutation_key = point_pair_distance_key(mutation_left, mutation_right);
    let exact_rounded = f64::from_bits(0x402F_FE5C_67AF_AA57);
    eprintln!(
        "retained interval: lo_bits={:#018x} exact_rounded_bits={:#018x} hi_bits={:#018x}",
        mutation_key.filter.lo.to_bits(),
        exact_rounded.to_bits(),
        mutation_key.filter.hi.to_bits(),
    );
    assert!(
        mutation_key.filter.lo <= exact_rounded && exact_rounded <= mutation_key.filter.hi,
        "lo_bits={:#018x} exact_rounded_bits={:#018x} hi_bits={:#018x}",
        mutation_key.filter.lo.to_bits(),
        exact_rounded.to_bits(),
        mutation_key.filter.hi.to_bits(),
    );
    assert!(mutation_key.contains_exact());

    for index in -128..=128 {
        let shift = f64::from(index) / 32.0;
        let point = XY::new(shift + 0.375, 1.75 - shift / 8.0);
        let segment = Segment {
            start: XY::new(shift - 2.0, -0.75),
            end: XY::new(shift + 3.0, 0.625),
        };
        let key = point_segment_distance_key(point, segment).unwrap();
        assert!(key.is_bounded(), "index={index}");
        assert!(key.contains_exact(), "index={index}");
    }
}

#[test]
fn clearance_aabb_certificates_enclose_and_compare_exact_source_distances() {
    let point = XY::new(0.0, 0.0);
    let minimum = SquaredDistanceFilter::MIN_OPERAND;
    let maximum = SquaredDistanceFilter::MAX_OPERAND;
    assert!(filter_operand(minimum));
    assert!(!filter_operand(f64::from_bits(minimum.to_bits() - 1)));
    assert!(filter_operand(maximum));
    assert!(!filter_operand(f64::from_bits(maximum.to_bits() + 1)));

    let cases = [
        ([3.0, 4.0, 3.0, 4.0], XY::new(3.0, 4.0)),
        ([0.5, -0.75, 2.0, 1.25], XY::new(0.5, 0.0)),
        ([-2.0, -3.0, 2.0, 3.0], XY::new(0.0, 0.0)),
        (
            [
                f64::from_bits(0x3FEC_F685_D0D1_D6E4),
                f64::from_bits(0xC08D_0425_31F4_2A4E),
                f64::from_bits(0x3FEC_F685_D0D1_D6E4),
                f64::from_bits(0xC08D_0425_31F4_2A4E),
            ],
            XY::new(
                f64::from_bits(0x3FEC_F685_D0D1_D6E4),
                f64::from_bits(0xC08D_0425_31F4_2A4E),
            ),
        ),
    ];

    for (aabb, expected_closest) in cases {
        let (witness, filter) = clearance_aabb_filter(point, aabb);
        assert!(filter.lo.is_finite() && filter.hi.is_finite());
        assert!(filter.lo <= filter.hi);
        assert!(matches!(
            witness,
            ExactDistanceWitness::Points(_, closest) if closest == expected_closest
        ));
        assert!(filter.bind(witness).contains_exact());

        let exact_tie = point_pair_distance_key(point, expected_closest);
        let exact_order = exact_selection_distance_cmp(witness, exact_tie.witness).unwrap();
        let binary_order = filter.cmp_witness_key(witness, &exact_tie).unwrap();
        assert_eq!(binary_order, exact_order, "aabb={aabb:?}");
    }

    CLEARANCE_AABB_FILTERS.with(|count| count.set(0));
    CLEARANCE_AABB_FILTER_DECLINES.with(|count| count.set(0));
    CLEARANCE_AABB_COMPARE_DECISIONS.with(|count| count.set(0));
    CLEARANCE_AABB_COMPARE_DECLINES.with(|count| count.set(0));
    let incumbent = point_pair_distance_key(point, XY::new(1.0, 0.0));
    let (far_witness, far_filter) = clearance_aabb_filter(point, [3.0, 0.0, 4.0, 0.0]);
    let far_order = far_filter.cmp_witness_key(far_witness, &incumbent).unwrap();
    assert_eq!(
        far_order,
        exact_selection_distance_cmp(far_witness, incumbent.witness).unwrap()
    );
    assert!(far_order.is_gt());
    assert_eq!(
        CLEARANCE_AABB_COMPARE_DECISIONS.with(std::cell::Cell::get),
        1
    );

    let (tie_witness, tie_filter) = clearance_aabb_filter(point, [1.0, 0.0, 1.0, 0.0]);
    assert_eq!(
        tie_filter.cmp_witness_key(tie_witness, &incumbent).unwrap(),
        Ordering::Equal,
        "an exact AABB tie must reach exact selection rather than prune"
    );
    assert_eq!(
        CLEARANCE_AABB_COMPARE_DECLINES.with(std::cell::Cell::get),
        1
    );

    let tiny = 2.0_f64.powi(-700);
    let (tiny_witness, out_of_band_filter) = clearance_aabb_filter(point, [tiny, 0.0, tiny, 0.0]);
    assert!(!out_of_band_filter.lo.is_finite() || !out_of_band_filter.hi.is_finite());
    assert_eq!(CLEARANCE_AABB_FILTER_DECLINES.with(std::cell::Cell::get), 1);
    assert!(
        out_of_band_filter
            .cmp_witness_key(tiny_witness, &incumbent)
            .unwrap()
            .is_lt(),
        "out-of-band bounds must reach exact comparison without pruning"
    );
    assert_eq!(
        CLEARANCE_AABB_COMPARE_DECLINES.with(std::cell::Cell::get),
        2
    );
}

#[test]
fn binary64_filter_declines_nonzero_products_that_underflow() {
    assert_eq!(
        checked_normal_product(2.0_f64.powi(-600), 2.0_f64.powi(-450)),
        None,
        "a nonzero subnormal product is not an exact zero"
    );
    assert_eq!(
        checked_normal_product(2.0_f64.powi(-600), 2.0_f64.powi(-500)),
        None,
        "a nonzero product rounded all the way to zero must decline"
    );
    assert_eq!(checked_normal_product(0.0, 2.0_f64.powi(200)), Some(0.0));
}

#[test]
fn point_segment_binary64_filters_enclose_exact_oblique_distances_and_decline_edges() {
    POINT_SEGMENT_BINARY64_DECISIONS.with(|count| count.set(0));
    POINT_SEGMENT_BINARY64_DECLINES.with(|count| count.set(0));
    let cases = [
        (XY::new(2.0, 3.0), Segment {
            start: XY::new(0.0, 0.0),
            end: XY::new(3.0, 4.0),
        }),
        (XY::new(0.25, 1.0), Segment {
            start: XY::new(0.0, 0.0),
            end: XY::new(2.0, 1.0),
        }),
        (XY::new(-1.0, 0.0), Segment {
            start: XY::new(0.0, 0.0),
            end: XY::new(3.0, 4.0),
        }),
        (XY::new(4.0, 5.0), Segment {
            start: XY::new(0.0, 0.0),
            end: XY::new(3.0, 4.0),
        }),
        (
            XY::new(
                f64::from_bits(0x3FF2_5843_AE08_FA60),
                f64::from_bits(0x3FEA_C529_C8C6_C6F0),
            ),
            Segment {
                start: XY::new(0.0, 0.0),
                end: XY::new(
                    f64::from_bits(0x3FFE_BB3B_2D82_F9B6),
                    f64::from_bits(0x3FF6_6C3E_CDB1_4CD8),
                ),
            },
        ),
    ];
    let (cancellation_point, cancellation_segment) = cases[4];
    let dx = FilterValue::new(cancellation_segment.end.x).unwrap();
    let dy = FilterValue::new(cancellation_segment.end.y).unwrap();
    let qx = FilterValue::new(cancellation_point.x).unwrap();
    let qy = FilterValue::new(cancellation_point.y).unwrap();
    let exact_determinant = ExactDyadic::from_f64(dx.0)
        .try_product(&ExactDyadic::from_f64(qy.0))
        .unwrap()
        .try_add(
            ExactDyadic::from_f64(-dy.0)
                .try_product(&ExactDyadic::from_f64(qx.0))
                .unwrap(),
        )
        .unwrap();
    let (determinant_lower, determinant_upper) = source_product_sum_interval(
        dx,
        qy,
        dy,
        qx,
        true,
        SquaredDistanceFilter::SEGMENT_DOT_ERROR,
    )
    .expect("ordinary cancellation stays inside the binary64 filter band");
    assert!(
        !ExactDyadic::from_f64(determinant_lower)
            .cmp(&exact_determinant)
            .is_gt()
            && !ExactDyadic::from_f64(determinant_upper)
                .cmp(&exact_determinant)
                .is_lt(),
        "the determinant interval must contain the exact stored-double value"
    );

    for (point, segment) in cases {
        let key = point_segment_exact_distance_key(point, segment);
        assert!(key.is_bounded(), "point={point:?} segment={segment:?}");
        assert!(key.contains_exact(), "point={point:?} segment={segment:?}");
    }
    assert_eq!(
        POINT_SEGMENT_BINARY64_DECISIONS.with(std::cell::Cell::get),
        cases.len(),
        "ordinary oblique projections and endpoint classifications use the binary64 filter"
    );

    for exponent in [-180, -120, -60, 0, 60, 120, 180] {
        let scale = 2.0_f64.powi(exponent);
        let point = XY::new(0.375 * scale, 1.75 * scale);
        let segment = Segment {
            start: XY::new(-2.0 * scale, -0.75 * scale),
            end: XY::new(3.0 * scale, 0.625 * scale),
        };
        let key = point_segment_exact_distance_key(point, segment);
        assert!(key.is_bounded(), "exponent={exponent}");
        assert!(
            key.contains_exact(),
            "exponent={exponent} point={point:?} segment={segment:?}"
        );
    }
    for row in 0..16 {
        let point = XY::new(7.25, 2.75);
        let vertex = |column: i32| {
            XY::new(
                f64::from(column),
                (f64::from(column) * 0.37 + f64::from(row) * 0.03125).sin()
                    + f64::from(row) * 0.002,
            )
        };
        for index in 0..8 {
            let segment = Segment {
                start: vertex(index),
                end: vertex(index + 1),
            };
            let key = point_segment_exact_distance_key(point, segment);
            assert!(
                key.contains_exact(),
                "row={row} segment={index} point={point:?} segment={segment:?}"
            );
        }
    }
    assert!(
        POINT_SEGMENT_BINARY64_DECISIONS.with(std::cell::Cell::get) > cases.len(),
        "the ordinary nearest-row family also uses the binary64 filter"
    );

    let boundary = point_segment_exact_distance_key(XY::new(0.0, 0.0), Segment {
        start: XY::new(0.0, 0.0),
        end: XY::new(3.0, 4.0),
    });
    assert!(!boundary.is_bounded());
    assert!(boundary.contains_exact());

    let tiny = 2.0_f64.powi(-700);
    let tiny_key = point_segment_exact_distance_key(XY::new(0.0, tiny), Segment {
        start: XY::new(0.0, 0.0),
        end: XY::new(tiny, tiny),
    });
    assert!(!tiny_key.is_bounded());
    assert!(tiny_key.contains_exact());
    assert!(
        POINT_SEGMENT_BINARY64_DECLINES.with(std::cell::Cell::get) >= 2,
        "uncertain endpoint and out-of-band coordinates use the existing path"
    );
}

#[test]
fn touching_distance_intervals_defer_equal_witnesses_to_exact_selection() {
    let left_witness = ExactDistanceWitness::Points(XY::new(0.0, 0.0), XY::new(1.0, 1.0));
    let right_witness = ExactDistanceWitness::Points(XY::new(0.0, 0.0), XY::new(1.0, -1.0));
    let left_filter = SquaredDistanceFilter { lo: 1.0, hi: 2.0 };
    let right_filter = SquaredDistanceFilter { lo: 2.0, hi: 3.0 };
    assert_eq!(
        exact_selection_distance_cmp(left_witness, right_witness).unwrap(),
        Ordering::Equal
    );
    assert_eq!(
        left_filter.disjoint_cmp(right_filter),
        None,
        "touching conservative intervals do not establish strict distance order"
    );
    assert_eq!(
        right_filter.disjoint_cmp(left_filter),
        None,
        "touching conservative intervals do not establish strict pruning order"
    );
}

#[test]
fn binary64_source_dot_enclosures_match_exact_dyadic_values() {
    let cases = [
        ([1.0, 1.0], XY::new(3.0, 4.0), XY::new(1.0, 2.0)),
        ([0.0, 1.0], XY::new(0.0, 3.0), XY::new(8.0, 1.0)),
        (
            [1.0, 1.0],
            XY::new(1.0 + f64::EPSILON, 1.0),
            XY::new(1.0, 1.0 + f64::EPSILON),
        ),
        (
            [1.0, -1.0],
            XY::new(2.0_f64.powi(60) + 2.0_f64.powi(40), 2.0_f64.powi(60)),
            XY::new(2.0_f64.powi(60), 2.0_f64.powi(60)),
        ),
    ];
    for (normal, left, right) in cases {
        let exact = source_dot_difference_exact(normal, left, right).unwrap();
        assert_eq!(
            source_dot_cmp(normal, left, right).unwrap(),
            exact.cmp(&ExactDyadic::zero()),
            "normal={normal:?} left={left:?} right={right:?}"
        );
        if let Some((lower, upper)) = source_dot_difference_binary64(normal, left, right) {
            assert!(
                !ExactDyadic::from_f64(lower).cmp(&exact).is_gt(),
                "lower={lower:?} normal={normal:?} left={left:?} right={right:?}"
            );
            assert!(
                !ExactDyadic::from_f64(upper).cmp(&exact).is_lt(),
                "upper={upper:?} normal={normal:?} left={left:?} right={right:?}"
            );
        }
    }

    CLEARANCE_DOT_BINARY64_DECISIONS.with(|count| count.set(0));
    CLEARANCE_DOT_BINARY64_DECLINES.with(|count| count.set(0));
    assert_eq!(
        source_dot_cmp([1.0, 1.0], XY::new(3.0, 4.0), XY::new(1.0, 2.0)).unwrap(),
        Ordering::Greater
    );
    assert_eq!(
        source_dot_cmp(
            [1.0, 1.0],
            XY::new(1.0 + f64::EPSILON, 1.0),
            XY::new(1.0, 1.0 + f64::EPSILON),
        )
        .unwrap(),
        Ordering::Equal
    );
    let maximum = f64::MAX;
    assert_eq!(
        source_dot_cmp(
            [1.0, 1.0],
            XY::new(maximum, maximum),
            XY::new(-maximum, -maximum),
        )
        .unwrap(),
        Ordering::Greater
    );
    assert!(
        CLEARANCE_DOT_BINARY64_DECISIONS.with(std::cell::Cell::get) > 0,
        "ordinary separated dots should use the binary64 certificate"
    );
    assert!(
        CLEARANCE_DOT_BINARY64_DECLINES.with(std::cell::Cell::get) > 0,
        "cancellation should decline to the existing DD/exact path"
    );
}

#[test]
fn clearance_strip_binary64_decisions_keep_ties_and_decline_extremes() {
    CLEARANCE_STRIP_BINARY64_DECISIONS.with(|count| count.set(0));
    CLEARANCE_STRIP_BINARY64_DECLINES.with(|count| count.set(0));
    CLEARANCE_STRIP_EXACT_FALLBACKS.with(|count| count.set(0));

    let incumbent = point_pair_distance_key(XY::new(0.0, 0.0), XY::new(1.0, 0.0));
    let cases = [
        (
            XY::new(2.0, 0.0),
            [1.0, 0.0],
            XY::new(0.0, 0.0),
            XY::new(0.0, 1.0),
        ),
        (
            XY::new(0.5, 0.0),
            [1.0, 0.0],
            XY::new(0.0, 0.0),
            XY::new(0.0, 1.0),
        ),
        (
            XY::new(0.875, 0.875),
            [1.0, 1.0],
            XY::new(0.0, 0.0),
            XY::new(0.0, 0.0),
        ),
        (
            XY::new(0.625, 0.625),
            [1.0, 1.0],
            XY::new(0.0, 0.0),
            XY::new(0.0, 0.0),
        ),
    ];
    for (point, normal, low, high) in cases {
        let exact = exact_clearance_strip_oracle(point, normal, low, high, &incumbent);
        assert_eq!(
            clearance_strip_binary64_result(point, normal, low, high, &incumbent),
            Some(exact),
            "point={point:?} normal={normal:?}"
        );
    }
    assert!(
        clearance_strip_exceeds(
            XY::new(2.0, 0.0),
            [1.0, 0.0],
            XY::new(0.0, 0.0),
            XY::new(0.0, 1.0),
            &incumbent,
        )
        .unwrap()
    );
    assert!(
        !clearance_strip_exceeds(
            XY::new(0.5, 0.0),
            [1.0, 0.0],
            XY::new(0.0, 0.0),
            XY::new(0.0, 1.0),
            &incumbent,
        )
        .unwrap()
    );
    assert_eq!(
        clearance_strip_binary64_result(
            XY::new(1.0, 0.0),
            [1.0, 0.0],
            XY::new(0.0, 0.0),
            XY::new(0.0, 1.0),
            &incumbent,
        ),
        None,
        "the exact boundary tie must not be decided by rounded intervals"
    );
    assert!(
        !exact_clearance_strip_oracle(
            XY::new(1.0, 0.0),
            [1.0, 0.0],
            XY::new(0.0, 0.0),
            XY::new(0.0, 1.0),
            &incumbent,
        ),
        "the exact equality case is retain, not prune"
    );
    assert!(
        !clearance_strip_exceeds(
            XY::new(1.0, 0.0),
            [1.0, 0.0],
            XY::new(0.0, 0.0),
            XY::new(0.0, 1.0),
            &incumbent,
        )
        .unwrap()
    );
    assert_eq!(
        CLEARANCE_STRIP_EXACT_FALLBACKS.with(std::cell::Cell::get),
        1
    );

    let tiny = 2.0_f64.powi(-700);
    let tiny_incumbent = point_pair_distance_key(XY::new(0.0, 0.0), XY::new(tiny, 0.0));
    assert_eq!(
        clearance_strip_binary64_result(
            XY::new(2.0 * tiny, 0.0),
            [1.0, 0.0],
            XY::new(0.0, 0.0),
            XY::new(0.0, tiny),
            &tiny_incumbent,
        ),
        None,
        "out-of-band subnormal squared products must decline"
    );
    assert!(
        clearance_strip_exceeds(
            XY::new(2.0 * tiny, 0.0),
            [1.0, 0.0],
            XY::new(0.0, 0.0),
            XY::new(0.0, tiny),
            &tiny_incumbent,
        )
        .unwrap()
    );
    assert!(
        CLEARANCE_STRIP_BINARY64_DECISIONS.with(std::cell::Cell::get) >= 2,
        "ordinary prune and retain cases should use binary64"
    );
    assert_eq!(
        CLEARANCE_STRIP_BINARY64_DECLINES.with(std::cell::Cell::get),
        2,
        "the public boundary tie and tiny case should decline to the existing path"
    );
}

#[test]
fn clearance_strip_bounds_compare_exact_source_distances_at_all_scales() {
    CLEARANCE_STRIP_BINARY64_DECISIONS.with(|count| count.set(0));
    CLEARANCE_STRIP_BINARY64_DECLINES.with(|count| count.set(0));
    CLEARANCE_STRIP_EXACT_FALLBACKS.with(|count| count.set(0));
    let ordinary_key = point_pair_distance_key(XY::new(0.0, 0.0), XY::new(1.0, 0.0));
    let ordinary_fallbacks = CLEARANCE_STRIP_EXACT_FALLBACKS.with(std::cell::Cell::get);
    assert!(
        !clearance_strip_exceeds(
            XY::new(0.0, 0.0),
            [1.0, 0.0],
            XY::new(0.0, -1.0),
            XY::new(0.0, 1.0),
            &ordinary_key,
        )
        .unwrap()
    );
    assert_eq!(
        CLEARANCE_STRIP_EXACT_FALLBACKS.with(std::cell::Cell::get),
        ordinary_fallbacks,
        "a point inside its source strip should certify no-prune"
    );
    assert!(
        !clearance_strip_exceeds(
            XY::new(0.5, 0.0),
            [1.0, 0.0],
            XY::new(0.0, 0.0),
            XY::new(0.0, 1.0),
            &ordinary_key,
        )
        .unwrap()
    );
    assert_eq!(
        CLEARANCE_STRIP_EXACT_FALLBACKS.with(std::cell::Cell::get),
        ordinary_fallbacks,
        "a positive strip bound below the incumbent should certify no-prune"
    );
    assert!(
        clearance_strip_exceeds(
            XY::new(2.0, 0.0),
            [1.0, 0.0],
            XY::new(0.0, 0.0),
            XY::new(0.0, 1.0),
            &ordinary_key,
        )
        .unwrap()
    );
    assert_eq!(
        CLEARANCE_STRIP_EXACT_FALLBACKS.with(std::cell::Cell::get),
        ordinary_fallbacks,
        "an ordinary strict strip separation should prune without exact fallback"
    );
    assert!(
        !clearance_strip_exceeds(
            XY::new(0.625, 0.625),
            [1.0, 1.0],
            XY::new(0.0, 0.0),
            XY::new(0.0, 0.0),
            &ordinary_key,
        )
        .unwrap()
    );
    assert_eq!(
        CLEARANCE_STRIP_EXACT_FALLBACKS.with(std::cell::Cell::get),
        ordinary_fallbacks,
        "an oblique positive bound below the incumbent should certify no-prune"
    );
    assert!(
        clearance_strip_exceeds(
            XY::new(0.875, 0.875),
            [1.0, 1.0],
            XY::new(0.0, 0.0),
            XY::new(0.0, 0.0),
            &ordinary_key,
        )
        .unwrap()
    );
    assert_eq!(
        CLEARANCE_STRIP_EXACT_FALLBACKS.with(std::cell::Cell::get),
        ordinary_fallbacks,
        "an oblique strict bound above the incumbent should prune"
    );
    for spacing in [2.0_f64.powi(-700), 1.0, 2.0_f64.powi(600)] {
        let low = XY::new(0.0, 0.0);
        let high = XY::new(0.0, spacing);
        let incumbent = point_pair_distance_key(low, XY::new(spacing, 0.0));
        assert!(
            clearance_strip_exceeds(
                XY::new(2.0 * spacing, 0.0),
                [1.0, 0.0],
                low,
                high,
                &incumbent,
            )
            .unwrap()
        );

        let exact_tie = point_pair_distance_key(low, XY::new(spacing, 0.0));
        assert!(
            !clearance_strip_exceeds(XY::new(spacing, 0.0), [1.0, 0.0], low, high, &exact_tie,)
                .unwrap()
        );
    }
    assert!(
        CLEARANCE_STRIP_EXACT_FALLBACKS.with(std::cell::Cell::get) >= 4,
        "tiny/huge strip squares must reach the exact ratio comparison"
    );
    assert!(
        CLEARANCE_STRIP_BINARY64_DECISIONS.with(std::cell::Cell::get) > 0,
        "ordinary strip comparisons should use binary64 decisions"
    );
    assert!(
        CLEARANCE_STRIP_BINARY64_DECLINES.with(std::cell::Cell::get) > 0,
        "out-of-band strip comparisons should decline"
    );

    let origin = 2.0_f64.powi(60);
    let translated = XY::new(origin + 2.0_f64.powi(40), origin + 3.0 * 2.0_f64.powi(12));
    let source_origin = XY::new(origin, origin);
    assert_eq!(
        source_dot_cmp([1.0, -1.0], translated, source_origin).unwrap(),
        Ordering::Greater,
    );
    let maximum = f64::MAX;
    assert_eq!(
        source_dot_cmp(
            [1.0, 1.0],
            XY::new(maximum, maximum),
            XY::new(-maximum, -maximum),
        )
        .unwrap(),
        Ordering::Greater,
    );
}

#[test]
fn certified_zero_source_dot_avoids_exact_fallback() {
    CLEARANCE_STRIP_SUPPORT_FALLBACKS.with(|count| count.set(0));
    assert_eq!(
        source_dot_cmp([1.0, 0.0], XY::new(0.0, 0.0), XY::new(0.0, 1.0)).unwrap(),
        Ordering::Equal,
    );
    assert_eq!(
        CLEARANCE_STRIP_SUPPORT_FALLBACKS.with(std::cell::Cell::get),
        0,
    );
}

#[test]
fn ordinary_selection_uses_no_exact_fallback() {
    EXACT_SELECTION_FALLBACKS.with(|count| count.set(0));
    for index in -512..=512 {
        let shift = f64::from(index) / 64.0;
        let probe = XY::new(shift + 0.25, 2.0 + shift / 32.0);
        let near = Segment {
            start: XY::new(shift - 1.0, 0.0),
            end: XY::new(shift + 2.0, 0.0),
        };
        let far = Segment {
            start: XY::new(shift - 1.0, -4.0),
            end: XY::new(shift + 2.0, -4.0),
        };
        assert!(
            point_segment_distance_key(probe, near)
                .unwrap()
                .cmp(&point_segment_distance_key(probe, far).unwrap())
                .unwrap()
                .is_lt()
        );
    }
    let entries = EXACT_SELECTION_FALLBACKS.with(std::cell::Cell::get);
    eprintln!("ordinary exact-selection fallback entries: {entries}");
    assert_eq!(entries, 0);
}

#[test]
fn clearance_distance_comparison_counts_its_exact_fallback() {
    let tiny = 2.0_f64.powi(-700);
    let near = point_pair_distance_key(XY::new(0.0, 0.0), XY::new(tiny, 0.0));
    let far = point_pair_distance_key(XY::new(0.0, 0.0), XY::new(2.0 * tiny, 0.0));
    EXACT_SELECTION_FALLBACKS.with(|count| count.set(0));
    assert!(near.cmp_distance(&far).unwrap().is_lt());
    assert_eq!(EXACT_SELECTION_FALLBACKS.with(std::cell::Cell::get), 1);

    let near = point_pair_distance_key(XY::new(0.0, 0.0), XY::new(1.0, 0.0));
    let far = point_pair_distance_key(XY::new(0.0, 0.0), XY::new(2.0, 0.0));
    EXACT_SELECTION_FALLBACKS.with(|count| count.set(0));
    assert!(near.cmp_distance(&far).unwrap().is_lt());
    assert_eq!(EXACT_SELECTION_FALLBACKS.with(std::cell::Cell::get), 0);
}

#[test]
fn ordinary_nearest_rows_use_no_exact_fallback() {
    let probe = XY::new(7.25, 2.75);
    EXACT_SELECTION_FALLBACKS.with(|count| count.set(0));
    for row in 0..128 {
        let mut best = None;
        for index in 0..16 {
            let vertex = |column: i32| {
                XY::new(
                    f64::from(column),
                    (f64::from(column) * 0.37 + f64::from(row) * 0.03125).sin()
                        + f64::from(row) * 0.002,
                )
            };
            let key = point_segment_distance_key(probe, Segment {
                start: vertex(index),
                end: vertex(index + 1),
            })
            .unwrap();
            if best
                .as_ref()
                .is_none_or(|incumbent| key.cmp(incumbent).unwrap().is_lt())
            {
                best = Some(key);
            }
        }
    }
    let entries = EXACT_SELECTION_FALLBACKS.with(std::cell::Cell::get);
    eprintln!("ordinary nearest-row exact-selection fallback entries: {entries}");
    assert_eq!(entries, 0);
}

#[test]
fn ordinary_public_nearest_rows_use_no_exact_fallback() {
    let probe = Shape::Point(Point::new_raw_xy(7.25, 2.75));
    EXACT_SELECTION_FALLBACKS.with(|count| count.set(0));
    for row in 0..128 {
        let points = (0..17)
            .map(|column| {
                Point::new_raw_xy(
                    f64::from(column),
                    (f64::from(column) * 0.37 + f64::from(row) * 0.03125).sin()
                        + f64::from(row) * 0.002,
                )
            })
            .collect::<Vec<_>>();
        let line = Shape::LineString(
            LineSeq::try_new(CoordSeq::try_from_points(&points).unwrap())
                .expect("ordinary test line is valid"),
        );
        line.nearest_points(&probe)
            .expect("non-empty inputs have a witness");
    }
    let entries = EXACT_SELECTION_FALLBACKS.with(std::cell::Cell::get);
    eprintln!("ordinary public nearest-row exact-selection fallback entries: {entries}");
    assert_eq!(entries, 0);
}

#[test]
fn cancelling_near_endpoint_selection_escalates_and_keeps_the_interior() {
    let segment = Segment {
        start: XY::new(0.0, 0.0),
        end: XY::new(
            f64::from_bits(0x3FF7_1758_A881_ED14),
            f64::from_bits(0xBFE2_894F_8720_F120),
        ),
    };
    let point = XY::new(
        f64::from_bits(0x3FDC_60BC_D080_4429),
        f64::from_bits(0x3FF1_ACED_9BE6_93E6),
    );
    EXACT_SELECTION_FALLBACKS.with(|count| count.set(0));
    let interior = point_segment_distance_key(point, segment).unwrap();
    let endpoint = point_pair_distance_key(point, segment.start);
    assert!(interior.is_bounded());
    assert!(interior.contains_exact());
    assert!(interior.cmp(&endpoint).unwrap().is_lt());
    let entries = EXACT_SELECTION_FALLBACKS.with(std::cell::Cell::get);
    eprintln!("cancelling exact-selection fallback entries: {entries}");
    assert_eq!(entries, 1);
}

#[test]
fn faithful_case_a_keeps_the_exactly_closer_lerp_witness() {
    let segment = Segment {
        start: XY::new(0.0, 0.0),
        end: XY::new(
            f64::from_bits(0x2013_6323_7586_2748),
            f64::from_bits(0x2000_FBF1_9953_AE0A),
        ),
    };
    let point = XY::new(
        f64::from_bits(0xA018_FBE4_C3D7_9F2E),
        f64::from_bits(0x2039_6423_3D01_E367),
    );
    let witness = segment_projection(point, segment)
        .unwrap()
        .interpolate_xy(segment)
        .unwrap();
    assert_eq!(
        witness,
        XY::new(
            f64::from_bits(0x2010_5D33_A16A_A5D1),
            f64::from_bits(0x1FFC_ABE0_86EE_B4F9),
        ),
        "witness_bits=({:#018x},{:#018x})",
        witness.x.to_bits(),
        witness.y.to_bits(),
    );

    let reversed = reverse_segment(segment);
    let reversed_witness = segment_projection(point, reversed)
        .unwrap()
        .interpolate_xy(reversed)
        .unwrap();
    assert_eq!(
        reversed_witness,
        XY::new(
            f64::from_bits(0x2010_5D33_A16A_A5D2),
            f64::from_bits(0x1FFC_ABE0_86EE_B4F9),
        ),
    );
    assert!(
        point_segment_distance_key(point, segment)
            .unwrap()
            .cmp(&point_segment_distance_key(point, reversed).unwrap())
            .unwrap()
            .is_eq(),
        "source selection must identify equal reversed-support distances",
    );
}

#[test]
fn source_selection_compares_the_continuum_foot_against_a_point_candidate() {
    let segment = Segment {
        start: XY::new(-2.504_967_266_659_778_4, -0.228_559_855_772_118_68),
        end: XY::new(-2.640_447_616_310_041, 2.361_006_325_545_838),
    };
    let probe = XY::new(-2.506_180_295_312_065, -0.186_351_125_729_832_03);
    let point_candidate = XY::new(-2.506_533_121_444_889_4, -0.185_421_981_330_493_8);
    let emitted = segment_projection(probe, segment)
        .unwrap()
        .interpolate_xy(segment)
        .unwrap();
    assert_eq!(
        emitted,
        XY::new(-2.507_172_816_964_487_3, -0.186_403_052_257_709_84),
    );
    assert!(
        point_pair_distance_key(probe, point_candidate)
            .cmp(&point_segment_distance_key(probe, segment).unwrap())
            .unwrap()
            .is_gt(),
        "the source projection is exactly closer than the point candidate",
    );
}

#[test]
fn exact_source_distance_survives_a_coincident_emitted_foot() {
    let segment = Segment {
        start: XY::new(0.0, 0.0),
        end: XY::new(
            f64::from_bits(0x10D2_C800_0000_0000),
            f64::from_bits(0x10BB_2000_0000_0000),
        ),
    };
    let point = XY::new(
        f64::from_bits(0x10CB_BC18_D59F_50B3),
        f64::from_bits(0x10B4_0731_074E_D707),
    );
    let projection = segment_projection(point, segment).unwrap();
    let emitted = projection.interpolate_xy(segment).unwrap();
    assert_eq!(emitted, point);
    assert!(projection.uses_exact_ratio());

    let distance = super::point_segment_distance(point, segment).unwrap();
    assert_eq!(distance.to_bits(), 0x0D06_6F97_90E5_906D);
    assert!(distance > 0.0);
    assert!(
        super::point_segment_distance_key(point, segment)
            .unwrap()
            .cmp(&super::point_pair_distance_key(point, point))
            .unwrap()
            .is_gt(),
        "source selection must rank a positive separation after true contact"
    );
    assert_eq!(
        super::point_segment_distance_squared(point, segment)
            .unwrap()
            .to_bits(),
        0,
        "the source square is below the binary64 normal range, while the source distance remains positive"
    );
}

#[test]
fn source_distance_uses_the_original_determinant_for_ulp_skew() {
    let segment = Segment {
        start: XY::new(-1.0, (-1.0_f64).next_up()),
        end: XY::new(1.0, 1.0),
    };
    let point = XY::new(0.5, 0.5);
    let (distance, projection) =
        super::point_segment_distance_and_projection(point, segment).unwrap();

    assert_eq!(distance.to_bits(), 0x3C76_A09E_667F_3BCD);
    assert!(!projection.is_start() && !projection.is_end());
    assert!(
        super::point_segment_distance_key(point, segment)
            .unwrap()
            .cmp(&super::point_pair_distance_key(point, point))
            .unwrap()
            .is_gt()
    );
}

#[test]
fn source_key_retains_positive_distance_when_scalar_underflows() {
    let minimum = f64::from_bits(1);
    let segment = Segment {
        start: XY::new(0.0, 0.0),
        end: XY::new(f64::MAX, minimum),
    };
    let point = XY::new(minimum, 0.0);
    let (distance, _) = super::point_segment_distance_and_projection(point, segment).unwrap();
    assert_eq!(distance, 0.0);
    assert!(
        super::point_segment_distance_key(point, segment)
            .unwrap()
            .cmp(&super::point_pair_distance_key(point, point))
            .unwrap()
            .is_gt()
    );
}

#[test]
fn exact_source_distance_reports_infinity_for_finite_overflowing_distance() {
    let maximum = f64::MAX;
    let segment = Segment {
        start: XY::new(-maximum, -maximum),
        end: XY::new(maximum, maximum),
    };
    let point = XY::new(maximum, -maximum);
    let projection = segment_projection(point, segment).unwrap();
    assert!(projection.uses_exact_ratio());
    assert_eq!(
        super::point_segment_distance(point, segment).unwrap(),
        f64::INFINITY
    );
    assert_eq!(
        super::point_segment_distance_squared(point, segment).unwrap(),
        f64::INFINITY
    );
}
