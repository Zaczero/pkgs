use crate::error::ErrorKind;
use crate::geometry::segments::{
    ExactDistanceWitness, FilterValue, SquaredDistanceFilter, checked_normal_product,
    clearance_strip_binary64_result, clip_segment_rect_interval, compare_along_segment,
    exact_selection_distance_cmp, filter_operand, segment_nearest_points,
    source_dot_difference_binary64, source_dot_difference_exact, source_product_sum_interval,
};
use crate::geometry::tessellation::exact::{ExactDyadic, compare_ratios};
use crate::geometry::*;

struct RemainingWork(u64);

impl Work for RemainingWork {
    type Error = crate::error::Error;

    fn charge(&mut self, ticks: u64) -> crate::error::Result<()> {
        let Some(remaining) = self.0.checked_sub(ticks) else {
            return Err(crate::error::ResourceError::SizeOverflow.into());
        };
        self.0 = remaining;
        Ok(())
    }

    fn charge_product(&mut self, count: usize, ticks_per_item: usize) -> crate::error::Result<()> {
        self.charge(crate::geometry::work_product_ticks(count, ticks_per_item)?)
    }
}

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
fn cuts_sort_by_directed_coordinate_at_float_extremes() {
    let cases = [
        (
            Segment {
                start: XY::new(0.0, 0.0),
                end: XY::new(4.0, 0.0),
            },
            XY::new(1.0, 0.0),
            XY::new(3.0, 0.0),
        ),
        (
            Segment {
                start: XY::new(0.0, 0.0),
                end: XY::new(0.0, 4.0),
            },
            XY::new(0.0, 1.0),
            XY::new(0.0, 3.0),
        ),
        (
            Segment {
                start: XY::new(4.0, 0.0),
                end: XY::new(0.0, 0.0),
            },
            XY::new(3.0, 0.0),
            XY::new(1.0, 0.0),
        ),
        (
            Segment {
                start: XY::new(-4.0, 0.0),
                end: XY::new(0.0, 0.0),
            },
            XY::new(-3.0, 0.0),
            XY::new(-1.0, 0.0),
        ),
        (
            Segment {
                start: XY::new(0.0, 0.0),
                end: XY::new(4.0 * f64::from_bits(1), 0.0),
            },
            XY::new(f64::from_bits(1), 0.0),
            XY::new(3.0 * f64::from_bits(1), 0.0),
        ),
        (
            Segment {
                start: XY::new(-f64::MAX, 0.0),
                end: XY::new(f64::MAX, 0.0),
            },
            XY::new(-f64::MAX / 2.0, 0.0),
            XY::new(f64::MAX / 2.0, 0.0),
        ),
    ];
    for (segment, first, second) in cases {
        assert_eq!(
            compare_along_segment(segment, first, second),
            std::cmp::Ordering::Less
        );
    }
}

#[test]
fn cut_order_canonicalizes_signed_zero_before_secondary_ties() {
    let diagonal = Segment {
        start: XY::new(-2.0, 0.0),
        end: XY::new(2.0, 4.0),
    };
    assert_eq!(
        compare_along_segment(diagonal, XY::new(0.0, 1.0), XY::new(-0.0, 3.0)),
        std::cmp::Ordering::Less
    );
    assert_eq!(
        compare_along_segment(diagonal, XY::new(-0.0, 2.0), XY::new(0.0, 2.0)),
        std::cmp::Ordering::Equal
    );

    let descending = Segment {
        start: XY::new(0.0, 4.0),
        end: XY::new(4.0, 0.0),
    };
    assert_eq!(
        compare_along_segment(descending, XY::new(2.0, 3.0), XY::new(2.0, 1.0)),
        std::cmp::Ordering::Less
    );
}

/// A real unit-length shared edge on a 1e13-long segment has a projection
/// fraction span of ~1e-13 — below the old `1e-12` cutoff that wrongly
/// dropped it to a point touch. The exact-span path must keep it, returning
/// the actual input endpoints (no interpolation jitter).
#[test]
fn shared_segment_part_keeps_tiny_overlap_on_huge_coordinates() {
    let left = Segment {
        start: XY::new(0.0, 0.0),
        end: XY::new(1.0e13, 0.0),
    };
    let right = Segment {
        start: XY::new(1.0e13 - 1.0, 0.0),
        end: XY::new(2.0e13, 0.0),
    };
    let span = shared_segment_part(left, right).expect("positive-length shared run");
    assert!(span.same_direction);
    assert_eq!(span.start, XY::new(1.0e13 - 1.0, 0.0));
    assert_eq!(span.end, XY::new(1.0e13, 0.0));
}

/// Direction of a shared collinear run comes from endpoint ORDER on the
/// dominant axis, never a direction-vector dot product: for tiny (~1e-162)
/// segments the products underflow to `0.0`, which wrongly reported two
/// IDENTICAL segments as opposite-direction. Every scale must classify identical
/// segments as same-direction.
#[test]
fn shared_segment_part_tiny_identical_segments_stay_same_direction() {
    for length in [1.0, 1.0e-160, 1.0e-162, 1.0e-170] {
        let segment = Segment {
            start: XY::new(0.0, 0.0),
            end: XY::new(length, 0.0),
        };
        let span = shared_segment_part(segment, segment).expect("positive-length shared run");
        assert!(
            span.same_direction,
            "length {length:e} misclassified as opposite"
        );
    }
}

/// Collinear segments meeting at exactly one endpoint are a point touch,
/// not a run — the exact identity test (`start == end`) rejects them.
#[test]
fn shared_segment_part_rejects_collinear_point_touch() {
    let left = Segment {
        start: XY::new(0.0, 0.0),
        end: XY::new(1.0, 0.0),
    };
    let right = Segment {
        start: XY::new(1.0, 0.0),
        end: XY::new(2.0, 0.0),
    };
    assert!(shared_segment_part(left, right).is_none());
}

/// The closest pair of two non-intersecting segments can be an EDGE-INTERIOR
/// foot, not a vertex: a vertical stub above the middle of a horizontal
/// segment witnesses at the perpendicular foot (5, 0), which is interior to
/// the horizontal segment.
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
fn segment_cross_point_is_the_correctly_rounded_crossing() {
    // Exactly (-1/3, 5/6).
    let left = Segment {
        start: XY::new(0.5, 0.5),
        end: XY::new(-2.0, 1.5),
    };
    let right = Segment {
        start: XY::new(0.0, 1.0),
        end: XY::new(-1.0, 0.5),
    };
    let point = segment_cross_point(left, right)
        .unwrap()
        .expect("certified crossing");
    assert_eq!(
        point,
        segment_cross_point(right, left)
            .unwrap()
            .expect("symmetric crossing")
    );
    assert_eq!(point.x.to_bits(), (-1.0_f64 / 3.0).to_bits());
    assert_eq!(point.y.to_bits(), (5.0_f64 / 6.0).to_bits());
}

/// An axis-aligned operand pins its own ordinate exactly, so the crossing of a
/// diagonal with a vertical edge is representable on both axes.
#[test]
fn segment_cross_point_meets_an_axis_aligned_edge_exactly() {
    let diagonal = Segment {
        start: XY::new(-1.0, 2.0),
        end: XY::new(2.0, -1.0),
    };
    for (name, edge, expected) in [
        (
            "vertical",
            Segment {
                start: XY::new(0.0, 0.0),
                end: XY::new(0.0, 4.0),
            },
            XY::new(0.0, 1.0),
        ),
        (
            "horizontal",
            Segment {
                start: XY::new(-4.0, 0.0),
                end: XY::new(4.0, 0.0),
            },
            XY::new(1.0, 0.0),
        ),
    ] {
        let point = segment_cross_point(diagonal, edge)
            .unwrap()
            .expect("certified crossing");
        assert_eq!(point.x.to_bits(), expected.x.to_bits(), "{name}: x");
        assert_eq!(point.y.to_bits(), expected.y.to_bits(), "{name}: y");
    }
}

/// A near-parallel crossing that the robust predicate finds is placed, and
/// the same point is returned whichever segment comes first.
#[test]
fn segment_cross_point_places_near_parallel_crossing_symmetrically() {
    let left = Segment {
        start: XY::new(0.0, 0.0),
        end: XY::new(1.0, 1.0e-13),
    };
    let right = Segment {
        start: XY::new(0.0, 1.0e-13),
        end: XY::new(1.0, 0.0),
    };
    let point = segment_cross_point(left, right)
        .unwrap()
        .expect("certified crossing");
    assert_eq!(
        point,
        segment_cross_point(right, left)
            .unwrap()
            .expect("symmetric crossing")
    );
    assert_eq!(point.x.to_bits(), 0.5_f64.to_bits());
    assert_eq!(point.y.to_bits(), 5.0e-14_f64.to_bits());
}

/// Products at this scale overflow binary64; the crossing is still the
/// rounding of the exact point, invariant under operand order and direction.
#[test]
fn segment_cross_point_rounds_crossings_of_extreme_finite_coordinates() {
    let scale = 2.0_f64.powi(600);
    let left = Segment {
        start: XY::new(scale, scale),
        end: XY::new(2.0 * scale, 2.0 * scale),
    };
    let right = Segment {
        start: XY::new(scale, 2.0 * scale),
        end: XY::new(2.0 * scale, scale),
    };
    let point = segment_cross_point(left, right)
        .unwrap()
        .expect("scaled crossing");
    assert!(point.x.is_finite() && point.y.is_finite());
    for (a, b) in [
        (right, left),
        (reverse_segment(left), right),
        (left, reverse_segment(right)),
        (reverse_segment(right), reverse_segment(left)),
    ] {
        assert_eq!(
            point,
            segment_cross_point(a, b).unwrap().expect("same crossing")
        );
    }
    assert_eq!(point.x.to_bits(), (1.5 * scale).to_bits());
    assert_eq!(point.x.to_bits(), point.y.to_bits());
    assert!(point_on_segment(point, left.start, left.end));
    assert!(point_on_segment(point, right.start, right.end));
}

/// Determinants below the ordinary-product underflow boundary still describe
/// a real crossing, and the predicate and the construction agree on it.
#[test]
fn segment_cross_point_rounds_crossings_with_subnormal_products() {
    let scale = 1.0e-180;
    let left = Segment {
        start: XY::new(0.0, 0.0),
        end: XY::new(scale, 2.0 * scale),
    };
    let right = Segment {
        start: XY::new(0.25 * scale, -scale),
        end: XY::new(0.25 * scale, 3.0 * scale),
    };
    let point = segment_cross_point(left, right)
        .unwrap()
        .expect("scaled-up crossing");
    for (a, b) in [
        (right, left),
        (reverse_segment(left), right),
        (left, reverse_segment(right)),
        (reverse_segment(right), reverse_segment(left)),
    ] {
        assert_eq!(
            point,
            segment_cross_point(a, b).unwrap().expect("same crossing")
        );
    }
    assert_eq!(point.x.to_bits(), (0.25 * scale).to_bits());
    assert_eq!(point.y.to_bits(), (0.5 * scale).to_bits());
    assert!(point_on_segment(point, left.start, left.end));
    assert!(point_on_segment(point, right.start, right.end));
}

/// A tiny vertical span crossing a huge horizontal span keeps both axes: a
/// single scale chosen from the largest coordinate would collapse the
/// vertical segment to zero.
#[test]
fn segment_cross_point_preserves_mixed_exponent_axes() {
    let horizontal = Segment {
        start: XY::new(-1.0e300, 0.0),
        end: XY::new(1.0e300, 0.0),
    };
    let vertical = Segment {
        start: XY::new(1.0e-300, -1.0e-300),
        end: XY::new(1.0e-300, 1.0e-300),
    };
    let point = segment_cross_point(horizontal, vertical)
        .unwrap()
        .expect("mixed-scale crossing");
    assert_eq!(point.x.to_bits(), 1.0e-300_f64.to_bits());
    assert_eq!(point.y.to_bits(), 0.0_f64.to_bits());
    assert!(point_on_segment(point, horizontal.start, horizontal.end));
    assert!(point_on_segment(point, vertical.start, vertical.end));
    assert_eq!(
        point,
        segment_cross_point(reverse_segment(vertical), reverse_segment(horizontal))
            .unwrap()
            .expect("same crossing")
    );
}

/// A long diagonal crossing a tiny diagonal close to its start has an exact
/// witness, but the long segment's parametric fraction is below binary64's
/// representable range; the exact construction still places it.
#[test]
fn segment_cross_point_preserves_mixed_exponents_within_each_axis() {
    let huge = 2.0_f64.powi(1000);
    let tiny = 2.0_f64.powi(-999);
    let long = Segment {
        start: XY::new(0.0, 0.0),
        end: XY::new(huge, huge),
    };
    let short = Segment {
        start: XY::new(0.0, tiny),
        end: XY::new(tiny, 0.0),
    };
    let expected = 2.0_f64.powi(-1000);
    let point = segment_cross_point(long, short)
        .unwrap()
        .expect("mixed-scale diagonal crossing");
    assert_eq!(point, XY::new(expected, expected));
    assert!(point_on_segment(point, long.start, long.end));
    assert!(point_on_segment(point, short.start, short.end));
    assert_eq!(
        point,
        segment_cross_point(reverse_segment(short), reverse_segment(long))
            .unwrap()
            .expect("same crossing")
    );
}

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

fn assert_projection_ratio(
    projection: &super::SegmentProjection,
    numerator: f64,
    denominator: f64,
) {
    let expected_numerator = ExactDyadic::from_f64(numerator);
    let expected_denominator = ExactDyadic::from_f64(denominator);
    match projection {
        super::SegmentProjection::CertifiedInterior(certified) => {
            let exact = certified.exact().unwrap();
            assert_eq!(
                compare_ratios(
                    &exact.numerator,
                    &exact.denominator,
                    &expected_numerator,
                    &expected_denominator,
                )
                .unwrap(),
                std::cmp::Ordering::Equal,
            );
        },
        super::SegmentProjection::ExactInterior(exact) => {
            assert_eq!(
                compare_ratios(
                    &exact.numerator,
                    &exact.denominator,
                    &expected_numerator,
                    &expected_denominator,
                )
                .unwrap(),
                std::cmp::Ordering::Equal,
            );
        },
        other @ (super::SegmentProjection::Start
        | super::SegmentProjection::Interior(_)
        | super::SegmentProjection::End) => {
            panic!("expected a source-bound interior, got {other:?}")
        },
    }
}

#[test]
fn cancelling_projection_retains_exact_ratio_on_reversed_supports() {
    let segment = Segment {
        start: XY::new(0.0, 0.0),
        end: XY::new(13.0, 13.0),
    };
    let point = XY::new(20.0, -6.0);
    let projection = segment_projection(point, segment).unwrap();
    assert!(projection.uses_exact_ratio());
    assert_projection_ratio(&projection, 7.0, 13.0);
    assert_eq!(
        projection.interpolate_xy(segment).unwrap(),
        XY::new(7.0, 7.0)
    );
    assert!(
        projection
            .cmp_along(&super::SegmentProjection::Start)
            .unwrap()
            .is_gt()
    );
    assert!(
        projection
            .cmp_along(&super::SegmentProjection::End)
            .unwrap()
            .is_lt()
    );

    let reversed = reverse_segment(segment);
    let reversed_projection = segment_projection(point, reversed).unwrap();
    assert!(reversed_projection.uses_exact_ratio());
    assert_projection_ratio(&reversed_projection, 6.0, 13.0);
    assert_eq!(
        reversed_projection.interpolate_xy(reversed).unwrap(),
        XY::new(7.0, 7.0)
    );
    assert!(projection.cmp_along(&reversed_projection).unwrap().is_gt());
}

#[test]
fn exact_interior_ratio_that_rounds_to_zero_keeps_xy_and_ordinates() {
    let minimum = f64::from_bits(1);
    let extent = f64::from_bits(0x7FE0_0000_0000_0000);
    let segment = Segment {
        start: XY::new(0.0, 0.0),
        end: XY::new(extent, 0.0),
    };
    let point = XY::new(minimum, 1.0);
    let projection = segment_projection(point, segment).unwrap();
    assert!(projection.uses_exact_ratio());
    assert!(!projection.is_start() && !projection.is_end());
    assert_eq!(projection.fraction().to_bits(), 0.0_f64.to_bits());
    assert!(
        projection
            .cmp_along(&super::SegmentProjection::Start)
            .unwrap()
            .is_gt()
    );
    assert!(
        projection
            .cmp_along(&super::SegmentProjection::End)
            .unwrap()
            .is_lt()
    );
    assert_eq!(
        projection.interpolate_xy(segment).unwrap(),
        XY::new(minimum, 0.0)
    );

    let start = Point::new_raw_axes(0.0, 0.0, ZOrdinate(Some(0.0)), MOrdinate(Some(0.0)));
    let end = Point::new_raw_axes(
        extent,
        0.0,
        ZOrdinate(Some(extent)),
        MOrdinate(Some(extent)),
    );
    let lifted = projection.interpolate_point(start, end).unwrap();
    assert_eq!(lifted.x.to_bits(), minimum.to_bits());
    assert_eq!(lifted.z().unwrap().to_bits(), minimum.to_bits());
    assert_eq!(lifted.m().unwrap().to_bits(), minimum.to_bits());
}

#[test]
fn exact_interior_ratio_that_rounds_to_one_keeps_large_ordinates_interior() {
    let previous = 1.0_f64.next_down();
    let large = f64::from_bits(0x7FE0_0000_0000_0000);
    let segment = Segment {
        start: XY::new(0.0, 0.0),
        end: XY::new(1.0, 1.0),
    };
    let projection = segment_projection(XY::new(1.0, previous), segment).unwrap();
    assert!(projection.uses_exact_ratio());
    assert!(!projection.is_start() && !projection.is_end());
    assert_eq!(projection.fraction().to_bits(), 1.0_f64.to_bits());
    assert_eq!(projection.interpolate_xy(segment).unwrap(), segment.end);

    let start = Point::new_raw_axes(0.0, 0.0, ZOrdinate(Some(-large)), MOrdinate(Some(-large)));
    let end = Point::new_raw_axes(1.0, 1.0, ZOrdinate(Some(large)), MOrdinate(Some(large)));
    let lifted = projection.interpolate_point(start, end).unwrap();
    let expected = large.next_down();
    assert_eq!(lifted.z().unwrap().to_bits(), expected.to_bits());
    assert_eq!(lifted.m().unwrap().to_bits(), expected.to_bits());
    assert_ne!(lifted.z().unwrap().to_bits(), end.z().unwrap().to_bits());
    assert_ne!(lifted.m().unwrap().to_bits(), end.m().unwrap().to_bits());
}

#[test]
fn exact_clamp_classification_wins_around_one_plus_two_to_the_minus_fifty_four() {
    let half_ulp = 2.0_f64.powi(-53);
    let segment = Segment {
        start: XY::new(0.0, -half_ulp),
        end: XY::new(1.0, 1.0 - half_ulp),
    };
    let point = XY::new(1.0, 1.0);
    let terms = super::exact_projection_terms(point, segment).unwrap();
    let expected_numerator = ExactDyadic::from_f64(1.0)
        .try_add(ExactDyadic::from_f64(2.0_f64.powi(-54)))
        .unwrap();
    assert_eq!(
        compare_ratios(
            &terms.numerator,
            &terms.denominator,
            &expected_numerator,
            &ExactDyadic::from_f64(1.0),
        )
        .unwrap(),
        std::cmp::Ordering::Equal
    );
    assert!(matches!(
        super::classify_segment_projection_exact(point, segment).unwrap(),
        super::ExactClassifiedSegmentProjection::End
    ));
    let projection = segment_projection(point, segment).unwrap();
    assert!(projection.is_end());
    assert_eq!(
        projection.interpolate_xy(segment).unwrap().y.to_bits(),
        segment.end.y.to_bits()
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

#[test]
fn endpoint_projection_preserves_source_signed_zero_bits() {
    let segment = Segment {
        start: XY::new(-0.0, 0.0),
        end: XY::new(1.0, 1.0),
    };
    let projection = segment_projection(XY::new(-0.0, 0.0), segment).unwrap();
    assert!(projection.is_start());
    let foot = projection.interpolate_xy(segment).unwrap();
    assert_eq!(foot.x.to_bits(), (-0.0_f64).to_bits());
    let start = Point::new_raw_axes(-0.0, 0.0, ZOrdinate(Some(-0.0)), MOrdinate(Some(3.0)));
    let end = Point::new_raw_axes(1.0, 1.0, ZOrdinate(Some(9.0)), MOrdinate(Some(7.0)));
    let lifted = projection.interpolate_point(start, end).unwrap();
    assert_eq!(lifted.x.to_bits(), start.x.to_bits());
    assert_eq!(lifted.z().unwrap().to_bits(), start.z().unwrap().to_bits());
    assert_eq!(lifted.m().unwrap().to_bits(), start.m().unwrap().to_bits());
}

#[test]
fn clip_interval_corner_duplicates_preserve_source_endpoint_bits() {
    let bounds = Bounds::new(0.0, 0.0, 1.0, 1.0).expect("ordered bounds");
    let segment = Segment {
        start: XY::new(-0.0, -0.0),
        end: XY::new(2.0, 2.0),
    };
    let interval = clip_segment_rect_interval(segment, bounds)
        .unwrap()
        .expect("the segment crosses the rectangle");
    assert_eq!(interval.start.point.x.to_bits(), (-0.0_f64).to_bits());
    assert_eq!(interval.start.point.y.to_bits(), (-0.0_f64).to_bits());
    assert_eq!(interval.end.point, XY::new(1.0, 1.0));

    let reversed = Segment {
        start: segment.end,
        end: segment.start,
    };
    let interval = clip_segment_rect_interval(reversed, bounds)
        .unwrap()
        .expect("the reversed segment crosses the rectangle");
    assert_eq!(interval.start.point, XY::new(1.0, 1.0));
    assert_eq!(interval.end.point.x.to_bits(), (-0.0_f64).to_bits());
    assert_eq!(interval.end.point.y.to_bits(), (-0.0_f64).to_bits());
}

fn reverse_segment(segment: Segment) -> Segment {
    Segment {
        start: segment.end,
        end: segment.start,
    }
}

#[cfg(test)]
mod exact_projection_allocation_tests {
    use super::*;
    use crate::test_support::allocation_count;

    fn assert_allocation_count(
        expected_allocations: usize,
        run: impl FnOnce() -> crate::error::Result<()>,
    ) {
        let ((), allocations) = allocation_count(|| run().unwrap());
        assert_eq!(allocations, expected_allocations);
    }

    #[test]
    fn endpoint_exact_classification_is_allocation_free() {
        let segment = Segment {
            start: XY::new(0.0, 0.0),
            end: XY::new(1.0, 0.0),
        };
        let point = XY::new(0.0, 0.0);
        assert_allocation_count(0, || {
            super::super::classify_segment_projection_exact(point, segment).map(|_| ())
        });
    }

    #[test]
    fn historical_equal_exact_projection_retains_ratio_on_publication() {
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
        assert_allocation_count(6, || {
            segment_projection(point, segment).map(|projection| {
                assert!(matches!(
                    projection,
                    super::super::SegmentProjection::ExactInterior(inner)
                        if inner.emitted_foot.is_some()
                ));
            })
        });
    }

    #[test]
    fn ordinary_projection_path_stays_allocation_free() {
        let segment = Segment {
            start: XY::new(0.0, 0.0),
            end: XY::new(2.0, 0.0),
        };
        let ((), allocations) = allocation_count(|| {
            let projection = segment_projection(XY::new(1.0, 1.0), segment).unwrap();
            assert!(!projection.uses_exact_ratio());
            projection.interpolate_xy(segment).unwrap();
        });
        assert_eq!(allocations, 0);
    }

    #[test]
    fn interior_exact_terms_are_allocation_free() {
        let segment = Segment {
            start: XY::new(0.0, 0.0),
            end: XY::new(2.0, 0.0),
        };
        let point = XY::new(1.0, 1.0);
        assert_allocation_count(0, || {
            super::super::exact_projection_terms(point, segment).map(|_| ())
        });
    }

    #[test]
    fn interior_exact_classification_is_allocation_free() {
        let segment = Segment {
            start: XY::new(0.0, 0.0),
            end: XY::new(1.0, 1.0),
        };
        let point = XY::new(1.0, 1.0_f64.next_down());
        assert_allocation_count(0, || {
            super::super::classify_segment_projection_exact(point, segment).map(|classification| {
                assert!(matches!(
                    classification,
                    super::super::ExactClassifiedSegmentProjection::Interior(_)
                ));
            })
        });
    }

    #[test]
    fn retained_exact_ratio_publication_allocates_one_owner() {
        let segment = Segment {
            start: XY::new(0.0, 0.0),
            end: XY::new(1.0, 1.0),
        };
        let point = XY::new(1.0, 1.0_f64.next_down());
        let selection = super::super::point_segment_selection(point, segment).unwrap();
        assert!(
            selection
                .projection(point, segment)
                .unwrap()
                .uses_exact_ratio()
        );
        assert_allocation_count(1, || {
            selection.projection(point, segment).map(|projection| {
                assert!(projection.uses_exact_ratio());
            })
        });
    }

    #[test]
    fn exact_ratio_interpolation_is_allocation_free() {
        let segment = Segment {
            start: XY::new(0.0, 0.0),
            end: XY::new(1.0, 1.0),
        };
        let projection = segment_projection(XY::new(1.0, 1.0_f64.next_down()), segment).unwrap();
        let start = Point::new_raw_axes(0.0, 0.0, ZOrdinate(Some(-1.0)), MOrdinate(Some(-3.0)));
        let end = Point::new_raw_axes(1.0, 1.0, ZOrdinate(Some(5.0)), MOrdinate(Some(9.0)));
        let expected = projection.interpolate_point(start, end).unwrap();
        assert_allocation_count(0, || projection.interpolate_point(start, end).map(|_| ()));
        assert_eq!(
            projection.interpolate_point(start, end).unwrap(),
            expected,
            "repeated interpolation preserves the emitted point"
        );
    }

    #[test]
    fn historical_equal_exact_projection_does_not_rebuild_ratio_on_interpolation() {
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
        let retained = segment_projection(point, segment).unwrap();
        let exact = super::super::exact_interior_projection(point, segment).unwrap();
        let exact = super::super::SegmentProjection::ExactInterior(Box::new(exact));
        let start = Point::new_raw_xy(0.0, 0.0);
        let end = Point::new_raw_axes(
            segment.end.x,
            segment.end.y,
            ZOrdinate(Some(13.0)),
            MOrdinate(Some(29.0)),
        );

        let (retained_result, retained_allocations) =
            allocation_count(|| retained.interpolate_point(start, end));
        let (exact_result, exact_allocations) =
            allocation_count(|| exact.interpolate_point(start, end));
        assert_eq!(retained_result.unwrap(), exact_result.unwrap());
        assert_eq!(
            retained_allocations, exact_allocations,
            "an exact owner with an emitted-foot override must not rebuild N/D"
        );
    }

    #[test]
    fn certified_ratio_interpolation_is_allocation_free() {
        let segment = Segment {
            start: XY::new(0.0, 0.0),
            end: XY::new(13.0, 13.0),
        };
        let projection = segment_projection(XY::new(20.0, -6.0), segment).unwrap();
        assert!(matches!(
            projection,
            super::super::SegmentProjection::CertifiedInterior(_)
        ));
        let start = Point::new_raw_axes(0.0, 0.0, ZOrdinate(Some(1.0)), MOrdinate(Some(3.0)));
        let end = Point::new_raw_axes(13.0, 13.0, ZOrdinate(Some(5.0)), MOrdinate(Some(9.0)));
        assert_allocation_count(0, || projection.interpolate_point(start, end).map(|_| ()));
    }

    #[test]
    fn exact_ratio_xy_interpolation_is_allocation_free() {
        let segment = Segment {
            start: XY::new(0.0, 0.0),
            end: XY::new(1.0, 1.0),
        };
        let projection = segment_projection(XY::new(1.0, 1.0_f64.next_down()), segment).unwrap();
        assert_allocation_count(0, || projection.interpolate_xy(segment).map(|_| ()));
    }

    #[test]
    fn extreme_exact_projection_is_allocation_free() {
        let segment = Segment {
            start: XY::new(1e200, 0.0),
            end: XY::new(1e200 + 1.0, 0.0),
        };
        let point = XY::new(1e200 + 0.5, 1.0);
        assert_allocation_count(0, || segment_projection(point, segment).map(|_| ()));
    }
}

#[test]
fn fallible_sort_is_stable_and_matches_the_standard_sort() {
    for len in (0..=70).chain([255, 256, 257, 1000]) {
        let items: Vec<(u32, usize)> = (0..len)
            .map(|index| ((index as u32).wrapping_mul(2_654_435_761) % 7, index))
            .collect();
        let mut expected = items.clone();
        #[expect(
            clippy::disallowed_methods,
            reason = "The standard stable sort is the independent oracle for equal-key ordering."
        )]
        expected.sort_by_key(|item| item.0);
        let mut sorted = items;
        try_sort_by(&mut sorted, |left, right| Ok(left.0.cmp(&right.0))).unwrap();
        assert_eq!(sorted, expected, "length {len}");
    }

    let mut tied = [
        (1, 'A'),
        (1, 'B'),
        (1, 'C'),
        (1, 'D'),
        (1, 'E'),
        (1, 'F'),
        (1, 'G'),
        (1, 'H'),
        (2, 'I'),
        (2, 'J'),
        (2, 'K'),
        (2, 'L'),
        (2, 'M'),
        (2, 'N'),
        (2, 'O'),
        (2, 'P'),
        (1, 'Q'),
        (2, 'R'),
    ];
    try_sort_by(&mut tied, |left, right| Ok(left.0.cmp(&right.0))).unwrap();
    assert_eq!(tied, [
        (1, 'A'),
        (1, 'B'),
        (1, 'C'),
        (1, 'D'),
        (1, 'E'),
        (1, 'F'),
        (1, 'G'),
        (1, 'H'),
        (1, 'Q'),
        (2, 'I'),
        (2, 'J'),
        (2, 'K'),
        (2, 'L'),
        (2, 'M'),
        (2, 'N'),
        (2, 'O'),
        (2, 'P'),
        (2, 'R'),
    ]);
}

#[test]
fn synthetic_work_error_precedes_a_sort_comparison() {
    let mut items = [(2_u8, 'A'), (1, 'B')];
    let original = items;
    let mut comparisons = 0;
    let mut work = RemainingWork(0);

    let result = try_sort_by_with_work(&mut items, &mut work, |left, right, _work| {
        comparisons += 1;
        Ok(left.0.cmp(&right.0))
    });

    let error = result.unwrap_err();
    assert!(matches!(
        error.kind(),
        ErrorKind::Resource(crate::error::ResourceError::SizeOverflow)
    ));
    assert_eq!(comparisons, 0);
    assert_eq!(items, original);
}

#[test]
fn synthetic_work_error_precedes_a_sort_swap() {
    let mut items = [(2_u8, 'A'), (1, 'B')];
    let original = items;
    let mut comparisons = 0;
    let mut work = RemainingWork(1);

    let result = try_sort_by_with_work(&mut items, &mut work, |left, right, _work| {
        comparisons += 1;
        Ok(left.0.cmp(&right.0))
    });

    let error = result.unwrap_err();
    assert!(matches!(
        error.kind(),
        ErrorKind::Resource(crate::error::ResourceError::SizeOverflow)
    ));
    assert_eq!(comparisons, 1);
    assert_eq!(items, original);
}

#[test]
fn fallible_sort_stops_at_the_first_comparison_error() {
    let mut items: Vec<u32> = (0..100).rev().collect();
    let mut comparisons = 0;
    let result = try_sort_by(&mut items, |left, right| {
        comparisons += 1;
        if comparisons == 40 {
            return Err(GeometryErrorKind::voronoi("stop"));
        }
        Ok(left.cmp(right))
    });
    assert!(result.is_err());
    assert_eq!(comparisons, 40);
}

#[test]
fn fallible_sort_stops_at_a_merge_error_with_a_valid_item_permutation() {
    let mut items = [
        (1_u8, 101_u16),
        (3, 102),
        (5, 103),
        (7, 104),
        (9, 105),
        (11, 106),
        (13, 107),
        (15, 108),
        (17, 109),
        (19, 110),
        (21, 111),
        (23, 112),
        (25, 113),
        (27, 114),
        (29, 115),
        (31, 116),
        (0, 201),
        (2, 202),
        (4, 203),
        (6, 204),
        (8, 205),
        (10, 206),
        (12, 207),
        (14, 208),
        (16, 209),
        (18, 210),
        (20, 211),
        (22, 212),
        (24, 213),
        (26, 214),
        (28, 215),
        (30, 216),
    ];
    let expected_records = [
        (1, 101),
        (3, 102),
        (5, 103),
        (7, 104),
        (9, 105),
        (11, 106),
        (13, 107),
        (15, 108),
        (17, 109),
        (19, 110),
        (21, 111),
        (23, 112),
        (25, 113),
        (27, 114),
        (29, 115),
        (31, 116),
        (0, 201),
        (2, 202),
        (4, 203),
        (6, 204),
        (8, 205),
        (10, 206),
        (12, 207),
        (14, 208),
        (16, 209),
        (18, 210),
        (20, 211),
        (22, 212),
        (24, 213),
        (26, 214),
        (28, 215),
        (30, 216),
    ];
    let mut observed_merge_pairs = Vec::new();
    let mut error_observed = false;
    let mut post_error_comparisons = 0;

    let result = try_sort_by(&mut items, |left, right| {
        if error_observed {
            post_error_comparisons += 1;
            return Err(GeometryErrorKind::voronoi("comparison after merge error"));
        }
        let cross_run = (101..=116).contains(&left.1) && (201..=216).contains(&right.1);
        if cross_run {
            let pair = (left.1, right.1);
            observed_merge_pairs.push(pair);
            if (left.0, left.1, right.0, right.1) == (3, 102, 2, 202) {
                error_observed = true;
                return Err(GeometryErrorKind::voronoi("merge comparator stop"));
            }
        }
        Ok(left.0.cmp(&right.0))
    });

    let error = result.expect_err("the selected cross-run comparison must fail");
    assert!(matches!(
        error.kind(),
        ErrorKind::Geometry(GeometryErrorKind::Voronoi(message))
            if message.as_ref() == "merge comparator stop"
    ));
    assert!(
        error_observed,
        "the comparator must reach the merge error pair"
    );
    assert_eq!(observed_merge_pairs, [(101, 201), (101, 202), (102, 202)]);
    assert_eq!(
        post_error_comparisons, 0,
        "the first comparator error must stop further callback calls"
    );

    assert_eq!(items.len(), expected_records.len());
    for (key, source_tag) in expected_records {
        assert_eq!(
            items
                .iter()
                .filter(|record| **record == (key, source_tag))
                .count(),
            1,
            "record ({key}, {source_tag})"
        );
    }
}
