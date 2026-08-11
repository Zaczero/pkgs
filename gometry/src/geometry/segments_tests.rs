use crate::geometry::segments::{EXACT_SELECTION_FALLBACKS, segment_nearest_points};
use crate::geometry::*;

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
    let (same_direction, span) =
        shared_segment_part(left, right).expect("positive-length shared run");
    assert!(same_direction);
    assert_eq!(span, vec![XY::new(1.0e13 - 1.0, 0.0), XY::new(1.0e13, 0.0)]);
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
        let (same_direction, _) =
            shared_segment_part(segment, segment).expect("positive-length shared run");
        assert!(
            same_direction,
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
    let (on_horizontal, on_stub) = segment_nearest_points(horizontal, stub);
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
    let (left, right) = segment_nearest_points(a, b);
    assert_eq!(left, XY::new(1.0, 1.0));
    assert_eq!(left, right);
}

/// The split-product crossing solver must keep the robust predicate's
/// near-parallel crossing and place it symmetrically, without relying on a
/// target-specific fused multiply-add path.
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
    let point = segment_cross_point(left, right).expect("certified crossing");
    assert_eq!(
        point,
        segment_cross_point(right, left).expect("symmetric crossing")
    );
    assert_eq!(point.x.to_bits(), 0.5_f64.to_bits());
    assert_eq!(point.y.to_bits(), 5.0e-14_f64.to_bits());
}

/// Products at this scale overflow without the shared power-of-two frame.
/// Existence and placement must use the same normalized coordinates and
/// remain operand/direction invariant after replacing FMA with Dekker
/// splitting.
#[test]
fn segment_cross_point_rescales_extreme_finite_coordinates() {
    let scale = 2.0_f64.powi(600);
    let left = Segment {
        start: XY::new(scale, scale),
        end: XY::new(2.0 * scale, 2.0 * scale),
    };
    let right = Segment {
        start: XY::new(scale, 2.0 * scale),
        end: XY::new(2.0 * scale, scale),
    };
    let point = segment_cross_point(left, right).expect("scaled crossing");
    assert!(point.x.is_finite() && point.y.is_finite());
    for (a, b) in [
        (right, left),
        (reverse_segment(left), right),
        (left, reverse_segment(right)),
        (reverse_segment(right), reverse_segment(left)),
    ] {
        assert_eq!(point, segment_cross_point(a, b).expect("same crossing"));
    }
    assert_eq!(point.x.to_bits(), (1.5 * scale).to_bits());
    assert_eq!(point.x.to_bits(), point.y.to_bits());
    assert!(point_on_segment(point, left.start, left.end));
    assert!(point_on_segment(point, right.start, right.end));
}

/// Determinants below the ordinary-product underflow boundary still describe
/// a real crossing. Normalization must scale them UP before both orient2d and
/// double-double placement; otherwise the predicate and constructor diverge.
#[test]
fn segment_cross_point_rescales_subnormal_products() {
    let scale = 1.0e-180;
    let left = Segment {
        start: XY::new(0.0, 0.0),
        end: XY::new(scale, 2.0 * scale),
    };
    let right = Segment {
        start: XY::new(0.25 * scale, -scale),
        end: XY::new(0.25 * scale, 3.0 * scale),
    };
    let point = segment_cross_point(left, right).expect("scaled-up crossing");
    for (a, b) in [
        (right, left),
        (reverse_segment(left), right),
        (left, reverse_segment(right)),
        (reverse_segment(right), reverse_segment(left)),
    ] {
        assert_eq!(point, segment_cross_point(a, b).expect("same crossing"));
    }
    assert_eq!(point.x.to_bits(), (0.25 * scale).to_bits());
    assert_eq!(point.y.to_bits(), (0.5 * scale).to_bits());
    assert!(point_on_segment(point, left.start, left.end));
    assert!(point_on_segment(point, right.start, right.end));
}

/// Per-axis normalization preserves a tiny vertical span crossing a huge
/// horizontal span. A single scale chosen from the largest coordinate would
/// collapse the vertical segment to zero before orient2d saw it.
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
    let point = segment_cross_point(horizontal, vertical).expect("mixed-scale crossing");
    assert_eq!(point.x.to_bits(), 1.0e-300_f64.to_bits());
    assert_eq!(point.y.to_bits(), 0.0_f64.to_bits());
    assert!(point_on_segment(point, horizontal.start, horizontal.end));
    assert!(point_on_segment(point, vertical.start, vertical.end));
    assert_eq!(
        point,
        segment_cross_point(reverse_segment(vertical), reverse_segment(horizontal))
            .expect("same crossing")
    );
}

/// A long diagonal crossing a tiny diagonal close to its start has an exact
/// witness, but the long segment's parametric fraction is below binary64's
/// representable range. Raw mixed-exponent products and the better-conditioned
/// short segment must determine placement; no axis snap can hide a failure.
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
    let point = segment_cross_point(long, short).expect("mixed-scale diagonal crossing");
    assert_eq!(point, XY::new(expected, expected));
    assert!(point_on_segment(point, long.start, long.end));
    assert!(point_on_segment(point, short.start, short.end));
    assert_eq!(
        point,
        segment_cross_point(reverse_segment(short), reverse_segment(long)).expect("same crossing")
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
            .rounded_trustworthy()
            .expect("ordinary point-pair square rounds normally")
            .to_bits(),
        point_pair.filter.hi.to_bits(),
    );

    // Generated independently with long-double screening, then adjudicated
    // with Python Fraction from these stored doubles. The correctly rounded
    // exact square is ...a57. Replacing gamma(4) with gamma(1) raises the
    // lower endpoint to ...a58, so this is a non-vacuous bound mutation.
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
        let key = point_segment_distance_key(point, segment);
        assert!(key.is_bounded(), "index={index}");
        assert!(key.contains_exact(), "index={index}");
    }
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
                .cmp(&point_segment_distance_key(probe, far))
                .is_lt()
        );
    }
    let entries = EXACT_SELECTION_FALLBACKS.with(std::cell::Cell::get);
    eprintln!("ordinary exact-selection fallback entries: {entries}");
    assert_eq!(entries, 0);
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
            });
            if best
                .as_ref()
                .is_none_or(|incumbent| key.cmp(incumbent).is_lt())
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
    let probe = Shape::Point(Point::new_unchecked_xy(7.25, 2.75));
    EXACT_SELECTION_FALLBACKS.with(|count| count.set(0));
    for row in 0..128 {
        let points = (0..17)
            .map(|column| {
                Point::new_unchecked_xy(
                    f64::from(column),
                    (f64::from(column) * 0.37 + f64::from(row) * 0.03125).sin()
                        + f64::from(row) * 0.002,
                )
            })
            .collect::<Vec<_>>();
        let line = Shape::LineString(
            LineSeq::try_new(CoordSeq::from(points)).expect("ordinary test line is valid"),
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
    let interior = point_segment_distance_key(point, segment);
    let endpoint = point_pair_distance_key(point, segment.start);
    assert!(interior.is_bounded());
    assert!(interior.contains_exact());
    assert!(interior.cmp(&endpoint).is_lt());
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
    let witness = segment_projection(point, segment).interpolate_xy(segment);
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
    let reversed_witness = segment_projection(point, reversed).interpolate_xy(reversed);
    assert_eq!(
        reversed_witness,
        XY::new(
            f64::from_bits(0x2010_5D33_A16A_A5D2),
            f64::from_bits(0x1FFC_ABE0_86EE_B4F9),
        ),
    );
    assert!(
        point_segment_distance_key(point, segment)
            .cmp(&point_segment_distance_key(point, reversed))
            .is_lt(),
        "selection must compare the two emitted coordinates, not identify reversed supports",
    );
}

#[test]
fn exact_selection_compares_the_emitted_foot_against_a_point_candidate() {
    let segment = Segment {
        start: XY::new(-2.504_967_266_659_778_4, -0.228_559_855_772_118_68),
        end: XY::new(-2.640_447_616_310_041, 2.361_006_325_545_838),
    };
    let probe = XY::new(-2.506_180_295_312_065, -0.186_351_125_729_832_03);
    let point_candidate = XY::new(-2.506_533_121_444_889_4, -0.185_421_981_330_493_8);
    let emitted = segment_projection(probe, segment).interpolate_xy(segment);
    assert_eq!(
        emitted,
        XY::new(-2.507_172_816_964_488, -0.186_403_052_257_709_84),
    );
    assert!(
        point_pair_distance_key(probe, point_candidate)
            .cmp(&point_segment_distance_key(probe, segment))
            .is_lt(),
        "the rejected point is exactly closer than the emitted line foot",
    );
}

fn reverse_segment(segment: Segment) -> Segment {
    Segment {
        start: segment.end,
        end: segment.start,
    }
}
