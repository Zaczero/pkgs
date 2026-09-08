use crate::geometry::segments::{
    ClipVerdict, EXACT_SELECTION_FALLBACKS, clip_segment, clip_segment_authority,
    clip_segment_liang_barsky, compare_along_segment, segment_nearest_points,
};
use crate::geometry::*;

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

fn nearest_rect_edge(point: XY, rect: Bounds) -> usize {
    [
        (point.x - rect.minx()).abs(),
        (point.x - rect.maxx()).abs(),
        (point.y - rect.miny()).abs(),
        (point.y - rect.maxy()).abs(),
    ]
    .into_iter()
    .enumerate()
    .min_by(|left, right| left.1.total_cmp(&right.1))
    .expect("rectangle has four edges")
    .0
}

fn exact_rect_edge(point: XY, rect: Bounds) -> Option<usize> {
    if same_topological_coordinate(point.x, rect.minx()) {
        Some(0)
    } else if same_topological_coordinate(point.x, rect.maxx()) {
        Some(1)
    } else if same_topological_coordinate(point.y, rect.miny()) {
        Some(2)
    } else if same_topological_coordinate(point.y, rect.maxy()) {
        Some(3)
    } else {
        None
    }
}

fn assert_liang_authority_agrees(name: &str, start: XY, end: XY, rect: Bounds) {
    let fast = clip_segment_liang_barsky(start, end, rect);
    let authority = clip_segment_authority(start, end, rect);
    match fast {
        ClipVerdict::Declined => {
            assert_eq!(clip_segment(start, end, rect).unwrap(), authority, "{name}");
        },
        decided => {
            let fast = decided.into_clip();
            assert_eq!(fast.is_some(), authority.is_some(), "{name}");
            assert_eq!(clip_segment(start, end, rect).unwrap(), fast, "{name}");
            let (Some((fast_start, fast_end)), Some((authority_start, authority_end))) =
                (fast, authority)
            else {
                return;
            };
            for (fast_point, authority_point) in
                [(fast_start, authority_start), (fast_end, authority_end)]
            {
                if let Some(edge) = exact_rect_edge(fast_point, rect) {
                    assert_eq!(nearest_rect_edge(authority_point, rect), edge, "{name}");
                } else {
                    assert!(
                        fast_point.x > rect.minx() && fast_point.x < rect.maxx(),
                        "{name}: admitted free-axis point escaped X bounds: {fast_point:?}"
                    );
                    assert!(
                        fast_point.y > rect.miny() && fast_point.y < rect.maxy(),
                        "{name}: admitted free-axis point escaped Y bounds: {fast_point:?}"
                    );
                    assert_eq!(
                        nearest_rect_edge(authority_point, rect),
                        nearest_rect_edge(fast_point, rect),
                        "{name}"
                    );
                }
            }
        },
    }
}

#[test]
fn liang_barsky_and_exact_authorities_preserve_structural_clip_results() {
    let rect = Bounds::new(0.0, 0.0, 1.0, 1.0).expect("valid unit rectangle");
    let cases = [
        (
            "ordinary rising crossing",
            XY::new(-1.0, 0.1),
            XY::new(2.0, 0.9),
            rect,
        ),
        (
            "ordinary falling crossing",
            XY::new(2.0, 0.9),
            XY::new(-1.0, 0.1),
            rect,
        ),
        (
            "quarter-slope crossing",
            XY::new(-1.0, 0.25),
            XY::new(2.0, 0.75),
            rect,
        ),
        (
            "reverse quarter-slope crossing",
            XY::new(-1.0, 0.75),
            XY::new(2.0, 0.25),
            rect,
        ),
        // Near-parallel to the lower edge, but with ample free-axis margin.
        (
            "near-parallel crossing",
            XY::new(-1.0, 0.5),
            XY::new(2.0, 0.500_000_000_001),
            rect,
        ),
        (
            "large offset crossing",
            XY::new(1.0e15 - 32.0, 1.0e15 + 4.0),
            XY::new(1.0e15 + 32.0, 1.0e15 + 12.0),
            Bounds::new(1.0e15, 1.0e15, 1.0e15 + 16.0, 1.0e15 + 16.0)
                .expect("valid offset rectangle"),
        ),
        (
            "zero lower-edge ratio",
            XY::new(-1.0e15, 0.5),
            XY::new(1.0e15, 0.5),
            rect,
        ),
        (
            "subnormal horizontal delta",
            XY::new(0.0, 0.0),
            XY::new(f64::from_bits(1), 1.0),
            rect,
        ),
        (
            "opposite maximum endpoints",
            XY::new(-f64::MAX, -1.0),
            XY::new(f64::MAX, 1.0),
            rect,
        ),
        (
            "corner endpoint",
            XY::new(-1.0, -1.0),
            XY::new(0.0, 0.0),
            rect,
        ),
        (
            "signed-zero endpoint",
            XY::new(-1.0, -0.0),
            XY::new(2.0, 0.5),
            rect,
        ),
        (
            "tiny rectangle",
            XY::new(-1.0e-200, 0.0),
            XY::new(2.0e-200, 1.0e-200),
            Bounds::new(0.0, 0.0, 1.0e-200, 1.0e-200).expect("valid tiny rectangle"),
        ),
        (
            "huge offset rectangle",
            XY::new(1.0e20 - 2.0e16, 1.0e20 + 4.0e15),
            XY::new(1.0e20 + 2.0e16, 1.0e20 + 1.2e16),
            Bounds::new(1.0e20, 1.0e20, 1.0e20 + 2.0e16, 1.0e20 + 2.0e16)
                .expect("valid huge offset rectangle"),
        ),
    ];
    for (name, start, end, rect) in cases {
        assert_liang_authority_agrees(name, start, end, rect);
    }
}

#[test]
fn exact_clip_rounds_thin_boundary_crossings_in_source_order() {
    let rect = Bounds::new(0.0, 0.0, 1.0, 1.0).expect("valid unit rectangle");
    let cases = [
        (XY::new(-1.0e15, 0.0), XY::new(1.0e15, 1.0e-20), [
            XY::new(0.0, f64::from_bits(0x3BB7_9CA1_0C92_4223)),
            XY::new(1.0, f64::from_bits(0x3BB7_9CA1_0C92_422A)),
        ]),
        (XY::new(1.0e15, 1.0e-20), XY::new(-1.0e15, 0.0), [
            XY::new(1.0, f64::from_bits(0x3BB7_9CA1_0C92_422A)),
            XY::new(0.0, f64::from_bits(0x3BB7_9CA1_0C92_4223)),
        ]),
        (XY::new(-1.0e15, 0.0), XY::new(1.0e15, 1.0e-19), [
            XY::new(0.0, f64::from_bits(0x3BED_83C9_4FB6_D2AC)),
            XY::new(1.0, f64::from_bits(0x3BED_83C9_4FB6_D2B4)),
        ]),
    ];
    for (start, end, expected) in cases {
        let actual = clip_segment(start, end, rect)
            .expect("exact clip placement")
            .expect("positive-length clip");
        assert_eq!(actual.0, expected[0]);
        assert_eq!(actual.1, expected[1]);
    }
}

#[test]
fn exact_clip_rounds_mixed_exponent_boundary_crossings() {
    let tiny = 2.0_f64.powi(-601);
    let rect = Bounds::new(0.0, 0.0, 1.0, 2.0_f64.powi(600)).expect("valid rectangle");
    let actual = clip_segment(
        XY::new(2.0_f64.powi(-600), -tiny),
        XY::new(-2.0_f64.powi(500), 2.0_f64.powi(500)),
        rect,
    )
    .expect("exact clip placement")
    .expect("positive-length clip");
    assert_eq!(actual.0, XY::new(tiny, 0.0));
    assert_eq!(actual.1, XY::new(0.0, tiny));

    let unit = Bounds::new(0.0, 0.0, 1.0, 1.0).expect("valid unit rectangle");
    let actual = clip_segment(
        XY::new(f64::from_bits(1), 2.0_f64.powi(-500)),
        XY::new(-2.0_f64.powi(500), -2.0_f64.powi(1023)),
        unit,
    )
    .expect("exact clip placement")
    .expect("positive-length clip");
    assert_eq!(actual.0, XY::new(f64::from_bits(1), 2.0_f64.powi(-500)));
    assert_eq!(
        actual.1,
        XY::new(0.0, f64::from_bits(0x20AF_FFFF_FFFF_FFFC))
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

/// Placement rounds once. The parametric solve has three roundings available
/// to it — the fraction, the product, and the sum — and a crossing whose exact
/// ordinates are representable must survive all of them. Interpolating from a
/// binary64 fraction placed this pair at `(-0.333_333_333_333_333_37,
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
    let point = segment_cross_point(left, right).expect("certified crossing");
    assert_eq!(
        point,
        segment_cross_point(right, left).expect("symmetric crossing")
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
        let point = segment_cross_point(diagonal, edge).expect("certified crossing");
        assert_eq!(point.x.to_bits(), expected.x.to_bits(), "{name}: x");
        assert_eq!(point.y.to_bits(), expected.y.to_bits(), "{name}: y");
    }
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
        let key = point_segment_distance_key(point, segment).unwrap();
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
    let emitted = segment_projection(probe, segment)
        .unwrap()
        .interpolate_xy(segment)
        .unwrap();
    assert_eq!(
        emitted,
        XY::new(-2.507_172_816_964_488, -0.186_403_052_257_709_84),
    );
    assert!(
        point_pair_distance_key(probe, point_candidate)
            .cmp(&point_segment_distance_key(probe, segment).unwrap())
            .unwrap()
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

#[cfg(test)]
mod exact_projection_allocation_tests {
    use super::*;
    use crate::error::{ErrorKind, ResourceError};
    use crate::test_support::{allocation_count, fail_allocations_after};

    fn assert_allocation(error: &crate::error::Error) {
        assert!(matches!(
            error.kind(),
            ErrorKind::Resource(ResourceError::Allocation)
        ));
    }

    fn prefix_sweep(run: impl Fn() -> crate::error::Result<()>) {
        let (_, allocations) = allocation_count(|| run().unwrap());
        assert!(allocations > 0);
        for allowed in 0..allocations {
            let error = fail_allocations_after(allowed, || run())
                .expect_err("exact projection scratch must propagate allocation failure");
            assert_allocation(&error);
        }
    }

    #[test]
    fn endpoint_exact_classification_rejects_every_allocation() {
        let segment = Segment {
            start: XY::new(0.0, 0.0),
            end: XY::new(1.0, 0.0),
        };
        let point = XY::new(0.0, 0.0);
        prefix_sweep(|| super::classify_segment_projection_exact(point, segment).map(|_| ()));
    }

    #[test]
    fn interior_exact_terms_reject_every_allocation() {
        let segment = Segment {
            start: XY::new(0.0, 0.0),
            end: XY::new(2.0, 0.0),
        };
        let point = XY::new(1.0, 1.0);
        prefix_sweep(|| super::exact_projection_terms(point, segment).map(|_| ()));
    }

    #[test]
    fn extreme_exact_projection_rejects_every_allocation() {
        let segment = Segment {
            start: XY::new(1e200, 0.0),
            end: XY::new(1e200 + 1.0, 0.0),
        };
        let point = XY::new(1e200 + 0.5, 1.0);
        prefix_sweep(|| segment_projection(point, segment).map(|_| ()));
    }
}
