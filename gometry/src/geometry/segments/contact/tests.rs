use super::super::test_support::reverse_segment;
use super::*;

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
    use crate::geometry::numerics::product::exact_power_of_two;
    let scale = exact_power_of_two(600);
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
    use crate::geometry::numerics::product::exact_power_of_two;
    let huge = exact_power_of_two(1000);
    let tiny = exact_power_of_two(-999);
    let long = Segment {
        start: XY::new(0.0, 0.0),
        end: XY::new(huge, huge),
    };
    let short = Segment {
        start: XY::new(0.0, tiny),
        end: XY::new(tiny, 0.0),
    };
    let expected = exact_power_of_two(-1000);
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
