use super::*;
use crate::geometry::*;

fn loop_segments(points: &[(f64, f64)]) -> Vec<Segment> {
    (0..points.len())
        .map(|index| {
            let (sx, sy) = points[index];
            let (ex, ey) = points[(index + 1) % points.len()];
            Segment {
                start: XY::new(sx, sy),
                end: XY::new(ex, ey),
            }
        })
        .collect()
}

fn general(segments: &[Segment]) -> Arrangement<i32> {
    Arrangement::new(&super::super::overlay::self_node_segments(segments))
}

/// Bit-exact equality over every derived structure the consumers read:
/// topology columns, faces (rings, areas, components), and the kept
/// region's boundary rings.
fn assert_bit_parity(fast: &Arrangement<i32>, slow: &Arrangement<i32>) {
    let bits = |points: &[XY]| -> Vec<(u64, u64)> {
        points
            .iter()
            .map(|point| (point.x.to_bits(), point.y.to_bits()))
            .collect()
    };
    assert_eq!(bits(&fast.points), bits(&slow.points), "points diverge");
    assert_eq!(fast.starts, slow.starts, "starts diverge");
    assert_eq!(fast.targets, slow.targets, "targets diverge");
    assert_eq!(fast.owners, slow.owners, "owners diverge");
    assert_eq!(
        fast.multiplicities, slow.multiplicities,
        "multiplicities diverge"
    );
    assert_eq!(fast.face_of, slow.face_of, "face_of diverges");
    assert_eq!(fast.face_starts, slow.face_starts, "face CSR diverges");
    assert_eq!(fast.face_slots, slow.face_slots, "face slots diverge");
    assert_eq!(fast.component_of, slow.component_of, "components diverge");
    assert_eq!(fast.component_count, slow.component_count);
    assert_eq!(fast.faces.len(), slow.faces.len(), "face count diverges");
    for (left, right) in std::iter::zip(&fast.faces, &slow.faces) {
        assert_eq!(bits(&left.ring), bits(&right.ring), "face ring diverges");
        assert_eq!(left.decision_area.sign(), right.decision_area.sign());
        assert_eq!(
            left.decision_area.magnitude().get().to_bits(),
            right.decision_area.magnitude().get().to_bits(),
            "face area diverges"
        );
        assert_eq!(left.component, right.component);
    }
    let windings = fast.face_windings(&[0]);
    assert_eq!(windings, slow.face_windings(&[0]), "windings diverge");
    let fast_rings = fast.region_rings(&windings, |w| w >= 1);
    let slow_rings = slow.region_rings(&windings, |w| w >= 1);
    assert_eq!(fast_rings.len(), slow_rings.len(), "ring count diverges");
    for (left, right) in std::iter::zip(&fast_rings, &slow_rings) {
        assert_eq!(bits(left), bits(right), "region ring diverges");
    }
}

fn assert_parity(points: &[(f64, f64)], expect_cuts: bool) {
    let segments = loop_segments(points);
    let fast = Arrangement::from_single_loop(&segments)
        .expect("single-loop fast path must accept this loop");
    if expect_cuts {
        assert!(
            fast.vertex_count() > segments.len(),
            "fixture was meant to self-intersect"
        );
    }
    assert_bit_parity(&fast, &general(&segments));
}

#[test]
fn simple_loop_matches_general() {
    assert_parity(&[(0.0, 0.0), (4.0, 0.0), (4.0, 3.0), (0.0, 3.0)], false);
}

#[test]
fn figure_eight_matches_general() {
    assert_parity(&[(0.0, 0.0), (2.0, 2.0), (2.0, 0.0), (0.0, 2.0)], true);
}

#[test]
fn pentagram_matches_general() {
    // Five transversal crossings, winding 2 in the core — a closed
    // {5/2} star walked vertex order 0, 2, 4, 1, 3.
    let points: Vec<(f64, f64)> = [0, 2, 4, 1, 3]
        .iter()
        .map(|&step| {
            let angle =
                std::f64::consts::FRAC_PI_2 + f64::from(step) * 2.0 * std::f64::consts::PI / 5.0;
            (angle.cos(), angle.sin())
        })
        .collect();
    assert_parity(&points, true);
}

#[test]
fn irregular_multi_cross_matches_general() {
    // Awkward coordinates: crossings at non-representable points so the
    // parametric placement (double-double solver) is exercised, plus a
    // crossing-pinch shape from a folded zigzag.
    assert_parity(
        &[
            (0.1, 0.7),
            (5.3, 0.21),
            (5.1, 3.33),
            (1.9, -1.13),
            (3.7, 4.09),
            (-0.6, 2.51),
        ],
        true,
    );
}

#[test]
fn t_junction_bails() {
    // The vertical edge ends exactly on the bottom edge's interior:
    // the pierced segment gets a one-sided cut event AND the cut key
    // collides with an original vertex — both walls catch it.
    let segments = loop_segments(&[
        (0.0, 0.0),
        (4.0, 0.0),
        (4.0, 4.0),
        (2.0, 4.0),
        (2.0, 0.0),
        (1.0, 3.0),
    ]);
    assert!(Arrangement::from_single_loop(&segments).is_none());
    // The general path stays the owner of the case.
    assert!(general(&segments).vertex_count() > 0);
}

#[test]
fn repeated_vertex_bails() {
    let segments = loop_segments(&[
        (0.0, 0.0),
        (2.0, 0.0),
        (1.0, 1.0),
        (2.0, 2.0),
        (0.0, 2.0),
        (1.0, 1.0),
    ]);
    assert!(Arrangement::from_single_loop(&segments).is_none());
}

#[test]
fn collinear_fold_bails() {
    // The closing edge retraces the bottom edge collinearly with an
    // interior endpoint — the overlaps_found exactness pre-pass case.
    let segments = loop_segments(&[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (2.0, 0.0)]);
    assert!(Arrangement::from_single_loop(&segments).is_none());
}

#[test]
fn open_chain_bails() {
    let mut segments = loop_segments(&[(0.0, 0.0), (2.0, 2.0), (2.0, 0.0), (0.0, 2.0)]);
    segments.pop();
    assert!(Arrangement::from_single_loop(&segments).is_none());
    assert!(Arrangement::from_single_loop(&[]).is_none());
}

/// Two positional loops with weight `1` each, against the general
/// arrangement over their joint atomic linework: bit-exact topology.
fn two_loop(a: &[(f64, f64)], b: &[(f64, f64)]) -> (Vec<Segment>, Vec<(u32, u32)>) {
    let mut segments = loop_segments(a);
    let split = segments.len() as u32;
    segments.extend(loop_segments(b));
    let total = segments.len() as u32;
    (segments, vec![(0, split), (split, total)])
}

#[test]
fn from_loops_matches_general_when_clean() {
    // Disjoint, crossing, and one-inside-other loop pairs build
    // positionally and node to the SAME vertex/face counts as the
    // general arrangement. (Internal vertex ORDER legitimately
    // differs — per-loop vs atomic-chain id assignment — so this
    // pins the structural invariants, not the bit layout
    // `assert_bit_parity` pins for the connected single-loop case;
    // full overlay-output parity is covered by the 300-fixture
    // differential in tests/.)
    for (a, b) in [
        (vec![(0.0, 0.0), (2.0, 0.0), (2.0, 2.0), (0.0, 2.0)], vec![
            (5.0, 5.0),
            (7.0, 5.0),
            (7.0, 7.0),
            (5.0, 7.0),
        ]),
        (vec![(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0)], vec![
            (2.0, 2.0),
            (6.0, 2.0),
            (6.0, 6.0),
            (2.0, 6.0),
        ]),
        (
            vec![(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0)],
            vec![(3.0, 3.0), (7.0, 3.0), (7.0, 7.0), (3.0, 7.0)],
        ),
    ] {
        let (segments, ranges) = two_loop(&a, &b);
        let fast = Arrangement::<i32>::from_loops(&segments, &ranges, |_| 1)
            .expect("clean loop pair builds positionally");
        let slow = general(&segments);
        assert_eq!(
            fast.vertex_count(),
            slow.vertex_count(),
            "vertex count diverges"
        );
        assert_eq!(fast.faces.len(), slow.faces.len(), "face count diverges");
        assert_eq!(
            fast.component_count, slow.component_count,
            "component count diverges"
        );
    }
}

#[test]
fn from_loops_bails_on_shared_vertex() {
    // Two loops touching at one shared vertex — positional identity
    // cannot express the merge; the general path owns it.
    let (segments, ranges) = two_loop(&[(0.0, 0.0), (2.0, 0.0), (1.0, 2.0)], &[
        (1.0, 2.0),
        (3.0, 2.0),
        (2.0, 4.0),
    ]);
    assert!(Arrangement::<i32>::from_loops(&segments, &ranges, |_| 1).is_none());
}
