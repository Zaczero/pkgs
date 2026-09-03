use crate::geometry::arrangement::*;
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
fn assert_bit_parity(fast: &mut Arrangement<i32>, slow: &mut Arrangement<i32>) {
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
        fast.typed.multiplicities, slow.typed.multiplicities,
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
            matches!(left.decision_area, FaceDecisionArea::Exact(_)),
            matches!(right.decision_area, FaceDecisionArea::Exact(_)),
            "face area availability diverges"
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
    let mut fast = Arrangement::from_single_loop(&segments)
        .expect("single-loop fast path must accept this loop");
    if expect_cuts {
        assert!(
            fast.vertex_count() > segments.len(),
            "fixture was meant to self-intersect"
        );
    }
    let mut slow = general(&segments);
    assert_bit_parity(&mut fast, &mut slow);
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

#[test]
fn positional_fan_order_survives_opposite_finite_extremes() {
    let extreme = 1e308;
    let points = [
        XY::new(extreme, extreme),
        XY::new(-extreme, extreme),
        XY::new(-extreme, -extreme),
        XY::new(extreme, -extreme),
        XY::new(0.0, -extreme),
    ];
    let starts = [0, 4, 6, 8, 10, 12];
    let mut targets = [1, 2, 3, 4, 0, 2, 0, 1, 0, 4, 0, 3];
    let mut multiplicities = [1_i32, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];

    assert!(order_single_loop_rows(
        &points,
        &starts,
        &mut targets,
        &mut multiplicities,
    ));
    assert_eq!(targets[..4], [1, 2, 4, 3]);
    assert_eq!(multiplicities[..4], [1, 2, 4, 3]);
}

#[test]
fn winding_faces_keep_only_exact_sign_but_ring_faces_keep_magnitude() {
    let e = 2.0_f64.powi(-27);
    let segments = loop_segments(&[(0.0, 0.0), (1.0, 1.0 + e), (1.0 - e, 1.0)]);
    assert!(segments.iter().all(|segment| {
        segment.start.x * segment.end.y - segment.end.x * segment.start.y == 0.0
    }));
    let mut winding = Arrangement::new(&segments);
    assert!(
        winding
            .faces
            .iter()
            .all(|face| matches!(face.decision_area, FaceDecisionArea::Sign(_)))
    );
    assert!(winding.faces.iter().any(|face| matches!(
        face.decision_area,
        FaceDecisionArea::Sign(AreaSign::Positive)
    )));

    let mut rings = Arrangement::new_with_rings(&segments);
    assert!(rings.faces.iter().all(|face| {
        matches!(face.decision_area, FaceDecisionArea::Exact(area) if area.magnitude().get() > 0.0)
    }));
    assert_eq!(winding.face_windings(&[0]), rings.face_windings(&[0]));
}

#[test]
fn fan_order_uses_geometry_after_a_pseudo_angle_collision() {
    let high = f64::from_bits(1.0_f64.to_bits() + 1);
    assert_eq!(
        crate::geometry::predicates::pseudo_angle(1.0, 1.0),
        crate::geometry::predicates::pseudo_angle(1.0, high),
        "adjacent diagonal rays collide after pseudo-angle rounding"
    );

    // Target 1 is geometrically above target 2, but an identifier tie-break
    // put it first before the certified angular comparator.
    let points = [
        XY::new(0.0, 0.0),
        XY::new(1.0, high),
        XY::new(1.0, 1.0),
        XY::new(-1.0, 0.0),
    ];
    let starts = [0, 3, 4, 5, 6];
    let mut targets = [1, 2, 3, 0, 0, 0];
    let mut multiplicities = [1_i32, 2, 3, 4, 5, 6];

    sort_rows_counterclockwise(&points, &starts, &mut targets, &mut multiplicities);

    assert_eq!(targets[..3], [2, 1, 3]);
    assert_eq!(multiplicities[..3], [2, 1, 3]);
}

#[test]
fn nested_arrangements_keep_the_last_dropped_outer_scratch() {
    let small = loop_segments(&[(0.0, 0.0), (2.0, 0.0), (1.0, 1.0)]);
    let large = loop_segments(&[
        (0.0, 0.0),
        (4.0, 0.0),
        (5.0, 2.0),
        (4.0, 4.0),
        (0.0, 4.0),
        (-1.0, 2.0),
    ]);
    let outer = Arrangement::new(&small);
    let outer_capacity = outer.points.capacity();
    let inner = Arrangement::new(&large);
    assert_ne!(outer.points.as_ptr(), inner.points.as_ptr());
    drop(inner);
    drop(outer);
    assert_eq!(arrangement_spares().points.capacity(), outer_capacity);
}

#[test]
fn panicking_weight_callback_returns_cleared_leases() {
    let segments = loop_segments(&[(0.0, 0.0), (2.0, 0.0), (1.0, 1.0)]);
    let result = std::panic::catch_unwind(|| {
        Arrangement::<i32>::weighted(&segments, |_| panic!("weight failed"));
    });
    assert!(result.is_err());
    assert!(arrangement_spares().points.is_empty());
    assert!(typed_spares::<i32>().edges.is_empty());
}
