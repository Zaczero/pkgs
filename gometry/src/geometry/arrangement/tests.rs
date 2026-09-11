use crate::geometry::arrangement::*;
use crate::geometry::{predicates, *};

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
    Arrangement::new(&super::super::overlay::self_node_segments(segments).unwrap())
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
        assert_eq!(left.area_sign, right.area_sign);
        assert_eq!(left.component, right.component);
    }
    let windings = fast.face_windings(&[0]);
    assert_eq!(windings, slow.face_windings(&[0]), "windings diverge");
    let fast_rings = fast.region_rings(&windings, |w| w != 0);
    let slow_rings = slow.region_rings(&windings, |w| w != 0);
    assert_eq!(fast_rings.len(), slow_rings.len(), "ring count diverges");
    for (left, right) in std::iter::zip(&fast_rings, &slow_rings) {
        assert_eq!(bits(left), bits(right), "region ring diverges");
    }
}

fn assert_parity(points: &[(f64, f64)], expect_cuts: bool) {
    let segments = loop_segments(points);
    let mut fast = Arrangement::from_single_loop(&segments)
        .unwrap()
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
fn pentagram_multi_cut_fast_path_matches_general_noder() {
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
    let segments = loop_segments(&points);
    let (cuts, _) = single_loop_cuts(&segments)
        .unwrap()
        .expect("pentagram is a positional loop");
    assert!(
        cuts.iter().filter(|cut| cut.ordinal == 0).count() >= 2,
        "the first edge must exercise per-segment cut ordering",
    );
    let mut fast = Arrangement::from_single_loop(&segments)
        .unwrap()
        .expect("pentagram fast path must accept");
    let mut slow = general(&segments);
    assert_bit_parity(&mut fast, &mut slow);
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
    assert!(Arrangement::from_single_loop(&segments).unwrap().is_none());
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
    assert!(Arrangement::from_single_loop(&segments).unwrap().is_none());
}

#[test]
fn collinear_fold_bails() {
    // The closing edge retraces the bottom edge collinearly with an
    // interior endpoint — the overlaps_found exactness pre-pass case.
    let segments = loop_segments(&[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (2.0, 0.0)]);
    assert!(Arrangement::from_single_loop(&segments).unwrap().is_none());
}

#[test]
fn open_chain_bails() {
    let mut segments = loop_segments(&[(0.0, 0.0), (2.0, 2.0), (2.0, 0.0), (0.0, 2.0)]);
    segments.pop();
    assert!(Arrangement::from_single_loop(&segments).unwrap().is_none());
    assert!(Arrangement::from_single_loop(&[]).unwrap().is_none());
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
            .unwrap()
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
    assert!(
        Arrangement::<i32>::from_loops(&segments, &ranges, |_| 1)
            .unwrap()
            .is_none()
    );
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
    let mut edge_of_slot = [0, 1, 2, 3, 4, 5, 0, 1, 2, 3, 4, 5];

    assert!(order_single_loop_rows(
        &points,
        &starts,
        &mut targets,
        &mut multiplicities,
        &mut edge_of_slot,
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
    assert!(winding.faces.iter().all(|face| face.ring.is_empty()));
    assert!(
        winding
            .faces
            .iter()
            .any(|face| face.area_sign == AreaSign::Positive)
    );

    let mut rings = Arrangement::new_with_rings(&segments);
    assert!(
        rings
            .faces
            .iter()
            .all(|face| { open_xy_cycle_decision(&face.ring).magnitude().get() > 0.0 })
    );
    assert_eq!(winding.face_windings(&[0]), rings.face_windings(&[0]));
}

#[test]
fn fan_order_uses_geometry_after_a_pseudo_angle_collision() {
    let high = f64::from_bits(1.0_f64.to_bits() + 1);
    assert_eq!(
        predicates::pseudo_angle(1.0, 1.0),
        predicates::pseudo_angle(1.0, high),
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
    let mut edge_of_slot = [0, 1, 2, 0, 1, 2];

    sort_rows_counterclockwise(
        &points,
        &starts,
        &mut targets,
        &mut multiplicities,
        &mut edge_of_slot,
    );

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

/// Connected square + full chord + hole + bridge. Occupancy from a reversible
/// DFS on the real arrangement dual; points classified by actual face rings.
#[test]
fn connected_chord_bridge_dfs_undo_keeps_south_body() {
    let shell = [
        XY::new(0.0, 0.0),
        XY::new(6.0, 0.0),
        XY::new(6.0, 6.0),
        XY::new(0.0, 6.0),
        XY::new(0.0, 0.0),
    ];
    let hole = [
        XY::new(2.0, 4.0),
        XY::new(4.0, 4.0),
        XY::new(4.0, 5.0),
        XY::new(2.0, 5.0),
        XY::new(2.0, 4.0),
    ];
    let mut raw: Vec<(Segment, [i32; 2])> = Vec::new();
    for window in shell.windows(2) {
        raw.push((
            Segment {
                start: window[0],
                end: window[1],
            },
            [1, 0],
        ));
    }
    for window in hole.windows(2) {
        raw.push((
            Segment {
                start: window[0],
                end: window[1],
            },
            [0, 1],
        ));
    }
    raw.push((
        Segment {
            start: XY::new(0.0, 3.0),
            end: XY::new(6.0, 3.0),
        },
        [0, 0],
    ));
    raw.push((
        Segment {
            start: XY::new(2.0, 3.0),
            end: XY::new(2.0, 4.0),
        },
        [0, 0],
    ));
    let sources: Vec<Segment> = raw.iter().map(|entry| entry.0).collect();
    let (atomic, provenance) =
        crate::geometry::overlay::self_node_segments_sourced(sources).expect("noded sources");
    let arrangement = Arrangement::<[i32; 2]>::build::<true>(&atomic, |piece| {
        let mut weight = [0, 0];
        for &(ordinal, reversed) in provenance.owners(piece) {
            let unit = raw[ordinal as usize].1;
            if reversed {
                weight[0] -= unit[0];
                weight[1] -= unit[1];
            } else {
                weight[0] += unit[0];
                weight[1] += unit[1];
            }
        }
        weight
    });
    assert_eq!(
        arrangement.component_count, 1,
        "chord and bridge must yield one vertex component, got {}",
        arrangement.component_count
    );
    assert!(
        arrangement.faces.iter().all(|face| !face.ring.is_empty()),
        "need materialized face rings to locate queries"
    );

    let unbounded = unbounded_face_predecessor_of_minus_x(&arrangement);
    let adj = dual_adjacency(&arrangement);
    let undo = dfs_occupancy(&adj, unbounded, true);
    let leak = dfs_occupancy(&adj, unbounded, false);

    let body = XY::new(3.0, 1.0);
    let in_hole = XY::new(3.0, 4.5);
    let body_face = locate_face(&arrangement, unbounded, body);
    let hole_face = locate_face(&arrangement, unbounded, in_hole);
    assert_ne!(
        body_face, unbounded,
        "body point must sit in a bounded face"
    );
    assert_ne!(
        hole_face, unbounded,
        "hole point must sit in a bounded face"
    );
    assert_ne!(body_face, hole_face);

    let ray_body = ring_occupied(&shell, &hole, body);
    let ray_hole = ring_occupied(&shell, &hole, in_hole);
    assert!(ray_body, "independent ring winding: south body occupied");
    assert!(
        !ray_hole,
        "independent ring winding: hole not polygon-occupied"
    );

    assert!(
        occupied(undo[body_face]),
        "undo DFS must occupy south body; winding={:?}",
        undo[body_face]
    );
    assert!(
        !occupied(undo[hole_face]),
        "undo DFS must leave hole unoccupied; winding={:?}",
        undo[hole_face]
    );
    assert!(
        !occupied(leak[body_face]),
        "omit-undo must leak hole winding onto south body; winding={:?}",
        leak[body_face]
    );
}

fn occupied(winding: [i32; 2]) -> bool {
    winding[0] != 0 && winding[1] == 0
}

fn ring_occupied(shell: &[XY], hole: &[XY], point: XY) -> bool {
    winding_of(shell, point) != 0 && winding_of(hole, point) == 0
}

fn winding_of(ring: &[XY], point: XY) -> i32 {
    let mut winding = 0;
    for window in ring.windows(2) {
        let (start, end) = (window[0], window[1]);
        if (start.y > point.y) != (end.y > point.y)
            && crate::geometry::ray_crossing_is_right(
                start.x, start.y, end.x, end.y, point.x, point.y,
            )
        {
            winding += if end.y > start.y { 1 } else { -1 };
        }
    }
    winding
}

/// Left face of the CCW-fan slot whose wedge contains −x at the
/// lexicographic-minimum vertex (`compare_departures` order, wrap last→first).
fn unbounded_face_predecessor_of_minus_x(arrangement: &Arrangement<[i32; 2]>) -> usize {
    let (vertex, origin) = arrangement
        .points
        .iter()
        .copied()
        .enumerate()
        .min_by(|(_, left), (_, right)| left.x.total_cmp(&right.x).then(left.y.total_cmp(&right.y)))
        .expect("arrangement has vertices");
    let range = arrangement.starts[vertex] as usize..arrangement.starts[vertex + 1] as usize;
    let mut outgoing: Vec<usize> = range.collect();
    outgoing.sort_by(|&left, &right| {
        compare_departures(
            origin,
            arrangement.points[arrangement.targets[left] as usize],
            arrangement.targets[left],
            arrangement.points[arrangement.targets[right] as usize],
            arrangement.targets[right],
        )
    });
    let minus_x = XY::new(origin.x - 1.0, origin.y);
    let insert = outgoing.iter().position(|&slot| {
        compare_departures(
            origin,
            minus_x,
            u32::MAX,
            arrangement.points[arrangement.targets[slot] as usize],
            arrangement.targets[slot],
        ) == std::cmp::Ordering::Less
    });
    let predecessor = match insert {
        Some(0) | None => *outgoing.last().expect("degree ≥ 1"),
        Some(index) => outgoing[index - 1],
    };
    arrangement.face_of[predecessor] as usize
}

fn dual_adjacency(arrangement: &Arrangement<[i32; 2]>) -> Vec<Vec<(usize, [i32; 2])>> {
    let mut adj = vec![Vec::new(); arrangement.faces.len()];
    for slot in 0..arrangement.targets.len() {
        let twin = arrangement.twin_slot(slot);
        if slot > twin {
            continue;
        }
        let left = arrangement.face_of[slot] as usize;
        let right = arrangement.face_of[twin] as usize;
        if left == right {
            continue;
        }
        let multiplicity = arrangement.typed.multiplicities[slot];
        // L→R subtracts m; R→L adds m.
        adj[left].push((right, [-multiplicity[0], -multiplicity[1]]));
        adj[right].push((left, multiplicity));
    }
    let touches_hole: Vec<bool> = adj
        .iter()
        .map(|neighbors| neighbors.iter().any(|&(_, d)| d[1] != 0))
        .collect();
    for neighbors in &mut adj {
        neighbors.sort_by_key(|&(neighbor, delta)| {
            (!touches_hole[neighbor], -(delta[1].abs()), neighbor)
        });
    }
    adj
}

fn dfs_occupancy(adj: &[Vec<(usize, [i32; 2])>], start: usize, undo: bool) -> Vec<[i32; 2]> {
    let mut winding = [0, 0];
    let mut seen = vec![None; adj.len()];
    seen[start] = Some([0, 0]);
    let mut stack: Vec<(usize, usize, Option<[i32; 2]>)> = vec![(start, 0, None)];
    while let Some((face, next, incoming)) = stack.pop() {
        if next == adj[face].len() {
            if undo && let Some(delta) = incoming {
                winding[0] -= delta[0];
                winding[1] -= delta[1];
            }
            continue;
        }
        stack.push((face, next + 1, incoming));
        let (neighbor, delta) = adj[face][next];
        if seen[neighbor].is_some() {
            continue;
        }
        winding[0] += delta[0];
        winding[1] += delta[1];
        seen[neighbor] = Some(winding);
        stack.push((neighbor, 0, undo.then_some(delta)));
    }
    seen.into_iter()
        .map(|entry| entry.expect("DFS reaches every face of the component"))
        .collect()
}

fn locate_face(arrangement: &Arrangement<[i32; 2]>, unbounded: usize, point: XY) -> usize {
    let mut best = None;
    for (face, data) in arrangement.faces.iter().enumerate() {
        if face == unbounded || data.ring.len() < 4 {
            continue;
        }
        let ring = CoordSeq::try_from_xy(&data.ring).expect("face ring coordinates");
        if ring_classify_point(&ring, point.point()) != RingClass::Interior {
            continue;
        }
        let xs = data.ring.iter().map(|vertex| vertex.x);
        let ys = data.ring.iter().map(|vertex| vertex.y);
        let (minx, maxx) = (
            xs.clone().fold(f64::INFINITY, f64::min),
            xs.fold(f64::NEG_INFINITY, f64::max),
        );
        let (miny, maxy) = (
            ys.clone().fold(f64::INFINITY, f64::min),
            ys.fold(f64::NEG_INFINITY, f64::max),
        );
        let area = (maxx - minx) * (maxy - miny);
        if best.is_none_or(|(_, previous)| area < previous) {
            best = Some((face, area));
        }
    }
    best.map(|(face, _)| face)
        .expect("query lies in a bounded face ring")
}
