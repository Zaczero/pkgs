use super::*;

const INDEXED_EAR_NEIGHBOR_RING: [Point; 68] = [
    Point::new_raw_xy(-3.0, 1.0),
    Point::new_raw_xy(0.0, 0.0),
    Point::new_raw_xy(2.0, -1.0),
    Point::new_raw_xy(4.0, 0.0),
    Point::new_raw_xy(7.0, 1.0),
    Point::new_raw_xy(7.0, 2.0),
    Point::new_raw_xy(7.0, 3.0),
    Point::new_raw_xy(7.0, 4.0),
    Point::new_raw_xy(7.0, 5.0),
    Point::new_raw_xy(7.0, 6.0),
    Point::new_raw_xy(7.0, 7.0),
    Point::new_raw_xy(7.0, 8.0),
    Point::new_raw_xy(7.0, 9.0),
    Point::new_raw_xy(7.0, 10.0),
    Point::new_raw_xy(7.0, 11.0),
    Point::new_raw_xy(7.0, 12.0),
    Point::new_raw_xy(7.0, 13.0),
    Point::new_raw_xy(7.0, 14.0),
    Point::new_raw_xy(7.0, 15.0),
    Point::new_raw_xy(7.0, 16.0),
    Point::new_raw_xy(7.0, 17.0),
    Point::new_raw_xy(7.0, 18.0),
    Point::new_raw_xy(7.0, 19.0),
    Point::new_raw_xy(7.0, 20.0),
    Point::new_raw_xy(7.0, 21.0),
    Point::new_raw_xy(7.0, 22.0),
    Point::new_raw_xy(7.0, 23.0),
    Point::new_raw_xy(7.0, 24.0),
    Point::new_raw_xy(7.0, 25.0),
    Point::new_raw_xy(7.0, 26.0),
    Point::new_raw_xy(7.0, 27.0),
    Point::new_raw_xy(7.0, 28.0),
    Point::new_raw_xy(7.0, 29.0),
    Point::new_raw_xy(7.0, 30.0),
    Point::new_raw_xy(7.0, 31.0),
    Point::new_raw_xy(7.0, 32.0),
    Point::new_raw_xy(7.0, 33.0),
    Point::new_raw_xy(7.0, 34.0),
    Point::new_raw_xy(7.0, 35.0),
    Point::new_raw_xy(7.0, 36.0),
    Point::new_raw_xy(7.0, 37.0),
    Point::new_raw_xy(7.0, 38.0),
    Point::new_raw_xy(7.0, 39.0),
    Point::new_raw_xy(7.0, 40.0),
    Point::new_raw_xy(7.0, 41.0),
    Point::new_raw_xy(7.0, 42.0),
    Point::new_raw_xy(7.0, 43.0),
    Point::new_raw_xy(7.0, 44.0),
    Point::new_raw_xy(7.0, 45.0),
    Point::new_raw_xy(7.0, 46.0),
    Point::new_raw_xy(7.0, 47.0),
    Point::new_raw_xy(7.0, 48.0),
    Point::new_raw_xy(7.0, 49.0),
    Point::new_raw_xy(7.0, 50.0),
    Point::new_raw_xy(7.0, 51.0),
    Point::new_raw_xy(7.0, 52.0),
    Point::new_raw_xy(7.0, 53.0),
    Point::new_raw_xy(7.0, 54.0),
    Point::new_raw_xy(7.0, 55.0),
    Point::new_raw_xy(7.0, 56.0),
    Point::new_raw_xy(7.0, 57.0),
    Point::new_raw_xy(7.0, 58.0),
    Point::new_raw_xy(7.0, 59.0),
    Point::new_raw_xy(7.0, 60.0),
    Point::new_raw_xy(7.0, 61.0),
    Point::new_raw_xy(7.0, 62.0),
    Point::new_raw_xy(7.0, 64.0),
    Point::new_raw_xy(-3.0, 64.0),
];

#[test]
fn indexed_ear_removal_updates_both_reclassified_neighbors() {
    let mut arena = EarArena::new(&INDEXED_EAR_NEIGHBOR_RING);
    let convex_vertices =
        |tree: &EarCandidateTree| tree.convex.iter().map(|id| id.index()).collect::<Vec<_>>();

    let candidates = arena
        .candidates
        .as_ref()
        .expect("68 vertices use the index");
    assert!(is_ear(&arena, NodeId::from_index(2)));
    assert!(candidates.reflex[1]);
    assert!(candidates.reflex[3]);
    assert_eq!(candidates.nodes[0].live, 63);
    assert_eq!(candidates.nodes[candidates.leaves[1]].live, 1);
    assert_eq!(candidates.nodes[candidates.leaves[3]].live, 1);
    assert_eq!(convex_vertices(candidates), [0, 2, 4, 66, 67]);

    arena.remove(NodeId::from_index(2));

    let candidates = arena
        .candidates
        .as_ref()
        .expect("indexed ring stays indexed");
    assert_eq!(arena.active, 67);
    assert_eq!(
        arena.node(NodeId::from_index(1)).next,
        NodeId::from_index(3)
    );
    assert_eq!(
        arena.node(NodeId::from_index(3)).prev,
        NodeId::from_index(1)
    );
    assert!(!candidates.reflex[1]);
    assert!(!candidates.reflex[3]);
    assert_eq!(candidates.nodes[0].live, 61);
    assert_eq!(candidates.nodes[candidates.leaves[1]].live, 0);
    assert_eq!(candidates.nodes[candidates.leaves[3]].live, 0);
    assert_eq!(convex_vertices(candidates), [0, 1, 3, 4, 66, 67]);
}
