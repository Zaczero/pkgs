use super::super::normalize::{filter_earcut_ring_points, orient_earcut_ring};
use super::*;
use crate::geometry::tessellation::earcut_bridges::count_bridge_work;

#[test]
fn indexed_hole_bridge_selection_work_scales_subquadratically() {
    let mut counts = Vec::new();
    for hole_count in [25_usize, 100, 400] {
        let mut ring: Vec<_> = (0..500)
            .map(|index| {
                let angle = std::f64::consts::TAU * f64::from(index) / 500.0;
                Point::new_raw_xy(100.0 * angle.cos(), 100.0 * angle.sin())
            })
            .collect();
        orient_earcut_ring(&mut ring, false);
        let width = hole_count.isqrt();
        let spacing = 120.0 / width as f64;
        let holes: Vec<_> = (0..hole_count)
            .map(|index| {
                let x = -50.0 + (index % width) as f64 * spacing;
                let y = -50.0 + (index / width) as f64 * spacing;
                let mut hole = vec![
                    Point::new_raw_xy(x - 1.5, y - 1.5),
                    Point::new_raw_xy(x + 1.5, y - 1.5),
                    Point::new_raw_xy(x + 1.5, y + 1.5),
                    Point::new_raw_xy(x - 1.5, y + 1.5),
                ];
                orient_earcut_ring(&mut hole, true);
                hole
            })
            .collect();
        let mut identities: Vec<_> = (0..ring.len())
            .map(|vertex| EarcutCornerId::source(0, vertex, vertex))
            .collect();
        let hole_identities: Vec<Vec<_>> = (0..hole_count)
            .map(|hole| {
                (0..4)
                    .map(|vertex| EarcutCornerId::source(hole + 1, vertex, 500 + hole * 4 + vertex))
                    .collect()
            })
            .collect();
        let ((), work) = count_bridge_work(|| {
            eliminate_holes_with_ids(
                &mut ring,
                &mut identities,
                &holes,
                &hole_identities,
                500 + hole_count * 4,
            )
            .expect("the grid holes have visible bridges");
        });
        assert_eq!(ring.len(), identities.len());
        assert!(work > 0, "the counter must observe bridge selection");
        counts.push(work);
    }
    for pair in counts.windows(2) {
        if let &[small, large] = pair {
            assert!(
                large < small.wrapping_mul(6),
                "quadrupling holes: {counts:?}",
            );
        }
    }
}

#[rstest::rstest]
#[case::count_1_usize(1_usize, 0.0, 1.0)]
#[case::count_2(2, 0.0, 1.0)]
#[case::count_4(4, 0.0, 1.0)]
#[case::count_8(8, 0.0, 1.0)]
#[case::count_8_a739e232(8, 1.0e12, 0.125)]
#[case::count_8_39485f4b(8, 0.0, 1.0e-170)]
#[case::count_8_a84f0e96(8, 0.0, 1.0e150)]
fn indexed_hole_bridges_preserve_linear_selection_and_filtering(
    #[case] count: usize,
    #[case] offset: f64,
    #[case] scale: f64,
) {
    let width = std::num::NonZeroUsize::new((count as f64).sqrt().ceil() as usize)
        .expect("positive fixture width");
    let side = width.get() as f64 + 1.0;
    let mut shell = vec![
        Point::new_raw_xy(0.0, 0.0),
        Point::new_raw_xy(side, 0.0),
        Point::new_raw_xy(side, side),
        Point::new_raw_xy(0.0, side),
    ];
    let mut holes = Vec::new();
    for index in 0..count {
        let x = (index % width) as f64;
        let y = (index / width) as f64;
        let mut hole = vec![
            Point::new_raw_xy(x + 0.2, y + 0.2),
            Point::new_raw_xy(x + 0.8, y + 0.2),
            Point::new_raw_xy(x + 0.8, y + 0.8),
            Point::new_raw_xy(x + 0.2, y + 0.8),
        ];
        orient_earcut_ring(&mut hole, true);
        holes.push(hole);
    }
    for point in shell.iter_mut().chain(holes.iter_mut().flatten()) {
        point.x = offset + point.x * scale;
        point.y = offset + point.y * scale;
    }
    holes.reverse();
    let mut reference = shell.clone();
    let mut indexed = BridgeArena::new(&shell, shell.len().wrapping_add(count.wrapping_mul(6)));
    let mut order: Vec<_> = holes.iter().enumerate().collect();
    order.sort_unstable_by(|(li, left), (ri, right)| {
        compare_hole_leftmost_xy_slope(left, right).then_with(|| li.cmp(ri))
    });
    for (batch, (_, hole)) in order.into_iter().enumerate() {
        let hole = rotate_ring(hole, leftmost_vertex(hole));
        let bridge = find_hole_bridge(&reference, &hole).expect("visible reference bridge");
        let walk = indexed.filtered_walk();
        assert_eq!(
            indexed.find_bridge(*hole.first().expect("nonempty hole")),
            walk.get(bridge.0).map(|(id, _)| *id),
            "{count} holes, bridge {batch}"
        );
        splice_hole_bridge(&mut reference, bridge, &hole);
        filter_earcut_ring_points(&mut reference);
        indexed.splice(
            walk.get(bridge.0).expect("reference bridge").0,
            &hole,
            batch.wrapping_add(1),
        );
        assert_eq!(
            indexed
                .filtered_walk()
                .into_iter()
                .map(|(_, point)| point)
                .collect::<Vec<_>>(),
            reference,
            "{count} holes, filtered splice {batch}"
        );
    }
}
