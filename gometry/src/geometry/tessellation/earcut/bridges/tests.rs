use super::*;

#[test]
fn indexed_hole_bridges_preserve_linear_selection_and_filtering() {
    for (count, offset, scale) in [
        (1_usize, 0.0, 1.0),
        (2, 0.0, 1.0),
        (4, 0.0, 1.0),
        (8, 0.0, 1.0),
        (16, 0.0, 1.0),
        (32, 0.0, 1.0),
        (64, 0.0, 1.0),
        (8, 1.0e12, 0.125),
        (8, 0.0, 1.0e-170),
        (8, 0.0, 1.0e150),
    ] {
        let width = (count as f64).sqrt().ceil() as usize;
        let side = width as f64 + 1.0;
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
}
