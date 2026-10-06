#[test]
fn source_current_certificate_requires_every_edge_to_be_reversed() {
    use crate::geometry::Point;

    let frame = GeographicFrame::crs84();
    let reversed = Polygon::new(
        Ring::closed(
            [
                (0.0, 0.0),
                (4.0, 0.0),
                (4.0, 4.0),
                (0.0, 4.0),
                (0.0, 0.0),
                (0.0, 4.0),
                (4.0, 4.0),
                (4.0, 0.0),
                (0.0, 0.0),
            ]
            .into_iter()
            .map(|(x, y)| Point::new_raw_xy(x, y))
            .collect(),
        )
        .unwrap(),
        Vec::new(),
    );
    assert!(reversed_source_zero(&reversed, frame));

    let mut fifth_moment_lobes = vec![(0.0, 0.0)];
    for (index, weight) in [1.0, 4.0, 6.0, 4.0, 1.0].into_iter().enumerate() {
        let low = index as f64 / 8.0;
        let high = low + 1.0 / 256.0;
        let width = weight / 256.0;
        let mut rectangle = vec![
            (0.0, low),
            (width, low),
            (width, high),
            (0.0, high),
            (0.0, low),
        ];
        if index % 2 != 0 {
            rectangle.reverse();
        }
        fifth_moment_lobes.extend(rectangle);
        fifth_moment_lobes.push((0.0, 0.0));
    }
    let nonzero_after_prefix = Polygon::new(
        Ring::closed(
            fifth_moment_lobes
                .into_iter()
                .map(|(x, y)| Point::new_raw_xy(x, y))
                .collect(),
        )
        .unwrap(),
        Vec::new(),
    );
    assert!(!reversed_source_zero(&nonzero_after_prefix, frame));

    let half_period = Polygon::new(
        Ring::closed(
            [(0.0, 0.0), (180.0, 0.0), (180.0, 4.0), (0.0, 4.0)]
                .into_iter()
                .map(|(x, y)| Point::new_raw_xy(x, y))
                .collect(),
        )
        .unwrap(),
        Vec::new(),
    );
    assert!(!reversed_source_zero(&half_period, frame));

    let reversed_shell_open_hole = Polygon::new(
        reversed.shell.clone(),
        vec![Ring::closed(
            [(1.0, 1.0), (3.0, 1.0), (3.0, 3.0), (1.0, 3.0)]
                .into_iter()
                .map(|(x, y)| Point::new_raw_xy(x, y))
                .collect(),
        )
        .unwrap()],
    );
    assert!(!reversed_source_zero(&reversed_shell_open_hole, frame));
}
