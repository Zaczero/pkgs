use super::support::{box_ring, multi_polygon, shift_rings, signature};

fn diamond(center_x: f64, center_y: f64, radius: f64) -> Vec<(f64, f64)> {
    vec![
        (center_x - radius, center_y),
        (center_x, center_y - radius),
        (center_x + radius, center_y),
        (center_x, center_y + radius),
        (center_x - radius, center_y),
    ]
}

fn shift_ring(ring: &[(f64, f64)], shift: f64) -> Vec<(f64, f64)> {
    ring.iter().map(|&(x, y)| (x + shift, y)).collect()
}

struct ConnectivityRow {
    name: &'static str,
    polygons: Vec<Vec<Vec<(f64, f64)>>>,
    expected: Option<&'static str>,
}

fn connectivity_rows() -> Vec<ConnectivityRow> {
    let b = box_ring;
    vec![
        ConnectivityRow {
            name: "hole touches shell once",
            polygons: vec![vec![b(0.0, 20.0, 0.0, 20.0), diamond(5.0, 10.0, 5.0)]],
            expected: None,
        },
        ConnectivityRow {
            name: "hole touches shell twice",
            polygons: vec![vec![b(0.0, 20.0, 0.0, 20.0), diamond(10.0, 10.0, 10.0)]],
            expected: Some("interior_disconnected"),
        },
        ConnectivityRow {
            name: "chain of two hole touches",
            polygons: vec![vec![
                b(0.0, 20.0, 0.0, 20.0),
                diamond(5.0, 10.0, 5.0),
                diamond(14.0, 10.0, 4.0),
            ]],
            expected: None,
        },
        ConnectivityRow {
            name: "cycle of two diamond holes",
            polygons: vec![vec![
                b(0.0, 20.0, 0.0, 20.0),
                diamond(5.0, 10.0, 5.0),
                diamond(15.0, 10.0, 5.0),
            ]],
            expected: Some("interior_disconnected"),
        },
        ConnectivityRow {
            name: "hole touches shell twice, +360",
            polygons: vec![vec![
                b(0.0, 20.0, 0.0, 20.0),
                shift_ring(&diamond(10.0, 10.0, 10.0), 360.0),
            ]],
            expected: Some("interior_disconnected"),
        },
        ConnectivityRow {
            name: "hole touches shell once, +360",
            polygons: vec![vec![
                b(0.0, 20.0, 0.0, 20.0),
                shift_ring(&diamond(5.0, 10.0, 5.0), 360.0),
            ]],
            expected: None,
        },
        ConnectivityRow {
            name: "two holes touch each other and shell in a cycle",
            polygons: vec![vec![
                b(0.0, 20.0, 0.0, 20.0),
                vec![
                    (0.0, 10.0),
                    (5.0, 5.0),
                    (10.0, 10.0),
                    (5.0, 15.0),
                    (0.0, 10.0),
                ],
                vec![
                    (20.0, 10.0),
                    (15.0, 5.0),
                    (10.0, 10.0),
                    (15.0, 15.0),
                    (20.0, 10.0),
                ],
            ]],
            expected: Some("interior_disconnected"),
        },
    ]
}

#[test]
fn q_touch_node_cycles_report_disconnected_interiors() {
    let rows = connectivity_rows();
    assert_eq!(rows.len(), 7);
    for row in rows {
        let mut baseline = None;
        for shift in [0.0, 360.0, -360.0] {
            let geometry = multi_polygon(&shift_rings(&row.polygons, shift));
            let actual = signature(&geometry);
            assert_eq!(
                actual.code, row.expected,
                "{} shift {shift}: {actual:?}",
                row.name
            );
            if let Some(expected) = &baseline {
                assert_eq!(&actual, expected, "{} shift {shift}", row.name);
            } else {
                baseline = Some(actual);
            }
        }
    }
}
