use super::support::{box_ring, multi_polygon, shift_rings, signature};

struct TallyRow {
    name: &'static str,
    polygons: Vec<Vec<Vec<(f64, f64)>>>,
    expected: Option<&'static str>,
}

fn tally_rows() -> Vec<TallyRow> {
    let b = box_ring;
    let lift = |ring: Vec<(f64, f64)>| {
        ring.into_iter()
            .map(|(x, y)| (x + 360.0, y))
            .collect::<Vec<_>>()
    };
    let world_band = vec![vec![
        (-180.0, 0.0),
        (-60.0, 0.0),
        (60.0, 0.0),
        (180.0, 0.0),
        (180.0, 10.0),
        (60.0, 10.0),
        (-60.0, 10.0),
        (-180.0, 10.0),
        (-180.0, 0.0),
    ]];
    vec![
        TallyRow {
            name: "hole inside shell",
            polygons: vec![vec![b(0., 20., 0., 20.), b(5., 10., 5., 10.)]],
            expected: None,
        },
        TallyRow {
            name: "hole inside shell, hole +360",
            polygons: vec![vec![b(0., 20., 0., 20.), lift(b(5., 10., 5., 10.))]],
            expected: None,
        },
        TallyRow {
            name: "hole outside shell",
            polygons: vec![vec![b(0., 20., 0., 20.), b(25., 30., 5., 10.)]],
            expected: Some("hole_outside_shell"),
        },
        TallyRow {
            name: "hole outside shell, +360",
            polygons: vec![vec![b(0., 20., 0., 20.), lift(b(25., 30., 5., 10.))]],
            expected: Some("hole_outside_shell"),
        },
        TallyRow {
            name: "overlapping holes",
            polygons: vec![vec![
                b(0., 20., 0., 20.),
                b(2., 8., 5., 10.),
                b(5., 12., 6., 12.),
            ]],
            expected: Some("ring_intersection"),
        },
        TallyRow {
            name: "nested holes",
            polygons: vec![vec![
                b(0., 20., 0., 20.),
                b(2., 12., 2., 12.),
                b(4., 8., 4., 8.),
            ]],
            expected: Some("hole_overlap"),
        },
        TallyRow {
            name: "hole across seam in crossing shell",
            polygons: vec![vec![b(170., 190., 0., 20.), b(178., 182., 5., 10.)]],
            expected: None,
        },
        TallyRow {
            name: "overlapping members",
            polygons: vec![vec![b(0., 10., 0., 10.)], vec![b(5., 15., 5., 15.)]],
            expected: Some("ring_intersection"),
        },
        TallyRow {
            name: "duplicate member +360",
            polygons: vec![vec![b(0., 10., 0., 10.)], vec![lift(b(0., 10., 0., 10.))]],
            expected: Some("shared_linework"),
        },
        TallyRow {
            name: "adjacent members",
            polygons: vec![vec![b(0., 10., 0., 10.)], vec![b(10., 20., 0., 10.)]],
            expected: Some("shared_linework"),
        },
        TallyRow {
            name: "member inside another's hole",
            polygons: vec![vec![b(0., 20., 0., 20.), b(5., 15., 5., 15.)], vec![b(
                8., 12., 8., 12.,
            )]],
            expected: None,
        },
        TallyRow {
            name: "a,b(adjacent),d",
            polygons: vec![
                vec![b(20., 40., 20., 40.)],
                vec![b(10., 30., 0., 20.)],
                vec![b(100., 120., 20., 40.)],
            ],
            expected: Some("shared_linework"),
        },
        TallyRow {
            name: "a,b(overlap),d",
            polygons: vec![
                vec![b(20., 40., 20., 40.)],
                vec![b(10., 30., 0., 25.)],
                vec![b(100., 120., 20., 40.)],
            ],
            expected: Some("ring_intersection"),
        },
        TallyRow {
            name: "three disjoint boxes",
            polygons: vec![
                vec![b(20., 40., 20., 40.)],
                vec![b(50., 60., 20., 40.)],
                vec![b(100., 120., 20., 40.)],
            ],
            expected: None,
        },
        TallyRow {
            name: "E/W seam members",
            polygons: vec![vec![b(170., 180., 0., 10.)], vec![b(-180., -170., 0., 10.)]],
            expected: None,
        },
        TallyRow {
            name: "same-side seam overlap",
            polygons: vec![vec![b(180., 190., 0., 10.)], vec![b(-180., -170., 0., 10.)]],
            expected: Some("shared_linework"),
        },
        TallyRow {
            name: "world band + overlapping box",
            polygons: vec![world_band, vec![b(0., 5., 0., 5.)]],
            expected: Some("shared_linework"),
        },
        TallyRow {
            name: "p9: shell + hole + seam cut pair",
            polygons: vec![
                vec![b(-10., 10., 0., 20.), b(2., 5., 5., 10.)],
                vec![b(170., 180., 0., 10.)],
                vec![b(-180., -170., 0., 10.)],
            ],
            expected: None,
        },
        TallyRow {
            name: "p4: overlapping members + seam cut pair",
            polygons: vec![
                vec![b(-10., 5., 0., 10.)],
                vec![b(0., 10., 0., 10.)],
                vec![b(170., 180., 0., 10.)],
                vec![b(-180., -170., 0., 10.)],
            ],
            expected: Some("shared_linework"),
        },
        TallyRow {
            name: "p11: one-ULP gap + seam cut pair",
            polygons: vec![
                vec![b(-20., -10.000_000_000_000_002, 0., 10.)],
                vec![b(-10., 0., 0., 10.)],
                vec![b(170., 180., 0., 10.)],
                vec![b(-180., -170., 0., 10.)],
            ],
            expected: None,
        },
        TallyRow {
            name: "p11 control: exact touch + cut pair",
            polygons: vec![
                vec![b(-20., -10., 0., 10.)],
                vec![b(-10., 0., 0., 10.)],
                vec![b(170., 180., 0., 10.)],
                vec![b(-180., -170., 0., 10.)],
            ],
            expected: Some("shared_linework"),
        },
    ]
}

#[test]
fn cross_ring_face_tallies_use_q_occupancy_and_exact_precedence() {
    let rows = tally_rows();
    assert_eq!(rows.len(), 21);
    for row in rows {
        for shift in [0.0, 360.0, -360.0] {
            let geometry = multi_polygon(&shift_rings(&row.polygons, shift));
            let actual = signature(&geometry);
            if let Some(code) = row.expected {
                assert_eq!(
                    actual.code,
                    Some(code),
                    "{} shift {shift}: {actual:?}",
                    row.name
                );
            }
        }
    }
}
