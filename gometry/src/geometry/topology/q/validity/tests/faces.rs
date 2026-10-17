use super::support::{box_ring, multi_polygon, shift_rings, signature};

struct TallyRow {
    name: &'static str,
    polygons: Vec<Vec<Vec<(f64, f64)>>>,
    expected: Option<&'static str>,
}

fn lifted_ring(ring: Vec<(f64, f64)>) -> Vec<(f64, f64)> {
    ring.into_iter().map(|(x, y)| (x + 360.0, y)).collect()
}

fn world_band() -> Vec<Vec<(f64, f64)>> {
    vec![vec![
        (-180.0, 0.0),
        (-60.0, 0.0),
        (60.0, 0.0),
        (180.0, 0.0),
        (180.0, 10.0),
        (60.0, 10.0),
        (-60.0, 10.0),
        (-180.0, 10.0),
        (-180.0, 0.0),
    ]]
}

#[test]
fn one_ulp_gap_with_a_seam_cut_pair_is_valid_without_rounding_the_source() {
    let geometry = multi_polygon(&[
        vec![box_ring(-20., -10.000_000_000_000_002, 0., 10.)],
        vec![box_ring(-10., 0., 0., 10.)],
        vec![box_ring(170., 180., 0., 10.)],
        vec![box_ring(-180., -170., 0., 10.)],
    ]);
    assert_eq!(signature(&geometry).code, None);
}

fn assert_cross_ring_face_tally(row: &TallyRow, shift: f64) {
    let geometry = multi_polygon(&shift_rings(&row.polygons, shift));
    let actual = signature(&geometry);
    assert_eq!(
        actual.code, row.expected,
        "{} shift {shift}: {actual:?}",
        row.name
    );
}

/// Give each face-tally input its own three independently runnable period spellings.
macro_rules! face_tally_cases {
    ($(($name:ident, $label:literal, $polygons:expr, $expected:expr)),* $(,)?) => {
        $(
            mod $name {
                use super::*;

                fn row() -> TallyRow {
                    TallyRow { name: $label, polygons: $polygons, expected: $expected }
                }

                #[test]
                fn original() { assert_cross_ring_face_tally(&row(), 0.0); }

                #[test]
                fn east_period() { assert_cross_ring_face_tally(&row(), 360.0); }

                #[test]
                fn west_period() { assert_cross_ring_face_tally(&row(), -360.0); }
            }
        )*
    };
}

face_tally_cases!(
    (
        hole_inside_shell,
        "hole inside shell",
        vec![vec![box_ring(0., 20., 0., 20.), box_ring(5., 10., 5., 10.)]],
        None
    ),
    (
        hole_inside_shell_hole_360,
        "hole inside shell, hole +360",
        vec![vec![
            box_ring(0., 20., 0., 20.),
            lifted_ring(box_ring(5., 10., 5., 10.))
        ]],
        None
    ),
    (
        hole_outside_shell,
        "hole outside shell",
        vec![vec![
            box_ring(0., 20., 0., 20.),
            box_ring(25., 30., 5., 10.)
        ]],
        Some("hole_outside_shell")
    ),
    (
        hole_outside_shell_360,
        "hole outside shell, +360",
        vec![vec![
            box_ring(0., 20., 0., 20.),
            lifted_ring(box_ring(25., 30., 5., 10.))
        ]],
        Some("hole_outside_shell")
    ),
    (
        overlapping_holes,
        "overlapping holes",
        vec![vec![
            box_ring(0., 20., 0., 20.),
            box_ring(2., 8., 5., 10.),
            box_ring(5., 12., 6., 12.)
        ]],
        Some("ring_intersection")
    ),
    (
        nested_holes,
        "nested holes",
        vec![vec![
            box_ring(0., 20., 0., 20.),
            box_ring(2., 12., 2., 12.),
            box_ring(4., 8., 4., 8.)
        ]],
        Some("hole_overlap")
    ),
    (
        hole_across_seam_in_crossing_shell,
        "hole across seam in crossing shell",
        vec![vec![
            box_ring(170., 190., 0., 20.),
            box_ring(178., 182., 5., 10.)
        ]],
        None
    ),
    (
        overlapping_members,
        "overlapping members",
        vec![vec![box_ring(0., 10., 0., 10.)], vec![box_ring(
            5., 15., 5., 15.
        )]],
        Some("ring_intersection")
    ),
    (
        duplicate_member_360,
        "duplicate member +360",
        vec![vec![box_ring(0., 10., 0., 10.)], vec![lifted_ring(
            box_ring(0., 10., 0., 10.)
        )]],
        Some("shared_linework")
    ),
    (
        adjacent_members,
        "adjacent members",
        vec![vec![box_ring(0., 10., 0., 10.)], vec![box_ring(
            10., 20., 0., 10.
        )]],
        Some("shared_linework")
    ),
    (
        member_inside_another_s_hole,
        "member inside another's hole",
        vec![
            vec![box_ring(0., 20., 0., 20.), box_ring(5., 15., 5., 15.)],
            vec![box_ring(8., 12., 8., 12.)]
        ],
        None
    ),
    (
        a_b_adjacent_d,
        "a,b(adjacent),d",
        vec![
            vec![box_ring(20., 40., 20., 40.)],
            vec![box_ring(10., 30., 0., 20.)],
            vec![box_ring(100., 120., 20., 40.)]
        ],
        Some("shared_linework")
    ),
    (
        a_b_overlap_d,
        "a,b(overlap),d",
        vec![
            vec![box_ring(20., 40., 20., 40.)],
            vec![box_ring(10., 30., 0., 25.)],
            vec![box_ring(100., 120., 20., 40.)]
        ],
        Some("ring_intersection")
    ),
    (
        three_disjoint_boxes,
        "three disjoint boxes",
        vec![
            vec![box_ring(20., 40., 20., 40.)],
            vec![box_ring(50., 60., 20., 40.)],
            vec![box_ring(100., 120., 20., 40.)]
        ],
        None
    ),
    (
        e_w_seam_members,
        "E/W seam members",
        vec![vec![box_ring(170., 180., 0., 10.)], vec![box_ring(
            -180., -170., 0., 10.
        )]],
        None
    ),
    (
        same_side_seam_overlap,
        "same-side seam overlap",
        vec![vec![box_ring(180., 190., 0., 10.)], vec![box_ring(
            -180., -170., 0., 10.
        )]],
        Some("shared_linework")
    ),
    (
        world_band_overlapping_box,
        "world band + overlapping box",
        vec![world_band(), vec![box_ring(0., 5., 0., 5.)]],
        Some("shared_linework")
    ),
    (
        p9_shell_hole_seam_cut_pair,
        "p9: shell + hole + seam cut pair",
        vec![
            vec![box_ring(-10., 10., 0., 20.), box_ring(2., 5., 5., 10.)],
            vec![box_ring(170., 180., 0., 10.)],
            vec![box_ring(-180., -170., 0., 10.)]
        ],
        None
    ),
    (
        p4_overlapping_members_seam_cut_pair,
        "p4: overlapping members + seam cut pair",
        vec![
            vec![box_ring(-10., 5., 0., 10.)],
            vec![box_ring(0., 10., 0., 10.)],
            vec![box_ring(170., 180., 0., 10.)],
            vec![box_ring(-180., -170., 0., 10.)]
        ],
        Some("shared_linework")
    ),
    (
        period_lift_stable_gap_seam_cut_pair,
        "period-lift-stable gap + seam cut pair",
        vec![
            vec![box_ring(-20., -10.000_000_000_000_057, 0., 10.)],
            vec![box_ring(-10., 0., 0., 10.)],
            vec![box_ring(170., 180., 0., 10.)],
            vec![box_ring(-180., -170., 0., 10.)]
        ],
        None
    ),
    (
        p11_control_exact_touch_cut_pair,
        "p11 control: exact touch + cut pair",
        vec![
            vec![box_ring(-20., -10., 0., 10.)],
            vec![box_ring(-10., 0., 0., 10.)],
            vec![box_ring(170., 180., 0., 10.)],
            vec![box_ring(-180., -170., 0., 10.)]
        ],
        Some("shared_linework")
    ),
);
