use super::super::support::pole_hop;
use super::*;

fn pole_line(longitude: f64, latitude: f64, pole: f64) -> Vec<(f64, f64)> {
    (0..=4)
        .map(|index| (longitude + 90.0 * f64::from(index), latitude))
        .chain([(longitude + 360.0, pole)])
        .chain((1..=4).map(|index| (longitude + 360.0 - 90.0 * f64::from(index), pole)))
        .chain([(longitude, latitude)])
        .collect()
}

fn pole_apex(longitude: f64, latitude: f64, pole: f64) -> Vec<(f64, f64)> {
    (0..=4)
        .map(|index| (longitude + 90.0 * f64::from(index), latitude))
        .chain([
            (longitude + 360.0, pole),
            (longitude + 360.0, latitude),
            (longitude, latitude),
        ])
        .collect()
}

fn repeated_pole_pair(latitude: f64, pole: f64) -> Vec<(f64, f64)> {
    (0..=4)
        .map(|index| (-180.0 + 90.0 * f64::from(index), latitude))
        .chain([
            (180.0, pole),
            (-180.0, pole),
            (180.0, pole),
            (-180.0, pole),
            (-180.0, latitude),
        ])
        .collect()
}

fn stepped_band(longitude: f64) -> Vec<(f64, f64)> {
    (0..=3)
        .map(|index| (longitude + 120.0 * f64::from(index), -30.0))
        .chain(
            (0..=3)
                .rev()
                .map(|index| (longitude + 120.0 * f64::from(index), 30.0)),
        )
        .chain([(longitude, -30.0)])
        .collect()
}

fn full_turn_band(longitude: f64) -> Vec<(f64, f64)> {
    vec![
        (longitude, -30.0),
        (longitude + 360.0, -30.0),
        (longitude + 360.0, 30.0),
        (longitude, 30.0),
        (longitude, -30.0),
    ]
}

fn ordinary_box_ring(west: f64, east: f64) -> Vec<(f64, f64)> {
    super::super::support::box_ring(west, east, 0.0, 10.0)
}

ring_rule_cases!(
    (
        north_hop_zero,
        "hop@0 lat80",
        vec![pole_hop(0.0, 80.0, 90.0)],
        false,
        Expectation::Valid
    ),
    (
        north_pole_line_zero,
        "pole-line@0 lat80",
        vec![pole_line(0.0, 80.0, 90.0)],
        false,
        Expectation::Valid
    ),
    (
        north_apex_zero,
        "apex@0 lat80",
        vec![pole_apex(0.0, 80.0, 90.0)],
        false,
        Expectation::Invalid("self_intersection")
    ),
    (
        north_hop_forty,
        "hop@40 lat80",
        vec![pole_hop(40.0, 80.0, 90.0)],
        false,
        Expectation::Valid
    ),
    (
        north_pole_line_forty,
        "pole-line@40 lat80",
        vec![pole_line(40.0, 80.0, 90.0)],
        false,
        Expectation::Valid
    ),
    (
        north_apex_forty,
        "apex@40 lat80",
        vec![pole_apex(40.0, 80.0, 90.0)],
        false,
        Expectation::Invalid("self_intersection")
    ),
    (
        north_hop_minus_ninety,
        "hop@-90 lat80",
        vec![pole_hop(-90.0, 80.0, 90.0)],
        false,
        Expectation::Valid
    ),
    (
        north_pole_line_minus_ninety,
        "pole-line@-90 lat80",
        vec![pole_line(-90.0, 80.0, 90.0)],
        false,
        Expectation::Valid
    ),
    (
        north_apex_minus_ninety,
        "apex@-90 lat80",
        vec![pole_apex(-90.0, 80.0, 90.0)],
        false,
        Expectation::Invalid("self_intersection")
    ),
    (
        north_hop_one_hundred,
        "hop@100 lat80",
        vec![pole_hop(100.0, 80.0, 90.0)],
        false,
        Expectation::Valid
    ),
    (
        north_pole_line_one_hundred,
        "pole-line@100 lat80",
        vec![pole_line(100.0, 80.0, 90.0)],
        false,
        Expectation::Valid
    ),
    (
        north_apex_one_hundred,
        "apex@100 lat80",
        vec![pole_apex(100.0, 80.0, 90.0)],
        false,
        Expectation::Invalid("self_intersection")
    ),
    (
        north_hop_minus_179,
        "hop@-179 lat80",
        vec![pole_hop(-179.0, 80.0, 90.0)],
        false,
        Expectation::Valid
    ),
    (
        north_pole_line_minus_179,
        "pole-line@-179 lat80",
        vec![pole_line(-179.0, 80.0, 90.0)],
        false,
        Expectation::Valid
    ),
    (
        north_apex_minus_179,
        "apex@-179 lat80",
        vec![pole_apex(-179.0, 80.0, 90.0)],
        false,
        Expectation::Invalid("self_intersection")
    ),
    (
        north_hop_east_seam,
        "hop@180 lat80",
        vec![pole_hop(180.0, 80.0, 90.0)],
        false,
        Expectation::Valid
    ),
    (
        north_pole_line_east_seam,
        "pole-line@180 lat80",
        vec![pole_line(180.0, 80.0, 90.0)],
        false,
        Expectation::Valid
    ),
    (
        north_apex_east_seam,
        "apex@180 lat80",
        vec![pole_apex(180.0, 80.0, 90.0)],
        false,
        Expectation::Invalid("self_intersection")
    ),
    (
        north_hop_west_seam,
        "hop@-180 lat80",
        vec![pole_hop(-180.0, 80.0, 90.0)],
        false,
        Expectation::Valid
    ),
    (
        north_pole_line_west_seam,
        "pole-line@-180 lat80",
        vec![pole_line(-180.0, 80.0, 90.0)],
        false,
        Expectation::Valid
    ),
    (
        north_apex_west_seam,
        "apex@-180 lat80",
        vec![pole_apex(-180.0, 80.0, 90.0)],
        false,
        Expectation::Invalid("self_intersection")
    ),
    (
        north_hop_two_seventy,
        "hop@270 lat80",
        vec![pole_hop(270.0, 80.0, 90.0)],
        false,
        Expectation::Valid
    ),
    (
        north_pole_line_two_seventy,
        "pole-line@270 lat80",
        vec![pole_line(270.0, 80.0, 90.0)],
        false,
        Expectation::Valid
    ),
    (
        north_apex_two_seventy,
        "apex@270 lat80",
        vec![pole_apex(270.0, 80.0, 90.0)],
        false,
        Expectation::Invalid("self_intersection")
    ),
    (
        north_pole_pair,
        "U1 pair A lat80",
        vec![pole_hop(-180.0, 80.0, 90.0)],
        false,
        Expectation::Valid
    ),
    (
        north_repeated_pole_pair,
        "U1 pair repeated C lat80",
        vec![repeated_pole_pair(80.0, 90.0)],
        false,
        Expectation::Invalid("self_intersection")
    ),
    (
        south_hop_zero,
        "hop@0 lat-80",
        vec![pole_hop(0.0, -80.0, -90.0)],
        false,
        Expectation::Valid
    ),
    (
        south_pole_line_zero,
        "pole-line@0 lat-80",
        vec![pole_line(0.0, -80.0, -90.0)],
        false,
        Expectation::Valid
    ),
    (
        south_apex_zero,
        "apex@0 lat-80",
        vec![pole_apex(0.0, -80.0, -90.0)],
        false,
        Expectation::Invalid("self_intersection")
    ),
    (
        south_hop_forty,
        "hop@40 lat-80",
        vec![pole_hop(40.0, -80.0, -90.0)],
        false,
        Expectation::Valid
    ),
    (
        south_pole_line_forty,
        "pole-line@40 lat-80",
        vec![pole_line(40.0, -80.0, -90.0)],
        false,
        Expectation::Valid
    ),
    (
        south_apex_forty,
        "apex@40 lat-80",
        vec![pole_apex(40.0, -80.0, -90.0)],
        false,
        Expectation::Invalid("self_intersection")
    ),
    (
        south_hop_minus_ninety,
        "hop@-90 lat-80",
        vec![pole_hop(-90.0, -80.0, -90.0)],
        false,
        Expectation::Valid
    ),
    (
        south_pole_line_minus_ninety,
        "pole-line@-90 lat-80",
        vec![pole_line(-90.0, -80.0, -90.0)],
        false,
        Expectation::Valid
    ),
    (
        south_apex_minus_ninety,
        "apex@-90 lat-80",
        vec![pole_apex(-90.0, -80.0, -90.0)],
        false,
        Expectation::Invalid("self_intersection")
    ),
    (
        south_hop_one_hundred,
        "hop@100 lat-80",
        vec![pole_hop(100.0, -80.0, -90.0)],
        false,
        Expectation::Valid
    ),
    (
        south_pole_line_one_hundred,
        "pole-line@100 lat-80",
        vec![pole_line(100.0, -80.0, -90.0)],
        false,
        Expectation::Valid
    ),
    (
        south_apex_one_hundred,
        "apex@100 lat-80",
        vec![pole_apex(100.0, -80.0, -90.0)],
        false,
        Expectation::Invalid("self_intersection")
    ),
    (
        south_hop_minus_179,
        "hop@-179 lat-80",
        vec![pole_hop(-179.0, -80.0, -90.0)],
        false,
        Expectation::Valid
    ),
    (
        south_pole_line_minus_179,
        "pole-line@-179 lat-80",
        vec![pole_line(-179.0, -80.0, -90.0)],
        false,
        Expectation::Valid
    ),
    (
        south_apex_minus_179,
        "apex@-179 lat-80",
        vec![pole_apex(-179.0, -80.0, -90.0)],
        false,
        Expectation::Invalid("self_intersection")
    ),
    (
        south_hop_east_seam,
        "hop@180 lat-80",
        vec![pole_hop(180.0, -80.0, -90.0)],
        false,
        Expectation::Valid
    ),
    (
        south_pole_line_east_seam,
        "pole-line@180 lat-80",
        vec![pole_line(180.0, -80.0, -90.0)],
        false,
        Expectation::Valid
    ),
    (
        south_apex_east_seam,
        "apex@180 lat-80",
        vec![pole_apex(180.0, -80.0, -90.0)],
        false,
        Expectation::Invalid("self_intersection")
    ),
    (
        south_hop_west_seam,
        "hop@-180 lat-80",
        vec![pole_hop(-180.0, -80.0, -90.0)],
        false,
        Expectation::Valid
    ),
    (
        south_pole_line_west_seam,
        "pole-line@-180 lat-80",
        vec![pole_line(-180.0, -80.0, -90.0)],
        false,
        Expectation::Valid
    ),
    (
        south_apex_west_seam,
        "apex@-180 lat-80",
        vec![pole_apex(-180.0, -80.0, -90.0)],
        false,
        Expectation::Invalid("self_intersection")
    ),
    (
        south_hop_two_seventy,
        "hop@270 lat-80",
        vec![pole_hop(270.0, -80.0, -90.0)],
        false,
        Expectation::Valid
    ),
    (
        south_pole_line_two_seventy,
        "pole-line@270 lat-80",
        vec![pole_line(270.0, -80.0, -90.0)],
        false,
        Expectation::Valid
    ),
    (
        south_apex_two_seventy,
        "apex@270 lat-80",
        vec![pole_apex(270.0, -80.0, -90.0)],
        false,
        Expectation::Invalid("self_intersection")
    ),
    (
        south_pole_pair,
        "U1 pair A lat-80",
        vec![pole_hop(-180.0, -80.0, -90.0)],
        false,
        Expectation::Valid
    ),
    (
        south_repeated_pole_pair,
        "U1 pair repeated C lat-80",
        vec![repeated_pole_pair(-80.0, -90.0)],
        false,
        Expectation::Invalid("self_intersection")
    ),
    (
        band_steps_west_seam,
        "band 120deg steps @-180",
        vec![stepped_band(-180.0)],
        false,
        Expectation::Valid
    ),
    (
        band_full_turn_west_seam,
        "band explicit full-turn edges @-180",
        vec![full_turn_band(-180.0)],
        false,
        Expectation::Invalid("self_intersection")
    ),
    (
        band_steps_zero,
        "band 120deg steps @0",
        vec![stepped_band(0.0)],
        false,
        Expectation::Valid
    ),
    (
        band_full_turn_zero,
        "band explicit full-turn edges @0",
        vec![full_turn_band(0.0)],
        false,
        Expectation::Valid
    ),
    (
        band_steps_forty,
        "band 120deg steps @40",
        vec![stepped_band(40.0)],
        false,
        Expectation::Valid
    ),
    (
        band_full_turn_forty,
        "band explicit full-turn edges @40",
        vec![full_turn_band(40.0)],
        false,
        Expectation::Valid
    ),
    (
        band_steps_minus_ninety,
        "band 120deg steps @-90",
        vec![stepped_band(-90.0)],
        false,
        Expectation::Valid
    ),
    (
        band_full_turn_minus_ninety,
        "band explicit full-turn edges @-90",
        vec![full_turn_band(-90.0)],
        false,
        Expectation::Valid
    ),
    (
        band_steps_east_seam,
        "band 120deg steps @180",
        vec![stepped_band(180.0)],
        false,
        Expectation::Valid
    ),
    (
        band_full_turn_east_seam,
        "band explicit full-turn edges @180",
        vec![full_turn_band(180.0)],
        false,
        Expectation::Invalid("self_intersection")
    ),
    (
        keyhole_slit,
        "keyhole slit",
        vec![vec![
            (0.0, 0.0),
            (40.0, 0.0),
            (40.0, 40.0),
            (20.0, 40.0),
            (20.0, 20.0),
            (20.0, 40.0),
            (0.0, 40.0),
            (0.0, 0.0)
        ]],
        false,
        Expectation::Invalid("self_intersection")
    ),
    (
        outward_spur,
        "outward spur",
        vec![vec![
            (0.0, 0.0),
            (40.0, 0.0),
            (40.0, 40.0),
            (20.0, 40.0),
            (20.0, 60.0),
            (20.0, 40.0),
            (0.0, 40.0),
            (0.0, 0.0)
        ]],
        false,
        Expectation::Invalid("self_intersection")
    ),
    (
        stutter_equal_vertices,
        "stutter equal vertices",
        vec![vec![
            (0.0, 0.0),
            (0.0, 0.0),
            (40.0, 0.0),
            (40.0, 40.0),
            (0.0, 40.0),
            (0.0, 0.0)
        ]],
        false,
        Expectation::Valid
    ),
    (
        stutter_seam_pair_at_lat_0,
        "stutter seam pair at lat 0",
        vec![vec![
            (170.0, 0.0),
            (180.0, 0.0),
            (-180.0, 0.0),
            (-170.0, 0.0),
            (-170.0, 10.0),
            (170.0, 10.0),
            (170.0, 0.0)
        ]],
        false,
        Expectation::Valid
    ),
    (
        triangle_to_pole_with_seam_pair_stutter,
        "triangle to pole with seam pair stutter",
        vec![vec![
            (0.0, 80.0),
            (40.0, 80.0),
            (180.0, 90.0),
            (-180.0, 90.0),
            (0.0, 80.0)
        ]],
        false,
        Expectation::Valid
    ),
    (
        triangle_touching_pole_once,
        "triangle touching pole once",
        vec![vec![(0.0, 80.0), (40.0, 80.0), (20.0, 90.0), (0.0, 80.0)]],
        false,
        Expectation::Valid
    ),
    (
        ring_touching_pole_twice,
        "ring touching pole twice",
        vec![vec![
            (0.0, 70.0),
            (30.0, 90.0),
            (60.0, 70.0),
            (90.0, 70.0),
            (120.0, 90.0),
            (150.0, 70.0),
            (0.0, 70.0)
        ]],
        false,
        Expectation::Invalid("self_intersection")
    ),
    (
        ring_along_the_pole_line_only,
        "ring along the pole line only",
        vec![vec![(0.0, 90.0), (120.0, 90.0), (240.0, 90.0), (0.0, 90.0)]],
        false,
        Expectation::Invalid("ring_collapsed")
    ),
    (
        pinched_full_longitude_ring,
        "pinched full-longitude ring",
        vec![vec![
            (0.0, 10.0),
            (120.0, 30.0),
            (240.0, 20.0),
            (360.0, 10.0),
            (240.0, 0.0),
            (120.0, 5.0),
            (0.0, 10.0)
        ]],
        false,
        Expectation::Valid
    ),
    (
        zigzag_sided_full_longitude_band,
        "zigzag-sided full-longitude band",
        vec![vec![
            (0.0, 0.0),
            (120.0, 0.0),
            (240.0, 0.0),
            (360.0, 0.0),
            (370.0, 10.0),
            (360.0, 20.0),
            (240.0, 20.0),
            (120.0, 20.0),
            (0.0, 20.0),
            (10.0, 10.0),
            (0.0, 0.0)
        ]],
        false,
        Expectation::Valid
    ),
    (
        ring_wider_than_a_period,
        "ring wider than a period",
        vec![vec![
            (0.0, 0.0),
            (150.0, 0.0),
            (300.0, 0.0),
            (450.0, 0.0),
            (450.0, 20.0),
            (300.0, 20.0),
            (150.0, 20.0),
            (0.0, 20.0),
            (0.0, 0.0)
        ]],
        false,
        Expectation::Invalid("self_intersection")
    ),
    (
        bowtie,
        "bowtie",
        vec![vec![
            (0.0, 0.0),
            (10.0, 10.0),
            (10.0, 0.0),
            (0.0, 10.0),
            (0.0, 0.0)
        ]],
        false,
        Expectation::Invalid("self_intersection")
    ),
    (
        self_t_touch,
        "self T-touch",
        vec![vec![
            (0.0, 0.0),
            (20.0, 0.0),
            (20.0, 10.0),
            (10.0, 0.0),
            (10.0, 10.0),
            (0.0, 10.0),
            (0.0, 0.0)
        ]],
        false,
        Expectation::Invalid("self_intersection")
    ),
    (
        seam_bowtie_explicit_lift,
        "bowtie across the seam (explicit lift)",
        vec![vec![
            (170.0, -10.0),
            (190.0, 10.0),
            (170.0, 10.0),
            (190.0, -10.0),
            (170.0, -10.0)
        ]],
        false,
        Expectation::Invalid("self_intersection")
    ),
    (
        seam_bowtie_canonical_spelling,
        "bowtie across the seam (canonical spelling)",
        vec![vec![
            (170.0, -10.0),
            (-170.0, 10.0),
            (170.0, 10.0),
            (-170.0, -10.0),
            (170.0, -10.0)
        ]],
        false,
        Expectation::Invalid("self_intersection")
    ),
    (
        ordinary_box,
        "ordinary box",
        vec![vec![
            (0.0, 0.0),
            (10.0, 0.0),
            (10.0, 10.0),
            (0.0, 10.0),
            (0.0, 0.0)
        ]],
        false,
        Expectation::Valid
    ),
    (
        seam_crossing_box_explicit_lift,
        "seam-crossing box (explicit lift)",
        vec![vec![
            (170.0, 0.0),
            (190.0, 0.0),
            (190.0, 10.0),
            (170.0, 10.0),
            (170.0, 0.0)
        ]],
        false,
        Expectation::Valid
    ),
    (
        e_w_seam_members,
        "E/W seam members",
        vec![
            ordinary_box_ring(170.0, 180.0),
            ordinary_box_ring(-180.0, -170.0)
        ],
        true,
        Expectation::Valid
    ),
    (
        e_e_seam_members,
        "E/E seam members",
        vec![
            ordinary_box_ring(170.0, 180.0),
            ordinary_box_ring(180.0, 190.0)
        ],
        true,
        Expectation::Valid
    ),
    (
        adjacent_ordinary_members,
        "adjacent ordinary members",
        vec![ordinary_box_ring(0.0, 10.0), ordinary_box_ring(10.0, 20.0)],
        true,
        Expectation::Invalid("shared_linework")
    ),
    (
        duplicate_member_360,
        "duplicate member +360",
        vec![
            ordinary_box_ring(0.0, 10.0),
            ordinary_box_ring(360.0, 370.0)
        ],
        true,
        Expectation::Invalid("shared_linework")
    ),
    (
        hole_sharing_shell_edge,
        "hole sharing shell edge",
        vec![ordinary_box_ring(0.0, 20.0), vec![
            (0.0, 2.0),
            (5.0, 2.0),
            (5.0, 5.0),
            (0.0, 5.0),
            (0.0, 2.0)
        ]],
        false,
        Expectation::Invalid("shared_linework")
    ),
);
