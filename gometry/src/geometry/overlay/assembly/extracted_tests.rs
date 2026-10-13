use super::super::test_support::{chain_coords, chains_coordinate_bits};
use crate::geometry::overlay::{Strictness, merge_chains};
use crate::geometry::{
    CoordSeq, CoordinateAxes, GeometryErrorKind, MOrdinate, Point, Shape, ZOrdinate,
};

#[test]
fn merge_chains_discards_empty_and_xy_coincident_sequences() {
    let no_edges: [CoordSeq; 0] = [];
    assert!(merge_chains(&no_edges).unwrap().is_empty());

    let one_point = CoordSeq::try_from_points(&[Point::new_raw_xy(2.0, 3.0)]).unwrap();
    let coincident_xy = CoordSeq::try_from_points(&[
        Point::new_axes(7.0, 9.0, ZOrdinate(Some(1.0)), MOrdinate(Some(11.0))).unwrap(),
        Point::new_axes(7.0, 9.0, ZOrdinate(Some(2.0)), MOrdinate(Some(22.0))).unwrap(),
    ])
    .unwrap();
    let edges = [
        CoordSeq::empty(CoordinateAxes::XY),
        one_point,
        coincident_xy,
    ];
    assert!(merge_chains(&edges).unwrap().is_empty());
}

#[test]
fn merge_chains_keeps_a_late_single_noncoincident_closed_edge() {
    let ignored = chain_coords(&[(7.0, 9.0), (7.0, 9.0)]);
    let closed = chain_coords(&[(0.0, 0.0), (1.0, 0.0), (0.0, 0.0)]);
    let edges = [CoordSeq::empty(CoordinateAxes::XY), ignored, closed];

    let chains = merge_chains(&edges).unwrap();

    assert_eq!(chains_coordinate_bits(&chains), [vec![
        (0.0_f64.to_bits(), 0.0_f64.to_bits(), None, None),
        (1.0_f64.to_bits(), 0.0_f64.to_bits(), None, None),
        (0.0_f64.to_bits(), 0.0_f64.to_bits(), None, None),
    ]]);
}

#[test]
fn merge_chains_preserves_reversed_path_after_filtered_input_ordinals() {
    let edges = [
        chain_coords(&[(9.0, 9.0), (9.0, 9.0)]),
        chain_coords(&[(2.0, 0.0), (1.0, 0.0)]),
        chain_coords(&[(8.0, 8.0), (8.0, 8.0)]),
        chain_coords(&[(0.0, 0.0), (1.0, 0.0)]),
    ];

    let chains = merge_chains(&edges).unwrap();

    assert_eq!(chains_coordinate_bits(&chains), [vec![
        (2.0_f64.to_bits(), 0.0_f64.to_bits(), None, None),
        (1.0_f64.to_bits(), 0.0_f64.to_bits(), None, None),
        (0.0_f64.to_bits(), 0.0_f64.to_bits(), None, None),
    ]]);
}

#[test]
fn merge_chains_turns_parallel_duplicate_edges_into_one_cycle() {
    let edge = chain_coords(&[(0.0, 0.0), (1.0, 0.0)]);
    let edges = [edge.clone(), edge];

    let chains = merge_chains(&edges).unwrap();

    assert_eq!(chains_coordinate_bits(&chains), [vec![
        (0.0_f64.to_bits(), 0.0_f64.to_bits(), None, None),
        (1.0_f64.to_bits(), 0.0_f64.to_bits(), None, None),
        (0.0_f64.to_bits(), 0.0_f64.to_bits(), None, None),
    ]]);
}

#[test]
fn merge_chains_emits_open_paths_before_lower_seed_cycles() {
    let edges = [
        chain_coords(&[(10.0, 0.0), (11.0, 0.0)]),
        chain_coords(&[(11.0, 0.0), (10.0, 1.0)]),
        chain_coords(&[(10.0, 1.0), (10.0, 0.0)]),
        chain_coords(&[(2.0, 0.0), (1.0, 0.0)]),
        chain_coords(&[(0.0, 0.0), (1.0, 0.0)]),
    ];

    let chains = merge_chains(&edges).unwrap();

    assert_eq!(chains_coordinate_bits(&chains), [
        vec![
            (2.0_f64.to_bits(), 0.0_f64.to_bits(), None, None),
            (1.0_f64.to_bits(), 0.0_f64.to_bits(), None, None),
            (0.0_f64.to_bits(), 0.0_f64.to_bits(), None, None),
        ],
        vec![
            (10.0_f64.to_bits(), 0.0_f64.to_bits(), None, None),
            (11.0_f64.to_bits(), 0.0_f64.to_bits(), None, None),
            (10.0_f64.to_bits(), 1.0_f64.to_bits(), None, None),
            (10.0_f64.to_bits(), 0.0_f64.to_bits(), None, None),
        ],
    ]);
}

#[test]
fn merge_chains_splits_each_arm_at_a_degree_three_junction() {
    let edges = [
        chain_coords(&[(0.0, 0.0), (1.0, 0.0)]),
        chain_coords(&[(1.0, 0.0), (2.0, 0.0)]),
        chain_coords(&[(1.0, 0.0), (1.0, 1.0)]),
    ];

    let chains = merge_chains(&edges).unwrap();

    assert_eq!(chains_coordinate_bits(&chains), [
        vec![
            (0.0_f64.to_bits(), 0.0_f64.to_bits(), None, None),
            (1.0_f64.to_bits(), 0.0_f64.to_bits(), None, None),
        ],
        vec![
            (1.0_f64.to_bits(), 0.0_f64.to_bits(), None, None),
            (2.0_f64.to_bits(), 0.0_f64.to_bits(), None, None),
        ],
        vec![
            (1.0_f64.to_bits(), 0.0_f64.to_bits(), None, None),
            (1.0_f64.to_bits(), 1.0_f64.to_bits(), None, None),
        ],
    ]);
}

#[test]
fn merge_chains_preserves_xyz_m_bits_and_signed_zero_at_reversed_joins() {
    let point = |x_coordinate, y_coordinate, elevation, measure| {
        Point::new_axes(
            x_coordinate,
            y_coordinate,
            ZOrdinate(Some(elevation)),
            MOrdinate(Some(measure)),
        )
        .unwrap()
    };
    let first = CoordSeq::try_from_points(&[
        point(2.0, -0.0, 22.0, 202.0),
        point(1.5, -0.0, 15.0, 150.0),
        point(1.0, -0.0, 11.0, 101.0),
    ])
    .unwrap();
    let second =
        CoordSeq::try_from_points(&[point(0.0, -0.0, 0.0, 0.0), point(1.0, 0.0, 11.0, 101.0)])
            .unwrap();

    let chains = merge_chains(&[first, second]).unwrap();

    assert_eq!(chains_coordinate_bits(&chains), [vec![
        (
            2.0_f64.to_bits(),
            (-0.0_f64).to_bits(),
            Some(22.0_f64.to_bits()),
            Some(202.0_f64.to_bits()),
        ),
        (
            1.5_f64.to_bits(),
            (-0.0_f64).to_bits(),
            Some(15.0_f64.to_bits()),
            Some(150.0_f64.to_bits()),
        ),
        (
            1.0_f64.to_bits(),
            (-0.0_f64).to_bits(),
            Some(11.0_f64.to_bits()),
            Some(101.0_f64.to_bits()),
        ),
        (
            0.0_f64.to_bits(),
            (-0.0_f64).to_bits(),
            Some(0.0_f64.to_bits()),
            Some(0.0_f64.to_bits()),
        ),
    ]]);
}

#[test]
fn merge_chains_preserves_coordinate_axis_mismatch_error() {
    let xy = chain_coords(&[(0.0, 0.0), (1.0, 0.0)]);
    let xyz = CoordSeq::try_from_points(&[
        Point::new_axes(1.0, 0.0, ZOrdinate(Some(3.0)), MOrdinate(None)).unwrap(),
        Point::new_axes(2.0, 0.0, ZOrdinate(Some(4.0)), MOrdinate(None)).unwrap(),
    ])
    .unwrap();

    let error = merge_chains(&[xy, xyz]).unwrap_err();

    assert!(matches!(
        error.kind(),
        crate::error::ErrorKind::Geometry(GeometryErrorKind::CoordinateAxesMismatch { .. })
    ));
}

/// Every overlay lane leaves through one builder, so a node on a straight
/// run never survives to the output, whichever lane produced the ring.
#[test]
fn overlay_output_rings_carry_no_collinear_nodes() {
    let shape = |wkt: &str| crate::io::parse_wkt(wkt).unwrap();
    let wkt = |shape: &Shape| crate::io::to_wkt(shape).unwrap();
    let holed = shape("POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1))");
    let plain = shape("POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))");
    let partner = shape("POLYGON ((1 1, 3 1, 3 4, 1 4, 1 1))");
    let strict = Strictness::Strict;
    assert_eq!(
        wkt(&plain.union(&partner, strict).unwrap()),
        "POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))"
    );
    assert_eq!(
        wkt(&holed.union(&partner, strict).unwrap()),
        "POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))"
    );
    assert_eq!(
        wkt(&holed.difference(&partner, strict).unwrap()),
        "POLYGON ((0 0, 4 0, 4 4, 3 4, 3 1, 1 1, 1 4, 0 4, 0 0))"
    );
}
