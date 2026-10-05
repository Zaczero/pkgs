use alloc::{format, vec};

use super::*;

#[test]
fn direction_at() {
    use Resolution::{
        Eight, Eleven, Fifteen, Five, Four, Fourteen, Nine, One, Seven, Six, Ten, Thirteen, Three,
        Twelve, Two, Zero,
    };

    let cell = CellIndex::new_unchecked(0x8C2BAE305336BFF);

    assert_eq!(cell.resolution(), Twelve);

    assert_eq!(cell.direction_at(Zero), None);
    assert_eq!(cell.direction_at(One), Some(Direction::IJ));
    assert_eq!(cell.direction_at(Two), Some(Direction::IK));
    assert_eq!(cell.direction_at(Three), Some(Direction::IJ));
    assert_eq!(cell.direction_at(Four), Some(Direction::K));
    assert_eq!(cell.direction_at(Five), Some(Direction::I));
    assert_eq!(cell.direction_at(Six), Some(Direction::Center));
    assert_eq!(cell.direction_at(Seven), Some(Direction::IK));
    assert_eq!(cell.direction_at(Eight), Some(Direction::K));
    assert_eq!(cell.direction_at(Nine), Some(Direction::I));
    assert_eq!(cell.direction_at(Ten), Some(Direction::IJ));
    assert_eq!(cell.direction_at(Eleven), Some(Direction::IJ));
    assert_eq!(cell.direction_at(Twelve), Some(Direction::IK));
    assert_eq!(cell.direction_at(Thirteen), None);
    assert_eq!(cell.direction_at(Fourteen), None);
    assert_eq!(cell.direction_at(Fifteen), None);
}

#[test]
fn ordering() {
    let mut cells = vec![
        CellIndex::new_unchecked(0x8A194E699AB7FFF),
        CellIndex::new_unchecked(0x89194E69D4FFFFF),
    ];
    let expected = vec![
        CellIndex::new_unchecked(0x8A194E699AB7FFF),
        CellIndex::new_unchecked(0x89194E69D4FFFFF),
    ];

    cells.sort_unstable();

    // Make sure that
    // `0x89194e69d4fffff` (12-5-1-6-3-2-3-5-2-3) comes AFTER
    // `0x8a194e699ab7fff` (12-5-1-6-3-2-3-1-5-2-6) when sorting.
    assert_eq!(cells, expected);
}

#[test]
fn debug_impl() {
    assert_eq!(
        format!("{:?}", CellIndex::new_unchecked(0x802BFFFFFFFFFFF)),
        "21-777777777777777 (802bfffffffffff)"
    );
    assert_eq!(
        format!("{:?}", CellIndex::new_unchecked(0x8C2BAE305336BFF)),
        "21-656140514665777 (8c2bae305336bff)"
    );
    assert_eq!(
        format!("{:?}", CellIndex::new_unchecked(0x8F2834782B9C2AB)),
        "20-064360256341253 (8f2834782b9c2ab)"
    );
}

#[test]
// gometry: pins public-boundary ABI while exercising the raw carrier.
fn projected_boundary_keeps_public_boundary_and_paired_raw_seeds() {
    use core::mem::size_of;

    use crate::{Boundary, LatLng};

    let cell = CellIndex::new_unchecked(0x8101BFFFFFFFFFF);
    let projected = cell.boundary_with_seeds();

    // This is the frozen pre-sidecar layout.  The projection carrier must not
    // enlarge the public `Boundary` value copied by `CellIndex::boundary()`.
    assert_eq!(size_of::<Boundary>(), 168);

    // A manual public boundary built from frozen stored doubles proves that
    // the paired carrier did not change the public result.  Comparing
    // `boundary()` to `boundary_with_seeds().public` alone would be
    // tautological because the former delegates to the latter.
    let mut expected = Boundary::new();
    for (lat, lng) in [
        (4608065448697526987, 4606117890866068563),
        (4608133339173069226, 4607579327983253324),
        (4608437835581619178, 4607948982609115469),
        (4608705082211284320, 4606924285960815223),
        (4608574985539229724, 4604465222861442637),
        (4608256833593926307, 4604635963253136447),
        (4608167527875997405, 4605480909246894239),
    ] {
        expected.push(
            LatLng::from_radians(f64::from_bits(lat), f64::from_bits(lng))
                .expect("frozen finite boundary point"),
        );
    }
    assert_eq!(cell.boundary(), expected);
    assert_eq!(projected.projection_seeds().len(), projected.public.len());
    assert!(projected.projection_seeds().all(|seed| matches!(
        seed,
        crate::ProjectionSeed::Point { .. } | crate::ProjectionSeed::Intersection { .. }
    )));
    for (seed, point) in projected.projection_seeds().zip(projected.public.iter()) {
        let replay = seed.native_projection_point();
        assert_eq!(
            replay.lat_radians().to_bits(),
            point.lat_radians().to_bits()
        );
        assert_eq!(
            replay.lng_radians().to_bits(),
            point.lng_radians().to_bits()
        );
    }
}

#[test]
fn target_cap_profile_carries_all_native_suffixes_vertices_and_directed_seams() {
    use crate::{CapLatticeProfile, ProjectionSeed, Resolution};

    let profile = CapLatticeProfile::for_target(Resolution::Fifteen);
    assert_eq!(profile.target(), Resolution::Fifteen);
    assert_eq!(profile.adjusted_resolution(), 16);
    assert_eq!(profile.max_dim(), 11_529_602);
    assert!(profile.suffix_offsets(Resolution::Zero).is_none());
    assert_eq!(profile.suffix_offsets(Resolution::One).unwrap().len(), 42);
    assert_eq!(
        profile.suffix_offsets(Resolution::Fifteen).unwrap().len(),
        42
    );
    assert_eq!(profile.vertex_offsets().len(), 36);
    assert!(
        profile
            .vertex_offsets()
            .iter()
            .all(|offset| offset.x2 != 0 || offset.y_sqrt3 != 0)
    );
    assert_eq!(profile.seams().len(), 60);
    for seam in profile.seams() {
        let left = seam.left();
        let right = seam.right();
        let (
            ProjectionSeed::Point {
                face: left_face,
                resolution: left_resolution,
                substrate: left_substrate,
                ..
            },
            ProjectionSeed::Point {
                face: right_face,
                resolution: right_resolution,
                substrate: right_substrate,
                ..
            },
        ) = (left[0], right[0])
        else {
            panic!("cap seams are raw lattice point carriers");
        };
        assert_eq!(left[0].face(), left[1].face());
        assert_eq!(right[0].face(), right[1].face());
        assert_ne!(left_face, right_face);
        assert_eq!(left_resolution, 16);
        assert_eq!(right_resolution, 16);
        assert!(left_substrate && right_substrate);
    }
}

#[test]
fn projected_boundary_pins_raw_seed_order_and_insertion_edge_identity() {
    use crate::{Hex2Int, ProjectionSeed};

    let point = |face, x2, y_sqrt3, resolution| ProjectionSeed::Point {
        face,
        point: Hex2Int::new(x2, y_sqrt3),
        resolution,
        substrate: true,
    };
    let insertion = |face, segment, face_edge, resolution| ProjectionSeed::Intersection {
        face,
        segment,
        face_edge,
        resolution,
        substrate: true,
    };
    for (cell, expected, expected_edges) in [
        (
            0x82754FFFFFFFFFF,
            vec![
                point(9, -30, -32, 2),
                point(9, -33, -31, 2),
                point(9, -36, -32, 2),
                point(9, -36, -34, 2),
                point(9, -33, -35, 2),
                point(9, -30, -34, 2),
            ],
            vec![0, 1, 2, 3, 4, 5],
        ),
        (
            0x8101BFFFFFFFFFF,
            vec![
                point(0, 48, -10, 2),
                point(1, 42, 14, 2),
                point(1, 36, 10, 2),
                point(1, 39, 5, 2),
                point(1, 48, 4, 2),
                point(1, 54, 8, 2),
                insertion(
                    1,
                    [Hex2Int::new(54, 8), Hex2Int::new(51, 13)],
                    [Hex2Int::new(84, 0), Hex2Int::new(-42, 42)],
                    2,
                ),
            ],
            vec![0, 1, 2, 3, 4, 5, 5],
        ),
        (
            0x830136FFFFFFFFF,
            vec![
                point(0, 426, -52, 4),
                point(1, 420, 56, 4),
                point(1, 414, 52, 4),
                point(1, 417, 47, 4),
                point(1, 426, 46, 4),
                point(1, 432, 50, 4),
                insertion(
                    1,
                    [Hex2Int::new(432, 50), Hex2Int::new(429, 55)],
                    [Hex2Int::new(588, 0), Hex2Int::new(-294, 294)],
                    4,
                ),
            ],
            vec![0, 1, 2, 3, 4, 5, 5],
        ),
        (
            0x81083FFFFFFFFFF,
            vec![
                point(3, 75, 1, 2),
                insertion(
                    3,
                    [Hex2Int::new(75, 1), Hex2Int::new(78, -4)],
                    [Hex2Int::new(-42, -42), Hex2Int::new(84, 0)],
                    2,
                ),
                point(4, 75, 1, 2),
                insertion(
                    4,
                    [Hex2Int::new(75, 1), Hex2Int::new(78, -4)],
                    [Hex2Int::new(-42, -42), Hex2Int::new(84, 0)],
                    2,
                ),
                point(0, 75, 1, 2),
                insertion(
                    0,
                    [Hex2Int::new(75, 1), Hex2Int::new(78, -4)],
                    [Hex2Int::new(-42, -42), Hex2Int::new(84, 0)],
                    2,
                ),
                point(1, 75, 1, 2),
                insertion(
                    1,
                    [Hex2Int::new(75, 1), Hex2Int::new(78, -4)],
                    [Hex2Int::new(-42, -42), Hex2Int::new(84, 0)],
                    2,
                ),
                point(2, 75, 1, 2),
                insertion(
                    2,
                    [Hex2Int::new(75, 1), Hex2Int::new(78, -4)],
                    [Hex2Int::new(-42, -42), Hex2Int::new(84, 0)],
                    2,
                ),
            ],
            vec![0, 0, 1, 1, 2, 2, 3, 3, 4, 4],
        ),
        (
            0x820807FFFFFFFFF,
            vec![
                point(3, 81, 1, 2),
                point(4, 81, 1, 2),
                point(0, 81, 1, 2),
                point(0, 81, -1, 2),
                point(1, 81, -1, 2),
            ],
            vec![0, 1, 2, 3, 4],
        ),
    ] {
        let actual = CellIndex::new_unchecked(cell).boundary_with_seeds();
        assert_eq!(
            actual.projection_seeds().copied().collect::<Vec<_>>(),
            expected
        );
        assert_eq!(
            actual.projection_seed_edges().collect::<Vec<_>>(),
            expected_edges
        );
    }
}

#[test]
fn raw_target_seed_and_pole_owner_use_native_carriers() {
    let cell = CellIndex::new_unchecked(0x8101BFFFFFFFFFF);
    let seed = cell
        .target_substrate_center_seed(Resolution::Two)
        .expect("target is not coarser than source");
    assert!(matches!(seed, crate::ProjectionSeed::Point {
        substrate: true,
        ..
    }));
}
