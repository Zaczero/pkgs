use super::super::h3_lattice::{certified_face_projection, certified_profile_scale};
use super::*;

#[test]
fn raw_c_projection_radicands_stay_positive_at_pinned_face_and_resolution_pairs() {
    const RADICAND_HI_BITS: [u64; 5] = [
        0x3FF0_0000_0000_0003,
        0x3FF0_0000_0000_0004,
        0x3FF0_0000_0000_0003,
        0x3FF0_0000_0000_0003,
        0x3FF0_0000_0000_0003,
    ];
    for ((face, adjusted), expected_bits) in [(0, 0_u8), (7, 1), (19, 2), (2, 10), (15, 16)]
        .into_iter()
        .zip(RADICAND_HI_BITS)
    {
        let seed = ProjectionSeed::Point {
            face,
            point: Hex2Int::new(0, 0),
            resolution: adjusted,
            substrate: true,
        };
        let max_dim = 2 * 7_i64.pow(u32::from(adjusted / 2));
        let raw = CapLatticeProfile::for_target(Resolution::Zero);
        let scale = certified_profile_scale(&raw).unwrap();
        let certificate = certified_face_projection(&seed, max_dim, scale).unwrap();
        assert!(certificate.raw_c_radicand.strictly_positive());
        assert_eq!(certificate.raw_c_radicand.hi.to_bits(), expected_bits);
    }
}

#[test]
fn raw_intersection_parameter_is_positive_and_pins_horizontal_half() {
    // segment x=0..2 crossed by the vertical face edge x=1 has t=+1/2.
    let (numerator, denominator) =
        rational_intersection_parameter(0, 4, 2, 0, 1, -2).expect("proper crossing");
    assert_eq!((numerator, denominator), (1, 2));
    let point = intersection_native_point([Hex2Int::new(0, 0), Hex2Int::new(2, 0)], [
        Hex2Int::new(1, -2),
        Hex2Int::new(1, 2),
    ])
    .expect("proper native intersection");
    assert!(point.0.lo < 1.0 && point.0.hi > 1.0);
    assert!(point.1.contains_zero());
}

#[test]
fn h3_raw_seed_projects_without_roundtripping_through_latlng() {
    let cell = h3o::CellIndex::try_from(0x0810_1BFF_FFFF_FFFF_u64).unwrap();
    let boundary = cell.boundary_with_seeds();
    for (seed, public) in boundary.projection_seeds().zip(boundary.public.iter()) {
        let point = project_h3_seed(seed).expect("stored native seed is projectable");
        for component in [point.x, point.y, point.z] {
            assert!(component.lo.is_finite() && component.hi.is_finite());
        }
        let expected = [
            public.lat_radians().cos() * public.lng_radians().cos(),
            public.lat_radians().cos() * public.lng_radians().sin(),
            public.lat_radians().sin(),
        ];
        for (actual, expected) in [point.x, point.y, point.z].into_iter().zip(expected) {
            assert!(
                actual.lo <= expected && expected <= actual.hi,
                "seed={seed:?}, certified={actual:?}, public={expected:.17e}"
            );
        }
    }
}

#[test]
fn non_substrate_class_iii_q_rotation_replays_native_projector() {
    // H3 boundary seeds are substrate points.  This native seed exercises
    // the separate Class-III Q branch directly; `native_projection_point`
    // is a test oracle only, never a predicate input.
    let seed = ProjectionSeed::Point {
        face: 0,
        point: Hex2Int::new(48, -10),
        resolution: 1,
        substrate: false,
    };
    let actual = project_h3_seed(&seed).expect("finite native seed");
    let native = seed.native_projection_point();
    let expected = [
        native.lat_radians().cos() * native.lng_radians().cos(),
        native.lat_radians().cos() * native.lng_radians().sin(),
        native.lat_radians().sin(),
    ];
    for (component, expected) in [actual.x, actual.y, actual.z].into_iter().zip(expected) {
        assert!(
            component.lo <= expected && expected <= component.hi,
            "certified={component:?}, native={expected:.17e}"
        );
    }
}
