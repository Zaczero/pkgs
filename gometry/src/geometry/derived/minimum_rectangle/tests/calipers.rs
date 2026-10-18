#[test]
fn exact_projection_breaks_rounded_plateau() {
    let points = [
        point(0.0, 0.0),
        point(1.0, 1.0),
        point(0.999_999_999_999_989, 2.999_999_999_999_955_6),
        point(0.999_999_999_999_988_9, 2.999_999_999_999_956),
        point(0.999_999_999_999_986_8, 2.999_999_999_999_960_5),
    ];
    let direction = (2.0_f64.sqrt().recip(), 2.0_f64.sqrt().recip());
    assert_ne!(
        projection_delta_order(direction, points[2], points[3]),
        Ordering::Equal
    );
    assert_ne!(
        projection_delta_order(direction, points[3], points[4]),
        Ordering::Equal
    );

    let inversion = [
        point(0.0, 0.0),
        point(1.0, 1.0),
        point(0.999_999_999_999_978_5, 2.999_999_999_999_911),
        point(0.999_999_999_999_978_1, 2.999_999_999_999_911_6),
        point(0.999_999_999_999_976_2, 2.999_999_999_999_913_4),
    ];
    assert!(projection_delta_order(direction, inversion[2], inversion[3]).is_gt());
}

#[test]
fn exact_advance_crosses_the_rounded_support_plateau() {
    let points = [
        point(0.0, 0.0),
        point(1.0, 1.0),
        point(0.999_999_999_999_989, 2.999_999_999_999_955_6),
        point(0.999_999_999_999_988_9, 2.999_999_999_999_956),
        point(0.999_999_999_999_986_8, 2.999_999_999_999_960_5),
    ];
    let direction = (2.0_f64.sqrt().recip(), 2.0_f64.sqrt().recip());
    assert_eq!(advance_max(&points, direction, 1), 4);
}

#[rstest::rstest]
#[case::vertices_3(3)]
#[case::vertices_5(5)]
#[case::vertices_12(12)]
#[case::vertices_31(31)]
#[case::vertices_64(64)]
fn caliper_support_progression_handles_rotated_hulls(#[case] vertices: usize) {
    // `select_minimum_candidate`'s debug assertions require every
    // unwrapped pointer to stop at an exact local support maximum. The
    // affine family below exercises rotations, anisotropy, and offsets
    // across several hull sizes.

    for (angle, offset) in [(0.0_f64, 0.0), (0.37, 5.0e6), (1.17, -1.0e12)] {
        let (sin, cos) = angle.sin_cos();
        let points = (0..vertices)
            .map(|index| {
                let theta = std::f64::consts::TAU * index as f64 / vertices as f64;
                let x = 7.0 * theta.cos();
                let y = 2.0 * theta.sin();
                point(offset + cos * x - sin * y, -offset + sin * x + cos * y)
            })
            .collect::<Vec<_>>();
        let hull = hull_vertices(&points);
        assert_eq!(hull.len(), vertices);
        let _ = select_minimum_candidate(&hull);
    }
}

#[test]
fn sparse_wide_multiplication_propagates_carries_at_the_actual_top_limb() {
    let mut left = [0_u64; LIMBS];
    left[0] = u64::MAX;
    let mut right = [0_u64; LIMBS];
    right[0] = u64::MAX;
    right[2] = 1;
    let product: [u64; DOUBLE_LIMBS] = wide_product(&left, &right);
    assert_eq!(product[0], 1);
    assert_eq!(product[1], u64::MAX - 1);
    assert_eq!(product[2], u64::MAX);
    assert!(product[3..].iter().all(|&word| word == 0));
}

#[test]
fn score_divides_by_the_exact_rounded_basis_norm() {
    // Exact support widths alone choose edge zero on this fixture, while
    // the physical area `(da * do) / N` chooses edge one.
    let hull = [
        point(
            f64::from_bits(0x4010_2EB2_5C0F_1A90),
            f64::from_bits(0xC04F_EF9D_E158_CAB8),
        ),
        point(
            f64::from_bits(0x404A_A55F_8BBE_E593),
            f64::from_bits(0x4041_B844_B820_564B),
        ),
        point(
            f64::from_bits(0xC04C_AB35_D740_C8E3),
            f64::from_bits(0x403C_6EB2_5270_E8CA),
        ),
    ];
    assert_eq!(select_minimum_candidate(&hull).supports.outward_min, 1);
}

#[test]
fn exact_equal_area_candidates_keep_the_first_hull_edge() {
    let a = point(0.0, 0.0);
    let d = point(1.0, -1.0);
    let c = point(2.0, 0.0);
    let b = point(1.0, 1.0);
    let hull = [a, d, c, b];
    let first_basis = Basis::from_edge(a, d).expect("nonzero first edge");
    let second_basis = Basis::from_edge(d, c).expect("nonzero second edge");
    let mut first = RankedCandidate::new(&hull, Candidate {
        basis: first_basis,
        supports: Supports {
            along_min: 0,
            along_max: 1,
            outward_min: 0,
            outward_max: 2,
        },
    });
    let mut second = RankedCandidate::new(&hull, Candidate {
        basis: second_basis,
        supports: Supports {
            along_min: 0,
            along_max: 2,
            outward_min: 1,
            outward_max: 0,
        },
    });
    assert_eq!(
        candidate_area_order(&hull, &mut first, &mut second),
        Ordering::Equal
    );

    let selected = select_minimum_candidate(&hull);
    assert_eq!(
        selected.basis.along.0.to_bits(),
        first_basis.along.0.to_bits()
    );
    assert_eq!(
        selected.basis.along.1.to_bits(),
        first_basis.along.1.to_bits()
    );
}
