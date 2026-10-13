use super::*;

#[test]
fn target_lattice_profile_encloses_every_vendor_suffix_and_rotation() {
    let target = Resolution::Fifteen;
    let raw = CapLatticeProfile::for_target(target);
    let sqrt3_2 = certified_profile_sqrt3_2(&raw, 16)
        .expect("all paired profile seams preserve the native constant");
    for resolution in Resolution::range(Resolution::One, target) {
        for (index, point) in raw.suffix_offsets(resolution).unwrap().iter().enumerate() {
            assert!(
                certified_lattice_norm(*point, sqrt3_2).is_some(),
                "target suffix {resolution:?} offset {index} must have an outward norm"
            );
        }
        assert!(
            certified_lattice_max(raw.suffix_offsets(resolution).unwrap(), sqrt3_2).is_some(),
            "target suffix {resolution:?} must stay finite"
        );
    }
    assert!(certified_lattice_max(raw.vertex_offsets(), sqrt3_2).is_some());
    let half = Bound::exact(0.5).unwrap();
    let b = Bound::exact(-3.0)
        .unwrap()
        .div(Bound::exact(4.0).unwrap().mul(sqrt3_2).unwrap())
        .unwrap();
    let g00 = half
        .square()
        .unwrap()
        .add_nonnegative(sqrt3_2.square().unwrap())
        .unwrap();
    let g01 = half
        .mul(b)
        .unwrap()
        .add(sqrt3_2.mul(half).unwrap())
        .unwrap();
    let g11 = b
        .square()
        .unwrap()
        .add_nonnegative(half.square().unwrap())
        .unwrap();
    let discriminant_input = g00
        .sub(g11)
        .unwrap()
        .square()
        .unwrap()
        .add_nonnegative(
            Bound::exact(4.0)
                .unwrap()
                .mul_nonnegative(g01.square().unwrap())
                .unwrap(),
        )
        .unwrap();
    let discriminant = discriminant_input
        .sqrt()
        .expect("the non-negative Gram discriminant has an outward root");
    let eigenvalue = g00
        .add_nonnegative(g11)
        .unwrap()
        .add_nonnegative(discriminant)
        .unwrap()
        .div_nonnegative(Bound::exact(2.0).unwrap())
        .unwrap();
    eigenvalue
        .sqrt()
        .expect("the upper Gram eigenvalue has an outward root");
    assert!(certified_native_rotation_norm(sqrt3_2).is_some());
    let profile = CertifiedTargetLatticeProfile::for_target(target)
        .expect("the fixed vendor target profile is finite");
    let at_root = profile
        .native_descendant_radius(Resolution::Zero)
        .expect("root is an ancestor of the target");
    let at_leaf = profile
        .native_descendant_radius(target)
        .expect("target is its own ancestor");
    assert!(at_root.hi > at_leaf.hi);
    assert!(at_leaf.hi > 0.0);
    assert!(profile.rotation_norm().hi > 0.0);
    assert_eq!(profile.raw().seams().len(), 60);
    assert_eq!(
        profile
            .raw()
            .suffix_offsets(Resolution::Fifteen)
            .unwrap()
            .len(),
        42
    );
}

#[test]
fn descendant_windows_use_the_target_substrate_center_for_named_spills() {
    let parent = h3o::CellIndex::try_from(0x0800_9FFF_FFFF_FFFF_u64).unwrap();
    for raw in [0x0810_93FF_FFFF_FFFF_u64, 0x0810_97FF_FFFF_FFFF_u64] {
        let target = h3o::CellIndex::try_from(raw).unwrap();
        assert_eq!(target.parent(parent.resolution()), Some(parent));
    }
    let cap = CertifiedH3DescendantCap::for_target(Resolution::One).unwrap();
    assert!(
        CertifiedCapCenter::for_target_cell(
            parent,
            Resolution::One,
            cap.adjusted_resolution,
            cap.max_dim,
        )
        .is_some(),
        "the cap must enter the target-child substrate path, not the ordinary parent centre"
    );

    // The former `Boundary`/`Windows` oracle failed under the ordinary
    // parent mutation for an accidental longitude-envelope reason.  These
    // rays coincide exactly, so only target-child provenance observes the
    // aperture-7 distinction.
    let required = Hex2Int::new(84, 0);
    let ordinary_parent = Hex2Int::new(12, 0);
    assert!(certified_center_seed_is_admissible(
        required,
        2,
        true,
        cap.adjusted_resolution,
        cap.max_dim,
    ));
    assert!(!certified_center_seed_is_admissible(
        ordinary_parent,
        0,
        true,
        cap.adjusted_resolution,
        cap.max_dim,
    ));
}

#[test]
fn angular_radius_composition_keeps_three_seam_charges_and_k_cubed() {
    let radius = certified_angular_radius(
        Bound::exact(2.0).unwrap(),
        Bound::exact(3.0).unwrap(),
        Bound::exact(125.0).unwrap(),
        Bound::exact(7.0).unwrap(),
        Bound::exact(11.0).unwrap(),
    )
    .unwrap();
    assert!(radius.lo < 5283.0 && radius.hi > 5283.0);
}

#[test]
fn cap_latitude_envelope_is_outward_before_it_can_prune() {
    let latitude = Bound::exact(0.1).unwrap();
    let radius = Bound::exact(0.2).unwrap();
    let (south, north, absolute) = certified_cap_latitude_bounds(latitude, radius).unwrap();
    let rounded_sum = 0.1 + 0.2;
    assert!(south.lo < -0.1 && south.hi > -0.1);
    assert!(north.lo < rounded_sum && north.hi > rounded_sum);
    assert!(absolute.lo < rounded_sum && absolute.hi > rounded_sum);
}

#[test]
fn generic_cap_fallback_covers_every_bilateral_pole_owner_ancestor() {
    const POLE_OWNERS: [(u64, u64); H3_RESOLUTION_COUNT] = [
        (0x0800_1FFF_FFFF_FFFF, 0x080F_3FFF_FFFF_FFFF),
        (0x0810_33FF_FFFF_FFFF, 0x081F_2BFF_FFFF_FFFF),
        (0x0820_327F_FFFF_FFFF, 0x082F_297F_FFFF_FFFF),
        (0x0830_326F_FFFF_FFFF, 0x083F_293F_FFFF_FFFF),
        (0x0840_3263_FFFF_FFFF, 0x084F_2939_FFFF_FFFF),
        (0x0850_3262_3FFF_FFFF, 0x085F_2938_3FFF_FFFF),
        (0x0860_3262_37FF_FFFF, 0x086F_2938_0FFF_FFFF),
        (0x0870_3262_33FF_FFFF, 0x087F_2938_0EFF_FFFF),
        (0x0880_3262_33BF_FFFF, 0x088F_2938_0E1F_FFFF),
        (0x0890_3262_33AB_FFFF, 0x089F_2938_0E0F_FFFF),
        (0x08A0_3262_33AB_7FFF, 0x08AF_2938_0E0D_7FFF),
        (0x08B0_3262_33AB_0FFF, 0x08BF_2938_0E0D_0FFF),
        (0x08C0_3262_33AB_03FF, 0x08CF_2938_0E0D_0DFF),
        (0x08D0_3262_33AB_03BF, 0x08DF_2938_0E0D_0CFF),
        (0x08E0_3262_33AB_039F, 0x08EF_2938_0E0D_0CC7),
        (0x08F0_3262_33AB_0399, 0x08FF_2938_0E0D_0CC4),
    ];
    for (target, (north, south)) in
        Resolution::range(Resolution::Zero, Resolution::Fifteen).zip(POLE_OWNERS)
    {
        let cap = CertifiedH3DescendantCap::for_target(target).unwrap();
        for (owner, northern) in [(north, true), (south, false)] {
            let owner = h3o::CellIndex::try_from(owner).unwrap();
            for depth in Resolution::range(Resolution::Zero, target) {
                let cell = owner.parent(depth).unwrap();
                let DegreeWindowResult::Windows(CertifiedDegreeWindows {
                    latitude,
                    longitude: CertifiedLongitudeDegrees::Full,
                }) = cap.descendant_windows(cell)
                else {
                    panic!("{target:?} {depth:?} must take the generic pole fallback");
                };
                assert!(if northern {
                    latitude.hi >= 90.0
                } else {
                    latitude.lo <= -90.0
                });
            }
        }
    }
}

#[test]
fn six_native_rotation_powers_have_frozen_outward_norms() {
    let raw = CapLatticeProfile::for_target(Resolution::Fifteen);
    let sqrt3_2 = certified_profile_sqrt3_2(&raw, 16).unwrap();
    let powers = certified_native_rotation_power_norms(sqrt3_2).unwrap();
    assert_eq!(powers.map(|norm| norm.hi.to_bits()), [
        0x3FF0_0000_0000_0003,
        0x3FF0_0000_0000_0007,
        0x3FF0_0000_0000_000C,
        0x3FF0_0000_0000_000F,
        0x3FF0_0000_0000_001D,
        0x3FF0_0000_0000_002A,
    ]);
}

#[test]
fn cap_profile_coupling_rejects_a_synthetic_larger_face() {
    let target = Resolution::Ten;
    let adjusted = target_value(target).unwrap() + u8::from(target.is_class3());
    let dimension = 2 * 7_i64.pow(u32::from(adjusted / 2));
    assert!(!certified_profile_is_coupled(
        target,
        adjusted,
        dimension + 1,
        60,
        target,
    ));
}

#[test]
fn strict_face_crossing_refuses_an_exact_unit_ratio() {
    assert_eq!(
        certified_strict_face_ratio(Bound::exact(1.0).unwrap()),
        None
    );
}

#[test]
fn center_certificate_requires_the_substrate_carrier_and_target_resolution() {
    let point = Hex2Int::new(0, 0);
    assert!(!certified_center_seed_is_admissible(point, 2, false, 2, 14));
    assert!(!certified_center_seed_is_admissible(point, 1, true, 2, 14));
}

#[test]
fn descendant_cap_certifies_every_target_depth_below_a_hemisphere() {
    for target in Resolution::range(Resolution::Zero, Resolution::Fifteen) {
        let lattice = CertifiedTargetLatticeProfile::for_target(target)
            .unwrap_or_else(|| panic!("target {target:?} needs a finite lattice profile"));
        let scale = certified_profile_scale(lattice.raw())
            .unwrap_or_else(|| panic!("target {target:?} needs a finite projection scale"));
        for (index, seam) in lattice.raw().seams().iter().enumerate() {
            for seed in seam.left().into_iter().chain(seam.right()) {
                let certificate = certified_face_projection(&seed, lattice.raw().max_dim(), scale)
                    .unwrap_or_else(|| {
                        panic!("target {target:?} seam {index} needs a finite face projection")
                    });
                assert!(
                    certificate.minimum_denominator.strictly_positive(),
                    "target {target:?} seam {index} needs a positive native-C projection denominator"
                );
                assert!(
                    certificate.basis_norm.hi > 0.0,
                    "target {target:?} seam {index} needs a non-degenerate projection basis"
                );
            }
        }
        let stretch = certified_chart_stretch(lattice.raw(), scale)
            .unwrap_or_else(|| panic!("target {target:?} needs a finite chart stretch"));
        assert!(stretch.hi > 0.0);
        let seam_cost = certified_seam_cost(lattice.raw(), scale)
            .unwrap_or_else(|| panic!("target {target:?} needs a finite seam cost"));
        assert!(seam_cost.hi > 0.0);
        let cap = CertifiedH3DescendantCap::for_target(target)
            .unwrap_or_else(|| panic!("target {target:?} needs a finite projection and seam cap"));
        assert_eq!(cap.target(), target);
        for depth in Resolution::range(Resolution::Zero, target) {
            let ideal = certified_ideal_face_ratio(target, depth).unwrap_or_else(|| {
                panic!("target {target:?}, depth {depth:?} needs a strict face-crossing proof")
            });
            assert!(ideal.hi < 1.0);
            let radius = cap.angular_radius(depth).unwrap_or_else(|| {
                panic!("target {target:?}, depth {depth:?} needs a sub-hemisphere cap")
            });
            assert!(radius.hi < HALF_PI_LO);
        }
    }
}

#[test]
fn cap_radius_uses_its_certified_outer_halfwidth_not_an_arc_endpoint_pair() {
    let target = Resolution::Two;
    let cap = CertifiedH3DescendantCap::for_target(target).unwrap();
    let windows = h3o::CellIndex::base_cells()
        .filter(|&cell| matches!(cap.descendant_windows(cell), DegreeWindowResult::Windows(_)))
        .count();
    assert_eq!(
        windows, 122,
        "a correlated [0, R] cap radius retains every native r2 root window"
    );
}

#[test]
fn descendant_window_certificate_rejects_unprofiled_centers() {
    for target in Resolution::range(Resolution::Zero, Resolution::Fifteen) {
        let cap = CertifiedH3DescendantCap::for_target(target)
            .unwrap_or_else(|| panic!("target {target:?} has an atomic cap"));
        let invalid = Hex2Int::new(6 * cap.max_dim + 2, 0);
        assert!(
            !certified_center_seed_is_admissible(
                invalid,
                cap.adjusted_resolution,
                true,
                cap.adjusted_resolution,
                cap.max_dim,
            ),
            "an IJK point beyond the certified face domain must not enter a window certificate"
        );
    }
}

#[test]
fn cap_halfwidth_uses_the_correlated_upper_endpoint() {
    let center = Bound::exact(0.0).unwrap();
    let radius = Bound::new(0.0, 1.0).unwrap();
    assert!(
        matches!(
            cap_longitude_span(center, radius),
            Some(CertifiedLongitudeRad::Span(_))
        ),
        "the cap's upper radius endpoint creates an ordered outward span"
    );
}

#[test]
fn ideal_face_crossing_stays_strict_at_the_pinned_profile_depths() {
    for (depth, target) in [(0, 1), (0, 2), (1, 3), (4, 5), (4, 10), (0, 15), (14, 15)] {
        let target = Resolution::try_from(target).expect("stored H3 resolution");
        let depth = Resolution::try_from(depth).expect("stored H3 resolution");
        let ratio = certified_ideal_face_ratio(target, depth)
            .expect("the stored finite profile proves fewer than three face-family crossings");
        assert!(ratio.hi < 1.0, "target {target:?}, depth {depth:?}");
    }
}
