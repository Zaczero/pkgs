use super::*;

#[test]
fn source_endpoint_carrier_certifies_the_seam_contact_exclusion() {
    use crate::geometry::{CoordSeq, LineSeq, Point, Shape};
    use crate::grid::affine_source::{GeographicCoverageSource, SphericalGridTarget};

    let source = GeographicCoverageSource::new(
        &Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    Point::new(170.0, -10.0).unwrap(),
                    Point::new(-170.0, 10.0).unwrap(),
                ])
                .unwrap(),
            )
            .expect("line coordinate sequence must contain at least two vertices"),
        ),
        SphericalGridTarget::H3(Resolution::Seven),
    )
    .unwrap();
    let cell = h3o::CellIndex::try_from(0x0877_EB57_26FF_FFFF_u64).unwrap();
    let source_arc = source.authority_pieces().unwrap().next().unwrap().arc();
    let endpoint = Bound::exact(source_arc.domain.hi).unwrap();
    assert_eq!(
        source_arc.endpoint_identity_at(endpoint),
        Some(AffineEndpointIdentity {
            longitude: Some(AxisMeridian::Antimeridian),
            latitude: Some(AxisLatitude::Equator),
        }),
        "the exact rational seam split, not its rounded enclosure, carries both axes"
    );
    let arcs = h3_ordered_arcs(cell).unwrap();
    // These are the two seam-side pieces whose overlap terminates at the
    // source parent endpoint.  Its outward longitude `hi` is not a new
    // exact longitude on the parent map, so direct inverse evaluation is
    // deliberately unavailable; the endpoint carrier supplies the sign.
    for (index, shift) in [(1, 0), (5, 1)] {
        let arc = arcs
            .get(index)
            .unwrap()
            .with_longitude_shift(shift)
            .unwrap();
        let LongitudeOverlap::Contact { outer, .. } = source_arc.longitude_span().unwrap().overlap(
            arc.longitude
                .and_then(DirectedLongitudeSpan::envelope)
                .unwrap(),
        ) else {
            panic!("fixture must enter the certified seam overlap");
        };
        assert!(
            plane_at_longitude(&source_arc, &arc, outer.hi).is_none(),
            "the rounded enclosure endpoint is intentionally not authority"
        );
        let endpoint_plane = plane_at_overlap_longitude(&source_arc, &arc, outer.hi);
        assert!(
            endpoint_plane
                .is_some_and(|value| value.strictly_positive() || value.strictly_negative()),
            "the retained parent endpoint proves the missing strict sign"
        );
        assert_eq!(
            classify_general_contact(&source_arc, &arc),
            ArcContact::None,
            "the endpoint carrier excludes the non-contacting seam cell"
        );
    }
}

#[test]
fn arc_contact_searches_the_source_piece_own_periodic_sheet() {
    let source = test_affine_source_arc(800.0, 0.0, 900.0, 0.0)
        .expect("a finite lifted equatorial source piece");
    let arc = OrderedMinorArc::from_test_points([0.0, 1.0, 0.0], [-1.0, 0.0, 0.0]);
    let (first, last) = source_arc_shift_range(&source, &arc)
        .expect("the finite source piece has a bounded periodic-copy range");
    assert!(
        first <= 2 && 2 <= last,
        "the 800..900 degree source piece needs the arc's +2 sheet"
    );
    assert_ne!(
        classify_arc_contact(&source, &arc),
        ArcContact::None,
        "the exact relation must not turn a remote periodic source contact into a negative"
    );
}

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
fn ideal_face_crossing_stays_strict_at_the_pinned_profile_depths() {
    for (depth, target) in [(0, 1), (0, 2), (1, 3), (4, 5), (4, 10), (0, 15), (14, 15)] {
        let target = Resolution::try_from(target).expect("stored H3 resolution");
        let depth = Resolution::try_from(depth).expect("stored H3 resolution");
        let ratio = certified_ideal_face_ratio(target, depth)
            .expect("the stored finite profile proves fewer than three face-family crossings");
        assert!(ratio.hi < 1.0, "target {target:?}, depth {depth:?}");
    }
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
fn strict_face_crossing_refuses_an_exact_unit_ratio() {
    assert_eq!(
        certified_strict_face_ratio(Bound::exact(1.0).unwrap()),
        None
    );
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
fn ordered_arc_latitude_includes_an_interior_analytic_extremum() {
    // The plane is y=z.  Its north-axis projection is the interior ray
    // (0, 1, 1), above both unit endpoint rays (±1/3, 2/3, 2/3).
    let arc = OrderedMinorArc::from_test_points([1.0 / 3.0, 2.0 / 3.0, 2.0 / 3.0], [
        -1.0 / 3.0,
        2.0 / 3.0,
        2.0 / 3.0,
    ]);
    let endpoints = [
        certified_vector_latitude(arc.start).unwrap(),
        certified_vector_latitude(arc.end).unwrap(),
    ];
    let bounds = certified_arc_latitude_bounds(&arc).unwrap();
    assert!(bounds.hi > endpoints[0].hi.max(endpoints[1].hi));
    assert!(bounds.hi > HALF_PI_LO * 0.5);
}

#[test]
fn polar_h3_witness_has_an_arc_latitude_extremum_beyond_its_vertices() {
    let cell = h3o::CellIndex::try_from(0x0820_377F_FFFF_FFFF_u64).unwrap();
    let arcs = h3_ordered_arcs(cell).unwrap();
    let mut vertex_max = -HALF_PI_HI;
    let mut arc_max = -HALF_PI_HI;
    for arc in arcs.iter() {
        for endpoint in [arc.start, arc.end] {
            vertex_max = vertex_max.max(certified_vector_latitude(endpoint).unwrap().hi);
        }
        arc_max = arc_max.max(certified_arc_latitude_bounds(arc).unwrap().hi);
    }
    assert!(arc_max > vertex_max);
}

#[test]
fn directed_h3_bbox_retains_the_vendor_2pi_sheet() {
    // The R0 polar root is Full; the next three roots exercise each
    // canonical seam form.  The remaining cells force the same carrier
    // through ordinary, polar-extremum, pentagon, and Class-III paths.
    // Every assertion calls the bbox certificate directly, before any
    // traversal can turn uncertainty into a conservative candidate.
    for (raw, expected) in [
        (0x0800_1FFF_FFFF_FFFF_u64, 0_u8),
        // Ordinary non-pentagon away from both poles.  It crosses the
        // degree seam, so erasing the accumulated lift makes this exact
        // bbox fail open to `Boundary`.
        (0x0805_BFFF_FFFF_FFFF_u64, 2_u8),
        (0x0800_3FFF_FFFF_FFFF_u64, 2_u8),
        (0x0800_5FFF_FFFF_FFFF_u64, 2_u8),
        (0x0800_DFFF_FFFF_FFFF_u64, 2_u8),
        (0x0810_1BFF_FFFF_FFFF_u64, 1_u8),
        (0x0820_377F_FFFF_FFFF_u64, 1_u8),
        (0x0827_54FF_FFFF_FFFF_u64, 1_u8),
        (0x0830_136F_FFFF_FFFF_u64, 1_u8),
    ] {
        let cell = h3o::CellIndex::try_from(raw).unwrap();
        let arcs = h3_ordered_arcs(cell).unwrap();
        let DegreeWindowResult::Windows(CertifiedDegreeWindows { longitude, .. }) =
            exact_h3_bbox(&arcs)
        else {
            panic!("{cell:?} must retain a certifiable directed longitude sheet");
        };
        assert_eq!(
            match longitude {
                CertifiedLongitudeDegrees::Full => 0,
                CertifiedLongitudeDegrees::One(_) => 1,
                CertifiedLongitudeDegrees::Two(_) => 2,
            },
            expected,
            "{cell:?} must retain its canonical degree seam form"
        );
    }
}

#[test]
fn r15_raw_seam_carrier_pins_matching_and_reversed_endpoint_extrema() {
    // These are RAW, unnormalised H3 projection carriers at R15/adjusted
    // substrate R16.  They are not the degree-LatLng/unit-XYZ roundtrip.
    let raw = CapLatticeProfile::for_target(Resolution::Fifteen);
    assert_eq!(raw.adjusted_resolution(), 16);
    let mut matching_max = Bound::exact(0.0).unwrap();
    for (left, right) in certified_matching_seam_pairs(&raw) {
        let delta = certified_seam_delta(&left, &right).unwrap();
        matching_max = Bound::new(0.0, matching_max.hi.max(delta.hi)).unwrap();
    }
    let mut reversed_min = Bound::new(f64::MAX, f64::MAX).unwrap();
    for seam in raw.seams() {
        for (left, right) in seam.left().into_iter().zip(seam.right().into_iter().rev()) {
            let delta = certified_seam_delta(&left, &right).unwrap();
            reversed_min = Bound::new(reversed_min.lo.min(delta.lo), f64::MAX).unwrap();
        }
    }
    assert_eq!(matching_max.hi.to_bits(), 0x3D41_32D6_5721_E925);
    assert_eq!(reversed_min.lo.to_bits(), 0x3FF5_2BB3_566A_4667);
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
fn center_certificate_requires_the_substrate_carrier_and_target_resolution() {
    let point = Hex2Int::new(0, 0);
    assert!(!certified_center_seed_is_admissible(point, 2, false, 2, 14));
    assert!(!certified_center_seed_is_admissible(point, 1, true, 2, 14));
}

#[test]
fn bounds_are_outward_and_division_refuses_zero() {
    let one = Bound::exact(1.0).unwrap();
    let three = Bound::exact(3.0).unwrap();
    let quotient = one.div(three).unwrap();
    assert!(quotient.lo < 1.0 / 3.0 && quotient.hi > 1.0 / 3.0);
    assert!(one.div(Bound::new(-1.0, 1.0).unwrap()).is_none());
}

#[test]
fn square_root_requires_outward_multiplication_witnesses() {
    let four = Bound::exact(4.0).unwrap();
    assert!(verified_sqrt_enclosure(four, 2.0_f64.next_up(), 3.0).is_none());
    assert!(verified_sqrt_enclosure(four, 1.0, 2.0_f64.next_down()).is_none());

    let root = Bound::new(2.0, 9.0).unwrap().sqrt().unwrap();
    assert!(root.lo <= 2.0_f64.sqrt() && 3.0_f64.sqrt() <= root.hi);
    assert_eq!(Bound::exact(0.0).unwrap().sqrt(), Bound::exact(0.0));
}

#[test]
fn degree_windows_are_outward_and_refuse_an_ambiguous_seam() {
    let radians = |degrees| degrees_to_radians_bound(degrees).unwrap();
    let split = CertifiedCircularBoundsRad {
        latitude: radians(10.0),
        longitude: CertifiedLongitudeRad::Span(
            LongitudeSpan::ordered(radians(170.0), radians(190.0)).unwrap(),
        ),
    };
    let DegreeWindowResult::Windows(CertifiedDegreeWindows {
        latitude,
        longitude: CertifiedLongitudeDegrees::Two([west, east]),
    }) = split.to_degree_windows()
    else {
        panic!("a strictly ordered +180 crossing has two certified windows");
    };
    assert!(latitude.lo <= 10.0 && 10.0 <= latitude.hi);
    assert!(west.lo <= 170.0 && 180.0 <= west.hi);
    assert!(east.lo <= -180.0 && -170.0 <= east.hi);

    let seam = CertifiedCircularBoundsRad {
        latitude: Bound::exact(0.0).unwrap(),
        longitude: CertifiedLongitudeRad::Span(
            LongitudeSpan::ordered(radians(180.0), radians(180.0)).unwrap(),
        ),
    };
    assert_eq!(seam.to_degree_windows(), DegreeWindowResult::Boundary);
}

#[test]
fn directed_longitude_refuses_a_lower_pi_boundary_enclosure() {
    // `-PI_LO` is the stored-double upper bound of -π.  Treating it as
    // strictly inside would let a half-circle establish a directed sheet.
    let lower_boundary = Bound::exact(-PI_LO).unwrap();
    assert!(!strict_minor_longitude_delta(lower_boundary));
    assert!(strict_minor_longitude_delta(Bound::exact(0.0).unwrap()));
}

#[test]
fn ball_sign_never_decides_through_zero() {
    assert_eq!(Ball::new(1.0, 0.25).unwrap().sign(), CertSign::Positive);
    assert_eq!(Ball::new(-1.0, 0.25).unwrap().sign(), CertSign::Negative);
    assert_eq!(Ball::new(0.0, 0.25).unwrap().sign(), CertSign::Uncertain);
}

#[test]
fn trig_encloses_fast_path_without_libm_in_the_carrier() {
    for value in [-0.7, -0.2, 0.0, 0.2, 0.7, 1.0, -1.0] {
        let (sin, cos) = sin_cos(Bound::exact(value).unwrap()).unwrap();
        assert!(sin.bound().unwrap().lo <= value.sin());
        assert!(sin.bound().unwrap().hi >= value.sin());
        assert!(cos.bound().unwrap().lo <= value.cos());
        assert!(cos.bound().unwrap().hi >= value.cos());
    }
}

#[test]
fn trig_rejects_enclosed_quadrant_boundary() {
    let boundary = Bound::new(HALF_PI_LO, HALF_PI_HI).unwrap();
    assert!(sin_cos(boundary).is_none());
}

#[test]
fn trig_rejects_an_enclosed_period_boundary_but_keeps_exact_zero() {
    assert!(sin_cos(degrees_to_radians_bound(360.0).unwrap()).is_none());
    let (sin, cos) = sin_cos(Bound::exact(0.0).unwrap()).expect("structural zero");
    assert!(sin.bound().unwrap().contains_zero());
    assert!(cos.bound().unwrap().lo <= 1.0 && cos.bound().unwrap().hi >= 1.0);
}

#[test]
fn trig_fallback_keeps_an_outward_pi_over_four_enclosure() {
    let input = degrees_to_radians_bound(45.0).unwrap();
    let (sin, cos) =
        sin_cos(input).expect("the degree conversion only spans an artificial pi/4 cut");
    let value = (PI_LO + PI_HI) * 0.125;
    assert!(sin.bound().unwrap().lo <= value.sin());
    assert!(sin.bound().unwrap().hi >= value.sin());
    assert!(cos.bound().unwrap().lo <= value.cos());
    assert!(cos.bound().unwrap().hi >= value.cos());
}

#[test]
fn contact_order_is_fail_open() {
    assert_eq!(
        ArcContact::None.combine(ArcContact::ClosedOnly),
        ArcContact::ClosedOnly
    );
    assert_eq!(
        ArcContact::ClosedOnly.combine(ArcContact::Uncertain),
        ArcContact::Uncertain
    );
    assert_eq!(
        ArcContact::Uncertain.combine(ArcContact::Open),
        ArcContact::Open
    );
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
fn h3_region_certificate_covers_the_complete_rectangle() {
    let cell = h3o::CellIndex::try_from(0x0827_54FF_FFFF_FFFF_u64).unwrap();
    let center = h3o::LatLng::from(cell);
    let window = H3BoundingWindow {
        west: center.lng() - 0.01,
        south: center.lat() - 0.01,
        east: center.lng() + 0.01,
        north: center.lat() + 0.01,
    };
    assert!(h3_cell_contains_windows(cell, &[window]));
    assert!(!h3_cell_contains_windows(cell, &[H3BoundingWindow {
        west: center.lng() - 1.0,
        south: center.lat() - 1.0,
        east: center.lng() + 1.0,
        north: center.lat() + 1.0,
    }]));
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
fn ordered_h3_arcs_preserve_class_iii_face_insertions() {
    let cell = h3o::CellIndex::try_from(0x0810_1BFF_FFFF_FFFF_u64).unwrap();
    let boundary = cell.boundary_with_seeds();
    let arcs = h3_ordered_arcs(cell).expect("every stored H3 boundary seed is an ordered arc");

    assert_eq!(boundary.public.len(), 7);
    assert_eq!(arcs.len(), boundary.public.len());
    assert_eq!(arcs.iter().count(), arcs.len());
    assert!(arcs.len() <= MAX_H3_BOUNDARY_ARCS);
    assert!(matches!(
        arcs.get(6).expect("closing arc").endpoints[1],
        PhysicalEndpointKey::CellVertex(CellVertexKey::H3 { ordinal: 0, .. })
    ));
    assert!(arcs.iter().any(|arc| {
        arc.endpoints.iter().any(|endpoint| {
            matches!(
                endpoint,
                PhysicalEndpointKey::CellVertex(CellVertexKey::H3Insertion { .. })
            )
        })
    }));
    assert!(matches!(
        arcs.get(5).expect("preceding arc").endpoints[1],
        PhysicalEndpointKey::CellVertex(CellVertexKey::H3Insertion {
            edge: 5,
            ordinal: 6,
            ..
        })
    ));
    let plan = h3_cell_plan(cell, cell.resolution());
    assert_eq!(plan.cell, cell);
    assert_eq!(plan.arcs.as_ref().map(H3ArcSet::len), Some(arcs.len()));
    assert!(matches!(plan.bbox, DegreeWindowResult::Windows(_)));
    assert!(matches!(plan.fan(), H3FanPlan::Certified(_)));
    let mismatched = h3_cell_plan(cell, Resolution::Two);
    assert!(mismatched.arcs.is_none());
    assert_eq!(mismatched.bbox, DegreeWindowResult::Boundary);
    assert!(matches!(mismatched.fan(), H3FanPlan::Uncertain));
    assert!(matches!(h3_fan_plan(cell), H3FanPlan::Certified(_)));
}

#[test]
fn ordered_h3_arcs_certify_ordinary_pentagon_class_iii_and_both_poles() {
    let cells = [
        h3o::CellIndex::try_from(0x0827_54FF_FFFF_FFFF_u64).unwrap(),
        h3o::CellIndex::try_from(0x0810_1BFF_FFFF_FFFF_u64).unwrap(),
        h3o::CellIndex::try_from(0x0830_136F_FFFF_FFFF_u64).unwrap(),
        h3o::CellIndex::try_from(0x0810_83FF_FFFF_FFFF_u64).unwrap(),
        h3o::CellIndex::try_from(0x0820_327F_FFFF_FFFF_u64).unwrap(),
        h3o::CellIndex::try_from(0x082F_297F_FFFF_FFFF_u64).unwrap(),
    ];
    for cell in cells {
        let raw = cell.boundary_with_seeds();
        let arcs = h3_ordered_arcs(cell).expect("raw ordered H3 boundary is certified");
        assert_eq!(arcs.len(), raw.public.len());
        assert_eq!(arcs.iter().count(), arcs.len());
        assert!(arcs.len() <= MAX_H3_BOUNDARY_ARCS);
        assert!(matches!(h3_fan_plan(cell), H3FanPlan::Certified(_)));
    }
}

#[test]
fn h3_reflex_vertex_uses_adjacent_triangle_union() {
    for raw in [0x0810_1BFF_FFFF_FFFF_u64, 0x0830_136F_FFFF_FFFF_u64] {
        let cell = h3o::CellIndex::try_from(raw).unwrap();
        let arcs = h3_ordered_arcs(cell).expect("raw H3 arcs");
        let center = project_h3_seed(
            &cell
                .target_substrate_center_seed(cell.resolution())
                .expect("target substrate centre"),
        )
        .expect("certified centre");
        let vertex = arcs.get(6).expect("fixture reflex arc");
        let previous = arcs.get(5).expect("fixture preceding arc");
        let candidate = vertex
            .start
            .scale(Bound::exact(0.999).unwrap())
            .and_then(|value| {
                center
                    .scale(Bound::exact(0.0005).unwrap())
                    .and_then(|center| value.add(center))
            })
            .and_then(|value| {
                previous
                    .start
                    .scale(Bound::exact(0.0005).unwrap())
                    .and_then(|previous| value.add(previous))
            })
            .expect("finite reflex tangent proposal");
        let tangent = Vec3Ball {
            x: Ball::from_bound(candidate.x).unwrap(),
            y: Ball::from_bound(candidate.y).unwrap(),
            z: Ball::from_bound(candidate.z).unwrap(),
        };
        let PhysicalEndpointKey::CellVertex(key) = vertex.endpoints[0] else {
            panic!("fixture ordinal six is a structural H3 insertion");
        };
        let plan = h3_fan_plan(cell);
        assert_eq!(
            plan.side_at_vertex(key, tangent),
            FanSide::Open,
            "the previous fan triangle admits the real Class-III reflex motion"
        );
    }
}

#[test]
fn zero_straddling_fan_diagonal_never_promotes_a_strict_real_edge() {
    // The real cell edge is strictly inward, while the artificial center
    // diagonal contains zero.  Only the latter is ambiguous: treating it
    // as open would manufacture a source/cell contact from the fan's
    // triangulation rather than from a physical H3 boundary.
    let triangle = H3FanTriangle {
        real_edge: Vec3Bound::exact([1.0, 0.0, 0.0]).unwrap(),
        diagonal: Vec3Bound::exact([0.0, 1.0, 0.0]).unwrap(),
    };
    let tangent = Vec3Ball {
        x: Ball::exact(1.0).unwrap(),
        y: Ball::new(0.0, 1.0).unwrap(),
        z: Ball::exact(0.0).unwrap(),
    };
    assert_eq!(triangle_side(&triangle, tangent), FanSide::Uncertain);
}

#[test]
fn transverse_affine_edge_is_open_and_exterior_edge_is_none() {
    let arc = OrderedMinorArc::from_test_points([1.0, 0.0, 0.0], [0.0, 1.0, 0.0]);
    let transverse =
        test_affine_source_arc(40.0, -10.0, 40.0, 10.0).expect("finite source degrees");
    let exterior =
        test_affine_source_arc(-40.0, -10.0, -40.0, 10.0).expect("finite source degrees");

    assert_eq!(classify_arc_contact(&transverse, &arc), ArcContact::Open);
    assert_eq!(classify_arc_contact(&exterior, &arc), ArcContact::None);
}

#[test]
fn affine_parent_map_keeps_structure_and_endpoint_roles_explicit() {
    let general_zero_longitude = AffineParentMap {
        lambda0: Bound::exact(0.0).unwrap(),
        phi0: Bound::exact(0.0).unwrap(),
        dlambda: Bound::exact(0.0).unwrap(),
        dphi: Bound::exact(1.0).unwrap(),
        structure: AffineStructure::General,
    };
    let source = AffineSourceArc::from_parent_map(
        &general_zero_longitude,
        Bound::new(0.0, 1.0).unwrap(),
        [SourceEndpointRole::Partition, SourceEndpointRole::ParentEnd],
        [None, Some(PhysicalEndpointKey::NorthPole)],
        [AffineEndpointIdentity::default(); 2],
    )
    .unwrap();
    assert!(!source.is_vertical());
    assert!(!source.is_axis(AxisMeridian::Prime));
    assert_eq!(source.endpoint_pole(0), None);
    assert_eq!(
        source.endpoint_pole(1),
        Some(PhysicalEndpointKey::NorthPole)
    );

    let inconsistent_constant = AffineParentMap {
        dlambda: Bound::exact(1.0).unwrap(),
        structure: AffineStructure::ConstantLongitude {
            axis: Some(AxisMeridian::Prime),
        },
        ..general_zero_longitude
    };
    assert!(
        AffineSourceArc::from_parent_map(
            &inconsistent_constant,
            Bound::new(0.0, 1.0).unwrap(),
            [
                SourceEndpointRole::ParentStart,
                SourceEndpointRole::ParentEnd
            ],
            [None, None],
            [AffineEndpointIdentity::default(); 2],
        )
        .is_none()
    );
    assert!(
        AffineSourceArc::from_parent_map(
            &general_zero_longitude,
            Bound::new(0.0, 1.0).unwrap(),
            [SourceEndpointRole::Partition, SourceEndpointRole::ParentEnd],
            [Some(PhysicalEndpointKey::NorthPole), None],
            [AffineEndpointIdentity::default(); 2],
        )
        .is_none()
    );
}

#[test]
fn general_relation_tries_the_three_certified_seam_shifts() {
    let radians = |degrees: f64| degrees.to_radians();
    let arc =
        OrderedMinorArc::from_test_points([radians(170.0).cos(), radians(170.0).sin(), 0.0], [
            radians(-170.0).cos(),
            radians(-170.0).sin(),
            0.0,
        ]);
    let source = test_affine_source_arc(180.0, -1.0, 180.0, 1.0).unwrap();
    assert_eq!(
        classify_arc_contact(&source, &arc),
        ArcContact::Open,
        "the +2pi copy is the only inner longitude overlap"
    );
}

#[test]
fn tangent_and_unresolved_plane_fail_open() {
    let arc = OrderedMinorArc::from_test_points([1.0, 0.0, 0.0], [0.0, 1.0, 0.0]);
    assert_eq!(
        arc.kind,
        ExactArcKind::General,
        "a strictly signed n.z is General even on the equatorial plane"
    );
    let tangent = test_affine_source_arc(20.0, 0.0, 70.0, 10.0).expect("finite source degrees");
    assert_eq!(classify_arc_contact(&tangent, &arc), ArcContact::Uncertain);

    let unresolved = OrderedMinorArc::from_test_points([0.0, 0.0, 1.0], [1.0, 0.0, 0.0]);
    assert_eq!(unresolved.kind, ExactArcKind::UnresolvedMeridian);
    assert_eq!(
        classify_arc_contact(&tangent, &unresolved),
        ArcContact::Uncertain
    );
}

#[test]
fn zero_length_source_on_a_general_arc_is_uncertain() {
    let arc = OrderedMinorArc::from_test_points([1.0, 0.0, 0.0], [0.0, 1.0, 0.0]);
    let point = test_affine_source_arc(45.0, 0.0, 45.0, 0.0).unwrap();
    assert_eq!(classify_arc_contact(&point, &arc), ArcContact::Uncertain);
}

#[test]
fn finite_extrema_certificate_controls_general_negative() {
    let arc = OrderedMinorArc::from_test_points([1.0, 0.0, 0.0], [0.0, 1.0, 1.0]);
    let source = test_affine_source_arc(30.0, 40.0, 60.0, 45.0).unwrap();
    let lambda = source.lambda_at(source.domain).unwrap();
    assert_eq!(
        certify_derivative_sign(&source, &arc, lambda),
        Some(CertSign::Negative),
        "this reaches B4's all-box strict derivative proof"
    );
    assert_eq!(
        classify_arc_contact(&source, &arc),
        ArcContact::None,
        "the generic branch consumes the B4 certificate for a negative"
    );

    let too_wide = test_affine_source_arc(-100.0, 40.0, 100.0, 45.0).unwrap();
    assert_eq!(
        certify_derivative_sign(
            &too_wide,
            &arc,
            too_wide.lambda_at(too_wide.domain).unwrap()
        ),
        None,
        "a source span beyond the one-minor-arc precondition cannot certify D"
    );
}
