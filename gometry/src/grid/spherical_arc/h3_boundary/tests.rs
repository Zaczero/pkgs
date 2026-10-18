use super::super::h3_lattice::{
    CertifiedLongitudeDegrees, certified_matching_seam_pairs, certified_seam_delta,
};
use super::*;

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

#[rstest::rstest]
#[case::raw_0x0800_1fff_ffff_ffff_u64(0x0800_1FFF_FFFF_FFFF_u64, 0_u8)]
// Ordinary non-pentagon away from both poles.  It crosses the
// degree seam, so erasing the accumulated lift makes this exact
// bbox fail open to `Boundary`.
#[case::raw_0x0805_bfff_ffff_ffff_u64(0x0805_BFFF_FFFF_FFFF_u64, 2_u8)]
#[case::raw_0x0800_3fff_ffff_ffff_u64(0x0800_3FFF_FFFF_FFFF_u64, 2_u8)]
#[case::raw_0x0800_5fff_ffff_ffff_u64(0x0800_5FFF_FFFF_FFFF_u64, 2_u8)]
#[case::raw_0x0800_dfff_ffff_ffff_u64(0x0800_DFFF_FFFF_FFFF_u64, 2_u8)]
#[case::raw_0x0810_1bff_ffff_ffff_u64(0x0810_1BFF_FFFF_FFFF_u64, 1_u8)]
#[case::raw_0x0820_377f_ffff_ffff_u64(0x0820_377F_FFFF_FFFF_u64, 1_u8)]
#[case::raw_0x0827_54ff_ffff_ffff_u64(0x0827_54FF_FFFF_FFFF_u64, 1_u8)]
#[case::raw_0x0830_136f_ffff_ffff_u64(0x0830_136F_FFFF_FFFF_u64, 1_u8)]
fn directed_h3_bbox_retains_the_vendor_2pi_sheet(#[case] raw: u64, #[case] expected: u8) {
    // The R0 polar root is Full; the next three roots exercise each
    // canonical seam form.  The remaining cells force the same carrier
    // through ordinary, polar-extremum, pentagon, and Class-III paths.
    // Every assertion calls the bbox certificate directly, before any
    // traversal can turn uncertainty into a conservative candidate.

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

#[rstest::rstest]
#[case::h3o_cellindex_try_from_0x0827_54ff_ffff_ffff_u64_unwrap(h3o::CellIndex::try_from(0x0827_54FF_FFFF_FFFF_u64).unwrap())]
#[case::h3o_cellindex_try_from_0x0810_1bff_ffff_ffff_u64_unwrap(h3o::CellIndex::try_from(0x0810_1BFF_FFFF_FFFF_u64).unwrap())]
#[case::h3o_cellindex_try_from_0x0830_136f_ffff_ffff_u64_unwrap(h3o::CellIndex::try_from(0x0830_136F_FFFF_FFFF_u64).unwrap())]
#[case::h3o_cellindex_try_from_0x0810_83ff_ffff_ffff_u64_unwrap(h3o::CellIndex::try_from(0x0810_83FF_FFFF_FFFF_u64).unwrap())]
#[case::h3o_cellindex_try_from_0x0820_327f_ffff_ffff_u64_unwrap(h3o::CellIndex::try_from(0x0820_327F_FFFF_FFFF_u64).unwrap())]
#[case::h3o_cellindex_try_from_0x082f_297f_ffff_ffff_u64_unwrap(h3o::CellIndex::try_from(0x082F_297F_FFFF_FFFF_u64).unwrap())]
fn ordered_h3_arcs_certify_ordinary_pentagon_class_iii_and_both_poles(
    #[case] cell: h3o::CellIndex,
) {
    let raw = cell.boundary_with_seeds();
    let arcs = h3_ordered_arcs(cell).expect("raw ordered H3 boundary is certified");
    assert_eq!(arcs.len(), raw.public.len());
    assert_eq!(arcs.iter().count(), arcs.len());
    assert!(arcs.len() <= MAX_H3_BOUNDARY_ARCS);
    assert!(matches!(h3_fan_plan(cell), H3FanPlan::Certified(_)));
}
