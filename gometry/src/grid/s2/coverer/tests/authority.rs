use super::*;

#[test]
fn multi_areal_collection_internal_rings_do_not_block_interior_certificates() {
    let adjacent = Shape::geometry_collection(vec![
        box_shape(-1.0, -1.0, 0.03, 1.0),
        box_shape(0.03, -1.0, 1.0, 1.0),
    ])
    .expect("test collection is valid");
    let seam_cell = CellId::from_lonlat(0.03, 0.0).parent(12).expect("level 12");
    assert_eq!(
        classify_with_native(&adjacent, seam_cell),
        CellClass::Interior,
        "adjacent collection seam cell {}",
        seam_cell.token()
    );

    let nested = Shape::geometry_collection(vec![
        box_shape(2.0, 2.0, 3.0, 3.0),
        box_shape(2.51, 2.45, 2.55, 2.56),
    ])
    .expect("test collection is valid");
    let nested_cell = CellId::from_token("1010011").expect("reviewer repro token");
    assert_eq!(nested_cell.level(), 12);
    assert_eq!(
        classify_with_native(&nested, nested_cell),
        CellClass::Interior,
        "nested collection cell {}",
        nested_cell.token()
    );
}

#[test]
fn split_working_window_cannot_prune_the_retained_seam_owner() {
    // The exact lifted parameter for the owner probe is t=7/20. The
    // working split follows the short seam representation. Its window
    // alone rejects this S2 owner, while the retained affine edge still
    // meets the owner's certified rectangle. That disagreement is
    // precisely the hazard corroboration must keep open.
    let source = Shape::LineString(
        LineSeq::try_new(
            CoordSeq::try_from_points(&[
                Point::new_raw_xy(170.0, 0.0),
                Point::new_raw_xy(-160.0, 40.0),
            ])
            .unwrap(),
        )
        .expect("test line is valid"),
    );
    let working = source
        .split_antimeridian()
        .expect("test line has a split working shape");
    let affine = GeographicCoverageSource::new(&source, SphericalGridTarget::S2)
        .expect("test source carrier allocates");
    let owner = CellId::from_token("7b32f3").expect("frozen S2 seam owner");
    let rect = Cell::from_id(owner).rect_bound(&Cell::from_id(owner).vertices_lonlat());

    assert!(affine.s2_needs_corroboration(WorkingShapeRelation::AntimeridianSplit));
    assert_eq!(
        affine
            .classify_rect(&affine.admit_rectangular_candidate(super::rect_bounds(rect)))
            .expect("exact rectangle classification"),
        AffineRectClass::Boundary,
        "the retained raw affine edge meets the candidate enclosure"
    );
    let ctx =
        CoverContext::prepare(&working, &affine, WorkingShapeRelation::AntimeridianSplit).unwrap();
    assert_eq!(classify(&ctx, owner), CellClass::Boundary);
    let covering = fixed(10)
        .cover(&working, &affine, WorkingShapeRelation::AntimeridianSplit)
        .expect("the regression line has a small fixed-level cover");
    assert!(covering.outer().contains(&owner));
}

#[test]
fn corroboration_is_relation_gated_before_candidate_traversal() {
    let source = box_shape(10.0, 10.0, 20.0, 20.0);
    let affine = GeographicCoverageSource::new(&source, SphericalGridTarget::S2)
        .expect("test source carrier allocates");

    assert!(
        !affine.s2_needs_corroboration(WorkingShapeRelation::Identity),
        "an ordinary physical source stays on the fast path"
    );
    let ordinary = CoverContext::prepare(&source, &affine, WorkingShapeRelation::Identity).unwrap();
    assert!(!ordinary.corroborate);

    assert!(
        affine.s2_needs_corroboration(WorkingShapeRelation::AntimeridianSplit),
        "working-image identity is an explicit part of the authority proof"
    );
    let split =
        CoverContext::prepare(&source, &affine, WorkingShapeRelation::AntimeridianSplit).unwrap();
    assert!(split.corroborate);
}

#[test]
fn split_seam_carrier_can_exclude_the_far_sheet() {
    let raw = box_shape(179.0, -1.0, -179.0, 1.0);
    let affine = GeographicCoverageSource::new(&raw, SphericalGridTarget::S2).unwrap();
    assert!(
        !affine.is_unknown(),
        "the exact 179/-179 seam rectangle must retain a complete affine carrier"
    );
    let working = raw.split_antimeridian().unwrap();
    let id = CellId::from_lonlat(0.0, 0.0).parent(8).unwrap();
    let cell = Cell::from_id(id);
    let rect = cell.rect_bound(&cell.vertices_lonlat());
    assert_eq!(
        affine
            .classify_rect(&affine.admit_rectangular_candidate(super::rect_bounds(rect)))
            .expect("exact rectangle classification"),
        AffineRectClass::Outside,
        "the continuous seam lift excludes lon 0 from a 179/-179 strip"
    );

    let far_seam = CellId::from_lonlat(179.5, 45.0).parent(8).unwrap();
    let far_cell = Cell::from_id(far_seam);
    let far_rect = far_cell.rect_bound(&far_cell.vertices_lonlat());
    assert_eq!(
        affine
            .classify_rect(&affine.admit_rectangular_candidate(super::rect_bounds(far_rect)))
            .expect("exact rectangle classification"),
        AffineRectClass::Outside,
        "the exact seam carrier excludes a far-latitude seam cell"
    );
    let ctx =
        CoverContext::prepare(&working, &affine, WorkingShapeRelation::AntimeridianSplit).unwrap();
    assert_eq!(classify(&ctx, far_seam), CellClass::Outside);
    assert_eq!(
        fixed(8)
            .cover(&working, &affine, WorkingShapeRelation::AntimeridianSplit)
            .unwrap()
            .outer()
            .len(),
        64,
        "the certified split seam rectangle keeps the s2sphere outer oracle"
    );
}

#[test]
fn sub_ulp_seam_sheet_can_certify_retained_authority_interior() {
    // `6554d` has one ordinary and one sub-ULP longitude sheet at the
    // seam. Its public S2 polygon is wholly inside the raw 179/-179 linear
    // source. The latter needs its own vertical closed-set proof; forcing
    // it to Boundary loses this exact `within` cell even though the split
    // working classifier is also Interior.
    let raw = box_shape(179.0, -1.0, -179.0, 1.0);
    let working = raw.split_antimeridian().unwrap();
    let affine = GeographicCoverageSource::new(&raw, SphericalGridTarget::S2).unwrap();
    let id = CellId::from_token("6554d").expect("frozen seam interior");
    let cell = Cell::from_id(id);
    let rect = cell.rect_bound(&cell.vertices_lonlat());
    assert!(rect.crosses_seam());
    let candidate = affine.admit_rectangular_candidate(super::rect_bounds(rect));
    let windows = candidate.windows();
    let DegreeWindowResult::Windows(CertifiedDegreeWindows {
        longitude: CertifiedLongitudeDegrees::Two([_, seam]),
        ..
    }) = windows
    else {
        panic!("the frozen seam cell has two certified longitude sheets");
    };
    assert_eq!(seam.lo.to_bits(), (-180.0_f64).to_bits());
    assert_eq!(seam.hi.to_bits(), (-180.0_f64).next_up().to_bits());
    assert!(
        seam.lo.next_up() >= seam.hi,
        "the frozen second sheet enters the adjacent-double closed witness"
    );
    assert_eq!(
        affine
            .classify_rect(&candidate)
            .expect("exact rectangle classification"),
        AffineRectClass::Interior,
        "candidate={windows:?} rect={rect:?}"
    );
    let ctx =
        CoverContext::prepare(&working, &affine, WorkingShapeRelation::AntimeridianSplit).unwrap();
    assert_eq!(classify(&ctx, id), CellClass::Interior);
}

#[test]
fn polar_cap_affine_certificate_prunes_the_south() {
    let cap = box_shape(-180.0, 80.0, 180.0, 90.0);
    let affine = GeographicCoverageSource::new(&cap, SphericalGridTarget::S2).unwrap();
    let id = CellId::from_lonlat(0.0, 0.0).parent(4).unwrap();
    let cell = Cell::from_id(id);
    let rect = cell.rect_bound(&cell.vertices_lonlat());
    assert_eq!(
        affine
            .classify_rect(&affine.admit_rectangular_candidate(super::rect_bounds(rect)))
            .expect("exact rectangle classification"),
        AffineRectClass::Outside,
        "the full-longitude cap's exact carrier excludes a south cell"
    );
    let ctx = CoverContext::prepare(&cap, &affine, WorkingShapeRelation::Identity).unwrap();
    assert!(ctx.corroborate, "the polar carrier is corroborated");
    assert!(
        !affine.s2_raw_owner_descends_from(cell),
        "the south cell owns no retained cap endpoint"
    );
    assert_eq!(
        classify(&ctx, id),
        CellClass::Outside,
        "a corroborated polar cap still prunes its disjoint S2 subtree"
    );
}

#[test]
fn admitted_rectangular_candidate_preserves_circular_s2_enclosures() {
    let source =
        GeographicCoverageSource::new(&box_shape(-5.0, 13.0, 6.0, 14.0), SphericalGridTarget::S2)
            .expect("test source carrier allocates");
    let ordinary = Bounds::new_raw(-5.0, 13.0, 6.0, 14.0);
    let ordinary = source.admit_rectangular_candidate(ordinary);
    let DegreeWindowResult::Windows(CertifiedDegreeWindows {
        latitude,
        longitude: CertifiedLongitudeDegrees::One(longitude),
    }) = ordinary.windows()
    else {
        panic!("ordinary S2 rectangle has a certified degree window");
    };
    assert_eq!(latitude.lo.to_bits(), 13.0_f64.to_bits());
    assert_eq!(latitude.hi.to_bits(), 14.0_f64.to_bits());
    assert_eq!(longitude.lo.to_bits(), (-5.0_f64).to_bits());
    assert_eq!(longitude.hi.to_bits(), 6.0_f64.to_bits());

    let DegreeWindowResult::Windows(CertifiedDegreeWindows {
        longitude: CertifiedLongitudeDegrees::Two([west, east]),
        ..
    }) = source
        .admit_rectangular_candidate(Bounds::new_raw(170.0, 13.0, -170.0, 14.0))
        .windows()
    else {
        panic!("a seam enclosure retains both canonical longitude sheets");
    };
    assert_eq!(west.lo.to_bits(), 170.0_f64.to_bits());
    assert_eq!(west.hi.to_bits(), 180.0_f64.to_bits());
    assert_eq!(east.lo.to_bits(), (-180.0_f64).to_bits());
    assert_eq!(east.hi.to_bits(), (-170.0_f64).to_bits());

    let DegreeWindowResult::Windows(CertifiedDegreeWindows {
        longitude: CertifiedLongitudeDegrees::Full,
        ..
    }) = source
        .admit_rectangular_candidate(Bounds::new_raw(-180.0, 13.0, 180.0, 14.0))
        .windows()
    else {
        panic!("a polar closure retains its full-longitude enclosure");
    };
}
