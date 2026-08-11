use h3o::{CellIndex, Resolution};

use crate::geometry::{CoordSeq, LineSeq, Point, Polygon, Ring, Shape};
use crate::grid::affine_source::{
    GridAffineSource, GridPointClass, RectClass, SphericalGridTarget,
};
use crate::grid::h3_coverer::{
    H3CoverError, H3CoverPlan, H3LeafClass, H3TraversalRule, TEST_DIAGNOSTIC_NODES,
    classify_h3_overlap, classify_h3_overlap_parts, fan_retains_point, h3_cover, h3_cover_shape,
    overlap_cell_plan_preparations, reset_overlap_cell_plan_preparations,
};
use crate::grid::spherical_arc::{
    ArcContact, Bound, H3FanPlan, H3FanPointClass, H3PoleOwners, classify_h3_arc_contact,
    exact_h3_bbox_for_cell, h3_cell_plan,
};

const CELL: u64 = 0x0827_54FF_FFFF_FFFF;
// Stored doubles from the independent analytic leaf corpus.  They are
// deliberately not re-derived from a public cell polygon.
const CENTER_LONGITUDE: f64 = 0.534_167_862_982_367_2;
const CENTER_LATITUDE: f64 = -0.459_695_642_341_289_07;

fn ring(west: f64, south: f64, east: f64, north: f64) -> Ring {
    Ring::from_trusted_closed(CoordSeq::from(vec![
        Point::new(west, south).unwrap(),
        Point::new(east, south).unwrap(),
        Point::new(east, north).unwrap(),
        Point::new(west, north).unwrap(),
        Point::new(west, south).unwrap(),
    ]))
}

fn polygon(west: f64, south: f64, east: f64, north: f64) -> Polygon {
    Polygon::new(ring(west, south, east, north), Vec::new())
}

fn latitude_ring(latitude: f64, reverse: bool) -> Ring {
    let mut points = Vec::new();
    if reverse {
        for longitude in (-180..180).step_by(5).rev() {
            points.push(Point::new(f64::from(longitude), latitude).unwrap());
        }
    } else {
        for longitude in (-180..180).step_by(5) {
            points.push(Point::new(f64::from(longitude), latitude).unwrap());
        }
    }
    points.push(points[0]);
    Ring::from_trusted_closed(CoordSeq::from(points))
}

fn classify(shape: &Shape) -> H3LeafClass {
    let cell = CellIndex::try_from(CELL).unwrap();
    let source = GridAffineSource::new(shape, SphericalGridTarget::H3(Resolution::Two)).unwrap();
    let plan = h3_cell_plan(cell, H3PoleOwners::for_target(Resolution::Two));
    classify_h3_overlap(&source, &plan)
}

#[test]
fn big_fixture_enters_each_area_proof_before_its_leaf_verdict() {
    let cell = CellIndex::try_from(CELL).unwrap();
    let source = GridAffineSource::new(
        &Shape::Polygon(polygon(-3.0, -3.0, 3.0, 3.0)),
        SphericalGridTarget::H3(Resolution::Two),
    )
    .unwrap();
    let plan = h3_cell_plan(cell, H3PoleOwners::for_target(Resolution::Two));
    let center = h3o::LatLng::from(cell);
    assert!(!source.is_unknown());
    assert_eq!(
        plan.fan().kernel_point_class(
            Bound::exact(center.lng()).unwrap(),
            Bound::exact(center.lat()).unwrap()
        ),
        H3FanPointClass::Open,
        "the stored H3 center reaches the certified fan"
    );
    assert_eq!(
        source.authority_point_class(center.lng(), center.lat()),
        GridPointClass::Interior,
        "the raw authority PIP sees the center in BIG"
    );
    let arcs = plan.arcs().unwrap();
    for piece in source
        .authority_pieces()
        .unwrap()
        .filter(|piece| piece.is_polygon())
    {
        assert!(
            arcs.iter().all(|arc| {
                classify_h3_arc_contact(piece.arc(), arc, plan.fan()) == ArcContact::None
            }),
            "BIG boundary is certified disjoint from every ordered H3 arc"
        );
    }
    for (_, _, point) in source.polygon_authority_witnesses() {
        let point = point.unwrap();
        assert_eq!(
            plan.fan().point_class(point.longitude, point.latitude),
            H3FanPointClass::Outside,
            "BIG's stored shell witness is outside the H3 fan"
        );
    }
}

#[test]
fn area_mixed_and_lower_dimensional_witnesses_obey_leaf_ordering() {
    let big = polygon(-3.0, -3.0, 3.0, 3.0);
    let small = polygon(0.52, -0.47, 0.55, -0.44);
    let holed = Polygon::new(ring(-3.0, -3.0, 3.0, 3.0), vec![ring(
        0.52, -0.47, 0.55, -0.44,
    )]);
    let line = Shape::LineString(LineSeq::from_trusted(CoordSeq::from(vec![
        Point::new(0.52, -0.47).unwrap(),
        Point::new(0.55, -0.44).unwrap(),
    ])));
    let point = Shape::Point(Point::new(CENTER_LONGITUDE, CENTER_LATITUDE).unwrap());

    assert_eq!(
        classify(&Shape::Polygon(big.clone())),
        H3LeafClass::Interior
    );
    assert_eq!(
        classify(&Shape::Polygon(small.clone())),
        H3LeafClass::Boundary
    );
    assert_eq!(classify(&Shape::Polygon(holed)), H3LeafClass::Boundary);
    assert_eq!(classify(&line), H3LeafClass::Boundary);
    assert_eq!(classify(&point), H3LeafClass::Boundary);
    assert_eq!(
        classify(&Shape::MultiPolygon(vec![big.clone(), small])),
        H3LeafClass::Boundary
    );
    assert_eq!(
        classify(&Shape::GeometryCollection(vec![
            Shape::Polygon(big),
            line,
            point
        ])),
        H3LeafClass::Interior
    );
}

#[test]
fn hole_witness_vetoes_the_otherwise_certified_outside() {
    let holed = Shape::Polygon(Polygon::new(ring(-3.0, -3.0, 3.0, 3.0), vec![ring(
        0.52, -0.47, 0.55, -0.44,
    )]));
    assert_eq!(classify(&holed), H3LeafClass::Boundary);
}

#[test]
fn authority_pip_is_the_only_path_that_certifies_big_interior() {
    let big = Shape::Polygon(polygon(-3.0, -3.0, 3.0, 3.0));
    assert_eq!(classify(&big), H3LeafClass::Interior);
}

#[test]
fn wrapped_selection_image_does_not_turn_an_unrelated_leaf_into_contact() {
    let line = Shape::LineString(LineSeq::from_trusted(CoordSeq::from(vec![
        Point::new(170.0, -10.0).unwrap(),
        Point::new(-170.0, 10.0).unwrap(),
    ])));
    let source = GridAffineSource::new(&line, SphericalGridTarget::H3(Resolution::Two)).unwrap();
    let far = CellIndex::try_from(CELL).unwrap();
    let plan = h3_cell_plan(far, H3PoleOwners::for_target(Resolution::Two));
    assert!(
        source
            .selection_pieces()
            .unwrap()
            .any(|piece| !piece.is_positive_only()),
        "the source enters the wrapped seam partition without a reflected image"
    );
    assert_eq!(classify_h3_overlap(&source, &plan), H3LeafClass::Outside);
}

#[test]
fn reflected_selection_pip_vetoes_only_the_candidate_it_contains() {
    // This stored-double shell reflects through the north pole to the
    // physical box [-20,-10] x [70,80].  The two candidates prove that a
    // reflection is neither ignored nor globally widened: only its exact
    // selection PIP may retain a leaf.
    let source = GridAffineSource::new(
        &Shape::Polygon(polygon(160.0, 100.0, 170.0, 110.0)),
        SphericalGridTarget::H3(Resolution::Two),
    )
    .unwrap();
    let inside = h3o::LatLng::new(75.0, -15.0)
        .unwrap()
        .to_cell(Resolution::Two);
    let inside_center = h3o::LatLng::from(inside);
    let inside_plan = h3_cell_plan(inside, H3PoleOwners::for_target(Resolution::Two));
    assert_eq!(
        source.positive_selection_point_class(inside_center.lng(), inside_center.lat()),
        GridPointClass::Interior,
        "the reflected selected shell contains this exact H3 center"
    );
    assert_eq!(
        classify_h3_overlap(&source, &inside_plan),
        H3LeafClass::Boundary
    );

    let far = h3o::LatLng::new(0.0, 0.0).unwrap().to_cell(Resolution::Two);
    let far_center = h3o::LatLng::from(far);
    let far_plan = h3_cell_plan(far, H3PoleOwners::for_target(Resolution::Two));
    assert_eq!(
        source.positive_selection_point_class(far_center.lng(), far_center.lat()),
        GridPointClass::Exterior,
        "the distant center is outside the exact reflected selection shell"
    );
    assert!(
        far_plan.arcs().unwrap().iter().all(|arc| {
            source
                .selection_pieces()
                .unwrap()
                .filter(|piece| piece.is_polygon() && piece.is_positive_only())
                .all(|piece| {
                    classify_h3_arc_contact(piece.arc(), arc, far_plan.fan()) == ArcContact::None
                })
        }),
        "the distant fan has no exact reflected-boundary contact"
    );
    assert_eq!(
        source.authority_point_class(far_center.lng(), far_center.lat()),
        GridPointClass::Exterior,
        "the raw out-of-strip shell cannot manufacture authority interior"
    );
    assert_eq!(
        classify_h3_overlap(&source, &far_plan),
        H3LeafClass::Outside
    );
}

#[test]
fn uncertified_fan_cannot_reach_an_empty_source_negative() {
    let cell = CellIndex::try_from(CELL).unwrap();
    let source = GridAffineSource::new(
        &Shape::GeometryCollection(Vec::new()),
        SphericalGridTarget::H3(Resolution::Two),
    )
    .unwrap();
    let plan = h3_cell_plan(cell, H3PoleOwners::for_target(Resolution::Two));
    assert_eq!(
        classify_h3_overlap_parts(&source, cell, plan.arcs().unwrap(), &H3FanPlan::Uncertain),
        H3LeafClass::Boundary,
        "an uncertified fan fails open before the source can establish Outside"
    );
}

#[test]
fn selected_pole_crossing_retains_the_owner_for_both_antimeridian_spellings() {
    let pole = h3o::LatLng::new(90.0, 0.0)
        .unwrap()
        .to_cell(Resolution::Two);
    let plan = h3_cell_plan(pole, H3PoleOwners::for_target(Resolution::Two));
    for longitude in [-180.0, 180.0] {
        let source = GridAffineSource::new(
            &Shape::LineString(LineSeq::from_trusted(CoordSeq::from(vec![
                Point::new(longitude, 80.0).unwrap(),
                Point::new(longitude, 100.0).unwrap(),
            ]))),
            SphericalGridTarget::H3(Resolution::Two),
        )
        .unwrap();
        let pole_witnesses: Vec<_> = source.selection_pole_witnesses().collect();
        assert!(
            !pole_witnesses.is_empty(),
            "the {longitude} crossing materializes its exact selection pole witness"
        );
        assert!(
            pole_witnesses.into_iter().any(|point| {
                point.latitude == Bound::exact(90.0).unwrap()
                    && fan_retains_point(plan.fan(), Some(point))
            }),
            "the exact {longitude} pole crossing reaches the native fan without a seam spelling"
        );
        assert_eq!(classify_h3_overlap(&source, &plan), H3LeafClass::Boundary);
    }
}

#[test]
fn root_descent_nominates_the_two_ring_logical_bbox_owner() {
    let source = GridAffineSource::new(
        &Shape::Point(Point::new(-170.0, 74.0).unwrap()),
        SphericalGridTarget::H3(Resolution::Zero),
    )
    .unwrap();
    let covered = h3_cover(
        &source,
        &H3CoverPlan::new(Resolution::Zero),
        H3TraversalRule::Bbox,
        None,
    )
    .unwrap();
    let owner = CellIndex::try_from(0x0800_1FFF_FFFF_FFFF_u64).unwrap();
    assert!(
        covered.iter().any(|entry| entry.cell == owner),
        "122-root bbox descent retains 8001 beyond the point owner's one-ring nomination"
    );
}

#[test]
fn certified_bbox_descent_selects_the_polar_nine() {
    let source = GridAffineSource::new(
        &Shape::LineString(LineSeq::from_trusted(CoordSeq::from(vec![
            Point::new(-90.0, 85.0).unwrap(),
            Point::new(90.0, 85.0).unwrap(),
        ]))),
        SphericalGridTarget::H3(Resolution::Two),
    )
    .unwrap();
    let covered = h3_cover(
        &source,
        &H3CoverPlan::new(Resolution::Two),
        H3TraversalRule::Bbox,
        None,
    )
    .unwrap();
    assert_eq!(
        covered.len(),
        9,
        "the exact logical bbox, not a padded candidate ring, selects the polar nine"
    );
}

#[test]
fn ordered_overlap_relation_retains_the_polar_arc_extremum_owner() {
    let source = GridAffineSource::new(
        &Shape::LineString(LineSeq::from_trusted(CoordSeq::from(vec![
            Point::new(-90.0, 85.0).unwrap(),
            Point::new(90.0, 85.0).unwrap(),
        ]))),
        SphericalGridTarget::H3(Resolution::Two),
    )
    .unwrap();
    let covered = h3_cover(
        &source,
        &H3CoverPlan::new(Resolution::Two),
        H3TraversalRule::Overlap,
        None,
    )
    .unwrap();
    let owner = CellIndex::try_from(0x0820_377F_FFFF_FFFF_u64).unwrap();
    assert!(
        covered.iter().any(|entry| entry.cell == owner),
        "the ordered arc relation retains 820377 where the chord proxy returned Outside"
    );
    assert_eq!(
        covered.len(),
        9,
        "the exact relation adds the missing polar owner without a padding ring"
    );
}

#[test]
fn physical_vertical_signs_exclude_the_raw_seam_bbox_only_cell() {
    let source = Shape::Polygon(Polygon::new(
        Ring::from_trusted_closed(CoordSeq::from(vec![
            Point::new(179.0, -5.0).unwrap(),
            Point::new(-179.0, -5.0).unwrap(),
            Point::new(-179.0, 5.0).unwrap(),
            Point::new(179.0, 5.0).unwrap(),
            Point::new(179.0, -5.0).unwrap(),
        ])),
        Vec::new(),
    ));
    let source = GridAffineSource::new(&source, SphericalGridTarget::H3(Resolution::Two)).unwrap();
    let plan = H3CoverPlan::new(Resolution::Two);
    let seam_cell = CellIndex::try_from(0x0827_FB7F_FFFF_FFFF_u64).unwrap();
    let overlap = h3_cover(&source, &plan, H3TraversalRule::Overlap, None).unwrap();
    let bbox = h3_cover(&source, &plan, H3TraversalRule::Bbox, None).unwrap();
    assert_eq!(overlap.len(), 9);
    assert!(!overlap.iter().any(|entry| entry.cell == seam_cell));
    assert_eq!(bbox.len(), 10);
    assert!(bbox.iter().any(|entry| entry.cell == seam_cell));
}

#[test]
fn periodic_polar_annulus_cap_never_prunes_an_intersecting_leaf() {
    let source = GridAffineSource::new(
        &Shape::Polygon(Polygon::new(latitude_ring(60.0, false), vec![
            latitude_ring(80.0, false),
        ])),
        SphericalGridTarget::H3(Resolution::Two),
    )
    .unwrap();
    let plan = H3CoverPlan::new(Resolution::Two);
    let cell = CellIndex::try_from(0x0820_007F_FFFF_FFFF_u64).unwrap();
    let cap = plan
        .cap
        .as_ref()
        .unwrap()
        .descendant_windows(cell, plan.poles);
    assert_ne!(
        source.classify_rect(cap),
        RectClass::Outside,
        "the target cap contains an annulus cell that the analytic universe intersects; cap={cap:?}"
    );
    assert_ne!(
        source.classify_rect(exact_h3_bbox_for_cell(cell)),
        RectClass::Outside,
        "the exact logical bbox retains the same annulus cell"
    );
}

#[test]
fn periodic_polar_winding_caps_retain_each_analytic_owner() {
    // A full linear longitude winding closes through the sign-selected
    // pole.  Its raw vertices all share latitude 60, but the forward
    // degree +1 ring covers the north cap and the reverse degree -1 ring
    // covers the south cap.  The companion affine-source unit test pins
    // those exact degree/roof identities; each derived H3 owner must
    // remain visible here.
    for (reverse, latitude) in [(false, 87.0), (true, -87.0)] {
        let shape = Shape::Polygon(Polygon::new(latitude_ring(85.0, reverse), Vec::new()));
        let source =
            GridAffineSource::new(&shape, SphericalGridTarget::H3(Resolution::Two)).unwrap();
        let owner = h3o::LatLng::new(latitude, 0.0)
            .unwrap()
            .to_cell(Resolution::Two);
        let plan = H3CoverPlan::new(Resolution::Two);
        let cap = plan
            .cap
            .as_ref()
            .unwrap()
            .descendant_windows(owner, plan.poles);
        assert_ne!(source.classify_rect(cap), RectClass::Outside);
        assert!(
            h3_cover(&source, &plan, H3TraversalRule::Overlap, None)
                .unwrap()
                .iter()
                .any(|entry| entry.cell == owner),
            "the cap traversal retains {owner} for the periodic {latitude} owner"
        );
    }
}

#[test]
fn globally_fail_open_multipart_recovers_independent_cap_certificates() {
    const TARGET: Resolution = Resolution::Zero;
    let polar = polygon(-180.0, 80.0, 180.0, 90.0);
    let middle = polygon(10.0, 0.0, 20.0, 10.0);
    let shape = Shape::MultiPolygon(vec![polar.clone(), middle.clone()]);
    let plan = H3CoverPlan::new(TARGET);
    let aggregate = GridAffineSource::new(&shape, SphericalGridTarget::H3(TARGET)).unwrap();
    assert!(
        aggregate.cap_is_globally_fail_open(),
        "the mixed aggregate enters per-component cap certification"
    );

    let actual = h3_cover_shape(&shape, &plan, H3TraversalRule::Overlap, None).unwrap();
    let mut expected = h3_cover(
        &GridAffineSource::new(&Shape::Polygon(polar), SphericalGridTarget::H3(TARGET)).unwrap(),
        &plan,
        H3TraversalRule::Overlap,
        None,
    )
    .unwrap();
    expected.extend(
        h3_cover(
            &GridAffineSource::new(&Shape::Polygon(middle), SphericalGridTarget::H3(TARGET))
                .unwrap(),
            &plan,
            H3TraversalRule::Overlap,
            None,
        )
        .unwrap(),
    );
    expected.sort_unstable_by_key(|entry| u64::from(entry.cell));

    assert_eq!(
        actual.len(),
        expected.len(),
        "the aggregate cannot globally fail open after per-component certification"
    );
    assert_eq!(
        actual, expected,
        "the two components retain their exact overlap union"
    );
    let one_below_global_union = actual.len() - 1;
    assert!(matches!(
        h3_cover_shape(
            &shape,
            &plan,
            H3TraversalRule::Overlap,
            Some(one_below_global_union),
        ),
        Err(H3CoverError::Budget(_))
    ));
}

#[test]
fn globally_fail_open_components_merge_duplicate_ids_before_budgeting() {
    const TARGET: Resolution = Resolution::Two;
    let partial = polygon(-180.0, 85.0, 0.0, 90.0);
    let full = polygon(-180.0, 85.0, 180.0, 90.0);
    let plan = H3CoverPlan::new(TARGET);
    let partial_cover = h3_cover(
        &GridAffineSource::new(
            &Shape::Polygon(partial.clone()),
            SphericalGridTarget::H3(TARGET),
        )
        .unwrap(),
        &plan,
        H3TraversalRule::Overlap,
        None,
    )
    .unwrap();
    let full_cover = h3_cover(
        &GridAffineSource::new(
            &Shape::Polygon(full.clone()),
            SphericalGridTarget::H3(TARGET),
        )
        .unwrap(),
        &plan,
        H3TraversalRule::Overlap,
        None,
    )
    .unwrap();
    let shared = partial_cover
        .iter()
        .find_map(|partial_entry| {
            full_cover
                .iter()
                .find(|full_entry| full_entry.cell == partial_entry.cell)
                .filter(|full_entry| !partial_entry.interior && full_entry.interior)
                .map(|full_entry| (partial_entry.cell, full_entry.interior))
        })
        .expect("partial and full caps share a boundary/interior cell");

    let mixed = Shape::GeometryCollection(vec![Shape::Polygon(partial), Shape::Polygon(full)]);
    assert!(
        GridAffineSource::new(&mixed, SphericalGridTarget::H3(TARGET))
            .unwrap()
            .cap_is_globally_fail_open(),
        "the overlapping collection enters per-component certification"
    );
    let merged = h3_cover_shape(&mixed, &plan, H3TraversalRule::Overlap, None).unwrap();
    assert_eq!(
        merged.iter().filter(|entry| entry.cell == shared.0).count(),
        1,
        "the shared cap cell is emitted only once"
    );
    assert!(
        merged
            .iter()
            .find(|entry| entry.cell == shared.0)
            .is_some_and(|entry| entry.interior),
        "a boundary member ORs with the full-cap interior certificate"
    );
}

// Split from the shared-cell test above: the two properties are independent
// and each needs its own covers, so as one test they serialized seven cover
// calls into a single slow unit.
#[test]
fn globally_fail_open_duplicate_components_merge_before_their_global_budget() {
    const TARGET: Resolution = Resolution::Two;
    // A tight cap: this test needs four separate covers, and neither the
    // duplicate merge nor the budget arithmetic depends on the cap's size.
    let full = polygon(-180.0, 85.0, 180.0, 90.0);
    let plan = H3CoverPlan::new(TARGET);
    let full_cover = h3_cover(
        &GridAffineSource::new(
            &Shape::Polygon(full.clone()),
            SphericalGridTarget::H3(TARGET),
        )
        .unwrap(),
        &plan,
        H3TraversalRule::Overlap,
        None,
    )
    .unwrap();

    let duplicate =
        Shape::GeometryCollection(vec![Shape::Polygon(full.clone()), Shape::Polygon(full)]);
    assert!(
        GridAffineSource::new(&duplicate, SphericalGridTarget::H3(TARGET))
            .unwrap()
            .cap_is_globally_fail_open(),
        "the duplicate collection enters per-component certification"
    );
    let deduplicated = h3_cover_shape(&duplicate, &plan, H3TraversalRule::Overlap, None)
        .expect("duplicate components merge before their global budget");
    assert_eq!(deduplicated, full_cover);
}

// The budget half needs three further covers of its own, so it is a separate
// test rather than a fourth and fifth cover appended to the merge above.
#[test]
fn globally_fail_open_duplicate_budget_applies_after_the_merge() {
    const TARGET: Resolution = Resolution::Two;
    let full = polygon(-180.0, 85.0, 180.0, 90.0);
    let plan = H3CoverPlan::new(TARGET);
    let duplicate =
        Shape::GeometryCollection(vec![Shape::Polygon(full.clone()), Shape::Polygon(full)]);
    let deduplicated = h3_cover_shape(&duplicate, &plan, H3TraversalRule::Overlap, None)
        .expect("duplicate components merge before their global budget");
    h3_cover_shape(
        &duplicate,
        &plan,
        H3TraversalRule::Overlap,
        Some(deduplicated.len()),
    )
    .expect("the global budget applies after duplicate merge");
    assert!(matches!(
        h3_cover_shape(
            &duplicate,
            &plan,
            H3TraversalRule::Overlap,
            Some(deduplicated.len() - 1),
        ),
        Err(H3CoverError::Budget(_))
    ));
}

#[test]
fn accepted_outward_pole_point_uses_the_physical_pole_owner() {
    let outward = 90.0_f64.next_up();
    let shape = Shape::Point(Point::new(17.0, outward).unwrap());
    let source = GridAffineSource::new(&shape, SphericalGridTarget::H3(Resolution::Two)).unwrap();
    let covered = h3_cover(
        &source,
        &H3CoverPlan::new(Resolution::Two),
        H3TraversalRule::Overlap,
        None,
    )
    .unwrap();
    assert_eq!(covered.len(), 1, "the physical pole has one r2 owner");
}

#[test]
fn every_depth_cap_certificate_reaches_the_north_cap_bulk_stream() {
    // The cap starts at 80 rather than 70 degrees. Cost here is driven by the
    // number of boundary cells that must be certified, which scales with the
    // cap's circumference, while the property needs only that a substantial
    // INTERIOR bulk exists to be streamed -- 256 of the 327 visible cells.
    let source = GridAffineSource::new(
        &Shape::Polygon(polygon(-180.0, 80.0, 180.0, 90.0)),
        SphericalGridTarget::H3(Resolution::Three),
    )
    .unwrap();
    assert!(
        source.is_unknown(),
        "the test enters the former global-unknown veto path before the exact rectangle certificate"
    );
    let covered = h3_cover(
        &source,
        &H3CoverPlan::new(Resolution::Three),
        H3TraversalRule::Bbox,
        None,
    )
    .unwrap();
    assert_eq!(
        covered.len(),
        327,
        "the analytic north-cap fixture has the adjudicated exact visible set"
    );
    assert_eq!(
        covered.iter().filter(|entry| entry.interior).count(),
        256,
        "strictly contained caps stream their descendants instead of invoking target cell plans"
    );
}

#[test]
fn unknown_overlap_leaves_reuse_the_certified_bbox_dispatch() {
    // A tighter polar cap than the 70-degree fixture: it still classifies as
    // unknown (asserted below, which is the property that routes this into
    // the dispatch) with far fewer boundary cells to certify.
    let source = GridAffineSource::new(
        &Shape::Polygon(polygon(-180.0, 85.0, 180.0, 90.0)),
        SphericalGridTarget::H3(Resolution::Three),
    )
    .unwrap();
    assert!(
        source.is_unknown(),
        "the north-cap fixture enters the dispatch"
    );
    let plan = H3CoverPlan::new(Resolution::Three);

    let bbox = h3_cover(&source, &plan, H3TraversalRule::Bbox, None).unwrap();
    reset_overlap_cell_plan_preparations();
    let overlap = h3_cover(&source, &plan, H3TraversalRule::Overlap, None).unwrap();

    assert_eq!(overlap, bbox, "unknown overlap leaves are bbox-equivalent");
    assert_eq!(
        overlap_cell_plan_preparations(),
        0,
        "unknown overlap leaves must not rebuild H3 arcs or fans after bbox certification"
    );
}

#[test]
fn global_unknown_veto_cannot_suppress_north_cap_bulk_certificates() {
    let source = GridAffineSource::new(
        &Shape::Polygon(polygon(-180.0, 70.0, 180.0, 90.0)),
        SphericalGridTarget::H3(Resolution::Three),
    )
    .unwrap();
    let plan = H3CoverPlan::new(Resolution::Three);
    let bulk_parent = h3o::LatLng::new(80.0, 0.0)
        .unwrap()
        .to_cell(Resolution::Two);
    let relation = plan.cap.as_ref().map_or(RectClass::Boundary, |cap| {
        source.classify_rect(cap.descendant_windows(bulk_parent, plan.poles))
    });
    assert_eq!(
        relation,
        RectClass::Interior,
        "this seven-child north-cap parent must reach bulk emission before the global unknown veto"
    );
    assert_eq!(bulk_parent.children_count(plan.target), 7);
}

#[test]
fn within_keeps_complete_target_bbox_certificates() {
    // One rung rather than three. The 70-degree cap and the two cell ids
    // asserted below were adjudicated together, so the fixture is kept
    // exactly and the deeper rungs -- whose expected counts were pure
    // characterizations -- are dropped instead. Cost grows steeply with
    // depth: this rung is ~0.4 s unoptimized, the Four rung was ~4 s.
    let shape = Shape::Polygon(polygon(-180.0, 70.0, 180.0, 90.0));
    for (resolution, expected) in [(Resolution::Two, 145)] {
        let covered = h3_cover_shape(
            &shape,
            &H3CoverPlan::new(resolution),
            H3TraversalRule::Within,
            None,
        )
        .unwrap();
        assert_eq!(
            covered.len(),
            expected,
            "polar within count at {resolution}"
        );
        if resolution == Resolution::Two {
            for raw in [0x0820_297F_FFFF_FFFF, 0x0820_B6FF_FFFF_FFFF] {
                let cell = CellIndex::try_from(raw).unwrap();
                assert!(
                    covered.iter().any(|entry| entry.cell == cell),
                    "missing {cell}"
                );
            }
        }
    }
}

#[test]
fn within_discards_non_areal_members_before_traversal() {
    let line = Shape::LineString(LineSeq::from_trusted(CoordSeq::from(vec![
        Point::new(-90.0, 0.0).unwrap(),
        Point::new(90.0, 0.0).unwrap(),
    ])));
    for resolution in [
        Resolution::Two,
        Resolution::Four,
        Resolution::Six,
        Resolution::Eight,
    ] {
        let covered = h3_cover_shape(
            &line,
            &H3CoverPlan::new(resolution),
            H3TraversalRule::Within,
            Some(1),
        )
        .unwrap();
        assert!(covered.is_empty());
        assert_eq!(TEST_DIAGNOSTIC_NODES.get(), 0);
    }

    // Resolution Three rather than Six: the property under test is that the
    // non-areal member is discarded before traversal, which every resolution
    // exercises identically, while the cost of a cover grows steeply with
    // depth. See the resolution note in `every_depth_cap_certificate_...`.
    let area = Shape::Polygon(polygon(-1.0, -1.0, 1.0, 1.0));
    let area_only = h3_cover_shape(
        &area,
        &H3CoverPlan::new(Resolution::Three),
        H3TraversalRule::Within,
        None,
    )
    .unwrap();
    let area_nodes = TEST_DIAGNOSTIC_NODES.get();

    let mixed = Shape::GeometryCollection(vec![area, line]);
    let mixed_cover = h3_cover_shape(
        &mixed,
        &H3CoverPlan::new(Resolution::Three),
        H3TraversalRule::Within,
        None,
    )
    .unwrap();
    assert_eq!(mixed_cover, area_only);
    assert_eq!(mixed_cover.len(), 2);
    assert_eq!(TEST_DIAGNOSTIC_NODES.get(), area_nodes);
}
