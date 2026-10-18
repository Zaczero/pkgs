#[rstest::rstest]
#[case::polygon_0_0_1_0_1_1_0_1_0_0_1_0_1_1_0_1_0_0(
    "POLYGON ((0 0,1 0,1 1,0 1,0 0,1 0,1 1,0 1,0 0))",
    1.0
)]
#[case::polygon_0_0_1_1_1_0_0_1_0_0("POLYGON ((0 0,1 1,1 0,0 1,0 0))", 0.5)]
#[case::multipolygon_0_0_1_0_1_1_0_1_0_0_0_5_0_1_5_0_1_5_1_0_5_1_0_5_0(
    "MULTIPOLYGON (((0 0,1 0,1 1,0 1,0 0)),((0.5 0,1.5 0,1.5 1,0.5 1,0.5 0)))",
    1.5
)]
#[case::polygon_0_0_1_0_1_1_0_1_0_0_3_0_4_0_4_1_3_1_3_0(
    "POLYGON ((0 0,1 0,1 1,0 1,0 0),(3 0,4 0,4 1,3 1,3 0))",
    1.0
)]
#[case::polygon_0_0_1_0_1_1_0_1_0_0_0_5_0_5_1_5_0_5_1_5_6688ce68(
    "POLYGON ((0 0,1 0,1 1,0 1,0 0),(-0.5 -0.5,1.5 -0.5,1.5 1.5,-0.5 1.5,-0.5 -0.5))",
    0.0
)]
fn occupied_support_owner_composes_stored_rings_and_parts(
    #[case] wkt: &'static str,
    #[case] expected: f64,
) {
    let shape = crate::io::parse_wkt(wkt).expect("valid test WKT");
    let support = occupied_cartesian_support(&shape).expect("exact occupied support");
    assert!(
        (support.area() - expected).abs() <= 64.0 * f64::EPSILON * expected,
        "{wkt}",
    );
}

#[rstest::rstest]
#[case::polygon_0_0_1_0_1_1_0_1_0_0_1_0_1_1_0_1_0_0(
    "POLYGON ((0 0,1 0,1 1,0 1,0 0,1 0,1 1,0 1,0 0))",
    1.0
)]
#[case::polygon_0_0_1_1_1_0_0_1_0_0("POLYGON ((0 0,1 1,1 0,0 1,0 0))", 0.5)]
#[case::multipolygon_0_0_1_0_1_1_0_1_0_0_0_5_0_1_5_0_1_5_1_0_5_1_0_5_0(
    "MULTIPOLYGON (((0 0,1 0,1 1,0 1,0 0)),((0.5 0,1.5 0,1.5 1,0.5 1,0.5 0)))",
    1.5
)]
#[case::polygon_0_0_1_0_1_1_0_1_0_0_3_0_4_0_4_1_3_1_3_0(
    "POLYGON ((0 0,1 0,1 1,0 1,0 0),(3 0,4 0,4 1,3 1,3 0))",
    1.0
)]
#[case::polygon_0_0_1_0_1_1_0_1_0_0_0_5_0_5_1_5_0_5_1_5_6688ce68(
    "POLYGON ((0 0,1 0,1 1,0 1,0 0),(-0.5 -0.5,1.5 -0.5,1.5 1.5,-0.5 1.5,-0.5 -0.5))",
    0.0
)]
fn occupied_area_owner_rounds_exact_selected_faces(
    #[case] wkt: &'static str,
    #[case] expected: f64,
) {
    let shape = crate::io::parse_wkt(wkt).expect("valid test WKT");
    assert_eq!(
        occupied_cartesian_area(&shape).expect("exact occupied area"),
        expected,
        "{wkt}"
    );
}

#[test]
fn occupied_area_inventory_folds_repeated_shared_collection_owners() {
    let polygon =
        crate::io::parse_wkt("POLYGON ((0 0,1 0,1 1,0 1,0 0))").expect("valid shared-DAG polygon");
    let mut shape =
        Shape::geometry_collection(vec![polygon]).expect("one-polygon collection is admitted");
    for _ in 0..16 {
        shape = Shape::geometry_collection(vec![shape.clone(), shape])
            .expect("collection depth is admitted");
    }

    let inventory = ExactSourceInventory::from_occupied_area_shape(&shape)
        .expect("area sources stage")
        .expect("the repeated DAG contains a polygon");
    assert_eq!(inventory.source_segments.len(), 4);
    assert_eq!(
        occupied_cartesian_area(&shape).expect("exact occupied area"),
        1.0
    );
}

#[test]
fn cartesian_set_support_consumers_match_unshared_expansion() {
    let base = crate::io::parse_wkt("POLYGON ((0 0,1 0,1 1,0 1,0 0))").unwrap();
    let mut shared = base;
    for _ in 0..3 {
        shared = Shape::geometry_collection(vec![shared.clone(), shared]).unwrap();
    }
    let expanded = crate::io::parse_wkb(
        &crate::io::to_wkb(&shared, None, false).expect("shared support serializes"),
    )
    .expect("unshared support expansion parses")
    .shape;

    let shared_support = occupied_cartesian_support(&shared).unwrap();
    let expanded_support = occupied_cartesian_support(&expanded).unwrap();
    assert_eq!(
        crate::io::to_wkb(&shared_support, None, false).unwrap(),
        crate::io::to_wkb(&expanded_support, None, false).unwrap()
    );
    approx::assert_relative_eq!(
        occupied_cartesian_area(&shared).unwrap(),
        occupied_cartesian_area(&expanded).unwrap(),
        max_relative = 64.0 * f64::EPSILON,
    );
    assert_eq!(
        cartesian_support_nonempty(&shared).unwrap(),
        cartesian_support_nonempty(&expanded).unwrap()
    );
    let bounds = Bounds::new_raw(0.25, 0.25, 0.75, 0.75);
    assert_eq!(
        exact_intersects_bounds(&shared, bounds).unwrap(),
        exact_intersects_bounds(&expanded, bounds).unwrap()
    );
}

#[test]
fn occupied_area_ingress_declines_collection_without_areal_members() {
    let shape = crate::io::parse_wkt("GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (0 0, 3 4))")
        .expect("valid nonareal collection");
    assert!(
        ExactSourceInventory::from_occupied_area_shape(&shape)
            .expect("nonareal collection is admitted")
            .is_none()
    );
    assert_eq!(occupied_cartesian_area(&shape).unwrap(), 0.0);
}
