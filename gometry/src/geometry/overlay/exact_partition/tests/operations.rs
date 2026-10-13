/// One shared operand must select exactly the cells separate operands do.
#[test]
fn polygonal_union_as_one_operand_matches_separate_operands() {
    let shapes: Vec<Shape> = [
        "POLYGON ((0 0,4 0,4 4,0 4,0 0),(1 1,3 1,3 3,1 3,1 1))",
        "POLYGON ((0 0,4 0,4 4,0 4,0 0),(1 1,3 1,3 3,1 3,1 1))",
        "POLYGON ((2 2,6 2,6 6,2 6,2 2))",
        "POLYGON ((4 0,8 0,8 2,4 2,4 0))",
        "POLYGON ((1.5 1.5,2.5 1.5,2.5 2.5,1.5 2.5,1.5 1.5))",
        "MULTIPOLYGON (((10 10,11 10,11 11,10 11,10 10)),((11 11,12 11,12 12,11 12,11 11)))",
        "POLYGON ((0 0,1 -1,2 0,1 1,0 0))",
    ]
    .iter()
    .map(|wkt| crate::io::parse_wkt(wkt).unwrap())
    .collect();
    let union = |inventory: ExactSourceInventory, inputs: &[&Shape]| {
        let mut budget = ExpansionBudget::new("union", "exact partition");
        let partition = inventory.partition(&mut budget).unwrap();
        let axes = super::super::overlay_declared_axes(inputs, crate::geometry::OverlayOp::Union);
        inventory
            .overlay_shape::<crate::crs::GeodesicMetric>(
                &partition,
                crate::geometry::OverlayOp::Union,
                axes,
                true,
                &crate::geometry::SourceArcContext::cartesian(),
            )
            .unwrap()
    };
    for count in 2..=shapes.len() {
        let inputs: Vec<&Shape> = shapes[..count].iter().collect();
        let shared = ExactSourceInventory::from_polygonal_union(&inputs)
            .unwrap()
            .expect("polygonal inputs share one operand");
        let separate = ExactSourceInventory::from_shapes(&inputs).unwrap();
        assert_eq!(
            union(shared, &inputs),
            union(separate, &inputs),
            "{count} inputs"
        );
    }
    let line = crate::io::parse_wkt("LINESTRING (0 0,1 1)").unwrap();
    assert!(
        ExactSourceInventory::from_polygonal_union(&[&shapes[0], &line])
            .unwrap()
            .is_none()
    );
}
