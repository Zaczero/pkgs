#[test]
fn generated_budget_charges_only_net_canonical_growth() {
    use crate::geometry::generated_budget::GENERATED_ITEM_LIMIT;

    let crossing = [seg(0.0, 0.0, 2.0, 2.0), seg(0.0, 2.0, 2.0, 0.0)];
    let cases = [
        (vec![seg(0.0, 0.0, 1.0, 0.0)], 0),
        (vec![seg(0.0, 0.0, 1.0, 0.0); 2], 0),
        (vec![seg(0.0, 0.0, 1.0, 0.0), seg(1.0, 0.0, 2.0, 0.0)], 0),
        (vec![seg(1.0, 1.0, 1.0, 1.0)], 0),
        (crossing.to_vec(), 3),
        (vec![seg(0.0, 0.0, 4.0, 0.0), seg(1.0, 0.0, 3.0, 0.0)], 1),
        (vec![seg(0.0, 0.0, 2.0, 0.0), seg(1.0, 0.0, 1.0, 0.0)], 1),
        (vec![crossing[0], crossing[1], seg(1.0, 1.0, 1.0, 1.0)], 2),
        (
            vec![
                crossing[0],
                crossing[1],
                seg(10.0, 0.0, 14.0, 0.0),
                seg(10.0, 0.0, 12.0, 0.0),
                seg(12.0, 0.0, 14.0, 0.0),
            ],
            2,
        ),
    ];
    for (segments, growth) in cases {
        let case = run(&segments);
        let mut budget = ExpansionBudget::new("exact_partition", "items");
        budget.add(GENERATED_ITEM_LIMIT - growth).unwrap();
        let partition = partition_unique_segments::<true>(
            &case.unique,
            &case.provenance,
            &case.isolated,
            &mut budget,
        )
        .unwrap();
        assert_eq!(budget.used(), GENERATED_ITEM_LIMIT);
        assert!(partition.node_count() > 0);
        if growth > 0 {
            let mut short = ExpansionBudget::new("exact_partition", "items");
            short.add(GENERATED_ITEM_LIMIT - growth + 1).unwrap();
            let Err(error) = partition_unique_segments::<true>(
                &case.unique,
                &case.provenance,
                &case.isolated,
                &mut short,
            ) else {
                panic!("one fewer generated item must reject the partition");
            };
            assert!(matches!(
                error.kind(),
                crate::error::ErrorKind::Geometry(
                    GeometryErrorKind::GeneratedOutputTooLarge { .. }
                )
            ));
        }
    }
}

#[test]
fn zero_length_unique_sources_do_not_credit_atom_growth() {
    use crate::geometry::generated_budget::GENERATED_ITEM_LIMIT;

    let (unique, provenance) = dedup_undirected_segments_sourced(vec![
        seg(0.0, 0.0, 2.0, 2.0),
        seg(0.0, 2.0, 2.0, 0.0),
        seg(10.0, 10.0, 10.0, 10.0),
    ])
    .unwrap();
    for ordinates in [false, true] {
        let mut budget = ExpansionBudget::new("exact_partition", "items");
        budget.add(GENERATED_ITEM_LIMIT - 3).unwrap();
        let partition = if ordinates {
            partition_unique_segments::<true>(&unique, &provenance, &[], &mut budget)
        } else {
            partition_unique_segments::<false>(&unique, &provenance, &[], &mut budget)
        }
        .unwrap();
        assert_eq!(partition.node_count(), 6);
        assert_eq!(partition.spans.len(), 4);
        assert_eq!(partition.isolated.len(), 1);
        assert_eq!(budget.used(), GENERATED_ITEM_LIMIT);
    }
}

#[test]
fn intermediate_partition_can_run_after_the_output_budget_is_exhausted() {
    use crate::geometry::generated_budget::GENERATED_ITEM_LIMIT;

    let crossing = [seg(0.0, 0.0, 2.0, 2.0), seg(0.0, 2.0, 2.0, 0.0)];
    let inventory = ExactSourceInventory::from_segments(&crossing).unwrap();
    let mut exhausted = ExpansionBudget::new("voronoi", "published output");
    exhausted.add(GENERATED_ITEM_LIMIT - 1).unwrap();
    let Err(error) = inventory.partition(&mut exhausted) else {
        panic!("partition growth exceeded the remaining output budget");
    };
    assert!(matches!(
        error.kind(),
        crate::error::ErrorKind::Geometry(GeometryErrorKind::GeneratedOutputTooLarge { .. })
    ));

    let partition = inventory.partition_without_budget().unwrap();
    assert_eq!(partition.node_count(), 5);
    assert_eq!(partition.spans.len(), 4);
}

#[test]
fn line_polygon_selection_preserves_charged_and_unbudgeted_partition_modes() {
    use crate::geometry::OverlayOp;
    use crate::geometry::generated_budget::GENERATED_ITEM_LIMIT;

    let line = crate::io::parse_wkt("LINESTRING (-1 1,3 1)").unwrap();
    let polygon = crate::io::parse_wkt("POLYGON ((0 0,2 0,2 2,1 3,0 2,0 0))").unwrap();
    let inventory = ExactSourceInventory::from_shapes(&[&line, &polygon]).unwrap();
    let mut budget = ExpansionBudget::new("intersection", "exact partition");
    budget.add(GENERATED_ITEM_LIMIT - 1).unwrap();
    let error = inventory
        .overlay_line_polygon(
            &polygon,
            0,
            OverlayOp::Intersection,
            CoordinateAxes::XY,
            false,
            Some(&mut budget),
        )
        .unwrap_err();
    assert!(matches!(
        error.kind(),
        crate::error::ErrorKind::Geometry(GeometryErrorKind::GeneratedOutputTooLarge { .. })
    ));
    let output = inventory
        .overlay_line_polygon(
            &polygon,
            0,
            OverlayOp::Intersection,
            CoordinateAxes::XY,
            false,
            None,
        )
        .unwrap();
    let Shape::LineString(line) = output else {
        panic!("the closed intersection is one line");
    };
    assert_eq!(line.xs(), &[0.0, 2.0]);
    assert_eq!(line.ys(), &[1.0, 1.0]);
}
