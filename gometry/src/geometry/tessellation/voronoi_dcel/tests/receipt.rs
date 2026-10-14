fn assert_polygon_receipt_alignment(outputs: &crate::geometry::VoronoiPolygonOutputs) {
    assert_eq!(outputs.len(), outputs.receipt.cells.len());
    for (output, cell) in outputs.iter().zip(&outputs.receipt.cells) {
        assert_eq!(output.owner, Some(cell.owner));
        let cell_rings = std::iter::once(&cell.shell).chain(cell.holes.iter());
        assert_eq!(output.edge_roles.len(), cell_rings.clone().count());
        for ((ring, roles), edge_uses) in output
            .polygon
            .rings()
            .zip(&output.edge_roles)
            .zip(cell_rings)
        {
            let open_count = ring.len().saturating_sub(1);
            let points = ring
                .iter()
                .take(open_count)
                .map(|point| point.xy())
                .collect::<Vec<_>>();
            assert!(!points.is_empty());
            assert_eq!(roles.len(), edge_uses.len());
            assert_eq!(roles.len(), open_count);
            for (index, (role, edge_use)) in roles.iter().zip(edge_uses).enumerate() {
                assert_eq!(role.edge, Some(edge_use.edge));
                assert_eq!(role.forward, edge_use.forward);
                let edge = receipt_edge(&outputs.receipt, edge_use.edge);
                assert!(
                    role.origins
                        .iter()
                        .all(|origin| edge.origins.contains(origin))
                );
                let [edge_start, edge_end] = edge.endpoints;
                let edge_start = receipt_vertex(&outputs.receipt, edge_start)
                    .assignment
                    .expect("published edge endpoints have assignments");
                let edge_end = receipt_vertex(&outputs.receipt, edge_end)
                    .assignment
                    .expect("published edge endpoints have assignments");
                let (expected_start, expected_end) = if role.forward {
                    (edge_start, edge_end)
                } else {
                    (edge_end, edge_start)
                };
                let actual_start = *points.get(index).expect("ring edge start exists");
                let next_index = index
                    .checked_add(1)
                    .filter(|&next| next < open_count)
                    .unwrap_or(0);
                let actual_end = *points.get(next_index).expect("ring edge end exists");
                assert_eq!(PointKey::new(actual_start), PointKey::new(expected_start));
                assert_eq!(PointKey::new(actual_end), PointKey::new(expected_end));
            }
        }
    }
}

#[test]
fn source_clip_contacts_keep_parameters_through_ring_normalization() {
    let polygon = Polygon::new(
        Ring::closed(vec![
            Point::new_raw_xy(0.0, 8.0),
            Point::new_raw_xy(8.0, 8.0),
            Point::new_raw_xy(8.0, 0.0),
            Point::new_raw_xy(8.0, 0.0),
            Point::new_raw_xy(0.0, 0.0),
        ])
        .unwrap(),
        vec![
            closed_ring(&[(4.0, 4.0), (6.0, 4.0), (6.0, 6.0), (4.0, 6.0)]),
            Ring::closed(vec![
                Point::new_raw_xy(2.0, 3.0),
                Point::new_raw_xy(2.0, 3.0),
                Point::new_raw_xy(2.0, 5.0),
                Point::new_raw_xy(-0.0, 4.0),
            ])
            .unwrap(),
        ],
    );
    assert!(polygon.holes[1].point_at(3).x.is_sign_negative());

    let mut source_contacts = Vec::new();
    assert!(
        polygon
            .validate_with_contacts("polygon", "$", &mut source_contacts)
            .unwrap()
            .is_none()
    );
    assert_eq!(source_contacts.len(), 2);
    for contact in &source_contacts {
        assert_eq!(contact.rings, [0, 2]);
        assert_eq!(contact.edges[0], 4);
        assert_eq!(
            PointKey::new(contact.point),
            PointKey::new(XY::new(0.0, 4.0))
        );
        let SegmentParameter::Ratio { num, den } = &contact.parameters[0] else {
            panic!("the shell contact is interior to its source edge");
        };
        assert_eq!(
            exact::ExactRational::new(num.clone(), den.clone())
                .unwrap()
                .round()
                .unwrap(),
            0.5
        );
        match contact.edges[1] {
            2 => assert!(matches!(contact.parameters[1], SegmentParameter::End)),
            3 => assert!(matches!(contact.parameters[1], SegmentParameter::Start)),
            edge => panic!("unexpected source hole edge {edge}"),
        }
    }

    let sites = sites_xy(&[(-1.0, 0.0), (1.0, 0.0)]);
    let clip = normalize_clip(&sites, VoronoiBoundary::Polygon(&polygon), false).unwrap();
    assert_eq!(
        clip.contacts
            .iter()
            .map(|contact| contact.segments)
            .collect::<Vec<_>>(),
        [[3, 4], [3, 6]]
    );
    assert!(clip.contacts.iter().all(|contact| {
        contact
            .point
            .same_position(&exact::ExactPoint::from_xy(XY::new(0.0, 4.0)))
            .unwrap()
    }));
    let rect = clip_rectangle(&clip).unwrap();
    let scaffold = build_scaffold(&rect, &clip).unwrap();
    for segment in [3_u32, 4, 6] {
        let primitive = scaffold
            .get(segment as usize)
            .expect("each normalized contact edge has a scaffold primitive");
        assert_eq!(primitive.clip_source, Some(ClipSegmentId(segment)));
        assert!(primitive.events.iter().any(|point| {
            point
                .same_position(&exact::ExactPoint::from_xy(XY::new(0.0, 4.0)))
                .unwrap()
        }));
    }
    for event in &scaffold[3].events[..2] {
        assert!(
            event
                .same_position(&exact::ExactPoint::from_xy(XY::new(0.0, 4.0)))
                .unwrap()
        );
    }

    let source = Shape::MultiPoint(
        sites
            .iter()
            .map(|site| site.point)
            .collect::<Vec<_>>()
            .into(),
    );
    let mut budget = ExpansionBudget::new("voronoi_cells", "sites/clip topology");
    let outputs = source
        .voronoi_polygon_outputs_budgeted(0.0, VoronoiBoundary::Polygon(&polygon), &mut budget)
        .unwrap();
    assert_eq!(outputs.receipt.producer(), VoronoiProducer::Collinear);
    assert_receipt_graph(&outputs.receipt);
    assert_polygon_receipt_alignment(&outputs);
    let cells: Vec<_> = outputs
        .iter()
        .map(|output| Shape::Polygon(output.polygon.clone()))
        .collect();
    assert_eq!(cells.len(), 1);
    assert!(
        cells.iter().all(
            |cell| matches!(cell, Shape::Polygon(polygon) if polygon_is_valid(polygon).unwrap())
        )
    );
    let expected = Shape::Polygon(Polygon::new(
        closed_ring(&[(-0.0, 4.0), (0.0, 0.0), (8.0, 0.0), (8.0, 8.0), (0.0, 8.0)]),
        vec![
            closed_ring(&[(-0.0, 4.0), (2.0, 5.0), (2.0, 3.0)]),
            closed_ring(&[(4.0, 4.0), (4.0, 6.0), (6.0, 6.0), (6.0, 4.0)]),
        ],
    ));
    assert_eq!(
        crate::io::to_wkb(&cells[0], None, false).unwrap(),
        crate::io::to_wkb(&expected, None, false).unwrap()
    );
}

#[test]
fn nonrectangular_shell_contacts_keep_scaffold_events_in_source_order() {
    let polygon = Polygon::new(
        closed_ring(&[
            (0.0, 0.0),
            (10.0, 0.0),
            (10.0, 10.0),
            (5.0, 15.0),
            (0.0, 10.0),
        ]),
        vec![
            closed_ring(&[(8.0, 12.0), (8.25, 11.5), (7.75, 11.5)]),
            closed_ring(&[(6.0, 14.0), (6.25, 13.5), (5.75, 13.5)]),
        ],
    );
    let sites = sites_xy(&[(3.0, 4.0), (7.0, 4.0)]);
    let clip = normalize_clip(&sites, VoronoiBoundary::Polygon(&polygon), false).unwrap();
    assert_eq!(
        clip.contacts
            .iter()
            .map(|contact| contact.segments)
            .collect::<Vec<_>>(),
        [[2, 5], [2, 6], [2, 8], [2, 9]]
    );

    let scaffold = build_scaffold(&clip_rectangle(&clip).unwrap(), &clip).unwrap();
    assert!(
        scaffold[..4]
            .iter()
            .all(|primitive| primitive.clip_source.is_none())
    );
    let first_contact = exact::ExactPoint::from_xy(XY::new(6.0, 14.0));
    let second_contact = exact::ExactPoint::from_xy(XY::new(8.0, 12.0));
    for (actual, expected) in scaffold[6].events[..4].iter().zip([
        &first_contact,
        &first_contact,
        &second_contact,
        &second_contact,
    ]) {
        assert!(actual.same_position(expected).unwrap());
    }
    for (segment, scaffold_index, point) in [
        (2_u32, 6_usize, &first_contact),
        (5, 9, &first_contact),
        (6, 10, &first_contact),
        (8, 12, &second_contact),
        (9, 13, &second_contact),
    ] {
        let primitive = scaffold
            .get(scaffold_index)
            .expect("normalized clip segment has its scaffold primitive");
        assert_eq!(primitive.clip_source, Some(ClipSegmentId(segment)));
        assert!(primitive.events[0].same_position(point).unwrap());
    }

    let source = Shape::MultiPoint(
        sites
            .iter()
            .map(|site| site.point)
            .collect::<Vec<_>>()
            .into(),
    );
    let mut budget = ExpansionBudget::new("voronoi_cells", "sites/clip topology");
    let cells = source
        .voronoi_cells_budgeted(0.0, VoronoiBoundary::Polygon(&polygon), &mut budget)
        .unwrap();
    assert_eq!(cells.len(), 2);
    assert!(
        cells.iter().all(
            |cell| matches!(cell, Shape::Polygon(polygon) if polygon_is_valid(polygon).unwrap())
        )
    );
    let expected = [
        Shape::Polygon(Polygon::new(
            closed_ring(&[(0.0, 0.0), (5.0, 0.0), (5.0, 15.0), (0.0, 10.0)]),
            Vec::new(),
        )),
        Shape::Polygon(Polygon::new(
            closed_ring(&[
                (5.0, 0.0),
                (10.0, 0.0),
                (10.0, 10.0),
                (8.0, 12.0),
                (6.0, 14.0),
                (5.0, 15.0),
            ]),
            vec![
                closed_ring(&[(5.75, 13.5), (6.0, 14.0), (6.25, 13.5)]),
                closed_ring(&[(7.75, 11.5), (8.0, 12.0), (8.25, 11.5)]),
            ],
        )),
    ];
    for (actual, expected) in cells.iter().zip(expected) {
        assert_eq!(
            crate::io::to_wkb(actual, None, false).unwrap(),
            crate::io::to_wkb(&expected, None, false).unwrap()
        );
    }
    let mut role_budget = ExpansionBudget::new("voronoi_cells", "sites/clip topology");
    let outputs = source
        .voronoi_polygon_outputs_budgeted(0.0, VoronoiBoundary::Polygon(&polygon), &mut role_budget)
        .unwrap();
    assert_eq!(outputs.len(), 2);
    assert_eq!(outputs.receipt.producer(), VoronoiProducer::Collinear);
    assert_receipt_graph(&outputs.receipt);
    assert_polygon_receipt_alignment(&outputs);
    assert!(outputs.receipt.vertices.iter().all(|vertex| {
        vertex
            .origins
            .iter()
            .any(|origin| matches!(origin, VoronoiVertexOrigin::OverlayNode { .. }))
    }));
    for output in &outputs {
        assert_eq!(output.edge_roles.len(), output.polygon.rings().count());
        assert!(
            output
                .edge_roles
                .iter()
                .flatten()
                .flat_map(|role| &role.origins)
                .any(|origin| !matches!(origin, VoronoiEdgeOrigin::SourceClip { .. }))
        );
        assert!(
            output
                .edge_roles
                .iter()
                .flatten()
                .flat_map(|role| &role.origins)
                .any(|origin| matches!(origin, VoronoiEdgeOrigin::SourceClip { .. }))
        );
    }

    let mut edge_budget = ExpansionBudget::new("voronoi_edge_lines", "sites/clip topology");
    let (coordinates, offsets) = source
        .voronoi_edge_column_budgeted(0.0, VoronoiBoundary::Polygon(&polygon), &mut edge_budget)
        .expect("clip-only shell and hole contacts do not invalidate Voronoi edges");
    assert_eq!(offsets.as_slice(), [0, 2]);
    assert_eq!(coordinates.len(), 2);
    assert_eq!(coordinates.point_at(0).xy(), XY::new(5.0, 0.0));
    assert_eq!(coordinates.point_at(1).xy(), XY::new(5.0, 15.0));
}

#[test]
fn source_clip_records_point_sized_collinear_contacts() {
    let polygon = Polygon::new(rectangle(0.0, 0.0, 10.0, 10.0), vec![
        closed_ring(&[(3.0, 5.0), (5.0, 5.0), (4.0, 7.0)]),
        closed_ring(&[(5.0, 5.0), (7.0, 5.0), (6.0, 7.0)]),
    ]);
    let mut source_contacts = Vec::new();
    assert!(
        polygon
            .validate_with_contacts("polygon", "$", &mut source_contacts)
            .unwrap()
            .is_none()
    );

    let clip = normalize_clip(
        &sites_xy(&[(-1.0, 0.0), (1.0, 0.0)]),
        VoronoiBoundary::Polygon(&polygon),
        false,
    )
    .unwrap();
    let shared_point = exact::ExactPoint::from_xy(XY::new(5.0, 5.0));
    assert!(
        clip.contacts
            .iter()
            .any(|contact| { contact.point.same_position(&shared_point).unwrap() })
    );
    let scaffold = build_scaffold(&clip_rectangle(&clip).unwrap(), &clip).unwrap();
    let contacted: Vec<_> = scaffold
        .iter()
        .filter(|primitive| {
            primitive
                .events
                .iter()
                .any(|point| point.same_position(&shared_point).unwrap())
        })
        .filter_map(|primitive| primitive.clip_source)
        .collect();
    assert!(contacted.len() >= 2);
}

#[test]
fn packed_separator_column_matches_chain_coordinates() {
    let (sites, polygon) = separator_fixture();
    let mut budget = ExpansionBudget::new("test", "packed chains");
    let packed = build_edges(
        &sites,
        VoronoiBoundary::Polygon(&polygon),
        &mut budget,
        true,
        &certified_delaunay(&sites).unwrap(),
    )
    .unwrap();
    let (coords, offsets, receipt) = packed.clone().into_line_column_with_receipt().unwrap();
    assert_eq!(receipt.producer(), VoronoiProducer::Exact);
    assert_receipt_graph(&receipt);
    assert_edge_column_receipt_alignment(&coords, &offsets, &receipt);
    let lines = packed.into_line_seqs().unwrap();
    assert!(!lines.is_empty());
    let ends = offsets.as_slice();
    assert_eq!(ends.len(), lines.len() + 1);
    assert_eq!(ends[0], 0);
    assert_eq!(ends[ends.len() - 1] as usize, coords.len());
    let mut cursor = 0_usize;
    for (line, &end) in lines.iter().zip(&ends[1..]) {
        let end = end as usize;
        assert_eq!(end - cursor, line.len());
        for index in 0..line.len() {
            assert_eq!(
                coords.point_at(cursor + index).xy(),
                line.point_at(index).xy()
            );
        }
        cursor = end;
    }
}

#[test]
fn geographic_voronoi_zero_meridian_class_preserves_clip_spelling() {
    let sites = Shape::MultiPoint(
        vec![
            Point::new_raw_xy(240.0, 5.0),
            Point::new_raw_xy(260.0, 5.0),
            Point::new_raw_xy(250.0, 15.0),
        ]
        .into(),
    );
    let clip = Polygon::new(
        closed_ring(&[(250.0, 0.0), (240.0, 10.0), (260.0, 10.0)]),
        Vec::new(),
    );
    let mut budget = ExpansionBudget::new("voronoi_cells", "sites/clip topology");
    let outputs = sites
        .voronoi_polygon_outputs_budgeted(0.0, VoronoiBoundary::Polygon(&clip), &mut budget)
        .unwrap();
    let frame = crate::geographic::GeographicFrame::crs84();
    let axis = frame.longitude();
    let mut edge_budget = ExpansionBudget::new("voronoi_cells", "computed edges");
    let published =
        crate::geometry::publish_voronoi_polygon_outputs(outputs, Some(frame), &mut edge_budget)
            .unwrap();
    let (published, _receipt) = published.into_parts();
    let mut keeps_clip_spelling = false;
    let mut meridian_reads_zero = false;
    for shape in &published {
        let Shape::Polygon(polygon) = shape else {
            panic!("Voronoi polygon publication changed the output kind");
        };
        for ring in polygon.rings() {
            let points = ring.coords().iter().collect::<Vec<_>>();
            keeps_clip_spelling |= points
                .iter()
                .any(|point| point.x.to_bits() == 250.0_f64.to_bits());
            for pair in points.array_windows::<2>() {
                if pair[0].x.to_bits() == 250.0_f64.to_bits()
                    && pair[1].x.to_bits() == 250.0_f64.to_bits()
                    && pair[0].y.to_bits() != pair[1].y.to_bits()
                {
                    meridian_reads_zero |=
                        axis.chart_step(pair[0].x, pair[1].x).unwrap().delta(axis) == 0.0;
                }
            }
        }
    }
    assert!(
        keeps_clip_spelling,
        "the clip longitude 250 must survive publication"
    );
    assert!(
        meridian_reads_zero,
        "the preserved meridian edge must read as zero longitude displacement"
    );
}
