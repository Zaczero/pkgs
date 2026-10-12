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
                for source_use in &role.source_uses {
                    bounded_source_use(source_use);
                    assert_eq!(
                        source_use.forward,
                        source_use
                            .start_parameter
                            .try_cmp(&source_use.end_parameter)
                            .unwrap()
                            != std::cmp::Ordering::Greater
                    );
                    if source_use.source_operand == Some(0) {
                        continue;
                    }
                    let segment = receipt_clip(&outputs.receipt, source_use.source_segment);
                    let source_ring = source_use
                        .source_ring
                        .expect("source edge use retains its original ring");
                    let source_edge = source_use
                        .source_edge
                        .expect("source edge use retains its original edge");
                    let root = segment
                        .source_roots
                        .iter()
                        .find(|root| {
                            root.ring == source_ring as usize && root.edge == source_edge as usize
                        })
                        .expect("source edge use names one of its clip roots");
                    let [edge_start, edge_end] = edge.endpoints;
                    let (start_id, end_id) = if role.forward {
                        (edge_start, edge_end)
                    } else {
                        (edge_end, edge_start)
                    };
                    let start_parameter = parameter_on_clip_segment(
                        segment,
                        receipt_vertex(&outputs.receipt, start_id),
                    );
                    let end_parameter = parameter_on_clip_segment(
                        segment,
                        receipt_vertex(&outputs.receipt, end_id),
                    );
                    let source_parameter = |parameter: &exact::ExactRational| {
                        if root.reversed {
                            exact::ExactRational::one().try_subtract(parameter).unwrap()
                        } else {
                            parameter.clone()
                        }
                    };
                    assert_eq!(
                        source_use.start_parameter,
                        source_parameter(&start_parameter)
                    );
                    assert_eq!(source_use.end_parameter, source_parameter(&end_parameter));
                }
            }
        }
    }
}

fn bounded_source_use(source_use: &VoronoiEdgeSourceUse) {
    for parameter in [&source_use.start_parameter, &source_use.end_parameter] {
        assert_ne!(
            parameter.try_cmp(&exact::ExactRational::zero()).unwrap(),
            std::cmp::Ordering::Less
        );
        assert_ne!(
            parameter.try_cmp(&exact::ExactRational::one()).unwrap(),
            std::cmp::Ordering::Greater
        );
    }
}

fn parameter_on_clip_segment(
    segment: &VoronoiClipSegmentReceipt,
    vertex: &VoronoiVertexReceipt,
) -> exact::ExactRational {
    let [start_x, start_y] = &segment.exact_start;
    let [end_x, end_y] = &segment.exact_end;
    let [point_x, point_y] = &vertex.exact_xy;
    let (start, end, point) = if start_x == end_x {
        (start_y, end_y, point_y)
    } else {
        (start_x, end_x, point_x)
    };
    let denominator = end.try_subtract(start).unwrap();
    assert!(!denominator.is_zero());
    point
        .try_subtract(start)
        .unwrap()
        .try_quotient(&denominator)
        .unwrap()
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
    assert!(
        outputs
            .iter()
            .flat_map(|output| &output.edge_roles)
            .flatten()
            .flat_map(|role| &role.source_uses)
            .filter(|source_use| source_use.source_operand == Some(1))
            .any(|source_use| {
                source_use.start_parameter != exact::ExactRational::zero()
                    || source_use.end_parameter != exact::ExactRational::one()
            })
    );
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

#[test]
fn retained_collinear_seam_cells_and_edges_use_the_admitted_chart() {
    let source = Shape::MultiPoint(
        vec![
            Point::new_raw_xy(172.0, 0.0),
            Point::new_raw_xy(-172.0, 0.0),
        ]
        .into(),
    );
    let clip = Polygon::new(
        Ring::closed(vec![
            Point::new_raw_xy(170.0, -4.0),
            Point::new_raw_xy(180.0, -4.0),
            Point::new_raw_xy(190.0, -4.0),
            Point::new_raw_xy(190.0, 4.0),
            Point::new_raw_xy(180.0, 4.0),
            Point::new_raw_xy(170.0, 4.0),
            Point::new_raw_xy(170.0, -4.0),
        ])
        .unwrap(),
        Vec::new(),
    );
    let frame = crate::geographic::GeographicFrame::crs84();
    let mut budget = ExpansionBudget::new("test", "retained collinear seam");
    let outputs = source
        .voronoi_polygon_outputs_in_frame_budgeted(
            0.0,
            VoronoiBoundary::Polygon(&clip),
            Some(frame),
            &mut budget,
        )
        .unwrap();
    assert_eq!(outputs.len(), 2);
    assert_eq!(outputs.receipt.producer(), VoronoiProducer::Collinear);
    assert_receipt_graph(&outputs.receipt);
    assert_polygon_receipt_alignment(&outputs);
    assert_eq!(outputs.receipt.cells.len(), 2);
    assert_eq!(outputs.receipt.separators.len(), 1);
    for separator in &outputs.receipt.separators {
        let mut expected_sites = separator.sites;
        expected_sites.sort_unstable();
        for &edge_id in &separator.edges {
            let edge = receipt_edge(&outputs.receipt, edge_id);
            let mut actual_owners: Vec<_> = edge.owner_transition.into_iter().flatten().collect();
            actual_owners.sort_unstable();
            assert_eq!(actual_owners.as_slice(), expected_sites.as_slice());
            let [Some(left_owner), Some(right_owner)] = edge.owner_transition else {
                panic!("the retained seam separator has owners on both sides");
            };
            let [start_id, end_id] = edge.endpoints;
            let start = receipt_vertex(&outputs.receipt, start_id)
                .assignment
                .expect("separator start has an assignment");
            let end = receipt_vertex(&outputs.receipt, end_id)
                .assignment
                .expect("separator end has an assignment");
            let site_xy = |site_id: u32| {
                let [x, y] = &receipt_site(&outputs.receipt, site_id).exact_xy;
                XY::new(x.round().unwrap(), y.round().unwrap())
            };
            assert_eq!(
                exact::orient2d(start, end, site_xy(left_owner)),
                ExactSign::Positive
            );
            assert_eq!(
                exact::orient2d(start, end, site_xy(right_owner)),
                ExactSign::Negative
            );
        }
    }
    assert!(
        outputs
            .iter()
            .flat_map(|output| &output.edge_roles)
            .flatten()
            .any(|role| !role.source_uses.is_empty())
    );

    let chart_sites = [
        Shape::Point(Point::new_raw_xy(172.0, 0.0)),
        Shape::Point(Point::new_raw_xy(188.0, 0.0)),
    ];
    for (site_index, site) in chart_sites.iter().enumerate() {
        let owner = outputs
            .iter()
            .position(|output| Shape::Polygon(output.polygon.clone()).covers(site).unwrap())
            .expect("each retained seam site lies in one output cell");
        assert_eq!(owner, site_index);
        assert_eq!(
            outputs
                .iter()
                .enumerate()
                .filter(|(_, output)| {
                    Shape::Polygon(output.polygon.clone()).covers(site).unwrap()
                })
                .count(),
            1
        );
    }

    let mut edge_budget = ExpansionBudget::new("test", "retained collinear seam edges");
    let output = source
        .voronoi_edge_column_in_frame_budgeted(
            0.0,
            VoronoiBoundary::Polygon(&clip),
            Some(frame),
            &mut edge_budget,
        )
        .unwrap();
    assert_eq!(output.offsets.as_slice(), &[0, 2]);
    assert_eq!(output.coordinates.xs(), &[180.0, 180.0]);
    assert_eq!(output.coordinates.ys(), &[-4.0, 4.0]);
    assert_eq!(output.receipt.producer(), VoronoiProducer::Collinear);
    assert_receipt_graph(&output.receipt);
    assert_edge_column_receipt_alignment(&output.coordinates, &output.offsets, &output.receipt);
    assert_eq!(output.receipt.separators.len(), 1);
}

#[test]
fn retained_seam_exact_and_dual_producers_keep_each_site_in_its_cell() {
    let source = Shape::MultiPoint(
        vec![
            Point::new_raw_xy(170.0, 0.0),
            Point::new_raw_xy(-170.0, 0.0),
            Point::new_raw_xy(-170.0, 10.0),
            Point::new_raw_xy(170.0, 8.0),
            Point::new_raw_xy(170.0, 0.0),
            Point::new_raw_xy(170.5, 0.0),
        ]
        .into(),
    );
    let frame = crate::geographic::GeographicFrame::crs84();
    let clip = Polygon::new(
        Ring::closed(vec![
            Point::new_raw_xy(160.0, -10.0),
            Point::new_raw_xy(200.0, -10.0),
            Point::new_raw_xy(200.0, 20.0),
            Point::new_raw_xy(160.0, 20.0),
            Point::new_raw_xy(160.0, -10.0),
        ])
        .unwrap(),
        Vec::new(),
    );
    let chart_sites = [
        Shape::Point(Point::new_raw_xy(170.0, 0.0)),
        Shape::Point(Point::new_raw_xy(190.0, 0.0)),
        Shape::Point(Point::new_raw_xy(190.0, 10.0)),
        Shape::Point(Point::new_raw_xy(170.0, 8.0)),
    ];

    let mut exact_budget = ExpansionBudget::new("test", "retained exact seam diagram");
    let exact_outputs = source
        .voronoi_polygon_outputs_in_frame_budgeted(
            1.0,
            VoronoiBoundary::Polygon(&clip),
            Some(frame),
            &mut exact_budget,
        )
        .unwrap();
    assert_eq!(exact_outputs.len(), chart_sites.len());
    assert_eq!(exact_outputs.receipt.producer(), VoronoiProducer::Exact);
    assert_receipt_graph(&exact_outputs.receipt);
    assert_polygon_receipt_alignment(&exact_outputs);
    assert_eq!(exact_outputs.receipt.sites.len(), 4);
    assert_eq!(exact_outputs.receipt.source_sites.len(), 6);
    assert_eq!(exact_outputs.receipt.sites[0].source_ordinal, 0);
    assert_eq!(
        exact_outputs.receipt.sites[0].source_record.xy(),
        XY::new(170.0, 0.0)
    );
    assert_eq!(exact_outputs.receipt.sites[0].quotient_aliases, [4]);
    assert_eq!(exact_outputs.receipt.sites[0].snapped_sources, [5]);
    assert!(exact_outputs.iter().all(|output| {
        output.owner.is_some_and(|owner| {
            exact_outputs
                .receipt
                .cells
                .iter()
                .any(|cell| cell.owner == owner)
        })
    }));
    for output in &exact_outputs {
        assert_eq!(output.edge_roles.len(), output.polygon.rings().count());
        for (ring, roles) in output.polygon.rings().zip(&output.edge_roles) {
            assert_eq!(roles.len(), ring.len().saturating_sub(1));
            assert!(roles.iter().all(|role| role.edge.is_some()));
        }
    }
    for site in &chart_sites {
        assert_eq!(
            exact_outputs
                .iter()
                .filter(|output| { Shape::Polygon(output.polygon.clone()).covers(site).unwrap() })
                .count(),
            1,
            "the exact producer assigns each retained chart site to one cell"
        );
    }

    let mut dual_budget = ExpansionBudget::new("test", "retained dual seam diagram");
    let dual_outputs = source
        .voronoi_polygon_outputs_in_frame_budgeted(
            1.0,
            VoronoiBoundary::Padded,
            Some(frame),
            &mut dual_budget,
        )
        .unwrap();
    assert_eq!(dual_outputs.len(), chart_sites.len());
    assert_eq!(dual_outputs.receipt.producer(), VoronoiProducer::Dual);
    assert_receipt_graph(&dual_outputs.receipt);
    assert_polygon_receipt_alignment(&dual_outputs);
    assert!(dual_outputs.receipt.vertices.iter().any(|vertex| {
        vertex
            .origins
            .iter()
            .any(|origin| matches!(origin, VoronoiVertexOrigin::Circumcenter { .. }))
    }));
    assert!(
        dual_outputs
            .receipt
            .darts
            .iter()
            .any(|dart| dart.twin.is_some())
    );
    assert!(
        dual_outputs
            .receipt
            .clip_segments
            .iter()
            .any(|segment| segment.frame_side.is_some() && !segment.atoms.is_empty())
    );
    for site in &chart_sites {
        assert_eq!(
            dual_outputs
                .iter()
                .filter(|output| { Shape::Polygon(output.polygon.clone()).covers(site).unwrap() })
                .count(),
            1,
            "the dual producer assigns each retained chart site to one cell"
        );
    }

    let dual_sites = canonical_voronoi_sites(
        [(170.0, 0.0), (190.0, 0.0), (190.0, 10.0), (170.0, 8.0)]
            .into_iter()
            .map(|(x, y)| Point::new_raw_xy(x, y))
            .collect(),
        0.0,
    )
    .unwrap();
    let mesh = certified_delaunay(&dual_sites).unwrap();
    // The chart trapezoid's unique Delaunay diagonal is (170, 8)-(190, 0).
    let mut center_receipts = Vec::new();
    let mut center_components = std::collections::BTreeSet::new();
    for vertex in &dual_outputs.receipt.vertices {
        for origin in &vertex.origins {
            if let VoronoiVertexOrigin::Circumcenter { component, sites } = origin {
                center_components.insert(*component);
                let mut sites = *sites;
                sites.sort_unstable();
                let [x, y] = &vertex.exact_xy;
                let assigned = XY::new(x.round().unwrap(), y.round().unwrap());
                assert_eq!(vertex.assignment, Some(assigned));
                center_receipts.push((sites, assigned));
            }
        }
    }
    center_receipts.sort_unstable_by_key(|(sites, _)| *sites);
    assert_eq!(center_components.len(), 2);
    assert_eq!(center_receipts, vec![
        ([0, 1, 2], XY::new(180.0, 4.0)),
        ([1, 2, 3], XY::new(180.4, 5.0)),
    ]);
    let mut rectangle_budget = ExpansionBudget::new("test", "retained dual source clip receipt");
    let dual_rectangle = dual_build_product(
        &dual_sites,
        VoronoiBoundary::Polygon(&clip),
        &mut rectangle_budget,
        &mesh,
        VoronoiProduct::Polygons,
        None,
    )
    .unwrap();
    let (dual_rectangle_cells, dual_rectangle_receipt) = dual_rectangle.into_polygon_outputs();
    assert_eq!(
        dual_rectangle_receipt.producer(),
        VoronoiProducer::Dual,
        "the axis-aligned source clip stays on the tagged dual producer"
    );
    assert_receipt_graph(&dual_rectangle_receipt);
    assert!(
        dual_rectangle_receipt
            .edges
            .iter()
            .filter(|edge| !edge.clip_sources.is_empty())
            .all(|edge| {
                edge.frame_side.is_none()
                    && !edge
                        .origins
                        .iter()
                        .any(|origin| matches!(origin, VoronoiEdgeOrigin::Frame { .. }))
            })
    );
    assert!(
        dual_rectangle_receipt
            .clip_segments
            .iter()
            .any(|segment| !segment.atoms.is_empty())
    );
    assert!(
        dual_rectangle_cells
            .iter()
            .flat_map(|cell| &cell.edge_roles)
            .flatten()
            .any(|role| role
                .origins
                .iter()
                .any(|origin| matches!(origin, VoronoiEdgeOrigin::SourceClip { .. }))
                && !role.source_uses.is_empty())
    );
}

#[test]
fn retained_lone_site_reuses_its_clip_and_empty_sites_stay_empty() {
    let frame = crate::geographic::GeographicFrame::crs84();
    let clip = Polygon::new(
        Ring::closed(vec![
            Point::new_raw_xy(170.0, -4.0),
            Point::new_raw_xy(180.0, -4.0),
            Point::new_raw_xy(190.0, -4.0),
            Point::new_raw_xy(190.0, 4.0),
            Point::new_raw_xy(180.0, 4.0),
            Point::new_raw_xy(170.0, 4.0),
            Point::new_raw_xy(170.0, -4.0),
        ])
        .unwrap(),
        Vec::new(),
    );
    let lone = Shape::MultiPoint(vec![Point::new_raw_xy(172.0, 0.0)].into());
    let mut budget = ExpansionBudget::new("test", "retained lone-site Voronoi");
    let outputs = lone
        .voronoi_polygon_outputs_in_frame_budgeted(
            0.0,
            VoronoiBoundary::Polygon(&clip),
            Some(frame),
            &mut budget,
        )
        .unwrap();
    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs.receipt.producer(), VoronoiProducer::LoneSite);
    assert_receipt_graph(&outputs.receipt);
    assert_polygon_receipt_alignment(&outputs);
    assert_eq!(outputs.receipt.cells.len(), 1);
    assert!(outputs.receipt.separators.is_empty());
    assert!(
        outputs
            .receipt
            .edges
            .iter()
            .all(|edge| edge.separator.is_none())
    );
    assert_eq!(
        outputs.receipt.edges.len(),
        clip.shell.len().saturating_sub(1)
    );
    assert!(outputs[0].edge_roles.iter().flatten().all(|role| {
        role.origins
            .iter()
            .all(|origin| matches!(origin, VoronoiEdgeOrigin::SourceClip { .. }))
    }));
    let mut lone_edge_budget = ExpansionBudget::new("test", "retained lone-site edges");
    let lone_edges = lone
        .voronoi_edge_column_in_frame_budgeted(
            0.0,
            VoronoiBoundary::Polygon(&clip),
            Some(frame),
            &mut lone_edge_budget,
        )
        .unwrap();
    assert_eq!(lone_edges.receipt.producer(), VoronoiProducer::LoneSite);
    assert_receipt_graph(&lone_edges.receipt);
    assert_edge_column_receipt_alignment(
        &lone_edges.coordinates,
        &lone_edges.offsets,
        &lone_edges.receipt,
    );
    let mut lone_envelope_budget = ExpansionBudget::new("test", "empty lone-site envelope");
    let lone_envelope = lone
        .voronoi_polygon_outputs_in_frame_budgeted(
            0.0,
            VoronoiBoundary::Envelope,
            Some(frame),
            &mut lone_envelope_budget,
        )
        .unwrap();
    assert!(lone_envelope.is_empty());
    assert_eq!(lone_envelope.receipt.producer(), VoronoiProducer::LoneSite);
    assert_receipt_graph(&lone_envelope.receipt);
    assert_polygon_receipt_alignment(&lone_envelope);
    assert!(lone_envelope.receipt.cells.is_empty());
    assert!(lone_envelope.receipt.separators.is_empty());
    let mut lone_envelope_edge_budget =
        ExpansionBudget::new("test", "empty lone-site envelope edges");
    let lone_envelope_edges = lone
        .voronoi_edge_column_in_frame_budgeted(
            0.0,
            VoronoiBoundary::Envelope,
            Some(frame),
            &mut lone_envelope_edge_budget,
        )
        .unwrap();
    assert_eq!(
        lone_envelope_edges.receipt.producer(),
        VoronoiProducer::LoneSite
    );
    assert_receipt_graph(&lone_envelope_edges.receipt);
    assert_edge_column_receipt_alignment(
        &lone_envelope_edges.coordinates,
        &lone_envelope_edges.offsets,
        &lone_envelope_edges.receipt,
    );

    let empty = Shape::typed_empty(
        crate::geometry::GeometryKind::MultiPoint,
        crate::geometry::CoordinateAxes::XY,
    );
    let mut empty_budget = ExpansionBudget::new("test", "empty retained Voronoi");
    let empty_outputs = empty
        .voronoi_polygon_outputs_in_frame_budgeted(
            0.0,
            VoronoiBoundary::Polygon(&clip),
            Some(frame),
            &mut empty_budget,
        )
        .unwrap();
    assert!(empty_outputs.is_empty());
    assert_eq!(empty_outputs.receipt.producer(), VoronoiProducer::Empty);
    assert_receipt_graph(&empty_outputs.receipt);
    assert_polygon_receipt_alignment(&empty_outputs);
    assert!(empty_outputs.receipt.sites.is_empty());
    let mut empty_edge_budget = ExpansionBudget::new("test", "empty Voronoi edges");
    let empty_edges = empty
        .voronoi_edge_column_in_frame_budgeted(
            0.0,
            VoronoiBoundary::Polygon(&clip),
            Some(frame),
            &mut empty_edge_budget,
        )
        .unwrap();
    assert_eq!(empty_edges.receipt.producer(), VoronoiProducer::Empty);
    assert_receipt_graph(&empty_edges.receipt);
    assert_edge_column_receipt_alignment(
        &empty_edges.coordinates,
        &empty_edges.offsets,
        &empty_edges.receipt,
    );
}
