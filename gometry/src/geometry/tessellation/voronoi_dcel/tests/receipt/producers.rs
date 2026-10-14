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
        dual_rectangle_cells
            .iter()
            .flat_map(|cell| &cell.edge_roles)
            .flatten()
            .any(|role| role
                .origins
                .iter()
                .any(|origin| matches!(origin, VoronoiEdgeOrigin::SourceClip { .. })))
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
