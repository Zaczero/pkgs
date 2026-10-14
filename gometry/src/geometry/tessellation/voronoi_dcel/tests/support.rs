fn receipt_site(receipt: &VoronoiDiagramReceipt, id: u32) -> &VoronoiSiteReceipt {
    receipt
        .sites
        .get(id as usize)
        .filter(|site| site.id == id)
        .expect("receipt site IDs are dense")
}

fn receipt_vertex(receipt: &VoronoiDiagramReceipt, id: u32) -> &VoronoiVertexReceipt {
    receipt
        .vertices
        .get(id as usize)
        .expect("receipt vertex indices are dense")
}

fn receipt_edge(receipt: &VoronoiDiagramReceipt, id: u32) -> &VoronoiEdgeReceipt {
    receipt
        .edges
        .get(id as usize)
        .filter(|edge| edge.id == id)
        .expect("receipt edge IDs are dense")
}

fn assert_receipt_graph(receipt: &VoronoiDiagramReceipt) {
    let frame_side = |side| match side {
        VoronoiFrameSide::Bottom
        | VoronoiFrameSide::Right
        | VoronoiFrameSide::Top
        | VoronoiFrameSide::Left => {},
    };
    let ring_is_closed = |uses: &[VoronoiDirectedEdgeUse]| {
        let first = uses.first().expect("receipt cell rings are non-empty");
        let first_edge = receipt_edge(receipt, first.edge);
        let [edge_start, edge_end] = first_edge.endpoints;
        let start = if first.forward { edge_start } else { edge_end };
        let mut cursor = start;
        for use_ in uses {
            let [edge_start, edge_end] = receipt_edge(receipt, use_.edge).endpoints;
            let (from, to) = if use_.forward {
                (edge_start, edge_end)
            } else {
                (edge_end, edge_start)
            };
            assert_eq!(cursor, from, "cell edge uses form one directed ring");
            cursor = to;
        }
        assert_eq!(cursor, start, "cell edge uses close at their first vertex");
    };

    match receipt.producer() {
        VoronoiProducer::Exact
        | VoronoiProducer::Dual
        | VoronoiProducer::Collinear
        | VoronoiProducer::LoneSite
        | VoronoiProducer::Empty => {},
    }
    for (id, site_receipt) in receipt.sites.iter().enumerate() {
        assert_eq!(
            site_receipt.id,
            u32::try_from(id).expect("site receipt fits its stored ID")
        );
        assert!(site_receipt.source_record.x.is_finite());
        assert!(site_receipt.source_record.y.is_finite());
        if let Some(retained) = site_receipt.retained_chart_site {
            assert_eq!(retained.source_ordinal(), site_receipt.source_ordinal);
        }
        for &source in &site_receipt.quotient_aliases {
            assert_ne!(source, site_receipt.source_ordinal);
        }
        for &source in &site_receipt.snapped_sources {
            assert_ne!(source, site_receipt.source_ordinal);
        }
    }
    for source in &receipt.source_sites {
        let owner = match source.decision {
            VoronoiSourceSiteDecision::Selected { site: site_id } => {
                assert_eq!(
                    receipt_site(receipt, site_id).source_ordinal,
                    source.source_ordinal
                );
                site_id
            },
            VoronoiSourceSiteDecision::QuotientAlias { site: site_id } => {
                assert!(
                    receipt_site(receipt, site_id)
                        .quotient_aliases
                        .contains(&source.source_ordinal)
                );
                site_id
            },
            VoronoiSourceSiteDecision::Snapped { site: site_id } => {
                assert!(
                    receipt_site(receipt, site_id)
                        .snapped_sources
                        .contains(&source.source_ordinal)
                );
                site_id
            },
        };
        assert_eq!(receipt_site(receipt, owner).id, owner);
    }

    for vertex_receipt in &receipt.vertices {
        for origin in &vertex_receipt.origins {
            match origin {
                VoronoiVertexOrigin::SourceSite { site: id } => {
                    let _ = receipt_site(receipt, *id);
                },
                VoronoiVertexOrigin::OverlayNode { .. } => {},
                VoronoiVertexOrigin::Circumcenter {
                    sites: defining_sites,
                    ..
                } => {
                    for &id in defining_sites {
                        let _ = receipt_site(receipt, id);
                    }
                },
                VoronoiVertexOrigin::Bisector { sites } => {
                    for &id in sites {
                        let _ = receipt_site(receipt, id);
                    }
                },
                VoronoiVertexOrigin::Frame { side } => frame_side(*side),
            }
        }
    }

    for (id, edge_receipt) in receipt.edges.iter().enumerate() {
        assert_eq!(
            edge_receipt.id,
            u32::try_from(id).expect("edge receipt fits its stored ID")
        );
        for &vertex_id in &edge_receipt.endpoints {
            let _ = receipt_vertex(receipt, vertex_id);
        }
        if let Some(sites) = edge_receipt.separator {
            for site_id in sites {
                let _ = receipt_site(receipt, site_id);
            }
        }
        for &segment_id in &edge_receipt.clip_sources {
            assert!(
                edge_receipt
                    .origins
                    .contains(&VoronoiEdgeOrigin::SourceClip {
                        segment: segment_id,
                    })
            );
        }
        if let Some(side) = edge_receipt.frame_side {
            frame_side(side);
            assert!(
                edge_receipt
                    .origins
                    .contains(&VoronoiEdgeOrigin::Frame { side })
            );
        }
        for owner in edge_receipt.owner_transition.into_iter().flatten() {
            let _ = receipt_site(receipt, owner);
        }
        for origin in &edge_receipt.origins {
            match origin {
                VoronoiEdgeOrigin::SourceClip { segment } => {
                    assert!(edge_receipt.clip_sources.contains(segment));
                },
                VoronoiEdgeOrigin::Bisector { sites } => {
                    for &id in sites {
                        let _ = receipt_site(receipt, id);
                    }
                },
                VoronoiEdgeOrigin::Frame { side } => frame_side(*side),
            }
        }
    }

    let mut faces = std::collections::BTreeSet::new();
    for (id, dart) in receipt.darts.iter().enumerate() {
        assert_eq!(
            dart.id,
            u32::try_from(id).expect("dart receipt fits its stored ID")
        );
        let _ = receipt_vertex(receipt, dart.origin);
        let _ = receipt_vertex(receipt, dart.target);
        let _ = receipt_edge(receipt, dart.edge);
        if let Some(owner) = dart.owner {
            let _ = receipt_site(receipt, owner);
        }
        if let Some(face) = dart.face {
            faces.insert(face);
        }
        if let Some(twin_id) = dart.twin {
            let twin = receipt
                .darts
                .get(twin_id as usize)
                .filter(|twin| twin.id == twin_id)
                .expect("receipt twin dart exists");
            assert_eq!(twin.twin, Some(dart.id));
            assert_eq!(twin.edge, dart.edge);
            assert_eq!((twin.origin, twin.target), (dart.target, dart.origin));
        }
    }
    for cell in &receipt.cells {
        let _ = receipt_site(receipt, cell.owner);
        if let Some(face) = cell.face {
            assert!(faces.contains(&face));
        }
        ring_is_closed(&cell.shell);
        for hole in &cell.holes {
            ring_is_closed(hole);
        }
    }
    for separator in &receipt.separators {
        for &site_id in &separator.sites {
            let _ = receipt_site(receipt, site_id);
        }
        assert_eq!(
            separator.vertices.len(),
            separator
                .edges
                .len()
                .checked_add(1)
                .expect("separator edge count fits its vertex count")
        );
        for vertex_id in &separator.vertices {
            let _ = receipt_vertex(receipt, *vertex_id);
        }
        for (index, &edge_id) in separator.edges.iter().enumerate() {
            let edge_receipt = receipt_edge(receipt, edge_id);
            assert_eq!(edge_receipt.separator, Some(separator.sites));
            let left = *separator
                .vertices
                .get(index)
                .expect("separator start exists");
            let right = *separator
                .vertices
                .get(
                    index
                        .checked_add(1)
                        .expect("separator edge index fits its following vertex"),
                )
                .expect("separator end exists");
            assert!(
                edge_receipt.endpoints == [left, right] || edge_receipt.endpoints == [right, left]
            );
        }
    }
    let mut embedded_assignments = std::collections::BTreeMap::new();
    for assignment in &receipt.assignments {
        let vertex_receipt = receipt_vertex(receipt, assignment.vertex);
        assert_eq!(vertex_receipt.assignment, Some(assignment.point));
        if let Some(previous) = embedded_assignments.insert(assignment.embedded, assignment.point) {
            assert_eq!(
                PointKey::new(previous),
                PointKey::new(assignment.point),
                "one embedded assignment ID denotes one coordinate"
            );
        }
    }
}

fn assert_edge_column_receipt_alignment(
    coordinates: &CoordSeq,
    offsets: &CsrOffsetColumn,
    receipt: &VoronoiDiagramReceipt,
) {
    let offsets = offsets.as_slice();
    let line_ranges = offsets.array_windows::<2>();
    assert_eq!(offsets.len().saturating_sub(1), receipt.separators.len());
    for (separator, &[start, end]) in receipt.separators.iter().zip(line_ranges) {
        let start = start as usize;
        let end = end as usize;
        assert_eq!(
            end.checked_sub(start)
                .expect("separator coordinate offsets are ordered"),
            separator.vertices.len()
        );
        for (index, &vertex_id) in separator.vertices.iter().enumerate() {
            let assigned = receipt_vertex(receipt, vertex_id)
                .assignment
                .expect("published separator vertices have assignments");
            assert_eq!(
                PointKey::new(
                    coordinates
                        .point_at(
                            start
                                .checked_add(index)
                                .expect("separator coordinate offset fits its row"),
                        )
                        .xy(),
                ),
                PointKey::new(assigned)
            );
        }
    }
}

fn cycle(points: &[(f64, f64)]) -> Vec<exact::ExactPoint> {
    points
        .iter()
        .map(|&(x, y)| exact::ExactPoint::from_xy(XY::new(x, y)))
        .collect()
}

type RingCoordinateKey = Vec<(u64, u64)>;

type PolygonRingKey = (RingCoordinateKey, Vec<RingCoordinateKey>);

fn assert_exact_contacts_equal(actual: &[ExactCycleContact], expected: &[ExactCycleContact]) {
    assert_eq!(actual.len(), expected.len());
    for (actual, expected) in actual.iter().zip(expected) {
        assert_eq!(actual.segments, expected.segments);
        assert!(actual.point.same_position(&expected.point).unwrap());
    }
}

fn grafted_clip_subdivision(sites: &[Site], clip_polygon: &Polygon) -> ExactSubdivision {
    let clip = normalize_clip(sites, VoronoiBoundary::Polygon(clip_polygon), false).unwrap();
    let mesh = certified_delaunay(sites).unwrap();
    let complex = delaunay_complex(&mesh, sites).unwrap();
    let primal_edges = mesh.primal_edges();
    let adjacency = certified_site_adjacency(&primal_edges, sites.len()).unwrap();
    let rect = clip_rectangle(&clip).unwrap();
    let scaffold = build_scaffold(&rect, &clip).unwrap();
    let separators = emit_clipped_separators(sites, &primal_edges, &complex, &rect).unwrap();
    let atoms = node_scaffold_and_separators(scaffold, separators, sites, &adjacency, &clip, &rect)
        .unwrap();
    let mut subdivision = build_exact_subdivision(atoms).unwrap();
    graft_components(&mut subdivision, sites, &adjacency).unwrap();
    subdivision
}

fn rectangle(min_x: f64, min_y: f64, max_x: f64, max_y: f64) -> Ring {
    Ring::closed(
        [
            (min_x, min_y),
            (max_x, min_y),
            (max_x, max_y),
            (min_x, max_y),
            (min_x, min_y),
        ]
        .into_iter()
        .map(|(x, y)| Point::new_raw_xy(x, y))
        .collect(),
    )
    .unwrap()
}

fn sites_xy(points: &[(f64, f64)]) -> Vec<Site> {
    points
        .iter()
        .enumerate()
        .map(|(id, &(x, y))| Site::raw(id, Point::new_raw_xy(x, y), id))
        .collect()
}

fn separator_fixture() -> (Vec<Site>, Polygon) {
    (
        sites_xy(&[
            (-4.0, -3.0),
            (4.0, -2.0),
            (3.0, 4.0),
            (-3.0, 3.0),
            (0.0, 0.0),
        ]),
        Polygon::new(rectangle(-6.0, -6.0, 6.0, 6.0), Vec::new()),
    )
}

fn node_polygon(sites: &[Site], polygon: &Polygon) -> NodedAtoms {
    let clip = normalize_clip(sites, VoronoiBoundary::Polygon(polygon), false).unwrap();
    let rect = clip_rectangle(&clip).unwrap();
    let mesh = certified_delaunay(sites).unwrap();
    let complex = delaunay_complex(&mesh, sites).unwrap();
    let primal_edges = mesh.primal_edges();
    let adjacency = certified_site_adjacency(&primal_edges, sites.len()).unwrap();
    let scaffold = build_scaffold(&rect, &clip).unwrap();
    let separators = emit_clipped_separators(sites, &primal_edges, &complex, &rect).unwrap();
    node_scaffold_and_separators(scaffold, separators, sites, &adjacency, &clip, &rect).unwrap()
}

fn closed_ring(points: &[(f64, f64)]) -> Ring {
    Ring::closed(
        points
            .iter()
            .chain(points.first())
            .map(|&(x, y)| Point::new_raw_xy(x, y))
            .collect(),
    )
    .unwrap()
}

fn site_shape(points: &[(f64, f64)]) -> Shape {
    Shape::MultiPoint(
        points
            .iter()
            .map(|&(x, y)| Point::new_raw_xy(x, y))
            .collect::<Vec<_>>()
            .into(),
    )
}

fn task163_tangent_clip(hole_count: usize) -> Polygon {
    let holes = (0..hole_count)
        .map(|index| {
            let ordinal =
                f64::from(u32::try_from(index).expect("the fixture has fewer than u32 holes"));
            let x = 1.0 + ordinal * 1.4;
            closed_ring(&[(x, 10.0), (x + 0.25, 9.5), (x - 0.25, 9.5)])
        })
        .collect();
    Polygon::new(rectangle(0.0, 0.0, 2000.0, 10.0), holes)
}

#[expect(
    clippy::arithmetic_side_effects,
    reason = "Ring constructors admit an explicit repeated closing coordinate"
)]
fn ring_coordinate_key(ring: &Ring) -> RingCoordinateKey {
    let mut coordinates: Vec<_> = ring
        .iter()
        .take(ring.len() - 1)
        .map(|point| (point.x.to_bits(), point.y.to_bits()))
        .collect();
    coordinates.sort_unstable();
    coordinates
}

fn polygon_ring_keys(polygon: &Polygon) -> PolygonRingKey {
    let shell = ring_coordinate_key(&polygon.shell);
    let mut holes: Vec<_> = polygon.holes.iter().map(ring_coordinate_key).collect();
    holes.sort_unstable();
    (shell, holes)
}
