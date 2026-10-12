use crate::geometry::overlay::shoot_left;

#[test]
fn direct_dual_subnormal_admission_matches_legacy_builder() {
    for (units, admitted) in [(1_u64, false), (4, true)] {
        let step = f64::from_bits(units);
        let sites: Vec<_> = [(0.0, 0.0), (step, 0.0), (0.0, step)]
            .into_iter()
            .enumerate()
            .map(|(id, (x, y))| Site::raw(id, Point::new_raw_xy(x, y), id))
            .collect();
        let mut dual_budget = ExpansionBudget::new("test", "subnormal dual");
        let dual = dual_build(
            &sites,
            VoronoiBoundary::Envelope,
            &mut dual_budget,
            &certified_delaunay(&sites).unwrap(),
        );
        let mut legacy_budget = ExpansionBudget::new("test", "subnormal legacy");
        let legacy = build(
            &sites,
            VoronoiBoundary::Envelope,
            &mut legacy_budget,
            false,
            &certified_delaunay(&sites).unwrap(),
        );

        match (admitted, dual, legacy) {
            (false, Err(dual), Err(legacy)) => {
                assert!(
                    matches!(
                        dual.kind(),
                        crate::error::ErrorKind::Geometry(
                            GeometryErrorKind::ExactEmbeddingUnrepresentable { .. }
                        )
                    ),
                    "unexpected dual error: {dual}"
                );
                assert!(
                    matches!(
                        legacy.kind(),
                        crate::error::ErrorKind::Geometry(
                            GeometryErrorKind::ExactEmbeddingUnrepresentable { .. }
                        )
                    ),
                    "unexpected legacy error: {legacy}"
                );
            },
            (true, Ok(dual), Ok(legacy)) => {
                let clip =
                    Shape::Polygon(Polygon::new(rectangle(0.0, 0.0, step, step), Vec::new()));
                for diagram in [dual, legacy] {
                    assert_eq!(diagram.polygons.len(), 3, "units={units}");
                    let cells: Vec<_> = diagram
                        .polygons
                        .iter()
                        .map(|output| Shape::Polygon(output.polygon.clone()))
                        .collect();
                    assert!(cells.iter().all(|cell| matches!(cell, Shape::Polygon(polygon) if polygon_is_valid(polygon).unwrap())));
                    assert!(coverage_is_valid(&cells, 0.0).unwrap(), "units={units}");
                    let union = Shape::union_all(&cells, Strictness::Lenient).unwrap();
                    assert!(
                        clip.difference(&union, Strictness::Lenient)
                            .unwrap()
                            .is_empty()
                    );
                    assert!(
                        union
                            .difference(&clip, Strictness::Lenient)
                            .unwrap()
                            .is_empty()
                    );
                }
            },
            (expected, dual, legacy) => panic!(
                "units={units} expected admitted={expected}, dual admitted={}, legacy admitted={}",
                dual.is_ok(),
                legacy.is_ok(),
            ),
        }
    }
}

use super::*;
use crate::geometry::for_each_indexed_overlapping_bounds_pair;
use crate::geometry::segment_index::checked_index_count;

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
        .filter(|vertex| vertex.id == id)
        .expect("receipt vertex IDs are dense")
}

fn receipt_edge(receipt: &VoronoiDiagramReceipt, id: u32) -> &VoronoiEdgeReceipt {
    receipt
        .edges
        .get(id as usize)
        .filter(|edge| edge.id == id)
        .expect("receipt edge IDs are dense")
}

fn receipt_clip(receipt: &VoronoiDiagramReceipt, id: u32) -> &VoronoiClipSegmentReceipt {
    receipt
        .clip_segments
        .get(id as usize)
        .filter(|segment| segment.id == id)
        .expect("receipt clip-segment IDs are dense")
}

fn assert_receipt_graph(receipt: &VoronoiDiagramReceipt) {
    let bounded = |parameter: &exact::ExactRational| {
        assert_ne!(
            parameter.try_cmp(&exact::ExactRational::zero()).unwrap(),
            std::cmp::Ordering::Less
        );
        assert_ne!(
            parameter.try_cmp(&exact::ExactRational::one()).unwrap(),
            std::cmp::Ordering::Greater
        );
    };
    let frame_side = |side| match side {
        VoronoiFrameSide::Bottom
        | VoronoiFrameSide::Right
        | VoronoiFrameSide::Top
        | VoronoiFrameSide::Left => {},
    };
    let mut clip_ring_edges = std::collections::BTreeSet::new();
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
        assert_eq!(site_receipt.exact_xy.len(), 2);
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

    for (id, segment) in receipt.clip_segments.iter().enumerate() {
        assert_eq!(
            segment.id,
            u32::try_from(id).expect("clip segment receipt fits its stored ID")
        );
        assert_eq!(segment.exact_start.len(), 2);
        assert_eq!(segment.exact_end.len(), 2);
        assert!(
            clip_ring_edges.insert((segment.ring, segment.edge)),
            "clip segment IDs retain unique ring-edge identities"
        );
        if let Some(side) = segment.frame_side {
            frame_side(side);
        }
        for root in &segment.source_roots {
            let _root_identity = (root.ring, root.edge, root.reversed);
            assert!(root.start.xy().x.is_finite() && root.start.xy().y.is_finite());
            assert!(root.end.xy().x.is_finite() && root.end.xy().y.is_finite());
            assert_ne!(
                PointKey::new(root.start.xy()),
                PointKey::new(root.end.xy()),
                "source clip roots retain a real directed segment"
            );
        }
        for atom in &segment.atoms {
            let edge = receipt_edge(receipt, atom.edge);
            bounded(&atom.start_parameter);
            bounded(&atom.end_parameter);
            if let Some(side) = segment.frame_side {
                assert_eq!(edge.frame_side, Some(side));
            } else if !segment.source_roots.is_empty() {
                assert!(edge.clip_sources.contains(&segment.id));
            }
            let _reversed = atom.reversed;
        }
    }

    for (id, vertex_receipt) in receipt.vertices.iter().enumerate() {
        assert_eq!(
            vertex_receipt.id,
            u32::try_from(id).expect("vertex receipt fits its stored ID")
        );
        assert_eq!(vertex_receipt.exact_xy.len(), 2);
        for origin in &vertex_receipt.origins {
            match origin {
                VoronoiVertexOrigin::SourceSite { site: id } => {
                    let _ = receipt_site(receipt, *id);
                },
                VoronoiVertexOrigin::OverlayNode { node } => {
                    let _node = node;
                },
                VoronoiVertexOrigin::Circumcenter {
                    component,
                    sites: defining_sites,
                } => {
                    let _component = component;
                    for &id in defining_sites {
                        let _ = receipt_site(receipt, id);
                    }
                },
                VoronoiVertexOrigin::SourceClip {
                    segment,
                    parameter,
                    source_ring,
                    source_edge,
                } => {
                    let clip_segment = receipt_clip(receipt, *segment);
                    bounded(parameter);
                    if let (Some(ring), Some(source_edge)) = (source_ring, source_edge) {
                        assert!(
                            clip_segment
                                .source_roots
                                .iter()
                                .any(|root| { root.ring == *ring && root.edge == *source_edge })
                        );
                    }
                },
                VoronoiVertexOrigin::Bisector { sites } => {
                    for &id in sites {
                        let _ = receipt_site(receipt, id);
                    }
                },
                VoronoiVertexOrigin::Frame { side } => frame_side(*side),
                VoronoiVertexOrigin::Pole { latitude } => {
                    assert!(latitude.is_finite());
                },
                VoronoiVertexOrigin::ChartCut { side } => match side {
                    VoronoiChartCutSide::Lower | VoronoiChartCutSide::Upper => {},
                },
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
            let _ = receipt_clip(receipt, segment_id);
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
                    let _ = receipt_clip(receipt, *segment);
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
        assert_eq!(separator.vertices.len(), separator.edges.len() + 1);
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
                .get(index + 1)
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
                let actual_end = *points
                    .get((index + 1) % open_count)
                    .expect("ring edge end exists");
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
        assert_eq!(end - start, separator.vertices.len());
        for (index, &vertex_id) in separator.vertices.iter().enumerate() {
            let assigned = receipt_vertex(receipt, vertex_id)
                .assignment
                .expect("published separator vertices have assignments");
            assert_eq!(
                PointKey::new(coordinates.point_at(start + index).xy()),
                PointKey::new(assigned)
            );
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
    let (start, end, point) = if start_x != end_x {
        (start_x, end_x, point_x)
    } else {
        (start_y, end_y, point_y)
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
fn box_index_keeps_ordinal_and_bounded_count_limits_distinct() {
    let full_range = (u32::MAX as usize).wrapping_add(1);
    assert_eq!(
        admit_box_ordinals(full_range).map_err(|error| error.to_string()),
        Ok(())
    );

    assert_eq!(
        checked_index_count(u32::MAX as usize).map_err(|error| error.to_string()),
        Ok(u32::MAX)
    );
    assert_eq!(
        checked_index_count(full_range).map_err(|error| error.to_string()),
        Err("resource size arithmetic overflowed".to_owned())
    );

    let too_many = full_range.wrapping_add(1);
    assert_eq!(
        admit_box_ordinals(too_many).map_err(|error| error.to_string()),
        Err("resource size arithmetic overflowed".to_owned())
    );
}

#[test]
fn rounded_fan_order_matches_exact_stored_points() {
    let points = [
        XY::new(-1.0, -0.0),
        XY::new(1.0, -0.0),
        XY::new(0.0, 1.0),
        XY::new(-1.0, 0.0),
        XY::new(0.0, -1.0),
    ];
    for origin in points {
        for left in points {
            for right in points {
                let actual = rounded_fan_angle_cmp(origin, left, right);
                let exact = exact::angle_ccw_cmp(
                    &exact::ExactPoint::from_xy(origin),
                    &exact::ExactPoint::from_xy(left),
                    &exact::ExactPoint::from_xy(right),
                );
                assert_eq!(actual.is_none(), exact.is_err());
                if let (Some(actual), Ok(exact)) = (actual, exact) {
                    assert_eq!(actual, exact);
                }
            }
        }
    }
}

fn cycle(points: &[(f64, f64)]) -> Vec<exact::ExactPoint> {
    points
        .iter()
        .map(|&(x, y)| exact::ExactPoint::from_xy(XY::new(x, y)))
        .collect()
}

fn cycle_xy(points: &[(f64, f64)]) -> Vec<XY> {
    points.iter().map(|&(x, y)| XY::new(x, y)).collect()
}

#[expect(
    clippy::arithmetic_side_effects,
    reason = "ExactSimpleRing carries the repeated closing vertex admitted by simple_ring"
)]
fn exact_ring_points(
    subdivision: &ExactSubdivision,
    ring: &assembly::ExactSimpleRing,
) -> Vec<exact::ExactPoint> {
    ring.vertices[..ring.vertices.len() - 1]
        .iter()
        .map(|&vertex| subdivision.vertex(vertex).point.clone())
        .collect()
}

type RingCoordinateKey = Vec<(u64, u64)>;
type PolygonRingKey = (RingCoordinateKey, Vec<RingCoordinateKey>);

fn exact_ring_coordinate_key(
    subdivision: &ExactSubdivision,
    ring: &assembly::ExactSimpleRing,
) -> RingCoordinateKey {
    let (_, open_vertices) = ring
        .vertices
        .split_last()
        .expect("simple exact rings are closed");
    let mut coordinates: Vec<_> = open_vertices
        .iter()
        .map(|&vertex| {
            let point = subdivision
                .vertex(vertex)
                .point
                .round_nearest_even()
                .unwrap();
            (point.x.to_bits(), point.y.to_bits())
        })
        .collect();
    coordinates.sort_unstable();
    coordinates
}

fn assert_owner_parts_match_exact_point_oracle(
    subdivision: &ExactSubdivision,
    parts: &[ExactCellPart],
) {
    for part in parts {
        let shell = exact_ring_points(subdivision, &part.shell);
        let holes: Vec<_> = part
            .holes
            .iter()
            .map(|hole| exact_ring_points(subdivision, hole))
            .collect();
        assert_holes_inside_exact_shell(&shell, &holes);
        let mut remaining = holes.as_slice();
        while let Some((left, later)) = remaining.split_first() {
            for right in later {
                assert!(
                    right.iter().all(|point| {
                        exact::point_in_cycle(left, point).unwrap() != exact::PointInCycle::Inside
                    }),
                    "one emitted hole is nested inside another"
                );
                assert!(
                    left.iter().all(|point| {
                        exact::point_in_cycle(right, point).unwrap() != exact::PointInCycle::Inside
                    }),
                    "one emitted hole is nested inside another"
                );
            }
            remaining = later;
        }
    }
}

fn assert_holes_inside_exact_shell(shell: &[exact::ExactPoint], holes: &[Vec<exact::ExactPoint>]) {
    for hole in holes {
        let mut saw_inside = false;
        for point in hole {
            match exact::point_in_cycle(shell, point).unwrap() {
                exact::PointInCycle::Outside => {
                    panic!("owner-local shell does not contain its emitted hole")
                },
                exact::PointInCycle::Boundary => {},
                exact::PointInCycle::Inside => saw_inside = true,
            }
        }
        assert!(saw_inside, "a boundary-only ring is not a hole");
    }
}

fn rounded_clip_segment_certificate(
    segment: Segment,
    exact_rings: &[Vec<exact::ExactPoint>],
    rounded_rings: &[Vec<XY>],
    events: &mut Vec<exact::ExactPoint>,
    parity: &mut ClipRingParity,
) -> Result<bool> {
    let mut budget = EvaluationBudget::new("rounded clip segment test", 0)?;
    let mut clip_edges = Vec::new();
    for (ring_index, (exact_ring, rounded_ring)) in
        exact_rings.iter().zip(rounded_rings).enumerate()
    {
        for ((start, end), (rounded_start, rounded_end)) in exact_ring
            .iter()
            .zip(exact_ring.iter().cycle().skip(1))
            .zip(rounded_ring.iter().zip(rounded_ring.iter().cycle().skip(1)))
        {
            clip_edges.push(ClipCertificateEdge {
                ring: ring_index,
                atom: EdgeId(
                    u32::try_from(clip_edges.len())
                        .map_err(|_| crate::error::ResourceError::SizeOverflow)?,
                ),
                start: start.clone(),
                end: end.clone(),
                rounded: [*rounded_start, *rounded_end],
            });
        }
    }
    let polygon = Polygon::new(
        Ring::closed(
            rounded_rings[0]
                .iter()
                .map(|point| Point::new_raw_xy(point.x, point.y))
                .collect(),
        )?,
        rounded_rings[1..]
            .iter()
            .map(|ring| {
                Ring::closed(
                    ring.iter()
                        .map(|point| Point::new_raw_xy(point.x, point.y))
                        .collect(),
                )
            })
            .collect::<Result<Vec<_>>>()?,
    );
    let point_index = PolygonPointIndex::build(&polygon)?;
    let edge_ids: Vec<_> = (0..clip_edges.len()).collect();
    let y_index = YStabbingIndex::build(edge_ids, |&edge| {
        let [start, end] = clip_edges[edge].rounded;
        (
            start.x.min(end.x),
            start.x.max(end.x),
            start.y.min(end.y),
            start.y.max(end.y),
        )
    })?;
    let candidates: Vec<_> = (0..clip_edges.len()).collect();
    rounded_segment_covered_by_clip(
        segment,
        &clip_edges,
        &candidates,
        &point_index,
        &y_index,
        events,
        parity,
        &mut budget,
    )
    .map(|failure| failure.is_none())
}

#[test]
fn rounded_segment_certificate_matches_closed_clip_membership() {
    let rectangle_points = [(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0)];
    let hole_points = [(1.0, 1.0), (3.0, 1.0), (3.0, 3.0), (1.0, 3.0)];
    let rectangle = cycle(&rectangle_points);
    let rectangle_xy = cycle_xy(&rectangle_points);
    let hole = cycle(&hole_points);
    let hole_xy = cycle_xy(&hole_points);
    let mut events = Vec::new();
    let mut parity = ClipRingParity::new(2);
    assert!(
        rounded_clip_segment_certificate(
            Segment {
                start: XY::new(0.0, 0.0),
                end: XY::new(4.0, 0.0),
            },
            std::slice::from_ref(&rectangle),
            std::slice::from_ref(&rectangle_xy),
            &mut events,
            &mut parity,
        )
        .unwrap()
    );
    assert!(
        rounded_clip_segment_certificate(
            Segment {
                start: XY::new(0.5, 0.5),
                end: XY::new(3.5, 3.5),
            },
            std::slice::from_ref(&rectangle),
            std::slice::from_ref(&rectangle_xy),
            &mut events,
            &mut parity,
        )
        .unwrap()
    );
    assert!(
        !rounded_clip_segment_certificate(
            Segment {
                start: XY::new(0.5, 2.0),
                end: XY::new(3.5, 2.0),
            },
            &[rectangle.clone(), hole.clone()],
            &[rectangle_xy.clone(), hole_xy.clone()],
            &mut events,
            &mut parity,
        )
        .unwrap()
    );
    assert!(
        rounded_clip_segment_certificate(
            Segment {
                start: XY::new(1.0, 1.0),
                end: XY::new(3.0, 1.0),
            },
            &[rectangle.clone(), hole],
            &[rectangle_xy.clone(), hole_xy],
            &mut events,
            &mut parity,
        )
        .unwrap()
    );
    let concave_points = [
        (0.0, 0.0),
        (4.0, 0.0),
        (4.0, 4.0),
        (3.0, 4.0),
        (3.0, 1.0),
        (1.0, 1.0),
        (1.0, 4.0),
        (0.0, 4.0),
    ];
    let concave = cycle(&concave_points);
    let concave_xy = cycle_xy(&concave_points);
    assert!(
        !rounded_clip_segment_certificate(
            Segment {
                start: XY::new(0.5, 3.5),
                end: XY::new(3.5, 3.5),
            },
            &[concave],
            &[concave_xy],
            &mut events,
            &mut parity,
        )
        .unwrap()
    );
    let l_shape_points = [
        (0.0, 0.0),
        (4.0, 0.0),
        (4.0, 1.0),
        (1.0, 1.0),
        (1.0, 4.0),
        (0.0, 4.0),
    ];
    let l_shape = cycle(&l_shape_points);
    let l_shape_xy = cycle_xy(&l_shape_points);
    assert!(
        !rounded_clip_segment_certificate(
            Segment {
                start: XY::new(4.0, 1.0),
                end: XY::new(1.0, 4.0),
            },
            &[l_shape],
            &[l_shape_xy],
            &mut events,
            &mut parity,
        )
        .unwrap()
    );
    let slanted_edge_points = [
        (2.949_570_888_116_326e-6, 1.769_742_532_869_795_6e-6),
        (3.097_351_995_393_694_8e-6, 1.858_411_197_236_216_9e-6),
        (3.0e-6, 2.5e-6),
    ];
    let slanted_edge = cycle(&slanted_edge_points);
    let slanted_edge_xy = cycle_xy(&slanted_edge_points);
    let slanted_segment = Segment {
        start: XY::new(2.949_570_888_116_328e-6, 1.769_742_532_869_796_8e-6),
        end: XY::new(3.097_351_995_393_692_7e-6, 1.858_411_197_236_215_6e-6),
    };
    let slanted_midpoint = XY::new(
        f64::midpoint(slanted_segment.start.x, slanted_segment.end.x),
        f64::midpoint(slanted_segment.start.y, slanted_segment.end.y),
    );
    assert_eq!(
        exact::orient2d(
            slanted_segment.start,
            slanted_segment.end,
            slanted_edge_xy[0],
        ),
        ExactSign::Zero
    );
    assert_eq!(
        exact::orient2d(
            slanted_segment.start,
            slanted_segment.end,
            slanted_edge_xy[1],
        ),
        ExactSign::Zero
    );
    assert_eq!(
        exact::orient2d(
            slanted_edge_xy[0],
            slanted_edge_xy[1],
            slanted_segment.start,
        ),
        ExactSign::Zero
    );
    assert_eq!(
        exact::orient2d(slanted_edge_xy[0], slanted_edge_xy[1], slanted_segment.end,),
        ExactSign::Zero
    );
    assert!(slanted_edge_xy[0].x < slanted_segment.start.x);
    assert!(slanted_segment.end.x < slanted_edge_xy[1].x);
    assert_ne!(
        exact::orient2d(slanted_segment.start, slanted_segment.end, slanted_midpoint,),
        ExactSign::Zero
    );
    let slanted_result = rounded_clip_segment_certificate(
        slanted_segment,
        &[slanted_edge],
        &[slanted_edge_xy],
        &mut events,
        &mut parity,
    )
    .unwrap();
    assert!(slanted_result);
    assert!(
        !rounded_clip_segment_certificate(
            Segment {
                start: XY::new(-1.0, 2.0),
                end: XY::new(2.0, 2.0),
            },
            &[rectangle],
            &[rectangle_xy],
            &mut events,
            &mut parity,
        )
        .unwrap()
    );
}

fn validates(cycles: &[Vec<exact::ExactPoint>]) -> bool {
    let refs: Vec<_> = cycles.iter().map(Vec::as_slice).collect();
    validate_exact_cycles(&refs, 0).is_ok()
}

fn noded_fixture(
    cycles: &[Vec<exact::ExactPoint>],
) -> Result<(ExactSubdivision, Vec<ExactSimpleRing>)> {
    let mut vertices: Vec<ExactVertex> = Vec::new();
    let mut edges = Vec::new();
    let mut rings = Vec::with_capacity(cycles.len());
    for (ring_index, cycle) in cycles.iter().enumerate() {
        let vertex_capacity = cycle
            .len()
            .checked_add(1)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        let mut ring_vertices = Vec::with_capacity(vertex_capacity);
        for point in cycle {
            let mut vertex = None;
            for (index, known) in vertices.iter().enumerate() {
                if known.point.same_position(point)? {
                    vertex = Some(VertexId(
                        u32::try_from(index)
                            .map_err(|_| crate::error::ResourceError::SizeOverflow)?,
                    ));
                    break;
                }
            }
            let vertex = if let Some(vertex) = vertex {
                vertex
            } else {
                let vertex = VertexId(
                    u32::try_from(vertices.len())
                        .map_err(|_| crate::error::ResourceError::SizeOverflow)?,
                );
                vertices.push(ExactVertex {
                    point: point.clone(),
                    fixed_xy: None,
                });
                vertex
            };
            ring_vertices.push(vertex);
        }
        let closing = *ring_vertices.first().ok_or_else(exact_cycle_error)?;
        ring_vertices.push(closing);
        let mut ring_edges = Vec::with_capacity(cycle.len());
        for pair in ring_vertices.windows(2) {
            let [start, end] = pair else {
                return Err(exact_cycle_error());
            };
            let edge = EdgeId(
                u32::try_from(edges.len())
                    .map_err(|_| crate::error::ResourceError::SizeOverflow)?,
            );
            let endpoints = if start < end {
                [*start, *end]
            } else {
                [*end, *start]
            };
            edges.push(AtomicEdge {
                endpoints,
                forward: Transition {
                    frame_delta: 0,
                    clip_delta: 0,
                    owner: OwnerTransition::Preserve,
                },
                separator: None,
                clip_sources: Vec::new(),
                frame_side: None,
            });
            ring_edges.push(edge);
        }
        rings.push(ExactSimpleRing {
            face: BoundaryFaceId(
                u32::try_from(ring_index).map_err(|_| crate::error::ResourceError::SizeOverflow)?,
            ),
            vertices: ring_vertices,
            edges: ring_edges,
            orientation: ExactSign::Positive,
        });
    }
    Ok((
        ExactSubdivision {
            vertices,
            edges,
            topology: IndexedTopology::default(),
            edge_of_slot: Vec::new(),
            labels: Vec::new(),
        },
        rings,
    ))
}

#[test]
fn exact_cycle_oracle_accepts_a_finite_max_triangle() {
    let below = f64::MAX.next_down();
    assert!(validates(&[cycle(&[
        (below, below),
        (f64::MAX, below),
        (f64::MAX, f64::MAX)
    ])]));
}

#[test]
fn exact_rectangle_contact_validator_rejects_crossings_and_overlaps() {
    let rectangle = cycle(&[(0.0, 0.0), (4.0, 0.0), (4.0, 3.0), (0.0, 3.0)]);
    validate_exact_rectangle(rectangle.as_slice().try_into().unwrap()).unwrap();

    let crossed = cycle(&[(0.0, 0.0), (4.0, 3.0), (4.0, 0.0), (0.0, 3.0)]);
    assert!(validate_exact_rectangle(crossed.as_slice().try_into().unwrap()).is_err());

    let overlapping = cycle(&[(0.0, 0.0), (4.0, 0.0), (2.0, 0.0), (0.0, 3.0)]);
    assert!(validate_exact_rectangle(overlapping.as_slice().try_into().unwrap()).is_err());
}

#[test]
fn assembled_and_generic_validators_preserve_a_one_ulp_boundary_gap() {
    let inner_right = 1.0_f64.next_down();
    assert_eq!(inner_right.next_up(), 1.0);
    let cycles = [
        cycle(&[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0)]),
        cycle(&[
            (0.5, 0.25),
            (inner_right, 0.25),
            (inner_right, 0.75),
            (0.5, 0.75),
        ]),
    ];
    let (subdivision, rings) = noded_fixture(&cycles).unwrap();
    let noded_cycles: Vec<_> = cycles
        .iter()
        .zip(&rings)
        .map(|(points, ring)| NodedExactCycle { points, ring })
        .collect();
    let point_cycles: Vec<_> = cycles.iter().map(Vec::as_slice).collect();

    validate_assembled_exact_cycles(&subdivision, &noded_cycles, 0, || {
        validate_exact_cycle_nesting(&point_cycles, 0)
    })
    .unwrap();
    let generic = validate_exact_cycles(&point_cycles, 0).unwrap();
    assert!(generic.contacts.is_empty());

    let index = NodedSegmentIndex::new(&noded_cycles).unwrap();
    let mut endpoint_pairs = 0_usize;
    for_each_assembled_pair(&point_cycles, &index, |_, contact| {
        endpoint_pairs = endpoint_pairs.checked_add(1).unwrap();
        assert!(contact.is_none());
        Ok(())
    })
    .unwrap();
    assert_eq!(endpoint_pairs, 8);
    assert_eq!(
        for_each_touch_fan(&index, |_, _, _, _, _| Ok(())).unwrap(),
        0
    );
}

#[test]
fn arbitrary_exact_cycle_oracle_keeps_crossing_t_and_overlap_rejections() {
    let make = |points: &[(f64, f64)]| cycle(points);
    let square = make(&[(0.0, 0.0), (8.0, 0.0), (8.0, 8.0), (0.0, 8.0)]);
    let crossing_diamond = make(&[(4.0, -2.0), (10.0, 4.0), (4.0, 10.0), (-2.0, 4.0)]);
    assert!(!validates(&[square.clone(), crossing_diamond]));

    let bowtie = make(&[(0.0, 0.0), (4.0, 4.0), (0.0, 4.0), (4.0, 0.0)]);
    assert!(!validates(&[bowtie]));

    let one_t = make(&[(0.0, 4.0), (2.0, 5.0), (2.0, 3.0)]);
    assert!(validates(&[square.clone(), one_t.clone()]));
    let noded_square = make(&[(0.0, 0.0), (8.0, 0.0), (8.0, 8.0), (0.0, 8.0), (0.0, 4.0)]);
    assert!(validates(&[noded_square, one_t]));

    let two_t = make(&[(0.0, 2.0), (2.0, 4.0), (0.0, 6.0)]);
    assert!(!validates(&[square, two_t]));

    let overlap_shell = make(&[(-2.0, -2.0), (2.0, -2.0), (2.0, 2.0), (-2.0, 2.0)]);
    let unshared_overlap = make(&[(-1.0, -2.0), (1.0, -2.0), (1.0, -3.0), (-1.0, -3.0)]);
    assert!(!validates(&[overlap_shell, unshared_overlap]));

    let shared_overlap_shell = make(&[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0)]);
    let shared_overlap = make(&[(0.0, 0.0), (2.0, 0.0), (2.0, -2.0), (0.0, -2.0)]);
    assert!(!validates(&[shared_overlap_shell, shared_overlap]));

    let adjacent_overlap = make(&[(0.0, 0.0), (4.0, 0.0), (2.0, 0.0), (2.0, 2.0), (0.0, 2.0)]);
    assert!(!validates(&[adjacent_overlap]));
}

#[test]
fn owner_ray_sort_and_sweep_use_exact_order_when_a_hint_is_unavailable() {
    let zero = exact::ExactPoint::from_xy(XY::new(0.0, 0.0));
    let shallow = exact::ExactPoint::from_xy(XY::new(1.0, f64::from_bits(1)));
    let one = exact::ExactPoint::from_xy(XY::new(0.0, 1.0));
    let one_next = exact::ExactPoint::from_xy(XY::new(1.0, 1.0));
    let high = exact::line_intersection(
        &exact::ExactLine::through_points(&zero, &shallow).unwrap(),
        &exact::ExactLine::through_points(&one, &one_next).unwrap(),
    )
    .unwrap();
    assert!(
        high.finite_rounding().unwrap().is_none(),
        "the exact intersection lies above the finite binary64 range"
    );

    let exact_vertices = [
        ExactVertex {
            point: high,
            fixed_xy: None,
        },
        ExactVertex {
            point: exact::ExactPoint::from_xy(XY::new(2.0, 0.0)),
            fixed_xy: None,
        },
        ExactVertex {
            point: exact::ExactPoint::from_xy(XY::new(-2.0, 0.0)),
            fixed_xy: None,
        },
    ];
    let global_vertices = [VertexId(0), VertexId(1), VertexId(2)];
    let points = assembly::OwnerRayPoints::new(&exact_vertices, &global_vertices);
    assert_eq!(
        [0_u32, 1, 2].map(|vertex| RayPoints::rounded(&points, vertex)),
        [XY::new(0.0, 0.0); 3],
        "one unavailable hint makes the whole key table constant"
    );

    let mut order = [0_u32, 1, 2];
    RayPoints::sort_by_axis(
        &points,
        &mut order,
        RayAxis::X,
        |vertex| vertex,
        |_, _| Ok(()),
    )
    .unwrap();
    assert_eq!(order, [2, 1, 0]);
    assert_eq!(
        shoot_left(&points, 1, |_| [0, 2], &[1]).unwrap(),
        [Some(0)],
        "constant hints must leave exact leftward-ray admission active"
    );
}

#[test]
fn owner_ray_sort_breaks_nearest_even_hint_ties_with_exact_coordinates() {
    let lower = exact::ExactPoint::from_xy(XY::new(1.0, 0.0));
    let upper = exact::ExactPoint::from_xy(XY::new(1.0_f64.next_up(), 0.0));
    let midpoint = lower.midpoint(&upper).unwrap();
    assert_ne!(lower.compare_x(&midpoint).unwrap(), Ordering::Equal);
    assert_eq!(
        lower.round_nearest_even().unwrap(),
        midpoint.round_nearest_even().unwrap()
    );

    let vertices = [
        ExactVertex {
            point: midpoint,
            fixed_xy: None,
        },
        ExactVertex {
            point: upper,
            fixed_xy: None,
        },
        ExactVertex {
            point: lower,
            fixed_xy: None,
        },
    ];
    let ids = [VertexId(0), VertexId(1), VertexId(2)];
    let points = assembly::OwnerRayPoints::new(&vertices, &ids);
    let mut order = [0_u32, 1, 2];
    RayPoints::sort_by_axis(
        &points,
        &mut order,
        RayAxis::X,
        |vertex| vertex,
        |_, _| Ok(()),
    )
    .unwrap();
    assert_eq!(order, [2, 0, 1]);
}

#[test]
fn embedded_clip_admission_rejects_crossed_polygon() {
    let ring = Ring::closed(vec![
        Point::new_raw_xy(0.0, 0.0),
        Point::new_raw_xy(2.0, 2.0),
        Point::new_raw_xy(0.0, 2.0),
        Point::new_raw_xy(2.0, 0.0),
        Point::new_raw_xy(0.0, 0.0),
    ])
    .unwrap();
    assert!(admit_embedded_clip(&Polygon::new(ring, Vec::new())).is_err());
}

fn reversed_cycles(cycles: &[Vec<exact::ExactPoint>]) -> Vec<Vec<exact::ExactPoint>> {
    std::iter::once(&cycles[0])
        .chain(cycles[1..].iter().rev())
        .map(|cycle| cycle.iter().cloned().rev().collect())
        .collect()
}

fn assert_exact_contacts_equal(actual: &[ExactCycleContact], expected: &[ExactCycleContact]) {
    assert_eq!(actual.len(), expected.len());
    for (actual, expected) in actual.iter().zip(expected) {
        assert_eq!(actual.segments, expected.segments);
        assert!(actual.point.same_position(&expected.point).unwrap());
    }
}

fn with_part_cycles<T>(
    subdivision: &ExactSubdivision,
    part: &ExactCellPart,
    visit: impl FnOnce(&[&[exact::ExactPoint]], &[NodedExactCycle<'_>]) -> Result<T>,
) -> Result<T> {
    let rings: Vec<_> = std::iter::once(&part.shell)
        .chain(part.holes.iter())
        .collect();
    let mut owned_cycles = Vec::with_capacity(rings.len());
    for ring in &rings {
        let (_, vertices) = ring
            .vertices
            .split_last()
            .expect("assembled rings are closed");
        let mut points = Vec::with_capacity(vertices.len());
        points.extend(
            vertices
                .iter()
                .map(|&vertex| subdivision.vertex(vertex).point.clone()),
        );
        owned_cycles.push(points);
    }
    let point_cycles: Vec<_> = owned_cycles.iter().map(Vec::as_slice).collect();
    let noded_cycles: Vec<_> = owned_cycles
        .iter()
        .zip(&rings)
        .map(|(points, ring)| NodedExactCycle { points, ring })
        .collect();
    visit(&point_cycles, &noded_cycles)
}

#[derive(Clone, Copy, Debug, Default)]
struct AssembledContactWork {
    edges: usize,
    endpoint_pairs: usize,
    exact_contacts: usize,
    touch_fans: usize,
    incidence_records: usize,
    aabb_pairs: usize,
}

fn actual_producer_contact_work(sites: &[Site], clip: &Polygon) -> Result<AssembledContactWork> {
    let subdivision = grafted_clip_subdivision(sites, clip);
    let parts = assemble_cells(&subdivision)?;
    let mut work = AssembledContactWork::default();
    for part in &parts {
        with_part_cycles(&subdivision, part, |cycles, noded_cycles| {
            let index = NodedSegmentIndex::new(noded_cycles)?;
            work.edges = work
                .edges
                .checked_add(index.endpoints.len())
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            work.incidence_records = work
                .incidence_records
                .checked_add(index.incidences.len())
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            work.touch_fans = work
                .touch_fans
                .checked_add(for_each_touch_fan(&index, |_, _, _, _, _| Ok(()))?)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            for_each_assembled_pair(cycles, &index, |_, contact| {
                work.endpoint_pairs = work
                    .endpoint_pairs
                    .checked_add(1)
                    .ok_or(crate::error::ResourceError::SizeOverflow)?;
                if contact.is_some() {
                    work.exact_contacts = work
                        .exact_contacts
                        .checked_add(1)
                        .ok_or(crate::error::ResourceError::SizeOverflow)?;
                }
                Ok(())
            })?;

            let mut boxes = Vec::with_capacity(index.endpoints.len());
            for cycle in cycles {
                for (start, end) in cycle.iter().zip(cycle.iter().cycle().skip(1)) {
                    let endpoints: [&exact::ExactPoint; 2] = (start, end).into();
                    boxes.push(rounded_box(endpoints)?);
                }
            }
            let mut aabb_pairs = Vec::new();
            let _completion = for_each_indexed_overlapping_bounds_pair(&boxes, |left, right| {
                aabb_pairs.push([left, right]);
                std::ops::ControlFlow::<()>::Continue(())
            })?;
            work.aabb_pairs = work
                .aabb_pairs
                .checked_add(aabb_pairs.len())
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            Ok(())
        })?;
    }
    Ok(work)
}

#[test]
fn assembled_touch_fans_match_generic_exact_contacts_in_source_order() {
    let shell = cycle(&[(-10.0, -10.0), (10.0, -10.0), (10.0, 10.0), (-10.0, 10.0)]);
    let holes = [
        cycle(&[(0.0, 0.0), (4.0, -1.0), (4.0, 1.0)]),
        cycle(&[(0.0, 0.0), (1.0, 4.0), (-1.0, 4.0)]),
        cycle(&[(0.0, 0.0), (-4.0, 1.0), (-4.0, -1.0)]),
        cycle(&[(0.0, 0.0), (-1.0, -4.0), (1.0, -4.0)]),
    ];
    let cycles = std::iter::once(shell).chain(holes).collect::<Vec<_>>();
    let (subdivision, rings) = noded_fixture(&cycles).unwrap();
    let noded_cycles: Vec<_> = cycles
        .iter()
        .zip(&rings)
        .map(|(points, ring)| NodedExactCycle { points, ring })
        .collect();
    let point_cycles: Vec<_> = cycles.iter().map(Vec::as_slice).collect();
    validate_assembled_exact_cycles(&subdivision, &noded_cycles, 0, || {
        validate_exact_cycle_nesting(&point_cycles, 0)
    })
    .unwrap();
    let expected = validate_exact_cycles(&point_cycles, 0).unwrap().contacts;
    let index = NodedSegmentIndex::new(&noded_cycles).unwrap();
    let mut actual = Vec::new();
    for_each_assembled_pair(&point_cycles, &index, |segments, contact| {
        if let Some(contact) = contact {
            assert_eq!(contact.segments, segments);
            actual.push(contact);
        }
        Ok(())
    })
    .unwrap();
    assert_eq!(actual.len(), 24);
    assert_exact_contacts_equal(&actual, &expected);
    assert!(
        actual
            .array_windows::<2>()
            .all(|[left, right]| left.segments < right.segments)
    );
}

#[test]
fn actual_voronoi_ring_contacts_match_the_exhaustive_oracle() {
    let holes = (0..8)
        .map(|index| {
            let x = 2.0 + f64::from(index) * 2.0;
            closed_ring(&[(x, 10.0), (x + 0.25, 9.5), (x - 0.25, 9.5)])
        })
        .collect();
    let clip = Polygon::new(rectangle(0.0, 0.0, 20.0, 10.0), holes);
    assert!(polygon_is_valid(&clip).unwrap());
    let sites = sites_xy(&[(5.0, 5.0), (15.0, 5.0), (10.0, 7.0)]);
    let subdivision = grafted_clip_subdivision(&sites, &clip);
    let parts = assemble_cells(&subdivision).unwrap();
    let mut contact_count = 0;

    for part in &parts {
        with_part_cycles(&subdivision, part, |point_cycles, noded_cycles| {
            validate_assembled_exact_cycles(&subdivision, noded_cycles, 0, || {
                validate_exact_cycle_nesting(point_cycles, 0)
            })?;
            let expected = validate_exact_cycles(point_cycles, 0)?.contacts;
            let index = NodedSegmentIndex::new(noded_cycles)?;
            let mut actual = Vec::new();
            for_each_assembled_pair(point_cycles, &index, |segments, contact| {
                if let Some(contact) = contact {
                    assert_eq!(contact.segments, segments);
                    actual.push(contact);
                }
                Ok(())
            })?;
            assert_exact_contacts_equal(&actual, &expected);
            assert!(
                actual
                    .array_windows::<2>()
                    .all(|[left, right]| left.segments < right.segments)
            );
            contact_count += actual.len();
            Ok(())
        })
        .unwrap();
    }
    assert!(contact_count > 0);
}

#[test]
fn actual_producer_pair_work_scales_with_ring_contacts() {
    let tangent_sites = sites_xy(&[(5.0, 5.0), (15.0, 5.0), (10.0, 7.0)]);
    let tangent_clip = |count: usize| {
        let holes = (0..count)
            .map(|index| {
                let ordinal =
                    f64::from(u32::try_from(index).expect("the fixture has fewer than u32 holes"));
                let x = 2.0 + ordinal;
                closed_ring(&[(x, 10.0), (x + 0.25, 9.5), (x - 0.25, 9.5)])
            })
            .collect();
        Polygon::new(rectangle(0.0, 0.0, 20.0, 10.0), holes)
    };
    let tangent_small = actual_producer_contact_work(&tangent_sites, &tangent_clip(8)).unwrap();
    let tangent_large = actual_producer_contact_work(&tangent_sites, &tangent_clip(16)).unwrap();
    for work in [tangent_small, tangent_large] {
        assert_eq!(
            work.incidence_records,
            work.edges.checked_mul(2).unwrap(),
            "{work:?}"
        );
        assert_eq!(
            work.endpoint_pairs,
            work.edges.checked_add(work.exact_contacts).unwrap(),
            "{work:?}"
        );
        assert!(work.exact_contacts > 0);
        assert!(work.touch_fans > 0);
    }
    assert!(tangent_large.endpoint_pairs > tangent_small.endpoint_pairs);

    let thin_sites = sites_xy(&[(16.0, 1.0), (16.0, 1_000_000.0), (1_000_000.0, 1.0)]);
    let thin_clip = |count: usize| {
        let holes = (0..count)
            .map(|index| {
                let ordinal =
                    f64::from(u32::try_from(index).expect("the fixture has fewer than u32 holes"));
                let base = 0.5 + ordinal / 128.0;
                let slope = 1.0 / 32.0 + ordinal / 32_768.0;
                let thickness = 1.0 / 1_024.0;
                closed_ring(&[
                    (1.0, base),
                    (31.0, base + slope * 30.0),
                    (31.0, base + slope * 30.0 + thickness),
                    (1.0, base + thickness),
                ])
            })
            .collect();
        Polygon::new(rectangle(0.0, 0.0, 32.0, 2.0), holes)
    };
    let thin_small = actual_producer_contact_work(&thin_sites, &thin_clip(8)).unwrap();
    let thin_large = actual_producer_contact_work(&thin_sites, &thin_clip(16)).unwrap();
    for work in [thin_small, thin_large] {
        assert_eq!(
            work.incidence_records,
            work.edges.checked_mul(2).unwrap(),
            "{work:?}"
        );
        assert_eq!(work.endpoint_pairs, work.edges, "{work:?}");
        assert_eq!(work.exact_contacts, 0, "{work:?}");
        assert_eq!(work.touch_fans, 0, "{work:?}");
        assert!(work.aabb_pairs > work.endpoint_pairs, "{work:?}");
    }
    assert!(
        thin_large.aabb_pairs > thin_small.aabb_pairs.checked_mul(2).unwrap(),
        "{thin_small:?} -> {thin_large:?}"
    );
    assert!(
        thin_large.endpoint_pairs < thin_small.endpoint_pairs.checked_mul(3).unwrap(),
        "{thin_small:?} -> {thin_large:?}"
    );
}

#[expect(
    clippy::arithmetic_side_effects,
    reason = "the Euler characteristic arithmetic is bounded by the admitted u32 vertex, edge, face, and component counts"
)]
fn assert_boundary_walk_euler(subdivision: &ExactSubdivision) {
    let components = subdivision
        .topology
        .component_of
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    let faces = subdivision
        .topology
        .face_starts
        .array_windows::<2>()
        .count();
    assert_eq!(
        subdivision.vertices.len() as isize - subdivision.edges.len() as isize + faces as isize,
        2 * components as isize
    );
    if !subdivision.labels.is_empty() {
        assert_eq!(subdivision.labels.len(), faces);
    }
}

#[test]
fn exact_cycle_validator_accepts_one_shell_hole_tangency() {
    let shell = cycle(&[(0.0, 0.0), (8.0, 0.0), (8.0, 8.0), (0.0, 8.0)]);
    let hole = cycle(&[(0.0, 4.0), (2.0, 5.0), (2.0, 3.0)]);
    assert!(validates(&[shell, hole]));
}

#[test]
fn exact_cycle_validator_rejects_two_shell_hole_contacts() {
    let shell = cycle(&[(0.0, 0.0), (8.0, 0.0), (8.0, 8.0), (0.0, 8.0)]);
    let hole = cycle(&[(0.0, 2.0), (2.0, 4.0), (0.0, 6.0)]);
    assert!(!validates(&[shell, hole]));
}

#[test]
fn exact_cycle_validator_rejects_crossed_ring() {
    let crossed = cycle(&[(0.0, 0.0), (4.0, 4.0), (0.0, 4.0), (4.0, 0.0)]);
    assert!(!validates(&[crossed]));
}

#[test]
fn exact_touch_graph_covers_all_five_outcomes_and_reversed_discovery() {
    let shell = cycle(&[(0.0, 0.0), (8.0, 0.0), (8.0, 8.0), (0.0, 8.0)]);
    let one_shell_touch = vec![shell.clone(), cycle(&[(0.0, 4.0), (2.0, 5.0), (2.0, 3.0)])];
    let two_shell_touches = vec![shell.clone(), cycle(&[(0.0, 2.0), (2.0, 4.0), (0.0, 6.0)])];
    let two_holes_touch_once = vec![
        shell.clone(),
        cycle(&[(2.0, 2.0), (4.0, 2.0), (4.0, 4.0), (2.0, 4.0)]),
        cycle(&[(4.0, 4.0), (6.0, 4.0), (6.0, 6.0), (4.0, 6.0)]),
    ];
    let four_hole_star = vec![
        cycle(&[(-10.0, -10.0), (10.0, -10.0), (10.0, 10.0), (-10.0, 10.0)]),
        cycle(&[(0.0, 0.0), (4.0, -1.0), (4.0, 1.0)]),
        cycle(&[(0.0, 0.0), (1.0, 4.0), (-1.0, 4.0)]),
        cycle(&[(0.0, 0.0), (-4.0, 1.0), (-4.0, -1.0)]),
        cycle(&[(0.0, 0.0), (-1.0, -4.0), (1.0, -4.0)]),
    ];
    let interleaved = vec![
        shell,
        cycle(&[(2.0, 2.0), (6.0, 2.0), (6.0, 6.0), (2.0, 6.0)]),
        cycle(&[(4.0, 0.0), (8.0, 4.0), (4.0, 8.0), (0.0, 4.0)]),
    ];
    let cases = [
        (one_shell_touch, true),
        (two_shell_touches, false),
        (two_holes_touch_once, true),
        (four_hole_star, true),
        (interleaved, false),
    ];
    for (cycles, expected) in cases {
        let restored = validates(&cycles);
        let reversed = validates(&reversed_cycles(&cycles));
        assert_eq!(restored, expected);
        assert_eq!(reversed, restored);
    }
}

#[test]
fn assembled_contact_validation_precedes_owner_local_containment() {
    let shell = cycle(&[
        (0.0, 0.0),
        (8.0, 0.0),
        (8.0, 8.0),
        (0.0, 8.0),
        (0.0, 6.0),
        (0.0, 2.0),
    ]);
    let twice_touching_hole = cycle(&[(0.0, 2.0), (2.0, 4.0), (0.0, 6.0)]);
    let invalid_cycles = [shell.clone(), twice_touching_hole];
    let (invalid_subdivision, invalid_rings) = noded_fixture(&invalid_cycles).unwrap();
    let invalid_noded: Vec<_> = invalid_cycles
        .iter()
        .zip(&invalid_rings)
        .map(|(points, ring)| NodedExactCycle { points, ring })
        .collect();
    let invalid_touch_facts = [
        OwnerCycleContainment {
            shell_position: OwnerShellPosition::Inside,
            nested_descendant: false,
        },
        OwnerCycleContainment {
            shell_position: OwnerShellPosition::Inside,
            nested_descendant: false,
        },
    ];
    let invalid_nesting_called = std::cell::Cell::new(false);
    let Err(invalid_facts) =
        validate_assembled_exact_cycles(&invalid_subdivision, &invalid_noded, 0, || {
            invalid_nesting_called.set(true);
            validate_owner_cycle_facts(&invalid_touch_facts, invalid_noded.len(), 0)
        })
    else {
        panic!("the exact touch graph admitted two shell contacts")
    };
    assert!(
        !invalid_nesting_called.get(),
        "contact validation must finish before owner-local containment runs"
    );
    assert!(
        invalid_facts
            .to_string()
            .contains("exact Voronoi cycle set is not a valid polygon boundary"),
        "the exact touch graph must reject before owner-local containment facts: {invalid_facts}"
    );

    let contained_hole = cycle(&[(2.0, 2.0), (6.0, 2.0), (6.0, 6.0), (2.0, 6.0)]);
    let valid_cycles = [shell, contained_hole];
    let (valid_subdivision, valid_rings) = noded_fixture(&valid_cycles).unwrap();
    let valid_noded: Vec<_> = valid_cycles
        .iter()
        .zip(&valid_rings)
        .map(|(points, ring)| NodedExactCycle { points, ring })
        .collect();
    let valid_facts = [
        OwnerCycleContainment {
            shell_position: OwnerShellPosition::Inside,
            nested_descendant: false,
        },
        OwnerCycleContainment {
            shell_position: OwnerShellPosition::Inside,
            nested_descendant: false,
        },
    ];
    let valid_nesting_called = std::cell::Cell::new(false);
    validate_assembled_exact_cycles(&valid_subdivision, &valid_noded, 0, || {
        valid_nesting_called.set(true);
        validate_owner_cycle_facts(&valid_facts, valid_noded.len(), 0)
    })
    .unwrap();
    assert!(valid_nesting_called.get());
}

#[test]
fn owner_local_containment_checks_shell_before_source_ordered_nesting() {
    let shell_first = [
        OwnerCycleContainment {
            shell_position: OwnerShellPosition::Inside,
            nested_descendant: false,
        },
        OwnerCycleContainment {
            shell_position: OwnerShellPosition::Outside,
            nested_descendant: true,
        },
        OwnerCycleContainment {
            shell_position: OwnerShellPosition::Inside,
            nested_descendant: true,
        },
    ];
    assert_eq!(
        first_owner_cycle_failure(&shell_first, 0),
        Some((1, OwnerCycleFailure::OutsideShell))
    );

    let source_order = [
        OwnerCycleContainment {
            shell_position: OwnerShellPosition::Inside,
            nested_descendant: false,
        },
        OwnerCycleContainment {
            shell_position: OwnerShellPosition::Inside,
            nested_descendant: false,
        },
        OwnerCycleContainment {
            shell_position: OwnerShellPosition::Inside,
            nested_descendant: true,
        },
        OwnerCycleContainment {
            shell_position: OwnerShellPosition::Outside,
            nested_descendant: false,
        },
    ];
    assert_eq!(
        first_owner_cycle_failure(&source_order, 0),
        Some((2, OwnerCycleFailure::NestedHole))
    );

    let boundary_only = [
        OwnerCycleContainment {
            shell_position: OwnerShellPosition::Inside,
            nested_descendant: false,
        },
        OwnerCycleContainment {
            shell_position: OwnerShellPosition::BoundaryOnly,
            nested_descendant: false,
        },
    ];
    assert_eq!(
        first_owner_cycle_failure(&boundary_only, 0),
        Some((1, OwnerCycleFailure::BoundaryOnly))
    );
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
fn ordinary_five_site_diagram_is_owned_and_admitted() {
    let sites: Vec<_> = [(0.0, 0.0), (0.0, 1.0), (0.5, 0.4), (1.0, 0.0), (1.0, 1.0)]
        .into_iter()
        .enumerate()
        .map(|(id, (x, y))| Site::raw(id, Point::new_raw_xy(x, y), id))
        .collect();
    let mut budget = ExpansionBudget::new("test", "Voronoi topology");
    let diagram = build(
        &sites,
        VoronoiBoundary::Padded,
        &mut budget,
        true,
        &certified_delaunay(&sites).unwrap(),
    )
    .unwrap();
    assert_eq!(diagram.polygons.len(), sites.len());
    assert!(
        diagram
            .polygons
            .iter()
            .all(|output| polygon_is_valid(&output.polygon).unwrap())
    );
}

#[test]
fn cosine_circle_fails_open_to_generic_coverage() {
    let sites: Vec<_> = (0..8)
        .map(|index| {
            let angle = f64::from(index) * std::f64::consts::TAU / 8.0;
            Point::new_raw_xy(10.0 * angle.cos(), 10.0 * angle.sin())
        })
        .enumerate()
        .map(|(id, point)| Site::raw(id, point, id))
        .collect();
    let mut budget = ExpansionBudget::new("test", "cosine circle");
    let diagram = build(
        &sites,
        VoronoiBoundary::Envelope,
        &mut budget,
        true,
        &certified_delaunay(&sites).unwrap(),
    )
    .unwrap();
    assert_eq!(diagram.polygons.len(), sites.len());
    assert!(
        diagram
            .polygons
            .iter()
            .all(|output| polygon_is_valid(&output.polygon).unwrap())
    );
}

fn assert_directed_partition(cells: &[Shape], bounds: [f64; 4]) {
    let mut edges: std::collections::BTreeMap<[PointKey; 2], Vec<[XY; 2]>> =
        std::collections::BTreeMap::new();
    for cell in cells {
        let Shape::Polygon(polygon) = cell else {
            panic!("Voronoi cells are polygons");
        };
        for (start, end) in polygon.shell.iter().zip(polygon.shell.iter().skip(1)) {
            let edge = [start.xy(), end.xy()];
            let keys = edge.map(PointKey::new);
            let key = if keys[0] <= keys[1] {
                keys
            } else {
                [keys[1], keys[0]]
            };
            edges.entry(key).or_default().push(edge);
        }
    }
    let mut sides: [Vec<(f64, f64)>; 4] = [Vec::new(), Vec::new(), Vec::new(), Vec::new()];
    for directions in edges.values() {
        match directions.as_slice() {
            [edge] => {
                let [a, b] = *edge;
                let (side, correctly_directed) = if a.y == bounds[2] && b.y == bounds[2] {
                    (0, a.x < b.x)
                } else if a.x == bounds[1] && b.x == bounds[1] {
                    (1, a.y < b.y)
                } else if a.y == bounds[3] && b.y == bounds[3] {
                    (2, a.x > b.x)
                } else if a.x == bounds[0] && b.x == bounds[0] {
                    (3, a.y > b.y)
                } else {
                    panic!("unmatched edge is not on the clip boundary");
                };
                assert!(correctly_directed, "boundary edge has the wrong direction");
                let (lo, hi) = if side % 2 == 0 {
                    (a.x.min(b.x), a.x.max(b.x))
                } else {
                    (a.y.min(b.y), a.y.max(b.y))
                };
                sides[side].push((lo, hi));
            },
            [first, second] => {
                assert_eq!(
                    PointKey::new(first[0]),
                    PointKey::new(second[1]),
                    "internal edge directions disagree"
                );
                assert_eq!(
                    PointKey::new(first[1]),
                    PointKey::new(second[0]),
                    "internal edge directions disagree"
                );
            },
            _ => panic!("edge has neither one nor two owners"),
        }
    }
    for (side_index, side) in sides.iter_mut().enumerate() {
        side.sort_unstable_by(|left, right| left.0.total_cmp(&right.0));
        let (start, end) = if side_index % 2 == 0 {
            (bounds[0], bounds[1])
        } else {
            (bounds[2], bounds[3])
        };
        let mut cursor = start;
        for &(lo, hi) in side.iter() {
            assert_eq!(lo, cursor, "clip boundary has a gap or overlap");
            cursor = hi;
        }
        assert_eq!(cursor, end, "clip boundary is not tiled");
    }
}

#[test]
fn extreme_cluster_is_a_valid_binary64_partition() {
    let points = [
        (1.552_304_940_926_043_4e127, -2.245_414_791_420_301e127),
        (1.552_304_940_919_857_5e127, -2.245_414_791_417_300_5e127),
        (1.552_304_940_926_441_2e127, -2.245_414_791_413_082e127),
        (1.552_304_940_932_905_3e127, -2.245_414_791_419_367e127),
        (1.552_304_940_935_468e127, -2.245_414_791_425_475_7e127),
        (1.552_304_940_933_517_1e127, -2.245_414_791_411_899e127),
        (1.552_304_940_934_345_6e127, -2.245_414_791_424_966e127),
        (1.552_304_940_920_426_7e127, -2.245_414_791_410_440_7e127),
        (1.552_304_940_920_431e127, -2.245_414_791_420_247_5e127),
        (1.552_304_940_933_332_2e127, -2.245_414_791_413_508e127),
        (1.552_304_940_916_651_8e127, -2.245_414_791_420_999e127),
        (1.552_304_940_931_348_2e127, -2.245_414_791_412_127e127),
        (1.552_304_940_931_112_7e127, -2.245_414_791_410_722_6e127),
        (1.552_304_940_915_999_5e127, -2.245_414_791_419_609_6e127),
        (1.552_304_940_921_551e127, -2.245_414_791_419_905_5e127),
        (1.552_304_940_935_471_1e127, -2.245_414_791_419_521e127),
        (1.552_304_940_921_168_7e127, -2.245_414_791_406_195_6e127),
        (1.552_304_940_932_703_6e127, -2.245_414_791_411_408e127),
        (1.552_304_940_925_026_4e127, -2.245_414_791_418_615e127),
        (1.552_304_940_934_617_9e127, -2.245_414_791_422_664_4e127),
        (1.552_304_940_917_422_5e127, -2.245_414_791_420_993e127),
        (1.552_304_940_928_658_7e127, -2.245_414_791_420_993e127),
        (1.552_304_940_921_511e127, -2.245_414_791_417_344_7e127),
        (1.552_304_940_919_395e127, -2.245_414_791_412_605_6e127),
        (1.552_304_940_921_255e127, -2.245_414_791_417_260_6e127),
        (1.552_304_940_933_857e127, -2.245_414_791_409_97e127),
    ];
    let source = Shape::MultiPoint(
        points
            .into_iter()
            .map(|(x, y)| Point::new_raw_xy(x, y))
            .collect::<Vec<_>>()
            .into(),
    );
    let cells = source
        .voronoi_cells(0.0, VoronoiBoundary::Padded)
        .expect("extreme cluster should be a valid partition");
    assert_eq!(cells.len(), points.len());
    assert!(cells.iter().all(|cell| {
        let Shape::Polygon(polygon) = cell else {
            return false;
        };
        polygon_is_valid(polygon).unwrap()
    }));
    let canonical = shape::canonical_voronoi_sites(
        points
            .into_iter()
            .map(|(x, y)| Point::new_raw_xy(x, y))
            .collect(),
        0.0,
    )
    .unwrap();
    for (site, cell) in canonical.iter().zip(&cells) {
        let Shape::Polygon(polygon) = cell else {
            unreachable!("Voronoi cells are polygons");
        };
        let rounded_ring: Vec<_> = polygon
            .shell
            .iter()
            .take(polygon.shell.len() - 1)
            .map(|point| exact::ExactPoint::from_xy(point.xy()))
            .collect();
        assert_ne!(
            exact::point_in_cycle(&rounded_ring, &exact::ExactPoint::from_xy(site.point.xy()),)
                .unwrap(),
            exact::PointInCycle::Outside,
        );
    }
    let padded = normalize_clip(&canonical, VoronoiBoundary::Padded, false).unwrap();
    let bounds = padded.frame_bounds();
    assert_directed_partition(&cells, bounds);
    assert!(coverage_is_valid(&cells, 0.0).unwrap());
    let clip = Shape::Polygon(Polygon::new(
        rectangle(bounds[0], bounds[2], bounds[1], bounds[3]),
        Vec::new(),
    ));
    let union = Shape::union_all(&cells, Strictness::Lenient).unwrap();
    assert!(
        clip.difference(&union, Strictness::Lenient)
            .unwrap()
            .is_empty()
    );
    assert!(
        union
            .difference(&clip, Strictness::Lenient)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn separated_repair_classes_defer_spanning_ring_orientation_to_global_check() {
    let sites = sites_xy(&[(0.0, -1.0), (0.0, 0.1), (0.0, 1.0), (100.0, 0.0)]);
    let source = Shape::MultiPoint(
        sites
            .iter()
            .map(|site| site.point)
            .collect::<Vec<_>>()
            .into(),
    );
    let clip_polygon = Polygon::new(
        closed_ring(&[
            (-1.0, -2.0),
            (1.0, -2.0),
            (1.0, -0.45),
            (1.0, -0.25),
            (1.0, 0.25),
            (1.0, 0.55),
            (1.0, 2.0),
            (-1.0, 2.0),
            (-1.0, 0.55),
            (-1.0, 0.25),
            (-1.0, -0.25),
            (-1.0, -0.45),
        ]),
        Vec::new(),
    );
    assert!(polygon_is_valid(&clip_polygon).unwrap());

    let mut budget = ExpansionBudget::new("voronoi_edge_lines", "cross-class ring regression");
    let (coordinates, offsets) = source
        .voronoi_edge_column_budgeted(0.0, VoronoiBoundary::Polygon(&clip_polygon), &mut budget)
        .expect("edge embedding should defer the spanning ring to the global certificate");
    assert_eq!(offsets.as_slice(), [0, 2, 4]);
    assert_eq!(coordinates.len(), 4);

    let clip = Shape::Polygon(clip_polygon);
    let mut lower_separator = false;
    let mut upper_separator = false;
    for [start, end] in offsets.as_slice().array_windows::<2>() {
        assert_eq!(*end - *start, 2);
        let points = [
            coordinates.point_at(*start as usize),
            coordinates.point_at(*end as usize - 1),
        ];
        let [first_point, second_point] = points;
        let (first, second) = (first_point.xy(), second_point.xy());
        assert_ne!(first, second, "each emitted chain is nondegenerate");
        let line = Shape::LineString(
            LineSeq::try_new(CoordSeq::try_from_points(&points).unwrap()).unwrap(),
        );
        assert!(
            clip.covers(&line).unwrap(),
            "each emitted chain lies in the clip"
        );

        let (west, east) = if first.x < second.x {
            (first, second)
        } else {
            (second, first)
        };
        assert!(west.x < east.x);
        let (below, above, seen) = if f64::midpoint(west.y, east.y) < 0.0 {
            (
                sites[0].point.xy(),
                sites[1].point.xy(),
                &mut lower_separator,
            )
        } else {
            (
                sites[1].point.xy(),
                sites[2].point.xy(),
                &mut upper_separator,
            )
        };
        assert_eq!(
            exact::orient2d(west, east, below),
            ExactSign::Negative,
            "the lower owner lies below its separator"
        );
        assert_eq!(
            exact::orient2d(west, east, above),
            ExactSign::Positive,
            "the upper owner lies above its separator"
        );
        assert!(!*seen, "each expected site pair has one separator chain");
        *seen = true;
    }
    assert!(lower_separator && upper_separator);
}

#[test]
fn thin_hole_clip_cells_cover_the_assigned_noded_boundary() {
    let sites = sites_xy(&[(0.0, -1.0), (0.0, 0.1), (0.0, 1.0), (100.0, 0.0)]);
    let source = Shape::MultiPoint(
        sites
            .iter()
            .map(|site| site.point)
            .collect::<Vec<_>>()
            .into(),
    );
    let clip = Polygon::new(
        closed_ring(&[
            (-1.0, -2.0),
            (1.0, -2.0),
            (1.0, -0.45),
            (1.0, -0.25),
            (1.0, 0.25),
            (1.0, 0.55),
            (1.0, 2.0),
            (-1.0, 2.0),
            (-1.0, 0.55),
            (-1.0, 0.25),
            (-1.0, -0.25),
            (-1.0, -0.45),
        ]),
        vec![closed_ring(&[
            (-1.0, 0.5),
            (-0.999_999_999_999_999_8, 0.75),
            (-0.5, 0.75),
        ])],
    );
    assert!(polygon_is_valid(&clip).unwrap());

    let mut edge_budget =
        ExpansionBudget::new("voronoi_edge_lines", "thin-hole embedding regression");
    let (coordinates, offsets) = source
        .voronoi_edge_column_budgeted(0.0, VoronoiBoundary::Polygon(&clip), &mut edge_budget)
        .unwrap();
    let mut polygon_budget =
        ExpansionBudget::new("voronoi_cells", "thin-hole embedding regression");
    let cells = source
        .voronoi_cells_budgeted(0.0, VoronoiBoundary::Polygon(&clip), &mut polygon_budget)
        .unwrap();
    assert!(offsets.as_slice().len() > 1);
    assert_eq!(
        offsets.as_slice().last().copied().map(|end| end as usize),
        Some(coordinates.len())
    );
    for [start, end] in offsets.as_slice().array_windows::<2>() {
        let points = (*start as usize..*end as usize)
            .map(|index| coordinates.point_at(index))
            .collect::<Vec<_>>();
        assert!(points.len() >= 2);
        assert_ne!(points.first().unwrap().xy(), points.last().unwrap().xy());
        assert!(
            points
                .array_windows::<2>()
                .all(|&[left, right]| left.xy() != right.xy()),
            "the edge chain has no zero-length segment"
        );
    }
    let assigned_clip = assigned_clip_from_cells(&sites, &clip, &cells);
    assert!(polygon_is_valid(&assigned_clip).unwrap());
    assert_cells_cover_clip(&cells, &assigned_clip);
    assert!(coverage_is_valid(&cells, 0.0).unwrap());
    let mut right_rows = cells.iter();
    for left in &cells {
        right_rows.next();
        for right in right_rows.clone() {
            assert!(left.relate_pattern(right, "F********").unwrap());
        }
    }
    for site in &sites {
        let point = Shape::Point(site.point);
        let owned = cells
            .iter()
            .filter(|cell| cell.covers(&point).unwrap())
            .count();
        let in_clip = Shape::Polygon(assigned_clip.clone())
            .covers(&point)
            .unwrap();
        assert_eq!(owned, usize::from(in_clip));
    }
    assert_separator_chains_covered_by_cells(&coordinates, offsets.as_slice(), &cells);
}

#[test]
fn five_site_binary64_reproduction_is_valid_for_polygons_and_edges() {
    let sites = sites_xy(&[(4.0, 3.0), (4.0, 4.0), (3.0, 2.0), (1.0, 3.0), (6.0, 5.0)]);
    let source = Shape::MultiPoint(
        sites
            .iter()
            .map(|site| site.point)
            .collect::<Vec<_>>()
            .into(),
    );

    let cells = source
        .voronoi_cells(0.0, VoronoiBoundary::Padded)
        .expect("adjacent-float search should repair the public reproduction");
    assert_eq!(cells.len(), sites.len());
    assert!(
        cells.iter().all(
            |cell| matches!(cell, Shape::Polygon(polygon) if polygon_is_valid(polygon).unwrap())
        )
    );
    assert!(coverage_is_valid(&cells, 0.0).unwrap());
    for site in &sites {
        let point = Shape::Point(site.point);
        assert_eq!(
            cells
                .iter()
                .filter(|cell| cell.covers(&point).unwrap())
                .count(),
            1
        );
    }

    let mut budget = ExpansionBudget::new("voronoi_edge_lines", "five-site reproduction");
    let (coordinates, offsets) = source
        .voronoi_edge_column_budgeted(0.0, VoronoiBoundary::Padded, &mut budget)
        .expect("the edge product should certify the same binary64 embedding");
    assert_eq!(
        offsets.as_slice().last().copied().map(|end| end as usize),
        Some(coordinates.len())
    );
    assert!(offsets.as_slice().len() > 1);
}

#[test]
fn near_tangent_separator_uses_the_assigned_clip_boundary() {
    let sites = sites_xy(&[(8.0, 4.0), (12.0, 5.0), (0.0, -100.0)]);
    let source = Shape::MultiPoint(
        sites
            .iter()
            .map(|site| site.point)
            .collect::<Vec<_>>()
            .into(),
    );
    let clip = Polygon::new(
        closed_ring(&[
            (0.0, 0.0),
            (10.0, 0.0),
            (10.0, 10.0),
            (4.0, 14.0),
            (0.0, 10.0),
        ]),
        vec![closed_ring(&[
            (7.0, 12.0),
            (8.5, 10.999_999_999_999_998),
            (7.5, 10.5),
        ])],
    );

    let mut edge_budget = ExpansionBudget::new("voronoi_edge_lines", "near-tangent assigned clip");
    let (edge_coordinates, offsets) = source
        .voronoi_edge_column_budgeted(0.0, VoronoiBoundary::Polygon(&clip), &mut edge_budget)
        .expect("the edge separator lies in the assigned noded clip");
    assert_eq!(offsets.as_slice(), [0, 2, 4]);
    let edge_points: Vec<_> = (0..edge_coordinates.len())
        .map(|index| edge_coordinates.point_at(index).xy())
        .collect();
    let shell_hit = XY::new(8.35, 11.1);
    let hole_hit = XY::new(8.35, 11.099_999_999_999_998);
    assert!(edge_points.contains(&shell_hit));
    assert!(edge_points.contains(&hole_hit));
    let (edge_pairs, trailing_points) = edge_points.as_chunks::<2>();
    assert!(trailing_points.is_empty());
    assert!(edge_pairs.iter().any(|&[start, end]| {
        (start == shell_hit && end == hole_hit) || (start == hole_hit && end == shell_hit)
    }));

    let mut polygon_budget = ExpansionBudget::new("voronoi_cells", "near-tangent assigned clip");
    let cells = source
        .voronoi_cells_budgeted(0.0, VoronoiBoundary::Polygon(&clip), &mut polygon_budget)
        .expect("the same selected embedding produces valid polygon cells");
    assert_eq!(cells.len(), 3);
    assert!(
        cells.iter().all(
            |cell| matches!(cell, Shape::Polygon(polygon) if polygon_is_valid(polygon).unwrap())
        )
    );
    let mut polygon_points = Vec::new();
    for cell in &cells {
        let Shape::Polygon(polygon) = cell else {
            panic!("Voronoi polygon output contains only polygons");
        };
        polygon_points.extend(polygon.shell.iter().map(|point| point.xy()));
        for hole in polygon.holes.iter() {
            polygon_points.extend(hole.iter().map(|point| point.xy()));
        }
    }
    assert!(
        edge_points
            .iter()
            .all(|point| polygon_points.contains(point))
    );
}

#[test]
fn rounded_ring_orientation_mutant_is_rejected_by_the_embedding_certificate() {
    let nearest = [XY::new(0.0, 0.0), XY::new(2.0, 0.0), XY::new(0.0, 2.0)];
    let mut vertices = EmbeddedVertices::new(nearest.len());
    for index in 0..nearest.len() {
        vertices.set(VertexId(index as u32), EmbeddedVertexId(index as u32));
    }
    let subdivision = ExactSubdivision {
        vertices: nearest
            .iter()
            .map(|&point| ExactVertex {
                point: exact::ExactPoint::from_xy(point),
                fixed_xy: None,
            })
            .collect(),
        edges: Vec::new(),
        topology: IndexedTopology::default(),
        edge_of_slot: Vec::new(),
        labels: Vec::new(),
    };
    let ring = ExactSimpleRing {
        face: BoundaryFaceId(0),
        vertices: vec![VertexId(0), VertexId(1), VertexId(2), VertexId(0)],
        edges: Vec::new(),
        orientation: ExactSign::Positive,
    };
    let parts = [ExactCellPart {
        owner: SiteId(0),
        face: ring.face,
        shell: ring,
        holes: Vec::new(),
    }];
    let live_nodes = [VertexId(0), VertexId(1), VertexId(2)];
    let mut rings_by_node = vec![Vec::new(); nearest.len()];
    for rings in &mut rings_by_node {
        rings.push(0);
    }
    let clip = NormalizedClip {
        rings: Vec::new(),
        fixed: Vec::new(),
        chart_fixed: Vec::new(),
        source_roots: Vec::new(),
        source: None,
        geometry: None,
        polygon_baseline: None,
        contacts: Vec::new(),
    };
    let clip_trace = ClipTrace {
        rings: Vec::new(),
        atom_count: 0,
    };
    let embedding = VoronoiEmbedding {
        subdivision: &subdivision,
        clip: &clip,
        clip_trace: &clip_trace,
        sites: &[],
        parts: &parts,
        includes_polygons: true,
        live_nodes: &live_nodes,
        live_edges: &[],
        vertices: &vertices,
        coordinates: &nearest,
        chords: Vec::new(),
        incident_chords: vec![Vec::new(); nearest.len()],
        fans: Vec::new(),
        rings: vec![VoronoiRing {
            part: 0,
            hole: None,
        }],
        rings_by_node,
        host_graphs: Vec::new(),
        separator_chords: Vec::new(),
    };
    let mut budget = EvaluationBudget::new("rounded ring", 0).unwrap();
    assert!(
        embedding
            .collect_conflicts(&nearest, &mut budget)
            .unwrap()
            .is_empty()
    );

    let inverted = [nearest[0], nearest[1], XY::new(0.0, -2.0)];
    let conflicts = embedding.collect_conflicts(&inverted, &mut budget).unwrap();
    assert!(matches!(conflicts.as_slice(), [VoronoiConflict::Ring(0)]));

    let degenerate = [nearest[0], nearest[1], XY::new(1.0, 0.0)];
    let conflicts = embedding
        .collect_conflicts(&degenerate, &mut budget)
        .unwrap();
    assert!(matches!(conflicts.as_slice(), [VoronoiConflict::Ring(0)]));
}

#[test]
fn assigned_clip_host_conflicts_seed_and_blame_separator_only_class() {
    let shell_points = [
        (0.0, 0.0),
        (10.0, 0.0),
        (10.0, 10.0),
        (0.0, 10.0),
        (0.0, 5.0),
    ];
    let hole_points = [(4.0, 4.0), (6.0, 4.0), (6.0, 6.0), (4.0, 6.0)];
    let clip_polygon = Polygon::new(closed_ring(&shell_points), vec![closed_ring(&hole_points)]);
    let shell: Vec<_> = shell_points.iter().map(|&(x, y)| XY::new(x, y)).collect();
    let hole: Vec<_> = hole_points.iter().map(|&(x, y)| XY::new(x, y)).collect();
    let clip_coordinates: Vec<_> = shell.iter().chain(&hole).copied().collect();
    let separator_nearest = XY::new(2.0, 5.0);
    let mut coordinate_table = clip_coordinates.clone();
    coordinate_table.push(separator_nearest);
    coordinate_table.sort_unstable_by(|left, right| lex_xy(*left, *right));
    assert!(
        coordinate_table
            .array_windows::<2>()
            .all(|&[left, right]| left != right),
        "the fixture assigns one coordinate to each exact vertex"
    );

    let subdivision_vertices = coordinate_table
        .iter()
        .map(|&point| ExactVertex {
            point: exact::ExactPoint::from_xy(point),
            fixed_xy: clip_coordinates.contains(&point).then_some(point),
        })
        .collect();
    let vertex_id = |point: XY| {
        VertexId(
            u32::try_from(
                coordinate_table
                    .binary_search_by(|candidate| lex_xy(*candidate, point))
                    .unwrap(),
            )
            .unwrap(),
        )
    };
    let anchor = vertex_id(XY::new(0.0, 5.0));
    let separator_only = vertex_id(separator_nearest);

    let clip = NormalizedClip {
        rings: vec![cycle(&shell_points), cycle(&hole_points)],
        chart_fixed: vec![shell.clone(), hole.clone()],
        source_roots: Vec::new(),
        fixed: vec![shell, hole],
        source: Some(&clip_polygon),
        geometry: Some(clip_polygon.clone()),
        polygon_baseline: None,
        contacts: Vec::new(),
    };
    let mut edges = Vec::new();
    let mut source_segment = 0_u32;
    for ring in [&shell_points[..], &hole_points[..]] {
        for index in 0..ring.len() {
            let next = index.wrapping_add(1) % ring.len();
            let start = vertex_id(XY::new(ring[index].0, ring[index].1));
            let end = vertex_id(XY::new(ring[next].0, ring[next].1));
            edges.push(AtomicEdge {
                endpoints: [start, end],
                forward: Transition {
                    frame_delta: 0,
                    clip_delta: 0,
                    owner: OwnerTransition::Preserve,
                },
                separator: None,
                clip_sources: vec![ClipSegmentId(source_segment)],
                frame_side: None,
            });
            source_segment = source_segment.wrapping_add(1);
        }
    }
    let separator_edge = EdgeId(u32::try_from(edges.len()).unwrap());
    edges.push(AtomicEdge {
        endpoints: [anchor, separator_only],
        forward: Transition {
            frame_delta: 0,
            clip_delta: 0,
            owner: OwnerTransition::Boundary {
                left: Some(SiteId(0)),
                right: Some(SiteId(1)),
            },
        },
        separator: Some([SiteId(0), SiteId(1)]),
        clip_sources: Vec::new(),
        frame_side: None,
    });
    let subdivision = ExactSubdivision {
        vertices: subdivision_vertices,
        edges,
        topology: IndexedTopology::default(),
        edge_of_slot: Vec::new(),
        labels: Vec::new(),
    };
    let mut trace_budget = EvaluationBudget::new("assigned clip test", 0).unwrap();
    let clip_trace = ClipTrace::build(&subdivision, &clip, &mut trace_budget).unwrap();
    let coordinates = coordinate_table.clone();
    let live_nodes: Vec<_> = (0..coordinates.len())
        .map(|index| VertexId(u32::try_from(index).unwrap()))
        .collect();
    let mut vertices = EmbeddedVertices::new(coordinates.len());
    for (index, &node) in live_nodes.iter().enumerate() {
        vertices.set(node, EmbeddedVertexId(u32::try_from(index).unwrap()));
    }
    let sites = sites_xy(&[(1.0, 7.0), (1.0, 3.0)]);
    let chord = ProximityChord {
        nodes: [anchor.0, separator_only.0],
        nearest: Segment {
            start: vertices.point(anchor, &coordinates).unwrap(),
            end: vertices.point(separator_only, &coordinates).unwrap(),
        },
        can_move: [false, true],
    };
    let live_edges = [separator_edge];
    let mut incident_chords = vec![Vec::new(); coordinates.len()];
    incident_chords[anchor.0 as usize].push(0);
    incident_chords[separator_only.0 as usize].push(0);
    let embedding = VoronoiEmbedding {
        subdivision: &subdivision,
        clip: &clip,
        clip_trace: &clip_trace,
        sites: &sites,
        parts: &[],
        includes_polygons: false,
        live_nodes: &live_nodes,
        live_edges: &live_edges,
        vertices: &vertices,
        coordinates: &coordinates,
        chords: vec![chord],
        incident_chords,
        fans: Vec::new(),
        rings: Vec::new(),
        rings_by_node: vec![Vec::new(); coordinates.len()],
        host_graphs: Vec::new(),
        separator_chords: vec![0],
    };

    let seeds = [separator_only];
    let mut problem = VoronoiProblem::new(&embedding, &seeds).unwrap();
    assert_eq!(problem.classes, [vec![separator_only]]);
    let variables = [separator_only];
    let accepted_values = [separator_nearest];
    let accepted_assignment = [ClassAssignment {
        variables: &variables,
        values: &accepted_values,
    }];
    let mut budget = EvaluationBudget::new("assigned clip test", 1).unwrap();
    assert!(
        embedding
            .collect_conflicts(&coordinates, &mut budget)
            .unwrap()
            .is_empty(),
        "the accepted separator is hosted by the assigned shell and hole"
    );
    assert!(matches!(
        problem
            .check_global(&accepted_assignment, &mut budget)
            .unwrap(),
        GlobalCheck::Pass(())
    ));

    for (candidate, ring) in [(XY::new(12.0, 5.0), 0), (XY::new(5.0, 5.0), 1)] {
        let values = [candidate];
        let assignment = [ClassAssignment {
            variables: &variables,
            values: &values,
        }];
        let mut assigned = coordinates.clone();
        vertices.set_point(separator_only, &mut assigned, candidate);
        let mut budget = EvaluationBudget::new("assigned clip test", 1).unwrap();
        let conflicts = embedding.collect_conflicts(&assigned, &mut budget).unwrap();
        let [VoronoiConflict::ClipHost(failure)] = conflicts.as_slice() else {
            panic!("outside-shell and inside-hole drawings must have one typed clip-host conflict");
        };
        assert_eq!(failure.chord, 0);
        assert_eq!(failure.boundary.ring, ring);
        assert!(failure.boundary.atom.is_some());
        let seeds = embedding.seed_nodes(&conflicts);
        assert!(seeds.contains(&anchor));
        assert!(seeds.contains(&separator_only));
        for atom in &clip_trace.rings[ring].atoms {
            for vertex in subdivision.edge(atom.edge).endpoints {
                assert!(seeds.contains(&vertex));
            }
        }

        let mut budget = EvaluationBudget::new("assigned clip test", 1).unwrap();
        let GlobalCheck::Reject(blamed) = problem.check_global(&assignment, &mut budget).unwrap()
        else {
            panic!("global certification must reject the assigned-boundary violation");
        };
        assert_eq!(
            blamed,
            [0],
            "the separator-only vertex's class is repairable"
        );
    }
}

#[test]
fn shifted_six_site_diagram_has_a_certified_binary64_partition() {
    let offset = 1e16;
    let sites: Vec<_> = [
        (0.0, 0.0),
        (10.0, 0.0),
        (10.0, 10.0),
        (0.0, 10.0),
        (5.0, 5.0),
        (2.0, 8.0),
    ]
    .into_iter()
    .enumerate()
    .map(|(id, (x, y))| Site::raw(id, Point::new_raw_xy(offset + x, offset + y), id))
    .collect();
    let source = Shape::MultiPoint(
        sites
            .iter()
            .map(|site| site.point)
            .collect::<Vec<_>>()
            .into(),
    );
    let cells = source
        .voronoi_cells(0.0, VoronoiBoundary::Envelope)
        .unwrap();
    let clip = Polygon::new(
        rectangle(offset, offset, offset + 10.0, offset + 10.0),
        Vec::new(),
    );
    assert_eq!(cells.len(), 6);
    assert!(coverage_is_valid(&cells, 0.0).unwrap());
    assert_cells_cover_clip(&cells, &clip);
    for site in sites {
        let point = Shape::Point(site.point);
        assert_eq!(
            cells
                .iter()
                .filter(|cell| cell.covers(&point).unwrap())
                .count(),
            1
        );
    }
}

#[test]
fn pinned_clip_vertices_exhaust_a_constructed_nodes_adjacent_float_domain() {
    let offset = 1e16;
    let sites = sites_xy(&[
        (offset - 4.0, offset),
        (offset + 8.0, offset + 2.0),
        (offset, offset + 10.0),
    ]);
    let origin = XY::new(offset, offset);
    let center = XY::new(offset + 2.0, offset + 2.0);
    let clip = Polygon::new(
        closed_ring(&[
            (offset, offset),
            (offset + 2.0, offset),
            (offset + 4.0, offset),
            (offset + 4.0, offset + 2.0),
            (offset + 4.0, offset + 4.0),
            (offset + 2.0, offset + 4.0),
            (offset, offset + 4.0),
            (offset, offset + 2.0),
            (offset + 2.0, offset + 2.0),
        ]),
        Vec::new(),
    );
    assert!(polygon_is_valid(&clip).unwrap());
    let subdivision = grafted_clip_subdivision(&sites, &clip);
    let [left, right, _] = sites.as_slice() else {
        panic!("the fixture has three sites");
    };
    let bisector =
        exact::ExactLine::perpendicular_bisector(left.point.xy(), right.point.xy()).unwrap();
    let diagonal = exact::ExactLine::through_points(
        &exact::ExactPoint::from_xy(origin),
        &exact::ExactPoint::from_xy(center),
    )
    .unwrap();
    // In relative coordinates the bisector is 6*x + y = 13 and the clip edge
    // is y = x. Their hit (13/7, 13/7) rounds to the pinned middle vertex.
    let hit = exact::line_intersection(&bisector, &diagonal).unwrap();
    let hit_vertex = subdivision.vertex_at(&hit).unwrap().unwrap();
    assert!(subdivision.vertex(hit_vertex).fixed_xy.is_none());
    assert_eq!(hit.round_nearest_even().unwrap(), center);
    assert!(
        !hit.same_position(&exact::ExactPoint::from_xy(center))
            .unwrap()
    );
    for x in [center.x.next_down(), center.x, center.x.next_up()] {
        for y in [center.y.next_down(), center.y, center.y.next_up()] {
            let candidate = XY::new(x, y);
            let pinned = subdivision
                .vertex_at(&exact::ExactPoint::from_xy(candidate))
                .unwrap()
                .unwrap();
            assert_eq!(subdivision.vertex(pinned).fixed_xy, Some(candidate));
        }
    }
    let source = Shape::MultiPoint(
        sites
            .iter()
            .map(|site| site.point)
            .collect::<Vec<_>>()
            .into(),
    );
    let mut edge_budget = ExpansionBudget::new("voronoi_edge_lines", "pinned clip vertices");
    for (operation, result) in [
        (
            "voronoi_cells",
            source
                .voronoi_cells(0.0, VoronoiBoundary::Polygon(&clip))
                .map(|_| ()),
        ),
        (
            "voronoi_edge_lines",
            source
                .voronoi_edge_column_budgeted(
                    0.0,
                    VoronoiBoundary::Polygon(&clip),
                    &mut edge_budget,
                )
                .map(|_| ()),
        ),
    ] {
        let error = result.expect_err("all nine adjacent coordinates are pinned");
        let crate::error::ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingUnrepresentable {
            operation: actual,
            ..
        }) = error.kind()
        else {
            panic!("unexpected error: {error}");
        };
        assert_eq!(*actual, operation);
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

fn walk_points(subdivision: &ExactSubdivision, face: BoundaryFaceId) -> Vec<exact::ExactPoint> {
    let topology = &subdivision.topology;
    let walk = &topology.face_slots[topology.face_starts[face.0 as usize] as usize
        ..topology.face_starts[face.0 as usize + 1] as usize];
    walk.iter()
        .map(|&slot| {
            subdivision.vertices[topology.owners[slot as usize] as usize]
                .point
                .clone()
        })
        .collect()
}

fn first_equal_label_misattaches_a_hole(subdivision: &ExactSubdivision) -> bool {
    let mut positives = Vec::new();
    let mut negatives = Vec::new();
    for (index, label) in subdivision.labels.iter().enumerate() {
        if !selected_cell_label(*label) {
            continue;
        }
        let face = BoundaryFaceId(index as u32);
        match exact::cycle_orientation(&walk_points(subdivision, face)).unwrap() {
            ExactSign::Positive => positives.push((face, *label)),
            ExactSign::Negative => negatives.push((face, *label)),
            ExactSign::Zero => panic!("selected walk has zero orientation"),
        }
    }
    positives.sort_unstable_by_key(|(face, _)| *face);
    negatives.into_iter().any(|(negative, label)| {
        let parent = positives
            .iter()
            .find(|(_, other)| other == &label)
            .expect("negative walk has a same-label positive")
            .0;
        let probe = &walk_points(subdivision, negative)[0];
        exact::point_in_cycle(&walk_points(subdivision, parent), probe).unwrap()
            != exact::PointInCycle::Inside
    })
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

#[test]
fn polygon_clip_with_many_disjoint_holes_is_budgeted_by_output() {
    let holes = (0..1_334)
        .map(|index| {
            let x = 1.0 + f64::from(index % 50) * 0.7;
            let y = -8.0 + f64::from(index / 50) * 0.6;
            Ring::closed(vec![
                Point::new_raw_xy(x, y),
                Point::new_raw_xy(x + 0.2, y),
                Point::new_raw_xy(x + 0.1, y + 0.2),
            ])
            .unwrap()
        })
        .collect();
    let polygon = Polygon::new(rectangle(-10.0, -10.0, 40.0, 10.0), holes);
    assert!(polygon_is_valid(&polygon).unwrap());

    let source = Shape::MultiPoint(
        sites_xy(&[(-1.0, 0.0), (1.0, 0.0)])
            .into_iter()
            .map(|site| site.point)
            .collect::<Vec<_>>()
            .into(),
    );
    let mut budget = ExpansionBudget::new("voronoi_cells", "sites/clip topology");
    let cells = source
        .voronoi_cells_budgeted(0.0, VoronoiBoundary::Polygon(&polygon), &mut budget)
        .expect("the 5,346-position partition is below the generated-output budget");

    assert_eq!(cells.len(), 2);
    assert!(
        cells.iter().all(
            |cell| matches!(cell, Shape::Polygon(polygon) if polygon_is_valid(polygon).unwrap())
        )
    );
    let (hole_count, coordinate_count) = cells.iter().fold((0, 0), |(holes, coords), cell| {
        let Shape::Polygon(polygon) = cell else {
            unreachable!("Voronoi polygon output contains only polygons");
        };
        (
            holes + polygon.holes.len(),
            coords
                + polygon.shell.len()
                + polygon.holes.iter().map(|ring| ring.len()).sum::<usize>(),
        )
    });
    assert_eq!(hole_count, 1_334);
    assert_eq!(coordinate_count, 5_346);
    assert_eq!(budget.used(), 7);
}

#[test]
fn polygon_clip_never_publishes_separator_edges_through_holes() {
    let source = Shape::MultiPoint(
        [
            (7.363_407_761_916_353, 0.910_061_356_872_448_7),
            (6.090_183_077_044_946, -8.212_595_695_268_158),
            (36.882_862_911_401_645, 2.797_623_572_491_808_4),
            (36.301_612_116_132_375, -7.745_261_677_494_59),
            (28.239_545_834_041_202, 0.914_252_377_592_369_2),
            (-7.512_276_371_063_782, 0.431_337_981_564_331_23),
            (18.856_535_571_352_17, 3.728_851_987_116_291_5),
            (7.907_218_354_022_312, 6.144_878_172_929_207),
            (37.461_899_718_154_09, -4.374_205_394_511_301),
            (28.995_254_695_626_983, 8.082_142_040_528_609),
            (34.199_497_713_157_854, -4.726_233_471_603_177),
            (4.480_941_274_147_678, 0.874_366_871_259_853_8),
            (13.083_919_124_933_26, -7.950_635_799_513_469),
            (16.680_131_726_928_522, 8.985_083_973_425_464),
            (12.409_534_275_855_67, 0.875_640_236_445_242_7),
            (12.801_013_459_163_723, 8.594_561_991_431_593),
            (15.491_850_637_223_166, 4.641_750_373_890_741),
            (27.311_848_761_232, 6.604_639_667_276_992),
            (1.396_568_785_930_606_2, 1.939_113_266_429_83),
            (3.841_055_416_132_109, 6.533_768_346_517_97),
            (27.292_924_130_985_24, 4.143_438_302_403_387),
            (35.301_683_216_775_01, -0.571_294_127_869_450_1),
            (12.340_258_256_482_5, -1.424_072_102_708_254_1),
            (38.356_471_151_004_33, -3.732_209_537_823_021),
        ]
        .into_iter()
        .map(|(x, y)| Point::new_raw_xy(x, y))
        .collect::<Vec<_>>()
        .into(),
    );
    let holes = (0..13)
        .map(|index| {
            let x = 1.0 + f64::from(index % 50) * 0.7;
            let y = -8.0 + f64::from(index / 50) * 0.6;
            closed_ring(&[(x, y), (x + 0.2, y), (x + 0.1, y + 0.2)])
        })
        .collect();
    let polygon = Polygon::new(rectangle(-10.0, -10.0, 40.0, 10.0), holes);
    assert!(polygon_is_valid(&polygon).unwrap());

    let mut edge_budget = ExpansionBudget::new("voronoi_edge_lines", "sites/clip topology");
    let (coordinates, offsets) = match source.voronoi_edge_column_budgeted(
        0.0,
        VoronoiBoundary::Polygon(&polygon),
        &mut edge_budget,
    ) {
        Ok(output) => output,
        Err(error) => {
            assert!(
                matches!(
                    error.kind(),
                    crate::error::ErrorKind::Geometry(
                        GeometryErrorKind::ExactEmbeddingUnrepresentable {
                            operation: "voronoi_edge_lines",
                            ..
                        }
                    )
                ),
                "unexpected Voronoi error: {error}"
            );
            return;
        },
    };
    assert!(offsets.as_slice().len() > 1);
    assert_eq!(
        offsets.as_slice().last().copied().map(|end| end as usize),
        Some(coordinates.len())
    );

    let mut polygon_budget = ExpansionBudget::new("voronoi_cells", "sites/clip topology");
    let cells = source
        .voronoi_cells_budgeted(0.0, VoronoiBoundary::Polygon(&polygon), &mut polygon_budget)
        .expect("the same clip publishes polygon cells for the edge coverage check");
    assert_separator_chains_covered_by_cells(&coordinates, offsets.as_slice(), &cells);
}

#[test]
fn sparse_collinear_edges_do_not_charge_clip_scan_work() {
    let mut boundary = Vec::with_capacity(20_000);
    for (start, end, subdivisions) in [
        (XY::new(-2.0, -1.0), XY::new(2.0, -0.5), 9_999_u32),
        (XY::new(2.0, -0.5), XY::new(2.0, 1.0), 1),
        (XY::new(2.0, 1.0), XY::new(-2.0, 0.5), 9_999),
        (XY::new(-2.0, 0.5), XY::new(-2.0, -1.0), 1),
    ] {
        for step in 0..subdivisions {
            let fraction = f64::from(step) / f64::from(subdivisions);
            boundary.push(Point::new_raw_xy(
                start.x + (end.x - start.x) * fraction,
                start.y + (end.y - start.y) * fraction,
            ));
        }
    }
    let polygon = Polygon::new(Ring::closed(boundary).unwrap(), Vec::new());
    assert_eq!(polygon.shell.len() - 1, 20_000);

    let sites = (0_u32..801)
        .map(|index| Point::new_raw_xy(0.0, -0.5 + f64::from(index) / 800.0))
        .collect::<Vec<_>>();
    let source = Shape::MultiPoint(sites.into());
    let mut budget = ExpansionBudget::new("voronoi_edge_lines", "sites/clip topology");
    let (coordinates, offsets) = source
        .voronoi_edge_column_budgeted(0.0, VoronoiBoundary::Polygon(&polygon), &mut budget)
        .expect("800 separator edges fit the output budget");

    assert_eq!(offsets.as_slice().len(), 801);
    assert_eq!(offsets.as_slice().last(), Some(&1_600));
    assert_eq!(coordinates.len(), 1_600);
    assert_eq!(budget.used(), 2_400);
}

fn coincident_frame_fixture() -> (Vec<Site>, Polygon) {
    (
        sites_xy(&[(0.0, -1.0), (0.0, 1.0), (2.0, 0.5)]),
        Polygon::new(rectangle(-1.0, 0.0, 0.5, 2.0), Vec::new()),
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

fn old_frame_halfplane_holds(edge: &AtomicEdge, vertices: &[NodedVertex], sites: &[Site]) -> bool {
    let OwnerTransition::Boundary { left, right } = edge.forward.owner else {
        return true;
    };
    let start = &vertices[edge.endpoints[0].0 as usize].point;
    let end = &vertices[edge.endpoints[1].0 as usize].point;
    left.is_none_or(|site| {
        exact::orient_points(
            start,
            end,
            &exact::ExactPoint::from_xy(sites[site.0 as usize].point.xy()),
        )
        .unwrap()
            == ExactSign::Positive
    }) && right.is_none_or(|site| {
        exact::orient_points(
            start,
            end,
            &exact::ExactPoint::from_xy(sites[site.0 as usize].point.xy()),
        )
        .unwrap()
            == ExactSign::Negative
    })
}

#[test]
fn canonical_separator_emitter_keeps_one_sorted_directed_edge_per_dual() {
    let (sites, polygon) = separator_fixture();
    let clip = normalize_clip(&sites, VoronoiBoundary::Polygon(&polygon), false).unwrap();
    let rect = clip_rectangle(&clip).unwrap();
    let mesh = certified_delaunay(&sites).unwrap();
    let complex = delaunay_complex(&mesh, &sites).unwrap();
    let primal_edges = mesh.primal_edges();
    let separators = emit_clipped_separators(&sites, &primal_edges, &complex, &rect).unwrap();
    let expected = primal_edges
        .iter()
        .filter(|edge| match edge {
            CertifiedPrimalEdge::Interior { faces, .. } => {
                complex.component_of_face(faces[0]) != complex.component_of_face(faces[1])
            },
            CertifiedPrimalEdge::Hull { .. } => true,
        })
        .count();
    assert_eq!(separators.len(), expected);
    for separator in &separators {
        assert!(separator.pair[0] < separator.pair[1]);
        let left = exact::ExactPoint::from_xy(sites[separator.pair[0].0 as usize].point.xy());
        assert_eq!(
            exact::orient_points(&separator.start, &separator.end, &left).unwrap(),
            ExactSign::Positive
        );
    }
}

#[test]
fn hull_center_outside_clip_aabb_keeps_the_enter_exit_chord() {
    let (sites, polygon) = coincident_frame_fixture();
    let clip = normalize_clip(&sites, VoronoiBoundary::Polygon(&polygon), false).unwrap();
    let rect = clip_rectangle(&clip).unwrap();
    let mesh = certified_delaunay(&sites).unwrap();
    let complex = delaunay_complex(&mesh, &sites).unwrap();
    let separators =
        emit_clipped_separators(&sites, &mesh.primal_edges(), &complex, &rect).unwrap();
    let pair = [SiteId(0), SiteId(1)];
    let chord = separators
        .iter()
        .find(|separator| separator.pair == pair)
        .expect("A-B hull ray clips to R");
    let bottom = [
        exact::ExactPoint::from_xy(XY::new(-1.0, 0.0)),
        exact::ExactPoint::from_xy(XY::new(0.5, 0.0)),
    ];
    assert!(
        chord.start.same_position(&bottom[0]).unwrap()
            && chord.end.same_position(&bottom[1]).unwrap()
            || chord.start.same_position(&bottom[1]).unwrap()
                && chord.end.same_position(&bottom[0]).unwrap()
    );
    let center = complex.center(0);
    assert!(!in_rectangle(&rect, center).unwrap());
    let adjacency = certified_site_adjacency(&mesh.primal_edges(), sites.len()).unwrap();
    let scaffold = build_scaffold(&rect, &clip).unwrap();
    let atoms =
        node_scaffold_and_separators(scaffold, separators, &sites, &adjacency, &clip, &rect)
            .unwrap();
    assert!(
        !atoms
            .vertices
            .iter()
            .any(|vertex| vertex.point.same_position(center).unwrap())
    );
}

#[test]
fn coincident_frame_separator_seeds_inside_owner_without_nearest_tie() {
    let (sites, polygon) = coincident_frame_fixture();
    let atoms = node_polygon(&sites, &polygon);
    let coincident: Vec<_> = atoms
        .edges
        .iter()
        .filter(|edge| edge.separator == Some([SiteId(0), SiteId(1)]) && edge.frame_side.is_some())
        .collect();
    assert!(!coincident.is_empty());
    for edge in &coincident {
        let OwnerTransition::Boundary { left, right } = edge.forward.owner else {
            panic!("coincident frame owner is not a boundary");
        };
        assert!(
            matches!(
                (left, right),
                (Some(SiteId(1)), None) | (None, Some(SiteId(1)))
            ),
            "coincident frame owner has wrong sites: {left:?}, {right:?}"
        );
        let midpoint = atoms.vertices[edge.endpoints[0].0 as usize]
            .point
            .midpoint(&atoms.vertices[edge.endpoints[1].0 as usize].point)
            .unwrap();
        nearest_site(&midpoint, &sites).unwrap_err();
    }
    let clip = normalize_clip(&sites, VoronoiBoundary::Polygon(&polygon), false).unwrap();
    let rect = clip_rectangle(&clip).unwrap();
    let mesh = certified_delaunay(&sites).unwrap();
    let adjacency = certified_site_adjacency(&mesh.primal_edges(), sites.len()).unwrap();
    let scaffold = build_scaffold(&rect, &clip).unwrap();
    assert!(
        node_scaffold_and_separators(scaffold, Vec::new(), &sites, &adjacency, &clip, &rect,)
            .is_err()
    );
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
fn direct_dual_edge_budget_counts_only_the_published_line_column() {
    let source = Shape::MultiPoint(
        vec![
            Point::new_raw_xy(0.0, 0.0),
            Point::new_raw_xy(2.0, 0.0),
            Point::new_raw_xy(0.0, 2.0),
            Point::new_raw_xy(2.0, 2.0),
            Point::new_raw_xy(0.8, 1.1),
        ]
        .into(),
    );
    let mut budget = ExpansionBudget::new("voronoi_edge_lines", "sites/clip topology");
    let output = source
        .voronoi_edge_column_in_frame_budgeted(0.0, VoronoiBoundary::Envelope, None, &mut budget)
        .unwrap();
    assert_eq!(output.receipt.producer(), VoronoiProducer::Dual);
    assert_receipt_graph(&output.receipt);
    assert_edge_column_receipt_alignment(&output.coordinates, &output.offsets, &output.receipt);
    let edge_count = output.offsets.as_slice().len() - 1;
    assert!(edge_count > 0);
    assert_eq!(output.offsets.as_slice().first(), Some(&0));
    assert_eq!(
        output
            .offsets
            .as_slice()
            .last()
            .copied()
            .map(|end| end as usize),
        Some(output.coordinates.len())
    );
    assert_eq!(budget.used(), output.coordinates.len() + edge_count);
}

#[test]
fn coincident_frame_separator_emits_one_packed_chain_and_excludes_frame_only() {
    let (sites, polygon) = coincident_frame_fixture();
    let mut budget = ExpansionBudget::new("test", "coincident packed");
    let lines = build_edges(
        &sites,
        VoronoiBoundary::Polygon(&polygon),
        &mut budget,
        true,
        &certified_delaunay(&sites).unwrap(),
    )
    .unwrap()
    .into_line_seqs()
    .unwrap();
    let bottom = [XY::new(-1.0, 0.0), XY::new(0.5, 0.0)];
    let matching = lines
        .iter()
        .filter(|line| {
            (0..line.len().saturating_sub(1)).any(|index| {
                let a = line.point_at(index).xy();
                let b = line.point_at(index + 1).xy();
                a == bottom[0] && b == bottom[1] || a == bottom[1] && b == bottom[0]
            })
        })
        .count();
    assert_eq!(matching, 1);
    let frame_only = lines.iter().any(|line| {
        (0..line.len()).all(|index| {
            let xy = line.point_at(index).xy();
            (xy.x == -1.0 || xy.x == 0.5 || xy.y == 0.0 || xy.y == 2.0)
                && !(xy == bottom[0]
                    || xy == bottom[1]
                    || (xy.y == 0.0 && xy.x > -1.0 && xy.x < 0.5))
        }) && (0..line.len().saturating_sub(1)).all(|index| {
            let a = line.point_at(index).xy();
            let b = line.point_at(index + 1).xy();
            !(a == bottom[0] && b == bottom[1] || a == bottom[1] && b == bottom[0])
        })
    });
    assert!(!frame_only, "frame-only support appeared as a packed chain");
}

#[test]
fn interior_first_separator_order_emits_one_full_chain() {
    let sites = sites_xy(&[(0.0, -1.0), (0.0, 1.0), (2.0, 0.5)]);
    let polygon = Polygon::new(
        closed_ring(&[
            (-1.0, 0.0),
            (-0.5, 0.0),
            (0.0, 0.0),
            (0.5, 0.0),
            (0.5, 2.0),
            (-1.0, 2.0),
        ]),
        Vec::new(),
    );
    let atoms = node_polygon(&sites, &polygon);
    let mut subdivision = build_exact_subdivision(atoms).unwrap();
    let primal_edges = certified_delaunay(&sites).unwrap().primal_edges();
    let adjacency = certified_site_adjacency(&primal_edges, sites.len()).unwrap();
    graft_components(&mut subdivision, &sites, &adjacency).unwrap();
    let assembled = assemble_edges(&subdivision).unwrap();
    let separator = [SiteId(0), SiteId(1)];
    let matching: Vec<_> = assembled
        .iter()
        .filter(|chain| chain.separator == separator)
        .collect();
    assert_eq!(matching.len(), 1);
    assert_eq!(matching[0].edges.len(), 3);
    assert_eq!(matching[0].vertices.len(), 4);
    let known = [
        XY::new(-1.0, 0.0),
        XY::new(-0.5, 0.0),
        XY::new(0.0, 0.0),
        XY::new(0.5, 0.0),
    ];
    let round = |vertex: VertexId| {
        let vertex = &subdivision.vertices[vertex.0 as usize];
        vertex
            .fixed_xy
            .map_or_else(|| vertex.point.round_nearest_even(), Ok)
            .unwrap()
    };
    let assembled_coordinates: Vec<_> = matching[0].vertices.iter().copied().map(round).collect();
    assert_eq!(assembled_coordinates.as_slice(), known.as_slice());
    let mut group: Vec<_> = matching[0]
        .edges
        .iter()
        .map(|&edge| (separator, edge))
        .collect();
    group.rotate_left(1);
    let [left, right] = subdivision.edges[group[0].1.0 as usize].endpoints;
    let mut degree = std::collections::BTreeMap::<VertexId, usize>::new();
    for (_, edge_id) in &group {
        for vertex in subdivision.edges[edge_id.0 as usize].endpoints {
            *degree.entry(vertex).or_default() += 1;
        }
    }
    assert_eq!(degree[&left], 2);
    assert_eq!(degree[&right], 2);
    let mut chains = Vec::new();
    assemble_separator_group(&subdivision, separator, &group, &mut chains).unwrap();
    assert_eq!(chains.len(), 1);
    assert_eq!(chains[0].edges.len(), 3);
    assert_eq!(chains[0].vertices.len(), 4);
    let shuffled_coordinates: Vec<_> = chains[0].vertices.iter().copied().map(round).collect();
    assert_eq!(shuffled_coordinates.as_slice(), known.as_slice());
}

#[test]
fn edges_only_embedding_publishes_certified_chains_with_colliding_nearest_vertices() {
    let offset = 1e16;
    let sites: Vec<_> = [
        (0.0, 0.0),
        (10.0, 0.0),
        (10.0, 10.0),
        (0.0, 10.0),
        (5.0, 5.0),
        (2.0, 8.0),
    ]
    .into_iter()
    .enumerate()
    .map(|(id, (x, y))| Site::raw(id, Point::new_raw_xy(offset + x, offset + y), id))
    .collect();
    let clip = Polygon::new(
        rectangle(offset - 1.0, offset - 1.0, offset + 11.0, offset + 11.0),
        Vec::new(),
    );
    let mut budget = ExpansionBudget::new("test", "edges-only collision");
    let lines = build_edges(
        &sites,
        VoronoiBoundary::Polygon(&clip),
        &mut budget,
        true,
        &certified_delaunay(&sites).unwrap(),
    )
    .expect("the emitted separator chains have a certified binary64 embedding")
    .into_line_seqs()
    .unwrap();
    assert_eq!(lines.len(), 7);
    let clip = Shape::Polygon(clip);
    assert!(
        lines.iter().all(|line| {
            line.len() == 2
                && line.point_at(0).xy() != line.point_at(1).xy()
                && clip.covers(&Shape::LineString(line.clone())).unwrap()
        }),
        "every output chain is a nondegenerate segment contained by the clip"
    );
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

fn assert_separator_chains_covered_by_cells(
    coordinates: &CoordSeq,
    offsets: &[i32],
    cells: &[Shape],
) {
    let cells_union = Shape::union_all(cells, Strictness::Lenient).unwrap();
    for &[start, end] in offsets.array_windows::<2>() {
        let points: Vec<_> = (usize::try_from(start).unwrap()..usize::try_from(end).unwrap())
            .map(|index| coordinates.point_at(index))
            .collect();
        let line = Shape::LineString(
            LineSeq::try_new(CoordSeq::try_from_points(&points).unwrap()).unwrap(),
        );
        assert!(
            cells_union.covers(&line).unwrap(),
            "the published cell union does not cover separator {points:?}"
        );
    }

    let mut polygon_vertices = Vec::new();
    for cell in cells {
        let Shape::Polygon(polygon) = cell else {
            panic!("Voronoi polygon output contains only polygons");
        };
        polygon_vertices.extend(polygon.shell.iter().map(|point| {
            let point = point.xy();
            (point.x.to_bits(), point.y.to_bits())
        }));
        for hole in polygon.holes.iter() {
            polygon_vertices.extend(hole.iter().map(|point| {
                let point = point.xy();
                (point.x.to_bits(), point.y.to_bits())
            }));
        }
    }
    for index in 0..coordinates.len() {
        let point = coordinates.point_at(index).xy();
        assert!(
            polygon_vertices.contains(&(point.x.to_bits(), point.y.to_bits())),
            "edge coordinate {point:?} differs from the polygon product's shared vertex"
        );
    }
}

fn assign_published_ring(
    subdivision: &ExactSubdivision,
    exact: &ExactSimpleRing,
    published: &Ring,
    coordinates: &mut [Option<XY>],
) {
    let (_, vertices) = exact.vertices.split_last().unwrap();
    let points: Vec<_> = published
        .iter()
        .take(vertices.len())
        .map(|point| point.xy())
        .collect();
    assert_eq!(published.len(), exact.vertices.len());
    let (vertex_start, anchor) = vertices
        .iter()
        .enumerate()
        .find_map(|(index, &node)| {
            subdivision
                .vertex(node)
                .fixed_xy
                .map(|point| (index, point))
        })
        .expect("each thin-hole cell ring has a stored clip vertex");
    let point_start = points.iter().position(|&point| point == anchor).unwrap();
    for (&vertex, &point) in vertices
        .iter()
        .cycle()
        .skip(vertex_start)
        .zip(points.iter().cycle().skip(point_start))
        .take(vertices.len())
    {
        let previous = coordinates
            .get_mut(vertex.0 as usize)
            .expect("the exact ring vertex belongs to the subdivision coordinate table")
            .replace(point);
        if let Some(previous) = previous {
            assert_eq!(
                (previous.x.to_bits(), previous.y.to_bits()),
                (point.x.to_bits(), point.y.to_bits())
            );
        }
    }
}

fn assigned_clip_from_cells(sites: &[Site], source: &Polygon, cells: &[Shape]) -> Polygon {
    let subdivision = grafted_clip_subdivision(sites, source);
    let parts = assemble_cells(&subdivision).unwrap();
    assert_eq!(parts.len(), cells.len());
    let mut coordinates = vec![None; subdivision.vertices.len()];
    for part in &parts {
        let mut matches = cells
            .iter()
            .map(|cell| {
                let Shape::Polygon(polygon) = cell else {
                    panic!("Voronoi cells are polygons");
                };
                polygon
            })
            .filter(|polygon| {
                polygon.shell.len() == part.shell.vertices.len()
                    && polygon.holes.len() == part.holes.len()
                    && part
                        .shell
                        .vertices
                        .iter()
                        .filter_map(|&node| subdivision.vertex(node).fixed_xy)
                        .all(|fixed| polygon.shell.iter().any(|point| point.xy() == fixed))
            });
        let polygon = matches
            .next()
            .expect("the exact part's stored anchors identify its published cell");
        assert!(matches.next().is_none());
        for (exact, published) in std::iter::once(&part.shell)
            .chain(&part.holes)
            .zip(std::iter::once(&polygon.shell).chain(polygon.holes.iter()))
        {
            assign_published_ring(&subdivision, exact, published, &mut coordinates);
        }
    }
    for (vertex, assigned) in subdivision.vertices.iter().zip(&coordinates) {
        let Some(assigned) = assigned else { continue };
        if let Some(fixed) = vertex.fixed_xy {
            assert_eq!(
                (assigned.x.to_bits(), assigned.y.to_bits()),
                (fixed.x.to_bits(), fixed.y.to_bits())
            );
        } else {
            let nearest = vertex.point.round_nearest_even().unwrap();
            assert!([nearest.x.next_down(), nearest.x, nearest.x.next_up()].contains(&assigned.x));
            assert!([nearest.y.next_down(), nearest.y, nearest.y.next_up()].contains(&assigned.y));
        }
    }
    let clip = normalize_clip(sites, VoronoiBoundary::Polygon(source), true).unwrap();
    for point in clip.rings.iter().flatten() {
        let node = subdivision.vertex_at(point).unwrap().unwrap();
        assert!(
            coordinates
                .get(node.0 as usize)
                .expect("the source clip vertex belongs to the subdivision coordinate table")
                .is_some()
        );
    }
    let mut budget = EvaluationBudget::new("assigned clip regression", 0).unwrap();
    let trace = ClipTrace::build(&subdivision, &clip, &mut budget).unwrap();
    let oriented = |atom: &ClipTraceAtom| {
        let [start, end] = subdivision.edge(atom.edge).endpoints;
        if atom.reversed {
            [end, start]
        } else {
            [start, end]
        }
    };
    let mut rings = trace.rings.iter().map(|ring| {
        let mut points = Vec::with_capacity(ring.atoms.len());
        for (atom, next) in ring.atoms.iter().zip(ring.atoms.iter().cycle().skip(1)) {
            let [start, end] = oriented(atom);
            let [next_start, _] = oriented(next);
            assert_eq!(end, next_start);
            let point = coordinates
                .get(start.0 as usize)
                .expect("the source atom endpoint belongs to the subdivision coordinate table")
                .expect("the source atom endpoint is published in its incident cell");
            points.push(Point::new_raw_xy(point.x, point.y));
        }
        points.push(*points.first().unwrap());
        Ring::closed(points).unwrap()
    });
    Polygon::new(rings.next().unwrap(), rings.collect())
}

fn assert_cells_cover_clip(cells: &[Shape], clip: &Polygon) {
    assert!(!cells.is_empty());
    assert!(cells.iter().all(|cell| {
        let Shape::Polygon(polygon) = cell else {
            return false;
        };
        polygon_is_valid(polygon).unwrap()
    }));
    let union = Shape::union_all(cells, Strictness::Lenient).unwrap();
    let clip_shape = Shape::Polygon(clip.clone());
    assert!(
        clip_shape
            .difference(&union, Strictness::Lenient)
            .unwrap()
            .is_empty()
    );
    assert!(
        union
            .difference(&clip_shape, Strictness::Lenient)
            .unwrap()
            .is_empty()
    );
    let mut right_rows = cells.iter();
    for left in cells {
        right_rows.next();
        for right in right_rows.clone() {
            assert_eq!(
                left.intersection(right, Strictness::Lenient)
                    .unwrap()
                    .area(),
                0.0
            );
        }
    }
}

fn assert_one_cell_equals_clip(cells: &[Shape], clip: &Polygon) {
    assert_eq!(cells.len(), 1);
    let Shape::Polygon(_) = &cells[0] else {
        panic!("Voronoi cells are polygons");
    };
    let cell = &cells[0];
    let clip_shape = Shape::Polygon(clip.clone());
    assert!(
        clip_shape
            .difference(cell, Strictness::Lenient)
            .unwrap()
            .is_empty()
    );
    assert!(
        cell.difference(&clip_shape, Strictness::Lenient)
            .unwrap()
            .is_empty()
    );
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

#[test]
fn owner_local_hosting_keeps_all_1334_noncollinear_interior_holes() {
    let sites = sites_xy(&[(1000.0, 5.0), (1000.0, 1_000_000.0), (1_000_000.0, 5.0)]);
    let holes = (0_u32..1_334)
        .map(|index| {
            let x = 1.0 + f64::from(index) * 1.4;
            closed_ring(&[(x, 5.0), (x + 0.25, 4.5), (x - 0.25, 4.5)])
        })
        .collect();
    let clip = Polygon::new(rectangle(0.0, 0.0, 2000.0, 10.0), holes);
    let subdivision = grafted_clip_subdivision(&sites, &clip);
    assembly::reset_owner_local_work();
    let parts = assemble_cells(&subdivision).unwrap();
    let work = assembly::owner_local_work();

    assert_eq!(parts.len(), 1);
    assert_eq!(parts[0].holes.len(), 1_334);
    assert_eq!(work.owner_ring_edges, 4_006);
    assert_eq!(work.owner_vertices, 4_006);
    assert_eq!(work.host_queries, 1_335);
    assert_eq!(work.ray_hits, 1_334);
    assert_eq!(work.ring_side_checks, 1_335);
    assert_eq!(work.global_edge_slot_visits, subdivision.edge_of_slot.len());
    let shell_points = exact_ring_points(&subdivision, &parts[0].shell);
    let hole_points: Vec<_> = parts[0]
        .holes
        .iter()
        .map(|hole| exact_ring_points(&subdivision, hole))
        .collect();
    assert_holes_inside_exact_shell(&shell_points, &hole_points);
    let mut emitted_rings = Vec::with_capacity(1 + parts[0].holes.len());
    emitted_rings.push(exact_ring_coordinate_key(&subdivision, &parts[0].shell));
    emitted_rings.extend(
        parts[0]
            .holes
            .iter()
            .map(|hole| exact_ring_coordinate_key(&subdivision, hole)),
    );
    emitted_rings.sort_unstable();
    let mut source_rings = Vec::with_capacity(1 + clip.holes.len());
    source_rings.push(ring_coordinate_key(&clip.shell));
    source_rings.extend(clip.holes.iter().map(ring_coordinate_key));
    source_rings.sort_unstable();
    assert_eq!(
        emitted_rings, source_rings,
        "owner-local assembly preserves every exact clip ring coordinate set"
    );
}

#[test]
fn public_noncollinear_one_owner_keeps_1334_interior_holes_and_clip_support() {
    let source = site_shape(&[(1000.0, 5.0), (1000.0, 1_000_000.0), (1_000_000.0, 5.0)]);
    let holes = (0_u32..1_334)
        .map(|index| {
            let x = 1.0 + f64::from(index) * 1.4;
            closed_ring(&[(x, 5.0), (x + 0.25, 4.5), (x - 0.25, 4.5)])
        })
        .collect();
    let clip = Polygon::new(rectangle(0.0, 0.0, 2000.0, 10.0), holes);
    let mut budget = ExpansionBudget::new("test", "noncollinear one-owner 1334 holes");
    assembly::reset_owner_local_work();
    let cells = source
        .voronoi_cells_budgeted(0.0, VoronoiBoundary::Polygon(&clip), &mut budget)
        .unwrap();
    let work = assembly::owner_local_work();

    assert_eq!(cells.len(), 1);
    let Shape::Polygon(cell) = &cells[0] else {
        panic!("Voronoi polygon output contains only polygons");
    };
    assert_eq!(cell.holes.len(), 1_334);
    assert_eq!(
        polygon_ring_keys(cell),
        polygon_ring_keys(&clip),
        "the sole owner public output preserves the clip's shell and every hole coordinate"
    );
    assert_eq!(work.owner_ring_edges, 4_006);
    assert_eq!(work.host_queries, 1_335);
    assert_eq!(work.ray_hits, 1_334);
    assert_eq!(work.ring_side_checks, 1_335);
}

#[test]
fn task163_noncollinear_public_path_retains_667_tangent_outputs() {
    let source = site_shape(&[(500.0, 5.0), (1500.0, 5.0), (1000.0, 7.0)]);
    let clip = task163_tangent_clip(667);
    let mut budget = ExpansionBudget::new("test", "667 tangent holes");
    assembly::reset_owner_local_work();
    let cells = source
        .voronoi_cells_budgeted(0.0, VoronoiBoundary::Polygon(&clip), &mut budget)
        .unwrap();
    let work = assembly::owner_local_work();
    let mut hole_count = 0;
    for cell in &cells {
        let Shape::Polygon(polygon) = cell else {
            panic!("Voronoi polygon output contains only polygons");
        };
        hole_count += polygon.holes.len();
    }

    eprintln!(
        "667 tangent-hole output: polygons={}, holes={hole_count}, budget={}, \
         ring_edges={}, global_slot_visits={}, owner_vertices={}, face_walks={}, \
         components={}, queries={}, hits={}, side_checks={}",
        cells.len(),
        budget.used(),
        work.owner_ring_edges,
        work.global_edge_slot_visits,
        work.owner_vertices,
        work.face_walks,
        work.components,
        work.host_queries,
        work.ray_hits,
        work.ring_side_checks,
    );
    assert_eq!(cells.len(), 4);
    assert_eq!(hole_count, 666);
    assert_eq!(work.owner_ring_edges, 2_680);
    assert_eq!(work.global_edge_slot_visits, 5_364);
    assert_eq!(work.owner_vertices, 2_013);
    assert_eq!(work.face_walks, 671);
    assert_eq!(work.components, 2);
    assert_eq!(work.host_queries, 2);
    assert_eq!(work.ray_hits, 0);
    assert_eq!(work.ring_side_checks, 669);
}

#[test]
fn task163_noncollinear_1334_public_output_preserves_owner_parts_and_rings() {
    let sites = sites_xy(&[(500.0, 5.0), (1500.0, 5.0), (1000.0, 7.0)]);
    let clip = task163_tangent_clip(1_334);
    let subdivision = grafted_clip_subdivision(&sites, &clip);
    let parts = assemble_cells(&subdivision).unwrap();
    let mut owner_parts_holes = std::collections::BTreeMap::<u32, (usize, usize)>::new();
    for part in &parts {
        let counts = owner_parts_holes.entry(part.owner.0).or_default();
        counts.0 += 1;
        counts.1 += part.holes.len();
    }
    assert_eq!(
        owner_parts_holes,
        std::collections::BTreeMap::from([(0, (1, 535)), (1, (1, 441)), (2, (3, 356)),])
    );

    let mut expected_parts: Vec<_> = parts
        .iter()
        .map(|part| {
            let mut holes: Vec<_> = part
                .holes
                .iter()
                .map(|ring| exact_ring_coordinate_key(&subdivision, ring))
                .collect();
            holes.sort_unstable();
            (exact_ring_coordinate_key(&subdivision, &part.shell), holes)
        })
        .collect();
    expected_parts.sort_unstable();

    let mut budget = ExpansionBudget::new("test", "1334 tangent holes");
    assembly::reset_owner_local_work();
    let cells = site_shape(&[(500.0, 5.0), (1500.0, 5.0), (1000.0, 7.0)])
        .voronoi_cells_budgeted(0.0, VoronoiBoundary::Polygon(&clip), &mut budget)
        .unwrap();
    let work = assembly::owner_local_work();
    let mut output_parts = Vec::with_capacity(cells.len());
    let mut output_holes = 0;
    for cell in &cells {
        let Shape::Polygon(polygon) = cell else {
            panic!("Voronoi polygon output contains only polygons");
        };
        output_holes += polygon.holes.len();
        let (shell, holes) = polygon_ring_keys(polygon);
        output_parts.push((shell, holes));
    }
    output_parts.sort_unstable();

    assert_eq!(cells.len(), 5);
    assert_eq!(output_holes, 1_332);
    assert_eq!(budget.used(), 1_358);
    assert_eq!(output_parts, expected_parts);
    assert_eq!(work.owner_ring_edges, 5_356);
    assert_eq!(work.owner_vertices, 4_022);
    assert_eq!(work.face_walks, 1_340);
    assert_eq!(work.components, 3);
    assert_eq!(work.host_queries, 3);
    assert_eq!(work.ray_hits, 0);
    assert_eq!(work.ring_side_checks, 1_337);
}

#[test]
fn diamond_clip_in_square_aabb_covers_clip_with_no_separator_through_contacts() {
    let diamond = Polygon::new(
        closed_ring(&[(0.0, 1.0), (1.0, 0.0), (2.0, 1.0), (1.0, 2.0)]),
        Vec::new(),
    );
    let sites = [(10.0, 0.0), (12.0, 0.0), (11.0, 1.0)];
    let cells = site_shape(&sites)
        .voronoi_cells(0.0, VoronoiBoundary::Polygon(&diamond))
        .unwrap();
    assert_one_cell_equals_clip(&cells, &diamond);
}

#[test]
fn partial_collinear_frame_overlap_covers_clip() {
    let clip = Polygon::new(
        closed_ring(&[(1.0, 0.0), (3.0, 0.0), (3.0, 2.0), (0.0, 2.0)]),
        Vec::new(),
    );
    let sites = [(2.0, 1.0), (1.2, 1.4), (2.4, 1.5)];
    let cells = site_shape(&sites)
        .voronoi_cells(0.0, VoronoiBoundary::Polygon(&clip))
        .unwrap();
    assert_cells_cover_clip(&cells, &clip);
}

#[test]
fn cyclically_rotated_rectangle_clip_covers_clip() {
    let clip = Polygon::new(
        closed_ring(&[(2.0, 0.0), (2.0, 2.0), (0.0, 2.0), (0.0, 0.0)]),
        Vec::new(),
    );
    let sites = [(0.5, 0.5), (1.5, 0.5), (1.0, 1.5)];
    let cells = site_shape(&sites)
        .voronoi_cells(0.0, VoronoiBoundary::Polygon(&clip))
        .unwrap();
    assert_cells_cover_clip(&cells, &clip);
}

#[test]
fn holed_diamond_clip_covers_shell_minus_hole() {
    let clip = Polygon::new(
        closed_ring(&[(0.0, 1.0), (1.0, 0.0), (2.0, 1.0), (1.0, 2.0)]),
        vec![closed_ring(&[
            (0.75, 0.75),
            (1.25, 0.75),
            (1.25, 1.25),
            (0.75, 1.25),
        ])],
    );
    let sites = [(10.0, 0.0), (12.0, 0.0), (11.0, 1.0)];
    let cells = site_shape(&sites)
        .voronoi_cells(0.0, VoronoiBoundary::Polygon(&clip))
        .unwrap();
    assert_one_cell_equals_clip(&cells, &clip);
    let Shape::Polygon(cell) = &cells[0] else {
        panic!("Voronoi cells are polygons");
    };
    assert_eq!(cell.holes.len(), 1);
}

#[test]
fn public_polygon_admits_sites_on_and_outside_clip_aabb() {
    let sites = sites_xy(&[(0.0, 0.0), (1.0, 0.0), (3.0, 1.0), (1.0, 1.0)]);
    let polygon = Polygon::new(rectangle(0.0, 0.0, 2.0, 2.0), Vec::new());
    let mut budget = ExpansionBudget::new("test", "sites on and outside R");
    let atoms = node_polygon(&sites, &polygon);
    assert!(
        atoms.edges.iter().any(|edge| {
            edge.separator.is_none()
                && edge.frame_side.is_some()
                && !old_frame_halfplane_holds(edge, &atoms.vertices, &sites)
        }),
        "old frame halfplane still holds on every frame-only atom"
    );
    build(
        &sites,
        VoronoiBoundary::Polygon(&polygon),
        &mut budget,
        true,
        &certified_delaunay(&sites).unwrap(),
    )
    .unwrap();
}

#[test]
fn subnormal_clip_aabb_exact_lane_admits_sites_on_corners() {
    let step = f64::from_bits(4);
    let sites = sites_xy(&[(0.0, 0.0), (step, 0.0), (0.0, step)]);
    let mut envelope_budget = ExpansionBudget::new("test", "units4 envelope");
    build(
        &sites,
        VoronoiBoundary::Envelope,
        &mut envelope_budget,
        false,
        &certified_delaunay(&sites).unwrap(),
    )
    .unwrap();
    let polygon = Polygon::new(rectangle(0.0, 0.0, step, step), Vec::new());
    let mut polygon_budget = ExpansionBudget::new("test", "units4 polygon");
    build(
        &sites,
        VoronoiBoundary::Polygon(&polygon),
        &mut polygon_budget,
        true,
        &certified_delaunay(&sites).unwrap(),
    )
    .unwrap();
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
fn canonical_separator_atoms_keep_one_identity_and_do_not_branch() {
    let (sites, polygon) = separator_fixture();
    let atoms = node_polygon(&sites, &polygon);
    let mut degree = std::collections::BTreeMap::<([SiteId; 2], VertexId), usize>::new();
    for edge in atoms.edges {
        if let Some(pair) = edge.separator {
            *degree.entry((pair, edge.endpoints[0])).or_default() += 1;
            *degree.entry((pair, edge.endpoints[1])).or_default() += 1;
        }
    }
    assert!(degree.values().all(|degree| *degree <= 2));
}

#[test]
fn crossed_component_keeps_distinct_faces_with_the_same_owner() {
    let sites: Vec<_> = [(-10.0, -10.0), (0.0, 10.0), (10.0, -10.0)]
        .into_iter()
        .enumerate()
        .map(|(id, (x, y))| Site::raw(id, Point::new_raw_xy(x, y), id))
        .collect();
    let clip_polygon = Polygon::new(rectangle(-20.0, -20.0, 20.0, 20.0), vec![rectangle(
        -12.0, -1.0, 12.0, 1.0,
    )]);
    let atoms = node_polygon(&sites, &clip_polygon);
    let mut subdivision = build_exact_subdivision(atoms).unwrap();
    assert_boundary_walk_euler(&subdivision);
    let primal_edges = certified_delaunay(&sites).unwrap().primal_edges();
    let adjacency = certified_site_adjacency(&primal_edges, sites.len()).unwrap();
    graft_components(&mut subdivision, &sites, &adjacency).unwrap();
    assert_boundary_walk_euler(&subdivision);
    let parts = assemble_cells(&subdivision).unwrap();
    for part in &parts {
        assert_eq!(part.shell.edges.len() + 1, part.shell.vertices.len());
        assert!(
            part.holes
                .iter()
                .all(|hole| hole.edges.len() + 1 == hole.vertices.len())
        );
    }
    let top_faces: std::collections::BTreeSet<_> = parts
        .iter()
        .filter(|part| part.owner == SiteId(1))
        .map(|part| part.face)
        .collect();
    assert_eq!(parts.len(), 4);
    assert_eq!(top_faces.len(), 2);
}

#[test]
fn isolated_component_exterior_inherits_the_containing_face() {
    let sites: Vec<_> = [(-10.0, -10.0), (0.0, 10.0), (10.0, -10.0)]
        .into_iter()
        .enumerate()
        .map(|(id, (x, y))| Site::raw(id, Point::new_raw_xy(x, y), id))
        .collect();
    let clip_polygon = Polygon::new(rectangle(-20.0, -20.0, 20.0, 20.0), vec![rectangle(
        -1.0, -1.0, 1.0, 1.0,
    )]);
    let atoms = node_polygon(&sites, &clip_polygon);
    let mut subdivision = build_exact_subdivision(atoms).unwrap();
    assert_boundary_walk_euler(&subdivision);
    let primal_edges = certified_delaunay(&sites).unwrap().primal_edges();
    let adjacency = certified_site_adjacency(&primal_edges, sites.len()).unwrap();
    graft_components(&mut subdivision, &sites, &adjacency).unwrap();
    assert_boundary_walk_euler(&subdivision);
    let inherited: Vec<_> = subdivision
        .labels
        .iter()
        .enumerate()
        .filter(|(index, label)| {
            selected_cell_label(**label)
                && exact::cycle_orientation(&walk_points(
                    &subdivision,
                    BoundaryFaceId(*index as u32),
                ))
                .unwrap()
                    == ExactSign::Negative
        })
        .map(|(_, label)| *label)
        .collect();
    assert_eq!(inherited.len(), 1);
    assert_eq!(inherited[0].frame_winding, 1);
    assert_eq!(inherited[0].clip_winding, 1);
    assert!(inherited[0].owner.is_some());
    let parts = assemble_cells(&subdivision).unwrap();
    assert_eq!(parts.iter().map(|part| part.holes.len()).sum::<usize>(), 1);
}

#[test]
fn owner_local_faces_match_exact_membership_on_touching_producer_rings() {
    let sites = sites_xy(&[(-10.0, -10.0), (10.0, -10.0), (0.0, 10.0)]);
    let fixtures = [
        (
            "one-hole-multi-owner",
            Polygon::new(rectangle(-20.0, -20.0, 20.0, 20.0), vec![rectangle(
                -1.0, -1.0, 1.0, 1.0,
            )]),
            3,
            4,
        ),
        (
            "shell-tangent",
            Polygon::new(rectangle(0.0, 0.0, 8.0, 8.0), vec![closed_ring(&[
                (0.0, 4.0),
                (2.0, 5.0),
                (2.0, 3.0),
            ])]),
            2,
            3,
        ),
        (
            "two-holes-touch",
            Polygon::new(rectangle(0.0, 0.0, 10.0, 10.0), vec![
                rectangle(2.0, 2.0, 4.0, 4.0),
                rectangle(4.0, 4.0, 6.0, 6.0),
            ]),
            2,
            4,
        ),
    ];

    for (name, clip, expected_parts, expected_rings) in fixtures {
        let subdivision = grafted_clip_subdivision(&sites, &clip);
        assembly::reset_owner_local_work();
        let parts = assemble_cells(&subdivision).unwrap_or_else(|error| {
            panic!("{name}: owner-local assembly rejected producer output: {error}")
        });
        let work = assembly::owner_local_work();

        assert_eq!(parts.len(), expected_parts, "{name}");
        assert_eq!(
            parts.iter().map(|part| 1 + part.holes.len()).sum::<usize>(),
            expected_rings,
            "{name}"
        );
        assert_owner_parts_match_exact_point_oracle(&subdivision, &parts);
        assert_eq!(
            work.global_edge_slot_visits,
            subdivision.edge_of_slot.len(),
            "{name}: build one global slot inverse, independent of owner count"
        );
        assert!(
            work.owner_ring_edges <= subdivision.edge_of_slot.len(),
            "{name}"
        );
        assert!(work.host_queries <= work.components, "{name}");
        assert!(
            work.face_walks <= work.owner_ring_edges + work.components,
            "{name}"
        );
        assert!(work.ring_side_checks > 0, "{name}");

        if name == "one-hole-multi-owner" {
            let slots_by_edge = assembly::inverse_edge_slots(&subdivision);
            let owner_zero = parts
                .iter()
                .find(|part| part.owner == SiteId(0))
                .expect("minimal source fixture has owner zero");
            let owner_only = assembly::OwnerLocalFaces::build(
                &subdivision,
                &[&owner_zero.shell],
                &slots_by_edge,
            )
            .unwrap();
            assert!(
                owner_only
                    .ring_side_faces(&subdivision, &slots_by_edge, &owner_zero.shell)
                    .is_ok(),
                "filtering foreign darts must leave a constant shell side"
            );

            let all_owner_rings: Vec<_> = parts
                .iter()
                .flat_map(|part| std::iter::once(&part.shell).chain(part.holes.iter()))
                .collect();
            let foreign_boundary_mutant =
                assembly::OwnerLocalFaces::build(&subdivision, &all_owner_rings, &slots_by_edge)
                    .unwrap();
            let mutant_error = foreign_boundary_mutant
                .ring_side_faces(&subdivision, &slots_by_edge, &owner_zero.shell)
                .expect_err("foreign owner ring edges must not be retained as barriers");
            assert!(
                mutant_error
                    .to_string()
                    .contains("nonconstant left or right face"),
                "foreign-boundary mutant failed unexpectedly: {mutant_error}"
            );
        }
    }
}

#[test]
fn reversed_owner_local_host_face_is_rejected_by_exact_shell_membership() {
    let sites = sites_xy(&[(-10.0, -10.0), (10.0, -10.0), (0.0, 10.0)]);
    let clip = Polygon::new(rectangle(-20.0, -20.0, 20.0, 20.0), vec![rectangle(
        -1.0, -1.0, 1.0, 1.0,
    )]);
    let subdivision = grafted_clip_subdivision(&sites, &clip);
    let parts = assemble_cells(&subdivision).unwrap();
    assert_eq!(parts.len(), 3);
    assert_owner_parts_match_exact_point_oracle(&subdivision, &parts);

    let part = parts
        .iter()
        .find(|part| !part.holes.is_empty())
        .expect("the producer fixture has one hole-bearing owner part");
    let owner_rings: Vec<_> = std::iter::once(&part.shell)
        .chain(part.holes.iter())
        .collect();
    let cycle_rings: Vec<_> = (0..owner_rings.len()).collect();
    let slots_by_edge = assembly::inverse_edge_slots(&subdivision);
    let reversed = assembly::OwnerLocalFaces::reversed_host_cycle_facts(
        &subdivision,
        &owner_rings,
        &slots_by_edge,
        &cycle_rings,
        &part.shell,
        &part.holes,
    );
    let rejected = match reversed {
        Err(error) => {
            assert!(
                error.to_string().contains("owner-local"),
                "reversed host face failed for an unrelated reason: {error}"
            );
            true
        },
        Ok(facts) => validate_owner_cycle_facts(&facts, owner_rings.len(), 0).is_err(),
    };
    assert!(
        rejected,
        "choosing the west face of a leftward-ray hit must not satisfy the exact shell oracle"
    );
}

#[test]
fn owner_local_work_scales_with_selected_ring_edges_and_keeps_no_hole_fast_path() {
    let sites = sites_xy(&[(1000.0, 5.0), (1000.0, 1_000_000.0), (1_000_000.0, 5.0)]);
    for hole_count in [128_usize, 256, 512] {
        let holes = (0..hole_count)
            .map(|index| {
                let ordinal =
                    f64::from(u32::try_from(index).expect("the fixture has fewer than u32 holes"));
                let x = 1.0 + ordinal * 1.4;
                closed_ring(&[(x, 5.0), (x + 0.25, 4.5), (x - 0.25, 4.5)])
            })
            .collect();
        let clip = Polygon::new(rectangle(0.0, 0.0, 2000.0, 10.0), holes);
        let subdivision = grafted_clip_subdivision(&sites, &clip);
        assembly::reset_owner_local_work();
        let (parts, allocations) =
            crate::test_support::allocation_count(|| assemble_cells(&subdivision).unwrap());
        let work = assembly::owner_local_work();
        eprintln!(
            "owner-local scaling holes={hole_count} allocations={allocations} \
             ring_edges={} global_slot_visits={} vertices={} faces={} components={} \
             queries={} hits={} axis_comparisons={} orientations={}",
            work.owner_ring_edges,
            work.global_edge_slot_visits,
            work.owner_vertices,
            work.face_walks,
            work.components,
            work.host_queries,
            work.ray_hits,
            work.ray_axis_comparisons,
            work.ray_orientations,
        );

        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].holes.len(), hole_count);
        assert_eq!(work.owner_ring_edges, 4 + 3 * hole_count);
        assert_eq!(work.owner_vertices, work.owner_ring_edges);
        assert_eq!(work.host_queries, hole_count + 1);
        assert_eq!(work.ray_hits, hole_count);
        assert_eq!(work.ring_side_checks, hole_count + 1);
        assert_eq!(work.global_edge_slot_visits, subdivision.edge_of_slot.len());
        assert!(work.global_edge_slot_visits <= work.owner_ring_edges * 2);
        assert!(work.face_walks <= work.owner_ring_edges + work.components);
        assert!(work.ray_axis_comparisons < work.owner_ring_edges * 256);
        assert!(work.ray_orientations < work.owner_ring_edges * 256);
    }

    let clip = Polygon::new(rectangle(0.0, 0.0, 2000.0, 10.0), Vec::new());
    let subdivision = grafted_clip_subdivision(&sites, &clip);
    assembly::reset_owner_local_work();
    let parts = assemble_cells(&subdivision).unwrap();
    let work = assembly::owner_local_work();
    assert_eq!(parts.len(), 1);
    assert_eq!(work, assembly::OwnerLocalWork::default());
}

#[test]
fn two_isolated_holes_in_split_same_owner_cell_pair_by_containment() {
    let sites: Vec<_> = [(-10.0, -10.0), (0.0, 10.0), (10.0, -10.0)]
        .into_iter()
        .enumerate()
        .map(|(id, (x, y))| Site::raw(id, Point::new_raw_xy(x, y), id))
        .collect();
    let clip_polygon = Polygon::new(rectangle(-20.0, -20.0, 20.0, 20.0), vec![
        rectangle(-12.0, -1.0, 12.0, 1.0),
        rectangle(-8.0, 6.0, -6.0, 8.0),
        rectangle(-1.2, -1.7, -0.6, -1.2),
    ]);
    let subdivision = grafted_clip_subdivision(&sites, &clip_polygon);
    assert_boundary_walk_euler(&subdivision);
    assert!(
        first_equal_label_misattaches_a_hole(&subdivision),
        "first equal-label attachment must pick a non-containing shell on this split same-owner diagram"
    );
    let restored = assemble_cells(&subdivision).unwrap();
    let top: Vec<_> = restored
        .iter()
        .filter(|part| part.owner == SiteId(1))
        .collect();
    assert_eq!(top.len(), 2);
    assert_eq!(top.iter().map(|part| part.holes.len()).sum::<usize>(), 2);
    assert!(top.iter().all(|part| part.holes.len() == 1));
}

#[test]
fn validate_rejects_a_mutated_twin_walk_label() {
    let sites: Vec<_> = [(-10.0, -10.0), (0.0, 10.0), (10.0, -10.0)]
        .into_iter()
        .enumerate()
        .map(|(id, (x, y))| Site::raw(id, Point::new_raw_xy(x, y), id))
        .collect();
    let clip_polygon = Polygon::new(rectangle(-20.0, -20.0, 20.0, 20.0), vec![rectangle(
        -1.0, -1.0, 1.0, 1.0,
    )]);
    let mut subdivision = grafted_clip_subdivision(&sites, &clip_polygon);
    validate_exact_subdivision(&subdivision).unwrap();
    let slot = (0..subdivision.topology.targets.len())
        .find(|&slot| {
            let left = walk_label(
                &subdivision,
                BoundaryFaceId(subdivision.topology.face_of[slot]),
            );
            selected_cell_label(left)
        })
        .expect("selected walk has a slot");
    let face = subdivision.topology.face_of[slot] as usize;
    subdivision.labels[face] = RegionLabel {
        frame_winding: 0,
        clip_winding: 0,
        owner: None,
    };
    assert!(validate_exact_subdivision(&subdivision).is_err());
}

#[test]
fn collinear_holed_clip_splits_each_separator_into_two_pieces() {
    let sites: Vec<_> = [(-2.0, 0.0), (0.0, 0.0), (2.0, 0.0)]
        .into_iter()
        .enumerate()
        .map(|(id, (x, y))| Site::raw(id, Point::new_raw_xy(x, y), id))
        .collect();
    let clip = Polygon::new(rectangle(-3.0, -3.0, 3.0, 3.0), vec![rectangle(
        -1.5, -1.0, 1.5, 1.0,
    )]);
    let mut budget = ExpansionBudget::new("test", "collinear Voronoi topology");
    let edges = build_collinear_edges(
        &sites,
        VoronoiBoundary::Polygon(&clip),
        &mut budget,
        &certified_delaunay(&sites).unwrap(),
    )
    .unwrap()
    .into_line_seqs()
    .unwrap();
    assert_eq!(edges.len(), 4);
    let mut endpoints: Vec<_> = edges
        .iter()
        .map(|edge| {
            let first = edge.first().expect("collinear edge has a first point").xy();
            let last = edge.last().expect("collinear edge has a last point").xy();
            (first.x, first.y, last.x, last.y)
        })
        .collect();
    endpoints.sort_unstable_by(|left, right| {
        left.0
            .total_cmp(&right.0)
            .then_with(|| left.1.total_cmp(&right.1))
    });
    assert_eq!(endpoints, vec![
        (-1.0, -3.0, -1.0, -1.0),
        (-1.0, 1.0, -1.0, 3.0),
        (1.0, -3.0, 1.0, -1.0),
        (1.0, 1.0, 1.0, 3.0),
    ]);

    let source = Shape::MultiPoint(
        sites
            .iter()
            .map(|site| site.point)
            .collect::<Vec<_>>()
            .into(),
    );
    let mut polygon_budget = ExpansionBudget::new("voronoi_cells", "sites/clip topology");
    let outputs = source
        .voronoi_polygon_outputs_budgeted(0.0, VoronoiBoundary::Polygon(&clip), &mut polygon_budget)
        .unwrap();
    assert_eq!(outputs.len(), 4);
    assert!(outputs.iter().all(|output| {
        output.edge_roles.len() == output.polygon.rings().count()
            && output.edge_roles.iter().all(|edges| {
                !edges.is_empty()
                    && edges.iter().any(|role| {
                        role.origins
                            .iter()
                            .any(|origin| !matches!(origin, VoronoiEdgeOrigin::SourceClip { .. }))
                    })
            })
    }));
    assert!(outputs.iter().any(|output| {
        output
            .edge_roles
            .iter()
            .flatten()
            .flat_map(|role| &role.origins)
            .any(|origin| matches!(origin, VoronoiEdgeOrigin::SourceClip { .. }))
    }));
}

#[test]
fn frame_nearest_ignores_a_tie_that_is_not_the_minimum() {
    let sites: Vec<_> = [(-1.0, 0.0), (1.0, 0.0), (0.0, 0.0)]
        .into_iter()
        .enumerate()
        .map(|(id, (x, y))| Site::raw(id, Point::new_raw_xy(x, y), id))
        .collect();
    let point = exact::ExactPoint::from_xy(XY::new(0.0, 10.0));
    assert_eq!(nearest_site(&point, &sites).unwrap(), SiteId(2));
    let true_tie = exact::ExactPoint::from_xy(XY::new(0.0, 0.0));
    nearest_site(&true_tie, &sites[..2]).unwrap_err();
}

#[test]
fn finite_extreme_collinear_envelope_is_explicitly_degenerate() {
    let sites: Vec<_> = [(-f64::MAX, 0.0), (f64::MAX, 0.0)]
        .into_iter()
        .enumerate()
        .map(|(id, (x, y))| Site::raw(id, Point::new_raw_xy(x, y), id))
        .collect();
    let mut budget = ExpansionBudget::new("test", "collinear Voronoi topology");
    assert!(
        build_collinear_edges(
            &sites,
            VoronoiBoundary::Envelope,
            &mut budget,
            &certified_delaunay(&sites).unwrap(),
        )
        .unwrap()
        .into_line_seqs()
        .unwrap()
        .is_empty()
    );
}

#[test]
fn sloped_collinear_envelope_emits_analytic_bisector() {
    let sites: Vec<_> = [(0.0, 0.0), (2.0, 2.0)]
        .into_iter()
        .enumerate()
        .map(|(id, (x, y))| Site::raw(id, Point::new_raw_xy(x, y), id))
        .collect();
    let mut budget = ExpansionBudget::new("test", "collinear Voronoi topology");
    let edges = build_collinear_edges(
        &sites,
        VoronoiBoundary::Envelope,
        &mut budget,
        &certified_delaunay(&sites).unwrap(),
    )
    .unwrap()
    .into_line_seqs()
    .unwrap();
    assert_eq!(edges.len(), 1);
    let mut endpoints: Vec<_> = edges[0].iter().map(|point| point.xy()).collect();
    endpoints.sort_unstable_by(|left, right| left.x.total_cmp(&right.x));
    assert_eq!(endpoints, [XY::new(0.0, 2.0), XY::new(2.0, 0.0)]);
}

#[test]
fn collinear_edge_projection_follows_mesh_site_order() {
    let sites = sites_xy(&[
        (-1.0, 100.0),
        (-0.5, 50.0),
        (0.0, 0.0),
        (0.5, -50.0),
        (1.0, -100.0),
    ]);
    let mesh = certified_delaunay(&sites).unwrap();
    let order = mesh.collinear_order().unwrap();
    assert_eq!(order, &[4, 3, 2, 1, 0]);

    let corners = [
        XY::new(-2.0, -200.0),
        XY::new(2.0, -200.0),
        XY::new(2.0, 200.0),
        XY::new(-2.0, 200.0),
    ];
    let mut boundary = Vec::new();
    for side in 0..corners.len() {
        let start = corners[side];
        let end = corners[(side + 1) % corners.len()];
        for step in 0..20 {
            let fraction = f64::from(step) / 20.0;
            boundary.push(Point::new_raw_xy(
                start.x + (end.x - start.x) * fraction,
                start.y + (end.y - start.y) * fraction,
            ));
        }
    }
    let clip = Polygon::new(Ring::closed(boundary).unwrap(), Vec::new());
    let mut budget = ExpansionBudget::new("test", "collinear projection order");
    let edges = build_collinear_edges(&sites, VoronoiBoundary::Polygon(&clip), &mut budget, &mesh)
        .unwrap()
        .into_line_seqs()
        .unwrap();

    assert_eq!(edges.len(), order.len() - 1);
}

#[test]
fn tolerance_clustered_sites_do_not_tie_private_frame_ownership() {
    let sites = super::shape::canonical_voronoi_sites(
        [(0.0, 0.0), (0.09, 0.0), (0.18, 0.0), (0.0, 1.0), (1.0, 0.0)]
            .into_iter()
            .map(|(x, y)| Point::new_raw_xy(x, y))
            .collect(),
        0.1,
    )
    .unwrap();
    let clip = Polygon::new(rectangle(-1.0, -1.0, 2.0, 2.0), Vec::new());
    let mut budget = ExpansionBudget::new("test", "Voronoi topology");
    let diagram = build(
        &sites,
        VoronoiBoundary::Polygon(&clip),
        &mut budget,
        true,
        &certified_delaunay(&sites).unwrap(),
    )
    .unwrap();
    assert!(!diagram.into_edges().into_line_seqs().unwrap().is_empty());
}

#[test]
fn semantic_admission_mutants_change_validity_overlap_gap_and_spill() {
    let shape = |min_x, min_y, max_x, max_y| {
        Shape::Polygon(Polygon::new(
            rectangle(min_x, min_y, max_x, max_y),
            Vec::new(),
        ))
    };
    let clip = shape(0.0, 0.0, 4.0, 4.0);
    let restored_rows = vec![shape(0.0, 0.0, 2.0, 4.0), shape(2.0, 0.0, 4.0, 4.0)];
    let mutated_overlap = vec![clip.clone(), shape(0.0, 0.0, 2.0, 4.0)];
    let restored_coverage = coverage_is_valid(&restored_rows, 0.0).unwrap();
    let mutated_coverage = coverage_is_valid(&mutated_overlap, 0.0).unwrap();
    assert_ne!(
        mutated_coverage, restored_coverage,
        "mutated overlap={mutated_coverage}, restored partition={restored_coverage}"
    );
    assert!(!mutated_coverage && restored_coverage);

    let restored_union = Shape::union_all(&restored_rows, Strictness::Lenient).unwrap();
    let mutated_gap_union = Shape::union_all(
        &[shape(0.0, 0.0, 1.9, 4.0), shape(2.1, 0.0, 4.0, 4.0)],
        Strictness::Lenient,
    )
    .unwrap();
    let restored_gap = !clip
        .difference(&restored_union, Strictness::Lenient)
        .unwrap()
        .is_empty();
    let mutated_gap = !clip
        .difference(&mutated_gap_union, Strictness::Lenient)
        .unwrap()
        .is_empty();
    assert_ne!(
        mutated_gap, restored_gap,
        "mutated gap={mutated_gap}, restored gap={restored_gap}"
    );

    let mutated_spill_union = Shape::union_all(
        &[clip.clone(), shape(5.0, 0.0, 6.0, 1.0)],
        Strictness::Lenient,
    )
    .unwrap();
    let restored_spill = !restored_union
        .difference(&clip, Strictness::Lenient)
        .unwrap()
        .is_empty();
    let mutated_spill = !mutated_spill_union
        .difference(&clip, Strictness::Lenient)
        .unwrap()
        .is_empty();
    assert_ne!(
        mutated_spill, restored_spill,
        "mutated spill={mutated_spill}, restored spill={restored_spill}"
    );

    let restored_polygon = Polygon::new(rectangle(0.0, 0.0, 4.0, 4.0), Vec::new());
    let mutated_polygon = Polygon::new(
        Ring::closed(
            [(0.0, 0.0), (4.0, 4.0), (0.0, 4.0), (4.0, 0.0), (0.0, 0.0)]
                .into_iter()
                .map(|(x, y)| Point::new_raw_xy(x, y))
                .collect(),
        )
        .unwrap(),
        Vec::new(),
    );
    let restored_valid = polygon_is_valid(&restored_polygon).unwrap();
    let mutated_valid = polygon_is_valid(&mutated_polygon).unwrap();
    assert_ne!(
        mutated_valid, restored_valid,
        "mutated valid={mutated_valid}, restored valid={restored_valid}"
    );
}

#[test]
fn transition_and_budget_mutants_report_distinct_restored_values() {
    let left = RegionLabel {
        frame_winding: 1,
        clip_winding: 1,
        owner: Some(SiteId(0)),
    };
    let transition = Transition {
        frame_delta: 0,
        clip_delta: 1,
        owner: OwnerTransition::Boundary {
            left: Some(SiteId(0)),
            right: Some(SiteId(1)),
        },
    };
    let restored = crossed_label(left, transition).unwrap();
    let mutated = crossed_label(left, Transition {
        clip_delta: 0,
        owner: OwnerTransition::Preserve,
        ..transition
    })
    .unwrap();
    assert_ne!(
        mutated, restored,
        "mutated transition={mutated:?}, restored transition={restored:?}"
    );
    assert_eq!(restored.clip_winding, 0);
    assert_eq!(restored.owner, Some(SiteId(1)));

    let inherited = left;
    let zero_seeded = RegionLabel {
        frame_winding: 0,
        clip_winding: 0,
        owner: None,
    };
    assert_ne!(
        zero_seeded, inherited,
        "mutated inherited label={zero_seeded:?}, restored inherited label={inherited:?}"
    );

    let exact = ExpansionBudget::check("test", "work", GENERATED_ITEM_LIMIT).unwrap();
    let plus_one_rejected =
        ExpansionBudget::check("test", "work", GENERATED_ITEM_LIMIT + 1).is_err();
    assert_ne!(
        plus_one_rejected,
        exact != GENERATED_ITEM_LIMIT,
        "limit+1 rejected={plus_one_rejected}, exact admitted={exact}"
    );
    let finite_product = ExpansionBudget::product("test", "work", 4, 4).unwrap();
    let overflow_rejected = ExpansionBudget::product("test", "work", usize::MAX, 2).is_err();
    assert_ne!(
        overflow_rejected,
        finite_product != 16,
        "overflow rejected={overflow_rejected}, restored product={finite_product}"
    );
}

#[test]
fn dual_completeness_rejects_a_missing_cell_mutant() {
    let sites: Vec<_> = [(0.0, 0.0), (2.0, 0.0), (0.0, 2.0), (2.0, 2.0), (0.8, 1.1)]
        .into_iter()
        .enumerate()
        .map(|(id, (x, y))| Site::raw(id, Point::new_raw_xy(x, y), id))
        .collect();
    let clip = normalize_clip(&sites, VoronoiBoundary::Envelope, false).unwrap();
    let mesh = certified_delaunay(&sites).unwrap();
    let complex = delaunay_complex(&mesh, &sites).unwrap();
    let restored = dual_build_rings(&sites, &mesh, complex, &clip).unwrap();
    let mut mutated = restored.clone();
    mutated.rings.pop();

    let mut restored_budget = ExpansionBudget::new("test", "restored dual completeness");
    let restored_diagram = dual_embed(
        &restored,
        &clip,
        &sites,
        VoronoiProduct::Both,
        &mut restored_budget,
        None,
    )
    .unwrap();
    let restored_admitted = true;
    let mut missing_rows: Vec<_> = restored_diagram
        .polygons
        .iter()
        .map(|output| Shape::Polygon(output.polygon.clone()))
        .collect();
    missing_rows.pop();
    assert!(
        coverage_is_valid(&missing_rows, 0.0).unwrap(),
        "coverage validity deliberately does not certify clip completeness"
    );
    let mut mutated_budget = ExpansionBudget::new("test", "mutated dual completeness");
    let mutated_admitted = dual_embed(
        &mutated,
        &clip,
        &sites,
        VoronoiProduct::Both,
        &mut mutated_budget,
        None,
    )
    .is_ok();
    assert_ne!(
        mutated_admitted, restored_admitted,
        "mutated missing-cell admitted={mutated_admitted}, restored admitted={restored_admitted}"
    );
    assert!(!mutated_admitted && restored_admitted);
}

fn shared_dual_vertex(rings: &DualRings) -> (usize, usize, exact::ExactPoint) {
    for (ring_index, ring) in rings.rings.iter().enumerate() {
        for (vertex_index, vertex) in ring.iter().enumerate() {
            let point = dual_vertex_point(vertex, &rings.centers);
            if rings.rings.iter().enumerate().any(|(other_index, other)| {
                other_index != ring_index
                    && other.iter().any(|other_vertex| {
                        point
                            .same_position(dual_vertex_point(other_vertex, &rings.centers))
                            .unwrap()
                    })
            }) {
                return (ring_index, vertex_index, point.clone());
            }
        }
    }
    panic!("dual mutation fixture has no shared vertex");
}

fn shared_boundary_vertex(
    rings: &DualRings,
    bounds: [f64; 4],
) -> (usize, usize, exact::ExactPoint, usize) {
    for (ring_index, ring) in rings.rings.iter().enumerate() {
        for (vertex_index, vertex) in ring.iter().enumerate() {
            let point = dual_vertex_point(vertex, &rings.centers);
            let rounded = point.round_nearest_even().unwrap();
            let Some(side) = rounded_boundary_side(rounded, bounds) else {
                continue;
            };
            if rings.rings.iter().enumerate().any(|(other_index, other)| {
                other_index != ring_index
                    && other.iter().any(|other_vertex| {
                        point
                            .same_position(dual_vertex_point(other_vertex, &rings.centers))
                            .unwrap()
                    })
            }) {
                return (ring_index, vertex_index, point.clone(), side);
            }
        }
    }
    panic!("dual mutation fixture has no shared boundary vertex");
}

fn rounded_duplicate(point: &exact::ExactPoint) -> exact::ExactPoint {
    let rounded = point.round_nearest_even().unwrap();
    let base = exact::ExactPoint::from_xy(rounded);
    let upper = exact::ExactPoint::from_xy(XY::new(rounded.x.next_up(), rounded.y));
    base.midpoint(&upper).unwrap().midpoint(&base).unwrap()
}

fn rounded_boundary_side(point: XY, bounds: [f64; 4]) -> Option<usize> {
    if point.y == bounds[2] {
        Some(0)
    } else if point.x == bounds[1] {
        Some(1)
    } else if point.y == bounds[3] {
        Some(2)
    } else if point.x == bounds[0] {
        Some(3)
    } else {
        None
    }
}

#[test]
fn dual_local_validator_rejects_deterministic_partition_mutations() {
    let sites: Vec<_> = [
        (0.0, 0.0),
        (1.0, 0.0),
        (0.0, 1.0),
        (1.0, 1.0),
        (0.5, 0.4),
        (0.2, 0.7),
    ]
    .into_iter()
    .enumerate()
    .map(|(id, (x, y))| Site::raw(id, Point::new_raw_xy(x, y), id))
    .collect();
    let clip = normalize_clip(&sites, VoronoiBoundary::Envelope, false).unwrap();
    let mesh = certified_delaunay(&sites).unwrap();
    let complex = delaunay_complex(&mesh, &sites).unwrap();
    let restored = dual_build_rings(&sites, &mesh, complex, &clip).unwrap();
    let mut restored_budget = ExpansionBudget::new("test", "restored dual mutation fixture");
    dual_embed(
        &restored,
        &clip,
        &sites,
        VoronoiProduct::Both,
        &mut restored_budget,
        None,
    )
    .unwrap();

    let mut mutations = Vec::new();

    let mut missing_cell = restored.clone();
    missing_cell.rings.pop();
    mutations.push(("missing cell", missing_cell));

    let mut reversed_ring = restored.clone();
    reversed_ring.rings[0].reverse();
    mutations.push(("reversed ring", reversed_ring));

    let (shared_ring, shared_vertex, shared_point) = shared_dual_vertex(&restored);
    let mut moved_shared_vertex = restored.clone();
    let moved = shared_point.round_nearest_even().unwrap();
    moved_shared_vertex.rings[shared_ring][shared_vertex] = DualVertex::Generated(Box::new(
        exact::ExactPoint::from_xy(XY::new(moved.x + 0.125, moved.y)),
    ));
    mutations.push(("moved shared vertex", moved_shared_vertex));

    let mut duplicate_key = restored.clone();
    let duplicate = rounded_duplicate(&shared_point);
    assert_eq!(
        PointKey::new(duplicate.round_nearest_even().unwrap()),
        PointKey::new(shared_point.round_nearest_even().unwrap())
    );
    assert!(!duplicate.same_position(&shared_point).unwrap());
    duplicate_key.rings[shared_ring][shared_vertex] = DualVertex::Generated(Box::new(duplicate));
    mutations.push(("duplicate rounded key", duplicate_key));

    let bounds = clip.frame_bounds();
    let mut wrong_boundary_direction = restored.clone();
    let mut swapped_boundary = false;
    'find_boundary: for ring in &mut wrong_boundary_direction.rings {
        for index in 0..ring.len() {
            let next = (index + 1) % ring.len();
            let left = dual_vertex_point(&ring[index], &restored.centers)
                .round_nearest_even()
                .unwrap();
            let right = dual_vertex_point(&ring[next], &restored.centers)
                .round_nearest_even()
                .unwrap();
            if rounded_boundary_side(left, bounds).is_some()
                && rounded_boundary_side(left, bounds) == rounded_boundary_side(right, bounds)
            {
                ring.swap(index, next);
                swapped_boundary = true;
                break 'find_boundary;
            }
        }
    }
    assert!(
        swapped_boundary,
        "dual mutation fixture has no boundary edge"
    );
    mutations.push(("wrong boundary direction", wrong_boundary_direction));

    let (boundary_ring, boundary_vertex, boundary_point, boundary_side) =
        shared_boundary_vertex(&restored, bounds);
    let boundary_rounded = boundary_point.round_nearest_even().unwrap();
    for (name, direction) in [("gap", 1.0), ("overlap", -1.0)] {
        let mut mutation = restored.clone();
        let shifted = if boundary_side % 2 == 0 {
            XY::new(boundary_rounded.x + direction * 0.125, boundary_rounded.y)
        } else {
            XY::new(boundary_rounded.x, boundary_rounded.y + direction * 0.125)
        };
        mutation.rings[boundary_ring][boundary_vertex] =
            DualVertex::Generated(Box::new(exact::ExactPoint::from_xy(shifted)));
        mutations.push((name, mutation));
    }

    let mut crossed_ring = restored.clone();
    let crossed_index = crossed_ring
        .rings
        .iter()
        .position(|ring| ring.len() >= 5)
        .expect("dual mutation fixture has no crossable ring");
    crossed_ring.rings[crossed_index].swap(1, 2);
    mutations.push(("crossed ring", crossed_ring));

    for (name, mutation) in mutations {
        let mut budget = ExpansionBudget::new("test", "mutated dual partition");
        assert!(
            dual_embed(
                &mutation,
                &clip,
                &sites,
                VoronoiProduct::Both,
                &mut budget,
                None,
            )
            .is_err(),
            "dual mutation unexpectedly admitted: {name}"
        );
    }
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
        assert!(
            outputs
                .iter()
                .enumerate()
                .filter(|(_, output)| {
                    Shape::Polygon(output.polygon.clone()).covers(site).unwrap()
                })
                .count()
                == 1
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

    let dual_sites = crate::geometry::tessellation::shape::canonical_voronoi_sites(
        [(170.0, 0.0), (190.0, 0.0), (190.0, 10.0), (170.0, 8.0)]
            .into_iter()
            .map(|(x, y)| Point::new_raw_xy(x, y))
            .collect(),
        0.0,
    )
    .unwrap();
    let mesh = certified_delaunay(&dual_sites).unwrap();
    let complex = delaunay_complex(&mesh, &dual_sites).unwrap();
    for vertex in &dual_outputs.receipt.vertices {
        for origin in &vertex.origins {
            if let VoronoiVertexOrigin::Circumcenter { component, sites } = origin {
                let component = usize::try_from(*component).unwrap();
                assert_eq!(
                    complex.center_sites.get(component),
                    Some(sites),
                    "dual receipt preserves each center's defining Delaunay face"
                );
                let center = complex.center(component);
                assert_eq!(vertex.exact_xy, exact_xy_coordinates(center).unwrap());
                assert_eq!(
                    vertex.assignment,
                    Some(center.round_nearest_even().unwrap())
                );
            }
        }
    }
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
