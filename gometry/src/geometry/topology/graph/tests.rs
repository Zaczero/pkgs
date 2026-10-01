use super::*;
use crate::geometry::{CoordinateAxes, GeometryKind, LineSeq, MultiMember, Ring};

fn degrees() -> crate::geographic::GeographicFrame {
    crate::crs::info("EPSG:4326")
        .unwrap()
        .geographic_frame()
        .unwrap()
}

fn geographic_graph(shape: &Shape) -> TopologyGraph {
    TopologyGraph::geographic(
        crate::geographic::ScalarGeographicView::try_new(degrees(), shape).expect("fixture admits"),
    )
    .unwrap()
}

fn geographic_graph_in(shape: &Shape, frame: crate::geographic::GeographicFrame) -> TopologyGraph {
    TopologyGraph::geographic(
        crate::geographic::ScalarGeographicView::try_new(frame, shape).expect("fixture admits"),
    )
    .unwrap()
}

fn point(x: f64, y: f64) -> Point {
    Point::new_raw_xy(x, y)
}

fn line(points: &[(f64, f64)]) -> LineSeq {
    LineSeq::try_new(
        CoordSeq::try_from_points(&points.iter().map(|&(x, y)| point(x, y)).collect::<Vec<_>>())
            .unwrap(),
    )
    .expect("line coordinate sequence must contain at least two vertices")
}

fn ring(points: &[(f64, f64)]) -> Ring {
    Ring::from_closed(
        CoordSeq::try_from_points(&points.iter().map(|&(x, y)| point(x, y)).collect::<Vec<_>>())
            .unwrap(),
    )
    .unwrap()
}

fn polygon(shell: &[(f64, f64)], holes: &[&[(f64, f64)]]) -> Polygon {
    Polygon::new(ring(shell), holes.iter().map(|hole| ring(hole)).collect())
}

fn collection(shapes: Vec<Shape>) -> Shape {
    Shape::geometry_collection(shapes).expect("test collection is valid")
}

fn edge_ordinals_at(graph: &TopologyGraph, point: XY) -> Vec<usize> {
    let mut ordinals = Vec::new();
    let _: Option<()> = graph
        .support
        .for_each_edge_at(graph.frame, &graph.edges, point, |edge| {
            let ordinal = graph
                .edges
                .iter()
                .position(|candidate| std::ptr::eq(candidate, edge))
                .expect("edge callback points into the graph edge slice");
            ordinals.push(ordinal);
            std::ops::ControlFlow::Continue(())
        });
    ordinals.sort_unstable();
    ordinals
}

fn brute_edge_ordinals_at(graph: &TopologyGraph, point: XY) -> Vec<usize> {
    graph
        .edges
        .iter()
        .enumerate()
        .filter_map(|(ordinal, edge)| {
            point_on_graph_segment(graph.frame, point, edge.segment).then_some(ordinal)
        })
        .collect()
}

#[test]
fn geographic_seam_aliases_share_one_node() {
    let shape = Shape::MultiPoint(
        crate::geometry::MultiPoint::try_new(CoordinateAxes::XY, vec![
            MultiMember::Value(point(-180.0, 5.0)),
            MultiMember::Value(point(180.0, 5.0)),
        ])
        .expect("homogeneous points"),
    );
    let graph = geographic_graph(&shape);
    assert_eq!(graph.nodes.len(), 1);
    assert!(graph.equivalent(XY::new(-180.0, 5.0), XY::new(180.0, 5.0)));
}

#[test]
fn geographic_pole_longitudes_share_one_node() {
    let shape = Shape::MultiPoint(
        crate::geometry::MultiPoint::try_new(CoordinateAxes::XY, vec![
            MultiMember::Value(point(-30.0, 90.0)),
            MultiMember::Value(point(120.0, 90.0)),
        ])
        .expect("homogeneous points"),
    );
    let graph = geographic_graph(&shape);
    assert_eq!(graph.nodes.len(), 1);
    assert_eq!(graph.node_for(XY::new(0.0, 90.0)), Some(TopologyNodeId(0)));
}

#[test]
fn chart_cut_is_line_interior_not_source_boundary() {
    let shape = Shape::LineString(line(&[(170.0, 0.0), (-170.0, 0.0)]));
    let graph = geographic_graph(&shape);
    assert_eq!(graph.edges.len(), 2);
    assert!(!graph.has_line_boundary(XY::new(180.0, 0.0)));
    assert!(graph.has_line_boundary(XY::new(170.0, 0.0)));
    assert!(graph.has_line_boundary(XY::new(-170.0, 0.0)));
}

#[test]
fn donut_hole_is_not_areal_interior_support() {
    let shape = Shape::Polygon(polygon(
        &[
            (0.0, 0.0),
            (10.0, 0.0),
            (10.0, 10.0),
            (0.0, 10.0),
            (0.0, 0.0),
        ],
        &[&[(3.0, 3.0), (7.0, 3.0), (7.0, 7.0), (3.0, 7.0), (3.0, 3.0)]],
    ));
    let graph = geographic_graph(&shape);
    assert!(graph.contains_area_interior(XY::new(1.0, 1.0)));
    assert!(!graph.contains_area_interior(XY::new(5.0, 5.0)));
    assert!(graph.has_area_boundary(XY::new(5.0, 3.0)));
}

#[expect(
    clippy::float_cmp,
    reason = "a pole or antimeridian is one specific representable coordinate, and the question is whether a vertex IS that coordinate; a tolerance would admit vertices merely near it"
)]
#[test]
fn geographic_donut_hole_stays_outside_across_the_seam() {
    let shape = Shape::Polygon(polygon(
        &[
            (170.0, -10.0),
            (-170.0, -10.0),
            (-170.0, 10.0),
            (170.0, 10.0),
            (170.0, -10.0),
        ],
        &[&[
            (175.0, -5.0),
            (-175.0, -5.0),
            (-175.0, 5.0),
            (175.0, 5.0),
            (175.0, -5.0),
        ]],
    ));
    let graph = geographic_graph(&shape);
    assert_eq!(
        geographic_ring_location(
            graph.area_support[0].geographic_rings().0,
            XY::new(179.0, 8.0),
        ),
        RingLocation::Inside,
    );
    assert!(graph.contains_area_interior(XY::new(179.0, 8.0)));
    assert!(!graph.contains_area_interior(XY::new(-180.0, 0.0)));
    let seam = graph
        .nodes
        .iter()
        .find(|node| node.point.x.abs() == 180.0)
        .expect("crossing polygon has a chart seam node")
        .point;
    assert!(graph.has_area_boundary(seam));
}

#[test]
fn geographic_polar_ring_uses_the_pole_quotient_for_interior_support() {
    let shape = Shape::Polygon(polygon(
        &[
            (-180.0, 80.0),
            (-90.0, 80.0),
            (0.0, 80.0),
            (90.0, 80.0),
            (180.0, 80.0),
            (-180.0, 80.0),
        ],
        &[],
    ));
    let graph = geographic_graph(&shape);
    assert!(graph.contains_area_interior(XY::new(-120.0, 85.0)));
    assert!(graph.contains_area_interior(XY::new(120.0, 85.0)));
    assert!(graph.contains_area_interior(XY::new(0.0, 90.0)));
    assert!(!graph.contains_area_interior(XY::new(0.0, 70.0)));
}

#[test]
fn geographic_full_longitude_band_preserves_area_across_the_quotient_seam() {
    let shape = Shape::Polygon(polygon(
        &[
            (-180.0, -10.0),
            (-90.0, -10.0),
            (0.0, -10.0),
            (90.0, -10.0),
            (180.0, -10.0),
            (180.0, 10.0),
            (90.0, 10.0),
            (0.0, 10.0),
            (-90.0, 10.0),
            (-180.0, 10.0),
            (-180.0, -10.0),
        ],
        &[],
    ));
    let graph = geographic_graph(&shape);
    assert_eq!(
        graph.locate_point(XY::new(0.0, 0.0)),
        TopologyLocation::Interior
    );
    assert_eq!(
        graph.locate_point(XY::new(179.0, 0.0)),
        TopologyLocation::Interior
    );
    assert_eq!(
        graph.locate_point(XY::new(180.0, 0.0)),
        TopologyLocation::Interior
    );
    assert_eq!(
        graph.locate_point(XY::new(0.0, 10.0)),
        TopologyLocation::Boundary
    );
}

#[test]
fn geographic_edge_lookup_is_unique_at_ordinary_and_seam_points() {
    let shape = collection(vec![
        Shape::LineString(line(&[(-10.0, -1.0), (10.0, 3.0)])),
        Shape::LineString(line(&[(170.0, -2.0), (-170.0, 4.0)])),
        Shape::LineString(line(&[(-180.0, -3.0), (180.0, 3.0)])),
    ]);
    let graph = geographic_graph(&shape);
    for point in [
        XY::new(0.0, 1.0),
        XY::new(360.0, 1.0),
        XY::new(180.0, 0.0),
        XY::new(-180.0, 0.0),
        XY::new(180.0, 3.0),
    ] {
        assert_eq!(
            edge_ordinals_at(&graph, point),
            brute_edge_ordinals_at(&graph, point),
            "edge lookup mismatch at {point:?}",
        );
    }
}

#[test]
fn geographic_point_queries_reuse_index_without_allocations() {
    let shape = collection(vec![
        Shape::LineString(line(&[(-10.0, -1.0), (10.0, 3.0)])),
        Shape::LineString(line(&[(170.0, -2.0), (-170.0, 4.0)])),
        Shape::LineString(line(&[(-180.0, -3.0), (180.0, 3.0)])),
    ]);
    let graph = geographic_graph(&shape);
    let points = [
        XY::new(0.0, 1.0),
        XY::new(360.0, 1.0),
        XY::new(180.0, 0.0),
        XY::new(-180.0, 0.0),
        XY::new(23.0, 30.0),
        XY::new(-123.0, 90.0),
    ];
    let ((), allocations) = crate::test_support::allocation_count(|| {
        for _ in 0..8 {
            for point in points {
                let _ = graph.locate_point(point);
            }
        }
    });
    assert_eq!(allocations, 0);
}

#[test]
fn geographic_high_degree_absent_queries_reuse_index_without_allocations() {
    let lines = (0..64)
        .map(|index| {
            let offset = (f64::from(index) + 0.5) / 64.0 - 0.5;
            Shape::LineString(line(&[(-1.0, -1.0 + offset), (1.0, 1.0 + offset)]))
        })
        .collect();
    let graph = geographic_graph(&collection(lines));
    let ((), allocations) = crate::test_support::allocation_count(|| {
        for _ in 0..8 {
            let _ = graph.locate_point(XY::new(0.0, 0.0));
            let _ = graph.locate_point(XY::new(360.0, 0.0));
            let _ = graph.locate_point(XY::new(180.0, 30.0));
            let _ = graph.locate_point(XY::new(12.0, 30.0));
        }
    });
    assert_eq!(allocations, 0);
}

#[test]
fn geographic_polar_hole_excludes_the_pole_without_synthetic_boundary() {
    let shape = Shape::Polygon(polygon(
        &[
            (-180.0, 70.0),
            (-90.0, 70.0),
            (0.0, 70.0),
            (90.0, 70.0),
            (180.0, 70.0),
            (-180.0, 70.0),
        ],
        &[&[
            (-180.0, 80.0),
            (-90.0, 80.0),
            (0.0, 80.0),
            (90.0, 80.0),
            (180.0, 80.0),
            (-180.0, 80.0),
        ]],
    ));
    let graph = geographic_graph(&shape);
    assert_eq!(
        graph.locate_point(XY::new(0.0, 75.0)),
        TopologyLocation::Interior
    );
    assert_eq!(
        graph.locate_point(XY::new(0.0, 85.0)),
        TopologyLocation::Exterior
    );
    assert_eq!(
        graph.locate_point(XY::new(0.0, 90.0)),
        TopologyLocation::Exterior
    );
}

#[test]
fn geographic_duplicate_polygons_aggregate_by_or() {
    let member = Shape::Polygon(polygon(
        &[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (0.0, 0.0)],
        &[],
    ));
    for count in [2, 3] {
        let graph = geographic_graph(&collection(vec![member.clone(); count]));
        assert_eq!(
            graph.locate_point(XY::new(2.0, 2.0)),
            TopologyLocation::Interior
        );
        assert_eq!(
            graph.locate_point(XY::new(2.0, 0.0)),
            TopologyLocation::Boundary
        );
    }
}

#[test]
fn geographic_opposite_retrace_does_not_create_a_face_transition() {
    let shape = Shape::Polygon(polygon(
        &[(0.0, 0.0), (4.0, 0.0), (0.0, 0.0), (0.0, 0.0)],
        &[],
    ));
    let graph = geographic_graph(&shape);
    assert_eq!(
        graph.locate_point(XY::new(2.0, 0.0)),
        TopologyLocation::Boundary
    );
    assert_eq!(
        graph.locate_point(XY::new(2.0, 1.0)),
        TopologyLocation::Exterior
    );
}

#[test]
fn geographic_grad_frame_uses_native_period_and_pole() {
    let unit =
        crate::geographic::AngularFactor::try_new("grad", std::f64::consts::TAU / 400.0).unwrap();
    let frame = crate::geographic::GeographicFrame::try_new(unit, unit, 0.0).unwrap();
    let shape = Shape::Polygon(polygon(
        &[
            (-200.0, 80.0),
            (-100.0, 80.0),
            (0.0, 80.0),
            (100.0, 80.0),
            (200.0, 80.0),
            (-200.0, 80.0),
        ],
        &[],
    ));
    let graph = geographic_graph_in(&shape, frame);
    assert!(frame.longitude().period() > 399.0);
    assert_eq!(
        graph.locate_point(XY::new(0.0, frame.pole())),
        TopologyLocation::Interior
    );
    assert_eq!(
        graph.locate_point(XY::new(0.0, 70.0)),
        TopologyLocation::Exterior
    );
}

#[test]
fn line_boundary_parity_is_collection_wide() {
    let lines = crate::geometry::MultiLineString::try_new(CoordinateAxes::XY, vec![
        MultiMember::Value(line(&[(0.0, 0.0), (1.0, 0.0)])),
        MultiMember::Value(line(&[(1.0, 0.0), (2.0, 0.0)])),
    ])
    .expect("homogeneous lines");
    let graph = geographic_graph(&Shape::MultiLineString(lines));
    assert_eq!(graph.line_boundary.len(), 2);
    assert!(!graph.has_line_boundary(XY::new(1.0, 0.0)));
    assert!(graph.has_line_boundary(XY::new(0.0, 0.0)));
    assert!(graph.has_line_boundary(XY::new(2.0, 0.0)));
}

fn brute_contains_area_interior(graph: &TopologyGraph, point: XY) -> bool {
    graph
        .area_support
        .iter()
        .any(|polygon| polygon_contains_interior(polygon, point))
}

fn brute_has_area_boundary(graph: &TopologyGraph, point: XY) -> bool {
    graph
        .node_for(point)
        .is_some_and(|id| graph.nodes[id.index()].support.area_boundary)
        || graph.edges.iter().any(|edge| {
            edge.is_boundary && point_on_graph_segment(graph.frame, point, edge.segment)
        })
}

fn brute_has_line_support(graph: &TopologyGraph, point: XY) -> bool {
    graph.edges.iter().any(|edge| {
        edge.sources.iter().any(|&source| {
            matches!(
                graph.source_edges[source as usize].support,
                SourceSupport::Line
            )
        }) && point_on_graph_segment(graph.frame, point, edge.segment)
    })
}

fn brute_locate_point(
    graph: &TopologyGraph,
    point: XY,
    include_collapsed_line_points: bool,
) -> TopologyLocation {
    if let Some(node) = graph.node_for(point) {
        match graph.nodes[node.index()].support.area_interior {
            AreaNodeSupport::Complete => return TopologyLocation::Interior,
            AreaNodeSupport::Partial if !brute_contains_area_interior(graph, point) => {
                return TopologyLocation::Boundary;
            },
            AreaNodeSupport::Partial | AreaNodeSupport::None => {},
        }
    }
    if brute_contains_area_interior(graph, point)
        || graph.edges.iter().any(|edge| {
            edge.area_interior_support && point_on_graph_segment(graph.frame, point, edge.segment)
        })
    {
        return TopologyLocation::Interior;
    }
    let area_boundary = brute_has_area_boundary(graph, point);
    let line_boundary = graph.has_line_boundary(point);
    if area_boundary || line_boundary {
        return TopologyLocation::Boundary;
    }
    let point_support = if include_collapsed_line_points {
        graph
            .source_points
            .iter()
            .any(|source| graph.equivalent(source.point.xy(), point))
    } else {
        graph
            .source_points
            .iter()
            .any(|source| !source.collapsed_line && graph.equivalent(source.point.xy(), point))
    };
    if point_support || brute_has_line_support(graph, point) {
        return TopologyLocation::Interior;
    }
    TopologyLocation::Exterior
}

fn assert_indexed_locations_match_brute(
    graph: &TopologyGraph,
    points: impl IntoIterator<Item = XY>,
) {
    for point in points {
        assert_eq!(
            graph.locate_point(point),
            brute_locate_point(graph, point, true),
            "indexed location mismatch at {point:?} in {:?}",
            graph.frame
        );
        assert_eq!(
            graph.locate_point_for_overlay(point),
            brute_locate_point(graph, point, false),
            "indexed overlay location mismatch at {point:?} in {:?}",
            graph.frame
        );
    }
}

#[test]
fn indexed_support_matches_brute_locations_at_source_noded_and_aliased_points() {
    let planar = collection(vec![
        Shape::Polygon(polygon(
            &[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (0.0, 0.0)],
            &[&[(1.0, 1.0), (3.0, 1.0), (3.0, 3.0), (1.0, 3.0), (1.0, 1.0)]],
        )),
        Shape::LineString(line(&[(-1.0, 2.0), (2.0, 2.0), (5.0, 2.0)])),
        Shape::Point(point(8.0, 8.0)),
    ]);
    let geographic = collection(vec![
        Shape::Polygon(polygon(
            &[
                (170.0, 0.0),
                (-170.0, 0.0),
                (-170.0, 12.0),
                (170.0, 12.0),
                (170.0, 0.0),
            ],
            &[&[
                (175.0, 3.0),
                (-175.0, 3.0),
                (-175.0, 8.0),
                (175.0, 8.0),
                (175.0, 3.0),
            ]],
        )),
        Shape::Polygon(polygon(
            &[
                (-180.0, 80.0),
                (-90.0, 80.0),
                (0.0, 80.0),
                (90.0, 80.0),
                (180.0, 80.0),
                (-180.0, 80.0),
            ],
            &[],
        )),
        Shape::LineString(line(&[(175.0, 6.0), (180.0, 6.0), (-175.0, 6.0)])),
    ]);
    for graph in [geographic_graph(&planar), geographic_graph(&geographic)] {
        let mut points = Vec::new();
        points.extend(graph.nodes.iter().map(|node| node.point));
        for edge in &graph.edges {
            points.push(edge.segment.start);
            points.push(edge.segment.end);
            points.push(XY::new(
                f64::midpoint(edge.segment.start.x, edge.segment.end.x),
                f64::midpoint(edge.segment.start.y, edge.segment.end.y),
            ));
        }
        points.extend(graph.source_points.iter().map(|source| source.point.xy()));
        let frame = graph.frame;
        let period = frame.longitude().period();
        let aliases = points.clone();
        points.extend(aliases.into_iter().flat_map(|point| {
            [
                XY::new(point.x - period, point.y),
                point,
                XY::new(point.x + period, point.y),
            ]
        }));
        points.extend([
            XY::new(-137.0, frame.pole()),
            XY::new(223.0, frame.pole()),
            XY::new(-137.0, -frame.pole()),
        ]);
        let one_ulp = f64::from_bits(1.0_f64.to_bits() + 1);
        points.extend([XY::new(one_ulp, 1.0), XY::new(1.0, one_ulp)]);
        assert_indexed_locations_match_brute(&graph, points);
    }
}

#[test]
fn area_support_callback_returns_individual_polygon_ids() {
    let shape = collection(vec![
        Shape::Polygon(polygon(
            &[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (0.0, 0.0)],
            &[],
        )),
        Shape::Polygon(polygon(
            &[(2.0, 2.0), (6.0, 2.0), (6.0, 6.0), (2.0, 6.0), (2.0, 2.0)],
            &[],
        )),
    ]);
    let graph = geographic_graph(&shape);
    let mut ids = Vec::new();
    let _: Option<()> = graph.for_each_area_interior(XY::new(3.0, 3.0), |polygon| {
        ids.push(polygon);
        std::ops::ControlFlow::Continue(())
    });
    ids.sort_unstable();
    assert_eq!(ids, [0, 1]);
    assert!(graph.contains_area_interior(XY::new(3.0, 3.0)));
    assert!(!graph.contains_area_interior(XY::new(1.0, 5.0)));

    let geographic = Shape::Polygon(polygon(
        &[
            (170.0, 0.0),
            (-170.0, 0.0),
            (-170.0, 10.0),
            (170.0, 10.0),
            (170.0, 0.0),
        ],
        &[],
    ));
    let graph = geographic_graph(&geographic);
    for longitude in [-181.0, 179.0, 539.0] {
        let mut ids = Vec::new();
        let _: Option<()> = graph.for_each_area_interior(XY::new(longitude, 5.0), |polygon| {
            ids.push(polygon);
            std::ops::ControlFlow::Continue(())
        });
        assert_eq!(ids, [0]);
    }
}

#[test]
fn resolved_node_locations_match_full_point_location() {
    let planar = collection(vec![
        Shape::Polygon(polygon(
            &[(0.0, 0.0), (2.0, 0.0), (2.0, 2.0), (0.0, 2.0), (0.0, 0.0)],
            &[],
        )),
        Shape::LineString(line(&[(-1.0, 1.0), (1.0, 1.0), (3.0, 1.0)])),
    ]);
    let geographic = collection(vec![
        Shape::Polygon(polygon(
            &[
                (170.0, 0.0),
                (-170.0, 0.0),
                (-170.0, 10.0),
                (170.0, 10.0),
                (170.0, 0.0),
            ],
            &[],
        )),
        Shape::LineString(line(&[(175.0, 5.0), (180.0, 5.0), (-175.0, 5.0)])),
    ]);
    for graph in [geographic_graph(&planar), geographic_graph(&geographic)] {
        for node in &graph.nodes {
            assert_eq!(node.location, graph.locate_point(node.point));
        }
    }
}

#[test]
fn overlapping_line_members_keep_shared_support_and_cancel_duplicate_boundary() {
    let collection = collection(vec![
        Shape::LineString(line(&[(0.0, 0.0), (2.0, 0.0)])),
        Shape::LineString(line(&[(0.0, 0.0), (2.0, 0.0)])),
        Shape::Point(point(4.0, 4.0)),
    ]);
    let graph = geographic_graph(&collection);
    assert_eq!(graph.effective_dimension, Some(Dimension::Curve));
    assert!(graph.line_boundary.is_empty());
    assert_eq!(graph.edges.len(), 1);
    assert_eq!(graph.edges[0].sources.len(), 2);
    assert!(graph.nodes.iter().any(|node| node.support.point));
}

#[test]
fn noding_preserves_one_source_record_on_each_atomic_piece() {
    let collection = collection(vec![
        Shape::LineString(line(&[(0.0, 0.0), (2.0, 2.0)])),
        Shape::LineString(line(&[(0.0, 2.0), (2.0, 0.0)])),
    ]);
    let graph = geographic_graph(&collection);
    assert_eq!(graph.edges.len(), 4);
    assert!(graph.edges.iter().all(|edge| edge.sources.len() == 1));
    assert!(!graph.has_line_boundary(XY::new(1.0, 1.0)));
}

fn assert_edge_endpoints_and_provenance(graph: &TopologyGraph) {
    for edge in &graph.edges {
        assert!(graph.node_for(edge.segment.start).is_some());
        assert!(graph.node_for(edge.segment.end).is_some());
        assert!(!edge.sources.is_empty());
        for &source in &edge.sources {
            let source = &graph.source_edges[source as usize];
            assert!(
                chart_segments(graph.frame, source.segment)
                    .into_iter()
                    .flatten()
                    .any(|chart| {
                        point_on_segment(edge.segment.start, chart.start, chart.end)
                            && point_on_segment(edge.segment.end, chart.start, chart.end)
                    })
            );
        }
    }
}

#[test]
fn noded_bundles_keep_endpoint_provenance() {
    let shape = collection(vec![
        Shape::LineString(line(&[(0.0, 0.0), (2.0, 2.0)])),
        Shape::LineString(line(&[(0.0, 2.0), (2.0, 0.0)])),
    ]);
    let graph = geographic_graph(&shape);
    assert_eq!(graph.edges.len(), 4);
    assert_edge_endpoints_and_provenance(&graph);
}

#[test]
fn geographic_seam_bundles_keep_endpoint_provenance() {
    let shape = Shape::Polygon(polygon(
        &[
            (170.0, 0.0),
            (-170.0, 0.0),
            (-170.0, 10.0),
            (170.0, 10.0),
            (170.0, 0.0),
        ],
        &[],
    ));
    let graph = geographic_graph(&shape);
    assert!(graph.edges.iter().any(|edge| {
        graph.equivalent(edge.segment.start, XY::new(180.0, 0.0))
            || graph.equivalent(edge.segment.end, XY::new(180.0, 0.0))
    }));
    assert_edge_endpoints_and_provenance(&graph);
}

#[test]
fn geographic_chart_keeps_exact_one_ulp_source_endpoint() {
    let one_ulp_shape = geographic_one_ulp_endpoint_fixture();
    let one_ulp_graph = geographic_graph(&one_ulp_shape);
    let endpoint = (-170.0_f64).next_down();
    // A rounded `start + delta` would leave a second node at the shared
    // source endpoint instead of selecting its exact canonical copy.
    assert_eq!(one_ulp_graph.nodes.len(), 6);
    let endpoint_node = one_ulp_graph
        .node_for(XY::new(endpoint, 0.0))
        .expect("one-ULP source endpoint must own a graph node");
    assert_eq!(
        one_ulp_graph.nodes[endpoint_node.index()].point,
        XY::new(endpoint, 0.0)
    );
    assert_edge_endpoints_and_provenance(&one_ulp_graph);
}

fn geographic_one_ulp_endpoint_fixture() -> Shape {
    let endpoint = (-170.0_f64).next_down();
    Shape::Polygon(polygon(
        &[
            (170.0, 0.0),
            (endpoint, 0.0),
            (endpoint, 10.0),
            (170.0, 10.0),
            (170.0, 0.0),
        ],
        &[],
    ))
}

fn quotient_dart(edge: usize, start: XY, end: XY) -> QuotientDart {
    QuotientDart {
        edge,
        from: TopologyNodeId(0),
        to: TopologyNodeId(1),
        segment: Segment { start, end },
    }
}

#[test]
fn quotient_fan_orders_adjacent_diagonals_geometrically() {
    let high = f64::from_bits(1.0_f64.to_bits() + 1);
    let upper = quotient_dart(0, XY::new(0.0, 0.0), XY::new(1.0, high));
    let lower = quotient_dart(1, XY::new(0.0, 0.0), XY::new(1.0, 1.0));
    assert_eq!(
        compare_quotient_rays(degrees(), XY::new(0.0, 0.0), &upper, &lower),
        std::cmp::Ordering::Greater,
    );
}

#[test]
fn pole_fan_orders_equal_longitude_quotient_directions_geometrically() {
    let high = f64::from_bits(1.0_f64.to_bits() + 1);
    let wider = quotient_dart(0, XY::new(0.0, 90.0), XY::new(high, 89.0));
    let narrower = quotient_dart(1, XY::new(0.0, 90.0), XY::new(1.0, 89.0));
    assert_eq!(
        compare_quotient_rays(degrees(), XY::new(0.0, 90.0), &wider, &narrower),
        std::cmp::Ordering::Greater,
    );
}

#[test]
fn typed_empty_has_no_effective_dimension() {
    let shape = Shape::Empty(GeometryKind::Polygon, CoordinateAxes::XYZM);
    let graph = geographic_graph(&shape);
    assert_eq!(graph.effective_dimension, None);
    assert!(graph.nodes.is_empty());
    assert!(graph.edges.is_empty());
}

#[test]
fn mixed_collection_keeps_highest_nonempty_dimension() {
    let collection = collection(vec![
        Shape::Empty(GeometryKind::Point, CoordinateAxes::XYZ),
        Shape::LineString(line(&[(0.0, 0.0), (1.0, 0.0)])),
        Shape::Polygon(polygon(
            &[(2.0, 0.0), (4.0, 0.0), (4.0, 2.0), (2.0, 2.0), (2.0, 0.0)],
            &[],
        )),
    ]);
    let graph = geographic_graph(&collection);
    assert_eq!(graph.effective_dimension, Some(Dimension::Surface));
}

#[test]
fn aggregate_face_support_does_not_bridge_nearby_components() {
    let shape = collection(vec![
        Shape::Polygon(polygon(
            &[
                (-1.01e-7, -1e-9),
                (-9.9e-8, -1e-9),
                (-9.9e-8, 1e-9),
                (-1.01e-7, 1e-9),
                (-1.01e-7, -1e-9),
            ],
            &[],
        )),
        Shape::Polygon(polygon(
            &[
                (9.9e-8, -1e-9),
                (1.01e-7, -1e-9),
                (1.01e-7, 1e-9),
                (9.9e-8, 1e-9),
                (9.9e-8, -1e-9),
            ],
            &[],
        )),
    ]);
    let graph = geographic_graph(&shape);
    assert_eq!(
        graph.locate_point(XY::new(0.0, 0.0)),
        TopologyLocation::Exterior
    );
}

#[test]
fn aggregate_face_support_absorbs_shared_edges_but_not_point_pinches() {
    let adjacent = collection(vec![
        Shape::Polygon(polygon(
            &[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)],
            &[],
        )),
        Shape::Polygon(polygon(
            &[(1.0, 0.0), (2.0, 0.0), (2.0, 1.0), (1.0, 1.0), (1.0, 0.0)],
            &[],
        )),
    ]);
    let graph = geographic_graph(&adjacent);
    assert_eq!(
        graph.locate_point(XY::new(1.0, 0.5)),
        TopologyLocation::Interior
    );
    assert_eq!(
        graph.locate_point(XY::new(1.0, 0.0)),
        TopologyLocation::Boundary
    );

    let left = Shape::Polygon(polygon(
        &[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)],
        &[],
    ));
    let right = Shape::Polygon(polygon(
        &[(1.0, 0.0), (2.0, 0.0), (2.0, 1.0), (1.0, 1.0), (1.0, 0.0)],
        &[],
    ));
    let duplicates = Shape::geometry_collection(vec![left.clone(), left.clone(), left, right])
        .expect("test collection is valid");
    assert_eq!(
        geographic_graph(&duplicates).locate_point(XY::new(1.0, 0.5)),
        TopologyLocation::Interior
    );

    let pinch = collection(vec![
        Shape::Polygon(polygon(
            &[(0.0, 0.0), (-2.0, -1.0), (-2.0, 1.0), (0.0, 0.0)],
            &[],
        )),
        Shape::Polygon(polygon(
            &[(0.0, 0.0), (2.0, 1.0), (2.0, -1.0), (0.0, 0.0)],
            &[],
        )),
    ]);
    let graph = geographic_graph(&pinch);
    assert_eq!(
        graph.locate_point(XY::new(0.0, 0.0)),
        TopologyLocation::Boundary
    );
}
