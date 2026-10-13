use super::*;
use crate::geographic::{AngularFactor, GeographicFrame, GeographicInputs};
use crate::geometry::Coordinates as _;
use crate::geometry::overlay::{Strictness, overlay_all};
use crate::geometry::topology::emission::sites::canonicalize_ring_vertices;
use crate::geometry::topology::emission::ring_roles::round_canonical_longitude;
use crate::geometry::topology::q_embedding::compare_q_ring_role;
use crate::geometry::topology::q_embedding::face_structure::{
    QFaceStructure, face_structure_mismatch_nodes, subgraph_chart,
};

fn build_q_plan(
    topology: &ExactGeographicTopology,
    operation: OverlayOp,
) -> Result<QEmissionPlan> {
    let mut work = crate::work::Unmetered;
    QEmissionPlan::build(topology, operation, &mut work)
}

fn build_q_boundary_plan(
    topology: &ExactGeographicTopology,
) -> Result<(QEmissionPlan, BoundarySupport)> {
    let mut work = crate::work::Unmetered;
    QEmissionPlan::build_boundary(topology, &mut work)
}

fn build_q_emission_unmetered<'topology, 'emission>(
    plan: &'emission QEmissionPlan,
    topology: &'topology ExactGeographicTopology,
    source_points: &'emission [Option<EmittedVertex<'topology>>],
    publication: &'emission PublicationInventory,
) -> Result<QEmbeddingEmission<'topology, 'emission>> {
    let mut work = crate::work::Unmetered;
    plan.embedding_emission(topology, source_points, publication, &mut work)
}

fn draw_q_plan_unmetered<'topology>(
    plan: &QEmissionPlan,
    topology: &'topology ExactGeographicTopology,
    coordinates: &QCoordinates,
    source_points: &[Option<EmittedVertex<'topology>>],
) -> Result<QEmissionDrawing<'topology>> {
    let mut work = crate::work::Unmetered;
    plan.drawing(topology, coordinates, source_points, &mut work)
}


fn exact_boundary(shape: &Shape) -> Shape {
    let frame = GeographicFrame::crs84();
    let inputs = GeographicInputs::try_from_shapes(frame, std::iter::once(shape))
        .expect("source admits");
    let topology =
        ExactGeographicTopology::from_views(frame, inputs.views().iter().copied()).unwrap();
    exact_geographic_boundary::<crate::crs::GeodesicMetric>(&topology, shape.axes(), None)
        .unwrap()
}

fn source_boundary_sink(
    topology: &ExactGeographicTopology,
    plan: &QEmissionPlan,
) -> Result<(Shape, ShapeLiteralLayout<QSourceImage>)> {
    let source_points = boundary_source_points(topology, SourceRowId(0))?;
    source_sink(topology, plan, &source_points)
}

fn source_sink<'topology>(
    topology: &'topology ExactGeographicTopology,
    plan: &QEmissionPlan,
    source_points: &[Option<EmittedVertex<'topology>>],
) -> Result<(Shape, ShapeLiteralLayout<QSourceImage>)> {
    let mut work = crate::work::Unmetered;
    let nearest = QCoordinates::build(topology)?;
    let drawing = plan.drawing(topology, &nearest, source_points, &mut work)?;
    let ring_roles = certify_nearest_ring_roles(topology, &drawing, &mut work)?;
    let ordinates = vec![NodeOrdinates::default(); topology.q.nodes.len()];
    let axes = CoordinateAxes::XY;
    let WrittenDrawingParts {
        points,
        lines,
        polygons,
        layout,
        diagnostic_lines: _,
    } = write_drawing(
        drawing,
        &ring_roles,
        &ordinates,
        None,
        None,
        None,
        axes,
        &mut work,
    )?;
    let (shape, order) =
        seal_output_parts(plan, points, lines, polygons, axes, axes, true, &mut work)?;
    let layout = {
        let order = order.ok_or_else(|| {
            GeometryErrorKind::Invalid("source boundary lost its literal row order".into())
        })?;
        layout.in_output_order(&order, &mut work)?
    };
    Ok((shape, layout))
}

fn bind_source_shape(
    topology: &ExactGeographicTopology,
    shape: Shape,
    layout: ShapeLiteralLayout<QSourceImage>,
) -> Result<LiteralCheck<QSourceImage>> {
    let mut work = crate::work::Unmetered;
    let ordinates = vec![NodeOrdinates::default(); topology.q.nodes.len()];
    super::super::q_embedding::bind_source_only(
        super::super::emission::SourceFrozenDrawing {
            buffers: FrozenLiteralBuffers::shapes(shape, layout),
            diagnostic_lines: Vec::new(),
        },
        topology,
        None,
        &ordinates,
        None,
        &[],
        &[],
        &[],
        &mut work,
    )
}

fn test_node_ordinates(topology: &ExactGeographicTopology) -> Result<Vec<NodeOrdinates>> {
    let mut work = crate::work::Unmetered;
    node_ordinates::<crate::crs::GeodesicMetric>(topology, CoordinateAxes::XY, None, &mut work)
}



fn resolve_q_embedding_xy(
    topology: &ExactGeographicTopology,
    plan: &QEmissionPlan,
    source_points: &[Option<EmittedVertex<'_>>],
    nearest: &QCoordinates,
    operation: &'static str,
) -> Result<CertifiedQEmission> {
    let metric_access: Option<&SourceArcMetricAccess<'_, '_, crate::crs::GeodesicMetric>> =
        None;
    let mut budget = EvaluationBudget::new(operation, 0)?;
    let mut preparation_work = EvaluationOnly::new(&mut budget);
    let mut expansion_budget = ExpansionBudget::new(operation, "computed edges");
    resolve_q_embedding(
        topology,
        plan,
        source_points,
        nearest,
        CoordinateAxes::XY,
        false,
        operation,
        metric_access,
        &mut expansion_budget,
        &mut preparation_work,
    )
}

fn certify_q_embedding_nearest_xy<'topology>(
    topology: &'topology ExactGeographicTopology,
    emission: &QEmbeddingEmission<'topology, '_>,
    nearest: &QCoordinates,
    live_keys: &[(EmittedNodeKey, QNodeId)],
    drawing: QEmissionDrawing<'topology>,
    operation: &'static str,
    budget: &mut EvaluationBudget,
) -> Result<NearestQCertificate> {
    let ordinates = test_node_ordinates(topology)?;
    let metric_access: Option<&SourceArcMetricAccess<'_, '_, crate::crs::GeodesicMetric>> =
        None;
    let certification = QEmbeddingCertification {
        topology,
        emission,
        nearest,
        ordinates: &ordinates,
        declared_axes: CoordinateAxes::XY,
        strict_axes: false,
        operation,
        metric_access,
    };
    certify_q_embedding_nearest(&certification, live_keys, drawing, budget)
}

fn q_union_one_source(shape: &Shape, frame: GeographicFrame) -> Shape {
    let inputs = GeographicInputs::try_from_shapes(frame, std::iter::once(shape))
        .expect("source admits");
    let topology =
        ExactGeographicTopology::from_views_as_set(frame, inputs.views().iter().copied())
            .expect("Q topology builds");
    exact_geographic_overlay::<crate::crs::GeodesicMetric>(
        &topology,
        OverlayOp::Union,
        shape.axes(),
        false,
        None,
    )
    .expect("Q source-only union emits")
}


fn emitted_single_point(shape: &Shape) -> Point {
    match shape {
        Shape::Point(point) => *point,
        Shape::MultiPoint(points) if points.len() == 1 => points.point_at(0),
        Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::Polygon(_)
        | Shape::MultiPolygon(_)
        | Shape::GeometryCollection(_)
        | Shape::Empty(..)
        | Shape::MultiPoint(_) => panic!("Q emitted one point, got {shape:?}"),
    }
}

fn collect_points(shape: &Shape, output: &mut Vec<Point>) {
    match shape {
        Shape::Point(point) => output.push(*point),
        Shape::MultiPoint(points) => output.extend(points.iter_coords()),
        Shape::GeometryCollection(geometries) => {
            for geometry in geometries {
                collect_points(geometry, output);
            }
        },
        Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::Polygon(_)
        | Shape::MultiPolygon(_)
        | Shape::Empty(..) => {},
    }
}





type TestPoint = (u64, u64);
type TestEdge = (TestPoint, TestPoint);

fn edge(left: (f64, f64), right: (f64, f64)) -> TestEdge {
    let left = (left.0.to_bits(), left.1.to_bits());
    let right = (right.0.to_bits(), right.1.to_bits());
    if left <= right {
        (left, right)
    } else {
        (right, left)
    }
}

fn line_edges(shape: &Shape) -> Vec<TestEdge> {
    let mut edges = Vec::new();
    let mut append = |line: &CoordSeq| {
        for [start, end] in line.segment_pairs() {
            edges.push(edge((start.x, start.y), (end.x, end.y)));
        }
    };
    match shape {
        Shape::LineString(line) => append(line.as_coords()),
        Shape::MultiLineString(lines) => {
            for line in lines {
                append(line.as_coords());
            }
        },
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::Polygon(_)
        | Shape::MultiPolygon(_)
        | Shape::GeometryCollection(_)
        | Shape::Empty(..) => panic!("area boundary emits linework, got {shape:?}"),
    }
    edges.sort_unstable();
    edges
}

fn q_ring_chart_points(
    frame: GeographicFrame,
    coordinates: &[(f64, f64)],
    repeated_start_nodes: &[usize],
) -> Result<Vec<Point>> {
    let last = coordinates.len().wrapping_sub(1);
    let ports = coordinates
        .iter()
        .enumerate()
        .map(|(index, &(x, y))| {
            let node = if index == 0 || index == last || repeated_start_nodes.contains(&index) {
                QNodeId(0)
            } else {
                QNodeId(u32::try_from(index).expect("fixed Q cycle fits its node IDs"))
            };
            super::super::q::QPort {
                id: QPortId(u32::try_from(index).expect("fixed Q cycle fits its port IDs")),
                point: super::super::exact::ExactChartPoint::from_rationals(
                    ExactRational::from_f64(x),
                    ExactRational::from_f64(y),
                ),
                node,
                cut: false,
            }
        })
        .collect::<Vec<_>>();
    let vertices = coordinates
        .iter()
        .zip(&ports)
        .map(|(&(longitude, latitude), port)| EmittedVertex {
            port,
            point: Point::new_raw_xy(longitude, latitude),
            literal_image: QSourceImage::Port(port.id),
        })
        .collect::<Vec<_>>();
    let mut chart_points = vertices
        .iter()
        .map(|vertex| vertex.point)
        .collect::<Vec<_>>();
    let mut work = crate::work::Unmetered;
    canonicalize_ring_vertices(frame, &vertices, &mut chart_points, &mut work)?;
    Ok(chart_points)
}

const B3_COUNTRIES_FIXTURE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/osm_countries_0_1.geojson"
);

fn b3_read_countries(codes: &[&str]) -> Vec<Shape> {
    let document: serde_json::Value = serde_json::from_slice(
        &std::fs::read(B3_COUNTRIES_FIXTURE).expect("read OSM country GeoJSON fixture"),
    )
    .expect("parse OSM country GeoJSON fixture");
    let features = document
        .get("features")
        .and_then(serde_json::Value::as_array)
        .expect("OSM country feature collection");
    codes
        .iter()
        .map(|code| {
            let feature = features
                .iter()
                .find(|feature| {
                    feature
                        .get("properties")
                        .and_then(|properties| properties.get("tags"))
                        .and_then(|tags| tags.get("ISO3166-1:alpha2"))
                        .and_then(serde_json::Value::as_str)
                        == Some(*code)
                })
                .unwrap_or_else(|| panic!("country fixture has no ISO alpha-2 code {code}"));
            crate::io::parse_geojson(feature.get("geometry").expect("country geometry"))
                .expect("parse country geometry")
        })
        .collect()
}

fn append_xy_bits(shape: &Shape, output: &mut Vec<(u64, u64)>) {
    let mut append = |point: Point| output.push((point.x.to_bits(), point.y.to_bits()));
    match shape {
        Shape::Point(point) => append(*point),
        Shape::MultiPoint(points) => points.iter().for_each(&mut append),
        Shape::LineString(line) => line.iter().for_each(&mut append),
        Shape::MultiLineString(lines) => {
            for line in lines.iter() {
                line.iter().for_each(&mut append);
            }
        },
        Shape::Polygon(polygon) => {
            for ring in std::iter::once(&polygon.shell).chain(polygon.holes.iter()) {
                ring.coords().iter().for_each(&mut append);
            }
        },
        Shape::MultiPolygon(polygons) => {
            for polygon in polygons.iter() {
                for ring in std::iter::once(&polygon.shell).chain(polygon.holes.iter()) {
                    ring.coords().iter().for_each(&mut append);
                }
            }
        },
        Shape::GeometryCollection(collection) => {
            for member in collection {
                append_xy_bits(member, output);
            }
        },
        Shape::Empty(..) => {},
    }
}

fn xy_bitstream(shape: &Shape) -> Vec<(u64, u64)> {
    let mut output = Vec::new();
    append_xy_bits(shape, &mut output);
    output
}

fn test_topology(shapes: &[Shape]) -> ExactGeographicTopology {
    let frame = GeographicFrame::crs84();
    let views = shapes.iter().map(|shape| {
        crate::geographic::ScalarGeographicView::try_new(frame, shape)
            .expect("test geometry is admitted")
    });
    ExactGeographicTopology::from_views(frame, views).expect("build exact test Q topology")
}




fn q_grid_parallelogram(u: (i32, i32), v: (i32, i32)) -> Result<Shape> {
    // 2^-46 is the spacing between adjacent binary64 values at 100.0.
    let unit = f64::from_bits(0x3D10_0000_0000_0000);
    let add =
        |left: i32, right: i32| left.checked_add(right).expect("Q grid offsets are bounded");
    let subtract =
        |left: i32, right: i32| left.checked_sub(right).expect("Q grid offsets are bounded");
    let negate = |value: i32| value.checked_neg().expect("Q grid offsets are bounded");
    let points = [
        (add(u.0, v.0), add(u.1, v.1)),
        (add(negate(u.0), v.0), add(negate(u.1), v.1)),
        (subtract(negate(u.0), v.0), subtract(negate(u.1), v.1)),
        (subtract(u.0, v.0), subtract(u.1, v.1)),
        (add(u.0, v.0), add(u.1, v.1)),
    ];
    let wkt = points
        .iter()
        .map(|&(x, y)| {
            format!(
                "{} {}",
                100.0 + f64::from(x) * unit,
                80.0 + f64::from(y) * unit
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    crate::io::parse_wkt(&format!("POLYGON (({wkt}))"))
}

fn next_q_grid_pair(random: &mut u64) -> Result<[Shape; 2]> {
    let mut draw = || {
        *random ^= *random << 13;
        *random ^= *random >> 7;
        *random ^= *random << 17;
        i32::try_from(*random % 61)
            .expect("bounded")
            .checked_sub(30)
            .expect("Q grid draw is bounded")
    };
    let determinant = |u: (i32, i32), v: (i32, i32)| {
        i64::from(u.0)
            .checked_mul(i64::from(v.1))
            .expect("Q grid determinant is bounded")
            .checked_sub(
                i64::from(u.1)
                    .checked_mul(i64::from(v.0))
                    .expect("Q grid determinant is bounded"),
            )
            .expect("Q grid determinant is bounded")
    };
    loop {
        let (a_u, a_v, b_u, b_v) = (
            (draw(), draw()),
            (draw(), draw()),
            (draw(), draw()),
            (draw(), draw()),
        );
        if determinant(a_u, a_v).abs() >= 100 && determinant(b_u, b_v).abs() >= 100 {
            return Ok([
                q_grid_parallelogram(a_u, a_v)?,
                q_grid_parallelogram(b_u, b_v)?,
            ]);
        }
    }
}

#[derive(Debug, Default)]
struct QGridSweepCounts {
    pairs: usize,
    nearest_certified: usize,
    search_paths: usize,
}

fn q_grid_certificate_sweep(pair_count: usize) -> Result<QGridSweepCounts> {
    let mut random = 0x81C4_4B29_35AF_108D_u64;
    let mut counts = QGridSweepCounts::default();
    for _ in 0..pair_count {
        let operands = next_q_grid_pair(&mut random)?;
        let topology = test_topology(&operands);
        let plan = build_q_plan(&topology, OverlayOp::Intersection)?;
        let source_points = source_point_coordinates(&topology)?;
        let nearest = QCoordinates::build(&topology)?;
        let nearest_nodes = nearest.node.clone();
        let resolved = resolve_q_embedding_xy(
            &topology,
            &plan,
            &source_points,
            &nearest,
            OverlayOp::Intersection.name(),
        )?;
        counts.pairs = counts
            .pairs
            .checked_add(1)
            .ok_or(ResourceError::SizeOverflow)?;
        match resolved.choice {
            QEmbeddingChoice::Nearest => {
                counts.nearest_certified = counts
                    .nearest_certified
                    .checked_add(1)
                    .ok_or(ResourceError::SizeOverflow)?;
                assert_eq!(
                    resolved.coordinates.node, nearest_nodes,
                    "nearest certificate pass must publish the nearest table"
                );
            },
            QEmbeddingChoice::Searched => {
                counts.search_paths = counts
                    .search_paths
                    .checked_add(1)
                    .ok_or(ResourceError::SizeOverflow)?;
            },
        }
    }
    Ok(counts)
}









fn b3_selected_subgraph(
    topology: &ExactGeographicTopology,
    plan: &QEmissionPlan,
) -> Result<Option<(Vec<usize>, Vec<QNodeId>)>> {
    if plan.whole_sphere {
        return Ok(None);
    }
    let mut work = crate::work::Unmetered;
    let mut spans = BTreeSet::new();
    match &plan.polygons {
        PolygonPlan::WholeSphere(_) => return Ok(None),
        PolygonPlan::Regions { rings, .. } => {
            for ring in rings {
                match *ring {
                    PlannedAreaRing::Cycle { cycle, .. } => {
                        let cycle = plan.cycles.get(cycle).ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "B3 polygon plan references an unknown cycle".into(),
                            )
                        })?;
                        for &dart_id in &cycle.darts {
                            let dart =
                                topology.q.darts.get(dart_id.0 as usize).ok_or_else(|| {
                                    GeometryErrorKind::Invalid(
                                        "B3 cycle references an unknown dart".into(),
                                    )
                                })?;
                            spans.insert(dart.span.0 as usize);
                        }
                    },
                    PlannedAreaRing::Source { ring, .. } => {
                        let source_ring =
                            topology.sources.rings.get(ring).ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "B3 polygon plan references an unknown source ring".into(),
                                )
                            })?;
                        for cycle in &plan.cycles {
                            if !cycle_belongs_to_source_ring(
                                topology,
                                cycle,
                                source_ring.id,
                                &mut work,
                            )? {
                                continue;
                            }
                            for &dart_id in &cycle.darts {
                                let dart = topology
                                    .q
                                    .darts
                                    .get(dart_id.0 as usize)
                                    .ok_or_else(|| {
                                        GeometryErrorKind::Invalid(
                                            "B3 source cycle references an unknown dart".into(),
                                        )
                                    })?;
                                spans.insert(dart.span.0 as usize);
                            }
                        }
                    },
                }
            }
        },
    }
    for line in &plan.line_paths {
        spans.extend(line.path.iter().map(|step| step.span));
    }
    let mut nodes = plan.point_nodes.iter().copied().collect::<BTreeSet<_>>();
    for &span_index in &spans {
        let span = topology.q.spans.get(span_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("B3 subgraph references an unknown span".into())
        })?;
        nodes.extend(span.nodes);
    }
    Ok(Some((
        spans.into_iter().collect(),
        nodes.into_iter().collect(),
    )))
}

fn b3_same_rotation_system(exact: &[QDartId], emitted: &[QDartId], offsets: &[usize]) -> bool {
    if exact.len() != emitted.len() || offsets.len() < 2 {
        return false;
    }
    for row in offsets.windows(2) {
        let Some((&start, &end)) = row.first().zip(row.get(1)) else {
            return false;
        };
        let Some(exact_row) = exact.get(start..end) else {
            return false;
        };
        let Some(emitted_row) = emitted.get(start..end) else {
            return false;
        };
        if exact_row.len() != emitted_row.len() {
            return false;
        }
        if exact_row.is_empty() {
            continue;
        }
        let Some(rotation) = emitted_row
            .iter()
            .position(|dart| exact_row.first() == Some(dart))
        else {
            return false;
        };
        for (index, dart) in exact_row.iter().enumerate() {
            let Some(offset) = rotation.checked_add(index) else {
                return false;
            };
            let Some(emitted_index) = offset.checked_rem(emitted_row.len()) else {
                return false;
            };
            if emitted_row.get(emitted_index) != Some(dart) {
                return false;
            }
        }
    }
    true
}

fn b3_face_structures_match(
    exact: &super::super::q::B3SubgraphFaceStructure,
    emitted: &super::super::q::B3SubgraphFaceStructure,
) -> bool {
    fn bind(
        exact: GlobalFaceId,
        emitted: GlobalFaceId,
        exact_to_emitted: &mut [Option<GlobalFaceId>],
        emitted_to_exact: &mut [Option<GlobalFaceId>],
    ) -> bool {
        let Some(exact_slot) = exact_to_emitted.get_mut(exact.0 as usize) else {
            return false;
        };
        let Some(emitted_slot) = emitted_to_exact.get_mut(emitted.0 as usize) else {
            return false;
        };
        if exact_slot.is_some_and(|previous| previous != emitted)
            || emitted_slot.is_some_and(|previous| previous != exact)
        {
            return false;
        }
        *exact_slot = Some(emitted);
        *emitted_slot = Some(exact);
        true
    }

    let exact_global = &exact.global;
    let emitted_global = &emitted.global;
    if exact_global.count != emitted_global.count
        || exact_global.span_global_faces.len() != emitted_global.span_global_faces.len()
        || exact_global.component_hosts.len() != emitted_global.component_hosts.len()
        || exact_global.cut_interval_faces.len() != emitted_global.cut_interval_faces.len()
        || exact_global.source_node_faces.len() != emitted_global.source_node_faces.len()
        || exact.outgoing_offsets != emitted.outgoing_offsets
        || !b3_same_rotation_system(&exact.outgoing, &emitted.outgoing, &exact.outgoing_offsets)
    {
        return false;
    }
    let mut exact_to_emitted = vec![None; exact_global.count];
    let mut emitted_to_exact = vec![None; emitted_global.count];
    if exact_global.count == 1
        && !bind(
            GlobalFaceId(0),
            GlobalFaceId(0),
            &mut exact_to_emitted,
            &mut emitted_to_exact,
        )
    {
        return false;
    }
    for (exact_faces, emitted_faces) in exact_global
        .span_global_faces
        .iter()
        .zip(&emitted_global.span_global_faces)
    {
        match (exact_faces, emitted_faces) {
            (None, None) => {},
            (Some(exact_faces), Some(emitted_faces)) => {
                for (exact_face, emitted_face) in exact_faces.iter().zip(emitted_faces) {
                    if !bind(
                        *exact_face,
                        *emitted_face,
                        &mut exact_to_emitted,
                        &mut emitted_to_exact,
                    ) {
                        return false;
                    }
                }
            },
            _ => return false,
        }
    }
    for (exact_face, emitted_face) in exact_global
        .component_hosts
        .iter()
        .zip(&emitted_global.component_hosts)
    {
        match (exact_face, emitted_face) {
            (None, None) => {},
            (Some(exact_face), Some(emitted_face)) => {
                if !bind(
                    *exact_face,
                    *emitted_face,
                    &mut exact_to_emitted,
                    &mut emitted_to_exact,
                ) {
                    return false;
                }
            },
            _ => return false,
        }
    }
    for (&exact_face, &emitted_face) in exact_global
        .cut_interval_faces
        .iter()
        .zip(&emitted_global.cut_interval_faces)
    {
        if !bind(
            exact_face,
            emitted_face,
            &mut exact_to_emitted,
            &mut emitted_to_exact,
        ) {
            return false;
        }
    }
    for (exact_face, emitted_face) in exact_global
        .source_node_faces
        .iter()
        .zip(&emitted_global.source_node_faces)
    {
        match (exact_face, emitted_face) {
            (None, None) => {},
            (Some(exact_face), Some(emitted_face)) => {
                if !bind(
                    *exact_face,
                    *emitted_face,
                    &mut exact_to_emitted,
                    &mut emitted_to_exact,
                ) {
                    return false;
                }
            },
            _ => return false,
        }
    }
    exact_to_emitted.iter().all(Option::is_some) && emitted_to_exact.iter().all(Option::is_some)
}

fn b3_source_endpoint_nodes(topology: &ExactGeographicTopology) -> BTreeSet<QNodeId> {
    let mut nodes = BTreeSet::new();
    for ports in &topology.q.source_edge_ports {
        for &port_id in ports {
            if let Some(port) = topology.q.ports.get(port_id.0 as usize) {
                nodes.insert(port.node);
            }
        }
    }
    for &port_id in &topology.q.source_point_ports {
        if let Some(port) = topology.q.ports.get(port_id.0 as usize) {
            nodes.insert(port.node);
        }
    }
    nodes
}

fn b3_constructed_collision_nodes(
    plan: &QEmissionPlan,
    topology: &ExactGeographicTopology,
    coordinates: &QCoordinates,
    source_points: &[Option<EmittedVertex<'_>>],
) -> Result<Vec<QNodeId>> {
    let live = plan.live_node_keys(topology, coordinates, source_points)?;
    let source_nodes = b3_source_endpoint_nodes(topology);
    let mut groups = BTreeMap::<EmittedNodeKey, BTreeSet<QNodeId>>::new();
    for (key, node) in live {
        groups.entry(key).or_default().insert(node);
    }
    Ok(groups
        .into_values()
        .filter(|nodes| nodes.len() > 1)
        .flat_map(|nodes| {
            nodes
                .into_iter()
                .filter(|node| !source_nodes.contains(node))
        })
        .collect())
}

fn b3_compare_assignment(
    topology: &ExactGeographicTopology,
    plan: &QEmissionPlan,
    source_points: &[Option<EmittedVertex<'_>>],
    coordinates: &QCoordinates,
    spans: &[usize],
    point_nodes: &[QNodeId],
    exact: &super::super::q::B3SubgraphFaceStructure,
) -> Result<bool> {
    let mut live = plan.live_node_keys(topology, coordinates, source_points)?;
    certify_node_injectivity(&mut live)?;
    let emitted = super::super::q::b3_subgraph_face_structure(
        topology.sources.frame,
        &topology.q,
        spans,
        point_nodes,
        Some((&coordinates.node, &coordinates.pole_port_longitude)),
    )?;
    Ok(b3_face_structures_match(exact, &emitted))
}
