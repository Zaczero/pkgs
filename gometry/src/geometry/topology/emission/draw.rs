use super::super::exact::{SourceEdgeId, SourceEdgeKind, SourceRowId};
use super::super::q::{ExactGeographicTopology, QNodeId, QPortId};
use super::line_paths::boundary_owner_index;
use super::plan::{
    BoundaryCycle, DrawnAreaOrigin, DrawnAreaRing, DrawnPolygons, DrawnQLine, DrawnSourceRing,
    PlannedAreaRing, PlannedQLine, PolygonPlan, QEmissionDrawing, QLineOrigin, QLinePrimitive,
    SpanEndpoint,
};
use super::sites::{
    EmittedNodeKey, EmittedVertex, QCoordinates, emitted_node_key, for_each_q_cycle_vertex,
    source_ring_emitted_vertices,
};
use super::source_reuse::source_edge_at;
use super::{
    GeometryErrorKind, Point, QEmissionPlan, QSourceImage, RecipeWalkOwner, ResourceError, Result,
};
use crate::geographic::SeamIdentity;
use crate::work::Work;

pub(in crate::geometry::topology) fn emitted_point<'topology>(
    topology: &'topology ExactGeographicTopology,
    coordinates: &QCoordinates,
    span_index: usize,
    owner_index: usize,
    port: QPortId,
) -> Result<EmittedVertex<'topology>> {
    let q = &topology.q;
    let span = q
        .spans
        .get(span_index)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q emitted span is unknown".into()))?;
    span.owners
        .get(owner_index)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q emitted owner is unknown".into()))?;
    if !span.ports.contains(&port) {
        return Err(GeometryErrorKind::Invalid("Q port is not on its emitted span".into()).into());
    }
    let q_port = q
        .ports
        .get(port.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q emitted port is unknown".into()))?;
    Ok(EmittedVertex {
        port: q_port,
        point: coordinates.point(topology, port)?,
        literal_image: QSourceImage::Port(port),
    })
}
pub(in crate::geometry::topology) fn push_line_part<'topology>(
    output: &mut Vec<DrawnQLine<'topology>>,
    origin: QLineOrigin,
    recipe_walk: RecipeWalkOwner,
    source_row: Option<SourceRowId>,
    close: bool,
    vertices: &mut Vec<EmittedVertex<'topology>>,
) {
    if vertices.len() >= 2 {
        output.push(QLinePrimitive {
            origin,
            recipe_walk,
            source_row,
            close,
            path: std::mem::take(vertices),
        });
    } else {
        vertices.clear();
    }
}
pub(in crate::geometry::topology) fn draw_cycle_vertices<'topology>(
    topology: &'topology ExactGeographicTopology,
    coordinates: &QCoordinates,
    cycle: &BoundaryCycle,
    work: &mut impl Work,
) -> Result<Vec<EmittedVertex<'topology>>> {
    let mut vertices = Vec::with_capacity(cycle.darts.len().saturating_add(1));
    for_each_q_cycle_vertex(
        topology,
        &cycle.darts,
        |vertex, work| {
            let owner_index = boundary_owner_index(topology, vertex.span, work)?;
            vertices.push(emitted_point(
                topology,
                coordinates,
                vertex.span,
                owner_index,
                vertex.port,
            )?);
            Ok(())
        },
        work,
    )?;
    Ok(vertices)
}
pub(in crate::geometry::topology) fn draw_lines<'topology>(
    topology: &'topology ExactGeographicTopology,
    paths: &[PlannedQLine],
    coordinates: &QCoordinates,
    work: &mut impl Work,
) -> Result<Vec<DrawnQLine<'topology>>> {
    let q = &topology.q;
    let mut output = Vec::new();
    for line in paths {
        work.charge_into_error(1)?;
        let recipe_walk = line.recipe_walk;
        let source_row = line.source_row;
        let close = line.close;
        let path = &line.path;
        let mut start = None;
        for (index, step) in path.iter().enumerate() {
            work.charge_into_error(1)?;
            let span = q
                .spans
                .get(step.span)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q line span is unknown".into()))?;
            let [start_port, end_port] = span.ports;
            let from_port = match step.from_endpoint {
                SpanEndpoint::Start => start_port,
                SpanEndpoint::End => end_port,
            };
            let port = q.ports.get(from_port.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q line span has an unknown port".into())
            })?;
            if !port.cut {
                start = Some(index);
                break;
            }
        }
        let start = start.unwrap_or(0);
        let (before_start, from_start) = path.split_at(start);
        let mut points: Vec<EmittedVertex<'topology>> = Vec::new();
        let mut previous: Option<EmittedVertex<'topology>> = None;
        for step in from_start.iter().chain(before_start) {
            work.charge_into_error(1)?;
            let span = q
                .spans
                .get(step.span)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q line span is unknown".into()))?;
            let [start_port, end_port] = span.ports;
            let (from_port, to_port) = match step.from_endpoint {
                SpanEndpoint::Start => (start_port, end_port),
                SpanEndpoint::End => (end_port, start_port),
            };
            let from = emitted_point(topology, coordinates, step.span, step.owner, from_port)?;
            let to = emitted_point(topology, coordinates, step.span, step.owner, to_port)?;
            let from_cut = q
                .ports
                .get(from_port.0 as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q line port is unknown".into()))?
                .cut;
            let to_cut = q
                .ports
                .get(to_port.0 as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q line port is unknown".into()))?
                .cut;

            if !from_cut {
                if let Some(previous) = previous {
                    if previous.port.node != from.port.node {
                        return Err(GeometryErrorKind::Invalid(
                            "Q line path changed quotient nodes between spans".into(),
                        )
                        .into());
                    }
                    if previous.point != from.point {
                        let previous_key =
                            emitted_node_key(topology.sources.frame, previous.point.xy())?;
                        let from_key = emitted_node_key(topology.sources.frame, from.point.xy())?;
                        if previous_key != from_key {
                            return Err(GeometryErrorKind::ExactNodeCollision {
                                first: previous.port.node.0,
                                second: from.port.node.0,
                            }
                            .into());
                        }
                        let is_seam = matches!(
                            topology
                                .sources
                                .frame
                                .longitude()
                                .canonicalize(from.point.x)
                                .map_err(|error| {
                                    GeometryErrorKind::Invalid(error.to_string().into())
                                })?
                                .identity(topology.sources.frame.longitude()),
                            SeamIdentity::Seam
                        );
                        let is_pole = matches!(
                            from_key,
                            EmittedNodeKey::NorthPole | EmittedNodeKey::SouthPole
                        );
                        if is_seam || is_pole {
                            push_line_part(
                                &mut output,
                                line.origin,
                                recipe_walk,
                                source_row,
                                false,
                                &mut points,
                            );
                        } else if points
                            .last()
                            .is_some_and(|last: &EmittedVertex<'_>| last.point != previous.point)
                        {
                            return Err(GeometryErrorKind::Invalid(
                                "Q line endpoint aliases disagree away from a quotient singularity"
                                    .into(),
                            )
                            .into());
                        }
                    }
                }
                if points
                    .last()
                    .is_none_or(|last: &EmittedVertex<'_>| last.point != from.point)
                {
                    points.push(from);
                }
            }
            if !to_cut {
                points.push(to);
            }
            previous = Some(to);
        }
        if close && let Some(&first) = points.first() {
            points.push(first);
        }
        push_line_part(
            &mut output,
            line.origin,
            recipe_walk,
            source_row,
            close,
            &mut points,
        );
    }
    Ok(output)
}
pub(in crate::geometry::topology) fn draw_points<'topology>(
    topology: &'topology ExactGeographicTopology,
    nodes: &[QNodeId],
    source_points: &[Option<EmittedVertex<'topology>>],
    coordinates: &QCoordinates,
    work: &mut impl Work,
) -> Result<Vec<EmittedVertex<'topology>>> {
    let q = &topology.q;
    let mut output = Vec::with_capacity(nodes.len());
    for &node in nodes {
        work.charge_into_error(1)?;
        let vertex = if let Some(source) = source_points.get(node.0 as usize).copied().flatten() {
            source
        } else {
            let node_data = q
                .nodes
                .get(node.0 as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q point node is unknown".into()))?;
            let port_id = *q.node_ports.get(node_data.ports.start).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q point node has no first port".into())
            })?;
            let port = q
                .ports
                .get(port_id.0 as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q point port is unknown".into()))?;
            EmittedVertex {
                port,
                point: coordinates.point(topology, port_id)?,
                literal_image: QSourceImage::Port(port_id),
            }
        };
        output.push(vertex);
    }
    Ok(output)
}
#[cfg(test)]
pub(in crate::geometry::topology) fn source_point_coordinates(
    topology: &ExactGeographicTopology,
) -> Result<Vec<Option<EmittedVertex<'_>>>> {
    let mut work = crate::work::Unmetered;
    source_point_coordinates_with_work(topology, &mut work)
}
pub(in crate::geometry::topology) fn source_point_coordinates_with_work<'topology>(
    topology: &'topology ExactGeographicTopology,
    work: &mut impl Work,
) -> Result<Vec<Option<EmittedVertex<'topology>>>> {
    work.charge_into_error(
        u64::try_from(
            topology
                .sources
                .points
                .len()
                .checked_add(topology.q.nodes.len())
                .ok_or(ResourceError::SizeOverflow)?,
        )
        .map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut source_points = vec![None; topology.q.nodes.len()];
    for (source_index, (source, &port)) in topology
        .sources
        .points
        .iter()
        .zip(topology.q.source_point_ports.iter())
        .enumerate()
    {
        let q_port =
            topology.q.ports.get(port.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q source point port is unknown".into())
            })?;
        let slot = source_points
            .get_mut(q_port.node.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q source point node is unknown".into()))?;
        if slot.is_none() {
            *slot = Some(EmittedVertex {
                port: q_port,
                point: Point::new_raw_xy(source.point.x, source.point.y),
                literal_image: QSourceImage::SourcePoint(
                    u32::try_from(source_index).map_err(|_| ResourceError::SizeOverflow)?,
                ),
            });
        }
    }
    Ok(source_points)
}
#[cfg(test)]
pub(in crate::geometry::topology) fn boundary_source_points(
    topology: &ExactGeographicTopology,
    row: SourceRowId,
) -> Result<Vec<Option<EmittedVertex<'_>>>> {
    let mut work = crate::work::Unmetered;
    boundary_source_points_with_work(topology, row, &mut work)
}
pub(in crate::geometry::topology) fn boundary_source_points_with_work<'topology>(
    topology: &'topology ExactGeographicTopology,
    row: SourceRowId,
    work: &mut impl Work,
) -> Result<Vec<Option<EmittedVertex<'topology>>>> {
    let q = &topology.q;
    work.charge_into_error(u64::try_from(q.nodes.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut selected = Vec::with_capacity(q.nodes.len());
    selected.resize(q.nodes.len(), None::<(bool, SourceEdgeId, QPortId, Point)>);
    for (span_index, span) in q.spans.iter().enumerate() {
        work.charge_into_error(1)?;
        if span.nodes[0] == span.nodes[1] || !q.collapsed_rows_for_span(span_index)?.contains(&row)
        {
            continue;
        }
        for owner in &span.owners {
            work.charge_into_error(1)?;
            let edge = source_edge_at(topology, owner.source_edge)?;
            if edge.row != row
                || !matches!(edge.kind, SourceEdgeKind::Ring {
                    kind: crate::geometry::RingKind::Collapsed,
                    ..
                })
            {
                continue;
            }
            let [start_port, end_port] = q
                .source_edge_ports
                .get(edge.id.0 as usize)
                .copied()
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q collapsed edge has no source ports".into())
                })?;
            let (start_parameter, end_parameter) = if owner.reversed {
                (&owner.source_range.end, &owner.source_range.start)
            } else {
                (&owner.source_range.start, &owner.source_range.end)
            };
            let [start_node, end_node] = span.nodes;
            for (node, parameter) in [(start_node, start_parameter), (end_node, end_parameter)] {
                work.charge_into_error(1)?;
                // Prefer a stored source start at the node; its ring traversal spelling wins
                // over the canonical chart port and over an incoming edge's endpoint alias.
                let candidate = if parameter.is_zero() {
                    Some((false, edge.id, start_port, edge.source[0]))
                } else if parameter.is_one() {
                    Some((true, edge.id, end_port, edge.source[1]))
                } else {
                    None
                };
                let Some(candidate) = candidate else {
                    continue;
                };
                let stored = selected.get_mut(node.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q source endpoint has an unknown node".into())
                })?;
                if stored
                    .is_none_or(|previous| (candidate.0, candidate.1) < (previous.0, previous.1))
                {
                    *stored = Some(candidate);
                }
            }
        }
    }
    let mut output = Vec::with_capacity(selected.len());
    for entry in selected {
        work.charge_into_error(1)?;
        let Some((is_end, edge, port_id, point)) = entry else {
            output.push(None);
            continue;
        };
        let port = q.ports.get(port_id.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q collapsed edge endpoint port is unknown".into())
        })?;
        output.push(Some(EmittedVertex {
            port,
            point,
            literal_image: if is_end {
                QSourceImage::CollapsedEnd(edge)
            } else {
                QSourceImage::CollapsedStart(edge)
            },
        }));
    }
    Ok(output)
}
impl QEmissionPlan {
    #[cfg(test)]
    pub(in crate::geometry::topology) fn live_node_keys(
        &self,
        topology: &ExactGeographicTopology,
        coordinates: &QCoordinates,
        source_points: &[Option<EmittedVertex<'_>>],
    ) -> Result<Vec<(EmittedNodeKey, QNodeId)>> {
        let mut work = crate::work::Unmetered;
        let drawing = self.drawing(topology, coordinates, source_points, &mut work)?;
        let mut work = crate::work::Unmetered;
        drawing.live_node_keys(topology.sources.frame, &mut work)
    }
    pub(in crate::geometry::topology) fn drawing<'topology>(
        &self,
        topology: &'topology ExactGeographicTopology,
        coordinates: &QCoordinates,
        source_points: &[Option<EmittedVertex<'topology>>],
        work: &mut impl Work,
    ) -> Result<QEmissionDrawing<'topology>> {
        let polygons = match &self.polygons {
            PolygonPlan::WholeSphere(source_polygons) => {
                let mut drawn_polygons = Vec::with_capacity(source_polygons.len());
                for &polygon_index in source_polygons {
                    work.charge_into_error(1)?;
                    let polygon =
                        topology
                            .sources
                            .polygons
                            .get(polygon_index)
                            .ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "Q whole-sphere plan references an unknown polygon".into(),
                                )
                            })?;
                    let mut rings = Vec::with_capacity(polygon.rings.len());
                    for ring_index in polygon.rings.clone() {
                        work.charge_into_error(1)?;
                        let ring = topology.sources.rings.get(ring_index).ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "Q whole-sphere plan references an unknown ring".into(),
                            )
                        })?;
                        let vertices = source_ring_emitted_vertices(topology, ring, false, work)?;
                        rings.push(DrawnSourceRing {
                            hole: ring.hole,
                            recipe_walk: RecipeWalkOwner::SourceRing(ring_index),
                            vertices,
                        });
                    }
                    drawn_polygons.push(rings);
                }
                DrawnPolygons::WholeSphere(drawn_polygons)
            },
            PolygonPlan::Regions {
                rings,
                region_count,
            } => {
                let mut drawn_rings = Vec::with_capacity(rings.len());
                for planned in rings {
                    work.charge_into_error(1)?;
                    let (region, origin, recipe_walk, native_start_port, vertices) = match *planned
                    {
                        PlannedAreaRing::Source {
                            ring,
                            region,
                            reverse,
                            cycle_start,
                        } => {
                            let source_ring =
                                topology.sources.rings.get(ring).ok_or_else(|| {
                                    GeometryErrorKind::Invalid(
                                        "Q area plan references an unknown source ring".into(),
                                    )
                                })?;
                            let vertices =
                                source_ring_emitted_vertices(topology, source_ring, reverse, work)?;
                            (
                                region,
                                DrawnAreaOrigin::Source,
                                RecipeWalkOwner::SourceRing(ring),
                                cycle_start,
                                vertices,
                            )
                        },
                        PlannedAreaRing::Cycle {
                            cycle: cycle_index,
                            region,
                        } => {
                            let cycle = self.cycles.get(cycle_index).ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "Q area plan references an unknown cycle".into(),
                                )
                            })?;
                            let vertices = draw_cycle_vertices(topology, coordinates, cycle, work)?;
                            (
                                region,
                                DrawnAreaOrigin::Cycle,
                                RecipeWalkOwner::Cycle(cycle_index),
                                vertices.first().map(|vertex| vertex.port.id),
                                vertices,
                            )
                        },
                    };
                    drawn_rings.push(DrawnAreaRing {
                        region,
                        origin,
                        recipe_walk,
                        native_start_port,
                        vertices,
                    });
                }
                DrawnPolygons::Regions {
                    rings: drawn_rings,
                    region_count: *region_count,
                }
            },
        };
        let lines = draw_lines(topology, &self.line_paths, coordinates, work)?;
        let points = draw_points(
            topology,
            &self.point_nodes,
            source_points,
            coordinates,
            work,
        )?;
        Ok(QEmissionDrawing {
            polygons,
            lines,
            points,
        })
    }
}
