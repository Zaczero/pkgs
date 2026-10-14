use super::super::exact::ExactSourceRing;
use super::cut_ports::build_cut_node_ports;
use super::plan::{
    PlannedAreaRing, PolygonPlan, QDiagnosticLine, QEmbeddingEmission, QEmissionDrawing,
    QLineOrigin,
};
use super::sites::EmittedVertex;
use super::source_publication::SourcePublication;
use super::source_reuse::cycle_belongs_to_source_ring;
use super::write::WrittenDrawingParts;
use super::{
    CoordinateAxes, EmissionNodeRegistry, ExactChartPoint, ExactGeographicTopology,
    FrozenLiteralBuffers, GeometryErrorKind, NativeMaterializedParts, NodeOrdinates,
    PublicationInventory, QEmissionPlan, QNodeId, QSourceImage, ResourceError, Result,
    SourceFrozenDrawing, index_recipe_walks, try_sort_by_with_work,
};
use crate::geometry::embedding_completion::literals::ShapeLiteralLayout;
use crate::geometry::{HasM, HasZ, Shape};
use crate::work::Work;

fn append_source_ring_live_nodes(
    topology: &ExactGeographicTopology,
    source_ring: &ExactSourceRing,
    live_nodes: &mut Vec<QNodeId>,
    work: &mut impl Work,
) -> Result<()> {
    for edge_index in source_ring.source_edges.clone() {
        work.charge_into_error(1)?;
        let edge =
            topology.sources.edges.get(edge_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q source ring edge is unknown".into())
            })?;
        let ports = topology
            .q
            .source_edge_ports
            .get(edge.id.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q source ring ports are unknown".into()))?;
        for &port_id in ports {
            work.charge_into_error(1)?;
            let port = topology.q.ports.get(port_id.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q source ring port is unknown".into())
            })?;
            live_nodes.push(port.node);
        }
    }
    Ok(())
}

impl super::plan::QEmissionPlan {
    pub(in crate::geometry::topology) fn embedding_emission<'topology, 'emission>(
        &'emission self,
        topology: &ExactGeographicTopology,
        source_points: &'emission [Option<EmittedVertex<'topology>>],
        publication: &'emission PublicationInventory,
        work: &mut impl Work,
    ) -> Result<QEmbeddingEmission<'topology, 'emission>> {
        let mut spans = Vec::new();
        let mut areal_spans = Vec::new();
        let mut live_nodes = Vec::new();
        let q = &topology.q;
        match &self.polygons {
            PolygonPlan::WholeSphere(source_polygons) => {
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
                    for ring_index in polygon.rings.clone() {
                        work.charge_into_error(1)?;
                        let ring = topology.sources.rings.get(ring_index).ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "Q whole-sphere plan references an unknown ring".into(),
                            )
                        })?;
                        append_source_ring_live_nodes(topology, ring, &mut live_nodes, work)?;
                    }
                }
            },
            PolygonPlan::Regions {
                rings: planned_rings,
                ..
            } => {
                for planned in planned_rings {
                    work.charge_into_error(1)?;
                    match *planned {
                        PlannedAreaRing::Source { ring, .. } => {
                            let source_ring =
                                topology.sources.rings.get(ring).ok_or_else(|| {
                                    GeometryErrorKind::Invalid(
                                        "Q area plan references an unknown source ring".into(),
                                    )
                                })?;
                            append_source_ring_live_nodes(
                                topology,
                                source_ring,
                                &mut live_nodes,
                                work,
                            )?;
                            for cycle in &self.cycles {
                                work.charge_into_error(1)?;
                                if !cycle_belongs_to_source_ring(
                                    topology,
                                    cycle,
                                    source_ring.id,
                                    work,
                                )? {
                                    continue;
                                }
                                for &dart_id in &cycle.darts {
                                    work.charge_into_error(1)?;
                                    let dart =
                                        q.darts.get(dart_id.0 as usize).ok_or_else(|| {
                                            GeometryErrorKind::Invalid(
                                                "Q source cycle references an unknown dart".into(),
                                            )
                                        })?;
                                    let span = dart.span.0 as usize;
                                    spans.push(span);
                                    areal_spans.push(span);
                                }
                            }
                        },
                        PlannedAreaRing::Cycle { cycle, .. } => {
                            let cycle = self.cycles.get(cycle).ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "Q area plan references an unknown cycle".into(),
                                )
                            })?;
                            for &dart_id in &cycle.darts {
                                work.charge_into_error(1)?;
                                let dart = q.darts.get(dart_id.0 as usize).ok_or_else(|| {
                                    GeometryErrorKind::Invalid(
                                        "Q cycle references an unknown dart".into(),
                                    )
                                })?;
                                let span = dart.span.0 as usize;
                                spans.push(span);
                                areal_spans.push(span);
                                for port_id in [dart.from_port, dart.to_port] {
                                    work.charge_into_error(1)?;
                                    let port =
                                        q.ports.get(port_id.0 as usize).ok_or_else(|| {
                                            GeometryErrorKind::Invalid(
                                                "Q cycle references an unknown port".into(),
                                            )
                                        })?;
                                    if !port.cut {
                                        live_nodes.push(port.node);
                                    }
                                }
                            }
                        },
                    }
                }
            },
        }
        for line in &self.line_paths {
            work.charge_into_error(1)?;
            if line.origin == QLineOrigin::ArealBoundary {
                for step in &line.path {
                    work.charge_into_error(1)?;
                    areal_spans.push(step.span);
                }
            }
            for step in &line.path {
                work.charge_into_error(1)?;
                spans.push(step.span);
                let span = q.spans.get(step.span).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q line plan references an unknown span".into())
                })?;
                for port_id in span.ports {
                    work.charge_into_error(1)?;
                    let port = q.ports.get(port_id.0 as usize).ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q line plan references an unknown port".into())
                    })?;
                    if !port.cut {
                        live_nodes.push(port.node);
                    }
                }
            }
        }
        for recipe in self.fragment_recipes.as_slice() {
            work.charge_into_error(1)?;
            for port_id in recipe.endpoints {
                work.charge_into_error(1)?;
                let port = q.ports.get(port_id.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "Q emission recipe endpoint references an unknown port".into(),
                    )
                })?;
                live_nodes.push(port.node);
            }
        }
        work.charge_into_error(
            u64::try_from(self.point_nodes.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        live_nodes.extend_from_slice(&self.point_nodes);
        try_sort_by_with_work(&mut spans, work, |left, right, _work| Ok(left.cmp(right)))?;
        work.charge_into_error(
            u64::try_from(spans.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        spans.dedup();
        try_sort_by_with_work(&mut areal_spans, work, |left, right, _work| {
            Ok(left.cmp(right))
        })?;
        work.charge_into_error(
            u64::try_from(areal_spans.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        areal_spans.dedup();
        try_sort_by_with_work(&mut live_nodes, work, |left, right, _work| {
            Ok(left.cmp(right))
        })?;
        work.charge_into_error(
            u64::try_from(live_nodes.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        live_nodes.dedup();

        let mut selected_ports = Vec::new();
        for &span_index in &spans {
            work.charge_into_error(1)?;
            let span = q.spans.get(span_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q embedding index references an unknown span".into())
            })?;
            for port_id in span.ports {
                work.charge_into_error(1)?;
                let port = q.ports.get(port_id.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "Q embedding index references an unknown port".into(),
                    )
                })?;
                selected_ports.push((port.node, port_id));
            }
        }
        for &node_id in &self.point_nodes {
            work.charge_into_error(1)?;
            let node = q.nodes.get(node_id.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q embedding index references an unknown point".into())
            })?;
            for &port_id in q.node_ports.get(node.ports.clone()).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q embedding point has an invalid port run".into())
            })? {
                work.charge_into_error(1)?;
                selected_ports.push((node_id, port_id));
            }
        }
        try_sort_by_with_work(&mut selected_ports, work, |left, right, _work| {
            Ok(left.cmp(right))
        })?;
        work.charge_into_error(
            u64::try_from(selected_ports.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        selected_ports.dedup();
        let mut selected_nodes = selected_ports
            .iter()
            .map(|(node, _)| *node)
            .collect::<Vec<_>>();
        try_sort_by_with_work(&mut selected_nodes, work, |left, right, _work| {
            Ok(left.cmp(right))
        })?;
        work.charge_into_error(
            u64::try_from(selected_nodes.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        selected_nodes.dedup();
        let cut_nodes = build_cut_node_ports(topology, &spans, &selected_ports, work)?;
        let source_nodes = EmissionNodeRegistry::with_sources(topology, &live_nodes, work)?;
        Ok(QEmbeddingEmission {
            plan: self,
            force_native: self.force_native,
            source_points,
            selected_regions: &self.selected_regions,
            selected_face_roots: &self.selected_face_roots,
            fragment_recipes: self.fragment_recipes.as_slice(),
            fragment_walks: self.fragment_recipes.walks(),
            publication,
            source_nodes,
            spans,
            areal_spans,
            point_nodes: &self.point_nodes,
            live_nodes,
            selected_ports,
            selected_nodes,
            cut_nodes,
        })
    }
}

use super::write::{seal_output_parts, write_drawing};

impl QEmissionPlan {
    pub(in crate::geometry::topology) fn write_source_drawing_parts(
        &self,
        drawing: QEmissionDrawing<'_>,
        ring_roles: &[bool],
        ordinates: &[NodeOrdinates],
        publication: Option<SourcePublication<'_>>,
        source_ports: Option<&[Option<ExactChartPoint>]>,
        declared_axes: CoordinateAxes,
        strict_axes: bool,
        operation: &'static str,
        work: &mut impl Work,
    ) -> Result<(
        Shape,
        ShapeLiteralLayout<QSourceImage>,
        Vec<QDiagnosticLine>,
    )> {
        let walks = publication
            .map(|_| index_recipe_walks(self.fragment_recipes.walks(), work))
            .transpose()?;
        if let Some(publication) = publication
            && (publication.points.len() != publication.inventory.samples.len()
                || publication.ordinates.len() != publication.inventory.samples.len()
                || publication.live_samples.len() != publication.inventory.samples.len())
        {
            return Err(GeometryErrorKind::Invalid(
                "source publication buffers do not match their sample rows".into(),
            )
            .into());
        }
        let source_axes = drawing.axes(declared_axes, ordinates, work)?;
        let mut has_z = source_axes.has_z();
        let mut has_m = source_axes.has_m();
        if let Some(publication) = publication {
            for (sample, &live) in publication.live_samples.iter().enumerate() {
                if !live {
                    continue;
                }
                work.charge_into_error(1)?;
                let sample_ordinates = publication.ordinates.get(sample).ok_or_else(|| {
                    GeometryErrorKind::Invalid("live publication sample has no ordinate row".into())
                })?;
                has_z &= sample_ordinates.z.value().is_some();
                has_m &= sample_ordinates.m.value().is_some();
            }
        }
        let axes = CoordinateAxes::new(HasZ(has_z), HasM(has_m));
        if strict_axes && axes != declared_axes {
            return Err(GeometryErrorKind::ordinate_dropped(operation));
        }
        let WrittenDrawingParts {
            points,
            lines,
            polygons,
            layout,
            diagnostic_lines,
        } = write_drawing(
            drawing,
            ring_roles,
            ordinates,
            publication,
            source_ports,
            walks.as_ref(),
            axes,
            work,
        )?;
        let (shape, order) = seal_output_parts(
            self,
            points,
            lines,
            polygons,
            axes,
            declared_axes,
            true,
            work,
        )?;
        let layout = {
            let order = order.ok_or_else(|| {
                GeometryErrorKind::Invalid("source shape lost its literal output ordering".into())
            })?;
            layout.in_output_order(&order, work)?
        };
        let layout = layout.with_structure(&shape);
        Ok((shape, layout, diagnostic_lines))
    }

    pub(in crate::geometry::topology) fn seal_source_drawing(
        &self,
        drawing: QEmissionDrawing<'_>,
        ring_roles: &[bool],
        ordinates: &[NodeOrdinates],
        publication: Option<SourcePublication<'_>>,
        source_ports: Option<&[Option<ExactChartPoint>]>,
        declared_axes: CoordinateAxes,
        strict_axes: bool,
        operation: &'static str,
        work: &mut impl Work,
    ) -> Result<SourceFrozenDrawing> {
        let (shape, layout, diagnostic_lines) = self.write_source_drawing_parts(
            drawing,
            ring_roles,
            ordinates,
            publication,
            source_ports,
            declared_axes,
            strict_axes,
            operation,
            work,
        )?;
        Ok(SourceFrozenDrawing {
            buffers: FrozenLiteralBuffers::shapes(shape, layout),
            diagnostic_lines,
        })
    }
    pub(in crate::geometry::topology) fn seal_native_parts(
        &self,
        parts: NativeMaterializedParts,
        declared_axes: CoordinateAxes,
        work: &mut impl Work,
    ) -> Result<super::super::emission::NativeFrozenDrawing> {
        parts.validate_literal_uses(work)?;
        let NativeMaterializedParts {
            axes,
            points,
            lines,
            polygons,
            literal_uses,
            diagnostic_lines,
        } = parts;
        let (shape, order) = seal_output_parts(
            self,
            points,
            lines,
            polygons,
            axes,
            declared_axes,
            true,
            work,
        )?;
        let order = order.ok_or_else(|| {
            GeometryErrorKind::Invalid("native shape lost its literal output ordering".into())
        })?;
        let literal_uses = literal_uses
            .in_output_order(&order, work)?
            .with_structure(&shape);
        Ok(super::super::emission::NativeFrozenDrawing {
            shape,
            literal_uses,
            diagnostic_lines,
        })
    }
}
