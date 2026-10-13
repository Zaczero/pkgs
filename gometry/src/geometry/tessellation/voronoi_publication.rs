use std::sync::Arc;

use crate::Frame;
use crate::array::StreamingShapes;
use crate::error::{ResourceError, Result};
use crate::geographic::{ChartPole, GeographicFrame};
use crate::geometry::embedding_completion::literals::{
    LiteralAddress, LiteralPolygon, LiteralRowLayout, LiteralSequence, ShapeLiteralLayout,
};
use crate::geometry::embedding_completion::storage::FrozenLiteralBuffers;
use crate::geometry::ordinates::{RootId, RootRange};
use crate::geometry::publication::{
    ChartCutSide, ChartRestriction, DirectedChartRestrictionUse, FinishedGeometry,
    ProducerDrawingReceipt, ProducerImageId, ProducerLiteral, ProducerSourceRootUse, StorageTarget,
    SyntheticChartVertex, finish_producer_drawing,
};
use crate::geometry::storage::{PackedRow, RowSelection, ShapeRow, ValidityMask};
use crate::geometry::tessellation::exact::ExactRational;
use crate::geometry::tessellation::{
    VoronoiChartCutSide, VoronoiDiagramReceipt, VoronoiEdgeColumn, VoronoiEdgeOrigin,
    VoronoiPolygonOutputs, VoronoiRingEdgeRole, VoronoiVertexOrigin, VoronoiVertexReceipt,
};
use crate::geometry::{ExpansionBudget, Polygon, Ring, Shape};
use crate::work::{Unmetered, Work};

pub(crate) struct VoronoiDrawingWriter {
    target: StorageTarget,
    output: StreamingShapes,
    row_map: Vec<Option<LiteralRowLayout<ProducerImageId>>>,
    literals: Vec<ProducerLiteral>,
    source_root_uses: Vec<ProducerSourceRootUse>,
    chart_restrictions: Vec<DirectedChartRestrictionUse>,
    group_offsets: Vec<i64>,
    source_root_offsets: Vec<usize>,
}

impl VoronoiDrawingWriter {
    pub(crate) fn new_array(clip: Option<&Polygon>) -> Result<Self> {
        Self::new(StorageTarget::Array, clip)
    }

    pub(crate) fn new_groups(clip: Option<&Polygon>) -> Result<Self> {
        Self::new(StorageTarget::Groups, clip)
    }

    fn new(target: StorageTarget, clip: Option<&Polygon>) -> Result<Self> {
        let source_root_offsets = root_offsets(clip)?;
        Ok(Self {
            target,
            output: StreamingShapes::new(),
            row_map: Vec::new(),
            literals: Vec::new(),
            source_root_uses: Vec::new(),
            chart_restrictions: Vec::new(),
            group_offsets: if target == StorageTarget::Groups {
                vec![0]
            } else {
                Vec::new()
            },
            source_root_offsets,
        })
    }

    pub(crate) fn push_polygon_outputs(
        &mut self,
        outputs: VoronoiPolygonOutputs,
        frame: GeographicFrame,
        budget: &mut ExpansionBudget,
    ) -> Result<()> {
        let receipt = outputs.receipt;
        for output in outputs.outputs {
            let shape =
                crate::geometry::publish_voronoi_polygon_shape(&output, &receipt, frame, budget)?;
            let Shape::Polygon(polygon) = &shape else {
                return Err(voronoi_publication_error(
                    "Voronoi polygon publisher returned a non-polygon",
                ));
            };
            let address = self.next_address()?;
            let rings = std::iter::once(&polygon.shell).chain(polygon.holes.iter());
            let mut literal_rings = Vec::with_capacity(polygon.holes.len().saturating_add(1));
            for (ring_index, ring) in rings.enumerate() {
                let roles = output
                    .edge_roles
                    .get(ring_index)
                    .ok_or_else(|| voronoi_publication_error("Voronoi ring roles are absent"))?;
                literal_rings.push(
                    self.polygon_ring_literals(ring, roles, &receipt, address, frame, budget)?,
                );
            }
            if literal_rings.len() != output.edge_roles.len() {
                return Err(voronoi_publication_error(
                    "Voronoi output has unused ring roles",
                ));
            }
            let mut rings = literal_rings.into_iter();
            let shell = rings
                .next()
                .ok_or_else(|| voronoi_publication_error("Voronoi polygon has no shell"))?;
            let holes = rings.collect();
            let layout = ShapeLiteralLayout {
                points: Vec::new(),
                lines: Vec::new(),
                polygons: vec![LiteralPolygon {
                    output: 0,
                    axes: polygon.axes(),
                    shell,
                    holes,
                }],
            };
            self.output.try_push(shape)?;
            self.row_map.push(Some(LiteralRowLayout {
                operation: address.operation,
                logical_row: address.logical_row,
                group_row: address.group_row,
                value_row: address.value_row,
                layout,
            }));
        }
        Ok(())
    }

    pub(crate) fn push_edge_column(
        &mut self,
        output: VoronoiEdgeColumn,
        frame: GeographicFrame,
        budget: &mut ExpansionBudget,
    ) -> Result<()> {
        let (coordinates, offsets, receipt) =
            crate::geometry::publish_voronoi_edge_column(output, Some(frame), budget)?;
        let separators = &receipt.separators;
        let offset_pairs = offsets.as_slice().array_windows::<2>();
        if offset_pairs.len() != separators.len() {
            return Err(voronoi_publication_error(
                "Voronoi separator receipts do not match packed line rows",
            ));
        }
        let row_selection = RowSelection::Identity;
        for (line, (&[start, end], separator)) in offset_pairs.zip(separators).enumerate() {
            let start = usize::try_from(start).map_err(|_| ResourceError::SizeOverflow)?;
            let end = usize::try_from(end).map_err(|_| ResourceError::SizeOverflow)?;
            let vertex_count = end
                .checked_sub(start)
                .ok_or(crate::geometry::GeometryErrorKind::MalformedCsrOffsets)?;
            if separator.vertices.len() != vertex_count {
                return Err(voronoi_publication_error(
                    "Voronoi separator vertices do not match packed line coordinates",
                ));
            }
            let address = self.next_address()?;
            let mut images = Vec::with_capacity(vertex_count);
            for (point_index, &vertex_id) in separator.vertices.iter().enumerate() {
                let point_offset = start
                    .checked_add(point_index)
                    .ok_or(ResourceError::SizeOverflow)?;
                let point = coordinates.point_at(point_offset);
                let image = self.push_literal(address, point, budget)?;
                let vertex = receipt_vertex(&receipt, vertex_id)?;
                let mut work = Unmetered;
                let producer_parameter = sequence_parameter(point_index, vertex_count, &mut work)?;
                self.push_vertex_source_roots(
                    vertex,
                    &receipt,
                    address,
                    image,
                    &producer_parameter,
                    budget,
                )?;
                images.push(image);
            }
            self.output
                .try_push_row(Some(ShapeRow::from_packed(PackedRow::line(
                    line,
                    &coordinates,
                    &offsets,
                    row_selection.as_deref(),
                ))))?;
            self.row_map.push(Some(LiteralRowLayout {
                operation: address.operation,
                logical_row: address.logical_row,
                group_row: address.group_row,
                value_row: address.value_row,
                layout: ShapeLiteralLayout {
                    points: Vec::new(),
                    lines: vec![LiteralSequence {
                        output: 0,
                        axes: coordinates.axes(),
                        images,
                    }],
                    polygons: Vec::new(),
                },
            }));
        }
        Ok(())
    }

    pub(crate) fn end_group(&mut self) -> Result<()> {
        if self.target != StorageTarget::Groups {
            return Err(voronoi_publication_error(
                "only a Groups writer has logical group boundaries",
            ));
        }
        let next = i64::try_from(self.row_map.len()).map_err(|_| ResourceError::SizeOverflow)?;
        self.group_offsets.push(next);
        Ok(())
    }

    pub(crate) fn finish_array(self, frame: Frame) -> Result<FinishedGeometry> {
        self.finish(StorageTarget::Array, frame, None)
    }

    pub(crate) fn finish_groups(
        self,
        frame: Frame,
        missing: Option<ValidityMask>,
    ) -> Result<FinishedGeometry> {
        self.finish(StorageTarget::Groups, frame, missing)
    }

    fn finish(
        self,
        target: StorageTarget,
        frame: Frame,
        missing: Option<ValidityMask>,
    ) -> Result<FinishedGeometry> {
        if self.target != target {
            return Err(voronoi_publication_error(
                "Voronoi literal writer target changed before final publication",
            ));
        }
        let Self {
            output,
            row_map,
            literals,
            source_root_uses,
            chart_restrictions,
            group_offsets,
            ..
        } = self;
        let (storage, value_missing, layout) = output.finish_parts()?;
        let mut work = Unmetered;
        let buffers = match target {
            StorageTarget::Array => {
                if missing.is_some() || !group_offsets.is_empty() {
                    return Err(voronoi_publication_error(
                        "array writer retained Groups-only storage state",
                    ));
                }
                FrozenLiteralBuffers::rows(
                    Arc::new(storage),
                    value_missing,
                    layout,
                    row_map,
                    &mut work,
                )?
            },
            StorageTarget::Groups => {
                let group_count = group_offsets
                    .len()
                    .checked_sub(1)
                    .ok_or_else(|| voronoi_publication_error("Voronoi groups have no offsets"))?;
                if missing
                    .as_ref()
                    .is_some_and(|mask| mask.len() != group_count)
                {
                    return Err(voronoi_publication_error(
                        "Voronoi Groups offsets do not match source rows",
                    ));
                }
                FrozenLiteralBuffers::groups(
                    Arc::new(storage),
                    value_missing,
                    layout,
                    row_map,
                    group_offsets,
                    missing,
                    &mut work,
                )?
            },
            StorageTarget::Scalar => {
                return Err(voronoi_publication_error(
                    "Voronoi public output requires ArrayRows or Groups storage",
                ));
            },
        };
        let receipt =
            ProducerDrawingReceipt::new(buffers, literals, source_root_uses, chart_restrictions);
        finish_producer_drawing(receipt, frame, target)
    }

    fn polygon_ring_literals(
        &mut self,
        ring: &Ring,
        roles: &[VoronoiRingEdgeRole],
        receipt: &VoronoiDiagramReceipt,
        address: LiteralAddress,
        frame: GeographicFrame,
        budget: &mut ExpansionBudget,
    ) -> Result<LiteralSequence<ProducerImageId>> {
        let edge_count = ring
            .len()
            .checked_sub(1)
            .ok_or(crate::geometry::GeometryErrorKind::RingTooShort(0))?;
        if roles.len() != edge_count {
            return Err(voronoi_publication_error(
                "Voronoi polygon coordinates do not match edge roles",
            ));
        }
        let mut vertices = Vec::with_capacity(ring.len());
        let mut first_vertex = None;
        let mut previous_end = None;
        for role in roles {
            let edge_id = role
                .edge
                .ok_or_else(|| voronoi_publication_error("Voronoi polygon edge has no identity"))?;
            let edge = receipt
                .edges
                .get(edge_id as usize)
                .filter(|edge| edge.id == edge_id)
                .ok_or_else(|| voronoi_publication_error("Voronoi polygon edge is unknown"))?;
            let [first, second] = edge.endpoints;
            let (start, end) = if role.forward {
                (first, second)
            } else {
                (second, first)
            };
            if previous_end.is_some_and(|previous| previous != start) {
                return Err(voronoi_publication_error(
                    "Voronoi polygon directed edges do not form one ring",
                ));
            }
            first_vertex.get_or_insert(start);
            vertices.push(start);
            previous_end = Some(end);
        }
        let Some(first_vertex) = first_vertex else {
            return Err(voronoi_publication_error("Voronoi polygon ring is empty"));
        };
        if vertices.len() != edge_count || previous_end != Some(first_vertex) {
            return Err(voronoi_publication_error(
                "Voronoi polygon edge identities do not close their ring",
            ));
        }
        vertices.push(first_vertex);

        let mut images = Vec::with_capacity(ring.len());
        for point in ring {
            let image = self.push_literal(address, point, budget)?;
            images.push(image);
        }
        for (edge_index, role) in roles.iter().enumerate() {
            let start_id = *vertices
                .get(edge_index)
                .ok_or_else(|| voronoi_publication_error("Voronoi edge start is absent"))?;
            let end_index = edge_index
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?;
            let end_id = *vertices
                .get(end_index)
                .ok_or_else(|| voronoi_publication_error("Voronoi edge end is absent"))?;
            let start_image = *images
                .get(edge_index)
                .ok_or_else(|| voronoi_publication_error("Voronoi start literal is absent"))?;
            let end_image = *images
                .get(end_index)
                .ok_or_else(|| voronoi_publication_error("Voronoi end literal is absent"))?;
            self.push_edge_source_roots(role, receipt, address, start_image, end_image, budget)?;
            let start_vertex = receipt_vertex(receipt, start_id)?;
            let end_vertex = receipt_vertex(receipt, end_id)?;
            let start_point = ring.coords().point_at(edge_index);
            let end_point = ring.coords().point_at(end_index);
            if let Some(restriction) = chart_restriction_use(
                role,
                start_vertex,
                end_vertex,
                start_point,
                end_point,
                receipt,
                frame,
            )? {
                self.chart_restrictions.push(restriction);
            }
        }
        Ok(LiteralSequence {
            output: 0,
            axes: ring.coords().axes(),
            images,
        })
    }

    fn push_edge_source_roots(
        &mut self,
        role: &VoronoiRingEdgeRole,
        receipt: &VoronoiDiagramReceipt,
        address: LiteralAddress,
        start: ProducerImageId,
        end: ProducerImageId,
        budget: &mut ExpansionBudget,
    ) -> Result<()> {
        for source in &role.source_uses {
            if source.source_operand == Some(0) {
                continue;
            }
            let root = self.source_root_for_clip_segment(
                receipt,
                source.source_segment,
                source.source_ring,
                source.source_edge,
            )?;
            budget.add(2)?;
            let mut work = Unmetered;
            let start_parameter = source.start_parameter.try_clone_with_work(&mut work)?;
            let end_parameter = source.end_parameter.try_clone_with_work(&mut work)?;
            self.source_root_uses.push(ProducerSourceRootUse {
                address,
                image: start,
                range: RootRange::point_with_work(
                    root,
                    start_parameter,
                    ExactRational::zero(),
                    &mut work,
                )?,
            });
            self.source_root_uses.push(ProducerSourceRootUse {
                address,
                image: end,
                range: RootRange::point_with_work(
                    root,
                    end_parameter,
                    ExactRational::one(),
                    &mut work,
                )?,
            });
        }
        Ok(())
    }

    fn push_vertex_source_roots(
        &mut self,
        vertex: &VoronoiVertexReceipt,
        receipt: &VoronoiDiagramReceipt,
        address: LiteralAddress,
        image: ProducerImageId,
        producer_parameter: &ExactRational,
        budget: &mut ExpansionBudget,
    ) -> Result<()> {
        for origin in &vertex.origins {
            let VoronoiVertexOrigin::SourceClip {
                segment,
                parameter,
                source_ring: Some(ring),
                source_edge: Some(edge),
            } = origin
            else {
                continue;
            };
            let ring = u32::try_from(*ring).map_err(|_| ResourceError::SizeOverflow)?;
            let edge = u32::try_from(*edge).map_err(|_| ResourceError::SizeOverflow)?;
            let root =
                self.source_root_for_clip_segment(receipt, *segment, Some(ring), Some(edge))?;
            budget.add(1)?;
            let mut work = Unmetered;
            let source_parameter = parameter.try_clone_with_work(&mut work)?;
            let producer_parameter = producer_parameter.try_clone_with_work(&mut work)?;
            self.source_root_uses.push(ProducerSourceRootUse {
                address,
                image,
                range: RootRange::point_with_work(
                    root,
                    source_parameter,
                    producer_parameter,
                    &mut work,
                )?,
            });
        }
        Ok(())
    }

    fn source_root_for_clip_segment(
        &self,
        receipt: &VoronoiDiagramReceipt,
        segment: u32,
        ring: Option<u32>,
        edge: Option<u32>,
    ) -> Result<RootId> {
        let (Some(ring), Some(edge)) = (ring, edge) else {
            return Err(voronoi_publication_error(
                "Voronoi source-root use has no source ring or edge",
            ));
        };
        let segment_index = usize::try_from(segment).map_err(|_| ResourceError::SizeOverflow)?;
        let clip_segment = receipt
            .clip_segments
            .get(segment_index)
            .filter(|clip_segment| clip_segment.id == segment)
            .ok_or_else(|| voronoi_publication_error("Voronoi source segment is unknown"))?;
        let ring_index = usize::try_from(ring).map_err(|_| ResourceError::SizeOverflow)?;
        let edge_index = usize::try_from(edge).map_err(|_| ResourceError::SizeOverflow)?;
        if !clip_segment
            .source_roots
            .iter()
            .any(|root| root.ring == ring_index && root.edge == edge_index)
        {
            return Err(voronoi_publication_error(
                "Voronoi source segment does not retain its source root",
            ));
        }
        self.source_root_id(Some(ring), Some(edge))
    }

    fn source_root_id(&self, ring: Option<u32>, edge: Option<u32>) -> Result<RootId> {
        let (Some(ring), Some(edge)) = (ring, edge) else {
            return Err(voronoi_publication_error(
                "Voronoi source-root use has no source ring or edge",
            ));
        };
        let ring = usize::try_from(ring).map_err(|_| ResourceError::SizeOverflow)?;
        let edge = usize::try_from(edge).map_err(|_| ResourceError::SizeOverflow)?;
        let start = *self
            .source_root_offsets
            .get(ring)
            .ok_or_else(|| voronoi_publication_error("Voronoi source ring is unknown"))?;
        let end = *self
            .source_root_offsets
            .get(ring.checked_add(1).ok_or(ResourceError::SizeOverflow)?)
            .ok_or_else(|| voronoi_publication_error("Voronoi source ring is unknown"))?;
        let edge_count = end
            .checked_sub(start)
            .ok_or_else(|| voronoi_publication_error("Voronoi source root offsets are invalid"))?;
        if edge >= edge_count {
            return Err(voronoi_publication_error("Voronoi source edge is unknown"));
        }
        let index = start.checked_add(edge).ok_or(ResourceError::SizeOverflow)?;
        Ok(RootId::from_index(
            u32::try_from(index).map_err(|_| ResourceError::SizeOverflow)?,
        ))
    }

    fn push_literal(
        &mut self,
        address: LiteralAddress,
        point: crate::Point,
        budget: &mut ExpansionBudget,
    ) -> Result<ProducerImageId> {
        budget.add(1)?;
        let image = ProducerImageId::new(self.literals.len());
        self.literals
            .push(ProducerLiteral::from_point(address, image, point));
        Ok(image)
    }

    fn next_address(&self) -> Result<LiteralAddress> {
        let (logical_row, group_row, value_row) = match self.target {
            StorageTarget::Array => (self.row_map.len(), None, None),
            StorageTarget::Groups => {
                let logical_row = self
                    .group_offsets
                    .len()
                    .checked_sub(1)
                    .ok_or_else(|| voronoi_publication_error("Voronoi group has no start"))?;
                (logical_row, Some(logical_row), Some(self.row_map.len()))
            },
            StorageTarget::Scalar => {
                return Err(voronoi_publication_error(
                    "Voronoi output writer does not publish scalar storage",
                ));
            },
        };
        Ok(LiteralAddress {
            operation: 0,
            logical_row,
            group_row,
            value_row,
            output: 0,
        })
    }
}

fn root_offsets(clip: Option<&Polygon>) -> Result<Vec<usize>> {
    let mut offsets = vec![0_usize];
    if let Some(clip) = clip {
        for ring in std::iter::once(&clip.shell).chain(clip.holes.iter()) {
            let edge_count = ring
                .len()
                .checked_sub(1)
                .ok_or(crate::geometry::GeometryErrorKind::RingTooShort(0))?;
            let next = offsets
                .last()
                .copied()
                .unwrap_or(0)
                .checked_add(edge_count)
                .ok_or(ResourceError::SizeOverflow)?;
            offsets.push(next);
        }
    }
    Ok(offsets)
}

fn receipt_vertex(receipt: &VoronoiDiagramReceipt, id: u32) -> Result<&VoronoiVertexReceipt> {
    let index = usize::try_from(id).map_err(|_| ResourceError::SizeOverflow)?;
    receipt
        .vertices
        .get(index)
        .filter(|vertex| vertex.id == id)
        .ok_or_else(|| voronoi_publication_error("Voronoi output vertex is unknown"))
}

fn chart_cut_side(
    vertex: &VoronoiVertexReceipt,
    receipt: &VoronoiDiagramReceipt,
) -> Result<Option<ChartCutSide>> {
    let mut side = None;
    for origin in &vertex.origins {
        let VoronoiVertexOrigin::ChartCut { side: source_side } = origin else {
            continue;
        };
        let cuts = receipt.selected_chart_cuts.ok_or_else(|| {
            voronoi_publication_error("Voronoi chart-cut origin has no selected cuts")
        })?;
        let [lower, upper] = cuts;
        let site = match source_side {
            VoronoiChartCutSide::Lower => lower,
            VoronoiChartCutSide::Upper => upper,
        };
        let side_for_cut = match site.periods() {
            0 => ChartCutSide::West,
            1 => ChartCutSide::East,
            _ => {
                return Err(voronoi_publication_error(
                    "Voronoi cut lift is not a chart boundary",
                ));
            },
        };
        if side.is_some_and(|previous| previous != side_for_cut) {
            return Err(voronoi_publication_error(
                "Voronoi vertex has conflicting chart-cut origins",
            ));
        }
        side = Some(side_for_cut);
    }
    Ok(side)
}

fn chart_pole(vertex: &VoronoiVertexReceipt, frame: GeographicFrame) -> Result<Option<ChartPole>> {
    let mut pole = None;
    for origin in &vertex.origins {
        let VoronoiVertexOrigin::Pole { latitude } = origin else {
            continue;
        };
        let source_pole = if latitude.to_bits() == frame.pole().to_bits() {
            ChartPole::North
        } else if latitude.to_bits() == (-frame.pole()).to_bits() {
            ChartPole::South
        } else {
            return Err(voronoi_publication_error(
                "Voronoi pole origin is outside its geographic frame",
            ));
        };
        if pole.is_some_and(|previous| previous != source_pole) {
            return Err(voronoi_publication_error(
                "Voronoi vertex has conflicting pole origins",
            ));
        }
        pole = Some(source_pole);
    }
    Ok(pole)
}

fn chart_restriction_use(
    role: &VoronoiRingEdgeRole,
    start: &VoronoiVertexReceipt,
    end: &VoronoiVertexReceipt,
    start_point: crate::Point,
    end_point: crate::Point,
    receipt: &VoronoiDiagramReceipt,
    frame: GeographicFrame,
) -> Result<Option<DirectedChartRestrictionUse>> {
    if role
        .origins
        .iter()
        .any(|origin| matches!(origin, VoronoiEdgeOrigin::SourceClip { .. }))
    {
        return Ok(None);
    }
    let start_pole = chart_pole(start, frame)?;
    let end_pole = chart_pole(end, frame)?;
    if let (Some(start_pole), Some(end_pole)) = (start_pole, end_pole)
        && start_pole == end_pole
    {
        return Ok(Some(DirectedChartRestrictionUse {
            restriction: ChartRestriction::PolarCap { pole: start_pole },
            start: SyntheticChartVertex::Pole {
                node: start.id,
                pole: start_pole,
                native_longitude_bits: start_point.x.to_bits(),
            },
            end: SyntheticChartVertex::Pole {
                node: end.id,
                pole: end_pole,
                native_longitude_bits: end_point.x.to_bits(),
            },
        }));
    }
    let start_cut = chart_cut_side(start, receipt)?;
    let end_cut = chart_cut_side(end, receipt)?;
    if let (Some(start_side), Some(end_side)) = (start_cut, end_cut)
        && start_side == end_side
    {
        return Ok(Some(DirectedChartRestrictionUse {
            restriction: ChartRestriction::CutChord,
            start: SyntheticChartVertex::Cut {
                node: start.id,
                side: start_side,
            },
            end: SyntheticChartVertex::Cut {
                node: end.id,
                side: end_side,
            },
        }));
    }
    Ok(None)
}

fn sequence_parameter(index: usize, count: usize, work: &mut impl Work) -> Result<ExactRational> {
    let denominator = count
        .checked_sub(1)
        .ok_or_else(|| voronoi_publication_error("Voronoi line has fewer than two vertices"))?;
    let numerator = ExactRational::from_f64(f64::from(
        u32::try_from(index).map_err(|_| ResourceError::SizeOverflow)?,
    ));
    let denominator = ExactRational::from_f64(f64::from(
        u32::try_from(denominator).map_err(|_| ResourceError::SizeOverflow)?,
    ));
    numerator.try_quotient_with_work(&denominator, work)
}

fn voronoi_publication_error(message: &'static str) -> crate::error::Error {
    crate::geometry::GeometryErrorKind::Invalid(message.into()).into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorKind;
    use crate::geometry::tessellation::VoronoiProducer;
    use crate::geometry::{GeometryErrorKind, VoronoiBoundary};

    fn vertex(id: u32, origins: Vec<VoronoiVertexOrigin>) -> VoronoiVertexReceipt {
        VoronoiVertexReceipt {
            id,
            exact_xy: [ExactRational::zero(), ExactRational::zero()],
            origins,
            assignment: None,
        }
    }

    fn edge_role(origins: Vec<VoronoiEdgeOrigin>) -> VoronoiRingEdgeRole {
        VoronoiRingEdgeRole {
            edge: None,
            forward: true,
            origins,
            source_uses: Vec::new(),
        }
    }

    fn chart_receipt() -> (GeographicFrame, VoronoiDiagramReceipt) {
        let frame = GeographicFrame::crs84();
        let chart = crate::geographic::PointSelectionChart::try_from_points(frame, &[
            crate::Point::new_raw_xy(40.0, 5.0),
            crate::Point::new_raw_xy(60.0, 10.0),
        ])
        .unwrap()
        .expect("the selected site interval has a chart");
        let mut receipt = VoronoiDiagramReceipt::empty(VoronoiProducer::Exact);
        receipt.selected_chart_cuts = Some(chart.cut_longitudes().unwrap());
        (frame, receipt)
    }

    fn small_voronoi_writer() -> VoronoiDrawingWriter {
        let source = Shape::MultiPoint(
            vec![
                crate::Point::new_raw_xy(-1.0, -1.0),
                crate::Point::new_raw_xy(1.0, -1.0),
                crate::Point::new_raw_xy(1.0, 1.0),
                crate::Point::new_raw_xy(-1.0, 1.0),
            ]
            .into(),
        );
        let frame = GeographicFrame::crs84();
        let mut budget = ExpansionBudget::new("voronoi_cells", "mesh receipt omission");
        let outputs = source
            .voronoi_polygon_outputs_in_frame_budgeted(
                0.0,
                VoronoiBoundary::Padded,
                Some(frame),
                &mut budget,
            )
            .unwrap();
        let mut writer = VoronoiDrawingWriter::new_array(None).unwrap();
        writer
            .push_polygon_outputs(outputs, frame, &mut budget)
            .unwrap();
        writer
    }

    #[test]
    fn voronoi_receipt_omission_is_rejected_by_final_readback() {
        let (control_values, _) = small_voronoi_writer()
            .finish_array(Frame::None)
            .unwrap()
            .into_array()
            .unwrap();
        assert_eq!(control_values.len(), 4);

        let mut writer = small_voronoi_writer();
        let omitted = writer.literals.pop().expect("Voronoi output has literals");
        writer.source_root_uses.retain(|source_use| {
            source_use.address != omitted.address || source_use.image != omitted.image
        });
        assert!(
            !writer.source_root_uses.iter().any(|source_use| {
                source_use.address == omitted.address && source_use.image == omitted.image
            }),
            "the omitted literal has no retained source-root use",
        );

        match writer.finish_array(Frame::None) {
            Err(error) => assert!(matches!(
                error.kind(),
                ErrorKind::Geometry(GeometryErrorKind::Invalid(_))
            )),
            Ok(_) => panic!("the shared readback rejects a missing Voronoi literal"),
        }
    }

    #[test]
    fn selected_cut_pair_maps_to_directed_west_and_east_uses() {
        let (frame, receipt) = chart_receipt();
        let west_start = vertex(17, vec![VoronoiVertexOrigin::ChartCut {
            side: VoronoiChartCutSide::Lower,
        }]);
        let west_end = vertex(18, vec![VoronoiVertexOrigin::ChartCut {
            side: VoronoiChartCutSide::Lower,
        }]);
        let west = chart_restriction_use(
            &edge_role(Vec::new()),
            &west_start,
            &west_end,
            crate::Point::new_raw_xy(40.0, 2.0),
            crate::Point::new_raw_xy(40.0, 8.0),
            &receipt,
            frame,
        )
        .unwrap()
        .expect("same-side lower chart cuts form a directed chord");
        assert_eq!(west.restriction, ChartRestriction::CutChord);
        assert_eq!(west.start, SyntheticChartVertex::Cut {
            node: 17,
            side: ChartCutSide::West,
        });
        assert_eq!(west.end, SyntheticChartVertex::Cut {
            node: 18,
            side: ChartCutSide::West,
        });

        let east_start = vertex(21, vec![VoronoiVertexOrigin::ChartCut {
            side: VoronoiChartCutSide::Upper,
        }]);
        let east_end = vertex(22, vec![VoronoiVertexOrigin::ChartCut {
            side: VoronoiChartCutSide::Upper,
        }]);
        let east = chart_restriction_use(
            &edge_role(Vec::new()),
            &east_start,
            &east_end,
            crate::Point::new_raw_xy(44.0, 2.0),
            crate::Point::new_raw_xy(44.0, 8.0),
            &receipt,
            frame,
        )
        .unwrap()
        .expect("same-side upper chart cuts form a directed chord");
        assert_eq!(east.start, SyntheticChartVertex::Cut {
            node: 21,
            side: ChartCutSide::East,
        });
    }

    #[test]
    fn polar_cap_use_keeps_pole_identity_at_chart_cut_vertices() {
        let (frame, receipt) = chart_receipt();
        let pole_vertex = |id| {
            vertex(id, vec![
                VoronoiVertexOrigin::ChartCut {
                    side: VoronoiChartCutSide::Lower,
                },
                VoronoiVertexOrigin::Pole {
                    latitude: frame.pole(),
                },
            ])
        };
        let start = pole_vertex(31);
        let end = pole_vertex(32);
        let use_ = chart_restriction_use(
            &edge_role(Vec::new()),
            &start,
            &end,
            crate::Point::new_raw_xy(40.0, frame.pole()),
            crate::Point::new_raw_xy(80.0, frame.pole()),
            &receipt,
            frame,
        )
        .unwrap()
        .expect("same-pole vertices form a polar cap restriction");
        assert_eq!(use_.restriction, ChartRestriction::PolarCap {
            pole: ChartPole::North,
        });
        assert_eq!(use_.start, SyntheticChartVertex::Pole {
            node: 31,
            pole: ChartPole::North,
            native_longitude_bits: 40.0_f64.to_bits(),
        });
    }

    #[test]
    fn authentic_clip_edge_is_not_a_chart_restriction() {
        let (frame, receipt) = chart_receipt();
        let start = vertex(41, vec![VoronoiVertexOrigin::ChartCut {
            side: VoronoiChartCutSide::Lower,
        }]);
        let end = vertex(42, vec![VoronoiVertexOrigin::ChartCut {
            side: VoronoiChartCutSide::Lower,
        }]);
        assert!(
            chart_restriction_use(
                &edge_role(vec![VoronoiEdgeOrigin::SourceClip { segment: 0 }]),
                &start,
                &end,
                crate::Point::new_raw_xy(40.0, 2.0),
                crate::Point::new_raw_xy(40.0, 8.0),
                &receipt,
                frame,
            )
            .unwrap()
            .is_none()
        );
    }
}
