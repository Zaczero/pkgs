use std::sync::Arc;

use crate::Frame;
use crate::array::StreamingShapes;
use crate::error::{ResourceError, Result};
use crate::geographic::GeographicFrame;
use crate::geometry::embedding_completion::literals::{
    LiteralAddress, LiteralPolygon, LiteralRowLayout, LiteralSequence, ShapeLiteralLayout,
};
use crate::geometry::embedding_completion::storage::FrozenLiteralBuffers;
use crate::geometry::publication::{
    FinishedGeometry, ProducerDrawingReceipt, ProducerImageId, ProducerLiteral, StorageTarget,
    finish_producer_drawing,
};
use crate::geometry::storage::{PackedRow, RowSelection, ShapeRow, ValidityMask};
use crate::geometry::tessellation::{
    VoronoiDiagramReceipt, VoronoiEdgeColumn, VoronoiPolygonOutputs, VoronoiRingEdgeRole,
};
use crate::geometry::{ExpansionBudget, Ring, Shape};
use crate::work::Unmetered;

pub(crate) struct VoronoiDrawingWriter {
    target: StorageTarget,
    output: StreamingShapes,
    row_map: Vec<Option<LiteralRowLayout<ProducerImageId>>>,
    literals: Vec<ProducerLiteral>,
    group_offsets: Vec<i64>,
}

impl VoronoiDrawingWriter {
    pub(crate) fn new_array() -> Self {
        Self::new(StorageTarget::Array)
    }

    pub(crate) fn new_groups() -> Self {
        Self::new(StorageTarget::Groups)
    }

    fn new(target: StorageTarget) -> Self {
        Self {
            target,
            output: StreamingShapes::new(),
            row_map: Vec::new(),
            literals: Vec::new(),
            group_offsets: if target == StorageTarget::Groups {
                vec![0]
            } else {
                Vec::new()
            },
        }
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
                literal_rings
                    .push(self.polygon_ring_literals(ring, roles, &receipt, address, budget)?);
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
            for (point_index, _) in separator.vertices.iter().enumerate() {
                let point_offset = start
                    .checked_add(point_index)
                    .ok_or(ResourceError::SizeOverflow)?;
                let point = coordinates.point_at(point_offset);
                let image = self.push_literal(address, point, budget)?;
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
        let receipt = ProducerDrawingReceipt::new(buffers, literals);
        finish_producer_drawing(receipt, frame, target)
    }

    fn polygon_ring_literals(
        &mut self,
        ring: &Ring,
        roles: &[VoronoiRingEdgeRole],
        receipt: &VoronoiDiagramReceipt,
        address: LiteralAddress,
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
        Ok(LiteralSequence {
            output: 0,
            axes: ring.coords().axes(),
            images,
        })
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

fn voronoi_publication_error(message: &'static str) -> crate::error::Error {
    crate::geometry::GeometryErrorKind::Invalid(message.into()).into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorKind;
    use crate::geometry::{GeometryErrorKind, VoronoiBoundary};

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
        let mut writer = VoronoiDrawingWriter::new_array();
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
        writer.literals.pop().expect("Voronoi output has literals");

        match writer.finish_array(Frame::None) {
            Err(error) => assert!(matches!(
                error.kind(),
                ErrorKind::Geometry(GeometryErrorKind::Invalid(_))
            )),
            Ok(_) => panic!("the shared readback rejects a missing Voronoi literal"),
        }
    }
}
