use crate::error::{ResourceError, Result};
use crate::geographic::GeographicFrame;
use crate::geometry::publication::PublicationRows;
use crate::geometry::storage::{PackedRow, RowSelection, ShapeRow};
use crate::geometry::tessellation::{
    VoronoiDiagramReceipt, VoronoiEdgeColumn, VoronoiPolygonOutputs, VoronoiRingEdgeRole,
};
use crate::geometry::{ExpansionBudget, Ring, Shape};

impl PublicationRows {
    pub(crate) fn push_voronoi_polygon_outputs(
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
            let rings = std::iter::once(&polygon.shell).chain(polygon.holes.iter());
            let mut ring_count = 0_usize;
            for (ring_index, ring) in rings.enumerate() {
                let roles = output
                    .edge_roles
                    .get(ring_index)
                    .ok_or_else(|| voronoi_publication_error("Voronoi ring roles are absent"))?;
                validate_polygon_ring(ring, roles, &receipt)?;
                for _ in ring {
                    budget.add(1)?;
                }
                ring_count = ring_count
                    .checked_add(1)
                    .ok_or(ResourceError::SizeOverflow)?;
            }
            if ring_count != output.edge_roles.len() {
                return Err(voronoi_publication_error(
                    "Voronoi output has unused ring roles",
                ));
            }
            self.try_push_drawing(shape, self.next_drawing_row(), "voronoi_cells")?;
        }
        Ok(())
    }

    pub(crate) fn push_voronoi_edge_column(
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
            for _ in &separator.vertices {
                budget.add(1)?;
            }
            self.try_push_drawing_line(
                ShapeRow::from_packed(PackedRow::line(
                    line,
                    &coordinates,
                    &offsets,
                    row_selection.as_deref(),
                )),
                self.next_drawing_row(),
                "voronoi_edges",
            )?;
        }
        Ok(())
    }
}

fn validate_polygon_ring(
    ring: &Ring,
    roles: &[VoronoiRingEdgeRole],
    receipt: &VoronoiDiagramReceipt,
) -> Result<()> {
    let edge_count = ring
        .len()
        .checked_sub(1)
        .ok_or(crate::geometry::GeometryErrorKind::RingTooShort(0))?;
    if roles.len() != edge_count {
        return Err(voronoi_publication_error(
            "Voronoi polygon coordinates do not match edge roles",
        ));
    }
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
        previous_end = Some(end);
    }
    let Some(first_vertex) = first_vertex else {
        return Err(voronoi_publication_error("Voronoi polygon ring is empty"));
    };
    if previous_end != Some(first_vertex) {
        return Err(voronoi_publication_error(
            "Voronoi polygon edge identities do not close their ring",
        ));
    }
    Ok(())
}

fn voronoi_publication_error(message: &'static str) -> crate::error::Error {
    crate::geometry::GeometryErrorKind::Invalid(message.into()).into()
}
