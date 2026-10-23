use super::{
    ExactPartition, ExactSourceInventory, ExpansionBudget, NodeId, PolygonOccupancy, Result, Shape,
};
use crate::geometry::output::build_certified_shape;

impl ExactSourceInventory {
    /// Measure the occupied Cartesian support without serializing rounded
    /// boundary coordinates. Face selection follows the same stored-ring
    /// winding fold as ordinary overlay; the selected homogeneous face
    /// integrals are accumulated as one exact rational before final rounding.
    #[expect(
        clippy::indexing_slicing,
        reason = "face, dart, twin, and node IDs address vectors built by this partition's checked topology"
    )]
    pub(in crate::geometry) fn occupied_cartesian_area(
        &self,
        partition: &ExactPartition<'_>,
    ) -> Result<f64> {
        debug_assert_eq!(
            self.operand_count(),
            1,
            "occupied area is an operation over one composed operand"
        );
        let (topology, edges) = partition.local_topology()?;
        let global = partition.global_faces(&topology, &edges)?;
        let winding_rings = self.winding_source_rings();
        let mut selected = Vec::with_capacity(global.count as usize);
        selected.resize(global.count as usize, false);
        let mut occupancy = PolygonOccupancy::new(self, false);
        if let Some((_, seed)) = &partition.winding_seed {
            occupancy.seed(seed)?;
        }
        partition.visit_face_windings_with_hook(
            &topology,
            &edges,
            &global,
            &winding_rings,
            self.ring_count,
            &mut occupancy,
            |winding, ring, direction, occupancy| {
                occupancy.cross(winding, ring, direction);
                Ok(())
            },
            |face, _, occupancy| {
                selected[face as usize] = occupancy.operand_occupied(0);
                Ok(())
            },
        )?;
        let mut area = crate::geometry::ExactRationalArea::new();
        for (walk, &[start, end]) in topology.face_starts.array_windows::<2>().enumerate() {
            if !selected[global.walk_faces[walk] as usize] {
                continue;
            }
            let range = start as usize..end as usize;
            for &slot in &topology.face_slots[range] {
                let slot = slot as usize;
                let twin_face =
                    global.walk_faces[topology.face_of[topology.twins[slot] as usize] as usize];
                if selected[twin_face as usize] {
                    continue;
                }
                area.add_homogeneous_cross(
                    &*partition.exact(NodeId(topology.owners[slot]))?,
                    &*partition.exact(NodeId(topology.targets[slot]))?,
                )?;
            }
        }
        area.round_abs_half()
    }

    /// Serialize the occupied Cartesian support from the same selected faces
    /// used by the exact fill selection. The caller may reuse this
    /// named owner for metrics whose conversion is not planar, such as the
    /// geographic area metric.
    pub(in crate::geometry) fn occupied_cartesian_support(
        &self,
        partition: &ExactPartition<'_>,
    ) -> Result<Shape> {
        let (points, lines, polygons) =
            self.overlay_parts_with_fill(partition, crate::geometry::OverlayOp::Union, "area")?;
        build_certified_shape(points, lines, polygons)
    }
}

pub(crate) fn occupied_cartesian_area(shape: &Shape) -> Result<f64> {
    let Some(inventory) = ExactSourceInventory::from_occupied_area_shape(shape)? else {
        return Ok(0.0);
    };
    let mut budget = ExpansionBudget::new("area", "exact partition");
    let partition = inventory.partition(&mut budget)?;
    inventory.occupied_cartesian_area(&partition)
}

pub(crate) fn occupied_cartesian_support(shape: &Shape) -> Result<Shape> {
    let inventory = ExactSourceInventory::from_shapes(&[shape])?;
    let mut budget = ExpansionBudget::new("area", "exact partition");
    let partition = inventory.partition(&mut budget)?;
    inventory.occupied_cartesian_support(&partition)
}

#[cfg(test)]
pub(crate) fn cartesian_support_nonempty(shape: &Shape) -> Result<bool> {
    let inventory = ExactSourceInventory::from_shapes(&[shape])?;
    let mut budget = ExpansionBudget::new("support", "exact partition");
    let partition = inventory.partition(&mut budget)?;
    let (topology, edges) = partition.local_topology()?;
    let global = partition.global_faces(&topology, &edges)?;
    let cells = inventory.select_cells(
        &partition,
        &topology,
        &edges,
        &global,
        crate::geometry::OverlayOp::Union,
        false,
    )?;
    Ok(cells.faces.iter().any(|&value| value)
        || cells.spans.iter().any(|&value| value)
        || cells.nodes.iter().any(|&value| value))
}
