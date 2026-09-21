//! Coverage cleaning and certification over an exact partition.
//!
//! A coverage is a set of rows whose areal supports do not overlap. These
//! methods walk the partition's global faces once, carrying the rows that
//! occupy each face, so a face-by-row membership matrix is never built.

#[cfg(test)]
use super::metrics::update as update_partition_metrics;
use super::{
    ArealCells, CoverageFaceState, ExactPartition, ExactSourceInventory, GlobalFaces, KeptNodes,
    NodeId, NodeParts, bucket_axes, with_axes,
};
use crate::error::ResourceError;
use crate::geometry::arrangement::IndexedTopology;
use crate::geometry::{
    CoordSeq, CoordinateAxes, CoverageOverlapRule, ExactRationalArea, GeometryErrorKind, HashMap,
    Point, Polygon, Result, Ring, Shape, point_distance,
};

impl ExactSourceInventory {
    /// Visit each global face with the rows whose exact areal supports occupy
    /// that face. The active row list is updated from sparse ring crossings,
    /// avoiding a face-by-row membership matrix for disjoint coverage.
    pub(super) fn visit_coverage_face_rows(
        &self,
        partition: &ExactPartition<'_>,
        topology: &IndexedTopology,
        edges: &[u32],
        global: &GlobalFaces,
        row_count: usize,
        mut visit: impl FnMut(u32, &[usize]) -> Result<()>,
    ) -> Result<()> {
        let winding_rings = self.winding_source_rings();
        let mut state = CoverageFaceState::new(self.polygon_count, row_count);
        partition.visit_face_windings_with_hook(
            topology,
            edges,
            global,
            &winding_rings,
            self.ring_count,
            &mut state,
            |winding, ring, direction, state| state.update_ring(self, winding, ring, direction),
            |face, _, state| visit(face, &state.active_rows),
        )
    }

    /// Label every bounded global face and publish each coverage row from that
    /// one exact partition.
    ///
    /// Face membership and gap ownership are decided from stored-ring
    /// windings and exact span provenance.  Boundary plans are then built one
    /// row at a time, but all of them consume the same rounded node map and
    /// the same resolved ordinate map.  No emitted coordinate participates in
    /// selection, and no row is sent through a second partition.
    pub(in crate::geometry) fn coverage_clean_rows(
        &self,
        partition: &mut ExactPartition<'_>,
        row_areas: &[f64],
        gap_width: f64,
        overlaps: CoverageOverlapRule,
        declared_axes: CoordinateAxes,
    ) -> Result<Vec<Shape>> {
        let row_count = self.operand_count();
        if row_areas.len() != row_count {
            return Err(GeometryErrorKind::Invalid(
                "coverage row areas are out of sync with exact operands".into(),
            )
            .into());
        }
        let row_count_u32 = u32::try_from(row_count).map_err(|_| ResourceError::SizeOverflow)?;
        // Cleaning historically kept every source node while extracting
        // minimal faces. Keep that identity and provenance contract in the
        // direct partition publisher rather than relying on emitted geometry
        // to preserve a source vertex.
        partition.kept_nodes = KeptNodes::All;

        #[cfg(test)]
        let topology_faces_phase = crate::test_support::allocation_phase(
            crate::test_support::AllocationPhase::TopologyFaces,
        );
        let (topology, edges) = partition.local_topology()?;
        let global = partition.global_faces(&topology, &edges)?;
        #[cfg(test)]
        update_partition_metrics(|metrics| {
            metrics.topology_halfedges = topology.face_of.len();
            metrics.global_faces = global.count as usize;
        });
        let (face_starts, face_slots) = self.global_face_slots(&topology, &edges, &global)?;
        let mut face_owners = Vec::with_capacity(global.count as usize);
        face_owners.resize(global.count as usize, u32::MAX);
        let mut face_seen = Vec::with_capacity(global.count as usize);
        face_seen.resize(global.count as usize, false);

        let mut candidates = Vec::with_capacity(row_count);
        let mut border_lengths = Vec::with_capacity(row_count);
        border_lengths.resize(row_count, 0.0);
        let mut seen_rows = Vec::new();

        self.visit_coverage_face_rows(
            partition,
            &topology,
            &edges,
            &global,
            row_count,
            |face, active_rows| {
                let face_index = face as usize;
                let seen = face_seen.get_mut(face_index).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "exact coverage winding visited an unknown global face".into(),
                    )
                })?;
                *seen = true;
                if face == 0 {
                    return Ok(());
                }
                candidates.clear();
                candidates.reserve(active_rows.len());
                candidates.extend_from_slice(active_rows);
                candidates.sort_unstable();
                let needs_stats = candidates.is_empty() && gap_width > 0.0
                    || candidates.len() > 1
                        && matches!(overlaps, CoverageOverlapRule::LongestBorder);
                let (area, perimeter) = if needs_stats {
                    let measure_area = candidates.is_empty();
                    self.coverage_face_stats(
                        partition,
                        &topology,
                        &edges,
                        &face_starts,
                        &face_slots,
                        face as usize,
                        &mut border_lengths,
                        &mut seen_rows,
                        measure_area,
                    )?
                } else {
                    (0.0, 0.0)
                };
                let owner = match candidates.as_slice() {
                    [] if gap_width > 0.0
                        && perimeter > 0.0
                        && 2.0 * area / perimeter < gap_width =>
                    {
                        (0..row_count)
                            .filter_map(|row| {
                                let border = border_lengths[row];
                                (border > 0.0).then_some((row, border))
                            })
                            // The ascending scan and ordinary `max_by` tie
                            // behavior retain the historical highest row on
                            // equal gap borders.
                            .max_by(|left, right| left.1.total_cmp(&right.1))
                            .map(|(row, _)| row)
                    },
                    [] => None,
                    [only] => Some(*only),
                    _ => Some(match overlaps {
                        CoverageOverlapRule::MinIndex => candidates[0],
                        CoverageOverlapRule::MaxArea => candidates
                            .iter()
                            .copied()
                            .max_by(|left, right| row_areas[*left].total_cmp(&row_areas[*right]))
                            .expect("non-empty face candidates"),
                        CoverageOverlapRule::MinArea => candidates
                            .iter()
                            .copied()
                            .min_by(|left, right| row_areas[*left].total_cmp(&row_areas[*right]))
                            .expect("non-empty face candidates"),
                        CoverageOverlapRule::LongestBorder => candidates
                            .iter()
                            .copied()
                            .map(|row| (row, border_lengths[row]))
                            // Reversed row comparison retains the historical
                            // lowest row on equal overlap borders.
                            .max_by(|left, right| {
                                left.1.total_cmp(&right.1).then(right.0.cmp(&left.0))
                            })
                            .map(|(row, _)| row)
                            .expect("non-empty face candidates"),
                    }),
                };
                if let Some(owner) = owner {
                    face_owners[face as usize] =
                        u32::try_from(owner).map_err(|_| ResourceError::SizeOverflow)?;
                }
                Ok(())
            },
        )?;
        #[cfg(test)]
        drop(topology_faces_phase);

        let output_nodes = self.certify_coverage_partition(
            partition,
            &face_owners,
            &face_seen,
            row_count,
            &topology,
            &edges,
            &global,
        )?;
        #[cfg(test)]
        let _publication_phase = crate::test_support::allocation_phase(
            crate::test_support::AllocationPhase::Publication,
        );
        #[cfg(test)]
        update_partition_metrics(|metrics| {
            metrics.publication_nodes = output_nodes.len();
        });
        ExactPartition::check_node_injectivity(
            &output_nodes,
            &partition.round_nodes(&output_nodes),
        )?;
        let by_node = self.resolve_node_points(
            partition,
            &output_nodes,
            |node| Ok(partition.node_xy(node)),
            declared_axes,
        )?;

        let mut outputs = Vec::with_capacity(row_count);
        for row in 0..row_count_u32 {
            let mut cells = ArealCells {
                faces: Vec::new(),
                spans: Vec::new(),
                nodes: Vec::new(),
            };
            cells.faces.reserve(face_owners.len());
            for &owner in &face_owners {
                cells.faces.push(owner == row);
            }
            cells.spans.reserve(partition.spans.len());
            cells.spans.resize(partition.spans.len(), false);
            cells.nodes.reserve(partition.node_count());
            cells.nodes.resize(partition.node_count(), false);
            let plan = cells.emission_plan(partition, &topology, &edges, &global)?;
            let parts = plan.emit_node_parts_with_shared_rounding(partition, &topology)?;
            outputs.push(self.coverage_shape_from_node_parts(parts, &by_node, declared_axes)?);
        }
        #[cfg(test)]
        update_partition_metrics(|metrics| {
            metrics.published_rows = outputs.len();
        });
        Ok(outputs)
    }

    pub(super) fn certify_coverage_partition(
        &self,
        partition: &ExactPartition<'_>,
        face_owners: &[u32],
        face_seen: &[bool],
        row_count: usize,
        topology: &IndexedTopology,
        edges: &[u32],
        global: &GlobalFaces,
    ) -> Result<Vec<NodeId>> {
        if face_owners.len() != global.count as usize
            || face_owners.first().copied() != Some(u32::MAX)
            || face_seen.len() != global.count as usize
            || face_seen.iter().any(|&seen| !seen)
        {
            return Err(GeometryErrorKind::Invalid(
                "exact coverage face labels are out of sync with global faces".into(),
            )
            .into());
        }
        for &owner in &face_owners[1..] {
            if owner != u32::MAX && owner as usize >= row_count {
                return Err(GeometryErrorKind::Invalid(
                    "exact coverage face owner is out of range".into(),
                )
                .into());
            }
        }
        let mut boundary_spans = 0_usize;
        for (slot, _) in edges.iter().enumerate() {
            let twin = topology.twins[slot] as usize;
            if slot > twin {
                continue;
            }
            let left = face_owners[global.walk_faces[topology.face_of[slot] as usize] as usize];
            let right = face_owners[global.walk_faces[topology.face_of[twin] as usize] as usize];
            if left != right && (left != u32::MAX || right != u32::MAX) {
                boundary_spans = boundary_spans
                    .checked_add(1)
                    .ok_or(ResourceError::SizeOverflow)?;
            }
        }
        let mut output_nodes = Vec::new();
        let node_capacity = boundary_spans
            .checked_mul(2)
            .ok_or(ResourceError::SizeOverflow)?;
        output_nodes.reserve(node_capacity);
        for (slot, &edge) in edges.iter().enumerate() {
            let twin = topology.twins[slot] as usize;
            if slot > twin {
                continue;
            }
            let left = face_owners[global.walk_faces[topology.face_of[slot] as usize] as usize];
            let right = face_owners[global.walk_faces[topology.face_of[twin] as usize] as usize];
            if left == right || (left == u32::MAX && right == u32::MAX) {
                continue;
            }
            let span = partition.spans[edge as usize];
            output_nodes.push(span.start);
            output_nodes.push(span.end);
        }
        output_nodes.sort_unstable_by_key(|node| node.0);
        output_nodes.dedup_by_key(|node| node.0);
        Ok(output_nodes)
    }

    pub(super) fn coverage_face_stats(
        &self,
        partition: &ExactPartition<'_>,
        topology: &IndexedTopology,
        edges: &[u32],
        face_starts: &[u32],
        face_slots: &[u32],
        face: usize,
        border_lengths: &mut [f64],
        seen_rows: &mut Vec<u32>,
        measure_area: bool,
    ) -> Result<(f64, f64)> {
        border_lengths.fill(0.0);
        let start = face_starts[face] as usize;
        let end = face_starts[face + 1] as usize;
        let mut area = measure_area.then(ExactRationalArea::new);
        let mut perimeter = 0.0;
        for &slot in &face_slots[start..end] {
            let slot = slot as usize;
            let span_index = edges[slot] as usize;
            let span = partition.spans[span_index];
            let start_xy = partition.node_xy(span.start);
            let end_xy = partition.node_xy(span.end);
            let length = point_distance(start_xy, end_xy);
            perimeter += length;
            if let Some(area) = &mut area {
                area.add_homogeneous_cross(
                    &*partition.exact(NodeId(topology.owners[slot]))?,
                    &*partition.exact(NodeId(topology.targets[slot]))?,
                )?;
            }
            seen_rows.clear();
            for &(source, _) in partition.owners(span_index) {
                let operand = *self.source_operands.get(source as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "exact partition provenance references an unknown source".into(),
                    )
                })?;
                if seen_rows.contains(&operand) {
                    continue;
                }
                seen_rows.push(operand);
                let operand = operand as usize;
                let border = border_lengths.get_mut(operand).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "exact coverage provenance operand is out of range".into(),
                    )
                })?;
                *border += length;
            }
        }
        let area = match area {
            Some(area) => area.round_abs_half()?,
            None => 0.0,
        };
        Ok((area, perimeter))
    }

    pub(super) fn coverage_shape_from_node_parts(
        &self,
        parts: NodeParts,
        by_node: &HashMap<NodeId, Point>,
        declared_axes: CoordinateAxes,
    ) -> Result<Shape> {
        debug_assert!(parts.points.is_empty());
        debug_assert!(parts.lines.is_empty());
        let node_count = parts
            .polygons
            .iter()
            .flat_map(|polygon| polygon.iter())
            .map(Vec::len)
            .try_fold(0_usize, |count, ring| {
                count.checked_add(ring).ok_or(ResourceError::SizeOverflow)
            })?;
        let mut nodes = Vec::with_capacity(node_count);
        for polygon in &parts.polygons {
            for ring in polygon {
                nodes.extend_from_slice(ring);
            }
        }
        let axes = bucket_axes(&nodes, by_node, declared_axes, false, "coverage_clean")?;
        let mut polygons = Vec::with_capacity(parts.polygons.len());
        for polygon in parts.polygons {
            let mut rings = Vec::with_capacity(polygon.len());
            for ring in polygon {
                let mut points = Vec::with_capacity(ring.len());
                for node in ring {
                    let point = by_node.get(&node).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "exact coverage output lost a rounded node".into(),
                        )
                    })?;
                    points.push(with_axes(*point, axes));
                }
                rings.push(Ring::try_from_closed(CoordSeq::try_from_points(&points)?)?);
            }
            let mut rings = rings.into_iter();
            let shell = rings.next().expect("coverage polygon has a shell");
            let mut holes = Vec::with_capacity(rings.size_hint().0);
            holes.extend(rings);
            polygons.push(Polygon::new(shell, holes)?);
        }
        Shape::narrow_polygons(axes, polygons)
    }
}
