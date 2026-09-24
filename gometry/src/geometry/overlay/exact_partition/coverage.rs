//! Coverage cleaning and certification over an exact partition.
//!
//! A coverage is a set of rows whose areal supports do not overlap. These
//! methods walk the partition's global faces once, carrying the rows that
//! occupy each face, so a face-by-row membership matrix is never built.

use std::cmp::Ordering;

#[cfg(test)]
use super::metrics::update as update_partition_metrics;
use super::{
    ArealCells, CoverageFaceState, ExactPartition, ExactSourceInventory, GlobalFaces, KeptNodes,
    NodeId, NodeParts, bucket_axes, with_axes,
};
use crate::error::ResourceError;
use crate::geometry::arrangement::IndexedTopology;
use crate::geometry::exact_arith::{dd_add, floor_binary_exponent, two_sum};
use crate::geometry::{
    CoordSeq, CoordinateAxes, CoverageOverlapRule, ExactRationalArea, GeometryErrorKind, HashMap,
    Point, Polygon, Result, Ring, Shape, XY, axis_pow2_scale, point_distance,
    power_of_two_exponent, scale_by_power_of_two, scaled_residual,
};

/// A nonnegative binary64 significand times an extended binary exponent.
#[derive(Clone, Copy, Debug, Default)]
struct ScaledLength {
    significand: f64,
    exponent: i32,
}

impl ScaledLength {
    fn from_finite(value: f64) -> Self {
        debug_assert!(value.is_finite() && value >= 0.0);
        Self::from_scaled(value, 0)
    }

    fn from_scaled(value: f64, exponent: i32) -> Self {
        debug_assert!(value.is_finite() && value >= 0.0);
        if value.total_cmp(&0.0) == Ordering::Equal {
            return Self::default();
        }
        let adjustment = floor_binary_exponent(value);
        Self {
            significand: scale_by_power_of_two(value, adjustment.wrapping_neg()),
            exponent: exponent.wrapping_add(adjustment),
        }
    }

    fn is_positive(self) -> bool {
        self.significand > 0.0
    }

    fn compare(self, other: Self) -> Ordering {
        match (
            self.significand.total_cmp(&0.0) == Ordering::Equal,
            other.significand.total_cmp(&0.0) == Ordering::Equal,
        ) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Less,
            (false, true) => Ordering::Greater,
            (false, false) => self
                .exponent
                .cmp(&other.exponent)
                .then_with(|| self.significand.total_cmp(&other.significand)),
        }
    }
}

/// A guarded binary64 reduction of positive lengths without an f64 range cap.
///
/// The rank describes binary64 distance terms, not an exact sum of irrational
/// Euclidean lengths.
#[derive(Clone, Copy, Debug, Default)]
struct ScaledLengthSum {
    high: f64,
    low: f64,
    exponent: i32,
    nonzero: bool,
}

#[derive(Clone, Copy)]
enum BorderTieBreak {
    LowestRow,
    HighestRow,
}

impl ScaledLengthSum {
    fn add(&mut self, length: ScaledLength) {
        if !length.is_positive() {
            return;
        }
        if !self.nonzero {
            self.high = length.significand;
            self.exponent = length.exponent;
            self.nonzero = true;
            return;
        }

        let term = if length.exponent > self.exponent {
            let shift = self.exponent.wrapping_sub(length.exponent);
            self.high = scale_by_power_of_two(self.high, shift);
            self.low = scale_by_power_of_two(self.low, shift);
            self.exponent = length.exponent;
            length.significand
        } else {
            scale_by_power_of_two(
                length.significand,
                length.exponent.wrapping_sub(self.exponent),
            )
        };
        (self.high, self.low) = dd_add((self.high, self.low), (term, 0.0));
        self.normalize();
    }

    fn normalize(&mut self) {
        let adjustment = floor_binary_exponent(self.high);
        self.high = scale_by_power_of_two(self.high, adjustment.wrapping_neg());
        self.low = scale_by_power_of_two(self.low, adjustment.wrapping_neg());
        self.exponent = self.exponent.wrapping_add(adjustment);
    }

    fn rank(self) -> ScaledLength {
        if !self.nonzero {
            return ScaledLength::default();
        }
        ScaledLength::from_scaled(two_sum(self.high, self.low).0, self.exponent)
    }
}

#[expect(
    clippy::indexing_slicing,
    reason = "callers provide row IDs from the parallel border-rank column"
)]
fn longest_border_owner(
    rows: impl IntoIterator<Item = usize>,
    overflowed_ranks: &[ScaledLength],
    tie_break: BorderTieBreak,
    include_zero: bool,
) -> Option<usize> {
    let mut best: Option<(usize, ScaledLength)> = None;
    for row in rows {
        let rank = overflowed_ranks[row];
        if !include_zero && !rank.is_positive() {
            continue;
        }
        let replace = if let Some((best_row, best_rank)) = best {
            let order = rank.compare(best_rank);
            order == Ordering::Greater
                || (order == Ordering::Equal
                    && match tie_break {
                        BorderTieBreak::LowestRow => row < best_row,
                        BorderTieBreak::HighestRow => row > best_row,
                    })
        } else {
            true
        };
        if replace {
            best = Some((row, rank));
        }
    }
    best.map(|(row, _)| row)
}

fn scaled_point_distance(start: XY, end: XY) -> ScaledLength {
    let distance = point_distance(start, end);
    if distance.is_finite() {
        return ScaledLength::from_finite(distance);
    }

    let (dx, dx_base_exponent) = scaled_difference(start.x, end.x);
    let (dy, dy_base_exponent) = scaled_difference(start.y, end.y);
    let dx_world_exponent = if dx.abs().total_cmp(&0.0) == Ordering::Equal {
        None
    } else {
        Some(dx_base_exponent.wrapping_add(floor_binary_exponent(dx.abs())))
    };
    let dy_world_exponent = if dy.abs().total_cmp(&0.0) == Ordering::Equal {
        None
    } else {
        Some(dy_base_exponent.wrapping_add(floor_binary_exponent(dy.abs())))
    };
    let common_exponent = match (dx_world_exponent, dy_world_exponent) {
        (Some(dx), Some(dy)) => dx.max(dy),
        (Some(dx), None) => dx,
        (None, Some(dy)) => dy,
        (None, None) => return ScaledLength::default(),
    };
    // `dx` and `dy` are already framed; subtract only each base exponent to
    // bring both components into the common scale used by `hypot`. The largest
    // normalized axis is in [1, 2), so the norm stays finite; zero axes remain
    // zero and tiny axes can round away only below that rank's f64 precision.
    let dx = dx_world_exponent.map_or(0.0, |_| {
        scale_by_power_of_two(dx, dx_base_exponent.wrapping_sub(common_exponent))
    });
    let dy = dy_world_exponent.map_or(0.0, |_| {
        scale_by_power_of_two(dy, dy_base_exponent.wrapping_sub(common_exponent))
    });
    ScaledLength::from_scaled(dx.hypot(dy), common_exponent)
}

// Return a framed difference and base exponent, representing the coordinate
// delta as `framed_delta * 2^base_exponent`.
fn scaled_difference(start: f64, end: f64) -> (f64, i32) {
    let delta = end - start;
    if delta.is_finite() {
        return (delta, 0);
    }
    // Keep finite residuals unscaled; scale this axis only to rescue overflow.
    let scale = axis_pow2_scale(start.abs().max(end.abs()));
    (
        scaled_residual(end, start, scale),
        power_of_two_exponent(scale).wrapping_neg(),
    )
}

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

    fn for_each_span_row(
        &self,
        partition: &ExactPartition<'_>,
        span: usize,
        seen_rows: &mut Vec<u32>,
        mut visit: impl FnMut(usize) -> Result<()>,
    ) -> Result<()> {
        seen_rows.clear();
        for &(source, _) in partition.owners(span) {
            let operand = *self.source_operands.get(source as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "exact partition provenance references an unknown source".into(),
                )
            })?;
            if seen_rows.contains(&operand) {
                continue;
            }
            seen_rows.push(operand);
            visit(operand as usize)?;
        }
        Ok(())
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "the admitted face CSR and partition spans own these indices"
    )]
    fn overflowed_border_ranks(
        &self,
        partition: &ExactPartition<'_>,
        edges: &[u32],
        face_starts: &[u32],
        face_slots: &[u32],
        face: usize,
        row_count: usize,
        seen_rows: &mut Vec<u32>,
    ) -> Result<Vec<ScaledLength>> {
        let mut lengths = vec![ScaledLengthSum::default(); row_count];
        let start = face_starts[face] as usize;
        let end = face_starts[face.wrapping_add(1)] as usize;
        for &slot in &face_slots[start..end] {
            let span = edges[slot as usize] as usize;
            let atomic = partition.spans[span];
            let length = scaled_point_distance(
                partition.node_xy(atomic.start),
                partition.node_xy(atomic.end),
            );
            self.for_each_span_row(partition, span, seen_rows, |row| {
                lengths[row].add(length);
                Ok(())
            })?;
        }
        Ok(lengths.into_iter().map(ScaledLengthSum::rank).collect())
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
                let (area, perimeter, any_border_overflowed) = if needs_stats {
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
                    (0.0, 0.0, false)
                };
                #[expect(
                    clippy::indexing_slicing,
                    reason = "candidate rows are admitted and border_lengths is row_count-sized"
                )]
                let border_overflowed = if !needs_stats {
                    false
                } else if candidates.is_empty() {
                    any_border_overflowed
                } else {
                    candidates
                        .iter()
                        .any(|&row| border_lengths[row].is_infinite())
                };
                let owner = match candidates.as_slice() {
                    [] if gap_width > 0.0
                        && perimeter > 0.0
                        && 2.0 * area / perimeter < gap_width =>
                    {
                        if border_overflowed {
                            let border_ranks = self.overflowed_border_ranks(
                                partition,
                                &edges,
                                &face_starts,
                                &face_slots,
                                face_index,
                                row_count,
                                &mut seen_rows,
                            )?;
                            longest_border_owner(
                                0..row_count,
                                &border_ranks,
                                BorderTieBreak::HighestRow,
                                false,
                            )
                        } else {
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
                        }
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
                        CoverageOverlapRule::LongestBorder => {
                            if border_overflowed {
                                let border_ranks = self.overflowed_border_ranks(
                                    partition,
                                    &edges,
                                    &face_starts,
                                    &face_slots,
                                    face_index,
                                    row_count,
                                    &mut seen_rows,
                                )?;
                                longest_border_owner(
                                    candidates.iter().copied(),
                                    &border_ranks,
                                    BorderTieBreak::LowestRow,
                                    true,
                                )
                                .expect("non-empty face candidates")
                            } else {
                                candidates
                                    .iter()
                                    .copied()
                                    .map(|row| (row, border_lengths[row]))
                                    // Reversed row comparison retains the historical
                                    // lowest row on equal overlap borders.
                                    .max_by(|left, right| {
                                        left.1.total_cmp(&right.1).then(right.0.cmp(&left.0))
                                    })
                                    .map(|(row, _)| row)
                                    .expect("non-empty face candidates")
                            }
                        },
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
    ) -> Result<(f64, f64, bool)> {
        border_lengths.fill(0.0);
        let start = face_starts[face] as usize;
        let end = face_starts[face + 1] as usize;
        let mut area = measure_area.then(ExactRationalArea::new);
        let mut perimeter = 0.0;
        // Perimeter remains in distance space; only border ranking needs an
        // extended exponent when its binary64 sum overflows.
        let mut border_overflowed = false;
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
            self.for_each_span_row(partition, span_index, seen_rows, |operand| {
                let border = border_lengths.get_mut(operand).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "exact coverage provenance operand is out of range".into(),
                    )
                })?;
                *border += length;
                if border.is_infinite() {
                    border_overflowed = true;
                }
                Ok(())
            })?;
        }
        let area = match area {
            Some(area) => area.round_abs_half()?,
            None => 0.0,
        };
        Ok((area, perimeter, border_overflowed))
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
            polygons.push(Polygon::new(shell, holes));
        }
        Ok(Shape::narrow_polygons(axes, polygons))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scaled_point_distance_keeps_small_residual_at_large_offset() {
        let origin = XY::new(1.0e308, 0.0);
        let short = scaled_point_distance(origin, XY::new(1.0e308, 1.0e-300));
        let long = scaled_point_distance(origin, XY::new(1.0e308, 2.0e-300));

        assert!(short.is_positive());
        assert!(long.is_positive());
        assert_eq!(short.compare(long), Ordering::Less);
    }

    #[test]
    fn scaled_point_distance_carries_norm_past_f64_max() {
        let maximum = f64::MAX;
        let length = scaled_point_distance(XY::new(-maximum, -maximum), XY::new(maximum, maximum));

        assert!(length.is_positive());
        assert!(length.significand.is_finite());
        assert!(length.exponent > floor_binary_exponent(maximum));
    }

    #[test]
    fn scaled_point_distance_keeps_multiaxis_overflow_ranks_finite() {
        let axis = scaled_point_distance(XY::new(0.0, 0.0), XY::new(1.5e308, 0.0));
        let finite_delta_diagonal =
            scaled_point_distance(XY::new(0.0, 0.0), XY::new(1.5e308, 1.5e308));

        assert!((1.0..2.0).contains(&axis.significand));
        assert!((1.0..2.0).contains(&finite_delta_diagonal.significand));
        assert_eq!(finite_delta_diagonal.compare(axis), Ordering::Greater);

        let overflowed_delta_axis =
            scaled_point_distance(XY::new(-1.2e308, 0.0), XY::new(1.2e308, 0.0));
        let overflowed_delta_diagonal =
            scaled_point_distance(XY::new(-1.2e308, -1.2e308), XY::new(1.2e308, 1.2e308));

        assert!((1.0..2.0).contains(&overflowed_delta_axis.significand));
        assert!((1.0..2.0).contains(&overflowed_delta_diagonal.significand));
        assert_eq!(
            overflowed_delta_diagonal.compare(overflowed_delta_axis),
            Ordering::Greater
        );

        let subnormal_y =
            scaled_point_distance(XY::new(-1.2e308, 0.0), XY::new(1.2e308, f64::from_bits(1)));
        // This least-subnormal y delta is below binary64 rank resolution beside x.
        assert_eq!(subnormal_y.compare(overflowed_delta_axis), Ordering::Equal);
    }

    #[test]
    fn compensated_length_rank_retains_small_following_terms() {
        let large = ScaledLength::from_finite(2.0_f64.powi(60));
        let mut base = ScaledLengthSum::default();
        base.add(large);
        let base_rank = base.rank();

        let mut below = base;
        for _ in 0..127 {
            below.add(ScaledLength::from_finite(1.0));
        }
        assert_eq!(below.rank().compare(base_rank), Ordering::Equal);

        let mut above = base;
        for _ in 0..256 {
            above.add(ScaledLength::from_finite(1.0));
        }
        assert_eq!(above.rank().compare(base_rank), Ordering::Greater);
    }

    #[test]
    fn overlap_rank_preserves_lowest_row_when_all_borders_are_zero() {
        let zero = ScaledLength::default();
        let owner = longest_border_owner([0, 1], &[zero, zero], BorderTieBreak::LowestRow, true);
        assert_eq!(owner, Some(0));
    }

    #[test]
    fn gap_rank_ignores_rows_without_a_border() {
        let zero = ScaledLength::default();
        let positive = ScaledLength::from_finite(1.0);
        let positive_owner =
            longest_border_owner([0, 1], &[zero, positive], BorderTieBreak::HighestRow, false);
        assert_eq!(positive_owner, Some(1));

        let empty_owner =
            longest_border_owner([0, 1], &[zero, zero], BorderTieBreak::HighestRow, false);
        assert_eq!(empty_owner, None);
    }

    #[test]
    fn overflowed_gap_rank_keeps_highest_row_tie() {
        let tied = ScaledLength::from_scaled(1.5, 1024);
        let owner = longest_border_owner([0, 1], &[tied, tied], BorderTieBreak::HighestRow, false);
        assert_eq!(owner, Some(1));
    }
}
