#[cfg(test)]
use super::metrics::{record_partition_call, update as update_partition_metrics};
use super::{
    Bounds, ExactDyadic, ExactPartition, ExactSourceInventory, ExpansionBudget, GeometryErrorKind,
    IndexedTopology, KeptNodes, ResourceError, Result, RingKind, SegmentClipInterval, XY,
    embedding, homogeneous_midpoint, partition_unique_segments,
    partition_unique_segments_in_window, partition_unique_segments_unbudgeted,
    rectangle_side_outward, same_point, stored_orientation_sign, symbolic_ray_crossing,
};
use crate::geometry::graph::GlobalFaces;
use crate::geometry::numerics::product::ExactSign;
use crate::geometry::overlay::ray_crossing;

impl ExactSourceInventory {
    pub(in crate::geometry) fn partition(
        &self,
        budget: &mut ExpansionBudget,
    ) -> Result<ExactPartition<'_>> {
        self.partition_with_budget(Some(budget))
    }

    pub(in crate::geometry) fn partition_without_budget(&self) -> Result<ExactPartition<'_>> {
        self.partition_with_budget(None)
    }

    pub(super) fn partition_with_budget(
        &self,
        budget: Option<&mut ExpansionBudget>,
    ) -> Result<ExactPartition<'_>> {
        if self.has_source_ordinates() {
            self.partition_with_ordinates::<true>(budget)
        } else {
            self.partition_with_ordinates::<false>(budget)
        }
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "active provenance source ordinals address the staged source-point column"
    )]
    pub(super) fn has_source_ordinates(&self) -> bool {
        let Some(points) = &self.source_points else {
            return false;
        };
        self.provenance.iter().flatten().any(|&(source, _)| {
            points[source as usize]
                .points
                .iter()
                .any(|point| point.z().is_some() || point.m().is_some())
        })
    }

    pub(super) fn partition_with_ordinates<const ORDINATES: bool>(
        &self,
        budget: Option<&mut ExpansionBudget>,
    ) -> Result<ExactPartition<'_>> {
        self.record_partition_inventory();
        let partition = match budget {
            Some(budget) => partition_unique_segments::<ORDINATES>(
                &self.unique,
                &self.provenance,
                &self.isolated,
                budget,
            )?,
            None => partition_unique_segments_unbudgeted::<ORDINATES>(
                &self.unique,
                &self.provenance,
                &self.isolated,
            )?,
        };
        Ok(self.finish_partition(partition, embedding::EmbeddingDomain::Plane))
    }

    pub(super) fn partition_in_window(
        &self,
        intervals: &[SegmentClipInterval],
        bounds: Bounds,
        budget: &mut ExpansionBudget,
    ) -> Result<ExactPartition<'_>> {
        self.record_partition_inventory();
        let partition = if self.has_source_ordinates() {
            partition_unique_segments_in_window::<true>(
                &self.unique,
                &self.provenance,
                &self.isolated,
                intervals,
                bounds,
                budget,
            )?
        } else {
            partition_unique_segments_in_window::<false>(
                &self.unique,
                &self.provenance,
                &self.isolated,
                intervals,
                bounds,
                budget,
            )?
        };
        Ok(self.finish_partition(partition, embedding::EmbeddingDomain::Rectangle(bounds)))
    }

    #[expect(
        clippy::float_cmp,
        reason = "a degenerate rectangle is defined by exact equality of its admitted bounds"
    )]
    #[expect(
        clippy::indexing_slicing,
        reason = "slot, walk, span, source, and ring IDs are admitted by the checked local topology and staged inventory"
    )]
    pub(super) fn rectangle_winding_seed(
        &self,
        partition: &ExactPartition<'_>,
        topology: &IndexedTopology,
        edges: &[u32],
        global: &GlobalFaces,
        bounds: Bounds,
    ) -> Result<Option<(u32, Vec<i64>)>> {
        if bounds.minx() == bounds.maxx() || bounds.miny() == bounds.maxy() {
            return Ok(None);
        }

        let clip_polygon = self
            .polygon_operands
            .iter()
            .position(|&operand| operand == 1)
            .ok_or(ResourceError::SizeOverflow)?;
        let clip_ring = self
            .polygon_ring_ranges
            .get(clip_polygon)
            .map(|range| range.start)
            .ok_or(ResourceError::SizeOverflow)?;
        for (slot, &span_id) in edges.iter().enumerate() {
            if global.walk_faces[topology.face_of[slot] as usize] != 0 {
                continue;
            }
            let span_index = span_id as usize;
            if !partition
                .owners(span_index)
                .iter()
                .any(|&(source, _)| self.source_operands[source as usize] == 1)
            {
                continue;
            }
            let span = partition.spans[span_index];
            let Some(outward) = rectangle_side_outward(partition, span.start, span.end, bounds)?
            else {
                continue;
            };
            let twin = topology.twins[slot] as usize;
            let face = global.walk_faces[topology.face_of[twin] as usize];
            if face == 0 {
                return Err(GeometryErrorKind::Invalid(
                    "rectangle boundary does not separate an interior face".into(),
                )
                .into());
            }

            for node in [span.start, span.end] {
                if !partition.is_stored(node) {
                    continue;
                }
                let point = partition.nearest_xy(node);
                let is_corner = [
                    XY::new(bounds.minx(), bounds.miny()),
                    XY::new(bounds.maxx(), bounds.miny()),
                    XY::new(bounds.maxx(), bounds.maxy()),
                    XY::new(bounds.minx(), bounds.maxy()),
                ]
                .into_iter()
                .any(|corner| same_point(point, corner));
                if !is_corner {
                    continue;
                }
                let (mut winding, boundary) = self.source_winding_at_stored(point);
                if !boundary {
                    let clip = winding
                        .get_mut(clip_ring)
                        .ok_or(ResourceError::SizeOverflow)?;
                    *clip = 1;
                    return Ok(Some((face, winding)));
                }
            }

            let start = partition.exact(span.start)?;
            let end = partition.exact(span.end)?;
            let probe = homogeneous_midpoint(&start, &end)?;
            let direction = outward.map(i8::wrapping_neg);
            let mut winding = self.source_winding_at_symbolic(&probe, direction)?;
            let clip = winding
                .get_mut(clip_ring)
                .ok_or(ResourceError::SizeOverflow)?;
            *clip = 1;
            return Ok(Some((face, winding)));
        }
        Err(GeometryErrorKind::Invalid(
            "rectangle-local partition has no exterior side interval for a winding seed".into(),
        )
        .into())
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "source and ring IDs are admitted by source staging, and winding is sized to ring_count"
    )]
    pub(super) fn source_winding_at_stored(&self, point: XY) -> (Vec<i64>, bool) {
        let mut winding = vec![0_i64; self.ring_count];
        let mut boundary = false;
        for (source, &segment) in self.source_segments.iter().enumerate() {
            if self.source_operands[source] != 0 {
                continue;
            }
            let Some(ring) = self.source_rings[source] else {
                continue;
            };
            if self.ring_kinds[ring as usize] != RingKind::Winding {
                continue;
            }
            boundary |= point.x >= segment.start.x.min(segment.end.x)
                && point.x <= segment.start.x.max(segment.end.x)
                && point.y >= segment.start.y.min(segment.end.y)
                && point.y <= segment.start.y.max(segment.end.y)
                && stored_orientation_sign(
                    segment.start.x,
                    segment.start.y,
                    segment.end.x,
                    segment.end.y,
                    point.x,
                    point.y,
                ) == ExactSign::Zero;
            winding[ring as usize] =
                winding[ring as usize].wrapping_add(i64::from(ray_crossing(segment, point)));
        }
        (winding, boundary)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "source and ring IDs are admitted by source staging, and winding is sized to ring_count"
    )]
    pub(super) fn source_winding_at_symbolic(
        &self,
        point: &[ExactDyadic; 3],
        direction: [i8; 2],
    ) -> Result<Vec<i64>> {
        let mut winding = vec![0_i64; self.ring_count];
        for (source, &segment) in self.source_segments.iter().enumerate() {
            if self.source_operands[source] != 0 {
                continue;
            }
            let Some(ring) = self.source_rings[source] else {
                continue;
            };
            if self.ring_kinds[ring as usize] != RingKind::Winding {
                continue;
            }
            winding[ring as usize] = winding[ring as usize]
                .wrapping_add(i64::from(symbolic_ray_crossing(segment, point, direction)?));
        }
        Ok(winding)
    }

    #[cfg_attr(
        not(test),
        expect(
            clippy::missing_const_for_fn,
            reason = "test-only partition metrics update thread-local counters"
        )
    )]
    pub(super) fn record_partition_inventory(&self) {
        #[cfg(test)]
        record_partition_call();
        #[cfg(test)]
        update_partition_metrics(|metrics| {
            metrics.partition_builds = metrics.partition_builds.wrapping_add(1);
            metrics.source_segments = self.source_segments.len();
            metrics.unique_source_segments = self.unique.len();
        });
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "span owners and node IDs are admitted by the same exact partition"
    )]
    pub(super) fn finish_partition<'a>(
        &self,
        mut partition: ExactPartition<'a>,
        embedding_domain: embedding::EmbeddingDomain,
    ) -> ExactPartition<'a> {
        #[cfg(test)]
        update_partition_metrics(|metrics| {
            metrics.canonical_spans = partition.spans.len();
            metrics.canonical_nodes = partition.node_count();
        });
        let mut on_ring = Vec::with_capacity(partition.node_count());
        on_ring.resize(partition.node_count(), false);
        for (index, span) in partition.spans.iter().enumerate() {
            if partition
                .owners(index)
                .iter()
                .any(|&(source, _)| self.source_rings[source as usize].is_some())
            {
                on_ring[span.start.0 as usize] = true;
                on_ring[span.end.0 as usize] = true;
            }
        }
        partition.kept_nodes = KeptNodes::OffRings(on_ring);
        partition.embedding_domain = embedding_domain;
        partition
    }
}
