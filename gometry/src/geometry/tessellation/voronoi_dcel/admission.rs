//! Exact contact, clip-source, and embedded-boundary admission.

#[cfg(test)]
use super::ValidatedExactCycles;
use super::{
    Bounds, ClipBoundaryFailure, ClipCertificate, ClipCertificateEdge, ClipRingParity,
    ClipSourceRow, ClipSourceSegment, ClipTrace, ClipTraceAtom, ClipTraceRing, ControlFlow,
    CoordSeq, EdgeId, EvaluationBudget, ExactCycleContact, ExactPairTouch, ExactSign,
    ExactSubdivision, ExactVertexSegment, NodedExactCycle, NodedSegmentIndex, NormalizedClip,
    NormalizedSourceRing, OpenClipRing, Ordering, OwnerCycleContainment, OwnerCycleFailure,
    OwnerShellPosition, Point, PointKey, PointSelectionChart, Polygon, PolygonPointIndex, Result,
    Ring, SearchAccount, Segment, SegmentParameter, Site, VertexId, VoronoiBoundary,
    VoronoiClipSourceRoot, XY, YStabbingIndex, ensure_coordseq_vertex_capacity, exact,
    polygon_is_valid, polygon_output_baseline, sort_unique_along, try_dedup_by,
    try_hash_map_with_capacity, try_sort_by, uf_union, voronoi_error,
};

mod clip;
mod cycles;

#[cfg(test)]
pub(super) use clip::normalize_clip;
pub(super) use clip::{
    clip_certificate_data, exact_chart_point, exact_segment_parameter, normalize_clip_with_chart,
    rounded_segment_covered_by_clip,
};
use cycles::validate_exact_contact_graph;
pub(super) use cycles::{
    admit_embedded_clip, exact_cycle_error, validate_assembled_exact_cycles,
    validate_exact_rectangle, validate_owner_cycle_facts,
};
#[cfg(test)]
pub(super) use cycles::{
    first_owner_cycle_failure, for_each_assembled_pair, for_each_touch_fan,
    validate_exact_cycle_nesting, validate_exact_cycles,
};

struct ExactTouchForest {
    cycle_count: usize,
    parents: Vec<usize>,
}

impl ExactTouchForest {
    fn new(cycle_count: usize, touch_count: usize) -> Result<Self> {
        let parent_count = cycle_count
            .checked_add(touch_count)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        let mut parents = Vec::with_capacity(parent_count);
        parents.extend(0..parent_count);
        Ok(Self {
            cycle_count,
            parents,
        })
    }

    fn attach(&mut self, touch: usize, ring: usize) -> Result<()> {
        let point_node = self
            .cycle_count
            .checked_add(touch)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        if uf_union(&mut self.parents, ring, point_node) {
            Ok(())
        } else {
            Err(exact_cycle_error())
        }
    }
}

impl NodedSegmentIndex {
    pub(super) fn new(cycles: &[NodedExactCycle<'_>]) -> Result<Self> {
        let offset_capacity = cycles
            .len()
            .checked_add(1)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        let mut offsets = Vec::with_capacity(offset_capacity);
        offsets.push(0_usize);
        for cycle in cycles {
            let Some((closing, vertices)) = cycle.ring.vertices.split_last() else {
                return Err(exact_cycle_error());
            };
            if vertices.len() < 3
                || vertices.len() != cycle.points.len()
                || vertices.len() != cycle.ring.edges.len()
                || vertices.first() != Some(closing)
            {
                return Err(exact_cycle_error());
            }
            let next = offsets
                .last()
                .expect("the segment offset column starts at zero")
                .checked_add(vertices.len())
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            offsets.push(next);
        }
        let segment_count = *offsets
            .last()
            .expect("the segment offset column contains its terminal value");
        let incidence_count = segment_count
            .checked_mul(2)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        let mut endpoints = Vec::with_capacity(segment_count);
        let mut incidences = Vec::with_capacity(incidence_count);
        for (ring, cycle) in cycles.iter().enumerate() {
            let (_, vertices) = cycle
                .ring
                .vertices
                .split_last()
                .expect("assembled exact rings are closed");
            for edge in 0..vertices.len() {
                let start = *vertices.get(edge).ok_or_else(exact_cycle_error)?;
                let next = edge
                    .checked_add(1)
                    .filter(|&next| next < vertices.len())
                    .unwrap_or(0);
                let end = *vertices.get(next).ok_or_else(exact_cycle_error)?;
                if start == end {
                    return Err(exact_cycle_error());
                }
                let segment = offsets
                    .get(ring)
                    .copied()
                    .ok_or_else(exact_cycle_error)?
                    .checked_add(edge)
                    .ok_or(crate::error::ResourceError::SizeOverflow)?;
                endpoints.push([start, end]);
                incidences.extend([
                    ExactVertexSegment {
                        vertex: start,
                        segment,
                        ring,
                        neighbor: end,
                    },
                    ExactVertexSegment {
                        vertex: end,
                        segment,
                        ring,
                        neighbor: start,
                    },
                ]);
            }
        }
        incidences.sort_unstable_by_key(|incidence| (incidence.vertex, incidence.segment));

        Ok(Self {
            offsets,
            endpoints,
            incidences,
        })
    }

    fn incidence_range(&self, vertex: VertexId) -> (usize, usize) {
        let start = self
            .incidences
            .partition_point(|incidence| incidence.vertex < vertex);
        let end = self
            .incidences
            .partition_point(|incidence| incidence.vertex <= vertex);
        (start, end)
    }

    fn for_each_source_pair(
        &self,
        mut visit: impl FnMut(usize, usize) -> Result<()>,
    ) -> Result<()> {
        for (segment, endpoints) in self.endpoints.iter().copied().enumerate() {
            let (mut left, left_end) = self.incidence_range(endpoints[0]);
            let (mut right, right_end) = self.incidence_range(endpoints[1]);
            while left < left_end || right < right_end {
                let left_segment = (left < left_end).then(|| {
                    self.incidences
                        .get(left)
                        .expect("left cursor is below its incidence-row end")
                        .segment
                });
                let right_segment = (right < right_end).then(|| {
                    self.incidences
                        .get(right)
                        .expect("right cursor is below its incidence-row end")
                        .segment
                });
                let next = match (left_segment, right_segment) {
                    (Some(left), Some(right)) => left.min(right),
                    (Some(segment), None) | (None, Some(segment)) => segment,
                    (None, None) => unreachable!("a nonempty incidence merge has a head"),
                };
                if next > segment {
                    visit(segment, next)?;
                }
                if left_segment == Some(next) {
                    left = left.wrapping_add(1);
                }
                if right_segment == Some(next) {
                    right = right.wrapping_add(1);
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
struct MappedSourceEdge {
    ring: usize,
    edge: usize,
    segment: usize,
    endpoint: Option<usize>,
}

impl ClipTrace {
    pub(super) fn build(
        subdivision: &ExactSubdivision,
        clip: &NormalizedClip<'_>,
        budget: &mut EvaluationBudget,
    ) -> Result<Self> {
        let source_segment_count = clip.rings.iter().try_fold(0_usize, |count, ring| {
            count
                .checked_add(ring.len())
                .ok_or(crate::error::ResourceError::SizeOverflow)
        })?;
        u32::try_from(source_segment_count)
            .map_err(|_| crate::error::ResourceError::SizeOverflow)?;
        budget.charge_evaluations(
            u64::try_from(source_segment_count.div_ceil(16))
                .map_err(|_| crate::error::ResourceError::SizeOverflow)?,
        )?;

        let mut source_segments = Vec::with_capacity(source_segment_count);
        for (ring_index, ring) in clip.rings.iter().enumerate() {
            if ring.len() < 3 {
                return Err(clip_source_trace_error());
            }
            for (start, end) in ring.iter().zip(ring.iter().cycle().skip(1)) {
                let segment = u32::try_from(source_segments.len())
                    .map_err(|_| crate::error::ResourceError::SizeOverflow)?;
                let start_vertex = subdivision
                    .vertex_at(start)?
                    .expect("every normalized clip source endpoint is interned by noding");
                let end_vertex = subdivision
                    .vertex_at(end)?
                    .expect("every normalized clip source endpoint is interned by noding");
                source_segments.push(ClipSourceSegment {
                    segment,
                    ring: ring_index,
                    start,
                    end,
                    start_vertex,
                    end_vertex,
                });
            }
        }
        budget.charge_evaluations(
            u64::try_from(subdivision.edges.len().div_ceil(16))
                .map_err(|_| crate::error::ResourceError::SizeOverflow)?,
        )?;
        let source_atom_count = subdivision.edges.iter().try_fold(0_usize, |count, edge| {
            count
                .checked_add(edge.clip_sources.len())
                .ok_or(crate::error::ResourceError::SizeOverflow)
        })?;
        let source_atom_units = u64::try_from(source_atom_count.div_ceil(16))
            .map_err(|_| crate::error::ResourceError::SizeOverflow)?;
        budget.charge_evaluations(source_atom_units)?;
        let mut rows: Vec<_> = std::iter::repeat_with(ClipSourceRow::default)
            .take(source_segment_count)
            .collect();
        for (edge_index, edge) in subdivision.edges.iter().enumerate() {
            let edge_id = EdgeId(
                u32::try_from(edge_index).map_err(|_| crate::error::ResourceError::SizeOverflow)?,
            );
            for &source in &edge.clip_sources {
                let source = usize::try_from(source.0)
                    .map_err(|_| crate::error::ResourceError::SizeOverflow)?;
                let row = rows.get_mut(source).ok_or_else(clip_source_trace_error)?;
                row.vertices.extend(edge.endpoints);
                row.edges.push(edge_id);
            }
        }

        budget.charge_evaluations(source_atom_units)?;
        let mut rings: Vec<_> = std::iter::repeat_with(ClipTraceRing::default)
            .take(clip.rings.len())
            .collect();
        for (segment, row) in source_segments.iter().zip(&mut rows) {
            try_sort_by(&mut row.vertices, |&left, &right| {
                exact::incidence::compare_along(
                    segment.start,
                    segment.end,
                    &subdivision.vertex(left).point,
                    &subdivision.vertex(right).point,
                )
            })?;
            try_dedup_by(&mut row.vertices, |later, earlier| Ok(later == earlier))?;
            debug_assert_eq!(
                row.vertices.first(),
                Some(&segment.start_vertex),
                "a source trace starts at its noded endpoint"
            );
            debug_assert_eq!(
                row.vertices.last(),
                Some(&segment.end_vertex),
                "a source trace ends at its noded endpoint"
            );
            let expected_vertex_count = row
                .edges
                .len()
                .checked_add(1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            debug_assert_eq!(
                expected_vertex_count,
                row.vertices.len(),
                "each source atom joins consecutive noded vertices"
            );
            #[cfg(debug_assertions)]
            for &vertex in &row.vertices {
                let point = &subdivision.vertex(vertex).point;
                let intersection = exact::incidence::segment_intersection(
                    segment.start,
                    segment.end,
                    point,
                    point,
                )
                .expect("a noded clip vertex has an exact source-segment classification");
                debug_assert!(
                    matches!(intersection, exact::SegmentIntersection::Point(_)),
                    "each atom endpoint lies on its tagged source segment"
                );
            }

            let mut atoms = Vec::with_capacity(row.edges.len());
            for &edge in &row.edges {
                let [left, right] = subdivision.edge(edge).endpoints;
                let along = exact::incidence::compare_along(
                    segment.start,
                    segment.end,
                    &subdivision.vertex(left).point,
                    &subdivision.vertex(right).point,
                )?;
                debug_assert_ne!(
                    along,
                    Ordering::Equal,
                    "a source atom has distinct endpoints along its segment"
                );
                let reversed = along == Ordering::Greater;
                let (start, end) = if reversed {
                    (right, left)
                } else {
                    (left, right)
                };
                atoms.push(ClipTraceAtom {
                    edge,
                    reversed,
                    segment: segment.segment,
                    start_parameter: exact_segment_parameter(
                        segment.start,
                        segment.end,
                        &subdivision.vertex(start).point,
                    )?,
                    end_parameter: exact_segment_parameter(
                        segment.start,
                        segment.end,
                        &subdivision.vertex(end).point,
                    )?,
                });
            }
            try_sort_by(&mut atoms, |left, right| {
                let [left_start, left_end] = subdivision.edge(left.edge).endpoints;
                let [right_start, right_end] = subdivision.edge(right.edge).endpoints;
                let left_start = if left.reversed { left_end } else { left_start };
                let right_start = if right.reversed {
                    right_end
                } else {
                    right_start
                };
                exact::incidence::compare_along(
                    segment.start,
                    segment.end,
                    &subdivision.vertex(left_start).point,
                    &subdivision.vertex(right_start).point,
                )
            })?;
            for (index, atom) in atoms.iter().enumerate() {
                let [left, right] = subdivision.edge(atom.edge).endpoints;
                let (start, end) = if atom.reversed {
                    (right, left)
                } else {
                    (left, right)
                };
                let next_index = index
                    .checked_add(1)
                    .ok_or(crate::error::ResourceError::SizeOverflow)?;
                debug_assert_eq!(
                    row.vertices.get(index),
                    Some(&start),
                    "ordered source atoms begin at the corresponding trace vertex"
                );
                debug_assert_eq!(
                    row.vertices.get(next_index),
                    Some(&end),
                    "ordered source atoms end at the next trace vertex"
                );
                rings
                    .get_mut(segment.ring)
                    .ok_or_else(clip_source_trace_error)?
                    .atoms
                    .push(atom.clone());
            }
        }
        let atom_count = rings.iter().try_fold(0_usize, |count, ring| {
            count
                .checked_add(ring.atoms.len())
                .ok_or(crate::error::ResourceError::SizeOverflow)
        })?;
        debug_assert_eq!(
            atom_count, source_atom_count,
            "each source-tagged atomic edge appears in its clip ring trace"
        );
        Ok(Self { rings, atom_count })
    }
}

impl ClipRingParity {
    pub(super) fn new(ring_count: usize) -> Self {
        Self {
            generation: 0,
            generations: vec![0; ring_count],
            inside: vec![false; ring_count],
            shell_inside: false,
            holes_inside: 0,
        }
    }

    fn begin_query(&mut self) {
        self.generation = self.generation.wrapping_add(1);
        if self.generation == 0 {
            self.generations.fill(0);
            self.generation = 1;
        }
        self.shell_inside = false;
        self.holes_inside = 0;
    }

    fn toggle(&mut self, ring: usize) -> Result<()> {
        let known_generation = *self.generations.get(ring).ok_or_else(exact_cycle_error)?;
        let was_inside = known_generation == self.generation
            && *self.inside.get(ring).ok_or_else(exact_cycle_error)?;
        *self
            .generations
            .get_mut(ring)
            .ok_or_else(exact_cycle_error)? = self.generation;
        let is_inside = !was_inside;
        *self.inside.get_mut(ring).ok_or_else(exact_cycle_error)? = is_inside;
        if ring == 0 {
            self.shell_inside = is_inside;
        } else if is_inside {
            self.holes_inside = self
                .holes_inside
                .checked_add(1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
        } else {
            self.holes_inside = self
                .holes_inside
                .checked_sub(1)
                .ok_or_else(exact_cycle_error)?;
        }
        Ok(())
    }

    fn covers(&self) -> bool {
        self.shell_inside && self.holes_inside == 0
    }

    fn outside_ring(&self) -> Option<usize> {
        if !self.shell_inside {
            return Some(0);
        }
        self.generations
            .iter()
            .zip(&self.inside)
            .enumerate()
            .skip(1)
            .find_map(|(ring, (&generation, &inside))| {
                (generation == self.generation && inside).then_some(ring)
            })
    }
}

pub(super) fn clip_source_trace_error() -> crate::error::Error {
    voronoi_error("exact clip source atoms do not reconstruct the normalized clip boundary")
}

#[cfg(test)]
/// Reconstructs rounded source boxes for the exhaustive contact-work oracle.
pub(super) fn rounded_box<'a>(
    points: impl IntoIterator<Item = &'a exact::ExactPoint>,
) -> Result<Option<Bounds>> {
    let mut bounds: Option<Bounds> = None;
    for point in points {
        let Some(point) = point.finite_rounding()? else {
            return Ok(None);
        };
        match &mut bounds {
            Some(bounds) => bounds.include_xy(point),
            None => bounds = Some(Bounds::new_raw(point.x, point.y, point.x, point.y)),
        }
    }
    Ok(bounds)
}

/// Closed boxes indexed for point queries. A missing box contains every
/// point, and a missing point lies in every box.
pub(super) struct BoxIndex {
    index: YStabbingIndex<usize>,
    unbounded: Vec<usize>,
    count: usize,
}

pub(super) fn admit_box_ordinals(count: usize) -> crate::error::Result<()> {
    u32::try_from(count.saturating_sub(1))
        .map(|_| ())
        .map_err(|_| crate::error::ResourceError::SizeOverflow.into())
}

impl BoxIndex {
    #[expect(
        clippy::indexing_slicing,
        reason = "bounded item ordinals are collected from this exact boxes slice only when their bounds are present"
    )]
    pub(super) fn build(boxes: &[Option<Bounds>]) -> Result<Self> {
        admit_box_ordinals(boxes.len())?;
        let mut bounded = Vec::new();
        let mut unbounded = Vec::new();
        bounded.reserve(boxes.len());
        for (item, bounds) in boxes.iter().enumerate() {
            if bounds.is_some() {
                bounded.push(item);
            } else {
                unbounded.push(item);
            }
        }
        let index = YStabbingIndex::build(bounded, |&item| {
            let bounds = boxes[item]
                .as_ref()
                .expect("bounded box ordinals are collected only for present bounds");
            (bounds.minx(), bounds.maxx(), bounds.miny(), bounds.maxy())
        })?;
        Ok(Self {
            index,
            unbounded,
            count: boxes.len(),
        })
    }

    /// Visit every box containing `point`, in no particular order, stopping at
    /// the first error.
    pub(super) fn for_each_containing(
        &self,
        point: Option<XY>,
        mut visit: impl FnMut(usize) -> Result<()>,
    ) -> Result<()> {
        let Some(point) = point else {
            return (0..self.count).try_for_each(visit);
        };
        for &item in &self.unbounded {
            visit(item)?;
        }
        let stopped = self
            .index
            .for_each_at_point(point.x, point.y, |&item| match visit(item) {
                Ok(()) => ControlFlow::Continue(()),
                Err(error) => ControlFlow::Break(error),
            });
        stopped.map_or(Ok(()), Err)
    }
}
