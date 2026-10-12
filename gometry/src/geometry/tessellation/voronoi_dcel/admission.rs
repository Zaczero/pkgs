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
    polygon_is_valid, polygon_output_baseline, retained_site_longitude, sort_unique_along,
    try_dedup_by, try_hash_map_with_capacity, try_sort_by, uf_union, voronoi_error,
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
                exact::compare_along(
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
                let intersection =
                    exact::segment_intersection(segment.start, segment.end, point, point)
                        .expect("a noded clip vertex has an exact source-segment classification");
                debug_assert!(
                    matches!(intersection, exact::SegmentIntersection::Point(_)),
                    "each atom endpoint lies on its tagged source segment"
                );
            }

            let mut atoms = Vec::with_capacity(row.edges.len());
            for &edge in &row.edges {
                let [left, right] = subdivision.edge(edge).endpoints;
                let along = exact::compare_along(
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
                exact::compare_along(
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

pub(super) fn first_owner_cycle_failure(
    facts: &[OwnerCycleContainment],
    shell: usize,
) -> Option<(usize, OwnerCycleFailure)> {
    for (ring, fact) in facts.iter().enumerate() {
        if ring == shell {
            continue;
        }
        match fact.shell_position {
            OwnerShellPosition::Outside => {
                return Some((ring, OwnerCycleFailure::OutsideShell));
            },
            OwnerShellPosition::BoundaryOnly => {
                return Some((ring, OwnerCycleFailure::BoundaryOnly));
            },
            OwnerShellPosition::Inside => {},
        }
        if fact.nested_descendant {
            return Some((ring, OwnerCycleFailure::NestedHole));
        }
    }
    None
}

pub(super) fn validate_owner_cycle_facts(
    facts: &[OwnerCycleContainment],
    expected_cycles: usize,
    shell: usize,
) -> Result<()> {
    if facts.len() != expected_cycles || first_owner_cycle_failure(facts, shell).is_some() {
        return Err(exact_cycle_error());
    }
    Ok(())
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

pub(super) fn exact_cycle_error() -> crate::error::Error {
    voronoi_error("exact Voronoi cycle set is not a valid polygon boundary")
}

pub(super) fn admit_embedded_clip(polygon: &Polygon) -> Result<()> {
    if polygon_is_valid(polygon)? {
        Ok(())
    } else {
        Err(voronoi_error(
            "embedded Voronoi clip is not a valid polygon",
        ))
    }
}

#[expect(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    reason = "each corner index comes from the fixed four-entry range and each next-corner index wraps within that array"
)]
pub(super) fn validate_exact_rectangle(corners: &[exact::ExactPoint; 4]) -> Result<()> {
    for left_edge in 0..4 {
        let left_next = (left_edge + 1) % 4;
        let left = [&corners[left_edge], &corners[left_next]];
        for right_edge in left_edge + 1..4 {
            let right_next = (right_edge + 1) % 4;
            let right = [&corners[right_edge], &corners[right_next]];
            let adjacent = right_edge == left_edge + 1 || (left_edge == 0 && right_edge == 3);
            match exact::segment_intersection(left[0], left[1], right[0], right[1])? {
                exact::SegmentIntersection::None if !adjacent => {},
                exact::SegmentIntersection::Point(point) if adjacent => {
                    if position_of(&left, &point)?.is_none()
                        || position_of(&right, &point)?.is_none()
                    {
                        return Err(exact_cycle_error());
                    }
                },
                exact::SegmentIntersection::None
                | exact::SegmentIntersection::Point(_)
                | exact::SegmentIntersection::Overlap { .. } => {
                    return Err(exact_cycle_error());
                },
            }
        }
    }
    Ok(())
}

#[expect(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    reason = "binary search maintains low <= middle < high <= points.len(), so both the midpoint read and lower-bound advance remain in this admitted sorted column"
)]
fn exact_point_index(points: &[exact::ExactPoint], point: &exact::ExactPoint) -> Result<usize> {
    let mut low = 0;
    let mut high = points.len();
    while low < high {
        let middle = low.midpoint(high);
        match points[middle].compare_lex(point)? {
            Ordering::Less => {
                low = middle + 1;
            },
            Ordering::Equal => return Ok(middle),
            Ordering::Greater => high = middle,
        }
    }
    Err(exact_cycle_error())
}

fn validate_touch_wedge<'a>(
    point: &exact::ExactPoint,
    first_directions: impl IntoIterator<Item = &'a exact::ExactPoint>,
    second_directions: impl IntoIterator<Item = &'a exact::ExactPoint>,
    rays: &mut Vec<(usize, &'a exact::ExactPoint)>,
) -> Result<()> {
    rays.clear();
    rays.extend(first_directions.into_iter().map(|target| (0, target)));
    rays.extend(second_directions.into_iter().map(|target| (1, target)));
    let angle = |left: &exact::ExactPoint, right: &exact::ExactPoint| {
        exact::angle_ccw_cmp(point, left, right).map_err(|_| exact_cycle_error())
    };
    try_sort_by(rays, |left, right| angle(left.1, right.1))?;
    for [left, right] in rays.array_windows::<2>() {
        angle(left.1, right.1)?;
    }
    if rays.len() > 1 {
        let (Some(first), Some(last)) = (rays.first(), rays.last()) else {
            return Err(exact_cycle_error());
        };
        angle(last.1, first.1)?;
    }
    let changes = rays
        .iter()
        .zip(rays.iter().cycle().skip(1))
        .filter(|(left, right)| left.0 != right.0)
        .count();
    if changes == 2 {
        Ok(())
    } else {
        Err(exact_cycle_error())
    }
}

fn validate_exact_contact_graph(
    cycles: &[&[exact::ExactPoint]],
    shell: usize,
    pair_touches: &[ExactPairTouch],
) -> Result<()> {
    if cycles.is_empty() || shell >= cycles.len() || cycles.iter().any(|cycle| cycle.len() < 3) {
        return Err(exact_cycle_error());
    }
    let mut rays = Vec::new();
    for touch in pair_touches {
        let [first_directions, second_directions] = &touch.directions;
        let ray_capacity = first_directions
            .len()
            .checked_add(second_directions.len())
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        if rays.capacity() < ray_capacity {
            rays.clear();
            rays.reserve(ray_capacity);
        }
        validate_touch_wedge(
            &touch.point,
            first_directions.iter(),
            second_directions.iter(),
            &mut rays,
        )?;
    }
    let mut touch_points = Vec::with_capacity(pair_touches.len());
    touch_points.extend(pair_touches.iter().map(|touch| touch.point.clone()));
    try_sort_by(&mut touch_points, exact::ExactPoint::compare_lex)?;
    try_dedup_by(&mut touch_points, exact::ExactPoint::same_position)?;
    let mut forest = ExactTouchForest::new(cycles.len(), touch_points.len())?;
    let mut incidences = Vec::new();
    for touch in pair_touches {
        let point = exact_point_index(&touch_points, &touch.point)?;
        for ring in touch.rings {
            incidences.push((point, ring));
        }
    }
    incidences.sort_unstable();
    incidences.dedup();
    for (point, ring) in incidences {
        forest.attach(point, ring)?;
    }
    Ok(())
}

pub(super) fn for_each_touch_fan(
    index: &NodedSegmentIndex,
    mut visit: impl FnMut(usize, VertexId, usize, usize, usize) -> Result<()>,
) -> Result<usize> {
    let mut cursor = 0_usize;
    let mut touch_index = 0_usize;
    while cursor < index.incidences.len() {
        let vertex = index
            .incidences
            .get(cursor)
            .ok_or_else(exact_cycle_error)?
            .vertex;
        let start = cursor;
        let mut end = start.wrapping_add(1);
        while end < index.incidences.len()
            && index
                .incidences
                .get(end)
                .ok_or_else(exact_cycle_error)?
                .vertex
                == vertex
        {
            end = end.wrapping_add(1);
        }

        let mut ring_count = 0_usize;
        let mut ring_start = start;
        while ring_start < end {
            let ring = index
                .incidences
                .get(ring_start)
                .ok_or_else(exact_cycle_error)?
                .ring;
            let mut ring_end = ring_start.wrapping_add(1);
            while ring_end < end
                && index
                    .incidences
                    .get(ring_end)
                    .ok_or_else(exact_cycle_error)?
                    .ring
                    == ring
            {
                ring_end = ring_end.wrapping_add(1);
            }
            if ring_end.wrapping_sub(ring_start) != 2 {
                return Err(exact_cycle_error());
            }
            ring_count = ring_count
                .checked_add(1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            ring_start = ring_end;
        }
        if ring_count > 1 {
            visit(touch_index, vertex, start, end, ring_count)?;
            touch_index = touch_index
                .checked_add(1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
        }
        cursor = end;
    }
    Ok(touch_index)
}

fn validate_assembled_touch_fans(
    subdivision: &ExactSubdivision,
    cycle_count: usize,
    index: &NodedSegmentIndex,
) -> Result<()> {
    let touch_count = for_each_touch_fan(index, |_, _, _, _, _| Ok(()))?;
    let mut rays = Vec::with_capacity(4);

    for_each_touch_fan(index, |_, vertex, start, end, ring_count| {
        let touch = &subdivision.vertex(vertex).point;
        let mut left = start;
        for left_group in 0..ring_count {
            let left_first = index.incidences.get(left).ok_or_else(exact_cycle_error)?;
            let left_second = index
                .incidences
                .get(left.wrapping_add(1))
                .ok_or_else(exact_cycle_error)?;
            let first_directions = [
                &subdivision.vertex(left_first.neighbor).point,
                &subdivision.vertex(left_second.neighbor).point,
            ];
            let mut right = left.wrapping_add(2);
            for _ in left_group.wrapping_add(1)..ring_count {
                let right_first = index.incidences.get(right).ok_or_else(exact_cycle_error)?;
                let right_second = index
                    .incidences
                    .get(right.wrapping_add(1))
                    .ok_or_else(exact_cycle_error)?;
                let second_directions = [
                    &subdivision.vertex(right_first.neighbor).point,
                    &subdivision.vertex(right_second.neighbor).point,
                ];
                validate_touch_wedge(touch, first_directions, second_directions, &mut rays)?;
                right = right.wrapping_add(2);
            }
            left = left.wrapping_add(2);
        }
        if left != end {
            return Err(exact_cycle_error());
        }
        Ok(())
    })?;

    let mut forest = ExactTouchForest::new(cycle_count, touch_count)?;
    for_each_touch_fan(index, |touch_index, _, start, end, _| {
        let mut cursor = start;
        while cursor < end {
            let ring = index
                .incidences
                .get(cursor)
                .ok_or_else(exact_cycle_error)?
                .ring;
            forest.attach(touch_index, ring)?;
            cursor = cursor.wrapping_add(2);
        }
        Ok(())
    })?;
    Ok(())
}

#[expect(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    reason = "segment ordinals are admitted by offsets and each cycle's closed next-edge index wraps within its nonzero point slice"
)]
fn classify_exact_segment_pair(
    cycles: &[&[exact::ExactPoint]],
    offsets: &[usize],
    [left_segment, right_segment]: [usize; 2],
    noded_endpoint_pair: bool,
) -> Result<Option<ExactCycleContact>> {
    let left_ring = offsets.partition_point(|offset| *offset <= left_segment) - 1;
    let right_ring = offsets.partition_point(|offset| *offset <= right_segment) - 1;
    let left_edge = left_segment - offsets[left_ring];
    let right_edge = right_segment - offsets[right_ring];
    let left_cycle = cycles[left_ring];
    let left_next = left_edge
        .checked_add(1)
        .filter(|&next| next < left_cycle.len())
        .unwrap_or(0);
    let left = [&left_cycle[left_edge], &left_cycle[left_next]];
    let right_cycle = cycles[right_ring];
    let right_next = right_edge
        .checked_add(1)
        .filter(|&next| next < right_cycle.len())
        .unwrap_or(0);
    let right = [&right_cycle[right_edge], &right_cycle[right_next]];
    let adjacent = left_ring == right_ring
        && (right_edge == left_edge + 1 || (left_edge == 0 && right_edge + 1 == left_cycle.len()));
    match exact::segment_intersection(left[0], left[1], right[0], right[1])? {
        exact::SegmentIntersection::None if noded_endpoint_pair => Err(exact_cycle_error()),
        exact::SegmentIntersection::None => Ok(None),
        exact::SegmentIntersection::Overlap { .. } => Err(exact_cycle_error()),
        exact::SegmentIntersection::Point(point) => {
            let endpoints = [position_of(&left, &point)?, position_of(&right, &point)?];
            if adjacent {
                if endpoints[0].is_none() || endpoints[1].is_none() {
                    return Err(exact_cycle_error());
                }
                return Ok(None);
            }
            if left_ring == right_ring
                || (endpoints[0].is_none() && endpoints[1].is_none())
                || (noded_endpoint_pair && (endpoints[0].is_none() || endpoints[1].is_none()))
            {
                return Err(exact_cycle_error());
            }
            Ok(Some(ExactCycleContact {
                segments: [left_segment, right_segment],
                point,
            }))
        },
    }
}

pub(super) fn for_each_assembled_pair(
    cycles: &[&[exact::ExactPoint]],
    index: &NodedSegmentIndex,
    mut visit: impl FnMut([usize; 2], Option<ExactCycleContact>) -> Result<()>,
) -> Result<()> {
    index.for_each_source_pair(|left_segment, right_segment| {
        let contact = classify_exact_segment_pair(
            cycles,
            &index.offsets,
            [left_segment, right_segment],
            true,
        )?;
        visit([left_segment, right_segment], contact)
    })
}

/// The nesting step runs only after noded contact and touch validation succeeds.
pub(super) fn validate_assembled_exact_cycles(
    subdivision: &ExactSubdivision,
    cycles: &[NodedExactCycle<'_>],
    shell: usize,
    validate_nesting: impl FnOnce() -> Result<()>,
) -> Result<()> {
    let mut points = Vec::with_capacity(cycles.len());
    points.extend(cycles.iter().map(|cycle| cycle.points));
    validate_exact_cycle_inputs(&points, shell)?;
    validate_exact_cycle_rounding(&points)?;
    let index = NodedSegmentIndex::new(cycles)?;
    for_each_assembled_pair(&points, &index, |_, _| Ok(()))?;
    validate_assembled_touch_fans(subdivision, cycles.len(), &index)?;
    validate_nesting()
}

#[cfg(test)]
pub(super) fn validate_exact_cycles(
    cycles: &[&[exact::ExactPoint]],
    shell: usize,
) -> Result<ValidatedExactCycles> {
    validate_exact_cycle_inputs(cycles, shell)?;
    let offsets = exact_cycle_offsets(cycles)?;
    validate_exact_cycle_rounding(cycles)?;

    let segment_count = *offsets
        .last()
        .expect("cycle offsets contain their terminal value");
    let mut contacts = Vec::new();
    let mut pair_touches = Vec::new();
    for left_segment in 0..segment_count {
        let right_start = left_segment.wrapping_add(1);
        for right_segment in right_start..segment_count {
            let Some(contact) = classify_exact_segment_pair(
                cycles,
                &offsets,
                [left_segment, right_segment],
                false,
            )?
            else {
                continue;
            };
            record_generic_exact_touch(cycles, &offsets, &contact, &mut pair_touches)?;
            contacts.push(contact);
        }
    }
    validate_exact_contact_graph(cycles, shell, &pair_touches)?;
    validate_exact_cycle_nesting(cycles, shell)?;
    try_sort_by(&mut contacts, |left, right| {
        Ok(match left.segments.cmp(&right.segments) {
            Ordering::Equal => left.point.compare_lex(&right.point)?,
            Ordering::Less => Ordering::Less,
            Ordering::Greater => Ordering::Greater,
        })
    })?;
    try_dedup_by(&mut contacts, |later, earlier| {
        Ok(later.segments == earlier.segments && later.point.same_position(&earlier.point)?)
    })?;
    Ok(ValidatedExactCycles { contacts })
}

fn validate_exact_cycle_inputs(cycles: &[&[exact::ExactPoint]], shell: usize) -> Result<()> {
    if cycles.is_empty() || shell >= cycles.len() || cycles.iter().any(|cycle| cycle.len() < 3) {
        return Err(exact_cycle_error());
    }

    Ok(())
}

#[cfg(test)]
fn exact_cycle_offsets(cycles: &[&[exact::ExactPoint]]) -> Result<Vec<usize>> {
    let offset_capacity = cycles
        .len()
        .checked_add(1)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    let mut offsets = Vec::with_capacity(offset_capacity);
    offsets.push(0_usize);
    for cycle in cycles {
        let offset = offsets
            .last()
            .expect("cycle offsets contain their initial value")
            .checked_add(cycle.len())
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        offsets.push(offset);
    }
    Ok(offsets)
}

fn validate_exact_cycle_rounding(cycles: &[&[exact::ExactPoint]]) -> Result<()> {
    for cycle in cycles {
        for point in *cycle {
            let _ = point.finite_rounding()?;
        }
    }
    Ok(())
}

#[cfg(test)]
pub(super) fn validate_exact_cycle_nesting(
    cycles: &[&[exact::ExactPoint]],
    shell: usize,
) -> Result<()> {
    let shell_cycle = cycles.get(shell).copied().ok_or_else(exact_cycle_error)?;
    for (hole, hole_cycle) in cycles.iter().enumerate() {
        if hole == shell {
            continue;
        }
        let mut saw_inside = false;
        for point in *hole_cycle {
            match exact::point_in_cycle(shell_cycle, point)? {
                exact::PointInCycle::Outside => return Err(exact_cycle_error()),
                exact::PointInCycle::Inside => saw_inside = true,
                exact::PointInCycle::Boundary => {},
            }
        }
        if !saw_inside {
            return Err(exact_cycle_error());
        }
        for (other, other_cycle) in cycles.iter().enumerate() {
            if other == shell || other == hole {
                continue;
            }
            for point in *other_cycle {
                if exact::point_in_cycle(hole_cycle, point)? == exact::PointInCycle::Inside {
                    return Err(exact_cycle_error());
                }
            }
        }
    }
    Ok(())
}

/// The index of the first point at `point`'s exact position.
fn position_of<P: std::borrow::Borrow<exact::ExactPoint>>(
    points: &[P],
    point: &exact::ExactPoint,
) -> Result<Option<usize>> {
    for (index, candidate) in points.iter().enumerate() {
        if candidate.borrow().same_position(point)? {
            return Ok(Some(index));
        }
    }
    Ok(None)
}

#[cfg(test)]
fn record_generic_exact_touch(
    cycles: &[&[exact::ExactPoint]],
    offsets: &[usize],
    contact: &ExactCycleContact,
    pair_touches: &mut Vec<ExactPairTouch>,
) -> Result<()> {
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "contact segment ordinals came from the exhaustive pair loop and map through the admitted cycle offsets"
    )]
    fn source_edge<'a>(
        cycles: &'a [&'a [exact::ExactPoint]],
        offsets: &[usize],
        segment: usize,
    ) -> (usize, usize, [&'a exact::ExactPoint; 2]) {
        let ring = offsets.partition_point(|offset| *offset <= segment) - 1;
        let edge = segment - offsets[ring];
        let cycle = cycles[ring];
        let next = edge
            .checked_add(1)
            .filter(|&next| next < cycle.len())
            .unwrap_or(0);
        (ring, edge, [&cycle[edge], &cycle[next]])
    }

    let [left_segment, right_segment] = contact.segments;
    let (left_ring, _, left) = source_edge(cycles, offsets, left_segment);
    let (right_ring, _, right) = source_edge(cycles, offsets, right_segment);
    let endpoints = [
        position_of(&left, &contact.point)?,
        position_of(&right, &contact.point)?,
    ];
    let rings = [left_ring, right_ring];
    let mut known_touch = None;
    for (index, touch) in pair_touches.iter().enumerate() {
        if touch.rings == rings && touch.point.same_position(&contact.point)? {
            known_touch = Some(index);
            break;
        }
    }
    if let Some(index) = known_touch {
        let touch = pair_touches.get_mut(index).ok_or_else(exact_cycle_error)?;
        for (directions, (segment, endpoint)) in touch
            .directions
            .iter_mut()
            .zip([(left, endpoints[0]), (right, endpoints[1])])
        {
            for (index, direction) in segment.into_iter().enumerate() {
                if endpoint == Some(index) || position_of(directions, direction)?.is_some() {
                    continue;
                }
                directions.push(direction.clone());
            }
        }
    } else {
        let mut directions = [Vec::new(), Vec::new()];
        for (ring_directions, (segment, endpoint)) in directions
            .iter_mut()
            .zip([(left, endpoints[0]), (right, endpoints[1])])
        {
            for (index, direction) in segment.into_iter().enumerate() {
                if endpoint.is_none_or(|endpoint| index != endpoint) {
                    ring_directions.push(direction.clone());
                }
            }
        }
        pair_touches.push(ExactPairTouch {
            rings,
            point: contact.point.clone(),
            directions,
        });
    }
    Ok(())
}

pub(super) fn exact_chart_point(
    chart: Option<&PointSelectionChart>,
    point: XY,
) -> Result<exact::ExactPoint> {
    let Some(chart) = chart else {
        return Ok(exact::ExactPoint::from_xy(point));
    };
    let frame = chart.frame();
    let longitude = chart.longitude_site(point.x)?;
    Ok(exact::ExactPoint::from_dyadics(
        retained_site_longitude(frame.longitude(), longitude)?,
        exact::ExactDyadic::from_f64(point.y),
    ))
}

#[expect(
    clippy::arithmetic_side_effects,
    reason = "Ring guarantees at least four closed coordinates, so subtracting its explicit closing point is safe"
)]
pub(super) fn open_ring(ring: &Ring, chart: Option<&PointSelectionChart>) -> Result<OpenClipRing> {
    let open = ring.len() - 1;
    let mut exact_points = Vec::new();
    let mut fixed = Vec::new();
    let mut chart_fixed = Vec::new();
    let mut vertex_indices = Vec::with_capacity(open);
    exact_points.reserve(open);
    fixed.reserve(open);
    for point in ring.iter().take(open) {
        let point = point.xy();
        let exact = exact_chart_point(chart, point)?;
        if let Some(previous) = exact_points.last()
            && exact.same_position(previous)?
        {
            vertex_indices.push(
                exact_points
                    .len()
                    .checked_sub(1)
                    .expect("a repeated point follows an admitted ring vertex"),
            );
            continue;
        }
        vertex_indices.push(exact_points.len());
        chart_fixed.push(exact.round_nearest_even()?);
        exact_points.push(exact);
        fixed.push(point);
    }
    if exact_points.len() < 3 {
        return Err(voronoi_error(
            "exact Voronoi clip ring has fewer than three vertices",
        ));
    }
    let source_edges = vertex_indices
        .iter()
        .copied()
        .zip(vertex_indices.iter().copied().cycle().skip(1))
        .take(open)
        .map(|(start, end)| (start != end).then_some(start))
        .collect();
    Ok((exact_points, fixed, chart_fixed, source_edges))
}

fn reverse_ring(points: &mut [exact::ExactPoint], fixed: &mut [XY], chart_fixed: &mut [XY]) {
    points.reverse();
    fixed.reverse();
    chart_fixed.reverse();
}

#[expect(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    reason = "least cyclic rotation compares random offsets modulo this admitted nonempty ring; direct access preserves constant-time candidate steps without a linear nth lookup"
)]
pub(super) fn rotate_ring(
    points: &mut [exact::ExactPoint],
    fixed: &mut [XY],
    chart_fixed: &mut [XY],
) -> Result<usize> {
    let count = points.len();
    let mut least = 0;
    for candidate in 1..count {
        for offset in 0..count {
            match points[(candidate + offset) % count]
                .compare_lex(&points[(least + offset) % count])?
            {
                Ordering::Less => least = candidate,
                Ordering::Greater => {},
                Ordering::Equal => continue,
            }
            break;
        }
    }
    points.rotate_left(least);
    fixed.rotate_left(least);
    chart_fixed.rotate_left(least);
    Ok(least)
}

#[expect(
    clippy::arithmetic_side_effects,
    reason = "source edge and rotation are admitted ring ordinals modulo its nonzero length"
)]
fn normalized_source_edge(edge: usize, count: usize, rotation: usize, reversed: bool) -> usize {
    // Source edges are ordinals modulo the admitted nonempty ring length.
    let oriented = if reversed {
        count
            .wrapping_sub(1)
            .wrapping_sub(edge.wrapping_add(1) % count)
    } else {
        edge
    };
    oriented.wrapping_add(count).wrapping_sub(rotation) % count
}

fn map_source_polygon_contacts(
    source_contacts: &[crate::geometry::PolygonContact],
    source_edges: &[Vec<Option<usize>>],
    source_rings: &[Option<NormalizedSourceRing>],
    rings: &[Vec<exact::ExactPoint>],
    fixed: &[Vec<XY>],
    chart: Option<&PointSelectionChart>,
) -> Result<(Vec<ExactCycleContact>, Vec<ExactPairTouch>)> {
    let mut contacts = Vec::with_capacity(source_contacts.len());
    let mut pair_touches = Vec::new();
    let mut touch_index = try_hash_map_with_capacity(source_contacts.len())?;
    for source in source_contacts {
        let [first_ring, second_ring] = source.rings;
        let [first_edge, second_edge] = source.edges;
        let [first_parameter, second_parameter] = &source.parameters;
        let first = map_source_polygon_edge(
            first_ring,
            first_edge,
            first_parameter,
            source.point,
            source_edges,
            source_rings,
            rings,
            fixed,
        )?;
        let second = map_source_polygon_edge(
            second_ring,
            second_edge,
            second_parameter,
            source.point,
            source_edges,
            source_rings,
            rings,
            fixed,
        )?;
        if first.ring == second.ring || (first.endpoint.is_none() && second.endpoint.is_none()) {
            return Err(exact_cycle_error());
        }
        let [first, second] = if first.ring > second.ring {
            [second, first]
        } else {
            [first, second]
        };
        let ring_pair = [first.ring, second.ring];
        let segments = [first.segment, second.segment];
        let point = exact_chart_point(chart, source.point)?;
        contacts.push(ExactCycleContact {
            segments,
            point: point.clone(),
        });
        let key = (ring_pair, PointKey::new(source.point));
        let touch = match touch_index.entry(key) {
            std::collections::hash_map::Entry::Occupied(entry) => *entry.get(),
            std::collections::hash_map::Entry::Vacant(entry) => {
                let index = pair_touches.len();
                pair_touches.push(ExactPairTouch {
                    rings: ring_pair,
                    point,
                    directions: [Vec::new(), Vec::new()],
                });
                entry.insert(index);
                index
            },
        };
        let pair_touch = pair_touches.get_mut(touch).ok_or_else(exact_cycle_error)?;
        let [first_directions, second_directions] = &mut pair_touch.directions;
        for (edge, directions) in [(first, first_directions), (second, second_directions)] {
            let cycle = rings.get(edge.ring).ok_or_else(exact_cycle_error)?;
            let start = cycle.get(edge.edge).ok_or_else(exact_cycle_error)?;
            let next_edge = edge
                .edge
                .checked_add(1)
                .filter(|&next| next < cycle.len())
                .unwrap_or(0);
            let end = cycle.get(next_edge).ok_or_else(exact_cycle_error)?;
            for (index, direction) in [start, end].into_iter().enumerate() {
                if edge.endpoint == Some(index) {
                    continue;
                }
                directions.push(direction.clone());
            }
        }
    }
    for touch in &mut pair_touches {
        for directions in &mut touch.directions {
            try_sort_by(directions, exact::ExactPoint::compare_lex)?;
            try_dedup_by(directions, exact::ExactPoint::same_position)?;
        }
    }
    try_sort_by(&mut contacts, |left, right| {
        Ok(match left.segments.cmp(&right.segments) {
            Ordering::Equal => left.point.compare_lex(&right.point)?,
            Ordering::Less => Ordering::Less,
            Ordering::Greater => Ordering::Greater,
        })
    })?;
    try_dedup_by(&mut contacts, |later, earlier| {
        Ok(later.segments == earlier.segments && later.point.same_position(&earlier.point)?)
    })?;
    Ok((contacts, pair_touches))
}

fn map_source_polygon_edge(
    source_ring: usize,
    source_edge: usize,
    source_parameter: &SegmentParameter,
    source_point: XY,
    source_edges: &[Vec<Option<usize>>],
    source_rings: &[Option<NormalizedSourceRing>],
    rings: &[Vec<exact::ExactPoint>],
    fixed: &[Vec<XY>],
) -> Result<MappedSourceEdge> {
    let normalization = source_rings
        .get(source_ring)
        .copied()
        .flatten()
        .ok_or_else(exact_cycle_error)?;
    let edge = source_edges
        .get(source_ring)
        .and_then(|edges| edges.get(source_edge))
        .copied()
        .flatten()
        .ok_or_else(exact_cycle_error)?;
    let parameter = exact_source_parameter(source_parameter)?;
    let parameter = if normalization.reversed {
        exact::ExactRational::one().try_subtract(&parameter)?
    } else {
        parameter
    };
    let normalized_ring = rings
        .get(normalization.ring)
        .ok_or_else(exact_cycle_error)?;
    let normalized_coordinates = fixed
        .get(normalization.ring)
        .ok_or_else(exact_cycle_error)?;
    if normalized_ring.len() != normalized_coordinates.len() || normalized_ring.is_empty() {
        return Err(exact_cycle_error());
    }
    let start = *normalized_coordinates
        .get(edge)
        .ok_or_else(exact_cycle_error)?;
    let next_edge = edge
        .checked_add(1)
        .filter(|&next| next < normalized_coordinates.len())
        .unwrap_or(0);
    let end = *normalized_coordinates
        .get(next_edge)
        .ok_or_else(exact_cycle_error)?;
    let rounded = parameter.round_segment(start, end)?;
    if !crate::geometry::same_point(rounded, source_point) {
        return Err(exact_cycle_error());
    }
    let endpoint = if parameter.is_zero() {
        Some(0)
    } else if parameter.is_one() {
        Some(1)
    } else {
        None
    };
    let segment = normalization
        .segment_offset
        .checked_add(edge)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    Ok(MappedSourceEdge {
        ring: normalization.ring,
        edge,
        segment,
        endpoint,
    })
}

fn exact_source_parameter(parameter: &SegmentParameter) -> Result<exact::ExactRational> {
    match parameter {
        SegmentParameter::Start => Ok(exact::ExactRational::zero()),
        SegmentParameter::End => Ok(exact::ExactRational::one()),
        SegmentParameter::Ratio { num, den } => exact::ExactRational::new(num.clone(), den.clone()),
    }
}

pub(super) fn exact_segment_parameter(
    start: &exact::ExactPoint,
    end: &exact::ExactPoint,
    point: &exact::ExactPoint,
) -> Result<exact::ExactRational> {
    let one = exact::ExactDyadic::one();
    let zero = exact::ExactDyadic::zero();
    let x = |point: &exact::ExactPoint| point.projected_coordinate(&one, &zero);
    let y = |point: &exact::ExactPoint| point.projected_coordinate(&zero, &one);
    let start_x = x(start)?;
    let end_x = x(end)?;
    let point_x = x(point)?;
    let denominator_x = end_x.try_subtract(&start_x)?;
    if !denominator_x.is_zero() {
        return point_x.try_subtract(&start_x)?.try_quotient(&denominator_x);
    }
    let start_y = y(start)?;
    let end_y = y(end)?;
    let point_y = y(point)?;
    let denominator_y = end_y.try_subtract(&start_y)?;
    if denominator_y.is_zero() {
        return Err(clip_source_trace_error());
    }
    point_y.try_subtract(&start_y)?.try_quotient(&denominator_y)
}

fn polygon_from_open_xy_rings(rings: &[Vec<XY>]) -> Result<Polygon> {
    let mut rings = rings.iter();
    let make_ring = |open: &[XY]| -> Result<Ring> {
        let first = *open.first().ok_or_else(exact_cycle_error)?;
        let mut closed = Vec::with_capacity(open.len().saturating_add(1));
        closed.extend_from_slice(open);
        closed.push(first);
        Ring::try_from_closed(CoordSeq::try_from_xy(&closed)?)
    };
    let shell = make_ring(rings.next().ok_or_else(exact_cycle_error)?)?;
    let holes = rings
        .map(|ring| make_ring(ring))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(Polygon::new(shell, holes))
}

#[cfg(test)]
pub(super) fn normalize_clip<'a>(
    sites: &[Site],
    boundary: VoronoiBoundary<'a>,
    polygon_output: bool,
) -> Result<NormalizedClip<'a>> {
    normalize_clip_with_chart(sites, boundary, polygon_output, None)
}

pub(super) fn normalize_clip_with_chart<'a>(
    sites: &[Site],
    boundary: VoronoiBoundary<'a>,
    polygon_output: bool,
    chart: Option<&PointSelectionChart>,
) -> Result<NormalizedClip<'a>> {
    let mut rings = Vec::new();
    let mut fixed = Vec::new();
    let mut chart_fixed = Vec::new();
    let mut source_roots;
    let mut contacts = Vec::new();
    let mut polygon_baseline = None;
    let source = match boundary {
        VoronoiBoundary::Polygon(polygon) => {
            let mut polygon_contacts = Vec::new();
            if polygon
                .validate_with_contacts("polygon", "$", &mut polygon_contacts)?
                .is_some()
            {
                return Err(voronoi_error("Voronoi clip polygon is invalid"));
            }
            polygon_baseline = polygon_output
                .then(|| polygon_output_baseline(polygon))
                .transpose()?;
            let clip_ring_count = polygon
                .holes
                .len()
                .checked_add(1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            let source_point_rings: Vec<Vec<Point>> = std::iter::once(&polygon.shell)
                .chain(polygon.holes.iter())
                .map(|ring| ring.iter().collect())
                .collect();
            let mut clip_rings = Vec::with_capacity(clip_ring_count);
            for (ring_index, ring) in std::iter::once(&polygon.shell)
                .chain(polygon.holes.iter())
                .enumerate()
            {
                let (mut points, mut coordinates, mut chart_coordinates, mut source_edges) =
                    open_ring(ring, chart)?;
                let orientation = exact::cycle_orientation(&points)?;
                let wanted = if ring_index == 0 {
                    ExactSign::Positive
                } else {
                    ExactSign::Negative
                };
                let reversed = orientation != wanted;
                if reversed {
                    reverse_ring(&mut points, &mut coordinates, &mut chart_coordinates);
                }
                let rotation = rotate_ring(&mut points, &mut coordinates, &mut chart_coordinates)?;
                let count = points.len();
                for edge in source_edges.iter_mut().flatten() {
                    *edge = normalized_source_edge(*edge, count, rotation, reversed);
                }
                clip_rings.push((
                    ring_index,
                    points,
                    coordinates,
                    chart_coordinates,
                    source_edges,
                    reversed,
                ));
            }
            let (_, holes) = clip_rings
                .split_first_mut()
                .expect("the normalized clip ring sequence begins with its shell");
            try_sort_by(holes, |(_, left, ..), (_, right, ..)| {
                compare_exact_sequences(left, right)
            })?;
            rings.reserve(clip_rings.len());
            fixed.reserve(clip_rings.len());
            chart_fixed.reserve(clip_rings.len());
            let mut source_edge_map = vec![Vec::new(); clip_rings.len()];
            let mut source_ring_map = vec![None; clip_rings.len()];
            let mut segment_offset = 0_usize;
            for (source_ring, points, coordinates, chart_coordinates, edges, reversed) in clip_rings
            {
                let normalized_ring = rings.len();
                *source_edge_map
                    .get_mut(source_ring)
                    .ok_or_else(exact_cycle_error)? = edges;
                *source_ring_map
                    .get_mut(source_ring)
                    .ok_or_else(exact_cycle_error)? = Some(NormalizedSourceRing {
                    ring: normalized_ring,
                    segment_offset,
                    reversed,
                });
                segment_offset = segment_offset
                    .checked_add(points.len())
                    .ok_or(crate::error::ResourceError::SizeOverflow)?;
                rings.push(points);
                fixed.push(coordinates);
                chart_fixed.push(chart_coordinates);
            }
            source_roots = vec![Vec::new(); segment_offset];
            for (source_ring, edges) in source_edge_map.iter().enumerate() {
                let normalization = source_ring_map
                    .get(source_ring)
                    .copied()
                    .flatten()
                    .ok_or_else(exact_cycle_error)?;
                let source_points = source_point_rings
                    .get(source_ring)
                    .ok_or_else(exact_cycle_error)?;
                for (source_edge, &normalized_edge) in edges.iter().enumerate() {
                    let Some(normalized_edge) = normalized_edge else {
                        continue;
                    };
                    let source_end = source_edge
                        .checked_add(1)
                        .ok_or(crate::error::ResourceError::SizeOverflow)?;
                    let start = *source_points
                        .get(source_edge)
                        .ok_or_else(exact_cycle_error)?;
                    let end = *source_points
                        .get(source_end)
                        .ok_or_else(exact_cycle_error)?;
                    let segment = normalization
                        .segment_offset
                        .checked_add(normalized_edge)
                        .ok_or(crate::error::ResourceError::SizeOverflow)?;
                    source_roots
                        .get_mut(segment)
                        .ok_or_else(exact_cycle_error)?
                        .push(VoronoiClipSourceRoot {
                            ring: source_ring,
                            edge: source_edge,
                            reversed: normalization.reversed,
                            start,
                            end,
                        });
                }
            }
            let (source_cycle_contacts, pair_touches) = map_source_polygon_contacts(
                &polygon_contacts,
                &source_edge_map,
                &source_ring_map,
                &rings,
                &fixed,
                chart,
            )?;
            let mut open = Vec::with_capacity(rings.len());
            open.extend(rings.iter().map(Vec::as_slice));
            validate_exact_contact_graph(&open, 0, &pair_touches)?;
            contacts = source_cycle_contacts;
            Some((polygon, polygon_from_open_xy_rings(&chart_fixed)?))
        },
        VoronoiBoundary::Envelope | VoronoiBoundary::Padded => {
            let points: Vec<_> = sites.iter().map(|site| &site.exact_point).collect();
            let rectangle = exact::rectangular_boundary_for_exact(
                &points,
                matches!(boundary, VoronoiBoundary::Padded),
            )?;
            let mut corners = Vec::with_capacity(rectangle.len());
            for corner in &rectangle {
                corners.push(corner.round_nearest_even()?);
            }
            validate_exact_rectangle(&rectangle)?;
            let mut ring = Vec::with_capacity(rectangle.len());
            ring.extend(rectangle);
            rings.reserve(1);
            fixed.reserve(1);
            chart_fixed.reserve(1);
            source_roots = vec![Vec::new(); ring.len()];
            rings.push(ring);
            fixed.push(corners.clone());
            chart_fixed.push(corners);
            None
        },
    };
    let (source, geometry) = source.map_or((None, None), |(source, geometry)| {
        (Some(source), Some(geometry))
    });
    Ok(NormalizedClip {
        rings,
        fixed,
        chart_fixed,
        source_roots,
        source,
        geometry,
        polygon_baseline,
        contacts,
    })
}

fn compare_exact_sequences(
    left: &[exact::ExactPoint],
    right: &[exact::ExactPoint],
) -> Result<Ordering> {
    for (left, right) in left.iter().zip(right) {
        let order = left.compare_lex(right)?;
        if order.is_ne() {
            return Ok(order);
        }
    }
    Ok(left.len().cmp(&right.len()))
}

fn point_on_segment_xy(point: XY, start: XY, end: XY) -> bool {
    point.x >= start.x.min(end.x)
        && point.x <= start.x.max(end.x)
        && point.y >= start.y.min(end.y)
        && point.y <= start.y.max(end.y)
        && exact::orient2d(start, end, point) == ExactSign::Zero
}

fn exact_point_covered_by_clip_index<A: SearchAccount>(
    point: &exact::ExactPoint,
    edges: &[ClipCertificateEdge],
    index: &YStabbingIndex<usize>,
    parity: &mut ClipRingParity,
    account: &mut A,
) -> Result<bool> {
    account.charge_evaluations(1)?;
    let query_y = point.round_nearest_even()?.y;
    parity.begin_query();
    let mut boundary = false;
    let mut failure: Result<()> = Ok(());
    let _ = index.for_each_at_y(query_y, |&edge_index| {
        if boundary || failure.is_err() {
            return ControlFlow::Break(());
        }
        let Some(edge) = edges.get(edge_index) else {
            failure = Err(exact_cycle_error());
            return ControlFlow::Break(());
        };
        failure = (|| {
            account.charge_evaluations(1)?;
            let (start_y, end_y) = (edge.start.compare_y(point)?, edge.end.compare_y(point)?);
            let (low_y, high_y) = if edge.start.compare_y(&edge.end)?.is_le() {
                (&edge.start, &edge.end)
            } else {
                (&edge.end, &edge.start)
            };
            if point.compare_y(low_y)?.is_lt() || point.compare_y(high_y)?.is_gt() {
                return Ok(());
            }
            let (low_x, high_x) = if edge.start.compare_x(&edge.end)?.is_le() {
                (&edge.start, &edge.end)
            } else {
                (&edge.end, &edge.start)
            };
            let in_x_range = point.compare_x(low_x)?.is_ge() && point.compare_x(high_x)?.is_le();
            let crosses_ray = start_y.is_gt() != end_y.is_gt();
            if !in_x_range && !crosses_ray {
                return Ok(());
            }
            let orientation = exact::orient_points(&edge.start, &edge.end, point)?;
            if in_x_range && orientation == ExactSign::Zero {
                boundary = true;
                return Ok(());
            }
            if crosses_ray
                && (orientation == ExactSign::Positive) == edge.end.compare_y(&edge.start)?.is_gt()
            {
                parity.toggle(edge.ring)?;
            }
            Ok(())
        })();
        if failure.is_err() || boundary {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    });
    failure?;
    Ok(boundary || parity.covers())
}

pub(super) fn rounded_segment_covered_by_clip<A: SearchAccount>(
    segment: Segment,
    clip_edges: &[ClipCertificateEdge],
    candidates: &[usize],
    point_index: &PolygonPointIndex,
    y_index: &YStabbingIndex<usize>,
    events: &mut Vec<exact::ExactPoint>,
    parity: &mut ClipRingParity,
    account: &mut A,
) -> Result<Option<ClipBoundaryFailure>> {
    let mut interior_contact = false;
    let mut start_on_boundary = false;
    let mut end_on_boundary = false;
    for &edge_index in candidates {
        account.charge_evaluations(1)?;
        let edge = clip_edges.get(edge_index).ok_or_else(exact_cycle_error)?;
        let [start, end] = edge.rounded;
        start_on_boundary |= point_on_segment_xy(segment.start, start, end);
        end_on_boundary |= point_on_segment_xy(segment.end, start, end);
        let segment_start = exact::orient2d(segment.start, segment.end, start);
        let segment_end = exact::orient2d(segment.start, segment.end, end);
        let edge_start = exact::orient2d(start, end, segment.start);
        let edge_end = exact::orient2d(start, end, segment.end);
        if segment_start != ExactSign::Zero
            && segment_end != ExactSign::Zero
            && edge_start != ExactSign::Zero
            && edge_end != ExactSign::Zero
            && segment_start != segment_end
            && edge_start != edge_end
        {
            return Ok(Some(ClipBoundaryFailure {
                ring: edge.ring,
                atom: Some(edge.atom),
            }));
        }
        if (point_on_segment_xy(start, segment.start, segment.end)
            && start != segment.start
            && start != segment.end)
            || (point_on_segment_xy(end, segment.start, segment.end)
                && end != segment.start
                && end != segment.end)
        {
            interior_contact = true;
        }
    }
    for (point, on_boundary) in [
        (segment.start, start_on_boundary),
        (segment.end, end_on_boundary),
    ] {
        if !on_boundary {
            account.charge_evaluations(1)?;
            if !point_index.covers_point(Point::new_raw_xy(point.x, point.y)) {
                let point = exact::ExactPoint::from_xy(point);
                if exact_point_covered_by_clip_index(&point, clip_edges, y_index, parity, account)?
                {
                    return Err(voronoi_error(
                        "assigned clip point index disagrees with exact ring membership",
                    ));
                }
                return Ok(Some(ClipBoundaryFailure {
                    ring: parity.outside_ring().ok_or_else(clip_source_trace_error)?,
                    atom: None,
                }));
            }
        }
    }
    let start = exact::ExactPoint::from_xy(segment.start);
    let end = exact::ExactPoint::from_xy(segment.end);
    if !interior_contact {
        let midpoint = start.midpoint(&end)?;
        if exact_point_covered_by_clip_index(&midpoint, clip_edges, y_index, parity, account)? {
            return Ok(None);
        }
        return Ok(Some(ClipBoundaryFailure {
            ring: parity.outside_ring().ok_or_else(clip_source_trace_error)?,
            atom: None,
        }));
    }
    events.clear();
    events.reserve(2);
    events.extend([start.clone(), end.clone()]);
    for &edge_index in candidates {
        account.charge_evaluations(1)?;
        let edge = clip_edges.get(edge_index).ok_or_else(exact_cycle_error)?;
        match exact::segment_intersection(&start, &end, &edge.start, &edge.end)? {
            exact::SegmentIntersection::None => {},
            exact::SegmentIntersection::Point(point) => events.push(point),
            exact::SegmentIntersection::Overlap { start, end } => {
                events.reserve(2);
                events.extend([start, end]);
            },
        }
    }
    sort_unique_along(&start, &end, events)?;
    for [left, right] in events.array_windows::<2>() {
        if exact::compare_along(&start, &end, left, right)? != Ordering::Less {
            continue;
        }
        let midpoint = left.midpoint(right)?;
        if !exact_point_covered_by_clip_index(&midpoint, clip_edges, y_index, parity, account)? {
            return Ok(Some(ClipBoundaryFailure {
                ring: parity.outside_ring().ok_or_else(clip_source_trace_error)?,
                atom: None,
            }));
        }
    }
    Ok(None)
}

pub(super) fn clip_certificate_data<A: SearchAccount>(
    subdivision: &ExactSubdivision,
    clip: &NormalizedClip<'_>,
    trace: &ClipTrace,
    xy_of: impl Fn(VertexId) -> Result<XY>,
    account: &mut A,
) -> Result<ClipCertificate> {
    if trace.rings.len() != clip.rings.len() {
        return Err(clip_source_trace_error());
    }
    account.charge_evaluations(
        u64::try_from(trace.atom_count.div_ceil(16))
            .map_err(|_| crate::error::ResourceError::SizeOverflow)?,
    )?;

    let mut edges = Vec::with_capacity(trace.atom_count);
    let mut output_rings = Vec::with_capacity(trace.rings.len());
    for (ring_index, (source_ring, trace_ring)) in clip.rings.iter().zip(&trace.rings).enumerate() {
        if source_ring.len() < 3 || trace_ring.atoms.len() < source_ring.len() {
            return Err(clip_source_trace_error());
        }
        let first_atom = trace_ring
            .atoms
            .first()
            .ok_or_else(clip_source_trace_error)?;
        let [first_left, first_right] = subdivision.edge(first_atom.edge).endpoints;
        let first_vertex = if first_atom.reversed {
            first_right
        } else {
            first_left
        };
        let mut previous_end = None;
        let mut rounded_ring = Vec::with_capacity(trace_ring.atoms.len());
        let mut exact_ring = Vec::with_capacity(trace_ring.atoms.len());
        let mut output_points = Vec::with_capacity(trace_ring.atoms.len());
        for atom in &trace_ring.atoms {
            let [left, right] = subdivision.edge(atom.edge).endpoints;
            let (start, end) = if atom.reversed {
                (right, left)
            } else {
                (left, right)
            };
            if previous_end.is_some_and(|previous| previous != start) {
                return Err(clip_source_trace_error());
            }
            let rounded = xy_of(start)?;
            rounded_ring.push(rounded);
            exact_ring.push(exact::ExactPoint::from_xy(rounded));
            output_points.push(Point::new_raw_xy(rounded.x, rounded.y));
            previous_end = Some(end);
        }
        if previous_end != Some(first_vertex) {
            return Err(clip_source_trace_error());
        }
        let atom_pairs = trace_ring
            .atoms
            .iter()
            .zip(exact_ring.iter().zip(exact_ring.iter().cycle().skip(1)))
            .zip(rounded_ring.iter().zip(rounded_ring.iter().cycle().skip(1)));
        for ((atom, (start, end)), (rounded_start, rounded_end)) in atom_pairs {
            edges.push(ClipCertificateEdge {
                ring: ring_index,
                atom: atom.edge,
                start: start.clone(),
                end: end.clone(),
                rounded: [*rounded_start, *rounded_end],
            });
        }
        ensure_coordseq_vertex_capacity(
            output_points
                .len()
                .checked_add(1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?,
        )?;
        output_rings.push(Ring::closed(output_points)?);
    }
    let mut output_rings = output_rings.into_iter();
    let shell = output_rings.next().ok_or_else(clip_source_trace_error)?;
    Ok(ClipCertificate {
        edges,
        polygon: Polygon::new(shell, output_rings.collect()),
    })
}
