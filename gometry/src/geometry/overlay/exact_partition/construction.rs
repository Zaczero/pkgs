//! Building the partition: interning points and sweeping for contacts.
//!
//! The sweep visits candidate pairs of unique segments, classifies each
//! contact, and records the cuts it implies. [`PointArena`] owns the rounding
//! rule those handles share -- a stored endpoint keeps its source spelling, a
//! crossing rounds from a certified double-double and materialises its exact
//! homogeneous point only when a comparison ties.

use std::borrow::Cow;
use std::cmp::Ordering;
use std::mem;
use std::ops::ControlFlow;

#[cfg(test)]
use super::metrics::{record_candidate_pair, record_overlap, update as update_partition_metrics};
use super::rays::RayPoints;
use super::{Axis, ExactPartition, ExactSpan, KeptNodes, NodeId, NodeRecipe};
use crate::error::{Error, ResourceError};
use crate::geometry::exact_arith::ExactSign;
use crate::geometry::overlay::noding::{SegmentProvenance, SegmentSource};
use crate::geometry::segments::{
    SegmentClipInterval, SegmentClipPoint, crossing_homogeneous, crossing_point,
    point_in_segment_envelope, segment_envelopes_disjoint, stored_orientation_sign,
};
use crate::geometry::tessellation::exact::{ExactDyadic, compare_ratios, round_ratio};
use crate::geometry::{
    Bounds, ExpansionBudget, GeometryErrorKind, HashMap, PointKey, Result, Segment, XY,
    for_each_candidate_pair, same_point, same_topological_coordinate, single_chain,
};

/// Stored endpoints and segment crossings: each point's nearest-even binary64
/// and, for a crossing, its exact homogeneous point built on first demand.
/// Construction handles and the partition nodes they compact into share this
/// one owner of the rounding-order rule.
#[derive(Debug)]
pub(super) struct PointArena<'a> {
    pub(super) unique: &'a [Segment],
    pub(super) recipes: Vec<NodeRecipe>,
    pub(super) rounded: Vec<XY>,
}

impl<'a> PointArena<'a> {
    pub(super) fn with_capacity(unique: &'a [Segment], capacity: usize) -> Self {
        Self {
            unique,
            recipes: Vec::with_capacity(capacity),
            rounded: Vec::with_capacity(capacity),
        }
    }

    pub(super) const fn len(&self) -> usize {
        self.recipes.len()
    }

    pub(super) fn push(&mut self, recipe: NodeRecipe, rounded: XY) -> Result<u32> {
        let index = point_id_for_append(self.recipes.len())?;
        self.recipes.push(recipe);
        self.rounded.push(rounded);
        Ok(index)
    }

    /// A stored endpoint, keeping the source spelling of signed zero.
    pub(super) fn push_stored(&mut self, point: XY) -> Result<u32> {
        self.push(NodeRecipe::Stored, point)
    }

    /// The crossing of two unique segments' carriers, lower index first,
    /// rounded by the double-double certificate or, when it declines, by the
    /// exact construction it then keeps.
    #[expect(
        clippy::indexing_slicing,
        reason = "crossing segment IDs are admitted from the unique-segment candidate sweep"
    )]
    pub(super) fn push_crossing(&mut self, segments: [u32; 2]) -> Result<u32> {
        let [first, second] = segments;
        let (first, second) = (self.unique[first as usize], self.unique[second as usize]);
        let crossing = crossing_point(first, second)?;
        let exact = crossing
            .exact
            .map_or_else(std::cell::OnceCell::new, |point| {
                #[cfg(test)]
                update_partition_metrics(|metrics| {
                    metrics.declined_crossings = metrics.declined_crossings.wrapping_add(1);
                });
                std::cell::OnceCell::from(Box::new(point))
            });
        self.push(NodeRecipe::Crossing { segments, exact }, crossing.point)
    }

    pub(super) fn push_crossing_with_exact(
        &mut self,
        segments: [u32; 2],
        rounded: XY,
        exact: [ExactDyadic; 3],
    ) -> Result<u32> {
        self.push(
            NodeRecipe::Crossing {
                segments,
                exact: std::cell::OnceCell::from(Box::new(exact)),
            },
            rounded,
        )
    }

    pub(super) fn push_constructed(&mut self, rounded: XY, exact: [ExactDyadic; 3]) -> Result<u32> {
        self.push(
            NodeRecipe::Constructed {
                exact: std::cell::OnceCell::from(Box::new(exact)),
            },
            rounded,
        )
    }

    /// Move an entry out, leaving a stored placeholder behind.
    #[expect(
        clippy::indexing_slicing,
        reason = "the admitted PointArena node ID addresses its recipe and rounded point together"
    )]
    pub(super) fn take(&mut self, index: u32) -> (NodeRecipe, XY) {
        (
            mem::replace(&mut self.recipes[index as usize], NodeRecipe::Stored),
            self.rounded[index as usize],
        )
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "the admitted PointArena node ID addresses its recipe column"
    )]
    pub(super) fn is_stored(&self, index: u32) -> bool {
        matches!(self.recipes[index as usize], NodeRecipe::Stored)
    }

    pub(super) fn same_exact_point(&self, left: u32, right: u32) -> Result<bool> {
        if left == right {
            return Ok(true);
        }
        let left = self.exact(left)?;
        let right = self.exact(right)?;
        let [left_x, left_y, left_w] = &*left;
        let [right_x, right_y, right_w] = &*right;
        Ok(
            compare_ratios(left_x, left_w, right_x, right_w)? == Ordering::Equal
                && compare_ratios(left_y, left_w, right_y, right_w)? == Ordering::Equal,
        )
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "the admitted PointArena node ID addresses its rounded point column"
    )]
    pub(super) fn stored_key(&self, index: u32) -> Option<PointKey> {
        self.is_stored(index)
            .then(|| PointKey::new(self.rounded[index as usize]))
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "the admitted PointArena node ID addresses aligned recipe and rounded columns; crossing segment IDs came from the candidate sweep"
    )]
    pub(super) fn exact(&self, index: u32) -> Result<Cow<'_, [ExactDyadic; 3]>> {
        match &self.recipes[index as usize] {
            NodeRecipe::Stored => {
                let point = self.rounded[index as usize];
                Ok(Cow::Owned([
                    ExactDyadic::from_f64(point.x),
                    ExactDyadic::from_f64(point.y),
                    ExactDyadic::from_f64(1.0),
                ]))
            },
            NodeRecipe::Constructed { exact } => Ok(Cow::Borrowed(
                &**exact.get().expect("constructed nodes carry exact points"),
            )),
            NodeRecipe::Crossing { segments, exact } => {
                let point = exact.get_or_try_init(|| -> Result<_> {
                    #[cfg(test)]
                    update_partition_metrics(|metrics| {
                        metrics.exact_crossings = metrics.exact_crossings.wrapping_add(1);
                    });
                    let [first, second] = *segments;
                    Ok(Box::new(crossing_homogeneous(
                        self.unique[first as usize],
                        self.unique[second as usize],
                    )?))
                })?;
                Ok(Cow::Borrowed(&**point))
            },
        }
    }

    /// Order a point's coordinate against a binary64 value by the same rule.
    #[expect(
        clippy::indexing_slicing,
        reason = "the admitted PointArena node ID addresses aligned recipe and rounded columns"
    )]
    pub(super) fn compare_axis_to(&self, index: u32, axis: Axis, value: f64) -> Result<Ordering> {
        let rounded = axis.of(self.rounded[index as usize]);
        if rounded < value {
            return Ok(Ordering::Less);
        }
        if rounded > value {
            return Ok(Ordering::Greater);
        }
        if self.is_stored(index) {
            return Ok(Ordering::Equal);
        }
        let point = self.exact(index)?;
        let [x, y, w] = &*point;
        let (coordinate, denominator) = match axis {
            Axis::X => (x, w),
            Axis::Y => (y, w),
        };
        compare_ratios(
            coordinate,
            denominator,
            &ExactDyadic::from_f64(value),
            &ExactDyadic::from_f64(1.0),
        )
    }

    /// Certified outward bounds on a coordinate: the stored value, or the
    /// neighbours of a crossing's nearest-even rounding.
    #[expect(
        clippy::indexing_slicing,
        reason = "the admitted PointArena node ID addresses its rounded point column"
    )]
    pub(super) fn axis_bounds(&self, index: u32, axis: Axis) -> (f64, f64) {
        let rounded = axis.of(self.rounded[index as usize]);
        if self.is_stored(index) {
            (rounded, rounded)
        } else {
            (rounded.next_down(), rounded.next_up())
        }
    }

    /// Order items whose points lie on `segment`'s carrier from its start to
    /// its end, reporting each adjacent pair at one point to `equal`.
    pub(super) fn order_along<T: Copy>(
        &self,
        segment: Segment,
        items: &mut [T],
        point: impl Fn(T) -> u32,
        equal: impl FnMut(T, T) -> Result<()>,
    ) -> Result<()> {
        let (axis, increasing) = segment_axis(segment);
        RayPoints::sort_by_axis(self, items, axis, point, equal)?;
        if !increasing {
            items.reverse();
        }
        Ok(())
    }
}

/// Admit a node ID only when its append leaves the count representable by the
/// `u32`-counted topology and emission loops.
pub(super) fn point_id_for_append(current_count: usize) -> Result<u32> {
    let id = u32::try_from(current_count).map_err(|_| ResourceError::SizeOverflow)?;
    if id == u32::MAX {
        return Err(ResourceError::SizeOverflow.into());
    }
    Ok(id)
}

/// Convert per-segment contact counts into native-vector offsets in place.
pub(super) fn prefix_contact_offsets(offsets: &mut [usize]) {
    let Some((first, rest)) = offsets.split_first_mut() else {
        return;
    };
    let mut total = *first;
    for offset in rest {
        total = total.wrapping_add(*offset);
        *offset = total;
    }
}

/// The exact parameter of `node` along `segment`'s carrier, as `[num, den]`
/// with `node = start + (num / den) (end - start)`, read on x unless the
/// segment is vertical.
pub(super) fn parameter_along(
    segment: Segment,
    point: &[ExactDyadic; 3],
) -> Result<[ExactDyadic; 2]> {
    let [x, y, w] = point;
    let (coordinate, start, end) = if same_topological_coordinate(segment.start.x, segment.end.x) {
        (y, segment.start.y, segment.end.y)
    } else {
        (x, segment.start.x, segment.end.x)
    };
    let start = ExactDyadic::from_f64(start);
    let direction = ExactDyadic::from_f64(end).try_subtract(start.clone())?;
    Ok([
        coordinate.clone().try_subtract(start.try_product(w)?)?,
        w.try_product(&direction)?,
    ])
}

#[derive(Clone, Copy, Default)]
pub(super) struct AxisValue {
    value: Option<f64>,
    conflict: bool,
}

impl AxisValue {
    pub(super) const fn observe(&mut self, value: Option<f64>) {
        let Some(value) = value else {
            return;
        };
        match self.value {
            None => self.value = Some(value),
            Some(previous) if previous.to_bits() == value.to_bits() => {},
            Some(_) => self.conflict = true,
        }
    }

    pub(super) fn value(self) -> Option<f64> {
        (!self.conflict).then_some(self.value).flatten()
    }
}

/// The ordinate at parameter `num / den` along a source from `start` to
/// `end`, rounded once. A source endpoint keeps its stored spelling, signed
/// zero included.
pub(super) fn interpolate_ordinate(
    start: Option<f64>,
    end: Option<f64>,
    num: &ExactDyadic,
    den: &ExactDyadic,
) -> Result<Option<f64>> {
    let (Some(start), Some(end)) = (start, end) else {
        return Ok(None);
    };
    if start.to_bits() == end.to_bits() || num.is_zero() {
        return Ok(Some(start));
    }
    if num == den {
        return Ok(Some(end));
    }
    let start = ExactDyadic::from_f64(start);
    let end = ExactDyadic::from_f64(end);
    let left = den.clone().try_subtract(num.clone())?;
    let numerator = start.try_product(&left)?.try_add(end.try_product(num)?)?;
    Ok(Some(round_ratio(&numerator, den)?))
}

/// Construction handles: interned stored endpoints and crossings, unioned
/// where they denote one point.
pub(super) struct Forest<'a> {
    parent: Vec<u32>,
    points: PointArena<'a>,
}

impl<'a> Forest<'a> {
    pub(super) fn with_capacity(unique: &'a [Segment], capacity: usize) -> Self {
        Self {
            parent: Vec::with_capacity(capacity),
            points: PointArena::with_capacity(unique, capacity),
        }
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "every DSU handle was admitted by adopt and the parent column stays one-for-one with PointArena"
    )]
    pub(super) fn find(&mut self, mut node: u32) -> u32 {
        while self.parent[node as usize] != node {
            self.parent[node as usize] = self.parent[self.parent[node as usize] as usize];
            node = self.parent[node as usize];
        }
        node
    }

    pub(super) fn adopt(&mut self, handle: u32) -> u32 {
        debug_assert_eq!(handle as usize, self.parent.len());
        self.parent.push(handle);
        handle
    }

    pub(super) fn push_stored(&mut self, point: XY) -> Result<u32> {
        let handle = self.points.push_stored(point)?;
        Ok(self.adopt(handle))
    }

    pub(super) fn push_crossing(&mut self, segments: [u32; 2]) -> Result<u32> {
        let handle = self.points.push_crossing(segments)?;
        Ok(self.adopt(handle))
    }

    pub(super) fn push_crossing_with_exact(
        &mut self,
        segments: [u32; 2],
        rounded: XY,
        exact: [ExactDyadic; 3],
    ) -> Result<u32> {
        let handle = self
            .points
            .push_crossing_with_exact(segments, rounded, exact)?;
        Ok(self.adopt(handle))
    }

    pub(super) fn push_constructed(&mut self, rounded: XY, exact: [ExactDyadic; 3]) -> Result<u32> {
        let handle = self.points.push_constructed(rounded, exact)?;
        Ok(self.adopt(handle))
    }

    /// Join two handles denoting one point. A stored root wins, and between
    /// crossings the lower segment pair, so a node's recipe is deterministic.
    #[expect(
        clippy::indexing_slicing,
        reason = "find returns roots from the same admitted forest and PointArena"
    )]
    pub(super) fn union(&mut self, a: u32, b: u32) -> Result<()> {
        let a = self.find(a);
        let b = self.find(b);
        if a == b {
            return Ok(());
        }
        let left_recipe = &self.points.recipes[a as usize];
        let right_recipe = &self.points.recipes[b as usize];
        let priority = |recipe: &NodeRecipe| match recipe {
            NodeRecipe::Stored => 0,
            NodeRecipe::Constructed { .. } => 1,
            NodeRecipe::Crossing { .. } => 2,
        };
        let (keep, merge) = match (priority(left_recipe), priority(right_recipe)) {
            (0, 0) => {
                return Err(GeometryErrorKind::Invalid(
                    "exact partition unioned distinct stored points".into(),
                )
                .into());
            },
            (left, right) if left < right => (a, b),
            (left, right) if right < left => (b, a),
            (2, 2) => {
                let (
                    NodeRecipe::Crossing { segments: left, .. },
                    NodeRecipe::Crossing {
                        segments: right, ..
                    },
                ) = (left_recipe, right_recipe)
                else {
                    unreachable!("equal crossing priorities have crossing recipes")
                };
                if right < left { (b, a) } else { (a, b) }
            },
            _ => {
                if a < b {
                    (a, b)
                } else {
                    (b, a)
                }
            },
        };
        self.parent[merge as usize] = keep;
        Ok(())
    }
}

/// The axis on which a segment's direction is largest, so it orders points
/// on its carrier, and whether the segment runs toward increasing values.
pub(super) fn segment_axis(segment: Segment) -> (Axis, bool) {
    let (dx, dy) = (
        segment.end.x - segment.start.x,
        segment.end.y - segment.start.y,
    );
    if dx.abs() >= dy.abs() {
        (Axis::X, dx > 0.0)
    } else {
        (Axis::Y, dy > 0.0)
    }
}

/// Whether `point`, on the carrier of the nondegenerate `segment`, lies
/// strictly between its endpoints.
pub(super) fn strictly_inside(segment: Segment, point: XY) -> bool {
    let (axis, _) = segment_axis(segment);
    let (start, end, value) = (axis.of(segment.start), axis.of(segment.end), axis.of(point));
    start.min(end) < value && value < start.max(end)
}

pub(in crate::geometry) fn partition_unique_segments<'a>(
    unique: &'a [Segment],
    provenance: &SegmentProvenance,
    isolated: &[XY],
    budget: &mut ExpansionBudget,
) -> Result<ExactPartition<'a>> {
    partition_unique_segments_with_budget(unique, provenance, isolated, None, Some(budget))
}

pub(in crate::geometry) fn partition_unique_segments_in_window<'a>(
    unique: &'a [Segment],
    provenance: &SegmentProvenance,
    intervals: &[SegmentClipInterval],
    window: Bounds,
    budget: &mut ExpansionBudget,
) -> Result<ExactPartition<'a>> {
    partition_unique_segments_with_budget(
        unique,
        provenance,
        &[],
        Some((intervals, window)),
        Some(budget),
    )
}

pub(in crate::geometry) fn partition_unique_segments_unbudgeted<'a>(
    unique: &'a [Segment],
    provenance: &SegmentProvenance,
    isolated: &[XY],
) -> Result<ExactPartition<'a>> {
    partition_unique_segments_with_budget(unique, provenance, isolated, None, None)
}

fn partition_unique_segments_with_budget<'a>(
    unique: &'a [Segment],
    provenance: &SegmentProvenance,
    isolated: &[XY],
    windowed: Option<(&[SegmentClipInterval], Bounds)>,
    mut budget: Option<&mut ExpansionBudget>,
) -> Result<ExactPartition<'a>> {
    debug_assert_eq!(provenance.len(), unique.len());
    if let Some((intervals, _)) = windowed {
        debug_assert_eq!(intervals.len(), unique.len());
    }
    let endpoint_capacity = unique
        .len()
        .checked_mul(2)
        .and_then(|count| count.checked_add(isolated.len()))
        .ok_or(ResourceError::SizeOverflow)?;
    let mut endpoint_of: HashMap<PointKey, u32> =
        HashMap::with_hasher(crate::collections::collection_hash_state()?.clone());
    endpoint_of.reserve(endpoint_capacity);
    let mut forest = Forest::with_capacity(unique, endpoint_capacity);

    let intern_stored =
        |point: XY, endpoint_of: &mut HashMap<PointKey, u32>, forest: &mut Forest| -> Result<u32> {
            let key = PointKey::new(point);
            if let Some(&id) = endpoint_of.get(&key) {
                return Ok(id);
            }
            let id = forest.push_stored(point)?;
            endpoint_of.insert(key, id);
            Ok(id)
        };

    let mut start_handle = Vec::new();
    let mut end_handle = Vec::new();
    start_handle.reserve(unique.len());
    end_handle.reserve(unique.len());
    let mut clip_endpoint_keys = Vec::new();
    if let Some((intervals, _)) = windowed {
        for interval in intervals {
            let mut push_endpoint = |endpoint: &SegmentClipPoint| {
                let handle = if let Some(exact) = &endpoint.exact {
                    let handle = forest.push_constructed(endpoint.point, (**exact).clone())?;
                    clip_endpoint_keys.push((PointKey::new(endpoint.point), handle));
                    Ok(handle)
                } else {
                    intern_stored(endpoint.point, &mut endpoint_of, &mut forest)
                }?;
                Ok::<_, Error>(handle)
            };
            start_handle.push(push_endpoint(&interval.start)?);
            end_handle.push(push_endpoint(&interval.end)?);
        }
    } else {
        for &segment in unique {
            start_handle.push(intern_stored(segment.start, &mut endpoint_of, &mut forest)?);
            end_handle.push(intern_stored(segment.end, &mut endpoint_of, &mut forest)?);
        }
    }
    if !clip_endpoint_keys.is_empty() {
        for &(key, handle) in &clip_endpoint_keys {
            if let Some(&stored) = endpoint_of.get(&key)
                && forest.points.same_exact_point(stored, handle)?
            {
                forest.union(stored, handle)?;
            }
        }
        clip_endpoint_keys.sort_unstable_by_key(|(key, _)| *key);
        for group in clip_endpoint_keys.chunk_by(|left, right| left.0 == right.0) {
            for (right, &(_, right_handle)) in group.iter().enumerate().skip(1) {
                for &(_, left_handle) in group.iter().take(right) {
                    if forest.points.same_exact_point(left_handle, right_handle)? {
                        forest.union(left_handle, right_handle)?;
                        break;
                    }
                }
            }
        }
    }

    let mut extra_handles = Vec::with_capacity(isolated.len());
    let mut point_handles = Vec::with_capacity(isolated.len());
    for &point in isolated {
        let key = PointKey::new(point);
        let existed = endpoint_of.contains_key(&key);
        let handle = intern_stored(point, &mut endpoint_of, &mut forest)?;
        point_handles.push(handle);
        if existed {
            continue;
        }
        extra_handles.push(handle);
    }
    drop(endpoint_of);
    let pool = if let Some((intervals, _)) = windowed {
        let capacity = unique
            .len()
            .checked_add(extra_handles.len())
            .ok_or(ResourceError::SizeOverflow)?;
        let mut pool = Vec::with_capacity(capacity);
        pool.extend(intervals.iter().map(|interval| Segment {
            start: interval.start.point,
            end: interval.end.point,
        }));
        #[expect(
            clippy::indexing_slicing,
            reason = "extra handles were admitted by PointArena and address its aligned rounded-point column"
        )]
        pool.extend(extra_handles.iter().map(|&handle| {
            let point = forest.points.rounded[handle as usize];
            Segment {
                start: point,
                end: point,
            }
        }));
        Cow::Owned(pool)
    } else if extra_handles.is_empty() {
        Cow::Borrowed(unique)
    } else {
        let capacity = unique
            .len()
            .checked_add(extra_handles.len())
            .ok_or(ResourceError::SizeOverflow)?;
        let mut pool = Vec::with_capacity(capacity);
        pool.extend_from_slice(unique);
        #[expect(
            clippy::indexing_slicing,
            reason = "extra handles were admitted by PointArena and address its aligned rounded-point column"
        )]
        pool.extend(extra_handles.iter().map(|&handle| {
            let point = forest.points.rounded[handle as usize];
            Segment {
                start: point,
                end: point,
            }
        }));
        Cow::Owned(pool)
    };
    #[cfg(test)]
    update_partition_metrics(|metrics| {
        metrics.candidate_pool_items = pool.len();
    });

    // Interior contacts as (unique, handle): a crossing, a stored point
    // touching a segment's interior, or an overlap end.
    let mut contacts: Vec<(u32, u32)> = Vec::new();
    let handles = EndpointHandles {
        start: &start_handle,
        end: &end_handle,
        extra: &extra_handles,
    };
    let mut pair_error = None;
    #[cfg(test)]
    let candidate_phase =
        crate::test_support::allocation_phase(crate::test_support::AllocationPhase::CandidatePairs);
    if for_each_candidate_pair(&pool, single_chain, |left, right| {
        #[cfg(test)]
        record_candidate_pair();
        match visit_pool_pair(
            unique,
            &pool,
            left,
            right,
            &handles,
            &mut contacts,
            &mut forest,
            windowed.map(|(_, bounds)| bounds),
        ) {
            Ok(()) => ControlFlow::Continue(()),
            Err(error) => {
                pair_error = Some(error);
                ControlFlow::Break(())
            },
        }
    })?
    .is_break()
        && let Some(error) = pair_error
    {
        return Err(error);
    }
    #[cfg(test)]
    drop(candidate_phase);
    drop(pool);
    drop(extra_handles);
    #[cfg(test)]
    update_partition_metrics(|metrics| {
        metrics.source_cut_entries = contacts.len();
    });

    // Group contacts by segment (counting placement), then order each group
    // along its segment: by rounded coordinate first, and exactly only within
    // a run of equal rounded coordinates, whose equal points are one node.
    let mut contact_starts = vec![0_usize; unique.len().wrapping_add(1)];
    #[expect(
        clippy::indexing_slicing,
        reason = "contact source IDs come from unique and the count vector includes a terminal slot"
    )]
    for &(segment, _) in &contacts {
        let next = (segment as usize).wrapping_add(1);
        contact_starts[next] = contact_starts[next].wrapping_add(1);
    }
    prefix_contact_offsets(&mut contact_starts);
    let mut along = vec![0_u32; contacts.len()];
    {
        let mut cursor = Vec::with_capacity(unique.len());
        cursor.extend(contact_starts.iter().take(unique.len()).copied());
        #[expect(
            clippy::indexing_slicing,
            reason = "prefix cursors place each contact inside the along slice for its source"
        )]
        for &(segment, handle) in &contacts {
            let segment = segment as usize;
            along[cursor[segment]] = handle;
            cursor[segment] = cursor[segment].wrapping_add(1);
        }
    }
    drop(contacts);
    let mut equal_pairs: Vec<(u32, u32)> = Vec::new();
    #[expect(
        clippy::indexing_slicing,
        reason = "prefix offsets partition along in unique-segment order"
    )]
    for (index, &segment) in unique.iter().enumerate() {
        let start = contact_starts[index];
        let end = contact_starts[index.wrapping_add(1)];
        let group = &mut along[start..end];
        if group.len() > 1 {
            forest.points.order_along(
                segment,
                group,
                |handle| handle,
                |left, right| {
                    equal_pairs.push((left, right));
                    Ok(())
                },
            )?;
        }
    }
    for (left, right) in equal_pairs {
        forest.union(left, right)?;
    }

    // Compact roots into nodes, moving each root's point out of the forest.
    let mut compact_of: Vec<Option<u32>> = Vec::with_capacity(forest.parent.len());
    compact_of.resize(forest.parent.len(), None);
    let mut points = PointArena::with_capacity(unique, 0);
    #[expect(
        clippy::indexing_slicing,
        reason = "forest roots are valid handles; compact_of is sized one-for-one with the forest and PointArena"
    )]
    for handle in 0..forest.parent.len() as u32 {
        let root = forest.find(handle);
        if compact_of[root as usize].is_some() {
            continue;
        }
        let (recipe, rounded) = forest.points.take(root);
        if !matches!(recipe, NodeRecipe::Stored)
            && let Some(budget) = budget.as_deref_mut()
        {
            budget.add(1)?;
        }
        compact_of[root as usize] = Some(points.push(recipe, rounded)?);
    }
    #[expect(
        clippy::indexing_slicing,
        reason = "each forest handle and its root have entries in the compact map sized from that forest"
    )]
    for handle in 0..forest.parent.len() {
        let root = forest.find(handle as u32);
        compact_of[handle] = compact_of[root as usize];
    }
    drop(forest);
    let node_of = |handle: u32| -> Result<u32> {
        compact_of
            .get(handle as usize)
            .copied()
            .flatten()
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("partition handle was not compacted".into()).into()
            })
    };

    // Walk every segment from its start through its ordered contacts to its
    // end: consecutive distinct nodes are its atoms, and every node met is
    // bound to it once. Segments are visited in ascending order, so a
    // counting placement keeps each node's bindings in that order.
    let node_count = points.len();
    let mut last_unique = Vec::with_capacity(node_count);
    last_unique.resize(node_count, u32::MAX);
    let mut binding_nodes: Vec<(u32, u32)> = Vec::new();
    let mut binding_starts = vec![0_u32; node_count.wrapping_add(1)];
    let mut intervals: Vec<(u32, u32, u32)> = Vec::new();
    for (index, (((segment, &start_handle), &end_handle), &[start_contact, end_contact])) in unique
        .iter()
        .zip(&start_handle)
        .zip(&end_handle)
        .zip(contact_starts.array_windows::<2>())
        .enumerate()
    {
        // A zero-length segment is an isolated point, bound as a point source.
        if same_point(segment.start, segment.end) {
            continue;
        }
        let source = index as u32;
        #[expect(
            clippy::indexing_slicing,
            reason = "the checked contact prefix window bounds this source's contiguous range in along"
        )]
        let group = &along[start_contact..end_contact];
        let walk = std::iter::once(start_handle)
            .chain(group.iter().copied())
            .chain(std::iter::once(end_handle));
        let mut previous: Option<u32> = None;
        #[expect(
            clippy::indexing_slicing,
            reason = "node IDs come from this partition's PointArena and index its aligned binding columns"
        )]
        for handle in walk {
            let node = node_of(handle)?;
            if last_unique[node as usize] != source {
                last_unique[node as usize] = source;
                let next = node.wrapping_add(1) as usize;
                binding_starts[next] = binding_starts[next].wrapping_add(1);
                binding_nodes.push((node, source));
            }
            if let Some(start) = previous
                && start != node
            {
                intervals.push((source, start, node));
            }
            previous = Some(node);
        }
    }
    drop(end_handle);
    drop(along);
    drop(contact_starts);
    drop(last_unique);
    let mut total = 0_u32;
    for offset in binding_starts.iter_mut().skip(1) {
        total = total
            .checked_add(*offset)
            .ok_or(ResourceError::SizeOverflow)?;
        *offset = total;
    }
    let mut bindings = vec![0_u32; binding_nodes.len()];
    {
        let mut cursor = Vec::with_capacity(node_count);
        cursor.extend(binding_starts.iter().take(node_count).copied());
        #[expect(
            clippy::indexing_slicing,
            reason = "binding nodes and cursors were counted into the same checked CSR offsets"
        )]
        for &(node, source) in &binding_nodes {
            bindings[cursor[node as usize] as usize] = source;
            cursor[node as usize] = cursor[node as usize].wrapping_add(1);
        }
    }
    drop(binding_nodes);

    // The budget counts operation-wide net growth: overlap coalescing can
    // offset span growth elsewhere, while stored nodes and source atoms are free.
    let baseline = unique
        .iter()
        .filter(|segment| !same_point(segment.start, segment.end))
        .count();
    let (spans, atom_provenance) = aggregate_atoms(intervals, provenance, baseline, budget)?;

    let mut degree = vec![0_u32; node_count];
    #[expect(
        clippy::indexing_slicing,
        reason = "each span endpoint is a NodeId admitted by the PointArena that sized degree"
    )]
    for span in &spans {
        degree[span.start.0 as usize] = degree[span.start.0 as usize].saturating_add(1);
        degree[span.end.0 as usize] = degree[span.end.0 as usize].saturating_add(1);
    }
    let mut isolated_nodes = Vec::new();
    for (index, &count) in degree.iter().enumerate() {
        if count == 0 {
            isolated_nodes.push(NodeId(index as u32));
        }
    }
    drop(degree);

    let mut point_nodes = Vec::with_capacity(point_handles.len());
    for handle in point_handles {
        point_nodes.push(NodeId(node_of(handle)?));
    }

    let mut point_sources = Vec::new();
    for ((segment, &start_handle), owners) in
        unique.iter().zip(&start_handle).zip(provenance.iter())
    {
        if same_point(segment.start, segment.end) {
            let node = NodeId(node_of(start_handle)?);
            point_sources.reserve(owners.len());
            point_sources.extend(owners.iter().map(|&(ordinal, _)| (node, ordinal)));
        }
    }

    Ok(ExactPartition {
        unique,
        points,
        spans,
        provenance: atom_provenance,
        isolated: isolated_nodes,
        point_nodes,
        point_sources,
        winding_seed: None,
        binding_starts,
        bindings,
        kept_nodes: KeptNodes::Selected,
    })
}

/// The stored handles a pair visit can cut with: each unique segment's
/// endpoints, and the isolated points pooled after the segments.
pub(super) struct EndpointHandles<'h> {
    start: &'h [u32],
    end: &'h [u32],
    extra: &'h [u32],
}

#[expect(
    clippy::indexing_slicing,
    reason = "candidate pair IDs index the exact pool partition and its parallel endpoint-handle columns"
)]
pub(super) fn visit_pool_pair(
    unique: &[Segment],
    pool: &[Segment],
    left: usize,
    right: usize,
    handles: &EndpointHandles<'_>,
    contacts: &mut Vec<(u32, u32)>,
    forest: &mut Forest<'_>,
    window: Option<Bounds>,
) -> Result<()> {
    let count = unique.len();
    match (left < count, right < count) {
        (true, true) => classify_pair_into(unique, left, right, handles, contacts, forest, window),
        (true, false) => push_point_on_source(
            unique[left],
            left,
            pool[right].start,
            handles.extra[right.wrapping_sub(count)],
            contacts,
            window,
        ),
        (false, true) => push_point_on_source(
            unique[right],
            right,
            pool[left].start,
            handles.extra[left.wrapping_sub(count)],
            contacts,
            window,
        ),
        (false, false) => Ok(()),
    }
}

pub(super) fn push_contact(
    contacts: &mut Vec<(u32, u32)>,
    segment: usize,
    handle: u32,
) -> Result<()> {
    let segment = u32::try_from(segment).map_err(|_| ResourceError::SizeOverflow)?;
    contacts.push((segment, handle));
    Ok(())
}

/// A stored point exactly on a segment's carrier and strictly between its
/// endpoints cuts that segment.
pub(super) fn push_point_on_source(
    segment: Segment,
    index: usize,
    point: XY,
    handle: u32,
    contacts: &mut Vec<(u32, u32)>,
    window: Option<Bounds>,
) -> Result<()> {
    if same_point(segment.start, segment.end)
        || !point_in_segment_envelope(point, segment)
        || window.is_some_and(|bounds| !bounds.contains_xy(point))
    {
        return Ok(());
    }
    let side = stored_orientation_sign(
        segment.start.x,
        segment.start.y,
        segment.end.x,
        segment.end.y,
        point.x,
        point.y,
    );
    if side == ExactSign::Zero && strictly_inside(segment, point) {
        push_contact(contacts, index, handle)?;
    }
    Ok(())
}

/// Cut two collinear segments at every endpoint of one strictly inside the
/// other.
#[expect(
    clippy::indexing_slicing,
    reason = "overlap pair IDs index the unique segments' aligned start/end handle columns"
)]
pub(super) fn push_overlap(
    [a, b]: [Segment; 2],
    [left, right]: [usize; 2],
    handles: &EndpointHandles<'_>,
    contacts: &mut Vec<(u32, u32)>,
    window: Option<Bounds>,
) -> Result<()> {
    #[cfg(test)]
    let before = contacts.len();
    for (host, host_index, guest, guest_index) in [(a, left, b, right), (b, right, a, left)] {
        for (point, handle) in [
            (guest.start, handles.start[guest_index]),
            (guest.end, handles.end[guest_index]),
        ] {
            if strictly_inside(host, point) && window.is_none_or(|bounds| bounds.contains_xy(point))
            {
                push_contact(contacts, host_index, handle)?;
            }
        }
    }
    #[cfg(test)]
    if contacts.len() > before {
        record_overlap(contacts.len().wrapping_sub(before));
    }
    Ok(())
}

#[expect(
    clippy::indexing_slicing,
    reason = "candidate pair IDs index the unique segment and endpoint-handle columns"
)]
#[expect(
    clippy::float_cmp,
    reason = "exact represented-coordinate equality identifies a rounded crossing on a window side"
)]
pub(super) fn classify_pair_into(
    unique: &[Segment],
    left: usize,
    right: usize,
    handles: &EndpointHandles<'_>,
    contacts: &mut Vec<(u32, u32)>,
    forest: &mut Forest<'_>,
    window: Option<Bounds>,
) -> Result<()> {
    let a = unique[left];
    let b = unique[right];
    if same_point(a.start, a.end) {
        return push_point_on_source(b, right, a.start, handles.start[left], contacts, window);
    }
    if same_point(b.start, b.end) {
        return push_point_on_source(a, left, b.start, handles.start[right], contacts, window);
    }
    if segment_envelopes_disjoint(a, b) {
        return Ok(());
    }
    #[cfg(test)]
    let _contact_phase = crate::test_support::allocation_phase(
        crate::test_support::AllocationPhase::OverlapContacts,
    );
    let orient = |segment: Segment, point: XY| {
        stored_orientation_sign(
            segment.start.x,
            segment.start.y,
            segment.end.x,
            segment.end.y,
            point.x,
            point.y,
        )
    };
    // A shared endpoint lies on both segments, so the pair meets elsewhere
    // only along a collinear overlap. Adjacent ring segments are most pairs,
    // and their zero orientation is the one the floating filter cannot certify.
    let shared = |point: XY| same_point(point, a.start) || same_point(point, a.end);
    let unshared = match (shared(b.start), shared(b.end)) {
        // Unique segments never repeat, so this is `a` itself.
        (true, true) => return Ok(()),
        (true, false) => Some(b.end),
        (false, true) => Some(b.start),
        (false, false) => None,
    };
    if let Some(other) = unshared {
        if orient(a, other) == ExactSign::Zero {
            return push_overlap([a, b], [left, right], handles, contacts, window);
        }
        return Ok(());
    }
    let (o_b0, o_b1) = (orient(a, b.start), orient(a, b.end));
    if o_b0 == ExactSign::Zero && o_b1 == ExactSign::Zero {
        return push_overlap([a, b], [left, right], handles, contacts, window);
    }
    // One endpoint on the other carrier: a touch when it lies strictly
    // inside, otherwise the segments do not meet.
    for (zero, host, index, point, handle) in [
        (
            o_b0 == ExactSign::Zero,
            a,
            left,
            b.start,
            handles.start[right],
        ),
        (o_b1 == ExactSign::Zero, a, left, b.end, handles.end[right]),
    ] {
        if zero {
            if strictly_inside(host, point) && window.is_none_or(|bounds| bounds.contains_xy(point))
            {
                push_contact(contacts, index, handle)?;
            }
            return Ok(());
        }
    }
    let (o_a0, o_a1) = (orient(b, a.start), orient(b, a.end));
    for (zero, host, index, point, handle) in [
        (
            o_a0 == ExactSign::Zero,
            b,
            right,
            a.start,
            handles.start[left],
        ),
        (o_a1 == ExactSign::Zero, b, right, a.end, handles.end[left]),
    ] {
        if zero {
            if strictly_inside(host, point) && window.is_none_or(|bounds| bounds.contains_xy(point))
            {
                push_contact(contacts, index, handle)?;
            }
            return Ok(());
        }
    }
    // Strictly opposite sides on both carriers: a proper crossing, interior
    // to both segments.
    if o_b0 != o_b1 && o_a0 != o_a1 {
        let segments = [
            u32::try_from(left.min(right)).map_err(|_| ResourceError::SizeOverflow)?,
            u32::try_from(left.max(right)).map_err(|_| ResourceError::SizeOverflow)?,
        ];
        let handle = if let Some(bounds) = window {
            let crossing = crossing_point(a, b)?;
            let rounded = crossing.point;
            if rounded.x < bounds.minx()
                || rounded.x > bounds.maxx()
                || rounded.y < bounds.miny()
                || rounded.y > bounds.maxy()
            {
                return Ok(());
            }
            let exact = if rounded.x == bounds.minx()
                || rounded.x == bounds.maxx()
                || rounded.y == bounds.miny()
                || rounded.y == bounds.maxy()
            {
                Some(
                    crossing
                        .exact
                        .map_or_else(|| crossing_homogeneous(a, b), Ok)?,
                )
            } else {
                crossing.exact
            };
            if let Some(exact_point) = &exact
                && !homogeneous_point_in_bounds(exact_point, bounds)?
            {
                return Ok(());
            }
            match exact {
                Some(exact) => forest.push_crossing_with_exact(segments, rounded, exact)?,
                None => forest.push_crossing(segments)?,
            }
        } else {
            forest.push_crossing(segments)?
        };
        push_contact(contacts, left, handle)?;
        push_contact(contacts, right, handle)?;
    }
    Ok(())
}

fn homogeneous_point_in_bounds(point: &[ExactDyadic; 3], bounds: Bounds) -> Result<bool> {
    let [x, y, w] = point;
    let one = ExactDyadic::one();
    Ok(
        compare_ratios(x, w, &ExactDyadic::from_f64(bounds.minx()), &one)?.is_ge()
            && compare_ratios(x, w, &ExactDyadic::from_f64(bounds.maxx()), &one)?.is_le()
            && compare_ratios(y, w, &ExactDyadic::from_f64(bounds.miny()), &one)?.is_ge()
            && compare_ratios(y, w, &ExactDyadic::from_f64(bounds.maxy()), &one)?.is_le(),
    )
}

#[expect(
    clippy::indexing_slicing,
    reason = "atom IDs are assigned from spans and their checked provenance CSR counts"
)]
pub(super) fn aggregate_atoms(
    intervals: Vec<(u32, u32, u32)>,
    source_provenance: &SegmentProvenance,
    baseline: usize,
    mut budget: Option<&mut ExpansionBudget>,
) -> Result<(Vec<ExactSpan>, SegmentProvenance)> {
    let mut atom_of: HashMap<(u32, u32), u32> =
        HashMap::with_hasher(crate::collections::collection_hash_state()?.clone());
    atom_of.reserve(intervals.len());
    let mut spans: Vec<ExactSpan> = Vec::new();
    let mut contributors: Vec<(u32, u32, bool)> = Vec::with_capacity(intervals.len());
    for (unique, start, end) in intervals {
        let lo = start.min(end);
        let hi = start.max(end);
        if let Some(&atom) = atom_of.get(&(lo, hi)) {
            let reversed = start != spans[atom as usize].start.0;
            contributors.push((atom, unique, reversed));
            continue;
        }
        if spans.len() >= baseline
            && let Some(budget) = budget.as_deref_mut()
        {
            budget.add(1)?;
        }
        let atom = u32::try_from(spans.len()).map_err(|_| ResourceError::SizeOverflow)?;
        spans.push(ExactSpan {
            start: NodeId(start),
            end: NodeId(end),
            unique,
        });
        atom_of.insert((lo, hi), atom);
        contributors.push((atom, unique, false));
    }
    drop(atom_of);

    let atom_count = spans.len();
    let mut counts = vec![0_u32; atom_count];
    for &(atom, unique, _) in &contributors {
        let count = u32::try_from(source_provenance.owners(unique as usize).len())
            .map_err(|_| ResourceError::SizeOverflow)?;
        counts[atom as usize] = counts[atom as usize]
            .checked_add(count)
            .ok_or(ResourceError::SizeOverflow)?;
    }
    let offsets = super::placement_offsets(&mut counts)?;
    let mut cursor = counts;
    let mut entries: Vec<SegmentSource> = Vec::with_capacity(offsets[atom_count] as usize);
    entries.resize(offsets[atom_count] as usize, (0, false));
    for &(atom, unique, reversed_geom) in &contributors {
        for &(ordinal, reversed) in source_provenance.owners(unique as usize) {
            let slot = cursor[atom as usize] as usize;
            entries[slot] = (ordinal, reversed != reversed_geom);
            cursor[atom as usize] = cursor[atom as usize].wrapping_add(1);
        }
    }
    Ok((spans, SegmentProvenance::from_csr(offsets, entries)))
}
