//! Certified binary64 coordinates for exact-partition output.
//!
//! One node-coordinate table is checked for live-node identity, prescribed
//! chord incidence, cyclic order, and optional global-face correspondence.
//! Emissions carry the topology labels that those checks need; selection stays
//! in the exact partition.
//!
//! Every emitted item, including an isolated point as `[n, n]`, is checked
//! for identity and pairwise incidence. Pure linework has no areal faces and
//! skips fan and global-face checks. A face-bearing emission checks fan order
//! at each node incident to an areal chord, including every emitted chord at
//! that node. Fans with at most two departures have a unique cyclic order and
//! satisfy F vacuously. Global-face correspondence covers every component and
//! isolated point in the emission.

use std::borrow::Cow;
use std::cmp::Ordering;
use std::mem;
use std::ops::ControlFlow;

use super::rays::{EmittedPoints, shoot_left};
use super::{ExactPartition, NodeId};
use crate::collections::try_hash_map_with_capacity;
use crate::geometry::arrangement::compare_departures;
use crate::geometry::overlay::exact_partition::emission::emitted_pair_preserves_incidence;
use crate::geometry::{
    GeometryErrorKind, PointKey, Result, Segment, XY, for_each_candidate_pair, single_chain,
};

/// The coordinates used to check an emission. Nearest borrows the immutable
/// arena table; an accepted search assignment owns a patched copy.
#[derive(Debug)]
pub(super) struct Embedding<'p> {
    table: Cow<'p, [XY]>,
}

impl<'p> Embedding<'p> {
    pub(super) fn nearest(partition: &'p ExactPartition<'_>) -> Self {
        Self {
            table: Cow::Borrowed(&partition.points.rounded),
        }
    }

    #[cfg(test)]
    pub(super) const fn from_owned(table: Vec<XY>) -> Self {
        Self {
            table: Cow::Owned(table),
        }
    }

    /// Borrow once at serialization boundaries instead of redispatching `Cow`
    /// for each emitted node.
    pub(super) fn coordinates(&self) -> &[XY] {
        self.table.as_ref()
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "each NodeId is admitted by the same ExactPartition whose coordinate table this embedding owns"
    )]
    pub(super) fn xy(&self, node: NodeId) -> XY {
        Self::xy_at(self.coordinates(), node)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "the node ID and coordinate slice are paired from the same certified embedding"
    )]
    pub(super) fn xy_at(coordinates: &[XY], node: NodeId) -> XY {
        coordinates[node.0 as usize]
    }

    pub(super) fn segment_at(coordinates: &[XY], [start, end]: [NodeId; 2]) -> Segment {
        Segment {
            start: Self::xy_at(coordinates, start),
            end: Self::xy_at(coordinates, end),
        }
    }
}

/// One output's chords and points. Areal boundary chords form the initial
/// `areal_chords` prefix; face-bearing emissions retain fan spans and host
/// labels for every chord and point.
pub(super) struct Emission {
    pub(super) items: Vec<[NodeId; 2]>,
    pub(super) chords: usize,
    pub(super) areal_chords: usize,
    pub(super) fan_spans: Vec<[u32; 2]>,
    pub(super) faces: Option<Vec<[u32; 2]>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Witness {
    Collision(NodeId, NodeId),
    Contact {
        left_emission: usize,
        left: usize,
        right_emission: usize,
        right: usize,
    },
    Fan(NodeId),
    Face {
        emission: usize,
        root: NodeId,
        hit: Option<usize>,
    },
}

impl Witness {
    fn into_error(self) -> crate::error::Error {
        match self {
            Self::Collision(first, second) => GeometryErrorKind::ExactNodeCollision {
                first: first.0,
                second: second.0,
            }
            .into(),
            Self::Contact { left, right, .. } => GeometryErrorKind::ExactUnexpectedContact {
                first: left,
                second: right,
            }
            .into(),
            Self::Fan(node) => GeometryErrorKind::ExactCyclicOrder { node: node.0 }.into(),
            Self::Face { root, .. } => GeometryErrorKind::ExactGlobalFace { node: root.0 }.into(),
        }
    }
}

pub(super) trait Sink {
    fn report(&mut self, witness: Witness) -> ControlFlow<()>;
}

#[derive(Default)]
pub(super) struct FirstFailure {
    witness: Option<Witness>,
}

impl FirstFailure {
    fn into_error(self) -> crate::error::Error {
        self.witness
            .expect("a broken certificate reports its witness")
            .into_error()
    }
}

impl Sink for FirstFailure {
    fn report(&mut self, witness: Witness) -> ControlFlow<()> {
        debug_assert!(self.witness.is_none());
        self.witness = Some(witness);
        ControlFlow::Break(())
    }
}

struct Injectivity {
    owners: crate::collections::HashMap<PointKey, NodeId>,
}

impl Injectivity {
    fn new(capacity: usize) -> Result<Self> {
        Ok(Self {
            owners: try_hash_map_with_capacity(capacity)?,
        })
    }

    fn witness(&mut self, node: NodeId, coordinate: XY) -> Option<Witness> {
        self.owners
            .insert(PointKey::new(coordinate), node)
            .filter(|&first| first != node)
            .map(|first| Witness::Collision(first, node))
    }
}

/// Certify one shared coordinate table against every emission.
#[expect(
    clippy::indexing_slicing,
    reason = "emission item, segment, span, face, and node IDs are built from the same partition and aligned emission"
)]
pub(super) fn certify<S: Sink>(
    partition: &ExactPartition<'_>,
    embedding: &Embedding<'_>,
    emissions: &[&Emission],
    segments: &[&[Segment]],
    sink: &mut S,
) -> Result<ControlFlow<()>> {
    debug_assert_eq!(emissions.len(), segments.len());
    let mut prior_witness = false;

    let mut seen = Vec::with_capacity(partition.node_count());
    seen.resize(partition.node_count(), false);
    let mut injectivity = Injectivity::new(
        emissions
            .iter()
            .map(|emission| emission.items.len())
            .fold(0, usize::wrapping_add),
    )?;
    for emission in emissions {
        for &[start, end] in &emission.items {
            for node in [start, end] {
                if mem::replace(&mut seen[node.0 as usize], true) {
                    continue;
                }
                if let Some(witness) = injectivity.witness(node, embedding.xy(node)) {
                    let flow = sink.report(witness);
                    if flow.is_break() {
                        return Ok(flow);
                    }
                    prior_witness = true;
                }
            }
        }
    }

    let mut contact_items = Vec::new();
    let mut contact_segments = Vec::new();
    let contact_capacity = emissions
        .iter()
        .map(|emission| emission.items.len())
        .fold(0, usize::wrapping_add);
    contact_items.reserve(contact_capacity);
    contact_segments.reserve(contact_capacity);
    let mut seen_chords = (emissions.len() > 1)
        .then(|| try_hash_map_with_capacity::<[u32; 2], ()>(contact_capacity))
        .transpose()?;
    for (emission_index, emission) in emissions.iter().enumerate() {
        for (item_index, &[start, end]) in emission.items.iter().enumerate() {
            if let Some(seen_chords) = &mut seen_chords {
                let key = [start.0.min(end.0), start.0.max(end.0)];
                if seen_chords.insert(key, ()).is_some() {
                    continue;
                }
            }
            contact_items.push((emission_index, item_index));
            contact_segments.push(segments[emission_index][item_index]);
        }
    }
    if for_each_candidate_pair(&contact_segments, single_chain, |left, right| {
        let (left_emission, left_item) = contact_items[left];
        let (right_emission, right_item) = contact_items[right];
        let left_nodes = emissions[left_emission].items[left_item];
        let right_nodes = emissions[right_emission].items[right_item];
        let left_segment = contact_segments[left];
        let right_segment = contact_segments[right];
        let endpoints = |[start, end]: [NodeId; 2], segment: Segment| {
            [(start, segment.start), (end, segment.end)]
        };
        if emitted_pair_preserves_incidence(
            endpoints(left_nodes, left_segment),
            endpoints(right_nodes, right_segment),
        ) {
            ControlFlow::Continue(())
        } else {
            let flow = sink.report(Witness::Contact {
                left_emission,
                left: left_item,
                right_emission,
                right: right_item,
            });
            if flow.is_continue() {
                prior_witness = true;
            }
            flow
        }
    })?
    .is_break()
    {
        return Ok(ControlFlow::Break(()));
    }

    for (emission_index, emission) in emissions.iter().enumerate() {
        debug_assert_eq!(emission.faces.is_some(), emission.areal_chords > 0);
        if emission.faces.is_none() {
            continue;
        }
        debug_assert!(emission.areal_chords <= emission.chords);
        debug_assert_eq!(emission.chords, emission.fan_spans.len());
        debug_assert!(emission.chords <= segments[emission_index].len());
        let flow = visit_fans(
            partition,
            embedding,
            &emission.items[..emission.chords],
            &emission.fan_spans,
            &segments[emission_index][..emission.chords],
            emission.areal_chords,
            &mut prior_witness,
            sink,
        );
        if flow.is_break() {
            return Ok(flow);
        }
    }

    if prior_witness {
        return Ok(ControlFlow::Continue(()));
    }

    for (emission_index, emission) in emissions.iter().enumerate() {
        let Some(faces) = &emission.faces else {
            continue;
        };
        debug_assert_eq!(emission.items.len(), faces.len());
        debug_assert_eq!(emission.items.len(), segments[emission_index].len());
        let flow = visit_faces(
            partition,
            embedding,
            &emission.items,
            faces,
            segments[emission_index],
            emission_index,
            sink,
        )?;
        if flow.is_break() {
            return Ok(flow);
        }
    }
    Ok(ControlFlow::Continue(()))
}

pub(super) fn rounding_moves(partition: &ExactPartition<'_>, items: &[[NodeId; 2]]) -> bool {
    let moved = items
        .iter()
        .flatten()
        .any(|&node| !partition.is_stored(node));
    #[cfg(test)]
    if !moved {
        super::metrics::update(|metrics| {
            metrics.unmoved_emissions = metrics.unmoved_emissions.wrapping_add(1);
        });
    }
    moved
}

impl ExactPartition<'_> {
    /// Return one nearest-coordinate table only after every emission passes its
    /// complete certificate.
    pub(super) fn embed<'p>(
        &'p self,
        emissions: &[&Emission],
        _operation: &'static str,
    ) -> Result<Embedding<'p>> {
        let embedding = Embedding::nearest(self);
        let mut needs_certificate = false;
        for emission in emissions {
            needs_certificate |= rounding_moves(self, &emission.items);
        }
        if needs_certificate {
            let coordinates = embedding.coordinates();
            let segments: Vec<Vec<_>> = emissions
                .iter()
                .map(|emission| {
                    emission
                        .items
                        .iter()
                        .map(|&item| Embedding::segment_at(coordinates, item))
                        .collect()
                })
                .collect();
            let segment_views: Vec<_> = segments.iter().map(Vec::as_slice).collect();
            let mut sink = FirstFailure::default();
            if certify(self, &embedding, emissions, &segment_views, &mut sink)?.is_break() {
                return Err(sink.into_error());
            }
        }
        Ok(embedding)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "each selected span ID comes from this ExactPartition's span domain"
    )]
    pub(super) fn span_emission(&self, spans: impl IntoIterator<Item = u32>) -> Emission {
        let mut items = Vec::new();
        for span_id in spans {
            let span = self.spans[span_id as usize];
            items.push([span.start, span.end]);
        }
        let chords = items.len();
        Emission {
            items,
            chords,
            areal_chords: 0,
            fan_spans: Vec::new(),
            faces: None,
        }
    }
}

#[cfg(test)]
pub(super) fn check_node_injectivity(nodes: &[NodeId], coordinates: &[XY]) -> Result<()> {
    debug_assert_eq!(nodes.len(), coordinates.len());
    let mut injectivity = Injectivity::new(nodes.len())?;
    for (&node, &coordinate) in nodes.iter().zip(coordinates) {
        if let Some(witness) = injectivity.witness(node, coordinate) {
            return Err(witness.into_error());
        }
    }
    Ok(())
}

#[cfg(test)]
pub(super) fn check_emitted_fans(
    partition: &ExactPartition<'_>,
    embedding: &Embedding<'_>,
    items: &[[NodeId; 2]],
    fan_spans: &[[u32; 2]],
    segments: &[Segment],
    areal_chords: usize,
) -> Result<()> {
    let mut sink = FirstFailure::default();
    let mut had_witness = false;
    if visit_fans(
        partition,
        embedding,
        items,
        fan_spans,
        segments,
        areal_chords,
        &mut had_witness,
        &mut sink,
    )
    .is_break()
    {
        return Err(sink.into_error());
    }
    Ok(())
}

#[derive(Clone, Copy, Default)]
struct FanDeparture {
    span: u32,
    item: usize,
    at_start: bool,
}

#[expect(
    clippy::indexing_slicing,
    reason = "emitted item, span and node IDs are admitted by the same exact partition"
)]
fn visit_fans<S: Sink>(
    partition: &ExactPartition<'_>,
    embedding: &Embedding<'_>,
    items: &[[NodeId; 2]],
    fan_spans: &[[u32; 2]],
    segments: &[Segment],
    areal_chords: usize,
    had_witness: &mut bool,
    sink: &mut S,
) -> ControlFlow<()> {
    debug_assert_eq!(items.len(), fan_spans.len());
    debug_assert_eq!(items.len(), segments.len());
    debug_assert!(areal_chords <= items.len());
    let node_count = partition.node_count();
    let mut areal_nodes = vec![false; node_count];
    for &[start, end] in &items[..areal_chords] {
        areal_nodes[start.0 as usize] = true;
        areal_nodes[end.0 as usize] = true;
    }
    let mut starts = vec![0_usize; node_count.wrapping_add(1)];
    for &[start, end] in items {
        let start = start.0 as usize;
        let end = end.0 as usize;
        starts[start.wrapping_add(1)] = starts[start.wrapping_add(1)].wrapping_add(1);
        starts[end.wrapping_add(1)] = starts[end.wrapping_add(1)].wrapping_add(1);
    }
    let mut total = 0_usize;
    for offset in starts.iter_mut().skip(1) {
        total = total.wrapping_add(*offset);
        *offset = total;
    }
    let mut cursor = starts[..node_count].to_vec();
    let mut departures = Vec::new();
    departures.resize(total, FanDeparture::default());
    for (item, &[start, end]) in items.iter().enumerate() {
        let [start_span, end_span] = fan_spans[item];
        let start_position = cursor[start.0 as usize];
        departures[start_position] = FanDeparture {
            span: start_span,
            item,
            at_start: true,
        };
        cursor[start.0 as usize] = start_position.wrapping_add(1);
        let end_position = cursor[end.0 as usize];
        departures[end_position] = FanDeparture {
            span: end_span,
            item,
            at_start: false,
        };
        cursor[end.0 as usize] = end_position.wrapping_add(1);
    }
    for node in 0..node_count {
        let range = starts[node]..starts[node.wrapping_add(1)];
        if !areal_nodes[node] || range.len() <= 2 {
            continue;
        }
        let origin = NodeId(node as u32);
        let row = &mut departures[range];
        row.sort_unstable_by(|left, right| {
            partition.compare_departures(origin, left.span as usize, right.span as usize)
        });
        let endpoint = |departure: FanDeparture| {
            let [start, end] = items[departure.item];
            let segment = segments[departure.item];
            if departure.at_start {
                (end, segment.end)
            } else {
                (start, segment.start)
            }
        };
        let origin_xy = embedding.xy(origin);
        let mut first: Option<(NodeId, XY)> = None;
        let mut previous: Option<(NodeId, XY)> = None;
        let mut descents = 0_usize;
        for &departure in row.iter() {
            let (target, point) = endpoint(departure);
            first.get_or_insert((target, point));
            if let Some((last_target, last_point)) = previous {
                descents = descents.wrapping_add(usize::from(
                    compare_departures(origin_xy, last_point, last_target.0, point, target.0)
                        == Ordering::Greater,
                ));
            }
            previous = Some((target, point));
        }
        let (first_target, first_point) = first.expect("a fan row has a first departure");
        let (last_target, last_point) = previous.expect("a fan row has a last departure");
        descents = descents.wrapping_add(usize::from(
            compare_departures(
                origin_xy,
                last_point,
                last_target.0,
                first_point,
                first_target.0,
            ) == Ordering::Greater,
        ));
        if descents != 1 {
            *had_witness = true;
            let flow = sink.report(Witness::Fan(origin));
            if flow.is_break() {
                return flow;
            }
        }
    }
    ControlFlow::Continue(())
}

#[cfg(test)]
pub(super) fn check_global_faces(
    partition: &ExactPartition<'_>,
    embedding: &Embedding<'_>,
    items: &[[NodeId; 2]],
    sides: &[[u32; 2]],
    segments: &[Segment],
) -> Result<()> {
    let mut sink = FirstFailure::default();
    if visit_faces(partition, embedding, items, sides, segments, 0, &mut sink)?.is_break() {
        return Err(sink.into_error());
    }
    Ok(())
}

#[expect(
    clippy::indexing_slicing,
    clippy::float_cmp,
    reason = "emitted node IDs index the certified coordinate table; face labels and item ordinals are built from those same items"
)]
fn visit_faces<S: Sink>(
    partition: &ExactPartition<'_>,
    embedding: &Embedding<'_>,
    items: &[[NodeId; 2]],
    sides: &[[u32; 2]],
    segments: &[Segment],
    emission: usize,
    sink: &mut S,
) -> Result<ControlFlow<()>> {
    debug_assert_eq!(sides.len(), items.len());
    debug_assert_eq!(segments.len(), items.len());
    let node_count = partition.node_count();
    let coordinates = embedding.coordinates();
    let incidence_rows = node_count.wrapping_add(1);
    let mut incident_starts = vec![0_usize; incidence_rows];
    let mut parents = Vec::with_capacity(node_count);
    parents.extend(0..node_count as u32);
    for (&[start, end], _) in items.iter().zip(segments) {
        let start_node = start.0 as usize;
        let start_offset = start_node.wrapping_add(1);
        incident_starts[start_offset] = incident_starts[start_offset].wrapping_add(1);
        if end != start {
            let end_node = end.0 as usize;
            let end_offset = end_node.wrapping_add(1);
            incident_starts[end_offset] = incident_starts[end_offset].wrapping_add(1);
        }
        let mut left = start.0;
        let mut right = end.0;
        while parents[left as usize] != left {
            parents[left as usize] = parents[parents[left as usize] as usize];
            left = parents[left as usize];
        }
        while parents[right as usize] != right {
            parents[right as usize] = parents[parents[right as usize] as usize];
            right = parents[right as usize];
        }
        parents[left.max(right) as usize] = left.min(right);
    }
    super::construction::prefix_contact_offsets(&mut incident_starts);
    let incident_count = incident_starts[node_count];
    let mut incident = vec![0_usize; incident_count];
    {
        let mut cursor = Vec::with_capacity(node_count);
        cursor.extend(incident_starts.iter().take(node_count).copied());
        for (index, &[start, end]) in items.iter().enumerate() {
            for node in [start, end]
                .into_iter()
                .take(if end == start { 1 } else { 2 })
            {
                let node = node.0 as usize;
                let position = cursor[node];
                incident[position] = index;
                cursor[node] = position.wrapping_add(1);
            }
        }
    }
    let mut roots: Vec<Option<(usize, XY)>> = Vec::with_capacity(node_count);
    roots.resize(node_count, None);
    for node in 0..node_count {
        if incident_starts[node] == incident_starts[node.wrapping_add(1)] {
            continue;
        }
        let mut root = node as u32;
        while parents[root as usize] != root {
            root = parents[root as usize];
        }
        let point = coordinates[node];
        let entry = &mut roots[root as usize];
        if entry.is_none_or(|(_, prior): (usize, XY)| {
            point.x < prior.x || (point.x == prior.x && point.y < prior.y)
        }) {
            *entry = Some((node, point));
        }
    }
    drop(parents);
    let mut queries = Vec::new();
    let mut exteriors = Vec::new();
    for &(node, point) in roots.iter().flatten() {
        let mut exterior = None;
        let mut chosen = None;
        let mut has_upper = false;
        let range = incident_starts[node]..incident_starts[node.wrapping_add(1)];
        for &index in &incident[range] {
            let [start, end] = items[index];
            let (target, target_point, face) = if start.0 as usize == node {
                (end.0, segments[index].end, sides[index][0])
            } else {
                (start.0, segments[index].start, sides[index][1])
            };
            if target as usize == node {
                exterior = Some(face);
                continue;
            }
            let upper = target_point.y >= point.y;
            if chosen.is_none()
                || (upper && !has_upper)
                || (upper == has_upper && {
                    let (prior_target, prior_point) =
                        chosen.expect("a selected departure exists before comparing the next one");
                    compare_departures(point, prior_point, prior_target, target_point, target)
                        == Ordering::Less
                })
            {
                chosen = Some((target, target_point));
                exterior = Some(face);
                has_upper = upper;
            }
        }
        queries.push(node as u32);
        exteriors.push(exterior.expect("a component root has an incident emitted edge"));
    }
    drop(roots);
    drop(incident);
    drop(incident_starts);
    let hits = shoot_left(
        &EmittedPoints(coordinates),
        items.len(),
        |index| [items[index][0].0, items[index][1].0],
        &queries,
    )?;
    for ((&node, &exterior), hit) in queries.iter().zip(&exteriors).zip(hits) {
        let host = hit.map_or(0, |chord| {
            let segment = segments[chord];
            if segment.start.y < segment.end.y {
                sides[chord][1]
            } else {
                sides[chord][0]
            }
        });
        if host != exterior {
            let flow = sink.report(Witness::Face {
                emission,
                root: NodeId(node),
                hit,
            });
            if flow.is_break() {
                return Ok(flow);
            }
        }
    }
    Ok(ControlFlow::Continue(()))
}
