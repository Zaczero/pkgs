//! Certified binary64 coordinates for exact-partition output.
//!
//! One node-coordinate table is checked for live-node identity, prescribed
//! chord incidence, cyclic order, and optional global-face correspondence.
//! Emissions carry the topology labels that those checks need; selection stays
//! in the exact partition.

use std::borrow::Cow;
use std::cmp::Ordering;
use std::mem;
use std::ops::ControlFlow;

use super::rays::{EmittedPoints, shoot_left};
use super::{ExactPartition, NodeId};
use crate::collections::try_hash_map_with_capacity;
use crate::geometry::arrangement::{IndexedTopology, compare_departures};
use crate::geometry::overlay::exact_partition::emission::emitted_pair_preserves_incidence;
use crate::geometry::{
    GeometryErrorKind, PointKey, Result, Segment, XY, for_each_candidate_pair, single_chain,
};

/// The coordinates used to check an emission. Nearest borrows the immutable
/// arena table; an accepted search assignment owns a patched copy.
pub(super) struct Embedding<'p> {
    table: Cow<'p, [XY]>,
}

impl<'p> Embedding<'p> {
    pub(super) fn nearest(partition: &'p ExactPartition<'_>) -> Self {
        Self {
            table: Cow::Borrowed(&partition.points.rounded),
        }
    }

    pub(super) fn from_owned(table: Vec<XY>) -> Self {
        Self {
            table: Cow::Owned(table),
        }
    }

    pub(super) fn coordinates(&self) -> &[XY] {
        self.table.as_ref()
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "each NodeId is admitted by the same ExactPartition whose coordinate table this embedding owns"
    )]
    pub(super) fn xy(&self, node: NodeId) -> XY {
        self.table[node.0 as usize]
    }

    pub(super) fn segment(&self, [start, end]: [NodeId; 2]) -> Segment {
        Segment {
            start: self.xy(start),
            end: self.xy(end),
        }
    }
}

/// The chords, topology darts, and optional face labels one output publishes.
pub(super) struct Emission {
    pub(super) items: Vec<[NodeId; 2]>,
    pub(super) chords: usize,
    pub(super) darts: Vec<[u32; 2]>,
    pub(super) faces: Option<Vec<[u32; 2]>>,
}

impl Emission {
    pub(super) fn incident_items(items: Vec<[NodeId; 2]>) -> Self {
        let chords = items.len();
        Self {
            items,
            chords,
            darts: Vec::new(),
            faces: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Witness {
    Collision(NodeId, NodeId),
    Contact {
        emission: usize,
        left: usize,
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

/// Certify one shared coordinate table against every emission. `topology` is
/// absent only for the temporary incidence-only line path; emissions carrying
/// darts always provide it.
#[expect(
    clippy::indexing_slicing,
    reason = "emission item, segment, dart, face, and node IDs are built from the same partition and aligned emission"
)]
pub(super) fn certify<S: Sink>(
    partition: &ExactPartition<'_>,
    topology: Option<&IndexedTopology>,
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
    if let ControlFlow::Break(()) =
        for_each_candidate_pair(&contact_segments, single_chain, |left, right| {
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
                    emission: left_emission,
                    left: left_item,
                    right: right_item,
                });
                if flow.is_continue() {
                    prior_witness = true;
                }
                flow
            }
        })?
    {
        return Ok(ControlFlow::Break(()));
    }

    for (emission_index, emission) in emissions.iter().enumerate() {
        if emission.darts.is_empty() {
            continue;
        }
        let topology = topology.expect("emissions with darts require their indexed topology");
        debug_assert_eq!(emission.chords, emission.darts.len());
        debug_assert!(emission.chords <= segments[emission_index].len());
        let flow = visit_fans(
            topology,
            &emission.darts,
            &segments[emission_index][..emission.chords],
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

/// Return the nearest table only after its applicable certificate passes.
pub(super) fn embed<'p>(
    partition: &'p ExactPartition<'_>,
    topology: Option<&IndexedTopology>,
    emissions: &[&Emission],
) -> Result<(Embedding<'p>, Vec<Vec<Segment>>)> {
    let embedding = Embedding::nearest(partition);
    let segments: Vec<Vec<_>> = emissions
        .iter()
        .map(|emission| {
            emission
                .items
                .iter()
                .map(|&item| embedding.segment(item))
                .collect()
        })
        .collect();
    let segment_views: Vec<_> = segments.iter().map(Vec::as_slice).collect();
    let mut needs_certificate = false;
    for emission in emissions {
        needs_certificate |= partition.rounding_moves(&emission.items);
    }
    if needs_certificate {
        let mut sink = FirstFailure::default();
        if certify(
            partition,
            topology,
            &embedding,
            emissions,
            &segment_views,
            &mut sink,
        )?
        .is_break()
        {
            return Err(sink.into_error());
        }
    }
    Ok((embedding, segments))
}

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

#[expect(
    clippy::indexing_slicing,
    reason = "darts and segments are paired from the same boundary-plan emission and topology"
)]
pub(super) fn check_emitted_fans(
    topology: &IndexedTopology,
    darts: &[[u32; 2]],
    segments: &[Segment],
) -> Result<()> {
    let mut sink = FirstFailure::default();
    let mut had_witness = false;
    if visit_fans(topology, darts, segments, &mut had_witness, &mut sink).is_break() {
        return Err(sink.into_error());
    }
    Ok(())
}

#[expect(
    clippy::indexing_slicing,
    reason = "dart IDs and fan ranges are produced by local_topology and the emitted-dart vector is sized from those slots"
)]
fn visit_fans<S: Sink>(
    topology: &IndexedTopology,
    darts: &[[u32; 2]],
    segments: &[Segment],
    had_witness: &mut bool,
    sink: &mut S,
) -> ControlFlow<()> {
    debug_assert_eq!(darts.len(), segments.len());
    let mut departures: Vec<Option<(XY, XY)>> = Vec::with_capacity(topology.targets.len());
    departures.resize(topology.targets.len(), None);
    for (&[start, end], segment) in darts.iter().zip(segments) {
        debug_assert!(departures[start as usize].is_none() && departures[end as usize].is_none());
        departures[start as usize] = Some((segment.start, segment.end));
        departures[end as usize] = Some((segment.end, segment.start));
    }
    for (node, &[start, end]) in topology.starts.array_windows::<2>().enumerate() {
        let mut first = None;
        let mut previous = None;
        let mut descents = 0_usize;
        let mut count = 0_usize;
        for (target, departure) in (start..end).zip(&mut departures[start as usize..end as usize]) {
            let Some((origin, point)) = departure.take() else {
                continue;
            };
            let current = (target, point);
            first.get_or_insert((origin, current));
            if let Some((last_target, last_point)) = previous {
                descents = descents.wrapping_add(usize::from(
                    compare_departures(origin, last_point, last_target, point, target)
                        == Ordering::Greater,
                ));
            }
            previous = Some(current);
            count = count.wrapping_add(1);
        }
        if count > 2 {
            let (origin, (first_target, first_point)) =
                first.expect("a node with multiple emitted darts has a first dart");
            let (last_target, last_point) =
                previous.expect("a node with multiple emitted darts has a last dart");
            descents = descents.wrapping_add(usize::from(
                compare_departures(origin, last_point, last_target, first_point, first_target)
                    == Ordering::Greater,
            ));
            if descents != 1 {
                *had_witness = true;
                let flow = sink.report(Witness::Fan(NodeId(node as u32)));
                if flow.is_break() {
                    return flow;
                }
            }
        }
    }
    debug_assert!(
        departures.iter().all(Option::is_none),
        "emitted chords must retain exact dart identities"
    );
    ControlFlow::Continue(())
}

#[expect(
    clippy::indexing_slicing,
    reason = "emitted NodeIds index the partition and face labels and item ordinals are built from the same emission"
)]
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
