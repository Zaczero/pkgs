//! Exact source partition of unique stored segments.
//!
//! Borrows unique undirected representatives and existing CSR provenance.
//! Constructed nodes are identified by pair-shared events and equal-param
//! union, not by interned coordinates. This is the noding kernel foundation;
//! arrangement, overlay, and checked embedding are later cuts.

use std::cmp::Ordering;
use std::ops::ControlFlow;

use crate::error::ResourceError;
use crate::geometry::exact_arith::ExactSign;
use crate::geometry::overlay::noding::{SegmentProvenance, SegmentSource};
use crate::geometry::segments::stored_orientation_sign;
use crate::geometry::tessellation::exact::{ExactDyadic, try_compare_ratios};
use crate::geometry::{
    ExpansionBudget, HashMap, HashMapExt as _, PointKey, Result, Segment, XY,
    for_each_candidate_pair, same_point, segment_envelopes_disjoint, single_chain,
};
use crate::try_reserve_vec;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub(in crate::geometry) struct NodeId(pub u32);

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::geometry) struct ExactSpan {
    pub start: NodeId,
    pub end: NodeId,
    pub unique: u32,
    pub owners: Vec<SegmentSource>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::geometry) enum NodeOrigin {
    Stored(PointKey),
    Constructed,
}

#[derive(Debug)]
pub(in crate::geometry) struct ExactPartition {
    pub origins: Vec<NodeOrigin>,
    pub spans: Vec<ExactSpan>,
    pub isolated: Vec<NodeId>,
}

impl ExactPartition {
    pub(in crate::geometry) fn node_count(&self) -> usize {
        self.origins.len()
    }

    pub(in crate::geometry) fn stored_node(&self, point: XY) -> Option<NodeId> {
        let key = PointKey::new(point);
        self.origins
            .iter()
            .position(|origin| *origin == NodeOrigin::Stored(key))
            .map(|index| NodeId(index as u32))
    }

    pub(in crate::geometry) fn constructed_count(&self) -> usize {
        self.origins
            .iter()
            .filter(|origin| matches!(origin, NodeOrigin::Constructed))
            .count()
    }

    pub(in crate::geometry) fn component_count(&self) -> usize {
        let n = self.origins.len();
        if n == 0 {
            return 0;
        }
        let mut parent: Vec<u32> = (0..n as u32).collect();
        let find = |parent: &mut [u32], mut node: u32| -> u32 {
            while parent[node as usize] != node {
                parent[node as usize] = parent[parent[node as usize] as usize];
                node = parent[node as usize];
            }
            node
        };
        for span in &self.spans {
            let a = find(&mut parent, span.start.0);
            let b = find(&mut parent, span.end.0);
            if a != b {
                parent[b as usize] = a;
            }
        }
        let mut roots = Vec::new();
        for node in 0..n as u32 {
            let root = find(&mut parent, node);
            if !roots.contains(&root) {
                roots.push(root);
            }
        }
        roots.len()
    }
}

struct Param {
    num: ExactDyadic,
    den: ExactDyadic,
}

impl Param {
    fn zero() -> Result<Self> {
        Ok(Self {
            num: ExactDyadic::try_from_f64(0.0)?,
            den: ExactDyadic::try_from_f64(1.0)?,
        })
    }

    fn one() -> Result<Self> {
        Ok(Self {
            num: ExactDyadic::try_from_f64(1.0)?,
            den: ExactDyadic::try_from_f64(1.0)?,
        })
    }

    fn cmp(&self, other: &Self) -> Result<Ordering> {
        try_compare_ratios(&self.num, &self.den, &other.num, &other.den)
    }

    fn is_zero(&self) -> bool {
        self.num.is_zero()
    }

    fn is_open_unit(&self) -> Result<bool> {
        if self.is_zero() {
            return Ok(false);
        }
        let zero = Param::zero()?;
        let one = Param::one()?;
        Ok(self.cmp(&zero)? == Ordering::Greater && self.cmp(&one)? == Ordering::Less)
    }
}

struct Event {
    left: u32,
    right: u32,
    t: Param,
    u: Param,
    handle: u32,
}

struct UnionFind {
    parent: Vec<u32>,
}

impl UnionFind {
    fn with_len(len: u32) -> Result<Self> {
        let mut parent = Vec::new();
        try_reserve_vec(&mut parent, len as usize)?;
        parent.extend(0..len);
        Ok(Self { parent })
    }

    fn find(&mut self, mut node: u32) -> u32 {
        while self.parent[node as usize] != node {
            self.parent[node as usize] = self.parent[self.parent[node as usize] as usize];
            node = self.parent[node as usize];
        }
        node
    }

    fn union(&mut self, a: u32, b: u32) {
        let a = self.find(a);
        let b = self.find(b);
        if a != b {
            self.parent[b as usize] = a;
        }
    }

    fn push(&mut self) -> Result<u32> {
        let id = u32::try_from(self.parent.len()).map_err(|_| ResourceError::SizeOverflow)?;
        try_reserve_vec(&mut self.parent, 1)?;
        self.parent.push(id);
        Ok(id)
    }
}

pub(in crate::geometry) fn partition_unique_segments(
    unique: &[Segment],
    provenance: &SegmentProvenance,
    isolated: &[XY],
    budget: &mut ExpansionBudget,
) -> Result<ExactPartition> {
    partition_inner(unique, provenance, isolated, budget, true)
}

fn partition_inner(
    unique: &[Segment],
    provenance: &SegmentProvenance,
    isolated: &[XY],
    budget: &mut ExpansionBudget,
    union_equal_params: bool,
) -> Result<ExactPartition> {
    debug_assert_eq!(provenance.len(), unique.len());
    let mut endpoint_of: HashMap<PointKey, u32> = HashMap::new();
    endpoint_of
        .try_reserve(
            unique
                .len()
                .saturating_mul(2)
                .saturating_add(isolated.len()),
        )
        .map_err(|_| ResourceError::Allocation)?;
    let mut origins: Vec<NodeOrigin> = Vec::new();
    let intern_stored = |point: XY,
                         origins: &mut Vec<NodeOrigin>,
                         endpoint_of: &mut HashMap<PointKey, u32>,
                         budget: &mut ExpansionBudget|
     -> Result<u32> {
        let key = PointKey::new(point);
        if let Some(&id) = endpoint_of.get(&key) {
            return Ok(id);
        }
        budget.add(1)?;
        let id = u32::try_from(origins.len()).map_err(|_| ResourceError::SizeOverflow)?;
        try_reserve_vec(origins, 1)?;
        origins.push(NodeOrigin::Stored(key));
        endpoint_of.insert(key, id);
        Ok(id)
    };

    let mut start_handle = Vec::new();
    let mut end_handle = Vec::new();
    try_reserve_vec(&mut start_handle, unique.len())?;
    try_reserve_vec(&mut end_handle, unique.len())?;
    let mut uf = UnionFind::with_len(0)?;
    for &segment in unique {
        let start = intern_stored(segment.start, &mut origins, &mut endpoint_of, budget)?;
        let end = intern_stored(segment.end, &mut origins, &mut endpoint_of, budget)?;
        while uf.parent.len() < origins.len() {
            uf.push()?;
        }
        start_handle.push(start);
        end_handle.push(end);
    }
    for &point in isolated {
        intern_stored(point, &mut origins, &mut endpoint_of, budget)?;
        while uf.parent.len() < origins.len() {
            uf.push()?;
        }
    }

    let mut events: Vec<Event> = Vec::new();
    let mut pair_error = None;
    let _ = for_each_candidate_pair(unique, single_chain, |left, right| match classify_and_push(
        unique,
        left,
        right,
        &mut events,
        &mut uf,
        &mut origins,
        &start_handle,
        &end_handle,
        budget,
    ) {
        Ok(()) => ControlFlow::Continue(()),
        Err(error) => {
            pair_error = Some(error);
            ControlFlow::Break(())
        },
    });
    if let Some(error) = pair_error {
        return Err(error);
    }

    if union_equal_params {
        union_equal_params_per_source(unique, &start_handle, &end_handle, &events, &mut uf)?;
    }

    let mut compact_of: Vec<Option<u32>> = Vec::new();
    try_reserve_vec(&mut compact_of, origins.len())?;
    compact_of.resize(origins.len(), None);
    let mut compact_origins: Vec<NodeOrigin> = Vec::new();
    let intern_root = |root: u32,
                       origins: &[NodeOrigin],
                       compact_of: &mut [Option<u32>],
                       compact_origins: &mut Vec<NodeOrigin>|
     -> Result<u32> {
        let root = root as usize;
        if let Some(id) = compact_of[root] {
            return Ok(id);
        }
        let id = u32::try_from(compact_origins.len()).map_err(|_| ResourceError::SizeOverflow)?;
        try_reserve_vec(compact_origins, 1)?;
        compact_origins.push(origins[root]);
        compact_of[root] = Some(id);
        Ok(id)
    };

    let mut spans: Vec<ExactSpan> = Vec::new();
    for (source, &segment) in unique.iter().enumerate() {
        if same_point(segment.start, segment.end) {
            continue;
        }
        let mut keys: Vec<(Param, u32)> = Vec::new();
        try_reserve_vec(&mut keys, 2 + events.len())?;
        keys.push((Param::zero()?, uf.find(start_handle[source])));
        keys.push((Param::one()?, uf.find(end_handle[source])));
        for event in &events {
            if event.left as usize == source {
                keys.push((
                    Param {
                        num: event.t.num.try_clone()?,
                        den: event.t.den.try_clone()?,
                    },
                    uf.find(event.handle),
                ));
            } else if event.right as usize == source {
                keys.push((
                    Param {
                        num: event.u.num.try_clone()?,
                        den: event.u.den.try_clone()?,
                    },
                    uf.find(event.handle),
                ));
            }
        }
        insertion_sort_params(&mut keys)?;
        let mut ordered: Vec<(Param, u32)> = Vec::new();
        try_reserve_vec(&mut ordered, keys.len())?;
        for item in keys {
            if ordered.last().is_some_and(|prev| prev.1 == item.1) {
                continue;
            }
            ordered.push(item);
        }
        for window in ordered.windows(2) {
            if window[0].1 == window[1].1 {
                continue;
            }
            let start = intern_root(window[0].1, &origins, &mut compact_of, &mut compact_origins)?;
            let end = intern_root(window[1].1, &origins, &mut compact_of, &mut compact_origins)?;
            if start == end {
                continue;
            }
            budget.add(1)?;
            let src = provenance.owners(source);
            let mut owners = Vec::new();
            try_reserve_vec(&mut owners, src.len())?;
            owners.extend_from_slice(src);
            try_reserve_vec(&mut spans, 1)?;
            spans.push(ExactSpan {
                start: NodeId(start),
                end: NodeId(end),
                unique: source as u32,
                owners,
            });
        }
    }

    let mut isolated_nodes = Vec::new();
    try_reserve_vec(&mut isolated_nodes, isolated.len())?;
    for &point in isolated {
        let handle = intern_stored(point, &mut origins, &mut endpoint_of, budget)?;
        while uf.parent.len() < origins.len() {
            uf.push()?;
        }
        if compact_of.len() < origins.len() {
            let extra = origins.len() - compact_of.len();
            try_reserve_vec(&mut compact_of, extra)?;
            compact_of.resize(origins.len(), None);
        }
        let id = intern_root(
            uf.find(handle),
            &origins,
            &mut compact_of,
            &mut compact_origins,
        )?;
        isolated_nodes.push(NodeId(id));
    }

    Ok(ExactPartition {
        origins: compact_origins,
        spans,
        isolated: isolated_nodes,
    })
}

fn classify_and_push(
    unique: &[Segment],
    left: usize,
    right: usize,
    events: &mut Vec<Event>,
    uf: &mut UnionFind,
    origins: &mut Vec<NodeOrigin>,
    start_handle: &[u32],
    end_handle: &[u32],
    budget: &mut ExpansionBudget,
) -> Result<()> {
    let a = unique[left];
    let b = unique[right];
    if same_point(a.start, a.end) || same_point(b.start, b.end) {
        return Ok(());
    }
    if segment_envelopes_disjoint(a, b) {
        return Ok(());
    }
    match classify_pair(a, b)? {
        PairContact::Disjoint | PairContact::SharedEndpoint => Ok(()),
        PairContact::Touch {
            on_left,
            at_start,
            param,
        } => {
            let handle = if on_left {
                if at_start {
                    start_handle[right]
                } else {
                    end_handle[right]
                }
            } else if at_start {
                start_handle[left]
            } else {
                end_handle[left]
            };
            let (t, u) = if on_left {
                (
                    param,
                    if at_start {
                        Param::zero()?
                    } else {
                        Param::one()?
                    },
                )
            } else {
                (
                    if at_start {
                        Param::zero()?
                    } else {
                        Param::one()?
                    },
                    param,
                )
            };
            push_event(events, Event {
                left: left as u32,
                right: right as u32,
                t,
                u,
                handle,
            })
        },
        PairContact::Overlap { cuts } => {
            for cut in cuts {
                let handle = intern_overlap_handle(
                    cut.at_left,
                    cut.at_start,
                    left,
                    right,
                    start_handle,
                    end_handle,
                );
                let (t, u) = overlap_params(cut.at_left, cut.at_start, cut.param)?;
                push_event(events, Event {
                    left: left as u32,
                    right: right as u32,
                    t,
                    u,
                    handle,
                })?;
            }
            Ok(())
        },
        PairContact::Cross { t, u } => {
            budget.add(1)?;
            try_reserve_vec(origins, 1)?;
            origins.push(NodeOrigin::Constructed);
            let handle = uf.push()?;
            push_event(events, Event {
                left: left as u32,
                right: right as u32,
                t,
                u,
                handle,
            })
        },
    }
}

fn intern_overlap_handle(
    at_left: bool,
    at_start: bool,
    left: usize,
    right: usize,
    start_handle: &[u32],
    end_handle: &[u32],
) -> u32 {
    if at_left {
        if at_start {
            start_handle[left]
        } else {
            end_handle[left]
        }
    } else if at_start {
        start_handle[right]
    } else {
        end_handle[right]
    }
}

fn overlap_params(at_left: bool, at_start: bool, param: Param) -> Result<(Param, Param)> {
    if at_left {
        Ok((
            if at_start {
                Param::zero()?
            } else {
                Param::one()?
            },
            param,
        ))
    } else {
        Ok((
            param,
            if at_start {
                Param::zero()?
            } else {
                Param::one()?
            },
        ))
    }
}

fn push_event(events: &mut Vec<Event>, event: Event) -> Result<()> {
    try_reserve_vec(events, 1)?;
    events.push(event);
    Ok(())
}

fn union_equal_params_per_source(
    unique: &[Segment],
    start_handle: &[u32],
    end_handle: &[u32],
    events: &[Event],
    uf: &mut UnionFind,
) -> Result<()> {
    for source in 0..unique.len() {
        let mut keys: Vec<(Param, u32)> = Vec::new();
        try_reserve_vec(&mut keys, 2 + events.len())?;
        keys.push((Param::zero()?, start_handle[source]));
        keys.push((Param::one()?, end_handle[source]));
        for event in events {
            if event.left as usize == source {
                keys.push((
                    Param {
                        num: event.t.num.try_clone()?,
                        den: event.t.den.try_clone()?,
                    },
                    event.handle,
                ));
            } else if event.right as usize == source {
                keys.push((
                    Param {
                        num: event.u.num.try_clone()?,
                        den: event.u.den.try_clone()?,
                    },
                    event.handle,
                ));
            }
        }
        insertion_sort_params(&mut keys)?;
        let mut index = 0;
        while index < keys.len() {
            let mut end = index + 1;
            while end < keys.len() && keys[end].0.cmp(&keys[index].0)? == Ordering::Equal {
                uf.union(keys[index].1, keys[end].1);
                end += 1;
            }
            index = end;
        }
    }
    Ok(())
}

fn insertion_sort_params(keys: &mut [(Param, u32)]) -> Result<()> {
    for i in 1..keys.len() {
        let mut j = i;
        while j > 0 && keys[j].0.cmp(&keys[j - 1].0)? == Ordering::Less {
            keys.swap(j, j - 1);
            j -= 1;
        }
    }
    Ok(())
}

enum PairContact {
    Disjoint,
    SharedEndpoint,
    Touch {
        on_left: bool,
        at_start: bool,
        param: Param,
    },
    Overlap {
        cuts: Vec<OverlapCut>,
    },
    Cross {
        t: Param,
        u: Param,
    },
}

struct OverlapCut {
    at_left: bool,
    at_start: bool,
    param: Param,
}

fn classify_pair(a: Segment, b: Segment) -> Result<PairContact> {
    let shares = same_point(a.start, b.start)
        || same_point(a.start, b.end)
        || same_point(a.end, b.start)
        || same_point(a.end, b.end);
    let o_b0 =
        stored_orientation_sign(a.start.x, a.start.y, a.end.x, a.end.y, b.start.x, b.start.y);
    let o_b1 = stored_orientation_sign(a.start.x, a.start.y, a.end.x, a.end.y, b.end.x, b.end.y);
    let o_a0 =
        stored_orientation_sign(b.start.x, b.start.y, b.end.x, b.end.y, a.start.x, a.start.y);
    let o_a1 = stored_orientation_sign(b.start.x, b.start.y, b.end.x, b.end.y, a.end.x, a.end.y);
    let collinear_b = o_b0 == ExactSign::Zero && o_b1 == ExactSign::Zero;
    if collinear_b {
        return overlap_or_touch(a, b, shares);
    }
    if o_b0 == ExactSign::Zero {
        let t = param_on_segment(a, b.start)?;
        if t.is_open_unit()? {
            return Ok(PairContact::Touch {
                on_left: true,
                at_start: true,
                param: t,
            });
        }
        return Ok(if shares {
            PairContact::SharedEndpoint
        } else {
            PairContact::Disjoint
        });
    }
    if o_b1 == ExactSign::Zero {
        let t = param_on_segment(a, b.end)?;
        if t.is_open_unit()? {
            return Ok(PairContact::Touch {
                on_left: true,
                at_start: false,
                param: t,
            });
        }
        return Ok(if shares {
            PairContact::SharedEndpoint
        } else {
            PairContact::Disjoint
        });
    }
    if o_a0 == ExactSign::Zero {
        let u = param_on_segment(b, a.start)?;
        if u.is_open_unit()? {
            return Ok(PairContact::Touch {
                on_left: false,
                at_start: true,
                param: u,
            });
        }
        return Ok(if shares {
            PairContact::SharedEndpoint
        } else {
            PairContact::Disjoint
        });
    }
    if o_a1 == ExactSign::Zero {
        let u = param_on_segment(b, a.end)?;
        if u.is_open_unit()? {
            return Ok(PairContact::Touch {
                on_left: false,
                at_start: false,
                param: u,
            });
        }
        return Ok(if shares {
            PairContact::SharedEndpoint
        } else {
            PairContact::Disjoint
        });
    }
    if shares {
        return Ok(PairContact::SharedEndpoint);
    }
    let straddle_b = matches!(
        (o_b0, o_b1),
        (ExactSign::Negative, ExactSign::Positive) | (ExactSign::Positive, ExactSign::Negative)
    );
    let straddle_a = matches!(
        (o_a0, o_a1),
        (ExactSign::Negative, ExactSign::Positive) | (ExactSign::Positive, ExactSign::Negative)
    );
    if straddle_a && straddle_b {
        let (t, u) = crossing_params(a, b)?;
        if t.is_open_unit()? && u.is_open_unit()? {
            return Ok(PairContact::Cross { t, u });
        }
    }
    Ok(PairContact::Disjoint)
}

fn overlap_or_touch(a: Segment, b: Segment, shares: bool) -> Result<PairContact> {
    let t0 = param_on_segment(a, b.start)?;
    let t1 = param_on_segment(a, b.end)?;
    let u0 = param_on_segment(b, a.start)?;
    let u1 = param_on_segment(b, a.end)?;
    let mut cuts = Vec::new();
    if t0.is_open_unit()? {
        try_reserve_vec(&mut cuts, 1)?;
        cuts.push(OverlapCut {
            at_left: false,
            at_start: true,
            param: t0,
        });
    }
    if t1.is_open_unit()? {
        try_reserve_vec(&mut cuts, 1)?;
        cuts.push(OverlapCut {
            at_left: false,
            at_start: false,
            param: t1,
        });
    }
    if u0.is_open_unit()? {
        try_reserve_vec(&mut cuts, 1)?;
        cuts.push(OverlapCut {
            at_left: true,
            at_start: true,
            param: u0,
        });
    }
    if u1.is_open_unit()? {
        try_reserve_vec(&mut cuts, 1)?;
        cuts.push(OverlapCut {
            at_left: true,
            at_start: false,
            param: u1,
        });
    }
    if cuts.is_empty() {
        return Ok(if shares {
            PairContact::SharedEndpoint
        } else {
            PairContact::Disjoint
        });
    }
    Ok(PairContact::Overlap { cuts })
}

fn param_on_segment(segment: Segment, point: XY) -> Result<Param> {
    let dx = ExactDyadic::try_from_f64(segment.end.x)?
        .try_subtract(ExactDyadic::try_from_f64(segment.start.x)?)?;
    let dy = ExactDyadic::try_from_f64(segment.end.y)?
        .try_subtract(ExactDyadic::try_from_f64(segment.start.y)?)?;
    if dx.cmp_magnitude(&dy) != Ordering::Less {
        let num = ExactDyadic::try_from_f64(point.x)?
            .try_subtract(ExactDyadic::try_from_f64(segment.start.x)?)?;
        Ok(Param { num, den: dx })
    } else {
        let num = ExactDyadic::try_from_f64(point.y)?
            .try_subtract(ExactDyadic::try_from_f64(segment.start.y)?)?;
        Ok(Param { num, den: dy })
    }
}

fn crossing_params(a: Segment, b: Segment) -> Result<(Param, Param)> {
    let rx =
        ExactDyadic::try_from_f64(a.end.x)?.try_subtract(ExactDyadic::try_from_f64(a.start.x)?)?;
    let ry =
        ExactDyadic::try_from_f64(a.end.y)?.try_subtract(ExactDyadic::try_from_f64(a.start.y)?)?;
    let sx =
        ExactDyadic::try_from_f64(b.end.x)?.try_subtract(ExactDyadic::try_from_f64(b.start.x)?)?;
    let sy =
        ExactDyadic::try_from_f64(b.end.y)?.try_subtract(ExactDyadic::try_from_f64(b.start.y)?)?;
    let cx = ExactDyadic::try_from_f64(b.start.x)?
        .try_subtract(ExactDyadic::try_from_f64(a.start.x)?)?;
    let cy = ExactDyadic::try_from_f64(b.start.y)?
        .try_subtract(ExactDyadic::try_from_f64(a.start.y)?)?;
    let den = rx.try_product(&sy)?.try_subtract(ry.try_product(&sx)?)?;
    let t_num = cx.try_product(&sy)?.try_subtract(cy.try_product(&sx)?)?;
    let u_num = cx.try_product(&ry)?.try_subtract(cy.try_product(&rx)?)?;
    Ok((
        Param {
            num: t_num,
            den: den.try_clone()?,
        },
        Param { num: u_num, den },
    ))
}

#[cfg(test)]
mod tests {
    use super::super::noding::dedup_undirected_segments_sourced;
    use super::*;
    use crate::geometry::XY;

    fn seg(x0: f64, y0: f64, x1: f64, y1: f64) -> Segment {
        Segment {
            start: XY::new(x0, y0),
            end: XY::new(x1, y1),
        }
    }

    fn run(segments: &[Segment]) -> ExactPartition {
        run_with(segments, true)
    }

    fn run_with(segments: &[Segment], union_equal: bool) -> ExactPartition {
        let mut edges = Vec::new();
        let mut isolated = Vec::new();
        for &segment in segments {
            if same_point(segment.start, segment.end) {
                isolated.push(segment.start);
            } else {
                edges.push(segment);
            }
        }
        let (unique, provenance) = dedup_undirected_segments_sourced(edges);
        let mut budget = ExpansionBudget::new("exact_partition", "items");
        partition_inner(&unique, &provenance, &isolated, &mut budget, union_equal).unwrap()
    }

    #[test]
    fn public_entry_matches_inner_on_empty() {
        let mut budget = ExpansionBudget::new("exact_partition", "items");
        let (unique, provenance) = dedup_undirected_segments_sourced(Vec::<Segment>::new());
        let partition = partition_unique_segments(&unique, &provenance, &[], &mut budget).unwrap();
        assert_eq!(partition.node_count(), 0);
    }

    #[test]
    fn generated_t_has_seven_nodes_five_spans_two_components() {
        let rn_sixth = 1.0_f64 / 6.0;
        let partition = run(&[
            seg(0.0, 0.0, 3.0, 1.0),
            seg(1.0, -1.0, 1.0, 1.0),
            seg(0.5, 0.0, 0.5, rn_sixth),
        ]);
        assert_eq!(partition.node_count(), 7);
        assert_eq!(partition.spans.len(), 5);
        assert_eq!(partition.constructed_count(), 1);
        assert_eq!(partition.component_count(), 2);
        let c_end = partition.stored_node(XY::new(0.5, rn_sixth)).unwrap();
        let on_a = partition
            .spans
            .iter()
            .any(|span| span.unique == 0 && (span.start == c_end || span.end == c_end));
        assert!(!on_a, "C's endpoint must not node onto A");
    }

    #[test]
    fn endpoint_near_miss_five_nodes_four_spans() {
        let nd = 1.0_f64.next_down();
        let partition = run(&[seg(0.0, 0.0, 1.0, 1.0), seg(1.0, nd, 0.0, 2.0)]);
        assert_eq!(partition.node_count(), 5);
        assert_eq!(partition.spans.len(), 4);
        assert_eq!(partition.constructed_count(), 1);
        let q_num = ExactDyadic::try_from_f64(1.0).unwrap();
        let two = ExactDyadic::try_from_f64(2.0).unwrap();
        // q = 2^54 / (2^54 + 1); compare crossing param of A against that ratio.
        let p54 = {
            let mut p = ExactDyadic::try_from_f64(1.0).unwrap();
            for _ in 0..54 {
                p = p.try_product(&two).unwrap();
            }
            p
        };
        let den = p54
            .try_clone()
            .unwrap()
            .try_add(q_num.try_clone().unwrap())
            .unwrap();
        let a = Segment {
            start: XY::new(0.0, 0.0),
            end: XY::new(1.0, 1.0),
        };
        let b = Segment {
            start: XY::new(1.0, nd),
            end: XY::new(0.0, 2.0),
        };
        let (t, _) = crossing_params(a, b).unwrap();
        assert_eq!(
            try_compare_ratios(&t.num, &t.den, &p54, &den).unwrap(),
            Ordering::Equal
        );
    }

    #[test]
    fn concurrent_three_sources_one_constructed_node() {
        let partition = run(&[
            seg(0.0, 0.0, 3.0, 1.0),
            seg(1.0, -1.0, 1.0, 1.0),
            seg(0.0, 1.0, 3.0, -1.0),
        ]);
        assert_eq!(partition.constructed_count(), 1);
        let a = seg(0.0, 0.0, 3.0, 1.0);
        let b = seg(1.0, -1.0, 1.0, 1.0);
        let (t, _) = crossing_params(a, b).unwrap();
        let one = ExactDyadic::try_from_f64(1.0).unwrap();
        let three = ExactDyadic::try_from_f64(3.0).unwrap();
        // t along A to x=1 is 1/3.
        assert_eq!(
            try_compare_ratios(&t.num, &t.den, &one, &three).unwrap(),
            Ordering::Equal
        );
    }

    #[test]
    fn omit_equal_param_union_splits_concurrent_vertex() {
        let honest = run(&[
            seg(0.0, 0.0, 3.0, 1.0),
            seg(1.0, -1.0, 1.0, 1.0),
            seg(0.0, 1.0, 3.0, -1.0),
        ]);
        let mutant = run_with(
            &[
                seg(0.0, 0.0, 3.0, 1.0),
                seg(1.0, -1.0, 1.0, 1.0),
                seg(0.0, 1.0, 3.0, -1.0),
            ],
            false,
        );
        assert_eq!(honest.constructed_count(), 1);
        assert!(
            mutant.constructed_count() > 1 || mutant.node_count() > honest.node_count(),
            "missing equal-param union must split the concurrent vertex"
        );
    }

    #[test]
    fn overlap_reversed_duplicate_keeps_reverse_owner() {
        let partition = run(&[
            seg(0.0, 0.0, 4.0, 0.0),
            seg(1.0, 0.0, 3.0, 0.0),
            seg(2.0, -1.0, 2.0, 1.0),
            seg(4.0, 0.0, 0.0, 0.0),
        ]);
        let reversed = partition
            .spans
            .iter()
            .flat_map(|span| span.owners.iter())
            .any(|&(ordinal, reversed)| ordinal == 3 && reversed);
        assert!(reversed, "reversed duplicate must keep the reverse flag");
        let ends = |span: &ExactSpan| {
            let a = span.start.0.min(span.end.0);
            let b = span.start.0.max(span.end.0);
            (a, b)
        };
        let a_ends: Vec<_> = partition
            .spans
            .iter()
            .filter(|span| span.owners.iter().any(|(ordinal, _)| *ordinal == 0))
            .map(ends)
            .collect();
        let b_ends: Vec<_> = partition
            .spans
            .iter()
            .filter(|span| span.owners.iter().any(|(ordinal, _)| *ordinal == 1))
            .map(ends)
            .collect();
        assert!(
            a_ends.iter().any(|pair| b_ends.contains(pair)),
            "overlap of A and B must share a noded interval"
        );
    }

    #[test]
    fn degenerate_point_is_isolated_node() {
        let partition = run(&[seg(0.0, 0.0, 2.0, 0.0), seg(1.0, 1.0, 1.0, 1.0)]);
        assert_eq!(partition.isolated.len(), 1);
        assert_eq!(
            partition.stored_node(XY::new(1.0, 1.0)),
            Some(partition.isolated[0])
        );
    }

    #[test]
    fn signed_zero_endpoints_share_a_stored_node() {
        let partition = run(&[seg(-0.0, 0.0, 1.0, 0.0), seg(0.0, 0.0, 0.0, 1.0)]);
        let a = partition.stored_node(XY::new(-0.0, 0.0)).unwrap();
        let b = partition.stored_node(XY::new(0.0, 0.0)).unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn full_finite_exponent_pair_constructs_or_misses_exactly() {
        let min_sub = f64::from_bits(1);
        let partition = run(&[
            seg(0.0, 0.0, f64::MAX, min_sub),
            seg(min_sub, -f64::MAX, f64::MAX, f64::MAX),
        ]);
        assert!(partition.node_count() >= 4);
        assert!(partition.spans.len() >= 2);
    }

    #[test]
    fn try_add_matches_infallible_add_on_minsub() {
        let a = ExactDyadic::try_from_f64(1.0).unwrap();
        let b = ExactDyadic::try_from_f64(f64::from_bits(1)).unwrap();
        let fallible = a
            .try_clone()
            .unwrap()
            .try_subtract(b.try_clone().unwrap())
            .unwrap();
        let infail = ExactDyadic::from_f64(1.0).subtract(ExactDyadic::from_f64(f64::from_bits(1)));
        assert_eq!(fallible, infail);
    }
}
