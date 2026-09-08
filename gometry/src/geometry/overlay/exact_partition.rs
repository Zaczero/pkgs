//! Exact source partition of unique stored segments.
//!
//! Borrows unique undirected representatives and existing CSR provenance.
//! Constructed nodes keep one source-parameter recipe after equal-param union;
//! stored nodes keep `PointKey`. Arrangement, overlay, and checked embedding
//! are later cuts.
//!
//! Candidates come from the shared fallible sweep over unique segments plus
//! extra isolated points as zero-length items. Contact-derived cuts, compact
//! nodes, and canonical atoms are generated items. Sweep scratch is
//! [`ResourceError`], not the item cap.

#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "exact source partition is the future incidence owner; overlay has not connected it yet"
    )
)]

use std::cmp::Ordering;
use std::mem;
use std::ops::ControlFlow;

use crate::error::ResourceError;
use crate::geometry::exact_arith::ExactSign;
use crate::geometry::overlay::noding::{SegmentProvenance, SegmentSource};
use crate::geometry::segments::{
    point_in_segment_envelope, segment_envelopes_disjoint, stored_orientation_sign,
};
use crate::geometry::tessellation::exact::{ExactDyadic, try_compare_ratios};
use crate::geometry::{
    ExpansionBudget, GeometryErrorKind, HashMap, PointKey, Result, Segment, XY, same_point,
    single_chain, try_for_each_candidate_pair,
};
use crate::try_reserve_vec;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub(in crate::geometry) struct NodeId(pub u32);

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::geometry) enum NodeRecipe {
    Stored(PointKey),
    Constructed {
        unique: u32,
        num: ExactDyadic,
        den: ExactDyadic,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::geometry) struct ExactSpan {
    pub start: NodeId,
    pub end: NodeId,
    pub unique: u32,
}

#[derive(Debug)]
pub(in crate::geometry) struct ExactPartition<'a> {
    unique: &'a [Segment],
    pub recipes: Vec<NodeRecipe>,
    pub spans: Vec<ExactSpan>,
    pub provenance: SegmentProvenance,
    pub isolated: Vec<NodeId>,
}

impl<'a> ExactPartition<'a> {
    pub(in crate::geometry) const fn unique(&self) -> &'a [Segment] {
        self.unique
    }

    pub(in crate::geometry) fn recipe(&self, node: NodeId) -> &NodeRecipe {
        &self.recipes[node.0 as usize]
    }

    pub(in crate::geometry) fn owners(&self, span: usize) -> &[SegmentSource] {
        self.provenance.owners(span)
    }
}

enum SourceParam {
    Start,
    End,
    Ratio { num: ExactDyadic, den: ExactDyadic },
}

impl SourceParam {
    fn cmp(&self, other: &Self, zero: &ExactDyadic, one: &ExactDyadic) -> Result<Ordering> {
        match (self, other) {
            (Self::Start, Self::Start) | (Self::End, Self::End) => Ok(Ordering::Equal),
            (Self::Start, Self::End) => Ok(Ordering::Less),
            (Self::End, Self::Start) => Ok(Ordering::Greater),
            (Self::Start, Self::Ratio { num, den }) => try_compare_ratios(zero, one, num, den),
            (Self::End, Self::Ratio { num, den }) => try_compare_ratios(one, one, num, den),
            (Self::Ratio { num, den }, Self::Start) => try_compare_ratios(num, den, zero, one),
            (Self::Ratio { num, den }, Self::End) => try_compare_ratios(num, den, one, one),
            (
                Self::Ratio {
                    num: left_n,
                    den: left_d,
                },
                Self::Ratio {
                    num: right_n,
                    den: right_d,
                },
            ) => try_compare_ratios(left_n, left_d, right_n, right_d),
        }
    }

    fn is_open_unit(&self, zero: &ExactDyadic, one: &ExactDyadic) -> Result<bool> {
        match self {
            Self::Start | Self::End => Ok(false),
            Self::Ratio { num, den } => Ok(try_compare_ratios(num, den, zero, one)?
                == Ordering::Greater
                && try_compare_ratios(num, den, one, one)? == Ordering::Less),
        }
    }
}

struct SourceCut {
    param: SourceParam,
    handle: u32,
}

#[derive(Clone, Copy)]
enum HandleKind {
    Stored(PointKey),
    Constructed { unique: u32, cut: u32 },
}

struct Forest {
    parent: Vec<u32>,
    kind: Vec<HandleKind>,
}

impl Forest {
    fn with_capacity(capacity: usize) -> Result<Self> {
        let mut parent = Vec::new();
        let mut kind = Vec::new();
        try_reserve_vec(&mut parent, capacity)?;
        try_reserve_vec(&mut kind, capacity)?;
        Ok(Self { parent, kind })
    }

    fn find(&mut self, mut node: u32) -> u32 {
        while self.parent[node as usize] != node {
            self.parent[node as usize] = self.parent[self.parent[node as usize] as usize];
            node = self.parent[node as usize];
        }
        node
    }

    fn push(&mut self, kind: HandleKind) -> Result<u32> {
        let id = u32::try_from(self.parent.len()).map_err(|_| ResourceError::SizeOverflow)?;
        try_reserve_vec(&mut self.parent, 1)?;
        try_reserve_vec(&mut self.kind, 1)?;
        self.parent.push(id);
        self.kind.push(kind);
        Ok(id)
    }

    fn union(&mut self, a: u32, b: u32) -> Result<()> {
        let a = self.find(a);
        let b = self.find(b);
        if a == b {
            return Ok(());
        }
        match (&self.kind[a as usize], &self.kind[b as usize]) {
            (HandleKind::Stored(left), HandleKind::Stored(right)) if left != right => Err(
                GeometryErrorKind::Invalid("exact partition unioned distinct stored points".into())
                    .into(),
            ),
            (HandleKind::Stored(_), HandleKind::Stored(_) | HandleKind::Constructed { .. }) => {
                self.parent[b as usize] = a;
                Ok(())
            },
            (HandleKind::Constructed { .. }, HandleKind::Stored(_)) => {
                self.parent[a as usize] = b;
                Ok(())
            },
            (
                HandleKind::Constructed { unique: left, .. },
                HandleKind::Constructed { unique: right, .. },
            ) => {
                if right < left {
                    self.parent[a as usize] = b;
                } else {
                    self.parent[b as usize] = a;
                }
                Ok(())
            },
        }
    }
}

pub(in crate::geometry) fn partition_unique_segments<'a>(
    unique: &'a [Segment],
    provenance: &SegmentProvenance,
    isolated: &[XY],
    budget: &mut ExpansionBudget,
) -> Result<ExactPartition<'a>> {
    debug_assert_eq!(provenance.len(), unique.len());
    let zero = ExactDyadic::try_from_f64(0.0)?;
    let one = ExactDyadic::try_from_f64(1.0)?;

    let mut endpoint_of: HashMap<PointKey, u32> =
        HashMap::with_hasher(crate::collections::topology_hash_state()?.clone());
    endpoint_of
        .try_reserve(
            unique
                .len()
                .saturating_mul(2)
                .saturating_add(isolated.len()),
        )
        .map_err(crate::error::Error::from)?;
    let mut forest = Forest::with_capacity(
        unique
            .len()
            .saturating_mul(2)
            .saturating_add(isolated.len()),
    )?;

    let intern_stored =
        |point: XY, endpoint_of: &mut HashMap<PointKey, u32>, forest: &mut Forest| -> Result<u32> {
            let key = PointKey::new(point);
            if let Some(&id) = endpoint_of.get(&key) {
                return Ok(id);
            }
            let id = forest.push(HandleKind::Stored(key))?;
            endpoint_of.insert(key, id);
            Ok(id)
        };

    let mut start_handle = Vec::new();
    let mut end_handle = Vec::new();
    try_reserve_vec(&mut start_handle, unique.len())?;
    try_reserve_vec(&mut end_handle, unique.len())?;
    for &segment in unique {
        start_handle.push(intern_stored(segment.start, &mut endpoint_of, &mut forest)?);
        end_handle.push(intern_stored(segment.end, &mut endpoint_of, &mut forest)?);
    }
    let mut cuts: Vec<Vec<SourceCut>> = Vec::new();
    try_reserve_vec(&mut cuts, unique.len())?;
    for _ in 0..unique.len() {
        cuts.push(Vec::new());
    }

    let mut pool = Vec::new();
    try_reserve_vec(&mut pool, unique.len().saturating_add(isolated.len()))?;
    pool.extend_from_slice(unique);
    let mut extra_handles = Vec::new();
    try_reserve_vec(&mut extra_handles, isolated.len())?;
    for &point in isolated {
        let key = PointKey::new(point);
        let existed = endpoint_of.contains_key(&key);
        intern_stored(point, &mut endpoint_of, &mut forest)?;
        if existed {
            continue;
        }
        extra_handles.push(endpoint_of[&key]);
        pool.push(Segment {
            start: point,
            end: point,
        });
    }
    let unique_len = unique.len();
    let mut pair_error = None;
    match try_for_each_candidate_pair(&pool, single_chain, |left, right| {
        match visit_pool_pair(
            unique,
            unique_len,
            left,
            right,
            &start_handle,
            &end_handle,
            &extra_handles,
            &pool,
            &mut cuts,
            &mut forest,
            &zero,
            &one,
        ) {
            Ok(()) => ControlFlow::Continue(()),
            Err(error) => {
                pair_error = Some(error);
                ControlFlow::Break(())
            },
        }
    })? {
        ControlFlow::Break(()) => {
            if let Some(error) = pair_error {
                return Err(error);
            }
        },
        ControlFlow::Continue(()) => {},
    }

    let mut order: Vec<Vec<u32>> = Vec::new();
    try_reserve_vec(&mut order, unique.len())?;
    for source in 0..unique.len() {
        if !same_point(unique[source].start, unique[source].end) {
            push_contact_cut(&mut cuts[source], SourceParam::Start, start_handle[source])?;
            push_contact_cut(&mut cuts[source], SourceParam::End, end_handle[source])?;
        }
        order.push(try_sort_indices(&cuts[source], &zero, &one)?);
        union_equal_params(&cuts[source], &order[source], &mut forest, &zero, &one)?;
    }

    let mut compact_of: Vec<Option<u32>> = Vec::new();
    try_reserve_vec(&mut compact_of, forest.parent.len())?;
    compact_of.resize(forest.parent.len(), None);
    let mut recipes: Vec<NodeRecipe> = Vec::new();
    for handle in 0..forest.parent.len() {
        let root = forest.find(handle as u32);
        if compact_of[root as usize].is_some() {
            continue;
        }
        let id = u32::try_from(recipes.len()).map_err(|_| ResourceError::SizeOverflow)?;
        match forest.kind[root as usize] {
            HandleKind::Stored(key) => {
                try_reserve_vec(&mut recipes, 1)?;
                recipes.push(NodeRecipe::Stored(key));
            },
            HandleKind::Constructed { unique, cut } => {
                budget.add(1)?;
                let param = mem::replace(
                    &mut cuts[unique as usize][cut as usize].param,
                    SourceParam::Start,
                );
                let SourceParam::Ratio { num, den } = param else {
                    return Err(GeometryErrorKind::Invalid(
                        "constructed partition node has no source-parameter recipe".into(),
                    )
                    .into());
                };
                try_reserve_vec(&mut recipes, 1)?;
                recipes.push(NodeRecipe::Constructed { unique, num, den });
            },
        }
        compact_of[root as usize] = Some(id);
    }
    for handle in 0..forest.parent.len() {
        let root = forest.find(handle as u32);
        compact_of[handle] = compact_of[root as usize];
    }

    let mut intervals: Vec<(u32, u32, u32)> = Vec::new();
    for source in 0..unique.len() {
        if same_point(unique[source].start, unique[source].end) {
            continue;
        }
        let mut prev: Option<u32> = None;
        for &index in &order[source] {
            let handle = forest.find(cuts[source][index as usize].handle);
            let id = compact_of[handle as usize].ok_or_else(|| {
                GeometryErrorKind::Invalid("partition handle was not compacted".into())
            })?;
            if prev == Some(id) {
                continue;
            }
            if let Some(start) = prev
                && start != id
            {
                try_reserve_vec(&mut intervals, 1)?;
                intervals.push((source as u32, start, id));
            }
            prev = Some(id);
        }
    }

    // The budget counts operation-wide net growth: overlap coalescing can
    // offset span growth elsewhere, while stored nodes and source atoms are free.
    let baseline = unique
        .iter()
        .filter(|segment| !same_point(segment.start, segment.end))
        .count();
    let (spans, atom_provenance) = aggregate_atoms(intervals, provenance, baseline, budget)?;

    let mut degree: Vec<u32> = Vec::new();
    try_reserve_vec(&mut degree, recipes.len())?;
    degree.resize(recipes.len(), 0);
    for span in &spans {
        degree[span.start.0 as usize] = degree[span.start.0 as usize].saturating_add(1);
        degree[span.end.0 as usize] = degree[span.end.0 as usize].saturating_add(1);
    }
    let mut isolated_nodes = Vec::new();
    for (index, &count) in degree.iter().enumerate() {
        if count == 0 {
            try_reserve_vec(&mut isolated_nodes, 1)?;
            isolated_nodes.push(NodeId(index as u32));
        }
    }

    Ok(ExactPartition {
        unique,
        recipes,
        spans,
        provenance: atom_provenance,
        isolated: isolated_nodes,
    })
}

fn push_contact_cut(cuts: &mut Vec<SourceCut>, param: SourceParam, handle: u32) -> Result<()> {
    try_reserve_vec(cuts, 1)?;
    cuts.push(SourceCut { param, handle });
    Ok(())
}

fn push_point_on_source(
    segment: Segment,
    point: XY,
    handle: u32,
    cuts: &mut Vec<SourceCut>,
    zero: &ExactDyadic,
    one: &ExactDyadic,
) -> Result<()> {
    if same_point(segment.start, segment.end) {
        return Ok(());
    }
    if !point_in_segment_envelope(point, segment) {
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
    if side != ExactSign::Zero {
        return Ok(());
    }
    let param = param_on_segment(segment, point)?;
    if param.is_open_unit(zero, one)? {
        push_contact_cut(cuts, param, handle)?;
    }
    Ok(())
}

const fn pair_index(which: u32, left: usize, right: usize) -> usize {
    if which == 0 { left } else { right }
}

const fn endpoint_handle(
    source: usize,
    at_start: bool,
    start_handle: &[u32],
    end_handle: &[u32],
) -> u32 {
    if at_start {
        start_handle[source]
    } else {
        end_handle[source]
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "pair visit needs the unique pool, endpoint handles, and cut sinks together"
)]
fn visit_pool_pair(
    unique: &[Segment],
    unique_len: usize,
    left: usize,
    right: usize,
    start_handle: &[u32],
    end_handle: &[u32],
    extra_handles: &[u32],
    pool: &[Segment],
    cuts: &mut [Vec<SourceCut>],
    forest: &mut Forest,
    zero: &ExactDyadic,
    one: &ExactDyadic,
) -> Result<()> {
    match (left < unique_len, right < unique_len) {
        (true, true) => classify_pair_into(
            unique,
            left,
            right,
            start_handle,
            end_handle,
            cuts,
            forest,
            zero,
            one,
        ),
        (true, false) => {
            let handle = extra_handles[right - unique_len];
            push_point_on_source(
                unique[left],
                pool[right].start,
                handle,
                &mut cuts[left],
                zero,
                one,
            )
        },
        (false, true) => {
            let handle = extra_handles[left - unique_len];
            push_point_on_source(
                unique[right],
                pool[left].start,
                handle,
                &mut cuts[right],
                zero,
                one,
            )
        },
        (false, false) => Ok(()),
    }
}

fn classify_pair_into(
    unique: &[Segment],
    left: usize,
    right: usize,
    start_handle: &[u32],
    end_handle: &[u32],
    cuts: &mut [Vec<SourceCut>],
    forest: &mut Forest,
    zero: &ExactDyadic,
    one: &ExactDyadic,
) -> Result<()> {
    let a = unique[left];
    let b = unique[right];
    if same_point(a.start, a.end) {
        return push_point_on_source(b, a.start, start_handle[left], &mut cuts[right], zero, one);
    }
    if same_point(b.start, b.end) {
        return push_point_on_source(a, b.start, start_handle[right], &mut cuts[left], zero, one);
    }
    if segment_envelopes_disjoint(a, b) {
        return Ok(());
    }
    match classify_pair(a, b, zero, one)? {
        PairContact::None => Ok(()),
        PairContact::Touch {
            source,
            handle_source,
            at_start,
            param,
        } => {
            let source = pair_index(source, left, right);
            let handle_source = pair_index(handle_source, left, right);
            let handle = endpoint_handle(handle_source, at_start, start_handle, end_handle);
            push_contact_cut(&mut cuts[source], param, handle)
        },
        PairContact::Overlap { cuts: overlap } => {
            for cut in overlap {
                let source = pair_index(cut.source, left, right);
                let handle_source = pair_index(cut.handle_source, left, right);
                let handle = endpoint_handle(handle_source, cut.at_start, start_handle, end_handle);
                push_contact_cut(&mut cuts[source], cut.param, handle)?;
            }
            Ok(())
        },
        PairContact::Cross { t, u } => {
            let unique_index = left.min(right);
            let unique = u32::try_from(unique_index).map_err(|_| ResourceError::SizeOverflow)?;
            let (first_param, second, second_param) = if unique_index == left {
                (t, right, u)
            } else {
                (u, left, t)
            };
            let cut =
                u32::try_from(cuts[unique_index].len()).map_err(|_| ResourceError::SizeOverflow)?;
            let handle = forest.push(HandleKind::Constructed { unique, cut })?;
            push_contact_cut(&mut cuts[unique_index], first_param, handle)?;
            push_contact_cut(&mut cuts[second], second_param, handle)
        },
    }
}

fn union_equal_params(
    cuts: &[SourceCut],
    order: &[u32],
    forest: &mut Forest,
    zero: &ExactDyadic,
    one: &ExactDyadic,
) -> Result<()> {
    let mut index = 0;
    while index < order.len() {
        let mut end = index + 1;
        while end < order.len()
            && cuts[order[end] as usize]
                .param
                .cmp(&cuts[order[index] as usize].param, zero, one)?
                == Ordering::Equal
        {
            forest.union(
                cuts[order[index] as usize].handle,
                cuts[order[end] as usize].handle,
            )?;
            end += 1;
        }
        index = end;
    }
    Ok(())
}

fn aggregate_atoms(
    intervals: Vec<(u32, u32, u32)>,
    source_provenance: &SegmentProvenance,
    baseline: usize,
    budget: &mut ExpansionBudget,
) -> Result<(Vec<ExactSpan>, SegmentProvenance)> {
    let mut atom_of: HashMap<(u32, u32), u32> =
        HashMap::with_hasher(crate::collections::topology_hash_state()?.clone());
    atom_of
        .try_reserve(intervals.len())
        .map_err(crate::error::Error::from)?;
    let mut spans: Vec<ExactSpan> = Vec::new();
    let mut contributors: Vec<(u32, u32, bool)> = Vec::new();
    try_reserve_vec(&mut contributors, intervals.len())?;
    for (unique, start, end) in intervals {
        let lo = start.min(end);
        let hi = start.max(end);
        if let Some(&atom) = atom_of.get(&(lo, hi)) {
            let reversed = start != spans[atom as usize].start.0;
            contributors.push((atom, unique, reversed));
            continue;
        }
        if spans.len() >= baseline {
            budget.add(1)?;
        }
        let atom = u32::try_from(spans.len()).map_err(|_| ResourceError::SizeOverflow)?;
        try_reserve_vec(&mut spans, 1)?;
        spans.push(ExactSpan {
            start: NodeId(start),
            end: NodeId(end),
            unique,
        });
        atom_of.insert((lo, hi), atom);
        contributors.push((atom, unique, false));
    }

    let atom_count = spans.len();
    let mut counts: Vec<usize> = Vec::new();
    try_reserve_vec(&mut counts, atom_count)?;
    counts.resize(atom_count, 0);
    for &(atom, unique, _) in &contributors {
        counts[atom as usize] = counts[atom as usize]
            .checked_add(source_provenance.owners(unique as usize).len())
            .ok_or(ResourceError::SizeOverflow)?;
    }
    let mut offsets: Vec<u32> = Vec::new();
    try_reserve_vec(
        &mut offsets,
        atom_count
            .checked_add(1)
            .ok_or(ResourceError::SizeOverflow)?,
    )?;
    offsets.push(0);
    for &count in &counts {
        let next = (offsets[offsets.len() - 1] as usize)
            .checked_add(count)
            .ok_or(ResourceError::SizeOverflow)?;
        let next = u32::try_from(next).map_err(|_| ResourceError::SizeOverflow)?;
        offsets.push(next);
    }
    let mut entries: Vec<SegmentSource> = Vec::new();
    try_reserve_vec(&mut entries, offsets[atom_count] as usize)?;
    entries.resize(offsets[atom_count] as usize, (0, false));
    let mut cursor: Vec<u32> = Vec::new();
    try_reserve_vec(&mut cursor, atom_count)?;
    cursor.extend_from_slice(&offsets[..atom_count]);
    for &(atom, unique, reversed_geom) in &contributors {
        for &(ordinal, reversed) in source_provenance.owners(unique as usize) {
            let slot = cursor[atom as usize] as usize;
            entries[slot] = (ordinal, reversed != reversed_geom);
            cursor[atom as usize] += 1;
        }
    }
    Ok((spans, SegmentProvenance::from_csr(offsets, entries)))
}

fn try_sort_indices(cuts: &[SourceCut], zero: &ExactDyadic, one: &ExactDyadic) -> Result<Vec<u32>> {
    let n = cuts.len();
    let mut src = Vec::new();
    try_reserve_vec(&mut src, n)?;
    src.extend(0..n as u32);
    if n < 2 {
        return Ok(src);
    }
    let mut dst = Vec::new();
    try_reserve_vec(&mut dst, n)?;
    dst.resize(n, 0);
    let mut width = 1;
    while width < n {
        let mut start = 0;
        while start < n {
            let mid = start.saturating_add(width).min(n);
            let end = mid.saturating_add(width).min(n);
            merge_indices(&src, &mut dst, start, mid, end, cuts, zero, one)?;
            start = end;
        }
        mem::swap(&mut src, &mut dst);
        width = width.saturating_mul(2);
    }
    Ok(src)
}

fn merge_indices(
    src: &[u32],
    dst: &mut [u32],
    start: usize,
    mid: usize,
    end: usize,
    cuts: &[SourceCut],
    zero: &ExactDyadic,
    one: &ExactDyadic,
) -> Result<()> {
    let mut left = start;
    let mut right = mid;
    let mut out = start;
    while left < mid && right < end {
        let order =
            cuts[src[left] as usize]
                .param
                .cmp(&cuts[src[right] as usize].param, zero, one)?;
        if order.is_le() {
            dst[out] = src[left];
            left += 1;
        } else {
            dst[out] = src[right];
            right += 1;
        }
        out += 1;
    }
    while left < mid {
        dst[out] = src[left];
        left += 1;
        out += 1;
    }
    while right < end {
        dst[out] = src[right];
        right += 1;
        out += 1;
    }
    Ok(())
}

enum PairContact {
    None,
    Touch {
        source: u32,
        handle_source: u32,
        at_start: bool,
        param: SourceParam,
    },
    Overlap {
        cuts: Vec<OverlapCut>,
    },
    Cross {
        t: SourceParam,
        u: SourceParam,
    },
}

struct OverlapCut {
    source: u32,
    handle_source: u32,
    at_start: bool,
    param: SourceParam,
}

fn classify_pair(
    a: Segment,
    b: Segment,
    zero: &ExactDyadic,
    one: &ExactDyadic,
) -> Result<PairContact> {
    let o_b0 =
        stored_orientation_sign(a.start.x, a.start.y, a.end.x, a.end.y, b.start.x, b.start.y);
    let o_b1 = stored_orientation_sign(a.start.x, a.start.y, a.end.x, a.end.y, b.end.x, b.end.y);
    let o_a0 =
        stored_orientation_sign(b.start.x, b.start.y, b.end.x, b.end.y, a.start.x, a.start.y);
    let o_a1 = stored_orientation_sign(b.start.x, b.start.y, b.end.x, b.end.y, a.end.x, a.end.y);
    if o_b0 == ExactSign::Zero && o_b1 == ExactSign::Zero {
        return overlap_or_touch(a, b, zero, one);
    }
    if o_b0 == ExactSign::Zero {
        let param = param_on_segment(a, b.start)?;
        if param.is_open_unit(zero, one)? {
            return Ok(PairContact::Touch {
                source: 0,
                handle_source: 1,
                at_start: true,
                param,
            });
        }
        return Ok(PairContact::None);
    }
    if o_b1 == ExactSign::Zero {
        let param = param_on_segment(a, b.end)?;
        if param.is_open_unit(zero, one)? {
            return Ok(PairContact::Touch {
                source: 0,
                handle_source: 1,
                at_start: false,
                param,
            });
        }
        return Ok(PairContact::None);
    }
    if o_a0 == ExactSign::Zero {
        let param = param_on_segment(b, a.start)?;
        if param.is_open_unit(zero, one)? {
            return Ok(PairContact::Touch {
                source: 1,
                handle_source: 0,
                at_start: true,
                param,
            });
        }
        return Ok(PairContact::None);
    }
    if o_a1 == ExactSign::Zero {
        let param = param_on_segment(b, a.end)?;
        if param.is_open_unit(zero, one)? {
            return Ok(PairContact::Touch {
                source: 1,
                handle_source: 0,
                at_start: false,
                param,
            });
        }
        return Ok(PairContact::None);
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
        if t.is_open_unit(zero, one)? && u.is_open_unit(zero, one)? {
            return Ok(PairContact::Cross { t, u });
        }
    }
    Ok(PairContact::None)
}

fn overlap_or_touch(
    a: Segment,
    b: Segment,
    zero: &ExactDyadic,
    one: &ExactDyadic,
) -> Result<PairContact> {
    let t0 = param_on_segment(a, b.start)?;
    let t1 = param_on_segment(a, b.end)?;
    let u0 = param_on_segment(b, a.start)?;
    let u1 = param_on_segment(b, a.end)?;
    let mut cuts = Vec::new();
    if t0.is_open_unit(zero, one)? {
        try_reserve_vec(&mut cuts, 1)?;
        cuts.push(OverlapCut {
            source: 0,
            handle_source: 1,
            at_start: true,
            param: t0,
        });
    }
    if t1.is_open_unit(zero, one)? {
        try_reserve_vec(&mut cuts, 1)?;
        cuts.push(OverlapCut {
            source: 0,
            handle_source: 1,
            at_start: false,
            param: t1,
        });
    }
    if u0.is_open_unit(zero, one)? {
        try_reserve_vec(&mut cuts, 1)?;
        cuts.push(OverlapCut {
            source: 1,
            handle_source: 0,
            at_start: true,
            param: u0,
        });
    }
    if u1.is_open_unit(zero, one)? {
        try_reserve_vec(&mut cuts, 1)?;
        cuts.push(OverlapCut {
            source: 1,
            handle_source: 0,
            at_start: false,
            param: u1,
        });
    }
    if cuts.is_empty() {
        return Ok(PairContact::None);
    }
    Ok(PairContact::Overlap { cuts })
}

fn param_on_segment(segment: Segment, point: XY) -> Result<SourceParam> {
    let dx = ExactDyadic::try_from_f64(segment.end.x)?
        .try_subtract(ExactDyadic::try_from_f64(segment.start.x)?)?;
    let dy = ExactDyadic::try_from_f64(segment.end.y)?
        .try_subtract(ExactDyadic::try_from_f64(segment.start.y)?)?;
    if dx.cmp_magnitude(&dy).is_ge() {
        let num = ExactDyadic::try_from_f64(point.x)?
            .try_subtract(ExactDyadic::try_from_f64(segment.start.x)?)?;
        Ok(SourceParam::Ratio { num, den: dx })
    } else {
        let num = ExactDyadic::try_from_f64(point.y)?
            .try_subtract(ExactDyadic::try_from_f64(segment.start.y)?)?;
        Ok(SourceParam::Ratio { num, den: dy })
    }
}

fn crossing_params(a: Segment, b: Segment) -> Result<(SourceParam, SourceParam)> {
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
        SourceParam::Ratio {
            num: t_num,
            den: den.try_clone()?,
        },
        SourceParam::Ratio { num: u_num, den },
    ))
}

#[cfg(test)]
mod tests {
    use super::super::noding::dedup_undirected_segments_sourced;
    use super::*;
    use crate::geometry::generated_budget::PAIRWISE_WORK_LIMIT;

    fn seg(x0: f64, y0: f64, x1: f64, y1: f64) -> Segment {
        Segment {
            start: XY::new(x0, y0),
            end: XY::new(x1, y1),
        }
    }

    struct Case {
        unique: Vec<Segment>,
        provenance: SegmentProvenance,
        isolated: Vec<XY>,
    }

    impl Case {
        fn from_segments(segments: &[Segment]) -> Self {
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
            Self {
                unique,
                provenance,
                isolated,
            }
        }

        fn partition(&self) -> ExactPartition<'_> {
            let mut budget = ExpansionBudget::new("exact_partition", "items");
            partition_unique_segments(&self.unique, &self.provenance, &self.isolated, &mut budget)
                .unwrap()
        }
    }

    fn run(segments: &[Segment]) -> Case {
        Case::from_segments(segments)
    }

    #[test]
    fn generated_budget_charges_only_net_canonical_growth() {
        use crate::geometry::generated_budget::GENERATED_ITEM_LIMIT;

        let crossing = [seg(0.0, 0.0, 2.0, 2.0), seg(0.0, 2.0, 2.0, 0.0)];
        let cases = [
            (vec![seg(0.0, 0.0, 1.0, 0.0)], 0),
            (vec![seg(0.0, 0.0, 1.0, 0.0); 2], 0),
            (vec![seg(0.0, 0.0, 1.0, 0.0), seg(1.0, 0.0, 2.0, 0.0)], 0),
            (vec![seg(1.0, 1.0, 1.0, 1.0)], 0),
            (crossing.to_vec(), 3),
            (vec![seg(0.0, 0.0, 4.0, 0.0), seg(1.0, 0.0, 3.0, 0.0)], 1),
            (vec![seg(0.0, 0.0, 2.0, 0.0), seg(1.0, 0.0, 1.0, 0.0)], 1),
            (vec![crossing[0], crossing[1], seg(1.0, 1.0, 1.0, 1.0)], 2),
            (
                vec![
                    crossing[0],
                    crossing[1],
                    seg(10.0, 0.0, 14.0, 0.0),
                    seg(10.0, 0.0, 12.0, 0.0),
                    seg(12.0, 0.0, 14.0, 0.0),
                ],
                2,
            ),
        ];
        for (segments, growth) in cases {
            let case = run(&segments);
            let mut budget = ExpansionBudget::new("exact_partition", "items");
            budget.add(GENERATED_ITEM_LIMIT - growth).unwrap();
            let partition = partition_unique_segments(
                &case.unique,
                &case.provenance,
                &case.isolated,
                &mut budget,
            )
            .unwrap();
            assert_eq!(budget.used(), GENERATED_ITEM_LIMIT);
            assert!(!partition.recipes.is_empty());
            if growth > 0 {
                let mut short = ExpansionBudget::new("exact_partition", "items");
                short.add(GENERATED_ITEM_LIMIT - growth + 1).unwrap();
                let error = match partition_unique_segments(
                    &case.unique,
                    &case.provenance,
                    &case.isolated,
                    &mut short,
                ) {
                    Err(error) => error,
                    Ok(_) => panic!("one fewer generated item must reject the partition"),
                };
                assert!(matches!(
                    error.kind(),
                    crate::error::ErrorKind::Geometry(
                        GeometryErrorKind::GeneratedOutputTooLarge { .. }
                    )
                ));
            }
        }
    }

    fn constructed_recipe<'a>(partition: &'a ExactPartition<'_>) -> &'a NodeRecipe {
        partition
            .recipes
            .iter()
            .find(|recipe| matches!(recipe, NodeRecipe::Constructed { .. }))
            .expect("expected a constructed recipe")
    }

    fn assert_ratio_eq(
        num: &ExactDyadic,
        den: &ExactDyadic,
        expected_n: &ExactDyadic,
        expected_d: &ExactDyadic,
    ) {
        assert_eq!(
            try_compare_ratios(num, den, expected_n, expected_d).unwrap(),
            Ordering::Equal
        );
    }

    fn constructed_count(partition: &ExactPartition) -> usize {
        partition
            .recipes
            .iter()
            .filter(|recipe| matches!(recipe, NodeRecipe::Constructed { .. }))
            .count()
    }

    fn stored_node(partition: &ExactPartition, point: XY) -> Option<NodeId> {
        let key = PointKey::new(point);
        partition
            .recipes
            .iter()
            .position(|recipe| matches!(recipe, NodeRecipe::Stored(stored) if *stored == key))
            .map(|index| NodeId(index as u32))
    }

    fn component_count(partition: &ExactPartition) -> usize {
        let n = partition.recipes.len();
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
        for span in &partition.spans {
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

    fn recipe_is_open_unit(recipe: &NodeRecipe) -> bool {
        let NodeRecipe::Constructed { num, den, .. } = recipe else {
            return false;
        };
        let zero = ExactDyadic::try_from_f64(0.0).unwrap();
        let one = ExactDyadic::try_from_f64(1.0).unwrap();
        try_compare_ratios(num, den, &zero, &one).unwrap() == Ordering::Greater
            && try_compare_ratios(num, den, &one, &one).unwrap() == Ordering::Less
    }

    #[test]
    fn public_entry_matches_inner_on_empty() {
        let mut budget = ExpansionBudget::new("exact_partition", "items");
        let (unique, provenance) = dedup_undirected_segments_sourced(Vec::<Segment>::new());
        let partition = partition_unique_segments(&unique, &provenance, &[], &mut budget).unwrap();
        assert_eq!(partition.recipes.len(), 0);
    }

    #[test]
    fn generated_t_has_seven_nodes_five_spans_two_components() {
        let rn_sixth = 1.0_f64 / 6.0;
        let case = run(&[
            seg(0.0, 0.0, 3.0, 1.0),
            seg(1.0, -1.0, 1.0, 1.0),
            seg(0.5, 0.0, 0.5, rn_sixth),
        ]);
        let partition = case.partition();
        assert_eq!(partition.unique().len(), 3);
        assert_eq!(partition.recipes.len(), 7);
        assert_eq!(partition.spans.len(), 5);
        assert_eq!(constructed_count(&partition), 1);
        assert_eq!(component_count(&partition), 2);
        let c_end = stored_node(&partition, XY::new(0.5, rn_sixth)).unwrap();
        let on_a = partition
            .spans
            .iter()
            .any(|span| span.unique == 0 && (span.start == c_end || span.end == c_end));
        assert!(!on_a, "C's endpoint must not node onto A");
        let NodeRecipe::Constructed { unique, num, den } = constructed_recipe(&partition) else {
            panic!("expected a constructed recipe");
        };
        let one = ExactDyadic::try_from_f64(1.0).unwrap();
        let three = ExactDyadic::try_from_f64(3.0).unwrap();
        let two = ExactDyadic::try_from_f64(2.0).unwrap();
        match *unique {
            0 => assert_ratio_eq(num, den, &one, &three),
            1 => assert_ratio_eq(num, den, &two, &three),
            other => panic!("unexpected constructed unique {other}"),
        }
    }

    #[test]
    fn endpoint_near_miss_five_nodes_four_spans() {
        let nd = 1.0_f64.next_down();
        let case = run(&[seg(0.0, 0.0, 1.0, 1.0), seg(1.0, nd, 0.0, 2.0)]);
        let partition = case.partition();
        assert_eq!(partition.recipes.len(), 5);
        assert_eq!(partition.spans.len(), 4);
        assert_eq!(constructed_count(&partition), 1);
        let q_num = ExactDyadic::try_from_f64(1.0).unwrap();
        let two = ExactDyadic::try_from_f64(2.0).unwrap();
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
        let NodeRecipe::Constructed {
            unique,
            num,
            den: recipe_den,
        } = constructed_recipe(&partition)
        else {
            panic!("expected a constructed recipe");
        };
        match *unique {
            0 => assert_ratio_eq(num, recipe_den, &p54, &den),
            other => panic!("unexpected constructed unique {other}"),
        }
    }

    #[test]
    fn concurrent_three_sources_one_constructed_node() {
        let case = run(&[
            seg(0.0, 0.0, 3.0, 1.0),
            seg(1.0, -1.0, 1.0, 1.0),
            seg(0.0, 1.0, 3.0, -1.0),
        ]);
        let partition = case.partition();
        assert_eq!(constructed_count(&partition), 1);
        let NodeRecipe::Constructed { unique, num, den } = constructed_recipe(&partition) else {
            panic!("expected a constructed recipe");
        };
        let one = ExactDyadic::try_from_f64(1.0).unwrap();
        let two = ExactDyadic::try_from_f64(2.0).unwrap();
        let three = ExactDyadic::try_from_f64(3.0).unwrap();
        match *unique {
            0 => assert_ratio_eq(num, den, &one, &three),
            1 => assert_ratio_eq(num, den, &two, &three),
            other => panic!("unexpected constructed unique {other}"),
        }
    }

    #[test]
    fn overlap_reversed_duplicate_keeps_reverse_owner() {
        let case = run(&[
            seg(0.0, 0.0, 4.0, 0.0),
            seg(1.0, 0.0, 3.0, 0.0),
            seg(2.0, -1.0, 2.0, 1.0),
            seg(4.0, 0.0, 0.0, 0.0),
        ]);
        let partition = case.partition();
        let reversed = partition.spans.iter().enumerate().any(|(index, _)| {
            partition
                .owners(index)
                .iter()
                .any(|&(ordinal, reversed)| ordinal == 3 && reversed)
        });
        assert!(reversed, "reversed duplicate must keep the reverse flag");
        let overlap = partition.spans.iter().enumerate().filter(|(index, _)| {
            let owners = partition.owners(*index);
            owners.iter().any(|(ordinal, _)| *ordinal == 0)
                && owners.iter().any(|(ordinal, _)| *ordinal == 1)
        });
        assert!(
            overlap.count() > 0,
            "overlap of A and B must be one aggregated atom"
        );
    }

    #[test]
    fn overlap_aggregate_owners_are_one_atom() {
        let case = run(&[seg(0.0, 0.0, 4.0, 0.0), seg(1.0, 0.0, 3.0, 0.0)]);
        let partition = case.partition();
        let shared: Vec<_> = partition
            .spans
            .iter()
            .enumerate()
            .filter(|(index, _)| {
                let owners = partition.owners(*index);
                owners.iter().any(|(ordinal, _)| *ordinal == 0)
                    && owners.iter().any(|(ordinal, _)| *ordinal == 1)
            })
            .collect();
        assert_eq!(shared.len(), 1);
        assert_eq!(partition.owners(shared[0].0).len(), 2);
    }

    #[test]
    fn degenerate_point_is_isolated_node() {
        let case = run(&[seg(0.0, 0.0, 2.0, 0.0), seg(1.0, 1.0, 1.0, 1.0)]);
        let partition = case.partition();
        assert_eq!(partition.isolated.len(), 1);
        assert_eq!(
            stored_node(&partition, XY::new(1.0, 1.0)),
            Some(partition.isolated[0])
        );
    }

    #[test]
    fn degenerate_unique_without_isolated_slice_does_not_vanish() {
        let (unique, provenance) = dedup_undirected_segments_sourced(vec![
            seg(0.0, 0.0, 2.0, 0.0),
            seg(1.0, 1.0, 1.0, 1.0),
        ]);
        let mut budget = ExpansionBudget::new("exact_partition", "items");
        let partition = partition_unique_segments(&unique, &provenance, &[], &mut budget).unwrap();
        assert!(stored_node(&partition, XY::new(1.0, 1.0)).is_some());
        assert_eq!(partition.isolated.len(), 1);
    }

    #[test]
    fn signed_zero_endpoints_share_a_stored_node() {
        let case = run(&[seg(-0.0, 0.0, 1.0, 0.0), seg(0.0, 0.0, 0.0, 1.0)]);
        let partition = case.partition();
        let a = stored_node(&partition, XY::new(-0.0, 0.0)).unwrap();
        let b = stored_node(&partition, XY::new(0.0, 0.0)).unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn full_finite_exponent_pair_is_five_nodes_four_spans() {
        let min_sub = f64::from_bits(1);
        let a = seg(0.0, 0.0, f64::MAX, min_sub);
        let b = seg(min_sub, -f64::MAX, f64::MAX, f64::MAX);
        let case = run(&[a, b]);
        let partition = case.partition();
        assert_eq!(partition.recipes.len(), 5);
        assert_eq!(partition.spans.len(), 4);
        assert_eq!(constructed_count(&partition), 1);
        for point in [a.start, a.end, b.start, b.end] {
            assert!(stored_node(&partition, point).is_some());
        }
        let constructed = partition
            .recipes
            .iter()
            .find(|recipe| matches!(recipe, NodeRecipe::Constructed { .. }))
            .unwrap();
        assert!(recipe_is_open_unit(constructed));
    }

    #[test]
    fn isolated_on_line_cuts_the_edge() {
        let mut budget = ExpansionBudget::new("exact_partition", "items");
        let (unique, provenance) = dedup_undirected_segments_sourced(vec![seg(0.0, 0.0, 2.0, 0.0)]);
        let partition =
            partition_unique_segments(&unique, &provenance, &[XY::new(1.0, 0.0)], &mut budget)
                .unwrap();
        assert_eq!(partition.recipes.len(), 3);
        assert_eq!(partition.spans.len(), 2);
        assert!(partition.isolated.is_empty());
        let mid = stored_node(&partition, XY::new(1.0, 0.0)).unwrap();
        assert!(
            partition
                .spans
                .iter()
                .any(|span| span.start == mid || span.end == mid)
        );
    }

    #[test]
    fn isolated_at_crossing_is_stored_not_constructed() {
        let mut budget = ExpansionBudget::new("exact_partition", "items");
        let (unique, provenance) = dedup_undirected_segments_sourced(vec![
            seg(0.0, 0.0, 2.0, 2.0),
            seg(0.0, 2.0, 2.0, 0.0),
        ]);
        let partition =
            partition_unique_segments(&unique, &provenance, &[XY::new(1.0, 1.0)], &mut budget)
                .unwrap();
        assert_eq!(partition.recipes.len(), 5);
        assert_eq!(partition.spans.len(), 4);
        assert_eq!(constructed_count(&partition), 0);
        assert!(partition.isolated.is_empty());
        assert!(matches!(
            partition.recipe(stored_node(&partition, XY::new(1.0, 1.0)).unwrap()),
            NodeRecipe::Stored(_)
        ));
    }

    #[test]
    fn stored_endpoint_concurrency_is_six_nodes_five_spans() {
        let case = run(&[
            seg(0.0, 0.0, 2.0, 0.0),
            seg(1.0, 0.0, 1.0, 1.0),
            seg(0.0, 1.0, 2.0, -1.0),
        ]);
        let partition = case.partition();
        assert_eq!(partition.recipes.len(), 6);
        assert_eq!(partition.spans.len(), 5);
        assert_eq!(constructed_count(&partition), 0);
        let junction = stored_node(&partition, XY::new(1.0, 0.0)).unwrap();
        let incident = partition
            .spans
            .iter()
            .filter(|span| span.start == junction || span.end == junction)
            .count();
        assert_eq!(incident, 5);
    }

    #[test]
    fn concurrent_three_sources_recipe_when_vertical_is_first() {
        let case = run(&[
            seg(1.0, -1.0, 1.0, 1.0),
            seg(0.0, 0.0, 3.0, 1.0),
            seg(0.0, 1.0, 3.0, -1.0),
        ]);
        let partition = case.partition();
        assert_eq!(constructed_count(&partition), 1);
        let NodeRecipe::Constructed { unique, num, den } = constructed_recipe(&partition) else {
            panic!("expected a constructed recipe");
        };
        let two = ExactDyadic::try_from_f64(2.0).unwrap();
        let three = ExactDyadic::try_from_f64(3.0).unwrap();
        match *unique {
            0 => assert_ratio_eq(num, den, &two, &three),
            1 => {
                let one = ExactDyadic::try_from_f64(1.0).unwrap();
                assert_ratio_eq(num, den, &one, &three);
            },
            other => panic!("unexpected constructed unique {other}"),
        }
    }

    #[test]
    fn star_crossings_keep_one_recipe_per_constructed_node() {
        let mut segments = vec![seg(0.0, 0.0, 5.0, 0.0)];
        for x in 1..=4 {
            segments.push(seg(f64::from(x), -1.0, f64::from(x), 1.0));
        }
        let case = run(&segments);
        let partition = case.partition();
        assert_eq!(constructed_count(&partition), 4);
        let five = ExactDyadic::try_from_f64(5.0).unwrap();
        for recipe in &partition.recipes {
            let NodeRecipe::Constructed { unique, num, den } = recipe else {
                continue;
            };
            assert_eq!(*unique, 0);
            let k = (1..=4).find(|&k| {
                let k_dy = ExactDyadic::try_from_f64(f64::from(k)).unwrap();
                try_compare_ratios(num, den, &k_dy, &five).unwrap() == Ordering::Equal
            });
            assert!(k.is_some(), "constructed param must be k/5 for k=1..=4");
        }
    }

    #[test]
    fn disjoint_thirty_two_thousand_segments_are_not_capped_as_pairs() {
        let mut segments = Vec::with_capacity(32_000);
        for index in 0..32_000 {
            let x = f64::from(index);
            segments.push(seg(x, 0.0, x, 1.0));
        }
        let case = run(&segments);
        let partition = case.partition();
        assert_eq!(partition.recipes.len(), 64_000);
        assert_eq!(partition.spans.len(), 32_000);
        assert_eq!(constructed_count(&partition), 0);
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

    #[test]
    fn pairwise_limit_is_not_an_allocation_error() {
        let err = ExpansionBudget::pairwise("exact_partition", "pairs", PAIRWISE_WORK_LIMIT, 2)
            .unwrap_err();
        assert!(matches!(
            err.kind(),
            crate::error::ErrorKind::Geometry(GeometryErrorKind::GeneratedOutputTooLarge { .. })
        ));
    }
}
