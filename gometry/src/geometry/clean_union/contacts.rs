#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use ahash::HashSetExt as _;

use crate::geometry::clean_union::{
    Cut, HashSet, OperandPool, OverlayOp, RUN_NODING_MIN, SharedSection, add_cut,
    for_each_candidate_pair, shared_section, sort_dedup_cuts,
};
use crate::geometry::{
    Contact, PointKey, SharedSpan, XY, same_point, segment_contact_exact,
    segment_envelopes_disjoint, undirected_edge_key,
};

#[derive(Clone, Copy)]
pub(crate) struct SharedArc {
    pub(crate) start: PointKey,
    pub(crate) end: PointKey,
    pub(crate) same_direction: bool,
}

pub(crate) struct BoundaryContacts {
    pub(crate) cuts: Vec<Vec<Cut>>,
    pub(crate) shared: Vec<Vec<SharedArc>>,
    pub(crate) shared_nodes: HashSet<PointKey>,
    // Cross-operand touch points; the clean model accepts a touch ONLY when it
    // is a shared-run endpoint, so only the point identity matters.
    pub(crate) touch_nodes: HashSet<PointKey>,
    pub(crate) has_cross: bool,
    pub(crate) has_shared: bool,
}

thread_local! {
    // The per-segment cut index (one list per segment, ~N at large n) is the
    // same shape every overlay; reuse it per-thread so it is allocated ONCE, not
    // once per call (matching `segment_index`'s sweep scratch). Returned on drop.
    static CUTS_SCRATCH: std::cell::RefCell<Vec<Vec<Cut>>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

impl BoundaryContacts {
    pub(crate) fn new(len: usize) -> Self {
        let mut cuts = CUTS_SCRATCH.with(|scratch| std::mem::take(&mut *scratch.borrow_mut()));
        cuts.iter_mut().for_each(Vec::clear);
        cuts.resize_with(len, Vec::new);
        Self {
            cuts,
            // Shared runs are rare (only adjacent/coincident boundaries); allocate
            // the per-segment lists LAZILY on the first one so the common
            // transverse case never pays for the empty `Vec<Vec<_>>`.
            shared: Vec::new(),
            shared_nodes: HashSet::new(),
            touch_nodes: HashSet::new(),
            has_cross: false,
            has_shared: false,
        }
    }
}

impl Drop for BoundaryContacts {
    fn drop(&mut self) {
        // Return the cut buffer (capacity intact) to the per-thread pool.
        CUTS_SCRATCH.with(|scratch| {
            *scratch.borrow_mut() = std::mem::take(&mut self.cuts);
        });
    }
}

/// Collect cross-operand crossings, exact shared runs, and touch nodes. Returns
/// `None` on any contact outside the clean model.
pub(crate) fn collect_boundary_contacts(
    pool: &OperandPool,
    op: OverlayOp,
) -> Option<BoundaryContacts> {
    if matches!(
        op,
        OverlayOp::Union | OverlayOp::Intersection | OverlayOp::Difference
    ) && let Some(scan) = crate::geometry::convex_contact::try_collect(pool)
    {
        let mut contacts = BoundaryContacts::new(pool.segments.len());
        for (segment, cuts) in scan.cuts.into_iter().enumerate() {
            for cut in cuts {
                add_cut(&mut contacts.cuts[segment], cut.point, cut.cross);
            }
        }
        contacts.has_cross = scan.has_cross;
        return Some(contacts);
    }

    let mut contacts = BoundaryContacts::new(pool.segments.len());
    let mut clean = true;
    let visit = |left: usize, right: usize| -> std::ops::ControlFlow<()> {
        let (a_seg, b_seg) = (pool.segments[left], pool.segments[right]);
        // Cheap envelope reject FIRST (4 compares), matching `full_noding_events`:
        // the sweep prunes only its own axis, so cross-axis-disjoint candidates
        // reach here and would otherwise pay four robust orientation determinants
        // in `segment_contact_exact` only to resolve to `Disjoint`. An
        // envelope-disjoint pair shares no point — no contact of any kind.
        if segment_envelopes_disjoint(a_seg, b_seg) {
            return std::ops::ControlFlow::Continue(());
        }
        let cross_operand = pool.operands[left] != pool.operands[right];
        match segment_contact_exact(a_seg, b_seg) {
            Contact::Disjoint => {},
            Contact::Cross { point } => {
                if !cross_operand {
                    clean = false;
                    return std::ops::ControlFlow::Break(());
                }
                add_cut(&mut contacts.cuts[left], point, true);
                add_cut(&mut contacts.cuts[right], point, true);
                contacts.has_cross = true;
            },
            Contact::Touch { point } => {
                if cross_operand {
                    add_cut(&mut contacts.cuts[left], point, false);
                    add_cut(&mut contacts.cuts[right], point, false);
                    record_touch_node(&mut contacts, point);
                }
            },
            Contact::Collinear { span } => {
                let positive = PointKey::new(span.start) != PointKey::new(span.end);
                if !cross_operand {
                    if positive {
                        clean = false;
                        return std::ops::ControlFlow::Break(());
                    }
                } else if positive {
                    record_shared_arc(&mut contacts, left, right, span);
                } else {
                    add_cut(&mut contacts.cuts[left], span.start, false);
                    add_cut(&mut contacts.cuts[right], span.start, false);
                    record_touch_node(&mut contacts, span.start);
                }
            },
        }
        std::ops::ControlFlow::Continue(())
    };
    // The L1-aware noding sweep, with chains broken at ring boundaries so a
    // ring junction cannot hide a same- or cross-operand contact.
    let _ = for_each_candidate_pair::<RUN_NODING_MIN>(
        &pool.segments,
        |index| pool.ring_of[index],
        visit,
    );
    if !clean {
        return None;
    }
    if contacts
        .touch_nodes
        .iter()
        .any(|key| !contacts.shared_nodes.contains(key))
    {
        return None;
    }
    validate_shared_sections(pool, &mut contacts)?;
    Some(contacts)
}

pub(crate) fn record_touch_node(contacts: &mut BoundaryContacts, point: XY) {
    contacts.touch_nodes.insert(PointKey::new(point));
}

pub(crate) fn record_shared_arc(
    contacts: &mut BoundaryContacts,
    left: usize,
    right: usize,
    span: SharedSpan,
) {
    add_cut(&mut contacts.cuts[left], span.start, false);
    add_cut(&mut contacts.cuts[left], span.end, false);
    add_cut(&mut contacts.cuts[right], span.start, false);
    add_cut(&mut contacts.cuts[right], span.end, false);
    let shared = SharedArc {
        start: PointKey::new(span.start),
        end: PointKey::new(span.end),
        same_direction: span.same_direction,
    };
    // Materialize the per-segment shared lists on first use (see `new`).
    if contacts.shared.is_empty() {
        contacts.shared.resize_with(contacts.cuts.len(), Vec::new);
    }
    contacts.shared[left].push(shared);
    contacts.shared[right].push(shared);
    contacts.shared_nodes.insert(shared.start);
    contacts.shared_nodes.insert(shared.end);
    contacts.has_shared = true;
}

pub(crate) fn validate_shared_sections(
    pool: &OperandPool,
    contacts: &mut BoundaryContacts,
) -> Option<()> {
    // No shared runs (the common transverse case): nothing to validate, and the
    // per-segment cut sort is deferred to the arc walk.
    if contacts.shared.is_empty() {
        return Some(());
    }
    for index in 0..pool.segments.len() {
        let segment = pool.segments[index];
        sort_dedup_cuts(segment, &mut contacts.cuts[index]);
        if contacts.shared[index].is_empty() {
            continue;
        }
        let mut matched = HashSet::new();
        let mut from = segment.start;
        // Borrow the cuts in place — the loop only reads them (and the disjoint
        // `contacts.shared` field), so no defensive clone is needed.
        for cut in &contacts.cuts[index] {
            if !same_point(from, cut.point) {
                if let SharedSection::Shared(shared) =
                    shared_section(segment, from, cut.point, &contacts.shared[index])?
                {
                    matched.insert(undirected_edge_key(shared.start, shared.end));
                }
                from = cut.point;
            }
        }
        if !same_point(from, segment.end)
            && let SharedSection::Shared(shared) =
                shared_section(segment, from, segment.end, &contacts.shared[index])?
        {
            matched.insert(undirected_edge_key(shared.start, shared.end));
        }
        if contacts.shared[index]
            .iter()
            .any(|shared| !matched.contains(&undirected_edge_key(shared.start, shared.end)))
        {
            return None;
        }
    }
    Some(())
}
