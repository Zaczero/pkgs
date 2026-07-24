#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! Clean-case exact binary overlay fast path (outside-arc reassembly).
//!
//! For two SIMPLE polygons — shells AND holes — whose boundaries meet at proper
//! transverse crossings and/or exact shared boundary runs, each overlay result
//! is the directed arcs of EVERY ring (shell + holes) selected and oriented per
//! the op (including shared-edge cancellation), endpoint-chained into rings —
//! an O(n + k) construction that skips the full DCEL arrangement (face topology
//! and winding BFS) and yields a `Polygon` or, for split results, a
//! `MultiPolygon`. Holes are not special: a hole boundary is part of
//! `∂operand`, so it nodes and contributes arcs by the same rule, and "inside
//! the other operand" is the even-odd membership across the other's
//! shell+holes. Every ring is canonicalized interior-on-left (shell CCW, hole
//! CW) so all kept arcs carry the RESULT interior on their left and chain into
//! consistently-wound rings (classified by signed area).
//!
//! [`clean_overlay`] returns `None` (deferring to the exact arrangement engine,
//! which stays the correctness oracle) on ANY degeneracy outside that clean
//! model: same-operand shared/cross contacts, unexplained vertex/endpoint
//! touches, 3+ coincident boundaries, T-junctions inside shared runs, ambiguous
//! membership reseeds, a pinch vertex, a hole not nesting in its shell, or a
//! multi-shell-with-holes nesting. A debug differential test (`union` /
//! `difference` / `symmetric_difference` / `intersection`, convex +
//! non-convex + HOLED + shared-edge fixtures) pins it to the engine.

use super::*;
use crate::collections::HashMap;
use crate::geometry::topology::{self, Operand, OperandPool, OrientedRing};

pub(crate) fn clean_overlay<P: AsRef<Polygon>, Q: AsRef<Polygon>>(
    a: &[P],
    b: &[Q],
    op: OverlayOp,
) -> Option<Shape> {
    // Every ring of every component of both operands, oriented interior-on-left
    // (shell CCW, hole CW), staged into one thin segment pool. Holes and extra
    // components are direct boundary contributors under the same policy.
    let pool = topology::build_operand_pool(a, b);
    debug_assert_eq!(pool.ring_of.len(), pool.segments.len());
    debug_assert!(pool.split <= pool.segments.len());
    debug_assert!(
        pool.rings
            .iter()
            .all(|ring| ring.is_hole == (ring.ring != 0))
    );
    // Bail if any ring was degenerate (fewer staged than the component inventory)
    // — the exact engine owns degenerate input.
    if pool.rings.len() != topology::operand_ring_count(a) + topology::operand_ring_count(b) {
        return None;
    }

    // Boundary contacts per pool edge (global index). Bails on any contact
    // outside the clean model; the exact arrangement owns those cases.
    let mut contacts = collect_boundary_contacts(&pool, op)?;
    // No crosses or shared runs ⟹ disjoint/nested/touch-only; upstream
    // shortcuts or the exact engine own those.
    if !contacts.has_cross && !contacts.has_shared {
        return None;
    }

    // Pre-size the kept-arc upper bound (every pool edge plus the two extra
    // sub-arcs each cut can split) so the hot push loop rarely reallocs.
    let cut_count: usize = contacts.cuts.iter().map(Vec::len).sum();
    let arc_cap = pool.segments.len() + 2 * cut_count;

    // `symmetric_difference` is two clean DIFFERENCEs reassembled SEPARATELY —
    // it cannot share the single-pass walk (the a−b and b−a pieces meet at every
    // crossing, so one combined arc walk would pinch). Measured 2026-06-17: this
    // two-pass chained reassembly (≈2.2× a single union) is ~2× FASTER than
    // bailing to the arrangement XOR path (the DCEL/face build outweighs two
    // lightweight chained walks), so it is the better of the two available
    // approaches — the residual vs GEOS's unified single-pass XOR stays here.
    if op == OverlayOp::SymmetricDifference && !contacts.has_shared {
        return symmetric_difference_shape(&pool, &mut contacts, arc_cap);
    }
    if op == OverlayOp::SymmetricDifference && contacts.has_cross {
        return None;
    }

    // UNION / INTERSECTION / DIFFERENCE (no shared runs): chain in O(crossings)
    // instead of hashing every endpoint — the lever for the super-linear large-n
    // reassembly. Union/intersection keep arcs forward; difference walks B's kept
    // (inside) arcs in reverse via the section-reversal trick. A bail falls
    // THROUGH to the hash path (the oracle-equivalent), so it is never wrong.
    if !contacts.has_shared
        && matches!(
            op,
            OverlayOp::Union | OverlayOp::Intersection | OverlayOp::Difference
        )
        && let Some(shape) = overlay_transverse_shape(&pool, &mut contacts, arc_cap, op)
    {
        return Some(shape);
    }

    // Directed result-boundary arcs from each ring, oriented per the op rule.
    let mut arcs: Vec<(XY, XY)> = Vec::with_capacity(arc_cap);
    keep_operand_arcs(op, &pool, Operand::Left, true, &mut contacts, &mut arcs)?;
    keep_operand_arcs(op, &pool, Operand::Right, false, &mut contacts, &mut arcs)?;
    if arcs.is_empty() {
        return None;
    }
    reassemble(&arcs)
}

/// Walk one operand's rings, keeping and orienting each ring's sections per
/// `op`/`is_a` into `arcs`.
pub(crate) fn keep_operand_arcs(
    op: OverlayOp,
    pool: &OperandPool,
    operand: Operand,
    is_a: bool,
    contacts: &mut BoundaryContacts,
    arcs: &mut Vec<(XY, XY)>,
) -> Option<()> {
    let other = opposite(operand);
    // The shared-edge case is rare; when there are no shared runs anywhere, the
    // common transverse path skips the per-section shared/reseed bookkeeping.
    let transverse = !contacts.has_shared;
    for ring in pool.rings.iter().filter(|ring| ring.operand == operand) {
        if transverse {
            keep_arcs_transverse(op, is_a, ring, contacts, pool, other, arcs)?;
        } else {
            keep_arcs(op, is_a, ring, contacts, pool, other, arcs)?;
        }
    }
    Some(())
}

/// Light arc selection for the COMMON case with NO shared runs: build the
/// ring's transverse arcs inline (no per-section `ArcSection`/shared-run
/// lookup), seed membership ONCE, then alternate at each crossing (Jordan
/// parity). Bit- identical to [`keep_arcs`] when the operand has no shared
/// edges — it just avoids the shared-edge machinery the general path pays per
/// section.
pub(crate) fn keep_arcs_transverse(
    op: OverlayOp,
    is_a: bool,
    ring: &OrientedRing,
    contacts: &mut BoundaryContacts,
    pool: &OperandPool,
    other: Operand,
    arcs: &mut Vec<(XY, XY)>,
) -> Option<()> {
    with_section_scratch(|ordered| {
        build_transverse_sections(ring, contacts, pool, ordered);
        if ordered.is_empty() {
            return Some(());
        }
        let mut inside = seed_membership(ordered, pool, other)?;
        let start_inside = inside;
        for &(from, to, ends_at_crossing) in ordered.iter() {
            match arc_rule(op, is_a, inside) {
                ArcAction::KeepForward => arcs.push((from, to)),
                ArcAction::KeepReversed => arcs.push((to, from)),
                ArcAction::Drop => {},
            }
            if ends_at_crossing {
                inside = !inside;
            }
        }
        (inside == start_inside).then_some(())
    })
}

/// Split one ring into directed `(from, to, ends_at_crossing)` sections at its
/// sorted cut points — the shared front of the membership-alternation keepers.
pub(crate) fn build_transverse_sections(
    ring: &OrientedRing,
    contacts: &mut BoundaryContacts,
    pool: &OperandPool,
    ordered: &mut Vec<(XY, XY, bool)>,
) {
    ordered.clear();
    ordered.reserve(ring.points.len());
    for segment_index in ring.segments.clone() {
        let segment = pool.segments[segment_index];
        sort_dedup_cuts(segment, &mut contacts.cuts[segment_index]);
        let mut from = segment.start;
        for cut in &contacts.cuts[segment_index] {
            if !same_point(from, cut.point) {
                ordered.push((from, cut.point, cut.cross));
                from = cut.point;
            }
        }
        if !same_point(from, segment.end) {
            ordered.push((from, segment.end, false));
        }
    }
}

/// A reusable `(from, to, ends_at_crossing)` section buffer — the per-ring
/// section list is a transient consumed inside one `keep_*` call, so the
/// allocation is recycled across every ring of every overlay instead of a fresh
/// `Vec` per ring (matches the cut/run scratch pools).
pub(crate) fn with_section_scratch<R>(body: impl FnOnce(&mut Vec<(XY, XY, bool)>) -> R) -> R {
    thread_local! {
        static SECTIONS: std::cell::Cell<Vec<(XY, XY, bool)>> = const { std::cell::Cell::new(Vec::new()) };
    }
    let mut sections = SECTIONS.take();
    let result = body(&mut sections);
    sections.clear();
    SECTIONS.set(sections);
    result
}

/// Strict membership of the ring's first section midpoint in the other operand.
pub(crate) fn seed_membership(
    ordered: &[(XY, XY, bool)],
    pool: &OperandPool,
    other: Operand,
) -> Option<bool> {
    let (seed_from, seed_to, _) = ordered[0];
    strict_section_membership(pool, other, ArcSection {
        from: seed_from,
        to: seed_to,
        shared: None,
        starts_after_reseed: false,
        end: SectionEnd::None,
    })
}

/// UNION fast path with O(crossings) chaining. The kept arcs (every ring's
/// OUTSIDE sections, never reversed for union) are already in result-boundary
/// order, so ring-internal successors are known by construction (the next
/// pushed arc, or the cyclic wrap); ONLY the `k` crossings need a point-keyed
/// lookup — turning reassembly's O(n) endpoint hash (which spills cache
/// super-linearly at large n) into O(k). `None` bails to the caller's hash path
/// (a chaining bail is harmless — the hash path is the oracle-equivalent
/// fallback).
/// O(crossings) RUN chain: consecutive kept sections between two crossings are
/// ONE element holding the run's point sequence (`points[lo..hi]`), instead of
/// one `(from,to)` arc per section. This collapses the per-section push/link of
/// the old per-segment chain to per-RUN, and stores each vertex once (the old
/// `(from,to)` arcs stored every interior vertex twice). Crossing successors
/// are the only point-keyed lookups (O(crossings)); the run that ends a wrap
/// links straight to the ring's first run.
pub(crate) struct RunChain {
    /// All runs' points concatenated; run `i` occupies
    /// `points[run_lo[i]..run_hi[i]]`.
    pub(crate) points: Vec<XY>,
    pub(crate) run_lo: Vec<u32>,
    pub(crate) run_hi: Vec<u32>,
    pub(crate) next: Vec<u32>,
    pub(crate) ends_at_crossing: Vec<bool>,
    pub(crate) run_start: HashMap<PointKey, u32>,
}

impl RunChain {
    pub(crate) fn with_capacity(arc_cap: usize) -> Self {
        Self {
            points: Vec::with_capacity(arc_cap),
            run_lo: Vec::new(),
            run_hi: Vec::new(),
            next: Vec::new(),
            ends_at_crossing: Vec::new(),
            run_start: HashMap::new(),
        }
    }

    pub(crate) const fn is_empty(&self) -> bool {
        self.run_lo.is_empty()
    }
}

/// Per-operand mode for the chained transverse walk:
/// union keeps each operand's OUTSIDE arcs forward; intersection keeps INSIDE
/// forward; difference keeps A's OUTSIDE forward and B's INSIDE reversed.
#[derive(Clone, Copy)]
pub(crate) struct ArcMode {
    keep_inside: bool,
    reverse: bool,
}

pub(crate) const fn chained_arc_mode(op: OverlayOp, is_a: bool) -> Option<ArcMode> {
    match op {
        OverlayOp::Union => Some(ArcMode {
            keep_inside: false,
            reverse: false,
        }),
        OverlayOp::Intersection => Some(ArcMode {
            keep_inside: true,
            reverse: false,
        }),
        OverlayOp::Difference => Some(if is_a {
            ArcMode {
                keep_inside: false,
                reverse: false,
            }
        } else {
            ArcMode {
                keep_inside: true,
                reverse: true,
            }
        }),
        OverlayOp::SymmetricDifference => None,
    }
}

pub(crate) fn overlay_transverse_shape(
    pool: &OperandPool,
    contacts: &mut BoundaryContacts,
    arc_cap: usize,
    op: OverlayOp,
) -> Option<Shape> {
    let mut chain = RunChain::with_capacity(arc_cap);
    for (operand, is_a) in [(Operand::Left, true), (Operand::Right, false)] {
        let other = opposite(operand);
        let mode = chained_arc_mode(op, is_a)?;
        for ring in pool.rings.iter().filter(|ring| ring.operand == operand) {
            keep_transverse_ring_runs(
                ring,
                contacts,
                pool,
                other,
                mode.keep_inside,
                mode.reverse,
                &mut chain,
            )?;
        }
    }
    if chain.is_empty() {
        return None;
    }
    assemble_rings(resolve_run_chain(chain)?)
}

/// Resolve a built chain's crossing successors (the only point-keyed lookups,
/// O(k)) and walk it into rings. An empty chain yields no rings (a valid empty
/// overlay piece — e.g. one operand entirely inside the other in `a−b`).
pub(crate) fn resolve_run_chain(mut chain: RunChain) -> Option<Vec<Vec<XY>>> {
    if chain.is_empty() {
        return Some(Vec::new());
    }
    // A run that ends at a crossing hands off to the run that STARTS at that
    // crossing point (the only point-keyed lookups, O(crossings)).
    for index in 0..chain.run_lo.len() {
        if chain.ends_at_crossing[index] {
            let end_point = chain.points[chain.run_hi[index] as usize - 1];
            let &target = chain.run_start.get(&PointKey::new(end_point))?;
            chain.next[index] = target;
        }
    }
    reassemble_run_rings(&chain)
}

/// Build the O(crossings) chain for one directed difference `self_op −
/// other_op` (self's OUTSIDE arcs forward + other's INSIDE arcs reversed) and
/// walk it into rings — the per-direction half of the chained
/// `symmetric_difference`.
pub(crate) fn difference_chain_rings(
    pool: &OperandPool,
    contacts: &mut BoundaryContacts,
    arc_cap: usize,
    self_op: Operand,
    other_op: Operand,
) -> Option<Vec<Vec<XY>>> {
    let mut chain = RunChain::with_capacity(arc_cap);
    for ring in pool.rings.iter().filter(|ring| ring.operand == self_op) {
        keep_transverse_ring_runs(ring, contacts, pool, other_op, false, false, &mut chain)?;
    }
    for ring in pool.rings.iter().filter(|ring| ring.operand == other_op) {
        keep_transverse_ring_runs(ring, contacts, pool, self_op, true, true, &mut chain)?;
    }
    resolve_run_chain(chain)
}

/// Streaming state for one ring's union arc walk — the FUSED replacement for
/// materializing the ring's `(from, to, ends_at_crossing)` sections into a Vec
/// and re-iterating it. Sections are pushed straight from the segment/cut scan.
///
/// The only value the original walk read ahead-of-time — whether the FIRST kept
/// arc's `from` is a run-start, i.e. whether the ring's cyclically-LAST section
/// ended at a crossing — is DEFERRED: the first kept arc remembers itself, and
/// the run-start is resolved in [`Self::finish`] once the last flag is known.
/// So no lookahead/precompute (and none of its collapsed-segment edge cases).
pub(crate) fn keep_transverse_ring_runs(
    ring: &OrientedRing,
    contacts: &mut BoundaryContacts,
    pool: &OperandPool,
    other: Operand,
    keep_inside: bool,
    reverse: bool,
    chain: &mut RunChain,
) -> Option<()> {
    with_section_scratch(|ordered| {
        build_transverse_sections(ring, contacts, pool, ordered);
        if ordered.is_empty() {
            return Some(());
        }
        // Difference keeps the OTHER operand's inside arcs REVERSED: walk the ring
        // in the OPPOSITE direction (reverse the section order, flip each
        // `(from, to)`, rotate `ends_at_crossing` left by one) so the forward
        // run-builder below emits the same arcs in result order.
        if reverse {
            let count = ordered.len();
            ordered.reverse();
            let wrap_crossing = ordered[0].2;
            for index in 0..count {
                let (from, to, _) = ordered[index];
                let ends_at_crossing = if index + 1 < count {
                    ordered[index + 1].2
                } else {
                    wrap_crossing
                };
                ordered[index] = (to, from, ends_at_crossing);
            }
        }
        let mut inside = seed_membership(ordered, pool, other)?;
        let start_inside = inside;
        // A section's `from` is a crossing iff the cyclically-previous section
        // ended at one (the run started there).
        let mut prev_crossing = ordered[ordered.len() - 1].2;
        // Collapse consecutive kept sections into ONE run: a run is open while
        // `inside == keep_inside` (which only changes at a crossing, which closes
        // the run), so every section seen while a run is open is kept. The run's
        // points are `from` then each section's `to`.
        let mut ring_first: Option<u32> = None;
        let mut open_run: Option<u32> = None;
        let mut last_run: Option<u32> = None;
        let mut last_to_crossing = false;
        for &(from, to, ends_at_crossing) in ordered.iter() {
            if inside == keep_inside {
                if open_run.is_none() {
                    let idx = chain.run_lo.len() as u32;
                    chain.run_lo.push(chain.points.len() as u32);
                    chain.run_hi.push(0);
                    chain.next.push(u32::MAX);
                    chain.ends_at_crossing.push(false);
                    chain.points.push(from);
                    if prev_crossing && chain.run_start.insert(PointKey::new(from), idx).is_some() {
                        return None; // two run-starts at one crossing — a pinch
                    }
                    if ring_first.is_none() {
                        ring_first = Some(idx);
                    }
                    open_run = Some(idx);
                }
                chain.points.push(to);
                if ends_at_crossing {
                    let idx = open_run.take().expect("run is open");
                    chain.run_hi[idx as usize] = chain.points.len() as u32;
                    chain.ends_at_crossing[idx as usize] = true;
                    last_run = Some(idx);
                    last_to_crossing = true;
                }
            }
            prev_crossing = ends_at_crossing;
            if ends_at_crossing {
                inside = !inside;
            }
        }
        // A run still open at the end WRAPS the section-0 boundary — close it (no
        // crossing) and link it onto the ring's first run, which is its
        // continuation across the wrap (its end vertex == that run's start vertex).
        if let Some(idx) = open_run {
            chain.run_hi[idx as usize] = chain.points.len() as u32;
            last_run = Some(idx);
            last_to_crossing = false;
        }
        if let Some(last) = last_run
            && !last_to_crossing
        {
            chain.next[last as usize] = ring_first?;
        }
        (inside == start_inside).then_some(())
    })
}

/// Walk the run successors into rings, concatenating each run's points. Each
/// run holds `[start .. end]`; the end vertex is the successor's start vertex,
/// so all but the last point are emitted (the successor emits the shared join).
/// `None` on an unresolved successor, a walk that fails to close, or a sub-3
/// ring.
pub(crate) fn reassemble_run_rings(chain: &RunChain) -> Option<Vec<Vec<XY>>> {
    let run_count = chain.run_lo.len();
    let mut visited = vec![false; run_count];
    let mut rings: Vec<Vec<XY>> = Vec::new();
    let mut remaining = chain.points.len();
    for seed in 0..run_count {
        if visited[seed] {
            continue;
        }
        let mut ring: Vec<XY> = Vec::with_capacity(remaining);
        let mut cursor = seed;
        while !visited[cursor] {
            visited[cursor] = true;
            let lo = chain.run_lo[cursor] as usize;
            let hi = chain.run_hi[cursor] as usize;
            ring.extend_from_slice(&chain.points[lo..hi - 1]);
            remaining -= hi - lo;
            let nxt = chain.next[cursor];
            if nxt == u32::MAX {
                return None;
            }
            cursor = nxt as usize;
        }
        if cursor != seed || ring.len() < 3 {
            return None;
        }
        rings.push(ring);
    }
    Some(rings)
}

pub(crate) const fn opposite(operand: Operand) -> Operand {
    match operand {
        Operand::Left => Operand::Right,
        Operand::Right => Operand::Left,
    }
}
