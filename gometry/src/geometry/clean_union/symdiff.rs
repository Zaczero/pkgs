use super::*;
/// `symmetric_difference` = (a−b) ∪ (b−a). For proper-cross-only cases, the
/// two pieces touch at crossing points, so a single combined arc walk would
/// PINCH (two outgoing arcs per crossing vertex). Instead reassemble each clean
/// DIFFERENCE arc set on its own (one outgoing arc per vertex) and assemble the
/// COMBINED rings — the signed-area classifier then sorts the crescents/voids.
/// Either piece may be empty (one operand inside the other), which is a valid
/// contribution, not a bail. The single contact table is shared; `keep_arcs`
/// re-sorts cut lists idempotently, so the two difference passes reuse it.
pub(crate) fn symmetric_difference_shape(
    pool: &OperandPool,
    contacts: &mut BoundaryContacts,
    arc_cap: usize,
) -> Option<Shape> {
    // Fast path: each direction is a clean DIFFERENCE, so chain both in
    // O(crossings) (no endpoint hashing) and assemble the COMBINED rings. A bail
    // falls through to the hash path below — itself the oracle-equivalent.
    // (Measured 2026-06-17: a single-walk dual-chain sharing the per-ring
    // section build was a WASH — 4.48 vs 4.47ms — so the proven two-pass stays;
    // the only further lever is an intricate single-loop reverse-during-forward
    // chain, high-risk for a sub-parity gain. See #95.)
    if let Some(mut rings) =
        difference_chain_rings(pool, contacts, arc_cap, Operand::Left, Operand::Right)
        && let Some(more) =
            difference_chain_rings(pool, contacts, arc_cap, Operand::Right, Operand::Left)
    {
        rings.extend(more);
        if rings.is_empty() {
            return None;
        }
        return assemble_rings(rings);
    }
    // `a ^ b = (a−b) ∪ (b−a)`. The two difference results meet at every
    // crossing, so they must reassemble SEPARATELY (a single combined walk
    // pinches). But each ring section belongs to exactly ONE of them — an A
    // section outside B bounds `a−b`, inside B it bounds `b−a` (reversed); a B
    // section inside A bounds `a−b` (reversed), outside A it bounds `b−a`. So
    // classify every ring ONCE (the costly section-split + membership seed) and
    // route its sections to both arc lists, instead of re-walking each ring in a
    // second full pass.
    let mut arcs_ab: Vec<(XY, XY)> = Vec::with_capacity(arc_cap);
    let mut arcs_ba: Vec<(XY, XY)> = Vec::with_capacity(arc_cap);
    for ring in pool
        .rings
        .iter()
        .filter(|ring| ring.operand == Operand::Left)
    {
        keep_arcs_symdiff(
            ring,
            true,
            contacts,
            pool,
            Operand::Right,
            &mut arcs_ab,
            &mut arcs_ba,
        )?;
    }
    for ring in pool
        .rings
        .iter()
        .filter(|ring| ring.operand == Operand::Right)
    {
        keep_arcs_symdiff(
            ring,
            false,
            contacts,
            pool,
            Operand::Left,
            &mut arcs_ab,
            &mut arcs_ba,
        )?;
    }
    let mut rings = reassemble_to_rings(&arcs_ab)?;
    rings.extend(reassemble_to_rings(&arcs_ba)?);
    if rings.is_empty() {
        return None;
    }
    assemble_rings(rings)
}

/// Classify one ring's transverse sections ONCE and route each to the `a−b`
/// (`arcs_ab`) or `b−a` (`arcs_ba`) boundary with the orientation that
/// [`arc_rule`] gives each `Difference` direction — the single-pass core of
/// [`symmetric_difference_shape`]. `is_left` marks an A ring (vs a B ring);
/// `other` is the opposite operand the membership is tested against.
pub(crate) fn keep_arcs_symdiff(
    ring: &OrientedRing,
    is_left: bool,
    contacts: &mut BoundaryContacts,
    pool: &OperandPool,
    other: Operand,
    arcs_ab: &mut Vec<(XY, XY)>,
    arcs_ba: &mut Vec<(XY, XY)>,
) -> Option<()> {
    with_section_scratch(|ordered| {
        build_transverse_sections(ring, contacts, pool, ordered);
        if ordered.is_empty() {
            return Some(());
        }
        let mut inside = seed_membership(ordered, pool, other)?;
        let start_inside = inside;
        for &(from, to, ends_at_crossing) in ordered.iter() {
            // A: outside→a−b forward, inside→b−a reversed. B: inside→a−b
            // reversed, outside→b−a forward. (The `Difference` arc_rule, split.)
            match (is_left, inside) {
                (true, false) => arcs_ab.push((from, to)),
                (true, true) => arcs_ba.push((to, from)),
                (false, true) => arcs_ab.push((to, from)),
                (false, false) => arcs_ba.push((from, to)),
            }
            if ends_at_crossing {
                inside = !inside;
            }
        }
        (inside == start_inside).then_some(())
    })
}
