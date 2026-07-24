use super::*;

#[derive(Clone, Copy)]
pub(crate) struct ArcSection {
    pub(crate) from: XY,
    pub(crate) to: XY,
    pub(crate) shared: Option<SharedArc>,
    pub(crate) starts_after_reseed: bool,
    pub(crate) end: SectionEnd,
}

#[derive(Clone, Copy)]
pub(crate) enum SharedSection {
    Open,
    Shared(SharedArc),
}

/// Split `ring` into directed sections, cancel exact shared runs, and push the
/// non-shared sections selected by `op`. Membership is seeded from strict
/// section midpoints and toggled only after proper crosses; tangential shared
/// or touch boundaries force a fresh reseed.
pub(crate) fn keep_arcs(
    op: OverlayOp,
    is_a: bool,
    ring: &OrientedRing,
    contacts: &mut BoundaryContacts,
    pool: &OperandPool,
    other: Operand,
    arcs: &mut Vec<(XY, XY)>,
) -> Option<()> {
    let sections = ring_sections(ring, pool, contacts)?;
    if sections.is_empty() {
        return Some(());
    }
    let seed_index = sections.iter().position(|section| section.shared.is_none());
    let mut inside = if let Some(seed_index) = seed_index {
        strict_section_membership(pool, other, sections[seed_index])?
    } else {
        false
    };
    let start_inside = inside;
    let mut saw_reseed = seed_index.is_some_and(|index| sections[index].starts_after_reseed);
    let mut reseed_next = false;
    let mut seeded = false;
    let start = seed_index.unwrap_or(0);
    for step in 0..sections.len() {
        let section = sections[(start + step) % sections.len()];
        if let Some(shared) = section.shared {
            match shared_arc_rule(op, is_a, shared.same_direction) {
                SharedAction::KeepForward => arcs.push((section.from, section.to)),
                SharedAction::Drop => {},
                SharedAction::BailOp => return None,
            }
            reseed_next = true;
            saw_reseed = true;
            continue;
        }
        if seeded {
            if section.starts_after_reseed || reseed_next {
                inside = strict_section_membership(pool, other, section)?;
                saw_reseed = true;
                reseed_next = false;
            }
        } else {
            seeded = true;
            reseed_next = false;
        }
        match arc_rule(op, is_a, inside) {
            ArcAction::KeepForward => arcs.push((section.from, section.to)),
            ArcAction::KeepReversed => arcs.push((section.to, section.from)),
            ArcAction::Drop => {},
        }
        match section.end {
            SectionEnd::Cross => inside = !inside,
            SectionEnd::Reseed => {
                reseed_next = true;
                saw_reseed = true;
            },
            SectionEnd::None => {},
        }
    }
    // Without a tangential reseed, a simple ring crosses another an EVEN number
    // of times, so membership must close back to where it started.
    (saw_reseed || inside == start_inside).then_some(())
}

pub(crate) fn ring_sections(
    ring: &OrientedRing,
    pool: &OperandPool,
    contacts: &mut BoundaryContacts,
) -> Option<Vec<ArcSection>> {
    let mut sections = Vec::with_capacity(ring.points.len());
    for segment_index in ring.segments.clone() {
        let segment = pool.segments[segment_index];
        sort_dedup_cuts(segment, &mut contacts.cuts[segment_index]);
        // Borrow the sorted cuts + shared runs in place (disjoint fields) — no
        // per-segment clone of the cut list (that was ~one alloc per edge).
        let cuts = &contacts.cuts[segment_index];
        let shared_runs = &contacts.shared[segment_index];
        let mut from = segment.start;
        let mut starts_after_reseed = cuts
            .iter()
            .any(|cut| !cut.cross && same_point(cut.point, segment.start));
        for cut in cuts {
            if !same_point(from, cut.point) {
                let shared = match shared_section(segment, from, cut.point, shared_runs)? {
                    SharedSection::Open => None,
                    SharedSection::Shared(shared) => Some(shared),
                };
                sections.push(ArcSection {
                    from,
                    to: cut.point,
                    shared,
                    starts_after_reseed,
                    end: section_end(cut.cross, shared.is_some()),
                });
                starts_after_reseed = !cut.cross;
                from = cut.point;
            }
        }
        if !same_point(from, segment.end) {
            let shared = match shared_section(segment, from, segment.end, shared_runs)? {
                SharedSection::Open => None,
                SharedSection::Shared(shared) => Some(shared),
            };
            sections.push(ArcSection {
                from,
                to: segment.end,
                shared,
                starts_after_reseed,
                end: SectionEnd::None,
            });
        }
    }
    Some(sections)
}

pub(crate) fn shared_section(
    segment: Segment,
    from: XY,
    to: XY,
    shared: &[SharedArc],
) -> Option<SharedSection> {
    // Common transverse case: no shared runs on this edge — skip the key work.
    if shared.is_empty() {
        return Some(SharedSection::Open);
    }
    let start = PointKey::new(from);
    let end = PointKey::new(to);
    let mut found = None;
    let mut covered_by_longer = false;
    for arc in shared {
        let exact =
            (arc.start == start && arc.end == end) || (arc.start == end && arc.end == start);
        if exact {
            if found.is_some() {
                return None;
            }
            found = Some(*arc);
        } else if section_inside_shared_arc(segment, from, to, *arc) {
            covered_by_longer = true;
        }
    }
    if found.is_none() && covered_by_longer {
        return None;
    }
    Some(found.map_or(SharedSection::Open, SharedSection::Shared))
}

pub(crate) fn section_inside_shared_arc(
    segment: Segment,
    from: XY,
    to: XY,
    shared: SharedArc,
) -> bool {
    let a = shared.start.xy();
    let b = shared.end.xy();
    let (lo, hi) = if compare_along_segment(segment, a, b).is_gt() {
        (b, a)
    } else {
        (a, b)
    };
    !compare_along_segment(segment, lo, from).is_gt()
        && !compare_along_segment(segment, to, hi).is_gt()
}

pub(crate) fn strict_section_membership(
    pool: &OperandPool,
    operand: Operand,
    section: ArcSection,
) -> Option<bool> {
    let midpoint = XY::new(
        f64::midpoint(section.from.x, section.to.x),
        f64::midpoint(section.from.y, section.to.y),
    );
    if same_point(midpoint, section.from)
        || same_point(midpoint, section.to)
        || operand_covers_boundary(pool, operand, midpoint)
    {
        return None;
    }
    Some(other_contains(pool, operand, midpoint))
}
