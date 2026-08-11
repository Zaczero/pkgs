use crate::geometry::relate_ng::{
    AreaTesters, BoundaryContacts, Operand, OperandPool, OrientedRing, PointKey, RingClass,
    SectionEnd, Segment, SharedRun, TopologyComputer, XY, operand_covers_boundary, other_contains,
    same_point, section_end, sort_dedup_cuts,
};

#[derive(Default)]
pub(crate) struct NodeIncidence {
    pub(crate) through: Vec<Segment>,
    pub(crate) endpoints: Vec<XY>,
}

impl NodeIncidence {
    pub(crate) fn add(&mut self, segment: Segment, point: XY) {
        if same_point(point, segment.start) {
            self.push_endpoint(segment.end);
        } else if same_point(point, segment.end) {
            self.push_endpoint(segment.start);
        } else {
            self.through.push(segment);
        }
    }

    fn push_endpoint(&mut self, point: XY) {
        if !self.endpoints.iter().any(|known| same_point(*known, point)) {
            self.endpoints.push(point);
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct BoundarySection {
    from: XY,
    to: XY,
    /// A point strictly inside this section that provably lies ON this
    /// operand's boundary — the seed/reseed membership probe. For a section
    /// confined to one segment this is its chord midpoint; for a coalesced
    /// multi-segment arc the chord would cut through the interior, so it is
    /// the midpoint of the arc's first segment instead.
    probe: XY,
    starts_after_reseed: bool,
    shared: bool,
    end: SectionEnd,
}

pub(crate) fn classify_boundary_sections(
    operand: Operand,
    pool: &OperandPool,
    contacts: &mut BoundaryContacts,
    computer: &mut TopologyComputer<'_>,
    testers: AreaTesters<'_>,
) -> bool {
    // Per-ring boundary sections are the same ~N shape every relate; reuse one
    // thread-local buffer instead of allocating a fresh `Vec` per ring (the
    // large-polygon profile's hot allocation).
    thread_local! {
        static SECTIONS_SCRATCH: std::cell::Cell<Vec<BoundarySection>> =
            const { std::cell::Cell::new(Vec::new()) };
    }
    let mut sections = SECTIONS_SCRATCH.take();
    let other = opposite(operand);
    let mut early_decided = false;
    let mut deferred = false;
    for ring in pool.rings.iter().filter(|ring| ring.operand == operand) {
        ring_sections(ring, pool, contacts, &mut sections);
        if sections.is_empty() {
            continue;
        }
        let Some(seed_index) = sections.iter().position(|section| !section.shared) else {
            continue;
        };
        let seed = sections[seed_index];
        let Some(mut inside) = strict_section_membership(pool, other, seed, testers) else {
            deferred = true;
            break;
        };
        let start_inside = inside;
        let mut saw_reseed = seed.starts_after_reseed;
        let mut reseed_next = false;
        // Walk the cyclic section order starting at `seed_index` via a split+chain
        // (tail then head) — identical visit order to `(seed_index + step) % len`
        // without the per-iteration `div` that modulo indexing compiled to.
        let (head, tail) = sections.split_at(seed_index);
        for &section in tail.iter().chain(head.iter()) {
            if section.shared {
                reseed_next = true;
                saw_reseed = true;
                continue;
            }
            if section.starts_after_reseed || reseed_next {
                let Some(reseeded) = strict_section_membership(pool, other, section, testers)
                else {
                    deferred = true;
                    break;
                };
                inside = reseeded;
                saw_reseed = true;
                reseed_next = false;
            }
            if !computer.boundary_cells_saturated(operand) {
                add_boundary_section_cells(operand, inside, computer);
                if computer.decided() {
                    early_decided = true;
                    break;
                }
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
        if deferred || early_decided {
            break;
        }
        if !saw_reseed && inside != start_inside {
            deferred = true;
            break;
        }
    }
    sections.clear();
    SECTIONS_SCRATCH.set(sections);
    !deferred
}

fn ring_sections(
    ring: &OrientedRing,
    pool: &OperandPool,
    contacts: &mut BoundaryContacts,
    sections: &mut Vec<BoundarySection>,
) {
    sections.clear();
    let n = ring.points.len();
    let base = ring.segments.start;
    let mut local = 0;
    while local < n {
        let segment_index = base + local;
        let segment = pool.segments[segment_index];
        sort_dedup_cuts(segment, &mut contacts.cuts[segment_index]);
        if contacts.cuts[segment_index].is_empty() && contacts.shared[segment_index].is_empty() {
            // A maximal run of segments with no cuts and no shared overlap is
            // ONE uniform-membership arc: nothing along it flips inside/outside
            // and it contributes a single boundary cell, so emitting one
            // section instead of one per segment is the dominant win on simple
            // overlaps (a thousand interior arc segments collapse to a handful).
            // The seed probe is the first segment's midpoint — a real boundary
            // point — since the whole-arc chord would cut through the interior.
            let probe = segment_midpoint(segment.start, segment.end);
            let mut to = segment.end;
            local += 1;
            while local < n {
                let next = base + local;
                if !contacts.cuts[next].is_empty() || !contacts.shared[next].is_empty() {
                    break;
                }
                to = pool.segments[next].end;
                local += 1;
            }
            sections.push(BoundarySection {
                from: segment.start,
                to,
                probe,
                starts_after_reseed: false,
                shared: false,
                end: SectionEnd::None,
            });
        } else {
            emit_segment_sections(segment, segment_index, contacts, sections);
            local += 1;
        }
    }
}

/// Emit the boundary sections of ONE segment that carries cuts or a shared
/// overlap (cuts are already sorted/deduped). Each section stays within the
/// segment, so its chord midpoint is a valid on-boundary probe.
fn emit_segment_sections(
    segment: Segment,
    segment_index: usize,
    contacts: &BoundaryContacts,
    sections: &mut Vec<BoundarySection>,
) {
    let cuts = &contacts.cuts[segment_index];
    let mut from = segment.start;
    let mut starts_after_reseed = cuts
        .iter()
        .any(|cut| !cut.cross && same_point(cut.point, segment.start));
    for cut in cuts.iter().copied() {
        if !same_point(from, cut.point) {
            let shared = is_shared_section(
                PointKey::new(from),
                cut.key,
                &contacts.shared[segment_index],
            );
            sections.push(BoundarySection {
                from,
                to: cut.point,
                probe: segment_midpoint(from, cut.point),
                starts_after_reseed,
                shared,
                end: section_end(cut.cross, shared),
            });
            starts_after_reseed = !cut.cross;
            from = cut.point;
        }
    }
    if !same_point(from, segment.end) {
        let shared = is_shared_section(
            PointKey::new(from),
            PointKey::new(segment.end),
            &contacts.shared[segment_index],
        );
        sections.push(BoundarySection {
            from,
            to: segment.end,
            probe: segment_midpoint(from, segment.end),
            starts_after_reseed,
            shared,
            end: SectionEnd::None,
        });
    }
}

fn is_shared_section(start: PointKey, end: PointKey, runs: &[SharedRun]) -> bool {
    runs.iter()
        .any(|run| (run.start == start && run.end == end) || (run.start == end && run.end == start))
}

const fn add_boundary_section_cells(
    operand: Operand,
    inside_other: bool,
    computer: &mut TopologyComputer<'_>,
) {
    match (operand, inside_other) {
        (Operand::Left, true) => {
            computer.add_cell(1, 0, 1);
            computer.add_cell(0, 0, 2);
            computer.add_cell(2, 0, 2);
        },
        (Operand::Left, false) => {
            computer.add_cell(1, 2, 1);
            computer.add_cell(0, 2, 2);
        },
        (Operand::Right, true) => {
            computer.add_cell(0, 1, 1);
            computer.add_cell(0, 0, 2);
            computer.add_cell(0, 2, 2);
        },
        (Operand::Right, false) => {
            computer.add_cell(2, 1, 1);
            computer.add_cell(2, 0, 2);
        },
    }
}

const fn segment_midpoint(start: XY, end: XY) -> XY {
    crate::geometry::segment_midpoint_xy(start, end)
}

fn strict_section_membership(
    pool: &OperandPool,
    operand: Operand,
    section: BoundarySection,
    testers: AreaTesters<'_>,
) -> Option<bool> {
    let midpoint = section.probe;
    if same_point(midpoint, section.from) || same_point(midpoint, section.to) {
        return None;
    }
    // Prepared `PointBatchTester`: one hierarchical 3-way classification
    // fuses the boundary-defer check and the interior test. Boundary →
    // defer (None), exactly as the raw `operand_covers_boundary` +
    // `other_contains` pair.
    if let Some(tester) = testers.for_operand(operand) {
        return match tester.classify_area_point(midpoint.point()) {
            Some(RingClass::Interior) => Some(true),
            Some(RingClass::Exterior) => Some(false),
            Some(RingClass::Boundary) => None,
            None => Some(other_contains(pool, operand, midpoint)),
        };
    }
    if operand_covers_boundary(pool, operand, midpoint) {
        return None;
    }
    Some(other_contains(pool, operand, midpoint))
}

pub(crate) const fn opposite(operand: Operand) -> Operand {
    match operand {
        Operand::Left => Operand::Right,
        Operand::Right => Operand::Left,
    }
}
