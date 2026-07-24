use std::ops::ControlFlow;

use super::*;

#[derive(Clone, Copy)]
pub(crate) struct SharedRun {
    pub(crate) start: PointKey,
    pub(crate) end: PointKey,
}

pub(crate) struct TouchNode {
    point: XY,
    segments: Vec<usize>,
}

pub(crate) struct BoundaryContacts {
    pub(crate) cuts: Vec<Vec<Cut>>,
    pub(crate) shared: Vec<Vec<SharedRun>>,
    shared_nodes: HashSet<PointKey>,
    touch_nodes: HashMap<PointKey, TouchNode>,
    shared_run: bool,
    same_direction_shared: bool,
}

thread_local! {
    // The per-segment cut + shared-run indices are the same ~N shape every
    // relate; reuse them per-thread so they are allocated ONCE, not once per call
    // (mirrors `clean_union`'s `CUTS_SCRATCH`). Returned on drop.
    static RELATE_CUTS_SCRATCH: std::cell::RefCell<Vec<Vec<Cut>>> =
        const { std::cell::RefCell::new(Vec::new()) };
    static RELATE_SHARED_SCRATCH: std::cell::RefCell<Vec<Vec<SharedRun>>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

impl BoundaryContacts {
    pub(crate) fn new(len: usize) -> Self {
        let mut cuts =
            RELATE_CUTS_SCRATCH.with(|scratch| std::mem::take(&mut *scratch.borrow_mut()));
        cuts.iter_mut().for_each(Vec::clear);
        cuts.resize_with(len, Vec::new);
        let mut shared =
            RELATE_SHARED_SCRATCH.with(|scratch| std::mem::take(&mut *scratch.borrow_mut()));
        shared.iter_mut().for_each(Vec::clear);
        shared.resize_with(len, Vec::new);
        Self {
            cuts,
            shared,
            shared_nodes: HashSet::new(),
            touch_nodes: HashMap::new(),
            shared_run: false,
            same_direction_shared: false,
        }
    }
}

impl Drop for BoundaryContacts {
    fn drop(&mut self) {
        RELATE_CUTS_SCRATCH.with(|scratch| *scratch.borrow_mut() = std::mem::take(&mut self.cuts));
        RELATE_SHARED_SCRATCH
            .with(|scratch| *scratch.borrow_mut() = std::mem::take(&mut self.shared));
    }
}

pub(crate) fn scan_boundary_contacts(
    pool: &OperandPool,
    contacts: &mut BoundaryContacts,
    computer: &mut TopologyComputer<'_>,
    both_simple: bool,
) -> bool {
    let mut invalid = false;
    let flow = for_each_candidate_pair::<RUN_NODING_MIN>(
        &pool.segments,
        |index| pool.ring_of[index],
        |left_index, right_index| {
            let left_segment = pool.segments[left_index];
            let right_segment = pool.segments[right_index];
            if pool.operands[left_index] == pool.operands[right_index] {
                // The same-operand visit exists only to reject a self-
                // intersecting operand (a degenerate-collinear or crossing
                // pair → bail to the arrangement oracle). When both operands
                // are known-valid (cached `is_simple`), that can never fire, so
                // skip the contact kernel entirely for every within-operand
                // candidate pair — the broad-phase win on multipolygon inputs.
                if both_simple {
                    return ControlFlow::Continue(());
                }
                invalid = same_operand_invalid(left_segment, right_segment);
                return if invalid {
                    ControlFlow::Break(())
                } else {
                    ControlFlow::Continue(())
                };
            }
            if !record_cross_operand_contact(
                (left_index, left_segment),
                (right_index, right_segment),
                contacts,
                computer,
            ) {
                invalid = true;
                return ControlFlow::Break(());
            }
            if computer.decided() {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        },
    );
    if invalid {
        return false;
    }
    // A break with `invalid == false` is the `computer.decided()` early-out: a
    // Pattern goal whose verdict is irrevocably FALSE (cells are monotonic and
    // `decided` only ever records `false`). That verdict holds regardless of the
    // not-yet-scanned contacts, so return `true` and let the caller surface
    // `computer.finish()` instead of conservatively deferring to the oracle.
    if flow.is_break() {
        return true;
    }
    if contacts.same_direction_shared {
        return false;
    }
    if contacts.shared_run && pool_has_large_coordinates(pool) {
        return false;
    }
    touch_nodes_gradable(pool, contacts)
}

fn same_operand_invalid(left: Segment, right: Segment) -> bool {
    match segment_contact_exact(left, right) {
        Contact::Disjoint | Contact::Touch { .. } => false,
        Contact::Cross { .. } => true,
        Contact::Collinear { span } => PointKey::new(span.start) != PointKey::new(span.end),
    }
}

fn pool_has_large_coordinates(pool: &OperandPool) -> bool {
    pool.segments.iter().any(|segment| {
        [
            segment.start.x,
            segment.start.y,
            segment.end.x,
            segment.end.y,
        ]
        .into_iter()
        .any(|value| value.abs() >= 1.0e12)
    })
}

fn record_cross_operand_contact(
    left: (usize, Segment),
    right: (usize, Segment),
    contacts: &mut BoundaryContacts,
    computer: &mut TopologyComputer<'_>,
) -> bool {
    let ((left_index, left), (right_index, right)) = (left, right);
    match segment_contact_with_orientations(left, right).0 {
        Contact::Disjoint => true,
        Contact::Cross { point } => {
            add_cut(&mut contacts.cuts[left_index], point, true);
            add_cut(&mut contacts.cuts[right_index], point, true);
            computer.add_cell(1, 1, 0);
            true
        },
        Contact::Touch { point } => {
            add_cut(&mut contacts.cuts[left_index], point, false);
            add_cut(&mut contacts.cuts[right_index], point, false);
            record_touch_node(contacts, point, left_index, right_index);
            computer.add_cell(1, 1, 0);
            true
        },
        Contact::Collinear { span } => {
            record_shared_run(left_index, right_index, span, contacts, computer);
            true
        },
    }
}

fn record_shared_run(
    left_index: usize,
    right_index: usize,
    span: SharedSpan,
    contacts: &mut BoundaryContacts,
    computer: &mut TopologyComputer<'_>,
) {
    let start_key = PointKey::new(span.start);
    let end_key = PointKey::new(span.end);
    add_cut(&mut contacts.cuts[left_index], span.start, false);
    add_cut(&mut contacts.cuts[left_index], span.end, false);
    add_cut(&mut contacts.cuts[right_index], span.start, false);
    add_cut(&mut contacts.cuts[right_index], span.end, false);
    if start_key == end_key {
        computer.add_cell(1, 1, 0);
        return;
    }
    contacts.shared_nodes.insert(start_key);
    contacts.shared_nodes.insert(end_key);
    contacts.shared_run = true;
    contacts.same_direction_shared |= span.same_direction;
    contacts.shared[left_index].push(SharedRun {
        start: start_key,
        end: end_key,
    });
    contacts.shared[right_index].push(SharedRun {
        start: start_key,
        end: end_key,
    });
    computer.add_cell(1, 1, 1);
    if span.same_direction {
        computer.add_cell(0, 0, 2);
    } else {
        computer.add_cell(0, 2, 2);
        computer.add_cell(2, 0, 2);
    }
}

fn record_touch_node(contacts: &mut BoundaryContacts, point: XY, left: usize, right: usize) {
    let node = contacts
        .touch_nodes
        .entry(PointKey::new(point))
        .or_insert_with(|| TouchNode {
            point,
            segments: Vec::new(),
        });
    if !node.segments.contains(&left) {
        node.segments.push(left);
    }
    if !node.segments.contains(&right) {
        node.segments.push(right);
    }
}

fn touch_nodes_gradable(pool: &OperandPool, contacts: &BoundaryContacts) -> bool {
    contacts
        .touch_nodes
        .iter()
        .all(|(key, node)| contacts.shared_nodes.contains(key) || touch_node_gradable(pool, node))
}

fn touch_node_gradable(pool: &OperandPool, node: &TouchNode) -> bool {
    let mut left = NodeIncidence::default();
    let mut right = NodeIncidence::default();
    for &index in &node.segments {
        let incidence = if pool.operands[index] == Operand::Left {
            &mut left
        } else {
            &mut right
        };
        incidence.add(pool.segments[index], node.point);
    }
    let through_count = left.through.len() + right.through.len();
    if through_count == 0 {
        return true;
    }
    if through_count != 1 {
        return false;
    }
    let (through, other) = if let Some(segment) = left.through.first().copied() {
        (segment, &right)
    } else {
        (*right.through.first().expect("one through segment"), &left)
    };
    if other.endpoints.len() < 2 {
        return false;
    }
    let mut clockwise = false;
    let mut counter = false;
    for endpoint in &other.endpoints {
        match orientation(through.start, through.end, *endpoint) {
            Orientation::Clockwise => clockwise = true,
            Orientation::CounterClockwise => counter = true,
            Orientation::Collinear => return false,
        }
    }
    clockwise || counter
}
