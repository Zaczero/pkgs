#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use crate::geometry::overlay::OverlayOp;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ArcAction {
    Drop,
    KeepForward,
    KeepReversed,
}

pub(crate) const fn arc_rule(op: OverlayOp, is_a: bool, inside_other: bool) -> ArcAction {
    match op {
        // Result keeps each ring's arcs OUTSIDE the other, forward.
        OverlayOp::Union => {
            if inside_other {
                ArcAction::Drop
            } else {
                ArcAction::KeepForward
            }
        },
        // a − b: A's arcs outside B (forward) bound the kept part; B's arcs
        // inside A bound the removed bite and are traversed REVERSED.
        OverlayOp::Difference => {
            if is_a {
                if inside_other {
                    ArcAction::Drop
                } else {
                    ArcAction::KeepForward
                }
            } else if inside_other {
                ArcAction::KeepReversed
            } else {
                ArcAction::Drop
            }
        },
        // a ^ b = (a−b) ∪ (b−a): every arc is kept; the ones inside the other
        // operand bound a removed region and reverse.
        OverlayOp::SymmetricDifference => {
            if inside_other {
                ArcAction::KeepReversed
            } else {
                ArcAction::KeepForward
            }
        },
        // Intersection keeps each ring's arcs INSIDE the other, forward.
        OverlayOp::Intersection => {
            if inside_other {
                ArcAction::KeepForward
            } else {
                ArcAction::Drop
            }
        },
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SharedAction {
    KeepForward,
    Drop,
    BailOp,
}

/// Exact shared-boundary cancellation rule. `is_a` is the logical operand role;
/// same-oriented survivors are owned by A so reassembly sees only one outgoing
/// arc for that run.
pub(crate) const fn shared_arc_rule(
    op: OverlayOp,
    is_a: bool,
    same_direction: bool,
) -> SharedAction {
    use SharedAction::{BailOp, Drop, KeepForward};
    match op {
        OverlayOp::Union => {
            if same_direction && is_a {
                KeepForward
            } else {
                Drop
            }
        },
        OverlayOp::Intersection => {
            if same_direction {
                if is_a { KeepForward } else { Drop }
            } else {
                BailOp
            }
        },
        OverlayOp::Difference => {
            if !same_direction && is_a {
                KeepForward
            } else {
                Drop
            }
        },
        OverlayOp::SymmetricDifference => Drop,
    }
}
