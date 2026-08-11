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

use crate::collections::{HashMap, HashSet};
use crate::geometry::overlay::{OverlayOp, polygon_parts_to_shape};
use crate::geometry::segment_index::{RUN_NODING_MIN, for_each_candidate_pair};
pub(in crate::geometry) use crate::geometry::topology::{
    self, Cut, Operand, OperandPool, OrientedRing, SectionEnd, add_cut, compare_along_segment,
    operand_covers_boundary, other_contains, section_end, sort_dedup_cuts,
};

mod contacts;
mod overlay;
mod reassemble;
mod rules;
mod sections;
mod symdiff;

pub(in crate::geometry) use contacts::{BoundaryContacts, SharedArc, collect_boundary_contacts};
pub(in crate::geometry) use overlay::{
    build_transverse_sections, clean_overlay, difference_chain_rings, seed_membership,
    with_section_scratch,
};
pub(in crate::geometry) use reassemble::{assemble_rings, reassemble, reassemble_to_rings};
pub(in crate::geometry) use rules::{ArcAction, SharedAction, arc_rule, shared_arc_rule};
pub(in crate::geometry) use sections::{
    ArcSection, SharedSection, keep_arcs, shared_section, strict_section_membership,
};
pub(in crate::geometry) use symdiff::symmetric_difference_shape;

#[cfg(test)]
mod tests;
