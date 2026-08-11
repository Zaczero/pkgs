//! RelateNG-style pure-areal DE-9IM without building the full arrangement.
//!
//! This scans exact boundary contacts, splits directed ring edges at every
//! cross-operand touch/crossing, then classifies open boundary sections by one
//! point-in-area probe. Cases outside that model return `None` and keep the
//! arrangement oracle as the exact fallback.

pub(in crate::geometry) use crate::collections::{HashMap, HashSet};
pub(in crate::geometry) use crate::geometry::topology::{
    Cut, Operand, OperandPool, OrientedRing, SectionEnd, StagedRings, add_cut,
    operand_covers_boundary, other_contains, section_end, sort_dedup_cuts,
};
use crate::geometry::{
    Contact, Loc, Orientation, PointBatchTester, PointKey, Polygon, RUN_NODING_MIN, RingClass,
    Segment, SharedSpan, XY, for_each_candidate_pair, orientation, same_point,
    segment_contact_exact, segment_contact_with_orientations, topology, wrap_index,
};

mod computer;
mod contacts;
mod probe;
mod sections;

pub(crate) use computer::CompiledPattern;
pub(in crate::geometry) use computer::{
    AreaTesters, RelateDecision, RelateGoal, TopologyComputer, areal_relate_ng,
    areal_relate_ng_staged,
};
pub(in crate::geometry) use contacts::{BoundaryContacts, SharedRun, scan_boundary_contacts};
#[cfg(test)]
pub(in crate::geometry) use probe::polygon_interior_probe;
pub(in crate::geometry) use probe::probe_interior_faces;
pub(in crate::geometry) use sections::{NodeIncidence, classify_boundary_sections};

#[cfg(test)]
mod tests;
