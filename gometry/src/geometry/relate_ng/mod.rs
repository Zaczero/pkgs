#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! RelateNG-style pure-areal DE-9IM without building the full arrangement.
//!
//! This scans exact boundary contacts, splits directed ring edges at every
//! cross-operand touch/crossing, then classifies open boundary sections by one
//! point-in-area probe. Cases outside that model return `None` and keep the
//! arrangement oracle as the exact fallback.

pub(in crate::geometry) use super::topology::{
    Cut, Operand, OperandPool, OrientedRing, SectionEnd, StagedRings, add_cut,
    operand_covers_boundary, other_contains, section_end, sort_dedup_cuts,
};
use super::{topology, *};
pub(in crate::geometry) use crate::collections::{HashMap, HashSet};

mod computer;
mod contacts;
mod probe;
mod sections;

pub(crate) use computer::CompiledPattern;
pub(in crate::geometry) use computer::{
    AreaTesters, RelateDecision, RelateGoal, TopologyComputer, areal_relate_ng,
    areal_relate_ng_staged,
};
pub(in crate::geometry) use contacts::*;
pub(in crate::geometry) use probe::*;
pub(in crate::geometry) use sections::*;

#[cfg(test)]
mod tests;
