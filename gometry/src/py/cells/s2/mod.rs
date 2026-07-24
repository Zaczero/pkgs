#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! The S2 cell system: typed cells, coverages, set algebra, and flat `s2_*`
//! functions — backed by gometry's own S2 core (`crate::s2`), with coverings
//! classified exactly against the source geometry.

use super::*;
use crate::grid::s2::cell_set;

mod cell;
mod coverage;
mod functions;
mod grid_cell;
mod parse;
mod register;
mod types;

pub(crate) use cell::{s2_cell_from_xy, s2_cell_id, *};
pub(super) use functions::*;
pub(crate) use parse::parse_s2_level;
pub(super) use parse::*;
pub(crate) use register::{build_coverage, register as register_s2};
pub(super) use types::*;
