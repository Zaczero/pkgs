//! The S2 cell system: typed cells, coverages, set algebra, and flat `s2_*`
//! functions — backed by gometry's own S2 core (`crate::s2`), with coverings
//! classified exactly against the source geometry.

use crate::grid::s2::cell_set;
mod cell;
mod coverage;
mod functions;
mod grid_cell;
mod parse;
mod register;
mod types;

pub(crate) use cell::{s2_cell_from_xy, s2_cell_id, *};
pub(super) use functions::{s2_dissolve, s2_dissolve_sorted};
pub(super) use parse::parse_s2_min_level_value;
pub(crate) use parse::{parse_s2_level, parse_s2_level_budget, parse_s2_level_value};
pub(crate) use register::{build_coverage, register as register_s2};
pub(crate) use types::{PyS2Cell, PyS2Coverage};
use types::{PyS2CoverageIter, S2Membership};
