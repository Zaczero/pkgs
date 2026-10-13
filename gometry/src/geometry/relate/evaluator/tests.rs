use super::atoms::{build_atom_coverage, order_event_cuts};
use super::contacts::stored_exact_point;
#[cfg(debug_assertions)]
use super::model::{ATOM_METRICS, AtomMetrics};
use super::rectangle::rectangle_relation;
use super::sides::{atom_owner_label, segment_direction};
use super::staging::stage_components;
use super::*;
use crate::geometry::ExpansionBudget;
use crate::geometry::overlay::ExactSourceInventory;

include!("tests/support.rs");
include!("tests/rectangle.rs");
include!("tests/points.rs");
include!("tests/queries.rs");
include!("tests/contacts_atoms.rs");
