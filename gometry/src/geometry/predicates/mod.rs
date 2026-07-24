//! Topological predicates and validity on `Shape`: `is_empty`/`is_closed`/
//! `is_ring`/`is_simple`, antimeridian, min-clearance, validate/repair, and the
//! binary relations (contains/within/covers/intersects/touches/…/equals).

mod convex;
mod membership;
mod pole;
mod properties;
mod relate;
mod repair;
mod shape;
mod shape_data;
mod simplicity;
mod validate;
mod validity_helpers;

pub(crate) use convex::*;
pub(crate) use membership::*;
pub(crate) use pole::*;
pub(crate) use properties::shape_spans_full_longitude;
pub(crate) use relate::*;
pub(crate) use repair::*;
pub(crate) use shape_data::*;
pub(crate) use simplicity::*;
pub(crate) use validate::*;
pub(crate) use validity_helpers::*;

#[cfg(test)]
mod tests;
