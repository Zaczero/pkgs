//! PROJ FFI introspection — raw-pointer metadata readers that copy PROJ
//! object descriptions (authority, datum, ellipsoid, prime meridian, area of
//! use, axes, methods, parameters, grids, operation steps) into the owned
//! `types.rs` DTOs. Reached via `use super::*`.

mod metadata;
mod objects;
mod operations;

pub(super) use metadata::*;
pub(crate) use objects::Confidence;
pub(super) use objects::*;
pub(super) use operations::*;
