//! Admission and completion checks for an already-known embedded graph.
//!
//! Producers own graph selection and stable source identities. This module
//! owns the shared proof that a finite coordinate assignment preserves the
//! admitted graph, its embedding, and the identities of distinct vertices.

pub(in crate::geometry) mod admission;
pub(in crate::geometry) mod areal;
pub(in crate::geometry) mod binding;
mod contacts;
pub(in crate::geometry) mod layout;
pub(in crate::geometry) mod literals;
pub(in crate::geometry) mod points;
pub(in crate::geometry) mod storage;
#[cfg(test)]
mod tests;
