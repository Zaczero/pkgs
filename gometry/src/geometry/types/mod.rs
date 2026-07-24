//! Geometry data model: every type and its inherent/core-trait impls.
//!
//! The `Shape` enum, coordinate storage (`CoordSeq`, `Point`, `Ring`,
//! `Polygon`, `Bounds`, `Segment`), the `Coordinates` and `GeodesicMetric`
//! traits, the option/kind enums, and `GeometryErrorKind`. Behavior lives in the
//! sibling concern modules; the shared geometry kernel stays in the module
//! root.

mod coordseq;
mod csr;
mod error;
mod point;
mod primitives;
mod ring;
mod shape;
mod shape_data;

pub(crate) use coordseq::*;
pub(crate) use csr::*;
pub(crate) use error::*;
pub(crate) use primitives::*;
pub(crate) use ring::*;
pub(crate) use shape::*;
pub(crate) use shape_data::*;

#[cfg(test)]
mod tests;
