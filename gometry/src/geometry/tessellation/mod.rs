#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! Tessellation outputs from `Shape`: Delaunay/constrained triangulation,
//! ear-cut polygon triangles, Voronoi polygons/edges, minimum rotated
//! rectangle, boundary, and linework polygonization.

use spade::handles::{FixedFaceHandle, FixedVertexHandle, InnerTag};
use spade::{
    AngleLimit, ConstrainedDelaunayTriangulation, DelaunayTriangulation, InsertionError, Point2,
    RefinementParameters, Triangulation,
};

use super::*;
pub(crate) use crate::error::Error;

/// snap radius matching geo's `SpadeTriangulationConfig::default()` (0.0001):
/// coordinates within this distance collapse to one constraint vertex, which
/// keeps spade from panicking on near-coincident input.
pub(crate) const CONSTRAINT_SNAP_RADIUS: f64 = 0.000_1;

mod boundary;
mod delaunay;
mod earcut;
mod polygonize;
mod sampling;
mod shape;
mod voronoi;
mod voronoi_native;

pub(crate) use boundary::*;
pub(crate) use delaunay::*;
pub(crate) use earcut::*;
pub(crate) use polygonize::*;
pub(crate) use sampling::*;
pub(crate) use voronoi::*;
pub(crate) use voronoi_native::*;

#[cfg(test)]
mod tests;
