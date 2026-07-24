//! Derived single-geometry outputs from `Shape`: centroid, representative
//! point on surface, envelope, convex/concave hull, and pole of
//! inaccessibility.

mod centroid;
mod concave;
mod hull;
mod polylabel;
mod shape;
mod surface;

pub(crate) use centroid::*;
pub(crate) use concave::*;
pub(crate) use hull::*;
pub(crate) use polylabel::*;
pub(crate) use surface::*;

#[cfg(test)]
mod tests;
