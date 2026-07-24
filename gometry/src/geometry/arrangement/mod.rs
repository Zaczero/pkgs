//! Columnar half-edge arrangement — the shared planar-subdivision core.
//!
//! Built from NODED directed segments (segments only touch at endpoints):
//! vertices dedup to `u32` ids, adjacency is one CSR sorted CCW by
//! departure angle, and a single global walk assigns every directed
//! half-edge its face. Faces keep their ring, signed doubled area, and the
//! component they belong to; directed multiplicities (how many source
//! segments cover each half-edge, net of direction) drive an exact
//! winding-number fill: one seed probe per CONNECTED COMPONENT, then BFS
//! across twin half-edges (`w(right) = w(left) − multiplicity(a→b)`).
//!
//! Consumers classify faces by winding and extract the kept region's
//! boundary — every cost is a flat array pass; the only hash map is the
//! vertex dedup at construction.

mod build;
mod loop_ops;
mod walk;
mod winding;

pub(crate) use loop_ops::*;
pub(crate) use walk::*;
pub(crate) use winding::*;

#[cfg(test)]
mod tests;
