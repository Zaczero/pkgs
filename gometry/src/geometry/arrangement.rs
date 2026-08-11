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

pub(crate) use loop_ops::{
    order_single_loop_rows, positional_loop_cuts, positional_loop_pieces, single_loop_cuts,
    single_loop_pieces,
};
pub(crate) use walk::{
    build_csr, dedup_vertices_and_edges, departure_angle, departure_scales,
    sort_rows_counterclockwise, split_ring_at_pinches, walk_faces,
};
pub(crate) use winding::{
    Arrangement, Columns, Face, FinishSpares, RegionSpares, TypedSpares, WindingWeight,
    arrangement_spares_with, region_spares_with, restore_typed_spares, take_typed_spares,
};

#[cfg(test)]
mod tests;
