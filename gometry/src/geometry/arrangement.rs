//! CSR topology shared by the exact Cartesian partition and geographic
//! quotient tests. The winding-based [`Arrangement`] implementation exists
//! only for the geographic test oracle.

#[cfg(test)]
mod build;
mod indexed;
#[cfg(test)]
mod loop_ops;
mod walk;
#[cfg(test)]
mod winding;

pub(crate) use indexed::{
    IndexedTopology, IndexedTopologyScratch, indexed_edge_slots_with_work,
    indexed_topology_from_edge_fn_with_work, indexed_topology_from_edges_with_work,
};
#[cfg(test)]
pub(crate) use loop_ops::{order_single_loop_rows, positional_loop_cuts, positional_loop_pieces};
pub(crate) use walk::compare_departures;
#[cfg(test)]
pub(crate) use walk::{build_csr, dedup_vertices_and_edges, sort_rows_counterclockwise};
#[cfg(test)]
pub(crate) use winding::{
    Arrangement, ArrangementSpares, Face, TypedSpares, WindingWeight, arrangement_spares,
    typed_spares,
};
