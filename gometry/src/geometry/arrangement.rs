//! CSR topology shared by the exact Cartesian partition and geographic
//! quotient topology.

mod indexed;
mod walk;

pub(crate) use indexed::{
    IndexedTopology, IndexedTopologyScratch, indexed_edge_slots_with_work,
    indexed_topology_from_edges_with_work,
};
pub(crate) use walk::compare_departures;
