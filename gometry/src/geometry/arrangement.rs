//! CSR topology shared by the exact Cartesian partition and geographic
//! quotient topology.

mod indexed;
mod walk;

pub(crate) use indexed::{IndexedTopology, IndexedTopologyScratch};
pub(crate) use walk::compare_departures;
