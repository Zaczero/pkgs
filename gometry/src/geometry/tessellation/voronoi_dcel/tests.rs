use super::*;
use crate::geometry::for_each_indexed_overlapping_bounds_pair;
use crate::geometry::overlay::shoot_left;
use crate::geometry::segment_index::checked_index_count;
use crate::geometry::tessellation::shape::canonical_voronoi_sites;

include!("tests/support.rs");
include!("tests/admission.rs");
include!("tests/assembly.rs");
include!("tests/assembly/contacts.rs");
include!("tests/assembly/owners.rs");
include!("tests/collinear.rs");
include!("tests/dual.rs");
include!("tests/embedding.rs");
include!("tests/embedding/repair.rs");
include!("tests/embedding/partition.rs");
include!("tests/receipt.rs");
include!("tests/receipt/producers.rs");
