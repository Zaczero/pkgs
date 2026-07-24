#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! Geohash cells: the GeohashCell class and the top-level `geohash_*`
//! functions (`geohash_cover`/`geohash_cells`/`geohash_bounding_cell` + set algebra).

use pyo3::prelude::*;
use pyo3::types::PyModule;

mod cell;
mod coverage;
mod functions;

use cell::*;
pub(crate) use cell::{PyGeohashCell, geohash_cell_arg, parse_geohash_precision};
use coverage::*;
use functions::*;

pub(super) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    crate::add_functions!(m;
        geohash_cells, geohash_cover, geohash_bounding_cell,
        geohash_union, geohash_intersection, geohash_difference,
        _unpickle_geohash_cell, _unpickle_geohash_coverage,
    );
    crate::add_classes!(m; PyGeohashCell, PyGeohashCoverage, PyGeohashCoverageIter);
    Ok(())
}
