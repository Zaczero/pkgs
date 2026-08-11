//! Geohash cells: the GeohashCell class and the top-level `geohash_*`
//! functions (`geohash_cover`/`geohash_cells`/`geohash_bounding_cell` + set algebra).

use pyo3::prelude::*;
use pyo3::types::PyModule;

mod cell;
mod coverage;
mod functions;

use cell::_unpickle_geohash_cell;
pub(crate) use cell::{PyGeohashCell, geohash_cell_arg, parse_geohash_precision};
pub(crate) use coverage::PyGeohashCoverage;
use coverage::{_unpickle_geohash_coverage, PyGeohashCoverageIter, geohash_cover};
use functions::{
    geohash_bounding_cell, geohash_cells, geohash_difference, geohash_intersection, geohash_union,
};

pub(super) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    crate::add_functions!(m;
        geohash_cells, geohash_cover, geohash_bounding_cell,
        geohash_union, geohash_intersection, geohash_difference,
        _unpickle_geohash_cell, _unpickle_geohash_coverage,
    );
    crate::add_classes!(m; PyGeohashCell, PyGeohashCoverage, PyGeohashCoverageIter);
    Ok(())
}
