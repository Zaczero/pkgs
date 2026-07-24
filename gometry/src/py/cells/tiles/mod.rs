#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! XYZ tiles: the Tile class and the top-level `tile_*` functions.

use pyo3::prelude::*;
use pyo3::types::PyModule;

mod cell;
mod coverage;
mod functions;

use cell::*;
pub(crate) use cell::{PyTile, parse_tile_zoom, tile_arg};
use coverage::*;
use functions::*;

pub(super) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    crate::add_functions!(m;
        tile_cells, tile_cover,
        tile_bounding_cell, tile_union, tile_intersection, tile_difference,
        _unpickle_tile, _unpickle_tile_coverage,
    );
    crate::add_classes!(m; PyTile, PyTileCoverage, PyTileCoverageIter);
    Ok(())
}
