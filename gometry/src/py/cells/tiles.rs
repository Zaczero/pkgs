//! XYZ tiles: the Tile class and the top-level `tile_*` functions.

use pyo3::prelude::*;
use pyo3::types::PyModule;

mod cell;
mod coverage;
mod functions;

use cell::_unpickle_tile;
pub(crate) use cell::{PyTile, parse_tile_zoom, tile_arg};
use coverage::tile_cover;
use functions::{tile_bounding_cell, tile_cells, tile_difference, tile_intersection, tile_union};

pub(super) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    crate::add_functions!(m;
        tile_cells, tile_cover,
        tile_bounding_cell, tile_union, tile_intersection, tile_difference,
        _unpickle_tile,
    );
    crate::add_classes!(m; PyTile);
    Ok(())
}
