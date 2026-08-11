//! The H3 cell system: typed cells/vertices, coverages, and flat `h3_*` functions.

use h3o::{CellIndex, DirectedEdgeIndex, LatLng, LocalIJ, Resolution, VertexIndex};
use pyo3::PyResult;

use crate::Typed;
mod cell;
mod compact;
mod coverage;
mod dissolve;
mod functions;
mod grid_cell;
mod index;
mod register;
mod set_algebra;
mod tile;
mod types;
mod vertex_edge;
mod vertex_edge_array;

use compact::{h3_compact_with_floor, h3_floor, parse_h3_grid_k};
use dissolve::{h3_dissolve, h3_dissolve_sorted};
use functions::{ensure_h3_uncompact_budget, h3_bounding_cell, h3_cells, h3_cover};
pub(super) use functions::{h3_cell_from_xy, h3_cell_index};
pub(super) use index::{H3Index, validate_h3_index_id, validate_h3_index_ids};
use index::{collect_h3_index_ids, parse_h3_index};
use register::{build_coverage, h3_resolution, h3_resolution_from_i64};
pub(super) use register::{parse_h3_resolution, register};
use set_algebra::{
    _unpickle_h3_cell, _unpickle_h3_coverage, h3_base_cells, h3_difference, h3_intersection,
    h3_pentagons, h3_union,
};
use tile::{h3_cell_shape, h3_tile};
use types::{
    H3Membership, PyH3CoverageIter, PyH3Edge, PyH3Vertex, TiledCell, h3_cell_array, h3_cell_vec,
    h3_latlng, py_h3_cell_array,
};
pub(crate) use types::{PyH3Cell, PyH3Coverage};
use vertex_edge::{_unpickle_h3_edge, _unpickle_h3_vertex, h3_edge_index, h3_vertex_index};
use vertex_edge_array::{
    _unpickle_h3_edge_array, _unpickle_h3_vertex_array, PyH3EdgeArrayIter, PyH3VertexArrayIter,
};
pub(crate) use vertex_edge_array::{PyH3EdgeArray, PyH3VertexArray};

pub(super) fn resolution_from_depth(depth: u8) -> PyResult<Resolution> {
    h3_resolution(depth)
}

pub(super) fn compact_cells(
    cells: Vec<CellIndex>,
    min_resolution: Resolution,
) -> PyResult<Vec<CellIndex>> {
    h3_compact_with_floor(cells, min_resolution)
}

pub(super) fn uncompact_cells(
    cells: Vec<CellIndex>,
    resolution: Resolution,
) -> PyResult<Vec<CellIndex>> {
    let estimated = ensure_h3_uncompact_budget(cells.iter().copied(), resolution)?;
    let mut expanded = Vec::with_capacity(estimated);
    expanded.extend(CellIndex::uncompact(cells, resolution));
    Ok(expanded)
}

pub(super) fn dissolve_cells(cells: Vec<CellIndex>) -> PyResult<Typed> {
    h3_dissolve(cells)
}
