#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
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

use compact::*;
use dissolve::*;
use functions::*;
pub(super) use functions::{h3_cell_from_xy, h3_cell_index};
pub(super) use index::{H3Index, validate_h3_index_id, validate_h3_index_ids};
use index::{collect_h3_index_ids, parse_h3_index};
use register::*;
pub(super) use register::{parse_h3_resolution, register};
use set_algebra::*;
use tile::*;
pub(super) use types::*;
use vertex_edge::*;
use vertex_edge_array::*;

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
