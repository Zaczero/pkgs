//! The H3 cell system: typed cells/vertices, coverages, and flat `h3_*` functions.

use h3o::{CellIndex, DirectedEdgeIndex, LatLng, LocalIJ, Resolution, VertexIndex};

mod cell;
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

pub(super) use dissolve::h3_dissolve;
use functions::{h3_bounding_cell, h3_cells, h3_cover, parse_h3_grid_k};
pub(super) use functions::{h3_cell_from_xy, h3_cell_index};
pub(super) use index::{H3Index, validate_h3_index_id, validate_h3_index_ids};
use index::{collect_h3_index_ids, parse_h3_index};
use register::h3_resolution;
pub(super) use register::{parse_h3_resolution, register};
use set_algebra::{
    _unpickle_h3_cell_v1, h3_base_cells, h3_difference, h3_intersection, h3_pentagons, h3_union,
};
use tile::h3_tile;
pub(crate) use types::PyH3Cell;
use types::{PyH3Edge, PyH3Vertex, TiledCell, h3_cell_array, h3_latlng};
use vertex_edge::{_unpickle_h3_edge_v1, _unpickle_h3_vertex_v1, h3_edge_index, h3_vertex_index};
use vertex_edge_array::{
    _unpickle_h3_edge_array_v1, _unpickle_h3_vertex_array_v1, PyH3EdgeArrayIter,
    PyH3VertexArrayIter,
};
pub(crate) use vertex_edge_array::{PyH3EdgeArray, PyH3VertexArray};
