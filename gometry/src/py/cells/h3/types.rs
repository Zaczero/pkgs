#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use h3o::{CellIndex, DirectedEdgeIndex, VertexIndex};

use crate::py::cells::{GridKind, PyCellArray, pyclass};

pub(super) const fn h3_latlng(
    p: crate::geometry::types::Point,
) -> Result<h3o::LatLng, h3o::error::InvalidLatLng> {
    h3o::LatLng::new(p.y, p.x)
}

/// One cell selected by the historical center/within tiler.
#[derive(Clone, Copy, Debug)]
pub(super) struct TiledCell {
    pub(super) cell: CellIndex,
}

/// One H3 cell: a resolution-addressed hexagonal (or pentagonal) tile.
///
/// Wraps the 64-bit cell index with typed accessors (``cell.resolution``,
/// ``cell.polygon``, ``cell.center``, ``cell.area``), hierarchy moves
/// (``parent``/``children``), and grid traversal (``grid_disk``,
/// ``grid_ring``, ``grid_path``, ``grid_distance``). Convert via
/// ``H3Cell(...)``, and back with ``int(cell)``.
#[pyclass(
    name = "H3Cell",
    module = "gometry",
    frozen,
    immutable_type,
    skip_from_py_object
)]
#[derive(Clone, Copy, Debug)]
pub(crate) struct PyH3Cell {
    pub(crate) cell: CellIndex,
}

crate::heapless!(CellIndex, PyH3Cell);

pub(super) fn h3_cell_array(mut cells: Vec<CellIndex>) -> PyCellArray {
    cells.sort_unstable_by_key(|cell| u64::from(*cell));
    cells.dedup_by_key(|cell| u64::from(*cell));
    PyCellArray::from_trusted_ids(GridKind::H3Cell, cells.into_iter().map(u64::from).collect())
}

/// A canonical H3 topological vertex.
///
/// Vertexes carry shared identity: adjacent cells yield *equal* vertex
/// objects for their shared corners, so they deduplicate across cell sets
/// (``{v for cell in cells for v in cell.vertices}``). Obtained from
/// `H3Cell.vertices`; convert with ``int(vertex)`` or ``vertex.token``.
///
/// Parameters
/// ----------
/// value : H3Vertex, int, or str
///     An existing vertex, its 64-bit id, or its token.
#[pyclass(
    name = "H3Vertex",
    module = "gometry",
    frozen,
    immutable_type,
    skip_from_py_object
)]
#[derive(Clone, Copy, Debug)]
pub(super) struct PyH3Vertex {
    pub(super) vertex: VertexIndex,
}
/// One directed H3 edge: the shared boundary between an origin cell and a
/// neighboring destination cell, with its own 64-bit H3 index.
///
/// Obtained from `H3Cell.edge` / `H3Cell.edges`; convert with
/// ``int(edge)`` or ``edge.token``, and rebuild with ``H3Edge(token)``.
///
/// Parameters
/// ----------
/// value : H3Edge, int, or str
///     An existing edge, its 64-bit id, or its token.
#[pyclass(
    name = "H3Edge",
    module = "gometry",
    frozen,
    immutable_type,
    skip_from_py_object
)]
#[derive(Clone, Copy, Debug)]
pub(super) struct PyH3Edge {
    pub(super) edge: DirectedEdgeIndex,
}
