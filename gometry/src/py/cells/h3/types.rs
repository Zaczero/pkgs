#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use std::sync::Arc;

use h3o::{CellIndex, DirectedEdgeIndex, Resolution, VertexIndex};

use crate::HeapSize;
use crate::grid::cell::CellDepth;
use crate::py::cells::coverage_ops::{CoverageCells, CoveragePartition};
use crate::py::cells::*;

pub(super) const fn h3_latlng(
    p: crate::geometry::types::Point,
) -> Result<h3o::LatLng, h3o::error::InvalidLatLng> {
    h3o::LatLng::new(p.y, p.x)
}

/// One cell selected by the polyfill, with whether it lies entirely inside the
/// source geometry (the native annotated-cell record).
#[derive(Clone, Copy, Debug)]
pub(super) struct TiledCell {
    pub(super) cell: CellIndex,
    pub(super) is_fully_contained: bool,
}
/// annotated `Covers` tiling pass.
#[derive(Clone, Debug)]
pub(super) struct H3Membership {
    pub(super) partition: CoveragePartition<PyH3Cell>,
    resolution: Resolution,
}

impl H3Membership {
    pub(super) fn from_annotated(annotated: &[TiledCell], resolution: Resolution) -> Self {
        let mut tagged = annotated.to_vec();
        tagged.sort_unstable_by_key(|cell| u64::from(cell.cell));
        let mut canonical: Vec<TiledCell> = Vec::with_capacity(tagged.len());
        for cell in tagged {
            if let Some(last) = canonical.last_mut()
                && last.cell == cell.cell
            {
                last.is_fully_contained |= cell.is_fully_contained;
            } else {
                canonical.push(cell);
            }
        }
        Self {
            partition: CoveragePartition::from_sorted_tagged(
                canonical
                    .into_iter()
                    .map(|cell| (PyH3Cell { cell: cell.cell }, cell.is_fully_contained)),
            ),
            resolution,
        }
    }

    pub(super) const fn resolution(&self) -> Resolution {
        self.resolution
    }
}

/// An H3 covering of a geometry.
///
/// Returned by ``h3_cover(...)``: ``coverage.cells`` materializes the
/// cells selected by ``cell_rule`` (join keys, bins, visualization), while
/// ``covers``/``contains``/``intersects`` answer exactly against the source
/// geometry, independent of the rule. Iterate it, test ``cell in coverage``,
/// or ``compact``/``with_parents`` across resolutions.
#[pyclass(
    name = "H3Coverage",
    module = "gometry",
    frozen,
    sequence,
    skip_from_py_object
)]
#[derive(Clone, Debug)]
pub(crate) struct PyH3Coverage {
    // Source geometry (canonical WGS84 lon/lat): the exact membership
    // predicates always test against it, never against the cells.
    pub(super) geometry: PyGeometry,
    pub(super) cells: CoverageCells<PyH3Cell>,
    pub(super) cell_rule: CellRule,
    pub(super) depth: CellDepth,
    // Rule-independent partition data, shared by derived coverages — it depends
    // only on the source and resolution, not on the visible cell set.
    pub(super) membership: Arc<H3Membership>,
    /// Factory `max_cells` budget (serialized for pickle recompute — D07).
    /// `None` = unlimited (adult factory choice; recompute stays unbounded).
    pub(super) max_cells: Option<usize>,
}

impl PyH3Coverage {
    pub(super) fn retained_heap_bytes(&self) -> usize {
        self.geometry.shape.shape().coordinate_bytes()
            + self.membership.partition.heap_bytes()
            + self.cells.additional_heap_bytes(&self.membership.partition)
    }
}

impl HeapSize for H3Membership {
    fn heap_bytes(&self) -> usize {
        self.partition.heap_bytes()
    }
}

crate::heapless!(CellIndex, PyH3Cell);

/// One H3 cell: a resolution-addressed hexagonal (or pentagonal) tile.
///
/// Wraps the 64-bit cell index with typed accessors (``cell.resolution``,
/// ``cell.polygon``, ``cell.center``, ``cell.area``), hierarchy moves
/// (``parent``/``children``), and grid traversal (``grid_disk``,
/// ``grid_ring``, ``grid_path``, ``grid_distance``). Convert via
/// ``H3Cell(...)``, and back with ``int(cell)``.
#[pyclass(name = "H3Cell", module = "gometry", frozen, skip_from_py_object)]
#[derive(Clone, Copy, Debug)]
pub(crate) struct PyH3Cell {
    pub(crate) cell: CellIndex,
}

pub(super) fn h3_cell_vec(mut cells: Vec<CellIndex>) -> Vec<PyH3Cell> {
    cells.sort_unstable_by_key(|cell| u64::from(*cell));
    cells.dedup_by_key(|cell| u64::from(*cell));
    cells.into_iter().map(|cell| PyH3Cell { cell }).collect()
}

pub(super) fn h3_cell_array(mut cells: Vec<CellIndex>) -> PyCellArray {
    cells.sort_unstable_by_key(|cell| u64::from(*cell));
    cells.dedup_by_key(|cell| u64::from(*cell));
    PyCellArray::from_trusted_ids(GridKind::H3Cell, cells.into_iter().map(u64::from).collect())
}

pub(super) fn py_h3_cell_array(cells: &CoverageCells<PyH3Cell>) -> PyCellArray {
    cells.cell_array(GridKind::H3Cell)
}

// Lazy iterator over a coverage's cells, yielding one cell per step.
coverage_iter_pyclass! { iter: PyH3CoverageIter, cell: PyH3Cell, name: "H3CoverageIterator" }

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
#[pyclass(name = "H3Vertex", module = "gometry", frozen, skip_from_py_object)]
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
#[pyclass(name = "H3Edge", module = "gometry", frozen, skip_from_py_object)]
#[derive(Clone, Copy, Debug)]
pub(super) struct PyH3Edge {
    pub(super) edge: DirectedEdgeIndex,
}
