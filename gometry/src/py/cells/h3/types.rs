#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use std::sync::{Arc, Mutex, OnceLock};
#[cfg(test)]
use std::sync::{
    Barrier,
    atomic::{AtomicUsize, Ordering},
};

use h3o::{CellIndex, DirectedEdgeIndex, Resolution, VertexIndex};
use pyo3::exceptions::PyMemoryError;

use crate::HeapSize;
use crate::grid::CoverBudgetExceeded;
use crate::grid::cell::CellDepth;
use crate::grid::h3_coverer::{
    H3CoverError, H3CoverPlan, H3CoveredCell, H3TraversalRule, h3_cover_shape,
};
use crate::py::cells::coverage_ops::{CoverageCells, CoveragePartition};
use crate::py::cells::{
    CellRule, GeometryError, GridKind, PyCellArray, PyGeometry, PyResult, pyclass, pymethods,
};

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

/// Rule-independent overlap partition for inspection (`interior_cells` /
/// `boundary_cells` / `explain`), keyed by source geometry + resolution +
/// factory `max_cells`. Built lazily: visible-cell tiling never forces it, and
/// hierarchical transforms share the same [`Arc`] so a first inspection pays
/// once. Caches a cloneable Rust budget error (never a `PyErr`).
#[derive(Debug)]
pub(super) struct H3Membership {
    partition: OnceLock<Result<CoveragePartition<PyH3Cell>, CoverBudgetExceeded>>,
    // `OnceLock` owns the retained result; this narrow mutex owns only the
    // fallible first-computation window, so free-threaded inspectors cannot
    // race to build and discard duplicate partitions.
    partition_initialization: Mutex<()>,
    resolution: Resolution,
    #[cfg(test)]
    partition_computations: AtomicUsize,
    #[cfg(test)]
    initialization_rendezvous: Mutex<Option<Arc<Barrier>>>,
}

impl H3Membership {
    /// Empty holder: partition is computed on first inspection.
    pub(super) fn lazy(resolution: Resolution) -> Arc<Self> {
        Arc::new(Self {
            partition: OnceLock::new(),
            partition_initialization: Mutex::new(()),
            resolution,
            #[cfg(test)]
            partition_computations: AtomicUsize::new(0),
            #[cfg(test)]
            initialization_rendezvous: Mutex::new(None),
        })
    }

    /// The all-root traversal returns an id-sorted, root-disjoint stream.  Do
    /// not rebuild a proxy partition here: eager cells and delayed inspection
    /// share this exact classified product.
    pub(super) fn partition_from_covered(
        covered: impl IntoIterator<Item = H3CoveredCell>,
    ) -> CoveragePartition<PyH3Cell> {
        CoveragePartition::from_sorted_tagged(
            covered
                .into_iter()
                .map(|cell| (PyH3Cell { cell: cell.cell }, cell.interior)),
        )
    }

    pub(super) const fn resolution(&self) -> Resolution {
        self.resolution
    }

    pub(super) const fn partition_slot(
        &self,
    ) -> &OnceLock<Result<CoveragePartition<PyH3Cell>, CoverBudgetExceeded>> {
        &self.partition
    }

    #[cfg(test)]
    fn record_partition_computation(&self) {
        self.partition_computations.fetch_add(1, Ordering::Relaxed);
    }

    #[cfg(test)]
    fn partition_computations(&self) -> usize {
        self.partition_computations.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    fn set_initialization_rendezvous(&self, barrier: Option<Arc<Barrier>>) {
        *self
            .initialization_rendezvous
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = barrier;
    }

    #[cfg(test)]
    fn wait_for_initialization_rendezvous(&self) {
        let barrier = self
            .initialization_rendezvous
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some(barrier) = barrier {
            barrier.wait();
        }
    }
}

impl HeapSize for H3Membership {
    fn heap_bytes(&self) -> usize {
        match self.partition.get() {
            Some(Ok(partition)) => partition.heap_bytes(),
            Some(Err(_)) | None => 0,
        }
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
    immutable_type,
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
    // only on the source and resolution, not on the visible cell set. Lazy:
    // initialized on first inspection surface use.
    pub(super) membership: Arc<H3Membership>,
    /// Factory `max_cells` budget (serialized for pickle recompute — D07).
    /// `None` = unlimited (adult factory choice; recompute stays unbounded).
    pub(super) max_cells: Option<usize>,
}

impl PyH3Coverage {
    pub(super) fn retained_heap_bytes(&self) -> usize {
        let geometry_bytes = self.geometry.shape.shape().coordinate_bytes();
        let membership_bytes = self.membership.heap_bytes();
        // Count only an already-initialized partition; never trigger compute.
        match self.membership.partition_slot().get() {
            Some(Ok(partition)) => {
                geometry_bytes + membership_bytes + self.cells.additional_heap_bytes(partition)
            },
            Some(Err(_)) | None => geometry_bytes + membership_bytes + self.cells.heap_bytes(),
        }
    }

    /// Resolve the overlap inspection partition, computing it once on first use.
    /// May raise when the delayed overlap pass exceeds the factory `max_cells`.
    pub(super) fn partition(&self) -> PyResult<&CoveragePartition<PyH3Cell>> {
        use crate::py::cells::coverage_ops::cover_budget_err;

        // Inspection is genuinely lazy, but once any inspection surface has
        // populated the shared `Arc<OnceLock<_>>`, every later surface must
        // borrow that exact partition. Rebuilding before `set` would silently
        // repeat a discarded traversal on each access.
        if let Some(ready) = self.membership.partition_slot().get() {
            return match ready {
                Ok(partition) => Ok(partition),
                Err(error) => Err(cover_budget_err(*error)),
            };
        }

        #[cfg(test)]
        self.membership.wait_for_initialization_rendezvous();

        let _initializing = self
            .membership
            .partition_initialization
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // A concurrent first inspector may have completed while this caller
        // waited for the initialization gate. Borrow the retained result
        // rather than rebuilding it.
        if let Some(ready) = self.membership.partition_slot().get() {
            return match ready {
                Ok(partition) => Ok(partition),
                Err(error) => Err(cover_budget_err(*error)),
            };
        }

        #[cfg(test)]
        self.membership.record_partition_computation();

        let resolution = self.membership.resolution();
        let max_cells = self.max_cells;
        // Build before observing or filling the once-lock: an allocation
        // failure is transient and must leave lazy inspection cold.  A budget
        // result is deterministic for this retained source/resolution/limit
        // and remains a useful sticky result.
        let produced = match h3_cover_shape(
            self.geometry.shape.as_ref(),
            &H3CoverPlan::new(resolution),
            H3TraversalRule::Overlap,
            max_cells,
        ) {
            Ok(covered) => Ok(H3Membership::partition_from_covered(covered)),
            Err(H3CoverError::Budget(error)) => Err(error),
            Err(H3CoverError::Allocation) => {
                return Err(PyMemoryError::new_err("H3 coverage allocation failed"));
            },
            Err(H3CoverError::CapacityOverflow) => {
                return Err(GeometryError::new_err(
                    "H3 coverage traversal exceeded its representable capacity",
                ));
            },
            Err(H3CoverError::Geometry(error)) => return Err(error.into()),
        };
        let _ = self.membership.partition_slot().set(produced);
        let ready = self
            .membership
            .partition_slot()
            .get()
            .expect("H3 membership once-lock set or raced");
        match ready {
            Ok(partition) => Ok(partition),
            Err(err) => Err(cover_budget_err(*err)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geometry::{Point, Shape};

    #[test]
    fn lazy_inspection_traverses_once_after_the_partition_is_warm() {
        let membership = H3Membership::lazy(Resolution::Zero);
        let coverage = PyH3Coverage {
            geometry: PyGeometry::wgs84(Shape::Point(Point::new_unchecked_xy(0.0, 0.0))),
            cells: CoverageCells::from_cells(Vec::new()),
            cell_rule: CellRule::Center,
            depth: CellDepth::Uniform(0),
            membership: Arc::clone(&membership),
            max_cells: None,
        };
        let first = coverage
            .partition()
            .expect("the first inspection builds the retained partition");
        assert_eq!(membership.partition_computations(), 1);
        let second = coverage
            .partition()
            .expect("a warm inspection borrows the retained partition");
        assert!(std::ptr::eq(first, second));
        assert_eq!(
            membership.partition_computations(),
            1,
            "interior_cells, boundary_cells, and explain must not discard a recomputed traversal"
        );
    }

    #[test]
    fn concurrent_first_inspections_share_one_partition_traversal() {
        let membership = H3Membership::lazy(Resolution::Zero);
        let coverage = PyH3Coverage {
            geometry: PyGeometry::wgs84(Shape::Point(Point::new_unchecked_xy(0.0, 0.0))),
            cells: CoverageCells::from_cells(Vec::new()),
            cell_rule: CellRule::Center,
            depth: CellDepth::Uniform(0),
            membership: Arc::clone(&membership),
            max_cells: None,
        };
        membership.set_initialization_rendezvous(Some(Arc::new(Barrier::new(2))));
        std::thread::scope(|scope| {
            scope.spawn(|| {
                coverage
                    .partition()
                    .expect("the first concurrent inspection builds a partition");
            });
            scope.spawn(|| {
                coverage
                    .partition()
                    .expect("the second concurrent inspection shares that partition");
            });
        });
        membership.set_initialization_rendezvous(None);
        assert_eq!(
            membership.partition_computations(),
            1,
            "two free-threaded first inspections may wait, but may not duplicate the traversal"
        );
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
