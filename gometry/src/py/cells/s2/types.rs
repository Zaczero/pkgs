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

use pyo3::exceptions::PyMemoryError;

use crate::geometry::Shape;
use crate::grid::CoverBudgetExceeded;
use crate::grid::s2::cellid::CellId;
use crate::py::cells::coverage_ops::{CoverageCells, CoveragePartition};
use crate::py::cells::{CellRule, PyResult, pyclass, pymethods};
use crate::{HeapSize, PyGeometry};

/// Rule-independent coverer partition for inspection (`interior_cells` /
/// `boundary_cells` / `explain`), keyed by source geometry + level budget +
/// factory `max_cells`. Built lazily: visible-cell selection never forces it
/// for non-overlap rules, and hierarchical transforms share the same [`Arc`]
/// so a first inspection pays once. Caches a cloneable Rust budget error.
///
/// Deliberately S2-local (not shared with [`H3Membership`]): S2 recomputes via
/// the leaf-range coverer with four level params, not H3's native polyfill.
#[derive(Debug)]
pub(super) struct S2Membership {
    partition: OnceLock<Result<CoveragePartition<PyS2Cell>, CoverBudgetExceeded>>,
    // `OnceLock` retains the winning result, while this narrow gate prevents
    // free-threaded first inspectors from each constructing and discarding an
    // identical partition before they race to store it.
    partition_initialization: Mutex<()>,
    /// Split-normalized working shape used by delayed partition recompute.
    cover_shape: Shape,
    /// True when antimeridian split allocated storage distinct from the
    /// membership geometry — only then is `cover_shape` counted in heap.
    cover_is_split: bool,
    min_level: u8,
    max_level: u8,
    level_mod: u8,
    target_cells: usize,
    #[cfg(test)]
    partition_computations: AtomicUsize,
    #[cfg(test)]
    initialization_rendezvous: Mutex<Option<Arc<Barrier>>>,
}

impl S2Membership {
    /// Empty holder: partition is computed on first inspection.
    pub(super) fn lazy(
        cover_shape: Shape,
        cover_is_split: bool,
        min_level: u8,
        max_level: u8,
        level_mod: u8,
        target_cells: usize,
    ) -> Arc<Self> {
        Arc::new(Self {
            partition: OnceLock::new(),
            partition_initialization: Mutex::new(()),
            cover_shape,
            cover_is_split,
            min_level,
            max_level,
            level_mod,
            target_cells,
            #[cfg(test)]
            partition_computations: AtomicUsize::new(0),
            #[cfg(test)]
            initialization_rendezvous: Mutex::new(None),
        })
    }

    /// Seed from an already-built coverer partition (overlap rule — construction
    /// already paid for the full tagged product).
    pub(super) fn seeded(
        partition: CoveragePartition<PyS2Cell>,
        cover_shape: Shape,
        cover_is_split: bool,
        min_level: u8,
        max_level: u8,
        level_mod: u8,
        target_cells: usize,
    ) -> Arc<Self> {
        let lock = OnceLock::new();
        let _ = lock.set(Ok(partition));
        Arc::new(Self {
            partition: lock,
            partition_initialization: Mutex::new(()),
            cover_shape,
            cover_is_split,
            min_level,
            max_level,
            level_mod,
            target_cells,
            #[cfg(test)]
            partition_computations: AtomicUsize::new(0),
            #[cfg(test)]
            initialization_rendezvous: Mutex::new(None),
        })
    }

    pub(super) const fn partition_slot(
        &self,
    ) -> &OnceLock<Result<CoveragePartition<PyS2Cell>, CoverBudgetExceeded>> {
        &self.partition
    }

    pub(super) const fn min_level(&self) -> u8 {
        self.min_level
    }

    pub(super) const fn max_level(&self) -> u8 {
        self.max_level
    }

    pub(super) const fn level_mod(&self) -> u8 {
        self.level_mod
    }

    pub(super) const fn target_cells(&self) -> usize {
        self.target_cells
    }

    /// Build the inspection partition from a coverer emission.
    pub(super) fn partition_from_covering(
        covering: impl IntoIterator<Item = (CellId, bool)>,
    ) -> CoveragePartition<PyS2Cell> {
        CoveragePartition::from_sorted_tagged(
            covering
                .into_iter()
                .map(|(cell, interior)| (PyS2Cell { cell }, interior)),
        )
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

impl HeapSize for S2Membership {
    fn heap_bytes(&self) -> usize {
        // Count the cover working shape only when antimeridian normalization
        // created distinct storage from the membership geometry.
        let cover_bytes = if self.cover_is_split {
            self.cover_shape.coordinate_bytes()
        } else {
            0
        };
        cover_bytes
            + match self.partition.get() {
                Some(Ok(partition)) => partition.heap_bytes(),
                Some(Err(_)) | None => 0,
            }
    }
}

/// One S2 cell: a level-addressed quadrilateral tile on the sphere.
///
/// Wraps the 64-bit cell id with typed accessors (``cell.level``,
/// ``cell.token``, ``cell.polygon``, ``cell.center``) and hierarchy
/// moves (``parent``/``children``/``neighbors``). Convert via
/// ``S2Cell(...)``, and back with ``int(cell)``.
#[pyclass(
    name = "S2Cell",
    module = "gometry",
    frozen,
    immutable_type,
    skip_from_py_object
)]
#[derive(Clone, Copy, Debug)]
pub(crate) struct PyS2Cell {
    pub(crate) cell: CellId,
}

// Lazy iterator over a coverage's cells, yielding one cell per step.
coverage_iter_pyclass! { iter: PyS2CoverageIter, cell: PyS2Cell, name: "S2CoverageIterator" }

/// An exact-classified S2 covering of a geometry.
///
/// Returned by ``s2_cover(...)``: ``coverage.cells`` materializes the
/// cells selected by ``cell_rule`` within the level budget (join keys,
/// bins, visualization), while ``covers``/``contains``/``intersects``
/// answer exactly against the source geometry, independent of the rule.
/// Iterate it, test ``cell in coverage``, or ``compact``/``uncompact``
/// across levels.
#[pyclass(
    name = "S2Coverage",
    module = "gometry",
    frozen,
    immutable_type,
    sequence,
    skip_from_py_object
)]
#[derive(Clone, Debug)]
pub(crate) struct PyS2Coverage {
    // Source geometry (canonical WGS84 lon/lat): the exact membership
    // predicates always test against it, never against the cells.
    pub(super) geometry: PyGeometry,
    pub(super) cells: CoverageCells<PyS2Cell>,
    pub(super) cell_rule: CellRule,
    pub(super) min_level: u8,
    pub(super) max_level: u8,
    pub(super) level_mod: u8,
    /// Hard emission cap from the factory. `None` = unlimited
    /// (`max_cells=None`).
    pub(super) max_cells: Option<usize>,
    /// Adaptive refinement target from the factory.
    pub(super) target_cells: usize,
    // Rule-independent partition data, shared by derived coverages — depends
    // only on the source and level budget, not the visible cell set. Lazy:
    // initialized on first inspection surface use (seeded immediately only for
    // the overlap rule, which already produces the tagged product).
    pub(super) membership: Arc<S2Membership>,
}

impl PyS2Coverage {
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

    /// Resolve the coverer inspection partition, computing it once on first use.
    /// May raise when the delayed coverer pass exceeds the factory `max_cells`.
    pub(super) fn partition(&self) -> PyResult<&CoveragePartition<PyS2Cell>> {
        use crate::py::cells::coverage_ops::cover_budget_err;

        if let Some(ready) = self.membership.partition_slot().get() {
            return match ready {
                Ok(partition) => Ok(partition),
                Err(err) => Err(cover_budget_err(*err)),
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
        // waited for the gate. Always reuse the retained result in that case.
        if let Some(ready) = self.membership.partition_slot().get() {
            return match ready {
                Ok(partition) => Ok(partition),
                Err(err) => Err(cover_budget_err(*err)),
            };
        }
        #[cfg(test)]
        self.membership.record_partition_computation();
        let max_cells = self.max_cells;
        let min_level = self.membership.min_level();
        let max_level = self.membership.max_level();
        let level_mod = self.membership.level_mod();
        let target_cells = self.membership.target_cells();
        // Build before touching the once-lock: allocation failure is transient
        // and must leave lazy inspection cold.  Reuse the construction owner
        // so a lazy aggregate cannot bypass component decomposition.
        let computed = match super::register::cover_s2_components(
            self.geometry.shape.as_ref(),
            min_level,
            max_level,
            level_mod,
            max_cells,
            target_cells,
        ) {
            Ok(covering) => Ok(S2Membership::partition_from_covering(covering)),
            Err(super::register::S2ComponentCoverError::Budget(error)) => Err(error),
            Err(super::register::S2ComponentCoverError::Allocation) => {
                return Err(PyMemoryError::new_err("S2 coverage allocation failed"));
            },
        };
        let _ = self.membership.partition_slot().set(computed);
        let ready = self
            .membership
            .partition_slot()
            .get()
            .expect("S2 membership once-lock set while initialization gate is held");
        match ready {
            Ok(partition) => Ok(partition),
            Err(err) => Err(cover_budget_err(*err)),
        }
    }
}

crate::heapless!(CellId, PyS2Cell);

#[cfg(test)]
mod tests {
    use std::thread;

    use super::*;
    use crate::geometry::Point;
    use crate::py::cells::coverage_ops::CoverageCells;

    fn cold_coverage(membership: Arc<S2Membership>) -> PyS2Coverage {
        PyS2Coverage {
            geometry: PyGeometry::wgs84(Shape::Point(Point::new_unchecked_xy(0.0, 0.0))),
            cells: CoverageCells::from_cells(Vec::new()),
            cell_rule: CellRule::Center,
            min_level: 0,
            max_level: 0,
            level_mod: 1,
            max_cells: None,
            target_cells: 8,
            membership,
        }
    }

    #[test]
    fn racing_cold_inspections_build_one_partition() {
        let membership = S2Membership::lazy(
            Shape::Point(Point::new_unchecked_xy(0.0, 0.0)),
            false,
            0,
            0,
            1,
            8,
        );
        membership.set_initialization_rendezvous(Some(Arc::new(Barrier::new(2))));
        let left = cold_coverage(Arc::clone(&membership));
        let right = cold_coverage(Arc::clone(&membership));
        thread::scope(|scope| {
            scope.spawn(|| left.partition().expect("left inspection"));
            scope.spawn(|| right.partition().expect("right inspection"));
        });
        membership.set_initialization_rendezvous(None);
        assert_eq!(membership.partition_computations(), 1);
    }
}
