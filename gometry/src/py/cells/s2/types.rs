#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use std::sync::Arc;

use super::*;
use crate::grid::s2::cellid::CellId;
use crate::py::cells::coverage_ops::{CoverageCells, CoveragePartition};
use crate::{HeapSize, PyGeometry};

/// The covering's rule-independent partition: normalized outer
/// (intersecting) and interior (certified fully covered) cell sets.
#[derive(Clone, Debug)]
pub(super) struct S2Membership {
    pub(super) partition: CoveragePartition<PyS2Cell>,
}

/// One S2 cell: a level-addressed quadrilateral tile on the sphere.
///
/// Wraps the 64-bit cell id with typed accessors (``cell.level``,
/// ``cell.token``, ``cell.polygon``, ``cell.center``) and hierarchy
/// moves (``parent``/``children``/``neighbors``). Convert via
/// ``S2Cell(...)``, and back with ``int(cell)``.
#[pyclass(name = "S2Cell", module = "gometry", frozen, skip_from_py_object)]
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
    // Rule-independent partition data, shared by derived coverages.
    pub(super) membership: Arc<S2Membership>,
}

impl PyS2Coverage {
    pub(super) fn retained_heap_bytes(&self) -> usize {
        self.geometry.shape.shape().coordinate_bytes()
            + self.membership.partition.heap_bytes()
            + self.cells.additional_heap_bytes(&self.membership.partition)
    }
}

impl HeapSize for S2Membership {
    fn heap_bytes(&self) -> usize {
        self.partition.heap_bytes()
    }
}

crate::heapless!(CellId, PyS2Cell);
