use pyo3::IntoPyObjectExt;

use super::cell::py_s2_cell_array;
use super::*;
use crate::Typed;
use crate::grid::cell::CellDepth;
use crate::py::cells::coverage_ops::{CoverageCells, HierarchicalCoverageOps};
#[pymethods]
impl PyS2Coverage {
    /// Minimum (coarsest) cell level allowed in the covering.
    ///
    /// Returns
    /// -------
    /// int
    #[getter]
    const fn min_level(&self) -> u8 {
        self.min_level
    }

    /// Maximum (finest) cell level allowed in the covering.
    ///
    /// Returns
    /// -------
    /// int
    #[getter]
    const fn max_level(&self) -> u8 {
        self.max_level
    }

    /// Level stride of the covering (emitted levels step by this much).
    ///
    /// Returns
    /// -------
    /// int
    #[getter]
    const fn level_mod(&self) -> u8 {
        self.level_mod
    }

    /// Maximum number of cells in the covering (the hard emission cap from
    /// the factory). ``None`` means unlimited.
    ///
    /// Returns
    /// -------
    /// int or None
    #[getter]
    const fn max_cells(&self) -> Option<usize> {
        self.max_cells
    }

    /// Adaptive refinement target from the factory. It guides optional
    /// subdivision only; ``max_cells`` remains the hard emission cap.
    ///
    /// Returns
    /// -------
    /// int
    #[getter]
    const fn target_cells(&self) -> usize {
        self.target_cells
    }

    /// Fixed S2 cell level of the **visible** cell set, or ``None`` when the
    /// visible cells span multiple levels (adaptive / compacted).
    ///
    /// Factory cover bounds stay on ``min_level`` / ``max_level`` (pickle
    /// recompute uses those); after ``uncompact`` the visible set is uniform
    /// even when the source covering was adaptive.
    ///
    /// Returns
    /// -------
    /// int or None
    #[getter]
    fn level(&self) -> Option<u8> {
        match CellDepth::from_levels(self.cells.iter().map(|cell| cell.cell.level())) {
            Some(CellDepth::Uniform(level)) => Some(level),
            _ => None,
        }
    }
}

grid_hierarchical_coverage_common_pymethods! {
    impl PyS2Coverage {
        compact_doc: "Compact the cell set to its coarsest covering (merge complete sibling\ngroups into their parent).\n\nParameters\n----------\nmin_level : int, default 0\n    Coarsest level compaction may produce; merging stops at this floor\n    (cells already coarser pass through unchanged).\n\nReturns\n-------\nS2Coverage\n    The compacted covering (same area, fewest cells).\n\nExamples\n--------\n>>> import gometry as gm\n>>> p = gm.Point(-122.4194, 37.7749, crs=4326)\n>>> cov = gm.s2_cover(p, level=12)\n>>> len(cov.compact().cells) <= len(cov.cells)\nTrue\n",
        compact_param: min_level,
        compact_default: 0,
        compact_text_signature: "($self, *, min_level=0)",
        uncompact_doc: "Expand the cell set to a uniform level (every cell subdivided down to\n``level``).\n\nParameters\n----------\nlevel : int\n    Target S2 level (``0``-``30``); no coarser than any current cell.\n\nReturns\n-------\nS2Coverage\n    The expanded covering.\n\nRaises\n------\nGeometryError\n    If ``level`` is out of range.\n\nExamples\n--------\n>>> import gometry as gm\n>>> p = gm.Point(-122.4194, 37.7749, crs=4326)\n>>> cov = gm.s2_cover(p, level=12)\n>>> len(cov.uncompact(12).cells) >= len(cov.cells)\nTrue\n",
        uncompact_param: level,
        uncompact_text_signature: "($self, level)",
        with_parents_doc: "Include parent cells down to a minimum level.\n\nParameters\n----------\nmin_level : int, default 0\n    Coarsest level to add parents for (0 is the root face level).\n\nReturns\n-------\nS2Coverage\n\nRaises\n------\nGeometryError\n    If ``min_level`` is out of range.\n\nExamples\n--------\n>>> import gometry as gm\n>>> p = gm.Point(-122.4194, 37.7749, crs=4326)\n>>> cov = gm.s2_cover(p, level=12)\n>>> len(cov.with_parents().cells) >= len(cov.cells)\nTrue\n",
        with_parents_param: min_level,
        with_parents_default: 0,
        with_parents_text_signature: "($self, *, min_level=0)",
    }
}

impl HierarchicalCoverageOps for PyS2Coverage {
    type Cell = PyS2Cell;

    fn cells(&self) -> &CoverageCells<Self::Cell> {
        &self.cells
    }

    fn cell_level(cell: &Self::Cell) -> u8 {
        cell.cell.level()
    }

    fn parse_floor(value: i64) -> PyResult<u8> {
        parse_s2_min_level_value(value)
    }

    fn parse_target_depth(value: &Bound<'_, PyAny>) -> PyResult<u8> {
        parse_s2_level(value)
    }

    fn compact_cells(cells: Vec<Self::Cell>, floor: u8) -> PyResult<Vec<Self::Cell>> {
        // CoverageCells::from_cells sorts/dedups; no pre-pass needed.
        let cells = cells.into_iter().map(|cell| cell.cell).collect();
        Ok(cell_set::compact_with_floor(cells, floor)
            .into_iter()
            .map(|cell| PyS2Cell { cell })
            .collect())
    }

    fn uncompact_cells(cells: Vec<Self::Cell>, depth: u8) -> PyResult<Vec<Self::Cell>> {
        // Explicit coverage transform — no cell budget re-cap.
        let cells = cells.into_iter().map(|cell| cell.cell).collect::<Vec<_>>();
        Ok(
            cell_set::uncompact_unlimited(&cell_set::normalize(cells), depth)
                .into_iter()
                .map(|cell| PyS2Cell { cell })
                .collect(),
        )
    }

    fn parent_cell(cell: &Self::Cell, depth: u8) -> PyResult<Option<Self::Cell>> {
        Ok(Some(PyS2Cell {
            cell: cell.cell.parent(depth).expect("coarser level"),
        }))
    }

    fn uncompact_floor_error(cell: &Self::Cell) -> PyErr {
        uncompact_floor_error(GridKind::S2Cell, "level", cell.cell.token())
    }

    fn with_compacted_cells(&self, cells: Vec<Self::Cell>) -> Self {
        // Keep factory min/max/level_mod/max_cells/target_cells for pickle recompute (same
        // contract as uncompact). Visible depth is reported by `.level`, not
        // by mutating factory provenance — overwriting min/max with the
        // compacted cells' depths caused silent interior/boundary partition
        // drift across pickle.
        Self {
            cells: CoverageCells::from_cells(cells),
            ..self.clone()
        }
    }

    fn with_uncompacted_cells(&self, cells: Vec<Self::Cell>, _depth: u8) -> Self {
        // Keep factory min/max/level_mod/max_cells/target_cells for pickle recompute; the
        // visible cells are uniform at `_depth` (reported by `.level`).
        Self {
            cells: CoverageCells::from_cells(cells),
            ..self.clone()
        }
    }

    fn with_parent_cells(&self, cells: Vec<Self::Cell>, _floor: u8) -> Self {
        // Decorative ancestors must not rewrite factory min_level: pickle
        // recompute and partition membership ride the original cover bounds.
        // CoverageCells::from_cells sorts/dedups — no pre-pass.
        Self {
            cells: CoverageCells::from_cells(cells),
            ..self.clone()
        }
    }
}

grid_coverage_common_pymethods! {
    impl PyS2Coverage {
        this: coverage,
        kind: GridKind::S2Cell,
        iter: PyS2CoverageIter,
        cell_array: py_s2_cell_array,
        parse_cell: s2_cell_id,
        parsed_key: |cell| cell.raw(),
        interior_doc: "Cells certified entirely inside the source geometry.\n\nWith ``boundary_cells`` this is the rule-independent classification of the covering: any point in an interior cell is inside the area, no geometry test needed. Render these solid and ``boundary_cells`` outlined for a faithful core-vs-fringe picture; together with ``boundary_cells`` it partitions the ``'overlap'`` covering.\n\nReturns\n-------\nCellArray of S2Cell",
        interior_cells: { coverage.membership.partition.interior().cell_array(GridKind::S2Cell) },
        boundary_doc: "Cells partially overlapping the source geometry (the fringe where cell membership cannot answer the geometry question).\n\nReturns\n-------\nCellArray of S2Cell",
        boundary_cells: {
            coverage.membership.partition.boundary().cell_array(GridKind::S2Cell)
        },
        depth_fields: [min_level, max_level, level_mod, max_cells, target_cells],
        hash_depth: (coverage.min_level, coverage.max_level, coverage.level_mod, coverage.max_cells, coverage.target_cells,),
        cell_hash_key: |cell| { cell.cell.raw() },
        explain_grid: "s2",
        explain_depth: {
            coverage.level().map_or_else(
                || {
                    format!(
                        "levels {}..{}, level_mod {}, target_cells {}, max_cells {}",
                        coverage.min_level,
                        coverage.max_level,
                        coverage.level_mod,
                        coverage.target_cells,
                        coverage
                            .max_cells
                            .map_or_else(|| "None".to_owned(), |n| n.to_string())
                    )
                },
                |level| format!("level {level}"),
            )
        },
        explain_cells: "cells",
        explain_interior_len: { coverage.membership.partition.interior_len() },
        explain_outer_len: { coverage.membership.partition.outer_len() },
        to_polygon_doc: "Dissolve the coverage into one outline geometry.\n\nShared cell edges are removed, so the result is the coverage's region as one geometry, not one polygon per cell like ``coverage.cells.polygon``. Mixed levels dissolve too, so ``coverage.compact().to_polygon()`` works. Disjoint covered regions return a `MultiPolygon`.\n\nReturns\n-------\n`Polygon` or `MultiPolygon`\n    The dissolved region, tagged ``EPSG:4326``.\n\nRaises\n------\nGeometryError\n    If the coverage is empty.",
        to_polygon: {
            let cells: Vec<_> = coverage.cells.iter().map(|cell| cell.cell).collect();
            s2_dissolve_sorted(&cells)
        },
        reduce_unpickle: "_unpickle_s2_coverage",
        reduce_args: {
            (
                Typed(coverage.geometry.clone()),
                coverage.cells
                    .iter()
                    .map(|cell| cell.cell.raw())
                    .collect::<Vec<_>>(),
                coverage.cell_rule.token(),
                coverage.min_level,
                coverage.max_level,
                coverage.level_mod,
                coverage.max_cells,
                coverage.target_cells,
            )
        },
        repr: {
            format!(
                "<S2Coverage levels={}..{} cell_rule={} target_cells={} max_cells={:?} cells={}>",
                coverage.min_level,
                coverage.max_level,
                coverage.cell_rule.token(),
                coverage.target_cells,
                coverage.max_cells,
                coverage.cells.len()
            )
        },
        index_error: "S2 coverage index out of range",
    }
}
