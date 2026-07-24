#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use super::*;
use crate::grid::cell::CellDepth;
use crate::py::cells::coverage_ops::{CoverageCells, HierarchicalCoverageOps};
use crate::py::cells::*;

#[pymethods]
impl PyH3Coverage {
    /// Uniform H3 resolution of the covering's cells, or ``None`` for mixed
    /// resolutions.
    ///
    /// Returns
    /// -------
    /// int or None
    #[getter]
    const fn resolution(&self) -> Option<u8> {
        self.depth.uniform_level()
    }
}

grid_hierarchical_coverage_common_pymethods! {
    impl PyH3Coverage {
        compact_doc: "Compact the cell set to its coarsest covering.\n\nParameters\n----------\nmin_resolution : int, default 0\n    Coarsest resolution compaction may produce; merging stops at this\n    floor (cells already coarser pass through unchanged).\n\nReturns\n-------\nH3Coverage\n    The compacted covering (same area, fewest cells).\n\nExamples\n--------\n>>> import gometry as gm\n>>> poly = gm.box(-122.5, 37.7, -122.3, 37.85, crs=4326)\n>>> cov = gm.h3_cover(poly, resolution=7)\n>>> len(cov.compact().cells) < len(cov.cells)\nTrue\n",
        compact_param: min_resolution,
        compact_default: 0,
        compact_text_signature: "($self, *, min_resolution=0)",
        uncompact_doc: "Expand the cell set to a uniform resolution (every cell subdivided down\nto ``resolution``).\n\nParameters\n----------\nresolution : int\n    Target H3 resolution (``0``-``15``); no coarser than any\n    current cell.\n\nReturns\n-------\nH3Coverage\n    The expanded covering.\n\nRaises\n------\nGeometryError\n    If ``resolution`` is out of range.\n\nExamples\n--------\n>>> import gometry as gm\n>>> poly = gm.box(-122.5, 37.7, -122.3, 37.85, crs=4326)\n>>> cov = gm.h3_cover(poly, resolution=7).compact()\n>>> len(cov.uncompact(7).cells) >= len(cov.cells)\nTrue\n",
        uncompact_param: resolution,
        uncompact_text_signature: "($self, resolution)",
        with_parents_doc: "Include parent cells down to a minimum resolution.\n\nParameters\n----------\nmin_resolution : int, default 0\n    Coarsest resolution to add parents for (0 is the base-cell\n    resolution).\n\nReturns\n-------\nH3Coverage\n\nRaises\n------\nGeometryError\n    If ``min_resolution`` is out of range.\n\nExamples\n--------\n>>> import gometry as gm\n>>> poly = gm.box(-122.5, 37.7, -122.3, 37.85, crs=4326)\n>>> cov = gm.h3_cover(poly, resolution=7)\n>>> len(cov.with_parents().cells) > len(cov.cells)\nTrue\n",
        with_parents_param: min_resolution,
        with_parents_default: 0,
        with_parents_text_signature: "($self, *, min_resolution=0)",
    }
}

impl HierarchicalCoverageOps for PyH3Coverage {
    type Cell = PyH3Cell;

    fn cells(&self) -> &CoverageCells<Self::Cell> {
        &self.cells
    }

    fn cell_level(cell: &Self::Cell) -> u8 {
        cell.cell.resolution().into()
    }

    fn parse_floor(value: i64) -> PyResult<u8> {
        // h3_floor already validates 0..=15; consume the validated value.
        h3_floor(value)
    }

    fn parse_target_depth(value: &Bound<'_, PyAny>) -> PyResult<u8> {
        parse_h3_resolution(value).map(Into::into)
    }

    fn compact_cells(cells: Vec<Self::Cell>, floor: u8) -> PyResult<Vec<Self::Cell>> {
        let cells = cells.into_iter().map(|cell| cell.cell).collect::<Vec<_>>();
        // floor is validated by parse_floor; convert without re-checking the range.
        let resolution = Resolution::try_from(floor).expect("floor validated by parse_floor");
        Ok(h3_cell_vec(h3_compact_with_floor(cells, resolution)?))
    }

    fn uncompact_cells(cells: Vec<Self::Cell>, depth: u8) -> PyResult<Vec<Self::Cell>> {
        // Explicit coverage transform — no cell budget re-cap.
        // depth is validated by parse_target_depth.
        let resolution =
            Resolution::try_from(depth).expect("depth validated by parse_target_depth");
        let expanded: Vec<CellIndex> =
            CellIndex::uncompact(cells.into_iter().map(|cell| cell.cell), resolution).collect();
        Ok(h3_cell_vec(expanded))
    }

    fn parent_cell(cell: &Self::Cell, depth: u8) -> PyResult<Option<Self::Cell>> {
        let resolution = Resolution::try_from(depth).expect("parent depth within hierarchy");
        Ok(cell.cell.parent(resolution).map(|cell| PyH3Cell { cell }))
    }

    fn uncompact_floor_error(cell: &Self::Cell) -> PyErr {
        uncompact_floor_error(GridKind::H3Cell, "resolution", cell.cell)
    }

    fn with_compacted_cells(&self, cells: Vec<Self::Cell>) -> Self {
        self.with_cells_depth(cells, self.depth)
    }

    fn with_uncompacted_cells(&self, cells: Vec<Self::Cell>, depth: u8) -> Self {
        self.with_cells_depth(cells, CellDepth::Uniform(depth))
    }

    fn with_parent_cells(&self, cells: Vec<Self::Cell>, _floor: u8) -> Self {
        self.with_cells_depth(
            h3_cell_vec(cells.into_iter().map(|cell| cell.cell).collect()),
            self.depth,
        )
    }
}

grid_coverage_common_pymethods! {
    impl PyH3Coverage {
        this: coverage,
        kind: GridKind::H3Cell,
        iter: PyH3CoverageIter,
        cell_array: py_h3_cell_array,
        parse_cell: h3_cell_index,
        parsed_key: |cell| u64::from(cell),
        interior_doc: "Cells certified entirely inside the source geometry.\n\nWith ``boundary_cells`` this is the rule-independent classification of the covering: any point in an interior cell is inside the area, no geometry test needed. Render these solid and ``boundary_cells`` outlined for a faithful core-vs-fringe picture.\n\nReturns\n-------\nCellArray of H3Cell",
        interior_cells: { coverage.membership.partition.interior().cell_array(GridKind::H3Cell) },
        boundary_doc: "Cells partially overlapping the source geometry (the fringe where cell membership cannot answer the geometry question).\n\nReturns\n-------\nCellArray of H3Cell",
        boundary_cells: {
            coverage.membership.partition.boundary().cell_array(GridKind::H3Cell)
        },
        depth_fields: [depth],
        hash_depth: (coverage.depth,),
        cell_hash_key: |cell| { u64::from(cell.cell) },
        explain_grid: "h3",
        explain_depth: { coverage.depth.explain("resolution") },
        explain_cells: "cells",
        explain_interior_len: { coverage.membership.partition.interior_len() },
        explain_outer_len: { coverage.membership.partition.outer_len() },
        to_polygon_doc: "Dissolve the coverage into one outline geometry.\n\nShared cell edges are removed, so the result is the coverage's region as one geometry (the ``cellsToMultiPolygon`` operation), not one polygon per cell like ``coverage.cells.polygon``. Mixed resolutions dissolve too, so ``coverage.compact().to_polygon()`` works directly. Disjoint covered regions return a `MultiPolygon`.\n\nReturns\n-------\n`Polygon` or `MultiPolygon`\n    The dissolved region, tagged ``EPSG:4326``.\n\nRaises\n------\nGeometryError\n    If the coverage is empty.",
        to_polygon: { h3_dissolve_sorted(coverage.cells.iter().map(|cell| cell.cell).collect()) },
        reduce_unpickle: "_unpickle_h3_coverage",
        reduce_args: {
            (
                Typed(coverage.geometry.clone()),
                coverage.cells
                    .iter()
                    .map(|cell| u64::from(cell.cell))
                    .collect::<Vec<_>>(),
                coverage.cell_rule.token(),
                // Factory partition depth (recompute key).
                u8::from(coverage.membership.resolution()),
                // Visible depth when empty (cannot be inferred from cells);
                // nonempty coverings restore depth from the cell set.
                if coverage.cells.is_empty() {
                    coverage.depth.uniform_level()
                } else {
                    None
                },
                // Factory max_cells budget for bounded unpickle recompute (D07).
                coverage.max_cells,
            )
        },
        repr: {
            format!(
                "<H3Coverage {} cell_rule={} cells={}>",
                coverage.depth.explain("resolution"),
                coverage.cell_rule.token(),
                coverage.cells.len()
            )
        },
        index_error: "H3 coverage index out of range",
    }
}

fn h3_cell_depth(cells: &CoverageCells<PyH3Cell>, fallback: CellDepth) -> CellDepth {
    CellDepth::from_levels(cells.iter().map(|cell| cell.cell.resolution().into()))
        .unwrap_or(fallback)
}

impl PyH3Coverage {
    fn with_cells_depth(&self, cells: Vec<PyH3Cell>, fallback: CellDepth) -> Self {
        let cells = CoverageCells::from_cells(cells);
        let depth = h3_cell_depth(&cells, fallback);
        Self {
            geometry: self.geometry.clone(),
            cells,
            cell_rule: self.cell_rule,
            depth,
            membership: self.membership.clone(),
            max_cells: self.max_cells,
        }
    }
}
