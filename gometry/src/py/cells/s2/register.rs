use std::sync::Arc;

use super::*;
use crate::grid::s2::cell::Cell as S2GeomCell;
use crate::grid::s2::coverer::Coverer;
use crate::py::cells::coverage_ops::{
    self, CoverageCells, CoveragePartition, coverage_factory_shapes,
};

/// Build an exact-classified S2 coverage of `geometry` (the
/// ``s2_cover(...)`` backend).
pub(crate) fn build_coverage(
    geometry: &PyGeometry,
    level: Option<&Bound<'_, PyAny>>,
    max_cells: Option<i64>,
    target_cells: i64,
    min_level: Option<&Bound<'_, PyAny>>,
    max_level: Option<&Bound<'_, PyAny>>,
    level_mod: i64,
    cell_rule: CellRule,
) -> PyResult<PyS2Coverage> {
    let budget = parse_s2_level_budget(
        level,
        max_cells,
        target_cells,
        min_level,
        max_level,
        level_mod,
    )?;
    let (membership_geometry, cover_shape) = coverage_factory_shapes(geometry, "S2")?;
    let covering = Coverer {
        min_level: budget.min_level,
        max_level: budget.max_level,
        level_mod: budget.level_mod,
        max_cells: budget.max_cells,
        target_cells: budget.target_cells,
    }
    .cover(&cover_shape)
    .map_err(coverage_ops::cover_budget_err)?;
    let bbox_visible = if cell_rule == CellRule::Bbox {
        let bounds = cover_shape
            .bounds()
            .ok_or_else(|| coverage_ops::empty_coverage_err("S2"))?;
        let bounds_shape = bounds_query_shape(bounds)?;
        Some(
            Coverer {
                min_level: budget.min_level,
                max_level: budget.max_level,
                level_mod: budget.level_mod,
                max_cells: budget.max_cells,
                target_cells: budget.target_cells,
            }
            .cover(&bounds_shape)
            .map_err(coverage_ops::cover_budget_err)?
            .outer(),
        )
    } else {
        None
    };
    let partition = CoveragePartition::from_sorted_tagged(
        covering
            .into_cells()
            .into_iter()
            .map(|(cell, interior)| (PyS2Cell { cell }, interior)),
    );
    let cells = match cell_rule {
        CellRule::Overlap => partition.all(),
        CellRule::Bbox => CoverageCells::from_cells(
            bbox_visible
                .expect("bbox cells were built")
                .into_iter()
                .map(|cell| PyS2Cell { cell })
                .collect(),
        ),
        CellRule::Within => partition.interior(),
        // Center probes the cover working shape (split-normalized).
        CellRule::Center => partition.select(|cell| {
            cover_shape.covers_point(S2GeomCell::from_id(cell.cell).center_lonlat())
        }),
    };
    let membership = S2Membership { partition };
    Ok(PyS2Coverage {
        geometry: membership_geometry,
        cells,
        cell_rule,
        min_level: budget.min_level,
        max_level: budget.max_level,
        level_mod: budget.level_mod,
        max_cells: budget.max_cells,
        target_cells: budget.target_cells,
        membership: Arc::new(membership),
    })
}

/// Register the S2 classes, flat functions, and pickle rebuilder.
pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    crate::add_functions!(m;
        s2_cells, s2_cover, s2_union,
        s2_intersection, s2_difference, s2_bounding_cell,
        _unpickle_s2_cell, _unpickle_s2_coverage,
    );
    crate::add_classes!(m; PyS2Coverage, PyS2CoverageIter, PyS2Cell);
    Ok(())
}
