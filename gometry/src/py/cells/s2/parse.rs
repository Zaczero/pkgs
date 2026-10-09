use pyo3::{PyAny, PyResult};

use crate::grid::s2::cellid::S2Level;
use crate::grid::s2::projection::MAX_LEVEL as S2_MAX_LEVEL;
use crate::py::cells;
use crate::py::cells::{Bound, coverage_ops};
use crate::py::errors::{GeometryError, integer_parameter_error};
use crate::py_i64_required;

#[derive(Clone, Copy)]
pub(crate) struct S2LevelBudget {
    pub(super) min_level: u8,
    pub(super) max_level: u8,
    pub(super) level_mod: u8,
    /// Hard emission cap. `None` = unlimited.
    pub(super) max_cells: Option<usize>,
    /// Adaptive refinement target.
    pub(super) target_cells: usize,
}
pub(crate) fn parse_s2_level_value(value: i64) -> PyResult<S2Level> {
    u8::try_from(value)
        .ok()
        .and_then(S2Level::try_new)
        .ok_or_else(|| {
            integer_parameter_error(
                format!("S2 level must be between 0 and {S2_MAX_LEVEL}, got {value}"),
                "level",
                value,
            )
        })
}

pub(crate) fn parse_s2_min_level_value(value: i64) -> PyResult<u8> {
    cells::checked_depth(
        value,
        "S2 min_level",
        "min_level",
        0,
        i64::from(S2_MAX_LEVEL),
    )
}

/// An S2 level argument as the validated level type.
pub(crate) fn parse_s2_level_validated(value: &Bound<'_, PyAny>) -> PyResult<S2Level> {
    parse_s2_level_value(py_i64_required("S2 level", value)?)
}

/// An S2 level argument as the plain depth the grid-generic surface uses.
pub(crate) fn parse_s2_level(value: &Bound<'_, PyAny>) -> PyResult<u8> {
    parse_s2_level_validated(value).map(S2Level::get)
}

pub(crate) fn validate_s2_level_mod(level_mod: i64) -> PyResult<u8> {
    if !(1..=3).contains(&level_mod) {
        return Err(integer_parameter_error(
            format!("level_mod must be between 1 and 3, got {level_mod}"),
            "level_mod",
            level_mod,
        ));
    }
    Ok(level_mod as u8)
}
pub(crate) fn parse_s2_fixed_budget(level: i64, max_cells: Option<i64>) -> PyResult<S2LevelBudget> {
    let level = parse_s2_level_value(level)?.get();
    Ok(S2LevelBudget {
        min_level: level,
        max_level: level,
        level_mod: 1,
        max_cells: coverage_ops::parse_max_cells(max_cells)?,
        target_cells: 8,
    })
}

pub(crate) fn parse_s2_adaptive_budget(
    target_cells: i64,
    min_level: i64,
    max_level: i64,
    level_mod: i64,
    max_cells: Option<i64>,
) -> PyResult<S2LevelBudget> {
    let min_level = parse_s2_level_value(min_level)?.get();
    let max_level = parse_s2_level_value(max_level)?.get();
    if min_level > max_level {
        return Err(GeometryError::new_err("min_level must be <= max_level"));
    }
    Ok(S2LevelBudget {
        min_level,
        max_level,
        level_mod: validate_s2_level_mod(level_mod)?,
        max_cells: coverage_ops::parse_max_cells(max_cells)?,
        target_cells: parse_s2_target_cells(target_cells)?,
    })
}

pub(crate) fn parse_s2_target_cells(target_cells: i64) -> PyResult<usize> {
    if target_cells <= 0 {
        return Err(integer_parameter_error(
            format!("target_cells must be greater than zero, got {target_cells}"),
            "target_cells",
            target_cells,
        ));
    }
    usize::try_from(target_cells).map_err(|_| {
        integer_parameter_error("target_cells is too large", "target_cells", target_cells)
    })
}
