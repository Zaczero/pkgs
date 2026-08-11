#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use pyo3::{PyAny, PyResult};

use crate::geometry::{CoordSeq, LineSeq, Point, Shape};
use crate::grid::s2::projection::MAX_LEVEL as S2_MAX_LEVEL;
use crate::py::cells::{Bound, PyAnyMethods as _};
use crate::py::errors::{GeometryError, integer_parameter_error};
use crate::py_i64_required;

pub(crate) struct S2LevelBudget {
    pub(super) min_level: u8,
    pub(super) max_level: u8,
    pub(super) level_mod: u8,
    /// Hard emission cap. `None` = unlimited.
    pub(super) max_cells: Option<usize>,
    /// Adaptive refinement target.
    pub(super) target_cells: usize,
}
pub(crate) fn parse_s2_level_value(value: i64) -> PyResult<u8> {
    crate::py::cells::checked_depth(value, "S2 level", "level", 0, i64::from(S2_MAX_LEVEL))
}

pub(crate) fn parse_s2_min_level_value(value: i64) -> PyResult<u8> {
    crate::py::cells::checked_depth(
        value,
        "S2 min_level",
        "min_level",
        0,
        i64::from(S2_MAX_LEVEL),
    )
}

pub(crate) fn parse_s2_level(value: &Bound<'_, PyAny>) -> PyResult<u8> {
    parse_s2_level_value(py_i64_required("S2 level", value)?)
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
pub(crate) fn parse_s2_level_budget(
    level: Option<&Bound<'_, PyAny>>,
    max_cells: Option<i64>,
    target_cells: i64,
    min_level: Option<&Bound<'_, PyAny>>,
    max_level: Option<&Bound<'_, PyAny>>,
    level_mod: i64,
) -> PyResult<S2LevelBudget> {
    let level = parse_optional_s2_level(level)?;
    let min_level = parse_optional_s2_level(min_level)?;
    let max_level = parse_optional_s2_level(max_level)?;
    if level.is_some() && (min_level.is_some() || max_level.is_some()) {
        return Err(GeometryError::new_err(
            "level cannot be combined with min_level or max_level",
        ));
    }
    let min_level = min_level.or(level).unwrap_or(0);
    let max_level = max_level.or(level).unwrap_or(S2_MAX_LEVEL);
    if min_level > max_level {
        return Err(GeometryError::new_err("min_level must be <= max_level"));
    }
    Ok(S2LevelBudget {
        min_level,
        max_level,
        level_mod: validate_s2_level_mod(level_mod)?,
        max_cells: crate::py::cells::coverage_ops::parse_max_cells(max_cells)?,
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

pub(crate) fn parse_optional_s2_level(value: Option<&Bound<'_, PyAny>>) -> PyResult<Option<u8>> {
    match value {
        None => Ok(None),
        Some(value) if value.is_none() => Ok(None),
        Some(value) => parse_s2_level(value).map(Some),
    }
}

pub(crate) fn bounds_query_shape(bounds: crate::geometry::Bounds) -> PyResult<Shape> {
    // Exact identity on purpose: only literally-degenerate bounds collapse
    // to lower-dimensional query shapes; near-degenerate boxes stay boxes.
    #[expect(clippy::float_cmp, reason = "exact degenerate-bounds dispatch")]
    Ok(
        if bounds.minx() == bounds.maxx() && bounds.miny() == bounds.maxy() {
            Shape::Point(Point::new_unchecked_xy(bounds.minx(), bounds.miny()))
        } else if bounds.minx() == bounds.maxx() || bounds.miny() == bounds.maxy() {
            Shape::LineString(
                LineSeq::try_new(CoordSeq::from(vec![
                    Point::new_unchecked_xy(bounds.minx(), bounds.miny()),
                    Point::new_unchecked_xy(bounds.maxx(), bounds.maxy()),
                ]))
                .expect("degenerate bounds line has two vertices"),
            )
        } else {
            Shape::Polygon(crate::box_polygon(
                bounds.minx(),
                bounds.miny(),
                bounds.maxx(),
                bounds.maxy(),
            )?)
        },
    )
}
