//! Discrete global grid `PyO3` surface — grid factories, cells, and their
//! `CellArray`/`Groups` results. Factories derive cell keys from caller-owned
//! source geometry; exact membership uses free `covers`/`contains`/`intersects`
//! predicates.

use pyo3::prelude::*;
use pyo3::types::PyModule;

use crate::geometry::{Point, Shape};
use crate::py::errors::{GeometryError, integer_parameter_error, parse_error};
use crate::py::support::mark_sequence_flag;
use crate::py::vectors::Groups;
use crate::{
    H3_MAX_RESOLUTION, PyCoordinates, PyGeometry, PyGeometryArray, PyGeometryParts, Typed,
    lonlat_shape, py_i64_required,
};

mod array;
mod array_iter;
mod cell_ops;
mod construct;
mod coverage_ops;
mod engine;
mod grid_kind;
#[macro_use]
mod pymethod_macros;
mod geohash;
mod h3;
mod s2;
mod tiles;

pub(crate) use array::PyCellArray;
pub(crate) use array_iter::PyCellArrayIter;
pub(crate) use construct::{
    construct_geohash_cell, construct_h3_cell, construct_s2_cell, construct_tile,
    dispatch_grid_cell_array,
};
pub(super) use engine::grid_cover_dispatch;
pub(crate) use engine::grid_lonlat_points;
use engine::{
    bounding_query_bounds, cell_items, cell_limit_err, coverage_to_polygon, lonlat_point_geometry,
    rect_cells_to_polygon, uncompact_budget_err,
};
pub(crate) use grid_kind::{GridKind, uncompact_floor_error};
pub(crate) use h3::{PyH3EdgeArray, PyH3VertexArray};

pub(crate) fn checked_depth(
    value: i64,
    noun: &str,
    param: &str,
    min: i64,
    max: i64,
) -> PyResult<u8> {
    if (min..=max).contains(&value) {
        return Ok(value as u8);
    }
    Err(integer_parameter_error(
        format!("{noun} must be between {min} and {max}, got {value}"),
        param,
        value,
    ))
}

crate::tokens::token_enum! {
    /// Which cells of the grid belong to a coverage — the tiling rule, ordered
    /// strictest (fewest cells) to loosest (most). The SAME four modes apply
    /// identically to every grid system (H3, S2, geohash, tiles).
    pub(crate) enum CellRule("cell_rule", token = none, param = "cell_rule") {
        /// The cell's CENTER lies inside the geometry — uniquely assigns each
        /// cell (adjacent geometries never share cells), but may overshoot the
        /// edge and leave gaps.
        Center = "center",
        /// The cell lies ENTIRELY within the geometry — no overshoot, undercovers.
        Within = "within",
        /// The cell OVERLAPS the geometry at all — complete coverage, overshoots.
        Overlap = "overlap",
        /// The cell's bounding box overlaps the geometry — loosest and fastest.
        /// For rectangular grids (geohash, tiles) a cell IS its bbox, so this
        /// coincides with `overlap`.
        Bbox = "bbox",
    }
}

crate::tokens::token_from_pyobject!(CellRule);

/// Register every cell-system surface on the module.
pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    h3::register(m)?;
    s2::register_s2(m)?;
    geohash::register(m)?;
    tiles::register(m)?;
    crate::add_functions!(m; array::_unpickle_cell_array);
    crate::add_classes!(m; PyCellArray, PyCellArrayIter);
    mark_sequence_flag::<PyGeometryArray>(m);
    mark_sequence_flag::<PyCellArray>(m);
    mark_sequence_flag::<Groups>(m);
    mark_sequence_flag::<PyGeometryParts>(m);
    mark_sequence_flag::<PyCoordinates>(m);
    mark_sequence_flag::<PyH3VertexArray>(m);
    mark_sequence_flag::<PyH3EdgeArray>(m);
    Ok(())
}
