#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! Discrete global grid `PyO3` surface — `Grid`, the H3/S2 coverages, and
//! cells. A coverage is an exact region predicate plus the cell keys that
//! accelerate and shard it: `cells` answer the cell question exactly (per
//! `cell_rule`), `covers`/`contains`/`intersects` answer the geometry
//! question exactly against the source geometry.

use std::sync::Arc;

use pyo3::IntoPyObjectExt;
use pyo3::prelude::*;
use pyo3::types::PyModule;

use crate::geometry::{Point, Shape};
use crate::py::errors::{GeometryError, integer_parameter_error, parse_error};
use crate::py::functions::predicate::{Predicate, topology_scalar_pair};
use crate::{
    H3_MAX_RESOLUTION, PyGeometry, Typed, exact_geometry, expected_geometry_or_array, lonlat_shape,
    py_i64_required,
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
pub(crate) use engine::grid_lonlat_points;
pub(super) use engine::*;
pub(crate) use grid_kind::{GridKind, uncompact_floor_error};

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
    pub(crate) enum CellRule("cell_rule", param = "cell_rule") {
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

/// The exact predicate a coverage membership method applies to candidates —
/// always against the source geometry, never the cells.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum CoveragePredicate {
    Contains,
    Covers,
    Intersects,
}

impl CoveragePredicate {
    fn test_data(
        self,
        geometry: &PyGeometry,
        source: &crate::geometry::ShapeData,
        candidate: &crate::geometry::ShapeData,
    ) -> bool {
        topology_scalar_pair(
            &self.predicate().spec(),
            source,
            candidate,
            crate::geometry::is_geographic_frame(&geometry.frame),
        )
    }

    /// Point membership — same geographic gate as free predicates (poles +
    /// antimeridian split-normalization), not a planar-only probe.
    fn test_point(
        self,
        geometry: &PyGeometry,
        source: &crate::geometry::ShapeData,
        point: Point,
    ) -> bool {
        topology_scalar_pair(
            &self.predicate().spec(),
            source,
            &crate::geometry::ShapeData::from(Shape::Point(point)),
            crate::geometry::is_geographic_frame(&geometry.frame),
        )
    }

    const fn includes_boundary_for_points(self) -> bool {
        match self {
            Self::Contains => false,
            Self::Covers | Self::Intersects => true,
        }
    }

    const fn predicate(self) -> Predicate {
        match self {
            Self::Contains => Predicate::Contains,
            Self::Covers => Predicate::Covers,
            Self::Intersects => Predicate::Intersects,
        }
    }
}

/// Register every cell-system surface on the module.
pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    h3::register(m)?;
    s2::register_s2(m)?;
    geohash::register(m)?;
    tiles::register(m)?;
    crate::add_functions!(m; array::_unpickle_cell_array);
    crate::add_classes!(m; PyCellArray, PyCellArrayIter);
    Ok(())
}
