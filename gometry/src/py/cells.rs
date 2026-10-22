//! Discrete global grid `PyO3` surface — grid factories, cells, and their
//! `CellArray`/`Groups` results. Factories derive cell keys from caller-owned
//! source geometry; exact membership uses free `covers`/`contains`/`intersects`
//! predicates.

use pyo3::exceptions::PyOverflowError;
use pyo3::prelude::*;
use pyo3::types::PyModule;

use crate::geometry::{Point, Shape};
use crate::py::errors::{GeometryError, integer_parameter_error, parse_error};
use crate::py::support::initialize_sequence_type;
use crate::py::vectors::Groups;
use crate::{
    H3_MAX_RESOLUTION, PyCoordinates, PyGeometry, PyGeometryArray, PyGeometryParts, Typed,
    py_i64_required,
};

/// Expands the entry match at each monomorphic call site so the hot loop
/// keeps one hash lookup per row.
macro_rules! first_seen_slot {
    ($slot_of:ident, $unique_ids:ident, $id:ident $(, $on_first_seen:block)?) => {{
        match $slot_of.entry($id) {
            std::collections::hash_map::Entry::Occupied(entry) => *entry.get(),
            std::collections::hash_map::Entry::Vacant(entry) => {
                let slot = $unique_ids.len();
                entry.insert(slot);
                $unique_ids.push($id);
                $($on_first_seen)?
                slot
            },
        }
    }};
}
pub(crate) use first_seen_slot;

/// Expands the entry match and count update at each monomorphic call site,
/// keeping one hash lookup per hot-loop row. Its `?` propagates
/// `FactorizationError` from the caller.
macro_rules! record_first_seen_count {
    ($slot_of:ident, $unique_ids:ident, $counts:ident, $id:ident) => {{
        let slot = $crate::py::cells::first_seen_slot!($slot_of, $unique_ids, $id, {
            $counts.push(0_i64);
        });
        let count = $counts
            .get_mut(slot)
            .expect("first-seen id slot has a count");
        *count = (*count)
            .checked_add(1)
            .ok_or(FactorizationError::CountOverflow)?;
    }};
}
pub(crate) use record_first_seen_count;

mod array;
mod array_iter;
mod cell_array_error;
mod cell_ops;
mod construct;
mod coverage_ops;
mod engine;
mod grid_kind;
mod grid_outline;
mod input;
#[macro_use]
mod pymethod_macros;
mod geohash;
mod h3;
mod s2;
mod tiles;

pub(crate) use array::PyCellArray;
pub(crate) use array_iter::PyCellArrayIter;
pub(crate) use cell_array_error::CellArrayError;
pub(crate) use construct::{
    construct_geohash_cell, construct_h3_cell, construct_s2_cell, construct_tile,
    dispatch_grid_cell_array,
};
#[cfg(test)]
pub(crate) use coverage_ops::GridCoverError;
pub(super) use engine::grid_cover_dispatch;
use engine::{
    bounding_query_bounds, bounding_query_region, cell_items, coverage_to_polygon,
    lonlat_point_geometry, rect_cells_to_polygon,
};
pub(crate) use grid_kind::GridKind;
pub(crate) use h3::{PyH3EdgeArray, PyH3VertexArray};

#[derive(Debug)]
enum FactorizationError {
    CountOverflow,
    CodeOverflow,
}

/// Callers first normalize dtype and alignment with `require_aligned_numpy`.
/// They must also keep the source quiescent; this readonly borrow does not
/// synchronize native writers.
fn copy_numeric_ids<T>(
    ids: &numpy::PyReadonlyArrayDyn<'_, T>,
    invalid_value: impl Fn() -> pyo3::PyErr,
) -> PyResult<Vec<u64>>
where
    T: numpy::Element + Copy + TryInto<u64>,
{
    let view = ids.as_array();
    let mut output = Vec::with_capacity(view.len());
    {
        let mut append = |value: T| -> PyResult<()> {
            output.push(value.try_into().map_err(|_| invalid_value())?);
            Ok(())
        };
        if let Ok(values) = ids.as_slice() {
            for &value in values {
                append(value)?;
            }
        } else if view.ndim() == 1 {
            let view = view
                .into_dimensionality::<numpy::Ix1>()
                .expect("one-dimensional numeric ID view keeps one dimension");
            for &value in &view {
                append(value)?;
            }
        } else {
            for &value in &view {
                append(value)?;
            }
        }
    }
    Ok(output)
}

impl FactorizationError {
    fn into_py_err(self, count_overflow: &'static str, code_overflow: &'static str) -> PyErr {
        match self {
            Self::CountOverflow => PyOverflowError::new_err(count_overflow),
            Self::CodeOverflow => PyOverflowError::new_err(code_overflow),
        }
    }
}

fn sort_value_counts(uniques: &[u64], counts: &[i64]) -> (Vec<u64>, Vec<i64>) {
    let mut order: Vec<_> = (0..uniques.len()).collect();
    order.sort_unstable_by(|left, right| {
        let left_count = counts
            .get(*left)
            .expect("value-count slot matches its count");
        let right_count = counts
            .get(*right)
            .expect("value-count slot matches its count");
        right_count.cmp(left_count).then_with(|| left.cmp(right))
    });
    let mut sorted_uniques = Vec::with_capacity(order.len());
    let mut sorted_counts = Vec::with_capacity(order.len());
    for slot in order {
        sorted_uniques.push(*uniques.get(slot).expect("value-count slot matches its id"));
        sorted_counts.push(
            *counts
                .get(slot)
                .expect("value-count slot matches its count"),
        );
    }
    (sorted_uniques, sorted_counts)
}

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
        /// The cell meets the geometry's ENVELOPE — one window per part, split
        /// at the antimeridian. Loosest and fastest: no shape test decides it.
        Bbox = "bbox",
    }
}

crate::tokens::token_from_pyobject!(CellRule);

/// Register every cell-system surface on the module.
pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    crate::add_classes!(m;
        crate::py::circles::PyCircle,
        crate::py::circles::PyCircleArray,
        crate::py::circles::PyCircleArrayIter
    );
    initialize_sequence_type::<crate::py::circles::PyCircleArray>(m)?;
    h3::register(m)?;
    s2::register_s2(m)?;
    geohash::register(m)?;
    tiles::register(m)?;
    crate::add_functions!(m; array::_unpickle_cell_array_v1);
    crate::add_classes!(m; PyCellArray, PyCellArrayIter);
    initialize_sequence_type::<PyGeometryArray>(m)?;
    initialize_sequence_type::<PyCellArray>(m)?;
    initialize_sequence_type::<Groups>(m)?;
    initialize_sequence_type::<PyGeometryParts>(m)?;
    initialize_sequence_type::<PyCoordinates>(m)?;
    initialize_sequence_type::<PyH3VertexArray>(m)?;
    initialize_sequence_type::<PyH3EdgeArray>(m)?;
    Ok(())
}
