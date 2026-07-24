//! Generic cell `#[pymethods]` backing functions — written once per `GridCell`.

use pyo3::IntoPyObjectExt;
use pyo3::basic::CompareOp;
use pyo3::prelude::*;

use crate::grid::UncompactBudgetExceeded;
use crate::grid::cell::{CellPickleArg, GridCell};
use crate::py::cells::{GridKind, PyCellArray, lonlat_point_geometry};
use crate::py::errors::GeometryError;
use crate::{PyGeometry, Typed};

pub(crate) fn cell_center<G: GridCell>(cell: G) -> Typed {
    Typed(lonlat_point_geometry(cell.center_point()))
}

pub(crate) fn cell_boundary<G: GridCell>(cell: G) -> Typed {
    Typed(PyGeometry::wgs84(cell.boundary_shape()))
}

/// Resolve the target depth for parent walks (explicit depth, or one coarser).
fn parent_target_depth<G, P>(
    cell: G,
    depth: Option<&Bound<'_, PyAny>>,
    parse_depth: P,
) -> PyResult<u8>
where
    G: GridCell,
    P: Fn(&Bound<'_, PyAny>) -> PyResult<u8>,
{
    let current = cell.depth();
    match depth {
        Some(value) if !value.is_none() => parse_depth(value),
        _ => current
            .checked_sub(1)
            .filter(|target| (G::MIN_DEPTH..=G::MAX_DEPTH).contains(target))
            .ok_or_else(|| {
                GeometryError::new_err(format!("a minimum-{} cell has no parent", G::DEPTH_NAME))
            }),
    }
}

/// Resolve the target depth for children / descendant walks.
///
/// Returns `None` when the default walk is requested at max depth (empty / zero).
fn children_target_depth<G, P>(
    cell: G,
    depth: Option<&Bound<'_, PyAny>>,
    parse_depth: P,
) -> PyResult<Option<u8>>
where
    G: GridCell,
    P: Fn(&Bound<'_, PyAny>) -> PyResult<u8>,
{
    let current = cell.depth();
    match depth {
        Some(value) if !value.is_none() => {
            let target = parse_depth(value)?;
            if target < current {
                return Err(GeometryError::new_err(format!(
                    "children {} must be >= cell {}",
                    G::DEPTH_NAME,
                    G::DEPTH_NAME
                )));
            }
            Ok(Some(target))
        },
        _ if current >= G::MAX_DEPTH => Ok(None),
        _ => Ok(Some(current + 1)),
    }
}

pub(crate) fn cell_parent<G, P>(
    cell: G,
    depth: Option<&Bound<'_, PyAny>>,
    parse_depth: P,
) -> PyResult<G>
where
    G: GridCell,
    P: Fn(&Bound<'_, PyAny>) -> PyResult<u8>,
{
    let target = parent_target_depth(cell, depth, parse_depth)?;
    cell.parent_at(target).ok_or_else(|| {
        GeometryError::new_err(format!(
            "parent {} must be <= cell {}",
            G::DEPTH_NAME,
            G::DEPTH_NAME
        ))
    })
}

pub(crate) fn cell_children_array<G, P>(
    kind: GridKind,
    cell: G,
    depth: Option<&Bound<'_, PyAny>>,
    parse_depth: P,
) -> PyResult<PyCellArray>
where
    G: GridCell,
    P: Fn(&Bound<'_, PyAny>) -> PyResult<u8>,
{
    Ok(cell_array_from_keys(
        kind,
        cell_children(cell, depth, parse_depth)?,
    ))
}

pub(crate) fn cell_neighbors_array<G: GridCell>(kind: GridKind, cell: G) -> PyCellArray {
    cell_array_from_keys(kind, cell.neighbors())
}

pub(crate) fn cell_children<G, P>(
    cell: G,
    depth: Option<&Bound<'_, PyAny>>,
    parse_depth: P,
) -> PyResult<Vec<G>>
where
    G: GridCell,
    P: Fn(&Bound<'_, PyAny>) -> PyResult<u8>,
{
    let Some(target) = children_target_depth(cell, depth, parse_depth)? else {
        return Ok(Vec::new());
    };
    cell.children_to(target)
        .map_err(|err: UncompactBudgetExceeded| GeometryError::new_err(err.to_string()))
}

pub(crate) fn cell_descendant_count<G, P>(
    cell: G,
    depth: Option<&Bound<'_, PyAny>>,
    parse_depth: P,
) -> PyResult<u64>
where
    G: GridCell,
    P: Fn(&Bound<'_, PyAny>) -> PyResult<u8>,
{
    let Some(target) = children_target_depth(cell, depth, parse_depth)? else {
        return Ok(0);
    };
    Ok(cell.descendant_count(target))
}

pub(crate) fn cell_contains<G, P>(
    cell: G,
    other: &Bound<'_, PyAny>,
    parse_cell: P,
) -> PyResult<bool>
where
    G: GridCell,
    P: Fn(&Bound<'_, PyAny>) -> PyResult<G>,
{
    Ok(cell.contains_cell(parse_cell(other)?))
}

pub(crate) fn cell_intersects<G, P>(
    cell: G,
    other: &Bound<'_, PyAny>,
    parse_cell: P,
) -> PyResult<bool>
where
    G: GridCell,
    P: Fn(&Bound<'_, PyAny>) -> PyResult<G>,
{
    let other = parse_cell(other)?;
    Ok(cell.contains_cell(other) || other.contains_cell(cell))
}

pub(crate) fn cell_hash<G: GridCell>(cell: G) -> u64 {
    cell.hash_key()
}

pub(crate) fn cell_richcmp<G: GridCell>(left: G, right: G, op: CompareOp) -> bool {
    op.matches(left.cmp(&right))
}

pub(crate) fn cell_reduce<G: GridCell>(
    cell: G,
    py: Python<'_>,
    unpickle: &str,
) -> PyResult<(Py<PyAny>, Py<PyAny>)> {
    let arg = match cell.pickle_arg() {
        CellPickleArg::U64(id) => (id,).into_py_any(py)?,
        CellPickleArg::Str(token) => (token,).into_py_any(py)?,
    };
    Ok((
        crate::gometry_lib_module(py)?.getattr(unpickle)?.unbind(),
        arg,
    ))
}

pub(crate) fn cell_array_from_keys<G: GridCell>(
    kind: GridKind,
    cells: impl IntoIterator<Item = G>,
) -> PyCellArray {
    let ids: Vec<u64> = cells.into_iter().map(GridCell::hash_key).collect();
    PyCellArray::from_trusted_ids(kind, ids)
}
