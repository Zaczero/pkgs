//! Generic cell `#[pymethods]` backing functions — written once per `GridCell`.

use pyo3::IntoPyObjectExt as _;
use pyo3::basic::CompareOp;
use pyo3::prelude::*;

use crate::grid::cell::{CellPickleArg, GridCell};
use crate::py::cells::{GridKind, PyCellArray, lonlat_point_geometry};
use crate::py::errors::GeometryError;
use crate::{PyGeometry, Typed};

pub(crate) fn cell_center<G: GridCell>(cell: G) -> Typed {
    Typed(lonlat_point_geometry(cell.center_point()))
}

pub(crate) fn cell_boundary<G: GridCell>(cell: G) -> PyResult<Typed> {
    Ok(Typed(PyGeometry::wgs84(cell.boundary_shape()?)))
}

pub(crate) fn parse_explicit_depth<P>(
    depth: Option<&Bound<'_, PyAny>>,
    parse_depth: P,
) -> PyResult<Option<u8>>
where
    P: FnOnce(&Bound<'_, PyAny>) -> PyResult<u8>,
{
    match depth {
        Some(value) if !value.is_none() => parse_depth(value).map(Some),
        _ => Ok(None),
    }
}

/// Resolve the target depth for parent walks (explicit depth, or one coarser).
fn parent_target_depth<G: GridCell>(cell: G, depth: Option<u8>) -> PyResult<u8> {
    match depth {
        Some(target) => Ok(target),
        None => cell
            .depth()
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
fn children_target_depth<G: GridCell>(cell: G, depth: Option<u8>) -> PyResult<Option<u8>> {
    let current = cell.depth();
    match depth {
        Some(target) => {
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
        _ => Ok(Some(current.wrapping_add(1))),
    }
}

pub(crate) fn cell_parent<G, P>(
    cell: G,
    depth: Option<&Bound<'_, PyAny>>,
    parse_depth: P,
) -> PyResult<G>
where
    G: GridCell,
    P: FnOnce(&Bound<'_, PyAny>) -> PyResult<u8>,
{
    let depth = parse_explicit_depth(depth, parse_depth)?;
    cell_parent_at_depth(cell, depth)
}

pub(crate) fn cell_parent_at_depth<G: GridCell>(cell: G, depth: Option<u8>) -> PyResult<G> {
    let target = parent_target_depth(cell, depth)?;
    cell.parent_at(target).ok_or_else(|| {
        GeometryError::new_err(format!(
            "parent {} must be <= cell {}",
            G::DEPTH_NAME,
            G::DEPTH_NAME
        ))
    })
}

pub(crate) fn cell_neighbors_array<G: GridCell>(kind: GridKind, cell: G) -> PyCellArray {
    cell_array_from_keys(kind, cell.neighbors())
}

pub(crate) fn cell_descendant_count<G, P>(
    cell: G,
    depth: Option<&Bound<'_, PyAny>>,
    parse_depth: P,
) -> PyResult<u64>
where
    G: GridCell,
    P: FnOnce(&Bound<'_, PyAny>) -> PyResult<u8>,
{
    let depth = parse_explicit_depth(depth, parse_depth)?;
    cell_descendant_count_at_depth(cell, depth)
}

pub(crate) fn cell_descendant_count_at_depth<G: GridCell>(
    cell: G,
    depth: Option<u8>,
) -> PyResult<u64> {
    let Some(target) = children_target_depth(cell, depth)? else {
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
    let ids = cells.into_iter().map(GridCell::hash_key).collect();
    PyCellArray::from_ids(kind, ids)
}
