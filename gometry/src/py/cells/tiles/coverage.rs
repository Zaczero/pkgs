//! Internal tile coverer adapter. Cell arrays are returned directly.

use crate::grid::tile::{self as kernel, Tile, ensure_shape_in_tile_domain};
use crate::py::cells::coverage_ops::{
    RectCoverSpec, build_rect_coverage_ids, coverage_factory_shapes, parse_max_cells,
    rect_cell_array_for,
};
use crate::py::cells::tiles::cell::parse_tile_zoom;
use crate::py::cells::{
    Bound, CellRule, GridKind, Py, PyAny, PyCellArray, PyResult, Python, grid_cover_dispatch,
    pyfunction,
};
use crate::py::errors::InvalidGeometryError;

struct TileCoverSpec;
impl RectCoverSpec for TileCoverSpec {
    type Cell = Tile;
    const KIND: GridKind = GridKind::Tile;
    fn roots() -> Vec<Tile> {
        vec![kernel::root()]
    }
    fn id(cell: &Tile) -> u64 {
        cell.id()
    }
    fn parse_depth(value: &Bound<'_, PyAny>) -> PyResult<u8> {
        parse_tile_zoom(value)
    }
    fn coverage_label() -> &'static str {
        "tile"
    }
}
pub(super) fn tile_cell_array(cells: impl IntoIterator<Item = Tile>) -> PyCellArray {
    rect_cell_array_for::<TileCoverSpec>(cells)
}

/// Cover a geometry with Web Mercator tiles at a fixed zoom.
///
/// The result is a ``CellArray`` for scalar input or ``Groups[CellArray]`` for
/// array input, with tiles selected by ``cell_rule``. Keep the source geometry
/// separately and use the free ``gm.*`` predicates for exact geometry
/// questions.
///
/// Parameters
/// ----------
/// geom : Geometry or GeometryArray
///     Geometry to cover (WGS84 lon/lat or projected). A scalar returns a
///     flat cell array; an array returns one grouped cell row per input
///     geometry, preserving source-row association.
///
/// zoom : int
///     Tile zoom (``0``-``29``; finer at higher values).
///
/// cell_rule : {'center', 'within', 'overlap', 'bbox'}, default 'overlap'
///     Which cells to materialize. ``'center'`` selects cells whose center is
///     inside; ``'within'`` selects cells entirely inside; ``'overlap'``
///     selects every cell touching the geometry; and ``'bbox'`` selects cells
///     whose bounding box overlaps. For tiles, ``'bbox'`` and ``'overlap'``
///     are equivalent. The rule never affects exact predicates.
///
/// max_cells : int or None, default 1000000
///     Finite hard cap on candidate cells considered by the coverer before the
///     ``within`` filter. A ``within`` cover can yield fewer final cells while
///     raising when the pre-within-filter candidate count would exceed this
///     candidate budget. Pass
///     ``None`` for an unlimited cover.
///
/// Returns
/// -------
/// CellArray or Groups[CellArray]
///     A scalar returns a ``CellArray``; an array returns one ``CellArray``
///     group per input geometry.
///
/// Raises
/// ------
/// GeometryError
///     If the geometry, zoom, ``cell_rule``, or ``max_cells`` is invalid, or
///     if the pre-``within``-filter candidate count would exceed ``max_cells``.
/// InvalidGeometryError
///     If the geometry is empty or any latitude is outside the Web Mercator
///     domain (±85.05112878 degrees).
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> point = gm.Point(1, 1)
/// >>> cells = gm.tile_cover(point, zoom=0)
/// >>> len(cells)
/// 1
///
#[pyfunction]
#[pyo3(
    signature = (geom, zoom, *, cell_rule = CellRule::Overlap, max_cells = Some(1_000_000)),
    text_signature = "(geom, zoom, *, cell_rule='overlap', max_cells=1000000)"
)]
pub(super) fn tile_cover(
    py: Python<'_>,
    geom: &Bound<'_, PyAny>,
    zoom: &Bound<'_, PyAny>,
    cell_rule: CellRule,
    max_cells: Option<i64>,
) -> PyResult<Py<PyAny>> {
    let max_cells = parse_max_cells(max_cells)?;
    grid_cover_dispatch(
        py,
        geom,
        GridKind::Tile,
        max_cells,
        |geometry, effective_max_cells| {
            let (_, cover_shape, _) = coverage_factory_shapes(geometry, "Tile")?;
            if let Err(lat) = ensure_shape_in_tile_domain(&cover_shape) {
                return Err(InvalidGeometryError::new_err(format!(
                    "latitude {lat} is outside the Web Mercator domain ±85.05112878 degrees"
                )));
            }
            build_rect_coverage_ids::<TileCoverSpec>(geometry, zoom, cell_rule, effective_max_cells)
        },
    )
}
