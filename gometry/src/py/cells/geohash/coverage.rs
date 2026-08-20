//! Internal geohash coverer adapter. Cell arrays are returned directly.

use crate::grid::geohash::{self as kernel, Geohash};
use crate::py::cells::coverage_ops::{
    RectCoverSpec, build_rect_coverage_ids, parse_max_cells, rect_cell_array_for,
};
use crate::py::cells::geohash::cell::parse_geohash_precision;
use crate::py::cells::{
    Bound, CellRule, GridKind, Py, PyAny, PyCellArray, PyResult, Python, grid_cover_dispatch,
    pyfunction,
};

struct GeohashCoverSpec;
impl RectCoverSpec for GeohashCoverSpec {
    type Cell = Geohash;
    const KIND: GridKind = GridKind::GeohashCell;
    fn roots() -> Vec<Geohash> {
        kernel::roots()
    }
    fn id(cell: &Geohash) -> u64 {
        cell.identity_key()
    }
    fn parse_depth(value: &Bound<'_, PyAny>) -> PyResult<u8> {
        parse_geohash_precision(value)
    }
    fn coverage_label() -> &'static str {
        "geohash"
    }
}
pub(super) fn geohash_cell_array(cells: impl IntoIterator<Item = Geohash>) -> PyCellArray {
    rect_cell_array_for::<GeohashCoverSpec>(cells)
}

/// Cover a geometry with geohash cells at a fixed precision.
///
/// The result is a ``CellArray`` for scalar input or ``Groups[CellArray]`` for
/// array input, with geohash cells selected by ``cell_rule``.
/// Keep the source geometry separately and use the free ``gm.*`` predicates
/// for exact geometry questions.
///
/// Parameters
/// ----------
/// geom : Geometry or GeometryArray
///     Geometry to cover (WGS84 lon/lat or projected). A scalar returns a
///     flat cell array; an array returns one grouped cell row per input
///     geometry, preserving source-row association.
///
/// precision : int
///     Geohash precision (``1``-``12``; finer at higher values).
///
/// cell_rule : {'center', 'within', 'overlap', 'bbox'}, default 'overlap'
///     Which cells to materialize. ``'center'`` selects cells whose center is
///     inside; ``'within'`` selects cells entirely inside; ``'overlap'``
///     selects every cell touching the geometry; and ``'bbox'`` selects cells
///     whose bounding box overlaps. For geohash cells, ``'bbox'`` and
///     ``'overlap'`` are equivalent. The rule never affects exact predicates.
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
///     If the geometry, precision, ``cell_rule``, or ``max_cells`` is invalid,
///     or if the pre-``within``-filter candidate count would exceed
///     ``max_cells``.
/// InvalidGeometryError
///     If the geometry is empty.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> point = gm.Point(1, 1)
/// >>> cells = gm.geohash_cover(point, precision=1)
/// >>> len(cells)
/// 1
///
#[pyfunction]
#[pyo3(
    signature = (geom, precision, *, cell_rule = CellRule::Overlap, max_cells = Some(1_000_000)),
    text_signature = "(geom, precision, *, cell_rule='overlap', max_cells=1000000)"
)]
pub(super) fn geohash_cover(
    py: Python<'_>,
    geom: &Bound<'_, PyAny>,
    precision: &Bound<'_, PyAny>,
    cell_rule: CellRule,
    max_cells: Option<i64>,
) -> PyResult<Py<PyAny>> {
    let max_cells = parse_max_cells(max_cells)?;
    grid_cover_dispatch(
        py,
        geom,
        GridKind::GeohashCell,
        max_cells,
        |geometry, effective_max_cells| {
            build_rect_coverage_ids::<GeohashCoverSpec>(
                geometry,
                precision,
                cell_rule,
                effective_max_cells,
            )
        },
    )
}
