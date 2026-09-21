//! Internal geohash coverer adapter. Cell arrays are returned directly.

use crate::grid::geohash::{self as kernel, Geohash};
use crate::py::cells::coverage_ops::{
    RectCoverSpec, build_rect_coverage_ids, coverage_factory_shapes, parse_max_cells,
    rect_cell_array_for,
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
/// cell_rule : {'within', 'center', 'overlap', 'bbox'}, default 'overlap'
///     Which cells to materialize. ``'within'`` selects cells entirely inside;
///     ``'center'`` selects cells whose center is inside; ``'overlap'``
///     selects every cell touching the geometry; and ``'bbox'`` selects every
///     cell meeting the geometry's envelope (one window per part, split at the
///     antimeridian). The rule never affects exact predicates.
///
/// max_cells : int or None, default 1000000
///     Hard cap on returned cells and retained result allocation. Discarded
///     candidates do not consume it; array calls apply it cumulatively across
///     groups. Pass ``None`` for unlimited output (bounded only by memory).
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
///     or if the returned coverage would exceed ``max_cells``.
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
    signature = (geom, precision, *, cell_rule = CellRule::Overlap, max_cells = Some(crate::grid::GRID_MAX_CELLS)),
    text_signature = "(geom, precision, *, cell_rule='overlap', max_cells=1000000)"
)]
pub(super) fn geohash_cover(
    py: Python<'_>,
    geom: &Bound<'_, PyAny>,
    precision: &Bound<'_, PyAny>,
    cell_rule: CellRule,
    #[pyo3(from_py_with = crate::strict_optional_i64)] max_cells: Option<i64>,
) -> PyResult<Py<PyAny>> {
    let max_cells = parse_max_cells(max_cells)?;
    let precision = parse_geohash_precision(precision)?;
    grid_cover_dispatch(
        py,
        geom,
        GridKind::GeohashCell,
        max_cells,
        |geometry, effective_max_cells| {
            let cover_shape = coverage_factory_shapes(geometry, "geohash")?;
            build_rect_coverage_ids::<GeohashCoverSpec>(
                geometry,
                &cover_shape,
                precision,
                cell_rule,
                effective_max_cells,
            )
        },
    )
}
