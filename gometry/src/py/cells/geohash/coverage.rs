//! GeohashCoverage and the geohash-cover backend.

use pyo3::IntoPyObjectExt;
use pyo3::types::PyAny;

use super::super::*;
use super::cell::{
    PyGeohashCell, geohash_cell_arg, geohash_floor, parse_geohash_precision, parse_geohash_token,
};
use crate::Typed;
use crate::grid::geohash::{self as kernel, GEOHASH_MAX_PRECISION, Geohash};
use crate::py::cells::coverage_ops::{
    CoverageCells, build_rect_coverage_state, parse_max_cells, rect_cell_array_for,
    rect_cell_polygon, rect_coverage_cells, unpickle_rect_coverage_state,
};

/// Rebuild a pickled GeohashCoverage from its public fields (internal; see
/// ``GeohashCoverage.__reduce__``).
#[pyfunction]
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
/// >>> cov = gm.geohash_cover(p, precision=6)
/// >>> (len(cov.cells), cov.contains(p), cov.cells[0].token)
/// (1, True, '9q8yyk')
pub(super) fn _unpickle_geohash_coverage(
    geometry: &Bound<'_, PyAny>,
    cells: &Bound<'_, PyAny>,
    cell_rule: &str,
    factory_precision: u8,
    visible_depth: Option<u8>,
    max_cells: Option<i64>,
) -> PyResult<PyGeohashCoverage> {
    let decode = |tokens: Vec<String>| -> PyResult<Vec<Geohash>> {
        tokens
            .into_iter()
            .map(|token| parse_geohash_token(&token))
            .collect()
    };
    let parse_depth = |value: u8| -> PyResult<u8> {
        super::super::checked_depth(
            i64::from(value),
            "geohash precision",
            "precision",
            1,
            i64::from(GEOHASH_MAX_PRECISION),
        )
    };
    Ok(PyGeohashCoverage(unpickle_rect_coverage_state::<
        GeohashCoverSpec,
        PyGeohashCell,
        String,
        _,
        _,
    >(
        geometry,
        cells,
        cell_rule,
        factory_precision,
        visible_depth,
        max_cells,
        parse_depth,
        decode,
    )?))
}

rect_coverage_pyclass! {
    spec: GeohashCoverSpec,
    coverage: PyGeohashCoverage,
    cell: PyGeohashCell,
    kernel_cell: Geohash,
    kind: GridKind::GeohashCell,
    roots: { kernel::roots() },
    level: |cell| cell.precision,
    parse_depth: parse_geohash_precision,
    label: "geohash",
    class_name: "GeohashCoverage",
    class_doc: "A geohash covering of a geometry (the ``geohash_cover`` backend).\n\nReturned by ``geohash_cover(...)``: ``coverage.cells`` materializes\nthe cells selected by ``cell_rule`` at the chosen precision (join keys,\nbins, visualization), while ``covers``/``contains``/``intersects``\nanswer exactly against the source geometry, independent of the rule.",
    iter: PyGeohashCoverageIter,
    iter_name: "GeohashCoverageIterator",
    depth_getter: precision,
    depth_doc: "Uniform geohash precision of the covering's cells, or ``None`` for\nmixed precisions.\n\nReturns\n-------\nint or None",
}

grid_rect_coverage_common_pymethods! {
    impl PyGeohashCoverage {
        cell: Geohash,
        kind: GridKind::GeohashCell,
        kernel: kernel,
        cell_vec: geohash_cell_vec,
        depth: precision,
        depth_field: precision,
        depth_name: "precision",
        min_depth: min_precision,
        floor_default: 1,
        floor: geohash_floor,
        parse_depth: parse_geohash_precision,
        compact_doc: "Compact the cell set to its coarsest covering.\n\nParameters\n----------\nmin_precision : int, default 1\n    Coarsest precision compaction may produce.\n\nReturns\n-------\nGeohashCoverage\n\nExamples\n--------\n>>> import gometry as gm\n>>> p = gm.Point(-122.4194, 37.7749, crs=4326)\n>>> cov = gm.geohash_cover(p, precision=6)\n>>> len(cov.compact().cells) <= len(cov.cells)\nTrue\n",
        compact_text_signature: "($self, *, min_precision=1)",
        uncompact_doc: "Expand the cell set to a uniform precision.\n\nParameters\n----------\nprecision : int\n    Target precision (``1``-``12``); no coarser than any current cell.\n\nReturns\n-------\nGeohashCoverage\n\nExamples\n--------\n>>> import gometry as gm\n>>> p = gm.Point(-122.4194, 37.7749, crs=4326)\n>>> cov = gm.geohash_cover(p, precision=6)\n>>> len(cov.uncompact(7).cells) >= len(cov.cells)\nTrue\n",
        uncompact_text_signature: "($self, precision)",
        with_parents_doc: "Include parent cells down to a minimum precision.\n\nParameters\n----------\nmin_precision : int, default 1\n    Coarsest precision to add parents for.\n\nReturns\n-------\nGeohashCoverage\n\nExamples\n--------\n>>> import gometry as gm\n>>> p = gm.Point(-122.4194, 37.7749, crs=4326)\n>>> cov = gm.geohash_cover(p, precision=6)\n>>> len(cov.with_parents().cells) >= len(cov.cells)\nTrue\n",
        with_parents_text_signature: "($self, *, min_precision=1)",
        fine_token: |cell| { cell.cell.token() },
    }
}

grid_coverage_common_pymethods! {
    impl PyGeohashCoverage {
        this: coverage,
        kind: GridKind::GeohashCell,
        iter: PyGeohashCoverageIter,
        cell_array: py_geohash_cell_array,
        parse_cell: geohash_cell_arg,
        parsed_key: |cell| cell.identity_key(),
        interior_doc: "Cells certified entirely inside the source geometry.\n\nReturns\n-------\nCellArray of GeohashCell",
        interior_cells: { coverage.partition.interior().cell_array(GridKind::GeohashCell) },
        boundary_doc: "Cells partially overlapping the source geometry (the fringe where cell membership cannot answer the geometry question).\n\nReturns\n-------\nCellArray of GeohashCell",
        boundary_cells: {
            coverage.partition.boundary().cell_array(GridKind::GeohashCell)
        },
        depth_fields: [depth],
        hash_depth: (coverage.depth,),
        cell_hash_key: |cell| { cell.cell },
        explain_grid: "geohash",
        explain_depth: { coverage.depth.explain("precision") },
        explain_cells: "cells",
        explain_interior_len: { coverage.partition.interior_len() },
        explain_outer_len: { coverage.partition.outer_len() },
        to_polygon_doc: "Dissolve the coverage into one outline geometry.\n\nDisjoint covered regions return a `MultiPolygon`; adjacent cells dissolve shared edges into one outline.\n\nReturns\n-------\n`Polygon` or `MultiPolygon`\n\nRaises\n------\nGeometryError\n    If the coverage is empty.",
        to_polygon: {
            let inners: Vec<_> = coverage.cells.iter().map(|cell| cell.cell).collect();
            if let Some(typed) = super::super::rect_dissolve(&inners)? {
                Ok(typed)
            } else {
                let shapes: Vec<crate::geometry::Shape> = inners
                    .iter()
                    .map(|kernel| {
                        rect_cell_polygon(*kernel)
                            .shape
                            .shape()
                            .clone()
                            .split_antimeridian()
                    })
                    .collect::<crate::error::Result<_>>()?;
                super::super::coverage_to_polygon(&shapes)
            }
        },
        reduce_unpickle: "_unpickle_geohash_coverage",
        reduce_args: {
            {
                let tokens = coverage
                    .cells
                    .iter()
                    .map(|cell| cell.cell.token())
                    .collect::<Vec<_>>();
                (
                    Typed(coverage.geometry.clone()),
                    tokens,
                    coverage.cell_rule.token(),
                    // Factory partition precision (recompute key).
                    coverage
                        .partition
                        .all()
                        .get(0)
                        .map(|cell| cell.cell.precision)
                        .or_else(|| coverage.depth.uniform_level())
                        .expect("coverage has factory or visible depth"),
                    // Visible depth when empty (cannot be inferred from cells).
                    if coverage.cells.is_empty() {
                        coverage.depth.uniform_level()
                    } else {
                        None
                    },
                    // Factory max_cells budget for bounded unpickle recompute (D07).
                    coverage.max_cells,
                )
            }
        },
        repr: {
            format!(
                "<GeohashCoverage {} cell_rule={} cells={}>",
                coverage.depth.explain("precision"),
                coverage.cell_rule.token(),
                coverage.cells.len()
            )
        },
        index_error: "geohash coverage index out of range",
    }
}

pub(super) fn geohash_cell_array(cells: impl IntoIterator<Item = Geohash>) -> PyCellArray {
    rect_cell_array_for::<GeohashCoverSpec>(cells)
}

fn py_geohash_cell_array(cells: &CoverageCells<PyGeohashCell>) -> PyCellArray {
    cells.cell_array(GridKind::GeohashCell)
}

fn geohash_cell_vec(cells: Vec<Geohash>) -> Vec<PyGeohashCell> {
    rect_coverage_cells::<PyGeohashCell>(cells)
}

/// Cover a geometry with geohash cells at ``precision``.
///
/// The result carries both ``cells`` — exactly the
/// cells satisfying ``cell_rule`` — and the exact membership predicates
/// ``covers``/``contains``/``intersects``, which always answer against
/// the source geometry.
///
/// Parameters
/// ----------
/// geom : Geometry or GeometryArray
///     Geometry to cover (WGS84 lon/lat or projected). An array returns one
///     grouped cell row per input geometry.
///
/// precision : int
///     Geohash precision (``1``-``12``; finer at higher values).
///
/// cell_rule : {'center', 'within', 'overlap', 'bbox'}, default 'overlap'
///     Which cells to materialize, strictest to loosest. ``'center'``:
///     cells whose center is inside — unique assignment, balanced point
///     binning. ``'within'``: only cells entirely inside — cells the area
///     fully owns. ``'overlap'``: every cell touching the geometry — a
///     complete-coverage superset, the safe default for candidate keys.
///     ``'bbox'``: cells whose bounding box overlaps — loosest and fastest;
///     for geohash a cell IS its bbox, so identical to ``'overlap'``. The
///     rule never affects the exact predicates.
///
/// max_cells : int or None, default 1000000
///     Upper bound on emitted cells. Raise to allow a larger covering, or
///     pass ``None`` for unlimited (bounded only by memory).
///
/// Returns
/// -------
/// GeohashCoverage or Groups of CellArray
///     A scalar returns its coverage; an array returns one cell group per row.
///
/// Raises
/// ------
/// GeometryError
///     If the geometry, depth, or a coverage parameter is invalid, or if
///     the covering would exceed ``max_cells``.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
/// >>> cov = gm.geohash_cover(p, precision=6)
/// >>> (len(cov.cells), cov.contains(p), cov.cells[0].token)
/// (1, True, '9q8yyk')
#[pyfunction]
#[pyo3(
    signature = (geom, precision, *, cell_rule = CellRule::Overlap, max_cells = 1_000_000),
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
        |geometry| build_coverage(geometry, precision, cell_rule, max_cells),
        |coverage| {
            coverage
                .cells
                .iter()
                .map(|cell| cell.cell.identity_key())
                .collect()
        },
    )
}

/// Build a geohash covering of `geometry` (the ``geohash_cover(...)``
/// backend).
pub(super) fn build_coverage(
    geometry: &PyGeometry,
    precision: &Bound<'_, PyAny>,
    cell_rule: CellRule,
    max_cells: Option<usize>,
) -> PyResult<PyGeohashCoverage> {
    build_rect_coverage_state::<GeohashCoverSpec, PyGeohashCell>(
        geometry, precision, cell_rule, max_cells,
    )
    .map(PyGeohashCoverage)
}
