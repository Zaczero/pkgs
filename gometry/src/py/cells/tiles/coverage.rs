//! TileCoverage and the tile-cover backend.

use pyo3::IntoPyObjectExt;
use pyo3::types::PyAny;

use super::super::*;
use super::cell::{PyTile, parse_tile_zoom, tile_arg, tile_floor};
use crate::Typed;
use crate::grid::tile::{self as kernel, TILE_MAX_ZOOM, Tile, root as tile_root};
use crate::py::cells::coverage_ops::{
    CoverageCells, build_rect_coverage_state, parse_max_cells, rect_cell_array_for,
    rect_cell_polygon, rect_coverage_cells, unpickle_rect_coverage_state,
};
use crate::py::errors::GeometryError;

/// Rebuild a pickled TileCoverage from its public fields (internal; see
/// ``TileCoverage.__reduce__``).
#[pyfunction]
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
/// >>> cov = gm.tile_cover(p, zoom=10)
/// >>> (len(cov.cells), cov.contains(p), str(cov.cells[0]))
/// (1, True, '0230102033')
pub(super) fn _unpickle_tile_coverage(
    geometry: &Bound<'_, PyAny>,
    cells: &Bound<'_, PyAny>,
    cell_rule: &str,
    factory_zoom: u8,
    visible_depth: Option<u8>,
    max_cells: Option<i64>,
) -> PyResult<PyTileCoverage> {
    let decode = |ids: Vec<u64>| -> PyResult<Vec<Tile>> {
        ids.into_iter()
            .map(|id| {
                Tile::from_id(id)
                    .ok_or_else(|| GeometryError::new_err(format!("invalid tile id {id}")))
            })
            .collect()
    };
    let parse_depth = |value: u8| -> PyResult<u8> {
        super::super::checked_depth(
            i64::from(value),
            "tile zoom",
            "zoom",
            0,
            i64::from(TILE_MAX_ZOOM),
        )
    };
    Ok(PyTileCoverage(unpickle_rect_coverage_state::<
        TileCoverSpec,
        PyTile,
        u64,
        _,
        _,
    >(
        geometry,
        cells,
        cell_rule,
        factory_zoom,
        visible_depth,
        max_cells,
        parse_depth,
        decode,
    )?))
}

rect_coverage_pyclass! {
    spec: TileCoverSpec,
    coverage: PyTileCoverage,
    cell: PyTile,
    kernel_cell: Tile,
    kind: GridKind::Tile,
    roots: { vec![tile_root()] },
    level: |cell| cell.z,
    parse_depth: parse_tile_zoom,
    label: "tile",
    class_name: "TileCoverage",
    class_doc: "An XYZ-tile covering of a geometry (the ``tile_cover`` backend).\n\nReturned by ``tile_cover(...)``: ``coverage.cells`` materializes\nthe tiles selected by ``cell_rule`` at the chosen zoom (join keys,\nbins, visualization), while ``covers``/``contains``/``intersects``\nanswer exactly against the source geometry, independent of the rule.",
    iter: PyTileCoverageIter,
    iter_name: "TileCoverageIterator",
    depth_getter: zoom,
    depth_doc: "Uniform zoom level of the covering's tiles, or ``None`` for mixed\nzooms.\n\nReturns\n-------\nint or None",
}

grid_rect_coverage_common_pymethods! {
    impl PyTileCoverage {
        cell: Tile,
        kind: GridKind::Tile,
        kernel: kernel,
        cell_vec: tile_vec,
        depth: zoom,
        depth_field: z,
        depth_name: "zoom",
        min_depth: min_zoom,
        floor_default: 0,
        floor: tile_floor,
        parse_depth: parse_tile_zoom,
        compact_doc: "Compact the tile set to its coarsest covering.\n\nParameters\n----------\nmin_zoom : int, default 0\n    Coarsest zoom compaction may produce.\n\nReturns\n-------\nTileCoverage\n\nExamples\n--------\n>>> import gometry as gm\n>>> p = gm.Point(-122.4194, 37.7749, crs=4326)\n>>> cov = gm.tile_cover(p, zoom=10)\n>>> len(cov.compact().cells) <= len(cov.cells)\nTrue\n",
        compact_text_signature: "($self, *, min_zoom=0)",
        uncompact_doc: "Expand the tile set to a uniform zoom.\n\nParameters\n----------\nzoom : int\n    Target zoom (``0``-``29``); no coarser than any current tile.\n\nReturns\n-------\nTileCoverage\n\nRaises\n------\nGeometryError\n    If ``zoom`` is coarser than a current tile.\n\nExamples\n--------\n>>> import gometry as gm\n>>> p = gm.Point(-122.4194, 37.7749, crs=4326)\n>>> cov = gm.tile_cover(p, zoom=10)\n>>> len(cov.uncompact(10).cells) >= len(cov.cells)\nTrue\n",
        uncompact_text_signature: "($self, zoom)",
        with_parents_doc: "Include parent tiles down to a minimum zoom.\n\nParameters\n----------\nmin_zoom : int, default 0\n    Coarsest zoom to add parents for.\n\nReturns\n-------\nTileCoverage\n\nExamples\n--------\n>>> import gometry as gm\n>>> p = gm.Point(-122.4194, 37.7749, crs=4326)\n>>> cov = gm.tile_cover(p, zoom=10)\n>>> len(cov.with_parents().cells) >= len(cov.cells)\nTrue\n",
        with_parents_text_signature: "($self, *, min_zoom=0)",
        fine_token: |cell| { cell.cell.quadkey() },
    }
}

grid_coverage_common_pymethods! {
    impl PyTileCoverage {
        this: coverage,
        kind: GridKind::Tile,
        iter: PyTileCoverageIter,
        cell_array: py_tile_cell_array,
        parse_cell: tile_arg,
        parsed_key: |cell| cell.id(),
        interior_doc: "Cells certified entirely inside the source geometry.\n\nReturns\n-------\nCellArray of Tile",
        interior_cells: { coverage.partition.interior().cell_array(GridKind::Tile) },
        boundary_doc: "Cells partially overlapping the source geometry (the fringe where tile membership cannot answer the geometry question).\n\nReturns\n-------\nCellArray of Tile",
        boundary_cells: {
            coverage.partition.boundary().cell_array(GridKind::Tile)
        },
        depth_fields: [depth],
        hash_depth: (coverage.depth,),
        cell_hash_key: |cell| { cell.cell.id() },
        explain_grid: "tile",
        explain_depth: { coverage.depth.explain("zoom") },
        explain_cells: "tiles",
        explain_interior_len: { coverage.partition.interior_len() },
        explain_outer_len: { coverage.partition.outer_len() },
        to_polygon_doc: "Dissolve the coverage into one outline geometry.\n\nDisjoint covered regions return a `MultiPolygon`; adjacent tiles dissolve shared edges into one outline.\n\nReturns\n-------\n`Polygon` or `MultiPolygon`\n\nRaises\n------\nGeometryError\n    If the coverage is empty.",
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
        reduce_unpickle: "_unpickle_tile_coverage",
        reduce_args: {
            {
                let ids = coverage
                    .cells
                    .iter()
                    .map(|cell| cell.cell.id())
                    .collect::<Vec<_>>();
                (
                    Typed(coverage.geometry.clone()),
                    ids,
                    coverage.cell_rule.token(),
                    // Factory partition zoom (recompute key).
                    coverage
                        .partition
                        .all()
                        .get(0)
                        .map(|cell| cell.cell.z)
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
                "<TileCoverage {} cell_rule={} tiles={}>",
                coverage.depth.explain("zoom"),
                coverage.cell_rule.token(),
                coverage.cells.len()
            )
        },
        index_error: "tile coverage index out of range",
    }
}

pub(super) fn tile_cell_array(tiles: impl IntoIterator<Item = Tile>) -> PyCellArray {
    rect_cell_array_for::<TileCoverSpec>(tiles)
}

fn py_tile_cell_array(tiles: &CoverageCells<PyTile>) -> PyCellArray {
    tiles.cell_array(GridKind::Tile)
}

fn tile_vec(tiles: Vec<Tile>) -> Vec<PyTile> {
    rect_coverage_cells::<PyTile>(tiles)
}

/// Cover a geometry with XYZ web-mercator tiles at ``zoom``.
///
/// The result carries both ``cells`` — exactly the
/// tiles satisfying ``cell_rule`` — and the exact membership predicates
/// ``covers``/``contains``/``intersects``, which always answer against
/// the source geometry.
///
/// Parameters
/// ----------
/// geom : Geometry or GeometryArray
///     Geometry to cover (WGS84 lon/lat or projected). An array returns one
///     grouped cell row per input geometry.
///
/// zoom : int
///     Tile zoom (``0``-``29``; finer at higher values).
///
/// cell_rule : {'center', 'within', 'overlap', 'bbox'}, default 'overlap'
///     Which tiles to materialize, strictest to loosest. ``'center'``:
///     tiles whose center is inside — unique assignment, balanced point
///     binning. ``'within'``: only tiles entirely inside — tiles the area
///     fully owns. ``'overlap'``: every tile touching the geometry — a
///     complete-coverage superset, the safe default for candidate keys.
///     ``'bbox'``: tiles whose bounding box overlaps — loosest and fastest;
///     a tile IS its bbox, so identical to ``'overlap'``. The rule never
///     affects the exact predicates.
///
/// max_cells : int or None, default 1000000
///     Upper bound on emitted cells. Raise to allow a larger covering, or
///     pass ``None`` for unlimited (bounded only by memory).
///
/// Returns
/// -------
/// TileCoverage or Groups of CellArray
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
/// >>> cov = gm.tile_cover(p, zoom=10)
/// >>> (len(cov.cells), cov.contains(p), str(cov.cells[0]))
/// (1, True, '0230102033')
#[pyfunction]
#[pyo3(
    signature = (geom, zoom, *, cell_rule = CellRule::Overlap, max_cells = 1_000_000),
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
        |geometry| build_coverage(geometry, zoom, cell_rule, max_cells),
        |coverage| coverage.cells.iter().map(|cell| cell.cell.id()).collect(),
    )
}

/// Build a tile covering of `geometry` (the ``tile_cover(...)``
/// backend).
pub(super) fn build_coverage(
    geometry: &PyGeometry,
    zoom: &Bound<'_, PyAny>,
    cell_rule: CellRule,
    max_cells: Option<usize>,
) -> PyResult<PyTileCoverage> {
    build_rect_coverage_state::<TileCoverSpec, PyTile>(geometry, zoom, cell_rule, max_cells)
        .map(PyTileCoverage)
}
