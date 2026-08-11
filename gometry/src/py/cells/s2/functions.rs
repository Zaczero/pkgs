#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use crate::Typed;
use crate::geometry::CoordSeq;
use crate::grid::s2::cell::Cell as S2GeomCell;
use crate::grid::s2::cellid::CellId;
use crate::py::cells::coverage_ops::{
    CoverageCells, GridDissolver, dissolve_grid_cells, parse_max_cells,
};
use crate::py::cells::s2::{
    PyS2Cell, PyS2Coverage, S2Membership, cell_pole_side, parse_s2_level_budget,
    parse_s2_level_value, s2_boundary_geometry, s2_cell_array, s2_cell_from_xy, s2_cell_id,
};
use crate::py::cells::{
    Bound, CellRule, GeometryError, GridKind, Py, PyAny, PyCellArray, PyResult, Python, Shape,
    bounding_query_bounds, dispatch_grid_cell_array, grid_cover_dispatch, parse_error, pyfunction,
};
/// Build S2 cells from parallel lon/lat columns.
///
/// Parameters
/// ----------
/// values : GeometryArray of Point, float, or sequence of float
///     Point geometries or WGS84 longitudes. Projected point arrays are
///     reprojected in one native batch.
///
/// lat : float or sequence of float, optional
///     WGS84 latitude per row when ``values`` supplies longitudes. Scalars
///     broadcast numpy-style; at least one coordinate column must be sequence of float.
///
/// level : int or sequence of int
///     S2 cell level (0-30; finer at higher values). A scalar broadcasts to
///     every row; an array supplies one level per row.
///
/// Returns
/// -------
/// CellArray of S2Cell
///     One cell per input coordinate.
///
/// Raises
/// ------
/// GeometryError
///     If ``level`` is out of range or every argument is scalar.
/// InvalidGeometryError
///     If a coordinate is non-finite or columns differ in length.
///
/// See Also
/// --------
/// S2Cell : Build a single cell.
/// s2_cover : Cover a geometry with S2 cells.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> len(gm.s2_cells([21.0, 22.0], [52.0, 53.0], level=10))
/// 2
#[pyfunction]
#[pyo3(
    signature = (values, lat = None, *, level),
    text_signature = "(values, lat=None, *, level)"
)]
pub(crate) fn s2_cells(
    py: Python<'_>,
    values: &Bound<'_, PyAny>,
    lat: Option<&Bound<'_, PyAny>>,
    level: &Bound<'_, PyAny>,
) -> PyResult<PyCellArray> {
    dispatch_grid_cell_array(
        py,
        values,
        lat,
        level,
        GridKind::S2Cell,
        "S2Cell",
        "level",
        parse_s2_level_value,
        s2_cell_from_xy,
    )
}

#[pyfunction]
pub(crate) fn _unpickle_s2_coverage(
    geometry: &Bound<'_, PyAny>,
    cell_ids: &Bound<'_, PyAny>,
    cell_rule: &str,
    min_level: u8,
    max_level: u8,
    level_mod: u8,
    max_cells: Option<i64>,
    target_cells: i64,
) -> PyResult<PyS2Coverage> {
    use crate::py::cells::coverage_ops::{collect_coverage_sequence, normalize_coverage_source};
    use crate::py::cells::s2::parse::{
        parse_s2_level_value, parse_s2_target_cells, validate_s2_level_mod,
    };

    let (geometry, cover_shape, cover_is_split) = normalize_coverage_source(geometry, "S2")?;
    // Public parameter parsers (D28): reject out-of-range/reversed levels,
    // level_mod=0, max_cells<=0.
    let min_level = parse_s2_level_value(i64::from(min_level))?;
    let max_level = parse_s2_level_value(i64::from(max_level))?;
    if min_level > max_level {
        return Err(GeometryError::new_err("min_level must be <= max_level"));
    }
    let level_mod = validate_s2_level_mod(i64::from(level_mod))?;
    // Factory budget from the pickle (D07). None = adult unlimited factory.
    let max_cells = parse_max_cells(max_cells)?;
    let target_cells = parse_s2_target_cells(target_cells)?;
    let cell_rule = CellRule::parse(cell_rule)
        .map_err(|message| crate::py::errors::parameter_error(message, "cell_rule"))?;

    // Exact built-in list of primitive ids (D09) — no discarded outer/interior.
    let cell_ids: Vec<u64> = collect_coverage_sequence(cell_ids, "S2 coverage pickle cells")?;

    let decode = |ids: Vec<u64>| -> PyResult<Vec<CellId>> {
        ids.into_iter()
            .map(|id| {
                CellId::from_raw(id)
                    .ok_or_else(|| GeometryError::new_err(format!("invalid S2 cell id {id}")))
            })
            .collect()
    };
    let cells = CoverageCells::from_cells(
        decode(cell_ids)?
            .into_iter()
            .map(|cell| PyS2Cell { cell })
            .collect(),
    );

    // Lazy membership: no coverer recompute on unpickle (same contract as H3).
    // D07 budget applies when inspection first materializes the partition.
    let membership = S2Membership::lazy(
        cover_shape,
        cover_is_split,
        min_level,
        max_level,
        level_mod,
        target_cells,
    );
    Ok(PyS2Coverage {
        geometry,
        cells,
        cell_rule,
        min_level,
        max_level,
        level_mod,
        max_cells,
        target_cells,
        membership,
    })
}

/// Rebuild a pickled S2Cell from its 64-bit id (internal; see
/// ``S2Cell.__reduce__``).
#[pyfunction]
pub(crate) fn _unpickle_s2_cell(id: u64) -> PyResult<PyS2Cell> {
    CellId::from_raw(id)
        .map(|cell| PyS2Cell { cell })
        .ok_or_else(|| {
            parse_error(
                format!("invalid S2 cell id {id}"),
                crate::error::ParseFormat::S2,
            )
        })
}

/// Return the deepest S2 cell that **provably** contains a geometry's lon/lat bounding box.
///
/// Sibling-consistent with ``geohash_bounding_cell`` / ``tile_bounding_cell`` /
/// ``h3_bounding_cell``: non-point inputs collapse to their lon/lat envelope,
/// then the deepest cell that can be proven to cover that rectangle is returned.
/// Near cell boundaries the result may be one level coarser than the theoretical
/// deepest (always containing). A single point yields its exact level-30 leaf;
/// a multipoint uses its bounding box (same path as any multi-vertex region —
/// not a leaf-LCA of the vertices alone). Regions that span multiple cube faces
/// have no single containing cell and raise.
///
/// Parameters
/// ----------
/// value : Geometry, GeometryArray, or sequence of float
///     A geometry/array (non-WGS84 frames reproject), or a bare lon/lat
///     ``(minx, miny, maxx, maxy)`` bounds.
///
/// Returns
/// -------
/// S2Cell
///     The deepest cell that provably contains the geometry's bounding box;
///     near boundaries may be coarser than theoretical deepest (always
///     containing).
///
/// Raises
/// ------
/// InvalidGeometryError
///     If the geometry is empty or coordinates leave the lon/lat domain.
/// GeometryError
///     If no single S2 cell contains the bounds, or bare bounds are not
///     ordered min <= max.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> berlin = gm.box(13.3, 52.4, 13.5, 52.6, crs=4326)
/// >>> gm.s2_bounding_cell(berlin).level
/// 8
#[pyfunction]
pub(crate) fn s2_bounding_cell(value: &Bound<'_, PyAny>) -> PyResult<PyS2Cell> {
    use crate::py::errors::InvalidGeometryError;
    use crate::{exact_geometry, exact_geometry_array, lonlat_shape, validate_lonlat_shape};

    // Kernel routes single-point → L30 leaf and multi-point/region → R18 bbox.
    if let Some(geometry) = exact_geometry(value) {
        let shape = lonlat_shape(geometry)?;
        validate_lonlat_shape(&shape)?;
        if shape.is_empty() {
            return Err(InvalidGeometryError::new_err(
                "bounding cell requires a non-empty geometry",
            ));
        }
        return bounding_cell_result(crate::grid::s2::bounding::bounding_cell(&shape));
    }
    if let Some(array) = exact_geometry_array(value) {
        let crs = array.crs_str();
        let mut parts = Vec::new();
        for (row, shape) in array.storage().iter_shapes().enumerate() {
            if array.is_row_missing(row) {
                continue;
            }
            let shape = crate::lonlat_shape_under(&shape, crs)?.into_owned();
            validate_lonlat_shape(&shape)?;
            if !shape.is_empty() {
                parts.push(shape);
            }
        }
        if parts.is_empty() {
            return Err(InvalidGeometryError::new_err(
                "bounding cell requires a non-empty geometry",
            ));
        }
        // ONE seam-aware owner for every representation: scalar, collection,
        // and GeometryArray all fold through `bounding_cell`. The former
        // planar envelope fold on multi-row regions dropped antimeridian
        // handling and disagreed with GeometryCollection on seam cases (C15).
        let collection = if parts.len() == 1 {
            parts.pop().expect("one part")
        } else {
            Shape::GeometryCollection(parts)
        };
        return bounding_cell_result(crate::grid::s2::bounding::bounding_cell(&collection));
    }
    let bounds = bounding_query_bounds(value)?;
    bounding_cell_result(crate::grid::s2::bounding::bounding_cell_bbox(bounds))
}

fn bounding_cell_result(cell: Option<CellId>) -> PyResult<PyS2Cell> {
    cell.map(|cell| PyS2Cell { cell }).ok_or_else(|| {
        GeometryError::new_err(
            "no single S2 cell contains the bounds (they span multiple face cells)",
        )
    })
}

grid_free_functions! {
    @set_algebra {
        cell_set_arg: s2_cell_set_arg,
        cell_type: CellId,
        label: "S2",
        cell_doc: "S2Cell",
        item_doc: "S2Cell, int, str, or iterable of those",
        contract_doc: "",
        parse_cell: s2_cell_id,
        array: |cells| s2_cell_array(cells),
        union: s2_union,
        intersection: s2_intersection,
        difference: s2_difference,
        example_union: r"
>>> import gometry as gm
>>> p = gm.Point(-122.4194, 37.7749, crs=4326)
>>> cells = list(gm.s2_cover(p, level=12).cells)
>>> len(gm.s2_union(cells, cells))
1
",
        example_intersection: r"
>>> import gometry as gm
>>> p = gm.Point(-122.4194, 37.7749, crs=4326)
>>> cells = list(gm.s2_cover(p, level=12).cells)
>>> len(gm.s2_intersection(cells, cells))
1
",
        example_difference: r"
>>> import gometry as gm
>>> p = gm.Point(-122.4194, 37.7749, crs=4326)
>>> cells = list(gm.s2_cover(p, level=12).cells)
>>> len(gm.s2_difference(cells, []))
1
",
    }
}

/// Dissolve a slice of S2 cells into one outline geometry — the shared engine
/// behind `S2Coverage.to_polygon` and `CellArray.to_polygon`.
///
/// Topology fast path (uniform-level sets). An S2 cell edge is interior to the
/// set iff the edge-neighbour across it is also present, so the dissolved
/// outline is exactly the cell edges whose neighbour is absent — a pure
/// grid-adjacency test that materialises no geometry for the (typically >90%)
/// interior cells. Each quad edge `k` (corners `vertices[k] -> vertices[k+1]`)
/// is across `edge_neighbours[k]` (both use the same face IJ axes). Only
/// uniform-level sets cancel by id identity; mixed-level sets fall through to
/// the general union, as do antimeridian-straddling edges (caught per edge).
///
pub(crate) fn s2_dissolve(cells: &[CellId]) -> PyResult<Typed> {
    let mut sorted: Vec<CellId> = cells.to_vec();
    sorted.sort_unstable();
    sorted.dedup();
    s2_dissolve_sorted(&sorted)
}

/// Input is already sorted+deduped by the caller.
pub(crate) fn s2_dissolve_sorted(cells: &[CellId]) -> PyResult<Typed> {
    dissolve_grid_cells::<S2Dissolver>(cells)
}

struct S2Dissolver;

impl GridDissolver for S2Dissolver {
    type Cell = CellId;

    fn fast_path_cells(cells: &[Self::Cell]) -> PyResult<Option<Vec<Self::Cell>>> {
        let Some(first) = cells.first() else {
            return Ok(None);
        };
        let level = first.level();
        Ok(cells
            .iter()
            .all(|cell| cell.level() == level)
            .then(|| cells.to_vec()))
    }

    fn adjacency_neighbors(cell: Self::Cell) -> impl Iterator<Item = Self::Cell> {
        cell.edge_neighbors().into_iter()
    }

    fn exterior_edge_segments(
        cell: Self::Cell,
        is_member: &dyn Fn(Self::Cell) -> bool,
    ) -> Vec<CoordSeq> {
        let vertices = S2GeomCell::from_id(cell).vertices_lonlat();
        cell.edge_neighbors()
            .into_iter()
            .enumerate()
            .filter_map(|(k, neighbor)| {
                if is_member(neighbor) {
                    return None;
                }
                Some(CoordSeq::from_points(&[vertices[k], vertices[(k + 1) % 4]]))
            })
            .collect()
    }

    fn crosses_seam(segment: &CoordSeq) -> bool {
        let points = segment.to_vec();
        (points[0].x - points[1].x).abs() > 180.0
    }

    fn fallback_shape(cell: Self::Cell) -> crate::error::Result<Shape> {
        // S2 cells can straddle the antimeridian; polar cells need forced pole
        // closure so the seam split cannot choose the opposite cap.
        let boundary = s2_boundary_geometry(cell).shape.shape().clone();
        cell_pole_side(&boundary).map_or_else(
            || boundary.split_antimeridian(),
            |north| boundary.split_antimeridian_over_pole(north),
        )
    }
}

/// Cover a geometry with S2 cells within a level budget.
///
/// The result carries both ``cells`` — the S2 cells selected by
/// ``cell_rule`` within the level budget — and exact membership
/// predicates ``covers``/``contains``/``intersects``, which always answer
/// against the source geometry, never the cells.
///
/// Parameters
/// ----------
/// geom : Geometry or GeometryArray
///     Geometry to cover (WGS84 lon/lat or projected). An array returns one
///     grouped cell row per input geometry.
///
/// level : int, optional
///     S2 cell level (``0``-``30``; finer at higher values). Fixes both
///     ``min_level`` and ``max_level``. Omit for an adaptive multi-level
///     covering guided by ``target_cells``.
///
/// cell_rule : {'center', 'within', 'overlap', 'bbox'}, default 'overlap'
///     Which cells to materialize, strictest to loosest. ``'center'``:
///     cells whose center is inside — unique assignment, balanced point
///     binning. ``'within'``: only cells entirely inside — cells the area
///     fully owns. ``'overlap'``: every cell touching the geometry — a
///     complete-coverage superset, the safe default for candidate keys.
///     ``'bbox'``: cells whose bounding box overlaps — loosest and fastest.
///     The rule never affects the exact predicates.
///
/// max_cells : int or None, default 1000000
///     Hard cap on emitted cells when ``level`` fixes the cover depth. It is
///     retained as metadata for adaptive covers, whose size is instead guided
///     by ``target_cells``. Pass ``None`` for an unlimited fixed-level cover.
///
/// target_cells : int, default 8
///     S2-idiomatic approximation target for optional adaptive refinement
///     when ``level`` is omitted. It does not affect fixed-level coverings.
///
/// min_level, max_level : int, optional
///     Coarsest/finest S2 levels allowed (default to ``level``).
///
/// level_mod : int, default 1
///     Restrict cells to levels a multiple of ``level_mod`` from
///     ``min_level``.
///
/// Returns
/// -------
/// S2Coverage or Groups of CellArray
///     A scalar returns its coverage; an array returns one cell group per row.
///
/// Raises
/// ------
/// GeometryError
///     If the geometry, depth, or a coverage parameter is invalid, or if
///     a fixed-level covering would exceed ``max_cells``.
///
/// See Also
/// --------
/// s2_cells : Build S2 cells from lon/lat columns.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
/// >>> cov = gm.s2_cover(p, level=12)
/// >>> (len(cov.cells), cov.contains(p), cov.cells[0].token)
/// (1, True, '8085809')
#[pyfunction]
#[pyo3(
    signature = (geom, level = None, *, cell_rule = CellRule::Overlap, max_cells = Some(1_000_000), target_cells = 8, min_level = None, max_level = None, level_mod = 1),
    text_signature = "(geom, level=None, *, cell_rule='overlap', max_cells=1000000, target_cells=8, min_level=None, max_level=None, level_mod=1)"
)]
pub(crate) fn s2_cover(
    py: Python<'_>,
    geom: &Bound<'_, PyAny>,
    level: Option<&Bound<'_, PyAny>>,
    cell_rule: CellRule,
    max_cells: Option<i64>,
    target_cells: i64,
    min_level: Option<&Bound<'_, PyAny>>,
    max_level: Option<&Bound<'_, PyAny>>,
    level_mod: i64,
) -> PyResult<Py<PyAny>> {
    let parsed = parse_s2_level_budget(
        level,
        max_cells,
        target_cells,
        min_level,
        max_level,
        level_mod,
    )?;
    let budget_cells = (parsed.min_level == parsed.max_level)
        .then_some(parsed.max_cells)
        .flatten();
    grid_cover_dispatch(
        py,
        geom,
        GridKind::S2Cell,
        budget_cells,
        |geometry| {
            super::build_coverage(
                geometry,
                level,
                max_cells,
                target_cells,
                min_level,
                max_level,
                level_mod,
                cell_rule,
            )
        },
        |coverage| coverage.cells.iter().map(|cell| cell.cell.raw()).collect(),
    )
}
