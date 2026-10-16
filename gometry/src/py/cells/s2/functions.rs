use crate::Typed;
use crate::geometry::CoordSeq;
use crate::grid::s2::bounding;
use crate::grid::s2::cell::Cell as S2GeomCell;
use crate::grid::s2::cellid::CellId;
use crate::py::cells::coverage_ops::{
    GridCoverError, GridDissolver, GridSourceAdmission, SortedCells, admit_grid_geometry,
    dissolve_grid_cells,
};
use crate::py::cells::s2::parse::S2LevelBudget;
use crate::py::cells::s2::{
    PyS2Cell, cell_pole_side, parse_s2_adaptive_budget, parse_s2_fixed_budget,
    parse_s2_level_value, register, s2_boundary_geometry, s2_cell_array, s2_cell_from_xy,
    s2_cell_id,
};
use crate::py::cells::{
    Bound, CellRule, GeometryError, GridKind, Py, PyAny, PyCellArray, PyErr, PyResult, Python,
    Shape, bounding_query_bounds, coverage_ops, dispatch_grid_cell_array, grid_cover_dispatch,
    parse_error, pyfunction,
};
/// Build S2 cells from points, coordinate rows, or parallel lon/lat columns.
///
/// Parameters
/// ----------
/// values : GeometryArray of Point, iterable of Point or None, coordinate matrix, or float
///     Point geometries (``None`` makes a missing cell), a coordinate matrix
///     with at least two columns, or WGS84 longitudes. Iterable points must
///     share one CRS and epoch; projected point inputs are reprojected to
///     WGS84. Coordinate matrices use only X and Y, ignoring extra ordinates.
///
/// lat : float or sequence of float, optional
///     WGS84 latitude per row when ``values`` supplies longitudes. Coordinate
///     columns broadcast numpy-style; at least one must be a sequence.
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

#[expect(
    clippy::used_underscore_items,
    reason = "the leading underscore is the Python convention for a module-private helper, and pickle stores these names in its stream, so the name is a wire contract rather than a Rust style choice"
)]
/// Rebuild a pickled S2Cell from its 64-bit id (internal; see
/// ``S2Cell.__reduce__``).
#[pyfunction]
pub(crate) fn _unpickle_s2_cell_v1(
    #[pyo3(from_py_with = crate::strict_u64)] id: u64,
) -> PyResult<PyS2Cell> {
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
    use crate::{exact_geometry, exact_geometry_array};

    // Kernel routes single-point → L30 leaf and multi-point/region → R18 bbox.
    if let Some(geometry) = exact_geometry(value) {
        let admitted = admit_grid_geometry(geometry)?;
        let shape = admitted.shape.as_ref();
        if shape.is_empty() {
            return Err(InvalidGeometryError::new_err(
                "bounding cell requires a non-empty geometry",
            ));
        }
        return bounding_cell_result(bounding::bounding_cell(shape));
    }
    if let Some(array) = exact_geometry_array(value) {
        let array = array.clone();
        let result = value.py().detach(move || {
            let admission = match GridSourceAdmission::new_for_array(&array) {
                Ok(admission) => admission,
                Err(error) => return Err(GridCoverError::Native(error)),
            };
            admission.admit_bounding_collection(&array)
        });
        let collection = result?;
        return bounding_cell_result(bounding::bounding_cell(&collection));
    }
    let bounds = bounding_query_bounds(value)?;
    bounding_cell_result(bounding::bounding_cell_bbox(bounds))
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
        item_doc: "S2Cell, int, str, or iterable of those with missing values skipped",
        contract_doc: "",
        parse_error_doc: "If an id or token is not a valid S2 cell.",
        parse_cell: s2_cell_id,
        array: |cells| s2_cell_array(cells),
        union: s2_union,
        intersection: s2_intersection,
        difference: s2_difference,
        example_union: r">>> import gometry as gm
>>> p = gm.Point(-122.4194, 37.7749, crs=4326)
>>> cells = [cell for cell in gm.s2_cover(p, level=12) if cell is not None]
>>> len(gm.s2_union(cells, cells))
1
",
        example_intersection: r">>> import gometry as gm
>>> p = gm.Point(-122.4194, 37.7749, crs=4326)
>>> cells = [cell for cell in gm.s2_cover(p, level=12) if cell is not None]
>>> len(gm.s2_intersection(cells, cells))
1
",
        example_difference: r">>> import gometry as gm
>>> p = gm.Point(-122.4194, 37.7749, crs=4326)
>>> cells = [cell for cell in gm.s2_cover(p, level=12) if cell is not None]
>>> len(gm.s2_difference(cells, []))
1
",
    }
}

/// Dissolve a slice of S2 cells into one outline geometry — the shared engine
/// behind `CellArray.to_polygon`.
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
    let owned = cells.to_vec();
    dissolve_grid_cells::<S2Dissolver>(SortedCells::new(owned))
}

struct S2Dissolver;

impl GridDissolver for S2Dissolver {
    type Cell = CellId;

    fn fast_path_cells(
        cells: &SortedCells<Self::Cell>,
    ) -> PyResult<Option<SortedCells<Self::Cell>>> {
        let Some(first) = cells.as_slice().first() else {
            return Ok(None);
        };
        let level = first.level();
        Ok(cells
            .as_slice()
            .iter()
            .all(|cell| cell.level() == level)
            .then(|| cells.clone()))
    }

    fn adjacency_neighbors(cell: Self::Cell) -> impl Iterator<Item = Self::Cell> {
        cell.edge_neighbors().into_iter()
    }

    fn exterior_edge_segments(
        cell: Self::Cell,
        is_member: &dyn Fn(Self::Cell) -> bool,
    ) -> PyResult<Vec<CoordSeq>> {
        let vertices = S2GeomCell::from_id(cell).vertices_lonlat();
        let [first, second, third, fourth] = vertices;
        let edges = [[first, second], [second, third], [third, fourth], [
            fourth, first,
        ]];
        let mut segments = Vec::with_capacity(4);
        for (neighbor, [start, end]) in cell.edge_neighbors().into_iter().zip(edges) {
            if is_member(neighbor) {
                continue;
            }
            segments.push(CoordSeq::try_from_points(&[start, end]).map_err(PyErr::from)?);
        }
        Ok(segments)
    }

    fn crosses_seam(segment: &CoordSeq) -> bool {
        (segment.point_at(0).x - segment.point_at(1).x).abs() > 180.0
    }

    fn fallback_shape(cell: Self::Cell) -> crate::error::Result<Shape> {
        // S2 cells can straddle the antimeridian; polar cells need forced pole
        // closure so the seam split cannot choose the opposite cap.
        let boundary = s2_boundary_geometry(cell)?.shape.shape().clone();
        cell_pole_side(&boundary).map_or_else(
            || boundary.split_antimeridian(),
            |north| boundary.split_antimeridian_over_pole(north),
        )
    }
}

/// Cover a geometry with fixed-level or adaptively refined S2 cells.
///
/// Omit ``level`` to adapt refinement to ``target_cells`` and the optional
/// level range. Supplying ``level`` fixes the output depth; non-default
/// adaptive controls cannot be combined with it. Keep the source geometry
/// separately and use the free ``gm.*`` predicates for exact geometry
/// questions.
///
/// Parameters
/// ----------
/// geom : Geometry, GeometryArray, or iterable of Geometry or None
///     Geometry to cover (WGS84 lon/lat or projected). An array or iterable
///     returns one group per input row; ``None`` produces a missing group and
///     a present empty geometry produces an empty group.
///
/// level : int, optional
///     Fixed S2 cell level (``0``-``30``; finer at higher values). The level is
///     never coarsened to fit the output cap. Omit it for adaptive refinement.
///
/// target_cells : int, default 8
///     Approximate adaptive refinement target; the minimum admissible cover
///     can exceed this value.
///
/// min_level : int or None, default None
///     Coarsest adaptive S2 level allowed; omission uses level ``0``.
///
/// max_level : int or None, default None
///     Finest adaptive S2 level allowed; omission uses level ``30``.
///
/// level_mod : {1, 2, 3}, default 1
///     Restrict adaptive output levels to increments from ``min_level``.
///
/// cell_rule : {'within', 'center', 'overlap', 'bbox'}, default 'overlap'
///     Which cells to materialize, strictest to loosest. ``'within'``: only
///     cells entirely inside — cells the area fully owns. ``'center'``:
///     cells whose center is inside — unique assignment, balanced point
///     binning. ``'overlap'``: every cell touching the geometry — a
///     complete-coverage superset, the safe default for candidate keys.
///     ``'bbox'``: every cell meeting the geometry's envelope, one window per
///     part and split at the antimeridian — loosest and fastest.
///     The rule never affects the exact predicates.
///
/// max_cells : int or None, default 1000000
///     Hard cap on returned cells, applied cumulatively across array groups.
///     In fixed-level mode an insufficient cap raises; adaptive mode may
///     coarsen only within its requested level range. Pass ``None`` for an
///     unlimited cover.
///
/// Returns
/// -------
/// CellArray or Groups of CellArray
///     A scalar returns its coverage; an array returns one cell group per row.
///
/// Raises
/// ------
/// GeometryError
///     If the geometry, level, or a coverage parameter is invalid, if fixed
///     and non-default adaptive controls are combined, or if the cover exceeds
///     ``max_cells``.
/// InvalidGeometryError
///     If a scalar geometry is empty or coordinates leave the lon/lat domain.
///
/// See Also
/// --------
/// s2_cells : Build S2 cells from lon/lat columns.
/// s2_cover : Cover with fixed or adaptive levels.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
/// >>> cov = gm.s2_cover(p, level=12)
/// >>> cell = cov[0]
/// >>> assert cell is not None
/// >>> (len(cov), cell.token)
/// (1, '8085809')
#[pyfunction]
#[pyo3(
    signature = (geom, level = None, *, target_cells = 8, min_level = None, max_level = None, level_mod = 1, cell_rule = CellRule::Overlap, max_cells = Some(1_000_000)),
    text_signature = "(geom, level=None, *, target_cells=8, min_level=None, max_level=None, level_mod=1, cell_rule='overlap', max_cells=1000000)"
)]
pub(crate) fn s2_cover(
    py: Python<'_>,
    geom: &Bound<'_, PyAny>,
    #[pyo3(from_py_with = crate::strict_optional_i64)] level: Option<i64>,
    #[pyo3(from_py_with = crate::strict_i64)] target_cells: i64,
    #[pyo3(from_py_with = crate::strict_optional_i64)] min_level: Option<i64>,
    #[pyo3(from_py_with = crate::strict_optional_i64)] max_level: Option<i64>,
    #[pyo3(from_py_with = crate::strict_i64)] level_mod: i64,
    cell_rule: CellRule,
    #[pyo3(from_py_with = crate::strict_optional_i64)] max_cells: Option<i64>,
) -> PyResult<Py<PyAny>> {
    let budget = if let Some(level) = level {
        if target_cells != 8 || min_level.is_some() || max_level.is_some() || level_mod != 1 {
            return Err(GeometryError::new_err(
                "fixed S2 level cannot be combined with adaptive cover controls",
            ));
        }
        parse_s2_fixed_budget(level, max_cells)?
    } else {
        parse_s2_adaptive_budget(
            target_cells,
            min_level.unwrap_or(0),
            max_level.unwrap_or(30),
            level_mod,
            max_cells,
        )?
    };
    s2_cover_with_budget(py, geom, budget, cell_rule)
}

fn s2_cover_with_budget(
    py: Python<'_>,
    geom: &Bound<'_, PyAny>,
    budget: S2LevelBudget,
    cell_rule: CellRule,
) -> PyResult<Py<PyAny>> {
    grid_cover_dispatch(
        py,
        geom,
        GridKind::S2Cell,
        budget.max_cells,
        |geometry, effective_max_cells| {
            let effective_budget = S2LevelBudget {
                max_cells: effective_max_cells,
                ..budget
            };
            let cells = register::build_coverage(geometry, effective_budget, cell_rule)?;
            Ok(coverage_ops::collect_cover_ids(
                SortedCells::from_sorted(cells)
                    .into_vec()
                    .into_iter()
                    .map(CellId::raw),
            ))
        },
    )
}
