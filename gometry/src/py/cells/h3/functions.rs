use crate::grid::spherical_arc::{H3BoundingWindow, h3_cell_contains_windows};
use crate::py::cells::coverage_ops::{SortedCells, collect_cover_ids};
use crate::py::cells::h3::register::build_coverage;
use crate::py::cells::h3::{
    CellIndex, LatLng, PyH3Cell, Resolution, h3_resolution, h3_resolution_from_i64, parse_h3_index,
};
use crate::py::cells::{
    Bound, CellRule, GeometryError, GridKind, H3_MAX_RESOLUTION, Py, PyAny, PyCellArray, PyResult,
    Python, bounding_query_region, dispatch_grid_cell_array, grid_cover_dispatch, h3, pyfunction,
};
use crate::py::errors::InvalidGeometryError;

pub(super) fn parse_h3_grid_k(value: &Bound<'_, PyAny>) -> PyResult<u32> {
    let k = crate::py_i64_required("H3 grid distance", value)?;
    if k < 0 {
        return Err(GeometryError::new_err(
            "H3 grid distance must be non-negative",
        ));
    }
    u32::try_from(k).map_err(|_| GeometryError::new_err("H3 grid distance is too large"))
}

/// Build H3 cells from parallel lon/lat columns.
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
/// resolution : int or sequence of int
///     H3 resolution (0-15; finer at higher values). A scalar broadcasts to
///     every row; an array supplies one resolution per row.
///
/// Returns
/// -------
/// CellArray of H3Cell
///     One cell per input coordinate.
///
/// Raises
/// ------
/// GeometryError
///     If ``resolution`` is out of range or every argument is scalar.
/// InvalidGeometryError
///     If a coordinate is non-finite or columns differ in length.
///
/// See Also
/// --------
/// H3Cell : Build a single cell.
/// h3_cover : Cover a geometry with H3 cells.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> cells = gm.h3_cells([-122.4, -122.3], [37.8, 37.7], resolution=7)
/// >>> cell = cells[0]
/// >>> assert cell is not None
/// >>> (len(cells), cell.token)
/// (2, '87283080cffffff')
#[pyfunction]
#[pyo3(
    signature = (values, lat = None, *, resolution),
    text_signature = "(values, lat=None, *, resolution)"
)]
pub(super) fn h3_cells(
    py: Python<'_>,
    values: &Bound<'_, PyAny>,
    lat: Option<&Bound<'_, PyAny>>,
    resolution: &Bound<'_, PyAny>,
) -> PyResult<PyCellArray> {
    dispatch_grid_cell_array(
        py,
        values,
        lat,
        resolution,
        GridKind::H3Cell,
        "H3Cell",
        "resolution",
        h3_resolution_from_i64,
        h3_cell_from_xy,
    )
}

/// Return the deepest single H3 cell containing a geometry or lon/lat bounds.
///
/// Walks a necessary ancestor path, then proves the complete region against
/// every ordered native H3 edge. Vertex ownership is not a containment proof;
/// when the finest candidate cannot be proved, a coarser proven ancestor is
/// returned.
///
/// Parameters
/// ----------
/// value : Geometry, GeometryArray, or sequence of float
///     A geometry/array (non-WGS84 frames reproject), or a bare lon/lat
///     ``(minx, miny, maxx, maxy)`` bounds.
///
/// Returns
/// -------
/// H3Cell
///     The deepest cell whose region contains the whole bounds.
///
/// Raises
/// ------
/// InvalidGeometryError
///     If the geometry is empty or coordinates leave the lon/lat domain.
/// GeometryError
///     If no single H3 cell contains the bounds, or bare bounds are not
///     ordered min <= max.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> berlin = gm.box(13.3, 52.4, 13.5, 52.6, crs=4326)
/// >>> gm.h3_bounding_cell(berlin).resolution
/// 2
#[pyfunction]
pub(super) fn h3_bounding_cell(value: &Bound<'_, PyAny>) -> PyResult<PyH3Cell> {
    let query = bounding_query_region(value)?;
    let bounds = query.bounds;
    let ordinary = H3BoundingWindow {
        west: bounds.minx(),
        south: bounds.miny(),
        east: bounds.maxx(),
        north: bounds.maxy(),
    };
    let (windows, window_count) = if query.crosses_antimeridian || bounds.minx() > bounds.maxx() {
        (
            [
                H3BoundingWindow {
                    west: bounds.minx(),
                    south: bounds.miny(),
                    east: 180.0,
                    north: bounds.maxy(),
                },
                H3BoundingWindow {
                    west: -180.0,
                    south: bounds.miny(),
                    east: bounds.maxx(),
                    north: bounds.maxy(),
                },
            ],
            2,
        )
    } else {
        ([ordinary, ordinary], 1)
    };
    let max_res = h3_resolution(H3_MAX_RESOLUTION)?;
    let mut corners = Vec::with_capacity(windows.len() * 4);
    for window in windows.iter().take(window_count) {
        for (lon, lat) in [
            (window.west, window.south),
            (window.east, window.south),
            (window.east, window.north),
            (window.west, window.north),
        ] {
            corners.push(h3_corner_cell(lon, lat, max_res)?);
        }
    }
    if corners.is_empty() {
        return Err(InvalidGeometryError::new_err(
            "bounding cell requires a non-empty geometry",
        ));
    }
    while !corners.iter().all(|cell| *cell == corners[0]) {
        let current = u8::from(corners[0].resolution());
        if current == 0 {
            return Err(GeometryError::new_err(
                "no single H3 cell contains the bounds (they straddle the resolution-0 grid)",
            ));
        }
        let resolution = h3_resolution(current - 1)?;
        for corner in &mut corners {
            *corner = corner
                .parent(resolution)
                .ok_or_else(|| GeometryError::new_err("no single H3 cell contains the bounds"))?;
        }
    }
    let mut candidate = corners[0];
    loop {
        if h3_cell_contains_windows(candidate, &windows[..window_count]) {
            return Ok(PyH3Cell { cell: candidate });
        }
        let current = u8::from(candidate.resolution());
        if current == 0 {
            return Err(GeometryError::new_err(
                "no single H3 cell contains the bounds (they straddle the resolution-0 grid)",
            ));
        }
        candidate = candidate
            .parent(h3_resolution(current - 1)?)
            .ok_or_else(|| GeometryError::new_err("no single H3 cell contains the bounds"))?;
    }
}

fn h3_corner_cell(lon: f64, lat: f64, resolution: Resolution) -> PyResult<CellIndex> {
    let (lon, lat) = crate::grid::canonical_lonlat(lon, lat);
    Ok(LatLng::new(lat, lon)
        .map_err(|error| GeometryError::new_err(error.to_string()))?
        .to_cell(resolution))
}

pub(in crate::py::cells) fn h3_cell_from_xy(
    lon: f64,
    lat: f64,
    resolution: Resolution,
) -> PyResult<PyH3Cell> {
    let (lon, lat) = crate::grid::canonical_lonlat(lon, lat);
    let latlng =
        LatLng::new(lat, lon).map_err(|error| GeometryError::new_err(error.to_string()))?;
    Ok(PyH3Cell {
        cell: latlng.to_cell(resolution),
    })
}

pub(in crate::py::cells) fn h3_cell_index(cell: &Bound<'_, PyAny>) -> PyResult<CellIndex> {
    parse_h3_index(cell, |value| {
        value
            .cast_exact::<PyH3Cell>()
            .ok()
            .map(|cell| cell.get().cell)
    })
}

/// Cover a geometry with H3 cells at ``resolution``.
///
/// The result is a ``CellArray`` of cells satisfying ``cell_rule``. Keep the
/// source geometry separately and use the free ``gm.*`` predicates for exact
/// geometry questions.
///
/// Parameters
/// ----------
/// geom : Geometry or GeometryArray
///     Geometry to cover (WGS84 lon/lat or projected). An array returns one
///     grouped cell row per input geometry.
///
/// resolution : int
///     H3 resolution (``0``-``15``; finer at higher values).
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
///     Hard cap on returned cells and retained result allocation. Discarded
///     candidates do not consume it; array calls apply it cumulatively across
///     groups. Pass ``None`` for unlimited output (bounded only by memory).
///
/// Returns
/// -------
/// CellArray or Groups of CellArray
///     A scalar returns its coverage; an array returns one cell group per row.
///
/// Raises
/// ------
/// GeometryError
///     If the geometry, depth, or a coverage parameter is invalid, or if
///     the covering would exceed ``max_cells``.
/// InvalidGeometryError
///     If the geometry is empty or coordinates leave the lon/lat domain.
///
/// See Also
/// --------
/// h3_cells : Build H3 cells from lon/lat columns.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
/// >>> cov = gm.h3_cover(p, resolution=7)
/// >>> cell = cov[0]
/// >>> assert cell is not None
/// >>> (len(cov), cell.token)
/// (1, '872830828ffffff')
#[pyfunction]
#[pyo3(
    signature = (geom, resolution, *, cell_rule = CellRule::Overlap, max_cells = crate::grid::GRID_MAX_CELLS),
    text_signature = "(geom, resolution, *, cell_rule='overlap', max_cells=1000000)"
)]
pub(super) fn h3_cover(
    py: Python<'_>,
    geom: &Bound<'_, PyAny>,
    resolution: &Bound<'_, PyAny>,
    cell_rule: CellRule,
    #[pyo3(from_py_with = crate::strict_optional_i64)] max_cells: Option<i64>,
) -> PyResult<Py<PyAny>> {
    let max_cells = super::super::coverage_ops::parse_max_cells(max_cells)?;
    let resolution = h3::parse_h3_resolution(resolution)?;
    grid_cover_dispatch(
        py,
        geom,
        GridKind::H3Cell,
        max_cells,
        |geometry, effective_max_cells| {
            let cells = build_coverage(geometry, resolution, cell_rule, effective_max_cells)?;
            Ok(collect_cover_ids(
                SortedCells::from_sorted(cells)
                    .into_vec()
                    .into_iter()
                    .map(u64::from),
            ))
        },
    )
}
