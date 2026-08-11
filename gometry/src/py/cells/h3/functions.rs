use crate::py::cells::h3::{
    CellIndex, LatLng, PyH3Cell, Resolution, h3_resolution, h3_resolution_from_i64, parse_h3_index,
};
use crate::py::cells::{
    Bound, CellRule, GeometryError, GridKind, H3_MAX_RESOLUTION, Py, PyAny, PyCellArray, PyResult,
    Python, bounding_query_bounds, dispatch_grid_cell_array, grid_cover_dispatch, pyfunction,
};
use crate::py::errors::InvalidGeometryError;

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
/// >>> (len(cells), cells[0].token)
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
/// Walks all four corner cells up to a common ancestor, then verifies the
/// candidate's actual H3 boundary covers the whole bounds.
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
    // Prove the antimeridian-aware *region*, not merely source vertices.
    // Vertex-only sampling was the C14 defect: a MultiPoint on the diagonal of
    // a rectangle returned a root cell that failed to cover the opposite
    // corners of the promised bounding box.
    let samples = bounding_cell_region_samples(value)?;
    let max_res = h3_resolution(H3_MAX_RESOLUTION)?;
    let mut corners: Vec<CellIndex> = samples
        .iter()
        .map(|&(lon, lat)| h3_corner_cell(lon, lat, max_res))
        .collect::<PyResult<_>>()?;
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
        if h3_cell_covers_samples(candidate, &samples) {
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

/// Lon/lat points that a bounding cell must contain — the four corners of the
/// antimeridian-aware region for ordinary input, or the short-arc vertex set
/// when linework crosses ±180 (planar envelope corners take the long way).
fn bounding_cell_region_samples(value: &Bound<'_, PyAny>) -> PyResult<Vec<(f64, f64)>> {
    use crate::geometry::Shape;
    use crate::{exact_geometry, exact_geometry_array, lonlat_shape, validate_lonlat_shape};

    if let Some(geometry) = exact_geometry(value) {
        let shape = lonlat_shape(geometry)?;
        validate_lonlat_shape(&shape)?;
        return region_samples_for_shape(&shape);
    }
    if let Some(array) = exact_geometry_array(value) {
        let crs = array.crs_str();
        let mut parts: Vec<Shape> = Vec::new();
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
        let collection = if parts.len() == 1 {
            parts.pop().expect("one part")
        } else {
            Shape::GeometryCollection(parts)
        };
        return region_samples_for_shape(&collection);
    }
    let bounds = bounding_query_bounds(value)?;
    Ok(rectangle_corner_samples(bounds))
}

/// Region witnesses for one lon/lat shape.
///
/// Non-crossing shapes prove the full lon/lat rectangle (all four corners),
/// never merely the source vertices. Crossing / wrap shapes use the vertex set
/// so the short-arc spherical extent is checked rather than the long-way
/// planar envelope.
fn region_samples_for_shape(shape: &crate::geometry::Shape) -> PyResult<Vec<(f64, f64)>> {
    let mut vertex_samples = Vec::new();
    shape.for_each_point(|point| {
        vertex_samples.push((point.x, point.y));
    });
    if vertex_samples.is_empty() {
        return Err(InvalidGeometryError::new_err(
            "bounding cell requires a non-empty geometry",
        ));
    }
    let Some(bounds) = shape.bounds() else {
        return Ok(vertex_samples);
    };
    if bounds.minx() > bounds.maxx() || shape.crosses_antimeridian() {
        return Ok(vertex_samples);
    }
    Ok(rectangle_corner_samples(bounds))
}

fn rectangle_corner_samples(bounds: crate::geometry::Bounds) -> Vec<(f64, f64)> {
    vec![
        (bounds.minx(), bounds.miny()),
        (bounds.maxx(), bounds.miny()),
        (bounds.maxx(), bounds.maxy()),
        (bounds.minx(), bounds.maxy()),
    ]
}

fn h3_corner_cell(lon: f64, lat: f64, resolution: Resolution) -> PyResult<CellIndex> {
    Ok(LatLng::new(lat, lon)
        .map_err(|error| GeometryError::new_err(error.to_string()))?
        .to_cell(resolution))
}

/// Whether every sample lies inside `cell` via hierarchical id containment
/// (`LatLng::to_cell` at the cell's resolution). Avoids planar `covers` on
/// antimeridian-crossing cell polygons, which falsely reject near ±180.
fn h3_cell_covers_samples(cell: CellIndex, samples: &[(f64, f64)]) -> bool {
    let res = cell.resolution();
    samples
        .iter()
        .all(|&(lon, lat)| LatLng::new(lat, lon).is_ok_and(|latlng| latlng.to_cell(res) == cell))
}

pub(super) fn ensure_h3_uncompact_budget(
    cells: impl IntoIterator<Item = CellIndex>,
    resolution: Resolution,
) -> PyResult<usize> {
    let estimated =
        usize::try_from(CellIndex::uncompact_size(cells, resolution)).unwrap_or(usize::MAX);
    crate::grid::ensure_uncompact_budget(estimated).map_err(super::super::uncompact_budget_err)?;
    Ok(estimated)
}

pub(in crate::py::cells) fn h3_cell_from_xy(
    lon: f64,
    lat: f64,
    resolution: Resolution,
) -> PyResult<PyH3Cell> {
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
/// The result carries both ``cells`` — exactly the cells
/// satisfying ``cell_rule`` (join keys, bins, visualization) — and the
/// exact membership predicates ``covers``/``contains``/``intersects``,
/// which always answer against the source geometry.
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
///     Upper bound on emitted cells. Raise to allow a larger covering, or
///     pass ``None`` for unlimited (bounded only by memory).
///
/// Returns
/// -------
/// H3Coverage or Groups of CellArray
///     A scalar returns its coverage; an array returns one cell group per row.
///
/// Raises
/// ------
/// GeometryError
///     If the geometry, depth, or a coverage parameter is invalid, or if
///     the covering would exceed ``max_cells``.
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
/// >>> (len(cov.cells), cov.contains(p), cov.cells[0].token)
/// (1, True, '872830828ffffff')
#[pyfunction]
#[pyo3(
    signature = (geom, resolution, *, cell_rule = CellRule::Overlap, max_cells = 1_000_000),
    text_signature = "(geom, resolution, *, cell_rule='overlap', max_cells=1000000)"
)]
pub(super) fn h3_cover(
    py: Python<'_>,
    geom: &Bound<'_, PyAny>,
    resolution: &Bound<'_, PyAny>,
    cell_rule: CellRule,
    max_cells: Option<i64>,
) -> PyResult<Py<PyAny>> {
    let max_cells = super::super::coverage_ops::parse_max_cells(max_cells)?;
    grid_cover_dispatch(
        py,
        geom,
        GridKind::H3Cell,
        max_cells,
        |geometry| super::build_coverage(geometry, resolution, cell_rule, max_cells),
        |coverage| {
            coverage
                .cells
                .iter()
                .map(|cell| u64::from(cell.cell))
                .collect()
        },
    )
}
