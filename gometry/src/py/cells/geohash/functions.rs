//! Top-level `geohash_*` free functions.

use pyo3::types::PyAny;

use crate::grid::geohash::{GEOHASH_MAX_PRECISION, Geohash};
use crate::py::cells::geohash::cell::{PyGeohashCell, geohash_cell_arg};
use crate::py::cells::geohash::coverage::geohash_cell_array;
use crate::py::cells::{
    Bound, CellArrayError, GridKind, PyCellArray, PyResult, Python, bounding_query_region,
    dispatch_grid_cell_array, pyfunction,
};
use crate::py::errors::GeometryError;

/// Build geohash cells from points, coordinate rows, or parallel lon/lat columns.
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
/// precision : int or sequence of int
///     Geohash precision (1-12 characters; finer at higher values). A scalar
///     broadcasts to every row; an array supplies one precision per row.
///
/// Returns
/// -------
/// CellArray of GeohashCell
///     One cell per input coordinate.
///
/// Raises
/// ------
/// GeometryError
///     If the input is scalar, precision is invalid, or coordinate columns
///     have different lengths.
/// InvalidGeometryError
///     If coordinates are non-finite or leave the lon/lat domain.
///
/// See Also
/// --------
/// GeohashCell : Build a single cell.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.GeohashCell(112.5584, 37.8324, precision=9).token
/// 'ww8p1r4t8'
#[pyfunction]
#[pyo3(
    signature = (values, lat = None, *, precision),
    text_signature = "(values, lat=None, *, precision)"
)]
pub(super) fn geohash_cells(
    py: Python<'_>,
    values: &Bound<'_, PyAny>,
    lat: Option<&Bound<'_, PyAny>>,
    precision: &Bound<'_, PyAny>,
) -> PyResult<PyCellArray> {
    dispatch_grid_cell_array(
        py,
        values,
        lat,
        precision,
        GridKind::GeohashCell,
        "GeohashCell",
        "precision",
        |value| {
            CellArrayError::depth(
                value,
                "geohash precision",
                "precision",
                1,
                i64::from(GEOHASH_MAX_PRECISION),
            )
        },
        |lon, lat, precision| {
            Ok::<_, CellArrayError>(PyGeohashCell {
                cell: Geohash::from_lonlat(lon, lat, precision),
            })
        },
    )
}

/// Return the deepest single geohash cell containing a geometry or lon/lat bounds.
///
/// Descends the cell containing the lower corner and accepts it only after its
/// complete rectangle contains both axes of the query. There is no global
/// wrapped geohash cell, so an antimeridian-spanning region has no single-cell
/// result and raises.
///
/// Parameters
/// ----------
/// value : Geometry, GeometryArray, or sequence of float
///     A geometry/array (non-WGS84 frames reproject), or a bare lon/lat
///     ``(minx, miny, maxx, maxy)`` bounds.
///
/// Returns
/// -------
/// GeohashCell
///     The deepest cell whose rectangle contains the whole bounds.
///
/// Raises
/// ------
/// InvalidGeometryError
///     If the geometry is empty or coordinates leave the lon/lat domain.
/// GeometryError
///     If no single geohash cell contains the bounds, or bare bounds are
///     not ordered min <= max.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> berlin = gm.box(13.3, 52.4, 13.5, 52.6, crs=4326)
/// >>> gm.geohash_bounding_cell(berlin).token
/// 'u33'
#[pyfunction]
pub(super) fn geohash_bounding_cell(value: &Bound<'_, PyAny>) -> PyResult<PyGeohashCell> {
    let query = bounding_query_region(value)?;
    if query.crosses_antimeridian {
        return Err(GeometryError::new_err(
            "no single geohash cell contains the bounds (they span the antimeridian)",
        ));
    }
    let bounds = query.bounds;
    let finest = Geohash::from_lonlat(bounds.minx(), bounds.miny(), GEOHASH_MAX_PRECISION);
    for precision in (1..=GEOHASH_MAX_PRECISION).rev() {
        let cell = finest.parent_at(precision);
        let cell_bounds = cell.bounds();
        if cell_bounds.minx() <= bounds.minx()
            && bounds.maxx() <= cell_bounds.maxx()
            && cell_bounds.miny() <= bounds.miny()
            && bounds.maxy() <= cell_bounds.maxy()
        {
            return Ok(PyGeohashCell { cell });
        }
    }
    Err(GeometryError::new_err(
        "no single geohash cell contains the bounds (they straddle the precision-1 grid)",
    ))
}

grid_free_functions! {
    @set_algebra {
        cell_set_arg: geohash_cell_set_arg,
        cell_type: Geohash,
        label: "geohash",
        cell_doc: "GeohashCell",
        item_doc: "GeohashCell, str, or iterable of those with missing values skipped",
        contract_doc: "",
        parse_error_doc: "If a cell or token is not a valid geohash cell.",
        parse_cell: geohash_cell_arg,
        array: |cells| geohash_cell_array(cells),
        union: geohash_union,
        intersection: geohash_intersection,
        difference: geohash_difference,
        example_union: r">>> import gometry as gm
>>> p = gm.Point(-122.4194, 37.7749, crs=4326)
>>> cells = [cell for cell in gm.geohash_cover(p, precision=6) if cell is not None]
>>> len(gm.geohash_union(cells, cells))
1
",
        example_intersection: r">>> import gometry as gm
>>> p = gm.Point(-122.4194, 37.7749, crs=4326)
>>> cells = [cell for cell in gm.geohash_cover(p, precision=6) if cell is not None]
>>> len(gm.geohash_intersection(cells, cells))
1
",
        example_difference: r">>> import gometry as gm
>>> p = gm.Point(-122.4194, 37.7749, crs=4326)
>>> cells = [cell for cell in gm.geohash_cover(p, precision=6) if cell is not None]
>>> len(gm.geohash_difference(cells, []))
1
",
    }
}
