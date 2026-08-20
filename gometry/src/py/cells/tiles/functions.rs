//! Top-level `tile_*` free functions.

use pyo3::types::PyAny;

use crate::grid::tile::{TILE_MAX_LATITUDE, TILE_MAX_ZOOM, Tile, root as tile_root};
use crate::py::cells::tiles::cell::{PyTile, tile_arg};
use crate::py::cells::tiles::coverage::tile_cell_array;
use crate::py::cells::{
    Bound, GridKind, PyCellArray, PyResult, Python, bounding_query_bounds,
    dispatch_grid_cell_array, pyfunction,
};
use crate::py::errors::InvalidGeometryError;

/// Build tiles from parallel lon/lat columns.
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
///     Latitudes outside the Web Mercator domain (±85.051129°) raise
///     ``InvalidGeometryError`` (no silent clamp).
///
/// zoom : int or sequence of int
///     Zoom level (0-29; finer at higher values). A scalar broadcasts to
///     every row; an array supplies one zoom per row.
///
/// Returns
/// -------
/// CellArray of Tile
///     One tile per input coordinate.
///
/// Raises
/// ------
/// GeometryError
///     If the input is scalar, zoom is invalid, or coordinate columns have
///     different lengths.
/// InvalidGeometryError
///     If coordinates are non-finite or leave the lon/lat domain, or a
///     latitude is outside the Web Mercator domain (±85.051129°).
///
/// See Also
/// --------
/// Tile : Build a single tile.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> tile = gm.Tile(lon=-105.939, lat=35.687, zoom=9)
/// >>> (tile.zoom, tile.x, tile.y)
/// (9, 105, 201)
#[pyfunction]
#[pyo3(
    signature = (values, lat = None, *, zoom),
    text_signature = "(values, lat=None, *, zoom)"
)]
pub(super) fn tile_cells(
    py: Python<'_>,
    values: &Bound<'_, PyAny>,
    lat: Option<&Bound<'_, PyAny>>,
    zoom: &Bound<'_, PyAny>,
) -> PyResult<PyCellArray> {
    dispatch_grid_cell_array(
        py,
        values,
        lat,
        zoom,
        GridKind::Tile,
        "Tile",
        "zoom",
        super::cell::parse_tile_zoom_value,
        |lon, lat, zoom| {
            let cell = Tile::from_lonlat(lon, lat, zoom).ok_or_else(|| {
                InvalidGeometryError::new_err(format!(
                    "latitude {lat} is outside the Web Mercator domain ±{TILE_MAX_LATITUDE} degrees"
                ))
            })?;
            Ok(PyTile { cell })
        },
    )
}

/// Return the deepest single tile containing a geometry or lon/lat bounds.
///
/// The mercantile ``bounding_tile``: walks corner tiles up to their common
/// ancestor. Bounds spanning hemispheres bottom out at the ``z0`` root.
/// Latitudes outside the Web Mercator domain raise
/// ``InvalidGeometryError`` (no silent clamp).
///
/// Parameters
/// ----------
/// value : Geometry, GeometryArray, or sequence of float
///     A geometry/array (non-WGS84 frames reproject), or a bare lon/lat
///     ``(minx, miny, maxx, maxy)`` bounds.
///
/// Returns
/// -------
/// Tile
///     The deepest tile whose rectangle contains the whole bounds.
///
/// Raises
/// ------
/// InvalidGeometryError
///     If the geometry is empty or coordinates leave the lon/lat domain.
/// GeometryError
///     If bare bounds are not ordered min <= max.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> berlin = gm.box(13.3, 52.4, 13.5, 52.6, crs=4326)
/// >>> tile = gm.tile_bounding_cell(berlin)
/// >>> (tile.zoom, tile.x, tile.y)
/// (5, 17, 10)
#[pyfunction]
pub(super) fn tile_bounding_cell(value: &Bound<'_, PyAny>) -> PyResult<PyTile> {
    let bounds = bounding_query_bounds(value)?;
    let tile_at = |lon: f64, lat: f64| {
        Tile::from_lonlat(lon, lat, TILE_MAX_ZOOM).ok_or_else(|| {
            InvalidGeometryError::new_err(format!(
                "latitude {lat} is outside the Web Mercator domain ±{TILE_MAX_LATITUDE} degrees"
            ))
        })
    };
    let mut sw = tile_at(bounds.minx(), bounds.miny())?;
    let mut ne = tile_at(bounds.maxx(), bounds.maxy())?;
    while sw != ne {
        if sw.z == 0 {
            return Ok(PyTile { cell: tile_root() });
        }
        sw = sw.parent_at(sw.z - 1);
        ne = ne.parent_at(ne.z - 1);
    }
    Ok(PyTile { cell: sw })
}

grid_free_functions! {
    @set_algebra {
        cell_set_arg: tile_cell_set_arg,
        cell_type: Tile,
        label: "tile",
        cell_doc: "Tile",
        item_doc: "Tile, int, str, or iterable of those",
        contract_doc: "",
        parse_error_doc: "If an id or token is not a valid tile cell.",
        parse_cell: tile_arg,
        array: |cells| tile_cell_array(cells),
        union: tile_union,
        intersection: tile_intersection,
        difference: tile_difference,
        example_union: r"
>>> import gometry as gm
>>> p = gm.Point(-122.4194, 37.7749, crs=4326)
>>> cells = [cell for cell in gm.tile_cover(p, zoom=10) if cell is not None]
>>> len(gm.tile_union(cells, cells))
1
",
        example_intersection: r"
>>> import gometry as gm
>>> p = gm.Point(-122.4194, 37.7749, crs=4326)
>>> cells = [cell for cell in gm.tile_cover(p, zoom=10) if cell is not None]
>>> len(gm.tile_intersection(cells, cells))
1
",
        example_difference: r"
>>> import gometry as gm
>>> p = gm.Point(-122.4194, 37.7749, crs=4326)
>>> cells = [cell for cell in gm.tile_cover(p, zoom=10) if cell is not None]
>>> len(gm.tile_difference(cells, []))
1
",
    }
}
