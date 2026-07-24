#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! Top-level `geohash_*` free functions.

use pyo3::types::PyAny;

use super::super::*;
use super::cell::{PyGeohashCell, geohash_cell_arg};
use super::coverage::geohash_cell_array;
use crate::grid::geohash::{GEOHASH_MAX_PRECISION, Geohash};
use crate::py::cells::{GridKind, PyCellArray, bounding_query_bounds, dispatch_grid_cell_array};
use crate::py::errors::GeometryError;

/// Build geohash cells from parallel lon/lat columns.
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
/// precision : int or sequence of int
///     Geohash precision (1-12 characters; finer at higher values). A scalar
///     broadcasts to every row; an array supplies one precision per row.
///
/// Returns
/// -------
/// CellArray of GeohashCell
///     One cell per input coordinate.
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
        super::cell::parse_geohash_precision_value,
        |lon, lat, precision| {
            Ok(PyGeohashCell {
                cell: Geohash::from_lonlat(lon, lat, precision),
            })
        },
    )
}

/// Return the deepest single geohash cell containing a geometry or lon/lat bounds.
///
/// Walks the corner cells up to their common prefix. There is no global
/// geohash root, so bounds that straddle the precision-1 grid have no
/// containing cell and raise.
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
    let bounds = bounding_query_bounds(value)?;
    let sw = Geohash::from_lonlat(bounds.minx(), bounds.miny(), GEOHASH_MAX_PRECISION);
    let ne = Geohash::from_lonlat(bounds.maxx(), bounds.maxy(), GEOHASH_MAX_PRECISION);
    for precision in (1..=GEOHASH_MAX_PRECISION).rev() {
        let (a, b) = (sw.parent_at(precision), ne.parent_at(precision));
        if a == b {
            return Ok(PyGeohashCell { cell: a });
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
        item_doc: "GeohashCell, str, or iterable of those",
        contract_doc: "",
        parse_cell: geohash_cell_arg,
        array: |cells| geohash_cell_array(cells),
        union: geohash_union,
        intersection: geohash_intersection,
        difference: geohash_difference,
        example_union: r"
>>> import gometry as gm
>>> p = gm.Point(-122.4194, 37.7749, crs=4326)
>>> cells = list(gm.geohash_cover(p, precision=6).cells)
>>> len(gm.geohash_union(cells, cells))
1
",
        example_intersection: r"
>>> import gometry as gm
>>> p = gm.Point(-122.4194, 37.7749, crs=4326)
>>> cells = list(gm.geohash_cover(p, precision=6).cells)
>>> len(gm.geohash_intersection(cells, cells))
1
",
        example_difference: r"
>>> import gometry as gm
>>> p = gm.Point(-122.4194, 37.7749, crs=4326)
>>> cells = list(gm.geohash_cover(p, precision=6).cells)
>>> len(gm.geohash_difference(cells, []))
1
",
    }
}
