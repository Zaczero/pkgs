#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! Callable cell constructors (`H3Cell(...)`, `S2Cell(...)`, ...).

use pyo3::exceptions::PyTypeError;
use pyo3::types::PyAny;

use super::geohash::{PyGeohashCell, geohash_cell_arg, parse_geohash_precision};
use super::h3::{PyH3Cell, h3_cell_from_xy, h3_cell_index, parse_h3_resolution};
use super::s2::{PyS2Cell, parse_s2_level, s2_cell_from_xy, s2_cell_id};
use super::tiles::{PyTile, parse_tile_zoom, tile_arg};
use super::*;
use crate::grid::geohash::Geohash;
use crate::grid::tile::Tile;
use crate::{
    I64Param, broadcast_coordinate_group, coordinate_input, coordinate_inputs_are_scalar,
    exact_geometry, exact_geometry_array, finite_coordinate_required, lonlat_shape, point_xy,
};

pub(crate) trait CellArrayId {
    fn into_cell_array_id(self) -> u64;
}

impl CellArrayId for PyH3Cell {
    fn into_cell_array_id(self) -> u64 {
        u64::from(self.cell)
    }
}

impl CellArrayId for PyS2Cell {
    fn into_cell_array_id(self) -> u64 {
        self.cell.raw()
    }
}

impl CellArrayId for PyGeohashCell {
    fn into_cell_array_id(self) -> u64 {
        self.cell.identity_key()
    }
}

impl CellArrayId for PyTile {
    fn into_cell_array_id(self) -> u64 {
        self.cell.id()
    }
}

fn reject_lonlat_with_depth_only(lat: Option<&Bound<'_, PyAny>>, depth_kw: &str) -> PyResult<()> {
    if lat.is_some() {
        return Err(PyTypeError::new_err(format!(
            "lat is only valid with {depth_kw} for coordinate construction"
        )));
    }
    Ok(())
}

fn cell_from_lonlat_or_point<C>(
    lon: &Bound<'_, PyAny>,
    lat: Option<&Bound<'_, PyAny>>,
    depth_kw: &str,
    from_xy: impl FnOnce(f64, f64) -> PyResult<C>,
) -> PyResult<C> {
    if exact_geometry(lon).is_some() && lat.is_some() {
        return Err(PyTypeError::new_err(
            "lat must not be provided when the first argument is a geometry",
        ));
    }
    if let Some(lat) = lat {
        let lon = finite_coordinate_required("longitude", lon)?;
        let lat = finite_coordinate_required("latitude", lat)?;
        crate::boundary::geographic::validate_lonlat_xy(lon, lat)?;
        return from_xy(lon, lat);
    }
    if let Some(geometry) = exact_geometry(lon) {
        let shape = lonlat_shape(geometry)?;
        let (lon, lat) = point_xy(&shape)?;
        return from_xy(lon, lat);
    }
    Err(PyTypeError::new_err(format!(
        "{depth_kw} requires a point geometry or a lon, lat pair"
    )))
}

fn construct_cell<C, D>(
    lon: &Bound<'_, PyAny>,
    lat: Option<&Bound<'_, PyAny>>,
    depth: Option<&Bound<'_, PyAny>>,
    depth_kw: &str,
    parse_depth: impl FnOnce(&Bound<'_, PyAny>) -> PyResult<D>,
    from_xy: impl FnOnce(f64, f64, D) -> PyResult<C>,
    parse_cell: impl FnOnce(&Bound<'_, PyAny>) -> PyResult<C>,
) -> PyResult<C> {
    if let Some(depth) = depth {
        let depth = parse_depth(depth)?;
        return cell_from_lonlat_or_point(lon, lat, depth_kw, |lon, lat| from_xy(lon, lat, depth));
    }
    reject_lonlat_with_depth_only(lat, depth_kw)?;
    parse_cell(lon)
}

pub(crate) fn construct_h3_cell(
    lon: &Bound<'_, PyAny>,
    lat: Option<&Bound<'_, PyAny>>,
    resolution: Option<&Bound<'_, PyAny>>,
) -> PyResult<PyH3Cell> {
    construct_cell(
        lon,
        lat,
        resolution,
        "resolution",
        parse_h3_resolution,
        h3_cell_from_xy,
        |cell| {
            Ok(PyH3Cell {
                cell: h3_cell_index(cell)?,
            })
        },
    )
}

pub(crate) fn construct_s2_cell(
    lon: &Bound<'_, PyAny>,
    lat: Option<&Bound<'_, PyAny>>,
    level: Option<&Bound<'_, PyAny>>,
) -> PyResult<PyS2Cell> {
    construct_cell(
        lon,
        lat,
        level,
        "level",
        parse_s2_level,
        s2_cell_from_xy,
        |cell| {
            Ok(PyS2Cell {
                cell: s2_cell_id(cell)?,
            })
        },
    )
}

pub(crate) fn construct_geohash_cell(
    lon: &Bound<'_, PyAny>,
    lat: Option<&Bound<'_, PyAny>>,
    precision: Option<&Bound<'_, PyAny>>,
) -> PyResult<PyGeohashCell> {
    construct_cell(
        lon,
        lat,
        precision,
        "precision",
        parse_geohash_precision,
        |lon, lat, precision| {
            Ok(PyGeohashCell {
                cell: Geohash::from_lonlat(lon, lat, precision),
            })
        },
        |cell| {
            Ok(PyGeohashCell {
                cell: geohash_cell_arg(cell)?,
            })
        },
    )
}

fn tile_from_xyz(lon: &Bound<'_, PyAny>, lat: &Bound<'_, PyAny>, zoom: u8) -> PyResult<PyTile> {
    let x = crate::non_negative_int("tile", "x", py_i64_required("x", lon)?)?;
    let y = crate::non_negative_int("tile", "y", py_i64_required("y", lat)?)?;
    let limit = 1_i64 << i64::from(zoom);
    if x >= limit || y >= limit {
        return Err(GeometryError::new_err(format!(
            "tile x and y must be < 2**zoom ({limit}) at zoom {zoom}, got ({x}, {y})"
        )));
    }
    Ok(PyTile {
        cell: Tile {
            z: zoom,
            x: x as u32,
            y: y as u32,
        },
    })
}

pub(crate) fn construct_tile(
    value: Option<&Bound<'_, PyAny>>,
    lon: Option<&Bound<'_, PyAny>>,
    lat: Option<&Bound<'_, PyAny>>,
    zoom: Option<&Bound<'_, PyAny>>,
    x: Option<&Bound<'_, PyAny>>,
    y: Option<&Bound<'_, PyAny>>,
) -> PyResult<PyTile> {
    // Explicit tile coordinates are keyword-only (`x=`, `y=`, `zoom=`), and
    // geographic coordinates are named too. Two numeric values do not carry a
    // coordinate-frame tag, so neither form may be inferred from their shape.
    if x.is_some() || y.is_some() {
        if value.is_some() || lon.is_some() || lat.is_some() {
            return Err(PyTypeError::new_err(
                "pass either value/lon=/lat= or keyword x=/y=/zoom=, not both",
            ));
        }
        let (Some(x), Some(y), Some(zoom)) = (x, y, zoom) else {
            return Err(PyTypeError::new_err(
                "tile coordinates require all of x=, y=, and zoom=",
            ));
        };
        return tile_from_xyz(x, y, parse_tile_zoom(zoom)?);
    }
    if let Some(value) = value {
        if lon.is_some() || lat.is_some() {
            return Err(PyTypeError::new_err(
                "pass either value or lon=/lat=/zoom=, not both",
            ));
        }
        if let Some(zoom) = zoom {
            let geometry = exact_geometry(value).ok_or_else(|| {
                PyTypeError::new_err(
                    "zoom with value requires a Point geometry; use lon=/lat=/zoom= for geographic coordinates",
                )
            })?;
            let shape = lonlat_shape(geometry)?;
            let (lon, lat) = point_xy(&shape)?;
            return Ok(PyTile {
                cell: Tile::from_lonlat(lon, lat, parse_tile_zoom(zoom)?),
            });
        }
        return Ok(PyTile {
            cell: tile_arg(value)?,
        });
    }
    let Some(lon) = lon else {
        return Err(PyTypeError::new_err(
            "Tile requires a tile id/quadkey, a Point with zoom=, lon=/lat=/zoom=, or x=/y=/zoom=",
        ));
    };
    let (Some(lat), Some(zoom)) = (lat, zoom) else {
        return Err(PyTypeError::new_err(
            "geographic tile coordinates require lon=, lat=, and zoom=",
        ));
    };
    let lon = finite_coordinate_required("longitude", lon)?;
    let lat = finite_coordinate_required("latitude", lat)?;
    crate::boundary::geographic::validate_lonlat_xy(lon, lat)?;
    Ok(PyTile {
        cell: Tile::from_lonlat(lon, lat, parse_tile_zoom(zoom)?),
    })
}

/// Bulk cell builder returning a typed [`PyCellArray`] (mirrors `points`).
///
/// Encodes the canonical cell-array id directly from each Rust cell wrapper,
/// avoiding the scalar `PyO3` object constructor/parser path.
pub(crate) fn dispatch_grid_cell_array<'py, C, D>(
    py: Python<'py>,
    values: &Bound<'py, PyAny>,
    lat: Option<&Bound<'py, PyAny>>,
    depth: &Bound<'py, PyAny>,
    kind: GridKind,
    class_name: &str,
    depth_kw: &str,
    validate_depth: impl Fn(i64) -> PyResult<D>,
    from_xy: impl Fn(f64, f64, D) -> PyResult<C>,
) -> PyResult<PyCellArray>
where
    C: CellArrayId,
{
    if let Some(array) = exact_geometry_array(values) {
        if lat.is_some() {
            return Err(PyTypeError::new_err(
                "lat must not be provided when values is a geometry array",
            ));
        }
        let points = super::grid_lonlat_points(array)?;
        let depth = I64Param::parse(depth, depth_kw, points.len())?;
        let mut ids = Vec::with_capacity(points.len());
        for (row, point) in points.iter().enumerate() {
            // Validate each depth once (no pre-pass + re-validate).
            let depth = validate_depth(depth.get(row))?;
            ids.push(from_xy(point.x, point.y, depth)?.into_cell_array_id());
        }
        return Ok(PyCellArray::from_trusted_ids(kind, ids));
    }
    let lat = lat
        .ok_or_else(|| PyTypeError::new_err("lat is required when values is a longitude column"))?;
    let mut lon = coordinate_input(py, values, "lon")?;
    let mut lat = coordinate_input(py, lat, "lat")?;
    if coordinate_inputs_are_scalar(&lon, &lat, None, None) {
        return Err(GeometryError::new_err(format!(
            "cells expects coordinate columns; use {class_name}(lon, lat, {depth_kw}=...) for a single cell"
        )));
    }
    broadcast_coordinate_group([(&mut lon, "lon"), (&mut lat, "lat")], "lon and lat")?;
    let len = lon.values.len();
    let depth = I64Param::parse(depth, depth_kw, len)?;
    let mut ids = Vec::with_capacity(len);
    for (row, (&lon, &lat)) in lon.values.iter().zip(lat.values.iter()).enumerate() {
        crate::boundary::geographic::validate_lonlat_xy(lon, lat)?;
        let depth = validate_depth(depth.get(row))?;
        ids.push(from_xy(lon, lat, depth)?.into_cell_array_id());
    }
    Ok(PyCellArray::from_trusted_ids(kind, ids))
}
