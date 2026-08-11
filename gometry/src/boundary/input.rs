//! Scalar argument parsers shared across the `#[pyfunction]` surfaces.
//!
//! Small boundary helpers that coerce Python ints/floats/option values and
//! parse affine-transform / origin / epoch / accuracy arguments. Re-exported at
//! the crate root so every module reaches them through `use super::*`.
#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]

use pyo3::FromPyObject;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict};

use crate::py::errors::GeometryError;
use crate::{Positive, Shape, coordinate_values, crs};

pub(crate) fn py_i64_required(name: &str, value: &Bound<'_, PyAny>) -> PyResult<i64> {
    // A non-int where an int is required is a plain `TypeError` (like the
    // float lane) — `GeometryError` is for bad *values*: an oversized int has
    // the right type and stays a value error.
    if value.cast::<pyo3::types::PyInt>().is_err() {
        return Err(PyTypeError::new_err(format!("{name} must be an integer")));
    }
    value
        .extract::<i64>()
        .map_err(|_| GeometryError::new_err(format!("{name} is too large")))
}

/// Range-bounded boundary parser: extract an integer and enforce an
/// inclusive range, raising the caller's exact error — the one engine behind
/// precision/resolution/level/positive-count parsers. The closure builds the
/// full `PyErr`, so each surface owns its exact message (every lane is a
/// `GeometryError` parameter-value violation).
pub(crate) fn py_i64_bounded(
    name: &str,
    value: &Bound<'_, PyAny>,
    range: std::ops::RangeInclusive<i64>,
    error: &dyn Fn(i64) -> PyErr,
) -> PyResult<i64> {
    let parsed = py_i64_required(name, value)?;
    if range.contains(&parsed) {
        Ok(parsed)
    } else {
        Err(error(parsed))
    }
}

/// Optional sibling of [`py_i64_bounded`] (None/`None` → `default`,
/// which is trusted to be in range).
/// Canonical parameter-value violation for integer minimum bounds.
///
/// Attaches structured ``.param`` / ``.value`` so handlers can branch without
/// parsing the message (AGENTS error-attribute contract).
pub(crate) fn int_min_error(op: &str, param: &str, min: i64, got: i64) -> PyErr {
    let label = if op.is_empty() {
        param.to_owned()
    } else {
        format!("{op} {param}")
    };
    crate::py::errors::integer_parameter_error(
        format!("{label} must be >= {min}, got {got}"),
        param,
        got,
    )
}

/// Require ``value >= 0`` with the canonical ``{op} {param}`` message.
pub(crate) fn non_negative_int(op: &str, param: &str, value: i64) -> PyResult<i64> {
    if value >= 0 {
        Ok(value)
    } else {
        Err(int_min_error(op, param, 0, value))
    }
}

/// Require ``value >= 1`` with the canonical ``{op} {param}`` message.
pub(crate) fn positive_int(op: &str, param: &str, value: i64) -> PyResult<i64> {
    if value >= 1 {
        Ok(value)
    } else {
        Err(int_min_error(op, param, 1, value))
    }
}

/// Inclusive ``[min, max]`` integer gate with canonical bound messages.
pub(crate) fn bounded_int(
    op: &str,
    param: &str,
    value: i64,
    range: std::ops::RangeInclusive<i64>,
) -> PyResult<i64> {
    if range.contains(&value) {
        return Ok(value);
    }
    if value < *range.start() {
        Err(int_min_error(op, param, *range.start(), value))
    } else {
        Err(crate::py::errors::integer_parameter_error(
            format!("{op} {param} must be <= {}, got {value}", range.end()),
            param,
            value,
        ))
    }
}

/// An exactly-two float lane (any iterable or buffer, like `bounds`) shared
/// by the grid pair parameters; wrong type raises `TypeError`, wrong arity
/// `GeometryError`.
fn grid_pair(value: &Bound<'_, PyAny>, name: &str, expected: &str) -> PyResult<(f64, f64)> {
    let values = coordinate_values(value.py(), value, name)
        .map_err(|_| PyTypeError::new_err(format!("snap_to_grid {name} must be {expected}")))?;
    let [x, y] = values.as_slice() else {
        return Err(crate::py::errors::parameter_error(
            format!(
                "snap_to_grid {name} must be {expected}, got {} values",
                values.len()
            ),
            name,
        ));
    };
    Ok((*x, *y))
}

/// Boundary parser for `snap_to_grid` size: one positive finite float for
/// a square grid, or an `(sx, sy)` pair.
pub(crate) fn parse_grid_size(value: &Bound<'_, PyAny>) -> PyResult<(f64, f64)> {
    let pair = if let Ok(size) = value.extract::<f64>() {
        (size, size)
    } else {
        grid_pair(value, "size", "a positive number or an (sx, sy) pair")?
    };
    let sx = Positive::try_new("size", pair.0)?;
    let sy = Positive::try_new("size", pair.1)?;
    Ok((sx.get(), sy.get()))
}

/// Validate `snap_to_grid` origin: a finite `(x, y)` pair.
pub(crate) fn validate_grid_origin(origin: (f64, f64)) -> PyResult<(f64, f64)> {
    let (x, y) = origin;
    if x.is_finite() && y.is_finite() {
        Ok((x, y))
    } else {
        Err(crate::py::errors::float_parameter_error(
            format!("snap_to_grid origin must be finite, got ({x}, {y})"),
            "origin",
            if x.is_finite() { y } else { x },
        ))
    }
}

/// Parse `snap_to_grid` origin from a scalar-or-pair lane.
pub(crate) fn parse_grid_origin(value: &Bound<'_, PyAny>) -> PyResult<(f64, f64)> {
    validate_grid_origin(grid_pair(
        value,
        "origin",
        "a finite (x, y) pair of numbers",
    )?)
}

/// Non-optional `snap_to_grid` origin: Rust default ``(0.0, 0.0)``; explicit
/// Python ``None`` is rejected by [`FromPyObject`].
#[derive(Clone, Copy)]
pub struct GridOrigin(pub f64, pub f64);

impl GridOrigin {
    pub(crate) const DEFAULT: Self = Self(0.0, 0.0);

    pub(crate) const fn as_pair(self) -> (f64, f64) {
        (self.0, self.1)
    }
}

impl<'a, 'py> FromPyObject<'a, 'py> for GridOrigin {
    type Error = PyErr;

    fn extract(value: pyo3::Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        if value.is_none() {
            return Err(PyTypeError::new_err("origin cannot be None"));
        }
        let (x, y) = parse_grid_origin(&value)?;
        Ok(Self(x, y))
    }
}

/// Validate space-filling-curve `level` (grid order): `1..=32`, default 16
/// (the GeoPandas/DuckDB-interoperable order).
pub(crate) fn validate_curve_level(level: i64) -> PyResult<crate::curves::CurveLevel> {
    if !(1..=32).contains(&level) {
        return Err(crate::py::errors::integer_parameter_error(
            format!("curve level must be between 1 and 32, got {level}"),
            "level",
            level,
        ));
    }
    let level = u8::try_from(level).expect("level <= 32 fits u8");
    crate::curves::CurveLevel::try_new(level).map_err(|_| {
        crate::py::errors::integer_parameter_error(
            format!("curve level must be between 1 and 32, got {level}"),
            "level",
            i64::from(level),
        )
    })
}

/// `subdivide` budget: at least 8 vertices per part (anything lower cannot
/// represent a clipped polygon part).
pub(crate) fn validate_subdivide_max_vertices(max_vertices: i64) -> PyResult<usize> {
    bounded_int("subdivide", "max_vertices", max_vertices, 8..=i64::MAX)?;
    usize::try_from(max_vertices).map_err(|_| {
        crate::py::errors::integer_parameter_error(
            "subdivide max_vertices is too large",
            "max_vertices",
            max_vertices,
        )
    })
}

/// Boundary parser for `sample_points` count: any non-negative integer
/// (zero samples nothing).
pub(crate) fn parse_sample_count(value: &Bound<'_, PyAny>) -> PyResult<usize> {
    let count = py_i64_required("count", value)?;
    non_negative_int("sample_points", "count", count)?;
    usize::try_from(count).map_err(|_| {
        crate::py::errors::integer_parameter_error(
            "sample_points count is too large",
            "count",
            count,
        )
    })
}

/// Boundary parser for `sample_points` seed: any integer, folded into the
/// 64-bit stream state (negatives wrap two's-complement, deterministically).
pub(crate) fn parse_sample_seed(value: &Bound<'_, PyAny>) -> PyResult<u64> {
    Ok(py_i64_required("seed", value)? as u64)
}

/// Transform options for the geometry/array `to_crs` surface: the source epoch
/// is the geometry's own metadata (not a kwarg) and the output epoch is set by
/// the caller, so this parser carries no `source_epoch`/`target_epoch` and
/// leaves both `None` for the caller to fill.
pub(crate) fn parse_geometry_transform_options(
    area_of_interest: Option<&Bound<'_, PyAny>>,
    authority: Option<String>,
    accuracy: Option<&Bound<'_, PyAny>>,
    allow_ballpark: Option<bool>,
    only_best: Option<bool>,
    force_over: bool,
) -> PyResult<crs::TransformOptions> {
    Ok(crs::TransformOptions {
        area_of_interest: parse_area(area_of_interest, "area_of_interest")?,
        source_epoch: None,
        target_epoch: None,
        authority,
        accuracy: accuracy_option(accuracy)?,
        allow_ballpark,
        only_best,
        force_over,
    })
}

/// Parse an angle argument, converting degrees to radians unless `radians` is
/// set.
pub(crate) fn angle_radians(name: &str, value: &Bound<'_, PyAny>, radians: bool) -> PyResult<f64> {
    let angle = finite_f64_required(name, value)?;
    Ok(if radians { angle } else { angle.to_radians() })
}

/// Parse a 6-element `(a, b, d, e, xoff, yoff)` affine matrix of finite floats.
pub(crate) fn parse_affine_matrix(matrix: &Bound<'_, PyAny>) -> PyResult<[f64; 6]> {
    let items = matrix.try_iter().map_err(|_| {
        GeometryError::new_err("affine matrix must be a sequence of 6 finite numbers")
    })?;
    let names = ["a", "b", "d", "e", "xoff", "yoff"];
    let mut values = [0.0; 6];
    let mut count = 0;
    for item in items {
        let item = item?;
        if count == 6 {
            return Err(GeometryError::new_err(
                "affine matrix must have exactly 6 elements",
            ));
        }
        values[count] = finite_f64_required(names[count], &item)?;
        count += 1;
    }
    if count != 6 {
        return Err(GeometryError::new_err(
            "affine matrix must have exactly 6 elements",
        ));
    }
    Ok(values)
}

/// A parsed `origin` argument for the ergonomic affine helpers: parsed
/// once per call, resolved per shape — which also lets array bindings
/// notice when the origin is per-shape (`Centroid`/`Center`, an identity
/// for packed point rows) versus one fixed pivot.
#[derive(Clone, Copy)]
pub enum OriginSpec {
    Centroid,
    Center,
    Fixed(f64, f64),
}

impl OriginSpec {
    pub(crate) fn resolve_shape(self, shape: &Shape) -> crate::error::Result<(f64, f64)> {
        Ok(match self {
            Self::Centroid => shape.centroid_xy()?,
            Self::Center => shape.bounds_center_xy(),
            Self::Fixed(x, y) => (x, y),
        })
    }
}

impl<'a, 'py> FromPyObject<'a, 'py> for OriginSpec {
    type Error = PyErr;

    fn extract(value: pyo3::Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        if value.is_none() {
            return Err(PyTypeError::new_err("origin cannot be None"));
        }
        if let Ok(label) = value.cast::<pyo3::types::PyString>() {
            let label = label.to_cow()?;
            return match label.as_ref() {
                "centroid" => Ok(Self::Centroid),
                "center" => Ok(Self::Center),
                other => Err(GeometryError::new_err(format!(
                    "origin must be 'centroid', 'center', or an (x, y) pair, got {other:?}"
                ))),
            };
        }
        let (x, y) = parse_xy_pair(&value)?;
        Ok(Self::Fixed(x, y))
    }
}

/// Parse a 2-element `(x, y)` sequence of finite numbers.
pub(crate) fn parse_xy_pair(value: &Bound<'_, PyAny>) -> PyResult<(f64, f64)> {
    let py = value.py();
    // Propagate provider exceptions from `__iter__` / `__next__`; only a
    // bare "not iterable" TypeError becomes the domain message.
    let mut items = match value.try_iter() {
        Ok(it) => it,
        Err(err) if err.is_instance_of::<pyo3::exceptions::PyTypeError>(py) => {
            return Err(GeometryError::new_err(
                "origin must be an (x, y) pair of finite numbers",
            ));
        },
        Err(err) => return Err(err),
    };
    let x = items
        .next()
        .ok_or_else(|| GeometryError::new_err("origin (x, y) pair requires two values"))??;
    let y = items
        .next()
        .ok_or_else(|| GeometryError::new_err("origin (x, y) pair requires two values"))??;
    if items.next().is_some() {
        return Err(GeometryError::new_err(
            "origin (x, y) pair must have exactly two values",
        ));
    }
    Ok((
        finite_f64_required("origin x", &x)?,
        finite_f64_required("origin y", &y)?,
    ))
}

fn finite_f64_with(
    name: &str,
    value: &Bound<'_, PyAny>,
    make_err: impl FnOnce(String) -> PyErr,
) -> PyResult<f64> {
    let value = f64_required(name, value)?;
    if value.is_finite() {
        Ok(value)
    } else {
        Err(make_err(format!("{name} must be finite")))
    }
}

pub(crate) fn finite_f64_required(name: &str, value: &Bound<'_, PyAny>) -> PyResult<f64> {
    finite_f64_with(name, value, GeometryError::new_err)
}

/// [`finite_f64_required`] for coordinate ordinates: a non-finite coordinate
/// is a structural geometry rule, so it raises `InvalidGeometryError` (matching the
/// column lanes), not the generic parameter class.
pub(crate) fn finite_coordinate_required(name: &str, value: &Bound<'_, PyAny>) -> PyResult<f64> {
    finite_f64_with(
        name,
        value,
        crate::py::errors::InvalidGeometryError::new_err,
    )
}

pub(crate) fn f64_required(name: &str, value: &Bound<'_, PyAny>) -> PyResult<f64> {
    value
        .extract::<f64>()
        .map_err(|_| PyTypeError::new_err(format!("{name} must be a finite float")))
}

pub(crate) fn finite_f64_option(
    name: &str,
    value: Option<&Bound<'_, PyAny>>,
) -> PyResult<Option<f64>> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.is_none() {
        return Ok(None);
    }
    finite_f64_required(name, value).map(Some)
}

/// Parse CDT mesh-refinement controls. Supplying either quality constraint
/// activates refinement; omitting both requests the plain constrained mesh.
pub(crate) fn parse_cdt_refinement(
    min_angle: Option<&Bound<'_, PyAny>>,
    max_area: Option<&Bound<'_, PyAny>>,
) -> PyResult<crate::geometry::CdtRefinement> {
    cdt_refinement_values(
        finite_f64_option("min_angle", min_angle)?,
        finite_f64_option("max_area", max_area)?,
    )
}

/// Validate already-parsed constrained-mesh quality values. Array callers use
/// this per row after `F64Param` has performed scalar/column broadcasting.
pub(crate) fn cdt_refinement_values(
    min_angle: Option<f64>,
    max_area: Option<f64>,
) -> PyResult<crate::geometry::CdtRefinement> {
    use crate::geometry::CdtRefinement;
    let min_angle = match min_angle {
        Some(angle) => {
            let angle = crate::Positive::try_new("min_angle", angle)?;
            if angle.get() > 30.0 {
                return Err(GeometryError::new_err(format!(
                    "min_angle must be at most 30 degrees, got {}",
                    angle.get()
                )));
            }
            Some(angle)
        },
        None => None,
    };
    let max_area = match max_area {
        Some(area) => Some(crate::Positive::try_new("max_area", area)?),
        None => None,
    };
    if min_angle.is_some() || max_area.is_some() {
        Ok(CdtRefinement::On {
            min_angle,
            max_area,
        })
    } else {
        Ok(CdtRefinement::Off)
    }
}

pub(crate) fn coordinate_epoch_option(
    name: &str,
    value: Option<&Bound<'_, PyAny>>,
) -> PyResult<Option<f64>> {
    let Some(value) = f64_option(name, value)? else {
        return Ok(None);
    };
    if value.is_finite() {
        // Canonicalize so `-0.0` and `+0.0` are one epoch: plain `==` and
        // bitwise hash keys then agree everywhere (no `to_bits` for equality).
        Ok(Some(if value == 0.0 { 0.0 } else { value }))
    } else if name == "epoch" {
        Err(GeometryError::new_err(
            "coordinate epoch must be a finite decimal year",
        ))
    } else {
        Err(GeometryError::new_err(format!(
            "{name} must be a finite decimal year"
        )))
    }
}

pub(crate) fn accuracy_option(
    value: Option<&Bound<'_, PyAny>>,
) -> PyResult<Option<crate::NonNegative>> {
    let Some(value) = f64_option("accuracy", value)? else {
        return Ok(None);
    };
    Ok(Some(crate::NonNegative::try_new("accuracy", value)?))
}

pub(crate) fn f64_option(name: &str, value: Option<&Bound<'_, PyAny>>) -> PyResult<Option<f64>> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.is_none() {
        return Ok(None);
    }
    f64_required(name, value).map(Some)
}

// --- Area-of-interest argument parsing (used by transform options) ---

pub(crate) fn parse_area(
    value: Option<&Bound<'_, PyAny>>,
    name: &str,
) -> PyResult<Option<crs::AreaOfInterest>> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.is_none() {
        return Ok(None);
    }
    if let Some(dict) = crate::mapping_as_dict(value)? {
        let dict = &dict;
        return Ok(Some(crs::AreaOfInterest::new(
            area_dict_f64(dict, name, "west", "west_lon_degree")?,
            area_dict_f64(dict, name, "south", "south_lat_degree")?,
            area_dict_f64(dict, name, "east", "east_lon_degree")?,
            area_dict_f64(dict, name, "north", "north_lat_degree")?,
        )?));
    }
    if let Some(area) = area_from_object(value, name)? {
        return Ok(Some(area));
    }
    let values = coordinate_values(value.py(), value, name)?;
    let [west, south, east, north] = values.as_slice() else {
        return Err(GeometryError::new_err(format!(
            "{name} must be a (west, south, east, north) tuple, area dictionary, \
             or AreaOfInterest-like object"
        )));
    };
    Ok(Some(crs::AreaOfInterest::new(
        *west, *south, *east, *north,
    )?))
}

pub(crate) fn area_dict_f64(
    dict: &Bound<'_, PyDict>,
    name: &str,
    key: &str,
    alias: &str,
) -> PyResult<f64> {
    if let Some(value) = dict.get_item(key)? {
        return finite_f64_required(&format!("{name} {key}"), &value);
    }
    if let Some(value) = dict.get_item(alias)? {
        return finite_f64_required(&format!("{name} {alias}"), &value);
    }
    Err(GeometryError::new_err(format!(
        "{name} dictionary requires {key:?} or {alias:?}"
    )))
}

pub(crate) fn area_from_object(
    value: &Bound<'_, PyAny>,
    name: &str,
) -> PyResult<Option<crs::AreaOfInterest>> {
    if value.hasattr("west_lon_degree")?
        || value.hasattr("south_lat_degree")?
        || value.hasattr("east_lon_degree")?
        || value.hasattr("north_lat_degree")?
    {
        return Ok(Some(crs::AreaOfInterest::new(
            area_attr_f64(value, name, "west_lon_degree")?,
            area_attr_f64(value, name, "south_lat_degree")?,
            area_attr_f64(value, name, "east_lon_degree")?,
            area_attr_f64(value, name, "north_lat_degree")?,
        )?));
    }
    if value.hasattr("west")?
        || value.hasattr("south")?
        || value.hasattr("east")?
        || value.hasattr("north")?
    {
        return Ok(Some(crs::AreaOfInterest::new(
            area_attr_f64(value, name, "west")?,
            area_attr_f64(value, name, "south")?,
            area_attr_f64(value, name, "east")?,
            area_attr_f64(value, name, "north")?,
        )?));
    }
    Ok(None)
}

pub(crate) fn area_attr_f64(value: &Bound<'_, PyAny>, name: &str, attr: &str) -> PyResult<f64> {
    let value = value.getattr(attr).map_err(|_| {
        GeometryError::new_err(format!("{name} object requires attribute \"{attr}\""))
    })?;
    finite_f64_required(&format!("{name} {attr}"), &value)
}
