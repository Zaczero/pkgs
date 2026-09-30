//! Shape -> Python materializers for coordinate objects and JSON values.
//!
//! Convert a `Shape` into the nested Python coordinate tuples returned by the
//! `.coords`-style accessors and generic JSON mappings.
//! Reached from the crate root and the geometry `#[pymethods]` via `use
//! super::*`.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};
use serde_json::Value;

use crate::py::errors::{GeometryError, parse_error};
use crate::{CoordinateAxes, Coordinates, Point, Polygon, PyList, Shape, geometry, py_bool};

pub(crate) fn coordinates_object(
    py: Python<'_>,
    shape: &Shape,
    layout: Option<CoordinateAxes>,
) -> PyResult<Py<PyAny>> {
    let value = match shape {
        Shape::Point(point) => point_coordinates_object(py, *point, layout)?,
        Shape::MultiPoint(points) => points_to_py(py, points, layout)?.unbind().into(),
        Shape::LineString(points) => points_to_py(py, points, layout)?.unbind().into(),
        Shape::MultiLineString(lines) => {
            let values: Vec<Py<PyAny>> = lines
                .iter()
                .map(|line| Ok(points_to_py(py, line, layout)?.unbind().into()))
                .collect::<pyo3::PyResult<Vec<_>>>()?;
            PyList::new(py, values)?.unbind().into()
        },
        Shape::Polygon(polygon) => polygon_coordinates_object(py, polygon, layout)?
            .unbind()
            .into(),
        Shape::MultiPolygon(polygons) => {
            let values: Vec<Py<PyAny>> = polygons
                .iter()
                .map(|polygon| {
                    Ok(polygon_coordinates_object(py, polygon, layout)?
                        .unbind()
                        .into())
                })
                .collect::<pyo3::PyResult<Vec<_>>>()?;
            PyList::new(py, values)?.unbind().into()
        },
        Shape::GeometryCollection(geometries) => {
            let values = geometries
                .iter()
                .map(|geometry| coordinates_object(py, geometry, layout))
                .collect::<pyo3::PyResult<Vec<_>>>()?;
            PyList::new(py, values)?.unbind().into()
        },
        geometry::Shape::Empty(..) => PyList::empty(py).unbind().into(),
    };
    Ok(value)
}

pub(crate) fn polygon_coordinates_object<'py>(
    py: Python<'py>,
    polygon: &Polygon,
    layout: Option<CoordinateAxes>,
) -> PyResult<Bound<'py, PyList>> {
    let values: Vec<Py<PyAny>> = std::iter::once(&polygon.shell)
        .chain(polygon.holes.iter())
        .map(|ring| Ok(points_to_py(py, ring, layout)?.unbind().into()))
        .collect::<pyo3::PyResult<Vec<_>>>()?;
    PyList::new(py, values)
}

pub(crate) fn points_to_py<'py, C: Coordinates + ?Sized>(
    py: Python<'py>,
    points: &C,
    layout: Option<CoordinateAxes>,
) -> PyResult<Bound<'py, PyList>> {
    let values = points
        .iter_coords()
        .map(|point| point_tuple_with_layout(py, point, layout))
        .collect::<pyo3::PyResult<Vec<_>>>()?;
    PyList::new(py, values)
}

pub(crate) fn point_coordinates_object(
    py: Python<'_>,
    point: Point,
    layout: Option<CoordinateAxes>,
) -> PyResult<Py<PyAny>> {
    if let Some(axes) = layout {
        return Ok(PyList::new(py, point_values(point, axes))?.unbind().into());
    }
    Ok(match (point.z(), point.m()) {
        (None, None) => vec![point.x, point.y].into_pyobject(py)?.unbind(),
        (Some(z), None) => vec![point.x, point.y, z].into_pyobject(py)?.unbind(),
        (None, Some(m)) => vec![point.x, point.y, m].into_pyobject(py)?.unbind(),
        (Some(z), Some(m)) => vec![point.x, point.y, z, m].into_pyobject(py)?.unbind(),
    })
}

pub(crate) fn point_tuple(py: Python<'_>, point: Point) -> PyResult<Py<PyAny>> {
    Ok(match (point.z(), point.m()) {
        (None, None) => (point.x, point.y).into_pyobject(py)?.unbind().into(),
        (Some(z), None) => (point.x, point.y, z).into_pyobject(py)?.unbind().into(),
        (None, Some(m)) => (point.x, point.y, m).into_pyobject(py)?.unbind().into(),
        (Some(z), Some(m)) => (point.x, point.y, z, m).into_pyobject(py)?.unbind().into(),
    })
}

fn point_tuple_with_layout(
    py: Python<'_>,
    point: Point,
    layout: Option<CoordinateAxes>,
) -> PyResult<Py<PyAny>> {
    let Some(axes) = layout else {
        return point_tuple(py, point);
    };
    Ok(PyTuple::new(py, point_values(point, axes))?
        .unbind()
        .into_any())
}

fn point_values(point: Point, axes: CoordinateAxes) -> Vec<Option<f64>> {
    let mut values = Vec::with_capacity(axes.dimension());
    values.extend([Some(point.x), Some(point.y)]);
    if axes.has_z() {
        values.push(point.z());
    }
    if axes.has_m() {
        values.push(point.m());
    }
    values
}

pub(crate) fn json_to_py(py: Python<'_>, value: &Value) -> PyResult<Py<PyAny>> {
    Ok(match value {
        Value::Null => py.None(),
        Value::Bool(value) => py_bool(py, *value),
        Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                value.into_pyobject(py)?.unbind().into()
            } else if let Some(value) = value.as_u64() {
                value.into_pyobject(py)?.unbind().into()
            } else {
                value
                    .as_f64()
                    .ok_or_else(|| {
                        parse_error(
                            format!("GeoJSON coordinate {value} is not a finite JSON number"),
                            crate::error::ParseFormat::GeoJson,
                        )
                    })?
                    .into_pyobject(py)?
                    .unbind()
                    .into()
            }
        },
        Value::String(value) => value.into_pyobject(py)?.unbind().into(),
        Value::Array(values) => {
            let values = values
                .iter()
                .map(|value| json_to_py(py, value))
                .collect::<pyo3::PyResult<Vec<_>>>()?;
            PyList::new(py, values)?.unbind().into()
        },
        Value::Object(values) => {
            let dict = PyDict::new(py);
            for (key, value) in values {
                dict.set_item(key, json_to_py(py, value)?)?;
            }
            dict.unbind().into()
        },
    })
}

pub(crate) fn json_text_to_py(py: Python<'_>, text: &str) -> PyResult<Py<PyAny>> {
    let value = serde_json::from_str::<Value>(text).map_err(|error| {
        GeometryError::new_err(format!("native GeoJSON materialization failed: {error}"))
    })?;
    json_to_py(py, &value)
}
