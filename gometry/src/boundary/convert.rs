#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! Shape -> Python materializers for coordinate objects and `GeoJSON` dicts.
//!
//! Convert a `Shape` into the nested Python coordinate tuples returned by the
//! `.coords`-style accessors and the `__geo_interface__` / `GeoJSON` mapping.
//! Reached from the crate root and the geometry `#[pymethods]` via `use
//! super::*`.

use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict};
use serde_json::Value;

use crate::py::errors::parse_error;
use crate::*;

pub(crate) fn coordinates_object(py: Python<'_>, shape: &Shape) -> PyResult<Py<PyAny>> {
    let value = match shape {
        Shape::Point(point) => point_coordinates_object(py, *point)?,
        Shape::MultiPoint(points) => points_to_py(py, points)?.unbind().into(),
        Shape::LineString(points) => points_to_py(py, points)?.unbind().into(),
        Shape::MultiLineString(lines) => {
            let values = lines
                .iter()
                .map(|line| Ok(points_to_py(py, line)?.unbind().into()))
                .collect::<PyResult<Vec<Py<PyAny>>>>()?;
            PyList::new(py, values)?.unbind().into()
        },
        Shape::Polygon(polygon) => polygon_coordinates_object(py, polygon)?.unbind().into(),
        Shape::MultiPolygon(polygons) => {
            let values = polygons
                .iter()
                .map(|polygon| Ok(polygon_coordinates_object(py, polygon)?.unbind().into()))
                .collect::<PyResult<Vec<Py<PyAny>>>>()?;
            PyList::new(py, values)?.unbind().into()
        },
        Shape::GeometryCollection(geometries) => {
            let values = geometries
                .iter()
                .map(|geometry| coordinates_object(py, geometry))
                .collect::<PyResult<Vec<_>>>()?;
            PyList::new(py, values)?.unbind().into()
        },
        geometry::Shape::Empty(..) => PyList::empty(py).unbind().into(),
    };
    Ok(value)
}

pub(crate) fn polygon_coordinates_object<'py>(
    py: Python<'py>,
    polygon: &Polygon,
) -> PyResult<Bound<'py, PyList>> {
    let values = std::iter::once(&polygon.shell)
        .chain(polygon.holes.iter())
        .map(|ring| Ok(points_to_py(py, ring)?.unbind().into()))
        .collect::<PyResult<Vec<Py<PyAny>>>>()?;
    PyList::new(py, values)
}

pub(crate) fn points_to_py<'py, C: Coordinates + ?Sized>(
    py: Python<'py>,
    points: &C,
) -> PyResult<Bound<'py, PyList>> {
    let values = points
        .iter_coords()
        .map(|point| point_tuple(py, point))
        .collect::<PyResult<Vec<_>>>()?;
    PyList::new(py, values)
}

pub(crate) fn point_coordinates_object(py: Python<'_>, point: Point) -> PyResult<Py<PyAny>> {
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

pub(crate) fn geojson_dict<'py>(py: Python<'py>, shape: &Shape) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    match shape {
        Shape::Point(point) => {
            dict.set_item("type", "Point")?;
            dict.set_item("coordinates", geojson_point_object(py, *point, true)?)?;
        },
        Shape::MultiPoint(points) => {
            dict.set_item("type", "MultiPoint")?;
            dict.set_item("coordinates", geojson_points_object(py, points, true)?)?;
        },
        Shape::LineString(points) => {
            dict.set_item("type", "LineString")?;
            dict.set_item("coordinates", geojson_points_object(py, points, true)?)?;
        },
        Shape::MultiLineString(lines) => {
            let values = lines
                .iter()
                .map(|line| Ok(geojson_points_object(py, line, true)?.unbind().into()))
                .collect::<PyResult<Vec<Py<PyAny>>>>()?;
            dict.set_item("type", "MultiLineString")?;
            dict.set_item("coordinates", PyList::new(py, values)?)?;
        },
        Shape::Polygon(polygon) => {
            dict.set_item("type", "Polygon")?;
            dict.set_item("coordinates", geojson_polygon_object(py, polygon, true)?)?;
        },
        Shape::MultiPolygon(polygons) => {
            let values = polygons
                .iter()
                .map(|polygon| Ok(geojson_polygon_object(py, polygon, true)?.unbind().into()))
                .collect::<PyResult<Vec<Py<PyAny>>>>()?;
            dict.set_item("type", "MultiPolygon")?;
            dict.set_item("coordinates", PyList::new(py, values)?)?;
        },
        Shape::GeometryCollection(geometries) => {
            let values = geometries
                .iter()
                .map(|geometry| Ok(geojson_dict(py, geometry)?.unbind().into()))
                .collect::<PyResult<Vec<Py<PyAny>>>>()?;
            dict.set_item("type", "GeometryCollection")?;
            dict.set_item("geometries", PyList::new(py, values)?)?;
        },
        // GeoJSON has no dimensional-empty form; every typed empty flattens
        // to its kind with an empty coordinates/geometries member.
        Shape::Empty(kind, _) => {
            dict.set_item("type", kind.geometry_type())?;
            let member = match kind {
                EmptyKind::GeometryCollection => "geometries",
                _ => "coordinates",
            };
            dict.set_item(member, PyList::empty(py))?;
        },
    }
    Ok(dict)
}

fn geojson_polygon_object<'py>(
    py: Python<'py>,
    polygon: &Polygon,
    include_z: bool,
) -> PyResult<Bound<'py, PyList>> {
    let values = std::iter::once(&polygon.shell)
        .chain(polygon.holes.iter())
        .map(|ring| Ok(geojson_points_object(py, ring, include_z)?.unbind().into()))
        .collect::<PyResult<Vec<Py<PyAny>>>>()?;
    PyList::new(py, values)
}

fn geojson_points_object<'py, C: Coordinates + ?Sized>(
    py: Python<'py>,
    points: &C,
    include_z: bool,
) -> PyResult<Bound<'py, PyList>> {
    let values = points
        .iter_coords()
        .map(|point| geojson_point_object(py, point, include_z))
        .collect::<PyResult<Vec<_>>>()?;
    PyList::new(py, values)
}

fn geojson_point_object(py: Python<'_>, point: Point, include_z: bool) -> PyResult<Py<PyAny>> {
    Ok(if include_z {
        point.z().map_or_else(
            || vec![point.x, point.y].into_pyobject(py).map(Bound::unbind),
            |z| {
                vec![point.x, point.y, z]
                    .into_pyobject(py)
                    .map(Bound::unbind)
            },
        )?
    } else {
        vec![point.x, point.y].into_pyobject(py)?.unbind()
    })
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
                            crate::py::errors::ParseFormat::GeoJson,
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
                .collect::<PyResult<Vec<_>>>()?;
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
