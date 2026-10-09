use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::PyIterator;

use crate::boundary::metadata;
use crate::crs::frame::FrameAdoption;
use crate::geometry::Shape;
use crate::geometry::storage::ValidityMask;
use crate::{PyGeometryArray, finite_coordinate_required};

pub(super) enum PointCellInput {
    GeometryArray(PyGeometryArray),
    Coordinates(Vec<f64>, Vec<f64>),
    MissingRows(usize),
}

pub(super) fn geometry_iterable_array(
    values: &Bound<'_, PyAny>,
    point_only: bool,
    context: &str,
) -> PyResult<Option<PyGeometryArray>> {
    let Some(items) = collect_iterable(values)? else {
        return Ok(None);
    };
    geometry_array_from_items(items, point_only, context)
}

pub(super) fn point_cell_input(values: &Bound<'_, PyAny>) -> PyResult<Option<PointCellInput>> {
    let Some(items) = collect_iterable(values)? else {
        return Ok(None);
    };
    if items.is_empty() || items.iter().all(|item| item.is_none()) {
        return Ok(Some(PointCellInput::MissingRows(items.len())));
    }
    if items
        .iter()
        .any(|item| crate::exact_geometry(item).is_some())
    {
        return Ok(Some(PointCellInput::GeometryArray(
            geometry_array_from_items(items, true, "GeometryArray")?
                .expect("geometry rows form a geometry array"),
        )));
    }
    let Some((longitudes, latitudes)) = coordinate_matrix(&items)? else {
        return Ok(None);
    };
    Ok(Some(PointCellInput::Coordinates(longitudes, latitudes)))
}

fn collect_iterable<'py>(values: &Bound<'py, PyAny>) -> PyResult<Option<Vec<Bound<'py, PyAny>>>> {
    if values.is_none() {
        return Ok(None);
    }
    let Some(iter) = iterable_or_none(values)? else {
        return Ok(None);
    };
    crate::collect_py_iter_from_iter(values, iter, Ok).map(Some)
}

fn iterable_or_none<'py>(value: &Bound<'py, PyAny>) -> PyResult<Option<Bound<'py, PyIterator>>> {
    match value.try_iter() {
        Ok(iter) => Ok(Some(iter)),
        Err(error)
            if error.is_instance_of::<PyTypeError>(value.py())
                && value
                    .getattr_opt(pyo3::intern!(value.py(), "__iter__"))?
                    .is_none()
                && value
                    .getattr_opt(pyo3::intern!(value.py(), "__getitem__"))?
                    .is_none() =>
        {
            Ok(None)
        },
        Err(error) => Err(error),
    }
}

fn geometry_array_from_items(
    items: Vec<Bound<'_, PyAny>>,
    point_only: bool,
    context: &str,
) -> PyResult<Option<PyGeometryArray>> {
    let recognized = items
        .iter()
        .all(|item| item.is_none() || crate::exact_geometry(item).is_some());
    if !recognized {
        if items
            .iter()
            .any(|item| crate::exact_geometry(item).is_some())
        {
            return Err(PyTypeError::new_err(
                "geometry collections may contain only Geometry values and None",
            ));
        }
        return Ok(None);
    }

    let mut missing = Vec::with_capacity(items.len());
    let mut geometries = Vec::with_capacity(items.len());
    for (row, item) in items.into_iter().enumerate() {
        if item.is_none() {
            missing.push(true);
            continue;
        }
        let geometry = crate::exact_geometry(&item)
            .expect("recognized geometry collection contains a Geometry");
        if point_only && !matches!(geometry.shape.shape(), Shape::Point(_)) {
            return Err(PyTypeError::new_err(format!(
                "point cell input row {row} must be a Point geometry"
            )));
        }
        missing.push(false);
        geometries.push(geometry.clone());
    }

    let frame =
        metadata::resolve_geometry_frames(&mut geometries, FrameAdoption::default(), context)?;
    let present = PyGeometryArray::from_shapes(
        geometries
            .iter()
            .map(|geometry| geometry.shape.shape().clone())
            .collect(),
        frame,
    )?;
    let result = missing
        .iter()
        .any(|is_missing| *is_missing)
        .then(|| {
            PyGeometryArray::scatter_present_rows(
                &present,
                ValidityMask::from_vec(missing.len(), missing)
                    .expect("at least one missing row produces a validity mask"),
            )
        })
        .transpose()?
        .unwrap_or(present);
    Ok(Some(result))
}

fn coordinate_matrix(rows: &[Bound<'_, PyAny>]) -> PyResult<Option<(Vec<f64>, Vec<f64>)>> {
    let Some(first) = rows.first() else {
        return Ok(Some((Vec::new(), Vec::new())));
    };
    let Some(mut first_iter) = iterable_or_none(first)? else {
        return Ok(None);
    };
    let Some(longitude) = first_iter.next() else {
        return Err(PyTypeError::new_err(
            "coordinate matrix rows must contain at least x and y",
        ));
    };
    let longitude = finite_coordinate_required("longitude", &longitude?)?;
    let Some(latitude) = first_iter.next() else {
        return Err(PyTypeError::new_err(
            "coordinate matrix rows must contain at least x and y",
        ));
    };
    let latitude = finite_coordinate_required("latitude", &latitude?)?;

    let mut longitudes = Vec::with_capacity(rows.len());
    let mut latitudes = Vec::with_capacity(rows.len());
    longitudes.push(longitude);
    latitudes.push(latitude);
    for (row, item) in rows.iter().enumerate().skip(1) {
        let Some(mut ordinates) = iterable_or_none(item)? else {
            return Err(PyTypeError::new_err(format!(
                "coordinate matrix row {row} must contain at least x and y"
            )));
        };
        let Some(longitude) = ordinates.next() else {
            return Err(PyTypeError::new_err(format!(
                "coordinate matrix row {row} must contain at least x and y"
            )));
        };
        let longitude = finite_coordinate_required("longitude", &longitude?)?;
        let Some(latitude) = ordinates.next() else {
            return Err(PyTypeError::new_err(format!(
                "coordinate matrix row {row} must contain at least x and y"
            )));
        };
        let latitude = finite_coordinate_required("latitude", &latitude?)?;
        longitudes.push(longitude);
        latitudes.push(latitude);
    }
    Ok(Some((longitudes, latitudes)))
}
