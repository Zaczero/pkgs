#![feature(ascii_char)]
#![feature(next_index)]

mod constants;
mod decode;
mod encode;
mod errors;
mod zigzag;

use std::ascii::Char;

use constants::precision_scale;
use decode::decode;
use encode::Encoder;
use errors::PolylineError;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyList, PyTuple};

impl From<errors::PolylineError> for PyErr {
    fn from(error: errors::PolylineError) -> Self {
        PyValueError::new_err(error.to_string())
    }
}

fn extract_coord_pair(coord: &Bound<'_, PyAny>, index: usize) -> PyResult<(f64, f64)> {
    if let Ok(tuple) = coord.cast_exact::<PyTuple>() {
        let mut items = tuple.iter_borrowed();
        let (Some(first), Some(second)) = (items.next(), items.next()) else {
            return Err(PolylineError::CoordinateMustContain2Values { index }.into());
        };
        return Ok((first.extract()?, second.extract()?));
    }
    let mut it = coord.try_iter()?;
    let first = it
        .next()
        .ok_or(PolylineError::CoordinateMustContain2Values { index })??;
    let second = it
        .next()
        .ok_or(PolylineError::CoordinateMustContain2Values { index })??;
    Ok((first.extract()?, second.extract()?))
}

fn encode<const LATLON: bool>(
    coordinates: &Bound<'_, PyAny>,
    precision: i32,
) -> PyResult<Vec<Char>> {
    let scale =
        precision_scale(precision).ok_or(PolylineError::PrecisionOutOfRange { precision })?;
    let capacity = coordinates
        .len()
        .ok()
        .and_then(|n| n.checked_mul(12))
        .unwrap_or(0);
    let mut encoder = Encoder::new(&scale, capacity);
    for (index, coord) in coordinates.try_iter()?.enumerate() {
        encoder.push::<LATLON>(extract_coord_pair(&coord?, index)?, index)?;
    }
    Ok(encoder.finish())
}

#[pyfunction]
#[pyo3(signature = (coordinates, precision = 5))]
fn encode_lonlat(coordinates: &Bound<'_, PyAny>, precision: i32) -> PyResult<Vec<Char>> {
    encode::<false>(coordinates, precision)
}

#[pyfunction]
#[pyo3(signature = (coordinates, precision = 5))]
fn encode_latlon(coordinates: &Bound<'_, PyAny>, precision: i32) -> PyResult<Vec<Char>> {
    encode::<true>(coordinates, precision)
}

#[pyfunction]
#[pyo3(signature = (polyline, precision = 5))]
fn decode_lonlat<'py>(
    py: Python<'py>,
    polyline: &str,
    precision: i32,
) -> PyResult<Bound<'py, PyList>> {
    let coordinates = py.detach(|| decode::<false>(polyline, precision))?;
    PyList::new(py, coordinates)
}

#[pyfunction]
#[pyo3(signature = (polyline, precision = 5))]
fn decode_latlon<'py>(
    py: Python<'py>,
    polyline: &str,
    precision: i32,
) -> PyResult<Bound<'py, PyList>> {
    let coordinates = py.detach(|| decode::<true>(polyline, precision))?;
    PyList::new(py, coordinates)
}

#[pymodule]
#[pyo3(name = "_lib")]
fn lib(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(encode_lonlat, m)?)?;
    m.add_function(wrap_pyfunction!(encode_latlon, m)?)?;
    m.add_function(wrap_pyfunction!(decode_lonlat, m)?)?;
    m.add_function(wrap_pyfunction!(decode_latlon, m)?)?;
    Ok(())
}
