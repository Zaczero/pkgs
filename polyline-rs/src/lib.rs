mod constants;
mod decode;
mod encode;
mod errors;
mod zigzag;

use decode::decode;
use encode::encode;
use pyo3::prelude::*;
use pyo3::types::PyList;

#[pyfunction]
#[pyo3(signature = (coordinates, precision = 5))]
fn encode_lonlat(coordinates: &Bound<'_, PyAny>, precision: i32) -> PyResult<String> {
    encode::<false>(coordinates, precision)
}

#[pyfunction]
#[pyo3(signature = (coordinates, precision = 5))]
fn encode_latlon(coordinates: &Bound<'_, PyAny>, precision: i32) -> PyResult<String> {
    encode::<true>(coordinates, precision)
}

#[pyfunction]
#[pyo3(signature = (polyline, precision = 5))]
fn decode_lonlat<'py>(
    py: Python<'py>,
    polyline: &str,
    precision: i32,
) -> PyResult<Bound<'py, PyList>> {
    decode::<false>(py, polyline.trim(), precision)
}

#[pyfunction]
#[pyo3(signature = (polyline, precision = 5))]
fn decode_latlon<'py>(
    py: Python<'py>,
    polyline: &str,
    precision: i32,
) -> PyResult<Bound<'py, PyList>> {
    decode::<true>(py, polyline.trim(), precision)
}

#[pymodule(gil_used = false)]
#[pyo3(name = "_lib")]
fn lib(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(encode_lonlat, m)?)?;
    m.add_function(wrap_pyfunction!(encode_latlon, m)?)?;
    m.add_function(wrap_pyfunction!(decode_lonlat, m)?)?;
    m.add_function(wrap_pyfunction!(decode_latlon, m)?)?;
    Ok(())
}
