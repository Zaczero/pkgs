#![feature(likely_unlikely)]
#![feature(portable_simd)]

mod codec;
mod constants;
mod errors;

use pyo3::prelude::*;
use pyo3::types::PyString;

#[pyfunction]
fn shortlink_encode(py: Python<'_>, lon: f64, lat: f64, zoom: u8) -> PyResult<Py<PyString>> {
    let encoded = codec::encode(lon, lat, zoom).map_err(errors::EncodeError::into_pyerr)?;
    Ok(PyString::new(py, &encoded).into())
}

#[pyfunction]
fn shortlink_decode(s: &str) -> PyResult<(f64, f64, u8)> {
    codec::decode(s).map_err(errors::DecodeError::into_pyerr)
}

#[pymodule]
#[pyo3(name = "_lib")]
fn lib(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(shortlink_encode, m)?)?;
    m.add_function(wrap_pyfunction!(shortlink_decode, m)?)?;
    Ok(())
}
