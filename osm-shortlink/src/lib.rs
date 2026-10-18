#![feature(likely_unlikely)]
#![feature(portable_simd)]
#![feature(const_index)]
#![feature(const_trait_impl)]

mod codec;
mod constants;
mod errors;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyString;

impl From<errors::DecodeError> for PyErr {
    fn from(error: errors::DecodeError) -> Self {
        PyValueError::new_err(error.to_string())
    }
}

impl From<errors::EncodeError> for PyErr {
    fn from(error: errors::EncodeError) -> Self {
        PyValueError::new_err(error.to_string())
    }
}

#[pyfunction]
fn shortlink_encode(py: Python<'_>, lon: f64, lat: f64, zoom: u8) -> PyResult<Py<PyString>> {
    let encoded = codec::encode(lon, lat, zoom)?;
    Ok(PyString::new(py, &encoded).into())
}

#[pyfunction]
fn shortlink_decode(py: Python<'_>, s: &str) -> PyResult<(f64, f64, u8)> {
    py.detach(|| codec::decode(s)).map_err(PyErr::from)
}

#[pymodule]
#[pyo3(name = "_lib")]
fn lib(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(shortlink_encode, m)?)?;
    m.add_function(wrap_pyfunction!(shortlink_decode, m)?)?;
    Ok(())
}
