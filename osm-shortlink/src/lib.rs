#![feature(likely_unlikely)]
#![feature(portable_simd)]

mod codec;
mod constants;

use pyo3::prelude::*;
use pyo3::types::PyString;

#[pyfunction]
fn shortlink_encode(py: Python<'_>, lon: f64, lat: f64, zoom: u8) -> Py<PyString> {
    PyString::new(py, &codec::encode(lon, lat, zoom)).into()
}

#[pyfunction]
fn shortlink_decode(s: &str) -> (f64, f64, u8) {
    codec::decode(s)
}

#[pymodule(gil_used = false)]
#[pyo3(name = "_lib")]
fn lib(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(shortlink_encode, m)?)?;
    m.add_function(wrap_pyfunction!(shortlink_decode, m)?)?;
    Ok(())
}
