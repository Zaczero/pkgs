#![feature(likely_unlikely)]
#![feature(portable_simd)]

mod base83;
mod cos;
mod decode;
mod encode;
mod srgb;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyByteArray;

#[pyfunction]
fn encode_rgb(
    rgb: &[u8],
    width: usize,
    height: usize,
    x_components: usize,
    y_components: usize,
) -> PyResult<String> {
    encode::encode_rgb(rgb, width, height, x_components, y_components)
        .map_err(PyValueError::new_err)
}

#[pyfunction]
fn decode_rgb(
    py: Python<'_>,
    blurhash: &str,
    width: usize,
    height: usize,
    punch: f32,
) -> PyResult<Py<PyByteArray>> {
    let out_len = width
        .checked_mul(height)
        .and_then(|v| v.checked_mul(3))
        .ok_or_else(|| PyValueError::new_err("Image is too large"))?;

    let out = PyByteArray::new_with(py, out_len, |buf| {
        decode::decode_rgb_into(blurhash, width, height, punch, buf).map_err(PyValueError::new_err)
    })?;
    Ok(out.unbind())
}

#[pymodule(gil_used = false)]
#[pyo3(name = "_lib")]
fn lib(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(encode_rgb, m)?)?;
    m.add_function(wrap_pyfunction!(decode_rgb, m)?)?;
    Ok(())
}
