#![feature(const_index)]
#![feature(const_trait_impl)]
#![feature(likely_unlikely)]
#![feature(portable_simd)]

mod base83;
pub(crate) mod color;
mod cos;
mod decode;
mod encode;
mod errors;
mod srgb;

use std::num::NonZeroUsize;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::errors::BlurhashError;

impl From<BlurhashError> for PyErr {
    fn from(error: BlurhashError) -> Self {
        PyValueError::new_err(error.to_string())
    }
}

struct RgbDimensions {
    width: NonZeroUsize,
    height: NonZeroUsize,
    pixels: usize,
    rgb_len: usize,
}

impl RgbDimensions {
    fn new(width: usize, height: usize) -> Result<Self, BlurhashError> {
        let Some(width) = NonZeroUsize::new(width) else {
            return Err(BlurhashError::InvalidImageDimensions { width, height });
        };
        let Some(height) = NonZeroUsize::new(height) else {
            return Err(BlurhashError::InvalidImageDimensions {
                width: width.get(),
                height: 0,
            });
        };

        let pixels = width.get().checked_mul(height.get()).ok_or_else(|| {
            BlurhashError::ImageDimensionsTooLarge {
                width: width.get(),
                height: height.get(),
            }
        })?;
        let rgb_len =
            pixels
                .checked_mul(3)
                .ok_or_else(|| BlurhashError::ImageDimensionsTooLarge {
                    width: width.get(),
                    height: height.get(),
                })?;
        if rgb_len > isize::MAX as usize {
            return Err(BlurhashError::ImageDimensionsTooLarge {
                width: width.get(),
                height: height.get(),
            });
        }

        Ok(Self {
            width,
            height,
            pixels,
            rgb_len,
        })
    }

    const fn width(&self) -> usize {
        self.width.get()
    }

    const fn height(&self) -> usize {
        self.height.get()
    }

    const fn pixels(&self) -> usize {
        self.pixels
    }

    const fn rgb_len(&self) -> usize {
        self.rgb_len
    }

    fn validate_working_storage(
        &self,
        x_components: usize,
        y_components: usize,
    ) -> Result<(), BlurhashError> {
        if cos::dimensions_fit(self.width(), self.height(), x_components, y_components) {
            return Ok(());
        }

        Err(BlurhashError::ImageDimensionsTooLarge {
            width: self.width(),
            height: self.height(),
        })
    }
}

#[pyfunction]
fn encode_rgb(
    py: Python<'_>,
    rgb: &[u8],
    width: usize,
    height: usize,
    x_components: u8,
    y_components: u8,
) -> PyResult<String> {
    let dimensions = RgbDimensions::new(width, height)?;
    py.detach(|| encode::encode_rgb(rgb, &dimensions, x_components, y_components))
        .map_err(PyErr::from)
}

#[pyfunction]
fn decode_rgb(
    py: Python<'_>,
    blurhash: &str,
    width: usize,
    height: usize,
    punch: f64,
) -> PyResult<Py<PyBytes>> {
    let dimensions = RgbDimensions::new(width, height)?;
    let blurhash = blurhash.trim();
    let layout = decode::decode_layout(blurhash.as_bytes(), punch)?;
    dimensions.validate_working_storage(layout.num_x, layout.num_y)?;
    let out = PyBytes::new_with(py, dimensions.rgb_len(), |buf| {
        py.detach(|| decode::decode_rgb_into(blurhash, &dimensions, &layout, buf))
            .map_err(PyErr::from)
    })?;
    Ok(out.unbind())
}

#[pymodule]
#[pyo3(name = "_lib")]
fn lib(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(encode_rgb, m)?)?;
    m.add_function(wrap_pyfunction!(decode_rgb, m)?)?;
    Ok(())
}
