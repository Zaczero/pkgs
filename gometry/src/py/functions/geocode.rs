use pyo3::prelude::*;
use pyo3::types::{PyAny, PyString};

use crate::py::errors::native_memory_error;
use crate::{Bound, PyResult};

mod pluscode;
mod pyfuncs;
mod shortlink;

pub(crate) use pyfuncs::{
    osm_shortlink_encode, osm_shortlink_location, pluscode_encode, pluscode_polygon,
    pluscode_recover, pluscode_shorten,
};

enum CodeInput {
    Scalar(String),
    Many(Vec<String>),
}

impl CodeInput {
    fn parse(value: &Bound<'_, PyAny>, name: &str) -> PyResult<Self> {
        if let Ok(text) = value.cast::<PyString>() {
            let text = text.to_str()?;
            let text = crate::try_string_from_str(text).map_err(|()| native_memory_error())?;
            return Ok(Self::Scalar(text));
        }
        crate::collect_py_strings(value, name).map(Self::Many)
    }
}
