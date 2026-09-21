use pyo3::prelude::*;
use pyo3::types::{PyAny, PyString};

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
            return Ok(Self::Scalar(text.to_owned()));
        }
        crate::collect_py_strings(value, name).map(Self::Many)
    }
}
