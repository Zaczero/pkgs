use pyo3::exceptions::PyTypeError;
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
    Many(Vec<Option<String>>),
}

impl CodeInput {
    fn parse(value: &Bound<'_, PyAny>, name: &str) -> PyResult<Self> {
        if let Ok(text) = value.cast::<PyString>() {
            let text = text.to_str()?;
            return Ok(Self::Scalar(text.to_owned()));
        }
        let iter = crate::py_iter_or_error(value, || {
            PyTypeError::new_err(format!("{name} must be an iterable of strings"))
        })?;
        let mut out = Vec::new();
        crate::reserve_from_py_len(value, &mut out)?;
        for (index, item) in iter.enumerate() {
            let item = item?;
            crate::check_python_signals(value.py(), index)?;
            if item.is_none() {
                out.push(None);
                continue;
            }
            let text = item.cast::<PyString>().map_err(|_| {
                PyTypeError::new_err(format!("{name} elements have the wrong type"))
            })?;
            out.push(Some(text.to_str()?.to_owned()));
        }
        Ok(Self::Many(out))
    }
}
