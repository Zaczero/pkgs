#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyString};

use crate::*;

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
            return Ok(Self::Scalar(text.to_cow()?.into_owned()));
        }
        crate::iterable_lane::<String>(value, name, "str").map(Self::Many)
    }
}
