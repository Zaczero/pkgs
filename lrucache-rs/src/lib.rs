#![feature(likely_unlikely)]

mod cache;
mod errors;
mod key;

use crate::cache::LRUCache;
use pyo3::prelude::*;

#[pymodule(gil_used = false)]
#[pyo3(name = "_lib")]
fn lib(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<LRUCache>()?;
    Ok(())
}
