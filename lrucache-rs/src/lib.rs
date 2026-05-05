mod cache;
mod store;

use crate::cache::LRUCache;
use pyo3::prelude::*;

#[pymodule]
#[pyo3(name = "_lib")]
fn lib(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<LRUCache>()?;
    Ok(())
}
