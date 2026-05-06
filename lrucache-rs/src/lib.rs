mod cache;
mod errors;
mod store;

use pyo3::prelude::*;

use crate::cache::LRUCache;

#[pymodule]
#[pyo3(name = "_lib")]
fn lib(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<LRUCache>()?;
    Ok(())
}
