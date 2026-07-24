//! Optional lonboard HTML preview for ``GeometryArray`` (thin Python glue).

use pyo3::prelude::*;

use crate::array::PyGeometryArray;

pub(crate) fn try_array_repr_html(
    py: Python<'_>,
    array: &PyGeometryArray,
) -> PyResult<Option<String>> {
    let Ok(viz) = py.import("gometry._viz") else {
        return Ok(None);
    };
    let result = viz.call_method1("array_repr_html", (array.clone(),))?;
    if result.is_none() {
        return Ok(None);
    }
    result.extract()
}
