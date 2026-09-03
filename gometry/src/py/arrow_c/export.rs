use std::sync::Arc;

use crate::py::arrow_c::{ExportedArray, GometryArrowArray, SchemaNode};

pub(crate) fn export_with_schema_node(
    schema_node: SchemaNode,
    array: GometryArrowArray,
) -> pyo3::PyResult<ExportedArray> {
    let schema = schema_node.clone().into_schema()?;
    Ok(ExportedArray {
        schema,
        schema_node,
        array,
    })
}

/// Apply an already-planned validity bitmap while the C shell is attached.
/// The bitmap itself was built by the detached planner.
pub(crate) fn apply_exported_validity_plan(
    export: &mut ExportedArray,
    bitmap: Arc<[u8]>,
    null_count: usize,
) -> pyo3::PyResult<()> {
    crate::py::arrow_c::apply_prebuilt_top_level_validity(&mut export.array, bitmap, null_count)
}
