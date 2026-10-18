use crate::py::arrow_c;
use crate::py::arrow_c::{ExportedArray, GometryArrowArray, SchemaNode};

pub(crate) fn export_with_schema_node(
    schema_node: SchemaNode,
    array: GometryArrowArray,
) -> arrow_c::NativeArrowExportResult<ExportedArray> {
    let schema = schema_node.materialize_schema()?;
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
    bitmap: crate::geometry::SharedSlice<u8>,
    null_count: usize,
) -> pyo3::PyResult<()> {
    arrow_c::apply_prebuilt_top_level_validity(&mut export.array, bitmap, null_count)
}
