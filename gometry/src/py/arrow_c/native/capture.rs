use super::buffers::snapshot_native_buffers;
use super::decode::NativeDecodedBatch;
use super::layout::ArrowFormat;
use super::node::{build_owned_node_admitted, merge_owned_row_missing, project_ancestor_missing};
use super::*;

/// Snapshot selected array subtree into owned `NativeNode` while shell is live.
///
/// # Safety
///
/// - **Quiescence:** no thread or native callback writes producer buffers or
///   child tables for the duration of this call.
/// - **Capacity:** every buffer span sized by schema/layout is readable under
///   the shell for its full computed length.
/// - **No suspension:** capture performs no nested provider call and does not
///   detach into Python until owned buffers are complete.
/// - **Owned result:** only the returned `NativeNode` (and errors) may escape;
///   no producer-backed slice or layout escapes.
unsafe fn capture_selected_node(
    array_shell: &MovedArrowShell<ArrowArray>,
    schema: &AdmittedArrowSchema,
    select_struct_child: Option<usize>,
) -> PyResult<NativeNode> {
    if let Some(child_index) = select_struct_child {
        // Selected-span: F5 ParseError gates on child index/null before owned
        // admit of the geometry child (parent buffer table not required).
        // SAFETY: same obligations as this function.
        return unsafe { admit_selected_struct_geometry(array_shell, schema, child_index) };
    }
    let format = ArrowFormat::parse(schema.format())?;
    let schema_n_children = schema.children.len();
    // SAFETY: shell pins the array for this capture window; obligations match
    // this function's Safety section.
    let mut layout = unsafe {
        CaptureLayout::new(array_shell, array_shell.as_ptr(), format, schema_n_children)
    }?;
    build_owned_node_admitted(&mut layout, schema, None)
}

impl NativeArrowBatch {
    pub(crate) fn decode(self) -> Result<NativeDecodedBatch, NativeArrowImportError> {
        decode::decode_batch(self)
    }
}

/// Admit a direct PyArrow export after its type and field metadata have been
/// reconciled by the attached caller.  The exported schema is physical
/// storage, not a table wrapper, so a bare `+s` is captured as coordinates.
pub(crate) fn native_arrow_batch_from_capsules_with_classification(
    schema_capsule: &Bound<'_, PyAny>,
    array_capsule: &Bound<'_, PyAny>,
    classification: NativeArrowClassification,
) -> PyResult<NativeArrowBatch> {
    let schema = capsule_pointer::<ArrowSchema>(schema_capsule, schema_capsule_name())?;
    let array = capsule_pointer::<ArrowArray>(array_capsule, array_capsule_name())?;
    // SAFETY: capsules resolved to live named pointers with release still set.
    let schema_shell = unsafe { MovedArrowShell::take(schema) }?;
    // SAFETY: the moved schema shell pins the physical schema for capture only.
    let admitted_schema = unsafe { AdmittedArrowSchema::capture_geometry(&schema_shell) }?;
    drop(schema_shell);
    validate_direct_physical_schema(&admitted_schema, &classification)?;
    // SAFETY: the array capsule is a live one-shot producer shell.
    let array_shell = unsafe { MovedArrowShell::take(array) }?;
    // SAFETY: only owned buffers escape this capture.
    let node = unsafe { capture_selected_node(&array_shell, &admitted_schema, None) }?;
    drop(array_shell);
    Ok(NativeArrowBatch {
        node,
        classification,
    })
}

/// Admit a stream or pure Arrow-C array whose schema was captured and
/// classified independently.  `struct_child` is consumed here and nowhere in
/// the owned batch, keeping table selection an admission-only concern.
pub(crate) unsafe fn native_arrow_batch_from_raw_array(
    array: *mut ArrowArray,
    admitted_schema: &AdmittedArrowSchema,
    classification: NativeArrowClassification,
    struct_child: Option<usize>,
) -> PyResult<NativeArrowBatch> {
    // The stream callback has just initialized this output shell. Install a
    // non-allocating owner before transferring shell ownership.
    // SAFETY: caller guarantees a live, writable producer batch array.
    let _output_guard = unsafe { ArrowReleaseGuard::from_raw(array) }?;
    // SAFETY: caller guarantees a live producer batch array.
    let array_shell = unsafe { MovedArrowShell::take(array) }?;
    // SAFETY: the shell pins every selected buffer and child table until the
    // recursive owned snapshot completes.
    let node = unsafe { capture_selected_node(&array_shell, admitted_schema, struct_child) }?;
    drop(array_shell);
    Ok(NativeArrowBatch {
        node,
        classification,
    })
}

/// Admit and classify a pure Arrow-C array/stream schema, then build one owned
/// batch.  Direct PyArrow callers use the classification-aware sibling above
/// so field metadata is resolved before their protocol call.
pub(crate) fn native_arrow_batch_from_capsules(
    schema_capsule: &Bound<'_, PyAny>,
    array_capsule: &Bound<'_, PyAny>,
) -> PyResult<(NativeArrowBatch, ClassifiedGeometrySchema)> {
    let schema = capsule_pointer::<ArrowSchema>(schema_capsule, schema_capsule_name())?;
    let array = capsule_pointer::<ArrowArray>(array_capsule, array_capsule_name())?;
    // SAFETY: capsule points at a live producer schema shell.
    let schema_shell = unsafe { MovedArrowShell::take(schema) }?;
    // SAFETY: shell pins the schema tree through owned capture.
    let admitted_schema = unsafe { AdmittedArrowSchema::capture(&schema_shell) }?;
    drop(schema_shell);
    let classified = classify_admitted_geometry_schema(&admitted_schema)?;
    let (classification, struct_child) = NativeArrowClassification::from_schema(&classified);
    // SAFETY: capsule points at a live producer array shell.
    let array_shell = unsafe { MovedArrowShell::take(array) }?;
    // SAFETY: only the owned node escapes.
    let node = unsafe { capture_selected_node(&array_shell, &admitted_schema, struct_child) }?;
    drop(array_shell);
    Ok((
        NativeArrowBatch {
            node,
            classification,
        },
        classified,
    ))
}

fn validate_direct_physical_schema(
    schema: &AdmittedArrowSchema,
    classification: &NativeArrowClassification,
) -> PyResult<()> {
    let actual_extension = schema.extension_name()?;
    if let Some(actual_extension) = actual_extension {
        if actual_extension != classification.encoding.extension_name() {
            return Err(PyTypeError::new_err(format!(
                "Arrow C physical schema extension '{actual_extension}' conflicts with admitted '{}'",
                classification.encoding.extension_name()
            )));
        }
        let metadata = schema.extension_metadata_bytes();
        let (crs, epoch) =
            arrow::parse_geoarrow_extension_metadata_for(&metadata, Some(classification.encoding))?;
        if !frame::optional_crs_labels_operationally_equal(
            classification.crs.as_deref().map(String::as_str),
            crs.as_deref(),
        )? || !frame::optional_epochs_equal(classification.epoch, epoch)
        {
            return Err(arrow::geoarrow_parse_error(
                "Arrow C physical schema metadata conflicts with admitted Arrow metadata",
            ));
        }
    }
    admitted::validate_admitted_encoding_storage(schema, classification.encoding)?;
    if matches!(classification.encoding, GeometryEncoding::Wkb) {
        let actual_width = match schema.format() {
            "vz" => WkbOffsetWidth::View,
            "Z" => WkbOffsetWidth::Int64,
            "z" => WkbOffsetWidth::Int32,
            _ => {
                return Err(PyTypeError::new_err(
                    "geoarrow.wkb storage must be binary, large_binary, or binary_view",
                ));
            },
        };
        if actual_width != classification.wkb_offset_width {
            return Err(PyTypeError::new_err(
                "Arrow C physical schema offset width conflicts with admitted Arrow metadata",
            ));
        }
    }
    Ok(())
}

/// Selected-span table root → own only the geometry child.
///
/// # Safety
///
/// Same as [`capture_selected_node`].
unsafe fn admit_selected_struct_geometry(
    array_shell: &MovedArrowShell<ArrowArray>,
    schema: &AdmittedArrowSchema,
    child_index: usize,
) -> PyResult<NativeNode> {
    if schema.format() != "+s" {
        return Err(PyTypeError::new_err(
            "Arrow table geometry selection requires a struct root",
        ));
    }
    // SAFETY: shell pins the root array for the capture window.
    let root = unsafe { &*array_shell.as_ptr() };
    let n_children = usize::try_from(root.n_children).map_err(|_| {
        errors::parse_error(
            "Arrow array n_children is negative or too large",
            crate::error::ParseFormat::GeoArrow,
        )
    })?;
    if child_index >= n_children {
        return Err(errors::parse_error(
            format!("Arrow child index {child_index} is out of range for n_children={n_children}"),
            crate::error::ParseFormat::GeoArrow,
        ));
    }
    let child_schema = schema.children.get(child_index).ok_or_else(|| {
        errors::parse_error(
            format!(
                "Arrow child index {child_index} is out of range for schema n_children={}",
                schema.children.len()
            ),
            crate::error::ParseFormat::GeoArrow,
        )
    })?;
    let child_format = ArrowFormat::parse(child_schema.format())?;
    let child_schema_n = child_schema.children.len();
    if root.children.is_null() {
        return Err(errors::parse_error(
            "Arrow struct array children pointer is null",
            crate::error::ParseFormat::GeoArrow,
        ));
    }
    // SAFETY: children table non-null; index in range under live shell.
    let child_ptr = unsafe { *root.children.add(child_index) };
    if child_ptr.is_null() {
        return Err(errors::parse_error(
            "Arrow child array is null",
            crate::error::ParseFormat::GeoArrow,
        ));
    }
    let parent_offset = usize::try_from(root.offset)
        .map_err(|_| PyTypeError::new_err("Arrow array offset is negative or too large"))?;
    let parent_length = usize::try_from(root.length)
        .map_err(|_| PyTypeError::new_err("Arrow array length is negative or too large"))?;
    // Optional parent validity for ancestor missing. Capture/snapshot errors
    // must propagate (C11): only a successfully admitted, genuinely absent
    // validity bitmap becomes `None`. Swallowing layout failures previously
    // accepted a parent with forged `n_buffers` / null_count / validity.
    // SAFETY: the moved shell owns the live root; `CaptureLayout::new` validates
    // the array's child count against the admitted schema before snapshotting.
    let mut parent_layout = unsafe {
        CaptureLayout::new(
            array_shell,
            array_shell.as_ptr(),
            ArrowFormat::Struct,
            schema.children.len(),
        )
    }?;
    let parent_buffers = snapshot_native_buffers(&mut parent_layout)?;
    let parent_effective = merge_owned_row_missing(&parent_layout, &parent_buffers, None)?;
    let parent_missing = project_ancestor_missing(
        &parent_layout,
        &parent_buffers,
        parent_effective.as_ref(),
        parent_length,
    )?;
    // SAFETY: child lives under the same capture shell as root.
    let mut child_layout = unsafe {
        CaptureLayout::struct_child(
            array_shell,
            child_ptr,
            child_format,
            parent_offset,
            parent_length,
            child_schema_n,
        )?
    };
    build_owned_node_admitted(&mut child_layout, child_schema, parent_missing)
}
