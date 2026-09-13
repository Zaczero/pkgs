//! Native Arrow C Data Interface admission into owned Arrow batches.
#![allow(
    clippy::too_many_lines,
    clippy::type_complexity,
    clippy::needless_range_loop,
    clippy::only_used_in_recursion,
    clippy::option_as_ref_deref,
    clippy::ref_option,
    clippy::unnecessary_wraps,
    clippy::needless_bool,
    clippy::if_not_else,
    clippy::manual_map,
    clippy::use_self,
    clippy::needless_question_mark,
    clippy::manual_find_map,
    clippy::manual_flatten,
    clippy::missing_const_for_fn,
    clippy::option_if_let_else,
    reason = "owned Arrow admission + recursive capture is intentionally explicit"
)]
#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]

use std::ffi::c_void;
use std::ptr;
use std::sync::Arc;

use pyo3::exceptions::PyTypeError;
use pyo3::prelude::{Bound, PyAny, PyResult};

use crate::collections::{HashMap, HashMapExt as _};
use crate::py::arrow::WkbOffsetWidth;
use crate::py::arrow_c::admitted::{
    AdmittedArrowSchema, ArrowReleaseGuard, MovedArrowShell, classify_admitted_geometry_schema,
};
use crate::py::arrow_c::foreign_buffer::ForeignArrowBuffer;
use crate::py::arrow_c::{
    ArrowArray, ArrowReleaseSlot, ArrowSchema, ClassifiedGeometrySchema, array_capsule_name,
    capsule_pointer, schema_capsule_name, try_arc_from_str, try_arc_from_vec, try_arc_new,
};

#[path = "native_decode.rs"]
mod decode;
pub(crate) use decode::{NativeArrowDecodeError, NativeDecodedBatch, NativeDecodedStorage};

/// Arrow C Data Interface `null_count`: `-1` means unknown; known counts must
/// not exceed the logical length.
///
/// Spec note (Arrow C Data Interface): a **null validity bitmap** means there
/// are no null values. When `null_count == -1` (unknown) and the bitmap pointer
/// is also null, treating the array as all-valid is the correct interpretation
/// — there is no bitmap to invent nulls from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NullCount {
    Unknown,
    Known(usize),
}

impl NullCount {
    fn parse(raw: i64, logical_length: usize) -> PyResult<Self> {
        if raw == -1 {
            return Ok(Self::Unknown);
        }
        if raw < -1 {
            return Err(PyTypeError::new_err("Arrow array null_count is below -1"));
        }
        let known = usize::try_from(raw)
            .map_err(|_| PyTypeError::new_err("Arrow array null_count is negative or too large"))?;
        if known > logical_length {
            return Err(PyTypeError::new_err(
                "Arrow array null_count exceeds logical length",
            ));
        }
        Ok(Self::Known(known))
    }
}

/// Schema format once, so buffer layout is schema-driven not position-inferred.
/// Only formats decoded end-to-end are admitted — no size-guessing `Other` arm.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ArrowFormat {
    /// `g` — float64 primitive values.
    Float64,
    /// `z` — binary with i32 offsets.
    Binary,
    /// `Z` — large binary with i64 offsets.
    LargeBinary,
    /// `vz` — binary view.
    BinaryView,
    /// `+l` — list with i32 offsets.
    List,
    /// `+L` — large list with i64 offsets.
    LargeList,
    /// `+w:N` — fixed-size list with exactly `N` values per row.
    FixedSizeList(usize),
    /// `+s` — struct.
    Struct,
}

impl ArrowFormat {
    /// Fallible parse: admit only formats decoded end-to-end.
    fn parse(format: &str) -> PyResult<Self> {
        match format {
            "g" => Ok(Self::Float64),
            "z" => Ok(Self::Binary),
            "Z" => Ok(Self::LargeBinary),
            "vz" => Ok(Self::BinaryView),
            "+l" => Ok(Self::List),
            "+L" => Ok(Self::LargeList),
            "+s" => Ok(Self::Struct),
            fixed if let Some(size) = fixed.strip_prefix("+w:") => {
                let size = size.parse::<usize>().map_err(|_| {
                    PyTypeError::new_err(format!("invalid Arrow fixed-size-list format '{format}'"))
                })?;
                if size == 0 {
                    return Err(PyTypeError::new_err(
                        "Arrow fixed-size-list size must be greater than zero",
                    ));
                }
                Ok(Self::FixedSizeList(size))
            },
            _ => Err(PyTypeError::new_err(format!(
                "unsupported Arrow schema format '{format}'"
            ))),
        }
    }

    const fn offset_width(self) -> Option<usize> {
        match self {
            Self::Binary | Self::List => Some(4),
            Self::LargeBinary | Self::LargeList => Some(8),
            _ => None,
        }
    }

    /// Exact buffer/child layout for this format. BinaryView admits
    /// `n_buffers >= 3` (validity + views + mandatory variadic-sizes table,
    /// with optional variadic data buffers before that table).
    fn validate_layout(
        self,
        n_buffers: usize,
        n_children: usize,
        schema_n_children: usize,
    ) -> PyResult<()> {
        if n_children != schema_n_children {
            return Err(PyTypeError::new_err(
                "Arrow array n_children does not match schema n_children",
            ));
        }
        match self {
            Self::Float64 => {
                if n_buffers != 2 || n_children != 0 {
                    return Err(PyTypeError::new_err(
                        "Arrow float64 array requires exactly 2 buffers and 0 children",
                    ));
                }
            },
            Self::Binary | Self::LargeBinary => {
                if n_buffers != 3 || n_children != 0 {
                    return Err(PyTypeError::new_err(
                        "Arrow binary array requires exactly 3 buffers and 0 children",
                    ));
                }
            },
            Self::List | Self::LargeList => {
                if n_buffers != 2 || n_children != 1 {
                    return Err(PyTypeError::new_err(
                        "Arrow list array requires exactly 2 buffers and 1 child",
                    ));
                }
            },
            Self::FixedSizeList(_) => {
                if n_buffers != 1 || n_children != 1 {
                    return Err(PyTypeError::new_err(
                        "Arrow fixed-size-list array requires exactly 1 buffer and 1 child",
                    ));
                }
            },
            Self::Struct => {
                if n_buffers != 1 {
                    return Err(PyTypeError::new_err(
                        "Arrow struct array requires exactly 1 buffer",
                    ));
                }
            },
            Self::BinaryView => {
                if n_buffers < 3 || n_children != 0 {
                    return Err(PyTypeError::new_err(
                        "Arrow binary-view array requires at least 3 buffers and 0 children",
                    ));
                }
            },
        }
        Ok(())
    }
}

/// Capture-window layout over a live producer array. **Not Send/Sync.** Holds
/// raw buffer/children pointer tables only for the duration of snapshot while a
/// [`MovedArrowShell`] pins the array; never stored on `NativeNode`.
///
/// The `'shell` lifetime is load-bearing: safe code cannot retain this layout
/// (or any producer table pointer it carries) after the owner shell drops.
///
/// Design-1: this is the temporary stand-in for the deleted `ValidatedArrowArray`
/// + `ArrowArrayPtr` pair. It must not escape admission.
struct CaptureLayout<'shell> {
    /// Owner shell that pins producer buffer/child tables for this window.
    shell: &'shell MovedArrowShell<ArrowArray>,
    /// Visible logical length (parent length for struct-field views).
    length: usize,
    /// Visible start index into the allocation.
    offset: usize,
    null_count: NullCount,
    n_buffers: usize,
    n_children: usize,
    format: ArrowFormat,
    /// Producer buffer table; live only while the capture shell is held.
    buffers: *const *const c_void,
    /// Producer children table; live only while the capture shell is held.
    children: *mut *mut ArrowArray,
    /// BinaryView only: sparse selected payload windows keyed by referenced
    /// non-inline data-buffer index. `None` for non-BinaryView.
    binary_view_data_ranges: Option<Arc<HashMap<usize, std::ops::Range<usize>>>>,
    /// Selected logical child span from an owned list-offset window.
    child_window: Option<(usize, usize)>,
}

impl<'shell> CaptureLayout<'shell> {
    /// Root / non-struct child: visible range equals the array's own offset+length.
    ///
    /// # Safety
    ///
    /// - `shell` pins the producer array tree for every use of the returned
    ///   layout (including buffer table walks during snapshot).
    /// - `ptr` is either the shell root or a live child under that shell.
    /// - Producer buffers/children stay readable and quiescent for the full
    ///   capture; no nested provider call or Python detach until snapshot ends.
    unsafe fn new(
        shell: &'shell MovedArrowShell<ArrowArray>,
        ptr: *const ArrowArray,
        format: ArrowFormat,
        schema_n_children: usize,
    ) -> PyResult<Self> {
        // SAFETY: forwarded from this function's caller.
        unsafe { Self::from_raw(shell, ptr, format, None, schema_n_children) }
    }

    /// Struct field: visible length is the parent's, offset inherits parent
    /// offset; reject when the visible end exceeds the child's raw allocation.
    ///
    /// # Safety
    ///
    /// Same as [`Self::new`]: `ptr` is a live child of the owner-backed tree.
    unsafe fn struct_child(
        shell: &'shell MovedArrowShell<ArrowArray>,
        ptr: *const ArrowArray,
        format: ArrowFormat,
        parent_offset: usize,
        parent_length: usize,
        schema_n_children: usize,
    ) -> PyResult<Self> {
        // SAFETY: forwarded from this function's caller.
        unsafe {
            Self::from_raw(
                shell,
                ptr,
                format,
                Some((parent_offset, parent_length)),
                schema_n_children,
            )
        }
    }

    /// # Safety
    ///
    /// Same as [`Self::new`].
    unsafe fn from_raw(
        shell: &'shell MovedArrowShell<ArrowArray>,
        ptr: *const ArrowArray,
        format: ArrowFormat,
        parent_slice: Option<(usize, usize)>,
        schema_n_children: usize,
    ) -> PyResult<Self> {
        if ptr.is_null() {
            return Err(PyTypeError::new_err("Arrow array pointer is null"));
        }
        // SAFETY: caller guarantees live shell-backed array for this pointer.
        let raw = unsafe { &*ptr };
        let raw_length = usize::try_from(raw.length)
            .map_err(|_| PyTypeError::new_err("Arrow array length is negative or too large"))?;
        let raw_offset = usize::try_from(raw.offset)
            .map_err(|_| PyTypeError::new_err("Arrow array offset is negative or too large"))?;
        let raw_end = raw_offset
            .checked_add(raw_length)
            .ok_or_else(|| PyTypeError::new_err("Arrow array offset+length overflows"))?;

        let (offset, length) = if let Some((parent_offset, parent_length)) = parent_slice {
            let offset = raw_offset
                .checked_add(parent_offset)
                .ok_or_else(|| PyTypeError::new_err("Arrow struct child offset overflows"))?;
            let end = offset.checked_add(parent_length).ok_or_else(|| {
                PyTypeError::new_err("Arrow struct child offset+length overflows")
            })?;
            if end > raw_end {
                return Err(PyTypeError::new_err(
                    "Arrow struct child visible range exceeds child allocation",
                ));
            }
            (offset, parent_length)
        } else {
            (raw_offset, raw_length)
        };
        offset
            .checked_add(length)
            .ok_or_else(|| PyTypeError::new_err("Arrow array offset+length overflows"))?;
        // Struct-child null_count is validated against the RAW child length;
        // a projected view preserves Known(0), preserves the raw count when the
        // view equals the full raw range, else exposes Unknown (inspect bitmap).
        let null_count = if parent_slice.is_some() {
            let raw_count = NullCount::parse(raw.null_count, raw_length)?;
            match raw_count {
                NullCount::Known(0) => NullCount::Known(0),
                count if offset == raw_offset && length == raw_length => count,
                _ => NullCount::Unknown,
            }
        } else {
            NullCount::parse(raw.null_count, length)?
        };
        let n_buffers = usize::try_from(raw.n_buffers)
            .map_err(|_| PyTypeError::new_err("Arrow array n_buffers is negative or too large"))?;
        let n_children = usize::try_from(raw.n_children)
            .map_err(|_| PyTypeError::new_err("Arrow array n_children is negative or too large"))?;
        format.validate_layout(n_buffers, n_children, schema_n_children)?;
        if n_buffers > 0 && raw.buffers.is_null() {
            return Err(PyTypeError::new_err("Arrow array buffers pointer is null"));
        }
        if n_children > 0 && raw.children.is_null() {
            return Err(PyTypeError::new_err("Arrow array children pointer is null"));
        }
        // Design-1: ONLY scalar/layout checks here. Content validation
        // (null_count vs bitmap, BinaryView descriptor scan) runs on OWNED
        // buffers after `snapshot_native_buffers` — never via producer
        // `from_raw_parts` or long-lived foreign slices.
        Ok(Self {
            shell,
            length,
            offset,
            null_count,
            n_buffers,
            n_children,
            format,
            buffers: raw.buffers,
            children: raw.children,
            binary_view_data_ranges: None,
            child_window: None,
        })
    }

    /// Buffer pointer at `index`, or null when absent.
    ///
    /// # Safety
    ///
    /// Capture shell must still be live (buffer table valid).
    unsafe fn buffer_ptr(&self, index: usize) -> *const c_void {
        if index >= self.n_buffers || self.buffers.is_null() {
            return ptr::null();
        }
        // SAFETY: layout validated n_buffers and non-null table when n_buffers > 0.
        unsafe { *self.buffers.add(index) }
    }

    /// Child array pointer at `index`.
    ///
    /// # Safety
    ///
    /// Capture shell must still be live.
    unsafe fn child_ptr(&self, index: usize) -> PyResult<*mut ArrowArray> {
        if index >= self.n_children {
            return Err(crate::py::errors::parse_error(
                format!(
                    "Arrow child index {index} is out of range for n_children={}",
                    self.n_children
                ),
                crate::error::ParseFormat::GeoArrow,
            ));
        }
        if self.children.is_null() {
            return Err(crate::py::errors::parse_error(
                "Arrow array children pointer is null",
                crate::error::ParseFormat::GeoArrow,
            ));
        }
        // SAFETY: layout-validated children table under live shell.
        let child = unsafe { *self.children.add(index) };
        if child.is_null() {
            return Err(crate::py::errors::parse_error(
                "Arrow child array is null",
                crate::error::ParseFormat::GeoArrow,
            ));
        }
        Ok(child)
    }
}

/// No-op release for stack-owned test arrays moved into [`MovedArrowShell`].
#[cfg(test)]
unsafe extern "C" fn test_stack_array_release(array: *mut ArrowArray) {
    // SAFETY: test owns the stack shell; null the slot so Drop is idempotent.
    unsafe {
        if !array.is_null() {
            *ArrowArray::release_slot(array) = None;
        }
    }
}

/// Run `f` with a capture layout whose lifetime is bound to a temporary shell
/// over a stack-owned test array (design-1 type-closure for unit tests).
#[cfg(test)]
fn with_stack_layout<R>(
    array: &mut ArrowArray,
    format: ArrowFormat,
    schema_n_children: usize,
    f: impl FnOnce(&mut CaptureLayout<'_>) -> R,
) -> R {
    if array.release.is_none() {
        array.release = Some(test_stack_array_release);
    }
    // SAFETY: stack array is live; release was just ensured non-null.
    let shell = unsafe { MovedArrowShell::take(array) }.expect("test shell take");
    // SAFETY: shell pins the moved stack array for the layout window.
    let mut layout =
        unsafe { CaptureLayout::new(&shell, shell.as_ptr(), format, schema_n_children) }
            .expect("test layout");
    f(&mut layout)
}

/// Like [`with_stack_layout`] but returns `PyResult` from the body.
#[cfg(test)]
fn try_with_stack_layout<R>(
    array: &mut ArrowArray,
    format: ArrowFormat,
    schema_n_children: usize,
    f: impl FnOnce(&mut CaptureLayout<'_>) -> PyResult<R>,
) -> PyResult<R> {
    if array.release.is_none() {
        array.release = Some(test_stack_array_release);
    }
    // SAFETY: stack array is live; release was just ensured non-null.
    let shell = unsafe { MovedArrowShell::take(array) }?;
    // SAFETY: shell pins the moved stack array for the layout window.
    let mut layout =
        unsafe { CaptureLayout::new(&shell, shell.as_ptr(), format, schema_n_children) }?;
    f(&mut layout)
}

/// Run capture + owned content validation (null_count / BinaryView ranges).
/// Unit tests that previously expected content errors from `new` use this.
#[cfg(test)]
fn layout_with_owned_content(
    array: &mut ArrowArray,
    format: ArrowFormat,
    schema_n_children: usize,
) -> PyResult<()> {
    try_with_stack_layout(array, format, schema_n_children, |layout| {
        let _ = snapshot_native_buffers(layout)?;
        Ok(())
    })
}

/// Content-validated layout for tests that need lengths after BinaryView scan.
#[cfg(test)]
fn with_owned_content_layout<R>(
    array: &mut ArrowArray,
    format: ArrowFormat,
    schema_n_children: usize,
    f: impl FnOnce(&mut CaptureLayout<'_>) -> R,
) -> PyResult<R> {
    try_with_stack_layout(array, format, schema_n_children, |layout| {
        let _ = snapshot_native_buffers(layout)?;
        Ok(f(layout))
    })
}

/// Shallow-move one Arrow base structure, leaving the producer's source shell
/// released without invoking its callback.
///
/// # Safety
///
/// `source` must be a live producer-owned base structure. The caller takes
/// ownership of the returned shell and must pass it to `drop_moved_arrow`.
pub(crate) unsafe fn move_arrow_shell<T: ArrowReleaseSlot>(source: *mut T) -> PyResult<*mut T> {
    // SAFETY: the caller guarantees a live base structure. A bytewise copy is
    // the Arrow-specified move operation; nulling source release leaves its
    // capsule-owned shell inert.
    unsafe {
        // Keep the producer release slot intact until the consumer shell has
        // been allocated. If the allocation is refused, the caller can still
        // release the original producer-owned shell exactly once.
        let moved = Box::try_new(ptr::read(source))
            .map_err(|_| crate::py::errors::native_memory_error())?;
        *T::release_slot(source) = None;
        Ok(Box::into_raw(moved))
    }
}

/// Release and deallocate the one consumer-owned shell returned by
/// `move_arrow_shell`.
///
/// # Safety
///
/// `shell` must be owned exactly once by this consumer.
pub(crate) unsafe fn drop_moved_arrow<T: ArrowReleaseSlot>(shell: *mut T) {
    // SAFETY: the caller transfers the single moved-shell allocation here.
    // Arrow requires the producer release callback to receive the LIVE structure
    // (with `release` still non-null) and mark it released itself. Clearing the
    // slot before the call made a conforming observing callback see NULL.
    unsafe {
        let release = *T::release_slot(shell);
        if let Some(release) = release {
            release(shell);
            // Conforming producers null the slot. Defensively clear so a
            // non-conforming callback cannot be observed twice from this shell.
            *T::release_slot(shell) = None;
        }
        drop(Box::from_raw(shell));
    }
}

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
    pub(crate) fn decode(self) -> Result<NativeDecodedBatch, NativeArrowDecodeError> {
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
    // non-allocating owner before the moved-shell allocation can fail.
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
    let (classification, struct_child) = NativeArrowClassification::from_schema(&classified)?;
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
        let metadata = schema.extension_metadata_bytes()?;
        let (crs, epoch) = crate::py::arrow::parse_geoarrow_extension_metadata_for(
            &metadata,
            Some(classification.encoding),
        )?;
        if !crate::boundary::metadata::optional_crs_labels_operationally_equal(
            classification.crs.as_deref().map(String::as_str),
            crs.as_deref(),
        )? || !crate::boundary::metadata::optional_epochs_equal(classification.epoch, epoch)
        {
            return Err(crate::py::arrow::geoarrow_parse_error(
                "Arrow C physical schema metadata conflicts with admitted Arrow metadata",
            ));
        }
    }
    crate::py::arrow_c::admitted::validate_admitted_encoding_storage(
        schema,
        classification.encoding,
    )?;
    if matches!(
        classification.encoding,
        crate::py::geoarrow::GeometryEncoding::Wkb
    ) {
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
        crate::py::errors::parse_error(
            "Arrow array n_children is negative or too large",
            crate::error::ParseFormat::GeoArrow,
        )
    })?;
    if child_index >= n_children {
        return Err(crate::py::errors::parse_error(
            format!("Arrow child index {child_index} is out of range for n_children={n_children}"),
            crate::error::ParseFormat::GeoArrow,
        ));
    }
    if root.children.is_null() {
        return Err(crate::py::errors::parse_error(
            "Arrow struct array children pointer is null",
            crate::error::ParseFormat::GeoArrow,
        ));
    }
    // SAFETY: children table non-null; index in range under live shell.
    let child_ptr = unsafe { *root.children.add(child_index) };
    if child_ptr.is_null() {
        return Err(crate::py::errors::parse_error(
            "Arrow child array is null",
            crate::error::ParseFormat::GeoArrow,
        ));
    }
    if child_index >= schema.children.len() {
        return Err(crate::py::errors::parse_error(
            format!(
                "Arrow child index {child_index} is out of range for schema n_children={}",
                schema.children.len()
            ),
            crate::error::ParseFormat::GeoArrow,
        ));
    }
    let child_schema = &schema.children[child_index];
    let child_format = ArrowFormat::parse(child_schema.format())?;
    let child_schema_n = child_schema.children.len();
    let parent_offset = usize::try_from(root.offset)
        .map_err(|_| PyTypeError::new_err("Arrow array offset is negative or too large"))?;
    let parent_length = usize::try_from(root.length)
        .map_err(|_| PyTypeError::new_err("Arrow array length is negative or too large"))?;
    // Optional parent validity for ancestor missing. Capture/snapshot errors
    // must propagate (C11): only a successfully admitted, genuinely absent
    // validity bitmap becomes `None`. Swallowing layout failures previously
    // accepted a parent with forged `n_buffers` / null_count / validity.
    // SAFETY: the moved shell owns the live root and `n_children` came from
    // that same root after checked conversion; `CaptureLayout::new` validates
    // its pointer table before any snapshot reads it.
    let mut parent_layout = unsafe {
        CaptureLayout::new(
            array_shell,
            array_shell.as_ptr(),
            ArrowFormat::Struct,
            n_children,
        )
    }?;
    let (parent_buffers, _) = snapshot_native_buffers(&mut parent_layout)?;
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

/// Owned native array node: buffers + children snapshotted at admission.
/// No producer shells, no raw buffer pointers after construction.
struct NativeNode {
    length: usize,
    /// Every owned payload is rebased to its visible logical origin.
    /// The physical bit alignment retained with the copied validity bytes.
    validity_offset: usize,
    n_buffers: usize,
    format: ArrowFormat,
    buffers: OwnedBufferSlots,
    #[expect(
        clippy::rc_buffer,
        reason = "the sharing handle must remain Arc<Vec<T>> so capacity stays part of heap accounting"
    )]
    children: Arc<Vec<NativeNode>>,
    /// This node's own schema field name (used by fixed-size-list dimensions).
    #[expect(
        clippy::rc_buffer,
        reason = "the sharing handle must remain Arc<String> so capacity stays part of heap accounting"
    )]
    schema_name: Arc<String>,
    /// Field names for struct children (empty otherwise).
    #[expect(
        clippy::rc_buffer,
        reason = "the sharing handle must remain Arc<Vec<T>> so capacity stays part of heap accounting"
    )]
    field_names: Arc<Vec<String>>,
    /// Combined ancestor + self missing mask (`true` = null).
    #[expect(
        clippy::rc_buffer,
        reason = "the sharing handle must remain Arc<Vec<T>> so capacity stays part of heap accounting"
    )]
    effective_missing: Option<Arc<Vec<bool>>>,
}

/// Classification resolved at the attached ingress boundary and carried with
/// one fully-owned batch.  `struct_child` deliberately does not live here: it
/// is only an admission instruction for table/record-batch roots.
#[derive(Clone, Debug)]
pub(crate) struct NativeArrowClassification {
    pub(crate) encoding: crate::py::geoarrow::GeometryEncoding,
    pub(crate) wkb_offset_width: WkbOffsetWidth,
    #[expect(
        clippy::rc_buffer,
        reason = "the sharing handle must remain Arc<String> so capacity stays part of heap accounting"
    )]
    pub(crate) crs: Option<Arc<String>>,
    pub(crate) epoch: Option<f64>,
}

/// One selected Arrow batch after producer-owned shells and buffers have been
/// detached from the protocol.  The decoder never needs Python or a raw Arrow
/// pointer after this value is constructed.
pub(crate) struct NativeArrowBatch {
    node: NativeNode,
    classification: NativeArrowClassification,
}

impl NativeArrowClassification {
    pub(crate) fn new(
        encoding: crate::py::geoarrow::GeometryEncoding,
        wkb_offset_width: WkbOffsetWidth,
        crs: Option<String>,
        epoch: Option<f64>,
    ) -> PyResult<Self> {
        Ok(Self {
            encoding,
            wkb_offset_width,
            crs: crs
                .map(crate::py::arrow_c::try_arc_from_string)
                .transpose()?,
            epoch,
        })
    }

    pub(crate) fn from_schema(
        classified: &ClassifiedGeometrySchema,
    ) -> PyResult<(Self, Option<usize>)> {
        Ok((
            Self {
                encoding: classified.encoding,
                wkb_offset_width: classified.wkb_offset_width,
                crs: classified
                    .crs
                    .as_deref()
                    .map(try_arc_from_str)
                    .transpose()?,
                epoch: classified.epoch,
            },
            classified.struct_child,
        ))
    }
}

impl NativeNode {
    fn child(&self, index: usize) -> Option<&Self> {
        self.children.get(index)
    }

    fn missing_at(&self, index: usize) -> bool {
        debug_assert!(self.validity_offset < 8);
        self.effective_missing
            .as_ref()
            .and_then(|missing| missing.get(index))
            .copied()
            .unwrap_or(false)
    }

    fn bytes_at(&self, index: usize) -> Option<&[u8]> {
        if index >= self.n_buffers {
            return None;
        }
        match self.buffers.get(index)? {
            OwnedBuffer::Bytes(bytes) => Some(bytes.as_slice()),
            OwnedBuffer::F64(_) => None,
        }
    }

    #[expect(
        clippy::rc_buffer,
        reason = "the sharing handle must remain Arc<Vec<T>> so capacity stays part of heap accounting"
    )]
    fn f64_values(&self) -> Option<&Arc<Vec<f64>>> {
        match self.buffers.get(1) {
            Some(OwnedBuffer::F64(values)) => Some(values),
            _ => None,
        }
    }

    fn offset_at(&self, index: usize) -> Option<usize> {
        let width = self.format.offset_width()?;
        let start = index.checked_mul(width)?;
        let end = start.checked_add(width)?;
        let bytes = self.bytes_at(1)?.get(start..end)?;
        let value = if width == 4 {
            let bytes = bytes.first_chunk::<4>()?;
            i64::from(i32::from_le_bytes(*bytes))
        } else {
            let bytes = bytes.first_chunk::<8>()?;
            i64::from_le_bytes(*bytes)
        };
        usize::try_from(value).ok()
    }

    fn list_range(&self, index: usize) -> Option<std::ops::Range<usize>> {
        if !matches!(self.format, ArrowFormat::List | ArrowFormat::LargeList) {
            return None;
        }
        let end_index = index.checked_add(1)?;
        if end_index > self.length {
            return None;
        }
        let start = self.offset_at(index)?;
        let end = self.offset_at(end_index)?;
        if start > end {
            return None;
        }
        let child_len = self.children.first().map_or(0, |child| child.length);
        if end > child_len {
            return None;
        }
        Some(start..end)
    }
}

/// Recursively snapshot one array node and its children while the array shell
/// is live; formats come from the already-owned schema tree.
#[expect(
    clippy::rc_buffer,
    reason = "the sharing handle must remain Arc<Vec<T>> so capacity stays part of heap accounting"
)]
fn build_owned_node_admitted(
    validated: &mut CaptureLayout,
    schema: &AdmittedArrowSchema,
    effective_missing: Option<Arc<Vec<bool>>>,
) -> PyResult<NativeNode> {
    let (buffers, _) = snapshot_native_buffers(validated)?;
    // Own-row nulls from owned validity, OR'd with any ancestor mask.
    let effective_missing = merge_owned_row_missing(validated, &buffers, effective_missing)?;
    let mut field_names = crate::try_vec_with_capacity(validated.n_children)?;
    let mut children = crate::try_vec_with_capacity(validated.n_children)?;
    if validated.n_children > 0 {
        if schema.children.len() != validated.n_children {
            return Err(crate::py::errors::parse_error(
                "Arrow child schema index out of range",
                crate::error::ParseFormat::GeoArrow,
            ));
        }
        for index in 0..validated.n_children {
            let child_schema = &schema.children[index];
            let Ok(name) = crate::boundary::try_string_from_str(child_schema.name()) else {
                drop(children);
                drop(field_names);
                return Err(crate::py::errors::native_memory_error());
            };
            field_names.push(name);
            // SAFETY: children live under capture shell for this construction.
            let child_array_ptr = unsafe { validated.child_ptr(index)? };
            let child_format = ArrowFormat::parse(child_schema.format())?;
            let child_schema_n = child_schema.children.len();
            let is_struct_field = matches!(validated.format, ArrowFormat::Struct);
            // SAFETY: same capture shell as parent.
            let mut child_validated = unsafe {
                if is_struct_field {
                    CaptureLayout::struct_child(
                        validated.shell,
                        child_array_ptr,
                        child_format,
                        validated.offset,
                        validated.length,
                        child_schema_n,
                    )?
                } else if matches!(validated.format, ArrowFormat::List | ArrowFormat::LargeList) {
                    let (child_offset, child_length) = validated.child_window.ok_or_else(|| {
                        PyTypeError::new_err(
                            "Arrow list offsets were not captured before child admission",
                        )
                    })?;
                    CaptureLayout::struct_child(
                        validated.shell,
                        child_array_ptr,
                        child_format,
                        child_offset,
                        child_length,
                        child_schema_n,
                    )?
                } else if let ArrowFormat::FixedSizeList(size) = validated.format {
                    let child_offset = validated.offset.checked_mul(size).ok_or_else(|| {
                        PyTypeError::new_err("Arrow fixed-size-list child offset overflows")
                    })?;
                    let child_length = validated.length.checked_mul(size).ok_or_else(|| {
                        PyTypeError::new_err("Arrow fixed-size-list child length overflows")
                    })?;
                    CaptureLayout::struct_child(
                        validated.shell,
                        child_array_ptr,
                        child_format,
                        child_offset,
                        child_length,
                        child_schema_n,
                    )?
                } else {
                    CaptureLayout::new(
                        validated.shell,
                        child_array_ptr,
                        child_format,
                        child_schema_n,
                    )?
                }
            };
            // Project the parent-effective mask onto every child carrier. A
            // Struct maps rows 1:1; list and fixed-size-list parents expand a
            // missing outer row to its selected child range. Child-own nulls
            // merge only after the child's snapshot in the recursive call.
            let child_missing = project_ancestor_missing(
                validated,
                &buffers,
                effective_missing.as_ref(),
                child_validated.length,
            )?;
            children.push(build_owned_node_admitted(
                &mut child_validated,
                child_schema,
                child_missing,
            )?);
        }
    }
    Ok(NativeNode {
        length: validated.length,
        validity_offset: validated.offset % 8,
        n_buffers: validated.n_buffers,
        format: validated.format,
        buffers,
        children: try_arc_from_vec(children)?,
        schema_name: crate::py::arrow_c::try_arc_from_str(schema.name())?,
        field_names: try_arc_from_vec(field_names)?,
        effective_missing,
    })
}

/// Project an already-owned effective parent mask onto its selected child.
///
/// The relation is structural, not a Struct special case: Struct preserves a
/// row index, variable lists expand it through their rebased owned offsets,
/// and fixed-size lists expand it through their declared width.  Child-own
/// nulls merge later via [`merge_owned_row_missing`] after the child has been
/// snapshotted — never through producer memory.
#[expect(
    clippy::rc_buffer,
    reason = "the sharing handle must remain Arc<Vec<T>> so capacity stays part of heap accounting"
)]
fn project_ancestor_missing(
    parent: &CaptureLayout,
    parent_buffers: &OwnedBufferSlots,
    parent_effective: Option<&Arc<Vec<bool>>>,
    child_length: usize,
) -> PyResult<Option<Arc<Vec<bool>>>> {
    let Some(parent_missing) = parent_effective else {
        return Ok(None);
    };
    if parent_missing.len() != parent.length {
        return Err(PyTypeError::new_err(
            "Arrow effective parent missing length does not match parent",
        ));
    }
    match parent.format {
        ArrowFormat::Struct => {
            if child_length != parent.length {
                return Err(PyTypeError::new_err(
                    "Arrow struct child visible length does not match parent",
                ));
            }
            Ok(Some(Arc::clone(parent_missing)))
        },
        ArrowFormat::List | ArrowFormat::LargeList => {
            let OwnedBuffer::Bytes(offsets) = parent_buffers.get(1).ok_or_else(|| {
                PyTypeError::new_err("Arrow list offsets are required for child mask projection")
            })?
            else {
                return Err(PyTypeError::new_err(
                    "Arrow list offsets must be owned bytes for child mask projection",
                ));
            };
            let width = parent
                .format
                .offset_width()
                .expect("list formats have an offset width");
            let read_offset = |row: usize| -> PyResult<usize> {
                let start = row
                    .checked_mul(width)
                    .ok_or_else(|| PyTypeError::new_err("Arrow list offset index overflows"))?;
                let end = start
                    .checked_add(width)
                    .ok_or_else(|| PyTypeError::new_err("Arrow list offset index overflows"))?;
                let raw = offsets.get(start..end).ok_or_else(|| {
                    PyTypeError::new_err("Arrow list offsets are shorter than declared")
                })?;
                let value = if width == 4 {
                    let raw = raw
                        .first_chunk::<4>()
                        .ok_or_else(|| PyTypeError::new_err("Arrow list offset is truncated"))?;
                    i64::from(i32::from_le_bytes(*raw))
                } else {
                    let raw = raw.first_chunk::<8>().ok_or_else(|| {
                        PyTypeError::new_err("Arrow list offset width is invalid")
                    })?;
                    i64::from_le_bytes(*raw)
                };
                usize::try_from(value).map_err(|_| {
                    crate::py::arrow::geoarrow_parse_error("Arrow offsets must be non-negative")
                })
            };
            let mut missing = crate::try_vec_with_capacity(child_length)?;
            missing.resize(child_length, false);
            for (row, &is_missing) in parent_missing.iter().enumerate() {
                if !is_missing {
                    continue;
                }
                let start = read_offset(row)?;
                let end =
                    read_offset(row.checked_add(1).ok_or_else(|| {
                        PyTypeError::new_err("Arrow list offset index overflows")
                    })?)?;
                if end < start {
                    return Err(crate::py::arrow::geoarrow_parse_error(
                        "Arrow list offsets must be ordered",
                    ));
                }
                let range = missing.get_mut(start..end).ok_or_else(|| {
                    crate::py::arrow::geoarrow_parse_error(
                        "Arrow list child offsets exceed the selected child length",
                    )
                })?;
                range.fill(true);
            }
            if missing.iter().any(|&is_missing| is_missing) {
                Ok(Some(try_arc_from_vec(missing)?))
            } else {
                Ok(None)
            }
        },
        ArrowFormat::FixedSizeList(width) => {
            let expected = parent.length.checked_mul(width).ok_or_else(|| {
                PyTypeError::new_err("Arrow fixed-size-list child length overflows")
            })?;
            if child_length != expected {
                return Err(PyTypeError::new_err(
                    "Arrow fixed-size-list child visible length does not match parent",
                ));
            }
            let mut missing = crate::try_vec_with_capacity(child_length)?;
            missing.resize(child_length, false);
            for (row, &is_missing) in parent_missing.iter().enumerate() {
                if !is_missing {
                    continue;
                }
                let start = row.checked_mul(width).ok_or_else(|| {
                    PyTypeError::new_err("Arrow fixed-size-list child offset overflows")
                })?;
                missing[start..start + width].fill(true);
            }
            Ok(Some(try_arc_from_vec(missing)?))
        },
        ArrowFormat::Binary
        | ArrowFormat::LargeBinary
        | ArrowFormat::BinaryView
        | ArrowFormat::Float64 => Ok(None),
    }
}

/// OR this node's owned validity nulls into an ancestor missing mask.
/// Runs only after [`snapshot_native_buffers`] so content is owned.
#[expect(
    clippy::rc_buffer,
    reason = "the sharing handle must remain Arc<Vec<T>> so capacity stays part of heap accounting"
)]
fn merge_owned_row_missing(
    array: &CaptureLayout,
    buffers: &OwnedBufferSlots,
    ancestor: Option<Arc<Vec<bool>>>,
) -> PyResult<Option<Arc<Vec<bool>>>> {
    let len = array.length;
    let mut missing = if let Some(a) = ancestor {
        if a.len() != len {
            return Err(PyTypeError::new_err(
                "Arrow ancestor missing length does not match child length",
            ));
        }
        let mut missing = crate::try_vec_with_capacity(len)?;
        missing.extend_from_slice(a.as_slice());
        missing
    } else {
        let mut missing = crate::try_vec_with_capacity(len)?;
        missing.resize(len, false);
        missing
    };
    let mut any = missing.iter().any(|&m| m);
    for row in 0..len {
        if missing[row] {
            continue;
        }
        if !owned_or_raw_row_valid(array, buffers, row)? {
            missing[row] = true;
            any = true;
        }
    }
    if any {
        Ok(Some(try_arc_from_vec(missing)?))
    } else {
        Ok(None)
    }
}

fn owned_or_raw_row_valid(
    array: &CaptureLayout,
    buffers: &OwnedBufferSlots,
    row: usize,
) -> PyResult<bool> {
    match array.null_count {
        NullCount::Known(0) => Ok(true),
        NullCount::Known(_) | NullCount::Unknown => {
            let Some(OwnedBuffer::Bytes(bitmap)) = buffers.get(0) else {
                if matches!(array.null_count, NullCount::Known(n) if n > 0) {
                    return Err(PyTypeError::new_err(
                        "Arrow validity bitmap is required when null_count > 0",
                    ));
                }
                return Ok(true);
            };
            if bitmap.is_empty() {
                return Ok(true);
            }
            let bit = array
                .offset
                .rem_euclid(8)
                .checked_add(row)
                .ok_or_else(|| PyTypeError::new_err("Arrow validity index overflows"))?;
            let byte_index = bit / 8;
            let byte = bitmap.get(byte_index).copied().ok_or_else(|| {
                PyTypeError::new_err("Arrow validity bitmap is shorter than declared")
            })?;
            Ok((byte & (1 << (bit % 8))) != 0)
        },
    }
}

/// Snapshot every schema-sized buffer once for this native node, then run
/// content validation **only on the owned copies** (design-1: never
/// `from_raw_parts` over producer memory for null_count / BinaryView scans).
///
/// Float64 *values* buffers (index 1) are decoded into `Arc<Vec<f64>>` at
/// admission so coordinate import reuses that Arc instead of retaining
/// LE bytes **and** materializing a second `Arc<Vec<f64>>`.
#[derive(Clone)]
enum OwnedBuffer {
    #[expect(
        clippy::rc_buffer,
        reason = "the sharing handle must remain Arc<Vec<T>> so capacity stays part of heap accounting"
    )]
    Bytes(Arc<Vec<u8>>),
    #[expect(
        clippy::rc_buffer,
        reason = "the sharing handle must remain Arc<Vec<T>> so capacity stays part of heap accounting"
    )]
    F64(Arc<Vec<f64>>),
}

/// Owned Arrow buffers. Ordinary formats have a fixed small dense layout;
/// BinaryView's variadic payload slots are deliberately sparse because an
/// imported selected range may reference only a few of thousands of producer
/// fragments. Keeping absent payloads out of this representation makes
/// admission proportional to selected descriptors, not `n_buffers`.
#[derive(Clone)]
enum OwnedBufferSlots {
    #[expect(
        clippy::rc_buffer,
        reason = "the sharing handle must remain Arc<Vec<T>> so capacity stays part of heap accounting"
    )]
    Dense(Arc<Vec<Option<OwnedBuffer>>>),
    BinaryView {
        validity: OwnedBuffer,
        views: OwnedBuffer,
        data: Arc<HashMap<usize, OwnedBuffer>>,
        n_buffers: usize,
    },
}

impl OwnedBufferSlots {
    fn get(&self, index: usize) -> Option<&OwnedBuffer> {
        match self {
            Self::Dense(slots) => slots.get(index).and_then(Option::as_ref),
            Self::BinaryView {
                validity,
                views,
                data,
                n_buffers,
            } => match index {
                0 => Some(validity),
                1 => Some(views),
                index if index >= 2 && index + 1 < *n_buffers => data.get(&(index - 2)),
                _ => None,
            },
        }
    }
}

/// Snapshot producer buffers under the live shell, then validate content on
/// owned storage. Returns owned slots + optional BinaryView selected-range map.
#[expect(
    clippy::iter_over_hash_type,
    reason = "range iteration only fills a keyed owned-buffer map; descriptor order remains the public order"
)]
fn snapshot_native_buffers(
    array: &mut CaptureLayout,
) -> PyResult<(
    OwnedBufferSlots,
    Option<Arc<HashMap<usize, std::ops::Range<usize>>>>,
)> {
    if matches!(array.format, ArrowFormat::BinaryView) {
        // Snapshot only the descriptor control buffers. Unrelated variadic payload
        // slots never participate in validation or decoding for this visible
        // range, so neither a dense slot vector nor a full sizes-table walk is
        // justified here.
        let (validity_start, validity_len) = visible_validity_range(array)?;
        let validity = snapshot_one_buffer_range(array, 0, validity_start, validity_len)?;
        let views = snapshot_one_buffer_range(
            array,
            1,
            checked_byte_span(array.offset, 16, "Arrow binary-view offset overflows")?,
            checked_byte_span(
                array.length,
                16,
                "Arrow binary-view buffer length overflows",
            )?,
        )?;
        // Phase 2: content-scan owned descriptors and snapshot just the
        // referenced 8-byte variadic-size entries → sparse ranges.
        let ranges = scan_binary_view_data_ranges_owned(array, &validity, &views)?;
        let views = rebase_owned_binary_view_offsets(array, &validity, &views, &ranges)?;
        array.binary_view_data_ranges = Some(Arc::clone(&ranges));
        // Phase 3: each referenced payload retains only its selected envelope
        // (not the physical parent prefix), after the owned descriptors have
        // been rebased to that local origin.
        let mut data = HashMap::new();
        data.try_reserve(ranges.len())
            .map_err(|_| crate::py::errors::native_memory_error())?;
        for (&data_index, range) in ranges.iter() {
            let len = range
                .end
                .checked_sub(range.start)
                .ok_or_else(|| PyTypeError::new_err("Arrow binary-view data range is invalid"))?;
            data.insert(
                data_index,
                snapshot_one_buffer_range(array, data_index + 2, range.start, len)?,
            );
        }
        validate_binary_view_prefixes(&validity, &views, &data, array)?;
        let slots = OwnedBufferSlots::BinaryView {
            validity,
            views,
            data: try_arc_new(data)?,
            n_buffers: array.n_buffers,
        };
        validate_owned_null_count(array, &slots)?;
        return Ok((slots, array.binary_view_data_ranges.clone()));
    }

    let mut snapshots = crate::try_vec_with_capacity(array.n_buffers)?;
    snapshots.push(Some(snapshot_one_buffer_range(
        array,
        0,
        visible_validity_range(array)?.0,
        visible_validity_range(array)?.1,
    )?));
    match array.format {
        ArrowFormat::Float64 => snapshots.push(Some(snapshot_one_buffer_range(
            array,
            1,
            checked_byte_span(array.offset, 8, "Arrow float64 offset overflows")?,
            checked_byte_span(array.length, 8, "Arrow float64 values length overflows")?,
        )?)),
        ArrowFormat::Binary
        | ArrowFormat::LargeBinary
        | ArrowFormat::List
        | ArrowFormat::LargeList => {
            let (offsets, start, end) = snapshot_rebased_offsets(array)?;
            snapshots.push(Some(offsets));
            if matches!(array.format, ArrowFormat::Binary | ArrowFormat::LargeBinary) {
                let len = end.checked_sub(start).ok_or_else(|| {
                    crate::py::arrow::geoarrow_parse_error("Arrow offsets must be ordered")
                })?;
                snapshots.push(Some(snapshot_one_buffer_range(array, 2, start, len)?));
            } else {
                array.child_window = Some((
                    start,
                    end.checked_sub(start).ok_or_else(|| {
                        crate::py::arrow::geoarrow_parse_error("Arrow offsets must be ordered")
                    })?,
                ));
            }
        },
        ArrowFormat::Struct | ArrowFormat::FixedSizeList(_) => {},
        ArrowFormat::BinaryView => unreachable!("handled above"),
    }

    let slots = OwnedBufferSlots::Dense(try_arc_from_vec(snapshots)?);
    // Content validation on OWNED validity only (P02 null_count match).
    validate_owned_null_count(array, &slots)?;
    let ranges = array.binary_view_data_ranges.clone();
    Ok((slots, ranges))
}

fn snapshot_one_buffer_range(
    array: &CaptureLayout,
    index: usize,
    byte_start: usize,
    len: usize,
) -> PyResult<OwnedBuffer> {
    if len == 0 {
        // Typed empty for float values so decode reuses Arc<Vec<f64>> without
        // a second LE→f64 materialization path.
        if matches!(array.format, ArrowFormat::Float64) && index == 1 {
            return Ok(OwnedBuffer::F64(try_arc_from_vec(Vec::new())?));
        }
        return Ok(OwnedBuffer::Bytes(try_arc_from_vec(Vec::new())?));
    }
    // SAFETY: layout validation + capture shell pin buffer table.
    let source = unsafe { array.buffer_ptr(index) };
    if source.is_null() {
        // Validity is optional. Every non-empty payload/offset buffer is
        // required; returning an empty snapshot here would let a later reader
        // index it and panic instead of reporting malformed Arrow input.
        if index == 0 {
            return Ok(OwnedBuffer::Bytes(try_arc_from_vec(Vec::new())?));
        }
        return Err(PyTypeError::new_err("Arrow required buffer is missing"));
    }
    // SAFETY: layout-validated buffer table under live shell; producer
    // quiescent for this finite capture; no nested Python.
    let source = unsafe { source.cast::<u8>().add(byte_start) };
    // SAFETY: `source` spans the layout-validated selected range and the
    // borrowed shell pins its producer allocation until this snapshot finishes.
    let foreign = unsafe { ForeignArrowBuffer::new(source, len, array.shell) };
    // SAFETY: ForeignArrowBuffer::snapshot contract (same as construction).
    let bytes = unsafe {
        foreign
            .snapshot()
            .map_err(|_| crate::py::errors::native_memory_error())?
    };
    // Float64 values: decode LE f64 once at admission; drop the byte copy.
    if matches!(array.format, ArrowFormat::Float64) && index == 1 {
        return owned_f64_from_le_bytes(&bytes).map(OwnedBuffer::F64);
    }
    Ok(OwnedBuffer::Bytes(try_arc_from_vec(bytes)?))
}

fn visible_validity_range(array: &CaptureLayout) -> PyResult<(usize, usize)> {
    let start = array.offset / 8;
    let bits = array
        .length
        .checked_add(array.offset % 8)
        .ok_or_else(|| PyTypeError::new_err("Arrow validity bitmap length overflows"))?;
    Ok((start, bits.div_ceil(8)))
}

/// Snapshot exactly the visible `(length + 1)` offset entries, then rebase
/// them in owned storage. The returned first/last offsets are the selected
/// child or byte span; no producer offset is inspected after this copy.
fn snapshot_rebased_offsets(array: &CaptureLayout) -> PyResult<(OwnedBuffer, usize, usize)> {
    let width = array
        .format
        .offset_width()
        .ok_or_else(|| PyTypeError::new_err("Arrow offsets require a variable-width format"))?;
    let slots = array
        .length
        .checked_add(1)
        .ok_or_else(|| PyTypeError::new_err("Arrow offset count overflows"))?;
    let start = checked_byte_span(array.offset, width, "Arrow offset start overflows")?;
    let len = checked_byte_span(slots, width, "Arrow offset window length overflows")?;
    // Arrow's canonical empty variable-width representation is one zero
    // offset. Some ABI-conforming producers omit that otherwise-empty buffer;
    // it carries no payload and is unambiguous only at physical offset zero.
    // A sliced empty array still has a meaningful start slot and therefore
    // must provide it for validation.
    let raw = if array.length == 0
        && array.offset == 0
        // SAFETY: `CaptureLayout` pins the producer buffer table for this
        // admission snapshot.
        && unsafe { array.buffer_ptr(1) }.is_null()
    {
        let mut empty = crate::try_vec_with_capacity(width)?;
        empty.resize(width, 0);
        try_arc_from_vec(empty)?
    } else {
        let OwnedBuffer::Bytes(raw) = snapshot_one_buffer_range(array, 1, start, len)? else {
            unreachable!("offset snapshots are byte buffers")
        };
        raw
    };
    let read = |index: usize| -> PyResult<i64> {
        let at = index
            .checked_mul(width)
            .ok_or_else(|| PyTypeError::new_err("Arrow offset index overflows"))?;
        let end = at
            .checked_add(width)
            .ok_or_else(|| PyTypeError::new_err("Arrow offset index overflows"))?;
        let bytes = raw
            .get(at..end)
            .ok_or_else(|| PyTypeError::new_err("Arrow offsets buffer is shorter than declared"))?;
        Ok(if width == 4 {
            let bytes = bytes
                .first_chunk::<4>()
                .ok_or_else(|| PyTypeError::new_err("Arrow offset is truncated"))?;
            i64::from(i32::from_le_bytes(*bytes))
        } else {
            let bytes = bytes
                .first_chunk::<8>()
                .ok_or_else(|| PyTypeError::new_err("Arrow offset width is invalid"))?;
            i64::from_le_bytes(*bytes)
        })
    };
    let first = usize::try_from(read(0)?).map_err(|_| {
        crate::py::arrow::geoarrow_parse_error("Arrow offsets must be non-negative")
    })?;
    let mut rebased = crate::try_vec_with_capacity(raw.len())?;
    rebased.extend_from_slice(&raw);
    let mut last = first;
    for index in 0..slots {
        let value = usize::try_from(read(index)?).map_err(|_| {
            crate::py::arrow::geoarrow_parse_error("Arrow offsets must be non-negative")
        })?;
        if value < last {
            return Err(crate::py::arrow::geoarrow_parse_error(
                "Arrow offsets must be ordered",
            ));
        }
        let local = value.checked_sub(first).ok_or_else(|| {
            crate::py::arrow::geoarrow_parse_error("Arrow offsets must be ordered")
        })?;
        last = value;
        let at = index * width;
        if width == 4 {
            let local = i32::try_from(local).map_err(|_| {
                crate::py::arrow::geoarrow_parse_error("Arrow offset exceeds i32 range")
            })?;
            rebased[at..at + width].copy_from_slice(&local.to_le_bytes());
        } else {
            let local = i64::try_from(local).map_err(|_| {
                crate::py::arrow::geoarrow_parse_error("Arrow offset exceeds i64 range")
            })?;
            rebased[at..at + width].copy_from_slice(&local.to_le_bytes());
        }
    }
    Ok((OwnedBuffer::Bytes(try_arc_from_vec(rebased)?), first, last))
}

/// Validate the four-byte BinaryView inline prefix against the selected owned
/// payload.  The descriptor and data are already copied, so this check never
/// re-borrows producer memory and remains safe for sparse high-index buffers.
fn validate_binary_view_prefixes(
    validity: &OwnedBuffer,
    views: &OwnedBuffer,
    data: &HashMap<usize, OwnedBuffer>,
    array: &CaptureLayout,
) -> PyResult<()> {
    let OwnedBuffer::Bytes(validity) = validity else {
        return Ok(());
    };
    let OwnedBuffer::Bytes(views) = views else {
        return Ok(());
    };
    let validity_buffer = OwnedBuffer::Bytes(Arc::clone(validity));
    let (descriptors, _) = views.as_chunks::<16>();
    for (row, view) in descriptors.iter().take(array.length).enumerate() {
        if !owned_binary_view_row_is_valid(array, &validity_buffer, row)? {
            continue;
        }
        let length = i32::from_le_bytes([view[0], view[1], view[2], view[3]]);
        if length <= 12 {
            continue;
        }
        let buffer_index =
            usize::try_from(i32::from_le_bytes([view[8], view[9], view[10], view[11]]))
                .map_err(|_| PyTypeError::new_err("Arrow binary-view buffer index is negative"))?;
        let byte_offset =
            usize::try_from(i32::from_le_bytes([view[12], view[13], view[14], view[15]]))
                .map_err(|_| PyTypeError::new_err("Arrow binary-view byte offset is negative"))?;
        // `views` has already been rebased to the selected data envelope, so
        // the descriptor's offset is directly an index into the owned payload.
        let local = byte_offset;
        let prefix_end = local
            .checked_add(4)
            .ok_or_else(|| PyTypeError::new_err("Arrow binary-view prefix range overflows"))?;
        let payload = match data.get(&buffer_index) {
            Some(OwnedBuffer::Bytes(bytes)) => bytes
                .get(local..prefix_end)
                .ok_or_else(|| PyTypeError::new_err("Arrow binary-view prefix is out of bounds"))?,
            _ => {
                return Err(PyTypeError::new_err(
                    "Arrow binary-view data buffer is missing",
                ));
            },
        };
        if payload != &view[4..8] {
            return Err(PyTypeError::new_err(
                "Arrow binary-view prefix does not match referenced data",
            ));
        }
    }
    Ok(())
}

#[expect(
    clippy::rc_buffer,
    reason = "the sharing handle must remain Arc<Vec<T>> so capacity stays part of heap accounting"
)]
fn owned_f64_from_le_bytes(bytes: &[u8]) -> PyResult<Arc<Vec<f64>>> {
    if !bytes.len().is_multiple_of(8) {
        return Err(PyTypeError::new_err(
            "Arrow float64 values length is not a multiple of 8",
        ));
    }
    let (chunks, rest) = bytes.as_chunks::<8>();
    debug_assert!(rest.is_empty());
    let mut values = crate::try_vec_with_capacity(chunks.len())?;
    for chunk in chunks {
        crate::try_push(&mut values, f64::from_le_bytes(*chunk))?;
    }
    Ok(try_arc_from_vec(values)?)
}

/// P02: Known(n>0) null_count must match the owned validity bitmap.
fn validate_owned_null_count(array: &CaptureLayout, buffers: &OwnedBufferSlots) -> PyResult<()> {
    let NullCount::Known(expected) = array.null_count else {
        return Ok(());
    };
    if expected == 0 {
        return Ok(());
    }
    let mut actual = 0_usize;
    for row in 0..array.length {
        if !owned_or_raw_row_valid(array, buffers, row)? {
            actual += 1;
        }
    }
    if actual != expected {
        return Err(crate::py::arrow::geoarrow_parse_error(format!(
            "Arrow null_count ({expected}) does not match validity bitmap ({actual} null rows)"
        )));
    }
    Ok(())
}

/// Checked byte span for every `from_raw_parts` length: reject `usize`
/// overflow and any result greater than `isize::MAX` (Rust slice UB).
fn checked_byte_span(count: usize, width: usize, overflow: &'static str) -> PyResult<usize> {
    let bytes = count
        .checked_mul(width)
        .ok_or_else(|| PyTypeError::new_err(overflow))?;
    if bytes > isize::MAX as usize {
        return Err(PyTypeError::new_err(overflow));
    }
    Ok(bytes)
}

/// Decode one entry of an **owned** BinaryView variadic-sizes table.
fn binary_view_declared_data_size(sizes: &[u8], buffer_index: usize) -> PyResult<usize> {
    let byte_offset = checked_byte_span(
        buffer_index,
        8,
        "Arrow binary-view variadic-sizes index overflows",
    )?;
    let end = byte_offset
        .checked_add(8)
        .ok_or_else(|| PyTypeError::new_err("Arrow binary-view variadic-sizes index overflows"))?;
    let bytes = sizes
        .get(byte_offset..end)
        .ok_or_else(|| PyTypeError::new_err("Arrow binary-view buffer index out of range"))?
        .first_chunk::<8>()
        .ok_or_else(|| PyTypeError::new_err("Arrow binary-view variadic size is truncated"))?;
    usize::try_from(i64::from_le_bytes(*bytes)).map_err(|_| {
        PyTypeError::new_err("Arrow binary-view variadic data buffer size is negative or too large")
    })
}

/// BinaryView selected-span scan on owned validity/views (design-1). Each
/// referenced descriptor snapshots its own 8-byte variadic-size entry; data
/// buffers record the smallest enclosing selected range, never unrelated
/// producer fragments or a dense sizes table.
fn scan_binary_view_data_ranges_owned(
    array: &CaptureLayout,
    validity: &OwnedBuffer,
    views: &OwnedBuffer,
) -> PyResult<Arc<HashMap<usize, std::ops::Range<usize>>>> {
    let mut ranges = HashMap::new();
    if array.length == 0 {
        return Ok(try_arc_new(ranges)?);
    }
    let max_data_buffers = array.n_buffers - 3;
    let OwnedBuffer::Bytes(views) = views else {
        return Ok(try_arc_new(ranges)?);
    };
    if views.is_empty() {
        return Ok(try_arc_new(ranges)?);
    }
    let sizes_index = array.n_buffers - 1;
    let mut declared_sizes = HashMap::new();
    let (descriptors, _) = views.as_chunks::<16>();
    if descriptors.len() < array.length {
        return Err(PyTypeError::new_err(
            "Arrow binary-view buffer is shorter than declared",
        ));
    }
    for (row, view) in descriptors[..array.length].iter().enumerate() {
        if !owned_binary_view_row_is_valid(array, validity, row)? {
            continue;
        }
        let length = i32::from_le_bytes([view[0], view[1], view[2], view[3]]);
        let length = usize::try_from(length)
            .map_err(|_| PyTypeError::new_err("Arrow binary-view length is negative"))?;
        if length <= 12 {
            if view[4 + length..16].iter().any(|&b| b != 0) {
                return Err(PyTypeError::new_err(
                    "Arrow binary-view inline padding must be zero",
                ));
            }
            continue;
        }
        let buffer_index = i32::from_le_bytes([view[8], view[9], view[10], view[11]]);
        let buffer_index = usize::try_from(buffer_index)
            .map_err(|_| PyTypeError::new_err("Arrow binary-view buffer index is negative"))?;
        if buffer_index >= max_data_buffers {
            return Err(PyTypeError::new_err(
                "Arrow binary-view buffer index out of range",
            ));
        }
        let byte_offset = i32::from_le_bytes([view[12], view[13], view[14], view[15]]);
        let byte_offset = usize::try_from(byte_offset)
            .map_err(|_| PyTypeError::new_err("Arrow binary-view byte offset is negative"))?;
        let span_end = byte_offset
            .checked_add(length)
            .ok_or_else(|| PyTypeError::new_err("Arrow binary-view byte range overflows"))?;
        checked_byte_span(span_end, 1, "Arrow binary-view data length overflows")?;
        // The sizes table is advisory for unreferenced fragments. Snapshot and
        // validate precisely the selected descriptor's entry, not the whole
        // `n_buffers - 3` table.
        let declared_size = if let Some(size) = declared_sizes.get(&buffer_index) {
            *size
        } else {
            declared_sizes
                .try_reserve(1)
                .map_err(|_| crate::py::errors::native_memory_error())?;
            let byte_start = checked_byte_span(
                buffer_index,
                8,
                "Arrow binary-view variadic-sizes index overflows",
            )?;
            let OwnedBuffer::Bytes(size) =
                snapshot_one_buffer_range(array, sizes_index, byte_start, 8)?
            else {
                unreachable!("variadic sizes are byte buffers")
            };
            let declared_size = binary_view_declared_data_size(&size, 0)?;
            declared_sizes.insert(buffer_index, declared_size);
            declared_size
        };
        if span_end > declared_size {
            return Err(PyTypeError::new_err(
                "Arrow binary-view byte range exceeds declared data buffer size",
            ));
        }
        // Prefix match deferred until data buffers are snapshotted (owned).
        if !ranges.contains_key(&buffer_index) {
            ranges
                .try_reserve(1)
                .map_err(|_| crate::py::errors::native_memory_error())?;
        }
        ranges
            .entry(buffer_index)
            .and_modify(|range: &mut std::ops::Range<usize>| {
                range.start = range.start.min(byte_offset);
                range.end = range.end.max(span_end);
            })
            .or_insert(byte_offset..span_end);
    }
    Ok(try_arc_new(ranges)?)
}

/// Rebase non-inline BinaryView descriptor offsets to their selected owned
/// data envelope. This happens only after every descriptor has been checked
/// against the declared producer size, and before the producer payloads are
/// captured, so later generic decode sees a self-contained Arrow view.
fn rebase_owned_binary_view_offsets(
    array: &CaptureLayout,
    validity: &OwnedBuffer,
    views: &OwnedBuffer,
    ranges: &HashMap<usize, std::ops::Range<usize>>,
) -> PyResult<OwnedBuffer> {
    let OwnedBuffer::Bytes(views) = views else {
        return Ok(views.clone());
    };
    let mut rebased = crate::try_vec_with_capacity(views.len())?;
    rebased.extend_from_slice(views);
    let (views, remainder) = rebased.as_chunks_mut::<16>();
    debug_assert!(remainder.is_empty());
    for (row, view) in views.iter_mut().enumerate() {
        // Null BinaryView descriptors are explicitly inert: their control
        // bytes may be arbitrary and must neither establish a range nor be
        // parsed/rebased (the same rule as the owned range scan).
        if !owned_binary_view_row_is_valid(array, validity, row)? {
            continue;
        }
        let length = i32::from_le_bytes([view[0], view[1], view[2], view[3]]);
        let length = usize::try_from(length)
            .map_err(|_| PyTypeError::new_err("Arrow binary-view length is negative"))?;
        if length <= 12 {
            continue;
        }
        let buffer_index = i32::from_le_bytes([view[8], view[9], view[10], view[11]]);
        let buffer_index = usize::try_from(buffer_index)
            .map_err(|_| PyTypeError::new_err("Arrow binary-view buffer index is negative"))?;
        let byte_offset = i32::from_le_bytes([view[12], view[13], view[14], view[15]]);
        let byte_offset = usize::try_from(byte_offset)
            .map_err(|_| PyTypeError::new_err("Arrow binary-view byte offset is negative"))?;
        let range = ranges.get(&buffer_index).ok_or_else(|| {
            PyTypeError::new_err("Arrow binary-view data range is missing for a referenced view")
        })?;
        let local = byte_offset
            .checked_sub(range.start)
            .ok_or_else(|| PyTypeError::new_err("Arrow binary-view data range is invalid"))?;
        let local = i32::try_from(local)
            .map_err(|_| PyTypeError::new_err("Arrow binary-view offset exceeds i32 range"))?;
        view[12..16].copy_from_slice(&local.to_le_bytes());
    }
    Ok(OwnedBuffer::Bytes(try_arc_from_vec(rebased)?))
}

/// Owned-bitmap BinaryView validity (no producer slice).
fn owned_binary_view_row_is_valid(
    array: &CaptureLayout,
    validity: &OwnedBuffer,
    row: usize,
) -> PyResult<bool> {
    match array.null_count {
        NullCount::Known(0) => Ok(true),
        NullCount::Known(n) if n == array.length => Ok(false),
        NullCount::Known(_) | NullCount::Unknown => {
            let OwnedBuffer::Bytes(bitmap) = validity else {
                if matches!(array.null_count, NullCount::Known(n) if n > 0) {
                    return Err(PyTypeError::new_err(
                        "Arrow binary-view validity bitmap is required when null_count > 0",
                    ));
                }
                return Ok(true);
            };
            if bitmap.is_empty() {
                return Ok(true);
            }
            let bit = (array.offset % 8).checked_add(row).ok_or_else(|| {
                PyTypeError::new_err("Arrow binary-view validity index overflows")
            })?;
            let byte_index = bit / 8;
            let byte = bitmap.get(byte_index).copied().ok_or_else(|| {
                PyTypeError::new_err("Arrow binary-view validity bitmap is shorter than declared")
            })?;
            Ok((byte & (1 << (bit % 8))) != 0)
        },
    }
}

/// Layout-validation unit tests — drive the shipped pure validation path
/// without pyarrow, so a broken `NullCount` / buffer-length / struct-child
/// gate fails this battery even when Python round-trips still pass.
#[cfg(test)]
#[expect(
    clippy::assertions_on_result_states,
    reason = "error-path unit tests assert is_err without Debug on Ok variants"
)]
#[expect(
    clippy::undocumented_unsafe_blocks,
    reason = "test shells are stack-owned; each CaptureLayout::new is live only for the assertion"
)]
mod layout_validation_tests {
    use std::ptr;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use pyo3::exceptions::PyMemoryError;
    use pyo3::ffi;
    use pyo3::prelude::Python;

    use super::*;

    // Global release counters are shared across the two capsule lifecycle
    // tests. Parallel nextest interleaves `before + N` assertions and flakes
    // (passes alone, fails with -j2). Serialize both tests on this mutex so
    // each `before + N` window is exclusive; counters stay process-global for
    // the release callbacks (which cannot carry per-test state).
    static CAPSULE_RELEASE_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    static MOVED_SCHEMA_RELEASES: AtomicUsize = AtomicUsize::new(0);
    static MOVED_ARRAY_RELEASES: AtomicUsize = AtomicUsize::new(0);

    unsafe extern "C" fn count_moved_schema_release(schema: *mut ArrowSchema) {
        MOVED_SCHEMA_RELEASES.fetch_add(1, Ordering::Relaxed);
        // SAFETY: the test callback receives the consumer-owned moved shell.
        unsafe {
            (*schema).release = None;
        }
    }

    unsafe extern "C" fn stack_schema_release(schema: *mut ArrowSchema) {
        // SAFETY: the test owns this stack shell and only clears the release
        // slot so `MovedArrowShell` can finish its idempotent drop.
        unsafe {
            if !schema.is_null() {
                (*schema).release = None;
            }
        }
    }

    unsafe extern "C" fn count_moved_array_release(array: *mut ArrowArray) {
        MOVED_ARRAY_RELEASES.fetch_add(1, Ordering::Relaxed);
        // SAFETY: the test callback receives the consumer-owned moved shell.
        unsafe {
            (*array).release = None;
        }
    }

    fn ensure_python() {
        // `NullCount::parse` and friends construct `PyErr` on failure paths.
        crate::test_support::initialize_python();
    }

    fn stack_array(
        length: i64,
        offset: i64,
        null_count: i64,
        n_buffers: i64,
        n_children: i64,
        buffers: *const *const c_void,
        children: *mut *mut ArrowArray,
    ) -> ArrowArray {
        ArrowArray {
            length,
            null_count,
            offset,
            n_buffers,
            n_children,
            buffers,
            children,
            dictionary: ptr::null_mut(),
            release: None,
            private_data: ptr::null_mut(),
        }
    }

    /// Dummy non-null buffer pointer table so `n_buffers > 0` arrays pass the
    /// null-buffers gate when the test only exercises schema-driven lengths.
    fn dummy_buffers(n: usize) -> Vec<*const c_void> {
        vec![ptr::null(); n]
    }

    #[test]
    fn null_count_accepts_unknown_minus_one_and_known_in_range() {
        ensure_python();
        assert_eq!(NullCount::parse(-1, 10).unwrap(), NullCount::Unknown);
        assert_eq!(NullCount::parse(-1, 0).unwrap(), NullCount::Unknown);
        assert_eq!(NullCount::parse(0, 10).unwrap(), NullCount::Known(0));
        assert_eq!(NullCount::parse(3, 10).unwrap(), NullCount::Known(3));
        assert_eq!(NullCount::parse(10, 10).unwrap(), NullCount::Known(10));
        assert_eq!(NullCount::parse(0, 0).unwrap(), NullCount::Known(0));
    }

    #[test]
    fn null_count_rejects_below_minus_one() {
        ensure_python();
        assert!(NullCount::parse(-2, 10).is_err());
        assert!(NullCount::parse(-100, 1).is_err());
        assert!(NullCount::parse(i64::MIN, 0).is_err());
    }

    #[test]
    fn null_count_rejects_known_exceeding_logical_length() {
        ensure_python();
        assert!(NullCount::parse(1, 0).is_err());
        assert!(NullCount::parse(11, 10).is_err());
        assert!(NullCount::parse(i64::MAX, 1).is_err());
    }

    #[test]
    fn arrow_format_parse_is_fallible_rejects_other_and_accepts_large_list() {
        ensure_python();
        assert_eq!(ArrowFormat::parse("g").unwrap(), ArrowFormat::Float64);
        assert_eq!(ArrowFormat::parse("z").unwrap(), ArrowFormat::Binary);
        assert_eq!(ArrowFormat::parse("Z").unwrap(), ArrowFormat::LargeBinary);
        assert_eq!(ArrowFormat::parse("vz").unwrap(), ArrowFormat::BinaryView);
        assert_eq!(ArrowFormat::parse("+l").unwrap(), ArrowFormat::List);
        assert_eq!(ArrowFormat::parse("+L").unwrap(), ArrowFormat::LargeList);
        assert_eq!(ArrowFormat::parse("+s").unwrap(), ArrowFormat::Struct);
        // Defect: int8 "c" tagged geoarrow.wkb used to size-guess as Other.
        assert!(ArrowFormat::parse("c").is_err());
        assert!(ArrowFormat::parse("i").is_err());
        assert_eq!(ArrowFormat::Binary.offset_width(), Some(4));
        assert_eq!(ArrowFormat::LargeBinary.offset_width(), Some(8));
        assert_eq!(ArrowFormat::List.offset_width(), Some(4));
        assert_eq!(ArrowFormat::LargeList.offset_width(), Some(8));
        assert_eq!(ArrowFormat::Float64.offset_width(), None);
    }

    #[test]
    fn validity_snapshot_uses_offset_plus_length_not_null_count() {
        ensure_python();
        // Visible rows [5, 10) straddle bitmap bytes 0 and 1.  This enters
        // `snapshot_native_buffers`, the only shipping owner of the visible
        // validity window.  Mutating `visible_validity_range` to `(0, 0)`
        // makes the first assertion fail with an empty snapshot.
        let validity = [0b1110_0000_u8, 0b0000_0011];
        let values: [f64; 10] = std::array::from_fn(|index| index as f64);
        let buffers: [*const c_void; 2] = [validity.as_ptr().cast(), values.as_ptr().cast()];
        let mut array = stack_array(5, 5, 0, 2, 0, buffers.as_ptr(), ptr::null_mut());
        try_with_stack_layout(&mut array, ArrowFormat::Float64, 0, |layout| {
            let (slots, _) = snapshot_native_buffers(layout)?;
            let OwnedBuffer::Bytes(bitmap) = slots.get(0).expect("owned validity") else {
                panic!("validity is a byte bitmap")
            };
            assert_eq!(bitmap.as_slice(), validity);
            let OwnedBuffer::F64(visible_values) = slots.get(1).expect("owned values") else {
                panic!("float64 values are decoded at admission")
            };
            assert_eq!(visible_values.as_slice(), &values[5..]);
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn validated_array_rejects_excessive_null_count_at_construction() {
        ensure_python();
        let bufs = dummy_buffers(2);
        let mut array = stack_array(3, 0, 4, 2, 0, bufs.as_ptr(), ptr::null_mut());
        assert!(try_with_stack_layout(&mut array, ArrowFormat::Float64, 0, |_| Ok(())).is_err());
        let mut array = stack_array(3, 0, -2, 2, 0, bufs.as_ptr(), ptr::null_mut());
        assert!(try_with_stack_layout(&mut array, ArrowFormat::Float64, 0, |_| Ok(())).is_err());
    }

    #[test]
    fn known_null_count_must_match_visible_validity_bitmap() {
        ensure_python();
        // null_count=1 but validity bit set (all valid) — P02 mismatch.
        let validity: [u8; 1] = [0b0000_0001];
        let data = [0_u8; 8];
        let buffers: [*const c_void; 2] = [validity.as_ptr().cast(), data.as_ptr().cast()];
        let mut array = stack_array(1, 0, 1, 2, 0, buffers.as_ptr(), ptr::null_mut());
        let err = layout_with_owned_content(&mut array, ArrowFormat::Float64, 0);
        let message = match err {
            Ok(()) => panic!("expected null_count/bitmap mismatch to be rejected"),
            Err(error) => error.to_string(),
        };
        assert!(
            message.contains("null_count") && message.contains("validity bitmap"),
            "unexpected error: {message}"
        );
        // Matching pair: null_count=1, bit0 clear.
        let validity_null: [u8; 1] = [0];
        let buffers: [*const c_void; 2] = [validity_null.as_ptr().cast(), data.as_ptr().cast()];
        let mut array = stack_array(1, 0, 1, 2, 0, buffers.as_ptr(), ptr::null_mut());
        assert!(layout_with_owned_content(&mut array, ArrowFormat::Float64, 0).is_ok());
    }

    #[test]
    fn validated_array_accepts_unknown_null_count() {
        ensure_python();
        let bufs = dummy_buffers(3);
        let mut array = stack_array(4, 0, -1, 3, 0, bufs.as_ptr(), ptr::null_mut());
        with_stack_layout(&mut array, ArrowFormat::Binary, 0, |validated| {
            assert_eq!(validated.null_count, NullCount::Unknown);
            assert_eq!(validated.null_count, NullCount::Unknown);
        });
    }

    #[test]
    fn layout_rejects_wrong_buffer_and_child_cardinalities() {
        ensure_python();
        // Float64 with excess n_buffers (pointer-table OOB path).
        let bufs = dummy_buffers(3);
        let mut array = stack_array(1, 0, 0, 3, 0, bufs.as_ptr(), ptr::null_mut());
        assert!(try_with_stack_layout(&mut array, ArrowFormat::Float64, 0, |_| Ok(())).is_err());
        // Binary with too few buffers.
        let bufs = dummy_buffers(2);
        let mut array = stack_array(1, 0, 0, 2, 0, bufs.as_ptr(), ptr::null_mut());
        assert!(try_with_stack_layout(&mut array, ArrowFormat::Binary, 0, |_| Ok(())).is_err());
        // Schema/array child-count mismatch.
        let bufs = dummy_buffers(2);
        let mut array = stack_array(1, 0, 0, 2, 0, bufs.as_ptr(), ptr::null_mut());
        assert!(try_with_stack_layout(&mut array, ArrowFormat::Float64, 1, |_| Ok(())).is_err());
        // BinaryView now requires validity + views + mandatory sizes table.
        assert!(layout_with_owned_content(&mut array, ArrowFormat::BinaryView, 0).is_err());
    }

    #[test]
    fn imported_capsules_move_once_and_release_the_owned_shells() {
        ensure_python();
        let _guard = CAPSULE_RELEASE_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let schema_before = MOVED_SCHEMA_RELEASES.load(Ordering::Relaxed);
        let array_before = MOVED_ARRAY_RELEASES.load(Ordering::Relaxed);
        Python::attach(|py| {
            // Bare binary WKB admits without extension metadata.
            let mut schema = ArrowSchema {
                format: c"z".as_ptr(),
                name: ptr::null(),
                metadata: ptr::null(),
                flags: 0,
                n_children: 0,
                children: ptr::null_mut(),
                dictionary: ptr::null_mut(),
                release: Some(count_moved_schema_release),
                private_data: ptr::null_mut(),
            };
            let buffers = [ptr::null(), ptr::null(), ptr::null()];
            let mut array = ArrowArray {
                length: 0,
                null_count: 0,
                offset: 0,
                n_buffers: 3,
                n_children: 0,
                buffers: buffers.as_ptr(),
                children: ptr::null_mut(),
                dictionary: ptr::null_mut(),
                release: Some(count_moved_array_release),
                private_data: ptr::null_mut(),
            };
            // SAFETY: these capsules borrow stack-owned shells for this test;
            // their no-op destructors never free those shells.
            let schema_capsule = unsafe {
                Bound::<PyAny>::from_owned_ptr(
                    py,
                    ffi::PyCapsule_New(
                        ptr::from_mut(&mut schema).cast(),
                        schema_capsule_name(),
                        None,
                    ),
                )
            };
            // SAFETY: see the schema capsule immediately above.
            let array_capsule = unsafe {
                Bound::<PyAny>::from_owned_ptr(
                    py,
                    ffi::PyCapsule_New(
                        ptr::from_mut(&mut array).cast(),
                        array_capsule_name(),
                        None,
                    ),
                )
            };
            // Design-1: admit moves+releases both shells; no ImportedCapsules.
            let (batch, _) =
                native_arrow_batch_from_capsules(&schema_capsule, &array_capsule).unwrap();
            assert!(schema.release.is_none());
            assert!(array.release.is_none());
            // Second admit on already-released capsules must fail.
            assert!(native_arrow_batch_from_capsules(&schema_capsule, &array_capsule).is_err());
            drop(batch);
        });
        assert_eq!(
            MOVED_SCHEMA_RELEASES.load(Ordering::Relaxed),
            schema_before + 1
        );
        assert_eq!(
            MOVED_ARRAY_RELEASES.load(Ordering::Relaxed),
            array_before + 1
        );
    }

    #[test]
    fn moved_shell_allocation_failure_keeps_source_release_slot() {
        ensure_python();
        let _guard = CAPSULE_RELEASE_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let array_before = MOVED_ARRAY_RELEASES.load(Ordering::Relaxed);
        let buffers = dummy_buffers(2);
        let mut array = stack_array(0, 0, 0, 2, 0, buffers.as_ptr(), ptr::null_mut());
        array.release = Some(count_moved_array_release);

        let result = crate::test_support::fail_allocations_after(0, || unsafe {
            MovedArrowShell::take(&raw mut array)
        });
        let Err(error) = result else {
            panic!("moved-shell allocation should have been rejected");
        };
        Python::attach(|py| assert!(error.is_instance_of::<PyMemoryError>(py)));
        assert!(
            array.release.is_some(),
            "failed move must retain producer ownership"
        );

        // The source shell is still live after the failed consumer allocation;
        // releasing it here proves the failure did not consume or duplicate it.
        unsafe {
            let release = array.release.expect("source release remains live");
            release(&raw mut array);
        }
        assert_eq!(
            MOVED_ARRAY_RELEASES.load(Ordering::Relaxed),
            array_before + 1
        );
    }

    #[test]
    fn foreign_snapshot_allocation_failure_is_memory_error_and_releases_once() {
        ensure_python();
        let _guard = CAPSULE_RELEASE_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let array_before = MOVED_ARRAY_RELEASES.load(Ordering::Relaxed);
        let values = [1.0_f64];
        let buffers: [*const c_void; 2] = [ptr::null(), values.as_ptr().cast()];
        let mut array = stack_array(1, 0, 0, 2, 0, buffers.as_ptr(), ptr::null_mut());
        array.release = Some(count_moved_array_release);

        let result = try_with_stack_layout(&mut array, ArrowFormat::Float64, 0, |layout| {
            crate::test_support::fail_allocations_after(0, || {
                snapshot_one_buffer_range(layout, 1, 0, std::mem::size_of::<f64>())
            })
        });
        let Err(error) = result else {
            panic!("foreign snapshot allocation should have been rejected");
        };
        Python::attach(|py| assert!(error.is_instance_of::<PyMemoryError>(py)));
        assert_eq!(
            MOVED_ARRAY_RELEASES.load(Ordering::Relaxed),
            array_before + 1,
            "the failed snapshot must still release its moved shell exactly once"
        );
    }

    #[test]
    fn admit_releases_array_shell_per_batch_with_owned_schema() {
        ensure_python();
        let _guard = CAPSULE_RELEASE_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let schema_before = MOVED_SCHEMA_RELEASES.load(Ordering::Relaxed);
        let array_before = MOVED_ARRAY_RELEASES.load(Ordering::Relaxed);
        Python::attach(|_py| {
            let mut schema = ArrowSchema {
                format: c"g".as_ptr(),
                name: ptr::null(),
                metadata: ptr::null(),
                flags: 0,
                n_children: 0,
                children: ptr::null_mut(),
                dictionary: ptr::null_mut(),
                release: Some(count_moved_schema_release),
                private_data: ptr::null_mut(),
            };
            // Capture schema once, release immediately (stream design-1).
            // SAFETY: stack-owned live schema.
            // SAFETY: stack-owned live schema moved into shell for capture.
            let shell = unsafe { MovedArrowShell::take(&raw mut schema) }.unwrap();
            let admitted = unsafe { AdmittedArrowSchema::capture(&shell) }.unwrap();
            // Release after owned schema snapshot.
            drop(shell);
            let buffers = [ptr::null(), ptr::null()];
            let mut first_array = ArrowArray {
                length: 0,
                null_count: 0,
                offset: 0,
                n_buffers: 2,
                n_children: 0,
                buffers: buffers.as_ptr(),
                children: ptr::null_mut(),
                dictionary: ptr::null_mut(),
                release: Some(count_moved_array_release),
                private_data: ptr::null_mut(),
            };
            let mut second_array = ArrowArray {
                length: 0,
                null_count: 0,
                offset: 0,
                n_buffers: 2,
                n_children: 0,
                buffers: buffers.as_ptr(),
                children: ptr::null_mut(),
                dictionary: ptr::null_mut(),
                release: Some(count_moved_array_release),
                private_data: ptr::null_mut(),
            };
            let classification = NativeArrowClassification {
                encoding: crate::py::geoarrow::GeometryEncoding::Point,
                wkb_offset_width: WkbOffsetWidth::Int32,
                crs: None,
                epoch: None,
            };
            let n1 = unsafe {
                native_arrow_batch_from_raw_array(
                    &raw mut first_array,
                    &admitted,
                    classification.clone(),
                    None,
                )
            }
            .unwrap();
            assert!(first_array.release.is_none());
            drop(n1);
            let n2 = unsafe {
                native_arrow_batch_from_raw_array(
                    &raw mut second_array,
                    &admitted,
                    classification,
                    None,
                )
            }
            .unwrap();
            assert!(second_array.release.is_none());
            drop(n2);
        });
        assert_eq!(
            MOVED_SCHEMA_RELEASES.load(Ordering::Relaxed),
            schema_before + 1
        );
        assert_eq!(
            MOVED_ARRAY_RELEASES.load(Ordering::Relaxed),
            array_before + 2
        );
    }

    #[test]
    fn struct_child_rejects_visible_end_beyond_raw_allocation() {
        ensure_python();
        let bufs = dummy_buffers(2);
        let mut child = stack_array(4, 0, 0, 2, 0, bufs.as_ptr(), ptr::null_mut());
        if child.release.is_none() {
            child.release = Some(test_stack_array_release);
        }

        let shell = unsafe { MovedArrowShell::take(&raw mut child) }.unwrap();

        let err = unsafe {
            CaptureLayout::struct_child(&shell, shell.as_ptr(), ArrowFormat::Float64, 2, 3, 0)
        };
        let message = match err {
            Ok(_) => panic!("expected struct child range to be rejected"),
            Err(error) => error.to_string(),
        };
        assert!(
            message.contains("exceeds child allocation") || message.contains("overflow"),
            "unexpected error: {message}"
        );
    }

    #[test]
    fn struct_child_null_count_validated_against_raw_not_projected() {
        ensure_python();
        // Parent offset=2, length=3; child raw length=10, null_count=6.
        // Old code rejected 6 > 3; fixed code accepts and exposes Unknown.
        let bufs = dummy_buffers(2);
        let mut child = stack_array(10, 0, 6, 2, 0, bufs.as_ptr(), ptr::null_mut());
        if child.release.is_none() {
            child.release = Some(test_stack_array_release);
        }

        let shell = unsafe { MovedArrowShell::take(&raw mut child) }.unwrap();

        let validated = unsafe {
            CaptureLayout::struct_child(&shell, shell.as_ptr(), ArrowFormat::Float64, 2, 3, 0)
        }
        .unwrap();
        assert_eq!(validated.length, 3);
        assert_eq!(validated.null_count, NullCount::Unknown);
        // Known(0) is preserved under projection.
        let mut child = stack_array(10, 0, 0, 2, 0, bufs.as_ptr(), ptr::null_mut());
        if child.release.is_none() {
            child.release = Some(test_stack_array_release);
        }

        let shell = unsafe { MovedArrowShell::take(&raw mut child) }.unwrap();

        let validated = unsafe {
            CaptureLayout::struct_child(&shell, shell.as_ptr(), ArrowFormat::Float64, 2, 3, 0)
        }
        .unwrap();
        assert_eq!(validated.null_count, NullCount::Known(0));
        // Full-view (parent covers raw range) preserves the known count.
        // Bitmap must match: 6 nulls of 10 rows (bits 0..5 clear, 6..9 set).
        let validity: [u8; 2] = [0b1100_0000, 0b0000_0011];
        let data = [0_u8; 80];
        let buffers: [*const c_void; 2] = [validity.as_ptr().cast(), data.as_ptr().cast()];
        let mut child = stack_array(10, 0, 6, 2, 0, buffers.as_ptr(), ptr::null_mut());
        if child.release.is_none() {
            child.release = Some(test_stack_array_release);
        }

        let shell = unsafe { MovedArrowShell::take(&raw mut child) }.unwrap();

        let validated = unsafe {
            CaptureLayout::struct_child(&shell, shell.as_ptr(), ArrowFormat::Float64, 0, 10, 0)
        }
        .unwrap();
        assert_eq!(validated.null_count, NullCount::Known(6));
    }

    #[test]
    fn struct_child_exposes_only_the_parent_slice() {
        ensure_python();
        // This fixture uses the same process-global release callbacks as the
        // exact-once lifecycle fixtures below. Keep their observed counters
        // exclusive under libtest's parallel scheduler.
        let _guard = CAPSULE_RELEASE_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Python::attach(|py| {
            let mut child_schema = ArrowSchema {
                format: c"g".as_ptr(),
                name: c"geometry".as_ptr(),
                metadata: ptr::null(),
                flags: 0,
                n_children: 0,
                children: ptr::null_mut(),
                dictionary: ptr::null_mut(),
                release: None,
                private_data: ptr::null_mut(),
            };
            let mut schema_children = [ptr::from_mut(&mut child_schema)];
            let schema = ArrowSchema {
                format: c"+s".as_ptr(),
                name: ptr::null(),
                metadata: ptr::null(),
                flags: 0,
                n_children: 1,
                children: schema_children.as_mut_ptr(),
                dictionary: ptr::null_mut(),
                release: Some(count_moved_schema_release),
                private_data: ptr::null_mut(),
            };
            let values = [0.0_f64, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];
            let child_buffers: [*const c_void; 2] = [ptr::null(), values.as_ptr().cast()];
            let mut child = stack_array(10, 1, 0, 2, 0, child_buffers.as_ptr(), ptr::null_mut());
            let mut array_children = [ptr::from_mut(&mut child)];
            let root_buffers = [ptr::null()];
            let mut array = stack_array(
                3,
                2,
                0,
                1,
                1,
                root_buffers.as_ptr(),
                array_children.as_mut_ptr(),
            );
            array.release = Some(count_moved_array_release);
            // SAFETY: array capsule borrows a stack-owned shell for this test.
            let array_capsule = unsafe {
                Bound::<PyAny>::from_owned_ptr(
                    py,
                    ffi::PyCapsule_New(
                        ptr::from_mut(&mut array).cast(),
                        array_capsule_name(),
                        None,
                    ),
                )
            };
            // Selected-span: re-root the geometry child (index 0) at admission.
            let mut schema_for_admit = schema;
            // SAFETY: stack schema moved into shell for owned capture.
            let shell = unsafe { MovedArrowShell::take(&raw mut schema_for_admit) }.unwrap();
            let admitted = unsafe { AdmittedArrowSchema::capture(&shell) }.unwrap();
            drop(shell);
            let array_ptr =
                capsule_pointer::<ArrowArray>(&array_capsule, array_capsule_name()).unwrap();
            // SAFETY: one-shot array capsule.
            let batch = unsafe {
                native_arrow_batch_from_raw_array(
                    array_ptr,
                    &admitted,
                    NativeArrowClassification {
                        encoding: crate::py::geoarrow::GeometryEncoding::Point,
                        wkb_offset_width: WkbOffsetWidth::Int32,
                        crs: None,
                        epoch: None,
                    },
                    Some(0),
                )
            }
            .unwrap();
            let output = batch.node;
            assert_eq!(output.length, 3);
            // Admission re-roots the selected child span: its three values
            // are [3, 4, 5], not a retained physical prefix plus offset 3.
            let OwnedBuffer::F64(values) = output.buffers.get(1).unwrap() else {
                panic!("expected f64 values");
            };
            assert_eq!(values.len(), 3);
            assert_eq!(values[0].to_bits(), 3.0_f64.to_bits());
            assert_eq!(values[2].to_bits(), 5.0_f64.to_bits());
        });
    }

    #[test]
    fn sliced_list_snapshot_rebases_offsets_and_projects_child() {
        // Capture owns only a list slice's bitmap, offsets, and child window.
        // This is intentionally below the Python decode layer: decoded geometry
        // cannot observe a temporary physical-parent copy. Restoring a full
        // parent snapshot makes the three exact byte/value assertions fail.
        ensure_python();
        let validity = [0_u8, 0b0000_0100, 0_u8];
        let offsets: [i32; 12] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 16];
        let values: [f64; 16] = std::array::from_fn(|index| index as f64);
        let child_buffers: [*const c_void; 2] = [ptr::null(), values.as_ptr().cast()];
        let mut child = stack_array(16, 0, 0, 2, 0, child_buffers.as_ptr(), ptr::null_mut());
        child.release = Some(test_stack_array_release);
        let mut child_ptrs = [ptr::from_mut(&mut child)];
        let parent_buffers: [*const c_void; 2] =
            [validity.as_ptr().cast(), offsets.as_ptr().cast()];
        // The producer's one visible parent row is physical row 10 and selects
        // child values [10, 16).  Its allocation deliberately has no public
        // length metadata beyond the Arrow offset window.
        let mut parent = stack_array(
            1,
            10,
            0,
            2,
            1,
            parent_buffers.as_ptr(),
            child_ptrs.as_mut_ptr(),
        );
        with_stack_layout(&mut parent, ArrowFormat::List, 1, |layout| {
            let (slots, _) = snapshot_native_buffers(layout).unwrap();
            let OwnedBuffer::Bytes(bitmap) = slots.get(0).unwrap() else {
                panic!("expected copied validity byte");
            };
            assert_eq!(bitmap.as_slice(), &validity[1..2]);
            let OwnedBuffer::Bytes(local_offsets) = slots.get(1).unwrap() else {
                panic!("expected copied list offsets");
            };
            assert_eq!(local_offsets.len(), 2 * std::mem::size_of::<i32>());
            let (offsets, remainder) = local_offsets.as_slice().as_chunks::<4>();
            assert!(remainder.is_empty());
            assert_eq!(
                offsets
                    .iter()
                    .map(|bytes| i32::from_le_bytes(*bytes))
                    .collect::<Vec<_>>(),
                vec![0, 6]
            );
            assert_eq!(layout.child_window, Some((10, 6)));

            let child_ptr = unsafe { layout.child_ptr(0).unwrap() };
            let mut child_layout = unsafe {
                CaptureLayout::struct_child(layout.shell, child_ptr, ArrowFormat::Float64, 10, 6, 0)
            }
            .unwrap();
            let (child_slots, _) = snapshot_native_buffers(&mut child_layout).unwrap();
            let OwnedBuffer::F64(child_values) = child_slots.get(1).unwrap() else {
                panic!("expected owned f64 child window");
            };
            assert_eq!(child_values.as_slice(), &values[10..16]);
        });
    }

    #[test]
    fn binary_data_len_reads_terminal_offset_at_matching_width() {
        ensure_python();
        let offsets: [i32; 3] = [0, 3, 7];
        // Minimal non-null data buffer so snapshot of index 2 is non-empty span.
        let data = [0_u8; 7];
        let buffers: [*const c_void; 3] =
            [ptr::null(), offsets.as_ptr().cast(), data.as_ptr().cast()];
        let mut array = stack_array(2, 0, 0, 3, 0, buffers.as_ptr(), ptr::null_mut());
        with_stack_layout(&mut array, ArrowFormat::Binary, 0, |validated| {
            let (slots, _) = snapshot_native_buffers(validated).unwrap();
            let OwnedBuffer::Bytes(data_slot) = slots.get(2).unwrap() else {
                panic!("expected bytes data buffer");
            };
            assert_eq!(data_slot.len(), 7);
        });

        let large_offsets: [i64; 3] = [0, 10, 25];
        let large_data = [0_u8; 25];
        let large_buffers: [*const c_void; 3] = [
            ptr::null(),
            large_offsets.as_ptr().cast(),
            large_data.as_ptr().cast(),
        ];
        let mut large = stack_array(2, 0, 0, 3, 0, large_buffers.as_ptr(), ptr::null_mut());
        with_stack_layout(&mut large, ArrowFormat::LargeBinary, 0, |validated| {
            let (slots, _) = snapshot_native_buffers(validated).unwrap();
            let OwnedBuffer::Bytes(data_slot) = slots.get(2).unwrap() else {
                panic!("expected bytes data buffer");
            };
            assert_eq!(data_slot.len(), 25);
        });
    }

    #[test]
    fn binary_data_len_rejects_negative_terminal_offset() {
        ensure_python();
        let offsets: [i32; 2] = [0, -1];
        let buffers: [*const c_void; 3] = [ptr::null(), offsets.as_ptr().cast(), ptr::null()];
        let mut array = stack_array(1, 0, 0, 3, 0, buffers.as_ptr(), ptr::null_mut());
        with_stack_layout(&mut array, ArrowFormat::Binary, 0, |validated| {
            assert!(snapshot_native_buffers(validated).is_err());
        });
    }

    #[test]
    fn checked_byte_span_rejects_overflow_and_isize_max() {
        ensure_python();
        assert!(checked_byte_span(usize::MAX, 2, "overflow").is_err());
        assert!(checked_byte_span((usize::MAX / 8) + 1, 8, "overflow").is_err());
        assert!(checked_byte_span(usize::MAX / 4 + 1, 4, "overflow").is_err());
        assert_eq!(checked_byte_span(4, 8, "overflow").unwrap(), 32);
        // Defect: format `g`, length 2^60 → 2^63 bytes fits usize but exceeds isize::MAX.
        let huge = 1_usize << 60;
        assert!(checked_byte_span(huge, 8, "overflow").is_err());
    }

    #[test]
    fn binary_view_rejects_out_of_range_buffer_index() {
        ensure_python();
        // One non-inline view: length=13, buffer_index=5, but only validity,
        // views, and the mandatory sizes table (no variadic data) → reject.
        let mut view = [0_u8; 16];
        view[0..4].copy_from_slice(&13_i32.to_le_bytes());
        view[8..12].copy_from_slice(&5_i32.to_le_bytes());
        let buffers: [*const c_void; 3] = [ptr::null(), view.as_ptr().cast(), ptr::null()];
        let mut array = stack_array(1, 0, 0, 3, 0, buffers.as_ptr(), ptr::null_mut());
        assert!(layout_with_owned_content(&mut array, ArrowFormat::BinaryView, 0).is_err());
    }

    #[test]
    fn zero_length_skips_binary_view_descriptor_scan() {
        ensure_python();
        let bufs = dummy_buffers(3);
        let mut array = stack_array(0, 0, -1, 3, 0, bufs.as_ptr(), ptr::null_mut());
        with_owned_content_layout(&mut array, ArrowFormat::BinaryView, 0, |validated| {
            assert_eq!(validated.length, 0);
            assert_eq!(validated.null_count, NullCount::Unknown);
            let ranges = validated.binary_view_data_ranges.as_ref().unwrap();
            assert!(ranges.is_empty(), "sparse empty map, not dense n_buffers-3");
        })
        .unwrap();
    }

    #[test]
    fn binary_view_huge_n_buffers_length_zero_is_sparse_not_oom() {
        ensure_python();
        // Release blocker: format "vz", length=0, n_buffers huge must not
        // allocate `vec![0; n_buffers-3]` (capacity overflow / multi-GB).
        let n_buffers = 1_000_000;
        let sizes = vec![0_i64; n_buffers - 3];
        let mut bufs = dummy_buffers(n_buffers);
        bufs[n_buffers - 1] = sizes.as_ptr().cast();
        // The mandatory sizes table has one entry per variadic buffer, but the
        // selected-range cache remains sparse instead of mirroring that table.
        let mut array = stack_array(0, 0, 0, n_buffers as i64, 0, bufs.as_ptr(), ptr::null_mut());
        with_owned_content_layout(&mut array, ArrowFormat::BinaryView, 0, |validated| {
            let ranges = validated.binary_view_data_ranges.as_ref().unwrap();
            assert!(ranges.is_empty());
        })
        .unwrap();
    }

    #[test]
    fn binary_view_selected_fragment_does_not_retain_unreferenced_size_entries() {
        ensure_python();
        // One high-index payload has a 5,000-entry producer size table. The
        // imported node owns only that descriptor's 8-byte declaration and
        // selected payload, not all 40 KiB of unrelated advisory metadata.
        let data_index = 4_999_usize;
        let n_buffers = data_index + 4;
        let mut view = [0_u8; 16];
        view[0..4].copy_from_slice(&20_i32.to_le_bytes());
        view[4..8].copy_from_slice(&[7, 7, 7, 7]);
        view[8..12].copy_from_slice(&(data_index as i32).to_le_bytes());
        let data = [7_u8; 20];
        let mut sizes = vec![0_i64; n_buffers - 3];
        sizes[data_index] = data.len() as i64;
        let mut buffers = dummy_buffers(n_buffers);
        buffers[1] = view.as_ptr().cast();
        buffers[data_index + 2] = data.as_ptr().cast();
        buffers[n_buffers - 1] = sizes.as_ptr().cast();
        let mut array = stack_array(
            1,
            0,
            0,
            n_buffers as i64,
            0,
            buffers.as_ptr(),
            ptr::null_mut(),
        );
        try_with_stack_layout(&mut array, ArrowFormat::BinaryView, 0, |layout| {
            let (owned, _) = snapshot_native_buffers(layout)?;
            assert!(matches!(
                owned.get(data_index + 2),
                Some(OwnedBuffer::Bytes(bytes)) if bytes.as_slice() == &data[..]
            ));
            assert!(owned.get(2).is_none(), "unreferenced low slot was retained");
            assert!(
                owned.get(data_index + 3).is_none(),
                "the high-index sizes slot was exposed as payload"
            );
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn binary_view_sparse_high_buffer_index_rejects_not_dense_grow() {
        ensure_python();
        // Present non-inline: buffer_index=5 with no variadic data buffers
        // (only validity, views, and sizes) → reject. No dense growth to 5.
        let mut view = [0_u8; 16];
        view[0..4].copy_from_slice(&20_i32.to_le_bytes());
        view[8..12].copy_from_slice(&5_i32.to_le_bytes());
        view[12..16].copy_from_slice(&0_i32.to_le_bytes());
        let buffers: [*const c_void; 3] = [ptr::null(), view.as_ptr().cast(), ptr::null()];
        let mut array = stack_array(1, 0, 0, 3, 0, buffers.as_ptr(), ptr::null_mut());
        assert!(layout_with_owned_content(&mut array, ArrowFormat::BinaryView, 0).is_err());
    }

    #[test]
    fn binary_view_multi_buffer_endpoints_are_sparse_and_correct() {
        ensure_python();
        // Two non-inline rows on distinct data buffers: ends keyed sparsely.
        let mut views = [0_u8; 32];
        // Row 0: length=20, buffer_index=0, byte_offset=0 → end=20
        views[0..4].copy_from_slice(&20_i32.to_le_bytes());
        views[8..12].copy_from_slice(&0_i32.to_le_bytes());
        views[12..16].copy_from_slice(&0_i32.to_le_bytes());
        // Row 1: length=15, buffer_index=2, byte_offset=4 → end=19
        views[16..20].copy_from_slice(&15_i32.to_le_bytes());
        views[24..28].copy_from_slice(&2_i32.to_le_bytes());
        views[28..32].copy_from_slice(&4_i32.to_le_bytes());
        let data0 = [0_u8; 32];
        let data1 = [0_u8; 1]; // unused index 1
        let data2 = [0_u8; 32];
        let sizes = [32_i64, 1, 32];
        let buffers: [*const c_void; 6] = [
            ptr::null(),
            views.as_ptr().cast(),
            data0.as_ptr().cast(),
            data1.as_ptr().cast(),
            data2.as_ptr().cast(),
            sizes.as_ptr().cast(),
        ];
        let mut array = stack_array(2, 0, 0, 6, 0, buffers.as_ptr(), ptr::null_mut());
        with_owned_content_layout(&mut array, ArrowFormat::BinaryView, 0, |validated| {
            let ranges = validated.binary_view_data_ranges.as_ref().unwrap();
            assert_eq!(ranges.len(), 2);
            assert_eq!(ranges.get(&0), Some(&(0..20)));
            assert_eq!(ranges.get(&2), Some(&(4..19)));
            assert!(!ranges.contains_key(&1));
        })
        .unwrap();
    }

    #[test]
    fn binary_view_null_row_descriptor_is_ignored_not_oob() {
        ensure_python();
        // Conforming null row: validity bit0=0, ignored descriptor claims a
        // 1_000_000-byte span over a 1-byte data buffer. Pre-fix code treated
        // the descriptor as live and OOB-read the allocation.
        let mut view = [0_u8; 16];
        view[0..4].copy_from_slice(&1_000_000_i32.to_le_bytes());
        view[8..12].copy_from_slice(&0_i32.to_le_bytes());
        view[12..16].copy_from_slice(&0_i32.to_le_bytes());
        let validity: [u8; 1] = [0]; // bit0 = null
        let data: [u8; 1] = [0xAB];
        let sizes = [1_i64];
        let buffers: [*const c_void; 4] = [
            validity.as_ptr().cast(),
            view.as_ptr().cast(),
            data.as_ptr().cast(),
            sizes.as_ptr().cast(),
        ];
        let mut array = stack_array(1, 0, 1, 4, 0, buffers.as_ptr(), ptr::null_mut());
        with_owned_content_layout(&mut array, ArrowFormat::BinaryView, 0, |validated| {
            let ranges = validated.binary_view_data_ranges.as_ref().unwrap();
            // Sparse: null-only arrays leave the range map empty (absent ≡ empty).
            assert!(ranges.is_empty());
        })
        .unwrap();
    }

    #[test]
    fn binary_view_present_descriptor_bounds_and_caches_selected_range() {
        ensure_python();
        // Present non-inline: length=20, buffer_index=0, byte_offset=2 → end=22.
        let mut view = [0_u8; 16];
        view[0..4].copy_from_slice(&20_i32.to_le_bytes());
        view[4..8].copy_from_slice(&[0, 0, 0, 0]);
        view[8..12].copy_from_slice(&0_i32.to_le_bytes());
        view[12..16].copy_from_slice(&2_i32.to_le_bytes());
        let data = [0_u8; 32];
        let sizes = [32_i64];
        let buffers: [*const c_void; 4] = [
            ptr::null(),
            view.as_ptr().cast(),
            data.as_ptr().cast(),
            sizes.as_ptr().cast(),
        ];
        let mut array = stack_array(1, 0, 0, 4, 0, buffers.as_ptr(), ptr::null_mut());
        try_with_stack_layout(&mut array, ArrowFormat::BinaryView, 0, |layout| {
            let (owned, ranges) = snapshot_native_buffers(layout)?;
            assert_eq!(ranges.as_ref().unwrap().get(&0), Some(&(2..22)));
            let Some(OwnedBuffer::Bytes(payload)) = owned.get(2) else {
                panic!("selected payload slot was not retained");
            };
            assert_eq!(payload.as_slice(), &data[2..22]);
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn binary_view_rebases_nonzero_selected_payload_origin() {
        ensure_python();
        // One selected non-inline descriptor starts at physical byte 2. Native
        // admission must copy only [2, 22) and rewrite the owned descriptor
        // to offset 0; otherwise downstream BinaryView decode indexes the
        // rebased payload with a stale physical offset. This is deliberately a
        // mutation guard for `rebase_owned_binary_view_offsets`.
        let mut view = [0_u8; 16];
        view[0..4].copy_from_slice(&20_i32.to_le_bytes());
        view[4..8].copy_from_slice(&[2, 3, 4, 5]);
        view[8..12].copy_from_slice(&0_i32.to_le_bytes());
        view[12..16].copy_from_slice(&2_i32.to_le_bytes());
        let data: [u8; 32] = std::array::from_fn(|index| index as u8);
        let sizes = [32_i64];
        let buffers: [*const c_void; 4] = [
            ptr::null(),
            view.as_ptr().cast(),
            data.as_ptr().cast(),
            sizes.as_ptr().cast(),
        ];
        let mut array = stack_array(1, 0, 0, 4, 0, buffers.as_ptr(), ptr::null_mut());
        try_with_stack_layout(&mut array, ArrowFormat::BinaryView, 0, |layout| {
            let (owned, _) = snapshot_native_buffers(layout)?;
            let view = owned
                .get(1)
                .and_then(|buffer| match buffer {
                    OwnedBuffer::Bytes(bytes) => Some(bytes.as_slice()),
                    OwnedBuffer::F64(_) => None,
                })
                .expect("owned BinaryView descriptors");
            let offset = i32::from_le_bytes(view[12..16].try_into().expect("view offset"));
            assert_eq!(offset, 0, "selected payload origin must be rebased");
            let payload = owned
                .get(2)
                .and_then(|buffer| match buffer {
                    OwnedBuffer::Bytes(bytes) => Some(bytes.as_slice()),
                    OwnedBuffer::F64(_) => None,
                })
                .expect("owned BinaryView payload");
            assert_eq!(payload, &data[2..22]);
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn binary_view_rejects_span_beyond_declared_variadic_buffer_size() {
        ensure_python();
        // The backing allocation is 21 bytes, but the mandatory sizes table
        // declares only one byte. Old code read the external prefix anyway.
        let mut view = [0_u8; 16];
        view[0..4].copy_from_slice(&13_i32.to_le_bytes());
        view[4..8].copy_from_slice(&[0, 0, 0, 0]);
        view[8..12].copy_from_slice(&0_i32.to_le_bytes());
        view[12..16].copy_from_slice(&0_i32.to_le_bytes());
        let data = [0_u8; 21];
        let sizes = [1_i64];
        let buffers: [*const c_void; 4] = [
            ptr::null(),
            view.as_ptr().cast(),
            data.as_ptr().cast(),
            sizes.as_ptr().cast(),
        ];
        let mut array = stack_array(1, 0, 0, 4, 0, buffers.as_ptr(), ptr::null_mut());
        assert!(layout_with_owned_content(&mut array, ArrowFormat::BinaryView, 0).is_err());
    }

    #[test]
    fn owned_batch_decoder_runs_detached() {
        ensure_python();
        let leaf = |value: f64| NativeNode {
            length: 1,
            validity_offset: 0,
            n_buffers: 2,
            format: ArrowFormat::Float64,
            buffers: OwnedBufferSlots::Dense(Arc::new(vec![
                Some(OwnedBuffer::Bytes(Arc::new(Vec::new()))),
                Some(OwnedBuffer::F64(Arc::new(vec![value]))),
            ])),
            children: Arc::new(Vec::new()),
            schema_name: Arc::new(String::new()),
            field_names: Arc::new(Vec::new()),
            effective_missing: None,
        };
        let node = NativeNode {
            length: 1,
            validity_offset: 0,
            n_buffers: 1,
            format: ArrowFormat::Struct,
            buffers: OwnedBufferSlots::Dense(Arc::new(vec![Some(OwnedBuffer::Bytes(Arc::new(
                Vec::new(),
            )))])),
            children: Arc::new(vec![leaf(1.0), leaf(2.0)]),
            schema_name: Arc::new(String::new()),
            field_names: Arc::new(vec![String::from("x"), String::from("y")]),
            effective_missing: None,
        };
        let batch = NativeArrowBatch {
            node,
            classification: NativeArrowClassification {
                encoding: crate::py::geoarrow::GeometryEncoding::Point,
                wkb_offset_width: WkbOffsetWidth::Int32,
                crs: None,
                epoch: None,
            },
        };
        Python::attach(|py| {
            let attached = py.detach(move || {
                // PyO3 0.29 has no public `Python::is_attached`; the C API
                // check is the same observation without attaching again.
                let detached = unsafe { ffi::PyGILState_Check() != 0 };
                let decoded = batch.decode();
                (detached, decoded)
            });
            let (detached, decoded) = attached;
            let decoded = decoded.expect("owned batch decoder");
            assert!(!detached);
            assert_eq!(decoded.length, 1);
            assert_eq!(decoded.storage.len(), 1);
            assert!(matches!(
                decoded.storage,
                NativeDecodedStorage::Packed(crate::array::GeometryArrayStorage::Packed(
                    crate::array::PackedStorage::Points { .. }
                ))
            ));
        });
    }

    #[test]
    fn native_c_struct_decode_publishes_packed_points_without_python_import() {
        ensure_python();
        let mut x_schema = ArrowSchema {
            format: c"g".as_ptr(),
            name: c"x".as_ptr(),
            metadata: ptr::null(),
            flags: 0,
            n_children: 0,
            children: ptr::null_mut(),
            dictionary: ptr::null_mut(),
            release: None,
            private_data: ptr::null_mut(),
        };
        let mut y_schema = ArrowSchema {
            format: c"g".as_ptr(),
            name: c"y".as_ptr(),
            metadata: ptr::null(),
            flags: 0,
            n_children: 0,
            children: ptr::null_mut(),
            dictionary: ptr::null_mut(),
            release: None,
            private_data: ptr::null_mut(),
        };
        let mut schema_children = [ptr::from_mut(&mut x_schema), ptr::from_mut(&mut y_schema)];
        let mut schema = ArrowSchema {
            format: c"+s".as_ptr(),
            name: c"".as_ptr(),
            metadata: ptr::null(),
            flags: 0,
            n_children: 2,
            children: schema_children.as_mut_ptr(),
            dictionary: ptr::null_mut(),
            release: Some(stack_schema_release),
            private_data: ptr::null_mut(),
        };
        // SAFETY: this test owns the stack schema and keeps its child tree live
        // until the admitted owned schema has been captured.
        let schema_shell = unsafe { MovedArrowShell::take(&raw mut schema) }.unwrap();
        let admitted = unsafe { AdmittedArrowSchema::capture_geometry(&schema_shell) }.unwrap();
        drop(schema_shell);

        let x_values = [1.0_f64, 3.0];
        let y_values = [2.0_f64, 4.0];
        let x_buffers: [*const c_void; 2] = [ptr::null(), x_values.as_ptr().cast()];
        let y_buffers: [*const c_void; 2] = [ptr::null(), y_values.as_ptr().cast()];
        let mut x_array = stack_array(2, 0, 0, 2, 0, x_buffers.as_ptr(), ptr::null_mut());
        let mut y_array = stack_array(2, 0, 0, 2, 0, y_buffers.as_ptr(), ptr::null_mut());
        let mut array_children = [ptr::from_mut(&mut x_array), ptr::from_mut(&mut y_array)];
        let root_buffers = [ptr::null()];
        let mut array = stack_array(
            2,
            0,
            0,
            1,
            2,
            root_buffers.as_ptr(),
            array_children.as_mut_ptr(),
        );
        array.release = Some(test_stack_array_release);
        // SAFETY: the stack array and all children remain live through owned
        // admission; only the copied NativeNode enters the detached decoder.
        let batch = unsafe {
            native_arrow_batch_from_raw_array(
                &raw mut array,
                &admitted,
                NativeArrowClassification {
                    encoding: crate::py::geoarrow::GeometryEncoding::Point,
                    wkb_offset_width: WkbOffsetWidth::Int32,
                    crs: None,
                    epoch: None,
                },
                None,
            )
        }
        .unwrap();
        assert!(array.release.is_none());

        Python::attach(|py| {
            let (decoded, allocations) =
                py.detach(move || crate::test_support::allocation_count(|| batch.decode()));
            let decoded = decoded.expect("native C struct point decode");
            assert_eq!(
                allocations, 1,
                "only the final packed storage Arc allocates"
            );
            assert!(matches!(
                decoded.storage,
                NativeDecodedStorage::Packed(crate::array::GeometryArrayStorage::Packed(
                    crate::array::PackedStorage::Points { .. }
                ))
            ));
        });
    }
}
