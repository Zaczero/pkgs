//! Native Arrow C Data Interface import — PyArrow-shaped views over imported
//! capsules so the existing GeoArrow decode lane runs without ``pyarrow``.
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
use pyo3::prelude::{Bound, Py, PyAny, PyResult, Python};
#[cfg(test)]
use pyo3::types::PyBytesMethods as _;
use pyo3::types::{PyBytes, PyTuple};
use pyo3::{IntoPyObjectExt as _, pyclass, pymethods};

use crate::HeapSize;
use crate::collections::{HashMap, HashMapExt as _};
use crate::py::arrow::{
    ArrowStorage, arrow_storage_from_native_geometry, geometries_from_arrow_storages,
};
use crate::py::arrow_c::admitted::{
    AdmittedArrowSchema, MovedArrowShell, classify_admitted_geometry_schema,
};
use crate::py::arrow_c::foreign_buffer::ForeignArrowBuffer;
use crate::py::arrow_c::{
    ArrowArray, ArrowReleaseSlot, ArrowSchema, ClassifiedGeometrySchema, GeometryEncoding,
    IntoPyObject as _, PyAnyMethods as _, PyModule, PyModuleMethods as _, array_capsule_name,
    capsule_pointer, schema_capsule_name,
};

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

    /// Python/Arrow view: `-1` unknown, else the known non-negative count.
    const fn as_i64(self) -> i64 {
        match self {
            Self::Unknown => -1,
            Self::Known(n) => n as i64,
        }
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
pub(crate) unsafe fn move_arrow_shell<T: ArrowReleaseSlot>(source: *mut T) -> *mut T {
    // SAFETY: the caller guarantees a live base structure. A bytewise copy is
    // the Arrow-specified move operation; nulling source release leaves its
    // capsule-owned shell inert.
    unsafe {
        let moved = Box::into_raw(Box::new(ptr::read(source)));
        *T::release_slot(source) = None;
        moved
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

/// Design-1 direct-capsule admission: move schema → owned snapshot → release
/// schema → classify → move array → selected capture → release array → owned node.
///
/// No `ImportedCapsules` / `Arrow*Ptr` survives this function.
fn admit_geometry_from_capsules(
    schema_capsule: &Bound<'_, PyAny>,
    array_capsule: &Bound<'_, PyAny>,
) -> PyResult<(NativeNode, ClassifiedGeometrySchema)> {
    let schema = capsule_pointer::<ArrowSchema>(schema_capsule, schema_capsule_name())?;
    let array = capsule_pointer::<ArrowArray>(array_capsule, array_capsule_name())?;
    // SAFETY: capsules resolved to live named pointers with release still set.
    let schema_shell = unsafe { MovedArrowShell::take(schema) }?;
    // SAFETY: shell pins producer schema for capture only.
    let admitted_schema = unsafe { AdmittedArrowSchema::capture(&schema_shell) }?;
    drop(schema_shell); // release producer schema before array work
    let classified = classify_admitted_geometry_schema(&admitted_schema)?;
    // SAFETY: array capsule still live; move one-shot.
    let array_shell = unsafe { MovedArrowShell::take(array) }?;
    // SAFETY: shell pins the array; producer quiescent for selected-span
    // snapshot; only owned NativeNode escapes.
    let node =
        unsafe { capture_selected_node(&array_shell, &admitted_schema, classified.struct_child) }?;
    drop(array_shell); // release producer array before decode
    Ok((node, classified))
}

/// Design-1 stream-batch admission: array only, formats from owned schema.
///
/// # Safety
///
/// `array` must be a live producer batch base structure (release non-null).
/// On return the producer array has been moved and released.
unsafe fn admit_array_with_admitted_schema(
    array: *mut ArrowArray,
    admitted_schema: &AdmittedArrowSchema,
    select_struct_child: Option<usize>,
) -> PyResult<NativeNode> {
    // SAFETY: caller guarantees live batch array.
    let array_shell = unsafe { MovedArrowShell::take(array) }?;
    // SAFETY: shell pins the batch array; producer quiescent for selected-span
    // snapshot; only owned NativeNode escapes.
    let node =
        unsafe { capture_selected_node(&array_shell, admitted_schema, select_struct_child) }?;
    drop(array_shell);
    Ok(node)
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
    let ext_name = schema
        .extension_name()?
        .map(|s| Arc::<str>::from(s.as_str()));
    let ext_meta = Arc::from(schema.extension_metadata_bytes().as_slice());
    build_owned_node_admitted(
        &mut layout,
        schema,
        Arc::clone(&schema.format),
        ext_name,
        ext_meta,
        None,
    )
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
    let child_ext_name = child_schema
        .extension_name()?
        .map(|s| Arc::<str>::from(s.as_str()));
    let child_ext_meta = Arc::from(child_schema.extension_metadata_bytes().as_slice());
    build_owned_node_admitted(
        &mut child_layout,
        child_schema,
        Arc::clone(&child_schema.format),
        child_ext_name,
        child_ext_meta,
        parent_missing,
    )
}

/// Owned native array node: buffers + children snapshotted at admission.
/// No producer shells, no raw buffer pointers after construction.
#[derive(Clone)]
struct NativeNode {
    length: usize,
    /// Every owned payload is rebased to its visible logical origin.
    offset: usize,
    /// The physical bit alignment retained with the copied validity bytes.
    validity_offset: usize,
    null_count: NullCount,
    n_buffers: usize,
    format: ArrowFormat,
    buffers: OwnedBufferSlots,
    children: Arc<[NativeNode]>,
    /// Schema format string for this node.
    schema_format: Arc<str>,
    /// This node's own schema field name (used by fixed-size-list dimensions).
    schema_name: Arc<str>,
    /// Field names for struct children (empty otherwise).
    field_names: Arc<[String]>,
    /// Root extension metadata (shared by identity across tree).
    extension_name: Option<Arc<str>>,
    extension_metadata: Arc<[u8]>,
    /// Combined ancestor + self missing mask (`true` = null).
    effective_missing: Option<Arc<[bool]>>,
}

impl NativeNode {
    #[expect(
        clippy::same_name_method,
        reason = "the inherent operation deliberately shares the domain vocabulary of its trait contract"
    )]
    fn heap_bytes(&self) -> usize {
        HeapSize::heap_bytes(self)
    }

    fn child(&self, index: usize) -> PyResult<Self> {
        self.children.get(index).cloned().ok_or_else(|| {
            crate::py::errors::parse_error(
                format!(
                    "Arrow child index {index} is out of range for n_children={}",
                    self.children.len()
                ),
                crate::error::ParseFormat::GeoArrow,
            )
        })
    }

    fn struct_field(&self, index: usize) -> PyResult<Self> {
        self.child(index)
    }

    const fn effective_offset(&self) -> usize {
        self.offset
    }

    fn format(&self) -> &str {
        self.schema_format.as_ref()
    }

    fn field_index(&self, name: &str) -> PyResult<usize> {
        self.field_names
            .iter()
            .position(|n| n == name)
            .ok_or_else(|| {
                PyTypeError::new_err(format!("Arrow struct has no field named '{name}'"))
            })
    }

    fn into_py_any(self, py: Python<'_>, is_extension_root: bool) -> PyResult<Py<PyAny>> {
        Ok(NativeArrowArray::from_node(self, is_extension_root)
            .into_pyobject(py)?
            .into_any()
            .unbind())
    }
}

/// Recursively snapshot one array node and its children while the array shell
/// is live; formats come from the already-owned schema tree.
fn build_owned_node_admitted(
    validated: &mut CaptureLayout,
    schema: &AdmittedArrowSchema,
    schema_format: Arc<str>,
    extension_name: Option<Arc<str>>,
    extension_metadata: Arc<[u8]>,
    effective_missing: Option<Arc<[bool]>>,
) -> PyResult<NativeNode> {
    let (buffers, _) = snapshot_native_buffers(validated)?;
    // Own-row nulls from owned validity, OR'd with any ancestor mask.
    let effective_missing = merge_owned_row_missing(validated, &buffers, effective_missing)?;
    let mut field_names = Vec::new();
    let mut children = Vec::new();
    if validated.n_children > 0 {
        if schema.children.len() != validated.n_children {
            return Err(crate::py::errors::parse_error(
                "Arrow child schema index out of range",
                crate::error::ParseFormat::GeoArrow,
            ));
        }
        field_names.try_reserve(validated.n_children).map_err(|_| {
            PyTypeError::new_err("Arrow schema child count is too large to allocate")
        })?;
        children.try_reserve(validated.n_children).map_err(|_| {
            PyTypeError::new_err("Arrow array child count is too large to allocate")
        })?;
        for index in 0..validated.n_children {
            let child_schema = &schema.children[index];
            field_names.push(child_schema.name().to_owned());
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
                Arc::clone(&child_schema.format),
                extension_name.clone(),
                Arc::clone(&extension_metadata),
                child_missing,
            )?);
        }
    }
    Ok(NativeNode {
        length: validated.length,
        offset: 0,
        validity_offset: validated.offset % 8,
        null_count: validated.null_count,
        n_buffers: validated.n_buffers,
        format: validated.format,
        buffers,
        children: Arc::from(children),
        schema_format,
        schema_name: Arc::from(schema.name()),
        field_names: Arc::from(field_names),
        extension_name,
        extension_metadata,
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
fn project_ancestor_missing(
    parent: &CaptureLayout,
    parent_buffers: &OwnedBufferSlots,
    parent_effective: Option<&Arc<[bool]>>,
    child_length: usize,
) -> PyResult<Option<Arc<[bool]>>> {
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
            let mut missing = vec![false; child_length];
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
            Ok(missing
                .iter()
                .any(|&is_missing| is_missing)
                .then(|| Arc::from(missing)))
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
            let mut missing = vec![false; child_length];
            for (row, &is_missing) in parent_missing.iter().enumerate() {
                if !is_missing {
                    continue;
                }
                let start = row.checked_mul(width).ok_or_else(|| {
                    PyTypeError::new_err("Arrow fixed-size-list child offset overflows")
                })?;
                missing[start..start + width].fill(true);
            }
            Ok(Some(Arc::from(missing)))
        },
        ArrowFormat::Binary
        | ArrowFormat::LargeBinary
        | ArrowFormat::BinaryView
        | ArrowFormat::Float64 => Ok(None),
    }
}

/// OR this node's owned validity nulls into an ancestor missing mask.
/// Runs only after [`snapshot_native_buffers`] so content is owned.
fn merge_owned_row_missing(
    array: &CaptureLayout,
    buffers: &OwnedBufferSlots,
    ancestor: Option<Arc<[bool]>>,
) -> PyResult<Option<Arc<[bool]>>> {
    let len = array.length;
    let mut missing = match ancestor {
        Some(a) => {
            if a.len() != len {
                return Err(PyTypeError::new_err(
                    "Arrow ancestor missing length does not match child length",
                ));
            }
            a.to_vec()
        },
        None => vec![false; len],
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
        Ok(Some(Arc::from(missing)))
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

#[pyclass(
    name = "_NativeArrowType",
    module = "gometry._lib",
    immutable_type,
    frozen,
    skip_from_py_object
)]
#[derive(Clone)]
struct NativeArrowType {
    node: NativeNode,
    extension_name: Option<String>,
    extension_metadata: Vec<u8>,
    names: Vec<String>,
    name: String,
}

#[pymethods]
impl NativeArrowType {
    #[getter]
    fn extension_name(&self) -> Option<&str> {
        self.extension_name.as_deref()
    }

    #[getter]
    fn names(&self) -> Vec<String> {
        self.names.clone()
    }

    #[getter]
    fn format(&self) -> String {
        self.node.format().to_owned()
    }

    #[getter]
    fn name(&self) -> &str {
        &self.name
    }

    #[getter]
    fn list_size(&self) -> PyResult<usize> {
        match self.node.format {
            ArrowFormat::FixedSizeList(size) => Ok(size),
            _ => Err(PyTypeError::new_err("Arrow type is not a fixed-size list")),
        }
    }

    #[getter]
    fn value_field(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        NativeArrowType::from_node(&self.node.child(0)?)?.into_py_any(py)
    }

    fn __arrow_ext_serialize__(&self) -> Vec<u8> {
        self.extension_metadata.clone()
    }

    fn __sizeof__(&self) -> usize {
        self.total_size()
    }
}

#[pyclass(
    name = "_NativeArrowArray",
    module = "gometry._lib",
    immutable_type,
    frozen,
    skip_from_py_object
)]
#[derive(Clone)]
struct NativeArrowArray {
    node: NativeNode,
    /// When true, ``storage`` returns ``self`` (GeoArrow extension top-level).
    is_extension_root: bool,
}

#[pymethods]
impl NativeArrowArray {
    const fn __len__(&self) -> usize {
        self.node.length
    }

    fn __sizeof__(&self) -> usize {
        self.total_size()
    }

    #[getter]
    const fn offset(&self) -> usize {
        self.node.effective_offset()
    }

    #[getter]
    fn null_count(&self) -> i64 {
        self.node.effective_missing.as_ref().map_or_else(
            || self.node.null_count.as_i64(),
            |missing| missing.iter().filter(|&&is_missing| is_missing).count() as i64,
        )
    }

    #[getter]
    #[pyo3(name = "type")]
    fn arrow_type(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        NativeArrowType::from_node(&self.node)?.into_py_any(py)
    }

    #[getter]
    fn storage(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        if self.is_extension_root {
            Ok(self.clone().into_pyobject(py)?.into_any().unbind())
        } else {
            Err(PyTypeError::new_err(
                "native Arrow array is not an extension array",
            ))
        }
    }

    #[getter]
    fn values(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.node.child(0)?.into_py_any(py, false)
    }

    fn field(&self, py: Python<'_>, name: &str) -> PyResult<Py<PyAny>> {
        let index = self.node.field_index(name)?;
        self.node.struct_field(index)?.into_py_any(py, false)
    }

    /// Single buffer by index from owned admission storage only.
    fn buffer(&self, py: Python<'_>, index: usize) -> PyResult<Py<PyAny>> {
        if index == 0
            && let Some(missing) = self.node.effective_missing.as_ref()
        {
            let bitmap = crate::py::arrow::validity_bitmap_from_missing(missing);
            return PyBytes::new(py, &bitmap).into_py_any(py);
        }
        native_buffer_at(py, &self.node, index)
    }

    fn buffers(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let count = self.node.n_buffers;
        let mut items = Vec::new();
        items
            .try_reserve(count)
            .map_err(|_| PyTypeError::new_err("Arrow buffer count is too large to allocate"))?;
        for index in 0..count {
            if index == 0
                && let Some(missing) = self.node.effective_missing.as_ref()
            {
                let bitmap = crate::py::arrow::validity_bitmap_from_missing(missing);
                items.push(PyBytes::new(py, &bitmap).into_py_any(py)?);
            } else {
                items.push(native_buffer_at(py, &self.node, index)?);
            }
        }
        PyTuple::new(py, items)?.into_py_any(py)
    }
}

impl HeapSize for NativeNode {
    fn heap_bytes(&self) -> usize {
        let mut total = std::mem::size_of::<Self>();
        total = total.saturating_add(self.buffers.heap_bytes());
        for child in self.children.iter() {
            total = total.saturating_add(child.heap_bytes());
        }
        total
    }
}

impl HeapSize for NativeArrowType {
    fn heap_bytes(&self) -> usize {
        self.node.heap_bytes()
            + self.extension_name.heap_bytes()
            + self.extension_metadata.heap_bytes()
            + self.names.heap_bytes()
    }
}

impl HeapSize for NativeArrowArray {
    fn heap_bytes(&self) -> usize {
        self.node.heap_bytes()
    }
}

impl NativeArrowType {
    fn into_py_any(self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(self.into_pyobject(py)?.into_any().unbind())
    }

    fn from_node(node: &NativeNode) -> PyResult<Self> {
        Ok(Self {
            node: node.clone(),
            extension_name: node.extension_name.as_ref().map(|s| s.as_ref().to_owned()),
            extension_metadata: node.extension_metadata.as_ref().to_vec(),
            names: node.field_names.as_ref().to_vec(),
            name: node.schema_name.as_ref().to_owned(),
        })
    }
}

impl NativeArrowArray {
    const fn from_node(node: NativeNode, is_extension_root: bool) -> Self {
        Self {
            node,
            is_extension_root,
        }
    }
}

/// Snapshot every schema-sized buffer once for this native node, then run
/// content validation **only on the owned copies** (design-1: never
/// `from_raw_parts` over producer memory for null_count / BinaryView scans).
///
/// Float64 *values* buffers (index 1) are decoded into `Arc<[f64]>` at
/// admission so coordinate import reuses that Arc instead of retaining
/// LE bytes **and** materializing a second `Arc<[f64]>`.
#[derive(Clone)]
enum OwnedBuffer {
    Bytes(Arc<[u8]>),
    F64(Arc<[f64]>),
}

impl OwnedBuffer {
    #[cfg(test)]
    fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Bytes(b) => Some(b.as_ref()),
            Self::F64(_) => None,
        }
    }

    fn heap_bytes(&self) -> usize {
        match self {
            Self::Bytes(b) => b.len(),
            Self::F64(v) => v.len() * 8,
        }
    }
}

/// Owned Arrow buffers. Ordinary formats have a fixed small dense layout;
/// BinaryView's variadic payload slots are deliberately sparse because an
/// imported selected range may reference only a few of thousands of producer
/// fragments. Keeping absent payloads out of this representation makes
/// admission proportional to selected descriptors, not `n_buffers`.
#[derive(Clone)]
enum OwnedBufferSlots {
    Dense(Arc<[Option<OwnedBuffer>]>),
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

    fn heap_bytes(&self) -> usize {
        match self {
            Self::Dense(slots) => slots.iter().flatten().map(OwnedBuffer::heap_bytes).sum(),
            Self::BinaryView {
                validity,
                views,
                data,
                ..
            } => {
                validity.heap_bytes()
                    + views.heap_bytes()
                    + data.values().map(OwnedBuffer::heap_bytes).sum::<usize>()
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
        data.try_reserve(ranges.len()).map_err(|_| {
            PyTypeError::new_err("Arrow binary-view data range count is too large to allocate")
        })?;
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
        let slots = OwnedBufferSlots::BinaryView {
            validity,
            views,
            data: Arc::new(data),
            n_buffers: array.n_buffers,
        };
        validate_owned_null_count(array, &slots)?;
        return Ok((slots, array.binary_view_data_ranges.clone()));
    }

    let mut snapshots: Vec<Option<OwnedBuffer>> = Vec::new();
    snapshots
        .try_reserve_exact(array.n_buffers)
        .map_err(|_| PyTypeError::new_err("Arrow buffer snapshots are too large to allocate"))?;
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

    let slots = OwnedBufferSlots::Dense(Arc::from(snapshots));
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
        // Typed empty for float values so decode reuses Arc<[f64]> without
        // a second LE→f64 materialization path.
        if matches!(array.format, ArrowFormat::Float64) && index == 1 {
            return Ok(OwnedBuffer::F64(Arc::from([])));
        }
        return Ok(OwnedBuffer::Bytes(Arc::from(Vec::<u8>::new())));
    }
    // SAFETY: layout validation + capture shell pin buffer table.
    let source = unsafe { array.buffer_ptr(index) };
    if source.is_null() {
        // Validity is optional. Every non-empty payload/offset buffer is
        // required; returning an empty snapshot here would let a later reader
        // index it and panic instead of reporting malformed Arrow input.
        if index == 0 {
            return Ok(OwnedBuffer::Bytes(Arc::from(Vec::<u8>::new())));
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
            .map_err(|_| PyTypeError::new_err("Arrow buffer snapshot allocation failed"))?
    };
    // Float64 values: decode LE f64 once at admission; drop the byte copy.
    if matches!(array.format, ArrowFormat::Float64) && index == 1 {
        return owned_f64_from_le_bytes(&bytes).map(OwnedBuffer::F64);
    }
    Ok(OwnedBuffer::Bytes(Arc::from(bytes)))
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
        Arc::from(vec![0_u8; width])
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
    let mut rebased = raw.to_vec();
    let mut last = first;
    for index in 0..slots {
        let value = usize::try_from(read(index)?).map_err(|_| {
            crate::py::arrow::geoarrow_parse_error("Arrow offsets must be non-negative")
        })?;
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
    Ok((OwnedBuffer::Bytes(Arc::from(rebased)), first, last))
}

fn owned_f64_from_le_bytes(bytes: &[u8]) -> PyResult<Arc<[f64]>> {
    if !bytes.len().is_multiple_of(8) {
        return Err(PyTypeError::new_err(
            "Arrow float64 values length is not a multiple of 8",
        ));
    }
    let (chunks, rest) = bytes.as_chunks::<8>();
    debug_assert!(rest.is_empty());
    let values: Vec<f64> = chunks
        .iter()
        .map(|chunk| f64::from_le_bytes(*chunk))
        .collect();
    Ok(Arc::from(values))
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

fn native_buffer_at(py: Python<'_>, node: &NativeNode, index: usize) -> PyResult<Py<PyAny>> {
    if index >= node.n_buffers {
        return Ok(py.None());
    }
    let Some(slot) = node.buffers.get(index) else {
        return Ok(py.None());
    };
    match slot {
        OwnedBuffer::Bytes(bytes) => PyBytes::new(py, bytes).into_py_any(py),
        OwnedBuffer::F64(values) => {
            let mut out = Vec::with_capacity(values.len() * 8);
            for &v in values.iter() {
                out.extend_from_slice(&v.to_le_bytes());
            }
            PyBytes::new(py, &out).into_py_any(py)
        },
    }
}

/// Admit a native buffer as owned [`AdmittedBuffer`] (design-1: no producer view,
/// no separate lease type — only admission Arc spans).
pub(crate) fn try_native_admitted_buffer(
    value: &Bound<'_, PyAny>,
    index: usize,
) -> Option<crate::py::arrow::AdmittedBuffer> {
    let Ok(array) = value.cast::<NativeArrowArray>() else {
        return None;
    };
    let node = &array.get().node;
    // Ancestor-OR validity for buffer 0 is a logical bitmap owned by the node,
    // not a producer buffer — force the provider-copy path for that slot.
    if index == 0 && node.effective_missing.is_some() {
        return None;
    }
    if index >= node.n_buffers {
        return None;
    }
    let slot = node.buffers.get(index)?;
    match slot {
        OwnedBuffer::Bytes(bytes) => Some(crate::py::arrow::AdmittedBuffer::from_arc_range(
            Arc::clone(bytes),
            0..bytes.len(),
        )),
        // Float64 values are typed at admission — re-encode LE for the generic
        // byte path only when a caller asks for raw buffer bytes.
        OwnedBuffer::F64(values) => {
            let mut out = Vec::with_capacity(values.len() * 8);
            for &v in values.iter() {
                out.extend_from_slice(&v.to_le_bytes());
            }
            Some(crate::py::arrow::AdmittedBuffer::from_vec(out))
        },
    }
}

/// Prefer typed float64 admission Arc for native coordinate leaves (no dual
/// byte+f64 retention). Returns `None` when the value is not a native float
/// values buffer.
pub(crate) fn try_native_f64_values_arc(
    value: &Bound<'_, PyAny>,
    start: usize,
    count: usize,
) -> Option<PyResult<Arc<[f64]>>> {
    let Ok(array) = value.cast::<NativeArrowArray>() else {
        return None;
    };
    let node = &array.get().node;
    if !matches!(node.format, ArrowFormat::Float64) {
        return None;
    }
    let Some(OwnedBuffer::F64(values)) = node.buffers.get(1) else {
        return None;
    };
    let end = start.checked_add(count)?;
    if end > values.len() {
        return Some(Err(PyTypeError::new_err(
            "Arrow values buffer is shorter than the visible coordinate span",
        )));
    }
    if start == 0 && end == values.len() {
        return Some(Ok(Arc::clone(values)));
    }
    Some(Ok(Arc::from(values[start..end].to_vec())))
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
        return Ok(Arc::new(ranges));
    }
    let max_data_buffers = array.n_buffers - 3;
    let OwnedBuffer::Bytes(views) = views else {
        return Ok(Arc::new(ranges));
    };
    if views.is_empty() {
        return Ok(Arc::new(ranges));
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
        ranges
            .entry(buffer_index)
            .and_modify(|range: &mut std::ops::Range<usize>| {
                range.start = range.start.min(byte_offset);
                range.end = range.end.max(span_end);
            })
            .or_insert(byte_offset..span_end);
    }
    Ok(Arc::new(ranges))
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
    let mut rebased = views.to_vec();
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
    Ok(OwnedBuffer::Bytes(Arc::from(rebased)))
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

/// Build a native Arrow view for one stream batch from an owned admitted schema
/// and a one-shot array capsule (design-1: no borrowed raw schema).
pub(crate) fn native_arrow_from_array_capsule_with_schema(
    py: Python<'_>,
    array_capsule: &Bound<'_, PyAny>,
    admitted_schema: &AdmittedArrowSchema,
    select_struct_child: Option<usize>,
) -> PyResult<Py<PyAny>> {
    let array = capsule_pointer::<ArrowArray>(array_capsule, array_capsule_name())?;
    // SAFETY: one-shot batch capsule; released inside admit.
    let node =
        unsafe { admit_array_with_admitted_schema(array, admitted_schema, select_struct_child) }?;
    let has_extension = node.extension_name.is_some();
    node.into_py_any(py, has_extension)
}

/// Classify a direct Arrow-C array capsule through owned admission, then build storage.
pub(crate) fn geometries_from_native_capsules_classified(
    py: Python<'_>,
    schema_capsule: &Bound<'_, PyAny>,
    array_capsule: &Bound<'_, PyAny>,
    crs: Option<&Bound<'_, PyAny>>,
    epoch: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyAny>> {
    let (node, classified) = admit_geometry_from_capsules(schema_capsule, array_capsule)?;
    let has_extension = node.extension_name.is_some();
    let geometry = node.into_py_any(py, has_extension)?;
    let storage = arrow_storage_from_native_geometry(
        geometry.bind(py),
        classified.encoding,
        classified.wkb_offset_width,
        classified.crs,
        classified.epoch,
    )?;
    geometries_from_arrow_storages(py, vec![storage], crs, epoch)
}

/// Check only the root storage token for a native fallback view.
///
/// Full child layout was already admitted from the owned schema tree. This
/// fallback therefore verifies exactly the part it can observe, rather than
/// pretending a root-format check validates a nested GeoArrow encoding.
pub(crate) fn validate_native_encoding_root_format(
    value: &Bound<'_, PyAny>,
    encoding: GeometryEncoding,
) -> PyResult<()> {
    let array = value.cast::<NativeArrowArray>().map_err(|_| {
        PyTypeError::new_err("expected a native Arrow array for encoding storage validation")
    })?;
    // Layout already validated at owned admission against the producer schema.
    // Re-check encoding vs stored format string only.
    let format = array.get().node.format();
    match encoding {
        crate::py::geoarrow::GeometryEncoding::Wkb => {
            if !matches!(format, "z" | "Z" | "vz") {
                return Err(PyTypeError::new_err(
                    "geoarrow.wkb storage must be binary, large_binary, or binary_view",
                ));
            }
        },
        crate::py::geoarrow::GeometryEncoding::Point => {
            if format != "+s" && !format.starts_with("+w:") {
                return Err(PyTypeError::new_err(
                    "unsupported Arrow schema format for geometry storage (expected +s or +w:N)",
                ));
            }
        },
        _ if matches!(format, "+l" | "+L") => {},
        _ => {
            return Err(PyTypeError::new_err(
                "geoarrow list geometry storage must be list (+l) or large_list (+L)",
            ));
        },
    }
    Ok(())
}

/// Prefer full schema classification for a native array node (returns `None`
/// when the value is not a native Arrow array).
pub(crate) fn native_arrow_storage_via_classifier(
    value: &Bound<'_, PyAny>,
) -> PyResult<Option<ArrowStorage>> {
    let Ok(array) = value.cast::<NativeArrowArray>() else {
        return Ok(None);
    };
    // Classify the node's own schema (geometry array, not a multi-column root —
    // table roots are resolved to the geometry child before this is called).
    let node = &array.get().node;
    let Ok((encoding, width, crs, epoch)) = classify_owned_node_geometry(node) else {
        // Fall through to the legacy attribute walk when the node is a bare
        // intermediate (e.g. storage child) without extension/binary.
        return Ok(None);
    };
    Ok(Some(arrow_storage_from_native_geometry(
        value, encoding, width, crs, epoch,
    )?))
}

/// Combined ancestor+self missing mask for a native array view, when present.
/// `true` means the logical row is null. Used by `arrow_validity` so import
/// never consults only the raw child bitmap after a struct re-root.
pub(crate) fn native_arrow_effective_missing(value: &Bound<'_, PyAny>) -> Option<Arc<[bool]>> {
    value
        .cast::<NativeArrowArray>()
        .ok()
        .and_then(|array| array.get().node.effective_missing.clone())
}

/// Bit alignment of the copied visible validity-byte window. All other owned
/// payloads are offset-zero; only this physical bitmap prefix needs retention.
pub(crate) fn native_arrow_validity_offset(value: &Bound<'_, PyAny>) -> Option<usize> {
    value
        .cast::<NativeArrowArray>()
        .ok()
        .map(|array| array.get().node.validity_offset)
}

pub(crate) fn is_native_arrow_array(value: &Bound<'_, PyAny>) -> bool {
    // Exact PyO3 class only — never match an arbitrary type by class-name string.
    value.is_instance_of::<NativeArrowArray>()
}

pub(crate) fn native_schema_format_is_large_binary(value: &Bound<'_, PyAny>) -> PyResult<bool> {
    native_schema_format(value).map(|format| format == "Z")
}

pub(crate) fn native_schema_format_is_binary_view(value: &Bound<'_, PyAny>) -> PyResult<bool> {
    native_schema_format(value).map(|format| format == "vz")
}

pub(crate) fn native_schema_format_is_binary(value: &Bound<'_, PyAny>) -> PyResult<bool> {
    native_schema_format(value).map(|format| format == "z" || format == "Z" || format == "vz")
}

fn native_schema_format(value: &Bound<'_, PyAny>) -> PyResult<String> {
    value.getattr("type")?.getattr("format")?.extract()
}

/// Classify an already-owned native node from format + extension metadata.
fn classify_owned_node_geometry(
    node: &NativeNode,
) -> PyResult<(
    crate::py::geoarrow::GeometryEncoding,
    crate::py::arrow::WkbOffsetWidth,
    Option<String>,
    Option<f64>,
)> {
    use crate::py::arrow::WkbOffsetWidth;
    use crate::py::geoarrow::GeometryEncoding;
    let format = node.format();
    let (crs, epoch) = if let Some(meta) = node.extension_metadata.as_ref().get(..) {
        if !meta.is_empty() {
            crate::py::arrow::parse_geoarrow_extension_metadata(meta)?
        } else {
            (None, None)
        }
    } else {
        (None, None)
    };
    if let Some(name) = node.extension_name.as_ref() {
        let encoding = GeometryEncoding::from_extension_name(name)
            .ok_or_else(|| PyTypeError::new_err(GeometryEncoding::EXPECTED_EXTENSION))?;
        let width = if matches!(encoding, GeometryEncoding::Wkb) {
            match format {
                "vz" => WkbOffsetWidth::View,
                "Z" => WkbOffsetWidth::Int64,
                _ => WkbOffsetWidth::Int32,
            }
        } else {
            WkbOffsetWidth::Int32
        };
        return Ok((encoding, width, crs, epoch));
    }
    if matches!(format, "z" | "Z" | "vz") {
        let width = match format {
            "vz" => WkbOffsetWidth::View,
            "Z" => WkbOffsetWidth::Int64,
            _ => WkbOffsetWidth::Int32,
        };
        return Ok((GeometryEncoding::Wkb, width, None, None));
    }
    Err(PyTypeError::new_err(
        "expected a geoarrow point, multipoint, linestring, multilinestring, polygon, multipolygon, WKB, binary, or large_binary Arrow array",
    ))
}

pub(crate) fn register_native_arrow_classes(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<NativeArrowArray>()?;
    m.add_class::<NativeArrowType>()?;
    Ok(())
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

    use pyo3::ffi;

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
        assert_eq!(NullCount::Unknown.as_i64(), -1);
        assert_eq!(NullCount::Known(4).as_i64(), 4);
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
            assert_eq!(bitmap.as_ref(), validity);
            let OwnedBuffer::F64(visible_values) = slots.get(1).expect("owned values") else {
                panic!("float64 values are decoded at admission")
            };
            assert_eq!(visible_values.as_ref(), &values[5..]);
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
            assert_eq!(validated.null_count.as_i64(), -1);
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
            let (node, _) = admit_geometry_from_capsules(&schema_capsule, &array_capsule).unwrap();
            assert!(schema.release.is_none());
            assert!(array.release.is_none());
            // Second admit on already-released capsules must fail.
            assert!(admit_geometry_from_capsules(&schema_capsule, &array_capsule).is_err());
            drop(node);
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
            // SAFETY: one-shot batch arrays with owned schema.
            let n1 =
                unsafe { admit_array_with_admitted_schema(&raw mut first_array, &admitted, None) }
                    .unwrap();
            assert!(first_array.release.is_none());
            drop(n1);
            let n2 =
                unsafe { admit_array_with_admitted_schema(&raw mut second_array, &admitted, None) }
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
            let node =
                unsafe { admit_array_with_admitted_schema(array_ptr, &admitted, Some(0)) }.unwrap();
            let output = NativeArrowArray::from_node(node, false);
            assert_eq!(output.__len__(), 3);
            // Admission re-roots the selected child span: its three values
            // are [3, 4, 5], not a retained physical prefix plus offset 3.
            assert_eq!(output.offset(), 0);
            let bytes = output.buffer(py, 1).unwrap();
            let bytes = bytes.bind(py).cast::<PyBytes>().unwrap().as_bytes();
            assert_eq!(bytes.len(), 24);
            assert_eq!(
                f64::from_ne_bytes(bytes[0..8].try_into().unwrap()).to_bits(),
                3.0_f64.to_bits()
            );
            assert_eq!(
                f64::from_ne_bytes(bytes[16..24].try_into().unwrap()).to_bits(),
                5.0_f64.to_bits()
            );
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
            assert_eq!(bitmap.as_ref(), &validity[1..2]);
            let OwnedBuffer::Bytes(local_offsets) = slots.get(1).unwrap() else {
                panic!("expected copied list offsets");
            };
            assert_eq!(local_offsets.len(), 2 * std::mem::size_of::<i32>());
            let (offsets, remainder) = local_offsets.as_ref().as_chunks::<4>();
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
            assert_eq!(child_values.as_ref(), &values[10..16]);
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
            assert_eq!(
                owned.get(data_index + 2).and_then(OwnedBuffer::as_bytes),
                Some(&data[..])
            );
            assert!(
                owned.heap_bytes() < 128,
                "unselected size table was retained"
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
            assert_eq!(
                owned.get(2).and_then(OwnedBuffer::as_bytes),
                Some(&data[2..22])
            );
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
                .and_then(OwnedBuffer::as_bytes)
                .expect("owned BinaryView descriptors");
            let offset = i32::from_le_bytes(view[12..16].try_into().expect("view offset"));
            assert_eq!(offset, 0, "selected payload origin must be rebased");
            let payload = owned
                .get(2)
                .and_then(OwnedBuffer::as_bytes)
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
}
