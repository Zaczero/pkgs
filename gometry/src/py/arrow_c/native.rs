//! Native Arrow C Data Interface import — PyArrow-shaped views over imported
//! capsules so the existing GeoArrow decode lane runs without ``pyarrow``.
#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]

use std::ffi::CStr;
use std::ptr;
use std::sync::Arc;

use pyo3::exceptions::PyTypeError;
use pyo3::prelude::{Bound, Py, PyAny, PyResult, Python};
use pyo3::types::{PyBytes, PyTuple};
use pyo3::{IntoPyObjectExt, pyclass, pymethods};

use crate::HeapSize;
use crate::collections::{HashMap, HashMapExt};
use crate::py::arrow::{
    ArrowStorage, arrow_storage_from_native_geometry, geometries_from_arrow_storages,
};
use crate::py::arrow_c::*;

/// Imported schema pointers are Send+Sync because every view is tied to an
/// ``Arc<ImportedCapsules>`` that owns the moved producer schema.
#[repr(transparent)]
#[derive(Clone, Copy)]
struct ArrowSchemaPtr {
    ptr: *const ArrowSchema,
}

// SAFETY: pointer stability is guaranteed by the boxed moved schema in
// `Arc<ImportedCapsules>`.
unsafe impl Send for ArrowSchemaPtr {}
// SAFETY: imported Arrow schemas are read-only for the decode lifetime.
unsafe impl Sync for ArrowSchemaPtr {}

impl ArrowSchemaPtr {
    const fn new(ptr: *const ArrowSchema) -> Self {
        Self { ptr }
    }

    fn as_ref(&self) -> PyResult<&ArrowSchema> {
        if self.ptr.is_null() {
            return Err(PyTypeError::new_err("Arrow schema pointer is null"));
        }
        // SAFETY: callers only dereference while the owning capsules are alive.
        Ok(unsafe { &*self.ptr })
    }
}

/// Imported array pointers are Send+Sync for the same moved-shell,
/// read-only reason as [`ArrowSchemaPtr`].
#[repr(transparent)]
#[derive(Clone, Copy)]
struct ArrowArrayPtr {
    ptr: *const ArrowArray,
}

// SAFETY: pointer stability is guaranteed by the boxed moved array in
// `Arc<ImportedCapsules>`.
unsafe impl Send for ArrowArrayPtr {}
// SAFETY: imported Arrow arrays are read-only for the decode lifetime.
unsafe impl Sync for ArrowArrayPtr {}

impl ArrowArrayPtr {
    const fn new(ptr: *const ArrowArray) -> Self {
        Self { ptr }
    }

    fn as_ref(&self) -> PyResult<&ArrowArray> {
        if self.ptr.is_null() {
            return Err(PyTypeError::new_err("Arrow array pointer is null"));
        }
        // SAFETY: callers only dereference while the owning capsules are alive.
        Ok(unsafe { &*self.ptr })
    }
}

/// Arrow C Data Interface `null_count`: `-1` means unknown; known counts must
/// not exceed the logical length.
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
    /// `+s` — struct.
    Struct,
}

/// Shared LargeList rejection message (schema-validation keystone).
pub(crate) const LARGE_LIST_UNSUPPORTED: &str =
    "Arrow LargeList (+L) is not supported; use list (+l) with i32 offsets";

impl ArrowFormat {
    /// Fallible parse: reject every format not decoded end-to-end (including
    /// LargeList `+L`, which the nested reader always treats as i32 offsets).
    fn parse(format: &str) -> PyResult<Self> {
        reject_large_list_format(format)?;
        match format {
            "g" => Ok(Self::Float64),
            "z" => Ok(Self::Binary),
            "Z" => Ok(Self::LargeBinary),
            "vz" => Ok(Self::BinaryView),
            "+l" => Ok(Self::List),
            "+s" => Ok(Self::Struct),
            _ => Err(PyTypeError::new_err(format!(
                "unsupported Arrow schema format '{format}'"
            ))),
        }
    }

    const fn offset_width(self) -> Option<usize> {
        match self {
            Self::Binary | Self::List => Some(4),
            Self::LargeBinary => Some(8),
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
            Self::List => {
                if n_buffers != 2 || n_children != 1 {
                    return Err(PyTypeError::new_err(
                        "Arrow list array requires exactly 2 buffers and 1 child",
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

#[derive(Clone)]
struct ValidatedArrowArray {
    ptr: ArrowArrayPtr,
    /// Visible logical length (parent length for struct-field views).
    length: usize,
    /// Visible start index into the allocation.
    offset: usize,
    /// `offset + length` (checked).
    end: usize,
    null_count: NullCount,
    n_buffers: usize,
    n_children: usize,
    format: ArrowFormat,
    /// BinaryView only: sparse max valid endpoint keyed by referenced
    /// non-inline data-buffer index (never consults null-row descriptors).
    /// Absent key ≡ logical endpoint 0. Never sized from untrusted `n_buffers`.
    /// `None` for non-BinaryView.
    binary_view_data_ends: Option<Arc<HashMap<usize, usize>>>,
}

impl ValidatedArrowArray {
    /// Root / non-struct child: visible range equals the array's own offset+length.
    fn new(
        ptr: *const ArrowArray,
        format: ArrowFormat,
        schema_n_children: usize,
    ) -> PyResult<Self> {
        Self::from_raw(ptr, format, None, schema_n_children)
    }

    /// Struct field: visible length is the parent's, offset inherits parent
    /// offset; reject when the visible end exceeds the child's raw allocation.
    fn struct_child(
        ptr: *const ArrowArray,
        format: ArrowFormat,
        parent_offset: usize,
        parent_length: usize,
        schema_n_children: usize,
    ) -> PyResult<Self> {
        Self::from_raw(
            ptr,
            format,
            Some((parent_offset, parent_length)),
            schema_n_children,
        )
    }

    fn from_raw(
        ptr: *const ArrowArray,
        format: ArrowFormat,
        parent_slice: Option<(usize, usize)>,
        schema_n_children: usize,
    ) -> PyResult<Self> {
        let ptr = ArrowArrayPtr::new(ptr);
        let raw = ptr.as_ref()?;
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
        let end = offset
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
        let mut validated = Self {
            ptr,
            length,
            offset,
            end,
            null_count,
            n_buffers,
            n_children,
            format,
            binary_view_data_ends: None,
        };
        // Known-nonnegative null_count must match the visible validity window
        // (P02). Known(0) is a fast path that never inspects the bitmap;
        // Known(n>0) and Unknown both read it — only Known is checked here.
        if let NullCount::Known(expected) = null_count
            && expected > 0
        {
            let mut actual = 0_usize;
            for row in 0..length {
                if !validated_row_is_valid(&validated, row)? {
                    actual += 1;
                }
            }
            if actual != expected {
                return Err(crate::py::arrow::geoarrow_parse_error(format!(
                    "Arrow null_count ({expected}) does not match validity bitmap ({actual} null rows)"
                )));
            }
        }
        // BinaryView: one validity-aware pass bounds present descriptors and
        // caches each data buffer's max valid endpoint (null slots ignored).
        // Binary/List offset *content* is validated on the import lanes
        // (`ensure_pyarrow_storage_offsets_monotonic` / zero-row stream check),
        // not here — layout unit tests use null dummy buffers.
        if matches!(format, ArrowFormat::BinaryView) {
            validated.binary_view_data_ends = Some(scan_binary_view_data_ends(&validated)?);
        }
        Ok(validated)
    }

    fn raw(&self) -> PyResult<&ArrowArray> {
        if self.ptr.ptr.is_null() {
            return Err(PyTypeError::new_err("Arrow array pointer is null"));
        }
        // SAFETY: the wrapper is only stored on `NativeNode`, and every node
        // keeps the capsule-owning `ImportedCapsules` alive.
        Ok(unsafe { &*self.ptr.ptr })
    }
}

pub(crate) fn schema_format_str(schema: &ArrowSchema) -> PyResult<&str> {
    if schema.format.is_null() {
        return Err(PyTypeError::new_err("Arrow schema format is null"));
    }
    // SAFETY: Arrow format strings are NUL-terminated for the capsule lifetime.
    unsafe { CStr::from_ptr(schema.format) }
        .to_str()
        .map_err(|_| PyTypeError::new_err("Arrow schema format is not valid UTF-8"))
}

/// Reject format token `+L` (keystone leaf of schema validation).
pub(crate) fn reject_large_list_format(format: &str) -> PyResult<()> {
    if format == "+L" {
        return Err(PyTypeError::new_err(LARGE_LIST_UNSUPPORTED));
    }
    Ok(())
}

/// Shallow-move one Arrow base structure, leaving the producer's source shell
/// released without invoking its callback.
///
/// # Safety
///
/// `source` must be a live producer-owned base structure. The caller takes
/// ownership of the returned shell and must pass it to `drop_moved_arrow`.
unsafe fn move_arrow_shell<T: ArrowReleaseSlot>(source: *mut T) -> *mut T {
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
unsafe fn drop_moved_arrow<T: ArrowReleaseSlot>(shell: *mut T) {
    // SAFETY: the caller transfers the single moved-shell allocation here.
    unsafe {
        if let Some(release) = (*T::release_slot(shell)).take() {
            // Clearing first makes the consumer's call exactly-once even for a
            // non-conforming producer callback.
            release(shell);
        }
        drop(Box::from_raw(shell));
    }
}

/// Owns the moved array base structure and, for direct capsules, the moved
/// schema base structure for the duration of a decode.
pub(crate) struct ImportedCapsules {
    /// Direct capsules move and own their schema; stream batches borrow the
    /// stream-owned schema shared across all arrays.
    schema: ArrowSchemaPtr,
    schema_shell: Option<ArrowSchemaPtr>,
    /// Owned moved array shell; `array.ptr` is the same allocation for reads.
    array_shell: ArrowArrayPtr,
    array: ValidatedArrowArray,
}

impl Drop for ImportedCapsules {
    fn drop(&mut self) {
        // SAFETY: every construction path owns `array_shell`; direct capsules
        // also own `schema_shell`, while stream schemas remain externally owned.
        unsafe {
            if let Some(schema_shell) = self.schema_shell {
                drop_moved_arrow(schema_shell.ptr.cast_mut());
            }
            drop_moved_arrow(self.array_shell.ptr.cast_mut());
        }
    }
}

impl ImportedCapsules {
    pub(crate) fn new(
        schema_capsule: &Bound<'_, PyAny>,
        array_capsule: &Bound<'_, PyAny>,
    ) -> PyResult<Arc<Self>> {
        let schema = capsule_pointer::<ArrowSchema>(schema_capsule, schema_capsule_name())?;
        let array = capsule_pointer::<ArrowArray>(array_capsule, array_capsule_name())?;
        let schema_ptr = ArrowSchemaPtr::new(schema);
        if schema_ptr.as_ref()?.release.is_none() {
            return Err(PyTypeError::new_err("Arrow schema is already released"));
        }
        let array_ptr = ArrowArrayPtr::new(array);
        if array_ptr.as_ref()?.release.is_none() {
            return Err(PyTypeError::new_err("Arrow array is already released"));
        }
        // SAFETY: both source base structures were checked live above. Direct
        // capsules transfer their ownership into these shells before decoding.
        let schema_shell = unsafe { move_arrow_shell(schema) };
        // SAFETY: see the schema move directly above.
        let array_shell = unsafe { move_arrow_shell(array) };
        let schema_ptr = ArrowSchemaPtr::new(schema_shell);
        let array_ptr = ArrowArrayPtr::new(array_shell);
        Self::from_parts(schema_ptr, Some(schema_ptr), array_ptr)
    }

    /// Import one stream batch: the stream owns one schema across all batches,
    /// so borrow that live schema while moving the batch's one-shot array.
    pub(crate) fn new_with_borrowed_schema(
        schema_capsule: &Bound<'_, PyAny>,
        array_capsule: &Bound<'_, PyAny>,
    ) -> PyResult<Arc<Self>> {
        let schema = capsule_pointer::<ArrowSchema>(schema_capsule, schema_capsule_name())?;
        let array = capsule_pointer::<ArrowArray>(array_capsule, array_capsule_name())?;
        let schema_ptr = ArrowSchemaPtr::new(schema);
        if schema_ptr.as_ref()?.release.is_none() {
            return Err(PyTypeError::new_err("Arrow schema is already released"));
        }
        let array_ptr = ArrowArrayPtr::new(array);
        if array_ptr.as_ref()?.release.is_none() {
            return Err(PyTypeError::new_err("Arrow array is already released"));
        }
        // SAFETY: each stream batch array is one-shot even though its schema is
        // shared by the stream; transfer only this array's ownership.
        let array_shell = unsafe { move_arrow_shell(array) };
        Self::from_parts(schema_ptr, None, ArrowArrayPtr::new(array_shell))
    }

    fn from_parts(
        schema: ArrowSchemaPtr,
        schema_shell: Option<ArrowSchemaPtr>,
        array_shell: ArrowArrayPtr,
    ) -> PyResult<Arc<Self>> {
        let schema_ref = schema.as_ref()?;
        let decoded = (|| {
            let format = ArrowFormat::parse(schema_format_str(schema_ref)?)?;
            let schema_n_children = usize::try_from(schema_ref.n_children).map_err(|_| {
                PyTypeError::new_err("Arrow schema n_children is negative or too large")
            })?;
            ValidatedArrowArray::new(array_shell.ptr, format, schema_n_children)
        })();
        match decoded {
            Ok(array) => Ok(Arc::new(Self {
                schema,
                schema_shell,
                array_shell,
                array,
            })),
            Err(error) => {
                // SAFETY: validation failed before ownership entered
                // `ImportedCapsules`; release only the shells this path owns.
                unsafe {
                    if let Some(schema_shell) = schema_shell {
                        drop_moved_arrow(schema_shell.ptr.cast_mut());
                    }
                    drop_moved_arrow(array_shell.ptr.cast_mut());
                }
                Err(error)
            },
        }
    }
}

#[derive(Clone)]
struct NativeNode {
    owner: Arc<ImportedCapsules>,
    array: ValidatedArrowArray,
    schema: ArrowSchemaPtr,
    /// Top-level extension metadata lives on the root schema; inner nodes inherit
    /// the frame from here.
    root_schema: ArrowSchemaPtr,
    /// Combined ancestor + self missing mask for the visible window (`true` =
    /// null). A geometry row is missing if ANY ancestor struct said so — raw
    /// child validity alone is not enough. When set, supersedes the child's
    /// raw null_count / buffer-0 for geometry-level validity reads.
    effective_missing: Option<Arc<[bool]>>,
}

impl NativeNode {
    fn heap_bytes(&self) -> usize {
        HeapSize::heap_bytes(self)
    }
}

impl NativeNode {
    fn root(owner: Arc<ImportedCapsules>) -> Self {
        let schema = owner.schema;
        let array = owner.array.clone();
        Self {
            owner,
            array,
            schema,
            root_schema: schema,
            // Root has no ancestor; own nulls ride the raw validity bitmap.
            effective_missing: None,
        }
    }

    fn child(&self, index: usize) -> PyResult<Self> {
        self.child_node(index, false)
    }

    fn struct_field(&self, index: usize) -> PyResult<Self> {
        self.child_node(index, true)
    }

    fn child_node(&self, index: usize, as_struct_field: bool) -> PyResult<Self> {
        let array = self.array.raw()?;
        // SAFETY: child pointers are valid while the imported tree is alive.
        unsafe {
            // F5: structural n_children bound before children.add (typed ParseError).
            if index >= self.array.n_children {
                return Err(crate::py::errors::parse_error(
                    format!(
                        "Arrow child index {index} is out of range for n_children={}",
                        self.array.n_children
                    ),
                    crate::py::errors::ParseFormat::GeoArrow,
                ));
            }
            if array.children.is_null() {
                return Err(crate::py::errors::parse_error(
                    "Arrow array children pointer is null",
                    crate::py::errors::ParseFormat::GeoArrow,
                ));
            }
            let child_array = *array.children.add(index);
            if child_array.is_null() {
                return Err(crate::py::errors::parse_error(
                    "Arrow child array is null",
                    crate::py::errors::ParseFormat::GeoArrow,
                ));
            }
            let schema = self.schema.as_ref()?;
            let schema_children = usize::try_from(schema.n_children).map_err(|_| {
                crate::py::errors::parse_error(
                    "Arrow schema n_children is negative or too large",
                    crate::py::errors::ParseFormat::GeoArrow,
                )
            })?;
            let child_schema = if schema.children.is_null() || index >= schema_children {
                return Err(crate::py::errors::parse_error(
                    "Arrow child schema index out of range",
                    crate::py::errors::ParseFormat::GeoArrow,
                ));
            } else {
                *schema.children.add(index)
            };
            if child_schema.is_null() {
                return Err(crate::py::errors::parse_error(
                    "Arrow child schema is null",
                    crate::py::errors::ParseFormat::GeoArrow,
                ));
            }
            let child_schema_ptr = ArrowSchemaPtr::new(child_schema);
            let child_schema_ref = child_schema_ptr.as_ref()?;
            let format = ArrowFormat::parse(schema_format_str(child_schema_ref)?)?;
            let child_schema_n_children =
                usize::try_from(child_schema_ref.n_children).map_err(|_| {
                    PyTypeError::new_err("Arrow schema n_children is negative or too large")
                })?;
            let validated = if as_struct_field {
                ValidatedArrowArray::struct_child(
                    child_array,
                    format,
                    self.effective_offset(),
                    self.array.length,
                    child_schema_n_children,
                )?
            } else {
                ValidatedArrowArray::new(child_array, format, child_schema_n_children)?
            };
            // Struct fields inherit every ancestor's nulls (OR into the child
            // mask). List/values children keep an independent window — their
            // parent list nulls are geometry-level on the list itself, not
            // re-applied here.
            let effective_missing = if as_struct_field {
                combine_struct_ancestor_missing(self, &validated)?
            } else {
                None
            };
            Ok(Self {
                owner: Arc::clone(&self.owner),
                array: validated,
                schema: child_schema_ptr,
                root_schema: self.root_schema,
                effective_missing,
            })
        }
    }

    const fn effective_offset(&self) -> usize {
        self.array.offset
    }

    const fn array(&self) -> &ValidatedArrowArray {
        &self.array
    }

    fn schema(&self) -> PyResult<&ArrowSchema> {
        self.schema.as_ref()
    }

    fn root_schema(&self) -> PyResult<&ArrowSchema> {
        self.root_schema.as_ref()
    }

    fn format(&self) -> PyResult<&str> {
        schema_format_str(self.schema()?)
    }

    fn field_index(&self, name: &str) -> PyResult<usize> {
        let schema = self.schema()?;
        if schema.children.is_null() || schema.n_children <= 0 {
            return Err(PyTypeError::new_err(format!(
                "Arrow struct has no field named '{name}'"
            )));
        }
        let n_children = usize::try_from(schema.n_children).map_err(|_| {
            PyTypeError::new_err("Arrow schema n_children is negative or too large")
        })?;
        // SAFETY: children are valid for the capsule lifetime.
        unsafe {
            for index in 0..n_children {
                let child = *schema.children.add(index);
                if child.is_null() || (*child).name.is_null() {
                    continue;
                }
                let child_name = CStr::from_ptr((*child).name).to_str().unwrap_or("");
                if child_name == name {
                    return Ok(index);
                }
            }
        }
        Err(PyTypeError::new_err(format!(
            "Arrow struct has no field named '{name}'"
        )))
    }
}

#[pyclass(
    name = "_NativeArrowType",
    module = "gometry._lib",
    frozen,
    skip_from_py_object
)]
#[derive(Clone)]
struct NativeArrowType {
    node: NativeNode,
    extension_name: Option<String>,
    extension_metadata: Vec<u8>,
    names: Vec<String>,
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
    fn format(&self) -> PyResult<String> {
        self.node.format().map(str::to_owned)
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
        self.node.array().length
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
            || self.node.array().null_count.as_i64(),
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

    /// Single buffer by index — O(1) length via the BinaryView endpoint cache;
    /// does not rebuild the full buffer tuple (avoids O(N³) BinaryView DoS).
    fn buffer(&self, py: Python<'_>, index: usize) -> PyResult<Py<PyAny>> {
        if index == 0
            && let Some(ref missing) = self.node.effective_missing
        {
            // Ancestor-OR validity is logical over the visible window (offset 0).
            let bitmap = crate::py::arrow::validity_bitmap_from_missing(missing);
            return PyBytes::new(py, &bitmap).into_py_any(py);
        }
        native_buffer_at(py, self.node.array(), index)
    }

    fn buffers(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let array = self.node.array();
        let count = array.n_buffers;
        let mut items = Vec::new();
        items
            .try_reserve(count)
            .map_err(|_| PyTypeError::new_err("Arrow buffer count is too large to allocate"))?;
        for index in 0..count {
            if index == 0
                && let Some(ref missing) = self.node.effective_missing
            {
                let bitmap = crate::py::arrow::validity_bitmap_from_missing(missing);
                items.push(PyBytes::new(py, &bitmap).into_py_any(py)?);
            } else {
                items.push(native_buffer_at(py, array, index)?);
            }
        }
        PyTuple::new(py, items)?.into_py_any(py)
    }
}

impl HeapSize for NativeNode {
    fn heap_bytes(&self) -> usize {
        // The Arrow buffers and C metadata are producer-owned behind the
        // imported pointers. Count only the Rust holder keeping those producer
        // capsules alive for this native view family.
        std::mem::size_of::<ImportedCapsules>()
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
    fn from_node(node: &NativeNode) -> PyResult<Self> {
        let root = node.root_schema()?;
        let mut extension_name = None;
        let mut extension_metadata = Vec::new();
        if let Some(name) = schema_metadata_value(root, b"ARROW:extension:name")? {
            extension_name = Some(crate::py::arrow::decode_extension_name(name)?);
        }
        if let Some(metadata) = schema_metadata_value(root, b"ARROW:extension:metadata")? {
            extension_metadata = metadata;
        }
        let schema = node.schema()?;
        let mut names = Vec::new();
        if schema.n_children > 0 && !schema.children.is_null() {
            let n_children = usize::try_from(schema.n_children).map_err(|_| {
                PyTypeError::new_err("Arrow schema n_children is negative or too large")
            })?;
            names.try_reserve(n_children).map_err(|_| {
                PyTypeError::new_err("Arrow schema child count is too large to allocate")
            })?;
            // SAFETY: schema children are valid for the capsule lifetime.
            unsafe {
                for index in 0..n_children {
                    let child = *schema.children.add(index);
                    if child.is_null() || (*child).name.is_null() {
                        names.push(String::new());
                    } else {
                        names.push(
                            CStr::from_ptr((*child).name)
                                .to_str()
                                .unwrap_or("")
                                .to_owned(),
                        );
                    }
                }
            }
        }
        Ok(Self {
            node: node.clone(),
            extension_name,
            extension_metadata,
            names,
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

impl NativeNode {
    fn into_py_any(self, py: Python<'_>, is_extension_root: bool) -> PyResult<Py<PyAny>> {
        Ok(NativeArrowArray::from_node(self, is_extension_root)
            .into_pyobject(py)?
            .into_any()
            .unbind())
    }
}

fn native_buffer_at(
    py: Python<'_>,
    array: &ValidatedArrowArray,
    index: usize,
) -> PyResult<Py<PyAny>> {
    if index >= array.n_buffers {
        return Ok(py.None());
    }
    // Schema-driven length first — never touch the pointer table for a
    // zero-length slot, and never build a slice until the span validates.
    let len = native_buffer_len(array, index)?;
    // Zero-sized Arrow buffers may carry a null data pointer (C Data Interface);
    // present them as empty bytes so required-buffer consumers succeed.
    if len == 0 {
        return PyBytes::new(py, &[]).into_py_any(py);
    }
    let raw = array.raw()?;
    // SAFETY: buffer pointers are valid for the capsule lifetime; `index` is
    // in-range after layout validation of `n_buffers`.
    unsafe {
        let ptr = *raw.buffers.add(index);
        if ptr.is_null() {
            return Ok(py.None());
        }
        let slice = std::slice::from_raw_parts(ptr.cast::<u8>(), len);
        PyBytes::new(py, slice).into_py_any(py)
    }
}

/// Schema-driven buffer length: exhaustive dispatch on format, checked
/// arithmetic (including the `isize::MAX` `from_raw_parts` bound), validity
/// from `(offset+length).div_ceil(8)`.
fn native_buffer_len(array: &ValidatedArrowArray, index: usize) -> PyResult<usize> {
    match array.format {
        ArrowFormat::BinaryView => native_binary_view_buffer_len(array, index),
        ArrowFormat::Float64 => match index {
            0 => Ok(validity_buffer_len(array)),
            1 => checked_byte_span(array.end, 8, "Arrow float64 values length overflows"),
            _ => Ok(0),
        },
        ArrowFormat::Binary | ArrowFormat::LargeBinary => match index {
            0 => Ok(validity_buffer_len(array)),
            1 => {
                let width = array
                    .format
                    .offset_width()
                    .expect("binary formats have offset width");
                let slots = array
                    .end
                    .checked_add(1)
                    .ok_or_else(|| PyTypeError::new_err("Arrow binary offset count overflows"))?;
                checked_byte_span(slots, width, "Arrow binary offsets length overflows")
            },
            2 => native_binary_data_len(array),
            _ => Ok(0),
        },
        ArrowFormat::List => match index {
            0 => Ok(validity_buffer_len(array)),
            1 => {
                let slots = array
                    .end
                    .checked_add(1)
                    .ok_or_else(|| PyTypeError::new_err("Arrow list offset count overflows"))?;
                checked_byte_span(slots, 4, "Arrow list offsets length overflows")
            },
            _ => Ok(0),
        },
        ArrowFormat::Struct => match index {
            0 => Ok(validity_buffer_len(array)),
            _ => Ok(0),
        },
    }
}

const fn validity_buffer_len(array: &ValidatedArrowArray) -> usize {
    // Spec: validity is sized from the physical bitmap span covering
    // `[0, offset+length)`, never from null_count.
    array.end.div_ceil(8)
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

fn native_binary_view_buffer_len(array: &ValidatedArrowArray, index: usize) -> PyResult<usize> {
    if index >= array.n_buffers {
        return Ok(0);
    }
    let data_buffer_count = array.n_buffers - 3;
    if index == array.n_buffers - 1 {
        return checked_byte_span(
            data_buffer_count,
            8,
            "Arrow binary-view variadic-sizes length overflows",
        );
    }
    match index {
        0 => Ok(validity_buffer_len(array)),
        1 => checked_byte_span(array.end, 16, "Arrow binary-view buffer length overflows"),
        _ => native_binary_view_data_len(array, index - 2),
    }
}

/// Decode the mandatory final BinaryView variadic-sizes buffer without
/// assuming alignment. The C Data Interface fixes this table's byte length at
/// `8 * (n_buffers - 3)`; each entry describes the matching data buffer.
fn binary_view_data_buffer_sizes(array: &ValidatedArrowArray) -> PyResult<&[u8]> {
    let data_buffer_count = array.n_buffers - 3;
    if data_buffer_count == 0 {
        return Ok(&[]);
    }
    let byte_len = checked_byte_span(
        data_buffer_count,
        8,
        "Arrow binary-view variadic-sizes length overflows",
    )?;
    let raw = array.raw()?;
    // SAFETY: layout validation guarantees a buffer table with `n_buffers`
    // entries; this final entry is the mandatory BinaryView sizes table.
    let sizes_ptr = unsafe { *raw.buffers.add(array.n_buffers - 1) };
    if sizes_ptr.is_null() {
        return Err(PyTypeError::new_err(
            "Arrow binary-view variadic-sizes buffer is required but missing",
        ));
    }
    // SAFETY: an ABI-conforming BinaryView producer provides the mandatory
    // `8 * (n_buffers - 3)`-byte variadic-sizes table at this pointer.
    Ok(unsafe { std::slice::from_raw_parts(sizes_ptr.cast::<u8>(), byte_len) })
}

fn binary_view_declared_data_size(sizes: &[u8], buffer_index: usize) -> PyResult<usize> {
    let byte_offset = checked_byte_span(
        buffer_index,
        8,
        "Arrow binary-view variadic-sizes index overflows",
    )?;
    let end = byte_offset
        .checked_add(8)
        .ok_or_else(|| PyTypeError::new_err("Arrow binary-view variadic-sizes index overflows"))?;
    let bytes: [u8; 8] = sizes
        .get(byte_offset..end)
        .ok_or_else(|| PyTypeError::new_err("Arrow binary-view buffer index out of range"))?
        .try_into()
        .expect("8-byte validated variadic size");
    usize::try_from(i64::from_le_bytes(bytes)).map_err(|_| {
        PyTypeError::new_err("Arrow binary-view variadic data buffer size is negative or too large")
    })
}

fn validate_binary_view_data_buffer_sizes(array: &ValidatedArrowArray) -> PyResult<&[u8]> {
    let sizes = binary_view_data_buffer_sizes(array)?;
    for buffer_index in 0..array.n_buffers - 3 {
        binary_view_declared_data_size(sizes, buffer_index)?;
    }
    Ok(sizes)
}

/// Whether the visible row is valid. Null-row BinaryView descriptors are
/// semantically ignored and must never contribute to buffer-index or span
/// validation (their payload bytes may be arbitrary).
fn binary_view_row_is_valid(array: &ValidatedArrowArray, row: usize) -> PyResult<bool> {
    match array.null_count {
        NullCount::Known(0) => Ok(true),
        NullCount::Known(n) if n == array.length => Ok(false),
        NullCount::Known(_) | NullCount::Unknown => {
            let raw = array.raw()?;
            if array.n_buffers == 0 {
                return Ok(true);
            }
            // SAFETY: layout requires non-null buffer table when n_buffers > 0.
            let validity_ptr = unsafe { *raw.buffers.add(0) };
            if validity_ptr.is_null() {
                // No bitmap: only legal when null_count is 0 or unknown-with-no-nulls.
                // Known positive nulls without a bitmap is a layout error.
                if matches!(array.null_count, NullCount::Known(n) if n > 0) {
                    return Err(PyTypeError::new_err(
                        "Arrow binary-view validity bitmap is required when null_count > 0",
                    ));
                }
                return Ok(true);
            }
            let bit = array.offset.checked_add(row).ok_or_else(|| {
                PyTypeError::new_err("Arrow binary-view validity index overflows")
            })?;
            let byte_index = bit / 8;
            let validity_len = validity_buffer_len(array);
            if byte_index >= validity_len {
                return Err(PyTypeError::new_err(
                    "Arrow binary-view validity bitmap is shorter than declared",
                ));
            }
            // SAFETY: byte_index is in-range for the schema-sized validity span.
            let byte = unsafe { *validity_ptr.cast::<u8>().add(byte_index) };
            Ok((byte & (1 << (bit % 8))) != 0)
        },
    }
}

/// One validity-aware BinaryView pass: never reads null descriptors; bounds
/// present non-inline `buffer_index`; caches each *referenced* data buffer's
/// max valid endpoint in a sparse map so later length queries are O(1) and a
/// forged `n_buffers` cannot force a multi-GB dense allocation.
fn scan_binary_view_data_ends(array: &ValidatedArrowArray) -> PyResult<Arc<HashMap<usize, usize>>> {
    // Sparse only — never `vec![0; n_buffers-3]` from untrusted counts.
    let mut ends = HashMap::new();
    let sizes = validate_binary_view_data_buffer_sizes(array)?;
    if array.length == 0 {
        return Ok(Arc::new(ends));
    }
    let max_data_buffers = array.n_buffers - 3;
    let view_bytes = checked_byte_span(array.end, 16, "Arrow binary-view buffer length overflows")?;
    if view_bytes == 0 {
        return Ok(Arc::new(ends));
    }
    let raw = array.raw()?;
    // SAFETY: layout validation requires buffers non-null when n_buffers > 0.
    let views_ptr = unsafe { *raw.buffers.add(1) };
    if views_ptr.is_null() {
        // No views allocation: cannot inspect descriptors. Schema-driven view
        // buffer length still applies; materialization fails later if non-null
        // rows need the views pointer. Matches zero-descriptor-cache ends.
        return Ok(Arc::new(ends));
    }
    // SAFETY: `view_bytes` is schema-sized and within isize::MAX.
    let views = unsafe { std::slice::from_raw_parts(views_ptr.cast::<u8>(), view_bytes) };
    for row in 0..array.length {
        if !binary_view_row_is_valid(array, row)? {
            continue;
        }
        let start = array
            .offset
            .checked_add(row)
            .and_then(|index| index.checked_mul(16))
            .ok_or_else(|| PyTypeError::new_err("Arrow binary-view row index overflows"))?;
        let end = start
            .checked_add(16)
            .ok_or_else(|| PyTypeError::new_err("Arrow binary-view row index overflows"))?;
        if end > views.len() {
            return Err(PyTypeError::new_err(
                "Arrow binary-view buffer is shorter than declared",
            ));
        }
        let view = &views[start..end];
        let length = i32::from_le_bytes(view[0..4].try_into().expect("view length"));
        let length = usize::try_from(length)
            .map_err(|_| PyTypeError::new_err("Arrow binary-view length is negative"))?;
        if length <= 12 {
            // m02: PRESENT inline rows require zero padding past the payload.
            if view[4 + length..16].iter().any(|&b| b != 0) {
                return Err(PyTypeError::new_err(
                    "Arrow binary-view inline padding must be zero",
                ));
            }
            continue;
        }
        let buffer_index = i32::from_le_bytes(view[8..12].try_into().expect("view buffer index"));
        let buffer_index = usize::try_from(buffer_index)
            .map_err(|_| PyTypeError::new_err("Arrow binary-view buffer index is negative"))?;
        if buffer_index >= max_data_buffers {
            return Err(PyTypeError::new_err(
                "Arrow binary-view buffer index out of range",
            ));
        }
        let byte_offset = i32::from_le_bytes(view[12..16].try_into().expect("view byte offset"));
        let byte_offset = usize::try_from(byte_offset)
            .map_err(|_| PyTypeError::new_err("Arrow binary-view byte offset is negative"))?;
        let span_end = byte_offset
            .checked_add(length)
            .ok_or_else(|| PyTypeError::new_err("Arrow binary-view byte range overflows"))?;
        // Reject spans that cannot form a Rust slice (isize::MAX).
        checked_byte_span(span_end, 1, "Arrow binary-view data length overflows")?;
        let declared_size = binary_view_declared_data_size(sizes, buffer_index)?;
        if span_end > declared_size {
            return Err(PyTypeError::new_err(
                "Arrow binary-view byte range exceeds declared data buffer size",
            ));
        }
        // m02: external view prefix must match the first 4 bytes of data.
        // SAFETY: layout validation requires buffers non-null for n_buffers.
        let data_ptr = unsafe { *raw.buffers.add(2 + buffer_index) };
        if data_ptr.is_null() {
            return Err(PyTypeError::new_err(
                "Arrow binary-view data buffer is required but missing",
            ));
        }
        // SAFETY: `span_end <= declared_size` was validated against the
        // mandatory BinaryView variadic-sizes table before this bounded read.
        let prefix_src =
            unsafe { std::slice::from_raw_parts(data_ptr.cast::<u8>().add(byte_offset), 4) };
        if view[4..8] != *prefix_src {
            return Err(PyTypeError::new_err(
                "Arrow binary-view prefix does not match referenced data",
            ));
        }
        ends.entry(buffer_index)
            .and_modify(|existing| *existing = (*existing).max(span_end))
            .or_insert(span_end);
    }
    Ok(Arc::new(ends))
}

fn native_binary_view_data_len(
    array: &ValidatedArrowArray,
    target_buffer: usize,
) -> PyResult<usize> {
    let Some(ends) = array.binary_view_data_ends.as_ref() else {
        return Ok(0);
    };
    // Absent key ≡ endpoint 0 (never dense-grown to the index).
    let max_end = ends.get(&target_buffer).copied().unwrap_or(0);
    // Terminal data-buffer span must also satisfy the isize::MAX slice bound.
    checked_byte_span(max_end, 1, "Arrow binary-view data length overflows")
}

/// Structural offset-chain check for Binary/LargeBinary/List over the array's
/// visible window `[offset, offset + length]` (inclusive of the start slot).
/// Length-0 arrays still have one start offset that must be non-negative (D18).
/// `child_len` is the list child length or binary data-buffer byte length
/// (N2: terminal offset must not exceed it).
fn ensure_validated_array_offsets_monotonic(
    array: &ValidatedArrowArray,
    child_len: usize,
) -> PyResult<()> {
    let width = array
        .format
        .offset_width()
        .ok_or_else(|| PyTypeError::new_err("Arrow format has no offsets buffer"))?;
    if array.n_buffers < 2 {
        return Err(PyTypeError::new_err(
            "Arrow offsets buffer is required but missing",
        ));
    }
    let raw = array.raw()?;
    // SAFETY: layout validated; buffers pointer non-null when n_buffers > 0.
    let offsets_ptr = unsafe { *raw.buffers.add(1) };
    if offsets_ptr.is_null() {
        // Empty arrays may omit buffers (implicit start offset 0); non-empty
        // require a real offsets chain.
        return if array.length == 0 {
            Ok(())
        } else {
            Err(crate::py::arrow::geoarrow_parse_error(
                "Arrow offsets buffer is required but missing",
            ))
        };
    }
    let slots = array
        .end
        .checked_add(1)
        .ok_or_else(|| PyTypeError::new_err("Arrow offset count overflows"))?;
    let bytes = checked_byte_span(slots, width, "Arrow offsets length overflows")?;
    // SAFETY: schema-derived length, producer-owned for the capsule lifetime.
    let offsets = unsafe { std::slice::from_raw_parts(offsets_ptr.cast::<u8>(), bytes) };
    // Visible window needs `length + 1` slots starting at `array.offset`.
    let window = array.offset;
    let count = array.length;
    let need = window
        .checked_add(count)
        .and_then(|end| end.checked_add(1))
        .ok_or_else(|| {
            crate::py::arrow::geoarrow_parse_error(
                "Arrow offsets buffer is shorter than declared array length",
            )
        })?;
    if need > slots {
        return Err(crate::py::arrow::geoarrow_parse_error(
            "Arrow offsets buffer is shorter than declared array length",
        ));
    }
    if width == 8 {
        let mut prev: Option<i64> = None;
        let mut terminal: i64 = 0;
        for index in 0..=count {
            let byte = (window + index)
                .checked_mul(8)
                .ok_or_else(|| PyTypeError::new_err("Arrow offset index overflows"))?;
            let chunk = offsets[byte..byte + 8].try_into().expect("8-byte offset");
            let value = i64::from_le_bytes(chunk);
            crate::py::arrow::i64_offset_to_usize(value)?;
            if let Some(lo) = prev
                && lo > value
            {
                return Err(crate::py::arrow::geoarrow_parse_error(
                    "Arrow offsets must be ordered",
                ));
            }
            prev = Some(value);
            terminal = value;
        }
        let terminal = crate::py::arrow::i64_offset_to_usize(terminal)?;
        crate::py::arrow::ensure_offset_terminal_within_child(terminal, child_len)?;
    } else {
        let mut prev: Option<i32> = None;
        let mut terminal: i32 = 0;
        for index in 0..=count {
            let byte = (window + index)
                .checked_mul(4)
                .ok_or_else(|| PyTypeError::new_err("Arrow offset index overflows"))?;
            let chunk = offsets[byte..byte + 4].try_into().expect("4-byte offset");
            let value = i32::from_le_bytes(chunk);
            crate::py::arrow::i32_offset_to_usize(value)?;
            if let Some(lo) = prev
                && lo > value
            {
                return Err(crate::py::arrow::geoarrow_parse_error(
                    "Arrow offsets must be ordered",
                ));
            }
            prev = Some(value);
            terminal = value;
        }
        let terminal = crate::py::arrow::i32_offset_to_usize(terminal)?;
        crate::py::arrow::ensure_offset_terminal_within_child(terminal, child_len)?;
    }
    Ok(())
}

/// Validate offset structure on a zero-row geometry array before a stream batch
/// is discarded (D18). Encoding selects binary vs list nesting depth so this
/// works after the stream schema has been moved into a capsule (no live schema
/// pointer required). Nested list children are walked for full physical windows.
pub(crate) fn ensure_zero_row_geometry_offsets(
    array: &ArrowArray,
    encoding: GeometryEncoding,
    wkb_offset_width: crate::py::arrow::WkbOffsetWidth,
) -> PyResult<()> {
    use crate::py::arrow::WkbOffsetWidth;
    match encoding {
        GeometryEncoding::Point => Ok(()),
        GeometryEncoding::Wkb => match wkb_offset_width {
            WkbOffsetWidth::View => Ok(()),
            WkbOffsetWidth::Int32 => {
                let validated =
                    ValidatedArrowArray::new(ptr::from_ref(array), ArrowFormat::Binary, 0)?;
                // Arrow C omits explicit data-buffer sizes (producer-trusted
                // capacity). Bound is unbounded here; PyArrow frontends pass
                // real buffer lengths via ensure_i32_offsets_monotonic.
                ensure_validated_array_offsets_monotonic(&validated, usize::MAX)
            },
            WkbOffsetWidth::Int64 => {
                let validated =
                    ValidatedArrowArray::new(ptr::from_ref(array), ArrowFormat::LargeBinary, 0)?;
                ensure_validated_array_offsets_monotonic(&validated, usize::MAX)
            },
        },
        GeometryEncoding::MultiPoint | GeometryEncoding::LineString => {
            ensure_list_offset_chain_depth(array, 1)
        },
        GeometryEncoding::MultiLineString | GeometryEncoding::Polygon => {
            ensure_list_offset_chain_depth(array, 2)
        },
        GeometryEncoding::MultiPolygon => ensure_list_offset_chain_depth(array, 3),
    }
}

/// Walk `depth` nested list arrays, validating each level's full offset window
/// (including empty start slots and null-hidden child ranges). Terminal offset
/// at each level is bounded by the child array's physical length (N2).
fn ensure_list_offset_chain_depth(array: &ArrowArray, depth: usize) -> PyResult<()> {
    let mut current = ptr::from_ref(array);
    for level in 0..depth {
        let validated = ValidatedArrowArray::new(current, ArrowFormat::List, 1)?;
        let raw = validated.raw()?;
        if raw.children.is_null() {
            return Err(PyTypeError::new_err("Arrow list children pointer is null"));
        }
        // SAFETY: List layout requires exactly one child.
        let child = unsafe { *raw.children };
        if child.is_null() {
            return Err(PyTypeError::new_err("Arrow list child array is null"));
        }
        // SAFETY: child pointer non-null; producer-owned for this batch.
        let child_len = usize::try_from(unsafe { (*child).length }).map_err(|_| {
            PyTypeError::new_err("Arrow list child length is negative or too large")
        })?;
        ensure_validated_array_offsets_monotonic(&validated, child_len)?;
        if level + 1 == depth {
            return Ok(());
        }
        current = child;
    }
    Ok(())
}

fn native_binary_data_len(array: &ValidatedArrowArray) -> PyResult<usize> {
    if !matches!(array.format, ArrowFormat::Binary | ArrowFormat::LargeBinary) {
        return Ok(0);
    }
    let width = array.format.offset_width().unwrap_or(4);
    if array.n_buffers < 2 {
        return Ok(0);
    }
    let raw = array.raw()?;
    // SAFETY: `buffers` is valid for the imported array lifetime.
    let offsets_ptr = unsafe { *raw.buffers.add(1) };
    if offsets_ptr.is_null() {
        return Ok(0);
    }
    let slots = array
        .end
        .checked_add(1)
        .ok_or_else(|| PyTypeError::new_err("Arrow binary offset count overflows"))?;
    let bytes = checked_byte_span(slots, width, "Arrow binary offsets length overflows")?;
    if bytes < width {
        return Ok(0);
    }
    // SAFETY: offsets buffer length was schema-derived with checked arithmetic.
    let offsets = unsafe { std::slice::from_raw_parts(offsets_ptr.cast::<u8>(), bytes) };
    let last = if width == 8 {
        let chunk = offsets[bytes - 8..bytes].try_into().expect("8-byte tail");
        i64::from_le_bytes(chunk)
    } else {
        let chunk = offsets[bytes - 4..bytes].try_into().expect("4-byte tail");
        i64::from(i32::from_le_bytes(chunk))
    };
    let data_len = usize::try_from(last).map_err(|_| {
        // Negative terminal is an offset defect (D18), not a type layout issue.
        crate::py::arrow::geoarrow_parse_error("Arrow offsets must be non-negative")
    })?;
    checked_byte_span(data_len, 1, "Arrow binary data length overflows")
}

/// Build a native Arrow view for one stream batch. The stream owns its schema
/// for the whole iteration, so this borrows that schema and moves only the
/// batch's fresh array capsule.
pub(crate) fn native_arrow_from_capsules_with_borrowed_schema(
    py: Python<'_>,
    schema_capsule: &Bound<'_, PyAny>,
    array_capsule: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let owner = ImportedCapsules::new_with_borrowed_schema(schema_capsule, array_capsule)?;
    native_arrow_from_owner(py, owner)
}

fn native_arrow_from_owner(py: Python<'_>, owner: Arc<ImportedCapsules>) -> PyResult<Py<PyAny>> {
    let has_extension = {
        let root = owner.schema.as_ref()?;
        schema_metadata_value(root, b"ARROW:extension:name")?.is_some()
    };
    NativeNode::root(owner).into_py_any(py, has_extension)
}

/// Classify a direct Arrow-C array capsule through the **same** schema
/// classifier as streams, then build storage. No frontend may read buffers
/// without this admission.
pub(crate) fn geometries_from_native_capsules_classified(
    py: Python<'_>,
    schema_capsule: &Bound<'_, PyAny>,
    array_capsule: &Bound<'_, PyAny>,
    crs: Option<&Bound<'_, PyAny>>,
    epoch: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyAny>> {
    let owner = ImportedCapsules::new(schema_capsule, array_capsule)?;
    let classified = classify_stream_geometry_schema(owner.schema.as_ref()?)?;
    let has_extension = {
        let root = owner.schema.as_ref()?;
        schema_metadata_value(root, b"ARROW:extension:name")?.is_some()
    };
    let root = NativeNode::root(owner).into_py_any(py, has_extension)?;
    let geometry = match classified.struct_child {
        Some(index) => native_arrow_struct_child(py, root.bind(py), index)?,
        None => root,
    };
    let storage = arrow_storage_from_native_geometry(
        geometry.bind(py),
        classified.encoding,
        classified.wkb_offset_width,
        classified.crs,
        classified.epoch,
    )?;
    geometries_from_arrow_storages(py, vec![storage], crs, epoch)
}

/// Run the shared encoding-storage classifier against a native array's schema.
pub(crate) fn validate_native_encoding_storage(
    value: &Bound<'_, PyAny>,
    encoding: GeometryEncoding,
) -> PyResult<()> {
    let array = value.cast::<NativeArrowArray>().map_err(|_| {
        PyTypeError::new_err("expected a native Arrow array for encoding storage validation")
    })?;
    let schema = array.get().node.schema()?;
    validate_encoding_storage(schema, encoding)
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
    let schema = array.get().node.schema()?;
    let Ok((encoding, width, crs, epoch)) = classify_geometry_array_schema(schema) else {
        // Fall through to the legacy attribute walk when the node is a bare
        // intermediate (e.g. storage child) without extension/binary.
        return Ok(None);
    };
    Ok(Some(arrow_storage_from_native_geometry(
        value, encoding, width, crs, epoch,
    )?))
}

/// Extract struct child `index` as a geometry array node, re-rooting so the
/// child's own schema metadata (extension name / CRS) is visible.
///
/// Parent (and deeper ancestor) struct nulls are OR'd into the child's
/// effective missing mask — a null parent row must never resurrect child
/// payload as a present geometry.
pub(crate) fn native_arrow_struct_child(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    index: usize,
) -> PyResult<Py<PyAny>> {
    let array = value.cast::<NativeArrowArray>().map_err(|_| {
        PyTypeError::new_err("expected a native Arrow array for stream struct child extraction")
    })?;
    let child = array.get().node.struct_field(index)?;
    // Re-root: table field metadata lives on the child schema, not the struct root.
    // Keep the ancestor-OR missing mask produced by `struct_field`.
    let re_rooted = NativeNode {
        owner: child.owner,
        array: child.array,
        schema: child.schema,
        root_schema: child.schema,
        effective_missing: child.effective_missing,
    };
    let has_extension = {
        let schema = re_rooted.schema.as_ref()?;
        schema_metadata_value(schema, b"ARROW:extension:name")?.is_some()
    };
    re_rooted.into_py_any(py, has_extension)
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

/// OR every ancestor struct's validity (and the child's own) into one
/// visible-window missing mask. Shared by capsule and stream struct-child
/// extraction — do not re-implement per path.
fn combine_struct_ancestor_missing(
    parent: &NativeNode,
    child: &ValidatedArrowArray,
) -> PyResult<Option<Arc<[bool]>>> {
    let len = child.length;
    if len != parent.array.length {
        return Err(PyTypeError::new_err(
            "Arrow struct child visible length does not match parent",
        ));
    }
    let mut missing = vec![false; len];
    let mut any = false;

    // Parent effective (already includes its ancestors) or parent own bitmap.
    if let Some(ref parent_missing) = parent.effective_missing {
        if parent_missing.len() != len {
            return Err(PyTypeError::new_err(
                "Arrow ancestor missing mask length does not match visible length",
            ));
        }
        for (slot, &is_missing) in missing.iter_mut().zip(parent_missing.iter()) {
            if is_missing {
                *slot = true;
                any = true;
            }
        }
    } else {
        for (row, slot) in missing.iter_mut().enumerate() {
            if !validated_row_is_valid(&parent.array, row)? {
                *slot = true;
                any = true;
            }
        }
    }

    // Child own validity for the same visible window.
    for (row, slot) in missing.iter_mut().enumerate() {
        if *slot {
            continue;
        }
        if !validated_row_is_valid(child, row)? {
            *slot = true;
            any = true;
        }
    }

    if any {
        Ok(Some(Arc::from(missing)))
    } else {
        Ok(None)
    }
}

/// Whether logical row `row` (0..length) is valid under this array's own
/// validity bitmap, respecting `array.offset`. Known-zero null_count is a
/// fast path that never touches the bitmap pointer.
fn validated_row_is_valid(array: &ValidatedArrowArray, row: usize) -> PyResult<bool> {
    match array.null_count {
        NullCount::Known(0) => Ok(true),
        NullCount::Known(_) | NullCount::Unknown => {
            if array.n_buffers == 0 {
                return Ok(true);
            }
            let raw = array.raw()?;
            // SAFETY: layout requires a non-null buffer table when n_buffers > 0.
            let validity_ptr = unsafe { *raw.buffers.add(0) };
            if validity_ptr.is_null() {
                if matches!(array.null_count, NullCount::Known(n) if n > 0) {
                    return Err(PyTypeError::new_err(
                        "Arrow validity bitmap is required when null_count > 0",
                    ));
                }
                return Ok(true);
            }
            let bit = array
                .offset
                .checked_add(row)
                .ok_or_else(|| PyTypeError::new_err("Arrow validity index overflows"))?;
            let byte_index = bit / 8;
            let validity_len = validity_buffer_len(array);
            if byte_index >= validity_len {
                return Err(PyTypeError::new_err(
                    "Arrow validity bitmap is shorter than declared",
                ));
            }
            // SAFETY: byte_index is in-range for the schema-sized validity span.
            let byte = unsafe { *validity_ptr.cast::<u8>().add(byte_index) };
            Ok((byte & (1 << (bit % 8))) != 0)
        },
    }
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
mod layout_validation_tests {
    use std::ptr;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use pyo3::ffi;

    use super::*;

    static MOVED_SCHEMA_RELEASES: AtomicUsize = AtomicUsize::new(0);
    static MOVED_ARRAY_RELEASES: AtomicUsize = AtomicUsize::new(0);

    unsafe extern "C" fn count_moved_schema_release(schema: *mut ArrowSchema) {
        MOVED_SCHEMA_RELEASES.fetch_add(1, Ordering::Relaxed);
        // SAFETY: the test callback receives the consumer-owned moved shell.
        unsafe { (*schema).release = None };
    }

    unsafe extern "C" fn count_moved_array_release(array: *mut ArrowArray) {
        MOVED_ARRAY_RELEASES.fetch_add(1, Ordering::Relaxed);
        // SAFETY: the test callback receives the consumer-owned moved shell.
        unsafe { (*array).release = None };
    }

    fn ensure_python() {
        // `NullCount::parse` and friends construct `PyErr` on failure paths.
        pyo3::Python::initialize();
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

    fn f64_ok(
        length: i64,
        offset: i64,
        null_count: i64,
        bufs: &[*const c_void],
    ) -> ValidatedArrowArray {
        let array = stack_array(
            length,
            offset,
            null_count,
            2,
            0,
            bufs.as_ptr(),
            ptr::null_mut(),
        );
        ValidatedArrowArray::new(&raw const array, ArrowFormat::Float64, 0).unwrap()
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
    fn arrow_format_parse_is_fallible_rejects_other_and_large_list() {
        ensure_python();
        assert_eq!(ArrowFormat::parse("g").unwrap(), ArrowFormat::Float64);
        assert_eq!(ArrowFormat::parse("z").unwrap(), ArrowFormat::Binary);
        assert_eq!(ArrowFormat::parse("Z").unwrap(), ArrowFormat::LargeBinary);
        assert_eq!(ArrowFormat::parse("vz").unwrap(), ArrowFormat::BinaryView);
        assert_eq!(ArrowFormat::parse("+l").unwrap(), ArrowFormat::List);
        assert_eq!(ArrowFormat::parse("+s").unwrap(), ArrowFormat::Struct);
        // Defect: int8 "c" tagged geoarrow.wkb used to size-guess as Other.
        assert!(ArrowFormat::parse("c").is_err());
        assert!(ArrowFormat::parse("i").is_err());
        // Defect: LargeList accepted but decoded as i32 offsets.
        assert!(ArrowFormat::parse("+L").is_err());
        assert_eq!(ArrowFormat::Binary.offset_width(), Some(4));
        assert_eq!(ArrowFormat::LargeBinary.offset_width(), Some(8));
        assert_eq!(ArrowFormat::List.offset_width(), Some(4));
        assert_eq!(ArrowFormat::Float64.offset_width(), None);
    }

    #[test]
    fn validity_buffer_sized_from_offset_plus_length_never_null_count() {
        ensure_python();
        // end = 10 → ceil(10/8) = 2, independent of any null_count value.
        // Use Known(0) so construction does not require a real validity bitmap.
        let bufs = dummy_buffers(2);
        let validated = f64_ok(5, 5, 0, &bufs);
        assert_eq!(validated.end, 10);
        assert_eq!(validity_buffer_len(&validated), 2);
        // Struct: unknown nulls, non-zero offset.
        let bufs = dummy_buffers(1);
        let array = stack_array(1, 15, -1, 1, 0, bufs.as_ptr(), ptr::null_mut());
        let validated = ValidatedArrowArray::new(&raw const array, ArrowFormat::Struct, 0).unwrap();
        assert_eq!(validated.null_count, NullCount::Unknown);
        assert_eq!(validated.end, 16);
        assert_eq!(validity_buffer_len(&validated), 2);
        // Empty binary: end = 0 → zero bytes; still exact 3-buffer layout.
        let bufs = dummy_buffers(3);
        let empty = stack_array(0, 0, 0, 3, 0, bufs.as_ptr(), ptr::null_mut());
        let validated = ValidatedArrowArray::new(&raw const empty, ArrowFormat::Binary, 0).unwrap();
        assert_eq!(validity_buffer_len(&validated), 0);
    }

    #[test]
    fn validated_array_rejects_excessive_null_count_at_construction() {
        ensure_python();
        let bufs = dummy_buffers(2);
        let array = stack_array(3, 0, 4, 2, 0, bufs.as_ptr(), ptr::null_mut());
        assert!(ValidatedArrowArray::new(&raw const array, ArrowFormat::Float64, 0).is_err());
        let array = stack_array(3, 0, -2, 2, 0, bufs.as_ptr(), ptr::null_mut());
        assert!(ValidatedArrowArray::new(&raw const array, ArrowFormat::Float64, 0).is_err());
    }

    #[test]
    fn known_null_count_must_match_visible_validity_bitmap() {
        ensure_python();
        // null_count=1 but validity bit set (all valid) — P02 mismatch.
        let validity: [u8; 1] = [0b0000_0001];
        let data = [0_u8; 8];
        let buffers: [*const c_void; 2] = [validity.as_ptr().cast(), data.as_ptr().cast()];
        let array = stack_array(1, 0, 1, 2, 0, buffers.as_ptr(), ptr::null_mut());
        let err = ValidatedArrowArray::new(&raw const array, ArrowFormat::Float64, 0);
        let message = match err {
            Ok(_) => panic!("expected null_count/bitmap mismatch to be rejected"),
            Err(error) => error.to_string(),
        };
        assert!(
            message.contains("null_count") && message.contains("validity bitmap"),
            "unexpected error: {message}"
        );
        // Matching pair: null_count=1, bit0 clear.
        let validity_null: [u8; 1] = [0];
        let buffers: [*const c_void; 2] = [validity_null.as_ptr().cast(), data.as_ptr().cast()];
        let array = stack_array(1, 0, 1, 2, 0, buffers.as_ptr(), ptr::null_mut());
        assert!(ValidatedArrowArray::new(&raw const array, ArrowFormat::Float64, 0).is_ok());
    }

    #[test]
    fn validated_array_accepts_unknown_null_count() {
        ensure_python();
        let bufs = dummy_buffers(3);
        let array = stack_array(4, 0, -1, 3, 0, bufs.as_ptr(), ptr::null_mut());
        let validated = ValidatedArrowArray::new(&raw const array, ArrowFormat::Binary, 0).unwrap();
        assert_eq!(validated.null_count, NullCount::Unknown);
        assert_eq!(validated.null_count.as_i64(), -1);
    }

    #[test]
    fn layout_rejects_wrong_buffer_and_child_cardinalities() {
        ensure_python();
        // Float64 with excess n_buffers (pointer-table OOB path).
        let bufs = dummy_buffers(3);
        let array = stack_array(1, 0, 0, 3, 0, bufs.as_ptr(), ptr::null_mut());
        assert!(ValidatedArrowArray::new(&raw const array, ArrowFormat::Float64, 0).is_err());
        // Binary with too few buffers.
        let bufs = dummy_buffers(2);
        let array = stack_array(1, 0, 0, 2, 0, bufs.as_ptr(), ptr::null_mut());
        assert!(ValidatedArrowArray::new(&raw const array, ArrowFormat::Binary, 0).is_err());
        // Schema/array child-count mismatch.
        let bufs = dummy_buffers(2);
        let array = stack_array(1, 0, 0, 2, 0, bufs.as_ptr(), ptr::null_mut());
        assert!(ValidatedArrowArray::new(&raw const array, ArrowFormat::Float64, 1).is_err());
        // BinaryView now requires validity + views + mandatory sizes table.
        assert!(ValidatedArrowArray::new(&raw const array, ArrowFormat::BinaryView, 0).is_err());
    }

    #[test]
    fn imported_capsules_move_once_and_release_the_owned_shells() {
        ensure_python();
        let schema_before = MOVED_SCHEMA_RELEASES.load(Ordering::Relaxed);
        let array_before = MOVED_ARRAY_RELEASES.load(Ordering::Relaxed);
        Python::attach(|py| {
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
            let buffers = [ptr::null(), ptr::null()];
            let mut array = ArrowArray {
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
            let owner = ImportedCapsules::new(&schema_capsule, &array_capsule).unwrap();
            assert!(schema.release.is_none());
            assert!(array.release.is_none());
            assert!(ImportedCapsules::new(&schema_capsule, &array_capsule).is_err());
            drop(owner);
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
    fn imported_capsules_stream_batches_borrow_the_shared_schema() {
        ensure_python();
        let schema_before = MOVED_SCHEMA_RELEASES.load(Ordering::Relaxed);
        let array_before = MOVED_ARRAY_RELEASES.load(Ordering::Relaxed);
        Python::attach(|py| {
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
            let first_array_capsule = unsafe {
                Bound::<PyAny>::from_owned_ptr(
                    py,
                    ffi::PyCapsule_New(
                        ptr::from_mut(&mut first_array).cast(),
                        array_capsule_name(),
                        None,
                    ),
                )
            };
            // SAFETY: see the schema capsule immediately above.
            let second_array_capsule = unsafe {
                Bound::<PyAny>::from_owned_ptr(
                    py,
                    ffi::PyCapsule_New(
                        ptr::from_mut(&mut second_array).cast(),
                        array_capsule_name(),
                        None,
                    ),
                )
            };
            let first =
                ImportedCapsules::new_with_borrowed_schema(&schema_capsule, &first_array_capsule)
                    .unwrap();
            assert!(schema.release.is_some());
            assert!(first_array.release.is_none());
            drop(first);
            let second =
                ImportedCapsules::new_with_borrowed_schema(&schema_capsule, &second_array_capsule)
                    .unwrap();
            assert!(schema.release.is_some());
            assert!(second_array.release.is_none());
            drop(second);
            // The stream retains and releases its schema after all batches;
            // this test performs that final release explicitly.
            release_imported_schema(&raw mut schema);
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
        let child = stack_array(4, 0, 0, 2, 0, bufs.as_ptr(), ptr::null_mut());
        let err =
            ValidatedArrowArray::struct_child(&raw const child, ArrowFormat::Float64, 2, 3, 0);
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
        let child = stack_array(10, 0, 6, 2, 0, bufs.as_ptr(), ptr::null_mut());
        let validated =
            ValidatedArrowArray::struct_child(&raw const child, ArrowFormat::Float64, 2, 3, 0)
                .unwrap();
        assert_eq!(validated.length, 3);
        assert_eq!(validated.null_count, NullCount::Unknown);
        // Known(0) is preserved under projection.
        let child = stack_array(10, 0, 0, 2, 0, bufs.as_ptr(), ptr::null_mut());
        let validated =
            ValidatedArrowArray::struct_child(&raw const child, ArrowFormat::Float64, 2, 3, 0)
                .unwrap();
        assert_eq!(validated.null_count, NullCount::Known(0));
        // Full-view (parent covers raw range) preserves the known count.
        // Bitmap must match: 6 nulls of 10 rows (bits 0..5 clear, 6..9 set).
        let validity: [u8; 2] = [0b1100_0000, 0b0000_0011];
        let data = [0_u8; 80];
        let buffers: [*const c_void; 2] = [validity.as_ptr().cast(), data.as_ptr().cast()];
        let child = stack_array(10, 0, 6, 2, 0, buffers.as_ptr(), ptr::null_mut());
        let validated =
            ValidatedArrowArray::struct_child(&raw const child, ArrowFormat::Float64, 0, 10, 0)
                .unwrap();
        assert_eq!(validated.null_count, NullCount::Known(6));
    }

    #[test]
    fn struct_child_exposes_only_the_parent_slice() {
        ensure_python();
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
            let mut schema = ArrowSchema {
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
            // SAFETY: these capsules borrow stack-owned shells for this test;
            // their release callbacks only clear the moved shell.
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
            let owner = ImportedCapsules::new(&schema_capsule, &array_capsule).unwrap();
            let output = NativeArrowArray::from_node(
                NativeNode::root(owner).struct_field(0).unwrap(),
                false,
            );
            assert_eq!(output.__len__(), 3);
            assert_eq!(output.offset(), 3);
            let bytes = output.buffer(py, 1).unwrap();
            let bytes = bytes.bind(py).cast::<PyBytes>().unwrap().as_bytes();
            assert_eq!(bytes.len(), 48);
            assert_eq!(
                f64::from_ne_bytes(bytes[24..32].try_into().unwrap()).to_bits(),
                3.0_f64.to_bits()
            );
        });
    }

    #[test]
    fn binary_offset_buffer_length_is_i32_width_slots() {
        ensure_python();
        let bufs = dummy_buffers(3);
        let array = stack_array(3, 1, 0, 3, 0, bufs.as_ptr(), ptr::null_mut());
        let validated = ValidatedArrowArray::new(&raw const array, ArrowFormat::Binary, 0).unwrap();
        assert_eq!(validated.end, 4);
        assert_eq!(native_buffer_len(&validated, 0).unwrap(), 1);
        assert_eq!(native_buffer_len(&validated, 1).unwrap(), 20);
    }

    #[test]
    fn large_binary_offset_buffer_length_is_i64_width_slots() {
        ensure_python();
        let bufs = dummy_buffers(3);
        let array = stack_array(3, 1, 0, 3, 0, bufs.as_ptr(), ptr::null_mut());
        let validated =
            ValidatedArrowArray::new(&raw const array, ArrowFormat::LargeBinary, 0).unwrap();
        assert_eq!(native_buffer_len(&validated, 1).unwrap(), 40);
        let f64_bufs = dummy_buffers(2);
        let f64_v = f64_ok(3, 1, 0, &f64_bufs);
        assert_eq!(native_buffer_len(&f64_v, 1).unwrap(), 32);
    }

    #[test]
    fn list_and_struct_buffer_layouts() {
        ensure_python();
        let bufs = dummy_buffers(2);
        // List requires exactly one child; null children pointer is rejected.
        let list = stack_array(2, 0, 0, 2, 1, bufs.as_ptr(), ptr::null_mut());
        assert!(ValidatedArrowArray::new(&raw const list, ArrowFormat::List, 1).is_err());
        // Real child pointer table.
        let mut child = stack_array(0, 0, 0, 2, 0, bufs.as_ptr(), ptr::null_mut());
        let mut child_ptrs = [ptr::from_mut(&mut child)];
        let list_ok = stack_array(2, 0, 0, 2, 1, bufs.as_ptr(), child_ptrs.as_mut_ptr());
        let list_v = ValidatedArrowArray::new(&raw const list_ok, ArrowFormat::List, 1).unwrap();
        assert_eq!(native_buffer_len(&list_v, 1).unwrap(), 12); // (2+1)*4

        let strukt_bufs = dummy_buffers(1);
        let strukt = stack_array(2, 0, 0, 1, 0, strukt_bufs.as_ptr(), ptr::null_mut());
        let strukt_v = ValidatedArrowArray::new(&raw const strukt, ArrowFormat::Struct, 0).unwrap();
        assert_eq!(native_buffer_len(&strukt_v, 0).unwrap(), 1);
        assert_eq!(native_buffer_len(&strukt_v, 1).unwrap(), 0);
    }

    #[test]
    fn binary_data_len_reads_terminal_offset_at_matching_width() {
        ensure_python();
        let offsets: [i32; 3] = [0, 3, 7];
        let buffers: [*const c_void; 3] = [ptr::null(), offsets.as_ptr().cast(), ptr::null()];
        let array = stack_array(2, 0, 0, 3, 0, buffers.as_ptr(), ptr::null_mut());
        let validated = ValidatedArrowArray::new(&raw const array, ArrowFormat::Binary, 0).unwrap();
        assert_eq!(native_buffer_len(&validated, 2).unwrap(), 7);

        let large_offsets: [i64; 3] = [0, 10, 25];
        let large_buffers: [*const c_void; 3] =
            [ptr::null(), large_offsets.as_ptr().cast(), ptr::null()];
        let large = stack_array(2, 0, 0, 3, 0, large_buffers.as_ptr(), ptr::null_mut());
        let large_v =
            ValidatedArrowArray::new(&raw const large, ArrowFormat::LargeBinary, 0).unwrap();
        assert_eq!(native_buffer_len(&large_v, 2).unwrap(), 25);
    }

    #[test]
    fn binary_data_len_rejects_negative_terminal_offset() {
        ensure_python();
        let offsets: [i32; 2] = [0, -1];
        let buffers: [*const c_void; 3] = [ptr::null(), offsets.as_ptr().cast(), ptr::null()];
        let array = stack_array(1, 0, 0, 3, 0, buffers.as_ptr(), ptr::null_mut());
        let validated = ValidatedArrowArray::new(&raw const array, ArrowFormat::Binary, 0).unwrap();
        assert!(native_buffer_len(&validated, 2).is_err());
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
        // Reject when end*8 exceeds isize::MAX via native_buffer_len.
        let bufs = dummy_buffers(2);
        let mid = (isize::MAX as usize / 8) + 1;
        let array = stack_array(mid as i64, 0, 0, 2, 0, bufs.as_ptr(), ptr::null_mut());
        if let Ok(validated) = ValidatedArrowArray::new(&raw const array, ArrowFormat::Float64, 0) {
            assert!(native_buffer_len(&validated, 1).is_err());
        }
    }

    #[test]
    fn multirow_binary_and_large_binary_offset_slot_count() {
        ensure_python();
        let bufs = dummy_buffers(3);
        let array = stack_array(5, 0, 0, 3, 0, bufs.as_ptr(), ptr::null_mut());
        let binary = ValidatedArrowArray::new(&raw const array, ArrowFormat::Binary, 0).unwrap();
        assert_eq!(native_buffer_len(&binary, 1).unwrap(), 24);
        let large =
            ValidatedArrowArray::new(&raw const array, ArrowFormat::LargeBinary, 0).unwrap();
        assert_eq!(native_buffer_len(&large, 1).unwrap(), 48);
        let shifted = stack_array(3, 2, 0, 3, 0, bufs.as_ptr(), ptr::null_mut());
        let shifted_v =
            ValidatedArrowArray::new(&raw const shifted, ArrowFormat::Binary, 0).unwrap();
        assert_eq!(shifted_v.offset, 2);
        assert_eq!(shifted_v.length, 3);
        assert_eq!(native_buffer_len(&shifted_v, 1).unwrap(), 24);
    }

    #[test]
    fn binary_view_view_buffer_is_16_bytes_per_slot_to_end() {
        ensure_python();
        let bufs = dummy_buffers(3);
        let array = stack_array(2, 1, -1, 3, 0, bufs.as_ptr(), ptr::null_mut());
        let validated =
            ValidatedArrowArray::new(&raw const array, ArrowFormat::BinaryView, 0).unwrap();
        assert_eq!(validated.null_count, NullCount::Unknown);
        assert_eq!(validated.end, 3);
        assert_eq!(native_buffer_len(&validated, 0).unwrap(), 1);
        assert_eq!(native_buffer_len(&validated, 1).unwrap(), 48);
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
        let array = stack_array(1, 0, 0, 3, 0, buffers.as_ptr(), ptr::null_mut());
        assert!(ValidatedArrowArray::new(&raw const array, ArrowFormat::BinaryView, 0).is_err());
    }

    #[test]
    fn zero_length_skips_binary_view_descriptor_scan() {
        ensure_python();
        let bufs = dummy_buffers(3);
        let array = stack_array(0, 0, -1, 3, 0, bufs.as_ptr(), ptr::null_mut());
        let validated =
            ValidatedArrowArray::new(&raw const array, ArrowFormat::BinaryView, 0).unwrap();
        assert_eq!(validated.length, 0);
        assert_eq!(validated.null_count, NullCount::Unknown);
        let ends = validated.binary_view_data_ends.as_ref().unwrap();
        assert!(ends.is_empty(), "sparse empty map, not dense n_buffers-3");
        // Empty BinaryView: zero-length slots yield empty buffers, not missing.
        assert_eq!(native_buffer_len(&validated, 1).unwrap(), 0);
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
        // endpoint cache remains sparse instead of mirroring that table.
        let array = stack_array(0, 0, 0, n_buffers as i64, 0, bufs.as_ptr(), ptr::null_mut());
        let validated =
            ValidatedArrowArray::new(&raw const array, ArrowFormat::BinaryView, 0).unwrap();
        let ends = validated.binary_view_data_ends.as_ref().unwrap();
        assert!(ends.is_empty());
        assert_eq!(native_buffer_len(&validated, 2).unwrap(), 0);
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
        let array = stack_array(1, 0, 0, 3, 0, buffers.as_ptr(), ptr::null_mut());
        assert!(ValidatedArrowArray::new(&raw const array, ArrowFormat::BinaryView, 0).is_err());
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
        let array = stack_array(2, 0, 0, 6, 0, buffers.as_ptr(), ptr::null_mut());
        let validated =
            ValidatedArrowArray::new(&raw const array, ArrowFormat::BinaryView, 0).unwrap();
        assert_eq!(native_buffer_len(&validated, 2).unwrap(), 20);
        assert_eq!(native_buffer_len(&validated, 3).unwrap(), 0); // unused
        assert_eq!(native_buffer_len(&validated, 4).unwrap(), 19);
        let ends = validated.binary_view_data_ends.as_ref().unwrap();
        assert_eq!(ends.len(), 2);
        assert_eq!(ends.get(&0).copied(), Some(20));
        assert_eq!(ends.get(&2).copied(), Some(19));
        assert!(!ends.contains_key(&1));
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
        let array = stack_array(1, 0, 1, 4, 0, buffers.as_ptr(), ptr::null_mut());
        let validated =
            ValidatedArrowArray::new(&raw const array, ArrowFormat::BinaryView, 0).unwrap();
        // Null descriptor must not enlarge the data buffer endpoint.
        assert_eq!(native_buffer_len(&validated, 2).unwrap(), 0);
        let ends = validated.binary_view_data_ends.as_ref().unwrap();
        // Sparse: null-only arrays leave the endpoint map empty (absent ≡ 0).
        assert!(ends.is_empty());
    }

    #[test]
    fn binary_view_present_descriptor_bounds_and_caches_endpoint() {
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
        let array = stack_array(1, 0, 0, 4, 0, buffers.as_ptr(), ptr::null_mut());
        let validated =
            ValidatedArrowArray::new(&raw const array, ArrowFormat::BinaryView, 0).unwrap();
        assert_eq!(native_buffer_len(&validated, 2).unwrap(), 22);
        // O(1) cache: second lookup must match without re-scan side effects.
        assert_eq!(native_buffer_len(&validated, 2).unwrap(), 22);
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
        let array = stack_array(1, 0, 0, 4, 0, buffers.as_ptr(), ptr::null_mut());
        assert!(ValidatedArrowArray::new(&raw const array, ArrowFormat::BinaryView, 0).is_err());
    }

    #[test]
    fn empty_binary_and_float64_zero_length_buffers() {
        ensure_python();
        let bufs = dummy_buffers(3);
        let empty_bin = stack_array(0, 0, 0, 3, 0, bufs.as_ptr(), ptr::null_mut());
        let binary =
            ValidatedArrowArray::new(&raw const empty_bin, ArrowFormat::Binary, 0).unwrap();
        // Offsets still need one slot for the terminal 0; data length is 0.
        assert_eq!(native_buffer_len(&binary, 1).unwrap(), 4);
        assert_eq!(native_buffer_len(&binary, 2).unwrap(), 0);

        let f64_bufs = dummy_buffers(2);
        let empty_f64 = stack_array(0, 0, 0, 2, 0, f64_bufs.as_ptr(), ptr::null_mut());
        let f64v = ValidatedArrowArray::new(&raw const empty_f64, ArrowFormat::Float64, 0).unwrap();
        assert_eq!(native_buffer_len(&f64v, 1).unwrap(), 0);
    }
}
