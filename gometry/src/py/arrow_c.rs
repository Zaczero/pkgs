//! Arrow `PyCapsule` C Data Interface export/import.
//!
//! This module owns the raw ABI edge for `__arrow_c_schema__`,
//! `__arrow_c_array__`, and `__arrow_c_stream__`. Keep the unsafe surface
//! boring: every C struct has one `private_data` box that owns all memory the
//! raw pointers reference, and release callbacks are idempotent.

use std::ffi::{CStr, c_char, c_int, c_void};
use std::sync::Arc;

use pyo3::exceptions::PyOverflowError;
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyCapsule, PyTuple};

use crate::geometry::{CoordSeq, CoordinateAxes};
use crate::py::errors;
use crate::py::errors::{GeometryError, InvalidGeometryError};
use crate::py::geoarrow::GeometryEncoding;
use crate::{PyGeometry, PyGeometryArray};

/// Native failures produced while planning an Arrow export. No variant owns
/// Python exception state; conversion is deliberately restricted to the
/// attached capsule/PyArrow egress.
#[derive(Debug)]
pub(crate) enum NativeArrowExportError {
    Core(crate::error::Error),
    CoreRow {
        row: usize,
        error: crate::error::Error,
    },
    Overflow(Box<str>),
    GeoArrow(Box<str>),
    Geometry(Box<str>),
    InvalidGeometry(Box<str>),
}

pub(crate) type NativeArrowExportResult<T> = std::result::Result<T, NativeArrowExportError>;

/// Share an owned vector while retaining its capacity for native heap accounting.
#[expect(
    clippy::rc_buffer,
    reason = "the sharing handle must remain Arc<Vec<T>> so capacity stays part of heap accounting"
)]
pub(crate) fn arc_from_vec<T>(values: Vec<T>) -> Arc<Vec<T>> {
    Arc::new(values)
}

/// Share an owned string while retaining its capacity for native heap accounting.
#[expect(
    clippy::rc_buffer,
    reason = "the sharing handle must remain Arc<String> so capacity stays part of heap accounting"
)]
pub(crate) fn arc_from_string(value: String) -> Arc<String> {
    Arc::new(value)
}

#[expect(
    clippy::rc_buffer,
    reason = "the sharing handle must remain Arc<String> so capacity stays part of heap accounting"
)]
pub(crate) fn arc_from_str(value: &str) -> Arc<String> {
    arc_from_string(value.to_owned())
}

pub(crate) fn arc_new<T>(value: T) -> Arc<T> {
    Arc::new(value)
}

impl From<crate::error::Error> for NativeArrowExportError {
    fn from(error: crate::error::Error) -> Self {
        Self::Core(error)
    }
}

impl NativeArrowExportError {
    pub(crate) fn overflow(message: impl Into<Box<str>>) -> Self {
        Self::Overflow(message.into())
    }

    pub(crate) fn geoarrow(message: impl Into<Box<str>>) -> Self {
        Self::GeoArrow(message.into())
    }

    pub(crate) fn geometry(message: impl Into<Box<str>>) -> Self {
        Self::Geometry(message.into())
    }

    pub(crate) fn invalid_geometry(message: impl Into<Box<str>>) -> Self {
        Self::InvalidGeometry(message.into())
    }

    pub(crate) fn into_py_err(self, _py: Python<'_>) -> PyErr {
        match self {
            Self::Core(error) => error.into(),
            Self::CoreRow { row, error } => crate::broadcast::rows_err((row, error)),
            Self::Overflow(message) => PyOverflowError::new_err(message.into_string()),
            Self::GeoArrow(message) => {
                errors::parse_error(message.into_string(), crate::error::ParseFormat::GeoArrow)
            },
            Self::Geometry(message) => GeometryError::new_err(message.into_string()),
            Self::InvalidGeometry(message) => InvalidGeometryError::new_err(message.into_string()),
        }
    }
}

#[repr(C)]
pub(crate) struct ArrowSchema {
    format: *const c_char,
    name: *const c_char,
    metadata: *const c_char,
    flags: i64,
    n_children: i64,
    children: *mut *mut Self,
    dictionary: *mut Self,
    release: Option<unsafe extern "C" fn(*mut Self)>,
    private_data: *mut c_void,
}

#[repr(C)]
pub(crate) struct ArrowArray {
    length: i64,
    null_count: i64,
    offset: i64,
    n_buffers: i64,
    n_children: i64,
    buffers: *const *const c_void,
    children: *mut *mut Self,
    dictionary: *mut Self,
    release: Option<unsafe extern "C" fn(*mut Self)>,
    private_data: *mut c_void,
}

#[repr(C)]
pub(crate) struct ArrowArrayStream {
    get_schema: Option<unsafe extern "C" fn(*mut Self, *mut ArrowSchema) -> c_int>,
    get_next: Option<unsafe extern "C" fn(*mut Self, *mut ArrowArray) -> c_int>,
    get_last_error: Option<unsafe extern "C" fn(*mut Self) -> *const c_char>,
    release: Option<unsafe extern "C" fn(*mut Self)>,
    private_data: *mut c_void,
}

struct SchemaPrivate {
    format: Vec<u8>,
    name: Vec<u8>,
    metadata: Option<Vec<u8>>,
    children: Vec<GometryArrowSchema>,
    child_ptrs: Vec<*mut ArrowSchema>,
}

struct ArrayPrivate {
    owners: Vec<OwnedBufferSlot>,
    buffers: Vec<*const c_void>,
    _children: Vec<GometryArrowArray>,
    child_ptrs: Vec<*mut ArrowArray>,
}

struct StreamPrivate {
    schema: SchemaNode,
    array: Option<GometryArrowArray>,
    last_error: &'static CStr,
}

#[derive(Clone)]
pub(crate) struct SchemaNode {
    format: &'static str,
    name: &'static str,
    metadata: Option<Vec<(String, String)>>,
    nullable: bool,
    children: Vec<Self>,
}

pub(crate) const ARROW_FLAG_NULLABLE: i64 = 2;

impl SchemaNode {
    /// Compare an Arrow C schema request without consuming the caller's capsule.
    pub(crate) fn matches_requested_schema(
        &self,
        requested_schema: &Bound<'_, PyAny>,
    ) -> PyResult<Option<bool>> {
        if !requested_schema.is_instance_of::<PyCapsule>() {
            return Ok(None);
        }
        // SAFETY: `requested_schema` is a live Python object retained by `Bound`;
        // PyCapsule_IsValid checks its type/name without dereferencing its payload.
        if unsafe { ffi::PyCapsule_IsValid(requested_schema.as_ptr(), schema_capsule_name()) } == 0
        {
            return Ok(None);
        }
        // SAFETY: the capsule name proves this is a live ArrowSchema pointer.
        let schema = unsafe {
            ffi::PyCapsule_GetPointer(requested_schema.as_ptr(), schema_capsule_name())
                .cast::<ArrowSchema>()
        };
        if schema.is_null() {
            return Err(PyErr::fetch(requested_schema.py()));
        }
        // SAFETY: the borrowed capsule remains alive and its producer schema is
        // read-only for the duration of this comparison.
        unsafe { self.matches_schema_node(&*schema, true) }
    }

    /// Compare every schema field and metadata entry against an exported node.
    ///
    /// # Safety
    ///
    /// The producer owns readable strings, metadata, and children for the
    /// duration of this call, as required by the Arrow C Data Interface.
    unsafe fn matches_schema_node(
        &self,
        schema: &ArrowSchema,
        allow_missing_extension_metadata: bool,
    ) -> PyResult<Option<bool>> {
        if !c_schema_string_matches(schema.format, self.format)
            || !c_schema_string_matches(schema.name, self.name)
            || schema.flags
                != if self.nullable {
                    ARROW_FLAG_NULLABLE
                } else {
                    0
                }
            || !schema.dictionary.is_null()
        {
            return Ok(None);
        }

        let Ok(child_count) = usize::try_from(schema.n_children) else {
            return Ok(None);
        };
        if child_count != self.children.len() {
            return Ok(None);
        }
        // SAFETY: the caller retains the live schema capsule for the duration
        // of this comparison, so its producer-owned metadata remains readable.
        let actual_metadata = unsafe { import::schema_metadata_pairs_owned(schema)? };
        let mut actual_metadata = actual_metadata
            .into_iter()
            .map(|(key, value)| (key.into_vec(), value.into_vec()))
            .collect::<Vec<_>>();
        let mut expected_metadata = self
            .metadata
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(|(key, value)| (key.as_bytes().to_vec(), value.as_bytes().to_vec()))
            .collect::<Vec<_>>();
        actual_metadata.sort_unstable();
        expected_metadata.sort_unstable();
        // PyArrow's C schema request for a DataType carries its physical
        // structure but omits extension metadata. Treat that as a physical
        // schema request; when metadata is present, it must match exactly.
        let extension_metadata_omitted = allow_missing_extension_metadata
            && !expected_metadata.is_empty()
            && actual_metadata.is_empty()
            && expected_metadata
                .iter()
                .all(|(key, _)| key.starts_with(b"ARROW:extension:"));
        if actual_metadata != expected_metadata && !extension_metadata_omitted {
            return Ok(None);
        }
        if child_count == 0 {
            return Ok(Some(extension_metadata_omitted));
        }
        if schema.children.is_null() {
            return Ok(None);
        }
        // SAFETY: the Arrow producer keeps this initialized child-pointer table
        // alive under the borrowed schema capsule.
        let children =
            unsafe { std::slice::from_raw_parts(schema.children.cast_const(), child_count) };
        let mut omit_extension_metadata = extension_metadata_omitted;
        for (expected, &child) in self.children.iter().zip(children) {
            if child.is_null() {
                return Ok(None);
            }
            // SAFETY: each non-null child belongs to the live schema tree.
            let Some(child_omits_metadata) =
                (unsafe { expected.matches_schema_node(&*child, false) })?
            else {
                return Ok(None);
            };
            omit_extension_metadata |= child_omits_metadata;
        }
        Ok(Some(omit_extension_metadata))
    }

    pub(crate) fn without_extension_metadata(&self) -> Self {
        let mut schema = self.clone();
        if let Some(metadata) = &mut schema.metadata {
            metadata.retain(|(key, _)| !key.starts_with("ARROW:extension:"));
            if metadata.is_empty() {
                schema.metadata = None;
            }
        }
        schema
    }
}

fn c_schema_string_matches(value: *const c_char, expected: &str) -> bool {
    if value.is_null() {
        return expected.is_empty();
    }
    // SAFETY: Arrow C schema strings are NUL-terminated and live with the
    // producer schema for the duration of the comparison.
    unsafe { CStr::from_ptr(value) }.to_bytes() == expected.as_bytes()
}

pub(crate) struct ExportedArray {
    schema: GometryArrowSchema,
    schema_node: SchemaNode,
    array: GometryArrowArray,
}

pub(crate) const fn schema_capsule_name() -> *const c_char {
    c"arrow_schema".as_ptr()
}

pub(crate) const fn used_schema_capsule_name() -> *const c_char {
    c"used_arrow_schema".as_ptr()
}

pub(crate) const fn array_capsule_name() -> *const c_char {
    c"arrow_array".as_ptr()
}

pub(crate) const fn used_array_capsule_name() -> *const c_char {
    c"used_arrow_array".as_ptr()
}

pub(crate) const fn stream_capsule_name() -> *const c_char {
    c"arrow_array_stream".as_ptr()
}

pub(crate) const fn used_stream_capsule_name() -> *const c_char {
    c"used_arrow_array_stream".as_ptr()
}

mod admitted;
mod build;
mod capsule_lifecycle;
mod export;
mod export_plan;
mod foreign_buffer;
mod import;
mod native;
mod release;
#[cfg(test)]
mod tests;

pub(crate) use admitted::{
    ArrowReleaseGuard, ClassifiedGeometrySchema, MovedArrowShell, admit_and_classify_raw_schema,
};
pub(crate) use build::{
    GometryArrowArray, GometryArrowSchema, OwnedBufferSlot, admitted_binary_array,
    admitted_list_array_windowed, apply_prebuilt_top_level_validity, apply_top_level_validity,
    coordinate_array, coordinate_schema, empty_array, extension_schema, list_schema,
    primitive_u64_arc_array_windowed, primitive_u64_array_windowed, primitive_u64_schema,
    utf8_array, utf8_schema, wkb_schema,
};
pub(crate) use capsule_lifecycle::{
    ArrowReleaseSlot, array_capsules_with_schema, array_to_array_capsules, array_to_schema_capsule,
    array_to_stream_capsule, capsule_destructor, geometry_to_array_capsules,
    geometry_to_schema_capsule, geometry_to_stream_capsule, pyarrow_array_from_export,
};
pub(crate) use export::{apply_exported_validity_plan, export_with_schema_node};
#[cfg(test)]
pub(crate) use export_plan::materialize_schema_plan;
pub(crate) use export_plan::{
    PreparedArrowExportPlan, describe_geometry_array, describe_shape, materialize_export_plan,
    plan_geometry_array, plan_shapes, plan_wkb_shapes, prepare_export_plan,
};
#[cfg(test)]
pub(crate) use export_plan::{payload_events, reset_payload_events};
pub(crate) use import::{capsule_pointer, geometries_from_arrow_c};
pub(crate) use native::capture::{
    native_arrow_batch_from_capsules, native_arrow_batch_from_capsules_with_classification,
    native_arrow_batch_from_raw_array,
};
pub(crate) use native::decode::{NativeDecodedBatch, NativeDecodedStorage};
pub(crate) use native::{NativeArrowBatch, NativeArrowClassification, NativeArrowImportError};
pub(crate) use release::{
    array_capsule_destructor, release_array, release_schema, release_stream,
    schema_capsule_destructor, stream_capsule_destructor, stream_get_last_error, stream_get_next,
    stream_get_schema,
};
#[cfg(test)]
pub(crate) use release::{reset_schema_release_callbacks, schema_release_callbacks};
