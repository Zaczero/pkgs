//! Arrow `PyCapsule` C Data Interface export/import.
//!
//! This module owns the raw ABI edge for `__arrow_c_schema__`,
//! `__arrow_c_array__`, and `__arrow_c_stream__`. Keep the unsafe surface
//! boring: every C struct has one `private_data` box that owns all memory the
//! raw pointers reference, and release callbacks are idempotent.
#![allow(
    clippy::unnecessary_box_returns,
    clippy::vec_box,
    reason = "C Data Interface child pointers require stable pointee addresses"
)]

use std::ffi::{CStr, CString, c_char, c_int, c_void};
use std::sync::Arc;

use pyo3::exceptions::PyOverflowError;
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyTuple};
use serde_json::{Map, Number, Value};

use crate::geometry::{CoordSeq, CoordinateAxes};
use crate::py::errors::GeometryError;
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
            Self::GeoArrow(message) => crate::py::errors::parse_error(
                message.into_string(),
                crate::error::ParseFormat::GeoArrow,
            ),
            Self::Geometry(message) => {
                crate::py::errors::GeometryError::new_err(message.into_string())
            },
            Self::InvalidGeometry(message) => {
                crate::py::errors::InvalidGeometryError::new_err(message.into_string())
            },
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
    format: CString,
    name: CString,
    metadata: Option<Vec<u8>>,
    children: Vec<Box<ArrowSchema>>,
    child_ptrs: Vec<*mut ArrowSchema>,
}

struct ArrayPrivate {
    _f64_buffers: Vec<Arc<[f64]>>,
    _i32_buffers: Vec<Arc<[i32]>>,
    u8_buffers: Vec<Arc<[u8]>>,
    buffers: Vec<*const c_void>,
    children: Vec<Box<ArrowArray>>,
    child_ptrs: Vec<*mut ArrowArray>,
}

struct StreamPrivate {
    schema: SchemaNode,
    array: Option<Box<ArrowArray>>,
    last_error: &'static CStr,
}

#[derive(Clone)]
pub(crate) struct SchemaNode {
    format: &'static str,
    name: &'static str,
    metadata: Option<Vec<(String, String)>>,
    children: Vec<Self>,
}

pub(crate) struct ExportedArray {
    schema: Box<ArrowSchema>,
    schema_node: SchemaNode,
    array: GometryArrowArray,
}

struct ImportedStreamGuard {
    stream: *mut ArrowArrayStream,
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

pub(crate) use admitted::{ClassifiedGeometrySchema, admit_and_classify_raw_schema};
pub(crate) use build::{
    GometryArrowArray, admitted_binary_array, admitted_list_array_windowed,
    apply_prebuilt_top_level_validity, coordinate_array, coordinate_schema, empty_array,
    extension_schema, list_schema, wkb_schema,
};
pub(crate) use capsule_lifecycle::{
    ArrowReleaseSlot, array_to_array_capsules, array_to_schema_capsule, array_to_stream_capsule,
    capsule_destructor, geometry_to_array_capsules, geometry_to_schema_capsule,
    geometry_to_stream_capsule, pyarrow_array_from_export, release_imported,
};
pub(crate) use export::{apply_exported_validity_plan, export_with_schema_node};
pub(crate) use export_plan::{
    PreparedArrowExportPlan, materialize_export_plan, materialize_schema_plan, plan_geometry_array,
    plan_shapes, plan_wkb_shapes, prepare_export_plan,
};
pub(crate) use import::{capsule_pointer, geometries_from_arrow_c, release_imported_stream};
pub(crate) use native::{
    NativeArrowBatch, NativeArrowClassification, NativeArrowDecodeError, NativeDecodedBatch,
    native_arrow_batch_from_capsules, native_arrow_batch_from_capsules_with_classification,
    native_arrow_batch_from_raw_array,
};
pub(crate) use release::{
    array_capsule_destructor, release_array, release_schema, release_stream,
    schema_capsule_destructor, stream_capsule_destructor, stream_get_last_error, stream_get_next,
    stream_get_schema,
};
