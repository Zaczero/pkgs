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

use std::ffi::{CString, c_char, c_int, c_void};
use std::sync::Arc;

use pyo3::exceptions::PyTypeError;
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyCapsule, PyTuple};
use serde_json::{Map, Number, Value};

use crate::geometry::{CoordSeq, CoordinateAxes, Polygon, Shape};
use crate::py::errors::{GeometryError, ParseError};
use crate::py::geoarrow::GeometryEncoding;
use crate::{PyGeometry, PyGeometryArray, crs_arc};

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
    last_error: CString,
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
mod foreign_buffer;
mod import;
mod native;
mod release;
#[cfg(test)]
mod tests;

pub(crate) use admitted::{ClassifiedGeometrySchema, admit_and_classify_raw_schema};
pub(crate) use build::{
    GometryArrowArray, apply_top_level_validity, binary_array, coordinate_array, coordinate_schema,
    empty_array, extension_schema, list_array, list_array_windowed, list_schema, wkb_schema,
};
pub(crate) use capsule_lifecycle::{
    ArrowReleaseSlot, array_to_array_capsules, array_to_schema_capsule, array_to_stream_capsule,
    capsule_destructor, geometry_to_array_capsules, geometry_to_schema_capsule,
    geometry_to_stream_capsule, owned_capsule, release_imported,
};
pub(crate) use export::{
    export_from_geometries, export_from_geometry_array, schema_from_geometries,
    schema_from_geometry_array,
};
pub(crate) use import::{capsule_pointer, geometries_from_arrow_c, release_imported_stream};
pub(crate) use native::{
    geometries_from_native_capsules_classified, is_native_arrow_array,
    native_arrow_effective_missing, native_arrow_from_array_capsule_with_schema,
    native_arrow_storage_via_classifier, native_arrow_validity_offset,
    native_schema_format_is_binary, native_schema_format_is_binary_view,
    native_schema_format_is_large_binary, register_native_arrow_classes,
    try_native_admitted_buffer, try_native_f64_values_arc, validate_native_encoding_root_format,
};
pub(crate) use release::{
    array_capsule_destructor, release_array, release_schema, release_stream,
    schema_capsule_destructor, stream_capsule_destructor, stream_get_last_error, stream_get_next,
    stream_get_schema,
};
