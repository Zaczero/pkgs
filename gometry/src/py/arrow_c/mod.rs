#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
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
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyCapsule, PyTuple};
use pyo3::{IntoPyObjectExt, ffi};
use serde_json::{Map, Number, Value};

use crate::geometry::{CoordSeq, CoordinateAxes, Polygon, Shape};
use crate::py::errors::{GeometryError, ParseError};
use crate::py::geoarrow::GeometryEncoding;
use crate::{PyGeometry, PyGeometryArray, common_crs_required, crs_arc, io};

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
    array: Box<ArrowArray>,
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

mod build;
mod capsule_lifecycle;
mod classify;
mod export;
mod import;
mod native;
mod release;
#[cfg(test)]
mod tests;

pub(crate) use build::*;
pub(crate) use capsule_lifecycle::*;
pub(crate) use classify::*;
pub(crate) use export::*;
pub(crate) use import::*;
pub(crate) use native::*;
pub(crate) use release::*;
