//! Native Arrow C Data Interface admission into owned Arrow batches.
use std::ffi::c_void;
use std::ptr;
use std::sync::Arc;

use pyo3::exceptions::{PyOverflowError, PyTypeError};
use pyo3::prelude::{Bound, PyAny, PyResult};

use crate::collections::{HashMap, HashMapExt as _};
use crate::crs::frame;
use crate::geometry::storage;
use crate::geometry::storage::validity::union_many;
use crate::py::arrow::WkbOffsetWidth;
use crate::py::arrow_c::admitted::{
    AdmittedArrowSchema, ArrowReleaseGuard, MovedArrowShell, classify_admitted_geometry_schema,
};
use crate::py::arrow_c::foreign_buffer::ForeignArrowBuffer;
use crate::py::arrow_c::{
    ArrowArray, ArrowSchema, ClassifiedGeometrySchema, admitted, arc_from_str, arc_from_vec,
    arc_new, array_capsule_name, capsule_pointer, schema_capsule_name,
};
use crate::py::geoarrow::GeometryEncoding;
use crate::py::{arrow, arrow_c, errors};

mod binary_view;
pub(super) mod buffers;
pub(super) mod capture;
pub(super) mod decode;
mod layout;
mod node;

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
    null_count: layout::NullCount,
    n_buffers: usize,
    n_children: usize,
    format: layout::ArrowFormat,
    /// Producer buffer table; live only while the capture shell is held.
    buffers: *const *const c_void,
    /// Producer children table; live only while the capture shell is held.
    children: *mut *mut ArrowArray,
    /// Selected logical child span from an owned list-offset window.
    child_window: Option<(usize, usize)>,
}

/// Owned native array node: buffers + children snapshotted at admission.
/// No producer shells, no raw buffer pointers after construction.
struct NativeNode {
    length: usize,
    /// Every owned payload is rebased to its visible logical origin.
    /// The physical bit alignment retained with the copied validity bytes.
    validity_offset: usize,
    n_buffers: usize,
    format: layout::ArrowFormat,
    buffers: buffers::OwnedBufferSlots,
    #[expect(
        clippy::rc_buffer,
        reason = "the sharing handle must remain Arc<Vec<T>> so capacity stays part of heap accounting"
    )]
    children: Arc<Vec<Self>>,
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
    /// Combined ancestor and self validity, sharing owned Arrow bitmap bytes.
    effective_missing: Option<storage::ValidityMask>,
}

/// Classification resolved at the attached ingress boundary and carried with
/// one fully-owned batch.  `struct_child` deliberately does not live here: it
/// is only an admission instruction for table/record-batch roots.
#[derive(Clone, Debug)]
pub(crate) struct NativeArrowClassification {
    pub(crate) encoding: GeometryEncoding,
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
#[path = "native/tests.rs"]
mod layout_validation_tests;
