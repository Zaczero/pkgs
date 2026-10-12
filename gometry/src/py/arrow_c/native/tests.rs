use std::sync::atomic::{AtomicUsize, Ordering};

use pyo3::ffi;
use pyo3::prelude::Python;

use super::buffers::{OwnedBuffer, OwnedBufferSlots, checked_byte_span, snapshot_native_buffers};
use super::capture::{native_arrow_batch_from_capsules, native_arrow_batch_from_raw_array};
use super::decode::NativeDecodedStorage;
use super::layout::{ArrowFormat, NullCount};
use super::*;

include!("tests/support.rs");
include!("tests/layout.rs");
include!("tests/capture.rs");
include!("tests/buffers.rs");
include!("tests/binary_view.rs");
include!("tests/decode.rs");
