#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use std::ffi::{CStr, c_int, c_void};
use std::ptr;

use pyo3::exceptions::PyBufferError;
use pyo3::prelude::*;
use pyo3::{Bound, ffi};

struct TypedBufferBacking {
    dims: [isize; 2],
}

/// Fill a 1-D typed, read-only buffer view over live contiguous storage.
///
/// # Safety
/// `view` must be a valid buffer-request pointer and `data`/`count`/
/// `itemsize` must describe the owner's live storage.
pub(crate) unsafe fn fill_typed_view(
    view: *mut ffi::Py_buffer,
    flags: c_int,
    data: *const c_void,
    count: usize,
    itemsize: usize,
    format: &'static CStr,
    owner: Bound<'_, PyAny>,
    readonly_message: &'static str,
) -> PyResult<()> {
    if view.is_null() {
        return Err(PyBufferError::new_err("view is null"));
    }
    if (flags & ffi::PyBUF_WRITABLE) == ffi::PyBUF_WRITABLE {
        return Err(PyBufferError::new_err(readonly_message));
    }
    let len = count
        .checked_mul(itemsize)
        .and_then(|len| isize::try_from(len).ok())
        .ok_or_else(|| PyBufferError::new_err("buffer is too large for CPython"))?;
    let count = isize::try_from(count)
        .map_err(|_| PyBufferError::new_err("buffer is too large for CPython"))?;
    let itemsize = isize::try_from(itemsize)
        .map_err(|_| PyBufferError::new_err("buffer item size is too large for CPython"))?;
    // SAFETY: caller contract; `view.obj` keeps the owner-backed storage alive.
    unsafe {
        (*view).obj = owner.into_ptr();
        (*view).buf = data.cast_mut();
        (*view).len = len;
        (*view).readonly = 1;
        (*view).itemsize = itemsize;
        (*view).ndim = 1;
        let backing = Box::into_raw(Box::new(TypedBufferBacking {
            dims: [count, itemsize],
        }));
        (*view).internal = backing.cast();
        (*view).shape = if (flags & ffi::PyBUF_ND) == ffi::PyBUF_ND {
            (&raw mut (*backing).dims[0]).cast()
        } else {
            ptr::null_mut()
        };
        (*view).strides = if (flags & ffi::PyBUF_STRIDES) == ffi::PyBUF_STRIDES {
            (&raw mut (*backing).dims[1]).cast()
        } else {
            ptr::null_mut()
        };
        (*view).format = if (flags & ffi::PyBUF_FORMAT) == ffi::PyBUF_FORMAT {
            // A `&'static CStr` literal: no allocation per acquisition, and
            // `release_typed_view` must NOT free it.
            format.as_ptr().cast_mut()
        } else {
            ptr::null_mut()
        };
        (*view).suboffsets = ptr::null_mut();
    }
    Ok(())
}

/// # Safety
/// `view` must have been filled by [`fill_typed_view`].
pub(crate) unsafe fn release_typed_view(view: *mut ffi::Py_buffer) {
    // SAFETY: `internal` is exactly the raw allocation made by
    // `fill_typed_view` (or null); `format` is a `&'static CStr` and is never
    // freed.
    unsafe {
        if !(*view).internal.is_null() {
            drop(Box::from_raw((*view).internal.cast::<TypedBufferBacking>()));
        }
    }
}
