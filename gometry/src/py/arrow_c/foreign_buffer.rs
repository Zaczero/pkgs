//! Temporary foreign Arrow buffer view used only during owned admission.
//!
//! This module is intentionally **std-only** so compile-fail fixtures can
//! `#[path]`-include the production type. It must never grow a slice accessor,
//! `Deref`, `AsRef`, `Index`, or `Send`/`Sync` implementation.

use std::collections::TryReserveError;
use std::marker::PhantomData;
use std::ptr::NonNull;
use std::rc::Rc;

/// Producer-backed buffer span that can only be **consumed** into an owned copy.
///
/// Neither `Send` nor `Sync`: `NonNull<u8>` is neither, and
/// `PhantomData<Rc<()>>` independently reinforces both. There is no `as_slice`
/// (or any other way to reborrow the producer memory as `&[u8]`).
pub(crate) struct ForeignArrowBuffer<'owner> {
    ptr: NonNull<u8>,
    len: usize,
    _owner: PhantomData<&'owner ()>,
    _not_send_sync: PhantomData<Rc<()>>,
}

impl<'owner> ForeignArrowBuffer<'owner> {
    /// Build a temporary foreign view over `ptr..ptr+len`.
    ///
    /// # Safety
    ///
    /// - **Provenance:** `ptr` comes from a live, ABI-conforming Arrow buffer
    ///   under `owner` (or is a zero-length null span).
    /// - **Capacity:** the addressed span is fully readable for `len` bytes,
    ///   and `len <= isize::MAX`.
    /// - **Lifetime:** `owner` pins the producer allocation; release,
    ///   reallocation, and unmapping cannot occur before `snapshot` returns.
    /// - **Threading:** no thread or native callback writes the addressed
    ///   buffer during capture.
    /// - **Suspension:** capture performs no nested provider call and does not
    ///   detach or otherwise suspend into Python between construction and
    ///   `snapshot`.
    /// - **Destination:** the fresh owned destination from `snapshot` cannot
    ///   overlap producer memory.
    /// - **Escape:** no reference, pointer accessor, `Deref`, `AsRef`, `Index`,
    ///   or slice backed by producer memory leaves capture — only the owned
    ///   `Vec<u8>` from `snapshot` may escape.
    pub(crate) unsafe fn new<Owner: ?Sized>(
        ptr: *const u8,
        len: usize,
        _owner: &'owner Owner,
    ) -> Self {
        debug_assert!(isize::try_from(len).is_ok());
        let ptr = if len == 0 {
            NonNull::dangling()
        } else {
            debug_assert!(!ptr.is_null());
            // SAFETY: caller guarantees a non-null readable base for len > 0.
            unsafe { NonNull::new_unchecked(ptr.cast_mut()) }
        };
        Self {
            ptr,
            len,
            _owner: PhantomData,
            _not_send_sync: PhantomData,
        }
    }

    /// Consume this foreign view into an owned byte vector.
    ///
    /// # Safety
    ///
    /// Same obligations as [`Self::new`] for the full lifetime of this value
    /// through the return of this method.
    pub(crate) unsafe fn snapshot(self) -> Result<Vec<u8>, TryReserveError> {
        if self.len == 0 {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        out.try_reserve_exact(self.len)?;
        // SAFETY: construction requires the full `len` span is readable and
        // quiescent for the capture; destination is a fresh exclusive Vec.
        unsafe {
            out.set_len(self.len);
            std::ptr::copy_nonoverlapping(self.ptr.as_ptr(), out.as_mut_ptr(), self.len);
        }
        Ok(out)
    }
}
