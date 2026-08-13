//! libPROJ environment hooks for [`super::context`].
//!
//! Production wires create/destroy/config to bundled PROJ. Compile-fail
//! fixtures stub this module so `context.rs` can be `#[path]`-included under
//! bare `rustc` without pulling `proj-sys` or the rest of the CRS tree.

use crate::crs::runtime::{apply_runtime_config, runtime_config_snapshot};
use crate::crs::{CrsError, ProjContext};
use crate::error::{Error, Result};

/// Opaque raw context type — production is `proj_sys::PJ_CONTEXT`.
pub(super) type ContextRaw = proj_sys::PJ_CONTEXT;

/// Result alias used by [`super::context`] construction.
pub(super) type EnvResult<T> = Result<T>;

/// # Safety
///
/// Caller adopts the returned pointer (null or uniquely owned `PJ_CONTEXT` on
/// the creating thread) into [`ProjContext`] before any fallible work.
pub(super) unsafe fn context_create() -> *mut ContextRaw {
    // SAFETY: no pointer inputs; returns null or uniquely owned context.
    unsafe { proj_sys::proj_context_create() }
}

/// # Safety
///
/// `ptr` is the uniquely owned live context transferred into this destroy, on
/// the creating thread, exactly once.
pub(super) unsafe fn context_destroy(ptr: *mut ContextRaw) {
    // SAFETY: caller guarantees unique ownership on the creating thread.
    unsafe {
        proj_sys::proj_context_destroy(ptr);
    }
}

pub(super) fn cleanup_thread_context() {
    // SAFETY: PROJ cleans up only its current thread's default context. All
    // cached contexts are dropped before this runs.
    unsafe {
        proj_sys::proj_cleanup();
    }
}

pub(super) fn create_failed() -> Error {
    CrsError::invalid("PROJ context creation returned null".to_owned())
}

pub(super) fn prepare(context: &ProjContext) -> Result<()> {
    let config = runtime_config_snapshot()?;
    apply_runtime_config(context, &config)
}
