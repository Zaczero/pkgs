//! libPROJ environment hooks for [`super::context`].
//!
//! Production wires a fresh private context constructor plus destroy/config to
//! bundled PROJ. Compile-fail fixtures stub this module so `context.rs` can be
//! `#[path]`-included under bare `rustc` without pulling `proj-sys` or the rest
//! of the CRS tree.

use crate::crs::runtime::apply_runtime_config;
use crate::crs::{CrsError, ProjContext, RuntimeState};
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
    unsafe { proj_sys::gometry_proj_context_create() }
}

pub(super) fn enable_default_angular_units(context: &ProjContext) -> Result<()> {
    // SAFETY: context is a fresh live owner on its creating thread. This is
    // called once before CRS operation resolution and never for literal work.
    // SAFETY: `context` is the fresh live owner on this thread described above.
    if unsafe { proj_sys::gometry_enable_default_angular_units(context.as_ptr()) } {
        Ok(())
    } else {
        Err(CrsError::invalid(
            "bundled PROJ could not enable default angular-unit semantics".to_owned(),
        ))
    }
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

pub(super) fn create_failed() -> Error {
    CrsError::invalid("PROJ context creation returned null".to_owned())
}

pub(super) fn prepare_with_state(context: &ProjContext, state: &RuntimeState) -> Result<()> {
    apply_runtime_config(context, &state.config)
}
