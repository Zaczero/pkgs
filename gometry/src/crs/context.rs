//! RAII [`ProjContext`] ownership and scoped context access for CRS operations.
//!
//! [`ProjContext`] retains the unique libPROJ context and its diagnostic state,
//! then destroys both through the creating thread's lifecycle. Creation,
//! destruction, and runtime configuration are delegated to
//! [`super::proj_env`]; callers use the scoped helpers to borrow the typed
//! owner.
//!
//! # Type-level contract
//!
//! Safe crate-internal APIs take `&ProjContext`. `with_proj_context` lends that
//! borrow only for the closure, so the reference cannot outlive its owner.
//! `ProjContext::as_ptr` exposes a raw pointer for a final FFI expression; raw
//! pointers are not lifetime-tracked, so every later dereference or use must
//! uphold the owner's lifetime, creating-thread, and pointer-validity
//! obligations in its unsafe FFI boundary.

use std::ptr::NonNull;

use super::proj_env::{self, ContextRaw, EnvResult};
use super::{RuntimeState, runtime_state_snapshot};

/// Diagnostic storage owned by one PROJ context.
///
/// PROJ invokes its log callback synchronously on the context's creating
/// thread. Keeping the slot with the context makes a transform execution's
/// diagnostic explicit instead of routing it through process-thread TLS.
pub(crate) struct ProjDiagnostic {
    message: std::cell::RefCell<Option<String>>,
}

impl ProjDiagnostic {
    pub(crate) const fn new() -> Self {
        Self {
            message: std::cell::RefCell::new(None),
        }
    }

    pub(crate) fn clear(&self) {
        *self.message.borrow_mut() = None;
    }

    pub(crate) fn take(&self) -> Option<String> {
        self.message.borrow_mut().take()
    }

    pub(crate) fn store(&self, message: String) {
        if let Ok(mut slot) = self.message.try_borrow_mut() {
            *slot = Some(message);
        }
    }
}

#[cfg(test)]
type ContextObservation = std::sync::Arc<std::sync::Mutex<Option<(u64, Option<Vec<String>>)>>>;
#[cfg(test)]
type ContextCreationHook = (
    std::sync::Arc<std::sync::Barrier>,
    std::sync::Arc<std::sync::Barrier>,
    Option<ContextObservation>,
);
#[cfg(test)]
thread_local! {
    static CONTEXT_CREATION_HOOK: std::cell::RefCell<Option<ContextCreationHook>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
pub(super) fn install_context_creation_state_hook(
    entered: std::sync::Arc<std::sync::Barrier>,
    resume: std::sync::Arc<std::sync::Barrier>,
    observed: ContextObservation,
) {
    CONTEXT_CREATION_HOOK.with(|hook| *hook.borrow_mut() = Some((entered, resume, Some(observed))));
}

#[cfg(test)]
fn run_context_creation_hook(state: &RuntimeState) {
    let hook = CONTEXT_CREATION_HOOK.with(|hook| hook.borrow_mut().take());
    if let Some((entered, resume, observed)) = hook {
        entered.wait();
        resume.wait();
        if let Some(observed) = observed {
            *observed
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) =
                Some((state.generation, state.config.search_paths.clone()));
        }
    }
}

/// Typed owner of a libPROJ `PJ_CONTEXT`.
///
/// Safe APIs receive `&ProjContext`. Raw pointers are for the final documented
/// FFI expression only — never a safe crate-internal parameter type.
pub(crate) struct ProjContext {
    raw: NonNull<ContextRaw>,
    diagnostic: Box<ProjDiagnostic>,
}

impl ProjContext {
    #[cfg(test)]
    pub(crate) fn new() -> EnvResult<Self> {
        let state = runtime_state_snapshot()?;
        Self::new_with_state(&state)
    }

    pub(crate) fn new_with_state(state: &RuntimeState) -> EnvResult<Self> {
        #[cfg(test)]
        run_context_creation_hook(state);
        // SAFETY: `context_create` returns null or a uniquely owned context on
        // this thread; we adopt into the RAII owner before any fallible work.
        let ptr = unsafe { proj_env::context_create() };
        let raw = NonNull::new(ptr).ok_or_else(proj_env::create_failed)?;
        let context = Self {
            raw,
            diagnostic: Box::new(ProjDiagnostic::new()),
        };
        proj_env::prepare_with_state(&context, state)?;
        Ok(context)
    }

    /// Raw context pointer for a final FFI expression.
    ///
    /// The pointer is not lifetime-tracked. Callers must keep any dereference
    /// or FFI use within this owner's lifetime and creating thread and uphold
    /// the pointer-validity contract of that FFI operation.
    pub(crate) const fn as_ptr(&self) -> *mut ContextRaw {
        self.raw.as_ptr()
    }

    pub(crate) fn diagnostic_ptr(&self) -> *mut std::ffi::c_void {
        std::ptr::from_ref::<ProjDiagnostic>(self.diagnostic.as_ref()) as *mut std::ffi::c_void
    }

    pub(crate) fn clear_diagnostic(&self) {
        self.diagnostic.clear();
    }

    pub(crate) fn take_diagnostic(&self) -> Option<String> {
        self.diagnostic.take()
    }
}

impl Drop for ProjContext {
    fn drop(&mut self) {
        // SAFETY: unique ownership; destroy exactly once on the creating thread.
        unsafe {
            proj_env::context_destroy(self.raw.as_ptr());
        }
    }
}

/// Run `operation` with a short-lived typed context.
///
/// The closure receives a borrow that cannot outlive the context owner. A
/// closure may copy a raw pointer obtained from [`ProjContext::as_ptr`], but
/// that raw pointer does not extend the owner's lifetime; any later use is an
/// unsafe FFI obligation of the caller.
pub(crate) fn with_proj_context<R>(operation: impl FnOnce(&ProjContext) -> R) -> EnvResult<R> {
    let state = runtime_state_snapshot()?;
    with_proj_context_at(&state, operation)
}

pub(crate) fn with_proj_context_at<R>(
    state: &RuntimeState,
    operation: impl FnOnce(&ProjContext) -> R,
) -> EnvResult<R> {
    let context = ProjContext::new_with_state(state)?;
    Ok(operation(&context))
}
