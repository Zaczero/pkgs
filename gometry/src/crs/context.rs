//! Production [`ProjContext`] and [`with_proj_context`] (R13-L3 keystone).
//!
//! Free of `proj-sys` so compile-fail fixtures can `#[path]`-include this
//! module. FFI create/destroy/config ride [`super::proj_env`], which production
//! wires to libPROJ and the fixture stubs.
//!
//! # Type-level contract
//!
//! Safe crate-internal APIs take `&ProjContext`. A caller that extracts a raw
//! pointer via [`ProjContext::as_ptr`] cannot pass that raw pointer to a typed
//! consumer without **E0308**. Compile-fail `escaping_context` pins this.

use std::ptr::NonNull;

use super::proj_env::{self, ContextRaw, EnvResult};

#[cfg(test)]
thread_local! {
    static CONTEXT_CREATION_HOOK: std::cell::RefCell<Option<(
        std::sync::Arc<std::sync::Barrier>,
        std::sync::Arc<std::sync::Barrier>,
    )>> = const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
pub(super) fn install_context_creation_hook(
    entered: std::sync::Arc<std::sync::Barrier>,
    resume: std::sync::Arc<std::sync::Barrier>,
) {
    CONTEXT_CREATION_HOOK.with(|hook| *hook.borrow_mut() = Some((entered, resume)));
}

#[cfg(test)]
fn run_context_creation_hook() {
    let hook = CONTEXT_CREATION_HOOK.with(|hook| hook.borrow_mut().take());
    if let Some((entered, resume)) = hook {
        entered.wait();
        resume.wait();
    }
}

/// Typed owner of a libPROJ `PJ_CONTEXT`.
///
/// Safe APIs receive `&ProjContext`. Raw pointers are for the final documented
/// FFI expression only — never a safe crate-internal parameter type.
pub(crate) struct ProjContext {
    raw: NonNull<ContextRaw>,
}

impl ProjContext {
    /// Create a configured context via [`proj_env`].
    pub(crate) fn new() -> EnvResult<Self> {
        #[cfg(test)]
        run_context_creation_hook();
        // SAFETY: `context_create` returns null or a uniquely owned context on
        // this thread; we adopt into the RAII owner before any fallible work.
        let ptr = unsafe { proj_env::context_create() };
        let raw = NonNull::new(ptr).ok_or_else(proj_env::create_failed)?;
        let context = Self { raw };
        proj_env::prepare(&context)?;
        Ok(context)
    }

    /// Raw context pointer for the final FFI expression only.
    pub(crate) const fn as_ptr(&self) -> *mut ContextRaw {
        self.raw.as_ptr()
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
/// The closure receives `&ProjContext` so a safe crate-internal caller cannot
/// return a raw `*mut PJ_CONTEXT` and use it after the owner drops.
pub(crate) fn with_proj_context<R>(operation: impl FnOnce(&ProjContext) -> R) -> EnvResult<R> {
    let context = ProjContext::new()?;
    Ok(operation(&context))
}

/// Typed-consumer gate used by the `escaping_context` compile-fail fixture.
///
/// Named regression: change the parameter to `*mut ContextRaw` (the type
/// [`ProjContext::as_ptr`] returns). The fixture passes exactly that raw type,
/// so the regression makes the fragment **compile** and the compile-fail gate
/// goes red. A different raw type (e.g. `*mut c_void`) is the wrong regression
/// — it still yields E0308 for the wrong reason.
#[expect(
    dead_code,
    reason = "compile-fail fixture consumer; production callers take &ProjContext directly"
)]
pub(crate) const fn require_typed_proj_context(_context: &ProjContext) {}
