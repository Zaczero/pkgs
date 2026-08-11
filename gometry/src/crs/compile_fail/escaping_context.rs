//! Compile-fail fragment for R13-L3 keystone.
//!
//! Path-includes the real production `context.rs` (not stand-in types). After
//! the type-level fix, `with_proj_context` lends `&ProjContext` and consumers
//! accept that typed borrow — not a raw handle. A safe crate-internal sequence
//! that stashes the raw pointer and feeds it to a typed consumer is rejected
//! at compile time.
//!
//! Expected: **E0308** mismatched types — raw `*mut ContextRaw` is not
//! `&ProjContext` (message/label must name `&ProjContext`).
//!
//! Mutation proof (mandatory): change production
//! `require_typed_proj_context` to take `*mut ContextRaw` (the SAME type
//! `as_ptr` returns) → this fixture **compiles** → compile-fail gate goes
//! RED. Revert.
//!
//! Shape rule: the value passed is exactly `*mut ContextRaw` from production
//! `as_ptr`. A widened raw-pointer signature must accept that type so
//! "compiled" is the failure signal — not a second, unrelated E0308.

/// Stub the external PROJ environment only. Guarded items
/// (`ProjContext`, `with_proj_context`, `require_typed_proj_context`) come from
/// the production module below — never redeclared here.
mod proj_env {
    use std::ptr::NonNull;

    pub type ContextRaw = u8;
    pub type EnvResult<T> = Result<T, String>;

    pub fn ensure_caches() {}

    /// Return a non-null dangling pointer so `NonNull::new` succeeds. Never
    /// dereferenced — type-check path only.
    pub unsafe fn context_create() -> *mut ContextRaw {
        NonNull::<ContextRaw>::dangling().as_ptr()
    }

    pub unsafe fn context_destroy(_: *mut ContextRaw) {}

    pub fn create_failed() -> String {
        "PROJ context creation returned null".to_owned()
    }

    pub fn prepare(_: &super::context::ProjContext) -> EnvResult<()> {
        Ok(())
    }
}

#[path = "../context.rs"]
mod context;

use context::{require_typed_proj_context, with_proj_context};

fn escape_and_use_after_drop() {
    // Keystone anti-pattern: escape the raw handle that production `as_ptr`
    // returns (`*mut ContextRaw`), then feed it to the typed consumer.
    //
    // Under the correct signature this is E0308 (`&ProjContext` expected).
    // Under the named regression (`*mut ContextRaw` parameter) this compiles
    // — that is the RED signal.
    let p: *mut proj_env::ContextRaw =
        with_proj_context(|c| c.as_ptr()).expect("stub create");
    // ERROR: expected `&ProjContext`, found `*mut ContextRaw`
    require_typed_proj_context(p);
}

fn main() {
    escape_and_use_after_drop();
}
