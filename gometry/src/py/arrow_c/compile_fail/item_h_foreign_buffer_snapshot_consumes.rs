//! Compile-fail fragment: [`ForeignArrowBuffer::snapshot`] consumes its view.
//! Path-includes the real production module, so changing `snapshot(self)` to
//! `snapshot(&self)` makes this fragment compile and turns the gate red.
//!
//! Expected: E0382 — use of moved value: `foreign`.
//!
//! Mutation proof (mandatory): change only production `snapshot(self)` to
//! `snapshot(&self)` → this fragment compiles →
//! `compile_fail_fixtures_guard_production` goes red. Revert and rebuild.

#[path = "../foreign_buffer.rs"]
mod foreign_buffer;

use foreign_buffer::ForeignArrowBuffer;

fn main() {
    let owner = ();
    // SAFETY: a zero-length span may use a null pointer. The fixture checks
    // ownership of the view, not producer-memory validity.
    let foreign = unsafe { ForeignArrowBuffer::new(std::ptr::null(), 0, &owner) };
    let _first = unsafe { foreign.snapshot() };
    // ERROR: use of moved value: `foreign`
    let _second = unsafe { foreign.snapshot() };
}
