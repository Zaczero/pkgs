//! Compile-fail fragment: ForeignArrowBuffer must not outlive its owner.
//! Includes the real production module so widening the lifetime (or dropping
//! the owner witness) turns this fixture green and the gate red.
//!
//! Expected: E0515 — cannot return value referencing local variable `owner`.

#[path = "../foreign_buffer.rs"]
mod foreign_buffer;

use foreign_buffer::ForeignArrowBuffer;

fn escape() -> ForeignArrowBuffer<'static> {
    let owner = ();
    // SAFETY: zero-length span; the lifetime of the returned value is what
    // this fixture checks, not the pointer validity.
    unsafe { ForeignArrowBuffer::new(std::ptr::null(), 0, &owner) }
}

fn main() {
    let _ = escape();
}
