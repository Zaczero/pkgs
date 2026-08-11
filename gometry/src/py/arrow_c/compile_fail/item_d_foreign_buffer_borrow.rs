//! Compile-fail fragment: ForeignArrowBuffer must not expose as_slice.
//! Includes the real production module so a regression (adding as_slice)
//! turns this fixture green and the gate red.
//!
//! Expected: no method named `as_slice` (called via `unsafe` so E0133 cannot
//! mask the missing-method failure).

#[path = "../foreign_buffer.rs"]
mod foreign_buffer;

use foreign_buffer::ForeignArrowBuffer;

fn main() {
    let owner = ();
    // SAFETY: zero-length span with dangling pointer is allowed by new.
    let foreign = unsafe { ForeignArrowBuffer::new(std::ptr::null(), 0, &owner) };
    // ERROR: no method named `as_slice`
    let _: &[u8] = unsafe { foreign.as_slice() };
}
