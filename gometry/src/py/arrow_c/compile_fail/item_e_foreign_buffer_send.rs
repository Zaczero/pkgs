//! Compile-fail fragment: ForeignArrowBuffer must not be Send.
//! Includes the real production module so a regression (making the type Send)
//! turns this fixture green and the gate red.
//!
//! What blocks Send: `NonNull<u8>` is neither Send nor Sync, and
//! `PhantomData<Rc<()>>` independently reinforces both negative auto traits.

#[path = "../foreign_buffer.rs"]
mod foreign_buffer;

use foreign_buffer::ForeignArrowBuffer;

fn require_send<T: Send>() {}

fn main() {
    // ERROR: Rc<()> is not Send
    require_send::<ForeignArrowBuffer<'static>>();
}
