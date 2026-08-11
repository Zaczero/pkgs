//! Compile-fail fragment: ForeignArrowBuffer must not be Send.
//! Includes the real production module so a regression (making the type Send)
//! turns this fixture green and the gate red.
//!
//! What blocks Send: `PhantomData<Rc<()>>` (and independently `NonNull<u8>`
//! is Send but not Sync — Send is blocked by the Rc phantom).

#[path = "../foreign_buffer.rs"]
mod foreign_buffer;

use foreign_buffer::ForeignArrowBuffer;

fn require_send<T: Send>() {}

fn main() {
    // ERROR: Rc<()> is not Send
    require_send::<ForeignArrowBuffer<'static>>();
}
