//! Compile-fail fragment: ForeignArrowBuffer must not be Sync.
//! Includes the real production module so a regression (making the type Sync)
//! turns this fixture green and the gate red.
//!
//! What blocks Sync: `PhantomData<Rc<()>>` and independently `NonNull<u8>`
//! (NonNull is !Sync).

#[path = "../foreign_buffer.rs"]
mod foreign_buffer;

use foreign_buffer::ForeignArrowBuffer;

fn require_sync<T: Sync>() {}

fn main() {
    // ERROR: not Sync
    require_sync::<ForeignArrowBuffer<'static>>();
}
