// Encoded polylines use printable ASCII by adding '?' (63) to each chunk.
pub(crate) const ASCII_OFFSET: u8 = b'?';

// A chunk contains 5 bits of payload and may set a continuation bit.
pub(crate) const CHUNK_BITS: u32 = 5;
pub(crate) const CHUNK_MASK: u8 = 0b1_1111;

pub(crate) const CONTINUATION_BIT: u8 = 0x20;
