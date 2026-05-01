// Encoded polylines use printable ASCII by adding '?' (63) to each chunk.
pub const ASCII_OFFSET: u8 = b'?';

// A chunk contains 5 bits of payload and may set a continuation bit.
pub const CHUNK_BITS: u32 = 5;
pub const CHUNK_MASK: u8 = 0b1_1111;

pub const CONTINUATION_BIT: u8 = 0x20;
