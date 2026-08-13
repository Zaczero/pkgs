use bytes::{Buf, Bytes, BytesMut};

use crate::h2_frame::DEFAULT_HEADER_TABLE_SIZE;
use crate::hpack::dynamic_table::DynamicBuffer;
use crate::hpack::field::{DecodeBlockError, HpackField};
use crate::hpack::{huffman, static_table};

const HUFFMAN_ARENA_CAPACITY: usize = 4096;

#[derive(Debug)]
pub(crate) struct Decoder {
    max_dynamic_size: usize,
    table: DynamicBuffer<HpackField>,
    buffer: BytesMut,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) enum DecoderError {
    InvalidTableIndex,
    InvalidHuffmanCode,
    InvalidMaxDynamicSize,
    IntegerOverflow,
    NeedMore(NeedMore),
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) enum NeedMore {
    UnexpectedEndOfStream,
    IntegerUnderflow,
    StringUnderflow,
}

#[derive(Clone, Copy)]
enum Representation {
    Indexed,
    LiteralWithIndexing,
    LiteralWithoutIndexing,
    LiteralNeverIndexed,
    SizeUpdate,
}

impl Decoder {
    pub(crate) fn new(size: usize) -> Self {
        Self {
            max_dynamic_size: size,
            table: DynamicBuffer::new(size),
            buffer: BytesMut::new(),
        }
    }

    #[cfg(test)]
    pub(crate) fn decode_bytes<F>(&mut self, src: &mut Bytes, mut f: F) -> Result<(), DecoderError>
    where
        F: FnMut(HpackField),
    {
        match self.decode_block(src, |field| {
            f(field);
            Ok::<(), ()>(())
        }) {
            Ok(()) | Err(DecodeBlockError::Rejected(())) => Ok(()),
            Err(DecodeBlockError::Compression(err)) => Err(err),
        }
    }

    /// Decode a whole header block, calling `accept` for each field.
    ///
    /// A rejection from `accept` does not stop the decode. The dynamic table is
    /// shared by every stream on the connection, so abandoning a block
    /// half-read leaves this decoder disagreeing with the peer's encoder for
    /// the life of the connection: one stream answered 431 used to make the
    /// *next*, perfectly valid, stream fail with `COMPRESSION_ERROR`. The
    /// block is consumed in full; after the first rejection further semantic
    /// callbacks are skipped while table mutations continue. A later HPACK
    /// failure returns `Compression`, overriding any recorded rejection.
    pub(crate) fn decode_block<E>(
        &mut self,
        src: &mut Bytes,
        mut accept: impl FnMut(HpackField) -> Result<(), E>,
    ) -> Result<(), DecodeBlockError<E>> {
        let mut can_resize = true;
        let mut rejected: Option<E> = None;

        while Self::has_more(src) {
            let field = match self.decode_next(src, &mut can_resize) {
                Ok(Some(field)) => field,
                Ok(None) => continue,
                Err(err) => return Err(DecodeBlockError::Compression(err)),
            };

            if rejected.is_none()
                && let Err(err) = accept(field)
            {
                rejected = Some(err);
            }
        }

        rejected.map_or(Ok(()), |err| Err(DecodeBlockError::Rejected(err)))
    }

    fn has_more(src: &Bytes) -> bool {
        src.has_remaining()
    }

    /// Decode one representation. Returns `None` for size updates (no field).
    fn decode_next(
        &mut self,
        src: &mut Bytes,
        can_resize: &mut bool,
    ) -> Result<Option<HpackField>, DecoderError> {
        let Some(&byte) = src.chunk().first() else {
            return Ok(None);
        };

        match Representation::load(byte) {
            Representation::Indexed => {
                *can_resize = false;
                Ok(Some(self.decode_indexed(src)?))
            },
            Representation::LiteralWithIndexing => {
                *can_resize = false;
                let entry = self.decode_literal::<6>(src)?;
                // Insert before any semantic callback so table state tracks
                // successfully decoded bytes regardless of message validity.
                self.table.insert(entry.clone(), entry.table_size());
                Ok(Some(entry))
            },
            Representation::LiteralWithoutIndexing | Representation::LiteralNeverIndexed => {
                *can_resize = false;
                Ok(Some(self.decode_literal::<4>(src)?))
            },
            Representation::SizeUpdate => {
                if !*can_resize {
                    return Err(DecoderError::InvalidMaxDynamicSize);
                }
                self.process_size_update(src)?;
                Ok(None)
            },
        }
    }

    fn process_size_update(&mut self, buf: &mut Bytes) -> Result<(), DecoderError> {
        let new_size = decode_int::<5, _>(buf)?;
        if new_size > self.max_dynamic_size {
            return Err(DecoderError::InvalidMaxDynamicSize);
        }
        self.table.set_max_size(new_size);
        Ok(())
    }

    fn decode_indexed(&self, buf: &mut Bytes) -> Result<HpackField, DecoderError> {
        let index = decode_int::<7, _>(buf)?;
        get_indexed_entry(&self.table, index)
    }

    fn decode_literal<const PREFIX_SIZE: u8>(
        &mut self,
        buf: &mut Bytes,
    ) -> Result<HpackField, DecoderError> {
        let table_idx = decode_int::<PREFIX_SIZE, _>(buf)?;

        if table_idx == 0 {
            return self.decode_new_name_literal(buf);
        }

        let name = get_indexed_name(&self.table, table_idx)?;
        self.decode_indexed_name_literal(buf, name)
    }

    #[cfg(test)]
    fn decode_string(&mut self, buf: &mut Bytes) -> Result<Bytes, DecoderError> {
        Self::decode_string_into(&mut self.buffer, buf)
    }

    fn decode_string_into(buffer: &mut BytesMut, buf: &mut Bytes) -> Result<Bytes, DecoderError> {
        const HUFF_FLAG: u8 = 0x80;

        let Some(&first) = buf.chunk().first() else {
            return Err(DecoderError::NeedMore(NeedMore::UnexpectedEndOfStream));
        };
        let huffman = first & HUFF_FLAG != 0;
        let len = decode_int::<7, _>(buf)?;
        if len > buf.remaining() {
            return Err(DecoderError::NeedMore(NeedMore::StringUnderflow));
        }

        if !huffman {
            return Ok(buf.copy_to_bytes(len));
        }

        if buffer.capacity() == 0 {
            buffer.reserve(HUFFMAN_ARENA_CAPACITY);
        }
        let decoded = huffman::decode(&buf.chunk()[..len], buffer)?;
        buf.advance(len);
        Ok(decoded.freeze())
    }

    fn decode_new_name_literal(&mut self, buf: &mut Bytes) -> Result<HpackField, DecoderError> {
        let name = Self::decode_string_into(&mut self.buffer, buf)?;
        let value = Self::decode_string_into(&mut self.buffer, buf)?;
        Ok(HpackField::from_parts(name, value))
    }

    fn decode_indexed_name_literal(
        &mut self,
        buf: &mut Bytes,
        name: Bytes,
    ) -> Result<HpackField, DecoderError> {
        let value = Self::decode_string_into(&mut self.buffer, buf)?;
        Ok(HpackField::from_parts(name, value))
    }
}

impl Default for Decoder {
    fn default() -> Self {
        Self::new(DEFAULT_HEADER_TABLE_SIZE)
    }
}

impl Representation {
    const fn load(byte: u8) -> Self {
        match byte {
            0..16 => Self::LiteralWithoutIndexing,
            16..32 => Self::LiteralNeverIndexed,
            32..64 => Self::SizeUpdate,
            64..128 => Self::LiteralWithIndexing,
            128..=255 => Self::Indexed,
        }
    }
}

fn dynamic_entry(
    table: &DynamicBuffer<HpackField>,
    index: usize,
) -> Result<&HpackField, DecoderError> {
    let offset = index - static_table::DYNAMIC_INDEX_OFFSET;
    table
        .entry_from_end(offset)
        .ok_or(DecoderError::InvalidTableIndex)
}

fn get_indexed_entry(
    table: &DynamicBuffer<HpackField>,
    index: usize,
) -> Result<HpackField, DecoderError> {
    match index {
        0 => Err(DecoderError::InvalidTableIndex),
        1..=static_table::STATIC_TABLE_LEN => Ok(static_table::get(index)),
        _ => Ok(dynamic_entry(table, index)?.clone()),
    }
}

fn get_indexed_name(
    table: &DynamicBuffer<HpackField>,
    index: usize,
) -> Result<Bytes, DecoderError> {
    match index {
        0 => Err(DecoderError::InvalidTableIndex),
        1..=static_table::STATIC_TABLE_LEN => Ok(static_table::name(index)),
        _ => {
            let field = dynamic_entry(table, index)?;
            Ok(field.name_bytes())
        },
    }
}

fn decode_int<const PREFIX_SIZE: u8, B: Buf>(buf: &mut B) -> Result<usize, DecoderError> {
    const MAX_BYTES: usize = 5;
    const VARINT_MASK: u8 = 0b0111_1111;
    const VARINT_FLAG: u8 = 0b1000_0000;

    debug_assert!((1..=8).contains(&PREFIX_SIZE));
    if !buf.has_remaining() {
        return Err(DecoderError::NeedMore(NeedMore::IntegerUnderflow));
    }

    let mask = u8::MAX >> (u8::BITS - u32::from(PREFIX_SIZE));
    let mut value = usize::from(buf.get_u8() & mask);
    if value < usize::from(mask) {
        return Ok(value);
    }

    let mut bytes = 1_usize;
    let mut shift = 0_u32;

    while buf.has_remaining() {
        let byte = buf.get_u8();
        bytes += 1;
        let addition = usize::from(byte & VARINT_MASK)
            .checked_shl(shift)
            .ok_or(DecoderError::IntegerOverflow)?;
        value = value
            .checked_add(addition)
            .ok_or(DecoderError::IntegerOverflow)?;
        if byte & VARINT_FLAG == 0 {
            return Ok(value);
        }
        if bytes == MAX_BYTES {
            return Err(DecoderError::IntegerOverflow);
        }
        shift += 7;
    }

    Err(DecoderError::NeedMore(NeedMore::IntegerUnderflow))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hpack::Encoder;

    fn decode_all(src: &[u8], table_size: usize) -> Vec<HpackField> {
        let mut decoder = Decoder::new(table_size);
        let mut bytes = Bytes::copy_from_slice(src);
        let mut headers = Vec::new();
        decoder
            .decode_bytes(&mut bytes, |header| headers.push(header))
            .unwrap();
        headers
    }

    #[test]
    fn decode_string_empty() {
        let mut decoder = Decoder::new(0);
        let mut bytes = Bytes::new();
        let err = decoder.decode_string(&mut bytes).unwrap_err();
        assert_eq!(err, DecoderError::NeedMore(NeedMore::UnexpectedEndOfStream));
    }

    #[test]
    fn decode_empty_block() {
        let mut decoder = Decoder::new(0);
        let mut bytes = Bytes::new();
        decoder.decode_bytes(&mut bytes, |_| {}).unwrap();
    }

    #[test]
    fn huffman_arena_is_allocated_only_on_first_huffman_string() {
        let mut decoder = Decoder::new(0);
        assert_eq!(decoder.buffer.capacity(), 0);

        let mut plain = Bytes::from_static(b"\x03abc");
        assert_eq!(decoder.decode_string(&mut plain).unwrap(), b"abc"[..]);
        assert_eq!(decoder.buffer.capacity(), 0);

        let mut encoded = BytesMut::new();
        huffman::encode(b"custom-header-value", &mut encoded);
        let encoded_len = u8::try_from(encoded.len()).unwrap();
        let mut huffman = BytesMut::with_capacity(encoded.len() + 1);
        huffman.extend_from_slice(&[0x80 | encoded_len]);
        huffman.extend_from_slice(&encoded);
        let mut huffman = huffman.freeze();

        assert_eq!(
            decoder.decode_string(&mut huffman).unwrap(),
            b"custom-header-value"[..]
        );
        assert!(decoder.buffer.capacity() >= HUFFMAN_ARENA_CAPACITY / 2);
    }

    #[test]
    fn literal_with_indexing_round_trips() {
        let mut encoder = Encoder::new();
        let mut block = BytesMut::new();
        encoder.begin_block(&mut block);
        encoder.encode_field_bytes(b"x-h2corn", b"value", &mut block);

        let headers = decode_all(&block, 256);
        assert_eq!(headers.len(), 1);
        assert_eq!(headers[0].name(), b"x-h2corn");
        assert_eq!(headers[0].value(), b"value");
    }

    #[test]
    fn indexed_name_literal_round_trips_without_dynamic_lookup() {
        let mut encoder = Encoder::new();
        let mut block = BytesMut::new();
        encoder.begin_block(&mut block);
        encoder.encode_indexed_name_bytes(38, b"example.com", &mut block);

        let headers = decode_all(&block, 256);

        assert_eq!(headers.len(), 1);
        assert_eq!(headers[0].name(), b"host");
        assert_eq!(headers[0].value(), b"example.com");
    }

    #[test]
    fn dynamic_indexed_name_survives_table_eviction() {
        let mut decoder = Decoder::new(41);
        let mut first = Bytes::from_static(b"\x40\x04name\x01v");
        decoder.decode_bytes(&mut first, |_| {}).unwrap();

        // The second entry reuses the first entry's name, then evicts it.
        let mut second = Bytes::from_static(b"\x7e\x01w");
        let mut retained_name = None;
        decoder
            .decode_bytes(&mut second, |field| {
                retained_name = Some(field.into_parts().0);
            })
            .unwrap();

        assert_eq!(retained_name.as_deref(), Some(b"name".as_slice()));

        // A later insertion and eviction must not invalidate the callback-owned name.
        let mut third = Bytes::from_static(b"\x40\x04next\x01z");
        decoder.decode_bytes(&mut third, |_| {}).unwrap();
        assert_eq!(retained_name.as_deref(), Some(b"name".as_slice()));
    }

    #[test]
    fn static_indexed_name_literal_uses_static_name() {
        let headers = decode_all(&[0x01, 0x01, b'x'], 0);
        assert_eq!(headers[0].name(), b":authority");
        assert_eq!(headers[0].value(), b"x");
    }

    #[test]
    fn indexed_name_literal_invalid_index_is_unchanged() {
        let mut decoder = Decoder::new(0);
        let mut bytes = Bytes::from_static(&[0x7F, 0x61]);
        assert_eq!(
            decoder.decode_bytes(&mut bytes, |_| {}),
            Err(DecoderError::InvalidTableIndex)
        );
    }

    #[test]
    fn indexed_header_is_reused_from_dynamic_table() {
        let mut encoder = Encoder::new();
        let mut first = BytesMut::new();
        encoder.begin_block(&mut first);
        encoder.encode_field_bytes(b"x-h2corn", b"value", &mut first);

        let mut second = BytesMut::new();
        encoder.begin_block(&mut second);
        encoder.encode_field_bytes(b"x-h2corn", b"value", &mut second);

        let mut decoder = Decoder::new(256);
        let mut first_bytes = first.freeze();
        let mut second_bytes = second.freeze();
        let mut headers = Vec::new();

        decoder
            .decode_bytes(&mut first_bytes, |header| headers.push(header))
            .unwrap();
        decoder
            .decode_bytes(&mut second_bytes, |header| headers.push(header))
            .unwrap();

        assert_eq!(headers.len(), 2);
        assert_eq!(second_bytes.len(), 0);
    }

    #[test]
    fn decode_partial_huffman_value_reports_underflow() {
        let mut encoder = Encoder::new();
        let mut block = BytesMut::new();
        encoder.begin_block(&mut block);
        encoder.encode_field_bytes(b"x-h2corn", b"custom-value", &mut block);

        let mut decoder = Decoder::new(0);
        let mut bytes = block.freeze();
        let truncated = bytes.split_to(bytes.len() - 1);
        let mut truncated = truncated;
        let err = decoder.decode_bytes(&mut truncated, |_| {}).unwrap_err();
        assert_eq!(err, DecoderError::NeedMore(NeedMore::StringUnderflow));
    }

    #[test]
    fn table_size_update_after_headers_is_rejected() {
        let mut bytes = Bytes::from_static(&[0x82, 0x20]);
        let mut decoder = Decoder::new(4096);
        let err = decoder.decode_bytes(&mut bytes, |_| {}).unwrap_err();
        assert_eq!(err, DecoderError::InvalidMaxDynamicSize);
    }

    #[test]
    fn decode_integer_boundaries_are_checked() {
        let mut exact = Bytes::from_static(&[0x1F, 0xFF, 0xFF, 0xFF, 0x07]);
        decode_int::<5, _>(&mut exact).unwrap();

        let mut truncated = Bytes::from_static(&[0x1F, 0x80]);
        assert_eq!(
            decode_int::<5, _>(&mut truncated),
            Err(DecoderError::NeedMore(NeedMore::IntegerUnderflow))
        );

        let mut overflow = Bytes::from_static(&[0x1F, 0x80, 0x80, 0x80, 0x80]);
        assert_eq!(
            decode_int::<5, _>(&mut overflow),
            Err(DecoderError::IntegerOverflow)
        );
    }

    #[test]
    fn dynamic_table_resize_rules_hold_at_the_exact_limit() {
        let mut decoder = Decoder::new(4096);
        // Dynamic-table update with the five-bit HPACK prefix: 4096 and one
        // over, encoded without relying on the encoder under test.
        let mut exact = Bytes::from_static(&[0x3F, 0xE1, 0x1F]);
        decoder
            .decode_bytes(&mut exact, |_| {})
            .expect("the configured maximum is legal at block start");

        let mut over = Bytes::from_static(&[0x3F, 0xE2, 0x1F]);
        assert_eq!(
            decoder.decode_bytes(&mut over, |_| {}).unwrap_err(),
            DecoderError::InvalidMaxDynamicSize
        );
    }

    /// Uppercase names are HTTP-invalid but still HPACK-valid. Insertion must
    /// happen before the reject callback so a later indexed reference works.
    #[test]
    fn rejected_literal_with_indexing_is_still_inserted() {
        // LiteralWithIndexing, new name "X-Bad", value "1"
        // 0x40 = LiteralWithIndexing, name index 0
        let mut block = BytesMut::new();
        block.extend_from_slice(&[0x40]);
        // name length 5, "X-Bad"
        block.extend_from_slice(&[5]);
        block.extend_from_slice(b"X-Bad");
        // value length 1, "1"
        block.extend_from_slice(&[1]);
        block.extend_from_slice(b"1");

        let mut decoder = Decoder::new(4096);
        let mut first = block.freeze();
        let err = decoder
            .decode_block(&mut first, |_field| Err("reject"))
            .unwrap_err();
        assert!(matches!(err, DecodeBlockError::Rejected("reject")));

        // Indexed dynamic entry 62 (first dynamic slot)
        let mut second = Bytes::from_static(&[0xBE]);
        let mut got = None;
        decoder
            .decode_block(&mut second, |field| {
                got = Some(field);
                Ok::<(), &str>(())
            })
            .unwrap();
        let field = got.expect("indexed dynamic entry must decode");
        assert_eq!(field.name(), b"X-Bad");
        assert_eq!(field.value(), b"1");
    }

    /// A message rejection mid-block must not suppress a later compression
    /// error: HPACK failures outrank recorded stream/budget rejections.
    #[test]
    fn compression_error_overrides_prior_rejection() {
        let mut decoder = Decoder::new(4096);
        // LiteralWithIndexing "x-ok"/"1", then invalid table index 0 (0x80).
        let mut block = BytesMut::new();
        block.extend_from_slice(&[0x40, 4]);
        block.extend_from_slice(b"x-ok");
        block.extend_from_slice(&[1]);
        block.extend_from_slice(b"1");
        block.extend_from_slice(&[0x80]); // Indexed, index 0 → InvalidTableIndex

        let mut bytes = block.freeze();
        let err = decoder
            .decode_block(&mut bytes, |_| Err("budget"))
            .unwrap_err();
        assert!(matches!(
            err,
            DecodeBlockError::Compression(DecoderError::InvalidTableIndex)
        ));
    }

    /// Wire bytes from Python `hpack.Encoder` (huffman=False) must decode
    /// identically for static-indexed, never-indexed, and dynamic reuse.
    #[test]
    fn python_hpack_reference_static_never_and_dynamic() {
        // hpack.Encoder().encode([(b':method', b'GET')], huffman=False) → 0x82
        let headers = decode_all(&[0x82], 4096);
        assert_eq!(headers.len(), 1);
        assert_eq!(headers[0].name(), b":method");
        assert_eq!(headers[0].value(), b"GET");

        // authorization is never-indexed: 0x57 0x08 "Bearer x"
        let auth = decode_all(
            &[0x57, 0x08, b'B', b'e', b'a', b'r', b'e', b'r', b' ', b'x'],
            4096,
        );
        assert_eq!(auth[0].name(), b"authorization");
        assert_eq!(auth[0].value(), b"Bearer x");

        // Dynamic insert then reuse: first 0x40 0x06 "x-demo" 0x03 "val", second 0xBE
        let mut decoder = Decoder::new(4096);
        let mut first = Bytes::from_static(b"\x40\x06x-demo\x03val");
        let mut second = Bytes::from_static(&[0xBE]);
        let mut got = Vec::new();
        decoder
            .decode_bytes(&mut first, |field| got.push(field))
            .unwrap();
        decoder
            .decode_bytes(&mut second, |field| got.push(field))
            .unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].name(), b"x-demo");
        assert_eq!(got[0].value(), b"val");
        assert_eq!(got[1], got[0]);
    }
}
