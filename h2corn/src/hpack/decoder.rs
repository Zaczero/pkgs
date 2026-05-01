use std::str::Utf8Error;

use bytes::{Buf, Bytes, BytesMut};
use http::{header, method, status};

use crate::frame::DEFAULT_HEADER_TABLE_SIZE;

use super::{Header, header::OwnedName, huffman, static_table};

#[derive(Debug)]
pub struct Decoder {
    max_size_update: Option<usize>,
    last_max_update: usize,
    table: Table,
    buffer: BytesMut,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum DecoderError {
    InvalidRepresentation,
    InvalidTableIndex,
    InvalidHuffmanCode,
    InvalidUtf8,
    InvalidStatusCode,
    InvalidPseudoheader,
    InvalidMaxDynamicSize,
    IntegerOverflow,
    NeedMore(NeedMore),
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum NeedMore {
    UnexpectedEndOfStream,
    IntegerUnderflow,
    StringUnderflow,
}

enum Representation {
    Indexed,
    LiteralWithIndexing,
    LiteralWithoutIndexing,
    LiteralNeverIndexed,
    SizeUpdate,
}

const INVALID_REPRESENTATION: u8 = 0;
const INDEXED_REPRESENTATION: u8 = 1;
const LITERAL_WITH_INDEXING_REPRESENTATION: u8 = 2;
const LITERAL_WITHOUT_INDEXING_REPRESENTATION: u8 = 3;
const LITERAL_NEVER_INDEXED_REPRESENTATION: u8 = 4;
const SIZE_UPDATE_REPRESENTATION: u8 = 5;
const REPRESENTATION_TABLE: [u8; 256] = {
    let mut table = [INVALID_REPRESENTATION; 256];
    let mut byte = 0;
    while byte < 256 {
        let value = byte as u8;
        table[byte] = if value & 0b1000_0000 == 0b1000_0000 {
            INDEXED_REPRESENTATION
        } else if value & 0b0100_0000 == 0b0100_0000 {
            LITERAL_WITH_INDEXING_REPRESENTATION
        } else if value & 0b1111_0000 == 0 {
            LITERAL_WITHOUT_INDEXING_REPRESENTATION
        } else if value & 0b1111_0000 == 0b0001_0000 {
            LITERAL_NEVER_INDEXED_REPRESENTATION
        } else if value & 0b1110_0000 == 0b0010_0000 {
            SIZE_UPDATE_REPRESENTATION
        } else {
            INVALID_REPRESENTATION
        };
        byte += 1;
    }
    table
};

#[derive(Debug)]
struct Table {
    entries: Vec<DynamicEntry>,
    start: usize,
    size: usize,
    max_size: usize,
}

#[derive(Debug)]
struct DynamicEntry {
    header: Header,
    size: usize,
}

impl Decoder {
    pub fn new(size: usize) -> Self {
        Self {
            max_size_update: None,
            last_max_update: size,
            table: Table::new(size),
            buffer: BytesMut::with_capacity(4096),
        }
    }

    #[cfg(test)]
    pub fn decode_bytes<F>(&mut self, src: &mut Bytes, mut f: F) -> Result<(), DecoderError>
    where
        F: FnMut(Header),
    {
        self.decode_block(src, |header| {
            f(header);
            Ok(())
        })
    }

    const fn begin_block(&mut self) {
        if let Some(size) = self.max_size_update.take() {
            self.last_max_update = size;
        }
    }

    pub fn decode_block<F, E>(&mut self, src: &mut Bytes, mut f: F) -> Result<(), E>
    where
        F: FnMut(Header) -> Result<(), E>,
        E: From<DecoderError>,
    {
        self.begin_block();
        let mut can_resize = true;

        while self.decode_next(src, &mut can_resize, &mut f)? {}

        Ok(())
    }

    fn decode_next<F, E>(
        &mut self,
        src: &mut Bytes,
        can_resize: &mut bool,
        f: &mut F,
    ) -> Result<bool, E>
    where
        F: FnMut(Header) -> Result<(), E>,
        E: From<DecoderError>,
    {
        let Some(&byte) = src.chunk().first() else {
            return Ok(false);
        };

        match Representation::load(byte).map_err(E::from)? {
            Representation::Indexed => {
                *can_resize = false;
                f(self.decode_indexed(src).map_err(E::from)?)?;
            }
            Representation::LiteralWithIndexing => {
                *can_resize = false;
                let entry = self.decode_literal::<6>(src).map_err(E::from)?;
                self.table.insert(entry.clone());
                f(entry)?;
            }
            Representation::LiteralWithoutIndexing | Representation::LiteralNeverIndexed => {
                *can_resize = false;
                f(self.decode_literal::<4>(src).map_err(E::from)?)?;
            }
            Representation::SizeUpdate => {
                if !*can_resize {
                    return Err(E::from(DecoderError::InvalidMaxDynamicSize));
                }
                self.process_size_update(src).map_err(E::from)?;
            }
        }

        Ok(true)
    }

    fn process_size_update(&mut self, buf: &mut Bytes) -> Result<(), DecoderError> {
        let new_size = decode_int::<5, _>(buf)?;
        if new_size > self.last_max_update {
            return Err(DecoderError::InvalidMaxDynamicSize);
        }
        self.table.set_max_size(new_size);
        Ok(())
    }

    fn decode_indexed(&self, buf: &mut Bytes) -> Result<Header, DecoderError> {
        let index = decode_int::<7, _>(buf)?;
        self.table.get(index)
    }

    fn decode_literal<const PREFIX_SIZE: u8>(
        &mut self,
        buf: &mut Bytes,
    ) -> Result<Header, DecoderError> {
        let table_idx = decode_int::<PREFIX_SIZE, _>(buf)?;

        if table_idx == 0 {
            return self.decode_new_name_literal(buf);
        }

        let name = self.table.name(table_idx)?;
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
            return Ok(Self::decode_raw_string(buf, len));
        }

        Self::decode_huffman_string(buffer, buf, len)
    }

    fn decode_raw_string(buf: &mut Bytes, len: usize) -> Bytes {
        buf.copy_to_bytes(len)
    }

    fn decode_huffman_string(
        buffer: &mut BytesMut,
        buf: &mut Bytes,
        len: usize,
    ) -> Result<Bytes, DecoderError> {
        let decoded = huffman::decode(&buf.chunk()[..len], buffer)?;
        buf.advance(len);
        Ok(decoded.freeze())
    }

    fn decode_new_name_literal(&mut self, buf: &mut Bytes) -> Result<Header, DecoderError> {
        let name = Self::decode_string_into(&mut self.buffer, buf)?;
        let value = Self::decode_string_into(&mut self.buffer, buf)?;
        Header::new(name, value)
    }

    fn decode_indexed_name_literal(
        &mut self,
        buf: &mut Bytes,
        name: OwnedName,
    ) -> Result<Header, DecoderError> {
        let value = Self::decode_string_into(&mut self.buffer, buf)?;
        name.into_entry(value)
    }
}

impl Default for Decoder {
    fn default() -> Self {
        Self::new(DEFAULT_HEADER_TABLE_SIZE)
    }
}

impl Representation {
    fn load(byte: u8) -> Result<Self, DecoderError> {
        match REPRESENTATION_TABLE[usize::from(byte)] {
            INDEXED_REPRESENTATION => Ok(Self::Indexed),
            LITERAL_WITH_INDEXING_REPRESENTATION => Ok(Self::LiteralWithIndexing),
            LITERAL_WITHOUT_INDEXING_REPRESENTATION => Ok(Self::LiteralWithoutIndexing),
            LITERAL_NEVER_INDEXED_REPRESENTATION => Ok(Self::LiteralNeverIndexed),
            SIZE_UPDATE_REPRESENTATION => Ok(Self::SizeUpdate),
            _ => Err(DecoderError::InvalidRepresentation),
        }
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
        value += usize::from(byte & VARINT_MASK) << shift;
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

impl Table {
    const fn new(max_size: usize) -> Self {
        Self {
            entries: Vec::new(),
            start: 0,
            size: 0,
            max_size,
        }
    }

    fn get(&self, index: usize) -> Result<Header, DecoderError> {
        match index {
            0 => Err(DecoderError::InvalidTableIndex),
            1..=static_table::STATIC_TABLE_LEN => Ok(static_table::get(index)),
            _ => Ok(self.dynamic_entry(index)?.header.clone()),
        }
    }

    fn name(&self, index: usize) -> Result<OwnedName, DecoderError> {
        match index {
            0 => Err(DecoderError::InvalidTableIndex),
            1..=static_table::STATIC_TABLE_LEN => Ok(static_table::name(index)),
            _ => Ok(self.dynamic_entry(index)?.header.owned_name()),
        }
    }

    fn insert(&mut self, header: Header) {
        let size = header.len();
        if size > self.max_size {
            self.clear();
            return;
        }

        self.evict_to_fit(size);
        self.entries.push(DynamicEntry { header, size });
        self.size += size;
    }

    fn set_max_size(&mut self, size: usize) {
        self.max_size = size;
        self.evict_to_fit(0);
    }

    fn evict_to_fit(&mut self, incoming: usize) {
        while self.size + incoming > self.max_size {
            let Some(entry) = self.entries.get(self.start) else {
                self.clear();
                return;
            };
            self.size -= entry.size;
            self.start += 1;
        }
        self.compact();
    }

    fn clear(&mut self) {
        self.entries.clear();
        self.start = 0;
        self.size = 0;
    }

    fn dynamic_entry(&self, index: usize) -> Result<&DynamicEntry, DecoderError> {
        let live_len = self.entries.len() - self.start;
        let dynamic_offset = index - static_table::DYNAMIC_INDEX_OFFSET;
        if dynamic_offset >= live_len {
            return Err(DecoderError::InvalidTableIndex);
        }

        let slot = self.entries.len() - 1 - dynamic_offset;
        Ok(&self.entries[slot])
    }

    fn compact(&mut self) {
        if self.start == 0 {
            return;
        }
        if self.start == self.entries.len() {
            self.entries.clear();
            self.start = 0;
            return;
        }
        if self.start >= 32 && self.start * 2 >= self.entries.len() {
            self.entries.drain(..self.start);
            self.start = 0;
        }
    }
}

impl From<Utf8Error> for DecoderError {
    fn from(_: Utf8Error) -> Self {
        Self::InvalidUtf8
    }
}

impl From<header::InvalidHeaderValue> for DecoderError {
    fn from(_: header::InvalidHeaderValue) -> Self {
        Self::InvalidUtf8
    }
}

impl From<header::InvalidHeaderName> for DecoderError {
    fn from(_: header::InvalidHeaderName) -> Self {
        Self::InvalidUtf8
    }
}

impl From<method::InvalidMethod> for DecoderError {
    fn from(_: method::InvalidMethod) -> Self {
        Self::InvalidUtf8
    }
}

impl From<status::InvalidStatusCode> for DecoderError {
    fn from(_: status::InvalidStatusCode) -> Self {
        Self::InvalidUtf8
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::hpack::Encoder;

    fn decode_all(src: &[u8], table_size: usize) -> Vec<Header> {
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
    fn literal_with_indexing_round_trips() {
        let mut encoder = Encoder::new();
        let mut block = BytesMut::new();
        encoder.begin_block(&mut block);
        encoder.encode_field_bytes(b"x-h2corn", b"value", &mut block);

        let headers = decode_all(&block, 256);
        assert_eq!(headers.len(), 1);
        match &headers[0] {
            Header::Field { name, value } => {
                assert_eq!(name.as_str(), "x-h2corn");
                assert_eq!(value.as_ref(), b"value");
            }
            _ => panic!("unexpected header kind"),
        }
    }

    #[test]
    fn indexed_name_literal_round_trips_without_dynamic_lookup() {
        let mut encoder = Encoder::new();
        let mut block = BytesMut::new();
        encoder.begin_block(&mut block);
        encoder.encode_indexed_name_bytes(38, b"example.com", &mut block);

        let headers = decode_all(&block, 256);

        assert_eq!(headers.len(), 1);
        match &headers[0] {
            Header::Field { name, value } => {
                assert_eq!(name.as_str(), "host");
                assert_eq!(value.as_ref(), b"example.com");
            }
            _ => panic!("unexpected header kind"),
        }
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
}
