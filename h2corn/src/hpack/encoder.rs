use bytes::{Bytes, BytesMut};

use crate::frame::DEFAULT_HEADER_TABLE_SIZE;

use super::{
    huffman,
    static_table::{self, StaticFieldEntry},
};

#[derive(Debug)]
pub struct Encoder {
    table: DynamicTable,
    committed_max_size: usize,
    current_max_size: usize,
    pending_floor: Option<usize>,
}

#[derive(Debug)]
struct DynamicTable {
    entries: Vec<DynamicEntry>,
    start: usize,
    size: usize,
    max_size: usize,
}

#[derive(Debug)]
struct DynamicEntry {
    name: Bytes,
    value: Bytes,
    size: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LiteralMode {
    IncrementalIndexing,
    WithoutIndexing,
    NeverIndexed,
}

impl LiteralMode {
    fn prefix(self) -> (u8, u8) {
        match self {
            Self::IncrementalIndexing => (6, 0x40),
            Self::WithoutIndexing => (4, 0x00),
            Self::NeverIndexed => (4, 0x10),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LiteralName<'a> {
    Indexed(usize),
    Literal(&'a [u8]),
}

impl Encoder {
    pub fn new() -> Self {
        Self::with_max_size(DEFAULT_HEADER_TABLE_SIZE)
    }

    fn with_max_size(max_size: usize) -> Self {
        Self {
            table: DynamicTable::new(max_size),
            committed_max_size: max_size,
            current_max_size: max_size,
            pending_floor: None,
        }
    }

    pub fn update_max_size(&mut self, size: usize) {
        if size == self.current_max_size {
            return;
        }

        self.current_max_size = size;
        self.table.set_max_size(size);

        if size == self.committed_max_size {
            self.pending_floor = None;
            return;
        }

        self.pending_floor = Some(self.pending_floor.unwrap_or(size).min(size));
    }

    pub fn begin_block(&mut self, dst: &mut BytesMut) {
        if self.current_max_size == self.committed_max_size {
            self.pending_floor = None;
            return;
        }

        if let Some(floor) = self.pending_floor
            && floor < self.committed_max_size
            && floor < self.current_max_size
        {
            encode_int(floor, 5, 0x20, dst);
        }

        encode_int(self.current_max_size, 5, 0x20, dst);
        self.committed_max_size = self.current_max_size;
        self.pending_floor = None;
    }

    pub fn encode_indexed(&self, index: usize, dst: &mut BytesMut) {
        encode_int(index, 7, 0x80, dst);
    }

    pub fn encode_indexed_name_bytes(&self, index: usize, value: &[u8], dst: &mut BytesMut) {
        encode_literal(
            LiteralName::Indexed(index),
            value,
            LiteralMode::WithoutIndexing,
            dst,
        );
    }

    pub fn encode_field_bytes(&mut self, name: &[u8], value: &[u8], dst: &mut BytesMut) {
        let static_name = static_table::field_index_entry(name);

        if let Some(entry) = static_name {
            if entry.exact_value == value {
                self.encode_indexed(entry.index, dst);
                return;
            }
            if entry.skip_value_index || entry.never_index {
                let mode = if entry.never_index {
                    LiteralMode::NeverIndexed
                } else {
                    LiteralMode::WithoutIndexing
                };
                encode_literal(LiteralName::Indexed(entry.index), value, mode, dst);
                return;
            }
        }
        let (exact_index, dynamic_name_index) = self.table.find(name, value);
        if let Some(index) = exact_index {
            self.encode_indexed(index, dst);
            return;
        }

        let literal_name = static_name
            .map(|entry| entry.index)
            .or(dynamic_name_index)
            .map_or(LiteralName::Literal(name), LiteralName::Indexed);

        if should_index_header(static_name, name.len(), value.len(), self.current_max_size) {
            encode_literal(literal_name, value, LiteralMode::IncrementalIndexing, dst);
            self.table.insert(name, value);
            return;
        }

        let mode = if static_name.is_some_and(|entry| entry.never_index) {
            LiteralMode::NeverIndexed
        } else {
            LiteralMode::WithoutIndexing
        };
        encode_literal(literal_name, value, mode, dst);
    }
}

impl Default for Encoder {
    fn default() -> Self {
        Self::new()
    }
}

impl DynamicTable {
    fn new(max_size: usize) -> Self {
        Self {
            entries: Vec::new(),
            start: 0,
            size: 0,
            max_size,
        }
    }

    fn set_max_size(&mut self, max_size: usize) {
        self.max_size = max_size;
        self.evict_to_fit(0);
    }

    fn find(&self, name: &[u8], value: &[u8]) -> (Option<usize>, Option<usize>) {
        let mut name_index = None;
        for (offset, entry) in self.entries[self.start..].iter().rev().enumerate() {
            if entry.name.as_ref() != name {
                continue;
            }

            let index = static_table::DYNAMIC_INDEX_OFFSET + offset;
            if entry.value.as_ref() == value {
                return (Some(index), Some(index));
            }
            if name_index.is_none() {
                name_index = Some(index);
            }
        }
        (None, name_index)
    }

    fn insert(&mut self, name: &[u8], value: &[u8]) {
        let size = dynamic_entry_size(name.len(), value.len());
        if size > self.max_size {
            self.clear();
            return;
        }

        self.evict_to_fit(size);
        self.entries.push(DynamicEntry {
            name: Bytes::copy_from_slice(name),
            value: Bytes::copy_from_slice(value),
            size,
        });
        self.size += size;
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

fn should_index_header(
    static_name: Option<StaticFieldEntry>,
    name_len: usize,
    value_len: usize,
    max_size: usize,
) -> bool {
    if max_size == 0 || static_name.is_some_and(|entry| entry.skip_value_index || entry.never_index)
    {
        return false;
    }

    let size = dynamic_entry_size(name_len, value_len);
    size <= max_size && size.saturating_mul(4) <= max_size.saturating_mul(3)
}

fn dynamic_entry_size(name_len: usize, value_len: usize) -> usize {
    32 + name_len + value_len
}

fn encode_literal(name: LiteralName<'_>, value: &[u8], mode: LiteralMode, dst: &mut BytesMut) {
    let (prefix_bits, prefix_mask) = mode.prefix();
    match name {
        LiteralName::Indexed(name_index) => encode_int(name_index, prefix_bits, prefix_mask, dst),
        LiteralName::Literal(name) => {
            dst.extend_from_slice(&[prefix_mask]);
            encode_string(name, dst);
        }
    }

    encode_string(value, dst);
}

fn encode_string(value: &[u8], dst: &mut BytesMut) {
    if value.is_empty() {
        dst.extend_from_slice(&[0]);
        return;
    }
    if value.len() < 3 {
        encode_int(value.len(), 7, 0x00, dst);
        dst.extend_from_slice(value);
        return;
    }

    let huffman_len = huffman::encoded_len(value);
    if huffman_len < value.len() {
        encode_int(huffman_len, 7, 0x80, dst);
        huffman::encode_with_len(value, huffman_len, dst);
        return;
    }

    encode_int(value.len(), 7, 0x00, dst);
    dst.extend_from_slice(value);
}

fn encode_int(value: usize, prefix: u8, mask: u8, dst: &mut BytesMut) {
    debug_assert!((1..=8).contains(&prefix));
    let max_prefix = (1_usize << usize::from(prefix)) - 1;
    if value < max_prefix {
        dst.extend_from_slice(&[(value as u8) | mask]);
        return;
    }

    dst.extend_from_slice(&[(max_prefix as u8) | mask]);
    let mut remaining = value - max_prefix;
    while remaining >= 128 {
        dst.extend_from_slice(&[((remaining & 0x7f) as u8) | 0x80]);
        remaining >>= 7;
    }
    dst.extend_from_slice(&[remaining as u8]);
}

#[cfg(test)]
mod test {
    use bytes::BytesMut;

    use super::*;

    #[test]
    fn repeated_header_uses_dynamic_table_index() {
        let mut encoder = Encoder::new();
        let mut dst = BytesMut::new();

        encoder.begin_block(&mut dst);
        encoder.encode_field_bytes(b"x-h2corn-test", b"value", &mut dst);
        assert!(!dst.is_empty());

        dst.clear();
        encoder.begin_block(&mut dst);
        encoder.encode_field_bytes(b"x-h2corn-test", b"value", &mut dst);
        assert_eq!(&dst[..], &[0xbe]);
    }

    #[test]
    fn skipped_static_header_is_not_dynamic_indexed() {
        let mut encoder = Encoder::new();
        let mut dst = BytesMut::new();

        encoder.begin_block(&mut dst);
        encoder.encode_field_bytes(b"content-length", b"7", &mut dst);
        assert_eq!(&dst[..], &[0x0f, 13, 1, b'7']);

        dst.clear();
        encoder.begin_block(&mut dst);
        encoder.encode_field_bytes(b"content-length", b"7", &mut dst);
        assert_eq!(&dst[..], &[0x0f, 13, 1, b'7']);
    }

    #[test]
    fn size_update_is_emitted_before_next_block() {
        let mut encoder = Encoder::new();
        let mut dst = BytesMut::new();

        encoder.update_max_size(0);
        encoder.begin_block(&mut dst);

        assert_eq!(&dst[..], &[0x20]);
    }

    #[test]
    fn size_update_clears_when_limit_returns_to_committed_value() {
        let mut encoder = Encoder::new();
        let mut dst = BytesMut::new();

        encoder.update_max_size(128);
        encoder.update_max_size(DEFAULT_HEADER_TABLE_SIZE);
        encoder.begin_block(&mut dst);

        assert!(dst.is_empty());
    }
}
