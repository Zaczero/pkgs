use bytes::BytesMut;

use super::dynamic_table::DynamicBuffer;
use super::static_table::{self, StaticFieldEntry};
use super::{HpackField, huffman};
use crate::h2_frame::DEFAULT_HEADER_TABLE_SIZE;

#[derive(Debug)]
pub(crate) struct Encoder {
    table: DynamicBuffer<HpackField>,
    committed_max_size: usize,
    current_max_size: usize,
    pending_floor: Option<usize>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LiteralMode {
    IncrementalIndexing,
    WithoutIndexing,
    NeverIndexed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LiteralName<'a> {
    Indexed(usize),
    Literal(&'a [u8]),
}

impl Encoder {
    pub(crate) const fn new() -> Self {
        Self::with_max_size(DEFAULT_HEADER_TABLE_SIZE)
    }

    const fn with_max_size(max_size: usize) -> Self {
        Self {
            table: DynamicBuffer::new(max_size),
            committed_max_size: max_size,
            current_max_size: max_size,
            pending_floor: None,
        }
    }

    pub(crate) fn update_max_size(&mut self, size: usize) {
        if size == self.current_max_size {
            return;
        }

        self.current_max_size = size;
        self.table.set_max_size(size);
        // The floor survives a return to the committed size. RFC 7541 §4.2
        // requires the *smallest* maximum seen between two blocks to be
        // signalled before the final one, so `4096 -> 0 -> 4096` still owes
        // the peer a `0` update even though nothing appears to have changed.
        self.pending_floor = Some(self.pending_floor.unwrap_or(size).min(size));
    }

    #[cfg(test)]
    pub(crate) const fn max_size(&self) -> usize {
        self.current_max_size
    }

    #[cfg(test)]
    pub(crate) fn table_size(&self) -> usize {
        self.table.live().iter().map(|(_, size)| *size).sum()
    }

    #[cfg(test)]
    pub(crate) fn table_len(&self) -> usize {
        self.table.live().len()
    }

    pub(crate) fn begin_block(&mut self, dst: &mut BytesMut) {
        let floor = self
            .pending_floor
            .filter(|floor| *floor < self.committed_max_size && *floor < self.current_max_size);
        if floor.is_none() && self.current_max_size == self.committed_max_size {
            self.pending_floor = None;
            return;
        }

        if let Some(floor) = floor {
            encode_int(floor, 5, 0x20, dst);
        }

        encode_int(self.current_max_size, 5, 0x20, dst);
        self.committed_max_size = self.current_max_size;
        self.pending_floor = None;
    }

    pub(crate) fn encode_indexed(&self, index: usize, dst: &mut BytesMut) {
        encode_int(index, 7, 0x80, dst);
    }

    pub(crate) fn encode_indexed_name_bytes(&self, index: usize, value: &[u8], dst: &mut BytesMut) {
        encode_literal(
            LiteralName::Indexed(index),
            value,
            LiteralMode::WithoutIndexing,
            dst,
        );
    }

    pub(crate) fn encode_field_bytes(&mut self, name: &[u8], value: &[u8], dst: &mut BytesMut) {
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
        let (exact_index, dynamic_name_index) = find_dynamic(&self.table, name, value);
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
            let field = HpackField::copy_from_slices(name, value);
            let size = field.table_size();
            self.table.insert(field, size);
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

fn find_dynamic(
    table: &DynamicBuffer<HpackField>,
    name: &[u8],
    value: &[u8],
) -> (Option<usize>, Option<usize>) {
    let mut name_index = None;
    for (offset, (entry, _)) in table.live().iter().rev().enumerate() {
        if entry.name() != name {
            continue;
        }
        let index = static_table::DYNAMIC_INDEX_OFFSET + offset;
        if entry.value() == value {
            return (Some(index), Some(index));
        }
        if name_index.is_none() {
            name_index = Some(index);
        }
    }
    (None, name_index)
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

    let size = 32 + name_len + value_len;
    size <= max_size && size.saturating_mul(4) <= max_size.saturating_mul(3)
}

fn encode_literal(name: LiteralName<'_>, value: &[u8], mode: LiteralMode, dst: &mut BytesMut) {
    let (prefix_bits, prefix_mask) = match mode {
        LiteralMode::IncrementalIndexing => (6, 0x40),
        LiteralMode::WithoutIndexing => (4, 0x00),
        LiteralMode::NeverIndexed => (4, 0x10),
    };
    match name {
        LiteralName::Indexed(name_index) => encode_int(name_index, prefix_bits, prefix_mask, dst),
        LiteralName::Literal(name) => {
            dst.extend_from_slice(&[prefix_mask]);
            encode_string(name, dst);
        },
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
        dst.extend_from_slice(&[((remaining & 0x7F) as u8) | 0x80]);
        remaining >>= 7;
    }
    dst.extend_from_slice(&[remaining as u8]);
}

#[cfg(test)]
mod tests {
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
        assert_eq!(&dst[..], &[0xBE]);
    }

    #[test]
    fn skipped_static_header_is_not_dynamic_indexed() {
        let mut encoder = Encoder::new();
        let mut dst = BytesMut::new();

        encoder.begin_block(&mut dst);
        encoder.encode_field_bytes(b"content-length", b"7", &mut dst);
        assert_eq!(&dst[..], &[0x0F, 13, 1, b'7']);

        dst.clear();
        encoder.begin_block(&mut dst);
        encoder.encode_field_bytes(b"content-length", b"7", &mut dst);
        assert_eq!(&dst[..], &[0x0F, 13, 1, b'7']);
    }

    #[test]
    fn size_update_is_emitted_before_next_block() {
        let mut encoder = Encoder::new();
        let mut dst = BytesMut::new();

        encoder.update_max_size(0);
        encoder.begin_block(&mut dst);

        assert_eq!(&dst[..], &[0x20]);
    }

    /// RFC 7541 §4.2: when the limit changes more than once between blocks,
    /// the smallest intervening maximum is signalled first and the final one
    /// second — even when the final value is the one already committed, which
    /// makes the sequence look like a no-op from the encoder's own state.
    #[test]
    fn size_update_signals_the_floor_when_the_limit_returns_to_its_committed_value() {
        let mut encoder = Encoder::new();
        let mut dst = BytesMut::new();

        encoder.update_max_size(128);
        encoder.update_max_size(DEFAULT_HEADER_TABLE_SIZE);
        encoder.begin_block(&mut dst);

        let mut expected = BytesMut::new();
        encode_int(128, 5, 0x20, &mut expected);
        encode_int(DEFAULT_HEADER_TABLE_SIZE, 5, 0x20, &mut expected);
        assert_eq!(&dst[..], &expected[..]);
    }

    /// A limit that never moves owes the peer nothing.
    #[test]
    fn unchanged_limit_emits_no_size_update() {
        let mut encoder = Encoder::new();
        let mut dst = BytesMut::new();

        encoder.update_max_size(DEFAULT_HEADER_TABLE_SIZE);
        encoder.begin_block(&mut dst);

        assert!(dst.is_empty());
    }

    /// Never-indexed static fields must stay never-indexed on the wire.
    #[test]
    fn never_indexed_authorization_is_not_dynamic_indexed() {
        let mut encoder = Encoder::new();
        let mut dst = BytesMut::new();

        encoder.begin_block(&mut dst);
        encoder.encode_field_bytes(b"authorization", b"Bearer x", &mut dst);
        let first = dst.clone();
        assert!(!first.is_empty());
        // NeverIndexed prefix for static index 23: 0x10 | name encoding
        assert_eq!(first[0] & 0xF0, 0x10);

        dst.clear();
        encoder.begin_block(&mut dst);
        encoder.encode_field_bytes(b"authorization", b"Bearer x", &mut dst);
        assert_eq!(dst, first);
    }
}
