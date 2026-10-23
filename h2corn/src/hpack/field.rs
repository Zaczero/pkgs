use bytes::Bytes;

/// Raw HPACK name/value pair with no HTTP semantics applied.
///
/// Table state depends only on successfully decoded bytes. Message validity
/// may reject a stream, but never suppresses an insertion or becomes a
/// compression error.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct HpackField {
    name: Bytes,
    value: Bytes,
}

impl HpackField {
    pub(crate) const fn from_static(name: &'static [u8], value: &'static [u8]) -> Self {
        Self {
            name: Bytes::from_static(name),
            value: Bytes::from_static(value),
        }
    }

    pub(crate) fn copy_from_slices(name: &[u8], value: &[u8]) -> Self {
        let mut value = Bytes::from([name, value].concat());
        let name = value.split_to(name.len());
        Self { name, value }
    }

    pub(crate) const fn from_parts(name: Bytes, value: Bytes) -> Self {
        Self { name, value }
    }

    pub(crate) fn name(&self) -> &[u8] {
        self.name.as_ref()
    }

    pub(crate) fn name_bytes(&self) -> Bytes {
        self.name.clone()
    }

    pub(crate) fn value(&self) -> &[u8] {
        self.value.as_ref()
    }

    pub(crate) fn into_parts(self) -> (Bytes, Bytes) {
        (self.name, self.value)
    }

    /// RFC 7541 §4.1 entry size: 32 + name length + value length.
    pub(crate) const fn table_size(&self) -> usize {
        self.name
            .len()
            .checked_add(self.value.len())
            .expect("HPACK field storage size fits usize")
            .checked_add(32)
            .expect("HPACK entry storage size fits usize")
    }
}

#[derive(Debug)]
pub(crate) enum DecodeBlockError<E> {
    Compression(super::DecoderError),
    Rejected(E),
}
