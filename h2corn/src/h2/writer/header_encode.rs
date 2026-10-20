use bytes::BytesMut;

use crate::h2::writer::ENCODED_HEADER_BLOCK_CAPACITY;
use crate::h2_frame;
use crate::hpack::Encoder;
use crate::http::digits;
use crate::http::types::{
    EarlyHintLinks, HttpStatusCode, ResponseField, ResponseHeaders, ResponseTrailers,
    common_status_codes, status_code,
};

macro_rules! common_status_hpack_index_match {
    ($(($constant:ident, $line:expr, $hpack:expr)),+ $(,)?) => {
        const fn common_status_hpack_index(status: HttpStatusCode) -> u8 {
            match status {
                $(status_code::$constant => $hpack,)+
                _ => 0,
            }
        }
    };
}

const LOCAL_ENCODER_TABLE_SIZE: usize = h2_frame::DEFAULT_HEADER_TABLE_SIZE;

pub(super) struct HeaderEncodeState {
    encoder: Encoder,
    block: BytesMut,
}

impl HeaderEncodeState {
    pub(super) fn block(&self) -> &[u8] {
        &self.block
    }
    pub(super) fn new() -> Self {
        Self {
            encoder: Encoder::new(),
            block: BytesMut::with_capacity(ENCODED_HEADER_BLOCK_CAPACITY),
        }
    }

    pub(super) fn encode_response(
        &mut self,
        status: HttpStatusCode,
        headers: &ResponseHeaders,
    ) -> &[u8] {
        self.block.clear();
        self.encoder.begin_block(&mut self.block);
        encode_header_block(&mut self.encoder, &mut self.block, Some(status), headers);
        self.block.as_ref()
    }

    /// One interim 103 block: the fixed status and one `link` field per value.
    ///
    /// No Date, Server, Content-Length or any other final-response default --
    /// those belong to the response this hint precedes, not to the hint.
    pub(super) fn encode_early_hint(&mut self, links: &EarlyHintLinks) -> &[u8] {
        self.block.clear();
        self.encoder.begin_block(&mut self.block);
        encode_status_header(&self.encoder, &mut self.block, status_code::EARLY_HINTS);
        for link in links.values() {
            self.encoder
                .encode_field_bytes(b"link", link.as_bytes(), &mut self.block);
        }
        self.block.as_ref()
    }

    pub(super) fn encode_trailers(&mut self, trailers: &ResponseTrailers) -> &[u8] {
        self.block.clear();
        self.encoder.begin_block(&mut self.block);
        encode_header_fields(&mut self.encoder, &mut self.block, trailers.as_fields());
        &self.block
    }

    pub(super) fn update_max_size(&mut self, peer: usize) {
        self.encoder
            .update_max_size(peer.min(LOCAL_ENCODER_TABLE_SIZE));
    }

    #[cfg(test)]
    pub(super) const fn encoder_max_size(&self) -> usize {
        self.encoder.max_size()
    }

    #[cfg(test)]
    pub(super) fn encoder_table_size(&self) -> usize {
        self.encoder.table_size()
    }

    #[cfg(test)]
    pub(super) fn encoder_table_len(&self) -> usize {
        self.encoder.table_len()
    }
}

common_status_codes!(common_status_hpack_index_match);

fn encode_header_block(
    encoder: &mut Encoder,
    out: &mut BytesMut,
    status: Option<HttpStatusCode>,
    headers: &ResponseHeaders,
) {
    if let Some(status) = status {
        encode_status_header(encoder, out, status);
    }
    encode_header_fields(encoder, out, headers);
}

fn encode_status_header(encoder: &Encoder, out: &mut BytesMut, status: HttpStatusCode) {
    let indexed = common_status_hpack_index(status);
    if indexed != 0 {
        out.extend_from_slice(&[indexed]);
    } else {
        encoder.encode_indexed_name_bytes(8, &digits::three_digit_bytes(status.get()), out);
    }
}

fn encode_header_fields(encoder: &mut Encoder, out: &mut BytesMut, headers: &[ResponseField]) {
    for (name, value) in headers {
        encoder.encode_field_bytes(name.as_bytes(), value.as_bytes(), out);
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::HeaderEncodeState;
    use crate::http::types::{ResponseHeaders, status_code};

    macro_rules! common_status_encodings_stay_in_sync {
        ($(($constant:ident, $line:expr, $hpack:expr)),+ $(,)?) => {
            #[test]
            fn every_common_status_has_an_h2_encoding() {
                let mut state = HeaderEncodeState::new();
                $(
                    let block = state.encode_response(status_code::$constant, &ResponseHeaders::new());
                    if $hpack == 0 {
                        assert_eq!(block.first(), Some(&0x08), stringify!($constant));
                    } else {
                        assert_eq!(block, &[$hpack], stringify!($constant));
                    }
                )+
            }
        };
    }

    crate::http::types::common_status_codes!(common_status_encodings_stay_in_sync);

    #[test]
    fn response_encode_reuses_header_block_buffer() {
        let mut state = HeaderEncodeState::new();
        let initial_capacity = state.block.capacity();

        let first_len = state
            .encode_response(status_code::OK, &ResponseHeaders::new())
            .len();
        let second_len = state
            .encode_response(status_code::OK, &ResponseHeaders::new())
            .len();

        assert_ne!(first_len, 0);
        assert_eq!(second_len, first_len);
        assert_eq!(state.block.capacity(), initial_capacity);
    }

    #[test]
    fn common_status_uses_static_table_index() {
        let mut state = HeaderEncodeState::new();

        let block = state.encode_response(status_code::NOT_FOUND, &ResponseHeaders::new());

        assert_eq!(block, &[0x8D]);
    }

    #[test]
    fn encoder_peer_limit_is_locally_capped() {
        let mut state = HeaderEncodeState::new();

        state.update_max_size(0);
        let zero_block = state.encode_response(status_code::OK, &ResponseHeaders::new());
        assert!(!zero_block.is_empty());
        assert_eq!(state.encoder_max_size(), 0);
        assert_eq!(state.encoder_table_size(), 0);
        assert_eq!(state.encoder_table_len(), 0);

        state.update_max_size(usize::MAX);
        let capped_block = state.encode_response(status_code::OK, &ResponseHeaders::new());
        assert!(!capped_block.is_empty());
        assert_eq!(state.encoder_max_size(), super::LOCAL_ENCODER_TABLE_SIZE);
    }

    #[test]
    fn encoder_unique_values_remain_locally_bounded() {
        let mut state = HeaderEncodeState::new();
        let value = Bytes::from(vec![b'x'; 1024]);
        state.update_max_size(usize::MAX);

        for index in 0..12 {
            let headers = vec![(
                Bytes::from(format!("x-unique-{index}")).into(),
                value.clone().into(),
            )];
            let _ = state.encode_response(status_code::OK, &headers);
            assert!(state.encoder_table_size() <= super::LOCAL_ENCODER_TABLE_SIZE);
            assert!(state.encoder_table_len() <= 4);
        }
    }

    #[test]
    fn encoder_zero_disables_dynamic_indexing() {
        let mut state = HeaderEncodeState::new();
        state.update_max_size(0);
        let headers = vec![
            (
                Bytes::from_static(b"x-first").into(),
                Bytes::from_static(b"one").into(),
            ),
            (
                Bytes::from_static(b"x-second").into(),
                Bytes::from_static(b"two").into(),
            ),
        ];

        let _ = state.encode_response(status_code::OK, &headers);

        assert_eq!(state.encoder_max_size(), 0);
        assert_eq!(state.encoder_table_size(), 0);
        assert_eq!(state.encoder_table_len(), 0);
    }
}
