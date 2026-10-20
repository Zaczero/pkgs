//! The WebSocket handshake accept token: SHA-1 of the client key plus the
//! RFC 6455 GUID, base64-encoded.

use aws_lc_rs::digest::{SHA1_FOR_LEGACY_USE_ONLY, digest};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;

use crate::websocket::{WEBSOCKET_KEY_LEN, WebSocketKey};

const LEN: usize = 28;
const GUID: &[u8; 36] = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

pub(crate) fn websocket_accept(key: &WebSocketKey) -> [u8; LEN] {
    let mut input = [0; WEBSOCKET_KEY_LEN + GUID.len()];
    let (key_input, guid_input) = input.split_at_mut(WEBSOCKET_KEY_LEN);
    key_input.copy_from_slice(key);
    guid_input.copy_from_slice(GUID);

    let digest = digest(&SHA1_FOR_LEGACY_USE_ONLY, &input);
    let mut output = [0; LEN];
    let encoded_len = STANDARD
        .encode_slice(digest.as_ref(), &mut output)
        .expect("a SHA-1 digest fits its fixed-size base64 output");
    debug_assert_eq!(encoded_len, LEN);
    output
}

#[cfg(test)]
mod tests {
    use super::websocket_accept;

    #[test]
    fn websocket_accept_matches_rfc_example() {
        assert_eq!(
            websocket_accept(b"dGhlIHNhbXBsZSBub25jZQ=="),
            *b"s3pPLMBiTxaQ9kYGzzhZRbK+xOo=",
        );
    }
}
