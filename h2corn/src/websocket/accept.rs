//! The WebSocket handshake accept token: SHA-1 of the client key plus the
//! RFC 6455 GUID, base64-encoded.

use aws_lc_rs::digest::{SHA1_FOR_LEGACY_USE_ONLY, digest};

use crate::base64;
use crate::websocket::{WEBSOCKET_KEY_LEN, WebSocketKey};

const LEN: usize = 28;
const GUID: &[u8; 36] = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

pub(crate) fn websocket_accept(key: &WebSocketKey) -> [u8; LEN] {
    let mut input = [0; WEBSOCKET_KEY_LEN + GUID.len()];
    let (key_input, guid_input) = input.split_at_mut(WEBSOCKET_KEY_LEN);
    key_input.copy_from_slice(key);
    guid_input.copy_from_slice(GUID);

    let digest = digest(&SHA1_FOR_LEGACY_USE_ONLY, &input);
    let digest: &[u8; 20] = digest
        .as_ref()
        .try_into()
        .expect("a SHA-1 digest contains 20 bytes");
    let groups = [
        base64::encode_triplet(digest[0], digest[1], digest[2]),
        base64::encode_triplet(digest[3], digest[4], digest[5]),
        base64::encode_triplet(digest[6], digest[7], digest[8]),
        base64::encode_triplet(digest[9], digest[10], digest[11]),
        base64::encode_triplet(digest[12], digest[13], digest[14]),
        base64::encode_triplet(digest[15], digest[16], digest[17]),
        base64::encode_remainder(digest[18], digest[19]),
    ];
    let mut output = [0; LEN];
    output.copy_from_slice(groups.as_flattened());
    output
}

#[cfg(test)]
mod tests {
    use super::websocket_accept;

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute AWS-LC SHA1 FFI")]
    fn websocket_accept_matches_rfc_example() {
        assert_eq!(
            websocket_accept(b"dGhlIHNhbXBsZSBub25jZQ=="),
            *b"s3pPLMBiTxaQ9kYGzzhZRbK+xOo=",
        );
    }
}
