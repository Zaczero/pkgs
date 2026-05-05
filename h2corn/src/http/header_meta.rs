use bytes::Bytes;

use crate::ascii;
use crate::hpack::BytesStr;
use crate::http::types::KnownRequestHeaderName;
use crate::websocket::{
    WEBSOCKET_KEY_LEN, WEBSOCKET_VERSION, WebSocketRequestMeta,
    websocket_requested_permessage_deflate,
};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ProxyHeaderSlots {
    pub(crate) forwarded: Option<usize>,
    pub(crate) x_forwarded_for: Option<usize>,
    pub(crate) x_forwarded_proto: Option<usize>,
    pub(crate) x_forwarded_host: Option<usize>,
    pub(crate) x_forwarded_port: Option<usize>,
    pub(crate) x_forwarded_prefix: Option<usize>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct WebSocketHandshakeMeta {
    pub(crate) key: Option<[u8; WEBSOCKET_KEY_LEN]>,
    pub(crate) key_duplicate: bool,
    pub(crate) request: WebSocketRequestMeta,
    pub(crate) version_supported: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RequestHeaderMeta {
    pub(crate) accepts_trailers: bool,
    pub(crate) content_length: Option<u64>,
    pub(crate) websocket: WebSocketHandshakeMeta,
    pub(crate) proxy_headers: ProxyHeaderSlots,
}

impl RequestHeaderMeta {
    pub(crate) fn observe_known_header(
        &mut self,
        known_name: KnownRequestHeaderName,
        value: &Bytes,
        index: usize,
    ) {
        let value_bytes = value.as_ref();
        match known_name {
            KnownRequestHeaderName::Forwarded => {
                self.proxy_headers.forwarded = Some(index);
            },
            KnownRequestHeaderName::XForwardedFor => {
                self.proxy_headers.x_forwarded_for = Some(index);
            },
            KnownRequestHeaderName::XForwardedProto => {
                self.proxy_headers.x_forwarded_proto = Some(index);
            },
            KnownRequestHeaderName::XForwardedHost => {
                self.proxy_headers.x_forwarded_host = Some(index);
            },
            KnownRequestHeaderName::XForwardedPort => {
                self.proxy_headers.x_forwarded_port = Some(index);
            },
            KnownRequestHeaderName::XForwardedPrefix => {
                self.proxy_headers.x_forwarded_prefix = Some(index);
            },
            KnownRequestHeaderName::SecWebSocketVersion => {
                self.websocket.version_supported |= value_bytes == WEBSOCKET_VERSION;
            },
            KnownRequestHeaderName::SecWebSocketKey => {
                if self.websocket.key.is_some() {
                    self.websocket.key_duplicate = true;
                } else if let Ok(&key) = <&[u8; WEBSOCKET_KEY_LEN]>::try_from(value_bytes)
                    && websocket_key_is_syntactically_valid(value_bytes)
                {
                    self.websocket.key = Some(key);
                }
            },
            KnownRequestHeaderName::SecWebSocketProtocol => {
                push_requested_subprotocols(value, &mut self.websocket.request);
            },
            KnownRequestHeaderName::SecWebSocketExtensions => {
                self.websocket.request.per_message_deflate |=
                    websocket_requested_permessage_deflate(value_bytes);
            },
            _ => {},
        }
    }
}

fn websocket_key_is_syntactically_valid(value: &[u8]) -> bool {
    value.len() == WEBSOCKET_KEY_LEN
        && value[WEBSOCKET_KEY_LEN - 2..] == *b"=="
        && value[..WEBSOCKET_KEY_LEN - 2]
            .iter()
            .all(|byte| ascii::is_base64(*byte))
}

fn push_requested_subprotocols(value: &Bytes, out: &mut WebSocketRequestMeta) {
    if std::str::from_utf8(value.as_ref()).is_err() {
        return;
    }

    out.requested_subprotocols.extend(
        value
            .as_ref()
            .split(|&byte| byte == b',')
            .map(<[u8]>::trim_ascii)
            .filter(|item| !item.is_empty())
            .map(|item| {
                let bytes = value.slice_ref(item);
                // SAFETY: the complete header value was validated as UTF-8, and
                // this slice is borrowed from that same value on byte boundaries.
                unsafe { BytesStr::from_validated_utf8(bytes) }
            }),
    );
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::RequestHeaderMeta;
    use crate::hpack::BytesStr;
    use crate::http::types::KnownRequestHeaderName;
    use crate::websocket::RequestedSubprotocols;

    #[test]
    fn proxy_header_slots_keep_the_last_match() {
        let mut meta = RequestHeaderMeta::default();
        meta.observe_known_header(KnownRequestHeaderName::Forwarded, &Bytes::new(), 2);
        meta.observe_known_header(KnownRequestHeaderName::Forwarded, &Bytes::new(), 9);
        meta.observe_known_header(KnownRequestHeaderName::XForwardedProto, &Bytes::new(), 4);
        meta.observe_known_header(KnownRequestHeaderName::XForwardedProto, &Bytes::new(), 7);
        let slots = meta.proxy_headers;

        assert_eq!(slots.forwarded, Some(9));
        assert_eq!(slots.x_forwarded_proto, Some(7));
    }

    #[test]
    fn websocket_handshake_meta_parses_version_key_subprotocols_and_extensions() {
        let mut meta = RequestHeaderMeta::default();

        meta.observe_known_header(
            KnownRequestHeaderName::SecWebSocketVersion,
            &Bytes::from_static(b"13"),
            0,
        );
        meta.observe_known_header(
            KnownRequestHeaderName::SecWebSocketKey,
            &Bytes::from_static(b"dGhlIHNhbXBsZSBub25jZQ=="),
            1,
        );
        meta.observe_known_header(
            KnownRequestHeaderName::SecWebSocketProtocol,
            &Bytes::from_static(b"chat, superchat"),
            2,
        );
        meta.observe_known_header(
            KnownRequestHeaderName::SecWebSocketExtensions,
            &Bytes::from_static(b"permessage-deflate"),
            3,
        );

        assert!(meta.websocket.version_supported);
        assert_eq!(meta.websocket.key, Some(*b"dGhlIHNhbXBsZSBub25jZQ=="));
        let expected: RequestedSubprotocols = smallvec::smallvec![
            BytesStr::from_static("chat"),
            BytesStr::from_static("superchat"),
        ];
        assert_eq!(meta.websocket.request.requested_subprotocols, expected);
        assert!(meta.websocket.request.per_message_deflate);
    }
}
