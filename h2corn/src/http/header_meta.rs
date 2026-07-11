use std::num::NonZeroU32;
use std::str::from_utf8;

use bytes::Bytes;

use crate::ascii;
use crate::hpack::BytesStr;
use crate::http::types::KnownRequestHeaderName;
use crate::websocket::{
    WEBSOCKET_KEY_LEN, WEBSOCKET_VERSION, WebSocketRequestMeta,
    websocket_requested_permessage_deflate,
};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ProxyHeaderSlots {
    pub(crate) forwarded: Option<NonZeroU32>,
    pub(crate) x_forwarded_for: Option<NonZeroU32>,
    pub(crate) x_forwarded_proto: Option<NonZeroU32>,
    pub(crate) x_forwarded_host: Option<NonZeroU32>,
    pub(crate) x_forwarded_port: Option<NonZeroU32>,
    pub(crate) x_forwarded_prefix: Option<NonZeroU32>,
}

impl ProxyHeaderSlots {
    pub(crate) const fn index(slot: Option<NonZeroU32>) -> Option<usize> {
        match slot {
            Some(index) => Some(index.get() as usize - 1),
            None => None,
        }
    }

    const fn is_empty(&self) -> bool {
        self.forwarded.is_none()
            && self.x_forwarded_for.is_none()
            && self.x_forwarded_proto.is_none()
            && self.x_forwarded_host.is_none()
            && self.x_forwarded_port.is_none()
            && self.x_forwarded_prefix.is_none()
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct WebSocketHandshakeMeta {
    pub(crate) key: Option<[u8; WEBSOCKET_KEY_LEN]>,
    pub(crate) key_duplicate: bool,
    pub(crate) request: WebSocketRequestMeta,
    pub(crate) version_supported: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct RequestHeaderMeta {
    content_length: u64,
    flags: u8,
    rare: Option<Box<RequestHeaderSidecar>>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct RequestHeaderSidecar {
    websocket: WebSocketHandshakeMeta,
    proxy_headers: ProxyHeaderSlots,
}

impl RequestHeaderMeta {
    const HAS_CONTENT_LENGTH: u8 = 1 << 0;
    const ACCEPTS_TRAILERS: u8 = 1 << 1;

    pub(crate) const fn accepts_trailers(&self) -> bool {
        self.flags & Self::ACCEPTS_TRAILERS != 0
    }

    pub(crate) const fn set_accepts_trailers(&mut self) {
        self.flags |= Self::ACCEPTS_TRAILERS;
    }

    pub(crate) const fn content_length(&self) -> Option<u64> {
        if self.flags & Self::HAS_CONTENT_LENGTH != 0 {
            Some(self.content_length)
        } else {
            None
        }
    }

    pub(crate) const fn set_content_length(&mut self, content_length: Option<u64>) {
        if let Some(content_length) = content_length {
            self.content_length = content_length;
            self.flags |= Self::HAS_CONTENT_LENGTH;
        } else {
            self.content_length = 0;
            self.flags &= !Self::HAS_CONTENT_LENGTH;
        }
    }

    pub(crate) fn websocket(&self) -> Option<&WebSocketHandshakeMeta> {
        self.rare.as_deref().map(|rare| &rare.websocket)
    }

    pub(crate) fn proxy_headers(&self) -> Option<&ProxyHeaderSlots> {
        self.rare
            .as_deref()
            .map(|rare| &rare.proxy_headers)
            .filter(|headers| !headers.is_empty())
    }

    fn rare_mut(&mut self) -> &mut RequestHeaderSidecar {
        self.rare
            .get_or_insert_with(|| Box::new(RequestHeaderSidecar::default()))
    }

    pub(crate) fn observe_known_header(
        &mut self,
        known_name: KnownRequestHeaderName,
        value: &Bytes,
        index: usize,
    ) {
        self.observe_known_header_value(known_name, value.as_ref(), index);
        if known_name == KnownRequestHeaderName::SecWebSocketProtocol {
            push_requested_subprotocols(value, &mut self.rare_mut().websocket.request);
        }
    }

    pub(crate) fn observe_known_header_slice(
        &mut self,
        known_name: KnownRequestHeaderName,
        value: &[u8],
        index: usize,
    ) {
        self.observe_known_header_value(known_name, value, index);
        if known_name == KnownRequestHeaderName::SecWebSocketProtocol {
            push_copied_requested_subprotocols(value, &mut self.rare_mut().websocket.request);
        }
    }

    fn observe_known_header_value(
        &mut self,
        known_name: KnownRequestHeaderName,
        value_bytes: &[u8],
        index: usize,
    ) {
        let slot = || {
            let one_based = index
                .checked_add(1)
                .and_then(|value| u32::try_from(value).ok())
                .expect("a request cannot contain 2^32 materialized headers");
            NonZeroU32::new(one_based).expect("one-based header index is non-zero")
        };
        match known_name {
            KnownRequestHeaderName::Forwarded => {
                self.rare_mut().proxy_headers.forwarded = Some(slot());
            },
            KnownRequestHeaderName::XForwardedFor => {
                self.rare_mut().proxy_headers.x_forwarded_for = Some(slot());
            },
            KnownRequestHeaderName::XForwardedProto => {
                self.rare_mut().proxy_headers.x_forwarded_proto = Some(slot());
            },
            KnownRequestHeaderName::XForwardedHost => {
                self.rare_mut().proxy_headers.x_forwarded_host = Some(slot());
            },
            KnownRequestHeaderName::XForwardedPort => {
                self.rare_mut().proxy_headers.x_forwarded_port = Some(slot());
            },
            KnownRequestHeaderName::XForwardedPrefix => {
                self.rare_mut().proxy_headers.x_forwarded_prefix = Some(slot());
            },
            KnownRequestHeaderName::SecWebSocketVersion => {
                self.rare_mut().websocket.version_supported |= value_bytes == WEBSOCKET_VERSION;
            },
            KnownRequestHeaderName::SecWebSocketKey => {
                let websocket = &mut self.rare_mut().websocket;
                if websocket.key.is_some() {
                    websocket.key_duplicate = true;
                } else if let Ok(&key) = <&[u8; WEBSOCKET_KEY_LEN]>::try_from(value_bytes)
                    && websocket_key_is_syntactically_valid(value_bytes)
                {
                    websocket.key = Some(key);
                }
            },
            KnownRequestHeaderName::SecWebSocketExtensions => {
                self.rare_mut().websocket.request.per_message_deflate |=
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
    if from_utf8(value.as_ref()).is_err() {
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

fn push_copied_requested_subprotocols(value: &[u8], out: &mut WebSocketRequestMeta) {
    let Ok(value) = from_utf8(value) else {
        return;
    };
    out.requested_subprotocols.extend(
        value
            .split(',')
            .map(str::trim_ascii)
            .filter(|item| !item.is_empty())
            .map(BytesStr::from),
    );
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::{ProxyHeaderSlots, RequestHeaderMeta};
    use crate::hpack::BytesStr;
    use crate::http::types::KnownRequestHeaderName;
    use crate::websocket::RequestedSubprotocols;

    #[test]
    fn ordinary_header_metadata_stays_inline_without_a_rare_sidecar() {
        let mut meta = RequestHeaderMeta::default();
        meta.set_content_length(Some(0));
        meta.set_accepts_trailers();

        assert_eq!(meta.content_length(), Some(0));
        meta.set_content_length(Some(u64::MAX));
        assert_eq!(meta.content_length(), Some(u64::MAX));
        meta.set_content_length(None);
        assert_eq!(meta.content_length(), None);
        assert!(meta.accepts_trailers());
        assert!(meta.rare.is_none());
        assert!(meta.proxy_headers().is_none());
        assert!(meta.websocket().is_none());
    }

    #[test]
    fn proxy_header_slots_keep_the_last_match() {
        let mut meta = RequestHeaderMeta::default();
        meta.observe_known_header(KnownRequestHeaderName::Forwarded, &Bytes::new(), 2);
        meta.observe_known_header(KnownRequestHeaderName::Forwarded, &Bytes::new(), 9);
        meta.observe_known_header(KnownRequestHeaderName::XForwardedProto, &Bytes::new(), 4);
        meta.observe_known_header(KnownRequestHeaderName::XForwardedProto, &Bytes::new(), 7);
        let slots = *meta.proxy_headers().expect("proxy metadata sidecar exists");

        assert_eq!(ProxyHeaderSlots::index(slots.forwarded), Some(9));
        assert_eq!(ProxyHeaderSlots::index(slots.x_forwarded_proto), Some(7));
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

        let websocket = meta.websocket().expect("WebSocket metadata sidecar exists");
        assert!(websocket.version_supported);
        assert_eq!(websocket.key, Some(*b"dGhlIHNhbXBsZSBub25jZQ=="));
        let expected: RequestedSubprotocols = smallvec::smallvec![
            BytesStr::from_static("chat"),
            BytesStr::from_static("superchat"),
        ];
        assert_eq!(websocket.request.requested_subprotocols, expected);
        assert!(websocket.request.per_message_deflate);
    }
}
