use std::num::NonZeroU32;

use bitflags::bitflags;
use bytes::Bytes;

use crate::base64;
use crate::http::header::protocol_token_is_valid;
use crate::http::types::{BytesStr, KnownRequestHeaderName};
use crate::websocket::{
    ParsedRequestedSubprotocols, ParsedWebSocketRequestMeta, WEBSOCKET_KEY_LEN, WEBSOCKET_VERSION,
    WebSocketKey, offers_compatible_permessage_deflate,
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

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum ParsedWebSocketKey {
    #[default]
    Missing,
    Valid(WebSocketKey),
    Invalid,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum ParsedWebSocketVersion {
    #[default]
    Missing,
    Supported,
    Unsupported,
    Duplicate,
}

impl ParsedWebSocketVersion {
    pub(crate) const fn is_unsupported(self) -> bool {
        matches!(self, Self::Missing | Self::Unsupported)
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct WebSocketHandshakeMeta {
    pub(crate) key: ParsedWebSocketKey,
    pub(crate) request: ParsedWebSocketRequestMeta,
    pub(crate) version: ParsedWebSocketVersion,
}

bitflags! {
    #[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
    pub(crate) struct RequestHeaderFlags: u8 {
        const HAS_CONTENT_LENGTH = 1 << 0;
        const ACCEPTS_TRAILERS = 1 << 1;
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct RequestHeaderMeta {
    content_length: u64,
    flags: RequestHeaderFlags,
    rare: Option<Box<RequestHeaderSidecar>>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct RequestHeaderSidecar {
    websocket: WebSocketHandshakeMeta,
    proxy_headers: ProxyHeaderSlots,
}

impl RequestHeaderMeta {
    pub(crate) const fn accepts_trailers(&self) -> bool {
        self.flags.contains(RequestHeaderFlags::ACCEPTS_TRAILERS)
    }

    pub(crate) const fn set_accepts_trailers(&mut self) {
        self.flags = self.flags.union(RequestHeaderFlags::ACCEPTS_TRAILERS);
    }

    pub(crate) const fn content_length(&self) -> Option<u64> {
        if self.flags.contains(RequestHeaderFlags::HAS_CONTENT_LENGTH) {
            Some(self.content_length)
        } else {
            None
        }
    }

    /// Record a declared body length, rejecting a second, different one.
    ///
    /// RFC 9112 6.3 rule 5 and RFC 9113 8.1.1 state the same rule for both
    /// protocols, so it lives on the type that owns the field; each protocol
    /// maps the unit error to its own wire error.
    pub(crate) const fn try_set_content_length(&mut self, content_length: u64) -> Result<(), ()> {
        if let Some(existing) = self.content_length()
            && existing != content_length
        {
            return Err(());
        }
        self.set_content_length(Some(content_length));
        Ok(())
    }

    pub(crate) const fn set_content_length(&mut self, content_length: Option<u64>) {
        if let Some(content_length) = content_length {
            self.content_length = content_length;
            self.flags = self.flags.union(RequestHeaderFlags::HAS_CONTENT_LENGTH);
        } else {
            self.content_length = 0;
            self.flags = self
                .flags
                .difference(RequestHeaderFlags::HAS_CONTENT_LENGTH);
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
        collect_proxy_headers: bool,
    ) {
        self.observe_known_header_value(known_name, value.as_ref(), index, collect_proxy_headers);
        if known_name == KnownRequestHeaderName::SecWebSocketProtocol {
            push_requested_subprotocols(
                value.as_ref(),
                |item| value.slice_ref(item),
                &mut self.rare_mut().websocket.request,
            );
        }
    }

    pub(crate) fn observe_known_header_slice(
        &mut self,
        known_name: KnownRequestHeaderName,
        value: &[u8],
        index: usize,
        collect_proxy_headers: bool,
    ) {
        self.observe_known_header_value(known_name, value, index, collect_proxy_headers);
        if known_name == KnownRequestHeaderName::SecWebSocketProtocol {
            push_requested_subprotocols(
                value,
                Bytes::copy_from_slice,
                &mut self.rare_mut().websocket.request,
            );
        }
    }

    fn observe_known_header_value(
        &mut self,
        known_name: KnownRequestHeaderName,
        value_bytes: &[u8],
        index: usize,
        collect_proxy_headers: bool,
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
                if collect_proxy_headers {
                    self.rare_mut().proxy_headers.forwarded = Some(slot());
                }
            },
            KnownRequestHeaderName::XForwardedFor => {
                if collect_proxy_headers {
                    self.rare_mut().proxy_headers.x_forwarded_for = Some(slot());
                }
            },
            KnownRequestHeaderName::XForwardedProto => {
                if collect_proxy_headers {
                    self.rare_mut().proxy_headers.x_forwarded_proto = Some(slot());
                }
            },
            KnownRequestHeaderName::XForwardedHost => {
                if collect_proxy_headers {
                    self.rare_mut().proxy_headers.x_forwarded_host = Some(slot());
                }
            },
            KnownRequestHeaderName::XForwardedPort => {
                if collect_proxy_headers {
                    self.rare_mut().proxy_headers.x_forwarded_port = Some(slot());
                }
            },
            KnownRequestHeaderName::XForwardedPrefix => {
                if collect_proxy_headers {
                    self.rare_mut().proxy_headers.x_forwarded_prefix = Some(slot());
                }
            },
            KnownRequestHeaderName::SecWebSocketVersion => {
                let version = &mut self.rare_mut().websocket.version;
                *version = match *version {
                    ParsedWebSocketVersion::Missing if value_bytes == WEBSOCKET_VERSION => {
                        ParsedWebSocketVersion::Supported
                    },
                    ParsedWebSocketVersion::Missing => ParsedWebSocketVersion::Unsupported,
                    _ => ParsedWebSocketVersion::Duplicate,
                };
            },
            KnownRequestHeaderName::SecWebSocketKey => {
                let key = &mut self.rare_mut().websocket.key;
                *key = match *key {
                    ParsedWebSocketKey::Missing
                        if let Ok(&key) = <&[u8; WEBSOCKET_KEY_LEN]>::try_from(value_bytes)
                            && websocket_key_is_syntactically_valid(value_bytes) =>
                    {
                        ParsedWebSocketKey::Valid(key)
                    },
                    _ => ParsedWebSocketKey::Invalid,
                };
            },
            KnownRequestHeaderName::SecWebSocketExtensions => {
                self.rare_mut().websocket.request.per_message_deflate |=
                    offers_compatible_permessage_deflate(value_bytes);
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
            .all(|byte| base64::is_base64(*byte))
}

/// Parse one `Sec-WebSocket-Protocol` field value into `out`.
///
/// `own` is the only thing that differed between the HTTP/1 and HTTP/2 copies
/// of this loop: HTTP/2 slices the connection buffer the field already lives
/// in, while HTTP/1 copies because its parse buffer is reused for the next
/// request. Everything else — splitting, trimming, skipping empty items,
/// token validation, and poisoning the whole field on a bad token — was
/// duplicated, so a change to the token rules had to be made twice.
fn push_requested_subprotocols(
    value: &[u8],
    own: impl Fn(&[u8]) -> Bytes,
    out: &mut ParsedWebSocketRequestMeta,
) {
    let tokens = || {
        value
            .split(|&byte| byte == b',')
            .map(<[u8]>::trim_ascii)
            .filter(|item| !item.is_empty())
    };
    // Validated before the list is borrowed, so one bad token poisons the
    // field without leaving a half-appended list behind.
    if !tokens().all(protocol_token_is_valid) {
        out.requested_subprotocols = ParsedRequestedSubprotocols::Invalid;
        return;
    }
    let ParsedRequestedSubprotocols::Valid(requested) = &mut out.requested_subprotocols else {
        // An earlier field already poisoned this one.
        return;
    };
    for item in tokens() {
        // SAFETY: an HTTP token is ASCII and therefore valid UTF-8.
        requested.push(unsafe { BytesStr::from_validated_ascii(own(item)) });
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::{ParsedWebSocketKey, ParsedWebSocketVersion, ProxyHeaderSlots, RequestHeaderMeta};
    use crate::http::types::{BytesStr, KnownRequestHeaderName};
    use crate::websocket::{ParsedRequestedSubprotocols, RequestedSubprotocols};

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
        meta.observe_known_header(KnownRequestHeaderName::Forwarded, &Bytes::new(), 2, true);
        meta.observe_known_header(KnownRequestHeaderName::Forwarded, &Bytes::new(), 9, true);
        meta.observe_known_header(
            KnownRequestHeaderName::XForwardedProto,
            &Bytes::new(),
            4,
            true,
        );
        meta.observe_known_header(
            KnownRequestHeaderName::XForwardedProto,
            &Bytes::new(),
            7,
            true,
        );
        let slots = *meta.proxy_headers().expect("proxy metadata sidecar exists");

        assert_eq!(ProxyHeaderSlots::index(slots.forwarded), Some(9));
        assert_eq!(ProxyHeaderSlots::index(slots.x_forwarded_proto), Some(7));
    }

    #[test]
    fn untrusted_proxy_headers_do_not_allocate_the_rare_sidecar() {
        let mut meta = RequestHeaderMeta::default();
        meta.observe_known_header(
            KnownRequestHeaderName::XForwardedFor,
            &Bytes::from_static(b"198.51.100.9"),
            0,
            false,
        );

        assert!(meta.proxy_headers().is_none());
        assert!(meta.rare.is_none());
    }

    #[test]
    fn websocket_handshake_meta_parses_version_key_subprotocols_and_extensions() {
        let mut meta = RequestHeaderMeta::default();

        meta.observe_known_header(
            KnownRequestHeaderName::SecWebSocketVersion,
            &Bytes::from_static(b"13"),
            0,
            true,
        );
        meta.observe_known_header(
            KnownRequestHeaderName::SecWebSocketKey,
            &Bytes::from_static(b"dGhlIHNhbXBsZSBub25jZQ=="),
            1,
            true,
        );
        meta.observe_known_header(
            KnownRequestHeaderName::SecWebSocketProtocol,
            &Bytes::from_static(b"chat, superchat"),
            2,
            true,
        );
        meta.observe_known_header(
            KnownRequestHeaderName::SecWebSocketExtensions,
            &Bytes::from_static(b"permessage-deflate"),
            3,
            true,
        );

        let websocket = meta.websocket().expect("WebSocket metadata sidecar exists");
        assert_eq!(websocket.version, ParsedWebSocketVersion::Supported);
        assert_eq!(
            websocket.key,
            ParsedWebSocketKey::Valid(*b"dGhlIHNhbXBsZSBub25jZQ==")
        );
        let expected: RequestedSubprotocols = smallvec::smallvec![
            BytesStr::from_static("chat"),
            BytesStr::from_static("superchat"),
        ];
        assert_eq!(
            websocket.request.requested_subprotocols,
            ParsedRequestedSubprotocols::Valid(expected)
        );
        assert!(websocket.request.per_message_deflate);
    }

    #[test]
    fn websocket_key_is_invalid_after_a_malformed_or_second_occurrence() {
        let valid = Bytes::from_static(b"dGhlIHNhbXBsZSBub25jZQ==");
        let malformed = Bytes::from_static(b"not-a-websocket-key");

        for (first, second) in [(&malformed, &valid), (&valid, &malformed), (&valid, &valid)] {
            let mut meta = RequestHeaderMeta::default();
            meta.observe_known_header(KnownRequestHeaderName::SecWebSocketKey, first, 0, true);
            meta.observe_known_header(KnownRequestHeaderName::SecWebSocketKey, second, 1, true);
            assert_eq!(
                meta.websocket()
                    .expect("key creates handshake metadata")
                    .key,
                ParsedWebSocketKey::Invalid
            );
        }
    }

    #[test]
    fn websocket_version_is_duplicate_after_any_second_occurrence() {
        for (first, second) in [
            (b"12".as_slice(), b"13".as_slice()),
            (b"13", b"12"),
            (b"13", b"13"),
        ] {
            let mut meta = RequestHeaderMeta::default();
            meta.observe_known_header_slice(
                KnownRequestHeaderName::SecWebSocketVersion,
                first,
                0,
                true,
            );
            meta.observe_known_header_slice(
                KnownRequestHeaderName::SecWebSocketVersion,
                second,
                1,
                true,
            );
            assert_eq!(
                meta.websocket()
                    .expect("version creates handshake metadata")
                    .version,
                ParsedWebSocketVersion::Duplicate
            );
        }
    }
}
