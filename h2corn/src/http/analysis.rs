use bytes::Bytes;

use crate::hpack::BytesStr;
use crate::http::types::KnownRequestHeaderName;
use crate::websocket::{
    WEBSOCKET_KEY_LEN, WEBSOCKET_VERSION, WebSocketRequestMeta,
    websocket_requested_permessage_deflate,
};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum RequestBodyFraming {
    #[default]
    None,
    ContentLength,
    Chunked,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum HostHeaderSource {
    #[default]
    None,
    Header,
    Synthesized,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct HostHeaderAnalysis {
    pub(crate) index: Option<usize>,
    pub(crate) source: HostHeaderSource,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ProxyHeaderSlots {
    pub(crate) forwarded: Option<usize>,
    pub(crate) x_forwarded_for: Option<usize>,
    pub(crate) x_forwarded_proto: Option<usize>,
    pub(crate) x_forwarded_host: Option<usize>,
    pub(crate) x_forwarded_port: Option<usize>,
    pub(crate) x_forwarded_prefix: Option<usize>,
}

impl ProxyHeaderSlots {
    pub(crate) fn observe(&mut self, name: KnownRequestHeaderName, index: usize) {
        match name {
            KnownRequestHeaderName::Forwarded => {
                self.forwarded = Some(index);
            }
            KnownRequestHeaderName::XForwardedFor => {
                self.x_forwarded_for = Some(index);
            }
            KnownRequestHeaderName::XForwardedProto => {
                self.x_forwarded_proto = Some(index);
            }
            KnownRequestHeaderName::XForwardedHost => {
                self.x_forwarded_host = Some(index);
            }
            KnownRequestHeaderName::XForwardedPort => {
                self.x_forwarded_port = Some(index);
            }
            KnownRequestHeaderName::XForwardedPrefix => {
                self.x_forwarded_prefix = Some(index);
            }
            _ => {}
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct WebSocketRequestAnalysis {
    pub(crate) key: Option<[u8; WEBSOCKET_KEY_LEN]>,
    pub(crate) meta: WebSocketRequestMeta,
    pub(crate) version_supported: bool,
}

impl WebSocketRequestAnalysis {
    pub(crate) fn observe(&mut self, name: KnownRequestHeaderName, value: &Bytes) {
        let value_bytes = value.as_ref();
        match name {
            KnownRequestHeaderName::SecWebSocketVersion => {
                self.version_supported |= value_bytes == WEBSOCKET_VERSION;
            }
            KnownRequestHeaderName::SecWebSocketKey if self.key.is_none() => {
                if let Ok(&key) = <&[u8; WEBSOCKET_KEY_LEN]>::try_from(value_bytes) {
                    self.key = Some(key);
                }
            }
            KnownRequestHeaderName::SecWebSocketProtocol => {
                push_requested_subprotocols(value, &mut self.meta);
            }
            KnownRequestHeaderName::SecWebSocketExtensions => {
                self.meta.per_message_deflate |=
                    websocket_requested_permessage_deflate(value_bytes);
            }
            _ => {}
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct RequestHeaderAnalysis {
    pub(crate) body_framing: RequestBodyFraming,
    pub(crate) accepts_trailers: bool,
    pub(crate) content_length: Option<u64>,
    pub(crate) connection_close: bool,
    pub(crate) websocket: WebSocketRequestAnalysis,
    pub(crate) proxy_headers: ProxyHeaderSlots,
    pub(crate) host_header: HostHeaderAnalysis,
}

fn push_requested_subprotocols(value: &Bytes, out: &mut WebSocketRequestMeta) {
    if std::str::from_utf8(value.as_ref()).is_err() {
        return;
    }

    out.requested_subprotocols.extend(
        value
            .as_ref()
            .split(|&byte| byte == b',')
            .map(|item| item.trim_ascii())
            .filter(|item| !item.is_empty())
            .map(|item| {
                BytesStr::try_from(value.slice_ref(item))
                    .expect("validated websocket subprotocol is UTF-8")
            }),
    );
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::{ProxyHeaderSlots, WebSocketRequestAnalysis};
    use crate::hpack::BytesStr;
    use crate::http::types::KnownRequestHeaderName;
    use crate::websocket::RequestedSubprotocols;

    #[test]
    fn proxy_header_slots_keep_the_last_match() {
        let mut slots = ProxyHeaderSlots::default();

        slots.observe(KnownRequestHeaderName::Forwarded, 2);
        slots.observe(KnownRequestHeaderName::Forwarded, 9);
        slots.observe(KnownRequestHeaderName::XForwardedProto, 4);
        slots.observe(KnownRequestHeaderName::XForwardedProto, 7);

        assert_eq!(slots.forwarded, Some(9));
        assert_eq!(slots.x_forwarded_proto, Some(7));
    }

    #[test]
    fn websocket_analysis_parses_version_key_subprotocols_and_extensions() {
        let mut analysis = WebSocketRequestAnalysis::default();

        analysis.observe(
            KnownRequestHeaderName::SecWebSocketVersion,
            &Bytes::from_static(b"13"),
        );
        analysis.observe(
            KnownRequestHeaderName::SecWebSocketKey,
            &Bytes::from_static(b"dGhlIHNhbXBsZSBub25jZQ=="),
        );
        analysis.observe(
            KnownRequestHeaderName::SecWebSocketProtocol,
            &Bytes::from_static(b"chat, superchat"),
        );
        analysis.observe(
            KnownRequestHeaderName::SecWebSocketExtensions,
            &Bytes::from_static(b"permessage-deflate"),
        );

        assert!(analysis.version_supported);
        assert_eq!(analysis.key, Some(*b"dGhlIHNhbXBsZSBub25jZQ=="));
        let expected: RequestedSubprotocols = smallvec::smallvec![
            BytesStr::from_static("chat"),
            BytesStr::from_static("superchat"),
        ];
        assert_eq!(analysis.meta.requested_subprotocols, expected);
        assert!(analysis.meta.per_message_deflate);
    }
}
