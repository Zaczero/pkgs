use std::str;

use bytes::Bytes;
use http::Method;
use pyo3::pybacked::PyBackedBytes;

use crate::ext::Protocol;
use crate::header_value::header_value_is_valid;
use crate::hpack::BytesStr;
use crate::http::header::{
    protocol_is_websocket, request_header_name_needs_lowercase, response_header_name_is_valid,
};
use crate::http::header_meta::RequestHeaderMeta;

pub(crate) type RequestHeaders = Vec<(RequestHeaderName, RequestHeaderValue)>;
pub(crate) type ResponseHeaders = Vec<(ResponseHeaderName, ResponseHeaderValue)>;
pub(crate) type HttpStatusCode = u16;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RequestAuthority(BytesStr);

impl RequestAuthority {
    pub(crate) fn new(value: BytesStr) -> Self {
        Self(value)
    }

    pub(crate) fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        self.0.as_ref()
    }

    pub(crate) fn as_bytes_str(&self) -> &BytesStr {
        &self.0
    }

    pub(crate) fn into_bytes_str(self) -> BytesStr {
        self.0
    }
}

impl AsRef<[u8]> for RequestAuthority {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl AsRef<str> for RequestAuthority {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RequestTarget {
    Normal {
        scheme: BytesStr,
        path_and_query: BytesStr,
    },
    Connect {
        authority: RequestAuthority,
        protocol: Option<Protocol>,
        scheme: Option<BytesStr>,
        path_and_query: Option<BytesStr>,
    },
}

impl RequestTarget {
    pub(crate) fn normal(scheme: BytesStr, path_and_query: BytesStr) -> Self {
        Self::Normal {
            scheme,
            path_and_query,
        }
    }

    pub(crate) fn connect(authority: RequestAuthority) -> Self {
        Self::Connect {
            authority,
            protocol: None,
            scheme: None,
            path_and_query: None,
        }
    }

    pub(crate) fn extended_connect(
        authority: RequestAuthority,
        protocol: Protocol,
        scheme: BytesStr,
        path_and_query: BytesStr,
    ) -> Self {
        Self::Connect {
            authority,
            protocol: Some(protocol),
            scheme: Some(scheme),
            path_and_query: Some(path_and_query),
        }
    }

    pub(crate) fn authority(&self) -> Option<&RequestAuthority> {
        match self {
            Self::Normal { .. } => None,
            Self::Connect { authority, .. } => Some(authority),
        }
    }

    pub(crate) fn scheme(&self) -> Option<&BytesStr> {
        match self {
            Self::Normal { scheme, .. } => Some(scheme),
            Self::Connect { scheme, .. } => scheme.as_ref(),
        }
    }

    pub(crate) fn scheme_str(&self) -> &str {
        self.scheme().map_or("", BytesStr::as_str)
    }

    pub(crate) fn path_and_query(&self) -> Option<&BytesStr> {
        match self {
            Self::Normal { path_and_query, .. } => Some(path_and_query),
            Self::Connect { path_and_query, .. } => path_and_query.as_ref(),
        }
    }

    pub(crate) fn protocol(&self) -> Option<&Protocol> {
        match self {
            Self::Normal { .. } => None,
            Self::Connect { protocol, .. } => protocol.as_ref(),
        }
    }

    pub(crate) fn is_connect(&self) -> bool {
        matches!(self, Self::Connect { .. })
    }

    pub(crate) fn protocol_is_websocket(&self) -> bool {
        self.protocol().is_some_and(protocol_is_websocket)
    }

    pub(crate) fn log_target(&self) -> &BytesStr {
        match self {
            Self::Normal { path_and_query, .. } => path_and_query,
            Self::Connect {
                authority,
                path_and_query,
                ..
            } => path_and_query
                .as_ref()
                .unwrap_or_else(|| authority.as_bytes_str()),
        }
    }
}

macro_rules! known_request_header_names {
    ($(($variant:ident, $name:literal)),+ $(,)?) => {
        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        pub(crate) enum KnownRequestHeaderName {
            $($variant),+
        }

        impl KnownRequestHeaderName {
            pub(crate) fn from_bytes(name: &[u8]) -> Option<Self> {
                match name {
                    $($name => Some(Self::$variant),)+
                    _ => None,
                }
            }

            pub(crate) fn from_bytes_ignore_ascii_case(name: &[u8]) -> Option<Self> {
                $(
                    if name.eq_ignore_ascii_case($name) {
                        return Some(Self::$variant);
                    }
                )+
                None
            }

            pub(crate) const fn as_bytes(self) -> &'static [u8] {
                match self {
                    $(Self::$variant => $name,)+
                }
            }

            pub(crate) fn as_str(self) -> &'static str {
                // SAFETY: all names are ASCII byte literals.
                unsafe { str::from_utf8_unchecked(self.as_bytes()) }
            }
        }
    };
}

known_request_header_names! {
    (Host, b"host"),
    (Connection, b"connection"),
    (ProxyConnection, b"proxy-connection"),
    (KeepAlive, b"keep-alive"),
    (Upgrade, b"upgrade"),
    (Te, b"te"),
    (ContentLength, b"content-length"),
    (TransferEncoding, b"transfer-encoding"),
    (Expect, b"expect"),
    (Http2Settings, b"http2-settings"),
    (Forwarded, b"forwarded"),
    (XForwardedFor, b"x-forwarded-for"),
    (XForwardedProto, b"x-forwarded-proto"),
    (XForwardedHost, b"x-forwarded-host"),
    (XForwardedPort, b"x-forwarded-port"),
    (XForwardedPrefix, b"x-forwarded-prefix"),
    (SecWebSocketVersion, b"sec-websocket-version"),
    (SecWebSocketKey, b"sec-websocket-key"),
    (SecWebSocketProtocol, b"sec-websocket-protocol"),
    (SecWebSocketExtensions, b"sec-websocket-extensions"),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RequestHeaderName {
    Known(KnownRequestHeaderName),
    Other(BytesStr),
}

impl RequestHeaderName {
    pub(crate) fn from_h1(head: &Bytes, name: &[u8]) -> Option<Self> {
        let needs_lowercase = request_header_name_needs_lowercase(name)?;

        if let Some(name) = if needs_lowercase {
            KnownRequestHeaderName::from_bytes_ignore_ascii_case(name)
        } else {
            KnownRequestHeaderName::from_bytes(name)
        } {
            return Some(Self::Known(name));
        }

        let bytes = if needs_lowercase {
            Bytes::from(name.to_ascii_lowercase())
        } else {
            head.slice_ref(name)
        };
        let bytes = BytesStr::try_from(bytes).expect("validated header names are valid UTF-8");
        if let Some(name) = KnownRequestHeaderName::from_bytes(bytes.as_ref()) {
            Some(Self::Known(name))
        } else {
            Some(Self::Other(bytes))
        }
    }

    pub(crate) fn from_h2_validated(name: BytesStr) -> Self {
        if let Some(known) = KnownRequestHeaderName::from_bytes(name.as_ref()) {
            Self::Known(known)
        } else {
            Self::Other(name)
        }
    }

    pub(crate) fn as_str(&self) -> &str {
        match self {
            Self::Known(name) => name.as_str(),
            Self::Other(name) => name.as_str(),
        }
    }

    pub(crate) fn known(&self) -> Option<KnownRequestHeaderName> {
        match self {
            Self::Known(name) => Some(*name),
            Self::Other(_) => None,
        }
    }
}

impl AsRef<[u8]> for RequestHeaderName {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Known(name) => name.as_bytes(),
            Self::Other(name) => name.as_ref(),
        }
    }
}

impl AsRef<str> for RequestHeaderName {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RequestHeaderValue(Bytes);

impl RequestHeaderValue {
    pub(crate) fn from_h1(head: &Bytes, value: &[u8]) -> Option<Self> {
        if !header_value_is_valid(value) {
            return None;
        }
        Some(Self(head.slice_ref(value)))
    }

    pub(crate) fn from_bytes(value: Bytes) -> Option<Self> {
        if !header_value_is_valid(value.as_ref()) {
            return None;
        }
        Some(Self(value))
    }

    pub(crate) fn from_h2_validated(value: Bytes) -> Self {
        Self(value)
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        self.0.as_ref()
    }

    pub(crate) fn inner(&self) -> &Bytes {
        &self.0
    }
}

impl AsRef<[u8]> for RequestHeaderValue {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl From<KnownRequestHeaderName> for RequestHeaderName {
    fn from(value: KnownRequestHeaderName) -> Self {
        Self::Known(value)
    }
}

#[derive(Debug)]
enum HeaderBytes {
    Rust(Bytes),
    Python(PyBackedBytes),
}

impl HeaderBytes {
    pub(crate) fn as_slice(&self) -> &[u8] {
        match self {
            Self::Rust(bytes) => bytes.as_ref(),
            Self::Python(bytes) => bytes.as_ref(),
        }
    }
}

impl AsRef<[u8]> for HeaderBytes {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl From<Bytes> for HeaderBytes {
    fn from(value: Bytes) -> Self {
        Self::Rust(value)
    }
}

impl From<PyBackedBytes> for HeaderBytes {
    fn from(value: PyBackedBytes) -> Self {
        Self::Python(value)
    }
}

#[derive(Debug)]
pub(crate) struct ResponseHeaderName(HeaderBytes);

impl ResponseHeaderName {
    pub(crate) fn from_python(value: PyBackedBytes) -> Option<Self> {
        response_header_name_is_valid(value.as_ref()).then_some(Self(value.into()))
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        self.0.as_slice()
    }
}

impl AsRef<[u8]> for ResponseHeaderName {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl From<Bytes> for ResponseHeaderName {
    fn from(value: Bytes) -> Self {
        assert!(response_header_name_is_valid(value.as_ref()));
        Self(value.into())
    }
}

#[derive(Debug)]
pub(crate) struct ResponseHeaderValue(HeaderBytes);

impl ResponseHeaderValue {
    pub(crate) fn from_python(value: PyBackedBytes) -> Option<Self> {
        header_value_is_valid(value.as_ref()).then_some(Self(value.into()))
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        self.0.as_slice()
    }
}

impl AsRef<[u8]> for ResponseHeaderValue {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl From<Bytes> for ResponseHeaderValue {
    fn from(value: Bytes) -> Self {
        assert!(header_value_is_valid(value.as_ref()));
        Self(value.into())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HttpVersion {
    Http1_1,
    Http2,
}

impl HttpVersion {
    pub(crate) const fn log_label(self) -> &'static str {
        match self {
            Self::Http1_1 => "HTTP/1.1",
            Self::Http2 => "HTTP/2",
        }
    }
}

pub(crate) mod status_code {
    use super::HttpStatusCode;

    pub(crate) const SWITCHING_PROTOCOLS: HttpStatusCode = 101;
    pub(crate) const OK: HttpStatusCode = 200;
    pub(crate) const NO_CONTENT: HttpStatusCode = 204;
    pub(crate) const PARTIAL_CONTENT: HttpStatusCode = 206;
    pub(crate) const NOT_MODIFIED: HttpStatusCode = 304;
    pub(crate) const BAD_REQUEST: HttpStatusCode = 400;
    pub(crate) const FORBIDDEN: HttpStatusCode = 403;
    pub(crate) const NOT_FOUND: HttpStatusCode = 404;
    pub(crate) const PAYLOAD_TOO_LARGE: HttpStatusCode = 413;
    pub(crate) const URI_TOO_LONG: HttpStatusCode = 414;
    pub(crate) const SERVICE_UNAVAILABLE: HttpStatusCode = 503;
    pub(crate) const UPGRADE_REQUIRED: HttpStatusCode = 426;
    pub(crate) const REQUEST_HEADER_FIELDS_TOO_LARGE: HttpStatusCode = 431;
    pub(crate) const INTERNAL_SERVER_ERROR: HttpStatusCode = 500;
    pub(crate) const NOT_IMPLEMENTED: HttpStatusCode = 501;
}

#[derive(Clone, Debug)]
pub(crate) struct RequestHead {
    pub http_version: HttpVersion,
    pub method: Method,
    pub target: RequestTarget,
    pub headers: RequestHeaders,
    pub header_meta: RequestHeaderMeta,
}

impl RequestHead {
    pub(crate) fn scheme(&self) -> Option<&BytesStr> {
        self.target.scheme()
    }

    pub(crate) fn path_and_query(&self) -> Option<&BytesStr> {
        self.target.path_and_query()
    }

    pub(crate) fn scheme_str(&self) -> &str {
        self.target.scheme_str()
    }

    pub(crate) fn is_connect(&self) -> bool {
        self.target.is_connect()
    }

    pub(crate) fn protocol_is_websocket(&self) -> bool {
        self.target.protocol_is_websocket()
    }

    pub(crate) fn log_target(&self) -> &BytesStr {
        self.target.log_target()
    }

    pub(crate) fn accepts_trailers(&self) -> bool {
        self.header_meta.accepts_trailers
    }

    pub(crate) fn content_length(&self) -> Option<u64> {
        self.header_meta.content_length
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::http::header_meta::RequestHeaderMeta;

    use super::{
        HttpVersion, KnownRequestHeaderName, RequestAuthority, RequestHead, RequestHeaderName,
        RequestTarget,
    };
    use crate::ext::Protocol;
    use crate::hpack::BytesStr;
    use http::Method;

    #[test]
    fn normal_request_target_exposes_scheme_path_and_log_target() {
        let target = RequestTarget::normal(
            BytesStr::from_static("https"),
            BytesStr::from_static("/demo"),
        );

        assert_eq!(target.scheme_str(), "https");
        assert_eq!(target.path_and_query().map(BytesStr::as_str), Some("/demo"));
        assert_eq!(target.protocol(), None);
        assert_eq!(target.log_target().as_str(), "/demo");
    }

    #[test]
    fn connect_request_target_without_path_uses_authority_for_logging() {
        let authority = RequestAuthority::new(BytesStr::from_static("example.com:443"));
        let target = RequestTarget::connect(authority);

        assert_eq!(target.scheme_str(), "");
        assert_eq!(target.path_and_query().map(BytesStr::as_str), None);
        assert_eq!(target.protocol(), None);
        assert!(target.is_connect());
        assert_eq!(target.log_target().as_str(), "example.com:443");
    }

    #[test]
    fn extended_connect_request_target_preserves_optional_fields() {
        let target = RequestTarget::extended_connect(
            RequestAuthority::new(BytesStr::from_static("example.com:443")),
            Protocol::from("websocket"),
            BytesStr::from_static("https"),
            BytesStr::from_static("/chat?room=blue"),
        );

        assert_eq!(target.scheme_str(), "https");
        assert_eq!(
            target.path_and_query().map(BytesStr::as_str),
            Some("/chat?room=blue")
        );
        assert_eq!(target.protocol().map(Protocol::as_str), Some("websocket"));
        assert!(target.protocol_is_websocket());
        assert_eq!(target.log_target().as_str(), "/chat?room=blue");
    }

    #[test]
    fn request_head_delegates_to_request_target_accessors() {
        let request = RequestHead {
            http_version: HttpVersion::Http2,
            method: Method::CONNECT,
            target: RequestTarget::extended_connect(
                RequestAuthority::new(BytesStr::from_static("example.com:443")),
                Protocol::from("websocket"),
                BytesStr::from_static("https"),
                BytesStr::from_static("/chat"),
            ),
            headers: Vec::new(),
            header_meta: RequestHeaderMeta::default(),
        };

        assert_eq!(request.scheme_str(), "https");
        assert_eq!(
            request.path_and_query().map(BytesStr::as_str),
            Some("/chat")
        );
        assert!(request.is_connect());
        assert!(request.protocol_is_websocket());
        assert_eq!(request.log_target().as_str(), "/chat");
    }

    #[test]
    fn from_h1_recognizes_known_headers_in_mixed_case() {
        let head = Bytes::from_static(b"Host: example.com\r\n");

        assert_eq!(
            RequestHeaderName::from_h1(&head, b"Host"),
            Some(RequestHeaderName::Known(KnownRequestHeaderName::Host))
        );
        assert_eq!(
            RequestHeaderName::from_h1(&head, b"HOST"),
            Some(RequestHeaderName::Known(KnownRequestHeaderName::Host))
        );
    }
}
