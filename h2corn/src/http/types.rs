#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "the generated known-name table follows its compile-time grammar checker"
)]

pub(crate) mod status_code {
    use crate::http::types::HttpStatusCode;

    pub(crate) const SWITCHING_PROTOCOLS: HttpStatusCode = HttpStatusCode::constant(101);
    /// Deliberately outside `common_status_codes!`: 103 is never a final
    /// status and never an HTTP/1 response line, so it has no entry there.
    pub(crate) const EARLY_HINTS: HttpStatusCode = HttpStatusCode::constant(103);
    pub(crate) const OK: HttpStatusCode = HttpStatusCode::constant(200);
    pub(crate) const NO_CONTENT: HttpStatusCode = HttpStatusCode::constant(204);
    pub(crate) const PARTIAL_CONTENT: HttpStatusCode = HttpStatusCode::constant(206);
    pub(crate) const NOT_MODIFIED: HttpStatusCode = HttpStatusCode::constant(304);
    pub(crate) const BAD_REQUEST: HttpStatusCode = HttpStatusCode::constant(400);
    pub(crate) const FORBIDDEN: HttpStatusCode = HttpStatusCode::constant(403);
    pub(crate) const NOT_FOUND: HttpStatusCode = HttpStatusCode::constant(404);
    pub(crate) const PAYLOAD_TOO_LARGE: HttpStatusCode = HttpStatusCode::constant(413);
    pub(crate) const URI_TOO_LONG: HttpStatusCode = HttpStatusCode::constant(414);
    pub(crate) const UPGRADE_REQUIRED: HttpStatusCode = HttpStatusCode::constant(426);
    pub(crate) const REQUEST_HEADER_FIELDS_TOO_LARGE: HttpStatusCode =
        HttpStatusCode::constant(431);
    pub(crate) const INTERNAL_SERVER_ERROR: HttpStatusCode = HttpStatusCode::constant(500);
    pub(crate) const NOT_IMPLEMENTED: HttpStatusCode = HttpStatusCode::constant(501);
    pub(crate) const SERVICE_UNAVAILABLE: HttpStatusCode = HttpStatusCode::constant(503);
}

/// Statuses with a fixed HTTP/1 line. The final value is the HPACK static
/// table encoding when one exists, otherwise zero. Both protocol writers are
/// generated from this list so adding a common status is one decision.
macro_rules! common_status_codes {
    ($emit:ident) => {
        $emit! {
            (SWITCHING_PROTOCOLS, b"HTTP/1.1 101 Switching Protocols\r\n", 0),
            (OK, b"HTTP/1.1 200 OK\r\n", 0x88),
            (NO_CONTENT, b"HTTP/1.1 204 No Content\r\n", 0x89),
            (PARTIAL_CONTENT, b"HTTP/1.1 206 Partial Content\r\n", 0x8A),
            (NOT_MODIFIED, b"HTTP/1.1 304 Not Modified\r\n", 0x8B),
            (BAD_REQUEST, b"HTTP/1.1 400 Bad Request\r\n", 0x8C),
            (FORBIDDEN, b"HTTP/1.1 403 Forbidden\r\n", 0),
            (NOT_FOUND, b"HTTP/1.1 404 Not Found\r\n", 0x8D),
            (PAYLOAD_TOO_LARGE, b"HTTP/1.1 413 Payload Too Large\r\n", 0),
            (URI_TOO_LONG, b"HTTP/1.1 414 URI Too Long\r\n", 0),
            (UPGRADE_REQUIRED, b"HTTP/1.1 426 Upgrade Required\r\n", 0),
            (REQUEST_HEADER_FIELDS_TOO_LARGE, b"HTTP/1.1 431 Request Header Fields Too Large\r\n", 0),
            (INTERNAL_SERVER_ERROR, b"HTTP/1.1 500 Internal Server Error\r\n", 0x8E),
            (NOT_IMPLEMENTED, b"HTTP/1.1 501 Not Implemented\r\n", 0),
            (SERVICE_UNAVAILABLE, b"HTTP/1.1 503 Service Unavailable\r\n", 0),
        }
    };
}

use std::num::NonZeroU16;
use std::ops::{self, Range};
use std::str::Utf8Error;
use std::{fmt, str};

use bitflags::bitflags;
use bytes::Bytes;
pub(crate) use common_status_codes;
use http::Method;
use http::method::InvalidMethod;
use pyo3::pybacked::PyBackedBytes;
use smallvec::SmallVec;

use crate::http::header::{
    protocol_is_websocket, request_authority_is_valid, request_header_name_needs_lowercase,
    trailer_field_name_is_forbidden,
};
use crate::http::header_meta::RequestHeaderMeta;
use crate::http::header_value::header_value_is_valid;

const fn request_header_name_bytes_are_valid(name: &[u8]) -> bool {
    let mut index = 0;
    while index < name.len() {
        if crate::ascii::HEADER_NAME_FLAGS[name[index] as usize] != crate::ascii::HEADER_NAME_VALID
        {
            return false;
        }
        index += 1;
    }
    !name.is_empty()
}

macro_rules! known_request_header_names {
    ($($first:literal => { $(($variant:ident, $name:literal)),+ $(,)? }),+ $(,)?) => {
        const _: () = {
            $($(
                assert!(!$name.is_empty());
                assert!($name[0] == $first);
                assert!(request_header_name_bytes_are_valid($name));
            )+)+
        };

        #[repr(u8)]
        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        pub(crate) enum KnownRequestHeaderName {
            $($($variant),+),+
        }

        impl KnownRequestHeaderName {
            pub(crate) const COUNT: usize = [$( $(Self::$variant),+ ),+].len();
            pub(crate) const ALL: [Self; Self::COUNT] = [$( $(Self::$variant),+ ),+];

            pub(crate) const fn from_bytes(name: &[u8]) -> Option<Self> {
                match name {
                    $($($name => Some(Self::$variant),)+)+
                    _ => None,
                }
            }

            pub(crate) fn from_bytes_ignore_ascii_case(name: &[u8]) -> Option<Self> {
                match name.first().map(u8::to_ascii_lowercase) {
                    $(
                    Some($first) => {
                        $(
                        if name.eq_ignore_ascii_case($name) {
                            return Some(Self::$variant);
                        }
                        )+
                        None
                    }
                    )+
                    _ => None,
                }
            }

            pub(crate) const fn as_bytes(self) -> &'static [u8] {
                match self {
                    $($(Self::$variant => $name,)+)+
                }
            }

            pub(crate) const fn as_str(self) -> &'static str {
                // SAFETY: all names are ASCII byte literals.
                unsafe { str::from_utf8_unchecked(self.as_bytes()) }
            }
        }
    };
}

const H1_HEADER_INLINE_CAPACITY: usize = 16;

pub(crate) type ResponseField = (ResponseHeaderName, ResponseHeaderValue);
pub(crate) type ResponseHeaders = Vec<ResponseField>;

/// Fields emitted after a response body. Constructing this type validates the
/// trailer-only policy at the Python ingress, so neither HTTP writer has to
/// remember a late filtering rule.
#[derive(Debug, Default)]
pub(crate) struct ResponseTrailers(ResponseHeaders);

impl TryFrom<ResponseHeaders> for ResponseTrailers {
    type Error = crate::error::H2CornError;

    fn try_from(headers: ResponseHeaders) -> Result<Self, Self::Error> {
        if headers
            .iter()
            .any(|(name, _)| trailer_field_name_is_forbidden(name.as_bytes()))
        {
            return Err(crate::error::HttpResponseError::InvalidResponseTrailerField.into());
        }
        Ok(Self(headers))
    }
}

impl ResponseTrailers {
    pub(crate) const fn new() -> Self {
        Self(Vec::new())
    }

    pub(crate) fn append(&mut self, other: Self) {
        self.0.extend(other.0);
    }

    pub(crate) fn as_fields(&self) -> &[ResponseField] {
        &self.0
    }
}

/// An ASGI/HTTP response status: exactly one three-digit non-zero code.
///
/// Validation happens once when untrusted Python input crosses into Rust.
/// Every protocol encoder and access-log path can then trust the invariant,
/// while `NonZeroU16` preserves the two-byte `Option` niche.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub(crate) struct HttpStatusCode(NonZeroU16);

impl HttpStatusCode {
    pub(crate) const fn new(value: u16) -> Option<Self> {
        if value < 100 || value > 999 {
            return None;
        }
        match NonZeroU16::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    #[expect(
        clippy::panic,
        reason = "const-evaluated only: every caller passes a literal, so a bad status is a \
                  compile error rather than a runtime panic"
    )]
    const fn constant(value: u16) -> Self {
        match Self::new(value) {
            Some(value) => value,
            None => panic!("HTTP status constants must have exactly three digits"),
        }
    }

    pub(crate) const fn get(self) -> u16 {
        self.0.get()
    }

    pub(crate) const fn is_informational(self) -> bool {
        matches!(self.get(), 100..=199)
    }

    /// RFC 9110 §8.6 forbids this field for informational and 204 responses,
    /// and for those only.
    ///
    /// 205 is deliberately absent. It carries no content, but RFC 9112 §6.3
    /// makes only HEAD, 1xx, 204 and 304 bodyless *by status alone*; an
    /// unframed 205 is therefore delimited by connection close, and on a
    /// keep-alive connection the client reads the next response as its body.
    /// It is framed with `Content-Length: 0` instead.
    ///
    /// Other bodyless statuses deliberately retain their representation
    /// metadata: notably, a 304 may carry the length a 200 would have sent.
    pub(crate) const fn forbids_content_length(self) -> bool {
        matches!(self.get(), 100..=199 | 204)
    }
}

/// The `link` values of one `http.response.early_hint` message.
///
/// Carries no status and no `end_stream`: it can represent 103 and nothing
/// else, so an early hint cannot terminate a stream or be mistaken for a final
/// response anywhere it travels. Each value becomes one lowercase `link`
/// field, which also makes arbitrary response headers unrepresentable here.
#[derive(Debug, Default)]
pub(crate) struct EarlyHintLinks(Vec<ResponseHeaderValue>);

impl EarlyHintLinks {
    pub(crate) const fn new(links: Vec<ResponseHeaderValue>) -> Self {
        Self(links)
    }

    pub(crate) fn values(&self) -> &[ResponseHeaderValue] {
        &self.0
    }
}

/// A status that may terminate an ASGI response. Informational codes need a
/// separate HTTP event sequence which ASGI does not define, so response
/// actions can only be built after this conversion succeeds.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub(crate) struct FinalResponseStatus(HttpStatusCode);

impl FinalResponseStatus {
    pub(crate) const fn new(status: HttpStatusCode) -> Option<Self> {
        if status.is_informational() {
            None
        } else {
            Some(Self(status))
        }
    }

    pub(crate) const fn get(self) -> HttpStatusCode {
        self.0
    }
}

impl fmt::Display for HttpStatusCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Owned UTF-8 string backed by `Bytes`.
///
/// HTTP/application text (paths, authorities, schemes, WebSocket reasons), not
/// HPACK table state.
#[doc(hidden)]
#[derive(Clone, Eq, PartialEq, Hash, Default)]
pub(crate) struct BytesStr(Bytes);

impl BytesStr {
    pub(crate) const fn from_static_bytes(value: &'static [u8]) -> Self {
        Self(Bytes::from_static(value))
    }

    pub(crate) const fn from_static(value: &'static str) -> Self {
        Self::from_static_bytes(value.as_bytes())
    }

    /// # Safety
    ///
    /// `value` must contain only ASCII bytes.
    pub(crate) unsafe fn from_validated_ascii(value: Bytes) -> Self {
        debug_assert!(value.iter().all(u8::is_ascii));
        Self(value)
    }

    pub(crate) fn as_str(&self) -> &str {
        // SAFETY: `BytesStr` is only constructed through validated UTF-8
        // conversion paths or from string literals, so its backing bytes are
        // always valid UTF-8.
        unsafe { str::from_utf8_unchecked(self.0.as_ref()) }
    }

    pub(crate) fn into_inner(self) -> Bytes {
        self.0
    }
}

impl ops::Deref for BytesStr {
    type Target = str;

    fn deref(&self) -> &str {
        self.as_str()
    }
}

impl AsRef<[u8]> for BytesStr {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl AsRef<str> for BytesStr {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Debug for BytesStr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<&str> for BytesStr {
    fn from(value: &str) -> Self {
        Self(Bytes::copy_from_slice(value.as_bytes()))
    }
}

impl From<String> for BytesStr {
    fn from(value: String) -> Self {
        Self(Bytes::from(value))
    }
}

impl TryFrom<Bytes> for BytesStr {
    type Error = str::Utf8Error;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        str::from_utf8(bytes.as_ref())?;
        Ok(Self(bytes))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RequestAuthority(BytesStr);

impl RequestAuthority {
    pub(crate) const fn new(value: BytesStr) -> Self {
        Self(value)
    }

    pub(crate) fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        self.0.as_ref()
    }

    pub(crate) const fn as_bytes_str(&self) -> &BytesStr {
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

/// An authority suitable for a plain CONNECT tunnel.
///
/// RFC 9112/9113 require a port for tunnel targets. Keeping that distinction
/// in the target type means a generic CONNECT cannot accidentally reuse the
/// port-optional authority accepted by extended CONNECT.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ConnectAuthority(RequestAuthority);

impl TryFrom<BytesStr> for ConnectAuthority {
    type Error = BytesStr;

    fn try_from(value: BytesStr) -> Result<Self, Self::Error> {
        let bytes = value.as_bytes();
        let has_port = bytes.strip_prefix(b"[").map_or_else(
            || bytes.contains(&b':'),
            |rest| {
                rest.iter()
                    .position(|byte| *byte == b']')
                    .is_some_and(|close| matches!(rest.get(close + 1..), Some([b':', ..])))
            },
        );
        if request_authority_is_valid(value.as_ref()) && has_port {
            Ok(Self(RequestAuthority::new(value)))
        } else {
            Err(value)
        }
    }
}

impl ConnectAuthority {
    const fn as_request_authority(&self) -> &RequestAuthority {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RequestTarget {
    Normal {
        scheme: BytesStr,
        path_and_query: BytesStr,
    },
    Connect(Box<ConnectTarget>),
}

/// A CONNECT target: either a plain tunnel or RFC 8441 extended CONNECT.
///
/// Extended CONNECT always carries all three of protocol, scheme and path, and
/// a plain tunnel carries none of them — so they live in one variant each
/// rather than as three `Option`s that must agree.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ConnectTarget {
    Tunnel {
        authority: ConnectAuthority,
    },
    Extended {
        authority: RequestAuthority,
        protocol: Protocol,
        scheme: BytesStr,
        path_and_query: BytesStr,
    },
}

impl ConnectTarget {
    const fn authority(&self) -> &RequestAuthority {
        match self {
            Self::Tunnel { authority } => authority.as_request_authority(),
            Self::Extended { authority, .. } => authority,
        }
    }

    const fn scheme(&self) -> Option<&BytesStr> {
        match self {
            Self::Tunnel { .. } => None,
            Self::Extended { scheme, .. } => Some(scheme),
        }
    }

    const fn path_and_query(&self) -> Option<&BytesStr> {
        match self {
            Self::Tunnel { .. } => None,
            Self::Extended { path_and_query, .. } => Some(path_and_query),
        }
    }

    const fn protocol(&self) -> Option<&Protocol> {
        match self {
            Self::Tunnel { .. } => None,
            Self::Extended { protocol, .. } => Some(protocol),
        }
    }
}

impl RequestTarget {
    pub(crate) const fn normal(scheme: BytesStr, path_and_query: BytesStr) -> Self {
        Self::Normal {
            scheme,
            path_and_query,
        }
    }

    pub(crate) fn connect(authority: ConnectAuthority) -> Self {
        Self::Connect(Box::new(ConnectTarget::Tunnel { authority }))
    }

    pub(crate) fn extended_connect(
        authority: RequestAuthority,
        protocol: Protocol,
        scheme: BytesStr,
        path_and_query: BytesStr,
    ) -> Self {
        Self::Connect(Box::new(ConnectTarget::Extended {
            authority,
            protocol,
            scheme,
            path_and_query,
        }))
    }

    pub(crate) const fn authority(&self) -> Option<&RequestAuthority> {
        match self {
            Self::Normal { .. } => None,
            Self::Connect(target) => Some(target.authority()),
        }
    }

    pub(crate) const fn scheme(&self) -> Option<&BytesStr> {
        match self {
            Self::Normal { scheme, .. } => Some(scheme),
            Self::Connect(target) => target.scheme(),
        }
    }

    pub(crate) fn scheme_str(&self) -> &str {
        self.scheme().map_or("", BytesStr::as_str)
    }

    pub(crate) const fn path_and_query(&self) -> Option<&BytesStr> {
        match self {
            Self::Normal { path_and_query, .. } => Some(path_and_query),
            Self::Connect(target) => target.path_and_query(),
        }
    }

    pub(crate) const fn protocol(&self) -> Option<&Protocol> {
        match self {
            Self::Normal { .. } => None,
            Self::Connect(target) => target.protocol(),
        }
    }

    pub(crate) const fn is_connect(&self) -> bool {
        matches!(self, Self::Connect(_))
    }

    pub(crate) fn protocol_is_websocket(&self) -> bool {
        self.protocol().is_some_and(protocol_is_websocket)
    }

    pub(crate) fn log_target(&self) -> &BytesStr {
        match self {
            Self::Normal { path_and_query, .. } => path_and_query,
            Self::Connect(target) => target
                .path_and_query()
                .unwrap_or_else(|| target.authority().as_bytes_str()),
        }
    }
}

known_request_header_names! {
    b'a' => {
        (Accept, b"accept"),
        (AcceptEncoding, b"accept-encoding"),
        (AcceptLanguage, b"accept-language"),
        (Authorization, b"authorization"),
    },
    b'c' => {
        (CacheControl, b"cache-control"),
        (Connection, b"connection"),
        (ContentLength, b"content-length"),
        (ContentType, b"content-type"),
        (Cookie, b"cookie"),
    },
    b'e' => {
        (Expect, b"expect"),
    },
    b'f' => {
        (Forwarded, b"forwarded"),
    },
    b'h' => {
        (Host, b"host"),
        (Http2Settings, b"http2-settings"),
    },
    b'i' => {
        (IfModifiedSince, b"if-modified-since"),
        (IfNoneMatch, b"if-none-match"),
    },
    b'k' => {
        (KeepAlive, b"keep-alive"),
    },
    b'o' => {
        (Origin, b"origin"),
    },
    b'p' => {
        (Pragma, b"pragma"),
        (ProxyConnection, b"proxy-connection"),
    },
    b'r' => {
        (Referer, b"referer"),
    },
    b's' => {
        (SecWebSocketVersion, b"sec-websocket-version"),
        (SecWebSocketKey, b"sec-websocket-key"),
        (SecWebSocketProtocol, b"sec-websocket-protocol"),
        (SecWebSocketExtensions, b"sec-websocket-extensions"),
    },
    b't' => {
        (Te, b"te"),
        (TransferEncoding, b"transfer-encoding"),
    },
    b'u' => {
        (Upgrade, b"upgrade"),
        (UserAgent, b"user-agent"),
    },
    b'x' => {
        (XForwardedFor, b"x-forwarded-for"),
        (XForwardedProto, b"x-forwarded-proto"),
        (XForwardedHost, b"x-forwarded-host"),
        (XForwardedPort, b"x-forwarded-port"),
        (XForwardedPrefix, b"x-forwarded-prefix"),
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RequestHeaderName {
    Known(KnownRequestHeaderName),
    Other(BytesStr),
}

impl RequestHeaderName {
    pub(crate) fn as_str(&self) -> &str {
        match self {
            Self::Known(name) => name.as_str(),
            Self::Other(name) => name.as_str(),
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
    pub(crate) const fn from_h2_validated(value: Bytes) -> Self {
        Self(value)
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        self.0.as_ref()
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RequestHeaderNameRef<'a> {
    Known(KnownRequestHeaderName),
    Other(&'a str),
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct RequestHeaderRef<'a> {
    name: RequestHeaderNameRef<'a>,
    value: &'a [u8],
}

impl<'a> RequestHeaderRef<'a> {
    pub(crate) const fn name(self) -> RequestHeaderNameRef<'a> {
        self.name
    }

    pub(crate) const fn value(self) -> &'a [u8] {
        self.value
    }
}

#[derive(Clone, Debug)]
pub(crate) enum RequestHeaders {
    // On supported Rust targets this enum is the same 24 bytes as the HTTP/2
    // `Vec`: its unused vector-pointer niche stores the HTTP/1 variant tag.
    H1(Box<H1RequestHeaders>),
    H2(Vec<(RequestHeaderName, RequestHeaderValue)>),
}

#[derive(Clone, Debug)]
pub(crate) struct H1RequestHeaders {
    head: Bytes,
    auxiliary: Vec<u8>,
    fields: SmallVec<[H1RequestHeader; H1_HEADER_INLINE_CAPACITY]>,
}

#[derive(Clone, Copy, Debug)]
struct H1RequestHeader {
    name_start: u32,
    name_end: u32,
    value_start: u32,
    value_end: u32,
    known_name: Option<KnownRequestHeaderName>,
    sources: H1FieldSources,
}

bitflags! {
    /// Which halves of a field were rewritten into the auxiliary buffer rather
    /// than pointing into the original head.
    #[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
    pub(crate) struct H1FieldSources: u8 {
        const NAME_AUXILIARY = 1 << 0;
        const VALUE_AUXILIARY = 1 << 1;
    }
}

impl Default for RequestHeaders {
    fn default() -> Self {
        Self::H2(Vec::new())
    }
}

impl RequestHeaders {
    pub(crate) fn from_h1(headers: H1RequestHeaders) -> Self {
        Self::H1(Box::new(headers))
    }

    pub(crate) const fn from_h2(headers: Vec<(RequestHeaderName, RequestHeaderValue)>) -> Self {
        Self::H2(headers)
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            Self::H1(headers) => headers.len(),
            Self::H2(headers) => headers.len(),
        }
    }

    pub(crate) fn get(&self, index: usize) -> Option<RequestHeaderRef<'_>> {
        match self {
            Self::H1(headers) => headers.get(index),
            Self::H2(headers) => headers.get(index).map(|(name, value)| RequestHeaderRef {
                name: match name {
                    RequestHeaderName::Known(name) => RequestHeaderNameRef::Known(*name),
                    RequestHeaderName::Other(name) => RequestHeaderNameRef::Other(name.as_str()),
                },
                value: value.as_bytes(),
            }),
        }
    }

    pub(crate) fn iter(&self) -> RequestHeadersIter<'_> {
        RequestHeadersIter {
            headers: self,
            range: 0..self.len(),
        }
    }
}

impl H1RequestHeaders {
    pub(crate) fn new(head: Bytes) -> Self {
        Self {
            head,
            auxiliary: Vec::new(),
            fields: SmallVec::new(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.fields.len()
    }

    pub(crate) fn push(
        &mut self,
        name: &[u8],
        value: &[u8],
    ) -> Result<Option<KnownRequestHeaderName>, ()> {
        if !header_value_is_valid(value) {
            return Err(());
        }
        // Generated names are grammar-proven at compile time, so an exact
        // match needs neither the generic token scan nor normalisation.
        let known_name = KnownRequestHeaderName::from_bytes(name);
        let needs_lowercase = if known_name.is_some() {
            false
        } else {
            request_header_name_needs_lowercase(name).ok_or(())?
        };
        let known_name = known_name.or_else(|| {
            if needs_lowercase {
                KnownRequestHeaderName::from_bytes_ignore_ascii_case(name)
            } else {
                None
            }
        });
        let (name_start, name_end, name_auxiliary) = if known_name.is_some() {
            (0, 0, false)
        } else if needs_lowercase {
            let start = u32::try_from(self.auxiliary.len()).map_err(|_| ())?;
            self.auxiliary
                .extend(name.iter().map(u8::to_ascii_lowercase));
            let end = u32::try_from(self.auxiliary.len()).map_err(|_| ())?;
            (start, end, true)
        } else {
            let (start, end) = slice_range(&self.head, name).ok_or(())?;
            (start, end, false)
        };
        let (value_start, value_end) = slice_range(&self.head, value).ok_or(())?;
        self.fields.push(H1RequestHeader {
            name_start,
            name_end,
            value_start,
            value_end,
            known_name,
            sources: if name_auxiliary {
                H1FieldSources::NAME_AUXILIARY
            } else {
                H1FieldSources::empty()
            },
        });
        Ok(known_name)
    }

    pub(crate) fn push_synthetic(&mut self, name: KnownRequestHeaderName, value: &[u8]) -> bool {
        if !header_value_is_valid(value) {
            return false;
        }
        let Ok(value_start) = u32::try_from(self.auxiliary.len()) else {
            return false;
        };
        self.auxiliary.extend_from_slice(value);
        let Ok(value_end) = u32::try_from(self.auxiliary.len()) else {
            return false;
        };
        self.fields.push(H1RequestHeader {
            name_start: 0,
            name_end: 0,
            value_start,
            value_end,
            known_name: Some(name),
            sources: H1FieldSources::VALUE_AUXILIARY,
        });
        true
    }

    pub(crate) fn get(&self, index: usize) -> Option<RequestHeaderRef<'_>> {
        self.fields.get(index).map(|field| self.view(*field))
    }

    fn view(&self, field: H1RequestHeader) -> RequestHeaderRef<'_> {
        let name = field.known_name.map_or_else(
            || {
                let source = if field.sources.contains(H1FieldSources::NAME_AUXILIARY) {
                    self.auxiliary.as_slice()
                } else {
                    self.head.as_ref()
                };
                let bytes = &source[field.name_start as usize..field.name_end as usize];
                // SAFETY: HTTP header-name validation restricts names to ASCII,
                // and optional normalization only lowercases those ASCII bytes.
                RequestHeaderNameRef::Other(unsafe { str::from_utf8_unchecked(bytes) })
            },
            RequestHeaderNameRef::Known,
        );
        let value_source = if field.sources.contains(H1FieldSources::VALUE_AUXILIARY) {
            self.auxiliary.as_slice()
        } else {
            self.head.as_ref()
        };
        RequestHeaderRef {
            name,
            value: &value_source[field.value_start as usize..field.value_end as usize],
        }
    }
}

pub(crate) struct RequestHeadersIter<'a> {
    headers: &'a RequestHeaders,
    range: Range<usize>,
}

impl<'a> Iterator for RequestHeadersIter<'a> {
    type Item = RequestHeaderRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.range.next().and_then(|index| self.headers.get(index))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.range.size_hint()
    }
}

impl ExactSizeIterator for RequestHeadersIter<'_> {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ResponseHeaderKind {
    Connection,
    ContentLength,
    Date,
    Other,
    Server,
    TransferEncoding,
}

impl ResponseHeaderKind {
    pub(crate) const fn from_bytes(value: &[u8]) -> Self {
        match value {
            b"connection" => Self::Connection,
            b"content-length" => Self::ContentLength,
            b"date" => Self::Date,
            b"server" => Self::Server,
            b"transfer-encoding" => Self::TransferEncoding,
            _ => Self::Other,
        }
    }
}

macro_rules! response_header_bytes {
    ($($kind:ident),+ $(,)?) => {
        #[derive(Debug)]
        enum HeaderBytes {
            $(${concat(Rust, $kind)}(Bytes), ${concat(Python, $kind)}(PyBackedBytes),)+
        }

        impl HeaderBytes {
            pub(crate) fn as_slice(&self) -> &[u8] {
                match self {
                    $(
                        Self::${concat(Rust, $kind)}(bytes) => bytes.as_ref(),
                        Self::${concat(Python, $kind)}(bytes) => bytes.as_ref(),
                    )+
                }
            }

            const fn response_name_rust(bytes: Bytes, kind: ResponseHeaderKind) -> Self {
                match kind {
                    $(ResponseHeaderKind::$kind => Self::${concat(Rust, $kind)}(bytes),)+
                }
            }

            const fn response_name_python(bytes: PyBackedBytes, kind: ResponseHeaderKind) -> Self {
                match kind {
                    $(ResponseHeaderKind::$kind => Self::${concat(Python, $kind)}(bytes),)+
                }
            }

            const fn response_name_kind(&self) -> ResponseHeaderKind {
                match self {
                    $(
                        Self::${concat(Rust, $kind)}(_) | Self::${concat(Python, $kind)}(_) => {
                            ResponseHeaderKind::$kind
                        },
                    )+
                }
            }
        }
    };
}
response_header_bytes! {
    Other,
    Connection,
    ContentLength,
    Date,
    Server,
    TransferEncoding,
}

impl AsRef<[u8]> for HeaderBytes {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

#[derive(Debug)]
pub(crate) struct ResponseHeaderName(HeaderBytes);

impl ResponseHeaderName {
    pub(crate) fn from_python(value: PyBackedBytes) -> Option<Self> {
        let needs_lowercase = request_header_name_needs_lowercase(value.as_ref())?;
        if !needs_lowercase {
            let kind = ResponseHeaderKind::from_bytes(value.as_ref());
            return Some(Self(HeaderBytes::response_name_python(value, kind)));
        }
        let normalized = Bytes::from(
            value
                .as_ref()
                .iter()
                .map(u8::to_ascii_lowercase)
                .collect::<Vec<_>>(),
        );
        let kind = ResponseHeaderKind::from_bytes(normalized.as_ref());
        Some(Self(HeaderBytes::response_name_rust(normalized, kind)))
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        self.0.as_slice()
    }

    pub(crate) const fn kind(&self) -> ResponseHeaderKind {
        self.0.response_name_kind()
    }

    pub(crate) const fn from_configured(bytes: Bytes, kind: ResponseHeaderKind) -> Self {
        Self(HeaderBytes::response_name_rust(bytes, kind))
    }
}

impl AsRef<[u8]> for ResponseHeaderName {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl From<Bytes> for ResponseHeaderName {
    fn from(value: Bytes) -> Self {
        assert!(matches!(
            request_header_name_needs_lowercase(value.as_ref()),
            Some(false)
        ));
        let kind = ResponseHeaderKind::from_bytes(value.as_ref());
        Self(HeaderBytes::response_name_rust(value, kind))
    }
}

#[derive(Debug)]
pub(crate) enum ResponseHeaderValue {
    Rust(Bytes),
    Python(PyBackedBytes),
}

impl ResponseHeaderValue {
    pub(crate) fn from_python(value: PyBackedBytes) -> Option<Self> {
        (header_value_is_valid(value.as_ref())
            && !value
                .as_ref()
                .first()
                .is_some_and(|byte| matches!(*byte, b' ' | b'\t'))
            && !value
                .as_ref()
                .last()
                .is_some_and(|byte| matches!(*byte, b' ' | b'\t')))
        .then_some(Self::Python(value))
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Rust(bytes) => bytes.as_ref(),
            Self::Python(bytes) => bytes.as_ref(),
        }
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
        Self::Rust(value)
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

#[derive(Clone, Debug)]
pub(crate) struct RequestHead {
    pub http_version: HttpVersion,
    pub method: Method,
    pub target: RequestTarget,
    pub headers: RequestHeaders,
    pub header_meta: RequestHeaderMeta,
}

impl RequestHead {
    pub(crate) const fn path_and_query(&self) -> Option<&BytesStr> {
        self.target.path_and_query()
    }

    pub(crate) fn scheme_str(&self) -> &str {
        self.target.scheme_str()
    }

    pub(crate) const fn is_connect(&self) -> bool {
        self.target.is_connect()
    }

    pub(crate) fn protocol_is_websocket(&self) -> bool {
        self.target.protocol_is_websocket()
    }

    pub(crate) fn log_target(&self) -> &BytesStr {
        self.target.log_target()
    }

    pub(crate) const fn accepts_trailers(&self) -> bool {
        self.header_meta.accepts_trailers()
    }

    pub(crate) const fn content_length(&self) -> Option<u64> {
        self.header_meta.content_length()
    }
}

/// Represents the `:protocol` pseudo-header used by
/// the [Extended CONNECT Protocol].
///
/// [Extended CONNECT Protocol]: https://datatracker.ietf.org/doc/html/rfc8441#section-4
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Protocol(BytesStr);

impl Protocol {
    /// Returns a str representation of the header.
    pub(crate) fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl From<&str> for Protocol {
    fn from(value: &str) -> Self {
        Self(value.into())
    }
}

impl TryFrom<Bytes> for Protocol {
    type Error = Utf8Error;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        Ok(Self(BytesStr::try_from(bytes)?))
    }
}

impl AsRef<[u8]> for Protocol {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl AsRef<str> for Protocol {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

fn slice_range(owner: &Bytes, slice: &[u8]) -> Option<(u32, u32)> {
    let owner_start = owner.as_ptr() as usize;
    let slice_start = slice.as_ptr() as usize;
    let start = slice_start.checked_sub(owner_start)?;
    let end = start.checked_add(slice.len())?;
    if end > owner.len() {
        return None;
    }
    Some((u32::try_from(start).ok()?, u32::try_from(end).ok()?))
}

pub(crate) fn parse_request_method(value: &[u8]) -> Result<Method, InvalidMethod> {
    let known = match value.len() {
        3 => match u32::from_le_bytes([value[0], value[1], value[2], 0]) {
            0x00_54_45_47 => Some(Method::GET),
            0x00_54_55_50 => Some(Method::PUT),
            _ => None,
        },
        4 => match u32::from_le_bytes(value.try_into().expect("length is four")) {
            0x44_41_45_48 => Some(Method::HEAD),
            0x54_53_4F_50 => Some(Method::POST),
            _ => None,
        },
        5 => match (
            u32::from_le_bytes(value[..4].try_into().expect("prefix is four bytes")),
            value[4],
        ) {
            (0x43_54_41_50, b'H') => Some(Method::PATCH),
            (0x43_41_52_54, b'E') => Some(Method::TRACE),
            _ => None,
        },
        6 => match (
            u32::from_le_bytes(value[..4].try_into().expect("prefix is four bytes")),
            value[4],
            value[5],
        ) {
            (0x45_4C_45_44, b'T', b'E') => Some(Method::DELETE),
            _ => None,
        },
        7 => match (
            u32::from_le_bytes(value[..4].try_into().expect("prefix is four bytes")),
            u32::from_le_bytes([value[4], value[5], value[6], 0]),
        ) {
            (0x4E_4E_4F_43, 0x00_54_43_45) => Some(Method::CONNECT),
            (0x49_54_50_4F, 0x00_53_4E_4F) => Some(Method::OPTIONS),
            _ => None,
        },
        _ => None,
    };
    known.map_or_else(|| Method::from_bytes(value), Ok)
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use http::Method;
    use pyo3::prelude::Python;
    use pyo3::types::PyBytes;

    use super::{
        BytesStr, ConnectAuthority, H1_HEADER_INLINE_CAPACITY, H1RequestHeaders, HttpStatusCode,
        HttpVersion, KnownRequestHeaderName, Protocol, RequestAuthority, RequestHead,
        RequestHeaderNameRef, RequestHeaders, RequestTarget, ResponseTrailers,
        parse_request_method,
    };
    use crate::http::header_meta::RequestHeaderMeta;

    #[test]
    fn request_method_fast_path_matches_standard_method_parsing() {
        for bytes in [
            b"GET".as_slice(),
            b"POST",
            b"HEAD",
            b"PUT",
            b"DELETE",
            b"PATCH",
            b"OPTIONS",
            b"TRACE",
            b"CONNECT",
            b"PROPFIND",
        ] {
            assert_eq!(
                parse_request_method(bytes).unwrap(),
                Method::from_bytes(bytes).unwrap()
            );
        }
    }

    #[test]
    fn response_header_kind_is_classified_at_construction() {
        use bytes::Bytes;

        let classifications = [
            (
                b"connection".as_slice(),
                super::ResponseHeaderKind::Connection,
            ),
            (
                b"content-length".as_slice(),
                super::ResponseHeaderKind::ContentLength,
            ),
            (b"date".as_slice(), super::ResponseHeaderKind::Date),
            (b"server".as_slice(), super::ResponseHeaderKind::Server),
            (
                b"transfer-encoding".as_slice(),
                super::ResponseHeaderKind::TransferEncoding,
            ),
            (b"content-type".as_slice(), super::ResponseHeaderKind::Other),
        ];
        for &(name, expected) in &classifications {
            let name: super::ResponseHeaderName = Bytes::copy_from_slice(name).into();
            assert_eq!(name.kind(), expected);
        }

        Python::initialize();
        Python::attach(|py| {
            for &(name, expected) in &classifications {
                let name = super::ResponseHeaderName::from_python(
                    pyo3::pybacked::PyBackedBytes::from(PyBytes::new(py, name)),
                )
                .expect("test header is valid");
                assert_eq!(name.kind(), expected);
            }
        });
    }
    #[test]
    fn http_status_code_encodes_the_three_digit_invariant_in_two_bytes() {
        assert_eq!(HttpStatusCode::new(99), None);
        assert_eq!(HttpStatusCode::new(100).unwrap().get(), 100);
        assert_eq!(HttpStatusCode::new(999).unwrap().get(), 999);
        assert_eq!(HttpStatusCode::new(1000), None);
    }

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
        let authority = ConnectAuthority::try_from(BytesStr::from_static("example.com:443"))
            .expect("test tunnel authority has a port");
        let target = RequestTarget::connect(authority);

        assert_eq!(target.scheme_str(), "");
        assert_eq!(target.path_and_query().map(BytesStr::as_str), None);
        assert_eq!(target.protocol(), None);
        assert!(target.is_connect());
        assert_eq!(target.log_target().as_str(), "example.com:443");
    }

    #[test]
    fn connect_authority_requires_a_port_after_generic_validation() {
        let _ = ConnectAuthority::try_from(BytesStr::from_static("example.com")).unwrap_err();
        let _ = ConnectAuthority::try_from(BytesStr::from_static("[::1]")).unwrap_err();
        let _ = ConnectAuthority::try_from(BytesStr::from_static("example.com:443")).unwrap();
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
            headers: RequestHeaders::default(),
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
    fn h1_header_arena_preserves_known_unknown_duplicate_and_value_bytes() {
        let head = Bytes::from_static(
            b"Host: example.com\r\nX-Demo: first\r\nx-demo: second\r\nUser-Agent: benchmark\r\n",
        );
        let mut headers = H1RequestHeaders::new(head.clone());
        let mut lines = head.as_ref().split(|byte| *byte == b'\n');
        for line in lines.by_ref().take(4) {
            let line = line.strip_suffix(b"\r").unwrap_or(line);
            let colon = line.iter().position(|byte| *byte == b':').unwrap();
            headers
                .push(&line[..colon], line[colon + 1..].trim_ascii())
                .unwrap();
        }
        let headers = RequestHeaders::from_h1(headers);

        assert_eq!(headers.len(), 4);
        assert_eq!(
            headers.get(0).unwrap().name(),
            RequestHeaderNameRef::Known(KnownRequestHeaderName::Host)
        );
        assert_eq!(
            headers.get(1).unwrap().name(),
            RequestHeaderNameRef::Other("x-demo")
        );
        assert_eq!(
            headers.get(2).unwrap().name(),
            RequestHeaderNameRef::Other("x-demo")
        );
        assert_eq!(headers.get(1).unwrap().value(), b"first");
        assert_eq!(headers.get(2).unwrap().value(), b"second");
        assert_eq!(
            headers.get(3).unwrap().name(),
            RequestHeaderNameRef::Known(KnownRequestHeaderName::UserAgent)
        );
        assert_eq!(headers.iter().len(), 4);
    }

    #[test]
    fn h1_known_names_skip_generic_validation_without_relaxing_unknown_names() {
        let head = Bytes::from_static(b"HOST: example.com\r\nX-Demo: value\r\n");
        let mut headers = H1RequestHeaders::new(head.clone());
        headers.push(&head[..4], &head[6..17]).unwrap();
        headers.push(&head[19..25], &head[27..32]).unwrap();

        let headers = RequestHeaders::from_h1(headers);
        assert_eq!(
            headers.get(0).unwrap().name(),
            RequestHeaderNameRef::Known(KnownRequestHeaderName::Host)
        );
        assert_eq!(
            headers.get(1).unwrap().name(),
            RequestHeaderNameRef::Other("x-demo")
        );

        let malformed_head = Bytes::from_static(b"ho st: value");
        let mut malformed = H1RequestHeaders::new(malformed_head.clone());
        malformed
            .push(&malformed_head[..5], &malformed_head[7..])
            .unwrap_err();
    }

    #[test]
    fn h1_synthetic_header_uses_auxiliary_arena_without_changing_order() {
        let head = Bytes::from_static(b"x: one");
        let mut headers = H1RequestHeaders::new(head.clone());
        headers.push(&head[..1], &head[3..]).unwrap();
        assert!(headers.push_synthetic(KnownRequestHeaderName::Host, b"example.com"));
        let headers = RequestHeaders::from_h1(headers);

        assert_eq!(headers.get(0).unwrap().value(), b"one");
        assert_eq!(headers.get(1).unwrap().value(), b"example.com");
        assert_eq!(
            headers.get(1).unwrap().name(),
            RequestHeaderNameRef::Known(KnownRequestHeaderName::Host)
        );
    }

    #[test]
    fn h1_arena_spills_only_above_its_inline_capacity() {
        for count in [0, 4, 8, 12, 16, 17, 32] {
            let mut raw = Vec::with_capacity(count * 12);
            for index in 0..count {
                raw.extend_from_slice(format!("x-{index}: value\r\n").as_bytes());
            }
            let head = Bytes::from(raw);
            let mut headers = H1RequestHeaders::new(head.clone());
            for line in head.as_ref().split(|byte| *byte == b'\n').take(count) {
                let line = line.strip_suffix(b"\r").unwrap_or(line);
                let colon = line.iter().position(|byte| *byte == b':').unwrap();
                headers
                    .push(&line[..colon], line[colon + 1..].trim_ascii())
                    .unwrap();
            }
            assert_eq!(headers.fields.len(), count);
            assert_eq!(headers.fields.spilled(), count > H1_HEADER_INLINE_CAPACITY);
            assert!(headers.auxiliary.is_empty());
        }
    }

    #[test]
    fn h1_header_arena_keeps_sixteen_fields_inline() {
        let count = 16;
        let mut raw = Vec::with_capacity(count * 12);
        for index in 0..count {
            raw.extend_from_slice(format!("x-{index}: value\r\n").as_bytes());
        }
        let head = Bytes::from(raw);
        let mut headers = H1RequestHeaders::new(head.clone());
        for line in head.as_ref().split(|byte| *byte == b'\n').take(count) {
            let line = line.strip_suffix(b"\r").unwrap_or(line);
            let colon = line.iter().position(|byte| *byte == b':').unwrap();
            headers
                .push(&line[..colon], line[colon + 1..].trim_ascii())
                .unwrap();
        }
        assert!(!headers.fields.spilled());
    }

    #[test]
    fn response_trailers_allow_extensions_and_reject_framing_fields() {
        let extension = ResponseTrailers::try_from(vec![(
            Bytes::from_static(b"x-checksum").into(),
            Bytes::from_static(b"abc").into(),
        )])
        .expect("extension trailers are permitted");
        assert_eq!(extension.as_fields().len(), 1);

        ResponseTrailers::try_from(vec![(
            Bytes::from_static(b"content-length").into(),
            Bytes::from_static(b"1").into(),
        )])
        .unwrap_err();
    }
}
