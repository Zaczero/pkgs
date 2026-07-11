pub(crate) mod status_code {
    use super::HttpStatusCode;

    pub(crate) const SWITCHING_PROTOCOLS: HttpStatusCode = HttpStatusCode::constant(101);
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
    pub(crate) const HTTP_VERSION_NOT_SUPPORTED: HttpStatusCode = HttpStatusCode::constant(505);
}

use std::mem::size_of;
use std::num::NonZeroU16;
use std::ops::Range;
use std::str::Utf8Error;
use std::{fmt, str};

use bytes::Bytes;
use http::Method;
use http::method::InvalidMethod;
use pyo3::pybacked::PyBackedBytes;
use smallvec::SmallVec;

use crate::hpack::BytesStr;
use crate::http::header::{
    lowercase_header_name_is_valid, protocol_is_websocket, request_header_name_needs_lowercase,
};
use crate::http::header_meta::RequestHeaderMeta;
use crate::http::header_value::header_value_is_valid;

macro_rules! known_request_header_names {
    ($($first:literal => { $(($variant:ident, $name:literal)),+ $(,)? }),+ $(,)?) => {
        const _: () = {
            $($(
                assert!(!$name.is_empty());
                assert!($name[0] == $first);
            )+)+
        };

        #[repr(u8)]
        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        pub(crate) enum KnownRequestHeaderName {
            $($($variant),+),+
        }

        impl KnownRequestHeaderName {
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

pub(crate) type ResponseHeaders = Vec<(ResponseHeaderName, ResponseHeaderValue)>;

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

    const fn constant(value: u16) -> Self {
        match Self::new(value) {
            Some(value) => value,
            None => panic!("HTTP status constants must have exactly three digits"),
        }
    }

    pub(crate) const fn get(self) -> u16 {
        self.0.get()
    }
}

impl fmt::Display for HttpStatusCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

const _: () = assert!(size_of::<HttpStatusCode>() == size_of::<u16>());
const _: () = assert!(size_of::<Option<HttpStatusCode>>() == size_of::<u16>());

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

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RequestTarget {
    Normal {
        scheme: BytesStr,
        path_and_query: BytesStr,
    },
    Connect(Box<ConnectTarget>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ConnectTarget {
    authority: RequestAuthority,
    protocol: Option<Protocol>,
    scheme: Option<BytesStr>,
    path_and_query: Option<BytesStr>,
}

impl RequestTarget {
    pub(crate) const fn normal(scheme: BytesStr, path_and_query: BytesStr) -> Self {
        Self::Normal {
            scheme,
            path_and_query,
        }
    }

    pub(crate) fn connect(authority: RequestAuthority) -> Self {
        Self::Connect(Box::new(ConnectTarget {
            authority,
            protocol: None,
            scheme: None,
            path_and_query: None,
        }))
    }

    pub(crate) fn extended_connect(
        authority: RequestAuthority,
        protocol: Protocol,
        scheme: BytesStr,
        path_and_query: BytesStr,
    ) -> Self {
        Self::Connect(Box::new(ConnectTarget {
            authority,
            protocol: Some(protocol),
            scheme: Some(scheme),
            path_and_query: Some(path_and_query),
        }))
    }

    pub(crate) const fn authority(&self) -> Option<&RequestAuthority> {
        match self {
            Self::Normal { .. } => None,
            Self::Connect(target) => Some(&target.authority),
        }
    }

    pub(crate) const fn scheme(&self) -> Option<&BytesStr> {
        match self {
            Self::Normal { scheme, .. } => Some(scheme),
            Self::Connect(target) => target.scheme.as_ref(),
        }
    }

    pub(crate) fn scheme_str(&self) -> &str {
        self.scheme().map_or("", BytesStr::as_str)
    }

    pub(crate) const fn path_and_query(&self) -> Option<&BytesStr> {
        match self {
            Self::Normal { path_and_query, .. } => Some(path_and_query),
            Self::Connect(target) => target.path_and_query.as_ref(),
        }
    }

    pub(crate) const fn protocol(&self) -> Option<&Protocol> {
        match self {
            Self::Normal { .. } => None,
            Self::Connect(target) => target.protocol.as_ref(),
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
                .path_and_query
                .as_ref()
                .unwrap_or_else(|| target.authority.as_bytes_str()),
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
    fields: SmallVec<[H1RequestHeader; 16]>,
}

#[derive(Clone, Copy, Debug)]
struct H1RequestHeader {
    name_start: u32,
    name_end: u32,
    value_start: u32,
    value_end: u32,
    known_name: Option<KnownRequestHeaderName>,
    sources: u8,
}

impl H1RequestHeader {
    const NAME_AUXILIARY: u8 = 1 << 0;
    const VALUE_AUXILIARY: u8 = 1 << 1;
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
        let needs_lowercase = request_header_name_needs_lowercase(name).ok_or(())?;
        if !header_value_is_valid(value) {
            return Err(());
        }
        let known_name = if needs_lowercase {
            KnownRequestHeaderName::from_bytes_ignore_ascii_case(name)
        } else {
            KnownRequestHeaderName::from_bytes(name)
        };
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
            sources: u8::from(name_auxiliary) * H1RequestHeader::NAME_AUXILIARY,
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
            sources: H1RequestHeader::VALUE_AUXILIARY,
        });
        true
    }

    pub(crate) fn get(&self, index: usize) -> Option<RequestHeaderRef<'_>> {
        self.fields.get(index).map(|field| self.view(*field))
    }

    fn view(&self, field: H1RequestHeader) -> RequestHeaderRef<'_> {
        let name = field.known_name.map_or_else(
            || {
                let source = if field.sources & H1RequestHeader::NAME_AUXILIARY != 0 {
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
        let value_source = if field.sources & H1RequestHeader::VALUE_AUXILIARY != 0 {
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

#[derive(Debug)]
enum HeaderBytes {
    Rust(Bytes),
    Python(PyBackedBytes),
    RustConnection(Bytes),
    PythonConnection(PyBackedBytes),
    RustContentLength(Bytes),
    PythonContentLength(PyBackedBytes),
    RustDate(Bytes),
    PythonDate(PyBackedBytes),
    RustServer(Bytes),
    PythonServer(PyBackedBytes),
    RustTransferEncoding(Bytes),
    PythonTransferEncoding(PyBackedBytes),
}

impl HeaderBytes {
    pub(crate) fn as_slice(&self) -> &[u8] {
        match self {
            Self::Rust(bytes)
            | Self::RustConnection(bytes)
            | Self::RustContentLength(bytes)
            | Self::RustDate(bytes)
            | Self::RustServer(bytes)
            | Self::RustTransferEncoding(bytes) => bytes.as_ref(),
            Self::Python(bytes)
            | Self::PythonConnection(bytes)
            | Self::PythonContentLength(bytes)
            | Self::PythonDate(bytes)
            | Self::PythonServer(bytes)
            | Self::PythonTransferEncoding(bytes) => bytes.as_ref(),
        }
    }

    const fn response_name_rust(bytes: Bytes, kind: ResponseHeaderKind) -> Self {
        match kind {
            ResponseHeaderKind::Connection => Self::RustConnection(bytes),
            ResponseHeaderKind::ContentLength => Self::RustContentLength(bytes),
            ResponseHeaderKind::Date => Self::RustDate(bytes),
            ResponseHeaderKind::Other => Self::Rust(bytes),
            ResponseHeaderKind::Server => Self::RustServer(bytes),
            ResponseHeaderKind::TransferEncoding => Self::RustTransferEncoding(bytes),
        }
    }

    const fn response_name_python(bytes: PyBackedBytes, kind: ResponseHeaderKind) -> Self {
        match kind {
            ResponseHeaderKind::Connection => Self::PythonConnection(bytes),
            ResponseHeaderKind::ContentLength => Self::PythonContentLength(bytes),
            ResponseHeaderKind::Date => Self::PythonDate(bytes),
            ResponseHeaderKind::Other => Self::Python(bytes),
            ResponseHeaderKind::Server => Self::PythonServer(bytes),
            ResponseHeaderKind::TransferEncoding => Self::PythonTransferEncoding(bytes),
        }
    }

    const fn response_name_kind(&self) -> ResponseHeaderKind {
        match self {
            Self::RustConnection(_) | Self::PythonConnection(_) => ResponseHeaderKind::Connection,
            Self::RustContentLength(_) | Self::PythonContentLength(_) => {
                ResponseHeaderKind::ContentLength
            },
            Self::RustDate(_) | Self::PythonDate(_) => ResponseHeaderKind::Date,
            Self::RustServer(_) | Self::PythonServer(_) => ResponseHeaderKind::Server,
            Self::RustTransferEncoding(_) | Self::PythonTransferEncoding(_) => {
                ResponseHeaderKind::TransferEncoding
            },
            Self::Rust(_) | Self::Python(_) => ResponseHeaderKind::Other,
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

#[derive(Debug)]
pub(crate) struct ResponseHeaderName(HeaderBytes);

impl ResponseHeaderName {
    pub(crate) fn from_python(value: PyBackedBytes) -> Option<Self> {
        if !lowercase_header_name_is_valid(value.as_ref()) {
            return None;
        }
        let kind = ResponseHeaderKind::from_bytes(value.as_ref());
        Some(Self(HeaderBytes::response_name_python(value, kind)))
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
        assert!(lowercase_header_name_is_valid(value.as_ref()));
        let kind = ResponseHeaderKind::from_bytes(value.as_ref());
        Self(HeaderBytes::response_name_rust(value, kind))
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

#[derive(Clone, Debug)]
pub(crate) struct RequestHead {
    pub http_version: HttpVersion,
    pub method: Method,
    pub target: RequestTarget,
    pub headers: RequestHeaders,
    pub header_meta: RequestHeaderMeta,
}

impl RequestHead {
    pub(crate) const fn scheme(&self) -> Option<&BytesStr> {
        self.target.scheme()
    }

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

    use super::{
        H1RequestHeaders, HttpStatusCode, HttpVersion, KnownRequestHeaderName, Protocol,
        RequestAuthority, RequestHead, RequestHeaderNameRef, RequestHeaders, RequestTarget,
        parse_request_method,
    };

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

        for (name, expected) in [
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
        ] {
            let name: super::ResponseHeaderName = Bytes::copy_from_slice(name).into();
            assert_eq!(name.kind(), expected);
        }
    }
    use crate::hpack::BytesStr;
    use crate::http::header_meta::RequestHeaderMeta;

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
    fn h1_arena_keeps_zero_four_and_twelve_fields_inline_and_bounds_thirty_two() {
        for count in [0, 4, 12, 32] {
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
            assert_eq!(headers.fields.spilled(), count > 16);
            assert!(headers.auxiliary.is_empty());
        }
    }
}
