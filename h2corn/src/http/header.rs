use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
#[cfg(not(target_os = "linux"))]
use std::time::{SystemTime, UNIX_EPOCH};
use std::{iter, str};

use atoi_simd::parse_pos;
use bitflags::bitflags;
use bytes::Bytes;
use itoa::Buffer as ItoaBuffer;
use memchr::{memchr, memchr3};
#[cfg(target_os = "linux")]
use rustix::time::{ClockId, clock_gettime};

use crate::ascii;
use crate::config::{ResponseHeaderConfig, ServerConfig};
use crate::http::digits;
use crate::http::types::{Protocol, ResponseHeaderKind, ResponseHeaderName, ResponseHeaders};
use crate::proxy_protocol::trusted_host_matches;

const SECONDS_PER_DAY: u64 = 86_400;
const HTTP_DATE_TEMPLATE: [u8; 29] = *b"Thu, 01 Jan 1970 00:00:00 GMT";
const WEEKDAYS: [[u8; 3]; 7] = [
    *b"Sun", *b"Mon", *b"Tue", *b"Wed", *b"Thu", *b"Fri", *b"Sat",
];
const MONTHS: [[u8; 3]; 12] = [
    *b"Jan", *b"Feb", *b"Mar", *b"Apr", *b"May", *b"Jun", *b"Jul", *b"Aug", *b"Sep", *b"Oct",
    *b"Nov", *b"Dec",
];

/// `server`, `date`, and `content-length` are the only built-in response
/// fields that default preparation can append. The exact Python-list ingress
/// reserves these slots before it has selected a response framing path.
pub(crate) const RESPONSE_DEFAULT_BUILTIN_SLOTS: usize = 3;

/// Application response fields whose transport semantics h2corn owns.
/// Names reach this classifier only after grammar validation and ASCII
/// normalisation, making this the one owner for both configuration and ASGI
/// response-header policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ApplicationResponseField {
    Connection,
    KeepAlive,
    ProxyConnection,
    Te,
    TransferEncoding,
    Upgrade,
    Other,
}

/// Response-start fact retained after transport-owned fields are removed.
/// `Connection` never travels as an application header: HTTP/1 consumes this
/// to choose connection lifetime/Upgrade syntax and HTTP/2 ignores it.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum ResponseConnectionDirective {
    #[default]
    None,
    Close,
    Upgrade,
}

/// What an HTTP/1.1 `Transfer-Encoding` list earns, which is not one answer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransferCoding {
    /// `chunked` alone -- the only transfer coding h2corn implements.
    Chunked,
    /// Chunked is the final coding, so the message *is* framed (RFC 9112
    /// 6.1), but a coding was applied that h2corn does not implement. RFC 9112
    /// 6.1 asks for 501 here; 400 would tell the client its message was
    /// malformed when the framing was in fact correct and only the coding was
    /// unsupported.
    Unsupported,
    /// Chunked is absent, not final, or applied more than once. The message is
    /// not reliably framed, and RFC 9112 6.3 rule 4 requires 400 plus a close.
    Malformed,
}

bitflags! {
    /// The `Connection` tokens h2corn acts on.
    ///
    /// A request may repeat the header, so these accumulate across occurrences:
    /// `Connection: close` followed by `Connection: upgrade` means both.
    #[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
    pub(crate) struct ConnectionHeaderTokens: u8 {
        const CLOSE = 1 << 0;
        const UPGRADE = 1 << 1;
        const HTTP2_SETTINGS = 1 << 2;
    }
}

const _: () = assert!(size_of::<ConnectionHeaderTokens>() == 1);

impl ConnectionHeaderTokens {
    pub(crate) const fn close(self) -> bool {
        self.contains(Self::CLOSE)
    }

    pub(crate) const fn upgrade(self) -> bool {
        self.contains(Self::UPGRADE)
    }

    pub(crate) const fn http2_settings(self) -> bool {
        self.contains(Self::HTTP2_SETTINGS)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ResponseContentLength {
    Missing,
    Valid(usize),
    NeedsRewrite,
}

bitflags! {
    #[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
    struct ResponseScanFlags: u8 {
        const HAS_SERVER = 1 << 0;
        const HAS_DATE = 1 << 1;
    }

    #[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
    struct ForwardedParameterNames: u8 {
        const FOR = 1 << 0;
        const BY = 1 << 1;
        const HOST = 1 << 2;
        const PROTO = 1 << 3;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ResponseHeaderScan {
    content_length: ResponseContentLength,
    flags: ResponseScanFlags,
}

#[derive(Clone, Debug)]
pub(crate) struct ForwardedView<'a> {
    pub(crate) client_host: Option<Cow<'a, str>>,
    pub(crate) proto: Option<Cow<'a, str>>,
    pub(crate) host: Option<(Cow<'a, str>, Option<u16>)>,
}

impl ResponseHeaderScan {
    const fn new() -> Self {
        Self {
            content_length: ResponseContentLength::Missing,
            flags: ResponseScanFlags::empty(),
        }
    }

    const fn has_server(self) -> bool {
        self.flags.contains(ResponseScanFlags::HAS_SERVER)
    }

    const fn has_date(self) -> bool {
        self.flags.contains(ResponseScanFlags::HAS_DATE)
    }

    const fn content_length_state(self) -> ResponseContentLength {
        self.content_length
    }

    /// A single syntactically valid declared length. Missing, malformed, and
    /// conflicting lengths deliberately collapse to `None`.
    pub(crate) const fn content_length(self) -> Option<usize> {
        match self.content_length_state() {
            ResponseContentLength::Valid(len) => Some(len),
            ResponseContentLength::Missing | ResponseContentLength::NeedsRewrite => None,
        }
    }

    fn observe_content_length(&mut self, value: &[u8]) {
        self.content_length = match (
            self.content_length,
            parse_content_length_header(value).and_then(|len| usize::try_from(len).ok()),
        ) {
            (ResponseContentLength::Missing, Some(len)) => ResponseContentLength::Valid(len),
            _ => ResponseContentLength::NeedsRewrite,
        };
    }

    const fn set_content_length(&mut self, len: usize) {
        self.content_length = ResponseContentLength::Valid(len);
    }

    const fn clear_content_length(&mut self) {
        self.content_length = ResponseContentLength::Missing;
    }
}

pub(crate) const fn application_response_field(name: &[u8]) -> ApplicationResponseField {
    match name {
        b"connection" => ApplicationResponseField::Connection,
        b"keep-alive" => ApplicationResponseField::KeepAlive,
        b"proxy-connection" => ApplicationResponseField::ProxyConnection,
        b"te" => ApplicationResponseField::Te,
        b"transfer-encoding" => ApplicationResponseField::TransferEncoding,
        b"upgrade" => ApplicationResponseField::Upgrade,
        _ => ApplicationResponseField::Other,
    }
}

/// Byte offsets of the top-level commas in a header value, skipping any that
/// sit inside a quoted string.
fn csv_delimiters(value: &str) -> impl Iterator<Item = usize> + '_ {
    let bytes = value.as_bytes();
    let mut in_quotes = false;
    let mut escaped = false;
    let mut index = 0;

    iter::from_fn(move || {
        while index < bytes.len() {
            if escaped {
                escaped = false;
                index += 1;
                continue;
            }
            let offset = memchr3(b'\\', b'"', b',', &bytes[index..])?;
            let current = index + offset;
            index = current + 1;
            match bytes[current] {
                b'\\' if in_quotes => escaped = true,
                b'"' => in_quotes = !in_quotes,
                b',' if !in_quotes => return Some(current),
                _ => {},
            }
        }
        None
    })
}

pub(crate) fn last_csv_token(value: &str) -> &str {
    // The delimiter is ASCII, so `index + 1` is always a char boundary and
    // `get` always returns `Some`; taking the fallible form keeps the panic out
    // of the type rather than arguing it away in a comment.
    csv_delimiters(value)
        .last()
        .and_then(|index| value.get(index + 1..))
        .unwrap_or(value)
        .trim_ascii()
}

/// Every comma-separated element of a header value, in wire order.
///
/// `Forwarded` is the one header that needs the whole list rather than just its
/// last element: its `for=` names a client and has to be walked back through
/// trusted hops the way `X-Forwarded-For` is.
pub(crate) fn split_commas_bytes(value: &[u8]) -> impl Iterator<Item = &[u8]> {
    let mut rest = Some(value);
    iter::from_fn(move || {
        let current = rest?;
        let comma = if current.len() <= 16 {
            current.iter().position(|&byte| byte == b',')
        } else {
            memchr(b',', current)
        };
        if let Some(index) = comma {
            rest = Some(&current[index + 1..]);
            Some(&current[..index])
        } else {
            rest = None;
            Some(current)
        }
    })
}

pub(crate) fn header_contains_token(value: &[u8], token: &[u8]) -> bool {
    split_commas_bytes(value).any(|current| current.trim_ascii().eq_ignore_ascii_case(token))
}

pub(crate) fn parse_connection_header_tokens(value: &[u8]) -> ConnectionHeaderTokens {
    let mut tokens = ConnectionHeaderTokens::default();

    for current in split_commas_bytes(value).map(<[u8]>::trim_ascii) {
        if current.eq_ignore_ascii_case(b"close") {
            tokens |= ConnectionHeaderTokens::CLOSE;
        }
        if current.eq_ignore_ascii_case(b"upgrade") {
            tokens |= ConnectionHeaderTokens::UPGRADE;
        }
        if current.eq_ignore_ascii_case(b"http2-settings") {
            tokens |= ConnectionHeaderTokens::HTTP2_SETTINGS;
        }
        if tokens.is_all() {
            return tokens;
        }
    }

    tokens
}

pub(crate) fn header_is_single_token(value: &[u8], token: &[u8]) -> bool {
    let mut found = None;
    for current in split_commas_bytes(value) {
        let current = current.trim_ascii();
        if current.is_empty() {
            continue;
        }
        if found.is_some() {
            return false;
        }
        found = Some(current);
    }
    found.is_some_and(|current| current.eq_ignore_ascii_case(token))
}

pub(crate) fn parse_content_length_header(value: &[u8]) -> Option<u64> {
    let mut parsed = None;
    for member in split_commas_bytes(value) {
        let member = member.trim_ascii();
        if member.is_empty() {
            return None;
        }
        let value = parse_pos::<u64, true>(member).ok()?;
        match parsed {
            Some(previous) if previous != value => return None,
            Some(_) => {},
            None => parsed = Some(value),
        }
    }
    parsed
}

/// Validate the grammar shared by HTTP/1 absolute-form and HTTP/2 `:scheme`.
///
/// Scheme values are protocol syntax, not arbitrary UTF-8. Keeping this at
/// ingress gives both protocols the same answer before a scope exists.
pub(crate) fn request_scheme_is_valid(value: &[u8]) -> bool {
    let Some((&first, rest)) = value.split_first() else {
        return false;
    };
    first.is_ascii_alphabetic()
        && rest
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(*byte, b'+' | b'-' | b'.'))
}

/// Check authority syntax without imposing a server-side port range.
///
/// The port is a decimal token in HTTP syntax; rejecting `99999` here merely
/// turns an application routing decision into a parser limitation. IPv6 must
/// be bracketed when it shares an authority with an optional port.
pub(crate) fn request_authority_is_valid(value: &[u8]) -> bool {
    if value.is_empty()
        || value.iter().any(|byte| {
            !byte.is_ascii()
                || byte.is_ascii_control()
                || matches!(*byte, b' ' | b'@' | b',' | b'/' | b'?' | b'#')
        })
        || !percent_escapes_are_valid(value)
    {
        return false;
    }

    if let Some(rest) = value.strip_prefix(b"[") {
        let Some(close) = rest.iter().position(|byte| *byte == b']') else {
            return false;
        };
        let host = &rest[..close];
        let suffix = &rest[close + 1..];
        if host.is_empty() || host.contains(&b'[') || host.contains(&b']') {
            return false;
        }
        return match suffix {
            [] => true,
            [b':', port @ ..] => !port.is_empty() && port.iter().all(u8::is_ascii_digit),
            _ => false,
        };
    }

    if value.contains(&b'[') || value.contains(&b']') {
        return false;
    }
    match memchr(b':', value) {
        None => true,
        Some(colon) if memchr(b':', &value[colon + 1..]).is_none() => {
            let (host, port) = (&value[..colon], &value[colon + 1..]);
            !host.is_empty() && !port.is_empty() && port.iter().all(u8::is_ascii_digit)
        },
        Some(_) => false,
    }
}

/// Validate an origin-form path or the method-specific asterisk form.
pub(crate) fn request_path_is_valid(method: &http::Method, value: &[u8]) -> bool {
    if value == b"*" {
        return *method == http::Method::OPTIONS;
    }
    value.starts_with(b"/")
        && value.iter().all(|byte| {
            byte.is_ascii() && !byte.is_ascii_control() && !matches!(*byte, b' ' | b'#' | 0x7F)
        })
        && percent_escapes_are_valid(value)
}

/// An HTTP token is the grammar used by `:protocol` and WebSocket subprotocols.
pub(crate) fn protocol_token_is_valid(value: &[u8]) -> bool {
    request_header_name_needs_lowercase(value).is_some()
}

pub(crate) fn classify_transfer_coding(value: &[u8]) -> TransferCoding {
    let mut chunked = 0_usize;
    let mut others = 0_usize;
    let mut final_is_chunked = false;
    for coding in split_commas_bytes(value).map(<[u8]>::trim_ascii) {
        // RFC 9110 5.6.1.2 lets a list carry empty elements; they mean nothing.
        if coding.is_empty() {
            continue;
        }
        if !protocol_token_is_valid(coding) {
            return TransferCoding::Malformed;
        }
        final_is_chunked = coding.eq_ignore_ascii_case(b"chunked");
        if final_is_chunked {
            chunked += 1;
        } else {
            others += 1;
        }
    }
    // "A sender MUST NOT apply the chunked transfer coding more than once."
    if chunked != 1 || !final_is_chunked {
        return TransferCoding::Malformed;
    }
    if others == 0 {
        TransferCoding::Chunked
    } else {
        TransferCoding::Unsupported
    }
}

/// Fields whose semantics are fixed before a trailer section is reached.
///
/// Extension fields deliberately remain admissible. Their defining extension,
/// rather than this generic HTTP transport, decides whether it permits a
/// trailer occurrence. `content-digest` is such a trailer-safe extension.
pub(crate) fn trailer_field_name_is_forbidden(name: &[u8]) -> bool {
    match name.first().map(u8::to_ascii_lowercase) {
        Some(b'a') => name.eq_ignore_ascii_case(b"authorization"),
        Some(b'c') => {
            name.eq_ignore_ascii_case(b"connection")
                || name.eq_ignore_ascii_case(b"content-length")
                || name.eq_ignore_ascii_case(b"cache-control")
                || name.eq_ignore_ascii_case(b"content-encoding")
                || name.eq_ignore_ascii_case(b"content-language")
                || name.eq_ignore_ascii_case(b"content-location")
                || name.eq_ignore_ascii_case(b"content-range")
                || name.eq_ignore_ascii_case(b"content-type")
        },
        Some(b'e') => name.eq_ignore_ascii_case(b"expires") || name.eq_ignore_ascii_case(b"etag"),
        Some(b'h') => name.eq_ignore_ascii_case(b"host"),
        Some(b'i') => {
            name.eq_ignore_ascii_case(b"if-match")
                || name.eq_ignore_ascii_case(b"if-none-match")
                || name.eq_ignore_ascii_case(b"if-modified-since")
                || name.eq_ignore_ascii_case(b"if-unmodified-since")
                || name.eq_ignore_ascii_case(b"if-range")
        },
        Some(b'k') => name.eq_ignore_ascii_case(b"keep-alive"),
        Some(b'l') => name.eq_ignore_ascii_case(b"last-modified"),
        Some(b'p') => {
            name.eq_ignore_ascii_case(b"proxy-connection")
                || name.eq_ignore_ascii_case(b"proxy-authorization")
                || name.eq_ignore_ascii_case(b"proxy-authenticate")
                || name.eq_ignore_ascii_case(b"pragma")
        },
        Some(b't') => {
            name.eq_ignore_ascii_case(b"transfer-encoding")
                || name.eq_ignore_ascii_case(b"te")
                || name.eq_ignore_ascii_case(b"trailer")
        },
        Some(b'u') => name.eq_ignore_ascii_case(b"upgrade"),
        Some(b'w') => name.eq_ignore_ascii_case(b"www-authenticate"),
        _ => false,
    }
}

/// Headers owned by framing or connection setup cannot be configured as
/// response defaults: they would otherwise be copied into every response
/// before the protocol writer gets a chance to choose the real value.
pub(crate) const fn configured_response_field_is_forbidden(name: &[u8]) -> bool {
    matches!(
        application_response_field(name),
        ApplicationResponseField::Connection
            | ApplicationResponseField::ProxyConnection
            | ApplicationResponseField::KeepAlive
            | ApplicationResponseField::TransferEncoding
            | ApplicationResponseField::Upgrade
            | ApplicationResponseField::Te
    ) || name.eq_ignore_ascii_case(b"content-length")
        || name.eq_ignore_ascii_case(b"trailer")
}

fn percent_escapes_are_valid(value: &[u8]) -> bool {
    let mut rest = value;
    while let Some(index) = memchr(b'%', rest) {
        let Some(hex) = rest.get(index + 1..index + 3) else {
            return false;
        };
        if ascii::HEX_VALUE[usize::from(hex[0])] == ascii::INVALID_VALUE
            || ascii::HEX_VALUE[usize::from(hex[1])] == ascii::INVALID_VALUE
        {
            return false;
        }
        rest = &rest[index + 3..];
    }
    true
}

/// One scan of the application's response headers.
///
/// `CONTENT_LENGTH` is const rather than a runtime flag so the caller that has
/// no use for the declared length — default-header preparation, which only
/// asks whether `server` and `date` are present — does not pay to parse one.
/// The two spellings were previously the same loop written twice.
fn inspect_response_headers_with<const CONTENT_LENGTH: bool>(
    headers: &ResponseHeaders,
) -> ResponseHeaderScan {
    let mut scan = ResponseHeaderScan::new();

    for (name, value) in headers {
        match name.kind() {
            ResponseHeaderKind::Server => {
                scan.flags = scan.flags.union(ResponseScanFlags::HAS_SERVER);
            },
            ResponseHeaderKind::Date => scan.flags = scan.flags.union(ResponseScanFlags::HAS_DATE),
            ResponseHeaderKind::ContentLength if CONTENT_LENGTH => {
                scan.observe_content_length(value.as_bytes());
            },
            _ => {},
        }
    }

    scan
}

pub(crate) fn inspect_response_default_headers(headers: &ResponseHeaders) -> ResponseHeaderScan {
    inspect_response_headers_with::<false>(headers)
}

pub(crate) fn inspect_response_headers(headers: &ResponseHeaders) -> ResponseHeaderScan {
    inspect_response_headers_with::<true>(headers)
}

pub(crate) fn canonicalize_fixed_length_response_headers_with_scan(
    headers: &mut ResponseHeaders,
    scan: &mut ResponseHeaderScan,
    len: usize,
) {
    let content_length = scan.content_length_state();
    if content_length == ResponseContentLength::Valid(len) {
        return;
    }

    let len_value = if len == 0 {
        Bytes::from_static(b"0")
    } else {
        let mut len_buffer = ItoaBuffer::new();
        Bytes::copy_from_slice(len_buffer.format(len).as_bytes())
    };

    if content_length == ResponseContentLength::Missing {
        headers.push((
            Bytes::from_static(b"content-length").into(),
            len_value.into(),
        ));
        scan.set_content_length(len);
        return;
    }

    let mut seen = false;
    headers.retain_mut(|(name, value)| {
        if name.kind() != ResponseHeaderKind::ContentLength {
            return true;
        }
        if seen {
            return false;
        }
        *value = Bytes::clone(&len_value).into();
        seen = true;
        true
    });
    debug_assert!(seen);
    scan.set_content_length(len);
}

/// Wall-clock seconds since the Unix epoch.
///
/// The date header distinguishes whole seconds and nothing finer, so on Linux
/// this reads the coarse clock instead: `CLOCK_REALTIME_COARSE`
/// is a plain load of the kernel's cached wall clock, where `CLOCK_REALTIME`
/// additionally reads and scales a timestamp counter. The tick granularity is
/// still finer than the value it feeds. Every response takes this reading.
#[cfg(target_os = "linux")]
fn unix_seconds() -> u64 {
    let seconds = clock_gettime(ClockId::RealtimeCoarse).tv_sec;
    u64::try_from(seconds).unwrap_or_default()
}

#[cfg(not(target_os = "linux"))]
fn unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn cached_date_value() -> Bytes {
    // Per-thread cache: each worker thread keeps its own `(unix_seconds, value)` so
    // every response is a refcount bump on a thread-owned `Bytes`. The previous
    // shared `RwLock` had readers contending on the inner counter across all
    // workers; this version has zero cross-thread atomics on the fast path.
    thread_local! {
        static CACHE: RefCell<(u64, Bytes)> = const { RefCell::new((0, Bytes::new())) };
    }
    let unix_seconds = unix_seconds();
    CACHE.with_borrow_mut(|(cached_seconds, cached_value)| {
        if *cached_seconds != unix_seconds || cached_value.is_empty() {
            *cached_value = format_http_date(unix_seconds);
            *cached_seconds = unix_seconds;
        }
        Bytes::clone(cached_value)
    })
}

fn format_http_date(unix_seconds: u64) -> Bytes {
    let mut value = HTTP_DATE_TEMPLATE;
    write_http_date(&mut value, unix_seconds);
    Bytes::copy_from_slice(&value)
}

fn write_http_date(out: &mut [u8; 29], unix_seconds: u64) {
    let days = (unix_seconds / SECONDS_PER_DAY) as i64;
    let seconds_of_day = (unix_seconds % SECONDS_PER_DAY) as usize;
    let hour = seconds_of_day / 3_600;
    let minute = (seconds_of_day / 60) % 60;
    let second = seconds_of_day % 60;
    let (year, month, day) = civil_from_days(days);
    debug_assert!(year < 10_000);

    out[..3].copy_from_slice(&WEEKDAYS[(days + 4).rem_euclid(7) as usize]);
    out[5..7].copy_from_slice(&digits::two_digit_bytes(day as usize));
    out[8..11].copy_from_slice(&MONTHS[(month - 1) as usize]);
    out[12..14].copy_from_slice(&digits::two_digit_bytes((year / 100) as usize));
    out[14..16].copy_from_slice(&digits::two_digit_bytes((year % 100) as usize));
    out[17..19].copy_from_slice(&digits::two_digit_bytes(hour));
    out[20..22].copy_from_slice(&digits::two_digit_bytes(minute));
    out[23..25].copy_from_slice(&digits::two_digit_bytes(second));
}

fn civil_from_days(days: i64) -> (u32, u32, u32) {
    let shifted = days + 719_468;
    let era = if shifted >= 0 {
        shifted
    } else {
        shifted - 146_096
    } / 146_097;
    let day_of_era = shifted - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    (year as u32, month as u32, day as u32)
}

pub(crate) fn apply_default_response_headers_with_scan(
    headers: &mut ResponseHeaders,
    scan: &mut ResponseHeaderScan,
    config: &ServerConfig,
) {
    let defaults = &config.response_headers;
    headers.reserve(
        usize::from(defaults.server_header.is_some() && !scan.has_server())
            + usize::from(defaults.date_header && !scan.has_date())
            + defaults.extra_headers.len(),
    );
    append_default_response_headers(headers, scan, defaults);
}

/// Fill in what the application did not send, most specific source first.
///
/// The ladder is: the application's own header wins, then a header the
/// operator configured by name, then this server's built-in defaults. That
/// ordering is why the configured headers are applied first — a `server:`
/// given with `--header` is a deliberate choice and must beat the generic
/// `server_header` mode.
fn append_default_response_headers(
    headers: &mut ResponseHeaders,
    scan: &mut ResponseHeaderScan,
    defaults: &ResponseHeaderConfig,
) {
    for header in &defaults.extra_headers {
        // Configured headers are defaults, exactly like `server` and `date`:
        // an application that set the name itself keeps its own value, and the
        // response never carries the same name twice. Appending regardless put
        // two of them on the wire.
        if headers
            .iter()
            .any(|(name, _)| name.as_bytes().eq_ignore_ascii_case(header.name.as_ref()))
        {
            continue;
        }
        match header.kind {
            ResponseHeaderKind::Server => scan.flags.insert(ResponseScanFlags::HAS_SERVER),
            ResponseHeaderKind::Date => scan.flags.insert(ResponseScanFlags::HAS_DATE),
            ResponseHeaderKind::ContentLength => scan.observe_content_length(header.value.as_ref()),
            _ => {},
        }
        headers.push((
            ResponseHeaderName::from_configured(Bytes::clone(&header.name), header.kind),
            Bytes::clone(&header.value).into(),
        ));
    }
    if let Some(server) = &defaults.server_header
        && !scan.has_server()
    {
        headers.push((
            Bytes::from_static(b"server").into(),
            Bytes::clone(server).into(),
        ));
        scan.flags = scan.flags.union(ResponseScanFlags::HAS_SERVER);
    }
    if defaults.date_header && !scan.has_date() {
        headers.push((
            Bytes::from_static(b"date").into(),
            cached_date_value().into(),
        ));
        scan.flags = scan.flags.union(ResponseScanFlags::HAS_DATE);
    }
}

pub(crate) fn prepare_fixed_length_response_headers_with_scan(
    headers: &mut ResponseHeaders,
    scan: &mut ResponseHeaderScan,
    defaults: &ResponseHeaderConfig,
    len: usize,
) {
    let additional = usize::from(defaults.server_header.is_some() && !scan.has_server())
        + usize::from(defaults.date_header && !scan.has_date())
        + defaults.extra_headers.len()
        + usize::from(scan.content_length_state() == ResponseContentLength::Missing);
    headers.reserve(additional);
    append_default_response_headers(headers, scan, defaults);
    canonicalize_fixed_length_response_headers_with_scan(headers, scan, len);
}

pub(crate) fn prepare_response_headers_without_content_length(
    headers: &mut ResponseHeaders,
    scan: &mut ResponseHeaderScan,
    defaults: &ResponseHeaderConfig,
) {
    let additional = usize::from(defaults.server_header.is_some() && !scan.has_server())
        + usize::from(defaults.date_header && !scan.has_date())
        + defaults.extra_headers.len();
    headers.reserve(additional);
    append_default_response_headers(headers, scan, defaults);
    if scan.content_length_state() != ResponseContentLength::Missing {
        headers.retain(|(name, _)| name.kind() != ResponseHeaderKind::ContentLength);
        scan.clear_content_length();
    }
}

/// Prepare an indeterminate-length response. A valid declared length remains
/// on the wire and is returned for stream enforcement; any invalid or
/// conflicting declaration is removed before a protocol writer can emit it.
pub(crate) fn prepare_streaming_response_headers_with_scan(
    headers: &mut ResponseHeaders,
    scan: &mut ResponseHeaderScan,
    defaults: &ResponseHeaderConfig,
) -> Option<usize> {
    let length = scan.content_length();
    if length.is_some() {
        let additional = usize::from(defaults.server_header.is_some() && !scan.has_server())
            + usize::from(defaults.date_header && !scan.has_date())
            + defaults.extra_headers.len();
        headers.reserve(additional);
        append_default_response_headers(headers, scan, defaults);
    } else {
        prepare_response_headers_without_content_length(headers, scan, defaults);
    }
    length
}

pub(crate) fn apply_default_response_headers(headers: &mut ResponseHeaders, config: &ServerConfig) {
    let mut scan = inspect_response_default_headers(headers);
    apply_default_response_headers_with_scan(headers, &mut scan, config);
}

pub(crate) fn request_header_name_needs_lowercase(name: &[u8]) -> Option<bool> {
    if name.is_empty() {
        return None;
    }

    let mut needs_lowercase = false;
    for byte in name {
        let flags = ascii::HEADER_NAME_FLAGS[usize::from(*byte)];
        if flags & ascii::HEADER_NAME_VALID == 0 {
            return None;
        }
        needs_lowercase |= flags & ascii::HEADER_NAME_UPPER != 0;
    }
    Some(needs_lowercase)
}

pub(crate) fn lowercase_header_name_is_valid(name: &[u8]) -> bool {
    if name.is_empty() {
        return false;
    }

    for &byte in name {
        let flags = ascii::HEADER_NAME_FLAGS[usize::from(byte)];
        if flags & ascii::HEADER_NAME_VALID == 0 || flags & ascii::HEADER_NAME_UPPER != 0 {
            return false;
        }
    }

    true
}

struct ForwardedParameterValue<'a> {
    value: &'a str,
    quoted: bool,
}

impl<'a> ForwardedParameterValue<'a> {
    fn parse(value: &'a str) -> Option<Self> {
        if let Some(value) = value.strip_prefix('"') {
            let value = value.strip_suffix('"')?;
            let mut escaped = false;
            for &byte in value.as_bytes() {
                if escaped {
                    if !(byte == b'\t' || byte == b' ' || byte >= 0x21) || byte == 0x7F {
                        return None;
                    }
                    escaped = false;
                } else if byte == b'\\' {
                    escaped = true;
                } else if byte == b'"' || (byte < 0x20 && byte != b'\t') || byte == 0x7F {
                    return None;
                }
            }
            if escaped {
                return None;
            }
            return Some(Self {
                value,
                quoted: true,
            });
        }
        protocol_token_is_valid(value.as_bytes()).then_some(Self {
            value,
            quoted: false,
        })
    }

    fn decode(self) -> Cow<'a, str> {
        if !self.quoted || !self.value.as_bytes().contains(&b'\\') {
            return Cow::Borrowed(self.value);
        }

        let mut decoded = Vec::with_capacity(self.value.len() - 1);
        let mut bytes = self.value.bytes();
        while let Some(byte) = bytes.next() {
            decoded.push(if byte == b'\\' {
                bytes
                    .next()
                    .expect("validated quoted-pair has an escaped byte")
            } else {
                byte
            });
        }
        // SAFETY: the input was UTF-8 and quoted-pair removes only ASCII backslashes.
        Cow::Owned(unsafe { String::from_utf8_unchecked(decoded) })
    }

    fn node(self) -> Result<Option<Cow<'a, str>>, ()> {
        let quoted = self.quoted;
        match self.decode() {
            Cow::Borrowed(value) => match parse_forwarded_node(value, quoted).ok_or(())? {
                ForwardedNode::Usable(host) => Ok(Some(Cow::Borrowed(host))),
                ForwardedNode::Opaque => Ok(None),
            },
            Cow::Owned(value) => match parse_forwarded_node(&value, quoted).ok_or(())? {
                ForwardedNode::Usable(host) => Ok(Some(Cow::Owned(host.to_owned()))),
                ForwardedNode::Opaque => Ok(None),
            },
        }
    }
}

struct ForwardedDelimiterCursor<'a> {
    value: &'a str,
    offset: usize,
    delimiter: u8,
}

impl<'a> ForwardedDelimiterCursor<'a> {
    const fn new(value: &'a str, delimiter: u8) -> Self {
        Self {
            value,
            offset: 0,
            delimiter,
        }
    }

    fn next(&mut self) -> Option<Result<&'a str, ()>> {
        if self.offset > self.value.len() {
            return None;
        }
        let start = self.offset;
        let bytes = self.value.as_bytes();
        let mut quoted = false;
        let mut escaped = false;
        for (index, &byte) in bytes.iter().enumerate().skip(start) {
            if (!quoted && byte < 0x20 && byte != b'\t') || byte == 0x7F {
                self.offset = self.value.len() + 1;
                return Some(Err(()));
            }
            if escaped {
                if !(byte == b'\t' || byte == b' ' || byte >= 0x21) || byte == 0x7F {
                    self.offset = self.value.len() + 1;
                    return Some(Err(()));
                }
                escaped = false;
            } else if quoted && byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                quoted = !quoted;
            } else if byte == self.delimiter && !quoted {
                self.offset = index + 1;
                return Some(self.value.get(start..index).ok_or(()));
            }
        }
        self.offset = self.value.len() + 1;
        if quoted || escaped {
            Some(Err(()))
        } else {
            Some(self.value.get(start..).ok_or(()))
        }
    }
}

#[derive(Clone, Copy)]
struct ForwardedExtensionName<'a>(&'a str);

impl PartialEq for ForwardedExtensionName<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq_ignore_ascii_case(other.0)
    }
}

impl Eq for ForwardedExtensionName<'_> {}

impl Hash for ForwardedExtensionName<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for byte in self.0.bytes() {
            state.write_u8(byte.to_ascii_lowercase());
        }
    }
}

enum ForwardedNode<'a> {
    Usable(&'a str),
    Opaque,
}

/// RFC 9110 7.8: protocol names are registered with a preferred case, but
/// recipients should match them case-insensitively. The HTTP/1 side already
/// does, via `header_contains_token`; matching exactly here made
/// `:protocol: WebSocket` fall through to a 501.
pub(crate) fn protocol_is_websocket(protocol: &Protocol) -> bool {
    protocol.as_str().eq_ignore_ascii_case("websocket")
}

pub(crate) fn header_value_text(value: &[u8]) -> Option<&str> {
    let value = str::from_utf8(value).ok()?.trim_ascii();
    (!value.is_empty()).then_some(value)
}

fn forwarded_ows_trim(value: &str) -> &str {
    value.trim_matches(|byte| byte == ' ' || byte == '\t')
}

fn forwarded_obfuscated(value: &str) -> bool {
    value.len() > 1
        && value.as_bytes()[0] == b'_'
        && value.as_bytes()[1..]
            .iter()
            .all(|&byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

fn forwarded_port(value: &str) -> bool {
    (!value.is_empty() && value.len() <= 5 && value.bytes().all(|byte| byte.is_ascii_digit()))
        || forwarded_obfuscated(value)
}

fn parse_forwarded_node(value: &str, quoted: bool) -> Option<ForwardedNode<'_>> {
    if value.starts_with('[') {
        if !quoted {
            return None;
        }
        let (host, rest) = value.split_once(']')?;
        let host = host.strip_prefix('[')?;
        if host.parse::<std::net::Ipv6Addr>().is_err() {
            return None;
        }
        if !rest.is_empty() && !rest.strip_prefix(':').is_some_and(forwarded_port) {
            return None;
        }
        return Some(ForwardedNode::Usable(host));
    }
    if value.contains(':') {
        if !quoted {
            return None;
        }
        let (host, port) = value.rsplit_once(':')?;
        if !forwarded_port(port) {
            return None;
        }
        if host.eq_ignore_ascii_case("unknown") || forwarded_obfuscated(host) {
            return Some(ForwardedNode::Opaque);
        }
        if host.parse::<std::net::Ipv4Addr>().is_err() {
            return None;
        }
        return Some(ForwardedNode::Usable(host));
    }
    if value.eq_ignore_ascii_case("unknown") || forwarded_obfuscated(value) {
        return Some(ForwardedNode::Opaque);
    }
    if value.parse::<std::net::Ipv4Addr>().is_ok() {
        return Some(ForwardedNode::Usable(value));
    }
    None
}

fn forwarded_reg_name(value: &str) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.iter().enumerate().all(|(index, &byte)| {
            if byte == b'%' {
                return index + 2 < bytes.len()
                    && bytes[index + 1].is_ascii_hexdigit()
                    && bytes[index + 2].is_ascii_hexdigit();
            }
            byte.is_ascii_alphanumeric()
                || matches!(byte, b'-' | b'.' | b'_' | b'~')
                || matches!(
                    byte,
                    b'!' | b'$' | b'&' | b'\'' | b'(' | b')' | b'*' | b'+' | b',' | b';' | b'='
                )
        })
}

fn forwarded_ipv_future(value: &str) -> bool {
    let Some((version, address)) = value
        .strip_prefix(['v', 'V'])
        .and_then(|value| value.split_once('.'))
    else {
        return false;
    };
    !version.is_empty()
        && version.bytes().all(|byte| byte.is_ascii_hexdigit())
        && !address.is_empty()
        && address.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(
                    byte,
                    b'-' | b'.'
                        | b'_'
                        | b'~'
                        | b':'
                        | b'!'
                        | b'$'
                        | b'&'
                        | b'\''
                        | b'('
                        | b')'
                        | b'*'
                        | b'+'
                        | b','
                        | b';'
                        | b'='
                )
        })
}

fn parse_forwarded_authority(value: &str) -> Option<(&str, Option<u16>)> {
    if value.starts_with('[') {
        let (host, rest) = value.split_once(']')?;
        let host = host.strip_prefix('[')?;
        if host.parse::<std::net::Ipv6Addr>().is_err() && !forwarded_ipv_future(host) {
            return None;
        }
        let port = if rest.is_empty() {
            None
        } else {
            Some(parse_pos::<u16, false>(rest.strip_prefix(':')?.as_bytes()).ok()?)
        };
        return Some((host, port));
    }
    if value.contains(':') {
        let (host, port) = value.rsplit_once(':')?;
        if host.contains(':')
            || (host.parse::<std::net::Ipv4Addr>().is_err() && !forwarded_reg_name(host))
        {
            return None;
        }
        return Some((host, Some(parse_pos::<u16, false>(port.as_bytes()).ok()?)));
    }
    if value.parse::<std::net::Ipv4Addr>().is_err() && !forwarded_reg_name(value) {
        return None;
    }
    Some((value, None))
}

fn parse_forwarded_element(element: &str) -> Option<ForwardedView<'_>> {
    let mut client_host = None;
    let mut proto = None;
    let mut host = None;
    let mut recognized_names = ForwardedParameterNames::empty();
    let mut extension_names = None::<HashSet<ForwardedExtensionName<'_>>>;
    let mut cursor = ForwardedDelimiterCursor::new(element, b';');
    while let Some(part) = cursor.next() {
        let parameter_part = forwarded_ows_trim(part.ok()?);
        if parameter_part.is_empty() {
            continue;
        }
        let (name, value) = parameter_part.split_once('=')?;
        if !protocol_token_is_valid(name.as_bytes()) {
            return None;
        }
        let parameter = ForwardedParameterValue::parse(value)?;
        let recognized_name = if name.eq_ignore_ascii_case("for") {
            ForwardedParameterNames::FOR
        } else if name.eq_ignore_ascii_case("by") {
            ForwardedParameterNames::BY
        } else if name.eq_ignore_ascii_case("host") {
            ForwardedParameterNames::HOST
        } else if name.eq_ignore_ascii_case("proto") {
            ForwardedParameterNames::PROTO
        } else {
            ForwardedParameterNames::empty()
        };
        if recognized_name.is_empty() {
            if !extension_names
                .get_or_insert_with(HashSet::new)
                .insert(ForwardedExtensionName(name))
            {
                return None;
            }
        } else {
            if recognized_names.contains(recognized_name) {
                return None;
            }
            recognized_names.insert(recognized_name);
        }
        if recognized_name == ForwardedParameterNames::FOR {
            client_host = parameter.node().ok()?;
        } else if recognized_name == ForwardedParameterNames::PROTO {
            proto = Some(parameter.decode());
            if !request_scheme_is_valid(proto.as_ref()?.as_bytes()) {
                return None;
            }
        } else if recognized_name == ForwardedParameterNames::HOST {
            let decoded = parameter.decode();
            host = match decoded {
                Cow::Borrowed(value) => {
                    let (host, authority_port) = parse_forwarded_authority(value)?;
                    Some((Cow::Borrowed(host), authority_port))
                },
                Cow::Owned(value) => {
                    let (host, authority_port) = parse_forwarded_authority(&value)?;
                    Some((Cow::Owned(host.to_owned()), authority_port))
                },
            };
        } else if recognized_name == ForwardedParameterNames::BY {
            parameter.node().ok()?;
        }
    }

    Some(ForwardedView {
        client_host,
        proto,
        host,
    })
}

/// The trusted view of a `Forwarded` header.
///
/// `proto` and `host` describe the hop that handed us the request, so they come
/// from the last element alone. `for` identifies the *client*, so it is walked
/// back through hops matching `--forwarded-allow-ips` exactly as
/// `parse_x_forwarded_for_value` walks `X-Forwarded-For` — taking the last
/// element's `for` would report the nearest proxy instead of the client
/// whenever more than one trusted hop is in front.
pub(crate) fn parse_forwarded_value<'a>(
    value: &'a str,
    config: &ServerConfig,
) -> Option<ForwardedView<'a>> {
    let mut cursor = ForwardedDelimiterCursor::new(value, b',');
    let mut selected = None;
    let mut nearest = None;
    let mut element_count = 0;
    while let Some(element) = cursor.next() {
        let element = element.ok()?;
        let element = forwarded_ows_trim(element);
        if element.is_empty() {
            continue;
        }
        element_count += 1;
        let parsed = parse_forwarded_element(element)?;
        let ForwardedView {
            client_host,
            proto,
            host,
        } = parsed;
        if let Some(host) = client_host {
            if trusted_host_matches(&config.proxy.trusted_peers, &host) {
                if selected.is_none() {
                    selected = Some(host);
                }
            } else {
                selected = Some(host);
            }
        } else {
            selected = None;
        }
        nearest = Some((proto, host));
    }
    if element_count == 0 {
        return None;
    }
    let (proto, host) = nearest?;
    Some(ForwardedView {
        client_host: selected,
        proto,
        host,
    })
}

pub(crate) fn parse_x_forwarded_for_value<'a>(
    value: &'a str,
    config: &ServerConfig,
) -> Option<&'a str> {
    let mut furthest_host = None;

    for host in value.rsplit(',').map(normalize_forwarded_value) {
        if host.is_empty() {
            continue;
        }
        let (normalized, _) = parse_host_port(host)?;
        furthest_host = Some(normalized);
        if !trusted_host_matches(&config.proxy.trusted_peers, normalized) {
            return Some(normalized);
        }
    }

    furthest_host
}

pub(crate) fn normalize_scheme(scheme: &str) -> Cow<'_, str> {
    if !scheme.as_bytes().iter().any(u8::is_ascii_uppercase) {
        return Cow::Borrowed(scheme);
    }

    Cow::Owned(scheme.to_ascii_lowercase())
}

pub(crate) fn parse_host_port(value: &str) -> Option<(&str, Option<u16>)> {
    if let Some(value) = value.strip_prefix('[') {
        let (host, rest) = value.split_once(']')?;
        if !forwarded_host_is_valid(host) {
            return None;
        }
        if rest.is_empty() {
            return Some((host, None));
        }
        let port = rest
            .strip_prefix(':')
            .and_then(|port| parse_pos::<u16, false>(port.as_bytes()).ok())?;
        return Some((host, Some(port)));
    }

    if let Some((host, port)) = value.rsplit_once(':')
        && !host.contains(':')
    {
        if !forwarded_host_is_valid(host) {
            return None;
        }
        return parse_pos::<u16, false>(port.as_bytes())
            .ok()
            .map(|port| (host, Some(port)));
    }

    forwarded_host_is_valid(value).then_some((value, None))
}

fn forwarded_host_is_valid(value: &str) -> bool {
    !value.is_empty()
        && value
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(*byte, b'.' | b':' | b'-'))
}

fn normalize_forwarded_value(value: &str) -> &str {
    value.trim_ascii().trim_matches('"')
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::{
        ResponseContentLength, civil_from_days, format_http_date, inspect_response_default_headers,
        inspect_response_headers, last_csv_token, parse_content_length_header,
        parse_forwarded_value, parse_host_port, parse_x_forwarded_for_value,
        prepare_fixed_length_response_headers_with_scan, request_authority_is_valid,
        request_path_is_valid, request_scheme_is_valid, trailer_field_name_is_forbidden,
    };
    use crate::config::{
        ConfiguredResponseHeader, ProxyConfig, ResponseHeaderConfig, ServerConfig,
    };
    use crate::http::header_value::header_value_is_valid;
    use crate::http::types::ResponseHeaders;
    use crate::proxy_protocol::{ProxyProtocolMode, parse_trusted_peer};
    use crate::runtime::test_fixtures;

    #[test]
    fn content_length_parser_accepts_trimmed_ascii_digits() {
        for (value, expected) in [
            (b"0".as_slice(), 0),
            (b"1", 1),
            (b"18446744073709551615", u64::MAX),
            (b" 42\t", 42),
            (b"42, 42", 42),
            (b"42,42, 42\t", 42),
        ] {
            assert_eq!(parse_content_length_header(value), Some(expected));
        }

        let leading_zeroes = [b'0'; 1024];
        assert_eq!(parse_content_length_header(&leading_zeroes), Some(0));
        assert_eq!(
            parse_content_length_header(&[leading_zeroes.as_slice(), b"1"].concat()),
            Some(1)
        );
    }

    #[test]
    fn content_length_parser_rejects_invalid_values() {
        assert_eq!(parse_content_length_header(b""), None);
        assert_eq!(parse_content_length_header(b" \t "), None);
        assert_eq!(parse_content_length_header(b"4x"), None);
        assert_eq!(parse_content_length_header(b"42, 43"), None);
        assert_eq!(parse_content_length_header(b"42,"), None);
        assert_eq!(parse_content_length_header(b",42"), None);
        assert_eq!(parse_content_length_header(b"+1"), None);
        assert_eq!(parse_content_length_header(b"18446744073709551616"), None);
        assert_eq!(parse_content_length_header(b"\xff"), None);
    }

    #[test]
    fn request_target_grammar_rejects_only_raw_illegal_octets() {
        assert!(request_scheme_is_valid(b"https+unix.1"));
        assert!(!request_scheme_is_valid(b"1https"));
        assert!(request_authority_is_valid(b"example:99999"));
        assert!(request_authority_is_valid(b"[2001:db8::1]:443"));
        assert!(request_authority_is_valid(b"example"));
        for authority in [
            b"user@example:443".as_slice(),
            b"example,other",
            b"example/path",
            b"[2001:db8::1",
            b"2001:db8::1",
            b"example:http",
        ] {
            assert!(!request_authority_is_valid(authority), "{authority:?}");
        }
        let get = http::Method::GET;
        let options = http::Method::OPTIONS;
        assert!(request_path_is_valid(&get, b"/%00/ok%20here"));
        assert!(!request_path_is_valid(&get, b"/bad%0"));
        assert!(!request_path_is_valid(&get, b"/bad%G0"));
        assert!(!request_path_is_valid(&get, b"/bad#fragment"));
        assert!(request_path_is_valid(&options, b"*"));
        assert!(!request_path_is_valid(&get, b"*"));
    }

    #[test]
    fn trailer_field_classifier_preserves_the_exact_case_insensitive_set() {
        for forbidden in [
            b"connection".as_slice(),
            b"proxy-connection",
            b"keep-alive",
            b"transfer-encoding",
            b"upgrade",
            b"te",
            b"host",
            b"content-length",
            b"trailer",
            b"authorization",
            b"proxy-authorization",
            b"www-authenticate",
            b"proxy-authenticate",
            b"if-match",
            b"if-none-match",
            b"if-modified-since",
            b"if-unmodified-since",
            b"if-range",
            b"cache-control",
            b"pragma",
            b"expires",
            b"content-encoding",
            b"content-language",
            b"content-location",
            b"content-range",
            b"content-type",
            b"last-modified",
            b"etag",
        ] {
            assert!(trailer_field_name_is_forbidden(forbidden), "{forbidden:?}");
            let upper = forbidden
                .iter()
                .map(u8::to_ascii_uppercase)
                .collect::<Vec<_>>();
            assert!(trailer_field_name_is_forbidden(&upper), "{upper:?}");
        }
        for allowed in [b"content-digest".as_slice(), b"digest", b"x-trailer", b""] {
            assert!(!trailer_field_name_is_forbidden(allowed), "{allowed:?}");
        }
    }

    #[test]
    fn header_value_validation_accepts_long_visible_ascii_and_rejects_controls() {
        assert!(header_value_is_valid(
            b"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/135.0 Safari/537.36"
        ));
        assert!(header_value_is_valid(
            b"session=abc123; theme=light; csrftoken=def456; locale=en-US"
        ));
        assert!(header_value_is_valid(b"\tallowed-tab"));
        assert!(!header_value_is_valid(b"abc\x7fxyz"));
        assert!(!header_value_is_valid(b"line\nbreak"));
    }

    #[test]
    fn last_csv_token_ignores_commas_inside_quotes() {
        assert_eq!(last_csv_token("a, b"), "b");
        assert_eq!(
            last_csv_token("for=1.1.1.1;host=\"a,b\", for=2.2.2.2"),
            "for=2.2.2.2"
        );
        assert_eq!(
            last_csv_token("host=\"a,\\\"b\\\"\", proto=https"),
            "proto=https"
        );
    }

    #[test]
    fn parse_host_port_rejects_empty_or_malformed_hosts() {
        assert_eq!(parse_host_port(":443"), None);
        assert_eq!(parse_host_port("example:"), None);
        assert_eq!(parse_host_port("example:abc"), None);
        assert_eq!(parse_host_port("[]:443"), None);
        assert_eq!(parse_host_port("1.1.1.1 \"GET /admin HTTP/1.1\" 200"), None);
        assert_eq!(parse_host_port("\u{e9}"), None);
    }

    #[test]
    fn parse_host_port_keeps_valid_host_forms() {
        assert_eq!(parse_host_port("example.com"), Some(("example.com", None)));
        assert_eq!(
            parse_host_port("example.com:443"),
            Some(("example.com", Some(443)))
        );
        assert_eq!(
            parse_host_port("[2001:db8::1]:443"),
            Some(("2001:db8::1", Some(443)))
        );
        assert_eq!(parse_host_port("2001:db8::1"), Some(("2001:db8::1", None)));
    }

    #[test]
    fn unix_trust_never_skips_textual_x_forwarded_for_hops() {
        let config = ServerConfig {
            proxy: ProxyConfig {
                trust_headers: true,
                trusted_peers: Box::new([parse_trusted_peer("unix").unwrap()]),
                protocol: ProxyProtocolMode::Off,
            },
            ..test_fixtures::server_config_parts()
        };

        assert_eq!(
            parse_x_forwarded_for_value("203.0.113.10, 198.51.100.7", &config),
            Some("198.51.100.7"),
        );
    }

    #[test]
    fn forged_x_forwarded_for_never_becomes_a_client_label() {
        let config = ServerConfig {
            proxy: ProxyConfig {
                trust_headers: true,
                trusted_peers: Box::new([parse_trusted_peer("unix").unwrap()]),
                protocol: ProxyProtocolMode::Off,
            },
            ..test_fixtures::server_config_parts()
        };

        assert_eq!(
            parse_x_forwarded_for_value("1.1.1.1 \"GET /admin HTTP/1.1\" 200", &config),
            None,
        );
    }

    #[test]
    fn forwarded_parser_accepts_quoted_delimiters_and_escaped_pairs() {
        let config = test_fixtures::server_config_parts();
        for value in [
            r#"for=203.0.113.10;proto=https;host="good.example:8443";ext="edge;one""#,
            r#"for=203.0.113.10;proto=https;host="good.example:8443";ext="edge\;one""#,
            r#"for="203.0.113.10";proto="https";host="good.example:8443";ext="edge;one""#,
            r#"for="203.0.113.10";ext="edge\;one""#,
            r#"for=203.0.113.10;ext="edge,one",for=198.51.100.7"#,
            r#"for=203.0.113.10;ext="edge\,one",for=198.51.100.7"#,
            "for=203.0.113.10;ext=unknown",
        ] {
            assert!(parse_forwarded_value(value, &config).is_some(), "{value:?}");
        }
        let parsed = parse_forwarded_value(
            r#"for="203.0.113.10";proto="https";host="good.example:8443";ext="edge\;one""#,
            &config,
        )
        .expect("quoted delimiters and pairs are valid");
        assert_eq!(parsed.client_host.as_deref(), Some("203.0.113.10"));
        assert_eq!(parsed.proto.as_deref(), Some("https"));
        assert_eq!(
            parsed
                .host
                .as_ref()
                .map(|(host, port)| (host.as_ref(), *port)),
            Some(("good.example", Some(8443)))
        );
    }

    #[test]
    fn forwarded_parser_rejects_malformed_and_duplicate_parameters() {
        let config = test_fixtures::server_config_parts();
        for value in [
            "for=1.1.1.1;proto",
            "for=1.1.1.1;proto=https=oops",
            "for=1.1.1.1;=https",
            "for=1.1.1.1;proto=",
            "for=1.1.1.1;proto=\"https\"junk",
            "for=1.1.1.1;proto=\"https",
            "for=1.1.1.1;proto=\"https\\",
            "for=1.1.1.1;proto=https;proto=http",
            "for=1.1.1.1;ext=x;EXT=y",
            "for=1.1.1.1;ext=\"bad\u{0001}\"",
            "for=1.1.1.1,proto=\"unterminated;for=2.2.2.2",
        ] {
            assert!(parse_forwarded_value(value, &config).is_none(), "{value:?}");
        }
    }

    #[test]
    fn forwarded_parser_accepts_empty_optional_pairs_but_not_empty_list_elements() {
        let config = test_fixtures::server_config_parts();
        for value in [
            ";for=1.1.1.1;",
            ";;for=1.1.1.1;;proto=https;;",
            "for=1.1.1.1;;proto=https",
        ] {
            assert!(parse_forwarded_value(value, &config).is_some(), "{value:?}");
        }
        for value in [",for=1.1.1.1", "for=1.1.1.1,", "for=1.1.1.1,,proto=https"] {
            assert!(parse_forwarded_value(value, &config).is_some(), "{value:?}");
        }
        for value in ["", ",", ", ,\t,"] {
            assert!(parse_forwarded_value(value, &config).is_none(), "{value:?}");
        }
    }

    #[test]
    fn forwarded_parser_validates_nodes_and_authorities_atomically() {
        let config = test_fixtures::server_config_parts();
        for value in [
            r#"for="203.0.113.10:abc";proto=https;host=good.example"#,
            "for=203.0.113.10;proto=https;host=[bad",
            "for=203.0.113.10;proto=https;host=2001:db8::1",
            "for=203.0.113.10;proto=https;host=good%2.example",
            "for=203.0.113.10;proto=https;host=good.example:65536",
        ] {
            assert!(parse_forwarded_value(value, &config).is_none(), "{value:?}");
        }
        for value in [
            "for=unknown;proto=https;host=good.example",
            "for=_hidden;proto=https;host=good.example",
            r#"for="[2001:db8::1]:443";proto=https;host=good.example"#,
            "for=203.0.113.10;proto=https;host=good.example",
            "for=203.0.113.10;proto=https;host=good%2Eexample",
            "for=203.0.113.10;proto=https;host=999.999.999.999",
            r#"for=203.0.113.10;proto=https;host="[v1.a]""#,
            r#"for=203.0.113.10;proto=https;host="[Vf.a:b]:443""#,
        ] {
            assert!(parse_forwarded_value(value, &config).is_some(), "{value:?}");
        }
    }

    #[test]
    fn forwarded_parser_matches_rfc_node_grammar() {
        let config = test_fixtures::server_config_parts();
        for value in [
            "for=UNKNOWN",
            "for=Unknown",
            r#"for="unknown:443";by="Unknown:_trace""#,
            r#"for="_hidden:123";by="_hidden:_port""#,
            r#"for="203.0.113.10:99999";by="[2001:db8::1]:_port""#,
            r#"for="[2001:db8::1]:12345""#,
        ] {
            assert!(parse_forwarded_value(value, &config).is_some(), "{value:?}");
        }
        for value in [
            "for=unknown:443",
            r#"for="unknown:""#,
            r#"for="_hidden:""#,
            r#"for="_hidden:123456""#,
            r#"for="_hidden:_trace!""#,
            r#"for="203.0.113.10:123456""#,
            r#"for="203.0.113.10:_trace!""#,
            r#"for=203.0.113.10;by="_bad!";proto=https"#,
            r#"for=203.0.113.10;by="203.0.113.999";proto=https"#,
        ] {
            assert!(parse_forwarded_value(value, &config).is_none(), "{value:?}");
        }
    }

    #[test]
    fn forwarded_parser_validates_every_element_and_allows_unknown_extensions() {
        let config = test_fixtures::server_config_parts();
        assert!(
            parse_forwarded_value("for=1.1.1.1;ext=valid, for=2.2.2.2;unknown=token", &config,)
                .is_some()
        );
        assert!(
            parse_forwarded_value("for=1.1.1.1;ext=valid, broken=\"unterminated", &config,)
                .is_none()
        );
    }

    #[test]
    fn forwarded_parser_detects_extension_duplicates_exactly() {
        let config = test_fixtures::server_config_parts();
        // These names collided in the former 1,024-bit probabilistic filter.
        assert!(parse_forwarded_value("for=1.1.1.1;x8=a;x20=b", &config).is_some());
        assert!(parse_forwarded_value("for=1.1.1.1;x8=a;X8=b", &config).is_none());

        let mut value = String::from("for=1.1.1.1");
        for index in 0..900 {
            use std::fmt::Write;
            write!(value, ";x{index}=v").unwrap();
        }
        assert!(parse_forwarded_value(&value, &config).is_some());
    }

    #[test]
    fn response_header_scan_detects_default_headers_and_content_length() {
        let headers = vec![
            (
                Bytes::from_static(b"server").into(),
                Bytes::from_static(b"h2corn").into(),
            ),
            (
                Bytes::from_static(b"date").into(),
                Bytes::from_static(b"Fri, 17 Apr 2026 12:00:00 GMT").into(),
            ),
            (
                Bytes::from_static(b"content-length").into(),
                Bytes::from_static(b"42").into(),
            ),
        ];

        let scan = inspect_response_headers(&headers);

        assert!(scan.has_server());
        assert!(scan.has_date());
        assert_eq!(
            scan.content_length_state(),
            ResponseContentLength::Valid(42)
        );
    }

    #[test]
    fn default_response_header_scan_has_a_complete_missing_length_state() {
        let scan = inspect_response_default_headers(&ResponseHeaders::new());

        assert_eq!(scan.content_length(), None);
    }

    #[test]
    fn response_header_scan_marks_duplicate_content_length_for_rewrite() {
        let headers: ResponseHeaders = vec![
            (
                Bytes::from_static(b"content-length").into(),
                Bytes::from_static(b"1").into(),
            ),
            (
                Bytes::from_static(b"content-length").into(),
                Bytes::from_static(b"1").into(),
            ),
        ];

        let scan = inspect_response_headers(&headers);

        assert_eq!(
            scan.content_length_state(),
            ResponseContentLength::NeedsRewrite
        );
    }

    #[test]
    fn response_header_scan_marks_invalid_content_length_for_rewrite() {
        let headers: ResponseHeaders = vec![(
            Bytes::from_static(b"content-length").into(),
            Bytes::from_static(b"oops").into(),
        )];

        let scan = inspect_response_headers(&headers);

        assert_eq!(
            scan.content_length_state(),
            ResponseContentLength::NeedsRewrite
        );
    }

    #[test]
    fn fixed_length_preparation_preserves_order_and_canonicalizes_duplicates() {
        let mut headers: ResponseHeaders = vec![
            (
                Bytes::from_static(b"x-first").into(),
                Bytes::from_static(b"one").into(),
            ),
            (
                Bytes::from_static(b"content-length").into(),
                Bytes::from_static(b"9").into(),
            ),
            (
                Bytes::from_static(b"content-length").into(),
                Bytes::from_static(b"9").into(),
            ),
            (
                Bytes::from_static(b"date").into(),
                Bytes::from_static(b"Fri, 17 Apr 2026 12:00:00 GMT").into(),
            ),
        ];
        let defaults = ResponseHeaderConfig {
            server_header: Some(Bytes::from_static(b"h2corn")),
            date_header: true,
            extra_headers: Box::new([ConfiguredResponseHeader::new(
                Bytes::from_static(b"x-extra"),
                Bytes::from_static(b"two"),
            )]),
        };
        let mut scan = inspect_response_headers(&headers);

        prepare_fixed_length_response_headers_with_scan(&mut headers, &mut scan, &defaults, 5);

        // The application's own headers keep their order; anything added
        // follows, configured headers before this server's own defaults.
        let names: Vec<&[u8]> = headers.iter().map(|(name, _)| name.as_bytes()).collect();
        assert_eq!(names, [
            b"x-first".as_slice(),
            b"content-length".as_slice(),
            b"date".as_slice(),
            b"x-extra".as_slice(),
            b"server".as_slice(),
        ]);
        assert_eq!(headers[1].1.as_bytes(), b"5");
        assert_eq!(scan.content_length(), Some(5));
    }

    #[test]
    fn fixed_length_preparation_appends_defaults_then_canonical_length() {
        let mut headers = ResponseHeaders::new();
        let defaults = ResponseHeaderConfig {
            server_header: Some(Bytes::from_static(b"h2corn")),
            date_header: true,
            extra_headers: Box::new([]),
        };
        let mut scan = inspect_response_headers(&headers);

        prepare_fixed_length_response_headers_with_scan(&mut headers, &mut scan, &defaults, 0);

        let names: Vec<&[u8]> = headers.iter().map(|(name, _)| name.as_bytes()).collect();
        assert_eq!(names, [
            b"server".as_slice(),
            b"date".as_slice(),
            b"content-length".as_slice(),
        ]);
        assert_eq!(headers[2].1.as_bytes(), b"0");
    }

    #[test]
    fn fixed_length_preparation_canonicalizes_configured_content_length() {
        let mut headers = ResponseHeaders::new();
        let defaults = ResponseHeaderConfig {
            server_header: None,
            date_header: false,
            extra_headers: Box::new([
                ConfiguredResponseHeader::new(
                    Bytes::from_static(b"x-extra"),
                    Bytes::from_static(b"before"),
                ),
                ConfiguredResponseHeader::new(
                    Bytes::from_static(b"content-length"),
                    Bytes::from_static(b"999"),
                ),
            ]),
        };
        let mut scan = inspect_response_headers(&headers);

        prepare_fixed_length_response_headers_with_scan(&mut headers, &mut scan, &defaults, 5);

        assert_eq!(headers.len(), 2);
        assert_eq!(headers[0].0.as_bytes(), b"x-extra");
        assert_eq!(headers[1].0.as_bytes(), b"content-length");
        assert_eq!(headers[1].1.as_bytes(), b"5");
        assert_eq!(scan.content_length(), Some(5));
    }

    #[test]
    fn http_date_format_matches_known_rfc7231_timestamps() {
        let cases = [
            (0, "Thu, 01 Jan 1970 00:00:00 GMT"),
            (2_678_399, "Sat, 31 Jan 1970 23:59:59 GMT"),
            (784_111_777, "Sun, 06 Nov 1994 08:49:37 GMT"),
            (946_684_799, "Fri, 31 Dec 1999 23:59:59 GMT"),
            (951_825_600, "Tue, 29 Feb 2000 12:00:00 GMT"),
            (4_107_542_400, "Mon, 01 Mar 2100 00:00:00 GMT"),
        ];

        for (unix_seconds, expected) in cases {
            assert_eq!(format_http_date(unix_seconds).as_ref(), expected.as_bytes());
        }
    }

    #[test]
    fn civil_from_days_handles_leap_and_non_leap_boundaries() {
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(11_016), (2000, 2, 29));
        assert_eq!(civil_from_days(47_541), (2100, 3, 1));
    }
}
