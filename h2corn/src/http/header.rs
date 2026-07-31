use std::borrow::Cow;
use std::cell::RefCell;
use std::ops::BitOrAssign;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use std::{iter, str};

use atoi_simd::parse_pos;
use bytes::Bytes;
use itoa::Buffer as ItoaBuffer;
use memchr::{memchr, memchr3};

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
const RESPONSE_HAS_SERVER: u8 = 1 << 0;
const RESPONSE_HAS_DATE: u8 = 1 << 1;

/// `server`, `date`, and `content-length` are the only built-in response
/// fields that default preparation can append. The exact Python-list ingress
/// reserves these slots before it has selected a response framing path.
pub(crate) const RESPONSE_DEFAULT_BUILTIN_SLOTS: usize = 3;

static RESPONSE_HEADER_STRIP_COUNTS: [[AtomicU64; 5]; 2] = [
    [
        AtomicU64::new(0),
        AtomicU64::new(0),
        AtomicU64::new(0),
        AtomicU64::new(0),
        AtomicU64::new(0),
    ],
    [
        AtomicU64::new(0),
        AtomicU64::new(0),
        AtomicU64::new(0),
        AtomicU64::new(0),
        AtomicU64::new(0),
    ],
];

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

/// Fixed application fields stripped at ASGI ingress. `transfer-encoding` is
/// intentionally absent: ASGI requires that field to be ignored silently.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ResponseHeaderStrips(u8);

impl ResponseHeaderStrips {
    const CONNECTION: u8 = 1 << 0;
    const KEEP_ALIVE: u8 = 1 << 1;
    const PROXY_CONNECTION: u8 = 1 << 2;
    const TE: u8 = 1 << 3;
    const UPGRADE: u8 = 1 << 4;

    pub(crate) const fn insert(&mut self, field: ApplicationResponseField) {
        self.0 |= match field {
            ApplicationResponseField::Connection => Self::CONNECTION,
            ApplicationResponseField::KeepAlive => Self::KEEP_ALIVE,
            ApplicationResponseField::ProxyConnection => Self::PROXY_CONNECTION,
            ApplicationResponseField::Te => Self::TE,
            ApplicationResponseField::Upgrade => Self::UPGRADE,
            ApplicationResponseField::TransferEncoding | ApplicationResponseField::Other => 0,
        };
    }

    pub(crate) const fn is_empty(self) -> bool {
        self.0 == 0
    }
}

/// Response-start facts retained after transport-owned fields are removed.
/// `Connection` never travels as an application header: HTTP/1 consumes these
/// facts to choose connection lifetime/Upgrade syntax and HTTP/2 ignores them.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum ResponseConnectionDirective {
    #[default]
    None,
    Close,
    Upgrade,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ResponseHeaderControl {
    pub(crate) directive: ResponseConnectionDirective,
    pub(crate) strips: ResponseHeaderStrips,
}

/// The `Connection` tokens h2corn acts on.
///
/// A request may repeat the header, so these accumulate across occurrences:
/// `Connection: close` followed by `Connection: upgrade` means both.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ConnectionHeaderTokens {
    pub(crate) close: bool,
    pub(crate) upgrade: bool,
    pub(crate) http2_settings: bool,
}

impl BitOrAssign for ConnectionHeaderTokens {
    fn bitor_assign(&mut self, other: Self) {
        self.close |= other.close;
        self.upgrade |= other.upgrade;
        self.http2_settings |= other.http2_settings;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ResponseContentLength {
    Missing,
    Valid(usize),
    NeedsRewrite,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ResponseHeaderScan {
    content_length: ResponseContentLength,
    flags: u8,
}

#[derive(Debug)]
pub(crate) struct ForwardedView<'a> {
    pub(crate) client_host: Option<&'a str>,
    pub(crate) proto: Option<&'a str>,
    pub(crate) host: Option<(&'a str, Option<u16>)>,
}

impl ResponseHeaderScan {
    const fn new() -> Self {
        Self {
            content_length: ResponseContentLength::Missing,
            flags: 0,
        }
    }

    const fn has_server(self) -> bool {
        self.flags & RESPONSE_HAS_SERVER != 0
    }

    const fn has_date(self) -> bool {
        self.flags & RESPONSE_HAS_DATE != 0
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

/// Bounded per-protocol/per-fixed-field observability for policy stripping.
/// The first event is diagnostic; subsequent events only increment one of ten
/// atomics. Values and application-controlled names never reach the log.
pub(crate) fn observe_response_header_strips(http2: bool, strips: ResponseHeaderStrips) {
    if strips.is_empty() {
        return;
    }
    let protocol = usize::from(http2);
    for (bit, name) in [
        (ResponseHeaderStrips::CONNECTION, "connection"),
        (ResponseHeaderStrips::KEEP_ALIVE, "keep-alive"),
        (ResponseHeaderStrips::PROXY_CONNECTION, "proxy-connection"),
        (ResponseHeaderStrips::TE, "te"),
        (ResponseHeaderStrips::UPGRADE, "upgrade"),
    ] {
        if strips.0 & bit == 0 {
            continue;
        }
        let index = bit.trailing_zeros() as usize;
        if RESPONSE_HEADER_STRIP_COUNTS[protocol][index].fetch_add(1, Ordering::Relaxed) == 0 {
            let protocol = if http2 { "HTTP/2" } else { "HTTP/1.1" };
            eprintln!(
                "stripped the application's {name} response header: hop-by-hop fields are owned by the {protocol} transport (reported once)"
            );
        }
    }
}

pub(crate) fn last_csv_token(value: &str) -> &str {
    let bytes = value.as_bytes();
    let mut in_quotes = false;
    let mut escaped = false;
    let mut last_delimiter = None;
    let mut index = 0;

    while index < bytes.len() {
        if escaped {
            escaped = false;
            index += 1;
            continue;
        }
        let Some(offset) = memchr3(b'\\', b'"', b',', &bytes[index..]) else {
            break;
        };
        let current = index + offset;
        match bytes[current] {
            b'\\' if in_quotes => escaped = true,
            b'"' => in_quotes = !in_quotes,
            b',' if !in_quotes => last_delimiter = Some(current),
            _ => {},
        }
        index = current + 1;
    }

    last_delimiter.map_or_else(
        || value.trim_ascii(),
        |index| value[index + 1..].trim_ascii(),
    )
}

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
            tokens.close = true;
        }
        if current.eq_ignore_ascii_case(b"upgrade") {
            tokens.upgrade = true;
        }
        if current.eq_ignore_ascii_case(b"http2-settings") {
            tokens.http2_settings = true;
        }
        if tokens.close && tokens.upgrade && tokens.http2_settings {
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

pub(crate) fn inspect_response_default_headers(headers: &ResponseHeaders) -> ResponseHeaderScan {
    let mut scan = ResponseHeaderScan::new();

    for (name, _) in headers {
        match name.kind() {
            ResponseHeaderKind::Server => scan.flags |= RESPONSE_HAS_SERVER,
            ResponseHeaderKind::Date => scan.flags |= RESPONSE_HAS_DATE,
            _ => {},
        }
    }

    scan
}

pub(crate) fn inspect_response_headers(headers: &ResponseHeaders) -> ResponseHeaderScan {
    let mut scan = ResponseHeaderScan::new();

    for (name, value) in headers {
        match name.kind() {
            ResponseHeaderKind::Server => scan.flags |= RESPONSE_HAS_SERVER,
            ResponseHeaderKind::Date => scan.flags |= RESPONSE_HAS_DATE,
            ResponseHeaderKind::ContentLength => scan.observe_content_length(value.as_bytes()),
            _ => {},
        }
    }
    scan
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

fn cached_date_value() -> Bytes {
    // Per-thread cache: each worker thread keeps its own `(unix_seconds, value)` so
    // every response is a refcount bump on a thread-owned `Bytes`. The previous
    // shared `RwLock` had readers contending on the inner counter across all
    // workers; this version has zero cross-thread atomics on the fast path.
    thread_local! {
        static CACHE: RefCell<(u64, Bytes)> = const { RefCell::new((0, Bytes::new())) };
    }
    let unix_seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
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
            ResponseHeaderKind::Server => scan.flags |= RESPONSE_HAS_SERVER,
            ResponseHeaderKind::Date => scan.flags |= RESPONSE_HAS_DATE,
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
        scan.flags |= RESPONSE_HAS_SERVER;
    }
    if defaults.date_header && !scan.has_date() {
        headers.push((
            Bytes::from_static(b"date").into(),
            cached_date_value().into(),
        ));
        scan.flags |= RESPONSE_HAS_DATE;
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

pub(crate) fn protocol_is_websocket(protocol: &Protocol) -> bool {
    protocol.as_str() == "websocket"
}

pub(crate) fn header_value_text(value: &[u8]) -> Option<&str> {
    let value = str::from_utf8(value).ok()?.trim_ascii();
    (!value.is_empty()).then_some(value)
}

pub(crate) fn parse_forwarded_value(value: &str) -> Option<ForwardedView<'_>> {
    let value = last_csv_token(value);
    let mut client_host = None;
    let mut proto = None;
    let mut host = None;

    for part in value.split(';') {
        let (name, value) = part.split_once('=')?;
        let value = normalize_forwarded_value(value);
        let name = name.trim_ascii();
        if name.eq_ignore_ascii_case("for") {
            if let Some((host_value, _)) = parse_host_port(value) {
                client_host = Some(host_value);
            }
        } else if name.eq_ignore_ascii_case("proto") {
            proto = Some(value);
        } else if name.eq_ignore_ascii_case("host") {
            host = parse_host_port(value);
        }
    }

    Some(ForwardedView {
        client_host,
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
        inspect_response_headers, last_csv_token, parse_content_length_header, parse_host_port,
        parse_x_forwarded_for_value, prepare_fixed_length_response_headers_with_scan,
        request_authority_is_valid, request_path_is_valid, request_scheme_is_valid,
        trailer_field_name_is_forbidden,
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
