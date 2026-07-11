use std::borrow::Cow;
use std::cell::RefCell;
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
const RESPONSE_CONTENT_LENGTH_SCANNED: u8 = 1 << 2;
const RESPONSE_HAS_CONTENT_LENGTH: u8 = 1 << 3;
const RESPONSE_CONTENT_LENGTH_NEEDS_REWRITE: u8 = 1 << 4;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ConnectionHeaderTokens {
    pub close: bool,
    pub upgrade: bool,
    pub http2_settings: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ResponseContentLength {
    Missing,
    Valid(usize),
    NeedsRewrite,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ResponseHeaderScan {
    content_length: usize,
    flags: u8,
}

impl ResponseHeaderScan {
    const fn has_server(self) -> bool {
        self.flags & RESPONSE_HAS_SERVER != 0
    }

    const fn has_date(self) -> bool {
        self.flags & RESPONSE_HAS_DATE != 0
    }

    const fn content_length_state(self) -> ResponseContentLength {
        assert!(
            self.flags & RESPONSE_CONTENT_LENGTH_SCANNED != 0,
            "response content-length must be scanned before reading",
        );
        if self.flags & RESPONSE_CONTENT_LENGTH_NEEDS_REWRITE != 0 {
            ResponseContentLength::NeedsRewrite
        } else if self.flags & RESPONSE_HAS_CONTENT_LENGTH != 0 {
            ResponseContentLength::Valid(self.content_length)
        } else {
            ResponseContentLength::Missing
        }
    }

    pub(crate) const fn content_length(self) -> Option<usize> {
        match self.content_length_state() {
            ResponseContentLength::Valid(len) => Some(len),
            ResponseContentLength::Missing | ResponseContentLength::NeedsRewrite => None,
        }
    }

    fn observe_content_length(&mut self, value: &[u8]) {
        if self.flags & (RESPONSE_HAS_CONTENT_LENGTH | RESPONSE_CONTENT_LENGTH_NEEDS_REWRITE) != 0 {
            self.flags |= RESPONSE_CONTENT_LENGTH_NEEDS_REWRITE;
            return;
        }
        if let Some(len) =
            parse_content_length_header(value).and_then(|len| usize::try_from(len).ok())
        {
            self.content_length = len;
            self.flags |= RESPONSE_HAS_CONTENT_LENGTH;
        } else {
            self.flags |= RESPONSE_CONTENT_LENGTH_NEEDS_REWRITE;
        }
    }

    const fn finish_content_length_scan(&mut self) {
        self.flags |= RESPONSE_CONTENT_LENGTH_SCANNED;
    }

    const fn set_content_length(&mut self, len: usize) {
        self.content_length = len;
        self.flags |= RESPONSE_CONTENT_LENGTH_SCANNED | RESPONSE_HAS_CONTENT_LENGTH;
        self.flags &= !RESPONSE_CONTENT_LENGTH_NEEDS_REWRITE;
    }
}

#[derive(Debug)]
pub(crate) struct ForwardedView<'a> {
    pub(crate) client_host: Option<&'a str>,
    pub(crate) proto: Option<&'a str>,
    pub(crate) host: Option<(&'a str, Option<u16>)>,
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
        tokens.close |= current.eq_ignore_ascii_case(b"close");
        tokens.upgrade |= current.eq_ignore_ascii_case(b"upgrade");
        tokens.http2_settings |= current.eq_ignore_ascii_case(b"http2-settings");
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
    parse_pos::<u64, false>(value.trim_ascii()).ok()
}

pub(crate) fn inspect_response_default_headers(headers: &ResponseHeaders) -> ResponseHeaderScan {
    let mut scan = ResponseHeaderScan::default();

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
    let mut scan = ResponseHeaderScan::default();

    for (name, value) in headers {
        match name.kind() {
            ResponseHeaderKind::Server => scan.flags |= RESPONSE_HAS_SERVER,
            ResponseHeaderKind::Date => scan.flags |= RESPONSE_HAS_DATE,
            ResponseHeaderKind::ContentLength => scan.observe_content_length(value.as_bytes()),
            _ => {},
        }
    }
    scan.finish_content_length_scan();
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
        usize::from(defaults.server_header && !scan.has_server())
            + usize::from(defaults.date_header && !scan.has_date())
            + defaults.extra_headers.len(),
    );
    append_default_response_headers(headers, scan, defaults);
}

fn append_default_response_headers(
    headers: &mut ResponseHeaders,
    scan: &mut ResponseHeaderScan,
    defaults: &ResponseHeaderConfig,
) {
    if defaults.server_header && !scan.has_server() {
        headers.push((
            Bytes::from_static(b"server").into(),
            Bytes::from_static(b"h2corn").into(),
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
    for header in &defaults.extra_headers {
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
}

pub(crate) fn prepare_fixed_length_response_headers_with_scan(
    headers: &mut ResponseHeaders,
    scan: &mut ResponseHeaderScan,
    defaults: &ResponseHeaderConfig,
    len: usize,
) {
    let additional = usize::from(defaults.server_header && !scan.has_server())
        + usize::from(defaults.date_header && !scan.has_date())
        + defaults.extra_headers.len()
        + usize::from(scan.content_length_state() == ResponseContentLength::Missing);
    headers.reserve(additional);
    append_default_response_headers(headers, scan, defaults);
    canonicalize_fixed_length_response_headers_with_scan(headers, scan, len);
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
        let normalized = parse_host_port(host).map_or(host, |(host, _)| host);
        furthest_host = Some(normalized);
        if !trusted_host_matches(&config.proxy.trusted_peers, normalized, false) {
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
        if host.is_empty() {
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
        if host.is_empty() {
            return None;
        }
        return parse_pos::<u16, false>(port.as_bytes())
            .ok()
            .map(|port| (host, Some(port)));
    }

    (!value.is_empty()).then_some((value, None))
}

fn normalize_forwarded_value(value: &str) -> &str {
    value.trim_ascii().trim_matches('"')
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::{
        ResponseContentLength, civil_from_days, format_http_date, inspect_response_headers,
        last_csv_token, parse_content_length_header, parse_host_port,
        prepare_fixed_length_response_headers_with_scan,
    };
    use crate::config::{ConfiguredResponseHeader, ResponseHeaderConfig};
    use crate::http::header_value::header_value_is_valid;
    use crate::http::types::ResponseHeaders;

    #[test]
    fn content_length_parser_accepts_trimmed_ascii_digits() {
        assert_eq!(parse_content_length_header(b"42"), Some(42));
        assert_eq!(parse_content_length_header(b" 42\t"), Some(42));
    }

    #[test]
    fn content_length_parser_rejects_invalid_values() {
        assert_eq!(parse_content_length_header(b""), None);
        assert_eq!(parse_content_length_header(b"4x"), None);
        assert_eq!(parse_content_length_header(b"\xff"), None);
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
        assert_eq!(scan.content_length(), Some(42));
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
        assert_eq!(scan.content_length(), None);
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
        assert_eq!(scan.content_length(), None);
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
            server_header: true,
            date_header: true,
            extra_headers: Box::new([ConfiguredResponseHeader::new(
                Bytes::from_static(b"x-extra"),
                Bytes::from_static(b"two"),
            )]),
        };
        let mut scan = inspect_response_headers(&headers);

        prepare_fixed_length_response_headers_with_scan(&mut headers, &mut scan, &defaults, 5);

        let names: Vec<&[u8]> = headers.iter().map(|(name, _)| name.as_bytes()).collect();
        assert_eq!(names, [
            b"x-first".as_slice(),
            b"content-length".as_slice(),
            b"date".as_slice(),
            b"server".as_slice(),
            b"x-extra".as_slice(),
        ]);
        assert_eq!(headers[1].1.as_bytes(), b"5");
        assert_eq!(scan.content_length(), Some(5));
    }

    #[test]
    fn fixed_length_preparation_appends_defaults_then_canonical_length() {
        let mut headers = ResponseHeaders::new();
        let defaults = ResponseHeaderConfig {
            server_header: true,
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
            server_header: false,
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
