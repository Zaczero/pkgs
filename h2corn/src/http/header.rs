use atoi_simd::parse_pos;
use bytes::Bytes;
use std::{
    borrow::Cow,
    iter, str,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::config::{CachedDateValue, ServerConfig};
use crate::ext::Protocol;
use crate::http::types::{RequestHeaderValue, ResponseHeaders};
use crate::proxy::trusted_host_matches;

const HEADER_NAME_VALID: u8 = 1;
const HEADER_NAME_UPPER: u8 = 2;
const SECONDS_PER_DAY: u64 = 86_400;
const HTTP_DATE_TEMPLATE: [u8; 29] = *b"Thu, 01 Jan 1970 00:00:00 GMT";
const WEEKDAYS: [[u8; 3]; 7] = [
    *b"Sun", *b"Mon", *b"Tue", *b"Wed", *b"Thu", *b"Fri", *b"Sat",
];
const MONTHS: [[u8; 3]; 12] = [
    *b"Jan", *b"Feb", *b"Mar", *b"Apr", *b"May", *b"Jun", *b"Jul", *b"Aug", *b"Sep", *b"Oct",
    *b"Nov", *b"Dec",
];
const DIGIT_PAIRS: [u8; 200] = digit_pairs();
const HEADER_NAME_TABLE: [u8; 256] = {
    let mut table = [0; 256];

    let mut byte = b'0';
    while byte <= b'9' {
        table[byte as usize] = HEADER_NAME_VALID;
        byte += 1;
    }

    let mut byte = b'a';
    while byte <= b'z' {
        table[byte as usize] = HEADER_NAME_VALID;
        byte += 1;
    }

    let mut byte = b'A';
    while byte <= b'Z' {
        table[byte as usize] = HEADER_NAME_VALID | HEADER_NAME_UPPER;
        byte += 1;
    }

    let bytes = b"!#$%&'*+-.^_`|~";
    let mut index = 0;
    while index < bytes.len() {
        table[bytes[index] as usize] = HEADER_NAME_VALID;
        index += 1;
    }

    table
};

const fn digit_pairs() -> [u8; 200] {
    let mut out = [0; 200];
    let mut value = 0;
    while value < 100 {
        out[value * 2] = b'0' + (value / 10) as u8;
        out[value * 2 + 1] = b'0' + (value % 10) as u8;
        value += 1;
    }
    out
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ConnectionHeaderTokens {
    pub close: bool,
    pub upgrade: bool,
    pub http2_settings: bool,
}

pub(crate) fn first_csv_token(value: &str) -> &str {
    let bytes = value.as_bytes();
    let end = find_byte(bytes, b',').unwrap_or(bytes.len());
    value[..end].trim_ascii()
}

pub(crate) fn last_csv_token(value: &str) -> &str {
    value.rsplit(',').next().map_or("", str::trim_ascii)
}

pub(crate) fn split_commas_bytes(value: &[u8]) -> impl Iterator<Item = &[u8]> {
    let mut rest = Some(value);
    iter::from_fn(move || {
        let current = rest?;
        if let Some(index) = find_byte(current, b',') {
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

    for current in split_commas_bytes(value).map(|current| current.trim_ascii()) {
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

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum ResponseContentLength {
    #[default]
    Missing,
    Valid(usize),
    NeedsRewrite,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ResponseHeaderScan {
    pub has_server: bool,
    pub has_date: bool,
    content_length: ResponseContentLength,
}

impl ResponseHeaderScan {
    pub(crate) fn content_length(self) -> Option<usize> {
        match self.content_length {
            ResponseContentLength::Valid(len) => Some(len),
            ResponseContentLength::Missing | ResponseContentLength::NeedsRewrite => None,
        }
    }
}

pub(crate) fn inspect_response_headers(headers: &ResponseHeaders) -> ResponseHeaderScan {
    let mut scan = ResponseHeaderScan::default();

    for (name, value) in headers {
        match name.as_bytes() {
            b"server" => scan.has_server = true,
            b"date" => scan.has_date = true,
            b"content-length" => {
                scan.content_length = match scan.content_length {
                    ResponseContentLength::Missing => parse_content_length_header(value.as_bytes())
                        .and_then(|len| usize::try_from(len).ok())
                        .map_or(
                            ResponseContentLength::NeedsRewrite,
                            ResponseContentLength::Valid,
                        ),
                    ResponseContentLength::Valid(_) | ResponseContentLength::NeedsRewrite => {
                        ResponseContentLength::NeedsRewrite
                    }
                };
            }
            _ => {}
        }
    }

    scan
}

pub(crate) fn canonicalize_fixed_length_response_headers_with_scan(
    headers: &mut ResponseHeaders,
    scan: &mut ResponseHeaderScan,
    len: usize,
) {
    if scan.content_length == ResponseContentLength::Valid(len) {
        return;
    }

    let mut seen = false;
    let len_value = Bytes::from(len.to_string());
    headers.retain_mut(|(name, value)| {
        if name.as_bytes() != b"content-length" {
            return true;
        }
        if seen {
            return false;
        }
        *value = Bytes::clone(&len_value).into();
        seen = true;
        true
    });
    if !seen {
        headers.push((
            Bytes::from_static(b"content-length").into(),
            len_value.into(),
        ));
    }
    scan.content_length = ResponseContentLength::Valid(len);
}

fn cached_date_value(config: &ServerConfig) -> Bytes {
    let unix_seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    {
        let cached = config.response_headers.cached_date.read();
        if cached.unix_seconds == unix_seconds && !cached.value.is_empty() {
            return Bytes::clone(&cached.value);
        }
    }
    let value = format_http_date(unix_seconds);
    let mut cached = config.response_headers.cached_date.write();
    *cached = CachedDateValue {
        unix_seconds,
        value: Bytes::clone(&value),
    };
    value
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
    copy_digit_pair(&mut out[5..7], day as usize);
    out[8..11].copy_from_slice(&MONTHS[(month - 1) as usize]);
    copy_digit_pair(&mut out[12..14], (year / 100) as usize);
    copy_digit_pair(&mut out[14..16], (year % 100) as usize);
    copy_digit_pair(&mut out[17..19], hour);
    copy_digit_pair(&mut out[20..22], minute);
    copy_digit_pair(&mut out[23..25], second);
}

fn copy_digit_pair(dst: &mut [u8], value: usize) {
    debug_assert!(value < 100);
    let offset = value * 2;
    dst.copy_from_slice(&DIGIT_PAIRS[offset..offset + 2]);
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
        usize::from(defaults.server_header && !scan.has_server)
            + usize::from(defaults.date_header && !scan.has_date)
            + defaults.extra_headers.len(),
    );
    if defaults.server_header && !scan.has_server {
        headers.push((
            Bytes::from_static(b"server").into(),
            Bytes::from_static(b"h2corn").into(),
        ));
        scan.has_server = true;
    }
    if defaults.date_header && !scan.has_date {
        headers.push((
            Bytes::from_static(b"date").into(),
            cached_date_value(config).into(),
        ));
        scan.has_date = true;
    }
    for (name, value) in defaults.extra_headers.iter() {
        headers.push((Bytes::clone(name).into(), Bytes::clone(value).into()));
    }
}

pub(crate) fn apply_default_response_headers(headers: &mut ResponseHeaders, config: &ServerConfig) {
    let mut scan = inspect_response_headers(headers);
    apply_default_response_headers_with_scan(headers, &mut scan, config);
}

pub(crate) fn request_header_name_needs_lowercase(name: &[u8]) -> Option<bool> {
    if name.is_empty() {
        return None;
    }

    let mut needs_lowercase = false;
    for byte in name {
        let flags = HEADER_NAME_TABLE[usize::from(*byte)];
        if flags & HEADER_NAME_VALID == 0 {
            return None;
        }
        needs_lowercase |= flags & HEADER_NAME_UPPER != 0;
    }
    Some(needs_lowercase)
}

pub(crate) fn header_value_is_valid(value: &[u8]) -> bool {
    value
        .iter()
        .all(|byte| *byte == b'\t' || (*byte >= 32 && *byte != 127))
}

pub(crate) fn response_header_name_is_valid(name: &[u8]) -> bool {
    !name.is_empty()
        && name.iter().all(|byte| {
            let flags = HEADER_NAME_TABLE[usize::from(*byte)];
            flags & HEADER_NAME_VALID != 0 && flags & HEADER_NAME_UPPER == 0
        })
}

pub(crate) fn protocol_is_websocket(protocol: &Protocol) -> bool {
    protocol.as_str() == "websocket"
}

#[derive(Debug)]
pub(crate) struct ForwardedView<'a> {
    pub(crate) client_host: Option<&'a str>,
    pub(crate) proto: Option<&'a str>,
    pub(crate) host: Option<(&'a str, Option<u16>)>,
}

pub(crate) fn header_value_text(value: &RequestHeaderValue) -> Option<&str> {
    let value = str::from_utf8(value.as_bytes()).ok()?.trim_ascii();
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
        && let Ok(port) = parse_pos::<u16, false>(port.as_bytes())
    {
        return Some((host, Some(port)));
    }

    Some((value, None))
}

fn normalize_forwarded_value(value: &str) -> &str {
    value.trim_ascii().trim_matches('"')
}

fn find_byte(value: &[u8], needle: u8) -> Option<usize> {
    value.iter().position(|&byte| byte == needle)
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::{
        ResponseContentLength, civil_from_days, format_http_date, inspect_response_headers,
        parse_content_length_header,
    };
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

        assert!(scan.has_server);
        assert!(scan.has_date);
        assert_eq!(scan.content_length, ResponseContentLength::Valid(42));
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

        assert_eq!(scan.content_length, ResponseContentLength::NeedsRewrite);
        assert_eq!(scan.content_length(), None);
    }

    #[test]
    fn response_header_scan_marks_invalid_content_length_for_rewrite() {
        let headers: ResponseHeaders = vec![(
            Bytes::from_static(b"content-length").into(),
            Bytes::from_static(b"oops").into(),
        )];

        let scan = inspect_response_headers(&headers);

        assert_eq!(scan.content_length, ResponseContentLength::NeedsRewrite);
        assert_eq!(scan.content_length(), None);
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
