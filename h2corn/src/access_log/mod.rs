mod sink;

use std::fmt::{self, Write as _};
use std::io::{self, Write};
use std::str;
use std::sync::LazyLock;
use std::time::{Duration, Instant};

use anstream::{AutoStream, ColorChoice};
use http::Method;
use itoa::{Buffer as ItoaBuffer, Integer};
use owo_colors::{OwoColorize as _, Style};
use smallvec::SmallVec;

use crate::config::{BindTarget, ServerConfig};
use crate::http::response::{FinalResponseBody, ResponseAction};
use crate::http::scope::{ScopeHost, resolve_scope_view};
use crate::http::types::{BytesStr, HttpStatusCode, HttpVersion, RequestHead, status_code};
use crate::proxy_protocol::ConnectionInfo;
use crate::runtime::RequestContext;
use crate::websocket::{WebSocketCloseCode, close_code};

const MAX_IPV4_CLIENT: &str = "255.255.255.255:65535";
const MAX_IPV6_CLIENT: &str = "[ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff]:65535";
const ACCESS_LOG_LINE_CAPACITY: usize = 128;
const IPV4_CLIENT_WIDTH: usize = MAX_IPV4_CLIENT.len() - 2;
const DECIMAL_FACTORS: [u64; 3] = power_table(10);
const BYTE_SCALES: [u64; 5] = power_table(1024);
const BYTE_UNITS: [&str; 5] = ["b", "kib", "mib", "gib", "tib"];
static ACCESS_LOG_MODE: LazyLock<AccessLogMode> = LazyLock::new(|| {
    if AutoStream::choice(&io::stderr()) == ColorChoice::Never {
        AccessLogMode::Plain
    } else {
        AccessLogMode::Styled
    }
});

/// How long graceful shutdown waits for queued access-log lines to be written.
const LOG_FLUSH_TIMEOUT: Duration = Duration::from_millis(500);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AccessLogMode {
    Plain,
    Styled,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RequestSummaryKind {
    Http,
    WebSocket,
}

#[derive(Clone, Debug)]
pub(crate) struct AccessLogRequest {
    method: Method,
    path_and_query: BytesStr,
    http_version: HttpVersion,
}

impl AccessLogRequest {
    pub(crate) fn from_request(request: &RequestHead) -> Self {
        Self {
            method: request.method.clone(),
            path_and_query: request.log_target().clone(),
            http_version: request.http_version,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct HttpAccessLogEntry<'a> {
    pub request: &'a AccessLogRequest,
    pub client_label: &'a str,
    pub status: HttpStatusCode,
    pub duration: Duration,
    pub rx_bytes: u64,
    pub tx_bytes: u64,
}

#[derive(Debug)]
pub(crate) struct WebSocketAccessLogEntry<'a> {
    pub request: &'a AccessLogRequest,
    pub client_label: &'a str,
    pub close_code: WebSocketCloseCode,
    pub duration: Duration,
    pub rx_bytes: u64,
    pub tx_bytes: u64,
}

#[derive(Clone, Copy, Debug)]
struct AccessLogIoSummary {
    duration: Duration,
    rx_bytes: u64,
    tx_bytes: u64,
}

struct RequestSummaryDisplay<'a> {
    request: &'a AccessLogRequest,
    summary_kind: RequestSummaryKind,
}

impl fmt::Display for RequestSummaryDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_request_summary_to(f, self.request, self.summary_kind)
    }
}

struct IoSummaryDisplay(AccessLogIoSummary);

impl fmt::Display for IoSummaryDisplay {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_io_summary_to(f, self.0.duration, self.0.rx_bytes, self.0.tx_bytes)
    }
}

type AccessLogBuf = SmallVec<[u8; ACCESS_LOG_LINE_CAPACITY]>;
type ClientLabelBuf = SmallVec<[u8; MAX_IPV6_CLIENT.len()]>;

struct BytesWriter<'a, const N: usize>(&'a mut SmallVec<[u8; N]>);

impl<const N: usize> fmt::Write for BytesWriter<'_, N> {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        self.0.extend_from_slice(value.as_bytes());
        Ok(())
    }
}

/// Client-controlled text, rendered so no byte of it can act on whoever reads
/// the log.
///
/// The HTTP/1 request-target grammar excludes space, CR and LF — so a client
/// cannot forge a second log entry — but not the other control bytes, so `ESC`
/// reaches this far and would otherwise drive an operator's terminal. Escaping
/// it here, rather than scrubbing at the write end, keeps the entry faithful:
/// `/\x1b[31m` is what was requested, where stripping it left `/` and said
/// nothing about the difference.
struct Escaped<'a>(&'a str);

impl fmt::Display for Escaped<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut rest = self.0;
        while let Some(offset) = rest.find(char::is_control) {
            let (printable, tail) = rest.split_at(offset);
            f.write_str(printable)?;
            let mut chars = tail.chars();
            let control = chars.next().ok_or(fmt::Error)?;
            // `\xNN` names one byte, so it may only stand for a character that
            // *is* one byte; C1 controls are two and get their code point.
            if control.is_ascii() {
                write!(f, "\\x{:02x}", u32::from(control))?;
            } else {
                write!(f, "\\u{{{:02x}}}", u32::from(control))?;
            }
            rest = chars.as_str();
        }
        f.write_str(rest)
    }
}

impl AsRef<str> for Escaped<'_> {
    fn as_ref(&self) -> &str {
        self.0
    }
}

struct HostDisplay<T>(T);

impl<T> fmt::Display for HostDisplay<T>
where
    T: AsRef<str> + fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0.as_ref().contains(':') && !self.0.as_ref().starts_with('[') {
            write!(f, "[{}]", self.0)
        } else {
            write!(f, "{}", self.0)
        }
    }
}

struct ClientLabel(ClientLabelBuf);

impl ClientLabel {
    fn build(ctx: &RequestContext) -> Self {
        // Formatting this costs a scope-view resolution and a `core::fmt` pass
        // over `IpAddr::Display`, per request. The socket peer does not change
        // for the life of the connection, so the label is built once -- but
        // only when trusted proxy headers cannot name a different client per
        // request, which is both the default and the benchmarked shape.
        if ctx.connection.info.proxy_headers_trusted {
            return Self(Self::format(ctx));
        }
        let cached = ctx
            .connection
            .client_label(|| Box::from(Self::format_str(ctx)));
        Self(ClientLabelBuf::from_slice(cached.as_bytes()))
    }

    fn format(ctx: &RequestContext) -> ClientLabelBuf {
        let mut label = ClientLabelBuf::new();
        append_log_client(&mut label, ctx);
        if label.first() != Some(&b'[') {
            label.resize(IPV4_CLIENT_WIDTH.max(label.len()), b' ');
        }
        label
    }

    fn format_str(ctx: &RequestContext) -> String {
        let label = Self::format(ctx);
        // SAFETY: `format` writes only valid UTF-8 plus ASCII padding.
        unsafe { str::from_utf8_unchecked(label.as_slice()) }.to_owned()
    }

    fn as_str(&self) -> &str {
        // SAFETY: `ClientLabel::build` only writes valid UTF-8 strings or
        // ASCII punctuation and padding. `Escaped` also emits valid UTF-8.
        unsafe { str::from_utf8_unchecked(self.0.as_slice()) }
    }
}

struct ActiveAccessLog {
    request: AccessLogRequest,
    client_label: ClientLabel,
    started_at: Instant,
}

impl ActiveAccessLog {
    fn new(ctx: &RequestContext) -> Self {
        Self {
            request: AccessLogRequest::from_request(&ctx.request),
            client_label: ClientLabel::build(ctx),
            started_at: Instant::now(),
        }
    }
}

pub(crate) struct HttpAccessLogState(Option<ActiveAccessLog>);

impl HttpAccessLogState {
    pub(crate) fn new(ctx: &RequestContext) -> Self {
        let connection = &ctx.connection;
        Self(
            connection
                .config
                .access_log
                .then(|| ActiveAccessLog::new(ctx)),
        )
    }

    pub(crate) fn emit_http_response(
        &self,
        log_state: ResponseLogState,
        read_body_bytes: impl FnOnce() -> u64,
    ) {
        if let (Some(state), Some(status)) = (&self.0, log_state.status) {
            emit_http_access_log(&HttpAccessLogEntry {
                request: &state.request,
                client_label: state.client_label.as_str(),
                status,
                duration: state.started_at.elapsed(),
                rx_bytes: read_body_bytes(),
                tx_bytes: log_state.response_body_bytes,
            });
        }
    }
}

pub(crate) struct WebSocketAccessLogState(Option<ActiveAccessLog>);

impl WebSocketAccessLogState {
    pub(crate) fn new(ctx: &RequestContext) -> Self {
        let connection = &ctx.connection;
        Self(
            connection
                .config
                .access_log
                .then(|| ActiveAccessLog::new(ctx)),
        )
    }

    pub(crate) fn emit_http_response(&self, status: HttpStatusCode, tx_bytes: u64) {
        if let Some(state) = &self.0 {
            emit_http_access_log(&HttpAccessLogEntry {
                request: &state.request,
                client_label: state.client_label.as_str(),
                status,
                duration: state.started_at.elapsed(),
                rx_bytes: 0,
                tx_bytes,
            });
        }
    }

    pub(crate) fn emit_session(
        &self,
        close_code: WebSocketCloseCode,
        duration: Duration,
        rx_bytes: u64,
        tx_bytes: u64,
    ) {
        if let Some(state) = &self.0 {
            emit_websocket_access_log(&WebSocketAccessLogEntry {
                request: &state.request,
                client_label: state.client_label.as_str(),
                close_code,
                duration,
                rx_bytes,
                tx_bytes,
            });
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ResponseLogState {
    pub(crate) status: Option<HttpStatusCode>,
    pub(crate) response_body_bytes: u64,
}

impl ResponseLogState {
    pub(crate) const fn started(&mut self, status: HttpStatusCode) {
        self.status = Some(status);
    }

    pub(crate) const fn sent_body(&mut self, len: usize) {
        self.response_body_bytes = self.response_body_bytes.saturating_add(len as u64);
    }

    pub(crate) const fn internal_error(&mut self) {
        self.started(status_code::INTERNAL_SERVER_ERROR);
    }

    /// Record the logical response this action represents before the transport
    /// lowers it. Counts accepted response bytes, not kernel write completion.
    pub(crate) fn observe(&mut self, action: &ResponseAction) {
        match action {
            ResponseAction::Final { start, body } => {
                self.started(start.status());
                if !matches!(
                    body,
                    FinalResponseBody::Empty | FinalResponseBody::Suppressed { .. }
                ) {
                    self.sent_body(body.len());
                }
            },
            ResponseAction::Start { start, .. } => self.started(start.status()),
            ResponseAction::Body(body) => self.sent_body(body.len()),
            ResponseAction::File { len, .. } => self.sent_body(*len),
            ResponseAction::InternalError => self.internal_error(),
            ResponseAction::Finish
            | ResponseAction::FinishWithTrailers(_)
            | ResponseAction::AbortIncomplete => {},
        }
    }
}

#[cfg(test)]
mod observe_tests {
    use bytes::Bytes;

    use super::ResponseLogState;
    use crate::bridge::PayloadBytes;
    use crate::http::response::{FinalResponseBody, ResponseAction, ResponseBody, ResponseStart};
    use crate::http::types::{HttpStatusCode, ResponseHeaders, ResponseTrailers, status_code};

    fn start(status: HttpStatusCode) -> ResponseStart {
        ResponseStart::new(status, ResponseHeaders::new())
    }

    #[test]
    fn observe_maps_every_action_shape() {
        let mut log = ResponseLogState::default();

        log.observe(&ResponseAction::Start {
            start: start(status_code::OK),
            expects_trailers: false,
        });
        assert_eq!(log.status, Some(status_code::OK));
        assert_eq!(log.response_body_bytes, 0);

        log.observe(&ResponseAction::Body(ResponseBody::new(
            PayloadBytes::from(Bytes::from_static(b"hi")),
        )));
        assert_eq!(log.response_body_bytes, 2);

        let mut log = ResponseLogState::default();
        log.observe(&ResponseAction::Final {
            start: start(status_code::NO_CONTENT),
            body: FinalResponseBody::Bytes(ResponseBody::new(PayloadBytes::from(
                Bytes::from_static(b"abc"),
            ))),
        });
        assert_eq!(log.status, Some(status_code::NO_CONTENT));
        assert_eq!(log.response_body_bytes, 3);

        let mut log = ResponseLogState::default();
        log.observe(&ResponseAction::Final {
            start: start(status_code::OK),
            body: FinalResponseBody::Empty,
        });
        assert_eq!(log.status, Some(status_code::OK));
        assert_eq!(log.response_body_bytes, 0);

        let mut log = ResponseLogState::default();
        log.observe(&ResponseAction::Final {
            start: start(status_code::OK),
            body: FinalResponseBody::Suppressed { len: 99 },
        });
        assert_eq!(log.status, Some(status_code::OK));
        assert_eq!(log.response_body_bytes, 0);

        let mut log = ResponseLogState::default();
        log.observe(&ResponseAction::File {
            file: Box::new(
                // Any open handle is enough: observe only reads `len`.
                std::fs::File::open("/dev/null").expect("/dev/null opens"),
            ),
            len: 7,
        });
        assert_eq!(log.response_body_bytes, 7);

        let mut log = ResponseLogState::default();
        log.observe(&ResponseAction::InternalError);
        assert_eq!(log.status, Some(status_code::INTERNAL_SERVER_ERROR));

        let mut log = ResponseLogState {
            status: Some(status_code::OK),
            response_body_bytes: 4,
        };
        log.observe(&ResponseAction::Finish);
        log.observe(&ResponseAction::FinishWithTrailers(ResponseTrailers::new()));
        log.observe(&ResponseAction::AbortIncomplete);
        assert_eq!(log.status, Some(status_code::OK));
        assert_eq!(log.response_body_bytes, 4);
    }

    #[test]
    fn observe_does_not_double_record_status_on_internal_error_after_start() {
        // Body-limit / finalize paths may still emit InternalError after a
        // response has already started; observe overwrites status but must not
        // invent body bytes.
        let mut log = ResponseLogState::default();
        log.observe(&ResponseAction::Start {
            start: start(status_code::OK),
            expects_trailers: false,
        });
        log.observe(&ResponseAction::Body(ResponseBody::new(
            PayloadBytes::from(Bytes::from_static(b"partial")),
        )));
        log.observe(&ResponseAction::InternalError);
        assert_eq!(log.status, Some(status_code::INTERNAL_SERVER_ERROR));
        assert_eq!(log.response_body_bytes, 7);
    }
}

pub(crate) fn emit_banner(config: &ServerConfig, tls: bool) {
    const LISTENING_PREFIX: &str = "Listening on ";
    const LISTENING_INDENT: &str = "             ";

    let mut stderr = anstream::stderr().lock();
    let _ = writeln!(
        stderr,
        "{} v{} • HTTP/2 ASGI",
        "h2corn".style(Style::new().bold().cyan()),
        env!("CARGO_PKG_VERSION"),
    );

    let mut binds = config
        .binds
        .iter()
        .map(|bind| format_listen_target(bind, tls));
    if let Some(first) = binds.next() {
        write_listen_target_line(&mut stderr, LISTENING_PREFIX, &first);
        for bind in binds {
            write_listen_target_line(&mut stderr, LISTENING_INDENT, &bind);
        }
    }

    if config.http1.enabled {
        let _ = writeln!(
            stderr,
            "HTTP/1 compatibility is enabled; disable with http1=False (--no-http1)",
        );
    }

    let _ = writeln!(stderr);
}

fn write_listen_target_line(stderr: &mut impl Write, prefix: &str, bind: &str) {
    let _ = writeln!(stderr, "{prefix}{}", bind.bold());
}

pub(crate) fn emit_http_access_log(entry: &HttpAccessLogEntry<'_>) {
    emit_access_log(
        entry.client_label,
        entry.request,
        RequestSummaryKind::Http,
        entry.status,
        status_style(entry.status),
        AccessLogIoSummary {
            duration: entry.duration,
            rx_bytes: entry.rx_bytes,
            tx_bytes: entry.tx_bytes,
        },
    );
}

pub(crate) fn emit_websocket_access_log(entry: &WebSocketAccessLogEntry<'_>) {
    emit_access_log(
        entry.client_label,
        entry.request,
        RequestSummaryKind::WebSocket,
        entry.close_code.get(),
        websocket_close_style(entry.close_code),
        AccessLogIoSummary {
            duration: entry.duration,
            rx_bytes: entry.rx_bytes,
            tx_bytes: entry.tx_bytes,
        },
    );
}

fn emit_access_log<T>(
    client_label: &str,
    request: &AccessLogRequest,
    summary_kind: RequestSummaryKind,
    code: T,
    code_style: Style,
    io_summary: AccessLogIoSummary,
) where
    T: fmt::Display + Copy,
{
    let mut line = AccessLogBuf::new();
    let summary = RequestSummaryDisplay {
        request,
        summary_kind,
    };
    let io_summary = IoSummaryDisplay(io_summary);
    match *ACCESS_LOG_MODE {
        AccessLogMode::Plain => {
            write_access_log_line(&mut line, client_label, &summary, &code, &io_summary);
        },
        AccessLogMode::Styled => {
            write_access_log_line(
                &mut line,
                client_label,
                &summary,
                &code.style(code_style),
                &io_summary.style(Style::new().dimmed()),
            );
        },
    }
    sink::write_line(&line);
}

fn format_listen_target(bind: &BindTarget, tls: bool) -> String {
    match bind {
        BindTarget::Tcp { host, port } => format!(
            "{}://{}:{port}",
            if tls { "https" } else { "http" },
            HostDisplay(host.as_ref())
        ),
        BindTarget::Unix { path } => format!("unix:{path}"),
        BindTarget::Fd { fd } => format!("fd://{fd}"),
    }
}

/// Start this worker's batched access-log writer.
///
/// Called from the serve path so the writer thread is created in the process
/// that will use it — never in a supervisor that later forks.
pub(crate) fn start_log_sink() {
    sink::start();
}

/// Give queued access-log lines a bounded chance to reach stderr.
pub(crate) fn flush_log_sink() {
    sink::flush(LOG_FLUSH_TIMEOUT);
}

/// Render one access-log line. Styling is applied by the caller, so both the
/// plain and the coloured form share this one layout.
fn write_access_log_line(
    line: &mut AccessLogBuf,
    client_label: &str,
    summary: &dyn fmt::Display,
    code: &dyn fmt::Display,
    io_summary: &dyn fmt::Display,
) {
    let _ = writeln!(
        BytesWriter(line),
        "{client_label} {summary} {code} {io_summary}"
    );
}

fn append_escaped_client_host(out: &mut ClientLabelBuf, host: &str) {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    let mut rest = host.as_bytes();
    while let Some(index) = rest.iter().position(u8::is_ascii_control) {
        out.extend_from_slice(&rest[..index]);
        let control = rest[index];
        out.extend_from_slice(&[
            b'\\',
            b'x',
            HEX[usize::from(control >> 4)],
            HEX[usize::from(control & 15)],
        ]);
        rest = &rest[index + 1..];
    }
    out.extend_from_slice(rest);
}

fn append_client_label(out: &mut ClientLabelBuf, host: &str, port: u16) {
    let bracketed = host.as_bytes().contains(&b':') && !host.starts_with('[');
    if bracketed {
        out.push(b'[');
    }
    append_escaped_client_host(out, host);
    if bracketed {
        out.push(b']');
    }
    if port != 0 {
        out.push(b':');
        let mut port_buffer = ItoaBuffer::new();
        out.extend_from_slice(port_buffer.format(port).as_bytes());
    }
}

fn append_connection_client_label(out: &mut ClientLabelBuf, info: &ConnectionInfo) {
    match info.client.as_ref() {
        Some(client) => {
            ScopeHost::Ip(&client.ip).with_text(|host| append_client_label(out, host, client.port));
        },
        None => out.extend_from_slice(b"unix"),
    }
}

fn append_log_client(out: &mut ClientLabelBuf, ctx: &RequestContext) {
    let connection = &ctx.connection;
    let view = resolve_scope_view(&ctx.request, &connection.config, &connection.info);
    if let Some((host, port)) = view.client {
        host.with_text(|host| append_client_label(out, host, port));
    } else {
        append_connection_client_label(out, &connection.info);
    }
}

fn write_request_summary_to(
    out: &mut impl fmt::Write,
    request: &AccessLogRequest,
    summary_kind: RequestSummaryKind,
) -> fmt::Result {
    out.write_char('"')?;
    match summary_kind {
        RequestSummaryKind::Http => {
            out.write_str(request.method.as_str())?;
            out.write_char(' ')?;
        },
        RequestSummaryKind::WebSocket => {
            out.write_str("WEBSOCKET ")?;
        },
    }
    write!(out, "{}", Escaped(request.path_and_query.as_str()))?;
    out.write_char(' ')?;
    out.write_str(request.http_version.log_label())?;
    out.write_char('"')
}

fn write_io_summary_to(
    out: &mut impl fmt::Write,
    duration: Duration,
    rx_bytes: u64,
    tx_bytes: u64,
) -> fmt::Result {
    out.write_char(' ')?;
    write_duration_to(out, duration)?;
    write_nonzero_byte_field(out, "rx", rx_bytes)?;
    write_nonzero_byte_field(out, "tx", tx_bytes)
}

fn write_nonzero_byte_field(out: &mut impl fmt::Write, label: &str, bytes: u64) -> fmt::Result {
    if bytes == 0 {
        return Ok(());
    }
    out.write_char(' ')?;
    out.write_str(label)?;
    out.write_char('=')?;
    write_bytes_to(out, bytes)
}

fn write_duration_to(out: &mut impl fmt::Write, duration: Duration) -> fmt::Result {
    let secs = duration.as_secs();
    if secs >= 86_400 {
        let days = secs / 86_400;
        let hours = (secs % 86_400) / 3_600;
        write_integer(out, days)?;
        out.write_char('d')?;
        write_two_digits(out, hours as u8)?;
        return out.write_char('h');
    }
    if secs >= 3_600 {
        let hours = secs / 3_600;
        let minutes = (secs % 3_600) / 60;
        write_integer(out, hours)?;
        out.write_char('h')?;
        write_two_digits(out, minutes as u8)?;
        return out.write_char('m');
    }
    if secs >= 60 {
        let minutes = secs / 60;
        let seconds = secs % 60;
        write_integer(out, minutes)?;
        out.write_char('m')?;
        write_two_digits(out, seconds as u8)?;
        return out.write_char('s');
    }
    // Sub-second durations are the overwhelmingly common case, and `u64`
    // microseconds span 584,000 years; the saturation is a type boundary, not
    // a limit anyone can reach.
    if duration < Duration::from_secs(1) {
        let micros = u64::try_from(duration.as_micros()).unwrap_or(u64::MAX);
        return write_scaled_to(out, micros, 1_000, "ms");
    }
    let millis = u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
    write_scaled_to(out, millis, 1_000, "s")
}

const fn power_table<const N: usize>(base: u64) -> [u64; N] {
    let mut table = [1; N];
    let mut index = 1;
    while index < table.len() {
        table[index] = table[index - 1] * base;
        index += 1;
    }
    table
}

fn write_bytes_to(out: &mut impl fmt::Write, bytes: u64) -> fmt::Result {
    if bytes < 1024 {
        write_integer(out, bytes)?;
        return out.write_char('b');
    }

    let mut unit = 0;
    while unit + 1 < BYTE_SCALES.len() && bytes >= BYTE_SCALES[unit + 1] {
        unit += 1;
    }
    write_scaled_to(out, bytes, BYTE_SCALES[unit], BYTE_UNITS[unit])
}

/// Write `value / scale` with two, one or no decimal places.
///
/// Deliberately `u64` rather than `u128`: dividing a 128-bit value by a runtime
/// divisor compiles to a `__udivti3`/`__umodti3` *libcall*, tens of cycles
/// each, and this runs two or three times for every logged request. Nothing
/// here needs the range. `precision` is derived from `value / scale`, so
/// `value * factor` is bounded by `1000 * scale`, and the largest scale in use
/// is one tebibyte -- about 1.1e15, comfortably inside `u64`.
fn write_scaled_to(out: &mut impl fmt::Write, value: u64, scale: u64, unit: &str) -> fmt::Result {
    let whole = value / scale;
    let precision: usize = if whole >= 100 {
        0
    } else if whole >= 10 {
        1
    } else {
        2
    };

    let factor = DECIMAL_FACTORS[precision];
    let scaled = (value * factor + (scale / 2)) / scale;
    let integer = scaled / factor;
    let fractional = scaled % factor;

    write_integer(out, integer)?;
    if precision != 0 && fractional != 0 {
        out.write_char('.')?;
        match precision {
            1 => {
                out.write_char(char::from(b'0' + fractional as u8))?;
            },
            2 => {
                let tens = (fractional / 10) as u8;
                let ones = (fractional % 10) as u8;
                out.write_char(char::from(b'0' + tens))?;
                if ones != 0 {
                    out.write_char(char::from(b'0' + ones))?;
                }
            },
            _ => {
                unreachable!("precision is derived from the unit threshold")
            },
        }
    }
    out.write_str(unit)
}

fn write_integer<T: Integer>(out: &mut impl fmt::Write, value: T) -> fmt::Result {
    let mut buffer = ItoaBuffer::new();
    out.write_str(buffer.format(value))
}

fn write_two_digits(out: &mut impl fmt::Write, value: u8) -> fmt::Result {
    debug_assert!(value < 100);
    out.write_char(char::from(b'0' + value / 10))?;
    out.write_char(char::from(b'0' + value % 10))
}

const fn status_style(status: HttpStatusCode) -> Style {
    let style = Style::new();
    match status.get() {
        200..=299 => style.green(),
        300..=399 => style.cyan(),
        400..=499 => style.yellow(),
        500..=599 => style.red(),
        _ => style.magenta(),
    }
}

const fn websocket_close_style(close_code: WebSocketCloseCode) -> Style {
    let style = Style::new();
    match close_code.get() {
        code if code == close_code::NORMAL.get() => style.green(),
        1001 => style.cyan(),
        1002..=2999 => style.yellow(),
        3000..=3999 => style.blue(),
        4000..=4999 => style.red(),
        _ => style.magenta(),
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;
    use http::Method;
    use pyo3::Python;

    use super::{
        AccessLogRequest, ClientLabelBuf, Escaped, RequestSummaryDisplay, RequestSummaryKind,
        append_client_label, append_connection_client_label, write_bytes_to, write_duration_to,
        write_io_summary_to,
    };
    use crate::config::{ProxyConfig, ServerConfig};
    use crate::http::header_meta::RequestHeaderMeta;
    use crate::http::types::{
        BytesStr, H1RequestHeaders, HttpVersion, RequestHead, RequestHeaders, RequestTarget,
    };
    use crate::proxy_protocol::{
        ConnectionInfo, ConnectionPeer, ProxyProtocolMode, parse_trusted_peer,
    };
    use crate::runtime::{ConnectionShared, RequestContext, test_fixtures};
    use crate::tls::ConnectionSecurity;

    fn render(f: impl FnOnce(&mut String)) -> String {
        let mut out = String::new();
        f(&mut out);
        out
    }

    fn format_client(host: &str, port: u16) -> String {
        let mut client = ClientLabelBuf::new();
        append_client_label(&mut client, host, port);
        String::from_utf8(client.into_vec()).expect("client labels are UTF-8")
    }

    fn format_connection_client(info: &ConnectionInfo) -> String {
        let mut client = ClientLabelBuf::new();
        append_connection_client_label(&mut client, info);
        String::from_utf8(client.into_vec()).expect("client labels are UTF-8")
    }

    fn append_duration_to(out: &mut String, duration: Duration) {
        write_duration_to(out, duration).expect("writing to String cannot fail");
    }

    fn append_bytes_to(out: &mut String, bytes: u64) {
        write_bytes_to(out, bytes).expect("writing to String cannot fail");
    }

    fn append_io_summary_to(out: &mut String, duration: Duration, rx_bytes: u64, tx_bytes: u64) {
        write_io_summary_to(out, duration, rx_bytes, tx_bytes)
            .expect("writing to String cannot fail");
    }

    fn format_request_summary(
        request: &AccessLogRequest,
        summary_kind: RequestSummaryKind,
    ) -> String {
        RequestSummaryDisplay {
            request,
            summary_kind,
        }
        .to_string()
    }

    #[test]
    fn client_format_brackets_ipv6() {
        assert_eq!(format_client("2001:db8::1", 443), "[2001:db8::1]:443");
        assert_eq!(format_client("192.0.2.1", 8080), "192.0.2.1:8080");
        assert_eq!(format_client("192.0.2.1", 0), "192.0.2.1");
        assert_eq!(format_client("client\u{1b}name", 9), "client\\x1bname:9");

        let unix = ConnectionInfo {
            actual_server: None,
            proxy_headers_trusted: false,
            client: None,
            server: None,
        };
        assert_eq!(format_connection_client(&unix), "unix");
    }

    #[test]
    fn forged_x_forwarded_for_never_reaches_the_log_client_column() {
        Python::initialize();
        Python::attach(|py| {
            let config = Arc::new(ServerConfig {
                proxy: ProxyConfig {
                    trust_headers: true,
                    trusted_peers: Box::new([parse_trusted_peer("127.0.0.1").unwrap()]),
                    protocol: ProxyProtocolMode::Off,
                },
                ..test_fixtures::server_config_parts()
            });
            let info = ConnectionInfo::from_peer(
                ConnectionPeer::Tcp(SocketAddr::from(([127, 0, 0, 1], 54321))),
                None,
                &config.proxy,
            );
            assert!(info.proxy_headers_trusted);
            let connection = ConnectionShared::new(
                test_fixtures::app_runtime(py),
                config,
                info,
                ConnectionSecurity::Plaintext,
            );

            let head =
                Bytes::from_static(b"X-Forwarded-For: 1.1.1.1 \"GET /admin HTTP/1.1\" 200\r\n");
            let mut headers = H1RequestHeaders::new(head.clone());
            let value = &head[b"X-Forwarded-For: ".len()..head.len() - 2];
            let known_header = headers
                .push(b"X-Forwarded-For", value)
                .unwrap()
                .expect("X-Forwarded-For is a known header");
            let mut header_meta = RequestHeaderMeta::default();
            header_meta.observe_known_header_slice(known_header, value, 0, true);
            let request = RequestHead {
                http_version: HttpVersion::Http1_1,
                method: Method::GET,
                target: RequestTarget::normal(
                    BytesStr::from_static("http"),
                    BytesStr::from_static("/"),
                ),
                headers: RequestHeaders::from_h1(headers),
                header_meta,
            };

            let context = RequestContext::new(Arc::clone(&connection), request);
            drop(connection);
            let label = super::ClientLabel::build(&context);
            assert_eq!(label.as_str().trim_end(), "127.0.0.1:54321");
            assert!(!label.as_str().trim_end().contains([' ', '"']));
        });
    }

    /// A trusted proxy multiplexes many end clients over one upstream
    /// keep-alive connection, naming each in `X-Forwarded-For`. The client is
    /// therefore a per-*request* fact there, and caching the formatted label on
    /// the connection would stamp one request's log line with another client's
    /// address -- mis-attributing security-relevant evidence to save an
    /// `IpAddr::Display`.
    #[test]
    fn a_trusted_proxy_gets_a_fresh_client_label_for_every_request() {
        Python::initialize();
        Python::attach(|py| {
            let config = Arc::new(ServerConfig {
                proxy: ProxyConfig {
                    trust_headers: true,
                    trusted_peers: Box::new([parse_trusted_peer("127.0.0.1").unwrap()]),
                    protocol: ProxyProtocolMode::Off,
                },
                ..test_fixtures::server_config_parts()
            });
            let info = ConnectionInfo::from_peer(
                ConnectionPeer::Tcp(SocketAddr::from(([127, 0, 0, 1], 54321))),
                None,
                &config.proxy,
            );
            assert!(info.proxy_headers_trusted);
            let connection = ConnectionShared::new(
                test_fixtures::app_runtime(py),
                config,
                info,
                ConnectionSecurity::Plaintext,
            );

            let label_for = |client: &str| {
                let head = Bytes::from(format!("X-Forwarded-For: {client}\r\n"));
                let mut headers = H1RequestHeaders::new(head.clone());
                let value = &head[b"X-Forwarded-For: ".len()..head.len() - 2];
                let known_header = headers
                    .push(b"X-Forwarded-For", value)
                    .unwrap()
                    .expect("X-Forwarded-For is a known header");
                let mut header_meta = RequestHeaderMeta::default();
                header_meta.observe_known_header_slice(known_header, value, 0, true);
                let request = RequestHead {
                    http_version: HttpVersion::Http1_1,
                    method: Method::GET,
                    target: RequestTarget::normal(
                        BytesStr::from_static("http"),
                        BytesStr::from_static("/"),
                    ),
                    headers: RequestHeaders::from_h1(headers),
                    header_meta,
                };
                let context = RequestContext::new(Arc::clone(&connection), request);
                super::ClientLabel::build(&context).as_str().trim_end().to_owned()
            };

            let first = label_for("198.51.100.9");
            let second = label_for("203.0.113.7");
            assert!(first.starts_with("198.51.100.9"), "got {first}");
            assert!(
                second.starts_with("203.0.113.7"),
                "the second request on this connection reported {second}, \
                 which is the first request's client"
            );
        });
    }

    #[test]
    fn duration_format_is_compact() {
        assert_eq!(
            render(|out| append_duration_to(out, Duration::from_micros(830))),
            "0.83ms"
        );
        assert_eq!(
            render(|out| append_duration_to(out, Duration::from_millis(1840))),
            "1.84s"
        );
        assert_eq!(
            render(|out| append_duration_to(out, Duration::from_secs(65))),
            "1m05s"
        );
        assert_eq!(
            render(|out| append_duration_to(out, Duration::from_mins(123))),
            "2h03m"
        );
        assert_eq!(
            render(|out| append_duration_to(out, Duration::from_hours(52))),
            "2d04h"
        );
    }

    #[test]
    fn byte_format_is_binary() {
        assert_eq!(render(|out| append_bytes_to(out, 0)), "0b");
        assert_eq!(render(|out| append_bytes_to(out, 2150)), "2.1kib");
        assert_eq!(render(|out| append_bytes_to(out, 1_258_291)), "1.2mib");
    }

    #[test]
    fn io_summary_omits_empty_byte_fields() {
        let duration = Duration::from_micros(400);

        assert_eq!(
            render(|out| append_io_summary_to(out, duration, 0, 25)),
            " 0.4ms tx=25b"
        );
        assert_eq!(
            render(|out| append_io_summary_to(out, duration, 12, 25)),
            " 0.4ms rx=12b tx=25b"
        );
        assert_eq!(
            render(|out| append_io_summary_to(out, duration, 12, 0)),
            " 0.4ms rx=12b"
        );
        assert_eq!(
            render(|out| append_io_summary_to(out, duration, 0, 0)),
            " 0.4ms"
        );
    }

    #[test]
    fn request_summary_includes_full_path() {
        let request = AccessLogRequest {
            method: Method::GET,
            path_and_query: BytesStr::from("/this/path/keeps/going/and/going/and/going/and/going"),
            http_version: HttpVersion::Http2,
        };

        let summary = format_request_summary(&request, RequestSummaryKind::Http);

        assert!(summary.starts_with("\"GET "));
        assert!(summary.ends_with('"'));
        assert!(summary.contains("HTTP/2"));
        assert!(summary.contains("/this/path/keeps/going/and/going/and/going/and/going"));
    }

    #[test]
    fn request_summary_escapes_control_bytes_a_client_can_send() {
        // The HTTP/1 target grammar excludes SP/CR/LF but not ESC or DEL, so
        // these reach the log and must not act on the terminal reading it.
        let request = AccessLogRequest {
            method: Method::GET,
            path_and_query: BytesStr::from("/\u{1b}[31mred\u{7f}\u{9}\u{85}"),
            http_version: HttpVersion::Http1_1,
        };

        let summary = format_request_summary(&request, RequestSummaryKind::Http);

        assert_eq!(summary, "\"GET /\\x1b[31mred\\x7f\\x09\\u{85} HTTP/1.1\"");
        assert!(!summary.contains('\u{1b}'));
    }

    #[test]
    fn escaping_leaves_ordinary_targets_untouched() {
        assert_eq!(
            Escaped("/a/b?c=d&e=%20f+g#h").to_string(),
            "/a/b?c=d&e=%20f+g#h"
        );
        assert_eq!(Escaped("/π/漢").to_string(), "/π/漢");
    }

    #[test]
    fn websocket_summary_includes_protocol_and_path() {
        let request = AccessLogRequest {
            method: Method::GET,
            path_and_query: BytesStr::from("/ws"),
            http_version: HttpVersion::Http2,
        };

        assert_eq!(
            format_request_summary(&request, RequestSummaryKind::WebSocket),
            "\"WEBSOCKET /ws HTTP/2\""
        );
    }
}
