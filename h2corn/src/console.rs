use std::{
    fmt::{self, Write as _},
    io::{self, Write},
    num::NonZeroU16,
    str,
    sync::LazyLock,
    time::{Duration, Instant},
};

use anstream::{AutoStream, ColorChoice};
use http::Method;
use itoa::{Buffer as ItoaBuffer, Integer};
use owo_colors::{OwoColorize, Stream, Style};
use smallvec::SmallVec;

use crate::config::{BindTarget, ServerConfig};
use crate::error::H2CornError;
use crate::hpack::BytesStr;
use crate::http::app::{HttpRequestBody, run_asgi_http_request};
use crate::http::response::HttpResponseTransport;
use crate::http::scope::scope_view_from_parts;
use crate::http::types::{HttpStatusCode, HttpVersion, RequestHead, status_code};
use crate::proxy::{ConnectionInfo, ConnectionPeer};
use crate::runtime::{RequestAdmission, RequestContext};
use crate::websocket::{WebSocketCloseCode, close_code};

const MAX_IPV4_CLIENT: &str = "255.255.255.255:65535";
const ACCESS_LOG_LINE_CAPACITY: usize = 128;
const IPV4_CLIENT_WIDTH: usize = MAX_IPV4_CLIENT.len() - 2;
static ACCESS_LOG_MODE: LazyLock<AccessLogMode> = LazyLock::new(|| {
    let choice = AutoStream::choice(&io::stderr());
    if choice == ColorChoice::Never {
        AccessLogMode::Plain
    } else {
        AccessLogMode::Styled(choice)
    }
});

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AccessLogMode {
    Plain,
    Styled(ColorChoice),
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
type ClientLabelBuf = SmallVec<[u8; MAX_IPV4_CLIENT.len()]>;

struct BytesWriter<'a, const N: usize>(&'a mut SmallVec<[u8; N]>);

impl<const N: usize> fmt::Write for BytesWriter<'_, N> {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        self.0.extend_from_slice(value.as_bytes());
        Ok(())
    }
}

struct HostDisplay<'a>(&'a str);

impl fmt::Display for HostDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0.contains(':') && !self.0.starts_with('[') {
            write!(f, "[{}]", self.0)
        } else {
            f.write_str(self.0)
        }
    }
}

struct ClientLabel(ClientLabelBuf);

impl ClientLabel {
    fn build(ctx: &RequestContext) -> Self {
        let mut label = ClientLabelBuf::new();
        let _ = append_log_client(&mut BytesWriter(&mut label), ctx);
        if label.first() != Some(&b'[') {
            label.resize(IPV4_CLIENT_WIDTH.max(label.len()), b' ');
        }
        Self(label)
    }

    fn as_str(&self) -> &str {
        // SAFETY: `ClientLabel::build` only writes ASCII via `fmt::Write`,
        // host strings, digits, and punctuation.
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
                status: status.get(),
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
    pub(crate) status: Option<NonZeroU16>,
    pub(crate) response_body_bytes: u64,
}

impl ResponseLogState {
    pub(crate) fn started(&mut self, status: HttpStatusCode) {
        self.status =
            Some(NonZeroU16::new(status).expect("response status codes are always non-zero"));
    }

    pub(crate) fn sent_body(&mut self, len: usize) {
        self.response_body_bytes = self.response_body_bytes.saturating_add(len as u64);
    }

    pub(crate) fn internal_error(&mut self) {
        self.started(status_code::INTERNAL_SERVER_ERROR);
    }
}

pub(crate) async fn run_http_request<T, F>(
    ctx: RequestContext,
    request_body: HttpRequestBody,
    admission: RequestAdmission,
    transport: &mut T,
    read_body_bytes: F,
) -> Result<(), H2CornError>
where
    T: HttpResponseTransport,
    F: FnOnce() -> u64,
{
    let access_log = HttpAccessLogState::new(&ctx);
    let result = run_asgi_http_request(ctx, request_body, admission, transport).await;
    access_log.emit_http_response(transport.response_log_state(), read_body_bytes);
    result
}

pub(crate) fn emit_banner(config: &ServerConfig) {
    const LISTENING_PREFIX: &str = "Listening on ";
    const LISTENING_INDENT: &str = "             ";

    let mut stderr = anstream::stderr().lock();
    let _ = writeln!(
        stderr,
        "{} v{} • HTTP/2 ASGI",
        "h2corn".if_supports_color(Stream::Stderr, |text| {
            text.style(Style::new().bold().cyan())
        }),
        env!("CARGO_PKG_VERSION"),
    );

    let mut binds = config
        .binds
        .iter()
        .map(|bind| format_listen_target(bind, config.tls.is_some()));
    if let Some(first) = binds.next() {
        write_listen_target_line(&mut stderr, LISTENING_PREFIX, &first);
        for bind in binds {
            write_listen_target_line(&mut stderr, LISTENING_INDENT, &bind);
        }
    }

    if config.http1.enabled {
        let _ = writeln!(
            stderr,
            "HTTP/1 compatibility is enabled; disable with --no-http1",
        );
    }

    let _ = writeln!(stderr);
}

fn write_listen_target_line(stderr: &mut impl Write, prefix: &str, bind: &str) {
    let _ = writeln!(
        stderr,
        "{prefix}{}",
        bind.if_supports_color(Stream::Stderr, |text| text.bold()),
    );
}

pub(crate) fn emit_http_access_log(entry: &HttpAccessLogEntry<'_>) {
    let io_summary = AccessLogIoSummary {
        duration: entry.duration,
        rx_bytes: entry.rx_bytes,
        tx_bytes: entry.tx_bytes,
    };
    match *ACCESS_LOG_MODE {
        AccessLogMode::Plain => {
            emit_plain_access_log(
                entry.client_label,
                entry.request,
                RequestSummaryKind::Http,
                entry.status,
                io_summary,
            );
        }
        AccessLogMode::Styled(choice) => {
            let mut stderr = AutoStream::new(io::stderr(), choice).lock();
            emit_styled_access_log(
                &mut stderr,
                entry.client_label,
                entry.request,
                RequestSummaryKind::Http,
                entry.status,
                status_style(entry.status),
                io_summary,
            );
        }
    }
}

pub(crate) fn emit_websocket_access_log(entry: &WebSocketAccessLogEntry<'_>) {
    let io_summary = AccessLogIoSummary {
        duration: entry.duration,
        rx_bytes: entry.rx_bytes,
        tx_bytes: entry.tx_bytes,
    };
    match *ACCESS_LOG_MODE {
        AccessLogMode::Plain => {
            emit_plain_access_log(
                entry.client_label,
                entry.request,
                RequestSummaryKind::WebSocket,
                entry.close_code,
                io_summary,
            );
        }
        AccessLogMode::Styled(choice) => {
            let mut stderr = AutoStream::new(io::stderr(), choice).lock();
            emit_styled_access_log(
                &mut stderr,
                entry.client_label,
                entry.request,
                RequestSummaryKind::WebSocket,
                entry.close_code,
                websocket_close_style(entry.close_code),
                io_summary,
            );
        }
    }
}

fn format_listen_target(bind: &BindTarget, tls: bool) -> String {
    match bind {
        BindTarget::Tcp { host, port } => format!(
            "{}://{}:{port}",
            if tls { "https" } else { "http" },
            HostDisplay(host.as_ref())
        ),
        BindTarget::Unix { path } => format!("unix:{path}"),
        BindTarget::Fd { fd, .. } => format!("fd://{fd}"),
    }
}

fn emit_plain_access_log<T>(
    client_label: &str,
    request: &AccessLogRequest,
    summary_kind: RequestSummaryKind,
    code: T,
    io_summary: AccessLogIoSummary,
) where
    T: fmt::Display,
{
    let mut line = AccessLogBuf::new();
    let _ = writeln!(
        BytesWriter(&mut line),
        "{} {} {} {}",
        client_label,
        RequestSummaryDisplay {
            request,
            summary_kind,
        },
        code,
        IoSummaryDisplay(io_summary),
    );
    let mut stderr = io::stderr().lock();
    let _ = stderr.write_all(&line);
}

fn emit_styled_access_log<T>(
    stderr: &mut impl Write,
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
    let _ = writeln!(
        BytesWriter(&mut line),
        "{} {} {} {}",
        client_label,
        RequestSummaryDisplay {
            request,
            summary_kind,
        },
        code.style(code_style),
        IoSummaryDisplay(io_summary).style(Style::new().dimmed()),
    );
    let _ = stderr.write_all(&line);
}

fn append_client(out: &mut impl fmt::Write, info: &ConnectionInfo) -> fmt::Result {
    if let Some(client) = &info.client {
        return write!(out, "{}:{}", HostDisplay(client.host.as_ref()), client.port);
    }

    match &info.actual_peer {
        ConnectionPeer::Tcp(peer) => write!(out, "{peer}"),
        ConnectionPeer::Unix => out.write_str("unix"),
    }
}

fn append_log_client(out: &mut impl fmt::Write, ctx: &RequestContext) -> fmt::Result {
    let connection = &ctx.connection;
    let view = scope_view_from_parts(
        ctx.request.scheme_str(),
        connection.config,
        &connection.info,
        &ctx.scope_overrides,
    );
    if let Some((host, port)) = view.client {
        if port == 0 {
            write!(out, "{}", HostDisplay(host))
        } else {
            write!(out, "{}:{port}", HostDisplay(host))
        }
    } else {
        append_client(out, &connection.info)
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
        }
        RequestSummaryKind::WebSocket => {
            out.write_str("WEBSOCKET ")?;
        }
    }
    out.write_str(request.path_and_query.as_str())?;
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
    if duration < Duration::from_secs(1) {
        return write_scaled_to(out, duration.as_micros(), 1_000, "ms");
    }
    write_scaled_to(out, duration.as_millis(), 1_000, "s")
}

fn write_bytes_to(out: &mut impl fmt::Write, bytes: u64) -> fmt::Result {
    const UNITS: [&str; 5] = ["b", "kib", "mib", "gib", "tib"];

    if bytes < 1024 {
        write_integer(out, bytes)?;
        return out.write_char('b');
    }

    let mut unit = 0;
    let mut scale = 1_u128;
    while unit < UNITS.len() - 1 && u128::from(bytes) >= scale * 1024 {
        scale *= 1024;
        unit += 1;
    }
    write_scaled_to(out, u128::from(bytes), scale, UNITS[unit])
}

fn write_scaled_to(out: &mut impl fmt::Write, value: u128, scale: u128, unit: &str) -> fmt::Result {
    let whole = value / scale;
    let precision: usize = if whole >= 100 {
        0
    } else if whole >= 10 {
        1
    } else {
        2
    };

    let factor = 10_u128.pow(precision as u32);
    let scaled = (value * factor + (scale / 2)) / scale;
    let integer = scaled / factor;
    let fractional = scaled % factor;

    write_integer(out, integer)?;
    if precision != 0 && fractional != 0 {
        out.write_char('.')?;
        match precision {
            1 => {
                out.write_char(char::from(b'0' + fractional as u8))?;
            }
            2 => {
                let tens = (fractional / 10) as u8;
                let ones = (fractional % 10) as u8;
                out.write_char(char::from(b'0' + tens))?;
                if ones != 0 {
                    out.write_char(char::from(b'0' + ones))?;
                }
            }
            _ => {
                unreachable!("precision is derived from the unit threshold")
            }
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

fn status_style(status: HttpStatusCode) -> Style {
    let style = Style::new();
    match status {
        200..=299 => style.green(),
        300..=399 => style.cyan(),
        400..=499 => style.yellow(),
        500..=599 => style.red(),
        _ => style.magenta(),
    }
}

fn websocket_close_style(close_code: WebSocketCloseCode) -> Style {
    let style = Style::new();
    match close_code {
        close_code::NORMAL => style.green(),
        1001 => style.cyan(),
        close_code::PROTOCOL_ERROR..=2999 => style.yellow(),
        3000..=3999 => style.blue(),
        4000..=4999 => style.red(),
        _ => style.magenta(),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AccessLogRequest, RequestSummaryDisplay, RequestSummaryKind, append_client, write_bytes_to,
        write_duration_to, write_io_summary_to,
    };
    use crate::hpack::BytesStr;
    use crate::http::types::HttpVersion;
    use crate::proxy::{ClientAddr, ConnectionInfo, ConnectionPeer, ServerAddr};
    use http::Method;
    use std::net::{IpAddr, Ipv6Addr, SocketAddr};
    use std::time::Duration;

    fn render(f: impl FnOnce(&mut String)) -> String {
        let mut out = String::new();
        f(&mut out);
        out
    }

    fn format_client(info: &ConnectionInfo) -> String {
        let mut client = String::new();
        let _ = append_client(&mut client, info);
        client
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
        let info = ConnectionInfo {
            actual_peer: ConnectionPeer::Tcp(SocketAddr::new(
                IpAddr::V6(Ipv6Addr::LOCALHOST),
                9000,
            )),
            actual_server: Some(ServerAddr {
                host: "::1".into(),
                port: Some(8000),
            }),
            proxy_headers_trusted: false,
            client: Some(ClientAddr {
                host: "2001:db8::1".into(),
                port: 443,
            }),
            server: None,
        };

        assert_eq!(format_client(&info), "[2001:db8::1]:443");
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
            render(|out| append_duration_to(out, Duration::from_secs(7_380))),
            "2h03m"
        );
        assert_eq!(
            render(|out| append_duration_to(out, Duration::from_secs(187_200))),
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
