use std::borrow::Cow;
use std::fmt::{self, Write};
use std::net::IpAddr;
use std::num::{NonZeroU16, NonZeroU32};

use atoi_simd::parse_pos;

use crate::config::{BindTarget, ServerConfig};
use crate::http::header::{
    header_value_text, last_csv_token, normalize_scheme, parse_forwarded_value, parse_host_port,
    parse_x_forwarded_for_value,
};
use crate::http::header_meta::ProxyHeaderSlots;
use crate::http::types::{RequestHead, RequestHeaders};
use crate::proxy_protocol::{ConnectionInfo, ServerEndpoint};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ScopeHost<'a> {
    Text(&'a str),
    Ip(&'a IpAddr),
}

impl ScopeHost<'_> {
    pub(crate) fn with_text<T>(self, f: impl FnOnce(&str) -> T) -> T {
        match self {
            Self::Text(text) => f(text),
            Self::Ip(ip) => {
                let mut text = IpText::default();
                write!(text, "{ip}").expect("an IP address fits in its fixed display buffer");
                // SAFETY: `fmt::Write` appended only UTF-8 input from `IpAddr::fmt`.
                f(unsafe { std::str::from_utf8_unchecked(&text.bytes[..text.len]) })
            },
        }
    }
}

struct IpText {
    bytes: [u8; 45],
    len: usize,
}

impl Default for IpText {
    fn default() -> Self {
        Self {
            bytes: [0; 45],
            len: 0,
        }
    }
}

impl Write for IpText {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        let end = self.len + value.len();
        if end > self.bytes.len() {
            return Err(fmt::Error);
        }
        self.bytes[self.len..end].copy_from_slice(value.as_bytes());
        self.len = end;
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ScopeView<'a> {
    pub scheme: Cow<'a, str>,
    pub client: Option<(ScopeHost<'a>, u16)>,
    pub server: (ScopeHost<'a>, Option<u16>),
    pub root_path: Cow<'a, str>,
}

#[derive(Clone, Copy, Debug, Default)]
struct ProxyHeaderView<'a> {
    forwarded: Option<&'a str>,
    x_forwarded_for: Option<&'a str>,
    x_forwarded_proto: Option<&'a str>,
    x_forwarded_host: Option<&'a str>,
    x_forwarded_port: Option<&'a str>,
    x_forwarded_prefix: Option<&'a str>,
}

fn default_server<'a>(
    config: &'a ServerConfig,
    info: &'a ConnectionInfo,
) -> (ScopeHost<'a>, Option<u16>) {
    if let Some(server) = &info.server {
        return (ScopeHost::Ip(&server.ip), server.port.map(NonZeroU16::get));
    }
    if let Some(server) = &info.actual_server {
        return match server {
            ServerEndpoint::Tcp(server) => {
                (ScopeHost::Ip(&server.ip), server.port.map(NonZeroU16::get))
            },
            ServerEndpoint::Unix(path) => (ScopeHost::Text(path), None),
        };
    }
    match config.binds.first() {
        Some(BindTarget::Tcp { host, port }) => (ScopeHost::Text(host), Some(*port)),
        Some(BindTarget::Unix { path }) => (ScopeHost::Text(path), None),
        Some(BindTarget::Fd { .. }) | None => (ScopeHost::Text(""), None),
    }
}

fn default_client(info: &ConnectionInfo) -> Option<(ScopeHost<'_>, u16)> {
    info.client
        .as_ref()
        .map(|client| (ScopeHost::Ip(&client.ip), client.port))
}

pub(crate) fn default_scope_view<'a>(
    scheme: &'a str,
    config: &'a ServerConfig,
    info: &'a ConnectionInfo,
) -> ScopeView<'a> {
    ScopeView {
        scheme: Cow::Borrowed(scheme),
        client: default_client(info),
        server: default_server(config, info),
        root_path: Cow::Borrowed(config.root_path.as_ref()),
    }
}

/// The request's scope view, and the connection defaults it started from.
///
/// Callers that need to know *whether* a field was overridden need both, and
/// building the defaults separately meant constructing the same value twice
/// per request -- for nothing on the common path, where no proxy is trusted
/// and the two are equal by construction.
pub(crate) fn scope_view_with_defaults<'a>(
    request: &'a RequestHead,
    config: &'a ServerConfig,
    info: &'a ConnectionInfo,
) -> (ScopeView<'a>, ScopeView<'a>) {
    let defaults = default_scope_view(request.scheme_str(), config, info);

    if !info.proxy_headers_trusted {
        return (defaults.clone(), defaults);
    }
    let mut view = defaults.clone();

    let proxy_headers = request_proxy_headers(request);
    let used_forwarded =
        if let Some(forwarded) = proxy_headers.forwarded.and_then(parse_forwarded_value) {
            if let Some(host) = forwarded.client_host {
                let port = view.client.as_ref().map_or(0, |(_, port)| *port);
                view.client = Some((ScopeHost::Text(host), port));
            }
            if let Some(proto) = forwarded.proto {
                view.scheme = normalize_scheme(proto);
            }
            if let Some((host, port)) = forwarded.host {
                view.server = (
                    ScopeHost::Text(host),
                    port.or_else(|| default_port_for_scheme(&view.scheme))
                        .or(view.server.1),
                );
            }
            true
        } else {
            false
        };

    if !used_forwarded {
        if let Some(host) = proxy_headers
            .x_forwarded_for
            .and_then(|value| parse_x_forwarded_for_value(value, config))
        {
            let port = view.client.as_ref().map_or(0, |(_, port)| *port);
            view.client = Some((ScopeHost::Text(host), port));
        }
        if let Some(proto) = proxy_headers
            .x_forwarded_proto
            .map(last_csv_token)
            .filter(|value| !value.is_empty())
        {
            view.scheme = normalize_scheme(proto);
        }
        if let Some(host) = proxy_headers
            .x_forwarded_host
            .map(last_csv_token)
            .filter(|value| !value.is_empty())
            && let Some((host, port)) = parse_host_port(host)
        {
            view.server = (
                ScopeHost::Text(host),
                port.or_else(|| default_port_for_scheme(&view.scheme))
                    .or(view.server.1),
            );
        }
        if let Some(port) = proxy_headers
            .x_forwarded_port
            .map(last_csv_token)
            .and_then(|value| parse_pos::<u16, false>(value.as_bytes()).ok())
        {
            view.server.1 = Some(port);
        }
    }

    if let Some(prefix) = proxy_headers
        .x_forwarded_prefix
        .map(last_csv_token)
        .filter(|value| !value.is_empty())
    {
        view.root_path = join_root_path(prefix, config.root_path.as_ref());
    }

    (view, defaults)
}

/// The request's scope view alone, for callers with no use for the defaults.
pub(crate) fn resolve_scope_view<'a>(
    request: &'a RequestHead,
    config: &'a ServerConfig,
    info: &'a ConnectionInfo,
) -> ScopeView<'a> {
    scope_view_with_defaults(request, config, info).0
}

fn request_proxy_headers(request: &RequestHead) -> ProxyHeaderView<'_> {
    let headers = &request.headers;
    let Some(slots) = request.header_meta.proxy_headers() else {
        return ProxyHeaderView::default();
    };

    ProxyHeaderView {
        forwarded: proxy_header_value(headers, slots.forwarded),
        x_forwarded_for: proxy_header_value(headers, slots.x_forwarded_for),
        x_forwarded_proto: proxy_header_value(headers, slots.x_forwarded_proto),
        x_forwarded_host: proxy_header_value(headers, slots.x_forwarded_host),
        x_forwarded_port: proxy_header_value(headers, slots.x_forwarded_port),
        x_forwarded_prefix: proxy_header_value(headers, slots.x_forwarded_prefix),
    }
}

fn proxy_header_value(headers: &RequestHeaders, index: Option<NonZeroU32>) -> Option<&str> {
    ProxyHeaderSlots::index(index)
        .and_then(|index| headers.get(index))
        .and_then(|header| header_value_text(header.value()))
}

fn default_port_for_scheme(scheme: &str) -> Option<u16> {
    match scheme {
        "http" | "ws" => Some(80),
        "https" | "wss" => Some(443),
        _ => None,
    }
}

fn join_root_path<'a>(prefix: &'a str, root_path: &'a str) -> Cow<'a, str> {
    let original_root_path = root_path;
    let prefix = prefix.trim_end_matches('/');
    let root_path = root_path.trim_start_matches('/');
    match (prefix.is_empty(), root_path.is_empty()) {
        (true, true) => Cow::Borrowed(""),
        (false, true) => Cow::Borrowed(prefix),
        (true, false) => {
            if let Some(rest) = original_root_path.strip_prefix('/')
                && !rest.starts_with('/')
            {
                return Cow::Borrowed(original_root_path);
            }
            let mut joined = String::with_capacity(root_path.len() + 1);
            joined.push('/');
            joined.push_str(root_path);
            Cow::Owned(joined)
        },
        (false, false) => {
            let mut joined = String::with_capacity(prefix.len() + root_path.len() + 1);
            joined.push_str(prefix);
            joined.push('/');
            joined.push_str(root_path);
            Cow::Owned(joined)
        },
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::sync::Arc;

    use bytes::Bytes;
    use http::Method;

    use super::resolve_scope_view;
    use crate::config::ServerConfig;
    use crate::http::header_meta::RequestHeaderMeta;
    use crate::http::types::{
        BytesStr, H1RequestHeaders, HttpVersion, RequestHead, RequestHeaders, RequestTarget,
    };
    use crate::proxy_protocol::{ConnectionInfo, ServerEndpoint};
    use crate::runtime::test_fixtures;

    #[test]
    fn forwarded_prefix_borrows_the_request_header_until_scope_construction() {
        let raw = Bytes::from_static(b"/edge");
        let mut headers = H1RequestHeaders::new(raw.clone());
        let known = headers
            .push(b"x-forwarded-prefix", raw.as_ref())
            .expect("field is valid")
            .expect("field is known");
        let mut header_meta = RequestHeaderMeta::default();
        header_meta.observe_known_header(known, &raw, 0, true);
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
        let info = ConnectionInfo {
            actual_server: None,
            proxy_headers_trusted: true,
            client: None,
            server: None,
        };
        let config = ServerConfig {
            root_path: Box::from(""),
            ..test_fixtures::server_config_parts()
        };

        let view = resolve_scope_view(&request, &config, &info);
        assert!(matches!(view.root_path, Cow::Borrowed("/edge")));
    }

    #[test]
    fn unix_listener_path_overrides_an_unrelated_first_configured_bind() {
        let info = ConnectionInfo {
            actual_server: Some(ServerEndpoint::Unix(Arc::from("/run/h2corn.sock"))),
            proxy_headers_trusted: false,
            client: None,
            server: None,
        };
        let config = test_fixtures::server_config_parts();

        let view = super::default_scope_view("http", &config, &info);

        assert_eq!(view.server.1, None);
        view.server
            .0
            .with_text(|host| assert_eq!(host, "/run/h2corn.sock"));
    }
}
