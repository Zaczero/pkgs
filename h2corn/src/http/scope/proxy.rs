use std::borrow::Cow;

use atoi_simd::parse_pos;

use crate::config::{BindTarget, ServerConfig};
use crate::http::header::{
    header_value_text, last_csv_token, normalize_scheme, parse_forwarded_value, parse_host_port,
    parse_x_forwarded_for_value,
};
use crate::http::types::{RequestHead, RequestHeaders};
use crate::proxy::{ClientAddr, ConnectionInfo, ServerAddr};

#[derive(Clone, Debug)]
pub struct ScopeView<'a> {
    pub scheme: Cow<'a, str>,
    pub client: Option<(&'a str, u16)>,
    pub server: (&'a str, Option<u16>),
    pub root_path: Cow<'a, str>,
}

#[derive(Clone, Debug, Default)]
pub struct ScopeOverrides {
    pub(crate) scheme: Option<Box<str>>,
    pub(crate) client: Option<ClientAddr>,
    pub(crate) server: Option<ServerAddr>,
    pub(crate) root_path: Option<Box<str>>,
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
) -> (&'a str, Option<u16>) {
    info.server.as_ref().map_or_else(
        || {
            info.actual_server.as_ref().map_or_else(
                || match config.binds.first() {
                    Some(BindTarget::Tcp { host, port }) => (host.as_ref(), Some(*port)),
                    Some(BindTarget::Unix { path }) => (path.as_ref(), None),
                    Some(BindTarget::Fd { .. }) | None => ("", None),
                },
                |server| (server.host.as_ref(), server.port),
            )
        },
        |server| (server.host.as_ref(), server.port),
    )
}

fn default_client(info: &ConnectionInfo) -> Option<(&str, u16)> {
    info.client
        .as_ref()
        .map(|client| (client.host.as_ref(), client.port))
}

pub fn resolve_scope_view<'a>(
    request: &'a RequestHead,
    config: &'a ServerConfig,
    info: &'a ConnectionInfo,
) -> ScopeView<'a> {
    let default_server = default_server(config, info);

    let mut view = ScopeView {
        scheme: Cow::Borrowed(request.scheme_str()),
        client: default_client(info),
        server: default_server,
        root_path: Cow::Borrowed(config.root_path.as_ref()),
    };

    if !info.proxy_headers_trusted {
        return view;
    }

    let proxy_headers = request_proxy_headers(request);
    let used_forwarded =
        if let Some(forwarded) = proxy_headers.forwarded.and_then(parse_forwarded_value) {
            if let Some(host) = forwarded.client_host {
                let port = view.client.as_ref().map_or(0, |(_, port)| *port);
                view.client = Some((host, port));
            }
            if let Some(proto) = forwarded.proto {
                view.scheme = normalize_scheme(proto);
            }
            if let Some((host, port)) = forwarded.host {
                view.server = (
                    host,
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
            view.client = Some((host, port));
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
                host,
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

    view
}

pub fn resolve_scope_overrides(
    request: &RequestHead,
    config: &ServerConfig,
    info: &ConnectionInfo,
) -> ScopeOverrides {
    let default_server = default_server(config, info);
    let default_client = default_client(info);
    let view = resolve_scope_view(request, config, info);
    let mut overrides = ScopeOverrides::default();

    if view.scheme != request.scheme_str() {
        overrides.scheme = Some(view.scheme.into_owned().into_boxed_str());
    }
    if view.client != default_client
        && let Some((host, port)) = view.client
    {
        overrides.client = Some(ClientAddr {
            host: host.into(),
            port,
        });
    }
    if view.server != default_server {
        overrides.server = Some(ServerAddr {
            host: view.server.0.into(),
            port: view.server.1,
        });
    }
    if view.root_path.as_ref() != config.root_path.as_ref() {
        overrides.root_path = Some(view.root_path.into_owned().into_boxed_str());
    }

    overrides
}

fn request_proxy_headers(request: &RequestHead) -> ProxyHeaderView<'_> {
    let headers = &request.headers;
    let slots = &request.header_meta.proxy_headers;

    ProxyHeaderView {
        forwarded: proxy_header_value(headers, slots.forwarded),
        x_forwarded_for: proxy_header_value(headers, slots.x_forwarded_for),
        x_forwarded_proto: proxy_header_value(headers, slots.x_forwarded_proto),
        x_forwarded_host: proxy_header_value(headers, slots.x_forwarded_host),
        x_forwarded_port: proxy_header_value(headers, slots.x_forwarded_port),
        x_forwarded_prefix: proxy_header_value(headers, slots.x_forwarded_prefix),
    }
}

fn proxy_header_value(headers: &RequestHeaders, index: Option<usize>) -> Option<&str> {
    index
        .and_then(|index| headers.get(index))
        .and_then(|(_, value)| header_value_text(value))
}

pub fn scope_view_from_parts<'a>(
    scheme: &'a str,
    config: &'a ServerConfig,
    info: &'a ConnectionInfo,
    overrides: &'a ScopeOverrides,
) -> ScopeView<'a> {
    ScopeView {
        scheme: overrides
            .scheme
            .as_deref()
            .map_or(Cow::Borrowed(scheme), Cow::Borrowed),
        client: overrides
            .client
            .as_ref()
            .map(|client| (client.host.as_ref(), client.port))
            .or_else(|| default_client(info)),
        server: overrides.server.as_ref().map_or_else(
            || default_server(config, info),
            |server| (server.host.as_ref(), server.port),
        ),
        root_path: overrides
            .root_path
            .as_deref()
            .map_or_else(|| Cow::Borrowed(config.root_path.as_ref()), Cow::Borrowed),
    }
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
