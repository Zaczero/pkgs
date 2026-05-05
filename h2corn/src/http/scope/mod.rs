mod proxy;

use std::borrow::Cow;

use http::Method;
use memchr::memchr;
pub use proxy::{ScopeOverrides, resolve_scope_overrides, scope_view_from_parts};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyString};
use pyo3::{ffi, intern};

use crate::ascii;
use crate::hpack::BytesStr;
use crate::http::types::{HttpVersion, KnownRequestHeaderName, RequestHeaderName, RequestHeaders};
use crate::python::{py_cached_dict, py_dict, py_match_cached_bytes, py_match_cached_string};
use crate::runtime::RequestContext;

fn decode_path(raw_path: &str) -> Cow<'_, str> {
    let bytes = raw_path.as_bytes();
    let first_escape = if bytes.len() <= 16 {
        let mut index = 0;
        while index < bytes.len() && bytes[index] != b'%' {
            index += 1;
        }
        if index == bytes.len() {
            return Cow::Borrowed(raw_path);
        }
        index
    } else {
        let Some(index) = memchr(b'%', bytes) else {
            return Cow::Borrowed(raw_path);
        };
        index
    };

    let mut out = None::<Vec<u8>>;
    let mut index = first_escape;
    let mut copied = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' && index + 2 < bytes.len() {
            let high = ascii::HEX_VALUE[usize::from(bytes[index + 1])];
            let low = ascii::HEX_VALUE[usize::from(bytes[index + 2])];
            if high != ascii::INVALID_VALUE && low != ascii::INVALID_VALUE {
                let out = out.get_or_insert_with(|| Vec::with_capacity(bytes.len()));
                out.extend_from_slice(&bytes[copied..index]);
                out.push((high << 4) | low);
                index += 3;
                copied = index;
                continue;
            }
        }
        index += 1;
    }
    let Some(mut out) = out else {
        return Cow::Borrowed(raw_path);
    };
    out.extend_from_slice(&bytes[copied..]);

    String::from_utf8(out).map_or_else(|_| Cow::Borrowed(raw_path), Cow::Owned)
}

pub fn build_http_scope<'py>(
    py: Python<'py>,
    ctx: &RequestContext,
) -> PyResult<Bound<'py, PyDict>> {
    build_base_scope::<true>(
        py,
        ctx,
        http_scope_extensions(py, ctx.request.accepts_trailers())?,
        None,
    )
}

pub fn build_websocket_scope<'py>(
    py: Python<'py>,
    ctx: &RequestContext,
    requested_subprotocols: &[BytesStr],
) -> PyResult<Bound<'py, PyDict>> {
    build_base_scope::<false>(
        py,
        ctx,
        websocket_scope_extensions(py)?,
        (!requested_subprotocols.is_empty()).then_some(requested_subprotocols),
    )
}

fn build_base_scope<'py, const IS_HTTP: bool>(
    py: Python<'py>,
    ctx: &RequestContext,
    extensions: Bound<'py, PyDict>,
    websocket_subprotocols: Option<&[BytesStr]>,
) -> PyResult<Bound<'py, PyDict>> {
    let request = &ctx.request;
    let view = scope_view_from_parts(
        request.scheme().map_or("", BytesStr::as_str),
        ctx.connection.config,
        &ctx.connection.info,
        &ctx.scope_overrides,
    );
    let path_and_query = request.path_and_query().map_or("", BytesStr::as_str);
    let (raw_path, query) = path_and_query
        .split_once('?')
        .unwrap_or((path_and_query, ""));
    let path = decode_path(raw_path);
    let resolved_scheme = if IS_HTTP {
        view.scheme.as_ref()
    } else if view.scheme == "https" {
        "wss"
    } else {
        "ws"
    };
    Ok(py_dict!(py, {
        "type" => scope_type_to_python::<IS_HTTP>(py),
        "asgi" => asgi_scope_dict(py)?,
        if IS_HTTP || request.http_version != HttpVersion::Http1_1 => {
            "http_version" => match request.http_version {
                HttpVersion::Http1_1 => intern!(py, "1.1"),
                HttpVersion::Http2 => intern!(py, "2"),
            },
        },
        "scheme" => scheme_to_python(py, resolved_scheme),
        "raw_path" => raw_path_to_python(py, raw_path),
        "path" => path_to_python(py, path.as_ref()),
        "query_string" => query_string_to_python(py, query),
        if !view.root_path.is_empty() => {
            "root_path" => PyString::new(py, view.root_path.as_ref()),
        },
        "server" => server_scope_value(py, ctx, view.server)?,
        "headers" => headers_to_python(py, &request.headers)?,
        "extensions" => extensions,
        if let Some(subprotocols) = websocket_subprotocols => {
            "subprotocols" => PyList::new(py, subprotocols.iter().map(BytesStr::as_str))?,
        },
        if IS_HTTP => {
            "method" => method_to_python(py, &request.method),
        },
        if let Some(client) = client_scope_value(py, ctx, view.client)? => {
            "client" => client,
        },
    }))
}

fn http_scope_extensions(py: Python<'_>, accepts_trailers: bool) -> PyResult<Bound<'_, PyDict>> {
    if accepts_trailers {
        py_cached_dict!(py, {
            "http.response.pathsend" => py_dict!(py, {}),
            "http.response.trailers" => py_dict!(py, {}),
        })
    } else {
        py_cached_dict!(py, {
            "http.response.pathsend" => py_dict!(py, {}),
        })
    }
}

fn websocket_scope_extensions(py: Python<'_>) -> PyResult<Bound<'_, PyDict>> {
    py_cached_dict!(py, {
        "websocket.http.response" => py_dict!(py, {}),
    })
}

fn asgi_scope_dict(py: Python<'_>) -> PyResult<Bound<'_, PyDict>> {
    py_cached_dict!(py, {
        "version" => "3.0",
        "spec_version" => "2.5",
    })
}

pub fn headers_to_python<'py>(
    py: Python<'py>,
    headers: &RequestHeaders,
) -> PyResult<Bound<'py, PyList>> {
    // SAFETY: the GIL is held by `py`; the list and each tuple are allocated to
    // their exact final length; each slot is written exactly once with a fresh
    // owned reference; and ownership is transferred immediately to the
    // containing Python object.
    unsafe {
        let list = Bound::from_owned_ptr_or_err(py, ffi::PyList_New(headers.len().cast_signed()))?
            .cast_into_unchecked::<PyList>();

        for (index, header) in headers.iter().enumerate() {
            let tuple = Bound::from_owned_ptr_or_err(py, ffi::PyTuple_New(2))?;

            let name = header_name_to_python(py, &header.0).unbind().into_ptr();
            #[cfg(PyPy)]
            if ffi::PyTuple_SetItem(tuple.as_ptr(), 0, name) != 0 {
                return Err(PyErr::fetch(py));
            }
            #[cfg(not(PyPy))]
            ffi::PyTuple_SET_ITEM(tuple.as_ptr(), 0, name);

            let value = PyBytes::new(py, header.1.as_bytes()).unbind().into_ptr();
            #[cfg(PyPy)]
            if ffi::PyTuple_SetItem(tuple.as_ptr(), 1, value) != 0 {
                return Err(PyErr::fetch(py));
            }
            #[cfg(not(PyPy))]
            ffi::PyTuple_SET_ITEM(tuple.as_ptr(), 1, value);

            ffi::PyList_SET_ITEM(
                list.as_ptr(),
                index.cast_signed(),
                tuple.unbind().into_ptr(),
            );
        }

        Ok(list)
    }
}

fn scope_type_to_python<const IS_HTTP: bool>(py: Python<'_>) -> Bound<'_, PyString> {
    if IS_HTTP {
        intern!(py, "http").clone()
    } else {
        intern!(py, "websocket").clone()
    }
}

fn header_name_to_python<'py>(py: Python<'py>, name: &RequestHeaderName) -> Bound<'py, PyBytes> {
    match name {
        RequestHeaderName::Known(name) => known_header_name_to_python(py, *name),
        RequestHeaderName::Other(name) => PyBytes::new(py, name.as_ref()),
    }
}

fn known_header_name_to_python(py: Python<'_>, name: KnownRequestHeaderName) -> Bound<'_, PyBytes> {
    py_match_cached_bytes!(
        py,
        name,
        {
            KnownRequestHeaderName::Host => b"host",
            KnownRequestHeaderName::Connection => b"connection",
            KnownRequestHeaderName::ProxyConnection => b"proxy-connection",
            KnownRequestHeaderName::KeepAlive => b"keep-alive",
            KnownRequestHeaderName::Upgrade => b"upgrade",
            KnownRequestHeaderName::Te => b"te",
            KnownRequestHeaderName::ContentLength => b"content-length",
            KnownRequestHeaderName::TransferEncoding => b"transfer-encoding",
            KnownRequestHeaderName::Expect => b"expect",
            KnownRequestHeaderName::Http2Settings => b"http2-settings",
            KnownRequestHeaderName::Forwarded => b"forwarded",
            KnownRequestHeaderName::XForwardedFor => b"x-forwarded-for",
            KnownRequestHeaderName::XForwardedProto => b"x-forwarded-proto",
            KnownRequestHeaderName::XForwardedHost => b"x-forwarded-host",
            KnownRequestHeaderName::XForwardedPort => b"x-forwarded-port",
            KnownRequestHeaderName::XForwardedPrefix => b"x-forwarded-prefix",
            KnownRequestHeaderName::SecWebSocketVersion => b"sec-websocket-version",
            KnownRequestHeaderName::SecWebSocketKey => b"sec-websocket-key",
            KnownRequestHeaderName::SecWebSocketProtocol => b"sec-websocket-protocol",
            KnownRequestHeaderName::SecWebSocketExtensions => b"sec-websocket-extensions",
        }
    )
}

fn scheme_to_python<'py>(py: Python<'py>, scheme: &str) -> Bound<'py, PyString> {
    py_match_cached_string!(py, scheme, ["http", "https", "ws", "wss"])
}

fn server_scope_value<'py>(
    py: Python<'py>,
    ctx: &RequestContext,
    server: (&str, Option<u16>),
) -> PyResult<Bound<'py, PyAny>> {
    if ctx.scope_overrides.server.is_some() {
        Ok(server.into_pyobject(py)?.into_any())
    } else {
        Ok(ctx.connection.default_server_scope_value(py))
    }
}

fn client_scope_value<'py>(
    py: Python<'py>,
    ctx: &RequestContext,
    client: Option<(&str, u16)>,
) -> PyResult<Option<Bound<'py, PyAny>>> {
    if ctx.scope_overrides.client.is_some() {
        Ok(Some(
            client
                .expect("client overrides should resolve to a client")
                .into_pyobject(py)?
                .into_any(),
        ))
    } else {
        Ok(ctx.connection.default_client_scope_value(py))
    }
}

fn raw_path_to_python<'py>(py: Python<'py>, raw_path: &str) -> Bound<'py, PyBytes> {
    py_match_cached_bytes!(py, raw_path, ["", "/"])
}

fn query_string_to_python<'py>(py: Python<'py>, query: &str) -> Bound<'py, PyBytes> {
    py_match_cached_bytes!(py, query, [""])
}

fn path_to_python<'py>(py: Python<'py>, path: &str) -> Bound<'py, PyString> {
    py_match_cached_string!(py, path, ["", "/"])
}

fn method_to_python<'py>(py: Python<'py>, method: &Method) -> Bound<'py, PyString> {
    py_match_cached_string!(py, method.as_str(), [
        "DELETE", "GET", "HEAD", "OPTIONS", "PATCH", "POST", "PUT",
    ])
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::num::NonZeroU32;
    use std::sync::Arc;
    use std::time::Duration;

    use http::Method;
    use pyo3::types::{PyAnyMethods, PyDictMethods};
    use pyo3::{PyResult, Python};
    use pyo3_async_runtimes::TaskLocals;
    use tokio::sync::watch;

    use super::{build_http_scope, decode_path};
    use crate::config::{
        BindTarget, Http1Config, Http2Config, ProxyConfig, ResponseHeaderConfig, ServerConfig,
        WebSocketConfig,
    };
    use crate::frame::DEFAULT_MAX_FRAME_SIZE;
    use crate::hpack::BytesStr;
    use crate::http::header_meta::RequestHeaderMeta;
    use crate::http::types::{HttpVersion, RequestHead, RequestTarget};
    use crate::proxy::{ClientAddr, ConnectionInfo, ConnectionPeer, ProxyProtocolMode, ServerAddr};
    use crate::runtime::{ConnectionContext, RequestContext, SharedApp, ShutdownState};

    fn init_python() {
        Python::initialize();
    }

    fn test_server_config() -> &'static ServerConfig {
        Box::leak(Box::new(ServerConfig {
            binds: Box::new([BindTarget::Tcp {
                host: Box::from("127.0.0.1"),
                port: 8000,
            }]),
            access_log: false,
            root_path: Box::from(""),
            http1: Http1Config {
                enabled: true,
                ..Default::default()
            },
            http2: Http2Config {
                max_concurrent_streams: 8,
                max_header_list_size: None,
                max_header_block_size: None,
                max_inbound_frame_size: NonZeroU32::new(DEFAULT_MAX_FRAME_SIZE as u32)
                    .expect("default HTTP/2 frame size is non-zero"),
            },
            max_request_body_size: None,
            timeout_graceful_shutdown: Duration::from_secs(30),
            timeout_keep_alive: None,
            timeout_request_header: None,
            timeout_request_body_idle: None,
            limit_concurrency: None,
            limit_connections: None,
            max_requests: None,
            runtime_threads: 2,
            websocket: WebSocketConfig::default(),
            proxy: ProxyConfig {
                trust_headers: false,
                trusted_peers: Box::new([]),
                protocol: ProxyProtocolMode::Off,
            },
            tls: None,
            timeout_handshake: Duration::from_secs(5),
            response_headers: ResponseHeaderConfig::default(),
        }))
    }

    fn test_connection(py: Python<'_>) -> ConnectionContext {
        let locals = TaskLocals::new(py.None().into_bound(py));
        let app = Arc::new(SharedApp {
            app: py.None(),
            locals,
            limits: None,
        });
        let info = Arc::new(ConnectionInfo::from_peer(
            ConnectionPeer::Tcp(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 54321)),
            Some(ServerAddr {
                host: "127.0.0.1".into(),
                port: Some(8000),
            }),
            false,
        ));
        let (_shutdown_tx, shutdown_rx) = watch::channel(ShutdownState::Running);
        ConnectionContext::new(app, test_server_config(), info, shutdown_rx)
    }

    fn test_request() -> RequestHead {
        RequestHead {
            http_version: HttpVersion::Http1_1,
            method: Method::GET,
            target: RequestTarget::normal(
                BytesStr::from_static("http"),
                BytesStr::from_static("/"),
            ),
            headers: Vec::new(),
            header_meta: RequestHeaderMeta::default(),
        }
    }

    #[test]
    fn http_scope_omits_empty_root_path_and_reuses_default_endpoint_objects() {
        init_python();
        Python::attach(|py| -> PyResult<()> {
            let connection = test_connection(py);
            let scope_one =
                build_http_scope(py, &RequestContext::new(connection.clone(), test_request()))?;
            let scope_two = build_http_scope(py, &RequestContext::new(connection, test_request()))?;

            assert_eq!(
                scope_one
                    .get_item("query_string")?
                    .expect("query_string exists")
                    .extract::<Vec<u8>>()?,
                Vec::<u8>::new()
            );
            assert!(scope_one.get_item("root_path")?.is_none());

            let server_one = scope_one.get_item("server")?.expect("server exists");
            let server_two = scope_two.get_item("server")?.expect("server exists");
            let client_one = scope_one.get_item("client")?.expect("client exists");
            let client_two = scope_two.get_item("client")?.expect("client exists");

            assert!(server_one.is(&server_two));
            assert!(client_one.is(&client_two));
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn http_scope_uses_overridden_endpoints_instead_of_cached_defaults() {
        init_python();
        Python::attach(|py| -> PyResult<()> {
            let connection = test_connection(py);
            let default_scope =
                build_http_scope(py, &RequestContext::new(connection.clone(), test_request()))?;

            let mut overridden = RequestContext::new(connection, test_request());
            overridden.scope_overrides.client = Some(ClientAddr {
                host: "10.0.0.9".into(),
                port: 9001,
            });
            overridden.scope_overrides.server = Some(ServerAddr {
                host: "edge.internal".into(),
                port: Some(8443),
            });

            let overridden_scope = build_http_scope(py, &overridden)?;
            let default_server = default_scope.get_item("server")?.expect("server exists");
            let default_client = default_scope.get_item("client")?.expect("client exists");
            let overridden_server = overridden_scope.get_item("server")?.expect("server exists");
            let overridden_client = overridden_scope.get_item("client")?.expect("client exists");

            assert!(!default_server.is(&overridden_server));
            assert!(!default_client.is(&overridden_client));
            assert_eq!(
                overridden_server.extract::<(String, Option<u16>)>()?,
                ("edge.internal".to_owned(), Some(8443)),
            );
            assert_eq!(
                overridden_client.extract::<(String, u16)>()?,
                ("10.0.0.9".to_owned(), 9001),
            );
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn decode_path_keeps_borrowed_input_without_valid_percent_escapes() {
        assert_eq!(decode_path("/demo%zz"), Cow::Borrowed("/demo%zz"));
        assert_eq!(decode_path("/demo%"), Cow::Borrowed("/demo%"));
        assert_eq!(
            decode_path("/demo%2Fok"),
            Cow::<'_, str>::Owned("/demo/ok".to_owned())
        );
    }
}
