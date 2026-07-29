mod forwarded;

use std::borrow::Cow;

pub(crate) use forwarded::{ScopeOverrides, resolve_scope_overrides, scope_view_from_parts};
use http::Method;
use memchr::memchr;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyBytes, PyDict, PyList, PyString};

use crate::ascii;
use crate::config::ServerConfig;
use crate::http::types::{
    BytesStr, HttpVersion, KnownRequestHeaderName, RequestHeaderNameRef, RequestHeaders,
};
use crate::python::{py_dict, py_match_cached_bytes, py_match_cached_string};
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

pub(crate) fn build_http_scope<'py>(
    py: Python<'py>,
    ctx: &RequestContext,
) -> PyResult<Bound<'py, PyDict>> {
    build_base_scope::<true>(py, ctx, http_scope_extensions(py, ctx)?, &[])
}

pub(crate) fn build_websocket_scope<'py>(
    py: Python<'py>,
    ctx: &RequestContext,
    requested_subprotocols: &[BytesStr],
) -> PyResult<Bound<'py, PyDict>> {
    build_base_scope::<false>(
        py,
        ctx,
        websocket_scope_extensions(py, ctx)?,
        requested_subprotocols,
    )
}

fn build_base_scope<'py, const IS_HTTP: bool>(
    py: Python<'py>,
    ctx: &RequestContext,
    extensions: Bound<'py, PyDict>,
    websocket_subprotocols: &[BytesStr],
) -> PyResult<Bound<'py, PyDict>> {
    let request = &ctx.request;
    let view = scope_view_from_parts(
        request.scheme().map_or("", BytesStr::as_str),
        &ctx.connection.config,
        &ctx.connection.info,
        ctx.scope_overrides.as_deref(),
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
        "http_version" => match request.http_version {
            HttpVersion::Http1_1 => intern!(py, "1.1"),
            HttpVersion::Http2 => intern!(py, "2"),
        },
        "scheme" => scheme_to_python(py, resolved_scheme),
        "raw_path" => raw_path_to_python(py, raw_path),
        "path" => path_to_python(py, path.as_ref()),
        "query_string" => query_string_to_python(py, query),
        if !view.root_path.is_empty() => {
            "root_path" => root_path_to_python(py, &ctx.connection.config, view.root_path.as_ref()),
        },
        "server" => server_scope_value(py, ctx, view.server)?,
        "headers" => headers_to_python(py, &request.headers)?,
        "extensions" => extensions,
        if !IS_HTTP => {
            "subprotocols" => PyList::new(
                py,
                websocket_subprotocols.iter().map(BytesStr::as_str),
            )?,
        },
        if IS_HTTP => {
            "method" => method_to_python(py, &request.method),
        },
        if let Some(client) = client_scope_value(py, ctx, view.client)? => {
            "client" => client,
        },
    }))
}

/// Cache one dict per known shape and hand the same object to every scope.
///
/// Rebuilding the ASGI version dict per request cost a measured 599
/// instructions — 1.75 % of this server's per-request work — to produce a value
/// identical every time.
///
/// The trade is that a mutation would be seen by every later scope, so this is
/// only for what an application reads and never writes — which rules out
/// `extensions`, where an application may add its own namespaced key.
fn cached_dict<'py>(
    py: Python<'py>,
    cell: &'static PyOnceLock<Py<PyDict>>,
    build: impl FnOnce() -> PyResult<Bound<'py, PyDict>>,
) -> PyResult<Bound<'py, PyDict>> {
    Ok(cell
        .get_or_try_init(py, || -> PyResult<Py<PyDict>> { Ok(build()?.unbind()) })?
        .bind(py)
        .clone())
}

fn http_scope_extensions<'py>(
    py: Python<'py>,
    ctx: &RequestContext,
) -> PyResult<Bound<'py, PyDict>> {
    // Deliberately per-request, unlike `asgi`: an application may write its own
    // namespaced key here (`scope["extensions"]["application.private"]`), and a
    // shared dict would carry that write into every later request. Worth the
    // measured 525 instructions.
    let extensions = if ctx.request.accepts_trailers() {
        py_dict!(py, {
            "http.response.pathsend" => py_dict!(py, {}),
            "http.response.trailers" => py_dict!(py, {}),
        })
    } else {
        py_dict!(py, {
            "http.response.pathsend" => py_dict!(py, {}),
        })
    };
    add_tls_extension(py, ctx, &extensions)?;
    Ok(extensions)
}

fn websocket_scope_extensions<'py>(
    py: Python<'py>,
    ctx: &RequestContext,
) -> PyResult<Bound<'py, PyDict>> {
    let extensions = py_dict!(py, {
        "websocket.http.response" => py_dict!(py, {}),
    });
    add_tls_extension(py, ctx, &extensions)?;
    Ok(extensions)
}

/// Add `tls` when the connection has one — the extension requires the key to
/// be absent, not empty, on a connection that is not TLS.
fn add_tls_extension(
    py: Python<'_>,
    ctx: &RequestContext,
    extensions: &Bound<'_, PyDict>,
) -> PyResult<()> {
    if let Some(tls) = ctx.connection.tls_scope_extension(py)? {
        extensions.set_item(pyo3::intern!(py, "tls"), tls)?;
    }
    Ok(())
}

fn asgi_scope_dict(py: Python<'_>) -> PyResult<Bound<'_, PyDict>> {
    static ASGI: PyOnceLock<Py<PyDict>> = PyOnceLock::new();
    cached_dict(py, &ASGI, || {
        Ok(py_dict!(py, {
            "version" => "3.0",
            "spec_version" => "2.5",
        }))
    })
}

pub(crate) fn headers_to_python<'py>(
    py: Python<'py>,
    headers: &RequestHeaders,
) -> PyResult<Bound<'py, PyList>> {
    // `PyList::new` over an exact-size iterator compiles to the same
    // `PyList_New` + `PyList_SET_ITEM` / `PyTuple_New` + `PyTuple_SET_ITEM`
    // sequence a hand-rolled fill would use.
    PyList::new(
        py,
        headers.iter().map(|header| {
            (
                header_name_to_python(py, header.name()),
                PyBytes::new(py, header.value()),
            )
        }),
    )
}

/// The configured root path is the same string on every request under a
/// mounted application, so it is built once; a forwarded prefix is not, and is
/// built per request.
///
/// Identity, not equality, decides: the view borrows the configuration's own
/// string unless a trusted proxy replaced it, and comparing the pointers says
/// which happened without walking the bytes.
fn root_path_to_python<'py>(
    py: Python<'py>,
    config: &ServerConfig,
    root_path: &str,
) -> Bound<'py, PyString> {
    if !std::ptr::eq(root_path, config.root_path.as_ref()) {
        return PyString::new(py, root_path);
    }
    config
        .root_path_scope
        .get_or_init(py, || PyString::new(py, root_path).unbind())
        .bind(py)
        .clone()
}

fn scope_type_to_python<const IS_HTTP: bool>(py: Python<'_>) -> Bound<'_, PyString> {
    if IS_HTTP {
        intern!(py, "http").clone()
    } else {
        intern!(py, "websocket").clone()
    }
}

fn header_name_to_python<'py>(
    py: Python<'py>,
    name: RequestHeaderNameRef<'_>,
) -> Bound<'py, PyBytes> {
    match name {
        RequestHeaderNameRef::Known(name) => known_header_name_to_python(py, name),
        RequestHeaderNameRef::Other(name) => PyBytes::new(py, name.as_bytes()),
    }
}

fn known_header_name_to_python(py: Python<'_>, name: KnownRequestHeaderName) -> Bound<'_, PyBytes> {
    py_match_cached_bytes!(
        py,
        name,
        {
            KnownRequestHeaderName::Accept => b"accept",
            KnownRequestHeaderName::AcceptEncoding => b"accept-encoding",
            KnownRequestHeaderName::AcceptLanguage => b"accept-language",
            KnownRequestHeaderName::Authorization => b"authorization",
            KnownRequestHeaderName::CacheControl => b"cache-control",
            KnownRequestHeaderName::Host => b"host",
            KnownRequestHeaderName::Connection => b"connection",
            KnownRequestHeaderName::ContentType => b"content-type",
            KnownRequestHeaderName::Cookie => b"cookie",
            KnownRequestHeaderName::ProxyConnection => b"proxy-connection",
            KnownRequestHeaderName::KeepAlive => b"keep-alive",
            KnownRequestHeaderName::Upgrade => b"upgrade",
            KnownRequestHeaderName::UserAgent => b"user-agent",
            KnownRequestHeaderName::Te => b"te",
            KnownRequestHeaderName::ContentLength => b"content-length",
            KnownRequestHeaderName::TransferEncoding => b"transfer-encoding",
            KnownRequestHeaderName::Expect => b"expect",
            KnownRequestHeaderName::Http2Settings => b"http2-settings",
            KnownRequestHeaderName::IfModifiedSince => b"if-modified-since",
            KnownRequestHeaderName::IfNoneMatch => b"if-none-match",
            KnownRequestHeaderName::Origin => b"origin",
            KnownRequestHeaderName::Pragma => b"pragma",
            KnownRequestHeaderName::Referer => b"referer",
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
    if ctx
        .scope_overrides
        .as_deref()
        .is_some_and(|overrides| overrides.server.is_some())
    {
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
    if ctx
        .scope_overrides
        .as_deref()
        .is_some_and(|overrides| overrides.client.is_some())
    {
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
    use std::sync::Arc;

    use http::Method;
    use pyo3::types::{PyAnyMethods, PyDict, PyDictMethods};
    use pyo3::{PyResult, Python};

    use super::{build_http_scope, build_websocket_scope, decode_path};
    use crate::config::ServerConfig;
    use crate::http::header_meta::RequestHeaderMeta;
    use crate::http::types::{BytesStr, HttpVersion, RequestHead, RequestHeaders, RequestTarget};
    use crate::proxy_protocol::{ClientAddr, ServerAddr};
    use crate::runtime::{ConnectionContext, RequestContext, test_fixtures};

    fn init_python() {
        Python::initialize();
    }

    fn test_connection(py: Python<'_>) -> ConnectionContext {
        test_fixtures::connection_context(py)
    }

    fn test_request() -> RequestHead {
        RequestHead {
            http_version: HttpVersion::Http1_1,
            method: Method::GET,
            target: RequestTarget::normal(
                BytesStr::from_static("http"),
                BytesStr::from_static("/"),
            ),
            headers: RequestHeaders::default(),
            header_meta: RequestHeaderMeta::default(),
        }
    }

    #[test]
    fn configured_root_path_is_built_once_and_a_forwarded_prefix_is_not() {
        init_python();
        Python::attach(|py| -> PyResult<()> {
            let config = ServerConfig {
                root_path: Box::from("/api"),
                ..test_fixtures::server_config_parts()
            };
            let connection = test_fixtures::connection_context_with(py, Arc::new(config));

            let scope_one = {
                let request = RequestContext::new(connection.clone(), test_request());
                build_http_scope(py, &request)?
            };
            let scope_two = {
                let request = RequestContext::new(connection.clone(), test_request());
                build_http_scope(py, &request)?
            };
            // A forwarded prefix is a different value per request and must not
            // come from the cache.
            let scope_three = {
                let mut overridden = RequestContext::new(connection.clone(), test_request());
                overridden
                    .scope_overrides
                    .get_or_insert_with(Box::default)
                    .root_path = Some(Box::from("/api/edge"));
                build_http_scope(py, &overridden)?
            };
            drop(connection);

            let root_one = scope_one.get_item("root_path")?.expect("root_path exists");
            let root_two = scope_two.get_item("root_path")?.expect("root_path exists");
            let root_three = scope_three
                .get_item("root_path")?
                .expect("root_path exists");

            assert_eq!(root_one.extract::<String>()?, "/api");
            assert!(
                root_one.is(&root_two),
                "the configured root path is the same string every request"
            );
            assert_eq!(root_three.extract::<String>()?, "/api/edge");
            assert!(!root_three.is(&root_one));
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn http_scope_omits_empty_root_path_and_reuses_default_endpoint_objects() {
        init_python();
        Python::attach(|py| -> PyResult<()> {
            let request_one = RequestContext::new(test_connection(py), test_request());
            assert!(request_one.scope_overrides.is_none());
            let scope_one = build_http_scope(py, &request_one)?;
            let request_two = RequestContext::new(request_one.connection.clone(), test_request());
            let scope_two = build_http_scope(py, &request_two)?;
            drop(request_one);
            drop(request_two);

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
            let default_request = RequestContext::new(test_connection(py), test_request());
            let default_scope = build_http_scope(py, &default_request)?;

            let mut overridden =
                RequestContext::new(default_request.connection.clone(), test_request());
            drop(default_request);
            let overrides = overridden.scope_overrides.get_or_insert_with(Box::default);
            overrides.client = Some(ClientAddr {
                host: "10.0.0.9".into(),
                port: 9001,
            });
            overrides.server = Some(ServerAddr {
                host: "edge.internal".into(),
                port: Some(8443),
            });

            let overridden_scope = build_http_scope(py, &overridden)?;
            drop(overridden);
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
    fn http_scope_shares_asgi_metadata_and_isolates_extensions() {
        init_python();
        Python::attach(|py| -> PyResult<()> {
            let request_one = RequestContext::new(test_connection(py), test_request());
            let request_two = RequestContext::new(request_one.connection.clone(), test_request());
            let scope_one = build_http_scope(py, &request_one)?;
            let scope_two = build_http_scope(py, &request_two)?;
            drop(request_one);
            drop(request_two);

            let asgi_one = scope_one
                .get_item("asgi")?
                .expect("asgi exists")
                .cast_into::<PyDict>()?;
            let asgi_two = scope_two
                .get_item("asgi")?
                .expect("asgi exists")
                .cast_into::<PyDict>()?;
            let extensions_one = scope_one
                .get_item("extensions")?
                .expect("extensions exists")
                .cast_into::<PyDict>()?;
            let extensions_two = scope_two
                .get_item("extensions")?
                .expect("extensions exists")
                .cast_into::<PyDict>()?;
            let pathsend_one = extensions_one
                .get_item("http.response.pathsend")?
                .expect("pathsend extension exists")
                .cast_into::<PyDict>()?;
            let pathsend_two = extensions_two
                .get_item("http.response.pathsend")?
                .expect("pathsend extension exists")
                .cast_into::<PyDict>()?;

            // `asgi` is constant metadata and is deliberately one shared
            // object; `extensions` stays per-request, because an application
            // may write its own keys there.
            assert!(asgi_one.is(&asgi_two));
            assert!(!extensions_one.is(&extensions_two));
            assert!(!pathsend_one.is(&pathsend_two));

            extensions_one.set_item("application.private", true)?;
            pathsend_one.set_item("application.private", true)?;

            assert_eq!(
                asgi_two
                    .get_item("version")?
                    .expect("ASGI version exists")
                    .extract::<&str>()?,
                "3.0",
            );
            assert!(extensions_two.get_item("application.private")?.is_none());
            assert!(pathsend_two.get_item("application.private")?.is_none());
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn websocket_scope_extension_dicts_are_isolated_per_request() {
        init_python();
        Python::attach(|py| -> PyResult<()> {
            let request_one = RequestContext::new(test_connection(py), test_request());
            let request_two = RequestContext::new(request_one.connection.clone(), test_request());
            let scope_one = build_websocket_scope(py, &request_one, &[])?;
            let scope_two = build_websocket_scope(py, &request_two, &[])?;
            drop(request_one);
            drop(request_two);

            let extensions_one = scope_one
                .get_item("extensions")?
                .expect("extensions exists")
                .cast_into::<PyDict>()?;
            let extensions_two = scope_two
                .get_item("extensions")?
                .expect("extensions exists")
                .cast_into::<PyDict>()?;
            let response_one = extensions_one
                .get_item("websocket.http.response")?
                .expect("HTTP response extension exists")
                .cast_into::<PyDict>()?;
            let response_two = extensions_two
                .get_item("websocket.http.response")?
                .expect("HTTP response extension exists")
                .cast_into::<PyDict>()?;

            assert!(!extensions_one.is(&extensions_two));
            assert!(!response_one.is(&response_two));
            extensions_one.set_item("application.private", true)?;
            response_one.set_item("application.private", true)?;
            assert!(extensions_two.get_item("application.private")?.is_none());
            assert!(response_two.get_item("application.private")?.is_none());
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn websocket_scope_always_exposes_http_version_and_subprotocols() {
        init_python();
        Python::attach(|py| -> PyResult<()> {
            for (http_version, expected) in
                [(HttpVersion::Http1_1, "1.1"), (HttpVersion::Http2, "2")]
            {
                let mut request = test_request();
                request.http_version = http_version;
                let request = RequestContext::new(test_connection(py), request);
                let scope = build_websocket_scope(py, &request, &[])?;

                assert_eq!(
                    scope
                        .get_item("http_version")?
                        .expect("http_version exists")
                        .extract::<&str>()?,
                    expected,
                );
                assert_eq!(
                    scope
                        .get_item("subprotocols")?
                        .expect("subprotocols exists")
                        .extract::<Vec<String>>()?,
                    Vec::<String>::new(),
                );
            }
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
