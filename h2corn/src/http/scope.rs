mod forwarded;

use std::borrow::Cow;

use http::Method;
use memchr::memchr;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyBytes, PyDict, PyList, PyString};

use crate::ascii;
use crate::config::ServerConfig;
pub(crate) use crate::http::scope::forwarded::{
    ScopeHost, default_scope_view, resolve_scope_view, scope_view_with_defaults,
};
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
    let (view, defaults) =
        scope_view_with_defaults(request, &ctx.connection.config, &ctx.connection.info);
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
        // Omitted when empty, which the spec allows and every ASGI framework
        // already handles -- Starlette, FastAPI and Django all read it with
        // `scope.get("root_path", "")`. Emitting it anyway measured
        // +0.554% instructions/request (95% CI +0.500..+0.593), so the default
        // is served by not building it.
        if !view.root_path.is_empty() => {
            "root_path" => root_path_to_python(py, &ctx.connection.config, view.root_path.as_ref()),
        },
        "server" => server_scope_value(py, ctx, view.server, view.server != defaults.server)?,
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
        if let Some(client) = client_scope_value(py, ctx, view.client, view.client != defaults.client)? => {
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
/// only for what an application reads and never writes. That rules out the
/// `extensions` mapping itself, where an application may add its own
/// namespaced key — but not the per-extension *parameter* dicts inside it,
/// which are server-published capability metadata that nothing writes to.
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

/// The parameter mapping for an extension that has none.
///
/// Every advertised extension but `tls` publishes an empty one, so a request
/// was allocating one per extension — each a GC-tracked object built to be read
/// and thrown away. One shared dict serves them all, measured against a control
/// differing only in this:
///
/// - HTTP/1: 37,602 -> 37,377 instructions/request (**-0.585%**, 95% CI
///   -0.617..-0.552)
/// - HTTP/2: 51,829 -> 51,443 instructions/request (**-0.712%**, 95% CI
///   -0.831..-0.586)
///
/// HTTP/2 gains more because it advertises more extensions, which is the shape
/// the mechanism predicts.
///
/// They are capability metadata, not application storage: nothing in the ASGI
/// specification, in Starlette, FastAPI or Django writes to them, and the
/// convention that *does* involve writing —
/// `scope["extensions"]["application.private"]` — targets the mapping above,
/// which stays per-request for exactly that reason. A read-only view would
/// make a stray write impossible rather than merely unlikely, but `mappingproxy`
/// costs an indirection on every read and is not a `dict`, which is the wrong
/// trade for a mapping that is almost never read and never written.
fn empty_extension_params(py: Python<'_>) -> PyResult<Bound<'_, PyDict>> {
    static EMPTY: PyOnceLock<Py<PyDict>> = PyOnceLock::new();
    cached_dict(py, &EMPTY, || Ok(PyDict::new(py)))
}

fn http_scope_extensions<'py>(
    py: Python<'py>,
    ctx: &RequestContext,
) -> PyResult<Bound<'py, PyDict>> {
    // Deliberately per-request, unlike `asgi`: an application may write its own
    // namespaced key here (`scope["extensions"]["application.private"]`), and a
    // shared dict would carry that write into every later request. The
    // mappings *inside* it are shared, so this costs a dict and its entries,
    // not a tree of them; remeasure with `bench/instr.py` before quoting a
    // figure.
    //
    // Early hints are HTTP/2 only. RFC 8297 names interoperability and
    // security risks with HTTP/1 clients that mishandle an interim response,
    // and a pipelined peer that ignores one desynchronizes -- so the extension
    // is not offered where it cannot be delivered safely. `zerocopysend` spans
    // both protocol versions -- unlike early hints it is a body, not an interim
    // response -- but only on Unix, where the descriptor handling it needs
    // exists. Advertising it elsewhere would promise a capability the parser is
    // not even compiled with.
    //
    // Conditional entries rather than one arm per combination: `py_dict!`
    // sizes the dict from the entries actually pushed, so spelling the four
    // combinations out bought nothing but four copies of one list that could
    // drift apart.
    //
    // It is not quite free -- measured at +0.181% instructions/request (95% CI
    // +0.052..+0.373) against the spelled-out form, because unconditional
    // entries let the slot indices constant-fold while a conditional one makes
    // the write index a runtime value. Accepted deliberately: ~50 instructions
    // against four copies of a list, and a fraction of what sharing the
    // mappings below just returned.
    let extensions = py_dict!(py, {
        "http.response.pathsend" => empty_extension_params(py)?,
        if cfg!(unix) => {
            "http.response.zerocopysend" => empty_extension_params(py)?,
        },
        if ctx.request.accepts_trailers() => {
            "http.response.trailers" => empty_extension_params(py)?,
        },
        if ctx.request.http_version == HttpVersion::Http2 => {
            "http.response.early_hint" => empty_extension_params(py)?,
        },
    });
    add_tls_extension(py, ctx, &extensions)?;
    Ok(extensions)
}

fn websocket_scope_extensions<'py>(
    py: Python<'py>,
    ctx: &RequestContext,
) -> PyResult<Bound<'py, PyDict>> {
    let extensions = py_dict!(py, {
        "websocket.http.response" => empty_extension_params(py)?,
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
    static CACHED: PyOnceLock<[Py<PyBytes>; KnownRequestHeaderName::COUNT]> = PyOnceLock::new();
    CACHED.get_or_init(py, || {
        std::array::from_fn(|index| {
            PyBytes::new(py, KnownRequestHeaderName::ALL[index].as_bytes()).unbind()
        })
    })[name as usize]
        .bind(py)
        .clone()
}

fn scheme_to_python<'py>(py: Python<'py>, scheme: &str) -> Bound<'py, PyString> {
    py_match_cached_string!(py, scheme, ["http", "https", "ws", "wss"])
}

fn server_scope_value<'py>(
    py: Python<'py>,
    ctx: &RequestContext,
    server: (ScopeHost<'_>, Option<u16>),
    overridden: bool,
) -> PyResult<Bound<'py, PyAny>> {
    if overridden {
        server
            .0
            .with_text(|host| Ok((host, server.1).into_pyobject(py)?.into_any()))
    } else {
        Ok(ctx.connection.default_server_scope_value(py))
    }
}

fn client_scope_value<'py>(
    py: Python<'py>,
    ctx: &RequestContext,
    client: Option<(ScopeHost<'_>, u16)>,
    overridden: bool,
) -> PyResult<Option<Bound<'py, PyAny>>> {
    if overridden {
        let (host, port) = client.expect("a changed client endpoint must exist");
        host.with_text(|host| Ok(Some((host, port).into_pyobject(py)?.into_any())))
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

    use bytes::Bytes;
    use http::Method;
    use pyo3::types::{PyAnyMethods as _, PyBytesMethods as _, PyDict, PyDictMethods as _};
    use pyo3::{PyResult, Python};

    use super::{build_http_scope, build_websocket_scope, decode_path};
    use crate::config::ServerConfig;
    use crate::http::header_meta::RequestHeaderMeta;
    use crate::http::types::{
        BytesStr, H1RequestHeaders, HttpVersion, KnownRequestHeaderName, RequestHead,
        RequestHeaders, RequestTarget,
    };
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
    fn configured_root_path_is_built_once() {
        init_python();
        Python::attach(|py| -> PyResult<()> {
            let config = ServerConfig {
                root_path: Box::from("/api"),
                ..test_fixtures::server_config_parts()
            };
            let connection = test_fixtures::connection_context_with(py, Arc::new(config));

            let scope_one = {
                let request = RequestContext::new(Arc::clone(&connection), test_request());
                build_http_scope(py, &request)?
            };
            let scope_two = {
                let request = RequestContext::new(Arc::clone(&connection), test_request());
                build_http_scope(py, &request)?
            };
            drop(connection);

            let root_one = scope_one.get_item("root_path")?.expect("root_path exists");
            let root_two = scope_two.get_item("root_path")?.expect("root_path exists");

            assert_eq!(root_one.extract::<String>()?, "/api");
            assert!(
                root_one.is(&root_two),
                "the configured root path is the same string every request"
            );
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn known_header_names_share_one_exhaustive_python_bytes_cache() {
        init_python();
        Python::attach(|py| -> PyResult<()> {
            assert_eq!(
                KnownRequestHeaderName::ALL.len(),
                KnownRequestHeaderName::COUNT,
                "the generated vocabulary fixes the cache length"
            );
            for (index, name) in KnownRequestHeaderName::ALL.iter().enumerate() {
                assert_eq!(
                    *name as usize, index,
                    "the generated enum remains densely indexable"
                );
                let first = super::known_header_name_to_python(py, *name);
                let second = super::known_header_name_to_python(py, *name);
                assert_eq!(first.as_bytes(), name.as_bytes());
                assert!(
                    first.is(&second),
                    "every known name reuses its one cached Python bytes object"
                );
            }
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn http_scope_omits_empty_root_path_and_reuses_default_endpoint_objects() {
        init_python();
        Python::attach(|py| -> PyResult<()> {
            let request_one = RequestContext::new(test_connection(py), test_request());
            let scope_one = build_http_scope(py, &request_one)?;
            let request_two =
                RequestContext::new(Arc::clone(&request_one.connection), test_request());
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
    fn scope_keeps_untrusted_proxy_headers_in_the_asgi_header_list() {
        init_python();
        Python::attach(|py| -> PyResult<()> {
            let raw = Bytes::from_static(b"198.51.100.9");
            let mut headers = H1RequestHeaders::new(raw.clone());
            headers
                .push(b"x-forwarded-for", raw.as_ref())
                .expect("header is valid");
            let request = RequestHead {
                http_version: HttpVersion::Http1_1,
                method: Method::GET,
                target: RequestTarget::normal(
                    BytesStr::from_static("http"),
                    BytesStr::from_static("/"),
                ),
                headers: RequestHeaders::from_h1(headers),
                header_meta: RequestHeaderMeta::default(),
            };
            let request = RequestContext::new(test_connection(py), request);
            let scope = build_http_scope(py, &request)?;
            drop(request);
            let headers = scope
                .get_item("headers")?
                .expect("ASGI headers are present")
                .extract::<Vec<(Vec<u8>, Vec<u8>)>>()?;

            assert!(headers.contains(&(b"x-forwarded-for".to_vec(), b"198.51.100.9".to_vec())));
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn http_scope_shares_asgi_metadata_and_isolates_extensions() {
        init_python();
        Python::attach(|py| -> PyResult<()> {
            let request_one = RequestContext::new(test_connection(py), test_request());
            let request_two =
                RequestContext::new(Arc::clone(&request_one.connection), test_request());
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
            // A parameterless extension's params are one shared dict. Nothing
            // writes to capability metadata, so rebuilding it per request paid
            // for isolation nobody used.
            assert!(pathsend_one.is(&pathsend_two));

            extensions_one.set_item("application.private", true)?;

            assert_eq!(
                asgi_two
                    .get_item("version")?
                    .expect("ASGI version exists")
                    .extract::<&str>()?,
                "3.0",
            );
            assert!(extensions_two.get_item("application.private")?.is_none());
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn websocket_scope_extension_dicts_are_isolated_per_request() {
        init_python();
        Python::attach(|py| -> PyResult<()> {
            let request_one = RequestContext::new(test_connection(py), test_request());
            let request_two =
                RequestContext::new(Arc::clone(&request_one.connection), test_request());
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
            // Shared, like the HTTP side: these carry no parameters and nothing
            // writes to them.
            assert!(response_one.is(&response_two));
            extensions_one.set_item("application.private", true)?;
            assert!(extensions_two.get_item("application.private")?.is_none());
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
                drop(request);

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
