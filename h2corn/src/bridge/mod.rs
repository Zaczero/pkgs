mod http;
mod websocket;

use std::future;
use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
pub use http::{PyHttpReceive, PyHttpSend};
use pyo3::exceptions::{PyRuntimeError, PyStopIteration, PyTypeError};
use pyo3::prelude::*;
use pyo3::pybacked::{PyBackedBytes, PyBackedStr};
use pyo3::types::{PyBool, PyByteArray, PyBytes, PyDict, PyList, PyString, PyTuple};
use pyo3::{PyTypeCheck, PyTypeInfo};
use pyo3_async_runtimes::TaskLocals;
use pyo3_async_runtimes::tokio::future_into_py_with_locals;
use tokio::sync::{Mutex, mpsc};
#[cfg(test)]
pub use websocket::WebSocketSendDisposition;
pub use websocket::{PyWebSocketReceive, PyWebSocketSend, WebSocketSendBuffer, WebSocketSendState};

use crate::async_util::{TryPush, try_push};
use crate::error::{
    AsgiContainer, AsgiError, ErrorExt, H2CornError, HttpResponseError, into_pyerr,
};
use crate::hpack::BytesStr;
use crate::http::types::{
    HttpStatusCode, ResponseHeaderName, ResponseHeaderValue, ResponseHeaders,
};
use crate::python::{StaticPyKey, py_dict};
use crate::websocket::WebSocketCloseCode;

macro_rules! asgi_item {
    ($fn:ident, $name:literal) => {
        fn $fn(&self) -> Result<Option<Bound<'py, PyAny>>, H2CornError> {
            static KEY: StaticPyKey = StaticPyKey::new($name);
            KEY.get_item(self.dict.py(), self.dict)
                .map_err(H2CornError::from)
        }
    };
}

pub const ASGI_QUEUE_CAPACITY: usize = 32;

#[derive(Debug)]
pub enum HttpInboundEvent {
    Request { body: Bytes, more_body: bool },
    HttpDisconnect,
}

#[derive(Debug)]
pub enum WebSocketInboundEvent {
    Connect,
    ReceiveBytes(Bytes),
    ReceiveText(BytesStr),
    Disconnect {
        code: WebSocketCloseCode,
        reason: Option<BytesStr>,
    },
}

#[derive(Debug)]
pub enum HttpOutboundEvent {
    Start {
        status: HttpStatusCode,
        headers: ResponseHeaders,
        trailers: bool,
    },
    Body {
        body: PayloadBytes,
        more_body: bool,
    },
    PathSend {
        path: PathBuf,
    },
    Trailers {
        headers: ResponseHeaders,
        more_trailers: bool,
    },
}

#[derive(Debug)]
pub enum WebSocketOutboundEvent {
    Accept {
        subprotocol: Option<PyBackedStr>,
        headers: ResponseHeaders,
    },
    SendBytes(PayloadBytes),
    SendText(PyBackedStr),
    Close {
        code: WebSocketCloseCode,
        reason: Option<PyBackedStr>,
    },
    HttpResponseStart {
        status: HttpStatusCode,
        headers: ResponseHeaders,
    },
    HttpResponseBody {
        body: PayloadBytes,
        more_body: bool,
    },
}

#[derive(Debug)]
pub enum PayloadBytes {
    Rust(Bytes),
    Python(PyBackedBytes),
}

impl PayloadBytes {
    pub(crate) fn len(&self) -> usize {
        self.as_slice().len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.as_slice().is_empty()
    }

    pub(crate) fn as_slice(&self) -> &[u8] {
        match self {
            Self::Rust(bytes) => bytes.as_ref(),
            Self::Python(bytes) => bytes.as_ref(),
        }
    }
}

impl AsRef<[u8]> for PayloadBytes {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl From<Bytes> for PayloadBytes {
    fn from(value: Bytes) -> Self {
        Self::Rust(value)
    }
}

impl From<PyBackedBytes> for PayloadBytes {
    fn from(value: PyBackedBytes) -> Self {
        Self::Python(value)
    }
}

type ParsedHeaderPair = (ResponseHeaderName, ResponseHeaderValue);

struct AsgiMessage<'py> {
    dict: &'py Bound<'py, PyDict>,
    message_type: Bound<'py, PyString>,
}

enum WebSocketSendPayload {
    Text(PyBackedStr),
    Bytes(PayloadBytes),
}

impl<'py> AsgiMessage<'py> {
    asgi_item!(headers_item, "headers");
    asgi_item!(trailers_item, "trailers");
    asgi_item!(body_item, "body");
    asgi_item!(more_body_item, "more_body");
    asgi_item!(path_item, "path");
    asgi_item!(more_trailers_item, "more_trailers");
    asgi_item!(subprotocol_item, "subprotocol");
    asgi_item!(text_item, "text");
    asgi_item!(bytes_item, "bytes");
    asgi_item!(code_item, "code");
    asgi_item!(reason_item, "reason");
    asgi_item!(status_item, "status");

    fn parse(dict: &'py Bound<'py, PyDict>) -> Result<Self, H2CornError> {
        Ok(Self {
            dict,
            message_type: {
                static KEY: StaticPyKey = StaticPyKey::new("type");
                KEY.get_item(dict.py(), dict).map_err(H2CornError::from)?
            }
            .ok_or_else(|| AsgiError::missing_field(AsgiContainer::Message, "type").into_error())
            .and_then(|value| cast_into_exact_first::<PyString>(value))?,
        })
    }

    fn message_type(&self) -> Result<&str, H2CornError> {
        self.message_type.to_str().map_err(H2CornError::from)
    }

    fn require(
        container: AsgiContainer,
        field: &'static str,
        value: Option<Bound<'py, PyAny>>,
    ) -> Result<Bound<'py, PyAny>, H2CornError> {
        value.ok_or_else(|| AsgiError::missing_field(container, field).into_error())
    }

    fn optional_item(value: Option<Bound<'py, PyAny>>) -> Option<Bound<'py, PyAny>> {
        value.filter(|value| !value.is_none())
    }

    fn bool_or_false(value: Option<Bound<'py, PyAny>>) -> Result<bool, H2CornError> {
        value.map_or(Ok(false), |value| {
            cast_exact_first::<PyBool>(&value).map_or_else(
                |_| value.extract::<bool>().map_err(H2CornError::from),
                |value| Ok(value.is_true()),
            )
        })
    }

    fn payload_bytes_or_empty(
        value: Option<Bound<'py, PyAny>>,
    ) -> Result<PayloadBytes, H2CornError> {
        value.map_or_else(
            || Ok(PayloadBytes::from(Bytes::new())),
            |value| extract_payload_bytes(&value),
        )
    }

    fn headers(&self) -> Result<ResponseHeaders, H2CornError> {
        parse_headers(self.headers_item()?)
    }

    fn status(&self, container: AsgiContainer) -> Result<u16, H2CornError> {
        Self::require(container, "status", self.status_item()?)?
            .extract::<u16>()
            .map_err(H2CornError::from)
    }

    fn trailers_flag(&self) -> Result<bool, H2CornError> {
        Self::bool_or_false(self.trailers_item()?)
    }

    fn body_or_empty(&self) -> Result<PayloadBytes, H2CornError> {
        Self::payload_bytes_or_empty(self.body_item()?)
    }

    fn more_body_flag(&self) -> Result<bool, H2CornError> {
        Self::bool_or_false(self.more_body_item()?)
    }

    fn path(&self, container: AsgiContainer) -> Result<PathBuf, H2CornError> {
        Self::require(container, "path", self.path_item()?)?
            .extract::<&str>()
            .map(PathBuf::from)
            .map_err(H2CornError::from)
    }

    fn more_trailers_flag(&self) -> Result<bool, H2CornError> {
        Self::bool_or_false(self.more_trailers_item()?)
    }

    fn optional_backed_str(
        value: Option<Bound<'py, PyAny>>,
    ) -> Result<Option<PyBackedStr>, H2CornError> {
        Self::optional_item(value)
            .map(|value| extract_backed_str(&value))
            .transpose()
    }

    fn subprotocol(&self) -> Result<Option<PyBackedStr>, H2CornError> {
        Self::optional_backed_str(self.subprotocol_item()?)
    }

    fn close_code_or_default(&self) -> Result<u16, H2CornError> {
        Self::optional_item(self.code_item()?).map_or(Ok(1000_u16), |value| {
            value.extract::<u16>().map_err(H2CornError::from)
        })
    }

    fn reason(&self) -> Result<Option<PyBackedStr>, H2CornError> {
        Self::optional_backed_str(self.reason_item()?)
    }

    fn websocket_send_payload(&self) -> Result<WebSocketSendPayload, H2CornError> {
        match (
            Self::optional_item(self.text_item()?),
            Self::optional_item(self.bytes_item()?),
        ) {
            (Some(value), None) => Ok(WebSocketSendPayload::Text(extract_backed_str(&value)?)),
            (None, Some(value)) => Ok(WebSocketSendPayload::Bytes(extract_payload_bytes(&value)?)),
            _ => AsgiError::WebSocketSendRequiresExactlyOnePayload.err(),
        }
    }
}

#[pyclass(freelist = 512)]
struct ReadyAwaitable {
    result: Option<Py<PyAny>>,
}

impl ReadyAwaitable {
    fn next_result(&mut self) -> PyResult<Py<PyAny>> {
        self.result.take().map_or_else(
            || Err(PyRuntimeError::new_err("awaitable was already awaited")),
            |result| Err(PyStopIteration::new_err((result,))),
        )
    }
}

#[pymethods]
impl ReadyAwaitable {
    fn send(&mut self, _py: Python<'_>, _value: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        self.next_result()
    }

    fn close(&mut self) {
        self.result = None;
    }

    const fn __await__(self_: Py<Self>) -> Py<Self> {
        self_
    }

    const fn __iter__(self_: Py<Self>) -> Py<Self> {
        self_
    }

    fn __next__(&mut self, _py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.next_result()
    }
}

pub trait ReceiveStateMachine: Send + 'static {
    type Event: Send + 'static;

    fn try_next(&mut self) -> Option<Self::Event>;

    fn next(&mut self) -> impl future::Future<Output = Self::Event> + Send + '_;
}

pub fn ready_awaitable(py: Python<'_>, result: Py<PyAny>) -> PyResult<Bound<'_, PyAny>> {
    Ok(Py::new(py, ReadyAwaitable {
        result: Some(result),
    })?
    .into_bound(py)
    .into_any())
}

pub fn buffered_or_send<'py, T: Send + 'static>(
    py: Python<'py>,
    locals: &TaskLocals,
    forwarded: Option<(mpsc::Sender<T>, T)>,
) -> PyResult<Bound<'py, PyAny>> {
    forwarded.map_or_else(
        || ready_awaitable(py, py.None()),
        |(tx, event)| try_send_or_await(py, locals, &tx, event),
    )
}

pub fn receive_or_await<'py, S>(
    py: Python<'py>,
    locals: &TaskLocals,
    state: &Arc<Mutex<S>>,
    build_event: fn(Python<'_>, S::Event) -> PyResult<Py<PyAny>>,
) -> PyResult<Bound<'py, PyAny>>
where
    S: ReceiveStateMachine,
{
    if let Ok(mut state) = state.try_lock()
        && let Some(event) = state.try_next()
    {
        return ready_awaitable(py, build_event(py, event)?);
    }

    let locals = locals.clone();
    let state = Arc::clone(state);
    future_into_py_with_locals(py, locals, async move {
        let event = state.lock().await.next().await;
        Python::attach(|py| build_event(py, event))
    })
}

pub fn build_http_inbound_event(py: Python<'_>, event: HttpInboundEvent) -> PyResult<Py<PyAny>> {
    let dict = match event {
        HttpInboundEvent::Request { body, more_body } => py_dict!(py, {
            "type" => "http.request",
            if !body.is_empty() => {
                "body" => PyBytes::new(py, body.as_ref()),
            },
            if more_body => {
                "more_body" => true,
            },
        }),
        HttpInboundEvent::HttpDisconnect => py_dict!(py, {
            "type" => "http.disconnect",
        }),
    };
    Ok(dict.into_any().unbind())
}

pub fn build_websocket_inbound_event(
    py: Python<'_>,
    event: WebSocketInboundEvent,
) -> PyResult<Py<PyAny>> {
    let dict = match event {
        WebSocketInboundEvent::Connect => py_dict!(py, {
            "type" => "websocket.connect",
        }),
        WebSocketInboundEvent::ReceiveBytes(body) => py_dict!(py, {
            "type" => "websocket.receive",
            "bytes" => PyBytes::new(py, body.as_ref()),
        }),
        WebSocketInboundEvent::ReceiveText(text) => py_dict!(py, {
            "type" => "websocket.receive",
            "text" => text.as_str(),
        }),
        WebSocketInboundEvent::Disconnect { code, reason } => py_dict!(py, {
            "type" => "websocket.disconnect",
            "code" => code,
            if let Some(reason) = reason.filter(|reason| !reason.is_empty()) => {
                "reason" => reason.as_str(),
            },
        }),
    };
    Ok(dict.into_any().unbind())
}

fn parse_headers(value: Option<Bound<'_, PyAny>>) -> Result<ResponseHeaders, H2CornError> {
    let Some(value) = value else {
        return Ok(ResponseHeaders::new());
    };

    if let Ok(list) = value.cast_exact::<PyList>()
        && let Some(headers) = try_parse_exact_header_list(list)?
    {
        return Ok(headers);
    }

    parse_header_iterable(&value)
}

fn cast_into_exact_first<T>(value: Bound<'_, PyAny>) -> Result<Bound<'_, T>, H2CornError>
where
    T: PyTypeInfo + PyTypeCheck,
{
    match value.cast_into_exact::<T>() {
        Ok(value) => Ok(value),
        Err(err) => err
            .into_inner()
            .cast_into::<T>()
            .map_err(PyErr::from)
            .map_err(H2CornError::from),
    }
}

fn parse_header_iterable(value: &Bound<'_, PyAny>) -> Result<ResponseHeaders, H2CornError> {
    let mut headers = ResponseHeaders::with_capacity(value.len().unwrap_or_default());
    for item in value.try_iter()? {
        let (name, value) = item?.extract::<(PyBackedBytes, PyBackedBytes)>()?;
        headers.push(parse_response_header(name, value)?);
    }
    Ok(headers)
}

fn try_parse_exact_header_list(
    list: &Bound<'_, PyList>,
) -> Result<Option<ResponseHeaders>, H2CornError> {
    let mut headers = ResponseHeaders::with_capacity(list.len());
    for item in list.iter() {
        let Ok(tuple) = item.cast_exact::<PyTuple>() else {
            return Ok(None);
        };
        let [name, value] = tuple.as_slice() else {
            return Ok(None);
        };
        let Ok(name) = name
            .cast_exact::<PyBytes>()
            .map(|bytes| PyBackedBytes::from(bytes.to_owned()))
        else {
            return Ok(None);
        };
        let Ok(value) = value
            .cast_exact::<PyBytes>()
            .map(|bytes| PyBackedBytes::from(bytes.to_owned()))
        else {
            return Ok(None);
        };
        headers.push(parse_response_header(name, value)?);
    }
    Ok(Some(headers))
}

fn parse_response_header(
    name: PyBackedBytes,
    value: PyBackedBytes,
) -> Result<ParsedHeaderPair, H2CornError> {
    let name = ResponseHeaderName::from_python(name)
        .ok_or_else(|| H2CornError::from(HttpResponseError::InvalidResponseHeaderName))?;
    let value = ResponseHeaderValue::from_python(value)
        .ok_or_else(|| H2CornError::from(HttpResponseError::InvalidResponseHeaderValue))?;
    Ok((name, value))
}

fn extract_payload_bytes(value: &Bound<'_, PyAny>) -> Result<PayloadBytes, H2CornError> {
    cast_exact_first::<PyBytes>(value).map_or_else(
        |_| {
            cast_exact_first::<PyByteArray>(value).map_or_else(
                |_| {
                    Err(H2CornError::from(PyTypeError::new_err(
                        "expected bytes or bytearray for ASGI body bytes",
                    )))
                },
                |bytearray| {
                    Ok(PayloadBytes::from(PyBackedBytes::from(
                        bytearray.to_owned(),
                    )))
                },
            )
        },
        |bytes| Ok(PayloadBytes::from(PyBackedBytes::from(bytes.to_owned()))),
    )
}

fn extract_backed_str(value: &Bound<'_, PyAny>) -> Result<PyBackedStr, H2CornError> {
    Ok(PyBackedStr::try_from(
        cast_exact_first::<PyString>(value)?.to_owned(),
    )?)
}

fn cast_exact_first<'a, 'py, T>(
    value: &'a Bound<'py, PyAny>,
) -> Result<&'a Bound<'py, T>, H2CornError>
where
    T: PyTypeInfo + PyTypeCheck,
{
    value
        .cast_exact::<T>()
        .or_else(|_| value.cast::<T>())
        .map_err(PyErr::from)
        .map_err(H2CornError::from)
}

pub fn try_send_or_await<'py, T: Send + 'static>(
    py: Python<'py>,
    locals: &TaskLocals,
    tx: &mpsc::Sender<T>,
    event: T,
) -> PyResult<Bound<'py, PyAny>> {
    match try_push(tx, event) {
        TryPush::Sent => ready_awaitable(py, py.None()),
        TryPush::Full(event) => {
            let locals = locals.clone();
            let tx = tx.clone();
            future_into_py_with_locals(py, locals, async move {
                tx.send(event)
                    .await
                    .map_err(|_| into_pyerr(AsgiError::SendAfterClose))?;
                Python::attach(|py| Ok(py.None()))
            })
        },
        TryPush::Closed(_) => Err(into_pyerr(AsgiError::SendAfterClose)),
    }
}

pub fn parse_http_outbound_event(
    message: &Bound<'_, PyDict>,
) -> Result<HttpOutboundEvent, H2CornError> {
    let message = AsgiMessage::parse(message)?;

    match message.message_type()? {
        "http.response.start" => {
            let status = message.status(AsgiContainer::HttpResponseStart)?;
            let headers = message.headers()?;
            let trailers = message.trailers_flag()?;
            Ok(HttpOutboundEvent::Start {
                status,
                headers,
                trailers,
            })
        },
        "http.response.body" => {
            let body = message.body_or_empty()?;
            let more_body = message.more_body_flag()?;
            Ok(HttpOutboundEvent::Body { body, more_body })
        },
        "http.response.pathsend" => Ok(HttpOutboundEvent::PathSend {
            path: message.path(AsgiContainer::HttpResponsePathsend)?,
        }),
        "http.response.trailers" => {
            let headers = message.headers()?;
            let more_trailers = message.more_trailers_flag()?;
            Ok(HttpOutboundEvent::Trailers {
                headers,
                more_trailers,
            })
        },
        _ => AsgiError::unsupported_http_outbound_message(message.message_type()?).err(),
    }
}

pub fn parse_websocket_outbound_event(
    message: &Bound<'_, PyDict>,
) -> Result<WebSocketOutboundEvent, H2CornError> {
    let message = AsgiMessage::parse(message)?;

    match message.message_type()? {
        "websocket.accept" => {
            let subprotocol = message.subprotocol()?;
            let headers = message.headers()?;
            Ok(WebSocketOutboundEvent::Accept {
                subprotocol,
                headers,
            })
        },
        "websocket.send" => match message.websocket_send_payload()? {
            WebSocketSendPayload::Text(text) => Ok(WebSocketOutboundEvent::SendText(text)),
            WebSocketSendPayload::Bytes(data) => Ok(WebSocketOutboundEvent::SendBytes(data)),
        },
        "websocket.close" => {
            let code = message.close_code_or_default()?;
            let reason = message.reason()?;
            Ok(WebSocketOutboundEvent::Close { code, reason })
        },
        "websocket.http.response.start" => {
            let status = message.status(AsgiContainer::WebSocketHttpResponseStart)?;
            let headers = message.headers()?;
            Ok(WebSocketOutboundEvent::HttpResponseStart { status, headers })
        },
        "websocket.http.response.body" => {
            let body = message.body_or_empty()?;
            let more_body = message.more_body_flag()?;
            Ok(WebSocketOutboundEvent::HttpResponseBody { body, more_body })
        },
        _ => AsgiError::unsupported_websocket_outbound_message(message.message_type()?).err(),
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use pyo3::types::{PyBytes, PyDict, PyDictMethods, PyTuple};
    use pyo3::{PyResult, Python};

    use super::{
        HttpInboundEvent, WebSocketOutboundEvent, build_http_inbound_event,
        parse_http_outbound_event, parse_websocket_outbound_event,
    };
    use crate::error::{AsgiContainer, AsgiError, H2CornError};
    use crate::python::py_dict;

    fn init_python() {
        Python::initialize();
    }

    #[test]
    fn http_response_start_requires_status() {
        init_python();
        Python::attach(|py| -> PyResult<()> {
            let message = py_dict!(py, {
                "type" => "http.response.start",
            });

            let err = parse_http_outbound_event(&message).unwrap_err();
            assert!(matches!(
                err,
                H2CornError::Asgi(AsgiError::MissingField {
                    container: AsgiContainer::HttpResponseStart,
                    field: "status",
                })
            ));
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn websocket_send_accepts_exactly_one_payload_variant() {
        init_python();
        Python::attach(|py| -> PyResult<()> {
            let text_message = py_dict!(py, {
                "type" => "websocket.send",
                "text" => "hello",
            });
            let bytes_message = py_dict!(py, {
                "type" => "websocket.send",
                "bytes" => PyBytes::new(py, b"hello"),
            });

            let text_event = parse_websocket_outbound_event(&text_message).unwrap();
            let bytes_event = parse_websocket_outbound_event(&bytes_message).unwrap();

            assert!(matches!(
                text_event,
                WebSocketOutboundEvent::SendText(text) if &*text == "hello"
            ));
            assert!(matches!(
                bytes_event,
                WebSocketOutboundEvent::SendBytes(body) if body.as_ref() == b"hello"
            ));
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn websocket_send_rejects_missing_or_ambiguous_payload() {
        init_python();
        Python::attach(|py| -> PyResult<()> {
            let missing_payload = py_dict!(py, {
                "type" => "websocket.send",
            });
            let ambiguous_payload = py_dict!(py, {
                "type" => "websocket.send",
                "text" => "hello",
                "bytes" => PyBytes::new(py, b"hello"),
            });

            for message in [&missing_payload, &ambiguous_payload] {
                let err = parse_websocket_outbound_event(message).unwrap_err();
                assert!(matches!(
                    err,
                    H2CornError::Asgi(AsgiError::WebSocketSendRequiresExactlyOnePayload)
                ));
            }
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn http_response_start_defaults_trailers_to_false() {
        init_python();
        Python::attach(|py| -> PyResult<()> {
            let message = py_dict!(py, {
                "type" => "http.response.start",
                "status" => 200,
                "headers" => [
                    (PyBytes::new(py, b"content-length"), PyBytes::new(py, b"2")),
                ],
            });

            let event = parse_http_outbound_event(&message).unwrap();
            assert!(matches!(
                event,
                super::HttpOutboundEvent::Start { status: 200, trailers: false, headers }
                    if headers.len() == 1
                        && headers[0].0.as_ref() == b"content-length"
                        && headers[0].1.as_ref() == b"2"
            ));
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn http_response_start_accepts_tuple_headers_via_generic_fallback() {
        init_python();
        Python::attach(|py| -> PyResult<()> {
            let headers =
                PyTuple::new(py, [(PyBytes::new(py, b"x-demo"), PyBytes::new(py, b"1"))])?;
            let message = py_dict!(py, {
                "type" => "http.response.start",
                "status" => 200,
                "headers" => headers,
            });

            let event = parse_http_outbound_event(&message).unwrap();
            assert!(matches!(
                event,
                super::HttpOutboundEvent::Start { status: 200, trailers: false, ref headers }
                    if headers.len() == 1
                        && headers[0].0.as_ref() == b"x-demo"
                        && headers[0].1.as_ref() == b"1"
            ));
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn http_request_event_omits_default_body_and_more_body() {
        init_python();
        Python::attach(|py| -> PyResult<()> {
            let event = build_http_inbound_event(py, HttpInboundEvent::Request {
                body: Bytes::new(),
                more_body: false,
            })?
            .bind(py)
            .clone()
            .cast_into::<PyDict>()?;

            assert!(event.get_item("type")?.is_some());
            assert!(event.get_item("body")?.is_none());
            assert!(event.get_item("more_body")?.is_none());
            Ok(())
        })
        .unwrap();
    }
}
