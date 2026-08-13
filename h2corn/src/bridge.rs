mod http;
mod websocket;

use std::path::PathBuf;
use std::sync::Arc;
use std::{fmt, future, ptr};

use bytes::Bytes;
use pyo3::exceptions::{PyRuntimeError, PyStopIteration};
use pyo3::prelude::*;
use pyo3::pybacked::{PyBackedBytes, PyBackedStr};
use pyo3::types::{PyBool, PyBoolMethods, PyByteArray, PyBytes, PyDict, PyInt, PyList, PyString};
use pyo3::{PyTypeCheck, PyTypeInfo};
use tokio::sync::{Mutex, OwnedMutexGuard, mpsc};
/// Everything only `http.response.zerocopysend` needs, which exists only on
/// Unix. Grouped so the extension leaves no trace in a build that cannot serve
/// it, rather than seven separately gated imports.
#[cfg(unix)]
use {
    pyo3::exceptions::{PyAttributeError, PyTypeError},
    pyo3::intern,
    rustix::fs::{FileType, fstat},
    std::fs::File,
    std::io,
};

use crate::async_util::{TryPush, try_push};
pub(crate) use crate::bridge::http::{
    PyHttpReceive, PyHttpSend, RequestBodyCounter, RequestInputShared,
};
#[cfg(test)]
pub(crate) use crate::bridge::websocket::WebSocketSendDisposition;
pub(crate) use crate::bridge::websocket::{
    PyWebSocketReceive, PyWebSocketSend, WebSocketDisconnect, WebSocketInboundMessage,
    WebSocketInboundReceiver, WebSocketInboundSender, WebSocketInboundTrySendError,
    WebSocketSendBuffer, WebSocketSendState, websocket_inbound_channel,
};
use crate::error::{
    AsgiChannel, AsgiContainer, AsgiError, ErrorExt as _, H2CornError, HttpResponseError,
    WebSocketError, into_pyerr,
};
use crate::http::app::HttpSendWaiter;
use crate::http::header::{
    ApplicationResponseField, RESPONSE_DEFAULT_BUILTIN_SLOTS, ResponseConnectionDirective,
    application_response_field, protocol_token_is_valid, split_commas_bytes,
};
#[cfg(unix)]
use crate::http::pathsend::read_at;
use crate::http::types::{
    BytesStr, EarlyHintLinks, FinalResponseStatus, HttpStatusCode, ResponseHeaderName,
    ResponseHeaderValue, ResponseHeaders, ResponseTrailers,
};
use crate::pyloop::{PumpEvent, ResolveOp, ResolvePayload, Shard, new_rust_future, runtime};
use crate::python::{StaticPyKey, py_dict};
use crate::runtime::H2InputCredit;
use crate::websocket::{
    SEC_WEBSOCKET_EXTENSIONS_HEADER_BYTES, SEC_WEBSOCKET_PROTOCOL_HEADER_BYTES, WebSocketCloseCode,
    close_code,
};

/// One declaration per outbound message type, per ASGI channel.
///
/// The enum, the interned-pointer fast path and the string fallback are all
/// generated from this list. Written out by hand they repeated every message
/// type three times, and omitting one from the pointer chain silently demoted
/// a canonical interned message to a string comparison on the hot path — a
/// slowdown with no wrong answer, so nothing would have caught it.
///
/// A variant may carry attributes, which reach every one of those places at
/// once. `#[cfg]` is the reason it exists: a message type the build cannot
/// serve should not have a name to resolve to, so gating it here makes the
/// wire string fall through to `unsupported_outbound_message` instead of
/// producing a variant with no handler.
macro_rules! asgi_outbound_types {
    (
        $name:ident, $channel:expr, $interned:ident, $resolve:ident {
            $($(#[$attr:meta])* $variant:ident => $wire:literal),+ $(,)?
        }
    ) => {
        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        enum $name {
            $($(#[$attr])* $variant),+
        }

        #[cfg(test)]
        impl $name {
            const WIRE_NAMES: &'static [(Self, &'static str)] =
                &[$($(#[$attr])* (Self::$variant, $wire)),+];
        }

        impl AsgiMessage<'_> {
            fn $interned(&self) -> Option<$name> {
                let py = self.message_type.py();
                let message_type = self.message_type.as_ptr();
                $(
                    $(#[$attr])*
                    {
                        static CELL: crate::python::PyOnceLock<Py<PyString>> =
                            crate::python::PyOnceLock::new();
                        if ptr::eq(
                            message_type,
                            CELL.get_or_init(py, || PyString::intern(py, $wire).unbind())
                                .bind(py)
                                .as_ptr(),
                        ) {
                            return Some($name::$variant);
                        }
                    }
                )+
                None
            }

            fn $resolve(&self) -> Result<$name, H2CornError> {
                self.$interned().map_or_else(
                    || match self.message_type()? {
                        $($(#[$attr])* $wire => Ok($name::$variant),)+
                        message_type => {
                            AsgiError::unsupported_outbound_message($channel, message_type).err()
                        },
                    },
                    Ok,
                )
            }
        }
    };
}

macro_rules! asgi_item {
    ($fn:ident, $name:literal) => {
        fn $fn(&self) -> Result<Option<Bound<'py, PyAny>>, H2CornError> {
            static KEY: StaticPyKey = StaticPyKey::new($name);
            KEY.get_item(self.dict.py(), self.dict)
                .map_err(H2CornError::from)
        }
    };
}

/// Small event queues are appropriate for HTTP body chunks and outbound ASGI
/// notifications. Complete inbound WebSocket messages are byte-accounted in
/// `bridge::websocket`; giving all three one capacity hid their very different
/// retention costs.
pub(crate) const HTTP_ASGI_QUEUE_CAPACITY: usize = 32;
pub(crate) const WEBSOCKET_OUTBOUND_QUEUE_CAPACITY: usize = 32;
pub(crate) const WEBSOCKET_INBOUND_BYTE_CAPACITY: usize = 16 * 1024 * 1024;

asgi_outbound_types! {
    HttpOutboundType, AsgiChannel::Http, interned_http_outbound_type, http_outbound_type {
        Start => "http.response.start",
        Body => "http.response.body",
        Pathsend => "http.response.pathsend",
        #[cfg(unix)]
        ZeroCopySend => "http.response.zerocopysend",
        Trailers => "http.response.trailers",
        EarlyHint => "http.response.early_hint",
    }
}

asgi_outbound_types! {
    WebSocketOutboundType,
    AsgiChannel::WebSocket,
    interned_websocket_outbound_type,
    websocket_outbound_type {
        Accept => "websocket.accept",
        Send => "websocket.send",
        Close => "websocket.close",
        HttpResponseStart => "websocket.http.response.start",
        HttpResponseBody => "websocket.http.response.body",
    }
}

#[derive(Debug)]
pub(crate) enum HttpInboundEvent {
    Request {
        body: Bytes,
        more_body: bool,
        credit: Option<H2InputCredit>,
    },
    RequestBatch {
        bodies: Vec<Bytes>,
        body_bytes: usize,
        credit: Option<H2InputCredit>,
    },
    HttpDisconnect,
}

const _: () = assert!(std::mem::size_of::<HttpInboundEvent>() <= 56);

#[derive(Debug)]
pub(crate) enum WebSocketInboundEvent {
    Connect,
    ReceiveBytes(Bytes),
    ReceiveText(BytesStr),
    Disconnect {
        code: WebSocketCloseCode,
        reason: Option<BytesStr>,
    },
}

#[derive(Debug)]
pub(crate) enum HttpOutboundEvent {
    Start {
        status: FinalResponseStatus,
        headers: ResponseHeaders,
        trailers: bool,
        directive: ResponseConnectionDirective,
    },
    Body {
        body: PayloadBytes,
        more_body: bool,
    },
    PathSend {
        path: PathBuf,
    },
    /// One `http.response.zerocopysend` segment.
    ///
    /// The descriptor is already ours: it is duplicated at parse time, while
    /// the application's `send()` is still on the stack. The spec leaves the
    /// original with the application — *"ASGI servers are not responsible for
    /// closing descriptors"* — so it may be closed the instant `send()`
    /// returns, which would otherwise pull the file out from under an
    /// in-flight sendfile.
    #[cfg(unix)]
    ZeroCopySend {
        file: File,
        /// Where to start reading, already resolved: the explicit `offset` when
        /// the application gave one, otherwise the descriptor's current
        /// position, which the spec names as the default.
        ///
        /// Reads are positional from here in **both** cases. The duplicate
        /// shares its file *description* — and therefore its position — with
        /// the application's own object, so sequential reads would drag that
        /// position along under an application still using the file. Resolving
        /// the default once, here, honours "current position" without ever
        /// moving it.
        start: u64,
        len: usize,
        more_body: bool,
    },
    Trailers {
        headers: ResponseTrailers,
        more_trailers: bool,
    },
    /// RFC 8297 103. Carries only its `link` values -- see `EarlyHintLinks`.
    EarlyHint(EarlyHintLinks),
}

#[derive(Debug)]
pub(crate) enum WebSocketOutboundEvent {
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
        status: FinalResponseStatus,
        headers: ResponseHeaders,
        directive: ResponseConnectionDirective,
    },
    HttpResponseBody {
        body: PayloadBytes,
        more_body: bool,
    },
}

impl WebSocketOutboundEvent {
    pub(crate) const fn message_type(&self) -> &'static str {
        match self {
            Self::Accept { .. } => "websocket.accept",
            Self::SendBytes(_) | Self::SendText(_) => "websocket.send",
            Self::Close { .. } => "websocket.close",
            Self::HttpResponseStart { .. } => "websocket.http.response.start",
            Self::HttpResponseBody { .. } => "websocket.http.response.body",
        }
    }
}

#[derive(Debug)]
/// An outbound payload that owns its bytes, whoever produced them.
///
/// Text carries its Python owner rather than a copy: a WebSocket message the
/// application sent as `str` is written straight from the interpreter's buffer,
/// exactly like `bytes`.
pub(crate) enum PayloadBytes {
    Rust(Bytes),
    Python(PyBackedBytes),
    Text(PyBackedStr),
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
            Self::Text(text) => text.as_bytes(),
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

impl From<PyBackedStr> for PayloadBytes {
    fn from(value: PyBackedStr) -> Self {
        Self::Text(value)
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
    #[cfg(unix)]
    asgi_item!(file_item, "file");
    #[cfg(unix)]
    asgi_item!(offset_item, "offset");
    #[cfg(unix)]
    asgi_item!(count_item, "count");
    asgi_item!(links_item, "links");
    asgi_item!(more_trailers_item, "more_trailers");
    asgi_item!(subprotocol_item, "subprotocol");
    asgi_item!(text_item, "text");
    asgi_item!(bytes_item, "bytes");
    asgi_item!(code_item, "code");
    asgi_item!(reason_item, "reason");
    asgi_item!(status_item, "status");

    fn parse(dict: &'py Bound<'py, PyDict>) -> Result<Self, H2CornError> {
        let value = {
            static KEY: StaticPyKey = StaticPyKey::new("type");
            KEY.get_item(dict.py(), dict).map_err(H2CornError::from)?
        }
        .ok_or_else(|| AsgiError::missing_field(AsgiContainer::Message, "type").into_error())?;
        let message_type = match value.cast_into_exact::<PyString>() {
            Ok(value) => value,
            Err(error) => {
                let value = error.into_inner();
                match value.cast_into::<PyString>() {
                    Ok(value) => value,
                    Err(error) => {
                        let value = error.into_inner();
                        return Err(field_type_error(
                            AsgiContainer::Message,
                            "type",
                            "a str",
                            &value,
                        ));
                    },
                }
            },
        };
        Ok(Self { dict, message_type })
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

    fn bool_or_false(
        container: AsgiContainer,
        field: &'static str,
        value: Option<Bound<'py, PyAny>>,
    ) -> Result<bool, H2CornError> {
        value.map_or(Ok(false), |value| {
            cast_exact_first::<PyBool>(&value)
                .map(PyBoolMethods::is_true)
                .map_err(|_| field_type_error(container, field, "a bool", &value))
        })
    }

    fn payload_bytes_or_empty(
        container: AsgiContainer,
        field: &'static str,
        value: Option<Bound<'py, PyAny>>,
    ) -> Result<PayloadBytes, H2CornError> {
        value.map_or_else(
            || Ok(PayloadBytes::from(Bytes::new())),
            |value| extract_payload_bytes(container, field, &value),
        )
    }

    fn response_headers(&self, container: AsgiContainer) -> Result<ResponseHeaders, H2CornError> {
        parse_headers(container, self.headers_item()?)
    }

    fn application_response_headers(
        &self,
        container: AsgiContainer,
    ) -> Result<(ResponseHeaders, ResponseConnectionDirective), H2CornError> {
        let mut headers = self.response_headers(container)?;
        let control = validate_application_response_headers(&mut headers)?;
        Ok((headers, control))
    }

    fn response_trailers(&self, container: AsgiContainer) -> Result<ResponseTrailers, H2CornError> {
        ResponseTrailers::try_from(parse_headers(container, self.headers_item()?)?)
    }

    fn status(&self, container: AsgiContainer) -> Result<HttpStatusCode, H2CornError> {
        let value = Self::require(container, "status", self.status_item()?)?;
        let status = cast_exact_first::<PyInt>(&value)
            .map_err(|_| field_type_error(container, "status", "an int", &value))?
            .extract::<i64>()
            .map_err(|_| HttpResponseError::StatusOutsideSigned64BitRange { container })?;
        let status = u16::try_from(status)
            .map_err(|_| HttpResponseError::StatusMustBeThreeDigitCode { container, status })?;
        HttpStatusCode::new(status).ok_or_else(|| {
            HttpResponseError::StatusMustBeThreeDigitCode {
                container,
                status: i64::from(status),
            }
            .into_error()
        })
    }

    fn trailers_flag(&self, container: AsgiContainer) -> Result<bool, H2CornError> {
        Self::bool_or_false(container, "trailers", self.trailers_item()?)
    }

    fn body_or_empty(&self, container: AsgiContainer) -> Result<PayloadBytes, H2CornError> {
        Self::payload_bytes_or_empty(container, "body", self.body_item()?)
    }

    fn more_body_flag(&self, container: AsgiContainer) -> Result<bool, H2CornError> {
        Self::bool_or_false(container, "more_body", self.more_body_item()?)
    }

    /// The `link` values of an `http.response.early_hint`.
    ///
    /// Any iterable of `bytes` is accepted, including a generator, and order
    /// and duplicates are preserved: RFC 8288 grammar is not parsed here
    /// because a Link value is inert application data to this server. An
    /// empty iterable is valid and produces a bare 103.
    fn early_hint_links(&self, container: AsgiContainer) -> Result<EarlyHintLinks, H2CornError> {
        let value = Self::require(container, "links", self.links_item()?)?;
        // A failure inside `__iter__`/`__next__` belongs to the application's
        // producer, not to the type of `links` -- propagate it rather than
        // reporting a `TypeError` the caller cannot act on.
        let iterator = value.try_iter().map_err(H2CornError::from)?;
        let mut links = Vec::new();
        for item in iterator {
            let item = item.map_err(H2CornError::from)?;
            let bytes = PyBackedBytes::from(
                cast_exact_first::<PyBytes>(&item)
                    .map_err(|_| field_type_error(container, "links", "bytes", &item))?
                    .to_owned(),
            );
            // The type is right and the value is not: CR/LF or a control byte
            // in a field value is a `ValueError`, exactly as it is for an
            // ordinary response header.
            let link = ResponseHeaderValue::from_python(bytes)
                .ok_or_else(|| H2CornError::from(HttpResponseError::InvalidResponseHeaderValue))?;
            links.push(link);
        }
        Ok(EarlyHintLinks::new(links))
    }

    fn path(&self, container: AsgiContainer) -> Result<PathBuf, H2CornError> {
        let value = Self::require(container, "path", self.path_item()?)?;
        Ok(PathBuf::from(
            extract_backed_str(container, "path", &value)?.as_str(),
        ))
    }

    fn more_trailers_flag(&self, container: AsgiContainer) -> Result<bool, H2CornError> {
        Self::bool_or_false(container, "more_trailers", self.more_trailers_item()?)
    }

    /// The descriptor, range and continuation flag of an
    /// `http.response.zerocopysend`.
    ///
    /// Unix only, because the whole message is: `std::os::fd` does not exist on
    /// Windows, and a CRT file descriptor there is a different object needing
    /// its own duplication and metadata handling. The extension is not
    /// advertised where this is not compiled, so an application never sees a
    /// capability the server cannot honour.
    ///
    /// The duplicate is taken here, synchronously, because this is the closest
    /// this server can get to the moment the application still holds the
    /// descriptor.
    #[cfg(unix)]
    fn zerocopysend(&self, container: AsgiContainer) -> Result<(File, u64, usize), H2CornError> {
        use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd, RawFd};

        let value = Self::require(container, "file", self.file_item()?)?;
        // The spec's field is a file *object*, not a raw descriptor. A missing
        // or non-callable `fileno` is a wrong type; a `fileno()` that raises --
        // a closed file raises `ValueError` -- is the application's own error
        // and is propagated rather than relabelled as a type problem.
        let fileno = match value.call_method0(intern!(value.py(), "fileno")) {
            Ok(fileno) => fileno,
            Err(err)
                if err.is_instance_of::<PyAttributeError>(value.py())
                    || err.is_instance_of::<PyTypeError>(value.py()) =>
            {
                return Err(field_type_error(
                    container,
                    "file",
                    "an object with fileno()",
                    &value,
                ));
            },
            Err(err) => return Err(err.into()),
        };
        let raw = fileno.extract::<RawFd>().map_err(|_| {
            field_type_error(container, "file", "fileno() returning an int", &fileno)
        })?;

        // Duplicated straight from the integer. Every typed entry point would
        // need a `BorrowedFd` first, and its safety contract cannot be honoured
        // for a value the application chose -- `-1` panics on construction, and
        // a stale number is simply a lie. `F_DUPFD_CLOEXEC` takes the integer,
        // answers `EBADF` when it does not name anything, and sets
        // close-on-exec *atomically*: a separate `fcntl` would leave a window
        // in which a concurrently spawned process inherits the duplicate and
        // keeps the file alive past every Rust owner.
        //
        // SAFETY: the descriptor is constructed only from a successful return,
        // so it is one the kernel just handed us and nobody else owns.
        // SAFETY: `fcntl` accepts any integer and answers `EBADF` rather than
        // misbehaving, so passing an application-chosen value is defined.
        let duplicated = match unsafe { libc::fcntl(raw, libc::F_DUPFD_CLOEXEC, 0) } {
            -1 => return Err(H2CornError::from(io::Error::last_os_error())),
            // SAFETY: a successful return is a descriptor the kernel just
            // created for us, so this is its sole owner.
            duplicated => unsafe { OwnedFd::from_raw_fd(duplicated) },
        };

        let metadata = fstat(&duplicated).map_err(|err| H2CornError::from(io::Error::from(err)))?;
        // sendfile requires a regular file as its input, and a directory or
        // socket here is an application bug rather than a transport condition.
        if FileType::from_raw_mode(metadata.st_mode) != FileType::RegularFile {
            return HttpResponseError::ZeroCopySendNotRegularFile.err();
        }
        // Readability is checked here rather than discovered by the first read:
        // an `O_WRONLY` or `O_PATH` descriptor fails only once the response
        // head is already committed, where the application can no longer
        // substitute a fallback.
        // SAFETY: the descriptor is one we own and have not closed.
        let flags = match unsafe { libc::fcntl(duplicated.as_raw_fd(), libc::F_GETFL) } {
            -1 => return Err(H2CornError::from(io::Error::last_os_error())),
            flags => flags,
        };
        if flags & libc::O_ACCMODE == libc::O_WRONLY {
            return HttpResponseError::ZeroCopySendNotReadable.err();
        }
        let file = File::from(duplicated);

        let offset = Self::optional_item(self.offset_item()?)
            .map(|value| extract_unsigned(container, "offset", &value))
            .transpose()?;
        let count = Self::optional_item(self.count_item()?)
            .map(|value| extract_unsigned(container, "count", &value))
            .transpose()?;

        // "Defaults to current position if absent". Read once, here, and never
        // advanced: the duplicate shares its file *description* -- and so its
        // position -- with the object the application still holds.
        let start = match offset {
            Some(offset) => offset,
            None => rustix::fs::seek(&file, rustix::fs::SeekFrom::Current(0))
                .map_err(|err| H2CornError::from(io::Error::from(err)))?,
        };
        let size = u64::try_from(metadata.st_size).unwrap_or(0);
        let available = size.saturating_sub(start);
        let len = usize::try_from(count.map_or(available, |count| count.min(available))).map_err(
            |_| {
                H2CornError::from(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "zerocopysend length does not fit usize",
                ))
            },
        )?;

        // "Until its end" is an I/O condition; `st_size` is only a snapshot of
        // it. They disagree on the synthetic filesystems: `/proc/version` is a
        // regular, seekable file reporting `st_size == 0` that nonetheless
        // reads its contents. Believing the metadata there would answer an
        // empty body and call it a complete response, so the disagreement is
        // detected and reported instead of served.
        if len == 0 && count.is_none() {
            let mut probe = [0_u8; 1];
            if read_at(&file, &mut probe, start).map_err(H2CornError::from)? != 0 {
                return HttpResponseError::ZeroCopySendLengthUnknown.err();
            }
        }

        Ok((file, start, len))
    }

    fn optional_backed_str(
        container: AsgiContainer,
        field: &'static str,
        value: Option<Bound<'py, PyAny>>,
    ) -> Result<Option<PyBackedStr>, H2CornError> {
        Self::optional_item(value)
            .map(|value| extract_backed_str(container, field, &value))
            .transpose()
    }

    fn subprotocol(&self, container: AsgiContainer) -> Result<Option<PyBackedStr>, H2CornError> {
        Self::optional_backed_str(container, "subprotocol", self.subprotocol_item()?)
    }

    fn close_code_or_default(
        &self,
        container: AsgiContainer,
    ) -> Result<WebSocketCloseCode, H2CornError> {
        Self::optional_item(self.code_item()?).map_or(Ok(close_code::NORMAL), |value| {
            let code = cast_exact_first::<PyInt>(&value)
                .map_err(|_| field_type_error(container, "code", "an int", &value))?
                .extract::<u16>()
                .map_err(|_| WebSocketError::CloseCodeInvalid.into_error())?;
            WebSocketCloseCode::new(code)
                .ok_or_else(|| WebSocketError::CloseCodeInvalid.into_error())
        })
    }

    fn reason(&self, container: AsgiContainer) -> Result<Option<PyBackedStr>, H2CornError> {
        Self::optional_backed_str(container, "reason", self.reason_item()?)
    }

    fn websocket_send_payload(&self) -> Result<WebSocketSendPayload, H2CornError> {
        match (
            Self::optional_item(self.text_item()?),
            Self::optional_item(self.bytes_item()?),
        ) {
            (Some(value), None) => Ok(WebSocketSendPayload::Text(extract_backed_str(
                AsgiContainer::WebSocketSend,
                "text",
                &value,
            )?)),
            (None, Some(value)) => Ok(WebSocketSendPayload::Bytes(extract_payload_bytes(
                AsgiContainer::WebSocketSend,
                "bytes",
                &value,
            )?)),
            _ => AsgiError::WebSocketSendRequiresExactlyOnePayload.err(),
        }
    }
}

#[pyclass(name = "_ReadyAwaitable")]
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

/// A stateless, re-awaitable awaitable that always resolves to `None`.
///
/// Every successful ASGI `send()` resolves to `None`, so instead of building a
/// fresh awaitable per send we hand out a reference to one cached instance per
/// shard (see [`ready_none`]). It holds no state and takes `&self`, so sharing
/// it is sound even under repeated or concurrent awaits (including across
/// free-threaded shard threads): `__next__` just raises a bare `StopIteration`
/// synchronously, with nothing to corrupt.
///
/// The raise carries no argument. `StopIteration()` and `StopIteration(None)`
/// both have `.value is None`, which is what `await` reads; passing `None`
/// only differs in `.args`, and costs a one-element tuple on every `send()`.
#[pyclass(frozen, name = "_ReadyNone")]
pub(crate) struct ReadyNone;

#[pymethods]
impl ReadyNone {
    fn send(&self, _value: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        Err(PyStopIteration::new_err(()))
    }

    const fn close(&self) {}

    const fn __await__(self_: Py<Self>) -> Py<Self> {
        self_
    }

    const fn __iter__(self_: Py<Self>) -> Py<Self> {
        self_
    }

    fn __next__(&self) -> PyResult<Py<PyAny>> {
        Err(PyStopIteration::new_err(()))
    }
}

/// Protocol-specific event pull. The cancel-race requeue invariant lives in
/// [`Requeueable`], not in implementations of this trait.
pub(crate) trait EventSource: Send + 'static {
    type Event: Send + 'static;

    fn pull(&mut self) -> impl future::Future<Output = Self::Event> + Send + '_;

    fn try_pull(&mut self) -> Option<Self::Event>;

    /// Mark a slow-path waiter as live. HTTP request ownership uses this to
    /// distinguish an application already awaiting disconnect from an
    /// abandoned task that can be cancelled immediately.
    fn wait_signal(&self) -> Option<Arc<RequestInputShared>> {
        None
    }
}

/// Wraps an [`EventSource`] with the requeued-event slot: an event consumed
/// for a future that got cancelled before resolution (`wait_for(receive())`
/// and friends) is handed back and must be served before any new event —
/// the no-event-loss invariant lives here, once.
pub(crate) struct Requeueable<S: EventSource> {
    source: S,
    requeued: Option<S::Event>,
}

impl<S: EventSource + fmt::Debug> fmt::Debug for Requeueable<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Requeueable")
            .field("source", &self.source)
            .field("requeued", &self.requeued.is_some())
            .finish()
    }
}

impl<S: EventSource> Requeueable<S> {
    pub(crate) const fn new(source: S) -> Self {
        Self {
            source,
            requeued: None,
        }
    }

    fn try_next(&mut self) -> Option<S::Event> {
        self.requeued.take().or_else(|| self.source.try_pull())
    }

    async fn next(&mut self) -> S::Event {
        match self.try_next() {
            Some(event) => event,
            None => self.source.pull().await,
        }
    }

    fn requeue(&mut self, event: S::Event) {
        debug_assert!(self.requeued.is_none(), "at most one receive in flight");
        self.requeued = Some(event);
    }

    fn wait_signal(&self) -> Option<Arc<RequestInputShared>> {
        self.source.wait_signal()
    }
}

/// Resolve payload for a consumed receive event: convert on the loop thread,
/// or give the event back after a cancellation race.
struct ReceiveResolve<S: EventSource> {
    event: S::Event,
    guard: OwnedMutexGuard<Requeueable<S>>,
    build_event: fn(Python<'_>, S::Event) -> PyResult<Py<PyAny>>,
}

impl<S: EventSource> ResolveOp for ReceiveResolve<S> {
    fn convert(self: Box<Self>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let Self {
            event,
            guard,
            build_event,
        } = *self;
        let result = build_event(py, event);
        // Resolution callbacks may immediately call receive() again. Release
        // exclusive ownership before RustFuture invokes any of them.
        drop(guard);
        result
    }

    fn requeue(self: Box<Self>) {
        let Self {
            event, mut guard, ..
        } = *self;
        guard.requeue(event);
    }
}

pub(crate) fn ready_awaitable(py: Python<'_>, result: Py<PyAny>) -> PyResult<Bound<'_, PyAny>> {
    Ok(Py::new(py, ReadyAwaitable {
        result: Some(result),
    })?
    .into_bound(py)
    .into_any())
}

/// The shard's cached `None`-resolving awaitable, for a `send()` that
/// completed synchronously. No allocation — just a new reference to the shared
/// [`ReadyNone`] singleton.
pub(crate) fn ready_none<'py>(py: Python<'py>, shard: &Shard) -> Bound<'py, PyAny> {
    shard.ready_none().bind(py).clone().into_any()
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "the returned awaitable and pump resolver require independent Py references"
)]
pub(crate) fn receive_or_await<'py, S>(
    py: Python<'py>,
    shard: &Shard,
    state: &Arc<Mutex<Requeueable<S>>>,
    build_event: fn(Python<'_>, S::Event) -> PyResult<Py<PyAny>>,
) -> PyResult<Bound<'py, PyAny>>
where
    S: EventSource,
{
    let wait_signal = if let Ok(mut guard) = state.try_lock() {
        if let Some(event) = guard.try_next() {
            return ready_awaitable(py, build_event(py, event)?);
        }
        guard.wait_signal()
    } else {
        None
    };
    // Register the common uncontended waiter before returning the awaitable
    // to Python. A peer can close immediately after receive() is called; the
    // producer must not mistake that scheduling window for an abandoned app.
    let wait_guard = wait_signal.map(|signal| signal.begin_wait());

    // Slow path: a duck future resolved through the pump. The waiter task
    // owns the event between consumption and resolution; cancellation either
    // aborts the waiter before it consumes (mpsc recv is cancel-safe) or the
    // pump requeues the in-flight event.
    // Two references are intentional: Python owns the returned awaitable
    // while the pump owns its resolver until completion. Neither can be
    // dropped after the clone, so Clippy's merge suggestion is invalid here.
    let fut = new_rust_future(py, Arc::clone(shard))?;
    let waiter_fut = fut.clone_ref(py);
    let waiter_shard = Arc::clone(shard);
    let state = Arc::clone(state);
    let join = runtime().spawn(async move {
        let mut guard = state.lock_owned().await;
        let wait_guard =
            wait_guard.or_else(|| guard.wait_signal().map(|signal| signal.begin_wait()));
        let event = guard.next().await;
        let payload = ResolvePayload::Op(Box::new(ReceiveResolve {
            event,
            guard,
            build_event,
        }));
        waiter_shard.push(PumpEvent::Resolve {
            fut: waiter_fut,
            payload,
        });
        // Completing the guard after push is the cancellation ordering
        // handshake. Its Drop also runs when this waiter is aborted, so a
        // producer can never wait forever on a stale "receive pending" bit.
        drop(wait_guard);
    });
    fut.get().set_abort(join.abort_handle());
    Ok(fut.into_bound(py).into_any())
}

pub(crate) fn build_http_inbound_event(
    py: Python<'_>,
    event: HttpInboundEvent,
) -> PyResult<Py<PyAny>> {
    let (dict, credit) = match event {
        HttpInboundEvent::Request {
            body,
            more_body,
            credit,
        } => (
            py_dict!(py, {
                "type" => "http.request",
                if !body.is_empty() => {
                    "body" => PyBytes::new(py, body.as_ref()),
                },
                if more_body => {
                    "more_body" => true,
                },
            }),
            credit,
        ),
        HttpInboundEvent::RequestBatch {
            bodies,
            body_bytes,
            credit,
        } => {
            debug_assert_eq!(body_bytes, bodies.iter().map(Bytes::len).sum::<usize>());
            let body = PyBytes::new_with_writer(py, body_bytes, |writer| {
                if let [first, second] = bodies.as_slice() {
                    writer.write_all(first.as_ref())?;
                    writer.write_all(second.as_ref())?;
                } else {
                    for body in &*bodies {
                        writer.write_all(body.as_ref())?;
                    }
                }
                Ok(())
            })?;
            (
                py_dict!(py, {
                    "type" => "http.request",
                    "body" => body,
                    "more_body" => true,
                }),
                credit,
            )
        },
        HttpInboundEvent::HttpDisconnect => (
            py_dict!(py, {
                "type" => "http.disconnect",
            }),
            None,
        ),
    };
    if let Some(credit) = credit {
        credit.release();
    }
    Ok(dict.into_any().unbind())
}

pub(crate) fn build_websocket_inbound_event(
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
            "code" => code.get(),
            if let Some(reason) = reason.filter(|reason| !reason.is_empty()) => {
                "reason" => reason.as_str(),
            },
        }),
    };
    Ok(dict.into_any().unbind())
}

fn parse_headers(
    container: AsgiContainer,
    value: Option<Bound<'_, PyAny>>,
) -> Result<ResponseHeaders, H2CornError> {
    let Some(value) = value else {
        return Ok(ResponseHeaders::new());
    };

    if let Ok(list) = value.cast_exact::<PyList>()
        && let Some(headers) = try_parse_exact_header_list(list)?
    {
        return Ok(headers);
    }

    parse_header_iterable(container, &value)
}

fn parse_header_iterable(
    container: AsgiContainer,
    value: &Bound<'_, PyAny>,
) -> Result<ResponseHeaders, H2CornError> {
    let mut headers = ResponseHeaders::new();
    let items = value.try_iter().map_err(|_| {
        field_type_error(
            container,
            "headers",
            "an iterable of two-item (bytes, bytes) pairs",
            value,
        )
    })?;
    for item in items {
        let item = item.map_err(|_| {
            field_type_error(
                container,
                "headers",
                "an iterable of two-item (bytes, bytes) pairs",
                value,
            )
        })?;
        let pair = item.try_iter().map_err(|_| {
            field_type_error(container, "headers", "two-item (bytes, bytes) pairs", &item)
        })?;
        let mut fields = [None, None];
        let mut field_count = 0;
        for field in pair {
            let field = field.map_err(|_| {
                field_type_error(container, "headers", "two-item (bytes, bytes) pairs", &item)
            })?;
            if field_count < fields.len() {
                fields[field_count] = Some(field);
            }
            if field_count < 3 {
                field_count += 1;
            }
        }
        let [Some(name), Some(value)] = fields else {
            return Err(field_type_error(
                container,
                "headers",
                "two-item (bytes, bytes) pairs",
                &item,
            ));
        };
        if field_count != 2 {
            return Err(field_type_error(
                container,
                "headers",
                "two-item (bytes, bytes) pairs",
                &item,
            ));
        }
        let name = cast_exact_first::<PyBytes>(&name)
            .map_err(|_| {
                field_type_error(container, "headers", "two-item (bytes, bytes) pairs", &item)
            })?
            .to_owned();
        let value = cast_exact_first::<PyBytes>(&value)
            .map_err(|_| {
                field_type_error(container, "headers", "two-item (bytes, bytes) pairs", &item)
            })?
            .to_owned();
        let name = PyBackedBytes::from(name);
        let value = PyBackedBytes::from(value);
        headers.push(parse_response_header(name, value)?);
    }
    Ok(headers)
}

fn try_parse_exact_header_list(
    list: &Bound<'_, PyList>,
) -> Result<Option<ResponseHeaders>, H2CornError> {
    let mut headers = ResponseHeaders::with_capacity(list.len() + RESPONSE_DEFAULT_BUILTIN_SLOTS);
    for item in list.iter() {
        let Ok(tuple) = item.cast_exact::<pyo3::types::PyTuple>() else {
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

fn validate_application_response_headers(
    headers: &mut ResponseHeaders,
) -> Result<ResponseConnectionDirective, H2CornError> {
    let mut directive = ResponseConnectionDirective::default();
    let mut has_upgrade = false;
    let mut invalid = false;

    headers.retain(|(name, value)| {
        match application_response_field(name.as_bytes()) {
            // Transport-owned fields. ASGI explicitly assigns
            // `transfer-encoding` to the server, and the rest are hop-by-hop:
            // an application may send any of them and the server removes them
            // without comment.
            ApplicationResponseField::TransferEncoding
            | ApplicationResponseField::Te
            | ApplicationResponseField::KeepAlive
            | ApplicationResponseField::ProxyConnection => false,
            ApplicationResponseField::Connection => {
                for token in split_commas_bytes(value.as_bytes()).map(<[u8]>::trim_ascii) {
                    match token {
                        b"close" if directive != ResponseConnectionDirective::Upgrade => {
                            directive = ResponseConnectionDirective::Close;
                        },
                        b"keep-alive" => {},
                        b"upgrade" if directive != ResponseConnectionDirective::Close => {
                            directive = ResponseConnectionDirective::Upgrade;
                        },
                        _ => {
                            invalid = true;
                            return false;
                        },
                    }
                }
                false
            },
            ApplicationResponseField::Upgrade => {
                let valid = split_commas_bytes(value.as_bytes()).all(|token| {
                    let token = token.trim_ascii();
                    !token.is_empty() && protocol_token_is_valid(token)
                });
                invalid |= !valid || has_upgrade;
                has_upgrade = true;
                valid
            },
            ApplicationResponseField::Other => {
                if name.as_bytes() == b"trailer" {
                    // The ASGI trailers extension puts semantic announcement
                    // policy in the framework. Only its list grammar belongs
                    // to the server; actual trailer fields stay separately
                    // validated by ResponseTrailers.
                    split_commas_bytes(value.as_bytes()).all(|field| {
                        let field = field.trim_ascii();
                        !field.is_empty() && protocol_token_is_valid(field)
                    })
                } else {
                    true
                }
            },
        }
    });

    // `retain` cannot carry an error. Recheck the observable facts after the
    // one-pass transformation so invalid Connection/Upgrade syntax raises out
    // of send(), rather than turning into an accidental strip.
    if invalid || (directive == ResponseConnectionDirective::Upgrade) != has_upgrade {
        return Err(HttpResponseError::InvalidResponseHeaderValue.into());
    }
    Ok(directive)
}

fn extract_payload_bytes(
    container: AsgiContainer,
    field: &'static str,
    value: &Bound<'_, PyAny>,
) -> Result<PayloadBytes, H2CornError> {
    cast_exact_first::<PyBytes>(value).map_or_else(
        |_| {
            cast_exact_first::<PyByteArray>(value).map_or_else(
                |_| {
                    Err(field_type_error(
                        container,
                        field,
                        "bytes or bytearray",
                        value,
                    ))
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

fn extract_backed_str(
    container: AsgiContainer,
    field: &'static str,
    value: &Bound<'_, PyAny>,
) -> Result<PyBackedStr, H2CornError> {
    Ok(PyBackedStr::try_from(
        cast_exact_first::<PyString>(value)
            .map_err(|_| field_type_error(container, field, "a str", value))?
            .to_owned(),
    )?)
}

/// A non-negative integer field.
///
/// A negative `offset` or `count` is rejected here rather than wrapping into a
/// huge unsigned value, which would otherwise surface much later as a
/// nonsensical range. The range is named in the message: a Python integer of
/// 2^64 is both an int and non-negative, so reporting it as neither would send
/// the caller looking for the wrong mistake.
#[cfg(unix)]
fn extract_unsigned(
    container: AsgiContainer,
    field: &'static str,
    value: &Bound<'_, PyAny>,
) -> Result<u64, H2CornError> {
    cast_exact_first::<PyInt>(value)
        .map_err(|_| field_type_error(container, field, "an int", value))?
        .extract::<u64>()
        .map_err(|_| field_type_error(container, field, "an int in 0..=2**64-1", value))
}

fn field_type_error(
    container: AsgiContainer,
    field: &'static str,
    expected: &'static str,
    value: &Bound<'_, PyAny>,
) -> H2CornError {
    let actual = value
        .get_type()
        .name()
        .ok()
        .and_then(|name| name.to_str().ok().map(str::to_owned))
        .unwrap_or_else(|| String::from("unknown"));
    AsgiError::invalid_field_type(container, field, expected, actual.into()).into_error()
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

pub(crate) fn try_send_or_await<'py, T: Send + 'static>(
    py: Python<'py>,
    shard: &Shard,
    tx: &mpsc::Sender<T>,
    event: T,
) -> PyResult<Bound<'py, PyAny>> {
    match try_push(tx, event) {
        // The uncontended send never needs an owned handle; only the
        // backpressure arm, which spawns, does.
        TryPush::Sent => Ok(ready_none(py, shard)),
        TryPush::Full(event) => send_after_full(py, Arc::clone(shard), tx.clone(), event),
        TryPush::Closed(_) => Err(into_pyerr(AsgiError::SendAfterClose)),
    }
}

/// Resolve a Python awaitable from an asynchronous send that reports whether
/// the event was enqueued.
///
/// Cancellation aborts the waiter; an aborted send never enqueues, so the
/// message is consistently "not sent". The two `Py` references are
/// intentional: Python owns the returned awaitable while the pump owns its
/// resolver until completion, and neither can be dropped after the clone --
/// which is why Clippy's merge suggestion does not apply.
#[expect(
    clippy::significant_drop_tightening,
    reason = "the returned awaitable and pump resolver require independent Py references"
)]
fn await_send_result<F>(py: Python<'_>, shard: Shard, deliver: F) -> PyResult<Bound<'_, PyAny>>
where
    F: Future<Output = bool> + Send + 'static,
{
    let fut = new_rust_future(py, Arc::clone(&shard))?;
    let waiter_fut = fut.clone_ref(py);
    let waiter_shard = shard;
    let join = runtime().spawn(async move {
        let sent = deliver.await;
        let payload = ResolvePayload::Simple(Box::new(move |py| {
            if sent {
                Ok(py.None())
            } else {
                Err(into_pyerr(AsgiError::SendAfterClose))
            }
        }));
        waiter_shard.push(PumpEvent::Resolve {
            fut: waiter_fut,
            payload,
        });
    });
    fut.get().set_abort(join.abort_handle());
    Ok(fut.into_bound(py).into_any())
}

/// Install a waiter after a bounded channel has already reported full.
/// Taking the sender by value makes the refcount transition explicit and
/// keeps clones out of the uncontended path.
pub(crate) fn send_after_full<T: Send + 'static>(
    py: Python<'_>,
    shard: Shard,
    tx: mpsc::Sender<T>,
    event: T,
) -> PyResult<Bound<'_, PyAny>> {
    await_send_result(py, shard, async move { tx.send(event).await.is_ok() })
}

/// Resolve an HTTP ASGI send after its connection-wide byte credit and event
/// queue slot are both admitted. Cancellation drops any acquired credit.
pub(crate) fn await_http_send(
    py: Python<'_>,
    shard: Shard,
    waiter: HttpSendWaiter,
) -> PyResult<Bound<'_, PyAny>> {
    await_send_result(py, shard, async move { waiter.send().await })
}

pub(crate) fn parse_http_outbound_event(
    message: &Bound<'_, PyDict>,
) -> Result<HttpOutboundEvent, H2CornError> {
    let message = AsgiMessage::parse(message)?;

    match message.http_outbound_type()? {
        HttpOutboundType::Start => {
            let status = message.status(AsgiContainer::HttpResponseStart)?;
            let status = FinalResponseStatus::new(status).ok_or_else(|| {
                HttpResponseError::InformationalStatusUnsupported {
                    container: AsgiContainer::HttpResponseStart,
                    status: status.get(),
                }
            })?;
            let (headers, directive) =
                message.application_response_headers(AsgiContainer::HttpResponseStart)?;
            let trailers = message.trailers_flag(AsgiContainer::HttpResponseStart)?;
            Ok(HttpOutboundEvent::Start {
                status,
                headers,
                trailers,
                directive,
            })
        },
        HttpOutboundType::Body => {
            let body = message.body_or_empty(AsgiContainer::HttpResponseBody)?;
            let more_body = message.more_body_flag(AsgiContainer::HttpResponseBody)?;
            Ok(HttpOutboundEvent::Body { body, more_body })
        },
        HttpOutboundType::EarlyHint => Ok(HttpOutboundEvent::EarlyHint(
            message.early_hint_links(AsgiContainer::HttpResponseEarlyHint)?,
        )),
        HttpOutboundType::Pathsend => Ok(HttpOutboundEvent::PathSend {
            path: message.path(AsgiContainer::HttpResponsePathsend)?,
        }),
        #[cfg(unix)]
        HttpOutboundType::ZeroCopySend => {
            let container = AsgiContainer::HttpResponseZeroCopySend;
            let (file, start, len) = message.zerocopysend(container)?;
            Ok(HttpOutboundEvent::ZeroCopySend {
                file,
                start,
                len,
                more_body: message.more_body_flag(container)?,
            })
        },
        HttpOutboundType::Trailers => {
            let headers = message.response_trailers(AsgiContainer::HttpResponseTrailers)?;
            let more_trailers = message.more_trailers_flag(AsgiContainer::HttpResponseTrailers)?;
            Ok(HttpOutboundEvent::Trailers {
                headers,
                more_trailers,
            })
        },
    }
}

pub(crate) fn parse_websocket_outbound_event(
    message: &Bound<'_, PyDict>,
) -> Result<WebSocketOutboundEvent, H2CornError> {
    let message = AsgiMessage::parse(message)?;

    match message.websocket_outbound_type()? {
        WebSocketOutboundType::Accept => {
            let subprotocol = message.subprotocol(AsgiContainer::WebSocketAccept)?;
            let (mut headers, _) =
                message.application_response_headers(AsgiContainer::WebSocketAccept)?;
            // Connection is handled by the generic response policy above.
            // The WebSocket transport exclusively owns its Upgrade and Accept
            // fields, so application copies are deliberately ignored.
            headers.retain(|(name, _)| {
                !name.as_bytes().eq_ignore_ascii_case(b"upgrade")
                    && !name
                        .as_bytes()
                        .eq_ignore_ascii_case(b"sec-websocket-accept")
            });
            if headers.iter().any(|(name, _)| {
                let name = name.as_bytes();
                name.eq_ignore_ascii_case(SEC_WEBSOCKET_PROTOCOL_HEADER_BYTES)
                    || name.eq_ignore_ascii_case(SEC_WEBSOCKET_EXTENSIONS_HEADER_BYTES)
            }) {
                return WebSocketError::AcceptHeadersForbidden.err();
            }
            Ok(WebSocketOutboundEvent::Accept {
                subprotocol,
                headers,
            })
        },
        WebSocketOutboundType::Send => match message.websocket_send_payload()? {
            WebSocketSendPayload::Text(text) => Ok(WebSocketOutboundEvent::SendText(text)),
            WebSocketSendPayload::Bytes(data) => Ok(WebSocketOutboundEvent::SendBytes(data)),
        },
        WebSocketOutboundType::Close => {
            let code = message.close_code_or_default(AsgiContainer::WebSocketClose)?;
            let reason = message.reason(AsgiContainer::WebSocketClose)?;
            Ok(WebSocketOutboundEvent::Close { code, reason })
        },
        WebSocketOutboundType::HttpResponseStart => {
            let status = message.status(AsgiContainer::WebSocketHttpResponseStart)?;
            let status = FinalResponseStatus::new(status).ok_or_else(|| {
                HttpResponseError::InformationalStatusUnsupported {
                    container: AsgiContainer::WebSocketHttpResponseStart,
                    status: status.get(),
                }
            })?;
            let (headers, directive) =
                message.application_response_headers(AsgiContainer::WebSocketHttpResponseStart)?;
            Ok(WebSocketOutboundEvent::HttpResponseStart {
                status,
                headers,
                directive,
            })
        },
        WebSocketOutboundType::HttpResponseBody => {
            let body = message.body_or_empty(AsgiContainer::WebSocketHttpResponseBody)?;
            let more_body = message.more_body_flag(AsgiContainer::WebSocketHttpResponseBody)?;
            Ok(WebSocketOutboundEvent::HttpResponseBody { body, more_body })
        },
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CString;
    use std::future::pending;
    use std::num::NonZeroU32;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use bytes::Bytes;
    use pyo3::ffi::c_str;
    use pyo3::types::{
        PyAnyMethods as _, PyBytes, PyDict, PyDictMethods as _, PyList, PyString, PyTuple,
    };
    use pyo3::{PyResult, Python};
    use tokio::sync::{Mutex, mpsc, oneshot};

    use super::{
        AsgiMessage, EventSource, HttpInboundEvent, HttpOutboundType, ReceiveResolve, Requeueable,
        ResolveOp, WebSocketOutboundEvent, WebSocketOutboundType, build_http_inbound_event,
        parse_http_outbound_event, parse_websocket_outbound_event,
    };
    use crate::config::{ConfiguredResponseHeader, ResponseHeaderConfig};
    use crate::error::{AsgiContainer, AsgiError, ErrorKind, HttpResponseError, WebSocketError};
    use crate::h2_frame::StreamId;
    use crate::http::header::{
        ResponseConnectionDirective, inspect_response_headers,
        prepare_fixed_length_response_headers_with_scan,
    };
    use crate::http::types::{ResponseField, status_code};
    use crate::python::py_dict;
    use crate::runtime::H2InputCreditQueue;

    #[derive(Debug)]
    struct NeverEventSource;

    impl EventSource for NeverEventSource {
        type Event = HttpInboundEvent;

        fn try_pull(&mut self) -> Option<Self::Event> {
            None
        }

        async fn pull(&mut self) -> Self::Event {
            pending().await
        }
    }

    #[derive(Debug)]
    struct CountingEventSource {
        rx: mpsc::Receiver<HttpInboundEvent>,
        pulls: Arc<AtomicUsize>,
    }

    impl EventSource for CountingEventSource {
        type Event = HttpInboundEvent;

        fn try_pull(&mut self) -> Option<Self::Event> {
            let event = self.rx.try_recv().ok()?;
            self.pulls.fetch_add(1, Ordering::Relaxed);
            Some(event)
        }

        async fn pull(&mut self) -> Self::Event {
            let event = self.rx.recv().await.expect("test input remains open");
            self.pulls.fetch_add(1, Ordering::Relaxed);
            event
        }
    }

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
                err.kind(),
                ErrorKind::Asgi(AsgiError::MissingField {
                    container: AsgiContainer::HttpResponseStart,
                    field: "status",
                })
            ));
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn asgi_message_boundaries_name_the_event_and_bad_field() {
        init_python();
        Python::attach(|py| -> PyResult<()> {
            let message = py_dict!(py, { "type" => 1 });
            assert_eq!(
                parse_http_outbound_event(&message).unwrap_err().to_string(),
                "ASGI message type must be a str, got int"
            );

            let message = py_dict!(py, {
                "type" => "http.response.start",
                "status" => "200",
            });
            assert_eq!(
                parse_http_outbound_event(&message).unwrap_err().to_string(),
                "http.response.start status must be an int, got str"
            );

            let message = py_dict!(py, {
                "type" => "http.response.pathsend",
                "path" => 1,
            });
            assert_eq!(
                parse_http_outbound_event(&message).unwrap_err().to_string(),
                "http.response.pathsend path must be a str, got int"
            );

            let message = py_dict!(py, {
                "type" => "http.response.body",
                "more_body" => "yes",
            });
            assert_eq!(
                parse_http_outbound_event(&message).unwrap_err().to_string(),
                "http.response.body more_body must be a bool, got str"
            );

            let message = py_dict!(py, {
                "type" => "http.response.start",
                "status" => 200,
                "headers" => [1],
            });
            assert_eq!(
                parse_http_outbound_event(&message).unwrap_err().to_string(),
                "http.response.start headers must be two-item (bytes, bytes) pairs, got int"
            );

            let message = py_dict!(py, {
                "type" => "websocket.close",
                "code" => "1000",
            });
            assert_eq!(
                parse_websocket_outbound_event(&message)
                    .unwrap_err()
                    .to_string(),
                "websocket.close code must be an int, got str"
            );

            let message = py_dict!(py, {
                "type" => "websocket.close",
                "code" => 0,
            });
            assert_eq!(
                parse_websocket_outbound_event(&message)
                    .unwrap_err()
                    .to_string(),
                "websocket close code is invalid"
            );
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn asgi_outbound_type_dispatches_interned_values_and_falls_back_to_strings() {
        init_python();
        Python::attach(|py| -> PyResult<()> {
            // Driven from the same table the dispatch is generated from, so
            // a new message type is covered the moment it is declared. The
            // property that matters is that every one of them takes the
            // interned pointer path: a miss there is only slower, never wrong,
            // so nothing else would report it.
            for (expected, text) in HttpOutboundType::WIRE_NAMES {
                let message = PyDict::new(py);
                message.set_item("type", PyString::intern(py, text))?;
                let message = AsgiMessage::parse(&message).unwrap();
                assert_eq!(message.interned_http_outbound_type(), Some(*expected));
                assert_eq!(message.http_outbound_type().unwrap(), *expected);
            }

            for (expected, text) in WebSocketOutboundType::WIRE_NAMES {
                let message = PyDict::new(py);
                message.set_item("type", PyString::intern(py, text))?;
                let message = AsgiMessage::parse(&message).unwrap();
                assert_eq!(message.interned_websocket_outbound_type(), Some(*expected));
                assert_eq!(message.websocket_outbound_type().unwrap(), *expected);
            }

            let dynamic = PyString::new(py, "http.response.body");
            assert!(!dynamic.is(PyString::intern(py, "http.response.body")));
            let message = PyDict::new(py);
            message.set_item("type", dynamic)?;
            let message = AsgiMessage::parse(&message).unwrap();
            assert_eq!(message.interned_http_outbound_type(), None);
            assert_eq!(
                message.http_outbound_type().unwrap(),
                HttpOutboundType::Body
            );

            let subclass = py.eval(
                c_str!("type('MessageType', (str,), {} )('websocket.send')"),
                None,
                None,
            )?;
            let message = PyDict::new(py);
            message.set_item("type", subclass)?;
            let message = AsgiMessage::parse(&message).unwrap();
            assert_eq!(message.interned_websocket_outbound_type(), None);
            assert_eq!(
                message.websocket_outbound_type().unwrap(),
                WebSocketOutboundType::Send
            );

            let message = PyDict::new(py);
            message.set_item("type", PyString::new(py, "http.response.unknown"))?;
            assert_eq!(
                parse_http_outbound_event(&message).unwrap_err().to_string(),
                "unsupported http ASGI outbound message: http.response.unknown"
            );
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
    fn unexpected_websocket_event_reports_its_asgi_type_not_application_payload() {
        init_python();
        Python::attach(|py| -> PyResult<()> {
            let message = py_dict!(py, {
                "type" => "websocket.send",
                "text" => "sk-live-SECRET-TOKEN-12345",
            });

            let event = parse_websocket_outbound_event(&message).unwrap();
            let error = WebSocketError::unexpected_initial_event(event.message_type());
            assert_eq!(
                error.to_string(),
                "unexpected websocket.send before handshake; the app must send websocket.accept or websocket.close first"
            );
            assert!(!error.to_string().contains("sk-live-SECRET-TOKEN-12345"));
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
                    err.kind(),
                    ErrorKind::Asgi(AsgiError::WebSocketSendRequiresExactlyOnePayload)
                ));
            }
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn websocket_accept_owns_handshake_headers_at_ingress() {
        init_python();
        Python::attach(|py| -> PyResult<()> {
            let message = py_dict!(py, {
                "type" => "websocket.accept",
                "headers" => [
                    (PyBytes::new(py, b"x-before"), PyBytes::new(py, b"one")),
                    (PyBytes::new(py, b"connection"), PyBytes::new(py, b"upgrade")),
                    (PyBytes::new(py, b"upgrade"), PyBytes::new(py, b"not-websocket")),
                    (PyBytes::new(py, b"sec-websocket-accept"), PyBytes::new(py, b"bogus")),
                    (PyBytes::new(py, b"x-after"), PyBytes::new(py, b"two")),
                ],
            });

            let WebSocketOutboundEvent::Accept { headers, .. } =
                parse_websocket_outbound_event(&message).expect("accept headers are parsed")
            else {
                panic!("websocket.accept must produce an accept event");
            };
            let fields = headers
                .iter()
                .map(|(name, value)| (name.as_bytes(), value.as_bytes()))
                .collect::<Vec<_>>();
            assert_eq!(fields, [
                (b"x-before".as_slice(), b"one".as_slice()),
                (b"x-after".as_slice(), b"two".as_slice())
            ]);

            for name in [
                b"sec-websocket-protocol".as_slice(),
                b"sec-websocket-extensions",
            ] {
                let message = py_dict!(py, {
                    "type" => "websocket.accept",
                    "headers" => [(PyBytes::new(py, name), PyBytes::new(py, b"value"))],
                });
                assert!(
                    parse_websocket_outbound_event(&message).is_err(),
                    "{name:?}"
                );
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
                super::HttpOutboundEvent::Start { status, trailers: false, headers, .. }
                    if status.get() == status_code::OK && headers.len() == 1
                        && headers[0].0.as_ref() == b"content-length"
                        && headers[0].1.as_ref() == b"2"
            ));
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn response_status_validation_handles_python_integer_boundaries() {
        init_python();
        Python::attach(|py| -> PyResult<()> {
            for status in [-1_i64, 0, 99] {
                let message = py_dict!(py, {
                    "type" => "http.response.start",
                    "status" => status,
                });
                let err = parse_http_outbound_event(&message).unwrap_err();
                assert!(matches!(
                    err.kind(),
                    ErrorKind::HttpResponse(HttpResponseError::StatusMustBeThreeDigitCode { .. })
                ));
                assert_eq!(
                    err.to_string(),
                    format!("http.response.start status must be a three-digit code, got {status}")
                );
            }

            let too_large = py.eval(c_str!("2**63"), None, None)?;
            let message = py_dict!(py, {
                "type" => "http.response.start",
                "status" => too_large,
            });
            assert_eq!(
                parse_http_outbound_event(&message).unwrap_err().to_string(),
                "http.response.start status must be a three-digit code, got an integer outside the signed 64-bit range"
            );

            for status in [100_u16, 199] {
                let message = py_dict!(py, {
                    "type" => "http.response.start",
                    "status" => status,
                });
                assert!(matches!(
                    parse_http_outbound_event(&message).unwrap_err().kind(),
                    ErrorKind::HttpResponse(HttpResponseError::InformationalStatusUnsupported {
                        status: actual,
                        ..
                    }) if *actual == status
                ));
            }

            {
                let status = 600_u16;
                let message = py_dict!(py, {
                    "type" => "http.response.start",
                    "status" => status,
                });
                assert!(matches!(
                    parse_http_outbound_event(&message),
                    Ok(super::HttpOutboundEvent::Start { status: actual, .. })
                        if actual.get().get() == status
                ));
            }

            let bool_message = py_dict!(py, {
                "type" => "http.response.start",
                "status" => true,
            });
            assert_eq!(
                parse_http_outbound_event(&bool_message).unwrap_err().to_string(),
                "http.response.start status must be a three-digit code, got 1"
            );

            let subclass = py.eval(c_str!("type('Status', (int,), {})(201)"), None, None)?;
            let subclass_message = py_dict!(py, {
                "type" => "http.response.start",
                "status" => subclass,
            });
            assert!(matches!(
                parse_http_outbound_event(&subclass_message),
                Ok(super::HttpOutboundEvent::Start { status, .. }) if status.get().get() == 201
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
                super::HttpOutboundEvent::Start { status, trailers: false, headers, .. }
                    if status.get() == status_code::OK && headers.len() == 1
                        && headers[0].0.as_ref() == b"x-demo"
                        && headers[0].1.as_ref() == b"1"
            ));
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn response_header_iterable_never_calls_len() {
        init_python();
        Python::attach(|py| -> PyResult<()> {
            let locals = PyDict::new(py);
            py.run(
                c_str!(
                    r#"
import sys
class Headers:
    def __init__(self, kind):
        self.kind = kind
    def __iter__(self):
        return iter(((b"x-test", b"ok"),))
    def __len__(self):
        if self.kind == 0:
            raise AssertionError("__len__ must not run")
        if self.kind == 1:
            return sys.maxsize
        return -1
headers = (Headers(0), Headers(1), Headers(2))
"#
                ),
                Some(&locals),
                Some(&locals),
            )?;
            let header_sets = locals
                .get_item("headers")?
                .expect("test created header iterables");
            for headers in header_sets.try_iter()? {
                let message = PyDict::new(py);
                message.set_item("type", "http.response.start")?;
                message.set_item("status", 200)?;
                message.set_item("headers", headers?)?;
                parse_http_outbound_event(&message).unwrap();
            }
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn generic_header_pairs_preserve_iterator_error_precedence() {
        init_python();
        Python::attach(|py| -> PyResult<()> {
            let locals = PyDict::new(py);
            py.run(
                c_str!(
                    r#"
class Pair:
    def __init__(self, values, error=None):
        self.values = values
        self.error = error
        self.index = 0
        self.next_calls = 0
    def __iter__(self):
        return self
    def __next__(self):
        self.next_calls += 1
        if self.index < len(self.values):
            value = self.values[self.index]
            self.index += 1
            return value
        if self.error:
            raise RuntimeError(self.error)
        raise StopIteration

class Headers:
    def __init__(self, pair, error=None):
        self.pair = pair
        self.error = error
    def __iter__(self):
        yield self.pair
        if self.error:
            raise RuntimeError(self.error)
"#
                ),
                Some(&locals),
                Some(&locals),
            )?;
            let cases = [
                ("Pair([], None)", Some("http.response.start headers must be two-item (bytes, bytes) pairs, got Pair")),
                ("Pair([b'x'], None)", Some("http.response.start headers must be two-item (bytes, bytes) pairs, got Pair")),
                ("Pair([b'x', b'y'], None)", None),
                ("Pair([b'x', b'y', b'z'], None)", Some("http.response.start headers must be two-item (bytes, bytes) pairs, got Pair")),
                ("Pair([1, b'y', b'z'])", Some("http.response.start headers must be two-item (bytes, bytes) pairs, got Pair")),
                ("Pair([b'x', 1, b'z'])", Some("http.response.start headers must be two-item (bytes, bytes) pairs, got Pair")),
                ("Pair([1, b'y'], None)", Some("http.response.start headers must be two-item (bytes, bytes) pairs, got Pair")),
                ("Pair([b'x', 1], None)", Some("http.response.start headers must be two-item (bytes, bytes) pairs, got Pair")),
                ("Pair([b'x', b'y'], 'late')", Some("http.response.start headers must be two-item (bytes, bytes) pairs, got Pair")),
            ];
            for (pair, expected) in cases {
                let pair_code = CString::new(pair).unwrap();
                let pair = py.eval(pair_code.as_c_str(), Some(&locals), None)?;
                locals.set_item("pair", &pair)?;
                let headers = py.eval(c_str!("Headers(pair)"), Some(&locals), None)?;
                let message = py_dict!(py, {
                    "type" => "http.response.start",
                    "status" => 200,
                    "headers" => headers,
                });
                let result = parse_http_outbound_event(&message);
                if let Some(expected) = expected {
                    assert_eq!(result.unwrap_err().to_string(), expected, "{pair}");
                } else {
                    assert!(result.is_ok(), "{pair}");
                }
            }

            let pair = py.eval(c_str!("Pair([b'x', b'y', b'z'], 'late')"), Some(&locals), None)?;
            locals.set_item("pair", &pair)?;
            let headers = py.eval(c_str!("Headers(pair)"), Some(&locals), None)?;
            let message = py_dict!(py, {
                "type" => "http.response.start",
                "status" => 200,
                "headers" => headers,
            });
            assert_eq!(pair.getattr("next_calls")?.extract::<usize>()?, 0);
            assert_eq!(
                parse_http_outbound_event(&message).unwrap_err().to_string(),
                "http.response.start headers must be two-item (bytes, bytes) pairs, got Pair"
            );
            assert_eq!(pair.getattr("next_calls")?.extract::<usize>()?, 4);

            let pair = py.eval(c_str!("Pair([b'x', b'y'], 'inner')"), Some(&locals), None)?;
            locals.set_item("pair", &pair)?;
            let headers = py.eval(c_str!("Headers(pair)"), Some(&locals), None)?;
            let message = py_dict!(py, {
                "type" => "http.response.start",
                "status" => 200,
                "headers" => headers,
            });
            assert!(
                parse_http_outbound_event(&message)
                    .unwrap_err()
                    .to_string()
                    .contains("two-item (bytes, bytes) pairs")
            );

            let message = py_dict!(py, {
                "type" => "http.response.start",
                "status" => 200,
                "headers" => 1,
            });
            assert!(
                parse_http_outbound_event(&message)
                    .unwrap_err()
                    .to_string()
                    .contains("an iterable of two-item (bytes, bytes) pairs")
            );

            let pair = py.eval(c_str!("Pair([b'x', b'y'], None)"), Some(&locals), None)?;
            locals.set_item("pair", &pair)?;
            let headers = py.eval(c_str!("Headers(pair, 'outer')"), Some(&locals), None)?;
            let message = py_dict!(py, {
                "type" => "http.response.start",
                "status" => 200,
                "headers" => headers,
            });
            assert!(
                parse_http_outbound_event(&message)
                    .unwrap_err()
                    .to_string()
                    .contains("an iterable of two-item (bytes, bytes) pairs")
            );
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn exact_header_list_reserves_all_builtin_default_slots() {
        init_python();
        Python::attach(|py| -> PyResult<()> {
            // The ingress reserve covers `server`, `date`, and
            // `content-length`; the default helper separately reserves any
            // configured extras because their count belongs to configuration.
            assert_eq!(std::mem::size_of::<ResponseField>(), 80);

            let one = PyList::new(py, [(PyBytes::new(py, b"x-app"), PyBytes::new(py, b"one"))])?;
            let mut headers = super::try_parse_exact_header_list(&one)
                .expect("response header list parses")
                .expect("exact list stays on the exact path");
            assert_eq!(headers.capacity(), headers.len() + 3);
            let allocation = headers.as_ptr();
            let defaults = ResponseHeaderConfig {
                server_header: Some(Bytes::from_static(b"h2corn")),
                date_header: true,
                extra_headers: Box::new([]),
            };
            let mut scan = inspect_response_headers(&headers);
            prepare_fixed_length_response_headers_with_scan(&mut headers, &mut scan, &defaults, 3);
            assert_eq!(headers.as_ptr(), allocation, "built-ins do not reallocate");
            assert_eq!(headers.capacity(), 4);
            assert_eq!(headers.len(), 4);

            // This is the old `list.len()` capacity. It has one 80-byte
            // field allocation, and default preparation grows it once to the
            // four-field (320-byte) allocation. The production path above
            // must remain the no-growth counterpart of this control.
            let mut old_capacity = super::try_parse_exact_header_list(&one)
                .expect("response header list parses")
                .expect("exact list stays on the exact path");
            old_capacity.shrink_to_fit();
            assert_eq!(old_capacity.capacity(), 1);
            let old_allocation = old_capacity.as_ptr();
            let mut scan = inspect_response_headers(&old_capacity);
            prepare_fixed_length_response_headers_with_scan(
                &mut old_capacity,
                &mut scan,
                &defaults,
                3,
            );
            assert_ne!(
                old_capacity.as_ptr(),
                old_allocation,
                "one field grows from 80 to 320 bytes"
            );
            assert_eq!(old_capacity.capacity(), 4);

            // Application-provided built-ins leave the three reserved slots
            // unused; disabled server/date therefore cannot grow this vector.
            let supplied = PyList::new(py, [
                (PyBytes::new(py, b"server"), PyBytes::new(py, b"app")),
                (PyBytes::new(py, b"date"), PyBytes::new(py, b"date")),
                (PyBytes::new(py, b"content-length"), PyBytes::new(py, b"3")),
            ])?;
            let mut supplied = super::try_parse_exact_header_list(&supplied)
                .expect("response header list parses")
                .expect("exact list stays on the exact path");
            let allocation = supplied.as_ptr();
            let defaults = ResponseHeaderConfig::default();
            let mut scan = inspect_response_headers(&supplied);
            prepare_fixed_length_response_headers_with_scan(&mut supplied, &mut scan, &defaults, 3);
            assert_eq!(supplied.as_ptr(), allocation);
            assert_eq!(supplied.len(), 3);

            // Configured extras retain their existing exact reserve in the
            // default helper and are appended once, after application fields.
            let empty = PyList::empty(py);
            let mut configured = super::try_parse_exact_header_list(&empty)
                .expect("response header list parses")
                .expect("exact list stays on the exact path");
            let defaults = ResponseHeaderConfig {
                server_header: None,
                date_header: false,
                extra_headers: Box::new([ConfiguredResponseHeader::new(
                    Bytes::from_static(b"x-configured"),
                    Bytes::from_static(b"yes"),
                )]),
            };
            let mut scan = inspect_response_headers(&configured);
            prepare_fixed_length_response_headers_with_scan(
                &mut configured,
                &mut scan,
                &defaults,
                0,
            );
            assert_eq!(
                configured
                    .iter()
                    .map(|(name, _)| name.as_bytes())
                    .collect::<Vec<_>>(),
                [b"x-configured".as_slice(), b"content-length"],
            );
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn response_headers_and_trailers_enforce_their_own_field_policies() {
        init_python();
        Python::attach(|py| -> PyResult<()> {
            for name in [b"upgrade".as_slice(), b"connection"] {
                let message = py_dict!(py, {
                    "type" => "http.response.start",
                    "status" => 200,
                    "headers" => [(PyBytes::new(py, name), PyBytes::new(py, b"x"))],
                });
                assert!(parse_http_outbound_event(&message).is_err(), "{name:?}");
            }

            let message = py_dict!(py, {
                "type" => "http.response.start",
                "status" => 200,
                "headers" => [
                    (PyBytes::new(py, b"Transfer-Encoding"), PyBytes::new(py, b"gzip")),
                    (PyBytes::new(py, b"TE"), PyBytes::new(py, b"trailers")),
                    (PyBytes::new(py, b"Content-Type"), PyBytes::new(py, b"text/plain")),
                ],
            });
            let event = parse_http_outbound_event(&message).expect("fixed fields are stripped");
            assert!(matches!(
                event,
                super::HttpOutboundEvent::Start { headers, .. }
                    if headers.len() == 1
                        && headers[0].0.as_ref() == b"content-type"
            ));

            let message = py_dict!(py, {
                "type" => "http.response.start",
                "status" => 200,
                "headers" => [
                    (PyBytes::new(py, b"connection"), PyBytes::new(py, b"content-security-policy")),
                    (PyBytes::new(py, b"content-security-policy"), PyBytes::new(py, b"default-src 'self'")),
                ],
            });
            let _error = parse_http_outbound_event(&message).unwrap_err();

            let message = py_dict!(py, {
                "type" => "http.response.start",
                "status" => 103,
            });
            let _error = parse_http_outbound_event(&message).unwrap_err();

            for value in [b"".as_slice(), b"internal\t whitespace"] {
                let message = py_dict!(py, {
                    "type" => "http.response.start",
                    "status" => 200,
                    "headers" => [(PyBytes::new(py, b"x-test"), PyBytes::new(py, value))],
                });
                assert!(parse_http_outbound_event(&message).is_ok(), "{value:?}");
            }
            for value in [
                b" leading".as_slice(),
                b"trailing ",
                b"\tleading",
                b"trailing\t",
            ] {
                let message = py_dict!(py, {
                    "type" => "http.response.start",
                    "status" => 200,
                    "headers" => [(PyBytes::new(py, b"x-test"), PyBytes::new(py, value))],
                });
                assert!(parse_http_outbound_event(&message).is_err(), "{value:?}");
            }

            for name in [
                b"content-length".as_slice(),
                b"authorization",
                b"content-type",
            ] {
                let message = py_dict!(py, {
                    "type" => "http.response.trailers",
                    "headers" => [(PyBytes::new(py, name), PyBytes::new(py, b"x"))],
                });
                assert!(parse_http_outbound_event(&message).is_err(), "{name:?}");
            }
            for name in [b"x-checksum".as_slice(), b"content-digest"] {
                let message = py_dict!(py, {
                    "type" => "http.response.trailers",
                    "headers" => [(PyBytes::new(py, name), PyBytes::new(py, b"x"))],
                });
                assert!(parse_http_outbound_event(&message).is_ok(), "{name:?}");
            }
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn response_connection_directive_has_exactly_one_meaning() {
        init_python();
        Python::attach(|py| -> PyResult<()> {
            for (connection, upgrade, expected) in [
                (
                    b"close".as_slice(),
                    false,
                    ResponseConnectionDirective::Close,
                ),
                (b"upgrade", true, ResponseConnectionDirective::Upgrade),
                (b"keep-alive", false, ResponseConnectionDirective::None),
                (b"close, close", false, ResponseConnectionDirective::Close),
                (
                    b"upgrade, upgrade",
                    true,
                    ResponseConnectionDirective::Upgrade,
                ),
            ] {
                let mut headers = vec![(
                    PyBytes::new(py, b"connection"),
                    PyBytes::new(py, connection),
                )];
                if upgrade {
                    headers.push((PyBytes::new(py, b"upgrade"), PyBytes::new(py, b"h2c")));
                }
                let message = py_dict!(py, {
                    "type" => "http.response.start",
                    "status" => 200,
                    "headers" => headers,
                });
                let event = parse_http_outbound_event(&message)
                    .expect("valid response connection directive");
                assert!(matches!(
                    event,
                    super::HttpOutboundEvent::Start { directive, .. }
                        if directive == expected
                ));
            }

            let contradictory = py_dict!(py, {
                "type" => "http.response.start",
                "status" => 200,
                "headers" => [
                    (PyBytes::new(py, b"connection"), PyBytes::new(py, b"close, upgrade")),
                    (PyBytes::new(py, b"upgrade"), PyBytes::new(py, b"h2c")),
                ],
            });
            let _ = parse_http_outbound_event(&contradictory)
                .expect_err("contradictory connection directives are rejected");
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
                credit: None,
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

    #[test]
    fn batched_http_request_materializes_one_contiguous_python_body() {
        init_python();
        Python::attach(|py| -> PyResult<()> {
            let event = build_http_inbound_event(py, HttpInboundEvent::RequestBatch {
                bodies: vec![
                    Bytes::from_static(b"segmented-"),
                    Bytes::from_static(b"body"),
                ],
                body_bytes: b"segmented-body".len(),
                credit: None,
            })?
            .bind(py)
            .clone()
            .cast_into::<PyDict>()?;

            assert_eq!(
                event
                    .get_item("body")?
                    .expect("body is present")
                    .extract::<Vec<u8>>()?,
                b"segmented-body"
            );
            assert!(
                event
                    .get_item("more_body")?
                    .expect("more_body is present")
                    .extract::<bool>()?
            );
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn cancelled_receive_requeue_retains_h2_credit_until_conversion() {
        init_python();
        let flow = Arc::new(H2InputCreditQueue::default());
        let stream_id = StreamId::new(1).expect("non-zero stream id");
        let mut receive = Requeueable::new(NeverEventSource);
        receive.requeue(HttpInboundEvent::Request {
            body: Bytes::from_static(b"body"),
            more_body: true,
            credit: Some(flow.credit(stream_id, NonZeroU32::new(4).unwrap())),
        });

        assert!(!flow.has_pending(), "requeue must retain receive credit");
        let event = receive
            .try_next()
            .expect("requeued event is returned first");
        assert!(
            !flow.has_pending(),
            "pulling alone must retain receive credit"
        );
        Python::attach(|py| build_http_inbound_event(py, event)).expect("event converts");

        assert!(flow.has_pending(), "conversion commits receive credit");
        let mut released = Vec::new();
        flow.drain_into(&mut released);
        assert_eq!(released.len(), 1);
        assert_eq!(released[0].stream_id, stream_id);
        assert_eq!(released[0].bytes.get(), 4);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "the test deliberately retains the owned receive guard until cancellation resolution"
    )]
    async fn cancelled_in_flight_receive_requeues_before_a_concurrent_waiter() {
        init_python();
        let flow = Arc::new(H2InputCreditQueue::default());
        let stream_id = StreamId::new(1).expect("non-zero stream id");
        let pulls = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = mpsc::channel(2);
        tx.send(HttpInboundEvent::Request {
            body: Bytes::from_static(b"first"),
            more_body: true,
            credit: Some(flow.credit(stream_id, NonZeroU32::new(5).unwrap())),
        })
        .await
        .expect("first input is queued");
        tx.send(HttpInboundEvent::Request {
            body: Bytes::from_static(b"second"),
            more_body: true,
            credit: None,
        })
        .await
        .expect("second input is queued");

        let state = Arc::new(Mutex::new(Requeueable::new(CountingEventSource {
            rx,
            pulls: Arc::clone(&pulls),
        })));
        let mut first_guard = Arc::clone(&state).lock_owned().await;
        let first = first_guard.next().await;
        // Model the interval after the Tokio waiter consumed the event but
        // before the Python-loop pump resolves its RustFuture.
        let resolution: Box<dyn ResolveOp + Send> = Box::new(ReceiveResolve {
            event: first,
            guard: first_guard,
            build_event: build_http_inbound_event,
        });
        assert_eq!(pulls.load(Ordering::Relaxed), 1);
        assert!(!flow.has_pending(), "consumption alone retains H2 credit");

        let second_state = Arc::clone(&state);
        let (attempted_tx, attempted_rx) = oneshot::channel();
        let second = tokio::spawn(async move {
            let _ = attempted_tx.send(());
            let mut guard = second_state.lock_owned().await;
            guard.next().await
        });
        attempted_rx
            .await
            .expect("the concurrent receive attempted the state lock");
        tokio::task::yield_now().await;
        assert!(
            !second.is_finished(),
            "one consumed event stays exclusively owned until pump resolution"
        );
        assert_eq!(
            pulls.load(Ordering::Relaxed),
            1,
            "the concurrent receive cannot consume the following source event"
        );

        // Cancellation wins at pump resolution. Requeue is synchronous while
        // the resolver still owns the mutex, so the next waiter must observe
        // the exact consumed event before touching the source again.
        resolution.requeue();
        let first_again = second.await.expect("concurrent receive completes");
        assert_eq!(pulls.load(Ordering::Relaxed), 1);
        let HttpInboundEvent::Request { body, .. } = &first_again else {
            panic!("the first request event is preserved")
        };
        assert_eq!(body.as_ref(), b"first");
        assert!(!flow.has_pending(), "requeue still retains H2 credit");

        Python::attach(|py| build_http_inbound_event(py, first_again))
            .expect("the requeued event converts");
        assert!(flow.has_pending(), "conversion releases H2 credit once");
        let mut released = Vec::new();
        flow.drain_into(&mut released);
        assert_eq!(released.len(), 1);
        assert_eq!(released[0].stream_id, stream_id);
        assert_eq!(released[0].bytes.get(), 5);
        assert!(!flow.has_pending());

        let mut guard = state.lock().await;
        let second_event = guard.next().await;
        drop(guard);
        assert_eq!(pulls.load(Ordering::Relaxed), 2);
        let HttpInboundEvent::Request { body, .. } = second_event else {
            panic!("the following request event is preserved")
        };
        assert_eq!(body.as_ref(), b"second");
    }
}
