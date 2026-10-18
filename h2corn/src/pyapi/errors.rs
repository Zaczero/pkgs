use pyo3::exceptions::{PyOSError, PyRuntimeError, PyTypeError, PyValueError};
use pyo3::{PyErr, PyResult};

use crate::error::{
    AsgiError, ErrorKind, H2CornError, HttpResponseError, PathsendError, WebSocketError,
};

pub(crate) trait IntoPyResult<T> {
    fn into_pyresult(self) -> PyResult<T>;
}

impl<T> IntoPyResult<T> for Result<T, H2CornError> {
    fn into_pyresult(self) -> PyResult<T> {
        self.map_err(into_pyerr)
    }
}

pyo3::create_exception!(
    h2corn._lib,
    SendAfterCloseError,
    PyOSError,
    "Raised when an application sends on a stream the server has already closed."
);

pub(crate) fn into_pyerr<E>(err: E) -> PyErr
where
    E: Into<H2CornError>,
{
    match err.into().into_kind() {
        ErrorKind::Python(err) => err,
        ErrorKind::Io(err) => err.into(),
        ErrorKind::Join(err) => PyRuntimeError::new_err(format!("background task failed: {err}")),
        ErrorKind::Config(err) => PyValueError::new_err(err.to_string()),
        // A wrong Python type in an outbound message.
        ErrorKind::Asgi(err @ AsgiError::InvalidFieldType { .. }) => {
            PyTypeError::new_err(err.to_string())
        },
        // Use of a stream the transport has already closed.
        ErrorKind::Asgi(AsgiError::SendAfterClose) => {
            SendAfterCloseError::new_err(AsgiError::SendAfterClose.to_string())
        },
        ErrorKind::Asgi(
            err @ (AsgiError::MissingField { .. }
            | AsgiError::WebSocketSendRequiresExactlyOnePayload),
        ) => PyValueError::new_err(err.to_string()),
        ErrorKind::Asgi(err @ AsgiError::UnsupportedOutboundMessage { .. }) => {
            PyValueError::new_err(err.to_string())
        },
        // Separate arms rather than three more alternatives below: `#[cfg]`
        // applies to a match arm but not to one alternative of an `|` pattern,
        // and these variants do not exist off Unix.
        #[cfg(unix)]
        ErrorKind::HttpResponse(
            err @ (HttpResponseError::ZeroCopySendNotRegularFile
            | HttpResponseError::ZeroCopySendNotReadable
            | HttpResponseError::ZeroCopySendLengthUnknown),
        ) => PyValueError::new_err(err.to_string()),
        ErrorKind::HttpResponse(
            err @ (HttpResponseError::InvalidResponseHeaderName
            | HttpResponseError::InvalidResponseHeaderValue
            | HttpResponseError::InvalidResponseTrailerField
            | HttpResponseError::StatusMustBeThreeDigitCode { .. }
            | HttpResponseError::StatusOutsideSigned64BitRange { .. }
            | HttpResponseError::InformationalStatusUnsupported { .. }
            | HttpResponseError::EarlyHintBeforeStart),
        ) => PyValueError::new_err(err.to_string()),
        // Messages that are individually well formed but arrive in an order
        // the response state machine does not allow.
        #[cfg(unix)]
        ErrorKind::HttpResponse(err @ HttpResponseError::ZeroCopySendBeforeStart) => {
            PyRuntimeError::new_err(err.to_string())
        },
        ErrorKind::HttpResponse(
            err @ (HttpResponseError::StartAlreadyReceived
            | HttpResponseError::TrailersNotAdvertised
            | HttpResponseError::BodyBeforeStart
            | HttpResponseError::PathsendBeforeStart
            | HttpResponseError::PathsendMixedWithBody
            | HttpResponseError::TrailersBeforeBodyCompleted
            | HttpResponseError::AppReturnedWithoutStartingResponse
            | HttpResponseError::AppReturnedWithoutCompletingResponse),
        ) => PyRuntimeError::new_err(err.to_string()),
        ErrorKind::WebSocket(
            err @ (WebSocketError::CloseReasonTooLong
            | WebSocketError::CloseCodeInvalid
            | WebSocketError::AcceptSubprotocolEmpty
            | WebSocketError::AcceptHeadersForbidden
            | WebSocketError::AcceptSubprotocolNotRequested),
        ) => PyValueError::new_err(err.to_string()),
        // Peer, transport and sequencing conditions. None of these describe a
        // value the application supplied, so none of them are `ValueError`.
        ErrorKind::WebSocket(
            err @ (WebSocketError::Protocol(_)
            | WebSocketError::HandshakeTimedOut
            | WebSocketError::CompressionFailed
            | WebSocketError::ReceiveChannelClosed { .. }
            | WebSocketError::AppEndedBeforeHandshake
            | WebSocketError::UnexpectedEvent { .. }),
        ) => PyRuntimeError::new_err(err.to_string()),
        ErrorKind::Pathsend(PathsendError::OpenFailed { path, source }) => {
            let error = PyRuntimeError::new_err(format!(
                "http.response.pathsend failed for file {path:?}: {source}"
            ));
            pyo3::Python::attach(|py| error.set_cause(py, Some(source.into())));
            error
        },
        // The application named a path that cannot be sent.
        ErrorKind::Pathsend(err @ PathsendError::NotRegularFile { .. }) => {
            PyValueError::new_err(err.to_string())
        },
        // Wire-level failures of the peer's own making. They reach an
        // application only as the reason its stream ended, never as a verdict
        // on something it passed to `send()`, so they stay `RuntimeError` as a
        // group rather than being classified variant by variant.
        ErrorKind::Http1(err) => PyRuntimeError::new_err(err.to_string()),
        ErrorKind::H2(err) => PyRuntimeError::new_err(err.to_string()),
        ErrorKind::Proxy(err) => PyRuntimeError::new_err(err.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use pyo3::Python;

    use super::*;
    use crate::error::AsgiContainer;

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
    fn asgi_value_and_sequence_errors_have_stable_python_types() {
        Python::initialize();
        Python::attach(|py| {
            let missing = into_pyerr(AsgiError::missing_field(
                AsgiContainer::HttpResponseStart,
                "status",
            ));
            assert!(missing.is_instance_of::<PyValueError>(py));

            let type_error = into_pyerr(AsgiError::invalid_field_type(
                AsgiContainer::HttpResponseStart,
                "status",
                "an int",
                "str".into(),
            ));
            assert!(type_error.is_instance_of::<PyTypeError>(py));
            assert_eq!(
                type_error.to_string(),
                "TypeError: http.response.start status must be an int, got str"
            );

            let invalid_status = into_pyerr(HttpResponseError::StatusMustBeThreeDigitCode {
                container: AsgiContainer::HttpResponseStart,
                status: 99,
            });
            assert!(invalid_status.is_instance_of::<PyValueError>(py));
            assert_eq!(
                invalid_status.to_string(),
                "ValueError: http.response.start status must be a three-digit code, got 99"
            );

            let out_of_order = into_pyerr(HttpResponseError::BodyBeforeStart);
            assert!(out_of_order.is_instance_of::<PyRuntimeError>(py));

            let unexpected = into_pyerr(WebSocketError::unexpected_initial_event("websocket.send"));
            assert!(unexpected.is_instance_of::<PyRuntimeError>(py));
        });
    }

    /// The Python type an application catches is public API, so every variant
    /// earns one by decision. `into_pyerr` has no catch-all arm: adding a
    /// variant to any of these enums fails the build until it is classified,
    /// which is how `failure_domain` already works.
    ///
    /// The WebSocket accept values below are the ones a wildcard used to
    /// swallow — an application saw `send()` fail with `RuntimeError` for a
    /// malformed value it had supplied.
    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
    fn every_application_facing_error_has_a_decided_python_type() {
        Python::initialize();
        Python::attach(|py| {
            let value_errors: [H2CornError; 8] = [
                WebSocketError::AcceptHeadersForbidden.into(),
                WebSocketError::AcceptSubprotocolNotRequested.into(),
                WebSocketError::AcceptSubprotocolEmpty.into(),
                WebSocketError::CloseCodeInvalid.into(),
                WebSocketError::CloseReasonTooLong.into(),
                HttpResponseError::InvalidResponseHeaderName.into(),
                AsgiError::WebSocketSendRequiresExactlyOnePayload.into(),
                PathsendError::NotRegularFile {
                    path: "/dev/null".into(),
                }
                .into(),
            ];
            for error in value_errors {
                let rendered = error.to_string();
                assert!(
                    into_pyerr(error).is_instance_of::<PyValueError>(py),
                    "{rendered} must reach the application as ValueError"
                );
            }

            let runtime_errors: [H2CornError; 5] = [
                WebSocketError::HandshakeTimedOut.into(),
                WebSocketError::CompressionFailed.into(),
                WebSocketError::AppEndedBeforeHandshake.into(),
                HttpResponseError::StartAlreadyReceived.into(),
                HttpResponseError::TrailersNotAdvertised.into(),
            ];
            for error in runtime_errors {
                let rendered = error.to_string();
                assert!(
                    into_pyerr(error).is_instance_of::<PyRuntimeError>(py),
                    "{rendered} must reach the application as RuntimeError"
                );
            }

            assert!(
                into_pyerr(AsgiError::SendAfterClose).is_instance_of::<PyOSError>(py),
                "a closed stream is an OS-level condition, not a bad value"
            );
        });
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
    fn python_errors_keep_io_types_and_pathsend_causes() {
        Python::initialize();
        Python::attach(|py| {
            let io_error = into_pyerr(io::Error::new(io::ErrorKind::PermissionDenied, "denied"));
            assert!(io_error.is_instance_of::<PyOSError>(py));

            let pathsend = into_pyerr(PathsendError::open_failed(
                std::path::Path::new("/tmp/secret"),
                io::Error::new(io::ErrorKind::NotFound, "missing"),
            ));
            assert!(pathsend.is_instance_of::<PyRuntimeError>(py));
            let cause = pathsend
                .cause(py)
                .expect("pathsend preserves its I/O cause");
            assert!(cause.is_instance_of::<PyOSError>(py));
            assert!(cause.to_string().contains("missing"));
        });
    }
}
