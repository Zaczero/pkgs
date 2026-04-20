use bytes::Bytes;

use crate::error::{ErrorExt, H2CornError, WebSocketError};
use crate::hpack::BytesStr;
use crate::http::types::{HttpStatusCode, RequestHead, ResponseHeaders, status_code};
use crate::websocket::{
    SEC_WEBSOCKET_EXTENSIONS_HEADER_BYTES, SEC_WEBSOCKET_PROTOCOL_HEADER_BYTES, WEBSOCKET_VERSION,
    WebSocketRequestMeta,
};

pub(crate) struct HandshakeRejection {
    pub status: HttpStatusCode,
    pub headers: ResponseHeaders,
}

impl HandshakeRejection {
    pub(crate) fn unsupported_version() -> Self {
        Self {
            status: status_code::UPGRADE_REQUIRED,
            headers: ResponseHeaders::from([(
                Bytes::from_static(b"sec-websocket-version").into(),
                Bytes::from_static(WEBSOCKET_VERSION).into(),
            )]),
        }
    }
}

#[allow(clippy::result_large_err)]
pub(crate) fn validate_websocket_request(
    request: &RequestHead,
) -> Result<WebSocketRequestMeta, HandshakeRejection> {
    if request.header_meta.websocket.version_supported {
        Ok(request.header_meta.websocket.request.clone())
    } else {
        Err(HandshakeRejection::unsupported_version())
    }
}

pub(crate) fn validate_accept_headers(headers: &ResponseHeaders) -> Result<(), H2CornError> {
    for (name, _) in headers {
        let name = name.as_bytes();
        if name.starts_with(b":")
            || name.eq_ignore_ascii_case(SEC_WEBSOCKET_PROTOCOL_HEADER_BYTES)
            || name.eq_ignore_ascii_case(SEC_WEBSOCKET_EXTENSIONS_HEADER_BYTES)
        {
            return WebSocketError::AcceptHeadersForbidden.err();
        }
    }
    Ok(())
}

pub(crate) fn validate_accepted_subprotocol(
    requested_subprotocols: &[BytesStr],
    subprotocol: Option<&str>,
) -> Result<(), H2CornError> {
    match subprotocol {
        None => Ok(()),
        Some("") => WebSocketError::AcceptSubprotocolEmpty.err(),
        Some(subprotocol)
            if requested_subprotocols
                .iter()
                .any(|requested| requested.as_str() == subprotocol) =>
        {
            Ok(())
        }
        Some(_) => WebSocketError::AcceptSubprotocolNotRequested.err(),
    }
}
