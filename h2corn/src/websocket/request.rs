use bytes::Bytes;

use crate::error::{ErrorExt, H2CornError, WebSocketError};
use crate::http::header_meta::ParsedWebSocketVersion;
use crate::http::types::{BytesStr, HttpStatusCode, RequestHead, ResponseHeaders, status_code};
use crate::websocket::{WEBSOCKET_VERSION, WebSocketRequestMeta};

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

    pub(crate) const fn bad_request() -> Self {
        Self {
            status: status_code::BAD_REQUEST,
            headers: ResponseHeaders::new(),
        }
    }
}

pub(crate) fn validate_websocket_request(
    request: &RequestHead,
) -> Result<WebSocketRequestMeta, HandshakeRejection> {
    let websocket = request
        .header_meta
        .websocket()
        .ok_or_else(HandshakeRejection::unsupported_version)?;
    match websocket.version {
        ParsedWebSocketVersion::Supported => {},
        ParsedWebSocketVersion::Missing | ParsedWebSocketVersion::Unsupported => {
            return Err(HandshakeRejection::unsupported_version());
        },
        ParsedWebSocketVersion::Duplicate => return Err(HandshakeRejection::bad_request()),
    }
    websocket
        .request
        .clone()
        .into_valid()
        .ok_or_else(HandshakeRejection::bad_request)
}

pub(super) fn validate_accepted_subprotocol(
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
        },
        Some(_) => WebSocketError::AcceptSubprotocolNotRequested.err(),
    }
}
