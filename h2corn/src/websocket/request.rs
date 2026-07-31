use crate::error::{ErrorExt, H2CornError, WebSocketError};
use crate::http::header_meta::ParsedWebSocketVersion;
use crate::http::planner::RejectedResponse;
use crate::http::types::{BytesStr, RequestHead};
use crate::websocket::WebSocketRequestMeta;

pub(crate) fn validate_websocket_request(
    request: &RequestHead,
) -> Result<WebSocketRequestMeta, RejectedResponse> {
    let websocket = request
        .header_meta
        .websocket()
        .ok_or_else(RejectedResponse::unsupported_websocket_version)?;
    match websocket.version {
        ParsedWebSocketVersion::Supported => {},
        ParsedWebSocketVersion::Missing | ParsedWebSocketVersion::Unsupported => {
            return Err(RejectedResponse::unsupported_websocket_version());
        },
        ParsedWebSocketVersion::Duplicate => return Err(RejectedResponse::bad_request()),
    }
    websocket
        .request
        .clone()
        .into_valid()
        .ok_or_else(RejectedResponse::bad_request)
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
