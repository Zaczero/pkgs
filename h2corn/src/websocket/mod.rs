mod app;
mod codec;
mod deflate;
mod request;
pub(crate) mod session;

pub(crate) use codec::{EncodedFrameHeader, WebSocketCloseCode, WebSocketCodec, close_code};
pub(crate) use deflate::{PERMESSAGE_DEFLATE_RESPONSE, offers_compatible_permessage_deflate};
pub(crate) use request::validate_websocket_request;
pub(crate) use session::WebSocketContext;

use crate::http::types::BytesStr;

pub(crate) const WEBSOCKET_KEY_LEN: usize = 24;
pub(crate) const WEBSOCKET_VERSION: &[u8; 2] = b"13";
pub(crate) const SEC_WEBSOCKET_PROTOCOL_HEADER_BYTES: &[u8] = b"sec-websocket-protocol";
pub(crate) const SEC_WEBSOCKET_EXTENSIONS_HEADER_BYTES: &[u8] = b"sec-websocket-extensions";

pub(crate) type WebSocketKey = [u8; WEBSOCKET_KEY_LEN];
pub(crate) type RequestedSubprotocols = smallvec::SmallVec<[BytesStr; 4]>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WebSocketRequestMeta {
    pub(crate) requested_subprotocols: RequestedSubprotocols,
    pub(crate) per_message_deflate: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ParsedRequestedSubprotocols {
    Valid(RequestedSubprotocols),
    Invalid,
}

impl Default for ParsedRequestedSubprotocols {
    fn default() -> Self {
        Self::Valid(RequestedSubprotocols::default())
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct ParsedWebSocketRequestMeta {
    pub(crate) requested_subprotocols: ParsedRequestedSubprotocols,
    pub(crate) per_message_deflate: bool,
}

impl ParsedWebSocketRequestMeta {
    pub(crate) fn into_valid(self) -> Option<WebSocketRequestMeta> {
        let ParsedRequestedSubprotocols::Valid(requested_subprotocols) =
            self.requested_subprotocols
        else {
            return None;
        };
        Some(WebSocketRequestMeta {
            requested_subprotocols,
            per_message_deflate: self.per_message_deflate,
        })
    }
}
