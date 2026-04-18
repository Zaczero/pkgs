mod app;
mod codec;
mod deflate;
mod request;
pub(crate) mod session;

use crate::hpack::BytesStr;

pub(crate) const WEBSOCKET_KEY_LEN: usize = 24;
pub(crate) type WebSocketKey = [u8; WEBSOCKET_KEY_LEN];
pub(crate) type RequestedSubprotocols = smallvec::SmallVec<[BytesStr; 4]>;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct WebSocketRequestMeta {
    pub(crate) requested_subprotocols: RequestedSubprotocols,
    pub(crate) per_message_deflate: bool,
}

pub(crate) const WEBSOCKET_VERSION: &[u8; 2] = b"13";
pub(crate) const SEC_WEBSOCKET_PROTOCOL_HEADER_BYTES: &[u8] = b"sec-websocket-protocol";
pub(crate) const SEC_WEBSOCKET_EXTENSIONS_HEADER_BYTES: &[u8] = b"sec-websocket-extensions";

pub(crate) use codec::{WebSocketCloseCode, WebSocketCodec, close_code};
pub(crate) use deflate::{
    PERMESSAGE_DEFLATE_RESPONSE, requested_by_client as websocket_requested_permessage_deflate,
};
pub(crate) use request::{HandshakeRejection, validate_websocket_request};
pub(crate) use session::WebSocketContext;
