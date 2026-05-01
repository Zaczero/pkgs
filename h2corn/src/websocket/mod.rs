mod app;
mod codec;
mod deflate;
mod request;
pub mod session;

use crate::hpack::BytesStr;

pub const WEBSOCKET_KEY_LEN: usize = 24;
pub type WebSocketKey = [u8; WEBSOCKET_KEY_LEN];
pub type RequestedSubprotocols = smallvec::SmallVec<[BytesStr; 4]>;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct WebSocketRequestMeta {
    pub(crate) requested_subprotocols: RequestedSubprotocols,
    pub(crate) per_message_deflate: bool,
}

pub const WEBSOCKET_VERSION: &[u8; 2] = b"13";
pub const SEC_WEBSOCKET_PROTOCOL_HEADER_BYTES: &[u8] = b"sec-websocket-protocol";
pub const SEC_WEBSOCKET_EXTENSIONS_HEADER_BYTES: &[u8] = b"sec-websocket-extensions";

pub use codec::{WebSocketCloseCode, WebSocketCodec, close_code};
pub use deflate::{
    PERMESSAGE_DEFLATE_RESPONSE, requested_by_client as websocket_requested_permessage_deflate,
};
pub use request::{HandshakeRejection, validate_websocket_request};
pub use session::WebSocketContext;
