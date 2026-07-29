//! The standard base64 alphabet, shared with the fixed-size WebSocket accept
//! encoder in `h1::websocket`. That encoder is specialised for a 20-byte
//! digest; there is no general-purpose encoder here.

pub(crate) const TABLE: &[u8; 64] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
