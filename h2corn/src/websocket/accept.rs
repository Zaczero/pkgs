//! The WebSocket handshake accept token: SHA-1 of the client key plus the
//! RFC 6455 GUID, base64-encoded.
//!
//! This is a pure `[u8; 24] -> [u8; 28]` transform with no transport in it.
//! It lived in the HTTP/1 module only because that is the one caller today;
//! keeping it there forced a module-wide ordering suppression so the generated
//! macros could sit beside their call site.

#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "the generated SHA-1 macros stay next to their compression call site"
)]

use crate::base64;
use crate::websocket::{WEBSOCKET_KEY_LEN, WebSocketKey};

const LEN: usize = 28;
const GUID: &[u8; 36] = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
const INPUT_LEN: usize = WEBSOCKET_KEY_LEN + GUID.len();
const INITIAL_STATE: [u32; 5] = [
    0x6745_2301_u32,
    0xEFCD_AB89,
    0x98BA_DCFE,
    0x1032_5476,
    0xC3D2_E1F0,
];
const FIRST_BLOCK_TEMPLATE: [u32; 16] = {
    let mut block = [0; 16];
    let mut index = 0;
    while index < GUID.len() / 4 {
        let offset = index * 4;
        block[WEBSOCKET_KEY_LEN / 4 + index] = u32::from_be_bytes([
            GUID[offset],
            GUID[offset + 1],
            GUID[offset + 2],
            GUID[offset + 3],
        ]);
        index += 1;
    }
    block[15] = 0x8000_0000;
    block
};
#[cfg(test)]
const SECOND_BLOCK: [u32; 16] = {
    let mut block = [0; 16];
    block[15] = (INPUT_LEN as u32) << 3;
    block
};

/// One SHA-1 round with the register roles made explicit. `sha1_compress!`
/// expands every schedule update and round at its call site: the WebSocket
/// accept input is always exactly two blocks, so a generic round loop would
/// only add branches and indexed schedule traffic to this fixed operation.
macro_rules! sha1_round {
    ($a:ident, $b:ident, $c:ident, $d:ident, $e:ident; $word:expr; $mix:expr; $round_constant:expr) => {{
        let temp = $a
            .rotate_left(5)
            .wrapping_add($mix)
            .wrapping_add($e)
            .wrapping_add($round_constant)
            .wrapping_add($word);
        $e = $d;
        $d = $c;
        $c = $b.rotate_left(30);
        $b = $a;
        $a = temp;
    }};
}

/// Expands one SHA-1 compression with its sixteen-word ring in registers.
/// The macro is invoked for both known-size blocks in `websocket_accept`,
/// yielding all 160 rounds directly in that hot function.
macro_rules! sha1_compress {
    ($a:ident, $b:ident, $c:ident, $d:ident, $e:ident; $w0:ident, $w1:ident, $w2:ident, $w3:ident, $w4:ident, $w5:ident, $w6:ident, $w7:ident, $w8:ident, $w9:ident, $w10:ident, $w11:ident, $w12:ident, $w13:ident, $w14:ident, $w15:ident) => {
        sha1_round!($a, $b, $c, $d, $e; $w0; ($b & $c) | ((!$b) & $d); 0x5A82_7999);
        sha1_round!($a, $b, $c, $d, $e; $w1; ($b & $c) | ((!$b) & $d); 0x5A82_7999);
        sha1_round!($a, $b, $c, $d, $e; $w2; ($b & $c) | ((!$b) & $d); 0x5A82_7999);
        sha1_round!($a, $b, $c, $d, $e; $w3; ($b & $c) | ((!$b) & $d); 0x5A82_7999);
        sha1_round!($a, $b, $c, $d, $e; $w4; ($b & $c) | ((!$b) & $d); 0x5A82_7999);
        sha1_round!($a, $b, $c, $d, $e; $w5; ($b & $c) | ((!$b) & $d); 0x5A82_7999);
        sha1_round!($a, $b, $c, $d, $e; $w6; ($b & $c) | ((!$b) & $d); 0x5A82_7999);
        sha1_round!($a, $b, $c, $d, $e; $w7; ($b & $c) | ((!$b) & $d); 0x5A82_7999);
        sha1_round!($a, $b, $c, $d, $e; $w8; ($b & $c) | ((!$b) & $d); 0x5A82_7999);
        sha1_round!($a, $b, $c, $d, $e; $w9; ($b & $c) | ((!$b) & $d); 0x5A82_7999);
        sha1_round!($a, $b, $c, $d, $e; $w10; ($b & $c) | ((!$b) & $d); 0x5A82_7999);
        sha1_round!($a, $b, $c, $d, $e; $w11; ($b & $c) | ((!$b) & $d); 0x5A82_7999);
        sha1_round!($a, $b, $c, $d, $e; $w12; ($b & $c) | ((!$b) & $d); 0x5A82_7999);
        sha1_round!($a, $b, $c, $d, $e; $w13; ($b & $c) | ((!$b) & $d); 0x5A82_7999);
        sha1_round!($a, $b, $c, $d, $e; $w14; ($b & $c) | ((!$b) & $d); 0x5A82_7999);
        sha1_round!($a, $b, $c, $d, $e; $w15; ($b & $c) | ((!$b) & $d); 0x5A82_7999);
        $w0 = ($w13 ^ $w8 ^ $w2 ^ $w0).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w0; ($b & $c) | ((!$b) & $d); 0x5A82_7999);
        $w1 = ($w14 ^ $w9 ^ $w3 ^ $w1).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w1; ($b & $c) | ((!$b) & $d); 0x5A82_7999);
        $w2 = ($w15 ^ $w10 ^ $w4 ^ $w2).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w2; ($b & $c) | ((!$b) & $d); 0x5A82_7999);
        $w3 = ($w0 ^ $w11 ^ $w5 ^ $w3).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w3; ($b & $c) | ((!$b) & $d); 0x5A82_7999);
        $w4 = ($w1 ^ $w12 ^ $w6 ^ $w4).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w4; $b ^ $c ^ $d; 0x6ED9_EBA1);
        $w5 = ($w2 ^ $w13 ^ $w7 ^ $w5).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w5; $b ^ $c ^ $d; 0x6ED9_EBA1);
        $w6 = ($w3 ^ $w14 ^ $w8 ^ $w6).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w6; $b ^ $c ^ $d; 0x6ED9_EBA1);
        $w7 = ($w4 ^ $w15 ^ $w9 ^ $w7).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w7; $b ^ $c ^ $d; 0x6ED9_EBA1);
        $w8 = ($w5 ^ $w0 ^ $w10 ^ $w8).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w8; $b ^ $c ^ $d; 0x6ED9_EBA1);
        $w9 = ($w6 ^ $w1 ^ $w11 ^ $w9).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w9; $b ^ $c ^ $d; 0x6ED9_EBA1);
        $w10 = ($w7 ^ $w2 ^ $w12 ^ $w10).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w10; $b ^ $c ^ $d; 0x6ED9_EBA1);
        $w11 = ($w8 ^ $w3 ^ $w13 ^ $w11).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w11; $b ^ $c ^ $d; 0x6ED9_EBA1);
        $w12 = ($w9 ^ $w4 ^ $w14 ^ $w12).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w12; $b ^ $c ^ $d; 0x6ED9_EBA1);
        $w13 = ($w10 ^ $w5 ^ $w15 ^ $w13).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w13; $b ^ $c ^ $d; 0x6ED9_EBA1);
        $w14 = ($w11 ^ $w6 ^ $w0 ^ $w14).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w14; $b ^ $c ^ $d; 0x6ED9_EBA1);
        $w15 = ($w12 ^ $w7 ^ $w1 ^ $w15).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w15; $b ^ $c ^ $d; 0x6ED9_EBA1);
        $w0 = ($w13 ^ $w8 ^ $w2 ^ $w0).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w0; $b ^ $c ^ $d; 0x6ED9_EBA1);
        $w1 = ($w14 ^ $w9 ^ $w3 ^ $w1).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w1; $b ^ $c ^ $d; 0x6ED9_EBA1);
        $w2 = ($w15 ^ $w10 ^ $w4 ^ $w2).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w2; $b ^ $c ^ $d; 0x6ED9_EBA1);
        $w3 = ($w0 ^ $w11 ^ $w5 ^ $w3).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w3; $b ^ $c ^ $d; 0x6ED9_EBA1);
        $w4 = ($w1 ^ $w12 ^ $w6 ^ $w4).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w4; $b ^ $c ^ $d; 0x6ED9_EBA1);
        $w5 = ($w2 ^ $w13 ^ $w7 ^ $w5).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w5; $b ^ $c ^ $d; 0x6ED9_EBA1);
        $w6 = ($w3 ^ $w14 ^ $w8 ^ $w6).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w6; $b ^ $c ^ $d; 0x6ED9_EBA1);
        $w7 = ($w4 ^ $w15 ^ $w9 ^ $w7).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w7; $b ^ $c ^ $d; 0x6ED9_EBA1);
        $w8 = ($w5 ^ $w0 ^ $w10 ^ $w8).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w8; ($b & $c) | ($b & $d) | ($c & $d); 0x8F1B_BCDC);
        $w9 = ($w6 ^ $w1 ^ $w11 ^ $w9).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w9; ($b & $c) | ($b & $d) | ($c & $d); 0x8F1B_BCDC);
        $w10 = ($w7 ^ $w2 ^ $w12 ^ $w10).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w10; ($b & $c) | ($b & $d) | ($c & $d); 0x8F1B_BCDC);
        $w11 = ($w8 ^ $w3 ^ $w13 ^ $w11).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w11; ($b & $c) | ($b & $d) | ($c & $d); 0x8F1B_BCDC);
        $w12 = ($w9 ^ $w4 ^ $w14 ^ $w12).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w12; ($b & $c) | ($b & $d) | ($c & $d); 0x8F1B_BCDC);
        $w13 = ($w10 ^ $w5 ^ $w15 ^ $w13).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w13; ($b & $c) | ($b & $d) | ($c & $d); 0x8F1B_BCDC);
        $w14 = ($w11 ^ $w6 ^ $w0 ^ $w14).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w14; ($b & $c) | ($b & $d) | ($c & $d); 0x8F1B_BCDC);
        $w15 = ($w12 ^ $w7 ^ $w1 ^ $w15).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w15; ($b & $c) | ($b & $d) | ($c & $d); 0x8F1B_BCDC);
        $w0 = ($w13 ^ $w8 ^ $w2 ^ $w0).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w0; ($b & $c) | ($b & $d) | ($c & $d); 0x8F1B_BCDC);
        $w1 = ($w14 ^ $w9 ^ $w3 ^ $w1).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w1; ($b & $c) | ($b & $d) | ($c & $d); 0x8F1B_BCDC);
        $w2 = ($w15 ^ $w10 ^ $w4 ^ $w2).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w2; ($b & $c) | ($b & $d) | ($c & $d); 0x8F1B_BCDC);
        $w3 = ($w0 ^ $w11 ^ $w5 ^ $w3).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w3; ($b & $c) | ($b & $d) | ($c & $d); 0x8F1B_BCDC);
        $w4 = ($w1 ^ $w12 ^ $w6 ^ $w4).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w4; ($b & $c) | ($b & $d) | ($c & $d); 0x8F1B_BCDC);
        $w5 = ($w2 ^ $w13 ^ $w7 ^ $w5).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w5; ($b & $c) | ($b & $d) | ($c & $d); 0x8F1B_BCDC);
        $w6 = ($w3 ^ $w14 ^ $w8 ^ $w6).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w6; ($b & $c) | ($b & $d) | ($c & $d); 0x8F1B_BCDC);
        $w7 = ($w4 ^ $w15 ^ $w9 ^ $w7).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w7; ($b & $c) | ($b & $d) | ($c & $d); 0x8F1B_BCDC);
        $w8 = ($w5 ^ $w0 ^ $w10 ^ $w8).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w8; ($b & $c) | ($b & $d) | ($c & $d); 0x8F1B_BCDC);
        $w9 = ($w6 ^ $w1 ^ $w11 ^ $w9).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w9; ($b & $c) | ($b & $d) | ($c & $d); 0x8F1B_BCDC);
        $w10 = ($w7 ^ $w2 ^ $w12 ^ $w10).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w10; ($b & $c) | ($b & $d) | ($c & $d); 0x8F1B_BCDC);
        $w11 = ($w8 ^ $w3 ^ $w13 ^ $w11).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w11; ($b & $c) | ($b & $d) | ($c & $d); 0x8F1B_BCDC);
        $w12 = ($w9 ^ $w4 ^ $w14 ^ $w12).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w12; $b ^ $c ^ $d; 0xCA62_C1D6);
        $w13 = ($w10 ^ $w5 ^ $w15 ^ $w13).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w13; $b ^ $c ^ $d; 0xCA62_C1D6);
        $w14 = ($w11 ^ $w6 ^ $w0 ^ $w14).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w14; $b ^ $c ^ $d; 0xCA62_C1D6);
        $w15 = ($w12 ^ $w7 ^ $w1 ^ $w15).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w15; $b ^ $c ^ $d; 0xCA62_C1D6);
        $w0 = ($w13 ^ $w8 ^ $w2 ^ $w0).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w0; $b ^ $c ^ $d; 0xCA62_C1D6);
        $w1 = ($w14 ^ $w9 ^ $w3 ^ $w1).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w1; $b ^ $c ^ $d; 0xCA62_C1D6);
        $w2 = ($w15 ^ $w10 ^ $w4 ^ $w2).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w2; $b ^ $c ^ $d; 0xCA62_C1D6);
        $w3 = ($w0 ^ $w11 ^ $w5 ^ $w3).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w3; $b ^ $c ^ $d; 0xCA62_C1D6);
        $w4 = ($w1 ^ $w12 ^ $w6 ^ $w4).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w4; $b ^ $c ^ $d; 0xCA62_C1D6);
        $w5 = ($w2 ^ $w13 ^ $w7 ^ $w5).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w5; $b ^ $c ^ $d; 0xCA62_C1D6);
        $w6 = ($w3 ^ $w14 ^ $w8 ^ $w6).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w6; $b ^ $c ^ $d; 0xCA62_C1D6);
        $w7 = ($w4 ^ $w15 ^ $w9 ^ $w7).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w7; $b ^ $c ^ $d; 0xCA62_C1D6);
        $w8 = ($w5 ^ $w0 ^ $w10 ^ $w8).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w8; $b ^ $c ^ $d; 0xCA62_C1D6);
        $w9 = ($w6 ^ $w1 ^ $w11 ^ $w9).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w9; $b ^ $c ^ $d; 0xCA62_C1D6);
        $w10 = ($w7 ^ $w2 ^ $w12 ^ $w10).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w10; $b ^ $c ^ $d; 0xCA62_C1D6);
        $w11 = ($w8 ^ $w3 ^ $w13 ^ $w11).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w11; $b ^ $c ^ $d; 0xCA62_C1D6);
        $w12 = ($w9 ^ $w4 ^ $w14 ^ $w12).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w12; $b ^ $c ^ $d; 0xCA62_C1D6);
        $w13 = ($w10 ^ $w5 ^ $w15 ^ $w13).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w13; $b ^ $c ^ $d; 0xCA62_C1D6);
        $w14 = ($w11 ^ $w6 ^ $w0 ^ $w14).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w14; $b ^ $c ^ $d; 0xCA62_C1D6);
        $w15 = ($w12 ^ $w7 ^ $w1 ^ $w15).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w15; $b ^ $c ^ $d; 0xCA62_C1D6);
    };
}

pub(crate) fn websocket_accept(key: &WebSocketKey) -> [u8; LEN] {
    let mut h0 = INITIAL_STATE[0];
    let mut h1 = INITIAL_STATE[1];
    let mut h2 = INITIAL_STATE[2];
    let mut h3 = INITIAL_STATE[3];
    let mut h4 = INITIAL_STATE[4];

    let mut w0 = u32::from_be_bytes([key[0], key[1], key[2], key[3]]);
    let mut w1 = u32::from_be_bytes([key[4], key[5], key[6], key[7]]);
    let mut w2 = u32::from_be_bytes([key[8], key[9], key[10], key[11]]);
    let mut w3 = u32::from_be_bytes([key[12], key[13], key[14], key[15]]);
    let mut w4 = u32::from_be_bytes([key[16], key[17], key[18], key[19]]);
    let mut w5 = u32::from_be_bytes([key[20], key[21], key[22], key[23]]);
    let mut w6 = FIRST_BLOCK_TEMPLATE[6];
    let mut w7 = FIRST_BLOCK_TEMPLATE[7];
    let mut w8 = FIRST_BLOCK_TEMPLATE[8];
    let mut w9 = FIRST_BLOCK_TEMPLATE[9];
    let mut w10 = FIRST_BLOCK_TEMPLATE[10];
    let mut w11 = FIRST_BLOCK_TEMPLATE[11];
    let mut w12 = FIRST_BLOCK_TEMPLATE[12];
    let mut w13 = FIRST_BLOCK_TEMPLATE[13];
    let mut w14 = FIRST_BLOCK_TEMPLATE[14];
    let mut w15 = FIRST_BLOCK_TEMPLATE[15];
    sha1_compress!(
        h0, h1, h2, h3, h4;
        w0, w1, w2, w3, w4, w5, w6, w7, w8, w9, w10, w11, w12, w13, w14, w15
    );

    let state0 = INITIAL_STATE[0].wrapping_add(h0);
    let state1 = INITIAL_STATE[1].wrapping_add(h1);
    let state2 = INITIAL_STATE[2].wrapping_add(h2);
    let state3 = INITIAL_STATE[3].wrapping_add(h3);
    let state4 = INITIAL_STATE[4].wrapping_add(h4);
    let mut h0 = state0;
    let mut h1 = state1;
    let mut h2 = state2;
    let mut h3 = state3;
    let mut h4 = state4;
    w0 = 0;
    w1 = 0;
    w2 = 0;
    w3 = 0;
    w4 = 0;
    w5 = 0;
    w6 = 0;
    w7 = 0;
    w8 = 0;
    w9 = 0;
    w10 = 0;
    w11 = 0;
    w12 = 0;
    w13 = 0;
    w14 = 0;
    w15 = (INPUT_LEN as u32) << 3;
    sha1_compress!(
        h0, h1, h2, h3, h4;
        w0, w1, w2, w3, w4, w5, w6, w7, w8, w9, w10, w11, w12, w13, w14, w15
    );

    let [w0, w1, w2, w3, w4] = [
        state0.wrapping_add(h0),
        state1.wrapping_add(h1),
        state2.wrapping_add(h2),
        state3.wrapping_add(h3),
        state4.wrapping_add(h4),
    ];
    let mut out = [0_u8; LEN];
    let (out_groups, out_remainder) = out.as_chunks_mut::<4>();
    debug_assert!(out_remainder.is_empty());
    out_groups[0] = base64::encode_triplet((w0 >> 24) as u8, (w0 >> 16) as u8, (w0 >> 8) as u8);
    out_groups[1] = base64::encode_triplet(w0 as u8, (w1 >> 24) as u8, (w1 >> 16) as u8);
    out_groups[2] = base64::encode_triplet((w1 >> 8) as u8, w1 as u8, (w2 >> 24) as u8);
    out_groups[3] = base64::encode_triplet((w2 >> 16) as u8, (w2 >> 8) as u8, w2 as u8);
    out_groups[4] = base64::encode_triplet((w3 >> 24) as u8, (w3 >> 16) as u8, (w3 >> 8) as u8);
    out_groups[5] = base64::encode_triplet(w3 as u8, (w4 >> 24) as u8, (w4 >> 16) as u8);
    out_groups[6] = base64::encode_remainder((w4 >> 8) as u8, w4 as u8);

    out
}

#[cfg(test)]
fn reference_websocket_accept(key: &WebSocketKey) -> [u8; LEN] {
    let mut state = INITIAL_STATE;
    let mut first = FIRST_BLOCK_TEMPLATE;
    let (key_words, remainder) = key.as_chunks::<4>();
    debug_assert!(remainder.is_empty());
    for (dst, word) in first[..6].iter_mut().zip(key_words) {
        *dst = u32::from_be_bytes(*word);
    }
    reference_compress_sha1_block(&mut state, first);
    reference_compress_sha1_block(&mut state, SECOND_BLOCK);

    let [w0, w1, w2, w3, w4] = state;
    let mut out = [0_u8; LEN];
    let (out_groups, out_remainder) = out.as_chunks_mut::<4>();
    debug_assert!(out_remainder.is_empty());
    out_groups[0] = base64::encode_triplet((w0 >> 24) as u8, (w0 >> 16) as u8, (w0 >> 8) as u8);
    out_groups[1] = base64::encode_triplet(w0 as u8, (w1 >> 24) as u8, (w1 >> 16) as u8);
    out_groups[2] = base64::encode_triplet((w1 >> 8) as u8, w1 as u8, (w2 >> 24) as u8);
    out_groups[3] = base64::encode_triplet((w2 >> 16) as u8, (w2 >> 8) as u8, w2 as u8);
    out_groups[4] = base64::encode_triplet((w3 >> 24) as u8, (w3 >> 16) as u8, (w3 >> 8) as u8);
    out_groups[5] = base64::encode_triplet(w3 as u8, (w4 >> 24) as u8, (w4 >> 16) as u8);
    out_groups[6] = base64::encode_remainder((w4 >> 8) as u8, w4 as u8);
    out
}

#[cfg(test)]
fn reference_compress_sha1_block(state: &mut [u32; 5], block: [u32; 16]) {
    let mut words = block;
    let [mut h0, mut h1, mut h2, mut h3, mut h4] = *state;

    for index in 0..80 {
        let word = if index < 16 {
            words[index]
        } else {
            let word = (words[(index + 13) & 15]
                ^ words[(index + 8) & 15]
                ^ words[(index + 2) & 15]
                ^ words[index & 15])
                .rotate_left(1);
            words[index & 15] = word;
            word
        };
        let (mix, round_constant) = match index {
            0..=19 => ((h1 & h2) | ((!h1) & h3), 0x5A82_7999),
            20..=39 => (h1 ^ h2 ^ h3, 0x6ED9_EBA1),
            40..=59 => ((h1 & h2) | (h1 & h3) | (h2 & h3), 0x8F1B_BCDC),
            _ => (h1 ^ h2 ^ h3, 0xCA62_C1D6),
        };
        let temp = h0
            .rotate_left(5)
            .wrapping_add(mix)
            .wrapping_add(h4)
            .wrapping_add(round_constant)
            .wrapping_add(word);
        h4 = h3;
        h3 = h2;
        h2 = h1.rotate_left(30);
        h1 = h0;
        h0 = temp;
    }

    state[0] = state[0].wrapping_add(h0);
    state[1] = state[1].wrapping_add(h1);
    state[2] = state[2].wrapping_add(h2);
    state[3] = state[3].wrapping_add(h3);
    state[4] = state[4].wrapping_add(h4);
}

#[cfg(test)]
mod tests {
    use super::{reference_websocket_accept, websocket_accept};

    #[test]
    fn websocket_accept_matches_rfc_example() {
        assert_eq!(
            websocket_accept(b"dGhlIHNhbXBsZSBub25jZQ=="),
            *b"s3pPLMBiTxaQ9kYGzzhZRbK+xOo=",
        );
    }

    #[test]
    fn websocket_accept_matches_reference_for_one_million_varied_keys() {
        let mut state = 0xD1B5_4A32_D192_ED03_u64;
        let mut key = [0_u8; crate::websocket::WEBSOCKET_KEY_LEN];
        for _ in 0..1_000_000 {
            for byte in &mut key {
                // SplitMix64 gives every byte position a varied stream without
                // introducing a test-only randomness dependency.
                state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
                let mut mixed = state;
                mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
                mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
                *byte = (mixed ^ (mixed >> 31)) as u8;
            }
            assert_eq!(websocket_accept(&key), reference_websocket_accept(&key));
        }
    }
}
