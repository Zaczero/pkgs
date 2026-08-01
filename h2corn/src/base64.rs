//! Base64, in one place: both alphabets, the charset test the WebSocket key
//! check needs, the fixed-size encoder the accept token needs, and the
//! URL-safe decoder the `HTTP2-Settings` upgrade header needs.
//!
//! The decoder reports a unit error. Which protocol was being parsed, and so
//! which wire error to raise, is the caller's to know -- this used to return
//! an HTTP/1 error from what is otherwise a generic codec.

use crate::ascii::{BASE64_VALUE, BASE64URL_VALUE, INVALID_VALUE};

const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Whether a byte is in the standard alphabet, padding excluded.
pub(crate) const fn is_base64(byte: u8) -> bool {
    BASE64_VALUE[byte as usize] != INVALID_VALUE
}

pub(crate) const fn encode_triplet(b0: u8, b1: u8, b2: u8) -> [u8; 4] {
    [
        TABLE[(b0 >> 2) as usize],
        TABLE[(((b0 & 0x03) << 4) | (b1 >> 4)) as usize],
        TABLE[(((b1 & 0x0F) << 2) | (b2 >> 6)) as usize],
        TABLE[(b2 & 0x3F) as usize],
    ]
}

pub(crate) const fn encode_remainder(b0: u8, b1: u8) -> [u8; 4] {
    [
        TABLE[(b0 >> 2) as usize],
        TABLE[(((b0 & 0x03) << 4) | (b1 >> 4)) as usize],
        TABLE[((b1 & 0x0F) << 2) as usize],
        b'=',
    ]
}

/// Decode unpadded URL-safe base64. `Err(())` means the input was not that.
pub(crate) fn decode_url(src: &[u8]) -> Result<Vec<u8>, ()> {
    let mut out = Vec::with_capacity((src.len() * 3) / 4 + 3);
    let mut block = [0_u8; 4];
    let mut used = 0;
    for &byte in src {
        let value = BASE64URL_VALUE[usize::from(byte)];
        if value == INVALID_VALUE {
            return Err(());
        }
        block[used] = value;
        used += 1;
        if used == 4 {
            out.push((block[0] << 2) | (block[1] >> 4));
            out.push((block[1] << 4) | (block[2] >> 2));
            out.push((block[2] << 6) | block[3]);
            used = 0;
        }
    }
    match used {
        0 => {},
        2 => out.push((block[0] << 2) | (block[1] >> 4)),
        3 => {
            out.push((block[0] << 2) | (block[1] >> 4));
            out.push((block[1] << 4) | (block[2] >> 2));
        },
        // One leftover sextet cannot have come from any whole input byte.
        _ => return Err(()),
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::{decode_url, is_base64};

    #[test]
    fn is_base64_excludes_padding_and_the_url_alphabet() {
        assert!(is_base64(b'A') && is_base64(b'z') && is_base64(b'0'));
        assert!(is_base64(b'+') && is_base64(b'/'));
        assert!(!is_base64(b'=') && !is_base64(b'-') && !is_base64(b'_'));
    }

    #[test]
    fn decode_url_accepts_its_dialect_and_rejects_the_rest() {
        assert_eq!(decode_url(b"").expect("empty input decodes"), b"");
        assert_eq!(
            decode_url(b"AAMAAABkAARAAAAA")
                .expect("a settings payload decodes")
                .len(),
            12
        );
        // A single leftover sextet cannot have come from a whole byte.
        decode_url(b"A").unwrap_err();
        // Padding and the standard alphabet are both outside this dialect.
        decode_url(b"AA==").unwrap_err();
        decode_url(b"a+b/").unwrap_err();
    }
}
