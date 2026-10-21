//! Base64 codecs shared by WebSocket and HTTP/2 upgrade headers.

use crate::ascii::{BASE64_VALUE, BASE64URL_VALUE, INVALID_VALUE};

const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Whether a byte is in the standard alphabet, padding excluded.
#[expect(
    clippy::indexing_slicing,
    reason = "a u8 index cannot leave a 256-entry table"
)]
pub(crate) const fn is_base64(byte: u8) -> bool {
    BASE64_VALUE[byte as usize] != INVALID_VALUE
}

#[expect(
    clippy::indexing_slicing,
    reason = "each operand is masked to six bits, which the 64-entry alphabet covers"
)]
pub(crate) const fn encode_triplet(b0: u8, b1: u8, b2: u8) -> [u8; 4] {
    [
        TABLE[(b0 >> 2) as usize],
        TABLE[(((b0 & 0x03) << 4) | (b1 >> 4)) as usize],
        TABLE[(((b1 & 0x0F) << 2) | (b2 >> 6)) as usize],
        TABLE[(b2 & 0x3F) as usize],
    ]
}

#[expect(
    clippy::indexing_slicing,
    reason = "each operand is masked to six bits, which the 64-entry alphabet covers"
)]
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
    let (blocks, remainder) = src.as_chunks::<4>();
    let extra = match remainder.len() {
        0 => 0,
        2 => 1,
        3 => 2,
        _ => return Err(()),
    };
    let mut out = Vec::with_capacity(blocks.len().saturating_mul(3).saturating_add(extra));
    for block in blocks {
        let [Ok(a), Ok(b), Ok(c), Ok(d)] = block.map(decode_sextet) else {
            return Err(());
        };
        out.extend_from_slice(&[(a << 2) | (b >> 4), (b << 4) | (c >> 2), (c << 6) | d]);
    }
    match remainder {
        [] => {},
        &[a, b] => out.push((decode_sextet(a)? << 2) | (decode_sextet(b)? >> 4)),
        &[a, b, c] => {
            let (a, b, c) = (decode_sextet(a)?, decode_sextet(b)?, decode_sextet(c)?);
            out.extend_from_slice(&[(a << 2) | (b >> 4), (b << 4) | (c >> 2)]);
        },
        // One leftover sextet cannot have come from any whole input byte.
        _ => return Err(()),
    }
    Ok(out)
}

fn decode_sextet(byte: u8) -> Result<u8, ()> {
    BASE64URL_VALUE
        .get(usize::from(byte))
        .copied()
        .filter(|&value| value != INVALID_VALUE)
        .ok_or(())
}

#[cfg(test)]
mod tests {
    use super::{decode_url, encode_remainder, encode_triplet, is_base64};

    #[test]
    fn is_base64_excludes_padding_and_the_url_alphabet() {
        assert!(is_base64(b'A') && is_base64(b'z') && is_base64(b'0'));
        assert!(is_base64(b'+') && is_base64(b'/'));
        assert!(!is_base64(b'=') && !is_base64(b'-') && !is_base64(b'_'));
    }

    #[test]
    fn encodes_complete_and_partial_groups() {
        assert_eq!(encode_triplet(0, 0, 0), *b"AAAA");
        assert_eq!(encode_triplet(0xFB, 0xEF, 0xFF), *b"++//");
        assert_eq!(encode_remainder(b'M', b'a'), *b"TWE=");
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
        assert_eq!(
            decode_url(b"AB").expect("unused trailing bits remain accepted"),
            [0]
        );
        // A single leftover sextet cannot have come from a whole input byte.
        decode_url(b"A").unwrap_err();
        // Padding and the standard alphabet are both outside this dialect.
        decode_url(b"AA==").unwrap_err();
        decode_url(b"a+b/").unwrap_err();
    }
}
