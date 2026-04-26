mod table;

use self::table::{DECODE_TABLE, ENCODE_TABLE};
use crate::hpack::DecoderError;

use bytes::{BufMut, BytesMut};

const MAYBE_EOS: u8 = 1;
const DECODED: u8 = 2;
const ERROR: u8 = 4;
const MIN_CODE_BITS: usize = 5;

pub fn decode(src: &[u8], buf: &mut BytesMut) -> Result<BytesMut, DecoderError> {
    let mut state = 0_usize;
    let mut maybe_eos = true;

    buf.reserve(src.len().saturating_mul(8).div_ceil(MIN_CODE_BITS));

    for &byte in src {
        let (next, symbol, flags) = DECODE_TABLE[state][usize::from(byte >> 4)];
        if flags & ERROR != 0 {
            return Err(DecoderError::InvalidHuffmanCode);
        }
        if flags & DECODED != 0 {
            buf.put_u8(symbol);
        }
        state = usize::from(next);

        let (next, symbol, flags) = DECODE_TABLE[state][usize::from(byte & 0x0f)];
        if flags & ERROR != 0 {
            return Err(DecoderError::InvalidHuffmanCode);
        }
        if flags & DECODED != 0 {
            buf.put_u8(symbol);
        }
        state = usize::from(next);
        maybe_eos = flags & MAYBE_EOS != 0;
    }

    if state != 0 && !maybe_eos {
        return Err(DecoderError::InvalidHuffmanCode);
    }

    Ok(buf.split())
}

pub fn encoded_len(src: &[u8]) -> usize {
    let mut bit_len = 0_usize;
    for &byte in src {
        bit_len += ENCODE_TABLE[usize::from(byte)].0;
    }
    (bit_len + 7) >> 3
}

#[cfg(test)]
pub fn encode(src: &[u8], dst: &mut BytesMut) {
    encode_with_len(src, encoded_len(src), dst);
}

pub fn encode_with_len(src: &[u8], encoded_len: usize, dst: &mut BytesMut) {
    dst.reserve(encoded_len);

    let mut bits: u64 = 0;
    let mut bits_left = 40;

    for &byte in src {
        let (code_bits, code) = ENCODE_TABLE[usize::from(byte)];
        bits |= code << (bits_left - code_bits);
        bits_left -= code_bits;

        while bits_left <= 32 {
            dst.put_u8((bits >> 32) as u8);
            bits <<= 8;
            bits_left += 8;
        }
    }

    if bits_left != 40 {
        bits |= (1 << bits_left) - 1;
        dst.put_u8((bits >> 32) as u8);
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn decode_bytes(src: &[u8]) -> Result<BytesMut, DecoderError> {
        let mut buf = BytesMut::new();
        decode(src, &mut buf)
    }

    #[test]
    fn decode_single_byte() {
        assert_eq!("o", decode_bytes(&[0b0011_1111]).unwrap());
        assert_eq!("0", decode_bytes(&[7]).unwrap());
        assert_eq!("A", decode_bytes(&[(0x21 << 2) + 3]).unwrap());
    }

    #[test]
    fn single_char_multi_byte() {
        assert_eq!("#", decode_bytes(&[255, 160 + 15]).unwrap());
        assert_eq!("$", decode_bytes(&[255, 200 + 7]).unwrap());
        assert_eq!("\x0a", decode_bytes(&[255, 255, 255, 240 + 3]).unwrap());
    }

    #[test]
    fn multi_char() {
        assert_eq!("!0", decode_bytes(&[254, 1]).unwrap());
        assert_eq!(" !", decode_bytes(&[0b0101_0011, 0b1111_1000]).unwrap());
    }

    #[test]
    fn encoded_length_matches_actual_output() {
        let data = b"custom-key";
        let mut dst = BytesMut::new();
        encode(data, &mut dst);

        assert_eq!(encoded_len(data), dst.len());
    }

    #[test]
    fn decode_reserve_bound_matches_shortest_code() {
        assert_eq!(
            ENCODE_TABLE
                .iter()
                .map(|(code_bits, _)| *code_bits)
                .min()
                .unwrap(),
            MIN_CODE_BITS
        );
    }

    #[test]
    fn encode_with_len_matches_encode() {
        let data = b"custom-header-value";
        let mut encoded = BytesMut::new();
        let mut encoded_with_len = BytesMut::new();

        encode(data, &mut encoded);
        encode_with_len(data, encoded_len(data), &mut encoded_with_len);

        assert_eq!(encoded, encoded_with_len);
    }

    #[test]
    fn encode_decode_round_trip() {
        const DATA: &[&[u8]] = &[
            b"hello world",
            b":method",
            b":scheme",
            b":authority",
            b"yahoo.co.jp",
            b"GET",
            b"http",
            b":path",
            b"/images/top/sp2/cmn/logo-ns-130528.png",
            b"example.com",
            b"hpack-test",
            b"xxxxxxx1",
            b"Mozilla/5.0 (Macintosh; Intel Mac OS X 10.8; rv:16.0) Gecko/20100101 Firefox/16.0",
            b"accept",
            b"Accept",
            b"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            b"cookie",
            b"B=76j09a189a6h4&b=3&s=0b",
            b"TE",
            b"\0",
            b"\0\0\0",
            b"\0\x01\x02\x03\x04\x05",
            b"\xFF\xF8",
        ];

        for sample in DATA {
            let mut dst = BytesMut::with_capacity(sample.len());
            encode(sample, &mut dst);
            let decoded = decode_bytes(&dst).unwrap();
            assert_eq!(&decoded[..], *sample);
        }
    }
}
