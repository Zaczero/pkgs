//! Chunk-size and extension admission over bounded byte-slice suffixes.

use crate::ascii::{self, HeaderNameClass};
use crate::error::{ErrorExt as _, H2CornError, Http1Error};

pub(super) fn parse_chunk_size(line: &[u8]) -> Result<usize, H2CornError> {
    // Whitespace is permitted between grammar elements, never at the end of
    // the size line. Quoted whitespace is followed by its closing quote.
    if matches!(line.last(), Some(b' ' | b'\t')) {
        return Http1Error::InvalidChunkSize.err();
    }
    let mut remaining = line;
    let mut value = 0_usize;
    let mut has_digit = false;
    while let Some((&byte, rest)) = remaining.split_first() {
        let digit = *ascii::HEX_VALUE
            .get(usize::from(byte))
            .expect("byte table entry");
        if digit == ascii::INVALID_VALUE {
            break;
        }
        value = value
            .checked_mul(16)
            .and_then(|value| value.checked_add(usize::from(digit)))
            .ok_or(Http1Error::InvalidChunkSize)?;
        has_digit = true;
        remaining = rest;
    }
    if !has_digit {
        return Http1Error::InvalidChunkSize.err();
    }
    remaining = skip_ows(remaining);
    while !remaining.is_empty() {
        let Some(extension) = remaining.strip_prefix(b";") else {
            return Http1Error::InvalidChunkSize.err();
        };
        remaining = token_suffix(skip_ows(extension)).ok_or(Http1Error::InvalidChunkSize)?;
        remaining = skip_ows(remaining);
        if let Some(value) = remaining.strip_prefix(b"=") {
            remaining = skip_ows(value);
            if let Some(mut quoted) = remaining.strip_prefix(b"\"") {
                loop {
                    let Some((&byte, rest)) = quoted.split_first() else {
                        return Http1Error::InvalidChunkSize.err();
                    };
                    quoted = rest;
                    match byte {
                        b'"' => {
                            remaining = rest;
                            break;
                        },
                        b'\\' => {
                            let Some((&escaped, rest)) = quoted.split_first() else {
                                return Http1Error::InvalidChunkSize.err();
                            };
                            if escaped.is_ascii_control() {
                                return Http1Error::InvalidChunkSize.err();
                            }
                            quoted = rest;
                        },
                        byte if byte.is_ascii_control() => {
                            return Http1Error::InvalidChunkSize.err();
                        },
                        _ => {},
                    }
                }
            } else {
                remaining = token_suffix(remaining).ok_or(Http1Error::InvalidChunkSize)?;
            }
            remaining = skip_ows(remaining);
        }
    }
    Ok(value)
}

const fn skip_ows(mut input: &[u8]) -> &[u8] {
    while let [b' ' | b'\t', rest @ ..] = input {
        input = rest;
    }
    input
}

/// Consume one nonempty HTTP token, retaining its already bounded suffix.
fn token_suffix(input: &[u8]) -> Option<&[u8]> {
    let end = input
        .iter()
        .position(|&byte| {
            matches!(
                ascii::classify_header_name_byte(byte),
                HeaderNameClass::Invalid
            )
        })
        .unwrap_or(input.len());
    let (token, rest) = input.split_at(end);
    (!token.is_empty()).then_some(rest)
}
