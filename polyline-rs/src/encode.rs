//! Stateful coordinate encoding into a type-guaranteed ASCII character stream.

use std::ascii::Char;

use crate::constants::{ASCII_OFFSET, CHUNK_BITS, CHUNK_MASK, CONTINUATION_BIT, PrecisionScale};
use crate::errors::PolylineError;
use crate::zigzag::zigzag_encode;

/// Admits the protocol's ASCII offset without a dependent alphabet-table load.
fn encode_chunk(chunk: u8) -> Char {
    chunk
        .checked_add(ASCII_OFFSET)
        .and_then(Char::from_u8)
        .expect("six-bit Polyline chunks encode as ASCII")
}

fn encode_value(out: &mut Vec<Char>, delta: i32) {
    let mut value = zigzag_encode(delta);
    while value >= u32::from(CONTINUATION_BIT) {
        let chunk = ((value as u8) & CHUNK_MASK) | CONTINUATION_BIT;
        out.push(encode_chunk(chunk));
        value >>= CHUNK_BITS;
    }
    out.push(encode_chunk(value as u8));
}

fn encode_coordinate(
    out: &mut Vec<Char>,
    coordinate_value: f64,
    scale: f64,
    previous: i32,
    index: usize,
    coordinate: &'static str,
) -> Result<i32, PolylineError> {
    let scaled = coordinate_value * scale;
    let rounded = scaled.round();
    if !rounded.is_finite() || rounded < f64::from(i32::MIN) || rounded > f64::from(i32::MAX) {
        return Err(PolylineError::CoordinateOutOfRange { index, coordinate });
    }

    let current = rounded as i32;
    let delta = current
        .checked_sub(previous)
        .ok_or(PolylineError::CoordinateDeltaOutOfRange { index, coordinate })?;
    encode_value(out, delta);
    Ok(current)
}

pub(crate) struct Encoder {
    out: Vec<Char>,
    scale: f64,
    last_lat: i32,
    last_lon: i32,
}

impl Encoder {
    pub(crate) fn new(scale: &PrecisionScale, capacity: usize) -> Self {
        Self {
            out: Vec::with_capacity(capacity),
            scale: scale.scale,
            last_lat: 0,
            last_lon: 0,
        }
    }

    pub(crate) fn push<const LATLON: bool>(
        &mut self,
        (first, second): (f64, f64),
        index: usize,
    ) -> Result<(), PolylineError> {
        let (lat_f, lon_f) = if LATLON {
            (first, second)
        } else {
            (second, first)
        };

        // `f64::round` preserves this codec's half-away-from-zero rule.
        self.last_lat = encode_coordinate(
            &mut self.out,
            lat_f,
            self.scale,
            self.last_lat,
            index,
            "latitude",
        )?;
        self.last_lon = encode_coordinate(
            &mut self.out,
            lon_f,
            self.scale,
            self.last_lon,
            index,
            "longitude",
        )?;
        Ok(())
    }

    pub(crate) fn finish(self) -> Vec<Char> {
        self.out
    }
}
