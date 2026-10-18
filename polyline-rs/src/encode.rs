use crate::constants::{ASCII_OFFSET, CHUNK_BITS, CHUNK_MASK, CONTINUATION_BIT, PrecisionScale};
use crate::errors::PolylineError;
use crate::zigzag::zigzag_encode;

fn encode_value(out: &mut Vec<u8>, delta: i32) {
    let mut value = zigzag_encode(delta);
    while value >= u32::from(CONTINUATION_BIT) {
        let chunk = ((value as u8) & CHUNK_MASK) | CONTINUATION_BIT;
        out.push(chunk.wrapping_add(ASCII_OFFSET));
        value >>= CHUNK_BITS;
    }
    out.push((value as u8).wrapping_add(ASCII_OFFSET));
}

fn encode_coordinate(
    out: &mut Vec<u8>,
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
    // The difference of two i32 coordinates fits in i64; admission below
    // rejects it if the wire's signed 32-bit delta cannot represent it.
    let delta = i64::from(current).wrapping_sub(i64::from(previous));
    let delta = i32::try_from(delta)
        .map_err(|_| PolylineError::CoordinateDeltaOutOfRange { index, coordinate })?;
    encode_value(out, delta);
    Ok(current)
}

pub(crate) struct Encoder {
    out: Vec<u8>,
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

    pub(crate) fn finish(self) -> String {
        // Safety: all bytes are ASCII.
        unsafe { String::from_utf8_unchecked(self.out) }
    }
}
