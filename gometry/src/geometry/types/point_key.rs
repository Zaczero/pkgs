use super::XY;

crate::heapless!(PointKey);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct PointKey {
    pub x: u64,
    pub y: u64,
}

impl PointKey {
    pub(crate) fn new(point: impl Into<XY>) -> Self {
        let point = point.into();
        Self {
            x: canonical_f64_bits(point.x),
            y: canonical_f64_bits(point.y),
        }
    }
}

pub(crate) fn canonical_f64_bits(value: f64) -> u64 {
    if value == 0.0 {
        0.0_f64.to_bits()
    } else {
        value.to_bits()
    }
}
