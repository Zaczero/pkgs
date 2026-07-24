//! Space-filling curve keys — Hilbert (`fast_hilbert`) and Morton (portable
//! magic-bits interleave) over bbox centers: the locality orderings behind
//! `sort_by_spatial_key`, GeoParquet row-group packing, and
//! distributed spatial shuffles. S2 cell ids and tile quadkeys are these
//! same curves over fixed frames.

use crate::error::Result;
use crate::geometry::{Bounds, GeometryErrorKind, Shape};
use crate::py::support::SpatialCurve;

/// The two public space-filling curve families. Keeping the dispatch beside
/// their kernels gives scalar keys, array keys, and sorting one exhaustive
/// definition instead of parallel boundary-layer enums.
#[derive(Clone, Copy)]
pub(crate) enum CurveKind {
    Hilbert,
    Morton,
}

impl CurveKind {
    pub(crate) const fn operation_name(self) -> &'static str {
        "spatial_key"
    }

    pub(crate) fn key(self, frame: &CurveFrame, shape: &Shape) -> Option<u64> {
        match self {
            Self::Hilbert => frame.hilbert_key(shape),
            Self::Morton => frame.morton_key(shape),
        }
    }
}

impl From<SpatialCurve> for CurveKind {
    fn from(value: SpatialCurve) -> Self {
        match value {
            SpatialCurve::Hilbert => Self::Hilbert,
            SpatialCurve::Morton => Self::Morton,
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct CurveLevel(u8);

impl CurveLevel {
    pub(crate) fn try_new(level: u8) -> Result<Self> {
        if (1..=32).contains(&level) {
            Ok(Self(level))
        } else {
            Err(GeometryErrorKind::message(format!(
                "curve level must be between 1 and 32, got {level}"
            )))
        }
    }

    pub(crate) const fn get(self) -> u8 {
        self.0
    }
}

/// One discretization frame: an extent covered by a `level`-order
/// `2^level x 2^level` grid, cell `(0, 0)` at the min corner. Centers
/// outside the frame clamp onto its edge, so explicit bounds can window a
/// larger world.
#[derive(Clone, Copy)]
pub(crate) struct CurveFrame {
    bounds: Bounds,
    level: CurveLevel,
}

impl CurveFrame {
    pub(crate) const fn new(bounds: Bounds, level: CurveLevel) -> Self {
        Self { bounds, level }
    }

    /// Hilbert key of a shape's bbox center (`None` for empty shapes).
    pub(crate) fn hilbert_key(&self, shape: &Shape) -> Option<u64> {
        let (x, y) = self.center_cell(shape)?;
        Some(fast_hilbert::xy2h(x, y, self.level.get()))
    }

    /// Morton (Z-order) key of a shape's bbox center (`None` for empty).
    pub(crate) fn morton_key(&self, shape: &Shape) -> Option<u64> {
        let (x, y) = self.center_cell(shape)?;
        Some(morton_interleave(x, y))
    }

    fn center_cell(&self, shape: &Shape) -> Option<(u32, u32)> {
        let bounds = shape.bounds()?;
        Some(self.cell(
            f64::midpoint(bounds.minx(), bounds.maxx()),
            f64::midpoint(bounds.miny(), bounds.maxy()),
        ))
    }

    fn cell(&self, x: f64, y: f64) -> (u32, u32) {
        let max = ((1_u64 << self.level.get()) - 1) as f64;
        let cell = |value: f64, low: f64, high: f64| -> u32 {
            // Halved terms keep every difference finite even when the raw
            // span (high - low) would overflow at 1e308-scale frames.
            let span = high / 2.0 - low / 2.0;
            if span > 0.0 {
                // Clamp before the cast: centers outside an explicit frame
                // pin to its edge instead of wrapping.
                ((value / 2.0 - low / 2.0) / span * max).clamp(0.0, max) as u32
            } else {
                0
            }
        };
        (
            cell(x, self.bounds.minx(), self.bounds.maxx()),
            cell(y, self.bounds.miny(), self.bounds.maxy()),
        )
    }
}

/// Interleave two 32-bit coordinates into a Morton (Z-order) key — the
/// portable magic-bits spread (BMI2 `pdep` is x86-64-v3 territory; this
/// shape vectorizes fine at the v2 baseline).
pub(crate) fn morton_interleave(x: u32, y: u32) -> u64 {
    spread_bits(x) | (spread_bits(y) << 1)
}

fn spread_bits(value: u32) -> u64 {
    let mut v = u64::from(value);
    v = (v | (v << 16)) & 0x0000_FFFF_0000_FFFF;
    v = (v | (v << 8)) & 0x00FF_00FF_00FF_00FF;
    v = (v | (v << 4)) & 0x0F0F_0F0F_0F0F_0F0F;
    v = (v | (v << 2)) & 0x3333_3333_3333_3333;
    v = (v | (v << 1)) & 0x5555_5555_5555_5555;
    v
}

/// Split a Morton key back into its two coordinates — the inverse of
/// [`morton_interleave`].
pub(crate) const fn morton_deinterleave(key: u64) -> (u32, u32) {
    (compact_bits(key), compact_bits(key >> 1))
}

const fn compact_bits(mut v: u64) -> u32 {
    v &= 0x5555_5555_5555_5555;
    v = (v | (v >> 1)) & 0x3333_3333_3333_3333;
    v = (v | (v >> 2)) & 0x0F0F_0F0F_0F0F_0F0F;
    v = (v | (v >> 4)) & 0x00FF_00FF_00FF_00FF;
    v = (v | (v >> 8)) & 0x0000_FFFF_0000_FFFF;
    v = (v | (v >> 16)) & 0x0000_0000_FFFF_FFFF;
    v as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn morton_interleave_spreads_and_orders() {
        assert_eq!(morton_interleave(0, 0), 0);
        assert_eq!(morton_interleave(1, 0), 1);
        assert_eq!(morton_interleave(0, 1), 2);
        assert_eq!(morton_interleave(1, 1), 3);
        assert_eq!(morton_interleave(u32::MAX, u32::MAX), u64::MAX);
        // x bits land in even positions, y bits in odd ones.
        assert_eq!(morton_interleave(0b101, 0b011), 0b01_1011);
    }

    #[test]
    fn morton_deinterleave_inverts_interleave() {
        for (x, y) in [
            (0, 0),
            (1, 0),
            (0, 1),
            (0b101, 0b011),
            (u32::MAX, 0),
            (0, u32::MAX),
            (u32::MAX, u32::MAX),
            (0xDEAD_BEEF, 0x1234_5678),
        ] {
            assert_eq!(morton_deinterleave(morton_interleave(x, y)), (x, y));
        }
    }

    #[test]
    fn hilbert_keys_are_locality_preserving_and_distinct() {
        let frame = CurveFrame::new(
            Bounds::new_unchecked(0.0, 0.0, 1.0, 1.0),
            CurveLevel::try_new(16).unwrap(),
        );
        let key = |x: f64, y: f64| {
            frame
                .hilbert_key(&Shape::Point(crate::geometry::Point::new_unchecked_xy(
                    x, y,
                )))
                .unwrap()
        };
        // Distinct cells, and near neighbors are closer than far ones.
        let near = key(0.1, 0.1).abs_diff(key(0.1001, 0.1));
        let far = key(0.1, 0.1).abs_diff(key(0.9, 0.9));
        assert!(near < far);
        assert!(frame.hilbert_key(&Shape::empty_point()).is_none());
    }

    #[test]
    fn curve_kind_dispatches_both_core_kernels() {
        let frame = CurveFrame::new(
            Bounds::new_unchecked(0.0, 0.0, 1.0, 1.0),
            CurveLevel::try_new(16).unwrap(),
        );
        let shape = Shape::Point(crate::geometry::Point::new_unchecked_xy(0.25, 0.75));
        assert_eq!(
            CurveKind::Hilbert.key(&frame, &shape),
            frame.hilbert_key(&shape)
        );
        assert_eq!(
            CurveKind::Morton.key(&frame, &shape),
            frame.morton_key(&shape)
        );
        assert_eq!(CurveKind::Hilbert.operation_name(), "spatial_key");
        assert_eq!(CurveKind::Morton.operation_name(), "spatial_key");
    }
}
