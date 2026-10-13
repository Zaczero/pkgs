#![expect(
    clippy::similar_names,
    reason = "axis and formula symbols name paired quantities here (minx/miny, xs/ys, lat_rad/lam_rad, step/stop), which this lint reads as typos"
)]
mod builder;
mod columns;
mod construct;
mod iter;
mod select;
pub(crate) use builder::CoordSeqBuilder;
pub(crate) use columns::{MColumn, SharedColumns, coordinate_columns_eq};
use columns::{admit_m_column, reversed_column};
pub(crate) use construct::{coerce_to_common_axes, coerced_pair, concat_coord_columns};
pub(crate) use iter::CoordIter;
#[cfg(test)]
mod tests;
use std::ops::Range;

use crate::error::Result;
use crate::geometry::column_all_finite;
use crate::geometry::types::{
    CoordinateAxes, GeometryErrorKind, HasM, HasZ, MOrdinate, Point, SharedSlice, XY, ZOrdinate,
    ensure_coordseq_vertex_capacity,
};
use crate::work::{Unmetered, Work};

/// Checked coordinate-row window into a [`CoordSeq`]'s physical columns.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CoordWindow {
    start: usize,
    len: usize,
}

impl CoordWindow {
    pub(crate) fn checked(range: Range<usize>, physical_len: usize) -> Result<Self> {
        if range.start > range.end || range.end > physical_len {
            return Err(GeometryErrorKind::CoordinateRange {
                start: range.start,
                end: range.end,
                length: physical_len,
            }
            .into());
        }
        Ok(Self {
            start: range.start,
            len: range.end.wrapping_sub(range.start),
        })
    }

    /// Construct a window at an internal boundary that already owns the range.
    /// Invalid ranges fail here in every build rather than escaping into a
    /// later slice operation.
    pub(crate) fn from_range(range: Range<usize>, physical_len: usize) -> Self {
        Self::checked(range, physical_len).expect("coordinate window must reference existing rows")
    }

    pub(crate) const fn start(self) -> usize {
        self.start
    }

    pub(crate) const fn end(self) -> usize {
        self.start.wrapping_add(self.len)
    }

    pub(crate) const fn len(self) -> usize {
        self.len
    }
}

/// Address-only identity for one visible XY window.
///
/// The owning CoordSeq borrow keeps both immutable Arc-backed columns alive;
/// including the visible starts distinguishes views into one backing owner.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct VisibleXYKey {
    x: usize,
    y: usize,
    len: usize,
}

/// Address-only identity for one visible coordinate run with all active axes.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct VisiblePointRunKey {
    x: usize,
    y: usize,
    z: usize,
    m: usize,
    axes: u8,
    len: usize,
}

/// Struct-of-arrays coordinate storage: parallel ordinate columns in place of a
/// `Vec<Point>` (40 B/vertex, every vertex carrying inactive Z/M sentinels).
///
/// The common XY case stores 16 B/vertex (two `f64` columns) instead of 40 —
/// 2.5× less cache/bandwidth — and contiguous `f64` columns let reductions
/// autovectorize and back zero-copy Arrow buffers. `Point` stays the by-value
/// interchange type: iterate with [`iter`](Self::iter)/[`points`](Self::points)
/// (which yield `Point` by value), index with [`point_at`](Self::point_at), and
/// read hot kernels straight off the [`xs`](Self::xs)/[`ys`](Self::ys) columns.
///
/// Column presence is the single source of axis truth: `zs`/`ms` are `Some`
/// exactly when the sequence carries that ordinate (see [`axes`](Self::axes)).
#[derive(Clone, Debug)]
pub struct CoordSeq {
    // `Arc`-shared columns + a row window: cloning a sequence (and any
    // `Shape` holding one) is O(parts), and `view` hands out zero-copy
    // sub-sequences — the enabler for packed lineal array storage.
    xs: SharedSlice<f64>,
    ys: SharedSlice<f64>,
    zs: Option<SharedSlice<f64>>,
    ms: Option<MColumn>,
    range: Range<u32>,
}

// Three (fat) `Arc` pointers, an explicit M-column owner, and the u32 window;
// the storage win is per-vertex, not per-sequence: the XY case (the common one)
// holds 16 B/vertex in two `f64` columns versus the 40-byte AoS `Point` — 2.5×
// less cache/bandwidth, and contiguous columns the reducers read with no gather.

impl AsRef<Self> for CoordSeq {
    fn as_ref(&self) -> &Self {
        self
    }
}

impl CoordSeq {
    /// Step-`1` slice bounds with `start <= stop` and both in-range describe a
    /// contiguous row window for packed point `__getitem__` slicing.
    pub(crate) const fn contiguous_positive_slice(
        start: isize,
        stop: isize,
        step: isize,
    ) -> Option<Range<usize>> {
        if step != 1 || start < 0 || stop < start {
            return None;
        }
        Some(start as usize..stop as usize)
    }

    pub fn len(&self) -> usize {
        self.range.len()
    }

    pub fn is_empty(&self) -> bool {
        self.range.is_empty()
    }

    /// The axes carried by every coordinate, derived from column presence.
    pub const fn axes(&self) -> CoordinateAxes {
        CoordinateAxes::new(HasZ(self.zs.is_some()), HasM(self.ms.is_some()))
    }
}

macro_rules! dispatch_coord_iter {
    ($self:expr, $method:ident $(, $arg:expr)*) => {
        match $self {
            CoordIter::Xy(iter) => iter.$method($($arg),*),
            CoordIter::Xyz(iter) => iter.$method($($arg),*),
            CoordIter::Xym(iter) => iter.$method($($arg),*),
            CoordIter::Xyzm(iter) => iter.$method($($arg),*),
        }
    };
}
