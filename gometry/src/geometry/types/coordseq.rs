#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use std::ops::Range;
use std::simd::cmp::SimdPartialEq;
use std::simd::num::SimdFloat;
use std::sync::Arc;

use super::*;
use crate::error::Result;
use crate::geometry::{REDUCE_LANES, ReduceSimd, column_all_finite, simd_mask_all};

/// One sequence's shared column storage + row window (see
/// [`CoordSeq::column_arcs`]).
pub struct SharedColumns {
    pub xs: Arc<[f64]>,
    pub ys: Arc<[f64]>,
    pub zs: Option<Arc<[f64]>>,
    pub ms: Option<Arc<[f64]>>,
    pub window: Range<usize>,
}

/// Checked coordinate-row window into a [`CoordSeq`]'s physical columns.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CoordWindow {
    start: usize,
    len: usize,
}

impl CoordWindow {
    pub(crate) fn trusted(range: Range<usize>, physical_len: usize) -> Self {
        debug_assert!(
            range.start <= range.end && range.end <= physical_len,
            "CoordWindow must reference an existing coordinate range",
        );
        Self {
            start: range.start,
            len: range.end - range.start,
        }
    }

    pub(crate) fn checked(range: Range<usize>, physical_len: usize) -> Result<Self> {
        if range.start > range.end || range.end > physical_len {
            return Err(GeometryErrorKind::CoordinateRange.into());
        }
        Ok(Self::trusted(range, physical_len))
    }

    pub(crate) const fn start(self) -> usize {
        self.start
    }

    pub(crate) const fn end(self) -> usize {
        self.start + self.len
    }
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
    xs: Arc<[f64]>,
    ys: Arc<[f64]>,
    zs: Option<Arc<[f64]>>,
    ms: Option<Arc<[f64]>>,
    range: Range<u32>,
}

// Four (fat) `Arc` pointers + the u32 window; the storage win is per-vertex,
// not per-sequence: the XY case (the common one) holds 16 B/vertex in two
// `f64` columns versus the 40-byte AoS `Point` — 2.5× less cache/bandwidth,
// and contiguous columns the reducers read with no gather. If this trips, a
// column field grew unexpectedly.
const _: () = assert!(std::mem::size_of::<CoordSeq>() <= 72);

impl AsRef<Self> for CoordSeq {
    fn as_ref(&self) -> &Self {
        self
    }
}

crate::heapless!(CoordSeq);

fn columns_all_finite(xs: &[f64], ys: &[f64], zs: Option<&[f64]>, ms: Option<&[f64]>) -> bool {
    column_all_finite(xs)
        && column_all_finite(ys)
        && zs.is_none_or(column_all_finite)
        && ms.is_none_or(column_all_finite)
}

impl CoordSeq {
    /// Empty sequence with the given axes.
    pub fn empty(axes: CoordinateAxes) -> Self {
        Self::from_columns_unchecked(
            Arc::from([]),
            Arc::from([]),
            axes.has_z().then(|| Arc::from([])),
            axes.has_m().then(|| Arc::from([])),
        )
    }

    /// Gather an array of `Point`s into columns in one pass. Trusted caller
    /// contract: all points carry the same axes. Use
    /// [`try_from_points`](Self::try_from_points) at untrusted ingestion
    /// boundaries; this fast path keeps internally-proven homogeneous callers
    /// on the straight gather.
    pub fn from_points(points: &[Point]) -> Self {
        let axes = points
            .first()
            .map_or(CoordinateAxes::XY, |point| point.axes);
        // Axis-homogeneity is a sequence invariant (DbC boundary rule); the
        // column gather below reads z/m only for the declared axes.
        debug_assert!(
            points.iter().all(|point| point.axes == axes),
            "from_points requires an axis-homogeneous sequence",
        );
        // Column-wise gather: `slice::Iter` is `TrustedLen`, so each `collect`
        // reserves once and fills via unchecked, vectorizable stores — the
        // strided AoS->SoA read auto-vectorizes (movups + blend), which a
        // per-element `Vec::push` loop cannot, since `push`'s grow path blocks
        // the optimizer. Confirmed in compiled output.
        let xs: Vec<f64> = points.iter().map(|point| point.x).collect();
        let ys: Vec<f64> = points.iter().map(|point| point.y).collect();
        let zs = axes
            .has_z()
            .then(|| points.iter().map(|point| point.z).collect::<Vec<f64>>());
        let ms = axes
            .has_m()
            .then(|| points.iter().map(|point| point.m).collect::<Vec<f64>>());
        Self::from_vecs(xs, ys, zs, ms)
    }

    /// Fallible [`from_points`](Self::from_points) for untrusted ingestion.
    pub fn try_from_points(points: &[Point]) -> Result<Self> {
        let Some(first) = points.first() else {
            return Ok(Self::empty(CoordinateAxes::XY));
        };
        if points.iter().any(|point| point.axes != first.axes) {
            return Err(GeometryErrorKind::CoordinateAxesMismatch.into());
        }
        Ok(Self::from_points(points))
    }

    /// Gather a slice of `XY` (planar, Z/M-less) coordinates into columns — the
    /// XY-slice sibling of [`from_points`](Self::from_points). The planar
    /// engine's boundary lift; building a sequence needs only a borrow.
    pub fn from_xy(points: &[XY]) -> Self {
        Self::from_vecs(
            points.iter().map(|point| point.x).collect(),
            points.iter().map(|point| point.y).collect(),
            None,
            None,
        )
    }

    /// Assemble from owned ordinate vectors. `zs`/`ms` presence sets the axes;
    /// in debug builds all present columns must match `xs` in length.
    pub fn from_vecs(
        xs: Vec<f64>,
        ys: Vec<f64>,
        zs: Option<Vec<f64>>,
        ms: Option<Vec<f64>>,
    ) -> Self {
        Self::from_columns(xs.into(), ys.into(), zs.map(Into::into), ms.map(Into::into))
    }

    /// Release-checked assembly for packed mutators that propagate overflow.
    pub fn try_from_vecs(
        xs: Vec<f64>,
        ys: Vec<f64>,
        zs: Option<Vec<f64>>,
        ms: Option<Vec<f64>>,
    ) -> Result<Self> {
        Self::try_from_columns(xs.into(), ys.into(), zs.map(Into::into), ms.map(Into::into))
    }

    /// Assemble directly from ordinate columns (anything that moves into an
    /// `Arc<[f64]>`: `Vec`, `Box<[f64]>`, `Arc`). `zs`/`ms` presence sets the
    /// axes; in debug builds all present columns must match `xs` in length.
    ///
    /// Carry invariant: when an op rewrites only some ordinates, pass the
    /// untouched ones through [`carried_xs`](Self::carried_xs) and friends
    /// (Arc-share, zero copy) rather than `Arc::from(self.xs())` (a needless
    /// reallocation). Public pointer/storage identity tests cover this contract.
    pub fn from_columns(
        xs: Arc<[f64]>,
        ys: Arc<[f64]>,
        zs: Option<Arc<[f64]>>,
        ms: Option<Arc<[f64]>>,
    ) -> Self {
        Self::try_from_columns(xs, ys, zs, ms)
            .expect("vertex column exceeds i32 CSR offset capacity")
    }

    /// Release-checked assembly for packed mutators that propagate overflow.
    pub fn try_from_columns(
        xs: Arc<[f64]>,
        ys: Arc<[f64]>,
        zs: Option<Arc<[f64]>>,
        ms: Option<Arc<[f64]>>,
    ) -> Result<Self> {
        ensure_coordseq_vertex_capacity(xs.len())?;
        if let Some(other) = [
            Some(ys.len()),
            zs.as_ref().map(|column| column.len()),
            ms.as_ref().map(|column| column.len()),
        ]
        .into_iter()
        .flatten()
        .find(|&len| len != xs.len())
        {
            return Err(GeometryErrorKind::CoordinateLength(xs.len(), other).into());
        }
        Ok(Self::from_columns_unchecked(xs, ys, zs, ms))
    }

    /// Infallible assembly when the caller already enforced the vertex cap.
    pub(crate) fn from_columns_unchecked(
        xs: Arc<[f64]>,
        ys: Arc<[f64]>,
        zs: Option<Arc<[f64]>>,
        ms: Option<Arc<[f64]>>,
    ) -> Self {
        debug_assert_eq!(xs.len(), ys.len());
        debug_assert!(zs.as_ref().is_none_or(|column| column.len() == xs.len()));
        debug_assert!(ms.as_ref().is_none_or(|column| column.len() == xs.len()));
        debug_assert!(ensure_coordseq_vertex_capacity(xs.len()).is_ok());
        let range = 0..xs.len() as u32;
        Self {
            xs,
            ys,
            zs,
            ms,
            range,
        }
    }

    /// Zero-copy sub-sequence: the same shared columns, a narrowed window —
    /// what packed array storage hands out as row geometry.
    pub fn view(&self, window: CoordWindow) -> Self {
        let start = self.range.start + window.start() as u32;
        let end = self.range.start + window.end() as u32;
        Self {
            xs: Arc::clone(&self.xs),
            ys: Arc::clone(&self.ys),
            zs: self.zs.clone(),
            ms: self.ms.clone(),
            range: start..end,
        }
    }

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

    /// `Some` when `rows` is a non-empty strictly increasing run by `1` — the
    /// `take`/`filter` fast-path predicate for zero-copy `view`.
    pub(crate) fn contiguous_row_window(
        rows: &[usize],
        physical_len: usize,
    ) -> Option<CoordWindow> {
        let first = *rows.first()?;
        let end = first + rows.len();
        (rows.len() <= 1
            || rows
                .iter()
                .enumerate()
                .all(|(offset, &row)| row == first + offset))
        .then(|| CoordWindow::checked(first..end, physical_len).ok())
        .flatten()
    }

    /// Gather selected rows — zero-copy `view` when `rows` is contiguous,
    /// column-wise `select` otherwise (scatter / reversed stride).
    pub fn select_rows(&self, rows: &[usize]) -> Self {
        Self::contiguous_row_window(rows, self.len()).map_or_else(
            || self.select(rows.iter().copied()),
            |window| self.view(window),
        )
    }

    /// Swap the X and Y columns — `O(1)`: the columns are `Arc`-shared, so this
    /// swaps two pointers (Z/M and the row window untouched) instead of the
    /// scalar per-vertex `(x, y) → (y, x)` copy a closure map would emit. Same
    /// coordinates, swapped — bit-identical to that map.
    pub fn swap_xy(&self) -> Self {
        Self {
            xs: Arc::clone(&self.ys),
            ys: Arc::clone(&self.xs),
            zs: self.zs.clone(),
            ms: self.ms.clone(),
            range: self.range.clone(),
        }
    }

    /// The window of one shared column (every accessor routes through this).
    fn window<'a>(&self, column: &'a [f64]) -> &'a [f64] {
        &column[self.range.start as usize..self.range.end as usize]
    }

    /// The shared column storage + this sequence's window — the zero-copy
    /// hand-off for buffer-protocol exports (Arrow coordinate buffers).
    pub fn column_arcs(&self) -> SharedColumns {
        SharedColumns {
            xs: Arc::clone(&self.xs),
            ys: Arc::clone(&self.ys),
            zs: self.zs.clone(),
            ms: self.ms.clone(),
            window: self.range.start as usize..self.range.end as usize,
        }
    }

    /// Build owned columns from already-owned `Vec`s, validating every
    /// coordinate is finite — the zero-copy columnar constructor for
    /// column-form input (`line_string(x=, y=)`, `points(...)`) that skips
    /// the per-vertex `Point` build and the `Vec<Point>` re-gather.
    /// `zs`/`ms` being `Some` sets the axes; the caller guarantees equal
    /// column lengths (checked in debug).
    pub fn from_owned_columns(
        xs: Vec<f64>,
        ys: Vec<f64>,
        zs: Option<Vec<f64>>,
        ms: Option<Vec<f64>>,
    ) -> Result<Self> {
        debug_assert_eq!(xs.len(), ys.len());
        if !columns_all_finite(&xs, &ys, zs.as_deref(), ms.as_deref()) {
            return Err(GeometryErrorKind::NonFiniteCoordinate.into());
        }
        Self::try_from_columns(xs.into(), ys.into(), zs.map(Into::into), ms.map(Into::into))
    }

    /// Assemble from `Arc` ordinate columns with finiteness validation — the
    /// columnar constructor fast path when numpy buffers were read directly
    /// into `Arc<[f64]>` (no `Vec`→`Arc` realloc). `zs`/`ms` presence sets
    /// the axes; the caller guarantees equal column lengths (checked in debug).
    pub fn from_arc_columns(
        xs: Arc<[f64]>,
        ys: Arc<[f64]>,
        zs: Option<Arc<[f64]>>,
        ms: Option<Arc<[f64]>>,
    ) -> Result<Self> {
        debug_assert_eq!(xs.len(), ys.len());
        if !columns_all_finite(&xs, &ys, zs.as_deref(), ms.as_deref()) {
            return Err(GeometryErrorKind::NonFiniteCoordinate.into());
        }
        Self::try_from_columns(xs, ys, zs, ms)
    }

    /// Gather selected rows into a new sequence (column-wise, no `Point`
    /// staging) — the packed-array `take`/`filter`/slice engine. Rows must
    /// be in range (callers bounds-check at the boundary).
    pub fn select(&self, rows: impl Iterator<Item = usize>) -> Self {
        let xs_src = self.xs();
        let ys_src = self.ys();
        let zs_src = self.zs();
        let ms_src = self.ms();
        let (lower, upper) = rows.size_hint();
        let capacity = upper.unwrap_or(lower);
        let mut xs = Vec::with_capacity(capacity);
        let mut ys = Vec::with_capacity(capacity);
        let mut zs = zs_src.map(|_| Vec::with_capacity(capacity));
        let mut ms = ms_src.map(|_| Vec::with_capacity(capacity));
        for row in rows {
            xs.push(xs_src[row]);
            ys.push(ys_src[row]);
            if let (Some(out), Some(src)) = (zs.as_mut(), zs_src) {
                out.push(src[row]);
            }
            if let (Some(out), Some(src)) = (ms.as_mut(), ms_src) {
                out.push(src[row]);
            }
        }
        Self::from_columns_unchecked(xs.into(), ys.into(), zs.map(Into::into), ms.map(Into::into))
    }

    /// Concatenate two sequences of identical axes (column-wise); `None` on
    /// an axes mismatch (callers fall back to row storage).
    pub fn concat(&self, other: &Self) -> Option<Self> {
        if self.axes() != other.axes() {
            return None;
        }
        concat_coord_columns(self, other).ok()
    }

    /// Trusted-column constructor: copies the ranges with NO finite
    /// validation — for already-validated flows only (PROJ output is checked
    /// by the pipeline itself, and revalidating would misreport projection
    /// failures as coordinate errors). Lengths must match (debug-checked).
    pub fn copy_from_trusted_columns(
        xs: &[f64],
        ys: &[f64],
        zs: Option<&[f64]>,
        ms: Option<&[f64]>,
    ) -> Self {
        debug_assert_eq!(xs.len(), ys.len());
        debug_assert!(zs.is_none_or(|column| column.len() == xs.len()));
        debug_assert!(ms.is_none_or(|column| column.len() == xs.len()));
        Self::from_columns_unchecked(xs.into(), ys.into(), zs.map(Into::into), ms.map(Into::into))
    }

    pub fn len(&self) -> usize {
        self.range.len()
    }

    pub fn is_empty(&self) -> bool {
        self.range.is_empty()
    }

    /// Raw ordinate-column payload in bytes (`f64` lanes actually stored).
    pub fn coordinate_bytes(&self) -> usize {
        self.axes().byte_width(self.len())
    }

    /// The axes carried by every coordinate, derived from column presence.
    pub const fn axes(&self) -> CoordinateAxes {
        CoordinateAxes::new(HasZ(self.zs.is_some()), HasM(self.ms.is_some()))
    }

    pub fn xs(&self) -> &[f64] {
        self.window(&self.xs)
    }

    pub fn ys(&self) -> &[f64] {
        self.window(&self.ys)
    }

    pub fn zs(&self) -> Option<&[f64]> {
        self.zs.as_deref().map(|column| self.window(column))
    }

    pub fn ms(&self) -> Option<&[f64]> {
        self.ms.as_deref().map(|column| self.window(column))
    }

    /// Whether this sequence's row window spans the whole backing column.
    fn is_full_window(&self, column: &Arc<[f64]>) -> bool {
        self.range.start == 0 && self.range.end as usize == column.len()
    }

    /// Carry one shared column through an op that leaves it untouched: zero
    /// copy (`Arc::clone`) when this sequence spans the column's whole backing
    /// `Arc` (the common case), falling back to copying just the windowed
    /// sub-range otherwise. The single source of truth for the carry rule that
    /// `carried_xs/ys/zs/ms` and every column-preserving constructor rely on.
    fn carried(&self, column: &Arc<[f64]>) -> Arc<[f64]> {
        if self.is_full_window(column) {
            Arc::clone(column)
        } else {
            Arc::from(self.window(column))
        }
    }

    /// The X column carried with zero copy on the full-window common case.
    pub(crate) fn carried_xs(&self) -> Arc<[f64]> {
        self.carried(&self.xs)
    }

    /// The Y column carried with zero copy on the full-window common case.
    pub(crate) fn carried_ys(&self) -> Arc<[f64]> {
        self.carried(&self.ys)
    }

    /// The Z column (if present) carried with zero copy on the full-window
    /// common case — for ops that rewrite XY but leave Z untouched.
    pub(crate) fn carried_zs(&self) -> Option<Arc<[f64]>> {
        self.zs.as_ref().map(|column| self.carried(column))
    }

    /// The M column (if present) carried with zero copy on the full-window
    /// common case — for ops that rewrite XY but leave M untouched.
    pub(crate) fn carried_ms(&self) -> Option<Arc<[f64]>> {
        self.ms.as_ref().map(|column| self.carried(column))
    }

    /// Reconstruct the `Point` at `index` (panics out of bounds, like slice
    /// indexing). Prefer column access in hot loops; this is for the by-value
    /// `Point` interchange paths.
    pub fn point_at(&self, index: usize) -> Point {
        // Windowed columns: length equals `self.len()`, so `0..len` iteration
        // proves each access in range (bounds-check elision).
        let xs = self.xs();
        let ys = self.ys();
        debug_assert!(index < xs.len());
        Point {
            x: xs[index],
            y: ys[index],
            z: self.zs().map_or(0.0, |column| column[index]),
            m: self.ms().map_or(0.0, |column| column[index]),
            axes: self.axes(),
        }
    }

    pub fn get(&self, index: usize) -> Option<Point> {
        (index < self.len()).then(|| self.point_at(index))
    }

    pub fn first(&self) -> Option<Point> {
        (!self.is_empty()).then(|| self.point_at(0))
    }

    pub fn last(&self) -> Option<Point> {
        self.len().checked_sub(1).map(|index| self.point_at(index))
    }

    /// Iterate the coordinates as `Point`s by value.
    pub fn iter(&self) -> CoordIter<'_> {
        CoordIter {
            seq: self,
            index: 0,
            end: self.len(),
        }
    }

    /// Alias for [`iter`](Self::iter); mirrors the shape-level vertex walk.
    pub fn points(&self) -> CoordIter<'_> {
        self.iter()
    }

    /// Materialize the coordinates into a `Vec<Point>` for the `AoS`
    /// interchange boundaries (geo-rs conversion, sorting, arbitrary slice
    /// algorithms).
    pub fn to_vec(&self) -> Vec<Point> {
        self.iter().collect()
    }

    /// Reverse the vertex order (orientation flips, ring normalization).
    pub fn reversed(&self) -> Self {
        let reverse = |column: &[f64]| -> Box<[f64]> { column.iter().rev().copied().collect() };
        Self::from_columns_unchecked(
            reverse(self.xs()).into(),
            reverse(self.ys()).into(),
            self.zs().map(reverse).map(Into::into),
            self.ms().map(reverse).map(Into::into),
        )
    }
}

/// By-value `Point` iterator over a [`CoordSeq`]'s columns.
pub struct CoordIter<'a> {
    seq: &'a CoordSeq,
    index: usize,
    end: usize,
}

impl Iterator for CoordIter<'_> {
    type Item = Point;

    fn next(&mut self) -> Option<Point> {
        (self.index < self.end).then(|| {
            let point = self.seq.point_at(self.index);
            self.index += 1;
            point
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.end - self.index;
        (remaining, Some(remaining))
    }
}

impl DoubleEndedIterator for CoordIter<'_> {
    fn next_back(&mut self) -> Option<Point> {
        (self.index < self.end).then(|| {
            self.end -= 1;
            self.seq.point_at(self.end)
        })
    }
}

impl ExactSizeIterator for CoordIter<'_> {}

impl<'a> IntoIterator for &'a CoordSeq {
    type Item = Point;
    type IntoIter = CoordIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl From<Vec<Point>> for CoordSeq {
    fn from(points: Vec<Point>) -> Self {
        Self::from_points(&points)
    }
}

impl From<Vec<XY>> for CoordSeq {
    fn from(points: Vec<XY>) -> Self {
        Self::from_xy(&points)
    }
}

/// Concatenate two axis-homogeneous coordinate columns in one allocation per
/// ordinate lane — the trusted packed-storage concat fast path.
pub(crate) fn concat_coord_columns(left: &CoordSeq, right: &CoordSeq) -> Result<CoordSeq> {
    debug_assert_eq!(left.axes(), right.axes());
    let len = left.len() + right.len();
    ensure_coordseq_vertex_capacity(len)?;
    let mut xs = Vec::with_capacity(len);
    xs.extend_from_slice(left.xs());
    xs.extend_from_slice(right.xs());
    let mut ys = Vec::with_capacity(len);
    ys.extend_from_slice(left.ys());
    ys.extend_from_slice(right.ys());
    let zs = match (left.zs(), right.zs()) {
        (Some(left_zs), Some(right_zs)) => {
            let mut column = Vec::with_capacity(len);
            column.extend_from_slice(left_zs);
            column.extend_from_slice(right_zs);
            Some(column)
        },
        (None, None) => None,
        _ => {
            debug_assert!(false, "concat_coord_columns requires matching axes");
            return Err(GeometryErrorKind::CoordinateAxesMismatch.into());
        },
    };
    let ms = match (left.ms(), right.ms()) {
        (Some(left_ms), Some(right_ms)) => {
            let mut column = Vec::with_capacity(len);
            column.extend_from_slice(left_ms);
            column.extend_from_slice(right_ms);
            Some(column)
        },
        (None, None) => None,
        _ => {
            debug_assert!(false, "concat_coord_columns requires matching axes");
            return Err(GeometryErrorKind::CoordinateAxesMismatch.into());
        },
    };
    Ok(CoordSeq::from_columns_unchecked(
        xs.into(),
        ys.into(),
        zs.map(Into::into),
        ms.map(Into::into),
    ))
}

/// The two-point sequence of a witness line, keeping the ordinate columns
/// BOTH endpoints carry — the axes intersection.
///
/// Backs `shortest_line`, `minimum_clearance_line`, and the circle-radius
/// lines. A vertex copy or interpolated foot keeps its resolvable Z/M when
/// the other side has them too, while a 2D endpoint never gets fabricated
/// zeros (sequences are axis-homogeneous, so a mixed pair cannot be stored
/// verbatim).
pub(crate) fn witness_pair(start: Point, end: Point) -> CoordSeq {
    let (start, end) = coerce_to_common_axes(start, end);
    let mut builder = CoordSeqBuilder::with_capacity(start.axes, 2);
    builder.push(start);
    builder.push(end);
    builder.finish_infallible()
}

/// Coerce a witness pair to the ordinate columns BOTH endpoints carry (the axes
/// intersection) — a 2D endpoint never gets fabricated Z/M. The single source of
/// the common-dimensionality rule for `nearest_points`/`shortest_line`, so the
/// witness `LineString` ([`witness_pair`]) and the point pair cannot diverge:
/// `nearest_points(left, right)` returns exactly `shortest_line`'s endpoints,
/// dimensionality included (an XYZM line vs an XY point yields XY witnesses).
pub(crate) fn coerce_to_common_axes(start: Point, end: Point) -> (Point, Point) {
    let has_z = start.axes.has_z() && end.axes.has_z();
    let has_m = start.axes.has_m() && end.axes.has_m();
    let coerce = |point: Point| {
        Point::new_unchecked_axes(
            point.x,
            point.y,
            ZOrdinate(has_z.then_some(point.z)),
            MOrdinate(has_m.then_some(point.m)),
        )
    };
    (coerce(start), coerce(end))
}

impl FromIterator<Point> for CoordSeq {
    /// One-pass gather: axes come from the first point (a valid sequence is
    /// axis-homogeneous), capacity from the size hint — no `Vec<Point>` stage.
    fn from_iter<I: IntoIterator<Item = Point>>(iter: I) -> Self {
        let mut points = iter.into_iter();
        let Some(first) = points.next() else {
            return Self::empty(CoordinateAxes::XY);
        };
        let mut builder =
            CoordSeqBuilder::with_capacity(first.axes, points.size_hint().0.saturating_add(1));
        builder.push(first);
        points.for_each(|point| builder.push(point));
        builder.finish().expect("axis-homogeneous Point iterator")
    }
}

/// Incremental column builder for [`CoordSeq`] — the one-pass replacement
/// for `Vec<Point>` staging.
///
/// Axes are declared up front (per the design-by-contract boundary rule:
/// sequences are axis-homogeneous, checked on [`finish`](Self::finish));
/// columns are pre-sized and filled in a single pass over the source.
pub(crate) struct CoordSeqBuilder {
    xs: Vec<f64>,
    ys: Vec<f64>,
    zs: Option<Vec<f64>>,
    ms: Option<Vec<f64>>,
    axes_mismatch: bool,
}

impl CoordSeqBuilder {
    /// A builder for `capacity` coordinates carrying `axes` ordinates.
    pub(crate) fn with_capacity(axes: CoordinateAxes, capacity: usize) -> Self {
        Self {
            xs: Vec::with_capacity(capacity),
            ys: Vec::with_capacity(capacity),
            zs: axes.has_z().then(|| Vec::with_capacity(capacity)),
            ms: axes.has_m().then(|| Vec::with_capacity(capacity)),
            axes_mismatch: false,
        }
    }

    /// Like [`Self::with_capacity`], using a source sequence's axes.
    pub(crate) fn like_coords(coords: &CoordSeq, capacity: usize) -> Self {
        Self::with_capacity(coords.axes(), capacity)
    }

    /// Fallibly reserve exact capacity on every live column. Used by
    /// constructive paths that have already enforced a coordinate budget and
    /// want an OOM error instead of an abort at the final allocation.
    pub(crate) fn try_reserve_exact(
        &mut self,
        additional: usize,
    ) -> std::result::Result<(), std::collections::TryReserveError> {
        self.xs.try_reserve_exact(additional)?;
        self.ys.try_reserve_exact(additional)?;
        if let Some(zs) = &mut self.zs {
            zs.try_reserve_exact(additional)?;
        }
        if let Some(ms) = &mut self.ms {
            ms.try_reserve_exact(additional)?;
        }
        Ok(())
    }

    /// The axes this builder was declared with.
    pub(crate) const fn axes(&self) -> CoordinateAxes {
        CoordinateAxes::new(HasZ(self.zs.is_some()), HasM(self.ms.is_some()))
    }

    /// Number of coordinates pushed so far.
    pub(crate) const fn len(&self) -> usize {
        self.xs.len()
    }

    /// Current allocated capacity of the X column (all columns stay in lockstep).
    pub(crate) const fn capacity_slots(&self) -> usize {
        self.xs.capacity()
    }

    /// Append one point (must match the declared axes — validated on
    /// [`finish`](Self::finish)).
    pub(crate) fn push(&mut self, point: Point) {
        if point.axes != self.axes() {
            self.axes_mismatch = true;
        }
        self.xs.push(point.x);
        self.ys.push(point.y);
        if let Some(zs) = &mut self.zs {
            zs.push(point.z);
        }
        if let Some(ms) = &mut self.ms {
            ms.push(point.m);
        }
    }

    /// Fallible [`push`](Self::push) — propagates the caller's error type.
    pub(crate) fn try_push<E>(
        &mut self,
        result: std::result::Result<Point, E>,
    ) -> std::result::Result<(), E> {
        self.push(result?);
        Ok(())
    }

    /// Append one coordinate from a [`CoordSeq`] by vertex index (gather paths).
    pub(crate) fn push_at(&mut self, coords: &CoordSeq, index: usize) {
        self.xs.push(coords.xs()[index]);
        self.ys.push(coords.ys()[index]);
        if let (Some(zs), Some(column)) = (self.zs.as_mut(), coords.zs()) {
            zs.push(column[index]);
        }
        if let (Some(ms), Some(column)) = (self.ms.as_mut(), coords.ms()) {
            ms.push(column[index]);
        }
    }

    /// Append one contiguous coordinate window column-wise. The caller owns
    /// row/CSR validation; axes are checked when the builder is finished.
    pub(crate) fn extend_window(&mut self, coords: &CoordSeq, window: Range<usize>) {
        if coords.axes() != self.axes() {
            self.axes_mismatch = true;
        }
        self.xs.extend_from_slice(&coords.xs()[window.clone()]);
        self.ys.extend_from_slice(&coords.ys()[window.clone()]);
        if let (Some(output), Some(column)) = (self.zs.as_mut(), coords.zs()) {
            output.extend_from_slice(&column[window.clone()]);
        }
        if let (Some(output), Some(column)) = (self.ms.as_mut(), coords.ms()) {
            output.extend_from_slice(&column[window]);
        }
    }

    /// Append one coordinate from raw ordinates. `z`/`m` must be `Some`
    /// exactly when the declared axes carry them — validated on
    /// [`finish`](Self::finish).
    pub(crate) fn push_xyzm(&mut self, x: f64, y: f64, z: Option<f64>, m: Option<f64>) {
        if z.is_some() != self.zs.is_some() || m.is_some() != self.ms.is_some() {
            self.axes_mismatch = true;
        }
        self.xs.push(x);
        self.ys.push(y);
        if let (Some(zs), Some(z)) = (&mut self.zs, z) {
            zs.push(z);
        }
        if let (Some(ms), Some(m)) = (&mut self.ms, m) {
            ms.push(m);
        }
    }

    /// Seal the columns into a [`CoordSeq`].
    pub(crate) fn finish(self) -> Result<CoordSeq> {
        if self.axes_mismatch {
            return Err(GeometryErrorKind::CoordinateAxesMismatch.into());
        }
        CoordSeq::try_from_vecs(self.xs, self.ys, self.zs, self.ms)
    }

    /// Trusted finish for mask-owned placeholder lanes that may contain NaN.
    /// Callers must have validated all present-user coordinates before pushing
    /// and use non-finite values only for rows guarded by an external missing
    /// mask.
    pub(crate) fn finish_unchecked(self) -> CoordSeq {
        debug_assert!(!self.axes_mismatch);
        CoordSeq::from_columns_unchecked(
            self.xs.into(),
            self.ys.into(),
            self.zs.map(Into::into),
            self.ms.map(Into::into),
        )
    }

    /// [`finish`](Self::finish) for infallible gather paths (same contract as
    /// [`CoordSeq::from_points`](CoordSeq::from_points)).
    pub(crate) fn finish_infallible(self) -> CoordSeq {
        self.finish()
            .expect("axis-homogeneous point column within vertex capacity")
    }

    #[cfg(test)]
    pub(crate) const fn from_mismatched_xy_columns(xs: Vec<f64>, ys: Vec<f64>) -> Self {
        Self {
            xs,
            ys,
            zs: None,
            ms: None,
            axes_mismatch: false,
        }
    }
}

/// Compare two columns by bit pattern (so `NaN` is deterministic and
/// `-0.0`/`0.0` stay distinct), mirroring [`Point`]'s structural identity.
fn columns_bits_eq(left: &[f64], right: &[f64]) -> bool {
    left.len() == right.len()
        && simd_mask_all(
            left.len(),
            |index| left[index].to_bits() == right[index].to_bits(),
            |start| {
                let chunk = start / REDUCE_LANES;
                let (left_chunks, _) = left.as_chunks::<REDUCE_LANES>();
                let (right_chunks, _) = right.as_chunks::<REDUCE_LANES>();
                ReduceSimd::from_array(left_chunks[chunk])
                    .to_bits()
                    .simd_eq(ReduceSimd::from_array(right_chunks[chunk]).to_bits())
            },
        )
}

impl PartialEq for CoordSeq {
    fn eq(&self, other: &Self) -> bool {
        // WINDOWED comparison: a view equals any sequence with the same
        // coordinates, independent of the backing buffer it shares.
        self.axes() == other.axes()
            && columns_bits_eq(self.xs(), other.xs())
            && columns_bits_eq(self.ys(), other.ys())
            && match (self.zs(), other.zs()) {
                (Some(a), Some(b)) => columns_bits_eq(a, b),
                (None, None) => true,
                _ => false,
            }
            && match (self.ms(), other.ms()) {
                (Some(a), Some(b)) => columns_bits_eq(a, b),
                (None, None) => true,
                _ => false,
            }
    }
}

impl Eq for CoordSeq {}

impl std::hash::Hash for CoordSeq {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let hash_column = |column: &[f64], state: &mut H| {
            for value in column {
                value.to_bits().hash(state);
            }
        };
        self.axes().bits().hash(state);
        hash_column(self.xs(), state);
        hash_column(self.ys(), state);
        if let Some(column) = self.zs() {
            hash_column(column, state);
        }
        if let Some(column) = self.ms() {
            hash_column(column, state);
        }
    }
}

#[cfg(test)]
mod coerce_tests {
    use super::*;

    /// The witness pair drops to the axes BOTH endpoints carry: a Z/M endpoint
    /// vs a bare XY endpoint becomes XY (no fabricated Z/M), and the result is
    /// axis-homogeneous so `witness_pair` and `nearest_points` cannot diverge.
    #[test]
    fn coerce_to_common_axes_drops_to_intersection() {
        let zm = Point::new_unchecked_axes(1.0, 0.0, ZOrdinate(Some(2.0)), MOrdinate(Some(12.0)));
        let xy = Point::new_unchecked_axes(1.0, 3.0, ZOrdinate(None), MOrdinate(None));
        let (a, b) = coerce_to_common_axes(zm, xy);
        assert!(!a.axes.has_z() && !a.axes.has_m());
        assert!(!b.axes.has_z() && !b.axes.has_m());
        assert_eq!(
            a,
            Point::new_unchecked_axes(1.0, 0.0, ZOrdinate(None), MOrdinate(None))
        );
        assert_eq!(b, xy);
    }

    /// Shared Z (no M on either) is preserved on both endpoints.
    #[test]
    fn coerce_to_common_axes_keeps_shared_z() {
        let a = Point::new_unchecked_axes(1.0, 0.0, ZOrdinate(Some(2.0)), MOrdinate(None));
        let b = Point::new_unchecked_axes(1.0, 3.0, ZOrdinate(Some(99.0)), MOrdinate(None));
        let (a2, b2) = coerce_to_common_axes(a, b);
        assert!(a2.axes.has_z() && !a2.axes.has_m());
        assert_eq!(a2, a);
        assert_eq!(b2, b);
    }
}
