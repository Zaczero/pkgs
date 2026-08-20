#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use std::ops::Range;
use std::simd::cmp::SimdPartialEq as _;
use std::simd::num::SimdFloat as _;
use std::sync::Arc;

use crate::error::Result;
use crate::geometry::types::{
    CoordinateAxes, GeometryErrorKind, HasM, HasZ, MOrdinate, Point, XY, ZOrdinate,
    ensure_coordseq_vertex_capacity,
};
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
            return Err(GeometryErrorKind::CoordinateRange {
                start: range.start,
                end: range.end,
                length: physical_len,
            }
            .into());
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
// and contiguous columns the reducers read with no gather.

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
        if let Some(point) = points.iter().find(|point| point.axes != first.axes) {
            return Err(GeometryErrorKind::CoordinateAxesMismatch {
                declared: first.axes,
                got: point.axes,
            }
            .into());
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
    ///
    /// When the iterator reports an exact length, columns fill an exact
    /// final `Arc` (no `Vec`/`Box` intermediate). The fill is *checked*:
    /// every write is bounds-gated against the allocation, and
    /// `assume_init` runs only when the actual yield count matches the
    /// hint. Under- or over-yielding producers (lying `size_hint`) fall
    /// back to a growable path that returns the true yield — never UB.
    #[expect(
        clippy::impl_trait_in_params,
        reason = "the row stream is consumed once and its concrete iterator type is not part of the API"
    )]
    pub fn select(&self, rows: impl Iterator<Item = usize>) -> Self {
        let xs_src = self.xs();
        let ys_src = self.ys();
        let zs_src = self.zs();
        let ms_src = self.ms();
        let (lower, upper) = rows.size_hint();
        if upper == Some(lower) {
            return select_exact_or_grow(xs_src, ys_src, zs_src, ms_src, rows, lower);
        }
        select_growable(xs_src, ys_src, zs_src, ms_src, rows, upper.unwrap_or(lower))
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
        let xs = self.xs();
        let ys = self.ys();
        match (self.zs(), self.ms()) {
            (None, None) => CoordIter::Xy(CoordIterColumns::new(xs, ys, &[], &[])),
            (Some(zs), None) => CoordIter::Xyz(CoordIterColumns::new(xs, ys, zs, &[])),
            (None, Some(ms)) => CoordIter::Xym(CoordIterColumns::new(xs, ys, &[], ms)),
            (Some(zs), Some(ms)) => CoordIter::Xyzm(CoordIterColumns::new(xs, ys, zs, ms)),
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
    ///
    /// Exact-final-Arc fill (no `Vec`/`Box` intermediate): allocate the
    /// column Arcs at the known length, write reversed values, `assume_init`.
    pub fn reversed(&self) -> Self {
        Self::from_columns_unchecked(
            reverse_column_arc(self.xs()),
            reverse_column_arc(self.ys()),
            self.zs().map(reverse_column_arc),
            self.ms().map(reverse_column_arc),
        )
    }
}

/// Exact-hint gather into final `Arc` columns, with a checked fill.
///
/// Allocates `expected` slots and writes only while `written < expected`.
/// `assume_init` runs only when the iterator yields *exactly* `expected`
/// items. Under-yield copies the initialized prefix into a correctly sized
/// sequence; over-yield spills the prefix plus remaining items through the
/// growable path. A lying `size_hint` therefore cannot leave uninit memory
/// readable or write past the allocation.
fn select_exact_or_grow(
    xs_src: &[f64],
    ys_src: &[f64],
    zs_src: Option<&[f64]>,
    ms_src: Option<&[f64]>,
    mut rows: impl Iterator<Item = usize>,
    expected: usize,
) -> CoordSeq {
    if expected == 0 {
        // Empty hint: any yield is an over-yield → growable.
        return rows.next().map_or_else(
            || CoordSeq::empty(axes_from_optional_columns(zs_src, ms_src)),
            |first| {
                select_growable(
                    xs_src,
                    ys_src,
                    zs_src,
                    ms_src,
                    std::iter::once(first).chain(rows),
                    1,
                )
            },
        );
    }

    let mut xs = Arc::<[f64]>::new_uninit_slice(expected);
    let mut ys = Arc::<[f64]>::new_uninit_slice(expected);
    let mut zs = zs_src.map(|_| Arc::<[f64]>::new_uninit_slice(expected));
    let mut ms = ms_src.map(|_| Arc::<[f64]>::new_uninit_slice(expected));
    let mut written = 0_usize;

    // SAFETY: unique Arc slices. Every write is gated by `written < expected`,
    // so no OOB store. `assume_init` is reached only when `written == expected`
    // and the iterator is exhausted (no over-yield).
    unsafe {
        let xs_dst = Arc::get_mut(&mut xs)
            .unwrap_unchecked()
            .as_mut_ptr()
            .cast::<f64>();
        let ys_dst = Arc::get_mut(&mut ys)
            .unwrap_unchecked()
            .as_mut_ptr()
            .cast::<f64>();
        let mut zs_dst = zs.as_mut().map(|column| {
            Arc::get_mut(column)
                .unwrap_unchecked()
                .as_mut_ptr()
                .cast::<f64>()
        });
        let mut ms_dst = ms.as_mut().map(|column| {
            Arc::get_mut(column)
                .unwrap_unchecked()
                .as_mut_ptr()
                .cast::<f64>()
        });

        while written < expected {
            let Some(row) = rows.next() else {
                // Under-yield: only `0..written` is initialized.
                return columns_from_partial_ptrs(
                    xs_dst,
                    ys_dst,
                    zs_dst.as_mut().copied(),
                    ms_dst.as_mut().copied(),
                    written,
                    zs_src.is_some(),
                    ms_src.is_some(),
                );
            };
            xs_dst.add(written).write(xs_src[row]);
            ys_dst.add(written).write(ys_src[row]);
            if let (Some(dst), Some(src)) = (zs_dst.as_mut(), zs_src) {
                dst.add(written).write(src[row]);
            }
            if let (Some(dst), Some(src)) = (ms_dst.as_mut(), ms_src) {
                dst.add(written).write(src[row]);
            }
            written += 1;
        }

        if let Some(extra) = rows.next() {
            // Over-yield: prefix is fully initialized at `expected`; spill.
            let mut xs_v = ptr_prefix_to_vec(xs_dst, expected);
            let mut ys_v = ptr_prefix_to_vec(ys_dst, expected);
            let mut zs_v = zs_dst.as_mut().map(|dst| ptr_prefix_to_vec(*dst, expected));
            let mut ms_v = ms_dst.as_mut().map(|dst| ptr_prefix_to_vec(*dst, expected));
            // Forget the uninit Arcs — their storage is abandoned; values
            // already live in the Vecs. Dropping MaybeUninit Arcs is safe
            // (no Drop glue for f64), but avoid double-free by mem::forget
            // only if we assume_init… we did NOT assume_init. Dropping
            // Arc<[MaybeUninit<f64>]> frees the allocation without dropping
            // elements — correct for both init and uninit slots.
            drop((xs, ys, zs, ms));
            push_row(
                &mut xs_v, &mut ys_v, &mut zs_v, &mut ms_v, xs_src, ys_src, zs_src, ms_src, extra,
            );
            for row in rows {
                push_row(
                    &mut xs_v, &mut ys_v, &mut zs_v, &mut ms_v, xs_src, ys_src, zs_src, ms_src, row,
                );
            }
            return CoordSeq::from_columns_unchecked(
                xs_v.into(),
                ys_v.into(),
                zs_v.map(Into::into),
                ms_v.map(Into::into),
            );
        }

        CoordSeq::from_columns_unchecked(
            xs.assume_init(),
            ys.assume_init(),
            zs.map(|column| column.assume_init()),
            ms.map(|column| column.assume_init()),
        )
    }
}

const fn axes_from_optional_columns(zs: Option<&[f64]>, ms: Option<&[f64]>) -> CoordinateAxes {
    match (zs.is_some(), ms.is_some()) {
        (false, false) => CoordinateAxes::XY,
        (true, false) => CoordinateAxes::XYZ,
        (false, true) => CoordinateAxes::XYM,
        (true, true) => CoordinateAxes::XYZM,
    }
}

/// Copy `0..len` from initialized raw column pointers into a `CoordSeq`.
///
/// # Safety
/// `xs`/`ys` (and optional z/m) must each point at least `len` initialized `f64`s.
unsafe fn columns_from_partial_ptrs(
    xs: *mut f64,
    ys: *mut f64,
    zs: Option<*mut f64>,
    ms: Option<*mut f64>,
    len: usize,
    has_z: bool,
    has_m: bool,
) -> CoordSeq {
    // SAFETY: caller guarantees `0..len` is initialized on each present column.
    unsafe {
        CoordSeq::from_columns_unchecked(
            ptr_prefix_to_vec(xs, len).into(),
            ptr_prefix_to_vec(ys, len).into(),
            has_z
                .then(|| zs.expect("z column present when has_z"))
                .map(|ptr| ptr_prefix_to_vec(ptr, len).into()),
            has_m
                .then(|| ms.expect("m column present when has_m"))
                .map(|ptr| ptr_prefix_to_vec(ptr, len).into()),
        )
    }
}

/// # Safety
/// `src` must point at least `len` initialized `f64` values.
unsafe fn ptr_prefix_to_vec(src: *mut f64, len: usize) -> Vec<f64> {
    // SAFETY: caller guarantees `src` has `len` initialized elements; the
    // slice is therefore a valid `&[f64]` of that length.
    unsafe { std::slice::from_raw_parts(src, len).to_vec() }
}

fn push_row(
    xs: &mut Vec<f64>,
    ys: &mut Vec<f64>,
    zs: &mut Option<Vec<f64>>,
    ms: &mut Option<Vec<f64>>,
    xs_src: &[f64],
    ys_src: &[f64],
    zs_src: Option<&[f64]>,
    ms_src: Option<&[f64]>,
    row: usize,
) {
    xs.push(xs_src[row]);
    ys.push(ys_src[row]);
    if let (Some(out), Some(src)) = (zs.as_mut(), zs_src) {
        out.push(src[row]);
    }
    if let (Some(out), Some(src)) = (ms.as_mut(), ms_src) {
        out.push(src[row]);
    }
}

fn select_growable(
    xs_src: &[f64],
    ys_src: &[f64],
    zs_src: Option<&[f64]>,
    ms_src: Option<&[f64]>,
    rows: impl Iterator<Item = usize>,
    capacity: usize,
) -> CoordSeq {
    let mut xs = Vec::with_capacity(capacity);
    let mut ys = Vec::with_capacity(capacity);
    let mut zs = zs_src.map(|_| Vec::with_capacity(capacity));
    let mut ms = ms_src.map(|_| Vec::with_capacity(capacity));
    for row in rows {
        push_row(
            &mut xs, &mut ys, &mut zs, &mut ms, xs_src, ys_src, zs_src, ms_src, row,
        );
    }
    CoordSeq::from_columns_unchecked(xs.into(), ys.into(), zs.map(Into::into), ms.map(Into::into))
}

/// Exact-size reverse into a fresh shared ordinate column.
pub(crate) fn reverse_column_arc(src: &[f64]) -> Arc<[f64]> {
    let len = src.len();
    let mut buf = Arc::<[f64]>::new_uninit_slice(len);
    // SAFETY: unique Arc; every slot `i` is written from `src[len - 1 - i]`.
    // Fresh allocation is non-overlapping with `src`.
    unsafe {
        let dst = Arc::get_mut(&mut buf)
            .unwrap_unchecked()
            .as_mut_ptr()
            .cast::<f64>();
        for i in 0..len {
            dst.add(i).write(src[len - 1 - i]);
        }
        buf.assume_init()
    }
}

#[doc(hidden)]
pub struct CoordIterColumns<'a, const HAS_Z: bool, const HAS_M: bool> {
    xs: &'a [f64],
    ys: &'a [f64],
    zs: &'a [f64],
    ms: &'a [f64],
    axes: CoordinateAxes,
    index: usize,
    end: usize,
}

impl<'a, const HAS_Z: bool, const HAS_M: bool> CoordIterColumns<'a, HAS_Z, HAS_M> {
    fn new(xs: &'a [f64], ys: &'a [f64], zs: &'a [f64], ms: &'a [f64]) -> Self {
        debug_assert!(!HAS_Z || zs.len() == xs.len());
        debug_assert!(!HAS_M || ms.len() == xs.len());
        Self {
            xs,
            ys,
            zs,
            ms,
            axes: CoordinateAxes::new(HasZ(HAS_Z), HasM(HAS_M)),
            index: 0,
            end: xs.len(),
        }
    }

    const fn point_at(&self, index: usize) -> Point {
        Point {
            x: self.xs[index],
            y: self.ys[index],
            z: if HAS_Z { self.zs[index] } else { 0.0 },
            m: if HAS_M { self.ms[index] } else { 0.0 },
            axes: self.axes,
        }
    }
}

impl<const HAS_Z: bool, const HAS_M: bool> Iterator for CoordIterColumns<'_, HAS_Z, HAS_M> {
    type Item = Point;

    fn next(&mut self) -> Option<Point> {
        (self.index < self.end).then(|| {
            let point = self.point_at(self.index);
            self.index += 1;
            point
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.end - self.index;
        (remaining, Some(remaining))
    }
}

impl<const HAS_Z: bool, const HAS_M: bool> DoubleEndedIterator
    for CoordIterColumns<'_, HAS_Z, HAS_M>
{
    fn next_back(&mut self) -> Option<Point> {
        (self.index < self.end).then(|| {
            self.end -= 1;
            self.point_at(self.end)
        })
    }
}

impl<const HAS_Z: bool, const HAS_M: bool> ExactSizeIterator
    for CoordIterColumns<'_, HAS_Z, HAS_M>
{
}

/// By-value `Point` iterator specialized once for the sequence's axes.
pub enum CoordIter<'a> {
    Xy(CoordIterColumns<'a, false, false>),
    Xyz(CoordIterColumns<'a, true, false>),
    Xym(CoordIterColumns<'a, false, true>),
    Xyzm(CoordIterColumns<'a, true, true>),
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

impl Iterator for CoordIter<'_> {
    type Item = Point;

    fn next(&mut self) -> Option<Point> {
        dispatch_coord_iter!(self, next)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        dispatch_coord_iter!(self, size_hint)
    }
}

impl DoubleEndedIterator for CoordIter<'_> {
    fn next_back(&mut self) -> Option<Point> {
        dispatch_coord_iter!(self, next_back)
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

/// Concatenate two axis-homogeneous coordinate columns in one exact allocation
/// per ordinate lane — the trusted packed-storage concat fast path.
///
/// Each lane is reserved to exact length, filled with two `copy_from_slice`
/// writes, and frozen once into an `Arc` (no builder, no double-init zero fill,
/// no spare-capacity shrink on freeze).
pub(crate) fn concat_coord_columns(left: &CoordSeq, right: &CoordSeq) -> Result<CoordSeq> {
    debug_assert_eq!(left.axes(), right.axes());
    let len = left.len() + right.len();
    ensure_coordseq_vertex_capacity(len)?;
    let xs = concat_f64_slices(left.xs(), right.xs());
    let ys = concat_f64_slices(left.ys(), right.ys());
    let zs = match (left.zs(), right.zs()) {
        (Some(left_zs), Some(right_zs)) => Some(concat_f64_slices(left_zs, right_zs)),
        (None, None) => None,
        _ => {
            debug_assert!(false, "concat_coord_columns requires matching axes");
            return Err(GeometryErrorKind::CoordinateAxesMismatch {
                declared: left.axes(),
                got: right.axes(),
            }
            .into());
        },
    };
    let ms = match (left.ms(), right.ms()) {
        (Some(left_ms), Some(right_ms)) => Some(concat_f64_slices(left_ms, right_ms)),
        (None, None) => None,
        _ => {
            debug_assert!(false, "concat_coord_columns requires matching axes");
            return Err(GeometryErrorKind::CoordinateAxesMismatch {
                declared: left.axes(),
                got: right.axes(),
            }
            .into());
        },
    };
    Ok(CoordSeq::from_columns_unchecked(xs, ys, zs, ms))
}

/// Exact-capacity two-slice join into a shared ordinate column (one Arc
/// allocation, two `copy_nonoverlapping` fills — no Vec/Box intermediate).
fn concat_f64_slices(left: &[f64], right: &[f64]) -> Arc<[f64]> {
    let len = left.len() + right.len();
    let mut buf = Arc::<[f64]>::new_uninit_slice(len);
    // SAFETY: both ranges are fully initialized by the copies below; the two
    // source slices are non-overlapping with `buf` (fresh allocation).
    // `get_mut` succeeds because we hold the only Arc reference.
    unsafe {
        let dst = Arc::get_mut(&mut buf)
            .unwrap_unchecked()
            .as_mut_ptr()
            .cast::<f64>();
        core::ptr::copy_nonoverlapping(left.as_ptr(), dst, left.len());
        core::ptr::copy_nonoverlapping(right.as_ptr(), dst.add(left.len()), right.len());
        buf.assume_init()
    }
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
    mismatch_axes: Option<CoordinateAxes>,
}

impl CoordSeqBuilder {
    /// A builder for `capacity` coordinates carrying `axes` ordinates.
    pub(crate) fn with_capacity(axes: CoordinateAxes, capacity: usize) -> Self {
        Self {
            xs: Vec::with_capacity(capacity),
            ys: Vec::with_capacity(capacity),
            zs: axes.has_z().then(|| Vec::with_capacity(capacity)),
            ms: axes.has_m().then(|| Vec::with_capacity(capacity)),
            mismatch_axes: None,
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
            self.mismatch_axes.get_or_insert(point.axes);
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
            self.mismatch_axes.get_or_insert_with(|| coords.axes());
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
            self.mismatch_axes
                .get_or_insert_with(|| CoordinateAxes::new(HasZ(z.is_some()), HasM(m.is_some())));
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
        if let Some(got) = self.mismatch_axes {
            return Err(GeometryErrorKind::CoordinateAxesMismatch {
                declared: self.axes(),
                got,
            }
            .into());
        }
        CoordSeq::try_from_vecs(self.xs, self.ys, self.zs, self.ms)
    }

    /// Trusted finish for mask-owned placeholder lanes that may contain NaN.
    /// Callers must have validated all present-user coordinates before pushing
    /// and use non-finite values only for rows guarded by an external missing
    /// mask.
    pub(crate) fn finish_unchecked(self) -> CoordSeq {
        debug_assert!(self.mismatch_axes.is_none());
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
            mismatch_axes: None,
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
mod select_soundness_tests {
    use super::*;

    /// Iterator that lies about its length via `size_hint` (safe trait abuse).
    struct LyingIter {
        items: Vec<usize>,
        index: usize,
        claimed: usize,
    }

    impl LyingIter {
        fn under(items: Vec<usize>, claimed: usize) -> Self {
            assert!(claimed > items.len());
            Self {
                items,
                index: 0,
                claimed,
            }
        }

        fn over(items: Vec<usize>, claimed: usize) -> Self {
            assert!(claimed < items.len());
            Self {
                items,
                index: 0,
                claimed,
            }
        }
    }

    impl Iterator for LyingIter {
        type Item = usize;

        fn next(&mut self) -> Option<usize> {
            (self.index < self.items.len()).then(|| {
                let item = self.items[self.index];
                self.index += 1;
                item
            })
        }

        fn size_hint(&self) -> (usize, Option<usize>) {
            let remaining_claimed = self.claimed.saturating_sub(self.index);
            (remaining_claimed, Some(remaining_claimed))
        }
    }

    fn sample_seq() -> CoordSeq {
        CoordSeq::from_columns_unchecked(
            Arc::from([0.0, 1.0, 2.0, 3.0]),
            Arc::from([10.0, 11.0, 12.0, 13.0]),
            None,
            None,
        )
    }

    /// Under-yielding with an inflated `size_hint` must not `assume_init`
    /// unwritten slots — the result length is the true yield count.
    #[test]
    fn select_lying_under_yield_returns_true_length() {
        let seq = sample_seq();
        // Claims 3, yields only [0, 2].
        let out = seq.select(LyingIter::under(vec![0, 2], 3));
        assert_eq!(out.len(), 2);
        assert_eq!(out.xs(), &[0.0, 2.0]);
        assert_eq!(out.ys(), &[10.0, 12.0]);
    }

    /// Over-yielding with a deflated `size_hint` must not write past the
    /// allocation — the result carries every yielded row.
    #[test]
    fn select_lying_over_yield_returns_all_items() {
        let seq = sample_seq();
        // Claims 2, yields [0, 1, 3].
        let out = seq.select(LyingIter::over(vec![0, 1, 3], 2));
        assert_eq!(out.len(), 3);
        assert_eq!(out.xs(), &[0.0, 1.0, 3.0]);
        assert_eq!(out.ys(), &[10.0, 11.0, 13.0]);
    }

    /// Honest exact-size iterators still take the exact-Arc path correctly.
    #[test]
    fn select_honest_exact_size_matches_source() {
        let seq = sample_seq();
        let rows = [3_usize, 1, 0];
        let out = seq.select(rows.iter().copied());
        assert_eq!(out.len(), 3);
        assert_eq!(out.xs(), &[3.0, 1.0, 0.0]);
        assert_eq!(out.ys(), &[13.0, 11.0, 10.0]);
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

#[cfg(test)]
mod coord_iter_tests {
    use super::*;

    fn sequence(zs: Option<Arc<[f64]>>, ms: Option<Arc<[f64]>>) -> CoordSeq {
        CoordSeq::from_columns_unchecked(
            Arc::from([1.0, 2.0, 3.0]),
            Arc::from([4.0, 5.0, 6.0]),
            zs,
            ms,
        )
    }

    #[test]
    fn specialized_iterators_exhaust_exactly_for_every_axis_variant() {
        for seq in [
            sequence(None, None),
            sequence(Some(Arc::from([7.0, 8.0, 9.0])), None),
            sequence(None, Some(Arc::from([10.0, 11.0, 12.0]))),
            sequence(
                Some(Arc::from([7.0, 8.0, 9.0])),
                Some(Arc::from([10.0, 11.0, 12.0])),
            ),
        ] {
            let expected = (0..seq.len())
                .map(|index| seq.point_at(index))
                .collect::<Vec<_>>();
            let mut iter = seq.iter();
            assert_eq!(iter.len(), 3);
            assert_eq!(iter.next(), Some(expected[0]));
            assert_eq!(iter.len(), 2);
            assert_eq!(iter.next_back(), Some(expected[2]));
            assert_eq!(iter.len(), 1);
            assert_eq!(iter.next(), Some(expected[1]));
            assert_eq!(iter.len(), 0);
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next_back(), None);
            assert_eq!(seq.to_vec(), expected);
        }
    }
}
