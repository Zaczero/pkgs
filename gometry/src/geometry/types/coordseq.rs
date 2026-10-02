#![expect(
    clippy::similar_names,
    reason = "axis and formula symbols name paired quantities here (minx/miny, xs/ys, lat_rad/lam_rad, step/stop), which this lint reads as typos"
)]
use std::ops::Range;
use std::simd::cmp::SimdPartialEq as _;
use std::simd::num::SimdFloat as _;

use crate::error::Result;
use crate::geometry::types::{
    CoordinateAxes, GeometryErrorKind, HasM, HasZ, MOrdinate, Point, SharedSlice, XY, ZOrdinate,
    ensure_coordseq_vertex_capacity,
};
use crate::geometry::{REDUCE_LANES, ReduceSimd, column_all_finite, simd_mask_all};

/// One sequence's shared column storage and coordinate row window (see
/// [`CoordSeq::column_arcs`]).
pub(crate) struct SharedColumns {
    pub xs: SharedSlice<f64>,
    pub ys: SharedSlice<f64>,
    pub zs: Option<SharedSlice<f64>>,
    pub ms: Option<MColumn>,
    pub window: Range<usize>,
}

impl SharedColumns {
    /// Return the M owner and its visible window in the same shape consumed by
    /// Arrow primitive-array builders.
    pub(crate) fn m_parts(&self) -> Option<(SharedSlice<f64>, Range<usize>)> {
        self.ms
            .as_ref()
            .map(|column| (column.owner(), column.window(self.window.clone())))
    }
}

/// M storage is either aligned with the shared X/Y/Z columns or has an
/// independent offset into a local owner. The offset advances when a nested
/// coordinate view is made; local views never copy their M payload.
#[derive(Debug)]
pub(crate) enum MColumn {
    Shared(SharedSlice<f64>),
    Local {
        owner: SharedSlice<f64>,
        offset: usize,
    },
}

impl Clone for MColumn {
    fn clone(&self) -> Self {
        match self {
            Self::Shared(owner) => Self::Shared(owner.clone()),
            Self::Local { owner, offset } => Self::Local {
                owner: owner.clone(),
                offset: *offset,
            },
        }
    }
}

impl MColumn {
    fn is_shared(&self) -> bool {
        match self {
            Self::Shared(owner) | Self::Local { owner, .. } => owner.is_shared(),
        }
    }

    fn owner(&self) -> SharedSlice<f64> {
        match self {
            Self::Shared(owner) | Self::Local { owner, .. } => owner.clone(),
        }
    }

    pub(crate) fn values(&self, range: Range<usize>) -> &[f64] {
        let window = self.window(range);
        match self {
            Self::Shared(owner) | Self::Local { owner, .. } => &owner[window],
        }
    }

    const fn window(&self, range: Range<usize>) -> Range<usize> {
        match self {
            Self::Shared(_) => range,
            Self::Local { offset, .. } => *offset..*offset + (range.end - range.start),
        }
    }

    fn view(&self, window: Range<usize>) -> Self {
        match self {
            Self::Shared(owner) => Self::Shared(owner.clone()),
            Self::Local { owner, offset } => Self::Local {
                owner: owner.clone(),
                offset: *offset + window.start,
            },
        }
    }

    fn carried(&self, xy_range: Range<usize>, xy_physical: usize) -> Self {
        match self {
            Self::Shared(owner) if xy_range.start == 0 && xy_range.end == xy_physical => {
                Self::Shared(owner.clone())
            },
            Self::Shared(owner) => Self::Local {
                owner: owner.clone(),
                offset: xy_range.start,
            },
            Self::Local { owner, offset } => Self::Local {
                owner: owner.clone(),
                offset: *offset,
            },
        }
    }
}

fn local_m_covers(owner: &SharedSlice<f64>, offset: usize, visible: usize) -> Result<()> {
    let Some(remaining) = owner.len().checked_sub(offset) else {
        return Err(GeometryErrorKind::CoordinateRange {
            start: offset,
            end: offset,
            length: owner.len(),
        }
        .into());
    };
    if visible > remaining {
        return Err(GeometryErrorKind::CoordinateRange {
            start: offset,
            end: offset + visible,
            length: owner.len(),
        }
        .into());
    }
    Ok(())
}

fn admit_m_column(ms: Option<&MColumn>, xy_physical: usize, visible: usize) -> Result<()> {
    match ms {
        None => Ok(()),
        Some(MColumn::Shared(owner)) => {
            if owner.len() == xy_physical {
                Ok(())
            } else {
                Err(GeometryErrorKind::CoordinateLength(xy_physical, owner.len()).into())
            }
        },
        Some(MColumn::Local { owner, offset }) => local_m_covers(owner, *offset, visible),
    }
}

fn reversed_column(values: &[f64]) -> Vec<f64> {
    values.iter().rev().copied().collect()
}

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
            len: range.end - range.start,
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
        self.start + self.len
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

impl crate::HeapSize for MColumn {
    fn heap_bytes(&self) -> usize {
        self.heap_bytes_saturating()
    }
}

impl MColumn {
    fn heap_bytes_saturating(&self) -> usize {
        match self {
            Self::Shared(owner) | Self::Local { owner, .. } => owner.shallow_heap_bytes(),
        }
    }

    fn logical_heap_bytes(&self, range: Range<usize>) -> usize {
        match self {
            Self::Shared(owner) => owner.logical_heap_bytes(range),
            Self::Local { owner, offset } => {
                owner.logical_heap_bytes(*offset..*offset + range.len())
            },
        }
    }

    fn is_full_owner(&self, physical_len: usize) -> bool {
        match self {
            Self::Shared(owner) => owner.len() == physical_len,
            Self::Local { owner, offset } => *offset == 0 && owner.len() == physical_len,
        }
    }
}

impl crate::HeapSize for CoordSeq {
    fn heap_bytes(&self) -> usize {
        self.heap_bytes_saturating()
    }
}

impl CoordSeq {
    /// Identity of a visible XY window when its backing columns are shared.
    /// Empty runs have no point payload and need no owner key.
    pub(crate) fn shared_xy_key(&self) -> Option<VisibleXYKey> {
        if self.is_empty() || !(self.xs.is_shared() || self.ys.is_shared()) {
            return None;
        }
        Some(VisibleXYKey {
            x: self.xs().as_ptr().addr(),
            y: self.ys().as_ptr().addr(),
            len: self.len(),
        })
    }

    pub(crate) fn shared_z_key(&self) -> Option<(usize, usize)> {
        let owner = self.zs.as_ref()?;
        let values = self.zs()?;
        (owner.is_shared() && !values.is_empty()).then(|| (values.as_ptr().addr(), values.len()))
    }

    pub(crate) fn shared_m_key(&self) -> Option<(usize, usize)> {
        let owner = self.ms.as_ref()?;
        let values = self.ms()?;
        (owner.is_shared() && !values.is_empty()).then(|| (values.as_ptr().addr(), values.len()))
    }

    /// Identity of a visible full-ordinate window when any active column is
    /// shared. The M address comes from the visible slice so local offsets are
    /// part of the key.
    pub(crate) fn shared_point_run_key(&self) -> Option<VisiblePointRunKey> {
        if self.is_empty()
            || !(self.xs.is_shared()
                || self.ys.is_shared()
                || self.zs.as_ref().is_some_and(SharedSlice::is_shared)
                || self.ms.as_ref().is_some_and(MColumn::is_shared))
        {
            return None;
        }
        Some(VisiblePointRunKey {
            x: self.xs().as_ptr().addr(),
            y: self.ys().as_ptr().addr(),
            z: self.zs().map_or(0, |values| values.as_ptr().addr()),
            m: self.ms().map_or(0, |values| values.as_ptr().addr()),
            axes: self.axes().bits(),
            len: self.len(),
        })
    }

    pub(crate) fn heap_bytes_saturating(&self) -> usize {
        self.xs
            .shallow_heap_bytes()
            .saturating_add(self.ys.shallow_heap_bytes())
            .saturating_add(self.zs.as_ref().map_or(0, SharedSlice::shallow_heap_bytes))
            .saturating_add(self.ms.as_ref().map_or(0, MColumn::heap_bytes_saturating))
    }

    /// Fixed owner overhead for a logical view into these shared columns.
    /// Coordinate payload is supplied separately because a packed array can
    /// select fewer rows than the physical sequence owns.
    pub(crate) fn logical_owner_heap_bytes(&self) -> usize {
        let column = |present: bool| {
            if present {
                crate::heap_size::ARC_CONTROL_BYTES + std::mem::size_of::<Vec<f64>>()
            } else {
                0
            }
        };
        column(!self.xs.is_empty())
            .saturating_add(column(!self.ys.is_empty()))
            .saturating_add(column(
                self.zs.as_ref().is_some_and(|column| !column.is_empty()),
            ))
            .saturating_add(column(self.ms.as_ref().is_some_and(
                |column| match column {
                    MColumn::Shared(owner) | MColumn::Local { owner, .. } => !owner.is_empty(),
                },
            )))
    }

    pub(crate) fn logical_heap_bytes_saturating(&self) -> usize {
        let range = self.range.start as usize..self.range.end as usize;
        self.xs
            .logical_heap_bytes(range.clone())
            .saturating_add(self.ys.logical_heap_bytes(range.clone()))
            .saturating_add(
                self.zs
                    .as_ref()
                    .map_or(0, |column| column.logical_heap_bytes(range.clone())),
            )
            .saturating_add(
                self.ms
                    .as_ref()
                    .map_or(0, |column| column.logical_heap_bytes(range)),
            )
    }

    /// Retained bytes for a packed coordinate owner exposing `count` rows.
    /// This keeps full capacity only when the sequence is the complete owner;
    /// selected/rebased views use their logical payload.
    pub(crate) fn logical_heap_bytes_for_count_saturating(&self, count: usize) -> usize {
        if count == self.len()
            && self.range.start == 0
            && self.range.end as usize == self.xs.len()
            && self
                .zs
                .as_ref()
                .is_none_or(|column| column.len() == self.xs.len())
            && self
                .ms
                .as_ref()
                .is_none_or(|column| column.is_full_owner(self.xs.len()))
        {
            self.heap_bytes_saturating()
        } else {
            self.logical_owner_heap_bytes()
                .saturating_add(count.saturating_mul(self.axes().byte_width(1)))
        }
    }
}

fn columns_all_finite(xs: &[f64], ys: &[f64], zs: Option<&[f64]>, ms: Option<&[f64]>) -> bool {
    column_all_finite(xs)
        && column_all_finite(ys)
        && zs.is_none_or(column_all_finite)
        && ms.is_none_or(column_all_finite)
}

impl CoordSeq {
    /// Empty sequence with the given axes.
    pub fn empty(axes: CoordinateAxes) -> Self {
        // Empty owners use the null representation, while the outer options
        // preserve present-empty Z/M axes.
        Self {
            xs: SharedSlice::empty(),
            ys: SharedSlice::empty(),
            zs: axes.has_z().then(SharedSlice::empty),
            ms: axes.has_m().then(|| MColumn::Shared(SharedSlice::empty())),
            range: 0..0,
        }
    }

    /// Fallible point gather for untrusted and resource-covered ingestion.
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
        let mut builder = CoordSeqBuilder::with_capacity(first.axes, points.len());
        for &point in points {
            builder.push(point);
        }
        builder.finish()
    }

    /// Map stored points directly into new columns, inferring axes from the
    /// first output. Callback errors precede column validation.
    pub(crate) fn try_map_points<E>(
        &self,
        mut map: impl FnMut(Point) -> Result<Point, E>,
    ) -> Result<Self, E>
    where
        E: From<crate::error::Error>,
    {
        let mut points = self.iter();
        let Some(first) = points.next() else {
            return Ok(Self::empty(self.axes()));
        };
        let first = map(first)?;
        let mut builder = CoordSeqBuilder::with_capacity(first.axes(), self.len());
        builder.push(first);
        for point in points {
            builder.try_push(map(point))?;
        }
        builder.finish().map_err(E::from)
    }

    /// Gather a slice of `XY` (planar, Z/M-less) coordinates into columns.
    pub fn try_from_xy(points: &[XY]) -> Result<Self> {
        ensure_coordseq_vertex_capacity(points.len())?;
        let mut xs = Vec::with_capacity(points.len());
        let mut ys = Vec::with_capacity(points.len());
        xs.extend(points.iter().map(|point| point.x));
        ys.extend(points.iter().map(|point| point.y));
        Self::try_from_vecs(xs, ys, None, None)
    }

    /// Assemble from owned ordinate vectors. `zs`/`ms` presence sets the axes;
    /// all present columns must match `xs` in length.
    pub fn try_from_vecs(
        xs: Vec<f64>,
        ys: Vec<f64>,
        zs: Option<Vec<f64>>,
        ms: Option<Vec<f64>>,
    ) -> Result<Self> {
        let xs = SharedSlice::from_vec(xs);
        let ys = SharedSlice::from_vec(ys);
        let zs = zs.map(SharedSlice::from_vec);
        let ms = ms.map(|values| MColumn::Shared(SharedSlice::from_vec(values)));
        Self::from_shared_columns(xs, ys, zs, ms)
    }

    pub(crate) fn from_shared_columns(
        xs: SharedSlice<f64>,
        ys: SharedSlice<f64>,
        zs: Option<SharedSlice<f64>>,
        ms: Option<MColumn>,
    ) -> Result<Self> {
        ensure_coordseq_vertex_capacity(xs.len())?;
        if ys.len() != xs.len() {
            return Err(GeometryErrorKind::CoordinateLength(xs.len(), ys.len()).into());
        }
        if let Some(zs) = &zs
            && zs.len() != xs.len()
        {
            return Err(GeometryErrorKind::CoordinateLength(xs.len(), zs.len()).into());
        }
        admit_m_column(ms.as_ref(), xs.len(), xs.len())?;
        // SAFETY: capacity and every column's required length were admitted above.
        Ok(unsafe { Self::from_shared_columns_unchecked(xs, ys, zs, ms) })
    }

    /// Infallible assembly from already-frozen, validated owners.
    ///
    /// # Safety
    ///
    /// XY/Z lengths are parallel. Shared M length equals XY. Local M covers
    /// `offset + XY length` without using remaining suffix as a logical length.
    /// Vertex capacity fits `i32`. Visible length is `0..xs.len()`.
    pub(crate) unsafe fn from_shared_columns_unchecked(
        xs: SharedSlice<f64>,
        ys: SharedSlice<f64>,
        zs: Option<SharedSlice<f64>>,
        ms: Option<MColumn>,
    ) -> Self {
        debug_assert_eq!(xs.len(), ys.len());
        debug_assert!(zs.as_ref().is_none_or(|column| column.len() == xs.len()));
        debug_assert!(admit_m_column(ms.as_ref(), xs.len(), xs.len()).is_ok());
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

    /// Assemble shared coordinate columns with a local M column.
    ///
    /// The window is a checked coordinate boundary, and all inputs are
    /// validated before the resulting sequence can expose them.
    pub(crate) fn from_shared_columns_with_window(
        xs: SharedSlice<f64>,
        ys: SharedSlice<f64>,
        zs: Option<SharedSlice<f64>>,
        ms: Option<MColumn>,
        window: CoordWindow,
    ) -> Result<Self> {
        debug_assert_eq!(xs.len(), ys.len());
        debug_assert!(zs.as_ref().is_none_or(|column| column.len() == xs.len()));
        debug_assert!(window.end() <= xs.len());
        debug_assert!(ensure_coordseq_vertex_capacity(xs.len()).is_ok());
        admit_m_column(ms.as_ref(), xs.len(), window.len())?;
        Ok(Self {
            xs,
            ys,
            zs,
            ms,
            range: window.start() as u32..window.end() as u32,
        })
    }

    /// Zero-copy coordinate sub-sequence: the same shared columns, a narrowed
    /// XY window. Local M advances `offset` by `window.start` once; Shared M
    /// stays aligned to the XY owner. Visible length is `CoordSeq::range`.
    pub fn view(&self, window: CoordWindow) -> Self {
        let start = self.range.start + window.start() as u32;
        let end = self.range.start + window.end() as u32;
        let ms = self
            .ms
            .as_ref()
            .map(|column| column.view(window.start()..window.end()));
        Self {
            xs: self.xs.clone(),
            ys: self.ys.clone(),
            zs: self.zs.clone(),
            ms,
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
    #[cfg(test)]
    pub(crate) fn contiguous_row_window(
        rows: &[usize],
        physical_len: usize,
    ) -> Option<CoordWindow> {
        let first = *rows.first()?;
        let end = first.checked_add(rows.len())?;
        (rows.len() <= 1
            || rows
                .iter()
                .enumerate()
                .all(|(offset, &row)| row.checked_sub(first) == Some(offset)))
        .then(|| CoordWindow::checked(first..end, physical_len).ok())
        .flatten()
    }

    /// Gather selected rows — zero-copy `view` when `rows` is contiguous,
    /// column-wise `select` otherwise (scatter / reversed stride).
    #[cfg(test)]
    pub fn select_rows(&self, rows: &[usize]) -> Result<Self> {
        Self::contiguous_row_window(rows, self.len()).map_or_else(
            || self.select(rows.iter().copied()),
            |window| Ok(self.view(window)),
        )
    }

    /// Swap the X and Y columns — `O(1)`: the columns are `Arc`-shared, so this
    /// swaps two pointers (Z/M and the row window untouched) instead of the
    /// scalar per-vertex `(x, y) → (y, x)` copy a closure map would emit. Same
    /// coordinates, swapped — bit-identical to that map.
    pub fn swap_xy(&self) -> Self {
        Self {
            xs: self.ys.clone(),
            ys: self.xs.clone(),
            zs: self.zs.clone(),
            ms: self.ms.clone(),
            range: self.range.clone(),
        }
    }

    /// The window of one shared column (every accessor routes through this).
    fn window<'a>(&self, column: &'a [f64]) -> &'a [f64] {
        &column[self.range.start as usize..self.range.end as usize]
    }

    /// The shared column storage + this sequence's windows — the zero-copy
    /// hand-off for buffer-protocol exports (Arrow coordinate buffers).
    pub(crate) fn column_arcs(&self) -> SharedColumns {
        SharedColumns {
            xs: self.xs.clone(),
            ys: self.ys.clone(),
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
        Self::try_from_vecs(xs, ys, zs, ms)
    }

    /// Gather selected rows into a new sequence (column-wise, no `Point`
    /// staging) — the packed-array `take`/`filter`/slice engine. Rows must
    /// be in range (callers bounds-check at the boundary).
    ///
    /// `size_hint` is advisory capacity only. Under- or over-yielding
    /// producers still return the true yield; a lying hint never writes
    /// past the allocation or assumes unwritten slots.
    #[expect(
        clippy::impl_trait_in_params,
        reason = "the row stream is consumed once and its concrete iterator type is not part of the API"
    )]
    pub fn select(&self, rows: impl Iterator<Item = usize>) -> Result<Self> {
        let xs_src = self.xs();
        let ys_src = self.ys();
        let zs_src = self.zs();
        let ms_src = self.ms();
        let (lower, upper) = rows.size_hint();
        select_growable(xs_src, ys_src, zs_src, ms_src, rows, upper.unwrap_or(lower))
    }

    pub(in crate::geometry) fn select_mask(&self, keep: &[bool], kept: usize) -> Self {
        debug_assert_eq!(keep.len(), self.len());
        let xs_src = self.xs();
        let ys_src = self.ys();
        let zs_src = self.zs();
        let ms_src = self.ms();
        let mut xs = Vec::with_capacity(kept);
        let mut ys = Vec::with_capacity(kept);
        let mut zs = zs_src.map(|_| Vec::with_capacity(kept));
        let mut ms = ms_src.map(|_| Vec::with_capacity(kept));
        let mut append = |range: std::ops::Range<usize>| {
            xs.extend_from_slice(xs_src.get(range.clone()).expect("selected source run"));
            ys.extend_from_slice(ys_src.get(range.clone()).expect("selected source run"));
            if let (Some(output), Some(source)) = (&mut zs, zs_src) {
                output.extend_from_slice(source.get(range.clone()).expect("selected source run"));
            }
            if let (Some(output), Some(source)) = (&mut ms, ms_src) {
                output.extend_from_slice(source.get(range).expect("selected source run"));
            }
        };
        let mut start = 0_usize;
        while start < keep.len() {
            if !keep.get(start).copied().expect("selection mask cursor") {
                start = start.wrapping_add(1);
                continue;
            }
            let end = start.wrapping_add(
                keep.get(start..)
                    .expect("selection mask suffix")
                    .iter()
                    .position(|&selected| !selected)
                    .unwrap_or(keep.len().wrapping_sub(start)),
            );
            append(start..end);
            start = end.wrapping_add(1);
        }
        let xs = SharedSlice::from_vec(xs);
        let ys = SharedSlice::from_vec(ys);
        let zs = zs.map(SharedSlice::from_vec);
        let ms = ms.map(|values| MColumn::Shared(SharedSlice::from_vec(values)));
        // SAFETY: the same source runs populate every column once, preserving
        // parallel lengths and the source sequence's admitted vertex capacity.
        unsafe { Self::from_shared_columns_unchecked(xs, ys, zs, ms) }
    }

    /// Concatenate two sequences of identical axes. `Ok(None)` means only an
    /// axes mismatch; size or layout overflow is returned as `Err`.
    #[cfg(test)]
    pub fn concat(&self, other: &Self) -> Result<Option<Self>> {
        if self.axes() != other.axes() {
            return Ok(None);
        }
        concat_coord_columns(self, other).map(Some)
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

    pub(crate) fn xy_shared_owner_count(&self) -> usize {
        self.xs.owner_count().min(self.ys.owner_count())
    }

    pub fn zs(&self) -> Option<&[f64]> {
        self.zs.as_deref().map(|column| self.window(column))
    }

    pub fn ms(&self) -> Option<&[f64]> {
        self.ms
            .as_ref()
            .map(|column| column.values(self.range.start as usize..self.range.end as usize))
    }

    /// Whether this sequence's row window spans the whole backing column.
    fn is_full_window(&self, column: &SharedSlice<f64>) -> bool {
        self.range.start == 0 && self.range.end as usize == column.len()
    }

    /// Carry one shared XY/Z column through an op that leaves it untouched:
    /// zero copy when this sequence spans the column's whole backing owner,
    /// otherwise a copy of the visible window.
    fn carried(&self, column: &SharedSlice<f64>) -> SharedSlice<f64> {
        if self.is_full_window(column) {
            column.clone()
        } else {
            SharedSlice::from_vec(self.window(column).to_vec())
        }
    }

    /// The X column carried with zero copy on the full-window common case.
    pub(crate) fn carried_xs(&self) -> SharedSlice<f64> {
        self.carried(&self.xs)
    }

    /// The Y column carried with zero copy on the full-window common case.
    pub(crate) fn carried_ys(&self) -> SharedSlice<f64> {
        self.carried(&self.ys)
    }

    /// The Z column (if present) carried with zero copy on the full-window
    /// common case — for ops that rewrite XY but leave Z untouched.
    pub(crate) fn carried_zs(&self) -> Option<SharedSlice<f64>> {
        self.zs.as_ref().map(|column| self.carried(column))
    }

    /// The M column carried without copying the payload: full Shared stays
    /// Shared; a Shared subrange becomes Local at `range.start`; Local keeps
    /// its existing offset. Visible length stays on `CoordSeq::range`.
    pub(crate) fn carried_ms(&self) -> Option<MColumn> {
        self.ms.as_ref().map(|column| {
            column.carried(
                self.range.start as usize..self.range.end as usize,
                self.xs.len(),
            )
        })
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

    #[cfg(test)]
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

    /// Reverse the vertex order (orientation flips, ring normalization).
    ///
    /// Reverse the vertex order into newly frozen ordinate owners.
    pub fn reversed(&self) -> Result<Self> {
        Self::try_from_vecs(
            reversed_column(self.xs()),
            reversed_column(self.ys()),
            self.zs().map(reversed_column),
            self.ms().map(reversed_column),
        )
    }
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
) -> Result<CoordSeq> {
    let mut xs = Vec::new();
    let mut ys = Vec::new();
    let mut zs = zs_src.map(|_| Vec::new());
    let mut ms = ms_src.map(|_| Vec::new());
    crate::collections::reserve_hint(&mut xs, capacity);
    crate::collections::reserve_hint(&mut ys, capacity);
    if let Some(zs) = &mut zs {
        crate::collections::reserve_hint(zs, capacity);
    }
    if let Some(ms) = &mut ms {
        crate::collections::reserve_hint(ms, capacity);
    }
    for row in rows {
        push_row(
            &mut xs, &mut ys, &mut zs, &mut ms, xs_src, ys_src, zs_src, ms_src, row,
        );
    }
    CoordSeq::try_from_vecs(xs, ys, zs, ms)
}

#[cfg(test)]
mod mask_selection_tests {
    use super::CoordSeq;

    #[test]
    fn mask_selection_copies_disjoint_runs_and_all_ordinates() {
        let source = CoordSeq::try_from_vecs(
            vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
            vec![11.0, 12.0, 13.0, 14.0, 15.0, 16.0],
            Some(vec![21.0, 22.0, 23.0, 24.0, 25.0, 26.0]),
            Some(vec![31.0, 32.0, 33.0, 34.0, 35.0, 36.0]),
        )
        .unwrap();
        let selected = source.select_mask(&[true, true, false, true, false, true], 4);
        assert_eq!(selected.xs(), &[1.0, 2.0, 4.0, 6.0]);
        assert_eq!(selected.ys(), &[11.0, 12.0, 14.0, 16.0]);
        assert_eq!(selected.zs().unwrap(), &[21.0, 22.0, 24.0, 26.0]);
        assert_eq!(selected.ms().unwrap(), &[31.0, 32.0, 34.0, 36.0]);
    }

    #[test]
    fn mask_selection_handles_empty_and_boundary_runs() {
        let source =
            CoordSeq::try_from_vecs(vec![1.0, 2.0, 3.0], vec![11.0, 12.0, 13.0], None, None)
                .unwrap();
        assert!(source.select_mask(&[false, false, false], 0).is_empty());
        let selected = source.select_mask(&[false, true, true], 2);
        assert_eq!(selected.xs(), &[2.0, 3.0]);
        assert_eq!(selected.ys(), &[12.0, 13.0]);
        let selected = source.select_mask(&[true, true, false], 2);
        assert_eq!(selected.xs(), &[1.0, 2.0]);
        assert_eq!(selected.ys(), &[11.0, 12.0]);
    }
}

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

/// Concatenate two axis-homogeneous coordinate columns in one exact allocation
/// per ordinate lane — the trusted packed-storage concat fast path.
///
/// Each lane is reserved to exact length, filled with two `copy_from_slice`
/// writes, and frozen once into an `Arc` (no builder, no double-init zero fill,
/// no spare-capacity shrink on freeze).
#[cfg(test)]
pub(crate) fn concat_coord_columns(left: &CoordSeq, right: &CoordSeq) -> Result<CoordSeq> {
    debug_assert_eq!(left.axes(), right.axes());
    let len = left.len().saturating_add(right.len());
    ensure_coordseq_vertex_capacity(len)?;
    let xs = concat_f64_slices(left.xs(), right.xs(), len);
    let ys = concat_f64_slices(left.ys(), right.ys(), len);
    let zs = match (left.zs(), right.zs()) {
        (Some(left_zs), Some(right_zs)) => Some(concat_f64_slices(left_zs, right_zs, len)),
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
        (Some(left_ms), Some(right_ms)) => Some(concat_f64_slices(left_ms, right_ms, len)),
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
    CoordSeq::try_from_vecs(xs, ys, zs, ms)
}

/// Two-slice join into mutable storage whose original capacity is retained by
/// the final shared owner.
#[cfg(test)]
fn concat_f64_slices(left: &[f64], right: &[f64], output_len: usize) -> Vec<f64> {
    let mut values = Vec::with_capacity(output_len);
    values.extend_from_slice(left);
    values.extend_from_slice(right);
    values
}

/// The two-point sequence of a witness line, keeping the ordinate columns
/// BOTH endpoints carry — the axes intersection.
///
/// A two-vertex sequence on the axes both endpoints carry. Reached through
/// [`crate::geometry::LineSeq::segment`], which is where the rule is documented.
pub(crate) fn coerced_pair(start: Point, end: Point) -> Result<CoordSeq> {
    let (start, end) = coerce_to_common_axes(start, end);
    let mut builder = CoordSeqBuilder::with_capacity(start.axes, 2);
    builder.push(start);
    builder.push(end);
    builder.finish()
}

/// Coerce a witness pair to the ordinate columns BOTH endpoints carry (the axes
/// intersection) — a 2D endpoint never gets fabricated Z/M. The single source of
/// the common-dimensionality rule for `nearest_points`/`shortest_line`, so the
/// witness `LineString` ([`crate::geometry::LineSeq::segment`]) and the point pair cannot diverge:
/// `nearest_points(left, right)` returns exactly `shortest_line`'s endpoints,
/// dimensionality included (an XYZM line vs an XY point yields XY witnesses).
pub(crate) fn coerce_to_common_axes(start: Point, end: Point) -> (Point, Point) {
    let has_z = start.axes.has_z() && end.axes.has_z();
    let has_m = start.axes.has_m() && end.axes.has_m();
    let coerce = |point: Point| {
        Point::new_raw_axes(
            point.x,
            point.y,
            ZOrdinate(has_z.then_some(point.z)),
            MOrdinate(has_m.then_some(point.m)),
        )
    };
    (coerce(start), coerce(end))
}

/// Incremental column builder for [`CoordSeq`] — the one-pass replacement
/// for `Vec<Point>` staging.
///
/// Axes are declared up front (per the design-by-contract boundary rule:
/// sequences are axis-homogeneous and finite, checked on
/// [`finish`](Self::finish)); columns are pre-sized and filled in a single
/// pass over the source.
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

    /// Best-effort reservation for an optional coordinate-count hint.
    pub(crate) fn reserve_hint(&mut self, additional: usize) {
        let _xs_reservation = self.xs.try_reserve(additional);
        let _ys_reservation = self.ys.try_reserve(additional);
        if let Some(column) = self.zs.as_mut() {
            let _zs_reservation = column.try_reserve(additional);
        }
        if let Some(column) = self.ms.as_mut() {
            let _ms_reservation = column.try_reserve(additional);
        }
    }

    fn reserve(&mut self, additional: usize) {
        self.xs.reserve(additional);
        self.ys.reserve(additional);
        if let Some(column) = self.zs.as_mut() {
            column.reserve(additional);
        }
        if let Some(column) = self.ms.as_mut() {
            column.reserve(additional);
        }
    }

    /// The axes this builder was declared with.
    pub(crate) const fn axes(&self) -> CoordinateAxes {
        CoordinateAxes::new(HasZ(self.zs.is_some()), HasM(self.ms.is_some()))
    }

    /// Number of coordinates pushed so far.
    pub(crate) const fn len(&self) -> usize {
        self.xs.len()
    }

    /// Append one point. Every live builder column is written (`Point` always
    /// carries z/m fields). Axes and finiteness are validated on
    /// [`finish`](Self::finish).
    pub(crate) fn push(&mut self, point: Point) {
        if self.xs.len() == self.xs.capacity() {
            self.reserve(1);
        }
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

    /// Propagate a callback error before appending its point.
    pub(crate) fn try_push<E>(
        &mut self,
        result: std::result::Result<Point, E>,
    ) -> std::result::Result<(), E> {
        self.push(result?);
        Ok(())
    }

    /// Append one coordinate from a [`CoordSeq`] by vertex index (gather paths).
    /// Axes and finiteness are validated on [`finish`](Self::finish).
    pub(crate) fn push_at(&mut self, coords: &CoordSeq, index: usize) {
        if self.xs.len() == self.xs.capacity() {
            self.reserve(1);
        }
        if coords.axes() != self.axes() {
            self.mismatch_axes.get_or_insert_with(|| coords.axes());
        }
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
    /// row/CSR validation; axes and finiteness are checked when the builder is
    /// finished.
    pub(crate) fn extend_window(&mut self, coords: &CoordSeq, window: Range<usize>) {
        self.reserve(window.len());
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
    /// exactly when the declared axes carry them; axes and finiteness are
    /// validated on [`finish`](Self::finish).
    pub(crate) fn push_xyzm(&mut self, x: f64, y: f64, z: Option<f64>, m: Option<f64>) {
        if self.xs.len() == self.xs.capacity() {
            self.reserve(1);
        }
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

    /// Seal finite, axis-homogeneous columns into a [`CoordSeq`].
    pub(crate) fn finish(self) -> Result<CoordSeq> {
        if let Some(got) = self.mismatch_axes {
            return Err(GeometryErrorKind::CoordinateAxesMismatch {
                declared: self.axes(),
                got,
            }
            .into());
        }
        if !columns_all_finite(&self.xs, &self.ys, self.zs.as_deref(), self.ms.as_deref()) {
            return Err(GeometryErrorKind::NonFiniteCoordinate.into());
        }
        CoordSeq::try_from_vecs(self.xs, self.ys, self.zs, self.ms)
    }

    /// Trusted finish for mask-owned placeholder lanes that may contain NaN.
    /// Callers must have validated all present-user coordinates before pushing
    /// and use non-finite values only for rows guarded by an external missing
    /// mask.
    #[expect(
        clippy::panic_in_result_fn,
        reason = "placeholder finish is trusted; mismatched axes are a caller programming error"
    )]
    pub(crate) fn finish_with_missing_placeholders(self) -> Result<CoordSeq> {
        assert!(
            self.mismatch_axes.is_none(),
            "missing-coordinate placeholders must preserve the builder axes"
        );
        // Same-axes appends keep live columns the same length. Route through
        // the checked boundary so a pathological length cannot truncate the
        // stored u32 window.
        CoordSeq::try_from_vecs(self.xs, self.ys, self.zs, self.ms)
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
        self.axes() == other.axes() && coordinate_columns_eq(self, other)
    }
}

pub(crate) fn coordinate_columns_eq(
    left: &(impl crate::geometry::Coordinates + ?Sized),
    right: &(impl crate::geometry::Coordinates + ?Sized),
) -> bool {
    let (Some((left_x, left_y)), Some((right_x, right_y))) =
        (left.xy_columns(), right.xy_columns())
    else {
        return false;
    };
    columns_bits_eq(left_x, right_x)
        && columns_bits_eq(left_y, right_y)
        && match (left.z_column(), right.z_column()) {
            (Some(a), Some(b)) => columns_bits_eq(a, b),
            (None, None) => true,
            _ => false,
        }
        && match (left.m_column(), right.m_column()) {
            (Some(a), Some(b)) => columns_bits_eq(a, b),
            (None, None) => true,
            _ => false,
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
                self.index = self.index.wrapping_add(1);
                item
            })
        }

        fn size_hint(&self) -> (usize, Option<usize>) {
            let remaining_claimed = self.claimed.saturating_sub(self.index);
            (remaining_claimed, Some(remaining_claimed))
        }
    }

    fn sample_seq() -> CoordSeq {
        CoordSeq::try_from_vecs(
            vec![0.0, 1.0, 2.0, 3.0],
            vec![10.0, 11.0, 12.0, 13.0],
            None,
            None,
        )
        .expect("sample columns")
    }

    /// Under-yielding with an inflated `size_hint` still returns the true
    /// yield count. The hint only sizes the initial reservation.
    #[test]
    fn select_lying_under_yield_returns_true_length() {
        let seq = sample_seq();
        for claimed in [3, usize::MAX] {
            let out = seq
                .select(LyingIter::under(vec![0, 2], claimed))
                .expect("selection");
            assert_eq!(out.len(), 2);
            assert_eq!(out.xs(), &[0.0, 2.0]);
            assert_eq!(out.ys(), &[10.0, 12.0]);
        }
    }

    /// Over-yielding with a deflated `size_hint` must not write past the
    /// allocation — the result carries every yielded row.
    #[test]
    fn select_lying_over_yield_returns_all_items() {
        let seq = sample_seq();
        // Claims 2, yields [0, 1, 3].
        let out = seq
            .select(LyingIter::over(vec![0, 1, 3], 2))
            .expect("selection");
        assert_eq!(out.len(), 3);
        assert_eq!(out.xs(), &[0.0, 1.0, 3.0]);
        assert_eq!(out.ys(), &[10.0, 11.0, 13.0]);
    }

    /// Honest exact-size iterators still size the reservation from the hint.
    #[test]
    fn select_honest_exact_size_matches_source() {
        let seq = sample_seq();
        let rows = [3_usize, 1, 0];
        let out = seq.select(rows.iter().copied()).expect("selection");
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
    /// axis-homogeneous so `LineSeq::segment` and `nearest_points` cannot diverge.
    #[test]
    fn coerce_to_common_axes_drops_to_intersection() {
        let zm = Point::new_raw_axes(1.0, 0.0, ZOrdinate(Some(2.0)), MOrdinate(Some(12.0)));
        let xy = Point::new_raw_axes(1.0, 3.0, ZOrdinate(None), MOrdinate(None));
        let (a, b) = coerce_to_common_axes(zm, xy);
        assert!(!a.axes.has_z() && !a.axes.has_m());
        assert!(!b.axes.has_z() && !b.axes.has_m());
        assert_eq!(
            a,
            Point::new_raw_axes(1.0, 0.0, ZOrdinate(None), MOrdinate(None))
        );
        assert_eq!(b, xy);
    }

    /// Shared Z (no M on either) is preserved on both endpoints.
    #[test]
    fn coerce_to_common_axes_keeps_shared_z() {
        let a = Point::new_raw_axes(1.0, 0.0, ZOrdinate(Some(2.0)), MOrdinate(None));
        let b = Point::new_raw_axes(1.0, 3.0, ZOrdinate(Some(99.0)), MOrdinate(None));
        let (a2, b2) = coerce_to_common_axes(a, b);
        assert!(a2.axes.has_z() && !a2.axes.has_m());
        assert_eq!(a2, a);
        assert_eq!(b2, b);
    }
}

#[cfg(test)]
mod coord_iter_tests {
    use super::*;

    fn sequence(zs: Option<Vec<f64>>, ms: Option<Vec<f64>>) -> CoordSeq {
        CoordSeq::try_from_vecs(vec![1.0, 2.0, 3.0], vec![4.0, 5.0, 6.0], zs, ms)
            .expect("test coordinate sequence")
    }

    #[test]
    fn specialized_iterators_exhaust_exactly_for_every_axis_variant() {
        for seq in [
            sequence(None, None),
            sequence(Some(vec![7.0, 8.0, 9.0]), None),
            sequence(None, Some(vec![10.0, 11.0, 12.0])),
            sequence(Some(vec![7.0, 8.0, 9.0]), Some(vec![10.0, 11.0, 12.0])),
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
            assert_eq!(seq.iter().collect::<Vec<_>>(), expected);
        }
    }
}
