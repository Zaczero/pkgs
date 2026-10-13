#[cfg(test)]
use super::construct::concat_coord_columns;
use super::{CoordSeq, CoordWindow, MColumn, Result, SharedSlice, reversed_column};

impl CoordSeq {
    /// Zero-copy coordinate sub-sequence: the same shared columns, a narrowed
    /// XY window. Local M advances `offset` by `window.start` once; Shared M
    /// stays aligned to the XY owner. Visible length is `CoordSeq::range`.
    pub fn view(&self, window: CoordWindow) -> Self {
        let start = self.range.start.wrapping_add(window.start() as u32);
        let end = self.range.start.wrapping_add(window.end() as u32);
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

    #[expect(
        clippy::or_fun_call,
        reason = "the mask-run fallback is bounded arithmetic and remains eagerly evaluated"
    )]
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

#[expect(
    clippy::indexing_slicing,
    reason = "row selection admits each gather index against these parallel coordinate columns"
)]
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
