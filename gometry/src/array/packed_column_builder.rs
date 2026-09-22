use std::ops::Range;

use crate::array::column_window;
use crate::error::Result as CrateResult;
use crate::geometry::{CoordSeq, CoordSeqBuilder, CoordWindow, CsrOffsetBuilder, CsrOffsetColumn};

/// One point per output row — direct column push, no `Vec<Point>` staging.
pub(crate) type PointColumnBuilder = CoordSeqBuilder;

pub(crate) struct PackedColumnBuilder {
    xs: Vec<f64>,
    ys: Vec<f64>,
    zs: Option<Vec<f64>>,
    ms: Option<Vec<f64>>,
    offsets: CsrOffsetBuilder,
}

impl PackedColumnBuilder {
    /// Reserve the active ordinate columns and their row offsets together.
    pub(crate) fn like(coords: &CoordSeq, capacity: usize, rows: usize) -> Self {
        Self {
            xs: Vec::with_capacity(capacity),
            ys: Vec::with_capacity(capacity),
            zs: coords.zs().map(|_| Vec::with_capacity(capacity)),
            ms: coords.ms().map(|_| Vec::with_capacity(capacity)),
            offsets: CsrOffsetBuilder::with_capacity(rows),
        }
    }

    fn reserve_columns(&mut self, additional: usize) {
        self.xs.reserve(additional);
        self.ys.reserve(additional);
        if let Some(zs) = &mut self.zs {
            zs.reserve(additional);
        }
        if let Some(ms) = &mut self.ms {
            ms.reserve(additional);
        }
    }

    /// Copy one CSR vertex window verbatim, then close the row.
    pub(crate) fn push_window(
        &mut self,
        coords: &CoordSeq,
        window: Range<usize>,
    ) -> CrateResult<()> {
        self.reserve_columns(window.len());
        let (xs, ys) = (coords.xs(), coords.ys());
        let (zs, ms) = (coords.zs(), coords.ms());
        self.xs.extend_from_slice(column_window(xs, &window));
        self.ys.extend_from_slice(column_window(ys, &window));
        if let (Some(out), Some(column)) = (self.zs.as_mut(), zs) {
            out.extend_from_slice(column_window(column, &window));
        }
        if let (Some(out), Some(column)) = (self.ms.as_mut(), ms) {
            out.extend_from_slice(column_window(column, &window));
        }
        self.offsets.push_end(self.xs.len(), i32::MAX as usize)?;
        Ok(())
    }

    /// Copy one CSR vertex window in reverse, then close the row.
    pub(crate) fn push_reversed_window(
        &mut self,
        coords: &CoordSeq,
        window: Range<usize>,
    ) -> CrateResult<()> {
        self.reserve_columns(window.len());
        let (xs, ys) = (coords.xs(), coords.ys());
        let (zs, ms) = (coords.zs(), coords.ms());
        self.xs
            .extend(column_window(xs, &window).iter().rev().copied());
        self.ys
            .extend(column_window(ys, &window).iter().rev().copied());
        if let (Some(out), Some(column)) = (self.zs.as_mut(), zs) {
            out.extend(column_window(column, &window).iter().rev().copied());
        }
        if let (Some(out), Some(column)) = (self.ms.as_mut(), ms) {
            out.extend(column_window(column, &window).iter().rev().copied());
        }
        self.offsets.push_end(self.xs.len(), i32::MAX as usize)?;
        Ok(())
    }

    /// Subdivide one CSR window through `subdivide`, then close the row.
    pub(crate) fn push_subdivided(
        &mut self,
        coords: &CoordSeq,
        window: Range<usize>,
        subdivide: &impl Fn(&CoordSeq, &mut crate::geometry::ExpansionBudget) -> CrateResult<CoordSeq>,
        budget: &mut crate::geometry::ExpansionBudget,
    ) -> CrateResult<()> {
        let row = coords.view(CoordWindow::from_range(window, coords.len()));
        let result = subdivide(&row, budget)?;
        self.extend_coords(&result);
        self.close_row()
    }

    fn extend_coords(&mut self, transformed: &CoordSeq) {
        self.reserve_columns(transformed.len());
        self.xs.extend_from_slice(transformed.xs());
        self.ys.extend_from_slice(transformed.ys());
        if let (Some(out), Some(column)) = (self.zs.as_mut(), transformed.zs()) {
            out.extend_from_slice(column);
        }
        if let (Some(out), Some(column)) = (self.ms.as_mut(), transformed.ms()) {
            out.extend_from_slice(column);
        }
    }

    /// Exact affine-map one CSR window about `origin` without closing a row.
    pub(crate) fn extend_affine_about_origin(
        &mut self,
        coords: &CoordSeq,
        window: Range<usize>,
        linear: [f64; 4],
        origin: (f64, f64),
    ) -> CrateResult<()> {
        let transformed = coords
            .view(CoordWindow::from_range(window, coords.len()))
            .try_affine_about_origin(linear, origin)?;
        self.extend_coords(&transformed);
        Ok(())
    }

    /// Exact affine-map one CSR window about `origin`, then close the row.
    pub(crate) fn push_affine_about_origin(
        &mut self,
        coords: &CoordSeq,
        window: Range<usize>,
        linear: [f64; 4],
        origin: (f64, f64),
    ) -> CrateResult<()> {
        self.extend_affine_about_origin(coords, window, linear, origin)?;
        self.offsets.push_end(self.xs.len(), i32::MAX as usize)?;
        Ok(())
    }

    /// Append one source vertex by column index (simplify keep-mask path).
    pub(crate) fn push_vertex(&mut self, coords: &CoordSeq, index: usize) {
        self.xs.push(coords.xs()[index]);
        self.ys.push(coords.ys()[index]);
        if let (Some(out), Some(column)) = (self.zs.as_mut(), coords.zs()) {
            out.push(column[index]);
        }
        if let (Some(out), Some(column)) = (self.ms.as_mut(), coords.ms()) {
            out.push(column[index]);
        }
    }

    /// Append a generated XY vertex to an XY-only packed result.
    pub(crate) fn push_xy(&mut self, x: f64, y: f64) {
        if self.xs.len() == self.xs.capacity() {
            self.reserve_columns(1);
        }
        self.xs.push(x);
        self.ys.push(y);
    }

    /// Close the current CSR row after a custom vertex append sequence.
    pub(crate) fn close_row(&mut self) -> CrateResult<()> {
        self.offsets.push_end(self.xs.len(), i32::MAX as usize)?;
        Ok(())
    }

    /// Current vertex count (== xs.len()), for callers that close ring rows by
    /// index.
    pub(crate) const fn vertex_len(&self) -> usize {
        self.xs.len()
    }

    /// Build the `CoordSeq` + the CSR column. `offset_cap` is what the original
    /// `offset_builder.finish(...)` was passed (`out_coords.len()` for
    /// line/ring cols).
    pub(crate) fn finish(self, offset_cap: usize) -> CrateResult<(CoordSeq, CsrOffsetColumn)> {
        let out_coords = CoordSeq::try_from_vecs(self.xs, self.ys, self.zs, self.ms)?;
        let out_offsets = self.offsets.finish(offset_cap)?;
        Ok((out_coords, out_offsets))
    }
}

/// Join packed line columns in one append pass. Pairwise prefix concat copies
/// already-merged vertices again on every extra column.
pub(crate) fn concat_packed_line_columns(
    columns: impl IntoIterator<Item = (CoordSeq, CsrOffsetColumn)>,
) -> CrateResult<(CoordSeq, CsrOffsetColumn)> {
    let mut builder: Option<PackedColumnBuilder> = None;
    for (coords, offsets) in columns {
        let n_lines = offsets.len().saturating_sub(1);
        if n_lines == 0 {
            continue;
        }
        let ends = offsets.as_slice();
        let builder = match &mut builder {
            Some(builder) => {
                builder.reserve_columns(coords.len());
                builder.offsets.reserve_rows(n_lines);
                builder
            },
            None => builder.insert(PackedColumnBuilder::like(&coords, coords.len(), n_lines)),
        };
        for line in 0..n_lines {
            builder.push_window(&coords, ends[line] as usize..ends[line + 1] as usize)?;
        }
    }
    match builder {
        Some(builder) => {
            let vertices = builder.vertex_len();
            builder.finish(vertices)
        },
        None => Ok((
            CoordSeq::try_from_vecs(Vec::new(), Vec::new(), None, None)?,
            CsrOffsetColumn::try_new(vec![0], 0)?,
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::{CoordSeq, CsrOffsetColumn, concat_packed_line_columns};

    #[test]
    fn concat_packed_line_columns_appends_coordinates_in_input_order() {
        let first = CoordSeq::try_from_vecs(vec![0.0, 1.0], vec![0.0, 0.0], None, None).unwrap();
        let first_offsets = CsrOffsetColumn::try_new(vec![0, 2], 2).unwrap();
        let second = CoordSeq::try_from_vecs(
            vec![2.0, 3.0, 3.0, 4.0],
            vec![2.0, 2.0, 2.0, 3.0],
            None,
            None,
        )
        .unwrap();
        let second_offsets = CsrOffsetColumn::try_new(vec![0, 2, 4], 4).unwrap();
        let empty = CoordSeq::try_from_vecs(Vec::new(), Vec::new(), None, None).unwrap();
        let empty_offsets = CsrOffsetColumn::try_new(vec![0], 0).unwrap();
        let (coords, offsets) = concat_packed_line_columns([
            (first, first_offsets),
            (empty, empty_offsets),
            (second, second_offsets),
        ])
        .unwrap();
        assert_eq!(coords.xs(), &[0.0, 1.0, 2.0, 3.0, 3.0, 4.0]);
        assert_eq!(coords.ys(), &[0.0, 0.0, 2.0, 2.0, 2.0, 3.0]);
        assert_eq!(offsets.as_slice(), &[0, 2, 4, 6]);
    }
}
