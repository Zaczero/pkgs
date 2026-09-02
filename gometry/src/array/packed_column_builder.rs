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
    /// Fallible counterpart for public packed-output paths. Every live
    /// ordinate column is reserved before any generated row is appended.
    pub(crate) fn try_like(coords: &CoordSeq, capacity: usize) -> CrateResult<Self> {
        let mut xs = Vec::new();
        crate::boundary::try_reserve_vec(&mut xs, capacity)?;
        let mut ys = Vec::new();
        if let Err(error) = crate::boundary::try_reserve_vec(&mut ys, capacity) {
            drop(xs);
            return Err(error);
        }
        let mut zs = None;
        if coords.zs().is_some() {
            let mut values = Vec::new();
            if let Err(error) = crate::boundary::try_reserve_vec(&mut values, capacity) {
                drop((xs, ys));
                return Err(error);
            }
            zs = Some(values);
        }
        let mut ms = None;
        if coords.ms().is_some() {
            let mut values = Vec::new();
            if let Err(error) = crate::boundary::try_reserve_vec(&mut values, capacity) {
                drop((xs, ys, zs));
                return Err(error);
            }
            ms = Some(values);
        }
        Ok(Self {
            xs,
            ys,
            zs,
            ms,
            offsets: CsrOffsetBuilder::new(),
        })
    }

    fn try_reserve_columns(&mut self, additional: usize) -> CrateResult<()> {
        crate::boundary::try_reserve_vec(&mut self.xs, additional)?;
        if let Err(error) = crate::boundary::try_reserve_vec(&mut self.ys, additional) {
            return Err(error);
        }
        if let Some(zs) = &mut self.zs
            && let Err(error) = crate::boundary::try_reserve_vec(zs, additional)
        {
            return Err(error);
        }
        if let Some(ms) = &mut self.ms
            && let Err(error) = crate::boundary::try_reserve_vec(ms, additional)
        {
            return Err(error);
        }
        Ok(())
    }

    /// Copy one CSR vertex window verbatim, then close the row.
    pub(crate) fn push_window(
        &mut self,
        coords: &CoordSeq,
        window: Range<usize>,
    ) -> CrateResult<()> {
        self.try_reserve_columns(window.len())?;
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
        self.try_reserve_columns(result.len())?;
        self.xs.extend_from_slice(result.xs());
        self.ys.extend_from_slice(result.ys());
        if let (Some(out), Some(column)) = (self.zs.as_mut(), result.zs()) {
            out.extend_from_slice(column);
        }
        if let (Some(out), Some(column)) = (self.ms.as_mut(), result.ms()) {
            out.extend_from_slice(column);
        }
        self.offsets.push_end(self.xs.len(), i32::MAX as usize)?;
        Ok(())
    }

    /// Affine-map one CSR window (origin-at-zero matrix) without closing a row.
    pub(crate) fn extend_affine(
        &mut self,
        coords: &CoordSeq,
        window: Range<usize>,
        matrix: &[f64; 6],
    ) -> CrateResult<()> {
        let transformed = coords
            .view(CoordWindow::from_range(window, coords.len()))
            .try_affine(matrix)?;
        self.try_reserve_columns(transformed.len())?;
        self.xs.extend_from_slice(transformed.xs());
        self.ys.extend_from_slice(transformed.ys());
        if let (Some(out), Some(column)) = (self.zs.as_mut(), transformed.zs()) {
            out.extend_from_slice(column);
        }
        if let (Some(out), Some(column)) = (self.ms.as_mut(), transformed.ms()) {
            out.extend_from_slice(column);
        }
        Ok(())
    }

    /// Affine-map one CSR window (origin-at-zero matrix), then close the row.
    pub(crate) fn push_affine(
        &mut self,
        coords: &CoordSeq,
        window: Range<usize>,
        matrix: &[f64; 6],
    ) -> CrateResult<()> {
        self.extend_affine(coords, window, matrix)?;
        self.offsets.push_end(self.xs.len(), i32::MAX as usize)?;
        Ok(())
    }

    /// Append one source vertex by column index (simplify keep-mask path).
    pub(crate) fn push_vertex(&mut self, coords: &CoordSeq, index: usize) -> CrateResult<()> {
        self.try_reserve_columns(1)?;
        self.xs.push(coords.xs()[index]);
        self.ys.push(coords.ys()[index]);
        if let (Some(out), Some(column)) = (self.zs.as_mut(), coords.zs()) {
            out.push(column[index]);
        }
        if let (Some(out), Some(column)) = (self.ms.as_mut(), coords.ms()) {
            out.push(column[index]);
        }
        Ok(())
    }

    /// Append a generated XY vertex to an XY-only packed result.
    pub(crate) fn push_xy(&mut self, x: f64, y: f64) -> CrateResult<()> {
        self.try_reserve_columns(1)?;
        self.xs.push(x);
        self.ys.push(y);
        Ok(())
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
