use std::ops::Range;

use crate::error::Result as CrateResult;
use crate::geometry::storage::column_window;
use crate::geometry::types::ensure_coordseq_vertex_capacity;
use crate::geometry::{
    CoordSeq, CoordSeqBuilder, CoordWindow, CsrOffsetBuilder, CsrOffsetColumn, GeometryErrorKind,
    storage,
};

/// One point per output row — direct column push, no `Vec<Point>` staging.
pub(crate) type PointColumnBuilder = CoordSeqBuilder;

pub(crate) struct PackedColumnBuilder {
    xs: Vec<f64>,
    ys: Vec<f64>,
    zs: Option<Vec<f64>>,
    ms: Option<Vec<f64>>,
    offsets: CsrOffsetBuilder,
}

fn has_spare_capacity<T>(values: &Vec<T>, additional: usize) -> bool {
    values
        .capacity()
        .checked_sub(values.len())
        .is_some_and(|available| available >= additional)
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

    fn admit_vertices(&self, additional: usize) -> CrateResult<()> {
        let total = self
            .xs
            .len()
            .checked_add(additional)
            .ok_or(GeometryErrorKind::OffsetCapacityExceeded)?;
        ensure_coordseq_vertex_capacity(total)
    }

    /// Copy one CSR vertex window verbatim, then close the row.
    pub(crate) fn push_window(
        &mut self,
        coords: &CoordSeq,
        window: Range<usize>,
    ) -> CrateResult<()> {
        self.admit_vertices(window.len())?;
        self.reserve_columns(window.len());
        self.push_window_admitted(coords, window)
    }

    pub(crate) fn push_window_admitted(
        &mut self,
        coords: &CoordSeq,
        window: Range<usize>,
    ) -> CrateResult<()> {
        debug_assert!(has_spare_capacity(&self.xs, window.len()));
        debug_assert!(has_spare_capacity(&self.ys, window.len()));
        debug_assert!(
            self.zs
                .as_ref()
                .is_none_or(|column| has_spare_capacity(column, window.len()))
        );
        debug_assert!(
            self.ms
                .as_ref()
                .is_none_or(|column| has_spare_capacity(column, window.len()))
        );
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

    /// Copy one admitted CSR vertex window in reverse, then close the row.
    pub(crate) fn push_reversed_window_admitted(
        &mut self,
        coords: &CoordSeq,
        window: Range<usize>,
    ) -> CrateResult<()> {
        debug_assert!(has_spare_capacity(&self.xs, window.len()));
        debug_assert!(has_spare_capacity(&self.ys, window.len()));
        debug_assert!(
            self.zs
                .as_ref()
                .is_none_or(|column| has_spare_capacity(column, window.len()))
        );
        debug_assert!(
            self.ms
                .as_ref()
                .is_none_or(|column| has_spare_capacity(column, window.len()))
        );
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
        self.extend_coords(&result)?;
        self.close_row()
    }

    fn extend_coords(&mut self, transformed: &CoordSeq) -> CrateResult<()> {
        self.admit_vertices(transformed.len())?;
        self.reserve_columns(transformed.len());
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

    /// Exact affine-map one admitted CSR window, then close the row.
    pub(crate) fn push_affine_about_origin_admitted(
        &mut self,
        coords: &CoordSeq,
        window: Range<usize>,
        transform: &crate::geometry::OriginAffine,
    ) -> CrateResult<()> {
        debug_assert!(has_spare_capacity(&self.xs, window.len()));
        debug_assert!(has_spare_capacity(&self.ys, window.len()));
        debug_assert!(
            self.zs
                .as_ref()
                .is_none_or(|column| has_spare_capacity(column, window.len()))
        );
        debug_assert!(
            self.ms
                .as_ref()
                .is_none_or(|column| has_spare_capacity(column, window.len()))
        );
        let previous_len = self.xs.len();
        self.xs.extend(std::iter::repeat_n(0.0, window.len()));
        self.ys.extend(std::iter::repeat_n(0.0, window.len()));
        let output_x = self
            .xs
            .get_mut(previous_len..)
            .expect("appended affine X window exists");
        let output_y = self
            .ys
            .get_mut(previous_len..)
            .expect("appended affine Y window exists");
        if let Err(error) = transform.map_xy_columns(
            column_window(coords.xs(), &window),
            column_window(coords.ys(), &window),
            Some(output_x),
            Some(output_y),
        ) {
            self.xs.truncate(previous_len);
            self.ys.truncate(previous_len);
            return Err(error);
        }
        if let (Some(output), Some(source)) = (self.zs.as_mut(), coords.zs()) {
            output.extend_from_slice(column_window(source, &window));
        }
        if let (Some(output), Some(source)) = (self.ms.as_mut(), coords.ms()) {
            output.extend_from_slice(column_window(source, &window));
        }
        self.offsets.push_end(self.xs.len(), i32::MAX as usize)?;
        Ok(())
    }

    /// Append a row's kept vertices after its caller admits the survivor count.
    pub(crate) fn push_kept_vertices_admitted(
        &mut self,
        coords: &CoordSeq,
        window: Range<usize>,
        keep: &[bool],
        count: usize,
    ) {
        debug_assert_eq!(window.len(), keep.len());
        debug_assert_eq!(keep.iter().filter(|&&kept| kept).count(), count);
        debug_assert!(has_spare_capacity(&self.xs, count));
        debug_assert!(has_spare_capacity(&self.ys, count));
        let (xs, ys, zs, ms) = (coords.xs(), coords.ys(), coords.zs(), coords.ms());
        let mut zs = zs.map(|column| column_window(column, &window).iter());
        let mut ms = ms.map(|column| column_window(column, &window).iter());
        for ((&kept, x), y) in keep
            .iter()
            .zip(column_window(xs, &window))
            .zip(column_window(ys, &window))
        {
            let z = zs.as_mut().and_then(|column| column.next());
            let m = ms.as_mut().and_then(|column| column.next());
            if kept {
                self.xs.push(*x);
                self.ys.push(*y);
                if let Some(out) = &mut self.zs {
                    out.push(*z.expect("Z columns share the admitted coordinate window"));
                }
                if let Some(out) = &mut self.ms {
                    out.push(*m.expect("M columns share the admitted coordinate window"));
                }
            }
        }
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
    columns: Vec<(CoordSeq, CsrOffsetColumn)>,
) -> CrateResult<(CoordSeq, CsrOffsetColumn)> {
    let mut vertices = 0_usize;
    let mut rows = 0_usize;
    for (_, offsets) in &columns {
        let n_lines = offsets.len().saturating_sub(1);
        if n_lines == 0 {
            continue;
        }
        vertices = storage::admit_packed_total(
            vertices,
            storage::csr_value_window(offsets.as_slice(), 0..n_lines).len(),
        )?;
        rows = rows
            .checked_add(n_lines)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
    }
    let Some((first_coords, _)) = columns.iter().find(|(_, offsets)| offsets.len() > 1) else {
        return Ok((
            CoordSeq::try_from_vecs(Vec::new(), Vec::new(), None, None)?,
            CsrOffsetColumn::try_new(vec![0], 0)?,
        ));
    };
    let mut builder = PackedColumnBuilder::like(first_coords, vertices, rows);
    for (coords, offsets) in columns {
        let n_lines = offsets.len().saturating_sub(1);
        if n_lines == 0 {
            continue;
        }
        for ends in offsets.as_slice().windows(2) {
            let [start, end] = ends else {
                unreachable!("CSR windows have exactly two boundaries");
            };
            builder.push_window_admitted(&coords, *start as usize..*end as usize)?;
        }
    }
    builder.finish(vertices)
}

#[cfg(test)]
mod tests {
    use super::{CoordSeq, CsrOffsetColumn, PackedColumnBuilder, concat_packed_line_columns};

    #[test]
    fn affine_error_preserves_completed_rows_and_all_ordinate_columns() {
        let coords = CoordSeq::try_from_vecs(
            vec![1.0, 2.0, f64::MAX],
            vec![3.0, 4.0, 5.0],
            Some(vec![6.0, 7.0, 8.0]),
            Some(vec![9.0, 10.0, 11.0]),
        )
        .unwrap();
        let mut builder = PackedColumnBuilder::like(&coords, 4, 2);
        builder.push_window(&coords, 0..2).unwrap();
        let transform = crate::geometry::AffineLinear::new([2.0, 0.0, 0.0, 1.0])
            .unwrap()
            .at((0.0, 0.0))
            .unwrap();
        assert!(
            builder
                .push_affine_about_origin_admitted(&coords, 1..3, &transform)
                .is_err()
        );
        let (output, offsets) = builder.finish(2).unwrap();
        assert_eq!(output.xs(), &[1.0, 2.0]);
        assert_eq!(output.ys(), &[3.0, 4.0]);
        assert_eq!(output.zs().unwrap(), &[6.0, 7.0]);
        assert_eq!(output.ms().unwrap(), &[9.0, 10.0]);
        assert_eq!(offsets.as_slice(), &[0, 2]);
    }

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
        let (coords, offsets) = concat_packed_line_columns(vec![
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
