use super::*;

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
    #[expect(
        clippy::indexing_slicing,
        reason = "the gather caller admits this row; direct lane reads preserve column-wise publication and error order"
    )]
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
    #[expect(
        clippy::indexing_slicing,
        reason = "the caller admits this contiguous window against the source's parallel coordinate columns"
    )]
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
