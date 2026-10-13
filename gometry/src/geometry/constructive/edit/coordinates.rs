use super::{
    CoordSeq, CoordinateAxes, GeometryErrorKind, HasM, HasZ, OrdinateAxis, OriginAffine,
    ReduceSimd, Result, Shape, affine_ordinate_simd, affine_x_row_identity, affine_y_row_identity,
    column_all_finite, decimal_scale, quantize_column_simd, set_point_ordinate, simd_xy_map_f64,
    simd_xy_map_one_f64, snap_column_simd, stable_affine_xy,
};

impl CoordSeq {
    /// `op` over every `(x, y)` pair, Z/M columns carried through untouched —
    /// the columnar form of a per-point XY map (no `Point` gather, no axes
    /// re-derivation, one pass per column).
    #[expect(
        clippy::impl_trait_in_params,
        reason = "the one-pass coordinate callback is deliberately existential"
    )]
    #[cfg(test)]
    pub fn map_xy(&self, mut op: impl FnMut(f64, f64) -> (f64, f64)) -> Result<Self> {
        // Pre-sized output columns written by index (no push branch), so the
        // per-pair arithmetic vectorizes.
        let mut xs = vec![0.0; self.len()];
        let mut ys = vec![0.0; self.len()];
        for (((out_x, out_y), &x), &y) in xs
            .iter_mut()
            .zip(ys.iter_mut())
            .zip(self.xs())
            .zip(self.ys())
        {
            (*out_x, *out_y) = op(x, y);
        }
        Self::from_shared_columns(
            crate::geometry::SharedSlice::from_vec(xs),
            crate::geometry::SharedSlice::from_vec(ys),
            self.carried_zs(),
            self.carried_ms(),
        )
    }

    /// Affine transform of the XY columns — `x' = a*x + b*y + xoff`,
    /// `y' = d*x + e*y + yoff` — 8-wide explicit SIMD (Z/M carried). The
    /// closure-based [`map_xy`](Self::map_xy) compiles to a scalar `mulsd`
    /// loop (the `FnMut` indirection blocks the vectorizer); this packs the
    /// pure multiply-add into `f64x8` in the SAME left-to-right op order as the
    /// scalar form (`(a*x + b*y) + xoff`, no FMA contraction), so it is
    /// bit-identical. Finiteness validated once over the produced columns.
    pub fn try_affine(&self, matrix: &[f64; 6]) -> Result<Self> {
        let [a, b, d, e, xoff, yoff] = *matrix;
        let x_identity = affine_x_row_identity(a, b, xoff);
        let y_identity = affine_y_row_identity(d, e, yoff);
        if x_identity && y_identity {
            return Ok(self.clone());
        }
        let n = self.len();
        let (xs, ys) = (self.xs(), self.ys());
        let mut out_x = (!x_identity).then(|| vec![0.0; n]);
        let mut out_y = (!y_identity).then(|| vec![0.0; n]);
        if let (Some(out_x), Some(out_y)) = (out_x.as_mut(), out_y.as_mut()) {
            let (va, vb, vd, ve, vxo, vyo) = (
                ReduceSimd::splat(a),
                ReduceSimd::splat(b),
                ReduceSimd::splat(d),
                ReduceSimd::splat(e),
                ReduceSimd::splat(xoff),
                ReduceSimd::splat(yoff),
            );
            simd_xy_map_f64(
                xs,
                ys,
                out_x,
                out_y,
                |x, y| stable_affine_xy(a, b, d, e, xoff, yoff, x, y),
                |x, y| {
                    (
                        affine_ordinate_simd(va, vb, vxo, x, y),
                        affine_ordinate_simd(vd, ve, vyo, x, y),
                    )
                },
            );
        } else if let Some(out_x) = out_x.as_mut() {
            simd_xy_map_one_f64(
                xs,
                ys,
                out_x,
                |x, y| stable_affine_xy(a, b, d, e, xoff, yoff, x, y).0,
                |x, y| {
                    affine_ordinate_simd(
                        ReduceSimd::splat(a),
                        ReduceSimd::splat(b),
                        ReduceSimd::splat(xoff),
                        x,
                        y,
                    )
                },
            );
        } else if let Some(out_y) = out_y.as_mut() {
            simd_xy_map_one_f64(
                xs,
                ys,
                out_y,
                |x, y| stable_affine_xy(a, b, d, e, xoff, yoff, x, y).1,
                |x, y| {
                    affine_ordinate_simd(
                        ReduceSimd::splat(d),
                        ReduceSimd::splat(e),
                        ReduceSimd::splat(yoff),
                        x,
                        y,
                    )
                },
            );
        }
        // SIMD fast path may still overflow extreme operands; rescue any
        // non-finite lane with the scalar stable form (ordinary mid-range
        // stays bit-identical on the SIMD path when all finite).
        if let Some(out_x) = out_x.as_mut() {
            if let Some(out_y) = out_y.as_mut() {
                for (((&x, &y), x_out), y_out) in xs
                    .iter()
                    .zip(ys)
                    .zip(out_x.iter_mut())
                    .zip(out_y.iter_mut())
                {
                    if !x_out.is_finite() || !y_out.is_finite() {
                        let (sx, sy) = stable_affine_xy(a, b, d, e, xoff, yoff, x, y);
                        *x_out = sx;
                        *y_out = sy;
                    }
                }
            } else {
                for ((&x, &y), x_out) in xs.iter().zip(ys).zip(out_x.iter_mut()) {
                    if !x_out.is_finite() {
                        let (sx, _) = stable_affine_xy(a, b, d, e, xoff, yoff, x, y);
                        *x_out = sx;
                    }
                }
            }
        } else if let Some(out_y) = out_y.as_mut() {
            for ((&x, &y), y_out) in xs.iter().zip(ys).zip(out_y.iter_mut()) {
                if !y_out.is_finite() {
                    let (_, sy) = stable_affine_xy(a, b, d, e, xoff, yoff, x, y);
                    *y_out = sy;
                }
            }
        }
        let x_finite = out_x.as_deref().is_none_or(column_all_finite);
        let y_finite = out_y.as_deref().is_none_or(column_all_finite);
        if !x_finite || !y_finite {
            Err(GeometryErrorKind::NonFiniteCoordinate.into())
        } else {
            Self::from_shared_columns(
                out_x.map_or_else(|| self.carried_xs(), crate::geometry::SharedSlice::from_vec),
                out_y.map_or_else(|| self.carried_ys(), crate::geometry::SharedSlice::from_vec),
                self.carried_zs(),
                self.carried_ms(),
            )
        }
    }

    /// Exact origin-relative affine transform of the XY columns. Identity
    /// rows carry their source column unchanged; other rows use a certified
    /// expansion and fall back to [`ExactSum`] when rounding is uncertain.
    pub(crate) fn try_affine_about_origin(&self, transform: &OriginAffine) -> Result<Self> {
        if transform.preserved == [true, true] {
            return Ok(self.clone());
        }
        let [x_identity, y_identity] = transform.preserved;
        let n = self.len();
        let (xs, ys) = (self.xs(), self.ys());
        let mut out_x = (!x_identity).then(|| vec![0.0; n]);
        let mut out_y = (!y_identity).then(|| vec![0.0; n]);
        transform.map_xy_columns(xs, ys, out_x.as_deref_mut(), out_y.as_deref_mut())?;
        Self::from_shared_columns(
            out_x.map_or_else(|| self.carried_xs(), crate::geometry::SharedSlice::from_vec),
            out_y.map_or_else(|| self.carried_ys(), crate::geometry::SharedSlice::from_vec),
            self.carried_zs(),
            self.carried_ms(),
        )
    }

    /// Snap the X/Y columns onto the grid `origin + k * size` (per-axis), Z/M
    /// carried — the SIMD column kernel behind `snap_to_grid` (the
    /// closure-based `map_xy_with` it replaced compiled to a scalar
    /// `divsd`/`roundsd` loop).
    pub fn try_snap_to_grid(
        &self,
        size: (f64, f64),
        origin: (f64, f64),
    ) -> Result<Self, crate::error::Error> {
        Self::from_shared_columns(
            crate::geometry::SharedSlice::from_vec(
                snap_column_simd(self.xs(), origin.0, size.0)?.into_vec(),
            ),
            crate::geometry::SharedSlice::from_vec(
                snap_column_simd(self.ys(), origin.1, size.1)?.into_vec(),
            ),
            self.carried_zs(),
            self.carried_ms(),
        )
    }

    /// Fallible [`map_xy`](Self::map_xy) generic over the error: per-pair
    /// `op` failures (e.g. projection domain errors) propagate immediately,
    /// Z/M columns carry through untouched.
    #[expect(
        clippy::impl_trait_in_params,
        reason = "the one-pass coordinate callback is deliberately existential"
    )]
    pub fn try_map_xy_with<E>(
        &self,
        mut op: impl FnMut(f64, f64) -> Result<(f64, f64), E>,
    ) -> Result<Self, E>
    where
        E: From<crate::error::Error>,
    {
        let mut xs = Vec::with_capacity(self.len());
        let mut ys = Vec::with_capacity(self.len());
        for (&x, &y) in std::iter::zip(self.xs(), self.ys()) {
            let (x, y) = op(x, y)?;
            xs.push(x);
            ys.push(y);
        }
        Self::from_shared_columns(
            crate::geometry::SharedSlice::from_vec(xs),
            crate::geometry::SharedSlice::from_vec(ys),
            self.carried_zs(),
            self.carried_ms(),
        )
        .map_err(E::from)
    }

    /// Every ordinate column quantized to `precision` decimal places.
    pub fn quantize(&self, precision: i32) -> Result<Self> {
        let scale = decimal_scale(precision);
        let map = |column: &[f64]| -> Vec<f64> { quantize_column_simd(column, scale).into_vec() };
        Self::try_from_vecs(
            map(self.xs()),
            map(self.ys()),
            self.zs().map(map),
            self.ms().map(map),
        )
    }

    /// The XY columns alone, dropping Z/M. Shares the backing `Arc`s with zero
    /// copy when this sequence spans its whole columns (the common case).
    pub fn force_2d(&self) -> Result<Self> {
        Self::from_shared_columns(self.carried_xs(), self.carried_ys(), None, None)
    }

    /// Set or clear the Z ordinate column. `value=None` removes Z (M is
    /// preserved); `Some(z)` with `overwrite` writes `z` at every vertex,
    /// otherwise fills only when the column is absent (existing Z is kept).
    pub fn replace_z(&self, value: Option<f64>, overwrite: bool) -> Result<Self> {
        self.set_ordinate(OrdinateAxis::Z, value, overwrite)
    }

    /// Set or clear the M ordinate column. `value=None` removes M (Z is
    /// preserved); `Some(m)` with `overwrite` writes `m` at every vertex,
    /// otherwise fills only when the column is absent (existing M is kept).
    pub fn replace_m(&self, value: Option<f64>, overwrite: bool) -> Result<Self> {
        self.set_ordinate(OrdinateAxis::M, value, overwrite)
    }

    /// The columnar engine behind [`Shape::replace_z`]/[`Shape::replace_m`]: one
    /// optional ordinate column is replaced or dropped, the other passes
    /// through. Homogeneous packed storage shares uniform axes per column,
    /// so fill-only (`overwrite=False` on an existing column) is an
    /// identity without a per-vertex scan.
    fn set_ordinate(
        &self,
        axis: OrdinateAxis,
        value: Option<f64>,
        overwrite: bool,
    ) -> Result<Self> {
        let len = self.len();
        let (zs, ms) = (self.zs(), self.ms());
        let column = match axis {
            OrdinateAxis::Z => zs,
            OrdinateAxis::M => ms,
        };
        if value.is_none() && column.is_none() || value.is_some() && !overwrite && column.is_some()
        {
            return Ok(self.clone());
        }
        let fill = |value: f64| -> Vec<f64> { vec![value; len] };
        // The set axis allocates exactly one fresh column; every untouched
        // column (X, Y, and the OTHER optional ordinate) is Arc-carried, not
        // re-copied — zero allocation on the full-window common case.
        // An empty sequence gets a zero-length column: axes are real state
        // on empties, so `replace_z`/`force_3d` flip has_z/has_m even with no
        // vertices (`MULTIPOINT EMPTY` → `MULTIPOINT Z EMPTY`).
        let ordinate_column = |value: Option<f64>| {
            value.map(|value| crate::geometry::SharedSlice::from_vec(fill(value)))
        };
        let (out_zs, out_ms) = match axis {
            OrdinateAxis::Z => (ordinate_column(value), self.carried_ms()),
            OrdinateAxis::M => (
                self.carried_zs(),
                ordinate_column(value).map(crate::geometry::MColumn::Shared),
            ),
        };
        Self::from_shared_columns(self.carried_xs(), self.carried_ys(), out_zs, out_ms)
    }
}

impl Shape {
    pub fn quantize(&self, precision: i32) -> Result<Self> {
        Ok(match self {
            Self::Point(point) => Self::Point(point.quantize(precision)),
            Self::MultiPoint(points) => Self::MultiPoint(points.quantize(precision)?),
            Self::LineString(points) => Self::LineString(points.quantize(precision)?),
            Self::MultiLineString(lines) => Self::MultiLineString(
                lines.try_map_values(lines.axes(), |line| line.quantize(precision))?,
            ),
            Self::Polygon(polygon) => Self::Polygon(polygon.quantize(precision)?),
            Self::MultiPolygon(polygons) => Self::MultiPolygon(
                polygons.try_map_values(polygons.axes(), |polygon| polygon.quantize(precision))?,
            ),
            Self::GeometryCollection(geometries) => Self::GeometryCollection(
                geometries.try_map_members(|geometry| geometry.quantize(precision))?,
            ),
            Self::Empty(..) => self.clone(),
        })
    }

    /// Set or clear the Z ordinate. `value=None` removes Z (M is preserved);
    /// `Some(z)` with `overwrite` writes `z` at every vertex, otherwise fills
    /// only vertices that lack Z (existing Z is kept). M passes through.
    pub fn replace_z(&self, value: Option<f64>, overwrite: bool) -> Result<Self> {
        self.set_ordinate(OrdinateAxis::Z, value, overwrite)
    }

    /// Set or clear the M ordinate. `value=None` removes M (Z is preserved);
    /// `Some(m)` with `overwrite` writes `m` at every vertex, otherwise fills
    /// only vertices that lack M (existing M is kept). Z passes through.
    pub fn replace_m(&self, value: Option<f64>, overwrite: bool) -> Result<Self> {
        self.set_ordinate(OrdinateAxis::M, value, overwrite)
    }

    /// The same-kind empty retagged to `axes`, or `None` when `self` is not a
    /// canonical empty representation. The ordinate verbs route empties here
    /// so declared axes are real state on emptiness (`replace_z` on
    /// `MULTIPOLYGON EMPTY` yields `MULTIPOLYGON Z EMPTY`). A non-canonical
    /// empty (e.g. a collection OF empties) is `None` — its members retag
    /// individually through the recursive verb.
    const fn retag_empty_axes(&self, axes: CoordinateAxes) -> Option<Self> {
        match self {
            Self::Empty(kind, _) => Some(Self::typed_empty(*kind, axes)),
            Self::Point(_)
            | Self::MultiPoint(_)
            | Self::LineString(_)
            | Self::MultiLineString(_)
            | Self::Polygon(_)
            | Self::MultiPolygon(_)
            | Self::GeometryCollection(_) => None,
        }
    }

    /// The declared axes after setting/clearing one ordinate axis.
    fn axes_with_ordinate(&self, axis: OrdinateAxis, present: bool) -> CoordinateAxes {
        let axes = self.axes();
        match axis {
            OrdinateAxis::Z => CoordinateAxes::new(HasZ(present), HasM(axes.has_m())),
            OrdinateAxis::M => CoordinateAxes::new(HasZ(axes.has_z()), HasM(present)),
        }
    }

    /// The single engine behind [`Shape::replace_z`]/[`Shape::replace_m`]: per-vertex
    /// replacement of one optional ordinate, leaving the other axis untouched.
    /// Empties retag their declared axes (no vertices to fill, but the
    /// dimensionality is real state); collections recurse so nested empties
    /// retag too.
    fn set_ordinate(
        &self,
        axis: OrdinateAxis,
        value: Option<f64>,
        overwrite: bool,
    ) -> Result<Self> {
        if let Some(empty) = self.retag_empty_axes(self.axes_with_ordinate(axis, value.is_some())) {
            return Ok(empty);
        }
        let axes = self.axes_with_ordinate(axis, value.is_some());
        if let Self::MultiPoint(points) = self
            && points.is_empty()
        {
            return Ok(Self::MultiPoint(points.with_axes(axes)));
        }
        if let Self::MultiLineString(lines) = self
            && lines.is_empty()
        {
            return Ok(Self::MultiLineString(lines.with_axes(axes)));
        }
        if let Self::MultiPolygon(polygons) = self
            && polygons.is_empty()
        {
            return Ok(Self::MultiPolygon(polygons.with_axes(axes)));
        }
        if let Self::GeometryCollection(geometries) = self {
            return Ok(Self::GeometryCollection(geometries.try_map_members(
                |geometry| geometry.set_ordinate(axis, value, overwrite),
            )?));
        }
        self.try_map_coordseqs(
            |seq| seq.set_ordinate(axis, value, overwrite),
            |point| Ok(set_point_ordinate(*point, axis, value, overwrite)),
        )
    }

    pub fn force_2d(&self) -> Result<Self> {
        // Already-2D input shares its storage: has_z/has_m are O(parts)
        // column flags, so the defensive force_2d-on-2D pattern is free.
        if !self.has_z() && !self.has_m() {
            return Ok(self.clone());
        }
        Ok(match self {
            Self::Point(point) => Self::Point(point.force_2d()),
            Self::MultiPoint(points) => Self::MultiPoint(points.force_2d()?),
            Self::LineString(points) => Self::LineString(points.force_2d()?),
            Self::MultiLineString(lines) => Self::MultiLineString(
                lines.try_map_values(CoordinateAxes::XY, crate::geometry::LineSeq::force_2d)?,
            ),
            Self::Polygon(polygon) => Self::Polygon(polygon.force_2d()?),
            Self::MultiPolygon(polygons) => Self::MultiPolygon(
                polygons.try_map_values(CoordinateAxes::XY, crate::geometry::Polygon::force_2d)?,
            ),
            Self::GeometryCollection(geometries) => {
                Self::GeometryCollection(geometries.try_map_members(Self::force_2d)?)
            },
            // A dimensional empty flattens to the canonical XY empty of its
            // kind (`POINT Z EMPTY` → `POINT EMPTY`).
            Self::Empty(kind, _) => Self::typed_empty(*kind, CoordinateAxes::XY),
        })
    }
}
