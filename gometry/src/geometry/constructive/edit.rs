#![expect(
    clippy::similar_names,
    reason = "axis and formula symbols name paired quantities here (minx/miny, xs/ys, lat_rad/lam_rad, step/stop), which this lint reads as typos"
)]
use crate::NonNegative;
use crate::geometry::constructive::{
    Result, SegmentPlacement, decimal_scale, normalized_line, quantize_column_simd,
    remove_repeated_line_points, remove_repeated_points, remove_repeated_ring_points,
    segmentize_points_budgeted, snap_column_simd, split,
};
use crate::geometry::exact_arith::product_sum::ExactSum;
use crate::geometry::overlay::{OverlayOp, overlay_all_geographic_from, overlay_view_error};
use crate::geometry::{
    Bounds, CoordSeq, CoordinateAxes, ExpansionBudget, GeometryErrorKind, GeometryKind, HasM, HasZ,
    LineSeq, MOrdinate, MultiLineString, MultiPoint, MultiPolygon, OrdinateAxis, Point, Polygon,
    ReduceSimd, Shape, Strictness, ZOrdinate, axis_pow2_scale, carry_ordinates,
    clip_multipoint_columns, clip_polygonal, clipped_line_parts, clipped_linestring,
    column_all_finite, compare_point_slices, compare_points, compare_polygons, compare_shapes,
    line_parts_to_shape, same_topological_coordinate, set_point_ordinate, simd_xy_map_f64,
    simd_xy_map_one_f64,
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
    #[expect(
        clippy::many_single_char_names,
        reason = "a,b,d,e are the canonical affine matrix coefficients"
    )]
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
                |x, y| (va * x + vb * y + vxo, vd * x + ve * y + vyo),
            );
        } else if let Some(out_x) = out_x.as_mut() {
            simd_xy_map_one_f64(
                xs,
                ys,
                out_x,
                |x, y| stable_affine_xy(a, b, d, e, xoff, yoff, x, y).0,
                |x, y| {
                    ReduceSimd::splat(a) * x + ReduceSimd::splat(b) * y + ReduceSimd::splat(xoff)
                },
            );
        } else if let Some(out_y) = out_y.as_mut() {
            simd_xy_map_one_f64(
                xs,
                ys,
                out_y,
                |x, y| stable_affine_xy(a, b, d, e, xoff, yoff, x, y).1,
                |x, y| {
                    ReduceSimd::splat(d) * x + ReduceSimd::splat(e) * y + ReduceSimd::splat(yoff)
                },
            );
        }
        // SIMD fast path may still overflow extreme operands; rescue any
        // non-finite lane with the scalar stable form (ordinary mid-range
        // stays bit-identical on the SIMD path when all finite).
        if let Some(out_x) = out_x.as_mut() {
            for (index, slot) in out_x.iter_mut().enumerate() {
                let y_out = out_y.as_ref().map_or(ys[index], |col| col[index]);
                if !slot.is_finite() || !y_out.is_finite() {
                    let (sx, sy) = stable_affine_xy(a, b, d, e, xoff, yoff, xs[index], ys[index]);
                    *slot = sx;
                    if let Some(out_y) = out_y.as_mut() {
                        out_y[index] = sy;
                    }
                }
            }
        } else if let Some(out_y) = out_y.as_mut() {
            for (index, slot) in out_y.iter_mut().enumerate() {
                if !slot.is_finite() {
                    let (_, sy) = stable_affine_xy(a, b, d, e, xoff, yoff, xs[index], ys[index]);
                    *slot = sy;
                }
            }
        }
        let x_finite = out_x.as_deref().is_none_or(column_all_finite);
        let y_finite = out_y.as_deref().is_none_or(column_all_finite);
        if !x_finite || !y_finite {
            Err(GeometryErrorKind::NonFiniteCoordinate.into())
        } else {
            Self::from_shared_columns(
                match out_x {
                    Some(values) => crate::geometry::SharedSlice::from_vec(values),
                    None => self.carried_xs(),
                },
                match out_y {
                    Some(values) => crate::geometry::SharedSlice::from_vec(values),
                    None => self.carried_ys(),
                },
                self.carried_zs(),
                self.carried_ms(),
            )
        }
    }

    /// Exact origin-relative affine transform of the XY columns. Identity
    /// rows carry their source column unchanged; every other row is rounded
    /// from its exact five-product expression in [`ExactSum`].
    #[expect(
        clippy::many_single_char_names,
        reason = "a, b, d, e are the stored affine matrix entries"
    )]
    #[expect(
        clippy::float_cmp,
        reason = "identity rows are exact unit coefficients; a tolerance would skip the exact five-product transform"
    )]
    pub(crate) fn try_affine_about_origin(
        &self,
        linear: [f64; 4],
        origin: (f64, f64),
    ) -> Result<Self> {
        let [a, b, d, e] = linear;
        if !linear.iter().all(|value| value.is_finite())
            || !origin.0.is_finite()
            || !origin.1.is_finite()
        {
            return Err(GeometryErrorKind::NonFiniteCoordinate.into());
        }
        let x_identity = a == 1.0 && same_topological_coordinate(b, 0.0);
        let y_identity = same_topological_coordinate(d, 0.0) && e == 1.0;
        if x_identity && y_identity {
            return Ok(self.clone());
        }
        let n = self.len();
        let (xs, ys) = (self.xs(), self.ys());
        let mut out_x = (!x_identity).then(|| vec![0.0; n]);
        let mut out_y = (!y_identity).then(|| vec![0.0; n]);
        for index in 0..n {
            if let Some(out_x) = out_x.as_mut() {
                out_x[index] = affine_about_origin_ordinate(
                    a, b, origin.0, origin.0, origin.1, xs[index], ys[index],
                )?;
            }
            if let Some(out_y) = out_y.as_mut() {
                out_y[index] = affine_about_origin_ordinate(
                    d, e, origin.1, origin.0, origin.1, xs[index], ys[index],
                )?;
            }
        }
        Self::from_shared_columns(
            match out_x {
                Some(values) => crate::geometry::SharedSlice::from_vec(values),
                None => self.carried_xs(),
            },
            match out_y {
                Some(values) => crate::geometry::SharedSlice::from_vec(values),
                None => self.carried_ys(),
            },
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
    pub fn set_z(&self, value: Option<f64>, overwrite: bool) -> Result<Self> {
        self.set_ordinate(OrdinateAxis::Z, value, overwrite)
    }

    /// Set or clear the M ordinate column. `value=None` removes M (Z is
    /// preserved); `Some(m)` with `overwrite` writes `m` at every vertex,
    /// otherwise fills only when the column is absent (existing M is kept).
    pub fn set_m(&self, value: Option<f64>, overwrite: bool) -> Result<Self> {
        self.set_ordinate(OrdinateAxis::M, value, overwrite)
    }

    /// The columnar engine behind [`Shape::set_z`]/[`Shape::set_m`]: one
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
        // on empties, so `set_z`/`force_3d` flip has_z/has_m even with no
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

/// Length-preserving lifts of the [`CoordSeq`] editors above. Each rebuilds
/// every column with the same vertex count, so a `LineSeq`'s "at least two
/// vertices" invariant carries through and no call site re-checks it. A
/// transform that can drop vertices is not here — it goes through
/// [`LineSeq::try_new`].
impl LineSeq {
    pub(crate) fn try_map_xy_with<E>(
        &self,
        op: impl FnMut(f64, f64) -> Result<(f64, f64), E>,
    ) -> Result<Self, E>
    where
        E: From<crate::error::Error>,
    {
        self.as_coords().try_map_xy_with(op).map(Self::from_proven)
    }

    pub(crate) fn quantize(&self, precision: i32) -> Result<Self> {
        Ok(Self::from_proven(self.as_coords().quantize(precision)?))
    }

    pub(crate) fn force_2d(&self) -> Result<Self> {
        Ok(Self::from_proven(self.as_coords().force_2d()?))
    }

    pub(crate) fn reversed(&self) -> Result<Self> {
        Ok(Self::from_proven(self.as_coords().reversed()?))
    }

    /// The canonical presentation of this line (see
    /// [`crate::geometry::constructive::ring::normalized_line`]) — a rotation
    /// and/or reversal, so the vertex count is unchanged.
    pub(crate) fn normalized(&self) -> Result<Self> {
        Ok(Self::from_proven(normalized_line(self.as_coords())?))
    }

    /// Repeated-vertex removal. The underlying pass keeps the first and last
    /// vertex when a line would otherwise collapse to a point, so a line stays
    /// a line.
    pub(crate) fn remove_repeated_points(&self, tolerance: f64) -> Result<Self> {
        Ok(Self::from_proven(remove_repeated_line_points(
            self, tolerance,
        )?))
    }

    /// Densification and segmentization only insert vertices between existing
    /// ones, so the result is at least as long as the input.
    pub(crate) fn densify(
        &self,
        fraction: f64,
        placement: SegmentPlacement<'_>,
        budget: &mut ExpansionBudget,
    ) -> Result<Self> {
        crate::geometry::densify_points_budgeted_with_placement(self, fraction, placement, budget)
            .map(Self::from_proven)
    }

    pub(crate) fn segmentize(
        &self,
        max_segment_length: f64,
        placement: SegmentPlacement<'_>,
        budget: &mut ExpansionBudget,
    ) -> Result<Self> {
        segmentize_points_budgeted(self, max_segment_length, placement, budget)
            .map(Self::from_proven)
    }
}

impl Shape {
    /// Fallible columnar XY transform generic over the error — the in-core
    /// CRS projections' engine (domain errors propagate per pair; Z/M carry
    /// through untouched, matching the per-point `with_xy` semantics).
    #[expect(
        clippy::impl_trait_in_params,
        reason = "the one-pass coordinate callback is deliberately existential"
    )]
    pub fn map_xy_with<E>(
        &self,
        op: impl Fn(f64, f64) -> Result<(f64, f64), E> + Copy,
    ) -> Result<Self, E>
    where
        E: From<crate::error::Error>,
    {
        Ok(match self {
            Self::Point(point) => {
                let (x, y) = op(point.x, point.y)?;
                Self::Point(point.with_xy_raw(x, y))
            },
            Self::MultiPoint(points) => Self::MultiPoint(points.try_map_xy_with(op)?),
            Self::LineString(points) => Self::LineString(points.try_map_xy_with(op)?),
            Self::MultiLineString(lines) => Self::MultiLineString(
                lines.try_map_values(lines.axes(), |line| line.try_map_xy_with(op))?,
            ),
            Self::Polygon(polygon) => Self::Polygon(polygon.map_rings_xy_with(op)?),
            Self::MultiPolygon(polygons) => Self::MultiPolygon(
                polygons
                    .try_map_values(polygons.axes(), |polygon| polygon.map_rings_xy_with(op))?,
            ),
            Self::GeometryCollection(geometries) => Self::GeometryCollection(
                geometries.try_map_members(|geometry| geometry.map_xy_with(op))?,
            ),
            Self::Empty(..) => self.clone(),
        })
    }

    /// Columnar XY transform over every coordinate sequence of the shape
    /// (Z/M untouched), validating finiteness once per column. This is the
    /// matrix-form affine path; origin-relative transforms use their exact
    /// pivot evaluator below.
    #[expect(
        clippy::many_single_char_names,
        reason = "a,b,d,e are the canonical affine matrix coefficients"
    )]
    fn affine(&self, matrix: &[f64; 6]) -> Result<Self> {
        let [a, b, d, e, xoff, yoff] = *matrix;
        self.try_map_coordseqs(
            |seq| seq.try_affine(matrix),
            |point| {
                let (x, y) =
                    if affine_x_row_identity(a, b, xoff) && affine_y_row_identity(d, e, yoff) {
                        (point.x, point.y)
                    } else {
                        stable_affine_xy(a, b, d, e, xoff, yoff, point.x, point.y)
                    };
                point.with_xy(x, y)
            },
        )
    }
}

#[expect(
    clippy::float_cmp,
    reason = "affine identity rows are exact coefficients"
)]
pub(crate) fn affine_x_row_identity(a: f64, b: f64, xoff: f64) -> bool {
    a == 1.0 && same_topological_coordinate(b, 0.0) && same_topological_coordinate(xoff, 0.0)
}

#[expect(
    clippy::float_cmp,
    reason = "affine identity rows are exact coefficients"
)]
pub(crate) fn affine_y_row_identity(d: f64, e: f64, yoff: f64) -> bool {
    same_topological_coordinate(d, 0.0) && e == 1.0 && same_topological_coordinate(yoff, 0.0)
}

/// Round one non-identity origin-relative affine row from the exact stored
/// binary64 expression. Identity rows are handled by callers so signed zero
/// and unchanged coordinates remain bit-identical.
pub(crate) fn affine_about_origin_ordinate(
    coefficient_x: f64,
    coefficient_y: f64,
    output_origin: f64,
    origin_x: f64,
    origin_y: f64,
    value_x: f64,
    value_y: f64,
) -> Result<f64> {
    if !coefficient_x.is_finite()
        || !coefficient_y.is_finite()
        || !output_origin.is_finite()
        || !origin_x.is_finite()
        || !origin_y.is_finite()
        || !value_x.is_finite()
        || !value_y.is_finite()
    {
        return Err(GeometryErrorKind::NonFiniteCoordinate.into());
    }
    let mut sum = ExactSum::zero();
    sum.add_product(coefficient_x, value_x);
    sum.add_product(coefficient_y, value_y);
    sum.add_product(output_origin, 1.0);
    sum.subtract_product(coefficient_x, origin_x);
    sum.subtract_product(coefficient_y, origin_y);
    let value = sum.round_to_binary64();
    value
        .is_finite()
        .then_some(value)
        .ok_or_else(|| GeometryErrorKind::NonFiniteCoordinate.into())
}

/// Affine map with power-of-two pre-scale of the input coordinates so
/// cancelling huge products (`2*1e308 + (-2)*1e308`) stay finite. Ordinary
/// mid-range inputs take the classic multiply-add (bit-identical).
#[expect(
    clippy::many_single_char_names,
    reason = "a,b,d,e,x,y are the canonical affine matrix and point coordinates"
)]
pub(crate) fn stable_affine_xy(
    a: f64,
    b: f64,
    d: f64,
    e: f64,
    xoff: f64,
    yoff: f64,
    x: f64,
    y: f64,
) -> (f64, f64) {
    let classic_x = a * x + b * y + xoff;
    let classic_y = d * x + e * y + yoff;
    if classic_x.is_finite() && classic_y.is_finite() {
        return (classic_x, classic_y);
    }
    let max_abs = x.abs().max(y.abs());
    if max_abs == 0.0 || !max_abs.is_finite() {
        return (classic_x, classic_y);
    }
    let scale = axis_pow2_scale(max_abs);
    let (xs, ys) = (x * scale, y * scale);
    let rx = (a * xs + b * ys) / scale + xoff;
    let ry = (d * xs + e * ys) / scale + yoff;
    (rx, ry)
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
    pub fn set_z(&self, value: Option<f64>, overwrite: bool) -> Result<Self> {
        self.set_ordinate(OrdinateAxis::Z, value, overwrite)
    }

    /// Set or clear the M ordinate. `value=None` removes M (Z is preserved);
    /// `Some(m)` with `overwrite` writes `m` at every vertex, otherwise fills
    /// only vertices that lack M (existing M is kept). Z passes through.
    pub fn set_m(&self, value: Option<f64>, overwrite: bool) -> Result<Self> {
        self.set_ordinate(OrdinateAxis::M, value, overwrite)
    }

    /// The same-kind empty retagged to `axes`, or `None` when `self` is not a
    /// canonical empty representation. The ordinate verbs route empties here
    /// so declared axes are real state on emptiness (`set_z` on
    /// `MULTIPOLYGON EMPTY` yields `MULTIPOLYGON Z EMPTY`). A non-canonical
    /// empty (e.g. a collection OF empties) is `None` — its members retag
    /// individually through the recursive verb.
    const fn retag_empty_axes(&self, axes: CoordinateAxes) -> Option<Self> {
        match self {
            Self::Empty(kind, _) => Some(Self::typed_empty(*kind, axes)),
            _ => None,
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

    /// The single engine behind [`Shape::set_z`]/[`Shape::set_m`]: per-vertex
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
                lines.try_map_values(CoordinateAxes::XY, |line| line.force_2d())?,
            ),
            Self::Polygon(polygon) => Self::Polygon(polygon.force_2d()?),
            Self::MultiPolygon(polygons) => Self::MultiPolygon(
                polygons.try_map_values(CoordinateAxes::XY, |polygon| polygon.force_2d())?,
            ),
            Self::GeometryCollection(geometries) => {
                Self::GeometryCollection(geometries.try_map_members(Self::force_2d)?)
            },
            // A dimensional empty flattens to the canonical XY empty of its
            // kind (`POINT Z EMPTY` → `POINT EMPTY`).
            Self::Empty(kind, _) => Self::typed_empty(*kind, CoordinateAxes::XY),
        })
    }

    /// Apply a 2D affine transform `(a, b, d, e, xoff, yoff)` to every
    /// coordinate: `x' = a*x + b*y + xoff`, `y' = d*x + e*y + yoff`. The z
    /// and m axes pass through unchanged via [`Shape::map_points`]. Plain
    /// multiply-adds: `mul_add` is a libm `fma` call per ordinate at the
    /// x86-64-v2 baseline, and affine placement is not a robustness seam.
    pub fn affine_transform(&self, matrix: &[f64; 6]) -> Result<Self> {
        // Identity placement is common in parameterized pipelines —
        // translate(0, 0), rotate(0), scale(1) all reduce to it here, the
        // one seam they share — so share the input instead of rebuilding.
        // Exact float equality is the point: only the bit-identical (or
        // -0.0) identity may skip.
        if affine_x_row_identity(matrix[0], matrix[1], matrix[4])
            && affine_y_row_identity(matrix[2], matrix[3], matrix[5])
        {
            return Ok(self.clone());
        }
        self.affine(matrix)
    }

    pub fn rotate(&self, angle: f64, origin: (f64, f64)) -> Result<Self> {
        let (sin, cos) = angle.sin_cos();
        self.affine_about_origin(cos, -sin, sin, cos, origin)
    }

    pub fn scale(&self, xfact: f64, yfact: f64, origin: (f64, f64)) -> Result<Self> {
        self.affine_about_origin(xfact, 0.0, 0.0, yfact, origin)
    }

    pub fn skew(&self, xs: f64, ys: f64, origin: (f64, f64)) -> Result<Self> {
        self.affine_about_origin(1.0, xs.tan(), ys.tan(), 1.0, origin)
    }

    /// Linear map about `origin` evaluated as `origin + M*(p - origin)` so
    /// extreme-but-finite centers do not overflow the expanded
    /// `xoff = ox - a*ox` form (which can produce `inf - inf` NaNs).
    #[expect(
        clippy::float_cmp,
        reason = "identity rows are exact unit coefficients; a tolerance would skip the exact five-product transform"
    )]
    fn affine_about_origin(
        &self,
        m00: f64,
        m01: f64,
        m10: f64,
        m11: f64,
        origin: (f64, f64),
    ) -> Result<Self> {
        let (ox, oy) = origin;
        // Degenerate to identity when M = I (exact bit match — caller-built).
        if same_topological_coordinate(m00, 1.0)
            && same_topological_coordinate(m01, 0.0)
            && same_topological_coordinate(m10, 0.0)
            && same_topological_coordinate(m11, 1.0)
        {
            return Ok(self.clone());
        }
        let linear = [m00, m01, m10, m11];
        self.try_map_coordseqs(
            |seq| seq.try_affine_about_origin(linear, origin),
            |point| {
                let x = if m00 == 1.0 && same_topological_coordinate(m01, 0.0) {
                    point.x
                } else {
                    affine_about_origin_ordinate(m00, m01, ox, ox, oy, point.x, point.y)?
                };
                let y = if same_topological_coordinate(m10, 0.0) && m11 == 1.0 {
                    point.y
                } else {
                    affine_about_origin_ordinate(m10, m11, oy, ox, oy, point.x, point.y)?
                };
                Point::new_axes(x, y, ZOrdinate(point.z()), MOrdinate(point.m()))
            },
        )
    }

    pub fn translate(&self, xoff: f64, yoff: f64) -> Result<Self> {
        self.affine_transform(&[1.0, 0.0, 0.0, 1.0, xoff, yoff])
    }

    /// Origin used by the ergonomic helpers when none is supplied: the
    /// centroid, or `(0, 0)` for empty geometries.
    pub fn centroid_xy(&self) -> Result<(f64, f64)> {
        Ok(match self.centroid()? {
            Self::Point(point) => (point.x, point.y),
            _ => (0.0, 0.0),
        })
    }

    /// Bounding-box center, or `(0, 0)` for empty geometries.
    pub fn bounds_center_xy(&self) -> (f64, f64) {
        self.bounds().map_or((0.0, 0.0), |bounds| {
            (
                f64::midpoint(bounds.minx(), bounds.maxx()),
                f64::midpoint(bounds.miny(), bounds.maxy()),
            )
        })
    }

    pub fn map_points<F>(&self, transform: &F) -> Result<Self>
    where
        F: Fn(Point) -> Result<Point> + ?Sized,
    {
        match self {
            Self::Point(point) => Ok(Self::Point(transform(*point)?)),
            Self::MultiPoint(points) => Ok(Self::MultiPoint(points.try_map_values(transform)?)),
            Self::LineString(points) => Ok(Self::LineString(points.try_map_points(transform)?)),
            Self::MultiLineString(lines) => {
                let members = lines
                    .members()
                    .map(|member| match member {
                        crate::geometry::MultiMember::Empty => {
                            Ok(crate::geometry::MultiMember::Empty)
                        },
                        crate::geometry::MultiMember::Value(index) => {
                            let points = lines.as_slice()[index]
                                .iter()
                                .map(transform)
                                .collect::<Result<Vec<_>, _>>()?;
                            Ok(crate::geometry::MultiMember::Value(LineSeq::from_proven(
                                CoordSeq::try_from_points(&points)?,
                            )))
                        },
                    })
                    .collect::<Result<Vec<_>, crate::error::Error>>()?;
                let axes = members
                    .iter()
                    .find_map(|member| match member {
                        crate::geometry::MultiMember::Value(line) => Some(line.axes()),
                        crate::geometry::MultiMember::Empty => None,
                    })
                    .unwrap_or_else(|| lines.axes());
                Ok(Self::MultiLineString(MultiLineString::try_new(
                    axes, members,
                )?))
            },
            Self::Polygon(polygon) => Ok(Self::Polygon(polygon.map_points(transform)?)),
            Self::MultiPolygon(polygons) => {
                let members = polygons
                    .members()
                    .map(|member| match member {
                        crate::geometry::MultiMember::Empty => {
                            Ok(crate::geometry::MultiMember::Empty)
                        },
                        crate::geometry::MultiMember::Value(index) => polygons.as_slice()[index]
                            .map_points(transform)
                            .map(crate::geometry::MultiMember::Value),
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let axes = members
                    .iter()
                    .find_map(|member| match member {
                        crate::geometry::MultiMember::Value(polygon) => Some(polygon.axes()),
                        crate::geometry::MultiMember::Empty => None,
                    })
                    .unwrap_or_else(|| polygons.axes());
                Ok(Self::MultiPolygon(MultiPolygon::try_new(axes, members)?))
            },
            Self::GeometryCollection(geometries) => Ok(Self::GeometryCollection(
                geometries.try_map_members(|geometry| geometry.map_points(transform))?,
            )),
            Self::Empty(..) => Ok(self.clone()),
        }
    }
}

impl Shape {
    /// Split the geometry into parts of at most `max_vertices` coordinates
    /// by selecting representable source-coordinate cuts across the longer
    /// axis and emitting both closed children from one exact partition (the
    /// `PostGIS` ``ST_Subdivide`` shape). Parts cover the input exactly;
    /// simple inputs return as a single part. The only over-budget emissions
    /// are spatially unsplittable: float-degenerate coordinate clusters and
    /// parts whose exact cut cannot reduce their coordinate complexity.
    pub fn subdivide(&self, max_vertices: usize, drop: bool) -> Result<Vec<Self>> {
        let mut parts = Vec::new();
        let mut budget = ExpansionBudget::new("subdivide", "max_vertices");
        split(self.clone(), max_vertices, drop, &mut parts, &mut budget)?;
        Ok(parts)
    }

    pub fn clip_by_rect(&self, rect: Bounds, drop: bool) -> Result<Self> {
        self.clip_by_rect_with_bounds(rect, drop, self.bounds())
    }

    /// Clip a canonical geographic rectangle in a frame, where subject
    /// longitude aliases and pole vertices require the frame-aware topology
    /// graph rather than Cartesian windows.
    pub(crate) fn clip_by_rect_in_geographic_frame(
        &self,
        rect: Bounds,
        drop: bool,
        bounds: Option<Bounds>,
        frame: crate::geographic::GeographicFrame,
    ) -> Result<Self> {
        let subject_plan = crate::geometry::PreparedPlan::transient(self);
        let subject_cache = crate::geometry::FrameDependentCaches::default();
        let subject =
            crate::geometry::GeographicPrepared::try_new(&subject_plan, &subject_cache, frame)?;
        let subject_bounds_sound = bounds.is_none_or(|bounds| {
            crate::geometry::raw_coordinate_bounds_are_sound_in_frame(
                Some(subject.view().shape()),
                bounds,
                frame,
            )
        });
        let rectangle_bounds_sound =
            crate::geometry::raw_coordinate_bounds_are_sound_in_frame(None, rect, frame);
        if subject_bounds_sound && rectangle_bounds_sound && !rect.crosses_antimeridian() {
            return self.clip_by_rect_with_bounds(rect, drop, bounds);
        }

        let rectangle = if rect.crosses_antimeridian() {
            let west = Bounds::new(
                rect.minx(),
                rect.miny(),
                frame.longitude().half_period(),
                rect.maxy(),
            )?;
            let east = Bounds::new(
                -frame.longitude().half_period(),
                rect.miny(),
                rect.maxx(),
                rect.maxy(),
            )?;
            Self::geometry_collection(vec![
                crate::geometry::geographic_bounds_to_shape(west, frame)?,
                crate::geometry::geographic_bounds_to_shape(east, frame)?,
            ])?
        } else {
            crate::geometry::geographic_bounds_to_shape(rect, frame)?
        };
        let originals = crate::geographic::GeographicInputs::try_from_first_and_shapes(
            subject.view(),
            std::iter::once(&rectangle),
        )
        .map_err(|error| overlay_view_error(&error))?;
        let clipped = overlay_all_geographic_from(
            &originals,
            OverlayOp::Intersection,
            Strictness::from(drop),
            true,
        )?;
        Ok(if clipped.is_empty() {
            empty_clip_shape(self)
        } else {
            clipped
        })
    }

    /// [`Self::clip_by_rect`] with the operand's bounds supplied by the
    /// caller — handle layers pass their MEMOIZED bounds so the per-call
    /// full-coordinate scan disappears from the hot clip path.
    pub fn clip_by_rect_with_bounds(
        &self,
        rect: Bounds,
        drop: bool,
        bounds: Option<Bounds>,
    ) -> Result<Self> {
        if rect.crosses_antimeridian() {
            let west = Bounds::new(rect.minx(), rect.miny(), 180.0, rect.maxy())?;
            let east = Bounds::new(-180.0, rect.miny(), rect.maxx(), rect.maxy())?;
            let left = self.clip_by_rect_with_bounds(west, drop, bounds)?;
            let right = self.clip_by_rect_with_bounds(east, drop, bounds)?;
            return left.union(&right, Strictness::from(drop));
        }
        // Bounds inside the rectangle clip to the geometry VERBATIM — the
        // dominant tiling case, and it sidesteps the polygon clipper's
        // grid snapping for untouched inputs. Disjoint bounds short to each
        // family's empty below without touching the clip machinery.
        if let Some(bounds) = bounds
            && bounds.minx() >= rect.minx()
            && bounds.maxx() <= rect.maxx()
            && bounds.miny() >= rect.miny()
            && bounds.maxy() <= rect.maxy()
        {
            return Ok(self.clone());
        }
        let disjoint = bounds.is_some_and(|bounds| !bounds.intersects(rect));
        let clipped = match self {
            Self::Point(point) if rect.contains_point(*point) => self.clone(),
            Self::MultiPoint(points) => {
                Self::multi_point(points.axes(), clip_multipoint_columns(points, rect)?)
            },
            Self::Point(_) => empty_clip_shape(self),
            Self::LineString(_)
            | Self::MultiLineString(_)
            | Self::Polygon(_)
            | Self::MultiPolygon(_)
                if disjoint =>
            {
                empty_clip_shape(self)
            },
            // Line crossings lie on input segments, so Z/M always resolve by
            // interpolation; polygon output can also include clip-rectangle
            // corners, which derive from no input vertex — there the policy
            // decides (restore-or-raise vs pure X/Y).
            Self::LineString(points) => carry_ordinates(
                clipped_linestring(points, rect)?,
                &[self],
                "clip_by_rect",
                drop,
            )?,
            Self::MultiLineString(lines) => {
                let mut clipped = Vec::new();
                for line in lines {
                    clipped.extend(clipped_line_parts(line, rect)?);
                }
                carry_ordinates(line_parts_to_shape(clipped)?, &[self], "clip_by_rect", drop)?
            },
            Self::Polygon(polygon) => carry_ordinates(
                clip_polygonal(std::slice::from_ref(polygon), rect)?,
                &[self],
                "clip_by_rect",
                drop,
            )?,
            Self::MultiPolygon(polygons) => carry_ordinates(
                clip_polygonal(polygons, rect)?,
                &[self],
                "clip_by_rect",
                drop,
            )?,
            Self::GeometryCollection(geometries) => Self::GeometryCollection(
                geometries.try_map_members(|geometry| geometry.clip_by_rect(rect, drop))?,
            ),
            Self::Empty(..) => self.clone(),
        };
        // Normalize a vanished clip to the SUBJECT's typed empty (a clipped-away
        // line is `LINESTRING EMPTY`, matching `intersection` with the clip box;
        // an all-empty collection flattens to `GEOMETRYCOLLECTION EMPTY` rather
        // than a nest of empties), so the op's output type stays stable.
        Ok(if clipped.is_empty() {
            empty_clip_shape(self)
        } else {
            clipped
        })
    }
}

/// Empty result of a rectangle clip, typed to the SUBJECT's family so the op's
/// output type is stable: clipping a line away yields `LINESTRING EMPTY`, a
/// polygon `POLYGON EMPTY` (matching `intersection` with the 2D clip box), and
/// a collection a flat `GEOMETRYCOLLECTION EMPTY`.
pub(crate) fn empty_clip_shape(subject: &Shape) -> Shape {
    match subject {
        // A collection clips to a flat empty collection, and an already-empty
        // subject keeps its exact kind; every typed family routes through the
        // ONE dim→empty source so the mapping has a single home.
        Shape::GeometryCollection(_) => {
            Shape::typed_empty(GeometryKind::GeometryCollection, subject.axes())
        },
        Shape::Empty(..) => subject.clone(),
        _ => crate::geometry::empty_shape_for_dimension_with_axes(
            subject.topological_dimension(),
            subject.axes(),
        ),
    }
}

impl Shape {
    /// Apply `transform` to every coordinate chain (lines and rings),
    /// keeping the structure; `None` from the transform reuses the
    /// original chain (cheap column clone, no re-materialization).
    pub(crate) fn map_chains(
        &self,
        transform: &impl Fn(&CoordSeq) -> Result<Option<CoordSeq>>,
    ) -> Result<Self> {
        Ok(match self {
            Self::Point(_) | Self::MultiPoint(_) | Self::Empty(..) => self.clone(),
            Self::LineString(points) => Self::LineString(
                transform(points)?.map_or_else(|| points.clone(), LineSeq::from_proven),
            ),
            Self::MultiLineString(lines) => {
                Self::MultiLineString(lines.try_map_values(lines.axes(), |line| {
                    Ok::<_, crate::error::Error>(
                        transform(line)?.map_or_else(|| line.clone(), LineSeq::from_proven),
                    )
                })?)
            },
            Self::Polygon(polygon) => Self::Polygon(
                polygon
                    .map_ring_seqs(|ring| Ok(transform(ring)?.unwrap_or_else(|| ring.clone())))?,
            ),
            Self::MultiPolygon(polygons) => {
                Self::MultiPolygon(polygons.try_map_values(polygons.axes(), |polygon| {
                    polygon
                        .map_ring_seqs(|ring| Ok(transform(ring)?.unwrap_or_else(|| ring.clone())))
                })?)
            },
            Self::GeometryCollection(geometries) => Self::GeometryCollection(
                geometries.try_map_members(|geometry| geometry.map_chains(transform))?,
            ),
        })
    }

    pub fn remove_repeated_points(&self, tolerance: f64) -> Result<Self> {
        let tolerance = NonNegative::try_new("tolerance", tolerance)?.get();
        let remove_repeated_polygon = |polygon: &Polygon| -> Result<Option<Polygon>> {
            polygon.map_rings_collapsing(|ring| remove_repeated_ring_points(ring, tolerance))
        };
        Ok(match self {
            Self::Point(point) => Self::Point(*point),
            Self::MultiPoint(points) => {
                let values = points
                    .members()
                    .filter_map(|member| match member {
                        crate::geometry::MultiMember::Empty => None,
                        crate::geometry::MultiMember::Value(index) => Some(points.point_at(index)),
                    })
                    .collect::<Vec<_>>();
                let cleaned =
                    remove_repeated_points(&CoordSeq::try_from_points(&values)?, tolerance)?;
                let mut cleaned = cleaned.iter();
                let members = points
                    .members()
                    .filter_map(|member| match member {
                        crate::geometry::MultiMember::Empty => {
                            Some(crate::geometry::MultiMember::Empty)
                        },
                        crate::geometry::MultiMember::Value(_) => {
                            cleaned.next().map(crate::geometry::MultiMember::Value)
                        },
                    })
                    .collect::<Vec<_>>();
                if members.is_empty() {
                    Self::typed_empty(GeometryKind::MultiPoint, points.axes())
                } else {
                    Self::MultiPoint(MultiPoint::from_values(points.axes(), members)?)
                }
            },
            Self::LineString(points) => Self::LineString(points.remove_repeated_points(tolerance)?),
            Self::MultiLineString(lines) => Self::MultiLineString(
                lines
                    .try_map_values(lines.axes(), |line| line.remove_repeated_points(tolerance))?,
            ),
            Self::Polygon(polygon) => remove_repeated_polygon(polygon)?.map_or_else(
                || Self::typed_empty(GeometryKind::Polygon, polygon.axes()),
                Self::Polygon,
            ),
            Self::MultiPolygon(polygons) => {
                let members = polygons
                    .members()
                    .map(|member| match member {
                        crate::geometry::MultiMember::Empty => {
                            Ok(crate::geometry::MultiMember::Empty)
                        },
                        crate::geometry::MultiMember::Value(index) => Ok(remove_repeated_polygon(
                            &polygons.as_slice()[index],
                        )?
                        .map_or(
                            crate::geometry::MultiMember::Empty,
                            crate::geometry::MultiMember::Value,
                        )),
                    })
                    .collect::<Result<Vec<_>>>()?;
                Self::MultiPolygon(MultiPolygon::from_values(polygons.axes(), members)?)
            },
            Self::GeometryCollection(geometries) => Self::GeometryCollection(
                geometries
                    .try_map_members(|geometry| geometry.remove_repeated_points(tolerance))?,
            ),
            Self::Empty(..) => self.clone(),
        })
    }

    /// Densify every segment chain by `fraction` (see `densify_points`) —
    /// the re-segmentation behind `segmentize(fraction=)` and the vertex
    /// refinement behind discrete Fréchet. Caller validates `fraction` in
    /// `(0, 1]` and names the budget after its public parameter.
    pub(crate) fn densified_budgeted(
        &self,
        fraction: f64,
        budget: &mut ExpansionBudget,
    ) -> Result<Self> {
        self.densified_budgeted_with_placement(fraction, SegmentPlacement::Planar, budget)
    }

    pub(crate) fn densified_budgeted_with_placement(
        &self,
        fraction: f64,
        placement: SegmentPlacement<'_>,
        budget: &mut ExpansionBudget,
    ) -> Result<Self> {
        Ok(match self {
            Self::Point(_) | Self::MultiPoint(_) | Self::Empty(..) => self.clone(),
            Self::LineString(points) => {
                Self::LineString(points.densify(fraction, placement, budget)?)
            },
            Self::MultiLineString(lines) => {
                Self::MultiLineString(lines.try_map_values(lines.axes(), |line| {
                    line.densify(fraction, placement, budget)
                })?)
            },
            Self::Polygon(polygon) => Self::Polygon(
                polygon.densified_budgeted_with_placement(fraction, placement, budget)?,
            ),
            Self::MultiPolygon(polygons) => {
                Self::MultiPolygon(polygons.try_map_values(polygons.axes(), |polygon| {
                    polygon.densified_budgeted_with_placement(fraction, placement, budget)
                })?)
            },
            Self::GeometryCollection(geometries) => {
                Self::GeometryCollection(geometries.try_map_members(|geometry| {
                    geometry.densified_budgeted_with_placement(fraction, placement, budget)
                })?)
            },
        })
    }

    /// Planar `segmentize` — the CRS-free reading. CRS-aware callers resolve a
    /// [`SegmentPlacement`] first and use [`Self::segmentize_budgeted`].
    #[cfg(test)]
    pub fn segmentize(&self, max_segment_length: f64) -> Result<Self> {
        let mut budget = ExpansionBudget::new("segmentize", "max_length");
        let output =
            self.segmentize_budgeted(max_segment_length, SegmentPlacement::Planar, &mut budget)?;
        crate::geometry::verify_planar_segmentize_shape(&output, max_segment_length)?;
        Ok(output)
    }

    pub(crate) fn segmentize_budgeted(
        &self,
        max_segment_length: f64,
        placement: SegmentPlacement<'_>,
        budget: &mut ExpansionBudget,
    ) -> Result<Self> {
        Ok(match self {
            Self::Point(point) => Self::Point(*point),
            Self::MultiPoint(points) => Self::MultiPoint(points.clone()),
            Self::LineString(points) => {
                Self::LineString(points.segmentize(max_segment_length, placement, budget)?)
            },
            Self::MultiLineString(lines) => {
                Self::MultiLineString(lines.try_map_values(lines.axes(), |line| {
                    line.segmentize(max_segment_length, placement, budget)
                })?)
            },
            Self::Polygon(polygon) => {
                Self::Polygon(polygon.segmentize_budgeted(max_segment_length, placement, budget)?)
            },
            Self::MultiPolygon(polygons) => {
                Self::MultiPolygon(polygons.try_map_values(polygons.axes(), |polygon| {
                    polygon.segmentize_budgeted(max_segment_length, placement, budget)
                })?)
            },
            Self::GeometryCollection(geometries) => {
                Self::GeometryCollection(geometries.try_map_members(|geometry| {
                    geometry.segmentize_budgeted(max_segment_length, placement, budget)
                })?)
            },
            Self::Empty(..) => self.clone(),
        })
    }

    /// Swap the X and Y ordinates of every coordinate (Z/M untouched) —
    /// the axis-order repair for latitude/longitude-ordered data.
    pub fn swap_xy(&self) -> Result<Self> {
        // O(1) column swap per sequence (Arc-pointer swap, no per-vertex copy);
        // same coordinates swapped, bit-identical to a `(x, y) → (y, x)` map.
        self.try_map_coordseqs(
            |seq| Ok(seq.swap_xy()),
            |point| Ok(point.with_xy_raw(point.y, point.x)),
        )
    }

    pub fn reverse(&self) -> Result<Self> {
        Ok(match self {
            Self::Point(point) => Self::Point(*point),
            Self::MultiPoint(points) => Self::MultiPoint(points.clone()),
            // Columnar reverse-copies — no AoS `Vec<Point>` round-trip.
            Self::LineString(points) => Self::LineString(points.reversed()?),
            Self::MultiLineString(lines) => {
                Self::MultiLineString(lines.try_map_values(lines.axes(), |line| line.reversed())?)
            },
            Self::Polygon(polygon) => Self::Polygon(polygon.reverse()?),
            Self::MultiPolygon(polygons) => {
                Self::MultiPolygon(polygons.try_map_values(polygons.axes(), Polygon::reverse)?)
            },
            Self::GeometryCollection(geometries) => {
                Self::GeometryCollection(geometries.try_map_members(Self::reverse)?)
            },
            Self::Empty(..) => self.clone(),
        })
    }

    pub fn orient_polygons(&self, exterior_cw: bool) -> Result<Self> {
        Ok(match self {
            Self::Polygon(polygon) => Self::Polygon(polygon.orient(exterior_cw)?),
            Self::MultiPolygon(polygons) => Self::MultiPolygon(
                polygons.try_map_values(polygons.axes(), |polygon| polygon.orient(exterior_cw))?,
            ),
            Self::GeometryCollection(geometries) => Self::GeometryCollection(
                geometries.try_map_members(|geometry| geometry.orient_polygons(exterior_cw))?,
            ),
            Self::Point(_)
            | Self::MultiPoint(_)
            | Self::LineString(_)
            | Self::MultiLineString(_)
            | Self::Empty(..) => self.clone(),
        })
    }

    pub fn normalize(&self) -> Result<Self> {
        Ok(match self {
            Self::Point(point) => Self::Point(*point),
            Self::MultiPoint(points) => {
                let mut values = points.try_to_vec()?;
                values.sort_unstable_by(compare_points);
                let mut values = values.into_iter();
                let mut members = Vec::new();
                crate::boundary::try_reserve_vec(&mut members, points.member_count())?;
                for member in points.members() {
                    let mapped = match member {
                        crate::geometry::MultiMember::Empty => crate::geometry::MultiMember::Empty,
                        crate::geometry::MultiMember::Value(_) => {
                            crate::geometry::MultiMember::Value(values.next().ok_or_else(|| {
                                GeometryErrorKind::message(
                                    "present MultiPoint member missing a sorted value",
                                )
                            })?)
                        },
                    };
                    crate::try_push_vec(&mut members, mapped)?;
                }
                Self::MultiPoint(MultiPoint::from_values(points.axes(), members)?)
            },
            Self::LineString(points) => Self::LineString(points.normalized()?),
            Self::MultiLineString(lines) => {
                let mut values = Vec::new();
                crate::try_reserve_vec(&mut values, lines.len())?;
                for line in lines.iter() {
                    crate::try_push_vec(&mut values, line.normalized()?)?;
                }
                values.sort_unstable_by(compare_point_slices);
                let mut values = values.into_iter();
                let mut members = Vec::new();
                crate::try_reserve_vec(&mut members, lines.member_count())?;
                for member in lines.members() {
                    let mapped = match member {
                        crate::geometry::MultiMember::Empty => crate::geometry::MultiMember::Empty,
                        crate::geometry::MultiMember::Value(_) => {
                            crate::geometry::MultiMember::Value(values.next().ok_or_else(|| {
                                GeometryErrorKind::message(
                                    "present MultiLineString member missing a normalized value",
                                )
                            })?)
                        },
                    };
                    crate::try_push_vec(&mut members, mapped)?;
                }
                Self::MultiLineString(MultiLineString::from_values(lines.axes(), members)?)
            },
            Self::Polygon(polygon) => Self::Polygon(polygon.normalize()?),
            Self::MultiPolygon(polygons) => {
                let mut values = Vec::new();
                crate::try_reserve_vec(&mut values, polygons.len())?;
                for polygon in polygons.iter() {
                    crate::try_push_vec(&mut values, polygon.normalize()?)?;
                }
                values.sort_unstable_by(compare_polygons);
                let mut values = values.into_iter();
                let mut members = Vec::new();
                crate::try_reserve_vec(&mut members, polygons.member_count())?;
                for member in polygons.members() {
                    let mapped = match member {
                        crate::geometry::MultiMember::Empty => crate::geometry::MultiMember::Empty,
                        crate::geometry::MultiMember::Value(_) => {
                            crate::geometry::MultiMember::Value(values.next().ok_or_else(|| {
                                GeometryErrorKind::message(
                                    "present MultiPolygon member missing a normalized value",
                                )
                            })?)
                        },
                    };
                    crate::try_push_vec(&mut members, mapped)?;
                }
                Self::MultiPolygon(MultiPolygon::from_values(polygons.axes(), members)?)
            },
            Self::GeometryCollection(geometries) => {
                let mut members = Vec::new();
                crate::try_reserve_vec(&mut members, geometries.len())?;
                for geometry in geometries {
                    crate::try_push_vec(&mut members, geometry.normalize()?)?;
                }
                members.sort_unstable_by(compare_shapes);
                Self::GeometryCollection(geometries.try_rebuild_members(members)?)
            },
            Self::Empty(..) => self.clone(),
        })
    }
}
