#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use std::convert::Infallible;

use crate::NonNegative;
use crate::geometry::constructive::{
    Result, SegmentPlacement, decimal_scale, densify_points_budgeted, normalized_line,
    quantize_column_simd, remove_repeated_line_points, remove_repeated_points,
    segmentize_points_budgeted, snap_column_simd, split,
};
use crate::geometry::{
    AxisFrame, Bounds, CoordSeq, CoordinateAxes, EmptyKind, ExpansionBudget, GeometryErrorKind,
    HasM, HasZ, LineSeq, MOrdinate, Point, Polygon, ReduceSimd, Shape, Strictness, ZOrdinate,
    carry_ordinates, clip_multipoint_columns, clip_polygonal, clipped_line_parts,
    clipped_linestring, column_all_finite, compare_point_slices, compare_points, compare_polygons,
    compare_shapes, line_parts_to_shape, same_topological_coordinate, simd_xy_map_f64,
    simd_xy_map_one_f64,
};

/// Which optional ordinate [`Shape::set_z`]/[`Shape::set_m`] act on.
#[derive(Clone, Copy)]
enum OrdinateAxis {
    Z,
    M,
}

impl CoordSeq {
    /// `op` over every `(x, y)` pair, Z/M columns carried through untouched —
    /// the columnar form of a per-point XY map (no `Point` gather, no axes
    /// re-derivation, one pass per column).
    #[expect(
        clippy::impl_trait_in_params,
        reason = "the one-pass coordinate callback is deliberately existential"
    )]
    pub fn map_xy(&self, mut op: impl FnMut(f64, f64) -> (f64, f64)) -> Self {
        // Pre-sized output columns written by index (no push branch), so the
        // per-pair arithmetic vectorizes.
        let mut xs = vec![0.0; self.len()].into_boxed_slice();
        let mut ys = vec![0.0; self.len()].into_boxed_slice();
        for (((out_x, out_y), &x), &y) in xs
            .iter_mut()
            .zip(ys.iter_mut())
            .zip(self.xs())
            .zip(self.ys())
        {
            (*out_x, *out_y) = op(x, y);
        }
        Self::from_columns(xs.into(), ys.into(), self.carried_zs(), self.carried_ms())
    }

    /// Fallible [`map_xy`](Self::map_xy) for transforms that can overflow:
    /// the per-point finiteness validation is hoisted into one columnar pass
    /// over the produced ordinates.
    #[expect(
        clippy::impl_trait_in_params,
        reason = "the one-pass coordinate callback is deliberately existential"
    )]
    pub fn try_map_xy(&self, op: impl FnMut(f64, f64) -> (f64, f64)) -> Result<Self> {
        let mapped = self.map_xy(op);
        if column_all_finite(mapped.xs()) && column_all_finite(mapped.ys()) {
            Ok(mapped)
        } else {
            Err(GeometryErrorKind::NonFiniteCoordinate.into())
        }
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
        let mut out_x = (!x_identity).then(|| vec![0.0; n].into_boxed_slice());
        let mut out_y = (!y_identity).then(|| vec![0.0; n].into_boxed_slice());
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
            Ok(Self::from_columns(
                out_x.map_or_else(|| self.carried_xs(), Into::into),
                out_y.map_or_else(|| self.carried_ys(), Into::into),
                self.carried_zs(),
                self.carried_ms(),
            ))
        }
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
        Ok(Self::from_columns(
            snap_column_simd(self.xs(), origin.0, size.0)?.into(),
            snap_column_simd(self.ys(), origin.1, size.1)?.into(),
            self.carried_zs(),
            self.carried_ms(),
        ))
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
    ) -> Result<Self, E> {
        let mut xs = Vec::with_capacity(self.len());
        let mut ys = Vec::with_capacity(self.len());
        for (&x, &y) in std::iter::zip(self.xs(), self.ys()) {
            let (x, y) = op(x, y)?;
            xs.push(x);
            ys.push(y);
        }
        Ok(Self::from_columns(
            xs.into(),
            ys.into(),
            self.carried_zs(),
            self.carried_ms(),
        ))
    }

    /// Every ordinate column quantized to `precision` decimal places.
    pub fn quantize(&self, precision: i32) -> Self {
        let scale = decimal_scale(precision);
        let map = |column: &[f64]| -> Box<[f64]> { quantize_column_simd(column, scale) };
        Self::from_columns(
            map(self.xs()).into(),
            map(self.ys()).into(),
            self.zs().map(map).map(Into::into),
            self.ms().map(map).map(Into::into),
        )
    }

    /// The XY columns alone, dropping Z/M. Shares the backing `Arc`s with zero
    /// copy when this sequence spans its whole columns (the common case).
    pub fn force_2d(&self) -> Self {
        Self::from_columns(self.carried_xs(), self.carried_ys(), None, None)
    }

    /// Set or clear the Z ordinate column. `value=None` removes Z (M is
    /// preserved); `Some(z)` with `overwrite` writes `z` at every vertex,
    /// otherwise fills only when the column is absent (existing Z is kept).
    pub fn set_z(&self, value: Option<f64>, overwrite: bool) -> Self {
        self.set_ordinate(OrdinateAxis::Z, value, overwrite)
    }

    /// Set or clear the M ordinate column. `value=None` removes M (Z is
    /// preserved); `Some(m)` with `overwrite` writes `m` at every vertex,
    /// otherwise fills only when the column is absent (existing M is kept).
    pub fn set_m(&self, value: Option<f64>, overwrite: bool) -> Self {
        self.set_ordinate(OrdinateAxis::M, value, overwrite)
    }

    /// The columnar engine behind [`Shape::set_z`]/[`Shape::set_m`]: one
    /// optional ordinate column is replaced or dropped, the other passes
    /// through. Homogeneous packed storage shares uniform axes per column,
    /// so fill-only (`overwrite=False` on an existing column) is an
    /// identity without a per-vertex scan.
    fn set_ordinate(&self, axis: OrdinateAxis, value: Option<f64>, overwrite: bool) -> Self {
        let len = self.len();
        let (zs, ms) = (self.zs(), self.ms());
        let column = match axis {
            OrdinateAxis::Z => zs,
            OrdinateAxis::M => ms,
        };
        if value.is_none() && column.is_none() || value.is_some() && !overwrite && column.is_some()
        {
            return self.clone();
        }
        let fill = |value: f64| -> Box<[f64]> { vec![value; len].into_boxed_slice() };
        // The set axis allocates exactly one fresh column; every untouched
        // column (X, Y, and the OTHER optional ordinate) is Arc-carried, not
        // re-copied — zero allocation on the full-window common case.
        // An empty sequence gets a zero-length column: axes are real state
        // on empties, so `set_z`/`force_3d` flip has_z/has_m even with no
        // vertices (`MULTIPOINT EMPTY` → `MULTIPOINT Z EMPTY`).
        let ordinate_column = |value: Option<f64>| value.map(|value| fill(value).into());
        let (out_zs, out_ms) = match axis {
            OrdinateAxis::Z => (ordinate_column(value), self.carried_ms()),
            OrdinateAxis::M => (self.carried_zs(), ordinate_column(value)),
        };
        Self::from_columns(self.carried_xs(), self.carried_ys(), out_zs, out_ms)
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
    ) -> Result<Self, E> {
        Ok(match self {
            Self::Point(point) => {
                let (x, y) = op(point.x, point.y)?;
                Self::Point(point.with_xy_unchecked(x, y))
            },
            Self::MultiPoint(points) => Self::MultiPoint(points.try_map_xy_with(op)?),
            Self::LineString(points) => {
                Self::LineString(LineSeq::from_trusted(points.try_map_xy_with(op)?))
            },
            Self::MultiLineString(lines) => Self::MultiLineString(
                lines
                    .iter()
                    .map(|line| line.try_map_xy_with(op).map(LineSeq::from_trusted))
                    .collect::<Result<_, _>>()?,
            ),
            Self::Polygon(polygon) => Self::Polygon(polygon.map_rings_xy_with(op)?),
            Self::MultiPolygon(polygons) => Self::MultiPolygon(
                polygons
                    .iter()
                    .map(|polygon| polygon.map_rings_xy_with(op))
                    .collect::<Result<_, _>>()?,
            ),
            Self::GeometryCollection(geometries) => Self::GeometryCollection(
                geometries
                    .iter()
                    .map(|geometry| geometry.map_xy_with(op))
                    .collect::<Result<_, _>>()?,
            ),
            Self::Empty(..) => self.clone(),
        })
    }

    /// Columnar XY transform over every coordinate sequence of the shape
    /// (Z/M untouched), validating finiteness once per column. The shared
    /// engine behind `affine_transform` and friends — routes every coordinate
    /// sequence through the SIMD [`CoordSeq::try_affine`] column kernel.
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

/// Affine map with power-of-two pre-scale of the input coordinates so
/// cancelling huge products (`2*1e308 + (-2)*1e308`) stay finite. Ordinary
/// mid-range inputs take the classic multiply-add (bit-identical).
#[expect(
    clippy::many_single_char_names,
    reason = "a,b,d,e,x,y are the canonical affine matrix and point coordinates"
)]
fn stable_affine_xy(
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
    // Map max |ordinate| into ~[0.5, 1).
    let exp = max_abs.log2().floor();
    let scale_exp = (-exp).clamp(-1022.0, 1023.0) as i32;
    let scale = f64::from_bits(((scale_exp + 1023) as u64) << 52);
    let (xs, ys) = (x * scale, y * scale);
    let rx = (a * xs + b * ys) / scale + xoff;
    let ry = (d * xs + e * ys) / scale + yoff;
    (rx, ry)
}

impl Shape {
    pub fn quantize(&self, precision: i32) -> Self {
        match self {
            Self::Point(point) => Self::Point(point.quantize(precision)),
            Self::MultiPoint(points) => Self::MultiPoint(points.quantize(precision)),
            Self::LineString(points) => {
                Self::LineString(LineSeq::from_trusted(points.quantize(precision)))
            },
            Self::MultiLineString(lines) => Self::MultiLineString(
                lines
                    .iter()
                    .map(|line| LineSeq::from_trusted(line.quantize(precision)))
                    .collect(),
            ),
            Self::Polygon(polygon) => Self::Polygon(polygon.quantize(precision)),
            Self::MultiPolygon(polygons) => Self::MultiPolygon(
                polygons
                    .iter()
                    .map(|polygon| polygon.quantize(precision))
                    .collect(),
            ),
            Self::GeometryCollection(geometries) => Self::GeometryCollection(
                geometries
                    .iter()
                    .map(|geometry| geometry.quantize(precision))
                    .collect(),
            ),
            Self::Empty(..) => self.clone(),
        }
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
    fn retag_empty_axes(&self, axes: CoordinateAxes) -> Option<Self> {
        match self {
            Self::Empty(kind, _) => Some(Self::typed_empty(*kind, axes)),
            Self::MultiPoint(points) if points.is_empty() => {
                Some(Self::MultiPoint(CoordSeq::empty(axes)))
            },
            Self::LineString(points) if points.is_empty() => {
                Some(Self::LineString(LineSeq::empty(axes)))
            },
            Self::MultiLineString(lines) if lines.is_empty() => {
                Some(Self::typed_empty(EmptyKind::MultiLineString, axes))
            },
            Self::MultiPolygon(polygons) if polygons.is_empty() => {
                Some(Self::typed_empty(EmptyKind::MultiPolygon, axes))
            },
            Self::GeometryCollection(geometries) if geometries.is_empty() => {
                Some(Self::typed_empty(EmptyKind::GeometryCollection, axes))
            },
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
        if let Self::GeometryCollection(geometries) = self {
            return Ok(Self::GeometryCollection(
                geometries
                    .iter()
                    .map(|geometry| geometry.set_ordinate(axis, value, overwrite))
                    .collect::<Result<_>>()?,
            ));
        }
        self.try_map_coordseqs(
            |seq| Ok(seq.set_ordinate(axis, value, overwrite)),
            |point| {
                Ok(CoordSeq::from(vec![*point])
                    .set_ordinate(axis, value, overwrite)
                    .first()
                    .expect("single-point sequence"))
            },
        )
    }

    pub fn force_2d(&self) -> Self {
        // Already-2D input shares its storage: has_z/has_m are O(parts)
        // column flags, so the defensive force_2d-on-2D pattern is free.
        if !self.has_z() && !self.has_m() {
            return self.clone();
        }
        match self {
            Self::Point(point) => Self::Point(point.force_2d()),
            Self::MultiPoint(points) => Self::MultiPoint(points.force_2d()),
            Self::LineString(points) => Self::LineString(LineSeq::from_trusted(points.force_2d())),
            Self::MultiLineString(lines) => Self::MultiLineString(
                lines
                    .iter()
                    .map(|line| LineSeq::from_trusted(line.force_2d()))
                    .collect(),
            ),
            Self::Polygon(polygon) => Self::Polygon(polygon.force_2d()),
            Self::MultiPolygon(polygons) => {
                Self::MultiPolygon(polygons.iter().map(Polygon::force_2d).collect())
            },
            Self::GeometryCollection(geometries) => {
                Self::GeometryCollection(geometries.iter().map(Self::force_2d).collect())
            },
            // A dimensional empty flattens to the canonical XY empty of its
            // kind (`POINT Z EMPTY` → `POINT EMPTY`).
            Self::Empty(kind, _) => Self::typed_empty(*kind, CoordinateAxes::XY),
        }
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
        self.map_points(&|point| {
            // Classic form first (bit-stable for ordinary coordinates).
            let dx = point.x - ox;
            let dy = point.y - oy;
            let mut x = ox + m00 * dx + m01 * dy;
            let mut y = oy + m10 * dx + m11 * dy;
            if !x.is_finite() || !y.is_finite() {
                // Frame axes independently. A common scale chosen from a
                // huge X turns a still-stored 1e-300 Y delta into zero before
                // a diagonal scale can preserve it; packed execution already
                // keeps that axis, so scalar must share the same frame rule.
                let extent_x = point.x.abs().max(ox.abs());
                let extent_y = point.y.abs().max(oy.abs());
                if let Some(frame) = AxisFrame::from_origin_extents(
                    Point::new_unchecked_xy(ox, oy),
                    extent_x,
                    extent_y,
                ) {
                    let local = frame.frame_xy(point.x, point.y);
                    let sx = frame.scale_x();
                    let sy = frame.scale_y();
                    // Diagonal transforms are the common `scale` path.  They
                    // stay entirely in their own axis frame: evaluating a
                    // zero off-axis coefficient through `0 * (huge / tiny)`
                    // would manufacture NaN and erase an unrelated axis.
                    // General affine maps retain the cross terms below.
                    let scaled_x = ox * sx
                        + m00 * local.x
                        + if m01 == 0.0 {
                            0.0
                        } else {
                            (m01 * (local.y / sy)) * sx
                        };
                    let scaled_y = oy * sy
                        + if m10 == 0.0 {
                            0.0
                        } else {
                            (m10 * (local.x / sx)) * sy
                        }
                        + m11 * local.y;
                    x = scaled_x / sx;
                    y = scaled_y / sy;
                }
            }
            Point::new_axes(x, y, ZOrdinate(point.z()), MOrdinate(point.m()))
        })
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
            Self::MultiPoint(points) => Ok(Self::MultiPoint(
                points.iter().map(transform).collect::<Result<_, _>>()?,
            )),
            Self::LineString(points) => {
                Ok(Self::LineString(LineSeq::from_trusted(CoordSeq::from(
                    points
                        .iter()
                        .map(transform)
                        .collect::<Result<Vec<_>, _>>()?,
                ))))
            },
            Self::MultiLineString(lines) => Ok(Self::MultiLineString(
                lines
                    .iter()
                    .map(|line| {
                        Ok::<LineSeq, crate::error::Error>(LineSeq::from_trusted(CoordSeq::from(
                            line.iter().map(transform).collect::<Result<Vec<_>, _>>()?,
                        )))
                    })
                    .collect::<Result<_, _>>()?,
            )),
            Self::Polygon(polygon) => Ok(Self::Polygon(polygon.map_points(transform)?)),
            Self::MultiPolygon(polygons) => Ok(Self::MultiPolygon(
                polygons
                    .iter()
                    .map(|polygon| polygon.map_points(transform))
                    .collect::<Result<_, _>>()?,
            )),
            Self::GeometryCollection(geometries) => Ok(Self::GeometryCollection(
                geometries
                    .iter()
                    .map(|geometry| geometry.map_points(transform))
                    .collect::<Result<_, _>>()?,
            )),
            Self::Empty(..) => Ok(self.clone()),
        }
    }
}

impl Shape {
    /// Split the geometry into parts of at most `max_vertices` coordinates
    /// by recursively halving its bounds across the longer axis and clipping
    /// (the `PostGIS` ``ST_Subdivide`` shape). Parts cover the input exactly;
    /// simple inputs return as a single part. The only over-budget emissions
    /// are spatially unsplittable: float-degenerate coordinate clusters, and
    /// parts where even a vertex-median cut cannot shed vertices (every cut
    /// segment crossing replaces a dropped vertex).
    pub fn subdivide(&self, max_vertices: usize, drop: bool) -> Result<Vec<Self>> {
        let mut parts = Vec::new();
        split(self.clone(), max_vertices, drop, &mut parts)?;
        Ok(parts)
    }

    pub fn clip_by_rect(&self, rect: Bounds, drop: bool) -> Result<Self> {
        self.clip_by_rect_with_bounds(rect, drop, self.bounds())
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
            Self::MultiPoint(points) => Self::MultiPoint(clip_multipoint_columns(points, rect)),
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
                clipped_linestring(points, rect),
                &[self],
                "clip_by_rect",
                drop,
            )?,
            Self::MultiLineString(lines) => {
                let clipped = lines
                    .iter()
                    .flat_map(|line| clipped_line_parts(line, rect))
                    .collect::<Vec<_>>();
                carry_ordinates(line_parts_to_shape(clipped), &[self], "clip_by_rect", drop)?
            },
            Self::Polygon(polygon) => carry_ordinates(
                clip_polygonal(std::slice::from_ref(polygon), rect),
                &[self],
                "clip_by_rect",
                drop,
            )?,
            Self::MultiPolygon(polygons) => carry_ordinates(
                clip_polygonal(polygons, rect),
                &[self],
                "clip_by_rect",
                drop,
            )?,
            Self::GeometryCollection(geometries) => Self::GeometryCollection(
                geometries
                    .iter()
                    .map(|geometry| geometry.clip_by_rect(rect, drop))
                    .collect::<Result<_, _>>()?,
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
        Shape::GeometryCollection(_) => Shape::GeometryCollection(Vec::new()),
        Shape::Empty(..) => subject.clone(),
        _ => crate::geometry::empty_shape_for_dimension(subject.topological_dimension()),
    }
}

impl Shape {
    /// Apply `transform` to every coordinate chain (lines and rings),
    /// keeping the structure; `None` from the transform reuses the
    /// original chain (cheap column clone, no re-materialization).
    pub(crate) fn map_chains(&self, transform: &impl Fn(&CoordSeq) -> Option<Vec<Point>>) -> Self {
        match self {
            Self::Point(_) | Self::MultiPoint(_) | Self::Empty(..) => self.clone(),
            Self::LineString(points) => Self::LineString(transform(points).map_or_else(
                || points.clone(),
                |line| {
                    LineSeq::try_new(CoordSeq::from(line))
                        .expect("chain transform produced empty or at least two vertices")
                },
            )),
            Self::MultiLineString(lines) => Self::MultiLineString(
                lines
                    .iter()
                    .map(|line| {
                        transform(line).map_or_else(
                            || line.clone(),
                            |line| {
                                LineSeq::try_new(CoordSeq::from(line)).expect(
                                    "chain transform produced empty or at least two vertices",
                                )
                            },
                        )
                    })
                    .collect(),
            ),
            Self::Polygon(polygon) => Self::Polygon(polygon.map_ring_seqs(|ring| {
                transform(ring).map_or_else(|| ring.clone(), CoordSeq::from)
            })),
            Self::MultiPolygon(polygons) => Self::MultiPolygon(
                polygons
                    .iter()
                    .map(|polygon| {
                        polygon.map_ring_seqs(|ring| {
                            transform(ring).map_or_else(|| ring.clone(), CoordSeq::from)
                        })
                    })
                    .collect(),
            ),
            Self::GeometryCollection(geometries) => Self::GeometryCollection(
                geometries
                    .iter()
                    .map(|geometry| geometry.map_chains(transform))
                    .collect(),
            ),
        }
    }

    pub fn remove_repeated_points(&self, tolerance: f64) -> Result<Self> {
        let tolerance = NonNegative::try_new("tolerance", tolerance)?.get();
        Ok(match self {
            Self::Point(point) => Self::Point(*point),
            Self::MultiPoint(points) => Self::MultiPoint(remove_repeated_points(points, tolerance)),
            Self::LineString(points) => Self::LineString(LineSeq::from_trusted(
                remove_repeated_line_points(points, tolerance),
            )),
            Self::MultiLineString(lines) => Self::MultiLineString(
                lines
                    .iter()
                    .map(|line| LineSeq::from_trusted(remove_repeated_line_points(line, tolerance)))
                    .collect(),
            ),
            Self::Polygon(polygon) => Self::Polygon(polygon.remove_repeated_points(tolerance)),
            Self::MultiPolygon(polygons) => Self::MultiPolygon(
                polygons
                    .iter()
                    .map(|polygon| polygon.remove_repeated_points(tolerance))
                    .collect(),
            ),
            Self::GeometryCollection(geometries) => Self::GeometryCollection(
                geometries
                    .iter()
                    .map(|geometry| geometry.remove_repeated_points(tolerance))
                    .collect::<Result<_, _>>()?,
            ),
            Self::Empty(..) => self.clone(),
        })
    }

    /// Densify every segment chain by `fraction` (see `densify_points`) —
    /// the re-segmentation behind continuous Hausdorff and the vertex
    /// refinement behind discrete Fréchet. Caller validates `fraction` in `(0, 1]`.
    pub fn densified(&self, fraction: f64) -> Result<Self> {
        let mut budget = ExpansionBudget::new("densify", "fraction");
        self.densified_budgeted(fraction, &mut budget)
    }

    pub(crate) fn densified_budgeted(
        &self,
        fraction: f64,
        budget: &mut ExpansionBudget,
    ) -> Result<Self> {
        Ok(match self {
            Self::Point(_) | Self::MultiPoint(_) | Self::Empty(..) => self.clone(),
            Self::LineString(points) => Self::LineString(LineSeq::from_trusted(
                densify_points_budgeted(points, fraction, budget)?,
            )),
            Self::MultiLineString(lines) => Self::MultiLineString(
                lines
                    .iter()
                    .map(|line| {
                        Ok(LineSeq::from_trusted(densify_points_budgeted(
                            line, fraction, budget,
                        )?))
                    })
                    .collect::<Result<_>>()?,
            ),
            Self::Polygon(polygon) => Self::Polygon(polygon.densified_budgeted(fraction, budget)?),
            Self::MultiPolygon(polygons) => Self::MultiPolygon(
                polygons
                    .iter()
                    .map(|polygon| polygon.densified_budgeted(fraction, budget))
                    .collect::<Result<_>>()?,
            ),
            Self::GeometryCollection(geometries) => Self::GeometryCollection(
                geometries
                    .iter()
                    .map(|geometry| geometry.densified_budgeted(fraction, budget))
                    .collect::<Result<_>>()?,
            ),
        })
    }

    /// Planar `segmentize` — the CRS-free reading. CRS-aware callers resolve a
    /// [`SegmentPlacement`] first and use [`Self::segmentize_budgeted`].
    pub fn segmentize(&self, max_segment_length: f64) -> Result<Self> {
        let mut budget = ExpansionBudget::new("segmentize", "max_segment_length");
        self.segmentize_budgeted(max_segment_length, SegmentPlacement::Planar, &mut budget)
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
            Self::LineString(points) => Self::LineString(LineSeq::from_trusted(
                segmentize_points_budgeted(points, max_segment_length, placement, budget)?,
            )),
            Self::MultiLineString(lines) => Self::MultiLineString(
                lines
                    .iter()
                    .map(|line| {
                        Ok(LineSeq::from_trusted(segmentize_points_budgeted(
                            line,
                            max_segment_length,
                            placement,
                            budget,
                        )?))
                    })
                    .collect::<Result<_>>()?,
            ),
            Self::Polygon(polygon) => {
                Self::Polygon(polygon.segmentize_budgeted(max_segment_length, placement, budget)?)
            },
            Self::MultiPolygon(polygons) => Self::MultiPolygon(
                polygons
                    .iter()
                    .map(|polygon| {
                        polygon.segmentize_budgeted(max_segment_length, placement, budget)
                    })
                    .collect::<Result<_>>()?,
            ),
            Self::GeometryCollection(geometries) => Self::GeometryCollection(
                geometries
                    .iter()
                    .map(|geometry| {
                        geometry.segmentize_budgeted(max_segment_length, placement, budget)
                    })
                    .collect::<Result<_, _>>()?,
            ),
            Self::Empty(..) => self.clone(),
        })
    }

    /// Swap the X and Y ordinates of every coordinate (Z/M untouched) —
    /// the axis-order repair for latitude/longitude-ordered data.
    pub fn swap_xy(&self) -> Self {
        // O(1) column swap per sequence (Arc-pointer swap, no per-vertex copy);
        // same coordinates swapped, bit-identical to a `(x, y) → (y, x)` map.
        // Infallible: the `Ok`/`?` wrapping collapses away in codegen.
        match self.try_map_coordseqs(
            |seq| Ok::<_, Infallible>(seq.swap_xy()),
            |point| Ok(point.with_xy_unchecked(point.y, point.x)),
        ) {
            Ok(shape) => shape,
            Err(infallible) => match infallible {},
        }
    }

    pub fn reverse(&self) -> Self {
        match self {
            Self::Point(point) => Self::Point(*point),
            Self::MultiPoint(points) => Self::MultiPoint(points.clone()),
            // Columnar reverse-copies — no AoS `Vec<Point>` round-trip.
            Self::LineString(points) => Self::LineString(LineSeq::from_trusted(points.reversed())),
            Self::MultiLineString(lines) => Self::MultiLineString(
                lines
                    .iter()
                    .map(|line| LineSeq::from_trusted(line.reversed()))
                    .collect(),
            ),
            Self::Polygon(polygon) => Self::Polygon(polygon.reverse()),
            Self::MultiPolygon(polygons) => {
                Self::MultiPolygon(polygons.iter().map(Polygon::reverse).collect())
            },
            Self::GeometryCollection(geometries) => {
                Self::GeometryCollection(geometries.iter().map(Self::reverse).collect())
            },
            Self::Empty(..) => self.clone(),
        }
    }

    pub fn orient_polygons(&self, exterior_cw: bool) -> Self {
        match self {
            Self::Polygon(polygon) => Self::Polygon(polygon.orient(exterior_cw)),
            Self::MultiPolygon(polygons) => Self::MultiPolygon(
                polygons
                    .iter()
                    .map(|polygon| polygon.orient(exterior_cw))
                    .collect(),
            ),
            Self::GeometryCollection(geometries) => Self::GeometryCollection(
                geometries
                    .iter()
                    .map(|geometry| geometry.orient_polygons(exterior_cw))
                    .collect(),
            ),
            Self::Point(_)
            | Self::MultiPoint(_)
            | Self::LineString(_)
            | Self::MultiLineString(_)
            | Self::Empty(..) => self.clone(),
        }
    }

    pub fn normalize(&self) -> Self {
        match self {
            Self::Point(point) => Self::Point(*point),
            Self::MultiPoint(points) => {
                let mut points = points.to_vec();
                points.sort_by(compare_points);
                Self::MultiPoint(points.into())
            },
            Self::LineString(points) => Self::LineString(LineSeq::from_trusted(CoordSeq::from(
                normalized_line(points),
            ))),
            Self::MultiLineString(lines) => {
                let mut lines = lines
                    .iter()
                    .map(|line| LineSeq::from_trusted(CoordSeq::from(normalized_line(line))))
                    .collect::<Vec<_>>();
                lines.sort_by(compare_point_slices);
                Self::MultiLineString(lines)
            },
            Self::Polygon(polygon) => Self::Polygon(polygon.normalize()),
            Self::MultiPolygon(polygons) => {
                let mut polygons = polygons.iter().map(Polygon::normalize).collect::<Vec<_>>();
                polygons.sort_by(compare_polygons);
                Self::MultiPolygon(polygons)
            },
            Self::GeometryCollection(geometries) => {
                let mut geometries = geometries.iter().map(Self::normalize).collect::<Vec<_>>();
                geometries.sort_by(compare_shapes);
                Self::GeometryCollection(geometries)
            },
            Self::Empty(..) => self.clone(),
        }
    }
}
