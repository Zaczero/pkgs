#![expect(
    clippy::similar_names,
    reason = "axis and formula symbols name paired quantities here (minx/miny, xs/ys, lat_rad/lam_rad, step/stop), which this lint reads as typos"
)]
use std::collections::HashMap;

use crate::NonNegative;
use crate::geometry::constructive::{
    Result, SegmentPlacement, decimal_scale, normalized_line, quantize_column_simd,
    remove_repeated_line_points, remove_repeated_points, remove_repeated_ring_points,
    segmentize_points_budgeted, snap_column_simd, split,
};
use crate::geometry::exact_arith::product_sum::ExactSum;
use crate::geometry::exact_arith::{
    DekkerParts, binary_zero, checked_normal_sum, floor_binary_exponent, normal_or_zero,
    two_product_dekker_admitted, two_product_dekker_checked_parts, two_sum,
};
use crate::geometry::overlay::{
    OverlayOp, overlay_all_geographic_from_with_metric, overlay_intersection_with_rectangle,
    overlay_view_error,
};
use crate::geometry::{
    Bounds, CoordSeq, CoordinateAxes, ExpansionBudget, GeometryErrorKind, GeometryKind, HasM, HasZ,
    LineSeq, MOrdinate, MultiPoint, MultiPolygon, OrdinateAxis, Point, Polygon, ReduceSimd, Shape,
    ShapeOwnerKey, Strictness, ZOrdinate, axis_pow2_scale, column_all_finite, compare_point_slices,
    compare_points, compare_polygons, compare_shapes, same_topological_coordinate,
    set_point_ordinate, simd_xy_map_f64, simd_xy_map_one_f64,
};

/// A parsed `origin` argument for the ergonomic affine helpers: parsed
/// once per call, resolved per shape — which also lets array bindings
/// notice when the origin is per-shape (`Centroid`/`Center`, an identity
/// for packed point rows) versus one fixed pivot.
#[derive(Clone, Copy)]
pub enum OriginSpec {
    Centroid,
    Center,
    Fixed(f64, f64),
}

impl OriginSpec {
    pub(crate) fn resolve_shape(self, shape: &Shape) -> crate::error::Result<(f64, f64)> {
        Ok(match self {
            Self::Centroid => shape.centroid_xy()?,
            Self::Center => shape.bounds_center_xy(),
            Self::Fixed(x, y) => (x, y),
        })
    }
}

/// Expand the fixed residual merge at its call site; omitted low limbs accrue
/// in `error`, and checked forms use `?` when an intermediate is unsafe.
macro_rules! add_affine_expansion_product {
    ($high:ident, $low:ident, $error:ident, $product:ident, $product_error:ident) => {{
        let (sum, sum_error) = checked_two_sum($high, $product)?;
        let (low_sum, low_error) = checked_two_sum($low, $product_error)?;
        let (merged_error, discarded_high) = checked_two_sum(sum_error, low_sum)?;
        let (next_high, next_residual) = checked_two_sum(sum, merged_error)?;
        $error = upper_sum($error, discarded_high.abs())?;
        $error = upper_sum($error, low_error.abs())?;
        $high = next_high;
        $low = next_residual;
    }};
}

/// Use a prepared matrix split and checked pivot split during transform setup.
macro_rules! add_affine_product_parts {
    ($high:ident, $low:ident, $error:ident, $lhs:expr, $rhs:expr) => {{
        let (product, product_error) = two_product_dekker_checked_parts($lhs, $rhs)?;
        add_affine_expansion_product!($high, $low, $error, product, product_error);
    }};
}

/// Inline the admitted product and residual merge into the coordinate loop.
macro_rules! add_affine_product_admitted {
    ($high:ident, $low:ident, $error:ident, $lhs:expr, $rhs:expr) => {{
        let (product, product_error) = two_product_dekker_admitted($lhs, $rhs);
        let (sum, sum_error) = two_sum($high, product);
        let (low_sum, low_error) = two_sum($low, product_error);
        let (merged_error, discarded_high) = two_sum(sum_error, low_sum);
        let (next_high, next_residual) = two_sum(sum, merged_error);
        $error = upper_sum_admitted($error, discarded_high.abs());
        $error = upper_sum_admitted($error, low_error.abs());
        $high = next_high;
        $low = next_residual;
    }};
}

#[derive(Clone, Copy)]
struct AffineExpansion {
    high: f64,
    low: f64,
    error: f64,
}

#[derive(Clone, Copy)]
struct ExponentExtent {
    min: Option<i32>,
    max: Option<i32>,
}

impl ExponentExtent {
    fn split_safe(self) -> bool {
        self.min.is_none_or(|exponent| exponent >= -970)
            && self.max.is_none_or(|exponent| exponent <= 995)
    }
}

#[derive(Clone, Copy)]
struct MagnitudeExtent {
    min_bits: u64,
    max_bits: u64,
}

impl MagnitudeExtent {
    const EMPTY: Self = Self {
        min_bits: 0x7FF0_0000_0000_0000,
        max_bits: 0,
    };

    fn include(&mut self, value: f64) -> bool {
        let magnitude_bits = value.to_bits() & 0x7FFF_FFFF_FFFF_FFFF;
        if magnitude_bits >= 0x7FF0_0000_0000_0000 {
            return false;
        }
        if magnitude_bits != 0 {
            self.min_bits = self.min_bits.min(magnitude_bits);
            self.max_bits = self.max_bits.max(magnitude_bits);
        }
        true
    }

    fn exponents(self) -> ExponentExtent {
        if self.min_bits == 0x7FF0_0000_0000_0000 {
            return ExponentExtent {
                min: None,
                max: None,
            };
        }
        ExponentExtent {
            min: Some(floor_binary_exponent(f64::from_bits(self.min_bits))),
            max: Some(floor_binary_exponent(f64::from_bits(self.max_bits))),
        }
    }
}

#[derive(Clone, Copy)]
struct AffineRowExtents {
    x: ExponentExtent,
    y: ExponentExtent,
}

impl AffineRowExtents {
    fn from_columns(xs: &[f64], ys: &[f64]) -> Option<Self> {
        debug_assert_eq!(xs.len(), ys.len());
        let mut x = MagnitudeExtent::EMPTY;
        let mut y = MagnitudeExtent::EMPTY;
        for (&x_value, &y_value) in xs.iter().zip(ys) {
            if !x.include(x_value) || !y.include(y_value) {
                return None;
            }
        }
        Some(Self {
            x: x.exponents(),
            y: y.exponents(),
        })
    }
}

#[derive(Clone, Copy)]
struct AffineOriginRow {
    coefficient_x: f64,
    coefficient_y: f64,
    coefficient_x_parts: Option<DekkerParts>,
    coefficient_y_parts: Option<DekkerParts>,
    output_origin: f64,
    origin_x: f64,
    origin_y: f64,
    bias: Option<AffineExpansion>,
}

impl AffineOriginRow {
    fn new(
        coefficient_x: f64,
        coefficient_y: f64,
        output_origin: f64,
        origin_x: f64,
        origin_y: f64,
        coefficient_x_parts: Option<DekkerParts>,
        coefficient_y_parts: Option<DekkerParts>,
        origin_x_parts: Option<DekkerParts>,
        origin_y_parts: Option<DekkerParts>,
        output_origin_parts: Option<DekkerParts>,
    ) -> Self {
        Self {
            coefficient_x,
            coefficient_y,
            coefficient_x_parts,
            coefficient_y_parts,
            output_origin,
            origin_x,
            origin_y,
            bias: affine_origin_bias(
                output_origin_parts,
                coefficient_x_parts,
                coefficient_y_parts,
                origin_x_parts,
                origin_y_parts,
            ),
        }
    }

    fn fast_map_finite(&self, value_x: f64, value_y: f64) -> Option<f64> {
        let AffineExpansion {
            mut high,
            mut low,
            mut error,
        } = self.bias?;
        let value_x_parts = DekkerParts::checked(value_x)?;
        let value_y_parts = DekkerParts::checked(value_y)?;
        add_affine_product_parts!(high, low, error, self.coefficient_x_parts?, value_x_parts);
        add_affine_product_parts!(high, low, error, self.coefficient_y_parts?, value_y_parts);
        let (candidate, certified) = round_certified_double_double::<false>(high, low, error);
        certified.then_some(candidate)
    }

    fn map_finite(&self, value_x: f64, value_y: f64) -> Result<f64> {
        if let Some(value) = self.fast_map_finite(value_x, value_y) {
            return Ok(value);
        }
        self.exact_map_finite(value_x, value_y)
    }

    fn exact_map_finite(&self, value_x: f64, value_y: f64) -> Result<f64> {
        exact_affine_origin_ordinate(
            self.coefficient_x,
            self.coefficient_y,
            self.output_origin,
            self.origin_x,
            self.origin_y,
            value_x,
            value_y,
        )
    }

    fn admitted(
        &self,
        x_extent: ExponentExtent,
        y_extent: ExponentExtent,
    ) -> Option<AdmittedAffineOriginRow> {
        let coefficient_x_parts = self.coefficient_x_parts?;
        let coefficient_y_parts = self.coefficient_y_parts?;
        let bias = self.bias?;
        if !x_extent.split_safe() || !y_extent.split_safe() {
            return None;
        }
        if !affine_product_exponents_admitted(coefficient_x_parts, x_extent)
            || !affine_product_exponents_admitted(coefficient_y_parts, y_extent)
        {
            return None;
        }
        if !affine_expansion_admitted(bias) {
            return None;
        }
        let x_is_zero = coefficient_x_parts.is_zero();
        let y_is_zero = coefficient_y_parts.is_zero();
        let bias_is_zero =
            binary_zero(bias.high) && binary_zero(bias.low) && binary_zero(bias.error);
        let mode = match (bias_is_zero, x_is_zero, y_is_zero) {
            (true, false, true) => AdmittedAffineMode::ProductX(coefficient_x_parts),
            (true, true, false) => AdmittedAffineMode::ProductY(coefficient_y_parts),
            (true, false, false) => {
                AdmittedAffineMode::Dot(coefficient_x_parts, coefficient_y_parts)
            },
            (true, true, true) => AdmittedAffineMode::Zero,
            (false, true, true) => AdmittedAffineMode::BiasOnly(bias),
            (false, false, true) => AdmittedAffineMode::BiasedProductX(coefficient_x_parts, bias),
            (false, true, false) => AdmittedAffineMode::BiasedProductY(coefficient_y_parts, bias),
            (false, false, false) => {
                AdmittedAffineMode::General(coefficient_x_parts, coefficient_y_parts, bias)
            },
        };
        Some(AdmittedAffineOriginRow { mode })
    }
}

#[derive(Clone, Copy)]
enum AdmittedAffineMode {
    Zero,
    ProductX(DekkerParts),
    ProductY(DekkerParts),
    BiasOnly(AffineExpansion),
    BiasedProductX(DekkerParts, AffineExpansion),
    BiasedProductY(DekkerParts, AffineExpansion),
    Dot(DekkerParts, DekkerParts),
    General(DekkerParts, DekkerParts, AffineExpansion),
}

#[derive(Clone, Copy)]
struct AdmittedAffineOriginRow {
    mode: AdmittedAffineMode,
}

impl AdmittedAffineOriginRow {
    const fn requires_coordinate_split(&self) -> bool {
        matches!(
            &self.mode,
            AdmittedAffineMode::BiasedProductX(..)
                | AdmittedAffineMode::BiasedProductY(..)
                | AdmittedAffineMode::Dot(..)
                | AdmittedAffineMode::General(..)
        )
    }

    fn map(
        self,
        value_x: f64,
        value_y: f64,
        parts_x: DekkerParts,
        parts_y: DekkerParts,
    ) -> (f64, bool) {
        match self.mode {
            AdmittedAffineMode::Zero => (0.0, true),
            AdmittedAffineMode::ProductX(coefficient) => {
                (single_product(coefficient.value(), value_x), true)
            },
            AdmittedAffineMode::ProductY(coefficient) => {
                (single_product(coefficient.value(), value_y), true)
            },
            AdmittedAffineMode::BiasOnly(bias) => {
                round_certified_double_double::<true>(bias.high, bias.low, bias.error)
            },
            AdmittedAffineMode::BiasedProductX(coefficient, bias) => {
                map_biased_product(bias, coefficient, parts_x)
            },
            AdmittedAffineMode::BiasedProductY(coefficient, bias) => {
                map_biased_product(bias, coefficient, parts_y)
            },
            AdmittedAffineMode::Dot(coefficient_x, coefficient_y) => {
                map_exact_dot(coefficient_x, coefficient_y, parts_x, parts_y)
            },
            AdmittedAffineMode::General(coefficient_x, coefficient_y, bias) => {
                map_general_affine_row(bias, coefficient_x, coefficient_y, parts_x, parts_y)
            },
        }
    }
}

/// A sole exact product rounds correctly with one binary64 multiply. ExactSum
/// canonicalizes a mathematically zero row to +0, unlike signed-zero multiply.
fn single_product(coefficient: f64, value: f64) -> f64 {
    let product = coefficient * value;
    if binary_zero(value) { 0.0 } else { product }
}

fn map_biased_product(
    bias: AffineExpansion,
    coefficient: DekkerParts,
    value: DekkerParts,
) -> (f64, bool) {
    let AffineExpansion {
        mut high,
        mut low,
        mut error,
    } = bias;
    add_affine_product_admitted!(high, low, error, coefficient, value);
    round_certified_double_double::<true>(high, low, error)
}

/// Compact two exact products; the residuals discarded from the low-limb
/// merge are added to the outward error bound.
fn map_exact_dot(
    coefficient_x: DekkerParts,
    coefficient_y: DekkerParts,
    value_x: DekkerParts,
    value_y: DekkerParts,
) -> (f64, bool) {
    let (product_x, error_x) = two_product_dekker_admitted(coefficient_x, value_x);
    let (product_y, error_y) = two_product_dekker_admitted(coefficient_y, value_y);
    let (head, head_error) = two_sum(product_x, product_y);
    let (product_tail, discarded_products) = two_sum(error_x, error_y);
    let (merged_tail, discarded_merge) = two_sum(head_error, product_tail);
    let (high, low) = two_sum(head, merged_tail);
    let error = upper_sum_admitted(0.0, discarded_products.abs());
    let error = upper_sum_admitted(error, discarded_merge.abs());
    round_certified_double_double::<true>(high, low, error)
}

fn map_general_affine_row(
    bias: AffineExpansion,
    coefficient_x: DekkerParts,
    coefficient_y: DekkerParts,
    value_x: DekkerParts,
    value_y: DekkerParts,
) -> (f64, bool) {
    let AffineExpansion {
        mut high,
        mut low,
        mut error,
    } = bias;
    add_affine_product_admitted!(high, low, error, coefficient_x, value_x);
    add_affine_product_admitted!(high, low, error, coefficient_y, value_y);
    round_certified_double_double::<true>(high, low, error)
}

const MIN_ADMITTED_PRODUCT_EXPONENT: i32 = -970;
const MAX_ADMITTED_PRODUCT_EXPONENT: i32 = 850;
const MAX_ADMITTED_ACCUMULATOR_EXPONENT: i32 = 860;
// A nonidentity affine output is finite or the operation errors. This NaN
// payload is therefore an in-buffer fixup tag, avoiding per-row scratch.
const AFFINE_FIXUP_MARKER: f64 = f64::from_bits(0x7FF8_0000_0000_0001);

// A binary64 with floor exponent e is a multiple of 2^(e-52). Dekker's split
// keeps both parts on that lattice, so every partial product in a two-product
// transform is a multiple of 2^(e_lhs+e_rhs-104). Requiring a product exponent
// sum of at least -970 keeps that lattice on or above 2^-1074, the minimum
// subnormal quantum; at -971 a half-subnormal residual can round away (the
// boundary witness below). Coordinate exponents [-970, 995] also keep the
// splitter product finite and every nonzero split limb normal. At the upper
// product sum of 850, each product stays below 2^852; the admitted bias limbs
// stay below 2^861, so their fixed-size accumulation remains far from overflow.
fn affine_product_exponents_admitted(coefficient: DekkerParts, coordinate: ExponentExtent) -> bool {
    if coefficient.is_zero() {
        return true;
    }
    let (Some(min_coordinate), Some(max_coordinate)) = (coordinate.min, coordinate.max) else {
        return true;
    };
    let Some(min_product_exponent) = coefficient.exponent().checked_add(min_coordinate) else {
        return false;
    };
    let Some(max_product_exponent) = coefficient.exponent().checked_add(max_coordinate) else {
        return false;
    };
    (MIN_ADMITTED_PRODUCT_EXPONENT..=MAX_ADMITTED_PRODUCT_EXPONENT).contains(&min_product_exponent)
        && max_product_exponent <= MAX_ADMITTED_PRODUCT_EXPONENT
}

fn affine_expansion_admitted(expansion: AffineExpansion) -> bool {
    [expansion.high, expansion.low, expansion.error]
        .into_iter()
        .all(|value| {
            normal_or_zero(value)
                && (binary_zero(value)
                    || floor_binary_exponent(value.abs()) <= MAX_ADMITTED_ACCUMULATOR_EXPONENT)
        })
}

#[expect(
    clippy::arithmetic_side_effects,
    reason = "SIMD affine lanes preserve the declared multiply-add expression order"
)]
fn affine_ordinate_simd(
    first: ReduceSimd,
    second: ReduceSimd,
    offset: ReduceSimd,
    x: ReduceSimd,
    y: ReduceSimd,
) -> ReduceSimd {
    first * x + second * y + offset
}

pub(crate) struct AffineLinear {
    matrix: [f64; 4],
    coefficient_parts: [Option<DekkerParts>; 4],
    preserved: [bool; 2],
}

impl AffineLinear {
    pub(crate) fn new(matrix: [f64; 4]) -> Result<Self> {
        if !matrix.iter().all(|value| value.is_finite()) {
            return Err(GeometryErrorKind::NonFiniteCoordinate.into());
        }
        let [m00, m01, m10, m11] = matrix;
        Ok(Self {
            matrix,
            coefficient_parts: matrix.map(DekkerParts::checked),
            preserved: [
                same_topological_coordinate(m00, 1.0) && same_topological_coordinate(m01, 0.0),
                same_topological_coordinate(m10, 0.0) && same_topological_coordinate(m11, 1.0),
            ],
        })
    }

    pub(crate) const fn is_identity(&self) -> bool {
        let [preserve_x, preserve_y] = self.preserved;
        preserve_x && preserve_y
    }

    pub(crate) fn at(&self, origin: (f64, f64)) -> Result<OriginAffine> {
        if !origin.0.is_finite() || !origin.1.is_finite() {
            return Err(GeometryErrorKind::NonFiniteCoordinate.into());
        }
        let origin_x_parts = DekkerParts::checked(origin.0);
        let origin_y_parts = DekkerParts::checked(origin.1);
        Ok(OriginAffine {
            preserved: self.preserved,
            rows: [
                (!self.preserved[0]).then(|| {
                    AffineOriginRow::new(
                        self.matrix[0],
                        self.matrix[1],
                        origin.0,
                        origin.0,
                        origin.1,
                        self.coefficient_parts[0],
                        self.coefficient_parts[1],
                        origin_x_parts,
                        origin_y_parts,
                        origin_x_parts,
                    )
                }),
                (!self.preserved[1]).then(|| {
                    AffineOriginRow::new(
                        self.matrix[2],
                        self.matrix[3],
                        origin.1,
                        origin.0,
                        origin.1,
                        self.coefficient_parts[2],
                        self.coefficient_parts[3],
                        origin_x_parts,
                        origin_y_parts,
                        origin_y_parts,
                    )
                }),
            ],
        })
    }
}

pub(crate) struct OriginAffine {
    preserved: [bool; 2],
    rows: [Option<AffineOriginRow>; 2],
}

impl OriginAffine {
    pub(crate) fn map_xy(&self, x: f64, y: f64) -> Result<(f64, f64)> {
        let [x_row, y_row] = &self.rows;
        if (x_row.is_some() || y_row.is_some()) && (!x.is_finite() || !y.is_finite()) {
            return Err(GeometryErrorKind::NonFiniteCoordinate.into());
        }
        let mapped_x = x_row.as_ref().map_or(Ok(x), |row| row.map_finite(x, y))?;
        let mapped_y = y_row.as_ref().map_or(Ok(y), |row| row.map_finite(x, y))?;
        Ok((mapped_x, mapped_y))
    }

    pub(crate) fn map_xy_columns(
        &self,
        xs: &[f64],
        ys: &[f64],
        out_x: Option<&mut [f64]>,
        out_y: Option<&mut [f64]>,
    ) -> Result<()> {
        debug_assert_eq!(xs.len(), ys.len());
        let out_x_len = out_x.as_ref().map(|column| column.len());
        let out_y_len = out_y.as_ref().map(|column| column.len());
        debug_assert!(out_x_len.is_none_or(|length| length == xs.len()));
        debug_assert!(out_y_len.is_none_or(|length| length == ys.len()));
        let Some(extents) = AffineRowExtents::from_columns(xs, ys) else {
            return self.map_xy_columns_checked(xs, ys, out_x, out_y);
        };
        let [x_row, y_row] = &self.rows;
        let admitted_x = x_row
            .as_ref()
            .and_then(|row| row.admitted(extents.x, extents.y));
        let admitted_y = y_row
            .as_ref()
            .and_then(|row| row.admitted(extents.x, extents.y));
        if (x_row.is_some() && admitted_x.is_none()) || (y_row.is_some() && admitted_y.is_none()) {
            return self.map_xy_columns_checked(xs, ys, out_x, out_y);
        }
        self.map_xy_columns_admitted(
            xs,
            ys,
            out_x,
            out_y,
            admitted_x.as_ref(),
            admitted_y.as_ref(),
        )
    }

    fn map_xy_columns_checked(
        &self,
        xs: &[f64],
        ys: &[f64],
        out_x: Option<&mut [f64]>,
        out_y: Option<&mut [f64]>,
    ) -> Result<()> {
        let [x_row, y_row] = &self.rows;
        match (x_row, y_row, out_x, out_y) {
            (Some(_), Some(_), Some(out_x), Some(out_y)) => {
                for (((&x, &y), out_x), out_y) in xs
                    .iter()
                    .zip(ys)
                    .zip(out_x.iter_mut())
                    .zip(out_y.iter_mut())
                {
                    (*out_x, *out_y) = self.map_xy(x, y)?;
                }
            },
            (Some(_), None, Some(out_x), out_y) => {
                for ((&x, &y), out_x) in xs.iter().zip(ys).zip(out_x.iter_mut()) {
                    *out_x = self.map_xy(x, y)?.0;
                }
                if let Some(out_y) = out_y {
                    out_y.copy_from_slice(ys);
                }
            },
            (None, Some(_), out_x, Some(out_y)) => {
                for ((&x, &y), out_y) in xs.iter().zip(ys).zip(out_y.iter_mut()) {
                    *out_y = self.map_xy(x, y)?.1;
                }
                if let Some(out_x) = out_x {
                    out_x.copy_from_slice(xs);
                }
            },
            (None, None, out_x, out_y) => {
                if let Some(out_x) = out_x {
                    out_x.copy_from_slice(xs);
                }
                if let Some(out_y) = out_y {
                    out_y.copy_from_slice(ys);
                }
            },
            _ => unreachable!("nonidentity affine axes have output columns"),
        }
        Ok(())
    }

    fn map_xy_columns_admitted(
        &self,
        xs: &[f64],
        ys: &[f64],
        out_x: Option<&mut [f64]>,
        out_y: Option<&mut [f64]>,
        admitted_x: Option<&AdmittedAffineOriginRow>,
        admitted_y: Option<&AdmittedAffineOriginRow>,
    ) -> Result<()> {
        let [x_row, y_row] = &self.rows;
        match (x_row, y_row, admitted_x, admitted_y, out_x, out_y) {
            (Some(x_row), Some(y_row), Some(x_fast), Some(y_fast), Some(out_x), Some(out_y)) => {
                map_admitted_xy_columns(xs, ys, out_x, out_y, x_row, y_row, x_fast, y_fast)
            },
            (Some(x_row), None, Some(x_fast), None, Some(out_x), out_y) => {
                map_admitted_one_column(xs, ys, out_x, x_row, x_fast)?;
                if let Some(out_y) = out_y {
                    out_y.copy_from_slice(ys);
                }
                Ok(())
            },
            (None, Some(y_row), None, Some(y_fast), out_x, Some(out_y)) => {
                map_admitted_one_column(xs, ys, out_y, y_row, y_fast)?;
                if let Some(out_x) = out_x {
                    out_x.copy_from_slice(xs);
                }
                Ok(())
            },
            (None, None, None, None, out_x, out_y) => {
                if let Some(out_x) = out_x {
                    out_x.copy_from_slice(xs);
                }
                if let Some(out_y) = out_y {
                    out_y.copy_from_slice(ys);
                }
                Ok(())
            },
            _ => unreachable!("row admission and output axes share one affine transform"),
        }
    }
}

fn upper_sum_admitted(lhs: f64, rhs: f64) -> f64 {
    let sum = lhs + rhs;
    if binary_zero(sum) { 0.0 } else { sum.next_up() }
}

fn map_admitted_xy_columns(
    xs: &[f64],
    ys: &[f64],
    out_x: &mut [f64],
    out_y: &mut [f64],
    x_row: &AffineOriginRow,
    y_row: &AffineOriginRow,
    x_fast: &AdmittedAffineOriginRow,
    y_fast: &AdmittedAffineOriginRow,
) -> Result<()> {
    if x_fast.requires_coordinate_split() || y_fast.requires_coordinate_split() {
        map_admitted_xy_columns_kernel::<true>(xs, ys, out_x, out_y, x_row, y_row, x_fast, y_fast)
    } else {
        map_admitted_xy_columns_kernel::<false>(xs, ys, out_x, out_y, x_row, y_row, x_fast, y_fast)
    }
}

fn map_admitted_xy_columns_kernel<const SPLIT_COORDINATES: bool>(
    xs: &[f64],
    ys: &[f64],
    out_x: &mut [f64],
    out_y: &mut [f64],
    x_row: &AffineOriginRow,
    y_row: &AffineOriginRow,
    x_fast: &AdmittedAffineOriginRow,
    y_fast: &AdmittedAffineOriginRow,
) -> Result<()> {
    for (((&x, &y), output_x), output_y) in xs
        .iter()
        .zip(ys)
        .zip(out_x.iter_mut())
        .zip(out_y.iter_mut())
    {
        let (x_parts, y_parts) = if SPLIT_COORDINATES {
            (DekkerParts::admitted(x), DekkerParts::admitted(y))
        } else {
            (DekkerParts::zero(), DekkerParts::zero())
        };
        let (mapped_x, certified_x) = x_fast.map(x, y, x_parts, y_parts);
        let (mapped_y, certified_y) = y_fast.map(x, y, x_parts, y_parts);
        *output_x = if certified_x {
            mapped_x
        } else {
            AFFINE_FIXUP_MARKER
        };
        *output_y = if certified_y {
            mapped_y
        } else {
            AFFINE_FIXUP_MARKER
        };
    }
    for (((&x, &y), output_x), output_y) in xs
        .iter()
        .zip(ys)
        .zip(out_x.iter_mut())
        .zip(out_y.iter_mut())
    {
        if output_x.to_bits() == AFFINE_FIXUP_MARKER.to_bits() {
            *output_x = x_row.exact_map_finite(x, y)?;
        }
        if output_y.to_bits() == AFFINE_FIXUP_MARKER.to_bits() {
            *output_y = y_row.exact_map_finite(x, y)?;
        }
    }
    Ok(())
}

fn map_admitted_one_column(
    xs: &[f64],
    ys: &[f64],
    output: &mut [f64],
    exact_row: &AffineOriginRow,
    fast_row: &AdmittedAffineOriginRow,
) -> Result<()> {
    if fast_row.requires_coordinate_split() {
        map_admitted_one_column_kernel::<true>(xs, ys, output, exact_row, fast_row)
    } else {
        map_admitted_one_column_kernel::<false>(xs, ys, output, exact_row, fast_row)
    }
}

fn map_admitted_one_column_kernel<const SPLIT_COORDINATES: bool>(
    xs: &[f64],
    ys: &[f64],
    output: &mut [f64],
    exact_row: &AffineOriginRow,
    fast_row: &AdmittedAffineOriginRow,
) -> Result<()> {
    for ((&x, &y), output) in xs.iter().zip(ys).zip(output.iter_mut()) {
        let (x_parts, y_parts) = if SPLIT_COORDINATES {
            (DekkerParts::admitted(x), DekkerParts::admitted(y))
        } else {
            (DekkerParts::zero(), DekkerParts::zero())
        };
        let (mapped, certified) = fast_row.map(x, y, x_parts, y_parts);
        *output = if certified {
            mapped
        } else {
            AFFINE_FIXUP_MARKER
        };
    }
    for ((&x, &y), output) in xs.iter().zip(ys).zip(output.iter_mut()) {
        if output.to_bits() == AFFINE_FIXUP_MARKER.to_bits() {
            *output = exact_row.exact_map_finite(x, y)?;
        }
    }
    Ok(())
}

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
        placement: &SegmentPlacement<'_>,
        budget: &mut ExpansionBudget,
    ) -> Result<Self> {
        crate::geometry::densify_points_budgeted_with_placement(self, fraction, placement, budget)
            .map(Self::from_proven)
    }

    pub(crate) fn segmentize(
        &self,
        max_segment_length: f64,
        placement: &SegmentPlacement<'_>,
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

fn exact_affine_origin_ordinate(
    coefficient_x: f64,
    coefficient_y: f64,
    output_origin: f64,
    origin_x: f64,
    origin_y: f64,
    value_x: f64,
    value_y: f64,
) -> Result<f64> {
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
#[cfg(test)]
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
    AffineOriginRow::new(
        coefficient_x,
        coefficient_y,
        output_origin,
        origin_x,
        origin_y,
        DekkerParts::checked(coefficient_x),
        DekkerParts::checked(coefficient_y),
        DekkerParts::checked(origin_x),
        DekkerParts::checked(origin_y),
        DekkerParts::checked(output_origin),
    )
    .map_finite(value_x, value_y)
}

/// Prepare the three invariant terms once per affine row. The accumulated
/// error bounds the exact bias outside the retained double-double.
fn affine_origin_bias(
    output_origin_parts: Option<DekkerParts>,
    coefficient_x_parts: Option<DekkerParts>,
    coefficient_y_parts: Option<DekkerParts>,
    origin_x_parts: Option<DekkerParts>,
    origin_y_parts: Option<DekkerParts>,
) -> Option<AffineExpansion> {
    let (mut high, mut low, mut error) = (0.0, 0.0, 0.0);
    let one = DekkerParts::checked(1.0)?;
    add_affine_product_parts!(high, low, error, output_origin_parts?, one);
    add_affine_product_parts!(
        high,
        low,
        error,
        coefficient_x_parts?.negated(),
        origin_x_parts?
    );
    add_affine_product_parts!(
        high,
        low,
        error,
        coefficient_y_parts?.negated(),
        origin_y_parts?
    );
    Some(AffineExpansion { high, low, error })
}

/// Correctly rounded fast path for the exact five-product origin-relative row.
/// Its invariant bias is prepared once; each coordinate adds only the two
/// variable products. Upward-rounded magnitudes bound all omitted limbs.
#[cfg(test)]
pub(super) fn fast_affine_about_origin_ordinate(
    coefficient_x: f64,
    coefficient_y: f64,
    output_origin: f64,
    origin_x: f64,
    origin_y: f64,
    value_x: f64,
    value_y: f64,
) -> Option<f64> {
    [
        coefficient_x,
        coefficient_y,
        output_origin,
        origin_x,
        origin_y,
        value_x,
        value_y,
    ]
    .into_iter()
    .all(f64::is_finite)
    .then(|| {
        AffineOriginRow::new(
            coefficient_x,
            coefficient_y,
            output_origin,
            origin_x,
            origin_y,
            DekkerParts::checked(coefficient_x),
            DekkerParts::checked(coefficient_y),
            DekkerParts::checked(origin_x),
            DekkerParts::checked(origin_y),
            DekkerParts::checked(output_origin),
        )
        .fast_map_finite(value_x, value_y)
    })
    .flatten()
}

fn checked_two_sum(lhs: f64, rhs: f64) -> Option<(f64, f64)> {
    if !normal_or_zero(lhs) || !normal_or_zero(rhs) {
        return None;
    }
    let (sum, error) = two_sum(lhs, rhs);
    (normal_or_zero(sum) && normal_or_zero(error)).then_some((sum, error))
}

/// Add nonnegative error magnitudes with an outward-rounded upper bound.
fn upper_sum(lhs: f64, rhs: f64) -> Option<f64> {
    if !normal_or_zero(lhs) || !normal_or_zero(rhs) {
        return None;
    }
    let sum = checked_normal_sum(lhs, rhs)?;
    if binary_zero(sum) {
        return Some(0.0);
    }
    let upper = sum.next_up();
    upper.is_normal().then_some(upper)
}

const fn next_down_finite(value: f64) -> f64 {
    const SIGN: u64 = 1 << 63;
    let bits = value.to_bits();
    let magnitude = bits & !SIGN;
    if magnitude == 0 {
        f64::from_bits(SIGN | 1)
    } else if bits & SIGN == 0 {
        f64::from_bits(bits.wrapping_sub(1))
    } else {
        f64::from_bits(bits.wrapping_add(1))
    }
}

const fn adjacent_finite_values(value: f64) -> (f64, f64) {
    const SIGN: u64 = 1 << 63;
    let bits = value.to_bits();
    let magnitude = bits & !SIGN;
    if magnitude == 0 {
        (f64::from_bits(SIGN | 1), f64::from_bits(1))
    } else if bits & SIGN == 0 {
        (
            f64::from_bits(bits.wrapping_sub(1)),
            f64::from_bits(bits.wrapping_add(1)),
        )
    } else {
        (
            f64::from_bits(bits.wrapping_add(1)),
            f64::from_bits(bits.wrapping_sub(1)),
        )
    }
}

fn lower_bound_certified<const ADMITTED: bool>(value: f64) -> (f64, bool) {
    if ADMITTED {
        (next_down_finite(value), true)
    } else if binary_zero(value) {
        (0.0, true)
    } else {
        let lower = value.next_down();
        (lower, value.is_normal() && normal_or_zero(lower))
    }
}

fn lower_sum_certified<const ADMITTED: bool>(lhs: f64, rhs: f64) -> (f64, bool) {
    let sum = lhs + rhs;
    let (lower, bound_is_valid) = lower_bound_certified::<ADMITTED>(sum);
    let valid = if ADMITTED {
        true
    } else {
        normal_or_zero(sum) && bound_is_valid
    };
    (lower, valid)
}

fn lower_difference_certified<const ADMITTED: bool>(lhs: f64, rhs: f64) -> (f64, bool) {
    let difference = lhs - rhs;
    let (lower, bound_is_valid) = lower_bound_certified::<ADMITTED>(difference);
    let valid = if ADMITTED {
        true
    } else {
        normal_or_zero(difference) && difference >= 0.0 && bound_is_valid
    };
    (lower, valid)
}

/// Accept only when `candidate + tail ± error` stays strictly inside its
/// asymmetric binary64 rounding cell. `ADMITTED` removes range checks already
/// proved by the row bounds; both lanes use the same strict cell inequalities.
fn round_certified_double_double<const ADMITTED: bool>(
    high: f64,
    low: f64,
    error: f64,
) -> (f64, bool) {
    let (candidate, tail) = two_sum(high, low);
    let (previous, next) = if ADMITTED {
        adjacent_finite_values(candidate)
    } else {
        (candidate.next_down(), candidate.next_up())
    };
    let lower_gap = candidate - previous;
    let upper_gap = next - candidate;
    let lower_half_gap = lower_gap * 0.5;
    let upper_half_gap = upper_gap * 0.5;
    let tail_magnitude = tail.abs();
    let (lower_margin, lower_is_valid, upper_margin, upper_is_valid) = if tail < 0.0 {
        let (lower, lower_valid) =
            lower_difference_certified::<ADMITTED>(lower_half_gap, tail_magnitude);
        let (upper, upper_valid) = lower_sum_certified::<ADMITTED>(upper_half_gap, tail_magnitude);
        (lower, lower_valid, upper, upper_valid)
    } else {
        let (lower, lower_valid) = lower_sum_certified::<ADMITTED>(lower_half_gap, tail_magnitude);
        let (upper, upper_valid) =
            lower_difference_certified::<ADMITTED>(upper_half_gap, tail_magnitude);
        (lower, lower_valid, upper, upper_valid)
    };
    let range_is_valid = if ADMITTED {
        true
    } else {
        normal_or_zero(high)
            && normal_or_zero(low)
            && normal_or_zero(tail)
            && previous.is_normal()
            && next.is_normal()
            && normal_or_zero(lower_gap)
            && normal_or_zero(upper_gap)
            && lower_half_gap.is_normal()
            && upper_half_gap.is_normal()
            && lower_is_valid
            && upper_is_valid
            && error >= 0.0
            && error.is_finite()
    };
    let certified =
        candidate.is_normal() && range_is_valid && error < lower_margin && error < upper_margin;
    (candidate, certified)
}

#[cfg(test)]
#[expect(
    clippy::items_after_test_module,
    reason = "keep affine rounding witnesses beside their private certificate helpers"
)]
mod affine_rounding_tests {
    use super::{
        AdmittedAffineMode, AdmittedAffineOriginRow, AffineExpansion, AffineLinear,
        AffineOriginRow, AffineRowExtents, DekkerParts, exact_affine_origin_ordinate,
    };
    use crate::geometry::exact_arith::product_sum::ExactSum;

    #[test]
    fn residual_bound_rejects_an_expansion_whose_exact_value_crosses_halfway() {
        // The retained sum is 2^-106 below halfway; the residual moves it
        // 2^-106 above halfway and therefore changes the rounded result.
        let low = 2.0_f64.powi(-53) - 2.0_f64.powi(-106);
        let residual = 2.0_f64.powi(-105);
        let row = AffineOriginRow {
            coefficient_x: 0.0,
            coefficient_y: 0.0,
            coefficient_x_parts: DekkerParts::checked(0.0),
            coefficient_y_parts: DekkerParts::checked(0.0),
            output_origin: 0.0,
            origin_x: 0.0,
            origin_y: 0.0,
            bias: Some(AffineExpansion {
                high: 1.0,
                low,
                error: residual,
            }),
        };
        let mut exact = ExactSum::zero();
        exact.add_product(1.0, 1.0);
        exact.add_product(low, 1.0);
        exact.add_product(residual, 1.0);
        assert_eq!(
            exact.round_to_binary64().to_bits(),
            1.0_f64.next_up().to_bits()
        );
        assert_eq!(row.fast_map_finite(0.0, 0.0), None);
    }

    #[test]
    fn row_admission_rejects_a_product_with_a_half_subnormal_tail() {
        let one_plus_ulp = 1.0 + 2.0_f64.powi(-52);
        let coefficient_x = one_plus_ulp * 2.0_f64.powi(-400);
        let value_x = one_plus_ulp * 2.0_f64.powi(-571);
        let coefficient_y = -0.5;
        let value_y = 2.0_f64.powi(-970) + 2.0_f64.powi(-1021);
        let origin_y = 2.0_f64.powi(-52);
        let row = AffineOriginRow::new(
            coefficient_x,
            coefficient_y,
            1.0,
            0.0,
            origin_y,
            DekkerParts::checked(coefficient_x),
            DekkerParts::checked(coefficient_y),
            DekkerParts::checked(0.0),
            DekkerParts::checked(origin_y),
            DekkerParts::checked(1.0),
        );
        let extents = AffineRowExtents::from_columns(&[value_x], &[value_y])
            .expect("the witness coordinates are finite");
        let expected = exact_affine_origin_ordinate(
            coefficient_x,
            coefficient_y,
            1.0,
            0.0,
            origin_y,
            value_x,
            value_y,
        )
        .unwrap();
        let (unchecked_candidate, unchecked_certified) = AdmittedAffineOriginRow {
            mode: AdmittedAffineMode::General(
                row.coefficient_x_parts.unwrap(),
                row.coefficient_y_parts.unwrap(),
                row.bias.unwrap(),
            ),
        }
        .map(
            value_x,
            value_y,
            DekkerParts::admitted(value_x),
            DekkerParts::admitted(value_y),
        );

        assert_eq!(expected.to_bits(), 1.0_f64.next_up().to_bits());
        assert_eq!(unchecked_candidate.to_bits(), 1.0_f64.to_bits());
        assert!(!unchecked_certified);
        assert_ne!(unchecked_candidate.to_bits(), expected.to_bits());
        assert!(
            row.admitted(extents.x, extents.y).is_none(),
            "admission would allow unchecked result {:#018x}, but ExactSum gives {:#018x}",
            unchecked_candidate.to_bits(),
            expected.to_bits(),
        );
    }

    #[test]
    fn admitted_coordinate_loop_matches_exact_rows_and_fixes_uncertain_vertices() {
        let xs = [
            2.0_f64.powi(-100),
            1.0,
            2.0_f64.powi(100),
            -(2.0_f64.powi(100)),
        ];
        let ys = [
            -(2.0_f64.powi(-80)),
            0.25,
            -(2.0_f64.powi(100)),
            2.0_f64.powi(99),
        ];
        let transform = AffineLinear::new([1.25, -0.75, 0.5, 2.0])
            .unwrap()
            .at((0.0, 0.0))
            .unwrap();
        let [Some(x_row), Some(y_row)] = &transform.rows else {
            panic!("both affine rows are nonidentity")
        };
        let extents = AffineRowExtents::from_columns(&xs, &ys).unwrap();
        assert!(x_row.admitted(extents.x, extents.y).is_some());
        assert!(y_row.admitted(extents.x, extents.y).is_some());

        let mut actual_x = vec![0.0; xs.len()];
        let mut actual_y = vec![0.0; ys.len()];
        transform
            .map_xy_columns(&xs, &ys, Some(&mut actual_x), Some(&mut actual_y))
            .unwrap();
        for (((&x, &y), &mapped_x), &mapped_y) in xs.iter().zip(&ys).zip(&actual_x).zip(&actual_y) {
            assert_eq!(
                mapped_x.to_bits(),
                exact_affine_origin_ordinate(1.25, -0.75, 0.0, 0.0, 0.0, x, y)
                    .unwrap()
                    .to_bits()
            );
            assert_eq!(
                mapped_y.to_bits(),
                exact_affine_origin_ordinate(0.5, 2.0, 0.0, 0.0, 0.0, x, y)
                    .unwrap()
                    .to_bits()
            );
        }

        let upper = 1.0_f64.next_up();
        let midpoint = AffineLinear::new([0.5, 0.0, 0.0, 1.0])
            .unwrap()
            .at((upper, 0.0))
            .unwrap();
        let [Some(x_row), None] = &midpoint.rows else {
            panic!("the X affine row is nonidentity and Y is preserved")
        };
        let (xs, ys) = ([1.0], [0.0]);
        let extents = AffineRowExtents::from_columns(&xs, &ys).unwrap();
        assert!(x_row.admitted(extents.x, extents.y).is_some());
        let mut actual_x = [0.0];
        midpoint
            .map_xy_columns(&xs, &ys, Some(&mut actual_x), None)
            .unwrap();
        let mapped_x = actual_x
            .first()
            .copied()
            .expect("the midpoint row has one coordinate");
        assert_eq!(
            mapped_x.to_bits(),
            exact_affine_origin_ordinate(0.5, 0.0, upper, upper, 0.0, 1.0, 0.0)
                .unwrap()
                .to_bits()
        );
        assert_eq!(mapped_x.to_bits(), 1.0_f64.to_bits());
    }

    #[test]
    fn origin_zero_single_product_rows_match_exact_product_rounding() {
        let xs = [-0.0, 2.0_f64.powi(-500), 1.0, -(2.0_f64.powi(100))];
        let ys = [0.0, -(2.0_f64.powi(-200)), -3.0, 2.0_f64.powi(100)];
        let transform = AffineLinear::new([1.25, 0.0, 0.0, 0.75])
            .unwrap()
            .at((0.0, 0.0))
            .unwrap();
        let [Some(x_row), Some(y_row)] = &transform.rows else {
            panic!("both nonidentity scale rows are present")
        };
        let extents = AffineRowExtents::from_columns(&xs, &ys).unwrap();
        assert!(x_row.admitted(extents.x, extents.y).is_some());
        assert!(y_row.admitted(extents.x, extents.y).is_some());

        let mut actual_x = vec![0.0; xs.len()];
        let mut actual_y = vec![0.0; ys.len()];
        transform
            .map_xy_columns(&xs, &ys, Some(&mut actual_x), Some(&mut actual_y))
            .unwrap();
        for (((&x, &y), &mapped_x), &mapped_y) in xs.iter().zip(&ys).zip(&actual_x).zip(&actual_y) {
            assert_eq!(
                mapped_x.to_bits(),
                exact_affine_origin_ordinate(1.25, 0.0, 0.0, 0.0, 0.0, x, y)
                    .unwrap()
                    .to_bits()
            );
            assert_eq!(
                mapped_y.to_bits(),
                exact_affine_origin_ordinate(0.0, 0.75, 0.0, 0.0, 0.0, x, y)
                    .unwrap()
                    .to_bits()
            );
        }
        assert_eq!(
            actual_x
                .first()
                .copied()
                .expect("the row has coordinates")
                .to_bits(),
            0.0_f64.to_bits()
        );
    }

    #[test]
    fn coordinate_row_with_subnormal_values_uses_the_checked_path() {
        let minimum_subnormal = f64::from_bits(1);
        let transform = AffineLinear::new([2.0, 0.0, 0.0, 1.0])
            .unwrap()
            .at((0.0, 0.0))
            .unwrap();
        let xs = [minimum_subnormal, -minimum_subnormal];
        let ys = [0.0, 0.0];
        let [Some(x_row), None] = &transform.rows else {
            panic!("only the X scale row is nonidentity")
        };
        let extents = AffineRowExtents::from_columns(&xs, &ys).unwrap();
        assert!(x_row.admitted(extents.x, extents.y).is_none());
        let mut actual_x = [0.0; 2];
        transform
            .map_xy_columns(&xs, &ys, Some(&mut actual_x), None)
            .unwrap();
        assert_eq!(actual_x.map(f64::to_bits), [
            f64::from_bits(2).to_bits(),
            f64::from_bits((1 << 63) | 2).to_bits()
        ]);
    }
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
    fn affine_about_origin(
        &self,
        m00: f64,
        m01: f64,
        m10: f64,
        m11: f64,
        origin: (f64, f64),
    ) -> Result<Self> {
        let linear = AffineLinear::new([m00, m01, m10, m11])?;
        if linear.is_identity() {
            return Ok(self.clone());
        }
        let transform = linear.at(origin)?;
        self.try_map_coordseqs(
            |seq| seq.try_affine_about_origin(&transform),
            |point| {
                let (x, y) = transform.map_xy(point.x, point.y)?;
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
            Self::MultiPoint(_)
            | Self::LineString(_)
            | Self::MultiLineString(_)
            | Self::Polygon(_)
            | Self::MultiPolygon(_)
            | Self::GeometryCollection(_)
            | Self::Empty(..) => (0.0, 0.0),
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
            Self::LineString(points) => Ok(Self::LineString(LineSeq::from_proven(
                points.as_coords().try_map_points(transform)?,
            ))),
            Self::MultiLineString(lines) => {
                let values = lines
                    .as_slice()
                    .iter()
                    .map(|line| {
                        line.as_coords()
                            .try_map_points(transform)
                            .map(LineSeq::from_proven)
                    })
                    .collect::<Result<Vec<_>, crate::error::Error>>()?;
                let axes = values
                    .first()
                    .map_or_else(|| lines.axes(), |line| line.axes());
                Ok(Self::MultiLineString(lines.try_with_values(axes, values)?))
            },
            Self::Polygon(polygon) => Ok(Self::Polygon(polygon.map_points(transform)?)),
            Self::MultiPolygon(polygons) => {
                let values = polygons
                    .as_slice()
                    .iter()
                    .map(|polygon| polygon.map_points(transform))
                    .collect::<Result<Vec<_>, _>>()?;
                let axes = values
                    .first()
                    .map_or_else(|| polygons.axes(), Polygon::axes);
                Ok(Self::MultiPolygon(polygons.try_with_values(axes, values)?))
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
    /// `PostGIS` ``ST_Subdivide`` shape). Parts cover the input up to
    /// binary64 rounding at constructed cut vertices; simple inputs return as
    /// a single part. Linework with no representable rectangular cut is
    /// divided between source vertices. If exact partitioning cannot produce
    /// bounded parts, subdivision returns an unrepresentable-output error
    /// instead of emitting an oversized part.
    pub fn subdivide(&self, max_vertices: usize, drop: bool) -> Result<Vec<Self>> {
        self.subdivide_with_frame(max_vertices, drop, None, None)
    }

    pub(crate) fn subdivide_in_geographic_frame(
        &self,
        max_vertices: usize,
        drop: bool,
        frame: crate::geographic::GeographicFrame,
        metric_access: &crate::geometry::SourceArcMetricAccess<'_>,
    ) -> Result<Vec<Self>> {
        self.subdivide_with_frame(max_vertices, drop, Some(frame), Some(metric_access))
    }

    fn subdivide_with_frame(
        &self,
        max_vertices: usize,
        drop: bool,
        frame: Option<crate::geographic::GeographicFrame>,
        metric_access: Option<&crate::geometry::SourceArcMetricAccess<'_>>,
    ) -> Result<Vec<Self>> {
        let mut parts = Vec::new();
        let mut budget = ExpansionBudget::new("subdivide", "max_vertices");
        split(
            self.clone(),
            max_vertices,
            drop,
            frame,
            metric_access,
            &mut parts,
            &mut budget,
        )?;
        Ok(parts)
    }

    pub fn clip_by_rect(&self, rect: Bounds, drop: bool) -> Result<Self> {
        self.clip_by_rect_with_bounds(rect, drop, self.bounds())
    }

    /// Clip a canonical geographic rectangle in a frame, where subject
    /// longitude aliases and pole vertices require the frame-aware topology
    /// graph rather than Cartesian windows.
    pub(crate) fn clip_by_rect_in_geographic_frame<M: crate::geometry::ChartMetric>(
        &self,
        rect: Bounds,
        drop: bool,
        frame: crate::geographic::GeographicFrame,
        metric_access: &crate::geometry::SourceArcMetricAccess<'_, M>,
    ) -> Result<Self> {
        let subject_plan = crate::geometry::PreparedPlan::transient(self);
        let subject_cache = crate::geometry::FrameDependentCaches::default();
        let subject =
            crate::geometry::GeographicPrepared::try_new(&subject_plan, &subject_cache, frame)?;

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
        let clipped = overlay_all_geographic_from_with_metric(
            &originals,
            OverlayOp::Intersection,
            Strictness::from(drop),
            true,
            Some(metric_access),
        )?;
        Ok(clipped)
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
        overlay_intersection_with_rectangle(self, rect, bounds, Strictness::from(drop))
    }
}

impl Shape {
    /// Apply `transform` to every coordinate chain (lines and rings),
    /// keeping the structure; `None` from the transform reuses the
    /// original chain (cheap column clone, no re-materialization).
    pub(crate) fn map_chains(
        &self,
        transform: &mut impl FnMut(&CoordSeq) -> Result<Option<CoordSeq>>,
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

    /// Visit corresponding line and ring chains from a geometry and its
    /// `map_chains` result with each chain's global site-coordinate offset.
    pub(crate) fn any_paired_chain(
        &self,
        support: &Self,
        visitor: &mut impl FnMut(usize, usize, &CoordSeq, &CoordSeq) -> Result<bool>,
    ) -> Result<bool> {
        #[derive(Default)]
        struct Offsets {
            sites: usize,
            chain: usize,
        }

        fn advance(offset: &mut usize, amount: usize) -> Result<()> {
            *offset = offset
                .checked_add(amount)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            Ok(())
        }

        fn visit_chain(
            sites: &CoordSeq,
            support: &CoordSeq,
            offsets: &mut Offsets,
            visitor: &mut impl FnMut(usize, usize, &CoordSeq, &CoordSeq) -> Result<bool>,
        ) -> Result<bool> {
            let chain = offsets.chain;
            offsets.chain = (offsets.chain)
                .checked_add(1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            let stop = visitor(chain, offsets.sites, sites, support)?;
            advance(&mut offsets.sites, sites.len())?;
            Ok(stop)
        }

        fn visit_shape(
            sites: &Shape,
            support: &Shape,
            offsets: &mut Offsets,
            visitor: &mut impl FnMut(usize, usize, &CoordSeq, &CoordSeq) -> Result<bool>,
        ) -> Result<bool> {
            match (sites, support) {
                (Shape::Point(_), Shape::Point(_)) => advance(&mut offsets.sites, 1)?,
                (Shape::MultiPoint(sites), Shape::MultiPoint(_)) => {
                    advance(&mut offsets.sites, sites.len())?;
                },
                (Shape::LineString(sites), Shape::LineString(support)) => {
                    if visit_chain(sites, support, offsets, visitor)? {
                        return Ok(true);
                    }
                },
                (Shape::MultiLineString(sites), Shape::MultiLineString(support)) => {
                    if sites.member_count() != support.member_count() {
                        return Err(GeometryErrorKind::message(
                            "paired geometries have different line member counts",
                        ));
                    }
                    for (site, working) in sites
                        .members_with_values()
                        .zip(support.members_with_values())
                    {
                        match (site, working) {
                            (
                                crate::geometry::MultiMember::Value(site),
                                crate::geometry::MultiMember::Value(working),
                            ) => {
                                if visit_chain(site, working, offsets, visitor)? {
                                    return Ok(true);
                                }
                            },
                            (
                                crate::geometry::MultiMember::Empty,
                                crate::geometry::MultiMember::Empty,
                            ) => {},
                            _ => {
                                return Err(GeometryErrorKind::message(
                                    "paired geometries have different line member layouts",
                                ));
                            },
                        }
                    }
                },
                (Shape::Polygon(sites), Shape::Polygon(support)) => {
                    if sites.rings().count() != support.rings().count() {
                        return Err(GeometryErrorKind::message(
                            "paired geometries have different polygon ring counts",
                        ));
                    }
                    for (site, working) in sites.rings().zip(support.rings()) {
                        if visit_chain(site.coords(), working.coords(), offsets, visitor)? {
                            return Ok(true);
                        }
                    }
                },
                (Shape::MultiPolygon(sites), Shape::MultiPolygon(support)) => {
                    if sites.member_count() != support.member_count() {
                        return Err(GeometryErrorKind::message(
                            "paired geometries have different polygon member counts",
                        ));
                    }
                    for (site, working) in sites
                        .members_with_values()
                        .zip(support.members_with_values())
                    {
                        match (site, working) {
                            (
                                crate::geometry::MultiMember::Value(site),
                                crate::geometry::MultiMember::Value(working),
                            ) => {
                                if site.rings().count() != working.rings().count() {
                                    return Err(GeometryErrorKind::message(
                                        "paired geometries have different polygon ring counts",
                                    ));
                                }
                                for (site, working) in site.rings().zip(working.rings()) {
                                    if visit_chain(
                                        site.coords(),
                                        working.coords(),
                                        offsets,
                                        visitor,
                                    )? {
                                        return Ok(true);
                                    }
                                }
                            },
                            (
                                crate::geometry::MultiMember::Empty,
                                crate::geometry::MultiMember::Empty,
                            ) => {},
                            _ => {
                                return Err(GeometryErrorKind::message(
                                    "paired geometries have different polygon member layouts",
                                ));
                            },
                        }
                    }
                },
                (Shape::GeometryCollection(sites), Shape::GeometryCollection(support)) => {
                    if sites.len() != support.len() {
                        return Err(GeometryErrorKind::message(
                            "paired geometries have different collection member counts",
                        ));
                    }
                    for (site, working) in sites.iter().zip(support.iter()) {
                        if visit_shape(site, working, offsets, visitor)? {
                            return Ok(true);
                        }
                    }
                },
                (Shape::Empty(site_kind, _), Shape::Empty(support_kind, _))
                    if site_kind == support_kind => {},
                _ => {
                    return Err(GeometryErrorKind::message(
                        "paired geometries have different structures",
                    ));
                },
            }
            Ok(false)
        }

        visit_shape(self, support, &mut Offsets::default(), visitor)
    }

    pub fn remove_repeated_points(&self, tolerance: f64) -> Result<Self> {
        let tolerance = NonNegative::try_new("tolerance", tolerance)?.get();
        let remove_repeated_polygon = |polygon: &Polygon| -> Result<Option<Polygon>> {
            polygon.map_rings_collapsing(|ring| remove_repeated_ring_points(ring, tolerance))
        };
        Ok(match self {
            Self::Point(point) => Self::Point(*point),
            Self::MultiPoint(points) => {
                let values = points.iter().collect::<Vec<_>>();
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
                    Self::MultiPoint(MultiPoint::try_new(points.axes(), members)?)
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
                    .members_with_values()
                    .map(|member| match member {
                        crate::geometry::MultiMember::Empty => {
                            Ok(crate::geometry::MultiMember::Empty)
                        },
                        crate::geometry::MultiMember::Value(polygon) => {
                            Ok(remove_repeated_polygon(polygon)?.map_or(
                                crate::geometry::MultiMember::Empty,
                                crate::geometry::MultiMember::Value,
                            ))
                        },
                    })
                    .collect::<Result<Vec<_>>>()?;
                Self::MultiPolygon(MultiPolygon::try_new(polygons.axes(), members)?)
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
        self.densified_budgeted_with_placement(fraction, &SegmentPlacement::Planar, budget)
    }

    pub(crate) fn densified_budgeted_with_placement(
        &self,
        fraction: f64,
        placement: &SegmentPlacement<'_>,
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
        self.segmentize_budgeted(max_segment_length, &SegmentPlacement::Planar, &mut budget)
    }

    pub(crate) fn segmentize_budgeted(
        &self,
        max_segment_length: f64,
        placement: &SegmentPlacement<'_>,
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
        self.orient_polygons_with_owners(exterior_cw, &mut None)
    }

    fn orient_polygons_with_owners(
        &self,
        exterior_cw: bool,
        owners: &mut Option<HashMap<ShapeOwnerKey, Self>>,
    ) -> Result<Self> {
        Ok(match self {
            Self::Polygon(polygon) => Self::Polygon(polygon.orient(exterior_cw)?),
            Self::MultiPolygon(polygons) => Self::MultiPolygon(
                polygons.try_map_values(polygons.axes(), |polygon| polygon.orient(exterior_cw))?,
            ),
            Self::GeometryCollection(geometries) => {
                let owner = geometries
                    .members_are_shared()
                    .then(|| ShapeOwnerKey::new(geometries.as_slice()));
                if let Some(oriented) = owner
                    .and_then(|owner| owners.as_ref()?.get(&owner))
                    .cloned()
                {
                    return Ok(oriented);
                }
                let oriented =
                    Self::GeometryCollection(geometries.try_map_members(|geometry| {
                        geometry.orient_polygons_with_owners(exterior_cw, owners)
                    })?);
                if let Some(owner) = owner {
                    owners
                        .get_or_insert_with(HashMap::new)
                        .insert(owner, oriented.clone());
                }
                oriented
            },
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
                let mut values = points.to_vec();
                values.sort_unstable_by(compare_points);
                Self::MultiPoint(points.with_values(&values)?)
            },
            Self::LineString(points) => Self::LineString(points.normalized()?),
            Self::MultiLineString(lines) => {
                let mut values = Vec::with_capacity(lines.len());
                for line in lines.iter() {
                    values.push(line.normalized()?);
                }
                values.sort_unstable_by(compare_point_slices);
                Self::MultiLineString(lines.with_values(values))
            },
            Self::Polygon(polygon) => Self::Polygon(polygon.normalize()?),
            Self::MultiPolygon(polygons) => {
                let mut values = Vec::with_capacity(polygons.len());
                for polygon in polygons.iter() {
                    values.push(polygon.normalize()?);
                }
                values.sort_unstable_by(compare_polygons);
                Self::MultiPolygon(polygons.with_values(values))
            },
            Self::GeometryCollection(geometries) => {
                let mut members = Vec::with_capacity(geometries.len());
                for geometry in geometries {
                    members.push(geometry.normalize()?);
                }
                members.sort_unstable_by(compare_shapes);
                Self::GeometryCollection(geometries.try_rebuild_members(members)?)
            },
            Self::Empty(..) => self.clone(),
        })
    }
}
