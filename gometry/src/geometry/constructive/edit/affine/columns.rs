use super::rounding::round_certified_double_double;
use super::row::{
    AFFINE_FIXUP_MARKER, AdmittedAffineMode, AdmittedAffineOriginRow, AffineOriginRow,
    AffineRowExtents, map_biased_product, map_exact_dot, map_general_affine_row, single_product,
};
use super::{DekkerParts, GeometryErrorKind, Result, binary_zero};

pub(crate) struct OriginAffine {
    pub(in crate::geometry::constructive::edit) preserved: [bool; 2],
    pub(super) rows: [Option<AffineOriginRow>; 2],
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

pub(super) fn upper_sum_admitted(lhs: f64, rhs: f64) -> f64 {
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
    /// Keep the chosen row formulas concrete through the fused coordinate loop.
    macro_rules! map_pair {
        ($split:literal, $x:expr, $y:expr) => {
            map_admitted_xy_columns_kernel::<$split, _, _>(
                xs, ys, out_x, out_y, x_row, y_row, $x, $y,
            )
        };
    }
    match (x_fast.mode, y_fast.mode) {
        (AdmittedAffineMode::ProductX(x), AdmittedAffineMode::ProductY(y)) => {
            return map_pair!(
                false,
                |value_x, _, _, _| (single_product(x.value(), value_x), true),
                |_, value_y, _, _| (single_product(y.value(), value_y), true)
            );
        },
        (
            AdmittedAffineMode::BiasedProductX(x, x_bias),
            AdmittedAffineMode::BiasedProductY(y, y_bias),
        ) => {
            return map_pair!(
                true,
                |_, _, parts_x, _| map_biased_product(x_bias, x, parts_x),
                |_, _, _, parts_y| map_biased_product(y_bias, y, parts_y)
            );
        },
        (AdmittedAffineMode::Dot(xx, xy), AdmittedAffineMode::Dot(yx, yy)) => {
            return map_pair!(
                true,
                |_, _, parts_x, parts_y| map_exact_dot(xx, xy, parts_x, parts_y),
                |_, _, parts_x, parts_y| map_exact_dot(yx, yy, parts_x, parts_y)
            );
        },
        (
            AdmittedAffineMode::General(xx, xy, x_bias),
            AdmittedAffineMode::General(yx, yy, y_bias),
        ) => {
            return map_pair!(
                true,
                |_, _, parts_x, parts_y| map_general_affine_row(x_bias, xx, xy, parts_x, parts_y),
                |_, _, parts_x, parts_y| map_general_affine_row(y_bias, yx, yy, parts_x, parts_y)
            );
        },
        _ => {},
    }
    let map_x = |x, y, parts_x, parts_y| x_fast.map(x, y, parts_x, parts_y);
    let map_y = |x, y, parts_x, parts_y| y_fast.map(x, y, parts_x, parts_y);
    if x_fast.requires_coordinate_split() || y_fast.requires_coordinate_split() {
        map_pair!(true, map_x, map_y)
    } else {
        map_pair!(false, map_x, map_y)
    }
}

fn map_admitted_xy_columns_kernel<const SPLIT_COORDINATES: bool, X, Y>(
    xs: &[f64],
    ys: &[f64],
    out_x: &mut [f64],
    out_y: &mut [f64],
    x_row: &AffineOriginRow,
    y_row: &AffineOriginRow,
    map_x: X,
    map_y: Y,
) -> Result<()>
where
    X: Fn(f64, f64, DekkerParts, DekkerParts) -> (f64, bool),
    Y: Fn(f64, f64, DekkerParts, DekkerParts) -> (f64, bool),
{
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
        let (mapped_x, certified_x) = map_x(x, y, x_parts, y_parts);
        let (mapped_y, certified_y) = map_y(x, y, x_parts, y_parts);
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
    /// Instantiate only the coordinate splitting required by the selected formula.
    macro_rules! map_column {
        ($split_x:literal, $split_y:literal, $map:expr) => {
            map_admitted_one_column_kernel::<$split_x, $split_y, _>(xs, ys, output, exact_row, $map)
        };
    }
    match fast_row.mode {
        AdmittedAffineMode::Zero => map_column!(false, false, |_, _, _, _| (0.0, true)),
        AdmittedAffineMode::ProductX(coefficient) => map_column!(false, false, |x, _, _, _| (
            single_product(coefficient.value(), x),
            true
        )),
        AdmittedAffineMode::ProductY(coefficient) => map_column!(false, false, |_, y, _, _| (
            single_product(coefficient.value(), y),
            true
        )),
        AdmittedAffineMode::BiasOnly(bias) => map_column!(false, false, |_, _, _, _| {
            round_certified_double_double::<true>(bias.high, bias.low, bias.error)
        }),
        AdmittedAffineMode::BiasedProductX(coefficient, bias) => {
            map_column!(true, false, |_, _, parts_x, _| {
                map_biased_product(bias, coefficient, parts_x)
            })
        },
        AdmittedAffineMode::BiasedProductY(coefficient, bias) => {
            map_column!(false, true, |_, _, _, parts_y| {
                map_biased_product(bias, coefficient, parts_y)
            })
        },
        AdmittedAffineMode::Dot(x, y) => map_column!(true, true, |_, _, parts_x, parts_y| {
            map_exact_dot(x, y, parts_x, parts_y)
        }),
        AdmittedAffineMode::General(x, y, bias) => {
            map_column!(true, true, |_, _, parts_x, parts_y| {
                map_general_affine_row(bias, x, y, parts_x, parts_y)
            })
        },
    }
}

fn map_admitted_one_column_kernel<const SPLIT_X: bool, const SPLIT_Y: bool, F>(
    xs: &[f64],
    ys: &[f64],
    output: &mut [f64],
    exact_row: &AffineOriginRow,
    map: F,
) -> Result<()>
where
    F: Fn(f64, f64, DekkerParts, DekkerParts) -> (f64, bool),
{
    for ((&x, &y), output) in xs.iter().zip(ys).zip(output.iter_mut()) {
        let x_parts = if SPLIT_X {
            DekkerParts::admitted(x)
        } else {
            DekkerParts::zero()
        };
        let y_parts = if SPLIT_Y {
            DekkerParts::admitted(y)
        } else {
            DekkerParts::zero()
        };
        let (mapped, certified) = map(x, y, x_parts, y_parts);
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
