use super::row::{AFFINE_FIXUP_MARKER, AdmittedAffineOriginRow, AffineOriginRow, AffineRowExtents};
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
