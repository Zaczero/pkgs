#![expect(
    clippy::similar_names,
    reason = "axis and formula symbols name paired quantities here (minx/miny, xs/ys, lat_rad/lam_rad, step/stop), which this lint reads as typos"
)]
use std::simd::cmp::{SimdPartialEq as _, SimdPartialOrd as _};
use std::simd::num::SimdFloat as _;

use pyo3::Python;

use crate::array::packed_columns::{
    LineColumns, PackedColumns, PointColumns, PolygonColumns, XyColumns,
};
use crate::array::{GeometryArrayStorage, PackedStorage, PyGeometryArray, PyResult, column_window};
use crate::geometry::{
    ReduceSimd, pair_select_mask, same_topological_coordinate, topology_coordinate_bits_simd,
};

/// `equals_exact` over two resolved packed line column sets.
fn packed_lines_equal_exact_columns_impl<const Z: bool, const M: bool>(
    left: &LineColumns<'_>,
    right: &LineColumns<'_>,
    tolerance: f64,
) -> Vec<bool> {
    use crate::geometry::columns_within;
    let left_coords = left.coords();
    let right_coords = right.coords();
    let rows = left.rows();
    if rows != right.rows()
        || (Z && left_coords.zs().is_some() != right_coords.zs().is_some())
        || (M && left_coords.ms().is_some() != right_coords.ms().is_some())
    {
        return vec![false; rows];
    }
    let left_xs = left_coords.xs();
    let left_ys = left_coords.ys();
    let right_xs = right_coords.xs();
    let right_ys = right_coords.ys();
    let zs = if Z {
        left_coords.zs().zip(right_coords.zs())
    } else {
        None
    };
    let ms = if M {
        left_coords.ms().zip(right_coords.ms())
    } else {
        None
    };
    (0..rows)
        .map(|row| {
            if left.is_missing(row) || right.is_missing(row) {
                return false;
            }
            let lw = left.row_window(row);
            let rw = right.row_window(row);
            lw.len() == rw.len()
                && columns_within(
                    column_window(left_xs, &lw),
                    column_window(right_xs, &rw),
                    tolerance,
                )
                && columns_within(
                    column_window(left_ys, &lw),
                    column_window(right_ys, &rw),
                    tolerance,
                )
                && zs.is_none_or(|(l, r)| {
                    columns_within(column_window(l, &lw), column_window(r, &rw), tolerance)
                })
                && ms.is_none_or(|(l, r)| {
                    columns_within(column_window(l, &lw), column_window(r, &rw), tolerance)
                })
        })
        .collect()
}

/// `equals_exact` over two resolved packed polygon column sets.
fn packed_polygons_equal_exact_columns_impl<const Z: bool, const M: bool>(
    left: &PolygonColumns<'_>,
    right: &PolygonColumns<'_>,
    tolerance: f64,
) -> Vec<bool> {
    use crate::geometry::columns_within;
    let left_coords = left.coords();
    let right_coords = right.coords();
    let rows = left.rows();
    if rows != right.rows()
        || (Z && left_coords.zs().is_some() != right_coords.zs().is_some())
        || (M && left_coords.ms().is_some() != right_coords.ms().is_some())
    {
        return vec![false; rows];
    }
    let left_xs = left_coords.xs();
    let left_ys = left_coords.ys();
    let right_xs = right_coords.xs();
    let right_ys = right_coords.ys();
    let zs = if Z {
        left_coords.zs().zip(right_coords.zs())
    } else {
        None
    };
    let ms = if M {
        left_coords.ms().zip(right_coords.ms())
    } else {
        None
    };
    (0..rows)
        .map(|row| {
            if left.is_missing(row) || right.is_missing(row) {
                return false;
            }
            let left_row = left.packed_row(row);
            let right_row = right.packed_row(row);
            let left_rings = left_row
                .polygon_rings()
                .expect("packed polygon row has a ring range");
            let right_rings = right_row
                .polygon_rings()
                .expect("packed polygon row has a ring range");
            if left_rings.len() != right_rings.len()
                || left_rings.clone().zip(right_rings).any(|(left, right)| {
                    left_row
                        .polygon_ring_window(left)
                        .expect("ring lies in its admitted polygon row")
                        .len()
                        != right_row
                            .polygon_ring_window(right)
                            .expect("ring lies in its admitted polygon row")
                            .len()
                })
            {
                return false;
            }
            if left_rings.is_empty() {
                return true;
            }
            let lw = left_row
                .vertex_window()
                .expect("present polygon row has a vertex window");
            let rw = right_row
                .vertex_window()
                .expect("present polygon row has a vertex window");
            columns_within(
                column_window(left_xs, &lw),
                column_window(right_xs, &rw),
                tolerance,
            ) && columns_within(
                column_window(left_ys, &lw),
                column_window(right_ys, &rw),
                tolerance,
            ) && zs.is_none_or(|(l, r)| {
                columns_within(column_window(l, &lw), column_window(r, &rw), tolerance)
            }) && ms.is_none_or(|(l, r)| {
                columns_within(column_window(l, &lw), column_window(r, &rw), tolerance)
            })
        })
        .collect()
}

/// `equals_exact` over two resolved packed point column sets.
#[expect(
    clippy::indexing_slicing,
    reason = "PointColumns cursors yield admitted physical rows; retain direct scalar coordinate-lane reads"
)]
fn packed_points_equal_exact_columns_impl<const Z: bool, const M: bool>(
    left: &PointColumns<'_>,
    right: &PointColumns<'_>,
    tolerance: f64,
) -> Vec<bool> {
    let left_coords = left.coords();
    let right_coords = right.coords();
    let rows = left.len();
    if rows != right.len()
        || (Z && left_coords.zs().is_some() != right_coords.zs().is_some())
        || (M && left_coords.ms().is_some() != right_coords.ms().is_some())
    {
        return vec![false; rows];
    }
    if let (Some(left_xy), Some(right_xy)) = (left.contiguous_xy(), right.contiguous_xy()) {
        return packed_points_equal_contiguous::<Z, M>(left, right, &left_xy, &right_xy, tolerance);
    }
    let mut out = Vec::with_capacity(rows);
    let mut left_rows = left.rows().cursor();
    let mut right_rows = right.rows().cursor();
    for row in 0..rows {
        let (left_logical, left_physical) =
            left_rows.next().expect("left point rows length is exact");
        let (right_logical, right_physical) =
            right_rows.next().expect("right point rows length is exact");
        debug_assert_eq!(left_logical, row);
        debug_assert_eq!(right_logical, row);
        let (Some(l), Some(r)) = (left_physical, right_physical) else {
            out.push(false);
            continue;
        };
        let mut equal = ordinate_equal(left_coords.xs()[l], right_coords.xs()[r], tolerance)
            && ordinate_equal(left_coords.ys()[l], right_coords.ys()[r], tolerance);
        if Z {
            equal &= left_coords
                .zs()
                .zip(right_coords.zs())
                .is_some_and(|(a, b)| ordinate_equal(a[l], b[r], tolerance));
        }
        if M {
            equal &= left_coords
                .ms()
                .zip(right_coords.ms())
                .is_some_and(|(a, b)| ordinate_equal(a[l], b[r], tolerance));
        }
        out.push(equal);
    }
    out
}

#[expect(
    clippy::arithmetic_side_effects,
    reason = "SIMD tolerance comparison intentionally uses IEEE subtraction, including overflow to infinities and NaN"
)]
fn packed_points_equal_contiguous<const Z: bool, const M: bool>(
    left: &PointColumns<'_>,
    right: &PointColumns<'_>,
    left_xy: &XyColumns<'_>,
    right_xy: &XyColumns<'_>,
    tolerance: f64,
) -> Vec<bool> {
    let left_coords = left.contiguous_coords().expect("contiguous point columns");
    let right_coords = right.contiguous_coords().expect("contiguous point columns");
    let XyColumns {
        xs: left_xs,
        ys: left_ys,
    } = left_xy;
    let XyColumns {
        xs: right_xs,
        ys: right_ys,
    } = right_xy;
    let rows = left_xs.len();
    let mut out = vec![false; rows];
    if tolerance == 0.0 && !Z && !M {
        pair_select_mask(
            left_xs,
            left_ys,
            right_xs,
            right_ys,
            &mut out,
            |lxi, lyi, rxi, ryi| {
                same_topological_coordinate(lxi, rxi) && same_topological_coordinate(lyi, ryi)
            },
            |lx, ly, rx, ry| {
                topology_coordinate_bits_simd(lx).simd_eq(topology_coordinate_bits_simd(rx))
                    & topology_coordinate_bits_simd(ly).simd_eq(topology_coordinate_bits_simd(ry))
            },
        );
        return out;
    }
    let tolerances = ReduceSimd::splat(tolerance);
    pair_select_mask(
        left_xs,
        left_ys,
        right_xs,
        right_ys,
        &mut out,
        |lxi, lyi, rxi, ryi| {
            ordinate_equal(lxi, rxi, tolerance) && ordinate_equal(lyi, ryi, tolerance)
        },
        |lx, ly, rx, ry| {
            let x_within = (lx - rx).abs().simd_le(tolerances) | lx.to_bits().simd_eq(rx.to_bits());
            let y_within = (ly - ry).abs().simd_le(tolerances) | ly.to_bits().simd_eq(ry.to_bits());
            x_within & y_within
        },
    );
    if Z {
        apply_optional_ordinate_equal(&mut out, left_coords.zs(), right_coords.zs(), tolerance);
    }
    if M {
        apply_optional_ordinate_equal(&mut out, left_coords.ms(), right_coords.ms(), tolerance);
    }
    out
}

fn ordinate_equal(left: f64, right: f64, tolerance: f64) -> bool {
    (left - right).abs() <= tolerance || same_topological_coordinate(left, right)
}

fn apply_optional_ordinate_equal(
    out: &mut [bool],
    left: Option<&[f64]>,
    right: Option<&[f64]>,
    tolerance: f64,
) {
    match (left, right) {
        (Some(left), Some(right)) => {
            for (slot, (&l, &r)) in out.iter_mut().zip(std::iter::zip(left, right)) {
                if *slot {
                    *slot = ordinate_equal(l, r, tolerance);
                }
            }
        },
        (None, None) => {},
        _ => out.fill(false),
    }
}

/// Packed line/polygon/point `equals_exact` through the detached column layer.
pub(crate) fn pair_packed_equals_exact(
    py: Python<'_>,
    left: &PyGeometryArray,
    right: &PyGeometryArray,
    tolerance: f64,
    include_z: bool,
    include_m: bool,
) -> PyResult<Option<Vec<bool>>> {
    crate::geometry::dispatch_axes!((include_z, include_m), HAS_Z, HAS_M, {
        pair_packed_equals_exact_impl::<HAS_Z, HAS_M>(py, left, right, tolerance)
    })
}

fn pair_packed_equals_exact_impl<const Z: bool, const M: bool>(
    py: Python<'_>,
    left: &PyGeometryArray,
    right: &PyGeometryArray,
    tolerance: f64,
) -> PyResult<Option<Vec<bool>>> {
    let both_points = matches!(
        (left.storage(), right.storage()),
        (
            GeometryArrayStorage::Packed(PackedStorage::Points { .. }),
            GeometryArrayStorage::Packed(PackedStorage::Points { .. })
        )
    );
    let both_lines = matches!(
        (left.storage(), right.storage()),
        (
            GeometryArrayStorage::Packed(PackedStorage::Lines { .. }),
            GeometryArrayStorage::Packed(PackedStorage::Lines { .. })
        )
    );
    let both_polys = matches!(
        (left.storage(), right.storage()),
        (
            GeometryArrayStorage::Packed(PackedStorage::Polygons { .. }),
            GeometryArrayStorage::Packed(PackedStorage::Polygons { .. })
        )
    );
    if !both_points && !both_lines && !both_polys {
        return Ok(None);
    }
    PyGeometryArray::pair_packed_columns_detached(py, left, right, move |left, right| {
        Ok(match (left, right) {
            (PackedColumns::Points(left), PackedColumns::Points(right)) => {
                packed_points_equal_exact_columns_impl::<Z, M>(&left, &right, tolerance)
            },
            (PackedColumns::Lines(left), PackedColumns::Lines(right)) => {
                packed_lines_equal_exact_columns_impl::<Z, M>(&left, &right, tolerance)
            },
            (PackedColumns::Polygons(left), PackedColumns::Polygons(right)) => {
                packed_polygons_equal_exact_columns_impl::<Z, M>(&left, &right, tolerance)
            },
            _ => unreachable!("storage kinds matched above"),
        })
    })
}
