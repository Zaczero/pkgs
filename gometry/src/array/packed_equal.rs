#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use std::simd::cmp::{SimdPartialEq as _, SimdPartialOrd as _};
use std::simd::num::SimdFloat as _;

use pyo3::Python;

use crate::array::packed_columns::{
    LineColumns, PackedColumns, PointColumns, PolygonColumns, XyColumns,
};
use crate::array::{GeometryArrayStorage, PyGeometryArray, PyResult, column_window};
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
    let left_offsets = left.offsets();
    let right_offsets = right.offsets();
    let rows = left.rows();
    if rows != right.rows()
        || (Z && left_coords.zs().is_some() != right_coords.zs().is_some())
        || (M && left_coords.ms().is_some() != right_coords.ms().is_some())
    {
        return vec![false; rows];
    }
    // Length-exact reborrows: `windows(2)` over rows+1 offsets, so each row's
    // CSR endpoints are proven in range (bounds-check elision).
    let left_offsets = &left_offsets[..=rows];
    let right_offsets = &right_offsets[..=rows];
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
    left_offsets
        .array_windows::<2>()
        .zip(right_offsets.array_windows::<2>())
        .map(|(lw, rw)| {
            let lw = lw[0] as usize..lw[1] as usize;
            let rw = rw[0] as usize..rw[1] as usize;
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
    let left_rings = left.ring_offsets();
    let right_rings = right.ring_offsets();
    let left_polys = left.polygon_offsets();
    let right_polys = right.polygon_offsets();
    let rows = left.rows();
    if rows != right.rows()
        || (Z && left_coords.zs().is_some() != right_coords.zs().is_some())
        || (M && left_coords.ms().is_some() != right_coords.ms().is_some())
    {
        return vec![false; rows];
    }
    // Length-exact reborrows: polygon CSR tops are rows+1 long (elision).
    let left_polys = &left_polys[..=rows];
    let right_polys = &right_polys[..=rows];
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
    left_polys
        .array_windows::<2>()
        .zip(right_polys.array_windows::<2>())
        .map(|(lp, rp)| {
            let lr_start = lp[0] as usize;
            let lr_end = lp[1] as usize;
            let rr_start = rp[0] as usize;
            let rr_end = rp[1] as usize;
            if lr_end - lr_start != rr_end - rr_start
                || (lr_start..lr_end).zip(rr_start..rr_end).any(|(l, r)| {
                    left_rings[l + 1] - left_rings[l] != right_rings[r + 1] - right_rings[r]
                })
            {
                return false;
            }
            let lw = left_rings[lr_start] as usize..left_rings[lr_end] as usize;
            let rw = right_rings[rr_start] as usize..right_rings[rr_end] as usize;
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
    let XyColumns {
        xs: left_xs,
        ys: left_ys,
    } = left.xy();
    let XyColumns {
        xs: right_xs,
        ys: right_ys,
    } = right.xy();
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
    match (include_z, include_m) {
        (false, false) => pair_packed_equals_exact_impl::<false, false>(py, left, right, tolerance),
        (true, false) => pair_packed_equals_exact_impl::<true, false>(py, left, right, tolerance),
        (false, true) => pair_packed_equals_exact_impl::<false, true>(py, left, right, tolerance),
        (true, true) => pair_packed_equals_exact_impl::<true, true>(py, left, right, tolerance),
    }
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
            GeometryArrayStorage::Points { .. },
            GeometryArrayStorage::Points { .. }
        )
    );
    let both_lines = matches!(
        (left.storage(), right.storage()),
        (
            GeometryArrayStorage::Lines { .. },
            GeometryArrayStorage::Lines { .. }
        )
    );
    let both_polys = matches!(
        (left.storage(), right.storage()),
        (
            GeometryArrayStorage::Polygons { .. },
            GeometryArrayStorage::Polygons { .. }
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
