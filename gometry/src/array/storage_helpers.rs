#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use super::*;
use crate::py::support::Bounds3D;

pub(crate) fn row_bounds(xs: &[f64], ys: &[f64]) -> Bounds {
    debug_assert!(!xs.is_empty());
    debug_assert_eq!(xs.len(), ys.len());
    let [minx, miny, maxx, maxy] = crate::geometry::xy_bounds_columns(xs, ys);
    Bounds::new_unchecked(minx, miny, maxx, maxy)
}

/// Borrow a CSR column window without `Range::clone()` — `std::ops::Range` is
/// not `Copy` on our toolchain, but indexing via `start`/`end` is zero-cost.
pub(crate) fn column_window<'a, T>(data: &'a [T], range: &std::ops::Range<usize>) -> &'a [T] {
    &data[range.start..range.end]
}

pub(crate) fn packed_line_measure<'a, F>(
    xs: &[f64],
    ys: &[f64],
    offsets: &[i32],
    map: impl Into<RowSelectionRef<'a>>,
    measure: F,
) -> Vec<f64>
where
    F: Fn(&[f64], &[f64], std::ops::Range<usize>) -> f64,
{
    let map = map.into();
    (0..line_logical_len(offsets, map))
        .map(|logical| {
            let window = map.csr_window(offsets, logical);
            measure(
                column_window(xs, &window),
                column_window(ys, &window),
                window,
            )
        })
        .collect()
}

pub(crate) fn packed_polygon_measure<'a, F, C>(
    xs: &[f64],
    ys: &[f64],
    ring_offsets: &[i32],
    polygon_offsets: &[i32],
    map: impl Into<RowSelectionRef<'a>>,
    ring_measure: F,
    combine: C,
) -> Vec<f64>
where
    F: Fn(&[f64], &[f64], std::ops::Range<usize>) -> f64,
    C: Fn(std::ops::Range<usize>, &dyn Fn(usize) -> f64) -> f64,
{
    let map = map.into();
    (0..polygon_logical_len(polygon_offsets, map))
        .map(|logical| {
            let rings = polygon_rings_range(polygon_offsets, map, logical);
            let ring = |index: usize| {
                let window = ring_offsets[index] as usize..ring_offsets[index + 1] as usize;
                ring_measure(
                    column_window(xs, &window),
                    column_window(ys, &window),
                    window,
                )
            };
            combine(rings, &ring)
        })
        .collect()
}

/// Map a packed centroid column result to a validated point (`POINT EMPTY` on
/// `None`, matching the scalar `Shape::centroid` cascade).
pub(crate) fn packed_centroid_xy(xy: Option<(f64, f64)>) -> Result<Point> {
    match xy {
        Some((x, y)) => Point::new(x, y),
        None => Ok(Point::new_unchecked_xy(f64::NAN, f64::NAN)),
    }
}

/// Map a packed point-on-surface column result to a validated point.
pub(crate) fn packed_surface_point(point: Option<Point>) -> Result<Point> {
    point.map_or_else(
        || Ok(Point::new_unchecked_xy(f64::NAN, f64::NAN)),
        |point| Point::new(point.x, point.y),
    )
}

/// Per-row pivot for packed-line affine ops — matches [`OriginSpec::resolve`]
/// on a `LineString` row (`centroid_xy` / `bounds_center_xy`).
pub(crate) fn packed_line_row_pivot(
    spec: OriginSpec,
    xs: &[f64],
    ys: &[f64],
    window: std::ops::Range<usize>,
) -> Result<(f64, f64)> {
    match spec {
        OriginSpec::Centroid => {
            if window.is_empty() {
                return Ok((0.0, 0.0));
            }
            match crate::geometry::centroid_line_row_columns(
                column_window(xs, &window),
                column_window(ys, &window),
            ) {
                Some((x, y)) => {
                    let point = Point::new(x, y)?;
                    Ok((point.x, point.y))
                },
                None => Ok((0.0, 0.0)),
            }
        },
        OriginSpec::Center => Ok(packed_line_bounds_center_pivot(xs, ys, window)),
        OriginSpec::Fixed(..) => unreachable!("fixed origins use packed_affine"),
    }
}

pub(crate) fn packed_line_bounds_center_pivot(
    xs: &[f64],
    ys: &[f64],
    window: std::ops::Range<usize>,
) -> (f64, f64) {
    if window.is_empty() {
        return (0.0, 0.0);
    }
    let bounds = row_bounds(column_window(xs, &window), column_window(ys, &window));
    (
        f64::midpoint(bounds.minx(), bounds.maxx()),
        f64::midpoint(bounds.miny(), bounds.maxy()),
    )
}

/// Per-row pivot for packed-polygon affine ops — matches
/// [`OriginSpec::resolve`] on a `Polygon` row (all rings for bounds center).
pub(crate) fn packed_polygon_row_pivot(
    spec: OriginSpec,
    xs: &[f64],
    ys: &[f64],
    ring_offsets: &[i32],
    rings: std::ops::Range<usize>,
) -> Result<(f64, f64)> {
    match spec {
        OriginSpec::Centroid => {
            match crate::geometry::centroid_polygon_row_columns(xs, ys, ring_offsets, rings) {
                Some((x, y)) => {
                    let point = Point::new(x, y)?;
                    Ok((point.x, point.y))
                },
                None => Ok((0.0, 0.0)),
            }
        },
        OriginSpec::Center => Ok(packed_polygon_bounds_center_pivot(
            xs,
            ys,
            ring_offsets,
            rings,
        )),
        OriginSpec::Fixed(..) => unreachable!("fixed origins use packed_affine"),
    }
}

pub(crate) fn packed_polygon_bounds_center_pivot(
    xs: &[f64],
    ys: &[f64],
    ring_offsets: &[i32],
    rings: std::ops::Range<usize>,
) -> (f64, f64) {
    let start = ring_offsets[rings.start] as usize;
    let end = ring_offsets[rings.end] as usize;
    if start >= end {
        return (0.0, 0.0);
    }
    let bounds = row_bounds(&xs[start..end], &ys[start..end]);
    (
        f64::midpoint(bounds.minx(), bounds.maxx()),
        f64::midpoint(bounds.miny(), bounds.maxy()),
    )
}

pub(crate) fn row_bounds_values(xs: &[f64], ys: &[f64], range: std::ops::Range<usize>) -> [f64; 4] {
    if range.is_empty() {
        return [f64::NAN; 4];
    }
    let bounds = row_bounds(column_window(xs, &range), column_window(ys, &range));
    [bounds.minx(), bounds.miny(), bounds.maxx(), bounds.maxy()]
}

/// Per-row `(min, max)` from a contiguous Z/M ordinate window — the kernel
/// behind packed `min_*`/`max_*`/`*_range`. Empty windows yield `None`.
pub(crate) fn row_ord_extremes(
    column: &[f64],
    range: std::ops::Range<usize>,
) -> Option<(f64, f64)> {
    if range.is_empty() {
        return None;
    }
    crate::geometry::column_minmax(&column[range])
}

/// Per-row 3D bounds from contiguous X/Y/Z column windows — the kernel
/// behind packed `bounds_3d`. Empty windows yield `None`.
pub(crate) fn row_bounds_3d(
    xs: &[f64],
    ys: &[f64],
    zs: &[f64],
    range: std::ops::Range<usize>,
) -> Option<Bounds3D> {
    if range.is_empty() {
        return None;
    }
    let (minx, maxx) = crate::geometry::column_minmax(column_window(xs, &range))?;
    let (miny, maxy) = crate::geometry::column_minmax(column_window(ys, &range))?;
    let (minz, maxz) = crate::geometry::column_minmax(column_window(zs, &range))?;
    Some(Bounds3D {
        minx,
        miny,
        minz,
        maxx,
        maxy,
        maxz,
    })
}
