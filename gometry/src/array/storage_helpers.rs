#![expect(
    clippy::similar_names,
    reason = "axis and formula symbols name paired quantities here (minx/miny, xs/ys, lat_rad/lam_rad, step/stop), which this lint reads as typos"
)]
use crate::array::{Bounds, OriginSpec, Point, Result};

/// Planar bounds of one row's contiguous vertex columns; `None` when empty.
pub(crate) fn row_bounds(xs: &[f64], ys: &[f64]) -> Option<Bounds> {
    debug_assert_eq!(xs.len(), ys.len());
    (!xs.is_empty()).then(|| {
        let [minx, miny, maxx, maxy] = crate::geometry::xy_bounds_columns(xs, ys);
        Bounds::new_raw(minx, miny, maxx, maxy)
    })
}

/// Borrow a CSR column window admitted by the source offsets.
pub(crate) fn column_window<'a, T>(data: &'a [T], range: &std::ops::Range<usize>) -> &'a [T] {
    data.get(range.start..range.end)
        .expect("column window is bounded by its admitted offsets")
}

/// Map a packed centroid column result to a validated point (`POINT EMPTY` on
/// `None`, matching the scalar `Shape::centroid` cascade).
pub(crate) fn packed_centroid_xy(xy: Option<(f64, f64)>) -> Result<Point> {
    match xy {
        Some((x, y)) => Point::new(x, y),
        None => Ok(Point::new_raw_xy(f64::NAN, f64::NAN)),
    }
}

/// Map a packed point-on-surface column result to a validated point.
pub(crate) fn packed_surface_point(point: Option<Point>) -> Result<Point> {
    point.map_or_else(
        || Ok(Point::new_raw_xy(f64::NAN, f64::NAN)),
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
        OriginSpec::Fixed(x, y) => Ok((x, y)),
    }
}

pub(crate) fn packed_line_bounds_center_pivot(
    xs: &[f64],
    ys: &[f64],
    window: std::ops::Range<usize>,
) -> (f64, f64) {
    row_bounds(column_window(xs, &window), column_window(ys, &window))
        .map_or((0.0, 0.0), bounds_center)
}

/// Per-row pivot for packed-polygon affine ops — matches
/// [`OriginSpec::resolve`] on a `Polygon` row (all rings for bounds center).
pub(crate) fn packed_polygon_row_pivot(
    spec: OriginSpec,
    row: crate::array::PackedRow<'_>,
) -> Result<(f64, f64)> {
    let crate::array::PackedRow::Polygons {
        coords,
        ring_offsets,
        ..
    } = row
    else {
        unreachable!("polygon pivot requires a packed polygon row")
    };
    let rings = row
        .polygon_rings()
        .expect("packed polygon row carries its ring range");
    match spec {
        OriginSpec::Centroid => match crate::geometry::centroid_polygon_row_columns(
            coords.xs(),
            coords.ys(),
            ring_offsets.as_slice(),
            rings,
        ) {
            Some((x, y)) => {
                let point = Point::new(x, y)?;
                Ok((point.x, point.y))
            },
            None => Ok((0.0, 0.0)),
        },
        OriginSpec::Center => Ok(row.vertex_window().map_or((0.0, 0.0), |window| {
            row_bounds(
                column_window(coords.xs(), &window),
                column_window(coords.ys(), &window),
            )
            .map_or((0.0, 0.0), bounds_center)
        })),
        OriginSpec::Fixed(x, y) => Ok((x, y)),
    }
}

const fn bounds_center(bounds: Bounds) -> (f64, f64) {
    (
        f64::midpoint(bounds.minx(), bounds.maxx()),
        f64::midpoint(bounds.miny(), bounds.maxy()),
    )
}

pub(crate) fn row_bounds_values(xs: &[f64], ys: &[f64], range: std::ops::Range<usize>) -> [f64; 4] {
    row_bounds(column_window(xs, &range), column_window(ys, &range))
        .map_or([f64::NAN; 4], |bounds| {
            [bounds.minx(), bounds.miny(), bounds.maxx(), bounds.maxy()]
        })
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
    crate::geometry::column_minmax(column_window(column, &range))
}
