use crate::error::Result;
use crate::geometry::tessellation::polygon_triangles;
use crate::geometry::{Coordinates as _, GeometryErrorKind, Point, Shape};

/// A common power-of-two factor that keeps weight products finite for
/// huge-but-valid coordinates (exact in binary floating point; weights
/// only need relative consistency).
pub(crate) fn weight_scale(largest: f64) -> f64 {
    if largest > 2.0_f64.powi(500) {
        2.0_f64.powi(500 - largest.log2().ceil() as i32)
    } else {
        1.0
    }
}

/// The weighted-pick engine behind `sample_points`: cumulative weights over
/// `pieces` (zero-weight slivers never selected), then `count` draws of
/// piece-by-prefix-sum followed by the piece's own placement map. `None`
/// when the total weight is not positive finite — the caller falls back to
/// a lower-dimension sample space.
pub(crate) fn sample_weighted<T>(
    count: usize,
    seed: u64,
    pieces: &[T],
    weight: impl Fn(&T) -> f64,
    place: impl Fn(&T, &mut u64) -> Result<Point>,
) -> Result<Option<Vec<Point>>> {
    let mut total = 0.0;
    let cumulative: Vec<f64> = pieces
        .iter()
        .map(|piece| {
            total += weight(piece);
            total
        })
        .collect();
    if total <= 0.0 || !total.is_finite() {
        return Ok(None);
    }
    let mut state = seed;
    let mut points = Vec::new();
    points.try_reserve_exact(count).map_err(|_| {
        GeometryErrorKind::message(format!(
            "sample_points could not allocate {count} output points"
        ))
    })?;
    for _ in 0..count {
        let pick = uniform_f64(&mut state) * total;
        let index = cumulative
            .partition_point(|&edge| edge <= pick)
            .min(pieces.len() - 1);
        points.push(place(&pieces[index], &mut state)?);
    }
    Ok(Some(points))
}

/// Triangles of every areal part, recursively through collections — the
/// dimension-2 sample space.
pub(crate) fn collect_sample_triangles(shape: &Shape, out: &mut Vec<[Point; 3]>) -> Result<()> {
    match shape {
        Shape::Polygon(polygon) => {
            out.extend(
                polygon_triangles(polygon)?
                    .iter()
                    .filter_map(triangle_corners),
            );
        },
        Shape::MultiPolygon(polygons) => {
            for polygon in polygons {
                out.extend(
                    polygon_triangles(polygon)?
                        .iter()
                        .filter_map(triangle_corners),
                );
            }
        },
        Shape::GeometryCollection(geometries) => {
            for geometry in geometries {
                collect_sample_triangles(geometry, out)?;
            }
        },
        _ => {},
    }
    Ok(())
}

/// The three corner points of a triangle `Shape::Polygon` produced by the
/// triangulators (shell of four coordinates, closure last).
pub(crate) fn triangle_corners(triangle: &Shape) -> Option<[Point; 3]> {
    let Shape::Polygon(polygon) = triangle else {
        return None;
    };
    let shell = polygon.shell.coords();
    (shell.coord_count() >= 3).then(|| [shell.nth_coord(0), shell.nth_coord(1), shell.nth_coord(2)])
}

/// The splitmix64 step — the whole RNG behind `sample_points`: tiny,
/// splittable, and deterministic across platforms (no `rand` dependency).
pub(crate) const fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// A uniform `f64` in `[0, 1)` from the top 53 bits of one splitmix64 draw.
pub(crate) fn uniform_f64(state: &mut u64) -> f64 {
    (splitmix64(state) >> 11) as f64 * (1.0 / 9_007_199_254_740_992.0)
}

/// Decorrelated per-row seed for array lanes: same geometry at different
/// rows samples different deterministic streams.
pub(crate) const fn row_sample_seed(seed: u64, row: usize) -> u64 {
    let mut state = seed ^ (row as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    splitmix64(&mut state)
}
