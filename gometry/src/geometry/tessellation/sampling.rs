use crate::error::Result;
use crate::geometry::tessellation::earcut_polygon_with;
use crate::geometry::{Point, Shape};

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
    let mut cumulative = Vec::with_capacity(pieces.len());
    for piece in pieces {
        total += weight(piece);
        cumulative.push(total);
    }
    if total <= 0.0 || !total.is_finite() {
        return Ok(None);
    }
    let mut points = Vec::with_capacity(count);
    let mut state = seed;
    for _ in 0..count {
        let pick = uniform_f64(&mut state) * total;
        let index = cumulative
            .partition_point(|&edge| edge <= pick)
            .min(pieces.len() - 1);
        let point = place(&pieces[index], &mut state)?;
        points.push(point);
    }
    Ok(Some(points))
}

/// Triangles of every areal part, recursively through collections — the
/// dimension-2 sample space.
pub(crate) fn collect_sample_triangles(shape: &Shape, out: &mut Vec<[Point; 3]>) -> Result<()> {
    match shape {
        Shape::Polygon(polygon) => {
            earcut_polygon_with(polygon, &mut |a, b, c| {
                out.push([a, b, c]);
                Ok(())
            })?;
        },
        Shape::MultiPolygon(polygons) => {
            for polygon in polygons {
                earcut_polygon_with(polygon, &mut |a, b, c| {
                    out.push([a, b, c]);
                    Ok(())
                })?;
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

#[cfg(test)]
mod tests {
    use super::sample_weighted;
    use crate::geometry::{Point, Segment, XY};

    #[test]
    fn seed1_unit_segment_two_draws_match_splitmix64() {
        let segment = Segment {
            start: XY::new(0.0, 0.0),
            end: XY::new(1.0, 0.0),
        };
        let points = sample_weighted(
            2,
            1,
            &[segment],
            |_| 1.0,
            |segment, state| {
                let along = super::uniform_f64(state);
                Point::new(
                    (1.0 - along) * segment.start.x + along * segment.end.x,
                    (1.0 - along) * segment.start.y + along * segment.end.y,
                )
            },
        )
        .expect("positive weight")
        .expect("samples");
        assert_eq!(points.len(), 2);
        assert_eq!(points[0].x.to_bits(), 0.745_781_757_262_701_1_f64.to_bits());
        assert_eq!(points[0].y.to_bits(), 0.0_f64.to_bits());
        assert_eq!(points[1].x.to_bits(), 0.444_359_217_055_772_1_f64.to_bits());
        assert_eq!(points[1].y.to_bits(), 0.0_f64.to_bits());
    }
}
