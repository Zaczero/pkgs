use crate::error::Result;
use crate::geometry::tessellation::earcut_polygon_with;
use crate::geometry::{Point, Shape};
use crate::{try_push_vec, try_reserve_vec};

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
    let mut cumulative = Vec::new();
    try_reserve_vec(&mut cumulative, pieces.len())?;
    for piece in pieces {
        total += weight(piece);
        try_push_vec(&mut cumulative, total)?;
    }
    if total <= 0.0 || !total.is_finite() {
        return Ok(None);
    }
    let mut points = Vec::new();
    try_reserve_vec(&mut points, count)?;
    let mut state = seed;
    for _ in 0..count {
        let pick = uniform_f64(&mut state) * total;
        let index = cumulative
            .partition_point(|&edge| edge <= pick)
            .min(pieces.len() - 1);
        let point = place(&pieces[index], &mut state)?;
        try_push_vec(&mut points, point)?;
    }
    Ok(Some(points))
}

/// Triangles of every areal part, recursively through collections — the
/// dimension-2 sample space.
pub(crate) fn collect_sample_triangles(shape: &Shape, out: &mut Vec<[Point; 3]>) -> Result<()> {
    match shape {
        Shape::Polygon(polygon) => {
            earcut_polygon_with(polygon, &mut |a, b, c| try_push_vec(out, [a, b, c]))?;
        },
        Shape::MultiPolygon(polygons) => {
            for polygon in polygons {
                earcut_polygon_with(polygon, &mut |a, b, c| try_push_vec(out, [a, b, c]))?;
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
    use crate::error::{ErrorKind, ResourceError};
    use crate::geometry::{CoordSeq, LineSeq, Point, Segment, Shape, XY};
    use crate::test_support::fail_allocations_after;

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

    #[test]
    #[expect(
        clippy::ignored_unit_patterns,
        reason = "place is Fn(&T, &mut u64); the unit piece is borrowed"
    )]
    fn cumulative_reserve_fails_before_place() {
        let error = fail_allocations_after(0, || {
            sample_weighted(2, 1, &[()], |_| 1.0, |_, _| panic!("place")).expect_err("alloc 0")
        });
        assert!(matches!(
            error.kind(),
            ErrorKind::Resource(ResourceError::Allocation)
        ));
    }

    #[test]
    #[expect(
        clippy::ignored_unit_patterns,
        reason = "place is Fn(&T, &mut u64); the unit piece is borrowed"
    )]
    fn points_reserve_fails_before_place() {
        let error = fail_allocations_after(1, || {
            sample_weighted(2, 1, &[()], |_| 1.0, |_, _| panic!("place")).expect_err("alloc 1")
        });
        assert!(matches!(
            error.kind(),
            ErrorKind::Resource(ResourceError::Allocation)
        ));
    }

    #[test]
    fn line_discovery_allocation_fails() {
        let line = Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_vecs(vec![0.0, 1.0], vec![0.0, 0.0], None, None).unwrap(),
            )
            .unwrap(),
        );
        let error = fail_allocations_after(0, || {
            line.sample_points(1, 1).expect_err("line staging alloc")
        });
        assert!(matches!(
            error.kind(),
            ErrorKind::Resource(ResourceError::Allocation)
        ));
    }

    #[test]
    fn atom_discovery_allocation_fails() {
        let points = Shape::Point(Point::new_raw_xy(7.0, 8.0));
        let error = fail_allocations_after(0, || {
            points.sample_points(1, 1).expect_err("atom staging alloc")
        });
        assert!(matches!(
            error.kind(),
            ErrorKind::Resource(ResourceError::Allocation)
        ));
    }
}
