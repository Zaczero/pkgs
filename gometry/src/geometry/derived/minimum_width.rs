use super::hull::monotone_chain_hull;
use crate::error::Result;
use crate::geometry::tessellation::exact::{
    ExactDyadic, compare_ratios, round_sqrt_ratio, try_difference as exact_difference,
};
use crate::geometry::{Point, Shape};

fn height_numerator(a: Point, b: Point, p: Point) -> Result<ExactDyadic> {
    exact_difference(b.x, a.x)?
        .try_product(&exact_difference(p.y, a.y)?)?
        .try_subtract(exact_difference(b.y, a.y)?.try_product(&exact_difference(p.x, a.x)?)?)
}

/// Narrowest enclosing strip. Candidate support edges retain their source
/// coordinates; neither normalized normals nor rounded distances select a winner.
#[expect(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    reason = "the allocated nondegenerate hull bounds the cyclic ordinals and has a nonzero length"
)]
pub(crate) fn minimum_width(shape: &Shape, scale: f64) -> Result<f64> {
    let points = shape.try_unique_xy_points()?;
    if points
        .iter()
        .any(|point| !point.x.is_finite() || !point.y.is_finite())
    {
        return Err(crate::geometry::GeometryErrorKind::unrepresentable_output(
            "minimum_width",
        ));
    }
    if points.is_empty() {
        return Ok(f64::NAN);
    }
    let hull = monotone_chain_hull(&points);
    if hull.len() <= 2 {
        return Ok(0.0);
    }
    let count = hull.len();
    let mut opposite = 1;
    let mut best: Option<(ExactDyadic, ExactDyadic)> = None;
    for edge in 0..count {
        let a = points[hull[edge]];
        let b = points[hull[(edge + 1) % count]];
        let mut cross = height_numerator(a, b, points[hull[opposite]])?;
        loop {
            let next = (opposite + 1) % count;
            let next_cross = height_numerator(a, b, points[hull[next]])?;
            if next_cross.cmp(&cross).is_le() {
                break;
            }
            opposite = next;
            cross = next_cross;
        }
        let dx = exact_difference(b.x, a.x)?;
        let dy = exact_difference(b.y, a.y)?;
        let denominator = dx.try_product(&dx)?.try_add(dy.try_product(&dy)?)?;
        let numerator = cross.try_product(&cross)?;
        let replace = match &best {
            None => true,
            Some((best_n, best_d)) => {
                compare_ratios(&numerator, &denominator, best_n, best_d)?.is_lt()
            },
        };
        if replace {
            best = Some((numerator, denominator));
        }
    }
    let (numerator, denominator) = best.expect("nondegenerate hull has support edges");
    let scale = ExactDyadic::from_f64(scale);
    round_sqrt_ratio(
        &numerator.try_product(&scale)?.try_product(&scale)?,
        &denominator,
    )
}
