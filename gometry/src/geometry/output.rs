//! Canonical construction and ordering for certified geometry output.

use crate::geometry::segments::ordering::try_sort_by_with_work;
use crate::geometry::{CoordSeq, CoordinateAxes, LineSeq, Point, Polygon, Shape};
use crate::work::Work;

/// Serialize an already certified exact-partition plan without reclassifying
/// points or lines against rounded polygon coordinates. The exact selector has
/// already performed dimensional absorption, so any further coverage test here
/// would be a second topology owner.
pub(crate) fn build_certified_shape(
    points: Vec<Point>,
    lines: Vec<CoordSeq>,
    polygons: Vec<Polygon>,
) -> crate::error::Result<Shape> {
    build_certified_shape_with_axes(points, lines, polygons, CoordinateAxes::XY)
}

pub(crate) fn build_certified_shape_with_axes(
    mut points: Vec<Point>,
    mut lines: Vec<CoordSeq>,
    mut polygons: Vec<Polygon>,
    axes: CoordinateAxes,
) -> crate::error::Result<Shape> {
    points.sort_unstable_by(crate::geometry::compare_points);
    lines.sort_unstable_by(crate::geometry::compare_point_slices);
    polygons.sort_unstable_by(crate::geometry::compare_polygons);
    assemble_sorted_overlay_shape(points, lines, polygons, axes)
}

pub(crate) struct CertifiedShapeOrder {
    pub(crate) points: Vec<usize>,
    pub(crate) lines: Vec<usize>,
    pub(crate) polygons: Vec<usize>,
}

pub(crate) fn build_certified_shape_with_axes_and_order<W: Work>(
    points: Vec<Point>,
    lines: Vec<CoordSeq>,
    polygons: Vec<Polygon>,
    axes: CoordinateAxes,
    work: &mut W,
) -> crate::error::Result<(Shape, CertifiedShapeOrder)> {
    work.charge_into_error(
        u64::try_from(points.len()).map_err(|_| crate::error::ResourceError::SizeOverflow)?,
    )?;
    let mut ordered_points = Vec::with_capacity(points.len());
    for (index, point) in points.into_iter().enumerate() {
        work.charge_into_error(1)?;
        ordered_points.push((index, point));
    }
    try_sort_by_with_work(&mut ordered_points, work, |left, right, work| {
        work.charge_into_error(1)?;
        Ok(crate::geometry::compare_points(&left.1, &right.1).then_with(|| left.0.cmp(&right.0)))
    })?;
    work.charge_into_error(
        u64::try_from(lines.len()).map_err(|_| crate::error::ResourceError::SizeOverflow)?,
    )?;
    let mut ordered_lines = Vec::with_capacity(lines.len());
    for (index, line) in lines.into_iter().enumerate() {
        work.charge_into_error(1)?;
        ordered_lines.push((index, line));
    }
    try_sort_by_with_work(&mut ordered_lines, work, |left, right, work| {
        work.charge_into_error(1)?;
        Ok(
            crate::geometry::compare_point_slices_with_work(&left.1, &right.1, work)?
                .then_with(|| left.0.cmp(&right.0)),
        )
    })?;
    work.charge_into_error(
        u64::try_from(polygons.len()).map_err(|_| crate::error::ResourceError::SizeOverflow)?,
    )?;
    let mut ordered_polygons = Vec::with_capacity(polygons.len());
    for (index, polygon) in polygons.into_iter().enumerate() {
        work.charge_into_error(1)?;
        ordered_polygons.push((index, polygon));
    }
    try_sort_by_with_work(&mut ordered_polygons, work, |left, right, work| {
        work.charge_into_error(1)?;
        Ok(
            crate::geometry::compare_polygons_with_work(&left.1, &right.1, work)?
                .then_with(|| left.0.cmp(&right.0)),
        )
    })?;
    let ordered_count = ordered_points
        .len()
        .checked_add(ordered_lines.len())
        .and_then(|count| count.checked_add(ordered_polygons.len()))
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    work.charge_into_error(
        u64::try_from(ordered_count).map_err(|_| crate::error::ResourceError::SizeOverflow)?,
    )?;
    let mut point_order = Vec::with_capacity(ordered_points.len());
    let mut points = Vec::with_capacity(ordered_points.len());
    for (index, point) in ordered_points {
        work.charge_into_error(1)?;
        point_order.push(index);
        points.push(point);
    }
    let mut line_order = Vec::with_capacity(ordered_lines.len());
    let mut lines = Vec::with_capacity(ordered_lines.len());
    for (index, line) in ordered_lines {
        work.charge_into_error(1)?;
        line_order.push(index);
        lines.push(line);
    }
    let mut polygon_order = Vec::with_capacity(ordered_polygons.len());
    let mut polygons = Vec::with_capacity(ordered_polygons.len());
    for (index, polygon) in ordered_polygons {
        work.charge_into_error(1)?;
        polygon_order.push(index);
        polygons.push(polygon);
    }
    let shape = assemble_sorted_overlay_shape(points, lines, polygons, axes)?;
    Ok((shape, CertifiedShapeOrder {
        points: point_order,
        lines: line_order,
        polygons: polygon_order,
    }))
}

fn assemble_sorted_overlay_shape(
    points: Vec<Point>,
    lines: Vec<CoordSeq>,
    polygons: Vec<Polygon>,
    axes: CoordinateAxes,
) -> crate::error::Result<Shape> {
    let point_part = (!points.is_empty())
        .then(|| Shape::narrow_points(axes, points))
        .transpose()?;
    let line_part = if lines.is_empty() {
        None
    } else {
        let mut proven = Vec::with_capacity(lines.len());
        for line in lines {
            proven.push(LineSeq::from_proven(line));
        }
        Some(Shape::narrow_lines(axes, proven))
    };
    let polygon_part = (!polygons.is_empty()).then(|| Shape::narrow_polygons(axes, polygons));
    let mut parts = Vec::with_capacity(3);
    parts.extend([point_part, line_part, polygon_part].into_iter().flatten());
    match <[Shape; 1]>::try_from(parts) {
        Ok([single]) => Ok(single),
        Err(parts) => Shape::geometry_collection(parts),
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::geometry::Ring;
    use crate::geometry::embedding_search::EvaluationBudget;

    struct RecordingBudget {
        budget: EvaluationBudget,
        charged: u128,
    }
    impl Work for RecordingBudget {
        type Error = crate::error::Error;

        fn charge(&mut self, ticks: u64) -> crate::error::Result<()> {
            self.budget.charge(ticks)?;
            self.charged = self.charged.wrapping_add(u128::from(ticks));
            Ok(())
        }

        fn charge_product(
            &mut self,
            count: usize,
            ticks_per_item: usize,
        ) -> crate::error::Result<()> {
            self.charge(crate::work::work_product_ticks(count, ticks_per_item)?)
        }
    }
    fn line(points: &[(f64, f64)]) -> crate::error::Result<CoordSeq> {
        let points = points
            .iter()
            .map(|&(x, y)| Point::new_raw_xy(x, y))
            .collect::<Vec<_>>();
        CoordSeq::try_from_points(&points)
    }
    fn sort_lines(lines: Vec<CoordSeq>) -> crate::error::Result<(Vec<usize>, u128)> {
        let mut work = RecordingBudget {
            budget: EvaluationBudget::new("literal prefix", 1)?,
            charged: 0,
        };
        let (_, order) = build_certified_shape_with_axes_and_order(
            Vec::new(),
            lines,
            Vec::new(),
            CoordinateAxes::XY,
            &mut work,
        )?;
        Ok((order.lines, work.charged))
    }
    fn polygon_with_hole(hole: &[(f64, f64)]) -> crate::error::Result<Polygon> {
        let shell =
            Ring::try_from_closed(line(&[(0.0, 0.0), (2.0, 0.0), (2.0, 2.0), (0.0, 0.0)])?)?;
        let hole = Ring::try_from_closed(line(hole)?)?;
        Ok(Polygon::new(shell, vec![hole]))
    }
    fn sort_polygons(polygons: Vec<Polygon>) -> crate::error::Result<(Vec<usize>, u128)> {
        let mut work = RecordingBudget {
            budget: EvaluationBudget::new("literal prefix", 1)?,
            charged: 0,
        };
        let (_, order) = build_certified_shape_with_axes_and_order(
            Vec::new(),
            Vec::new(),
            polygons,
            CoordinateAxes::XY,
            &mut work,
        )?;
        Ok((order.polygons, work.charged))
    }
    #[test]
    fn certified_overlay_order_tracks_sorted_point_slots() {
        let input = [Point::new_raw_xy(2.0, 0.0), Point::new_raw_xy(1.0, 0.0)];
        let mut work = crate::work::Unmetered;
        let (shape, order) = build_certified_shape_with_axes_and_order(
            input.to_vec(),
            Vec::new(),
            Vec::new(),
            CoordinateAxes::XY,
            &mut work,
        )
        .unwrap();
        assert_eq!(order.points, [1, 0]);
        let Shape::MultiPoint(points) = shape else {
            panic!("two certified points did not produce a MultiPoint");
        };
        assert_eq!(points.point_at(0).x, 1.0);
        assert_eq!(points.point_at(1).x, 2.0);
    }
    #[test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "assertions pin overlay sort order and prefix-comparison work"
    )]
    fn certified_overlay_sort_funds_long_line_and_hole_prefixes() -> crate::error::Result<()> {
        let long_line_low = line(&[
            (0.0, 0.0),
            (1.0, 0.0),
            (2.0, 0.0),
            (3.0, 0.0),
            (4.0, 0.0),
            (5.0, 0.0),
            (6.0, 0.0),
            (7.0, 0.0),
            (8.0, 0.0),
        ])?;
        let long_line_high = line(&[
            (0.0, 0.0),
            (1.0, 0.0),
            (2.0, 0.0),
            (3.0, 0.0),
            (4.0, 0.0),
            (5.0, 0.0),
            (6.0, 0.0),
            (7.0, 0.0),
            (9.0, 0.0),
        ])?;
        let (line_order, long_line_work) = sort_lines(vec![long_line_high, long_line_low])?;
        assert_eq!(line_order, [1, 0]);

        let short_line_low = line(&[
            (0.0, 0.0),
            (1.0, 0.0),
            (2.0, 0.0),
            (3.0, 0.0),
            (4.0, 0.0),
            (5.0, 0.0),
            (6.0, 0.0),
            (7.0, 0.0),
            (8.0, 0.0),
        ])?;
        let short_line_high = line(&[
            (10.0, 0.0),
            (1.0, 0.0),
            (2.0, 0.0),
            (3.0, 0.0),
            (4.0, 0.0),
            (5.0, 0.0),
            (6.0, 0.0),
            (7.0, 0.0),
            (9.0, 0.0),
        ])?;
        let (_, short_line_work) = sort_lines(vec![short_line_high, short_line_low])?;
        assert!(
            long_line_work > short_line_work,
            "equal-size lines with a longer common prefix must spend more comparison work"
        );

        let long_hole_low = polygon_with_hole(&[
            (0.0, 1.0),
            (1.0, 1.0),
            (2.0, 1.0),
            (3.0, 1.0),
            (4.0, 1.0),
            (5.0, 1.0),
            (6.0, 1.0),
            (7.0, 1.0),
            (0.0, 1.0),
        ])?;
        let long_hole_high = polygon_with_hole(&[
            (0.0, 1.0),
            (1.0, 1.0),
            (2.0, 1.0),
            (3.0, 1.0),
            (4.0, 1.0),
            (5.0, 1.0),
            (6.0, 1.0),
            (8.0, 1.0),
            (0.0, 1.0),
        ])?;
        let (hole_order, long_hole_work) = sort_polygons(vec![long_hole_high, long_hole_low])?;
        assert_eq!(hole_order, [1, 0]);

        let short_hole_low = polygon_with_hole(&[
            (0.0, 1.0),
            (1.0, 1.0),
            (2.0, 1.0),
            (3.0, 1.0),
            (4.0, 1.0),
            (5.0, 1.0),
            (6.0, 1.0),
            (7.0, 1.0),
            (0.0, 1.0),
        ])?;
        let short_hole_high = polygon_with_hole(&[
            (0.0, 1.0),
            (10.0, 1.0),
            (2.0, 1.0),
            (3.0, 1.0),
            (4.0, 1.0),
            (5.0, 1.0),
            (6.0, 1.0),
            (8.0, 1.0),
            (0.0, 1.0),
        ])?;
        let (_, short_hole_work) = sort_polygons(vec![short_hole_high, short_hole_low])?;
        assert!(
            long_hole_work > short_hole_work,
            "equal-size polygons with a longer common hole prefix must spend more comparison work"
        );
        Ok(())
    }
}
