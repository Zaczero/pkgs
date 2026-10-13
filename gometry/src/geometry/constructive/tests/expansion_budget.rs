use super::*;

#[cfg(test)]
mod expansion_budget_threading_tests {
    use super::*;
    use crate::geometry::{GENERATED_ITEM_LIMIT, SmoothMethod};

    fn exhausted(operation: &'static str, parameter: &'static str) -> ExpansionBudget {
        let mut budget = ExpansionBudget::new(operation, parameter);
        budget.add(GENERATED_ITEM_LIMIT).unwrap();
        budget
    }

    fn bent_line() -> Shape {
        Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    Point::new(0.0, 0.0).unwrap(),
                    Point::new(1.0, 0.0).unwrap(),
                    Point::new(1.0, 1.0).unwrap(),
                ])
                .unwrap(),
            )
            .expect("line coordinate sequence must contain at least two vertices"),
        )
    }

    fn straight_line() -> LineSeq {
        LineSeq::try_new(
            CoordSeq::try_from_points(&[
                Point::new(0.0, 0.0).unwrap(),
                Point::new(1.0, 0.0).unwrap(),
            ])
            .unwrap(),
        )
        .expect("line coordinate sequence must contain at least two vertices")
    }

    fn square_polygon(min_x: f64) -> Shape {
        Shape::Polygon(Polygon::new(
            Ring::from_closed(
                CoordSeq::try_from_points(&[
                    Point::new(min_x, 0.0).unwrap(),
                    Point::new(min_x + 2.0, 0.0).unwrap(),
                    Point::new(min_x + 2.0, 2.0).unwrap(),
                    Point::new(min_x, 2.0).unwrap(),
                    Point::new(min_x, 0.0).unwrap(),
                ])
                .unwrap(),
            )
            .unwrap(),
            Vec::new(),
        ))
    }

    #[test]
    fn buffer_and_offset_collection_children_use_the_callers_budget() {
        let style = Positive::try_new("miter_limit", 5.0).unwrap();
        let mut buffer_budget = exhausted("buffer", "quadrant_segments");
        let buffer = Shape::geometry_collection(vec![Shape::Point(Point::new(0.0, 0.0).unwrap())])
            .expect("test collection is valid");
        buffer
            .buffer_with_style_budgeted(
                1.0,
                BufferCapStyle::Round,
                BufferJoinStyle::Round,
                NonZeroU32::new(1).unwrap(),
                style,
                &mut buffer_budget,
            )
            .unwrap_err();

        let mut offset_budget = exhausted("offset_curve", "quadrant_segments");
        Shape::geometry_collection(vec![bent_line()])
            .expect("test collection is valid")
            .offset_curve_budgeted(
                1.0,
                BufferJoinStyle::Round,
                NonZeroU32::new(1).unwrap(),
                style,
                &mut offset_budget,
            )
            .unwrap_err();
    }

    #[test]
    fn smooth_children_and_triangle_emission_use_the_callers_budget() {
        let mut smooth_budget = exhausted("smooth", "iterations");
        Shape::geometry_collection(vec![bent_line()])
            .expect("test collection is valid")
            .smooth_budgeted(1, SmoothMethod::CatmullRom, true, &mut smooth_budget)
            .unwrap_err();

        let mut triangle_budget = exhausted("triangulate", "method");
        Shape::multi_point(
            crate::geometry::CoordinateAxes::XY,
            CoordSeq::try_from_points(&[
                Point::new(0.0, 0.0).unwrap(),
                Point::new(1.0, 0.0).unwrap(),
                Point::new(0.0, 1.0).unwrap(),
            ])
            .unwrap(),
        )
        .delaunay_triangles_budgeted(&mut triangle_budget)
        .unwrap_err();
    }

    #[test]
    fn segmentize_shares_the_callers_budget_across_parts_and_rings() {
        // One generated point per straight part: the first part lands exactly
        // on the cap and the second must observe that same exhausted owner.
        let mut parts_budget = ExpansionBudget::new("segmentize", "max_length");
        parts_budget.add(GENERATED_ITEM_LIMIT - 1).unwrap();
        Shape::multi_line_string(crate::geometry::CoordinateAxes::XY, vec![
            straight_line(),
            straight_line(),
        ])
        .segmentize_budgeted(
            0.5,
            &crate::geometry::SegmentPlacement::Planar,
            &mut parts_budget,
        )
        .unwrap_err();
        assert_eq!(parts_budget.used(), GENERATED_ITEM_LIMIT);

        // A unit square contributes twelve inserted vertices at this step. The
        // hole must reuse the shell's owner rather than start another budget.
        let square = |min: f64, max: f64| {
            Ring::closed(vec![
                Point::new(min, min).unwrap(),
                Point::new(max, min).unwrap(),
                Point::new(max, max).unwrap(),
                Point::new(min, max).unwrap(),
                Point::new(min, min).unwrap(),
            ])
            .unwrap()
        };
        let mut rings_budget = ExpansionBudget::new("segmentize", "max_length");
        rings_budget.add(GENERATED_ITEM_LIMIT - 12).unwrap();
        Shape::Polygon(Polygon::new(square(0.0, 1.0), vec![square(0.25, 0.75)]))
            .segmentize_budgeted(
                0.25,
                &crate::geometry::SegmentPlacement::Planar,
                &mut rings_budget,
            )
            .unwrap_err();
        assert_eq!(rings_budget.used(), GENERATED_ITEM_LIMIT);
    }

    #[test]
    fn collection_erosion_consumes_one_caller_budget_across_all_parts() {
        let collection = Shape::geometry_collection(vec![square_polygon(0.0), square_polygon(5.0)])
            .expect("test collection is valid");
        let style = Positive::try_new("miter_limit", 5.0).unwrap();
        let mut budget = ExpansionBudget::new("buffer", "quadrant_segments");
        budget.add(GENERATED_ITEM_LIMIT - 5).unwrap();
        let error = collection
            .buffer_with_style_budgeted(
                -0.5,
                BufferCapStyle::Round,
                BufferJoinStyle::Round,
                NonZeroU32::new(1).unwrap(),
                style,
                &mut budget,
            )
            .unwrap_err();
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(GeometryErrorKind::GeneratedOutputTooLarge {
                operation: "buffer",
                parameter: "quadrant_segments",
                ..
            })
        ));
        assert_eq!(budget.used(), GENERATED_ITEM_LIMIT - 1);
    }
}
