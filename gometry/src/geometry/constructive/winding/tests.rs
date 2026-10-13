use super::*;

#[cfg(test)]
mod winding_outcome_tests {
    use super::*;
    use crate::geometry::constructive::convex_buffer_budgeted;

    fn square() -> Polygon {
        Polygon::new(
            Ring::from_closed(
                CoordSeq::try_from_points(&[
                    Point::new_raw_xy(0.0, 0.0),
                    Point::new_raw_xy(4.0, 0.0),
                    Point::new_raw_xy(4.0, 4.0),
                    Point::new_raw_xy(0.0, 4.0),
                    Point::new_raw_xy(0.0, 0.0),
                ])
                .unwrap(),
            )
            .unwrap(),
            Vec::new(),
        )
    }

    #[test]
    fn geometric_declines_and_annihilation_remain_non_errors() {
        let polygon = square();
        let mut budget = ExpansionBudget::new("buffer", "quadrant_segments");
        assert!(
            convex_buffer_budgeted(
                &polygon,
                -3.0,
                WalkJoinRule::Arc,
                NonZeroU32::new(4).unwrap(),
                &mut budget,
            )
            .unwrap()
            .is_none()
        );

        let empty = Shape::Point(Point::new_raw_xy(0.0, 0.0))
            .buffer(-1.0)
            .unwrap();
        assert!(empty.is_empty());
    }

    #[test]
    fn preconstruction_convex_shortcut_does_not_charge_generated_output() {
        let polygon = square();
        let mut budget = ExpansionBudget::new("buffer", "quadrant_segments");
        reset_walk_plan_construction_count();
        assert!(
            convex_buffer_budgeted(
                &polygon,
                0.0,
                WalkJoinRule::Arc,
                NonZeroU32::new(8).unwrap(),
                &mut budget,
            )
            .unwrap()
            .is_none()
        );
        assert_eq!(budget.used(), 0);
        assert_eq!(walk_plan_construction_count(), 0);
    }
}
