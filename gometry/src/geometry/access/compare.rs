use crate::geometry::{Bounds, Coordinates, GeometryKind, Ordering, Point, Polygon, Shape, Work};
pub(crate) fn compare_shapes(left: &Shape, right: &Shape) -> Ordering {
    shape_sort_rank(left)
        .cmp(&shape_sort_rank(right))
        .then_with(|| match (left, right) {
            (Shape::Point(left), Shape::Point(right)) => compare_points(left, right),
            (Shape::MultiPoint(left), Shape::MultiPoint(right)) => {
                compare_point_slices(left, right)
            },
            (Shape::LineString(left), Shape::LineString(right)) => {
                compare_point_slices(left, right)
            },
            (Shape::MultiLineString(left), Shape::MultiLineString(right)) => {
                compare_nested_point_slices(left, right)
            },
            (Shape::Polygon(left), Shape::Polygon(right)) => compare_polygons(left, right),
            (Shape::MultiPolygon(left), Shape::MultiPolygon(right)) => {
                compare_polygon_slices(left, right)
            },
            (Shape::GeometryCollection(left), Shape::GeometryCollection(right)) => {
                compare_shape_slices(left, right)
            },
            // Same-kind typed empties order by their axes bits so the total
            // order stays deterministic across XY/Z/M/ZM empties.
            (Shape::Empty(_, left), Shape::Empty(_, right)) => left.bits().cmp(&right.bits()),
            _ => Ordering::Equal,
        })
}

pub(crate) const fn shape_sort_rank(shape: &Shape) -> u8 {
    match shape {
        Shape::Point(_) => 0,
        Shape::MultiPoint(_) => 1,
        Shape::LineString(_) => 2,
        Shape::MultiLineString(_) => 3,
        Shape::Polygon(_) => 4,
        Shape::MultiPolygon(_) => 5,
        Shape::GeometryCollection(_) => 6,
        Shape::Empty(GeometryKind::Point, _) => 7,
        Shape::Empty(GeometryKind::Polygon, _) => 8,
        Shape::Empty(GeometryKind::MultiLineString, _) => 9,
        Shape::Empty(GeometryKind::MultiPolygon, _) => 10,
        Shape::Empty(GeometryKind::GeometryCollection, _) => 11,
        Shape::Empty(GeometryKind::MultiPoint, _) => 12,
        Shape::Empty(GeometryKind::LineString, _) => 13,
    }
}

pub(crate) fn compare_shape_slices(left: &[Shape], right: &[Shape]) -> Ordering {
    left.iter()
        .zip(right)
        .map(|(left, right)| compare_shapes(left, right))
        .find(|order| *order != Ordering::Equal)
        .unwrap_or_else(|| left.len().cmp(&right.len()))
}

pub(crate) fn compare_polygon_slices(left: &[Polygon], right: &[Polygon]) -> Ordering {
    left.iter()
        .zip(right)
        .map(|(left, right)| compare_polygons(left, right))
        .find(|order| *order != Ordering::Equal)
        .unwrap_or_else(|| left.len().cmp(&right.len()))
}

pub(crate) fn compare_polygons(left: &Polygon, right: &Polygon) -> Ordering {
    compare_point_slices(&left.shell, &right.shell)
        .then_with(|| compare_nested_point_slices(&left.holes, &right.holes))
}

pub(crate) fn compare_polygons_with_work(
    left: &Polygon,
    right: &Polygon,
    work: &mut impl Work,
) -> crate::error::Result<Ordering> {
    let shell_order = compare_point_slices_with_work(&left.shell, &right.shell, work)?;
    if shell_order != Ordering::Equal {
        return Ok(shell_order);
    }
    compare_nested_point_slices_with_work(&left.holes, &right.holes, work)
}

pub(crate) fn compare_nested_point_slices<T: Coordinates>(left: &[T], right: &[T]) -> Ordering {
    left.iter()
        .zip(right)
        .map(|(left, right)| compare_point_slices(left, right))
        .find(|order| *order != Ordering::Equal)
        .unwrap_or_else(|| left.len().cmp(&right.len()))
}

pub(crate) fn compare_nested_point_slices_with_work<T: Coordinates>(
    left: &[T],
    right: &[T],
    work: &mut impl Work,
) -> crate::error::Result<Ordering> {
    let common_len = left.len().min(right.len());
    for index in 0..common_len {
        work.charge_into_error(1)?;
        let left = left
            .get(index)
            .expect("the common nested prefix is in range");
        let right = right
            .get(index)
            .expect("the common nested prefix is in range");
        let order = compare_point_slices_with_work(left, right, work)?;
        if order != Ordering::Equal {
            return Ok(order);
        }
    }
    Ok(left.len().cmp(&right.len()))
}

pub(crate) fn compare_point_slices<A: Coordinates + ?Sized, B: Coordinates + ?Sized>(
    left: &A,
    right: &B,
) -> Ordering {
    // Plain lexicographic order — pointwise, then shorter-first — gives every
    // canonical sort one total comparator.
    std::iter::zip(left.iter_coords(), right.iter_coords())
        .map(|(left, right)| compare_points(&left, &right))
        .find(|order| *order != Ordering::Equal)
        .unwrap_or_else(|| left.coord_count().cmp(&right.coord_count()))
}

pub(crate) fn compare_point_slices_with_work<A: Coordinates + ?Sized, B: Coordinates + ?Sized>(
    left: &A,
    right: &B,
    work: &mut impl Work,
) -> crate::error::Result<Ordering> {
    let common_len = left.coord_count().min(right.coord_count());
    for index in 0..common_len {
        work.charge_into_error(1)?;
        let left = left.nth_coord(index);
        let right = right.nth_coord(index);
        let order = compare_points(&left, &right);
        if order != Ordering::Equal {
            return Ok(order);
        }
    }
    Ok(left.coord_count().cmp(&right.coord_count()))
}

pub(crate) fn compare_points(left: &Point, right: &Point) -> Ordering {
    compare_f64(left.x, right.x)
        .then_with(|| compare_f64(left.y, right.y))
        .then_with(|| compare_optional_f64(left.z(), right.z()))
        .then_with(|| compare_optional_f64(left.m(), right.m()))
}

pub(crate) fn compare_optional_f64(left: Option<f64>, right: Option<f64>) -> Ordering {
    match (left, right) {
        (Some(left), Some(right)) => compare_f64(left, right),
        (Some(_), None) => Ordering::Greater,
        (None, Some(_)) => Ordering::Less,
        (None, None) => Ordering::Equal,
    }
}

pub(crate) fn compare_f64(left: f64, right: f64) -> Ordering {
    left.total_cmp(&right)
}

pub(crate) fn bounds_from_iter(bounds: impl IntoIterator<Item = Bounds>) -> Option<Bounds> {
    let mut bounds = bounds.into_iter();
    let mut result = bounds.next()?;
    for item in bounds {
        result.include_bounds(item);
    }
    Some(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geometry::embedding_search::{EvaluationBudget, EvaluationOnly};
    use crate::geometry::{CoordinateAxes, GeometryErrorKind, Unmetered};

    struct RecordingBudget {
        budget: EvaluationBudget,
        attempted: Vec<u64>,
        accepted: Vec<u64>,
    }

    impl RecordingBudget {
        fn with_remaining(remaining_ticks: u128) -> crate::error::Result<Self> {
            Ok(Self {
                budget: EvaluationBudget::with_remaining_ticks_for_test(
                    "coordinate comparison",
                    1,
                    remaining_ticks,
                )?,
                attempted: Vec::new(),
                accepted: Vec::new(),
            })
        }
    }

    impl Work for RecordingBudget {
        type Error = crate::error::Error;

        fn charge(&mut self, ticks: u64) -> crate::error::Result<()> {
            self.attempted.push(ticks);
            self.budget.charge(ticks)?;
            self.accepted.push(ticks);
            Ok(())
        }

        fn charge_product(
            &mut self,
            count: usize,
            ticks_per_item: usize,
        ) -> crate::error::Result<()> {
            self.charge(crate::geometry::work_product_ticks(count, ticks_per_item)?)
        }
    }

    #[test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "assertions pin refusal before coordinate comparison and its work record"
    )]
    fn point_prefix_refusal_precedes_coordinate_access() -> crate::error::Result<()> {
        let left = crate::geometry::CoordSeq::try_from_points(&[
            Point::new_raw_xy(0.0, 0.0),
            Point::new_raw_xy(1.0, 0.0),
        ])?;
        let right = crate::geometry::CoordSeq::try_from_points(&[
            Point::new_raw_xy(0.0, 0.0),
            Point::new_raw_xy(2.0, 0.0),
        ])?;
        let mut work = RecordingBudget::with_remaining(0)?;

        let error = compare_point_slices_with_work(&left, &right, &mut work).unwrap_err();

        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
                operation: "coordinate comparison",
                constructed: 1,
                ..
            })
        ));
        assert_eq!(work.attempted, [1]);
        assert!(work.accepted.is_empty());
        Ok(())
    }

    #[test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "assertions pin refusal before nested row comparison and its work record"
    )]
    fn hole_prefix_refusal_precedes_nested_row_access() -> crate::error::Result<()> {
        let left = [crate::geometry::CoordSeq::empty(CoordinateAxes::XY)];
        let right = [crate::geometry::CoordSeq::empty(CoordinateAxes::XY)];
        let mut work = RecordingBudget::with_remaining(0)?;

        let error = compare_nested_point_slices_with_work(&left, &right, &mut work).unwrap_err();

        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
                operation: "coordinate comparison",
                constructed: 1,
                ..
            })
        ));
        assert_eq!(work.attempted, [1]);
        assert!(work.accepted.is_empty());
        Ok(())
    }

    #[test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "assertions pin comparison order and EvaluationOnly work erasure"
    )]
    fn evaluation_only_comparison_keeps_order_and_erases_raw_work() -> crate::error::Result<()> {
        let left = crate::geometry::CoordSeq::try_from_points(&[
            Point::new_raw_xy(0.0, 0.0),
            Point::new_raw_xy(1.0, 0.0),
        ])?;
        let right = crate::geometry::CoordSeq::try_from_points(&[
            Point::new_raw_xy(0.0, 0.0),
            Point::new_raw_xy(2.0, 0.0),
        ])?;
        let expected = compare_point_slices(&left, &right);
        let mut unmetered = Unmetered;
        assert_eq!(
            compare_point_slices_with_work(&left, &right, &mut unmetered)?,
            expected
        );

        let mut budget =
            EvaluationBudget::with_remaining_ticks_for_test("coordinate comparison", 1, 1)?;
        {
            let mut work = EvaluationOnly::new(&mut budget);
            assert_eq!(
                compare_point_slices_with_work(&left, &right, &mut work)?,
                expected
            );
        }
        <EvaluationBudget as Work>::charge(&mut budget, 1)?;
        assert!(
            <EvaluationBudget as Work>::charge(&mut budget, 1).is_err(),
            "EvaluationOnly must not spend raw comparison work"
        );
        Ok(())
    }
}
