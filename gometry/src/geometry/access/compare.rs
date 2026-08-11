use crate::geometry::{Bounds, Coordinates, EmptyKind, Ordering, Point, Polygon, Shape};
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
        Shape::Empty(EmptyKind::Point, _) => 7,
        Shape::Empty(EmptyKind::Polygon, _) => 8,
        Shape::Empty(EmptyKind::MultiLineString, _) => 9,
        Shape::Empty(EmptyKind::MultiPolygon, _) => 10,
        Shape::Empty(EmptyKind::GeometryCollection, _) => 11,
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

pub(crate) fn compare_nested_point_slices<T: Coordinates>(left: &[T], right: &[T]) -> Ordering {
    left.iter()
        .zip(right)
        .map(|(left, right)| compare_point_slices(left, right))
        .find(|order| *order != Ordering::Equal)
        .unwrap_or_else(|| left.len().cmp(&right.len()))
}

pub(crate) fn compare_point_slices<A: Coordinates + ?Sized, B: Coordinates + ?Sized>(
    left: &A,
    right: &B,
) -> Ordering {
    // Plain lexicographic order — pointwise, then shorter-first — the
    // Python tuple/string intuition. One comparator backs every canonical
    // sort; deliberately NOT GEOS's count-major legacy order.
    std::iter::zip(left.iter_coords(), right.iter_coords())
        .map(|(left, right)| compare_points(&left, &right))
        .find(|order| *order != Ordering::Equal)
        .unwrap_or_else(|| left.coord_count().cmp(&right.coord_count()))
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
