use super::*;

#[cfg(test)]
mod parts_iter_tests {
    use super::*;

    #[test]
    fn parts_iterator_is_exact_and_exhausts_from_both_ends() {
        let points = [
            Point::new_raw_xy(1.0, 2.0),
            Point::new_raw_xy(3.0, 4.0),
            Point::new_raw_xy(5.0, 6.0),
        ];
        let shape = Shape::MultiPoint(points.to_vec().into());
        let mut parts = shape.parts();
        assert_eq!(parts.len(), 3);
        assert_eq!(parts.next(), Some(Shape::Point(points[0])));
        assert_eq!(parts.len(), 2);
        assert_eq!(parts.next_back(), Some(Shape::Point(points[2])));
        assert_eq!(parts.next(), Some(Shape::Point(points[1])));
        assert_eq!(parts.len(), 0);
        assert_eq!(parts.next(), None);
        assert_eq!(parts.next_back(), None);

        assert_eq!(
            Shape::Empty(GeometryKind::Point, CoordinateAxes::XY)
                .parts()
                .len(),
            1
        );
    }
}
