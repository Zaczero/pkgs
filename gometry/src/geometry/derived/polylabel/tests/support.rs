fn circle_points(center: Point, radius: f64) -> Vec<Point> {
    let mut points = Vec::with_capacity(64);
    for step in 0..64 {
        let (unit_x, unit_y) = if step % 16 == 0 {
            match (step / 16) % 4 {
                0 => (1.0, 0.0),
                1 => (0.0, 1.0),
                2 => (-1.0, 0.0),
                _ => (0.0, -1.0),
            }
        } else {
            let (sin, cos) = (std::f64::consts::TAU * f64::from(step) / 64.0).sin_cos();
            (cos, sin)
        };
        points.push(Point::new_raw_xy(
            center.x + radius * unit_x,
            center.y + radius * unit_y,
        ));
    }
    points
}

fn degenerate_line_shape() -> Shape {
    Shape::Polygon(Polygon::new(
        Ring::from_closed(
            crate::geometry::CoordSeq::try_from_points(&[
                Point::new_raw_xy(0.0, 0.0),
                Point::new_raw_xy(2.0, 0.0),
                Point::new_raw_xy(1.0, 0.0),
                Point::new_raw_xy(0.0, 0.0),
            ])
            .unwrap(),
        )
        .unwrap(),
        Vec::new(),
    ))
}

fn dense_ring_capacity(subdivisions: usize) -> usize {
    subdivisions
        .checked_add(1)
        .and_then(|count| count.checked_mul(2))
        .and_then(|count| count.checked_add(1))
        .expect("dense test-ring coordinate count fits usize")
}

fn dense_reciprocal_polygon(subdivisions: usize) -> Shape {
    let extent = 1.0e162;
    let half_height = 1.0e-162;
    let mut points = Vec::with_capacity(dense_ring_capacity(subdivisions));
    for index in 0..=subdivisions {
        let fraction = index as f64 / subdivisions as f64;
        let x = -extent + 2.0 * extent * fraction;
        points.push(Point::new_raw_xy(x, -half_height));
    }
    for index in (0..=subdivisions).rev() {
        let fraction = index as f64 / subdivisions as f64;
        let x = -extent + 2.0 * extent * fraction;
        points.push(Point::new_raw_xy(x, half_height));
    }
    points.push(points[0]);
    Shape::Polygon(Polygon::new(
        Ring::from_closed(CoordSeq::try_from_points(&points).unwrap()).unwrap(),
        Vec::new(),
    ))
}
