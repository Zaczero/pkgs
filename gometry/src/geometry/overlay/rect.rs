use crate::geometry::{Bounds, CoordSeq, CoordinateAxes, Point, Polygon, Ring, Shape};

pub(crate) fn rect_polygon(rect: Bounds) -> crate::error::Result<Polygon> {
    let mut shell = Vec::with_capacity(5);
    for point in [
        Point::new_raw_xy(rect.minx(), rect.miny()),
        Point::new_raw_xy(rect.maxx(), rect.miny()),
        Point::new_raw_xy(rect.maxx(), rect.maxy()),
        Point::new_raw_xy(rect.minx(), rect.maxy()),
        Point::new_raw_xy(rect.minx(), rect.miny()),
    ] {
        shell.push(point);
    }
    Ok(Polygon::new(
        Ring::try_from_closed(CoordSeq::try_from_points(&shell)?)?,
        Vec::new(),
    ))
}

#[expect(
    clippy::float_cmp,
    reason = "rectangle recognition compares exact stored corners; approximate rings are not box operands"
)]
pub(crate) fn rectangle_bounds(shape: &Shape) -> Option<Bounds> {
    let Shape::Polygon(polygon) = shape else {
        return None;
    };
    if !polygon.holes.is_empty() || polygon.axes() != CoordinateAxes::XY {
        return None;
    }
    let ring = polygon.shell.coords();
    if ring.len() != 5 {
        return None;
    }
    let (lower_left, lower_right, upper_right, upper_left, close) = (
        ring.point_at(0),
        ring.point_at(1),
        ring.point_at(2),
        ring.point_at(3),
        ring.point_at(4),
    );
    (lower_left == close
        && lower_left.y == lower_right.y
        && lower_right.x == upper_right.x
        && upper_right.y == upper_left.y
        && upper_left.x == lower_left.x
        && lower_left.x <= lower_right.x
        && lower_left.y <= upper_left.y)
        .then_some(Bounds::new_raw(
            lower_left.x,
            lower_left.y,
            lower_right.x,
            upper_right.y,
        ))
}
