use crate::geometry::{Bounds, Point, Polygon, Ring};

pub(crate) fn rect_polygon(rect: Bounds) -> Polygon {
    let shell = vec![
        Point::new_raw_xy(rect.minx(), rect.miny()),
        Point::new_raw_xy(rect.maxx(), rect.miny()),
        Point::new_raw_xy(rect.maxx(), rect.maxy()),
        Point::new_raw_xy(rect.minx(), rect.maxy()),
        Point::new_raw_xy(rect.minx(), rect.miny()),
    ];
    Polygon::new(Ring::from_closed(shell), Vec::new())
}
