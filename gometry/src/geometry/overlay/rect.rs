use crate::geometry::{Bounds, CoordSeq, Point, Polygon, Ring};

pub(crate) fn rect_polygon(rect: Bounds) -> crate::error::Result<Polygon> {
    let shell = vec![
        Point::new_raw_xy(rect.minx(), rect.miny()),
        Point::new_raw_xy(rect.maxx(), rect.miny()),
        Point::new_raw_xy(rect.maxx(), rect.maxy()),
        Point::new_raw_xy(rect.minx(), rect.maxy()),
        Point::new_raw_xy(rect.minx(), rect.miny()),
    ];
    Polygon::new(
        Ring::from_closed(CoordSeq::try_from_points(&shell)?),
        Vec::new(),
    )
}
