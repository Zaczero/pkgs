use crate::geometry::{Bounds, CoordSeq, Point, Polygon, Ring};
use crate::{try_push_vec, try_reserve_vec};

pub(crate) fn rect_polygon(rect: Bounds) -> crate::error::Result<Polygon> {
    let mut shell = Vec::new();
    try_reserve_vec(&mut shell, 5)?;
    for point in [
        Point::new_raw_xy(rect.minx(), rect.miny()),
        Point::new_raw_xy(rect.maxx(), rect.miny()),
        Point::new_raw_xy(rect.maxx(), rect.maxy()),
        Point::new_raw_xy(rect.minx(), rect.maxy()),
        Point::new_raw_xy(rect.minx(), rect.miny()),
    ] {
        try_push_vec(&mut shell, point)?;
    }
    Polygon::new(
        Ring::try_from_closed(CoordSeq::try_from_points(&shell)?)?,
        Vec::new(),
    )
}
