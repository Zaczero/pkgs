use super::*;

pub(super) fn cached_planar_locate(
    prepared: &crate::geometry::PreparedPlan,
    point: Point,
    normalized: bool,
) -> crate::error::Result<f64> {
    prepared
        .line_index()?
        .locate_point_prepared(point, normalized, || prepared.distance_parts())
}

pub(super) fn assert_locate_interpolate_matches_proximity(
    prepared: &crate::geometry::PreparedPlan,
    probe: Point,
) {
    let station = cached_planar_locate(prepared, probe, false).unwrap();
    let Shape::Point(interpolated) = prepared.line_interpolate_point(station, false).unwrap()
    else {
        unreachable!("line interpolation returns a point")
    };
    let query = crate::geometry::PreparedPlan::new(Shape::Point(probe));
    let (_, nearest) = query.nearest_points(prepared).unwrap().unwrap();
    assert_eq!(interpolated.x.to_bits(), nearest.x.to_bits());
    assert_eq!(interpolated.y.to_bits(), nearest.y.to_bits());
}
