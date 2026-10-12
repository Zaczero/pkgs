use geographiclib_rs::Geodesic;

use super::ChartEdge;
use crate::geographic::{AngularFactor, GeographicAxisOrientation, GeographicFrame};
use crate::geometry::Point;

pub(super) fn degrees() -> GeographicFrame {
    GeographicFrame::crs84()
}

pub(super) fn grads() -> GeographicFrame {
    let factor = AngularFactor::try_new("grad", std::f64::consts::PI / 200.0).unwrap();
    GeographicFrame::try_new(factor, factor, 0.0).unwrap()
}

pub(super) fn radians() -> GeographicFrame {
    let factor = AngularFactor::try_new("radian", 1.0).unwrap();
    GeographicFrame::try_new(factor, factor, 0.0).unwrap()
}

pub(super) fn south_axis_degrees() -> GeographicFrame {
    let factor = AngularFactor::try_new("degree", std::f64::consts::PI / 180.0).unwrap();
    GeographicFrame::try_new_with_orientation(
        factor,
        factor,
        0.0,
        GeographicAxisOrientation::from_axis_directions(true, false),
    )
    .unwrap()
}

pub(super) fn wgs84() -> Geodesic {
    Geodesic::new(6_378_137.0, 1.0 / 298.257_223_563)
}

pub(super) fn edge_length(
    frame: GeographicFrame,
    start: (f64, f64),
    end: (f64, f64),
) -> crate::error::Result<f64> {
    ChartEdge::new(
        Point::new_raw_xy(start.0, start.1),
        Point::new_raw_xy(end.0, end.1),
        frame,
    )?
    .length(&wgs84())
}
