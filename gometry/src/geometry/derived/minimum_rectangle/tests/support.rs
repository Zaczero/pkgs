//! Shared point and retained-chart fixtures for minimum rectangles.

use super::*;
use crate::geographic::PointSelectionChart;
use crate::geometry::{CoordinateAxes, Shape};

pub(super) fn point(x: f64, y: f64) -> Point {
    Point::new_raw_xy(x, y)
}

pub(super) fn point_selection_chart(points: &[Point]) -> (GeographicFrame, PointSelectionChart) {
    let frame = GeographicFrame::crs84();
    let shape = Shape::multi_point(
        CoordinateAxes::XY,
        CoordSeq::try_from_points(points).expect("point selection coordinates"),
    );
    let chart = PointSelectionChart::try_from_shape(frame, &shape, None)
        .expect("point selection chart")
        .expect("nonempty point support");
    (frame, chart)
}

pub(super) fn chart_hull(
    points: &[Point],
) -> (GeographicFrame, Vec<crate::geographic::ChartPointSite>) {
    let (frame, chart) = point_selection_chart(points);
    let sites = chart
        .sites_from_points(points)
        .expect("retained chart sites");
    let indices =
        crate::geometry::monotone_chain_chart_hull(&sites, frame).expect("common-chart point hull");
    let hull = indices
        .into_iter()
        .map(|index| *sites.get(index).expect("hull site belongs to chart sites"))
        .collect();
    (frame, hull)
}

pub(super) fn hull_vertices(points: &[Point]) -> Vec<Point> {
    super::super::monotone_chain_hull(points)
        .into_iter()
        .map(|index| points[index])
        .collect()
}
