use std::num::NonZeroU32;

use super::*;
use crate::geometry::Ring;
use crate::geometry::constructive::WalkJoinRule;
use crate::geometry::constructive::offset::offset_curve_points;

#[cfg(test)]
mod consecutive_dedup_tests {
    use super::*;
    use crate::geometry::constructive::offset::offset_source;
    use crate::geometry::constructive::strict_cycle;
    use crate::geometry::{CoordSeq, CoordinateAxes, LineSeq, Point};

    fn point(x: f64, y: f64) -> Point {
        Point::new(x, y).unwrap()
    }

    fn closed_ring(points: &[(f64, f64)]) -> CoordSeq {
        let mut coords: Vec<Point> = points.iter().map(|&(x, y)| point(x, y)).collect();
        coords.push(coords[0]);
        CoordSeq::try_from_points(&coords).unwrap()
    }

    #[test]
    fn strict_cycle_collapses_consecutive_duplicates() {
        let ring = closed_ring(&[(0.0, 0.0), (0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0)]);
        let strict = strict_cycle(&ring, false).expect("valid ring");
        assert_eq!(strict.len(), 4);
    }

    #[test]
    fn strict_cycle_rejects_degenerate_after_dedup() {
        let ring = closed_ring(&[(0.0, 0.0), (0.0, 0.0), (1.0, 0.0), (1.0, 0.0), (0.0, 0.0)]);
        assert!(strict_cycle(&ring, false).is_none());
    }

    #[test]
    fn offset_source_dedups_before_close_handling() {
        let line = CoordSeq::try_from_points(&[
            point(0.0, 0.0),
            point(0.0, 0.0),
            point(1.0, 0.0),
            point(1.0, 0.0),
        ])
        .unwrap();
        assert!(offset_source(&line).is_some());
        let mut budget = ExpansionBudget::new("offset_curve", "quadrant_segments");
        assert!(
            offset_curve_points(
                &line,
                1.0,
                WalkJoinRule::Arc,
                NonZeroU32::new(8).unwrap(),
                &mut budget,
            )
            .unwrap()
            .is_some()
        );
        let collapsed = CoordSeq::try_from_points(&[point(0.0, 0.0), point(0.0, 0.0)]).unwrap();
        assert!(offset_source(&collapsed).is_none());
    }

    #[test]
    fn snap_to_grid_recloses_ring_after_dedup() {
        let ring = Ring::from_closed(closed_ring(&[
            (0.0, 0.0),
            (0.0, 0.0),
            (2.0, 0.0),
            (2.0, 2.0),
            (0.0, 2.0),
            (0.0, 0.0),
        ]))
        .unwrap();
        let polygon = Shape::Polygon(Polygon::new(ring, Vec::new()));
        let cleaned = polygon.snap_to_grid((1.0, 1.0), (0.0, 0.0)).unwrap();
        let Shape::Polygon(polygon) = cleaned else {
            panic!("expected polygon");
        };
        assert_eq!(polygon.shell.coords().len(), 5);
        let shell = polygon.shell.coords();
        let first = shell.first().expect("closure");
        let last = shell.last().expect("closure");
        assert_eq!(first.x, last.x);
        assert_eq!(first.y, last.y);
    }

    #[test]
    fn empty_results_keep_source_axes() {
        let collapsed_ring = Ring::from_closed(
            CoordSeq::try_from_vecs(
                vec![0.0, 0.0, 0.0, 0.0],
                vec![0.0, 0.0, 0.0, 0.0],
                Some(vec![1.0, 1.0, 1.0, 1.0]),
                None,
            )
            .unwrap(),
        )
        .unwrap();
        let polygon = Shape::Polygon(Polygon::new(collapsed_ring, Vec::new()));

        assert_eq!(polygon.unique_points().unwrap().axes(), CoordinateAxes::XYZ);
        assert_eq!(
            polygon.snap_to_grid((1.0, 1.0), (0.0, 0.0)).unwrap(),
            Shape::typed_empty(GeometryKind::Polygon, CoordinateAxes::XYZ)
        );
        assert_eq!(
            polygon.simplify_vw(1.0, false).unwrap(),
            Shape::typed_empty(GeometryKind::Polygon, CoordinateAxes::XYZ)
        );
        assert_eq!(
            polygon.simplify_dp_raw(1.0).unwrap(),
            Shape::typed_empty(GeometryKind::Polygon, CoordinateAxes::XYZ)
        );
        assert_eq!(
            polygon.remove_repeated_points(1.0).unwrap(),
            Shape::typed_empty(GeometryKind::Polygon, CoordinateAxes::XYZ)
        );
    }

    #[test]
    fn simplify_vw_raw_keeps_repeated_xy_endpoint_ordinates() {
        let line = Shape::LineString(LineSeq::from_proven(
            CoordSeq::try_from_vecs(
                [0.0, 1.0, 0.0].into(),
                [0.0, 0.0, 0.0].into(),
                Some([1.0, 2.0, 9.0].into()),
                Some([10.0, 20.0, 90.0].into()),
            )
            .unwrap(),
        ));
        let Shape::LineString(out) = line.simplify_vw_raw(1.0).unwrap() else {
            panic!("A-B-A VW keep-mask must remain a line");
        };
        assert_eq!(out.axes(), CoordinateAxes::XYZM);
        assert_eq!(out.xs(), &[0.0, 0.0]);
        assert_eq!(out.ys(), &[0.0, 0.0]);
        assert_eq!(out.zs(), Some(&[1.0, 9.0][..]));
        assert_eq!(out.ms(), Some(&[10.0, 90.0][..]));
    }

    #[test]
    fn dropping_collapsed_shell_keeps_xyzm_empty() {
        let collapsed = Ring::from_closed(
            CoordSeq::try_from_vecs(
                [0.0, 0.0, 0.0, 0.0].into(),
                [0.0, 0.0, 0.0, 0.0].into(),
                Some([1.0, 2.0, 3.0, 1.0].into()),
                Some([10.0, 20.0, 30.0, 10.0].into()),
            )
            .unwrap(),
        )
        .unwrap();
        let polygon = Shape::Polygon(Polygon::new(collapsed, Vec::new()));
        assert_eq!(
            polygon.simplify_vw(0.0, false).unwrap(),
            Shape::typed_empty(GeometryKind::Polygon, CoordinateAxes::XYZM)
        );
    }

    #[test]
    fn dropping_collapsed_hole_keeps_the_shell() {
        let shell = Ring::from_closed(
            CoordSeq::try_from_vecs(
                [0.0, 4.0, 0.0, 0.0].into(),
                [0.0, 0.0, 4.0, 0.0].into(),
                None,
                None,
            )
            .unwrap(),
        )
        .unwrap();
        let hole = Ring::from_closed(
            CoordSeq::try_from_vecs(
                [1.0, 1.0, 1.0, 1.0].into(),
                [1.0, 1.0, 1.0, 1.0].into(),
                None,
                None,
            )
            .unwrap(),
        )
        .unwrap();
        let polygon = Shape::Polygon(Polygon::new(shell, vec![hole]));
        let Shape::Polygon(out) = polygon.simplify_vw(0.0, false).unwrap() else {
            panic!("three-corner shell must remain a polygon");
        };
        assert_eq!(out.holes.len(), 0);
        assert_eq!(out.shell.xs(), &[0.0, 4.0, 0.0, 0.0]);
        assert_eq!(out.shell.ys(), &[0.0, 0.0, 4.0, 0.0]);
    }
}
