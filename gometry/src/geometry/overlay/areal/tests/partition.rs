use super::test_support::polygon;
use crate::geometry::Shape;
use crate::geometry::overlay::OverlayOp;

/// The bounds-component partition is an optimization, so the only
/// evidence it can give is agreement with the unpartitioned lane on
/// inputs that actually split: several clusters of overlapping blobs
/// with clear water between them, plus a lone empty input and a lone
/// point so singleton components of every dimension are exercised.
#[test]
fn partitioned_union_matches_the_single_arrangement() {
    use crate::geometry::{Strictness, TopologyDomain};
    let blob = |cx: f64, cy: f64, r: f64| -> Shape {
        let points: Vec<(f64, f64)> = (0..12)
            .map(|i| {
                let theta = f64::from(i) / 12.0 * std::f64::consts::TAU;
                (cx + r * theta.cos(), cy + r * theta.sin())
            })
            .chain(std::iter::once((cx + r, cy)))
            .collect();
        Shape::Polygon(polygon(&points))
    };
    let mut inputs: Vec<Shape> = Vec::new();
    // Three clusters, each internally overlapping, far apart from each other.
    for (ox, oy) in [(0.0, 0.0), (100.0, 0.0), (0.0, 100.0)] {
        for k in 0..5 {
            inputs.push(blob(ox + 1.3 * f64::from(k), oy + 0.7 * f64::from(k), 1.5));
        }
    }
    inputs.push(Shape::Point(crate::geometry::Point::new_raw_xy(50.0, 50.0)));
    inputs.push(Shape::empty_polygon());
    for op in [OverlayOp::Union, OverlayOp::SymmetricDifference] {
        let partitioned = super::nary::overlay_all_with_bounds_certificates(
            &inputs,
            TopologyDomain::Cartesian,
            op,
            Strictness::Lenient,
            true,
        )
        .unwrap();
        let single = super::nary::overlay_all_with_bounds_certificates(
            &inputs,
            TopologyDomain::Cartesian,
            op,
            Strictness::Lenient,
            false,
        )
        .unwrap();
        assert!(
            partitioned.equals(&single).unwrap(),
            "{op:?}: partitioned overlay differs from the single arrangement"
        );
    }
}
