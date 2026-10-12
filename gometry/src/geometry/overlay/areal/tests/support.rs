use crate::error::Result;
use crate::geometry::overlay::{OverlayOp, Strictness, overlay_all};
use crate::geometry::{
    CoordSeq, CoordinateAxes, LineSeq, MOrdinate, MultiLineString, MultiMember, Point, Polygon,
    Ring, Shape, ZOrdinate,
};

pub(super) const EVERY_OP: [OverlayOp; 4] = [
    OverlayOp::Union,
    OverlayOp::Intersection,
    OverlayOp::Difference,
    OverlayOp::SymmetricDifference,
];

pub(super) fn polygon(points: &[(f64, f64)]) -> Polygon {
    Polygon::new(
        Ring::from_closed(
            CoordSeq::try_from_points(
                &points
                    .iter()
                    .map(|&(x, y)| Point::new(x, y).unwrap())
                    .collect::<Vec<_>>(),
            )
            .unwrap(),
        )
        .unwrap(),
        Vec::new(),
    )
}

pub(super) fn square(minx: f64, miny: f64, maxx: f64, maxy: f64) -> Shape {
    Shape::Polygon(polygon(&[
        (minx, miny),
        (maxx, miny),
        (maxx, maxy),
        (minx, maxy),
        (minx, miny),
    ]))
}

pub(super) fn regular_polygon(cx: f64, cy: f64, radius: f64, vertices: usize) -> Shape {
    let mut points: Vec<Point> = (0..vertices)
        .map(|index| {
            let angle = std::f64::consts::TAU * index as f64 / vertices as f64;
            Point::new_raw_xy(cx + radius * angle.cos(), cy + radius * angle.sin())
        })
        .collect();
    points.push(points[0]);
    Shape::Polygon(Polygon::new(
        Ring::from_closed(CoordSeq::try_from_points(&points).unwrap()).unwrap(),
        Vec::new(),
    ))
}

pub(super) fn zm_square(minx: f64, miny: f64, maxx: f64, maxy: f64, offset: f64) -> Shape {
    let points = [
        (minx, miny),
        (maxx, miny),
        (maxx, maxy),
        (minx, maxy),
        (minx, miny),
    ]
    .into_iter()
    .map(|(x, y)| {
        Point::new_axes(
            x,
            y,
            ZOrdinate(Some(offset + x + y)),
            MOrdinate(Some(offset - x + y)),
        )
    })
    .collect::<Result<Vec<_>>>()
    .unwrap();
    Shape::Polygon(Polygon::new(
        Ring::from_closed(CoordSeq::try_from_points(&points).unwrap()).unwrap(),
        Vec::new(),
    ))
}

pub(super) fn m_square(minx: f64, miny: f64, maxx: f64, maxy: f64, offset: f64) -> Shape {
    let points = [
        (minx, miny),
        (maxx, miny),
        (maxx, maxy),
        (minx, maxy),
        (minx, miny),
    ]
    .into_iter()
    .map(|(x, y)| Point::new_axes(x, y, ZOrdinate(None), MOrdinate(Some(offset + x + y))))
    .collect::<Result<Vec<_>>>()
    .unwrap();
    Shape::Polygon(Polygon::new(
        Ring::from_closed(CoordSeq::try_from_points(&points).unwrap()).unwrap(),
        Vec::new(),
    ))
}

pub(super) fn zm_line(start: (f64, f64), end: (f64, f64)) -> Shape {
    let points = [start, end]
        .into_iter()
        .map(|(x, y)| {
            Point::new_axes(
                x,
                y,
                ZOrdinate(Some(x + y + 10.0)),
                MOrdinate(Some(y - x + 10.0)),
            )
        })
        .collect::<Result<Vec<_>>>()
        .unwrap();
    Shape::LineString(
        LineSeq::try_new(CoordSeq::try_from_points(&points).unwrap()).expect("test line is valid"),
    )
}

pub(super) fn invalid_zm_bowtie() -> Shape {
    let points = [(0.0, 0.0), (2.0, 2.0), (0.0, 2.0), (2.0, 0.0), (0.0, 0.0)]
        .into_iter()
        .map(|(x, y)| {
            Point::new_axes(
                x,
                y,
                ZOrdinate(Some(x + y + 10.0)),
                MOrdinate(Some(y - x + 10.0)),
            )
        })
        .collect::<Result<Vec<_>>>()
        .unwrap();
    Shape::Polygon(Polygon::new(
        Ring::from_closed(CoordSeq::try_from_points(&points).unwrap()).unwrap(),
        Vec::new(),
    ))
}

pub(super) fn z_square(minx: f64, miny: f64, maxx: f64, maxy: f64, offset: f64) -> Shape {
    let points = [
        (minx, miny),
        (maxx, miny),
        (maxx, maxy),
        (minx, maxy),
        (minx, miny),
    ]
    .into_iter()
    .map(|(x, y)| Point::new_axes(x, y, ZOrdinate(Some(offset + x + y)), MOrdinate(None)))
    .collect::<Result<Vec<_>>>()
    .unwrap();
    Shape::Polygon(Polygon::new(
        Ring::from_closed(CoordSeq::try_from_points(&points).unwrap()).unwrap(),
        Vec::new(),
    ))
}

pub(super) fn line_seq(points: &[(f64, f64)]) -> LineSeq {
    LineSeq::try_new(
        CoordSeq::try_from_points(
            &points
                .iter()
                .map(|&(x, y)| Point::new(x, y).unwrap())
                .collect::<Vec<_>>(),
        )
        .unwrap(),
    )
    .expect("test line is valid")
}

pub(super) fn line(points: &[(f64, f64)]) -> Shape {
    Shape::LineString(line_seq(points))
}

pub(super) fn multiline(lines: &[&[(f64, f64)]]) -> Shape {
    Shape::MultiLineString(
        MultiLineString::try_new(
            CoordinateAxes::XY,
            lines
                .iter()
                .map(|points| MultiMember::Value(line_seq(points)))
                .collect(),
        )
        .unwrap(),
    )
}

pub(super) fn disjoint_multiline(count: usize, offset: f64) -> Shape {
    Shape::MultiLineString(
        MultiLineString::try_new(
            CoordinateAxes::XY,
            (0..count)
                .map(|index| {
                    let x = offset + index as f64 * 2.0;
                    MultiMember::Value(line_seq(&[(x, 0.0), (x, 1.0)]))
                })
                .collect(),
        )
        .unwrap(),
    )
}

pub(super) fn point(x: f64, y: f64) -> Shape {
    Shape::Point(Point::new(x, y).unwrap())
}

pub(super) fn manual_left_fold(inputs: &[&Shape], op: OverlayOp) -> Shape {
    let (first, rest) = inputs.split_first().expect("non-empty test inputs");
    let mut accumulated = (*first).clone();
    for input in rest {
        accumulated = accumulated.overlay(input, op, Strictness::Strict).unwrap();
    }
    accumulated
}

pub(super) fn assert_all_points_have_z(shape: &Shape) {
    shape.for_each_point(|point| {
        assert!(point.z().is_some(), "missing Z in {shape:?}");
    });
}

pub(super) fn assert_topologically_equal(left: &Shape, right: &Shape) {
    let delta = left
        .symmetric_difference(right, Strictness::Strict)
        .unwrap();
    assert!(
        delta.is_empty(),
        "left={left:?}\nright={right:?}\ndelta={delta:?}"
    );
}

pub(super) fn wkb(shape: &Shape) -> Vec<u8> {
    crate::io::to_wkb(shape, None, false).expect("overlay WKB")
}

pub(super) fn assert_disjoint_certificate_matches_graph(inputs: &[&Shape], op: OverlayOp) {
    let certified = overlay_all(
        inputs,
        crate::geometry::TopologyDomain::Cartesian,
        op,
        Strictness::Strict,
    )
    .unwrap();
    let graph = super::super::nary::overlay_all_without_bounds_certificates(
        inputs,
        crate::geometry::TopologyDomain::Cartesian,
        op,
        Strictness::Strict,
    )
    .unwrap();
    assert_eq!(
        wkb(&certified),
        wkb(&graph),
        "{}: {certified:?} != {graph:?}",
        op.name()
    );
}

pub(super) fn assert_disjoint_results_match(inputs: &[&Shape], op: OverlayOp) {
    let certified = overlay_all(
        inputs,
        crate::geometry::TopologyDomain::Cartesian,
        op,
        Strictness::Strict,
    );
    let forced = super::super::nary::overlay_all_without_bounds_certificates(
        inputs,
        crate::geometry::TopologyDomain::Cartesian,
        op,
        Strictness::Strict,
    );
    match (certified, forced) {
        (Ok(certified), Ok(forced)) => {
            assert_eq!(wkb(&certified), wkb(&forced), "{}", op.name());
        },
        (Err(certified), Err(forced)) => {
            assert_eq!(certified.to_string(), forced.to_string(), "{}", op.name());
        },
        (certified, forced) => panic!(
            "{} diverged: certified={certified:?}, forced={forced:?}",
            op.name()
        ),
    }
}
