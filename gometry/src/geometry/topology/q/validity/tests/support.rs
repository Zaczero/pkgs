use super::super::ExactGeographicTopology;
pub(super) use crate::geometry::test_support::{frame, line, ring};
use crate::geometry::{
    CoordSeq, CoordinateAxes, LineSeq, MultiLineString, MultiMember, MultiPolygon, Point, Polygon,
    Shape, ValidationIssue,
};

pub(super) fn multi_line(lines: &[&[(f64, f64)]]) -> Shape {
    Shape::MultiLineString(
        MultiLineString::try_new(
            CoordinateAxes::XY,
            lines
                .iter()
                .map(|points| {
                    let points = points
                        .iter()
                        .map(|&(x, y)| Point::new_raw_xy(x, y))
                        .collect::<Vec<_>>();
                    MultiMember::Value(
                        LineSeq::try_new(CoordSeq::try_from_points(&points).unwrap()).unwrap(),
                    )
                })
                .collect(),
        )
        .unwrap(),
    )
}

pub(super) fn polygon(rings: &[Vec<(f64, f64)>]) -> Polygon {
    let (shell, holes) = rings
        .split_first()
        .expect("test polygons include an exterior ring");
    Polygon::new(
        ring(shell),
        holes.iter().map(|points| ring(points)).collect(),
    )
}

pub(super) fn multi_polygon(polygons: &[Vec<Vec<(f64, f64)>>]) -> Shape {
    Shape::MultiPolygon(
        MultiPolygon::try_new(
            CoordinateAxes::XY,
            polygons
                .iter()
                .map(|rings| MultiMember::Value(polygon(rings)))
                .collect(),
        )
        .unwrap(),
    )
}

pub(super) fn shape(rings: &[Vec<(f64, f64)>], multi: bool) -> Shape {
    if multi {
        multi_polygon(
            &rings
                .iter()
                .cloned()
                .map(|ring| vec![ring])
                .collect::<Vec<_>>(),
        )
    } else {
        Shape::Polygon(polygon(rings))
    }
}

pub(super) fn pole_hop(longitude: f64, latitude: f64, pole: f64) -> Vec<(f64, f64)> {
    (0..=4)
        .map(|index| (longitude + 90.0 * f64::from(index), latitude))
        .chain([
            (longitude + 360.0, pole),
            (longitude, pole),
            (longitude, latitude),
        ])
        .collect()
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct Signature {
    pub(super) code: Option<&'static str>,
    pub(super) detail: Option<String>,
    pub(super) path: Option<String>,
    pub(super) location: Option<(u64, u64)>,
}

pub(super) fn signature(shape: &Shape) -> Signature {
    let frame = frame();
    let view = crate::geographic::ScalarGeographicView::try_new(frame, shape).unwrap();
    let topology = ExactGeographicTopology::from_views(frame, std::iter::once(view)).unwrap();
    signature_from_issue(topology.validity_issue(shape).unwrap())
}

pub(super) fn signature_from_issue(issue: Option<ValidationIssue>) -> Signature {
    match issue {
        Some(issue) => Signature {
            code: Some(issue.code),
            detail: Some(issue.detail),
            path: issue.path,
            location: issue
                .location
                .map(|point| (point.x.to_bits(), point.y.to_bits())),
        },
        None => Signature {
            code: None,
            detail: None,
            path: None,
            location: None,
        },
    }
}

pub(super) fn box_ring(west: f64, east: f64, south: f64, north: f64) -> Vec<(f64, f64)> {
    vec![
        (west, south),
        (east, south),
        (east, north),
        (west, north),
        (west, south),
    ]
}

pub(super) fn shift_rings(
    polygons: &[Vec<Vec<(f64, f64)>>],
    shift: f64,
) -> Vec<Vec<Vec<(f64, f64)>>> {
    polygons
        .iter()
        .map(|polygon| {
            polygon
                .iter()
                .map(|ring| ring.iter().map(|&(x, y)| (x + shift, y)).collect())
                .collect()
        })
        .collect()
}
