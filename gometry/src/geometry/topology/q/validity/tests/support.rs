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

#[derive(Clone, Copy)]
pub(super) enum Expectation {
    Valid,
    Invalid(&'static str),
}

pub(super) struct RingRow {
    pub(super) name: String,
    pub(super) rings: Vec<Vec<(f64, f64)>>,
    pub(super) multi: bool,
    pub(super) expectation: Expectation,
    pub(super) periodic: bool,
}

pub(super) fn rows() -> Vec<RingRow> {
    let mut rows = Vec::new();
    let mut add = |name: String,
                   rings: Vec<Vec<(f64, f64)>>,
                   multi: bool,
                   expectation: Expectation,
                   periodic: bool| {
        rows.push(RingRow {
            name,
            rings,
            multi,
            expectation,
            periodic,
        });
    };
    for &(latitude, pole) in &[(80.0, 90.0), (-80.0, -90.0)] {
        for &longitude in &[0.0, 40.0, -90.0, 100.0, -179.0, 180.0, -180.0, 270.0] {
            let parallel = (0..=4)
                .map(|index| (longitude + 90.0 * f64::from(index), latitude))
                .collect::<Vec<_>>();
            let mut hop = parallel.clone();
            hop.extend([
                (longitude + 360.0, pole),
                (longitude, pole),
                (longitude, latitude),
            ]);
            add(
                format!("hop@{longitude} lat{latitude}"),
                vec![hop],
                false,
                Expectation::Valid,
                true,
            );

            let mut pole_line = parallel.clone();
            pole_line.push((longitude + 360.0, pole));
            for index in 1..=4 {
                pole_line.push((longitude + 360.0 - 90.0 * f64::from(index), pole));
            }
            pole_line.push((longitude, latitude));
            add(
                format!("pole-line@{longitude} lat{latitude}"),
                vec![pole_line],
                false,
                Expectation::Valid,
                true,
            );

            let mut apex = parallel;
            apex.extend([
                (longitude + 360.0, pole),
                (longitude + 360.0, latitude),
                (longitude, latitude),
            ]);
            add(
                format!("apex@{longitude} lat{latitude}"),
                vec![apex],
                false,
                Expectation::Invalid("self_intersection"),
                true,
            );
        }
        let seam_parallel = (0..=4)
            .map(|index| (-180.0 + 90.0 * f64::from(index), latitude))
            .collect::<Vec<_>>();
        let mut pair_a = seam_parallel.clone();
        pair_a.extend([(180.0, pole), (-180.0, pole), (-180.0, latitude)]);
        add(
            format!("U1 pair A lat{latitude}"),
            vec![pair_a],
            false,
            Expectation::Valid,
            true,
        );
        let mut pair_c = seam_parallel;
        pair_c.extend([
            (180.0, pole),
            (-180.0, pole),
            (180.0, pole),
            (-180.0, pole),
            (-180.0, latitude),
        ]);
        add(
            format!("U1 pair repeated C lat{latitude}"),
            vec![pair_c],
            false,
            Expectation::Invalid("self_intersection"),
            true,
        );
    }

    for &longitude in &[-180.0, 0.0, 40.0, -90.0, 180.0] {
        let band = (0..=3)
            .map(|index| (longitude + 120.0 * f64::from(index), -30.0))
            .chain(
                (0..=3)
                    .rev()
                    .map(|index| (longitude + 120.0 * f64::from(index), 30.0)),
            )
            .chain([(longitude, -30.0)])
            .collect::<Vec<_>>();
        add(
            format!("band 120deg steps @{longitude}"),
            vec![band],
            false,
            Expectation::Valid,
            true,
        );
        let explicit = vec![
            (longitude, -30.0),
            (longitude + 360.0, -30.0),
            (longitude + 360.0, 30.0),
            (longitude, 30.0),
            (longitude, -30.0),
        ];
        let expectation = if longitude.abs().to_bits() == 180.0_f64.to_bits() {
            Expectation::Invalid("self_intersection")
        } else {
            Expectation::Valid
        };
        add(
            format!("band explicit full-turn edges @{longitude}"),
            vec![explicit],
            false,
            expectation,
            true,
        );
    }

    add(
        "keyhole slit".into(),
        vec![vec![
            (0.0, 0.0),
            (40.0, 0.0),
            (40.0, 40.0),
            (20.0, 40.0),
            (20.0, 20.0),
            (20.0, 40.0),
            (0.0, 40.0),
            (0.0, 0.0),
        ]],
        false,
        Expectation::Invalid("self_intersection"),
        false,
    );
    add(
        "outward spur".into(),
        vec![vec![
            (0.0, 0.0),
            (40.0, 0.0),
            (40.0, 40.0),
            (20.0, 40.0),
            (20.0, 60.0),
            (20.0, 40.0),
            (0.0, 40.0),
            (0.0, 0.0),
        ]],
        false,
        Expectation::Invalid("self_intersection"),
        false,
    );
    add(
        "stutter equal vertices".into(),
        vec![vec![
            (0.0, 0.0),
            (0.0, 0.0),
            (40.0, 0.0),
            (40.0, 40.0),
            (0.0, 40.0),
            (0.0, 0.0),
        ]],
        false,
        Expectation::Valid,
        false,
    );
    add(
        "stutter seam pair at lat 0".into(),
        vec![vec![
            (170.0, 0.0),
            (180.0, 0.0),
            (-180.0, 0.0),
            (-170.0, 0.0),
            (-170.0, 10.0),
            (170.0, 10.0),
            (170.0, 0.0),
        ]],
        false,
        Expectation::Valid,
        false,
    );
    add(
        "triangle to pole with seam pair stutter".into(),
        vec![vec![
            (0.0, 80.0),
            (40.0, 80.0),
            (180.0, 90.0),
            (-180.0, 90.0),
            (0.0, 80.0),
        ]],
        false,
        Expectation::Valid,
        true,
    );
    add(
        "triangle touching pole once".into(),
        vec![vec![(0.0, 80.0), (40.0, 80.0), (20.0, 90.0), (0.0, 80.0)]],
        false,
        Expectation::Valid,
        true,
    );
    add(
        "ring touching pole twice".into(),
        vec![vec![
            (0.0, 70.0),
            (30.0, 90.0),
            (60.0, 70.0),
            (90.0, 70.0),
            (120.0, 90.0),
            (150.0, 70.0),
            (0.0, 70.0),
        ]],
        false,
        Expectation::Invalid("self_intersection"),
        true,
    );
    add(
        "ring along the pole line only".into(),
        vec![vec![(0.0, 90.0), (120.0, 90.0), (240.0, 90.0), (0.0, 90.0)]],
        false,
        Expectation::Invalid("ring_collapsed"),
        true,
    );
    add(
        "pinched full-longitude ring".into(),
        vec![vec![
            (0.0, 10.0),
            (120.0, 30.0),
            (240.0, 20.0),
            (360.0, 10.0),
            (240.0, 0.0),
            (120.0, 5.0),
            (0.0, 10.0),
        ]],
        false,
        Expectation::Valid,
        true,
    );
    add(
        "zigzag-sided full-longitude band".into(),
        vec![vec![
            (0.0, 0.0),
            (120.0, 0.0),
            (240.0, 0.0),
            (360.0, 0.0),
            (370.0, 10.0),
            (360.0, 20.0),
            (240.0, 20.0),
            (120.0, 20.0),
            (0.0, 20.0),
            (10.0, 10.0),
            (0.0, 0.0),
        ]],
        false,
        Expectation::Valid,
        true,
    );
    add(
        "ring wider than a period".into(),
        vec![vec![
            (0.0, 0.0),
            (150.0, 0.0),
            (300.0, 0.0),
            (450.0, 0.0),
            (450.0, 20.0),
            (300.0, 20.0),
            (150.0, 20.0),
            (0.0, 20.0),
            (0.0, 0.0),
        ]],
        false,
        Expectation::Invalid("self_intersection"),
        true,
    );
    add(
        "bowtie".into(),
        vec![vec![
            (0.0, 0.0),
            (10.0, 10.0),
            (10.0, 0.0),
            (0.0, 10.0),
            (0.0, 0.0),
        ]],
        false,
        Expectation::Invalid("self_intersection"),
        false,
    );
    add(
        "self T-touch".into(),
        vec![vec![
            (0.0, 0.0),
            (20.0, 0.0),
            (20.0, 10.0),
            (10.0, 0.0),
            (10.0, 10.0),
            (0.0, 10.0),
            (0.0, 0.0),
        ]],
        false,
        Expectation::Invalid("self_intersection"),
        false,
    );
    for (name, ring) in [
        ("bowtie across the seam (explicit lift)", vec![
            (170.0, -10.0),
            (190.0, 10.0),
            (170.0, 10.0),
            (190.0, -10.0),
            (170.0, -10.0),
        ]),
        ("bowtie across the seam (canonical spelling)", vec![
            (170.0, -10.0),
            (-170.0, 10.0),
            (170.0, 10.0),
            (-170.0, -10.0),
            (170.0, -10.0),
        ]),
    ] {
        add(
            name.into(),
            vec![ring],
            false,
            Expectation::Invalid("self_intersection"),
            true,
        );
    }
    add(
        "ordinary box".into(),
        vec![vec![
            (0.0, 0.0),
            (10.0, 0.0),
            (10.0, 10.0),
            (0.0, 10.0),
            (0.0, 0.0),
        ]],
        false,
        Expectation::Valid,
        false,
    );
    add(
        "seam-crossing box (explicit lift)".into(),
        vec![vec![
            (170.0, 0.0),
            (190.0, 0.0),
            (190.0, 10.0),
            (170.0, 10.0),
            (170.0, 0.0),
        ]],
        false,
        Expectation::Valid,
        true,
    );

    let box_ring = |west: f64, east: f64| {
        vec![
            (west, 0.0),
            (east, 0.0),
            (east, 10.0),
            (west, 10.0),
            (west, 0.0),
        ]
    };
    add(
        "E/W seam members".into(),
        vec![box_ring(170.0, 180.0), box_ring(-180.0, -170.0)],
        true,
        Expectation::Valid,
        true,
    );
    add(
        "E/E seam members".into(),
        vec![box_ring(170.0, 180.0), box_ring(180.0, 190.0)],
        true,
        Expectation::Valid,
        true,
    );
    add(
        "adjacent ordinary members".into(),
        vec![box_ring(0.0, 10.0), box_ring(10.0, 20.0)],
        true,
        Expectation::Invalid("shared_linework"),
        false,
    );
    add(
        "duplicate member +360".into(),
        vec![box_ring(0.0, 10.0), box_ring(360.0, 370.0)],
        true,
        Expectation::Invalid("shared_linework"),
        true,
    );
    add(
        "hole sharing shell edge".into(),
        vec![box_ring(0.0, 20.0), vec![
            (0.0, 2.0),
            (5.0, 2.0),
            (5.0, 5.0),
            (0.0, 5.0),
            (0.0, 2.0),
        ]],
        false,
        Expectation::Invalid("shared_linework"),
        false,
    );
    rows
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
