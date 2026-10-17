use crate::geometry::{CoordSeq, Polygon, Ring, Segment, Shape, XY};

pub(in crate::geometry) fn argentina_uruguay_source_segments() -> [Segment; 6] {
    [
        [
            -58.404_001_6,
            -33.178_818,
            -58.122_989_509_606_21,
            -33.022_490_639_567_15,
        ],
        [
            -58.122_989_509_606_21,
            -33.022_490_639_567_15,
            -58.127_583_9,
            -33.022_275,
        ],
        [
            -58.114_883_013_439_915,
            -33.017_980_986_099_005,
            -58.113_024_156_144_505,
            -33.016_946_901_572_59,
        ],
        [
            -58.113_024_156_144_505,
            -33.016_946_901_572_59,
            -58.100_100_6,
            -32.951_622_7,
        ],
        [
            -58.111_802_159_656_57,
            -33.011_057_831_530_63,
            -58.112_953_902_396_03,
            -33.016_907_819_328_67,
        ],
        [
            -58.112_953_902_396_03,
            -33.016_907_819_328_67,
            -58.404_001_6,
            -33.178_818,
        ],
    ]
    .map(|[ax, ay, bx, by]| Segment {
        start: XY::new(ax, ay),
        end: XY::new(bx, by),
    })
}

pub(in crate::geometry) fn argentina_uruguay_source_arc_polygons() -> [Shape; 2] {
    let polygon = |points: &[XY]| {
        Shape::Polygon(Polygon::new(
            Ring::from_closed(CoordSeq::try_from_xy(points).unwrap()).unwrap(),
            Vec::new(),
        ))
    };
    let [ar_first, ar_second, ar_third, ar_last, uy_first, uy_last] =
        argentina_uruguay_source_segments();
    [
        polygon(&[
            ar_first.start,
            ar_first.end,
            ar_second.end,
            ar_third.start,
            ar_third.end,
            ar_last.end,
            XY::new(-58.5, -32.9),
            XY::new(-58.5, -33.3),
            ar_first.start,
        ]),
        polygon(&[
            uy_last.end,
            XY::new(-58.0, -33.3),
            XY::new(-58.0, -32.9),
            uy_first.start,
            uy_first.end,
            uy_last.end,
        ]),
    ]
}
