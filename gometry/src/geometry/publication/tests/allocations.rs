use crate::geometry::publication::{
    GeometryOutput, PublicationRows, finish_geographic_nearest_point_columns,
    geographic_line_pending_row,
};
use crate::geometry::{Coordinates as _, LineSeq, Point, Shape};
use crate::test_support::{allocation_count, allocation_stats};
use crate::{CoordSeq, Frame};

#[test]
fn accepted_endpoint_publication_allocation_profile() {
    let (control, requests) = allocation_count(|| {
        let values = Vec::<u8>::with_capacity(16);
        std::hint::black_box(&values);
        let allocated = allocation_stats();
        drop(values);
        (allocated, allocation_stats())
    });
    assert_eq!(requests, 1);
    assert_eq!(control.0.live_bytes, 16);
    assert_eq!(control.1.live_bytes, 0);
    assert_eq!(control.1.peak_live_bytes, 16);
    let frame = Frame::Crs(crate::crs::frame::crs_arc_static("EPSG:4326"));
    for count in [1, 8, 64, 10_000, 100_000] {
        let accepted: Vec<_> = (0..count)
            .map(|index| {
                let step = index as f64 * 0.000001;
                [
                    Point::new_raw_xy(1.25 + step, 2.5),
                    Point::new_raw_xy(3.75 + step, 4.5),
                ]
            })
            .collect();
        let (phases, _) = allocation_count(|| {
            let mut rows = PublicationRows::with_producer_drawing_capacity(count);
            for (row, pair) in accepted.iter().enumerate() {
                let line = Shape::LineString(
                    LineSeq::try_new(CoordSeq::try_from_points(pair).unwrap()).unwrap(),
                );
                let pending = geographic_line_pending_row(line, row, "shortest_line").unwrap();
                rows.try_push(GeometryOutput::Pending(pending)).unwrap();
            }
            let append = allocation_stats();
            let output = rows.finish(frame.clone()).unwrap();
            let finish = allocation_stats();
            assert_eq!(output.len(), count);
            for (row, expected) in accepted.iter().enumerate() {
                let actual = output.storage().row(row).line_coords().unwrap();
                assert!(actual.iter_coords().eq(expected.iter().copied()));
            }
            drop(output);
            [append, finish, allocation_stats()]
        });
        for (phase, stats) in ["append", "finish", "drop"].into_iter().zip(phases) {
            eprintln!("PUBLICATION_PROFILE route=line rows={count} phase={phase} {stats:?}");
        }
        let (phases, _) = allocation_count(|| {
            let left = accepted.iter().map(|pair| Shape::Point(pair[0])).collect();
            let right = accepted.iter().map(|pair| Shape::Point(pair[1])).collect();
            let append = allocation_stats();
            let (left, right) =
                finish_geographic_nearest_point_columns(left, right, &frame, None).unwrap();
            let finish = allocation_stats();
            assert_eq!(left.len(), count);
            assert_eq!(right.len(), count);
            for (row, expected) in accepted.iter().enumerate() {
                assert_eq!(left.storage().row(row).point(), Some(expected[0]));
                assert_eq!(right.storage().row(row).point(), Some(expected[1]));
            }
            drop((left, right));
            [append, finish, allocation_stats()]
        });
        for (phase, stats) in ["append", "finish", "drop"].into_iter().zip(phases) {
            eprintln!("PUBLICATION_PROFILE route=points rows={count} phase={phase} {stats:?}");
        }
    }
}
