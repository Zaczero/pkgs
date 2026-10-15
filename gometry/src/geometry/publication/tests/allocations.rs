use crate::crs::frame::crs_arc_static;
use crate::geometry::publication::{
    GeometryOutput, PublicationRows, finish_geographic_line, geographic_line_pending_row,
};
use crate::geometry::{Coordinates as _, LineSeq, Point, Shape};
use crate::test_support::{allocation_count, allocation_stats};
use crate::{CoordSeq, Frame};

#[test]
fn scalar_drawing_moves_the_owned_coordinate_buffers() {
    let points: Vec<_> = (0..4096)
        .map(|index| Point::new_raw_xy(1.25 + f64::from(index) * 0.000_001, 2.5))
        .collect();
    let coordinates = CoordSeq::try_from_points(&points).unwrap();
    let xs = coordinates.xs().as_ptr();
    let ys = coordinates.ys().as_ptr();
    let shape = Shape::LineString(LineSeq::try_new(coordinates).unwrap());
    let frame = Frame::Crs(crs_arc_static("EPSG:4326"));
    let output = finish_geographic_line(shape, frame).unwrap();
    let Shape::LineString(line) = output else {
        panic!("the scalar line sink preserves its kind");
    };
    assert_eq!(line.as_coords().xs().as_ptr(), xs);
    assert_eq!(line.as_coords().ys().as_ptr(), ys);
    assert!(line.points().eq(points));
}

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
    assert_eq!(control.0.requests, 1);
    assert_eq!(control.0.requested_bytes, 16);
    assert_eq!(control.1.released_bytes, 16);
    assert_eq!(control.0.live_bytes, 16);
    assert_eq!(control.1.live_bytes, 0);
    assert_eq!(control.1.peak_live_bytes, 16);
    let frame = Frame::Crs(crs_arc_static("EPSG:4326"));
    for count in [1, 8, 64, 10_000, 100_000] {
        let accepted: Vec<_> = (0..count)
            .map(|index| {
                let step = index as f64 * 0.000_001;
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
            let pairs = accepted
                .iter()
                .map(|pair| Ok((Some((*pair).into()), pair[0].axes)));
            let (left, right) = crate::nearest_point_columns_masked(
                pairs,
                None,
                crate::geometry::TopologyDomain::Geographic(
                    crate::geographic::GeographicFrame::crs84(),
                ),
            )
            .unwrap();
            let append = allocation_stats();
            let (left, right) = (
                left.finish(frame.clone()).unwrap(),
                right.finish(frame.clone()).unwrap(),
            );
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
