use super::*;

#[test]
fn clip_interval_corner_duplicates_preserve_source_endpoint_bits() {
    let bounds = Bounds::new(0.0, 0.0, 1.0, 1.0).expect("ordered bounds");
    let segment = Segment {
        start: XY::new(-0.0, -0.0),
        end: XY::new(2.0, 2.0),
    };
    let interval = clip_segment_rect_interval(segment, bounds)
        .unwrap()
        .expect("the segment crosses the rectangle");
    assert_eq!(interval.start.point.x.to_bits(), (-0.0_f64).to_bits());
    assert_eq!(interval.start.point.y.to_bits(), (-0.0_f64).to_bits());
    assert_eq!(interval.end.point, XY::new(1.0, 1.0));

    let reversed = Segment {
        start: segment.end,
        end: segment.start,
    };
    let interval = clip_segment_rect_interval(reversed, bounds)
        .unwrap()
        .expect("the reversed segment crosses the rectangle");
    assert_eq!(interval.start.point, XY::new(1.0, 1.0));
    assert_eq!(interval.end.point.x.to_bits(), (-0.0_f64).to_bits());
    assert_eq!(interval.end.point.y.to_bits(), (-0.0_f64).to_bits());
}
