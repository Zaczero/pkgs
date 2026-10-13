use crate::geometry::Segment;

pub(super) fn reverse_segment(segment: Segment) -> Segment {
    Segment {
        start: segment.end,
        end: segment.start,
    }
}
