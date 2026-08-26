use crate::geometry::{CoordSeq, Coordinates as _, Polygon, ring_winding, same_point};

pub(crate) fn line_has_nonzero_segment(coords: &CoordSeq) -> bool {
    coords
        .segment_pairs()
        .any(|[start, end]| !same_point(start, end))
}

pub(crate) fn multiline_has_nonzero_segment<L: AsRef<CoordSeq>>(lines: &[L]) -> bool {
    lines
        .iter()
        .any(|line| line_has_nonzero_segment(line.as_ref()))
}

pub(crate) fn polygon_has_nondegenerate_area(polygon: &Polygon) -> bool {
    polygon
        .rings()
        .any(|ring| ring.coord_count() >= 4 && !ring_winding(ring).is_degenerate())
}
