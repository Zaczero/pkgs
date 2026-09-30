use std::ops::ControlFlow;

use ahash::HashSetExt as _;

use crate::geometry::{
    Contact, CoordSeq, Coordinates as _, HashSet, IndexedSegment, LineworkChains, Orientation,
    Point, PointKey, Polygon, Ring, SegmentIndex, Shape, ValidationIssue, XY, orientation,
    point_on_segment, ring_winding, same_point, segment_contact_exact, segment_cross_point,
    segments_intersect,
};
/// Whether a vertex-SUBSET simplification of a polygon kept its linework
/// simple, checked on the DELTA only: removals replace vertex paths with
/// chords, so for VALID input the only possible new violations involve
/// the new chord edges. Each changed ring must keep a real ring (>= 4
/// closed coords, nonzero area), and every chord is tested against its
/// envelope candidates under the OGC simplicity rules — O(chords log n)
/// instead of re-validating the whole polygon.
pub(in crate::geometry) fn simplified_polygon_delta_is_simple(
    original: &Shape,
    simplified: &Shape,
) -> crate::error::Result<bool> {
    let same_ring_structure = match (original, simplified) {
        (Shape::Polygon(before), Shape::Polygon(after)) => {
            before.rings().count() == after.rings().count()
        },
        (Shape::MultiPolygon(before), Shape::MultiPolygon(after)) => {
            before.len() == after.len()
                && before
                    .iter()
                    .zip(after.iter())
                    .all(|(before, after)| before.rings().count() == after.rings().count())
        },
        _ => false,
    };
    if !same_ring_structure {
        return Ok(false);
    }
    let ring_pairs: Vec<(&CoordSeq, &CoordSeq)> = match (original, simplified) {
        (Shape::Polygon(before), Shape::Polygon(after)) => before
            .rings()
            .map(Ring::coords)
            .zip(after.rings().map(Ring::coords))
            .collect(),
        (Shape::MultiPolygon(before), Shape::MultiPolygon(after)) => before
            .iter()
            .flat_map(Polygon::rings)
            .map(Ring::coords)
            .zip(after.iter().flat_map(Polygon::rings).map(Ring::coords))
            .collect(),
        _ => return Ok(false),
    };
    let mut chains = LineworkChains::default();
    let mut chords: Vec<usize> = Vec::new();
    for (before, after) in ring_pairs {
        let ordinal_base = chains.segments.len();
        if chains.push_line(after)?.is_none() {
            return Ok(false); // a ring collapsed to nothing
        }
        let changed = after.coord_count() != before.coord_count();
        if !changed {
            continue;
        }
        // A changed ring must still be a ring with area.
        if after.coord_count() < Ring::MIN_VERTICES_CLOSED || ring_winding(after).is_degenerate() {
            return Ok(false);
        }
        // Two-pointer over the ordered vertex subset: an output edge
        // that skips original vertices is a NEW chord.
        let (bx, by) = (before.xs(), before.ys());
        let (ax, ay) = (after.xs(), after.ys());
        let mut source = 0_usize;
        for edge in 0..after.coord_count() - 1 {
            let (tx, ty) = (ax[edge + 1], ay[edge + 1]);
            let mut steps = 0_usize;
            loop {
                source += 1;
                steps += 1;
                if source >= bx.len() {
                    return Ok(false); // not a subset — re-check everything
                }
                // Bit equality is the intent: output vertices ARE input
                // vertices (the keep-mask contract).
                if bx[source].to_bits() == tx.to_bits() && by[source].to_bits() == ty.to_bits() {
                    break;
                }
            }
            if steps > 1 {
                chords.push(ordinal_base + edge);
            }
        }
    }
    if chords.is_empty() {
        return Ok(true);
    }
    let index = SegmentIndex::build(&chains.segments);
    Ok(chords.iter().all(|&ordinal| {
        let chord = chains.at(ordinal);
        index.intersecting_candidates(chord.segment).all(|entry| {
            entry.ordinal == ordinal || {
                let other = chains.at(entry.ordinal);
                !(segments_are_adjacent(chord, other)
                    || segments_intersect(chord.segment, other.segment))
                    || segment_intersection_is_simple(chord, other)
            }
        })
    }))
}

pub(crate) fn indexed_segments_are_simple(chains: &LineworkChains) -> crate::error::Result<bool> {
    Ok(chains
        .for_each_candidate_pair(|left, right| {
            let (left, right) = (chains.at(left), chains.at(right));
            // Chain-adjacent segments share an endpoint by construction —
            // always intersecting, no orientation tests needed.
            if (segments_are_adjacent(left, right)
                || segments_intersect(left.segment, right.segment))
                && !segment_intersection_is_simple(left, right)
            {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        })?
        .is_continue())
}

pub(crate) fn segment_intersection_is_simple(left: IndexedSegment, right: IndexedSegment) -> bool {
    if left.line != right.line {
        return cross_line_touch_is_simple(left, right);
    }
    if !segments_are_adjacent(left, right) {
        return false;
    }

    let (shared, left_other, right_other) = match [
        same_point(left.segment.start, right.segment.start),
        same_point(left.segment.start, right.segment.end),
        same_point(left.segment.end, right.segment.start),
        same_point(left.segment.end, right.segment.end),
    ] {
        [true, false, false, false] => (left.segment.start, left.segment.end, right.segment.end),
        [false, true, false, false] => (left.segment.start, left.segment.end, right.segment.start),
        [false, false, true, false] => (left.segment.end, left.segment.start, right.segment.end),
        [false, false, false, true] => (left.segment.end, left.segment.start, right.segment.start),
        _ => return false,
    };
    // Noncollinear adjacent rays meet only at the shared vertex. If collinear,
    // simplicity requires that vertex to lie between the two outer endpoints.
    if orientation(left_other, shared, right_other) != Orientation::Collinear {
        return true;
    }
    shared.x >= left_other.x.min(right_other.x)
        && shared.x <= left_other.x.max(right_other.x)
        && shared.y >= left_other.y.min(right_other.y)
        && shared.y <= left_other.y.max(right_other.y)
}

pub(crate) fn segments_are_adjacent(left: IndexedSegment, right: IndexedSegment) -> bool {
    left.line == right.line
        && (left.index.abs_diff(right.index) == 1
            || (left.closed
                && left.count > 1
                && left.index.min(right.index) == 0
                && left.index.max(right.index) == left.count - 1))
}

/// OGC simplicity for intersecting segments of DIFFERENT lines: the
/// contact must be a single point that is a BOUNDARY vertex of both
/// sides (an endpoint of an OPEN line — a closed line has an empty
/// boundary, so any contact with it offends). Positive-extent collinear
/// overlaps are never a finite point contact.
pub(crate) fn cross_line_touch_is_simple(left: IndexedSegment, right: IndexedSegment) -> bool {
    let contact = match segment_contact_exact(left.segment, right.segment) {
        // Interior to both segments, so an endpoint of neither line.
        Contact::Cross => return false,
        Contact::Touch { point } => point,
        Contact::Collinear { span } => return same_point(span.start, span.end),
        // Envelope-only candidates that never actually touch.
        Contact::Disjoint => return true,
    };
    is_line_boundary(left, contact) && is_line_boundary(right, contact)
}

/// Whether `point` is a boundary vertex of the segment's source line —
/// the first vertex of its first segment or the last vertex of its last
/// segment, and the line is OPEN (closed lines have no boundary).
pub(crate) fn is_line_boundary(segment: IndexedSegment, point: XY) -> bool {
    !segment.closed
        && ((segment.index == 0 && same_point(point, segment.segment.start))
            || (segment.index == segment.count - 1 && same_point(point, segment.segment.end)))
}

/// Duplicate point-atom coordinates (the `is_simple` rule for points):
/// each repeated XY is visited once, at its first repetition. One shared
/// seen-set spans collection members, so coincident points in different
/// parts report like every other cross-part contact.
pub(crate) fn collect_duplicate_points(shape: &Shape, visit: &mut impl FnMut(Point)) {
    fn walk(shape: &Shape, seen: &mut HashSet<PointKey>, visit: &mut impl FnMut(Point)) {
        match shape {
            Shape::Point(point) => {
                if !seen.insert(PointKey::new(*point)) {
                    visit(*point);
                }
            },
            Shape::MultiPoint(points) => {
                for point in points {
                    if !seen.insert(PointKey::new(point)) {
                        visit(point);
                    }
                }
            },
            Shape::GeometryCollection(geometries) => {
                for geometry in geometries {
                    walk(geometry, seen, visit);
                }
            },
            _ => {},
        }
    }
    // A lone point cannot coincide with itself; collections and multipoints
    // can.
    if matches!(shape, Shape::Point(_)) {
        return;
    }
    walk(shape, &mut HashSet::new(), visit);
}

/// Visit the self-intersection node(s) of one segment pair, when the pair
/// offends `is_simple`'s rules: the transversal crossing point, the touch
/// vertex, or every endpoint inside a collinear overlap.
pub(crate) fn collect_offending_pair(
    left: IndexedSegment,
    right: IndexedSegment,
    visit: &mut impl FnMut(Point),
) -> crate::error::Result<()> {
    if !segments_intersect(left.segment, right.segment)
        || segment_intersection_is_simple(left, right)
    {
        return Ok(());
    }
    let (a, b) = (left.segment, right.segment);
    let collinear = orientation(a.start, a.end, b.start) == Orientation::Collinear
        && orientation(a.start, a.end, b.end) == Orientation::Collinear;
    if collinear {
        for point in [b.start, b.end] {
            if point_on_segment(point, a.start, a.end) {
                visit(point.point());
            }
        }
        for point in [a.start, a.end] {
            if point_on_segment(point, b.start, b.end) {
                visit(point.point());
            }
        }
    } else if let Some(point) = segment_cross_point(a, b)? {
        visit(point.point());
    }
    Ok(())
}

impl ValidationIssue {
    #[expect(
        clippy::impl_trait_in_params,
        reason = "the input shapes are borrowed through their common trait contract, without a meaningful generic identity"
    )]
    pub fn new(
        code: &'static str,
        detail: impl Into<String>,
        location: Option<Point>,
        path: impl Into<String>,
    ) -> Self {
        Self {
            code,
            detail: detail.into(),
            location,
            path: Some(path.into()),
        }
    }

    pub fn with_path_prefix(mut self, prefix: &str) -> Self {
        self.path = Some(match self.path {
            Some(path) if path == "$" => prefix.to_owned(),
            Some(path) => path.strip_prefix('$').map_or_else(
                || format!("{prefix}.{path}"),
                |rest| format!("{prefix}{rest}"),
            ),
            None => prefix.to_owned(),
        });
        self
    }
}
