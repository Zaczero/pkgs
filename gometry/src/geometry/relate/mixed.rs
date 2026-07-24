#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use super::*;
use crate::geometry::*;

pub(crate) fn mixed_relate_shapes(left: &Shape, right: &Shape) -> Option<De9im> {
    mixed_relate_with(left, None, right, None)
}

/// [`mixed_relate_shapes`] over cached operands: the area side's prepared
/// banded [`PointBatchTester`] turns each line sub-piece's membership probe
/// from an O(ring) ring scan into an O(band) lookup, the dominant cost on
/// large polygons.
pub(crate) fn mixed_relate_data(left: &ShapeData, right: &ShapeData) -> Option<De9im> {
    mixed_relate_with(
        left.shape(),
        left.point_tester(),
        right.shape(),
        right.point_tester(),
    )
}

pub(crate) fn mixed_relate_with(
    left: &Shape,
    left_tester: Option<&PointBatchTester>,
    right: &Shape,
    right_tester: Option<&PointBatchTester>,
) -> Option<De9im> {
    if let (Some(line), Some(polygons)) = (LinealOperand::from_shape(left), polygon_parts(right)) {
        mixed_relate(&line, polygons, right, right_tester)
    } else if let (Some(line), Some(polygons)) =
        (LinealOperand::from_shape(right), polygon_parts(left))
    {
        Some(mixed_relate(&line, polygons, left, left_tester)?.transpose())
    } else {
        None
    }
}

/// Contact inventory between a line and an area's rings: split
/// parameters and boundary-collinear intervals per LINE segment, cover
/// intervals per RING segment (the `EB` gap test), and the graded 0-D
/// contacts.
pub(crate) struct MixedContacts {
    pub(crate) splits: Vec<(usize, f64)>,
    pub(crate) on_boundary: Vec<(usize, f64, f64)>,
    pub(crate) ring_cover: Vec<(usize, f64, f64)>,
    pub(crate) ib0: bool,
    pub(crate) bb: bool,
    pub(crate) shared_run: bool,
}

pub(crate) fn group_values_by_index<T: Copy>(flat: &[(usize, T)], count: usize) -> Vec<Vec<T>> {
    let mut grouped = vec![Vec::new(); count];
    for &(index, value) in flat {
        grouped[index].push(value);
    }
    grouped
}

pub(crate) fn group_intervals_by_index(
    flat: &[(usize, f64, f64)],
    count: usize,
) -> Vec<Vec<(f64, f64)>> {
    let mut grouped = vec![Vec::new(); count];
    for &(index, start, end) in flat {
        grouped[index].push((start, end));
    }
    grouped
}

pub(crate) fn mixed_relate(
    line: &LinealOperand,
    polygons: &[Polygon],
    area: &Shape,
    area_tester: Option<&PointBatchTester>,
) -> Option<De9im> {
    // The area-side membership probes: the cached banded raycaster when the
    // operand came through the prepared `ShapeData` path, else the raw ring
    // scan. Both share `Shape::contains_point`/`covers_point` semantics.
    let contains = |point: Point| {
        area_tester.map_or_else(|| area.contains_point(point), |t| t.contains_point(point))
    };
    let covers = |point: Point| {
        area_tester.map_or_else(|| area.covers_point(point), |t| t.covers_point(point))
    };
    let mut rings: Vec<Segment> = Vec::new();
    for polygon in polygons {
        for ring in polygon.rings() {
            for [start, end] in ring.segment_pairs() {
                if !same_point(start, end) {
                    rings.push(Segment {
                        start: start.xy(),
                        end: end.xy(),
                    });
                }
            }
        }
    }
    if rings.is_empty() {
        return None; // fully degenerate rings — keep the geo lane
    }
    let MixedContacts {
        splits: splits_flat,
        on_boundary: on_boundary_flat,
        ring_cover: ring_cover_flat,
        ib0,
        bb,
        shared_run,
    } = mixed_contact_scan(line, &rings);
    let mut splits = group_values_by_index(&splits_flat, line.segments.len());
    let on_boundary = group_intervals_by_index(&on_boundary_flat, line.segments.len());
    let mut ring_cover = group_intervals_by_index(&ring_cover_flat, rings.len());
    // Classify the line sub-pieces between splits: each lies wholly
    // inside, outside, or along a recorded boundary run, so ONE strict
    // midpoint raycast decides it.
    let mut ii = false;
    let mut ie = false;
    for (index, &segment) in line.segments.iter().enumerate() {
        let ts = &mut splits[index];
        ts.push(0.0);
        ts.push(1.0);
        ts.sort_unstable_by(f64::total_cmp);
        for &[t0, t1] in ts.array_windows::<2>() {
            if t1 <= t0 {
                continue;
            }
            let mid = f64::midpoint(t0, t1);
            if on_boundary[index]
                .iter()
                .any(|&(b0, b1)| mid >= b0 && mid <= b1)
            {
                continue; // collinear run — already graded IB = 1
            }
            let probe = Point::new_unchecked_xy(
                segment.start.x + mid * (segment.end.x - segment.start.x),
                segment.start.y + mid * (segment.end.y - segment.start.y),
            );
            if contains(probe) {
                ii = true;
            } else {
                ie = true;
            }
        }
        if ii && ie {
            break;
        }
    }
    let mut matrix = [b'F'; 9];
    matrix[6] = b'2';
    matrix[8] = b'2';
    if ii {
        matrix[0] = b'1';
    }
    matrix[1] = if shared_run {
        b'1'
    } else if ib0 {
        b'0'
    } else {
        b'F'
    };
    if ie {
        matrix[2] = b'1';
    }
    if bb {
        matrix[4] = b'0';
    }
    for key in &line.boundary {
        let point = key.xy().point();
        if contains(point) {
            matrix[3] = b'0';
        } else if covers(point) {
            matrix[4] = b'0';
        } else {
            matrix[5] = b'0';
        }
    }
    if !fully_covered(&mut ring_cover) {
        matrix[7] = b'1';
    }
    Some(De9im(matrix))
}

/// One classified pass over the line × ring candidate pairs (see
/// [`MixedContacts`]): crossings and mid-segment ring corners split the
/// line piece; collinear runs mark boundary intervals on the line and
/// cover intervals on the rings; every endpoint-involved contact point
/// grades by the line's mod-2 boundary.
pub(crate) fn mixed_contact_scan(line: &LinealOperand, rings: &[Segment]) -> MixedContacts {
    let mut contacts = MixedContacts {
        splits: Vec::new(),
        on_boundary: Vec::new(),
        ring_cover: Vec::new(),
        ib0: false,
        bb: false,
        shared_run: false,
    };
    let clamp =
        |segment: Segment, point: XY| segment_projection_fraction(point, segment).clamp(0.0, 1.0);
    let mut visit = |l_index: usize, r_index: usize| {
        let (a, b) = (line.segments[l_index], rings[r_index]);
        match segment_contact(a, b) {
            SegmentContact::None => {},
            SegmentContact::Cross => {
                // A transversal crossing splits the line piece and IS an
                // interior-of-line point on the boundary.
                if let Some(point) = segment_cross_point(a, b) {
                    contacts.splits.push((l_index, clamp(a, point)));
                    contacts.ib0 = true;
                }
            },
            SegmentContact::Touch => {
                if let Some((_, run)) = shared_segment_part(a, b) {
                    contacts.shared_run = true;
                    let (t0, t1) = (clamp(a, run[0]), clamp(a, run[1]));
                    contacts.splits.push((l_index, t0.min(t1)));
                    contacts.splits.push((l_index, t0.max(t1)));
                    contacts.on_boundary.push((l_index, t0.min(t1), t0.max(t1)));
                    let (u0, u1) = (clamp(b, run[0]), clamp(b, run[1]));
                    contacts.ring_cover.push((r_index, u0.min(u1), u0.max(u1)));
                }
                // Ring endpoints resting ON the line segment split it (the
                // line may pass straight through a ring corner into the
                // interior); line endpoints on the ring are piece
                // boundaries already. Every such point grades by the
                // line's mod-2 boundary.
                for point in [b.start, b.end] {
                    if point_on_segment(point, a.start, a.end) {
                        contacts.splits.push((l_index, clamp(a, point)));
                        if line.interior(point) {
                            contacts.ib0 = true;
                        } else {
                            contacts.bb = true;
                        }
                    }
                }
                for point in [a.start, a.end] {
                    if point_on_segment(point, b.start, b.end) {
                        if line.interior(point) {
                            contacts.ib0 = true;
                        } else {
                            contacts.bb = true;
                        }
                    }
                }
            },
        }
    };
    if line.segments.len() * rings.len() < SEGMENT_INDEX_MIN_PAIRS {
        for (l_index, &probe) in line.segments.iter().enumerate() {
            for (r_index, &ring) in rings.iter().enumerate() {
                if !segment_envelopes_disjoint(probe, ring) {
                    visit(l_index, r_index);
                }
            }
        }
    } else {
        for_each_bipartite_index_pair(&line.segments, rings, &mut visit);
    }
    contacts
}

/// Whether the merged t-intervals cover every segment completely — the
/// exact `int ∩ ext = ∅` test for collinear-covered lines.
pub(crate) fn fully_covered(cover: &mut [Vec<(f64, f64)>]) -> bool {
    cover.iter_mut().all(|intervals| {
        if intervals.is_empty() {
            return false;
        }
        intervals.sort_unstable_by(|left, right| left.0.total_cmp(&right.0));
        let mut reach = 0.0;
        for &(start, end) in intervals.iter() {
            if start > reach {
                return false;
            }
            reach = reach.max(end);
        }
        reach >= 1.0
    })
}
