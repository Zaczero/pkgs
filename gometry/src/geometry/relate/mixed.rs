use crate::geometry::relate::{De9im, LinealOperand, Loc, polygon_parts};
use crate::geometry::{
    Coordinates as _, Point, PointBatchTester, PointProbeUse, Polygon, RingClass,
    SEGMENT_INDEX_MIN_PAIRS, Segment, SegmentContact, SegmentProjection, Shape, ShapeData, XY,
    for_each_bipartite_index_pair, point_on_segment, same_point, segment_contact,
    segment_cross_point, segment_envelopes_disjoint, segment_midpoint, segment_projection,
    shared_segment_part,
};

pub(crate) fn mixed_relate_shapes(left: &Shape, right: &Shape) -> Option<De9im> {
    mixed_relate_with(left, None, None, right, None, None)
}

/// [`mixed_relate_shapes`] over cached operands: the area side's prepared
/// hierarchical [`PointBatchTester`] turns each line sub-piece's membership
/// probe from an O(ring) ring scan into a Y-stabbing lookup, the dominant
/// cost on large polygons.
pub(crate) fn mixed_relate_data(
    left: &ShapeData,
    right: &ShapeData,
    left_mode: PointProbeUse,
    right_mode: PointProbeUse,
) -> Option<De9im> {
    mixed_relate_with(
        left.shape(),
        None,
        Some((left, left_mode)),
        right.shape(),
        None,
        Some((right, right_mode)),
    )
}

pub(crate) fn mixed_relate_with(
    left: &Shape,
    left_tester: Option<&PointBatchTester>,
    left_source: Option<(&ShapeData, PointProbeUse)>,
    right: &Shape,
    right_tester: Option<&PointBatchTester>,
    right_source: Option<(&ShapeData, PointProbeUse)>,
) -> Option<De9im> {
    if let (Some(line), Some(polygons)) = (LinealOperand::from_shape(left), polygon_parts(right)) {
        mixed_relate(&line, polygons, right, right_tester, right_source)
    } else if let (Some(line), Some(polygons)) =
        (LinealOperand::from_shape(right), polygon_parts(left))
    {
        Some(mixed_relate(&line, polygons, left, left_tester, left_source)?.transpose())
    } else {
        None
    }
}

/// Contact inventory between a line and an area's rings: split
/// parameters and boundary-collinear intervals per LINE segment, cover
/// intervals per RING segment (the `EB` gap test), and the graded 0-D
/// contacts.
pub(crate) struct MixedContacts {
    pub(crate) splits: Vec<(usize, SegmentProjection)>,
    pub(crate) on_boundary: Vec<(usize, SegmentProjection, SegmentProjection)>,
    pub(crate) ring_cover: Vec<(usize, SegmentProjection, SegmentProjection)>,
    pub(crate) ib0: bool,
    pub(crate) bb: bool,
    pub(crate) shared_run: bool,
}

pub(crate) fn group_values_by_index<T>(flat: Vec<(usize, T)>, count: usize) -> Vec<Vec<T>> {
    let mut grouped: Vec<Vec<T>> = std::iter::repeat_with(Vec::new).take(count).collect();
    for (index, value) in flat {
        grouped[index].push(value);
    }
    grouped
}

pub(crate) fn group_intervals_by_index<T>(
    flat: Vec<(usize, T, T)>,
    count: usize,
) -> Vec<Vec<(T, T)>> {
    let mut grouped: Vec<Vec<(T, T)>> = std::iter::repeat_with(Vec::new).take(count).collect();
    for (index, start, end) in flat {
        grouped[index].push((start, end));
    }
    grouped
}

pub(crate) fn projection_interval(
    segment: Segment,
    start: XY,
    end: XY,
) -> (SegmentProjection, SegmentProjection) {
    let start = segment_projection(start, segment);
    let end = segment_projection(end, segment);
    if start.cmp_along(&end).is_le() {
        (start, end)
    } else {
        (end, start)
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "the scan, probe construction, and matrix assembly form one cohesive relation operation"
)]
#[expect(
    clippy::option_if_let_else,
    reason = "the explicit tester fallback keeps the two classification strategies readable"
)]
pub(crate) fn mixed_relate(
    line: &LinealOperand,
    polygons: &[Polygon],
    area: &Shape,
    area_tester: Option<&PointBatchTester>,
    area_source: Option<(&ShapeData, PointProbeUse)>,
) -> Option<De9im> {
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
    let mut splits = group_values_by_index(splits_flat, line.segments.len());
    let on_boundary = group_intervals_by_index(on_boundary_flat, line.segments.len());
    let mut ring_cover = group_intervals_by_index(ring_cover_flat, rings.len());
    // This is the operation's membership plan. The same points counted for
    // tester selection are consumed below; do not recompute midpoints later.
    let mut subpiece_probes = Vec::new();
    for (index, &segment) in line.segments.iter().enumerate() {
        let ts = &mut splits[index];
        ts.push(SegmentProjection::Start);
        ts.push(SegmentProjection::End);
        ts.sort_unstable_by(SegmentProjection::cmp_along);
        ts.dedup_by(|left, right| left.cmp_along(right).is_eq());
        for [t0, t1] in ts.array_windows::<2>() {
            if t1.cmp_along(t0).is_le() {
                continue;
            }
            if on_boundary[index]
                .iter()
                .any(|(b0, b1)| b0.cmp_along(t0).is_le() && b1.cmp_along(t1).is_ge())
            {
                continue;
            }
            subpiece_probes.push(segment_midpoint(Segment {
                start: t0.interpolate_xy(segment),
                end: t1.interpolate_xy(segment),
            }));
        }
    }
    let boundary_probes: Vec<Point> = line.boundary.iter().map(|key| key.xy().point()).collect();
    let probe_count = subpiece_probes.len().saturating_add(boundary_probes.len());
    let area_tester = area_source
        .and_then(|(shape, mode)| shape.point_tester_for(mode.for_plan(probe_count)))
        .or(area_tester);
    let locate = |point: Point| {
        if let Some(tester) = area_tester {
            match tester
                .classify_area_point(point)
                .expect("mixed relate area source is polygonal")
            {
                RingClass::Interior => Loc::Interior,
                RingClass::Boundary => Loc::Boundary,
                RingClass::Exterior => Loc::Exterior,
            }
        } else if area.contains_point(point) {
            Loc::Interior
        } else if area.covers_point(point) {
            Loc::Boundary
        } else {
            Loc::Exterior
        }
    };
    let mut ii = false;
    let mut ie = false;
    for probe in subpiece_probes {
        if matches!(locate(probe), Loc::Interior) {
            ii = true;
        } else {
            ie = true;
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
    for point in boundary_probes {
        match locate(point) {
            Loc::Interior => matrix[3] = b'0',
            Loc::Boundary => matrix[4] = b'0',
            Loc::Exterior => matrix[5] = b'0',
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
    let mut visit = |l_index: usize, r_index: usize| {
        let (a, b) = (line.segments[l_index], rings[r_index]);
        match segment_contact(a, b) {
            SegmentContact::None => {},
            SegmentContact::Cross => {
                // A transversal crossing splits the line piece and IS an
                // interior-of-line point on the boundary.
                if let Some(point) = segment_cross_point(a, b) {
                    contacts
                        .splits
                        .push((l_index, segment_projection(point, a)));
                    contacts.ib0 = true;
                }
            },
            SegmentContact::Touch => {
                if let Some((_, run)) = shared_segment_part(a, b) {
                    contacts.shared_run = true;
                    let (t0, t1) = projection_interval(a, run[0], run[1]);
                    contacts.splits.push((l_index, t0.clone()));
                    contacts.splits.push((l_index, t1.clone()));
                    contacts.on_boundary.push((l_index, t0, t1));
                    let (u0, u1) = projection_interval(b, run[0], run[1]);
                    contacts.ring_cover.push((r_index, u0, u1));
                }
                // Ring endpoints resting ON the line segment split it (the
                // line may pass straight through a ring corner into the
                // interior); line endpoints on the ring are piece
                // boundaries already. Every such point grades by the
                // line's mod-2 boundary.
                for point in [b.start, b.end] {
                    if point_on_segment(point, a.start, a.end) {
                        contacts
                            .splits
                            .push((l_index, segment_projection(point, a)));
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
pub(crate) fn fully_covered(cover: &mut [Vec<(SegmentProjection, SegmentProjection)>]) -> bool {
    cover.iter_mut().all(|intervals| {
        if intervals.is_empty() {
            return false;
        }
        intervals.sort_unstable_by(|left, right| left.0.cmp_along(&right.0));
        let start_projection = SegmentProjection::Start;
        let end_projection = SegmentProjection::End;
        let mut reach = &start_projection;
        for (start, end) in intervals.iter() {
            if start.cmp_along(reach).is_gt() {
                return false;
            }
            if end.cmp_along(reach).is_gt() {
                reach = end;
            }
        }
        reach.cmp_along(&end_projection).is_ge()
    })
}
