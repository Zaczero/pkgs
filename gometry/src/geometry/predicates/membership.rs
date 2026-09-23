use crate::geometry::{
    Bounds, Coordinates, HashMap, HashMapExt as _, IndexedSegment, LineworkChains, Orientation,
    Point, PointKey, Polygon, Ring, ValidationIssue, XY, classify_ring_pair, face_interior_point,
    for_each_overlapping_bounds_pair, orient_ring, orientation_xy, ray_crossing_is_right,
    ring_probe_point, settle_touches, topology, visit_interacting_pairs,
};
/// Where a probe point sits relative to one ring.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RingClass {
    Exterior,
    Boundary,
    Interior,
}

/// One edge's contribution to even-odd ring membership.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum RingEdgeMembership {
    None,
    Boundary,
    Crossing,
}

/// Classify one ring edge against a point. Boundary requires both closed-box
/// containment and robust collinearity; otherwise a half-open ray crossing
/// contributes one parity toggle.
pub(crate) fn ring_edge_membership(
    ax: f64,
    ay: f64,
    bx: f64,
    by: f64,
    px: f64,
    py: f64,
) -> RingEdgeMembership {
    let in_bbox = px >= ax.min(bx) && px <= ax.max(bx) && py >= ay.min(by) && py <= ay.max(by);
    let orientation = in_bbox.then(|| orientation_xy(ax, ay, bx, by, px, py));
    if orientation == Some(Orientation::Collinear) {
        return RingEdgeMembership::Boundary;
    }
    if (ay > py) != (by > py)
        && orientation.map_or_else(
            || ray_crossing_is_right(ax, ay, bx, by, px, py),
            |orientation| (orientation == Orientation::CounterClockwise) == (by > ay),
        )
    {
        RingEdgeMembership::Crossing
    } else {
        RingEdgeMembership::None
    }
}

/// Classify `point` against `ring` in ONE pass: the boundary test (envelope
/// gate first, exact robust orientation only for the few segments whose box
/// contains the probe) and the crossing parity accumulate together, instead of
/// the previous two full scans with an exact orientation per segment. The
/// verdicts are identical: boundary contact short-circuits (a boundary point
/// is boundary regardless of parity), and a non-boundary probe accumulates the
/// exact same parity terms in the same order.
pub(crate) fn ring_classify_point<C: Coordinates + ?Sized>(ring: &C, point: Point) -> RingClass {
    if let Some((xs, ys)) = ring.xy_columns() {
        return ring_classify_point_columns(xs, ys, point);
    }
    let mut inside = false;
    for [a, b] in ring.segment_pairs() {
        match ring_edge_membership(a.x, a.y, b.x, b.y, point.x, point.y) {
            RingEdgeMembership::Boundary => return RingClass::Boundary,
            RingEdgeMembership::Crossing => inside = !inside,
            RingEdgeMembership::None => {},
        }
    }
    if inside {
        RingClass::Interior
    } else {
        RingClass::Exterior
    }
}

/// Column form of [`ring_classify_point`] over the `SoA` columns. The SIMD
/// kernel returns the crossing parity plus a boundary-candidate flag (point in
/// some edge bbox). The far-from-boundary common case decides interior/exterior
/// straight from the parity (fully SIMD, bit-identical to the scalar toggle);
/// only when the point sits inside an edge's bbox — the precondition for an
/// exact boundary hit — do we pay the scalar `orientation_xy` resolution.
pub(crate) fn ring_classify_point_columns(xs: &[f64], ys: &[f64], point: Point) -> RingClass {
    let (crossings, near_edge) =
        topology::ring_classify_crossings_columns(xs, ys, XY::new(point.x, point.y));
    if near_edge {
        return ring_classify_point_columns_exact(xs, ys, point);
    }
    if crossings % 2 == 1 {
        RingClass::Interior
    } else {
        RingClass::Exterior
    }
}

/// Exact resolution when the point lies within some edge's bbox: the original
/// scalar scan with the robust `orientation_xy` collinearity test (a boundary
/// requires both bbox containment AND exact collinearity).
pub(crate) fn ring_classify_point_columns_exact(xs: &[f64], ys: &[f64], point: Point) -> RingClass {
    let mut inside = false;
    for ([ax, bx], [ay, by]) in std::iter::zip(xs.array_windows::<2>(), ys.array_windows::<2>()) {
        let (ax, ay, bx, by) = (*ax, *ay, *bx, *by);
        match ring_edge_membership(ax, ay, bx, by, point.x, point.y) {
            RingEdgeMembership::Boundary => return RingClass::Boundary,
            RingEdgeMembership::Crossing => inside = !inside,
            RingEdgeMembership::None => {},
        }
    }
    if inside {
        RingClass::Interior
    } else {
        RingClass::Exterior
    }
}

pub(crate) fn ring_contains_interior<C: Coordinates + ?Sized>(
    ring: &C,
    point: impl Into<XY>,
) -> bool {
    let point = point.into();
    // Column lane: native `SoA` columns feed the SIMD ray-crossing kernel
    // directly (no per-segment `Point` gather), bit-identical to the scalar
    // parity. The AoS fallback below keeps the scalar toggle.
    if let Some((xs, ys)) = ring.xy_columns() {
        return topology::ring_contains_interior_columns(xs, ys, point);
    }
    let mut inside = false;
    for [a, b] in ring.segment_pairs() {
        if (a.y > point.y) != (b.y > point.y)
            && ray_crossing_is_right(a.x, a.y, b.x, b.y, point.x, point.y)
        {
            inside = !inside;
        }
    }
    inside
}

// ---- Indexed polygonal validity -------------------------------------------
//
// One indexed pass over
// every ring segment classifies all interacting pairs: same-ring pairs use
// the simplicity rule, cross-ring pairs are crossings (invalid), collinear
// overlaps (invalid), or point touches. Touch nodes then settle the two
// hard verdicts pairwise scans miss: WEDGE INTERLEAVING (a ring passing
// through a shared vertex is a crossing even though no segment interiors
// meet) and INTERIOR CONNECTIVITY (a cycle in the rings-and-touch-points
// graph pinches off part of the interior). Witness locations ride on every
// issue.

/// One ring's local directions away from a touch point, per participating
/// ring — accumulated across the pair scan, settled in the post-pass.
pub(crate) type TouchDirections = HashMap<(usize, usize), HashMap<PointKey, [Vec<XY>; 2]>>;

pub(crate) fn ring_label(ring: usize) -> String {
    if ring == 0 {
        "exterior ring".into()
    } else {
        format!("interior ring at index {}", ring - 1)
    }
}

/// Validity of one polygon's ring arrangement (rings already individually
/// closed/finite/long-enough via `validate_ring`).
pub(in crate::geometry) fn polygon_rings_issue(
    polygon: &Polygon,
    path: &str,
) -> crate::error::Result<Option<ValidationIssue>> {
    let ring_count = polygon.holes.len() + 1;
    let mut chains = LineworkChains::default();
    if chains.push_line(polygon.shell.coords())?.is_none() {
        return Ok(None);
    }
    for hole in polygon.holes.iter() {
        if chains.push_line(hole.coords())?.is_none() {
            return Ok(None);
        }
    }

    let mut touches: TouchDirections = HashMap::new();
    if let Some(issue) = visit_interacting_pairs(&chains, |left, right| {
        classify_ring_pair(left, right, &mut touches, &|a, b| {
            format!(
                "{} and {} intersect",
                ring_label(a.min(b)),
                ring_label(a.max(b))
            )
        })
    })? {
        return Ok(Some(issue.with_path_prefix(path)));
    }
    if !touches.is_empty()
        && let Some(issue) = settle_touches(&touches, ring_count, true, &|a, b| {
            format!(
                "{} and {} intersect",
                ring_label(a.min(b)),
                ring_label(a.max(b))
            )
        })
    {
        return Ok(Some(issue.with_path_prefix(path)));
    }
    if polygon.holes.is_empty() {
        return Ok(None); // nothing to contain, nothing to nest
    }

    // Containment: every hole interior must sit inside the shell and
    // outside every other hole. With crossings excluded above, one probe
    // point per hole decides each relation. Shell containment is O(H);
    // hole–hole nesting visits only OVERLAPPING bounds pairs via the shared
    // sweep (never all-pairs compares of provably disjoint holes).
    let mut hole_bounds: Vec<Bounds> = Vec::with_capacity(polygon.holes.len());
    let mut probes: Vec<Point> = Vec::with_capacity(polygon.holes.len());
    for (index, hole) in polygon.holes.iter().enumerate() {
        let probe = ring_probe_point(hole.coords(), polygon.shell.coords());
        if ring_classify_point(polygon.shell.coords(), probe) != RingClass::Interior {
            return Ok(Some(ValidationIssue::new(
                "hole_outside_shell",
                format!(
                    "interior ring at index {index} is not contained within the polygon's exterior"
                ),
                Some(probe),
                path,
            )));
        }
        // Empty/degenerate hole bounds cannot nest — use an empty box so the
        // sweep never pairs them. Degenerate rings already failed earlier
        // gates in practice; this is a defensive alignment skip.
        hole_bounds.push(Bounds::from_coords(hole.coords()).unwrap_or_else(|| {
            Bounds::new_raw(
                f64::INFINITY,
                f64::INFINITY,
                f64::NEG_INFINITY,
                f64::NEG_INFINITY,
            )
        }));
        probes.push(probe);
    }
    let mut nest_issue: Option<ValidationIssue> = None;
    let _ = for_each_overlapping_bounds_pair(&hole_bounds, |index, other_index| {
        let other = &polygon.holes[other_index];
        if ring_classify_point(other.coords(), probes[index]) == RingClass::Interior {
            let (a, b) = (index.min(other_index), index.max(other_index));
            nest_issue = Some(ValidationIssue::new(
                "hole_overlap",
                format!(
                    "interior ring at index {a} and interior ring at index {b} intersect on an area"
                ),
                Some(probes[index]),
                path,
            ));
            return std::ops::ControlFlow::Break(());
        }
        let hole = &polygon.holes[index];
        if ring_classify_point(hole.coords(), probes[other_index]) == RingClass::Interior {
            let (a, b) = (index.min(other_index), index.max(other_index));
            nest_issue = Some(ValidationIssue::new(
                "hole_overlap",
                format!(
                    "interior ring at index {a} and interior ring at index {b} intersect on an area"
                ),
                Some(probes[other_index]),
                path,
            ));
            return std::ops::ControlFlow::Break(());
        }
        std::ops::ControlFlow::Continue(())
    })?;
    Ok(nest_issue)
}

/// Member-pair validity of a multipolygon (each member already valid on
/// its own): members may touch at finitely many points but never share
/// linework, cross — including through a shared vertex — or stack.
pub(in crate::geometry) fn multi_polygon_members_issue(
    polygons: &[Polygon],
    path: &str,
) -> crate::error::Result<Option<ValidationIssue>> {
    let overlap =
        |a: usize, b: usize| format!("polygons at indices {} and {} overlap", a.min(b), b.max(a));
    // Chains stage per RING (the kernel's monotone-run identities); the
    // classification reads per MEMBER, so candidate views remap their
    // line id through `member_of` (with adjacency disabled — same-member
    // pairs across rings must never read as chained neighbors).
    let mut chains = LineworkChains::default();
    let ring_count = polygons.iter().try_fold(0_usize, |count, polygon| {
        count
            .checked_add(polygon.rings().count())
            .ok_or(crate::error::ResourceError::SizeOverflow)
    })?;
    let mut member_of = Vec::with_capacity(ring_count);
    for (member, polygon) in polygons.iter().enumerate() {
        for ring in
            std::iter::once(polygon.shell.coords()).chain(polygon.holes.iter().map(Ring::coords))
        {
            if chains.push_line(ring)?.is_none() {
                return Ok(None);
            }
            member_of.push(member);
        }
    }
    let as_member = |seg: &IndexedSegment| IndexedSegment {
        line: member_of[seg.line],
        index: usize::MAX,
        ..*seg
    };
    let mut touches: TouchDirections = HashMap::new();
    if let Some(issue) = visit_interacting_pairs(&chains, |left, right| {
        let (left, right) = (as_member(left), as_member(right));
        if left.line == right.line {
            return Ok(None); // same member: already validated by itself
        }
        classify_ring_pair(&left, &right, &mut touches, &overlap)
    })? {
        return Ok(Some(issue.with_path_prefix(path)));
    }
    if !touches.is_empty()
        && let Some(issue) = settle_touches(&touches, polygons.len(), false, &overlap)
    {
        return Ok(Some(issue.with_path_prefix(path)));
    }

    // Nesting/stacking without boundary contact: one shell probe per
    // member against every other member's area (holes honored — a member
    // inside another's hole is valid). Only OVERLAPPING member bounds are
    // visited — the shared bounds sweep replaces the O(N²) all-pairs
    // compare that paid even for provably disjoint multipolygon parts.
    let mut member_bounds = Vec::with_capacity(polygons.len());
    for polygon in polygons {
        let bounds = Bounds::from_coords(polygon.shell.coords()).unwrap_or_else(|| {
            Bounds::new_raw(
                f64::INFINITY,
                f64::INFINITY,
                f64::NEG_INFINITY,
                f64::NEG_INFINITY,
            )
        });
        member_bounds.push(bounds);
    }
    let mut nest_issue: Option<ValidationIssue> = None;
    let _ = for_each_overlapping_bounds_pair(&member_bounds, |index, other_index| {
        // A shell vertex OFF the other member's whole boundary decides
        // the pair: no cross-member linework contact survived the pair
        // scan, so any non-touch vertex lies strictly inside or
        // outside — and touch points are finite, so the FIRST vertex
        // almost always answers.
        let polygon = &polygons[index];
        let other = &polygons[other_index];
        let probe = member_probe_point(polygon, other);
        if other.contains_point(probe) {
            nest_issue = Some(ValidationIssue::new(
                "polygon_overlap",
                overlap(index, other_index),
                Some(probe),
                path,
            ));
            return std::ops::ControlFlow::Break(());
        }
        let probe = member_probe_point(other, polygon);
        if polygon.contains_point(probe) {
            nest_issue = Some(ValidationIssue::new(
                "polygon_overlap",
                overlap(other_index, index),
                Some(probe),
                path,
            ));
            return std::ops::ControlFlow::Break(());
        }
        std::ops::ControlFlow::Continue(())
    })?;
    Ok(nest_issue)
}

/// A representative point of `member` that avoids every ring of `other`
/// — vertex-first, with [`face_interior_point`] as the all-vertices-touch
/// fallback (finitely many touches make it unreachable in practice).
pub(crate) fn member_probe_point(member: &Polygon, other: &Polygon) -> Point {
    if let Some(vertex) = member.shell.iter().find(|&vertex| {
        other
            .rings()
            .all(|ring| ring_classify_point(&ring, vertex) != RingClass::Boundary)
    }) {
        return vertex;
    }
    face_interior_point(
        &orient_ring(member.shell.coords(), false)
            .iter()
            .map(Point::xy)
            .collect::<Vec<_>>(),
    )
    .point()
}
