#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use crate::geometry::{
    Bounds, CoordSeq, Coordinates, Dimension, HashMap, HashMapExt as _, IndexedSegment,
    LineworkChains, Orientation, Point, PointKey, Polygon, Ring, Shape, ValidationIssue, XY,
    classify_ring_pair, face_interior_point, for_each_overlapping_bounds_pair,
    isolated_point_contact, orient_ring, orientation_xy, point_on_segment, ray_crossing_is_right,
    ring_probe_point, same_point, settle_touches, topology, vertex_witness,
    visit_interacting_pairs,
};
pub(crate) fn line_contains_point<C: Coordinates + ?Sized>(points: &C, point: Point) -> bool {
    if !points
        .segment_pairs()
        .any(|[start, end]| point_on_segment(point, start, end))
    {
        return false;
    }
    if points
        .first_coord()
        .zip(points.last_coord())
        .is_some_and(|(first, last)| same_point(first, last))
    {
        return true;
    }
    !points
        .first_coord()
        .zip(points.last_coord())
        .is_some_and(|(first, last)| same_point(first, point) || same_point(last, point))
}

pub(crate) fn multiline_contains_point<L: AsRef<CoordSeq>>(lines: &[L], point: Point) -> bool {
    let mut boundary_count = 0;
    let mut on_linework = false;
    for line in lines {
        let line = line.as_ref();
        if !line
            .segment_pairs()
            .any(|[start, end]| point_on_segment(point, start, end))
        {
            continue;
        }
        on_linework = true;
        let Some((first, last)) = line.first().zip(line.last()) else {
            continue;
        };
        if same_point(first, last) {
            return true;
        }
        let at_start = same_point(first, point);
        let at_end = same_point(last, point);
        if !at_start && !at_end {
            return true;
        }
        boundary_count += usize::from(at_start) + usize::from(at_end);
    }
    on_linework && boundary_count % 2 == 0
}

pub(crate) const fn bounds_equal_topological(left: Bounds, right: Bounds) -> bool {
    f64_topological_bits(left.minx()) == f64_topological_bits(right.minx())
        && f64_topological_bits(left.miny()) == f64_topological_bits(right.miny())
        && f64_topological_bits(left.maxx()) == f64_topological_bits(right.maxx())
        && f64_topological_bits(left.maxy()) == f64_topological_bits(right.maxy())
}

pub(crate) const fn f64_topological_bits(value: f64) -> u64 {
    match value.to_bits() {
        0x8000_0000_0000_0000 => 0,
        bits => bits,
    }
}

/// Where a probe point sits relative to one ring.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RingClass {
    Exterior,
    Boundary,
    Interior,
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
        if point_on_segment(point, a, b) {
            return RingClass::Boundary;
        }
        if (a.y > point.y) != (b.y > point.y)
            && ray_crossing_is_right(a.x, a.y, b.x, b.y, point.x, point.y)
        {
            inside = !inside;
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

/// Point membership for one packed-polygon row (shell + hole ring window) over
/// shared `SoA` columns — the column lane behind [`ShapeRow::Rings`] PIP.
pub(in crate::geometry) fn polygon_row_point_membership<const BOUNDARY: bool>(
    coords: &CoordSeq,
    ring_offsets: &[i32],
    rings: std::ops::Range<usize>,
    point: Point,
) -> bool {
    if rings.is_empty() || rings.start >= rings.end {
        return false;
    }
    let classify = |ring_index: usize| -> RingClass {
        let start = ring_offsets[ring_index] as usize;
        let end = ring_offsets[ring_index + 1] as usize;
        ring_classify_point_columns(&coords.xs()[start..end], &coords.ys()[start..end], point)
    };
    if BOUNDARY {
        classify(rings.start) != RingClass::Exterior
            && !(rings.start + 1..rings.end).any(|index| classify(index) == RingClass::Interior)
    } else {
        classify(rings.start) == RingClass::Interior
            && !(rings.start + 1..rings.end).any(|index| classify(index) != RingClass::Exterior)
    }
}

/// Exact resolution when the point lies within some edge's bbox: the original
/// scalar scan with the robust `orientation_xy` collinearity test (a boundary
/// requires both bbox containment AND exact collinearity).
pub(crate) fn ring_classify_point_columns_exact(xs: &[f64], ys: &[f64], point: Point) -> RingClass {
    let mut inside = false;
    for ([ax, bx], [ay, by]) in std::iter::zip(xs.array_windows::<2>(), ys.array_windows::<2>()) {
        let (ax, ay, bx, by) = (*ax, *ay, *bx, *by);
        if point.x >= ax.min(bx)
            && point.x <= ax.max(bx)
            && point.y >= ay.min(by)
            && point.y <= ay.max(by)
            && orientation_xy(ax, ay, bx, by, point.x, point.y) == Orientation::Collinear
        {
            return RingClass::Boundary;
        }
        if (ay > point.y) != (by > point.y)
            && ray_crossing_is_right(ax, ay, bx, by, point.x, point.y)
        {
            inside = !inside;
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
        return crate::geometry::topology::ring_contains_interior_columns(xs, ys, point);
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
// Replaces geo's `check_validation` for polygons/multipolygons, whose
// segment-pair scan is UNINDEXED (measured: 90% of a 1.29 s `is_valid` on a
// 16k-vertex ring inside geo's `Line::intersects`). One indexed pass over
// every ring segment classifies all interacting pairs: same-ring pairs use
// the simplicity rule, cross-ring pairs are crossings (invalid), collinear
// overlaps (invalid), or point touches. Touch nodes then settle the two
// hard verdicts pairwise scans miss: WEDGE INTERLEAVING (a ring passing
// through a shared vertex is a crossing even though no segment interiors
// meet) and INTERIOR CONNECTIVITY (a cycle in the rings-and-touch-points
// graph pinches off part of the interior — the GEOS "interior is
// disconnected" class geo does not detect at all). Witness locations ride
// on every issue.

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
) -> Option<ValidationIssue> {
    let rings: Vec<&CoordSeq> = std::iter::once(polygon.shell.coords())
        .chain(polygon.holes.iter().map(Ring::coords))
        .collect();
    let mut chains = LineworkChains::default();
    for ring in &rings {
        chains.push_line(ring)?;
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
    }) {
        return Some(issue.with_path_prefix(path));
    }
    if !touches.is_empty()
        && let Some(issue) = settle_touches(&touches, rings.len(), true, &|a, b| {
            format!(
                "{} and {} intersect",
                ring_label(a.min(b)),
                ring_label(a.max(b))
            )
        })
    {
        return Some(issue.with_path_prefix(path));
    }
    if polygon.holes.is_empty() {
        return None; // nothing to contain, nothing to nest
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
            return Some(ValidationIssue::new(
                format!(
                    "interior ring at index {index} is not contained within the polygon's exterior"
                ),
                Some(probe),
                path,
            ));
        }
        // Empty/degenerate hole bounds cannot nest — use an empty box so the
        // sweep never pairs them. Degenerate rings already failed earlier
        // gates in practice; this is a defensive alignment skip.
        hole_bounds.push(Bounds::from_coords(hole.coords()).unwrap_or_else(|| {
            Bounds::new_unchecked(
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
                format!(
                    "interior ring at index {a} and interior ring at index {b} intersect on an area"
                ),
                Some(probes[other_index]),
                path,
            ));
            return std::ops::ControlFlow::Break(());
        }
        std::ops::ControlFlow::Continue(())
    });
    nest_issue
}

/// Member-pair validity of a multipolygon (each member already valid on
/// its own): members may touch at finitely many points but never share
/// linework, cross — including through a shared vertex — or stack.
pub(in crate::geometry) fn multi_polygon_members_issue(
    polygons: &[Polygon],
    path: &str,
) -> Option<ValidationIssue> {
    let overlap =
        |a: usize, b: usize| format!("polygons at indices {} and {} overlap", a.min(b), b.max(a));
    // Chains stage per RING (the kernel's monotone-run identities); the
    // classification reads per MEMBER, so candidate views remap their
    // line id through `member_of` (with adjacency disabled — same-member
    // pairs across rings must never read as chained neighbors).
    let mut chains = LineworkChains::default();
    let mut member_of: Vec<usize> = Vec::new();
    for (member, polygon) in polygons.iter().enumerate() {
        for ring in
            std::iter::once(polygon.shell.coords()).chain(polygon.holes.iter().map(Ring::coords))
        {
            chains.push_line(ring)?;
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
            return None; // same member: already validated by itself
        }
        classify_ring_pair(&left, &right, &mut touches, &overlap)
    }) {
        return Some(issue.with_path_prefix(path));
    }
    if !touches.is_empty()
        && let Some(issue) = settle_touches(&touches, polygons.len(), false, &overlap)
    {
        return Some(issue.with_path_prefix(path));
    }

    // Nesting/stacking without boundary contact: one shell probe per
    // member against every other member's area (holes honored — a member
    // inside another's hole is valid). Only OVERLAPPING member bounds are
    // visited — the shared bounds sweep replaces the O(N²) all-pairs
    // compare that paid even for provably disjoint multipolygon parts.
    let member_bounds: Vec<Bounds> = polygons
        .iter()
        .map(|polygon| {
            Bounds::from_coords(polygon.shell.coords()).unwrap_or_else(|| {
                Bounds::new_unchecked(
                    f64::INFINITY,
                    f64::INFINITY,
                    f64::NEG_INFINITY,
                    f64::NEG_INFINITY,
                )
            })
        })
        .collect();
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
                overlap(index, other_index),
                Some(probe),
                path,
            ));
            return std::ops::ControlFlow::Break(());
        }
        let probe = member_probe_point(other, polygon);
        if polygon.contains_point(probe) {
            nest_issue = Some(ValidationIssue::new(
                overlap(other_index, index),
                Some(probe),
                path,
            ));
            return std::ops::ControlFlow::Break(());
        }
        std::ops::ControlFlow::Continue(())
    });
    nest_issue
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

/// `int(probe) ∩ int(target) ≠ ∅`, EXACT under no linework contact: every
/// 1D/2D component of `probe` then lies uniformly inside or outside
/// `target` (it cannot touch the boundary without segment contact), so one
/// strictly-contained representative vertex decides the whole component —
/// and bare point parts are their own representatives, classified exactly
/// by the strict kernel (a point ON the boundary counts for neither side).
/// Against a PUNTAL target the uniformity lemma fails (a 1D/2D component
/// cannot lie inside a point — a coinciding vertex is mere boundary
/// contact), so only the probe's bare point parts count there.
pub(crate) fn interior_part_uniform(probe: &Shape, target: &Shape) -> bool {
    if target.topology_dimension() >= Some(Dimension::Curve) {
        probe.any_component_representative(&mut |point| target.contains_point(point))
    } else {
        isolated_point_contact(probe, target)
    }
}

/// `int(probe) ∩ ext(target) ≠ ∅` under no linework contact: an uncovered
/// representative proves its component strictly outside (holes included —
/// a hole IS the exterior), see [`interior_part_uniform`].
pub(crate) fn exterior_part_uniform(probe: &Shape, target: &Shape) -> bool {
    probe.any_component_representative(&mut |point| !target.covers_point(point))
}

/// The symmetric interior-interior entry under no linework contact.
pub(crate) fn interiors_meet_uniform(left: &Shape, right: &Shape) -> bool {
    interior_part_uniform(left, right) || interior_part_uniform(right, left)
}

/// A vertex of one operand STRICTLY inside the other's area proves the
/// interiors intersect — every vertex neighborhood carries its
/// geometry's interior, so a strictly-interior vertex is a one-raycast
/// witness (the touches short-circuit).
pub(crate) fn strict_interior_witness(left: &Shape, right: &Shape) -> bool {
    (right.has_area_parts() && vertex_witness(left, |point| right.contains_point(point)))
        || (left.has_area_parts() && vertex_witness(right, |point| left.contains_point(point)))
}
