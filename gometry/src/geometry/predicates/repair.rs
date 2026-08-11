use ahash::HashSetExt as _;

use crate::error::Result;
use crate::geometry::predicates::{
    face_interior_point, prune_dangles, segments_contain_interior, validate_points,
};
use crate::geometry::{
    AreaSign, Arrangement, Coordinates, GeometryErrorKind, HashMap, HashMapExt as _, HashSet,
    OverlayOp, Point, PointKey, Polygon, RepairMethod, Ring, RingDecisionArea, Segment, Shape, XY,
    assemble_region_polygons, binary_areal_overlay, line_segments, minimal_positive_face_rings,
    ordered_edge, polygon_parts_to_shape, polygon_winding_loops, ring_decision_area, same_point,
    self_node_segments, winding_region, wrap_index,
};
///
/// `Linework` is even-odd parity over the DEDUPLICATED noded linework
/// (exactly GEOS' linework `make_valid`: a same-ring retraced edge counts
/// once); `Structure` is each ring's NONZERO-signed-winding enclosed area
/// (exactly GEOS' `GeometryFixer` per-ring zero-buffer: a many-wound shell
/// fills everything it winds around), recombined as
/// union(shells) − union(holes). Collapsed rings enclose nothing and drop
/// out of either fold.
pub(crate) fn polygonal_repair(polygons: &[Polygon], method: RepairMethod) -> Result<Shape> {
    let area: Vec<Polygon> = match method {
        RepairMethod::Linework => {
            // CLEAN-single-ring fast path: one self-crossing loop with no
            // duplicate edges, T-junctions, or repeats builds positionally
            // (`from_single_loop` owns every bail), and directed winding
            // parity == undirected crossing parity when no edge repeats —
            // one arrangement instead of the per-ring face decomposition
            // plus the joint re-noding pass below.
            if let [polygon] = polygons
                && polygon.holes.is_empty()
                && let Some(arrangement) = single_ring_arrangement(&polygon.shell)?
            {
                let windings = arrangement.face_windings(&[0]);
                assemble_region_polygons(
                    arrangement.region_rings(&windings, |winding| winding % 2 != 0),
                )
            } else {
                // The xor-fold across ring areas IS odd-coverage selection:
                // every face area is a simple CCW polygon, so a face of
                // their joint arrangement is kept exactly when an ODD
                // number of areas cover it (winding counts the covers).
                let mut faces = Vec::new();
                for polygon in polygons {
                    for ring in polygon.rings() {
                        faces.extend(ring_even_odd_area(&ring)?);
                    }
                }
                winding_region(&polygon_winding_loops(&faces), |winding| winding % 2 == 1)
            }
        },
        RepairMethod::Structure => {
            let mut shells = Vec::new();
            let mut holes = Vec::new();
            for polygon in polygons {
                shells.extend(ring_structure_area(&polygon.shell)?);
                for hole in polygon.holes.iter() {
                    holes.extend(ring_structure_area(hole)?);
                }
            }
            let single_shell_source = matches!(polygons, [polygon] if polygon.holes.is_empty());
            if single_shell_source {
                // One source shell: its nonzero-winding area is already the
                // final disjoint region — nothing to union or subtract.
                shells
            } else {
                // union(shells) − union(holes) in one overlay pass (empty
                // holes degrade to the pure shell union).
                binary_areal_overlay(&shells, &holes, OverlayOp::Difference)
            }
        },
    };
    let parts: Vec<Polygon> = area;
    // The face walk keeps zero-area antennas (a spike that re-enters at a
    // noded vertex is traversed once per side, leaving an `A, B, A`
    // backtrack in the boundary) — strip them so the rebuilt rings are
    // OGC-valid; an antenna encloses nothing, so the area is unchanged.
    let mut cleaned: Vec<Polygon> = Vec::with_capacity(parts.len());
    for polygon in parts {
        let Some(shell) = ring_without_antennas(&polygon.shell) else {
            continue;
        };
        // The boolean fold can also merge lobes that touch at one vertex
        // into a single pinched ring (invalid OGC shell) — split every
        // pinch back into its loops; lobes are disjoint by construction,
        // so each shell loop is its own part.
        // A pinched shell splits into loops; the face walk traverses the
        // boundary with the face consistently on one side, so loops winding
        // WITH the dominant loop are sibling lobes and loops winding
        // AGAINST it are holes touching the shell at the pinch.
        let loops = split_pinched_ring(&shell);
        let dominant = loops
            .iter()
            .map(|ring| ring_decision_area(ring.coords()))
            .max_by(|left, right| left.magnitude().abs_cmp(right.magnitude()))
            .map_or(AreaSign::Zero, RingDecisionArea::sign);
        let (shells, mut holes): (Vec<Ring>, Vec<Ring>) = loops
            .into_iter()
            .partition(|ring| ring_decision_area(ring.coords()).sign() == dominant);
        holes.extend(
            polygon
                .holes
                .iter()
                .filter_map(ring_without_antennas)
                .flat_map(|hole| split_pinched_ring(&hole)),
        );
        if shells.len() == 1 {
            cleaned.push(Polygon::new(
                shells.into_iter().next().expect("one shell"),
                holes,
            ));
        } else {
            // Re-attach each hole to the lobe that contains it.
            let mut lobes: Vec<Polygon> = shells
                .into_iter()
                .map(|shell| Polygon::new(shell, Vec::new()))
                .collect();
            let mut lobe_holes: Vec<Vec<Ring>> = vec![Vec::new(); lobes.len()];
            for hole in holes {
                let anchor = hole.coords().first_coord().expect("non-empty ring");
                if let Some(index) = lobes.iter().position(|lobe| lobe.covers_point(anchor)) {
                    lobe_holes[index].push(hole);
                }
            }
            for (lobe, holes) in lobes.iter_mut().zip(lobe_holes) {
                lobe.holes = holes.into();
            }
            cleaned.extend(lobes);
        }
    }
    Ok(polygon_parts_to_shape(cleaned))
}

/// Split a ring that touches itself at single vertices (a pinch) into its
/// constituent loops: each revisited vertex closes the loop opened at its
/// first occurrence. A valid ring comes back unchanged as one loop.
pub(crate) fn split_pinched_ring(ring: &Ring) -> Vec<Ring> {
    // CLEAN fast path (the overwhelmingly common case): no repeated
    // vertex besides the closure means the ring IS its own single loop —
    // detect with one presized pass and return it untouched, skipping the
    // `Vec<Point>` staging and the column rebuild entirely.
    {
        let coords = ring.coords();
        let (xs, ys) = (coords.xs(), coords.ys());
        let count = xs.len();
        let closed =
            count >= 2 && same_point(XY::new(xs[0], ys[0]), XY::new(xs[count - 1], ys[count - 1]));
        let scan = if closed { count - 1 } else { count };
        // PointKey identity is CONSERVATIVE here (canonicalizes ±0.0 and
        // ignores Z/M): a false duplicate just routes through the exact
        // splitter below, which then splits nothing — never the reverse.
        let mut seen: HashSet<PointKey> = HashSet::with_capacity(scan);
        if (0..scan).all(|index| seen.insert(PointKey::new(XY::new(xs[index], ys[index])))) {
            return vec![ring.clone()];
        }
    }
    let mut points: Vec<Point> = ring.coords().iter_coords().collect();
    if points.len() >= 2 && same_point(points[0], *points.last().expect("non-empty")) {
        points.pop();
    }
    let count = points.len();
    let mut loops: Vec<Vec<Point>> = Vec::new();
    let mut stack: Vec<Point> = Vec::with_capacity(count);
    let mut open: HashMap<PointKey, usize> = HashMap::with_capacity(count);
    for point in points {
        let key = PointKey::new(point);
        if let Some(&start) = open.get(&key) {
            let extracted = stack.split_off(start);
            for vertex in &extracted {
                open.remove(&PointKey::new(*vertex));
            }
            if extracted.len() >= 3 {
                loops.push(extracted);
            }
        }
        open.insert(key, stack.len());
        stack.push(point);
    }
    if stack.len() >= 3 {
        loops.push(stack);
    }
    loops
        .into_iter()
        .map(|mut points| {
            let first = points[0];
            points.push(first);
            Ring::from_trusted_closed(points)
        })
        .collect()
}

/// Strip zero-area antennas from a closed ring: any vertex whose circular
/// neighbors coincide (`prev == next`) sits at the tip of a doubly-traversed
/// spike — remove the tip and one duplicate, repeating until stable.
/// Returns `None` when the ring collapses below a triangle.
pub(crate) fn ring_without_antennas(ring: &Ring) -> Option<Ring> {
    // CLEAN fast path: a columnar circular `prev == next` sweep finds the
    // (rare) antenna tips without materializing a `Point` per vertex; an
    // antenna-free ring returns untouched.
    {
        let coords = ring.coords();
        let (xs, ys) = (coords.xs(), coords.ys());
        let mut len = xs.len();
        if len >= 2 && same_point(XY::new(xs[0], ys[0]), XY::new(xs[len - 1], ys[len - 1])) {
            len -= 1;
        }
        if len < 3 {
            return None;
        }
        let clean = (0..len).all(|index| {
            let prev = wrap_index(index + len - 1, len);
            let next = wrap_index(index + 1, len);
            !same_point(XY::new(xs[prev], ys[prev]), XY::new(xs[next], ys[next]))
        });
        if clean {
            return Some(ring.clone());
        }
    }
    let mut points: Vec<Point> = ring.coords().iter_coords().collect();
    if points.len() >= 2 && same_point(points[0], *points.last().expect("non-empty")) {
        points.pop();
    }
    loop {
        if points.len() < 3 {
            return None;
        }
        let mut removed = false;
        let mut index = 0;
        while index < points.len() {
            let len = points.len();
            if len < 3 {
                return None;
            }
            let prev = points[wrap_index(index + len - 1, len)];
            let next = points[wrap_index(index + 1, len)];
            if same_point(prev, next) {
                let duplicate = wrap_index(index + 1, len);
                if duplicate > index {
                    points.remove(duplicate);
                    points.remove(index);
                } else {
                    points.remove(index);
                    points.remove(duplicate);
                }
                removed = true;
            } else {
                index += 1;
            }
        }
        if !removed {
            break;
        }
    }
    if points.len() < 3 {
        return None;
    }
    let first = points[0];
    points.push(first);
    Some(Ring::from_trusted_closed(points))
}

/// The even-odd enclosed area of one closed ring, as geo polygons ready for
/// boolean combination.
///
/// The ring is noded against itself and decomposed into the minimal faces of
/// its planar subdivision ([`minimal_positive_face_rings`]); a face belongs to
/// the area when an interior probe point crosses the ring's boundary an odd
/// number of times. A single closed polyline yields a connected subdivision,
/// so faces are simply connected and parity is uniform per face. Only
/// non-finite coordinates are unrepairable; degenerate rings enclose nothing.
/// The CLEAN-single-ring arrangement for repair's fast path: finite-XY
/// validated, closed positionally, `None` when the loop needs the general
/// engine (duplicate edges, T-junctions, repeats — `from_single_loop`
/// owns every bail).
/// The repair lanes' finite gate: the branchless column probe of
/// [`validate_points`], surfaced as the repair error (witness formatting
/// stays off the hot path).
pub(crate) fn require_finite_ring<C: Coordinates + ?Sized>(ring: &C) -> Result<()> {
    match validate_points(ring, "$") {
        None => Ok(()),
        Some(issue) => Err(GeometryErrorKind::repair_failed(format!(
            "ring contains non-finite coordinate {}",
            issue.location.expect("finite-gate witness")
        ))),
    }
}

pub(crate) fn single_ring_arrangement(ring: &Ring) -> Result<Option<Arrangement>> {
    require_finite_ring(ring)?;
    let coords = ring.coords();
    let (xs, ys) = (coords.xs(), coords.ys());
    let count = xs.len();
    if count < 3 {
        return Ok(None);
    }
    let mut segments = Vec::with_capacity(count);
    for index in 0..count {
        let next = wrap_index(index + 1, count);
        let start = XY::new(xs[index], ys[index]);
        let end = XY::new(xs[next], ys[next]);
        if !same_point(start, end) {
            segments.push(Segment { start, end });
        }
    }
    Ok(Arrangement::from_single_loop(&segments))
}

/// A ring's NONZERO-signed-winding enclosed area — the `Structure` repair
/// primitive (GEOS `GeometryFixer` fixes each ring with a zero-buffer,
/// which fills every face the ring winds around in EITHER direction).
/// Directed retraces stay raw on purpose: same-direction duplicates raise
/// the winding magnitude and still fill, opposite-direction pairs cancel
/// and collapse — unlike the linework dedup in [`ring_even_odd_area`].
pub(crate) fn ring_structure_area<C: Coordinates + ?Sized>(ring: &C) -> Result<Vec<Polygon>> {
    require_finite_ring(ring)?;
    let (mut xs, mut ys) = if let Some((xs, ys)) = ring.xy_columns() {
        (xs.to_vec(), ys.to_vec())
    } else {
        let mut xs = Vec::with_capacity(ring.coord_count());
        let mut ys = Vec::with_capacity(ring.coord_count());
        for point in ring.iter_coords() {
            xs.push(point.x);
            ys.push(point.y);
        }
        (xs, ys)
    };
    // Drop an explicit closing duplicate — the loop closes positionally.
    if xs.len() > 1
        && let (Some(&first_x), Some(&first_y)) = (xs.first(), ys.first())
        && let (Some(&last_x), Some(&last_y)) = (xs.last(), ys.last())
        && same_point(XY::new(first_x, first_y), XY::new(last_x, last_y))
    {
        xs.pop();
        ys.pop();
    }
    if xs.len() < 3 {
        return Ok(Vec::new());
    }
    Ok(winding_region(&[(xs, ys)], |winding| winding != 0))
}

pub(crate) fn ring_even_odd_area<C: Coordinates + ?Sized>(ring: &C) -> Result<Vec<Polygon>> {
    require_finite_ring(ring)?;
    let mut closed = ring.iter_coords().map(Point::force_2d).collect::<Vec<_>>();
    if let (Some(&first), Some(&last)) = (closed.first(), closed.last())
        && !same_point(first, last)
    {
        closed.push(first);
    }

    let segments = line_segments(&closed).collect::<Vec<_>>();
    // Even-odd parity is taken over the DEDUPLICATED noded linework: an edge
    // traced twice within one ring counts once, so a fully re-traced ring
    // still encloses its area (matching GEOS ``make_valid``) instead of
    // cancelling to nothing. The face graph already deduplicates edges; the
    // parity probe must raycast the same deduplicated set.
    let mut atomic = self_node_segments(&segments);
    let mut seen = HashSet::with_capacity(atomic.len());
    atomic.retain(|segment| {
        seen.insert(ordered_edge(
            PointKey::new(segment.start),
            PointKey::new(segment.end),
        ))
    });
    // Deduplication can leave dangles (a spike traced out and back collapses
    // to one bare edge). A dangle encloses nothing but would flip the
    // raycast parity, so prune edges with a degree-1 endpoint iteratively
    // before taking parity (the polygonizer ignores them for faces anyway).
    prune_dangles(&mut atomic);
    Ok(minimal_positive_face_rings(&atomic)
        .into_iter()
        .filter(|face| segments_contain_interior(&atomic, face_interior_point(face)))
        .map(|face| Polygon::new(Ring::from_trusted_closed(face), Vec::new()))
        .collect())
}
