use ahash::HashSetExt as _;

use crate::error::Result;
use crate::geometry::overlay::outside_winding_seeds;
use crate::geometry::predicates::{prune_dangles, validate_points};
use crate::geometry::{
    AreaSign, Arrangement, Coordinates, GeometryErrorKind, HashMap, HashMapExt as _, HashSet,
    OverlayOp, Point, PointKey, Polygon, RepairMethod, Ring, Segment, Shape, XY,
    assemble_region_polygons, binary_areal_overlay, line_segments, ordered_edge,
    polygon_parts_to_shape, ring_decision_area, same_point, self_node_segments, winding_region,
    wrap_index,
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
                && let Some(mut arrangement) = single_ring_arrangement(&polygon.shell)?
            {
                let windings = arrangement.face_windings(&[0]);
                assemble_region_polygons(
                    arrangement.region_rings(&windings, |winding| winding % 2 != 0),
                )
            } else {
                linework_area(polygons)?
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
            .map(|ring| (ring, ring_decision_area(ring.coords())))
            .max_by(|(left, left_area), (right, right_area)| {
                crate::geometry::cmp_area_keys_then_closed(
                    left_area.magnitude().get(),
                    right_area.magnitude().get(),
                    left.coords(),
                    right.coords(),
                )
            })
            .map_or(AreaSign::Zero, |(_, decision)| decision.sign());
        let (shells, mut holes): (Vec<Ring>, Vec<Ring>) = loops
            .into_iter()
            .partition(|ring| crate::geometry::area::ring_area_sign(ring.coords()) == dominant);
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
                if !holes.is_empty() {
                    lobe.holes = holes.into();
                }
            }
            cleaned.extend(lobes);
        }
    }
    Ok(polygon_parts_to_shape(cleaned))
}

/// Build the linework repair region from one joint arrangement.
///
/// The previous shape decomposed every ring independently and then noded the
/// resulting face polygons again.  Noding all rings once is equivalent after
/// collapsing duplicate edges *within each source ring*: duplicate source
/// rings still contribute twice and cancel under parity, while a retraced edge
/// inside one ring contributes once as required by linework repair.
fn linework_area(polygons: &[Polygon]) -> Result<Vec<Polygon>> {
    let coordinate_count = polygons
        .iter()
        .map(|polygon| {
            polygon
                .rings()
                .map(|ring| ring.coord_count())
                .sum::<usize>()
        })
        .sum();
    let mut segments = Vec::with_capacity(coordinate_count);
    let mut segment_loop = Vec::with_capacity(coordinate_count);
    let mut loop_anchor = Vec::with_capacity(polygons.len());
    for polygon in polygons {
        for ring in polygon.rings() {
            require_finite_ring(&ring)?;
            let mut closed = ring.iter_coords().map(Point::force_2d).collect::<Vec<_>>();
            if let (Some(&first), Some(&last)) = (closed.first(), closed.last())
                && !same_point(first, last)
            {
                closed.push(first);
            }
            let mut ring_edges = HashSet::with_capacity(closed.len());
            let before = segments.len();
            for segment in line_segments(&closed) {
                if same_point(segment.start, segment.end)
                    || !ring_edges.insert(ordered_edge(
                        PointKey::new(segment.start),
                        PointKey::new(segment.end),
                    ))
                {
                    continue;
                }
                segments.push(segment);
                segment_loop.push(loop_anchor.len() as u32);
            }
            if segments.len() > before {
                loop_anchor.push(closed[0].xy());
            }
        }
    }
    if segments.is_empty() {
        return Ok(Vec::new());
    }

    let mut atomic = self_node_segments(&segments);
    prune_dangles(&mut atomic);
    if atomic.is_empty() {
        return Ok(Vec::new());
    }
    let mut arrangement = Arrangement::new(&atomic);
    drop(atomic);
    let all_source_vertices_survive = segments.iter().all(|segment| {
        arrangement.ids.contains_key(&PointKey::new(segment.start))
            && arrangement.ids.contains_key(&PointKey::new(segment.end))
    });
    let seeds = if all_source_vertices_survive {
        // The normal case keeps the original source slices: cloning them just
        // to make the seed query's inputs owned inflated each row's peak live
        // set without changing the ray crossings.
        let loop_component: Vec<u32> = loop_anchor
            .iter()
            .map(|&anchor| arrangement.component_of_point(anchor))
            .collect();
        let segment_operand = vec![0_u32; segments.len()];
        outside_winding_seeds(
            &segments,
            &segment_operand,
            &segment_loop,
            &loop_component,
            &arrangement.component_probes(),
        )
        .into_iter()
        .map(|seed| seed[0])
        .collect::<Vec<_>>()
    } else {
        // Dangle pruning can remove a repeated-edge branch, including the
        // original anchor. Keep only source edges that survived in the joint
        // graph when calculating outside seeds; otherwise a discarded antenna
        // could perturb the ray crossing for a component that did survive.
        let mut active_loop = vec![u32::MAX; loop_anchor.len()];
        let mut active_anchor = Vec::new();
        for (loop_index, _) in loop_anchor.iter().enumerate() {
            let anchor = segments
                .iter()
                .zip(&segment_loop)
                .filter(|(_, source)| **source as usize == loop_index)
                .find_map(|(segment, _)| {
                    (arrangement.ids.contains_key(&PointKey::new(segment.start))
                        && arrangement.ids.contains_key(&PointKey::new(segment.end)))
                    .then_some(segment.start)
                });
            if let Some(anchor) = anchor {
                active_loop[loop_index] = active_anchor.len() as u32;
                active_anchor.push(anchor);
            }
        }
        let mut seed_segments = Vec::new();
        let mut seed_loop = Vec::new();
        for (segment, &loop_index) in segments.iter().zip(&segment_loop) {
            let active = active_loop[loop_index as usize];
            if active != u32::MAX
                && arrangement.ids.contains_key(&PointKey::new(segment.start))
                && arrangement.ids.contains_key(&PointKey::new(segment.end))
            {
                seed_segments.push(*segment);
                seed_loop.push(active);
            }
        }
        let loop_component: Vec<u32> = active_anchor
            .iter()
            .map(|&anchor| arrangement.component_of_point(anchor))
            .collect();
        let segment_operand = vec![0_u32; seed_segments.len()];
        outside_winding_seeds(
            &seed_segments,
            &segment_operand,
            &seed_loop,
            &loop_component,
            &arrangement.component_probes(),
        )
        .into_iter()
        .map(|seed| seed[0])
        .collect::<Vec<_>>()
    };
    let windings = arrangement.face_windings(&seeds);
    Ok(assemble_region_polygons(
        arrangement.region_rings(&windings, |winding| winding % 2 != 0),
    ))
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
            Ring::from_closed(points)
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
    Some(Ring::from_closed(points))
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
