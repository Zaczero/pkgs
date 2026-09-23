use crate::error::Result;
use crate::geometry::area::ring_area_sign;
use crate::geometry::overlay::ExactSourceInventory;
use crate::geometry::{
    AreaSign, CoordSeq, Coordinates as _, HashMap, HashSet, PointKey, Polygon, RepairMethod, Ring,
    Shape, SharedSlice, XY, cmp_area_keys_then_closed, polygon_parts_to_shape, ring_decision_area,
    same_point, wrap_index,
};
///
/// `Linework` is the original stored ring's directed winding modulo two on the
/// exact noded partition, so opposite occurrences cancel and duplicate
/// occurrences retain their multiplicity. `Structure` is each ring's
/// nonzero-signed-winding enclosed area. Both methods recombine selected
/// regions as union(shells) − union(holes); collapsed rings contribute their
/// closed line support to the exact source fold before polygon repair.
pub(crate) fn polygonal_repair(polygons: &[Polygon], method: RepairMethod) -> Result<Shape> {
    let empty: &[Polygon] = &[];
    let inventory = ExactSourceInventory::from_polygon_operands(polygons, empty)?;
    let mut budget = crate::geometry::ExpansionBudget::new("repair", "exact partition");
    let partition = inventory.partition(&mut budget)?;
    let (_, _, parts) = match method {
        RepairMethod::Linework => inventory.repair_parts(&partition)?,
        RepairMethod::Structure => inventory.structure_parts(&partition)?,
    };
    // The face walk keeps zero-area antennas (a spike that re-enters at a
    // noded vertex is traversed once per side, leaving an `A, B, A`
    // backtrack in the boundary) — strip them so the rebuilt rings are
    // OGC-valid; an antenna encloses nothing, so the area is unchanged.
    let mut cleaned = Vec::with_capacity(parts.len());
    for polygon in parts {
        let Some(shell) = ring_without_antennas(&polygon.shell)? else {
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
        let loops = split_pinched_ring(&shell)?;
        let dominant = loops
            .iter()
            .map(|ring| (ring, ring_decision_area(ring.coords())))
            .max_by(|(left, left_area), (right, right_area)| {
                cmp_area_keys_then_closed(
                    left_area.magnitude().get(),
                    right_area.magnitude().get(),
                    left.coords(),
                    right.coords(),
                )
            })
            .map_or(AreaSign::Zero, |(_, decision)| decision.sign());
        let mut shells = Vec::with_capacity(loops.len());
        let mut holes = Vec::with_capacity(loops.len());
        for ring in loops {
            if ring_area_sign(ring.coords()) == dominant {
                shells.push(ring);
            } else {
                holes.push(ring);
            }
        }
        for hole in polygon.holes.iter() {
            if let Some(hole) = ring_without_antennas(hole)? {
                let split = split_pinched_ring(&hole)?;
                holes.reserve(split.len());
                holes.extend(split);
            }
        }
        if shells.len() == 1 {
            cleaned.push(Polygon::new(
                shells.into_iter().next().expect("one shell"),
                holes,
            ));
        } else {
            // Re-attach each hole to the lobe that contains it.
            let mut lobes: Vec<_> = shells
                .into_iter()
                .map(|shell| (Polygon::new(shell, Vec::new()), Vec::new()))
                .collect();
            for hole in holes {
                let anchor = hole.coords().first_coord().expect("non-empty ring");
                if let Some((_, assigned)) =
                    lobes.iter_mut().find(|(lobe, _)| lobe.covers_point(anchor))
                {
                    assigned.push(hole);
                }
            }
            cleaned.reserve(lobes.len());
            for (mut lobe, holes) in lobes {
                if !holes.is_empty() {
                    lobe.holes = SharedSlice::from_vec(holes);
                }
                cleaned.push(lobe);
            }
        }
    }
    Ok(polygon_parts_to_shape(cleaned))
}

/// Split a ring that touches itself at single vertices (a pinch) into its
/// constituent loops: each revisited vertex closes the loop opened at its
/// first occurrence. A valid ring comes back unchanged as one loop.
pub(crate) fn split_pinched_ring(ring: &Ring) -> Result<Vec<Ring>> {
    // CLEAN fast path (the overwhelmingly common case): no repeated
    // vertex besides the closure means the ring IS its own single loop —
    // detect with one presized pass and return it untouched, skipping loop
    // extraction and the column rebuild entirely.
    let count = {
        let coords = ring.coords();
        let (xs, ys) = (coords.xs(), coords.ys());
        let (_, xs) = xs.split_last().expect("ring has a closing coordinate");
        let (_, ys) = ys.split_last().expect("ring has a closing coordinate");
        // PointKey identity is CONSERVATIVE here (canonicalizes ±0.0 and
        // ignores Z/M): a false duplicate just routes through the exact
        // splitter below, which then splits nothing — never the reverse.
        let mut seen: HashSet<PointKey> = crate::collections::try_hash_set_with_capacity(xs.len())?;
        if xs
            .iter()
            .zip(ys)
            .all(|(&x, &y)| seen.insert(PointKey::new(XY::new(x, y))))
        {
            return Ok(vec![ring.clone()]);
        }
        xs.len()
    };
    let mut points = ring.coords().iter_coords();
    points.next_back().expect("ring has a closing coordinate");
    let mut loops = Vec::with_capacity(count);
    let mut stack = Vec::with_capacity(count);
    let mut open: HashMap<PointKey, usize> = crate::collections::try_hash_map_with_capacity(count)?;
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
    let mut result = Vec::with_capacity(loops.len());
    for mut points in loops {
        let first = points[0];
        points.push(first);
        result.push(Ring::try_from_closed(CoordSeq::try_from_points(&points)?)?);
    }
    Ok(result)
}

/// Strip zero-area antennas from a closed ring: any vertex whose circular
/// neighbors coincide (`prev == next`) sits at the tip of a doubly-traversed
/// spike — remove the tip and one duplicate, repeating until stable.
/// Returns `None` when the ring collapses below a triangle.
pub(crate) fn ring_without_antennas(ring: &Ring) -> Result<Option<Ring>> {
    // CLEAN fast path: a columnar circular `prev == next` sweep finds the
    // (rare) antenna tips without materializing a `Point` per vertex; an
    // antenna-free ring returns untouched.
    {
        let coords = ring.coords();
        let (xs, ys) = (coords.xs(), coords.ys());
        let (_, xs) = xs.split_last().expect("ring has a closing coordinate");
        let (_, ys) = ys.split_last().expect("ring has a closing coordinate");
        let len = xs.len();
        let clean = (0..len).all(|index| {
            let prev = wrap_index(index + len - 1, len);
            let next = wrap_index(index + 1, len);
            !same_point(XY::new(xs[prev], ys[prev]), XY::new(xs[next], ys[next]))
        });
        if clean {
            return Ok(Some(ring.clone()));
        }
    }
    let mut points = Vec::with_capacity(ring.coord_count());
    let mut source = ring.coords().iter_coords();
    source.next_back().expect("ring has a closing coordinate");
    points.extend(source);
    loop {
        if points.len() < 3 {
            return Ok(None);
        }
        let mut removed = false;
        let mut index = 0;
        while index < points.len() {
            let len = points.len();
            if len < 3 {
                return Ok(None);
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
    let first = *points.first().expect("surviving ring has vertices");
    points.push(first);
    Ok(Ring::from_closed(CoordSeq::try_from_points(&points)?))
}
