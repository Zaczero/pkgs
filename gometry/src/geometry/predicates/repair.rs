use crate::error::Result;
use crate::geometry::area::ring_area_sign;
use crate::geometry::overlay::ExactSourceInventory;
use crate::geometry::{
    AreaSign, CoordSeq, Coordinates as _, HashMap, HashSet, PointKey, Polygon, RepairMethod, Ring,
    Shape, SharedSlice, XY, cmp_area_keys_then_closed, polygon_parts_to_shape, ring_decision_area,
    same_point, wrap_index,
};
use crate::{try_push_vec, try_reserve_vec};
///
/// `Linework` is the original stored ring's directed winding modulo two on the
/// exact noded partition, so opposite occurrences cancel and duplicate
/// occurrences retain their multiplicity. `Structure` is each ring's
/// nonzero-signed-winding enclosed area. Both methods recombine selected
/// regions as union(shells) − union(holes); collapsed rings contribute their
/// closed line support to the exact source fold before polygon repair.
pub(crate) fn polygonal_repair(polygons: &[Polygon], method: RepairMethod) -> Result<Shape> {
    let area: Vec<Polygon> = match method {
        RepairMethod::Linework | RepairMethod::Structure => {
            let empty: &[Polygon] = &[];
            let inventory = ExactSourceInventory::from_polygon_operands(polygons, empty)?;
            let mut budget = crate::geometry::ExpansionBudget::new("repair", "exact partition");
            let partition = inventory.partition(&mut budget)?;
            let (_, _, parts) = if method == RepairMethod::Linework {
                inventory.repair_parts(&partition)?
            } else {
                inventory.structure_parts(&partition)?
            };
            parts
        },
    };
    let parts: Vec<Polygon> = area;
    // The face walk keeps zero-area antennas (a spike that re-enters at a
    // noded vertex is traversed once per side, leaving an `A, B, A`
    // backtrack in the boundary) — strip them so the rebuilt rings are
    // OGC-valid; an antenna encloses nothing, so the area is unchanged.
    let mut cleaned = Vec::new();
    try_reserve_vec(&mut cleaned, parts.len())?;
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
        let mut shells = Vec::new();
        let mut holes = Vec::new();
        try_reserve_vec(&mut shells, loops.len())?;
        try_reserve_vec(&mut holes, loops.len())?;
        for ring in loops {
            if ring_area_sign(ring.coords()) == dominant {
                try_push_vec(&mut shells, ring)?;
            } else {
                try_push_vec(&mut holes, ring)?;
            }
        }
        for hole in polygon.holes.iter() {
            if let Some(hole) = ring_without_antennas(hole)? {
                for hole in split_pinched_ring(&hole)? {
                    try_push_vec(&mut holes, hole)?;
                }
            }
        }
        if shells.len() == 1 {
            try_push_vec(
                &mut cleaned,
                Polygon::new(shells.into_iter().next().expect("one shell"), holes)?,
            )?;
        } else {
            // Re-attach each hole to the lobe that contains it.
            let mut lobes = Vec::new();
            try_reserve_vec(&mut lobes, shells.len())?;
            for shell in shells {
                try_push_vec(&mut lobes, Polygon::new(shell, Vec::new())?)?;
            }
            let mut lobe_holes = Vec::new();
            try_reserve_vec(&mut lobe_holes, lobes.len())?;
            lobe_holes.resize_with(lobes.len(), Vec::new);
            for hole in holes {
                let anchor = hole.coords().first_coord().expect("non-empty ring");
                if let Some(index) = lobes.iter().position(|lobe| lobe.covers_point(anchor)) {
                    try_push_vec(&mut lobe_holes[index], hole)?;
                }
            }
            for (lobe, holes) in lobes.iter_mut().zip(lobe_holes) {
                if !holes.is_empty() {
                    lobe.holes = SharedSlice::try_from_vec(holes)?;
                }
            }
            for lobe in lobes {
                try_push_vec(&mut cleaned, lobe)?;
            }
        }
    }
    polygon_parts_to_shape(cleaned)
}

/// Split a ring that touches itself at single vertices (a pinch) into its
/// constituent loops: each revisited vertex closes the loop opened at its
/// first occurrence. A valid ring comes back unchanged as one loop.
pub(crate) fn split_pinched_ring(ring: &Ring) -> Result<Vec<Ring>> {
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
        let mut seen: HashSet<PointKey> =
            HashSet::with_hasher(crate::collections::collection_hash_state()?.clone());
        seen.try_reserve(scan).map_err(crate::error::Error::from)?;
        if (0..scan).all(|index| seen.insert(PointKey::new(XY::new(xs[index], ys[index])))) {
            let mut result = Vec::new();
            try_push_vec(&mut result, ring.clone())?;
            return Ok(result);
        }
    }
    let mut points = Vec::new();
    try_reserve_vec(&mut points, ring.coord_count())?;
    for point in ring.coords().iter_coords() {
        try_push_vec(&mut points, point)?;
    }
    if points.len() >= 2 && same_point(points[0], *points.last().expect("non-empty")) {
        points.pop();
    }
    let count = points.len();
    let mut loops = Vec::new();
    try_reserve_vec(&mut loops, count)?;
    let mut stack = Vec::new();
    try_reserve_vec(&mut stack, count)?;
    let mut open: HashMap<PointKey, usize> =
        HashMap::with_hasher(crate::collections::collection_hash_state()?.clone());
    open.try_reserve(count).map_err(crate::error::Error::from)?;
    for point in points {
        let key = PointKey::new(point);
        if let Some(&start) = open.get(&key) {
            let extracted = stack.split_off(start);
            for vertex in &extracted {
                open.remove(&PointKey::new(*vertex));
            }
            if extracted.len() >= 3 {
                try_push_vec(&mut loops, extracted)?;
            }
        }
        open.insert(key, stack.len());
        try_push_vec(&mut stack, point)?;
    }
    if stack.len() >= 3 {
        try_push_vec(&mut loops, stack)?;
    }
    let mut result = Vec::new();
    try_reserve_vec(&mut result, loops.len())?;
    for mut points in loops {
        let first = points[0];
        try_push_vec(&mut points, first)?;
        try_push_vec(
            &mut result,
            Ring::from_closed(CoordSeq::try_from_points(&points)?),
        )?;
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
        let mut len = xs.len();
        if len >= 2 && same_point(XY::new(xs[0], ys[0]), XY::new(xs[len - 1], ys[len - 1])) {
            len -= 1;
        }
        if len < 3 {
            return Ok(None);
        }
        let clean = (0..len).all(|index| {
            let prev = wrap_index(index + len - 1, len);
            let next = wrap_index(index + 1, len);
            !same_point(XY::new(xs[prev], ys[prev]), XY::new(xs[next], ys[next]))
        });
        if clean {
            return Ok(Some(ring.clone()));
        }
    }
    let mut points = Vec::new();
    try_reserve_vec(&mut points, ring.coord_count())?;
    for point in ring.coords().iter_coords() {
        try_push_vec(&mut points, point)?;
    }
    if points.len() >= 2 && same_point(points[0], *points.last().expect("non-empty")) {
        points.pop();
    }
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
    if points.len() < 3 {
        return Ok(None);
    }
    let first = points[0];
    try_push_vec(&mut points, first)?;
    Ok(Some(Ring::from_closed(CoordSeq::try_from_points(&points)?)))
}
