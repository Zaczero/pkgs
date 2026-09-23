use crate::error::Result;
use crate::geometry::overlay::ExactSourceInventory;
use crate::geometry::{
    CoordSeq, Coordinates as _, Polygon, RepairMethod, Ring, Shape, XY, polygon_parts_to_shape,
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
        let mut holes = Vec::with_capacity(polygon.holes.len());
        for hole in polygon.holes.iter() {
            if let Some(hole) = ring_without_antennas(hole)? {
                holes.push(hole);
            }
        }
        cleaned.push(Polygon::new(shell, holes));
    }
    polygon_parts_to_shape(cleaned)
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
