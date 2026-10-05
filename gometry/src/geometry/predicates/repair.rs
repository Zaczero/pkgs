use super::validate::{collapsed_line_point, validate_line};
use crate::error::Result;
use crate::geometry::overlay::ExactSourceInventory;
use crate::geometry::{
    CoordSeq, Coordinates as _, GeometryErrorKind, LineSeq, MultiLineString, MultiMember,
    MultiPoint, Polygon, RepairMethod, Ring, Shape, XY, polygon_parts_to_shape, same_point,
};

pub(crate) fn line_repair(line: &LineSeq) -> Result<Shape> {
    if let Some(point) = collapsed_line_point(line) {
        return Ok(Shape::Point(point));
    }
    if validate_line(line, "line string").is_none() {
        return Ok(Shape::LineString(line.clone()));
    }
    Err(GeometryErrorKind::UnrepairableLineString.into())
}

pub(crate) fn multiline_repair(lines: &MultiLineString) -> Result<Shape> {
    let mut points = Vec::new();
    let mut line_members = Vec::new();
    for member in lines.members_with_values() {
        let MultiMember::Value(line) = member else {
            continue;
        };
        match line_repair(line)? {
            Shape::Point(point) => points.push(point),
            Shape::LineString(line) => line_members.push(line),
            Shape::MultiPoint(_)
            | Shape::MultiLineString(_)
            | Shape::Polygon(_)
            | Shape::MultiPolygon(_)
            | Shape::GeometryCollection(_)
            | Shape::Empty(..) => unreachable!("line repair only returns points or lines"),
        }
    }

    let axes = lines.axes();
    let point_shape = match points.as_slice() {
        [] => None,
        [point] => Some(Shape::Point(*point)),
        _ => Some(Shape::MultiPoint(MultiPoint::try_new(
            axes,
            points.into_iter().map(MultiMember::Value).collect(),
        )?)),
    };
    let line_shape = match line_members.as_slice() {
        [] => None,
        [line] => Some(Shape::LineString(line.clone())),
        _ => Some(Shape::MultiLineString(MultiLineString::try_new(
            axes,
            line_members.into_iter().map(MultiMember::Value).collect(),
        )?)),
    };

    match (point_shape, line_shape) {
        (Some(points), Some(lines)) => Shape::geometry_collection(vec![points, lines]),
        (Some(points), None) => Ok(points),
        (None, Some(lines)) => Ok(lines),
        (None, None) => Err(GeometryErrorKind::UnrepairableMultiLineString.into()),
    }
}

///
/// `Linework` is the original stored ring's directed winding modulo two on the
/// exact noded partition, so opposite occurrences cancel and duplicate
/// occurrences retain their multiplicity. `Structure` is each ring's
/// nonzero-signed-winding enclosed area. Both methods recombine selected
/// regions as union(shells) − union(holes); collapsed rings contribute their
/// closed line support to the exact source fold before polygon repair.
pub(crate) fn polygonal_repair(
    polygons: &[Polygon],
    method: RepairMethod,
    preserve_source_vertices: bool,
) -> Result<Shape> {
    let empty: &[Polygon] = &[];
    let inventory = ExactSourceInventory::from_polygon_operands(polygons, empty)?;
    let mut budget = crate::geometry::ExpansionBudget::new("repair", "exact partition");
    let mut partition = inventory.partition(&mut budget)?;
    if preserve_source_vertices {
        partition.preserve_source_vertices();
    }
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
    Ok(polygon_parts_to_shape(cleaned))
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
            let previous_index = if index == 0 {
                len.wrapping_sub(1)
            } else {
                index.wrapping_sub(1)
            };
            let next_index = if index.wrapping_add(1) == len {
                0
            } else {
                index.wrapping_add(1)
            };
            let previous = XY::new(
                *xs.get(previous_index)
                    .expect("ring columns share the admitted length"),
                *ys.get(previous_index)
                    .expect("ring columns share the admitted length"),
            );
            let next = XY::new(
                *xs.get(next_index)
                    .expect("ring columns share the admitted length"),
                *ys.get(next_index)
                    .expect("ring columns share the admitted length"),
            );
            !same_point(previous, next)
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
            let previous_index = if index == 0 {
                len.wrapping_sub(1)
            } else {
                index.wrapping_sub(1)
            };
            let next_index = if index.wrapping_add(1) == len {
                0
            } else {
                index.wrapping_add(1)
            };
            let prev = *points
                .get(previous_index)
                .expect("ring vertex ordinal is within the open ring");
            let next = *points
                .get(next_index)
                .expect("ring vertex ordinal is within the open ring");
            if same_point(prev, next) {
                let duplicate = next_index;
                if duplicate > index {
                    points.remove(duplicate);
                    points.remove(index);
                } else {
                    points.remove(index);
                    points.remove(duplicate);
                }
                removed = true;
            } else {
                index = index.wrapping_add(1);
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
