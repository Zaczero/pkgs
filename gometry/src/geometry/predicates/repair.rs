use super::validate::{collapsed_line_point, validate_line};
use crate::error::Result;
use crate::geometry::overlay::ExactSourceInventory;
use crate::geometry::{
    GeometryErrorKind, LineSeq, MultiLineString, MultiMember, MultiPoint, Polygon, RepairMethod,
    Shape, SourceArcContext, polygon_parts_to_shape,
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
    strict: bool,
    context: &SourceArcContext<'_, '_>,
) -> Result<Shape> {
    let empty: &[Polygon] = &[];
    let inventory = ExactSourceInventory::from_polygon_operands(polygons, empty)?;
    let mut budget = crate::geometry::ExpansionBudget::new("repair", "exact partition");
    let mut partition = inventory.partition(&mut budget)?;
    if preserve_source_vertices {
        partition.preserve_source_vertices();
    }
    let declared_axes = polygons
        .iter()
        .fold(crate::geometry::CoordinateAxes::XY, |axes, polygon| {
            axes.union(polygon.axes())
        });
    let parts = inventory.repair_polygons(&partition, method, declared_axes, strict, context)?;
    let mut polygons = Vec::with_capacity(parts.len());
    for part in parts {
        let Shape::Polygon(polygon) = part else {
            unreachable!("repair exact source emission yields polygon parts")
        };
        polygons.push(polygon);
    }
    Ok(polygon_parts_to_shape(polygons))
}

pub(in crate::geometry) fn remove_antenna_vertices<T: Copy>(
    mut points: Vec<T>,
    equal: impl Fn(T, T) -> bool,
) -> Option<Vec<T>> {
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
            let previous = *points
                .get(previous_index)
                .expect("ring vertex ordinal is within the open ring");
            let next = *points
                .get(next_index)
                .expect("ring vertex ordinal is within the open ring");
            if equal(previous, next) {
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
            return Some(points);
        }
    }
}
