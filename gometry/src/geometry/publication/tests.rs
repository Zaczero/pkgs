use super::receipt::ProducerImageId;
use crate::geometry::embedding_completion::literals::{
    LiteralPoint, LiteralRowLayout, ShapeLiteralLayout, ShapeStructure,
};
use crate::geometry::{CoordinateAxes, Point, Shape};

fn point_literal_layout(
    image: ProducerImageId,
    logical_row: usize,
    group_row: Option<usize>,
    value_row: Option<usize>,
    point: Point,
) -> LiteralRowLayout<ProducerImageId> {
    LiteralRowLayout {
        operation: 0,
        logical_row,
        group_row,
        value_row,
        layout: ShapeLiteralLayout {
            structure: Some(ShapeStructure::capture(&Shape::Point(point))),
            points: vec![LiteralPoint {
                output: 0,
                axes: CoordinateAxes::from_point(point),
                image,
            }],
            lines: Vec::new(),
            polygons: Vec::new(),
        },
    }
}

mod allocations;
mod public_destinations;
mod readback;
mod sampling;
