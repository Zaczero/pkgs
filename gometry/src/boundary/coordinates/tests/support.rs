use std::sync::Arc;

use crate::array::RingLevel;
use crate::geometry::{
    CoordSeq, CsrOffsetColumn, LineSeq, Point, PolygonLevel, Shape, ShapeData, storage,
};
use crate::{Frame, PyGeometryArray};

pub(super) fn pt(x: f64, y: f64) -> Point {
    Point::new(x, y).unwrap()
}

pub(super) fn line(coords: &[(f64, f64)]) -> Arc<ShapeData> {
    Arc::new(ShapeData::new(Shape::LineString(
        LineSeq::try_new(
            CoordSeq::try_from_points(&coords.iter().map(|&(x, y)| pt(x, y)).collect::<Vec<_>>())
                .unwrap(),
        )
        .expect("test line is valid"),
    )))
}

pub(super) fn closed_box(left: f64, bottom: f64, right: f64, top: f64) -> [Point; 5] {
    [
        pt(left, bottom),
        pt(right, bottom),
        pt(right, top),
        pt(left, top),
        pt(left, bottom),
    ]
}

pub(super) fn packed_polygons(
    coords: CoordSeq,
    ring_ends: Vec<usize>,
    polygon_ends: Vec<usize>,
    rows: std::ops::Range<usize>,
) -> PyGeometryArray {
    let n_rings = ring_ends
        .len()
        .checked_sub(1)
        .expect("ring offsets include their initial zero");
    let n_polygons = polygon_ends
        .len()
        .checked_sub(1)
        .expect("polygon offsets include their initial zero");
    let ring_offsets = CsrOffsetColumn::<RingLevel>::try_new(ring_ends, coords.len()).unwrap();
    let polygon_offsets = CsrOffsetColumn::<PolygonLevel>::try_new(polygon_ends, n_rings).unwrap();
    PyGeometryArray::packed_polygons_mapped(
        coords,
        ring_offsets,
        polygon_offsets,
        Frame::default(),
        storage::row_selection_from_logical_rows(
            storage::RowSelectionRef::Identity,
            n_polygons,
            rows,
        ),
    )
    .unwrap()
}

pub(super) fn concat3(a: &[Point; 5], b: &[Point; 5], c: &[Point; 5]) -> [Point; 15] {
    let mut points = [pt(0.0, 0.0); 15];
    points[..5].copy_from_slice(a);
    points[5..10].copy_from_slice(b);
    points[10..].copy_from_slice(c);
    points
}
