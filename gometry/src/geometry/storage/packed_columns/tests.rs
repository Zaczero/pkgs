
use super::*;
use crate::PyGeometryArray;
use crate::crs::frame::{Frame, GeometryTransformFrame, crs_arc_static};
use crate::geographic::AngularFactor;
use crate::geometry::storage::{RowSelection, StorageLayout};
use crate::geometry::{LineSeq, Polygon, Ring, Shape, SharedSlice};
use crate::test_support::allocation_count;

fn coords() -> CoordSeq {
    CoordSeq::try_from_vecs(
        vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0],
        vec![0.0, 1.0, 0.0, 3.0, 4.0, 3.0],
        None,
        None,
    )
    .expect("valid coordinates")
}

fn packed_owner(
    storage: GeometryArrayStorage,
    missing: Option<ValidityMask>,
    layout: StorageLayout,
) -> ArrayRows {
    ArrayRows::admit(Arc::new(storage), missing, layout).expect("test packed storage is admitted")
}

fn geographic_array(shapes: Vec<Shape>) -> PyGeometryArray {
    let frame = Frame::new(Some(crs_arc_static("EPSG:4326")), None)
        .expect("EPSG:4326 is a valid geographic frame");
    let array = PyGeometryArray::from_shapes(shapes, frame).unwrap();
    assert!(
        matches!(array.storage(), GeometryArrayStorage::Packed(_)),
        "homogeneous rows use packed storage",
    );
    array
}

fn ordinary_geographic(array: &PyGeometryArray, frame: GeographicFrame) -> bool {
    array
        .packed_columns()
        .expect("packed array resolves to column descriptors")
        .ordinary_geographic_predicates(frame)
}

fn geographic_ring(points: &[(f64, f64)]) -> Ring {
    Ring::closed(
        points
            .iter()
            .map(|&(x, y)| Point::new_raw_xy(x, y))
            .collect(),
    )
    .expect("test ring is closed")
}

fn geographic_polygon(west: f64, north: f64) -> Shape {
    Shape::Polygon(Polygon::new(
        geographic_ring(&[
            (west, 30.0),
            (west + 1.0, 30.0),
            (west + 1.0, north),
            (west, north),
            (west, 30.0),
        ]),
        Vec::new(),
    ))
}

fn gathered_lines(rows: usize) -> GeometryArrayStorage {
    GeometryArrayStorage::Packed(PackedStorage::Lines {
        coords: Arc::new(coords()),
        offsets: CsrOffsetColumn::try_new(vec![0, 3, 6], 6).expect("valid offsets"),
        row_map: RowSelection::gather_checked_native(
            (0..rows).map(|row| row & 1).collect::<Vec<_>>(),
            2,
        )
        .unwrap(),
    })
}

fn gather_map(rows: Vec<usize>, physical_len: usize) -> RowSelection {
    RowSelection::gather_checked_native(rows, physical_len).unwrap()
}

mod cursor;
mod geographic;
mod map;
mod replacement;
mod transform;
