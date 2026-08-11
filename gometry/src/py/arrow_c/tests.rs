use crate::PyGeometry;
use crate::geometry::{CoordSeq, LineSeq, Point, Polygon, Ring, Shape};
use crate::py::arrow_c::capsule_lifecycle::stream_from_export;
use crate::py::arrow_c::*;

fn release_twice(export: ExportedArray) {
    let mut schema = export.schema;
    let mut array = export.array.into_box();
    // SAFETY: this test owns both boxed C structs and is explicitly
    // exercising idempotent release callbacks before the boxes drop.
    unsafe {
        release_schema(schema.as_mut());
        release_schema(schema.as_mut());
        release_array(array.as_mut());
        release_array(array.as_mut());
    }
}

fn release_schema_twice(mut schema: Box<ArrowSchema>) {
    // SAFETY: this test owns the boxed C schema and is explicitly
    // exercising idempotent release callbacks before the shell drops.
    unsafe {
        release_schema(schema.as_mut());
        release_schema(schema.as_mut());
    }
}

#[test]
fn release_callbacks_are_idempotent_for_native_exports() {
    let point = PyGeometry::from_shape_crs(Shape::Point(Point::new_unchecked_xy(1.0, 2.0)), None);
    release_twice(export_from_geometries([&point], None, None).unwrap());

    let line = PyGeometry::from_shape_crs(
        Shape::LineString(
            LineSeq::try_new(CoordSeq::from_points(&[
                Point::new_unchecked_xy(0.0, 0.0),
                Point::new_unchecked_xy(1.0, 1.0),
            ]))
            .expect("test line is valid"),
        ),
        None,
    );
    release_twice(export_from_geometries([&line], None, None).unwrap());

    let ring = Ring::closed(vec![
        Point::new_unchecked_xy(0.0, 0.0),
        Point::new_unchecked_xy(1.0, 0.0),
        Point::new_unchecked_xy(1.0, 1.0),
        Point::new_unchecked_xy(0.0, 0.0),
    ])
    .unwrap();
    let polygon = PyGeometry::from_shape_crs(Shape::Polygon(Polygon::new(ring, Vec::new())), None);
    release_twice(export_from_geometries([&polygon], None, None).unwrap());
}

#[test]
fn schema_only_export_releases_without_array_export() {
    let point = PyGeometry::from_shape_crs(Shape::Point(Point::new_unchecked_xy(1.0, 2.0)), None);
    release_schema_twice(schema_from_geometries([&point], None, None).unwrap());

    let collection = PyGeometry::from_shape_crs(Shape::GeometryCollection(Vec::new()), None);
    release_schema_twice(schema_from_geometries([&collection], None, None).unwrap());
}

#[test]
fn stream_export_release_is_idempotent_after_batch_move() {
    let point = PyGeometry::from_shape_crs(Shape::Point(Point::new_unchecked_xy(1.0, 2.0)), None);
    let mut stream = stream_from_export(export_from_geometries([&point], None, None).unwrap());
    let mut array = empty_array();

    // SAFETY: this test owns the stream and moves its one batch into a
    // stack ArrowArray, then releases both sides through their callbacks.
    unsafe {
        assert_eq!(stream_get_next(stream.as_mut(), &raw mut array), 0);
        assert!(array.release.is_some());
        release_array(&raw mut array);
        release_stream(stream.as_mut());
        release_stream(stream.as_mut());
    }
}

// Compile-fail fixtures for arrow_c / frozen-i64 are driven by the crate-root
// `compile_fail_gate` (stable error codes + production dep-info).
