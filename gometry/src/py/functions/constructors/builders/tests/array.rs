use std::ffi::CStr;

use pyo3::prelude::*;
use rstest::rstest;

use crate::GeometryKind;
use crate::py::errors::InvalidGeometryError;
use crate::py::functions::constructors::build_polygon;
use crate::py::functions::constructors::builders::build_geometry_array;

#[rstest]
#[case::line(
    GeometryKind::LineString,
    c"__import__('numpy').array([[(1,2,3,4), (5,6,7,8)]], dtype='float64')",
    "LINESTRING ZM (1 2 3 4, 5 6 7 8)"
)]
#[case::points(
    GeometryKind::MultiPoint,
    c"__import__('numpy').array([[(1,2,3,4), (5,6,7,8)]], dtype='float64')",
    "MULTIPOINT ZM ((1 2 3 4), (5 6 7 8))"
)]
#[case::polygon(
    GeometryKind::Polygon,
    c"__import__('numpy').array([[(1,2,3,4), (5,6,7,8), (9,10,11,12)]], dtype='float64')",
    "POLYGON ZM ((1 2 3 4, 5 6 7 8, 9 10 11 12, 1 2 3 4))"
)]
#[case::lines(
    GeometryKind::MultiLineString,
    c"__import__('numpy').array([[[(1,2,3,4), (5,6,7,8)]]], dtype='float64')",
    "MULTILINESTRING ZM ((1 2 3 4, 5 6 7 8))"
)]
#[case::polygons(
    GeometryKind::MultiPolygon,
    c"__import__('numpy').array([[[[(1,2,3,4), (5,6,7,8), (9,10,11,12)]]]], dtype='float64')",
    "MULTIPOLYGON ZM (((1 2 3 4, 5 6 7 8, 9 10 11 12, 1 2 3 4)))"
)]
#[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
fn coordinate_factories_own_xyzm_rows(
    #[case] kind: GeometryKind,
    #[case] expression: &CStr,
    #[case] expected: &str,
    #[values(false, true)] iterable: bool,
) {
    crate::test_support::initialize_python();
    let array = Python::attach(|py| {
        let values = py.eval(expression, None, None).expect("coordinate fixture");
        let values = if iterable {
            values.call_method0("tolist").expect("iterable fixture")
        } else {
            values
        };
        build_geometry_array(&values, None, None, kind).expect("construct rows")
    });
    let shape = array
        .row_operation()
        .present_rows()
        .next()
        .expect("one present row")
        .row()
        .to_shape()
        .expect("materialize owned row");
    assert_eq!(array.len(), 1);
    assert_eq!(
        shape,
        crate::io::parse_wkt(expected).expect("expected shape")
    );
}

#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
fn polygon_shell_error_precedes_holes_provider_error() {
    crate::test_support::initialize_python();
    Python::attach(|py| {
        let shell = py
            .eval(c"[(1, 2), (3, 4)]", None, None)
            .expect("short shell");
        let holes = py
            .eval(c"(1 / x for x in [0])", None, None)
            .expect("holes provider");
        let error = build_polygon(
            py,
            Some(&shell),
            Some(&holes),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .expect_err("short shell is rejected before advancing holes");
        assert!(error.is_instance_of::<InvalidGeometryError>(py));
        assert!(error.to_string().contains("polygon ring"));
    });
}
