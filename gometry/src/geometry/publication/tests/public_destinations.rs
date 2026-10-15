use pyo3::prelude::*;
use pyo3::types::{PyList, PyTuple};

use crate::Frame;
use crate::crs::frame::crs_arc_static;
use crate::dispatch::MetricResolver;
use crate::dispatch::kernels::binary_overlay;
use crate::geometry::storage::ValidityMask;
use crate::geometry::{
    CoordinateAxes, Coordinates as _, GeographicTopologyRoute, GeometryKind, OverlayOp, Point,
    Polygon, Ring, Shape, Strictness, geographic_topology_route,
};

#[test]
fn geographic_nearest_point_columns_preserve_all_broadcasts_missing_empty_m_and_epoch() {
    crate::test_support::initialize_python();
    let frame = Frame::new(Some(crs_arc_static("OGC:CRS84")), Some(2021.25)).unwrap();
    let point = |x, y, m| {
        Shape::Point(Point::new_raw_axes(
            x,
            y,
            crate::geometry::ZOrdinate(None),
            crate::geometry::MOrdinate(Some(m)),
        ))
    };
    let first = point(1.0, 2.0, 11.0);
    let last = point(7.0, 8.0, 77.0);
    let fixed = point(-1.0, -2.0, -11.0);
    let empty = Shape::Empty(GeometryKind::Point, CoordinateAxes::XYM);
    let left = crate::PyGeometryArray::from_shapes(
        vec![first.clone(), empty.clone(), empty.clone(), last.clone()],
        frame.clone(),
    )
    .unwrap()
    .with_missing_mask(ValidityMask::from_vec(4, vec![false, false, true, false]))
    .unwrap();
    let right_first = point(3.0, 4.0, 33.0);
    let right_last = point(11.0, 12.0, 111.0);
    let right = crate::PyGeometryArray::from_shapes(
        vec![
            right_first.clone(),
            point(5.0, 6.0, 55.0),
            point(9.0, 10.0, 99.0),
            right_last.clone(),
        ],
        frame.clone(),
    )
    .unwrap();
    Python::attach(|py| {
        let left = Bound::new(py, left).unwrap().into_any();
        let right = Bound::new(py, right).unwrap().into_any();
        let fixed_object = Bound::new(
            py,
            crate::PyGeometry::with_frame(fixed.clone(), frame.clone()),
        )
        .unwrap()
        .into_any();
        for (a, b, expected_first, expected_last) in [
            (&left, &right, [first.clone(), right_first], [
                last.clone(),
                right_last,
            ]),
            (&fixed_object, &left, [fixed.clone(), first.clone()], [
                fixed.clone(),
                last.clone(),
            ]),
            (&left, &fixed_object, [first, fixed.clone()], [last, fixed]),
        ] {
            let output = crate::predicates::nearest_points(py, a, b, None).unwrap();
            let columns = output.bind(py).cast::<PyTuple>().unwrap();
            for (column, (first, last)) in expected_first.into_iter().zip(expected_last).enumerate()
            {
                let value = columns.get_item(column).unwrap();
                let array = value
                    .extract::<PyRef<'_, crate::PyGeometryArray>>()
                    .unwrap();
                assert_eq!(array.frame, frame);
                assert_eq!(array.len(), 4);
                assert!(array.is_row_missing(2));
                for (row, expected) in [(0, first), (1, empty.clone()), (3, last)] {
                    assert_eq!(
                        array.geometry_at(row).unwrap().prepared_plan().shape(),
                        &expected
                    );
                }
            }
        }
    });
}

#[test]
fn geographic_polygonize_public_destinations_preserve_group_domains_m_and_epoch() {
    use crate::py::functions::polygonize_all;
    crate::test_support::initialize_python();
    let frame = Frame::new(Some(crs_arc_static("OGC:CRS84")), Some(2022.5)).unwrap();
    let source = crate::io::parse_wkt("LINESTRING M (0 0 11,3 0 22,3 2 33,0 2 44,0 0 11)").unwrap();
    let expected = source.points_vec().unwrap();
    let empty = Shape::Empty(GeometryKind::LineString, CoordinateAxes::XYM);
    let array = crate::PyGeometryArray::from_shapes(
        vec![source.clone(), empty.clone(), empty, source.clone()],
        frame.clone(),
    )
    .unwrap()
    .with_missing_mask(ValidityMask::from_vec(4, vec![false, true, false, false]))
    .unwrap();
    Python::attach(|py| {
        let assert_polygons = |output: &crate::PyGeometryArray, count| {
            assert_eq!(output.frame, frame);
            assert_eq!(output.len(), count);
            for row in 0..count {
                let geometry = output.geometry_at(row).unwrap();
                let plan = geometry.prepared_plan();
                let Shape::Polygon(polygon) = plan.shape() else {
                    panic!("one closed source line publishes one polygon per output row");
                };
                assert!(polygon.holes.is_empty());
                let actual = polygon.shell.coords().iter_coords().collect::<Vec<_>>();
                assert_eq!(actual.len(), 5);
                assert!(expected.iter().all(|point| actual.contains(point)));
                assert!(actual.iter().all(|point| expected.contains(point)));
            }
        };
        let scalar = Bound::new(py, crate::PyGeometry::with_frame(source, frame.clone())).unwrap();
        let result = scalar.call_method0("polygonize").unwrap();
        assert_polygons(
            &result
                .extract::<PyRef<'_, crate::PyGeometryArray>>()
                .unwrap(),
            1,
        );
        let inputs = PyList::new(py, [&scalar]).unwrap().into_any();
        let pooled = polygonize_all(py, &inputs, false).unwrap();
        assert_polygons(
            &pooled
                .bind(py)
                .extract::<PyRef<'_, crate::PyGeometryArray>>()
                .unwrap(),
            1,
        );

        let array = Bound::new(py, array).unwrap();
        let groups = array.call_method0("polygonize").unwrap();
        assert_eq!(
            groups
                .getattr("offsets")
                .unwrap()
                .extract::<Vec<i64>>()
                .unwrap(),
            [0, 1, 1, 1, 2]
        );
        assert_eq!(
            groups
                .getattr("is_missing")
                .unwrap()
                .extract::<Vec<bool>>()
                .unwrap(),
            [false, true, false, false]
        );
        let values = groups.getattr("values").unwrap();
        assert_polygons(
            &values
                .extract::<PyRef<'_, crate::PyGeometryArray>>()
                .unwrap(),
            2,
        );
    });
}

#[test]
fn geographic_dissolve_publishes_q_rows_through_final_array_storage() {
    crate::test_support::initialize_python();
    let frame = Frame::new(Some(crs_arc_static("OGC:CRS84")), None).unwrap();
    let polygon = |points: &[(f64, f64)]| {
        Shape::Polygon(Polygon::new(
            Ring::closed(
                points
                    .iter()
                    .map(|&(x, y)| Point::new_raw_xy(x, y))
                    .collect(),
            )
            .unwrap(),
            Vec::new(),
        ))
    };
    let shapes = vec![
        polygon(&[
            (170.0, 0.0),
            (-170.0, 0.0),
            (-170.0, 20.0),
            (170.0, 20.0),
            (170.0, 0.0),
        ]),
        polygon(&[
            (175.0, 10.0),
            (-175.0, 10.0),
            (-175.0, 30.0),
            (175.0, 30.0),
            (175.0, 10.0),
        ]),
    ];
    let inputs = crate::geographic::GeographicInputs::try_from_shapes(
        crate::geographic::GeographicFrame::crs84(),
        shapes.iter(),
    )
    .unwrap();
    assert_eq!(
        geographic_topology_route(inputs.views().iter().copied()),
        GeographicTopologyRoute::Quotient
    );
    let array = crate::PyGeometryArray::from_shapes(shapes, frame).unwrap();

    Python::attach(|py| {
        let array = Py::new(py, array).unwrap();
        let keys = PyList::new(py, [0, 0]).unwrap();
        let result = array.bind(py).call_method1("dissolve", (&keys,)).unwrap();
        let result = result.cast::<PyTuple>().unwrap();
        let geometries = result.get_item(0).unwrap();
        let keys = result.get_item(1).unwrap();
        let wkts = geometries
            .call_method0("to_wkt")
            .unwrap()
            .extract::<Vec<String>>()
            .unwrap();
        assert_eq!(wkts.len(), 1);
        let wkt = wkts.first().expect("one dissolved geometry");
        assert_eq!(keys.extract::<Vec<i64>>().unwrap(), [0]);
        assert!(
            wkt.starts_with("POLYGON") || wkt.starts_with("MULTIPOLYGON"),
            "dissolve should publish one nonempty areal row: {wkt}"
        );
    });
}

#[test]
fn binary_geographic_array_overlay_binds_after_final_array_admission() {
    crate::test_support::initialize_python();
    let frame = Frame::new(Some(crs_arc_static("OGC:CRS84")), None).unwrap();
    let polygon = |points: &[(f64, f64)]| {
        Shape::Polygon(Polygon::new(
            Ring::closed(
                points
                    .iter()
                    .map(|&(x, y)| Point::new_raw_xy(x, y))
                    .collect(),
            )
            .unwrap(),
            Vec::new(),
        ))
    };
    let left = polygon(&[
        (360.0, 0.0),
        (361.0, 0.0),
        (361.0, 1.0),
        (360.0, 1.0),
        (360.0, 0.0),
    ]);
    let right = polygon(&[(0.5, 0.0), (1.5, 0.0), (1.5, 1.0), (0.5, 1.0), (0.5, 0.0)]);
    let geographic_inputs = crate::geographic::GeographicInputs::try_from_shapes(
        crate::geographic::GeographicFrame::crs84(),
        [&left, &right].into_iter(),
    )
    .unwrap();
    assert_eq!(
        geographic_topology_route(geographic_inputs.views().iter().copied()),
        GeographicTopologyRoute::Quotient
    );
    let left = crate::PyGeometryArray::from_shapes(vec![left], frame.clone()).unwrap();
    let right = crate::PyGeometryArray::from_shapes(vec![right], frame).unwrap();

    Python::attach(|py| {
        let left = Bound::new(py, left).unwrap().into_any();
        let right = Bound::new(py, right).unwrap().into_any();
        let output = crate::dispatch::dispatch_binary_geometry(
            py,
            &left,
            &right,
            "intersection",
            MetricResolver::GeographicCarry,
            false,
            |left, right, context| {
                binary_overlay(
                    left,
                    right,
                    context,
                    OverlayOp::Intersection,
                    Strictness::Lenient,
                )
            },
        )
        .unwrap();
        let wkts = output
            .bind(py)
            .call_method0("to_wkt")
            .unwrap()
            .extract::<Vec<String>>()
            .unwrap();
        assert_eq!(wkts.len(), 1);
        let wkt = wkts.first().expect("one array result geometry");
        assert!(wkt.starts_with("POLYGON"));
        assert!(!wkt.contains("EMPTY"));
    });
}
