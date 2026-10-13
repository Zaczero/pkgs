use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::{PyInt, PyList, PyTuple};

use super::receipt::ProducerSourceRootUse;
use crate::Frame;
use crate::array::StreamingShapes;
use crate::crs::frame::crs_arc_static;
use crate::dispatch::MetricResolver;
use crate::dispatch::kernels::binary_overlay;
use crate::geometry::embedding_completion::{
    FrozenLiteralBuffers, LiteralAddress, LiteralPoint, LiteralRowLayout, ShapeLiteralLayout,
};
use crate::geometry::ordinates::{RootId, RootRange};
use crate::geometry::publication::{
    ChartCutSide, ChartRestriction, DirectedChartRestrictionUse, ProducerDrawingReceipt,
    ProducerImageId, ProducerLiteral, ProducerReceipt, PublicationCall, PublicationOutcome,
    PublicationRejection, StorageTarget, SyntheticChartVertex,
};
use crate::geometry::{
    CoordinateAxes, GeographicTopologyRoute, OverlayOp, Point, Polygon, Ring, Shape, Strictness,
    Unmetered, geographic_topology_route,
};

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

fn producer_point_receipt(
    target: StorageTarget,
    stored: Point,
    expected: Point,
) -> ProducerDrawingReceipt {
    let image = ProducerImageId::new(0);
    let address = LiteralAddress {
        operation: 0,
        logical_row: 0,
        group_row: (target == StorageTarget::Groups).then_some(0),
        value_row: (target == StorageTarget::Groups).then_some(0),
        output: 0,
    };
    let mut work = Unmetered;
    let buffers = match target {
        StorageTarget::Scalar => FrozenLiteralBuffers::shapes(
            Shape::Point(stored),
            point_literal_layout(image, 0, None, None, stored).layout,
        ),
        StorageTarget::Array => {
            let mut writer = StreamingShapes::with_capacity(0, 1);
            writer.try_push(Shape::Point(stored)).unwrap();
            let (storage, missing, layout) = writer.finish_parts().unwrap();
            FrozenLiteralBuffers::rows(
                Arc::new(storage),
                missing,
                layout,
                vec![Some(point_literal_layout(image, 0, None, None, stored))],
                &mut work,
            )
            .unwrap()
        },
        StorageTarget::Groups => {
            let mut writer = StreamingShapes::with_capacity(0, 1);
            writer.try_push(Shape::Point(stored)).unwrap();
            let (storage, value_missing, layout) = writer.finish_parts().unwrap();
            FrozenLiteralBuffers::groups(
                Arc::new(storage),
                value_missing,
                layout,
                vec![Some(point_literal_layout(
                    image,
                    0,
                    Some(0),
                    Some(0),
                    stored,
                ))],
                vec![0, 1],
                None,
                &mut work,
            )
            .unwrap()
        },
    };
    ProducerDrawingReceipt::new(
        buffers,
        vec![ProducerLiteral::from_point(address, image, expected)],
        vec![ProducerSourceRootUse {
            address,
            image,
            range: RootRange::whole(RootId::from_index(0)),
        }],
        vec![DirectedChartRestrictionUse {
            restriction: ChartRestriction::CutChord,
            start: SyntheticChartVertex::Cut {
                node: 0,
                side: ChartCutSide::East,
            },
            end: SyntheticChartVertex::Cut {
                node: 1,
                side: ChartCutSide::West,
            },
        }],
    )
}

fn finish_producer_point(
    target: StorageTarget,
    stored: Point,
    expected: Point,
) -> PublicationOutcome {
    let frame = Frame::default();
    let mut work = Unmetered;
    super::finish::finish_geographic(
        PublicationCall::new(
            ProducerReceipt::ProducerDrawing(producer_point_receipt(target, stored, expected)),
            frame,
            &mut work,
        ),
        target,
    )
    .unwrap()
}

#[test]
fn producer_drawing_binds_scalar_array_and_groups_storage() {
    let point = Point::new_raw_xy(4.0, -2.0);
    for target in [
        StorageTarget::Scalar,
        StorageTarget::Array,
        StorageTarget::Groups,
    ] {
        let PublicationOutcome::Pass(finished) = finish_producer_point(target, point, point) else {
            panic!("matching producer drawing must bind its final storage");
        };
        match target {
            StorageTarget::Scalar => {
                let (Shape::Point(actual), _) = finished.into_scalar().unwrap() else {
                    panic!("scalar producer drawing retains its point");
                };
                assert_eq!(actual, point);
            },
            StorageTarget::Array => {
                let (rows, _) = finished.into_array().unwrap();
                assert_eq!(rows.len(), 1);
            },
            StorageTarget::Groups => {
                let (groups, _) = finished.into_groups().unwrap();
                assert_eq!(groups.values.len(), 1);
                assert_eq!(groups.offsets, [0, 1]);
            },
        }
    }
}

#[test]
fn producer_drawing_rejects_final_storage_readback_mismatch() {
    let expected = Point::new_raw_xy(4.0, -2.0);
    let corrupted = Point::new_raw_xy(40.0, -2.0);
    match finish_producer_point(StorageTarget::Array, corrupted, expected) {
        PublicationOutcome::Reject(PublicationRejection::ProducerDrawing(images)) => {
            assert_eq!(images, [ProducerImageId::new(0)]);
        },
        PublicationOutcome::Pass(finished) => {
            let (rows, _) = finished.into_array().unwrap();
            let actual = match rows.logical_row(0) {
                Some(crate::geometry::storage::LogicalRow::Present { row, .. }) => {
                    row.point().expect("one point literal is present")
                },
                _ => panic!("the producer-drawn final row remains present"),
            };
            assert_eq!(
                actual, expected,
                "omitting the producer-drawn binder must not publish changed coordinates"
            );
        },
        PublicationOutcome::Reject(_) => {
            panic!("producer-drawn storage returns its own rejection family")
        },
    }
}

#[test]
fn producer_drawing_rejects_omitted_literal_at_final_storage_readback() -> crate::error::Result<()>
{
    let point = Point::new_raw_xy(4.0, -2.0);
    let mut receipt = producer_point_receipt(StorageTarget::Array, point, point);
    receipt.literals.clear();
    receipt.source_root_uses.clear();
    let mut work = Unmetered;
    match super::finish::finish_geographic(
        PublicationCall::new(
            ProducerReceipt::ProducerDrawing(receipt),
            Frame::default(),
            &mut work,
        ),
        StorageTarget::Array,
    )? {
        PublicationOutcome::Reject(PublicationRejection::ProducerDrawing(images)) => {
            if images != [ProducerImageId::new(0)] {
                return Err(crate::geometry::GeometryErrorKind::Invalid(
                    "omitted producer literal returned an unexpected image identity".into(),
                )
                .into());
            }
        },
        PublicationOutcome::Pass(_) => {
            return Err(crate::geometry::GeometryErrorKind::Invalid(
                "final array readback accepted a producer receipt with its literal omitted".into(),
            )
            .into());
        },
        PublicationOutcome::Reject(_) => {
            return Err(crate::geometry::GeometryErrorKind::Invalid(
                "omitted producer literals returned another rejection family".into(),
            )
            .into());
        },
    }
    Ok(())
}

#[test]
fn geographic_scalar_sampling_publishes_prepared_draws_through_the_sink() {
    crate::test_support::initialize_python();
    let frame = Frame::new(Some(crs_arc_static("OGC:CRS84")), None).unwrap();
    let geometry = crate::PyGeometry::with_frame(Shape::Point(Point::new_raw_xy(4.0, -2.0)), frame);

    Python::attach(|py| {
        let count = PyInt::new(py, 3).into_any();
        let seed = PyInt::new(py, 0x1234).into_any();
        let output = geometry
            .sample_points_impl(py, &count, &seed)
            .expect("geographic sampling publishes its prepared draw receipt");
        let output = Py::new(py, output).unwrap();
        let wkts = output
            .bind(py)
            .call_method0("to_wkt")
            .unwrap()
            .extract::<Vec<String>>()
            .unwrap();
        assert_eq!(wkts, ["POINT (4 -2)"; 3]);
    });
}

#[test]
fn geographic_array_sampling_publishes_final_groups_storage() {
    crate::test_support::initialize_python();
    let frame = Frame::new(Some(crs_arc_static("OGC:CRS84")), None).unwrap();
    let array = crate::PyGeometryArray::from_shapes(
        vec![
            Shape::Point(Point::new_raw_xy(4.0, -2.0)),
            Shape::Point(Point::new_raw_xy(8.0, 3.0)),
        ],
        frame,
    )
    .unwrap();

    Python::attach(|py| {
        let count = PyInt::new(py, 2).into_any();
        let seed = PyInt::new(py, 0x1234).into_any();
        let groups = array
            .sample_points_impl(&count, &seed)
            .expect("geographic array sampling publishes its prepared group receipts");
        let groups = Py::new(py, groups).unwrap();
        let groups = groups.bind(py);
        assert_eq!(
            groups
                .getattr("offsets")
                .unwrap()
                .extract::<Vec<i64>>()
                .unwrap(),
            [0, 2, 4]
        );
        let values = groups.getattr("values").unwrap();
        let wkts = values
            .call_method0("to_wkt")
            .unwrap()
            .extract::<Vec<String>>()
            .unwrap();
        assert_eq!(wkts.len(), 4);
        assert!(wkts.iter().all(|wkt| wkt.starts_with("POINT")));
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
