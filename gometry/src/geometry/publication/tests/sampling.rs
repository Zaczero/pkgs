use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::{PyInt, PySlice};

use super::super::receipt::{
    ProducerDrawingReceipt, ProducerImageId, ProducerLiteral, ProducerReceipt, PublicationCall,
    PublicationOutcome, PublicationRejection, StorageTarget,
};
use super::point_literal_layout;
use crate::Frame;
use crate::array::StreamingShapes;
use crate::crs::frame::crs_arc_static;
use crate::geometry::embedding_completion::literals::LiteralAddress;
use crate::geometry::embedding_completion::storage::FrozenLiteralBuffers;
use crate::geometry::storage::ValidityMask;
use crate::geometry::{Point, Shape, TopologyDomain};
use crate::work::Unmetered;

fn sampling_array_receipt(stored: &[Point], expected: &[Point]) -> ProducerDrawingReceipt {
    assert_eq!(stored.len(), expected.len());
    let mut writer = StreamingShapes::with_capacity(0, stored.len());
    let mut row_map = Vec::with_capacity(stored.len());
    let mut literals = Vec::with_capacity(stored.len());
    for (logical_row, (&stored, &expected)) in stored.iter().zip(expected).enumerate() {
        let image = ProducerImageId::new(logical_row);
        let address = LiteralAddress {
            operation: 0,
            logical_row,
            group_row: None,
            value_row: None,
            output: 0,
        };
        writer.try_push(Shape::Point(stored)).unwrap();
        row_map.push(Some(point_literal_layout(
            image,
            logical_row,
            None,
            None,
            stored,
        )));
        literals.push(ProducerLiteral::from_point(address, image, expected));
    }
    let (storage, missing, layout) = writer.finish_parts().unwrap();
    let mut work = Unmetered;
    let buffers =
        FrozenLiteralBuffers::rows(Arc::new(storage), missing, layout, row_map, &mut work).unwrap();
    ProducerDrawingReceipt::new(buffers, literals)
}

fn selected_sampling_draws() -> Vec<Point> {
    let geographic = crate::geographic::GeographicFrame::crs84();
    [
        "LINESTRING (0 0, 12 4)",
        "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))",
        "MULTIPOINT ((7 8), (7 8), (7 8))",
    ]
    .into_iter()
    .flat_map(|wkt| {
        crate::io::parse_wkt(wkt)
            .unwrap()
            .sample_point_receipts_in_domain(2, 1, 0, TopologyDomain::Geographic(geographic))
            .unwrap()
            .draws
            .into_iter()
            .map(|draw| draw.point)
    })
    .collect()
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
fn sampling_array_rejects_a_draw_without_its_selected_support() {
    let mut prepared = crate::io::parse_wkt("LINESTRING (0 0, 2 0)")
        .unwrap()
        .sample_point_receipts_in_domain(1, 1, 0, TopologyDomain::Cartesian)
        .unwrap();
    prepared.draws.first_mut().unwrap().selected_element = prepared.support_elements.len();

    match crate::geometry::publish_sampling_array(prepared, Frame::default()) {
        Err(_) => {},
        Ok(_) => {
            panic!(
                "sampling must not publish a direct point write after its selected-support receipt is invalid"
            );
        },
    }
}

#[test]
fn sampling_groups_reject_a_draw_owned_by_another_logical_row() {
    let prepared = crate::io::parse_wkt("LINESTRING (0 0, 2 0)")
        .unwrap()
        .sample_point_receipts_in_domain(1, 1, 1, TopologyDomain::Cartesian)
        .unwrap();

    match crate::geometry::publish_sampling_groups(vec![Some(prepared)], Frame::default(), None) {
        Err(_) => {},
        Ok(_) => {
            panic!(
                "Groups must not publish a direct row write with another row's sampling receipt"
            );
        },
    }
}

#[test]
fn sampling_array_readback_rejects_an_omitted_line_area_or_atom_literal() {
    let expected = selected_sampling_draws();
    assert_eq!(expected.len(), 6);
    assert_eq!(
        expected.get(4),
        expected.get(5),
        "the point atoms are repeated"
    );
    let mut receipt = sampling_array_receipt(&expected, &expected);
    let omitted = receipt.literals.remove(2);
    let mut work = Unmetered;
    let frame = Frame::new(Some(crs_arc_static("OGC:CRS84")), None).unwrap();

    match super::super::finish::finish_geographic(
        PublicationCall::new(ProducerReceipt::ProducerDrawing(receipt), frame, &mut work),
        StorageTarget::Array,
    )
    .unwrap()
    {
        PublicationOutcome::Reject(PublicationRejection::ProducerDrawing(images)) => {
            assert_eq!(images, [omitted.image]);
        },
        PublicationOutcome::Pass(_) => {
            panic!("a missing sampling output literal must fail final array readback");
        },
        PublicationOutcome::Reject(_) => {
            panic!("sampling literal omission returns the producer-drawing rejection")
        },
    }
}

#[test]
fn sampling_array_readback_rejects_a_corrupted_area_draw() {
    let expected = selected_sampling_draws();
    let mut stored = expected.clone();
    let [_, _, corrupted_area_draw, ..] = expected.as_slice() else {
        panic!("the receipt fixture includes a selected area draw");
    };
    let Some(corrupted_slot) = stored.get_mut(2) else {
        panic!("the stored receipt includes the selected area draw");
    };
    *corrupted_slot = Point::new_raw_xy(corrupted_area_draw.x + 0.25, corrupted_area_draw.y);
    let receipt = sampling_array_receipt(&stored, &expected);
    let mut work = Unmetered;
    let frame = Frame::new(Some(crs_arc_static("OGC:CRS84")), None).unwrap();

    match super::super::finish::finish_geographic(
        PublicationCall::new(ProducerReceipt::ProducerDrawing(receipt), frame, &mut work),
        StorageTarget::Array,
    )
    .unwrap()
    {
        PublicationOutcome::Reject(PublicationRejection::ProducerDrawing(images)) => {
            assert_eq!(images, [ProducerImageId::new(2)]);
        },
        PublicationOutcome::Pass(_) => {
            panic!("a corrupted sampling area draw must fail final array readback");
        },
        PublicationOutcome::Reject(_) => {
            panic!("sampling literal corruption returns the producer-drawing rejection")
        },
    }
}

#[test]
fn geographic_array_sampling_preserves_line_area_atom_and_gathered_group_rows() {
    crate::test_support::initialize_python();
    let frame = Frame::new(Some(crs_arc_static("OGC:CRS84")), None).unwrap();
    let shapes = vec![
        crate::io::parse_wkt("LINESTRING (0 0, 12 4)").unwrap(),
        crate::io::parse_wkt("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))").unwrap(),
        crate::io::parse_wkt("MULTIPOINT ((7 8), (7 8), (7 8))").unwrap(),
        crate::io::parse_wkt("POINT EMPTY").unwrap(),
        crate::io::parse_wkt("POINT (20 30)").unwrap(),
        crate::io::parse_wkt("LINESTRING (-4 2, 5 7)").unwrap(),
    ];
    let array = crate::PyGeometryArray::from_shapes(shapes.clone(), frame.clone())
        .unwrap()
        .with_missing_mask(Some(
            ValidityMask::from_vec(shapes.len(), vec![false, false, false, false, true, false])
                .unwrap(),
        ))
        .unwrap();
    let seed = 0x1234;
    let geographic = crate::geographic::GeographicFrame::crs84();
    let expected_rows = shapes
        .iter()
        .enumerate()
        .map(|(logical_row, shape)| {
            if logical_row == 4 {
                return Vec::new();
            }
            shape
                .sample_point_receipts_in_domain(
                    2,
                    crate::geometry::row_sample_seed(seed, logical_row),
                    logical_row,
                    TopologyDomain::Geographic(geographic),
                )
                .unwrap()
                .draws
                .into_iter()
                .map(|draw| draw.point)
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let expected_points = expected_rows.iter().flatten().copied().collect::<Vec<_>>();
    assert_eq!(expected_points.len(), 8);
    let [_, _, atom_row, ..] = expected_rows.as_slice() else {
        panic!("the fixture contains a repeated-atom row");
    };
    let [first_atom, second_atom] = atom_row.as_slice() else {
        panic!("the atom row contains both sampled occurrences");
    };
    assert_eq!(
        first_atom, second_atom,
        "repeated point atoms remain separate draws"
    );
    let gathered_points = [0, 2]
        .into_iter()
        .filter_map(|row| expected_rows.get(row))
        .flatten()
        .copied()
        .map(Shape::Point)
        .collect::<Vec<_>>();

    Python::attach(|py| {
        let count = PyInt::new(py, 2).into_any();
        let seed = PyInt::new(py, seed).into_any();
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
            [0, 2, 4, 6, 6, 6, 8]
        );
        assert_eq!(
            groups
                .getattr("is_missing")
                .unwrap()
                .extract::<Vec<bool>>()
                .unwrap(),
            [false, false, false, false, true, false]
        );
        let values = groups.getattr("values").unwrap();
        let actual = values
            .call_method0("to_wkt")
            .unwrap()
            .extract::<Vec<String>>()
            .unwrap();
        let expected = crate::PyGeometryArray::from_shapes(
            expected_points.iter().copied().map(Shape::Point).collect(),
            frame.clone(),
        )
        .unwrap();
        let expected = Py::new(py, expected).unwrap();
        assert_eq!(
            actual,
            expected
                .bind(py)
                .call_method0("to_wkt")
                .unwrap()
                .extract::<Vec<String>>()
                .unwrap()
        );
        assert_eq!(
            groups
                .get_item(PyInt::new(py, 3))
                .unwrap()
                .call_method0("__len__")
                .unwrap()
                .extract::<usize>()
                .unwrap(),
            0,
            "present empty rows remain distinct from missing groups"
        );
        assert!(
            groups.get_item(PyInt::new(py, 4)).unwrap().is_none(),
            "missing source rows remain null"
        );

        let gathered = groups.get_item(PySlice::new(py, 0, 6, 2)).unwrap();
        assert_eq!(
            gathered
                .getattr("offsets")
                .unwrap()
                .extract::<Vec<i64>>()
                .unwrap(),
            [0, 2, 4, 4]
        );
        assert_eq!(
            gathered
                .getattr("is_missing")
                .unwrap()
                .extract::<Vec<bool>>()
                .unwrap(),
            [false, false, true]
        );
        assert!(
            gathered.get_item(PyInt::new(py, 2)).unwrap().is_none(),
            "gathered missing groups retain their row identity"
        );
        let gathered_values = gathered.getattr("values").unwrap();
        let expected_gathered =
            crate::PyGeometryArray::from_shapes(gathered_points, frame.clone()).unwrap();
        let expected_gathered = Py::new(py, expected_gathered).unwrap();
        assert_eq!(
            gathered_values
                .call_method0("to_wkt")
                .unwrap()
                .extract::<Vec<String>>()
                .unwrap(),
            expected_gathered
                .bind(py)
                .call_method0("to_wkt")
                .unwrap()
                .extract::<Vec<String>>()
                .unwrap()
        );
    });
}
