use pyo3::prelude::*;

use super::*;
use crate::crs::frame::crs_arc_static;
use crate::dispatch::{CachelessRowContext, Lane, MetricResolver, MetricScratch, kernels};
use crate::error::ErrorKind;
use crate::geometry::{MOrdinate, Point, PreparedPlan, ZOrdinate};

const SEAM: &str = "POLYGON M ((170 10 10,190 50 110,190 55 210,170 15 310,170 10 10))";
const ORDINARY: &str = "POLYGON M ((0 0 401,3 0 402,3 2 403,0 2 404,0 0 401))";

fn frame() -> Frame {
    Frame::new(Some(crs_arc_static("OGC:CRS84")), Some(2020.0)).unwrap()
}

fn pending(wkt: &str, logical: usize, frame: &Frame) -> PendingPublicationRow {
    let mut scratch = MetricScratch::default();
    let (domain, metric) = MetricResolver::GeographicCarry
        .resolve_topology_ctx(frame, &mut scratch)
        .unwrap();
    let data = PreparedPlan::new(crate::io::parse_wkt(wkt).unwrap());
    let context = CachelessRowContext {
        frame,
        domain,
        metric,
        lane: Lane::Array(logical),
    };
    let GeometryOutput::Pending(pending) = kernels::unary_boundary_array(&data, &context).unwrap()
    else {
        panic!("geographic boundary arrays require a pending publication row");
    };
    pending
}

fn mixed_rows() -> (PublicationRows, Vec<Option<Shape>>, Frame) {
    let frame = frame();
    let mut rows = PublicationRows::with_capacity(4);
    let mut expected = Vec::new();
    for (logical, source) in [Some(SEAM), None, Some(ORDINARY), Some(SEAM)]
        .into_iter()
        .enumerate()
    {
        if let Some(source) = source {
            let pending = pending(source, logical, &frame);
            assert_eq!(pending.is_producer_drawing(), logical == 2);
            expected.push(Some(pending.shape.clone()));
            rows.try_push(GeometryOutput::Pending(pending)).unwrap();
        } else {
            expected.push(None);
            rows.try_push_missing().unwrap();
        }
    }
    (rows, expected, frame)
}

fn replace_writer(rows: &mut PublicationRows, shapes: Vec<Option<Shape>>) {
    let mut writer = StreamingShapes::with_capacity(0, shapes.len());
    for shape in shapes {
        if let Some(shape) = shape {
            writer.try_push(shape).unwrap();
        } else {
            writer.try_push_missing().unwrap();
        }
    }
    rows.writer = writer;
}

fn assert_invalid(result: Result<crate::PyGeometryArray>) {
    let Err(error) = result else {
        panic!("final boundary storage accepted a corrupted publication");
    };
    assert!(matches!(
        error.kind(),
        ErrorKind::Geometry(GeometryErrorKind::Invalid(_))
    ));
}

fn unique_checked_index(records: &[CheckedPublicationLiteral], logical: usize) -> usize {
    records
        .iter()
        .position(|literal| {
            literal.address.logical_row == logical
                && records
                    .iter()
                    .filter(|other| {
                        other.address == literal.address && other.image == literal.image
                    })
                    .count()
                    == 1
        })
        .expect("a boundary has a non-closing unique image")
}

#[test]
fn mixed_boundary_rows_preserve_q_and_drawing_expectations() {
    let (rows, expected, frame) = mixed_rows();
    let output = rows.finish(frame.clone()).unwrap();
    assert_eq!(output.frame, frame);
    assert!(output.is_row_missing(1));
    for (logical, expected) in expected.into_iter().enumerate() {
        if let Some(expected) = expected {
            output
                .geometry_at(logical)
                .map(|actual| assert_eq!(actual.prepared_plan().shape(), &expected))
                .unwrap();
        }
    }
}

#[test]
fn boundary_final_writer_rejects_m_corruption_in_both_receipt_inventories() {
    for logical in [0, 2] {
        let (mut rows, mut expected, frame) = mixed_rows();
        let shape = expected
            .get_mut(logical)
            .expect("the selected logical row is present")
            .as_mut()
            .unwrap();
        let changed = std::cell::Cell::new(false);
        *shape = shape
            .map_points(&|point| {
                let m = point.m().unwrap();
                let m = if changed.replace(true) {
                    m
                } else {
                    f64::from_bits(m.to_bits() ^ 1)
                };
                Ok(Point::new_raw_axes(
                    point.x,
                    point.y,
                    ZOrdinate(point.z()),
                    MOrdinate(Some(m)),
                ))
            })
            .unwrap();
        assert!(changed.get());
        replace_writer(&mut rows, expected);
        assert_invalid(rows.finish(frame));
    }
}

#[test]
fn boundary_final_writer_rejects_unique_literal_omission_in_both_inventories() {
    for logical in [0, 2] {
        let (mut rows, _expected, frame) = mixed_rows();
        let unique = unique_checked_index(&rows.records, logical);
        rows.records.remove(unique);
        assert_invalid(rows.finish(frame));
    }
}

#[test]
fn boundary_final_writer_rejects_changed_logical_identity_in_both_inventories() {
    for logical in [0, 2] {
        let (mut rows, _expected, frame) = mixed_rows();
        let unique = unique_checked_index(&rows.records, logical);
        rows.records
            .get_mut(unique)
            .expect("the selected unique image exists")
            .address
            .logical_row = 3;
        assert_invalid(rows.finish(frame));
    }
}

#[test]
fn boundary_final_writer_rejects_same_sized_row_swaps() {
    let frame = frame();
    let first = pending(ORDINARY, 0, &frame);
    let second = pending(
        "POLYGON M ((0 0 501,3 0 502,3 2 503,0 2 504,0 0 501))",
        1,
        &frame,
    );
    let swapped = vec![Some(second.shape.clone()), Some(first.shape.clone())];
    let mut rows = PublicationRows::with_capacity(2);
    rows.try_push(GeometryOutput::Pending(first)).unwrap();
    rows.try_push(GeometryOutput::Pending(second)).unwrap();
    replace_writer(&mut rows, swapped);
    assert_invalid(rows.finish(frame));
}

#[test]
fn boundary_final_writer_rejects_a_structurally_valid_shortened_line() {
    let frame = frame();
    let pending = pending(ORDINARY, 0, &frame);
    let Shape::LineString(line) = &pending.shape else {
        panic!("one ordinary polygon yields one boundary line");
    };
    let points = line
        .points()
        .enumerate()
        .filter_map(|(index, point)| (index != 1).then_some(point))
        .collect::<Vec<_>>();
    let shortened = Shape::LineString(
        crate::geometry::LineSeq::try_new(
            crate::geometry::CoordSeq::try_from_points(&points).unwrap(),
        )
        .unwrap(),
    );
    let mut rows = PublicationRows::with_capacity(1);
    rows.try_push(GeometryOutput::Pending(pending)).unwrap();
    replace_writer(&mut rows, vec![Some(shortened)]);
    assert_invalid(rows.finish(frame));
}

#[test]
fn public_boundary_dispatch_preserves_nullable_mixed_rows_and_epoch() {
    crate::test_support::initialize_python();
    Python::attach(|py| {
        let frame = frame();
        let first = crate::io::parse_wkt(SEAM).unwrap();
        let ordinary = crate::io::parse_wkt(ORDINARY).unwrap();
        let present = crate::PyGeometryArray::from_shapes(
            vec![first.clone(), ordinary, first],
            frame.clone(),
        )
        .unwrap();
        let array = crate::PyGeometryArray::scatter_present_rows(
            &present,
            ValidityMask::from_vec(4, vec![false, true, false, false]).unwrap(),
        )
        .unwrap();
        let object = Bound::new(py, array).unwrap();
        let output = object.call_method0("boundary").unwrap();
        let output = output
            .extract::<PyRef<'_, crate::PyGeometryArray>>()
            .unwrap();
        assert_eq!(output.frame, frame);
        assert!(output.is_row_missing(1));
        assert_eq!(output.len(), 4);
        assert_eq!(
            output.geometry_at(0).unwrap().prepared_plan().shape(),
            output.geometry_at(3).unwrap().prepared_plan().shape(),
        );
    });
}

#[test]
fn boundary_checked_append_preserves_the_remaining_q_account() {
    let frame = frame();
    let mut pending = pending(SEAM, 0, &frame);
    let PendingAdmission::Checked { work, .. } = &mut pending.admission else {
        panic!("a seam boundary retains authentic Q admission");
    };
    *work =
        EvaluationBudget::with_remaining_ticks_for_test("boundary", work.constructed(), 0).unwrap();
    let mut rows = PublicationRows::with_capacity(1);
    let Err(error) = rows.try_push(GeometryOutput::Pending(pending)) else {
        panic!("checked append reset an exhausted Q account");
    };
    assert!(matches!(
        error.kind(),
        ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit { .. }),
    ));
}

#[test]
fn boundary_final_readback_consumes_the_same_remaining_q_account() {
    let frame = frame();
    let mut pending = pending(SEAM, 0, &frame);
    let coordinates = ShapeCountQuery::default()
        .coordinates(&pending.shape)
        .unwrap();
    let PendingAdmission::Checked { work, records, .. } = &mut pending.admission else {
        panic!("a seam boundary retains authentic Q admission");
    };
    let append = records.len() as u128 + 1;
    let admission = coordinates as u128 + 3;
    *work = EvaluationBudget::with_remaining_ticks_for_test(
        "boundary",
        work.constructed(),
        append + admission,
    )
    .unwrap();
    let mut rows = PublicationRows::with_capacity(1);
    rows.try_push(GeometryOutput::Pending(pending)).unwrap();
    let Err(error) = rows.finish(frame) else {
        panic!("final readback reset or erased the remaining Q account");
    };
    assert!(matches!(
        error.kind(),
        ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit { .. }),
    ));
}
