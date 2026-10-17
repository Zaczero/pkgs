use super::super::receipt::ProducerDrawingReceipt;
use super::*;
use crate::crs::frame::crs_arc_static;
use crate::dispatch::kernels::unary_convex_hull_array;
use crate::geometry::embedding_completion::literals::{
    LiteralPoint, LiteralRowLayout, ShapeLiteralLayout, ShapeStructure,
};
use crate::geometry::topology::QSourceImage;
use crate::geometry::{CoordinateAxes, MOrdinate, ZOrdinate};

fn small_voronoi_writer() -> PublicationRows {
    use crate::geographic::GeographicFrame;
    use crate::geometry::{ExpansionBudget, VoronoiBoundary};
    let source = Shape::MultiPoint(
        vec![
            Point::new_raw_xy(-1.0, -1.0),
            Point::new_raw_xy(1.0, -1.0),
            Point::new_raw_xy(1.0, 1.0),
            Point::new_raw_xy(-1.0, 1.0),
        ]
        .into(),
    );
    let frame = GeographicFrame::crs84();
    let mut budget = ExpansionBudget::new("voronoi_cells", "mesh receipt omission");
    let outputs = source
        .voronoi_polygon_outputs_in_frame_budgeted(
            0.0,
            VoronoiBoundary::Padded,
            Some(frame),
            &mut budget,
        )
        .unwrap();
    let mut writer = PublicationRows::with_producer_drawing_capacity(0);
    writer
        .push_voronoi_polygon_outputs(outputs, frame, &mut budget)
        .unwrap();
    writer
}

#[test]
fn voronoi_receipt_omission_is_rejected_by_final_readback() {
    let (control_values, _) = small_voronoi_writer()
        .finish_geometry(Frame::None)
        .unwrap()
        .into_array()
        .unwrap();
    assert_eq!(control_values.len(), 4);
    let mut writer = small_voronoi_writer();
    writer.records.pop().expect("Voronoi output has literals");
    match writer.finish_geometry(Frame::None) {
        Err(error) => assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(GeometryErrorKind::Invalid(_))
        )),
        Ok(_) => panic!("the shared readback rejects a missing Voronoi literal"),
    }
}
fn xyzm(x: f64, y: f64, z: f64, m: f64) -> Point {
    Point::new_raw_axes(x, y, ZOrdinate(Some(z)), MOrdinate(Some(m)))
}

fn captured(
    pending: &mut PendingPublicationRow,
) -> (
    &mut LiteralRowLayout<PublicationImage>,
    &mut Vec<CheckedPublicationLiteral>,
) {
    let PendingAdmission::Checked {
        layout, records, ..
    } = &mut pending.admission
    else {
        panic!("the fixture owns an independently captured drawing");
    };
    (layout, records)
}

fn drawing_pending(shape: Shape, row: usize) -> PendingPublicationRow {
    let mut image = 0;
    let mut outputs = (0, 0, 0);
    let (layout, literals) =
        ProducerDrawingReceipt::drawing_layout(&shape, row, None, None, &mut image, &mut outputs)
            .unwrap();
    PendingPublicationRow::producer_drawing(shape, layout, literals, row, "domain test").unwrap()
}

#[test]
fn final_writer_rejects_present_to_missing() {
    let mut rows = PublicationRows::with_producer_drawing_capacity(1);
    rows.try_push(GeometryOutput::Pending(drawing_pending(
        Shape::Point(xyzm(2., 3., 4., 5.)),
        0,
    )))
    .unwrap();
    let mut writer = StreamingShapes::with_capacity(0, 1);
    writer.try_push_missing().unwrap();
    rows.writer = writer;
    let Err(error) = rows.finish(Frame::default()) else {
        panic!("the final writer dropped an accepted present row");
    };
    assert!(matches!(
        error.kind(),
        crate::error::ErrorKind::Geometry(GeometryErrorKind::Invalid(_))
    ));
}

#[test]
fn array_batch_preserves_missing_and_other_producer_row_domains() {
    for (expected, actual) in [
        (vec![], vec![]),
        (vec![false], vec![false]),
        (vec![true], vec![true]),
        (vec![false], vec![true]),
        (vec![true], vec![false]),
        (vec![true, false], vec![false, true]),
        (vec![true, false], vec![true]),
    ] {
        let correct = expected == actual;
        let mut domain = FlatLiteralLayout::with_capacity(expected.len());
        domain.rows.extend(
            expected
                .into_iter()
                .map(|present| present.then_some(FlatRowBinding::OtherProducer)),
        );
        let mut writer = StreamingShapes::with_capacity(0, actual.len());
        for present in actual {
            if present {
                writer.try_push(Shape::Point(xyzm(2., 3., 4., 5.))).unwrap();
            } else {
                writer.try_push_missing().unwrap();
            }
        }
        let (storage, missing, layout) = writer.finish_parts().unwrap();
        let batch = ArrayPublicationBatch {
            storage: Arc::new(storage),
            missing,
            layout,
            expected: domain,
            records: Vec::new(),
            accounts: Vec::new(),
            drawing_only: false,
        };
        let result = finish_array_batch(batch, Frame::default(), &mut Unmetered);
        if correct {
            assert!(matches!(result.unwrap(), PublicationOutcome::Pass(_)));
        } else {
            let Err(error) = result else {
                panic!("a writer changed a missing or other-producer row domain");
            };
            assert!(matches!(
                error.kind(),
                crate::error::ErrorKind::Geometry(GeometryErrorKind::Invalid(_))
            ));
        }
    }
}

#[test]
fn final_writer_rejects_typed_empty_substitution() {
    let mut rows = PublicationRows::with_producer_drawing_capacity(1);
    rows.try_push(GeometryOutput::Pending(drawing_pending(
        Shape::Empty(
            crate::geometry::GeometryKind::LineString,
            CoordinateAxes::XYZM,
        ),
        0,
    )))
    .unwrap();
    let mut writer = StreamingShapes::with_capacity(0, 1);
    writer
        .try_push(Shape::Empty(
            crate::geometry::GeometryKind::Point,
            CoordinateAxes::XY,
        ))
        .unwrap();
    rows.writer = writer;
    let Err(error) = rows.finish(Frame::default()) else {
        panic!("the final writer substituted typed-empty metadata");
    };
    assert!(matches!(
        error.kind(),
        crate::error::ErrorKind::Geometry(GeometryErrorKind::Invalid(_))
    ));
}

#[test]
fn final_writer_rejects_changed_logical_domains() {
    let point = Shape::Point(xyzm(2., 3., 4., 5.));
    let empty = Shape::Empty(
        crate::geometry::GeometryKind::LineString,
        CoordinateAxes::XYZM,
    );
    for (name, expected, actual) in [
        ("missing becomes present", vec![None], vec![Some(
            point.clone(),
        )]),
        (
            "same-count validity swap",
            vec![Some(point.clone()), None],
            vec![None, Some(point.clone())],
        ),
        (
            "trailing missing row deletion",
            vec![Some(point.clone()), None],
            vec![Some(point)],
        ),
        ("all-missing truncation", vec![None, None], vec![None]),
        ("zero-row insertion", vec![], vec![None]),
        ("present empty becomes missing", vec![Some(empty)], vec![
            None,
        ]),
    ] {
        let mut rows = PublicationRows::with_producer_drawing_capacity(expected.len());
        for (logical, shape) in expected.into_iter().enumerate() {
            if let Some(shape) = shape {
                rows.try_push(GeometryOutput::Pending(drawing_pending(shape, logical)))
                    .unwrap();
            } else {
                rows.try_push_missing().unwrap();
            }
        }
        let mut writer = StreamingShapes::with_capacity(0, actual.len());
        for shape in actual {
            if let Some(shape) = shape {
                writer.try_push(shape).unwrap();
            } else {
                writer.try_push_missing().unwrap();
            }
        }
        rows.writer = writer;
        let Err(error) = rows.finish(Frame::default()) else {
            panic!("{name}: changed final domain was published");
        };
        assert!(
            matches!(
                error.kind(),
                crate::error::ErrorKind::Geometry(GeometryErrorKind::Invalid(_))
            ),
            "{name}: {error:?}"
        );
    }
}

#[test]
fn final_writer_preserves_empty_and_missing_domains() {
    for expected in [
        vec![],
        vec![None, None],
        vec![Some(Shape::Empty(
            crate::geometry::GeometryKind::LineString,
            CoordinateAxes::XYZM,
        ))],
        vec![None, Some(Shape::Point(xyzm(2., 3., 4., 5.))), None],
    ] {
        let mut rows = PublicationRows::with_producer_drawing_capacity(expected.len());
        for (logical, shape) in expected.iter().cloned().enumerate() {
            if let Some(shape) = shape {
                rows.try_push(GeometryOutput::Pending(drawing_pending(shape, logical)))
                    .unwrap();
            } else {
                rows.try_push_missing().unwrap();
            }
        }
        let result = rows.finish(Frame::default()).unwrap();
        assert_eq!(result.len(), expected.len());
    }
}

#[test]
fn final_writer_rejects_changed_container_structure() {
    for (expected, actual) in [
        ("POINT (2 3)", "MULTIPOINT ((2 3))"),
        (
            "GEOMETRYCOLLECTION (GEOMETRYCOLLECTION (POINT (2 3)))",
            "GEOMETRYCOLLECTION (POINT (2 3))",
        ),
        ("MULTIPOINT (EMPTY, (2 3))", "MULTIPOINT ((2 3), EMPTY)"),
        ("LINESTRING ZM EMPTY", "LINESTRING EMPTY"),
        ("LINESTRING ZM EMPTY", "POINT ZM EMPTY"),
    ] {
        let mut rows = PublicationRows::with_producer_drawing_capacity(1);
        rows.try_push(GeometryOutput::Pending(drawing_pending(
            crate::io::parse_wkt(expected).unwrap(),
            0,
        )))
        .unwrap();
        let mut writer = StreamingShapes::with_capacity(0, 1);
        writer
            .try_push(crate::io::parse_wkt(actual).unwrap())
            .unwrap();
        rows.writer = writer;
        let Err(error) = rows.finish(Frame::default()) else {
            panic!("{expected} was replaced with {actual}");
        };
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(GeometryErrorKind::Invalid(_))
        ));
    }
}

#[test]
fn final_writer_accepts_authorized_typed_empty_normalization() {
    let mut pending = drawing_pending(
        Shape::Empty(crate::geometry::GeometryKind::Point, CoordinateAxes::XY),
        0,
    );
    pending
        .replace_empty_shape(Shape::Empty(
            crate::geometry::GeometryKind::LineString,
            CoordinateAxes::XYZM,
        ))
        .unwrap();
    let mut rows = PublicationRows::with_producer_drawing_capacity(1);
    rows.try_push(GeometryOutput::Pending(pending)).unwrap();
    assert_eq!(rows.finish(Frame::default()).unwrap().len(), 1);
}

#[test]
fn pending_append_preserves_captured_empty_structure() {
    let mut pending = drawing_pending(
        Shape::Empty(
            crate::geometry::GeometryKind::LineString,
            CoordinateAxes::XYZM,
        ),
        0,
    );
    pending.shape = Shape::Empty(crate::geometry::GeometryKind::Point, CoordinateAxes::XY);
    let mut rows = PublicationRows::with_producer_drawing_capacity(1);
    rows.try_push(GeometryOutput::Pending(pending)).unwrap();
    let Err(error) = rows.finish(Frame::default()) else {
        panic!("append recovered its expectation from substituted output geometry");
    };
    assert!(matches!(
        error.kind(),
        crate::error::ErrorKind::Geometry(GeometryErrorKind::Invalid(_))
    ));
}

#[test]
fn final_writer_rejects_xym_record_corruption_without_domain_changes() {
    let expected = Point::new_raw_axes(-0.0, 3.0, ZOrdinate(None), MOrdinate(Some(-0.0)));
    for actual in [
        Point::new_raw_axes(0.0, 3.0, ZOrdinate(None), MOrdinate(Some(-0.0))),
        Point::new_raw_axes(-0.0, 3.0, ZOrdinate(None), MOrdinate(Some(0.0))),
        Point::new_raw_axes(-0.0, 3.0, ZOrdinate(None), MOrdinate(Some(7.0))),
        Point::new_raw_xy(-0.0, 3.0),
    ] {
        let mut rows = PublicationRows::with_producer_drawing_capacity(1);
        rows.try_push(GeometryOutput::Pending(drawing_pending(
            Shape::Point(expected),
            0,
        )))
        .unwrap();
        let mut writer = StreamingShapes::with_capacity(0, 1);
        writer.try_push(Shape::Point(actual)).unwrap();
        rows.writer = writer;
        let Err(error) = rows.finish(Frame::default()) else {
            panic!("the final writer changed active XYM bits or axes");
        };
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(GeometryErrorKind::Invalid(_))
        ));
    }
}

#[test]
fn final_groups_reject_changed_empty_group_domains() {
    for (expected, actual) in [(false, true), (true, false)] {
        let mut rows = PublicationRows::with_groups_capacity(1);
        rows.end_group(expected).unwrap();
        let missing = ValidityMask::from_vec(1, vec![actual]);
        let Err(error) = rows.finish_groups(Frame::default(), missing) else {
            panic!("an empty group's presence changed");
        };
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(GeometryErrorKind::Invalid(_))
        ));
    }
    let mut rows = PublicationRows::with_groups_capacity(2);
    rows.end_group(false).unwrap();
    rows.end_group(false).unwrap();
    let Destination::DrawingGroups { offsets, .. } = &mut rows.destination else {
        panic!("the group builder owns its offsets");
    };
    offsets.pop();
    let Err(error) = rows.finish_groups(Frame::default(), None) else {
        panic!("a trailing empty group was deleted");
    };
    assert!(matches!(
        error.kind(),
        crate::error::ErrorKind::Geometry(GeometryErrorKind::Invalid(_))
    ));
}

#[test]
fn final_groups_preserve_empty_missing_and_typed_empty_values() {
    let mut rows = PublicationRows::with_groups_capacity(5);
    rows.end_group(false).unwrap();
    rows.end_group(true).unwrap();
    let mut pending = drawing_pending(
        Shape::Empty(
            crate::geometry::GeometryKind::LineString,
            CoordinateAxes::XYZM,
        ),
        2,
    );
    let (layout, _) = captured(&mut pending);
    layout.group_row = Some(2);
    layout.value_row = Some(0);
    rows.try_push(GeometryOutput::Pending(pending)).unwrap();
    rows.end_group(false).unwrap();
    rows.end_group(false).unwrap();
    rows.end_group(false).unwrap();
    let groups = rows
        .finish_groups(
            Frame::default(),
            ValidityMask::from_vec(5, vec![false, true, false, false, false]),
        )
        .unwrap();
    assert_eq!(groups.geometry_values_view().unwrap().unwrap().len(), 1);
}

#[test]
fn final_groups_reject_a_typed_empty_value_substitution() {
    let mut rows = PublicationRows::with_groups_capacity(1);
    let mut pending = drawing_pending(
        Shape::Empty(
            crate::geometry::GeometryKind::LineString,
            CoordinateAxes::XYZM,
        ),
        0,
    );
    let (layout, _) = captured(&mut pending);
    layout.group_row = Some(0);
    layout.value_row = Some(0);
    rows.try_push(GeometryOutput::Pending(pending)).unwrap();
    rows.end_group(false).unwrap();
    let mut writer = StreamingShapes::with_capacity(0, 1);
    writer
        .try_push(Shape::Empty(
            crate::geometry::GeometryKind::Point,
            CoordinateAxes::XY,
        ))
        .unwrap();
    rows.writer = writer;
    let Err(error) = rows.finish_groups(Frame::default(), None) else {
        panic!("the final Groups writer replaced an accepted typed-empty value");
    };
    assert!(matches!(
        error.kind(),
        crate::error::ErrorKind::Geometry(GeometryErrorKind::Invalid(_))
    ));
}

#[test]
fn final_groups_reject_shifted_value_boundaries() {
    let mut rows = PublicationRows::with_groups_capacity(2);
    rows.end_group(false).unwrap();
    let mut pending = drawing_pending(Shape::Point(xyzm(2., 3., 4., 5.)), 1);
    let (layout, records) = captured(&mut pending);
    layout.group_row = Some(1);
    layout.value_row = Some(0);
    for literal in records {
        literal.address.logical_row = 1;
        literal.address.group_row = Some(1);
        literal.address.value_row = Some(0);
    }
    rows.try_push(GeometryOutput::Pending(pending)).unwrap();
    rows.end_group(false).unwrap();
    let Destination::DrawingGroups { offsets, .. } = &mut rows.destination else {
        panic!("the group builder owns its offsets");
    };
    offsets[1] = 1;
    let Err(error) = rows.finish_groups(Frame::default(), None) else {
        panic!("a value moved into another group");
    };
    assert!(matches!(
        error.kind(),
        crate::error::ErrorKind::Geometry(GeometryErrorKind::Invalid(_))
    ));
}

fn array_batch(stored: Point, expected: Point) -> (ArrayPublicationBatch, Frame) {
    let frame = Frame::default();
    let image = PublicationImage::SourceOnly(QSourceImage::PublicationSample(0));
    let mut writer = StreamingShapes::with_capacity(0, 1);
    writer.try_push(Shape::Point(stored)).unwrap();
    let (storage, missing, layout) = writer.finish_parts().unwrap();
    let mut domain = FlatLiteralLayout::with_capacity(1);
    domain
        .push_layout(LiteralRowLayout {
            operation: 0,
            logical_row: 0,
            group_row: None,
            value_row: None,
            layout: ShapeLiteralLayout {
                structure: Some(ShapeStructure::capture(&Shape::Point(expected))),
                points: vec![LiteralPoint {
                    output: 0,
                    axes: CoordinateAxes::XYZM,
                    image,
                }],
                lines: Vec::new(),
                polygons: Vec::new(),
            },
        })
        .unwrap();
    let records = vec![CheckedPublicationLiteral {
        address: LiteralAddress {
            operation: 0,
            logical_row: 0,
            group_row: None,
            value_row: None,
            output: 0,
        },
        image,
        record: expected,
    }];
    (
        ArrayPublicationBatch {
            storage: Arc::new(storage),
            missing,
            layout,
            expected: domain,
            records,
            accounts: vec![(
                0,
                EvaluationBudget::new("array publication test", 0).unwrap(),
            )],
            drawing_only: false,
        },
        frame,
    )
}

fn geographic_hull_pending() -> (PendingPublicationRow, Frame) {
    let frame = Frame::new(Some(crs_arc_static("OGC:CRS84")), None).unwrap();
    let shape = crate::io::parse_wkt(
        "MULTIPOINT ZM ((0 0 0 0), (3 0 10 100), (2 2 20 200), (0 2 30 300), (1 1 40 400))",
    )
    .unwrap();
    let mut scratch = crate::dispatch::MetricScratch::default();
    let (domain, metric) = crate::dispatch::MetricResolver::GeographicCarry
        .resolve_topology_ctx(&frame, &mut scratch)
        .unwrap();
    let data = crate::geometry::PreparedPlan::new(shape);
    let context = crate::dispatch::CachelessRowContext {
        frame: &frame,
        domain,
        metric,
        lane: crate::dispatch::Lane::Array(0),
    };
    let GeometryOutput::Pending(pending) = unary_convex_hull_array(&data, &context).unwrap() else {
        panic!("geographic array hull captures a pending producer row");
    };
    (pending, frame)
}

#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
fn geographic_array_hull_final_writer_rejects_m_only_corruption() {
    let (pending, frame) = geographic_hull_pending();
    let expected = pending.shape.points_vec().unwrap();
    let changed = std::cell::Cell::new(false);
    let corrupted = pending
        .shape
        .map_points(&|point| {
            let m = point.m().expect("the hull retains its M ordinate");
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
    assert!(changed.get(), "the hull has at least one captured literal");
    let corrupted_points = corrupted.points_vec().unwrap();
    assert_eq!(expected.len(), corrupted_points.len());
    assert!(
        expected
            .iter()
            .zip(&corrupted_points)
            .all(|(before, after)| {
                before.x.to_bits() == after.x.to_bits()
                    && before.y.to_bits() == after.y.to_bits()
                    && before.z().map(f64::to_bits) == after.z().map(f64::to_bits)
            })
    );
    assert!(
        expected
            .iter()
            .zip(&corrupted_points)
            .any(|(before, after)| before.m() != after.m())
    );

    let mut rows = PublicationRows::with_producer_drawing_capacity(1);
    rows.try_push(GeometryOutput::Pending(pending)).unwrap();
    let mut writer = StreamingShapes::with_capacity(0, 1);
    writer.try_push(corrupted).unwrap();
    rows.writer = writer;

    assert!(
        rows.finish(frame).is_err(),
        "final ArrayRows readback must reject the writer's changed M"
    );
}

#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
fn geographic_array_hull_final_writer_rejects_an_omitted_literal() {
    let (pending, frame) = geographic_hull_pending();
    let mut rows = PublicationRows::with_producer_drawing_capacity(1);
    rows.try_push(GeometryOutput::Pending(pending)).unwrap();
    assert!(rows.records.pop().is_some());

    assert!(
        rows.finish(frame).is_err(),
        "final ArrayRows readback must reject a missing captured literal"
    );
}

#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
fn geographic_array_hull_final_writer_rejects_a_wrong_logical_identity() {
    let (pending, frame) = geographic_hull_pending();
    let mut rows = PublicationRows::with_producer_drawing_capacity(1);
    rows.try_push(GeometryOutput::Pending(pending)).unwrap();
    rows.records
        .first_mut()
        .expect("the hull contains a literal")
        .address
        .logical_row = 1;
    assert!(
        rows.finish(frame).is_err(),
        "final ArrayRows readback must match the captured logical row"
    );
}

fn finish_group_with_receipt_identity_error(
    corrupt_group_identity: bool,
) -> crate::error::Result<Groups> {
    let shape = Shape::Point(xyzm(1.0, 2.0, 3.0, 4.0));
    let (layout, literals) = ProducerDrawingReceipt::point_selection_layout(&shape)?;
    let mut pending =
        PendingPublicationRow::producer_drawing(shape, layout, literals, 0, "Groups test")?;
    let (layout, records) = captured(&mut pending);
    layout.group_row = Some(0);
    layout.value_row = Some(0);
    for literal in records.iter_mut() {
        literal.address.group_row = Some(0);
        literal.address.value_row = Some(0);
    }
    if corrupt_group_identity {
        records
            .first_mut()
            .expect("the point output has a literal")
            .address
            .group_row = Some(1);
    }

    let mut rows = PublicationRows::with_groups_capacity(1);
    rows.try_push(GeometryOutput::Pending(pending))?;
    rows.end_group(false)?;
    rows.finish_groups(Frame::default(), None)
}

#[test]
fn geographic_groups_final_writer_rejects_a_wrong_group_identity() {
    finish_group_with_receipt_identity_error(false)
        .expect("valid publication identity is accepted");
    finish_group_with_receipt_identity_error(true)
        .err()
        .expect("a wrong group identity is rejected");
}

#[test]
fn final_array_storage_m_corruption_is_rejected() {
    let expected = xyzm(1.0, 2.0, 3.0, 4.0);
    let (batch, frame) = array_batch(expected, expected);
    let mut work = Unmetered;
    assert!(matches!(
        finish_array_batch(batch, frame, &mut work).unwrap(),
        PublicationOutcome::Pass(_)
    ));

    let corrupted = xyzm(1.0, 2.0, 3.0, 40.0);
    let (batch, frame) = array_batch(corrupted, expected);
    let mut work = Unmetered;
    match finish_array_batch(batch, frame, &mut work).unwrap() {
        PublicationOutcome::Reject(PublicationRejection::Batch(images)) => {
            assert_eq!(images, [PublicationImage::SourceOnly(
                QSourceImage::PublicationSample(0)
            )]);
        },
        PublicationOutcome::Pass(finished) => {
            let (rows, _) = finished.into_array().expect("array output is admitted");
            let actual = match rows.logical_row(0) {
                Some(crate::geometry::storage::LogicalRow::Present { row, .. }) => {
                    row.point().expect("one point literal is present")
                },
                _ => panic!("the final writer retains its present point row"),
            };
            assert_eq!(
                actual, expected,
                "an omitted final-storage binder must not publish corrupted M"
            );
        },
        PublicationOutcome::Reject(
            PublicationRejection::Native
            | PublicationRejection::SourceOnly(_)
            | PublicationRejection::ProducerDrawing(_),
        ) => {
            panic!("array storage cannot return a Q candidate rejection")
        },
    }
}
