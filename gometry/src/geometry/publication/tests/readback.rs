use std::sync::Arc;

use super::super::receipt::{
    ProducerDrawingReceipt, ProducerImageId, ProducerLiteral, ProducerReceipt, PublicationCall,
    PublicationOutcome, PublicationRejection, StorageTarget,
};
use super::super::routes::snap_drawing_receipt;
use super::point_literal_layout;
use crate::Frame;
use crate::array::StreamingShapes;
use crate::crs::frame::crs_arc_static;
use crate::dispatch::kernels::{unary_convex_hull, unary_minimum_rotated_rectangle};
use crate::geometry::embedding_completion::literals::{
    LiteralAddress, LiteralPolygon, LiteralRowLayout, LiteralSequence, ShapeLiteralLayout,
    ShapeStructure,
};
use crate::geometry::embedding_completion::storage::FrozenLiteralBuffers;
use crate::geometry::embedding_search::account::EvaluationBudget;
use crate::geometry::storage::ValidityMask;
use crate::geometry::{
    CoordinateAxes, Coordinates as _, GeometryKind, LineSeq, Point, Polygon, Ring, Shape,
    TopologyDomain,
};
use crate::work::Unmetered;

fn producer_line_sequence_layout(
    line: &LineSeq,
    logical_row: usize,
    output: usize,
    literals: &[ProducerLiteral],
    literal_cursor: &mut usize,
) -> crate::error::Result<LiteralSequence<ProducerImageId>> {
    let output = u32::try_from(output).map_err(|_| crate::error::ResourceError::SizeOverflow)?;
    let mut images = Vec::with_capacity(line.len());
    for point in line {
        let literal = literals.get(*literal_cursor).ok_or_else(|| {
            crate::geometry::GeometryErrorKind::Invalid(
                "producer line test has fewer literals than output coordinates".into(),
            )
        })?;
        let address = LiteralAddress {
            operation: 0,
            logical_row,
            group_row: None,
            value_row: None,
            output,
        };
        if literal.address != address || literal.record != point {
            return Err(crate::geometry::GeometryErrorKind::Invalid(
                "producer line test literal order differs from its row layout".into(),
            )
            .into());
        }
        images.push(literal.image);
        *literal_cursor = literal_cursor
            .checked_add(1)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
    }
    Ok(LiteralSequence {
        output,
        axes: line.as_coords().axes(),
        images,
    })
}

fn producer_line_shape_layout(
    shape: &Shape,
    logical_row: usize,
    literals: &[ProducerLiteral],
    literal_cursor: &mut usize,
) -> crate::error::Result<LiteralRowLayout<ProducerImageId>> {
    let lines = match shape {
        Shape::LineString(line) => vec![producer_line_sequence_layout(
            line,
            logical_row,
            0,
            literals,
            literal_cursor,
        )?],
        Shape::MultiLineString(lines) => lines
            .as_slice()
            .iter()
            .enumerate()
            .map(|(output, line)| {
                producer_line_sequence_layout(line, logical_row, output, literals, literal_cursor)
            })
            .collect::<crate::error::Result<Vec<_>>>()?,
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::Polygon(_)
        | Shape::MultiPolygon(_)
        | Shape::GeometryCollection(_)
        | Shape::Empty(
            GeometryKind::Point
            | GeometryKind::MultiPoint
            | GeometryKind::Polygon
            | GeometryKind::MultiPolygon
            | GeometryKind::GeometryCollection,
            _,
        ) => {
            return Err(crate::geometry::GeometryErrorKind::Invalid(
                "producer line test received a non-lineal output row".into(),
            )
            .into());
        },
        Shape::Empty(GeometryKind::LineString | GeometryKind::MultiLineString, _) => Vec::new(),
    };
    Ok(LiteralRowLayout {
        operation: 0,
        logical_row,
        group_row: None,
        value_row: None,
        layout: ShapeLiteralLayout {
            structure: Some(ShapeStructure::capture(shape)),
            points: Vec::new(),
            lines,
            polygons: Vec::new(),
        },
    })
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
    ProducerDrawingReceipt::new(buffers, vec![ProducerLiteral::from_point(
        address, image, expected,
    )])
}

fn finish_producer_point(
    target: StorageTarget,
    stored: Point,
    expected: Point,
) -> PublicationOutcome {
    let frame = Frame::default();
    let mut work = Unmetered;
    super::super::finish::finish_geographic(
        PublicationCall::new(
            ProducerReceipt::ProducerDrawing(producer_point_receipt(target, stored, expected)),
            frame,
            &mut work,
        ),
        target,
    )
    .unwrap()
}

fn producer_triangle_receipt(stored: &[Point; 4], expected: &[Point; 4]) -> ProducerDrawingReceipt {
    let images = (0..stored.len())
        .map(ProducerImageId::new)
        .collect::<Vec<_>>();
    let address = LiteralAddress {
        operation: 0,
        logical_row: 0,
        group_row: None,
        value_row: None,
        output: 0,
    };
    let mut writer = StreamingShapes::with_capacity(stored.len(), 1);
    writer
        .try_push(Shape::Polygon(Polygon::new(
            Ring::closed(stored.to_vec()).unwrap(),
            Vec::new(),
        )))
        .unwrap();
    let (storage, missing, layout) = writer.finish_parts().unwrap();
    let mut work = Unmetered;
    let buffers = FrozenLiteralBuffers::rows(
        Arc::new(storage),
        missing,
        layout,
        vec![Some(LiteralRowLayout {
            operation: 0,
            logical_row: 0,
            group_row: None,
            value_row: None,
            layout: ShapeLiteralLayout {
                structure: Some(ShapeStructure::primitive(
                    GeometryKind::Polygon,
                    CoordinateAxes::XY,
                    false,
                )),
                points: Vec::new(),
                lines: Vec::new(),
                polygons: vec![LiteralPolygon {
                    output: 0,
                    axes: CoordinateAxes::XY,
                    shell: LiteralSequence {
                        output: 0,
                        axes: CoordinateAxes::XY,
                        images: images.clone(),
                    },
                    holes: Vec::new(),
                }],
            },
        })],
        &mut work,
    )
    .unwrap();
    let literals = expected
        .iter()
        .copied()
        .zip(images.iter().copied())
        .map(|(point, image)| ProducerLiteral::from_point(address, image, point))
        .collect();
    ProducerDrawingReceipt::new(buffers, literals)
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
fn minimum_clearance_line_receipt_omission_fails_final_readback() {
    let frame = Frame::new(Some(crs_arc_static("OGC:CRS84")), None).unwrap();
    let shape = crate::io::parse_wkt("MULTIPOINT ((0 0), (1 0))").unwrap();
    let model = crate::crs::metric_model(Some("OGC:CRS84")).unwrap();
    let output =
        crate::broadcast::metric_minimum_clearance_line(&model, Some("OGC:CRS84"), &shape).unwrap();
    let mut receipt = ProducerDrawingReceipt::line(output).unwrap();
    let omitted = receipt.literals.remove(0);
    let mut work = Unmetered;

    match super::super::finish::finish_geographic(
        PublicationCall::new(ProducerReceipt::ProducerDrawing(receipt), frame, &mut work),
        StorageTarget::Scalar,
    )
    .unwrap()
    {
        PublicationOutcome::Reject(PublicationRejection::ProducerDrawing(images)) => {
            assert_eq!(images, [omitted.image]);
        },
        PublicationOutcome::Pass(_) => {
            panic!("minimum-clearance line literal omission must fail final scalar readback");
        },
        PublicationOutcome::Reject(_) => {
            panic!("minimum-clearance line omission returns producer-drawing rejection");
        },
    }
}

#[test]
fn nearest_points_receipt_omission_fails_final_readback() {
    let frame = Frame::new(Some(crs_arc_static("OGC:CRS84")), None).unwrap();
    let model = crate::crs::metric_model(Some("OGC:CRS84")).unwrap();
    let resolved = crate::crs::ResolvedMetric::from_model(&model).unwrap();
    let left = crate::geometry::PreparedPlan::new(Shape::Point(Point::new_raw_xy(0.0, 0.0)));
    let right = crate::geometry::PreparedPlan::new(Shape::Point(Point::new_raw_xy(1.0, 1.0)));
    let pair = crate::broadcast::metric_nearest_points_resolved(
        &resolved,
        &left,
        &crate::geometry::FrameDependentCaches::default(),
        &right,
        &crate::geometry::FrameDependentCaches::default(),
        None,
    )
    .unwrap()
    .expect("the points have a nearest pair");
    let (left, _) = crate::geometry::nearest_point_pair(Some(&pair), CoordinateAxes::XY);
    let mut receipt = ProducerDrawingReceipt::point(left).unwrap();
    let omitted = receipt.literals.remove(0);
    let mut work = Unmetered;

    match super::super::finish::finish_geographic(
        PublicationCall::new(ProducerReceipt::ProducerDrawing(receipt), frame, &mut work),
        StorageTarget::Scalar,
    )
    .unwrap()
    {
        PublicationOutcome::Reject(PublicationRejection::ProducerDrawing(images)) => {
            assert_eq!(images, [omitted.image]);
        },
        PublicationOutcome::Pass(_) => {
            panic!("nearest-point literal omission must fail final scalar readback");
        },
        PublicationOutcome::Reject(_) => {
            panic!("nearest-point omission returns producer-drawing rejection");
        },
    }
}

#[test]
fn nearest_point_columns_reject_a_shortened_missing_mask() {
    let missing = ValidityMask::from_vec(1, vec![true]);
    let result = crate::nearest_point_columns_masked(
        [
            Ok((
                Some((Point::new_raw_xy(1.0, 2.0), Point::new_raw_xy(3.0, 4.0))),
                CoordinateAxes::XY,
            )),
            Ok((None, CoordinateAxes::XY)),
        ]
        .into_iter(),
        missing.as_ref(),
        TopologyDomain::Geographic(crate::geographic::GeographicFrame::crs84()),
    );

    match result {
        Err((_, error)) => assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(crate::geometry::GeometryErrorKind::Invalid(_))
        )),
        Ok(_) => panic!("paired nearest-point validity must cover all logical rows"),
    }
}

#[test]
fn shortest_line_receipt_omission_fails_final_readback() {
    let frame = Frame::new(Some(crs_arc_static("OGC:CRS84")), None).unwrap();
    let model = crate::crs::metric_model(Some("OGC:CRS84")).unwrap();
    let resolved = crate::crs::ResolvedMetric::from_model(&model).unwrap();
    let left = crate::geometry::PreparedPlan::new(Shape::Point(Point::new_raw_xy(0.0, 0.0)));
    let right = crate::geometry::PreparedPlan::new(Shape::Point(Point::new_raw_xy(1.0, 1.0)));
    let source_context = crate::geometry::SourceArcContext::cartesian();
    let output = crate::broadcast::metric_shortest_line_resolved(
        &resolved,
        &left,
        &right,
        TopologyDomain::Geographic(crate::geographic::GeographicFrame::crs84()),
        &source_context,
    )
    .unwrap();
    let mut receipt = ProducerDrawingReceipt::line(output).unwrap();
    let omitted = receipt.literals.remove(0);
    let mut work = Unmetered;

    match super::super::finish::finish_geographic(
        PublicationCall::new(ProducerReceipt::ProducerDrawing(receipt), frame, &mut work),
        StorageTarget::Scalar,
    )
    .unwrap()
    {
        PublicationOutcome::Reject(PublicationRejection::ProducerDrawing(images)) => {
            assert_eq!(images, [omitted.image]);
        },
        PublicationOutcome::Pass(_) => {
            panic!("shortest-line literal omission must fail final scalar readback");
        },
        PublicationOutcome::Reject(_) => {
            panic!("shortest-line omission returns producer-drawing rejection");
        },
    }
}

#[test]
fn split_groups_receipt_omission_fails_final_readback() {
    let frame = Frame::new(Some(crs_arc_static("OGC:CRS84")), None).unwrap();
    let source = crate::io::parse_wkt("LINESTRING (0 0, 2 0)").unwrap();
    let splitter = crate::io::parse_wkt("POINT (1 0)").unwrap();
    let parts = source.split(&splitter, 0.0).unwrap();
    assert_eq!(parts.len(), 2);

    let mut writer = StreamingShapes::with_capacity(0, parts.len());
    let mut value_map = Vec::with_capacity(parts.len());
    let mut literals = Vec::new();
    let mut next_image = 0_usize;
    for (value_row, shape) in parts.into_iter().enumerate() {
        let (layout, mut row_literals) = ProducerDrawingReceipt::line_layout(
            &shape,
            0,
            Some(0),
            Some(value_row),
            0,
            &mut next_image,
        )
        .unwrap();
        writer.try_push(shape).unwrap();
        value_map.push(Some(LiteralRowLayout {
            operation: 0,
            logical_row: 0,
            group_row: Some(0),
            value_row: Some(value_row),
            layout,
        }));
        literals.append(&mut row_literals);
    }
    let (storage, value_missing, layout) = writer.finish_parts().unwrap();
    let mut work = Unmetered;
    let buffers = FrozenLiteralBuffers::groups(
        Arc::new(storage),
        value_missing,
        layout,
        value_map,
        vec![0, 2],
        None,
        &mut work,
    )
    .unwrap();
    let mut receipt = ProducerDrawingReceipt::new(buffers, literals);
    let omitted = receipt.literals.remove(0);

    match super::super::finish::finish_geographic(
        PublicationCall::new(ProducerReceipt::ProducerDrawing(receipt), frame, &mut work),
        StorageTarget::Groups,
    )
    .unwrap()
    {
        PublicationOutcome::Reject(PublicationRejection::ProducerDrawing(images)) => {
            assert_eq!(images, [omitted.image]);
        },
        PublicationOutcome::Pass(_) => {
            panic!("split group literal omission must fail final Groups readback");
        },
        PublicationOutcome::Reject(_) => {
            panic!("split omission returns producer-drawing rejection");
        },
    }
}

#[test]
fn convex_hull_receipt_omission_fails_final_readback() {
    let frame = Frame::new(Some(crs_arc_static("OGC:CRS84")), None).unwrap();
    let shape = crate::io::parse_wkt("MULTIPOINT ((0 0), (2 0), (1 2))").unwrap();
    let mut scratch = crate::dispatch::MetricScratch::default();
    let (domain, metric) = crate::dispatch::MetricResolver::GeographicCarry
        .resolve_topology_ctx(&frame, &mut scratch)
        .unwrap();
    let data = crate::geometry::PreparedPlan::new(shape);
    let context = crate::dispatch::CachelessRowContext {
        frame: &frame,
        domain,
        metric,
        lane: crate::dispatch::Lane::Scalar,
    };
    let output = unary_convex_hull(&data, &context).unwrap();
    let mut receipt = ProducerDrawingReceipt::point_selection(output).unwrap();
    let omitted = receipt.literals.remove(0);
    let mut work = Unmetered;

    match super::super::finish::finish_geographic(
        PublicationCall::new(ProducerReceipt::ProducerDrawing(receipt), frame, &mut work),
        StorageTarget::Scalar,
    )
    .unwrap()
    {
        PublicationOutcome::Reject(PublicationRejection::ProducerDrawing(images)) => {
            assert_eq!(images, [omitted.image]);
        },
        PublicationOutcome::Pass(_) => {
            panic!("convex-hull literal omission must fail final scalar readback");
        },
        PublicationOutcome::Reject(_) => {
            panic!("convex-hull omission returns producer-drawing rejection");
        },
    }
}

#[test]
fn degenerate_minimum_rectangle_receipt_omission_fails_final_readback() {
    let frame = Frame::new(Some(crs_arc_static("OGC:CRS84")), None).unwrap();
    let shape = crate::io::parse_wkt("LINESTRING (0 0, 1 1)").unwrap();
    let mut scratch = crate::dispatch::MetricScratch::default();
    let (domain, metric) = crate::dispatch::MetricResolver::GeographicCarry
        .resolve_topology_ctx(&frame, &mut scratch)
        .unwrap();
    let data = crate::geometry::PreparedPlan::new(shape);
    let context = crate::dispatch::CachelessRowContext {
        frame: &frame,
        domain,
        metric,
        lane: crate::dispatch::Lane::Scalar,
    };
    let output = unary_minimum_rotated_rectangle(&data, &context).unwrap();
    let mut receipt = ProducerDrawingReceipt::point_selection(output).unwrap();
    let omitted = receipt.literals.remove(0);
    let mut work = Unmetered;

    match super::super::finish::finish_geographic(
        PublicationCall::new(ProducerReceipt::ProducerDrawing(receipt), frame, &mut work),
        StorageTarget::Scalar,
    )
    .unwrap()
    {
        PublicationOutcome::Reject(PublicationRejection::ProducerDrawing(images)) => {
            assert_eq!(images, [omitted.image]);
        },
        PublicationOutcome::Pass(_) => {
            panic!("degenerate rectangle literal omission must fail final scalar readback");
        },
        PublicationOutcome::Reject(_) => {
            panic!("rectangle omission returns producer-drawing rejection");
        },
    }
}

#[test]
fn snap_receipt_omission_fails_final_readback() {
    let frame = Frame::new(Some(crs_arc_static("OGC:CRS84")), None).unwrap();
    let source = crate::io::parse_wkt("LINESTRING (0 0, 2 0)").unwrap();
    let reference = crate::io::parse_wkt("POINT (1 0.5)").unwrap();
    let geographic = crate::geographic::GeographicFrame::crs84();
    let context: crate::geometry::SourceArcContext<'_, '_, crate::crs::GeodesicMetric> =
        crate::geometry::SourceArcContext::new(TopologyDomain::Geographic(geographic), None);
    let output = source.snap_with_context(&reference, 0.5, &context).unwrap();
    let mut receipt = snap_drawing_receipt(output).unwrap();
    let omitted = receipt.literals.remove(0);
    let mut work = Unmetered;

    match super::super::finish::finish_geographic(
        PublicationCall::new(ProducerReceipt::ProducerDrawing(receipt), frame, &mut work),
        StorageTarget::Scalar,
    )
    .unwrap()
    {
        PublicationOutcome::Reject(PublicationRejection::ProducerDrawing(images)) => {
            assert_eq!(images, [omitted.image]);
        },
        PublicationOutcome::Pass(_) => {
            panic!("snap literal omission must fail final scalar readback");
        },
        PublicationOutcome::Reject(_) => {
            panic!("snap omission returns producer-drawing rejection");
        },
    }
}

#[test]
fn coverage_invalid_edge_binder_omission_fails_on_final_readback() {
    let input = [
        crate::io::parse_wkt("POLYGON ((170 0, -170 0, -170 10, 170 10, 170 0))")
            .expect("containing seam polygon"),
        crate::io::parse_wkt("POLYGON ((175 2, -175 2, -175 8, 175 8, 175 2))")
            .expect("contained seam polygon"),
    ];
    let (analysis, expected) = crate::geometry::coverage_invalid_edges_geographic_analysis(
        &input,
        crate::geographic::GeographicFrame::crs84(),
        0.0,
        "coverage_invalid_edges",
    )
    .unwrap();
    let [clean_row, offending_row] = expected.as_slice() else {
        panic!("the two source rows produce two diagnostic rows");
    };
    assert!(clean_row.is_empty());
    assert!(!offending_row.is_empty());

    let mut work = Unmetered;
    let mut receipt = analysis
        .invalid_edges_drawing_receipt(None, &mut work)
        .unwrap();
    let corrupted_row = expected
        .iter()
        .position(|shape| !shape.is_empty())
        .expect("overlap emits an offending source boundary");
    let corrupted_image = receipt
        .literals
        .iter()
        .find(|literal| literal.address.logical_row == corrupted_row)
        .expect("the offending row has a checked literal")
        .image;
    let changed = std::cell::Cell::new(false);
    let corrupted = expected
        .iter()
        .enumerate()
        .map(|(logical_row, shape)| {
            if logical_row == corrupted_row {
                shape.map_points(&|point| {
                    if changed.replace(true) {
                        Ok(point)
                    } else {
                        Ok(Point::new_raw_xy(point.x + 13.0, point.y))
                    }
                })
            } else {
                Ok(shape.clone())
            }
        })
        .collect::<crate::error::Result<Vec<_>>>()
        .unwrap();
    assert!(changed.get(), "the receipt-backed line was corrupted");

    let mut writer = StreamingShapes::with_capacity(0, corrupted.len());
    let mut row_map = Vec::with_capacity(expected.len());
    let mut literal_cursor = 0_usize;
    for (logical_row, (expected_shape, stored_shape)) in expected.iter().zip(corrupted).enumerate()
    {
        row_map.push(Some(
            producer_line_shape_layout(
                expected_shape,
                logical_row,
                &receipt.literals,
                &mut literal_cursor,
            )
            .unwrap(),
        ));
        writer.try_push(stored_shape).unwrap();
    }
    assert_eq!(literal_cursor, receipt.literals.len());
    let (storage, missing, layout) = writer.finish_parts().unwrap();
    receipt.buffers =
        FrozenLiteralBuffers::rows(Arc::new(storage), missing, layout, row_map, &mut work).unwrap();

    match super::super::finish::finish_geographic(
        PublicationCall::new(
            ProducerReceipt::ProducerDrawing(receipt),
            Frame::new(Some(crs_arc_static("OGC:CRS84")), None).unwrap(),
            &mut work,
        ),
        StorageTarget::Array,
    )
    .unwrap()
    {
        PublicationOutcome::Reject(PublicationRejection::ProducerDrawing(images)) => {
            assert!(images.contains(&corrupted_image));
        },
        PublicationOutcome::Pass(finished) => {
            let (rows, _) = finished.into_array().unwrap();
            for (logical_row, expected_shape) in expected.iter().enumerate() {
                let actual = match rows.logical_row(logical_row).expect("coverage row") {
                    crate::geometry::storage::LogicalRow::Present { row, .. } => {
                        row.with_shape(Clone::clone).unwrap()
                    },
                    crate::geometry::storage::LogicalRow::Missing { .. } => {
                        panic!("the coverage source row remains present")
                    },
                };
                assert_eq!(
                    &actual, expected_shape,
                    "omitting the producer-drawn binder must not publish a changed edge"
                );
            }
        },
        PublicationOutcome::Reject(_) => {
            panic!("coverage diagnostics return the producer-drawn rejection family")
        },
    }
}

fn geographic_coverage_clean_receipt() -> crate::error::Result<ProducerDrawingReceipt> {
    let input = [
        crate::io::parse_wkt("POLYGON ((170 0, -165 0, -165 10, 170 10, 170 0))")?,
        crate::io::parse_wkt("POLYGON ((175 0, -160 0, -160 10, 175 10, 175 0))")?,
    ];
    let frame = crate::geographic::GeographicFrame::crs84();
    let analysis =
        crate::geometry::analyze_geographic_coverage(&input, frame, 0.0, "coverage_clean")?;
    let areas = input.iter().map(Shape::area).collect::<Vec<_>>();
    let mut work = EvaluationBudget::new("coverage_clean", 0)?;
    let assignment = analysis.assign_cleaning_faces(
        &areas,
        0.0,
        crate::geometry::CoverageOverlapRule::MinIndex,
        None::<&crate::crs::GeodesicMetric>,
        &mut work,
    )?;
    let context: crate::geometry::SourceArcContext<'_, '_, crate::crs::GeodesicMetric> =
        crate::geometry::SourceArcContext::new(
            crate::geometry::TopologyDomain::Geographic(frame),
            None,
        );
    analysis.coverage_drawing_receipt(
        &assignment,
        &input.iter().map(Shape::axes).collect::<Vec<_>>(),
        &context,
        StorageTarget::Array,
        &mut work,
    )
}

fn producer_polygon_ring_layout(
    ring: &Ring,
    logical_row: usize,
    output: u32,
    literals: &[ProducerLiteral],
    literal_cursor: &mut usize,
) -> crate::error::Result<LiteralSequence<ProducerImageId>> {
    let mut images = Vec::with_capacity(ring.coords().len());
    for point in ring.coords() {
        let literal = literals.get(*literal_cursor).ok_or_else(|| {
            crate::geometry::GeometryErrorKind::Invalid(
                "coverage clean test has fewer literals than polygon coordinates".into(),
            )
        })?;
        let address = LiteralAddress {
            operation: 0,
            logical_row,
            group_row: None,
            value_row: None,
            output,
        };
        if literal.address != address || literal.record != point {
            return Err(crate::geometry::GeometryErrorKind::Invalid(
                "coverage clean test literal order differs from its row layout".into(),
            )
            .into());
        }
        images.push(literal.image);
        *literal_cursor = literal_cursor
            .checked_add(1)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
    }
    Ok(LiteralSequence {
        output,
        axes: ring.coords().axes(),
        images,
    })
}

fn producer_polygon_shape_layout(
    shape: &Shape,
    logical_row: usize,
    literals: &[ProducerLiteral],
    literal_cursor: &mut usize,
) -> crate::error::Result<LiteralRowLayout<ProducerImageId>> {
    let polygons = match shape {
        Shape::Polygon(polygon) => std::slice::from_ref(polygon),
        Shape::MultiPolygon(polygons) => polygons.as_slice(),
        Shape::Empty(GeometryKind::Polygon | GeometryKind::MultiPolygon, _) => &[],
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::GeometryCollection(_)
        | Shape::Empty(
            GeometryKind::Point
            | GeometryKind::MultiPoint
            | GeometryKind::LineString
            | GeometryKind::MultiLineString
            | GeometryKind::GeometryCollection,
            _,
        ) => {
            return Err(crate::geometry::GeometryErrorKind::Invalid(
                "coverage clean test received a non-polygonal output row".into(),
            )
            .into());
        },
    };
    let mut literal_polygons = Vec::with_capacity(polygons.len());
    for (output, polygon) in polygons.iter().enumerate() {
        let output =
            u32::try_from(output).map_err(|_| crate::error::ResourceError::SizeOverflow)?;
        let shell = producer_polygon_ring_layout(
            &polygon.shell,
            logical_row,
            output,
            literals,
            literal_cursor,
        )?;
        let mut holes = Vec::with_capacity(polygon.holes.len());
        for hole in polygon.holes.iter() {
            holes.push(producer_polygon_ring_layout(
                hole,
                logical_row,
                output,
                literals,
                literal_cursor,
            )?);
        }
        literal_polygons.push(LiteralPolygon {
            output,
            axes: polygon.axes(),
            shell,
            holes,
        });
    }
    Ok(LiteralRowLayout {
        operation: 0,
        logical_row,
        group_row: None,
        value_row: None,
        layout: ShapeLiteralLayout {
            structure: Some(ShapeStructure::capture(shape)),
            points: Vec::new(),
            lines: Vec::new(),
            polygons: literal_polygons,
        },
    })
}

#[test]
fn geographic_coverage_clean_producer_receipt_binds_all_output_rows() {
    let receipt = geographic_coverage_clean_receipt().unwrap();
    let mut work = Unmetered;
    match super::super::finish::finish_geographic(
        PublicationCall::new(
            ProducerReceipt::ProducerDrawing(receipt),
            Frame::None,
            &mut work,
        ),
        StorageTarget::Array,
    )
    .unwrap()
    {
        PublicationOutcome::Pass(finished) => {
            let (rows, _) = finished.into_array().unwrap();
            assert_eq!(rows.len(), 2);
        },
        PublicationOutcome::Reject(PublicationRejection::ProducerDrawing(_)) => {
            panic!("matching coverage clean storage must pass final readback");
        },
        PublicationOutcome::Reject(_) => {
            panic!("coverage clean must use the producer-drawing rejection family");
        },
    }
}

#[test]
fn geographic_coverage_clean_rejects_corrupted_final_storage() {
    let input = [
        crate::io::parse_wkt("POLYGON ((170 0, -165 0, -165 10, 170 10, 170 0))")
            .expect("first seam coverage row"),
        crate::io::parse_wkt("POLYGON ((175 0, -160 0, -160 10, 175 10, 175 0))")
            .expect("second seam coverage row"),
    ];
    let context: crate::geometry::SourceArcContext<'_, '_, crate::crs::GeodesicMetric> =
        crate::geometry::SourceArcContext::new(
            crate::geometry::TopologyDomain::Geographic(crate::geographic::GeographicFrame::crs84()),
            None,
        );
    let expected = crate::geometry::coverage_clean(
        &input,
        0.0,
        0.0,
        crate::geometry::CoverageOverlapRule::MinIndex,
        &context,
    )
    .unwrap();
    let mut receipt = geographic_coverage_clean_receipt().unwrap();
    let corrupted_row = expected
        .iter()
        .position(|shape| !shape.is_empty())
        .expect("the assigned seam coverage is non-empty");
    let corrupted_image = receipt
        .literals
        .iter()
        .find(|literal| literal.address.logical_row == corrupted_row)
        .expect("the cleaned row has a checked producer literal")
        .image;
    let changed = std::cell::Cell::new(false);
    let corrupted = expected
        .iter()
        .enumerate()
        .map(|(logical_row, shape)| {
            if logical_row == corrupted_row {
                shape.map_points(&|point| {
                    if changed.replace(true) {
                        Ok(point)
                    } else {
                        Ok(Point::new_raw_xy(point.x + 13.0, point.y))
                    }
                })
            } else {
                Ok(shape.clone())
            }
        })
        .collect::<crate::error::Result<Vec<_>>>()
        .unwrap();
    assert!(changed.get(), "the receipt-backed row was changed");

    let mut writer = StreamingShapes::with_capacity(0, corrupted.len());
    let mut row_map = Vec::with_capacity(expected.len());
    let mut literal_cursor = 0_usize;
    for (logical_row, (expected_shape, stored_shape)) in expected.iter().zip(corrupted).enumerate()
    {
        row_map.push(Some(
            producer_polygon_shape_layout(
                expected_shape,
                logical_row,
                &receipt.literals,
                &mut literal_cursor,
            )
            .unwrap(),
        ));
        writer.try_push(stored_shape).unwrap();
    }
    assert_eq!(literal_cursor, receipt.literals.len());
    let (storage, missing, layout) = writer.finish_parts().unwrap();
    let mut work = Unmetered;
    receipt.buffers =
        FrozenLiteralBuffers::rows(Arc::new(storage), missing, layout, row_map, &mut work).unwrap();

    match super::super::finish::finish_geographic(
        PublicationCall::new(
            ProducerReceipt::ProducerDrawing(receipt),
            Frame::None,
            &mut work,
        ),
        StorageTarget::Array,
    )
    .unwrap()
    {
        PublicationOutcome::Reject(PublicationRejection::ProducerDrawing(images)) => {
            assert!(images.contains(&corrupted_image));
        },
        PublicationOutcome::Pass(finished) => {
            let (rows, _) = finished.into_array().unwrap();
            let actual = match rows.logical_row(corrupted_row).expect("output row") {
                crate::geometry::storage::LogicalRow::Present { row, .. } => {
                    row.with_shape(Clone::clone).unwrap()
                },
                crate::geometry::storage::LogicalRow::Missing { .. } => {
                    panic!("coverage clean output rows remain present")
                },
            };
            assert_ne!(actual, expected[corrupted_row]);
        },
        PublicationOutcome::Reject(_) => {
            panic!("corrupted coverage storage returns producer-drawing rejection");
        },
    }
}

#[test]
fn geographic_coverage_clean_rejects_an_omitted_producer_literal() {
    let mut receipt = geographic_coverage_clean_receipt().unwrap();
    let omitted = receipt
        .literals
        .first()
        .expect("coverage output has a literal")
        .image;
    receipt.literals.retain(|literal| literal.image != omitted);
    let mut work = Unmetered;
    match super::super::finish::finish_geographic(
        PublicationCall::new(
            ProducerReceipt::ProducerDrawing(receipt),
            Frame::None,
            &mut work,
        ),
        StorageTarget::Array,
    )
    .unwrap()
    {
        PublicationOutcome::Reject(PublicationRejection::ProducerDrawing(images)) => {
            assert!(images.contains(&omitted));
        },
        PublicationOutcome::Pass(_) => {
            panic!("final coverage storage must reject an omitted producer literal");
        },
        PublicationOutcome::Reject(_) => {
            panic!("omitted coverage literals return producer-drawing rejection");
        },
    }
}

#[test]
fn coverage_duplicate_reversed_edges_merge_as_one_cycle() {
    let input = [
        crate::io::parse_wkt(
            "POLYGON ZM ((0 0 0 0, 2 0 20 200, 2 2 22 220, 2 0 24 240, 0 0 2 20, 0 -2 4 40, -2 -2 6 60, 0 0 0 0))",
        )
        .expect("one row repeats two edges in reverse with distinct ZM records"),
        crate::io::parse_wkt(
            "POLYGON ((0.5 -0.5, 1.5 -0.5, 1.5 0.5, 0.5 0.5, 0.5 -0.5))",
        )
        .expect("overlap marks the repeated horizontal source occurrences"),
    ];
    let source_points = input
        .first()
        .expect("first source row")
        .points_vec()
        .expect("ZM source coordinates are retained");
    let [
        first_start,
        first_end,
        _,
        reverse_start,
        reverse_end,
        _,
        _,
        _,
    ] = source_points.as_slice()
    else {
        panic!("the source ring retains its eight ZM records");
    };
    assert_ne!(first_start.z(), reverse_end.z());
    assert_ne!(first_end.z(), reverse_start.z());
    assert_ne!(first_start.m(), reverse_end.m());
    assert_ne!(first_end.m(), reverse_start.m());

    let (_, shapes) = crate::geometry::coverage_invalid_edges_geographic_analysis(
        &input,
        crate::geographic::GeographicFrame::crs84(),
        0.0,
        "coverage_invalid_edges",
    )
    .unwrap();
    let [Shape::LineString(line), _] = shapes.as_slice() else {
        panic!("the repeated source edges merge into one diagnostic cycle");
    };
    let source_start = Point::new_raw_xy(0.0, 0.0);
    let source_end = Point::new_raw_xy(2.0, 0.0);
    assert_eq!(line.iter().collect::<Vec<_>>(), [
        source_start,
        source_end,
        source_start
    ]);
}

#[test]
fn producer_drawing_rejects_corrupted_cdt_triangle_readback() {
    let first = Point::new_raw_xy(0.0, 0.0);
    let second = Point::new_raw_xy(1.0, 0.0);
    let third = Point::new_raw_xy(0.0, 1.0);
    let expected = [first, second, third, first];
    let corrupted = [first, Point::new_raw_xy(2.0, 0.0), third, first];
    let receipt = producer_triangle_receipt(&corrupted, &expected);
    let frame = Frame::default();
    let mut work = Unmetered;

    match super::super::finish::finish_geographic(
        PublicationCall::new(ProducerReceipt::ProducerDrawing(receipt), frame, &mut work),
        StorageTarget::Array,
    )
    .unwrap()
    {
        PublicationOutcome::Reject(PublicationRejection::ProducerDrawing(images)) => {
            assert_eq!(images, [ProducerImageId::new(1)]);
        },
        PublicationOutcome::Pass(finished) => {
            let (rows, _) = finished.into_array().unwrap();
            let Some(crate::geometry::storage::LogicalRow::Present { row, .. }) =
                rows.logical_row(0)
            else {
                panic!("the producer-drawn triangle row remains present");
            };
            let mut rings = row
                .polygon_rings()
                .expect("the producer-drawn triangle remains polygon storage");
            let ring = rings.next().expect("one triangle shell remains");
            let actual = (0..ring.coord_count())
                .map(|index| ring.nth_coord(index))
                .collect::<Vec<_>>();
            assert_eq!(
                actual, expected,
                "omitting the CDT producer binder must not publish changed coordinates"
            );
        },
        PublicationOutcome::Reject(_) => {
            panic!("CDT triangle storage returns the producer-drawing rejection family")
        },
    }
}

#[test]
fn producer_drawing_rejects_omitted_cdt_triangle_literal_on_readback() {
    let first = Point::new_raw_xy(0.0, 0.0);
    let second = Point::new_raw_xy(1.0, 0.0);
    let third = Point::new_raw_xy(0.0, 1.0);
    let expected = [first, second, third, first];
    let mut receipt = producer_triangle_receipt(&expected, &expected);
    let omitted = receipt.literals.remove(1);
    let mut work = Unmetered;

    match super::super::finish::finish_geographic(
        PublicationCall::new(
            ProducerReceipt::ProducerDrawing(receipt),
            Frame::default(),
            &mut work,
        ),
        StorageTarget::Array,
    )
    .unwrap()
    {
        PublicationOutcome::Reject(PublicationRejection::ProducerDrawing(images)) => {
            assert_eq!(images, [omitted.image]);
        },
        PublicationOutcome::Pass(_) => {
            panic!("an omitted CDT triangle literal must fail its storage readback");
        },
        PublicationOutcome::Reject(_) => {
            panic!("CDT triangle omission returns the producer-drawing rejection")
        },
    }
}

#[test]
fn mesh_receipt_omission_is_rejected_by_final_storage_readback() {
    let shape = Shape::Polygon(Polygon::new(
        Ring::try_from_closed(
            crate::geometry::CoordSeq::try_from_points(&[
                Point::new_raw_xy(0.0, 0.0),
                Point::new_raw_xy(4.0, 0.0),
                Point::new_raw_xy(0.0, 3.0),
                Point::new_raw_xy(0.0, 0.0),
            ])
            .unwrap(),
        )
        .unwrap(),
        Vec::new(),
    ));
    let mut budget = crate::geometry::ExpansionBudget::new("triangulate", "method");
    let mut mesh = shape.earcut_mesh_publication_budgeted(&mut budget).unwrap();
    let mut published =
        crate::geometry::PublishedTriangleVertices::uniform(mesh.take_points()).unwrap();
    published.mesh_publication = Some(mesh);
    let mut receipt = crate::geometry::mesh_publication_array_receipt(published).unwrap();
    assert_eq!(receipt.literals.len(), 4);
    receipt.literals.remove(0);

    let mut work = Unmetered;
    match super::super::finish::finish_geographic(
        PublicationCall::new(
            ProducerReceipt::ProducerDrawing(receipt),
            Frame::default(),
            &mut work,
        ),
        StorageTarget::Array,
    )
    .unwrap()
    {
        PublicationOutcome::Reject(PublicationRejection::ProducerDrawing(images)) => {
            assert_eq!(images, [ProducerImageId::new(0)]);
        },
        PublicationOutcome::Pass(_) => {
            panic!("a mesh literal omitted from the receipt must fail its storage readback");
        },
        PublicationOutcome::Reject(_) => {
            panic!("mesh literal omission returns the producer-drawing rejection")
        },
    }
}

#[test]
fn producer_drawing_rejects_omitted_literal_at_final_storage_readback() -> crate::error::Result<()>
{
    let point = Point::new_raw_xy(4.0, -2.0);
    let mut receipt = producer_point_receipt(StorageTarget::Array, point, point);
    receipt.literals.clear();
    let mut work = Unmetered;
    match super::super::finish::finish_geographic(
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
