#[test]
fn scalar_final_m_corruption_is_rejected_by_storage_binding() {
    let expected = xyzm_point(10.0, 20.0, 110.0, 1010.0);
    let corrupted = xyzm_point(10.0, 20.0, 110.0, 1011.0);
    let mut work = Unmetered;
    let mut valid_receiver = FullRecordReceiver {
        images: ImageRows(points(&[(10.0, 20.0)]).exact),
        expected: vec![(
            LiteralAddress {
                operation: 0,
                logical_row: 0,
                group_row: None,
                value_row: None,
                output: 0,
            },
            expected,
        )],
    };
    let valid = FrozenLiteralBuffers::shapes(Shape::Point(expected), ShapeLiteralLayout {
        structure: Some(ShapeStructure::capture(&Shape::Point(expected))),
        points: vec![LiteralPoint {
            output: 0,
            axes: CoordinateAxes::XYZM,
            image: 0,
        }],
        lines: Vec::new(),
        polygons: Vec::new(),
    });
    assert!(matches!(
        bind_literals(valid, &mut valid_receiver, &mut work).expect("bind valid scalar"),
        LiteralCheck::Pass(_)
    ));
    let mut receiver = FullRecordReceiver {
        images: ImageRows(points(&[(10.0, 20.0)]).exact),
        expected: vec![(
            LiteralAddress {
                operation: 0,
                logical_row: 0,
                group_row: None,
                value_row: None,
                output: 0,
            },
            expected,
        )],
    };
    let buffers = FrozenLiteralBuffers::shapes(Shape::Point(corrupted), ShapeLiteralLayout {
        structure: Some(ShapeStructure::capture(&Shape::Point(expected))),
        points: vec![LiteralPoint {
            output: 0,
            axes: CoordinateAxes::XYZM,
            image: 0,
        }],
        lines: Vec::new(),
        polygons: Vec::new(),
    });
    let LiteralCheck::Reject(rejected) =
        bind_literals(buffers, &mut receiver, &mut work).expect("bind scalar storage")
    else {
        panic!("a changed final M ordinate must be rejected");
    };
    assert_eq!(rejected, vec![0]);
}

#[test]
fn packed_rows_bind_after_present_only_admission() {
    let first = xyzm_point(10.0, 20.0, 110.0, 1010.0);
    let last = xyzm_point(30.0, 40.0, 330.0, 3030.0);
    let corrupted_last = xyzm_point(30.0, 40.0, 330.0, 3031.0);
    let mut work = Unmetered;
    let valid = FrozenLiteralBuffers::rows(
        point_rows(&[first, last]),
        ValidityMask::from_vec(3, vec![false, true, false]),
        StorageLayout::PresentOnly,
        vec![
            Some(point_literal_row(0, 7, 0, None, None)),
            None,
            Some(point_literal_row(1, 7, 2, None, None)),
        ],
        &mut work,
    )
    .expect("admit valid compact point storage and its writer map");
    let mut valid_receiver = FullRecordReceiver {
        images: ImageRows(points(&[(10.0, 20.0), (30.0, 40.0)]).exact),
        expected: vec![
            (
                LiteralAddress {
                    operation: 7,
                    logical_row: 0,
                    group_row: None,
                    value_row: None,
                    output: 0,
                },
                first,
            ),
            (
                LiteralAddress {
                    operation: 7,
                    logical_row: 2,
                    group_row: None,
                    value_row: None,
                    output: 0,
                },
                last,
            ),
        ],
    };
    assert!(matches!(
        bind_literals(valid, &mut valid_receiver, &mut work).expect("bind valid packed rows"),
        LiteralCheck::Pass(_)
    ));
    let missing = ValidityMask::from_vec(3, vec![false, true, false])
        .expect("the middle logical row is missing");
    let buffers = FrozenLiteralBuffers::rows(
        point_rows(&[first, corrupted_last]),
        Some(missing),
        StorageLayout::PresentOnly,
        vec![
            Some(point_literal_row(0, 7, 0, None, None)),
            None,
            Some(point_literal_row(1, 7, 2, None, None)),
        ],
        &mut work,
    )
    .expect("admit compact point storage and its writer map");
    let mut receiver = FullRecordReceiver {
        images: ImageRows(points(&[(10.0, 20.0), (30.0, 40.0)]).exact),
        expected: vec![
            (
                LiteralAddress {
                    operation: 7,
                    logical_row: 0,
                    group_row: None,
                    value_row: None,
                    output: 0,
                },
                first,
            ),
            (
                LiteralAddress {
                    operation: 7,
                    logical_row: 2,
                    group_row: None,
                    value_row: None,
                    output: 0,
                },
                last,
            ),
        ],
    };
    let LiteralCheck::Reject(rejected) =
        bind_literals(buffers, &mut receiver, &mut work).expect("bind packed row storage")
    else {
        panic!("the compacted row's changed final M ordinate must be rejected");
    };
    assert_eq!(rejected, vec![1]);
}

#[test]
fn gathered_rows_keep_duplicate_physical_rows_on_distinct_logical_receipts() {
    let first = xyzm_point(10.0, 20.0, 110.0, 1010.0);
    let second = xyzm_point(30.0, 40.0, 330.0, 3030.0);
    let storage = Arc::new(GeometryArrayStorage::Packed(PackedStorage::Points {
        coords: Arc::new(
            CoordSeq::try_from_points(&[first, second]).expect("pack gathered points"),
        ),
        row_map: RowSelection::gather_checked_native(vec![1, 0, 1], 2)
            .expect("admit duplicate gathered rows"),
    }));
    let mut work = Unmetered;
    let buffers = FrozenLiteralBuffers::rows(
        storage,
        None,
        StorageLayout::LogicalRows,
        vec![
            Some(point_literal_row(0, 23, 0, None, None)),
            Some(point_literal_row(1, 23, 1, None, None)),
            Some(point_literal_row(2, 23, 2, None, None)),
        ],
        &mut work,
    )
    .expect("admit gathered storage and logical writer maps");
    let mut receiver = FullRecordReceiver {
        images: ImageRows(points(&[(30.0, 40.0), (10.0, 20.0), (30.0, 40.0)]).exact),
        expected: vec![
            (
                LiteralAddress {
                    operation: 23,
                    logical_row: 0,
                    group_row: None,
                    value_row: None,
                    output: 0,
                },
                second,
            ),
            (
                LiteralAddress {
                    operation: 23,
                    logical_row: 1,
                    group_row: None,
                    value_row: None,
                    output: 0,
                },
                first,
            ),
            (
                LiteralAddress {
                    operation: 23,
                    logical_row: 2,
                    group_row: None,
                    value_row: None,
                    output: 0,
                },
                second,
            ),
        ],
    };
    assert!(matches!(
        bind_literals(buffers, &mut receiver, &mut work).expect("bind gathered final storage"),
        LiteralCheck::Pass(_)
    ));
}

#[test]
fn all_missing_packed_rows_have_no_literal_reads() {
    let storage = Arc::new(GeometryArrayStorage::Packed(PackedStorage::Points {
        coords: Arc::new(CoordSeq::try_from_points(&[]).expect("empty point column")),
        row_map: RowSelection::Identity,
    }));
    let mut work = Unmetered;
    let buffers = FrozenLiteralBuffers::rows(
        storage,
        ValidityMask::from_vec(3, vec![true, true, true]),
        StorageLayout::PresentOnly,
        vec![None, None, None],
        &mut work,
    )
    .expect("admit all-missing rows");
    let mut receiver = FullRecordReceiver {
        images: ImageRows(Vec::new()),
        expected: Vec::new(),
    };
    assert!(matches!(
        bind_literals(buffers, &mut receiver, &mut work).expect("bind all-missing rows"),
        LiteralCheck::Pass(_)
    ));
}

#[test]
fn empty_present_group_has_no_values_and_is_not_missing() {
    let storage = Arc::new(GeometryArrayStorage::Packed(PackedStorage::Points {
        coords: Arc::new(CoordSeq::try_from_points(&[]).expect("empty value column")),
        row_map: RowSelection::Identity,
    }));
    let mut work = Unmetered;
    let buffers = FrozenLiteralBuffers::groups(
        storage,
        None,
        StorageLayout::LogicalRows,
        Vec::new(),
        vec![0, 0],
        None,
        &mut work,
    )
    .expect("admit one empty-present group");
    let mut receiver = FullRecordReceiver {
        images: ImageRows(Vec::new()),
        expected: Vec::new(),
    };
    assert!(matches!(
        bind_literals(buffers, &mut receiver, &mut work).expect("bind empty-present group"),
        LiteralCheck::Pass(_)
    ));
}

#[test]
fn scalar_empty_readback_checks_kind_and_axes_without_literals() {
    use crate::geometry::embedding_completion::literals::ShapeStructure;
    use crate::geometry::embedding_completion::storage::FrozenStorage;
    use crate::geometry::GeometryKind;

    for actual in [
        Shape::Empty(GeometryKind::LineString, CoordinateAxes::XYZM),
        Shape::Empty(GeometryKind::LineString, CoordinateAxes::XY),
        Shape::Empty(GeometryKind::Point, CoordinateAxes::XYZM),
    ] {
        let correct = matches!(actual, Shape::Empty(GeometryKind::LineString, CoordinateAxes::XYZM));
        let mut buffers = FrozenLiteralBuffers::shapes(
            Shape::Empty(GeometryKind::LineString, CoordinateAxes::XYZM),
            ShapeLiteralLayout::<u32> {
                structure: Some(ShapeStructure::primitive(GeometryKind::LineString, CoordinateAxes::XYZM, true)),
                points: Vec::new(),
                lines: Vec::new(),
                polygons: Vec::new(),
            },
        );
        let FrozenStorage::Scalar { shape, .. } = &mut buffers.storage else {
            panic!("the scalar sink retains its storage owner");
        };
        *shape = actual;
        let mut receiver = FullRecordReceiver {
            images: ImageRows(Vec::new()),
            expected: Vec::new(),
        };
        let result = bind_literals(buffers, &mut receiver, &mut Unmetered);
        if correct {
            assert!(matches!(result.unwrap(), LiteralCheck::Pass(_)));
        } else {
            let Err(error) = result else {
                panic!("typed empty metadata changed after capture");
            };
            assert!(matches!(error.kind(), crate::error::ErrorKind::Geometry(crate::geometry::GeometryErrorKind::Invalid(_))));
        }
    }
}

#[test]
fn empty_group_readback_checks_captured_presence_and_length() {
    use crate::geometry::embedding_completion::storage::FrozenStorage;

    for (offsets, missing) in [
        (vec![0, 0], ValidityMask::from_vec(1, vec![true])),
        (vec![0], None),
    ] {
        let mut buffers = FrozenLiteralBuffers::<u32>::groups(
            Arc::new(GeometryArrayStorage::Mixed(Vec::new())),
            None, StorageLayout::LogicalRows, Vec::new(), vec![0, 0], None, &mut Unmetered,
        ).unwrap();
        let FrozenStorage::Groups { groups, .. } = &mut buffers.storage else {
            panic!("the group sink retains its storage owner");
        };
        groups.offsets = offsets;
        groups.missing = missing;
        let mut receiver = FullRecordReceiver {
            images: ImageRows(Vec::new()),
            expected: Vec::new(),
        };
        let Err(error) = bind_literals(buffers, &mut receiver, &mut Unmetered) else {
            panic!("an empty group changed after capture");
        };
        assert!(matches!(error.kind(), crate::error::ErrorKind::Geometry(crate::geometry::GeometryErrorKind::Invalid(_))));
    }
}

#[test]
fn packed_line_and_polygon_coordinates_bind_from_final_csr_windows() {
    let records = [
        xyzm_point(10.0, 20.0, 110.0, 1010.0),
        xyzm_point(12.0, 24.0, 120.0, 1020.0),
        xyzm_point(14.0, 28.0, 140.0, 1040.0),
    ];
    let exact = points(&[(10.0, 20.0), (12.0, 24.0), (14.0, 28.0)]).exact;
    let mut work = Unmetered;
    let line_buffers = FrozenLiteralBuffers::rows(
        line_rows(&records),
        None,
        StorageLayout::LogicalRows,
        vec![Some(LiteralRowLayout {
            operation: 13,
            logical_row: 0,
            group_row: None,
            value_row: None,
            layout: ShapeLiteralLayout {
                structure: Some(ShapeStructure::primitive(
                    crate::geometry::GeometryKind::LineString, CoordinateAxes::XYZM, false,
                )),
                points: Vec::new(),
                lines: vec![LiteralSequence {
                    output: 0,
                    axes: CoordinateAxes::XYZM,
                    images: vec![0, 1, 2],
                }],
                polygons: Vec::new(),
            },
        })],
        &mut work,
    )
    .expect("admit final packed line columns");
    let mut line_receiver = FullRecordReceiver {
        images: ImageRows(exact.clone()),
        expected: records
            .iter()
            .copied()
            .map(|record| {
                (
                    LiteralAddress {
                        operation: 13,
                        logical_row: 0,
                        group_row: None,
                        value_row: None,
                        output: 0,
                    },
                    record,
                )
            })
            .collect(),
    };
    assert!(matches!(
        bind_literals(line_buffers, &mut line_receiver, &mut work)
            .expect("bind final line columns"),
        LiteralCheck::Pass(_)
    ));

    let ring = [records[0], records[1], records[2], records[0]];
    let polygon_buffers = FrozenLiteralBuffers::rows(
        polygon_rows(&ring),
        None,
        StorageLayout::LogicalRows,
        vec![Some(LiteralRowLayout {
            operation: 17,
            logical_row: 0,
            group_row: None,
            value_row: None,
            layout: ShapeLiteralLayout {
                structure: Some(ShapeStructure::primitive(
                    crate::geometry::GeometryKind::Polygon, CoordinateAxes::XYZM, false,
                )),
                points: Vec::new(),
                lines: Vec::new(),
                polygons: vec![LiteralPolygon {
                    output: 0,
                    axes: CoordinateAxes::XYZM,
                    shell: LiteralSequence {
                        output: 0,
                        axes: CoordinateAxes::XYZM,
                        images: vec![0, 1, 2, 0],
                    },
                    holes: Vec::new(),
                }],
            },
        })],
        &mut work,
    )
    .expect("admit final packed polygon columns");
    let mut polygon_receiver = FullRecordReceiver {
        images: ImageRows(exact),
        expected: records
            .iter()
            .copied()
            .map(|record| {
                (
                    LiteralAddress {
                        operation: 17,
                        logical_row: 0,
                        group_row: None,
                        value_row: None,
                        output: 0,
                    },
                    record,
                )
            })
            .collect(),
    };
    assert!(matches!(
        bind_literals(polygon_buffers, &mut polygon_receiver, &mut work)
            .expect("bind final polygon columns"),
        LiteralCheck::Pass(_)
    ));
}

#[test]
fn groups_bind_values_after_csr_and_validity_admission() {
    let first = xyzm_point(10.0, 20.0, 110.0, 1010.0);
    let second = xyzm_point(20.0, 30.0, 220.0, 2020.0);
    let third = xyzm_point(30.0, 40.0, 330.0, 3030.0);
    let corrupted_third = xyzm_point(30.0, 40.0, 330.0, 3031.0);
    let mut work = Unmetered;
    let valid = FrozenLiteralBuffers::groups(
        point_rows(&[first, second, third]),
        None,
        StorageLayout::LogicalRows,
        vec![
            Some(point_literal_row(0, 11, 0, Some(0), Some(0))),
            Some(point_literal_row(1, 11, 0, Some(0), Some(1))),
            Some(point_literal_row(2, 11, 91, Some(3), Some(2))),
        ],
        vec![0, 2, 2, 2, 3],
        ValidityMask::from_vec(4, vec![false, false, true, false]),
        &mut work,
    )
    .expect("admit valid group CSR and values");
    let mut valid_receiver = FullRecordReceiver {
        images: ImageRows(points(&[(10.0, 20.0), (20.0, 30.0), (30.0, 40.0)]).exact),
        expected: vec![
            (
                LiteralAddress {
                    operation: 11,
                    logical_row: 0,
                    group_row: Some(0),
                    value_row: Some(0),
                    output: 0,
                },
                first,
            ),
            (
                LiteralAddress {
                    operation: 11,
                    logical_row: 0,
                    group_row: Some(0),
                    value_row: Some(1),
                    output: 0,
                },
                second,
            ),
            (
                LiteralAddress {
                    operation: 11,
                    logical_row: 91,
                    group_row: Some(3),
                    value_row: Some(2),
                    output: 0,
                },
                third,
            ),
        ],
    };
    assert!(matches!(
        bind_literals(valid, &mut valid_receiver, &mut work).expect("bind valid group storage"),
        LiteralCheck::Pass(_)
    ));
    let buffers = FrozenLiteralBuffers::groups(
        point_rows(&[first, second, corrupted_third]),
        None,
        StorageLayout::LogicalRows,
        vec![
            Some(point_literal_row(0, 11, 0, Some(0), Some(0))),
            Some(point_literal_row(1, 11, 0, Some(0), Some(1))),
            Some(point_literal_row(2, 11, 91, Some(3), Some(2))),
        ],
        vec![0, 2, 2, 2, 3],
        ValidityMask::from_vec(4, vec![false, false, true, false]),
        &mut work,
    )
    .expect("admit group CSR, validity, and value maps");
    let mut receiver = FullRecordReceiver {
        images: ImageRows(points(&[(10.0, 20.0), (20.0, 30.0), (30.0, 40.0)]).exact),
        expected: vec![
            (
                LiteralAddress {
                    operation: 11,
                    logical_row: 0,
                    group_row: Some(0),
                    value_row: Some(0),
                    output: 0,
                },
                first,
            ),
            (
                LiteralAddress {
                    operation: 11,
                    logical_row: 0,
                    group_row: Some(0),
                    value_row: Some(1),
                    output: 0,
                },
                second,
            ),
            (
                LiteralAddress {
                    operation: 11,
                    logical_row: 91,
                    group_row: Some(3),
                    value_row: Some(2),
                    output: 0,
                },
                third,
            ),
        ],
    };
    let LiteralCheck::Reject(rejected) =
        bind_literals(buffers, &mut receiver, &mut work).expect("bind final Groups storage")
    else {
        panic!("the grouped value's changed final M ordinate must be rejected");
    };
    assert_eq!(rejected, vec![2]);
}
