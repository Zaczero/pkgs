
    #[test]
    fn source_only_seam_union_stays_on_quotient_publication() -> Result<()> {
        let east = crate::io::parse_wkt("POLYGON ((170 0, 180 0, 180 10, 170 10, 170 0))")?;
        let west = crate::io::parse_wkt("POLYGON ((-180 0, -170 0, -170 10, -180 10, -180 0))")?;

        assert_eq!(
            native_demand_for_shapes(&[east, west], OverlayOp::Union)?,
            (false, false),
            "a connected seam union uses only unchanged source/cut ports"
        );
        Ok(())
    }
    #[test]
    fn source_only_receipt_binds_the_current_publication_buffers() -> Result<()> {
        use super::super::emission::{
            FeatureFamily, FeatureFamilyParent, FragmentRecipeId, PublicationInventory,
            PublishedSample, SampleProducerKind,
        };
        let source_shape = crate::io::parse_wkt("POINT (0 0)")?;
        let geographic_frame = GeographicFrame::crs84();
        let inputs = crate::geographic::GeographicInputs::try_from_shapes(
            geographic_frame,
            std::iter::once(&source_shape),
        )
        .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
        let output_frame = crate::Frame::CrsEpoch("OGC:CRS84".into(), 2024.5);
        let topology = ExactGeographicTopology::from_views_as_set_with_publication_frame(
            geographic_frame,
            output_frame.clone(),
            inputs.views().iter().copied(),
        )?;
        let point = Point::new_raw_axes(
            7.0,
            6.0,
            crate::geometry::ZOrdinate(Some(70.0)),
            crate::geometry::MOrdinate(Some(700.0)),
        );
        let corrupted_point = Point::new_raw_axes(
            7.0,
            6.0,
            crate::geometry::ZOrdinate(Some(70.0)),
            crate::geometry::MOrdinate(Some(701.0)),
        );
        let mut sample_ordinates = NodeOrdinates::default();
        sample_ordinates.z.observe(Some(70.0));
        sample_ordinates.m.observe(Some(700.0));
        let exact = ExactChartPoint::from_rationals(
            ExactRational::from_f64(7.0),
            ExactRational::from_f64(6.0),
        );
        let publication = PublicationInventory {
            fragments: Vec::new(),
            samples: vec![PublishedSample {
                request_row: RequestRowId::from_index(0)?,
                family: FeatureFamily {
                    parent: FeatureFamilyParent::Fragment(FragmentRecipeId(0)),
                    producer: SampleProducerKind::Publication,
                    occurrence: 0,
                },
                parameter: ExactRational::from_f64(0.5),
                written_vertex: 0,
                point,
                exact_point: exact.clone(),
                published_chart_point: exact.clone(),
                source_trace: None,
                endpoint_only: false,
            }],
            counts: vec![1],
            ordinates: vec![sample_ordinates],
        };
        let source_ordinates = [];
        let publication_images = [exact];
        let publication_points = [XY::new(7.0, 6.0)];
        let publication_ordinates = [sample_ordinates];
        let mut work = Unmetered;

        for target in [
            StorageTarget::Scalar,
            StorageTarget::Array,
            StorageTarget::Groups,
        ] {
            let source = source_drawing_for_storage(corrupted_point, target, &mut work)?;
            let receipt = SourceOnlyPublication::new(
                source,
                &topology,
                None,
                &source_ordinates,
                Some(&publication),
                &publication_images,
                &publication_points,
                &publication_ordinates,
            );
            let call = PublicationCall::new(
                ProducerReceipt::QSourceOnly(receipt),
                topology.publication_frame().clone(),
                &mut work,
            );
            match finish_geographic(call, target)? {
                PublicationOutcome::Reject(PublicationRejection::SourceOnly(images)) => {
                    assert_eq!(images, [QSourceImage::PublicationSample(0)]);
                },
                PublicationOutcome::Reject(PublicationRejection::Native) => {
                    return Err(GeometryErrorKind::Invalid(
                        "source-only receipt returned native rejection".into(),
                    )
                    .into());
                },
                PublicationOutcome::Reject(PublicationRejection::Batch(_)) => {
                    return Err(GeometryErrorKind::Invalid(
                        "scalar source-only receipt returned a batch rejection".into(),
                    )
                    .into());
                },
                PublicationOutcome::Reject(PublicationRejection::ProducerDrawing(_)) => {
                    return Err(GeometryErrorKind::Invalid(
                        "scalar source-only receipt returned a producer-drawn rejection".into(),
                    )
                    .into());
                },
                PublicationOutcome::Pass(finished) => {
                    let (actual, _) = read_finished_point(finished, target)?;
                    assert_eq!(
                        actual,
                        Some(point),
                        "omitting the binder must fail final-storage readback"
                    );
                },
            }

            let source = source_drawing_for_storage(point, target, &mut work)?;
            let receipt = SourceOnlyPublication::new(
                source,
                &topology,
                None,
                &source_ordinates,
                Some(&publication),
                &publication_images,
                &publication_points,
                &publication_ordinates,
            );
            let call = PublicationCall::new(
                ProducerReceipt::QSourceOnly(receipt),
                topology.publication_frame().clone(),
                &mut work,
            );
            let PublicationOutcome::Pass(finished) = finish_geographic(call, target)? else {
                return Err(GeometryErrorKind::Invalid(
                    "valid source-only output was rejected".into(),
                )
                .into());
            };
            let (actual, finished_frame) = read_finished_point(finished, target)?;
            assert_eq!(
                actual,
                Some(point),
                "final storage readback must retain the checked full record"
            );
            assert_eq!(finished_frame, output_frame);
        }

        let wrong_publication_images = [ExactChartPoint::from_rationals(
            ExactRational::from_f64(7.0),
            ExactRational::from_f64(6.0_f64.next_up()),
        )];
        let source = source_drawing_for_storage(point, StorageTarget::Scalar, &mut work)?;
        let receipt = SourceOnlyPublication::new(
            source,
            &topology,
            None,
            &source_ordinates,
            Some(&publication),
            &wrong_publication_images,
            &publication_points,
            &publication_ordinates,
        );
        assert!(matches!(
            finish_geographic(
                PublicationCall::new(
                    ProducerReceipt::QSourceOnly(receipt),
                    topology.publication_frame().clone(),
                    &mut work,
                ),
                StorageTarget::Scalar,
            )?,
            PublicationOutcome::Reject(PublicationRejection::SourceOnly(images))
                if images == [QSourceImage::PublicationSample(0)]
        ), "the unchanged record must still match its independently assigned publication image");

        let mut missing_sample_inventory = publication;
        missing_sample_inventory.samples.clear();
        let source = source_drawing_for_storage(point, StorageTarget::Scalar, &mut work)?;
        let receipt = SourceOnlyPublication::new(
            source,
            &topology,
            None,
            &source_ordinates,
            Some(&missing_sample_inventory),
            &publication_images,
            &publication_points,
            &publication_ordinates,
        );
        let error = finish_geographic(
            PublicationCall::new(
                ProducerReceipt::QSourceOnly(receipt),
                topology.publication_frame().clone(),
                &mut work,
            ),
            StorageTarget::Scalar,
        )
        .expect_err("an assigned but out-of-context sample remains an error");
        assert!(
            error
                .to_string()
                .contains("source-only literal sample is outside its publication context"),
            "sample inventory failures retain their SourceOnly error: {error}"
        );
        Ok(())
    }
    #[test]
    fn exactly_representable_constructed_seam_vertex_stays_on_quotient_publication() -> Result<()> {
        let source = crate::io::parse_wkt("POLYGON ((170 0, -170 10, 170 10, 170 0))")?;
        let cutter = crate::io::parse_wkt("POLYGON ((180 2, 185 2, 185 8, 180 8, 180 2))")?;

        assert_eq!(
            native_demand_for_shapes(&[source, cutter], OverlayOp::Intersection)?,
            (false, true),
            "an exact constructed seam vertex has no binary64 rounding residual"
        );
        Ok(())
    }
    #[test]
    fn exactly_representable_seam_difference_stays_on_quotient_publication() -> Result<()> {
        let source = crate::io::parse_wkt("POLYGON ((170 0, -170 0, -170 20, 170 20, 170 0))")?;
        let cutter = crate::io::parse_wkt("POLYGON ((175 10, -175 10, -175 30, 175 30, 175 10))")?;

        assert_eq!(
            native_demand_for_shapes(&[source, cutter], OverlayOp::Difference)?,
            (false, true),
            "constructed (175,20) and (-175,20) have exact binary64 coordinates"
        );
        Ok(())
    }
    #[test]
    fn nonrepresentable_constructed_seam_vertex_requires_native_completion() -> Result<()> {
        let source = crate::io::parse_wkt("POLYGON ((179 0, -178 1, 179 1, 179 0))")?;
        let cutter = crate::io::parse_wkt("POLYGON ((180 0, 181 0, 181 1, 180 1, 180 0))")?;

        let (demand, constructed_vertex) =
            native_demand_for_shapes(&[source, cutter], OverlayOp::Intersection)?;
        assert!(constructed_vertex);
        assert!(demand);
        Ok(())
    }
    #[test]
    fn seam_wall_reductions_complete_through_the_native_reference() -> Result<()> {
        let west = vec![
            crate::io::parse_wkt("POLYGON ((170 -1, -170 -1, -170 2, 170 2, 170 -1))")?,
            crate::io::parse_wkt("POLYGON ((177 0, -174 1, -174 3, 177 2, 177 0))")?,
            crate::io::parse_wkt("POLYGON ((-180 60, -179 61, -180 62, -180 60))")?,
            crate::io::parse_wkt("POLYGON ((-180 50, -179 51, -180 52, -180 50))")?,
        ];
        let east = vec![
            crate::io::parse_wkt("POLYGON ((170 -1, -170 -1, -170 2, 170 2, 170 -1))")?,
            crate::io::parse_wkt("POLYGON ((177 0, -174 1, -174 3, 177 2, 177 0))")?,
            crate::io::parse_wkt("POLYGON ((180 60, 180 62, 179 61, 180 60))")?,
        ];

        for (bank, shapes) in [("West", west), ("East", east)] {
            let output = native_union_for_shapes(&shapes)?;
            assert!(
                output.validate()?.is_none(),
                "{bank} seam-wall reduction did not produce a valid Native result"
            );
        }
        Ok(())
    }
    #[test]
    fn polar_q_cut_midpoint_rounds_to_its_native_site() -> Result<()> {
        let upper = 80.0_f64.next_up();
        let polar_shell = crate::io::parse_wkt(&format!(
            "POLYGON ((-180 80, -120 80, 20 {upper}, 90 80, 180 80, -180 80))"
        ))?;
        let overlap_left =
            crate::io::parse_wkt("POLYGON ((170 -1, -170 -1, -170 2, 170 2, 170 -1))")?;
        let overlap_right =
            crate::io::parse_wkt("POLYGON ((177 0, -174 1, -174 3, 177 2, 177 0))")?;
        let output = native_union_for_shapes(&[polar_shell, overlap_left, overlap_right])?;

        assert!(output.validate()?.is_none());
        Ok(())
    }
    #[test]
    fn selected_pole_wedge_is_a_fan_not_a_terminal_chart_closure() -> Result<()> {
        let cap = crate::io::parse_wkt("POLYGON ((0 80, 90 80, 180 80, -90 80, 0 80))")?;
        let wedge = crate::io::parse_wkt("POLYGON ((120 90, 110 75, 130 75, 120 90))")?;
        let topology = topology(&[cap, wedge])?;
        let mut work = Unmetered;
        let plan = QEmissionPlan::build(&topology, OverlayOp::Intersection, &mut work)?;
        let NativeSelectedPoleSectors { sectors, closures } =
            native_selected_pole_sectors(&topology.q, &plan.selected_face_roots, &mut work)?;
        let (region, clockwise, counterclockwise) = sectors
            .iter()
            .find_map(|selected| match selected.sector {
                super::super::q::PoleSector::Fan {
                    pole: ChartPole::North,
                    clockwise,
                    counterclockwise,
                    ..
                } => Some((selected.region, clockwise, counterclockwise)),
                super::super::q::PoleSector::Fan { .. }
                | super::super::q::PoleSector::Isolated { .. }
                | super::super::q::PoleSector::Terminal { .. } => None,
            })
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("north wedge has no selected pole fan".into())
            })?;

        assert_ne!(clockwise, counterclockwise);
        assert!(closures.contains(&(region, ChartPole::North)));
        assert!(!sectors.iter().any(|selected| {
            selected.region == region
                && matches!(selected.sector, super::super::q::PoleSector::Terminal {
                    pole: ChartPole::North,
                    ..
                })
        }));
        Ok(())
    }
    #[test]
    fn native_pole_closures_keep_terminal_isolated_and_full_turn_sectors() -> Result<()> {
        let sectors = [
            NativeSelectedPoleSector {
                region: 1,
                sector: super::super::q::PoleSector::Fan {
                    pole: ChartPole::North,
                    clockwise: QDartId(0),
                    counterclockwise: QDartId(1),
                    face: GlobalFaceId(0),
                },
            },
            NativeSelectedPoleSector {
                region: 4,
                sector: super::super::q::PoleSector::Fan {
                    pole: ChartPole::North,
                    clockwise: QDartId(2),
                    counterclockwise: QDartId(2),
                    face: GlobalFaceId(3),
                },
            },
            NativeSelectedPoleSector {
                region: 2,
                sector: super::super::q::PoleSector::Terminal {
                    pole: ChartPole::North,
                    face: GlobalFaceId(1),
                },
            },
            NativeSelectedPoleSector {
                region: 3,
                sector: super::super::q::PoleSector::Isolated {
                    pole: ChartPole::South,
                    node: QNodeId(0),
                    face: GlobalFaceId(2),
                },
            },
        ];
        let closures = native_pole_closure_regions(&sectors, &mut Unmetered)?;

        assert_eq!(
            closures,
            BTreeSet::from([
                (2, ChartPole::North),
                (3, ChartPole::South),
                (4, ChartPole::North),
            ])
        );
        Ok(())
    }
    #[test]
    fn exact_source_producer_identity_distinguishes_sub_ulp_latitudes() -> Result<()> {
        let frame = GeographicFrame::crs84();
        let one = ExactRational::one();
        let quarter_ulp = ExactRational::from_f64(2.0_f64.powi(-54));
        let half_ulp = ExactRational::from_f64(2.0_f64.powi(-53));
        let left = ExactChartPoint::from_rationals(
            ExactRational::from_f64(25.0),
            one.try_add(&quarter_ulp)?,
        );
        let right =
            ExactChartPoint::from_rationals(ExactRational::from_f64(25.0), one.try_add(&half_ulp)?);

        assert_eq!(rounded_chart_point(&left)?, rounded_chart_point(&right)?);
        let mut work = Unmetered;
        let left_key = quotient_key_for_chart_position_with_work(frame, &left, &mut work)?;
        let right_key = quotient_key_for_chart_position_with_work(frame, &right, &mut work)?;
        assert!(
            !compare_quotient_keys_with_work(&left_key, &right_key, &mut work)?.is_eq(),
            "producer correspondence must not collapse exact origins that round alike"
        );
        Ok(())
    }
    #[test]
    fn assigned_expanded_producer_fan_rejects_a_ray_swap() -> Result<()> {
        let point = |x, y| {
            ExactChartPoint::from_rationals(ExactRational::from_f64(x), ExactRational::from_f64(y))
        };
        let origin = point(0.0, 0.0);
        let east = point(1.0, 0.0);
        let north = point(0.0, 1.0);
        let west = point(-1.0, 0.0);
        let exact = vec![origin.clone(), east.clone(), north.clone(), west.clone()];
        let points = NativeReferencePoints {
            exact,
            rounded: vec![
                XY::new(0.0, 0.0),
                XY::new(1.0, 0.0),
                XY::new(0.0, 1.0),
                XY::new(-1.0, 0.0),
            ],
        };
        let expected = [QDartId(1), QDartId(2), QDartId(3)];
        let mut ordered = [
            (QDartId(1), 1, east.clone()),
            (QDartId(2), 2, north.clone()),
            (QDartId(3), 3, west.clone()),
        ];
        let mut work = Unmetered;
        assert!(assigned_producer_fan_order_matches(
            &points,
            &origin,
            &mut ordered,
            &expected,
            &mut work,
        )?);
        let mut swapped = [
            (QDartId(1), 1, east),
            (QDartId(2), 3, west),
            (QDartId(3), 2, north),
        ];
        let origin = point(0.0, 0.0);
        for (left_nodes, left_exact, left_candidate, right_nodes, right_exact, right_candidate) in [
            (
                [
                    QGraphNode::Source(QNodeId(0)),
                    QGraphNode::Published(EmissionNodeId(1)),
                ],
                [origin.clone(), point(1.0, 0.0)],
                [origin.clone(), point(1.0, 0.0)],
                [
                    QGraphNode::Source(QNodeId(0)),
                    QGraphNode::Published(EmissionNodeId(2)),
                ],
                [origin.clone(), point(0.0, 1.0)],
                [origin.clone(), point(-1.0, 0.0)],
            ),
            (
                [
                    QGraphNode::Source(QNodeId(0)),
                    QGraphNode::Published(EmissionNodeId(1)),
                ],
                [origin.clone(), point(1.0, 0.0)],
                [origin.clone(), point(1.0, 0.0)],
                [
                    QGraphNode::Source(QNodeId(0)),
                    QGraphNode::Published(EmissionNodeId(3)),
                ],
                [origin.clone(), point(-1.0, 0.0)],
                [origin.clone(), point(0.0, 1.0)],
            ),
            (
                [
                    QGraphNode::Source(QNodeId(0)),
                    QGraphNode::Published(EmissionNodeId(2)),
                ],
                [origin.clone(), point(0.0, 1.0)],
                [origin.clone(), point(-1.0, 0.0)],
                [
                    QGraphNode::Source(QNodeId(0)),
                    QGraphNode::Published(EmissionNodeId(3)),
                ],
                [origin.clone(), point(-1.0, 0.0)],
                [origin.clone(), point(0.0, 1.0)],
            ),
        ] {
            assert!(pair_preserves_incidence(
                left_nodes,
                &left_exact,
                &left_candidate,
                right_nodes,
                &right_exact,
                &right_candidate,
            )?);
        }
        assert!(!assigned_producer_fan_order_matches(
            &points,
            &origin,
            &mut swapped,
            &expected,
            &mut work,
        )?);
        Ok(())
    }
    #[test]
    fn periodic_interior_aliases_share_one_canonical_graph_vertex() -> Result<()> {
        let frame = GeographicFrame::crs84();
        let y = ExactRational::from_f64(20.0);
        let canonical_point =
            ExactChartPoint::from_rationals(ExactRational::from_f64(-179.8), y.clone());
        let periodic_point = ExactChartPoint::from_rationals(ExactRational::from_f64(180.2), y);
        let port = |point, periods| NativePortData {
            node: EmissionNodeId(7),
            point,
            spelling: PortSpelling::Interior { periods },
            source_record: None,
            occurrences: Vec::new(),
        };
        let mut area_edges = Vec::new();
        let mut line_edges = Vec::new();
        let mut ports = vec![port(canonical_point.clone(), 0), port(periodic_point, 1)];
        let mut work = Unmetered;

        normalize_native_chart_lifts(
            &mut area_edges,
            &mut line_edges,
            &mut ports,
            frame,
            &mut work,
        )?;
        let graph = new_native_reference_graph_builder(&area_edges, &ports, &mut work)?;

        assert_eq!(ports[1].point, canonical_point);
        assert!(matches!(&ports[1].spelling, PortSpelling::Interior {
            periods: 0
        }));
        assert_eq!(graph.points.exact.len(), 1);
        assert_eq!(
            native_chart_vertex_key(&ports[0]),
            native_chart_vertex_key(&ports[1])
        );
        Ok(())
    }
    #[test]
    fn native_seam_port_without_a_record_uses_its_assigned_bank() -> Result<()> {
        let frame = GeographicFrame::crs84();
        let mut work = Unmetered;
        for (longitude, side) in [
            (180.0, super::super::emission::ChartSide::East),
            (-180.0, super::super::emission::ChartSide::West),
        ] {
            let point = ExactChartPoint::from_rationals(
                ExactRational::from_f64(longitude),
                ExactRational::from_f64(-1.0),
            );
            let port = native_port_data(
                frame,
                EmissionNodeId(0),
                QCandidate {
                    native: XY::new(longitude, -1.0),
                    periods: 0,
                },
                &point,
                None,
                Vec::new(),
                None,
                &mut work,
            )?;

            assert_eq!(port.point, point);
            assert!(matches!(&port.spelling, PortSpelling::Seam(actual) if *actual == side));
        }
        Ok(())
    }
    #[test]
    fn native_seam_source_record_keeps_its_chart_bank() -> Result<()> {
        let frame = GeographicFrame::crs84();
        let mut work = Unmetered;
        for (longitude, side) in [
            (180.0, super::super::emission::ChartSide::East),
            (-180.0, super::super::emission::ChartSide::West),
        ] {
            let point = ExactChartPoint::from_rationals(
                ExactRational::from_f64(longitude),
                ExactRational::from_f64(-1.0),
            );
            let record = Point::new_raw_xy(longitude, -1.0);
            let port = native_port_data(
                frame,
                EmissionNodeId(0),
                QCandidate {
                    native: XY::new(longitude, -1.0),
                    periods: 0,
                },
                &point,
                Some(record),
                Vec::new(),
                None,
                &mut work,
            )?;

            assert_eq!(port.point, point);
            assert_eq!(
                port.source_record.map(|record| record.x.to_bits()),
                Some(longitude.to_bits())
            );
            assert!(matches!(&port.spelling, PortSpelling::Seam(actual) if *actual == side));
        }
        Ok(())
    }
    #[test]
    fn equatorial_east_west_aliases_are_seam_banks_not_pole_longitudes() -> Result<()> {
        let frame = GeographicFrame::crs84();
        let node = EmissionNodeId(0);
        let half = ExactRational::from_f64(frame.longitude().half_period());
        let west = ExactRational::zero().try_subtract(&half)?;
        let keys = [
            NativeChartVertexKey {
                node,
                spelling: NativeChartSpellingKey::SeamEast,
            },
            NativeChartVertexKey {
                node,
                spelling: NativeChartSpellingKey::SeamWest,
            },
        ];
        let points = [
            ExactChartPoint::from_rationals(half, ExactRational::zero()),
            ExactChartPoint::from_rationals(west, ExactRational::zero()),
        ];
        let mut work = Unmetered;
        let sewing = native_graph_sewing(&keys, &points, frame, &mut work)?;
        assert_eq!(sewing.as_slice(), &[NativeGraphSewing {
            vertices: [0, 1],
            kind: NativeGraphSewingKind::SeamBank,
        }]);
        Ok(())
    }
    #[test]
    fn duplicate_east_bank_provider_is_rejected() -> Result<()> {
        let frame = GeographicFrame::crs84();
        let node = EmissionNodeId(0);
        let keys = [
            NativeChartVertexKey {
                node,
                spelling: NativeChartSpellingKey::SeamEast,
            },
            NativeChartVertexKey {
                node,
                spelling: NativeChartSpellingKey::SeamWest,
            },
            NativeChartVertexKey {
                node,
                spelling: NativeChartSpellingKey::SeamEast,
            },
        ];
        let east = ExactRational::from_f64(frame.longitude().half_period());
        let west = ExactRational::zero()
            .try_subtract(&ExactRational::from_f64(frame.longitude().half_period()))?;
        let points = [
            ExactChartPoint::from_rationals(east.clone(), ExactRational::zero()),
            ExactChartPoint::from_rationals(west, ExactRational::zero()),
            ExactChartPoint::from_rationals(east, ExactRational::zero()),
        ];
        let mut work = Unmetered;
        native_graph_sewing(&keys, &points, frame, &mut work).unwrap_err();
        Ok(())
    }
    #[test]
    fn duplicate_west_bank_provider_is_rejected() -> Result<()> {
        let frame = GeographicFrame::crs84();
        let node = EmissionNodeId(0);
        let half = ExactRational::from_f64(frame.longitude().half_period());
        let west = ExactRational::zero().try_subtract(&half)?;
        let keys = [
            NativeChartVertexKey {
                node,
                spelling: NativeChartSpellingKey::SeamWest,
            },
            NativeChartVertexKey {
                node,
                spelling: NativeChartSpellingKey::SeamEast,
            },
            NativeChartVertexKey {
                node,
                spelling: NativeChartSpellingKey::SeamWest,
            },
        ];
        let points = [
            ExactChartPoint::from_rationals(west.clone(), ExactRational::zero()),
            ExactChartPoint::from_rationals(half, ExactRational::zero()),
            ExactChartPoint::from_rationals(west, ExactRational::zero()),
        ];
        let mut work = Unmetered;
        native_graph_sewing(&keys, &points, frame, &mut work).unwrap_err();
        Ok(())
    }
    #[test]
    fn opposite_seam_banks_must_have_the_same_latitude() -> Result<()> {
        let frame = GeographicFrame::crs84();
        let node = EmissionNodeId(0);
        let half = ExactRational::from_f64(frame.longitude().half_period());
        let west = ExactRational::zero().try_subtract(&half)?;
        let keys = [
            NativeChartVertexKey {
                node,
                spelling: NativeChartSpellingKey::SeamEast,
            },
            NativeChartVertexKey {
                node,
                spelling: NativeChartSpellingKey::SeamWest,
            },
        ];
        let points = [
            ExactChartPoint::from_rationals(half, ExactRational::zero()),
            ExactChartPoint::from_rationals(west, ExactRational::from_f64(1.0)),
        ];
        let mut work = Unmetered;
        native_graph_sewing(&keys, &points, frame, &mut work).unwrap_err();
        Ok(())
    }
    #[test]
    fn nonpolar_pole_spelling_is_rejected() {
        let frame = GeographicFrame::crs84();
        let keys = [NativeChartVertexKey {
            node: EmissionNodeId(0),
            spelling: NativeChartSpellingKey::Pole(0.0_f64.to_bits()),
        }];
        let points = [ExactChartPoint::from_rationals(
            ExactRational::zero(),
            ExactRational::zero(),
        )];
        let mut work = Unmetered;
        native_graph_sewing(&keys, &points, frame, &mut work).unwrap_err();
    }
    #[test]
    fn interior_spelling_at_a_pole_is_rejected() {
        let frame = GeographicFrame::crs84();
        let keys = [NativeChartVertexKey {
            node: EmissionNodeId(0),
            spelling: NativeChartSpellingKey::Interior(0),
        }];
        let points = [ExactChartPoint::from_rationals(
            ExactRational::zero(),
            ExactRational::from_f64(frame.chart_pole_latitude(ChartPole::North)),
        )];
        let mut work = Unmetered;
        native_graph_sewing(&keys, &points, frame, &mut work).unwrap_err();
    }
    #[test]
    fn native_whole_sphere_reference_covers_and_sews_two_chart_strips() -> Result<()> {
        let source = crate::geometry::Shape::Polygon(crate::geometry::Polygon::new(
            Ring::try_from_closed(CoordSeq::try_from_points(&[
                Point::new_raw_xy(-180.0, -90.0),
                Point::new_raw_xy(-90.0, -90.0),
                Point::new_raw_xy(0.0, -90.0),
                Point::new_raw_xy(90.0, -90.0),
                Point::new_raw_xy(180.0, -90.0),
                Point::new_raw_xy(180.0, 90.0),
                Point::new_raw_xy(90.0, 90.0),
                Point::new_raw_xy(0.0, 90.0),
                Point::new_raw_xy(-90.0, 90.0),
                Point::new_raw_xy(-180.0, 90.0),
                Point::new_raw_xy(-180.0, -90.0),
            ])?)?,
            Vec::new(),
        ));
        let topology = topology(&[source])?;
        let operation = OverlayOp::Union;
        let mut budget = EvaluationBudget::new("native whole-sphere cover", 0)?;
        let plan = super::super::emission::plan::QEmissionPlan::build(&topology, operation, &mut budget)?;
        assert!(matches!(&plan.polygons, PolygonPlan::WholeSphere(_)));
        let nearest = QCoordinates::build_with_work(&topology, &mut budget)?;
        let publication =
            plan.fragment_recipes
                .publish(&topology, &nearest, operation.name(), &mut budget)?;
        let region = plan
            .selected_face_roots
            .iter()
            .flatten()
            .copied()
            .next()
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("whole-sphere plan has no selected region".into())
            })?;
        let native_cut_ranges = publication
            .fragments
            .iter()
            .map(|fragment| vec![Vec::new(); fragment.trace.rows.edge_count()])
            .collect::<Vec<_>>();
        let region_families = (0..10_u32)
            .map(|occurrence| super::super::emission::FeatureFamily {
                parent: super::super::emission::FeatureFamilyParent::Region(region),
                producer: super::super::emission::SampleProducerKind::WholeSphereCover,
                occurrence,
            })
            .collect();
        let feature_bounds = FeatureFamilyBounds::from_prepared_domains(
            plan.fragment_recipes.as_slice(),
            &publication,
            &native_cut_ranges,
            region_families,
            &mut budget,
        )?;
        let mut families = feature_bounds.into_registry(0);
        let mut builder =
            super::super::emission::EmissionBuilder::new(&topology, &plan, &[], true, &mut budget)?;
        let mut presentation_candidates = Vec::new();
        let edges = native_whole_sphere_cell_edges(
            &mut builder,
            topology.sources.frame,
            region,
            &mut presentation_candidates,
            &mut families,
            &mut budget,
        )?;
        let graph_builder = new_native_reference_graph_builder(&edges, &[], &mut budget)?;
        let boundary = edges.first().ok_or_else(|| {
            GeometryErrorKind::Invalid("whole-sphere cover has no boundary edge".into())
        })?;
        assert!(graph_builder.has_area_edge(
            region,
            &boundary.vertices[0],
            &boundary.vertices[1],
            &mut budget,
        )?);
        graph_builder
            .has_area_edge(
                region,
                &boundary.vertices[1],
                &boundary.vertices[0],
                &mut budget,
            )
            .expect_err("opposite selected boundary orientation must be rejected");
        let mut graph = graph_builder.finish(topology.sources.frame, &mut budget)?;
        let NativeReferenceGraphResult {
            cycles,
            parts,
            face_regions,
            face_groups,
            face_cells,
        } = native_reference_cycles_and_parts(&graph, &edges, &[], true, true, &mut budget)?;
        graph.cycles = cycles;
        graph.parts = parts;
        graph.face_regions = face_regions;
        graph.face_groups = face_groups;
        graph.face_cells = face_cells;

        assert_eq!(edges.len(), 12);
        assert_eq!(graph.points.exact.len(), 10);
        assert_eq!(graph.written_edges.len(), 12);
        assert_eq!(graph.atoms.len(), 11);
        assert_eq!(graph.faces.count, 3);
        assert_eq!(graph.cycles.len(), 2);
        assert_eq!(graph.parts.len(), 2);
        assert!(graph.parts.iter().all(|part| part.holes.is_empty()));
        assert!(
            graph
                .cycles
                .iter()
                .all(|cycle| cycle.edges.len() == 6 && cycle.shell)
        );
        let point = |x, y| {
            ExactChartPoint::from_rationals(ExactRational::from_f64(x), ExactRational::from_f64(y))
        };
        let expected_cells = [
            [
                point(-180.0, -90.0),
                point(-90.0, -90.0),
                point(0.0, -90.0),
                point(0.0, 90.0),
                point(-90.0, 90.0),
                point(-180.0, 90.0),
            ],
            [
                point(0.0, -90.0),
                point(90.0, -90.0),
                point(180.0, -90.0),
                point(180.0, 90.0),
                point(90.0, 90.0),
                point(0.0, 90.0),
            ],
        ];
        let mut seen_cells = [false; 2];
        let mut seam_vertices = [None, None];
        for part in &graph.parts {
            let cycle = graph.cycles.get(part.shell).ok_or_else(|| {
                GeometryErrorKind::Invalid("whole-sphere test shell is unknown".into())
            })?;
            let mut cell = None;
            for cycle_edge in &cycle.edges {
                for &written_edge in &cycle_edge.contributors {
                    let written = edges.get(written_edge).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "whole-sphere test contributor is unknown".into(),
                        )
                    })?;
                    let NativeAreaEdgeKind::WholeSphereCover { cell: edge_cell } = &written.kind
                    else {
                        return Err(GeometryErrorKind::Invalid(
                            "whole-sphere test shell contains a non-cover edge".into(),
                        )
                        .into());
                    };
                    if cell.is_some_and(|previous| previous != *edge_cell) {
                        return Err(GeometryErrorKind::Invalid(
                            "whole-sphere test shell mixes cell owners".into(),
                        )
                        .into());
                    }
                    cell = Some(*edge_cell);
                }
            }
            let cell = cell.ok_or_else(|| {
                GeometryErrorKind::Invalid("whole-sphere test shell has no cell owner".into())
            })?;
            let expected = expected_cells.get(cell as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("whole-sphere test has an unknown cell".into())
            })?;
            let mut seen_edges = [false; 6];
            for cycle_edge in &cycle.edges {
                let start = native_graph_cycle_vertex(
                    topology.sources.frame,
                    &graph,
                    cycle_edge,
                    0,
                    &edges,
                    &mut budget,
                )?;
                let end = native_graph_cycle_vertex(
                    topology.sources.frame,
                    &graph,
                    cycle_edge,
                    1,
                    &edges,
                    &mut budget,
                )?;
                let mut matched = None;
                for start_index in 0..expected.len() {
                    let end_index = start_index
                        .checked_add(1)
                        .ok_or(ResourceError::SizeOverflow)?
                        .checked_rem(expected.len())
                        .ok_or(ResourceError::SizeOverflow)?;
                    if expected.get(start_index) == Some(&start.point)
                        && expected.get(end_index) == Some(&end.point)
                    {
                        matched = Some(start_index);
                        break;
                    }
                }
                let matched = matched.ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "whole-sphere test shell differs from its expected longitude strip".into(),
                    )
                })?;
                let seen = seen_edges.get_mut(matched).ok_or_else(|| {
                    GeometryErrorKind::Invalid("whole-sphere test edge is unknown".into())
                })?;
                assert!(!std::mem::replace(seen, true));
                let seam_edge = if cell == 0 { 5 } else { 2 };
                if matched == seam_edge {
                    let seam = seam_vertices.get_mut(cell as usize).ok_or_else(|| {
                        GeometryErrorKind::Invalid("whole-sphere test seam cell is unknown".into())
                    })?;
                    *seam = Some([
                        graph.local_vertex(native_chart_vertex_key(&start), &mut budget)?,
                        graph.local_vertex(native_chart_vertex_key(&end), &mut budget)?,
                    ]);
                }
            }
            assert!(seen_edges.into_iter().all(|seen| seen));
            *seen_cells.get_mut(cell as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("whole-sphere test cell is unknown".into())
            })? = true;
        }
        assert_eq!(seen_cells, [true, true]);
        let [Some(west), Some(east)] = seam_vertices else {
            return Err(GeometryErrorKind::Invalid(
                "whole-sphere test lost a sewn seam edge".into(),
            )
            .into());
        };
        let mut pole_aliases = UnionFind::new(graph.points.exact.len());
        for sewing in &graph.sewing {
            if sewing.kind == NativeGraphSewingKind::PoleLongitude {
                pole_aliases.union(sewing.vertices[0] as usize, sewing.vertices[1] as usize);
            }
        }
        for (west_vertex, east_vertex) in [(west[0], east[1]), (west[1], east[0])] {
            let west_key = graph.vertex_keys.get(west_vertex as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("whole-sphere test west pole is unknown".into())
            })?;
            let east_key = graph.vertex_keys.get(east_vertex as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("whole-sphere test east pole is unknown".into())
            })?;
            assert_eq!(west_key.node, east_key.node);
            assert!(matches!(west_key.spelling, NativeChartSpellingKey::Pole(_)));
            assert!(matches!(east_key.spelling, NativeChartSpellingKey::Pole(_)));
            assert_eq!(
                graph.topology.component_of.get(west_vertex as usize),
                graph.topology.component_of.get(east_vertex as usize),
            );
            assert_eq!(
                pole_aliases.find_with_work(west_vertex as usize, &mut budget)?,
                pole_aliases.find_with_work(east_vertex as usize, &mut budget)?,
            );
        }
        assert!(native_graph_sewing_matches(
            &graph.sewing,
            &graph.vertex_keys,
            &graph.points.exact,
            topology.sources.frame,
            &mut budget,
        )?);
        let mut omitted_pole_aliases = graph.sewing.clone();
        omitted_pole_aliases.retain(|sewing| sewing.kind != NativeGraphSewingKind::PoleLongitude);
        assert!(omitted_pole_aliases.len() < graph.sewing.len());
        assert!(
            !native_graph_sewing_matches(
                &omitted_pole_aliases,
                &graph.vertex_keys,
                &graph.points.exact,
                topology.sources.frame,
                &mut budget,
            )?,
            "required pole sewing must not be waived by connected ordinary chart atoms"
        );
        let shared = graph
            .atoms
            .iter()
            .filter(|atom| atom.contributors.len() == 2)
            .collect::<Vec<_>>();
        assert_eq!(shared.len(), 1);
        let shared_atom = shared.first().ok_or_else(|| {
            GeometryErrorKind::Invalid("whole-sphere shared edge is unknown".into())
        })?;
        let left = shared_atom.contributors.first().ok_or_else(|| {
            GeometryErrorKind::Invalid("whole-sphere shared edge has no left owner".into())
        })?;
        let right = shared_atom.contributors.get(1).ok_or_else(|| {
            GeometryErrorKind::Invalid("whole-sphere shared edge has no right owner".into())
        })?;
        assert_ne!(left.reversed, right.reversed);
        let left_edge = edges.get(left.written_edge).ok_or_else(|| {
            GeometryErrorKind::Invalid("whole-sphere shared edge owner is unknown".into())
        })?;
        let right_edge = edges.get(right.written_edge).ok_or_else(|| {
            GeometryErrorKind::Invalid("whole-sphere shared edge owner is unknown".into())
        })?;
        assert!(matches!(
            (&left_edge.kind, &right_edge.kind),
            (
                NativeAreaEdgeKind::WholeSphereCover { cell: 0 },
                NativeAreaEdgeKind::WholeSphereCover { cell: 1 },
            ) | (
                NativeAreaEdgeKind::WholeSphereCover { cell: 1 },
                NativeAreaEdgeKind::WholeSphereCover { cell: 0 },
            )
        ));
        assert_eq!(families.additional_constructed(), 10);
        Ok(())
    }
