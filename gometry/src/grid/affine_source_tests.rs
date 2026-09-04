use std::cmp::Ordering;

use h3o::Resolution;

use crate::geometry::{CoordSeq, LineSeq, Point, Polygon, Ring, Shape, is_axis_aligned_rectangle};
use crate::grid::affine_source::{
    CompactLimbs, DirectPreimages, EXPANSION_LIMBS, ExactExpansion, ExactParentParameter,
    ExactPlanarPoint, ExactRatio, FULL_TURN_DEGREES, GeographicCoverageSource, GridPointClass,
    LiftKind, LiftedChain, ParentCrossing, ParentParameterKey, PartitionConstraint, RectClass,
    SourceEdgeKey, SourceVertexKey, SphericalGridTarget, SymbolicAffineEdge,
    authority_edge_exact_refinements, authority_edge_proxy_envelopes, build_authority_edge_index,
    classify_lifted_ring_bucket, combine_polygon_point_classes, exact_affine_level,
    exact_axis_rectangle, exact_segment_touches_rectangle, integer_levels_between, lifted_step,
    reset_authority_edge_exact_refinements, subdivided_axis_aligned_rectangle,
};
use crate::grid::spherical_arc::{
    Bound, CertifiedDegreeWindows, CertifiedLongitudeDegrees, DegreeWindowResult,
    PhysicalEndpointKey,
};

fn periodic_latitude_ring(latitude: f64, reverse: bool) -> Ring {
    let mut points = Vec::new();
    if reverse {
        for longitude in (-180..180).step_by(5).rev() {
            points.push(Point::new(f64::from(longitude), latitude).unwrap());
        }
    } else {
        for longitude in (-180..180).step_by(5) {
            points.push(Point::new(f64::from(longitude), latitude).unwrap());
        }
    }
    points.push(points[0]);
    Ring::from_closed(CoordSeq::from(points))
}

fn exhaustive_authority_point_class(
    source: &GeographicCoverageSource,
    longitude: f64,
    latitude: f64,
) -> GridPointClass {
    if source.is_unknown()
        || !longitude.is_finite()
        || !latitude.is_finite()
        || latitude.to_bits() == 90.0_f64.to_bits()
        || latitude.to_bits() == (-90.0_f64).to_bits()
    {
        return GridPointClass::Unknown;
    }
    let Some(query) = ExactPlanarPoint::from_stored(longitude, latitude) else {
        return GridPointClass::Unknown;
    };
    let mut aggregate = GridPointClass::Exterior;
    for shell in source.ring_fills.iter().filter(|fill| fill.ring == 0) {
        let Some(chain) = source.chains.get(shell.chain_index) else {
            return GridPointClass::Unknown;
        };
        let shell_class = shell.direct_oracle_point_class(chain, longitude, &query);
        let component = match shell_class {
            GridPointClass::Exterior => GridPointClass::Exterior,
            GridPointClass::Boundary | GridPointClass::Unknown => shell_class,
            GridPointClass::Interior => {
                let mut unknown = false;
                let mut hole_class = GridPointClass::Interior;
                for hole in source
                    .ring_fills
                    .iter()
                    .filter(|fill| fill.component == shell.component && fill.ring != 0)
                {
                    let Some(chain) = source.chains.get(hole.chain_index) else {
                        return GridPointClass::Unknown;
                    };
                    match hole.direct_oracle_point_class(chain, longitude, &query) {
                        GridPointClass::Boundary => {
                            hole_class = GridPointClass::Boundary;
                            break;
                        },
                        GridPointClass::Interior => {
                            hole_class = GridPointClass::Exterior;
                            break;
                        },
                        GridPointClass::Unknown => unknown = true,
                        GridPointClass::Exterior => {},
                    }
                }
                if hole_class == GridPointClass::Interior && unknown {
                    GridPointClass::Unknown
                } else {
                    hole_class
                }
            },
        };
        aggregate = combine_polygon_point_classes(aggregate, component);
    }
    aggregate
}

fn assert_indexed_matches_exhaustive(source: &GeographicCoverageSource, points: &[(f64, f64)]) {
    for &(longitude, latitude) in points {
        assert_eq!(
            source.authority_point_class(longitude, latitude),
            exhaustive_authority_point_class(source, longitude, latitude),
            "source point ({longitude}, {latitude})",
        );
        let Some(query) = ExactPlanarPoint::from_stored(longitude, latitude) else {
            continue;
        };
        for shell in &source.ring_fills {
            let Some(chain) = source.chains.get(shell.chain_index) else {
                panic!("ring fill has no authoritative chain");
            };
            assert_eq!(
                shell.point_class(chain, longitude, &query),
                shell.direct_oracle_point_class(chain, longitude, &query),
                "ring {}/{} point ({longitude}, {latitude})",
                shell.component,
                shell.ring,
            );
        }
    }
}

fn polygon_source(shell: Vec<Point>, holes: Vec<Vec<Point>>) -> GeographicCoverageSource {
    GeographicCoverageSource::new(
        &Shape::Polygon(Polygon::new(
            Ring::from_closed(CoordSeq::from(shell)),
            holes
                .into_iter()
                .map(|hole| Ring::from_closed(CoordSeq::from(hole)))
                .collect(),
        )),
        SphericalGridTarget::H3(Resolution::Two),
    )
    .unwrap()
}

fn symbolic_edge(points: &[(f64, f64)], kind: LiftKind) -> SymbolicAffineEdge {
    let chain = LiftedChain::from_points(points, false).expect("finite affine source chain");
    let key = SourceEdgeKey {
        component: 0,
        ring: 0,
        ordinal: 0,
    };
    let endpoints = [
        SourceVertexKey {
            component: 0,
            ring: 0,
            ordinal: 0,
        },
        SourceVertexKey {
            component: 0,
            ring: 0,
            ordinal: 1,
        },
    ];
    SymbolicAffineEdge::new(
        key,
        endpoints,
        false,
        chain.vertices[0],
        chain.vertices[1],
        kind,
    )
    .expect("exact parent partitions")
}

fn unpartitioned_symbolic_edge(points: &[(f64, f64)]) -> SymbolicAffineEdge {
    let chain = LiftedChain::from_points(points, false).expect("finite affine source chain");
    SymbolicAffineEdge {
        key: SourceEdgeKey {
            component: 0,
            ring: 0,
            ordinal: 0,
        },
        endpoints: [
            SourceVertexKey {
                component: 0,
                ring: 0,
                ordinal: 0,
            },
            SourceVertexKey {
                component: 0,
                ring: 0,
                ordinal: 1,
            },
        ],
        polygon: false,
        start: chain.vertices[0],
        end: chain.vertices[1],
        parameters: vec![
            ExactParentParameter::start().unwrap(),
            ExactParentParameter::end().unwrap(),
        ],
    }
}

#[test]
fn symbolic_split_stages_cover_longitude_reflection_and_post_reflection_seams() {
    let mut edge = unpartitioned_symbolic_edge(&[(-200.0, -100.0), (160.0, 100.0)]);
    let longitude_start = edge.lifted_longitude_start().unwrap();
    let longitude_end = edge.lifted_longitude_end().unwrap();
    assert_eq!(
        integer_levels_between(&longitude_start, &longitude_end, 180, 360),
        Some(-2..=0)
    );
    let seam_crossing = edge
        .parameter_for(
            &longitude_start,
            &longitude_end,
            &ExactExpansion::from_i64(-180).unwrap(),
        )
        .unwrap();
    let ParentCrossing::Present(seam_ratio) = seam_crossing else {
        panic!("the -180 degree lifted seam must cross this full edge");
    };
    assert!(
        ExactParentParameter::new(
            ParentParameterKey::Partition(PartitionConstraint::LiftedLongitudeSeam),
            seam_ratio,
        )
        .is_some()
    );
    for level in integer_levels_between(&longitude_start, &longitude_end, 180, 360).unwrap() {
        let seam = exact_affine_level(180, 360, level).unwrap();
        assert!(
            edge.parameter_for(&longitude_start, &longitude_end, &seam)
                .is_some(),
            "level {level}"
        );
    }
    assert!(edge.collect_lifted_longitude_seams().is_some());
    assert!(edge.collect_latitude_reflections().is_some());
    assert!(edge.parameters.iter().any(|parameter| {
        parameter.key == ParentParameterKey::Partition(PartitionConstraint::LiftedLongitudeSeam)
    }));
    assert!(edge.parameters.iter().any(|parameter| {
        parameter.key == ParentParameterKey::Partition(PartitionConstraint::LatitudeReflection)
    }));
    let mut post_reflection = unpartitioned_symbolic_edge(&[(-100.0, 100.0), (260.0, 110.0)]);
    assert!(
        post_reflection
            .collect_post_reflection_longitude_seams()
            .is_some()
    );
    assert!(post_reflection.parameters.iter().any(|parameter| {
        parameter.key
            == ParentParameterKey::Partition(PartitionConstraint::PostReflectionLongitudeSeam)
    }));
}

#[test]
fn exact_ratio_orders_the_two_stored_doubles_around_forty_over_three() {
    let third = ExactRatio::from_i64(1, 3).unwrap();
    let lower = f64::from_bits(0x402A_AAAA_AAAA_AAAA);
    let upper = f64::from_bits(0x402A_AAAA_AAAA_AAAB);
    let scale = ExactExpansion::from_i64(40).unwrap();
    let ratio = |value| ExactRatio::new(&ExactExpansion::from_f64(value).unwrap(), &scale).unwrap();
    assert_eq!(ratio(lower).ordering(&third), Some(Ordering::Less));
    assert_eq!(ratio(upper).ordering(&third), Some(Ordering::Greater));
}

#[test]
fn compact_exact_storage_canonicalizes_heap_zero_and_grows_from_spare_capacity() {
    let mut backing = Vec::with_capacity(8);
    backing.extend_from_slice(&[1.0, 2.0, 4.0, 8.0]);
    let mut storage = CompactLimbs::Heap(backing);
    storage.prepare(12).unwrap();
    assert_eq!(&storage.as_slice()[..4], &[1.0, 2.0, 4.0, 8.0]);
    assert_eq!(storage.as_slice().len(), 12);
    storage.truncate(4).unwrap();
    assert_eq!(storage.as_slice(), &[1.0, 2.0, 4.0, 8.0]);
    assert!(storage.prepare(EXPANSION_LIMBS + 1).is_none());

    let heap = ExactExpansion {
        limbs: CompactLimbs::Heap(vec![
            2.0_f64.powi(-60),
            2.0_f64.powi(-40),
            2.0_f64.powi(-20),
            1.0,
            2.0,
        ]),
    };
    let heap_zero = heap.sub(&heap).unwrap();
    let inline_zero = ExactExpansion::zero();
    assert_eq!(heap_zero, inline_zero);
    assert!(heap_zero.as_slice().is_empty());
    assert_eq!(heap.ordering(), Ordering::Greater);
    assert_eq!(
        heap.add(&inline_zero).unwrap().ordering(),
        Ordering::Greater
    );
}

#[test]
fn exact_ratio_equality_deduplicates_inline_and_heap_representations() {
    let inline = ExactRatio::from_i64(1, 3).unwrap();
    let heap = ExactRatio::from_parts(
        ExactExpansion {
            limbs: CompactLimbs::Heap(vec![1.0]),
        },
        ExactExpansion {
            limbs: CompactLimbs::Heap(vec![3.0]),
        },
    )
    .unwrap();
    assert_eq!(inline, heap);

    let mut edge = unpartitioned_symbolic_edge(&[(0.0, 0.0), (1.0, 1.0)]);
    edge.insert_ratio(
        ParentParameterKey::Partition(PartitionConstraint::FullEdgeMidpoint),
        inline,
    )
    .unwrap();
    let parameter_count = edge.parameters.len();
    edge.insert_ratio(
        ParentParameterKey::Partition(PartitionConstraint::LatitudeReflection),
        heap,
    )
    .unwrap();
    assert_eq!(edge.parameters.len(), parameter_count);
    assert_eq!(
        edge.parameters[1].key,
        ParentParameterKey::Partition(PartitionConstraint::FullEdgeMidpoint)
    );
    assert_eq!(
        edge.parameters[1].ratio,
        ExactRatio::from_i64(1, 3).unwrap()
    );
}

#[test]
fn exact_parent_endpoints_have_certified_enclosures() {
    assert!(ExactParentParameter::start().is_some());
    assert!(ExactParentParameter::end().is_some());
    assert!(
        ExactParentParameter::new(
            ParentParameterKey::Partition(PartitionConstraint::FullEdgeMidpoint),
            ExactRatio::from_i64(1, 2).unwrap(),
        )
        .is_some()
    );
}

#[test]
fn generated_parent_split_retains_exact_one_third_through_the_affine_map() {
    // The latitude reflection at 90 degrees is generated from this parent
    // edge at exactly t = 1/3.  The corresponding longitude is 40/3, so
    // this reaches the real split path rather than supplying a test ratio.
    let edge = symbolic_edge(&[(0.0, 0.0), (40.0, 270.0)], LiftKind::Shortest);
    let split = edge
        .parameters
        .iter()
        .find(|parameter| {
            parameter.key == ParentParameterKey::Partition(PartitionConstraint::LatitudeReflection)
        })
        .expect("the generated 90-degree reflection split");
    assert_eq!(
        split.ratio.ordering(&ExactRatio::from_i64(1, 3).unwrap()),
        Some(Ordering::Equal)
    );
    let position = edge
        .coordinate_at(
            &split.ratio,
            &edge.lifted_longitude_start().unwrap(),
            &edge.lifted_longitude_end().unwrap(),
        )
        .unwrap();
    let lower = ExactRatio::from_f64(f64::from_bits(0x402A_AAAA_AAAA_AAAA)).unwrap();
    let upper = ExactRatio::from_f64(f64::from_bits(0x402A_AAAA_AAAA_AAAB)).unwrap();
    assert_eq!(position.ordering(&lower), Some(Ordering::Greater));
    assert_eq!(position.ordering(&upper), Some(Ordering::Less));
    let enclosure = position.certified_bound().unwrap();
    assert_ne!(
        position.ordering(&ExactRatio::from_f64(enclosure.lo).unwrap()),
        Some(Ordering::Less)
    );
    assert_ne!(
        position.ordering(&ExactRatio::from_f64(enclosure.hi).unwrap()),
        Some(Ordering::Greater)
    );
    let mut pieces = Vec::new();
    edge.selection_pieces(&mut pieces).unwrap();
    assert!(pieces.iter().any(|piece| {
        piece.interval.end.key
            == ParentParameterKey::Partition(PartitionConstraint::LatitudeReflection)
            && piece.interval.end.ratio.ordering(&split.ratio) == Some(Ordering::Equal)
    }));
}

#[test]
fn geographic_coverage_source_canonicalizes_accepted_pole_rounding_before_wrapping() {
    let input = CoordSeq::from(vec![
        Point::new(0.0, 90.0_f64.next_up()).unwrap(),
        Point::new(180.0_f64.next_up(), 0.0).unwrap(),
    ]);
    let source = GeographicCoverageSource::new(
        &Shape::MultiPoint(input.into()),
        SphericalGridTarget::H3(Resolution::Zero),
    )
    .unwrap();
    assert!(!source.is_unknown());
    assert_eq!(source.authority_points.len(), 2);
    assert_eq!(source.selection_points.len(), 2);
    let over_north = &source.selection_points[0];
    assert_eq!(
        source.authority_points[0]
            .latitude
            .checked_estimate()
            .unwrap()
            .to_bits(),
        90.0_f64.to_bits(),
    );
    assert_eq!(
        over_north.latitude.checked_estimate().unwrap().to_bits(),
        90.0_f64.to_bits()
    );
    assert_eq!(
        over_north
            .longitude
            .checked_estimate()
            .unwrap()
            .abs()
            .to_bits(),
        180.0_f64.to_bits()
    );

    let over_east = &source.selection_points[1];
    assert_eq!(
        over_east.longitude.checked_estimate().unwrap().to_bits(),
        (180.0_f64.next_up() - FULL_TURN_DEGREES).to_bits()
    );
    assert_eq!(
        over_east.latitude.checked_estimate().unwrap().to_bits(),
        0.0_f64.to_bits()
    );
    assert_eq!(source.selection_points[0].key.component, 0);
    assert_eq!(source.selection_points[1].key.ordinal, 1);
}

#[test]
fn geographic_coverage_source_retains_point_features_for_later_bbox_traversal() {
    let source = GeographicCoverageSource::new(
        &Shape::Point(Point::new(-170.0, 74.0).unwrap()),
        SphericalGridTarget::H3(Resolution::Zero),
    )
    .unwrap();
    assert!(!source.is_unknown());
    assert!(source.chains.is_empty());
    assert!(source.authority_edges.is_empty());
    assert!(source.selection_edges.is_empty());
    assert_eq!(source.authority_points.len(), 1);
    assert_eq!(source.selection_points.len(), 1);
    assert_eq!(source.authority_points[0].key.component, 0);
    assert_eq!(
        source.authority_points[0]
            .longitude
            .checked_estimate()
            .unwrap()
            .to_bits(),
        (-170.0_f64).to_bits()
    );
    assert_eq!(
        source.selection_points[0]
            .latitude
            .checked_estimate()
            .unwrap()
            .to_bits(),
        74.0_f64.to_bits()
    );
}

#[test]
fn full_and_half_world_edges_preserve_the_written_direction() {
    assert!(matches!(
        lifted_step(-180.0, 180.0).unwrap().kind,
        LiftKind::FullPositive
    ));
    assert!(matches!(
        lifted_step(180.0, -180.0).unwrap().kind,
        LiftKind::FullNegative
    ));
    assert_eq!(lifted_step(0.0, 180.0).unwrap().turn_delta, 0);
    assert_eq!(lifted_step(0.0, -180.0).unwrap().turn_delta, 0);
}

#[test]
fn full_edge_splits_symbolically_before_selection_wrap() {
    let edge = symbolic_edge(&[(-180.0, 0.0), (180.0, 0.0)], LiftKind::FullPositive);
    assert!(edge.parameters.iter().any(|parameter| {
        parameter.key == ParentParameterKey::Partition(PartitionConstraint::FullEdgeMidpoint)
            && parameter
                .ratio
                .ordering(&ExactRatio::from_i64(1, 2).unwrap())
                == Some(Ordering::Equal)
    }));
    let mut pieces = Vec::new();
    edge.selection_pieces(&mut pieces).unwrap();
    assert_eq!(pieces.len(), 2);
    assert!(pieces.iter().all(|piece| piece.latitude_zone == 0));
}

#[test]
fn fixed_capacity_handles_the_combined_full_edge_partition_determinant() {
    // These two fixed edges combine the widest parent operations: a
    // written full world with two interior pole reflections, then a
    // post-reflection longitude seam. Their successful exact ordering is
    // the deterministic capacity witness for the fixed 64-limb carrier;
    // any larger intermediate declines to `Unknown`, never guesses.
    let full = symbolic_edge(&[(-180.0, -270.0), (180.0, 270.0)], LiftKind::FullPositive);
    assert!(full.parameters.iter().any(|parameter| {
        parameter.key == ParentParameterKey::Partition(PartitionConstraint::FullEdgeMidpoint)
    }));
    assert!(full.parameters.iter().any(|parameter| {
        parameter.key == ParentParameterKey::Partition(PartitionConstraint::LatitudeReflection)
    }));
    let mut pieces = Vec::new();
    full.selection_pieces(&mut pieces).unwrap();
    let post = symbolic_edge(&[(-100.0, 100.0), (260.0, 110.0)], LiftKind::FullPositive);
    assert!(post.parameters.iter().any(|parameter| {
        parameter.key
            == ParentParameterKey::Partition(PartitionConstraint::PostReflectionLongitudeSeam)
    }));
    post.selection_pieces(&mut pieces).unwrap();
    assert!(pieces.len() >= 4);
}

#[test]
fn geographic_coverage_source_keeps_authority_and_selection_edges_separate() {
    let line = LineSeq::try_new(CoordSeq::from(vec![
        Point::new(-10.0, 0.0).unwrap(),
        Point::new(10.0, 0.0).unwrap(),
    ]))
    .unwrap();
    let source = GeographicCoverageSource::new(
        &Shape::LineString(line),
        SphericalGridTarget::H3(Resolution::Zero),
    )
    .unwrap();
    assert!(!source.is_unknown());
    assert_eq!(source.authority_edges.len(), 1);
    assert_eq!(source.selection_edges.len(), 1);
    assert_eq!(source.selection_arcs().unwrap().len(), 1);
    assert_eq!(source.authority_edges[0].endpoints[0].ordinal, 0);
    assert_eq!(source.authority_edges[0].endpoints[1].ordinal, 1);
}

#[test]
fn north_pole_neighbour_line_stays_in_a_certified_selection_strip() {
    // The exact stored-double neighbour below 90 must be a normal linear
    // source, not globally unknown because its outward bound rounds up to
    // the pole. The exact-pole sibling is deliberately a point carrier.
    let latitude = 90.0_f64.next_down();
    let line = LineSeq::try_new(CoordSeq::from(vec![
        Point::new(-10.0, latitude).unwrap(),
        Point::new(10.0, latitude).unwrap(),
    ]))
    .unwrap();
    let source = GeographicCoverageSource::new(
        &Shape::LineString(line),
        SphericalGridTarget::H3(Resolution::Two),
    )
    .unwrap();
    assert!(!source.is_unknown());
    assert_eq!(source.authority_edges.len(), 1);
    assert_eq!(source.selection_edges.len(), 1);
}

#[test]
fn geographic_coverage_source_keeps_checked_degree_two_rings_available() {
    let points = [
        Point::new(170.0, 10.0).unwrap(),
        Point::new(-170.0, 10.0).unwrap(),
        Point::new(0.0, 10.0).unwrap(),
        Point::new(170.0, 10.0).unwrap(),
        Point::new(-170.0, 10.0).unwrap(),
        Point::new(0.0, 10.0).unwrap(),
        Point::new(170.0, 10.0).unwrap(),
    ];
    let shape = Shape::Polygon(Polygon::new(
        Ring::from_closed(CoordSeq::from(points.to_vec())),
        Vec::new(),
    ));
    let source = GeographicCoverageSource::new(&shape, SphericalGridTarget::S2).unwrap();
    assert!(!source.is_unknown());
    assert_eq!(source.chains[0].degree, Some(2));
    assert_eq!(source.chains[0].direct_preimage_copies(175.0), Some(0..=1));
    let fill = source.ring_fills().next().unwrap();
    assert_eq!(fill.chain_index, 0);
    assert_eq!(fill.component, 0);
    assert_eq!(fill.ring, 0);
    assert_eq!(fill.degree, 2);
    let roof = fill
        .roof
        .as_ref()
        .expect("nonzero winding receives a finite pole roof");
    assert_eq!(roof.pole, PhysicalEndpointKey::NorthPole);
    assert_eq!(roof.turns, 2);
    assert_eq!(
        roof.span.checked_estimate().unwrap().to_bits(),
        720.0_f64.to_bits()
    );
    assert_eq!(fill.direct_preimage_copies(175.0), Some(0..=1));
    assert!(
        !fill.periodic_exact,
        "a two-turn lift is an outer periodic fill and may not certify interior"
    );
}

#[test]
fn geographic_coverage_source_fails_open_without_a_physical_pole_roof() {
    let points = [
        Point::new(-180.0, 0.0).unwrap(),
        Point::new(-60.0, 0.0).unwrap(),
        Point::new(60.0, 0.0).unwrap(),
        Point::new(-180.0, 0.0).unwrap(),
    ];
    let source = GeographicCoverageSource::new(
        &Shape::Polygon(Polygon::new(
            Ring::from_closed(CoordSeq::from(points.to_vec())),
            Vec::new(),
        )),
        SphericalGridTarget::H3(Resolution::Zero),
    )
    .unwrap();
    assert!(source.is_unknown());
    assert_eq!(source.ring_fills().len(), 0);
}

#[test]
fn periodic_pole_roof_blocks_raw_latitude_pruning() {
    // The roof follows the physical latitude hemisphere, not the direction
    // of the longitude walk. The candidate lies strictly inside each
    // analytic cap while being strictly disjoint from the raw ring latitude,
    // so removing the periodic roof from the rectangle proof makes this test
    // turn red.
    for (reverse, ring_latitude, latitude, degree, pole) in [
        (false, 60.0, 70.0, 1, PhysicalEndpointKey::NorthPole),
        (true, 60.0, 70.0, -1, PhysicalEndpointKey::NorthPole),
        (false, -60.0, -70.0, 1, PhysicalEndpointKey::SouthPole),
        (true, -60.0, -70.0, -1, PhysicalEndpointKey::SouthPole),
    ] {
        let source = GeographicCoverageSource::new(
            &Shape::Polygon(Polygon::new(
                periodic_latitude_ring(ring_latitude, reverse),
                Vec::new(),
            )),
            SphericalGridTarget::H3(Resolution::Two),
        )
        .unwrap();
        let fill = source.ring_fills().next().expect("one analytic shell");
        assert_eq!(fill.degree, degree);
        assert_eq!(
            fill.roof
                .as_ref()
                .expect("physical polar ring has a pole roof")
                .pole,
            pole
        );
        assert_eq!(
            source.authority_point_class(0.0, latitude),
            GridPointClass::Interior
        );
        let latitude = if latitude > 0.0 {
            Bound::new(69.0, 71.0).unwrap()
        } else {
            Bound::new(-71.0, -69.0).unwrap()
        };
        let candidate = DegreeWindowResult::Windows(CertifiedDegreeWindows {
            latitude,
            longitude: CertifiedLongitudeDegrees::One(Bound::new(-1.0, 1.0).unwrap()),
        });
        assert_ne!(source.classify_rect(candidate), RectClass::Outside);
    }
}

#[test]
fn non_exact_periodic_fill_demotes_a_planar_interior_to_unknown() {
    let points = [
        Point::new(-170.0, 80.0).unwrap(),
        Point::new(-60.0, 80.0).unwrap(),
        Point::new(60.0, 80.0).unwrap(),
        Point::new(170.0, 80.0).unwrap(),
        Point::new(-170.0, 80.0).unwrap(),
        Point::new(-60.0, 80.0).unwrap(),
        Point::new(60.0, 80.0).unwrap(),
        Point::new(170.0, 80.0).unwrap(),
        Point::new(-170.0, 80.0).unwrap(),
    ];
    let source = GeographicCoverageSource::new(
        &Shape::Polygon(Polygon::new(
            Ring::from_closed(CoordSeq::from(points.to_vec())),
            Vec::new(),
        )),
        SphericalGridTarget::H3(Resolution::Two),
    )
    .unwrap();
    let fill = &source.ring_fills[0];
    assert_eq!(fill.degree, 2);
    assert!(!fill.periodic_exact);
    let query = ExactPlanarPoint::from_stored(0.0, 85.0).unwrap();
    let DirectPreimages::Copies(copies) = source.chains[0].direct_preimages(0.0) else {
        panic!("the fixed two-turn cap has finite direct preimages");
    };
    assert_eq!(
        classify_lifted_ring_bucket(
            &source.chains[0],
            fill.y_index.as_ref(),
            fill.roof.as_ref(),
            &query,
            copies,
        ),
        GridPointClass::Interior,
        "the planar lift has an interior that is not yet a quotient proof"
    );
    assert_eq!(
        fill.point_class(&source.chains[0], 0.0, &query),
        GridPointClass::Unknown,
        "only the periodic-exact certificate may promote that interior"
    );
    assert_eq!(
        source.authority_point_class(0.0, 85.0),
        GridPointClass::Unknown
    );
}

#[test]
fn direct_lift_identity_blocks_collinear_long_path_coalescing() {
    let chain =
        LiftedChain::from_points(&[(0.0, 0.0), (100.0, 0.0), (-160.0, 0.0)], false).unwrap();
    assert_eq!(chain.vertices.len(), 3);
    assert_eq!(chain.vertices[2].longitude.turns, 1);
}

#[test]
fn direct_lift_identity_blocks_coalescing_across_a_full_world_edge() {
    let chain =
        LiftedChain::from_points(&[(-180.0, 0.0), (180.0, 0.0), (-160.0, 0.0)], false).unwrap();
    assert_eq!(chain.vertices.len(), 3);
    assert!(matches!(chain.edges[0], LiftKind::FullPositive));
    assert_eq!(chain.vertices[2].longitude.turns, 1);
}

#[test]
fn exact_collinear_middle_vertex_is_canonicalized_before_any_image() {
    let chain =
        LiftedChain::from_points(&[(0.0, 10.0), (10.0, 10.0), (20.0, 10.0)], false).unwrap();
    assert_eq!(chain.vertices.len(), 2);
    assert_eq!(chain.edges.len(), 1);
    assert_eq!(chain.vertices[0].longitude.raw.to_bits(), 0.0_f64.to_bits());
    assert_eq!(
        chain.vertices[1].longitude.raw.to_bits(),
        20.0_f64.to_bits()
    );
}

#[test]
fn multiwinding_ring_keeps_degree_two_and_all_direct_preimages() {
    let chain = LiftedChain::from_points(
        &[
            (170.0, 0.0),
            (-170.0, 0.0),
            (0.0, 0.0),
            (170.0, 0.0),
            (-170.0, 0.0),
            (0.0, 0.0),
            (170.0, 0.0),
        ],
        true,
    )
    .unwrap();
    assert_eq!(chain.degree, Some(2));
    assert_eq!(chain.direct_preimage_copies(175.0), Some(0..=1));
}

#[test]
fn multiwinding_ring_keeps_negative_degree_two_and_all_direct_preimages() {
    let chain = LiftedChain::from_points(
        &[
            (170.0, 0.0),
            (0.0, 0.0),
            (-170.0, 0.0),
            (170.0, 0.0),
            (0.0, 0.0),
            (-170.0, 0.0),
            (170.0, 0.0),
        ],
        true,
    )
    .unwrap();
    assert_eq!(chain.degree, Some(-2));
    assert_eq!(chain.direct_preimage_copies(175.0), Some(-2..=-1));
}

#[test]
fn geographic_coverage_source_records_a_physical_south_finite_negative_winding_roof() {
    let points = [
        Point::new(170.0, -10.0).unwrap(),
        Point::new(0.0, -10.0).unwrap(),
        Point::new(-170.0, -10.0).unwrap(),
        Point::new(170.0, -10.0).unwrap(),
        Point::new(0.0, -10.0).unwrap(),
        Point::new(-170.0, -10.0).unwrap(),
        Point::new(170.0, -10.0).unwrap(),
    ];
    let source = GeographicCoverageSource::new(
        &Shape::Polygon(Polygon::new(
            Ring::from_closed(CoordSeq::from(points.to_vec())),
            Vec::new(),
        )),
        SphericalGridTarget::S2,
    )
    .unwrap();
    assert!(!source.is_unknown());
    let fill = source.ring_fills().next().unwrap();
    assert_eq!(fill.degree, -2);
    let roof = fill
        .roof
        .as_ref()
        .expect("southern ring receives a finite pole roof");
    assert_eq!(roof.pole, PhysicalEndpointKey::SouthPole);
    assert_eq!(roof.turns, -2);
    assert_eq!(
        roof.span.checked_estimate().unwrap().to_bits(),
        (-720.0_f64).to_bits()
    );
    assert_eq!(fill.direct_preimage_copies(175.0), Some(-2..=-1));
}

#[test]
fn authority_point_location_respects_shell_hole_and_closed_boundary() {
    let shell = Ring::from_closed(CoordSeq::from(vec![
        Point::new(-10.0, -10.0).unwrap(),
        Point::new(10.0, -10.0).unwrap(),
        Point::new(10.0, 10.0).unwrap(),
        Point::new(-10.0, 10.0).unwrap(),
        Point::new(-10.0, -10.0).unwrap(),
    ]));
    let hole = Ring::from_closed(CoordSeq::from(vec![
        Point::new(-2.0, -2.0).unwrap(),
        Point::new(-2.0, 2.0).unwrap(),
        Point::new(2.0, 2.0).unwrap(),
        Point::new(2.0, -2.0).unwrap(),
        Point::new(-2.0, -2.0).unwrap(),
    ]));
    for (label, points) in [
        ("shell", [
            (-10.0, -10.0),
            (10.0, -10.0),
            (10.0, 10.0),
            (-10.0, 10.0),
            (-10.0, -10.0),
        ]),
        ("hole", [
            (-2.0, -2.0),
            (-2.0, 2.0),
            (2.0, 2.0),
            (2.0, -2.0),
            (-2.0, -2.0),
        ]),
    ] {
        for (ordinal, edge) in points.windows(2).enumerate() {
            let edge = symbolic_edge(edge, LiftKind::Shortest);
            let mut pieces = Vec::new();
            assert!(
                edge.selection_pieces(&mut pieces).is_some(),
                "{label} edge {ordinal}"
            );
        }
    }
    let source = GeographicCoverageSource::new(
        &Shape::Polygon(Polygon::new(shell, vec![hole])),
        SphericalGridTarget::H3(Resolution::Two),
    )
    .unwrap();
    assert!(!source.is_unknown());
    let query = ExactPlanarPoint::from_stored(3.0, 3.0).unwrap();
    assert_eq!(
        source.ring_fills[0].point_class(&source.chains[0], 3.0, &query),
        GridPointClass::Interior
    );
    assert_eq!(
        source.ring_fills[1].point_class(&source.chains[1], 3.0, &query),
        GridPointClass::Exterior
    );
    assert_eq!(
        source.ring_fills[0].direct_oracle_point_class(&source.chains[0], 3.0, &query),
        source.ring_fills[0].point_class(&source.chains[0], 3.0, &query),
        "the O(n) bucket evaluates the same finite periodic preimages as the direct oracle"
    );
    assert_eq!(
        source.ring_fills[1].direct_oracle_point_class(&source.chains[1], 3.0, &query),
        source.ring_fills[1].point_class(&source.chains[1], 3.0, &query),
        "an empty direct preimage remains a certified exterior in both paths"
    );
    assert_eq!(
        source.authority_point_class(3.0, 3.0),
        GridPointClass::Interior
    );
    assert_eq!(
        source.authority_point_class(0.0, 0.0),
        GridPointClass::Exterior
    );
    assert_eq!(
        source.authority_point_class(2.0, 0.0),
        GridPointClass::Boundary
    );
    assert_eq!(
        source.authority_point_class(11.0, 0.0),
        GridPointClass::Exterior
    );
}

#[test]
fn lifted_ring_classifier_matches_the_exhaustive_oracle_at_closed_y_boundaries() {
    let shell = vec![
        Point::new(-10.0, -10.0).unwrap(),
        Point::new(10.0, -10.0).unwrap(),
        Point::new(10.0, 10.0).unwrap(),
        Point::new(-10.0, 10.0).unwrap(),
        Point::new(-10.0, -10.0).unwrap(),
    ];
    let hole = vec![
        Point::new(-2.0, -2.0).unwrap(),
        Point::new(-2.0, 2.0).unwrap(),
        Point::new(2.0, 2.0).unwrap(),
        Point::new(2.0, -2.0).unwrap(),
        Point::new(-2.0, -2.0).unwrap(),
    ];
    let source = polygon_source(shell, vec![hole]);
    assert_indexed_matches_exhaustive(&source, &[
        (0.0, 0.0),
        (2.0, 0.0),
        (10.0, 0.0),
        (0.0, 10.0),
        (0.0, 10.0_f64.next_up()),
        (0.0, 10.0_f64.next_down()),
        (10.0, 10.0),
        (-180.0, 0.0),
        (180.0, 0.0),
    ]);

    let mut dense = Vec::new();
    for vertex in 0..64 {
        let angle = std::f64::consts::TAU * f64::from(vertex) / 64.0;
        dense.push(Point::new(80.0 * angle.cos(), 20.0 * angle.sin()).unwrap());
    }
    dense.push(dense[0]);
    let source = polygon_source(dense, Vec::new());
    assert_indexed_matches_exhaustive(&source, &[
        (0.0, 0.0),
        (0.0, 20.0),
        (0.0, 20.0_f64.next_up()),
        (0.0, 20.0_f64.next_down()),
        (-180.0, 0.0),
        (180.0, 0.0),
    ]);
}

#[test]
fn lifted_ring_classifier_matches_the_exhaustive_oracle_for_tall_edges_and_winding() {
    let mut tall = Vec::new();
    for vertex in 0..64 {
        let longitude = -80.0 + 160.0 * f64::from(vertex) / 64.0;
        let latitude = if vertex % 2 == 0 { -80.0 } else { 80.0 };
        tall.push(Point::new(longitude, latitude).unwrap());
    }
    tall.push(tall[0]);
    let source = polygon_source(tall, Vec::new());
    assert_indexed_matches_exhaustive(&source, &[
        (-100.0, 0.0),
        (0.0, 80.0),
        (0.0, 80.0_f64.next_up()),
        (0.0, 80.0_f64.next_down()),
        (-180.0, 0.0),
        (180.0, 0.0),
    ]);

    for (latitude, reverse) in [(80.0, false), (80.0, true), (-80.0, false), (-80.0, true)] {
        let points = if reverse {
            vec![
                (170.0, latitude),
                (0.0, latitude),
                (-170.0, latitude),
                (170.0, latitude),
                (0.0, latitude),
                (-170.0, latitude),
                (170.0, latitude),
            ]
        } else {
            vec![
                (170.0, latitude),
                (-170.0, latitude),
                (0.0, latitude),
                (170.0, latitude),
                (-170.0, latitude),
                (0.0, latitude),
                (170.0, latitude),
            ]
        };
        let source = polygon_source(
            points
                .into_iter()
                .map(|(longitude, latitude)| Point::new(longitude, latitude).unwrap())
                .collect(),
            Vec::new(),
        );
        let pole_query = if latitude > 0.0 { 85.0 } else { -85.0 };
        assert_indexed_matches_exhaustive(&source, &[
            (0.0, latitude),
            (0.0, pole_query),
            (-180.0, latitude),
            (180.0, latitude),
            (0.0, pole_query.next_up()),
            (0.0, pole_query.next_down()),
        ]);
    }
}

#[test]
fn lifted_ring_index_keeps_nonfinite_queries_and_chains_fail_open() {
    assert!(LiftedChain::from_points(&[], true).is_none());
    assert!(LiftedChain::from_points(&[(0.0, 0.0), (1.0, 1.0), (0.0, 0.0)], true).is_none());
    assert!(LiftedChain::from_points(&[(0.0, 0.0), (1.0, f64::NAN)], false).is_none());
    assert!(ExactPlanarPoint::from_stored(f64::INFINITY, 0.0).is_none());

    let source = polygon_source(
        vec![
            Point::new(-10.0, -10.0).unwrap(),
            Point::new(10.0, -10.0).unwrap(),
            Point::new(10.0, 10.0).unwrap(),
            Point::new(-10.0, 10.0).unwrap(),
            Point::new(-10.0, -10.0).unwrap(),
        ],
        Vec::new(),
    );
    assert_eq!(
        source.authority_point_class(f64::NAN, 0.0),
        GridPointClass::Unknown
    );
    assert_eq!(
        source.authority_point_class(0.0, f64::INFINITY),
        GridPointClass::Unknown
    );
}

#[test]
fn authority_point_location_is_invariant_to_polar_ring_winding() {
    let north = [
        Point::new(-170.0, 80.0).unwrap(),
        Point::new(-60.0, 80.0).unwrap(),
        Point::new(60.0, 80.0).unwrap(),
        Point::new(170.0, 80.0).unwrap(),
        Point::new(-170.0, 80.0).unwrap(),
    ];
    let forward = GeographicCoverageSource::new(
        &Shape::Polygon(Polygon::new(
            Ring::from_closed(CoordSeq::from(north.to_vec())),
            Vec::new(),
        )),
        SphericalGridTarget::H3(Resolution::Zero),
    )
    .unwrap();
    assert_eq!(
        forward.authority_point_class(0.0, 85.0),
        GridPointClass::Interior
    );
    assert_eq!(
        forward.authority_point_class(0.0, 0.0),
        GridPointClass::Exterior
    );
    let north_query = ExactPlanarPoint::from_stored(0.0, 85.0).unwrap();
    assert_eq!(
        forward.ring_fills[0].point_class(&forward.chains[0], 0.0, &north_query),
        forward.ring_fills[0].direct_oracle_point_class(&forward.chains[0], 0.0, &north_query),
        "the physical pole roof has identical bucket and direct-preimage parity"
    );

    let mut reversed = north.to_vec();
    reversed.reverse();
    let reverse = GeographicCoverageSource::new(
        &Shape::Polygon(Polygon::new(
            Ring::from_closed(CoordSeq::from(reversed)),
            Vec::new(),
        )),
        SphericalGridTarget::H3(Resolution::Zero),
    )
    .unwrap();
    assert_eq!(
        reverse.authority_point_class(0.0, 85.0),
        GridPointClass::Interior
    );
    assert_eq!(
        reverse.authority_point_class(0.0, -85.0),
        GridPointClass::Exterior
    );
    assert_eq!(
        reverse.authority_point_class(0.0, 0.0),
        GridPointClass::Exterior
    );
}

#[test]
fn exact_axis_rectangle_precedes_the_global_unknown_veto_for_cap_windows() {
    let rectangle = Shape::Polygon(Polygon::new(
        Ring::from_closed(CoordSeq::from(vec![
            Point::new(-180.0, 70.0).unwrap(),
            Point::new(180.0, 70.0).unwrap(),
            Point::new(180.0, 90.0).unwrap(),
            Point::new(-180.0, 90.0).unwrap(),
            Point::new(-180.0, 70.0).unwrap(),
        ])),
        Vec::new(),
    ));
    let source =
        GeographicCoverageSource::new(&rectangle, SphericalGridTarget::H3(Resolution::Three))
            .unwrap();
    assert!(
        source.is_unknown(),
        "the pole-cap rectangle enters the general carrier's fail-open state"
    );
    let full_longitude = CertifiedLongitudeDegrees::Full;
    let windows = |south, north| {
        DegreeWindowResult::Windows(CertifiedDegreeWindows {
            latitude: Bound::new(south, north).unwrap(),
            longitude: full_longitude,
        })
    };
    assert_eq!(
        source.classify_rect(windows(75.0, 80.0)),
        RectClass::Interior,
        "a full-longitude cap strictly inside the stored rectangle is a complete proof"
    );
    assert_eq!(
        source.classify_rect(windows(0.0, 60.0)),
        RectClass::Outside,
        "strict latitude separation still prunes under the unknown general lift"
    );
    assert_eq!(
        source.classify_rect(windows(60.0, 70.0)),
        RectClass::Boundary,
        "closed contact is retained rather than converted to a strict negative"
    );
}

#[test]
fn exact_axis_rectangle_requires_the_canonical_longitude_sheet() {
    let rectangle = |west, east| {
        Shape::Polygon(Polygon::new(
            Ring::from_closed(CoordSeq::from(vec![
                Point::new(west, -5.0).unwrap(),
                Point::new(east, -5.0).unwrap(),
                Point::new(east, 5.0).unwrap(),
                Point::new(west, 5.0).unwrap(),
                Point::new(west, -5.0).unwrap(),
            ])),
            Vec::new(),
        ))
    };
    let seam = GeographicCoverageSource::new(
        &rectangle(179.0, -179.0),
        SphericalGridTarget::H3(Resolution::Two),
    )
    .unwrap();
    assert!(
        seam.exact_axis_rect.is_none(),
        "the raw extrema name 358 degrees, while the retained lift is the 2-degree seam strip"
    );

    let ordinary = GeographicCoverageSource::new(
        &rectangle(-10.0, 10.0),
        SphericalGridTarget::H3(Resolution::Two),
    )
    .unwrap();
    assert!(
        ordinary.exact_axis_rect.is_some(),
        "a rectangle on the canonical longitude sheet remains a complete cap model"
    );

    let world = GeographicCoverageSource::new(
        &rectangle(-180.0, 180.0),
        SphericalGridTarget::H3(Resolution::Two),
    )
    .unwrap();
    assert!(
        world.exact_axis_rect.is_some(),
        "the written full-longitude domain is the one admitted wide source rectangle"
    );

    let unwrapped = GeographicCoverageSource::new(
        &rectangle(720.0, 1080.0),
        SphericalGridTarget::H3(Resolution::Two),
    )
    .unwrap();
    assert_eq!(
        unwrapped.exact_axis_rect, world.exact_axis_rect,
        "RingSummary gives equivalent full-longitude axis bands one canonical candidate window"
    );
}

#[test]
fn subdivided_parallel_edges_remain_an_exact_full_longitude_rectangle() {
    let shape = Shape::Polygon(Polygon::new(
        Ring::from_closed(CoordSeq::from(vec![
            Point::new(-180.0, 70.0).unwrap(),
            Point::new(-60.0, 70.0).unwrap(),
            Point::new(60.0, 70.0).unwrap(),
            Point::new(180.0, 70.0).unwrap(),
            Point::new(180.0, 80.0).unwrap(),
            Point::new(180.0, 90.0).unwrap(),
            Point::new(60.0, 90.0).unwrap(),
            Point::new(-60.0, 90.0).unwrap(),
            Point::new(-180.0, 90.0).unwrap(),
            Point::new(-180.0, 80.0).unwrap(),
            Point::new(-180.0, 70.0).unwrap(),
        ])),
        Vec::new(),
    ));
    let bounds = shape.bounds().unwrap();
    assert!(
        is_axis_aligned_rectangle(&shape, bounds),
        "the generic side-walk certificate admits geographic parallel subdivision"
    );
    assert!(
        subdivided_axis_aligned_rectangle(&shape, bounds),
        "each exact segment advances once around the four rectangle sides"
    );
    let source =
        GeographicCoverageSource::new(&shape, SphericalGridTarget::H3(Resolution::Three)).unwrap();
    assert!(
        source.exact_axis_rect.is_some(),
        "the cap fast path receives the full-longitude rectangle despite its collinear vertices"
    );
}

#[test]
fn exact_axis_rectangle_requires_an_ordered_directed_boundary_walk() {
    let polygon = |points| {
        Shape::Polygon(Polygon::new(
            Ring::from_closed(CoordSeq::from(points)),
            Vec::new(),
        ))
    };
    let bow_tie = polygon(vec![
        Point::new(-10.0, -5.0).unwrap(),
        Point::new(10.0, 5.0).unwrap(),
        Point::new(10.0, -5.0).unwrap(),
        Point::new(-10.0, 5.0).unwrap(),
        Point::new(-10.0, -5.0).unwrap(),
    ]);
    let bow_tie_bounds = bow_tie.bounds().unwrap();
    assert!(
        !exact_axis_rectangle(&bow_tie, bow_tie_bounds),
        "a corner set is not a side-ordered rectangle certificate"
    );

    let seam_zigzag = polygon(vec![
        Point::new(-180.0, 70.0).unwrap(),
        Point::new(170.0, 70.0).unwrap(),
        Point::new(-170.0, 70.0).unwrap(),
        Point::new(180.0, 70.0).unwrap(),
        Point::new(180.0, 90.0).unwrap(),
        Point::new(-170.0, 90.0).unwrap(),
        Point::new(170.0, 90.0).unwrap(),
        Point::new(-180.0, 90.0).unwrap(),
        Point::new(-180.0, 70.0).unwrap(),
    ]);
    let seam_zigzag_bounds = seam_zigzag.bounds().unwrap();
    assert!(
        !exact_axis_rectangle(&seam_zigzag, seam_zigzag_bounds),
        "a shortest-lift seam zigzag is not the written full-world side"
    );
}

#[test]
fn lifted_edge_window_search_reaches_its_exact_periodic_copy() {
    let start = ExactPlanarPoint::from_stored(720.0, 50.0).unwrap();
    let end = ExactPlanarPoint::from_stored(890.0, 60.0).unwrap();
    let longitude = Bound::new(84.0, 86.0).unwrap();
    let latitude = Bound::new(54.0, 56.0).unwrap();
    assert!(
        exact_segment_touches_rectangle(&start, &end, longitude, latitude).unwrap(),
        "the lifted 720..890 edge meets the third periodic copy of 84..86"
    );
}

#[test]
fn dense_collinear_chain_reduction_is_stack_safe() {
    let points: Vec<_> = (0..4_000)
        .map(|index| (-20.0 + 40.0 * f64::from(index) / 3_999.0, 10.0))
        .collect();
    let chain = LiftedChain::from_points(&points, false).unwrap();
    assert_eq!(chain.vertices.len(), 2);
    assert_eq!(chain.edges.len(), 1);
    assert_eq!(
        chain.vertices[0].longitude.raw.to_bits(),
        (-20.0_f64).to_bits()
    );
    assert_eq!(
        chain.vertices[1].longitude.raw.to_bits(),
        20.0_f64.to_bits()
    );
}

#[test]
fn incremental_reduction_preserves_seam_zigzags_and_full_turn_edges() {
    let seam = LiftedChain::from_points(
        &[(-180.0, 0.0), (170.0, 0.0), (-170.0, 0.0), (180.0, 0.0)],
        false,
    )
    .unwrap();
    assert_eq!(seam.vertices.len(), 4);
    assert_eq!(
        seam.vertices
            .iter()
            .map(|vertex| vertex.longitude.turns)
            .collect::<Vec<_>>(),
        vec![0, -1, 0, -1]
    );

    let full =
        LiftedChain::from_points(&[(-180.0, 0.0), (180.0, 0.0), (-180.0, 0.0)], false).unwrap();
    assert_eq!(full.vertices.len(), 3);
    assert_eq!(full.edges.as_ref(), &[
        LiftKind::FullPositive,
        LiftKind::FullNegative
    ]);
}

#[test]
fn incremental_reduction_preserves_a_dense_multiwinding_closure_degree() {
    let chain = LiftedChain::from_points(
        &[
            (170.0, 0.0),
            (180.0, 0.0),
            (-170.0, 0.0),
            (0.0, 0.0),
            (170.0, 0.0),
            (180.0, 0.0),
            (-170.0, 0.0),
            (0.0, 0.0),
            (170.0, 0.0),
        ],
        true,
    )
    .unwrap();
    assert_eq!(chain.degree, Some(2));
    assert_eq!(chain.vertices.len(), 8);
    assert_eq!(chain.direct_preimage_copies(175.0), Some(0..=1));
}

#[test]
fn authority_edge_index_matches_exhaustive_exact_classification() {
    let points: Vec<_> = (0..4_000)
        .map(|index| {
            let x = -100.0 + 200.0 * f64::from(index) / 3_999.0;
            let y = if index % 2 == 0 { 20.0 } else { 20.1 };
            Point::new(x, y).unwrap()
        })
        .collect();
    let source = GeographicCoverageSource::new(
        &Shape::LineString(LineSeq::try_new(CoordSeq::from(points)).unwrap()),
        SphericalGridTarget::H3(Resolution::Two),
    )
    .unwrap();
    assert!(source.authority_edge_index.is_some());
    assert_eq!(source.chains[0].vertices.len(), 4_000);

    let longitude = Bound::new(-1.0, 1.0).unwrap();
    let latitude = Bound::new(-1.0, 1.0).unwrap();
    reset_authority_edge_exact_refinements();
    let indexed = source.classify_rect_window(longitude, latitude);
    let indexed_visits = authority_edge_exact_refinements();
    reset_authority_edge_exact_refinements();
    let exhaustive = source.classify_rect_window_exhaustive(longitude, latitude);
    let exhaustive_visits = authority_edge_exact_refinements();
    assert_eq!(indexed, exhaustive);
    assert_eq!(indexed, RectClass::Outside);
    assert_eq!(indexed_visits, 0);
    assert_eq!(exhaustive_visits, source.authority_edges.len());

    let longitude = Bound::new(-1.0, 1.0).unwrap();
    let latitude = Bound::new(19.9, 20.2).unwrap();
    reset_authority_edge_exact_refinements();
    let indexed = source.classify_rect_window(longitude, latitude);
    let indexed_visits = authority_edge_exact_refinements();
    reset_authority_edge_exact_refinements();
    let exhaustive = source.classify_rect_window_exhaustive(longitude, latitude);
    let exhaustive_visits = authority_edge_exact_refinements();
    assert_eq!(indexed, exhaustive);
    assert_eq!(indexed, RectClass::Boundary);
    assert!(indexed_visits < exhaustive_visits);
}

#[test]
fn authority_edge_index_matches_polar_cap_interior() {
    let source = GeographicCoverageSource::new(
        &Shape::Polygon(Polygon::new(
            Ring::from_closed(CoordSeq::from(vec![
                Point::new(-60.0, 70.0).unwrap(),
                Point::new(0.0, 80.0).unwrap(),
                Point::new(60.0, 70.0).unwrap(),
                Point::new(-60.0, 70.0).unwrap(),
            ])),
            Vec::new(),
        )),
        SphericalGridTarget::H3(Resolution::Two),
    )
    .unwrap();
    assert!(source.authority_edge_index.is_some());
    let longitude = Bound::new(-10.0, 10.0).unwrap();
    let latitude = Bound::new(74.0, 75.0).unwrap();
    reset_authority_edge_exact_refinements();
    let indexed = source.classify_rect_window(longitude, latitude);
    let indexed_visits = authority_edge_exact_refinements();
    reset_authority_edge_exact_refinements();
    let exhaustive = source.classify_rect_window_exhaustive(longitude, latitude);
    let exhaustive_visits = authority_edge_exact_refinements();
    assert_eq!(indexed, exhaustive);
    assert_eq!(indexed, RectClass::Interior);
    assert!(indexed_visits < exhaustive_visits);
}

#[test]
fn authority_edge_index_matches_seam_endpoint_contact_on_both_sheets() {
    let source = GeographicCoverageSource::new(
        &Shape::LineString(
            LineSeq::try_new(CoordSeq::from(vec![
                Point::new(179.0, 0.0).unwrap(),
                Point::new(-179.0, 0.0).unwrap(),
            ]))
            .unwrap(),
        ),
        SphericalGridTarget::H3(Resolution::Two),
    )
    .unwrap();
    assert!(source.authority_edge_index.is_some());
    for longitude in [
        Bound::new(-179.0, -179.0).unwrap(),
        Bound::new(179.0, 179.0).unwrap(),
    ] {
        let latitude = Bound::new(0.0, 0.0).unwrap();
        reset_authority_edge_exact_refinements();
        let indexed = source.classify_rect_window(longitude, latitude);
        let indexed_visits = authority_edge_exact_refinements();
        reset_authority_edge_exact_refinements();
        let exhaustive = source.classify_rect_window_exhaustive(longitude, latitude);
        let exhaustive_visits = authority_edge_exact_refinements();
        assert_eq!(indexed, exhaustive);
        assert_eq!(indexed, RectClass::Boundary);
        assert_eq!(indexed_visits, 1);
        assert_eq!(exhaustive_visits, 1);
    }
}

#[test]
fn authority_edge_index_matches_an_unwrapped_longitude_alias() {
    let source = GeographicCoverageSource::new(
        &Shape::LineString(
            LineSeq::try_new(CoordSeq::from(vec![
                Point::new(720.0, 50.0).unwrap(),
                Point::new(890.0, 60.0).unwrap(),
            ]))
            .unwrap(),
        ),
        SphericalGridTarget::H3(Resolution::Two),
    )
    .unwrap();
    assert!(source.authority_edge_index.is_some());
    let longitude = Bound::new(84.0, 86.0).unwrap();
    let latitude = Bound::new(54.0, 56.0).unwrap();
    reset_authority_edge_exact_refinements();
    let indexed = source.classify_rect_window(longitude, latitude);
    let indexed_visits = authority_edge_exact_refinements();
    reset_authority_edge_exact_refinements();
    let exhaustive = source.classify_rect_window_exhaustive(longitude, latitude);
    let exhaustive_visits = authority_edge_exact_refinements();
    assert_eq!(indexed, exhaustive);
    assert_eq!(indexed, RectClass::Boundary);
    assert_eq!(indexed_visits, 1);
    assert_eq!(exhaustive_visits, 1);
}

#[test]
fn authority_edge_index_deduplicates_full_turn_sheet_hits() {
    let source = GeographicCoverageSource::new(
        &Shape::LineString(
            LineSeq::try_new(CoordSeq::from(vec![
                Point::new(-180.0, 0.0).unwrap(),
                Point::new(180.0, 0.0).unwrap(),
            ]))
            .unwrap(),
        ),
        SphericalGridTarget::H3(Resolution::Two),
    )
    .unwrap();
    assert!(source.authority_edge_index.is_some());
    let longitude = Bound::new(-180.0, 180.0).unwrap();
    let latitude = Bound::new(-1.0, 1.0).unwrap();
    reset_authority_edge_exact_refinements();
    let indexed = source.classify_rect_window(longitude, latitude);
    let indexed_visits = authority_edge_exact_refinements();
    reset_authority_edge_exact_refinements();
    let exhaustive = source.classify_rect_window_exhaustive(longitude, latitude);
    let exhaustive_visits = authority_edge_exact_refinements();
    assert_eq!(indexed, exhaustive);
    assert_eq!(indexed, RectClass::Boundary);
    assert_eq!(indexed_visits, 1);
    assert_eq!(exhaustive_visits, 1);
}

#[test]
fn unindexable_authority_edge_selects_the_exhaustive_fallback() {
    let mut edge = unpartitioned_symbolic_edge(&[(0.0, 0.0), (1.0, 1.0)]);
    edge.start.longitude.raw = f64::MAX;
    edge.end.longitude.raw = f64::MAX;
    assert!(authority_edge_proxy_envelopes(&edge).is_none());
    assert!(
        build_authority_edge_index(std::slice::from_ref(&edge)).is_none()
    );

    let source = GeographicCoverageSource {
        target: SphericalGridTarget::H3(Resolution::Two),
        exact_axis_rect: None,
        authority_latitude: None,
        chains: Vec::new().into_boxed_slice(),
        ring_fills: Vec::new(),
        authority_points: Vec::new(),
        selection_points: Vec::new(),
        authority_edges: vec![edge],
        authority_edge_index: None,
        authority_pieces: Vec::new(),
        selection_edges: Vec::new(),
        unknown: false,
    };
    reset_authority_edge_exact_refinements();
    assert_eq!(
        source.classify_rect_window(
            Bound::new(-1.0, 1.0).unwrap(),
            Bound::new(-1.0, 1.0).unwrap()
        ),
        RectClass::Boundary
    );
    assert_eq!(authority_edge_exact_refinements(), 1);
}
