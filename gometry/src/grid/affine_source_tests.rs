use std::cmp::Ordering;

use h3o::Resolution;

use crate::geometry::{CoordSeq, LineSeq, Point, Polygon, Ring, Shape, is_axis_aligned_rectangle};
use crate::grid::affine_source::{
    AuthorityCarrier, CompactLimbs, DirectPreimages, EXPANSION_LIMBS, ExactExpansion, ExactFailure,
    ExactParentParameter, ExactPlanarCoordinates as _, ExactPlanarPoint, ExactRatio,
    FULL_TURN_DEGREES, GeographicCoverageSource, GridPointClass, INLINE_EXPANSION_LIMBS, LiftKind,
    LiftedChain, LiftedLongitude, ParentCrossing, ParentParameterKey, PartitionConstraint,
    RectClass, SelectionCarrier, SourceEdgeKey, SourceVertexKey, SphericalGridTarget,
    SymbolicAffineEdge, authority_edge_exact_refinements, authority_edge_proxy_envelopes,
    build_authority_edge_index, classify_lifted_ring_bucket, closed_between,
    closed_between_expansion, closed_between_raw, combine_polygon_point_classes,
    exact_affine_level, exact_axis_rectangle, exact_segment_touches_rectangle,
    integer_levels_between, lifted_step, rectangular_candidate_admissions,
    reset_authority_edge_exact_refinements, reset_rectangular_candidate_admissions,
    selection_closed_between, subdivided_axis_aligned_rectangle,
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
    Ring::from_closed(CoordSeq::try_from_points(&points).unwrap()).unwrap()
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
    let Some(query) = ExactPlanarPoint::from_stored(longitude, latitude).ok() else {
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
            source
                .authority_point_class(longitude, latitude)
                .expect("exact point classification"),
            exhaustive_authority_point_class(source, longitude, latitude),
            "source point ({longitude}, {latitude})",
        );
        let Some(query) = ExactPlanarPoint::from_stored(longitude, latitude).ok() else {
            continue;
        };
        for shell in &source.ring_fills {
            let Some(chain) = source.chains.get(shell.chain_index) else {
                panic!("ring fill has no authoritative chain");
            };
            assert_eq!(
                shell
                    .point_class(chain, longitude, &query)
                    .expect("exact ring classification"),
                shell.direct_oracle_point_class(chain, longitude, &query),
                "ring {}/{} point ({longitude}, {latitude})",
                shell.component,
                shell.ring,
            );
        }
    }
}

fn polygon_source(shell: &[Point], holes: Vec<Vec<Point>>) -> GeographicCoverageSource {
    GeographicCoverageSource::new(
        &Shape::Polygon(
            Polygon::new(
                Ring::from_closed(CoordSeq::try_from_points(shell).unwrap()).unwrap(),
                holes
                    .into_iter()
                    .map(|hole| {
                        Ring::from_closed(CoordSeq::try_from_points(&hole).unwrap()).unwrap()
                    })
                    .collect(),
            )
            .unwrap(),
        ),
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
    let start_point = ExactPlanarPoint::from_lifted(edge.start).unwrap();
    let end_point = ExactPlanarPoint::from_lifted(edge.end).unwrap();
    let longitude_start = start_point.longitude();
    let longitude_end = end_point.longitude();
    assert_eq!(
        integer_levels_between(longitude_start, longitude_end, 180, 360).ok(),
        Some(-2..=0)
    );
    let seam_crossing = edge
        .parameter_for(
            longitude_start,
            longitude_end,
            &ExactExpansion::from_i64(-180).unwrap(),
        )
        .unwrap();
    let ParentCrossing::Present(seam_ratio) = seam_crossing else {
        panic!("the -180 degree lifted seam must cross this full edge");
    };
    ExactParentParameter::new(
        ParentParameterKey::Partition(PartitionConstraint::LiftedLongitudeSeam),
        seam_ratio,
    )
    .unwrap();
    for level in integer_levels_between(longitude_start, longitude_end, 180, 360).unwrap() {
        let seam = exact_affine_level(180, 360, level).unwrap();
        edge.parameter_for(longitude_start, longitude_end, &seam)
            .unwrap_or_else(|_| panic!("level {level}"));
    }
    edge.collect_lifted_longitude_seams(&start_point, &end_point)
        .unwrap();
    edge.collect_latitude_reflections(&start_point, &end_point)
        .unwrap();
    assert!(edge.parameters.iter().any(|parameter| {
        parameter.key == ParentParameterKey::Partition(PartitionConstraint::LiftedLongitudeSeam)
    }));
    assert!(edge.parameters.iter().any(|parameter| {
        parameter.key == ParentParameterKey::Partition(PartitionConstraint::LatitudeReflection)
    }));
    let mut post_reflection = unpartitioned_symbolic_edge(&[(-100.0, 100.0), (260.0, 110.0)]);
    let post_start = ExactPlanarPoint::from_lifted(post_reflection.start).unwrap();
    let post_end = ExactPlanarPoint::from_lifted(post_reflection.end).unwrap();
    post_reflection
        .collect_post_reflection_longitude_seams(&post_start, &post_end)
        .unwrap();
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
    assert_eq!(ratio(lower).ordering(&third), Ok(Ordering::Less));
    assert_eq!(ratio(upper).ordering(&third), Ok(Ordering::Greater));
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
    storage.prepare(EXPANSION_LIMBS + 1).unwrap_err();

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
fn compact_exact_heap_promotion_is_fallibly_reserved() {
    let ((), allocations) = crate::test_support::allocation_count(|| {
        let mut storage = CompactLimbs::single(1.0);
        storage
            .prepare(INLINE_EXPANSION_LIMBS + 1)
            .expect("heap promotion");
        assert!(matches!(storage, CompactLimbs::Heap(_)));
    });
    assert!(
        allocations > 0,
        "the direct control must enter the former heap-promotion allocation"
    );

    let error = crate::test_support::fail_allocations_after(0, || {
        let mut storage = CompactLimbs::single(1.0);
        storage
            .prepare(INLINE_EXPANSION_LIMBS + 1)
            .expect_err("allocation failure must remain recoverable")
    });
    assert_eq!(error, ExactFailure::Allocation);
}

#[test]
fn expansion_sub_cancels_unit_plus_half_ulp_without_a_scratch_clone() {
    // 1 - 2^-53 is one binary64. 1 + 2^-53 is not; add then sub must recover
    // the half-ulp limb, including empty-right identity.
    let unit = ExactExpansion::from_f64(1.0).unwrap();
    let half_ulp = ExactExpansion::from_f64(2.0_f64.powi(-53)).unwrap();
    let sum = unit.add(&half_ulp).unwrap();
    assert!(sum.as_f64().is_none());
    let recovered = sum.sub(&unit).unwrap();
    assert_eq!(recovered.is_exact(2.0_f64.powi(-53)), Ok(true));
    let unchanged = sum.sub(&ExactExpansion::zero()).unwrap();
    assert_eq!(unchanged.ordering(), sum.ordering());
    assert_eq!(unchanged.is_exact(1.0), Ok(false));
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
    ExactParentParameter::start().unwrap();
    ExactParentParameter::end().unwrap();
    ExactParentParameter::new(
        ParentParameterKey::Partition(PartitionConstraint::FullEdgeMidpoint),
        ExactRatio::from_i64(1, 2).unwrap(),
    )
    .unwrap();
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
        Ok(Ordering::Equal)
    );
    let start_point = ExactPlanarPoint::from_lifted(edge.start).unwrap();
    let end_point = ExactPlanarPoint::from_lifted(edge.end).unwrap();
    let position = edge
        .coordinate_at(&split.ratio, start_point.longitude(), end_point.longitude())
        .unwrap();
    let lower = ExactRatio::from_f64(f64::from_bits(0x402A_AAAA_AAAA_AAAA)).unwrap();
    let upper = ExactRatio::from_f64(f64::from_bits(0x402A_AAAA_AAAA_AAAB)).unwrap();
    assert_eq!(position.ordering(&lower), Ok(Ordering::Greater));
    assert_eq!(position.ordering(&upper), Ok(Ordering::Less));
    let enclosure = position.certified_bound().unwrap();
    assert_ne!(
        position.ordering(&ExactRatio::from_f64(enclosure.lo).unwrap()),
        Ok(Ordering::Less)
    );
    assert_ne!(
        position.ordering(&ExactRatio::from_f64(enclosure.hi).unwrap()),
        Ok(Ordering::Greater)
    );
    let mut pieces = Vec::new();
    edge.selection_pieces(&mut pieces).unwrap();
    assert!(pieces.iter().any(|piece| {
        piece.interval.end.key
            == ParentParameterKey::Partition(PartitionConstraint::LatitudeReflection)
            && piece.interval.end.ratio.ordering(&split.ratio) == Ok(Ordering::Equal)
    }));
}

#[test]
fn geographic_coverage_source_canonicalizes_accepted_pole_rounding_before_wrapping() {
    let input = CoordSeq::try_from_points(&[
        Point::new(0.0, 90.0_f64.next_up()).unwrap(),
        Point::new(180.0_f64.next_up(), 0.0).unwrap(),
    ])
    .unwrap();
    let source = GeographicCoverageSource::new(
        &Shape::MultiPoint(input.into()),
        SphericalGridTarget::H3(Resolution::Zero),
    )
    .unwrap();
    assert!(!source.is_unknown());
    assert_eq!(source.points.len(), 2);
    assert!(
        source
            .points
            .iter()
            .all(|record| record.selection.is_some())
    );
    let over_north = &source.points[0].selection.as_ref().unwrap().point;
    assert_eq!(
        source.points[0]
            .authority
            .point
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

    let over_east = &source.points[1].selection.as_ref().unwrap().point;
    assert_eq!(
        over_east.longitude.checked_estimate().unwrap().to_bits(),
        (180.0_f64.next_up() - FULL_TURN_DEGREES).to_bits()
    );
    assert_eq!(
        over_east.latitude.checked_estimate().unwrap().to_bits(),
        0.0_f64.to_bits()
    );
    assert_eq!(
        source.points[0]
            .selection
            .as_ref()
            .unwrap()
            .point
            .key
            .component,
        0
    );
    assert_eq!(
        source.points[1]
            .selection
            .as_ref()
            .unwrap()
            .point
            .key
            .ordinal,
        1
    );
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
    assert!(source.authority.edges.is_empty());
    assert!(source.selection.edges.is_empty());
    assert_eq!(source.points.len(), 1);
    assert!(source.points[0].selection.is_some());
    assert_eq!(source.points[0].authority.point.key.component, 0);
    assert_eq!(
        source.points[0]
            .authority
            .point
            .longitude
            .checked_estimate()
            .unwrap()
            .to_bits(),
        (-170.0_f64).to_bits()
    );
    assert_eq!(
        source.points[0]
            .selection
            .as_ref()
            .unwrap()
            .point
            .latitude
            .checked_estimate()
            .unwrap()
            .to_bits(),
        74.0_f64.to_bits()
    );
}

#[test]
fn admitted_split_rectangle_is_prepared_once_before_both_window_classifications() {
    let source = GeographicCoverageSource::new(
        &Shape::Point(Point::new(0.0, 0.0).unwrap()),
        SphericalGridTarget::H3(Resolution::Zero),
    )
    .unwrap();
    let windows = DegreeWindowResult::Windows(CertifiedDegreeWindows {
        latitude: Bound::new(-1.0, 1.0).unwrap(),
        longitude: CertifiedLongitudeDegrees::Two([
            Bound::new(170.0, 180.0).unwrap(),
            Bound::new(-180.0, -170.0).unwrap(),
        ]),
    });

    reset_rectangular_candidate_admissions();
    let candidate = source.admit_rectangular_candidate(windows);
    assert_eq!(candidate.windows(), windows);
    assert_eq!(
        source
            .classify_rect(&candidate)
            .expect("exact split rectangle classification"),
        RectClass::Outside
    );
    assert_eq!(
        rectangular_candidate_admissions(),
        1,
        "the split windows share one admitted candidate"
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
fn closure_degree_accepts_both_antimeridian_alias_directions() {
    let positive = LiftedChain::from_points(
        &[(-180.0, 0.0), (-90.0, 10.0), (0.0, 0.0), (180.0, 0.0)],
        true,
    )
    .unwrap();
    assert_eq!(positive.degree, Some(1));

    let negative = LiftedChain::from_points(
        &[(180.0, 0.0), (90.0, 10.0), (0.0, 0.0), (-180.0, 0.0)],
        true,
    )
    .unwrap();
    assert_eq!(negative.degree, Some(-1));
}

#[test]
fn full_edge_splits_symbolically_before_selection_wrap() {
    let edge = symbolic_edge(&[(-180.0, 0.0), (180.0, 0.0)], LiftKind::FullPositive);
    assert!(edge.parameters.iter().any(|parameter| {
        parameter.key == ParentParameterKey::Partition(PartitionConstraint::FullEdgeMidpoint)
            && parameter
                .ratio
                .ordering(&ExactRatio::from_i64(1, 2).unwrap())
                == Ok(Ordering::Equal)
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
    let line = LineSeq::try_new(
        CoordSeq::try_from_points(&[
            Point::new(-10.0, 0.0).unwrap(),
            Point::new(10.0, 0.0).unwrap(),
        ])
        .unwrap(),
    )
    .unwrap();
    let source = GeographicCoverageSource::new(
        &Shape::LineString(line),
        SphericalGridTarget::H3(Resolution::Zero),
    )
    .unwrap();
    assert!(!source.is_unknown());
    assert_eq!(source.authority.edges.len(), 1);
    assert_eq!(source.selection.edges.len(), 1);
    assert_eq!(source.selection_arcs().unwrap().len(), 1);
    assert_eq!(source.authority.edges[0].endpoints[0].ordinal, 0);
    assert_eq!(source.authority.edges[0].endpoints[1].ordinal, 1);
}

#[test]
fn north_pole_neighbour_line_stays_in_a_certified_selection_strip() {
    // The exact stored-double neighbour below 90 must be a normal linear
    // source, not globally unknown because its outward bound rounds up to
    // the pole. The exact-pole sibling is deliberately a point carrier.
    let latitude = 90.0_f64.next_down();
    let line = LineSeq::try_new(
        CoordSeq::try_from_points(&[
            Point::new(-10.0, latitude).unwrap(),
            Point::new(10.0, latitude).unwrap(),
        ])
        .unwrap(),
    )
    .unwrap();
    let source = GeographicCoverageSource::new(
        &Shape::LineString(line),
        SphericalGridTarget::H3(Resolution::Two),
    )
    .unwrap();
    assert!(!source.is_unknown());
    assert_eq!(source.authority.edges.len(), 1);
    assert_eq!(source.selection.edges.len(), 1);
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
    let shape = Shape::Polygon(
        Polygon::new(
            Ring::from_closed(CoordSeq::try_from_points(points.as_ref()).unwrap()).unwrap(),
            Vec::new(),
        )
        .unwrap(),
    );
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
        &Shape::Polygon(
            Polygon::new(
                Ring::from_closed(CoordSeq::try_from_points(points.as_ref()).unwrap()).unwrap(),
                Vec::new(),
            )
            .unwrap(),
        ),
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
            &Shape::Polygon(
                Polygon::new(periodic_latitude_ring(ring_latitude, reverse), Vec::new()).unwrap(),
            ),
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
            source
                .authority_point_class(0.0, latitude)
                .expect("exact point classification"),
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
        assert_ne!(
            source
                .classify_rect(&source.admit_rectangular_candidate(candidate))
                .expect("exact rectangle classification"),
            RectClass::Outside
        );
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
        &Shape::Polygon(
            Polygon::new(
                Ring::from_closed(CoordSeq::try_from_points(points.as_ref()).unwrap()).unwrap(),
                Vec::new(),
            )
            .unwrap(),
        ),
        SphericalGridTarget::H3(Resolution::Two),
    )
    .unwrap();
    let fill = &source.ring_fills[0];
    assert_eq!(fill.degree, 2);
    assert!(!fill.periodic_exact);
    let query = ExactPlanarPoint::from_stored(0.0, 85.0).unwrap();
    let DirectPreimages::Copies(copies) = source.chains[0]
        .direct_preimages(0.0)
        .expect("direct preimages")
    else {
        panic!("the fixed two-turn cap has finite direct preimages");
    };
    assert_eq!(
        classify_lifted_ring_bucket(
            &source.chains[0],
            fill.y_index.as_ref(),
            fill.roof.as_ref(),
            &query,
            copies,
        )
        .expect("exact ring classification"),
        GridPointClass::Interior,
        "the planar lift has an interior that is not yet a quotient proof"
    );
    assert_eq!(
        fill.point_class(&source.chains[0], 0.0, &query)
            .expect("exact ring classification"),
        GridPointClass::Unknown,
        "only the periodic-exact certificate may promote that interior"
    );
    assert_eq!(
        source
            .authority_point_class(0.0, 85.0)
            .expect("exact point classification"),
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
        &Shape::Polygon(
            Polygon::new(
                Ring::from_closed(CoordSeq::try_from_points(points.as_ref()).unwrap()).unwrap(),
                Vec::new(),
            )
            .unwrap(),
        ),
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
    let shell = Ring::from_closed(
        CoordSeq::try_from_points(&[
            Point::new(-10.0, -10.0).unwrap(),
            Point::new(10.0, -10.0).unwrap(),
            Point::new(10.0, 10.0).unwrap(),
            Point::new(-10.0, 10.0).unwrap(),
            Point::new(-10.0, -10.0).unwrap(),
        ])
        .unwrap(),
    )
    .unwrap();
    let hole = Ring::from_closed(
        CoordSeq::try_from_points(&[
            Point::new(-2.0, -2.0).unwrap(),
            Point::new(-2.0, 2.0).unwrap(),
            Point::new(2.0, 2.0).unwrap(),
            Point::new(2.0, -2.0).unwrap(),
            Point::new(-2.0, -2.0).unwrap(),
        ])
        .unwrap(),
    )
    .unwrap();
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
            edge.selection_pieces(&mut pieces)
                .unwrap_or_else(|_| panic!("{label} edge {ordinal}"));
        }
    }
    let source = GeographicCoverageSource::new(
        &Shape::Polygon(Polygon::new(shell, vec![hole]).unwrap()),
        SphericalGridTarget::H3(Resolution::Two),
    )
    .unwrap();
    assert!(!source.is_unknown());
    let query = ExactPlanarPoint::from_stored(3.0, 3.0).unwrap();
    assert_eq!(
        source.ring_fills[0]
            .point_class(&source.chains[0], 3.0, &query)
            .expect("exact ring classification"),
        GridPointClass::Interior
    );
    assert_eq!(
        source.ring_fills[1]
            .point_class(&source.chains[1], 3.0, &query)
            .expect("exact ring classification"),
        GridPointClass::Exterior
    );
    assert_eq!(
        source.ring_fills[0].direct_oracle_point_class(&source.chains[0], 3.0, &query),
        source.ring_fills[0]
            .point_class(&source.chains[0], 3.0, &query)
            .expect("exact ring classification"),
        "the O(n) bucket evaluates the same finite periodic preimages as the direct oracle"
    );
    assert_eq!(
        source.ring_fills[1].direct_oracle_point_class(&source.chains[1], 3.0, &query),
        source.ring_fills[1]
            .point_class(&source.chains[1], 3.0, &query)
            .expect("exact ring classification"),
        "an empty direct preimage remains a certified exterior in both paths"
    );
    assert_eq!(
        source
            .authority_point_class(3.0, 3.0)
            .expect("exact point classification"),
        GridPointClass::Interior
    );
    assert_eq!(
        source
            .authority_point_class(0.0, 0.0)
            .expect("exact point classification"),
        GridPointClass::Exterior
    );
    assert_eq!(
        source
            .authority_point_class(2.0, 0.0)
            .expect("exact point classification"),
        GridPointClass::Boundary
    );
    assert_eq!(
        source
            .authority_point_class(11.0, 0.0)
            .expect("exact point classification"),
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
    let source = polygon_source(&shell, vec![hole]);
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
    let source = polygon_source(&dense, Vec::new());
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
    let source = polygon_source(&tall, Vec::new());
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
        let ring: Vec<_> = points
            .into_iter()
            .map(|(longitude, latitude)| Point::new(longitude, latitude).unwrap())
            .collect();
        let source = polygon_source(&ring, Vec::new());
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
    LiftedChain::from_points(&[], true).unwrap_err();
    LiftedChain::from_points(&[(0.0, 0.0), (1.0, 1.0), (0.0, 0.0)], true).unwrap_err();
    LiftedChain::from_points(&[(0.0, 0.0), (1.0, f64::NAN)], false).unwrap_err();
    ExactPlanarPoint::from_stored(f64::INFINITY, 0.0).unwrap_err();

    let source = polygon_source(
        &[
            Point::new(-10.0, -10.0).unwrap(),
            Point::new(10.0, -10.0).unwrap(),
            Point::new(10.0, 10.0).unwrap(),
            Point::new(-10.0, 10.0).unwrap(),
            Point::new(-10.0, -10.0).unwrap(),
        ],
        Vec::new(),
    );
    assert_eq!(
        source
            .authority_point_class(f64::NAN, 0.0)
            .expect("invalid query remains fail-open"),
        GridPointClass::Unknown
    );
    assert_eq!(
        source
            .authority_point_class(0.0, f64::INFINITY)
            .expect("invalid query remains fail-open"),
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
        &Shape::Polygon(
            Polygon::new(
                Ring::from_closed(CoordSeq::try_from_points(north.as_ref()).unwrap()).unwrap(),
                Vec::new(),
            )
            .unwrap(),
        ),
        SphericalGridTarget::H3(Resolution::Zero),
    )
    .unwrap();
    assert_eq!(
        forward
            .authority_point_class(0.0, 85.0)
            .expect("exact point classification"),
        GridPointClass::Interior
    );
    assert_eq!(
        forward
            .authority_point_class(0.0, 0.0)
            .expect("exact point classification"),
        GridPointClass::Exterior
    );
    let north_query = ExactPlanarPoint::from_stored(0.0, 85.0).unwrap();
    assert_eq!(
        forward.ring_fills[0]
            .point_class(&forward.chains[0], 0.0, &north_query)
            .expect("exact ring classification"),
        forward.ring_fills[0].direct_oracle_point_class(&forward.chains[0], 0.0, &north_query),
        "the physical pole roof has identical bucket and direct-preimage parity"
    );

    let mut reversed = north.to_vec();
    reversed.reverse();
    let reverse = GeographicCoverageSource::new(
        &Shape::Polygon(
            Polygon::new(
                Ring::from_closed(CoordSeq::try_from_points(&reversed).unwrap()).unwrap(),
                Vec::new(),
            )
            .unwrap(),
        ),
        SphericalGridTarget::H3(Resolution::Zero),
    )
    .unwrap();
    assert_eq!(
        reverse
            .authority_point_class(0.0, 85.0)
            .expect("exact point classification"),
        GridPointClass::Interior
    );
    assert_eq!(
        reverse
            .authority_point_class(0.0, -85.0)
            .expect("exact point classification"),
        GridPointClass::Exterior
    );
    assert_eq!(
        reverse
            .authority_point_class(0.0, 0.0)
            .expect("exact point classification"),
        GridPointClass::Exterior
    );
}

#[test]
fn exact_axis_rectangle_precedes_the_global_unknown_veto_for_cap_windows() {
    let rectangle = Shape::Polygon(
        Polygon::new(
            Ring::from_closed(
                CoordSeq::try_from_points(&[
                    Point::new(-180.0, 70.0).unwrap(),
                    Point::new(180.0, 70.0).unwrap(),
                    Point::new(180.0, 90.0).unwrap(),
                    Point::new(-180.0, 90.0).unwrap(),
                    Point::new(-180.0, 70.0).unwrap(),
                ])
                .unwrap(),
            )
            .unwrap(),
            Vec::new(),
        )
        .unwrap(),
    );
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
        source
            .classify_rect(&source.admit_rectangular_candidate(windows(75.0, 80.0)))
            .expect("exact rectangle classification"),
        RectClass::Interior,
        "a full-longitude cap strictly inside the stored rectangle is a complete proof"
    );
    assert_eq!(
        source
            .classify_rect(&source.admit_rectangular_candidate(windows(0.0, 60.0)))
            .expect("exact rectangle classification"),
        RectClass::Outside,
        "strict latitude separation still prunes under the unknown general lift"
    );
    assert_eq!(
        source
            .classify_rect(&source.admit_rectangular_candidate(windows(60.0, 70.0)))
            .expect("exact rectangle classification"),
        RectClass::Boundary,
        "closed contact is retained rather than converted to a strict negative"
    );
}

#[test]
fn exact_axis_rectangle_requires_the_canonical_longitude_sheet() {
    let rectangle = |west, east| {
        Shape::Polygon(
            Polygon::new(
                Ring::from_closed(
                    CoordSeq::try_from_points(&[
                        Point::new(west, -5.0).unwrap(),
                        Point::new(east, -5.0).unwrap(),
                        Point::new(east, 5.0).unwrap(),
                        Point::new(west, 5.0).unwrap(),
                        Point::new(west, -5.0).unwrap(),
                    ])
                    .unwrap(),
                )
                .unwrap(),
                Vec::new(),
            )
            .unwrap(),
        )
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
    let shape = Shape::Polygon(
        Polygon::new(
            Ring::from_closed(
                CoordSeq::try_from_points(&[
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
                ])
                .unwrap(),
            )
            .unwrap(),
            Vec::new(),
        )
        .unwrap(),
    );
    let bounds = shape.bounds().unwrap();
    assert!(
        is_axis_aligned_rectangle(&shape, bounds),
        "the generic side-walk certificate admits geographic parallel subdivision"
    );
    assert!(
        subdivided_axis_aligned_rectangle(&shape, bounds).unwrap(),
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
    let polygon = |points: Vec<Point>| {
        Shape::Polygon(
            Polygon::new(
                Ring::from_closed(CoordSeq::try_from_points(&points).unwrap()).unwrap(),
                Vec::new(),
            )
            .unwrap(),
        )
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
        !exact_axis_rectangle(&bow_tie, bow_tie_bounds).unwrap(),
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
        !exact_axis_rectangle(&seam_zigzag, seam_zigzag_bounds).unwrap(),
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
    assert_eq!(full.edges.as_slice(), &[
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
        &Shape::LineString(LineSeq::try_new(CoordSeq::try_from_points(&points).unwrap()).unwrap()),
        SphericalGridTarget::H3(Resolution::Two),
    )
    .unwrap();
    assert!(source.authority.edge_index.is_some());
    assert_eq!(source.chains[0].vertices.len(), 4_000);

    // The indexed classification is checked against the exhaustive one. The
    // far query also proves the packed BVH prunes the large source before any
    // exact edge refinement.
    let longitude = Bound::new(-1.0, 1.0).unwrap();
    let latitude = Bound::new(-1.0, 1.0).unwrap();
    reset_authority_edge_exact_refinements();
    let indexed = source
        .classify_rect_window(longitude, latitude)
        .expect("exact rectangle classification");
    let indexed_refinements = authority_edge_exact_refinements();
    reset_authority_edge_exact_refinements();
    let exhaustive = source.classify_rect_window_exhaustive(longitude, latitude);
    let exhaustive_refinements = authority_edge_exact_refinements();
    assert_eq!(indexed, exhaustive);
    assert_eq!(indexed, RectClass::Outside);
    assert_eq!(indexed_refinements, 0);
    assert_eq!(exhaustive_refinements, 3_999);

    let longitude = Bound::new(-1.0, 1.0).unwrap();
    let latitude = Bound::new(19.9, 20.2).unwrap();
    let indexed = source
        .classify_rect_window(longitude, latitude)
        .expect("exact rectangle classification");
    let exhaustive = source.classify_rect_window_exhaustive(longitude, latitude);
    assert_eq!(indexed, exhaustive);
    assert_eq!(indexed, RectClass::Boundary);
}

#[test]
fn authority_edge_index_matches_polar_cap_interior() {
    let source = GeographicCoverageSource::new(
        &Shape::Polygon(
            Polygon::new(
                Ring::from_closed(
                    CoordSeq::try_from_points(&[
                        Point::new(-60.0, 70.0).unwrap(),
                        Point::new(0.0, 80.0).unwrap(),
                        Point::new(60.0, 70.0).unwrap(),
                        Point::new(-60.0, 70.0).unwrap(),
                    ])
                    .unwrap(),
                )
                .unwrap(),
                Vec::new(),
            )
            .unwrap(),
        ),
        SphericalGridTarget::H3(Resolution::Two),
    )
    .unwrap();
    assert!(source.authority.edge_index.is_some());
    let longitude = Bound::new(-10.0, 10.0).unwrap();
    let latitude = Bound::new(74.0, 75.0).unwrap();
    let indexed = source
        .classify_rect_window(longitude, latitude)
        .expect("exact rectangle classification");
    let exhaustive = source.classify_rect_window_exhaustive(longitude, latitude);
    assert_eq!(indexed, exhaustive);
    assert_eq!(indexed, RectClass::Interior);
}

#[test]
fn authority_edge_index_matches_seam_endpoint_contact_on_both_sheets() {
    let source = GeographicCoverageSource::new(
        &Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    Point::new(179.0, 0.0).unwrap(),
                    Point::new(-179.0, 0.0).unwrap(),
                ])
                .unwrap(),
            )
            .unwrap(),
        ),
        SphericalGridTarget::H3(Resolution::Two),
    )
    .unwrap();
    assert!(source.authority.edge_index.is_some());
    for longitude in [
        Bound::new(-179.0, -179.0).unwrap(),
        Bound::new(179.0, 179.0).unwrap(),
    ] {
        let latitude = Bound::new(0.0, 0.0).unwrap();
        let indexed = source
            .classify_rect_window(longitude, latitude)
            .expect("exact rectangle classification");
        let exhaustive = source.classify_rect_window_exhaustive(longitude, latitude);
        assert_eq!(indexed, exhaustive);
        assert_eq!(indexed, RectClass::Boundary);
    }
}

#[test]
fn authority_edge_index_matches_an_unwrapped_longitude_alias() {
    let source = GeographicCoverageSource::new(
        &Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    Point::new(720.0, 50.0).unwrap(),
                    Point::new(890.0, 60.0).unwrap(),
                ])
                .unwrap(),
            )
            .unwrap(),
        ),
        SphericalGridTarget::H3(Resolution::Two),
    )
    .unwrap();
    assert!(source.authority.edge_index.is_some());
    let longitude = Bound::new(84.0, 86.0).unwrap();
    let latitude = Bound::new(54.0, 56.0).unwrap();
    let indexed = source
        .classify_rect_window(longitude, latitude)
        .expect("exact rectangle classification");
    let exhaustive = source.classify_rect_window_exhaustive(longitude, latitude);
    assert_eq!(indexed, exhaustive);
    assert_eq!(indexed, RectClass::Boundary);
}

#[test]
fn authority_edge_index_deduplicates_full_turn_sheet_hits() {
    let source = GeographicCoverageSource::new(
        &Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    Point::new(-180.0, 0.0).unwrap(),
                    Point::new(180.0, 0.0).unwrap(),
                ])
                .unwrap(),
            )
            .unwrap(),
        ),
        SphericalGridTarget::H3(Resolution::Two),
    )
    .unwrap();
    assert!(source.authority.edge_index.is_some());
    let longitude = Bound::new(-180.0, 180.0).unwrap();
    let latitude = Bound::new(-1.0, 1.0).unwrap();
    let indexed = source
        .classify_rect_window(longitude, latitude)
        .expect("exact rectangle classification");
    let exhaustive = source.classify_rect_window_exhaustive(longitude, latitude);
    assert_eq!(indexed, exhaustive);
    assert_eq!(indexed, RectClass::Boundary);
}

#[test]
fn unindexable_authority_edge_selects_the_exhaustive_fallback() {
    let mut edge = unpartitioned_symbolic_edge(&[(0.0, 0.0), (1.0, 1.0)]);
    edge.start.longitude.raw = f64::MAX;
    edge.end.longitude.raw = f64::MAX;
    authority_edge_proxy_envelopes(&edge).unwrap_err();
    assert!(
        build_authority_edge_index(std::slice::from_ref(&edge))
            .unwrap()
            .is_none()
    );

    let source = GeographicCoverageSource {
        target: SphericalGridTarget::H3(Resolution::Two),
        exact_axis_rect: None,
        authority_latitude: None,
        chains: Vec::new(),
        ring_fills: Vec::new(),
        points: Vec::new(),
        authority: AuthorityCarrier {
            edges: vec![edge],
            edge_index: None,
            pieces: Vec::new(),
        },
        selection: SelectionCarrier {
            edges: Vec::new(),
            poles: Vec::new(),
        },
        unknown: false,
    };
    assert_eq!(
        source
            .classify_rect_window(
                Bound::new(-1.0, 1.0).unwrap(),
                Bound::new(-1.0, 1.0).unwrap()
            )
            .expect("exact rectangle classification"),
        RectClass::Boundary
    );
}

fn interval_oracle(left: f64, middle: f64, right: f64) -> bool {
    let lo = left.min(right);
    let hi = left.max(right);
    lo <= middle && middle <= hi
}

fn assert_all_oracles(left: f64, middle: f64, right: f64, expected: Option<bool>) {
    let expansion = closed_between_expansion(
        &ExactExpansion::from_f64(left).unwrap(),
        &ExactExpansion::from_f64(middle).unwrap(),
        &ExactExpansion::from_f64(right).unwrap(),
    );
    let raw = closed_between_raw(left, middle, right);
    let lifted = closed_between(
        LiftedLongitude {
            raw: left,
            turns: 0,
        },
        LiftedLongitude {
            raw: middle,
            turns: 0,
        },
        LiftedLongitude {
            raw: right,
            turns: 0,
        },
    );
    let ratio = selection_closed_between(
        &ExactRatio::from_f64(left).unwrap(),
        &ExactRatio::from_f64(middle).unwrap(),
        &ExactRatio::from_f64(right).unwrap(),
    );
    assert_eq!(
        expansion.ok(),
        expected,
        "expansion {left} {middle} {right}"
    );
    assert_eq!(raw.ok(), expected, "raw {left} {middle} {right}");
    assert_eq!(lifted.ok(), expected, "lifted {left} {middle} {right}");
    assert_eq!(ratio.ok(), expected, "ratio {left} {middle} {right}");
}

#[test]
fn closed_between_matches_integer_minmax_on_all_interval_oracles() {
    let values = [-2.0, -0.0, 0.0, 1.0, 2.0, 5.0];
    for &left in &values {
        for &middle in &values {
            for &right in &values {
                assert_all_oracles(
                    left,
                    middle,
                    right,
                    Some(interval_oracle(left, middle, right)),
                );
            }
        }
    }
}

#[test]
fn closed_between_certifies_overflowing_endpoint_span() {
    assert_all_oracles(1e308, 0.0, -1e308, Some(true));
    assert_all_oracles(-1e308, 0.0, 1e308, Some(true));
    assert_all_oracles(1e308, 1e308, -1e308, None);
    assert_all_oracles(-1e308, -1e308, 1e308, None);
}
