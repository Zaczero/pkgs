use crate::geometry::XY;
use crate::geometry::exact_arith::ExactSign;
use crate::geometry::segments::stored_orientation_sign;
use crate::geometry::tessellation::delaunay::take_constrained_output_reserves;
use crate::geometry::tessellation::shape::{
    BudgetedPackedTriangleSink, BudgetedTriangleSink, canonical_voronoi_sites,
    take_delaunay_output_reserves,
};

#[test]
fn exact_segment_rejects_projective_point_at_infinity() {
    let horizontal = super::exact::ExactLine::through_points(
        &super::exact::ExactPoint::from_xy(XY::new(0.0, 0.0)),
        &super::exact::ExactPoint::from_xy(XY::new(1.0, 0.0)),
    )
    .expect("distinct segment");
    let parallel = super::exact::ExactLine::through_points(
        &super::exact::ExactPoint::from_xy(XY::new(0.0, 1.0)),
        &super::exact::ExactPoint::from_xy(XY::new(1.0, 1.0)),
    )
    .expect("distinct segment");
    let infinity = super::exact::line_intersection(&horizontal, &parallel).unwrap();
    let start = super::exact::ExactPoint::from_xy(XY::new(0.0, 0.0));
    let end = super::exact::ExactPoint::from_xy(XY::new(1.0, 0.0));

    assert!(!infinity.is_finite());
    assert!(matches!(
        super::exact::segment_intersection(&start, &end, &infinity, &infinity),
        Ok(super::exact::SegmentIntersection::None)
    ));
}
use crate::error::ErrorKind;
use crate::geometry::segments::Orientation;
use crate::geometry::tessellation::delaunay::cleanup_constraint_lines;
use crate::geometry::tessellation::earcut::{
    cleaned_earcut_ring, open_ring, validate_earcut_ring, validate_hole_interactions,
};
use crate::geometry::{
    CdtRefinement, CoordSeq, CoordinateAxes, Coordinates as _, ExpansionBudget,
    GENERATED_ITEM_LIMIT, GeometryErrorKind, LineSeq, MOrdinate, MultiMember, MultiPolygon, Point,
    Polygon, Ring, Shape, Strictness, ZOrdinate, open_point_cycle_magnitude, orientation,
    ring_contains_interior,
};

fn xy(x: f64, y: f64) -> Point {
    Point::new(x, y).expect("finite test coordinate")
}

fn xyz(x: f64, y: f64, z: f64) -> Point {
    Point::new_axes(x, y, ZOrdinate(Some(z)), MOrdinate(None)).expect("finite test coordinate")
}

fn xym(x: f64, y: f64, m: f64) -> Point {
    Point::new_axes(x, y, ZOrdinate(None), MOrdinate(Some(m))).expect("finite test coordinate")
}

fn vertex_key(point: Point) -> (u64, u64, Option<u64>, Option<u64>) {
    (
        point.x.to_bits(),
        point.y.to_bits(),
        point.z().map(f64::to_bits),
        point.m().map(f64::to_bits),
    )
}

fn brute_snap_sites(sites: &[super::Site], radius: f64) -> Vec<super::Site> {
    let mut survivors: Vec<super::Site> = Vec::new();
    for site in sites {
        let mut snapped = false;
        for survivor in &survivors {
            if super::exact::distance_within(survivor.point.xy(), site.point.xy(), radius)
                .expect("finite binary64 site distances stay within exact dyadic bounds")
            {
                snapped = true;
                break;
            }
        }
        if !snapped {
            survivors.push(*site);
        }
    }
    survivors
}

fn assert_snap_matches_brute(sites: &[super::Site], radius: f64) -> Vec<super::Site> {
    let expected = brute_snap_sites(sites, radius);
    let actual = super::snap_sites(sites, radius).unwrap();
    let signature = |sites: &[super::Site]| {
        sites
            .iter()
            .map(|site| (site.id, site.point.x.to_bits(), site.point.y.to_bits()))
            .collect::<Vec<_>>()
    };
    assert_eq!(signature(&actual), signature(&expected));
    actual
}

#[test]
fn canonical_voronoi_sites_normalize_zero_and_globally_deduplicate_snaps() {
    let input = vec![
        xy(-0.0, 0.0),
        xy(0.0, 1.0),
        xy(0.09, 0.0),
        xy(0.18, 0.0),
        xy(1.0, 0.0),
    ];
    let restored = canonical_voronoi_sites(input.clone(), 0.1).unwrap();
    let mut reversed = input;
    reversed.reverse();
    let mutated_order = canonical_voronoi_sites(reversed, 0.1).unwrap();
    let signature = |sites: &[super::Site]| {
        sites
            .iter()
            .map(|site| (site.point.x.to_bits(), site.point.y.to_bits()))
            .collect::<Vec<_>>()
    };
    assert_eq!(signature(&restored), signature(&mutated_order));
    assert_eq!(restored.len(), 4);
    assert_eq!(restored[0].point.x.to_bits(), 0.0_f64.to_bits());
    assert_eq!(
        restored.iter().map(|site| site.id).collect::<Vec<_>>(),
        vec![0, 1, 2, 3]
    );
}

#[test]
fn voronoi_snap_matches_exact_reference_at_strict_radius_neighbors() {
    let below = 1.0_f64.next_down();
    let above = 1.0_f64.next_up();
    let sites = [
        super::Site {
            id: 0,
            point: xy(0.0, 0.0),
        },
        super::Site {
            id: 1,
            point: xy(0.0, below),
        },
        super::Site {
            id: 2,
            point: xy(0.0, 1.0),
        },
        super::Site {
            id: 3,
            point: xy(0.0, above),
        },
    ];

    assert_eq!(
        assert_snap_matches_brute(&sites, 1.0)
            .iter()
            .map(|site| site.id)
            .collect::<Vec<_>>(),
        [0, 2]
    );
}

#[test]
fn voronoi_snap_matches_exact_reference_at_subnormal_and_extreme_bounds() {
    let minimum = f64::from_bits(1);
    let subnormal_sites = [
        super::Site {
            id: 0,
            point: xy(0.0, 0.0),
        },
        super::Site {
            id: 1,
            point: xy(minimum, 0.0),
        },
        super::Site {
            id: 2,
            point: xy(2.0 * minimum, 0.0),
        },
    ];
    assert_eq!(
        assert_snap_matches_brute(&subnormal_sites, minimum)
            .iter()
            .map(|site| site.id)
            .collect::<Vec<_>>(),
        [0, 1, 2]
    );

    let maximum = f64::MAX;
    let overflowing_y_sites = [
        super::Site {
            id: 0,
            point: xy(0.0, -maximum),
        },
        super::Site {
            id: 1,
            point: xy(0.0, maximum),
        },
    ];
    assert_eq!(
        assert_snap_matches_brute(&overflowing_y_sites, maximum)
            .iter()
            .map(|site| site.id)
            .collect::<Vec<_>>(),
        [0, 1]
    );

    let opposite_x_sites = [
        super::Site {
            id: 0,
            point: xy(-maximum, 0.0),
        },
        super::Site {
            id: 1,
            point: xy(0.0, 0.0),
        },
        super::Site {
            id: 2,
            point: xy(maximum, 0.0),
        },
    ];
    assert_eq!(
        assert_snap_matches_brute(&opposite_x_sites, maximum)
            .iter()
            .map(|site| site.id)
            .collect::<Vec<_>>(),
        [0, 1, 2]
    );
}

#[test]
fn voronoi_snap_matches_exact_reference_when_y_order_differs() {
    let sites = [
        super::Site {
            id: 0,
            point: xy(0.0, 0.8),
        },
        super::Site {
            id: 1,
            point: xy(0.8, 0.0),
        },
        super::Site {
            id: 2,
            point: xy(0.81, 0.4),
        },
    ];

    assert_eq!(
        assert_snap_matches_brute(&sites, 1.0)
            .iter()
            .map(|site| site.id)
            .collect::<Vec<_>>(),
        [0, 1]
    );
}

#[test]
fn canonical_voronoi_keeps_4097_equal_x_sparse_y_sites_in_dense_id_order() {
    let points = (0..4_097)
        .map(|site| xy(0.0, 2.0 * f64::from(site)))
        .collect::<Vec<_>>();
    let unsnapped = canonical_voronoi_sites(points.clone(), 0.0).unwrap();
    let snapped = canonical_voronoi_sites(points, 1.0).unwrap();
    let signature = |sites: &[super::Site]| {
        sites
            .iter()
            .map(|site| (site.id, site.point.x.to_bits(), site.point.y.to_bits()))
            .collect::<Vec<_>>()
    };

    assert_eq!(signature(&snapped), signature(&unsnapped));
    assert_eq!(
        snapped.iter().map(|site| site.id).collect::<Vec<_>>(),
        (0..4_097).collect::<Vec<_>>()
    );
}

#[test]
fn exact_snap_does_not_use_a_rounded_axis_rejection() {
    let sites = [
        super::Site {
            id: 0,
            point: xy(0.0, 1e20),
        },
        super::Site {
            id: 1,
            point: xy(0.0, 1.0),
        },
    ];
    assert_eq!(
        (sites[0].point.y - sites[1].point.y).abs().to_bits(),
        1e20_f64.to_bits()
    );
    let survivors = super::snap_sites(&sites, 1e20).unwrap();
    assert_eq!(survivors.iter().map(|site| site.id).collect::<Vec<_>>(), [
        0
    ]);
}

#[test]
fn public_delaunay_keeps_collinear_empty_contract() {
    let source = Shape::MultiPoint(
        crate::geometry::CoordSeq::try_from_points(&[xy(0.0, 0.0), xy(1.0, 0.0), xy(2.0, 0.0)])
            .unwrap()
            .into(),
    );
    assert!(source.delaunay_triangles().unwrap().is_empty());
}

fn polygon(shell: Vec<Point>, holes: Vec<Vec<Point>>) -> Polygon {
    Polygon::new(
        Ring::closed(shell).expect("valid test shell"),
        holes
            .into_iter()
            .map(|hole| Ring::closed(hole).expect("valid test hole"))
            .collect(),
    )
}

fn assert_triangulation_error(polygon: Polygon) {
    let error = Shape::Polygon(polygon)
        .polygon_triangles()
        .expect_err("invalid polygon must fail earcut");
    assert!(matches!(
        error.kind(),
        ErrorKind::Geometry(GeometryErrorKind::Triangulation(_))
    ));
}

#[test]
fn triangle_sink_rejects_before_shape_allocation() {
    let mut budget = ExpansionBudget::new("triangulate", "method");
    budget.add(GENERATED_ITEM_LIMIT).unwrap();
    let mut sink = BudgetedTriangleSink::new(&mut budget);
    sink.emit(xy(0.0, 0.0), xy(1.0, 0.0), xy(0.0, 1.0))
        .unwrap_err();
    assert!(sink.into_shapes().is_empty());
}

#[test]
fn packed_triangle_sink_rejects_before_allocation() {
    let mut budget = ExpansionBudget::new("triangulate", "method");
    budget.add(GENERATED_ITEM_LIMIT).unwrap();
    let mut sink = BudgetedPackedTriangleSink::new(&mut budget);
    sink.emit(xy(0.0, 0.0), xy(1.0, 0.0), xy(0.0, 1.0))
        .unwrap_err();
    assert!(sink.into_vertices().is_empty());
}

#[test]
fn earcut_keeps_first_duplicate_xy_ordinates() {
    let source = Shape::Polygon(polygon(
        vec![
            xyz(0.0, 0.0, 1.0),
            xyz(0.0, 0.0, 2.0),
            xyz(1.0, 0.0, 3.0),
            xyz(0.0, 1.0, 4.0),
            xyz(0.0, 0.0, 1.0),
        ],
        Vec::new(),
    ));
    let triangles = source
        .polygon_triangles()
        .expect("duplicate XY triangulates");
    assert!(!triangles.is_empty());
    let mut saw_origin = false;
    for triangle in &triangles {
        for corner in triangle_corners(triangle).expect("triangle polygon") {
            if corner.x == 0.0 && corner.y == 0.0 {
                saw_origin = true;
                assert_eq!(corner.z(), Some(1.0));
            }
        }
    }
    assert!(saw_origin);
}

#[test]
fn earcut_keeps_first_duplicate_xy_m_ordinate() {
    let source = Shape::Polygon(polygon(
        vec![
            xym(0.0, 0.0, 1.0),
            xym(0.0, 0.0, 2.0),
            xym(1.0, 0.0, 3.0),
            xym(0.0, 1.0, 4.0),
            xym(0.0, 0.0, 1.0),
        ],
        Vec::new(),
    ));
    let triangles = source
        .polygon_triangles()
        .expect("duplicate XY triangulates");
    assert!(!triangles.is_empty());
    let mut saw_origin = false;
    for triangle in &triangles {
        for corner in triangle_corners(triangle).expect("triangle polygon") {
            if corner.x == 0.0 && corner.y == 0.0 {
                saw_origin = true;
                assert_eq!(corner.m(), Some(1.0));
            }
        }
    }
    assert!(saw_origin);
}

#[test]
fn earcut_multipolygon_parts_keep_distinct_z_at_shared_xy() {
    let left = polygon(
        vec![
            xyz(0.0, 0.0, 1.0),
            xyz(1.0, 0.0, 1.0),
            xyz(0.0, 1.0, 1.0),
            xyz(0.0, 0.0, 1.0),
        ],
        Vec::new(),
    );
    let right = polygon(
        vec![
            xyz(0.0, 0.0, 9.0),
            xyz(2.0, 0.0, 9.0),
            xyz(0.0, 2.0, 9.0),
            xyz(0.0, 0.0, 9.0),
        ],
        Vec::new(),
    );
    let source = Shape::MultiPolygon(
        MultiPolygon::try_new(CoordinateAxes::XYZ, vec![
            MultiMember::Value(left),
            MultiMember::Value(right),
        ])
        .expect("homogeneous XYZ parts"),
    );
    let triangles = source.polygon_triangles().expect("parts triangulate");
    let zs: Vec<f64> = triangles
        .iter()
        .flat_map(|triangle| triangle_corners(triangle).expect("triangle polygon"))
        .filter(|corner| corner.x == 0.0 && corner.y == 0.0)
        .map(|corner| corner.z().expect("origin keeps Z"))
        .collect();
    assert!(zs.contains(&1.0), "left part origin z=1: {zs:?}");
    assert!(zs.contains(&9.0), "right part origin z=9: {zs:?}");
}

#[test]
fn earcut_donut_bridge_keeps_source_ordinates() {
    let source = Shape::Polygon(polygon(
        vec![
            xyz(0.0, 0.0, 1.0),
            xyz(4.0, 0.0, 2.0),
            xyz(4.0, 4.0, 3.0),
            xyz(0.0, 4.0, 4.0),
            xyz(0.0, 0.0, 1.0),
        ],
        vec![vec![
            xyz(1.0, 1.0, 5.0),
            xyz(2.0, 1.0, 6.0),
            xyz(1.0, 2.0, 7.0),
            xyz(1.0, 1.0, 5.0),
        ]],
    ));
    let mut keys = std::collections::HashSet::new();
    if let Shape::Polygon(polygon) = &source {
        for ring in polygon.rings() {
            for point in ring.iter_coords() {
                keys.insert(vertex_key(point));
            }
        }
    }
    let triangles = source.polygon_triangles().expect("donut triangulates");
    assert!(!triangles.is_empty());
    for triangle in &triangles {
        for corner in triangle_corners(triangle).expect("triangle polygon") {
            assert!(
                keys.contains(&vertex_key(corner)),
                "corner {corner:?} was not a source vertex"
            );
        }
    }
}

fn xy_bits(point: Point) -> (u64, u64) {
    (
        crate::geometry::canonical_f64_bits(point.x),
        crate::geometry::canonical_f64_bits(point.y),
    )
}

/// Unique-XY source map taken from the input rings before clip. First-kept
/// on a duplicate would still win; this fixture must not repeat XY.
fn unique_xy_source_map(polygon: &Polygon) -> std::collections::HashMap<(u64, u64), Point> {
    let mut map = std::collections::HashMap::new();
    for ring in polygon.rings() {
        for point in open_ring(ring.coords()) {
            let key = xy_bits(point);
            assert!(
                map.insert(key, point).is_none(),
                "mixed-ring fixture XY must be unique, got {point:?}"
            );
        }
    }
    map
}

fn mixed_ring_polygon() -> Polygon {
    // Distinct shell Z so a fabricated constant cannot pass. Hole is XY in
    // the lower-left. Ring starts at the (8,3)/(10,4)/(8,5) convex bump so
    // that shell-only ear is clipped before the hole fan.
    polygon(
        vec![
            xyz(8.0, 3.0, 3.0),
            xyz(10.0, 4.0, 4.0),
            xyz(8.0, 5.0, 5.0),
            xyz(8.0, 8.0, 6.0),
            xyz(0.0, 8.0, 7.0),
            xyz(0.0, 0.0, 1.0),
            xyz(8.0, 0.0, 2.0),
            xyz(8.0, 3.0, 3.0),
        ],
        vec![vec![xy(1.0, 1.0), xy(2.0, 1.0), xy(1.0, 2.0), xy(1.0, 1.0)]],
    )
}

#[test]
fn earcut_mixed_ring_succeeds_with_per_triangle_common_axes() {
    let polygon = mixed_ring_polygon();
    let source = unique_xy_source_map(&polygon);
    let triangles = Shape::Polygon(polygon)
        .polygon_triangles()
        .expect("kernel mixed-ring polygon triangulates");
    assert!(!triangles.is_empty());
    let mut saw_mixed_source = false;
    let mut saw_all_xyz_source = false;
    for triangle in &triangles {
        let corners = triangle_corners(triangle).expect("triangle polygon");
        let sources = corners.map(|corner| {
            *source
                .get(&xy_bits(corner))
                .expect("triangle corner was not a source vertex")
        });
        let all_xyz = sources.iter().all(|point| point.z().is_some());
        let any_xyz = sources.iter().any(|point| point.z().is_some());
        if all_xyz {
            saw_all_xyz_source = true;
            assert_eq!(triangle.axes(), CoordinateAxes::XYZ);
            for (corner, origin) in corners.iter().zip(sources) {
                assert_eq!(corner.z(), origin.z());
            }
        } else {
            if any_xyz {
                saw_mixed_source = true;
            }
            assert_eq!(triangle.axes(), CoordinateAxes::XY);
            assert!(corners.iter().all(|corner| corner.z().is_none()));
        }
    }
    assert!(
        saw_mixed_source,
        "fixture must emit a mixed-source triangle"
    );
    assert!(
        saw_all_xyz_source,
        "fixture must emit an all-XYZ shell triangle"
    );
}

#[test]
fn earcut_packed_axes_none_on_mixed_rings_some_on_homogeneous() {
    assert_eq!(
        Shape::Polygon(mixed_ring_polygon()).earcut_packed_axes(),
        None
    );
    let homogeneous = polygon(
        vec![
            xyz(0.0, 0.0, 1.0),
            xyz(4.0, 0.0, 2.0),
            xyz(4.0, 4.0, 3.0),
            xyz(0.0, 4.0, 4.0),
            xyz(0.0, 0.0, 1.0),
        ],
        vec![vec![
            xyz(1.0, 1.0, 5.0),
            xyz(2.0, 1.0, 6.0),
            xyz(1.0, 2.0, 7.0),
            xyz(1.0, 1.0, 5.0),
        ]],
    );
    assert_eq!(
        Shape::Polygon(homogeneous).earcut_packed_axes(),
        Some(CoordinateAxes::XYZ)
    );
}

#[test]
fn earcut_packed_vertices_are_closed_earclip_tuples() {
    let source = Shape::Polygon(polygon(
        vec![
            xyz(0.0, 0.0, 1.0),
            xyz(1.0, 0.0, 2.0),
            xyz(0.0, 1.0, 3.0),
            xyz(0.0, 0.0, 1.0),
        ],
        Vec::new(),
    ));
    let vertices = source
        .polygon_triangle_vertices()
        .expect("packed earcut stream");
    assert!(!vertices.is_empty());
    assert_eq!(vertices.len() % 4, 0);
    for ring in vertices.as_chunks::<4>().0 {
        assert_eq!(ring[0], ring[3]);
        assert!(ring.iter().all(|point| point.z().is_some()));
    }
}

#[test]
fn triangulation_budgets_reject_before_reserving_generated_vertices() {
    let mut delaunay_budget = ExpansionBudget::new("triangulate", "method");
    delaunay_budget.add(GENERATED_ITEM_LIMIT).unwrap();
    assert_eq!(take_delaunay_output_reserves(), 0);
    Shape::MultiPoint(vec![xy(0.0, 0.0), xy(1.0, 0.0), xy(0.0, 1.0)].into())
        .delaunay_triangle_vertices_budgeted(&mut delaunay_budget)
        .unwrap_err();
    assert_eq!(take_delaunay_output_reserves(), 0);

    let source = Shape::Polygon(polygon(
        vec![
            xy(0.0, 0.0),
            xy(1.0, 0.0),
            xy(1.0, 1.0),
            xy(0.0, 1.0),
            xy(0.0, 0.0),
        ],
        Vec::new(),
    ));
    let mut constrained_budget = ExpansionBudget::new("triangulate", "min_angle/max_area");
    constrained_budget.add(GENERATED_ITEM_LIMIT).unwrap();
    assert_eq!(take_constrained_output_reserves(), 0);
    source
        .constrained_delaunay_vertices_budgeted(CdtRefinement::Off, &mut constrained_budget)
        .unwrap_err();
    assert_eq!(take_constrained_output_reserves(), 0);
}

#[test]
fn delaunay_triangles_accept_mixed_axis_collection_without_panicking() {
    let shape = Shape::geometry_collection(vec![
        Shape::Point(xy(0.0, 0.0)),
        Shape::Point(xyz(1.0, 0.0, 1.0)),
        Shape::Point(xy(0.0, 1.0)),
        Shape::Point(xyz(1.0, 1.0, 2.0)),
    ])
    .expect("test collection is valid");
    let triangles = shape
        .delaunay_triangles()
        .expect("mixed-axis collection triangulates row-wise");
    assert!(!triangles.is_empty());
    for triangle in triangles {
        let corners = triangle_corners(&triangle).expect("triangle polygon");
        assert_eq!(corners.len(), 3);
    }
}

#[test]
fn build_area_strict_rejects_mixed_output_axes() {
    let square = |points: Vec<Point>| {
        Shape::LineString(LineSeq::from_proven(
            CoordSeq::try_from_points(&points).unwrap(),
        ))
    };
    let source = Shape::geometry_collection(vec![
        square(vec![
            xy(0.0, 0.0),
            xy(2.0, 0.0),
            xy(2.0, 2.0),
            xy(0.0, 2.0),
            xy(0.0, 0.0),
        ]),
        square(vec![
            xyz(10.0, 0.0, 7.0),
            xyz(12.0, 0.0, 8.0),
            xyz(12.0, 2.0, 9.0),
            xyz(10.0, 2.0, 10.0),
            xyz(10.0, 0.0, 7.0),
        ]),
    ])
    .expect("mixed-axis collection is valid");

    let error = source
        .build_area(true)
        .expect_err("strict ordinate carry must reject mixed output axes");
    assert!(matches!(
        error.kind(),
        ErrorKind::Geometry(GeometryErrorKind::OrdinateDropped(operation))
            if operation.as_ref() == "build_area"
    ));
}

#[test]
fn polygonize_full_strict_scopes_diagnostic_axes_to_source_rows() {
    let z_line = Shape::LineString(LineSeq::from_proven(
        CoordSeq::try_from_points(&[xyz(0.0, 0.0, 1.0), xyz(1.0, 0.0, 2.0)]).unwrap(),
    ));
    let m_line = Shape::LineString(LineSeq::from_proven(
        CoordSeq::try_from_points(&[
            Point::new_axes(0.0, 0.0, ZOrdinate(None), MOrdinate(Some(10.0)))
                .expect("finite test coordinate"),
            Point::new_axes(-1.0, 0.0, ZOrdinate(None), MOrdinate(Some(20.0)))
                .expect("finite test coordinate"),
        ])
        .unwrap(),
    ));
    let sources = [&z_line, &m_line];

    let full = Shape::polygonize_full_all(&sources, true)
        .expect("each diagnostic row has one source axis");

    assert_eq!(full.dangles[0][0].coordinate_axes(), "XYZ");
    assert_eq!(full.dangles[1][0].coordinate_axes(), "XYM");
}

#[test]
fn constrained_pentagon_retains_reciprocal_axes() {
    // 1e77 × 1e-77: squared circumcircle products of these vertices underflow
    // in floating point, which once lost an axis. This is a topology test:
    // all five stored vertices must make three constrained faces.
    let large = 1.0e77;
    let tiny = 1.0e-77;
    let source = Shape::Polygon(polygon(
        vec![
            xy(-large, 0.0),
            xy(-0.5 * large, -tiny),
            xy(0.5 * large, -tiny),
            xy(large, 0.0),
            xy(0.0, tiny),
        ],
        Vec::new(),
    ));
    let vertices = source
        .constrained_delaunay_vertices(CdtRefinement::Off)
        .expect("reciprocal constrained pentagon must triangulate");
    assert_eq!(vertices.len(), 12, "three closed triangle rings");
}

#[test]
fn reciprocal_delaunay_legalizes_with_the_source_incircle() {
    for exponent in [77, 159, 200, 300] {
        let large = 10.0_f64.powi(exponent);
        let tiny = 10.0_f64.powi(-exponent);
        for swap_axes in [false, true] {
            let point = |x, y| {
                if swap_axes {
                    Point::new_raw_xy(y, x)
                } else {
                    Point::new_raw_xy(x, y)
                }
            };
            let points = [
                point(-large, 0.0),
                point(0.0, -tiny),
                point(large, 0.0),
                point(0.0, 2.0 * tiny),
            ];
            let triangulation = super::delaunay::delaunay_triangulation(&points).unwrap();
            let has_edge = |left, right| {
                triangulation
                    .triangles
                    .as_chunks::<3>()
                    .0
                    .iter()
                    .any(|triangle| {
                        triangle.iter().zip(triangle.iter().cycle().skip(1)).any(
                            |(&start, &end)| {
                                (start == left && end == right) || (start == right && end == left)
                            },
                        )
                    })
            };
            assert!(has_edge(1, 3), "exp={exponent} swap_axes={swap_axes}");
            assert!(!has_edge(0, 2), "exp={exponent} swap_axes={swap_axes}");
        }
    }
}

fn raw(x: f64, y: f64) -> Point {
    Point::new_raw_xy(x, y)
}

fn sorted_indices(cycle: &[usize]) -> Vec<usize> {
    let mut indices = cycle.to_vec();
    indices.sort_unstable();
    indices
}

fn assert_sites_covered_by_nondegenerate_triangles(points: &[Point]) {
    let triangulation = super::delaunay::delaunay_triangulation(points).unwrap();
    let mut used = std::collections::HashSet::<usize>::new();
    for triangle in triangulation.triangles.as_chunks::<3>().0 {
        assert_ne!(
            orientation(
                points[triangle[0]].xy(),
                points[triangle[1]].xy(),
                points[triangle[2]].xy()
            ),
            Orientation::Collinear,
            "degenerate face {triangle:?}"
        );
        used.extend(triangle);
    }
    assert_eq!(
        used.len(),
        points.len(),
        "every input site must appear in a triangle, used={used:?}"
    );
}

#[test]
fn mixed_zero_left_only_corner_hull_keeps_the_axis_extreme() {
    let points = [
        raw(-0.0, -3.0),
        raw(0.0, -2.0),
        raw(-0.0, -1.0),
        raw(-3.0, 0.0),
    ];
    assert_eq!(points[0].x.to_bits(), (-0.0_f64).to_bits());
    assert_eq!(points[1].x.to_bits(), 0.0_f64.to_bits());
    let hull = super::delaunay::exact_corner_hull_cycle(&points);
    assert_eq!(sorted_indices(&hull), vec![0, 2, 3]);
    assert_sites_covered_by_nondegenerate_triangles(&points);
}

#[test]
fn mixed_zero_right_only_corner_hull_drops_the_collinear_midpoint() {
    let points = [
        raw(0.0, -3.0),
        raw(-0.0, -2.0),
        raw(-0.0, -1.0),
        raw(1.0, 0.0),
    ];
    let hull = super::delaunay::exact_corner_hull_cycle(&points);
    assert_eq!(sorted_indices(&hull), vec![0, 2, 3]);
    assert_sites_covered_by_nondegenerate_triangles(&points);
}

#[test]
fn mixed_zero_both_sides_corner_hull_matches_unsigned_zero() {
    let mixed = [
        raw(-0.0, -3.0),
        raw(0.0, -2.0),
        raw(-0.0, -1.0),
        raw(-3.0, 0.0),
        raw(1.0, 0.0),
    ];
    let unsigned = [
        raw(0.0, -3.0),
        raw(0.0, -2.0),
        raw(0.0, -1.0),
        raw(-3.0, 0.0),
        raw(1.0, 0.0),
    ];
    let mixed_hull = super::delaunay::exact_corner_hull_cycle(&mixed);
    let unsigned_hull = super::delaunay::exact_corner_hull_cycle(&unsigned);
    assert_eq!(sorted_indices(&mixed_hull), sorted_indices(&unsigned_hull));
    assert_eq!(sorted_indices(&mixed_hull), vec![0, 3, 4]);
    assert_sites_covered_by_nondegenerate_triangles(&mixed);
}

#[test]
fn mixed_zero_vertical_collinear_orders_by_y() {
    let points = [
        raw(-0.0, 2.0),
        raw(0.0, -1.0),
        raw(-0.0, 0.0),
        raw(0.0, 1.0),
    ];
    assert_eq!(super::delaunay::exact_hull_cycle(&points), vec![1, 2, 3, 0]);
}

#[test]
fn mixed_zero_horizontal_collinear_orders_by_x() {
    let points = [
        raw(2.0, -0.0),
        raw(-1.0, 0.0),
        raw(0.0, -0.0),
        raw(1.0, 0.0),
    ];
    assert_eq!(super::delaunay::exact_hull_cycle(&points), vec![1, 2, 3, 0]);
}

fn triangle_corners(triangle: &Shape) -> Option<[Point; 3]> {
    let Shape::Polygon(polygon) = triangle else {
        return None;
    };
    let shell = polygon.shell.coords();
    (shell.coord_count() >= 3).then(|| [shell.nth_coord(0), shell.nth_coord(1), shell.nth_coord(2)])
}

fn triangle_shape(corners: &[Point; 3]) -> Shape {
    Shape::Polygon(polygon(corners.to_vec(), Vec::new()))
}

fn polygon_vertices(polygon: &Polygon) -> Vec<Point> {
    let mut vertices = open_ring(polygon.shell.coords());
    for hole in polygon.holes.iter() {
        vertices.extend(open_ring(hole.coords()));
    }
    vertices
}

fn polygon_holes(polygon: &Polygon) -> Vec<Vec<Point>> {
    polygon
        .holes
        .iter()
        .map(|hole| hole.coords().iter_coords().collect())
        .collect()
}

fn polygon_area_for_oracle(polygon: &Polygon) -> f64 {
    let shell = open_ring(polygon.shell.coords());
    let holes = polygon.holes.iter().map(|hole| open_ring(hole.coords()));
    let hole_area = holes
        .map(|hole| open_point_cycle_magnitude(&hole).get())
        .sum::<f64>();
    open_point_cycle_magnitude(&shell).get() - hole_area
}

fn triangle_area_for_oracle(triangle: &Shape) -> f64 {
    let corners = triangle_corners(triangle).expect("triangle polygon");
    open_point_cycle_magnitude(&corners).get()
}

fn assert_earcut_oracle(polygon: Polygon, expected_triangles: usize) -> Vec<Shape> {
    // Membership by canonical bits: the corner check below runs three times
    // per triangle, and a linear scan of it made the largest fixtures
    // quadratic in an oracle that proves nothing about ordering.
    let input_vertices: std::collections::HashSet<(u64, u64)> = polygon_vertices(&polygon)
        .into_iter()
        .map(|point| {
            (
                crate::geometry::canonical_f64_bits(point.x),
                crate::geometry::canonical_f64_bits(point.y),
            )
        })
        .collect();
    let holes = polygon_holes(&polygon);
    let source_area = polygon_area_for_oracle(&polygon);
    let source = Shape::Polygon(polygon);
    // Exact point location reuses the source index across centroids. For a
    // polygon and a point, contains means strictly interior membership.
    let source_plan = crate::geometry::PreparedPlan::transient(&source);
    let triangles = source
        .polygon_triangles()
        .expect("earcut triangulates test polygon");
    assert_eq!(triangles.len(), expected_triangles);

    let triangle_area = triangles.iter().map(triangle_area_for_oracle).sum::<f64>();
    let tolerance = source_area.abs().max(1.0) * 1.0e-12;
    assert!(
        (triangle_area - source_area).abs() <= tolerance,
        "triangle area {triangle_area} differs from polygon area {source_area}"
    );

    // Cache corners once: the pairwise disjointness scan is quadratic in the
    // triangle count, and each candidate uses exact orientation predicates.
    let mut all_corners: Vec<[Point; 3]> = Vec::with_capacity(triangles.len());

    for triangle in &triangles {
        let corners = triangle_corners(triangle).expect("triangle polygon");
        all_corners.push(corners);
        assert_eq!(
            orientation(corners[0], corners[1], corners[2]),
            Orientation::CounterClockwise
        );
        for corner in corners {
            assert!(
                input_vertices.contains(&(
                    crate::geometry::canonical_f64_bits(corner.x),
                    crate::geometry::canonical_f64_bits(corner.y),
                )),
                "triangle vertex {corner:?} was not an input vertex"
            );
        }
        let centroid_point = Point::new_raw_xy(
            (corners[0].x + corners[1].x + corners[2].x) / 3.0,
            (corners[0].y + corners[1].y + corners[2].y) / 3.0,
        );
        assert_eq!(
            source_plan.cartesian_point_location(centroid_point).unwrap(),
            crate::geometry::Loc::Interior,
            "triangle centroid outside source"
        );
        for hole in &holes {
            assert!(
                !ring_contains_interior(hole, centroid_point),
                "triangle centroid lies in a hole"
            );
        }
    }

    // The x-range rejection uses only stored-coordinate minima and maxima, so
    // it cannot discard a pair with positive-area overlap.
    let span = |corners: &[Point; 3]| {
        (
            corners[0].x.min(corners[1].x).min(corners[2].x),
            corners[0].x.max(corners[1].x).max(corners[2].x),
        )
    };
    let mut order: Vec<usize> = (0..triangles.len()).collect();
    order.sort_unstable_by(|&a, &b| span(&all_corners[a]).0.total_cmp(&span(&all_corners[b]).0));

    for (position, &left) in order.iter().enumerate() {
        let left_max_x = span(&all_corners[left]).1;
        let (_, at_or_after_left) = order.split_at(position);
        let Some((_, right_candidates)) = at_or_after_left.split_first() else {
            unreachable!("position was enumerated from the same order");
        };
        for &right in right_candidates {
            if span(&all_corners[right]).0 > left_max_x {
                break;
            }
            assert!(
                triangles_have_disjoint_interiors(&all_corners[left], &all_corners[right]),
                "triangles {left} and {right} overlap with positive area"
            );
        }
    }
    triangles
}

/// True when one triangle's edge has the other triangle entirely outside it.
///
/// Both triangles must be counterclockwise: their interiors lie on the
/// positive side of each edge, and the exact non-positive side includes the
/// exterior and boundary.
fn triangle_separates_interiors_from(triangle: &[Point; 3], other: &[Point; 3]) -> bool {
    triangle
        .iter()
        .copied()
        .zip(triangle.iter().copied().cycle().skip(1))
        .take(3)
        .any(|(from, to)| {
            other.iter().all(|corner| {
                matches!(
                    stored_orientation_sign(from.x, from.y, to.x, to.y, corner.x, corner.y),
                    ExactSign::Negative | ExactSign::Zero
                )
            })
        })
}

/// Exact separating-axis test for two counterclockwise convex triangles.
///
/// By the separating-axis theorem, their interiors are disjoint exactly when
/// some edge of either triangle places every vertex of the other on or beyond
/// the exterior side. Stored-coordinate orientation keeps this decision exact;
/// edge and corner contact therefore count as disjoint interiors.
fn triangles_have_disjoint_interiors(left: &[Point; 3], right: &[Point; 3]) -> bool {
    triangle_separates_interiors_from(left, right) || triangle_separates_interiors_from(right, left)
}

#[test]
fn exact_triangle_oracle_matches_overlay_for_overlap_contact_and_disjoint_pairs() {
    let left_corners = [xy(0.0, 0.0), xy(2.0, 0.0), xy(0.0, 2.0)];
    let left = triangle_shape(&left_corners);
    let cases = [
        (
            "positive-area overlap",
            [xy(0.5, 0.5), xy(2.5, 0.5), xy(0.5, 2.5)],
            false,
        ),
        (
            "shared edge",
            [xy(2.0, 0.0), xy(4.0, 0.0), xy(0.0, 2.0)],
            true,
        ),
        (
            "shared corner",
            [xy(2.0, 0.0), xy(3.0, -1.0), xy(3.0, 1.0)],
            true,
        ),
        ("disjoint", [xy(3.0, 0.0), xy(4.0, 0.0), xy(3.0, 1.0)], true),
    ];

    for (case, right_corners, interiors_are_disjoint) in cases {
        let right = triangle_shape(&right_corners);
        assert_eq!(
            triangles_have_disjoint_interiors(&left_corners, &right_corners),
            interiors_are_disjoint,
            "{case}: exact half-plane classification"
        );

        let intersection = left.intersection(&right, Strictness::Strict).expect(case);
        assert_eq!(
            intersection.area() > 0.0,
            !interiors_are_disjoint,
            "{case}: intersection area"
        );
        assert_eq!(
            left.overlaps(&right).expect(case),
            !interiors_are_disjoint,
            "{case}: overlaps predicate"
        );
    }
}

#[test]
fn exact_triangle_oracle_rejects_overflowed_projection_false_separation() {
    let base = 1.0e200_f64;
    let upper = base.next_up();
    let triangle = [xy(base, base), xy(upper, base), xy(base, upper)];
    let [origin, along_x, top] = triangle;
    assert_eq!(
        orientation(origin, along_x, top),
        Orientation::CounterClockwise
    );

    // The former dot-product projection collapses this identical pair to
    // [infinity, infinity], making its `high <= low` separation check true.
    let normal_x = -(along_x.y - origin.y);
    let normal_y = along_x.x - origin.x;
    let mut projected_low = f64::INFINITY;
    let mut projected_high = f64::NEG_INFINITY;
    for corner in &triangle {
        let projection = corner.x * normal_x + corner.y * normal_y;
        projected_low = projected_low.min(projection);
        projected_high = projected_high.max(projection);
    }
    assert!(
        projected_low.is_infinite()
            && projected_low.is_sign_positive()
            && projected_high.is_infinite()
            && projected_high.is_sign_positive()
            && projected_high <= projected_low,
        "the old projection should falsely separate identical triangles"
    );
    assert!(
        !triangles_have_disjoint_interiors(&triangle, &triangle),
        "an identical triangle has positive-area overlap"
    );
}

#[test]
fn earcut_triangulates_convex_shell() {
    let poly = polygon(
        vec![xy(0.0, 0.0), xy(4.0, 0.0), xy(4.0, 3.0), xy(0.0, 3.0)],
        Vec::new(),
    );
    assert_earcut_oracle(poly, 2);
}

#[test]
fn earcut_triangulates_concave_shell() {
    let poly = polygon(
        vec![
            xy(0.0, 0.0),
            xy(4.0, 0.0),
            xy(4.0, 1.0),
            xy(1.0, 1.0),
            xy(1.0, 4.0),
            xy(0.0, 4.0),
        ],
        Vec::new(),
    );
    assert_earcut_oracle(poly, 4);
}

#[test]
fn earcut_triangulates_square_with_square_hole() {
    let poly = polygon(
        vec![xy(0.0, 0.0), xy(10.0, 0.0), xy(10.0, 10.0), xy(0.0, 10.0)],
        vec![vec![xy(3.0, 3.0), xy(7.0, 3.0), xy(7.0, 7.0), xy(3.0, 7.0)]],
    );
    assert_earcut_oracle(poly, 8);
}

#[test]
fn earcut_triangulates_multiple_holes() {
    let poly = polygon(
        vec![xy(0.0, 0.0), xy(12.0, 0.0), xy(12.0, 12.0), xy(0.0, 12.0)],
        vec![
            vec![xy(2.0, 2.0), xy(4.0, 2.0), xy(4.0, 4.0), xy(2.0, 4.0)],
            vec![xy(7.0, 7.0), xy(9.0, 7.0), xy(9.0, 9.0), xy(7.0, 9.0)],
        ],
    );
    assert_earcut_oracle(poly, 14);
}

#[test]
fn earcut_triangulates_bridge_endpoint_reuse_regression() {
    let poly = polygon(
        vec![xy(0.0, 0.0), xy(10.0, 0.0), xy(10.0, 10.0), xy(0.0, 10.0)],
        vec![
            vec![
                xy(5.356_256_279_187_548, 6.741_403_960_767_902),
                xy(8.245_768_999_092_402, 6.741_403_960_767_902),
                xy(8.245_768_999_092_402, 7.308_839_455_349_556),
                xy(5.356_256_279_187_548, 7.308_839_455_349_556),
            ],
            vec![
                xy(4.195_660_001_847_262, 7.724_919_942_214_383),
                xy(5.633_739_425_298_997, 7.724_919_942_214_383),
                xy(5.633_739_425_298_997, 9.362_951_518_972_373),
                xy(4.195_660_001_847_262, 9.362_951_518_972_373),
            ],
        ],
    );
    assert_earcut_oracle(poly, 14);
}

#[test]
fn earcut_strips_repeated_and_collinear_vertices() {
    let poly = polygon(
        vec![
            xy(0.0, 0.0),
            xy(2.0, 0.0),
            xy(4.0, 0.0),
            xy(4.0, 0.0),
            xy(4.0, 4.0),
            xy(2.0, 4.0),
            xy(0.0, 4.0),
        ],
        Vec::new(),
    );
    assert_earcut_oracle(poly, 2);
}

#[test]
fn earcut_handles_huge_finite_coordinates() {
    let n = 1.0e150;
    let poly = polygon(vec![xy(-n, -n), xy(n, -n), xy(n, n), xy(-n, n)], Vec::new());
    assert_earcut_oracle(poly, 2);
}

#[test]
fn earcut_orients_clockwise_large_offset_polygon() {
    let base = 1.0e154;
    let side = 1.0e154;
    let poly = polygon(
        vec![
            xy(base, base),
            xy(base, base + side),
            xy(base + side, base + side),
            xy(base + side, base),
        ],
        Vec::new(),
    );
    assert_earcut_oracle(poly, 2);
}

#[test]
fn earcut_holed_polygon_carries_z_by_vertex_identity() {
    let zpoint = |x, y| xyz(x, y, x + y);
    let poly = polygon(
        vec![
            zpoint(0.0, 0.0),
            zpoint(10.0, 0.0),
            zpoint(10.0, 10.0),
            zpoint(0.0, 10.0),
        ],
        vec![vec![
            zpoint(3.0, 3.0),
            zpoint(7.0, 3.0),
            zpoint(7.0, 7.0),
            zpoint(3.0, 7.0),
        ]],
    );
    let triangles = assert_earcut_oracle(poly, 8);
    for triangle in triangles {
        let Shape::Polygon(polygon) = triangle else {
            panic!("expected triangle polygon");
        };
        for point in polygon.shell.coords().iter_coords().take(3) {
            assert_eq!(point.z(), Some(point.x + point.y));
        }
    }
}

#[test]
fn earcut_rejects_two_holes_touching_at_vertex() {
    let poly = polygon(
        vec![xy(0.0, 0.0), xy(10.0, 0.0), xy(10.0, 10.0), xy(0.0, 10.0)],
        vec![
            vec![xy(2.0, 2.0), xy(4.0, 2.0), xy(4.0, 4.0), xy(2.0, 4.0)],
            vec![xy(4.0, 2.0), xy(6.0, 2.0), xy(6.0, 4.0), xy(4.0, 4.0)],
        ],
    );
    assert_triangulation_error(poly);
}

#[test]
fn earcut_rejects_two_holes_sharing_edge() {
    let poly = polygon(
        vec![xy(0.0, 0.0), xy(10.0, 0.0), xy(10.0, 10.0), xy(0.0, 10.0)],
        vec![
            vec![xy(2.0, 2.0), xy(4.0, 2.0), xy(4.0, 4.0), xy(2.0, 4.0)],
            vec![xy(4.0, 2.0), xy(6.0, 2.0), xy(6.0, 4.0), xy(4.0, 4.0)],
        ],
    );
    assert_triangulation_error(poly);
}

#[test]
fn earcut_rejects_nested_holes() {
    let poly = polygon(
        vec![xy(0.0, 0.0), xy(20.0, 0.0), xy(20.0, 20.0), xy(0.0, 20.0)],
        vec![
            vec![xy(2.0, 2.0), xy(14.0, 2.0), xy(14.0, 14.0), xy(2.0, 14.0)],
            vec![xy(4.0, 4.0), xy(10.0, 4.0), xy(10.0, 10.0), xy(4.0, 10.0)],
        ],
    );
    assert_triangulation_error(poly);
}

#[test]
fn earcut_rejects_hole_touching_shell() {
    let poly = polygon(
        vec![xy(0.0, 0.0), xy(10.0, 0.0), xy(10.0, 10.0), xy(0.0, 10.0)],
        vec![vec![xy(0.0, 3.0), xy(3.0, 3.0), xy(3.0, 6.0), xy(0.0, 6.0)]],
    );
    assert_triangulation_error(poly);
}

#[test]
fn earcut_reports_outside_first_hole_before_a_later_invalid_hole() {
    let poly = polygon(
        vec![xy(0.0, 0.0), xy(10.0, 0.0), xy(10.0, 10.0), xy(0.0, 10.0)],
        vec![
            vec![
                xy(20.0, 20.0),
                xy(22.0, 20.0),
                xy(22.0, 22.0),
                xy(20.0, 22.0),
            ],
            vec![
                xy(4.0, 4.0),
                xy(6.0, 6.0),
                xy(4.0, 6.0),
                xy(6.0, 4.0),
                xy(7.0, 5.0),
            ],
        ],
    );
    let error = Shape::Polygon(poly)
        .polygon_triangles()
        .expect_err("the first outside hole owns the reported failure");
    assert!(matches!(
        error.kind(),
        ErrorKind::Geometry(GeometryErrorKind::Triangulation(message))
            if message.as_ref() == "polygon hole is not strictly inside the shell"
    ));
}

#[test]
fn earcut_rejects_self_crossing_ring() {
    let poly = polygon(
        vec![
            xy(0.0, 0.0),
            xy(4.0, 0.0),
            xy(0.0, 4.0),
            xy(4.0, 4.0),
            xy(2.0, -1.0),
        ],
        Vec::new(),
    );
    assert_triangulation_error(poly);
}

#[test]
fn earcut_rejects_collinear_backtracking_ring() {
    let poly = polygon(
        vec![
            xy(0.0, 0.0),
            xy(4.0, 0.0),
            xy(2.0, 0.0),
            xy(4.0, 4.0),
            xy(0.0, 4.0),
        ],
        Vec::new(),
    );
    assert_triangulation_error(poly);
}

#[test]
fn earcut_rejects_zero_area_bowtie_ring() {
    let poly = polygon(
        vec![xy(0.0, 0.0), xy(4.0, 4.0), xy(0.0, 4.0), xy(4.0, 0.0)],
        Vec::new(),
    );
    assert_triangulation_error(poly);
}

fn large_convex_ring(vertex_count: usize) -> Vec<Point> {
    (0..vertex_count)
        .map(|index| {
            let angle = std::f64::consts::TAU * index as f64 / vertex_count as f64;
            xy(angle.cos() * 100.0, angle.sin() * 100.0)
        })
        .collect()
}

#[test]
fn polygon_triangles_circle_candidate_work_scales_near_linearly() {
    let mut counts = Vec::new();
    for vertices in [2_048_usize, 4_096] {
        let shape = Shape::Polygon(polygon(large_convex_ring(vertices), Vec::new()));
        let (triangles, candidates) = super::earcut::count_ear_candidates(|| {
            shape.polygon_triangles().expect("circle triangulates")
        });
        assert_eq!(triangles.len(), vertices.wrapping_sub(2));
        assert!(
            candidates < vertices.wrapping_mul(128),
            "{vertices}: {candidates} candidate inspections"
        );
        counts.push(candidates);
    }
    eprintln!("circle candidate work: {counts:?}");
    assert!(
        counts[1].wrapping_mul(10) < counts[0].wrapping_mul(28),
        "doubling the circle must not quadruple candidate work: {counts:?}"
    );
}

#[test]
fn polygon_triangles_radial_validation_work_scales_near_linearly() {
    let mut counts = Vec::new();
    for vertices in [2_048_usize, 4_096] {
        let mut seed = 42_u64;
        let points = (0..vertices)
            .map(|index| {
                seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                let radius = 0.8 + 0.2 * f64::from((seed >> 32) as u32) / f64::from(u32::MAX);
                let angle = index as f64 * std::f64::consts::TAU / vertices as f64;
                xy(radius * angle.cos(), radius * angle.sin())
            })
            .collect();
        let shape = Shape::Polygon(polygon(points, Vec::new()));
        let (triangles, work) = super::earcut_sweep::count_work(|| {
            shape
                .polygon_triangles()
                .expect("simple radial ring triangulates")
        });
        assert_eq!(triangles.len(), vertices.wrapping_sub(2));
        counts.push(work);
    }
    eprintln!("radial validation work: {counts:?}");
    assert!(
        counts[1].wrapping_mul(10) < counts[0].wrapping_mul(25),
        "doubling the ring must not quadruple intersection decision work: {counts:?}"
    );
}

#[test]
fn earcut_sweep_preserves_box_scan_contact_decisions() {
    let cases = [
        // Vertical sides and permitted wrap-around adjacency.
        vec![xy(0.0, 0.0), xy(4.0, 0.0), xy(4.0, 4.0), xy(0.0, 4.0)],
        // A non-adjacent vertex lies inside the first edge.
        vec![
            xy(0.0, 0.0),
            xy(4.0, 0.0),
            xy(4.0, 4.0),
            xy(2.0, 0.0),
            xy(0.0, 4.0),
        ],
        // Overlapping vertical carriers.
        vec![
            xy(0.0, 0.0),
            xy(0.0, 4.0),
            xy(2.0, 4.0),
            xy(0.0, 3.0),
            xy(0.0, 1.0),
            xy(2.0, 0.0),
        ],
        // Crossing through a vertical side.
        vec![xy(0.0, 0.0), xy(4.0, 4.0), xy(0.0, 4.0), xy(4.0, 0.0)],
        // Distinct collinear starts with disjoint vertical intervals.
        vec![
            xy(0.0, 0.0),
            xy(0.0, 1.0),
            xy(1.0, 2.0),
            xy(0.0, 3.0),
            xy(0.0, 4.0),
            xy(4.0, 4.0),
            xy(4.0, 0.0),
        ],
    ];
    for (case, ring) in cases.iter().enumerate() {
        let mut long = ring.clone();
        let first = ring[0];
        let last = *ring.last().unwrap();
        for step in 1..=48 {
            let t = f64::from(step) / 49.0;
            let outward = 1.0 + 0.1 * f64::from(step % 2);
            long.push(if first.x == last.x {
                xy(first.x - outward, last.y + (first.y - last.y) * t)
            } else {
                xy(last.x + (first.x - last.x) * t, first.y - outward)
            });
        }
        // The outer chain retains the contact under test while exercising
        // both the direct lane and the event sweep.
        for (variant, ring) in [ring, &long].into_iter().enumerate() {
            for scale in [1e-170, 1.0, 1e150] {
                let points: Vec<_> = ring.iter().map(|p| xy(p.x * scale, p.y * scale)).collect();
                assert_eq!(
                    super::earcut_sweep::ring_has_contact(&points),
                    super::earcut::ring_has_contact_box_scan(&points),
                    "case {case}, variant {variant}, scale {scale}"
                );
            }
        }
    }
}

#[test]
fn validate_earcut_ring_accepts_large_simple_ring() {
    let ring = large_convex_ring(10_000);
    validate_earcut_ring(&ring).expect("large simple ring is valid");
    cleaned_earcut_ring(&ring, false).expect("large simple ring cleans");
}

#[test]
fn validate_earcut_ring_rejects_large_self_intersecting_ring() {
    let mut ring = large_convex_ring(10_000);
    ring[5_000] = ring[100];
    assert!(validate_earcut_ring(&ring).is_err());
}

#[test]
fn earcut_triangulates_thin_horizontal_sliver() {
    let poly = polygon(
        vec![
            xy(0.0, 0.0),
            xy(500.0, 0.0),
            xy(500.0, 1.0e-6),
            xy(0.0, 1.0e-6),
        ],
        Vec::new(),
    );
    assert_earcut_oracle(poly, 2);
}

#[test]
fn cleanup_constraint_lines_keeps_exact_distinct_endpoints() {
    use crate::geometry::XY;

    // Previously an absolute 1e-4 snap annihilated this 5e-5 square.
    let s = 5.0e-5;
    let lines = vec![
        [XY::new(0.0, 0.0), XY::new(s, 0.0)],
        [XY::new(s, 0.0), XY::new(s, s)],
        [XY::new(s, s), XY::new(0.0, s)],
        [XY::new(0.0, s), XY::new(0.0, 0.0)],
    ];
    let cleaned = cleanup_constraint_lines(lines.clone());
    assert_eq!(cleaned.len(), 4, "exact square edges must survive cleanup");
    // Exact duplicate undirected edges collapse.
    let with_dup = {
        let mut extended = lines;
        extended.push([XY::new(s, 0.0), XY::new(0.0, 0.0)]); // reverse of first
        extended
    };
    assert_eq!(cleanup_constraint_lines(with_dup).len(), 4);
}

fn convex_ring(vertex_count: usize, radius: f64) -> Vec<Point> {
    (0..vertex_count)
        .map(|index| {
            let angle = std::f64::consts::TAU * index as f64 / vertex_count as f64;
            xy(radius * angle.cos(), radius * angle.sin())
        })
        .collect()
}

fn star_ring(vertex_count: usize, outer_radius: f64, inner_radius: f64) -> Vec<Point> {
    (0..vertex_count)
        .map(|index| {
            let angle = std::f64::consts::TAU * index as f64 / vertex_count as f64;
            let radius = if index % 2 == 0 {
                outer_radius
            } else {
                inner_radius
            };
            xy(radius * angle.cos(), radius * angle.sin())
        })
        .collect()
}

fn axis_aligned_square(center_x: f64, center_y: f64, half_side: f64) -> Vec<Point> {
    vec![
        xy(center_x - half_side, center_y - half_side),
        xy(center_x + half_side, center_y - half_side),
        xy(center_x + half_side, center_y + half_side),
        xy(center_x - half_side, center_y + half_side),
    ]
}

#[test]
fn earcut_holes_curated_corpus_preserves_triangulation_invariants() {
    // Retaining collinear bridge occurrences gives V + 2H - 2 triangles
    // without T-junctions even in symmetric multi-hole layouts.
    // convex_small_one_hole
    assert_earcut_oracle(
        polygon(convex_ring(8, 100.0), vec![axis_aligned_square(
            0.0, 0.0, 2.0,
        )]),
        12,
    );
    // star_odd_one_hole
    assert_earcut_oracle(
        polygon(star_ring(9, 100.0, 40.0), vec![axis_aligned_square(
            0.0, 0.0, 2.0,
        )]),
        13,
    );
    // convex_medium_two_holes
    assert_earcut_oracle(
        polygon(convex_ring(31, 100.0), vec![
            axis_aligned_square(-20.0, 0.0, 3.0),
            axis_aligned_square(20.0, 0.0, 3.0),
        ]),
        41,
    );
    // star_medium_three_holes
    assert_earcut_oracle(
        polygon(star_ring(64, 100.0, 40.0), vec![
            axis_aligned_square(-15.0, -10.0, 2.0),
            axis_aligned_square(15.0, -10.0, 2.0),
            axis_aligned_square(0.0, 15.0, 2.0),
        ]),
        80,
    );
    // convex_max_four_holes
    assert_earcut_oracle(
        polygon(convex_ring(256, 100.0), vec![
            axis_aligned_square(-30.0, -30.0, 5.0),
            axis_aligned_square(30.0, -30.0, 5.0),
            axis_aligned_square(-30.0, 30.0, 5.0),
            axis_aligned_square(30.0, 30.0, 5.0),
        ]),
        278,
    );
    // star_max_four_holes
    assert_earcut_oracle(
        polygon(star_ring(255, 100.0, 40.0), vec![
            axis_aligned_square(-15.0, -10.0, 2.0),
            axis_aligned_square(15.0, -10.0, 2.0),
            axis_aligned_square(-10.0, 15.0, 2.0),
            axis_aligned_square(12.0, 15.0, 2.0),
        ]),
        277,
    );
}

fn square_shell_with_offset_holes(shell_vertices: usize) -> Polygon {
    let shell = large_convex_ring(shell_vertices);
    polygon(shell, vec![
        axis_aligned_square(-20.0, -20.0, 5.0),
        axis_aligned_square(20.0, -15.0, 4.0),
        axis_aligned_square(0.0, 25.0, 6.0),
    ])
}

fn square_shell_with_grid_holes(hole_count: usize) -> Polygon {
    let shell = large_convex_ring(500);
    let side = hole_count.isqrt().max(1);
    let spacing = 120.0 / side as f64;
    let mut holes = Vec::with_capacity(hole_count);
    'grid: for row in 0..side {
        for col in 0..side {
            if holes.len() >= hole_count {
                break 'grid;
            }
            let center_x = -50.0 + col as f64 * spacing;
            let center_y = -50.0 + row as f64 * spacing;
            holes.push(axis_aligned_square(center_x, center_y, 1.5));
        }
    }
    polygon(shell, holes)
}

#[test]
fn earcut_rejects_nested_holes_via_containment_index() {
    let outer = vec![xy(2.0, 2.0), xy(14.0, 2.0), xy(14.0, 14.0), xy(2.0, 14.0)];
    let inner = vec![xy(4.0, 4.0), xy(10.0, 4.0), xy(10.0, 10.0), xy(4.0, 10.0)];
    let holes = vec![outer, inner];
    let shell = [xy(0.0, 0.0), xy(20.0, 0.0), xy(20.0, 20.0), xy(0.0, 20.0)];
    let err =
        validate_hole_interactions(&shell, &holes).expect_err("nested holes must be rejected");
    assert!(matches!(
        err.kind(),
        ErrorKind::Geometry(GeometryErrorKind::Triangulation { .. })
    ));
}

#[test]
fn earcut_grid_holes_preserve_euler_triangle_count() {
    for hole_count in [10_usize, 20, 40, 80] {
        let poly = square_shell_with_grid_holes(hole_count);
        let expected_count = polygon_vertices(&poly)
            .len()
            .wrapping_add(poly.holes.len().wrapping_mul(2))
            .wrapping_sub(2);
        let source = Shape::Polygon(poly);
        let triangles = source
            .polygon_triangles()
            .expect("many-hole polygon triangulates");
        assert_eq!(triangles.len(), expected_count, "{hole_count} grid holes");
    }
}

#[test]
fn polygon_triangles_grid_bridge_work_scales_subquadratically() {
    let mut counts = Vec::new();
    // At 4x4/8x8/16x16, fixed shell work hides an exhaustive quadratic
    // bridge scan. The next square-grid ladder rejects that scan below.
    for holes in [25_usize, 100, 400] {
        let poly = square_shell_with_grid_holes(holes);
        let expected = polygon_vertices(&poly)
            .len()
            .wrapping_add(holes.wrapping_mul(2))
            .wrapping_sub(2);
        let source = Shape::Polygon(poly);
        let ((triangles, ear_work), bridge_work) = super::earcut_bridges::count_bridge_work(|| {
            super::earcut::count_ear_candidates(|| {
                source.polygon_triangles().expect("grid triangulates")
            })
        });
        assert_eq!(triangles.len(), expected);
        eprintln!(
            "{holes} holes: bridge work {bridge_work}, ear work {ear_work}, ear scans {}",
            super::earcut::ear_scan_steps()
        );
        counts.push(bridge_work);
    }
    for pair in counts.windows(2) {
        if let &[small, large] = pair {
            assert!(
                large < small.wrapping_mul(6),
                "quadrupling holes: {counts:?}"
            );
        }
    }
}

fn assert_offset_hole_shell_bridges(shell_vertices: usize) {
    // Structural check: an offset-hole shell triangulates without error and
    // produces a positive triangle count (no wall-clock).
    let poly = square_shell_with_offset_holes(shell_vertices);
    let source = Shape::Polygon(poly);
    let triangles = source
        .polygon_triangles()
        .expect("holed polygon triangulates");
    assert!(
        !triangles.is_empty(),
        "shell_vertices={shell_vertices} must produce triangles"
    );
}

// The size ladder is one test per rung rather than one loop over all of them:
// the rungs are independent, so separate tests run concurrently and a failure
// names its own size. These figures are a scale ladder, not a boundary.
#[test]
fn earcut_holes_bridge_succeeds_for_a_500_vertex_shell() {
    assert_offset_hole_shell_bridges(500);
}

#[test]
fn earcut_holes_bridge_succeeds_for_a_1000_vertex_shell() {
    assert_offset_hole_shell_bridges(1_000);
}

#[test]
fn earcut_holes_bridge_succeeds_for_a_2000_vertex_shell() {
    assert_offset_hole_shell_bridges(2_000);
}

/// The closing edge crosses the second edge: the boundary constraints cross,
/// which the constrained triangulation reports instead of walking forever.
#[test]
fn constrained_triangulation_rejects_crossing_boundaries() {
    let shape = Shape::Polygon(polygon(
        vec![
            xy(2.290_624_926_619_315_3, 9.562_313_792_007_403),
            xy(2.224_421_741_403_499, 9.329_885_065_819_159),
            xy(3.747_956_967_035_721_6, 9.184_604_808_378_285),
            xy(4.010_448_586_307_951, 9.297_996_312_041_94),
            xy(3.967_995_534_117_932_3, 7.729_577_524_518_278),
            xy(4.340_816_540_301_225, 6.815_248_837_539_205),
            xy(2.290_624_926_619_315_3, 9.562_313_792_007_403),
        ],
        vec![],
    ));
    let mut budget = ExpansionBudget::new("test", "crossing boundaries");
    let result =
        shape.constrained_delaunay_vertices_budgeted(CdtRefinement::default(), &mut budget);
    assert!(
        matches!(&result, Err(error) if matches!(error.kind(), ErrorKind::Geometry(_))),
        "{result:?}"
    );
}
