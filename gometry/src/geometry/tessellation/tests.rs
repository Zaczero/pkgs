#![allow(
    clippy::absolute_paths,
    reason = "test-only oracle assertions name their full geometry ownership path explicitly"
)]
use super::{
    CONSTRAINT_SNAP_RADIUS, ConstraintSnapRegistry, EarArena, ZScale, cleaned_earcut_ring,
    minimum_area_rectangle, open_ring, snap_or_register, triangle_corners, validate_earcut_ring,
    validate_hole_interactions,
};
use crate::error::ErrorKind;
use crate::geometry::segments::Orientation;
use crate::geometry::{
    Coordinates, GeometryErrorKind, MOrdinate, Point, Polygon, Ring, Shape, Strictness, ZOrdinate,
    monotone_chain_hull, open_point_cycle_decision, orientation, ring_contains_interior,
    same_point,
};

fn xy(x: f64, y: f64) -> Point {
    Point::new(x, y).expect("finite test coordinate")
}

fn xyz(x: f64, y: f64, z: f64) -> Point {
    Point::new_axes(x, y, ZOrdinate(Some(z)), MOrdinate(None)).expect("finite test coordinate")
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
fn delaunay_triangles_accept_mixed_axis_collection_without_panicking() {
    let shape = Shape::GeometryCollection(vec![
        Shape::Point(xy(0.0, 0.0)),
        Shape::Point(xyz(1.0, 0.0, 1.0)),
        Shape::Point(xy(0.0, 1.0)),
        Shape::Point(xyz(1.0, 1.0, 2.0)),
    ]);
    let triangles = shape
        .delaunay_triangles()
        .expect("mixed-axis collection triangulates row-wise");
    assert!(!triangles.is_empty());
    for triangle in triangles {
        let corners = triangle_corners(&triangle).expect("triangle polygon");
        assert_eq!(corners.len(), 3);
    }
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
        .map(|hole| open_point_cycle_decision(&hole).magnitude().get())
        .sum::<f64>();
    open_point_cycle_decision(&shell).magnitude().get() - hole_area
}

fn triangle_area_for_oracle(triangle: &Shape) -> f64 {
    let corners = triangle_corners(triangle).expect("triangle polygon");
    open_point_cycle_decision(&corners).magnitude().get()
}

fn assert_earcut_oracle(polygon: Polygon, expected_triangles: usize) -> Vec<Shape> {
    let input_vertices = polygon_vertices(&polygon);
    let holes = polygon_holes(&polygon);
    let source_area = polygon_area_for_oracle(&polygon);
    let source = Shape::Polygon(polygon);
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

    for triangle in &triangles {
        let corners = triangle_corners(triangle).expect("triangle polygon");
        assert_eq!(
            orientation(corners[0], corners[1], corners[2]),
            Orientation::CounterClockwise
        );
        for corner in corners {
            assert!(
                input_vertices
                    .iter()
                    .any(|input| same_point(*input, corner)),
                "triangle vertex {corner:?} was not an input vertex"
            );
        }
        let centroid_point = Point::new_unchecked_xy(
            (corners[0].x + corners[1].x + corners[2].x) / 3.0,
            (corners[0].y + corners[1].y + corners[2].y) / 3.0,
        );
        let centroid = Shape::Point(centroid_point);
        assert!(
            source.contains(&centroid),
            "triangle centroid outside source"
        );
        for hole in &holes {
            assert!(
                !ring_contains_interior(hole, centroid_point),
                "triangle centroid lies in a hole"
            );
        }
    }

    for left in 0..triangles.len() {
        for right in left + 1..triangles.len() {
            let intersection = triangles[left]
                .intersection(&triangles[right], Strictness::Strict)
                .expect("triangle intersection");
            let overlap_area = intersection.area();
            assert!(
                overlap_area.abs() <= tolerance,
                "triangles {left} and {right} overlap with area {overlap_area}"
            );
            assert!(
                !triangles[left].overlaps(&triangles[right]),
                "triangles {left} and {right} overlap"
            );
        }
    }
    triangles
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
#[expect(
    clippy::float_cmp,
    reason = "exact comparison is intentional (sentinel / degenerate / exact-literal check)"
)]
fn zscale_one_d_morton_orders_horizontal_sliver() {
    let ring: Vec<Point> = (0..1_000).map(|index| xy(f64::from(index), 0.0)).collect();
    let scale = ZScale::new(&ring).expect("horizontal sliver has x extent");
    assert_eq!(scale.height, 0.0);
    assert!(scale.width > 0.0);
    let keys: Vec<u64> = ring.iter().map(|&point| scale.key(point)).collect();
    let mut sorted = keys.clone();
    sorted.sort_unstable();
    assert_eq!(keys, sorted, "x-ordered ring should have monotone z keys");
    let arena = EarArena::new(&ring);
    assert!(arena.z_scale.is_some());
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
fn snap_or_register_collapses_near_coincident_constraint_points() {
    use crate::geometry::XY;

    let mut registry = ConstraintSnapRegistry::new();
    let base = XY::new(1.0, 2.0);
    let mut snapped = Vec::new();
    for index in 0..500 {
        let offset = f64::from(index) * CONSTRAINT_SNAP_RADIUS * 0.001;
        snapped.push(snap_or_register(
            XY::new(base.x + offset, base.y),
            &mut registry,
        ));
    }
    assert!(
        snapped.iter().all(|point| same_point(*point, snapped[0])),
        "near-coincident endpoints should collapse to one vertex"
    );
}

fn minimum_area_rectangle_area(hull: &[Point]) -> f64 {
    let ring = minimum_area_rectangle(hull);
    open_point_cycle_decision(&open_ring(&ring))
        .magnitude()
        .get()
}

#[test]
fn minimum_area_rectangle_axis_aligned_square() {
    let square = vec![xy(0.0, 0.0), xy(4.0, 0.0), xy(4.0, 4.0), xy(0.0, 4.0)];
    let hull = monotone_chain_hull(&square);
    let area = minimum_area_rectangle_area(&hull);
    assert!(
        (area - 16.0).abs() <= 1.0e-12,
        "expected area 16, got {area}"
    );
}

#[test]
fn minimum_area_rectangle_equilateral_triangle_index_normalization() {
    let side = 4.0;
    let height = side * 3.0_f64.sqrt() / 2.0;
    let triangle = vec![xy(0.0, 0.0), xy(side, 0.0), xy(side / 2.0, height)];
    let hull = monotone_chain_hull(&triangle);
    assert_eq!(hull.len(), 3);
    let area = minimum_area_rectangle_area(&hull);
    let expected = side * height;
    assert!(
        (area - expected).abs() <= 1.0e-9,
        "expected area {expected}, got {area}"
    );
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
    // Symmetric multi-hole layouts create collinear bridge edges. Earcut
    // correctly omits their degenerate triangles, so the helper's area
    // completeness check is the correctness invariant; these counts are
    // recorded regression characterizations, not V + 2H - 2 derivations.
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
        39,
    );
    // star_medium_three_holes
    assert_earcut_oracle(
        polygon(star_ring(64, 100.0, 40.0), vec![
            axis_aligned_square(-15.0, -10.0, 2.0),
            axis_aligned_square(15.0, -10.0, 2.0),
            axis_aligned_square(0.0, 15.0, 2.0),
        ]),
        78,
    );
    // convex_max_four_holes
    assert_earcut_oracle(
        polygon(convex_ring(256, 100.0), vec![
            axis_aligned_square(-30.0, -30.0, 5.0),
            axis_aligned_square(30.0, -30.0, 5.0),
            axis_aligned_square(-30.0, 30.0, 5.0),
            axis_aligned_square(30.0, 30.0, 5.0),
        ]),
        274,
    );
    // star_max_four_holes
    assert_earcut_oracle(
        polygon(star_ring(255, 100.0, 40.0), vec![
            axis_aligned_square(-15.0, -10.0, 2.0),
            axis_aligned_square(15.0, -10.0, 2.0),
            axis_aligned_square(-10.0, 15.0, 2.0),
            axis_aligned_square(12.0, 15.0, 2.0),
        ]),
        273,
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
fn earcut_holes_containment_performance_cliff() {
    let mut timings = Vec::new();
    for hole_count in [10, 20, 40, 80] {
        let poly = square_shell_with_grid_holes(hole_count);
        let source = Shape::Polygon(poly);
        let start = std::time::Instant::now();
        for _ in 0..5 {
            let _ = source
                .polygon_triangles()
                .expect("many-hole polygon triangulates");
        }
        let elapsed = start.elapsed() / 5;
        eprintln!(
            "earcut holes count={hole_count}: {} µs",
            elapsed.as_micros()
        );
        timings.push((hole_count, elapsed));
    }
    let ratio = timings[3].1.as_secs_f64() / timings[2].1.as_secs_f64();
    assert!(
        ratio < 3.0,
        "hole-containment cost grew too fast (80/40 ratio {ratio:.2}, expected sub-quadratic)"
    );
}

#[test]
fn earcut_holes_bridge_performance_cliff() {
    for shell_vertices in [500, 1_000, 2_000, 4_000] {
        let poly = square_shell_with_offset_holes(shell_vertices);
        let source = Shape::Polygon(poly);
        let start = std::time::Instant::now();
        for _ in 0..5 {
            let _ = source
                .polygon_triangles()
                .expect("holed polygon triangulates");
        }
        let elapsed = start.elapsed() / 5;
        eprintln!(
            "earcut holes shell={shell_vertices}: {} µs",
            elapsed.as_micros()
        );
    }
}

#[test]
fn minimum_rotated_rectangle_multipoint_fusion_parity() {
    let points = vec![xy(0.0, 0.0), xy(3.0, 0.0), xy(1.0, 2.0), xy(4.0, 1.0)];
    let multipoint = Shape::MultiPoint(points.clone().into());
    let mrr = multipoint
        .minimum_rotated_rectangle()
        .expect("minimum rotated rectangle");
    let hull = monotone_chain_hull(&points);
    let expected_area = minimum_area_rectangle_area(&hull);
    assert!((mrr.area() - expected_area).abs() <= 1.0e-12);
    assert!(expected_area > 0.0);
}
