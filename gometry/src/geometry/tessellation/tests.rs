#![allow(
    clippy::absolute_paths,
    reason = "test-only oracle assertions name their full geometry ownership path explicitly"
)]
use crate::geometry::XY;
use crate::geometry::tessellation::delaunay::take_constrained_output_reserves;
use crate::geometry::tessellation::shape::{
    BudgetedTriangleSink, canonical_voronoi_sites, take_delaunay_output_reserves,
};

#[test]
fn exact_segment_rejects_projective_point_at_infinity() {
    let horizontal = super::exact::ExactLine::through(XY::new(0.0, 0.0), XY::new(1.0, 0.0));
    let parallel = super::exact::ExactLine::through(XY::new(0.0, 1.0), XY::new(1.0, 1.0));
    let infinity = super::exact::line_intersection(&horizontal, &parallel);
    let start = super::exact::ExactPoint::from_xy(XY::new(0.0, 0.0));
    let end = super::exact::ExactPoint::from_xy(XY::new(1.0, 0.0));

    assert!(!infinity.is_finite());
    assert!(matches!(
        super::exact::segment_intersection(&start, &end, &infinity, &infinity),
        super::exact::SegmentIntersection::None
    ));
}
use crate::error::ErrorKind;
use crate::geometry::segments::Orientation;
use crate::geometry::tessellation::delaunay::cleanup_constraint_lines;
use crate::geometry::tessellation::earcut::{
    EarArena, ZScale, cleaned_earcut_ring, open_ring, validate_earcut_ring,
    validate_hole_interactions,
};
use crate::geometry::tessellation::sampling::triangle_corners;
use crate::geometry::{
    CdtRefinement, Coordinates as _, ExpansionBudget, GENERATED_ITEM_LIMIT, GeometryErrorKind,
    MOrdinate, Point, Polygon, Ring, Shape, Strictness, ZOrdinate, open_point_cycle_decision,
    orientation, ring_contains_interior, same_point,
};

fn xy(x: f64, y: f64) -> Point {
    Point::new(x, y).expect("finite test coordinate")
}

fn xyz(x: f64, y: f64, z: f64) -> Point {
    Point::new_axes(x, y, ZOrdinate(Some(z)), MOrdinate(None)).expect("finite test coordinate")
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
    let restored = canonical_voronoi_sites(input.clone(), 0.1);
    let mut reversed = input;
    reversed.reverse();
    let mutated_order = canonical_voronoi_sites(reversed, 0.1);
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

    let historical = |points: Vec<Point>| {
        let sites = points
            .into_iter()
            .enumerate()
            .map(|(id, point)| super::Site { id, point })
            .collect::<Vec<_>>();
        let mut seen = std::collections::HashSet::new();
        super::snap_sites(&sites, 0.1)
            .into_iter()
            .filter(|site| seen.insert(crate::geometry::PointKey::new(site.point)))
            .map(|site| (site.point.x.to_bits(), site.point.y.to_bits()))
            .collect::<Vec<_>>()
    };
    let historical_forward = historical(vec![
        xy(0.0, 0.0),
        xy(0.09, 0.0),
        xy(0.18, 0.0),
        xy(0.0, 1.0),
        xy(1.0, 0.0),
    ]);
    let historical_reverse = historical(vec![
        xy(1.0, 0.0),
        xy(0.0, 1.0),
        xy(0.18, 0.0),
        xy(0.09, 0.0),
        xy(0.0, 0.0),
    ]);
    assert_ne!(historical_forward, historical_reverse);
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
    let snapped = super::snap_sites(&sites, 1e20);
    assert_eq!(snapped[0].id, 0);
    assert_eq!(snapped[1].id, 0);
}

#[test]
fn public_delaunay_keeps_collinear_empty_contract() {
    let source = Shape::MultiPoint(crate::geometry::CoordSeq::from(vec![
        xy(0.0, 0.0),
        xy(1.0, 0.0),
        xy(2.0, 0.0),
    ]));
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

#[test]
fn constrained_pentagon_retains_reciprocal_axes_before_spade() {
    // 1e77 × 1e-77 is below the old 900-bit trigger but its squared
    // circumcircle products already enter Spade's degenerate range.  This is
    // a topology test: all five stored vertices must make three constrained
    // faces rather than raising `TooLarge`.
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
                    Point::new_unchecked_xy(y, x)
                } else {
                    Point::new_unchecked_xy(x, y)
                }
            };
            let points = [
                point(-large, 0.0),
                point(0.0, -tiny),
                point(large, 0.0),
                point(0.0, 2.0 * tiny),
            ];
            let triangulation = super::delaunay::delaunay_triangulation(&points);
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

    // Cached once per triangle: the pairwise disjointness scan below is
    // quadratic in the triangle count, so it must not pay for a geometric
    // intersection on pairs that cannot overlap.
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

    // Sweep in x rather than comparing all pairs: the largest fixtures reach a
    // few thousand triangles, where even a cheap quadratic scan dominates the
    // whole test. Sorting by the left edge lets each triangle stop as soon as
    // it reaches one that starts beyond its own right edge — every triangle
    // after that starts further right still, so none of them can overlap it.
    let span = |corners: &[Point; 3]| {
        (
            corners[0].x.min(corners[1].x).min(corners[2].x),
            corners[0].x.max(corners[1].x).max(corners[2].x),
        )
    };
    let mut order: Vec<usize> = (0..triangles.len()).collect();
    order.sort_by(|&a, &b| span(&all_corners[a]).0.total_cmp(&span(&all_corners[b]).0));

    for (position, &left) in order.iter().enumerate() {
        let left_max_x = span(&all_corners[left]).1;
        for &right in &order[position + 1..] {
            if span(&all_corners[right]).0 > left_max_x {
                break;
            }
            // A separating axis proves the pair cannot share interior area, so
            // the exact overlay below only runs on pairs that might genuinely
            // overlap — in a correct triangulation, just the edge- and
            // corner-adjacent ones. Axis-aligned boxes are not enough on their
            // own here: star fixtures produce long thin slivers radiating from
            // the centre whose boxes overlap even when the triangles do not.
            if triangles_are_separated(&all_corners[left], &all_corners[right]) {
                continue;
            }
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

/// Separating-axis test over the two triangles' six edge normals.
///
/// `true` means some axis separates them, which for convex shapes proves they
/// share no interior area. Contact along a shared edge or corner also reports
/// separated — the projections meet without crossing — and that is exactly
/// right: such a pair has zero intersection area and does not overlap, so
/// screening it out cannot change either assertion below.
///
/// `false` is the conservative answer: the caller falls through to the exact
/// overlay, so a genuine overlap is never screened away.
fn triangles_are_separated(left: &[Point; 3], right: &[Point; 3]) -> bool {
    let project = |corners: &[Point; 3], nx: f64, ny: f64| {
        let mut lo = f64::INFINITY;
        let mut hi = f64::NEG_INFINITY;
        for corner in corners {
            let value = corner.x * nx + corner.y * ny;
            lo = lo.min(value);
            hi = hi.max(value);
        }
        (lo, hi)
    };
    for corners in [left, right] {
        for edge in 0..3 {
            let from = corners[edge];
            let to = corners[(edge + 1) % 3];
            let (nx, ny) = (-(to.y - from.y), to.x - from.x);
            let (left_lo, left_hi) = project(left, nx, ny);
            let (right_lo, right_hi) = project(right, nx, ny);
            if left_hi <= right_lo || right_hi <= left_lo {
                return true;
            }
        }
    }
    false
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
fn earcut_holes_containment_scales_linearly_in_triangle_count() {
    // Structural subquadratic property (no wall-clock): a shell with h
    // simple interior holes triangulates, and triangle count grows as
    // Θ(vertices) — specifically the earcut output length is monotone in
    // hole_count and at most linear in total ring vertices (Euler: a
    // polygonal region with n boundary verts and h holes has ≤ n + 2h − 2
    // triangles; we assert the realized count stays within a small factor
    // of the vertex count, ruling out a quadratic fan-out of triangles).
    let mut prev_triangles = 0_usize;
    for hole_count in [10_usize, 20, 40, 80] {
        let poly = square_shell_with_grid_holes(hole_count);
        let vertex_count = poly.coord_count();
        let source = Shape::Polygon(poly);
        let triangles = source
            .polygon_triangles()
            .expect("many-hole polygon triangulates");
        let n_tri = triangles.len();
        assert!(
            n_tri > prev_triangles,
            "triangle count must grow with holes: hole_count={hole_count} n_tri={n_tri} prev={prev_triangles}"
        );
        assert!(
            n_tri <= vertex_count.saturating_mul(2),
            "triangle count must stay linear in vertices: hole_count={hole_count} n_tri={n_tri} verts={vertex_count}"
        );
        prev_triangles = n_tri;
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
// names its own size. Earcut has no size-dependent threshold (`z_order` is
// built unconditionally), so these figures are a scale ladder, not a boundary.
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
