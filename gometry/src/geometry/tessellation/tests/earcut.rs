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
fn earcut_id_callback_keeps_source_sites_and_distinct_bridge_occurrences() {
    let shell = vec![
        xyz(0.0, 0.0, 1.0),
        xyz(4.0, 0.0, 2.0),
        xyz(8.0, 0.0, 3.0),
        xyz(8.0, 8.0, 4.0),
        xyz(0.0, 8.0, 5.0),
        xyz(0.0, 0.0, 1.0),
    ];
    let hole = vec![
        xy(2.0, 2.0),
        xy(2.0, 4.0),
        xy(4.0, 4.0),
        xy(4.0, 2.0),
        xy(2.0, 2.0),
    ];
    let source = polygon(shell.clone(), vec![hole.clone()]);
    let mut triangles = Vec::new();
    super::earcut::earcut_polygon_with_ids(&source, &mut |triangle| {
        triangles.push(triangle);
        Ok(())
    })
    .unwrap();
    assert!(!triangles.is_empty());

    let mut occurrences_by_site = std::collections::HashMap::<
        super::earcut::EarcutSiteId,
        std::collections::HashSet<super::earcut::EarcutOccurrenceId>,
    >::new();
    let mut sites_by_occurrence = std::collections::HashMap::<
        super::earcut::EarcutOccurrenceId,
        super::earcut::EarcutSiteId,
    >::new();
    for (selected_face, triangle) in triangles.iter().enumerate() {
        assert_eq!(triangle.face, selected_face);
        let mut triangle_occurrences = std::collections::HashSet::new();
        for corner in 0..3 {
            let identity = triangle.corners[corner];
            let source = match identity.site.ring {
                0 => shell.get(identity.site.vertex),
                1 => hole.get(identity.site.vertex),
                ring => panic!("unexpected source ring {ring}"),
            }
            .expect("callback site ordinal belongs to its source ring");
            assert_eq!(triangle.points[corner], *source);
            assert_ne!(
                (identity.site.ring, identity.site.vertex),
                (0, 1),
                "the removed collinear source corner must not be reidentified"
            );
            assert!(triangle_occurrences.insert(identity.occurrence));
            if let Some(previous) = sites_by_occurrence.insert(identity.occurrence, identity.site) {
                assert_eq!(previous, identity.site);
            }
            occurrences_by_site
                .entry(identity.site)
                .or_default()
                .insert(identity.occurrence);
        }
    }
    assert!(
        occurrences_by_site
            .values()
            .any(|occurrences| occurrences.len() > 1),
        "a bridge copy must keep its source site while owning another occurrence ID"
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
            source_plan.locate_stored(centroid_point).unwrap().location,
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

#[rstest::rstest]
#[case::positive_area_overlap("positive-area overlap", [xy(0.5, 0.5), xy(2.5, 0.5), xy(0.5, 2.5)], false)]
#[case::shared_edge("shared edge", [xy(2.0, 0.0), xy(4.0, 0.0), xy(0.0, 2.0)], true)]
#[case::shared_corner("shared corner", [xy(2.0, 0.0), xy(3.0, -1.0), xy(3.0, 1.0)], true)]
#[case::disjoint("disjoint", [xy(3.0, 0.0), xy(4.0, 0.0), xy(3.0, 1.0)], true)]
fn exact_triangle_oracle_matches_overlay_for_overlap_contact_and_disjoint_pairs(
    #[case] case: &'static str,
    #[case] right_corners: [Point; 3],
    #[case] interiors_are_disjoint: bool,
) {
    let left_corners = [xy(0.0, 0.0), xy(2.0, 0.0), xy(0.0, 2.0)];
    let left = triangle_shape(&left_corners);

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
    for vertices in [128_usize, 256] {
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
    for vertices in [96_usize, 192] {
        let points = (0..vertices)
            .map(|index| {
                let radius = if index % 2 == 0 { 0.8 } else { 1.0 };
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
            long.push(
                if first.x.partial_cmp(&last.x) == Some(std::cmp::Ordering::Equal) {
                    xy(first.x - outward, last.y + (first.y - last.y) * t)
                } else {
                    xy(last.x + (first.x - last.x) * t, first.y - outward)
                },
            );
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
fn validate_earcut_ring_accepts_an_indexed_simple_ring() {
    let ring = large_convex_ring(129);
    validate_earcut_ring(&ring).expect("large simple ring is valid");
    cleaned_earcut_ring(&ring, false).expect("large simple ring cleans");
}

#[test]
fn validate_earcut_ring_rejects_an_indexed_self_intersecting_ring() {
    let mut ring = large_convex_ring(129);
    ring[64] = ring[16];
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
    assert_earcut_oracle(
        polygon(convex_ring(9, 100.0), vec![
            axis_aligned_square(-20.0, 0.0, 3.0),
            axis_aligned_square(20.0, 0.0, 3.0),
        ]),
        19,
    );
    assert_earcut_oracle(
        polygon(star_ring(12, 100.0, 40.0), vec![
            axis_aligned_square(-15.0, -10.0, 2.0),
            axis_aligned_square(15.0, -10.0, 2.0),
            axis_aligned_square(0.0, 15.0, 2.0),
        ]),
        28,
    );
    assert_earcut_oracle(
        polygon(convex_ring(16, 100.0), vec![
            axis_aligned_square(-30.0, -30.0, 5.0),
            axis_aligned_square(30.0, -30.0, 5.0),
            axis_aligned_square(-30.0, 30.0, 5.0),
            axis_aligned_square(30.0, 30.0, 5.0),
        ]),
        38,
    );
    assert_earcut_oracle(
        polygon(star_ring(15, 100.0, 40.0), vec![
            axis_aligned_square(-15.0, -10.0, 2.0),
            axis_aligned_square(15.0, -10.0, 2.0),
            axis_aligned_square(-10.0, 15.0, 2.0),
            axis_aligned_square(12.0, 15.0, 2.0),
        ]),
        37,
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
    let shell = large_convex_ring(8);
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

#[rstest::rstest]
#[case::hole_count_1_usize(1_usize)]
#[case::hole_count_4(4)]
#[case::hole_count_9(9)]
fn earcut_grid_holes_preserve_euler_triangle_count(#[case] hole_count: usize) {
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

#[rstest::rstest]
#[case::shell_vertices_64(64)]
#[case::shell_vertices_65(65)]
fn earcut_offset_holes_bridge_across_candidate_index_admission(#[case] shell_vertices: usize) {
    let source = Shape::Polygon(square_shell_with_offset_holes(shell_vertices));
    let triangles = source
        .polygon_triangles()
        .expect("holed polygon triangulates");
    assert_eq!(triangles.len(), shell_vertices.wrapping_add(16));
}
