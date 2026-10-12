#[test]
fn rounded_fan_order_matches_exact_stored_points() {
    let points = [
        XY::new(-1.0, -0.0),
        XY::new(1.0, -0.0),
        XY::new(0.0, 1.0),
        XY::new(-1.0, 0.0),
        XY::new(0.0, -1.0),
    ];
    for origin in points {
        for left in points {
            for right in points {
                let actual = rounded_fan_angle_cmp(origin, left, right);
                let exact = exact::angle_ccw_cmp(
                    &exact::ExactPoint::from_xy(origin),
                    &exact::ExactPoint::from_xy(left),
                    &exact::ExactPoint::from_xy(right),
                );
                assert_eq!(actual.is_none(), exact.is_err());
                if let (Some(actual), Ok(exact)) = (actual, exact) {
                    assert_eq!(actual, exact);
                }
            }
        }
    }
}

#[test]
fn ordinary_five_site_diagram_is_owned_and_admitted() {
    let sites: Vec<_> = [(0.0, 0.0), (0.0, 1.0), (0.5, 0.4), (1.0, 0.0), (1.0, 1.0)]
        .into_iter()
        .enumerate()
        .map(|(id, (x, y))| Site::raw(id, Point::new_raw_xy(x, y), id))
        .collect();
    let mut budget = ExpansionBudget::new("test", "Voronoi topology");
    let diagram = build(
        &sites,
        VoronoiBoundary::Padded,
        &mut budget,
        true,
        &certified_delaunay(&sites).unwrap(),
    )
    .unwrap();
    assert_eq!(diagram.polygons.len(), sites.len());
    assert!(
        diagram
            .polygons
            .iter()
            .all(|output| polygon_is_valid(&output.polygon).unwrap())
    );
}

#[test]
fn cosine_circle_fails_open_to_generic_coverage() {
    let sites: Vec<_> = (0..8)
        .map(|index| {
            let angle = f64::from(index) * std::f64::consts::TAU / 8.0;
            Point::new_raw_xy(10.0 * angle.cos(), 10.0 * angle.sin())
        })
        .enumerate()
        .map(|(id, point)| Site::raw(id, point, id))
        .collect();
    let mut budget = ExpansionBudget::new("test", "cosine circle");
    let diagram = build(
        &sites,
        VoronoiBoundary::Envelope,
        &mut budget,
        true,
        &certified_delaunay(&sites).unwrap(),
    )
    .unwrap();
    assert_eq!(diagram.polygons.len(), sites.len());
    assert!(
        diagram
            .polygons
            .iter()
            .all(|output| polygon_is_valid(&output.polygon).unwrap())
    );
}

fn assert_directed_partition(cells: &[Shape], bounds: [f64; 4]) {
    let mut edges: std::collections::BTreeMap<[PointKey; 2], Vec<[XY; 2]>> =
        std::collections::BTreeMap::new();
    for cell in cells {
        let Shape::Polygon(polygon) = cell else {
            panic!("Voronoi cells are polygons");
        };
        for (start, end) in polygon.shell.iter().zip(polygon.shell.iter().skip(1)) {
            let edge = [start.xy(), end.xy()];
            let keys = edge.map(PointKey::new);
            let key = if keys[0] <= keys[1] {
                keys
            } else {
                [keys[1], keys[0]]
            };
            edges.entry(key).or_default().push(edge);
        }
    }
    let mut sides: [Vec<(f64, f64)>; 4] = [Vec::new(), Vec::new(), Vec::new(), Vec::new()];
    for directions in edges.values() {
        match directions.as_slice() {
            [edge] => {
                let [a, b] = *edge;
                let (side, correctly_directed) = if a.y == bounds[2] && b.y == bounds[2] {
                    (0, a.x < b.x)
                } else if a.x == bounds[1] && b.x == bounds[1] {
                    (1, a.y < b.y)
                } else if a.y == bounds[3] && b.y == bounds[3] {
                    (2, a.x > b.x)
                } else if a.x == bounds[0] && b.x == bounds[0] {
                    (3, a.y > b.y)
                } else {
                    panic!("unmatched edge is not on the clip boundary");
                };
                assert!(correctly_directed, "boundary edge has the wrong direction");
                let (lo, hi) = if side % 2 == 0 {
                    (a.x.min(b.x), a.x.max(b.x))
                } else {
                    (a.y.min(b.y), a.y.max(b.y))
                };
                sides[side].push((lo, hi));
            },
            [first, second] => {
                assert_eq!(
                    PointKey::new(first[0]),
                    PointKey::new(second[1]),
                    "internal edge directions disagree"
                );
                assert_eq!(
                    PointKey::new(first[1]),
                    PointKey::new(second[0]),
                    "internal edge directions disagree"
                );
            },
            _ => panic!("edge has neither one nor two owners"),
        }
    }
    for (side_index, side) in sides.iter_mut().enumerate() {
        side.sort_unstable_by(|left, right| left.0.total_cmp(&right.0));
        let (start, end) = if side_index % 2 == 0 {
            (bounds[0], bounds[1])
        } else {
            (bounds[2], bounds[3])
        };
        let mut cursor = start;
        for &(lo, hi) in side.iter() {
            assert_eq!(lo, cursor, "clip boundary has a gap or overlap");
            cursor = hi;
        }
        assert_eq!(cursor, end, "clip boundary is not tiled");
    }
}

#[test]
fn extreme_cluster_is_a_valid_binary64_partition() {
    let points = [
        (1.552_304_940_926_043_4e127, -2.245_414_791_420_301e127),
        (1.552_304_940_919_857_5e127, -2.245_414_791_417_300_5e127),
        (1.552_304_940_926_441_2e127, -2.245_414_791_413_082e127),
        (1.552_304_940_932_905_3e127, -2.245_414_791_419_367e127),
        (1.552_304_940_935_468e127, -2.245_414_791_425_475_7e127),
        (1.552_304_940_933_517_1e127, -2.245_414_791_411_899e127),
        (1.552_304_940_934_345_6e127, -2.245_414_791_424_966e127),
        (1.552_304_940_920_426_7e127, -2.245_414_791_410_440_7e127),
        (1.552_304_940_920_431e127, -2.245_414_791_420_247_5e127),
        (1.552_304_940_933_332_2e127, -2.245_414_791_413_508e127),
        (1.552_304_940_916_651_8e127, -2.245_414_791_420_999e127),
        (1.552_304_940_931_348_2e127, -2.245_414_791_412_127e127),
        (1.552_304_940_931_112_7e127, -2.245_414_791_410_722_6e127),
        (1.552_304_940_915_999_5e127, -2.245_414_791_419_609_6e127),
        (1.552_304_940_921_551e127, -2.245_414_791_419_905_5e127),
        (1.552_304_940_935_471_1e127, -2.245_414_791_419_521e127),
        (1.552_304_940_921_168_7e127, -2.245_414_791_406_195_6e127),
        (1.552_304_940_932_703_6e127, -2.245_414_791_411_408e127),
        (1.552_304_940_925_026_4e127, -2.245_414_791_418_615e127),
        (1.552_304_940_934_617_9e127, -2.245_414_791_422_664_4e127),
        (1.552_304_940_917_422_5e127, -2.245_414_791_420_993e127),
        (1.552_304_940_928_658_7e127, -2.245_414_791_420_993e127),
        (1.552_304_940_921_511e127, -2.245_414_791_417_344_7e127),
        (1.552_304_940_919_395e127, -2.245_414_791_412_605_6e127),
        (1.552_304_940_921_255e127, -2.245_414_791_417_260_6e127),
        (1.552_304_940_933_857e127, -2.245_414_791_409_97e127),
    ];
    let source = Shape::MultiPoint(
        points
            .into_iter()
            .map(|(x, y)| Point::new_raw_xy(x, y))
            .collect::<Vec<_>>()
            .into(),
    );
    let cells = source
        .voronoi_cells(0.0, VoronoiBoundary::Padded)
        .expect("extreme cluster should be a valid partition");
    assert_eq!(cells.len(), points.len());
    assert!(cells.iter().all(|cell| {
        let Shape::Polygon(polygon) = cell else {
            return false;
        };
        polygon_is_valid(polygon).unwrap()
    }));
    let canonical = shape::canonical_voronoi_sites(
        points
            .into_iter()
            .map(|(x, y)| Point::new_raw_xy(x, y))
            .collect(),
        0.0,
    )
    .unwrap();
    for (site, cell) in canonical.iter().zip(&cells) {
        let Shape::Polygon(polygon) = cell else {
            unreachable!("Voronoi cells are polygons");
        };
        let rounded_ring: Vec<_> = polygon
            .shell
            .iter()
            .take(polygon.shell.len() - 1)
            .map(|point| exact::ExactPoint::from_xy(point.xy()))
            .collect();
        assert_ne!(
            exact::point_in_cycle(&rounded_ring, &exact::ExactPoint::from_xy(site.point.xy()),)
                .unwrap(),
            exact::PointInCycle::Outside,
        );
    }
    let padded = normalize_clip(&canonical, VoronoiBoundary::Padded, false).unwrap();
    let bounds = padded.frame_bounds();
    assert_directed_partition(&cells, bounds);
    assert!(coverage_is_valid(&cells, 0.0).unwrap());
    let clip = Shape::Polygon(Polygon::new(
        rectangle(bounds[0], bounds[2], bounds[1], bounds[3]),
        Vec::new(),
    ));
    let union = Shape::union_all(&cells, Strictness::Lenient).unwrap();
    assert!(
        clip.difference(&union, Strictness::Lenient)
            .unwrap()
            .is_empty()
    );
    assert!(
        union
            .difference(&clip, Strictness::Lenient)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn separated_repair_classes_defer_spanning_ring_orientation_to_global_check() {
    let sites = sites_xy(&[(0.0, -1.0), (0.0, 0.1), (0.0, 1.0), (100.0, 0.0)]);
    let source = Shape::MultiPoint(
        sites
            .iter()
            .map(|site| site.point)
            .collect::<Vec<_>>()
            .into(),
    );
    let clip_polygon = Polygon::new(
        closed_ring(&[
            (-1.0, -2.0),
            (1.0, -2.0),
            (1.0, -0.45),
            (1.0, -0.25),
            (1.0, 0.25),
            (1.0, 0.55),
            (1.0, 2.0),
            (-1.0, 2.0),
            (-1.0, 0.55),
            (-1.0, 0.25),
            (-1.0, -0.25),
            (-1.0, -0.45),
        ]),
        Vec::new(),
    );
    assert!(polygon_is_valid(&clip_polygon).unwrap());

    let mut budget = ExpansionBudget::new("voronoi_edge_lines", "cross-class ring regression");
    let (coordinates, offsets) = source
        .voronoi_edge_column_budgeted(0.0, VoronoiBoundary::Polygon(&clip_polygon), &mut budget)
        .expect("edge embedding should defer the spanning ring to the global certificate");
    assert_eq!(offsets.as_slice(), [0, 2, 4]);
    assert_eq!(coordinates.len(), 4);

    let clip = Shape::Polygon(clip_polygon);
    let mut lower_separator = false;
    let mut upper_separator = false;
    for [start, end] in offsets.as_slice().array_windows::<2>() {
        assert_eq!(*end - *start, 2);
        let points = [
            coordinates.point_at(*start as usize),
            coordinates.point_at(*end as usize - 1),
        ];
        let [first_point, second_point] = points;
        let (first, second) = (first_point.xy(), second_point.xy());
        assert_ne!(first, second, "each emitted chain is nondegenerate");
        let line = Shape::LineString(
            LineSeq::try_new(CoordSeq::try_from_points(&points).unwrap()).unwrap(),
        );
        assert!(
            clip.covers(&line).unwrap(),
            "each emitted chain lies in the clip"
        );

        let (west, east) = if first.x < second.x {
            (first, second)
        } else {
            (second, first)
        };
        assert!(west.x < east.x);
        let (below, above, seen) = if f64::midpoint(west.y, east.y) < 0.0 {
            (
                sites[0].point.xy(),
                sites[1].point.xy(),
                &mut lower_separator,
            )
        } else {
            (
                sites[1].point.xy(),
                sites[2].point.xy(),
                &mut upper_separator,
            )
        };
        assert_eq!(
            exact::orient2d(west, east, below),
            ExactSign::Negative,
            "the lower owner lies below its separator"
        );
        assert_eq!(
            exact::orient2d(west, east, above),
            ExactSign::Positive,
            "the upper owner lies above its separator"
        );
        assert!(!*seen, "each expected site pair has one separator chain");
        *seen = true;
    }
    assert!(lower_separator && upper_separator);
}

#[test]
fn thin_hole_clip_cells_cover_the_assigned_noded_boundary() {
    let sites = sites_xy(&[(0.0, -1.0), (0.0, 0.1), (0.0, 1.0), (100.0, 0.0)]);
    let source = Shape::MultiPoint(
        sites
            .iter()
            .map(|site| site.point)
            .collect::<Vec<_>>()
            .into(),
    );
    let clip = Polygon::new(
        closed_ring(&[
            (-1.0, -2.0),
            (1.0, -2.0),
            (1.0, -0.45),
            (1.0, -0.25),
            (1.0, 0.25),
            (1.0, 0.55),
            (1.0, 2.0),
            (-1.0, 2.0),
            (-1.0, 0.55),
            (-1.0, 0.25),
            (-1.0, -0.25),
            (-1.0, -0.45),
        ]),
        vec![closed_ring(&[
            (-1.0, 0.5),
            (-0.999_999_999_999_999_8, 0.75),
            (-0.5, 0.75),
        ])],
    );
    assert!(polygon_is_valid(&clip).unwrap());

    let mut edge_budget =
        ExpansionBudget::new("voronoi_edge_lines", "thin-hole embedding regression");
    let (coordinates, offsets) = source
        .voronoi_edge_column_budgeted(0.0, VoronoiBoundary::Polygon(&clip), &mut edge_budget)
        .unwrap();
    let mut polygon_budget =
        ExpansionBudget::new("voronoi_cells", "thin-hole embedding regression");
    let cells = source
        .voronoi_cells_budgeted(0.0, VoronoiBoundary::Polygon(&clip), &mut polygon_budget)
        .unwrap();
    assert!(offsets.as_slice().len() > 1);
    assert_eq!(
        offsets.as_slice().last().copied().map(|end| end as usize),
        Some(coordinates.len())
    );
    for [start, end] in offsets.as_slice().array_windows::<2>() {
        let points = (*start as usize..*end as usize)
            .map(|index| coordinates.point_at(index))
            .collect::<Vec<_>>();
        assert!(points.len() >= 2);
        assert_ne!(points.first().unwrap().xy(), points.last().unwrap().xy());
        assert!(
            points
                .array_windows::<2>()
                .all(|&[left, right]| left.xy() != right.xy()),
            "the edge chain has no zero-length segment"
        );
    }
    let assigned_clip = assigned_clip_from_cells(&sites, &clip, &cells);
    assert!(polygon_is_valid(&assigned_clip).unwrap());
    assert_cells_cover_clip(&cells, &assigned_clip);
    assert!(coverage_is_valid(&cells, 0.0).unwrap());
    let mut right_rows = cells.iter();
    for left in &cells {
        right_rows.next();
        for right in right_rows.clone() {
            assert!(left.relate_pattern(right, "F********").unwrap());
        }
    }
    for site in &sites {
        let point = Shape::Point(site.point);
        let owned = cells
            .iter()
            .filter(|cell| cell.covers(&point).unwrap())
            .count();
        let in_clip = Shape::Polygon(assigned_clip.clone())
            .covers(&point)
            .unwrap();
        assert_eq!(owned, usize::from(in_clip));
    }
    assert_separator_chains_covered_by_cells(&coordinates, offsets.as_slice(), &cells);
}

#[test]
fn five_site_binary64_reproduction_is_valid_for_polygons_and_edges() {
    let sites = sites_xy(&[(4.0, 3.0), (4.0, 4.0), (3.0, 2.0), (1.0, 3.0), (6.0, 5.0)]);
    let source = Shape::MultiPoint(
        sites
            .iter()
            .map(|site| site.point)
            .collect::<Vec<_>>()
            .into(),
    );

    let cells = source
        .voronoi_cells(0.0, VoronoiBoundary::Padded)
        .expect("adjacent-float search should repair the public reproduction");
    assert_eq!(cells.len(), sites.len());
    assert!(
        cells.iter().all(
            |cell| matches!(cell, Shape::Polygon(polygon) if polygon_is_valid(polygon).unwrap())
        )
    );
    assert!(coverage_is_valid(&cells, 0.0).unwrap());
    for site in &sites {
        let point = Shape::Point(site.point);
        assert_eq!(
            cells
                .iter()
                .filter(|cell| cell.covers(&point).unwrap())
                .count(),
            1
        );
    }

    let mut budget = ExpansionBudget::new("voronoi_edge_lines", "five-site reproduction");
    let (coordinates, offsets) = source
        .voronoi_edge_column_budgeted(0.0, VoronoiBoundary::Padded, &mut budget)
        .expect("the edge product should certify the same binary64 embedding");
    assert_eq!(
        offsets.as_slice().last().copied().map(|end| end as usize),
        Some(coordinates.len())
    );
    assert!(offsets.as_slice().len() > 1);
}

#[test]
fn near_tangent_separator_uses_the_assigned_clip_boundary() {
    let sites = sites_xy(&[(8.0, 4.0), (12.0, 5.0), (0.0, -100.0)]);
    let source = Shape::MultiPoint(
        sites
            .iter()
            .map(|site| site.point)
            .collect::<Vec<_>>()
            .into(),
    );
    let clip = Polygon::new(
        closed_ring(&[
            (0.0, 0.0),
            (10.0, 0.0),
            (10.0, 10.0),
            (4.0, 14.0),
            (0.0, 10.0),
        ]),
        vec![closed_ring(&[
            (7.0, 12.0),
            (8.5, 10.999_999_999_999_998),
            (7.5, 10.5),
        ])],
    );

    let mut edge_budget = ExpansionBudget::new("voronoi_edge_lines", "near-tangent assigned clip");
    let (edge_coordinates, offsets) = source
        .voronoi_edge_column_budgeted(0.0, VoronoiBoundary::Polygon(&clip), &mut edge_budget)
        .expect("the edge separator lies in the assigned noded clip");
    assert_eq!(offsets.as_slice(), [0, 2, 4]);
    let edge_points: Vec<_> = (0..edge_coordinates.len())
        .map(|index| edge_coordinates.point_at(index).xy())
        .collect();
    let shell_hit = XY::new(8.35, 11.1);
    let hole_hit = XY::new(8.35, 11.099_999_999_999_998);
    assert!(edge_points.contains(&shell_hit));
    assert!(edge_points.contains(&hole_hit));
    let (edge_pairs, trailing_points) = edge_points.as_chunks::<2>();
    assert!(trailing_points.is_empty());
    assert!(edge_pairs.iter().any(|&[start, end]| {
        (start == shell_hit && end == hole_hit) || (start == hole_hit && end == shell_hit)
    }));

    let mut polygon_budget = ExpansionBudget::new("voronoi_cells", "near-tangent assigned clip");
    let cells = source
        .voronoi_cells_budgeted(0.0, VoronoiBoundary::Polygon(&clip), &mut polygon_budget)
        .expect("the same selected embedding produces valid polygon cells");
    assert_eq!(cells.len(), 3);
    assert!(
        cells.iter().all(
            |cell| matches!(cell, Shape::Polygon(polygon) if polygon_is_valid(polygon).unwrap())
        )
    );
    let mut polygon_points = Vec::new();
    for cell in &cells {
        let Shape::Polygon(polygon) = cell else {
            panic!("Voronoi polygon output contains only polygons");
        };
        polygon_points.extend(polygon.shell.iter().map(|point| point.xy()));
        for hole in polygon.holes.iter() {
            polygon_points.extend(hole.iter().map(|point| point.xy()));
        }
    }
    assert!(
        edge_points
            .iter()
            .all(|point| polygon_points.contains(point))
    );
}

#[test]
fn rounded_ring_orientation_mutant_is_rejected_by_the_embedding_certificate() {
    let nearest = [XY::new(0.0, 0.0), XY::new(2.0, 0.0), XY::new(0.0, 2.0)];
    let mut vertices = EmbeddedVertices::new(nearest.len());
    for index in 0..nearest.len() {
        vertices.set(VertexId(index as u32), EmbeddedVertexId(index as u32));
    }
    let subdivision = ExactSubdivision {
        vertices: nearest
            .iter()
            .map(|&point| ExactVertex {
                point: exact::ExactPoint::from_xy(point),
                fixed_xy: None,
            })
            .collect(),
        edges: Vec::new(),
        topology: IndexedTopology::default(),
        edge_of_slot: Vec::new(),
        labels: Vec::new(),
    };
    let ring = ExactSimpleRing {
        face: BoundaryFaceId(0),
        vertices: vec![VertexId(0), VertexId(1), VertexId(2), VertexId(0)],
        edges: Vec::new(),
        orientation: ExactSign::Positive,
    };
    let parts = [ExactCellPart {
        owner: SiteId(0),
        face: ring.face,
        shell: ring,
        holes: Vec::new(),
    }];
    let live_nodes = [VertexId(0), VertexId(1), VertexId(2)];
    let mut rings_by_node = vec![Vec::new(); nearest.len()];
    for rings in &mut rings_by_node {
        rings.push(0);
    }
    let clip = NormalizedClip {
        rings: Vec::new(),
        fixed: Vec::new(),
        chart_fixed: Vec::new(),
        source_roots: Vec::new(),
        source: None,
        geometry: None,
        polygon_baseline: None,
        contacts: Vec::new(),
    };
    let clip_trace = ClipTrace {
        rings: Vec::new(),
        atom_count: 0,
    };
    let embedding = VoronoiEmbedding {
        subdivision: &subdivision,
        clip: &clip,
        clip_trace: &clip_trace,
        sites: &[],
        parts: &parts,
        includes_polygons: true,
        live_nodes: &live_nodes,
        live_edges: &[],
        vertices: &vertices,
        coordinates: &nearest,
        chords: Vec::new(),
        incident_chords: vec![Vec::new(); nearest.len()],
        fans: Vec::new(),
        rings: vec![VoronoiRing {
            part: 0,
            hole: None,
        }],
        rings_by_node,
        host_graphs: Vec::new(),
        separator_chords: Vec::new(),
    };
    let mut budget = EvaluationBudget::new("rounded ring", 0).unwrap();
    assert!(
        embedding
            .collect_conflicts(&nearest, &mut budget)
            .unwrap()
            .is_empty()
    );

    let inverted = [nearest[0], nearest[1], XY::new(0.0, -2.0)];
    let conflicts = embedding.collect_conflicts(&inverted, &mut budget).unwrap();
    assert!(matches!(conflicts.as_slice(), [VoronoiConflict::Ring(0)]));

    let degenerate = [nearest[0], nearest[1], XY::new(1.0, 0.0)];
    let conflicts = embedding
        .collect_conflicts(&degenerate, &mut budget)
        .unwrap();
    assert!(matches!(conflicts.as_slice(), [VoronoiConflict::Ring(0)]));
}

#[test]
fn assigned_clip_host_conflicts_seed_and_blame_separator_only_class() {
    let shell_points = [
        (0.0, 0.0),
        (10.0, 0.0),
        (10.0, 10.0),
        (0.0, 10.0),
        (0.0, 5.0),
    ];
    let hole_points = [(4.0, 4.0), (6.0, 4.0), (6.0, 6.0), (4.0, 6.0)];
    let clip_polygon = Polygon::new(closed_ring(&shell_points), vec![closed_ring(&hole_points)]);
    let shell: Vec<_> = shell_points.iter().map(|&(x, y)| XY::new(x, y)).collect();
    let hole: Vec<_> = hole_points.iter().map(|&(x, y)| XY::new(x, y)).collect();
    let clip_coordinates: Vec<_> = shell.iter().chain(&hole).copied().collect();
    let separator_nearest = XY::new(2.0, 5.0);
    let mut coordinate_table = clip_coordinates.clone();
    coordinate_table.push(separator_nearest);
    coordinate_table.sort_unstable_by(|left, right| lex_xy(*left, *right));
    assert!(
        coordinate_table
            .array_windows::<2>()
            .all(|&[left, right]| left != right),
        "the fixture assigns one coordinate to each exact vertex"
    );

    let subdivision_vertices = coordinate_table
        .iter()
        .map(|&point| ExactVertex {
            point: exact::ExactPoint::from_xy(point),
            fixed_xy: clip_coordinates.contains(&point).then_some(point),
        })
        .collect();
    let vertex_id = |point: XY| {
        VertexId(
            u32::try_from(
                coordinate_table
                    .binary_search_by(|candidate| lex_xy(*candidate, point))
                    .unwrap(),
            )
            .unwrap(),
        )
    };
    let anchor = vertex_id(XY::new(0.0, 5.0));
    let separator_only = vertex_id(separator_nearest);

    let clip = NormalizedClip {
        rings: vec![cycle(&shell_points), cycle(&hole_points)],
        chart_fixed: vec![shell.clone(), hole.clone()],
        source_roots: Vec::new(),
        fixed: vec![shell, hole],
        source: Some(&clip_polygon),
        geometry: Some(clip_polygon.clone()),
        polygon_baseline: None,
        contacts: Vec::new(),
    };
    let mut edges = Vec::new();
    let mut source_segment = 0_u32;
    for ring in [&shell_points[..], &hole_points[..]] {
        for index in 0..ring.len() {
            let next = index.wrapping_add(1) % ring.len();
            let start = vertex_id(XY::new(ring[index].0, ring[index].1));
            let end = vertex_id(XY::new(ring[next].0, ring[next].1));
            edges.push(AtomicEdge {
                endpoints: [start, end],
                forward: Transition {
                    frame_delta: 0,
                    clip_delta: 0,
                    owner: OwnerTransition::Preserve,
                },
                separator: None,
                clip_sources: vec![ClipSegmentId(source_segment)],
                frame_side: None,
            });
            source_segment = source_segment.wrapping_add(1);
        }
    }
    let separator_edge = EdgeId(u32::try_from(edges.len()).unwrap());
    edges.push(AtomicEdge {
        endpoints: [anchor, separator_only],
        forward: Transition {
            frame_delta: 0,
            clip_delta: 0,
            owner: OwnerTransition::Boundary {
                left: Some(SiteId(0)),
                right: Some(SiteId(1)),
            },
        },
        separator: Some([SiteId(0), SiteId(1)]),
        clip_sources: Vec::new(),
        frame_side: None,
    });
    let subdivision = ExactSubdivision {
        vertices: subdivision_vertices,
        edges,
        topology: IndexedTopology::default(),
        edge_of_slot: Vec::new(),
        labels: Vec::new(),
    };
    let mut trace_budget = EvaluationBudget::new("assigned clip test", 0).unwrap();
    let clip_trace = ClipTrace::build(&subdivision, &clip, &mut trace_budget).unwrap();
    let coordinates = coordinate_table.clone();
    let live_nodes: Vec<_> = (0..coordinates.len())
        .map(|index| VertexId(u32::try_from(index).unwrap()))
        .collect();
    let mut vertices = EmbeddedVertices::new(coordinates.len());
    for (index, &node) in live_nodes.iter().enumerate() {
        vertices.set(node, EmbeddedVertexId(u32::try_from(index).unwrap()));
    }
    let sites = sites_xy(&[(1.0, 7.0), (1.0, 3.0)]);
    let chord = ProximityChord {
        nodes: [anchor.0, separator_only.0],
        nearest: Segment {
            start: vertices.point(anchor, &coordinates).unwrap(),
            end: vertices.point(separator_only, &coordinates).unwrap(),
        },
        can_move: [false, true],
    };
    let live_edges = [separator_edge];
    let mut incident_chords = vec![Vec::new(); coordinates.len()];
    incident_chords[anchor.0 as usize].push(0);
    incident_chords[separator_only.0 as usize].push(0);
    let embedding = VoronoiEmbedding {
        subdivision: &subdivision,
        clip: &clip,
        clip_trace: &clip_trace,
        sites: &sites,
        parts: &[],
        includes_polygons: false,
        live_nodes: &live_nodes,
        live_edges: &live_edges,
        vertices: &vertices,
        coordinates: &coordinates,
        chords: vec![chord],
        incident_chords,
        fans: Vec::new(),
        rings: Vec::new(),
        rings_by_node: vec![Vec::new(); coordinates.len()],
        host_graphs: Vec::new(),
        separator_chords: vec![0],
    };

    let seeds = [separator_only];
    let mut problem = VoronoiProblem::new(&embedding, &seeds).unwrap();
    assert_eq!(problem.classes, [vec![separator_only]]);
    let variables = [separator_only];
    let accepted_values = [separator_nearest];
    let accepted_assignment = [ClassAssignment {
        variables: &variables,
        values: &accepted_values,
    }];
    let mut budget = EvaluationBudget::new("assigned clip test", 1).unwrap();
    assert!(
        embedding
            .collect_conflicts(&coordinates, &mut budget)
            .unwrap()
            .is_empty(),
        "the accepted separator is hosted by the assigned shell and hole"
    );
    assert!(matches!(
        problem
            .check_global(&accepted_assignment, &mut budget)
            .unwrap(),
        GlobalCheck::Pass(())
    ));

    for (candidate, ring) in [(XY::new(12.0, 5.0), 0), (XY::new(5.0, 5.0), 1)] {
        let values = [candidate];
        let assignment = [ClassAssignment {
            variables: &variables,
            values: &values,
        }];
        let mut assigned = coordinates.clone();
        vertices.set_point(separator_only, &mut assigned, candidate);
        let mut budget = EvaluationBudget::new("assigned clip test", 1).unwrap();
        let conflicts = embedding.collect_conflicts(&assigned, &mut budget).unwrap();
        let [VoronoiConflict::ClipHost(failure)] = conflicts.as_slice() else {
            panic!("outside-shell and inside-hole drawings must have one typed clip-host conflict");
        };
        assert_eq!(failure.chord, 0);
        assert_eq!(failure.boundary.ring, ring);
        assert!(failure.boundary.atom.is_some());
        let seeds = embedding.seed_nodes(&conflicts);
        assert!(seeds.contains(&anchor));
        assert!(seeds.contains(&separator_only));
        for atom in &clip_trace.rings[ring].atoms {
            for vertex in subdivision.edge(atom.edge).endpoints {
                assert!(seeds.contains(&vertex));
            }
        }

        let mut budget = EvaluationBudget::new("assigned clip test", 1).unwrap();
        let GlobalCheck::Reject(blamed) = problem.check_global(&assignment, &mut budget).unwrap()
        else {
            panic!("global certification must reject the assigned-boundary violation");
        };
        assert_eq!(
            blamed,
            [0],
            "the separator-only vertex's class is repairable"
        );
    }
}

#[test]
fn shifted_six_site_diagram_has_a_certified_binary64_partition() {
    let offset = 1e16;
    let sites: Vec<_> = [
        (0.0, 0.0),
        (10.0, 0.0),
        (10.0, 10.0),
        (0.0, 10.0),
        (5.0, 5.0),
        (2.0, 8.0),
    ]
    .into_iter()
    .enumerate()
    .map(|(id, (x, y))| Site::raw(id, Point::new_raw_xy(offset + x, offset + y), id))
    .collect();
    let source = Shape::MultiPoint(
        sites
            .iter()
            .map(|site| site.point)
            .collect::<Vec<_>>()
            .into(),
    );
    let cells = source
        .voronoi_cells(0.0, VoronoiBoundary::Envelope)
        .unwrap();
    let clip = Polygon::new(
        rectangle(offset, offset, offset + 10.0, offset + 10.0),
        Vec::new(),
    );
    assert_eq!(cells.len(), 6);
    assert!(coverage_is_valid(&cells, 0.0).unwrap());
    assert_cells_cover_clip(&cells, &clip);
    for site in sites {
        let point = Shape::Point(site.point);
        assert_eq!(
            cells
                .iter()
                .filter(|cell| cell.covers(&point).unwrap())
                .count(),
            1
        );
    }
}

#[test]
fn pinned_clip_vertices_exhaust_a_constructed_nodes_adjacent_float_domain() {
    let offset = 1e16;
    let sites = sites_xy(&[
        (offset - 4.0, offset),
        (offset + 8.0, offset + 2.0),
        (offset, offset + 10.0),
    ]);
    let origin = XY::new(offset, offset);
    let center = XY::new(offset + 2.0, offset + 2.0);
    let clip = Polygon::new(
        closed_ring(&[
            (offset, offset),
            (offset + 2.0, offset),
            (offset + 4.0, offset),
            (offset + 4.0, offset + 2.0),
            (offset + 4.0, offset + 4.0),
            (offset + 2.0, offset + 4.0),
            (offset, offset + 4.0),
            (offset, offset + 2.0),
            (offset + 2.0, offset + 2.0),
        ]),
        Vec::new(),
    );
    assert!(polygon_is_valid(&clip).unwrap());
    let subdivision = grafted_clip_subdivision(&sites, &clip);
    let [left, right, _] = sites.as_slice() else {
        panic!("the fixture has three sites");
    };
    let bisector =
        exact::ExactLine::perpendicular_bisector(left.point.xy(), right.point.xy()).unwrap();
    let diagonal = exact::ExactLine::through_points(
        &exact::ExactPoint::from_xy(origin),
        &exact::ExactPoint::from_xy(center),
    )
    .unwrap();
    // In relative coordinates the bisector is 6*x + y = 13 and the clip edge
    // is y = x. Their hit (13/7, 13/7) rounds to the pinned middle vertex.
    let hit = exact::line_intersection(&bisector, &diagonal).unwrap();
    let hit_vertex = subdivision.vertex_at(&hit).unwrap().unwrap();
    assert!(subdivision.vertex(hit_vertex).fixed_xy.is_none());
    assert_eq!(hit.round_nearest_even().unwrap(), center);
    assert!(
        !hit.same_position(&exact::ExactPoint::from_xy(center))
            .unwrap()
    );
    for x in [center.x.next_down(), center.x, center.x.next_up()] {
        for y in [center.y.next_down(), center.y, center.y.next_up()] {
            let candidate = XY::new(x, y);
            let pinned = subdivision
                .vertex_at(&exact::ExactPoint::from_xy(candidate))
                .unwrap()
                .unwrap();
            assert_eq!(subdivision.vertex(pinned).fixed_xy, Some(candidate));
        }
    }
    let source = Shape::MultiPoint(
        sites
            .iter()
            .map(|site| site.point)
            .collect::<Vec<_>>()
            .into(),
    );
    let mut edge_budget = ExpansionBudget::new("voronoi_edge_lines", "pinned clip vertices");
    for (operation, result) in [
        (
            "voronoi_cells",
            source
                .voronoi_cells(0.0, VoronoiBoundary::Polygon(&clip))
                .map(|_| ()),
        ),
        (
            "voronoi_edge_lines",
            source
                .voronoi_edge_column_budgeted(
                    0.0,
                    VoronoiBoundary::Polygon(&clip),
                    &mut edge_budget,
                )
                .map(|_| ()),
        ),
    ] {
        let error = result.expect_err("all nine adjacent coordinates are pinned");
        let crate::error::ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingUnrepresentable {
            operation: actual,
            ..
        }) = error.kind()
        else {
            panic!("unexpected error: {error}");
        };
        assert_eq!(*actual, operation);
    }
}

#[test]
fn polygon_clip_with_many_disjoint_holes_is_budgeted_by_output() {
    let holes = (0..1_334)
        .map(|index| {
            let x = 1.0 + f64::from(index % 50) * 0.7;
            let y = -8.0 + f64::from(index / 50) * 0.6;
            Ring::closed(vec![
                Point::new_raw_xy(x, y),
                Point::new_raw_xy(x + 0.2, y),
                Point::new_raw_xy(x + 0.1, y + 0.2),
            ])
            .unwrap()
        })
        .collect();
    let polygon = Polygon::new(rectangle(-10.0, -10.0, 40.0, 10.0), holes);
    assert!(polygon_is_valid(&polygon).unwrap());

    let source = Shape::MultiPoint(
        sites_xy(&[(-1.0, 0.0), (1.0, 0.0)])
            .into_iter()
            .map(|site| site.point)
            .collect::<Vec<_>>()
            .into(),
    );
    let mut budget = ExpansionBudget::new("voronoi_cells", "sites/clip topology");
    let cells = source
        .voronoi_cells_budgeted(0.0, VoronoiBoundary::Polygon(&polygon), &mut budget)
        .expect("the 5,346-position partition is below the generated-output budget");

    assert_eq!(cells.len(), 2);
    assert!(
        cells.iter().all(
            |cell| matches!(cell, Shape::Polygon(polygon) if polygon_is_valid(polygon).unwrap())
        )
    );
    let (hole_count, coordinate_count) = cells.iter().fold((0, 0), |(holes, coords), cell| {
        let Shape::Polygon(polygon) = cell else {
            unreachable!("Voronoi polygon output contains only polygons");
        };
        (
            holes + polygon.holes.len(),
            coords
                + polygon.shell.len()
                + polygon.holes.iter().map(|ring| ring.len()).sum::<usize>(),
        )
    });
    assert_eq!(hole_count, 1_334);
    assert_eq!(coordinate_count, 5_346);
    assert_eq!(budget.used(), 7);
}

#[test]
fn polygon_clip_never_publishes_separator_edges_through_holes() {
    let source = Shape::MultiPoint(
        [
            (7.363_407_761_916_353, 0.910_061_356_872_448_7),
            (6.090_183_077_044_946, -8.212_595_695_268_158),
            (36.882_862_911_401_645, 2.797_623_572_491_808_4),
            (36.301_612_116_132_375, -7.745_261_677_494_59),
            (28.239_545_834_041_202, 0.914_252_377_592_369_2),
            (-7.512_276_371_063_782, 0.431_337_981_564_331_23),
            (18.856_535_571_352_17, 3.728_851_987_116_291_5),
            (7.907_218_354_022_312, 6.144_878_172_929_207),
            (37.461_899_718_154_09, -4.374_205_394_511_301),
            (28.995_254_695_626_983, 8.082_142_040_528_609),
            (34.199_497_713_157_854, -4.726_233_471_603_177),
            (4.480_941_274_147_678, 0.874_366_871_259_853_8),
            (13.083_919_124_933_26, -7.950_635_799_513_469),
            (16.680_131_726_928_522, 8.985_083_973_425_464),
            (12.409_534_275_855_67, 0.875_640_236_445_242_7),
            (12.801_013_459_163_723, 8.594_561_991_431_593),
            (15.491_850_637_223_166, 4.641_750_373_890_741),
            (27.311_848_761_232, 6.604_639_667_276_992),
            (1.396_568_785_930_606_2, 1.939_113_266_429_83),
            (3.841_055_416_132_109, 6.533_768_346_517_97),
            (27.292_924_130_985_24, 4.143_438_302_403_387),
            (35.301_683_216_775_01, -0.571_294_127_869_450_1),
            (12.340_258_256_482_5, -1.424_072_102_708_254_1),
            (38.356_471_151_004_33, -3.732_209_537_823_021),
        ]
        .into_iter()
        .map(|(x, y)| Point::new_raw_xy(x, y))
        .collect::<Vec<_>>()
        .into(),
    );
    let holes = (0..13)
        .map(|index| {
            let x = 1.0 + f64::from(index % 50) * 0.7;
            let y = -8.0 + f64::from(index / 50) * 0.6;
            closed_ring(&[(x, y), (x + 0.2, y), (x + 0.1, y + 0.2)])
        })
        .collect();
    let polygon = Polygon::new(rectangle(-10.0, -10.0, 40.0, 10.0), holes);
    assert!(polygon_is_valid(&polygon).unwrap());

    let mut edge_budget = ExpansionBudget::new("voronoi_edge_lines", "sites/clip topology");
    let (coordinates, offsets) = match source.voronoi_edge_column_budgeted(
        0.0,
        VoronoiBoundary::Polygon(&polygon),
        &mut edge_budget,
    ) {
        Ok(output) => output,
        Err(error) => {
            assert!(
                matches!(
                    error.kind(),
                    crate::error::ErrorKind::Geometry(
                        GeometryErrorKind::ExactEmbeddingUnrepresentable {
                            operation: "voronoi_edge_lines",
                            ..
                        }
                    )
                ),
                "unexpected Voronoi error: {error}"
            );
            return;
        },
    };
    assert!(offsets.as_slice().len() > 1);
    assert_eq!(
        offsets.as_slice().last().copied().map(|end| end as usize),
        Some(coordinates.len())
    );

    let mut polygon_budget = ExpansionBudget::new("voronoi_cells", "sites/clip topology");
    let cells = source
        .voronoi_cells_budgeted(0.0, VoronoiBoundary::Polygon(&polygon), &mut polygon_budget)
        .expect("the same clip publishes polygon cells for the edge coverage check");
    assert_separator_chains_covered_by_cells(&coordinates, offsets.as_slice(), &cells);
}

#[test]
fn sparse_collinear_edges_do_not_charge_clip_scan_work() {
    let mut boundary = Vec::with_capacity(20_000);
    for (start, end, subdivisions) in [
        (XY::new(-2.0, -1.0), XY::new(2.0, -0.5), 9_999_u32),
        (XY::new(2.0, -0.5), XY::new(2.0, 1.0), 1),
        (XY::new(2.0, 1.0), XY::new(-2.0, 0.5), 9_999),
        (XY::new(-2.0, 0.5), XY::new(-2.0, -1.0), 1),
    ] {
        for step in 0..subdivisions {
            let fraction = f64::from(step) / f64::from(subdivisions);
            boundary.push(Point::new_raw_xy(
                start.x + (end.x - start.x) * fraction,
                start.y + (end.y - start.y) * fraction,
            ));
        }
    }
    let polygon = Polygon::new(Ring::closed(boundary).unwrap(), Vec::new());
    assert_eq!(polygon.shell.len() - 1, 20_000);

    let sites = (0_u32..801)
        .map(|index| Point::new_raw_xy(0.0, -0.5 + f64::from(index) / 800.0))
        .collect::<Vec<_>>();
    let source = Shape::MultiPoint(sites.into());
    let mut budget = ExpansionBudget::new("voronoi_edge_lines", "sites/clip topology");
    let (coordinates, offsets) = source
        .voronoi_edge_column_budgeted(0.0, VoronoiBoundary::Polygon(&polygon), &mut budget)
        .expect("800 separator edges fit the output budget");

    assert_eq!(offsets.as_slice().len(), 801);
    assert_eq!(offsets.as_slice().last(), Some(&1_600));
    assert_eq!(coordinates.len(), 1_600);
    assert_eq!(budget.used(), 2_400);
}

fn coincident_frame_fixture() -> (Vec<Site>, Polygon) {
    (
        sites_xy(&[(0.0, -1.0), (0.0, 1.0), (2.0, 0.5)]),
        Polygon::new(rectangle(-1.0, 0.0, 0.5, 2.0), Vec::new()),
    )
}

#[test]
fn canonical_separator_emitter_keeps_one_sorted_directed_edge_per_dual() {
    let (sites, polygon) = separator_fixture();
    let clip = normalize_clip(&sites, VoronoiBoundary::Polygon(&polygon), false).unwrap();
    let rect = clip_rectangle(&clip).unwrap();
    let mesh = certified_delaunay(&sites).unwrap();
    let complex = delaunay_complex(&mesh, &sites).unwrap();
    let primal_edges = mesh.primal_edges();
    let separators = emit_clipped_separators(&sites, &primal_edges, &complex, &rect).unwrap();
    let expected = primal_edges
        .iter()
        .filter(|edge| match edge {
            CertifiedPrimalEdge::Interior { faces, .. } => {
                complex.component_of_face(faces[0]) != complex.component_of_face(faces[1])
            },
            CertifiedPrimalEdge::Hull { .. } => true,
        })
        .count();
    assert_eq!(separators.len(), expected);
    for separator in &separators {
        assert!(separator.pair[0] < separator.pair[1]);
        let left = exact::ExactPoint::from_xy(sites[separator.pair[0].0 as usize].point.xy());
        assert_eq!(
            exact::orient_points(&separator.start, &separator.end, &left).unwrap(),
            ExactSign::Positive
        );
    }
}

#[test]
fn hull_center_outside_clip_aabb_keeps_the_enter_exit_chord() {
    let (sites, polygon) = coincident_frame_fixture();
    let clip = normalize_clip(&sites, VoronoiBoundary::Polygon(&polygon), false).unwrap();
    let rect = clip_rectangle(&clip).unwrap();
    let mesh = certified_delaunay(&sites).unwrap();
    let complex = delaunay_complex(&mesh, &sites).unwrap();
    let separators =
        emit_clipped_separators(&sites, &mesh.primal_edges(), &complex, &rect).unwrap();
    let pair = [SiteId(0), SiteId(1)];
    let chord = separators
        .iter()
        .find(|separator| separator.pair == pair)
        .expect("A-B hull ray clips to R");
    let bottom = [
        exact::ExactPoint::from_xy(XY::new(-1.0, 0.0)),
        exact::ExactPoint::from_xy(XY::new(0.5, 0.0)),
    ];
    assert!(
        chord.start.same_position(&bottom[0]).unwrap()
            && chord.end.same_position(&bottom[1]).unwrap()
            || chord.start.same_position(&bottom[1]).unwrap()
                && chord.end.same_position(&bottom[0]).unwrap()
    );
    let center = complex.center(0);
    assert!(!in_rectangle(&rect, center).unwrap());
    let adjacency = certified_site_adjacency(&mesh.primal_edges(), sites.len()).unwrap();
    let scaffold = build_scaffold(&rect, &clip).unwrap();
    let atoms =
        node_scaffold_and_separators(scaffold, separators, &sites, &adjacency, &clip, &rect)
            .unwrap();
    assert!(
        !atoms
            .vertices
            .iter()
            .any(|vertex| vertex.point.same_position(center).unwrap())
    );
}

#[test]
fn coincident_frame_separator_seeds_inside_owner_without_nearest_tie() {
    let (sites, polygon) = coincident_frame_fixture();
    let atoms = node_polygon(&sites, &polygon);
    let coincident: Vec<_> = atoms
        .edges
        .iter()
        .filter(|edge| edge.separator == Some([SiteId(0), SiteId(1)]) && edge.frame_side.is_some())
        .collect();
    assert!(!coincident.is_empty());
    for edge in &coincident {
        let OwnerTransition::Boundary { left, right } = edge.forward.owner else {
            panic!("coincident frame owner is not a boundary");
        };
        assert!(
            matches!(
                (left, right),
                (Some(SiteId(1)), None) | (None, Some(SiteId(1)))
            ),
            "coincident frame owner has wrong sites: {left:?}, {right:?}"
        );
        let midpoint = atoms.vertices[edge.endpoints[0].0 as usize]
            .point
            .midpoint(&atoms.vertices[edge.endpoints[1].0 as usize].point)
            .unwrap();
        nearest_site(&midpoint, &sites).unwrap_err();
    }
    let clip = normalize_clip(&sites, VoronoiBoundary::Polygon(&polygon), false).unwrap();
    let rect = clip_rectangle(&clip).unwrap();
    let mesh = certified_delaunay(&sites).unwrap();
    let adjacency = certified_site_adjacency(&mesh.primal_edges(), sites.len()).unwrap();
    let scaffold = build_scaffold(&rect, &clip).unwrap();
    assert!(
        node_scaffold_and_separators(scaffold, Vec::new(), &sites, &adjacency, &clip, &rect,)
            .is_err()
    );
}

#[test]
fn coincident_frame_separator_emits_one_packed_chain_and_excludes_frame_only() {
    let (sites, polygon) = coincident_frame_fixture();
    let mut budget = ExpansionBudget::new("test", "coincident packed");
    let lines = build_edges(
        &sites,
        VoronoiBoundary::Polygon(&polygon),
        &mut budget,
        true,
        &certified_delaunay(&sites).unwrap(),
    )
    .unwrap()
    .into_line_seqs()
    .unwrap();
    let bottom = [XY::new(-1.0, 0.0), XY::new(0.5, 0.0)];
    let matching = lines
        .iter()
        .filter(|line| {
            (0..line.len().saturating_sub(1)).any(|index| {
                let a = line.point_at(index).xy();
                let b = line.point_at(index + 1).xy();
                a == bottom[0] && b == bottom[1] || a == bottom[1] && b == bottom[0]
            })
        })
        .count();
    assert_eq!(matching, 1);
    let frame_only = lines.iter().any(|line| {
        (0..line.len()).all(|index| {
            let xy = line.point_at(index).xy();
            (xy.x == -1.0 || xy.x == 0.5 || xy.y == 0.0 || xy.y == 2.0)
                && !(xy == bottom[0]
                    || xy == bottom[1]
                    || (xy.y == 0.0 && xy.x > -1.0 && xy.x < 0.5))
        }) && (0..line.len().saturating_sub(1)).all(|index| {
            let a = line.point_at(index).xy();
            let b = line.point_at(index + 1).xy();
            !(a == bottom[0] && b == bottom[1] || a == bottom[1] && b == bottom[0])
        })
    });
    assert!(!frame_only, "frame-only support appeared as a packed chain");
}

#[test]
fn edges_only_embedding_publishes_certified_chains_with_colliding_nearest_vertices() {
    let offset = 1e16;
    let sites: Vec<_> = [
        (0.0, 0.0),
        (10.0, 0.0),
        (10.0, 10.0),
        (0.0, 10.0),
        (5.0, 5.0),
        (2.0, 8.0),
    ]
    .into_iter()
    .enumerate()
    .map(|(id, (x, y))| Site::raw(id, Point::new_raw_xy(offset + x, offset + y), id))
    .collect();
    let clip = Polygon::new(
        rectangle(offset - 1.0, offset - 1.0, offset + 11.0, offset + 11.0),
        Vec::new(),
    );
    let mut budget = ExpansionBudget::new("test", "edges-only collision");
    let lines = build_edges(
        &sites,
        VoronoiBoundary::Polygon(&clip),
        &mut budget,
        true,
        &certified_delaunay(&sites).unwrap(),
    )
    .expect("the emitted separator chains have a certified binary64 embedding")
    .into_line_seqs()
    .unwrap();
    assert_eq!(lines.len(), 7);
    let clip = Shape::Polygon(clip);
    assert!(
        lines.iter().all(|line| {
            line.len() == 2
                && line.point_at(0).xy() != line.point_at(1).xy()
                && clip.covers(&Shape::LineString(line.clone())).unwrap()
        }),
        "every output chain is a nondegenerate segment contained by the clip"
    );
}

fn assert_separator_chains_covered_by_cells(
    coordinates: &CoordSeq,
    offsets: &[i32],
    cells: &[Shape],
) {
    let cells_union = Shape::union_all(cells, Strictness::Lenient).unwrap();
    for &[start, end] in offsets.array_windows::<2>() {
        let points: Vec<_> = (usize::try_from(start).unwrap()..usize::try_from(end).unwrap())
            .map(|index| coordinates.point_at(index))
            .collect();
        let line = Shape::LineString(
            LineSeq::try_new(CoordSeq::try_from_points(&points).unwrap()).unwrap(),
        );
        assert!(
            cells_union.covers(&line).unwrap(),
            "the published cell union does not cover separator {points:?}"
        );
    }

    let mut polygon_vertices = Vec::new();
    for cell in cells {
        let Shape::Polygon(polygon) = cell else {
            panic!("Voronoi polygon output contains only polygons");
        };
        polygon_vertices.extend(polygon.shell.iter().map(|point| {
            let point = point.xy();
            (point.x.to_bits(), point.y.to_bits())
        }));
        for hole in polygon.holes.iter() {
            polygon_vertices.extend(hole.iter().map(|point| {
                let point = point.xy();
                (point.x.to_bits(), point.y.to_bits())
            }));
        }
    }
    for index in 0..coordinates.len() {
        let point = coordinates.point_at(index).xy();
        assert!(
            polygon_vertices.contains(&(point.x.to_bits(), point.y.to_bits())),
            "edge coordinate {point:?} differs from the polygon product's shared vertex"
        );
    }
}

fn assign_published_ring(
    subdivision: &ExactSubdivision,
    exact: &ExactSimpleRing,
    published: &Ring,
    coordinates: &mut [Option<XY>],
) {
    let (_, vertices) = exact.vertices.split_last().unwrap();
    let points: Vec<_> = published
        .iter()
        .take(vertices.len())
        .map(|point| point.xy())
        .collect();
    assert_eq!(published.len(), exact.vertices.len());
    let (vertex_start, anchor) = vertices
        .iter()
        .enumerate()
        .find_map(|(index, &node)| {
            subdivision
                .vertex(node)
                .fixed_xy
                .map(|point| (index, point))
        })
        .expect("each thin-hole cell ring has a stored clip vertex");
    let point_start = points.iter().position(|&point| point == anchor).unwrap();
    for (&vertex, &point) in vertices
        .iter()
        .cycle()
        .skip(vertex_start)
        .zip(points.iter().cycle().skip(point_start))
        .take(vertices.len())
    {
        let previous = coordinates
            .get_mut(vertex.0 as usize)
            .expect("the exact ring vertex belongs to the subdivision coordinate table")
            .replace(point);
        if let Some(previous) = previous {
            assert_eq!(
                (previous.x.to_bits(), previous.y.to_bits()),
                (point.x.to_bits(), point.y.to_bits())
            );
        }
    }
}

fn assigned_clip_from_cells(sites: &[Site], source: &Polygon, cells: &[Shape]) -> Polygon {
    let subdivision = grafted_clip_subdivision(sites, source);
    let parts = assemble_cells(&subdivision).unwrap();
    assert_eq!(parts.len(), cells.len());
    let mut coordinates = vec![None; subdivision.vertices.len()];
    for part in &parts {
        let mut matches = cells
            .iter()
            .map(|cell| {
                let Shape::Polygon(polygon) = cell else {
                    panic!("Voronoi cells are polygons");
                };
                polygon
            })
            .filter(|polygon| {
                polygon.shell.len() == part.shell.vertices.len()
                    && polygon.holes.len() == part.holes.len()
                    && part
                        .shell
                        .vertices
                        .iter()
                        .filter_map(|&node| subdivision.vertex(node).fixed_xy)
                        .all(|fixed| polygon.shell.iter().any(|point| point.xy() == fixed))
            });
        let polygon = matches
            .next()
            .expect("the exact part's stored anchors identify its published cell");
        assert!(matches.next().is_none());
        for (exact, published) in std::iter::once(&part.shell)
            .chain(&part.holes)
            .zip(std::iter::once(&polygon.shell).chain(polygon.holes.iter()))
        {
            assign_published_ring(&subdivision, exact, published, &mut coordinates);
        }
    }
    for (vertex, assigned) in subdivision.vertices.iter().zip(&coordinates) {
        let Some(assigned) = assigned else { continue };
        if let Some(fixed) = vertex.fixed_xy {
            assert_eq!(
                (assigned.x.to_bits(), assigned.y.to_bits()),
                (fixed.x.to_bits(), fixed.y.to_bits())
            );
        } else {
            let nearest = vertex.point.round_nearest_even().unwrap();
            assert!([nearest.x.next_down(), nearest.x, nearest.x.next_up()].contains(&assigned.x));
            assert!([nearest.y.next_down(), nearest.y, nearest.y.next_up()].contains(&assigned.y));
        }
    }
    let clip = normalize_clip(sites, VoronoiBoundary::Polygon(source), true).unwrap();
    for point in clip.rings.iter().flatten() {
        let node = subdivision.vertex_at(point).unwrap().unwrap();
        assert!(
            coordinates
                .get(node.0 as usize)
                .expect("the source clip vertex belongs to the subdivision coordinate table")
                .is_some()
        );
    }
    let mut budget = EvaluationBudget::new("assigned clip regression", 0).unwrap();
    let trace = ClipTrace::build(&subdivision, &clip, &mut budget).unwrap();
    let oriented = |atom: &ClipTraceAtom| {
        let [start, end] = subdivision.edge(atom.edge).endpoints;
        if atom.reversed {
            [end, start]
        } else {
            [start, end]
        }
    };
    let mut rings = trace.rings.iter().map(|ring| {
        let mut points = Vec::with_capacity(ring.atoms.len());
        for (atom, next) in ring.atoms.iter().zip(ring.atoms.iter().cycle().skip(1)) {
            let [start, end] = oriented(atom);
            let [next_start, _] = oriented(next);
            assert_eq!(end, next_start);
            let point = coordinates
                .get(start.0 as usize)
                .expect("the source atom endpoint belongs to the subdivision coordinate table")
                .expect("the source atom endpoint is published in its incident cell");
            points.push(Point::new_raw_xy(point.x, point.y));
        }
        points.push(*points.first().unwrap());
        Ring::closed(points).unwrap()
    });
    Polygon::new(rings.next().unwrap(), rings.collect())
}

fn assert_cells_cover_clip(cells: &[Shape], clip: &Polygon) {
    assert!(!cells.is_empty());
    assert!(cells.iter().all(|cell| {
        let Shape::Polygon(polygon) = cell else {
            return false;
        };
        polygon_is_valid(polygon).unwrap()
    }));
    let union = Shape::union_all(cells, Strictness::Lenient).unwrap();
    let clip_shape = Shape::Polygon(clip.clone());
    assert!(
        clip_shape
            .difference(&union, Strictness::Lenient)
            .unwrap()
            .is_empty()
    );
    assert!(
        union
            .difference(&clip_shape, Strictness::Lenient)
            .unwrap()
            .is_empty()
    );
    let mut right_rows = cells.iter();
    for left in cells {
        right_rows.next();
        for right in right_rows.clone() {
            assert_eq!(
                left.intersection(right, Strictness::Lenient)
                    .unwrap()
                    .area(),
                0.0
            );
        }
    }
}

fn assert_one_cell_equals_clip(cells: &[Shape], clip: &Polygon) {
    assert_eq!(cells.len(), 1);
    let Shape::Polygon(_) = &cells[0] else {
        panic!("Voronoi cells are polygons");
    };
    let cell = &cells[0];
    let clip_shape = Shape::Polygon(clip.clone());
    assert!(
        clip_shape
            .difference(cell, Strictness::Lenient)
            .unwrap()
            .is_empty()
    );
    assert!(
        cell.difference(&clip_shape, Strictness::Lenient)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn public_noncollinear_one_owner_keeps_1334_interior_holes_and_clip_support() {
    let source = site_shape(&[(1000.0, 5.0), (1000.0, 1_000_000.0), (1_000_000.0, 5.0)]);
    let holes = (0_u32..1_334)
        .map(|index| {
            let x = 1.0 + f64::from(index) * 1.4;
            closed_ring(&[(x, 5.0), (x + 0.25, 4.5), (x - 0.25, 4.5)])
        })
        .collect();
    let clip = Polygon::new(rectangle(0.0, 0.0, 2000.0, 10.0), holes);
    let mut budget = ExpansionBudget::new("test", "noncollinear one-owner 1334 holes");
    assembly::reset_owner_local_work();
    let cells = source
        .voronoi_cells_budgeted(0.0, VoronoiBoundary::Polygon(&clip), &mut budget)
        .unwrap();
    let work = assembly::owner_local_work();

    assert_eq!(cells.len(), 1);
    let Shape::Polygon(cell) = &cells[0] else {
        panic!("Voronoi polygon output contains only polygons");
    };
    assert_eq!(cell.holes.len(), 1_334);
    assert_eq!(
        polygon_ring_keys(cell),
        polygon_ring_keys(&clip),
        "the sole owner public output preserves the clip's shell and every hole coordinate"
    );
    assert_eq!(work.owner_ring_edges, 4_006);
    assert_eq!(work.host_queries, 1_335);
    assert_eq!(work.ray_hits, 1_334);
    assert_eq!(work.ring_side_checks, 1_335);
}

#[test]
fn task163_noncollinear_public_path_retains_667_tangent_outputs() {
    let source = site_shape(&[(500.0, 5.0), (1500.0, 5.0), (1000.0, 7.0)]);
    let clip = task163_tangent_clip(667);
    let mut budget = ExpansionBudget::new("test", "667 tangent holes");
    assembly::reset_owner_local_work();
    let cells = source
        .voronoi_cells_budgeted(0.0, VoronoiBoundary::Polygon(&clip), &mut budget)
        .unwrap();
    let work = assembly::owner_local_work();
    let mut hole_count = 0;
    for cell in &cells {
        let Shape::Polygon(polygon) = cell else {
            panic!("Voronoi polygon output contains only polygons");
        };
        hole_count += polygon.holes.len();
    }

    eprintln!(
        "667 tangent-hole output: polygons={}, holes={hole_count}, budget={}, \
         ring_edges={}, global_slot_visits={}, owner_vertices={}, face_walks={}, \
         components={}, queries={}, hits={}, side_checks={}",
        cells.len(),
        budget.used(),
        work.owner_ring_edges,
        work.global_edge_slot_visits,
        work.owner_vertices,
        work.face_walks,
        work.components,
        work.host_queries,
        work.ray_hits,
        work.ring_side_checks,
    );
    assert_eq!(cells.len(), 4);
    assert_eq!(hole_count, 666);
    assert_eq!(work.owner_ring_edges, 2_680);
    assert_eq!(work.global_edge_slot_visits, 5_364);
    assert_eq!(work.owner_vertices, 2_013);
    assert_eq!(work.face_walks, 671);
    assert_eq!(work.components, 2);
    assert_eq!(work.host_queries, 2);
    assert_eq!(work.ray_hits, 0);
    assert_eq!(work.ring_side_checks, 669);
}

#[test]
fn diamond_clip_in_square_aabb_covers_clip_with_no_separator_through_contacts() {
    let diamond = Polygon::new(
        closed_ring(&[(0.0, 1.0), (1.0, 0.0), (2.0, 1.0), (1.0, 2.0)]),
        Vec::new(),
    );
    let sites = [(10.0, 0.0), (12.0, 0.0), (11.0, 1.0)];
    let cells = site_shape(&sites)
        .voronoi_cells(0.0, VoronoiBoundary::Polygon(&diamond))
        .unwrap();
    assert_one_cell_equals_clip(&cells, &diamond);
}

#[test]
fn partial_collinear_frame_overlap_covers_clip() {
    let clip = Polygon::new(
        closed_ring(&[(1.0, 0.0), (3.0, 0.0), (3.0, 2.0), (0.0, 2.0)]),
        Vec::new(),
    );
    let sites = [(2.0, 1.0), (1.2, 1.4), (2.4, 1.5)];
    let cells = site_shape(&sites)
        .voronoi_cells(0.0, VoronoiBoundary::Polygon(&clip))
        .unwrap();
    assert_cells_cover_clip(&cells, &clip);
}

#[test]
fn cyclically_rotated_rectangle_clip_covers_clip() {
    let clip = Polygon::new(
        closed_ring(&[(2.0, 0.0), (2.0, 2.0), (0.0, 2.0), (0.0, 0.0)]),
        Vec::new(),
    );
    let sites = [(0.5, 0.5), (1.5, 0.5), (1.0, 1.5)];
    let cells = site_shape(&sites)
        .voronoi_cells(0.0, VoronoiBoundary::Polygon(&clip))
        .unwrap();
    assert_cells_cover_clip(&cells, &clip);
}

#[test]
fn holed_diamond_clip_covers_shell_minus_hole() {
    let clip = Polygon::new(
        closed_ring(&[(0.0, 1.0), (1.0, 0.0), (2.0, 1.0), (1.0, 2.0)]),
        vec![closed_ring(&[
            (0.75, 0.75),
            (1.25, 0.75),
            (1.25, 1.25),
            (0.75, 1.25),
        ])],
    );
    let sites = [(10.0, 0.0), (12.0, 0.0), (11.0, 1.0)];
    let cells = site_shape(&sites)
        .voronoi_cells(0.0, VoronoiBoundary::Polygon(&clip))
        .unwrap();
    assert_one_cell_equals_clip(&cells, &clip);
    let Shape::Polygon(cell) = &cells[0] else {
        panic!("Voronoi cells are polygons");
    };
    assert_eq!(cell.holes.len(), 1);
}

#[test]
fn canonical_separator_atoms_keep_one_identity_and_do_not_branch() {
    let (sites, polygon) = separator_fixture();
    let atoms = node_polygon(&sites, &polygon);
    let mut degree = std::collections::BTreeMap::<([SiteId; 2], VertexId), usize>::new();
    for edge in atoms.edges {
        if let Some(pair) = edge.separator {
            *degree.entry((pair, edge.endpoints[0])).or_default() += 1;
            *degree.entry((pair, edge.endpoints[1])).or_default() += 1;
        }
    }
    assert!(degree.values().all(|degree| *degree <= 2));
}
