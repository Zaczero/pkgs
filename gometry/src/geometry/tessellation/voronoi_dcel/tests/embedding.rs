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
                let exact = exact::incidence::angle_ccw_cmp(
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

#[expect(
    clippy::float_cmp,
    reason = "certified Voronoi topology classifies exact frame and vertex identity before rounding"
)]
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
            exact::incidence::point_in_cycle(
                &rounded_ring,
                &exact::ExactPoint::from_xy(site.point.xy()),
            )
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
