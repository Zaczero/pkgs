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

    let rings = vec![cycle(&shell_points), cycle(&hole_points)];
    let chart_fixed = vec![shell.clone(), hole.clone()];
    let fixed = vec![shell, hole];
    let source = Some(&clip_polygon);
    let geometry = Some(clip_polygon.clone());
    let clip = NormalizedClip {
        rings,
        fixed,
        chart_fixed,
        source,
        geometry,
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
    let hit = exact::incidence::line_intersection(&bisector, &diagonal).unwrap();
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
    use crate::geometry::generated_budget::GENERATED_ITEM_LIMIT;

    let holes = (0..8)
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
    budget.add(GENERATED_ITEM_LIMIT - 7).unwrap();
    let cells = source
        .voronoi_cells_budgeted(0.0, VoronoiBoundary::Polygon(&polygon), &mut budget)
        .expect("stored input rings do not spend the seven generated-item credits");

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
    assert_eq!(hole_count, 8);
    assert_eq!(coordinate_count, 42);
    assert_eq!(budget.used(), GENERATED_ITEM_LIMIT);
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
    use crate::geometry::generated_budget::GENERATED_ITEM_LIMIT;

    let mut boundary = Vec::with_capacity(32);
    for (start, end, subdivisions) in [
        (XY::new(-2.0, -1.0), XY::new(2.0, -0.5), 15_u32),
        (XY::new(2.0, -0.5), XY::new(2.0, 1.0), 1),
        (XY::new(2.0, 1.0), XY::new(-2.0, 0.5), 15),
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
    assert_eq!(polygon.shell.len() - 1, 32);

    let sites = (0_u32..9)
        .map(|index| Point::new_raw_xy(0.0, -0.5 + f64::from(index) / 8.0))
        .collect::<Vec<_>>();
    let source = Shape::MultiPoint(sites.into());
    let mut budget = ExpansionBudget::new("voronoi_edge_lines", "sites/clip topology");
    budget
        .add(GENERATED_ITEM_LIMIT - 24)
        .expect("leave credits for eight separators and their endpoints");
    let (coordinates, offsets) = source
        .voronoi_edge_column_budgeted(0.0, VoronoiBoundary::Polygon(&polygon), &mut budget)
        .expect("clip scans do not consume the 24 output credits");

    assert_eq!(offsets.as_slice().len(), 9);
    assert_eq!(offsets.as_slice().last(), Some(&16));
    assert_eq!(coordinates.len(), 16);
    assert_eq!(budget.used(), GENERATED_ITEM_LIMIT);
}
