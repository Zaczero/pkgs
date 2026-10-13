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
            exact::incidence::orient_points(&separator.start, &separator.end, &left).unwrap(),
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
#[expect(clippy::float_cmp, reason = "certified Voronoi topology classifies exact frame and vertex identity before rounding")]
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
