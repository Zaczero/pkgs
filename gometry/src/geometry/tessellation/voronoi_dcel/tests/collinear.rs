#[test]
fn collinear_holed_clip_splits_each_separator_into_two_pieces() {
    let sites: Vec<_> = [(-2.0, 0.0), (0.0, 0.0), (2.0, 0.0)]
        .into_iter()
        .enumerate()
        .map(|(id, (x, y))| Site::raw(id, Point::new_raw_xy(x, y), id))
        .collect();
    let clip = Polygon::new(rectangle(-3.0, -3.0, 3.0, 3.0), vec![rectangle(
        -1.5, -1.0, 1.5, 1.0,
    )]);
    let mut budget = ExpansionBudget::new("test", "collinear Voronoi topology");
    let edges = build_collinear_edges(
        &sites,
        VoronoiBoundary::Polygon(&clip),
        &mut budget,
        &certified_delaunay(&sites).unwrap(),
    )
    .unwrap()
    .into_line_seqs()
    .unwrap();
    assert_eq!(edges.len(), 4);
    let mut endpoints: Vec<_> = edges
        .iter()
        .map(|edge| {
            let first = edge.first().expect("collinear edge has a first point").xy();
            let last = edge.last().expect("collinear edge has a last point").xy();
            (first.x, first.y, last.x, last.y)
        })
        .collect();
    endpoints.sort_unstable_by(|left, right| {
        left.0
            .total_cmp(&right.0)
            .then_with(|| left.1.total_cmp(&right.1))
    });
    assert_eq!(endpoints, vec![
        (-1.0, -3.0, -1.0, -1.0),
        (-1.0, 1.0, -1.0, 3.0),
        (1.0, -3.0, 1.0, -1.0),
        (1.0, 1.0, 1.0, 3.0),
    ]);

    let source = Shape::MultiPoint(
        sites
            .iter()
            .map(|site| site.point)
            .collect::<Vec<_>>()
            .into(),
    );
    let mut polygon_budget = ExpansionBudget::new("voronoi_cells", "sites/clip topology");
    let outputs = source
        .voronoi_polygon_outputs_budgeted(0.0, VoronoiBoundary::Polygon(&clip), &mut polygon_budget)
        .unwrap();
    assert_eq!(outputs.len(), 4);
    assert!(outputs.iter().all(|output| {
        output.edge_roles.len() == output.polygon.rings().count()
            && output.edge_roles.iter().all(|edges| {
                !edges.is_empty()
                    && edges.iter().any(|role| {
                        role.origins
                            .iter()
                            .any(|origin| !matches!(origin, VoronoiEdgeOrigin::SourceClip { .. }))
                    })
            })
    }));
    assert!(outputs.iter().any(|output| {
        output
            .edge_roles
            .iter()
            .flatten()
            .flat_map(|role| &role.origins)
            .any(|origin| matches!(origin, VoronoiEdgeOrigin::SourceClip { .. }))
    }));
}

#[test]
fn frame_nearest_ignores_a_tie_that_is_not_the_minimum() {
    let sites: Vec<_> = [(-1.0, 0.0), (1.0, 0.0), (0.0, 0.0)]
        .into_iter()
        .enumerate()
        .map(|(id, (x, y))| Site::raw(id, Point::new_raw_xy(x, y), id))
        .collect();
    let point = exact::ExactPoint::from_xy(XY::new(0.0, 10.0));
    assert_eq!(nearest_site(&point, &sites).unwrap(), SiteId(2));
    let true_tie = exact::ExactPoint::from_xy(XY::new(0.0, 0.0));
    nearest_site(&true_tie, &sites[..2]).unwrap_err();
}

#[test]
fn finite_extreme_collinear_envelope_is_explicitly_degenerate() {
    let sites: Vec<_> = [(-f64::MAX, 0.0), (f64::MAX, 0.0)]
        .into_iter()
        .enumerate()
        .map(|(id, (x, y))| Site::raw(id, Point::new_raw_xy(x, y), id))
        .collect();
    let mut budget = ExpansionBudget::new("test", "collinear Voronoi topology");
    assert!(
        build_collinear_edges(
            &sites,
            VoronoiBoundary::Envelope,
            &mut budget,
            &certified_delaunay(&sites).unwrap(),
        )
        .unwrap()
        .into_line_seqs()
        .unwrap()
        .is_empty()
    );
}

#[test]
fn sloped_collinear_envelope_emits_analytic_bisector() {
    let sites: Vec<_> = [(0.0, 0.0), (2.0, 2.0)]
        .into_iter()
        .enumerate()
        .map(|(id, (x, y))| Site::raw(id, Point::new_raw_xy(x, y), id))
        .collect();
    let mut budget = ExpansionBudget::new("test", "collinear Voronoi topology");
    let edges = build_collinear_edges(
        &sites,
        VoronoiBoundary::Envelope,
        &mut budget,
        &certified_delaunay(&sites).unwrap(),
    )
    .unwrap()
    .into_line_seqs()
    .unwrap();
    assert_eq!(edges.len(), 1);
    let mut endpoints: Vec<_> = edges[0].iter().map(|point| point.xy()).collect();
    endpoints.sort_unstable_by(|left, right| left.x.total_cmp(&right.x));
    assert_eq!(endpoints, [XY::new(0.0, 2.0), XY::new(2.0, 0.0)]);
}

#[test]
fn collinear_edge_projection_follows_mesh_site_order() {
    let sites = sites_xy(&[
        (-1.0, 100.0),
        (-0.5, 50.0),
        (0.0, 0.0),
        (0.5, -50.0),
        (1.0, -100.0),
    ]);
    let mesh = certified_delaunay(&sites).unwrap();
    let order = mesh.collinear_order().unwrap();
    assert_eq!(order, &[4, 3, 2, 1, 0]);

    let corners = [
        XY::new(-2.0, -200.0),
        XY::new(2.0, -200.0),
        XY::new(2.0, 200.0),
        XY::new(-2.0, 200.0),
    ];
    let mut boundary = Vec::new();
    for side in 0..corners.len() {
        let start = corners[side];
        let end = corners[(side + 1) % corners.len()];
        for step in 0..20 {
            let fraction = f64::from(step) / 20.0;
            boundary.push(Point::new_raw_xy(
                start.x + (end.x - start.x) * fraction,
                start.y + (end.y - start.y) * fraction,
            ));
        }
    }
    let clip = Polygon::new(Ring::closed(boundary).unwrap(), Vec::new());
    let mut budget = ExpansionBudget::new("test", "collinear projection order");
    let edges = build_collinear_edges(&sites, VoronoiBoundary::Polygon(&clip), &mut budget, &mesh)
        .unwrap()
        .into_line_seqs()
        .unwrap();

    assert_eq!(edges.len(), order.len() - 1);
}

#[test]
fn tolerance_clustered_sites_do_not_tie_private_frame_ownership() {
    let sites = super::shape::canonical_voronoi_sites(
        [(0.0, 0.0), (0.09, 0.0), (0.18, 0.0), (0.0, 1.0), (1.0, 0.0)]
            .into_iter()
            .map(|(x, y)| Point::new_raw_xy(x, y))
            .collect(),
        0.1,
    )
    .unwrap();
    let clip = Polygon::new(rectangle(-1.0, -1.0, 2.0, 2.0), Vec::new());
    let mut budget = ExpansionBudget::new("test", "Voronoi topology");
    let diagram = build(
        &sites,
        VoronoiBoundary::Polygon(&clip),
        &mut budget,
        true,
        &certified_delaunay(&sites).unwrap(),
    )
    .unwrap();
    assert!(!diagram.into_edges().into_line_seqs().unwrap().is_empty());
}
