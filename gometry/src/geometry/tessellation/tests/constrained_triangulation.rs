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

#[test]
fn constrained_tiny_square_keeps_its_literal_diagonal_and_source_vertices() {
    let side = 5.0e-5;
    let source = Shape::Polygon(polygon(
        vec![xy(0.0, 0.0), xy(side, 0.0), xy(side, side), xy(0.0, side)],
        Vec::new(),
    ));
    let triangles = source
        .constrained_delaunay_vertices(CdtRefinement::Off)
        .unwrap();

    assert_eq!(triangles.len(), 8, "the square has two triangle cycles");
    assert_eq!(
        triangles
            .iter()
            .map(|point| (point.x.to_bits(), point.y.to_bits()))
            .collect::<Vec<_>>(),
        [
            (0.0_f64.to_bits(), 0.0_f64.to_bits()),
            (side.to_bits(), 0.0_f64.to_bits()),
            (0.0_f64.to_bits(), side.to_bits()),
            (0.0_f64.to_bits(), 0.0_f64.to_bits()),
            (0.0_f64.to_bits(), side.to_bits()),
            (side.to_bits(), 0.0_f64.to_bits()),
            (side.to_bits(), side.to_bits()),
            (0.0_f64.to_bits(), side.to_bits()),
        ],
        "the canonical cocircular diagonal joins the lower-right and upper-left corners"
    );
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

#[test]
fn constrained_triangulation_names_partial_shell_hole_overlap() {
    assert_constrained_triangulation_error(
        polygon(
            vec![xy(0.0, 0.0), xy(10.0, 0.0), xy(10.0, 10.0), xy(0.0, 10.0)],
            vec![vec![xy(0.0, 2.0), xy(4.0, 2.0), xy(4.0, 6.0), xy(0.0, 6.0)]],
        ),
        "polygon hole touches or crosses the shell",
    );
}

#[test]
fn constrained_triangulation_names_shared_shell_hole_edge() {
    assert_constrained_triangulation_error(
        polygon(
            vec![xy(0.0, 0.0), xy(10.0, 0.0), xy(10.0, 10.0), xy(0.0, 10.0)],
            vec![vec![xy(0.0, 0.0), xy(4.0, 5.0), xy(0.0, 10.0)]],
        ),
        "polygon hole touches or crosses the shell",
    );
}

#[test]
fn constrained_triangulation_names_shared_hole_edge() {
    assert_constrained_triangulation_error(
        polygon(
            vec![xy(0.0, 0.0), xy(20.0, 0.0), xy(20.0, 20.0), xy(0.0, 20.0)],
            vec![
                vec![xy(2.0, 2.0), xy(8.0, 2.0), xy(8.0, 8.0), xy(2.0, 8.0)],
                vec![xy(8.0, 2.0), xy(14.0, 2.0), xy(14.0, 8.0), xy(8.0, 8.0)],
            ],
        ),
        "polygon holes touch or cross",
    );
}

#[test]
fn constrained_triangulation_names_shell_backtracking() {
    assert_constrained_triangulation_error(
        polygon(
            vec![
                xy(0.0, 0.0),
                xy(10.0, 0.0),
                xy(10.0, 10.0),
                xy(0.0, 10.0),
                xy(0.0, 5.0),
                xy(0.0, 8.0),
                xy(0.0, 5.0),
            ],
            vec![],
        ),
        "polygon shell overlaps itself",
    );
}
