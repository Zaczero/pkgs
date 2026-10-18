use super::*;

fn xy(points: &[(f64, f64)]) -> Vec<XY> {
    points.iter().map(|&(x, y)| XY::new(x, y)).collect()
}

/// A square of sites with its centre: every circumcenter lies on the
/// envelope, so a clip hit and a center share one vertex, and the diagram
/// must still cancel every interior edge at any scale.
#[rstest::rstest]
#[case::scale_1_0(1.0)]
#[case::scale_1e_20(1e-20)]
#[case::scale_1e_150(1e-150)]
#[case::scale_1e150(1e150)]
fn clip_hits_coinciding_with_centers_share_one_vertex(#[case] scale: f64) {
    let sites: Vec<Site> = shape::canonical_voronoi_sites(
        [(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.5, 0.5)]
            .into_iter()
            .map(|(x, y)| Point::new_raw_xy(x * scale, y * scale))
            .collect(),
        0.0,
    )
    .unwrap();
    let mut budget = ExpansionBudget::new("test", "coinciding vertices");
    let cells = build(
        &sites,
        VoronoiBoundary::Envelope,
        &mut budget,
        true,
        &certified_delaunay(&sites).unwrap(),
    )
    .unwrap_or_else(|error| panic!("scale {scale:e}: {error:?}"))
    .into_polygons();
    assert_eq!(cells.len(), 5);
    let vertex_count: usize = cells.iter().map(|cell| cell.shell.iter().count()).sum();
    let shapes: Vec<_> = cells.iter().cloned().map(Shape::Polygon).collect();
    assert!(shapes.iter().all(
        |shape| matches!(shape, Shape::Polygon(polygon) if polygon_is_valid(polygon).unwrap())
    ));
    assert!(coverage_is_valid(&shapes, 0.0).unwrap());
    assert!(vertex_count > 0);
}

#[test]
fn reversing_a_tagged_ring_keeps_each_edge_with_its_tag() {
    let tagged = |x: f64, leaving: EdgeTag| Tagged {
        vertex: DualVertex::Generated(Box::new(exact::ExactPoint::from_xy(XY::new(x, 0.0)))),
        leaving,
    };
    // v0 -(a)-> v1 -(b)-> v2 -(c)-> v0
    let mut ring = vec![
        tagged(0.0, EdgeTag::Bisector(SiteId(0))),
        tagged(1.0, EdgeTag::Bisector(SiteId(1))),
        tagged(2.0, EdgeTag::Frame(FrameSide::Bottom)),
    ];
    reverse_tagged(&mut ring);
    // v2 -(b)-> v1 -(a)-> v0 -(c)-> v2
    let leaving: Vec<EdgeTag> = ring.iter().map(|tagged| tagged.leaving).collect();
    assert_eq!(leaving, [
        EdgeTag::Bisector(SiteId(1)),
        EdgeTag::Bisector(SiteId(0)),
        EdgeTag::Frame(FrameSide::Bottom)
    ]);
    let xs: Vec<f64> = ring
        .iter()
        .map(|tagged| {
            dual_vertex_point(&tagged.vertex, &[])
                .round_nearest_even()
                .unwrap()
                .x
        })
        .collect();
    assert_eq!(xs, [2.0, 1.0, 0.0]);
}

#[test]
fn one_construction_reached_from_two_cells_is_one_position() {
    let (a, b) = (XY::new(0.0, 0.0), XY::new(4.0, 2.0));
    let side = [XY::new(-10.0, 7.0), XY::new(10.0, 7.0)];
    let other_side = [XY::new(-10.0, -7.0), XY::new(10.0, -7.0)];
    let hit = |sites: [XY; 2], side: [XY; 2]| match exact::ExactPoint::certified_bisector_hit(
        sites, side,
    ) {
        exact::FrameHit::Point(point) => point,
        exact::FrameHit::Declined => panic!("certified bisector hit was declined"),
        exact::FrameHit::Parallel => panic!("certified bisector hit was parallel"),
    };
    let forward = hit([a, b], side);
    let swapped = hit([b, a], [side[1], side[0]]);
    assert!(forward.same_position(&swapped).unwrap());
    assert!(!forward.same_position(&hit([a, b], other_side)).unwrap());
    let center = exact::circumcenter(a, b, XY::new(0.0, 5.0)).unwrap();
    assert!(!forward.same_position(&center).unwrap());
    let center_permuted = exact::circumcenter(XY::new(0.0, 5.0), a, b).unwrap();
    assert!(center.same_position(&center_permuted).unwrap());
}

#[test]
fn accepts_convex_counter_clockwise_cells_with_the_site_inside_or_on_an_edge() {
    let square = xy(&[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0)]);
    assert!(certify_convex_cell(&square, XY::new(1.0, 2.0)));
    assert!(certify_convex_cell(&square, XY::new(4.0, 2.0)));
    assert!(certify_convex_cell(&square, XY::new(0.0, 0.0)));
    let hexagon = xy(&[
        (2.0, 0.0),
        (4.0, 1.0),
        (4.0, 3.0),
        (2.0, 4.0),
        (0.0, 3.0),
        (0.0, 1.0),
    ]);
    assert!(certify_convex_cell(&hexagon, XY::new(2.0, 2.0)));
}

#[test]
fn refuses_every_mutation_the_exact_lane_would_refuse() {
    let square = xy(&[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0)]);
    // Site outside.
    assert!(!certify_convex_cell(&square, XY::new(5.0, 2.0)));
    // Clockwise.
    let mut clockwise = square.clone();
    clockwise.reverse();
    assert!(!certify_convex_cell(&clockwise, XY::new(1.0, 2.0)));
    // Two vertices swapped: a bow tie.
    let mut swapped = square.clone();
    swapped.swap(1, 2);
    assert!(!certify_convex_cell(&swapped, XY::new(1.0, 2.0)));
    // A duplicated vertex and a collinear vertex both give a zero turn.
    let mut duplicated = square.clone();
    duplicated.insert(1, XY::new(4.0, 0.0));
    assert!(!certify_convex_cell(&duplicated, XY::new(1.0, 2.0)));
    // A forward collinear vertex is a clipped hull cell's shape: accepted.
    let mut collinear = square.clone();
    collinear.insert(1, XY::new(2.0, 0.0));
    assert!(certify_convex_cell(&collinear, XY::new(1.0, 2.0)));
    // A collinear reversal (a spike) is not.
    let spike = xy(&[
        (0.0, 0.0),
        (4.0, 0.0),
        (6.0, 0.0),
        (4.0, 0.0),
        (4.0, 4.0),
        (0.0, 4.0),
    ]);
    assert!(!certify_convex_cell(&spike, XY::new(1.0, 2.0)));
    let backtrack = xy(&[(0.0, 0.0), (4.0, 0.0), (3.0, 0.0), (4.0, 4.0), (0.0, 4.0)]);
    assert!(!certify_convex_cell(&backtrack, XY::new(1.0, 2.0)));
    // A reflex vertex.
    let reflex = xy(&[(0.0, 0.0), (4.0, 0.0), (2.0, 1.0), (4.0, 4.0), (0.0, 4.0)]);
    assert!(!certify_convex_cell(&reflex, XY::new(1.0, 2.0)));
    // A pentagram turns left everywhere yet self-intersects.
    let pentagram: Vec<XY> = (0..5)
        .map(|k| {
            let angle = std::f64::consts::TAU * f64::from(k * 2) / 5.0;
            XY::new(angle.cos(), angle.sin())
        })
        .collect();
    for index in 0..5 {
        let (a, b, c) = (
            pentagram[index],
            pentagram[(index + 1) % 5],
            pentagram[(index + 2) % 5],
        );
        assert_eq!(exact::orient2d(a, b, c), ExactSign::Positive);
    }
    assert!(!certify_convex_cell(&pentagram, XY::new(0.0, 0.0)));
    // Too few vertices.
    assert!(!certify_convex_cell(&square[..2], XY::new(1.0, 2.0)));
}
