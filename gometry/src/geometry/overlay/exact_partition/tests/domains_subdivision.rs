#[test]
fn planar_embedding_domain_admits_finite_coordinates_only() {
    let domain = embedding::EmbeddingDomain::Plane;
    let nearest = XY::new(0.0, 0.0);
    assert!(domain.admits(nearest, nearest));
    assert!(domain.admits(XY::new(0.0, 0.0), XY::new(f64::MAX, f64::MIN)));
    assert!(!domain.admits(XY::new(0.0, 0.0), XY::new(f64::INFINITY, 0.0)));
    assert!(!domain.admits(XY::new(0.0, 0.0), XY::new(0.0, f64::NAN)));
}

#[test]
fn rectangle_embedding_domain_keeps_candidates_in_the_closed_window() {
    let bounds = Bounds::new(-2.0, -3.0, 4.0, 5.0).unwrap();
    let domain = embedding::EmbeddingDomain::Rectangle(bounds);
    let nearest = XY::new(0.0, 0.0);
    assert!(domain.admits(nearest, nearest));
    assert!(domain.admits(nearest, XY::new(-2.0, 5.0)));
    assert!(domain.admits(nearest, XY::new(4.0, -3.0)));
    assert!(!domain.admits(nearest, XY::new((-2.0_f64).next_down(), 0.0)));
    assert!(!domain.admits(nearest, XY::new(4.0_f64.next_up(), 0.0)));
}

#[test]
fn geographic_embedding_domain_uses_frame_limits_and_existing_singular_lines() {
    use crate::geographic::{AngularFactor, GeographicFrame};

    let grad = AngularFactor::try_new("test angle", std::f64::consts::PI / 200.0).unwrap();
    let frame = GeographicFrame::try_new(grad, grad, 0.0).unwrap();
    let domain = embedding::EmbeddingDomain::Geographic(frame);
    let half_period = frame.longitude().half_period();
    let pole = frame.pole();
    let ordinary = XY::new(0.0, 0.0);

    assert!(domain.admits(ordinary, ordinary));
    assert!(domain.admits(ordinary, XY::new(25.0, 40.0)));
    assert!(!domain.admits(ordinary, XY::new(half_period, 0.0)));
    assert!(!domain.admits(ordinary, XY::new(0.0, pole)));
    assert!(!domain.admits(ordinary, XY::new(half_period.next_up(), 0.0)));
    assert!(!domain.admits(ordinary, XY::new(0.0, pole.next_up().next_up())));

    let seam = XY::new(-half_period, 0.0);
    assert!(domain.admits(seam, XY::new(-half_period, 1.0)));
    let pole_node = XY::new(0.0, pole);
    assert!(domain.admits(pole_node, XY::new(1.0, pole)));
    assert!(domain.admits(pole_node, XY::new(1.0, pole.next_up())));
}

#[test]
#[expect(
    clippy::float_cmp,
    reason = "the certified rectangle must retain its exact supplied bounds"
)]
fn rectangle_partition_uses_its_clip_window_as_embedding_domain() {
    let Shape::Polygon(polygon) =
        crate::io::parse_wkt("POLYGON ((-1 -1,1 -1,1 1,-1 1,-1 -1))").unwrap()
    else {
        panic!("expected polygon");
    };
    let bounds = Bounds::new(-0.5, -0.5, 0.5, 0.5).unwrap();
    let rectangle = rect_polygon(bounds).unwrap();
    let (inventory, intervals) =
        ExactSourceInventory::from_polygon_rect_clip(&[&polygon], &rectangle, bounds, false)
            .unwrap()
            .expect("the rectangle clips the polygon");
    let mut budget = ExpansionBudget::new("clip_by_rect", "embedding domain test");
    let partition = inventory
        .partition_in_window(&intervals, bounds, &mut budget)
        .unwrap();

    assert!(matches!(
        partition.embedding_domain,
        embedding::EmbeddingDomain::Rectangle(actual)
            if actual.into_array() == bounds.into_array()
    ));
}

#[test]
fn geographic_frame_installs_domain_and_preserves_all_source_vertices() {
    let source = crate::io::parse_wkt("LINESTRING (0 0,2 0)").unwrap();
    let inventory = ExactSourceInventory::from_shapes(&[&source]).unwrap();
    let mut budget = ExpansionBudget::new("overlay", "geographic embedding domain test");
    let mut partition = inventory.partition(&mut budget).unwrap();
    let frame = crate::geographic::GeographicFrame::crs84();
    partition.set_geographic_frame(frame);

    assert!(matches!(
        partition.embedding_domain,
        embedding::EmbeddingDomain::Geographic(actual) if actual == frame
    ));
    assert!(matches!(partition.kept_nodes, KeptNodes::All));
}

fn subdivision_children(source: &Shape, split_x: bool, cut: f64) -> [Shape; 2] {
    let bounds = source.bounds().unwrap();
    let (minx, miny, maxx, maxy) = bounds.into_tuple();
    let halves = if split_x {
        [
            Bounds::new_raw(minx, miny, cut, maxy),
            Bounds::new_raw(cut, miny, maxx, maxy),
        ]
    } else {
        [
            Bounds::new_raw(minx, miny, maxx, cut),
            Bounds::new_raw(minx, cut, maxx, maxy),
        ]
    };
    let [left_bounds, right_bounds] = halves;
    let left = Shape::Polygon(rect_polygon(left_bounds).unwrap());
    let right = Shape::Polygon(rect_polygon(right_bounds).unwrap());
    let inventory = ExactSourceInventory::from_shapes(&[source, &left, &right]).unwrap();
    let mut budget = ExpansionBudget::new("subdivide", "shared seam");
    let partition = inventory.partition(&mut budget).unwrap();
    let (topology, edges) = partition.local_topology().unwrap();
    let global = partition.global_faces(&topology, &edges).unwrap();
    let rings = inventory.winding_source_rings();
    let facts = inventory
        .build_cell_facts(&partition, &topology, &edges, &global, &rings, false)
        .unwrap();
    let first = inventory.select_subdivision_cells(&partition, &topology, &global, &facts, 0, 1);
    let second = inventory.select_subdivision_cells(&partition, &topology, &global, &facts, 0, 2);
    for ((&left, &right), &source) in first
        .faces
        .iter()
        .zip(&second.faces)
        .zip(facts.face_membership.iter().step_by(3))
    {
        assert_eq!(left || right, source);
        assert!(!(left && right));
    }
    for ((&left, &right), &source) in first
        .spans
        .iter()
        .zip(&second.spans)
        .zip(facts.span_support.iter().step_by(3))
    {
        assert_eq!(left || right, source);
    }
    let parent = inventory.select_subdivision_cells(&partition, &topology, &global, &facts, 0, 0);
    for ((&left, &right), &source) in first.nodes.iter().zip(&second.nodes).zip(&parent.nodes) {
        assert_eq!(left || right, source);
    }
    let plans: Vec<_> = [first, second]
        .iter()
        .map(|cells| {
            cells
                .emission_plan(&partition, &topology, &edges, &global)
                .unwrap()
        })
        .collect();
    let emissions: Vec<_> = plans
        .iter()
        .map(|plan| plan.emission(&topology, &edges))
        .collect();
    let refs: Vec<_> = emissions.iter().collect();
    let embedding = partition.embed(&refs, "subdivide").unwrap();
    if emissions
        .iter()
        .any(|emission| embedding::rounding_moves(&partition, &emission.items))
    {
        let segments: Vec<Vec<_>> = emissions
            .iter()
            .map(|emission| {
                emission
                    .items
                    .iter()
                    .map(|&item| embedding::Embedding::segment_at(embedding.coordinates(), item))
                    .collect()
            })
            .collect();
        let views: Vec<_> = segments.iter().map(Vec::as_slice).collect();
        assert!(
            embedding::certify(
                &partition,
                &embedding,
                &refs,
                &views,
                &mut embedding::FirstFailure::default()
            )
            .unwrap()
            .is_continue()
        );
    }
    let children = inventory
        .subdivision_parts::<crate::crs::GeodesicMetric>(
            &partition,
            source,
            source.axes(),
            false,
            [1, 2],
            &mut budget,
            &crate::geometry::SourceArcContext::cartesian(),
        )
        .unwrap();
    let [first, second] = &children;
    assert!(first.relate(second).unwrap().starts_with('F'));
    assert!(
        Shape::union_all(&children, Strictness::Lenient)
            .unwrap()
            .validate()
            .unwrap()
            .is_none()
    );
    children
}

#[test]
fn subdivision_rounds_the_exterior_crossing_without_cracking_the_seam() {
    let source =
        crate::io::parse_wkt("POLYGON((0 0,3 1,3 2,2 2,1 2,0 2,-1 2,-1 1,-1 0,0 0))").unwrap();
    let [left, right] = subdivision_children(&source, true, 0.5);
    let seam = crate::io::parse_wkt("LINESTRING(0.5 0.16666666666666666,0.5 2)").unwrap();
    let shared = left.intersection(&right, Strictness::Lenient).unwrap();
    assert!(shared.equals(&seam).unwrap());
    let union = Shape::union_all(&[left, right], Strictness::Lenient).unwrap();
    // The exact exterior crossing is (1/2, 1/6), not a binary64 point.
    assert!(!union.equals(&source).unwrap());
    let Shape::Polygon(polygon) = union else {
        panic!("one shell");
    };
    assert!(polygon.holes.is_empty());
}

#[test]
fn subdivision_through_a_component_gap_has_no_shared_seam() {
    let source = crate::io::parse_wkt(
        "MULTIPOLYGON(((0 0,1 0,2 0,2 1,2 2,1 2,0 2,0 1,0 0)),\
        ((10 0,11 0,12 0,12 1,12 2,11 2,10 2,10 1,10 0)))",
    )
    .unwrap();
    let [left, right] = subdivision_children(&source, true, 6.0);
    assert!(
        left.intersection(&right, Strictness::Lenient)
            .unwrap()
            .is_empty()
    );
    let union = Shape::union_all(&[left, right], Strictness::Lenient).unwrap();
    assert!(union.equals(&source).unwrap());
    let Shape::MultiPolygon(polygons) = union else {
        panic!("two shells");
    };
    assert_eq!(polygons.len(), 2);
    assert!(polygons.iter().all(|polygon| polygon.holes.is_empty()));
}

#[test]
fn subdivision_keeps_a_zero_dimensional_shared_cut_support() {
    let source =
        crate::io::parse_wkt("MULTIPOLYGON(((-2 0,-1 -1,0 0,-1 1,-2 0)),((0 0,1 -1,2 0,1 1,0 0)))")
            .unwrap();
    let [left, right] = subdivision_children(&source, true, 0.0);
    let shared = left.intersection(&right, Strictness::Lenient).unwrap();
    assert!(
        shared
            .equals(&Shape::Point(Point::new_raw_xy(0.0, 0.0)))
            .unwrap()
    );
    assert!(
        Shape::union_all(&[left, right], Strictness::Lenient)
            .unwrap()
            .equals(&source)
            .unwrap()
    );
}

#[test]
fn subdivision_keeps_a_hole_vertex_on_the_other_childs_dissolved_seam() {
    let source =
        crate::io::parse_wkt("POLYGON((-4 -3,4 -3,4 3,-4 3,-4 -3),(-2 -1,0 0,-2 1,-2 -1))")
            .unwrap();
    let [left, right] = subdivision_children(&source, true, 0.0);
    let vertex = XY::new(0.0, 0.0);
    let mut left_keeps = false;
    let mut right_keeps = false;
    left.for_each_point(|point| left_keeps |= point.xy() == vertex);
    right.for_each_point(|point| right_keeps |= point.xy() == vertex);
    assert!(left_keeps);
    assert!(!right_keeps);
    assert!(
        crate::geometry::boundary_with_metric::<crate::crs::GeodesicMetric>(
            &crate::geometry::PreparedPlan::transient(&right),
            &crate::geometry::SourceArcContext::cartesian(),
        )
        .unwrap()
        .intersects(&Shape::Point(Point::new_raw_xy(0.0, 0.0)))
        .unwrap()
    );
    let seam = crate::io::parse_wkt("LINESTRING(0 -3,0 3)").unwrap();
    assert!(
        left.intersection(&right, Strictness::Lenient)
            .unwrap()
            .equals(&seam)
            .unwrap()
    );
    let union = Shape::union_all(&[left, right], Strictness::Lenient).unwrap();
    assert!(union.equals(&source).unwrap());
    let Shape::Polygon(polygon) = union else {
        panic!("one shell");
    };
    assert_eq!(polygon.holes.len(), 1);
}
