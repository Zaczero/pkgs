fn raw(x: f64, y: f64) -> Point {
    Point::new_raw_xy(x, y)
}

#[test]
fn raw_delaunay_provider_keeps_the_existing_cocircular_square_mesh() {
    let points = [raw(0.0, 0.0), raw(2.0, 0.0), raw(2.0, 2.0), raw(0.0, 2.0)];
    let sites = points
        .iter()
        .enumerate()
        .map(|(id, &point)| super::delaunay::Site::raw(id, point, id))
        .collect::<Vec<_>>();
    let mesh = super::delaunay::certified_delaunay(&sites).unwrap();
    assert_eq!(mesh.triangles().len(), 2);
    assert_eq!(mesh.halfedges().len(), 6);
    assert_eq!(mesh.hull().len(), 4);
    assert_eq!(
        (0..mesh.halfedges().len())
            .filter(|&edge| mesh.edge_is_cocircular(edge))
            .count(),
        2
    );
}

#[test]
fn retained_site_mesh_keeps_adjacent_lifted_sites_and_first_source_records() {
    let frame = GeographicFrame::crs84();
    let source_points = [
        Point::new_axes(170.0, 1.0, ZOrdinate(Some(7.0)), MOrdinate(Some(10.0))).unwrap(),
        Point::new_axes(530.0, 1.0, ZOrdinate(Some(99.0)), MOrdinate(Some(99.0))).unwrap(),
        Point::new_axes(-170.0, 1.0, ZOrdinate(Some(8.0)), MOrdinate(Some(11.0))).unwrap(),
        Point::new_axes(-100.0, 1.0, ZOrdinate(Some(9.0)), MOrdinate(Some(12.0))).unwrap(),
        Point::new_axes(0.2, 1.0, ZOrdinate(Some(10.0)), MOrdinate(Some(13.0))).unwrap(),
        Point::new_axes(
            0.200_000_000_000_000_04,
            1.0,
            ZOrdinate(Some(11.0)),
            MOrdinate(Some(14.0)),
        )
        .unwrap(),
        Point::new_axes(170.0, 2.0, ZOrdinate(Some(12.0)), MOrdinate(Some(15.0))).unwrap(),
    ];
    let chart = PointSelectionChart::try_from_points(frame, &source_points)
        .unwrap()
        .unwrap();
    let mut sites = chart.sites_from_points(&source_points).unwrap();
    sites.sort_unstable_by(|left, right| left.compare_chart_xy(*right));
    assert_eq!(sites.len(), 6, "the periodic alias shares the first site");
    let mut source_ordinals = sites
        .iter()
        .map(|site| site.source_ordinal())
        .collect::<Vec<_>>();
    source_ordinals.sort_unstable();
    assert_eq!(source_ordinals, vec![0, 2, 3, 4, 5, 6]);
    let first_record = source_points
        .get(
            sites
                .iter()
                .find(|site| site.source_ordinal() == 0)
                .unwrap()
                .source_ordinal(),
        )
        .unwrap();
    assert_eq!(first_record.x.to_bits(), 170.0_f64.to_bits());
    assert_eq!(first_record.z(), Some(7.0));
    assert_eq!(first_record.m(), Some(10.0));

    let close_sites = sites
        .iter()
        .filter(|site| matches!(site.source_ordinal(), 4 | 5))
        .collect::<Vec<_>>();
    assert_eq!(close_sites.len(), 2);
    let mut close_sites = close_sites.iter();
    let close_left = close_sites.next().unwrap();
    let close_right = close_sites.next().unwrap();
    assert!(close_sites.next().is_none());
    let rounded_left = frame
        .longitude()
        .lifted_site_longitude(close_left.longitude())
        .unwrap();
    let rounded_right = frame
        .longitude()
        .lifted_site_longitude(close_right.longitude())
        .unwrap();
    assert_eq!(rounded_left.to_bits(), rounded_right.to_bits());
    let exact_left = retained_site_longitude(frame.longitude(), close_left.longitude()).unwrap();
    let exact_right = retained_site_longitude(frame.longitude(), close_right.longitude()).unwrap();
    assert_ne!(
        exact_left.try_subtract(exact_right).unwrap().sign(),
        ExactSign::Zero
    );

    let mesh = super::delaunay::certified_delaunay_for_retained_points(frame, &sites).unwrap();
    assert_eq!(mesh.triangles().len(), 4);
    let mut used = std::collections::HashSet::new();
    for triangle in mesh.triangles() {
        for &site in triangle {
            used.insert(site);
        }
        assert_eq!(
            super::delaunay::RetainedPointCoordinates::new(frame, &sites)
                .orient(*triangle)
                .unwrap(),
            ExactSign::Positive
        );
    }
    assert_eq!(used.len(), 6, "every exact retained site is covered");
}

#[test]
fn retained_delaunay_publication_keeps_canonical_faces_and_reciprocal_uses() {
    let source_points = [
        Point::new_axes(170.0, 0.0, ZOrdinate(Some(11.0)), MOrdinate(Some(21.0))).unwrap(),
        Point::new_axes(-170.0, 0.0, ZOrdinate(Some(12.0)), MOrdinate(Some(22.0))).unwrap(),
        Point::new_axes(-170.0, 10.0, ZOrdinate(Some(13.0)), MOrdinate(Some(23.0))).unwrap(),
        Point::new_axes(170.0, 8.0, ZOrdinate(Some(14.0)), MOrdinate(Some(24.0))).unwrap(),
    ];
    let shape = Shape::multi_point(
        CoordinateAxes::XY,
        CoordSeq::try_from_points(&source_points).unwrap(),
    );
    let publication = shape
        .delaunay_mesh_publication_in_domain(TopologyDomain::Geographic(GeographicFrame::crs84()))
        .unwrap();

    assert_eq!(publication.producer, super::MeshProducer::Delaunay);
    assert_eq!(publication.sites.len(), 4);
    assert_eq!(publication.faces.len(), 2);
    assert_eq!(publication.points.len(), 8);
    for (producer_ordinal, site) in publication.sites.iter().enumerate() {
        assert_eq!(site.producer_ordinal, producer_ordinal);
        assert_eq!(source_points[site.source_ordinal], site.record);
        assert!(matches!(site.source_ordinal, 0..=3));
        assert!(matches!(
            site.location,
            super::MeshSiteLocation::Retained(_)
        ));
    }

    let mut selected_faces = std::collections::HashSet::new();
    for face in &publication.faces {
        assert!(selected_faces.insert(face.selected_face));
        let output_vertices = face.output_vertices.clone();
        assert_eq!(output_vertices.len(), 4);
        assert!(!face.reversed);
        let mut selected_vertices = face.output_to_selected;
        selected_vertices.sort_unstable();
        assert_eq!(selected_vertices, [0, 1, 2]);
        for output_vertex in 0..3 {
            assert_eq!(
                face.output_sites[output_vertex],
                face.selected_sites[usize::from(face.output_to_selected[output_vertex])]
            );
            assert_eq!(
                publication.points[output_vertices.start + output_vertex],
                publication.sites[face.output_sites[output_vertex]].record
            );
        }
        assert_eq!(
            publication.points[output_vertices.start + 3],
            publication.points[output_vertices.start]
        );
        for (output_side, boundary) in face.boundary_uses.iter().enumerate() {
            assert_eq!(usize::from(boundary.output_side), output_side);
            assert_eq!(boundary.selected_use.face, face.selected_face);
            assert_eq!(
                usize::from(boundary.selected_use.side),
                usize::from(face.output_to_selected[output_side])
            );
            assert_eq!(boundary.role, super::MeshEdgeRole::Delaunay);
            assert!(!boundary.reversed_for_output);
            assert!(!boundary.cocircular);
            assert_eq!([boundary.start_site, boundary.end_site], [
                face.output_sites[output_side],
                face.output_sites[(output_side + 1) % 3],
            ]);
            if let Some(reverse) = boundary.reverse {
                let reciprocal = publication
                    .faces
                    .iter()
                    .flat_map(|other| other.boundary_uses.iter())
                    .find(|other| other.selected_use == reverse)
                    .expect("every selected interior use has its authentic twin");
                assert_eq!(reciprocal.reverse, Some(boundary.selected_use));
                assert_eq!([reciprocal.start_site, reciprocal.end_site], [
                    boundary.end_site,
                    boundary.start_site
                ]);
            }
        }
    }
}

#[test]
fn retained_site_mesh_flags_a_cocircular_seam_rectangle() {
    let frame = GeographicFrame::crs84();
    let source_points = [
        raw(170.0, 0.0),
        raw(-170.0, 0.0),
        raw(-170.0, 10.0),
        raw(170.0, 10.0),
    ];
    let chart = PointSelectionChart::try_from_points(frame, &source_points)
        .unwrap()
        .unwrap();
    let mut sites = chart.sites_from_points(&source_points).unwrap();
    sites.sort_unstable_by(|left, right| left.compare_chart_xy(*right));
    let mesh = super::delaunay::certified_delaunay_for_retained_points(frame, &sites).unwrap();
    assert_eq!(mesh.triangles().len(), 2);
    assert_eq!(
        (0..mesh.halfedges().len())
            .filter(|&edge| mesh.edge_is_cocircular(edge))
            .count(),
        2,
        "the one geometric-zero diagonal owns both directed slots"
    );
}

#[test]
fn retained_site_incircle_uses_the_common_chart_for_a_bank_trapezoid() {
    let frame = GeographicFrame::crs84();
    let source_points = [
        raw(170.0, 0.0),
        raw(-170.0, 0.0),
        raw(-170.0, 10.0),
        raw(170.0, 8.0),
    ];
    let chart = PointSelectionChart::try_from_points(frame, &source_points)
        .unwrap()
        .unwrap();
    let mut sites = chart.sites_from_points(&source_points).unwrap();
    sites.sort_unstable_by(|left, right| left.compare_chart_xy(*right));
    let site_for_source = |ordinal| {
        sites
            .iter()
            .position(|site| site.source_ordinal() == ordinal)
            .unwrap()
    };
    let [a, b, c, d] = [
        site_for_source(0),
        site_for_source(1),
        site_for_source(2),
        site_for_source(3),
    ];
    let mesh = super::delaunay::certified_delaunay_for_retained_points(frame, &sites).unwrap();
    let has_edge = |left, right| {
        mesh.triangles().iter().any(|triangle| {
            triangle
                .iter()
                .zip(triangle.iter().cycle().skip(1))
                .any(|(&start, &end)| {
                    (start == left && end == right) || (start == right && end == left)
                })
        })
    };
    assert_eq!(mesh.triangles().len(), 2);
    assert!(has_edge(b, d), "the exact chart Delaunay diagonal is BD");
    assert!(
        !has_edge(a, c),
        "the raw-X diagonal AC is not Delaunay here"
    );
}

#[test]
fn retained_collinear_hull_orders_every_chart_site() {
    let frame = GeographicFrame::crs84();
    let source_points = [raw(170.0, -40.0), raw(-180.0, 0.0), raw(-170.0, 40.0)];
    let chart = PointSelectionChart::try_from_points(frame, &source_points)
        .unwrap()
        .unwrap();
    let mut sites = chart.sites_from_points(&source_points).unwrap();
    sites.sort_unstable_by(|left, right| left.compare_chart_xy(*right));
    let coordinates = super::delaunay::RetainedPointCoordinates::new(frame, &sites);
    let mesh = super::delaunay::certified_delaunay_for_retained_points(frame, &sites).unwrap();
    assert!(mesh.triangles().is_empty());
    let order = mesh.collinear_order().unwrap();
    assert_eq!(order.len(), sites.len());
    assert_eq!(coordinates.orient([0, 1, 2]).unwrap(), ExactSign::Zero);
    assert_eq!(order, vec![0, 1, 2]);
    assert!(
        order
            .iter()
            .zip(order.iter().skip(1))
            .all(|(&left, &right)| {
                coordinates.compare_x(coordinates.query(left), coordinates.query(right))
                    != std::cmp::Ordering::Greater
            })
    );
}

#[derive(Clone, Copy)]
enum FailingMeshPredicate {
    Orient,
    Incircle,
}

struct FailingRawCoordinates {
    points: [XY; 4],
    fail: FailingMeshPredicate,
}

impl super::delaunay::exact_delaunay::MeshCoordinates for FailingRawCoordinates {
    type Query = XY;
    type Key = [u64; 2];

    fn query(&self, site: usize) -> Self::Query {
        let point = self.points.get(site).expect("test mesh site is admitted");
        *point
    }

    fn key(&self, query: Self::Query) -> Self::Key {
        let normalize = |value: f64| {
            if value == 0.0 {
                0.0_f64.to_bits()
            } else {
                value.to_bits()
            }
        };
        [normalize(query.x), normalize(query.y)]
    }

    fn compare(&self, left: Self::Query, right: Self::Query) -> std::cmp::Ordering {
        super::delaunay::exact_delaunay::compare_xy(left, right)
    }

    fn compare_x(&self, left: Self::Query, right: Self::Query) -> std::cmp::Ordering {
        super::delaunay::exact_delaunay::compare_f64(left.x, right.x)
    }

    fn compare_y(&self, left: Self::Query, right: Self::Query) -> std::cmp::Ordering {
        super::delaunay::exact_delaunay::compare_f64(left.y, right.y)
    }

    fn orient(&self, [a, b, c]: [Self::Query; 3]) -> crate::error::Result<ExactSign> {
        if matches!(self.fail, FailingMeshPredicate::Orient) {
            return Err(crate::error::ResourceError::SizeOverflow.into());
        }
        Ok(super::exact::orient2d(a, b, c))
    }

    fn incircle(&self, [a, b, c, d]: [Self::Query; 4]) -> crate::error::Result<ExactSign> {
        if matches!(self.fail, FailingMeshPredicate::Incircle) {
            return Err(crate::error::ResourceError::SizeOverflow.into());
        }
        super::exact::incircle(a, b, c, d)
    }

    fn compare_axis_spans(
        &self,
        [min_x, max_x]: [Self::Query; 2],
        [min_y, max_y]: [Self::Query; 2],
    ) -> crate::error::Result<std::cmp::Ordering> {
        Ok(super::delaunay::exact_delaunay::compare_f64(
            max_x.x - min_x.x,
            max_y.y - min_y.y,
        ))
    }

    fn locality_xy(&self, site: usize) -> XY {
        self.query(site)
    }
}

#[test]
fn exact_mesh_propagates_retained_orientation_errors() {
    let coordinates = FailingRawCoordinates {
        points: [
            XY::new(0.0, 0.0),
            XY::new(2.0, 0.0),
            XY::new(0.0, 2.0),
            XY::new(2.0, 2.0),
        ],
        fail: FailingMeshPredicate::Orient,
    };
    let Err(error) = super::delaunay::certified_delaunay_with_coordinates(&coordinates, 3) else {
        panic!("injected hull orientation error must propagate");
    };
    assert!(matches!(
        error.kind(),
        ErrorKind::Resource(crate::error::ResourceError::SizeOverflow)
    ));
}

#[test]
fn exact_mesh_propagates_retained_incircle_errors() {
    let coordinates = FailingRawCoordinates {
        points: [
            XY::new(0.0, 0.0),
            XY::new(2.0, 0.0),
            XY::new(2.0, 2.0),
            XY::new(0.0, 2.0),
        ],
        fail: FailingMeshPredicate::Incircle,
    };
    let Err(error) = super::delaunay::certified_delaunay_with_coordinates(&coordinates, 4) else {
        panic!("injected in-circle error must propagate");
    };
    assert!(matches!(
        error.kind(),
        ErrorKind::Resource(crate::error::ResourceError::SizeOverflow)
    ));
}

fn sorted_indices(cycle: &[usize]) -> Vec<usize> {
    let mut indices = cycle.to_vec();
    indices.sort_unstable();
    indices
}

fn assert_sites_covered_by_nondegenerate_triangles(points: &[Point]) {
    let triangulation = super::delaunay::delaunay_triangulation(points).unwrap();
    let mut used = std::collections::HashSet::<usize>::new();
    for triangle in triangulation.triangles.as_chunks::<3>().0 {
        assert_ne!(
            orientation(
                points[triangle[0]].xy(),
                points[triangle[1]].xy(),
                points[triangle[2]].xy()
            ),
            Orientation::Collinear,
            "degenerate face {triangle:?}"
        );
        used.extend(triangle);
    }
    assert_eq!(
        used.len(),
        points.len(),
        "every input site must appear in a triangle, used={used:?}"
    );
}

#[test]
fn mixed_zero_left_only_corner_hull_keeps_the_axis_extreme() {
    let points = [
        raw(-0.0, -3.0),
        raw(0.0, -2.0),
        raw(-0.0, -1.0),
        raw(-3.0, 0.0),
    ];
    assert_eq!(points[0].x.to_bits(), (-0.0_f64).to_bits());
    assert_eq!(points[1].x.to_bits(), 0.0_f64.to_bits());
    let hull = super::delaunay::exact_corner_hull_cycle(&points);
    assert_eq!(sorted_indices(&hull), vec![0, 2, 3]);
    assert_sites_covered_by_nondegenerate_triangles(&points);
}

#[test]
fn mixed_zero_right_only_corner_hull_drops_the_collinear_midpoint() {
    let points = [
        raw(0.0, -3.0),
        raw(-0.0, -2.0),
        raw(-0.0, -1.0),
        raw(1.0, 0.0),
    ];
    let hull = super::delaunay::exact_corner_hull_cycle(&points);
    assert_eq!(sorted_indices(&hull), vec![0, 2, 3]);
    assert_sites_covered_by_nondegenerate_triangles(&points);
}

#[test]
fn mixed_zero_both_sides_corner_hull_matches_unsigned_zero() {
    let mixed = [
        raw(-0.0, -3.0),
        raw(0.0, -2.0),
        raw(-0.0, -1.0),
        raw(-3.0, 0.0),
        raw(1.0, 0.0),
    ];
    let unsigned = [
        raw(0.0, -3.0),
        raw(0.0, -2.0),
        raw(0.0, -1.0),
        raw(-3.0, 0.0),
        raw(1.0, 0.0),
    ];
    let mixed_hull = super::delaunay::exact_corner_hull_cycle(&mixed);
    let unsigned_hull = super::delaunay::exact_corner_hull_cycle(&unsigned);
    assert_eq!(sorted_indices(&mixed_hull), sorted_indices(&unsigned_hull));
    assert_eq!(sorted_indices(&mixed_hull), vec![0, 3, 4]);
    assert_sites_covered_by_nondegenerate_triangles(&mixed);
}

#[test]
fn mixed_zero_vertical_collinear_orders_by_y() {
    let points = [
        raw(-0.0, 2.0),
        raw(0.0, -1.0),
        raw(-0.0, 0.0),
        raw(0.0, 1.0),
    ];
    assert_eq!(super::delaunay::exact_hull_cycle(&points), vec![1, 2, 3, 0]);
}

#[test]
fn mixed_zero_horizontal_collinear_orders_by_x() {
    let points = [
        raw(2.0, -0.0),
        raw(-1.0, 0.0),
        raw(0.0, -0.0),
        raw(1.0, 0.0),
    ];
    assert_eq!(super::delaunay::exact_hull_cycle(&points), vec![1, 2, 3, 0]);
}
