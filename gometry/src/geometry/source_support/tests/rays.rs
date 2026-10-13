    #[test]
fn twice_wound_nonzero_ring_has_areal_support() {
        let twice = "POLYGON ((0 0,4 0,4 4,0 4,0 0,4 0,4 4,0 4,0 0))";
        assert_eq!(location(twice, 2.0, 2.0), Loc::Interior);
        assert!(cartesian_area_covers_point(
            &shape(twice),
            Point::new_raw_xy(2.0, 2.0)
        ));
    }

    #[test]
fn cancelled_spur_does_not_create_support() {
        let spur = "POLYGON ((0 0,4 0,4 4,0 4,0 0,-2 0,0 0))";
        assert_eq!(location(spur, 2.0, 2.0), Loc::Interior);
        assert_eq!(location(spur, -1.0, 0.0), Loc::Exterior);
    }

    #[test]
fn cancelled_noncollinear_ring_has_no_support() {
        let cancelled = "POLYGON ((0 0,2 0,2 2,0 2,0 0,0 2,2 2,2 0,0 0))";
        let shape = shape(cancelled);
        assert_eq!(location(cancelled, 1.0, 1.0), Loc::Exterior);
        assert!(!shape.contains_point(Point::new_raw_xy(1.0, 1.0)));
        assert!(!crate::geometry::cartesian_support_nonempty(&shape).unwrap());
    }

    #[test]
fn shell_and_hole_are_composed_before_point_membership() {
        let external = "POLYGON ((0 0,10 0,10 10,0 10,0 0),(20 20,21 20,21 21,20 21,20 20))";
        let identical = "POLYGON ((0 0,10 0,10 10,0 10,0 0),(0 0,10 0,10 10,0 10,0 0))";
        assert_eq!(location(external, 5.0, 5.0), Loc::Interior);
        assert_eq!(location(identical, 5.0, 5.0), Loc::Exterior);
    }

    #[test]
fn collapsed_polygon_uses_closed_graph_support() {
        let collapsed = "POLYGON ((0 0,2 0,1 0,0 0))";
        assert_eq!(location(collapsed, 1.0, 0.0), Loc::Interior);
        assert_eq!(location(collapsed, 0.0, 0.0), Loc::Boundary);
        assert_eq!(location(collapsed, 3.0, 0.0), Loc::Exterior);
    }

    #[test]
fn areal_hole_trims_collapsed_shell_support() {
        let trimmed = "POLYGON ((0 0,10 0,5 0,0 0),(2 -1,8 -1,8 1,2 1,2 -1))";
        assert_eq!(location(trimmed, 1.0, 0.0), Loc::Interior);
        assert_eq!(location(trimmed, 5.0, 0.0), Loc::Exterior);
        assert_eq!(location(trimmed, 2.0, 0.0), Loc::Boundary);
    }

    #[test]
fn multiline_endpoint_parity_is_global() {
        let shared = "MULTILINESTRING ((0 0,1 0),(1 0,2 0))";
        let odd = "MULTILINESTRING ((0 0,2 0),(1 0,1 1))";
        assert_eq!(location(shared, 1.0, 0.0), Loc::Interior);
        assert_eq!(location(odd, 1.0, 0.0), Loc::Boundary);
    }

    #[test]
fn set_membership_skips_shared_owners_but_location_keeps_endpoint_parity() {
        let line = shape("LINESTRING (0 0,1 0)");
        let owner = Shape::geometry_collection(vec![line]).unwrap();
        let shared =
            Shape::geometry_collection(vec![owner.clone(), owner]).expect("shared owner admitted");
        let bytes = crate::io::to_wkb(&shared, None, false).expect("shared input serializes");
        let expanded = crate::io::parse_wkb(&bytes)
            .expect("unshared expansion parses")
            .shape;
        let endpoint = Point::new_raw_xy(0.0, 0.0);

        assert_eq!(
            cartesian_point_location(&shared, endpoint),
            cartesian_point_location(&expanded, endpoint)
        );
        assert_eq!(cartesian_point_location(&shared, endpoint), Loc::Interior);
        assert!(shared.contains_point(endpoint));
        assert!(cartesian_point_covers(&shared, endpoint));
        assert_eq!(
            cartesian_point_covers(&shared, endpoint),
            cartesian_point_covers(&expanded, endpoint)
        );
        assert!(!cartesian_point_covers(
            &shared,
            Point::new_raw_xy(2.0, 0.0)
        ));
    }

    #[test]
fn exact_relation_uses_the_same_support_owner() {
        let square = shape("POLYGON ((0 0,4 0,4 4,0 4,0 0))");
        let external_hole = shape("POLYGON ((0 0,4 0,4 4,0 4,0 0),(8 8,9 8,9 9,8 9,8 8))");
        assert!(square.equals(&external_hole).unwrap());
        assert!(square.covers(&external_hole).unwrap());
        let erased_left = shape("POLYGON ((0 0,2 0,2 2,0 2,0 0,0 2,2 2,2 0,0 0))");
        let erased_right =
            shape("POLYGON ((10 10,12 10,12 12,10 12,10 10,10 12,12 12,12 10,10 10))");
        assert!(erased_left.equals(&erased_right).unwrap());

        let twice = shape("POLYGON ((0 0,4 0,4 4,0 4,0 0,4 0,4 4,0 4,0 0))");
        assert!(twice.contains_point(Point::new_raw_xy(2.0, 2.0)));
        assert_eq!(
            crate::geometry::occupied_cartesian_area(&twice).unwrap(),
            16.0
        );

        let collapsed = shape("POLYGON ((0 0,2 0,1 0,0 0))");
        let line = shape("LINESTRING (1 0,3 0)");
        assert!(collapsed.overlaps(&line).unwrap());
    }

    #[test]
fn exact_cells_trim_collapsed_shells_by_areal_and_point_holes() {
        let trimmed = shape("POLYGON ((0 0,10 0,5 0,0 0),(2 -1,8 -1,8 1,2 1,2 -1))");
        assert!(trimmed.contains_point(Point::new_raw_xy(1.0, 0.0)));
        assert!(!trimmed.contains_point(Point::new_raw_xy(5.0, 0.0)));
        assert!(trimmed.intersects(&shape("POINT (1 0)")).unwrap());
        assert!(!trimmed.intersects(&shape("POINT (5 0)")).unwrap());
        let crossing = trimmed
            .intersection(
                &shape("LINESTRING (4 0,6 0)"),
                crate::geometry::Strictness::Lenient,
            )
            .unwrap();
        assert!(crossing.is_empty(), "{crossing:?}");

        let erased = shape("POLYGON ((1 1,1 1,1 1,1 1),(1 1,1 1,1 1,1 1))");
        assert!(!erased.contains_point(Point::new_raw_xy(1.0, 1.0)));
        assert!(!erased.intersects(&shape("POINT (1 1)")).unwrap());
        assert!(!crate::geometry::cartesian_support_nonempty(&erased).unwrap());

        let boundary_erased = shape("POLYGON ((1 1,1 1,1 1,1 1),(1 1,3 1,3 3,1 3,1 1))");
        assert!(!boundary_erased.contains_point(Point::new_raw_xy(1.0, 1.0)));
        let survivor = shape("POLYGON ((1 1,1 1,1 1,1 1))");
        let members = Shape::geometry_collection(vec![boundary_erased, survivor]).unwrap();
        assert!(members.contains_point(Point::new_raw_xy(1.0, 1.0)));
        assert!(members.intersects(&shape("POINT (1 1)")).unwrap());
    }

    #[test]
fn exact_node_keeps_node_identity_and_late_ordinate_failures() {
        let crossing = shape("MULTILINESTRING ((0 0,2 2),(0 2,2 0))");
        let noded = crossing
            .node_with_context(
                crate::geometry::Strictness::Strict,
                &crate::geometry::SourceArcContext::cartesian(),
            )
            .unwrap();
        let expected = shape("MULTILINESTRING ((0 0,1 1),(1 1,2 2),(0 2,1 1),(1 1,2 0))");
        assert!(noded.equals(&expected).unwrap(), "{noded:?}");

        let left = shape("LINESTRING Z (0 0 0,2 2 0)");
        let right = shape("LINESTRING Z (0 2 1,2 0 1)");
        let error = crate::geometry::Shape::geometry_collection(vec![left, right])
            .unwrap()
            .node_with_context(
                crate::geometry::Strictness::Strict,
                &crate::geometry::SourceArcContext::cartesian(),
            )
            .unwrap_err();
        assert!(
            error.to_string().contains("node cannot represent Z/M"),
            "{error}"
        );

        let point = crate::geometry::Shape::MultiPoint(
            vec![
                Point::new_axes(
                    0.0,
                    0.0,
                    crate::geometry::ZOrdinate(Some(1.0)),
                    crate::geometry::MOrdinate(None),
                )
                .unwrap(),
            ]
            .into(),
        );
        assert!(matches!(
            point
                .node_with_context(
                    crate::geometry::Strictness::Strict,
                    &crate::geometry::SourceArcContext::cartesian(),
                )
                .unwrap(),
            Shape::Empty(
                crate::geometry::GeometryKind::MultiLineString,
                crate::geometry::CoordinateAxes::XYZ
            )
        ));
    }

    #[test]
fn exact_overlay_retains_xy_collinear_ordinate_knots() {
        let source = shape("POLYGON Z ((0 0 0,1 0 1,2 0 2,2 2 3,0 2 4,0 0 0))");
        let result = crate::geometry::overlay_all(
            &[&source],
            crate::geometry::TopologyDomain::Cartesian,
            crate::geometry::OverlayOp::Union,
            crate::geometry::Strictness::Strict,
        )
        .unwrap();
        let knot = result
            .points_vec()
            .unwrap()
            .into_iter()
            .find(|point| point.x == 1.0 && point.y == 0.0)
            .expect("the source knot remains on the selected boundary");
        assert_eq!(knot.z(), Some(1.0));
    }

    #[test]
fn exact_overlay_checks_both_ordinates_of_zero_length_occurrences() {
        let source = shape("POLYGON Z ((0 0 1,0 0 2,0 0 1,0 0 1))");
        let error = crate::geometry::overlay_all(
            &[&source],
            crate::geometry::TopologyDomain::Cartesian,
            crate::geometry::OverlayOp::Union,
            crate::geometry::Strictness::Strict,
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("cannot represent Z/M"),
            "{error}"
        );
    }

    #[test]
fn exact_overlay_closes_selected_support_once() {
        let retraced = shape("LINESTRING (0 0,1 0,0 0)");
        let point = shape("POINT (2 0)");
        let result = crate::geometry::overlay_all(
            &[&retraced, &point],
            crate::geometry::TopologyDomain::Cartesian,
            crate::geometry::OverlayOp::Union,
            crate::geometry::Strictness::Lenient,
        )
        .unwrap();
        assert!(result.contains(&retraced).unwrap());
        assert!(result.covers(&point).unwrap());

        let a = shape("LINESTRING (0 0,1 0)");
        let b = shape("LINESTRING (1 0,2 0)");
        let c = shape("LINESTRING (0 0,2 0)");
        let result = crate::geometry::overlay_all(
            &[&a, &b, &c],
            crate::geometry::TopologyDomain::Cartesian,
            crate::geometry::OverlayOp::SymmetricDifference,
            crate::geometry::Strictness::Lenient,
        )
        .unwrap();
        assert!(result.equals(&shape("POINT (1 0)")).unwrap());

        let line = shape("LINESTRING (0 0,2 0)");
        let point = shape("POINT (1 0)");
        let result = crate::geometry::overlay_all(
            &[&line, &point],
            crate::geometry::TopologyDomain::Cartesian,
            crate::geometry::OverlayOp::Difference,
            crate::geometry::Strictness::Lenient,
        )
        .unwrap();
        assert!(result.equals(&line).unwrap());
    }

    #[test]
fn exact_overlay_resolves_incident_axes_without_nearby_sources() {
        let left = shape("LINESTRING Z (0 0 0,2 2 0)");
        let right = shape("LINESTRING Z (0 2 0,2 0 0)");
        let result = crate::geometry::overlay_all(
            &[&left, &right],
            crate::geometry::TopologyDomain::Cartesian,
            crate::geometry::OverlayOp::Intersection,
            crate::geometry::Strictness::Strict,
        )
        .unwrap();
        let Shape::Point(point) = result else {
            panic!("crossing intersection must be a point");
        };
        assert!(point.axes.has_z());
        assert_eq!(point.z(), Some(0.0));
    }

    #[test]
fn exact_overlay_ignores_nonincident_ordinate_sources() {
        let left = shape("LINESTRING Z (0 0 0,2 2 0)");
        let right = Shape::geometry_collection(vec![
            shape("LINESTRING Z (0 2 0,2 0 0)"),
            shape("LINESTRING Z (10 10 7,11 11 7)"),
        ])
        .unwrap();
        let result = crate::geometry::overlay_all(
            &[&left, &right],
            crate::geometry::TopologyDomain::Cartesian,
            crate::geometry::OverlayOp::Intersection,
            crate::geometry::Strictness::Strict,
        )
        .unwrap();
        let Shape::Point(point) = result else {
            panic!("crossing intersection must be a point");
        };
        assert_eq!(point.z(), Some(0.0));
    }

    #[test]
fn exact_overlay_interpolates_each_incident_axis_once() {
        let left = shape("LINESTRING ZM (0 0 0 10,2 2 2 20)");
        let right = shape("LINESTRING ZM (0 2 0 10,2 0 2 20)");
        let result = crate::geometry::overlay_all(
            &[&left, &right],
            crate::geometry::TopologyDomain::Cartesian,
            crate::geometry::OverlayOp::Intersection,
            crate::geometry::Strictness::Strict,
        )
        .unwrap();
        let Shape::Point(point) = result else {
            panic!("crossing intersection must be a point");
        };
        assert_eq!(point.z(), Some(1.0));
        assert_eq!(point.m(), Some(15.0));
    }

    #[test]
fn exact_bounds_use_selected_support_not_raw_canceled_edges() {
        let spur = shape("POLYGON ((0 0,4 0,4 4,0 4,0 0,-2 0,0 0))");
        assert!(
            !crate::geometry::exact_intersects_bounds(
                &spur,
                crate::geometry::Bounds::new_raw(-1.5, -0.1, -0.5, 0.1)
            )
            .unwrap()
        );
        let external_hole = shape("POLYGON ((0 0,4 0,4 4,0 4,0 0),(8 8,9 8,9 9,8 9,8 8))");
        assert!(
            !crate::geometry::exact_intersects_bounds(
                &external_hole,
                crate::geometry::Bounds::new_raw(7.5, 7.5, 9.5, 9.5)
            )
            .unwrap()
        );
    }

    #[test]
fn line_interior_in_a_collection_does_not_claim_area() {
        let source =
            shape("GEOMETRYCOLLECTION (POLYGON ((0 0,4 0,4 4,0 4,0 0)),LINESTRING (10 0,14 4))");
        let location = cartesian_point_location_and_area(&source, Point::new_raw_xy(12.0, 2.0));
        assert_eq!(location.location, Loc::Interior);
        assert!(!location.area);

        let point = [
            ExactDyadic::from_f64(12.0),
            ExactDyadic::from_f64(2.0),
            ExactDyadic::one(),
        ];
        let exact = cartesian_exact_point_location_and_area(&source, &point).unwrap();
        assert_eq!(exact, location);
        assert_eq!(
            crate::geometry::PreparedPlan::transient(&source)
                .locate_exact(&point)
                .unwrap(),
            exact
        );
    }
