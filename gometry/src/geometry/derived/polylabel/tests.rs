use super::*;
use crate::error::ErrorKind;
use crate::geometry::{LineSeq, constructive, overlay};

fn circle_points(center: Point, radius: f64) -> Vec<Point> {
    let mut points = Vec::with_capacity(64);
    for step in 0..64 {
        let (unit_x, unit_y) = if step % 16 == 0 {
            match (step / 16) % 4 {
                0 => (1.0, 0.0),
                1 => (0.0, 1.0),
                2 => (-1.0, 0.0),
                _ => (0.0, -1.0),
            }
        } else {
            let (sin, cos) = (std::f64::consts::TAU * f64::from(step) / 64.0).sin_cos();
            (cos, sin)
        };
        points.push(Point::new_raw_xy(
            center.x + radius * unit_x,
            center.y + radius * unit_y,
        ));
    }
    points
}

fn degenerate_line_shape() -> Shape {
    Shape::Polygon(Polygon::new(
        Ring::from_closed(
            crate::geometry::CoordSeq::try_from_points(&[
                Point::new_raw_xy(0.0, 0.0),
                Point::new_raw_xy(2.0, 0.0),
                Point::new_raw_xy(1.0, 0.0),
                Point::new_raw_xy(0.0, 0.0),
            ])
            .unwrap(),
        )
        .unwrap(),
        Vec::new(),
    ))
}

fn exact_inscribed_support(shape: &Shape) -> (PreparedSupport, PreparedSupport) {
    let mut budget = ExpansionBudget::new("test", "support");
    let source = PreparedSupport::from_shape(shape, &mut budget).unwrap();
    let boundary = source.boundary_clone().unwrap();
    (source, boundary)
}

fn dense_ring_capacity(subdivisions: usize) -> usize {
    subdivisions
        .checked_add(1)
        .and_then(|count| count.checked_mul(2))
        .and_then(|count| count.checked_add(1))
        .expect("dense test-ring coordinate count fits usize")
}

fn dense_reciprocal_polygon(subdivisions: usize) -> Shape {
    let extent = 1.0e162;
    let half_height = 1.0e-162;
    let mut points = Vec::with_capacity(dense_ring_capacity(subdivisions));
    for index in 0..=subdivisions {
        let fraction = index as f64 / subdivisions as f64;
        let x = -extent + 2.0 * extent * fraction;
        points.push(Point::new_raw_xy(x, -half_height));
    }
    for index in (0..=subdivisions).rev() {
        let fraction = index as f64 / subdivisions as f64;
        let x = -extent + 2.0 * extent * fraction;
        points.push(Point::new_raw_xy(x, half_height));
    }
    points.push(points[0]);
    Shape::Polygon(Polygon::new(
        Ring::from_closed(CoordSeq::try_from_points(&points).unwrap()).unwrap(),
        Vec::new(),
    ))
}

fn dense_unit_polygon(subdivisions: usize) -> Shape {
    let mut points = Vec::with_capacity(dense_ring_capacity(subdivisions));
    for index in 0..=subdivisions {
        let fraction = index as f64 / subdivisions as f64;
        points.push(Point::new_raw_xy(-1.0 + 2.0 * fraction, -1.0));
    }
    for index in (0..=subdivisions).rev() {
        let fraction = index as f64 / subdivisions as f64;
        points.push(Point::new_raw_xy(-1.0 + 2.0 * fraction, 1.0));
    }
    points.push(points[0]);
    Shape::Polygon(Polygon::new(
        Ring::from_closed(CoordSeq::try_from_points(&points).unwrap()).unwrap(),
        Vec::new(),
    ))
}

fn normalized_dense_reciprocal_polygon() -> PolylabelFrame {
    let shape = dense_reciprocal_polygon(256);
    let bounds = shape.bounds().expect("dense polygon has bounds");
    normalized_polylabel_frame(&shape, bounds)
        .unwrap()
        .expect("reciprocal axes have a frame")
}

fn flat_framed_distance(frame: AxisFrame, linework: &PreparedLinework, point: Point) -> f64 {
    let mut best = f64::INFINITY;
    linework.for_each_segment(|segment| {
        best = best.min(framed_metric_nearest_on_segment(frame, point, segment).1);
    });
    best
}

fn indexed_framed_distance(
    frame: AxisFrame,
    linework: &PreparedLinework,
    bvh: &FacetBvh,
    point: Point,
) -> f64 {
    let mut stack = Vec::new();
    bvh.min_point_distance_with_stack_framed(
        linework,
        point,
        frame,
        &mut stack,
        f64::INFINITY,
        |facet| {
            let mut best = f64::INFINITY;
            for index in 0..facet.segment_count as usize {
                best = best.min(
                    framed_metric_nearest_on_segment(frame, point, linework.segment(facet, index))
                        .1,
                );
            }
            best
        },
    )
    .unwrap()
}

#[test]
fn reciprocal_ribbon_keeps_the_physical_nearest_edge() {
    let extent = 1.0e162;
    let half_height = 1.0 / extent;
    let shape = Shape::Polygon(Polygon::new(
        Ring::from_closed(
            CoordSeq::try_from_points(&[
                Point::new_raw_xy(-extent, -half_height),
                Point::new_raw_xy(extent, -half_height),
                Point::new_raw_xy(extent, half_height),
                Point::new_raw_xy(-extent, half_height),
                Point::new_raw_xy(-extent, -half_height),
            ])
            .unwrap(),
        )
        .unwrap(),
        Vec::new(),
    ));
    let center = polylabel_point(&shape, None).expect("finite ribbon");
    assert_eq!(center, Point::new_raw_xy(0.0, 0.0));
}

#[test]
fn reciprocal_axis_dense_indexed_field_preserves_framed_distance() {
    let normalized = normalized_dense_reciprocal_polygon();
    let linework = PreparedLinework::build(&normalized.shape).unwrap();
    let bvh = FacetBvh::build(&linework).expect("dense linework has an index");
    let mut field = PolylabelField::new(
        &normalized.shape,
        normalized.max_inradius,
        Some(normalized.frame),
    )
    .unwrap();

    for point in [
        Point::new_raw_xy(0.0, 0.0),
        Point::new_raw_xy(0.25, -0.2),
        Point::new_raw_xy(-0.75, 0.4),
    ] {
        let flat_distance = flat_framed_distance(normalized.frame, &linework, point);
        let indexed_distance = indexed_framed_distance(normalized.frame, &linework, &bvh, point);
        let field_distance = field.boundary_distance(point, f64::INFINITY).unwrap();
        assert_eq!(indexed_distance.to_bits(), flat_distance.to_bits());
        assert_eq!(field_distance.to_bits(), flat_distance.to_bits());
    }
}

#[test]
fn framed_subnormal_seed_stays_in_unsquared_distance_units() {
    let frame = AxisFrame::from_origin_extents(Point::new_raw_xy(0.0, 0.0), 1.0, 1.0e-320)
        .expect("positive frame extents");
    let shape = Shape::LineString(
        LineSeq::try_new(CoordSeq::try_from_xy(&[XY::new(0.0, 1.0), XY::new(1.0, 1.0)]).unwrap())
            .unwrap(),
    );
    let linework = PreparedLinework::build(&shape).unwrap();
    let bvh = FacetBvh::build(&linework).expect("one facet has a tree root");
    let point = Point::new_raw_xy(0.0, 0.0);
    let expected =
        framed_metric_nearest_on_segment(frame, point, linework.segment(linework.facets[0], 0)).1;
    assert!(expected > 0.0 && expected < f64::MIN_POSITIVE);
    let mut stack = Vec::new();
    let actual = bvh
        .min_point_distance_with_stack_framed(
            &linework,
            point,
            frame,
            &mut stack,
            expected,
            |facet| {
                let mut best = f64::INFINITY;
                for index in 0..facet.segment_count as usize {
                    best = best.min(
                        framed_metric_nearest_on_segment(
                            frame,
                            point,
                            linework.segment(facet, index),
                        )
                        .1,
                    );
                }
                best
            },
        )
        .unwrap();
    assert_eq!(actual.to_bits(), expected.to_bits());
}

#[test]
fn unframed_positive_seed_underflow_does_not_prune_positive_distance() {
    let shape = dense_unit_polygon(256);
    let mut field = PolylabelField::new(&shape, 1.0, None).unwrap();
    let tiny_seed = f64::from_bits(1);
    assert_eq!(tiny_seed * tiny_seed, 0.0);
    let distance = field
        .boundary_distance(Point::new_raw_xy(0.0, 0.0), tiny_seed)
        .unwrap();
    assert_eq!(distance.to_bits(), 1.0_f64.to_bits());
}

#[test]
fn reciprocal_axis_index_refines_fewer_facets_and_segments_than_flat() {
    let normalized = normalized_dense_reciprocal_polygon();
    let linework = PreparedLinework::build(&normalized.shape).unwrap();
    let bvh = FacetBvh::build(&linework).expect("dense linework has an index");
    let point = Point::new_raw_xy(0.0, 0.0);
    let mut refined_facets = 0;
    let mut refined_segments = 0;
    let mut stack = Vec::new();
    let indexed_distance = bvh
        .min_point_distance_with_stack_framed(
            &linework,
            point,
            normalized.frame,
            &mut stack,
            f64::INFINITY,
            |facet| {
                refined_facets += 1;
                refined_segments += facet.segment_count as usize;
                let mut best = f64::INFINITY;
                for index in 0..facet.segment_count as usize {
                    best = best.min(
                        framed_metric_nearest_on_segment(
                            normalized.frame,
                            point,
                            linework.segment(facet, index),
                        )
                        .1,
                    );
                }
                best
            },
        )
        .unwrap();
    let flat_distance = flat_framed_distance(normalized.frame, &linework, point);
    assert_eq!(indexed_distance.to_bits(), flat_distance.to_bits());
    assert!(refined_facets < linework.facets.len());
    assert!(refined_segments < linework.segment_count());
}

#[test]
fn zero_radius_boundary_point_uses_closed_membership() {
    let shape = degenerate_line_shape();
    let result = shape.maximum_inscribed_circle(None).unwrap();
    let Shape::Point(point) = result else {
        panic!("degenerate maximum circle must emit its point center");
    };
    assert!(shape.covers_point(point));
    assert!(!shape.contains_point(point));
}

#[test]
fn zero_radius_outside_point_is_rejected_at_emission_boundary() {
    let shape = degenerate_line_shape();
    let result = emit_zero_radius_point(&shape, Point::new_raw_xy(1.0, 1.0));
    assert!(matches!(
        result,
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::Geometry(
                    crate::geometry::GeometryErrorKind::UnrepresentableOutput {
                        operation: "maximum_inscribed_circle"
                    }
                )
            )
    ));
}

#[test]
fn source_tangent_does_not_enter_inscribed_circle() {
    let center = Point::new_raw_xy(1.0, 1.0);
    let points = circle_points(center, 1.0);
    let segment = Segment {
        start: XY::new(0.0, 0.0),
        end: XY::new(2.0, 0.0),
    };
    assert!(!segment_enters_candidate(segment, &points, true).unwrap());
    for segment in [
        Segment {
            start: XY::new(0.0, 0.0),
            end: XY::new(2.0, 0.0),
        },
        Segment {
            start: XY::new(2.0, 0.0),
            end: XY::new(2.0, 2.0),
        },
        Segment {
            start: XY::new(2.0, 2.0),
            end: XY::new(0.0, 2.0),
        },
        Segment {
            start: XY::new(0.0, 2.0),
            end: XY::new(0.0, 0.0),
        },
    ] {
        assert!(!segment_enters_candidate(segment, &points, true).unwrap());
    }
    let mut closed = points.clone();
    closed.push(points[0]);
    let seq = CoordSeq::try_from_points(&closed).unwrap();
    assert!(!crate::geometry::ring_winding(&seq).is_degenerate());
    assert!(crate::geometry::shell_is_convex(&seq));
    assert!(candidate_center_is_strictly_inside(&points, center, true));
    let source = Shape::Polygon(Polygon::new(
        Ring::from_closed(
            crate::geometry::CoordSeq::try_from_points(&[
                Point::new_raw_xy(0.0, 0.0),
                Point::new_raw_xy(2.0, 0.0),
                Point::new_raw_xy(2.0, 2.0),
                Point::new_raw_xy(0.0, 2.0),
                Point::new_raw_xy(0.0, 0.0),
            ])
            .unwrap(),
        )
        .unwrap(),
        Vec::new(),
    ));
    assert!(source.contains_point(center));
    assert!(!source_boundary_enters_candidate(&source, &points, true).unwrap());
}

#[test]
fn exact_boundary_radius_uses_the_smallest_stored_distance() {
    let shape = Shape::Polygon(Polygon::new(
        Ring::from_closed(
            crate::geometry::CoordSeq::try_from_points(&[
                Point::new_raw_xy(0.1, 0.1),
                Point::new_raw_xy(0.3, 0.1),
                Point::new_raw_xy(0.3, 0.3),
                Point::new_raw_xy(0.1, 0.3),
                Point::new_raw_xy(0.1, 0.1),
            ])
            .unwrap(),
        )
        .unwrap(),
        Vec::new(),
    ));
    assert_eq!(
        exact_boundary_radius(&shape, Point::new_raw_xy(0.2, 0.2))
            .unwrap()
            .to_bits(),
        0x3FB9_9999_9999_9998
    );
}

#[test]
fn exact_boundary_radius_handles_reciprocal_source_axes() {
    let extent = 1.0e150;
    let half_height = 1.0e-150;
    let shape = Shape::Polygon(Polygon::new(
        Ring::from_closed(
            crate::geometry::CoordSeq::try_from_points(&[
                Point::new_raw_xy(-extent, -half_height),
                Point::new_raw_xy(extent, -half_height),
                Point::new_raw_xy(extent, half_height),
                Point::new_raw_xy(-extent, half_height),
                Point::new_raw_xy(-extent, -half_height),
            ])
            .unwrap(),
        )
        .unwrap(),
        Vec::new(),
    ));
    assert_eq!(
        exact_boundary_radius(&shape, Point::new_raw_xy(0.0, 0.0))
            .unwrap()
            .to_bits(),
        half_height.to_bits()
    );
}

#[test]
fn source_hole_inside_candidate_is_rejected_without_boundary_contact() {
    let source = Shape::Polygon(Polygon::new(
        Ring::from_closed(
            crate::geometry::CoordSeq::try_from_points(&[
                Point::new_raw_xy(0.0, 0.0),
                Point::new_raw_xy(10.0, 0.0),
                Point::new_raw_xy(10.0, 10.0),
                Point::new_raw_xy(0.0, 10.0),
                Point::new_raw_xy(0.0, 0.0),
            ])
            .unwrap(),
        )
        .unwrap(),
        vec![
            Ring::from_closed(
                CoordSeq::try_from_points(&[
                    Point::new_raw_xy(4.5, 4.5),
                    Point::new_raw_xy(5.5, 4.5),
                    Point::new_raw_xy(5.5, 5.5),
                    Point::new_raw_xy(4.5, 5.5),
                    Point::new_raw_xy(4.5, 4.5),
                ])
                .unwrap(),
            )
            .unwrap(),
        ],
    ));
    let candidate = circle_points(Point::new_raw_xy(5.0, 3.0), 3.0);
    assert!(source.contains_point(Point::new_raw_xy(5.0, 3.0)));
    assert!(source_boundary_enters_candidate(&source, &candidate, true).unwrap());
}

#[test]
fn enclosed_hole_cycle_is_rejected_even_when_candidate_endpoints_are_covered() {
    let source = Shape::Polygon(Polygon::new(
        Ring::from_closed(
            crate::geometry::CoordSeq::try_from_points(&[
                Point::new_raw_xy(0.0, 0.0),
                Point::new_raw_xy(100.0, 0.0),
                Point::new_raw_xy(100.0, 100.0),
                Point::new_raw_xy(0.0, 100.0),
                Point::new_raw_xy(0.0, 0.0),
            ])
            .unwrap(),
        )
        .unwrap(),
        vec![
            Ring::from_closed(
                crate::geometry::CoordSeq::try_from_points(&[
                    Point::new_raw_xy(48.0, 48.0),
                    Point::new_raw_xy(52.0, 48.0),
                    Point::new_raw_xy(52.0, 52.0),
                    Point::new_raw_xy(48.0, 52.0),
                    Point::new_raw_xy(48.0, 48.0),
                ])
                .unwrap(),
            )
            .unwrap(),
        ],
    ));
    let candidate = [
        Point::new_raw_xy(55.0, 50.0),
        Point::new_raw_xy(40.0, 65.0),
        Point::new_raw_xy(25.0, 50.0),
        Point::new_raw_xy(40.0, 35.0),
    ];
    assert!(source.contains_point(Point::new_raw_xy(40.0, 50.0)));
    assert!(!source_vertex_outside_candidate(&source, &candidate));
    assert!(source_hole_is_enclosed(&source, &candidate, true));
}

#[test]
fn inward_chord_is_rejected_after_endpoint_only_checks_pass() {
    let source = Shape::Polygon(Polygon::new(
        Ring::from_closed(
            crate::geometry::CoordSeq::try_from_points(&[
                Point::new_raw_xy(0.0, 0.0),
                Point::new_raw_xy(10.0, 0.0),
                Point::new_raw_xy(10.0, 5.0),
                Point::new_raw_xy(5.0, 5.0),
                Point::new_raw_xy(5.0, 10.0),
                Point::new_raw_xy(0.0, 10.0),
                Point::new_raw_xy(0.0, 0.0),
            ])
            .unwrap(),
        )
        .unwrap(),
        Vec::new(),
    ));
    let candidate = [
        Point::new_raw_xy(6.5, 4.0),
        Point::new_raw_xy(4.0, 6.5),
        Point::new_raw_xy(1.5, 4.0),
        Point::new_raw_xy(4.0, 1.5),
    ];
    assert!(source.contains_point(Point::new_raw_xy(4.0, 4.0)));
    assert!(!source_vertex_outside_candidate(&source, &candidate));
    assert!(source_boundary_enters_candidate(&source, &candidate, true).unwrap());
}

#[test]
fn inscribed_circle_retries_after_a_rounded_radius_overrun() {
    let source = Shape::Polygon(Polygon::new(
        Ring::from_closed(
            crate::geometry::CoordSeq::try_from_points(&[
                Point::new_raw_xy(0.0, 1.0),
                Point::new_raw_xy(0.0, 0.0),
                Point::new_raw_xy(1.0, 0.0),
                Point::new_raw_xy(1.0, 1.0),
                Point::new_raw_xy(0.0, 1.0),
            ])
            .unwrap(),
        )
        .unwrap(),
        Vec::new(),
    ));
    let (prepared, boundary) = exact_inscribed_support(&source);
    let support = Some((&prepared, &boundary));
    let center = Point::new_raw_xy(0.5, 0.5);
    let quadrants = NonZeroU32::new(16).expect("non-zero");
    let steps = quadrants.get() as usize * 4;
    let radius = 0.5_f64.next_up();
    let first = build_inscribed_circle_once(center, radius, steps, steps + 1, quadrants, support);
    assert!(
        first.is_err(),
        "the rounded first radius must expose the boundary-rounding retry"
    );
    let emitted = build_inscribed_circle(center, radius, quadrants, support)
        .expect("the one-ULP retry must produce a contained ring");
    assert_eq!(emitted.geometry_type(), "Polygon");
}

#[test]
fn certified_inscribed_circle_reuses_the_directed_support_at_emission() {
    let source =
        crate::io::parse_wkt("MULTIPOLYGON (((0 0,2 0,2 4,0 4,0 0)),((2 0,4 0,4 4,2 4,2 0)))")
            .unwrap();
    overlay::reset_partition_calls();
    source.maximum_inscribed_circle(Some(1e-9)).unwrap();
    assert_eq!(overlay::partition_calls(), 1);
}

#[test]
fn candidate_center_must_be_strictly_inside_every_edge() {
    let candidate = circle_points(Point::new_raw_xy(5.0, 3.0), 3.0);
    assert!(candidate_center_is_strictly_inside(
        &candidate,
        Point::new_raw_xy(5.0, 3.0),
        true,
    ));
    assert!(!candidate_center_is_strictly_inside(
        &candidate,
        Point::new_raw_xy(5.0, 6.0),
        true,
    ));
}

#[test]
fn representative_point_uses_an_interior_chord_instead_of_the_box_center() {
    let shape = Shape::Polygon(Polygon::new(
        Ring::from_closed(
            crate::geometry::CoordSeq::try_from_points(&[
                Point::new_raw_xy(0.0, 0.0),
                Point::new_raw_xy(4.0, 0.0),
                Point::new_raw_xy(0.0, 4.0),
                Point::new_raw_xy(0.0, 0.0),
            ])
            .unwrap(),
        )
        .unwrap(),
        Vec::new(),
    ));
    let bounds = shape.bounds().expect("nonempty polygon");
    let bbox_seed = Point::new_raw_xy(
        bounds.minx() + (bounds.maxx() - bounds.minx()) / 2.0,
        bounds.miny() + (bounds.maxy() - bounds.miny()) / 2.0,
    );
    let representative = representative_point(&shape, bounds).expect("representative point");
    assert_eq!((representative.x, representative.y), (1.0, 2.0));
    assert_ne!(
        (representative.x, representative.y),
        (bbox_seed.x, bbox_seed.y)
    );
}

#[test]
fn polylabel_refines_the_frontier_to_the_right_triangle_inradius() {
    let source = Shape::Polygon(Polygon::new(
        Ring::from_closed(
            CoordSeq::try_from_points(&[
                Point::new_raw_xy(0.0, 0.0),
                Point::new_raw_xy(10.0, 0.0),
                Point::new_raw_xy(0.0, 10.0),
                Point::new_raw_xy(0.0, 0.0),
            ])
            .unwrap(),
        )
        .unwrap(),
        Vec::new(),
    ));
    let tolerance = 0.01;
    let center = polylabel_point(&source, Some(tolerance)).expect("finite triangle pole");
    let expected_radius = 10.0 - 5.0 * 2.0_f64.sqrt();
    let clearance = center
        .x
        .min(center.y)
        .min((10.0 - center.x - center.y) / 2.0_f64.sqrt());

    assert!(center.x > 0.0 && center.y > 0.0 && center.x + center.y < 10.0);
    assert!(source.covers_point(center));
    assert!(clearance >= expected_radius - tolerance);
    assert!(clearance <= expected_radius + 8.0 * f64::EPSILON);
}

#[test]
fn minimum_circle_radius_rounds_min_subnormal_support_exactly() {
    let u = f64::from_bits(1);
    let shape = Shape::MultiPoint(
        crate::geometry::MultiPoint::try_new(crate::geometry::CoordinateAxes::XY, vec![
            crate::geometry::MultiMember::Value(Point::new_raw_xy(0.0, 0.0)),
            crate::geometry::MultiMember::Value(Point::new_raw_xy(4.0 * u, 2.0 * u)),
        ])
        .expect("minimum-subnormal pair"),
    );
    assert_eq!(
        shape.minimum_bounding_radius().unwrap().to_bits(),
        2,
        "the pair's exact radius sqrt(5)u rounds to 2u"
    );
}

#[test]
fn minimum_circle_edge_certificate_rejects_an_inscribed_source_passing_ring() {
    let source = [Point::new_raw_xy(-1.0, 0.0), Point::new_raw_xy(1.0, 0.0)];
    let circle = smallest_enclosing_circle(&source).expect("diameter support");
    let disk = circle.certified_disk(&source).expect("exact disk");
    let mut budget = ExpansionBudget::new("minimum_bounding_circle", "realization");
    let output = constructive::point_buffer(
        disk.center().expect("finite center"),
        1.0 + 1.0e-6,
        NonZeroU32::new(16).expect("non-zero"),
        &mut budget,
    )
    .expect("inscribed ring construction");
    let Shape::Polygon(polygon) = &output else {
        panic!("point buffer must emit a polygon");
    };
    let shell = polygon.rings().next().expect("shell");
    let vertices: Vec<_> = shell
        .coords()
        .iter_coords()
        .map(|point| point.xy())
        .collect();
    let open = &vertices[..vertices.len() - 1];
    assert!(
        open.iter().enumerate().all(|(index, &left)| {
            let right = open[(index + 1) % open.len()];
            source
                .iter()
                .all(|point| exact::orient2d(left, right, point.xy()) != ExactSign::Negative)
        }),
        "the old source-point-only check accepts this ring"
    );
    assert!(
        certify_minimum_circle_polygon(&output, &disk, NonZeroU32::new(16).expect("non-zero"))
            .is_err(),
        "the exact disk edge check must reject its inward chords"
    );
}

#[test]
fn minimum_circle_certificate_rejects_an_inward_vertex_mutation() {
    let source = [Point::new_raw_xy(-1.0, 0.0), Point::new_raw_xy(1.0, 0.0)];
    let circle = smallest_enclosing_circle(&source).expect("diameter support");
    let disk = circle.certified_disk(&source).expect("exact disk");
    let valid = emit_minimum_bounding_circle(
        &disk,
        NonZeroU32::new(16).expect("non-zero"),
        &mut realization_budget(),
    )
    .expect("valid ring");
    let Shape::Polygon(polygon) = valid else {
        panic!("minimum circle must emit a polygon");
    };
    let shell = polygon.rings().next().expect("shell");
    let mut vertices: Vec<_> = shell.coords().iter_coords().collect();
    vertices[0] = Point::new_raw_xy(0.5, 0.0);
    let mutated = Shape::Polygon(Polygon::new(
        Ring::from_closed(CoordSeq::try_from_points(&vertices).expect("mutated ring")).unwrap(),
        Vec::new(),
    ));
    assert!(
        certify_minimum_circle_polygon(&mutated, &disk, NonZeroU32::new(16).expect("non-zero"))
            .is_err(),
        "an inward vertex must fail the exact disk certificate"
    );
}

#[test]
fn minimum_circle_certificate_does_not_assume_radius_denominator_is_center_weight_squared() {
    let source = [Point::new_raw_xy(-1.0, 0.0), Point::new_raw_xy(1.0, 0.0)];
    let circle = smallest_enclosing_circle(&source).expect("diameter support");
    let disk = circle.certified_disk(&source).expect("exact disk");
    let two = ExactDyadic::from_f64(2.0);
    let equivalent = CertifiedDisk {
        center_x: disk.center_x.clone(),
        center_y: disk.center_y.clone(),
        center_w: disk.center_w.clone(),
        radius_squared_numerator: disk
            .radius_squared_numerator
            .try_product(&two)
            .expect("scaled numerator"),
        radius_squared_denominator: disk
            .radius_squared_denominator
            .try_product(&two)
            .expect("scaled denominator"),
        source_center: disk.source_center,
        exact_zero_radius: disk.exact_zero_radius,
    };
    let quadrants = NonZeroU32::new(16).expect("non-zero");
    let mut budget = ExpansionBudget::new("minimum_bounding_circle", "realization");
    let output = tangent_circle_polygon(
        equivalent.center().expect("finite center"),
        equivalent.radius().expect("finite radius"),
        quadrants,
        &mut budget,
    )
    .expect("equivalent disk polygon");
    certify_minimum_circle_polygon(&output, &equivalent, quadrants)
        .expect("equivalent S/T representation must certify");
}

#[test]
fn minimum_circle_emission_keeps_the_requested_quadrant_count() {
    let source = [Point::new_raw_xy(-1.0, 0.0), Point::new_raw_xy(1.0, 0.0)];
    let circle = smallest_enclosing_circle(&source).expect("diameter support");
    let disk = circle.certified_disk(&source).expect("exact disk");
    for quadrants in [1, 2, 16] {
        let output = emit_minimum_bounding_circle(
            &disk,
            NonZeroU32::new(quadrants).expect("non-zero"),
            &mut realization_budget(),
        )
        .expect("representable tangent polygon");
        let Shape::Polygon(polygon) = output else {
            panic!("minimum circle must emit a polygon");
        };
        assert_eq!(
            polygon.rings().next().expect("shell").coords().len(),
            quadrants as usize * 4 + 1
        );
    }
}

#[test]
fn minimum_circle_emission_retries_when_rounded_radius_is_below_tangent() {
    let source = [Point::new_raw_xy(0.0, 0.0), Point::new_raw_xy(1.0, 5.0)];
    let circle = smallest_enclosing_circle(&source).expect("diameter support");
    let disk = circle.certified_disk(&source).expect("exact disk");
    let quadrants = NonZeroU32::new(16).expect("non-zero");
    let mut budget = ExpansionBudget::new("minimum_bounding_circle", "realization");
    let candidate = tangent_circle_polygon(
        disk.center().expect("finite center"),
        disk.radius().expect("finite radius"),
        quadrants,
        &mut budget,
    )
    .expect("candidate polygon");
    assert!(
        certify_minimum_circle_polygon(&candidate, &disk, quadrants).is_err(),
        "the rounded-down radius must expose an inward tangent edge"
    );
    let emitted = emit_minimum_bounding_circle(&disk, quadrants, &mut realization_budget())
        .expect("outward radius retry");
    certify_minimum_circle_polygon(&emitted, &disk, quadrants)
        .expect("retry must publish a certified ring");
}

#[test]
fn minimum_circle_polygon_reports_the_realization_budget_for_a_collapsed_ring() {
    // At 2^60 a radius step is 256 units, so the first retries cannot
    // certify, and each charges one more ring. Three rings of headroom run
    // out before the inflation reaches a representable ring.
    let points = [
        Point::new_raw_xy(2.0_f64.powi(60), 2.0_f64.powi(60)),
        Point::new_raw_xy(2.0_f64.powi(60) + 1024.0, 2.0_f64.powi(60)),
        Point::new_raw_xy(2.0_f64.powi(60) + 256.0, 2.0_f64.powi(60) + 768.0),
    ];
    let disk = smallest_enclosing_circle(&points)
        .expect("three sites")
        .certified_disk(&points)
        .expect("exact disk");
    let quadrants = NonZeroU32::new(16).expect("non-zero");
    let mut budget = realization_budget();
    budget
        .add(crate::geometry::GENERATED_ITEM_LIMIT - 3 * (4 * 16 + 1))
        .expect("headroom for three rings");
    let error = emit_minimum_bounding_circle(&disk, quadrants, &mut budget).unwrap_err();
    assert!(matches!(
        error.kind(),
        ErrorKind::Geometry(
            crate::geometry::GeometryErrorKind::GeneratedOutputTooLarge {
                operation: "minimum_bounding_circle",
                parameter: "realization",
                ..
            }
        )
    ));
}

fn realization_budget() -> ExpansionBudget {
    ExpansionBudget::new("minimum_bounding_circle", "realization")
}

#[test]
fn minimum_circle_polygon_maps_nonfinite_source_to_unrepresentable_output() {
    let error = Shape::Point(Point::new_raw_xy(f64::INFINITY, 0.0))
        .minimum_bounding_circle()
        .unwrap_err();
    assert!(matches!(
        error.kind(),
        ErrorKind::Geometry(crate::geometry::GeometryErrorKind::UnrepresentableOutput {
            operation: "minimum_bounding_circle"
        })
    ));
}

#[test]
fn minimum_circle_support_matches_independent_boundary_cases() {
    let cases = [
        (
            vec![
                Point::new_raw_xy(0.0, 0.0),
                Point::new_raw_xy(4.0, 0.0),
                Point::new_raw_xy(1.0, 3.0),
            ],
            vec![XY::new(0.0, 0.0), XY::new(4.0, 0.0), XY::new(1.0, 3.0)],
        ),
        (
            vec![
                Point::new_raw_xy(0.0, 0.0),
                Point::new_raw_xy(4.0, 0.0),
                Point::new_raw_xy(1.0, 0.5),
            ],
            vec![XY::new(0.0, 0.0), XY::new(4.0, 0.0)],
        ),
        (
            vec![
                Point::new_raw_xy(-3.0, -3.0),
                Point::new_raw_xy(0.0, 0.0),
                Point::new_raw_xy(4.0, 4.0),
                Point::new_raw_xy(1.0, 1.0),
            ],
            vec![XY::new(-3.0, -3.0), XY::new(4.0, 4.0)],
        ),
    ];
    for (points, mut expected) in cases {
        let circle = smallest_enclosing_circle(&points).unwrap();
        let (support, count) = circle.support_xy(&points);
        let mut actual = support[..count].to_vec();
        actual.sort_unstable_by(|left, right| {
            left.x
                .total_cmp(&right.x)
                .then_with(|| left.y.total_cmp(&right.y))
        });
        expected.sort_unstable_by(|left, right| {
            left.x
                .total_cmp(&right.x)
                .then_with(|| left.y.total_cmp(&right.y))
        });
        assert_eq!(actual, expected);
    }
}

#[expect(
    clippy::arithmetic_side_effects,
    reason = "the Fisher-Yates upper bound iterates from length down through two"
)]
fn shuffle_with_seed(length: usize, seed: u64) -> Vec<usize> {
    let mut state = seed ^ length as u64;
    let mut order: Vec<usize> = (0..length).collect();
    for index in (1..length).rev() {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        let upper = index
            .checked_add(1)
            .expect("Fisher-Yates index is below the order length");
        order.swap(index, (state >> 33) as usize % upper);
    }
    order
}

fn fixed_shuffle(length: usize) -> Vec<usize> {
    shuffle_with_seed(length, 0x9E37_79B9_7F4A_7C15)
}

#[test]
fn minimum_circle_work_counters_cover_normal_and_inverse_fixed_shuffle_orders() {
    // These thresholds characterize this fixed parabola fixture: they
    // document the old fixed-shuffle adversary without making a universal
    // speed claim about the randomized algorithm.
    let length = 64;
    let source: Vec<_> = (0..length)
        .map(|index| {
            let x = index as f64;
            Point::new_raw_xy(x, x * x)
        })
        .collect();
    let ordinary_order = shuffle_with_seed(length, 0xD1B5_4A32_D192_ED03);
    let mut ordinary_work = CircleWork::default();
    let ordinary =
        solve_enclosing_circle_order(&source, &ordinary_order, &mut ordinary_work).unwrap();

    let permutation = fixed_shuffle(length);
    let mut inverse_source = vec![Point::new_raw_xy(0.0, 0.0); length];
    for (after, &before) in permutation.iter().enumerate() {
        inverse_source[before] = source[after];
    }
    let mut adversarial_work = CircleWork::default();
    let adversarial =
        solve_enclosing_circle_order(&inverse_source, &permutation, &mut adversarial_work).unwrap();

    assert_eq!(
        ordinary
            .certified_disk(&source)
            .unwrap()
            .radius()
            .unwrap()
            .to_bits(),
        adversarial
            .certified_disk(&inverse_source)
            .unwrap()
            .radius()
            .unwrap()
            .to_bits()
    );
    assert!(ordinary_work.contains > 0);
    assert!(adversarial_work.contains > 0);
    assert!(ordinary_work.contains < length * length / 4);
    assert!(adversarial_work.contains > length * length / 4);
    assert!(ordinary_work.one_supports > 0);
    assert!(ordinary_work.two_supports > 0);

    let three_source = [
        Point::new_raw_xy(0.0, 0.0),
        Point::new_raw_xy(4.0, 0.0),
        Point::new_raw_xy(1.0, 3.0),
    ];
    let mut three_work = CircleWork::default();
    solve_enclosing_circle_order(&three_source, &[0, 1, 2], &mut three_work).unwrap();
    assert!(three_work.three_supports > 0);
}
