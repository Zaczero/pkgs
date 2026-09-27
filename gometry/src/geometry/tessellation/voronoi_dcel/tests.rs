#[test]
fn direct_dual_subnormal_admission_matches_legacy_builder() {
    for (units, admitted) in [(1_u64, false), (4, true)] {
        let step = f64::from_bits(units);
        let sites: Vec<_> = [(0.0, 0.0), (step, 0.0), (0.0, step)]
            .into_iter()
            .enumerate()
            .map(|(id, (x, y))| Site {
                id,
                point: Point::new_raw_xy(x, y),
            })
            .collect();
        let mut dual_budget = ExpansionBudget::new("test", "subnormal dual");
        let dual = dual_build(
            &sites,
            VoronoiBoundary::Envelope,
            &mut dual_budget,
            &certified_delaunay(&sites).unwrap(),
        );
        let mut legacy_budget = ExpansionBudget::new("test", "subnormal legacy");
        let legacy = build(
            &sites,
            VoronoiBoundary::Envelope,
            &mut legacy_budget,
            false,
            &certified_delaunay(&sites).unwrap(),
        );

        match (admitted, dual, legacy) {
            (false, Err(dual), Err(legacy)) => {
                assert!(matches!(
                    dual.kind(),
                    crate::error::ErrorKind::Geometry(GeometryErrorKind::Voronoi(_))
                ));
                assert!(matches!(
                    legacy.kind(),
                    crate::error::ErrorKind::Geometry(GeometryErrorKind::Voronoi(_))
                ));
            },
            (true, Ok(dual), Ok(legacy)) => {
                let clip =
                    Shape::Polygon(Polygon::new(rectangle(0.0, 0.0, step, step), Vec::new()));
                for diagram in [dual, legacy] {
                    assert_eq!(diagram.polygons.len(), 3, "units={units}");
                    let cells: Vec<_> = diagram
                        .polygons
                        .iter()
                        .cloned()
                        .map(Shape::Polygon)
                        .collect();
                    assert!(cells.iter().all(|cell| matches!(cell, Shape::Polygon(polygon) if polygon_is_valid(polygon).unwrap())));
                    assert!(coverage_is_valid(&cells, 0.0).unwrap(), "units={units}");
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
            },
            (expected, dual, legacy) => panic!(
                "units={units} expected admitted={expected}, dual admitted={}, legacy admitted={}",
                dual.is_ok(),
                legacy.is_ok(),
            ),
        }
    }
}

use super::*;
use crate::geometry::segment_index::checked_index_count;

#[test]
fn box_index_keeps_ordinal_and_bounded_count_limits_distinct() {
    let full_range = (u32::MAX as usize).wrapping_add(1);
    assert_eq!(
        admit_box_ordinals(full_range).map_err(|error| error.to_string()),
        Ok(())
    );

    assert_eq!(
        checked_index_count(u32::MAX as usize).map_err(|error| error.to_string()),
        Ok(u32::MAX)
    );
    assert_eq!(
        checked_index_count(full_range).map_err(|error| error.to_string()),
        Err("resource size arithmetic overflowed".to_owned())
    );

    let too_many = full_range.wrapping_add(1);
    assert_eq!(
        admit_box_ordinals(too_many).map_err(|error| error.to_string()),
        Err("resource size arithmetic overflowed".to_owned())
    );
}

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
                let exact = exact::angle_ccw_cmp(
                    &exact::ExactPoint::from_xy(origin),
                    &exact::ExactPoint::from_xy(left),
                    &exact::ExactPoint::from_xy(right),
                );
                assert_eq!(actual.is_err(), exact.is_err());
                if let (Ok(actual), Ok(exact)) = (actual, exact) {
                    assert_eq!(actual, exact);
                }
            }
        }
    }
}

fn cycle(points: &[(f64, f64)]) -> Vec<exact::ExactPoint> {
    points
        .iter()
        .map(|&(x, y)| exact::ExactPoint::from_xy(XY::new(x, y)))
        .collect()
}

fn cycle_xy(points: &[(f64, f64)]) -> Vec<XY> {
    points.iter().map(|&(x, y)| XY::new(x, y)).collect()
}

fn rounded_clip_segment_certificate(
    segment: Segment,
    exact_rings: &[Vec<exact::ExactPoint>],
    rounded_rings: &[Vec<XY>],
    events: &mut Vec<exact::ExactPoint>,
    parity: &mut ClipRingParity,
) -> Result<bool> {
    let mut clip_edges = Vec::new();
    for (ring_index, (exact_ring, rounded_ring)) in
        exact_rings.iter().zip(rounded_rings).enumerate()
    {
        for ((start, end), (rounded_start, rounded_end)) in exact_ring
            .iter()
            .zip(exact_ring.iter().cycle().skip(1))
            .zip(rounded_ring.iter().zip(rounded_ring.iter().cycle().skip(1)))
        {
            clip_edges.push(ClipCertificateEdge {
                ring: ring_index,
                start,
                end,
                rounded: [*rounded_start, *rounded_end],
            });
        }
    }
    let polygon = Polygon::new(
        Ring::closed(
            rounded_rings[0]
                .iter()
                .map(|point| Point::new_raw_xy(point.x, point.y))
                .collect(),
        )?,
        rounded_rings[1..]
            .iter()
            .map(|ring| {
                Ring::closed(
                    ring.iter()
                        .map(|point| Point::new_raw_xy(point.x, point.y))
                        .collect(),
                )
            })
            .collect::<Result<Vec<_>>>()?,
    );
    let point_index = PolygonPointIndex::build(&polygon)?;
    let edge_ids: Vec<_> = (0..clip_edges.len()).collect();
    let y_index = YStabbingIndex::build(edge_ids, |&edge| {
        let [start, end] = clip_edges[edge].rounded;
        (
            start.x.min(end.x),
            start.x.max(end.x),
            start.y.min(end.y),
            start.y.max(end.y),
        )
    })?;
    let candidates: Vec<_> = (0..clip_edges.len()).collect();
    rounded_segment_covered_by_clip(
        segment,
        &clip_edges,
        &candidates,
        &point_index,
        &y_index,
        events,
        parity,
    )
}

#[test]
fn rounded_segment_certificate_matches_closed_clip_membership() {
    let rectangle_points = [(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0)];
    let hole_points = [(1.0, 1.0), (3.0, 1.0), (3.0, 3.0), (1.0, 3.0)];
    let rectangle = cycle(&rectangle_points);
    let rectangle_xy = cycle_xy(&rectangle_points);
    let hole = cycle(&hole_points);
    let hole_xy = cycle_xy(&hole_points);
    let mut events = Vec::new();
    let mut parity = ClipRingParity::new(2);
    assert!(
        rounded_clip_segment_certificate(
            Segment {
                start: XY::new(0.0, 0.0),
                end: XY::new(4.0, 0.0),
            },
            std::slice::from_ref(&rectangle),
            std::slice::from_ref(&rectangle_xy),
            &mut events,
            &mut parity,
        )
        .unwrap()
    );
    assert!(
        rounded_clip_segment_certificate(
            Segment {
                start: XY::new(0.5, 0.5),
                end: XY::new(3.5, 3.5),
            },
            std::slice::from_ref(&rectangle),
            std::slice::from_ref(&rectangle_xy),
            &mut events,
            &mut parity,
        )
        .unwrap()
    );
    assert!(
        !rounded_clip_segment_certificate(
            Segment {
                start: XY::new(0.5, 2.0),
                end: XY::new(3.5, 2.0),
            },
            &[rectangle.clone(), hole.clone()],
            &[rectangle_xy.clone(), hole_xy.clone()],
            &mut events,
            &mut parity,
        )
        .unwrap()
    );
    assert!(
        rounded_clip_segment_certificate(
            Segment {
                start: XY::new(1.0, 1.0),
                end: XY::new(3.0, 1.0),
            },
            &[rectangle.clone(), hole],
            &[rectangle_xy.clone(), hole_xy],
            &mut events,
            &mut parity,
        )
        .unwrap()
    );
    let concave_points = [
        (0.0, 0.0),
        (4.0, 0.0),
        (4.0, 4.0),
        (3.0, 4.0),
        (3.0, 1.0),
        (1.0, 1.0),
        (1.0, 4.0),
        (0.0, 4.0),
    ];
    let concave = cycle(&concave_points);
    let concave_xy = cycle_xy(&concave_points);
    assert!(
        !rounded_clip_segment_certificate(
            Segment {
                start: XY::new(0.5, 3.5),
                end: XY::new(3.5, 3.5),
            },
            &[concave],
            &[concave_xy],
            &mut events,
            &mut parity,
        )
        .unwrap()
    );
    let l_shape_points = [
        (0.0, 0.0),
        (4.0, 0.0),
        (4.0, 1.0),
        (1.0, 1.0),
        (1.0, 4.0),
        (0.0, 4.0),
    ];
    let l_shape = cycle(&l_shape_points);
    let l_shape_xy = cycle_xy(&l_shape_points);
    assert!(
        !rounded_clip_segment_certificate(
            Segment {
                start: XY::new(4.0, 1.0),
                end: XY::new(1.0, 4.0),
            },
            &[l_shape],
            &[l_shape_xy],
            &mut events,
            &mut parity,
        )
        .unwrap()
    );
    let slanted_edge_points = [
        (2.949_570_888_116_326e-6, 1.769_742_532_869_795_6e-6),
        (3.097_351_995_393_694_8e-6, 1.858_411_197_236_216_9e-6),
        (3.0e-6, 2.5e-6),
    ];
    let slanted_edge = cycle(&slanted_edge_points);
    let slanted_edge_xy = cycle_xy(&slanted_edge_points);
    let slanted_segment = Segment {
        start: XY::new(2.949_570_888_116_328e-6, 1.769_742_532_869_796_8e-6),
        end: XY::new(3.097_351_995_393_692_7e-6, 1.858_411_197_236_215_6e-6),
    };
    let slanted_midpoint = XY::new(
        f64::midpoint(slanted_segment.start.x, slanted_segment.end.x),
        f64::midpoint(slanted_segment.start.y, slanted_segment.end.y),
    );
    assert_eq!(
        exact::orient2d(
            slanted_segment.start,
            slanted_segment.end,
            slanted_edge_xy[0],
        ),
        ExactSign::Zero
    );
    assert_eq!(
        exact::orient2d(
            slanted_segment.start,
            slanted_segment.end,
            slanted_edge_xy[1],
        ),
        ExactSign::Zero
    );
    assert_eq!(
        exact::orient2d(
            slanted_edge_xy[0],
            slanted_edge_xy[1],
            slanted_segment.start,
        ),
        ExactSign::Zero
    );
    assert_eq!(
        exact::orient2d(slanted_edge_xy[0], slanted_edge_xy[1], slanted_segment.end,),
        ExactSign::Zero
    );
    assert!(slanted_edge_xy[0].x < slanted_segment.start.x);
    assert!(slanted_segment.end.x < slanted_edge_xy[1].x);
    assert_ne!(
        exact::orient2d(slanted_segment.start, slanted_segment.end, slanted_midpoint,),
        ExactSign::Zero
    );
    let slanted_result = rounded_clip_segment_certificate(
        slanted_segment,
        &[slanted_edge],
        &[slanted_edge_xy],
        &mut events,
        &mut parity,
    )
    .unwrap();
    assert!(slanted_result);
    assert!(
        !rounded_clip_segment_certificate(
            Segment {
                start: XY::new(-1.0, 2.0),
                end: XY::new(2.0, 2.0),
            },
            &[rectangle],
            &[rectangle_xy],
            &mut events,
            &mut parity,
        )
        .unwrap()
    );
}

fn validates(cycles: &[Vec<exact::ExactPoint>]) -> bool {
    let refs: Vec<_> = cycles.iter().map(Vec::as_slice).collect();
    validate_exact_cycles(&refs, 0).is_ok()
}

#[test]
fn finite_max_exact_candidate_boxes_remain_sparse() {
    let high = cycle(&[(f64::MAX, f64::MAX), (f64::MAX, f64::MAX.next_down())]);
    let low = cycle(&[(0.0, 0.0), (0.0, 1.0)]);
    let cycles = [high.as_slice(), low.as_slice()];
    let (offsets, candidates) = indexed_exact_segment_candidates(&cycles).unwrap();
    assert_eq!(offsets, [0, 2]);
    assert_eq!(candidates, [[0, 1], [2, 3]]);
}

#[test]
fn embedded_clip_admission_rejects_crossed_polygon() {
    let ring = Ring::closed(vec![
        Point::new_raw_xy(0.0, 0.0),
        Point::new_raw_xy(2.0, 2.0),
        Point::new_raw_xy(0.0, 2.0),
        Point::new_raw_xy(2.0, 0.0),
        Point::new_raw_xy(0.0, 0.0),
    ])
    .unwrap();
    assert!(admit_embedded_clip(&Polygon::new(ring, Vec::new())).is_err());
}

fn reversed_cycles(cycles: &[Vec<exact::ExactPoint>]) -> Vec<Vec<exact::ExactPoint>> {
    std::iter::once(&cycles[0])
        .chain(cycles[1..].iter().rev())
        .map(|cycle| cycle.iter().cloned().rev().collect())
        .collect()
}

fn assert_boundary_walk_euler(subdivision: &ExactSubdivision) {
    let components = subdivision
        .topology
        .component_of
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    let faces = subdivision.topology.face_starts.len() - 1;
    assert_eq!(
        subdivision.vertices.len() as isize - subdivision.edges.len() as isize + faces as isize,
        2 * components as isize
    );
    if !subdivision.labels.is_empty() {
        assert_eq!(subdivision.labels.len(), faces);
    }
}

#[test]
fn exact_cycle_validator_accepts_one_shell_hole_tangency() {
    let shell = cycle(&[(0.0, 0.0), (8.0, 0.0), (8.0, 8.0), (0.0, 8.0)]);
    let hole = cycle(&[(0.0, 4.0), (2.0, 5.0), (2.0, 3.0)]);
    assert!(validates(&[shell, hole]));
}

#[test]
fn exact_cycle_validator_rejects_two_shell_hole_contacts() {
    let shell = cycle(&[(0.0, 0.0), (8.0, 0.0), (8.0, 8.0), (0.0, 8.0)]);
    let hole = cycle(&[(0.0, 2.0), (2.0, 4.0), (0.0, 6.0)]);
    assert!(!validates(&[shell, hole]));
}

#[test]
fn exact_cycle_validator_rejects_crossed_ring() {
    let crossed = cycle(&[(0.0, 0.0), (4.0, 4.0), (0.0, 4.0), (4.0, 0.0)]);
    assert!(!validates(&[crossed]));
}

#[test]
fn exact_touch_graph_covers_all_five_outcomes_and_reversed_discovery() {
    let shell = cycle(&[(0.0, 0.0), (8.0, 0.0), (8.0, 8.0), (0.0, 8.0)]);
    let one_shell_touch = vec![shell.clone(), cycle(&[(0.0, 4.0), (2.0, 5.0), (2.0, 3.0)])];
    let two_shell_touches = vec![shell.clone(), cycle(&[(0.0, 2.0), (2.0, 4.0), (0.0, 6.0)])];
    let two_holes_touch_once = vec![
        shell.clone(),
        cycle(&[(2.0, 2.0), (4.0, 2.0), (4.0, 4.0), (2.0, 4.0)]),
        cycle(&[(4.0, 4.0), (6.0, 4.0), (6.0, 6.0), (4.0, 6.0)]),
    ];
    let four_hole_star = vec![
        cycle(&[(-10.0, -10.0), (10.0, -10.0), (10.0, 10.0), (-10.0, 10.0)]),
        cycle(&[(0.0, 0.0), (4.0, -1.0), (4.0, 1.0)]),
        cycle(&[(0.0, 0.0), (1.0, 4.0), (-1.0, 4.0)]),
        cycle(&[(0.0, 0.0), (-4.0, 1.0), (-4.0, -1.0)]),
        cycle(&[(0.0, 0.0), (-1.0, -4.0), (1.0, -4.0)]),
    ];
    let interleaved = vec![
        shell,
        cycle(&[(2.0, 2.0), (6.0, 2.0), (6.0, 6.0), (2.0, 6.0)]),
        cycle(&[(4.0, 0.0), (8.0, 4.0), (4.0, 8.0), (0.0, 4.0)]),
    ];
    let cases = [
        (one_shell_touch, true),
        (two_shell_touches, false),
        (two_holes_touch_once, true),
        (four_hole_star, true),
        (interleaved, false),
    ];
    for (cycles, expected) in cases {
        let restored = validates(&cycles);
        let reversed = validates(&reversed_cycles(&cycles));
        assert_eq!(restored, expected);
        assert_eq!(reversed, restored);
    }
}

#[test]
fn source_clip_contacts_keep_parameters_through_ring_normalization() {
    let polygon = Polygon::new(
        Ring::closed(vec![
            Point::new_raw_xy(0.0, 8.0),
            Point::new_raw_xy(8.0, 8.0),
            Point::new_raw_xy(8.0, 0.0),
            Point::new_raw_xy(8.0, 0.0),
            Point::new_raw_xy(0.0, 0.0),
        ])
        .unwrap(),
        vec![
            closed_ring(&[(4.0, 4.0), (6.0, 4.0), (6.0, 6.0), (4.0, 6.0)]),
            Ring::closed(vec![
                Point::new_raw_xy(2.0, 3.0),
                Point::new_raw_xy(2.0, 3.0),
                Point::new_raw_xy(2.0, 5.0),
                Point::new_raw_xy(-0.0, 4.0),
            ])
            .unwrap(),
        ],
    );
    assert!(polygon.holes[1].point_at(3).x.is_sign_negative());

    let mut source_contacts = Vec::new();
    assert!(
        polygon
            .validate_with_contacts("polygon", "$", &mut source_contacts)
            .unwrap()
            .is_none()
    );
    assert_eq!(source_contacts.len(), 2);
    for contact in &source_contacts {
        assert_eq!(contact.rings, [0, 2]);
        assert_eq!(contact.edges[0], 4);
        assert_eq!(
            PointKey::new(contact.point),
            PointKey::new(XY::new(0.0, 4.0))
        );
        let SegmentParameter::Ratio { num, den } = &contact.parameters[0] else {
            panic!("the shell contact is interior to its source edge");
        };
        assert_eq!(
            exact::ExactRational::new(num.clone(), den.clone())
                .unwrap()
                .round()
                .unwrap(),
            0.5
        );
        match contact.edges[1] {
            2 => assert!(matches!(contact.parameters[1], SegmentParameter::End)),
            3 => assert!(matches!(contact.parameters[1], SegmentParameter::Start)),
            edge => panic!("unexpected source hole edge {edge}"),
        }
    }

    let sites = sites_xy(&[(-1.0, 0.0), (1.0, 0.0)]);
    let clip = normalize_clip(&sites, VoronoiBoundary::Polygon(&polygon), false).unwrap();
    assert_eq!(
        clip.contacts
            .iter()
            .map(|contact| contact.segments)
            .collect::<Vec<_>>(),
        [[3, 4], [3, 6]]
    );
    assert!(clip.contacts.iter().all(|contact| {
        contact
            .point
            .same_position(&exact::ExactPoint::from_xy(XY::new(0.0, 4.0)))
            .unwrap()
    }));
    let rect = clip_rectangle(&clip).unwrap();
    let scaffold = build_scaffold(&rect, &clip).unwrap();
    for segment in [3_u32, 4, 6] {
        let primitive = scaffold
            .iter()
            .find(|primitive| primitive.clip_source == Some(ClipSegmentId(segment)))
            .expect("each normalized contact edge has a scaffold primitive");
        assert!(primitive.events.iter().any(|point| {
            point
                .same_position(&exact::ExactPoint::from_xy(XY::new(0.0, 4.0)))
                .unwrap()
        }));
    }
}

#[test]
fn source_clip_records_point_sized_collinear_contacts() {
    let polygon = Polygon::new(rectangle(0.0, 0.0, 10.0, 10.0), vec![
        closed_ring(&[(3.0, 5.0), (5.0, 5.0), (4.0, 7.0)]),
        closed_ring(&[(5.0, 5.0), (7.0, 5.0), (6.0, 7.0)]),
    ]);
    let mut source_contacts = Vec::new();
    assert!(
        polygon
            .validate_with_contacts("polygon", "$", &mut source_contacts)
            .unwrap()
            .is_none()
    );

    let clip = normalize_clip(
        &sites_xy(&[(-1.0, 0.0), (1.0, 0.0)]),
        VoronoiBoundary::Polygon(&polygon),
        false,
    )
    .unwrap();
    let shared_point = exact::ExactPoint::from_xy(XY::new(5.0, 5.0));
    assert!(
        clip.contacts
            .iter()
            .any(|contact| { contact.point.same_position(&shared_point).unwrap() })
    );
    let scaffold = build_scaffold(&clip_rectangle(&clip).unwrap(), &clip).unwrap();
    let contacted: Vec<_> = scaffold
        .iter()
        .filter(|primitive| {
            primitive
                .events
                .iter()
                .any(|point| point.same_position(&shared_point).unwrap())
        })
        .filter_map(|primitive| primitive.clip_source)
        .collect();
    assert!(contacted.len() >= 2);
}

#[test]
fn ordinary_five_site_diagram_is_owned_and_admitted() {
    let sites: Vec<_> = [(0.0, 0.0), (0.0, 1.0), (0.5, 0.4), (1.0, 0.0), (1.0, 1.0)]
        .into_iter()
        .enumerate()
        .map(|(id, (x, y))| Site {
            id,
            point: Point::new_raw_xy(x, y),
        })
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
            .all(|polygon| polygon_is_valid(polygon).unwrap())
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
        .map(|(id, point)| Site { id, point })
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
            .all(|polygon| polygon_is_valid(polygon).unwrap())
    );
}

fn assert_directed_partition(cells: &[Shape], bounds: [f64; 4]) {
    let mut edges: std::collections::BTreeMap<[PointKey; 2], Vec<[XY; 2]>> =
        std::collections::BTreeMap::new();
    for cell in cells {
        let Shape::Polygon(polygon) = cell else {
            panic!("Voronoi cells are polygons");
        };
        for index in 0..polygon.shell.len() - 1 {
            let edge = [
                polygon.shell.point_at(index).xy(),
                polygon.shell.point_at(index + 1).xy(),
            ];
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
        .voronoi_polygons(0.0, VoronoiBoundary::Padded)
        .expect("extreme cluster should be a valid partition");
    assert_eq!(cells.len(), points.len());
    assert!(cells.iter().all(|cell| match cell {
        Shape::Polygon(polygon) => polygon_is_valid(polygon).unwrap(),
        _ => false,
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
            exact::point_in_cycle(&rounded_ring, &exact::ExactPoint::from_xy(site.point.xy()),)
                .unwrap(),
            exact::PointInCycle::Outside,
        );
    }
    let padded = normalize_clip(&canonical, VoronoiBoundary::Padded, false).unwrap();
    let bounds = padded.frame_bounds();
    assert_directed_partition(&cells, bounds);
    // The generic coverage oracle intentionally retains its midpoint
    // certificate and rejects this one-ULP boundary case. The independent
    // union/difference oracle still observes the same rectangular result.
    assert!(!coverage_is_valid(&cells, 0.0).unwrap());
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
fn shifted_six_site_diagram_rejects_colliding_binary64_vertices() {
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
    .map(|(id, (x, y))| Site {
        id,
        point: Point::new_raw_xy(offset + x, offset + y),
    })
    .collect();
    let mut budget = ExpansionBudget::new("test", "Voronoi topology");
    let Err(error) = build(
        &sites,
        VoronoiBoundary::Envelope,
        &mut budget,
        true,
        &certified_delaunay(&sites).unwrap(),
    ) else {
        panic!("colliding exact vertices unexpectedly embedded")
    };
    assert!(
        error
            .to_string()
            .contains("no topology-preserving binary64 embedding")
    );
}

fn grafted_clip_subdivision(sites: &[Site], clip_polygon: &Polygon) -> ExactSubdivision {
    let clip = normalize_clip(sites, VoronoiBoundary::Polygon(clip_polygon), false).unwrap();
    let mesh = certified_delaunay(sites).unwrap();
    let complex = delaunay_complex(&mesh, sites).unwrap();
    let primal_edges = mesh.primal_edges();
    let adjacency = certified_site_adjacency(&primal_edges, sites.len()).unwrap();
    let rect = clip_rectangle(&clip).unwrap();
    let scaffold = build_scaffold(&rect, &clip).unwrap();
    let separators = emit_clipped_separators(sites, &primal_edges, &complex, &rect).unwrap();
    let atoms = node_scaffold_and_separators(scaffold, separators, sites, &adjacency, &clip, &rect)
        .unwrap();
    let mut subdivision = build_exact_subdivision(atoms).unwrap();
    graft_components(&mut subdivision, sites, &adjacency).unwrap();
    subdivision
}

fn walk_points(subdivision: &ExactSubdivision, face: BoundaryFaceId) -> Vec<exact::ExactPoint> {
    let topology = &subdivision.topology;
    let walk = &topology.face_slots[topology.face_starts[face.0 as usize] as usize
        ..topology.face_starts[face.0 as usize + 1] as usize];
    walk.iter()
        .map(|&slot| {
            subdivision.vertices[topology.owners[slot as usize] as usize]
                .point
                .clone()
        })
        .collect()
}

fn first_equal_label_misattaches_a_hole(subdivision: &ExactSubdivision) -> bool {
    let mut positives = Vec::new();
    let mut negatives = Vec::new();
    for (index, label) in subdivision.labels.iter().enumerate() {
        if !selected_cell_label(*label) {
            continue;
        }
        let face = BoundaryFaceId(index as u32);
        match exact::cycle_orientation(&walk_points(subdivision, face)).unwrap() {
            ExactSign::Positive => positives.push((face, *label)),
            ExactSign::Negative => negatives.push((face, *label)),
            ExactSign::Zero => panic!("selected walk has zero orientation"),
        }
    }
    positives.sort_unstable_by_key(|(face, _)| *face);
    negatives.into_iter().any(|(negative, label)| {
        let parent = positives
            .iter()
            .find(|(_, other)| other == &label)
            .expect("negative walk has a same-label positive")
            .0;
        let probe = &walk_points(subdivision, negative)[0];
        exact::point_in_cycle(&walk_points(subdivision, parent), probe).unwrap()
            != exact::PointInCycle::Inside
    })
}

fn rectangle(min_x: f64, min_y: f64, max_x: f64, max_y: f64) -> Ring {
    Ring::closed(
        [
            (min_x, min_y),
            (max_x, min_y),
            (max_x, max_y),
            (min_x, max_y),
            (min_x, min_y),
        ]
        .into_iter()
        .map(|(x, y)| Point::new_raw_xy(x, y))
        .collect(),
    )
    .unwrap()
}

fn sites_xy(points: &[(f64, f64)]) -> Vec<Site> {
    points
        .iter()
        .enumerate()
        .map(|(id, &(x, y))| Site {
            id,
            point: Point::new_raw_xy(x, y),
        })
        .collect()
}

fn separator_fixture() -> (Vec<Site>, Polygon) {
    (
        sites_xy(&[
            (-4.0, -3.0),
            (4.0, -2.0),
            (3.0, 4.0),
            (-3.0, 3.0),
            (0.0, 0.0),
        ]),
        Polygon::new(rectangle(-6.0, -6.0, 6.0, 6.0), Vec::new()),
    )
}

#[test]
fn polygon_clip_with_many_disjoint_holes_is_budgeted_by_output() {
    let holes = (0..1_334)
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
    let mut budget = ExpansionBudget::new("voronoi_polygons", "sites/clip topology");
    let cells = source
        .voronoi_polygons_budgeted(0.0, VoronoiBoundary::Polygon(&polygon), &mut budget)
        .expect("the 5,346-position partition is below the generated-output budget");

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
    assert_eq!(hole_count, 1_334);
    assert_eq!(coordinate_count, 5_346);
    assert_eq!(budget.used(), 7);
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

    let mut budget = ExpansionBudget::new("voronoi_edges", "sites/clip topology");
    match source.voronoi_edge_column_budgeted(0.0, VoronoiBoundary::Polygon(&polygon), &mut budget)
    {
        Err(error) => {
            assert!(
                error
                    .to_string()
                    .contains("no topology-preserving binary64 embedding"),
                "unexpected Voronoi error: {error}"
            );
        },
        Ok((coordinates, offsets)) => {
            assert!(offsets.as_slice().len() > 1);
            assert_eq!(
                offsets.as_slice().last().copied().map(|end| end as usize),
                Some(coordinates.len())
            );
            let clip = Shape::Polygon(polygon);
            for [start, end] in offsets.as_slice().array_windows::<2>() {
                let points = (*start as usize..*end as usize)
                    .map(|index| coordinates.point_at(index))
                    .collect::<Vec<_>>();
                let line = Shape::LineString(
                    LineSeq::try_new(CoordSeq::try_from_points(&points).unwrap()).unwrap(),
                );
                assert!(clip.covers(&line).unwrap());
            }
        },
    }
}

#[test]
fn sparse_collinear_edges_do_not_charge_clip_scan_work() {
    let mut boundary = Vec::with_capacity(20_000);
    for (start, end, subdivisions) in [
        (XY::new(-2.0, -1.0), XY::new(2.0, -0.5), 9_999_u32),
        (XY::new(2.0, -0.5), XY::new(2.0, 1.0), 1),
        (XY::new(2.0, 1.0), XY::new(-2.0, 0.5), 9_999),
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
    assert_eq!(polygon.shell.len() - 1, 20_000);

    let sites = (0_u32..801)
        .map(|index| Point::new_raw_xy(0.0, -0.5 + f64::from(index) / 800.0))
        .collect::<Vec<_>>();
    let source = Shape::MultiPoint(sites.into());
    let mut budget = ExpansionBudget::new("voronoi_edges", "sites/clip topology");
    let (coordinates, offsets) = source
        .voronoi_edge_column_budgeted(0.0, VoronoiBoundary::Polygon(&polygon), &mut budget)
        .expect("800 separator edges fit the output budget");

    assert_eq!(offsets.as_slice().len(), 801);
    assert_eq!(offsets.as_slice().last(), Some(&1_600));
    assert_eq!(coordinates.len(), 1_600);
    assert_eq!(budget.used(), 2_400);
}

fn coincident_frame_fixture() -> (Vec<Site>, Polygon) {
    (
        sites_xy(&[(0.0, -1.0), (0.0, 1.0), (2.0, 0.5)]),
        Polygon::new(rectangle(-1.0, 0.0, 0.5, 2.0), Vec::new()),
    )
}

fn node_polygon(sites: &[Site], polygon: &Polygon) -> NodedAtoms {
    let clip = normalize_clip(sites, VoronoiBoundary::Polygon(polygon), false).unwrap();
    let rect = clip_rectangle(&clip).unwrap();
    let mesh = certified_delaunay(sites).unwrap();
    let complex = delaunay_complex(&mesh, sites).unwrap();
    let primal_edges = mesh.primal_edges();
    let adjacency = certified_site_adjacency(&primal_edges, sites.len()).unwrap();
    let scaffold = build_scaffold(&rect, &clip).unwrap();
    let separators = emit_clipped_separators(sites, &primal_edges, &complex, &rect).unwrap();
    node_scaffold_and_separators(scaffold, separators, sites, &adjacency, &clip, &rect).unwrap()
}

fn old_frame_halfplane_holds(edge: &AtomicEdge, vertices: &[NodedVertex], sites: &[Site]) -> bool {
    let OwnerTransition::Boundary { left, right } = edge.forward.owner else {
        return true;
    };
    let start = &vertices[edge.endpoints[0].0 as usize].point;
    let end = &vertices[edge.endpoints[1].0 as usize].point;
    left.is_none_or(|site| {
        exact::orient_points(
            start,
            end,
            &exact::ExactPoint::from_xy(sites[site.0 as usize].point.xy()),
        )
        .unwrap()
            == ExactSign::Positive
    }) && right.is_none_or(|site| {
        exact::orient_points(
            start,
            end,
            &exact::ExactPoint::from_xy(sites[site.0 as usize].point.xy()),
        )
        .unwrap()
            == ExactSign::Negative
    })
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
            exact::orient_points(&separator.start, &separator.end, &left).unwrap(),
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
        match edge.forward.owner {
            OwnerTransition::Boundary {
                left: Some(SiteId(1)),
                right: None,
            }
            | OwnerTransition::Boundary {
                left: None,
                right: Some(SiteId(1)),
            } => {},
            other => panic!("coincident frame owner {other:?}"),
        }
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
fn packed_separator_column_matches_chain_coordinates() {
    let (sites, polygon) = separator_fixture();
    let mut budget = ExpansionBudget::new("test", "packed chains");
    let packed = build_edges(
        &sites,
        VoronoiBoundary::Polygon(&polygon),
        &mut budget,
        true,
        &certified_delaunay(&sites).unwrap(),
    )
    .unwrap();
    let (coords, offsets) = packed.clone().into_line_column().unwrap();
    let lines = packed.into_line_seqs().unwrap();
    assert!(!lines.is_empty());
    let ends = offsets.as_slice();
    assert_eq!(ends.len(), lines.len() + 1);
    assert_eq!(ends[0], 0);
    assert_eq!(ends[ends.len() - 1] as usize, coords.len());
    let mut cursor = 0_usize;
    for (line, &end) in lines.iter().zip(&ends[1..]) {
        let end = end as usize;
        assert_eq!(end - cursor, line.len());
        for index in 0..line.len() {
            assert_eq!(
                coords.point_at(cursor + index).xy(),
                line.point_at(index).xy()
            );
        }
        cursor = end;
    }
}

#[test]
fn direct_dual_edge_budget_counts_only_the_published_line_column() {
    let source = Shape::MultiPoint(
        vec![
            Point::new_raw_xy(0.0, 0.0),
            Point::new_raw_xy(2.0, 0.0),
            Point::new_raw_xy(0.0, 2.0),
            Point::new_raw_xy(2.0, 2.0),
            Point::new_raw_xy(0.8, 1.1),
        ]
        .into(),
    );
    let mut budget = ExpansionBudget::new("voronoi_edges", "sites/clip topology");
    let (coordinates, offsets) = source
        .voronoi_edge_column_budgeted(0.0, VoronoiBoundary::Envelope, &mut budget)
        .unwrap();
    let edge_count = offsets.as_slice().len() - 1;
    assert!(edge_count > 0);
    assert_eq!(offsets.as_slice().first(), Some(&0));
    assert_eq!(
        offsets.as_slice().last().copied().map(|end| end as usize),
        Some(coordinates.len())
    );
    assert_eq!(budget.used(), coordinates.len() + edge_count);
}

#[test]
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
fn interior_first_separator_order_emits_one_full_chain() {
    let sites = sites_xy(&[(0.0, -1.0), (0.0, 1.0), (2.0, 0.5)]);
    let polygon = Polygon::new(
        closed_ring(&[
            (-1.0, 0.0),
            (-0.5, 0.0),
            (0.0, 0.0),
            (0.5, 0.0),
            (0.5, 2.0),
            (-1.0, 2.0),
        ]),
        Vec::new(),
    );
    let atoms = node_polygon(&sites, &polygon);
    let mut subdivision = build_exact_subdivision(atoms).unwrap();
    let primal_edges = certified_delaunay(&sites).unwrap().primal_edges();
    let adjacency = certified_site_adjacency(&primal_edges, sites.len()).unwrap();
    graft_components(&mut subdivision, &sites, &adjacency).unwrap();
    let assembled = assemble_edges(&subdivision).unwrap();
    let separator = [SiteId(0), SiteId(1)];
    let matching: Vec<_> = assembled
        .iter()
        .filter(|chain| chain.separator == separator)
        .collect();
    assert_eq!(matching.len(), 1);
    assert_eq!(matching[0].edges.len(), 3);
    assert_eq!(matching[0].vertices.len(), 4);
    let known = [
        XY::new(-1.0, 0.0),
        XY::new(-0.5, 0.0),
        XY::new(0.0, 0.0),
        XY::new(0.5, 0.0),
    ];
    let round = |vertex: VertexId| {
        let vertex = &subdivision.vertices[vertex.0 as usize];
        vertex
            .fixed_xy
            .map_or_else(|| vertex.point.round_nearest_even(), Ok)
            .unwrap()
    };
    let assembled_coordinates: Vec<_> = matching[0].vertices.iter().copied().map(round).collect();
    assert_eq!(assembled_coordinates.as_slice(), known.as_slice());
    let mut group: Vec<_> = matching[0]
        .edges
        .iter()
        .map(|&edge| (separator, edge))
        .collect();
    group.rotate_left(1);
    let [left, right] = subdivision.edges[group[0].1.0 as usize].endpoints;
    let mut degree = std::collections::BTreeMap::<VertexId, usize>::new();
    for (_, edge_id) in &group {
        for vertex in subdivision.edges[edge_id.0 as usize].endpoints {
            *degree.entry(vertex).or_default() += 1;
        }
    }
    assert_eq!(degree[&left], 2);
    assert_eq!(degree[&right], 2);
    let mut chains = Vec::new();
    assemble_separator_group(&subdivision, separator, &group, &mut chains).unwrap();
    assert_eq!(chains.len(), 1);
    assert_eq!(chains[0].edges.len(), 3);
    assert_eq!(chains[0].vertices.len(), 4);
    let shuffled_coordinates: Vec<_> = chains[0].vertices.iter().copied().map(round).collect();
    assert_eq!(shuffled_coordinates.as_slice(), known.as_slice());
}

#[test]
fn edges_only_embed_still_rejects_colliding_binary64_vertices() {
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
    .map(|(id, (x, y))| Site {
        id,
        point: Point::new_raw_xy(offset + x, offset + y),
    })
    .collect();
    let clip = Polygon::new(
        rectangle(offset - 1.0, offset - 1.0, offset + 11.0, offset + 11.0),
        Vec::new(),
    );
    let mut budget = ExpansionBudget::new("test", "edges-only collision");
    let Err(error) = build_edges(
        &sites,
        VoronoiBoundary::Polygon(&clip),
        &mut budget,
        true,
        &certified_delaunay(&sites).unwrap(),
    ) else {
        panic!("colliding exact vertices unexpectedly embedded on the edges-only path")
    };
    assert!(
        error
            .to_string()
            .contains("no topology-preserving binary64 embedding")
    );
}

fn closed_ring(points: &[(f64, f64)]) -> Ring {
    Ring::closed(
        points
            .iter()
            .chain(points.first())
            .map(|&(x, y)| Point::new_raw_xy(x, y))
            .collect(),
    )
    .unwrap()
}

fn assert_cells_cover_clip(cells: &[Shape], clip: &Polygon) {
    assert!(!cells.is_empty());
    assert!(cells.iter().all(|cell| match cell {
        Shape::Polygon(polygon) => polygon_is_valid(polygon).unwrap(),
        _ => false,
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
    for (left, right) in cells
        .iter()
        .enumerate()
        .flat_map(|(index, left)| cells[index + 1..].iter().map(move |right| (left, right)))
    {
        assert_eq!(
            left.intersection(right, Strictness::Lenient)
                .unwrap()
                .area(),
            0.0
        );
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

fn site_shape(points: &[(f64, f64)]) -> Shape {
    Shape::MultiPoint(
        points
            .iter()
            .map(|&(x, y)| Point::new_raw_xy(x, y))
            .collect::<Vec<_>>()
            .into(),
    )
}

#[test]
fn diamond_clip_in_square_aabb_covers_clip_with_no_separator_through_contacts() {
    let diamond = Polygon::new(
        closed_ring(&[(0.0, 1.0), (1.0, 0.0), (2.0, 1.0), (1.0, 2.0)]),
        Vec::new(),
    );
    let sites = [(10.0, 0.0), (12.0, 0.0), (11.0, 1.0)];
    let cells = site_shape(&sites)
        .voronoi_polygons(0.0, VoronoiBoundary::Polygon(&diamond))
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
        .voronoi_polygons(0.0, VoronoiBoundary::Polygon(&clip))
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
        .voronoi_polygons(0.0, VoronoiBoundary::Polygon(&clip))
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
        .voronoi_polygons(0.0, VoronoiBoundary::Polygon(&clip))
        .unwrap();
    assert_one_cell_equals_clip(&cells, &clip);
    let Shape::Polygon(cell) = &cells[0] else {
        panic!("Voronoi cells are polygons");
    };
    assert_eq!(cell.holes.len(), 1);
}

#[test]
fn public_polygon_admits_sites_on_and_outside_clip_aabb() {
    let sites = sites_xy(&[(0.0, 0.0), (1.0, 0.0), (3.0, 1.0), (1.0, 1.0)]);
    let polygon = Polygon::new(rectangle(0.0, 0.0, 2.0, 2.0), Vec::new());
    let mut budget = ExpansionBudget::new("test", "sites on and outside R");
    let atoms = node_polygon(&sites, &polygon);
    assert!(
        atoms.edges.iter().any(|edge| {
            edge.separator.is_none()
                && edge.frame_side.is_some()
                && !old_frame_halfplane_holds(edge, &atoms.vertices, &sites)
        }),
        "old frame halfplane still holds on every frame-only atom"
    );
    build(
        &sites,
        VoronoiBoundary::Polygon(&polygon),
        &mut budget,
        true,
        &certified_delaunay(&sites).unwrap(),
    )
    .unwrap();
}

#[test]
fn subnormal_clip_aabb_exact_lane_admits_sites_on_corners() {
    let step = f64::from_bits(4);
    let sites = sites_xy(&[(0.0, 0.0), (step, 0.0), (0.0, step)]);
    let mut envelope_budget = ExpansionBudget::new("test", "units4 envelope");
    build(
        &sites,
        VoronoiBoundary::Envelope,
        &mut envelope_budget,
        false,
        &certified_delaunay(&sites).unwrap(),
    )
    .unwrap();
    let polygon = Polygon::new(rectangle(0.0, 0.0, step, step), Vec::new());
    let mut polygon_budget = ExpansionBudget::new("test", "units4 polygon");
    build(
        &sites,
        VoronoiBoundary::Polygon(&polygon),
        &mut polygon_budget,
        true,
        &certified_delaunay(&sites).unwrap(),
    )
    .unwrap();
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

#[test]
fn crossed_component_keeps_distinct_faces_with_the_same_owner() {
    let sites: Vec<_> = [(-10.0, -10.0), (0.0, 10.0), (10.0, -10.0)]
        .into_iter()
        .enumerate()
        .map(|(id, (x, y))| Site {
            id,
            point: Point::new_raw_xy(x, y),
        })
        .collect();
    let clip_polygon = Polygon::new(rectangle(-20.0, -20.0, 20.0, 20.0), vec![rectangle(
        -12.0, -1.0, 12.0, 1.0,
    )]);
    let atoms = node_polygon(&sites, &clip_polygon);
    let mut subdivision = build_exact_subdivision(atoms).unwrap();
    assert_boundary_walk_euler(&subdivision);
    let primal_edges = certified_delaunay(&sites).unwrap().primal_edges();
    let adjacency = certified_site_adjacency(&primal_edges, sites.len()).unwrap();
    graft_components(&mut subdivision, &sites, &adjacency).unwrap();
    assert_boundary_walk_euler(&subdivision);
    let parts = assemble_cells(&subdivision).unwrap();
    for part in &parts {
        assert_eq!(part.shell.edges.len() + 1, part.shell.vertices.len());
        assert!(
            part.holes
                .iter()
                .all(|hole| hole.edges.len() + 1 == hole.vertices.len())
        );
    }
    let top_faces: std::collections::BTreeSet<_> = parts
        .iter()
        .filter(|part| part.owner == SiteId(1))
        .map(|part| part.face)
        .collect();
    assert_eq!(parts.len(), 4);
    assert_eq!(top_faces.len(), 2);
}

#[test]
fn isolated_component_exterior_inherits_the_containing_face() {
    let sites: Vec<_> = [(-10.0, -10.0), (0.0, 10.0), (10.0, -10.0)]
        .into_iter()
        .enumerate()
        .map(|(id, (x, y))| Site {
            id,
            point: Point::new_raw_xy(x, y),
        })
        .collect();
    let clip_polygon = Polygon::new(rectangle(-20.0, -20.0, 20.0, 20.0), vec![rectangle(
        -1.0, -1.0, 1.0, 1.0,
    )]);
    let atoms = node_polygon(&sites, &clip_polygon);
    let mut subdivision = build_exact_subdivision(atoms).unwrap();
    assert_boundary_walk_euler(&subdivision);
    let primal_edges = certified_delaunay(&sites).unwrap().primal_edges();
    let adjacency = certified_site_adjacency(&primal_edges, sites.len()).unwrap();
    graft_components(&mut subdivision, &sites, &adjacency).unwrap();
    assert_boundary_walk_euler(&subdivision);
    let inherited: Vec<_> = subdivision
        .labels
        .iter()
        .enumerate()
        .filter(|(index, label)| {
            selected_cell_label(**label)
                && exact::cycle_orientation(&walk_points(
                    &subdivision,
                    BoundaryFaceId(*index as u32),
                ))
                .unwrap()
                    == ExactSign::Negative
        })
        .map(|(_, label)| *label)
        .collect();
    assert_eq!(inherited.len(), 1);
    assert_eq!(inherited[0].frame_winding, 1);
    assert_eq!(inherited[0].clip_winding, 1);
    assert!(inherited[0].owner.is_some());
    let parts = assemble_cells(&subdivision).unwrap();
    assert_eq!(parts.iter().map(|part| part.holes.len()).sum::<usize>(), 1);
}

#[test]
fn two_isolated_holes_in_split_same_owner_cell_pair_by_containment() {
    let sites: Vec<_> = [(-10.0, -10.0), (0.0, 10.0), (10.0, -10.0)]
        .into_iter()
        .enumerate()
        .map(|(id, (x, y))| Site {
            id,
            point: Point::new_raw_xy(x, y),
        })
        .collect();
    let clip_polygon = Polygon::new(rectangle(-20.0, -20.0, 20.0, 20.0), vec![
        rectangle(-12.0, -1.0, 12.0, 1.0),
        rectangle(-8.0, 6.0, -6.0, 8.0),
        rectangle(-1.2, -1.7, -0.6, -1.2),
    ]);
    let subdivision = grafted_clip_subdivision(&sites, &clip_polygon);
    assert_boundary_walk_euler(&subdivision);
    assert!(
        first_equal_label_misattaches_a_hole(&subdivision),
        "first equal-label attachment must pick a non-containing shell on this split same-owner diagram"
    );
    let restored = assemble_cells(&subdivision).unwrap();
    let top: Vec<_> = restored
        .iter()
        .filter(|part| part.owner == SiteId(1))
        .collect();
    assert_eq!(top.len(), 2);
    assert_eq!(top.iter().map(|part| part.holes.len()).sum::<usize>(), 2);
    assert!(top.iter().all(|part| part.holes.len() == 1));
}

#[test]
fn validate_rejects_a_mutated_twin_walk_label() {
    let sites: Vec<_> = [(-10.0, -10.0), (0.0, 10.0), (10.0, -10.0)]
        .into_iter()
        .enumerate()
        .map(|(id, (x, y))| Site {
            id,
            point: Point::new_raw_xy(x, y),
        })
        .collect();
    let clip_polygon = Polygon::new(rectangle(-20.0, -20.0, 20.0, 20.0), vec![rectangle(
        -1.0, -1.0, 1.0, 1.0,
    )]);
    let mut subdivision = grafted_clip_subdivision(&sites, &clip_polygon);
    validate_exact_subdivision(&subdivision).unwrap();
    let slot = (0..subdivision.topology.targets.len())
        .find(|&slot| {
            let left = walk_label(
                &subdivision,
                BoundaryFaceId(subdivision.topology.face_of[slot]),
            );
            selected_cell_label(left)
        })
        .expect("selected walk has a slot");
    let face = subdivision.topology.face_of[slot] as usize;
    subdivision.labels[face] = RegionLabel {
        frame_winding: 0,
        clip_winding: 0,
        owner: None,
    };
    assert!(validate_exact_subdivision(&subdivision).is_err());
}

#[test]
fn collinear_holed_clip_splits_each_separator_into_two_pieces() {
    let sites: Vec<_> = [(-2.0, 0.0), (0.0, 0.0), (2.0, 0.0)]
        .into_iter()
        .enumerate()
        .map(|(id, (x, y))| Site {
            id,
            point: Point::new_raw_xy(x, y),
        })
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
            let first = edge.get(0).unwrap().xy();
            let last = edge.get(edge.len() - 1).unwrap().xy();
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
}

#[test]
fn frame_nearest_ignores_a_tie_that_is_not_the_minimum() {
    let sites: Vec<_> = [(-1.0, 0.0), (1.0, 0.0), (0.0, 0.0)]
        .into_iter()
        .enumerate()
        .map(|(id, (x, y))| Site {
            id,
            point: Point::new_raw_xy(x, y),
        })
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
        .map(|(id, (x, y))| Site {
            id,
            point: Point::new_raw_xy(x, y),
        })
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
        .map(|(id, (x, y))| Site {
            id,
            point: Point::new_raw_xy(x, y),
        })
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

#[test]
fn semantic_admission_mutants_change_validity_overlap_gap_and_spill() {
    let shape = |min_x, min_y, max_x, max_y| {
        Shape::Polygon(Polygon::new(
            rectangle(min_x, min_y, max_x, max_y),
            Vec::new(),
        ))
    };
    let clip = shape(0.0, 0.0, 4.0, 4.0);
    let restored_rows = vec![shape(0.0, 0.0, 2.0, 4.0), shape(2.0, 0.0, 4.0, 4.0)];
    let mutated_overlap = vec![clip.clone(), shape(0.0, 0.0, 2.0, 4.0)];
    let restored_coverage = coverage_is_valid(&restored_rows, 0.0).unwrap();
    let mutated_coverage = coverage_is_valid(&mutated_overlap, 0.0).unwrap();
    assert_ne!(
        mutated_coverage, restored_coverage,
        "mutated overlap={mutated_coverage}, restored partition={restored_coverage}"
    );
    assert!(!mutated_coverage && restored_coverage);

    let restored_union = Shape::union_all(&restored_rows, Strictness::Lenient).unwrap();
    let mutated_gap_union = Shape::union_all(
        &[shape(0.0, 0.0, 1.9, 4.0), shape(2.1, 0.0, 4.0, 4.0)],
        Strictness::Lenient,
    )
    .unwrap();
    let restored_gap = !clip
        .difference(&restored_union, Strictness::Lenient)
        .unwrap()
        .is_empty();
    let mutated_gap = !clip
        .difference(&mutated_gap_union, Strictness::Lenient)
        .unwrap()
        .is_empty();
    assert_ne!(
        mutated_gap, restored_gap,
        "mutated gap={mutated_gap}, restored gap={restored_gap}"
    );

    let mutated_spill_union = Shape::union_all(
        &[clip.clone(), shape(5.0, 0.0, 6.0, 1.0)],
        Strictness::Lenient,
    )
    .unwrap();
    let restored_spill = !restored_union
        .difference(&clip, Strictness::Lenient)
        .unwrap()
        .is_empty();
    let mutated_spill = !mutated_spill_union
        .difference(&clip, Strictness::Lenient)
        .unwrap()
        .is_empty();
    assert_ne!(
        mutated_spill, restored_spill,
        "mutated spill={mutated_spill}, restored spill={restored_spill}"
    );

    let restored_polygon = Polygon::new(rectangle(0.0, 0.0, 4.0, 4.0), Vec::new());
    let mutated_polygon = Polygon::new(
        Ring::closed(
            [(0.0, 0.0), (4.0, 4.0), (0.0, 4.0), (4.0, 0.0), (0.0, 0.0)]
                .into_iter()
                .map(|(x, y)| Point::new_raw_xy(x, y))
                .collect(),
        )
        .unwrap(),
        Vec::new(),
    );
    let restored_valid = polygon_is_valid(&restored_polygon).unwrap();
    let mutated_valid = polygon_is_valid(&mutated_polygon).unwrap();
    assert_ne!(
        mutated_valid, restored_valid,
        "mutated valid={mutated_valid}, restored valid={restored_valid}"
    );
}

#[test]
fn transition_and_budget_mutants_report_distinct_restored_values() {
    let left = RegionLabel {
        frame_winding: 1,
        clip_winding: 1,
        owner: Some(SiteId(0)),
    };
    let transition = Transition {
        frame_delta: 0,
        clip_delta: 1,
        owner: OwnerTransition::Boundary {
            left: Some(SiteId(0)),
            right: Some(SiteId(1)),
        },
    };
    let restored = crossed_label(left, transition).unwrap();
    let mutated = crossed_label(left, Transition {
        clip_delta: 0,
        owner: OwnerTransition::Preserve,
        ..transition
    })
    .unwrap();
    assert_ne!(
        mutated, restored,
        "mutated transition={mutated:?}, restored transition={restored:?}"
    );
    assert_eq!(restored.clip_winding, 0);
    assert_eq!(restored.owner, Some(SiteId(1)));

    let inherited = left;
    let zero_seeded = RegionLabel {
        frame_winding: 0,
        clip_winding: 0,
        owner: None,
    };
    assert_ne!(
        zero_seeded, inherited,
        "mutated inherited label={zero_seeded:?}, restored inherited label={inherited:?}"
    );

    let exact = ExpansionBudget::check("test", "work", GENERATED_ITEM_LIMIT).unwrap();
    let plus_one_rejected =
        ExpansionBudget::check("test", "work", GENERATED_ITEM_LIMIT + 1).is_err();
    assert_ne!(
        plus_one_rejected,
        exact != GENERATED_ITEM_LIMIT,
        "limit+1 rejected={plus_one_rejected}, exact admitted={exact}"
    );
    let finite_product = ExpansionBudget::product("test", "work", 4, 4).unwrap();
    let overflow_rejected = ExpansionBudget::product("test", "work", usize::MAX, 2).is_err();
    assert_ne!(
        overflow_rejected,
        finite_product != 16,
        "overflow rejected={overflow_rejected}, restored product={finite_product}"
    );
}

#[test]
fn dual_completeness_rejects_a_missing_cell_mutant() {
    let sites: Vec<_> = [(0.0, 0.0), (2.0, 0.0), (0.0, 2.0), (2.0, 2.0), (0.8, 1.1)]
        .into_iter()
        .enumerate()
        .map(|(id, (x, y))| Site {
            id,
            point: Point::new_raw_xy(x, y),
        })
        .collect();
    let clip = normalize_clip(&sites, VoronoiBoundary::Envelope, false).unwrap();
    let mesh = certified_delaunay(&sites).unwrap();
    let complex = delaunay_complex(&mesh, &sites).unwrap();
    let restored = dual_build_rings(&sites, &mesh, complex, &clip).unwrap();
    let mut mutated = restored.clone();
    mutated.rings.pop();

    let mut restored_budget = ExpansionBudget::new("test", "restored dual completeness");
    let restored_diagram = dual_embed(
        &restored,
        &clip,
        &sites,
        VoronoiProduct::Both,
        &mut restored_budget,
    )
    .unwrap();
    let restored_admitted = true;
    let mut missing_rows: Vec<_> = restored_diagram
        .polygons
        .iter()
        .cloned()
        .map(Shape::Polygon)
        .collect();
    missing_rows.pop();
    assert!(
        coverage_is_valid(&missing_rows, 0.0).unwrap(),
        "coverage validity deliberately does not certify clip completeness"
    );
    let mut mutated_budget = ExpansionBudget::new("test", "mutated dual completeness");
    let mutated_admitted = dual_embed(
        &mutated,
        &clip,
        &sites,
        VoronoiProduct::Both,
        &mut mutated_budget,
    )
    .is_ok();
    assert_ne!(
        mutated_admitted, restored_admitted,
        "mutated missing-cell admitted={mutated_admitted}, restored admitted={restored_admitted}"
    );
    assert!(!mutated_admitted && restored_admitted);
}

fn shared_dual_vertex(rings: &DualRings) -> (usize, usize, exact::ExactPoint) {
    for (ring_index, ring) in rings.rings.iter().enumerate() {
        for (vertex_index, vertex) in ring.iter().enumerate() {
            let point = dual_vertex_point(vertex, &rings.centers);
            if rings.rings.iter().enumerate().any(|(other_index, other)| {
                other_index != ring_index
                    && other.iter().any(|other_vertex| {
                        point
                            .same_position(dual_vertex_point(other_vertex, &rings.centers))
                            .unwrap()
                    })
            }) {
                return (ring_index, vertex_index, point.clone());
            }
        }
    }
    panic!("dual mutation fixture has no shared vertex");
}

fn shared_boundary_vertex(
    rings: &DualRings,
    bounds: [f64; 4],
) -> (usize, usize, exact::ExactPoint, usize) {
    for (ring_index, ring) in rings.rings.iter().enumerate() {
        for (vertex_index, vertex) in ring.iter().enumerate() {
            let point = dual_vertex_point(vertex, &rings.centers);
            let rounded = point.round_nearest_even().unwrap();
            let Some(side) = rounded_boundary_side(rounded, bounds) else {
                continue;
            };
            if rings.rings.iter().enumerate().any(|(other_index, other)| {
                other_index != ring_index
                    && other.iter().any(|other_vertex| {
                        point
                            .same_position(dual_vertex_point(other_vertex, &rings.centers))
                            .unwrap()
                    })
            }) {
                return (ring_index, vertex_index, point.clone(), side);
            }
        }
    }
    panic!("dual mutation fixture has no shared boundary vertex");
}

fn rounded_duplicate(point: &exact::ExactPoint) -> exact::ExactPoint {
    let rounded = point.round_nearest_even().unwrap();
    let base = exact::ExactPoint::from_xy(rounded);
    let upper = exact::ExactPoint::from_xy(XY::new(rounded.x.next_up(), rounded.y));
    base.midpoint(&upper).unwrap().midpoint(&base).unwrap()
}

fn rounded_boundary_side(point: XY, bounds: [f64; 4]) -> Option<usize> {
    if point.y == bounds[2] {
        Some(0)
    } else if point.x == bounds[1] {
        Some(1)
    } else if point.y == bounds[3] {
        Some(2)
    } else if point.x == bounds[0] {
        Some(3)
    } else {
        None
    }
}

#[test]
fn dual_local_validator_rejects_deterministic_partition_mutations() {
    let sites: Vec<_> = [
        (0.0, 0.0),
        (1.0, 0.0),
        (0.0, 1.0),
        (1.0, 1.0),
        (0.5, 0.4),
        (0.2, 0.7),
    ]
    .into_iter()
    .enumerate()
    .map(|(id, (x, y))| Site {
        id,
        point: Point::new_raw_xy(x, y),
    })
    .collect();
    let clip = normalize_clip(&sites, VoronoiBoundary::Envelope, false).unwrap();
    let mesh = certified_delaunay(&sites).unwrap();
    let complex = delaunay_complex(&mesh, &sites).unwrap();
    let restored = dual_build_rings(&sites, &mesh, complex, &clip).unwrap();
    let mut restored_budget = ExpansionBudget::new("test", "restored dual mutation fixture");
    dual_embed(
        &restored,
        &clip,
        &sites,
        VoronoiProduct::Both,
        &mut restored_budget,
    )
    .unwrap();

    let mut mutations = Vec::new();

    let mut missing_cell = restored.clone();
    missing_cell.rings.pop();
    mutations.push(("missing cell", missing_cell));

    let mut reversed_ring = restored.clone();
    reversed_ring.rings[0].reverse();
    mutations.push(("reversed ring", reversed_ring));

    let (shared_ring, shared_vertex, shared_point) = shared_dual_vertex(&restored);
    let mut moved_shared_vertex = restored.clone();
    let moved = shared_point.round_nearest_even().unwrap();
    moved_shared_vertex.rings[shared_ring][shared_vertex] = DualVertex::Generated(Box::new(
        exact::ExactPoint::from_xy(XY::new(moved.x + 0.125, moved.y)),
    ));
    mutations.push(("moved shared vertex", moved_shared_vertex));

    let mut duplicate_key = restored.clone();
    let duplicate = rounded_duplicate(&shared_point);
    assert_eq!(
        PointKey::new(duplicate.round_nearest_even().unwrap()),
        PointKey::new(shared_point.round_nearest_even().unwrap())
    );
    assert!(!duplicate.same_position(&shared_point).unwrap());
    duplicate_key.rings[shared_ring][shared_vertex] = DualVertex::Generated(Box::new(duplicate));
    mutations.push(("duplicate rounded key", duplicate_key));

    let bounds = clip.frame_bounds();
    let mut wrong_boundary_direction = restored.clone();
    let mut swapped_boundary = false;
    'find_boundary: for ring in &mut wrong_boundary_direction.rings {
        for index in 0..ring.len() {
            let next = (index + 1) % ring.len();
            let left = dual_vertex_point(&ring[index], &restored.centers)
                .round_nearest_even()
                .unwrap();
            let right = dual_vertex_point(&ring[next], &restored.centers)
                .round_nearest_even()
                .unwrap();
            if rounded_boundary_side(left, bounds).is_some()
                && rounded_boundary_side(left, bounds) == rounded_boundary_side(right, bounds)
            {
                ring.swap(index, next);
                swapped_boundary = true;
                break 'find_boundary;
            }
        }
    }
    assert!(
        swapped_boundary,
        "dual mutation fixture has no boundary edge"
    );
    mutations.push(("wrong boundary direction", wrong_boundary_direction));

    let (boundary_ring, boundary_vertex, boundary_point, boundary_side) =
        shared_boundary_vertex(&restored, bounds);
    let boundary_rounded = boundary_point.round_nearest_even().unwrap();
    for (name, direction) in [("gap", 1.0), ("overlap", -1.0)] {
        let mut mutation = restored.clone();
        let shifted = if boundary_side % 2 == 0 {
            XY::new(boundary_rounded.x + direction * 0.125, boundary_rounded.y)
        } else {
            XY::new(boundary_rounded.x, boundary_rounded.y + direction * 0.125)
        };
        mutation.rings[boundary_ring][boundary_vertex] =
            DualVertex::Generated(Box::new(exact::ExactPoint::from_xy(shifted)));
        mutations.push((name, mutation));
    }

    let mut crossed_ring = restored.clone();
    let crossed_index = crossed_ring
        .rings
        .iter()
        .position(|ring| ring.len() >= 5)
        .expect("dual mutation fixture has no crossable ring");
    crossed_ring.rings[crossed_index].swap(1, 2);
    mutations.push(("crossed ring", crossed_ring));

    for (name, mutation) in mutations {
        let mut budget = ExpansionBudget::new("test", "mutated dual partition");
        assert!(
            dual_embed(&mutation, &clip, &sites, VoronoiProduct::Both, &mut budget,).is_err(),
            "dual mutation unexpectedly admitted: {name}"
        );
    }
}
