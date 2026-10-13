use super::super::keys::find_chart_port;
use super::super::{
    ExactChartPoint, ExactQuotientKey, SourceEdgeId, compare_quotient_keys, quotient_key,
    source_chart_point,
};
use super::support::{atom_touches_node, chart_step_identity_holds, degrees, inventory, line};
use crate::geographic::{AngularFactor, GeographicFrame};
use crate::geometry::{ExpansionBudget, Point, Shape, XY};

#[test]
fn chart_transit_is_the_integer_quotient_of_the_exact_chart_step() {
    let radians = GeographicFrame::try_new(
        AngularFactor::try_new("radians", 1.0).unwrap(),
        AngularFactor::try_new("radians", 1.0).unwrap(),
        0.0,
    )
    .unwrap();
    let grad_unit = AngularFactor::try_new("grads", std::f64::consts::TAU / 400.0).unwrap();
    let grads = GeographicFrame::try_new(grad_unit, grad_unit, 0.0).unwrap();
    let tau = std::f64::consts::TAU;
    let pi = std::f64::consts::PI;
    let samples: [(&str, GeographicFrame, Vec<f64>); 3] = [
        ("degrees", degrees(), vec![
            -720.0, -540.0, -360.0, -300.0, -180.0, -90.0, 0.0, 90.0, 180.0, 270.0, 360.0, 540.0,
            720.0, 179.5, 359.5, -179.5, 0.25,
        ]),
        ("radians", radians, vec![
            -2.0 * tau,
            -tau,
            -pi,
            -pi / 2.0,
            0.0,
            pi / 2.0,
            pi,
            tau,
            2.0 * tau,
            1.0,
        ]),
        ("grads", grads, vec![
            -400.0, -200.0, -100.0, 0.0, 50.0, 100.0, 200.0, 300.0, 400.0, 600.0,
        ]),
    ];
    let mut checked = 0_usize;
    let mut failures = Vec::new();
    for (name, frame, values) in &samples {
        for &from in values {
            for &to in values {
                if !chart_step_identity_holds(*frame, from, to) {
                    failures.push(format!("{name}: {from} -> {to}"));
                }
                checked += 1;
            }
        }
    }
    assert!(checked > 400);
    // Grads uses a rounded factor, so its nominal 200-grad tie differs
    // from the frame's exact derived half-period.
    assert!(failures.is_empty(), "{failures:?}");
}

/// A rounded raw difference of one period is not a circle unless the
/// represented source endpoints differ by exactly that period.
#[test]
fn source_points_share_exact_seam_and_pole_nodes() {
    let source = Shape::geometry_collection(vec![
        Shape::Point(Point::new_raw_xy(-180.0, 5.0)),
        Shape::Point(Point::new_raw_xy(180.0, 5.0)),
        Shape::Point(Point::new_raw_xy(-30.0, 90.0)),
        Shape::Point(Point::new_raw_xy(120.0, 90.0)),
        Shape::Point(Point::new_raw_xy(-180.0, -90.0)),
        Shape::Point(Point::new_raw_xy(47.0, -90.0)),
        Shape::Point(Point::new_raw_xy(-180.0, 6.0)),
    ])
    .unwrap();
    let sources = inventory(degrees(), &[source]);
    let mut budget = ExpansionBudget::new("test", "exact chart");
    let chart = sources.node(&mut budget).unwrap();
    let point_node = |index: usize| {
        let port = chart
            .source_point_ports
            .get(index)
            .expect("source point port exists");
        chart
            .ports
            .get(port.0 as usize)
            .expect("source point port is admitted")
            .node
    };

    assert_eq!(chart.source_point_ports.len(), 7);
    assert_eq!(point_node(0), point_node(1));
    assert_eq!(point_node(2), point_node(3));
    assert_eq!(point_node(4), point_node(5));
    assert_ne!(point_node(2), point_node(4));
    assert_ne!(point_node(0), point_node(2));
    assert_ne!(point_node(0), point_node(6));
}

#[test]
fn stored_vertex_identity_matches_exact_geographic_quotient() {
    let degrees = degrees();
    let degree_unit = AngularFactor::try_new("test degree", std::f64::consts::PI / 180.0)
        .expect("degree unit is valid");
    let grads_unit = AngularFactor::try_new("test grad", std::f64::consts::PI / 200.0)
        .expect("grad unit is valid");
    let grads = GeographicFrame::try_new(grads_unit, grads_unit, 0.0).unwrap();
    let prime_meridian = GeographicFrame::try_new(degree_unit, degree_unit, 0.1).unwrap();

    for (frame, points) in [
        (degrees, vec![
            XY::new(0.0, 0.0),
            XY::new(360.0, -0.0),
            XY::new(-0.0, 0.0),
            XY::new(180.0, 15.0),
            XY::new(-180.0, 15.0),
            XY::new(180.0, -15.0),
            XY::new(-180.0, -15.0),
            XY::new(0.0, 90.0),
            XY::new(123.0, 90.0),
            XY::new(-180.0, -90.0),
            XY::new(47.0, -90.0),
            XY::new(180.0_f64.next_down(), 15.0),
        ]),
        (grads, vec![
            XY::new(0.0, 0.0),
            XY::new(400.0, -0.0),
            XY::new(200.0, 25.0),
            XY::new(-200.0, 25.0),
            XY::new(200.0, -25.0),
            XY::new(-200.0, -25.0),
            XY::new(0.0, 100.0),
            XY::new(137.0, 100.0),
            XY::new(0.0, -100.0),
            XY::new(-91.0, -100.0),
        ]),
        (prime_meridian, vec![
            XY::new(0.0, 0.0),
            XY::new(360.0, -0.0),
            XY::new(180.0, 12.0),
            XY::new(-180.0, 12.0),
            XY::new(0.0, 90.0),
            XY::new(91.0, 90.0),
        ]),
    ] {
        let exact = points
            .iter()
            .copied()
            .map(|point| {
                quotient_key(
                    frame,
                    &source_chart_point(frame, Point::new_raw_xy(point.x, point.y)),
                )
                .expect("stored coordinates form exact quotient keys")
            })
            .collect::<Vec<_>>();
        let stored = points
            .iter()
            .copied()
            .map(|point| {
                crate::geometry::VertexKey::new(
                    Point::new_raw_xy(point.x, point.y),
                    crate::geometry::TopologyDomain::Geographic(frame),
                )
            })
            .collect::<Vec<_>>();

        for ((left_point, left_exact), left_stored) in points.iter().zip(&exact).zip(&stored) {
            for ((right_point, right_exact), right_stored) in points.iter().zip(&exact).zip(&stored)
            {
                assert_eq!(
                    left_stored == right_stored,
                    compare_quotient_keys(left_exact, right_exact)
                        .expect("exact quotient keys compare")
                        .is_eq(),
                    "frame={frame:?}, left={left_point:?}, right={right_point:?}",
                );
            }
        }
    }
}

#[test]
fn distinct_chart_ports_at_a_pole_share_one_quotient_node() {
    let shapes = [
        line(&[(0.0, 80.0), (0.0, 90.0)]),
        line(&[(120.0, 90.0), (120.0, 80.0)]),
    ];
    let inventory = inventory(degrees(), &shapes);
    let mut budget = ExpansionBudget::new("test", "exact chart");
    let chart = inventory.node(&mut budget).unwrap();
    let north_pole = chart
        .nodes
        .iter()
        .find(|node| matches!(node.key, ExactQuotientKey::NorthPole))
        .expect("the source edges reach the north pole");
    let first_port =
        find_chart_port(&chart.ports, &ExactChartPoint::from_xy(XY::new(0.0, 90.0))).unwrap();
    let second_port = find_chart_port(
        &chart.ports,
        &ExactChartPoint::from_xy(XY::new(120.0, 90.0)),
    )
    .unwrap();
    assert_ne!(first_port, second_port);
    assert_eq!(chart.ports[first_port.0 as usize].node, north_pole.id);
    assert_eq!(chart.ports[second_port.0 as usize].node, north_pole.id);
    let incident_sources = chart
        .atoms
        .iter()
        .filter(|atom| atom_touches_node(&chart, atom, north_pole.id))
        .flat_map(|atom| atom.owners.iter().map(|owner| owner.source_edge))
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(
        incident_sources,
        [SourceEdgeId(0), SourceEdgeId(1)].into_iter().collect()
    );
}
