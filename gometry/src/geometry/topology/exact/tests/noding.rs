use std::cmp::Ordering;

use super::super::keys::find_chart_port;
use super::super::{ExactChartPoint, SourceEdgeId};
use super::support::{
    atom_touches_node, degrees, inventory, line, ring, seam_node, source_uses_node,
};
use crate::geographic::{AngularFactor, GeographicAxisOrientation, GeographicFrame};
use crate::geometry::tessellation::exact::{ExactDyadic, ExactRational};
use crate::geometry::{ExpansionBudget, Polygon, Shape, XY};

#[test]
fn seam_contacts_keep_exact_parameters_and_distinct_quotient_nodes() {
    let delta = f64::from_bits(1);
    let shapes = [
        line(&[(170.0, 0.0), (-160.0, delta)]),
        line(&[(150.0, 0.0), (-90.0, delta)]),
        line(&[(180.0, 0.0), (180.0, delta)]),
    ];
    let inventory = inventory(degrees(), &shapes);
    assert_eq!(inventory.pieces.len(), 5);
    let third =
        ExactRational::new(ExactDyadic::from_f64(delta), ExactDyadic::from_f64(3.0)).unwrap();
    let quarter =
        ExactRational::new(ExactDyadic::from_f64(delta), ExactDyadic::from_f64(4.0)).unwrap();
    let source_third = ExactRational::new(ExactDyadic::one(), ExactDyadic::from_f64(3.0)).unwrap();
    let source_quarter =
        ExactRational::new(ExactDyadic::one(), ExactDyadic::from_f64(4.0)).unwrap();
    let first_source = inventory.edges[0].id;
    let second_source = inventory.edges[1].id;
    assert_eq!(
        inventory.pieces[0]
            .source_range
            .end
            .try_cmp(&source_third)
            .unwrap(),
        Ordering::Equal
    );
    assert_eq!(
        inventory.pieces[2]
            .source_range
            .end
            .try_cmp(&source_quarter)
            .unwrap(),
        Ordering::Equal
    );
    assert_eq!(
        inventory.pieces[0].end.y.try_cmp(&third).unwrap(),
        Ordering::Equal
    );
    assert_eq!(
        inventory.pieces[2].end.y.try_cmp(&quarter).unwrap(),
        Ordering::Equal
    );

    let mut budget = ExpansionBudget::new("test", "exact chart");
    let chart = inventory.node(&mut budget).unwrap();
    let third_node = seam_node(&chart, &third);
    let quarter_node = seam_node(&chart, &quarter);
    let zero_node = seam_node(&chart, &ExactRational::zero());
    assert_ne!(third_node, quarter_node);
    assert_ne!(third_node, zero_node);
    assert_ne!(quarter_node, zero_node);
    assert!(source_uses_node(&chart, first_source, third_node));
    assert!(!source_uses_node(&chart, first_source, zero_node));
    assert!(source_uses_node(&chart, second_source, quarter_node));
    assert!(!source_uses_node(&chart, second_source, zero_node));
    let crossing = ExactChartPoint::from_rationals(
        ExactRational::new(ExactDyadic::from_f64(530.0), ExactDyadic::from_f64(3.0)).unwrap(),
        ExactRational::new(
            ExactDyadic::from_f64(delta)
                .try_product(&ExactDyadic::from_f64(2.0))
                .unwrap(),
            ExactDyadic::from_f64(9.0),
        )
        .unwrap(),
    );
    let crossing_port = find_chart_port(&chart.ports, &crossing).unwrap();
    let crossing_node = chart.ports[crossing_port.0 as usize].node;
    assert!(source_uses_node(&chart, first_source, crossing_node));
    assert!(source_uses_node(&chart, second_source, crossing_node));
    assert_eq!(budget.used(), 7);
}

#[test]
fn opposite_seam_spans_node_overlaps_and_keep_each_source_parameter() {
    let shapes = [
        line(&[(180.0, 0.0), (180.0, 10.0)]),
        line(&[(-180.0, 5.0), (-180.0, 15.0)]),
    ];
    let inventory = inventory(degrees(), &shapes);
    let mut budget = ExpansionBudget::new("test", "exact chart");
    let chart = inventory.node(&mut budget).unwrap();
    let five = ExactRational::from_f64(5.0);
    let ten = ExactRational::from_f64(10.0);
    let five_node = seam_node(&chart, &five);
    let ten_node = seam_node(&chart, &ten);
    assert!(source_uses_node(&chart, SourceEdgeId(0), five_node));
    assert!(source_uses_node(&chart, SourceEdgeId(0), ten_node));
    assert!(source_uses_node(&chart, SourceEdgeId(1), five_node));
    assert!(source_uses_node(&chart, SourceEdgeId(1), ten_node));

    for source in [SourceEdgeId(0), SourceEdgeId(1)] {
        let mut ranges = chart
            .atoms
            .iter()
            .flat_map(|atom| atom.owners.iter())
            .filter(|owner| owner.source_edge == source)
            .map(|owner| owner.source_range.clone())
            .collect::<Vec<_>>();
        ranges.sort_unstable_by(|left, right| {
            left.start
                .try_cmp(&right.start)
                .expect("source parameters compare")
        });
        assert_eq!(ranges.len(), 2);
        assert!(
            ranges[0]
                .start
                .try_cmp(&ExactRational::zero())
                .unwrap()
                .is_eq()
        );
        assert!(
            ranges[0]
                .end
                .try_cmp(&ExactRational::from_f64(0.5))
                .unwrap()
                .is_eq()
        );
        assert!(
            ranges[1]
                .start
                .try_cmp(&ExactRational::from_f64(0.5))
                .unwrap()
                .is_eq()
        );
        assert!(
            ranges[1]
                .end
                .try_cmp(&ExactRational::one())
                .unwrap()
                .is_eq()
        );
    }
    assert_eq!(budget.used(), 2);
}

#[test]
fn proper_multiway_crossings_share_one_exact_node() {
    let shapes = [
        line(&[(0.0, 0.0), (4.0, 4.0)]),
        line(&[(0.0, 4.0), (4.0, 0.0)]),
        line(&[(2.0, -1.0), (2.0, 5.0)]),
    ];
    let inventory = inventory(degrees(), &shapes);
    let mut budget = ExpansionBudget::new("test", "exact chart");
    let chart = inventory.node(&mut budget).unwrap();
    let center = ExactChartPoint::from_xy(XY::new(2.0, 2.0));
    let center_port = find_chart_port(&chart.ports, &center).unwrap();
    let center_node = chart.ports[center_port.0 as usize].node;
    let incident_sources = chart
        .atoms
        .iter()
        .filter(|atom| atom_touches_node(&chart, atom, center_node))
        .flat_map(|atom| atom.owners.iter().map(|owner| owner.source_edge))
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(incident_sources.len(), 3);
    assert_eq!(budget.used(), 4);
}

#[test]
fn crossing_lines_keep_each_exact_half_source_range() {
    let shapes = [
        line(&[(0.0, 0.0), (4.0, 4.0)]),
        line(&[(0.0, 4.0), (4.0, 0.0)]),
    ];
    let inventory = inventory(degrees(), &shapes);
    let mut budget = ExpansionBudget::new("test", "exact chart");
    let chart = inventory.node(&mut budget).unwrap();
    let half = ExactRational::from_f64(0.5);

    assert_eq!(chart.atoms.len(), 4);
    assert!(chart.atoms.iter().all(|atom| atom.owners.len() == 1));
    for source in [SourceEdgeId(0), SourceEdgeId(1)] {
        let mut ranges = chart
            .atoms
            .iter()
            .flat_map(|atom| atom.owners.iter())
            .filter(|owner| owner.source_edge == source)
            .map(|owner| owner.source_range.clone())
            .collect::<Vec<_>>();
        ranges.sort_unstable_by(|left, right| {
            left.start
                .try_cmp(&right.start)
                .expect("source parameters compare")
        });
        assert_eq!(ranges.len(), 2);
        assert_eq!(
            ranges[0].start.try_cmp(&ExactRational::zero()).unwrap(),
            Ordering::Equal
        );
        assert_eq!(ranges[0].end.try_cmp(&half).unwrap(), Ordering::Equal);
        assert_eq!(ranges[1].start.try_cmp(&half).unwrap(), Ordering::Equal);
        assert_eq!(
            ranges[1].end.try_cmp(&ExactRational::one()).unwrap(),
            Ordering::Equal
        );
    }
}

#[test]
fn seam_split_ring_edges_keep_exact_parameters_and_quotient_owners() {
    let shape = Shape::Polygon(Polygon::new(
        ring(&[
            (170.0, 0.0),
            (-170.0, 0.0),
            (-170.0, 10.0),
            (170.0, 10.0),
            (170.0, 0.0),
        ]),
        vec![],
    ));
    let inventory = inventory(degrees(), &[shape]);
    let mut budget = ExpansionBudget::new("test", "exact chart");
    let chart = inventory.node(&mut budget).unwrap();
    let half = ExactRational::from_f64(0.5);

    for source in [SourceEdgeId(0), SourceEdgeId(2)] {
        let mut ranges = chart
            .atoms
            .iter()
            .flat_map(|atom| atom.owners.iter())
            .filter(|owner| owner.source_edge == source)
            .map(|owner| owner.source_range.clone())
            .collect::<Vec<_>>();
        ranges.sort_unstable_by(|left, right| {
            left.start
                .try_cmp(&right.start)
                .expect("source parameters compare")
        });
        assert_eq!(ranges.len(), 2);
        assert_eq!(
            ranges[0].start.try_cmp(&ExactRational::zero()).unwrap(),
            Ordering::Equal
        );
        assert_eq!(ranges[0].end.try_cmp(&half).unwrap(), Ordering::Equal);
        assert_eq!(ranges[1].start.try_cmp(&half).unwrap(), Ordering::Equal);
        assert_eq!(
            ranges[1].end.try_cmp(&ExactRational::one()).unwrap(),
            Ordering::Equal
        );
    }

    for (latitude, source) in [
        (ExactRational::zero(), SourceEdgeId(0)),
        (ExactRational::from_f64(10.0), SourceEdgeId(2)),
    ] {
        let node = seam_node(&chart, &latitude);
        assert!(source_uses_node(&chart, source, node));
    }
}

#[test]
fn reversed_native_axis_edges_keep_exact_seam_parameters() {
    let frame = GeographicFrame::try_new_with_orientation(
        AngularFactor::try_new("west longitude unit", std::f64::consts::TAU / 8.0).unwrap(),
        AngularFactor::try_new("south latitude radian", 1.0).unwrap(),
        0.0,
        GeographicAxisOrientation::from_axis_directions(false, false),
    )
    .unwrap();
    let sources = inventory(frame, &[
        line(&[(-3.75, 0.0), (3.25, -1.0)]),
        line(&[(3.25, -1.0), (-3.75, 0.0)]),
    ]);
    let quarter = ExactRational::from_f64(0.25);
    let three_quarters = ExactRational::from_f64(0.75);
    let seam_latitude = ExactRational::from_f64(-0.25);

    for (source, parameter) in [
        (SourceEdgeId(0), &quarter),
        (SourceEdgeId(1), &three_quarters),
    ] {
        let pieces = sources
            .pieces
            .iter()
            .filter(|piece| piece.source_edge == source)
            .collect::<Vec<_>>();
        assert_eq!(pieces.len(), 2);
        let crossing = pieces
            .iter()
            .flat_map(|piece| [&piece.start, &piece.end])
            .find(|point| {
                point
                    .x
                    .try_cmp(&ExactRational::from_f64(-4.0))
                    .unwrap()
                    .is_eq()
                    || point
                        .x
                        .try_cmp(&ExactRational::from_f64(4.0))
                        .unwrap()
                        .is_eq()
            })
            .expect("source edge crosses the native longitude seam");
        assert_eq!(crossing.y.try_cmp(&seam_latitude).unwrap(), Ordering::Equal);
        assert!(
            pieces
                .iter()
                .any(|piece| { piece.source_range.end.try_cmp(parameter).unwrap().is_eq() })
                || pieces
                    .iter()
                    .any(|piece| { piece.source_range.start.try_cmp(parameter).unwrap().is_eq() })
        );
    }
    let mut budget = ExpansionBudget::new("test", "exact chart");
    let chart = sources.node(&mut budget).unwrap();
    let seam = seam_node(&chart, &seam_latitude);
    assert!(source_uses_node(&chart, SourceEdgeId(0), seam));
    assert!(source_uses_node(&chart, SourceEdgeId(1), seam));
}

#[test]
fn one_ulp_source_endpoint_is_shared_by_adjacent_exact_edges() {
    let endpoint = (-170.0_f64).next_down();
    let shape = Shape::Polygon(Polygon::new(
        ring(&[
            (170.0, 0.0),
            (endpoint, 0.0),
            (endpoint, 10.0),
            (170.0, 10.0),
            (170.0, 0.0),
        ]),
        vec![],
    ));
    let inventory = inventory(degrees(), &[shape]);
    let mut budget = ExpansionBudget::new("test", "exact chart");
    let chart = inventory.node(&mut budget).unwrap();
    let first_edge = chart
        .source_edge_ports
        .first()
        .expect("first ring edge exists");
    let second_edge = chart
        .source_edge_ports
        .get(1)
        .expect("adjacent ring edge exists");
    let first_endpoint = chart
        .ports
        .get(first_edge[1].0 as usize)
        .expect("source endpoint port exists");
    let second_start = chart
        .ports
        .get(second_edge[0].0 as usize)
        .expect("adjacent source port exists");

    assert_eq!(first_endpoint.node, second_start.node);
    assert_eq!(
        first_endpoint
            .point
            .x
            .try_cmp(&ExactRational::from_f64(endpoint))
            .unwrap(),
        Ordering::Equal,
    );
    assert_eq!(
        first_endpoint
            .point
            .y
            .try_cmp(&ExactRational::zero())
            .unwrap(),
        Ordering::Equal,
    );
}

#[test]
fn collinear_overlap_atoms_retain_each_source_parameter_range() {
    let shapes = [
        line(&[(0.0, 0.0), (4.0, 0.0)]),
        line(&[(2.0, 0.0), (6.0, 0.0)]),
    ];
    let inventory = inventory(degrees(), &shapes);
    let mut budget = ExpansionBudget::new("test", "exact chart");
    let chart = inventory.node(&mut budget).unwrap();
    let overlaps = chart
        .atoms
        .iter()
        .filter(|atom| atom.owners.len() == 2)
        .collect::<Vec<_>>();
    assert_eq!(overlaps.len(), 1);
    let mut owners = overlaps[0].owners.iter().collect::<Vec<_>>();
    owners.sort_unstable_by_key(|owner| owner.source_edge);
    let half = ExactRational::from_f64(0.5);
    assert_eq!(owners[0].source_edge, SourceEdgeId(0));
    assert_eq!(
        owners[0].source_range.start.try_cmp(&half).unwrap(),
        Ordering::Equal
    );
    assert_eq!(
        owners[0]
            .source_range
            .end
            .try_cmp(&ExactRational::one())
            .unwrap(),
        Ordering::Equal
    );
    assert_eq!(owners[1].source_edge, SourceEdgeId(1));
    assert_eq!(
        owners[1]
            .source_range
            .start
            .try_cmp(&ExactRational::zero())
            .unwrap(),
        Ordering::Equal
    );
    assert_eq!(
        owners[1].source_range.end.try_cmp(&half).unwrap(),
        Ordering::Equal
    );
    assert_eq!(chart.atoms.len(), 3);
}

#[test]
fn endpoint_contact_splits_the_interior_span_and_keeps_both_owners() {
    let shapes = [
        line(&[(0.0, 0.0), (4.0, 0.0)]),
        line(&[(2.0, -1.0), (2.0, 0.0)]),
    ];
    let inventory = inventory(degrees(), &shapes);
    let mut budget = ExpansionBudget::new("test", "exact chart");
    let chart = inventory.node(&mut budget).unwrap();
    let contact = ExactChartPoint::from_xy(XY::new(2.0, 0.0));
    let contact_port = find_chart_port(&chart.ports, &contact).unwrap();
    let contact_node = chart.ports[contact_port.0 as usize].node;
    let mut ranges = chart
        .atoms
        .iter()
        .filter(|atom| atom_touches_node(&chart, atom, contact_node))
        .flat_map(|atom| {
            atom.owners
                .iter()
                .map(|owner| (owner.source_edge, owner.source_range.clone()))
        })
        .collect::<Vec<_>>();
    ranges.sort_unstable_by_key(|(source, _)| *source);
    assert_eq!(ranges.len(), 3);
    assert_eq!(ranges[0].0, SourceEdgeId(0));
    assert!(
        ranges[0]
            .1
            .start
            .try_cmp(&ExactRational::zero())
            .unwrap()
            .is_eq()
    );
    assert!(
        ranges[0]
            .1
            .end
            .try_cmp(&ExactRational::from_f64(0.5))
            .unwrap()
            .is_eq()
    );
    assert_eq!(ranges[1].0, SourceEdgeId(0));
    assert!(
        ranges[1]
            .1
            .start
            .try_cmp(&ExactRational::from_f64(0.5))
            .unwrap()
            .is_eq()
    );
    assert!(
        ranges[1]
            .1
            .end
            .try_cmp(&ExactRational::one())
            .unwrap()
            .is_eq()
    );
    assert_eq!(ranges[2].0, SourceEdgeId(1));
    assert!(
        ranges[2]
            .1
            .start
            .try_cmp(&ExactRational::zero())
            .unwrap()
            .is_eq()
    );
    assert!(
        ranges[2]
            .1
            .end
            .try_cmp(&ExactRational::one())
            .unwrap()
            .is_eq()
    );
    assert_eq!(budget.used(), 1);
}
