use super::super::{
    ExactChartAtom, ExactGeographicChartInventory, ExactQuotientKey, NodedExactChart,
    QuotientNodeId, SourceEdgeId, local_longitude_edge,
};
use crate::geographic::{AngularFactor, GeographicFrame, GeographicInputs};
use crate::geometry::tessellation::exact::{ExactDyadic, ExactRational};
use crate::geometry::{CoordSeq, LineSeq, Point, Ring, Shape};

pub(super) fn degrees() -> GeographicFrame {
    let unit = AngularFactor::try_new("test degrees", std::f64::consts::PI / 180.0)
        .expect("degrees are a valid angular factor");
    GeographicFrame::try_new(unit, unit, 0.0).expect("degree frame is valid")
}

pub(super) fn line(points: &[(f64, f64)]) -> Shape {
    let points = points
        .iter()
        .map(|&(x, y)| Point::new_raw_xy(x, y))
        .collect::<Vec<_>>();
    Shape::LineString(
        LineSeq::try_new(CoordSeq::try_from_points(&points).expect("coordinates"))
            .expect("a line has two coordinates"),
    )
}

pub(super) fn ring(points: &[(f64, f64)]) -> Ring {
    Ring::from_closed(
        CoordSeq::try_from_points(
            &points
                .iter()
                .map(|&(x, y)| Point::new_raw_xy(x, y))
                .collect::<Vec<_>>(),
        )
        .unwrap(),
    )
    .unwrap()
}

pub(super) fn chart_step_identity_holds(frame: GeographicFrame, from: f64, to: f64) -> bool {
    let axis = frame.longitude();
    let transit = axis.chart_transit(from, to).unwrap();
    let longitude = axis.chart_connection(from, to).unwrap().retained();
    let delta = local_longitude_edge(frame, from, longitude)
        .delta_x(frame.longitude().period())
        .unwrap();
    let canonical = |value: f64| ExactDyadic::from_f64(axis.canonicalize(value).unwrap().get());
    let period = ExactDyadic::from_f64(axis.period());
    let turn = match transit {
        0 => ExactDyadic::zero(),
        1 => period,
        -1 => period.neg(),
        other => panic!("an edge transits the seam at most once, got {other}"),
    };
    let expected = canonical(to)
        .try_subtract_ref(&canonical(from))
        .unwrap()
        .try_add(turn)
        .unwrap();
    delta.cmp(&expected).is_eq()
}

pub(super) fn inventory(frame: GeographicFrame, shapes: &[Shape]) -> ExactGeographicChartInventory {
    let inputs = GeographicInputs::try_from_shapes(frame, shapes.iter())
        .expect("source coordinates are admitted");
    ExactGeographicChartInventory::collect(&inputs).unwrap()
}

pub(super) fn seam_node(chart: &NodedExactChart, latitude: &ExactRational) -> QuotientNodeId {
    chart
        .nodes
        .iter()
        .find_map(|node| match &node.key {
            ExactQuotientKey::Seam(y)
                if y.try_cmp(latitude).expect("exact seam comparison").is_eq() =>
            {
                Some(node.id)
            },
            ExactQuotientKey::Coordinate(_)
            | ExactQuotientKey::Seam(_)
            | ExactQuotientKey::NorthPole
            | ExactQuotientKey::SouthPole => None,
        })
        .expect("the exact seam node exists")
}
pub(super) fn source_uses_node(
    chart: &NodedExactChart,
    source: SourceEdgeId,
    node: QuotientNodeId,
) -> bool {
    chart.atoms.iter().any(|atom| {
        atom_touches_node(chart, atom, node)
            && atom.owners.iter().any(|owner| owner.source_edge == source)
    })
}

pub(super) fn atom_touches_node(
    chart: &NodedExactChart,
    atom: &ExactChartAtom,
    node: QuotientNodeId,
) -> bool {
    atom.ports.iter().any(|&port_id| {
        chart
            .ports
            .get(port_id.0 as usize)
            .is_some_and(|port| port.node == node)
    })
}
