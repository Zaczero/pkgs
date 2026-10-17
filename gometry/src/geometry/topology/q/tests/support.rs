use super::*;

pub(super) fn degrees() -> GeographicFrame {
    let unit = AngularFactor::try_new("test degrees", std::f64::consts::PI / 180.0).unwrap();
    GeographicFrame::try_new(unit, unit, 0.0).unwrap()
}

pub(super) fn ring(points: &[(f64, f64)]) -> Ring {
    Ring::from_closed(
        CoordSeq::try_from_points(
            &points
                .iter()
                .map(|&(x, y)| crate::geometry::Point::new_raw_xy(x, y))
                .collect::<Vec<_>>(),
        )
        .unwrap(),
    )
    .unwrap()
}

pub(super) fn line(points: &[(f64, f64)]) -> Shape {
    let coordinates = CoordSeq::try_from_points(
        &points
            .iter()
            .map(|&(x, y)| crate::geometry::Point::new_raw_xy(x, y))
            .collect::<Vec<_>>(),
    )
    .unwrap();
    Shape::LineString(LineSeq::try_new(coordinates).unwrap())
}

pub(super) fn q_graph_for_shapes(
    shapes: &[Shape],
) -> (ExactGeographicChartInventory, ExactGeographicQ) {
    let frame = degrees();
    let inputs =
        crate::geographic::GeographicInputs::try_from_shapes(frame, shapes.iter()).unwrap();
    let sources = ExactGeographicChartInventory::collect(&inputs).unwrap();
    let mut budget = ExpansionBudget::new("test", "exact chart");
    let chart = sources.node(&mut budget).unwrap();
    let q = ExactGeographicQ::build(frame, &sources, &chart).unwrap();
    (sources, q)
}

pub(super) fn noded_chart_for_shapes(shapes: &[Shape]) -> (GeographicFrame, NodedExactChart) {
    let frame = degrees();
    let inputs =
        crate::geographic::GeographicInputs::try_from_shapes(frame, shapes.iter()).unwrap();
    let sources = ExactGeographicChartInventory::collect(&inputs).unwrap();
    let mut budget = ExpansionBudget::new("test", "exact chart");
    let chart = sources.node(&mut budget).unwrap();
    (frame, chart)
}

pub(super) fn q_graph(shape: &Shape) -> (ExactGeographicChartInventory, ExactGeographicQ) {
    q_graph_for_shapes(std::slice::from_ref(shape))
}

pub(super) fn frame_with_unit(radians_per_unit: f64) -> GeographicFrame {
    let unit = AngularFactor::try_new("test unit", radians_per_unit).unwrap();
    GeographicFrame::try_new(unit, unit, 0.0).unwrap()
}

pub(super) fn native_frames() -> [(&'static str, GeographicFrame); 3] {
    [
        ("degrees", degrees()),
        ("radians", frame_with_unit(1.0)),
        ("grads", frame_with_unit(std::f64::consts::TAU / 400.0)),
    ]
}

pub(super) fn native_ring(frame: GeographicFrame, points: &[(f64, f64)]) -> Ring {
    fn longitude(period: f64, degrees: f64) -> f64 {
        use crate::geometry::numerics::product::exact_power_of_two;
        if degrees >= 360.0 {
            longitude(period, degrees - 360.0) + period
        } else {
            let scale = exact_power_of_two(44);
            (period * (degrees / 360.0) * scale).round() / scale
        }
    }
    let period = frame.longitude().period();
    let pole = frame.chart_pole_latitude(ChartPole::North);
    ring(
        &points
            .iter()
            .map(|&(x, y)| {
                let latitude = if y.abs().to_bits() == 90.0_f64.to_bits() {
                    pole * y.signum()
                } else {
                    pole * (y / 90.0)
                };
                (longitude(period, x), latitude)
            })
            .collect::<Vec<_>>(),
    )
}

pub(super) fn q_graph_in_frame(
    frame: GeographicFrame,
    shape: &Shape,
) -> (ExactGeographicChartInventory, ExactGeographicQ) {
    let inputs =
        crate::geographic::GeographicInputs::try_from_shapes(frame, std::iter::once(shape))
            .unwrap();
    let sources = ExactGeographicChartInventory::collect(&inputs).unwrap();
    let mut budget = ExpansionBudget::new("test", "exact chart");
    let chart = sources.node(&mut budget).unwrap();
    let q = ExactGeographicQ::build(frame, &sources, &chart).unwrap();
    (sources, q)
}

pub(super) fn compare_source_ray_order(
    frame: GeographicFrame,
    shape: &Shape,
    origin: (f64, f64),
    left_target: (f64, f64),
    right_target: (f64, f64),
) -> Ordering {
    let (_, q) = q_graph_in_frame(frame, shape);
    let exact_point = |(x, y): (f64, f64)| {
        ExactChartPoint::from_rationals(ExactRational::from_f64(x), ExactRational::from_f64(y))
    };
    let origin = exact_point(origin);
    let left_target = exact_point(left_target);
    let right_target = exact_point(right_target);
    let period = ExactRational::from_f64(frame.longitude().period());
    let same_chart_location = |chart_point: &ExactChartPoint, source_point: &ExactChartPoint| {
        if !chart_point
            .y
            .try_cmp(&source_point.y)
            .expect("test latitude comparisons are exact")
            .is_eq()
        {
            return false;
        }
        let east = source_point
            .x
            .try_add(&period)
            .expect("one longitude period fits the test chart");
        let west = source_point
            .x
            .try_subtract(&period)
            .expect("one longitude period fits the test chart");
        [&source_point.x, &east, &west]
            .into_iter()
            .any(|longitude| {
                chart_point
                    .x
                    .try_cmp(longitude)
                    .expect("test longitude comparisons are exact")
                    .is_eq()
            })
    };
    let north_pole = ExactRational::from_f64(frame.chart_pole_latitude(ChartPole::North));
    let south_pole = ExactRational::from_f64(frame.chart_pole_latitude(ChartPole::South));
    let pole = if origin.y.try_cmp(&north_pole).unwrap().is_eq() {
        Some(ChartPole::North)
    } else if origin.y.try_cmp(&south_pole).unwrap().is_eq() {
        Some(ChartPole::South)
    } else {
        None
    };
    let origin_node = q
        .nodes
        .iter()
        .find(|node| {
            pole.is_some_and(|pole| node.pole == Some(pole))
                || q.node_ports.get(node.ports.clone()).is_some_and(|ports| {
                    ports.iter().any(|port| {
                        q.ports
                            .get(port.0 as usize)
                            .is_some_and(|port| same_chart_location(&port.point, &origin))
                    })
                })
        })
        .expect("the source rays share their origin node")
        .id;
    let dart_to = |target: &ExactChartPoint| {
        q.darts
            .iter()
            .enumerate()
            .find_map(|(index, dart)| {
                if dart.from_node != origin_node {
                    return None;
                }
                let port = q.ports.get(dart.to_port.0 as usize)?;
                same_chart_location(&port.point, target).then(|| {
                    QDartId(u32::try_from(index).expect("the test fan fits the Q dart identifier"))
                })
            })
            .expect("the source target has one outgoing Q dart")
    };
    let left = dart_to(&left_target);
    let right = dart_to(&right_target);
    let cut_east = q
        .cut
        .try_add(&ExactRational::from_f64(frame.longitude().period()))
        .unwrap();
    compare_q_darts(
        QFaceChart {
            cut: &q.cut,
            cut_east: &cut_east,
            ports: &q.ports,
            nodes: &q.nodes,
        },
        &q.darts,
        left,
        right,
    )
    .unwrap()
}
