use super::{chart_vertex_origins, exact, retained_site_longitude};
use crate::geometry::tessellation::{
    VoronoiChartCutSide, VoronoiDiagramReceipt, VoronoiProducer, VoronoiVertexOrigin,
};
use crate::geometry::{ExpansionBudget, Point, Polygon, Ring, Shape, VoronoiBoundary};

fn seam_clip() -> Polygon {
    Polygon::new(
        Ring::closed(vec![
            Point::new_raw_xy(160.0, -10.0),
            Point::new_raw_xy(200.0, -10.0),
            Point::new_raw_xy(200.0, 20.0),
            Point::new_raw_xy(160.0, 20.0),
            Point::new_raw_xy(160.0, -10.0),
        ])
        .expect("the Voronoi test clip is a closed ring"),
        Vec::new(),
    )
}

fn seam_quadrilateral() -> Shape {
    Shape::MultiPoint(
        vec![
            Point::new_raw_xy(170.0, 0.0),
            Point::new_raw_xy(-170.0, 0.0),
            Point::new_raw_xy(-170.0, 10.0),
            Point::new_raw_xy(170.0, 8.0),
        ]
        .into(),
    )
}

fn assert_selected_cut_longitudes(receipt: &VoronoiDiagramReceipt, west: f64) {
    let [lower, upper] = receipt
        .selected_chart_cuts
        .expect("geographic Voronoi receipt retains its selected chart cuts");
    assert_eq!(lower.native().get().to_bits(), west.to_bits());
    assert_eq!(upper.native().get().to_bits(), west.to_bits());
    assert_eq!(lower.periods(), 0);
    assert_eq!(upper.periods(), 1);
}

#[test]
fn exact_receipt_retains_selected_cut_longitudes() {
    let source = seam_quadrilateral();
    let clip = seam_clip();
    let frame = crate::geographic::GeographicFrame::crs84();
    let mut budget = ExpansionBudget::new("voronoi_cells", "sites/clip topology");

    let outputs = source
        .voronoi_polygon_outputs_in_frame_budgeted(
            0.0,
            VoronoiBoundary::Polygon(&clip),
            Some(frame),
            &mut budget,
        )
        .unwrap();

    assert_eq!(outputs.receipt.producer, VoronoiProducer::Exact);
    assert_selected_cut_longitudes(&outputs.receipt, 160.0);
}

#[test]
fn dual_receipt_retains_selected_cut_longitudes() {
    let source = seam_quadrilateral();
    let frame = crate::geographic::GeographicFrame::crs84();
    let mut budget = ExpansionBudget::new("voronoi_cells", "sites/clip topology");

    let outputs = source
        .voronoi_polygon_outputs_in_frame_budgeted(
            0.0,
            VoronoiBoundary::Padded,
            Some(frame),
            &mut budget,
        )
        .unwrap();

    assert_eq!(outputs.receipt.producer, VoronoiProducer::Dual);
    assert_selected_cut_longitudes(&outputs.receipt, 170.0);
}

#[test]
fn collinear_receipt_retains_selected_cut_longitudes() {
    let source = Shape::MultiPoint(
        vec![
            Point::new_raw_xy(172.0, 0.0),
            Point::new_raw_xy(-172.0, 0.0),
        ]
        .into(),
    );
    let clip = seam_clip();
    let frame = crate::geographic::GeographicFrame::crs84();
    let mut budget = ExpansionBudget::new("voronoi_cells", "sites/clip topology");

    let outputs = source
        .voronoi_polygon_outputs_in_frame_budgeted(
            0.0,
            VoronoiBoundary::Polygon(&clip),
            Some(frame),
            &mut budget,
        )
        .unwrap();

    assert_eq!(outputs.receipt.producer, VoronoiProducer::Collinear);
    assert_selected_cut_longitudes(&outputs.receipt, 160.0);
}

#[test]
fn lone_site_receipt_retains_selected_cut_longitudes() {
    let source = Shape::Point(Point::new_raw_xy(190.0, 0.0));
    let clip = seam_clip();
    let frame = crate::geographic::GeographicFrame::crs84();
    let mut budget = ExpansionBudget::new("voronoi_cells", "sites/clip topology");

    let outputs = source
        .voronoi_polygon_outputs_in_frame_budgeted(
            0.0,
            VoronoiBoundary::Polygon(&clip),
            Some(frame),
            &mut budget,
        )
        .unwrap();

    assert_eq!(outputs.receipt.producer, VoronoiProducer::LoneSite);
    assert_selected_cut_longitudes(&outputs.receipt, 160.0);
}

#[test]
fn empty_receipts_retain_clip_selected_chart_cuts() {
    let source = Shape::typed_empty(
        crate::geometry::GeometryKind::MultiPoint,
        crate::geometry::CoordinateAxes::XY,
    );
    let clip = seam_clip();
    let frame = crate::geographic::GeographicFrame::crs84();
    let mut budget = ExpansionBudget::new("voronoi_cells", "sites/clip topology");

    let outputs = source
        .voronoi_polygon_outputs_in_frame_budgeted(
            0.0,
            VoronoiBoundary::Polygon(&clip),
            Some(frame),
            &mut budget,
        )
        .unwrap();

    assert_eq!(outputs.receipt.producer, VoronoiProducer::Empty);
    assert_selected_cut_longitudes(&outputs.receipt, 160.0);

    let mut edge_budget = ExpansionBudget::new("voronoi_edge_lines", "sites/clip topology");
    let edges = source
        .voronoi_edge_column_in_frame_budgeted(
            0.0,
            VoronoiBoundary::Polygon(&clip),
            Some(frame),
            &mut edge_budget,
        )
        .unwrap();
    assert_eq!(edges.receipt.producer, VoronoiProducer::Empty);
    assert_selected_cut_longitudes(&edges.receipt, 160.0);
}

#[test]
fn empty_envelope_receipt_has_no_selected_chart_cut() {
    let source = Shape::typed_empty(
        crate::geometry::GeometryKind::MultiPoint,
        crate::geometry::CoordinateAxes::XY,
    );
    let frame = crate::geographic::GeographicFrame::crs84();
    let mut budget = ExpansionBudget::new("voronoi_cells", "sites/clip topology");

    let outputs = source
        .voronoi_polygon_outputs_in_frame_budgeted(
            0.0,
            VoronoiBoundary::Envelope,
            Some(frame),
            &mut budget,
        )
        .unwrap();

    assert_eq!(outputs.receipt.producer, VoronoiProducer::Empty);
    assert!(outputs.receipt.selected_chart_cuts.is_none());
}

#[test]
fn chart_cut_origins_follow_selected_longitudes_and_retain_poles() {
    let frame = crate::geographic::GeographicFrame::crs84();
    let chart = crate::geographic::PointSelectionChart::try_from_points(frame, &[
        Point::new_raw_xy(40.0, 5.0),
        Point::new_raw_xy(60.0, 10.0),
    ])
    .unwrap()
    .expect("the selected site interval has a chart");
    let cuts = chart.cut_longitudes().unwrap();

    for (side, cut) in [
        (VoronoiChartCutSide::Lower, cuts[0]),
        (VoronoiChartCutSide::Upper, cuts[1]),
    ] {
        let x = retained_site_longitude(frame.longitude(), cut)
            .expect("selected chart cuts have exact longitude lifts");
        let point = exact::ExactPoint::from_dyadics(x, exact::ExactDyadic::zero());
        let origins = chart_vertex_origins(Some(&chart), &point).unwrap();
        assert!(
            origins
                .iter()
                .any(|origin| matches!(origin, VoronoiVertexOrigin::ChartCut { side: actual } if *actual == side))
        );
        assert_eq!(
            origins
                .iter()
                .filter(|origin| matches!(origin, VoronoiVertexOrigin::ChartCut { .. }))
                .count(),
            1
        );
    }

    let pole_point = exact::ExactPoint::from_dyadics(
        retained_site_longitude(frame.longitude(), cuts[0])
            .expect("the lower selected cut has an exact longitude lift"),
        exact::ExactDyadic::from_f64(frame.pole()),
    );
    let pole_origins = chart_vertex_origins(Some(&chart), &pole_point).unwrap();
    assert!(
        pole_origins
            .iter()
            .any(|origin| matches!(origin, VoronoiVertexOrigin::ChartCut {
                side: VoronoiChartCutSide::Lower
            }))
    );
    assert!(pole_origins.iter().any(
        |origin| matches!(origin, VoronoiVertexOrigin::Pole { latitude } if *latitude == frame.pole())
    ));
}
