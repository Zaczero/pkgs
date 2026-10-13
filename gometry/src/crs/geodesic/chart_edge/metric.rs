use super::ChartEdge;
use super::derivatives::{chart_edge_differential, chart_edge_differential_bounds};
use crate::error::Result;
use crate::geographic::GeographicFrame;
use crate::geometry::types::ChartMetric;
use crate::geometry::{
    ChartBox, ChartEdgeDifferential, ChartEdgeDifferentialBounds, ChartPointDistanceSample,
    GeographicAreaDensity, Point, PointEdgeQuery, PointEdgeQueryResult, Work,
};

impl ChartMetric for super::GeodesicMetric {
    type PointEdgeScratch = super::edge_distance::PointEdgeScratch;

    fn chart_frame(&self) -> GeographicFrame {
        self.frame
    }

    fn chart_edge(&self, start: Point, end: Point) -> Result<ChartEdge> {
        ChartEdge::new(start, end, self.frame)
    }

    fn chart_edge_length(&self, edge: &ChartEdge) -> Result<f64> {
        edge.length(&self.geodesic)
    }

    fn chart_edge_length_with_work<W: Work>(&self, edge: &ChartEdge, work: &mut W) -> Result<f64> {
        edge.length_with_work(&self.geodesic, work)
    }

    fn chart_edge_length_to(&self, edge: &ChartEdge, parameter: f64) -> Result<f64> {
        edge.arclength(parameter, &self.geodesic)
    }

    fn chart_edge_arclength_fraction(
        &self,
        edge: &ChartEdge,
        parameter: f64,
        total_length: Option<f64>,
    ) -> Result<f64> {
        edge.arclength_fraction(parameter, total_length, &self.geodesic)
    }

    fn chart_edge_arclength_fraction_with_work<W: Work>(
        &self,
        edge: &ChartEdge,
        parameter: f64,
        total_length: Option<f64>,
        work: &mut W,
    ) -> Result<f64> {
        edge.arclength_fraction_with_work(parameter, total_length, &self.geodesic, work)
    }

    fn chart_edge_locate_t(&self, edge: &ChartEdge, distance: f64, length: f64) -> Result<f64> {
        edge.locate_t(distance, length, &self.geodesic)
    }

    fn chart_edge_interval_length(&self, edge: &ChartEdge, start: f64, end: f64) -> Result<f64> {
        edge.integrate(start, end, &self.geodesic)
    }

    fn chart_edge_witness(
        &self,
        start: Point,
        end: Point,
        edge: &ChartEdge,
        parameter: f64,
        length: Option<f64>,
    ) -> Result<Point> {
        edge.witness(start, end, parameter, length, &self.geodesic)
    }

    fn chart_edge_differential(&self, edge: &ChartEdge, parameter: f64) -> ChartEdgeDifferential {
        chart_edge_differential(edge, &self.geodesic, parameter)
    }

    fn chart_edge_differential_bounds(&self, edge: &ChartEdge) -> ChartEdgeDifferentialBounds {
        chart_edge_differential_bounds(edge, &self.geodesic)
    }

    fn chart_edge_moving_correspondence_upper(
        &self,
        source: &ChartEdge,
        target: &ChartEdge,
        source_interval: [f64; 2],
        target_interval: [f64; 2],
        source_points: &[Point; 2],
        target_points: &[Point; 2],
        endpoint_distances: [f64; 2],
    ) -> Option<f64> {
        source.moving_correspondence_upper(
            target,
            source_interval,
            target_interval,
            source_points,
            target_points,
            endpoint_distances,
            &self.geodesic,
        )
    }

    fn chart_edge_pole_distance_upper_bound(
        &self,
        edge: &ChartEdge,
        source_interval: [f64; 2],
        target: Point,
    ) -> Option<f64> {
        edge.pole_distance_upper_bound(&self.geodesic, self.frame, source_interval, target)
    }

    fn point_edge_query(
        &self,
        point: Point,
        edge: &ChartEdge,
        envelope_lower: f64,
        query: PointEdgeQuery,
        scratch: &mut Self::PointEdgeScratch,
    ) -> Result<PointEdgeQueryResult> {
        super::edge_distance::point_edge_query(
            &self.geodesic,
            self.frame,
            point,
            edge,
            envelope_lower,
            query,
            crate::geometry::GeodesicNumericalPolicy::new(self.geodesic.a),
            scratch,
        )
    }

    fn chart_point_distance_sample(
        &self,
        source: Point,
        target: Point,
        source_speed: f64,
        source_azimuth: f64,
    ) -> Result<ChartPointDistanceSample> {
        super::ellipsoid_metric::chart_point_distance_sample(
            &self.geodesic,
            self.frame,
            source,
            target,
            source_speed,
            source_azimuth,
        )
    }

    fn geodesic_distance_hessian_upper_bound(
        &self,
        lower_distance: f64,
        upper_distance: f64,
    ) -> f64 {
        super::edge_distance::curvature_hessian_bound(
            &self.geodesic,
            lower_distance,
            upper_distance,
        )
    }

    fn point_envelope_bound(&self, point: Point) -> impl Fn(ChartBox) -> f64 {
        let point = self
            .frame
            .to_geodesic_point(point)
            .expect("the query point was admitted before envelope pruning");
        let bound =
            super::envelope::GeodesicEnvelopeBound::new(self.geodesic.a, self.geodesic.f, point);
        move |bounds| bound.lower_bound(bounds)
    }

    fn chart_area_density(
        &self,
        reference_colatitude: f64,
        reference_pole: crate::geographic::GeodeticPole,
    ) -> GeographicAreaDensity {
        self.authalic_series
            .area_density(&self.geodesic, reference_colatitude, reference_pole)
    }
}
