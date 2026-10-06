use geographiclib_rs::Geodesic;

use super::chart_edge::{AuthalicSeries, ChartEdge};
use crate::crs::geodesic::InverseGeodesic as _;
use crate::geographic::GeographicFrame;
use crate::geometry::{
    ChartBox, ChartEdgeDifferential, ChartEdgeDifferentialBounds, ChartMetric,
    ChartPointDistanceSample, GeodesicMetric, GeodesicNumericalPolicy, Point, PointEdgeQuery,
    PointEdgeQueryResult,
};

pub(crate) fn inverse_distance(
    geodesic: &Geodesic,
    lon1: f64,
    lat1: f64,
    lon2: f64,
    lat2: f64,
) -> f64 {
    geodesic.inverse(lat1, lon1, lat2, lon2)
}

pub(crate) fn inverse_azimuths(
    geodesic: &Geodesic,
    lon1: f64,
    lat1: f64,
    lon2: f64,
    lat2: f64,
) -> (f64, f64) {
    let (forward, reverse, _): (f64, f64, f64) = geodesic.inverse(lat1, lon1, lat2, lon2);
    (forward, reverse)
}

pub(crate) fn inverse_distance_azimuths(
    geodesic: &Geodesic,
    lon1: f64,
    lat1: f64,
    lon2: f64,
    lat2: f64,
) -> (f64, f64, f64) {
    let (distance, forward, reverse, _): (f64, f64, f64, f64) =
        geodesic.inverse(lat1, lon1, lat2, lon2);
    (distance, forward, reverse)
}

pub(crate) fn geo_distance(g: &Geodesic, a: Point, b: Point) -> f64 {
    inverse_distance(g, a.x, a.y, b.x, b.y)
}

pub(super) fn chart_point_distance_sample(
    geodesic: &Geodesic,
    frame: GeographicFrame,
    source: Point,
    target: Point,
    source_speed: f64,
    source_azimuth: f64,
) -> crate::error::Result<ChartPointDistanceSample> {
    let source = frame
        .to_geodesic_point(source)
        .expect("geographic Hausdorff source was admitted");
    let target = frame
        .to_geodesic_point(target)
        .expect("geographic Hausdorff target witness was admitted");
    let (_, distance, _, _, sin_gradient, cos_gradient, ..) = geodesic._gen_inverse(
        target.y,
        target.x,
        source.y,
        source.x,
        geographiclib_rs::capability::DISTANCE,
    );
    if !distance.is_finite() {
        return Err(crate::crs::CrsError::message(
            "geodesic distance is not representable in binary64 metres",
        ));
    }
    let derivative = if distance == 0.0 || source_speed == 0.0 {
        0.0
    } else {
        // Only the terminal azimuth is needed. Reduce to the backend's
        // principal quadrant before degree conversion to retain its rounding.
        let (mut y, mut x) = (sin_gradient, cos_gradient);
        let swapped = y.abs() > x.abs();
        if swapped {
            std::mem::swap(&mut x, &mut y);
        }
        let reflected = x < 0.0;
        if reflected {
            x = -x;
        }
        let angle = y.atan2(x).to_degrees();
        let gradient_azimuth = match (swapped, reflected) {
            (false, false) => angle,
            (false, true) => {
                if y >= 0.0 {
                    180.0 - angle
                } else {
                    -180.0 - angle
                }
            },
            (true, false) => 90.0 - angle,
            (true, true) => angle + -90.0,
        };
        source_speed * (source_azimuth - gradient_azimuth.to_radians()).cos()
    };
    Ok(ChartPointDistanceSample {
        distance,
        derivative,
    })
}

/// Adapts a `geographiclib` [`Geodesic`] to the geometry layer's
/// [`GeodesicMetric`], so `Shape::geodesic_distance` can measure on the
/// ellipsoid without depending on the CRS module.
pub(crate) struct EllipsoidMetric<'a> {
    geodesic: &'a Geodesic,
    frame: GeographicFrame,
    runtime_generation: u64,
    policy: GeodesicNumericalPolicy,
    authalic: std::cell::OnceCell<AuthalicSeries>,
}

impl<'a> EllipsoidMetric<'a> {
    #[cfg(test)]
    pub(crate) const fn for_crs84_geometry_test(geodesic: &'a Geodesic) -> Self {
        Self::for_geodesic_at_frame(geodesic, 0, GeographicFrame::crs84())
    }

    pub(crate) const fn for_geodesic_at_frame(
        geodesic: &'a Geodesic,
        runtime_generation: u64,
        frame: GeographicFrame,
    ) -> Self {
        Self {
            geodesic,
            frame,
            runtime_generation,
            policy: GeodesicNumericalPolicy::new(geodesic.a),
            authalic: std::cell::OnceCell::new(),
        }
    }

    pub(crate) const fn ellipsoid_parameters(&self) -> (f64, f64) {
        (self.geodesic.a, self.geodesic.f)
    }

    pub(crate) const fn frame(&self) -> GeographicFrame {
        self.frame
    }

    fn authalic_series(&self) -> AuthalicSeries {
        *self
            .authalic
            .get_or_init(|| AuthalicSeries::new(self.geodesic))
    }

    fn to_geodesic_point(&self, point: Point) -> Point {
        self.frame
            .to_geodesic_point(point)
            .expect("geographic metric received an admitted point")
    }
}

impl GeodesicMetric for EllipsoidMetric<'_> {
    fn runtime_generation(&self) -> u64 {
        self.runtime_generation
    }

    fn ensure_shape(&self, shape: &crate::geometry::Shape) -> crate::error::Result<()> {
        crate::crs::ensure_geographic_domain_in_frame(self.frame, shape)
    }

    fn segment_length(&self, a: Point, b: Point) -> f64 {
        // Distance-only capability: this is the point-distance kernel of
        // every geodesic point lane — the azimuth grade is pure waste here.
        geo_distance(
            self.geodesic,
            self.to_geodesic_point(a),
            self.to_geodesic_point(b),
        )
    }

    fn point_distance_lower_bound(&self, a: Point, b: Point) -> crate::error::Result<f64> {
        let bound = auxiliary_sphere_bound(
            self.geodesic,
            self.to_geodesic_point(a),
            self.to_geodesic_point(b),
        );
        // The bound is tight only for short meridional arcs near the equator,
        // where its binary64 rounding is nanometres. Stepping down by the
        // backend floor keeps it below the distance the backend reports
        // there, so a threshold query cannot prune a point it would accept.
        Ok((bound - self.policy.backend_floor()).max(0.0))
    }

    fn geodesic_numerical_policy(&self) -> GeodesicNumericalPolicy {
        self.policy
    }
}

impl ChartMetric for EllipsoidMetric<'_> {
    type PointEdgeScratch = super::edge_distance::PointEdgeScratch;

    fn chart_frame(&self) -> GeographicFrame {
        self.frame
    }

    fn chart_edge(&self, start: Point, end: Point) -> crate::error::Result<ChartEdge> {
        ChartEdge::new(start, end, self.frame)
    }

    fn chart_edge_length(&self, edge: &ChartEdge) -> crate::error::Result<f64> {
        edge.length(self.geodesic)
    }

    fn chart_edge_length_to(&self, edge: &ChartEdge, parameter: f64) -> crate::error::Result<f64> {
        edge.arclength(parameter, self.geodesic, self.frame)
    }

    fn chart_edge_locate_t(
        &self,
        edge: &ChartEdge,
        distance: f64,
        length: f64,
    ) -> crate::error::Result<f64> {
        edge.locate_t(distance, length, self.geodesic, self.frame)
    }

    fn chart_edge_interval_length(
        &self,
        edge: &ChartEdge,
        start: f64,
        end: f64,
    ) -> crate::error::Result<f64> {
        edge.integrate(start, end, self.geodesic)
    }

    fn chart_edge_witness(
        &self,
        start: Point,
        end: Point,
        edge: &ChartEdge,
        parameter: f64,
        length: Option<f64>,
    ) -> crate::error::Result<Point> {
        edge.witness(start, end, parameter, length, self.geodesic)
    }

    fn chart_edge_differential(&self, edge: &ChartEdge, parameter: f64) -> ChartEdgeDifferential {
        super::chart_edge::chart_edge_differential(edge, self.geodesic, parameter)
    }

    fn chart_edge_differential_bounds(&self, edge: &ChartEdge) -> ChartEdgeDifferentialBounds {
        super::chart_edge::chart_edge_differential_bounds(edge, self.geodesic)
    }

    fn chart_edge_pole_distance_upper_bound(&self, edge: &ChartEdge, target: Point) -> Option<f64> {
        edge.parallel_pole_distance_upper_bound(self.geodesic, self.frame, target)
    }

    fn point_edge_query(
        &self,
        point: Point,
        edge: &ChartEdge,
        envelope_lower: f64,
        query: PointEdgeQuery,
        scratch: &mut Self::PointEdgeScratch,
    ) -> crate::error::Result<PointEdgeQueryResult> {
        super::edge_distance::point_edge_query(
            self.geodesic,
            self.frame,
            point,
            edge,
            envelope_lower,
            query,
            self.policy,
            scratch,
        )
    }

    fn chart_point_distance_sample(
        &self,
        source: Point,
        target: Point,
        source_speed: f64,
        source_azimuth: f64,
    ) -> crate::error::Result<ChartPointDistanceSample> {
        chart_point_distance_sample(
            self.geodesic,
            self.frame,
            source,
            target,
            source_speed,
            source_azimuth,
        )
    }

    fn geodesic_distance_hessian_upper_bound(&self, lower_distance: f64) -> f64 {
        super::edge_distance::curvature_hessian_bound(self.geodesic, lower_distance)
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
    ) -> crate::geometry::GeographicAreaDensity {
        self.authalic_series()
            .area_density(self.geodesic, reference_colatitude, reference_pole)
    }
}

/// Karney's auxiliary-sphere lower bound on the geodesic distance: the
/// semi-minor axis times the central angle between the reduced latitudes,
/// taken with the raw longitude gap, which never exceeds the auxiliary one.
/// Zero outside the oblate domain `0 <= f < 1`, where the bound is unproven.
fn auxiliary_sphere_bound(geodesic: &Geodesic, a: Point, b: Point) -> f64 {
    if geodesic.f < 0.0 || geodesic.f >= 1.0 {
        return 0.0;
    }
    // This is a degree/Greenwich kernel. Collapse periodic aliases before
    // deriving the longitude gap; otherwise 0 and 360 degrees could produce
    // a positive lower bound for two coincident points.
    let axis = GeographicFrame::crs84().longitude();
    let a_lon = axis
        .canonicalize(a.x)
        .expect("finite lower-bound longitude")
        .get();
    let b_lon = axis
        .canonicalize(b.x)
        .expect("finite lower-bound longitude")
        .get();
    let one_minus_f = 1.0 - geodesic.f;
    let reduced = |lat_degrees: f64| {
        f64::atan(one_minus_f * lat_degrees.to_radians().tan())
            .clamp(-std::f64::consts::FRAC_PI_2, std::f64::consts::FRAC_PI_2)
    };
    let (beta1, beta2) = (reduced(a.y), reduced(b.y));
    let sin_lat = ((beta2 - beta1) / 2.0).sin();
    let sin_lon = ((b_lon - a_lon).to_radians() / 2.0).sin();
    // Plain ops: scalar `mul_add` is a libm call below x86-64-v3.
    let h = (sin_lon * sin_lon * beta1.cos() * beta2.cos() + sin_lat * sin_lat).clamp(0.0, 1.0);
    geodesic.a * one_minus_f * 2.0 * h.sqrt().asin()
}
