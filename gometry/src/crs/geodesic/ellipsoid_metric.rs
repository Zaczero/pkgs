use geographiclib_rs::Geodesic;

use super::chart_edge::{AuthalicSeries, ChartEdge};
use crate::crs::geodesic::segment::{
    PreparedSegment, geodesic_curvature_domain, longitude_difference,
    spherical_meridional_branch_parameters,
};
use crate::crs::geodesic::{
    DirectGeodesic, InverseGeodesic as _, checked_geodesic_segment, geodesic_point_segment_query,
    geodesic_unit_vector,
};
use crate::geographic::GeographicFrame;
use crate::geometry::{
    ChartBox, ChartMetric, GeodesicMetric, GeodesicNumericalPolicy, GeodesicPreparedSegment,
    GeodesicSegment, GeodesicSegmentDerivative, GeodesicSegmentQuery, GeodesicSegmentQueryResult,
    GeodesicSegmentSample, GeodesicSegmentScratch, MOrdinate, Point, PointEdgeQuery,
    PointEdgeQueryResult, XY, ZOrdinate, interpolate_optional_ordinate,
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

pub(crate) fn geo_distance_azimuths(g: &Geodesic, a: Point, b: Point) -> (f64, f64, f64) {
    inverse_distance_azimuths(g, a.x, a.y, b.x, b.y)
}

pub(crate) fn geo_direct<T>(g: &Geodesic, from: Point, azimuth: f64, distance: f64) -> T
where
    Geodesic: DirectGeodesic<T>,
{
    g.direct(from.y, from.x, azimuth, distance)
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
    normalized: std::cell::OnceCell<Geodesic>,
}

#[derive(Clone, Copy)]
pub(crate) struct PreparedMetricSegment {
    source_segment: GeodesicSegment,
    inner: PreparedSegment,
    frame: GeographicFrame,
    semi_major: f64,
}

impl GeodesicPreparedSegment for PreparedMetricSegment {
    fn segment(&self) -> GeodesicSegment {
        self.source_segment
    }

    fn sample(
        &self,
        point: Point,
        along: f64,
        derivative: bool,
    ) -> crate::error::Result<GeodesicSegmentSample> {
        let point = self
            .frame
            .to_geodesic_point(point)
            .expect("geographic metric received an admitted point");
        let sample = self
            .inner
            .sample(point, along / self.semi_major, derivative)?;
        Ok(GeodesicSegmentSample {
            derivative: sample
                .derivative
                .map(|derivative| GeodesicSegmentDerivative {
                    value: derivative.value * self.semi_major,
                    error: derivative.error * self.semi_major,
                }),
        })
    }

    fn position(&self, along: f64) -> crate::error::Result<Point> {
        let point = self.inner.position(along / self.semi_major)?;
        Ok(self
            .frame
            .point_from_geodesic(point)
            .expect("geodesic metric produced a finite point"))
    }

    fn tangent(&self, along: f64) -> crate::error::Result<Option<[f64; 3]>> {
        self.inner.tangent(along / self.semi_major)
    }
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
            normalized: std::cell::OnceCell::new(),
        }
    }

    fn normalized(&self) -> &Geodesic {
        self.normalized
            .get_or_init(|| Geodesic::new(1.0, self.geodesic.f))
    }

    fn scale_distance(&self, normalized: f64) -> f64 {
        let distance = normalized * self.geodesic.a;
        if normalized > 0.0 && distance == 0.0 {
            f64::NAN
        } else {
            distance
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

    fn to_geodesic_segment(&self, segment: &GeodesicSegment) -> GeodesicSegment {
        GeodesicSegment {
            start: self.to_geodesic_point(segment.start),
            end: self.to_geodesic_point(segment.end),
            length: segment.length / self.geodesic.a,
            azimuth0: segment.azimuth0,
        }
    }
}

impl GeodesicMetric for EllipsoidMetric<'_> {
    type PreparedSegment = PreparedMetricSegment;

    fn runtime_generation(&self) -> u64 {
        self.runtime_generation
    }

    fn ensure_shape(&self, shape: &crate::geometry::Shape) -> crate::error::Result<()> {
        crate::crs::ensure_geographic_domain_in_frame(self.frame, shape)
    }

    fn auxiliary_unit(&self, point: XY) -> [f64; 3] {
        let (longitude, latitude) = self
            .frame
            .to_geodesic_xy(point.x, point.y)
            .expect("geographic metric received an admitted point");
        auxiliary_unit(XY::new(longitude, latitude))
    }

    fn make_segment(&self, start: Point, end: Point) -> crate::error::Result<GeodesicSegment> {
        let mut segment = checked_geodesic_segment(
            self.normalized(),
            self.to_geodesic_point(start),
            self.to_geodesic_point(end),
        )?;
        let length = self.scale_distance(segment.length);
        if segment.length > 0.0 && !(length.is_finite() && length > 0.0) {
            return Err(crate::crs::CrsError::message(
                "geodesic segment length is not representable in binary64 metres",
            ));
        }
        segment.length = length;
        // Geometry-layer working sets retain source-frame endpoints for
        // witnesses, ordering, and ordinate interpolation.
        segment.start = start;
        segment.end = end;
        Ok(segment)
    }

    fn prepare_segment(
        &self,
        segment: &GeodesicSegment,
    ) -> crate::error::Result<Self::PreparedSegment> {
        let geodesic_segment = self.to_geodesic_segment(segment);
        Ok(PreparedMetricSegment {
            source_segment: *segment,
            inner: PreparedSegment::new(self.normalized(), &geodesic_segment),
            frame: self.frame,
            semi_major: self.geodesic.a,
        })
    }

    fn point_segment_query(
        &self,
        point: Point,
        segment: &GeodesicSegment,
        query: GeodesicSegmentQuery,
        scratch: &mut GeodesicSegmentScratch,
    ) -> crate::error::Result<GeodesicSegmentQueryResult> {
        let query = normalize_query(query, self.geodesic.a);
        let point = self.to_geodesic_point(point);
        let segment = self.to_geodesic_segment(segment);
        let result =
            geodesic_point_segment_query(self.normalized(), point, segment, query, scratch)?;
        Ok(match result {
            GeodesicSegmentQueryResult::Attained { minimum, foot } => {
                let minimum = scale_minimum(minimum, self.geodesic.a)?;
                GeodesicSegmentQueryResult::Attained {
                    minimum,
                    foot: map_geodesic_foot(self, foot),
                }
            },
            GeodesicSegmentQueryResult::Excluded => result,
        })
    }

    fn segment_length(&self, a: Point, b: Point) -> f64 {
        // Distance-only capability: this is the point-distance kernel of
        // every geodesic point lane — the azimuth grade is pure waste here.
        self.scale_distance(geo_distance(
            self.normalized(),
            self.to_geodesic_point(a),
            self.to_geodesic_point(b),
        ))
    }

    #[expect(
        clippy::float_cmp,
        reason = "a pole or antimeridian is one specific representable coordinate, and the question is whether a vertex IS that coordinate; a tolerance would admit vertices merely near it"
    )]
    fn interpolate(&self, a: Point, b: Point, fraction: f64) -> Point {
        if fraction == 0.0 {
            return a;
        }
        if fraction == 1.0 {
            return b;
        }
        let a_geodesic = self.to_geodesic_point(a);
        let b_geodesic = self.to_geodesic_point(b);
        let direct_geodesic = if self.geodesic._c2.is_finite() && self.geodesic._c2 > 0.0 {
            self.geodesic
        } else {
            self.normalized()
        };
        let (length, azimuth) = if self.geodesic.f == 0.0
            && let Some((angular_length, azimuth)) =
                spherical_meridional_branch_parameters(a_geodesic, b_geodesic)
        {
            (angular_length * direct_geodesic.a, azimuth)
        } else {
            let (length, azimuth, _) =
                geo_distance_azimuths(direct_geodesic, a_geodesic, b_geodesic);
            (length, azimuth)
        };
        // Lat/lon-only direct — final azimuth is unused.
        // The equator is itself a geodesic. Keep its exact Cartesian longitude
        // ramp when the endpoints do not take the antimeridian shortcut; this
        // preserves exact legacy coordinates without changing the curve model.
        let (lat, lon): (f64, f64) = if a_geodesic.y == 0.0
            && b_geodesic.y == 0.0
            && (b_geodesic.x - a_geodesic.x).abs() < 180.0
        {
            (
                0.0,
                crate::geometry::interpolate_f64(a_geodesic.x, b_geodesic.x, fraction),
            )
        } else {
            geo_direct(direct_geodesic, a_geodesic, azimuth, length * fraction)
        };
        let point = Point::new_axes(
            lon,
            lat,
            ZOrdinate(interpolate_optional_ordinate(a.z(), b.z(), fraction)),
            MOrdinate(interpolate_optional_ordinate(a.m(), b.m(), fraction)),
        )
        .expect("geodesic interpolation is finite");
        self.frame
            .point_from_geodesic(point)
            .expect("geodesic interpolation is finite")
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

    fn geodesic_curvature_domain(&self) -> Option<(f64, f64, f64)> {
        geodesic_curvature_domain(self.normalized())
    }

    fn geodesic_backend_floor(&self) -> f64 {
        self.policy.backend_floor()
    }

    fn geodesic_radius(&self) -> f64 {
        self.geodesic.a
    }

    fn geodesic_numerical_policy(&self) -> GeodesicNumericalPolicy {
        self.policy
    }

    fn surface_unit(&self, point: Point) -> Option<[f64; 3]> {
        let point = self.to_geodesic_point(point);
        Some(geodesic_unit_vector(point.x, point.y))
    }

    fn is_spherical(&self) -> bool {
        self.geodesic.f == 0.0
    }

    fn is_equatorial_segment(&self, segment: &GeodesicSegment) -> bool {
        let segment = self.to_geodesic_segment(segment);
        segment.start.y == 0.0
            && segment.end.y == 0.0
            && longitude_difference(segment.start.x, segment.end.x).abs() < 180.0
    }

    #[expect(
        clippy::float_cmp,
        reason = "a pole is one specific representable coordinate; a tolerance would admit a merely near-polar point"
    )]
    fn is_polar_point(&self, point: Point) -> bool {
        self.to_geodesic_point(point).y.abs() == 90.0
    }

    fn equatorial_opposite_meridian_parameter(
        &self,
        segment: &GeodesicSegment,
        target: Point,
    ) -> Option<f64> {
        let segment = self.to_geodesic_segment(segment);
        let target = self.to_geodesic_point(target);
        if segment.start.y != 0.0 || segment.end.y != 0.0 {
            return None;
        }
        let delta = longitude_difference(segment.start.x, segment.end.x);
        if !(delta != 0.0 && delta.abs() < 180.0) {
            return None;
        }
        let opposite_delta = longitude_difference(segment.start.x, target.x + 180.0);
        Some(self.scale_distance(segment.length * opposite_delta.abs() / delta.abs()))
            .filter(|along| along.is_finite())
    }
}

impl ChartMetric for EllipsoidMetric<'_> {
    type Edge = ChartEdge;
    type PointEdgeScratch = super::edge_distance::PointEdgeScratch;

    fn chart_frame(&self) -> GeographicFrame {
        self.frame
    }

    fn chart_edge(&self, start: Point, end: Point) -> crate::error::Result<Self::Edge> {
        ChartEdge::new(start, end, self.frame)
    }

    fn chart_edge_length(&self, edge: &Self::Edge) -> crate::error::Result<f64> {
        edge.length(self.geodesic)
    }

    fn chart_edge_bounds(&self, edge: &Self::Edge) -> ChartBox {
        edge.bounds()
    }

    fn chart_edge_witness(
        &self,
        start: Point,
        end: Point,
        edge: &Self::Edge,
        parameter: f64,
    ) -> crate::error::Result<Point> {
        edge.witness(start, end, parameter, self.geodesic)
    }

    fn point_edge_query(
        &self,
        point: Point,
        edge: &Self::Edge,
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

    fn point_envelope_bound(&self, point: Point) -> impl Fn(ChartBox) -> f64 {
        let point = self
            .frame
            .to_geodesic_point(point)
            .expect("the query point was admitted before envelope pruning");
        let bound =
            super::envelope::GeodesicEnvelopeBound::new(self.geodesic.a, self.geodesic.f, point);
        move |bounds| bound.lower_bound(bounds)
    }

    fn chart_edge_seam_span(&self, edge: &Self::Edge) -> Option<[f64; 2]> {
        edge.seam_span()
    }

    fn chart_seam_span_length(&self, south: f64, north: f64) -> crate::error::Result<f64> {
        let half_period = self.frame.longitude().half_period();
        ChartEdge::new(
            Point::new_raw_xy(half_period, south),
            Point::new_raw_xy(-half_period, north),
            self.frame,
        )?
        .length(self.geodesic)
    }

    fn chart_area_edge_term(&self, edge: &Self::Edge, north: bool) -> f64 {
        edge.authalic_area_term(&self.authalic_series(), north)
    }

    fn chart_area_factor(&self) -> f64 {
        self.authalic_series().area_factor(self.geodesic.a)
    }

    fn chart_surface_area(&self) -> f64 {
        self.authalic_series().surface_area(self.geodesic.a)
    }
}

#[expect(
    clippy::single_option_map,
    reason = "the named helper centralizes conversion of an optional geodesic foot into the source frame"
)]
fn map_geodesic_foot(metric: &EllipsoidMetric<'_>, foot: Option<Point>) -> Option<Point> {
    foot.map(|foot| {
        metric
            .frame
            .point_from_geodesic(foot)
            .expect("geodesic witness is finite")
    })
}

fn normalize_query(query: GeodesicSegmentQuery, scale: f64) -> GeodesicSegmentQuery {
    match query {
        GeodesicSegmentQuery::Minimum { cutoff, witness } => GeodesicSegmentQuery::Minimum {
            cutoff: cutoff / scale,
            witness,
        },
    }
}

fn scale_minimum(
    minimum: crate::geometry::GeodesicMinimum,
    scale: f64,
) -> crate::error::Result<crate::geometry::GeodesicMinimum> {
    let policy = GeodesicNumericalPolicy::new(scale);
    let distance = minimum.distance * scale;
    let evaluation_error = minimum.evaluation_error * scale;
    if minimum.distance > 0.0 && (!distance.is_finite() || distance == 0.0) {
        return Err(crate::crs::CrsError::message(
            "geodesic distance is not representable in binary64 metres",
        ));
    }
    Ok(crate::geometry::GeodesicMinimum {
        witness: crate::geometry::GeodesicSegmentMinimum {
            distance,
            along: minimum.witness.along * scale,
        },
        lower: (minimum.lower * scale)
            .max((distance - evaluation_error - policy.minimum_gap()).max(0.0)),
        evaluation_error,
    })
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

fn auxiliary_unit(point: XY) -> [f64; 3] {
    let lon = point.x.to_radians();
    let lat = point.y.to_radians();
    // WGS84-ish reduced latitude is enough for a split heuristic; the
    // pruning proof uses the metric-provided lower bound, not this centroid.
    let beta = f64::atan((1.0 - 1.0 / 298.257_223_563) * lat.tan())
        .clamp(-std::f64::consts::FRAC_PI_2, std::f64::consts::FRAC_PI_2);
    [beta.cos() * lon.cos(), beta.cos() * lon.sin(), beta.sin()]
}
