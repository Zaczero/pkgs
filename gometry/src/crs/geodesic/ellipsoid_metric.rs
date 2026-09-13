use geographiclib_rs::Geodesic;

use crate::crs::geodesic;
use crate::crs::geodesic::segment::{
    PreparedSegment, geodesic_curvature_domain, longitude_difference,
};
use crate::crs::geodesic::{
    DirectGeodesic, GEODESIC_BACKEND_RELATIVE_FLOOR, IntersectSolver, InverseGeodesic as _,
    LowerBoundKernel, cached_lower_bound_kernel, checked_geodesic_segment,
    geodesic_point_segment_query, geodesic_segment_intersection_with_solver, geodesic_unit_vector,
    interpolate_optional_ordinate,
};
use crate::geographic::GeographicFrame;
use crate::geometry::{
    GeodesicMetric, GeodesicPreparedSegment, GeodesicSegment, GeodesicSegmentDerivative,
    GeodesicSegmentQuery, GeodesicSegmentQueryResult, GeodesicSegmentSample,
    GeodesicSegmentScratch, MOrdinate, Point, XY, ZOrdinate,
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
    /// Lazily built AND per-thread cached: the lower-bound tabulation is ~150
    /// µs of trig and is read ONLY by the foot-finding pruner
    /// ([`Self::point_distance_lower_bound`]). Point-to-point and other
    /// non-pruning paths never touch it; repeated foot-finding on one ellipsoid
    /// reuses the cached kernel (a cheap `Rc` clone) instead of rebuilding it.
    lower_bound: std::cell::OnceCell<std::rc::Rc<LowerBoundKernel>>,
    intersection: std::cell::OnceCell<IntersectSolver>,
}

#[derive(Clone, Copy)]
pub(crate) struct PreparedMetricSegment {
    source_segment: GeodesicSegment,
    inner: PreparedSegment,
    frame: GeographicFrame,
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
        let sample = self.inner.sample(point, along, derivative)?;
        Ok(GeodesicSegmentSample {
            derivative: sample
                .derivative
                .map(|derivative| GeodesicSegmentDerivative {
                    value: derivative.value,
                    error: derivative.error,
                }),
        })
    }

    fn position(&self, along: f64) -> crate::error::Result<Point> {
        let point = self.inner.position(along)?;
        Ok(self
            .frame
            .point_from_geodesic(point)
            .expect("geodesic metric produced a finite point"))
    }

    fn tangent(&self, along: f64) -> crate::error::Result<Option<[f64; 3]>> {
        self.inner.tangent(along)
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
            lower_bound: std::cell::OnceCell::new(),
            intersection: std::cell::OnceCell::new(),
        }
    }

    pub(crate) const fn ellipsoid_parameters(&self) -> (f64, f64) {
        (self.geodesic.a, self.geodesic.f)
    }

    pub(crate) const fn frame(&self) -> GeographicFrame {
        self.frame
    }

    fn to_geodesic_point(&self, point: Point) -> Point {
        self.frame
            .to_geodesic_point(point)
            .expect("geographic metric received an admitted point")
    }

    #[expect(
        clippy::large_types_passed_by_value,
        reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow"
    )]
    fn to_geodesic_segment(&self, segment: GeodesicSegment) -> GeodesicSegment {
        GeodesicSegment {
            start: self.to_geodesic_point(segment.start),
            end: self.to_geodesic_point(segment.end),
            ..segment
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
            self.geodesic,
            self.to_geodesic_point(start),
            self.to_geodesic_point(end),
        )?;
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
        let geodesic_segment = self.to_geodesic_segment(*segment);
        Ok(PreparedMetricSegment {
            source_segment: *segment,
            inner: PreparedSegment::new(self.geodesic, &geodesic_segment),
            frame: self.frame,
        })
    }

    fn point_segment_query(
        &self,
        point: Point,
        segment: GeodesicSegment,
        query: GeodesicSegmentQuery,
        scratch: &mut GeodesicSegmentScratch,
    ) -> crate::error::Result<GeodesicSegmentQueryResult> {
        let result = geodesic_point_segment_query(
            self.geodesic,
            self.to_geodesic_point(point),
            self.to_geodesic_segment(segment),
            query,
            scratch,
        )?;
        Ok(match result {
            GeodesicSegmentQueryResult::Attained { minimum, foot } => {
                GeodesicSegmentQueryResult::Attained {
                    minimum,
                    foot: foot.map(|foot| {
                        self.frame
                            .point_from_geodesic(foot)
                            .expect("geodesic witness is finite")
                    }),
                }
            },
            GeodesicSegmentQueryResult::Excluded
            | GeodesicSegmentQueryResult::ThresholdOutside
            | GeodesicSegmentQueryResult::ThresholdResolvedOutside { .. } => result,
        })
    }

    fn segments_cross(&self, a: Point, b: Point, c: Point, d: Point) -> crate::error::Result<bool> {
        Ok(self.segment_intersection(a, b, c, d)?.is_some())
    }

    fn segment_intersection(
        &self,
        a: Point,
        b: Point,
        c: Point,
        d: Point,
    ) -> crate::error::Result<Option<(Point, Point)>> {
        let first = geodesic::checked_geodesic_segment(
            self.geodesic,
            self.to_geodesic_point(a),
            self.to_geodesic_point(b),
        )?;
        let second = geodesic::checked_geodesic_segment(
            self.geodesic,
            self.to_geodesic_point(c),
            self.to_geodesic_point(d),
        )?;
        if self.intersection.get().is_none() {
            let solver = IntersectSolver::new(self.geodesic)?;
            let _ = self.intersection.set(solver);
        }
        let solver = self
            .intersection
            .get()
            .expect("intersection solver is initialized");
        Ok(
            geodesic_segment_intersection_with_solver(self.geodesic, solver, first, second)?.map(
                |(first, second)| {
                    (
                        self.frame
                            .point_from_geodesic(first)
                            .expect("geodesic witness is finite"),
                        self.frame
                            .point_from_geodesic(second)
                            .expect("geodesic witness is finite"),
                    )
                },
            ),
        )
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
        let (length, azimuth, _) = geo_distance_azimuths(self.geodesic, a_geodesic, b_geodesic);
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
            geo_direct(self.geodesic, a_geodesic, azimuth, length * fraction)
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

    fn point_distance_lower_bound(&self, a: Point, b: Point) -> f64 {
        self.lower_bound
            .get_or_init(|| cached_lower_bound_kernel(self.geodesic))
            .bound(
                self.geodesic,
                self.to_geodesic_point(a),
                self.to_geodesic_point(b),
            )
    }

    fn geodesic_curvature_domain(&self) -> Option<(f64, f64, f64)> {
        geodesic_curvature_domain(self.geodesic)
    }

    fn geodesic_backend_floor(&self) -> f64 {
        self.geodesic.a.abs() * GEODESIC_BACKEND_RELATIVE_FLOOR
    }

    fn geodesic_radius(&self) -> f64 {
        self.geodesic.a
    }

    fn surface_unit(&self, point: Point) -> Option<[f64; 3]> {
        let point = self.to_geodesic_point(point);
        Some(geodesic_unit_vector(point.x, point.y))
    }

    fn is_spherical(&self) -> bool {
        self.geodesic.f == 0.0
    }

    fn is_equatorial_segment(&self, segment: &GeodesicSegment) -> bool {
        let segment = self.to_geodesic_segment(*segment);
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
        let segment = self.to_geodesic_segment(*segment);
        let target = self.to_geodesic_point(target);
        if segment.start.y != 0.0 || segment.end.y != 0.0 {
            return None;
        }
        let delta = longitude_difference(segment.start.x, segment.end.x);
        if !(delta != 0.0 && delta.abs() < 180.0) {
            return None;
        }
        let opposite_delta = longitude_difference(segment.start.x, target.x + 180.0);
        Some(segment.length * opposite_delta.abs() / delta.abs()).filter(|along| along.is_finite())
    }
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
