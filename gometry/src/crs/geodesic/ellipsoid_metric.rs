use geographiclib_rs::Geodesic;

use crate::crs::geodesic::{
    DirectGeodesic, IntersectSolver, InverseGeodesic, LowerBoundKernel, cached_lower_bound_kernel,
    checked_geodesic_segment, geodesic_foot_on_segment, geodesic_locate_on_segment,
    geodesic_point_to_segment, geodesic_point_within_segment,
    geodesic_segment_intersection_with_solver, interpolate_optional_ordinate,
};
use crate::geometry::{
    GeodesicMetric, GeodesicSegment, GeodesicSegmentWitness, MOrdinate, Point, ZOrdinate,
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
    runtime_generation: u64,
    /// Lazily built AND per-thread cached: the lower-bound tabulation is ~150
    /// µs of trig and is read ONLY by the foot-finding pruner
    /// ([`Self::point_distance_lower_bound`]). Point-to-point and other
    /// non-pruning paths never touch it; repeated foot-finding on one ellipsoid
    /// reuses the cached kernel (a cheap `Rc` clone) instead of rebuilding it.
    lower_bound: std::cell::OnceCell<std::rc::Rc<LowerBoundKernel>>,
    intersection: std::cell::OnceCell<IntersectSolver>,
}

impl<'a> EllipsoidMetric<'a> {
    #[cfg(test)]
    pub(crate) const fn for_geodesic(geodesic: &'a Geodesic) -> Self {
        Self::for_geodesic_at(geodesic, 0)
    }

    pub(crate) const fn for_geodesic_at(geodesic: &'a Geodesic, runtime_generation: u64) -> Self {
        Self {
            geodesic,
            runtime_generation,
            lower_bound: std::cell::OnceCell::new(),
            intersection: std::cell::OnceCell::new(),
        }
    }

    pub(crate) const fn ellipsoid_parameters(&self) -> (f64, f64) {
        (self.geodesic.a, self.geodesic.f)
    }
}

impl GeodesicMetric for EllipsoidMetric<'_> {
    fn runtime_generation(&self) -> u64 {
        self.runtime_generation
    }

    fn make_segment(&self, start: Point, end: Point) -> crate::error::Result<GeodesicSegment> {
        checked_geodesic_segment(self.geodesic, start, end)
    }

    fn point_to_segment(
        &self,
        point: Point,
        segment: GeodesicSegment,
        best: f64,
    ) -> crate::error::Result<f64> {
        geodesic_point_to_segment(self.geodesic, point, segment, best)
    }

    fn point_to_segment_within(
        &self,
        point: Point,
        segment: GeodesicSegment,
        limit: f64,
    ) -> crate::error::Result<bool> {
        geodesic_point_within_segment(self.geodesic, point, segment, limit)
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
        if !(-0.25..=0.2).contains(&self.geodesic.f) {
            return Err(crate::crs::CrsError::message(
                "geodesic segment intersection is supported only for flattening in [-1/4, 1/5]",
            ));
        }
        let first = crate::crs::geodesic::checked_geodesic_segment(self.geodesic, a, b)?;
        let second = crate::crs::geodesic::checked_geodesic_segment(self.geodesic, c, d)?;
        if self.intersection.get().is_none() {
            let solver = IntersectSolver::new(self.geodesic)?;
            let _ = self.intersection.set(solver);
        }
        let solver = self
            .intersection
            .get()
            .expect("intersection solver is initialized");
        geodesic_segment_intersection_with_solver(self.geodesic, solver, first, second)
    }

    fn segment_length(&self, a: Point, b: Point) -> f64 {
        // Distance-only capability: this is the point-distance kernel of
        // every geodesic point lane — the azimuth grade is pure waste here.
        geo_distance(self.geodesic, a, b)
    }

    fn interpolate(&self, a: Point, b: Point, fraction: f64) -> Point {
        let (length, azimuth, _) = geo_distance_azimuths(self.geodesic, a, b);
        // Lat/lon-only direct — final azimuth is unused.
        // The equator is itself a geodesic. Keep its exact Cartesian longitude
        // ramp when the endpoints do not take the antimeridian shortcut; this
        // preserves exact legacy coordinates without changing the curve model.
        #[expect(
            clippy::float_cmp,
            reason = "the equator is the literal zero-latitude geodesic"
        )]
        let (lat, lon): (f64, f64) = if a.y == 0.0 && b.y == 0.0 && (b.x - a.x).abs() < 180.0 {
            (0.0, crate::geometry::interpolate_f64(a.x, b.x, fraction))
        } else {
            geo_direct(self.geodesic, a, azimuth, length * fraction)
        };
        Point::new_axes(
            lon,
            lat,
            ZOrdinate(interpolate_optional_ordinate(a.z(), b.z(), fraction)),
            MOrdinate(interpolate_optional_ordinate(a.m(), b.m(), fraction)),
        )
        .unwrap_or(a)
    }

    fn locate_on_segment(
        &self,
        point: Point,
        segment: GeodesicSegment,
        best: f64,
    ) -> crate::error::Result<(f64, f64)> {
        geodesic_locate_on_segment(self.geodesic, point, segment, best)
    }

    fn point_segment_witness(
        &self,
        point: Point,
        segment: GeodesicSegment,
        best: f64,
    ) -> crate::error::Result<GeodesicSegmentWitness> {
        geodesic_foot_on_segment(self.geodesic, point, segment, best)
    }

    fn point_distance_lower_bound(&self, a: Point, b: Point) -> f64 {
        self.lower_bound
            .get_or_init(|| cached_lower_bound_kernel(self.geodesic))
            .bound(self.geodesic, a, b)
    }
}
