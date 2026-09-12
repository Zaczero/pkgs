use geographiclib_rs::Geodesic;

use crate::crs::geodesic;
use crate::crs::geodesic::{
    DirectGeodesic, IntersectSolver, InverseGeodesic as _, LowerBoundKernel,
    cached_lower_bound_kernel, checked_geodesic_segment, geodesic_foot_on_segment,
    geodesic_locate_on_segment, geodesic_point_segment_sample_below, geodesic_point_to_segment,
    geodesic_point_within_segment, geodesic_segment_intersection_with_solver,
    interpolate_optional_ordinate,
};
use crate::geographic::GeographicFrame;
use crate::geometry::{
    GeodesicMetric, GeodesicSegment, GeodesicSegmentWitness, MOrdinate, Point, XY, ZOrdinate,
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

    fn point_to_segment(
        &self,
        point: Point,
        segment: GeodesicSegment,
        best: f64,
    ) -> crate::error::Result<f64> {
        geodesic_point_to_segment(
            self.geodesic,
            self.to_geodesic_point(point),
            self.to_geodesic_segment(segment),
            best,
        )
    }

    fn point_to_segment_within(
        &self,
        point: Point,
        segment: GeodesicSegment,
        limit: f64,
    ) -> crate::error::Result<bool> {
        geodesic_point_within_segment(
            self.geodesic,
            self.to_geodesic_point(point),
            self.to_geodesic_segment(segment),
            limit,
        )
    }

    fn point_to_segment_sample_below(
        &self,
        point: Point,
        segment: GeodesicSegment,
        limit: f64,
    ) -> crate::error::Result<Option<f64>> {
        geodesic_point_segment_sample_below(
            self.geodesic,
            self.to_geodesic_point(point),
            self.to_geodesic_segment(segment),
            limit,
        )
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

    fn locate_on_segment(
        &self,
        point: Point,
        segment: GeodesicSegment,
        best: f64,
    ) -> crate::error::Result<(f64, f64)> {
        geodesic_locate_on_segment(
            self.geodesic,
            self.to_geodesic_point(point),
            self.to_geodesic_segment(segment),
            best,
        )
    }

    fn point_segment_witness(
        &self,
        point: Point,
        segment: GeodesicSegment,
        best: f64,
    ) -> crate::error::Result<GeodesicSegmentWitness> {
        let witness = geodesic_foot_on_segment(
            self.geodesic,
            self.to_geodesic_point(point),
            self.to_geodesic_segment(segment),
            best,
        )?;
        Ok(GeodesicSegmentWitness {
            foot: self
                .frame
                .point_from_geodesic(witness.foot)
                .expect("geodesic witness is finite"),
            ..witness
        })
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
