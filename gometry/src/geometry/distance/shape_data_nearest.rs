use crate::geometry::distance::{
    Result, geodesic_nearest_points_with_parts, nearest_probe_to_parts, parts_boundary_witness,
    quick_area_overlap, squared_space_safe,
};
use crate::geometry::{
    FrameDependentCaches, GeodesicMetric, GeodesicPartsKey, Point, ShapeData, same_point,
    squared_norm_is_trustworthy,
};
impl ShapeData {
    /// Nearest point pair on cached prepared state — argmin vertex sweeps
    /// (the disjoint-pair minimum is always a vertex projection; see
    /// [`Self::distance`]) with an exact intersection witness for touching/
    /// crossing operands. Falls back to the brute shape path when a side
    /// mixes Z/M presence across chains (the packed columns cannot
    /// reconstruct faithful ordinates there). `None` only when an operand is
    /// empty (the surfaces map it to the output-type EMPTY sentinel).
    pub fn nearest_points(&self, other: &Self) -> Option<(Point, Point)> {
        // Disjoint boxes cannot intersect: the witness scans only run when
        // the bounds overlap (see `distance`). The pre-parts containment
        // probe answers the common area-overlap case before any staging.
        let overlapping = self
            .bounds()
            .zip(other.bounds())
            .is_some_and(|(left, right)| left.intersects(right));
        if overlapping && let Some(witness) = quick_area_overlap(self.shape(), other.shape()) {
            return Some((witness, witness));
        }
        let self_parts = self.distance_parts();
        let other_parts = other.distance_parts();
        // The argmin sweeps run in squared space (no hypot rescue), so
        // extreme-coordinate operands take the brute path alongside
        // mixed-ordinate linework. Tie identity (which of several equally
        // near pairs is returned) is deliberately unspecified, like
        // shapely's.
        if !self_parts.linework.uniform_ordinates()
            || !other_parts.linework.uniform_ordinates()
            || !squared_space_safe(self_parts)
            || !squared_space_safe(other_parts)
        {
            return self.shape().nearest_points(other.shape());
        }
        if overlapping
            && let Some(witness) =
                parts_boundary_witness(self.shape(), self_parts, other.shape(), other_parts)
        {
            return Some((witness, witness));
        }
        let best = nearest_probe_to_parts(self_parts, other_parts, None, false);
        let best = nearest_probe_to_parts(other_parts, self_parts, best, true);
        // Squared-space ordering cannot distinguish a false-zero or quantized
        // subnormal candidate from another tiny witness. Re-run the rare
        // untrustworthy class through the shape-level hypot reduction, whose
        // segment witness now comes from the certified projection owner.
        if best.as_ref().is_some_and(|oriented| {
            let candidate = &oriented.candidate;
            !squared_norm_is_trustworthy(
                candidate.distance_squared,
                same_point(candidate.probe, candidate.target),
            )
        }) {
            return self.shape().nearest_points(other.shape());
        }
        best.map(|oriented| {
            let candidate = oriented.candidate;
            if oriented.swapped {
                (candidate.target, candidate.probe)
            } else {
                (candidate.probe, candidate.target)
            }
        })
    }

    /// Geodesic nearest-point pair with cached geodesic working state. The
    /// witness tie order is identical to `Shape::geodesic_nearest_points`.
    pub(crate) fn geodesic_nearest_points_cached(
        &self,
        self_cache: &FrameDependentCaches,
        other: &Self,
        other_cache: &FrameDependentCaches,
        crs: &str,
        semi_major: f64,
        flattening: f64,
        metric: &impl GeodesicMetric,
    ) -> Result<Option<(Point, Point)>> {
        let key = GeodesicPartsKey::new(crs, semi_major, flattening);
        let self_parts = self.geodesic_parts(self_cache, key.clone(), metric)?;
        let other_parts = other.geodesic_parts(other_cache, key, metric)?;
        Ok(geodesic_nearest_points_with_parts(
            self.shape(),
            &self_parts,
            other.shape(),
            &other_parts,
            metric,
        ))
    }

    /// [`geodesic_nearest_points_cached`](Self::geodesic_nearest_points_cached)
    /// that first seam-splits whichever operand crosses the antimeridian, so the
    /// prepared geometry×array / array nearest-points fast paths stay
    /// seam-correct (a witness computed on the unsplit 340° band lands on the
    /// wrong edge). The non-crossing operand is borrowed — the common all-rows-
    /// non-crossing case keeps the prepared cache and pays nothing; only a
    /// crossing row allocates a split form.
    pub(crate) fn geodesic_nearest_points_cached_split(
        &self,
        self_cache: &FrameDependentCaches,
        other: &Self,
        other_cache: &FrameDependentCaches,
        crs: &str,
        semi_major: f64,
        flattening: f64,
        metric: &impl GeodesicMetric,
    ) -> Result<Option<(Point, Point)>> {
        if !self.shape().crosses_antimeridian() && !other.shape().crosses_antimeridian() {
            return self.geodesic_nearest_points_cached(
                self_cache,
                other,
                other_cache,
                crs,
                semi_major,
                flattening,
                metric,
            );
        }
        // Borrow the non-crossing operand; only a crossing one allocates a split
        // form (held in these deferred-init locals so the borrows outlive the call).
        let (self_split, other_split);
        let left = if self.shape().crosses_antimeridian() {
            self_split = Self::from(self.shape().split_antimeridian()?);
            &self_split
        } else {
            self
        };
        let right = if other.shape().crosses_antimeridian() {
            other_split = Self::from(other.shape().split_antimeridian()?);
            &other_split
        } else {
            other
        };
        let split_left_cache = FrameDependentCaches::default();
        let split_right_cache = FrameDependentCaches::default();
        let left_cache = if std::ptr::eq(left, self) {
            self_cache
        } else {
            &split_left_cache
        };
        let right_cache = if std::ptr::eq(right, other) {
            other_cache
        } else {
            &split_right_cache
        };
        left.geodesic_nearest_points_cached(
            left_cache,
            right,
            right_cache,
            crs,
            semi_major,
            flattening,
            metric,
        )
    }
}
