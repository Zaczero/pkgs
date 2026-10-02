use crate::crs::PointEdgeScratch;
use crate::geometry::distance::{
    Result, cached_chart_nearest_points, nearest_probe_to_parts, parts_boundary_witness,
    quick_area_overlap, squared_space_safe,
};
use crate::geometry::{
    ChartMetric, FrameDependentCaches, GeodesicMetric, Point, PreparedPlan, Shape,
};
impl PreparedPlan {
    /// Nearest point pair on cached prepared state — argmin vertex sweeps
    /// (the disjoint-pair minimum is always a vertex projection; see
    /// [`Self::distance`]) with an exact intersection witness for touching/
    /// crossing operands. Full-ordinate chain views retain mixed Z/M
    /// presence when reconstructing projected witnesses. `None` only when an operand is
    /// empty (the surfaces map it to the output-type EMPTY sentinel).
    pub(crate) fn nearest_points(&self, other: &Self) -> Result<Option<(Point, Point)>> {
        // Disjoint boxes cannot intersect: the witness scans only run when
        // the bounds overlap (see `distance`). The pre-parts containment
        // probe answers the common area-overlap case before any staging.
        let overlapping = self
            .bounds()
            .zip(other.bounds())
            .is_some_and(|(left, right)| left.intersects(right));
        if overlapping && let Some(witness) = quick_area_overlap(self.shape(), other.shape()) {
            return Ok(Some((witness, witness)));
        }
        let self_parts = self.distance_parts()?;
        let other_parts = other.distance_parts()?;
        if overlapping
            && let Some(witness) =
                parts_boundary_witness(self.shape(), self_parts, other.shape(), other_parts)?
        {
            return Ok(Some((witness, witness)));
        }
        let best = if squared_space_safe(self_parts) && squared_space_safe(other_parts) {
            let best = nearest_probe_to_parts::<true>(self_parts, other_parts, None, false)?;
            nearest_probe_to_parts::<true>(other_parts, self_parts, best, true)?
        } else {
            let best = nearest_probe_to_parts::<false>(self_parts, other_parts, None, false)?;
            nearest_probe_to_parts::<false>(other_parts, self_parts, best, true)?
        };
        best.map(|oriented| {
            let candidate = oriented.candidate;
            let target = candidate.target_point(if oriented.swapped {
                &self_parts.linework
            } else {
                &other_parts.linework
            })?;
            Ok(if oriented.swapped {
                (target, candidate.probe)
            } else {
                (candidate.probe, target)
            })
        })
        .transpose()
    }

    /// Geodesic nearest-point pair with cached chart-edge parts and witnesses.
    pub(crate) fn geodesic_nearest_points_cached<M>(
        &self,
        self_cache: &FrameDependentCaches,
        other: &Self,
        other_cache: &FrameDependentCaches,
        crs: &str,
        semi_major: f64,
        flattening: f64,
        metric: &M,
    ) -> Result<Option<(Point, Point)>>
    where
        M: GeodesicMetric + ChartMetric<PointEdgeScratch = PointEdgeScratch>,
    {
        if let (Shape::Point(left), Shape::Point(right)) = (self.shape(), other.shape()) {
            metric.ensure_shape(self.shape())?;
            metric.ensure_shape(other.shape())?;
            return Ok(Some((*left, *right)));
        }
        cached_chart_nearest_points(
            self,
            self_cache,
            other,
            other_cache,
            crs,
            semi_major,
            flattening,
            metric,
        )
    }
}
