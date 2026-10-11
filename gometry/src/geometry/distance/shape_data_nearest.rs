use crate::crs::PointEdgeScratch;
use crate::geographic::ChartStep;
use crate::geometry::distance::{
    Result, cached_chart_nearest_points, cached_chart_nearest_points_with_intent,
    nearest_probe_to_parts, parts_boundary_witness, quick_area_overlap, squared_space_safe,
};
use crate::geometry::facet_bvh::NearestSourceProjection;
use crate::geometry::{
    ChartMetric, CoordinateAxes, FrameDependentCaches, GeodesicMetric, MOrdinate, Point,
    PreparedPlan, Shape, SourceArcContext, TopologyDomain, ZOrdinate,
};

impl PreparedPlan {
    /// Nearest point pair on cached prepared state — argmin vertex sweeps
    /// (the disjoint-pair minimum is always a vertex projection; see
    /// [`Self::distance`]) with an exact intersection witness for touching/
    /// crossing operands. Full-ordinate chain views retain mixed Z/M
    /// presence when reconstructing projected witnesses. `None` only when an operand is
    /// empty (the surfaces map it to the output-type EMPTY sentinel).
    pub(crate) fn nearest_points(&self, other: &Self) -> Result<Option<(Point, Point)>> {
        self.nearest_points_inner(other, None)
    }

    pub(crate) fn nearest_points_with_source_context(
        &self,
        other: &Self,
        source_context: &SourceArcContext<'_, '_, crate::crs::GeodesicMetric>,
    ) -> Result<Option<(Point, Point)>> {
        self.nearest_points_inner(other, Some(source_context))
    }

    fn nearest_points_inner(
        &self,
        other: &Self,
        source_context: Option<&SourceArcContext<'_, '_, crate::crs::GeodesicMetric>>,
    ) -> Result<Option<(Point, Point)>> {
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
        let source_axes = if source_context.is_some_and(|context| context.domain().is_geographic())
        {
            crate::geometry::common_axes(self.shape(), other.shape())
        } else {
            CoordinateAxes::XY
        };
        best.map(|oriented| {
            let candidate = oriented.candidate;
            let (mut target, source_projection) = candidate.target_point(
                if oriented.swapped {
                    &self_parts.linework
                } else {
                    &other_parts.linework
                },
                source_axes,
            )?;
            if let (Some(source_projection), Some(source_context)) =
                (source_projection, source_context)
            {
                target = resolve_nearest_source_projection(
                    target,
                    source_projection,
                    source_axes,
                    source_context,
                )?;
            }
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

    /// Geodesic shortest-line witness together with the longitude step
    /// selected while the source/chart witness is still available.
    pub(crate) fn geodesic_nearest_line_cached<M>(
        &self,
        self_cache: &FrameDependentCaches,
        other: &Self,
        other_cache: &FrameDependentCaches,
        crs: &str,
        semi_major: f64,
        flattening: f64,
        metric: &M,
    ) -> Result<Option<(Point, Point, ChartStep)>>
    where
        M: GeodesicMetric + ChartMetric<PointEdgeScratch = PointEdgeScratch>,
    {
        if let (Shape::Point(left), Shape::Point(right)) = (self.shape(), other.shape()) {
            metric.ensure_shape(self.shape())?;
            metric.ensure_shape(other.shape())?;
            let direction = metric
                .chart_frame()
                .longitude()
                .shortest_alias_step(left.x, right.x)
                .map_err(|error| crate::crs::CrsError::invalid(error.to_string()))?;
            return Ok(Some((*left, *right, ChartStep::Shortest(direction))));
        }
        Ok(cached_chart_nearest_points_with_intent(
            self,
            self_cache,
            other,
            other_cache,
            crs,
            semi_major,
            flattening,
            metric,
        )?
        .map(|minimum| (minimum.first, minimum.second, minimum.intent)))
    }
}

fn resolve_nearest_source_projection(
    point: Point,
    source_projection: NearestSourceProjection,
    axes: CoordinateAxes,
    source_context: &SourceArcContext<'_, '_, crate::crs::GeodesicMetric>,
) -> Result<Point> {
    let TopologyDomain::Geographic(source_frame) = source_context.domain() else {
        return Ok(point);
    };
    let source = [source_projection.start, source_projection.end];
    let parameter = source_projection.projection.exact_fraction()?;
    let request = crate::geometry::ordinates::SourceArcRequest::new(
        source_frame,
        &source,
        axes,
        &parameter,
        || {
            source_frame
                .longitude()
                .chart_connection(source[0].x, source[1].x)
                .map(|connection| connection.retained())
                .map_err(|error| {
                    crate::geometry::GeometryErrorKind::Invalid(error.to_string().into()).into()
                })
        },
    )?;
    let metric_access = source_context.metric_access().ok_or_else(|| {
        crate::crs::CrsError::invalid(
            "geographic nearest-witness ordinates require the operation's CRS metric",
        )
    })?;
    let ordinates =
        crate::geometry::ordinates::source_arc_ordinates_unmetered(&request, || metric_access())?;
    Ok(Point::new_axes(
        point.x,
        point.y,
        ZOrdinate(ordinates.z.value()),
        MOrdinate(ordinates.m.value()),
    )
    .unwrap_or(point))
}
