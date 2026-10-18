use super::{
    Bounds, ExactDyadic, OnceLock, Point, PointMembership, PointQueryPlan, PreparedPlan,
    PreparedPointQueries, Result, Shape, ShapeData, SourcePointLocation,
    cartesian_point_location_and_area,
};
#[cfg(test)]
use super::{OccupiedToward, occupied_toward};
use crate::geometry::source_support::ExactProbe;

fn point_outside_bounds(bounds: Option<Bounds>, point: Point) -> bool {
    bounds.is_none_or(|bounds| {
        point.x < bounds.minx()
            || point.x > bounds.maxx()
            || point.y < bounds.miny()
            || point.y > bounds.maxy()
    })
}

const POINT_SCANS_BEFORE_INDEXING: u32 = 3;

impl PreparedPlan {
    pub(in crate::geometry) fn locate_probe(
        &self,
        probe: &ExactProbe<'_>,
    ) -> Result<SourcePointLocation> {
        if let Some(point) = probe.stored() {
            if self
                .shape
                .bounds
                .get()
                .is_some_and(|bounds| point_outside_bounds(*bounds, point))
            {
                return Ok(SourcePointLocation {
                    location: crate::geometry::Loc::Exterior,
                    area: false,
                });
            }
            return self.locate_stored(point);
        }
        self.prepare_point_queries(1)?.locate_probe(probe)
    }

    pub(in crate::geometry) fn locate_probe_offset(
        &self,
        probe: &ExactProbe<'_>,
        direction: &[ExactDyadic; 2],
    ) -> Result<SourcePointLocation> {
        self.prepare_point_queries(1)?
            .locate_probe_offset(probe, direction)
    }
    /// Exact Cartesian location, admitted by this operand's accumulated demand.
    pub(crate) fn cartesian_point_location(&self, point: Point) -> Result<crate::geometry::Loc> {
        self.locate_stored(point).map(|location| location.location)
    }

    pub(in crate::geometry) fn locate_stored(&self, point: Point) -> Result<SourcePointLocation> {
        if let Some(product) = self
            .topology
            .get()
            .and_then(|topology| topology.point_queries.get())
        {
            return product.as_ref().map_or_else(
                || Ok(cartesian_point_location_and_area(self.shape(), point)),
                |product| product.locate_stored(self.shape(), point),
            );
        }
        self.prepare_point_queries(1)?.locate_stored(point)
    }

    #[cfg(test)]
    pub(in crate::geometry) fn locate_exact(
        &self,
        point: &[ExactDyadic; 3],
    ) -> Result<SourcePointLocation> {
        self.locate_probe(&ExactProbe::new(point))
    }

    /// Locate from a borrowed source, deferring its owned plan until a stored
    /// exact point survives the source bounds already available to the caller.
    pub(in crate::geometry) fn locate_exact_lazy(
        cached_plan: &OnceLock<Self>,
        shape: &Shape,
        bounds: Option<Bounds>,
        point: &[ExactDyadic; 3],
    ) -> Result<SourcePointLocation> {
        let probe = ExactProbe::new(point);
        if probe
            .stored()
            .is_some_and(|stored| point_outside_bounds(bounds, stored))
        {
            return Ok(SourcePointLocation {
                location: crate::geometry::Loc::Exterior,
                area: false,
            });
        }
        let plan = cached_plan.get_or_init(|| {
            let data = ShapeData::new(shape.clone());
            data.seed_bounds(bounds);
            Self::new(data)
        });
        plan.locate_probe(&probe)
    }

    #[cfg(test)]
    pub(in crate::geometry) fn locate_exact_offset(
        &self,
        point: &[ExactDyadic; 3],
        direction: &[ExactDyadic; 2],
    ) -> Result<SourcePointLocation> {
        self.prepare_point_queries(1)?
            .locate_exact_offset(point, direction)
    }

    #[cfg(test)]
    pub(in crate::geometry) fn occupied_toward_exact(
        &self,
        point: &[ExactDyadic; 3],
        direction: &[ExactDyadic; 2],
    ) -> Result<OccupiedToward> {
        self.locate_exact_offset(point, direction)
            .map(occupied_toward)
    }

    /// Resolve once per fixed-target operation. Query counts are advisory;
    /// the handle remains usable beyond them without rebuilding or truncation.
    pub(crate) fn prepare_point_queries(
        &self,
        expected_queries: usize,
    ) -> Result<PreparedPointQueries<'_>> {
        if expected_queries == 0
            && self
                .topology
                .get()
                .and_then(|topology| topology.point_queries.get())
                .is_none()
        {
            return Ok(PreparedPointQueries::streaming(self.shape()));
        }
        let topology = self.topology();
        if let Some(product) = topology.point_queries.get() {
            return product.as_ref().map_or_else(
                || Ok(PreparedPointQueries::streaming(self.shape())),
                |product| product.resolve(self.shape(), expected_queries),
            );
        }
        if expected_queries > POINT_SCANS_BEFORE_INDEXING as usize
            || topology
                .point_scans
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                >= POINT_SCANS_BEFORE_INDEXING
        {
            let product = topology.point_queries.get_or_try_init(|| -> Result<_> {
                PointQueryPlan::new(self.shape()).map(|product| product.map(Box::new))
            })?;
            product.as_ref().map_or_else(
                || Ok(PreparedPointQueries::streaming(self.shape())),
                |product| product.resolve(self.shape(), expected_queries),
            )
        } else {
            Ok(PreparedPointQueries::streaming(self.shape()))
        }
    }

    pub(in crate::geometry) fn prepared_point_queries(
        &self,
    ) -> Result<Option<PreparedPointQueries<'_>>> {
        if self
            .topology
            .get()
            .and_then(|topology| topology.point_queries.get())
            .is_none()
        {
            return Ok(None);
        }
        self.prepare_point_queries(0).map(Some)
    }

    fn point_membership_without_bounds(
        &self,
        point: Point,
        membership: PointMembership,
    ) -> Result<bool> {
        if let Some(product) = self
            .topology
            .get()
            .and_then(|topology| topology.point_queries.get())
        {
            return product.as_ref().map_or_else(
                || PreparedPointQueries::streaming(self.shape()).membership(point, membership),
                |product| product.membership_xy(self.shape(), point.x, point.y, membership),
            );
        }
        self.prepare_point_queries(1)?.membership(point, membership)
    }

    pub(crate) fn cartesian_point_membership(
        &self,
        point: Point,
        membership: PointMembership,
    ) -> Result<bool> {
        if self
            .bounds()
            .is_none_or(|bounds| !bounds.contains_point(point.xy()))
        {
            return Ok(false);
        }
        self.point_membership_without_bounds(point, membership)
    }

    #[cfg(all(test, debug_assertions))]
    pub(crate) fn point_location_scans(&self) -> u32 {
        self.topology()
            .point_scans
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub(crate) fn with_seeded_bounds(self, bounds: Option<Bounds>) -> Self {
        self.shape.seed_bounds(bounds);
        self
    }
}
