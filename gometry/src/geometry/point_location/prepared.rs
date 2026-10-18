//! Demand admission and stable handles for exact source point queries.

use std::convert::Infallible;
use std::ops::ControlFlow;
use std::sync::OnceLock;

use super::{ExactLocationIndex, PointBatchTester, SourceLocationLabels};
use crate::HeapSize;
use crate::error::Result;
#[cfg(test)]
use crate::geometry::Loc;
use crate::geometry::overlay::ExactSourceInventory;
use crate::geometry::source_support::{
    PointMembership, SourcePointLocation, cartesian_exact_offset_location_and_area,
    cartesian_exact_point_location_and_area, cartesian_point_location_and_area, exact_stored_point,
};
use crate::geometry::tessellation::exact::ExactDyadic;
use crate::geometry::{MetricSupportRun, Point, RunIdentity, Shape};

fn compact_coordinate_count(shape: &Shape) -> usize {
    let mut coordinates = 0_usize;
    let ControlFlow::Continue(()): ControlFlow<Infallible> =
        shape.for_each_first_support_run(RunIdentity::Full, |run| {
            let count = match run {
                MetricSupportRun::Point(_) => 1,
                MetricSupportRun::MultiPoint(points) => points.len(),
                MetricSupportRun::SegmentChain(coords) => coords.len(),
            };
            coordinates = coordinates.saturating_add(count);
            ControlFlow::Continue(())
        });
    coordinates
}

pub(in crate::geometry) struct PointQueryPlan {
    rings: PointBatchTester,
    admission: Option<Box<SlabAdmission>>,
}

struct SlabAdmission {
    slab: OnceLock<ExactLocationIndex<SourceLocationLabels>>,
    after: usize,
    demand: std::sync::atomic::AtomicUsize,
}

impl PointQueryPlan {
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "coordinate bit width is at most 64 and candidate_work is at least one"
    )]
    pub(in crate::geometry) fn new(shape: &Shape) -> Result<Option<Self>> {
        const BOUNDED_QUERY_WORK: usize = 1024;
        const SMALL_POINT_INDEX_OPERAND: usize = 16;

        // Slab labels need per-occurrence line-endpoint parity, and staging a shared DAG is exponential.
        if shape.has_repeated_shared_owner() {
            return Ok(None);
        }
        let coordinates = compact_coordinate_count(shape);
        if coordinates <= SMALL_POINT_INDEX_OPERAND {
            return Ok(None);
        }

        let rings = PointBatchTester::new(shape)?;
        let work = rings.query_work_bound();
        let admission = if work.is_some_and(|work| work <= BOUNDED_QUERY_WORK) {
            None
        } else {
            // Charge candidate-pair noding as well as slab sorting before
            // replacing repeated ring queries with a complete arrangement.
            let candidate_work = work.unwrap_or(coordinates).max(1);
            let build_work = coordinates.saturating_mul(
                candidate_work
                    .saturating_mul(4)
                    .saturating_add((coordinates.ilog2() as usize + 1) * 32),
            );
            Some(Box::new(SlabAdmission {
                slab: OnceLock::new(),
                after: (build_work / candidate_work).max(128),
                demand: std::sync::atomic::AtomicUsize::new(0),
            }))
        };
        Ok(Some(Self { rings, admission }))
    }

    pub(in crate::geometry) fn locate_stored(
        &self,
        shape: &Shape,
        point: Point,
    ) -> Result<SourcePointLocation> {
        if self.admission.is_none() {
            return Ok(self.rings.location_and_area(point));
        }
        self.resolve(shape, 1)?.locate_stored(point)
    }

    pub(in crate::geometry) fn membership_xy(
        &self,
        shape: &Shape,
        x: f64,
        y: f64,
        membership: PointMembership,
    ) -> Result<bool> {
        if self.admission.is_none() {
            return Ok(self.rings.membership_xy(x, y, membership));
        }
        self.adaptive_membership(shape, Point::new_raw_xy(x, y), membership)
    }

    fn adaptive_membership(
        &self,
        shape: &Shape,
        point: Point,
        membership: PointMembership,
    ) -> Result<bool> {
        self.resolve(shape, 1)?.membership(point, membership)
    }

    pub(in crate::geometry) fn resolve<'a>(
        &'a self,
        shape: &'a Shape,
        expected_queries: usize,
    ) -> Result<PreparedPointQueries<'a>> {
        let Some(admission) = &self.admission else {
            return Ok(PreparedPointQueries(PointQueryRoute::Rings {
                rings: &self.rings,
                demand: None,
            }));
        };
        if let Some(index) = admission.slab.get() {
            return Ok(PreparedPointQueries(PointQueryRoute::Slabs(index)));
        }
        let observed = admission.demand.load(std::sync::atomic::Ordering::Relaxed);
        if observed.saturating_add(expected_queries) >= admission.after {
            let index = admission.slab.get_or_try_init(|| {
                let inventory = ExactSourceInventory::from_shapes_with_occurrences(&[shape])?;
                let partition = inventory.partition_without_budget()?;
                Ok::<_, crate::error::Error>(ExactLocationIndex::from_cells(
                    inventory.source_location_cells(&partition)?,
                ))
            })?;
            return Ok(PreparedPointQueries(PointQueryRoute::Slabs(index)));
        }
        Ok(PreparedPointQueries(PointQueryRoute::Rings {
            rings: &self.rings,
            demand: Some(&admission.demand),
        }))
    }
}

#[derive(Clone, Copy)]
enum PointQueryRoute<'a> {
    Streaming(&'a Shape),
    Rings {
        rings: &'a PointBatchTester,
        demand: Option<&'a std::sync::atomic::AtomicUsize>,
    },
    Slabs(&'a ExactLocationIndex<SourceLocationLabels>),
}

#[derive(Clone, Copy)]
pub(crate) struct PreparedPointQueries<'a>(PointQueryRoute<'a>);

impl<'a> PreparedPointQueries<'a> {
    pub(in crate::geometry) const fn streaming(shape: &'a Shape) -> Self {
        Self(PointQueryRoute::Streaming(shape))
    }

    #[cfg(test)]
    pub(crate) fn location(&self, point: Point) -> Result<Loc> {
        self.locate_stored(point).map(|location| location.location)
    }

    pub(crate) fn covers(&self, point: Point) -> Result<bool> {
        self.membership(point, PointMembership::BoundaryInclusive)
    }

    pub(crate) fn membership(&self, point: Point, membership: PointMembership) -> Result<bool> {
        self.record_query();
        match self.0 {
            PointQueryRoute::Rings { rings, .. } => {
                Ok(rings.membership_xy(point.x, point.y, membership))
            },
            PointQueryRoute::Streaming(shape) => match membership {
                PointMembership::Strict => {
                    Ok(membership.accepts(crate::geometry::cartesian_point_location(shape, point)))
                },
                PointMembership::BoundaryInclusive => {
                    Ok(crate::geometry::cartesian_point_covers(shape, point))
                },
            },
            PointQueryRoute::Slabs(index) => index
                .locate_xy(point.x, point.y)
                .map(|location| membership.accepts(location.location)),
        }
    }

    pub(in crate::geometry) fn locate_stored(&self, point: Point) -> Result<SourcePointLocation> {
        self.record_query();
        match self.0 {
            PointQueryRoute::Streaming(shape) => {
                Ok(cartesian_point_location_and_area(shape, point))
            },
            PointQueryRoute::Rings { rings, .. } => Ok(rings.location_and_area(point)),
            PointQueryRoute::Slabs(index) => index.locate_xy(point.x, point.y),
        }
    }

    pub(in crate::geometry) fn locate_exact(
        &self,
        point: &[ExactDyadic; 3],
    ) -> Result<SourcePointLocation> {
        self.record_query();
        match self.0 {
            PointQueryRoute::Slabs(index) => index.locate_exact(point),
            PointQueryRoute::Streaming(shape) => {
                if let Some(point) = exact_stored_point(point) {
                    return Ok(cartesian_point_location_and_area(shape, point));
                }
                cartesian_exact_point_location_and_area(shape, point)
            },
            PointQueryRoute::Rings { rings, .. } => {
                if let Some(point) = exact_stored_point(point) {
                    return Ok(rings.location_and_area(point));
                }
                rings.location_and_area_exact(point)?.map_or_else(
                    || match rings {
                        PointBatchTester::Generic(tester) => {
                            cartesian_exact_point_location_and_area(&tester.shape, point)
                        },
                        PointBatchTester::Polygon { .. }
                        | PointBatchTester::MultiPolygon { .. } => unreachable!(
                            "polygonal exact queries resolve contacts through indexed rings"
                        ),
                    },
                    Ok,
                )
            },
        }
    }

    pub(in crate::geometry) fn locate_exact_offset(
        &self,
        point: &[ExactDyadic; 3],
        direction: &[ExactDyadic; 2],
    ) -> Result<SourcePointLocation> {
        self.record_query();
        match self.0 {
            PointQueryRoute::Slabs(index) => index.locate_exact_offset(point, direction),
            PointQueryRoute::Streaming(shape) => {
                cartesian_exact_offset_location_and_area(shape, point, direction)
            },
            PointQueryRoute::Rings { rings, .. } => rings
                .location_and_area_exact_offset(point, direction)?
                .map_or_else(
                    || match rings {
                        PointBatchTester::Generic(tester) => {
                            cartesian_exact_offset_location_and_area(
                                &tester.shape,
                                point,
                                direction,
                            )
                        },
                        PointBatchTester::Polygon { .. }
                        | PointBatchTester::MultiPolygon { .. } => unreachable!(
                            "polygonal exact offsets resolve contacts through indexed rings"
                        ),
                    },
                    Ok,
                ),
        }
    }

    fn record_query(&self) {
        if let PointQueryRoute::Rings {
            demand: Some(demand),
            ..
        } = self.0
        {
            demand
                .try_update(
                    std::sync::atomic::Ordering::Relaxed,
                    std::sync::atomic::Ordering::Relaxed,
                    |count| Some(count.saturating_add(1)),
                )
                .expect("the demand update always supplies a value");
        }
    }

    #[cfg(test)]
    pub(in crate::geometry) const fn uses_indexed_plan(&self) -> bool {
        match self.0 {
            PointQueryRoute::Streaming(_) => false,
            PointQueryRoute::Rings { rings, .. } => rings.uses_indexed_plan(),
            PointQueryRoute::Slabs(_) => true,
        }
    }

    #[cfg(test)]
    pub(in crate::geometry) const fn uses_slabs(&self) -> bool {
        matches!(self.0, PointQueryRoute::Slabs(_))
    }
}

impl HeapSize for PointQueryPlan {
    fn heap_bytes(&self) -> usize {
        self.rings
            .heap_bytes()
            .saturating_add(self.admission.heap_bytes())
    }
}

impl HeapSize for SlabAdmission {
    fn heap_bytes(&self) -> usize {
        self.slab.heap_bytes()
    }
}
