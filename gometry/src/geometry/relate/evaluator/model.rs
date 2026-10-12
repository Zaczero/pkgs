use std::cell::OnceCell;

use super::contacts::stored_exact_point;
use super::points::point_from_key;
use super::{
    Cell, ExactDyadic, ExactPointForest, GeometryErrorKind, HashMap, Loc, OccupiedToward, Point,
    PointKey, PointMembership, PreparedPlan, PreparedPointQueries, Result, Segment, ShapeRow,
    SourcePointLocation, StagedExactSources, StagedSourceComponent, XY,
    cartesian_row_exact_offset_location_and_area, cartesian_row_exact_point_location_and_area,
    cartesian_row_point_location, cartesian_row_point_location_and_area, homogeneous_midpoint,
    matrix_predicates, occupied_toward,
};
use crate::geometry::segments::crossing_homogeneous_stored;

pub(super) struct Operand<'a> {
    pub(super) row: ShapeRow<'a>,
    pub(super) plan: Option<&'a PreparedPlan>,
    pub(super) point_queries: Cell<Option<PreparedPointQueries<'a>>>,
}

impl<'a> Operand<'a> {
    pub(super) fn from_row(row: ShapeRow<'a>) -> Self {
        let plan = match row {
            ShapeRow::Handle(plan) => Some(plan.as_ref()),
            ShapeRow::Shape(_) | ShapeRow::Point(_) | ShapeRow::Packed(_) => None,
        };
        Self {
            row,
            plan,
            point_queries: Cell::new(None),
        }
    }

    pub(super) fn from_plan(plan: &'a PreparedPlan) -> Self {
        Self {
            row: ShapeRow::Shape(plan.shape()),
            plan: Some(plan),
            point_queries: Cell::new(None),
        }
    }

    pub(super) fn bounds(&self) -> Option<crate::geometry::Bounds> {
        self.plan
            .map_or_else(|| self.row.quick_bounds(), |plan| plan.bounds())
    }

    pub(super) fn locate_stored(&self, point: Point) -> Result<SourcePointLocation> {
        self.point_queries()?.map_or_else(
            || {
                self.plan.map_or_else(
                    || Ok(cartesian_row_point_location_and_area(self.row, point)),
                    |plan| plan.locate_stored(point),
                )
            },
            |queries| queries.locate_stored(point),
        )
    }

    pub(super) fn locate_exact(&self, point: &[ExactDyadic; 3]) -> Result<SourcePointLocation> {
        self.point_queries()?.map_or_else(
            || {
                self.plan.map_or_else(
                    || cartesian_row_exact_point_location_and_area(self.row, point),
                    |plan| plan.locate_exact(point),
                )
            },
            |queries| queries.locate_exact(point),
        )
    }

    pub(super) fn locate_exact_offset(
        &self,
        point: &[ExactDyadic; 3],
        direction: &[ExactDyadic; 2],
    ) -> Result<SourcePointLocation> {
        self.point_queries()?.map_or_else(
            || {
                self.plan.map_or_else(
                    || cartesian_row_exact_offset_location_and_area(self.row, point, direction),
                    |plan| plan.locate_exact_offset(point, direction),
                )
            },
            |queries| queries.locate_exact_offset(point, direction),
        )
    }

    pub(super) fn occupied_toward_exact(
        &self,
        point: &[ExactDyadic; 3],
        direction: &[ExactDyadic; 2],
    ) -> Result<OccupiedToward> {
        self.point_queries()?.map_or_else(
            || {
                self.plan.map_or_else(
                    || {
                        self.locate_exact_offset(point, direction)
                            .map(occupied_toward)
                    },
                    |plan| plan.occupied_toward_exact(point, direction),
                )
            },
            |queries| {
                queries
                    .locate_exact_offset(point, direction)
                    .map(occupied_toward)
            },
        )
    }

    pub(super) fn point_queries(&self) -> Result<Option<PreparedPointQueries<'a>>> {
        if let Some(queries) = self.point_queries.get() {
            return Ok(Some(queries));
        }
        let Some(plan) = self.plan else {
            return Ok(None);
        };
        let Some(queries) = plan.prepared_point_queries()? else {
            return Ok(None);
        };
        self.point_queries.set(Some(queries));
        Ok(Some(queries))
    }

    pub(super) fn point_membership(
        &self,
        point: Point,
        membership: PointMembership,
    ) -> Result<bool> {
        if let Some(plan) = self.plan {
            return plan.cartesian_point_membership(point, membership);
        }
        let location = match membership {
            PointMembership::BoundaryInclusive => {
                cartesian_row_point_location(self.row, point, true)
            },
            PointMembership::Strict => cartesian_row_point_location(self.row, point, false),
        };
        Ok(match membership {
            PointMembership::Strict => location == Loc::Interior,
            PointMembership::BoundaryInclusive => location != Loc::Exterior,
        })
    }
}

pub(super) const fn by_operand<T>(values: &[T; 2], operand: matrix_predicates::Operand) -> &T {
    let [left, right] = values;
    match operand {
        matrix_predicates::Operand::Left => left,
        matrix_predicates::Operand::Right => right,
    }
}

pub(super) const fn by_operand_mut<T>(
    values: &mut [T; 2],
    operand: matrix_predicates::Operand,
) -> &mut T {
    let [left, right] = values;
    match operand {
        matrix_predicates::Operand::Left => left,
        matrix_predicates::Operand::Right => right,
    }
}

pub(super) const fn opposite_operand(
    operand: matrix_predicates::Operand,
) -> matrix_predicates::Operand {
    match operand {
        matrix_predicates::Operand::Left => matrix_predicates::Operand::Right,
        matrix_predicates::Operand::Right => matrix_predicates::Operand::Left,
    }
}

#[derive(Clone)]
pub(super) struct Component {
    pub(super) source: StagedSourceComponent,
    pub(super) side: matrix_predicates::Operand,
    pub(super) dirty: bool,
    pub(super) seed: Option<SourcePointLocation>,
    pub(super) left_area: Option<bool>,
}

pub(super) fn staged_operand(operand: u32) -> Result<matrix_predicates::Operand> {
    match operand {
        0 => Ok(matrix_predicates::Operand::Left),
        1 => Ok(matrix_predicates::Operand::Right),
        _ => Err(GeometryErrorKind::Invalid(
            "binary relation staging produced a non-pair operand".into(),
        )
        .into()),
    }
}

pub(super) struct StagedGeometry {
    pub(super) sources: StagedExactSources,
    pub(super) components: Vec<Component>,
    pub(super) segment_component: Vec<u32>,
    pub(super) operand_has_area: [bool; 2],
    pub(super) operand_has_ring: [bool; 2],
}

#[derive(Clone, Copy, Default)]
pub(super) struct SourceCertificates {
    pub(super) nonzero_line: bool,
    pub(super) point: bool,
    pub(super) hole_free_area: bool,
}

#[derive(Clone, Copy)]
pub(super) enum EventPointKind {
    Stored(XY),
    Crossing { segments: [u32; 2], rounded: XY },
}

pub(super) struct EventPoint {
    pub(super) kind: EventPointKind,
    pub(super) exact: Option<Box<[ExactDyadic; 3]>>,
    pub(super) handle: Option<u32>,
}

#[derive(Clone, Copy)]
pub(super) enum EventKind {
    Touch,
    Cross,
    Collinear { same_direction: bool },
}

pub(super) struct ContactEvent {
    pub(super) segments: [usize; 2],
    pub(super) components: [u32; 2],
    pub(super) kind: EventKind,
    pub(super) points: [Option<EventPoint>; 2],
}

#[derive(Clone, Copy)]
pub(super) struct Coverage {
    pub(super) partner_component: u32,
    pub(super) same_direction: bool,
}

#[derive(Clone, Copy)]
pub(super) enum AtomPoint {
    Endpoint(XY),
    Event(u32),
}

#[derive(Default)]
pub(super) struct ExactCoordinates {
    pub(super) stored: HashMap<PointKey, [ExactDyadic; 3]>,
}

impl ExactCoordinates {
    pub(super) fn stored(&mut self, point: XY) -> &[ExactDyadic; 3] {
        self.stored.entry(PointKey::new(point)).or_insert_with(|| {
            #[cfg(all(test, debug_assertions))]
            update_atom_metrics(|metrics| {
                metrics.stored_vertices = metrics.stored_vertices.wrapping_add(1);
            });
            stored_exact_point(point)
        })
    }

    pub(super) fn point(
        &mut self,
        point: AtomPoint,
        forest: &ExactPointForest<'_>,
    ) -> Result<[ExactDyadic; 3]> {
        match point {
            AtomPoint::Endpoint(point) => Ok(self.stored(point).clone()),
            AtomPoint::Event(handle) => {
                if let Some(key) = forest.points().stored_key(handle) {
                    Ok(self.stored(point_from_key(key).xy()).clone())
                } else {
                    Ok(forest.points().exact(handle)?.as_ref().clone())
                }
            },
        }
    }

    pub(super) fn direction(&mut self, segment: Segment) -> Result<[ExactDyadic; 2]> {
        let [x, y, _] = self.stored(segment.start).clone();
        let [end_x, end_y, _] = self.stored(segment.end);
        Ok([end_x.try_subtract_ref(&x)?, end_y.try_subtract_ref(&y)?])
    }

    pub(super) fn crossing(&mut self, left: Segment, right: Segment) -> Result<[ExactDyadic; 3]> {
        let a_start = self.stored(left.start).clone();
        let a_end = self.stored(left.end).clone();
        let b_start = self.stored(right.start).clone();
        let b_end = self.stored(right.end);
        crossing_homogeneous_stored(&a_start, &a_end, &b_start, b_end)
    }
}

pub(super) struct ExactAtom<'a> {
    pub(super) start: AtomPoint,
    pub(super) end: AtomPoint,
    pub(super) start_exact: OnceCell<[ExactDyadic; 3]>,
    pub(super) midpoint: OnceCell<[ExactDyadic; 3]>,
    pub(super) segment: Segment,
    pub(super) direction: &'a OnceCell<[ExactDyadic; 2]>,
}

impl ExactAtom<'_> {
    pub(super) fn offset<'a>(
        &'a self,
        coordinates: &mut ExactCoordinates,
        forest: &ExactPointForest<'_>,
    ) -> Result<(&'a [ExactDyadic; 3], &'a [ExactDyadic; 2])> {
        let start = self
            .start_exact
            .get_or_try_init(|| coordinates.point(self.start, forest))?;
        let direction = self.direction.get_or_try_init(|| {
            #[cfg(all(test, debug_assertions))]
            update_atom_metrics(|metrics| metrics.directions = metrics.directions.wrapping_add(1));
            coordinates.direction(self.segment)
        })?;
        Ok((start, direction))
    }

    pub(super) fn side_probe<'a>(
        &'a self,
        coordinates: &mut ExactCoordinates,
        forest: &ExactPointForest<'_>,
    ) -> Result<(&'a [ExactDyadic; 3], &'a [ExactDyadic; 2])> {
        let (start, direction) = self.offset(coordinates, forest)?;
        let midpoint = self.midpoint.get_or_try_init(|| {
            let end = coordinates.point(self.end, forest)?;
            #[cfg(all(test, debug_assertions))]
            update_atom_metrics(|metrics| metrics.midpoints = metrics.midpoints.wrapping_add(1));
            homogeneous_midpoint(start, &end)
        })?;
        Ok((midpoint, direction))
    }
}

#[cfg(all(test, debug_assertions))]
#[derive(Clone, Copy, Default, Debug)]
pub(super) struct AtomMetrics {
    pub(super) stored_vertices: usize,
    pub(super) directions: usize,
    pub(super) midpoints: usize,
    pub(super) node_locations: usize,
}

#[cfg(all(test, debug_assertions))]
thread_local! {
    pub(super) static ATOM_METRICS: std::cell::Cell<AtomMetrics> = const {
        std::cell::Cell::new(AtomMetrics { stored_vertices: 0, directions: 0, midpoints: 0, node_locations: 0 })
    };
}

#[cfg(all(test, debug_assertions))]
pub(super) fn update_atom_metrics(update: impl FnOnce(&mut AtomMetrics)) {
    ATOM_METRICS.with(|cell| {
        let mut metrics = cell.get();
        update(&mut metrics);
        cell.set(metrics);
    });
}
