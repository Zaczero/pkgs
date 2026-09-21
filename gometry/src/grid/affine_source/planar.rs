//! Exact planar classification for a lifted ring.
//!
//! Once a ring is lifted onto a planar copy, membership is decided by
//! orientation predicates over exact expansions: bucket the edges a query
//! can see, then ask each for the side it falls on. The filtered predicate
//! answers first when its error bound excludes zero, and only an uncertain
//! sign pays for the exact determinant.

use std::cmp::Ordering;

use super::exact_expansion::{ExactExpansion, ExactFailure, ExactRatio, ExactResult};
use super::{
    DirectPreimages, EdgeId, EdgeYIndex, GridPointClass, LiftedChain, LiftedVertex,
    ORIENTATION_ERRBOUND, PeriodicParityBuckets, PeriodicPoleRoof, PhysicalEndpointKey,
    direct_preimages_for_envelope,
};

#[derive(Debug)]
pub(super) struct ExactPlanarPoint {
    pub(super) longitude: ExactExpansion,
    pub(super) latitude: ExactExpansion,
}

#[derive(Clone, Copy)]
pub(super) struct ExactPlanarPointRef<'a> {
    pub(super) longitude: &'a ExactExpansion,
    pub(super) latitude: &'a ExactExpansion,
}

pub(super) trait ExactPlanarCoordinates {
    fn longitude(&self) -> &ExactExpansion;
    fn latitude(&self) -> &ExactExpansion;
}

impl ExactPlanarCoordinates for ExactPlanarPoint {
    fn longitude(&self) -> &ExactExpansion {
        &self.longitude
    }

    fn latitude(&self) -> &ExactExpansion {
        &self.latitude
    }
}

impl ExactPlanarCoordinates for ExactPlanarPointRef<'_> {
    fn longitude(&self) -> &ExactExpansion {
        self.longitude
    }

    fn latitude(&self) -> &ExactExpansion {
        self.latitude
    }
}

pub(super) struct ShiftedExactPlanarPoint<'a> {
    pub(super) longitude: ExactExpansion,
    pub(super) latitude: &'a ExactExpansion,
}

impl ExactPlanarCoordinates for ShiftedExactPlanarPoint<'_> {
    fn longitude(&self) -> &ExactExpansion {
        &self.longitude
    }

    fn latitude(&self) -> &ExactExpansion {
        self.latitude
    }
}

/// A selection-image endpoint can be a rational strip/reflection split.  It
/// stays rational until the exact PIP determinant consumes it; collapsing it
/// to a double would lose the t=1/3 identity this carrier was built to retain.
#[derive(Debug)]
pub(super) struct ExactSelectionPoint {
    pub(super) longitude: ExactRatio,
    pub(super) latitude: ExactRatio,
}

impl ExactPlanarPoint {
    pub(super) fn from_stored(longitude: f64, latitude: f64) -> ExactResult<Self> {
        Ok(Self {
            longitude: ExactExpansion::from_f64(longitude)?,
            latitude: ExactExpansion::from_f64(latitude)?,
        })
    }

    pub(super) fn from_lifted(vertex: LiftedVertex) -> ExactResult<Self> {
        let longitude = ExactExpansion::from_f64(vertex.longitude.raw)?;
        let turns = ExactExpansion::exact_degree_turns(vertex.longitude.turns)?;
        Ok(Self {
            longitude: longitude.add(&turns)?,
            latitude: ExactExpansion::from_f64(vertex.latitude)?,
        })
    }
}

pub(super) fn shifted_exact_planar_point<Q: ExactPlanarCoordinates + ?Sized>(
    point: &Q,
    turns: i64,
) -> ExactResult<ShiftedExactPlanarPoint<'_>> {
    let offset = ExactExpansion::exact_degree_turns(turns)?;
    Ok(ShiftedExactPlanarPoint {
        longitude: point.longitude().add(&offset)?,
        latitude: point.latitude(),
    })
}

pub(super) fn classify_lifted_ring_bucket(
    chain: &LiftedChain,
    y_index: Option<&EdgeYIndex>,
    roof: Option<&PeriodicPoleRoof>,
    query: &ExactPlanarPoint,
    copies: std::ops::RangeInclusive<i64>,
) -> ExactResult<GridPointClass> {
    let mut buckets = match PeriodicParityBuckets::new(copies, chain.edges.len()) {
        Ok(buckets) => buckets,
        Err(ExactFailure::Arithmetic) => return Ok(GridPointClass::Unknown),
    };
    if let Some(y_index) = y_index {
        let Some(query_y) = query.latitude.as_f64() else {
            return Ok(GridPointClass::Unknown);
        };
        let mut terminal = None;
        let mut visit =
            |edge: EdgeId| match bucket_lifted_chain_edge(chain, edge, query, &mut buckets) {
                BucketEdge::Continue => std::ops::ControlFlow::Continue(()),
                BucketEdge::Decisive(result) => {
                    terminal = Some(result);
                    std::ops::ControlFlow::Break(())
                },
                BucketEdge::Uncertain => {
                    terminal = Some(GridPointClass::Unknown);
                    std::ops::ControlFlow::Break(())
                },
            };
        if let Some(edges) = y_index.dense_band_edges(query_y) {
            for &edge in edges {
                if visit(edge).is_break() {
                    break;
                }
            }
        } else {
            let _ = y_index.for_each_edge(query_y, visit);
        }
        if let Some(result) = terminal {
            return Ok(result);
        }
    } else {
        let mut previous = match chain.vertices.first().copied() {
            Some(vertex) => match ExactPlanarPoint::from_lifted(vertex) {
                Ok(point) => point,
                Err(ExactFailure::Arithmetic) => return Ok(GridPointClass::Unknown),
            },
            None => return Ok(GridPointClass::Unknown),
        };
        for vertex in &chain.vertices[1..] {
            let current = match ExactPlanarPoint::from_lifted(*vertex) {
                Ok(point) => point,
                Err(ExactFailure::Arithmetic) => return Ok(GridPointClass::Unknown),
            };
            match bucket_lifted_edge(&previous, &current, query, false, &mut buckets) {
                BucketEdge::Decisive(result) => return Ok(result),
                BucketEdge::Continue => {},
                BucketEdge::Uncertain => return Ok(GridPointClass::Unknown),
            }
            previous = current;
        }
    }
    let previous = match chain.vertices.last().copied() {
        Some(vertex) => match ExactPlanarPoint::from_lifted(vertex) {
            Ok(point) => point,
            Err(ExactFailure::Arithmetic) => return Ok(GridPointClass::Unknown),
        },
        None => return Ok(GridPointClass::Unknown),
    };
    let first = match chain.vertices.first().copied() {
        Some(vertex) => match ExactPlanarPoint::from_lifted(vertex) {
            Ok(point) => point,
            Err(ExactFailure::Arithmetic) => return Ok(GridPointClass::Unknown),
        },
        None => return Ok(GridPointClass::Unknown),
    };
    if chain.vertices.len() < 2 {
        return Ok(GridPointClass::Unknown);
    }
    if let Some(roof) = roof {
        let pole_latitude = match roof.pole {
            PhysicalEndpointKey::NorthPole => Some(ExactExpansion::from_i64(90)?),
            PhysicalEndpointKey::SouthPole => Some(ExactExpansion::from_i64(-90)?),
            PhysicalEndpointKey::CellVertex(_) => None,
        };
        let Some(pole_latitude) = pole_latitude else {
            return Ok(GridPointClass::Unknown);
        };
        let end_pole = ExactPlanarPointRef {
            longitude: &previous.longitude,
            latitude: &pole_latitude,
        };
        let start_pole = ExactPlanarPointRef {
            longitude: &first.longitude,
            latitude: &pole_latitude,
        };
        for (start, end) in [
            (
                &previous as &dyn ExactPlanarCoordinates,
                &end_pole as &dyn ExactPlanarCoordinates,
            ),
            (
                &end_pole as &dyn ExactPlanarCoordinates,
                &start_pole as &dyn ExactPlanarCoordinates,
            ),
            (
                &start_pole as &dyn ExactPlanarCoordinates,
                &first as &dyn ExactPlanarCoordinates,
            ),
        ] {
            match bucket_lifted_edge(start, end, query, true, &mut buckets) {
                BucketEdge::Decisive(result) => return Ok(result),
                BucketEdge::Continue => {},
                BucketEdge::Uncertain => return Ok(GridPointClass::Unknown),
            }
        }
    }
    if buckets.any_interior() {
        Ok(GridPointClass::Interior)
    } else {
        Ok(GridPointClass::Exterior)
    }
}

pub(super) fn bucket_lifted_chain_edge(
    chain: &LiftedChain,
    edge: EdgeId,
    query: &ExactPlanarPoint,
    buckets: &mut PeriodicParityBuckets,
) -> BucketEdge {
    let edge = edge.as_usize();
    let Some(next) = edge.checked_add(1) else {
        return BucketEdge::Uncertain;
    };
    let start = match chain.vertices.get(edge).copied() {
        Some(vertex) => match ExactPlanarPoint::from_lifted(vertex) {
            Ok(point) => point,
            Err(ExactFailure::Arithmetic) => return BucketEdge::Uncertain,
        },
        None => return BucketEdge::Uncertain,
    };
    let end = match chain.vertices.get(next).copied() {
        Some(vertex) => match ExactPlanarPoint::from_lifted(vertex) {
            Ok(point) => point,
            Err(ExactFailure::Arithmetic) => return BucketEdge::Uncertain,
        },
        None => return BucketEdge::Uncertain,
    };
    bucket_lifted_edge(&start, &end, query, false, buckets)
}

/// One exact edge's impact on the periodic parity sweep.  `Uncertain` is
/// intentionally distinct from a boundary result: neither may establish a
/// negative, but the caller preserves the former as a failed proof.
pub(super) enum BucketEdge {
    Continue,
    Decisive(GridPointClass),
    Uncertain,
}

pub(super) fn bucket_lifted_edge<
    S: ExactPlanarCoordinates + ?Sized,
    E: ExactPlanarCoordinates + ?Sized,
    Q: ExactPlanarCoordinates + ?Sized,
>(
    start: &S,
    end: &E,
    query: &Q,
    artificial: bool,
    buckets: &mut PeriodicParityBuckets,
) -> BucketEdge {
    let start_y = match start.latitude().sub(query.latitude()) {
        Ok(value) => value,
        Err(ExactFailure::Arithmetic) => return BucketEdge::Uncertain,
    };
    let end_y = match end.latitude().sub(query.latitude()) {
        Ok(value) => value,
        Err(ExactFailure::Arithmetic) => return BucketEdge::Uncertain,
    };
    let on_latitude =
        match closed_between_expansion(start.latitude(), query.latitude(), end.latitude()) {
            Ok(value) => value,
            Err(ExactFailure::Arithmetic) => return BucketEdge::Uncertain,
        };
    if on_latitude {
        let delta = match start.longitude().sub(end.longitude()) {
            Ok(delta) => delta,
            Err(ExactFailure::Arithmetic) => return BucketEdge::Uncertain,
        };
        let (minimum, maximum) = if delta.ordering() == Ordering::Greater {
            (end.longitude(), start.longitude())
        } else {
            (start.longitude(), end.longitude())
        };
        match direct_preimages_for_envelope(minimum, maximum, query.longitude()) {
            DirectPreimages::Empty => {},
            DirectPreimages::Uncertain => {
                return BucketEdge::Decisive(GridPointClass::Unknown);
            },
            DirectPreimages::Copies(copies) => {
                let Some(count) = copies
                    .end()
                    .checked_sub(*copies.start())
                    .and_then(|value| value.checked_add(1))
                else {
                    return BucketEdge::Uncertain;
                };
                // A shortest/full original edge can meet at most two seam
                // spellings.  The artificial horizontal roof is excluded by
                // the non-pole query contract before reaching this branch.
                if count > 2 {
                    return BucketEdge::Decisive(GridPointClass::Unknown);
                }
                for copy in copies {
                    let shifted = match shifted_exact_planar_point(query, copy) {
                        Ok(shifted) => shifted,
                        Err(ExactFailure::Arithmetic) => return BucketEdge::Uncertain,
                    };
                    let orientation = match planar_orientation_ordering(start, end, &shifted) {
                        Ok(orientation) => orientation,
                        Err(ExactFailure::Arithmetic) => return BucketEdge::Uncertain,
                    };
                    let on_longitude = match closed_between_expansion(
                        start.longitude(),
                        &shifted.longitude,
                        end.longitude(),
                    ) {
                        Ok(on_longitude) => on_longitude,
                        Err(ExactFailure::Arithmetic) => return BucketEdge::Uncertain,
                    };
                    if orientation == Ordering::Equal && on_longitude {
                        return BucketEdge::Decisive(if artificial {
                            GridPointClass::Unknown
                        } else {
                            GridPointClass::Boundary
                        });
                    }
                }
            },
        }
    }
    let start_cmp = start_y.ordering();
    let end_cmp = end_y.ordering();
    let crosses = (start_cmp != Ordering::Greater && end_cmp == Ordering::Greater)
        || (end_cmp != Ordering::Greater && start_cmp == Ordering::Greater);
    if !crosses {
        return BucketEdge::Continue;
    }
    let orientation = match planar_orientation(start, end, query) {
        Ok(orientation) => orientation,
        Err(ExactFailure::Arithmetic) => return BucketEdge::Uncertain,
    };
    let delta_y = match end.latitude().sub(start.latitude()) {
        Ok(delta_y) => delta_y,
        Err(ExactFailure::Arithmetic) => return BucketEdge::Uncertain,
    };
    if delta_y.ordering() == Ordering::Equal {
        return BucketEdge::Continue;
    }
    let ratio = match ExactRatio::from_parts(orientation, delta_y) {
        Ok(ratio) => ratio,
        Err(ExactFailure::Arithmetic) => return BucketEdge::Uncertain,
    };
    let bucket = match ratio.floor_turn() {
        Ok(bucket) => bucket,
        Err(ExactFailure::Arithmetic) => return BucketEdge::Uncertain,
    };
    if buckets.add_crossing(bucket).is_none() {
        return BucketEdge::Uncertain;
    }
    BucketEdge::Continue
}

/// Exact ray contribution of one rational selection-image segment.  Selection
/// endpoints are frequently generated at rational strip cuts (notably t=1/3),
/// so this deliberately mirrors `bucket_lifted_edge` in `ExactRatio` space
/// instead of recovering a rounded degree endpoint.
pub(super) fn bucket_selection_edge(
    start: &ExactSelectionPoint,
    end: &ExactSelectionPoint,
    query: &ExactPlanarPoint,
    buckets: &mut PeriodicParityBuckets,
) -> BucketEdge {
    let query_longitude = match ExactRatio::from_expansion(&query.longitude) {
        Ok(value) => value,
        Err(ExactFailure::Arithmetic) => return BucketEdge::Uncertain,
    };
    let query_latitude = match ExactRatio::from_expansion(&query.latitude) {
        Ok(value) => value,
        Err(ExactFailure::Arithmetic) => return BucketEdge::Uncertain,
    };
    let start_y = match start.latitude.sub(&query_latitude) {
        Ok(value) => value,
        Err(ExactFailure::Arithmetic) => return BucketEdge::Uncertain,
    };
    let end_y = match end.latitude.sub(&query_latitude) {
        Ok(value) => value,
        Err(ExactFailure::Arithmetic) => return BucketEdge::Uncertain,
    };
    let (start_cmp, end_cmp) = (start_y.numerator.ordering(), end_y.numerator.ordering());

    // A query on an original selected edge is closed contact.  Test the three
    // only physical sheet preimages individually so exact +/-180 spellings
    // cannot be normalized into an unrelated line.
    let on_latitude =
        match selection_closed_between(&start.latitude, &query_latitude, &end.latitude) {
            Ok(value) => value,
            Err(ExactFailure::Arithmetic) => return BucketEdge::Uncertain,
        };
    if on_latitude {
        for turns in -1_i64..=1_i64 {
            let offset = match turns.checked_mul(360) {
                Some(value) => match ExactRatio::from_i64(value, 1) {
                    Ok(offset) => offset,
                    Err(ExactFailure::Arithmetic) => return BucketEdge::Uncertain,
                },
                None => return BucketEdge::Uncertain,
            };
            let shifted_longitude = match query_longitude.add(&offset) {
                Ok(value) => value,
                Err(ExactFailure::Arithmetic) => return BucketEdge::Uncertain,
            };
            let orientation =
                match selection_orientation_at(start, end, &shifted_longitude, &query_latitude) {
                    Ok(orientation) => orientation,
                    Err(ExactFailure::Arithmetic) => return BucketEdge::Uncertain,
                };
            let on_longitude = match selection_closed_between(
                &start.longitude,
                &shifted_longitude,
                &end.longitude,
            ) {
                Ok(on_longitude) => on_longitude,
                Err(ExactFailure::Arithmetic) => return BucketEdge::Uncertain,
            };
            if orientation.numerator.ordering() == Ordering::Equal && on_longitude {
                return BucketEdge::Decisive(GridPointClass::Boundary);
            }
        }
    }

    let crosses = (start_cmp != Ordering::Greater && end_cmp == Ordering::Greater)
        || (end_cmp != Ordering::Greater && start_cmp == Ordering::Greater);
    if !crosses {
        return BucketEdge::Continue;
    }
    let orientation = match selection_orientation_at(start, end, &query_longitude, &query_latitude)
    {
        Ok(orientation) => orientation,
        Err(ExactFailure::Arithmetic) => return BucketEdge::Uncertain,
    };
    let delta_y = match end.latitude.sub(&start.latitude) {
        Ok(delta_y) => delta_y,
        Err(ExactFailure::Arithmetic) => return BucketEdge::Uncertain,
    };
    let ratio = match orientation.div(&delta_y) {
        Ok(ratio) => ratio,
        Err(ExactFailure::Arithmetic) => return BucketEdge::Uncertain,
    };
    let bucket = match ratio.floor_turn() {
        Ok(bucket) => bucket,
        Err(ExactFailure::Arithmetic) => return BucketEdge::Uncertain,
    };
    if buckets.add_crossing(bucket).is_none() {
        return BucketEdge::Uncertain;
    }
    BucketEdge::Continue
}

pub(super) fn selection_orientation_at(
    start: &ExactSelectionPoint,
    end: &ExactSelectionPoint,
    point_longitude: &ExactRatio,
    point_latitude: &ExactRatio,
) -> ExactResult<ExactRatio> {
    let dx = end.longitude.sub(&start.longitude)?;
    let dy = end.latitude.sub(&start.latitude)?;
    let px = point_longitude.sub(&start.longitude)?;
    let py = point_latitude.sub(&start.latitude)?;
    let left = dx.mul(&py)?;
    let right = dy.mul(&px)?;
    left.sub(&right)
}

pub(crate) fn selection_closed_between(
    left: &ExactRatio,
    middle: &ExactRatio,
    right: &ExactRatio,
) -> ExactResult<bool> {
    let left_middle = left.ordering(middle)?;
    let middle_right = middle.ordering(right)?;
    Ok(closed_between_orderings(left_middle, middle_right))
}

/// Exact even-odd point location in the finite lifted fill.  This direct
/// per-copy form is deliberately private to the carrier proof; the traversal
/// installs the design's linear bucket walk before it calls this at leaves.
/// Until then no public path can acquire its potentially wider `Unknown`.
#[cfg(test)]
pub(super) fn classify_lifted_ring_copy<Q: ExactPlanarCoordinates + ?Sized>(
    chain: &LiftedChain,
    roof: Option<&PeriodicPoleRoof>,
    query: &Q,
) -> GridPointClass {
    let Some(mut previous) = chain
        .vertices
        .first()
        .copied()
        .and_then(|vertex| ExactPlanarPoint::from_lifted(vertex).ok())
    else {
        return GridPointClass::Unknown;
    };
    let mut parity = false;
    for vertex in &chain.vertices[1..] {
        let Some(current) = ExactPlanarPoint::from_lifted(*vertex).ok() else {
            return GridPointClass::Unknown;
        };
        match classify_lifted_edge(&previous, &current, query, false, &mut parity) {
            GridPointClass::Exterior => {},
            result => return result,
        }
        previous = current;
    }
    if let Some(roof) = roof {
        let Some(first) = chain
            .vertices
            .first()
            .copied()
            .and_then(|vertex| ExactPlanarPoint::from_lifted(vertex).ok())
        else {
            return GridPointClass::Unknown;
        };
        let pole_latitude = match roof.pole {
            PhysicalEndpointKey::NorthPole => ExactExpansion::from_i64(90).ok(),
            PhysicalEndpointKey::SouthPole => ExactExpansion::from_i64(-90).ok(),
            PhysicalEndpointKey::CellVertex(_) => None,
        };
        let Some(pole_latitude) = pole_latitude else {
            return GridPointClass::Unknown;
        };
        let end_pole = ExactPlanarPointRef {
            longitude: &previous.longitude,
            latitude: &pole_latitude,
        };
        let start_pole = ExactPlanarPointRef {
            longitude: &first.longitude,
            latitude: &pole_latitude,
        };
        for (start, end) in [
            (
                &previous as &dyn ExactPlanarCoordinates,
                &end_pole as &dyn ExactPlanarCoordinates,
            ),
            (
                &end_pole as &dyn ExactPlanarCoordinates,
                &start_pole as &dyn ExactPlanarCoordinates,
            ),
            (
                &start_pole as &dyn ExactPlanarCoordinates,
                &first as &dyn ExactPlanarCoordinates,
            ),
        ] {
            match classify_lifted_edge(start, end, query, true, &mut parity) {
                GridPointClass::Exterior => {},
                result => return result,
            }
        }
    }
    if parity {
        GridPointClass::Interior
    } else {
        GridPointClass::Exterior
    }
}

/// Returns `Boundary` only for an original source edge.  A point on the
/// artificial pole roof is not a source-boundary proof and therefore stays
/// `Unknown`, preserving the authority/selection separation.
#[cfg(test)]
pub(super) fn classify_lifted_edge<
    S: ExactPlanarCoordinates + ?Sized,
    E: ExactPlanarCoordinates + ?Sized,
    Q: ExactPlanarCoordinates + ?Sized,
>(
    start: &S,
    end: &E,
    query: &Q,
    artificial: bool,
    parity: &mut bool,
) -> GridPointClass {
    let Some(orientation) = planar_orientation_ordering(start, end, query).ok() else {
        return GridPointClass::Unknown;
    };
    let (Some(start_y), Some(end_y)) = (
        start.latitude().sub(query.latitude()).ok(),
        end.latitude().sub(query.latitude()).ok(),
    ) else {
        return GridPointClass::Unknown;
    };
    if orientation == Ordering::Equal
        && closed_between_expansion(start.longitude(), query.longitude(), end.longitude())
            .ok()
            .unwrap_or(false)
        && closed_between_expansion(start.latitude(), query.latitude(), end.latitude())
            .ok()
            .unwrap_or(false)
    {
        return if artificial {
            GridPointClass::Unknown
        } else {
            GridPointClass::Boundary
        };
    }
    let start_cmp = start_y.ordering();
    let end_cmp = end_y.ordering();
    let crosses = (start_cmp != Ordering::Greater && end_cmp == Ordering::Greater)
        || (end_cmp != Ordering::Greater && start_cmp == Ordering::Greater);
    if !crosses {
        return GridPointClass::Exterior;
    }
    let Some(delta_y) = end.latitude().sub(start.latitude()).ok() else {
        return GridPointClass::Unknown;
    };
    if delta_y.ordering() == Ordering::Equal {
        return GridPointClass::Exterior;
    }
    // x_cross - qx is orientation / (end.y - start.y).  Cross only when the
    // two exact signs agree; no division or rounded ray intersection exists.
    if orientation == delta_y.ordering() {
        *parity = !*parity;
    }
    GridPointClass::Exterior
}

pub(super) fn closed_between_orderings(left_middle: Ordering, middle_right: Ordering) -> bool {
    left_middle == Ordering::Equal || middle_right == Ordering::Equal || left_middle == middle_right
}

pub(super) fn closed_between_expansion(
    left: &ExactExpansion,
    middle: &ExactExpansion,
    right: &ExactExpansion,
) -> ExactResult<bool> {
    let left_middle = left.sub(middle)?.ordering();
    let middle_right = middle.sub(right)?.ordering();
    Ok(closed_between_orderings(left_middle, middle_right))
}

pub(super) fn closed_between_planar_points<
    L: ExactPlanarCoordinates + ?Sized,
    M: ExactPlanarCoordinates + ?Sized,
    R: ExactPlanarCoordinates + ?Sized,
>(
    left: &L,
    middle: &M,
    right: &R,
) -> ExactResult<bool> {
    let longitude =
        closed_between_expansion(left.longitude(), middle.longitude(), right.longitude())?;
    let latitude = closed_between_expansion(left.latitude(), middle.latitude(), right.latitude())?;
    Ok(longitude && latitude)
}

pub(super) fn planar_orientation<
    S: ExactPlanarCoordinates + ?Sized,
    E: ExactPlanarCoordinates + ?Sized,
    Q: ExactPlanarCoordinates + ?Sized,
>(
    start: &S,
    end: &E,
    query: &Q,
) -> ExactResult<ExactExpansion> {
    let dx = end.longitude().sub(start.longitude())?;
    let dy = end.latitude().sub(start.latitude())?;
    let qx = query.longitude().sub(start.longitude())?;
    let qy = query.latitude().sub(start.latitude())?;
    let left = dx.product(&qy)?;
    let right = dy.product(&qx)?;
    left.sub(&right)
}

/// Certify the ordinary one-limb case before constructing expansion products.
/// Every arithmetic result must be normal and finite, so neither overflow nor
/// underflow can invalidate the binary64 relative-error bound.
pub(super) fn filtered_planar_orientation<
    S: ExactPlanarCoordinates + ?Sized,
    E: ExactPlanarCoordinates + ?Sized,
    Q: ExactPlanarCoordinates + ?Sized,
>(
    start: &S,
    end: &E,
    query: &Q,
) -> Option<Ordering> {
    let (start_x, start_y, end_x, end_y, query_x, query_y) = (
        start.longitude().as_f64()?,
        start.latitude().as_f64()?,
        end.longitude().as_f64()?,
        end.latitude().as_f64()?,
        query.longitude().as_f64()?,
        query.latitude().as_f64()?,
    );
    let dx = end_x - start_x;
    let dy = end_y - start_y;
    let qx = query_x - start_x;
    let qy = query_y - start_y;
    let left = dx * qy;
    let right = dy * qx;
    let determinant = left - right;
    let magnitude = left.abs() + right.abs();
    if ![dx, dy, qx, qy, left, right, determinant, magnitude]
        .into_iter()
        .all(|value| value == 0.0 || value.is_normal())
    {
        return None;
    }
    let bound = (magnitude * ORIENTATION_ERRBOUND).next_up();
    (determinant.abs() > bound).then(|| determinant.total_cmp(&0.0))
}

pub(super) fn planar_orientation_ordering<
    S: ExactPlanarCoordinates + ?Sized,
    E: ExactPlanarCoordinates + ?Sized,
    Q: ExactPlanarCoordinates + ?Sized,
>(
    start: &S,
    end: &E,
    query: &Q,
) -> ExactResult<Ordering> {
    if let Some(ordering) = filtered_planar_orientation(start, end, query) {
        Ok(ordering)
    } else {
        Ok(planar_orientation(start, end, query)?.ordering())
    }
}
