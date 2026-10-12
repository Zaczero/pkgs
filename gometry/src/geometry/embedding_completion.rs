//! Admission and completion checks for an already-known embedded graph.
//!
//! Producers own graph selection and stable source identities. This module
//! owns the shared proof that a finite coordinate assignment preserves the
//! admitted graph, its embedding, and the identities of distinct vertices.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::ControlFlow;
#[cfg(test)]
use std::{cell::Cell, thread_local};

use crate::error::{ResourceError, Result};
use crate::geometry::arrangement::{
    IndexedTopology, indexed_edge_slots_with_work, indexed_topology_from_edges_with_work,
};
use crate::geometry::exact_arith::ExactSign;
use crate::geometry::overlay::{
    CertifiedOverlayOrder, GlobalFaces, RayAxis, RayPoints, RayPointsWithWork,
    compare_departures_with_work, graph_global_faces_with_work,
};
use crate::geometry::segments::try_sort_by_with_work;
use crate::geometry::tessellation::exact::ExactRational;
use crate::geometry::topology::{
    ExactChartPoint, exact_segments_intersect_with_work, point_on_exact_segment_with_work,
    vector_cross_with_work, vector_difference_with_work,
};
use crate::geometry::{
    Bounds, CoordinateAxes, GeometryErrorKind, Point, Shape, Unmetered, Work, XY,
    for_each_overlapping_bounds_cross_pair_with_work,
};

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ContactCheckProgress {
    bounds_attempted: usize,
    bounds_completed: usize,
    identity_attempted: usize,
    identity_completed: usize,
}

#[cfg(test)]
thread_local! {
    static CONTACT_CHECK_PROGRESS: Cell<ContactCheckProgress> =
        const { Cell::new(ContactCheckProgress {
            bounds_attempted: 0,
            bounds_completed: 0,
            identity_attempted: 0,
            identity_completed: 0,
        }) };
}

#[cfg(test)]
fn reset_contact_check_progress() {
    CONTACT_CHECK_PROGRESS.with(|progress| progress.set(ContactCheckProgress::default()));
}

#[cfg(test)]
fn contact_check_progress() -> ContactCheckProgress {
    CONTACT_CHECK_PROGRESS.with(Cell::get)
}

#[cfg(test)]
fn record_bounds_attempt() {
    CONTACT_CHECK_PROGRESS.with(|progress| {
        let mut value = progress.get();
        value.bounds_attempted = value.bounds_attempted.wrapping_add(1);
        progress.set(value);
    });
}

#[cfg(test)]
fn record_bounds_complete() {
    CONTACT_CHECK_PROGRESS.with(|progress| {
        let mut value = progress.get();
        value.bounds_completed = value.bounds_completed.wrapping_add(1);
        progress.set(value);
    });
}

#[cfg(test)]
fn record_identity_attempt() {
    CONTACT_CHECK_PROGRESS.with(|progress| {
        let mut value = progress.get();
        value.identity_attempted = value.identity_attempted.wrapping_add(1);
        progress.set(value);
    });
}

#[cfg(test)]
fn record_identity_complete() {
    CONTACT_CHECK_PROGRESS.with(|progress| {
        let mut value = progress.get();
        value.identity_completed = value.identity_completed.wrapping_add(1);
        progress.set(value);
    });
}

/// Exact points are chart images already admitted by their producer receipt.
/// An adapter must form a lifted longitude as exact native X plus the original
/// arbitrary-precision sheet times the exact period, then apply any bounded
/// relative neighbor offset. A finite-domain `i64` offset is never the original
/// sheet, and the global reference X is never rounded through `f64`.
#[cfg_attr(test, derive(Clone))]
pub(super) struct ExactPoints {
    pub(super) exact: Vec<ExactChartPoint>,
    pub(super) rounded: Vec<XY>,
}

impl ExactPoints {
    #[cfg(test)]
    pub(super) fn from_exact(exact: Vec<ExactChartPoint>, work: &mut impl Work) -> Result<Self> {
        work.charge_into_error(
            u64::try_from(exact.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut rounded = Vec::with_capacity(exact.len());
        for point in &exact {
            work.charge_into_error(1)?;
            let rounded_point = XY::new(
                point.x.round_with_work(work)?,
                point.y.round_with_work(work)?,
            );
            if !rounded_point.x.is_finite() || !rounded_point.y.is_finite() {
                return Err(GeometryErrorKind::Invalid(
                    "known graph point is outside the finite coordinate domain".into(),
                )
                .into());
            }
            rounded.push(rounded_point);
        }
        Ok(Self { exact, rounded })
    }
}

impl RayPoints for ExactPoints {
    fn rounded(&self, index: u32) -> XY {
        self.rounded
            .get(index as usize)
            .copied()
            .expect("graph point IDs belong to the admitted point table")
    }

    fn compare_axis(&self, left: u32, right: u32, axis: RayAxis) -> Result<Ordering> {
        self.compare_axis_with_work(left, right, axis, &mut Unmetered)
    }

    fn orientation(&self, a: u32, b: u32, c: u32) -> Result<ExactSign> {
        self.orientation_with_work(a, b, c, &mut Unmetered)
    }
}

impl RayPointsWithWork for ExactPoints {
    fn compare_axis_with_work<W: Work>(
        &self,
        left: u32,
        right: u32,
        axis: RayAxis,
        work: &mut W,
    ) -> Result<Ordering> {
        let left = self
            .exact
            .get(left as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("known graph point ID is unknown".into()))?;
        let right = self
            .exact
            .get(right as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("known graph point ID is unknown".into()))?;
        match axis {
            RayAxis::X => left.x.try_cmp_with_work(&right.x, work),
            RayAxis::Y => left.y.try_cmp_with_work(&right.y, work),
        }
    }

    fn orientation_with_work<W: Work>(
        &self,
        a: u32,
        b: u32,
        c: u32,
        work: &mut W,
    ) -> Result<ExactSign> {
        let a = self
            .exact
            .get(a as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("known graph point ID is unknown".into()))?;
        let b = self
            .exact
            .get(b as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("known graph point ID is unknown".into()))?;
        let c = self
            .exact
            .get(c as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("known graph point ID is unknown".into()))?;
        let direction = vector_difference_with_work(a, b, work)?;
        let offset = vector_difference_with_work(a, c, work)?;
        vector_cross_with_work(&direction, &offset, work).map(|cross| cross.sign())
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct LogicalArealWalk {
    pub(super) output: u32,
    pub(super) walk: u32,
    pub(super) vertices: Vec<u32>,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct DirectedArealUse {
    atom: u32,
    reversed: bool,
}

struct AdmittedArealWalk {
    output: u32,
    walk: u32,
    uses: Vec<DirectedArealUse>,
}

/// Dimensions are admitted from the actual output walks after the producer's
/// canonical atom inventory exists.
pub(super) struct LogicalLayout {
    areal_walks: Vec<AdmittedArealWalk>,
}

impl LogicalLayout {
    pub(super) fn admit(
        walks: Vec<LogicalArealWalk>,
        vertex_count: usize,
        edge_nodes: &[[u32; 2]],
        work: &mut impl Work,
    ) -> Result<Self> {
        if walks.is_empty() {
            return Ok(Self {
                areal_walks: Vec::new(),
            });
        }
        work.charge_into_error(
            u64::try_from(edge_nodes.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut atom_by_endpoints = BTreeMap::new();
        for (atom, &[left, right]) in edge_nodes.iter().enumerate() {
            work.charge_into_error(1)?;
            let key = if left < right {
                [left, right]
            } else {
                [right, left]
            };
            if atom_by_endpoints
                .insert(
                    key,
                    u32::try_from(atom).map_err(|_| ResourceError::SizeOverflow)?,
                )
                .is_some()
            {
                return Err(GeometryErrorKind::Invalid(
                    "logical layout received duplicate geometric atoms".into(),
                )
                .into());
            }
        }

        work.charge_into_error(
            u64::try_from(walks.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut identities = BTreeSet::new();
        let mut areal_walks = Vec::with_capacity(walks.len());
        for walk in walks {
            work.charge_into_error(1)?;
            if !identities.insert((walk.output, walk.walk))
                || walk.vertices.len() < 4
                || walk.vertices.first() != walk.vertices.last()
            {
                return Err(GeometryErrorKind::Invalid(
                    "logical areal walk identity or closure is invalid".into(),
                )
                .into());
            }
            let edge_count = walk
                .vertices
                .len()
                .checked_sub(1)
                .ok_or(ResourceError::SizeOverflow)?;
            work.charge_into_error(
                u64::try_from(edge_count).map_err(|_| ResourceError::SizeOverflow)?,
            )?;
            let mut uses = Vec::with_capacity(edge_count);
            for &[start, end] in walk.vertices.array_windows::<2>() {
                work.charge_into_error(1)?;
                if start as usize >= vertex_count || end as usize >= vertex_count {
                    return Err(GeometryErrorKind::Invalid(
                        "logical areal walk references an unknown graph vertex".into(),
                    )
                    .into());
                }
                let endpoints = if start < end {
                    [start, end]
                } else {
                    [end, start]
                };
                let atom = *atom_by_endpoints.get(&endpoints).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "logical areal walk uses an atom outside the admitted graph".into(),
                    )
                })?;
                let canonical = edge_nodes.get(atom as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("logical areal atom ID is unknown".into())
                })?;
                uses.push(DirectedArealUse {
                    atom,
                    reversed: [end, start] == *canonical,
                });
            }
            areal_walks.push(AdmittedArealWalk {
                output: walk.output,
                walk: walk.walk,
                uses,
            });
        }
        try_sort_by_with_work(&mut areal_walks, work, |left, right, work| {
            work.charge_into_error(1)?;
            Ok((left.output, left.walk).cmp(&(right.output, right.walk)))
        })?;
        Ok(Self { areal_walks })
    }

    pub(super) const fn has_areal_walks(&self) -> bool {
        !self.areal_walks.is_empty()
    }
}

/// Stable identities needed by a candidate proof survive the wall view. Its
/// indexed CSR and `GlobalFaces` are temporary admission/candidate data.
struct ArealSignature {
    face_keys: Vec<Vec<u32>>,
    exterior_face: u32,
    host_by_vertex: Vec<u32>,
    fan_rows: Vec<(u32, Vec<u32>)>,
}

pub(super) struct ArealProof {
    /// Stable IDs in the producer's complete canonical atom inventory.
    wall_atom_ids: Vec<u32>,
    signature: ArealSignature,
}

pub(super) struct AdmittedLayout {
    layout: LogicalLayout,
    pub(super) areal: Option<ArealProof>,
}

/// Admit identities and all-graph contacts first. Dimension-specific fan and
/// wall-face obligations are frozen only after `LogicalLayout` arrives.
pub(super) struct AdmittedGraph {
    pub(super) points: ExactPoints,
    pub(super) edge_nodes: Vec<[u32; 2]>,
    pub(super) edge_slots: Vec<[u32; 2]>,
    pub(super) edge_of_slot: Vec<u32>,
    pub(super) topology: IndexedTopology,
    point_identities: Vec<u32>,
    point_contact_queries: Vec<u32>,
}

/// A successful assignment retains the exact graph and layout that admitted
/// it. Its fields are issued only by `admit_candidate`.
pub(super) struct CheckedAssignment<'a> {
    reference: &'a AdmittedGraph,
    dimension: &'a AdmittedLayout,
    points: ExactPoints,
}

impl CheckedAssignment<'_> {
    pub(super) const fn points(&self) -> &ExactPoints {
        &self.points
    }

    pub(super) fn is_for(&self, reference: &AdmittedGraph, dimension: &AdmittedLayout) -> bool {
        std::ptr::eq(self.reference, reference) && std::ptr::eq(self.dimension, dimension)
    }
}

pub(super) fn exact_chord_fraction(
    start: &ExactChartPoint,
    end: &ExactChartPoint,
    point: &ExactChartPoint,
    work: &mut impl Work,
) -> Result<Option<ExactRational>> {
    work.charge_into_error(4)?;
    let dx = end.x.try_subtract_with_work(&start.x, work)?;
    let dy = end.y.try_subtract_with_work(&start.y, work)?;
    let fraction = if !dx.is_zero() {
        point
            .x
            .try_subtract_with_work(&start.x, work)?
            .try_quotient_with_work(&dx, work)?
    } else if !dy.is_zero() {
        point
            .y
            .try_subtract_with_work(&start.y, work)?
            .try_quotient_with_work(&dy, work)?
    } else {
        return Ok(None);
    };
    if fraction
        .try_cmp_with_work(&ExactRational::zero(), work)?
        .is_lt()
        || fraction
            .try_cmp_with_work(&ExactRational::one(), work)?
            .is_gt()
    {
        return Ok(None);
    }
    let projected = ExactChartPoint::from_rationals(
        start
            .x
            .try_add_with_work(&dx.try_product_with_work(&fraction, work)?, work)?,
        start
            .y
            .try_add_with_work(&dy.try_product_with_work(&fraction, work)?, work)?,
    );
    Ok((projected == *point).then_some(fraction))
}

#[derive(Debug, Eq, PartialEq)]
pub(super) struct LiteralPoint<I> {
    pub(super) output: u32,
    pub(super) axes: CoordinateAxes,
    pub(super) image: I,
}

#[derive(Debug, Eq, PartialEq)]
pub(super) struct LiteralSequence<I> {
    pub(super) output: u32,
    pub(super) axes: CoordinateAxes,
    pub(super) images: Vec<I>,
}

#[derive(Debug, Eq, PartialEq)]
pub(super) struct LiteralPolygon<I> {
    pub(super) output: u32,
    pub(super) axes: CoordinateAxes,
    pub(super) shell: LiteralSequence<I>,
    pub(super) holes: Vec<LiteralSequence<I>>,
}

pub(super) struct ShapeLiteralLayout<I> {
    pub(super) points: Vec<LiteralPoint<I>>,
    pub(super) lines: Vec<LiteralSequence<I>>,
    pub(super) polygons: Vec<LiteralPolygon<I>>,
}

impl<I> ShapeLiteralLayout<I> {
    pub(super) fn in_output_order(
        self,
        order: &CertifiedOverlayOrder,
        work: &mut impl Work,
    ) -> Result<Self> {
        Ok(Self {
            points: reorder_rows(self.points, &order.points, work)?,
            lines: reorder_rows(self.lines, &order.lines, work)?,
            polygons: reorder_rows(self.polygons, &order.polygons, work)?,
        })
    }
}

pub(super) fn reorder_rows<T>(
    rows: Vec<T>,
    order: &[usize],
    work: &mut impl Work,
) -> Result<Vec<T>> {
    if rows.len() != order.len() {
        return Err(
            GeometryErrorKind::Invalid("literal output order omits a physical row".into()).into(),
        );
    }
    work.charge_into_error(u64::try_from(rows.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut rows = rows.into_iter().map(Some).collect::<Vec<_>>();
    work.charge_into_error(u64::try_from(order.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut output = Vec::with_capacity(order.len());
    for &index in order {
        work.charge_into_error(1)?;
        let row = rows.get_mut(index).ok_or_else(|| {
            GeometryErrorKind::Invalid("literal output order references an unknown row".into())
        })?;
        output.push(row.take().ok_or_else(|| {
            GeometryErrorKind::Invalid("literal output order repeats a row".into())
        })?);
    }
    Ok(output)
}

/// Owns the real finalized sink and its physical-slot-to-logical-use map.
pub(super) struct FrozenLiteralBuffers<I> {
    shape: Shape,
    layout: ShapeLiteralLayout<I>,
}

/// Created only by the actual-buffer binder; it moves the same sink onward.
pub(super) struct SealedOutput<I> {
    buffers: FrozenLiteralBuffers<I>,
}

pub(super) enum LiteralCheck<I> {
    Pass(SealedOutput<I>),
    Reject(Vec<I>),
}

pub(super) trait CheckedImages<I> {
    fn image(&self, image: I) -> Result<&ExactChartPoint>;
}

pub(super) trait LiteralChart<I> {
    fn read_point<W: Work>(
        &mut self,
        output: u32,
        image: I,
        record: Point,
        expected_axes: CoordinateAxes,
        stored_axes: CoordinateAxes,
        work: &mut W,
    ) -> Result<Option<ExactChartPoint>>;
}

pub(super) trait LiteralReceiver<I>: CheckedImages<I> + LiteralChart<I> {}

impl<I, R> LiteralReceiver<I> for R where R: CheckedImages<I> + LiteralChart<I> {}

impl<I> FrozenLiteralBuffers<I> {
    pub(super) const fn shapes(shape: Shape, locations: ShapeLiteralLayout<I>) -> Self {
        Self {
            shape,
            layout: locations,
        }
    }
}

impl<I> SealedOutput<I> {
    pub(super) const fn shape_layout(&self) -> &ShapeLiteralLayout<I> {
        &self.buffers.layout
    }

    pub(super) fn into_shape(self) -> Shape {
        self.buffers.shape
    }
}

pub(super) fn bind_literals<I, R, W>(
    buffers: FrozenLiteralBuffers<I>,
    receiver: &mut R,
    work: &mut W,
) -> Result<LiteralCheck<I>>
where
    I: Copy + Eq + Ord,
    R: LiteralReceiver<I>,
    W: Work,
{
    let mut rejected = Vec::new();
    let mut check =
        |output, image, record, expected_axes, stored_axes, work: &mut W| -> Result<()> {
            work.charge_into_error(1)?;
            if expected_axes != stored_axes || CoordinateAxes::from_point(record) != stored_axes {
                rejected.push(image);
                return Ok(());
            }
            let Some(actual) =
                receiver.read_point(output, image, record, expected_axes, stored_axes, work)?
            else {
                rejected.push(image);
                return Ok(());
            };
            if actual != *receiver.image(image)? {
                rejected.push(image);
            }
            Ok(())
        };
    validate_shape_layout(&buffers.layout, work)?;
    visit_shape_literals(&buffers.shape, &buffers.layout, work, &mut check)?;
    if rejected.is_empty() {
        Ok(LiteralCheck::Pass(SealedOutput { buffers }))
    } else {
        rejected.sort_unstable();
        rejected.dedup();
        Ok(LiteralCheck::Reject(rejected))
    }
}

fn validate_dense_output_ids(
    values: impl Iterator<Item = u32>,
    count: usize,
    work: &mut impl Work,
) -> Result<()> {
    work.charge_into_error(u64::try_from(count).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut ids = values.collect::<Vec<_>>();
    if ids.len() != count {
        return Err(GeometryErrorKind::Invalid(
            "literal logical output IDs do not cover their rows".into(),
        )
        .into());
    }
    try_sort_by_with_work(&mut ids, work, |left, right, work| {
        work.charge_into_error(1)?;
        Ok(left.cmp(right))
    })?;
    if ids
        .iter()
        .enumerate()
        .any(|(index, &id)| usize::try_from(id).ok() != Some(index))
    {
        return Err(GeometryErrorKind::Invalid(
            "literal logical output IDs are missing or repeated".into(),
        )
        .into());
    }
    Ok(())
}

fn validate_shape_layout<I>(layout: &ShapeLiteralLayout<I>, work: &mut impl Work) -> Result<()> {
    validate_dense_output_ids(
        layout.points.iter().map(|row| row.output),
        layout.points.len(),
        work,
    )?;
    validate_dense_output_ids(
        layout.lines.iter().map(|row| row.output),
        layout.lines.len(),
        work,
    )?;
    validate_dense_output_ids(
        layout.polygons.iter().map(|row| row.output),
        layout.polygons.len(),
        work,
    )?;
    for polygon in &layout.polygons {
        work.charge_into_error(1)?;
        if polygon.shell.output != polygon.output
            || polygon
                .holes
                .iter()
                .any(|hole| hole.output != polygon.output)
        {
            return Err(GeometryErrorKind::Invalid(
                "polygon ring rows lost their logical output identity".into(),
            )
            .into());
        }
    }
    Ok(())
}

pub(super) fn visit_shape_literals<I: Copy, W: Work>(
    shape: &Shape,
    layout: &ShapeLiteralLayout<I>,
    work: &mut W,
    visit: &mut impl FnMut(u32, I, Point, CoordinateAxes, CoordinateAxes, &mut W) -> Result<()>,
) -> Result<()> {
    fn visit_sequence<I: Copy, W: Work>(
        sequence: &crate::geometry::CoordSeq,
        locations: &LiteralSequence<I>,
        work: &mut W,
        visit: &mut impl FnMut(u32, I, Point, CoordinateAxes, CoordinateAxes, &mut W) -> Result<()>,
    ) -> Result<()> {
        if sequence.len() != locations.images.len() {
            return Err(GeometryErrorKind::Invalid(
                "sealed coordinate sequence differs from its physical literal map".into(),
            )
            .into());
        }
        let axes = sequence.axes();
        if axes != locations.axes {
            return Err(GeometryErrorKind::Invalid(
                "sealed sequence axes differ from its logical output policy".into(),
            )
            .into());
        }
        for (&image, point) in locations.images.iter().zip(sequence.points()) {
            work.charge_into_error(1)?;
            visit(locations.output, image, point, locations.axes, axes, work)?;
        }
        Ok(())
    }

    fn visit_polygon<I: Copy, W: Work>(
        polygon: &crate::geometry::Polygon,
        locations: &LiteralPolygon<I>,
        work: &mut W,
        visit: &mut impl FnMut(u32, I, Point, CoordinateAxes, CoordinateAxes, &mut W) -> Result<()>,
    ) -> Result<()> {
        visit_sequence(polygon.shell.coords(), &locations.shell, work, visit)?;
        if polygon.holes.len() != locations.holes.len()
            || locations.shell.axes != locations.axes
            || locations
                .holes
                .iter()
                .any(|hole| hole.axes != locations.axes)
        {
            return Err(GeometryErrorKind::Invalid(
                "sealed polygon holes differ from their physical literal map".into(),
            )
            .into());
        }
        for (ring, locations) in polygon.holes.iter().zip(&locations.holes) {
            visit_sequence(ring.coords(), locations, work, visit)?;
        }
        Ok(())
    }

    fn visit_shape<I: Copy, W: Work>(
        shape: &Shape,
        layout: &ShapeLiteralLayout<I>,
        point_row: &mut usize,
        line_row: &mut usize,
        polygon_row: &mut usize,
        work: &mut W,
        visit: &mut impl FnMut(u32, I, Point, CoordinateAxes, CoordinateAxes, &mut W) -> Result<()>,
    ) -> Result<()> {
        match shape {
            Shape::Point(point) => {
                let location = layout.points.get(*point_row).ok_or_else(|| {
                    GeometryErrorKind::Invalid("sealed point has no physical literal row".into())
                })?;
                *point_row = point_row
                    .checked_add(1)
                    .ok_or(ResourceError::SizeOverflow)?;
                work.charge_into_error(1)?;
                visit(
                    location.output,
                    location.image,
                    *point,
                    location.axes,
                    CoordinateAxes::from_point(*point),
                    work,
                )
            },
            Shape::MultiPoint(points) => {
                let end = point_row
                    .checked_add(points.len())
                    .ok_or(ResourceError::SizeOverflow)?;
                let locations = layout.points.get(*point_row..end).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "sealed multipoint has incomplete physical literal rows".into(),
                    )
                })?;
                *point_row = end;
                for (location, point) in locations.iter().zip(points.points()) {
                    work.charge_into_error(1)?;
                    visit(
                        location.output,
                        location.image,
                        point,
                        location.axes,
                        points.axes(),
                        work,
                    )?;
                }
                Ok(())
            },
            Shape::LineString(line) => {
                let locations = layout.lines.get(*line_row).ok_or_else(|| {
                    GeometryErrorKind::Invalid("sealed line has no physical literal row".into())
                })?;
                *line_row = line_row.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
                visit_sequence(line.as_coords(), locations, work, visit)
            },
            Shape::MultiLineString(lines) => {
                for line in lines.as_slice() {
                    let locations = layout.lines.get(*line_row).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "sealed multiline has incomplete physical literal rows".into(),
                        )
                    })?;
                    *line_row = line_row.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
                    visit_sequence(line.as_coords(), locations, work, visit)?;
                }
                Ok(())
            },
            Shape::Polygon(polygon) => {
                let locations = layout.polygons.get(*polygon_row).ok_or_else(|| {
                    GeometryErrorKind::Invalid("sealed polygon has no physical literal row".into())
                })?;
                *polygon_row = polygon_row
                    .checked_add(1)
                    .ok_or(ResourceError::SizeOverflow)?;
                visit_polygon(polygon, locations, work, visit)
            },
            Shape::MultiPolygon(polygons) => {
                for polygon in polygons.as_slice() {
                    let locations = layout.polygons.get(*polygon_row).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "sealed multipolygon has incomplete physical literal rows".into(),
                        )
                    })?;
                    *polygon_row = polygon_row
                        .checked_add(1)
                        .ok_or(ResourceError::SizeOverflow)?;
                    visit_polygon(polygon, locations, work, visit)?;
                }
                Ok(())
            },
            Shape::GeometryCollection(collection) => {
                for part in collection {
                    visit_shape(part, layout, point_row, line_row, polygon_row, work, visit)?;
                }
                Ok(())
            },
            Shape::Empty(..) => Ok(()),
        }
    }

    let mut point_row = 0_usize;
    let mut line_row = 0_usize;
    let mut polygon_row = 0_usize;
    visit_shape(
        shape,
        layout,
        &mut point_row,
        &mut line_row,
        &mut polygon_row,
        work,
        visit,
    )?;
    if point_row != layout.points.len()
        || line_row != layout.lines.len()
        || polygon_row != layout.polygons.len()
    {
        return Err(GeometryErrorKind::Invalid(
            "sealed output left physical literal rows unused".into(),
        )
        .into());
    }
    Ok(())
}

/// Admit a producer-known graph without assigning it a dimension's face law.
pub(super) fn admit_known_graph(
    points: ExactPoints,
    edge_nodes: Vec<[u32; 2]>,
    point_identities: Vec<u32>,
    work: &mut impl Work,
) -> Result<AdmittedGraph> {
    if point_identities.len() != points.exact.len() {
        return Err(GeometryErrorKind::Invalid(
            "known graph identity rows do not match its point inventory".into(),
        )
        .into());
    }
    validate_point_hints(&points, work)?;
    validate_atoms(&points, &edge_nodes, work)?;
    if !edges_have_only_vertex_contacts(&points, &edge_nodes, work)? {
        return Err(GeometryErrorKind::Invalid(
            "known graph edges contain an unrepresented contact or overlap".into(),
        )
        .into());
    }
    if !identities_are_injective(&points, &point_identities, work)? {
        return Err(GeometryErrorKind::Invalid(
            "distinct known graph identities share one exact point".into(),
        )
        .into());
    }
    work.charge_into_error(
        u64::try_from(points.exact.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut degree = vec![0_usize; points.exact.len()];
    for &[start, end] in &edge_nodes {
        work.charge_into_error(2)?;
        for vertex in [start, end] {
            let count = degree.get_mut(vertex as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("known graph edge endpoint is unknown".into())
            })?;
            *count = count.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
        }
    }
    work.charge_into_error(u64::try_from(degree.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut point_contact_queries = Vec::new();
    for (vertex, &incident_edges) in degree.iter().enumerate() {
        work.charge_into_error(1)?;
        if incident_edges == 0 {
            point_contact_queries
                .push(u32::try_from(vertex).map_err(|_| ResourceError::SizeOverflow)?);
        }
    }
    let (topology, edge_of_slot) = indexed_topology(&points, &edge_nodes, work)?;
    let edge_slots = indexed_edge_slots_with_work(&edge_nodes, &topology, &edge_of_slot, work)?;
    Ok(AdmittedGraph {
        points,
        edge_nodes,
        edge_slots,
        edge_of_slot,
        topology,
        point_identities,
        point_contact_queries,
    })
}

impl AdmittedGraph {
    /// Extend the graph's degree-zero query set with the producer's actual
    /// output-point vertices before the reference is used for candidates.
    pub(super) fn with_output_point_contacts(
        mut self,
        vertices: &[u32],
        work: &mut impl Work,
    ) -> Result<Self> {
        work.charge_into_error(
            u64::try_from(vertices.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut requested = Vec::with_capacity(vertices.len());
        for &vertex in vertices {
            work.charge_into_error(1)?;
            if self.points.exact.get(vertex as usize).is_none() {
                return Err(GeometryErrorKind::Invalid(
                    "output point contact query is outside the admitted graph".into(),
                )
                .into());
            }
            requested.push(vertex);
        }
        if requested.is_empty() {
            return Ok(self);
        }
        try_sort_by_with_work(&mut requested, work, |left, right, work| {
            work.charge_into_error(1)?;
            Ok(left.cmp(right))
        })?;
        work.charge_into_error(
            u64::try_from(requested.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut unique = Vec::with_capacity(requested.len());
        for vertex in requested {
            work.charge_into_error(1)?;
            if unique.last() != Some(&vertex) {
                unique.push(vertex);
            }
        }
        self.point_contact_queries =
            merge_sorted_contact_queries(&self.point_contact_queries, &unique, work)?;
        Ok(self)
    }
}

/// Freeze dimension-specific obligations only after the producer's actual
/// logical output walks have been admitted against its canonical atoms.
/// A producer-owned full-graph face table is reused only when every admitted
/// atom is a wall; partial wall views build and drop their own face tables.
pub(super) fn admit_logical_layout(
    reference: &AdmittedGraph,
    walks: Vec<LogicalArealWalk>,
    full_graph_faces: Option<&GlobalFaces>,
    work: &mut impl Work,
) -> Result<AdmittedLayout> {
    let layout = LogicalLayout::admit(
        walks,
        reference.points.exact.len(),
        &reference.edge_nodes,
        work,
    )?;
    let areal = if layout.has_areal_walks() {
        Some(admit_areal_proof(
            reference,
            &layout,
            full_graph_faces,
            work,
        )?)
    } else {
        None
    };
    Ok(AdmittedLayout { layout, areal })
}

pub(super) fn admit_candidate<'a>(
    reference: &'a AdmittedGraph,
    dimension: &'a AdmittedLayout,
    points: ExactPoints,
    work: &mut impl Work,
) -> Result<Option<CheckedAssignment<'a>>> {
    if points.exact.len() != reference.points.exact.len()
        || points.rounded.len() != points.exact.len()
    {
        return Ok(None);
    }
    if !point_hints_match(&points, work)? {
        return Ok(None);
    }
    if !identities_are_injective(&points, &reference.point_identities, work)? {
        return Ok(None);
    }
    if !point_edge_contacts_preserved(
        &reference.points,
        &points,
        &reference.point_contact_queries,
        &reference.edge_nodes,
        work,
    )? {
        return Ok(None);
    }
    if !edges_have_only_vertex_contacts(&points, &reference.edge_nodes, work)? {
        return Ok(None);
    }
    let (topology, edge_of_slot) = match indexed_topology(&points, &reference.edge_nodes, work) {
        Ok(topology) => topology,
        Err(error) if is_tied_departure_error(&error) => return Ok(None),
        Err(error) => return Err(error),
    };
    if let Some(areal_reference) = &dimension.areal {
        let areal = build_areal_signature(
            reference,
            &dimension.layout,
            &areal_reference.wall_atom_ids,
            &points,
            &topology,
            &edge_of_slot,
            None,
            work,
        )?;
        if !stable_areal_signatures_match(&areal_reference.signature, &areal) {
            return Ok(None);
        }
    }
    Ok(Some(CheckedAssignment {
        reference,
        dimension,
        points,
    }))
}

fn merge_sorted_contact_queries(
    left: &[u32],
    right: &[u32],
    work: &mut impl Work,
) -> Result<Vec<u32>> {
    let capacity = left
        .len()
        .checked_add(right.len())
        .ok_or(ResourceError::SizeOverflow)?;
    work.charge_into_error(u64::try_from(capacity).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut merged = Vec::with_capacity(capacity);
    let (mut left_index, mut right_index) = (0_usize, 0_usize);
    while left_index < left.len() && right_index < right.len() {
        work.charge_into_error(1)?;
        let (Some(&left_row), Some(&right_row)) = (left.get(left_index), right.get(right_index))
        else {
            return Err(GeometryErrorKind::Invalid(
                "contact query merge cursor is outside its rows".into(),
            )
            .into());
        };
        if left_row <= right_row {
            work.charge_into_error(1)?;
            if merged.last() != Some(&left_row) {
                work.charge_into_error(1)?;
                merged.push(left_row);
            }
            left_index = left_index
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?;
        } else {
            work.charge_into_error(1)?;
            if merged.last() != Some(&right_row) {
                work.charge_into_error(1)?;
                merged.push(right_row);
            }
            right_index = right_index
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?;
        }
    }
    for &row in left.get(left_index..).ok_or_else(|| {
        GeometryErrorKind::Invalid("contact query merge left cursor is unknown".into())
    })? {
        work.charge_into_error(1)?;
        if merged.last() != Some(&row) {
            work.charge_into_error(1)?;
            merged.push(row);
        }
    }
    for &row in right.get(right_index..).ok_or_else(|| {
        GeometryErrorKind::Invalid("contact query merge right cursor is unknown".into())
    })? {
        work.charge_into_error(1)?;
        if merged.last() != Some(&row) {
            work.charge_into_error(1)?;
            merged.push(row);
        }
    }
    Ok(merged)
}

fn stable_areal_signatures_match(reference: &ArealSignature, candidate: &ArealSignature) -> bool {
    reference.face_keys == candidate.face_keys
        && reference.exterior_face == candidate.exterior_face
        && reference.host_by_vertex == candidate.host_by_vertex
        && reference.fan_rows == candidate.fan_rows
}

fn wall_covers_graph(wall_atom_ids: &[u32], atom_count: usize) -> bool {
    wall_atom_ids.len() == atom_count
        && wall_atom_ids
            .iter()
            .enumerate()
            .all(|(index, &atom)| usize::try_from(atom).ok() == Some(index))
}

fn is_tied_departure_error(error: &crate::error::Error) -> bool {
    matches!(
        error.kind(),
        crate::error::ErrorKind::Geometry(GeometryErrorKind::Invalid(message))
            if message.as_ref() == "distinct indexed graph edges share a departure ray"
    )
}

fn validate_atoms(
    points: &ExactPoints,
    edge_nodes: &[[u32; 2]],
    work: &mut impl Work,
) -> Result<()> {
    let mut seen = BTreeSet::new();
    for &[start, end] in edge_nodes {
        work.charge_into_error(1)?;
        if start == end
            || points.exact.get(start as usize).is_none()
            || points.exact.get(end as usize).is_none()
        {
            return Err(GeometryErrorKind::Invalid(
                "known graph atom has a collapsed or unknown endpoint".into(),
            )
            .into());
        }
        if points
            .exact
            .get(start as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("known graph start is unknown".into()))?
            .same_position(
                points.exact.get(end as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("known graph end is unknown".into())
                })?,
            )?
        {
            return Err(GeometryErrorKind::Invalid(
                "known graph atom has geometrically collapsed endpoints".into(),
            )
            .into());
        }
        let pair = if start < end {
            [start, end]
        } else {
            [end, start]
        };
        if !seen.insert(pair) {
            return Err(GeometryErrorKind::Invalid(
                "known graph contains duplicate geometric atoms".into(),
            )
            .into());
        }
    }
    Ok(())
}

fn validate_point_hints(points: &ExactPoints, work: &mut impl Work) -> Result<()> {
    if !point_hints_match(points, work)? {
        return Err(GeometryErrorKind::Invalid(
            "known graph rounded hints do not match their exact points".into(),
        )
        .into());
    }
    Ok(())
}

fn point_hints_match(points: &ExactPoints, work: &mut impl Work) -> Result<bool> {
    if points.rounded.len() != points.exact.len() {
        return Ok(false);
    }
    work.charge_into_error(
        u64::try_from(points.exact.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    for (exact, &rounded) in points.exact.iter().zip(&points.rounded) {
        work.charge_into_error(1)?;
        let expected = XY::new(
            exact.x.round_with_work(work)?,
            exact.y.round_with_work(work)?,
        );
        if !expected.x.is_finite() || !expected.y.is_finite() || expected != rounded {
            return Ok(false);
        }
    }
    Ok(true)
}

fn identities_are_injective(
    points: &ExactPoints,
    identities: &[u32],
    work: &mut impl Work,
) -> Result<bool> {
    if points.exact.len() != identities.len() {
        return Ok(false);
    }
    work.charge_into_error(
        u64::try_from(points.exact.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut order = (0..points.exact.len())
        .map(|index| {
            Ok((
                index,
                *identities.get(index).ok_or_else(|| {
                    GeometryErrorKind::Invalid("graph point has no identity receipt".into())
                })?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    try_sort_by_with_work(&mut order, work, |left, right, work| {
        work.charge_into_error(1)?;
        let left_point = points.exact.get(left.0).ok_or_else(|| {
            GeometryErrorKind::Invalid("graph identity point is outside its inventory".into())
        })?;
        let right_point = points.exact.get(right.0).ok_or_else(|| {
            GeometryErrorKind::Invalid("graph identity point is outside its inventory".into())
        })?;
        let position = left_point.compare_lex(right_point)?;
        Ok(if position.is_eq() {
            left.1.cmp(&right.1)
        } else {
            position
        })
    })?;
    for pair in order.array_windows::<2>() {
        work.charge_into_error(1)?;
        let [left, right] = pair;
        if left.1 != right.1
            && points
                .exact
                .get(left.0)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("graph identity point is unknown".into())
                })?
                .same_position(points.exact.get(right.0).ok_or_else(|| {
                    GeometryErrorKind::Invalid("graph identity point is unknown".into())
                })?)?
        {
            return Ok(false);
        }
    }
    Ok(true)
}

fn indexed_topology(
    points: &ExactPoints,
    edge_nodes: &[[u32; 2]],
    work: &mut impl Work,
) -> Result<(IndexedTopology, Vec<u32>)> {
    indexed_topology_from_edges_with_work(
        points.exact.len(),
        edge_nodes,
        work,
        |origin, left, right, work| {
            let left_edge = edge_nodes.get(left).ok_or_else(|| {
                GeometryErrorKind::Invalid("known graph departure edge is unknown".into())
            })?;
            let right_edge = edge_nodes.get(right).ok_or_else(|| {
                GeometryErrorKind::Invalid("known graph departure edge is unknown".into())
            })?;
            let departure = |edge: &[u32; 2]| -> Result<u32> {
                if edge[0] == origin {
                    Ok(edge[1])
                } else if edge[1] == origin {
                    Ok(edge[0])
                } else {
                    Err(GeometryErrorKind::Invalid(
                        "known graph departure does not touch its vertex".into(),
                    )
                    .into())
                }
            };
            compare_departures_with_work(
                points,
                origin,
                departure(left_edge)?,
                departure(right_edge)?,
                work,
            )
        },
    )
}

/// Build global faces for a previously admitted areal-wall projection only.
fn global_faces(
    points: &ExactPoints,
    topology: &IndexedTopology,
    edge_nodes: &[[u32; 2]],
    edge_of_slot: &[u32],
    work: &mut impl Work,
) -> Result<GlobalFaces> {
    graph_global_faces_with_work(
        points,
        topology,
        edge_nodes.len(),
        |edge| {
            edge_nodes.get(edge).copied().ok_or_else(|| {
                GeometryErrorKind::Invalid("known graph face edge is unknown".into()).into()
            })
        },
        edge_of_slot,
        work,
    )
}

fn areal_vertices(
    layout: &LogicalLayout,
    edge_nodes: &[[u32; 2]],
    work: &mut impl Work,
) -> Result<Vec<u32>> {
    let mut vertices = BTreeSet::new();
    for walk in &layout.areal_walks {
        for use_row in &walk.uses {
            work.charge_into_error(1)?;
            let [start, end] = *edge_nodes.get(use_row.atom as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("admitted areal use has an unknown atom".into())
            })?;
            vertices.insert(if use_row.reversed { end } else { start });
            vertices.insert(if use_row.reversed { start } else { end });
        }
    }
    Ok(vertices.into_iter().collect())
}

fn stable_fan_rows(
    topology: &IndexedTopology,
    edge_nodes: &[[u32; 2]],
    edge_of_slot: &[u32],
    vertices: &[u32],
    work: &mut impl Work,
) -> Result<Vec<(u32, Vec<u32>)>> {
    let vertex_count = topology.component_of.len();
    if topology.starts.len()
        != vertex_count
            .checked_add(1)
            .ok_or(ResourceError::SizeOverflow)?
        || edge_of_slot.len() != topology.targets.len()
    {
        return Err(GeometryErrorKind::Invalid(
            "stable fan correspondence received inconsistent topology tables".into(),
        )
        .into());
    }
    work.charge_into_error(
        u64::try_from(vertices.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut rotations = Vec::with_capacity(vertices.len());
    for &vertex in vertices {
        work.charge_into_error(1)?;
        if vertex as usize >= vertex_count {
            return Err(GeometryErrorKind::Invalid(
                "areal fan references an unknown graph vertex".into(),
            )
            .into());
        }
        let start = *topology.starts.get(vertex as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("stable graph rotation start is missing".into())
        })? as usize;
        let end_offset = (vertex as usize)
            .checked_add(1)
            .ok_or(ResourceError::SizeOverflow)?;
        let end = *topology.starts.get(end_offset).ok_or_else(|| {
            GeometryErrorKind::Invalid("stable graph rotation end is missing".into())
        })? as usize;
        let degree = end.checked_sub(start).ok_or(ResourceError::SizeOverflow)?;
        work.charge_into_error(u64::try_from(degree).map_err(|_| ResourceError::SizeOverflow)?)?;
        let mut rotation = Vec::with_capacity(degree);
        for slot in start..end {
            work.charge_into_error(1)?;
            let edge = *edge_of_slot.get(slot).ok_or_else(|| {
                GeometryErrorKind::Invalid("stable graph dart has no atom identity".into())
            })?;
            let endpoints = edge_nodes.get(edge as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("stable graph dart atom is unknown".into())
            })?;
            let origin = *topology.owners.get(slot).ok_or_else(|| {
                GeometryErrorKind::Invalid("stable graph dart has no origin".into())
            })?;
            let direction = if endpoints[0] == origin {
                0_u32
            } else if endpoints[1] == origin {
                1_u32
            } else {
                return Err(GeometryErrorKind::Invalid(
                    "stable graph dart origin disagrees with its atom".into(),
                )
                .into());
            };
            let dart = edge
                .checked_mul(2)
                .and_then(|value| value.checked_add(direction))
                .ok_or(ResourceError::SizeOverflow)?;
            rotation.push(dart);
        }
        if !rotation.is_empty() {
            let anchor = rotation
                .iter()
                .enumerate()
                .min_by_key(|(_, dart)| *dart)
                .map(|(index, _)| index)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("stable graph rotation has no anchor".into())
                })?;
            rotation.rotate_left(anchor);
        }
        rotations.push(rotation);
    }
    Ok(vertices.iter().copied().zip(rotations).collect())
}

fn stable_wall_face_keys(
    topology: &IndexedTopology,
    faces: &GlobalFaces,
    edge_nodes: &[[u32; 2]],
    edge_of_slot: &[u32],
    stable_edge_ids: &[u32],
    work: &mut impl Work,
) -> Result<(Vec<Vec<u32>>, Vec<u32>)> {
    let walk_count = topology.face_starts.len().checked_sub(1).ok_or_else(|| {
        GeometryErrorKind::Invalid(
            "stable areal wall correspondence has inconsistent face tables".into(),
        )
    })?;
    if faces.walk_faces.len() != walk_count {
        return Err(GeometryErrorKind::Invalid(
            "stable areal wall correspondence has inconsistent face tables".into(),
        )
        .into());
    }
    let face_count = usize::try_from(faces.count).map_err(|_| ResourceError::SizeOverflow)?;
    work.charge_into_error(u64::try_from(face_count).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut face_keys = vec![Vec::<u32>::new(); face_count];
    for walk in 0..walk_count {
        let start =
            *topology.face_starts.get(walk).ok_or_else(|| {
                GeometryErrorKind::Invalid("stable face walk start is missing".into())
            })? as usize;
        let next_walk = walk.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
        let end =
            *topology.face_starts.get(next_walk).ok_or_else(|| {
                GeometryErrorKind::Invalid("stable face walk end is missing".into())
            })? as usize;
        let slots = topology.face_slots.get(start..end).ok_or_else(|| {
            GeometryErrorKind::Invalid("stable face walk slot range is invalid".into())
        })?;
        if slots.is_empty() {
            return Err(GeometryErrorKind::Invalid(
                "stable face correspondence contains an empty boundary walk".into(),
            )
            .into());
        }
        work.charge_into_error(
            u64::try_from(slots.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut anchor = u32::MAX;
        for &slot in slots {
            let edge = *edge_of_slot.get(slot as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("stable face dart has no atom identity".into())
            })?;
            let stable_edge = *stable_edge_ids.get(edge as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("stable face dart has no stable atom ID".into())
            })?;
            let origin = *topology.owners.get(slot as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("stable face dart has no origin".into())
            })?;
            let endpoints = edge_nodes.get(edge as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("stable face dart atom is unknown".into())
            })?;
            let direction = if endpoints[0] == origin {
                0_u32
            } else if endpoints[1] == origin {
                1_u32
            } else {
                return Err(GeometryErrorKind::Invalid(
                    "stable face dart origin disagrees with its atom".into(),
                )
                .into());
            };
            anchor = anchor.min(
                stable_edge
                    .checked_mul(2)
                    .and_then(|value| value.checked_add(direction))
                    .ok_or(ResourceError::SizeOverflow)?,
            );
        }
        let global_face = *faces.walk_faces.get(walk).ok_or_else(|| {
            GeometryErrorKind::Invalid("stable face walk has no global face".into())
        })? as usize;
        face_keys
            .get_mut(global_face)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("stable face walk names an unknown face".into())
            })?
            .push(anchor);
    }
    for key in &mut face_keys {
        try_sort_by_with_work(key, work, |left, right, work| {
            work.charge_into_error(1)?;
            Ok(left.cmp(right))
        })?;
    }
    work.charge_into_error(u64::try_from(face_count).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut face_order = (0..face_count).collect::<Vec<_>>();
    try_sort_by_with_work(&mut face_order, work, |left, right, work| {
        work.charge_into_error(1)?;
        let left = face_keys
            .get(*left)
            .ok_or_else(|| GeometryErrorKind::Invalid("stable left face key is unknown".into()))?;
        let right = face_keys
            .get(*right)
            .ok_or_else(|| GeometryErrorKind::Invalid("stable right face key is unknown".into()))?;
        Ok(left.cmp(right))
    })?;
    for pair in face_order.array_windows::<2>() {
        work.charge_into_error(1)?;
        let [left, right] = pair;
        let left_key = face_keys
            .get(*left)
            .ok_or_else(|| GeometryErrorKind::Invalid("stable left face key is unknown".into()))?;
        let right_key = face_keys
            .get(*right)
            .ok_or_else(|| GeometryErrorKind::Invalid("stable right face key is unknown".into()))?;
        if left_key == right_key {
            return Err(GeometryErrorKind::Invalid(
                "stable global faces do not have distinct boundary identities".into(),
            )
            .into());
        }
    }
    work.charge_into_error(u64::try_from(face_count).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut stable_face_id = vec![0_u32; face_count];
    let mut stable_keys = Vec::with_capacity(face_count);
    for (stable, face) in face_order.into_iter().enumerate() {
        work.charge_into_error(1)?;
        *stable_face_id
            .get_mut(face)
            .ok_or_else(|| GeometryErrorKind::Invalid("stable face ordinal is unknown".into()))? =
            u32::try_from(stable).map_err(|_| ResourceError::SizeOverflow)?;
        stable_keys.push(
            face_keys
                .get(face)
                .cloned()
                .ok_or_else(|| GeometryErrorKind::Invalid("stable face key is unknown".into()))?,
        );
    }
    Ok((stable_keys, stable_face_id))
}

fn admit_areal_proof(
    graph: &AdmittedGraph,
    layout: &LogicalLayout,
    full_graph_faces: Option<&GlobalFaces>,
    work: &mut impl Work,
) -> Result<ArealProof> {
    let mut wall_atom_set = BTreeSet::new();
    for walk in &layout.areal_walks {
        for use_row in &walk.uses {
            work.charge_into_error(1)?;
            wall_atom_set.insert(use_row.atom);
        }
    }
    work.charge_into_error(
        u64::try_from(wall_atom_set.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let wall_atom_ids = wall_atom_set.into_iter().collect::<Vec<_>>();
    if wall_atom_ids.is_empty() {
        return Err(
            GeometryErrorKind::Invalid("areal logical layout has no wall atoms".into()).into(),
        );
    }
    let signature = build_areal_signature(
        graph,
        layout,
        &wall_atom_ids,
        &graph.points,
        &graph.topology,
        &graph.edge_of_slot,
        full_graph_faces,
        work,
    )?;
    Ok(ArealProof {
        wall_atom_ids,
        signature,
    })
}

fn build_areal_signature(
    graph: &AdmittedGraph,
    layout: &LogicalLayout,
    wall_atom_ids: &[u32],
    points: &ExactPoints,
    full_topology: &IndexedTopology,
    full_edge_of_slot: &[u32],
    full_graph_faces: Option<&GlobalFaces>,
    work: &mut impl Work,
) -> Result<ArealSignature> {
    let all_atoms_are_walls = wall_covers_graph(wall_atom_ids, graph.edge_nodes.len());
    let (projected_edges, projected_topology, projected_edge_of_slot) = if all_atoms_are_walls {
        (Vec::new(), None, Vec::new())
    } else {
        work.charge_into_error(
            u64::try_from(wall_atom_ids.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut projected_edges = Vec::with_capacity(wall_atom_ids.len());
        for &atom in wall_atom_ids {
            work.charge_into_error(1)?;
            projected_edges.push(*graph.edge_nodes.get(atom as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("areal logical wall names an unknown atom".into())
            })?);
        }
        let (topology, edge_of_slot) = indexed_topology(points, &projected_edges, work)?;
        (projected_edges, Some(topology), edge_of_slot)
    };
    let (wall_edges, wall_topology, wall_edge_of_slot) = if all_atoms_are_walls {
        (
            graph.edge_nodes.as_slice(),
            full_topology,
            full_edge_of_slot,
        )
    } else {
        (
            projected_edges.as_slice(),
            projected_topology.as_ref().ok_or_else(|| {
                GeometryErrorKind::Invalid("areal wall topology was not built".into())
            })?,
            projected_edge_of_slot.as_slice(),
        )
    };
    let computed_faces = if all_atoms_are_walls && full_graph_faces.is_some() {
        None
    } else {
        Some(global_faces(
            points,
            wall_topology,
            wall_edges,
            wall_edge_of_slot,
            work,
        )?)
    };
    let faces = full_graph_faces
        .filter(|_| all_atoms_are_walls)
        .or(computed_faces.as_ref())
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("required areal wall faces were not constructed".into())
        })?;
    let (face_keys, stable_face_id) = stable_wall_face_keys(
        wall_topology,
        faces,
        wall_edges,
        wall_edge_of_slot,
        wall_atom_ids,
        work,
    )?;
    // `graph_global_faces_with_work` reserves global face 0 for Exterior.
    let exterior_face = *stable_face_id.first().ok_or_else(|| {
        GeometryErrorKind::Invalid("areal wall graph has no Exterior face".into())
    })?;

    let mut component_count = 0_usize;
    for &component in &wall_topology.component_of {
        work.charge_into_error(1)?;
        component_count = component_count.max(
            usize::try_from(component)
                .map_err(|_| ResourceError::SizeOverflow)?
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?,
        );
    }
    if faces.component_hosts.len() != component_count {
        return Err(GeometryErrorKind::Invalid(
            "areal wall hosts do not align with their components".into(),
        )
        .into());
    }
    work.charge_into_error(
        u64::try_from(points.exact.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut host_by_vertex = Vec::with_capacity(points.exact.len());
    for &component in &wall_topology.component_of {
        work.charge_into_error(1)?;
        let face = *faces
            .component_hosts
            .get(component as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("areal wall component has no host face".into())
            })? as usize;
        host_by_vertex.push(*stable_face_id.get(face).ok_or_else(|| {
            GeometryErrorKind::Invalid("areal host face has no stable identity".into())
        })?);
    }

    let fan_vertices = areal_vertices(layout, &graph.edge_nodes, work)?;
    let fan_rows = stable_fan_rows(
        full_topology,
        &graph.edge_nodes,
        full_edge_of_slot,
        &fan_vertices,
        work,
    )?;
    Ok(ArealSignature {
        face_keys,
        exterior_face,
        host_by_vertex,
        fan_rows,
    })
}

fn outward_bounds(point: &ExactChartPoint, work: &mut impl Work) -> Result<Bounds> {
    #[cfg(test)]
    record_bounds_attempt();
    let [min_x, min_y, max_x, max_y] = point.outward_bounds_with_work(work)?;
    #[cfg(test)]
    record_bounds_complete();
    Ok(Bounds::new_raw(min_x, min_y, max_x, max_y))
}

fn outward_segment_bounds(
    start: &ExactChartPoint,
    end: &ExactChartPoint,
    work: &mut impl Work,
) -> Result<Bounds> {
    #[cfg(test)]
    record_bounds_attempt();
    let [start_min_x, start_min_y, start_max_x, start_max_y] =
        start.outward_bounds_with_work(work)?;
    let [end_min_x, end_min_y, end_max_x, end_max_y] = end.outward_bounds_with_work(work)?;
    #[cfg(test)]
    record_bounds_complete();
    Ok(Bounds::new_raw(
        start_min_x.min(end_min_x),
        start_min_y.min(end_min_y),
        start_max_x.max(end_max_x),
        start_max_y.max(end_max_y),
    ))
}

fn point_edge_contacts_preserved(
    reference_points: &ExactPoints,
    candidate_points: &ExactPoints,
    isolated_vertices: &[u32],
    edge_nodes: &[[u32; 2]],
    work: &mut impl Work,
) -> Result<bool> {
    fn contacts(
        points: &ExactPoints,
        isolated_vertices: &[u32],
        edge_nodes: &[[u32; 2]],
        work: &mut impl Work,
    ) -> Result<BTreeSet<(u32, usize)>> {
        work.charge_into_error(
            u64::try_from(isolated_vertices.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut point_bounds = Vec::with_capacity(isolated_vertices.len());
        for &vertex in isolated_vertices {
            work.charge_into_error(1)?;
            let point = points.exact.get(vertex as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("isolated graph point is unknown".into())
            })?;
            point_bounds.push(outward_bounds(point, work)?);
        }
        work.charge_into_error(
            u64::try_from(edge_nodes.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut edge_bounds = Vec::with_capacity(edge_nodes.len());
        for &[start, end] in edge_nodes {
            work.charge_into_error(1)?;
            edge_bounds.push(outward_segment_bounds(
                points.exact.get(start as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("point-contact edge start is unknown".into())
                })?,
                points.exact.get(end as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("point-contact edge end is unknown".into())
                })?,
                work,
            )?);
        }
        let mut result = BTreeSet::new();
        let completion = for_each_overlapping_bounds_cross_pair_with_work(
            &point_bounds,
            &edge_bounds,
            work,
            |point_index, edge_index, work| {
                work.charge_into_error(1)?;
                let Some(vertex) = isolated_vertices.get(point_index).copied() else {
                    return Err(GeometryErrorKind::Invalid(
                        "point-contact sweep returned an unknown point".into(),
                    )
                    .into());
                };
                let Some(edge) = edge_nodes.get(edge_index) else {
                    return Err(GeometryErrorKind::Invalid(
                        "point-contact sweep returned an unknown edge".into(),
                    )
                    .into());
                };
                let [start, end] = *edge;
                let (Some(point), Some(start), Some(end)) = (
                    points.exact.get(vertex as usize),
                    points.exact.get(start as usize),
                    points.exact.get(end as usize),
                ) else {
                    return Err(GeometryErrorKind::Invalid(
                        "point-contact graph row is outside its point inventory".into(),
                    )
                    .into());
                };
                if point_on_exact_segment_with_work(point, start, end, work)? {
                    work.charge_into_error(1)?;
                    result.insert((vertex, edge_index));
                }
                Ok(ControlFlow::<()>::Continue(()))
            },
        )?;
        debug_assert!(completion.is_continue());
        Ok(result)
    }

    Ok(
        contacts(reference_points, isolated_vertices, edge_nodes, work)?
            == contacts(candidate_points, isolated_vertices, edge_nodes, work)?,
    )
}

fn edges_have_only_vertex_contacts(
    points: &ExactPoints,
    edge_nodes: &[[u32; 2]],
    work: &mut impl Work,
) -> Result<bool> {
    work.charge_into_error(
        u64::try_from(edge_nodes.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut bounds = Vec::with_capacity(edge_nodes.len());
    for &[start, end] in edge_nodes {
        work.charge_into_error(1)?;
        let start = points
            .exact
            .get(start as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("candidate edge start is unknown".into()))?;
        let end = points
            .exact
            .get(end as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("candidate edge end is unknown".into()))?;
        #[cfg(test)]
        record_identity_attempt();
        let same_position = start.try_eq_with_work(end, work)?;
        #[cfg(test)]
        record_identity_complete();
        if same_position {
            return Ok(false);
        }
        bounds.push(outward_segment_bounds(start, end, work)?);
    }
    let completion = for_each_overlapping_bounds_cross_pair_with_work(
        &bounds,
        &bounds,
        work,
        |left_index, right_index, work| {
            if left_index >= right_index {
                return Ok(ControlFlow::Continue(()));
            }
            work.charge_into_error(1)?;
            let (Some([left_start, left_end]), Some([right_start, right_end])) =
                (edge_nodes.get(left_index), edge_nodes.get(right_index))
            else {
                return Err(GeometryErrorKind::Invalid(
                    "candidate edge sweep returned an unknown edge".into(),
                )
                .into());
            };
            let (
                Some(left_start_point),
                Some(left_end_point),
                Some(right_start_point),
                Some(right_end_point),
            ) = (
                points.exact.get(*left_start as usize),
                points.exact.get(*left_end as usize),
                points.exact.get(*right_start as usize),
                points.exact.get(*right_end as usize),
            )
            else {
                return Err(GeometryErrorKind::Invalid(
                    "candidate edge endpoint is outside its point inventory".into(),
                )
                .into());
            };
            match exact_segments_intersect_with_work(
                left_start_point,
                left_end_point,
                right_start_point,
                right_end_point,
                work,
            ) {
                Err(error) => Err(error),
                Ok(false) => Ok(ControlFlow::Continue(())),
                Ok(true) => {
                    let shared = [*left_start, *left_end]
                        .into_iter()
                        .find(|vertex| [*right_start, *right_end].contains(vertex));
                    let Some(shared) = shared else {
                        return Ok(ControlFlow::Break(()));
                    };
                    let left_other = if *left_start == shared {
                        *left_end
                    } else {
                        *left_start
                    };
                    let right_other = if *right_start == shared {
                        *right_end
                    } else {
                        *right_start
                    };
                    let (Some(left_other_point), Some(right_other_point)) = (
                        points.exact.get(left_other as usize),
                        points.exact.get(right_other as usize),
                    ) else {
                        return Err(GeometryErrorKind::Invalid(
                            "candidate edge has no nonshared endpoint".into(),
                        )
                        .into());
                    };
                    let left_on_right = point_on_exact_segment_with_work(
                        left_other_point,
                        right_start_point,
                        right_end_point,
                        work,
                    );
                    let right_on_left = point_on_exact_segment_with_work(
                        right_other_point,
                        left_start_point,
                        left_end_point,
                        work,
                    );
                    match (left_on_right, right_on_left) {
                        (Ok(false), Ok(false)) => Ok(ControlFlow::Continue(())),
                        (Ok(_), Ok(_)) => Ok(ControlFlow::Break(())),
                        (Err(error), _) | (_, Err(error)) => Err(error),
                    }
                },
            }
        },
    )?;
    Ok(completion.is_continue())
}

#[cfg(test)]
mod tests {
    use super::{
        AdmittedGraph, AdmittedLayout, CheckedImages, ExactPoints, FrozenLiteralBuffers,
        LiteralChart, LiteralCheck, LiteralPoint, LiteralPolygon, LiteralSequence,
        LogicalArealWalk, ShapeLiteralLayout, admit_candidate, admit_known_graph,
        admit_logical_layout, bind_literals, edges_have_only_vertex_contacts,
        point_edge_contacts_preserved,
    };
    use crate::error::{ErrorKind, Result};
    use crate::geometry::embedding_search::{EvaluationBudget, EvaluationOnly};
    use crate::geometry::segment_index::sweep::{
        CrossPairProbeProgress, cross_pair_probe_progress, reset_cross_pair_probe_progress,
    };
    use crate::geometry::tessellation::exact::{ExactDyadic, ExactRational};
    use crate::geometry::topology::ExactChartPoint;
    use crate::geometry::{
        CoordSeq, CoordinateAxes, GeometryErrorKind, LineSeq, Point, Polygon, Ring, Shape,
        Unmetered, Work, XY,
    };

    #[derive(Clone, Copy)]
    struct SweepCharge {
        spent_before: u128,
        ticks: u128,
        progress: CrossPairProbeProgress,
        contact_progress: super::ContactCheckProgress,
    }

    struct SweepRecordingWork<'a> {
        budget: &'a mut EvaluationBudget,
        spent: u128,
        charges: Vec<SweepCharge>,
        refused: Option<SweepCharge>,
    }

    impl<'a> SweepRecordingWork<'a> {
        fn new(budget: &'a mut EvaluationBudget) -> Self {
            Self {
                budget,
                spent: 0,
                charges: Vec::new(),
                refused: None,
            }
        }

        fn record(&mut self, ticks: u128, result: &Result<()>) {
            let event = SweepCharge {
                spent_before: self.spent,
                ticks,
                progress: cross_pair_probe_progress(),
                contact_progress: super::contact_check_progress(),
            };
            if result.is_ok() {
                self.charges.push(event);
                self.spent = self.spent.wrapping_add(ticks);
            } else {
                self.refused = Some(event);
            }
        }
    }

    impl Work for SweepRecordingWork<'_> {
        type Error = crate::error::Error;

        fn charge(&mut self, ticks: u64) -> Result<()> {
            let result = <EvaluationBudget as Work>::charge(self.budget, ticks);
            self.record(u128::from(ticks), &result);
            result
        }

        fn charge_product(&mut self, count: usize, ticks_per_item: usize) -> Result<()> {
            let ticks = (count as u128).wrapping_mul(ticks_per_item as u128);
            let result =
                <EvaluationBudget as Work>::charge_product(self.budget, count, ticks_per_item);
            self.record(ticks, &result);
            result
        }
    }

    fn budget_refusing_before(operation: &'static str, event: &SweepCharge) -> EvaluationBudget {
        let remaining = event
            .spent_before
            .checked_add(event.ticks)
            .and_then(|ticks| ticks.checked_sub(1))
            .expect("the target sweep charge is positive and fits the budget");
        EvaluationBudget::with_remaining_ticks_for_test(operation, 1, remaining)
            .expect("calibrated sweep budget is representable")
    }

    fn assert_sweep_limit(error: &crate::error::Error, operation: &'static str) {
        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
                operation: actual,
                ..
            }) if *actual == operation
        ));
    }

    fn isolated_contact_result<W: Work>(work: &mut W) -> Result<bool> {
        let points = points(&[
            (0.0, 0.0),
            (10.0, 10.0),
            (100.0, 100.0),
            (101.0, 100.0),
            (200.0, 200.0),
            (201.0, 200.0),
        ]);
        point_edge_contacts_preserved(&points, &points, &[0, 1], &[[2, 3], [4, 5]], work)
    }

    fn edge_contact_result<W: Work>(
        coordinates: &[(f64, f64)],
        edges: &[[u32; 2]],
        work: &mut W,
    ) -> Result<bool> {
        edges_have_only_vertex_contacts(&points(coordinates), edges, work)
    }

    fn points(values: &[(f64, f64)]) -> ExactPoints {
        let mut work = Unmetered;
        ExactPoints::from_exact(
            values
                .iter()
                .map(|&(x, y)| {
                    ExactChartPoint::from_rationals(
                        ExactRational::from_f64(x),
                        ExactRational::from_f64(y),
                    )
                })
                .collect(),
            &mut work,
        )
        .unwrap()
    }

    fn exact_points(values: &[(ExactRational, ExactRational)]) -> ExactPoints {
        let mut work = Unmetered;
        ExactPoints::from_exact(
            values
                .iter()
                .map(|(x, y)| ExactChartPoint::from_rationals(x.clone(), y.clone()))
                .collect(),
            &mut work,
        )
        .unwrap()
    }

    fn near_one_exact_chart_point() -> ExactChartPoint {
        let x = ExactDyadic::from_f64(1.0)
            .try_add_with_work(ExactDyadic::from_f64(2.0_f64.powi(-256)), &mut Unmetered)
            .expect("the test rational is representable");
        ExactChartPoint::from_rationals(ExactRational::from_dyadic(x), ExactRational::from_f64(0.0))
    }

    struct ImageRows(Vec<ExactChartPoint>);

    impl CheckedImages<u32> for ImageRows {
        fn image(&self, image: u32) -> Result<&ExactChartPoint> {
            self.0.get(image as usize).ok_or_else(|| {
                crate::geometry::GeometryErrorKind::Invalid(
                    "test literal image is outside its receipt".into(),
                )
                .into()
            })
        }
    }

    struct IdentityLiteralChart;

    impl LiteralChart<u32> for IdentityLiteralChart {
        fn read_point<W: crate::geometry::Work>(
            &mut self,
            _output: u32,
            _image: u32,
            record: Point,
            _expected_axes: CoordinateAxes,
            _stored_axes: CoordinateAxes,
            _work: &mut W,
        ) -> Result<Option<ExactChartPoint>> {
            Ok(Some(ExactChartPoint::from_rationals(
                ExactRational::from_f64(record.x),
                ExactRational::from_f64(record.y),
            )))
        }
    }

    struct TestLiteralReceiver {
        images: ImageRows,
        chart: IdentityLiteralChart,
    }

    impl CheckedImages<u32> for TestLiteralReceiver {
        fn image(&self, image: u32) -> Result<&ExactChartPoint> {
            self.images.image(image)
        }
    }

    struct EvaluationRecorder {
        budget: EvaluationBudget,
        successful_charges: usize,
        refusals: usize,
        charges_after_refusal: usize,
        refused: bool,
    }

    impl EvaluationRecorder {
        fn record(&mut self, result: Result<()>) -> Result<()> {
            match result {
                Ok(()) => {
                    self.successful_charges = self
                        .successful_charges
                        .checked_add(1)
                        .ok_or(crate::error::ResourceError::SizeOverflow)?;
                    Ok(())
                },
                Err(error) => {
                    self.refusals = self
                        .refusals
                        .checked_add(1)
                        .ok_or(crate::error::ResourceError::SizeOverflow)?;
                    self.refused = true;
                    Err(error)
                },
            }
        }
    }

    impl Work for EvaluationRecorder {
        type Error = crate::error::Error;

        fn charge(&mut self, ticks: u64) -> Result<()> {
            if self.refused {
                self.charges_after_refusal = self
                    .charges_after_refusal
                    .checked_add(1)
                    .ok_or(crate::error::ResourceError::SizeOverflow)?;
            }
            let result = self.budget.charge(ticks);
            self.record(result)
        }

        fn charge_product(&mut self, count: usize, ticks_per_item: usize) -> Result<()> {
            if self.refused {
                self.charges_after_refusal = self
                    .charges_after_refusal
                    .checked_add(1)
                    .ok_or(crate::error::ResourceError::SizeOverflow)?;
            }
            let result = self.budget.charge_product(count, ticks_per_item);
            self.record(result)
        }
    }

    impl LiteralChart<u32> for TestLiteralReceiver {
        fn read_point<W: crate::geometry::Work>(
            &mut self,
            output: u32,
            image: u32,
            record: Point,
            expected_axes: CoordinateAxes,
            stored_axes: CoordinateAxes,
            work: &mut W,
        ) -> Result<Option<ExactChartPoint>> {
            self.chart
                .read_point(output, image, record, expected_axes, stored_axes, work)
        }
    }

    fn polygon_layout(graph: &AdmittedGraph, vertices: &[u32]) -> AdmittedLayout {
        admit_logical_layout(
            graph,
            vec![LogicalArealWalk {
                output: 0,
                walk: 0,
                vertices: vertices.to_vec(),
            }],
            None,
            &mut Unmetered,
        )
        .unwrap()
    }

    #[test]
    fn independent_overlapping_logical_rings_bind_across_wall_faces() {
        let coordinates = [
            (0.0, 0.0),
            (2.0, 0.0),
            (3.0, 0.0),
            (4.0, 0.0),
            (4.0, 4.0),
            (3.0, 4.0),
            (2.0, 4.0),
            (0.0, 4.0),
            (2.0, -1.0),
            (3.0, -1.0),
            (3.0, 5.0),
            (2.0, 5.0),
        ];
        let atoms = vec![
            [0, 1],
            [1, 2],
            [2, 3],
            [3, 4],
            [4, 5],
            [5, 6],
            [6, 7],
            [7, 0],
            [8, 9],
            [9, 2],
            [2, 5],
            [5, 10],
            [10, 11],
            [11, 6],
            [6, 1],
            [1, 8],
        ];
        let mut work = Unmetered;
        let graph =
            admit_known_graph(points(&coordinates), atoms, (0..12).collect(), &mut work).unwrap();
        let layout = admit_logical_layout(
            &graph,
            vec![
                LogicalArealWalk {
                    output: 0,
                    walk: 0,
                    vertices: vec![0, 1, 2, 3, 4, 5, 6, 7, 0],
                },
                LogicalArealWalk {
                    output: 1,
                    walk: 0,
                    vertices: vec![8, 9, 2, 5, 10, 11, 6, 1, 8],
                },
            ],
            None,
            &mut work,
        )
        .unwrap();
        assert_eq!(
            layout
                .areal
                .as_ref()
                .map(|proof| proof.signature.face_keys.len()),
            Some(6),
            "the wall arrangement has Exterior and five bounded faces"
        );
        assert!(
            admit_candidate(&graph, &layout, graph.points.clone(), &mut work)
                .unwrap()
                .is_some(),
            "the unchanged noded reference must admit both complete ring walks"
        );

        let affine = points(&coordinates.map(|(x, y)| (x + y, y)));
        assert!(
            admit_candidate(&graph, &layout, affine, &mut work)
                .unwrap()
                .is_some(),
            "an exact orientation-preserving affine image keeps the same wall correspondence"
        );

        let make_ring = |vertices: &[u32]| -> Result<Ring> {
            let records = vertices
                .iter()
                .map(|&vertex| {
                    let xy = graph
                        .points
                        .rounded
                        .get(vertex as usize)
                        .copied()
                        .ok_or_else(|| {
                            crate::geometry::GeometryErrorKind::Invalid(
                                "overlapping collection ring vertex is unknown".into(),
                            )
                        })
                        .unwrap();
                    Ok(Point::new_raw_xy(xy.x, xy.y))
                })
                .collect::<Result<Vec<_>>>()
                .unwrap();
            Ring::try_from_closed(CoordSeq::try_from_points(&records).unwrap())
        };
        let left_vertices = [0, 1, 2, 3, 4, 5, 6, 7, 0];
        let right_vertices = [8, 9, 2, 5, 10, 11, 6, 1, 8];
        let sink = Shape::geometry_collection(vec![
            Shape::Polygon(Polygon::new(make_ring(&left_vertices).unwrap(), Vec::new())),
            Shape::Polygon(Polygon::new(
                make_ring(&right_vertices).unwrap(),
                Vec::new(),
            )),
        ])
        .unwrap();
        let literal_layout = ShapeLiteralLayout {
            points: Vec::new(),
            lines: Vec::new(),
            polygons: vec![
                LiteralPolygon {
                    output: 0,
                    axes: CoordinateAxes::XY,
                    shell: LiteralSequence {
                        output: 0,
                        axes: CoordinateAxes::XY,
                        images: vec![0, 1, 2, 3, 4, 5, 6, 7, 0],
                    },
                    holes: Vec::new(),
                },
                LiteralPolygon {
                    output: 1,
                    axes: CoordinateAxes::XY,
                    shell: LiteralSequence {
                        output: 1,
                        axes: CoordinateAxes::XY,
                        images: vec![8, 9, 2, 5, 10, 11, 6, 1, 8],
                    },
                    holes: Vec::new(),
                },
            ],
        };
        let mut receiver = TestLiteralReceiver {
            images: ImageRows(graph.points.exact.clone()),
            chart: IdentityLiteralChart,
        };
        assert!(matches!(
            bind_literals(
                FrozenLiteralBuffers::shapes(sink, literal_layout),
                &mut receiver,
                &mut work,
            )
            .unwrap(),
            LiteralCheck::Pass(_)
        ));
    }

    #[test]
    fn exterior_identity_tracks_global_face_zero_not_the_first_stable_key() {
        let mut work = Unmetered;
        let graph = admit_known_graph(
            points(&[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0)]),
            vec![[0, 1], [1, 2], [2, 3], [3, 0]],
            vec![0, 1, 2, 3],
            &mut work,
        )
        .unwrap();
        let layout = polygon_layout(&graph, &[0, 1, 2, 3, 0]);
        let proof = layout.areal.as_ref().unwrap();
        let [interior, exterior] = proof.signature.face_keys.as_slice() else {
            panic!("the square has distinct interior and exterior wall faces");
        };

        // On this CCW square the interior's stable boundary starts at dart 0,
        // so it sorts before the unbounded face's stable boundary at dart 1.
        assert_eq!(interior.first(), Some(&0));
        assert_eq!(exterior.first(), Some(&1));
        assert_eq!(proof.signature.exterior_face, 1);
    }

    #[test]
    fn areal_fan_uses_cyclic_order_not_opposed_ray_identity() {
        let mut work = Unmetered;
        let graph_points = points(&[(0.0, 0.0), (2.0, 0.0), (0.0, 2.0), (-1.0, 0.0), (0.0, -1.0)]);
        let graph = admit_known_graph(
            graph_points,
            vec![[0, 1], [1, 2], [2, 0], [0, 3], [0, 4]],
            vec![0, 1, 2, 3, 4],
            &mut work,
        )
        .unwrap();
        let layout = polygon_layout(&graph, &[0, 1, 2, 0]);

        // A half-turn changes the angular sort cut, not the anchored stable-dart cycle.
        let rotated = points(&[(0.0, 0.0), (-2.0, 0.0), (0.0, -2.0), (1.0, 0.0), (0.0, 1.0)]);
        assert!(
            admit_candidate(&graph, &layout, rotated.clone(), &mut work)
                .unwrap()
                .is_some()
        );

        // The area triangle stays fixed; its attached line rays bend through
        // opposite directions while retaining their cyclic order.
        let opposition_changed = points(&[
            (0.0, 0.0),
            (2.0, 0.0),
            (0.0, 2.0),
            (-1.0, -1.0),
            (1.0, -1.0),
        ]);
        assert!(
            admit_candidate(&graph, &layout, opposition_changed, &mut work)
                .unwrap()
                .is_some()
        );

        // Swapping the two stable line darts changes the actual rotation.
        let reordered = points(&[(0.0, 0.0), (2.0, 0.0), (0.0, 2.0), (0.0, -1.0), (-1.0, 0.0)]);
        assert!(
            admit_candidate(&graph, &layout, reordered, &mut work)
                .unwrap()
                .is_none()
        );
        assert_eq!(rotated.rounded[0], XY::new(0.0, 0.0));
    }

    #[test]
    fn known_graph_accepts_admitted_midpoint_repair_that_changes_opposition() {
        let two_to_53 = 1_i64 << 53;
        let ratio = |numerator, denominator| {
            ExactRational::new(
                ExactDyadic::from_i64(numerator),
                ExactDyadic::from_i64(denominator),
            )
            .unwrap()
        };
        let one = ExactRational::one();
        let zero = ExactRational::zero();
        let quarter = ratio(1, 4);
        let half = ratio(1, 2);
        let three_quarters = ratio(3, 4);
        let midpoint_x = ratio(two_to_53 + 1, two_to_53);
        let upper_x = ratio(two_to_53 + 2, two_to_53);
        let reference_points = exact_points(&[
            (one.clone(), zero),                // A
            (midpoint_x.clone(), half.clone()), // B
            (upper_x.clone(), one.clone()),     // C
            (midpoint_x, three_quarters),       // F
            (one.clone(), half),                // D
            (one, quarter),                     // E
        ]);
        let mut work = Unmetered;
        let graph = admit_known_graph(
            reference_points,
            vec![[0, 1], [1, 2], [2, 3], [3, 4], [4, 5], [5, 0]],
            vec![0, 1, 2, 3, 4, 5],
            &mut work,
        )
        .unwrap();
        let layout = polygon_layout(&graph, &[0, 1, 2, 3, 4, 5, 0]);

        let nearest = exact_points(&[
            (ExactRational::from_f64(1.0), ExactRational::zero()),
            (ExactRational::from_f64(1.0), ExactRational::from_f64(0.5)),
            (upper_x.clone(), ExactRational::one()),
            (ExactRational::from_f64(1.0), ExactRational::from_f64(0.75)),
            (ExactRational::from_f64(1.0), ExactRational::from_f64(0.5)),
            (ExactRational::from_f64(1.0), ExactRational::from_f64(0.25)),
        ]);
        assert!(
            admit_candidate(&graph, &layout, nearest, &mut work)
                .unwrap()
                .is_none()
        );

        let repaired = exact_points(&[
            (ExactRational::from_f64(1.0), ExactRational::zero()),
            (upper_x, ExactRational::from_f64(0.5)),
            (
                ExactRational::from_f64(1.0 + 2.0_f64.powi(-52)),
                ExactRational::one(),
            ),
            (ExactRational::from_f64(1.0), ExactRational::from_f64(0.75)),
            (ExactRational::from_f64(1.0), ExactRational::from_f64(0.5)),
            (ExactRational::from_f64(1.0), ExactRational::from_f64(0.25)),
        ]);
        let Some(assignment) =
            admit_candidate(&graph, &layout, repaired.clone(), &mut work).unwrap()
        else {
            panic!("the prescribed upper-neighbor assignment preserves this known graph");
        };
        let ring_points = [0_u32, 1, 2, 3, 4, 5, 0]
            .into_iter()
            .map(|index| {
                let point = repaired
                    .rounded
                    .get(index as usize)
                    .copied()
                    .ok_or_else(|| {
                        crate::geometry::GeometryErrorKind::Invalid(
                            "repaired point is outside its written ring".into(),
                        )
                    })?;
                Ok(Point::new_raw_xy(point.x, point.y))
            })
            .collect::<Result<Vec<_>>>()
            .unwrap();
        let shape = Shape::Polygon(Polygon::new(
            Ring::try_from_closed(CoordSeq::try_from_points(&ring_points).unwrap()).unwrap(),
            Vec::new(),
        ));
        let layout = ShapeLiteralLayout {
            points: Vec::new(),
            lines: Vec::new(),
            polygons: vec![LiteralPolygon {
                output: 0,
                axes: CoordinateAxes::XY,
                shell: LiteralSequence {
                    output: 0,
                    axes: CoordinateAxes::XY,
                    images: vec![0, 1, 2, 3, 4, 5, 0],
                },
                holes: Vec::new(),
            }],
        };
        let mut receiver = TestLiteralReceiver {
            images: ImageRows(assignment.points().exact.clone()),
            chart: IdentityLiteralChart,
        };
        assert!(matches!(
            bind_literals(
                FrozenLiteralBuffers::shapes(shape, layout),
                &mut receiver,
                &mut work,
            )
            .unwrap(),
            LiteralCheck::Pass(_)
        ));
    }

    #[test]
    fn degree_three_areal_fan_accepts_opposition_change_without_reordering() {
        let two_to_53 = 1_i64 << 53;
        let epsilon =
            ExactRational::new(ExactDyadic::one(), ExactDyadic::from_i64(two_to_53)).unwrap();
        let constructed_y = ExactRational::one().try_add(&epsilon).unwrap();
        let reference = exact_points(&[
            (ExactRational::zero(), ExactRational::zero()), // O
            (ExactRational::one(), ExactRational::from_f64(-1.0)), // P
            (ExactRational::zero(), ExactRational::one()),  // R
            (ExactRational::from_f64(-1.0), constructed_y), // Q
            (ExactRational::from_f64(-1.0), ExactRational::one()), // fixed collision
        ]);
        let mut work = Unmetered;
        let graph = admit_known_graph(
            reference,
            vec![[0, 1], [1, 2], [2, 0], [0, 3]],
            vec![0, 1, 2, 3, 4],
            &mut work,
        )
        .unwrap();
        let layout = polygon_layout(&graph, &[0, 1, 2, 0]);
        let nearest = points(&[
            (0.0, 0.0),
            (1.0, -1.0),
            (0.0, 1.0),
            (-1.0, 1.0),
            (-1.0, 1.0),
        ]);
        assert!(
            admit_candidate(&graph, &layout, nearest, &mut work)
                .unwrap()
                .is_none()
        );
        let upper = points(&[
            (0.0, 0.0),
            (1.0, -1.0),
            (0.0, 1.0),
            (-1.0, 1.0 + 2.0_f64.powi(-52)),
            (-1.0, 1.0),
        ]);
        assert!(
            admit_candidate(&graph, &layout, upper, &mut work)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn reflected_closed_line_seals_beside_an_unrelated_polygon() {
        let half_ulp =
            ExactRational::new(ExactDyadic::one(), ExactDyadic::from_i64(1_i64 << 54)).unwrap();
        let line_apex_y = ExactRational::one().try_add(&half_ulp).unwrap();
        let reference = exact_points(&[
            (ExactRational::from_f64(-4.0), ExactRational::from_f64(-4.0)),
            (ExactRational::from_f64(-2.0), ExactRational::from_f64(-4.0)),
            (ExactRational::from_f64(-2.0), ExactRational::from_f64(-2.0)),
            (ExactRational::from_f64(-4.0), ExactRational::from_f64(-2.0)),
            (ExactRational::zero(), ExactRational::one()),
            (ExactRational::from_f64(2.0), ExactRational::one()),
            (ExactRational::one(), line_apex_y),
        ]);
        let mut work = Unmetered;
        let graph = admit_known_graph(
            reference,
            vec![
                [0, 1],
                [1, 2],
                [2, 3],
                [3, 0], // polygon wall
                [4, 5],
                [5, 6],
                [6, 4], // separate closed line
            ],
            (0..7).collect(),
            &mut work,
        )
        .unwrap();
        let layout = polygon_layout(&graph, &[0, 1, 2, 3, 0]);

        let nearest = points(&[
            (-4.0, -4.0),
            (-2.0, -4.0),
            (-2.0, -2.0),
            (-4.0, -2.0),
            (0.0, 1.0),
            (2.0, 1.0),
            (1.0, 1.0),
        ]);
        assert!(
            admit_candidate(&graph, &layout, nearest, &mut work)
                .unwrap()
                .is_none()
        );

        let lower = 1.0 - 2.0_f64.powi(-53);
        let repaired = points(&[
            (-4.0, -4.0),
            (-2.0, -4.0),
            (-2.0, -2.0),
            (-4.0, -2.0),
            (0.0, 1.0),
            (2.0, 1.0),
            (1.0, lower),
        ]);
        assert!(
            admit_candidate(&graph, &layout, repaired.clone(), &mut work)
                .unwrap()
                .is_some()
        );

        let polygon = Shape::Polygon(Polygon::new(
            Ring::try_from_closed(
                CoordSeq::try_from_points(&[
                    Point::new_raw_xy(-4.0, -4.0),
                    Point::new_raw_xy(-2.0, -4.0),
                    Point::new_raw_xy(-2.0, -2.0),
                    Point::new_raw_xy(-4.0, -2.0),
                    Point::new_raw_xy(-4.0, -4.0),
                ])
                .unwrap(),
            )
            .unwrap(),
            Vec::new(),
        ));
        let line = Shape::LineString(LineSeq::from_proven(
            CoordSeq::try_from_points(&[
                Point::new_raw_xy(0.0, 1.0),
                Point::new_raw_xy(2.0, 1.0),
                Point::new_raw_xy(1.0, lower),
                Point::new_raw_xy(0.0, 1.0),
            ])
            .unwrap(),
        ));
        let sink = Shape::geometry_collection(vec![polygon, line]).unwrap();
        let literal_layout = ShapeLiteralLayout {
            points: Vec::new(),
            lines: vec![LiteralSequence {
                output: 0,
                axes: CoordinateAxes::XY,
                images: vec![4, 5, 6, 4],
            }],
            polygons: vec![LiteralPolygon {
                output: 0,
                axes: CoordinateAxes::XY,
                shell: LiteralSequence {
                    output: 0,
                    axes: CoordinateAxes::XY,
                    images: vec![0, 1, 2, 3, 0],
                },
                holes: Vec::new(),
            }],
        };
        let mut receiver = TestLiteralReceiver {
            images: ImageRows(repaired.exact),
            chart: IdentityLiteralChart,
        };
        assert!(matches!(
            bind_literals(
                FrozenLiteralBuffers::shapes(sink, literal_layout),
                &mut receiver,
                &mut work,
            )
            .unwrap(),
            LiteralCheck::Pass(_)
        ));
    }

    #[test]
    fn point_leaving_a_closed_line_face_keeps_its_areal_host() {
        let half_ulp =
            ExactRational::new(ExactDyadic::one(), ExactDyadic::from_i64(1_i64 << 54)).unwrap();
        let point_y = ExactRational::one().try_add(&half_ulp).unwrap();
        let reference = exact_points(&[
            (ExactRational::from_f64(-4.0), ExactRational::from_f64(-4.0)),
            (ExactRational::from_f64(-2.0), ExactRational::from_f64(-4.0)),
            (ExactRational::from_f64(-2.0), ExactRational::from_f64(-2.0)),
            (ExactRational::from_f64(-4.0), ExactRational::from_f64(-2.0)),
            (ExactRational::zero(), ExactRational::one()),
            (ExactRational::from_f64(2.0), ExactRational::one()),
            (ExactRational::one(), ExactRational::from_f64(2.0)),
            (ExactRational::one(), point_y),
        ]);
        let mut work = Unmetered;
        let graph = admit_known_graph(
            reference,
            vec![
                [0, 1],
                [1, 2],
                [2, 3],
                [3, 0], // polygon wall
                [4, 5],
                [5, 6],
                [6, 4], // closed line, not a wall
            ],
            (0..8).collect(),
            &mut work,
        )
        .unwrap();
        let layout = polygon_layout(&graph, &[0, 1, 2, 3, 0]);

        let nearest = points(&[
            (-4.0, -4.0),
            (-2.0, -4.0),
            (-2.0, -2.0),
            (-4.0, -2.0),
            (0.0, 1.0),
            (2.0, 1.0),
            (1.0, 2.0),
            (1.0, 1.0),
        ]);
        assert!(
            admit_candidate(&graph, &layout, nearest, &mut work)
                .unwrap()
                .is_none()
        );

        let lower = 1.0 - 2.0_f64.powi(-53);
        let repaired = points(&[
            (-4.0, -4.0),
            (-2.0, -4.0),
            (-2.0, -2.0),
            (-4.0, -2.0),
            (0.0, 1.0),
            (2.0, 1.0),
            (1.0, 2.0),
            (1.0, lower),
        ]);
        assert!(
            admit_candidate(&graph, &layout, repaired.clone(), &mut work)
                .unwrap()
                .is_some()
        );

        let polygon = Shape::Polygon(Polygon::new(
            Ring::try_from_closed(
                CoordSeq::try_from_points(&[
                    Point::new_raw_xy(-4.0, -4.0),
                    Point::new_raw_xy(-2.0, -4.0),
                    Point::new_raw_xy(-2.0, -2.0),
                    Point::new_raw_xy(-4.0, -2.0),
                    Point::new_raw_xy(-4.0, -4.0),
                ])
                .unwrap(),
            )
            .unwrap(),
            Vec::new(),
        ));
        let line = Shape::LineString(LineSeq::from_proven(
            CoordSeq::try_from_points(&[
                Point::new_raw_xy(0.0, 1.0),
                Point::new_raw_xy(2.0, 1.0),
                Point::new_raw_xy(1.0, 2.0),
                Point::new_raw_xy(0.0, 1.0),
            ])
            .unwrap(),
        ));
        let point = Shape::Point(Point::new_raw_xy(1.0, lower));
        let sink = Shape::geometry_collection(vec![polygon, line, point]).unwrap();
        let literal_layout = ShapeLiteralLayout {
            points: vec![LiteralPoint {
                output: 0,
                axes: CoordinateAxes::XY,
                image: 7,
            }],
            lines: vec![LiteralSequence {
                output: 0,
                axes: CoordinateAxes::XY,
                images: vec![4, 5, 6, 4],
            }],
            polygons: vec![LiteralPolygon {
                output: 0,
                axes: CoordinateAxes::XY,
                shell: LiteralSequence {
                    output: 0,
                    axes: CoordinateAxes::XY,
                    images: vec![0, 1, 2, 3, 0],
                },
                holes: Vec::new(),
            }],
        };
        let mut receiver = TestLiteralReceiver {
            images: ImageRows(repaired.exact),
            chart: IdentityLiteralChart,
        };
        assert!(matches!(
            bind_literals(
                FrozenLiteralBuffers::shapes(sink, literal_layout),
                &mut receiver,
                &mut work,
            )
            .unwrap(),
            LiteralCheck::Pass(_)
        ));
    }

    #[test]
    fn moving_a_point_across_an_areal_wall_changes_its_host() {
        let half_ulp =
            ExactRational::new(ExactDyadic::one(), ExactDyadic::from_i64(1_i64 << 54)).unwrap();
        let point_x = ExactRational::one().try_add(&half_ulp).unwrap();
        let reference = exact_points(&[
            (ExactRational::zero(), ExactRational::zero()),
            (ExactRational::one(), ExactRational::zero()),
            (ExactRational::one(), ExactRational::from_f64(2.0)),
            (ExactRational::zero(), ExactRational::from_f64(2.0)),
            (point_x, ExactRational::one()),
        ]);
        let mut work = Unmetered;
        let graph = admit_known_graph(
            reference,
            vec![[0, 1], [1, 2], [2, 3], [3, 0]],
            vec![0, 1, 2, 3, 4],
            &mut work,
        )
        .unwrap();
        let layout = polygon_layout(&graph, &[0, 1, 2, 3, 0]);

        let nearest = points(&[(0.0, 0.0), (1.0, 0.0), (1.0, 2.0), (0.0, 2.0), (1.0, 1.0)]);
        assert!(
            admit_candidate(&graph, &layout, nearest, &mut work)
                .unwrap()
                .is_none()
        );

        let lower = points(&[
            (0.0, 0.0),
            (1.0, 0.0),
            (1.0, 2.0),
            (0.0, 2.0),
            (1.0 - 2.0_f64.powi(-53), 1.0),
        ]);
        assert!(
            admit_candidate(&graph, &layout, lower, &mut work)
                .unwrap()
                .is_none()
        );

        let upper = points(&[
            (0.0, 0.0),
            (1.0, 0.0),
            (1.0, 2.0),
            (0.0, 2.0),
            (1.0 + 2.0_f64.powi(-52), 1.0),
        ]);
        assert!(
            admit_candidate(&graph, &layout, upper, &mut work)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn attached_chord_between_wall_vertices_uses_the_areal_host_view() {
        let half_ulp =
            ExactRational::new(ExactDyadic::one(), ExactDyadic::from_i64(1_i64 << 54)).unwrap();
        let line_midpoint_y = ExactRational::one().try_add(&half_ulp).unwrap();
        let reference = exact_points(&[
            (ExactRational::zero(), ExactRational::zero()),
            (ExactRational::from_f64(2.0), ExactRational::zero()),
            (ExactRational::from_f64(2.0), ExactRational::from_f64(2.0)),
            (ExactRational::zero(), ExactRational::from_f64(2.0)),
            (ExactRational::one(), line_midpoint_y),
        ]);
        let mut work = Unmetered;
        let graph = admit_known_graph(
            reference,
            vec![[0, 1], [1, 2], [2, 3], [3, 0], [0, 4], [4, 2]],
            (0..5).collect(),
            &mut work,
        )
        .unwrap();
        let layout = polygon_layout(&graph, &[0, 1, 2, 3, 0]);

        let nearest = points(&[(0.0, 0.0), (2.0, 0.0), (2.0, 2.0), (0.0, 2.0), (1.0, 1.0)]);
        assert!(
            admit_candidate(&graph, &layout, nearest, &mut work)
                .unwrap()
                .is_some()
        );
        let lower = points(&[
            (0.0, 0.0),
            (2.0, 0.0),
            (2.0, 2.0),
            (0.0, 2.0),
            (1.0, 1.0 - 2.0_f64.powi(-53)),
        ]);
        assert!(
            admit_candidate(&graph, &layout, lower, &mut work)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn line_bridge_between_areal_components_keeps_each_wall_host() {
        let half_ulp =
            ExactRational::new(ExactDyadic::one(), ExactDyadic::from_i64(1_i64 << 54)).unwrap();
        let bridge_y = ExactRational::one().try_add(&half_ulp).unwrap();
        let reference = exact_points(&[
            (ExactRational::zero(), ExactRational::zero()), // left lower-left
            (ExactRational::one(), ExactRational::zero()),  // left lower-right
            (ExactRational::one(), ExactRational::one()),   // left upper-right
            (ExactRational::zero(), ExactRational::one()),  // left upper-left
            (ExactRational::from_f64(3.0), ExactRational::zero()), // right lower-left
            (ExactRational::from_f64(4.0), ExactRational::zero()), // right lower-right
            (ExactRational::from_f64(4.0), ExactRational::one()), // right upper-right
            (ExactRational::from_f64(3.0), ExactRational::one()), // right upper-left
            (ExactRational::from_f64(2.0), bridge_y),       // bridge vertex
        ]);
        let mut work = Unmetered;
        let graph = admit_known_graph(
            reference,
            vec![
                [0, 1],
                [1, 2],
                [2, 3],
                [3, 0],
                [4, 5],
                [5, 6],
                [6, 7],
                [7, 4],
                [2, 8],
                [8, 7],
            ],
            (0..9).collect(),
            &mut work,
        )
        .unwrap();
        let layout = admit_logical_layout(
            &graph,
            vec![
                LogicalArealWalk {
                    output: 0,
                    walk: 0,
                    vertices: vec![0, 1, 2, 3, 0],
                },
                LogicalArealWalk {
                    output: 1,
                    walk: 0,
                    vertices: vec![4, 5, 6, 7, 4],
                },
            ],
            None,
            &mut work,
        )
        .unwrap();

        let nearest = points(&[
            (0.0, 0.0),
            (1.0, 0.0),
            (1.0, 1.0),
            (0.0, 1.0),
            (3.0, 0.0),
            (4.0, 0.0),
            (4.0, 1.0),
            (3.0, 1.0),
            (2.0, 1.0),
        ]);
        assert!(
            admit_candidate(&graph, &layout, nearest, &mut work)
                .unwrap()
                .is_some()
        );
        let lower = points(&[
            (0.0, 0.0),
            (1.0, 0.0),
            (1.0, 1.0),
            (0.0, 1.0),
            (3.0, 0.0),
            (4.0, 0.0),
            (4.0, 1.0),
            (3.0, 1.0),
            (2.0, 1.0 - 2.0_f64.powi(-53)),
        ]);
        assert!(
            admit_candidate(&graph, &layout, lower, &mut work)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn line_only_cycle_does_not_acquire_areal_orientation_or_host_obligations() {
        let reference = points(&[(0.0, 0.0), (2.0, 0.0), (0.0, 2.0)]);
        let reversed = points(&[(0.0, 0.0), (2.0, 0.0), (0.0, -2.0)]);
        let edges = vec![[0, 1], [1, 2], [2, 0]];
        let identities = vec![0, 1, 2];
        let mut work = Unmetered;
        let line_graph = admit_known_graph(
            points(&[(0.0, 0.0), (2.0, 0.0), (0.0, 2.0)]),
            edges.clone(),
            identities.clone(),
            &mut work,
        )
        .unwrap();
        let line_layout = admit_logical_layout(&line_graph, Vec::new(), None, &mut work).unwrap();
        assert!(
            admit_candidate(&line_graph, &line_layout, reversed.clone(), &mut work)
                .unwrap()
                .is_some()
        );

        let polygon_graph = admit_known_graph(reference, edges, identities, &mut work).unwrap();
        let layout = polygon_layout(&polygon_graph, &[0, 1, 2, 0]);
        assert!(
            admit_candidate(&polygon_graph, &layout, reversed, &mut work)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn line_only_actual_shape_seals_while_reversed_polygon_face_is_rejected() {
        let reference = points(&[(0.0, 0.0), (2.0, 0.0), (0.0, 2.0)]);
        let reversed = points(&[(0.0, 0.0), (2.0, 0.0), (0.0, -2.0)]);
        let edges = vec![[0, 1], [1, 2], [2, 0]];
        let identities = vec![0, 1, 2];
        let mut work = Unmetered;
        let line_graph = admit_known_graph(
            points(&[(0.0, 0.0), (2.0, 0.0), (0.0, 2.0)]),
            edges.clone(),
            identities.clone(),
            &mut work,
        )
        .unwrap();
        let Some(_assignment) = admit_candidate(
            &line_graph,
            &admit_logical_layout(&line_graph, Vec::new(), None, &mut work).unwrap(),
            reversed.clone(),
            &mut work,
        )
        .unwrap() else {
            panic!("closed line accepts its preserved incidence after reflection");
        };

        let line = Shape::LineString(LineSeq::from_proven(
            CoordSeq::try_from_points(&[
                Point::new_raw_xy(0.0, 0.0),
                Point::new_raw_xy(2.0, 0.0),
                Point::new_raw_xy(0.0, -2.0),
                Point::new_raw_xy(0.0, 0.0),
            ])
            .unwrap(),
        ));
        let literal_layout = ShapeLiteralLayout {
            points: Vec::<LiteralPoint<u32>>::new(),
            lines: vec![LiteralSequence {
                output: 0,
                axes: CoordinateAxes::XY,
                images: vec![0, 1, 2, 0],
            }],
            polygons: Vec::new(),
        };
        let mut receiver = TestLiteralReceiver {
            images: ImageRows(vec![
                ExactChartPoint::from_rationals(
                    ExactRational::from_f64(0.0),
                    ExactRational::from_f64(0.0),
                ),
                ExactChartPoint::from_rationals(
                    ExactRational::from_f64(2.0),
                    ExactRational::from_f64(0.0),
                ),
                ExactChartPoint::from_rationals(
                    ExactRational::from_f64(0.0),
                    ExactRational::from_f64(-2.0),
                ),
            ]),
            chart: IdentityLiteralChart,
        };
        let sealed = match bind_literals(
            FrozenLiteralBuffers::shapes(line, literal_layout),
            &mut receiver,
            &mut work,
        )
        .unwrap()
        {
            LiteralCheck::Pass(sealed) => sealed,
            LiteralCheck::Reject(_) => panic!("actual closed-line slots match their images"),
        };
        assert!(matches!(sealed.into_shape(), Shape::LineString(_)));

        let polygon_graph = admit_known_graph(reference, edges, identities, &mut work).unwrap();
        let layout = polygon_layout(&polygon_graph, &[0, 1, 2, 0]);
        assert!(
            admit_candidate(&polygon_graph, &layout, reversed, &mut work)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn distinct_known_identities_cannot_collapse_to_one_candidate_point() {
        let mut work = Unmetered;
        let graph = admit_known_graph(
            points(&[(0.0, 0.0), (1.0, 0.0)]),
            Vec::new(),
            vec![0, 1],
            &mut work,
        )
        .unwrap();
        let collapsed = points(&[(0.0, 0.0), (0.0, 0.0)]);
        assert!(
            admit_candidate(
                &graph,
                &admit_logical_layout(&graph, Vec::new(), None, &mut work).unwrap(),
                collapsed,
                &mut work
            )
            .unwrap()
            .is_none()
        );
    }

    #[test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "the recorder assertions pin point-contact progress and refusal before the next probe"
    )]
    fn native_point_contact_sweep_pays_no_survivor_probes() -> Result<()> {
        const OPERATION: &str = "native_point_contact_sweep";
        reset_cross_pair_probe_progress();
        let (result, charges) = {
            let mut budget = EvaluationBudget::new(OPERATION, 1)?;
            let mut work = SweepRecordingWork::new(&mut budget);
            let result = isolated_contact_result(&mut work)?;
            (result, work.charges)
        };
        assert!(result);
        assert_eq!(cross_pair_probe_progress(), CrossPairProbeProgress {
            attempted: 8,
            completed: 8,
        });
        let target = charges
            .into_iter()
            .find(|event| {
                event.progress
                    == CrossPairProbeProgress {
                        attempted: 2,
                        completed: 1,
                    }
            })
            .expect("the paid sweep charges a rejected point-edge probe");

        reset_cross_pair_probe_progress();
        let mut budget = budget_refusing_before(OPERATION, &target);
        let error = {
            let mut work = SweepRecordingWork::new(&mut budget);
            let error = isolated_contact_result(&mut work)
                .expect_err("the no-survivor sweep refuses before its second probe");
            assert_eq!(
                work.refused.map(|event| event.progress),
                Some(target.progress)
            );
            error
        };
        assert_sweep_limit(&error, OPERATION);
        assert_eq!(cross_pair_probe_progress(), target.progress);

        reset_cross_pair_probe_progress();
        assert!(isolated_contact_result(&mut Unmetered)?);
        let mut ordinary_budget = EvaluationBudget::new(OPERATION, 1)?;
        let ordinary_spend = ordinary_budget.spent_ticks_for_test();
        {
            let mut work = EvaluationOnly::new(&mut ordinary_budget);
            assert!(isolated_contact_result(&mut work)?);
        }
        assert_eq!(ordinary_budget.spent_ticks_for_test(), ordinary_spend);
        Ok(())
    }

    #[test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "the recorder assertions pin edge-contact progress and refusal before the next probe"
    )]
    fn native_edge_contact_sweep_pays_no_survivor_probes() -> Result<()> {
        const OPERATION: &str = "native_edge_contact_sweep";
        let coordinates = [(0.0, 0.0), (1.0, 0.0), (10.0, 0.0), (11.0, 0.0)];
        let edges = [[0, 1], [2, 3]];
        reset_cross_pair_probe_progress();
        let (result, charges) = {
            let mut budget = EvaluationBudget::new(OPERATION, 1)?;
            let mut work = SweepRecordingWork::new(&mut budget);
            let result = edge_contact_result(&coordinates, &edges, &mut work)?;
            (result, work.charges)
        };
        assert!(result);
        assert_eq!(cross_pair_probe_progress(), CrossPairProbeProgress {
            attempted: 4,
            completed: 4,
        });
        let target = charges
            .into_iter()
            .find(|event| {
                event.progress
                    == CrossPairProbeProgress {
                        attempted: 2,
                        completed: 1,
                    }
            })
            .expect("the paid self-sweep charges a rejected edge pair");

        reset_cross_pair_probe_progress();
        let mut budget = budget_refusing_before(OPERATION, &target);
        let error = {
            let mut work = SweepRecordingWork::new(&mut budget);
            let error = edge_contact_result(&coordinates, &edges, &mut work)
                .expect_err("the no-survivor self-sweep refuses before its second probe");
            assert_eq!(
                work.refused.map(|event| event.progress),
                Some(target.progress)
            );
            error
        };
        assert_sweep_limit(&error, OPERATION);
        assert_eq!(cross_pair_probe_progress(), target.progress);

        reset_cross_pair_probe_progress();
        assert!(edge_contact_result(&coordinates, &edges, &mut Unmetered)?);
        let mut ordinary_budget = EvaluationBudget::new(OPERATION, 1)?;
        let ordinary_spend = ordinary_budget.spent_ticks_for_test();
        {
            let mut work = EvaluationOnly::new(&mut ordinary_budget);
            assert!(edge_contact_result(&coordinates, &edges, &mut work)?);
        }
        assert_eq!(ordinary_budget.spent_ticks_for_test(), ordinary_spend);
        Ok(())
    }

    #[test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "the recorder assertions pin exact shared-contact work before a later-pair refusal"
    )]
    fn native_edge_contact_sweep_preserves_exact_charges_before_refusal() -> Result<()> {
        const OPERATION: &str = "native_edge_contact_exact";
        let coordinates = [(0.0, 0.0), (1.0, 1.0), (2.0, 0.0)];
        let edges = [[0, 1], [1, 2]];
        reset_cross_pair_probe_progress();
        let (result, charges) = {
            let mut budget = EvaluationBudget::new(OPERATION, 1)?;
            let mut work = SweepRecordingWork::new(&mut budget);
            let result = edge_contact_result(&coordinates, &edges, &mut work)?;
            (result, work.charges)
        };
        assert!(result);
        assert!(
            charges.iter().any(|event| {
                event.progress
                    == CrossPairProbeProgress {
                        attempted: 2,
                        completed: 2,
                    }
            }),
            "the shared-endpoint survivor runs its exact contact predicate"
        );
        let target = charges
            .into_iter()
            .find(|event| {
                event.progress
                    == CrossPairProbeProgress {
                        attempted: 3,
                        completed: 2,
                    }
            })
            .expect("the paid sweep charges the later rejected edge pair");

        reset_cross_pair_probe_progress();
        let mut budget = budget_refusing_before(OPERATION, &target);
        let error = {
            let mut work = SweepRecordingWork::new(&mut budget);
            let error = edge_contact_result(&coordinates, &edges, &mut work)
                .expect_err("later sweep probe refuses after the exact contact");
            assert_eq!(
                work.refused.map(|event| event.progress),
                Some(target.progress)
            );
            error
        };
        assert_sweep_limit(&error, OPERATION);
        assert_eq!(cross_pair_probe_progress(), target.progress);
        Ok(())
    }

    #[test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "the recorder assertions pin rational bounds work and refuse before bounds publication"
    )]
    fn native_contact_bounds_refuse_before_returning_bounds() -> Result<()> {
        const OPERATION: &str = "native_contact_bounds";
        let point = near_one_exact_chart_point();
        super::reset_contact_check_progress();
        let (expected, charges) = {
            let mut budget = EvaluationBudget::new(OPERATION, 1)?;
            let mut work = SweepRecordingWork::new(&mut budget);
            let expected = super::outward_bounds(&point, &mut work)?;
            (expected, work.charges)
        };
        assert_eq!(
            super::contact_check_progress().bounds_attempted,
            super::contact_check_progress().bounds_completed
        );
        let target = charges
            .into_iter()
            .find(|event| {
                event.contact_progress.bounds_attempted == 1
                    && event.contact_progress.bounds_completed == 0
            })
            .expect("contact bounds forward rational rounding work");

        super::reset_contact_check_progress();
        let mut budget = budget_refusing_before(OPERATION, &target);
        let error = {
            let mut work = SweepRecordingWork::new(&mut budget);
            let error = super::outward_bounds(&point, &mut work)
                .expect_err("failed exact rounding cannot publish outward bounds");
            assert_eq!(
                work.refused.map(|event| event.contact_progress),
                Some(target.contact_progress)
            );
            error
        };
        assert_sweep_limit(&error, OPERATION);
        assert_eq!(super::contact_check_progress(), target.contact_progress);

        let expected = [
            expected.minx().to_bits(),
            expected.miny().to_bits(),
            expected.maxx().to_bits(),
            expected.maxy().to_bits(),
        ];
        let ordinary = point.outward_bounds()?;
        assert_eq!(ordinary.map(f64::to_bits), expected);
        let mut ordinary_budget = EvaluationBudget::new(OPERATION, 1)?;
        let ordinary_spend = ordinary_budget.spent_ticks_for_test();
        {
            let mut work = EvaluationOnly::new(&mut ordinary_budget);
            let bounds = super::outward_bounds(&point, &mut work)?;
            assert_eq!(
                [
                    bounds.minx().to_bits(),
                    bounds.miny().to_bits(),
                    bounds.maxx().to_bits(),
                    bounds.maxy().to_bits(),
                ],
                expected
            );
        }
        assert_eq!(ordinary_budget.spent_ticks_for_test(), ordinary_spend);
        Ok(())
    }

    #[test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "the recorder assertions pin endpoint identity work before returning the degenerate-edge result"
    )]
    fn native_contact_endpoint_identity_refuses_inside_exact_equality() -> Result<()> {
        const OPERATION: &str = "native_contact_identity";
        let point = near_one_exact_chart_point();
        let exact = exact_points(&[(point.x.clone(), point.y.clone()), (point.x, point.y)]);
        let edges = [[0, 1]];
        super::reset_contact_check_progress();
        let (result, charges) = {
            let mut budget = EvaluationBudget::new(OPERATION, 1)?;
            let mut work = SweepRecordingWork::new(&mut budget);
            let result = edges_have_only_vertex_contacts(&exact, &edges, &mut work)?;
            (result, work.charges)
        };
        assert!(!result);
        let target = charges
            .into_iter()
            .find(|event| {
                event.contact_progress.identity_attempted == 1
                    && event.contact_progress.identity_completed == 0
            })
            .expect("degenerate endpoint identity uses metered rational equality");

        super::reset_contact_check_progress();
        let mut budget = budget_refusing_before(OPERATION, &target);
        let error = {
            let mut work = SweepRecordingWork::new(&mut budget);
            let error = edges_have_only_vertex_contacts(&exact, &edges, &mut work)
                .expect_err("endpoint equality refuses before the degenerate-edge result");
            assert_eq!(
                work.refused.map(|event| event.contact_progress),
                Some(target.contact_progress)
            );
            error
        };
        assert_sweep_limit(&error, OPERATION);
        assert_eq!(super::contact_check_progress(), target.contact_progress);

        assert!(!edges_have_only_vertex_contacts(
            &exact,
            &edges,
            &mut Unmetered
        )?);
        let mut ordinary_budget = EvaluationBudget::new(OPERATION, 1)?;
        let ordinary_spend = ordinary_budget.spent_ticks_for_test();
        {
            let mut work = EvaluationOnly::new(&mut ordinary_budget);
            assert!(!edges_have_only_vertex_contacts(&exact, &edges, &mut work)?);
        }
        assert_eq!(ordinary_budget.spent_ticks_for_test(), ordinary_spend);
        Ok(())
    }

    #[test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "assertion is the exact degree-zero point-contact admission regression"
    )]
    fn candidate_admission_keeps_degree_zero_point_contacts_with_the_reference() -> Result<()> {
        let half_ulp =
            ExactRational::new(ExactDyadic::one(), ExactDyadic::from_i64(1_i64 << 54)).unwrap();
        let reference = exact_points(&[
            (ExactRational::zero(), ExactRational::one()),
            (ExactRational::from_f64(2.0), ExactRational::one()),
            (
                ExactRational::one(),
                ExactRational::one().try_add(&half_ulp).unwrap(),
            ),
        ]);
        let nearest = points(&[(0.0, 1.0), (2.0, 1.0), (1.0, 1.0)]);
        let mut work = Unmetered;
        let graph = admit_known_graph(reference, vec![[0, 1]], vec![0, 1, 2], &mut work)?;
        let layout = admit_logical_layout(&graph, Vec::new(), None, &mut work)?;

        assert!(
            admit_candidate(&graph, &layout, nearest, &mut work)?.is_none(),
            "the nearest image of degree-zero P contacts AB and must not be admitted"
        );
        Ok(())
    }

    #[test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "assertions pin query refusal before any later progress"
    )]
    fn output_point_query_refusal_stops_before_progress() -> Result<()> {
        let mut setup_work = Unmetered;
        let graph = admit_known_graph(
            points(&[(0.0, 1.0), (2.0, 1.0), (1.0, 1.0)]),
            vec![[0, 1]],
            vec![0, 1, 2],
            &mut setup_work,
        )?;
        let budget = EvaluationBudget::with_remaining_ticks_for_test("point contacts", 0, 1)?;
        let mut recorder = EvaluationRecorder {
            budget,
            successful_charges: 0,
            refusals: 0,
            charges_after_refusal: 0,
            refused: false,
        };

        let result = graph.with_output_point_contacts(&[0], &mut recorder);
        assert!(matches!(
            result,
            Err(error)
                if matches!(
                    error.kind(),
                    crate::error::ErrorKind::Geometry(
                        crate::geometry::GeometryErrorKind::ExactEmbeddingSearchLimit { .. }
                    )
                )
        ));
        assert_eq!(recorder.successful_charges, 1);
        assert_eq!(recorder.refusals, 1);
        assert_eq!(recorder.charges_after_refusal, 0);
        Ok(())
    }

    #[test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "assertion pins the contact decision under EvaluationOnly work erasure"
    )]
    fn evaluation_only_erases_point_contact_work() -> Result<()> {
        let half_ulp =
            ExactRational::new(ExactDyadic::one(), ExactDyadic::from_i64(1_i64 << 54)).unwrap();
        let reference = exact_points(&[
            (ExactRational::zero(), ExactRational::one()),
            (ExactRational::from_f64(2.0), ExactRational::one()),
            (
                ExactRational::one(),
                ExactRational::one().try_add(&half_ulp).unwrap(),
            ),
        ]);
        let mut budget =
            EvaluationBudget::with_remaining_ticks_for_test("ordinary point contacts", 0, 0)?;
        let mut ordinary_work = EvaluationOnly::new(&mut budget);
        let graph = admit_known_graph(reference, vec![[0, 1]], vec![0, 1, 2], &mut ordinary_work)?;
        let layout = admit_logical_layout(&graph, Vec::new(), None, &mut ordinary_work)?;

        assert!(
            admit_candidate(
                &graph,
                &layout,
                points(&[(0.0, 1.0), (2.0, 1.0), (1.0, 1.0)]),
                &mut ordinary_work,
            )?
            .is_none(),
            "EvaluationOnly preserves the same geometric decision with zero raw-work allowance"
        );
        Ok(())
    }
}
