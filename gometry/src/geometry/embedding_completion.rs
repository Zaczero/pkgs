//! Admission and completion checks for an already-known embedded graph.
//!
//! Producers own graph selection and stable source identities. This module
//! owns the shared proof that a finite coordinate assignment preserves the
//! admitted graph, its embedding, and the identities of distinct vertices.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::ControlFlow;

use crate::error::{ResourceError, Result};
use crate::geometry::arrangement::{
    IndexedTopology, indexed_edge_slots_with_work, indexed_topology_from_edges_with_work,
};
use crate::geometry::exact_arith::ExactSign;
use crate::geometry::overlay::{
    CertifiedOverlayOrder, GlobalFaces, RayAxis, RayPoints, compare_departures_with_work,
    graph_global_faces_with_work,
};
use crate::geometry::segments::try_sort_by_with_work;
use crate::geometry::tessellation::exact::ExactRational;
use crate::geometry::topology::{
    ExactChartPoint, exact_segments_intersect, point_on_exact_segment, vector_cross,
    vector_difference,
};
use crate::geometry::{
    Bounds, CoordinateAxes, GeometryErrorKind, Point, Shape, Work, XY,
    for_each_overlapping_bounds_cross_pair,
};

/// Exact points are chart images already admitted by their producer receipt.
/// An adapter must form a lifted longitude as exact native X plus the original
/// arbitrary-precision sheet times the exact period, then apply any bounded
/// relative neighbor offset. A finite-domain `i64` offset is never the original
/// sheet, and the global reference X is never rounded through `f64`.
pub(super) struct ExactPoints {
    pub(super) exact: Vec<ExactChartPoint>,
    pub(super) rounded: Vec<XY>,
}

impl ExactPoints {
    #[cfg(test)]
    pub(super) fn from_exact(exact: Vec<ExactChartPoint>, work: &mut impl Work) -> Result<Self> {
        work.charge(u64::try_from(exact.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
        let mut rounded = Vec::with_capacity(exact.len());
        for point in &exact {
            work.charge(1)?;
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
        self.rounded[index as usize]
    }

    fn compare_axis(&self, left: u32, right: u32, axis: RayAxis) -> Result<Ordering> {
        let left = self
            .exact
            .get(left as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("known graph point ID is unknown".into()))?;
        let right = self
            .exact
            .get(right as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("known graph point ID is unknown".into()))?;
        match axis {
            RayAxis::X => left.compare_x(right),
            RayAxis::Y => left.compare_y(right),
        }
    }

    fn orientation(&self, a: u32, b: u32, c: u32) -> Result<ExactSign> {
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
        vector_cross(&vector_difference(a, b)?, &vector_difference(a, c)?).map(|cross| cross.sign())
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
        work.charge(u64::try_from(edge_nodes.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
        let mut atom_by_endpoints = BTreeMap::new();
        for (atom, &[left, right]) in edge_nodes.iter().enumerate() {
            work.charge(1)?;
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

        work.charge(u64::try_from(walks.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
        let mut identities = BTreeSet::new();
        let mut areal_walks = Vec::with_capacity(walks.len());
        for walk in walks {
            work.charge(1)?;
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
            work.charge(u64::try_from(edge_count).map_err(|_| ResourceError::SizeOverflow)?)?;
            let mut uses = Vec::with_capacity(edge_count);
            for &[start, end] in walk.vertices.array_windows::<2>() {
                work.charge(1)?;
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
            work.charge(1)?;
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
}

pub(super) struct AdmittedCandidate<'a> {
    pub(super) points: &'a ExactPoints,
}

pub(super) fn exact_chord_fraction(
    start: &ExactChartPoint,
    end: &ExactChartPoint,
    point: &ExactChartPoint,
    work: &mut impl Work,
) -> Result<Option<ExactRational>> {
    work.charge(4)?;
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
        order: CertifiedOverlayOrder,
        work: &mut impl Work,
    ) -> Result<Self> {
        Ok(Self {
            points: reorder_rows(self.points, &order.points, work)?,
            lines: reorder_rows(self.lines, &order.lines, work)?,
            polygons: reorder_rows(self.polygons, &order.polygons, work)?,
        })
    }
}

fn reorder_rows<T>(rows: Vec<T>, order: &[usize], work: &mut impl Work) -> Result<Vec<T>> {
    if rows.len() != order.len() {
        return Err(
            GeometryErrorKind::Invalid("literal output order omits a physical row".into()).into(),
        );
    }
    work.charge(u64::try_from(rows.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut rows = rows.into_iter().map(Some).collect::<Vec<_>>();
    work.charge(u64::try_from(order.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut output = Vec::with_capacity(order.len());
    for &index in order {
        work.charge(1)?;
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

/// Private producer receipt issued only after that producer's shared checks
/// have accepted the current assignment.
pub(super) struct CheckedAssignment<R>(R);

impl<R> CheckedAssignment<R> {
    pub(super) const fn new(receipt: R) -> Self {
        Self(receipt)
    }
}

impl<I, R: CheckedImages<I>> CheckedImages<I> for CheckedAssignment<R> {
    fn image(&self, image: I) -> Result<&ExactChartPoint> {
        self.0.image(image)
    }
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

pub(super) fn bind_literals<I, C, R, W>(
    buffers: FrozenLiteralBuffers<I>,
    chart: &mut C,
    images: &R,
    work: &mut W,
) -> Result<LiteralCheck<I>>
where
    I: Copy + Eq + Ord,
    C: LiteralChart<I>,
    R: CheckedImages<I>,
    W: Work,
{
    let mut rejected = Vec::new();
    let mut check =
        |output, image, record, expected_axes, stored_axes, work: &mut W| -> Result<()> {
            work.charge(1)?;
            if expected_axes != stored_axes || CoordinateAxes::from_point(record) != stored_axes {
                rejected.push(image);
                return Ok(());
            }
            let Some(actual) =
                chart.read_point(output, image, record, expected_axes, stored_axes, work)?
            else {
                rejected.push(image);
                return Ok(());
            };
            if actual != *images.image(image)? {
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
    work.charge(u64::try_from(count).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut ids = values.collect::<Vec<_>>();
    if ids.len() != count {
        return Err(GeometryErrorKind::Invalid(
            "literal logical output IDs do not cover their rows".into(),
        )
        .into());
    }
    try_sort_by_with_work(&mut ids, work, |left, right, work| {
        work.charge(1)?;
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
        work.charge(1)?;
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
            work.charge(1)?;
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
                work.charge(1)?;
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
                    work.charge(1)?;
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
    let (topology, edge_of_slot) = indexed_topology(&points, &edge_nodes, work)?;
    let edge_slots = indexed_edge_slots_with_work(&edge_nodes, &topology, &edge_of_slot, work)?;
    Ok(AdmittedGraph {
        points,
        edge_nodes,
        edge_slots,
        edge_of_slot,
        topology,
        point_identities,
    })
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
    reference: &AdmittedGraph,
    dimension: &AdmittedLayout,
    points: &'a ExactPoints,
    isolated_vertices: &[u32],
    work: &mut impl Work,
) -> Result<Option<AdmittedCandidate<'a>>> {
    if points.exact.len() != reference.points.exact.len()
        || points.rounded.len() != points.exact.len()
    {
        return Ok(None);
    }
    if !point_hints_match(points, work)? {
        return Ok(None);
    }
    if !identities_are_injective(points, &reference.point_identities, work)? {
        return Ok(None);
    }
    if !point_edge_contacts_preserved(
        &reference.points,
        points,
        isolated_vertices,
        &reference.edge_nodes,
        work,
    )? {
        return Ok(None);
    }
    if !edges_have_only_vertex_contacts(points, &reference.edge_nodes, work)? {
        return Ok(None);
    }
    let (topology, edge_of_slot) = match indexed_topology(points, &reference.edge_nodes, work) {
        Ok(topology) => topology,
        Err(error) if is_tied_departure_error(&error) => return Ok(None),
        Err(error) => return Err(error),
    };
    if let Some(areal_reference) = &dimension.areal {
        let areal = build_areal_signature(
            reference,
            &dimension.layout,
            &areal_reference.wall_atom_ids,
            points,
            &topology,
            &edge_of_slot,
            None,
            work,
        )?;
        if !stable_areal_signatures_match(&areal_reference.signature, &areal) {
            return Ok(None);
        }
        return Ok(Some(AdmittedCandidate { points }));
    }
    Ok(Some(AdmittedCandidate { points }))
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
        work.charge(1)?;
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
    work.charge(u64::try_from(points.exact.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
    for (exact, &rounded) in points.exact.iter().zip(&points.rounded) {
        work.charge(1)?;
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
    work.charge(u64::try_from(points.exact.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
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
        work.charge(1)?;
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
        work.charge(1)?;
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
            work.charge(1)?;
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
    work.charge(u64::try_from(vertices.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut rotations = Vec::with_capacity(vertices.len());
    for &vertex in vertices {
        work.charge(1)?;
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
        work.charge(u64::try_from(degree).map_err(|_| ResourceError::SizeOverflow)?)?;
        let mut rotation = Vec::with_capacity(degree);
        for slot in start..end {
            work.charge(1)?;
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
    if topology.face_starts.is_empty() || faces.walk_faces.len() + 1 != topology.face_starts.len() {
        return Err(GeometryErrorKind::Invalid(
            "stable areal wall correspondence has inconsistent face tables".into(),
        )
        .into());
    }
    let walk_count = topology.face_starts.len() - 1;
    let face_count = usize::try_from(faces.count).map_err(|_| ResourceError::SizeOverflow)?;
    work.charge(u64::try_from(face_count).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut face_keys = vec![Vec::<u32>::new(); face_count];
    for walk in 0..walk_count {
        let start =
            *topology.face_starts.get(walk).ok_or_else(|| {
                GeometryErrorKind::Invalid("stable face walk start is missing".into())
            })? as usize;
        let end =
            *topology.face_starts.get(walk + 1).ok_or_else(|| {
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
        work.charge(u64::try_from(slots.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
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
            work.charge(1)?;
            Ok(left.cmp(right))
        })?;
    }
    work.charge(u64::try_from(face_count).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut face_order = (0..face_count).collect::<Vec<_>>();
    try_sort_by_with_work(&mut face_order, work, |left, right, work| {
        work.charge(1)?;
        let left = face_keys
            .get(*left)
            .ok_or_else(|| GeometryErrorKind::Invalid("stable left face key is unknown".into()))?;
        let right = face_keys
            .get(*right)
            .ok_or_else(|| GeometryErrorKind::Invalid("stable right face key is unknown".into()))?;
        Ok(left.cmp(right))
    })?;
    for pair in face_order.array_windows::<2>() {
        work.charge(1)?;
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
    work.charge(u64::try_from(face_count).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut stable_face_id = vec![0_u32; face_count];
    let mut stable_keys = Vec::with_capacity(face_count);
    for (stable, face) in face_order.into_iter().enumerate() {
        work.charge(1)?;
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
            work.charge(1)?;
            wall_atom_set.insert(use_row.atom);
        }
    }
    work.charge(u64::try_from(wall_atom_set.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
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
    let (projected_edges, projected_topology, projected_edge_of_slot) = if !all_atoms_are_walls {
        work.charge(u64::try_from(wall_atom_ids.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
        let mut projected_edges = Vec::with_capacity(wall_atom_ids.len());
        for &atom in wall_atom_ids {
            work.charge(1)?;
            projected_edges.push(*graph.edge_nodes.get(atom as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("areal logical wall names an unknown atom".into())
            })?);
        }
        let (topology, edge_of_slot) = indexed_topology(points, &projected_edges, work)?;
        (projected_edges, Some(topology), edge_of_slot)
    } else {
        (Vec::new(), None, Vec::new())
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
        work.charge(1)?;
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
    work.charge(u64::try_from(points.exact.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut host_by_vertex = Vec::with_capacity(points.exact.len());
    for &component in &wall_topology.component_of {
        work.charge(1)?;
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

fn outward_bounds(point: &ExactChartPoint) -> Result<Bounds> {
    let [min_x, min_y, max_x, max_y] = point.outward_bounds()?;
    Ok(Bounds::new_raw(min_x, min_y, max_x, max_y))
}

fn outward_segment_bounds(start: &ExactChartPoint, end: &ExactChartPoint) -> Result<Bounds> {
    let [start_min_x, start_min_y, start_max_x, start_max_y] = start.outward_bounds()?;
    let [end_min_x, end_min_y, end_max_x, end_max_y] = end.outward_bounds()?;
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
        work.charge(
            u64::try_from(isolated_vertices.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut point_bounds = Vec::with_capacity(isolated_vertices.len());
        for &vertex in isolated_vertices {
            work.charge(1)?;
            let point = points.exact.get(vertex as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("isolated graph point is unknown".into())
            })?;
            point_bounds.push(outward_bounds(point)?);
        }
        work.charge(u64::try_from(edge_nodes.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
        let mut edge_bounds = Vec::with_capacity(edge_nodes.len());
        for &[start, end] in edge_nodes {
            work.charge(1)?;
            edge_bounds.push(outward_segment_bounds(
                points.exact.get(start as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("point-contact edge start is unknown".into())
                })?,
                points.exact.get(end as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("point-contact edge end is unknown".into())
                })?,
            )?);
        }
        let mut result = BTreeSet::new();
        let completion = for_each_overlapping_bounds_cross_pair(
            &point_bounds,
            &edge_bounds,
            |point_index, edge_index| {
                if let Err(error) = work.charge(1) {
                    return ControlFlow::Break(Err(error));
                }
                let Some(vertex) = isolated_vertices.get(point_index).copied() else {
                    return ControlFlow::Break(Err(GeometryErrorKind::Invalid(
                        "point-contact sweep returned an unknown point".into(),
                    )
                    .into()));
                };
                let [start, end] = match edge_nodes.get(edge_index) {
                    Some(edge) => *edge,
                    None => {
                        return ControlFlow::Break(Err(GeometryErrorKind::Invalid(
                            "point-contact sweep returned an unknown edge".into(),
                        )
                        .into()));
                    },
                };
                let (Some(point), Some(start), Some(end)) = (
                    points.exact.get(vertex as usize),
                    points.exact.get(start as usize),
                    points.exact.get(end as usize),
                ) else {
                    return ControlFlow::Break(Err(GeometryErrorKind::Invalid(
                        "point-contact graph row is outside its point inventory".into(),
                    )
                    .into()));
                };
                match point_on_exact_segment(point, start, end) {
                    Ok(false) => ControlFlow::Continue(()),
                    Ok(true) => {
                        if let Err(error) = work.charge(1) {
                            return ControlFlow::Break(Err(error));
                        }
                        result.insert((vertex, edge_index));
                        ControlFlow::Continue(())
                    },
                    Err(error) => ControlFlow::Break(Err(error)),
                }
            },
        )?;
        match completion {
            ControlFlow::Break(Err(error)) => return Err(error),
            ControlFlow::Break(Ok(())) | ControlFlow::Continue(()) => {},
        }
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
    work.charge(u64::try_from(edge_nodes.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut bounds = Vec::with_capacity(edge_nodes.len());
    for &[start, end] in edge_nodes {
        work.charge(1)?;
        let start = points
            .exact
            .get(start as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("candidate edge start is unknown".into()))?;
        let end = points
            .exact
            .get(end as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("candidate edge end is unknown".into()))?;
        if start.same_position(end)? {
            return Ok(false);
        }
        bounds.push(outward_segment_bounds(start, end)?);
    }
    let mut new_contact = false;
    let completion =
        for_each_overlapping_bounds_cross_pair(&bounds, &bounds, |left_index, right_index| {
            if left_index >= right_index {
                return ControlFlow::Continue(());
            }
            if let Err(error) = work.charge(1) {
                return ControlFlow::Break(Err(error));
            }
            let (Some([left_start, left_end]), Some([right_start, right_end])) =
                (edge_nodes.get(left_index), edge_nodes.get(right_index))
            else {
                return ControlFlow::Break(Err(GeometryErrorKind::Invalid(
                    "candidate edge sweep returned an unknown edge".into(),
                )
                .into()));
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
                return ControlFlow::Break(Err(GeometryErrorKind::Invalid(
                    "candidate edge endpoint is outside its point inventory".into(),
                )
                .into()));
            };
            match exact_segments_intersect(
                left_start_point,
                left_end_point,
                right_start_point,
                right_end_point,
            ) {
                Err(error) => ControlFlow::Break(Err(error)),
                Ok(false) => ControlFlow::Continue(()),
                Ok(true) => {
                    let shared = [*left_start, *left_end]
                        .into_iter()
                        .find(|vertex| [*right_start, *right_end].contains(vertex));
                    let Some(shared) = shared else {
                        new_contact = true;
                        return ControlFlow::Break(Ok(()));
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
                        return ControlFlow::Break(Err(GeometryErrorKind::Invalid(
                            "candidate edge has no nonshared endpoint".into(),
                        )
                        .into()));
                    };
                    match (
                        point_on_exact_segment(
                            left_other_point,
                            right_start_point,
                            right_end_point,
                        ),
                        point_on_exact_segment(right_other_point, left_start_point, left_end_point),
                    ) {
                        (Ok(false), Ok(false)) => ControlFlow::Continue(()),
                        (Ok(_), Ok(_)) => {
                            new_contact = true;
                            ControlFlow::Break(Ok(()))
                        },
                        (Err(error), _) | (_, Err(error)) => ControlFlow::Break(Err(error)),
                    }
                },
            }
        })?;
    match completion {
        ControlFlow::Break(Err(error)) => Err(error),
        ControlFlow::Break(Ok(())) | ControlFlow::Continue(()) => Ok(!new_contact),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AdmittedGraph, AdmittedLayout, CheckedImages, ExactPoints, FrozenLiteralBuffers,
        LiteralChart, LiteralCheck, LiteralPoint, LiteralPolygon, LiteralSequence,
        LogicalArealWalk, ShapeLiteralLayout, admit_candidate, admit_known_graph,
        admit_logical_layout, bind_literals,
    };
    use crate::error::Result;
    use crate::geometry::tessellation::exact::{ExactDyadic, ExactRational};
    use crate::geometry::topology::ExactChartPoint;
    use crate::geometry::{
        CoordSeq, CoordinateAxes, LineSeq, Point, Polygon, Ring, Shape, Unmetered, XY,
    };

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
            admit_candidate(&graph, &layout, &graph.points, &[], &mut work)
                .unwrap()
                .is_some(),
            "the unchanged noded reference must admit both complete ring walks"
        );

        let affine = points(&coordinates.map(|(x, y)| (x + y, y)));
        assert!(
            admit_candidate(&graph, &layout, &affine, &[], &mut work)
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
        let images = ImageRows(graph.points.exact.clone());
        assert!(matches!(
            bind_literals(
                FrozenLiteralBuffers::shapes(sink, literal_layout),
                &mut IdentityLiteralChart,
                &images,
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
            admit_candidate(&graph, &layout, &rotated, &[], &mut work)
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
            admit_candidate(&graph, &layout, &opposition_changed, &[], &mut work)
                .unwrap()
                .is_some()
        );

        // Swapping the two stable line darts changes the actual rotation.
        let reordered = points(&[(0.0, 0.0), (2.0, 0.0), (0.0, 2.0), (0.0, -1.0), (-1.0, 0.0)]);
        assert!(
            admit_candidate(&graph, &layout, &reordered, &[], &mut work)
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
            admit_candidate(&graph, &layout, &nearest, &[], &mut work)
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
        let Some(assignment) = admit_candidate(&graph, &layout, &repaired, &[], &mut work).unwrap()
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
        let images = ImageRows(assignment.points.exact.clone());
        assert!(matches!(
            bind_literals(
                FrozenLiteralBuffers::shapes(shape, layout),
                &mut IdentityLiteralChart,
                &images,
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
            admit_candidate(&graph, &layout, &nearest, &[4], &mut work)
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
            admit_candidate(&graph, &layout, &upper, &[4], &mut work)
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
            admit_candidate(&graph, &layout, &nearest, &[], &mut work)
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
            admit_candidate(&graph, &layout, &repaired, &[], &mut work)
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
        let images = ImageRows(repaired.exact.clone());
        assert!(matches!(
            bind_literals(
                FrozenLiteralBuffers::shapes(sink, literal_layout),
                &mut IdentityLiteralChart,
                &images,
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
            admit_candidate(&graph, &layout, &nearest, &[7], &mut work)
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
            admit_candidate(&graph, &layout, &repaired, &[7], &mut work)
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
        let images = ImageRows(repaired.exact.clone());
        assert!(matches!(
            bind_literals(
                FrozenLiteralBuffers::shapes(sink, literal_layout),
                &mut IdentityLiteralChart,
                &images,
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
            admit_candidate(&graph, &layout, &nearest, &[4], &mut work)
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
            admit_candidate(&graph, &layout, &lower, &[4], &mut work)
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
            admit_candidate(&graph, &layout, &upper, &[4], &mut work)
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
            admit_candidate(&graph, &layout, &nearest, &[], &mut work)
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
            admit_candidate(&graph, &layout, &lower, &[], &mut work)
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
            admit_candidate(&graph, &layout, &nearest, &[], &mut work)
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
            admit_candidate(&graph, &layout, &lower, &[], &mut work)
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
            admit_candidate(&line_graph, &line_layout, &reversed, &[], &mut work)
                .unwrap()
                .is_some()
        );

        let polygon_graph = admit_known_graph(reference, edges, identities, &mut work).unwrap();
        let layout = polygon_layout(&polygon_graph, &[0, 1, 2, 0]);
        assert!(
            admit_candidate(&polygon_graph, &layout, &reversed, &[], &mut work)
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
            &reversed,
            &[],
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
        let images = ImageRows(vec![
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
        ]);
        let sealed = match bind_literals(
            FrozenLiteralBuffers::shapes(line, literal_layout),
            &mut IdentityLiteralChart,
            &images,
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
            admit_candidate(&polygon_graph, &layout, &reversed, &[], &mut work)
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
                &collapsed,
                &[],
                &mut work
            )
            .unwrap()
            .is_none()
        );
    }
}
