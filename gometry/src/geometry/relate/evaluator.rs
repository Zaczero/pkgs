use std::cmp::Ordering;
use std::collections::HashSet;
use std::ops::ControlFlow;

use crate::geometry::storage::{PackedRow, ShapeRow};
use crate::collections::collection_hash_state;
use crate::error::{ResourceError, Result};
use crate::geometry::overlay::{
    ExactPointForest, StagedExactSources, StagedRelationItem, StagedSourceComponent,
    homogeneous_midpoint,
};
use crate::geometry::relate::matrix_predicates::{self, CellMask};
use crate::geometry::relate::{De9im, De9imInterval, Loc, OperandShape};
use crate::geometry::segments::{TouchLocation, crossing_homogeneous, crossing_point};
use crate::geometry::source_support::{
    OccupiedToward, SourcePointLocation, cartesian_row_exact_offset_location_and_area,
    cartesian_row_exact_point_location_and_area, cartesian_row_point_location_and_area,
    occupied_toward,
};
use crate::geometry::tessellation::exact::{ExactDyadic, compare_ratios};
use crate::geometry::{
    CompiledPattern, Contact, Coordinates as _, Dimension, GeometryErrorKind, HashMap, Point,
    PointKey, PreparedPlan, RingKind, Segment, Shape, XY, dimension, for_each_candidate_pair,
    ring_winding, same_point, segment_contact_exact,
};
use crate::predicates::Predicate;

const LOCATIONS: [Loc; 3] = [Loc::Interior, Loc::Boundary, Loc::Exterior];

trait Query {
    type Output: Copy;

    fn verdict(&self, interval: &De9imInterval) -> Option<Self::Output>;
    fn finish(&self, interval: &De9imInterval) -> Result<Self::Output>;
    fn cells(&self) -> CellMask;
    fn needs_dimensions(&self) -> bool;
}

struct MatrixQuery;

impl Query for MatrixQuery {
    type Output = De9im;

    fn verdict(&self, interval: &De9imInterval) -> Option<Self::Output> {
        interval.matrix()
    }

    fn finish(&self, interval: &De9imInterval) -> Result<Self::Output> {
        interval.matrix().ok_or_else(|| {
            GeometryErrorKind::Invalid("Cartesian relation left undecided DE-9IM cells".into())
                .into()
        })
    }

    fn cells(&self) -> CellMask {
        CellMask::all()
    }

    fn needs_dimensions(&self) -> bool {
        false
    }
}

struct PredicateQuery(Predicate);

impl Query for PredicateQuery {
    type Output = bool;

    fn verdict(&self, interval: &De9imInterval) -> Option<Self::Output> {
        matrix_predicates::eval(self.0, interval).option()
    }

    fn finish(&self, interval: &De9imInterval) -> Result<Self::Output> {
        self.verdict(interval).ok_or_else(|| {
            GeometryErrorKind::Invalid("Cartesian predicate left undecided DE-9IM cells".into())
                .into()
        })
    }

    fn cells(&self) -> CellMask {
        CellMask::for_predicate(self.0)
    }

    fn needs_dimensions(&self) -> bool {
        CellMask::needs_dimensions(self.0)
    }
}

struct PatternQuery(CompiledPattern);

impl Query for PatternQuery {
    type Output = bool;

    fn verdict(&self, interval: &De9imInterval) -> Option<Self::Output> {
        interval.pattern(self.0)
    }

    fn finish(&self, interval: &De9imInterval) -> Result<Self::Output> {
        self.verdict(interval).ok_or_else(|| {
            GeometryErrorKind::Invalid("Cartesian pattern left undecided DE-9IM cells".into())
                .into()
        })
    }

    fn cells(&self) -> CellMask {
        CellMask::for_pattern(self.0)
    }

    fn needs_dimensions(&self) -> bool {
        self.0
            .cells()
            .into_iter()
            .any(|literal| matches!(literal, b'0' | b'1' | b'2'))
    }
}

struct EndOnly<Q>(Q);

impl<Q: Query> Query for EndOnly<Q> {
    type Output = Q::Output;

    fn verdict(&self, _: &De9imInterval) -> Option<Self::Output> {
        None
    }

    fn finish(&self, interval: &De9imInterval) -> Result<Self::Output> {
        self.0.finish(interval)
    }

    fn cells(&self) -> CellMask {
        self.0.cells()
    }

    fn needs_dimensions(&self) -> bool {
        self.0.needs_dimensions()
    }
}

#[derive(Clone, Copy)]
struct Operand<'a> {
    row: ShapeRow<'a>,
    plan: Option<&'a PreparedPlan>,
}

impl<'a> Operand<'a> {
    fn from_row(row: ShapeRow<'a>) -> Self {
        let plan = match row {
            ShapeRow::Handle(plan) => Some(plan.as_ref()),
            ShapeRow::Shape(_) | ShapeRow::Point(_) | ShapeRow::Packed(_) => None,
        };
        Self { row, plan }
    }

    fn from_plan(plan: &'a PreparedPlan) -> Self {
        Self {
            row: ShapeRow::Shape(plan.shape()),
            plan: Some(plan),
        }
    }

    fn bounds(self) -> Option<crate::geometry::Bounds> {
        self.plan
            .and_then(|plan| plan.shape_data().bounds())
            .or_else(|| self.row.quick_bounds())
    }

    fn locate_stored(self, point: Point) -> Result<SourcePointLocation> {
        match self.plan {
            Some(plan) => plan.locate_stored(point),
            None => Ok(cartesian_row_point_location_and_area(self.row, point)),
        }
    }

    fn locate_exact(self, point: &[ExactDyadic; 3]) -> Result<SourcePointLocation> {
        match self.plan {
            Some(plan) => plan.locate_exact(point),
            None => cartesian_row_exact_point_location_and_area(self.row, point),
        }
    }

    fn locate_exact_offset(
        self,
        point: &[ExactDyadic; 3],
        direction: &[ExactDyadic; 2],
    ) -> Result<SourcePointLocation> {
        match self.plan {
            Some(plan) => plan.locate_exact_offset(point, direction),
            None => cartesian_row_exact_offset_location_and_area(self.row, point, direction),
        }
    }

    fn occupied_toward_exact(
        self,
        point: &[ExactDyadic; 3],
        direction: &[ExactDyadic; 2],
    ) -> Result<OccupiedToward> {
        self.locate_exact_offset(point, direction)
            .map(occupied_toward)
    }
}

#[derive(Clone)]
struct Component {
    source: StagedSourceComponent,
    dirty: bool,
    seed: Option<SourcePointLocation>,
    left_area: Option<bool>,
}

struct StagedGeometry {
    sources: StagedExactSources,
    components: Vec<Component>,
    segment_component: Vec<u32>,
    operand_has_area: [bool; 2],
    operand_has_ring: [bool; 2],
}

#[derive(Clone, Copy, Default)]
struct SourceCertificates {
    nonzero_line: bool,
    point: bool,
    hole_free_area: bool,
}

#[derive(Clone, Copy)]
enum EventPointKind {
    Stored(XY),
    Crossing { segments: [u32; 2], rounded: XY },
}

struct EventPoint {
    kind: EventPointKind,
    exact: Option<Box<[ExactDyadic; 3]>>,
    handle: Option<u32>,
}

#[derive(Clone, Copy)]
enum EventKind {
    Touch,
    Cross,
    Collinear { same_direction: bool },
}

struct ContactEvent {
    segments: [usize; 2],
    components: [u32; 2],
    kind: EventKind,
    points: [Option<EventPoint>; 2],
}

#[derive(Clone, Copy)]
struct Coverage {
    partner_component: u32,
    same_direction: bool,
}

#[derive(Clone, Copy)]
enum AtomPoint {
    Endpoint(XY),
    Event(u32),
}

pub(crate) fn relate(left: &PreparedPlan, right: &PreparedPlan) -> Result<De9im> {
    evaluate(
        [Operand::from_plan(left), Operand::from_plan(right)],
        MatrixQuery,
    )
}

pub(crate) fn predicate(
    left: &PreparedPlan,
    right: &PreparedPlan,
    predicate: Predicate,
) -> Result<bool> {
    evaluate(
        [Operand::from_plan(left), Operand::from_plan(right)],
        PredicateQuery(predicate),
    )
}

pub(crate) fn pattern(
    left: &PreparedPlan,
    right: &PreparedPlan,
    pattern: CompiledPattern,
) -> Result<bool> {
    evaluate(
        [Operand::from_plan(left), Operand::from_plan(right)],
        PatternQuery(pattern),
    )
}

fn evaluate<Q: Query>(operands: [Operand<'_>; 2], query: Q) -> Result<Q::Output> {
    evaluate_mode::<Q, true>(operands, query)
}

fn evaluate_mode<Q: Query, const SATURATE: bool>(
    operands: [Operand<'_>; 2],
    query: Q,
) -> Result<Q::Output> {
    let dimensions = [
        row_dimension(operands[0].row),
        row_dimension(operands[1].row),
    ];
    let bounds = [operands[0].bounds(), operands[1].bounds()];
    let shapes = [
        operand_shape(operands[0].row, dimensions[0]),
        operand_shape(operands[1].row, dimensions[1]),
    ];
    let empty = [dimensions[0].is_none(), dimensions[1].is_none()];
    let mut interval = De9imInterval::open(shapes[0], shapes[1]);
    settle_empty_operands(&mut interval, empty);
    if let [Some(left), Some(right)] = bounds
        && !left.intersects(right)
    {
        settle_disjoint_interiors(&mut interval);
    }
    if let Some(answer) = query.verdict(&interval) {
        return Ok(answer);
    }

    let certificates = [
        row_certificates(operands[0].row)?,
        row_certificates(operands[1].row)?,
    ];
    if query.needs_dimensions() {
        apply_dimension_certificates(&mut interval, dimensions, certificates);
    }
    if let Some(answer) = query.verdict(&interval) {
        return Ok(answer);
    }

    if let Some(answer) =
        point_only_phase(&mut interval, operands, dimensions, certificates, &query)?
    {
        return Ok(answer);
    }
    if let Some(answer) = query.verdict(&interval) {
        return Ok(answer);
    }

    let staged_sources =
        StagedExactSources::from_rows_for_relate([operands[0].row, operands[1].row].into_iter())?;
    let mut staged = stage_components(staged_sources, [operands[0].row, operands[1].row])?;
    apply_staged_dimension_floors(&mut interval, dimensions, &staged, query.needs_dimensions());
    if let Some(answer) = query.verdict(&interval) {
        return Ok(answer);
    }

    if let Some(answer) =
        representative_probes::<Q, SATURATE>(&mut interval, operands, &mut staged, &query)?
    {
        return Ok(answer);
    }
    if let Some(answer) = query.verdict(&interval) {
        return Ok(answer);
    }

    let mut events = Vec::new();
    let mut stored_points = HashMap::with_hasher(collection_hash_state()?.clone());
    let mut forest = ExactPointForest::with_capacity(staged.sources.segments(), 0);
    let sources = &staged.sources;
    let components = &mut staged.components;
    let segment_component = &staged.segment_component;
    let operand_has_ring = staged.operand_has_ring;
    let operand_has_area = staged.operand_has_area;
    if let Some(answer) = contact_sweep::<Q, SATURATE>(
        &mut interval,
        operands,
        &query,
        sources,
        components,
        segment_component,
        operand_has_ring,
        &mut events,
    )? {
        return Ok(answer);
    }
    initialize_clean_ring_sides(operands, sources, components)?;
    if let Some(answer) = exhaustive_pass::<Q, SATURATE>(
        &mut interval,
        operands,
        &query,
        sources,
        components,
        segment_component,
        operand_has_area,
        &mut events,
        &mut forest,
        &mut stored_points,
    )? {
        return Ok(answer);
    }
    query.finish(&interval)
}

fn operand_shape(row: ShapeRow<'_>, dimension: Option<Dimension>) -> OperandShape {
    let interior = dimension.unwrap_or(Dimension::Point);
    let boundary = if row.has_area_parts() {
        Some(Dimension::Curve)
    } else if interior == Dimension::Curve {
        Some(Dimension::Point)
    } else {
        None
    };
    OperandShape { interior, boundary }
}

fn row_dimension(row: ShapeRow<'_>) -> Option<Dimension> {
    match row {
        ShapeRow::Handle(plan) => dimension(plan.shape()),
        ShapeRow::Shape(shape) => dimension(shape),
        ShapeRow::Point(_) => Some(Dimension::Point),
        ShapeRow::Packed(PackedRow::Points { .. }) => {
            (!row.is_empty()).then_some(Dimension::Point)
        },
        ShapeRow::Packed(PackedRow::Lines { .. }) => {
            let coordinates = row.line_coords()?;
            let mut nonzero = false;
            for [start, end] in coordinates.segment_pairs() {
                nonzero |= !same_point(start.xy(), end.xy());
            }
            if nonzero {
                Some(Dimension::Curve)
            } else if coordinates.coord_count() != 0 {
                Some(Dimension::Point)
            } else {
                None
            }
        },
        ShapeRow::Packed(PackedRow::Polygons { .. }) => {
            (!row.is_empty()).then_some(Dimension::Surface)
        },
    }
}

fn settle_empty_operands(interval: &mut De9imInterval, empty: [bool; 2]) {
    for row in [Loc::Interior, Loc::Boundary] {
        if empty[0] {
            for col in LOCATIONS {
                interval.settle(row, col);
            }
        }
    }
    for col in [Loc::Interior, Loc::Boundary] {
        if empty[1] {
            for row in LOCATIONS {
                interval.settle(row, col);
            }
        }
    }
}

fn settle_disjoint_interiors(interval: &mut De9imInterval) {
    for row in [Loc::Interior, Loc::Boundary] {
        for col in [Loc::Interior, Loc::Boundary] {
            interval.settle(row, col);
        }
    }
}

fn settle_all(interval: &mut De9imInterval) {
    for row in LOCATIONS {
        for col in LOCATIONS {
            interval.settle(row, col);
        }
    }
}

fn point_keys(row: ShapeRow<'_>) -> Vec<PointKey> {
    let mut points = Vec::new();
    row.for_each_point_only(|point| points.push(PointKey::new(point.xy())));
    points.sort_unstable();
    points.dedup();
    points
}

fn point_from_key(point: PointKey) -> Point {
    Point::new_raw_xy(f64::from_bits(point.x), f64::from_bits(point.y))
}

fn point_only(row: ShapeRow<'_>, dimension: Option<Dimension>) -> bool {
    !row.has_area_parts() && dimension == Some(Dimension::Point)
}

fn point_only_phase<Q: Query>(
    interval: &mut De9imInterval,
    operands: [Operand<'_>; 2],
    dimensions: [Option<Dimension>; 2],
    certificates: [SourceCertificates; 2],
    query: &Q,
) -> Result<Option<Q::Output>> {
    let left_point_only = point_only(operands[0].row, dimensions[0]);
    let right_point_only = point_only(operands[1].row, dimensions[1]);
    if left_point_only && right_point_only {
        let left = point_keys(operands[0].row);
        let right = point_keys(operands[1].row);
        let mut left = left.into_iter().peekable();
        let mut right = right.into_iter().peekable();
        loop {
            match (left.peek().copied(), right.peek().copied()) {
                (Some(a), Some(b)) => match a.cmp(&b) {
                    Ordering::Less => {
                        interval.witness(Loc::Interior, Loc::Exterior, Dimension::Point);
                        left.next();
                    },
                    Ordering::Greater => {
                        interval.witness(Loc::Exterior, Loc::Interior, Dimension::Point);
                        right.next();
                    },
                    Ordering::Equal => {
                        interval.witness(Loc::Interior, Loc::Interior, Dimension::Point);
                        left.next();
                        right.next();
                    },
                },
                (Some(_), None) => {
                    interval.witness(Loc::Interior, Loc::Exterior, Dimension::Point);
                    left.next();
                },
                (None, Some(_)) => {
                    interval.witness(Loc::Exterior, Loc::Interior, Dimension::Point);
                    right.next();
                },
                (None, None) => break,
            }
            if let Some(answer) = query.verdict(interval) {
                return Ok(Some(answer));
            }
        }
        settle_all(interval);
        return Ok(query.verdict(interval));
    }

    let point_operand = if left_point_only {
        Some(0)
    } else if right_point_only {
        Some(1)
    } else {
        None
    };
    let Some(point_operand) = point_operand else {
        return Ok(None);
    };
    let target = point_operand ^ 1;
    let points = point_keys(operands[point_operand].row);
    let mut target_area = certificates[target].hole_free_area;
    for key in points {
        let point = point_from_key(key);
        let target_location = operands[target].locate_stored(point)?;
        let (left_location, right_location) = if point_operand == 0 {
            (
                SourcePointLocation {
                    location: Loc::Interior,
                    area: false,
                },
                target_location,
            )
        } else {
            (target_location, SourcePointLocation {
                location: Loc::Interior,
                area: false,
            })
        };
        witness_point(
            interval,
            left_location,
            right_location,
            query.needs_dimensions(),
        );
        target_area |= target_location.area;
        if let Some(answer) = query.verdict(interval) {
            return Ok(Some(answer));
        }
    }

    if target_area {
        let (area_row, boundary_row) = if point_operand == 0 {
            (Loc::Exterior, Loc::Exterior)
        } else {
            (Loc::Interior, Loc::Boundary)
        };
        let (area_col, boundary_col) = if point_operand == 0 {
            (Loc::Interior, Loc::Boundary)
        } else {
            (Loc::Exterior, Loc::Exterior)
        };
        interval.witness(area_row, area_col, Dimension::Surface);
        interval.witness(boundary_row, boundary_col, Dimension::Curve);
        interval.settle(area_row, area_col);
        interval.settle(boundary_row, boundary_col);
        if let Some(answer) = query.verdict(interval) {
            return Ok(Some(answer));
        }
        // The point operand has no boundary or faces. All of its interior
        // points were sampled, and the area certificate settles the partner's
        // remaining faces and boundary outside those finitely many points.
        for row in LOCATIONS {
            for col in LOCATIONS {
                if (point_operand == 0 && row == Loc::Interior)
                    || (point_operand == 1 && col == Loc::Interior)
                {
                    interval.settle(row, col);
                }
            }
        }
        return Ok(query.verdict(interval));
    }
    Ok(None)
}

fn witness_point(
    interval: &mut De9imInterval,
    left: SourcePointLocation,
    right: SourcePointLocation,
    needs_dimensions: bool,
) {
    if left.location != Loc::Exterior || right.location != Loc::Exterior {
        interval.witness(left.location, right.location, Dimension::Point);
    }
    if left.area && needs_dimensions {
        interval.floor(matrix_predicates::Operand::Left, Dimension::Surface);
    }
    if right.area && needs_dimensions {
        interval.floor(matrix_predicates::Operand::Right, Dimension::Surface);
    }

    if left.area && right.location == Loc::Exterior {
        interval.witness(Loc::Interior, Loc::Exterior, Dimension::Surface);
        if left.location == Loc::Boundary {
            interval.witness(Loc::Boundary, Loc::Exterior, Dimension::Curve);
        }
    }
    if right.area && left.location == Loc::Exterior {
        interval.witness(Loc::Exterior, Loc::Interior, Dimension::Surface);
        if right.location == Loc::Boundary {
            interval.witness(Loc::Exterior, Loc::Boundary, Dimension::Curve);
        }
    }
    if left.area && right.area {
        if right.location == Loc::Interior {
            interval.witness(Loc::Interior, Loc::Interior, Dimension::Surface);
            if left.location == Loc::Boundary {
                interval.witness(Loc::Boundary, Loc::Interior, Dimension::Curve);
            }
        }
        if left.location == Loc::Interior {
            interval.witness(Loc::Interior, Loc::Interior, Dimension::Surface);
            if right.location == Loc::Boundary {
                interval.witness(Loc::Interior, Loc::Boundary, Dimension::Curve);
            }
        }
    }
}

fn row_certificates(row: ShapeRow<'_>) -> Result<SourceCertificates> {
    let mut certificates = SourceCertificates::default();
    match row {
        ShapeRow::Handle(plan) => collect_shape_certificates(plan.shape(), &mut certificates)?,
        ShapeRow::Shape(shape) => collect_shape_certificates(shape, &mut certificates)?,
        ShapeRow::Point(_) => certificates.point = true,
        ShapeRow::Packed(PackedRow::Points { .. }) => {
            certificates.point = !row.is_empty();
        },
        ShapeRow::Packed(PackedRow::Lines { .. }) => {
            let coordinates = row
                .line_coords()
                .expect("packed line row carries coordinates");
            let mut line_has_nonzero_segment = false;
            for [start, end] in coordinates.segment_pairs() {
                if !same_point(start.xy(), end.xy()) {
                    line_has_nonzero_segment = true;
                    break;
                }
            }
            certificates.nonzero_line = line_has_nonzero_segment;
            certificates.point = coordinates.coord_count() != 0 && !line_has_nonzero_segment;
        },
        ShapeRow::Packed(PackedRow::Polygons { .. }) => {
            let mut rings = row
                .polygon_rings()
                .expect("packed polygon row carries rings");
            if let Some(shell) = rings.next()
                && rings.next().is_none()
                && shell.coord_count() >= crate::geometry::Ring::MIN_VERTICES_CLOSED
            {
                certificates.hole_free_area = !ring_winding(&shell).is_degenerate();
            }
        },
    }
    Ok(certificates)
}

fn collect_shape_certificates(shape: &Shape, certificates: &mut SourceCertificates) -> Result<()> {
    shape.try_for_each_component(&mut |component| {
        match component {
            crate::geometry::Component::Point(_) => certificates.point = true,
            crate::geometry::Component::LineString(line) => {
                let mut line_has_nonzero_segment = false;
                for [start, end] in line.segment_pairs() {
                    if !same_point(start.xy(), end.xy()) {
                        line_has_nonzero_segment = true;
                        break;
                    }
                }
                certificates.nonzero_line |= line_has_nonzero_segment;
                certificates.point |= line.coord_count() != 0 && !line_has_nonzero_segment;
            },
            crate::geometry::Component::Polygon(polygon) => {
                if polygon.holes.is_empty() && super::graph_matrix::ring_has_area(&polygon.shell) {
                    certificates.hole_free_area = true;
                }
            },
        }
        Ok::<(), crate::error::Error>(())
    })?;
    Ok(())
}

fn apply_dimension_certificates(
    interval: &mut De9imInterval,
    dimensions: [Option<Dimension>; 2],
    certificates: [SourceCertificates; 2],
) {
    for ((dimension, certificate), owner) in dimensions.into_iter().zip(certificates).zip([
        matrix_predicates::Operand::Left,
        matrix_predicates::Operand::Right,
    ]) {
        if certificate.hole_free_area {
            interval.floor(owner, Dimension::Surface);
        } else if certificate.nonzero_line {
            interval.floor(owner, Dimension::Curve);
        } else if dimension == Some(Dimension::Point) && certificate.point {
            interval.floor(owner, Dimension::Point);
        }
    }
}

fn stage_components(
    sources: StagedExactSources,
    rows: [ShapeRow<'_>; 2],
) -> Result<StagedGeometry> {
    let staged_components = sources
        .relation_components()
        .expect("relation staging retains component ranges");
    let mut components = Vec::with_capacity(staged_components.len());
    let mut segment_component = Vec::new();
    segment_component.resize(sources.segments().len(), u32::MAX);
    let mut operand_has_ring = [false; 2];
    for (component_id, source) in staged_components.iter().enumerate() {
        let component_id = u32::try_from(component_id).map_err(|_| ResourceError::SizeOverflow)?;
        if component_id == u32::MAX {
            return Err(ResourceError::SizeOverflow.into());
        }
        let operand = usize::try_from(source.operand).map_err(|_| ResourceError::SizeOverflow)?;
        let Some(has_ring) = operand_has_ring.get_mut(operand) else {
            return Err(GeometryErrorKind::Invalid(
                "relation staging produced a non-pair operand".into(),
            )
            .into());
        };
        let range = segment_component
            .get_mut(source.segments.clone())
            .ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "relation component range exceeds the staged segment pool".into(),
                )
            })?;
        range.fill(component_id);
        if let Some(ring) = source.ring {
            *has_ring = true;
            if usize::try_from(ring).map_err(|_| ResourceError::SizeOverflow)?
                >= sources.ring_kinds().len()
            {
                return Err(GeometryErrorKind::Invalid(
                    "relation component refers to a missing staged ring".into(),
                )
                .into());
            }
        }
        components.push(Component {
            source: source.clone(),
            dirty: source.ring_kind == Some(RingKind::Collapsed),
            seed: None,
            left_area: None,
        });
    }
    if segment_component.contains(&u32::MAX) {
        return Err(GeometryErrorKind::Invalid(
            "relation staging left a segment without a component".into(),
        )
        .into());
    }
    Ok(StagedGeometry {
        sources,
        components,
        segment_component,
        operand_has_area: [rows[0].has_area_parts(), rows[1].has_area_parts()],
        operand_has_ring,
    })
}

fn apply_staged_dimension_floors(
    interval: &mut De9imInterval,
    _dimensions: [Option<Dimension>; 2],
    staged: &StagedGeometry,
    needs_dimensions: bool,
) {
    if !needs_dimensions {
        return;
    }
    for (index, owner) in [
        matrix_predicates::Operand::Left,
        matrix_predicates::Operand::Right,
    ]
    .into_iter()
    .enumerate()
    {
        if staged.components.iter().any(|component| {
            component.source.operand as usize == index
                && component.source.ring.is_none()
                && component.source.has_nonzero_segment
        }) {
            interval.floor(owner, Dimension::Curve);
        }
    }
}

enum SweepStop<T> {
    Answer(T),
    Error(crate::error::Error),
}

fn contact_sweep<Q: Query, const SATURATE: bool>(
    interval: &mut De9imInterval,
    operands: [Operand<'_>; 2],
    query: &Q,
    sources: &StagedExactSources,
    components: &mut [Component],
    segment_component: &[u32],
    operand_has_ring: [bool; 2],
    events: &mut Vec<ContactEvent>,
) -> Result<Option<Q::Output>> {
    let segments = sources.segments();
    let flow = for_each_candidate_pair(
        segments,
        |segment| {
            *segment_component
                .get(segment)
                .expect("each staged segment has a component")
        },
        |left, right| match process_pair::<Q, SATURATE>(
            interval,
            operands,
            query,
            segments,
            components,
            segment_component,
            operand_has_ring,
            left,
            right,
            events,
        ) {
            Ok(Some(answer)) => ControlFlow::Break(SweepStop::Answer(answer)),
            Ok(None) => ControlFlow::Continue(()),
            Err(error) => ControlFlow::Break(SweepStop::Error(error)),
        },
    )?;
    match flow {
        ControlFlow::Continue(()) => Ok(None),
        ControlFlow::Break(SweepStop::Answer(answer)) => Ok(Some(answer)),
        ControlFlow::Break(SweepStop::Error(error)) => Err(error),
    }
}

fn process_pair<Q: Query, const SATURATE: bool>(
    interval: &mut De9imInterval,
    operands: [Operand<'_>; 2],
    query: &Q,
    segments: &[Segment],
    components: &mut [Component],
    segment_component: &[u32],
    operand_has_ring: [bool; 2],
    left: usize,
    right: usize,
    events: &mut Vec<ContactEvent>,
) -> Result<Option<Q::Output>> {
    let (Some(&a), Some(&b)) = (segments.get(left), segments.get(right)) else {
        return Err(GeometryErrorKind::Invalid(
            "candidate pair exceeds the staged segment pool".into(),
        )
        .into());
    };
    let left_component_id = *segment_component
        .get(left)
        .ok_or_else(|| GeometryErrorKind::Invalid("left segment has no component id".into()))?;
    let right_component_id = *segment_component
        .get(right)
        .ok_or_else(|| GeometryErrorKind::Invalid("right segment has no component id".into()))?;
    let left_component =
        usize::try_from(left_component_id).map_err(|_| ResourceError::SizeOverflow)?;
    let right_component =
        usize::try_from(right_component_id).map_err(|_| ResourceError::SizeOverflow)?;
    let (left_operand, right_operand) = (
        components
            .get(left_component)
            .ok_or_else(|| GeometryErrorKind::Invalid("left component id is out of range".into()))?
            .source
            .operand,
        components
            .get(right_component)
            .ok_or_else(|| GeometryErrorKind::Invalid("right component id is out of range".into()))?
            .source
            .operand,
    );
    if !components
        .get(left_component)
        .expect("left component was read above")
        .source
        .has_nonzero_segment
        || !components
            .get(right_component)
            .expect("right component was read above")
            .source
            .has_nonzero_segment
    {
        return Ok(None);
    }
    let same_operand = left_operand == right_operand;
    let operand_index = usize::try_from(left_operand).map_err(|_| ResourceError::SizeOverflow)?;
    if same_operand && !operand_has_ring[operand_index] {
        return Ok(None);
    }

    let contact = segment_contact_exact(a, b);
    let Some(event) = make_event(
        contact,
        a,
        b,
        left,
        right,
        left_component_id,
        right_component_id,
    )?
    else {
        return Ok(None);
    };

    if same_operand
        && !benign_self_touch(
            contact,
            left,
            right,
            &components
                .get(left_component)
                .expect("left component admitted above")
                .source,
            &components
                .get(right_component)
                .expect("right component admitted above")
                .source,
        )
    {
        if left_component == right_component {
            components
                .get_mut(left_component)
                .expect("left component admitted above")
                .dirty = true;
        } else {
            components
                .get_mut(left_component)
                .expect("left component admitted above")
                .dirty = true;
            components
                .get_mut(right_component)
                .expect("right component admitted above")
                .dirty = true;
        }
    }

    events.push(event);
    if same_operand {
        return Ok(None);
    }

    let event = events
        .last_mut()
        .expect("the current pair event was appended");
    for point_index in 0..event.points.len() {
        let Some(point) = event.points.get_mut(point_index).and_then(Option::as_mut) else {
            continue;
        };
        let exact = event_point_exact(point, segments)?;
        let left_location = operands[0].locate_exact(&exact)?;
        let right_location = operands[1].locate_exact(&exact)?;
        witness_point(
            interval,
            left_location,
            right_location,
            query.needs_dimensions(),
        );
        if let Some(answer) = query.verdict(interval) {
            return Ok(Some(answer));
        }
    }

    match event.kind {
        EventKind::Touch => {},
        EventKind::Collinear { .. } => {
            let (first_point, rest) = event.points.split_at_mut(1);
            let (Some(start), Some(end)) = (
                first_point.first_mut().and_then(Option::as_mut),
                rest.first_mut().and_then(Option::as_mut),
            ) else {
                return Err(GeometryErrorKind::Invalid(
                    "collinear event is missing a span endpoint".into(),
                )
                .into());
            };
            let start = event_point_exact(start, segments)?;
            let end = event_point_exact(end, segments)?;
            if !same_homogeneous_point(&start, &end)? {
                let midpoint = homogeneous_midpoint(&start, &end)?;
                let left_location = operands[0].locate_exact(&midpoint)?;
                let right_location = operands[1].locate_exact(&midpoint)?;
                witness_point(
                    interval,
                    left_location,
                    right_location,
                    query.needs_dimensions(),
                );
                let left_source = components.get(left_component).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "collinear event refers to a missing left component".into(),
                    )
                })?;
                let right_source = components.get(right_component).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "collinear event refers to a missing right component".into(),
                    )
                })?;
                let (left_edge, right_edge) = if left_operand == 0 {
                    (
                        component_edge_atom_location(left_source, left_location),
                        component_edge_atom_location(right_source, right_location),
                    )
                } else {
                    (
                        component_edge_atom_location(right_source, left_location),
                        component_edge_atom_location(left_source, right_location),
                    )
                };
                if left_edge != Loc::Exterior || right_edge != Loc::Exterior {
                    interval.witness(left_edge, right_edge, Dimension::Curve);
                }
                if let Some(answer) = query.verdict(interval) {
                    return Ok(Some(answer));
                }
            }
        },
        EventKind::Cross => {
            let Some(point) = event.points.first_mut().and_then(Option::as_mut) else {
                return Err(GeometryErrorKind::Invalid(
                    "crossing event is missing its point".into(),
                )
                .into());
            };
            let point = event_point_exact(point, segments)?;
            if !operands[0].row.has_area_parts() {
                for direction in segment_directions(a)? {
                    match operands[1].occupied_toward_exact(&point, &direction)? {
                        OccupiedToward::Yes => {
                            interval.witness(Loc::Interior, Loc::Interior, Dimension::Curve)
                        },
                        OccupiedToward::No => {
                            interval.witness(Loc::Interior, Loc::Exterior, Dimension::Curve)
                        },
                        OccupiedToward::Tangent => {},
                    }
                    if let Some(answer) = query.verdict(interval) {
                        return Ok(Some(answer));
                    }
                }
            }
            if !operands[1].row.has_area_parts() {
                for direction in segment_directions(b)? {
                    match operands[0].occupied_toward_exact(&point, &direction)? {
                        OccupiedToward::Yes => {
                            interval.witness(Loc::Interior, Loc::Interior, Dimension::Curve)
                        },
                        OccupiedToward::No => {
                            interval.witness(Loc::Exterior, Loc::Interior, Dimension::Curve)
                        },
                        OccupiedToward::Tangent => {},
                    }
                    if let Some(answer) = query.verdict(interval) {
                        return Ok(Some(answer));
                    }
                }
            }
            if operands[0].row.has_area_parts() && operands[1].row.has_area_parts() {
                for direction in crossing_quadrants(a, b)? {
                    let left = operands[0].occupied_toward_exact(&point, &direction)?;
                    let right = operands[1].occupied_toward_exact(&point, &direction)?;
                    let (left, right) = match (left, right) {
                        (OccupiedToward::Yes, OccupiedToward::Yes) => {
                            (Loc::Interior, Loc::Interior)
                        },
                        (OccupiedToward::Yes, OccupiedToward::No) => (Loc::Interior, Loc::Exterior),
                        (OccupiedToward::No, OccupiedToward::Yes) => (Loc::Exterior, Loc::Interior),
                        (OccupiedToward::No, OccupiedToward::No) => (Loc::Exterior, Loc::Exterior),
                        _ => continue,
                    };
                    interval.witness(left, right, Dimension::Surface);
                    if let Some(answer) = query.verdict(interval) {
                        return Ok(Some(answer));
                    }
                }
            }
        },
    }
    if SATURATE && interval.settled_for(query.cells()) {
        return Ok(query.verdict(interval));
    }
    Ok(None)
}

fn make_event(
    contact: Contact,
    left: Segment,
    right: Segment,
    left_index: usize,
    right_index: usize,
    left_component: u32,
    right_component: u32,
) -> Result<Option<ContactEvent>> {
    let (kind, points) = match contact {
        Contact::Disjoint => return Ok(None),
        Contact::Touch(location) => (EventKind::Touch, [
            Some(EventPoint::stored(location.point(left, right))),
            None,
        ]),
        Contact::Cross => {
            let segments = [
                u32::try_from(left_index).map_err(|_| ResourceError::SizeOverflow)?,
                u32::try_from(right_index).map_err(|_| ResourceError::SizeOverflow)?,
            ];
            let crossing = crossing_point(left, right)?;
            (EventKind::Cross, [
                Some(EventPoint::crossing(
                    segments,
                    crossing.point,
                    crossing.exact,
                )),
                None,
            ])
        },
        Contact::Collinear => {
            let span = contact
                .shared_span(left, right)
                .expect("collinear carriers with overlapping envelopes have a shared span");
            (
                EventKind::Collinear {
                    same_direction: span.same_direction,
                },
                [
                    Some(EventPoint::stored(span.start)),
                    Some(EventPoint::stored(span.end)),
                ],
            )
        },
    };
    Ok(Some(ContactEvent {
        segments: [left_index, right_index],
        components: [left_component, right_component],
        kind,
        points,
    }))
}

fn benign_self_touch(
    contact: Contact,
    left: usize,
    right: usize,
    left_component: &StagedSourceComponent,
    right_component: &StagedSourceComponent,
) -> bool {
    if left_component.segments != right_component.segments {
        return false;
    }
    if !matches!(contact, Contact::Touch(TouchLocation::Shared(_))) {
        return false;
    }
    right == left.wrapping_add(1)
        || (left_component.ring.is_some()
            && left == left_component.segments.start
            && right.wrapping_add(1) == left_component.segments.end)
}

impl EventPoint {
    fn stored(point: XY) -> Self {
        Self {
            kind: EventPointKind::Stored(point),
            exact: None,
            handle: None,
        }
    }

    fn crossing(segments: [u32; 2], rounded: XY, exact: Option<[ExactDyadic; 3]>) -> Self {
        Self {
            kind: EventPointKind::Crossing { segments, rounded },
            exact: exact.map(Box::new),
            handle: None,
        }
    }
}

fn event_point_exact(point: &mut EventPoint, segments: &[Segment]) -> Result<[ExactDyadic; 3]> {
    if let Some(exact) = &point.exact {
        return Ok(exact.as_ref().clone());
    }
    let exact = match point.kind {
        EventPointKind::Stored(point) => stored_exact_point(point),
        EventPointKind::Crossing {
            segments: [left, right],
            ..
        } => {
            let left = usize::try_from(left).map_err(|_| ResourceError::SizeOverflow)?;
            let right = usize::try_from(right).map_err(|_| ResourceError::SizeOverflow)?;
            let left = *segments.get(left).ok_or_else(|| {
                GeometryErrorKind::Invalid("crossing references a missing left segment".into())
            })?;
            let right = *segments.get(right).ok_or_else(|| {
                GeometryErrorKind::Invalid("crossing references a missing right segment".into())
            })?;
            crossing_homogeneous(left, right)?
        },
    };
    point.exact = Some(Box::new(exact.clone()));
    Ok(exact)
}

fn stored_exact_point(point: XY) -> [ExactDyadic; 3] {
    [
        ExactDyadic::from_f64(point.x),
        ExactDyadic::from_f64(point.y),
        ExactDyadic::one(),
    ]
}

fn same_homogeneous_point(left: &[ExactDyadic; 3], right: &[ExactDyadic; 3]) -> Result<bool> {
    Ok(
        compare_ratios(&left[0], &left[2], &right[0], &right[2])? == Ordering::Equal
            && compare_ratios(&left[1], &left[2], &right[1], &right[2])? == Ordering::Equal,
    )
}

fn segment_directions(segment: Segment) -> Result<[[ExactDyadic; 2]; 2]> {
    let x = ExactDyadic::from_f64(segment.end.x)
        .try_subtract(ExactDyadic::from_f64(segment.start.x))?;
    let y = ExactDyadic::from_f64(segment.end.y)
        .try_subtract(ExactDyadic::from_f64(segment.start.y))?;
    Ok([[x.clone(), y.clone()], [x.neg(), y.neg()]])
}

fn crossing_quadrants(left: Segment, right: Segment) -> Result<[[ExactDyadic; 2]; 4]> {
    let [u, _] = segment_directions(left)?;
    let [v, _] = segment_directions(right)?;
    let combine = |u: &[ExactDyadic; 2],
                   v: &[ExactDyadic; 2],
                   u_sign: bool,
                   v_sign: bool|
     -> Result<[ExactDyadic; 2]> {
        let ux = if u_sign {
            u[0].clone()
        } else {
            u[0].clone().neg()
        };
        let uy = if u_sign {
            u[1].clone()
        } else {
            u[1].clone().neg()
        };
        let vx = if v_sign {
            v[0].clone()
        } else {
            v[0].clone().neg()
        };
        let vy = if v_sign {
            v[1].clone()
        } else {
            v[1].clone().neg()
        };
        Ok([ux.try_add(vx)?, uy.try_add(vy)?])
    };
    Ok([
        combine(&u, &v, true, true)?,
        combine(&u, &v, true, false)?,
        combine(&u, &v, false, true)?,
        combine(&u, &v, false, false)?,
    ])
}

#[derive(Clone, Copy)]
struct AtomSides {
    left: bool,
    right: bool,
}

impl AtomSides {
    const fn new(left: bool, right: bool) -> Self {
        Self { left, right }
    }

    const fn swap(self) -> Self {
        Self {
            left: self.right,
            right: self.left,
        }
    }
}

#[derive(Clone, Copy)]
struct RunCache {
    component: u32,
    end: AtomPoint,
    label: Loc,
}

fn atom_owner_label(
    operand: Operand<'_>,
    component: &Component,
    start: &[ExactDyadic; 3],
    direction: &[ExactDyadic; 2],
) -> Result<Loc> {
    if !component.dirty {
        return match component.source.ring_kind {
            Some(RingKind::Winding) => component
                .seed
                .map(SourcePointLocation::edge_atom_location)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "a clean winding ring has no representative location".into(),
                    )
                    .into()
                }),
            Some(RingKind::Collapsed) => Err(GeometryErrorKind::Invalid(
                "collapsed rings cannot use a clean atom label".into(),
            )
            .into()),
            None => Ok(Loc::Interior),
        };
    }
    Ok(operand.locate_exact_offset(start, direction)?.location)
}

fn component_edge_atom_location(component: &Component, location: SourcePointLocation) -> Loc {
    if !component.dirty && component.source.ring_kind == Some(RingKind::Winding) {
        location.edge_atom_location()
    } else {
        location.location
    }
}

fn atom_sides(
    operand: Operand<'_>,
    component: &Component,
    has_area: bool,
    midpoint: &[ExactDyadic; 3],
    direction: &[ExactDyadic; 2],
) -> Result<AtomSides> {
    if !has_area {
        return Ok(AtomSides::new(false, false));
    }
    if !component.dirty {
        match component.source.ring_kind {
            Some(RingKind::Winding) => {
                let seed = component.seed.ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "a clean winding ring has no representative location".into(),
                    )
                })?;
                return match seed.edge_atom_location() {
                    Loc::Interior => Ok(AtomSides::new(true, true)),
                    Loc::Exterior => Ok(AtomSides::new(false, false)),
                    Loc::Boundary => {
                        let left = component.left_area.ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "a clean winding ring has no left-face label".into(),
                            )
                        })?;
                        Ok(AtomSides::new(left, !left))
                    },
                };
            },
            Some(RingKind::Collapsed) => {
                return Err(GeometryErrorKind::Invalid(
                    "collapsed rings cannot use clean side labels".into(),
                )
                .into());
            },
            None => {
                let area = component.left_area.ok_or_else(|| {
                    GeometryErrorKind::Invalid("a clean line has no areal side label".into())
                })?;
                return Ok(AtomSides::new(area, area));
            },
        }
    }
    let left = segment_left_normal_from_direction(direction);
    let right = [left[0].clone().neg(), left[1].clone().neg()];
    Ok(AtomSides::new(
        occupied_area_side(operand, midpoint, &left)?,
        occupied_area_side(operand, midpoint, &right)?,
    ))
}

fn occupied_area_side(
    operand: Operand<'_>,
    point: &[ExactDyadic; 3],
    direction: &[ExactDyadic; 2],
) -> Result<bool> {
    match operand.occupied_toward_exact(point, direction)? {
        OccupiedToward::Yes => Ok(true),
        OccupiedToward::No => Ok(false),
        OccupiedToward::Tangent => Err(GeometryErrorKind::Invalid(
            "an exhaustive atom-side probe is tangent to source support".into(),
        )
        .into()),
    }
}

fn segment_left_normal_from_direction(direction: &[ExactDyadic; 2]) -> [ExactDyadic; 2] {
    [direction[1].clone().neg(), direction[0].clone()]
}

fn run_label(
    operand: Operand<'_>,
    component: u32,
    start: AtomPoint,
    start_exact: &[ExactDyadic; 3],
    direction: &[ExactDyadic; 2],
    cache: &Option<RunCache>,
    forest: &mut ExactPointForest<'_>,
    cross_stored: &HashSet<PointKey>,
    cross_constructed: &HashSet<u32>,
) -> Result<Loc> {
    if !is_cross_event_node(start, forest, cross_stored, cross_constructed)
        && let Some(cache) = cache
        && cache.component == component
        && same_atom_point(cache.end, start, forest)?
    {
        return Ok(cache.label);
    }
    match operand.occupied_toward_exact(start_exact, direction)? {
        OccupiedToward::Yes => Ok(Loc::Interior),
        OccupiedToward::No => Ok(Loc::Exterior),
        OccupiedToward::Tangent => Err(GeometryErrorKind::Invalid(
            "an uncovered source atom has a tangent face probe".into(),
        )
        .into()),
    }
}

fn exhaustive_pass<Q: Query, const SATURATE: bool>(
    interval: &mut De9imInterval,
    operands: [Operand<'_>; 2],
    query: &Q,
    sources: &StagedExactSources,
    components: &[Component],
    segment_component: &[u32],
    operand_has_area: [bool; 2],
    events: &mut [ContactEvent],
    forest: &mut ExactPointForest<'_>,
    stored_points: &mut HashMap<PointKey, u32>,
) -> Result<Option<Q::Output>> {
    let segments = sources.segments();
    let mut segment_cuts = intern_event_points(events, segments, forest, stored_points)?;
    let (boundaries, cut_positions) = order_event_cuts(segments, &mut segment_cuts, forest)?;
    let mut coverage = build_atom_coverage(
        events,
        segments,
        components,
        &boundaries,
        &cut_positions,
        forest,
    )?;
    let (cross_stored, cross_constructed) = cross_event_nodes(events, components, forest)?;
    if let Some(answer) = event_node_witnesses(interval, operands, query, events, forest)? {
        return Ok(Some(answer));
    }

    let mut run_cache = None;
    for segment_index in 0..segments.len() {
        if SATURATE && interval.settled_for(query.cells()) {
            return Ok(query.verdict(interval));
        }
        let Some(&segment) = segments.get(segment_index) else {
            return Err(
                GeometryErrorKind::Invalid("segment loop exceeded the staged pool".into()).into(),
            );
        };
        if same_point(segment.start, segment.end) {
            continue;
        }
        let component_id = *segment_component
            .get(segment_index)
            .ok_or_else(|| GeometryErrorKind::Invalid("segment has no staged component".into()))?;
        let component_index =
            usize::try_from(component_id).map_err(|_| ResourceError::SizeOverflow)?;
        let component = components.get(component_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("segment component ID is out of range".into())
        })?;
        let operand_index =
            usize::try_from(component.source.operand).map_err(|_| ResourceError::SizeOverflow)?;
        let target_index = operand_index ^ 1;
        let cuts = boundaries.get(segment_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("segment has no ordered boundaries".into())
        })?;
        let atom_cover = coverage
            .get_mut(segment_index)
            .ok_or_else(|| GeometryErrorKind::Invalid("segment has no atom-coverage row".into()))?;
        let direction = segment_direction(segment)?;
        for atom_index in 0..cuts.len().saturating_sub(1) {
            if SATURATE && interval.settled_for(query.cells()) {
                return Ok(query.verdict(interval));
            }
            let start = *cuts
                .get(atom_index)
                .ok_or_else(|| GeometryErrorKind::Invalid("atom start cut is missing".into()))?;
            let end = *cuts
                .get(atom_index.wrapping_add(1))
                .ok_or_else(|| GeometryErrorKind::Invalid("atom end cut is missing".into()))?;
            if same_atom_point(start, end, forest)? {
                continue;
            }
            let start_exact = atom_point_exact(start, forest)?;
            let end_exact = atom_point_exact(end, forest)?;
            let midpoint = homogeneous_midpoint(&start_exact, &end_exact)?;
            let own =
                atom_owner_label(operands[operand_index], component, &start_exact, &direction)?;
            let cover = atom_cover.get(atom_index).copied().flatten();
            let (foreign, foreign_sides) = if let Some(cover) = cover {
                let partner_index = usize::try_from(cover.partner_component)
                    .map_err(|_| ResourceError::SizeOverflow)?;
                let partner = components.get(partner_index).ok_or_else(|| {
                    GeometryErrorKind::Invalid("coverage partner component is out of range".into())
                })?;
                let foreign =
                    atom_owner_label(operands[target_index], partner, &start_exact, &direction)?;
                let mut sides = atom_sides(
                    operands[target_index],
                    partner,
                    operand_has_area[target_index],
                    &midpoint,
                    &direction,
                )?;
                if !cover.same_direction {
                    sides = sides.swap();
                }
                (foreign, sides)
            } else {
                let foreign = if operand_has_area[target_index] {
                    run_label(
                        operands[target_index],
                        component_id,
                        start,
                        &start_exact,
                        &direction,
                        &mut run_cache,
                        forest,
                        &cross_stored,
                        &cross_constructed,
                    )?
                } else {
                    run_cache = None;
                    Loc::Exterior
                };
                let occupied = foreign == Loc::Interior;
                (foreign, AtomSides::new(occupied, occupied))
            };
            let own_sides = atom_sides(
                operands[operand_index],
                component,
                operand_has_area[operand_index],
                &midpoint,
                &direction,
            )?;

            let (left_label, right_label) = if operand_index == 0 {
                (own, foreign)
            } else {
                (foreign, own)
            };
            if left_label != Loc::Exterior || right_label != Loc::Exterior {
                interval.witness(left_label, right_label, Dimension::Curve);
            }
            let side_pairs = if operand_index == 0 {
                [
                    (own_sides.left, foreign_sides.left),
                    (own_sides.right, foreign_sides.right),
                ]
            } else {
                [
                    (foreign_sides.left, own_sides.left),
                    (foreign_sides.right, own_sides.right),
                ]
            };
            for (left_occupied, right_occupied) in side_pairs {
                interval.witness(
                    if left_occupied {
                        Loc::Interior
                    } else {
                        Loc::Exterior
                    },
                    if right_occupied {
                        Loc::Interior
                    } else {
                        Loc::Exterior
                    },
                    Dimension::Surface,
                );
            }
            if cover.is_some() {
                run_cache = None;
            } else if operand_has_area[target_index] {
                run_cache = Some(RunCache {
                    component: component_id,
                    end,
                    label: foreign,
                });
            }
            if let Some(answer) = query.verdict(interval) {
                return Ok(Some(answer));
            }
        }
    }
    settle_all(interval);
    Ok(query.verdict(interval))
}

fn intern_event_points(
    events: &mut [ContactEvent],
    segments: &[Segment],
    forest: &mut ExactPointForest<'_>,
    stored_points: &mut HashMap<PointKey, u32>,
) -> Result<Vec<Vec<u32>>> {
    let mut segment_cuts = Vec::with_capacity(segments.len());
    segment_cuts.resize_with(segments.len(), Vec::new);
    for event in events {
        let pair = event.segments;
        for point in event.points.iter_mut().flatten() {
            let handle = if let Some(handle) = point.handle {
                handle
            } else {
                let handle = match point.kind {
                    EventPointKind::Stored(coordinate) => {
                        let key = PointKey::new(coordinate);
                        if let Some(&handle) = stored_points.get(&key) {
                            handle
                        } else {
                            let handle = forest.push_stored(coordinate)?;
                            stored_points.insert(key, handle);
                            handle
                        }
                    },
                    EventPointKind::Crossing {
                        segments: crossing_segments,
                        rounded,
                    } => {
                        let exact = event_point_exact(point, segments)?;
                        forest.push_crossing_with_exact(crossing_segments, rounded, exact)?
                    },
                };
                point.handle = Some(handle);
                handle
            };
            for segment in pair {
                segment_cuts
                    .get_mut(segment)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "event references a missing staged segment".into(),
                        )
                    })?
                    .push(handle);
            }
        }
    }
    Ok(segment_cuts)
}

fn order_event_cuts(
    segments: &[Segment],
    segment_cuts: &mut [Vec<u32>],
    forest: &mut ExactPointForest<'_>,
) -> Result<(Vec<Vec<AtomPoint>>, HashMap<(usize, u32), usize>)> {
    for (segment_index, cuts) in segment_cuts.iter_mut().enumerate() {
        let segment = *segments
            .get(segment_index)
            .expect("cut list is aligned with the source segment pool");
        let mut equal = Vec::new();
        forest.points().order_along(
            segment,
            cuts,
            |handle| handle,
            |left, right| {
                equal.push((left, right));
                Ok(())
            },
        )?;
        for (left, right) in equal {
            if !forest.points().same_exact_point(left, right)? {
                return Err(GeometryErrorKind::Invalid(
                    "equal cut parameters identify different exact points".into(),
                )
                .into());
            }
            forest.union(left, right)?;
        }
    }

    let mut positions = HashMap::with_hasher(collection_hash_state()?.clone());
    let capacity = segment_cuts.iter().map(Vec::len).sum();
    positions.reserve(capacity);
    let mut boundaries = Vec::with_capacity(segments.len());
    for (segment_index, (segment, cuts)) in segments.iter().zip(segment_cuts).enumerate() {
        let mut ordered = Vec::with_capacity(cuts.len().wrapping_add(2));
        ordered.push(AtomPoint::Endpoint(segment.start));
        let mut previous = None;
        for &handle in cuts.iter() {
            let root = forest.find(handle);
            if previous == Some(root) {
                continue;
            }
            previous = Some(root);
            if point_handle_equals_xy(forest, root, segment.start)?
                || point_handle_equals_xy(forest, root, segment.end)?
            {
                continue;
            }
            let position = ordered.len();
            ordered.push(AtomPoint::Event(root));
            positions.insert((segment_index, root), position);
        }
        ordered.push(AtomPoint::Endpoint(segment.end));
        boundaries.push(ordered);
    }
    Ok((boundaries, positions))
}

fn point_handle_equals_xy(forest: &ExactPointForest<'_>, handle: u32, point: XY) -> Result<bool> {
    let arena = forest.points();
    if let Some(key) = arena.stored_key(handle) {
        return Ok(key == PointKey::new(point));
    }
    let exact = arena.exact(handle)?;
    let [x, y, weight] = &*exact;
    Ok(compare_ratios(
        x,
        weight,
        &ExactDyadic::from_f64(point.x),
        &ExactDyadic::one(),
    )? == Ordering::Equal
        && compare_ratios(
            y,
            weight,
            &ExactDyadic::from_f64(point.y),
            &ExactDyadic::one(),
        )? == Ordering::Equal)
}

fn same_atom_point(
    left: AtomPoint,
    right: AtomPoint,
    forest: &ExactPointForest<'_>,
) -> Result<bool> {
    match (left, right) {
        (AtomPoint::Endpoint(left), AtomPoint::Endpoint(right)) => Ok(same_point(left, right)),
        (AtomPoint::Event(left), AtomPoint::Event(right)) => {
            forest.points().same_exact_point(left, right)
        },
        (AtomPoint::Endpoint(endpoint), AtomPoint::Event(handle))
        | (AtomPoint::Event(handle), AtomPoint::Endpoint(endpoint)) => {
            point_handle_equals_xy(forest, handle, endpoint)
        },
    }
}

fn atom_point_exact(point: AtomPoint, forest: &ExactPointForest<'_>) -> Result<[ExactDyadic; 3]> {
    match point {
        AtomPoint::Endpoint(point) => Ok(stored_exact_point(point)),
        AtomPoint::Event(handle) => Ok(forest.points().exact(handle)?.as_ref().clone()),
    }
}

fn atom_position(
    segment_index: usize,
    segment: Segment,
    handle: u32,
    boundary_count: usize,
    positions: &HashMap<(usize, u32), usize>,
    forest: &ExactPointForest<'_>,
) -> Result<usize> {
    if point_handle_equals_xy(forest, handle, segment.start)? {
        return Ok(0);
    }
    if point_handle_equals_xy(forest, handle, segment.end)? {
        return Ok(boundary_count.wrapping_sub(1));
    }
    positions
        .get(&(segment_index, handle))
        .copied()
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("collinear cut endpoint is absent from its segment".into())
                .into()
        })
}

fn build_atom_coverage(
    events: &[ContactEvent],
    segments: &[Segment],
    components: &[Component],
    boundaries: &[Vec<AtomPoint>],
    positions: &HashMap<(usize, u32), usize>,
    forest: &mut ExactPointForest<'_>,
) -> Result<Vec<Vec<Option<Coverage>>>> {
    let mut coverage = boundaries
        .iter()
        .map(|cuts| vec![None; cuts.len().saturating_sub(1)])
        .collect::<Vec<_>>();
    for event in events {
        let EventKind::Collinear { same_direction } = event.kind else {
            continue;
        };
        let left_component =
            usize::try_from(event.components[0]).map_err(|_| ResourceError::SizeOverflow)?;
        let right_component =
            usize::try_from(event.components[1]).map_err(|_| ResourceError::SizeOverflow)?;
        let left_operand = components
            .get(left_component)
            .expect("event left component is admitted")
            .source
            .operand;
        let right_operand = components
            .get(right_component)
            .expect("event right component is admitted")
            .source
            .operand;
        if left_operand == right_operand {
            continue;
        }
        let (Some(start), Some(end)) = (
            event
                .points
                .first()
                .and_then(Option::as_ref)
                .and_then(|p| p.handle),
            event
                .points
                .get(1)
                .and_then(Option::as_ref)
                .and_then(|p| p.handle),
        ) else {
            return Err(GeometryErrorKind::Invalid(
                "collinear event has no interned span endpoints".into(),
            )
            .into());
        };
        let start = forest.find(start);
        let end = forest.find(end);
        for (side, other_component) in [(0, event.components[1]), (1, event.components[0])] {
            let segment_index = event.segments[side];
            let segment = *segments
                .get(segment_index)
                .expect("event segment index is admitted");
            let list = boundaries
                .get(segment_index)
                .expect("event segment has its boundary list");
            let first =
                atom_position(segment_index, segment, start, list.len(), positions, forest)?;
            let last = atom_position(segment_index, segment, end, list.len(), positions, forest)?;
            let (lo, hi) = (first.min(last), first.max(last));
            let atoms = coverage
                .get_mut(segment_index)
                .expect("coverage table is aligned with segments");
            for atom in lo..hi {
                let Some(slot) = atoms.get_mut(atom) else {
                    return Err(GeometryErrorKind::Invalid(
                        "collinear span exceeds its segment's atom list".into(),
                    )
                    .into());
                };
                if slot.is_none() {
                    *slot = Some(Coverage {
                        partner_component: other_component,
                        same_direction,
                    });
                }
            }
        }
    }
    Ok(coverage)
}

fn cross_event_nodes(
    events: &[ContactEvent],
    components: &[Component],
    forest: &mut ExactPointForest<'_>,
) -> Result<(HashSet<PointKey>, HashSet<u32>)> {
    let mut stored = HashSet::new();
    let mut constructed = HashSet::new();
    for event in events {
        let left = components
            .get(usize::try_from(event.components[0]).map_err(|_| ResourceError::SizeOverflow)?)
            .expect("event left component is admitted")
            .source
            .operand;
        let right = components
            .get(usize::try_from(event.components[1]).map_err(|_| ResourceError::SizeOverflow)?)
            .expect("event right component is admitted")
            .source
            .operand;
        if left == right {
            continue;
        }
        for point in event.points.iter().flatten() {
            let Some(handle) = point.handle else {
                return Err(
                    GeometryErrorKind::Invalid("cross event node was not interned".into()).into(),
                );
            };
            let root = forest.find(handle);
            if let Some(key) = forest.points().stored_key(root) {
                stored.insert(key);
            } else {
                constructed.insert(root);
            }
        }
    }
    Ok((stored, constructed))
}

fn is_cross_event_node(
    point: AtomPoint,
    forest: &mut ExactPointForest<'_>,
    cross_stored: &HashSet<PointKey>,
    cross_constructed: &HashSet<u32>,
) -> bool {
    match point {
        AtomPoint::Endpoint(point) => cross_stored.contains(&PointKey::new(point)),
        AtomPoint::Event(handle) => {
            let root = forest.find(handle);
            forest.points().stored_key(root).map_or_else(
                || cross_constructed.contains(&root),
                |key| cross_stored.contains(&key),
            )
        },
    }
}

fn event_node_witnesses<Q: Query>(
    interval: &mut De9imInterval,
    operands: [Operand<'_>; 2],
    query: &Q,
    events: &[ContactEvent],
    forest: &mut ExactPointForest<'_>,
) -> Result<Option<Q::Output>> {
    let mut seen = HashSet::new();
    for event in events {
        for point in event.points.iter().flatten() {
            let Some(handle) = point.handle else {
                return Err(GeometryErrorKind::Invalid(
                    "event node was not interned before the exhaustive pass".into(),
                )
                .into());
            };
            let root = forest.find(handle);
            if !seen.insert(root) {
                continue;
            }
            let exact = forest.points().exact(root)?;
            let left = operands[0].locate_exact(&exact)?;
            let right = operands[1].locate_exact(&exact)?;
            witness_point(interval, left, right, query.needs_dimensions());
            if let Some(answer) = query.verdict(interval) {
                return Ok(Some(answer));
            }
        }
    }
    Ok(None)
}

fn representative_probes<Q: Query, const SATURATE: bool>(
    interval: &mut De9imInterval,
    operands: [Operand<'_>; 2],
    staged: &mut StagedGeometry,
    query: &Q,
) -> Result<Option<Q::Output>> {
    let source_order = staged
        .sources
        .relation_source_order()
        .expect("relation staging retains source order");
    for order_index in 0..source_order.len() {
        if SATURATE && interval.settled_for(query.cells()) {
            return Ok(query.verdict(interval));
        }
        let item = *source_order
            .get(order_index)
            .expect("source-order index lies within staged entries");
        match item {
            StagedRelationItem::Isolated(index) => {
                let point = *staged
                    .sources
                    .isolated()
                    .get(index)
                    .expect("isolated source ordinal lies within staged points");
                let locations = [
                    operands[0].locate_stored(Point::new_raw_xy(point.x, point.y))?,
                    operands[1].locate_stored(Point::new_raw_xy(point.x, point.y))?,
                ];
                witness_point(
                    interval,
                    locations[0],
                    locations[1],
                    query.needs_dimensions(),
                );
            },
            StagedRelationItem::Component(index) => {
                let component = staged
                    .components
                    .get(index)
                    .expect("staged component ordinal lies within its table")
                    .source
                    .clone();
                let mut representative = None;
                let mut last = None;
                visit_component_representatives(
                    &component,
                    staged.sources.segments(),
                    &mut |point| {
                        representative.get_or_insert(point);
                        if !representative.is_some_and(|first| same_point(first, point)) {
                            last = Some(point);
                        }
                    },
                );
                let Some(first) = representative else {
                    return Err(GeometryErrorKind::Invalid(
                        "a staged component has no representative point".into(),
                    )
                    .into());
                };
                let operand_index =
                    usize::try_from(component.operand).map_err(|_| ResourceError::SizeOverflow)?;
                let first_location =
                    operands[operand_index].locate_stored(Point::new_raw_xy(first.x, first.y))?;
                let last_location = if let Some(last) = last {
                    Some(operands[operand_index].locate_stored(Point::new_raw_xy(last.x, last.y))?)
                } else {
                    None
                };
                let (left_location, right_location) = if operand_index == 0 {
                    (
                        first_location,
                        operands[1].locate_stored(Point::new_raw_xy(first.x, first.y))?,
                    )
                } else {
                    (
                        operands[0].locate_stored(Point::new_raw_xy(first.x, first.y))?,
                        first_location,
                    )
                };
                witness_point(
                    interval,
                    left_location,
                    right_location,
                    query.needs_dimensions(),
                );
                if let Some(last) = last
                    && let Some(last_location) = last_location
                {
                    let (left, right) = if operand_index == 0 {
                        (
                            last_location,
                            operands[1].locate_stored(Point::new_raw_xy(last.x, last.y))?,
                        )
                    } else {
                        (
                            operands[0].locate_stored(Point::new_raw_xy(last.x, last.y))?,
                            last_location,
                        )
                    };
                    witness_point(interval, left, right, query.needs_dimensions());
                }

                if component.ring.is_some()
                    && let Some(owner) = staged.components.get_mut(index)
                {
                    owner.seed = Some(first_location);
                } else if component.has_nonzero_segment
                    && let Some(owner) = staged.components.get_mut(index)
                {
                    owner.seed = Some(first_location);
                    owner.left_area = Some(first_location.area);
                }
            },
        }
        if let Some(answer) = query.verdict(interval) {
            return Ok(Some(answer));
        }
    }
    Ok(None)
}

fn visit_component_representatives(
    component: &StagedSourceComponent,
    segments: &[Segment],
    mut visit: impl FnMut(XY),
) {
    let Some(pool) = segments.get(component.segments.clone()) else {
        return;
    };
    if component.ring.is_some() {
        if let Some(first) = pool.first() {
            visit(first.start);
        }
        return;
    }
    let mut first = None;
    let mut last = None;
    for segment in pool {
        if !same_point(segment.start, segment.end) {
            first.get_or_insert(segment.start);
            last = Some(segment.end);
        }
    }
    if let Some(first) = first {
        visit(first);
    }
    if let Some(last) = last
        && !first.is_some_and(|first| same_point(first, last))
    {
        visit(last);
    }
}

fn ring_left_area(operand: Operand<'_>, segment: Segment) -> Result<bool> {
    let start = [
        ExactDyadic::from_f64(segment.start.x),
        ExactDyadic::from_f64(segment.start.y),
        ExactDyadic::one(),
    ];
    let end = [
        ExactDyadic::from_f64(segment.end.x),
        ExactDyadic::from_f64(segment.end.y),
        ExactDyadic::one(),
    ];
    let midpoint = homogeneous_midpoint(&start, &end)?;
    let direction = segment_left_normal(segment)?;
    match operand.occupied_toward_exact(&midpoint, &direction)? {
        OccupiedToward::Yes => Ok(true),
        OccupiedToward::No => Ok(false),
        OccupiedToward::Tangent => Err(GeometryErrorKind::Invalid(
            "a clean ring side probe is tangent to source support".into(),
        )
        .into()),
    }
}

fn initialize_clean_ring_sides(
    operands: [Operand<'_>; 2],
    sources: &StagedExactSources,
    components: &mut [Component],
) -> Result<()> {
    for component in components {
        if component.dirty
            || component.source.ring_kind != Some(RingKind::Winding)
            || component.left_area.is_some()
        {
            continue;
        }
        let segment = component
            .source
            .segments
            .clone()
            .filter_map(|index| sources.segments().get(index).copied())
            .find(|segment| !same_point(segment.start, segment.end))
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("a clean winding ring has no nonzero segment".into())
            })?;
        let operand =
            usize::try_from(component.source.operand).map_err(|_| ResourceError::SizeOverflow)?;
        component.left_area = Some(ring_left_area(operands[operand], segment)?);
    }
    Ok(())
}

fn segment_left_normal(segment: Segment) -> Result<[ExactDyadic; 2]> {
    segment_direction(segment).map(|direction| segment_left_normal_from_direction(&direction))
}

fn segment_direction(segment: Segment) -> Result<[ExactDyadic; 2]> {
    Ok([
        ExactDyadic::from_f64(segment.end.x)
            .try_subtract(ExactDyadic::from_f64(segment.start.x))?,
        ExactDyadic::from_f64(segment.end.y)
            .try_subtract(ExactDyadic::from_f64(segment.start.y))?,
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geometry::ExpansionBudget;
    use crate::geometry::overlay::ExactSourceInventory;

    fn partition_oracle(left: &Shape, right: &Shape) -> Result<De9im> {
        let rows = [ShapeRow::Shape(left), ShapeRow::Shape(right)];
        let inventory = ExactSourceInventory::from_rows_with_occurrences(rows.into_iter())?;
        let mut budget = ExpansionBudget::new("relate", "test exact partition oracle");
        let partition = inventory.partition(&mut budget)?;
        inventory.relate(&partition)
    }

    fn assert_locator_oracle_parity(name: &str, left: &Shape, right: &Shape) {
        let rows = [ShapeRow::Shape(left), ShapeRow::Shape(right)];
        let inventory = ExactSourceInventory::from_rows_with_occurrences(rows.into_iter()).unwrap();
        let mut budget = ExpansionBudget::new("relate", "test exact partition locator oracle");
        let partition = inventory.partition(&mut budget).unwrap();
        let (nodes, spans) = inventory.relation_location_samples(&partition).unwrap();
        let left_plan = PreparedPlan::transient(left);
        let right_plan = PreparedPlan::transient(right);

        for plan in [&left_plan, &right_plan] {
            for _ in 0..4 {
                plan.locate_stored(Point::new_raw_xy(0.0, 0.0)).unwrap();
            }
        }

        let directions = [
            [ExactDyadic::from_f64(1.0), ExactDyadic::from_f64(0.0)],
            [ExactDyadic::from_f64(-1.0), ExactDyadic::from_f64(0.0)],
            [ExactDyadic::from_f64(0.0), ExactDyadic::from_f64(1.0)],
            [ExactDyadic::from_f64(0.0), ExactDyadic::from_f64(-1.0)],
            [ExactDyadic::from_f64(1.0), ExactDyadic::from_f64(1.0)],
            [ExactDyadic::from_f64(1.0), ExactDyadic::from_f64(-1.0)],
            [ExactDyadic::from_f64(-1.0), ExactDyadic::from_f64(1.0)],
            [ExactDyadic::from_f64(-1.0), ExactDyadic::from_f64(-1.0)],
        ];
        for (kind, samples) in [("node", &nodes), ("span", &spans)] {
            for (sample_index, (point, locations)) in samples.iter().enumerate() {
                let [left_location, right_location] = *locations;
                for (operand, plan, expected) in [
                    (left, &left_plan, left_location),
                    (right, &right_plan, right_location),
                ] {
                    let row = ShapeRow::Shape(operand);
                    let streamed = cartesian_row_exact_point_location_and_area(row, point).unwrap();
                    assert_eq!(
                        streamed, expected,
                        "{name} {kind} {sample_index}: streaming point location"
                    );
                    let indexed = plan.locate_exact(point).unwrap();
                    assert_eq!(
                        indexed, expected,
                        "{name} {kind} {sample_index}: prepared point location"
                    );
                    for direction in &directions {
                        let streamed =
                            cartesian_row_exact_offset_location_and_area(row, point, direction)
                                .unwrap();
                        let indexed = plan.locate_exact_offset(point, direction).unwrap();
                        assert_eq!(
                            indexed, streamed,
                            "{name} {kind} {sample_index}: indexed exact offset"
                        );
                        assert_eq!(
                            occupied_toward(indexed),
                            occupied_toward(streamed),
                            "{name} {kind} {sample_index}: occupied-toward result"
                        );
                    }
                }
            }
        }
    }

    fn clean_ring_atom_labels(
        left: &Shape,
        right: &Shape,
        ring_operand: usize,
        representative: XY,
    ) -> Result<(Loc, Loc)> {
        let [left_row, right_row] = [ShapeRow::Shape(left), ShapeRow::Shape(right)];
        let (ring_row, other_row) = if ring_operand == 0 {
            (left_row, right_row)
        } else {
            (right_row, left_row)
        };
        let rows = [left_row, right_row];
        let sources = StagedExactSources::from_rows_for_relate(rows.into_iter())?;
        let mut staged = stage_components(sources, rows)?;
        let segments = staged.sources.segments();
        let ring_owner = Operand::from_row(ring_row);
        let other_owner = Operand::from_row(other_row);
        for component in &mut staged.components {
            if component.source.operand as usize != ring_operand
                || component.source.ring_kind != Some(RingKind::Winding)
            {
                continue;
            }
            let Some(segment) = segments.get(component.source.segments.start).copied() else {
                continue;
            };
            if PointKey::new(segment.start) != PointKey::new(representative) {
                continue;
            }
            let point = Point::new_raw_xy(representative.x, representative.y);
            component.seed = Some(ring_owner.locate_stored(point)?);
            let exact = stored_exact_point(segment.start);
            let direction = segment_direction(segment)?;
            let own = atom_owner_label(ring_owner, component, &exact, &direction)?;
            let other_location = other_owner.locate_exact_offset(&exact, &direction)?;
            let other = component_edge_atom_location(component, other_location);
            return Ok((own, other));
        }
        Err(GeometryErrorKind::Invalid(
            "test geometry has no clean winding-ring atom at its representative".into(),
        )
        .into())
    }

    fn assert_full_oracle_and_controls(name: &str, left: &Shape, right: &Shape, oracle: De9im) {
        assert_direction_against_oracle(name, "forward", left, right, oracle);
        assert_direction_against_oracle(name, "reverse", right, left, oracle.transpose());
    }

    fn assert_direction_against_oracle(
        name: &str,
        direction: &str,
        left: &Shape,
        right: &Shape,
        oracle: De9im,
    ) {
        let left_plan = PreparedPlan::transient(left);
        let right_plan = PreparedPlan::transient(right);
        let pair = [
            Operand::from_plan(&left_plan),
            Operand::from_plan(&right_plan),
        ];
        assert_eq!(
            relate(&left_plan, &right_plan).unwrap(),
            oracle,
            "{name} {direction}: production matrix"
        );
        assert_eq!(
            evaluate_mode::<MatrixQuery, false>(pair, MatrixQuery).unwrap(),
            oracle,
            "{name} {direction}: saturation-off matrix"
        );
        assert_eq!(
            evaluate_mode::<EndOnly<MatrixQuery>, false>(pair, EndOnly(MatrixQuery)).unwrap(),
            oracle,
            "{name} {direction}: end-only matrix"
        );
        assert_eq!(
            evaluate_mode::<MatrixQuery, false>(
                [
                    Operand::from_row(ShapeRow::Shape(left)),
                    Operand::from_row(ShapeRow::Shape(right)),
                ],
                MatrixQuery,
            )
            .unwrap(),
            oracle,
            "{name} {direction}: borrowed ShapeRow matrix"
        );
        for predicate in Predicate::ALL {
            let expected = matrix_predicates::decided(predicate, oracle);
            assert_eq!(
                self::predicate(&left_plan, &right_plan, predicate).unwrap(),
                expected,
                "{name} {direction}: {predicate:?}: production query"
            );
            assert_eq!(
                evaluate_mode::<PredicateQuery, false>(pair, PredicateQuery(predicate)).unwrap(),
                expected,
                "{name} {direction}: {predicate:?}: saturation-off query"
            );
            assert_eq!(
                evaluate_mode::<EndOnly<PredicateQuery>, false>(
                    pair,
                    EndOnly(PredicateQuery(predicate)),
                )
                .unwrap(),
                expected,
                "{name} {direction}: {predicate:?}: end-only query"
            );
        }
        let mut patterns = vec![
            CompiledPattern::new(&oracle.text()).expect("matrix string is a pattern"),
            CompiledPattern::new("*********").expect("wildcard pattern is valid"),
        ];
        for cell in 0..9 {
            for literal in [b'F', b'T', b'0', b'1', b'2'] {
                let mut bytes = *b"*********";
                *bytes.get_mut(cell).expect("DE-9IM has nine cells") = literal;
                patterns.push(
                    CompiledPattern::new(std::str::from_utf8(&bytes).expect("ASCII pattern"))
                        .expect("single-cell pattern is valid"),
                );
            }
        }
        for pattern in [
            "T*T***T**",
            "T*****FF*",
            "F**F*****",
            "T*F**F***",
            "**T***T**",
            "FF*FF****",
            "F**0**F**",
            "1********",
            "2********",
            "********0",
            "****1****",
        ] {
            patterns.push(CompiledPattern::new(pattern).expect("fixed pattern is valid"));
        }
        for pattern in patterns {
            let expected = oracle.matches(pattern);
            assert_eq!(
                self::pattern(&left_plan, &right_plan, pattern).unwrap(),
                expected,
                "{name} {direction}: {pattern:?}: production query"
            );
            assert_eq!(
                evaluate_mode::<PatternQuery, false>(pair, PatternQuery(pattern)).unwrap(),
                expected,
                "{name} {direction}: {pattern:?}: saturation-off query"
            );
            assert_eq!(
                evaluate_mode::<EndOnly<PatternQuery>, false>(
                    pair,
                    EndOnly(PatternQuery(pattern)),
                )
                .unwrap(),
                expected,
                "{name} {direction}: {pattern:?}: end-only query"
            );
        }
    }

    #[test]
    fn crossing_line_matrix_matches_partition_in_all_evaluator_modes() {
        let left = crate::io::parse_wkt("LINESTRING (-1 0,1 0)").unwrap();
        let right = crate::io::parse_wkt("LINESTRING (0 -1,0 1)").unwrap();
        let oracle = partition_oracle(&left, &right).unwrap();
        assert_eq!(oracle.text(), "0F1FF0102");

        let left_plan = PreparedPlan::transient(&left);
        let right_plan = PreparedPlan::transient(&right);
        let pair = [
            Operand::from_plan(&left_plan),
            Operand::from_plan(&right_plan),
        ];
        assert_eq!(relate(&left_plan, &right_plan).unwrap(), oracle);
        assert_eq!(
            evaluate_mode::<MatrixQuery, false>(pair, MatrixQuery).unwrap(),
            oracle
        );
        assert_eq!(
            evaluate_mode::<EndOnly<MatrixQuery>, false>(pair, EndOnly(MatrixQuery)).unwrap(),
            oracle
        );
        assert_eq!(
            evaluate_mode::<MatrixQuery, false>(
                [
                    Operand::from_row(ShapeRow::Shape(&left)),
                    Operand::from_row(ShapeRow::Shape(&right)),
                ],
                MatrixQuery,
            )
            .unwrap(),
            oracle
        );

        for predicate in Predicate::ALL {
            let expected = matrix_predicates::decided(predicate, oracle);
            assert_eq!(
                self::predicate(&left_plan, &right_plan, predicate).unwrap(),
                expected,
                "{predicate:?}"
            );
        }
        for pattern in ["0********", "F*1***0**", "*********", "T*****FF*"] {
            let pattern = CompiledPattern::new(pattern).expect("test pattern is valid");
            assert_eq!(
                self::pattern(&left_plan, &right_plan, pattern).unwrap(),
                oracle.matches(pattern),
                "{pattern:?}"
            );
        }
    }

    fn run_curated_relation_corpus(locator_only: bool) {
        let cases = [
            ("empty polygons", "POLYGON EMPTY", "POLYGON EMPTY"),
            (
                "empty collection and line",
                "GEOMETRYCOLLECTION EMPTY",
                "LINESTRING (0 0,1 1)",
            ),
            (
                "empty line and polygon",
                "LINESTRING EMPTY",
                "POLYGON ((0 0,2 0,2 2,0 2,0 0))",
            ),
            (
                "equal points with different ZM",
                "POINT ZM (1 2 3 4)",
                "POINT ZM (1 2 30 40)",
            ),
            (
                "point sets partly equal",
                "MULTIPOINT ((0 0),(1 1),(2 2))",
                "MULTIPOINT ((1 1),(3 3))",
            ),
            (
                "point in area",
                "POINT (1 1)",
                "POLYGON ((0 0,3 0,3 3,0 3,0 0))",
            ),
            (
                "point on area edge",
                "POINT (1 0)",
                "POLYGON ((0 0,3 0,3 3,0 3,0 0))",
            ),
            (
                "point in hole",
                "POINT (5 5)",
                "POLYGON ((0 0,10 0,10 10,0 10,0 0),(3 3,7 3,7 7,3 7,3 3))",
            ),
            (
                "crossing lines, non-dyadic node",
                "LINESTRING (0 0,3 3)",
                "LINESTRING (0 1,2 0)",
            ),
            (
                "vertex-through crossing",
                "LINESTRING (-1 0,0 0,1 0)",
                "LINESTRING (0 -1,0 1)",
            ),
            (
                "collinear partial overlap",
                "LINESTRING (0 0,4 0)",
                "LINESTRING (2 0,6 0)",
            ),
            (
                "collinear reversed overlap",
                "LINESTRING (0 0,4 0)",
                "LINESTRING (6 0,2 0)",
            ),
            (
                "collinear disjoint",
                "LINESTRING (0 0,1 0)",
                "LINESTRING (2 0,3 0)",
            ),
            (
                "closed line",
                "LINESTRING (0 0,4 0,4 4,0 0)",
                "LINESTRING (0 0,4 4,4 0,0 0)",
            ),
            ("collapsed line", "LINESTRING (7 7,7 7)", "POINT (7 7)"),
            (
                "line endpoint parity",
                "MULTILINESTRING ((0 0,1 0),(1 0,2 0),(2 0,0 0))",
                "POINT (1 0)",
            ),
            (
                "nested polygons",
                "POLYGON ((0 0,10 0,10 10,0 10,0 0))",
                "POLYGON ((2 2,4 2,4 4,2 4,2 2))",
            ),
            (
                "polygon overlap",
                "POLYGON ((0 0,4 0,4 4,0 4,0 0))",
                "POLYGON ((2 -1,5 -1,5 2,2 2,2 -1))",
            ),
            (
                "shared polygon edge",
                "POLYGON ((0 0,2 0,2 2,0 2,0 0))",
                "POLYGON ((2 0,4 0,4 2,2 2,2 0))",
            ),
            (
                "vertex-touching polygons",
                "POLYGON ((0 0,2 0,2 2,0 2,0 0))",
                "POLYGON ((2 2,4 2,4 4,2 4,2 2))",
            ),
            (
                "disjoint polygons",
                "POLYGON ((0 0,1 0,1 1,0 1,0 0))",
                "POLYGON ((3 3,4 3,4 4,3 4,3 3))",
            ),
            (
                "line crosses polygon",
                "LINESTRING (-1 1,4 1)",
                "POLYGON ((0 0,3 0,3 3,0 3,0 0))",
            ),
            (
                "line inside polygon",
                "LINESTRING (1 1,2 2)",
                "POLYGON ((0 0,3 0,3 3,0 3,0 0))",
            ),
            (
                "line on polygon edge",
                "LINESTRING (0 0,3 0)",
                "POLYGON ((0 0,3 0,3 3,0 3,0 0))",
            ),
            (
                "line through polygon hole",
                "LINESTRING (-1 5,11 5)",
                "POLYGON ((0 0,10 0,10 10,0 10,0 0),(3 3,7 3,7 7,3 7,3 3))",
            ),
            (
                "hole exterior point",
                "POINT (11 11)",
                "POLYGON ((0 0,10 0,10 10,0 10,0 0),(11 11,13 11,13 13,11 13,11 11))",
            ),
            (
                "hole touches shell",
                "POLYGON ((0 0,10 0,10 10,0 10,0 0),(0 5,2 4,2 6,0 5))",
                "LINESTRING (-1 5,3 5)",
            ),
            (
                "bow-tie ring",
                "POLYGON ((0 0,4 4,0 4,4 0,0 0))",
                "POLYGON ((1 1,3 1,3 3,1 3,1 1))",
            ),
            (
                "spiked ring",
                "POLYGON ((0 0,4 0,4 4,2 2,4 4,0 4,0 0))",
                "POLYGON ((1 1,3 1,3 3,1 3,1 1))",
            ),
            (
                "collapsed polygon ring",
                "POLYGON ((0 0,2 0,0 0,2 0,0 0))",
                "LINESTRING (0 0,2 0)",
            ),
            (
                "mixed collection",
                "GEOMETRYCOLLECTION (POINT (1 1),LINESTRING (0 0,3 0),POLYGON ((0 0,2 0,2 2,0 2,0 0)))",
                "LINESTRING (1 0,3 0)",
            ),
            (
                "repeated members",
                "GEOMETRYCOLLECTION (LINESTRING (0 0,2 0),LINESTRING (2 0,0 0),POINT (1 0),POINT (1 0))",
                "LINESTRING (1 -1,1 1)",
            ),
            (
                "overlapping multipolygon members",
                "MULTIPOLYGON (((0 0,3 0,3 3,0 3,0 0)),((2 0,5 0,5 3,2 3,2 0)))",
                "POLYGON ((1 1,4 1,4 2,1 2,1 1))",
            ),
            (
                "three concurrent segments",
                "MULTILINESTRING ((-2 0,2 0),(0 -2,0 2),(-2 -2,2 2))",
                "LINESTRING (-2 2,2 -2)",
            ),
            (
                "shell equals hole",
                "POLYGON ((0 0,4 0,4 4,0 4,0 0),(0 0,4 0,4 4,0 4,0 0))",
                "POLYGON ((1 1,2 1,2 2,1 2,1 1))",
            ),
            (
                "hole encloses shell",
                "POLYGON ((2 2,4 2,4 4,2 4,2 2),(0 0,10 0,10 10,0 10,0 0))",
                "POINT (3 3)",
            ),
            (
                "large coordinates",
                "LINESTRING (1e150 1e150,3e150 3e150)",
                "LINESTRING (1e150 2e150,2e150 1e150)",
            ),
            (
                "tiny coordinates",
                "POLYGON ((1e-150 1e-150,3e-150 1e-150,3e-150 3e-150,1e-150 3e-150,1e-150 1e-150))",
                "POLYGON ((2e-150 0.5e-150,4e-150 0.5e-150,4e-150 2e-150,2e-150 2e-150,2e-150 0.5e-150))",
            ),
            (
                "one-ulp-offset lines",
                "LINESTRING (0 0,1 1)",
                "LINESTRING (0 5e-324,1 1.0000000000000002)",
            ),
            (
                "duplicate source segment",
                "GEOMETRYCOLLECTION (LINESTRING (0 0,2 0),LINESTRING (2 0,0 0))",
                "LINESTRING (1 -1,1 1)",
            ),
        ];
        for (name, left_wkt, right_wkt) in cases {
            let left = crate::io::parse_wkt(left_wkt)
                .unwrap_or_else(|error| panic!("{name}: left WKT did not parse: {error}"));
            let right = crate::io::parse_wkt(right_wkt)
                .unwrap_or_else(|error| panic!("{name}: right WKT did not parse: {error}"));
            if locator_only {
                assert_locator_oracle_parity(name, &left, &right);
            } else {
                let oracle = partition_oracle(&left, &right)
                    .unwrap_or_else(|error| panic!("{name}: exact partition failed: {error}"));
                assert_full_oracle_and_controls(name, &left, &right, oracle);
            }
        }
    }

    #[test]
    fn curated_relation_corpus_matches_partition_in_both_orders() {
        run_curated_relation_corpus(false);
    }

    #[test]
    fn curated_locator_queries_match_partition_cell_facts() {
        run_curated_relation_corpus(true);
    }

    #[test]
    fn named_counter_cases_match_exact_matrices_and_predicates() {
        let cases = [
            (
                "collapsed polygon overlaps line",
                "POLYGON ((0 0,2 2,4 4,0 0))",
                "LINESTRING (1 1,5 5)",
                Some("1010F0102"),
                Some((Predicate::Overlaps, true)),
            ),
            (
                "collapsed polygon crosses polygon",
                "POLYGON ((0 0,2 2,4 4,0 0))",
                "POLYGON ((1 3,3 1,3 3,1 3))",
                Some("101FF0212"),
                Some((Predicate::Crosses, true)),
            ),
            (
                "collapsed line meets a nonempty line",
                "LINESTRING (0 0,0 0)",
                "LINESTRING (-1 0,1 0)",
                Some("0FFFFF102"),
                Some((Predicate::Intersects, true)),
            ),
            (
                "external hole equals itself",
                "POLYGON ((0 0,2 0,2 2,0 2,0 0),(5 5,6 5,6 6,5 6,5 5))",
                "POLYGON ((0 0,2 0,2 2,0 2,0 0),(5 5,6 5,6 6,5 6,5 5))",
                Some("2FFF1FFF2"),
                Some((Predicate::Equals, true)),
            ),
            (
                "empty equals shell-equals-hole",
                "POLYGON EMPTY",
                "POLYGON ((0 0,4 0,4 4,0 4,0 0),(0 0,4 0,4 4,0 4,0 0))",
                Some("FFFFFFFF2"),
                Some((Predicate::Equals, true)),
            ),
            (
                "shell-equals-hole against square",
                "POLYGON ((0 0,4 0,4 4,0 4,0 0),(0 0,4 0,4 4,0 4,0 0))",
                "POLYGON ((-1 -1,5 -1,5 5,-1 5,-1 -1))",
                Some("FFFFFF212"),
                Some((Predicate::Equals, false)),
            ),
            (
                "spike equals itself",
                "POLYGON ((0 0,4 0,4 4,2 2,4 4,0 4,0 0))",
                "POLYGON ((0 0,4 0,4 4,2 2,4 4,0 4,0 0))",
                Some("2FFF1FFF2"),
                Some((Predicate::Equals, true)),
            ),
            (
                "point inside hole",
                "POINT (5 5)",
                "POLYGON ((0 0,10 0,10 10,0 10,0 0),(3 3,7 3,7 7,3 7,3 3))",
                None,
                None,
            ),
            (
                "point on hole edge",
                "POINT (3 5)",
                "POLYGON ((0 0,10 0,10 10,0 10,0 0),(3 3,7 3,7 7,3 7,3 3))",
                None,
                None,
            ),
            (
                "point at touching-hole vertex",
                "POINT (4 4)",
                "POLYGON ((0 0,10 0,10 10,0 10,0 0),(2 2,4 2,4 4,2 4,2 2),(4 4,6 4,6 6,4 6,4 4))",
                None,
                None,
            ),
            (
                "monotone lines meet end to start",
                "LINESTRING (0 0,1 0,2 0,3 0,4 0)",
                "LINESTRING (4 0,5 0,6 0,7 0,8 0)",
                None,
                None,
            ),
            (
                "stored vertex and constructed crossing share one node",
                "GEOMETRYCOLLECTION (LINESTRING (-1 0,0 0,1 0),LINESTRING (-1 -1,1 1))",
                "LINESTRING (0 -1,0 1)",
                None,
                None,
            ),
            (
                "two holes touch at their first vertex",
                "POLYGON ((0 0,10 0,10 10,0 10,0 0),(2 2,4 2,4 4,2 4,2 2),(4 4,6 4,6 6,4 6,4 4))",
                "LINESTRING (3 3,5 5)",
                None,
                None,
            ),
            (
                "line interior in a mixed collection",
                "GEOMETRYCOLLECTION (POLYGON ((0 0,2 0,2 2,0 2,0 0)),LINESTRING (3 3,5 5))",
                "POINT (4 4)",
                None,
                None,
            ),
        ];
        for (name, left_wkt, right_wkt, expected_matrix, expected_predicate) in cases {
            let left = crate::io::parse_wkt(left_wkt)
                .unwrap_or_else(|error| panic!("{name}: left WKT did not parse: {error}"));
            let right = crate::io::parse_wkt(right_wkt)
                .unwrap_or_else(|error| panic!("{name}: right WKT did not parse: {error}"));
            let oracle = partition_oracle(&left, &right)
                .unwrap_or_else(|error| panic!("{name}: exact partition failed: {error}"));
            if let Some(expected) = expected_matrix {
                assert_eq!(oracle.text(), expected, "{name}: oracle matrix");
            }
            assert_full_oracle_and_controls(name, &left, &right, oracle);
            assert_locator_oracle_parity(name, &left, &right);
            if let Some((predicate, expected)) = expected_predicate {
                let left_plan = PreparedPlan::transient(&left);
                let right_plan = PreparedPlan::transient(&right);
                assert_eq!(
                    self::predicate(&left_plan, &right_plan, predicate).unwrap(),
                    expected,
                    "{name}: {predicate:?}"
                );
            }
        }

        let mixed = crate::io::parse_wkt(
            "GEOMETRYCOLLECTION (POLYGON ((0 0,2 0,2 2,0 2,0 0)),LINESTRING (3 3,5 5))",
        )
        .unwrap();
        let line_location = PreparedPlan::transient(&mixed)
            .locate_stored(Point::new_raw_xy(4.0, 4.0))
            .unwrap();
        assert_eq!(line_location.location, Loc::Interior);
        assert!(!line_location.area);
    }

    #[test]
    fn large_polygon_locator_matches_partition_cell_facts() {
        let mut points = (0..256)
            .map(|index| {
                let angle = std::f64::consts::TAU * index as f64 / 256.0;
                Point::new_raw_xy(10.0 * angle.cos(), 10.0 * angle.sin())
            })
            .collect::<Vec<_>>();
        points.push(*points.first().expect("generated polygon has a vertex"));
        let shell = crate::geometry::Ring::from_closed(
            crate::geometry::CoordSeq::try_from_points(&points).unwrap(),
        )
        .unwrap();
        let polygon = Shape::Polygon(crate::geometry::Polygon::new(shell, Vec::new()));
        let point = Shape::Point(Point::new_raw_xy(0.0, 0.0));
        assert_locator_oracle_parity("256-vertex polygon", &polygon, &point);
    }

    #[test]
    fn indexed_point_only_phase_matches_hole_boundary_oracle() {
        let polygon = crate::io::parse_wkt(
            "POLYGON ((0 0,10 0,10 10,0 10,0 0),\
             (2 2,4 2,4 4,2 4,2 2),(4 4,6 4,6 6,4 6,4 4))",
        )
        .unwrap();
        let polygon_plan = PreparedPlan::transient(&polygon);
        for (x, y) in [(3.0, 3.0), (2.0, 3.0), (4.0, 4.0), (0.0, 0.0), (3.0, 3.0)] {
            let point = Shape::Point(Point::new_raw_xy(x, y));
            let point_plan = PreparedPlan::transient(&point);
            let oracle = partition_oracle(&point, &polygon).unwrap();
            assert_eq!(
                relate(&point_plan, &polygon_plan).unwrap(),
                oracle,
                "POINT ({x} {y}): indexed P1 matrix"
            );
            assert_eq!(
                self::predicate(&point_plan, &polygon_plan, Predicate::Intersects).unwrap(),
                matrix_predicates::decided(Predicate::Intersects, oracle),
                "POINT ({x} {y}): indexed P1 intersects"
            );
        }
    }

    #[test]
    fn corrected_clean_ring_atom_labels_keep_full_oracle_matrices() {
        let line = crate::io::parse_wkt("LINESTRING (10 10,12 10)").unwrap();
        assert!(matches!(&line, Shape::LineString(_)));
        let external_hole = "POLYGON ((0 0,2 0,2 2,0 2,0 0),(10 10,12 10,12 12,10 12,10 10))";
        let cases = [
            ("POINT (10 10)", "F02FF1102"),
            ("POINT (11 10)", "0F2FF1102"),
            ("MULTIPOINT ((10 10),(11 10))", "002FF1102"),
            ("MULTIPOINT ((10 10),(12 10),(12 12),(10 12))", "F02FF11F2"),
        ];
        for (point_wkt, expected_matrix) in cases {
            let external =
                crate::io::parse_wkt(&format!("GEOMETRYCOLLECTION ({external_hole},{point_wkt})"))
                    .unwrap();
            let oracle = partition_oracle(&external, &line).unwrap();
            assert_eq!(oracle.text(), expected_matrix);
            assert_full_oracle_and_controls(point_wkt, &external, &line, oracle);
            assert_locator_oracle_parity(point_wkt, &external, &line);

            let node = PreparedPlan::transient(&external)
                .locate_stored(Point::new_raw_xy(10.0, 10.0))
                .unwrap();
            match point_wkt {
                "POINT (10 10)" => {
                    assert_eq!(
                        node.location,
                        Loc::Interior,
                        "first vertex keeps point support"
                    );
                    assert!(!node.area);
                },
                "POINT (11 10)" => {
                    assert_eq!(node.location, Loc::Exterior);
                    let midpoint = PreparedPlan::transient(&external)
                        .locate_stored(Point::new_raw_xy(11.0, 10.0))
                        .unwrap();
                    assert_eq!(midpoint.location, Loc::Interior);
                    assert!(!midpoint.area);
                },
                "MULTIPOINT ((10 10),(11 10))" => {
                    assert_eq!(node.location, Loc::Interior);
                    assert!(!node.area);
                    let midpoint = PreparedPlan::transient(&external)
                        .locate_stored(Point::new_raw_xy(11.0, 10.0))
                        .unwrap();
                    assert_eq!(midpoint.location, Loc::Interior);
                    assert!(!midpoint.area);
                },
                _ => {
                    for (x, y) in [(10.0, 10.0), (12.0, 10.0), (12.0, 12.0), (10.0, 12.0)] {
                        let node = PreparedPlan::transient(&external)
                            .locate_stored(Point::new_raw_xy(x, y))
                            .unwrap();
                        assert_eq!(node.location, Loc::Interior);
                        assert!(!node.area);
                    }
                },
            }
        }

        let filled = crate::io::parse_wkt(
            "GEOMETRYCOLLECTION (\
             POLYGON ((0 0,20 0,20 20,0 20,0 0)),POINT (5 5))",
        )
        .unwrap();
        let inner_ring = crate::io::parse_wkt("POLYGON ((5 5,10 5,10 10,5 10,5 5))").unwrap();
        let oracle = partition_oracle(&filled, &inner_ring).unwrap();
        assert_eq!(oracle.text(), "212FF1FF2");
        assert_full_oracle_and_controls("ring inside area", &filled, &inner_ring, oracle);
        assert_locator_oracle_parity("ring inside area", &filled, &inner_ring);

        let node = PreparedPlan::transient(&filled)
            .locate_stored(Point::new_raw_xy(5.0, 5.0))
            .unwrap();
        assert_eq!(node.location, Loc::Interior);
        assert!(node.area);
    }

    #[test]
    fn clean_ring_atom_labels_use_only_areal_support() {
        let line = crate::io::parse_wkt("LINESTRING (10 10,12 10)").unwrap();
        let external_hole = "POLYGON ((0 0,2 0,2 2,0 2,0 0),(10 10,12 10,12 12,10 12,10 10))";
        for point_wkt in [
            "POINT (10 10)",
            "POINT (11 10)",
            "MULTIPOINT ((10 10),(11 10))",
            "MULTIPOINT ((10 10),(12 10),(12 12),(10 12))",
        ] {
            let external =
                crate::io::parse_wkt(&format!("GEOMETRYCOLLECTION ({external_hole},{point_wkt})"))
                    .unwrap();
            let (edge, _) =
                clean_ring_atom_labels(&external, &line, 0, XY::new(10.0, 10.0)).unwrap();
            assert_eq!(edge, Loc::Exterior, "{point_wkt}: external-hole edge atom");
        }

        let filled = crate::io::parse_wkt(
            "GEOMETRYCOLLECTION (\
             POLYGON ((0 0,20 0,20 20,0 20,0 0)),POINT (5 5))",
        )
        .unwrap();
        let inner_ring = crate::io::parse_wkt("POLYGON ((5 5,10 5,10 10,5 10,5 5))").unwrap();
        let (_, filled_edge) =
            clean_ring_atom_labels(&filled, &inner_ring, 1, XY::new(5.0, 5.0)).unwrap();
        assert_eq!(filled_edge, Loc::Interior);
    }
}
