use std::ops::ControlFlow;

use ahash::HashSetExt as _;

use crate::error::Result;
use crate::geometry::predicates::{
    LineworkChains, MinimumClearanceWitness, collect_duplicate_points, collect_offending_pair,
    line_is_ccw, line_is_closed, line_is_closed_in_domain, line_is_simple, line_repair,
    minimum_clearance_witness, minimum_clearance_witness_with_support, multi_polygon_members_issue,
    multiline_is_simple, multiline_repair, polygonal_repair, shell_is_convex, validate_line,
    validate_point, validate_points,
};
use crate::geometry::{
    CoordinateAxes, Coordinates as _, GeometryErrorKind, GeometryKind, HashSet, LineSeq, Point,
    PointKey, RefinedChainProvenance, RepairMethod, Shape, SharedShapeOwners, ValidationIssue,
    emit_from_original, unique_xy_points,
};

impl Shape {
    pub fn is_empty(&self) -> bool {
        !shape_has_any_point(self, &mut SharedShapeOwners::for_collections())
    }

    pub fn is_closed(&self) -> bool {
        shape_is_closed_in_domain(self, crate::geometry::TopologyDomain::Cartesian)
    }

    pub fn is_ring(&self) -> crate::error::Result<bool> {
        match self {
            Self::LineString(points) => Ok(line_is_closed(points) && line_is_simple(points)?),
            Self::Point(_)
            | Self::MultiPoint(_)
            | Self::MultiLineString(_)
            | Self::Polygon(_)
            | Self::MultiPolygon(_)
            | Self::GeometryCollection(_)
            | Self::Empty(..) => Ok(false),
        }
    }

    /// Whether the coordinate-space geometry satisfies OGC simplicity.
    ///
    /// `Shape` carries no CRS. Geographic simplicity is supplied by the
    /// frame-aware geometry entry point, which uses quotient validity where
    /// the stored coordinates do not describe one faithful planar chart.
    pub fn is_simple(&self) -> crate::error::Result<bool> {
        match self {
            // Collections are never simple by convention, including the typed
            // empty (matching the empty container form below).
            Self::Empty(GeometryKind::GeometryCollection, _) | Self::GeometryCollection(_) => {
                Ok(false)
            },
            Self::Point(_) | Self::Empty(..) => Ok(true),
            Self::MultiPoint(points) => Ok(unique_xy_points(points)?.len() == points.coord_count()),
            Self::LineString(points) => line_is_simple(points),
            Self::MultiLineString(lines) => multiline_is_simple(lines),
            Self::Polygon(_) | Self::MultiPolygon(_) => Ok(self.validate()?.is_none()),
        }
    }

    /// The distinct XY locations where the geometry coincides with itself:
    /// proper linework self-crossings, non-adjacent touches, the endpoints
    /// of collinear overlaps (spikes and backtracks), and duplicate
    /// point-atom coordinates. Legal adjacent shared vertices, ring closures,
    /// and removable REPEATED CONSECUTIVE vertices (zero-length stutter) are
    /// never reported, so for point/lineal input the result is non-empty
    /// exactly when
    /// [`is_simple`](Self::is_simple) is `false`. Areal input diagnoses
    /// its rings' linework (polygon simplicity itself is validity-based);
    /// collections are diagnosed recursively (their `is_simple` is always
    /// `false` by convention, so no invariant holds there). Output points
    /// are XY only — one node can be created by segments carrying
    /// conflicting Z/M.
    pub fn self_intersections(&self) -> crate::error::Result<Vec<Point>> {
        let mut nodes = Vec::new();
        let mut seen = HashSet::new();
        let mut visit = |point: Point| {
            if seen.insert(PointKey::new(point)) {
                nodes.push(point);
            }
        };
        collect_duplicate_points(self, &mut visit);
        let mut chains = LineworkChains::default();
        chains.push_shape(self)?;
        let mut failure = Ok(());
        let _candidate_pairs = chains.for_each_candidate_pair(|left, right| {
            match collect_offending_pair(chains.at(left), chains.at(right), &mut visit) {
                Ok(()) => ControlFlow::Continue(()),
                Err(error) => {
                    failure = Err(error);
                    ControlFlow::Break(())
                },
            }
        })?;
        failure?;
        Ok(nodes)
    }

    pub fn crosses_antimeridian(&self) -> bool {
        crate::geometry::crosses_antimeridian_in_frame(
            self,
            crate::geographic::GeographicFrame::crs84(),
        )
    }
}

pub(crate) fn shape_is_closed_in_domain(
    shape: &Shape,
    domain: crate::geometry::TopologyDomain,
) -> bool {
    match shape {
        Shape::LineString(points) => line_is_closed_in_domain(points, domain),
        Shape::MultiLineString(lines) => {
            !lines.is_empty()
                && lines
                    .iter()
                    .all(|line| line_is_closed_in_domain(line, domain))
        },
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::Polygon(_)
        | Shape::MultiPolygon(_)
        | Shape::GeometryCollection(_)
        | Shape::Empty(..) => false,
    }
}

fn shape_has_any_point(shape: &Shape, owners: &mut SharedShapeOwners) -> bool {
    match shape {
        Shape::Point(_) => true,
        Shape::MultiPoint(points) => !points.is_empty(),
        Shape::LineString(points) => !points.is_empty(),
        Shape::MultiLineString(lines) => lines.iter().any(|line| !line.is_empty()),
        Shape::Polygon(polygon) => polygon.rings().any(|ring| !ring.is_empty()),
        Shape::MultiPolygon(polygons) => polygons
            .iter()
            .any(|polygon| polygon.rings().any(|ring| !ring.is_empty())),
        Shape::GeometryCollection(parts) => {
            owners.first_collection(parts.as_slice(), parts.members_are_shared())
                && parts.iter().any(|part| shape_has_any_point(part, owners))
        },
        Shape::Empty(..) => false,
    }
}

impl Shape {
    pub fn minimum_clearance(&self) -> Result<f64> {
        Ok(match minimum_clearance_witness(self)? {
            None => f64::INFINITY,
            Some(witness) => witness.planar_distance()?,
        })
    }

    pub(crate) fn minimum_clearance_plan_with_support(
        &self,
        support: &Self,
        refined_chains: &[RefinedChainProvenance],
    ) -> Result<Option<MinimumClearanceWitness>> {
        minimum_clearance_witness_with_support(self, support, refined_chains)
    }

    /// The two-point `LineString` realizing `minimum_clearance` —
    /// `LINESTRING EMPTY` when the clearance
    /// is infinite (fewer than two distinct vertices).
    pub fn minimum_clearance_line(&self) -> Result<Self> {
        match minimum_clearance_witness(self)? {
            None => Ok(Self::typed_empty(
                GeometryKind::LineString,
                CoordinateAxes::XY,
            )),
            Some(witness) => {
                let emitted = emit_from_original(&witness.points, &witness.plan)?;
                let [a, b] = emitted.as_slice() else {
                    unreachable!("minimum_clearance witness plan has two vertices");
                };
                Ok(Self::LineString(LineSeq::segment(*a, *b)?))
            },
        }
    }

    /// Return the first planar OGC validity issue for this coordinate-space shape.
    ///
    /// `Shape` carries no CRS. Geographic validation is frame-aware and reads
    /// exact quotient incidence when periodic aliases or seam topology matter.
    pub fn validate(&self) -> crate::error::Result<Option<ValidationIssue>> {
        self.validate_with_shared_owners(&mut SharedShapeOwners::for_collections())
    }

    fn validate_with_shared_owners(
        &self,
        owners: &mut SharedShapeOwners,
    ) -> crate::error::Result<Option<ValidationIssue>> {
        match self {
            Self::Point(point) => Ok(validate_point(*point, "$")),
            Self::MultiPoint(points) => Ok(validate_points(points, "$")),
            Self::LineString(points) => Ok(validate_line(points, "line string")),
            Self::MultiLineString(lines) => Ok(lines.iter().enumerate().find_map(|(idx, line)| {
                validate_line(line, "multi line string member")
                    .map(|issue| issue.with_path_prefix(&format!("$[{idx}]")))
            })),
            Self::Polygon(polygon) => polygon.validate("polygon", "$"),
            Self::MultiPolygon(polygons) => {
                for (idx, polygon) in polygons.iter().enumerate() {
                    if let Some(issue) = polygon.validate("multi polygon member", "$")? {
                        return Ok(Some(issue.with_path_prefix(&format!("$[{idx}]"))));
                    }
                }
                multi_polygon_members_issue(polygons, "$")
            },
            Self::GeometryCollection(geometries) => {
                if !owners.first_collection(geometries.as_slice(), geometries.members_are_shared())
                {
                    return Ok(None);
                }
                for (idx, geometry) in geometries.iter().enumerate() {
                    if let Some(issue) = geometry.validate_with_shared_owners(owners)? {
                        return Ok(Some(issue.with_path_prefix(&format!("$[{idx}]"))));
                    }
                }
                Ok(None)
            },
            Self::Empty(..) => Ok(None),
        }
    }

    /// Whether the polygon is convex: every shell turn has one orientation
    /// (collinear edges allowed), no holes. The empty polygon is convex.
    /// Non-polygon input is a type error — `False` would hide it.
    pub fn is_convex(&self) -> Result<bool> {
        match self {
            Self::Polygon(polygon) => {
                if !polygon.holes.is_empty() {
                    return Ok(false);
                }
                Ok(shell_is_convex(&polygon.shell))
            },
            Self::Empty(GeometryKind::Polygon, _) => Ok(true),
            Self::Point(_)
            | Self::MultiPoint(_)
            | Self::LineString(_)
            | Self::MultiLineString(_)
            | Self::MultiPolygon(_)
            | Self::GeometryCollection(_)
            | Self::Empty(..) => Err(GeometryErrorKind::SinglePolygonRequired.into()),
        }
    }

    /// Rebuild `self` into a valid geometry. PRECONDITION: the caller has
    /// already established invalidity (every call site gates on
    /// `validate()` — the Python layer on the handle's MEMOIZED verdict)
    /// so no validation is re-run here; valid input would simply be
    /// rebuilt.
    pub fn repair(&self, method: RepairMethod, drop: bool) -> Result<Self> {
        self.repair_with_context(
            method,
            drop,
            &crate::geometry::SourceArcContext::cartesian(),
        )
    }

    pub(crate) fn repair_with_context(
        &self,
        method: RepairMethod,
        drop: bool,
        context: &crate::geometry::SourceArcContext<'_, '_>,
    ) -> Result<Self> {
        self.repair_with_source_vertices(method, drop, false, context)
    }

    pub(crate) fn repair_preserving_source_vertices(
        &self,
        method: RepairMethod,
        drop: bool,
        context: &crate::geometry::SourceArcContext<'_, '_>,
    ) -> Result<Self> {
        self.repair_with_source_vertices(method, drop, true, context)
    }

    fn repair_with_source_vertices(
        &self,
        method: RepairMethod,
        drop: bool,
        preserve_source_vertices: bool,
        context: &crate::geometry::SourceArcContext<'_, '_>,
    ) -> Result<Self> {
        match self {
            // Empty shapes are always valid; an invalid (multi)point means
            // non-finite coordinates, which nothing can reconstruct.
            Self::Point(_) | Self::MultiPoint(_) | Self::Empty(..) => Err(
                GeometryErrorKind::repair_failed("non-finite coordinates cannot be repaired"),
            ),
            Self::LineString(line) => line_repair(line),
            Self::MultiLineString(lines) => multiline_repair(lines),
            Self::Polygon(polygon) => {
                let rebuilt = polygonal_repair(
                    std::slice::from_ref(polygon),
                    method,
                    preserve_source_vertices,
                    drop,
                    context,
                )?;
                Ok(rebuilt)
            },
            Self::MultiPolygon(polygons) => {
                polygonal_repair(polygons, method, preserve_source_vertices, drop, context)
            },
            Self::GeometryCollection(geometries) => Ok(Self::GeometryCollection(
                geometries.try_map_members(|geometry| {
                    // Per-member gate: valid members pass through untouched.
                    if geometry.validate()?.is_none() {
                        Ok(geometry.clone())
                    } else {
                        geometry.repair_with_source_vertices(
                            method,
                            drop,
                            preserve_source_vertices,
                            context,
                        )
                    }
                })?,
            )),
        }
    }

    /// Whether a closed linestring winds counter-clockwise
    /// (`false` for open lines and non-lineal geometry).
    pub fn is_ccw(&self) -> bool {
        match self {
            Self::LineString(points) => line_is_ccw(points),
            Self::Point(_)
            | Self::MultiPoint(_)
            | Self::MultiLineString(_)
            | Self::Polygon(_)
            | Self::MultiPolygon(_)
            | Self::GeometryCollection(_)
            | Self::Empty(..) => false,
        }
    }
}
