use std::ops::ControlFlow;

use ahash::HashSetExt as _;

use crate::error::Result;
use crate::geometry::predicates::{
    LineworkChains, MinimumClearanceWitness, collect_duplicate_points, collect_offending_pair,
    is_seam_cut_pair, line_is_ccw, line_is_closed, line_is_closed_in_domain, line_is_simple,
    line_repair, minimum_clearance_witness, multi_polygon_members_issue,
    multi_polygon_members_issue_with_seam_cuts, multiline_is_simple, multiline_repair,
    polygonal_repair, shell_is_convex, validate_line, validate_point, validate_points,
};
use crate::geometry::{
    Component, CoordinateAxes, Coordinates as _, GeometryErrorKind, GeometryKind, HashSet, LineSeq,
    Point, PointKey, RepairMethod, SeamCutEdgePairs, Shape, SharedShapeOwners, ValidationIssue,
    carry_ordinates, emit_from_original, point_distance, unique_xy_points,
};

fn advance_source_edge_base(source_edge_base: &mut u32, coordinate_count: usize) -> Result<()> {
    let edge_count = u32::try_from(coordinate_count.saturating_sub(1))
        .map_err(|_| crate::error::ResourceError::SizeOverflow)?;
    *source_edge_base = (*source_edge_base)
        .checked_add(edge_count)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    Ok(())
}

fn advance_polygon_source_edge_base(
    source_edge_base: &mut u32,
    polygon: &crate::geometry::Polygon,
) -> Result<()> {
    for ring in polygon.rings() {
        advance_source_edge_base(source_edge_base, ring.coord_count())?;
    }
    Ok(())
}

fn advance_source_polygon_base(source_polygon_base: &mut u32) -> Result<()> {
    *source_polygon_base = (*source_polygon_base)
        .checked_add(1)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    Ok(())
}

fn advance_source_ids_for_shape(
    shape: &Shape,
    source_edge_base: &mut u32,
    source_polygon_base: &mut u32,
) -> Result<()> {
    shape.try_for_each_component(&mut |component| -> Result<()> {
        match component {
            Component::Point(_) => (),
            Component::LineString(line) => {
                advance_source_edge_base(source_edge_base, line.coord_count())?;
            },
            Component::Polygon(polygon) => {
                advance_polygon_source_edge_base(source_edge_base, polygon)?;
                advance_source_polygon_base(source_polygon_base)?;
            },
        }
        Ok(())
    })
}

fn push_linework_with_source_edges(
    chains: &mut LineworkChains,
    coordinates: &crate::geometry::CoordSeq,
    source_edges: &mut Vec<u32>,
    source_offsets: &mut Vec<usize>,
    source_ring_bases: &mut Vec<u32>,
    source_edge_base: &mut u32,
) -> Result<()> {
    if coordinates.coord_count() == 0 {
        return Ok(());
    }
    source_offsets.push(source_edges.len());
    source_ring_bases.push(*source_edge_base);
    let _ = chains.push_line_with_source_edges(coordinates, source_edges)?;
    advance_source_edge_base(source_edge_base, coordinates.coord_count())
}

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

    pub(crate) fn is_simple_with_seam_cut_edge_pairs(
        &self,
        seam_cut_edge_pairs: &SeamCutEdgePairs,
    ) -> Result<bool> {
        match self {
            Self::Empty(GeometryKind::GeometryCollection, _) | Self::GeometryCollection(_) => {
                Ok(false)
            },
            Self::Point(_) | Self::Empty(..) => Ok(true),
            Self::MultiPoint(points) => Ok(unique_xy_points(points)?.len() == points.coord_count()),
            Self::LineString(points) => line_is_simple(points),
            Self::MultiLineString(lines) => multiline_is_simple(lines),
            Self::Polygon(_) | Self::MultiPolygon(_) => Ok(self
                .validate_with_seam_cut_edge_pairs(seam_cut_edge_pairs)?
                .is_none()),
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

    pub(crate) fn self_intersections_with_seam_cut_edge_pairs(
        &self,
        seam_cut_edge_pairs: &SeamCutEdgePairs,
    ) -> Result<Vec<Point>> {
        let mut nodes = Vec::new();
        let mut seen = HashSet::new();
        let mut visit = |point: Point| {
            if seen.insert(PointKey::new(point)) {
                nodes.push(point);
            }
        };
        collect_duplicate_points(self, &mut visit);

        let mut chains = LineworkChains::default();
        let mut source_edges = Vec::new();
        let mut source_offsets = Vec::new();
        let mut source_ring_bases = Vec::new();
        let mut source_edge_base = 0_u32;
        self.try_for_each_component(&mut |component| -> Result<()> {
            match component {
                Component::Point(_) => (),
                Component::LineString(line) => {
                    push_linework_with_source_edges(
                        &mut chains,
                        line.as_coords(),
                        &mut source_edges,
                        &mut source_offsets,
                        &mut source_ring_bases,
                        &mut source_edge_base,
                    )?;
                },
                Component::Polygon(polygon) => {
                    for ring in polygon.rings() {
                        push_linework_with_source_edges(
                            &mut chains,
                            ring.coords(),
                            &mut source_edges,
                            &mut source_offsets,
                            &mut source_ring_bases,
                            &mut source_edge_base,
                        )?;
                    }
                },
            }
            Ok(())
        })?;

        let mut failure = Ok(());
        let _candidate_interactions = chains.for_each_candidate_pair(|left, right| {
            let (left, right) = (chains.at(left), chains.at(right));
            match is_seam_cut_pair(
                &left,
                &right,
                &source_edges,
                &source_offsets,
                &source_ring_bases,
                seam_cut_edge_pairs,
            ) {
                Ok(true) => return ControlFlow::Continue(()),
                Ok(false) => {},
                Err(error) => {
                    failure = Err(error);
                    return ControlFlow::Break(());
                },
            }
            match collect_offending_pair(left, right, &mut visit) {
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
            Some(witness) => {
                // Shared squared-norm trust: normal → sqrt; zero/subnormal →
                // recompute the witness pair in distance space so tiny positive
                // clearances are not quantized (or dropped as false-zero).
                if witness.distance_squared.is_normal() {
                    witness.distance_squared.sqrt()
                } else if witness.distance_squared >= 0.0 && witness.distance_squared.is_finite() {
                    let emitted = emit_from_original(&witness.points, &witness.plan)?;
                    let [a, b] = emitted.as_slice() else {
                        return Ok(0.0);
                    };
                    point_distance(*a, *b)
                } else {
                    witness.distance_squared
                }
            },
        })
    }

    pub(crate) fn minimum_clearance_plan(&self) -> Result<Option<MinimumClearanceWitness>> {
        minimum_clearance_witness(self)
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

    pub fn validate(&self) -> crate::error::Result<Option<ValidationIssue>> {
        self.validate_with_shared_owners(&mut SharedShapeOwners::for_collections())
    }

    pub(crate) fn validate_with_seam_cut_edge_pairs(
        &self,
        seam_cut_edge_pairs: &SeamCutEdgePairs,
    ) -> Result<Option<ValidationIssue>> {
        let mut source_edge_base = 0_u32;
        let mut source_polygon_base = 0_u32;
        self.validate_with_seam_cut_edge_pairs_inner(
            &mut SharedShapeOwners::for_collections(),
            seam_cut_edge_pairs,
            &mut source_edge_base,
            &mut source_polygon_base,
        )
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

    fn validate_with_seam_cut_edge_pairs_inner(
        &self,
        owners: &mut SharedShapeOwners,
        seam_cut_edge_pairs: &SeamCutEdgePairs,
        source_edge_base: &mut u32,
        source_polygon_base: &mut u32,
    ) -> Result<Option<ValidationIssue>> {
        match self {
            Self::Point(point) => Ok(validate_point(*point, "$")),
            Self::MultiPoint(points) => Ok(validate_points(points, "$")),
            Self::LineString(points) => {
                if let Some(issue) = validate_line(points, "line string") {
                    return Ok(Some(issue));
                }
                advance_source_edge_base(source_edge_base, points.coord_count())?;
                Ok(None)
            },
            Self::MultiLineString(lines) => {
                for (index, line) in lines.iter().enumerate() {
                    if let Some(issue) = validate_line(line, "multi line string member") {
                        return Ok(Some(issue.with_path_prefix(&format!("$[{index}]"))));
                    }
                    advance_source_edge_base(source_edge_base, line.coord_count())?;
                }
                Ok(None)
            },
            Self::Polygon(polygon) => {
                if let Some(issue) = polygon.validate_with_seam_cut_edge_pairs(
                    "polygon",
                    "$",
                    *source_edge_base,
                    *source_polygon_base,
                    seam_cut_edge_pairs,
                )? {
                    return Ok(Some(issue));
                }
                advance_polygon_source_edge_base(source_edge_base, polygon)?;
                advance_source_polygon_base(source_polygon_base)?;
                Ok(None)
            },
            Self::MultiPolygon(polygons) => {
                let source_edge_start = *source_edge_base;
                let source_polygon_start = *source_polygon_base;
                let mut next_source_edge = source_edge_start;
                let mut next_source_polygon = source_polygon_start;
                for (index, polygon) in polygons.iter().enumerate() {
                    if let Some(issue) = polygon.validate_with_seam_cut_edge_pairs(
                        "multi polygon member",
                        "$",
                        next_source_edge,
                        next_source_polygon,
                        seam_cut_edge_pairs,
                    )? {
                        return Ok(Some(issue.with_path_prefix(&format!("$[{index}]"))));
                    }
                    advance_polygon_source_edge_base(&mut next_source_edge, polygon)?;
                    advance_source_polygon_base(&mut next_source_polygon)?;
                }
                if let Some(issue) = multi_polygon_members_issue_with_seam_cuts(
                    polygons.as_slice(),
                    "$",
                    Some((seam_cut_edge_pairs, source_edge_start, source_polygon_start)),
                )? {
                    return Ok(Some(issue));
                }
                *source_edge_base = next_source_edge;
                *source_polygon_base = next_source_polygon;
                Ok(None)
            },
            Self::GeometryCollection(geometries) => {
                if !owners.first_collection(geometries.as_slice(), geometries.members_are_shared())
                {
                    advance_source_ids_for_shape(self, source_edge_base, source_polygon_base)?;
                    return Ok(None);
                }
                for (index, geometry) in geometries.iter().enumerate() {
                    if let Some(issue) = geometry.validate_with_seam_cut_edge_pairs_inner(
                        owners,
                        seam_cut_edge_pairs,
                        source_edge_base,
                        source_polygon_base,
                    )? {
                        return Ok(Some(issue.with_path_prefix(&format!("$[{index}]"))));
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
        match self {
            // Empty shapes are always valid; an invalid (multi)point means
            // non-finite coordinates, which nothing can reconstruct.
            Self::Point(_) | Self::MultiPoint(_) | Self::Empty(..) => Err(
                GeometryErrorKind::repair_failed("non-finite coordinates cannot be repaired"),
            ),
            Self::LineString(line) => line_repair(line),
            Self::MultiLineString(lines) => multiline_repair(lines),
            Self::Polygon(polygon) => {
                let rebuilt = polygonal_repair(std::slice::from_ref(polygon), method)?;
                carry_ordinates(rebuilt, &[self], "repair", drop)
            },
            Self::MultiPolygon(polygons) => {
                let rebuilt = polygonal_repair(polygons, method)?;
                carry_ordinates(rebuilt, &[self], "repair", drop)
            },
            Self::GeometryCollection(geometries) => Ok(Self::GeometryCollection(
                geometries.try_map_members(|geometry| {
                    // Per-member gate: valid members pass through untouched.
                    if geometry.validate()?.is_none() {
                        Ok(geometry.clone())
                    } else {
                        geometry.repair(method, drop)
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
