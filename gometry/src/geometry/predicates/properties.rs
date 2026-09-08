use std::ops::ControlFlow;

use ahash::HashSetExt as _;

use crate::error::Result;
use crate::geometry::predicates::{
    LineworkChains, MinimumClearanceWitness, collect_duplicate_points, collect_offending_pair,
    line_crosses_antimeridian, line_is_ccw, line_is_closed, line_is_simple,
    minimum_clearance_witness, multi_polygon_members_issue, multiline_is_simple, polygonal_repair,
    shell_is_convex, validate_line, validate_point, validate_points,
};
use crate::geometry::{
    CoordinateAxes, Coordinates as _, GeometryErrorKind, GeometryKind, HashSet, LineSeq,
    MultiLineString, Point, PointKey, RepairMethod, Shape, ValidationIssue, carry_ordinates,
    emit_from_original, point_distance, unique_xy_points,
};

impl Shape {
    pub fn is_empty(&self) -> bool {
        // `any_point` recurses into collections, so a collection whose members
        // are all empty (e.g. `GEOMETRYCOLLECTION (POINT EMPTY)`) is empty too.
        !self.any_point(|_| true)
    }

    pub fn is_closed(&self) -> bool {
        match self {
            Self::LineString(points) => line_is_closed(points),
            Self::MultiLineString(lines) => !lines.is_empty() && lines.iter().all(line_is_closed),
            _ => false,
        }
    }

    pub fn is_ring(&self) -> crate::error::Result<bool> {
        match self {
            Self::LineString(points) => Ok(line_is_closed(points) && line_is_simple(points)?),
            _ => Ok(false),
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
            Self::MultiPoint(points) => {
                Ok(unique_xy_points(points).len() == points.coord_count())
            },
            Self::LineString(points) => line_is_simple(points),
            Self::MultiLineString(lines) => multiline_is_simple(lines),
            Self::Polygon(_) | Self::MultiPolygon(_) => Ok(self.validate()?.is_none()),
        }
    }

    /// The distinct XY locations where the geometry coincides with itself:
    /// proper linework self-crossings, non-adjacent touches, the endpoints
    /// of collinear overlaps (spikes and backtracks), and duplicate
    /// point-atom coordinates. Legal adjacent shared vertices, ring closures,
    /// and removable REPEATED CONSECUTIVE vertices (zero-length stutter, elided
    /// as redundancy like GEOS/JTS) are never reported, so for point/lineal
    /// input the result is non-empty exactly when
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
        chains.push_shape(self);
        let _ = chains.for_each_candidate_pair(|left, right| {
            collect_offending_pair(chains.at(left), chains.at(right), &mut visit);
            ControlFlow::Continue(())
        })?;
        Ok(nodes)
    }

    pub fn crosses_antimeridian(&self) -> bool {
        match self {
            Self::Point(_) | Self::MultiPoint(_) | Self::Empty(..) => false,
            Self::LineString(points) => line_crosses_antimeridian(points),
            Self::MultiLineString(lines) => lines.iter().any(line_crosses_antimeridian),
            Self::Polygon(polygon) => polygon.rings().any(|ring| line_crosses_antimeridian(&ring)),
            Self::MultiPolygon(polygons) => polygons
                .iter()
                .any(|polygon| polygon.rings().any(|ring| line_crosses_antimeridian(&ring))),
            Self::GeometryCollection(geometries) => {
                geometries.iter().any(Self::crosses_antimeridian)
            },
        }
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
                    let points = self.points_vec()?;
                    let emitted = emit_from_original(&points, &witness.plan);
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
                let points = self.points_vec()?;
                let emitted = emit_from_original(&points, &witness.plan);
                let [a, b] = emitted.as_slice() else {
                    unreachable!("minimum_clearance witness plan has two vertices");
                };
                Ok(Self::LineString(LineSeq::segment(*a, *b)?))
            },
        }
    }

    pub fn validate(&self) -> crate::error::Result<Option<ValidationIssue>> {
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
                for (idx, geometry) in geometries.iter().enumerate() {
                    if let Some(issue) = geometry.validate()? {
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
            _ => Err(GeometryErrorKind::SinglePolygonRequired.into()),
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
            Self::LineString(_) => Err(GeometryErrorKind::UnrepairableLineString.into()),
            Self::MultiLineString(lines) => {
                let repaired = lines
                    .members()
                    .filter_map(|member| match member {
                        crate::geometry::MultiMember::Empty => {
                            Some(crate::geometry::MultiMember::Empty)
                        },
                        crate::geometry::MultiMember::Value(index) => {
                            let line = &lines.as_slice()[index];
                            validate_line(line, "multi line string member")
                                .is_none()
                                .then(|| crate::geometry::MultiMember::Value(line.clone()))
                        },
                    })
                    .collect::<Vec<_>>();
                if repaired.is_empty() && !lines.is_empty() {
                    Err(GeometryErrorKind::UnrepairableMultiLineString.into())
                } else {
                    Ok(Self::MultiLineString(MultiLineString::from_values(
                        lines.axes(),
                        repaired,
                    )?))
                }
            },
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
            _ => false,
        }
    }
}
