use std::ops::ControlFlow;

use ahash::HashSetExt as _;

use crate::error::Result;
use crate::geometry::predicates::{
    LineworkChains, MinimumClearanceWitness, collect_duplicate_points, collect_offending_pair,
    line_crosses_antimeridian, line_is_closed, line_is_simple, minimum_clearance_witness,
    multiline_is_simple, polygonal_repair, shell_is_convex, validate_geo_multi_polygon,
    validate_line, validate_point, validate_points,
};
use crate::geometry::{
    CoordSeq, CoordinateAxes, Coordinates as _, EmptyKind, GeometryErrorKind, HashSet, LineSeq,
    Point, PointKey, RepairMethod, Shape, ValidationIssue, carry_ordinates, emit_from_original,
    finish_planar_squared_min, point_distance, ring_winding, same_point, unique_xy_points,
    witness_pair,
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

    pub fn is_ring(&self) -> bool {
        match self {
            Self::LineString(points) => line_is_closed(points) && line_is_simple(points),
            _ => false,
        }
    }

    pub fn is_simple(&self) -> bool {
        match self {
            // Collections are never simple by convention, including the typed
            // empty (matching the empty container form below).
            Self::Empty(EmptyKind::GeometryCollection, _) | Self::GeometryCollection(_) => false,
            Self::Point(_) | Self::Empty(..) => true,
            Self::MultiPoint(points) => unique_xy_points(points).len() == points.len(),
            Self::LineString(points) => line_is_simple(points),
            Self::MultiLineString(lines) => multiline_is_simple(lines),
            Self::Polygon(_) | Self::MultiPolygon(_) => self.validate().is_none(),
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
    pub fn self_intersections(&self) -> Vec<Point> {
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
        });
        nodes
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

/// Whether any ring/part's longitude values span the closed world band
/// ``[-180, 180]`` (or an equivalent ≥360° extent). Catches full-longitude
/// sources that neither enclose a pole nor cross the antimeridian as an edge
/// (e.g. ``box(-180, -10, 180, 10)``).
pub(crate) fn shape_spans_full_longitude(shape: &Shape) -> bool {
    const EPS: f64 = 1e-9;
    const FULL_SPAN: f64 = 360.0 - EPS;

    fn coords_span_full_longitude(coords: &CoordSeq) -> bool {
        if coords.is_empty() {
            return false;
        }
        let mut min_lon = f64::INFINITY;
        let mut max_lon = f64::NEG_INFINITY;
        for point in coords.points() {
            min_lon = min_lon.min(point.x);
            max_lon = max_lon.max(point.x);
        }
        (min_lon <= -180.0 + EPS && max_lon >= 180.0 - EPS) || (max_lon - min_lon) >= FULL_SPAN
    }

    match shape {
        Shape::LineString(coords) => coords_span_full_longitude(coords),
        Shape::MultiLineString(lines) => lines
            .iter()
            .any(|line| coords_span_full_longitude(line.as_ref())),
        Shape::Polygon(polygon) => polygon.rings().any(coords_span_full_longitude),
        Shape::MultiPolygon(polygons) => polygons
            .iter()
            .any(|polygon| polygon.rings().any(coords_span_full_longitude)),
        Shape::GeometryCollection(parts) => parts.iter().any(shape_spans_full_longitude),
        Shape::Point(_) | Shape::MultiPoint(_) | Shape::Empty(..) => false,
    }
}

impl Shape {
    pub fn minimum_clearance(&self) -> f64 {
        minimum_clearance_witness(self).map_or(f64::INFINITY, |witness| {
            // Shared squared-norm trust: normal → sqrt; zero/subnormal →
            // recompute the witness pair in distance space so tiny positive
            // clearances are not quantized (or dropped as false-zero).
            finish_planar_squared_min(witness.distance_squared, || {
                let points = self.points_vec();
                let emitted = emit_from_original(&points, &witness.plan);
                let [a, b] = emitted.as_slice() else {
                    return 0.0;
                };
                point_distance(*a, *b)
            })
        })
    }

    pub(crate) fn minimum_clearance_plan(&self) -> Option<MinimumClearanceWitness> {
        minimum_clearance_witness(self)
    }

    /// The two-point `LineString` realizing `minimum_clearance` —
    /// `LINESTRING EMPTY` when the clearance
    /// is infinite (fewer than two distinct vertices).
    pub fn minimum_clearance_line(&self) -> Self {
        minimum_clearance_witness(self).map_or_else(
            || Self::LineString(LineSeq::empty(CoordinateAxes::XY)),
            |witness| {
                let points = self.points_vec();
                let emitted = emit_from_original(&points, &witness.plan);
                let [a, b] = emitted.as_slice() else {
                    unreachable!("minimum_clearance witness plan has two vertices");
                };
                Self::LineString(
                    LineSeq::try_new(witness_pair(*a, *b))
                        .expect("minimum-clearance witness has two vertices"),
                )
            },
        )
    }

    pub fn validate(&self) -> Option<ValidationIssue> {
        match self {
            Self::Point(point) => validate_point(*point, "$"),
            Self::MultiPoint(points) => validate_points(points, "$"),
            Self::LineString(points) => validate_line(points, "line string"),
            Self::MultiLineString(lines) => lines.iter().enumerate().find_map(|(idx, line)| {
                validate_line(line, "multi line string member")
                    .map(|issue| issue.with_path_prefix(&format!("$[{idx}]")))
            }),
            Self::Polygon(polygon) => polygon.validate("polygon", "$"),
            Self::MultiPolygon(polygons) => polygons
                .iter()
                .enumerate()
                .find_map(|(idx, polygon)| {
                    polygon
                        .validate("multi polygon member", "$")
                        .map(|issue| issue.with_path_prefix(&format!("$[{idx}]")))
                })
                .or_else(|| validate_geo_multi_polygon(polygons, "$")),
            Self::GeometryCollection(geometries) => {
                geometries.iter().enumerate().find_map(|(idx, geometry)| {
                    geometry
                        .validate()
                        .map(|issue| issue.with_path_prefix(&format!("$[{idx}]")))
                })
            },
            Self::Empty(..) => None,
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
            Self::Empty(EmptyKind::Polygon, _) => Ok(true),
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
                    .iter()
                    .filter(|line| validate_line(line, "multi line string member").is_none())
                    .cloned()
                    .collect::<Vec<_>>();
                if repaired.is_empty() && !lines.is_empty() {
                    Err(GeometryErrorKind::UnrepairableMultiLineString.into())
                } else {
                    Ok(Self::MultiLineString(repaired))
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
            Self::GeometryCollection(geometries) => geometries
                .iter()
                .map(|geometry| {
                    // Per-member gate: valid members pass through untouched.
                    if geometry.validate().is_none() {
                        Ok(geometry.clone())
                    } else {
                        geometry.repair(method, drop)
                    }
                })
                .collect::<Result<Vec<_>, _>>()
                .map(Self::GeometryCollection),
        }
    }

    /// Whether a closed linestring winds counter-clockwise
    /// (`false` for open lines and non-lineal geometry).
    pub fn is_ccw(&self) -> bool {
        match self {
            Self::LineString(points) => {
                points.coord_count() >= 4
                    && points
                        .first_coord()
                        .zip(points.last_coord())
                        .is_some_and(|(first, last)| same_point(first, last))
                    && ring_winding(points).is_ccw()
            },
            _ => false,
        }
    }
}
