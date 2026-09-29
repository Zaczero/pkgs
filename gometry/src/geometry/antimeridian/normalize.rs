#![expect(
    clippy::similar_names,
    reason = "axis and formula symbols name paired quantities here (minx/miny, xs/ys, lat_rad/lam_rad, step/stop), which this lint reads as typos"
)]
use std::collections::HashMap;

use crate::error::Result;
use crate::geographic::GeographicFrame;
use crate::geometry::{
    Bounds, CoordSeq, Coordinates as _, GeometryErrorKind, Point, Polygon, PreparedPlan,
    RepairMethod, Segment, Shape, ShapeOwnerKey, TopologyDomain, ValidationIssue, VisibleXYKey,
    shape_encloses_pole_in_frame, shape_has_polar_ring_in_frame, shared_segment_part,
};

fn try_topology_split(shape: &Shape, domain: TopologyDomain) -> Result<Shape> {
    match domain {
        TopologyDomain::Cartesian => shape.force_2d(),
        TopologyDomain::Geographic(frame) => shape.force_2d()?.split_antimeridian_in_frame(frame),
    }
}

fn split_in_domain(shape: &Shape, domain: TopologyDomain) -> Result<Shape> {
    match domain {
        TopologyDomain::Cartesian => Ok(shape.clone()),
        TopologyDomain::Geographic(frame) => shape.split_antimeridian_in_frame(frame),
    }
}

fn needs_topology_split(shape: &Shape, domain: TopologyDomain) -> bool {
    match domain {
        TopologyDomain::Cartesian => false,
        TopologyDomain::Geographic(frame) => {
            crate::geometry::crosses_antimeridian_in_frame(shape, frame)
        },
    }
}

fn data_needs_topology_split(data: &PreparedPlan, domain: TopologyDomain) -> bool {
    match domain {
        TopologyDomain::Cartesian => false,
        TopologyDomain::Geographic(frame) if frame == GeographicFrame::crs84() => {
            data.crosses_antimeridian()
        },
        TopologyDomain::Geographic(frame) => {
            crate::geometry::crosses_antimeridian_in_frame(data.shape(), frame)
        },
    }
}

fn rings_share_source_linework(left: &CoordSeq, right: &CoordSeq) -> bool {
    left.segment_pairs().any(|[left_start, left_end]| {
        let left = Segment {
            start: left_start.xy(),
            end: left_end.xy(),
        };
        right.segment_pairs().any(|[right_start, right_end]| {
            shared_segment_part(left, Segment {
                start: right_start.xy(),
                end: right_end.xy(),
            })
            .is_some()
        })
    })
}

/// Validate relationships involving crossing polygon holes before the public
/// split's set-difference assembly. Difference is deliberately repair-like: an
/// outside hole simply subtracts nothing and overlapping holes collapse to
/// their union. That is useful to `repair`, but validity must diagnose the
/// malformed source rather than bless the normalized result.
fn crossing_hole_relationship_issue(
    shape: &Shape,
    path: &str,
    domain: TopologyDomain,
) -> Result<Option<ValidationIssue>> {
    let TopologyDomain::Geographic(frame) = domain else {
        return Ok(None);
    };
    let polygon_issue = |polygon: &Polygon, path: &str| -> Result<Option<ValidationIssue>> {
        let mut crossing = Vec::new();
        for (index, hole) in polygon.holes.iter().enumerate() {
            let candidate = Shape::Polygon(Polygon::new((*hole).clone(), Vec::new()));
            if crate::geometry::crosses_antimeridian_in_frame(&candidate, frame) {
                crossing.push((index, hole));
            }
        }
        if crossing.is_empty() {
            return Ok(None);
        }

        let crossing_indices: Vec<usize> = crossing.iter().map(|(index, _)| *index).collect();
        let stationary = polygon
            .holes
            .iter()
            .enumerate()
            .filter(|(index, _)| !crossing_indices.contains(index))
            .map(|(_, hole)| hole.clone())
            .collect();
        let container = Shape::Polygon(Polygon::new(polygon.shell.clone(), stationary));
        let container = match try_topology_split(&container, domain) {
            Ok(container) => container,
            Err(error) => {
                if matches!(error.kind(), crate::error::ErrorKind::Resource(_)) {
                    return Err(error);
                }
                return Ok(Some(ValidationIssue::new(
                    "topology_normalization_failed",
                    format!("geographic topology normalization failed: {error}"),
                    None,
                    path,
                )));
            },
        };
        let mut normalized_holes: Vec<(usize, Shape)> = Vec::with_capacity(crossing.len());
        for (index, hole) in crossing {
            let witness = hole.coords().first();
            if rings_share_source_linework(polygon.shell.coords(), hole.coords())
                || polygon
                    .holes
                    .iter()
                    .enumerate()
                    .any(|(other_index, other)| {
                        other_index != index
                            && rings_share_source_linework(other.coords(), hole.coords())
                    })
            {
                return Ok(Some(ValidationIssue::new(
                    "shared_linework",
                    format!("interior ring at index {index} intersects another ring on a line"),
                    witness,
                    path,
                )));
            }
            let hole = Shape::Polygon(Polygon::new(hole.clone(), Vec::new()));
            let hole = match try_topology_split(&hole, domain) {
                Ok(hole) => hole,
                Err(error) => {
                    if matches!(error.kind(), crate::error::ErrorKind::Resource(_)) {
                        return Err(error);
                    }
                    return Ok(Some(ValidationIssue::new(
                        "topology_normalization_failed",
                        format!("geographic topology normalization failed: {error}"),
                        witness,
                        format!("{path}.holes[{index}]"),
                    )));
                },
            };
            if !container.contains(&hole)? {
                return Ok(Some(ValidationIssue::new(
                    "hole_outside_shell",
                    format!(
                        "interior ring at index {index} is not contained within the polygon's exterior"
                    ),
                    witness,
                    path,
                )));
            }
            for (other_index, other) in &normalized_holes {
                let relation = other.relate(&hole)?;
                if relation.as_bytes()[0] != b'F' {
                    return Ok(Some(ValidationIssue::new(
                        "hole_overlap",
                        format!(
                            "interior ring at index {} and interior ring at index {} intersect on an area",
                            index.min(*other_index),
                            index.max(*other_index),
                        ),
                        witness,
                        path,
                    )));
                }
            }
            normalized_holes.push((index, hole));
        }
        Ok(None)
    };

    match shape {
        Shape::Polygon(polygon) => polygon_issue(polygon, path),
        Shape::MultiPolygon(polygons) => {
            for (index, polygon) in polygons.iter().enumerate() {
                if let Some(issue) = polygon_issue(polygon, &format!("{path}.geometries[{index}]"))?
                {
                    return Ok(Some(issue));
                }
            }
            Ok(None)
        },
        Shape::GeometryCollection(parts) => {
            for (index, part) in parts.iter().enumerate() {
                if let Some(issue) = crossing_hole_relationship_issue(
                    part,
                    &format!("{path}.geometries[{index}]"),
                    domain,
                )? {
                    return Ok(Some(issue));
                }
            }
            Ok(None)
        },
        _ => Ok(None),
    }
}

/// Validate stored geometry in its coordinate frame. Projected and CRS-free
/// geometries retain ordinary planar OGC validity; only seam-crossing
/// geographic geometry is normalized first.
pub(crate) fn validate_shape_in_frame(
    shape: &Shape,
    domain: TopologyDomain,
) -> Result<Option<ValidationIssue>> {
    if needs_topology_split(shape, domain) {
        let planar = shape.force_2d()?;
        if let Some(issue) = crossing_hole_relationship_issue(&planar, "$", domain)? {
            return Ok(Some(issue));
        }
        try_topology_split(shape, domain).map_or_else(
            |error| {
                if matches!(error.kind(), crate::error::ErrorKind::Resource(_)) {
                    return Err(error);
                }
                Ok(Some(ValidationIssue::new(
                    "topology_normalization_failed",
                    format!("geographic topology normalization failed: {error}"),
                    None,
                    "$",
                )))
            },
            |normalized| normalized.validate(),
        )
    } else {
        shape.validate()
    }
}

/// Cached-handle twin of [`validate_shape_in_frame`].
pub(crate) fn validate_data_in_frame(
    data: &PreparedPlan,
    domain: TopologyDomain,
) -> Result<Option<ValidationIssue>> {
    if data_needs_topology_split(data, domain) {
        validate_shape_in_frame(data.shape(), domain)
    } else {
        Ok(data.validate_cached()?.cloned())
    }
}

/// Frame-aware simplicity. Non-crossing inputs keep the frozen handle's cached
/// verdict; seam-crossing geographic inputs answer on normalized topology.
pub(crate) fn is_simple_data_in_frame(data: &PreparedPlan, domain: TopologyDomain) -> Result<bool> {
    if data_needs_topology_split(data, domain) {
        match try_topology_split(data.shape(), domain) {
            Ok(shape) => shape.is_simple(),
            Err(error) if matches!(error.kind(), crate::error::ErrorKind::Resource(_)) => {
                Err(error)
            },
            Err(_) => Ok(false),
        }
    } else {
        data.is_simple_cached()
    }
}

/// Frame-aware `is_ring`: closure is structural on the original line, while
/// simplicity is evaluated on normalized seam topology.
pub(crate) fn is_ring_data_in_frame(data: &PreparedPlan, domain: TopologyDomain) -> Result<bool> {
    if data_needs_topology_split(data, domain) {
        if !data.shape().is_closed() {
            return Ok(false);
        }
        match try_topology_split(data.shape(), domain) {
            Ok(shape) => shape.is_simple(),
            Err(error) if matches!(error.kind(), crate::error::ErrorKind::Resource(_)) => {
                Err(error)
            },
            Err(_) => Ok(false),
        }
    } else {
        data.shape().is_ring()
    }
}

/// Frame-aware self-intersection diagnostics, shared by scalar and grouped
/// array output.
pub(crate) fn self_intersections_in_frame(
    shape: &Shape,
    domain: TopologyDomain,
) -> Result<Vec<Point>> {
    if needs_topology_split(shape, domain) {
        match try_topology_split(shape, domain) {
            Ok(normalized) => {
                if normalized.is_simple()? {
                    Ok(Vec::new())
                } else {
                    normalized.self_intersections()
                }
            },
            Err(error) if matches!(error.kind(), crate::error::ErrorKind::Resource(_)) => {
                Err(error)
            },
            Err(_) => shape.self_intersections(),
        }
    } else {
        shape.self_intersections()
    }
}

fn repair_invalid_shape(
    shape: &Shape,
    domain: TopologyDomain,
    method: RepairMethod,
) -> Result<Shape> {
    let repaired = if needs_topology_split(shape, domain) {
        // Preserve source Z/M on ordinary seam crossings. Polar closure with
        // Z/M remains an honest error: no source ordinate exists at a
        // fabricated pole vertex.
        split_in_domain(shape, domain)?.repair(method, false)
    } else {
        shape.repair(method, false)
    }?;
    if let Some(issue) = validate_shape_in_frame(&repaired, domain)? {
        return Err(GeometryErrorKind::repair_failed(format!(
            "repair did not produce valid geometry: {}",
            issue.detail,
        )));
    }
    Ok(repaired)
}

/// Repair only when frame-aware validation finds a defect. `None` means the
/// caller must preserve the original frozen handle/storage unchanged.
pub(crate) fn repair_shape_in_frame(
    shape: &Shape,
    domain: TopologyDomain,
    method: RepairMethod,
) -> Result<Option<Shape>> {
    validate_shape_in_frame(shape, domain)?
        .map(|_| repair_invalid_shape(shape, domain, method))
        .transpose()
}

/// Cached-handle twin of [`repair_shape_in_frame`].
pub(crate) fn repair_data_in_frame(
    data: &PreparedPlan,
    domain: TopologyDomain,
    method: RepairMethod,
) -> Result<Option<Shape>> {
    validate_data_in_frame(data, domain)?
        .map(|_| repair_invalid_shape(data.shape(), domain, method))
        .transpose()
}

/// Shift canonical western longitudes by one native full turn so a crossing
/// geometry is contiguous for the planar centroid kernel.
fn unwrap_longitude(shape: &Shape, frame: GeographicFrame) -> Result<Shape> {
    let axis = frame.longitude();
    shape.map_xy_with(|x, y| {
        let longitude = axis
            .canonicalize(x)
            .map_err(|error| GeometryErrorKind::antimeridian_split_failed(error.to_string()))?
            .get();
        Ok((
            if longitude < 0.0 {
                longitude + axis.period()
            } else {
                longitude
            },
            y,
        ))
    })
}

/// Canonicalize a native longitude into the frame's half-open seam interval.
fn wrap_longitude(lon: f64, frame: GeographicFrame) -> Result<f64> {
    frame
        .longitude()
        .canonicalize(lon)
        .map(|longitude| longitude.get())
        .map_err(|error| GeometryErrorKind::antimeridian_split_failed(error.to_string()))
}

/// Rewrap a derived point's longitude after an unwrapped planar kernel.
fn rewrap_point(point: Point, frame: GeographicFrame) -> Result<Point> {
    point.with_xy(wrap_longitude(point.x, frame)?, point.y)
}

/// Derived representative point (centroid / `point_on_surface`) on a crossing
/// geographic geometry. Two strategies, because unwrap-rewrap cannot serve both:
///
/// * `interior` callers (`point_on_surface`, which must return a point strictly
///   inside) AND any pole-enclosing geometry route through the seam+pole SPLIT —
///   the unwrapped form of a polar cap is a degenerate longitude sliver (a
///   longitude shift can't reconstruct the cap), and the split's planar kernel
///   yields a point genuinely inside the true region. The split is in the
///   frame's canonical longitude interval, so no rewrap is needed.
///   `force_2d` keeps the split infallible
///   (a Z/M pole closure would otherwise trip the ordinate gate).
/// * every other crossing `centroid` uses unwrap-rewrap for the planar centroid
///   on the contiguous native longitude chart (the split's two opposite-seam
///   halves would average to a meaningless mid-longitude).
#[derive(Clone, Copy)]
pub(crate) enum DerivedPointStrategy {
    Centroid,
    Interior,
}

pub(crate) fn derived_point_unwrapped(
    shape: &Shape,
    frame: GeographicFrame,
    strategy: DerivedPointStrategy,
    kernel: impl FnOnce(&Shape) -> Result<Shape>,
) -> Result<Shape> {
    let encloses_pole = shape_encloses_pole_in_frame(shape, frame, true)
        || shape_encloses_pole_in_frame(shape, frame, false);
    if matches!(strategy, DerivedPointStrategy::Interior) || encloses_pole {
        return kernel(&try_topology_split(
            shape,
            TopologyDomain::Geographic(frame),
        )?);
    }
    let unwrapped = unwrap_longitude(shape, frame)?;
    match kernel(&unwrapped)? {
        Shape::Point(point) => Ok(Shape::Point(rewrap_point(point, frame)?)),
        other => Ok(other),
    }
}

/// Shared scalar/array gate for derived geographic points.
pub(crate) fn unary_antimeridian_derived(
    shape: &Shape,
    domain: TopologyDomain,
    strategy: DerivedPointStrategy,
    kernel: impl FnOnce(&Shape) -> Result<Shape>,
) -> Result<Shape> {
    match domain {
        TopologyDomain::Geographic(frame)
            if crate::geometry::crosses_antimeridian_in_frame(shape, frame) =>
        {
            derived_point_unwrapped(shape, frame, strategy, kernel)
        },
        TopologyDomain::Cartesian | TopologyDomain::Geographic(_) => kernel(shape),
    }
}

/// Geographic-crossing bounds in the west>east convention: ``minx`` is the
/// minimum eastern (``lon >= 0``) longitude and ``maxx`` the maximum western
/// (``lon < 0``) longitude; latitude extrema are ordinary planar min/max.
pub(crate) fn geographic_crossing_bounds(shape: &Shape) -> Option<Bounds> {
    geographic_crossing_bounds_in_frame(shape, GeographicFrame::crs84())
}

pub(crate) fn geographic_crossing_bounds_in_frame(
    shape: &Shape,
    frame: GeographicFrame,
) -> Option<Bounds> {
    geographic_crossing_bounds_for_shapes_in_frame(std::iter::once(shape), frame)
}

pub(crate) fn geographic_crossing_bounds_for_shapes_in_frame(
    shapes: impl IntoIterator<Item = impl std::borrow::Borrow<Shape>>,
    frame: GeographicFrame,
) -> Option<Bounds> {
    let axis = frame.longitude();
    let mut point_memo = CrossingBoundsMemo::default();
    let mut summary = CrossingBoundsSummary::default();
    let mut encloses_north = false;
    let mut encloses_south = false;
    let mut has_polar_ring = false;
    for shape in shapes {
        let shape = shape.borrow();
        summary.merge(point_memo.shape_summary(shape, frame));
        encloses_north |= shape_encloses_pole_in_frame(shape, frame, true);
        encloses_south |= shape_encloses_pole_in_frame(shape, frame, false);
        has_polar_ring |= shape_has_polar_ring_in_frame(shape, frame);
    }
    let (mut minx, mut miny, mut maxx, mut maxy) = (
        summary.eastern_min?,
        summary.miny?,
        summary.western_max?,
        summary.maxy?,
    );
    // A pole-enclosing ring covers the whole polar cap, so its envelope reaches
    // the pole and spans every longitude — the eastern_min/western_max sliver
    // (which only describes the seam window) would otherwise drop the cap. Mirrors
    // the spatial index's geographic envelopes so public bounds() and the index
    // agree.
    if has_polar_ring {
        (minx, maxx) = (-axis.half_period(), axis.half_period());
    }
    if encloses_north {
        maxy = frame.pole();
    }
    if encloses_south {
        miny = -frame.pole();
    }
    Some(Bounds::new_raw(minx, miny, maxx, maxy))
}

#[derive(Clone, Copy, Default)]
struct CrossingBoundsSummary {
    eastern_min: Option<f64>,
    western_max: Option<f64>,
    miny: Option<f64>,
    maxy: Option<f64>,
}

impl CrossingBoundsSummary {
    fn include_point(&mut self, point: Point, frame: GeographicFrame) {
        let longitude = frame
            .longitude()
            .canonicalize(point.x)
            .map_or(point.x, |value| value.get());
        if longitude >= 0.0 {
            include_min(&mut self.eastern_min, longitude);
        } else {
            include_max(&mut self.western_max, longitude);
        }
        include_min(&mut self.miny, point.y);
        include_max(&mut self.maxy, point.y);
    }

    fn merge(&mut self, other: Self) {
        if let Some(value) = other.eastern_min {
            include_min(&mut self.eastern_min, value);
        }
        if let Some(value) = other.western_max {
            include_max(&mut self.western_max, value);
        }
        if let Some(value) = other.miny {
            include_min(&mut self.miny, value);
        }
        if let Some(value) = other.maxy {
            include_max(&mut self.maxy, value);
        }
    }
}

fn include_min(accumulator: &mut Option<f64>, value: f64) {
    *accumulator = Some(accumulator.map_or(value, |current| current.min(value)));
}

fn include_max(accumulator: &mut Option<f64>, value: f64) {
    *accumulator = Some(accumulator.map_or(value, |current| current.max(value)));
}

#[derive(Default)]
struct CrossingBoundsMemo {
    collections: HashMap<ShapeOwnerKey, CrossingBoundsSummary>,
    xy_runs: HashMap<VisibleXYKey, CrossingBoundsSummary>,
}

impl CrossingBoundsMemo {
    fn coord_summary(
        &mut self,
        coords: &CoordSeq,
        frame: GeographicFrame,
    ) -> CrossingBoundsSummary {
        if let Some(key) = coords.shared_xy_key()
            && let Some(&summary) = self.xy_runs.get(&key)
        {
            return summary;
        }
        let summary = coords
            .iter()
            .fold(CrossingBoundsSummary::default(), |mut acc, point| {
                acc.include_point(point, frame);
                acc
            });
        if let Some(key) = coords.shared_xy_key() {
            self.xy_runs.insert(key, summary);
        }
        summary
    }

    fn polygon_summary(
        &mut self,
        polygon: &Polygon,
        frame: GeographicFrame,
    ) -> CrossingBoundsSummary {
        let mut summary = CrossingBoundsSummary::default();
        for ring in polygon.rings() {
            summary.merge(self.coord_summary(ring.coords(), frame));
        }
        summary
    }

    fn shape_summary(&mut self, shape: &Shape, frame: GeographicFrame) -> CrossingBoundsSummary {
        if let Shape::GeometryCollection(collection) = shape {
            let owner = collection
                .members_are_shared()
                .then(|| ShapeOwnerKey::new(collection.as_slice()));
            if let Some(&summary) = owner.as_ref().and_then(|owner| self.collections.get(owner)) {
                return summary;
            }
            let mut summary = CrossingBoundsSummary::default();
            for member in collection {
                summary.merge(self.shape_summary(member, frame));
            }
            if let Some(owner) = owner {
                self.collections.insert(owner, summary);
            }
            return summary;
        }
        match shape {
            Shape::Point(point) => {
                let mut summary = CrossingBoundsSummary::default();
                summary.include_point(*point, frame);
                summary
            },
            Shape::MultiPoint(coords) => self.coord_summary(coords, frame),
            Shape::LineString(coords) => self.coord_summary(coords, frame),
            Shape::MultiLineString(lines) => {
                let mut summary = CrossingBoundsSummary::default();
                for line in lines {
                    summary.merge(self.coord_summary(line.as_coords(), frame));
                }
                summary
            },
            Shape::Polygon(polygon) => self.polygon_summary(polygon, frame),
            Shape::MultiPolygon(polygons) => {
                let mut summary = CrossingBoundsSummary::default();
                for polygon in polygons {
                    summary.merge(self.polygon_summary(polygon, frame));
                }
                summary
            },
            Shape::GeometryCollection(_) => unreachable!("handled above"),
            Shape::Empty(..) => CrossingBoundsSummary::default(),
        }
    }
}
