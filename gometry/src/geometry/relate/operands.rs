#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use super::*;
use crate::geometry::*;

pub(crate) fn polygon_parts(shape: &Shape) -> Option<&[Polygon]> {
    match shape {
        Shape::Polygon(polygon) => Some(std::slice::from_ref(polygon)),
        Shape::MultiPolygon(polygons) => Some(polygons),
        _ => None,
    }
}

pub(crate) struct PuntalOperand {
    pub(crate) points: Vec<Point>,
    pub(crate) keys: HashSet<PointKey>,
}

impl PuntalOperand {
    pub(crate) fn from_shape(shape: &Shape) -> Option<Self> {
        let mut points = Vec::new();
        let mut keys = HashSet::new();
        match shape {
            Shape::Point(point) => push_unique_point(*point, &mut points, &mut keys),
            Shape::MultiPoint(coords) => {
                for point in coords {
                    push_unique_point(point, &mut points, &mut keys);
                }
            },
            Shape::LineString(coords) if line_is_collapsed(coords) => {
                for point in coords {
                    push_unique_point(point, &mut points, &mut keys);
                }
            },
            Shape::MultiLineString(lines) if multiline_is_collapsed(lines) => {
                for line in lines {
                    for point in line {
                        push_unique_point(point, &mut points, &mut keys);
                    }
                }
            },
            Shape::GeometryCollection(parts) => {
                for part in parts {
                    let part = Self::from_shape(part)?;
                    for point in part.points {
                        push_unique_point(point, &mut points, &mut keys);
                    }
                }
            },
            _ => return None,
        }
        (!points.is_empty()).then_some(Self { points, keys })
    }
}

pub(crate) fn push_unique_point(
    point: Point,
    points: &mut Vec<Point>,
    keys: &mut HashSet<PointKey>,
) {
    if keys.insert(PointKey::new(point)) {
        points.push(point.to_xy());
    }
}

pub(crate) fn line_is_collapsed(coords: &CoordSeq) -> bool {
    !coords.is_empty()
        && coords
            .segment_pairs()
            .all(|[start, end]| same_point(start, end))
}

pub(crate) fn multiline_is_collapsed<L: AsRef<CoordSeq>>(lines: &[L]) -> bool {
    lines.iter().any(|line| !line.as_ref().is_empty())
        && lines
            .iter()
            .flat_map(|line| line.as_ref().segment_pairs())
            .all(|[start, end]| same_point(start, end))
}

pub(crate) fn line_has_nonzero_segment(coords: &CoordSeq) -> bool {
    coords
        .segment_pairs()
        .any(|[start, end]| !same_point(start, end))
}

pub(crate) fn multiline_has_nonzero_segment<L: AsRef<CoordSeq>>(lines: &[L]) -> bool {
    lines
        .iter()
        .any(|line| line_has_nonzero_segment(line.as_ref()))
}

pub(crate) fn effective_dimension(shape: &Shape) -> Option<Dimension> {
    dimension(shape, DimMode::Effective)
}

pub(crate) fn boundary_dimension(shape: &Shape) -> Option<Dimension> {
    match shape {
        Shape::Empty(..) | Shape::Point(_) | Shape::MultiPoint(_) => None,
        Shape::LineString(_) | Shape::MultiLineString(_) => LinealOperand::from_shape(shape)
            .and_then(|line| (!line.boundary.is_empty()).then_some(Dimension::Point)),
        Shape::Polygon(_) | Shape::MultiPolygon(_) | Shape::GeometryCollection(_) => {
            RelateTopology::build(shape).boundary_dim
        },
    }
}

pub(crate) fn empty_relate(left: &Shape, right: &Shape) -> De9im {
    let mut matrix = De9im::empty_disjoint();
    if let Some(dim) = effective_dimension(left) {
        matrix.set_at_least(Loc::Interior, Loc::Exterior, dim);
    }
    if let Some(dim) = boundary_dimension(left) {
        matrix.set_at_least(Loc::Boundary, Loc::Exterior, dim);
    }
    if let Some(dim) = effective_dimension(right) {
        matrix.set_at_least(Loc::Exterior, Loc::Interior, dim);
    }
    if let Some(dim) = boundary_dimension(right) {
        matrix.set_at_least(Loc::Exterior, Loc::Boundary, dim);
    }
    matrix
}

pub(crate) fn classify_point_in_shape(shape: &Shape, point: Point) -> Loc {
    match shape {
        Shape::Empty(..) => Loc::Exterior,
        Shape::Point(value) => {
            if same_point(*value, point) {
                Loc::Interior
            } else {
                Loc::Exterior
            }
        },
        Shape::MultiPoint(points) => {
            if points.iter().any(|value| same_point(value, point)) {
                Loc::Interior
            } else {
                Loc::Exterior
            }
        },
        Shape::LineString(points) => classify_point_in_line(points, point),
        Shape::MultiLineString(lines) => classify_point_in_multiline(lines, point),
        Shape::Polygon(polygon) => classify_point_in_polygon(polygon, point),
        Shape::MultiPolygon(polygons) => {
            if polygons.iter().any(|polygon| polygon.contains_point(point)) {
                Loc::Interior
            } else if polygons.iter().any(|polygon| polygon.covers_point(point)) {
                Loc::Boundary
            } else {
                Loc::Exterior
            }
        },
        Shape::GeometryCollection(parts) => {
            let mut boundary = false;
            for part in parts {
                match classify_point_in_shape(part, point) {
                    Loc::Interior => return Loc::Interior,
                    Loc::Boundary => boundary = true,
                    Loc::Exterior => {},
                }
            }
            if boundary {
                Loc::Boundary
            } else {
                Loc::Exterior
            }
        },
    }
}

pub(crate) fn classify_point_in_line(points: &CoordSeq, point: Point) -> Loc {
    if line_is_collapsed(points) {
        if points.iter().any(|value| same_point(value, point)) {
            Loc::Interior
        } else {
            Loc::Exterior
        }
    } else if line_contains_point(points, point) {
        Loc::Interior
    } else if points
        .segment_pairs()
        .any(|[start, end]| point_on_segment(point, start, end))
    {
        Loc::Boundary
    } else {
        Loc::Exterior
    }
}

pub(crate) fn classify_point_in_multiline<L: AsRef<CoordSeq>>(lines: &[L], point: Point) -> Loc {
    if multiline_is_collapsed(lines) {
        if lines
            .iter()
            .flat_map(|line| line.as_ref().iter())
            .any(|value| same_point(value, point))
        {
            Loc::Interior
        } else {
            Loc::Exterior
        }
    } else if multiline_contains_point(lines, point) {
        Loc::Interior
    } else if lines
        .iter()
        .flat_map(|line| line.as_ref().segment_pairs())
        .any(|[start, end]| point_on_segment(point, start, end))
    {
        Loc::Boundary
    } else {
        Loc::Exterior
    }
}

pub(crate) fn classify_point_in_polygon(polygon: &Polygon, point: Point) -> Loc {
    if polygon.contains_point(point) {
        Loc::Interior
    } else if polygon.covers_point(point) {
        Loc::Boundary
    } else {
        Loc::Exterior
    }
}

pub(crate) fn puntal_relate(left: &PuntalOperand, right: &Shape) -> De9im {
    let mut matrix = De9im::empty_disjoint();
    for &point in &left.points {
        matrix.set_at_least(
            Loc::Interior,
            classify_point_in_shape(right, point),
            Dimension::Point,
        );
        if lineal_boundary_contains_point(right, point) {
            matrix.set_at_least(Loc::Interior, Loc::Boundary, Dimension::Point);
        }
    }
    if let Some(dim) = residual_interior_dim_after_removing_points(right, &left.keys) {
        matrix.set_at_least(Loc::Exterior, Loc::Interior, dim);
    }
    if let Some(dim) = residual_boundary_dim_after_removing_points(right, &left.keys) {
        matrix.set_at_least(Loc::Exterior, Loc::Boundary, dim);
    }
    matrix
}

pub(crate) fn lineal_boundary_contains_point(shape: &Shape, point: Point) -> bool {
    match shape {
        Shape::LineString(_) | Shape::MultiLineString(_) => LinealOperand::from_shape(shape)
            .is_some_and(|line| line.boundary.contains(&PointKey::new(point))),
        Shape::GeometryCollection(parts) => parts
            .iter()
            .any(|part| lineal_boundary_contains_point(part, point)),
        Shape::Empty(..)
        | Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::Polygon(_)
        | Shape::MultiPolygon(_) => false,
    }
}

pub(crate) fn residual_interior_dim_after_removing_points(
    shape: &Shape,
    keys: &HashSet<PointKey>,
) -> Option<Dimension> {
    match shape {
        Shape::Empty(..) => None,
        Shape::Point(point) => (!keys.contains(&PointKey::new(*point))).then_some(Dimension::Point),
        Shape::MultiPoint(points) => points
            .iter()
            .any(|point| !keys.contains(&PointKey::new(point)))
            .then_some(Dimension::Point),
        Shape::LineString(points) => {
            if line_has_nonzero_segment(points) {
                Some(Dimension::Curve)
            } else {
                points
                    .iter()
                    .any(|point| !keys.contains(&PointKey::new(point)))
                    .then_some(Dimension::Point)
            }
        },
        Shape::MultiLineString(lines) => {
            if multiline_has_nonzero_segment(lines) {
                Some(Dimension::Curve)
            } else {
                lines
                    .iter()
                    .flat_map(|line| line.iter())
                    .any(|point| !keys.contains(&PointKey::new(point)))
                    .then_some(Dimension::Point)
            }
        },
        Shape::Polygon(_) => Some(Dimension::Surface),
        Shape::MultiPolygon(polygons) => (!polygons.is_empty()).then_some(Dimension::Surface),
        Shape::GeometryCollection(parts) => parts
            .iter()
            .filter_map(|part| residual_interior_dim_after_removing_points(part, keys))
            .max(),
    }
}

pub(crate) fn residual_boundary_dim_after_removing_points(
    shape: &Shape,
    keys: &HashSet<PointKey>,
) -> Option<Dimension> {
    match shape {
        Shape::Empty(..) | Shape::Point(_) | Shape::MultiPoint(_) => None,
        Shape::LineString(_) | Shape::MultiLineString(_) => LinealOperand::from_shape(shape)
            .and_then(|line| {
                line.boundary
                    .iter()
                    .any(|key| !keys.contains(key))
                    .then_some(Dimension::Point)
            }),
        Shape::Polygon(_) => Some(Dimension::Curve),
        Shape::MultiPolygon(polygons) => (!polygons.is_empty()).then_some(Dimension::Curve),
        Shape::GeometryCollection(parts) => parts
            .iter()
            .filter_map(|part| residual_boundary_dim_after_removing_points(part, keys))
            .max(),
    }
}

pub(crate) fn polygon_has_nondegenerate_area(polygon: &Polygon) -> bool {
    polygon
        .rings()
        .any(|ring| ring.coord_count() >= 4 && !ring_winding(ring).is_degenerate())
}

/// One pure-lineal operand staged for the relate scan: its segment soup,
/// per-segment coverage scratch, and the OGC mod-2 boundary (part
/// endpoints with odd valence; closed parts contribute none).
pub(crate) struct LinealOperand {
    pub(crate) segments: Vec<Segment>,
    pub(crate) boundary: HashSet<PointKey>,
}

impl LinealOperand {
    pub(crate) fn from_shape(shape: &Shape) -> Option<Self> {
        let mut segments = Vec::new();
        let mut boundary = HashSet::new();
        let mut toggle = |point: XY| {
            let key = PointKey::new(point);
            if !boundary.insert(key) {
                boundary.remove(&key);
            }
        };
        let mut collect_part = |part: &CoordSeq| {
            let before = segments.len();
            for [start, end] in part.segment_pairs() {
                if !same_point(start, end) {
                    segments.push(Segment {
                        start: start.xy(),
                        end: end.xy(),
                    });
                }
            }
            if segments.len() > before {
                let first = segments[before].start;
                let last = segments[segments.len() - 1].end;
                toggle(first);
                toggle(last);
            }
        };
        match shape {
            Shape::LineString(points) => collect_part(points),
            Shape::MultiLineString(lines) => {
                for line in lines {
                    collect_part(line);
                }
            },
            _ => return None,
        }
        (!segments.is_empty()).then_some(Self { segments, boundary })
    }

    /// `point ∈ int(self)` — every point of the line off the mod-2
    /// boundary is interior.
    pub(crate) fn interior(&self, point: XY) -> bool {
        !self.boundary.contains(&PointKey::new(point))
    }

    /// Whether `point` lies ON the line at all (the boundary-row
    /// exterior tests; boundary sets are tiny, so the brute sweep is the
    /// economic structure).
    pub(crate) fn covers(&self, point: XY) -> bool {
        self.segments
            .iter()
            .any(|segment| point_on_segment(point, segment.start, segment.end))
    }
}
