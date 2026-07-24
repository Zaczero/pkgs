#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use crate::geometry::*;

/// Whether the geodesic short path between a segment's endpoints crosses the
/// antimeridian, i.e. the lon/lat (planar) representation takes the long way
/// round and can disagree with the true geodesic about crossings.
pub(crate) fn spans_antimeridian(segment: Segment) -> bool {
    (segment.start.x - segment.end.x).abs() > 180.0
}

/// Longitude delta wrapped to ``(-180, 180]`` for pole-winding accumulation.
fn wrap_longitude_delta(delta: f64) -> f64 {
    let mut wrapped = delta % 360.0;
    if wrapped <= -180.0 {
        wrapped += 360.0;
    }
    if wrapped > 180.0 {
        wrapped -= 360.0;
    }
    wrapped
}

/// Total signed longitude winding of a closed ring (sum of per-edge wrapped
/// ``Δlon``). A geographic ring that encircles a pole winds ``≈ ±360``; a
/// normal ring winds ``≈ 0``.
fn ring_longitude_winding<C: Coordinates + ?Sized>(ring: &C) -> f64 {
    ring.segment_pairs()
        .map(|[start, end]| wrap_longitude_delta(end.x - start.x))
        .sum()
}

fn ring_longitude_winding_is_polar<C: Coordinates + ?Sized>(ring: &C) -> bool {
    const TOL: f64 = 1e-6;
    let winding = ring_longitude_winding(ring);
    (winding - 360.0).abs() < TOL || (winding + 360.0).abs() < TOL
}

fn ring_lat_extents<C: Coordinates + ?Sized>(ring: &C) -> (f64, f64) {
    let mut min_y = f64::INFINITY;
    let mut max_y = f64::NEG_INFINITY;
    for coord in ring.iter_coords() {
        min_y = min_y.min(coord.y);
        max_y = max_y.max(coord.y);
    }
    (min_y, max_y)
}

/// Whether a ring encircles the north (``north == true``) or south pole via
/// full-longitude winding at high latitude.
pub(crate) fn ring_encloses_pole<C: Coordinates + ?Sized>(ring: &C, north: bool) -> bool {
    if !ring_longitude_winding_is_polar(ring) {
        return false;
    }
    let (min_y, max_y) = ring_lat_extents(ring);
    if north { min_y > 0.0 } else { max_y < 0.0 }
}

fn polygon_encloses_pole(polygon: &Polygon, north: bool) -> bool {
    if !ring_encloses_pole(polygon.shell.coords(), north) {
        return false;
    }
    !polygon
        .holes
        .iter()
        .any(|hole| ring_encloses_pole(hole.coords(), north))
}

/// Whether an areal ``shape`` encloses ``north``/south pole (shell/hole parity).
pub(crate) fn shape_encloses_pole(shape: &Shape, north: bool) -> bool {
    match shape {
        Shape::Polygon(polygon) => polygon_encloses_pole(polygon, north),
        Shape::MultiPolygon(polygons) => polygons
            .iter()
            .any(|polygon| polygon_encloses_pole(polygon, north)),
        Shape::GeometryCollection(parts) => {
            parts.iter().any(|part| shape_encloses_pole(part, north))
        },
        _ => false,
    }
}

/// Whether any areal ring winds around a pole, including a hole that removes
/// the pole from the final surface.  This differs deliberately from
/// [`shape_encloses_pole`]: a polar annulus contains neither pole, yet still
/// spans every longitude between its shell and polar hole and therefore needs
/// a full-longitude conservative envelope.
pub(crate) fn shape_has_polar_ring(shape: &Shape) -> bool {
    let polygon_has_polar_ring = |polygon: &Polygon| {
        ring_longitude_winding_is_polar(polygon.shell.coords())
            || polygon
                .holes
                .iter()
                .any(|hole| ring_longitude_winding_is_polar(hole.coords()))
    };
    match shape {
        Shape::Polygon(polygon) => polygon_has_polar_ring(polygon),
        Shape::MultiPolygon(polygons) => polygons.iter().any(polygon_has_polar_ring),
        Shape::GeometryCollection(parts) => parts.iter().any(shape_has_polar_ring),
        _ => false,
    }
}

/// Where a geographic pole sits relative to an areal shape: strictly inside,
/// ON the boundary ring, or outside. Distinct from [`shape_encloses_pole`]
/// (which is strict interior) because a ring with a *vertex at the pole* puts
/// the pole on the boundary — at a pole every longitude collapses to one point,
/// so a pole vertex of any longitude is coincident with the probe pole.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum PolePosition {
    Interior,
    Boundary,
    Exterior,
}

#[expect(
    clippy::float_cmp,
    reason = "only an exact ±90 vertex is the pole; any other latitude is a real value"
)]
fn ring_has_pole_vertex<C: Coordinates + ?Sized>(ring: &C, pole_lat: f64) -> bool {
    ring.iter_coords().any(|coord| coord.y == pole_lat)
}

fn polygon_has_pole_vertex(polygon: &Polygon, pole_lat: f64) -> bool {
    ring_has_pole_vertex(polygon.shell.coords(), pole_lat)
        || polygon
            .holes
            .iter()
            .any(|hole| ring_has_pole_vertex(hole.coords(), pole_lat))
}

fn polygon_pole_position(polygon: &Polygon, north: bool) -> PolePosition {
    let pole_lat = if north { 90.0 } else { -90.0 };
    if polygon_has_pole_vertex(polygon, pole_lat) {
        PolePosition::Boundary
    } else if polygon_encloses_pole(polygon, north) {
        PolePosition::Interior
    } else {
        PolePosition::Exterior
    }
}

/// Classify a geographic pole against an areal ``shape``. Within one polygon,
/// boundary takes precedence over interior (a pole vertex on an
/// otherwise-enclosing ring is a boundary touch); across collection members,
/// union semantics make interior dominate boundary. Non-areal shapes are
/// always [`PolePosition::Exterior`] (they have no interior the enclosure/cap
/// reasoning applies to).
pub(crate) fn pole_position(shape: &Shape, north: bool) -> PolePosition {
    match shape {
        Shape::Polygon(polygon) => polygon_pole_position(polygon, north),
        Shape::MultiPolygon(polygons) => collection_pole_position(
            polygons
                .iter()
                .map(|polygon| polygon_pole_position(polygon, north)),
        ),
        Shape::GeometryCollection(parts) => {
            collection_pole_position(parts.iter().map(|part| pole_position(part, north)))
        },
        _ => PolePosition::Exterior,
    }
}

/// Union semantics for multi/collection areal parts: any interior dominates a
/// boundary supplied by another member; boundary dominates exterior. A global
/// "any pole vertex" precheck inverted that order for overlapping collections.
fn collection_pole_position(positions: impl IntoIterator<Item = PolePosition>) -> PolePosition {
    let mut result = PolePosition::Exterior;
    for position in positions {
        match position {
            PolePosition::Interior => return PolePosition::Interior,
            PolePosition::Boundary => result = PolePosition::Boundary,
            PolePosition::Exterior => {},
        }
    }
    result
}

/// Whether ``point`` sits at a geographic pole (``Some(true)`` north,
/// ``Some(false)`` south, ``None`` otherwise).
#[expect(
    clippy::float_cmp,
    reason = "only the literal ±90 inputs are the poles; anything else is a real latitude"
)]
pub(crate) fn point_is_geographic_pole(point: Point) -> Option<bool> {
    if point.y == 90.0 {
        Some(true)
    } else if point.y == -90.0 {
        Some(false)
    } else {
        None
    }
}
