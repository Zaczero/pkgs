#![expect(
    clippy::similar_names,
    reason = "axis and formula symbols name paired quantities here (minx/miny, xs/ys, lat_rad/lam_rad, step/stop), which this lint reads as typos"
)]
use crate::error::Result;
#[cfg(test)]
use crate::geometry::dedup_consecutive_points;
use crate::geometry::exact_arith::ExactSign;
#[cfg(test)]
use crate::geometry::orientation_xy;
use crate::geometry::{
    Bounds, BulkRTree, CoordSeqBuilder, CoordinateAxes, Coordinates, GeometryErrorKind, HasM, HasZ,
    HashMap, HashSet, MOrdinate, Orientation, Point, PointKey, Polygon, Ring, RingWinding, Segment,
    SegmentIndex, Shape, XY, ZOrdinate, open_point_cycle_winding, orientation, point_on_segment,
    ring_contains_interior, same_point, segments_intersect, wrap_index,
};
mod admission;
mod bridges;
mod linked;
mod normalize;

#[cfg(test)]
pub(super) use admission::ring_has_contact_box_scan;
pub(super) use admission::ring_segments_adjacent;
use admission::{EarcutCycle, HoleId, HoleList, RingVertexId};
pub(crate) use admission::{
    validate_earcut_ring, validate_hole_inside_shell, validate_hole_interactions,
};
use bridges::eliminate_holes_with_ids;
pub(super) use bridges::point_in_triangle;
pub(crate) use linked::earcut_linked_ring_with_ids;
#[cfg(test)]
pub(super) use linked::{count_ear_candidates, ear_scan_steps};
#[cfg(test)]
pub(crate) use normalize::{
    cleaned_earcut_ring, filter_earcut_ring_points, orient_earcut_ring, strip_collinear_vertices,
};
use normalize::{cleaned_earcut_ring_with_ids, source_identities};
pub(crate) use normalize::{closed_ring, open_ring};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct EarcutSiteId {
    pub(crate) ring: usize,
    pub(crate) vertex: usize,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct EarcutOccurrenceId(pub(crate) usize);

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct EarcutCornerId {
    pub(crate) site: EarcutSiteId,
    pub(crate) occurrence: EarcutOccurrenceId,
}

impl EarcutCornerId {
    pub(super) const fn source(ring: usize, vertex: usize, occurrence: usize) -> Self {
        Self {
            site: EarcutSiteId { ring, vertex },
            occurrence: EarcutOccurrenceId(occurrence),
        }
    }

    pub(super) const fn copied(self, occurrence: usize) -> Self {
        Self {
            site: self.site,
            occurrence: EarcutOccurrenceId(occurrence),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct EarcutTriangle {
    pub(crate) face: usize,
    pub(crate) points: [Point; 3],
    pub(crate) corners: [EarcutCornerId; 3],
}

/// Walk a polygon's ear-cut triangles through one caller-owned sink. The
/// generated-output budget lives at that caller, so it can reject before a
/// triangle shape or array row is emitted.
pub(crate) fn earcut_polygon_with(
    polygon: &Polygon,
    emit: &mut impl FnMut(Point, Point, Point) -> Result<()>,
) -> Result<()> {
    earcut_polygon_with_ids(polygon, &mut |triangle| {
        let [a, b, c] = triangle.points;
        emit(a, b, c)
    })
}

/// Emit the ear selection with original ring-site and live occurrence IDs.
/// Bridge copies keep their source site but receive independent occurrences.
pub(crate) fn earcut_polygon_with_ids(
    polygon: &Polygon,
    emit: &mut impl FnMut(EarcutTriangle) -> Result<()>,
) -> Result<()> {
    let mut next_occurrence = 0_usize;
    let (mut ring, mut ring_ids) =
        cleaned_earcut_ring_with_ids(&polygon.shell, false, 0, &mut next_occurrence)?;
    if ring.len() < 3 || open_point_cycle_winding(&ring).is_degenerate() {
        return Ok(());
    }

    let mut holes = Vec::with_capacity(polygon.holes.len());
    let mut hole_ids = Vec::with_capacity(polygon.holes.len());
    for (hole_index, hole) in polygon.holes.iter().enumerate() {
        let (hole, ids) = cleaned_earcut_ring_with_ids(
            hole,
            true,
            hole_index.wrapping_add(1),
            &mut next_occurrence,
        )?;
        if hole.len() < 3 || open_point_cycle_winding(&hole).is_degenerate() {
            return Err(GeometryErrorKind::triangulation("polygon hole degenerates"));
        }
        validate_hole_inside_shell(&ring, &hole)?;
        holes.push(hole);
        hole_ids.push(ids);
    }
    validate_hole_interactions(&ring, &holes)?;
    eliminate_holes_with_ids(&mut ring, &mut ring_ids, &holes, &hole_ids, next_occurrence)?;
    earcut_linked_ring_with_ids(&ring, &ring_ids, emit)
}

/// Closed ear-clip triangle whose axes are the intersection of the three
/// source corners — drop an unshared ordinate, never invent one.
pub(crate) fn try_triangle_shape(a: Point, b: Point, c: Point) -> Result<Shape> {
    let has_z = a.axes.has_z() && b.axes.has_z() && c.axes.has_z();
    let has_m = a.axes.has_m() && b.axes.has_m() && c.axes.has_m();
    let coerce = |point: Point| {
        Point::new_raw_axes(
            point.x,
            point.y,
            ZOrdinate(has_z.then_some(point.z)),
            MOrdinate(has_m.then_some(point.m)),
        )
    };
    let (a, b, c) = (coerce(a), coerce(b), coerce(c));
    let axes = CoordinateAxes::new(HasZ(has_z), HasM(has_m));
    let mut builder = CoordSeqBuilder::with_capacity(axes, 4);
    builder.push(a);
    builder.push(b);
    builder.push(c);
    builder.push(a);
    Ok(Shape::Polygon(Polygon::new(
        Ring::try_from_closed(builder.finish()?)?,
        Vec::new(),
    )))
}

pub(crate) fn point_in_ccw_triangle(
    point: impl Into<XY>,
    a: impl Into<XY>,
    b: impl Into<XY>,
    c: impl Into<XY>,
) -> bool {
    let (point, a, b, c) = (point.into(), a.into(), b.into(), c.into());
    orientation(a, b, point) != Orientation::Clockwise
        && orientation(b, c, point) != Orientation::Clockwise
        && orientation(c, a, point) != Orientation::Clockwise
}
