#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use crate::geometry::overlay::{OverlayOp, window_clip_shape};
use crate::geometry::{Bounds, CoordSeq, Coordinates as _, Point, Polygon, Ring, Shape, ShapeData};
#[derive(Clone, Debug, Default)]
pub(crate) struct DimensionalParts<'a> {
    pub(crate) points: Vec<Point>,
    pub(crate) lines: Vec<&'a CoordSeq>,
    pub(crate) polygons: Vec<&'a Polygon>,
}

pub(crate) enum DimensionalComponent<'a> {
    Point(Point),
    Line(&'a CoordSeq),
    Polygon(&'a Polygon),
}

impl<'a> DimensionalParts<'a> {
    pub(crate) const POINT_BIT: u8 = 1 << 0;
    pub(crate) const LINE_BIT: u8 = 1 << 1;
    pub(crate) const POLYGON_BIT: u8 = 1 << 2;

    pub(crate) fn from_shape(shape: &'a Shape) -> Self {
        let mut parts = Self::default();
        parts.push_shape(shape);
        parts
    }

    pub(crate) fn push_shape(&mut self, shape: &'a Shape) {
        Self::for_each_component(shape, &mut |component| match component {
            DimensionalComponent::Point(point) => self.points.push(point),
            DimensionalComponent::Line(line) => self.lines.push(line),
            DimensionalComponent::Polygon(polygon) => self.polygons.push(polygon),
        });
    }

    pub(crate) fn non_empty_dimension_mask(shape: &Shape) -> u8 {
        let mut mask = 0_u8;
        Self::for_each_component(shape, &mut |component| match component {
            DimensionalComponent::Point(_) => mask |= Self::POINT_BIT,
            DimensionalComponent::Line(line) if !line.is_empty() => mask |= Self::LINE_BIT,
            DimensionalComponent::Line(_) => {},
            DimensionalComponent::Polygon(_) => mask |= Self::POLYGON_BIT,
        });
        mask
    }

    fn for_each_component<'s>(shape: &'s Shape, visit: &mut impl FnMut(DimensionalComponent<'s>)) {
        match shape {
            Shape::Point(point) => visit(DimensionalComponent::Point(*point)),
            Shape::MultiPoint(points) => {
                for point in points.iter_coords() {
                    visit(DimensionalComponent::Point(point));
                }
            },
            Shape::LineString(line) => visit(DimensionalComponent::Line(line)),
            Shape::MultiLineString(lines) => {
                for line in lines {
                    visit(DimensionalComponent::Line(line));
                }
            },
            Shape::Polygon(polygon) => visit(DimensionalComponent::Polygon(polygon)),
            Shape::MultiPolygon(polygons) => {
                for polygon in polygons {
                    visit(DimensionalComponent::Polygon(polygon));
                }
            },
            Shape::GeometryCollection(geometries) => {
                for geometry in geometries {
                    Self::for_each_component(geometry, visit);
                }
            },
            Shape::Empty(..) => {},
        }
    }
}

/// Evaluate the polygon bucket of an overlay via `geo`'s robust boolean ops.
/// Binary areal overlay on the [`Arrangement`] core: both operands' rings
/// node into ONE arrangement whose faces carry a winding number PER
/// OPERAND (`[left, right]`), and the op keeps faces by predicate —
/// shells walk CCW and holes CW per operand, so interior means
/// `winding >= 1` on each side independently. Input vertices are
/// preserved BIT-EXACTLY: only true crossings mint new points (never
/// `i_overlay`'s snapped grid).
/// OverlayNG-style window clipping: intersection only sees the envelope
/// overlap, and a difference's subtrahend only matters inside the
/// minuend's envelope — clipping a LARGE operand to the (margin-expanded)
/// relevant window first collapses the per-pair noding from O(operand)
/// to O(local neighborhood). The margin keeps every true crossing
/// strictly inside the clip, so results agree up to the engine's usual
/// placement tolerance. Returns the replacement operands (`None` =
/// untouched).
pub(crate) fn window_clip_large_operands(
    left: &Shape,
    right: &Shape,
    op: OverlayOp,
) -> (Option<Shape>, Option<Shape>) {
    let mut clipped_left = None;
    let mut clipped_right = None;
    if let (Some(left_bounds), Some(right_bounds)) = (left.bounds(), right.bounds()) {
        let window = |target: Bounds| -> Option<Bounds> {
            let margin = 0.01
                * (target.maxx() - target.minx())
                    .max(target.maxy() - target.miny())
                    .max(1e-9);
            Bounds::new(
                target.minx() - margin,
                target.miny() - margin,
                target.maxx() + margin,
                target.maxy() + margin,
            )
            .ok()
        };
        let outside = |bounds: Bounds, window: Bounds| {
            bounds.minx() < window.minx()
                || bounds.maxx() > window.maxx()
                || bounds.miny() < window.miny()
                || bounds.maxy() > window.maxy()
        };
        if op == OverlayOp::Intersection {
            if left.coord_count() >= 128
                && let Some(window) = window(right_bounds)
                && outside(left_bounds, window)
            {
                clipped_left = Some(window_clip_shape(left, window));
            }
            if right.coord_count() >= 128
                && let Some(window) = window(left_bounds)
                && outside(right_bounds, window)
            {
                clipped_right = Some(window_clip_shape(right, window));
            }
        } else if op == OverlayOp::Difference
            && right.coord_count() >= 128
            && let Some(window) = window(left_bounds)
            && outside(right_bounds, window)
        {
            clipped_right = Some(window_clip_shape(right, window));
        }
    }
    (clipped_left, clipped_right)
}

/// [`Shape`]'s contained gate on PREPARED handles: the containment probe
/// runs the cached engines, so a broadcast's fixed operand builds its
/// facet trees and point tester ONCE across every pair instead of per
/// pair.
pub(crate) fn contained_shortcut_cached(
    left: &ShapeData,
    right: &ShapeData,
    op: OverlayOp,
) -> Option<Shape> {
    contained_shortcut_impl(
        left.shape(),
        right.shape(),
        op,
        left.bounds(),
        right.bounds(),
        |left_is_outer| {
            if left_is_outer {
                left.contains_properly_cached_for(
                    right,
                    crate::geometry::PointProbeUse::OneShot(
                        crate::geometry::vertex_witness_probe_count(right.shape()),
                    ),
                )
            } else {
                right.contains_properly_cached_for(
                    left,
                    crate::geometry::PointProbeUse::OneShot(
                        crate::geometry::vertex_witness_probe_count(left.shape()),
                    ),
                )
            }
        },
    )
}

/// The shared gate body: pre-filters, then `probe(left_is_outer)` decides
/// containment with whatever engine the caller owns.
pub(crate) fn contained_shortcut_impl(
    left: &Shape,
    right: &Shape,
    op: OverlayOp,
    left_bounds: Option<Bounds>,
    right_bounds: Option<Bounds>,
    probe: impl Fn(bool) -> bool,
) -> Option<Shape> {
    if !(matches!(left, Shape::Polygon(_) | Shape::MultiPolygon(_))
        && matches!(right, Shape::Polygon(_) | Shape::MultiPolygon(_))
        && left.coord_count().max(right.coord_count()) >= 128)
    {
        return None;
    }
    let (left_bounds, right_bounds) = (left_bounds?, right_bounds?);
    let strictly_inside = |inner: Bounds, outer: Bounds| {
        inner.minx() > outer.minx()
            && inner.maxx() < outer.maxx()
            && inner.miny() > outer.miny()
            && inner.maxy() < outer.maxy()
    };
    if strictly_inside(right_bounds, left_bounds) && probe(true) {
        return Some(match op {
            OverlayOp::Intersection => right.clone(),
            OverlayOp::Union => left.clone(),
            OverlayOp::Difference | OverlayOp::SymmetricDifference => punch_holes(left, right),
        });
    }
    if strictly_inside(left_bounds, right_bounds) && probe(false) {
        return Some(match op {
            OverlayOp::Intersection => left.clone(),
            OverlayOp::Union => right.clone(),
            OverlayOp::Difference => Shape::empty_polygon(),
            OverlayOp::SymmetricDifference => punch_holes(right, left),
        });
    }
    None
}

/// `minuend ∖ subtrahend` when the subtrahend lies STRICTLY inside the
/// minuend's interior (no linework contact): every subtrahend shell
/// punches into its owning minuend part as a hole, and every subtrahend
/// hole surfaces back as an island — exact, with zero noding.
pub(crate) fn punch_holes(minuend: &Shape, subtrahend: &Shape) -> Shape {
    let mut parts: Vec<Polygon> = match minuend {
        Shape::Polygon(polygon) => vec![polygon.clone()],
        Shape::MultiPolygon(polygons) => polygons.clone(),
        _ => unreachable!("gated areal"),
    };
    let mut islands: Vec<Polygon> = Vec::new();
    let punch = |parts: &mut Vec<Polygon>, islands: &mut Vec<Polygon>, sub: &Polygon| {
        // The subtrahend is strictly interior, so its first shell vertex
        // is strictly inside exactly one part — the hole's owner.
        let probe = sub.shell.coords().point_at(0);
        let owner = parts
            .iter_mut()
            .find(|part| part.contains_point(probe))
            .expect("contained subtrahend has an owning part");
        let mut holes = owner.holes.to_vec();
        holes.push(Ring::from_trusted_closed(
            crate::geometry::constructive::orient_ring(sub.shell.coords(), true),
        ));
        owner.holes = holes.into();
        for hole in sub.holes.iter() {
            islands.push(Polygon::new(
                Ring::from_trusted_closed(crate::geometry::constructive::orient_ring(
                    hole.coords(),
                    false,
                )),
                Vec::new(),
            ));
        }
    };
    match subtrahend {
        Shape::Polygon(polygon) => punch(&mut parts, &mut islands, polygon),
        Shape::MultiPolygon(polygons) => {
            for polygon in polygons {
                punch(&mut parts, &mut islands, polygon);
            }
        },
        _ => unreachable!("gated areal"),
    }
    parts.extend(islands);
    if parts.len() == 1 {
        Shape::Polygon(parts.pop().expect("one part"))
    } else {
        Shape::MultiPolygon(parts)
    }
}
