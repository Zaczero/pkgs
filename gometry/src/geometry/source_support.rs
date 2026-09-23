//! Exact Cartesian source support for ordinary point queries.
//!
//! This module owns the source-side fold used by scalar, packed, prepared,
//! and indexed point membership.  It deliberately does not build an
//! arrangement: away from source contact, directed winding is locally
//! constant; at contact, the only relevant topology is the exact fan of
//! source rays incident to the queried stored point.

#![expect(
    clippy::float_cmp,
    reason = "coordinate identity and formal fan ordering use exact represented values"
)]

use std::cmp::Ordering;
use std::ops::Range;

use crate::geometry::exact_arith::ExactSign;
use crate::geometry::segments::stored_orientation_sign;
use crate::geometry::{Loc, Point, Polygon, Ring, Shape, XY, point_on_segment, same_point};

/// Whether a stored ring supplies areal winding or collapsed line support.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RingKind {
    Winding,
    Collapsed,
}

#[derive(Clone, Copy)]
pub(crate) struct RingView<'a> {
    pub(crate) xs: &'a [f64],
    pub(crate) ys: &'a [f64],
    pub(crate) kind: RingKind,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RingProbe {
    pub(crate) winding: i64,
    pub(crate) incident: bool,
}

struct CurveView<'a> {
    xs: &'a [f64],
    ys: &'a [f64],
}

struct SourceSupport<'a> {
    rings: Vec<RingView<'a>>,
    polygons: Vec<Range<usize>>,
    curves: Vec<CurveView<'a>>,
    points: Vec<XY>,
}

struct Ray {
    end: XY,
    ring: Option<(usize, i64)>,
    curve: bool,
    collapsed: bool,
}

struct RayGroup {
    start: usize,
    end: usize,
}

impl<'a> SourceSupport<'a> {
    const fn new() -> Self {
        Self {
            rings: Vec::new(),
            polygons: Vec::new(),
            curves: Vec::new(),
            points: Vec::new(),
        }
    }

    fn from_shape(shape: &'a Shape) -> Self {
        let mut source = Self::new();
        source.append_shape(shape);
        source
    }

    fn append_shape(&mut self, shape: &'a Shape) {
        match shape {
            Shape::Point(point) => {
                self.points.push(point.xy());
            },
            Shape::MultiPoint(points) => {
                self.points.reserve(points.len());
                self.points.extend(points.points().map(|point| point.xy()));
            },
            Shape::LineString(line) => self.append_curve(line.as_coords()),
            Shape::MultiLineString(lines) => {
                for line in lines {
                    self.append_curve(line.as_coords());
                }
            },
            Shape::Polygon(polygon) => self.append_polygon(polygon),
            Shape::MultiPolygon(polygons) => {
                for polygon in polygons {
                    self.append_polygon(polygon);
                }
            },
            Shape::GeometryCollection(geometries) => {
                for geometry in geometries {
                    self.append_shape(geometry);
                }
            },
            Shape::Empty(..) => {},
        }
    }

    fn append_polygon(&mut self, polygon: &'a Polygon) {
        let first = self.rings.len();
        self.rings.reserve(polygon.holes.len().wrapping_add(1));
        for ring in polygon.rings() {
            self.rings.push(RingView {
                xs: ring.xs(),
                ys: ring.ys(),
                kind: ring_kind(ring),
            });
        }
        self.polygons.push(first..self.rings.len());
    }

    fn append_curve(&mut self, curve: &'a crate::geometry::CoordSeq) {
        self.curves.push(CurveView {
            xs: curve.xs(),
            ys: curve.ys(),
        });
        if !has_nonzero_segment(curve.xs(), curve.ys()) {
            self.points.reserve(curve.len());
            for (&x, &y) in curve.xs().iter().zip(curve.ys()) {
                self.points.push(XY::new(x, y));
            }
        }
    }

    fn location(&self, point: Point) -> Loc {
        let query = point.xy();
        let mut windings = Vec::with_capacity(self.rings.len());
        let mut incident = false;
        for ring in &self.rings {
            let probe = ring_probe_columns(ring.xs, ring.ys, query);
            windings.push(if ring.kind == RingKind::Winding {
                probe.winding
            } else {
                0
            });
            incident |= probe.incident;
        }

        let explicit_point = self
            .points
            .iter()
            .any(|&candidate| same_point(candidate, query));
        let mut collapsed_points = Vec::new();
        let mut rays = Vec::new();
        for (ring_id, ring) in self.rings.iter().enumerate() {
            for_each_ring_segment(ring.xs, ring.ys, |start, end| {
                if same_point(start, end) {
                    if ring.kind == RingKind::Collapsed
                        && same_point(start, query)
                        && collapsed_points.last().copied() != Some(ring_id)
                    {
                        collapsed_points.push(ring_id);
                    }
                    return;
                }
                if !point_on_segment(query.point(), start, end) {
                    return;
                }
                incident = true;
                if !same_point(query, end) {
                    rays.push(Ray {
                        end,
                        ring: Some((ring_id, i64::from(ring.kind == RingKind::Winding))),
                        curve: false,
                        collapsed: ring.kind == RingKind::Collapsed,
                    });
                }
                if !same_point(query, start) {
                    rays.push(Ray {
                        end: start,
                        ring: Some((ring_id, -i64::from(ring.kind == RingKind::Winding))),
                        curve: false,
                        collapsed: ring.kind == RingKind::Collapsed,
                    });
                }
            });
        }
        for curve in &self.curves {
            for_each_xy_segment(curve.xs, curve.ys, |start, end| {
                if same_point(start, end) {
                    return;
                }
                if !point_on_segment(query.point(), start, end) {
                    return;
                }
                incident = true;
                if !same_point(query, end) {
                    rays.push(Ray {
                        end,
                        ring: None,
                        curve: true,
                        collapsed: false,
                    });
                }
                if !same_point(query, start) {
                    rays.push(Ray {
                        end: start,
                        ring: None,
                        curve: true,
                        collapsed: false,
                    });
                }
            });
        }

        if !incident {
            return self.location_without_contact(&windings, query, explicit_point);
        }

        rays.sort_unstable_by(|left, right| compare_rays(query, left.end, right.end));
        let groups = ray_groups(query, &rays);
        let endpoint_parity = self.curve_endpoint_parity(query);
        let mut state = LocalStar {
            source: self,
            query,
            windings,
            rays,
            groups,
            endpoint_parity,
            explicit_point,
            collapsed_points,
        };
        state.location()
    }

    fn area_covers(&self, point: Point) -> bool {
        let query = point.xy();
        let mut windings = Vec::with_capacity(self.rings.len());
        let mut incident = false;
        for ring in &self.rings {
            let probe = ring_probe_columns(ring.xs, ring.ys, query);
            incident |= probe.incident;
            windings.push(if ring.kind == RingKind::Winding {
                probe.winding
            } else {
                0
            });
        }
        if !incident {
            return self.area_occupied(&windings);
        }

        let mut rays = Vec::new();
        for (ring_id, ring) in self.rings.iter().enumerate() {
            for_each_ring_segment(ring.xs, ring.ys, |start, end| {
                if same_point(start, end) || !point_on_segment(query.point(), start, end) {
                    return;
                }
                if !same_point(query, end) {
                    rays.push(Ray {
                        end,
                        ring: Some((ring_id, i64::from(ring.kind == RingKind::Winding))),
                        curve: false,
                        collapsed: ring.kind == RingKind::Collapsed,
                    });
                }
                if !same_point(query, start) {
                    rays.push(Ray {
                        end: start,
                        ring: Some((ring_id, -i64::from(ring.kind == RingKind::Winding))),
                        curve: false,
                        collapsed: ring.kind == RingKind::Collapsed,
                    });
                }
            });
        }
        if rays.is_empty() {
            return false;
        }
        rays.sort_unstable_by(|left, right| compare_rays(query, left.end, right.end));
        let groups = ray_groups(query, &rays);
        let mut star = LocalStar {
            source: self,
            query,
            windings,
            rays,
            groups,
            endpoint_parity: false,
            explicit_point: false,
            collapsed_points: Vec::new(),
        };
        matches!(star.location(), Loc::Interior | Loc::Boundary)
    }

    fn location_without_contact(&self, windings: &[i64], query: XY, explicit_point: bool) -> Loc {
        if self.area_occupied(windings) {
            return Loc::Interior;
        }
        let curve = self
            .curves
            .iter()
            .any(|curve| curve_has_point(curve.xs, curve.ys, query));
        let collapsed = self.collapsed_polygon_support(windings, query);
        if curve || collapsed {
            return line_location(curve, collapsed, self.curve_endpoint_parity(query), 0);
        }
        if explicit_point {
            Loc::Interior
        } else {
            Loc::Exterior
        }
    }

    fn area_occupied(&self, windings: &[i64]) -> bool {
        self.polygons.iter().any(|rings| {
            let mut rings = rings.clone();
            rings.next().is_some_and(|shell| windings[shell] != 0)
                && !rings.any(|hole| windings[hole] != 0)
        })
    }

    fn collapsed_polygon_support(&self, windings: &[i64], query: XY) -> bool {
        self.polygons.iter().any(|rings| {
            let mut rings = rings.clone();
            let Some(shell) = rings.next() else {
                return false;
            };
            if self.rings[shell].kind != RingKind::Collapsed
                || !ring_has_point(self.rings[shell].xs, self.rings[shell].ys, query)
            {
                return false;
            }
            !rings.any(|hole| {
                if self.rings[hole].kind == RingKind::Winding {
                    windings[hole] != 0
                } else {
                    ring_has_point(self.rings[hole].xs, self.rings[hole].ys, query)
                }
            })
        })
    }

    fn curve_endpoint_parity(&self, query: XY) -> bool {
        self.curves.iter().fold(false, |parity, curve| {
            if !has_nonzero_segment(curve.xs, curve.ys) {
                return parity;
            }
            let Some((&first_x, &first_y)) = curve.xs.first().zip(curve.ys.first()) else {
                return parity;
            };
            let Some((&last_x, &last_y)) = curve.xs.last().zip(curve.ys.last()) else {
                return parity;
            };
            parity
                ^ (same_point(XY::new(first_x, first_y), query)
                    ^ same_point(XY::new(last_x, last_y), query))
        })
    }
}

struct LocalStar<'a, 'b> {
    source: &'a SourceSupport<'b>,
    query: XY,
    windings: Vec<i64>,
    rays: Vec<Ray>,
    groups: Vec<RayGroup>,
    endpoint_parity: bool,
    explicit_point: bool,
    collapsed_points: Vec<usize>,
}

impl LocalStar<'_, '_> {
    fn location(&mut self) -> Loc {
        if self.groups.is_empty() {
            return self.location_without_rays();
        }
        for (winding, ring) in self.windings.iter_mut().zip(&self.source.rings) {
            *winding = if ring.kind == RingKind::Winding {
                formal_ring_winding(ring.xs, ring.ys, self.query)
            } else {
                0
            };
        }

        let seed_group = self.groups.iter().position(|group| {
            let ray = &self.rays[group.start];
            ray.end.y == self.query.y && ray.end.x > self.query.x
        });
        let first = if let Some(index) = seed_group {
            (index + 1) % self.groups.len()
        } else {
            self.groups
                .iter()
                .position(|group| {
                    let ray = &self.rays[group.start];
                    ray.end.y > self.query.y
                        || (ray.end.y == self.query.y && ray.end.x > self.query.x)
                })
                .unwrap_or(0)
        };
        let mut sector_area = Vec::with_capacity(self.groups.len());
        let mut ray_polygon_support = false;
        let mut curve_ray = false;
        let mut residual_collapsed_degree = 0_usize;
        let mut ring_support = Vec::new();
        if !self.collapsed_points.is_empty() {
            ring_support.resize(self.source.rings.len(), false);
            for (ring, &winding) in self.windings.iter().enumerate() {
                ring_support[ring] |= winding != 0;
            }
            for &ring in &self.collapsed_points {
                ring_support[ring] = true;
            }
            for ray in &self.rays {
                if let Some((ring, _)) = ray.ring
                    && ray.collapsed
                {
                    ring_support[ring] = true;
                }
            }
        }

        let mut group_values = Vec::new();
        for offset in 0..self.groups.len() {
            let group_index = (first + offset) % self.groups.len();
            let group = &self.groups[group_index];
            let before_area = self.source.area_occupied(&self.windings);
            self.group_ring_values(group, &mut group_values);
            let group_curve = self.rays[group.start..group.end]
                .iter()
                .any(|ray| ray.curve);
            curve_ray |= group_curve;
            for ray in &self.rays[group.start..group.end] {
                if let Some((ring, delta)) = ray.ring {
                    self.windings[ring] += delta;
                    if !ring_support.is_empty() && self.source.rings[ring].kind == RingKind::Winding
                    {
                        ring_support[ring] |= self.windings[ring] != 0;
                    }
                }
            }
            let after_area = self.source.area_occupied(&self.windings);
            sector_area.push(before_area);
            let group_support =
                self.polygon_ray_support(group, &group_values, before_area, after_area);
            ray_polygon_support |= group_support;
            if group_support && !group_curve {
                residual_collapsed_degree += 1;
            }
        }

        let any_area = sector_area.iter().copied().any(|occupied| occupied);
        if any_area {
            return if sector_area.iter().copied().all(|occupied| occupied) {
                Loc::Interior
            } else {
                Loc::Boundary
            };
        }
        let line = curve_ray || ray_polygon_support;
        if line {
            return line_location(
                curve_ray,
                ray_polygon_support,
                self.endpoint_parity,
                residual_collapsed_degree,
            );
        }
        if !ring_support.is_empty() && self.collapsed_point_support(&ring_support) {
            return Loc::Interior;
        }
        if self.explicit_point {
            Loc::Interior
        } else {
            Loc::Exterior
        }
    }

    fn location_without_rays(&self) -> Loc {
        if self.source.area_occupied(&self.windings) || self.explicit_point {
            Loc::Interior
        } else {
            Loc::Exterior
        }
    }

    fn collapsed_point_support(&self, ring_support: &[bool]) -> bool {
        self.source.polygons.iter().any(|rings| {
            let mut rings = rings.clone();
            let Some(shell) = rings.next() else {
                return false;
            };
            self.collapsed_points.contains(&shell) && !rings.any(|hole| ring_support[hole])
        })
    }

    fn group_ring_values(&self, group: &RayGroup, values: &mut Vec<(usize, i64)>) {
        values.clear();
        for ray in &self.rays[group.start..group.end] {
            let Some((ring, _)) = ray.ring else {
                continue;
            };
            if values.iter().any(|&(candidate, _)| candidate == ring) {
                continue;
            }
            values.push((ring, self.windings[ring]));
        }
    }

    fn ring_value(
        &self,
        ring: usize,
        group: &RayGroup,
        before: &[(usize, i64)],
        after: bool,
    ) -> bool {
        let value = before
            .iter()
            .find_map(|&(candidate, value)| (candidate == ring).then_some(value))
            .map_or(self.windings[ring], |value| {
                if after {
                    value
                        + self.rays[group.start..group.end]
                            .iter()
                            .map(|ray| {
                                ray.ring
                                    .filter(|&(candidate, _)| candidate == ring)
                                    .map_or(0, |(_, delta)| delta)
                            })
                            .sum::<i64>()
                } else {
                    value
                }
            });
        value != 0
    }

    fn polygon_ray_support(
        &self,
        group: &RayGroup,
        before: &[(usize, i64)],
        _before_area: bool,
        _after_area: bool,
    ) -> bool {
        self.source.polygons.iter().any(|rings| {
            let mut rings = rings.clone();
            let Some(shell) = rings.next() else {
                return false;
            };
            let shell_support = self.ring_sector_support(shell, group, before, false)
                || self.ring_sector_support(shell, group, before, true);
            shell_support
                && !rings.any(|hole| {
                    self.ring_sector_support(hole, group, before, false)
                        || self.ring_sector_support(hole, group, before, true)
                })
        })
    }

    fn ring_sector_support(
        &self,
        ring: usize,
        group: &RayGroup,
        before: &[(usize, i64)],
        after: bool,
    ) -> bool {
        if self.source.rings[ring].kind == RingKind::Collapsed {
            return self.rays[group.start..group.end].iter().any(|ray| {
                ray.collapsed && ray.ring.is_some_and(|(candidate, _)| candidate == ring)
            });
        }
        self.ring_value(ring, group, before, after)
    }
}

fn line_location(
    explicit_curve: bool,
    collapsed_support: bool,
    endpoint_parity: bool,
    residual_collapsed_degree: usize,
) -> Loc {
    debug_assert!(explicit_curve || collapsed_support);
    let boundary = endpoint_parity ^ (residual_collapsed_degree % 2 == 1);
    if boundary {
        Loc::Boundary
    } else {
        Loc::Interior
    }
}

fn ray_groups(query: XY, rays: &[Ray]) -> Vec<RayGroup> {
    let mut groups = Vec::new();
    let mut start = 0;
    while start < rays.len() {
        let mut end = start + 1;
        while end < rays.len()
            && compare_rays(query, rays[start].end, rays[end].end) == Ordering::Equal
        {
            end += 1;
        }
        groups.push(RayGroup { start, end });
        start = end;
    }
    groups
}

fn compare_rays(query: XY, left: XY, right: XY) -> Ordering {
    let left_upper = left.y > query.y || (left.y == query.y && left.x > query.x);
    let right_upper = right.y > query.y || (right.y == query.y && right.x > query.x);
    if left_upper != right_upper {
        return right_upper.cmp(&left_upper);
    }
    match stored_orientation_sign(query.x, query.y, left.x, left.y, right.x, right.y) {
        ExactSign::Positive => Ordering::Less,
        ExactSign::Negative => Ordering::Greater,
        ExactSign::Zero => Ordering::Equal,
    }
}

/// Directed winding and exact source incidence for one stored ring.
fn ring_probe_columns(xs: &[f64], ys: &[f64], point: XY) -> RingProbe {
    let mut winding = 0_i64;
    let mut incident = false;
    for_each_xy_segment(xs, ys, |start, end| match edge_winding(start, end, point) {
        Some(delta) => winding += delta,
        None => incident = true,
    });
    RingProbe { winding, incident }
}

/// One stored edge's contribution to the winding number at `query`: its
/// signed upward or downward crossing of the rightward ray, or `None` when
/// the query lies on the edge.
pub(crate) fn edge_winding(start: XY, end: XY, query: XY) -> Option<i64> {
    if same_point(start, end) {
        return Some(0);
    }
    if point_on_segment(query.point(), start, end) {
        return None;
    }
    let (direction, sign) = if start.y <= query.y && query.y < end.y {
        (1, ExactSign::Positive)
    } else if end.y <= query.y && query.y < start.y {
        (-1, ExactSign::Negative)
    } else {
        return Some(0);
    };
    let orientation = stored_orientation_sign(start.x, start.y, end.x, end.y, query.x, query.y);
    Some(if orientation == sign { direction } else { 0 })
}

fn formal_ring_winding(xs: &[f64], ys: &[f64], point: XY) -> i64 {
    let mut winding = 0_i64;
    for_each_xy_segment(xs, ys, |start, end| {
        if same_point(start, end) {
            return;
        }
        let start_above = start.y > point.y;
        let end_above = end.y > point.y;
        if start_above == end_above {
            return;
        }
        let orientation = symbolic_orientation(start, end, point);
        if !start_above && end_above && orientation == ExactSign::Positive {
            winding += 1;
        } else if !end_above && start_above && orientation == ExactSign::Negative {
            winding -= 1;
        }
    });
    winding
}

fn symbolic_orientation(start: XY, end: XY, point: XY) -> ExactSign {
    let direct = stored_orientation_sign(start.x, start.y, end.x, end.y, point.x, point.y);
    if direct != ExactSign::Zero {
        return direct;
    }
    match start.y.total_cmp(&end.y) {
        Ordering::Less => ExactSign::Negative,
        Ordering::Greater => ExactSign::Positive,
        Ordering::Equal => match start.x.total_cmp(&end.x) {
            Ordering::Less => ExactSign::Positive,
            Ordering::Greater => ExactSign::Negative,
            Ordering::Equal => ExactSign::Zero,
        },
    }
}

fn ring_kind(ring: &Ring) -> RingKind {
    ring_kind_columns(ring.xs(), ring.ys())
}

pub(crate) fn ring_kind_columns(xs: &[f64], ys: &[f64]) -> RingKind {
    let Some((&ax, &ay)) = xs.first().zip(ys.first()) else {
        return RingKind::Collapsed;
    };
    let Some(index) = (0..xs.len()).find(|&index| xs[index] != ax || ys[index] != ay) else {
        return RingKind::Collapsed;
    };
    let bx = xs[index];
    let by = ys[index];
    let collinear = (0..xs.len()).all(|index| {
        stored_orientation_sign(ax, ay, bx, by, xs[index], ys[index]) == ExactSign::Zero
    });
    if collinear {
        RingKind::Collapsed
    } else {
        RingKind::Winding
    }
}

fn has_nonzero_segment(xs: &[f64], ys: &[f64]) -> bool {
    xs.array_windows::<2>()
        .zip(ys.array_windows::<2>())
        .any(|([ax, bx], [ay, by])| *ax != *bx || *ay != *by)
}

fn ring_has_point(xs: &[f64], ys: &[f64], point: XY) -> bool {
    let query = point.point();
    let mut hit = false;
    for_each_xy_segment(xs, ys, |start, end| {
        hit |= point_on_segment(query, start, end);
    });
    hit
}

fn curve_has_point(xs: &[f64], ys: &[f64], point: XY) -> bool {
    ring_has_point(xs, ys, point)
}

fn for_each_ring_segment(xs: &[f64], ys: &[f64], mut visit: impl FnMut(XY, XY)) {
    if xs.len() < 2 || ys.len() < 2 {
        return;
    }
    let count = if same_point(
        XY::new(xs[0], ys[0]),
        XY::new(xs[xs.len() - 1], ys[ys.len() - 1]),
    ) {
        xs.len() - 1
    } else {
        xs.len()
    };
    if count == 0 {
        return;
    }
    for index in 0..count {
        let next = if index + 1 == count { 0 } else { index + 1 };
        visit(XY::new(xs[index], ys[index]), XY::new(xs[next], ys[next]));
    }
}

fn for_each_xy_segment(xs: &[f64], ys: &[f64], mut visit: impl FnMut(XY, XY)) {
    for ([ax, bx], [ay, by]) in xs.array_windows::<2>().zip(ys.array_windows::<2>()) {
        visit(XY::new(*ax, *ay), XY::new(*bx, *by));
    }
}

/// Cartesian location for any source shape.
pub(crate) fn cartesian_point_location(shape: &Shape, point: Point) -> Loc {
    SourceSupport::from_shape(shape).location(point)
}

pub(crate) fn cartesian_area_covers_point(shape: &Shape, point: Point) -> bool {
    SourceSupport::from_shape(shape).area_covers(point)
}

/// Cartesian location over polygons given as ring views, shell first — the
/// path of prepared point testers, which already hold each ring's kind.
pub(crate) fn cartesian_polygon_views_location<'a, P: IntoIterator<Item = RingView<'a>>>(
    polygons: impl IntoIterator<Item = P>,
    point: Point,
) -> Loc {
    let mut source = SourceSupport::new();
    for rings in polygons {
        let first = source.rings.len();
        for ring in rings {
            source.rings.push(ring);
        }
        source.polygons.push(first..source.rings.len());
    }
    source.location(point)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shape(wkt: &str) -> Shape {
        crate::io::parse_wkt(wkt).expect("test WKT admits")
    }

    fn location(wkt: &str, x: f64, y: f64) -> Loc {
        cartesian_point_location(&shape(wkt), Point::new_raw_xy(x, y))
    }

    #[test]
    fn nonincident_line_query_has_no_contact_scratch_allocation() {
        let shape = Shape::LineString(crate::geometry::LineSeq::from_proven(
            crate::geometry::CoordSeq::try_from_vecs(
                (0..=4096).map(f64::from).collect(),
                (0..=4096).map(|index| f64::from(index % 2)).collect(),
                None,
                None,
            )
            .unwrap(),
        ));
        let source = SourceSupport::from_shape(&shape);
        let (location, allocations) = crate::test_support::allocation_count(|| {
            source.location(Point::new_raw_xy(2048.25, 0.4))
        });
        assert_eq!(location, Loc::Exterior);
        assert_eq!(allocations, 0);
    }

    #[test]
    fn twice_wound_nonzero_ring_has_areal_support() {
        let twice = "POLYGON ((0 0,4 0,4 4,0 4,0 0,4 0,4 4,0 4,0 0))";
        assert_eq!(location(twice, 2.0, 2.0), Loc::Interior);
        assert!(cartesian_area_covers_point(
            &shape(twice),
            Point::new_raw_xy(2.0, 2.0)
        ));
    }

    #[test]
    fn cancelled_spur_does_not_create_support() {
        let spur = "POLYGON ((0 0,4 0,4 4,0 4,0 0,-2 0,0 0))";
        assert_eq!(location(spur, 2.0, 2.0), Loc::Interior);
        assert_eq!(location(spur, -1.0, 0.0), Loc::Exterior);
    }

    #[test]
    fn cancelled_noncollinear_ring_has_no_support() {
        let cancelled = "POLYGON ((0 0,2 0,2 2,0 2,0 0,0 2,2 2,2 0,0 0))";
        let shape = shape(cancelled);
        assert_eq!(location(cancelled, 1.0, 1.0), Loc::Exterior);
        assert!(!shape.contains_point(Point::new_raw_xy(1.0, 1.0)));
        assert!(!crate::geometry::cartesian_support_nonempty(&shape).unwrap());
    }

    #[test]
    fn shell_and_hole_are_composed_before_point_membership() {
        let external = "POLYGON ((0 0,10 0,10 10,0 10,0 0),(20 20,21 20,21 21,20 21,20 20))";
        let identical = "POLYGON ((0 0,10 0,10 10,0 10,0 0),(0 0,10 0,10 10,0 10,0 0))";
        assert_eq!(location(external, 5.0, 5.0), Loc::Interior);
        assert_eq!(location(identical, 5.0, 5.0), Loc::Exterior);
    }

    #[test]
    fn collapsed_polygon_uses_closed_graph_support() {
        let collapsed = "POLYGON ((0 0,2 0,1 0,0 0))";
        assert_eq!(location(collapsed, 1.0, 0.0), Loc::Interior);
        assert_eq!(location(collapsed, 0.0, 0.0), Loc::Boundary);
        assert_eq!(location(collapsed, 3.0, 0.0), Loc::Exterior);
    }

    #[test]
    fn areal_hole_trims_collapsed_shell_support() {
        let trimmed = "POLYGON ((0 0,10 0,5 0,0 0),(2 -1,8 -1,8 1,2 1,2 -1))";
        assert_eq!(location(trimmed, 1.0, 0.0), Loc::Interior);
        assert_eq!(location(trimmed, 5.0, 0.0), Loc::Exterior);
        assert_eq!(location(trimmed, 2.0, 0.0), Loc::Boundary);
    }

    #[test]
    fn multiline_endpoint_parity_is_global() {
        let shared = "MULTILINESTRING ((0 0,1 0),(1 0,2 0))";
        let odd = "MULTILINESTRING ((0 0,2 0),(1 0,1 1))";
        assert_eq!(location(shared, 1.0, 0.0), Loc::Interior);
        assert_eq!(location(odd, 1.0, 0.0), Loc::Boundary);
    }

    #[test]
    fn exact_relation_uses_the_same_support_owner() {
        let square = shape("POLYGON ((0 0,4 0,4 4,0 4,0 0))");
        let external_hole = shape("POLYGON ((0 0,4 0,4 4,0 4,0 0),(8 8,9 8,9 9,8 9,8 8))");
        assert!(square.equals(&external_hole).unwrap());
        assert!(square.covers(&external_hole).unwrap());
        let erased_left = shape("POLYGON ((0 0,2 0,2 2,0 2,0 0,0 2,2 2,2 0,0 0))");
        let erased_right =
            shape("POLYGON ((10 10,12 10,12 12,10 12,10 10,10 12,12 12,12 10,10 10))");
        assert!(erased_left.equals(&erased_right).unwrap());

        let twice = shape("POLYGON ((0 0,4 0,4 4,0 4,0 0,4 0,4 4,0 4,0 0))");
        assert!(twice.contains_point(Point::new_raw_xy(2.0, 2.0)));
        assert_eq!(
            crate::geometry::occupied_cartesian_area(&twice).unwrap(),
            16.0
        );

        let collapsed = shape("POLYGON ((0 0,2 0,1 0,0 0))");
        let line = shape("LINESTRING (1 0,3 0)");
        assert!(collapsed.overlaps(&line).unwrap());
    }

    #[test]
    fn exact_cells_trim_collapsed_shells_by_areal_and_point_holes() {
        let trimmed = shape("POLYGON ((0 0,10 0,5 0,0 0),(2 -1,8 -1,8 1,2 1,2 -1))");
        assert!(trimmed.contains_point(Point::new_raw_xy(1.0, 0.0)));
        assert!(!trimmed.contains_point(Point::new_raw_xy(5.0, 0.0)));
        assert!(trimmed.intersects(&shape("POINT (1 0)")).unwrap());
        assert!(!trimmed.intersects(&shape("POINT (5 0)")).unwrap());
        let crossing = trimmed
            .intersection(
                &shape("LINESTRING (4 0,6 0)"),
                crate::geometry::Strictness::Lenient,
            )
            .unwrap();
        assert!(crossing.is_empty(), "{crossing:?}");

        let erased = shape("POLYGON ((1 1,1 1,1 1,1 1),(1 1,1 1,1 1,1 1))");
        assert!(!erased.contains_point(Point::new_raw_xy(1.0, 1.0)));
        assert!(!erased.intersects(&shape("POINT (1 1)")).unwrap());
        assert!(!crate::geometry::cartesian_support_nonempty(&erased).unwrap());

        let boundary_erased = shape("POLYGON ((1 1,1 1,1 1,1 1),(1 1,3 1,3 3,1 3,1 1))");
        assert!(!boundary_erased.contains_point(Point::new_raw_xy(1.0, 1.0)));
        let survivor = shape("POLYGON ((1 1,1 1,1 1,1 1))");
        let members = Shape::geometry_collection(vec![boundary_erased, survivor]).unwrap();
        assert!(members.contains_point(Point::new_raw_xy(1.0, 1.0)));
        assert!(members.intersects(&shape("POINT (1 1)")).unwrap());
    }

    #[test]
    fn exact_node_keeps_node_identity_and_late_ordinate_failures() {
        let crossing = shape("MULTILINESTRING ((0 0,2 2),(0 2,2 0))");
        let noded = crossing.node(crate::geometry::Strictness::Strict).unwrap();
        let expected = shape("MULTILINESTRING ((0 0,1 1),(1 1,2 2),(0 2,1 1),(1 1,2 0))");
        assert!(noded.equals(&expected).unwrap(), "{noded:?}");

        let left = shape("LINESTRING Z (0 0 0,2 2 0)");
        let right = shape("LINESTRING Z (0 2 1,2 0 1)");
        let error = crate::geometry::Shape::geometry_collection(vec![left, right])
            .unwrap()
            .node(crate::geometry::Strictness::Strict)
            .unwrap_err();
        assert!(
            error.to_string().contains("node cannot represent Z/M"),
            "{error}"
        );

        let point = crate::geometry::Shape::MultiPoint(
            vec![
                Point::new_axes(
                    0.0,
                    0.0,
                    crate::geometry::ZOrdinate(Some(1.0)),
                    crate::geometry::MOrdinate(None),
                )
                .unwrap(),
            ]
            .into(),
        );
        assert!(matches!(
            point.node(crate::geometry::Strictness::Strict).unwrap(),
            Shape::Empty(
                crate::geometry::GeometryKind::MultiLineString,
                crate::geometry::CoordinateAxes::XYZ
            )
        ));
    }

    #[test]
    fn exact_overlay_retains_xy_collinear_ordinate_knots() {
        let source = shape("POLYGON Z ((0 0 0,1 0 1,2 0 2,2 2 3,0 2 4,0 0 0))");
        let result = crate::geometry::overlay_all(
            &[&source],
            crate::geometry::TopologyDomain::Cartesian,
            crate::geometry::OverlayOp::Union,
            crate::geometry::Strictness::Strict,
        )
        .unwrap();
        let knot = result
            .points_vec()
            .into_iter()
            .find(|point| point.x == 1.0 && point.y == 0.0)
            .expect("the source knot remains on the selected boundary");
        assert_eq!(knot.z(), Some(1.0));
    }

    #[test]
    fn exact_overlay_checks_both_ordinates_of_zero_length_occurrences() {
        let source = shape("POLYGON Z ((0 0 1,0 0 2,0 0 1,0 0 1))");
        let error = crate::geometry::overlay_all(
            &[&source],
            crate::geometry::TopologyDomain::Cartesian,
            crate::geometry::OverlayOp::Union,
            crate::geometry::Strictness::Strict,
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("cannot represent Z/M"),
            "{error}"
        );
    }

    #[test]
    fn exact_overlay_closes_selected_support_once() {
        let retraced = shape("LINESTRING (0 0,1 0,0 0)");
        let point = shape("POINT (2 0)");
        let result = crate::geometry::overlay_all(
            &[&retraced, &point],
            crate::geometry::TopologyDomain::Cartesian,
            crate::geometry::OverlayOp::Union,
            crate::geometry::Strictness::Lenient,
        )
        .unwrap();
        assert!(result.contains(&retraced).unwrap());
        assert!(result.covers(&point).unwrap());

        let a = shape("LINESTRING (0 0,1 0)");
        let b = shape("LINESTRING (1 0,2 0)");
        let c = shape("LINESTRING (0 0,2 0)");
        let result = crate::geometry::overlay_all(
            &[&a, &b, &c],
            crate::geometry::TopologyDomain::Cartesian,
            crate::geometry::OverlayOp::SymmetricDifference,
            crate::geometry::Strictness::Lenient,
        )
        .unwrap();
        assert!(result.equals(&shape("POINT (1 0)")).unwrap());

        let line = shape("LINESTRING (0 0,2 0)");
        let point = shape("POINT (1 0)");
        let result = crate::geometry::overlay_all(
            &[&line, &point],
            crate::geometry::TopologyDomain::Cartesian,
            crate::geometry::OverlayOp::Difference,
            crate::geometry::Strictness::Lenient,
        )
        .unwrap();
        assert!(result.equals(&line).unwrap());
    }

    #[test]
    fn exact_overlay_resolves_incident_axes_without_nearby_sources() {
        let left = shape("LINESTRING Z (0 0 0,2 2 0)");
        let right = shape("LINESTRING Z (0 2 0,2 0 0)");
        let result = crate::geometry::overlay_all(
            &[&left, &right],
            crate::geometry::TopologyDomain::Cartesian,
            crate::geometry::OverlayOp::Intersection,
            crate::geometry::Strictness::Strict,
        )
        .unwrap();
        let Shape::Point(point) = result else {
            panic!("crossing intersection must be a point");
        };
        assert!(point.axes.has_z());
        assert_eq!(point.z(), Some(0.0));
    }

    #[test]
    fn exact_overlay_ignores_nonincident_ordinate_sources() {
        let left = shape("LINESTRING Z (0 0 0,2 2 0)");
        let right = Shape::geometry_collection(vec![
            shape("LINESTRING Z (0 2 0,2 0 0)"),
            shape("LINESTRING Z (10 10 7,11 11 7)"),
        ])
        .unwrap();
        let result = crate::geometry::overlay_all(
            &[&left, &right],
            crate::geometry::TopologyDomain::Cartesian,
            crate::geometry::OverlayOp::Intersection,
            crate::geometry::Strictness::Strict,
        )
        .unwrap();
        let Shape::Point(point) = result else {
            panic!("crossing intersection must be a point");
        };
        assert_eq!(point.z(), Some(0.0));
    }

    #[test]
    fn exact_overlay_interpolates_each_incident_axis_once() {
        let left = shape("LINESTRING ZM (0 0 0 10,2 2 2 20)");
        let right = shape("LINESTRING ZM (0 2 0 10,2 0 2 20)");
        let result = crate::geometry::overlay_all(
            &[&left, &right],
            crate::geometry::TopologyDomain::Cartesian,
            crate::geometry::OverlayOp::Intersection,
            crate::geometry::Strictness::Strict,
        )
        .unwrap();
        let Shape::Point(point) = result else {
            panic!("crossing intersection must be a point");
        };
        assert_eq!(point.z(), Some(1.0));
        assert_eq!(point.m(), Some(15.0));
    }

    #[test]
    fn exact_bounds_use_selected_support_not_raw_canceled_edges() {
        let spur = shape("POLYGON ((0 0,4 0,4 4,0 4,0 0,-2 0,0 0))");
        assert!(
            !crate::geometry::exact_intersects_bounds(
                &spur,
                crate::geometry::Bounds::new_raw(-1.5, -0.1, -0.5, 0.1)
            )
            .unwrap()
        );
        let external_hole = shape("POLYGON ((0 0,4 0,4 4,0 4,0 0),(8 8,9 8,9 9,8 9,8 8))");
        assert!(
            !crate::geometry::exact_intersects_bounds(
                &external_hole,
                crate::geometry::Bounds::new_raw(7.5, 7.5, 9.5, 9.5)
            )
            .unwrap()
        );
    }
}
