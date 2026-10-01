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

#[cfg(test)]
use crate::geometry::Ring;
use crate::geometry::exact_arith::ExactSign;
use crate::geometry::segments::stored_orientation_sign;
use crate::geometry::{
    Loc, Point, Polygon, Shape, SharedShapeOwners, XY, point_on_segment, same_point,
};

/// Whether a stored ring supplies areal winding or collapsed line support.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RingKind {
    Winding,
    Collapsed,
}

const fn ring_ray_delta(kind: RingKind, reverse: bool) -> i64 {
    match (kind, reverse) {
        (RingKind::Winding, false) => 1,
        (RingKind::Winding, true) => -1,
        (RingKind::Collapsed, _) => 0,
    }
}

#[derive(Clone, Copy)]
pub(crate) struct RingView<'a> {
    pub(crate) xs: &'a [f64],
    pub(crate) ys: &'a [f64],
    pub(crate) kind: RingKind,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg(test)]
struct RingProbe {
    pub(crate) winding: i64,
    pub(crate) incident: bool,
}

#[cfg(test)]
struct CurveView<'a> {
    xs: &'a [f64],
    ys: &'a [f64],
}

#[cfg(test)]
struct ReferenceSourceSupport<'a> {
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
    reverse: bool,
}

#[derive(Clone, Copy)]
struct RayGroup {
    start: usize,
    end: usize,
}

#[cfg(test)]
impl<'a> ReferenceSourceSupport<'a> {
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
                        ring: Some((ring_id, ring_ray_delta(ring.kind, false))),
                        curve: false,
                        collapsed: ring.kind == RingKind::Collapsed,
                        reverse: false,
                    });
                }
                if !same_point(query, start) {
                    rays.push(Ray {
                        end: start,
                        ring: Some((ring_id, ring_ray_delta(ring.kind, true))),
                        curve: false,
                        collapsed: ring.kind == RingKind::Collapsed,
                        reverse: true,
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
                        reverse: false,
                    });
                }
                if !same_point(query, start) {
                    rays.push(Ray {
                        end: start,
                        ring: None,
                        curve: true,
                        collapsed: false,
                        reverse: true,
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
            return line_location(self.curve_endpoint_parity(query), false);
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

#[cfg(test)]
struct LocalStar<'a, 'b> {
    source: &'a ReferenceSourceSupport<'b>,
    query: XY,
    windings: Vec<i64>,
    rays: Vec<Ray>,
    groups: Vec<RayGroup>,
    endpoint_parity: bool,
    explicit_point: bool,
    collapsed_points: Vec<usize>,
}

#[cfg(test)]
impl LocalStar<'_, '_> {
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "nonempty groups and bounded cyclic offsets keep group ordinals representable"
    )]
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
        let mut residual_collapsed_degree_odd = false;
        let mut ring_support = Vec::new();
        if !self.collapsed_points.is_empty() {
            ring_support.resize(self.source.rings.len(), false);
            for (support, &winding) in ring_support.iter_mut().zip(&self.windings) {
                *support |= winding != 0;
            }
            for &ring in &self.collapsed_points {
                Self::retain_ring_support(&mut ring_support, ring, true);
            }
            for ray in &self.rays {
                if let Some((ring, _)) = ray.ring
                    && ray.collapsed
                {
                    Self::retain_ring_support(&mut ring_support, ring, true);
                }
            }
        }

        let mut group_values = Vec::new();
        for offset in 0..self.groups.len() {
            let group_index = (first + offset) % self.groups.len();
            let group = &self.groups[group_index];
            let group_rays = &self.rays[group.start..group.end];
            let before_area = self.source.area_occupied(&self.windings);
            self.group_ring_values(group_rays, &mut group_values);
            let group_curve = group_rays.iter().any(|ray| ray.curve);
            curve_ray |= group_curve;
            for ray in group_rays {
                if let Some((ring, delta)) = ray.ring {
                    Self::cross_ring(
                        &mut self.windings,
                        &self.source.rings,
                        ring,
                        delta,
                        &mut ring_support,
                    );
                }
            }
            let after_area = self.source.area_occupied(&self.windings);
            sector_area.push(before_area);
            let group_support =
                self.polygon_ray_support(group_rays, &group_values, before_area, after_area);
            ray_polygon_support |= group_support;
            if group_support && !group_curve {
                residual_collapsed_degree_odd = !residual_collapsed_degree_odd;
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
            return line_location(self.endpoint_parity, residual_collapsed_degree_odd);
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

    fn cross_ring(
        windings: &mut [i64],
        rings: &[RingView<'_>],
        ring: usize,
        delta: i64,
        ring_support: &mut [bool],
    ) {
        let winding = &mut windings[ring];
        *winding = winding.wrapping_add(delta);
        if !ring_support.is_empty() && rings[ring].kind == RingKind::Winding {
            Self::retain_ring_support(ring_support, ring, *winding != 0);
        }
    }

    const fn retain_ring_support(ring_support: &mut [bool], ring: usize, supported: bool) {
        ring_support[ring] |= supported;
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

    fn group_ring_values(&self, group: &[Ray], values: &mut Vec<(usize, i64)>) {
        values.clear();
        for ray in group {
            let Some((ring, _)) = ray.ring else {
                continue;
            };
            if values.iter().any(|&(candidate, _)| candidate == ring) {
                continue;
            }
            values.push((ring, self.windings[ring]));
        }
    }

    fn ring_value(&self, ring: usize, group: &[Ray], before: &[(usize, i64)], after: bool) -> bool {
        let value = before
            .iter()
            .find_map(|&(candidate, value)| (candidate == ring).then_some(value))
            .map_or(self.windings[ring], |value| {
                if after {
                    let group_delta = group.iter().fold(0_i64, |sum, ray| {
                        sum.wrapping_add(
                            ray.ring
                                .filter(|&(candidate, _)| candidate == ring)
                                .map_or(0, |(_, delta)| delta),
                        )
                    });
                    value.wrapping_add(group_delta)
                } else {
                    value
                }
            });
        value != 0
    }

    fn polygon_ray_support(
        &self,
        group: &[Ray],
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
        group: &[Ray],
        before: &[(usize, i64)],
        after: bool,
    ) -> bool {
        if self.source.rings[ring].kind == RingKind::Collapsed {
            return group.iter().any(|ray| {
                ray.collapsed && ray.ring.is_some_and(|(candidate, _)| candidate == ring)
            });
        }
        self.ring_value(ring, group, before, after)
    }
}

const fn line_location(endpoint_parity: bool, residual_collapsed_degree_odd: bool) -> Loc {
    let boundary = endpoint_parity ^ residual_collapsed_degree_odd;
    if boundary {
        Loc::Boundary
    } else {
        Loc::Interior
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LocationMode {
    Full,
    Area,
}

struct BoundaryRing {
    kind: RingKind,
    winding: i64,
    shell: bool,
}

#[derive(Clone, Copy)]
struct PolygonShell {
    kind: RingKind,
    winding: bool,
    ring: Option<usize>,
    collapsed_point: bool,
}

#[derive(Clone, Copy)]
struct PolygonHoles {
    winding: bool,
    point_support: bool,
}

struct BoundaryPolygon {
    rings: Range<usize>,
    shell: Option<PolygonShell>,
    holes: PolygonHoles,
}

#[derive(Clone, Copy)]
struct PolygonSummary {
    ring_start: usize,
    shell: Option<PolygonShell>,
    holes: PolygonHoles,
}

impl PolygonSummary {
    const fn new(ring_start: usize) -> Self {
        Self {
            ring_start,
            shell: None,
            holes: PolygonHoles {
                winding: false,
                point_support: false,
            },
        }
    }

    fn add_ring(&mut self, shell: bool, scan: RingScan) {
        if shell {
            self.shell = Some(PolygonShell {
                kind: scan.kind,
                winding: scan.winding != 0,
                ring: scan.boundary_ring,
                collapsed_point: scan.kind == RingKind::Collapsed && scan.collapsed_point,
            });
            return;
        }

        if scan.kind == RingKind::Winding && scan.winding != 0 {
            self.holes.point_support = true;
            if scan.boundary_ring.is_none() {
                self.holes.winding = true;
            }
        } else if scan.kind == RingKind::Collapsed
            && (scan.boundary_ring.is_some() || scan.collapsed_point)
        {
            self.holes.point_support = true;
        }
    }
}

#[derive(Clone, Copy)]
struct RingScan {
    kind: RingKind,
    winding: i64,
    boundary_ring: Option<usize>,
    collapsed_point: bool,
}

struct EdgeProbe {
    on_segment: bool,
    winding: i64,
    formal_winding: i64,
}

/// Stamps the exact edge decision into each column loop so a boundary query
/// does not cross a Rust call boundary for every source edge.
macro_rules! edge_probe {
    ($start:expr, $end:expr, $query:expr) => {{
        let start = $start;
        let end = $end;
        let query = $query;
        let at_start = query.y == start.y && same_point(query, start);
        let at_end = query.y == end.y && same_point(query, end);
        let start_above = start.y > query.y;
        let end_above = end.y > query.y;
        // Opposite strict-above flags encode both half-open crossing directions.
        let crossing = start_above != end_above;
        let horizontal = query.y == start.y && query.y == end.y;
        let horizontal_on_segment = horizontal
            && if start.x <= end.x {
                start.x <= query.x && query.x <= end.x
            } else {
                end.x <= query.x && query.x <= start.x
            };
        let orientation = if at_start || at_end {
            Some(ExactSign::Zero)
        } else if crossing {
            Some(stored_orientation_sign(
                start.x, start.y, end.x, end.y, query.x, query.y,
            ))
        } else {
            None
        };
        let on_segment = at_start
            || at_end
            || horizontal_on_segment
            || (crossing && orientation == Some(ExactSign::Zero));
        let upward = crossing && !start_above;
        let downward = crossing && start_above;
        let winding = if upward && orientation == Some(ExactSign::Positive) {
            1
        } else if downward && orientation == Some(ExactSign::Negative) {
            -1
        } else {
            0
        };
        let formal_winding = if crossing {
            let orientation = orientation.expect("a formal crossing has an orientation");
            let orientation = symbolic_orientation_from_sign(start, end, orientation);
            if !start_above && end_above && orientation == ExactSign::Positive {
                1
            } else if !end_above && start_above && orientation == ExactSign::Negative {
                -1
            } else {
                0
            }
        } else {
            0
        };
        EdgeProbe {
            on_segment,
            winding,
            formal_winding,
        }
    }};
}

/// Query-local exact point location. Ordinary probes keep only scalar
/// winding and occupancy state; rays and per-ring state are retained only
/// when the query lies on source linework.
struct StreamingLocation {
    query: XY,
    mode: LocationMode,
    boundary_rings: Vec<BoundaryRing>,
    boundary_polygons: Vec<BoundaryPolygon>,
    rays: Vec<Ray>,
    area_without_contact: bool,
    point_support: bool,
    curve_endpoint_parity: bool,
}

impl StreamingLocation {
    const fn new(query: XY, mode: LocationMode) -> Self {
        Self {
            query,
            mode,
            boundary_rings: Vec::new(),
            boundary_polygons: Vec::new(),
            rays: Vec::new(),
            area_without_contact: false,
            point_support: false,
            curve_endpoint_parity: false,
        }
    }

    fn visit_shape(&mut self, shape: &Shape, owners: &mut Option<&mut SharedShapeOwners>) {
        match shape {
            Shape::Point(point) => {
                if self.mode == LocationMode::Full && same_point(point.xy(), self.query) {
                    self.point_support = true;
                }
            },
            Shape::MultiPoint(points) => {
                if self.mode == LocationMode::Full
                    && points
                        .points()
                        .any(|point| same_point(point.xy(), self.query))
                {
                    self.point_support = true;
                }
            },
            Shape::LineString(line) if self.mode == LocationMode::Full => {
                self.visit_curve(line.as_coords());
            },
            Shape::MultiLineString(lines) if self.mode == LocationMode::Full => {
                for line in lines {
                    self.visit_curve(line.as_coords());
                }
            },
            Shape::Polygon(polygon) => self.visit_polygon(polygon),
            Shape::MultiPolygon(polygons) => {
                for polygon in polygons {
                    self.visit_polygon(polygon);
                }
            },
            Shape::GeometryCollection(geometries) => {
                if owners.as_mut().is_none_or(|owners| {
                    owners.first_collection(geometries.as_slice(), geometries.members_are_shared())
                }) {
                    for geometry in geometries {
                        self.visit_shape(geometry, owners);
                    }
                }
            },
            Shape::LineString(_) | Shape::MultiLineString(_) | Shape::Empty(..) => {},
        }
    }

    fn visit_polygon(&mut self, polygon: &Polygon) {
        let mut summary = PolygonSummary::new(self.boundary_rings.len());
        for (index, ring) in polygon.rings().enumerate() {
            let scan = self.scan_ring(ring.xs(), ring.ys(), None, index == 0);
            summary.add_ring(index == 0, scan);
        }
        self.finish_polygon(summary);
    }

    fn visit_polygon_views<'a, P>(&mut self, polygons: impl IntoIterator<Item = P>)
    where
        P: IntoIterator<Item = RingView<'a>>,
    {
        for polygon in polygons {
            let mut summary = PolygonSummary::new(self.boundary_rings.len());
            for (index, ring) in polygon.into_iter().enumerate() {
                let scan = self.scan_ring(ring.xs, ring.ys, Some(ring.kind), index == 0);
                summary.add_ring(index == 0, scan);
            }
            self.finish_polygon(summary);
        }
    }

    fn finish_polygon(&mut self, summary: PolygonSummary) {
        if summary.ring_start == self.boundary_rings.len() {
            self.area_without_contact |= summary
                .shell
                .is_some_and(|shell| shell.winding && !summary.holes.winding);
            if self.mode == LocationMode::Full
                && summary.shell.is_some_and(|shell| shell.collapsed_point)
                && !summary.holes.point_support
            {
                self.point_support = true;
            }
            return;
        }

        self.boundary_polygons.push(BoundaryPolygon {
            rings: summary.ring_start..self.boundary_rings.len(),
            shell: summary.shell,
            holes: summary.holes,
        });
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "ray_start is the prior ray length and scanning only appends rays before rewriting that suffix"
    )]
    fn scan_ring(
        &mut self,
        xs: &[f64],
        ys: &[f64],
        kind_hint: Option<RingKind>,
        shell: bool,
    ) -> RingScan {
        let first = xs.first().zip(ys.first()).map(|(&x, &y)| XY::new(x, y));
        let mut distinct: Option<XY> = None;
        let mut non_collinear = false;
        let mut winding = 0_i64;
        let mut formal_winding = 0_i64;
        let mut collapsed_point = false;
        let ray_start = self.rays.len();
        let query = self.query;

        macro_rules! scan_edge {
            ($start:expr, $end:expr) => {{
                let start = $start;
                let end = $end;
                if same_point(start, end) {
                    collapsed_point |= same_point(start, query);
                } else {
                    let edge = edge_probe!(start, end, query);
                    if edge.on_segment {
                        if !same_point(query, end) {
                            self.rays.push(Ray {
                                end,
                                ring: None,
                                curve: false,
                                collapsed: false,
                                reverse: false,
                            });
                        }
                        if !same_point(query, start) {
                            self.rays.push(Ray {
                                end: start,
                                ring: None,
                                curve: false,
                                collapsed: false,
                                reverse: true,
                            });
                        }
                    } else {
                        winding = winding.wrapping_add(edge.winding);
                    }
                    formal_winding = formal_winding.wrapping_add(edge.formal_winding);
                }
            }};
        }

        if xs.len() >= 2 && ys.len() >= 2 {
            for ([ax, bx], [ay, by]) in xs.array_windows::<2>().zip(ys.array_windows::<2>()) {
                let start = XY::new(*ax, *ay);
                let end = XY::new(*bx, *by);
                if kind_hint.is_none()
                    && !non_collinear
                    && let Some(first) = first
                    && (end.x != first.x || end.y != first.y)
                {
                    if let Some(second) = distinct {
                        non_collinear = stored_orientation_sign(
                            first.x, first.y, second.x, second.y, end.x, end.y,
                        ) != ExactSign::Zero;
                    } else {
                        distinct = Some(end);
                    }
                }
                scan_edge!(start, end);
            }
            if let (Some(first), Some(last)) = (
                xs.first().zip(ys.first()).map(|(&x, &y)| XY::new(x, y)),
                xs.last().zip(ys.last()).map(|(&x, &y)| XY::new(x, y)),
            ) && !same_point(first, last)
            {
                scan_edge!(last, first);
            }
        }

        let kind = kind_hint.unwrap_or(if non_collinear {
            RingKind::Winding
        } else {
            RingKind::Collapsed
        });
        let touched = self.rays.len() != ray_start;
        if self.mode == LocationMode::Area && kind == RingKind::Collapsed {
            self.rays.truncate(ray_start);
        }
        let boundary_ring =
            if touched && (self.mode == LocationMode::Full || kind == RingKind::Winding) {
                let index = self.boundary_rings.len();
                for ray in &mut self.rays[ray_start..] {
                    ray.ring = Some((index, ring_ray_delta(kind, ray.reverse)));
                    ray.collapsed = kind == RingKind::Collapsed;
                }
                self.boundary_rings.push(BoundaryRing {
                    kind,
                    winding: if kind == RingKind::Winding {
                        formal_winding
                    } else {
                        0
                    },
                    shell,
                });
                Some(index)
            } else {
                if self.mode == LocationMode::Area {
                    self.rays.truncate(ray_start);
                }
                None
            };

        RingScan {
            kind,
            winding: if kind == RingKind::Winding {
                if boundary_ring.is_some() {
                    formal_winding
                } else {
                    winding
                }
            } else {
                0
            },
            boundary_ring,
            collapsed_point,
        }
    }

    fn visit_curve(&mut self, curve: &crate::geometry::CoordSeq) {
        let xs = curve.xs();
        let ys = curve.ys();
        let Some((&first_x, &first_y)) = xs.first().zip(ys.first()) else {
            return;
        };
        let Some((&last_x, &last_y)) = xs.last().zip(ys.last()) else {
            return;
        };
        let mut nonzero = false;
        let query = self.query;
        for ([ax, bx], [ay, by]) in xs.array_windows::<2>().zip(ys.array_windows::<2>()) {
            let start = XY::new(*ax, *ay);
            let end = XY::new(*bx, *by);
            nonzero |= start.x != end.x || start.y != end.y;
            if !nonzero || same_point(start, end) {
                continue;
            }
            if edge_probe!(start, end, query).on_segment {
                if !same_point(query, end) {
                    self.rays.push(Ray {
                        end,
                        ring: None,
                        curve: true,
                        collapsed: false,
                        reverse: false,
                    });
                }
                if !same_point(query, start) {
                    self.rays.push(Ray {
                        end: start,
                        ring: None,
                        curve: true,
                        collapsed: false,
                        reverse: true,
                    });
                }
            }
        }
        if nonzero {
            self.curve_endpoint_parity ^= same_point(XY::new(first_x, first_y), query)
                ^ same_point(XY::new(last_x, last_y), query);
        } else if self.mode == LocationMode::Full
            && xs
                .iter()
                .zip(ys)
                .any(|(&x, &y)| same_point(XY::new(x, y), query))
        {
            self.point_support = true;
        }
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "shell and boundary-ring IDs are created with the same boundary_rings vector they index"
    )]
    fn area_occupied(&self) -> bool {
        self.area_without_contact
            || self.boundary_polygons.iter().any(|polygon| {
                let shell = polygon.shell.is_some_and(|shell| {
                    shell
                        .ring
                        .map_or(shell.winding, |ring| self.boundary_rings[ring].winding != 0)
                });
                shell
                    && !polygon.holes.winding
                    && !polygon.rings.clone().any(|ring| {
                        let state = &self.boundary_rings[ring];
                        !state.shell && state.kind == RingKind::Winding && state.winding != 0
                    })
            })
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "ray ring IDs are assigned from boundary_rings before they are retained"
    )]
    fn ring_sector_support(&self, ring: usize, group: &[Ray], after: bool) -> bool {
        let state = &self.boundary_rings[ring];
        if state.kind == RingKind::Collapsed {
            return group.iter().any(|ray| {
                ray.collapsed && ray.ring.is_some_and(|(candidate, _)| candidate == ring)
            });
        }
        let winding = state.winding.wrapping_add(if after {
            group.iter().fold(0_i64, |delta, ray| {
                delta.wrapping_add(
                    ray.ring
                        .filter(|&(candidate, _)| candidate == ring)
                        .map_or(0, |(_, delta)| delta),
                )
            })
        } else {
            0
        });
        winding != 0
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "each polygon ring range and shell ID refer to the boundary-ring entries appended while scanning that polygon"
    )]
    fn polygon_ray_support(&self, group: &[Ray]) -> bool {
        self.boundary_polygons.iter().any(|polygon| {
            let shell = polygon.shell.is_some_and(|shell| {
                shell
                    .ring
                    .map_or(shell.kind == RingKind::Winding && shell.winding, |ring| {
                        self.ring_sector_support(ring, group, false)
                            || self.ring_sector_support(ring, group, true)
                    })
            });
            shell
                && !polygon.holes.winding
                && !polygon.rings.clone().any(|ring| {
                    let state = &self.boundary_rings[ring];
                    !state.shell
                        && (self.ring_sector_support(ring, group, false)
                            || self.ring_sector_support(ring, group, true))
                })
        })
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "ray groups partition the sorted ray vector and their ring IDs reference the same boundary-ring vector"
    )]
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "Cyclic group indices and offsets are bounded by the addressable RayGroup vector"
    )]
    fn boundary_location(mut self) -> Loc {
        self.rays
            .sort_unstable_by(|left, right| compare_rays(self.query, left.end, right.end));
        let groups = ray_groups(self.query, &self.rays);
        let seed_group = groups.iter().position(|group| {
            let ray = &self.rays[group.start];
            ray.end.y == self.query.y && ray.end.x > self.query.x
        });
        let first = seed_group.map_or_else(
            || {
                groups
                    .iter()
                    .position(|group| {
                        let ray = &self.rays[group.start];
                        ray.end.y > self.query.y
                            || (ray.end.y == self.query.y && ray.end.x > self.query.x)
                    })
                    .unwrap_or(0)
            },
            |index| index.wrapping_add(1) % groups.len(),
        );

        let mut any_area = false;
        let mut all_area = true;
        let mut line = false;
        let mut residual_collapsed_degree_odd = false;
        for offset in 0..groups.len() {
            let group_index = first.wrapping_add(offset) % groups.len();
            let group = groups[group_index];
            let rays = &self.rays[group.start..group.end];
            let before_area = self.area_occupied();
            let group_curve = rays.iter().any(|ray| ray.curve);
            let group_support = self.polygon_ray_support(rays);
            line |= group_curve || group_support;
            if group_support && !group_curve {
                residual_collapsed_degree_odd = !residual_collapsed_degree_odd;
            }
            for ray in rays {
                if let Some((ring, delta)) = ray.ring {
                    self.boundary_rings[ring].winding =
                        self.boundary_rings[ring].winding.wrapping_add(delta);
                }
            }
            any_area |= before_area;
            all_area &= before_area;
        }

        if self.mode == LocationMode::Area {
            return if any_area {
                Loc::Interior
            } else {
                Loc::Exterior
            };
        }
        if any_area {
            return if all_area {
                Loc::Interior
            } else {
                Loc::Boundary
            };
        }
        if line {
            return line_location(self.curve_endpoint_parity, residual_collapsed_degree_odd);
        }
        let point_support = self.point_support
            || self.boundary_polygons.iter().any(|polygon| {
                polygon.shell.is_some_and(|shell| shell.collapsed_point)
                    && !polygon.holes.point_support
            });
        if point_support {
            Loc::Interior
        } else {
            Loc::Exterior
        }
    }

    fn finish(self) -> Loc {
        if self.rays.is_empty() {
            if self.mode == LocationMode::Full && self.point_support {
                return Loc::Interior;
            }
            return if self.area_without_contact {
                Loc::Interior
            } else {
                Loc::Exterior
            };
        }
        self.boundary_location()
    }
}

fn symbolic_orientation_from_sign(start: XY, end: XY, direct: ExactSign) -> ExactSign {
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

#[expect(
    clippy::indexing_slicing,
    reason = "start and end advance only within the sorted ray slice being grouped"
)]
fn ray_groups(query: XY, rays: &[Ray]) -> Vec<RayGroup> {
    let mut groups = Vec::new();
    let mut start = 0_usize;
    while start < rays.len() {
        let mut end = start.wrapping_add(1);
        while end < rays.len()
            && compare_rays(query, rays[start].end, rays[end].end) == Ordering::Equal
        {
            end = end.wrapping_add(1);
        }
        groups.push(RayGroup { start, end });
        start = end;
    }
    groups
}

pub(in crate::geometry) fn compare_rays(query: XY, left: XY, right: XY) -> Ordering {
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
#[cfg(test)]
fn ring_probe_columns(xs: &[f64], ys: &[f64], point: XY) -> RingProbe {
    let mut winding = 0_i64;
    let mut incident = false;
    for_each_xy_segment(xs, ys, |start, end| match edge_winding(start, end, point) {
        Some(delta) => winding = winding.wrapping_add(delta),
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

#[cfg(test)]
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
            winding = winding.wrapping_add(1);
        } else if !end_above && start_above && orientation == ExactSign::Negative {
            winding = winding.wrapping_sub(1);
        }
    });
    winding
}

#[cfg(test)]
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

#[cfg(test)]
fn ring_kind(ring: &Ring) -> RingKind {
    ring_kind_columns(ring.xs(), ring.ys())
}

pub(crate) fn ring_kind_columns(xs: &[f64], ys: &[f64]) -> RingKind {
    let Some((&ax, &ay)) = xs.first().zip(ys.first()) else {
        return RingKind::Collapsed;
    };
    let Some((&bx, &by)) = xs.iter().zip(ys).find(|(x, y)| **x != ax || **y != ay) else {
        return RingKind::Collapsed;
    };
    let collinear = xs
        .iter()
        .zip(ys)
        .all(|(&x, &y)| stored_orientation_sign(ax, ay, bx, by, x, y) == ExactSign::Zero);
    if collinear {
        RingKind::Collapsed
    } else {
        RingKind::Winding
    }
}

#[cfg(test)]
fn has_nonzero_segment(xs: &[f64], ys: &[f64]) -> bool {
    xs.array_windows::<2>()
        .zip(ys.array_windows::<2>())
        .any(|([ax, bx], [ay, by])| *ax != *bx || *ay != *by)
}

#[cfg(test)]
fn ring_has_point(xs: &[f64], ys: &[f64], point: XY) -> bool {
    let query = point.point();
    let mut hit = false;
    for_each_xy_segment(xs, ys, |start, end| {
        hit |= point_on_segment(query, start, end);
    });
    hit
}

#[cfg(test)]
fn curve_has_point(xs: &[f64], ys: &[f64], point: XY) -> bool {
    ring_has_point(xs, ys, point)
}

#[cfg(test)]
fn for_each_ring_segment(xs: &[f64], ys: &[f64], mut visit: impl FnMut(XY, XY)) {
    if xs.len() < 2 || ys.len() < 2 {
        return;
    }
    let (Some(&first_x), Some(&first_y), Some(&last_x), Some(&last_y)) =
        (xs.first(), ys.first(), xs.last(), ys.last())
    else {
        return;
    };
    let first = XY::new(first_x, first_y);
    let last = XY::new(last_x, last_y);
    for ([ax, bx], [ay, by]) in xs.array_windows::<2>().zip(ys.array_windows::<2>()) {
        visit(XY::new(*ax, *ay), XY::new(*bx, *by));
    }
    if !same_point(first, last) {
        visit(last, first);
    }
}

#[cfg(test)]
fn for_each_xy_segment(xs: &[f64], ys: &[f64], mut visit: impl FnMut(XY, XY)) {
    for ([ax, bx], [ay, by]) in xs.array_windows::<2>().zip(ys.array_windows::<2>()) {
        visit(XY::new(*ax, *ay), XY::new(*bx, *by));
    }
}

/// Cartesian location for any source shape.
pub(crate) fn cartesian_point_location(shape: &Shape, point: Point) -> Loc {
    let mut location = StreamingLocation::new(point.xy(), LocationMode::Full);
    let mut owners = None;
    location.visit_shape(shape, &mut owners);
    location.finish()
}

/// Boolean point coverage is a set-membership query: duplicate components can
/// change a line point's boundary/interior label, but not whether it is present.
pub(crate) fn cartesian_point_covers(shape: &Shape, point: Point) -> bool {
    let mut location = StreamingLocation::new(point.xy(), LocationMode::Full);
    let mut shared_owners = SharedShapeOwners::for_collections();
    let mut owners = Some(&mut shared_owners);
    location.visit_shape(shape, &mut owners);
    location.finish() != Loc::Exterior
}

pub(crate) fn cartesian_area_covers_point(shape: &Shape, point: Point) -> bool {
    let mut location = StreamingLocation::new(point.xy(), LocationMode::Area);
    let mut shared_owners = SharedShapeOwners::for_collections();
    let mut owners = Some(&mut shared_owners);
    location.visit_shape(shape, &mut owners);
    location.finish() != Loc::Exterior
}

/// Cartesian location over polygons given as ring views, shell first — the
/// path of prepared point testers, which already hold each ring's kind.
pub(crate) fn cartesian_polygon_views_location<'a, P: IntoIterator<Item = RingView<'a>>>(
    polygons: impl IntoIterator<Item = P>,
    point: Point,
) -> Loc {
    let mut location = StreamingLocation::new(point.xy(), LocationMode::Full);
    location.visit_polygon_views(polygons);
    location.finish()
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

    fn assert_streaming_matches_reference(shape: &Shape, point: Point) {
        let expected = ReferenceSourceSupport::from_shape(shape).location(point);
        let actual = cartesian_point_location(shape, point);
        assert_eq!(actual, expected, "{shape:?} at ({}, {})", point.x, point.y);
        assert_eq!(
            cartesian_point_covers(shape, point),
            expected != Loc::Exterior,
            "coverage for {shape:?} at ({}, {})",
            point.x,
            point.y
        );
    }

    #[test]
    fn streaming_location_matches_source_support_on_degenerate_and_random_inputs() {
        let fixed = [
            (
                "POLYGON ((0 0,10 0,10 10,0 10,0 0),(2 2,2 8,8 8,8 2,2 2))",
                vec![
                    (0.0, 0.0),
                    (5.0, 0.0),
                    (10.0, 5.0),
                    (1.0, 1.0),
                    (2.0, 5.0),
                    (5.0, 5.0),
                    (11.0, 5.0),
                ],
            ),
            (
                "MULTIPOLYGON (((0 0,4 0,4 4,0 4,0 0)),((4 4,8 4,8 8,4 8,4 4)))",
                vec![(0.0, 0.0), (2.0, 0.0), (4.0, 4.0), (6.0, 6.0), (9.0, 9.0)],
            ),
            ("POLYGON ((0 0,4 0,2 0,0 0))", vec![
                (0.0, 0.0),
                (1.0, 0.0),
                (2.0, 0.0),
                (4.0, 0.0),
                (2.0, 1.0),
            ]),
            ("POLYGON ((1 1,1 1,1 1,1 1),(1 1,3 1,3 3,1 3,1 1))", vec![
                (1.0, 1.0),
                (2.0, 2.0),
                (3.0, 1.0),
                (4.0, 4.0),
            ]),
            (
                "GEOMETRYCOLLECTION (POLYGON ((0 0,4 0,4 4,0 4,0 0)),LINESTRING (0 0,2 0),LINESTRING (2 0,4 0),POINT (7 7))",
                vec![
                    (0.0, 0.0),
                    (1.0, 0.0),
                    (2.0, 0.0),
                    (4.0, 4.0),
                    (7.0, 7.0),
                    (8.0, 8.0),
                ],
            ),
            ("POLYGON ((0 0,1 0,1 2e-300,0 2e-300,0 0))", vec![
                (0.0, 0.0),
                (0.5, 0.0),
                (1.0, 1e-300),
                (0.5, 1e-300),
                (2.0, 1e-300),
            ]),
            ("POLYGON ((0 0,5e-324 0,5e-324 5e-324,0 5e-324,0 0))", vec![
                (0.0, 0.0),
                (5e-324, 0.0),
                (5e-324, 5e-324),
                (0.0, 5e-324),
            ]),
            (
                "POLYGON ((1e300 1e300,1.0000000000000002e300 1e300,1.0000000000000002e300 1.0000000000000002e300,1e300 1.0000000000000002e300,1e300 1e300))",
                vec![
                    (1e300, 1e300),
                    (1.000_000_000_000_000_2e300, 1e300),
                    (1.000_000_000_000_000_2e300, 1.000_000_000_000_000_2e300),
                    (1.000_000_000_000_000_2e300, 1.000_000_000_000_000_2e300),
                ],
            ),
        ];

        for (wkt, probes) in fixed {
            let source = shape(wkt);
            for (x, y) in probes {
                assert_streaming_matches_reference(&source, Point::new_raw_xy(x, y));
            }
        }

        let mut seed = 0x8F3D_9A71_2C56_B40D_u64;
        let mut next_coordinate = || {
            seed = seed
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            f64::from(u32::try_from(seed >> 32).expect("upper word fits u32") % 19) - 9.0
        };
        for ring_index in 0..12 {
            let ring_y = f64::from(u32::try_from(ring_index).expect("12 rings fit u32")) - 6.0;
            let mut vertices: Vec<(f64, f64)> = std::iter::repeat_with(|| {
                let x = next_coordinate();
                let y = if ring_index % 4 == 0 {
                    ring_y
                } else {
                    next_coordinate()
                };
                (x, y)
            })
            .take(24)
            .collect();
            if ring_index % 3 == 0 {
                let duplicate = *vertices.get(4).expect("24 generated vertices");
                *vertices.get_mut(5).expect("24 generated vertices") = duplicate;
                let ordinate = vertices.get(12).expect("24 generated vertices").1;
                vertices.get_mut(13).expect("24 generated vertices").1 = ordinate;
            }
            let first = *vertices.first().expect("24 generated vertices");
            vertices.push(first);
            let coordinates = vertices
                .iter()
                .map(|(x, y)| format!("{x} {y}"))
                .collect::<Vec<_>>()
                .join(", ");
            let source = shape(&format!("POLYGON (({coordinates}))"));
            for &(x, y) in &vertices {
                assert_streaming_matches_reference(&source, Point::new_raw_xy(x, y));
            }
            for pair in vertices.windows(2) {
                let [start, end] = pair else {
                    unreachable!();
                };
                assert_streaming_matches_reference(
                    &source,
                    Point::new_raw_xy(f64::midpoint(start.0, end.0), f64::midpoint(start.1, end.1)),
                );
            }
            for _ in 0..24 {
                assert_streaming_matches_reference(
                    &source,
                    Point::new_raw_xy(next_coordinate(), next_coordinate()),
                );
            }
        }
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
        let source = ReferenceSourceSupport::from_shape(&shape);
        let (location, allocations) = crate::test_support::allocation_count(|| {
            source.location(Point::new_raw_xy(2048.25, 0.4))
        });
        assert_eq!(location, Loc::Exterior);
        assert_eq!(allocations, 0);
    }

    #[test]
    fn one_shot_polygon_location_has_no_source_or_contact_allocations() {
        let source = shape("POLYGON ((0 0,10 0,10 10,0 10,0 0),(2 2,2 8,8 8,8 2,2 2))");
        let (location, allocations) = crate::test_support::allocation_count(|| {
            cartesian_point_location(&source, Point::new_raw_xy(1.0, 1.0))
        });
        assert_eq!(location, Loc::Interior);
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
    fn set_membership_skips_shared_owners_but_location_keeps_endpoint_parity() {
        let line = shape("LINESTRING (0 0,1 0)");
        let owner = Shape::geometry_collection(vec![line]).unwrap();
        let shared =
            Shape::geometry_collection(vec![owner.clone(), owner]).expect("shared owner admitted");
        let bytes = crate::io::to_wkb(&shared, None, false).expect("shared input serializes");
        let expanded = crate::io::parse_wkb(&bytes)
            .expect("unshared expansion parses")
            .shape;
        let endpoint = Point::new_raw_xy(0.0, 0.0);

        assert_eq!(
            cartesian_point_location(&shared, endpoint),
            cartesian_point_location(&expanded, endpoint)
        );
        assert_eq!(cartesian_point_location(&shared, endpoint), Loc::Interior);
        assert!(shared.contains_point(endpoint));
        assert!(cartesian_point_covers(&shared, endpoint));
        assert_eq!(
            cartesian_point_covers(&shared, endpoint),
            cartesian_point_covers(&expanded, endpoint)
        );
        assert!(!cartesian_point_covers(
            &shared,
            Point::new_raw_xy(2.0, 0.0)
        ));
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
            .unwrap()
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
