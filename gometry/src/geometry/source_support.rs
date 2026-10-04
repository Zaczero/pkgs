//! Exact Cartesian source support for point and contact-witness queries.
//!
//! This module owns the source-side fold used by scalar, packed, prepared,
//! and indexed point membership, plus the stored, rational, and infinitesimal
//! probes used by DE-9IM evaluation. It deliberately does not build an
//! arrangement: away from source contact, directed winding is locally
//! constant; at contact, the relevant topology is the exact fan of source
//! rays incident to the query.

#![expect(
    clippy::float_cmp,
    reason = "coordinate identity and formal fan ordering use exact represented values"
)]

use std::cmp::Ordering;
use std::ops::Range;

use crate::error::Result;
#[cfg(test)]
use crate::geometry::Ring;
use crate::geometry::exact_arith::ExactSign;
use crate::geometry::segments::{stored_orientation_sign, try_sort_by};
use crate::geometry::tessellation::exact::{
    ExactDyadic, compare_ratios, outward_ratio_bounds, round_ratio,
};
use crate::geometry::{
    Bounds, Loc, Point, Polygon, Shape, SharedShapeOwners, XY, point_on_segment, same_point,
    same_topological_coordinate, storage,
};

#[derive(Clone, Copy)]
pub(in crate::geometry) enum QueryAxis {
    X,
    Y,
}

#[derive(Clone, Copy)]
pub(in crate::geometry) struct AxisRelation {
    query_below: bool,
    equal: bool,
}

pub(in crate::geometry) trait PointQuery: Copy {
    fn compare_axis(self, axis: QueryAxis, value: f64) -> Result<Ordering>;
    fn orientation(self, start: XY, end: XY) -> Result<ExactSign>;
    fn index_y(self) -> Result<f64>;

    fn axis_relation(self, axis: QueryAxis, value: f64) -> Result<AxisRelation> {
        self.compare_axis(axis, value).map(|order| AxisRelation {
            query_below: order == Ordering::Less,
            equal: order == Ordering::Equal,
        })
    }

    fn misses_height(self, _start: f64, _end: f64) -> bool {
        false
    }

    fn same_point(self, point: XY) -> Result<bool> {
        Ok(self.compare_axis(QueryAxis::X, point.x)? == Ordering::Equal
            && self.compare_axis(QueryAxis::Y, point.y)? == Ordering::Equal)
    }
}

#[derive(Clone, Copy)]
pub(in crate::geometry) struct StoredQuery(XY);

impl PointQuery for StoredQuery {
    fn misses_height(self, start: f64, end: f64) -> bool {
        (start < self.0.y && end < self.0.y) || (start > self.0.y && end > self.0.y)
    }

    fn axis_relation(self, axis: QueryAxis, value: f64) -> Result<AxisRelation> {
        let query = match axis {
            QueryAxis::X => self.0.x,
            QueryAxis::Y => self.0.y,
        };
        Ok(AxisRelation {
            query_below: query < value,
            equal: query == value,
        })
    }

    fn compare_axis(self, axis: QueryAxis, value: f64) -> Result<Ordering> {
        let query = match axis {
            QueryAxis::X => self.0.x,
            QueryAxis::Y => self.0.y,
        };
        Ok(if query < value {
            Ordering::Less
        } else if query > value {
            Ordering::Greater
        } else if same_topological_coordinate(query, value) {
            Ordering::Equal
        } else {
            query.total_cmp(&value)
        })
    }

    fn orientation(self, start: XY, end: XY) -> Result<ExactSign> {
        Ok(stored_orientation_sign(
            start.x, start.y, end.x, end.y, self.0.x, self.0.y,
        ))
    }

    fn index_y(self) -> Result<f64> {
        Ok(self.0.y)
    }
}

#[derive(Clone, Copy)]
pub(in crate::geometry) struct ExactQuery<'a> {
    point: &'a [ExactDyadic; 3],
    x_bounds: (f64, f64),
    y_bounds: (f64, f64),
}

impl<'a> ExactQuery<'a> {
    fn new(point: &'a [ExactDyadic; 3]) -> Result<Self> {
        let [x, y, w] = point;
        Ok(Self {
            point,
            x_bounds: outward_ratio_bounds(x, w)?,
            y_bounds: outward_ratio_bounds(y, w)?,
        })
    }
}

impl PointQuery for ExactQuery<'_> {
    fn compare_axis(self, axis: QueryAxis, value: f64) -> Result<Ordering> {
        let (lower, upper) = match axis {
            QueryAxis::X => self.x_bounds,
            QueryAxis::Y => self.y_bounds,
        };
        if upper < value {
            return Ok(Ordering::Less);
        }
        if lower > value {
            return Ok(Ordering::Greater);
        }
        if lower == upper {
            return Ok(Ordering::Equal);
        }
        compare_exact_axis(self.point, axis, value)
    }

    fn orientation(self, start: XY, end: XY) -> Result<ExactSign> {
        let [x, y, w] = self.point;
        let dx = ExactDyadic::from_f64(end.x).try_subtract(ExactDyadic::from_f64(start.x))?;
        let dy = ExactDyadic::from_f64(end.y).try_subtract(ExactDyadic::from_f64(start.y))?;
        let from_x = x
            .clone()
            .try_subtract(ExactDyadic::from_f64(start.x).try_product(w)?)?;
        let from_y = y
            .clone()
            .try_subtract(ExactDyadic::from_f64(start.y).try_product(w)?)?;
        let determinant = dx
            .try_product(&from_y)?
            .try_subtract(dy.try_product(&from_x)?)?;
        let sign = determinant.sign();
        Ok(if w.sign() == ExactSign::Negative {
            match sign {
                ExactSign::Negative => ExactSign::Positive,
                ExactSign::Positive => ExactSign::Negative,
                ExactSign::Zero => ExactSign::Zero,
            }
        } else {
            sign
        })
    }

    fn index_y(self) -> Result<f64> {
        exact_query_y(self.point, None)
    }

    fn misses_height(self, start: f64, end: f64) -> bool {
        let (lower, upper) = self.y_bounds;
        (start < lower && end < lower) || (start > upper && end > upper)
    }
}

fn compare_exact_axis(point: &[ExactDyadic; 3], axis: QueryAxis, value: f64) -> Result<Ordering> {
    let [x, y, w] = point;
    let numerator = match axis {
        QueryAxis::X => x,
        QueryAxis::Y => y,
    };
    compare_ratios(
        numerator,
        w,
        &ExactDyadic::from_f64(value),
        &ExactDyadic::one(),
    )
}

fn exact_query_coordinate(
    numerator: &ExactDyadic,
    denominator: &ExactDyadic,
    direction: Option<&ExactDyadic>,
) -> Result<f64> {
    let rounded = round_ratio(numerator, denominator)?;
    let order = compare_ratios(
        numerator,
        denominator,
        &ExactDyadic::from_f64(rounded),
        &ExactDyadic::one(),
    )?;
    let below = order == Ordering::Less
        || (order == Ordering::Equal
            && direction.is_some_and(|direction| direction.sign() == ExactSign::Negative));
    Ok(if below { rounded.next_down() } else { rounded })
}

pub(in crate::geometry) fn exact_query_y(
    point: &[ExactDyadic; 3],
    direction_y: Option<&ExactDyadic>,
) -> Result<f64> {
    let [_, y, w] = point;
    exact_query_coordinate(y, w, direction_y)
}

pub(in crate::geometry) fn exact_query_xy(
    point: &[ExactDyadic; 3],
    direction: Option<&[ExactDyadic; 2]>,
) -> Result<XY> {
    let [x, y, w] = point;
    let direction = direction.map(|direction| {
        let [direction_x, direction_y] = direction;
        (direction_x, direction_y)
    });
    Ok(XY::new(
        exact_query_coordinate(x, w, direction.map(|(x, _)| x))?,
        exact_query_coordinate(y, w, direction.map(|(_, y)| y))?,
    ))
}

#[derive(Clone, Copy)]
pub(in crate::geometry) struct InfinitesimalQuery<'a> {
    point: ExactQuery<'a>,
    direction: &'a [ExactDyadic; 2],
}

impl PointQuery for InfinitesimalQuery<'_> {
    fn compare_axis(self, axis: QueryAxis, value: f64) -> Result<Ordering> {
        let order = self.point.compare_axis(axis, value)?;
        if order != Ordering::Equal {
            return Ok(order);
        }
        let [direction_x, direction_y] = self.direction;
        Ok(
            match match axis {
                QueryAxis::X => direction_x.sign(),
                QueryAxis::Y => direction_y.sign(),
            } {
                ExactSign::Negative => Ordering::Less,
                ExactSign::Zero => Ordering::Equal,
                ExactSign::Positive => Ordering::Greater,
            },
        )
    }

    fn orientation(self, start: XY, end: XY) -> Result<ExactSign> {
        let orientation = self.point.orientation(start, end)?;
        if orientation != ExactSign::Zero {
            return Ok(orientation);
        }
        let [direction_x, direction_y] = self.direction;
        let dx = ExactDyadic::from_f64(end.x).try_subtract(ExactDyadic::from_f64(start.x))?;
        let dy = ExactDyadic::from_f64(end.y).try_subtract(ExactDyadic::from_f64(start.y))?;
        Ok(dx
            .try_product(direction_y)?
            .try_subtract(dy.try_product(direction_x)?)?
            .sign())
    }

    fn index_y(self) -> Result<f64> {
        let [_direction_x, direction_y] = self.direction;
        exact_query_y(self.point.point, Some(direction_y))
    }

    fn misses_height(self, start: f64, end: f64) -> bool {
        self.point.misses_height(start, end)
    }
}

#[derive(Clone, Copy)]
pub(crate) enum PointMembership {
    Strict,
    BoundaryInclusive,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::geometry) struct SourcePointLocation {
    pub(in crate::geometry) location: Loc,
    pub(in crate::geometry) area: bool,
}

pub(in crate::geometry) fn exact_stored_point(point: &[ExactDyadic; 3]) -> Option<Point> {
    let [x, y, weight] = point;
    let scale = if weight.is_one() {
        0
    } else if weight.is_two() {
        -1
    } else {
        return None;
    };
    x.to_f64_exact_scaled(scale)
        .zip(y.to_f64_exact_scaled(scale))
        .map(|(x, y)| Point::new_raw_xy(x, y))
}

impl SourcePointLocation {
    /// Project a full node query onto the areal label of a one-dimensional
    /// ring atom. Point and line support cannot label that atom.
    pub(in crate::geometry) const fn edge_atom_location(self) -> Loc {
        if self.area {
            self.location
        } else {
            Loc::Exterior
        }
    }
}

pub(in crate::geometry) trait RingEdgeCandidates {
    fn ring_view(&self) -> RingView<'_>;
    fn visit_stabbed_edges(
        &self,
        y: f64,
        visit: &mut dyn FnMut(XY, XY) -> Result<()>,
    ) -> Result<()>;
}

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

struct RingScanState {
    first: Option<XY>,
    distinct: Option<XY>,
    non_collinear: bool,
    winding: i64,
    formal_winding: i64,
    collapsed_point: bool,
    ray_start: usize,
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
        let query_y_start = query.axis_relation(QueryAxis::Y, start.y)?;
        let query_y_end = query.axis_relation(QueryAxis::Y, end.y)?;
        let at_start = query_y_start.equal && query.same_point(start)?;
        let at_end = query_y_end.equal && query.same_point(end)?;
        let start_above = query_y_start.query_below;
        let end_above = query_y_end.query_below;
        // Opposite strict-above flags encode both half-open crossing directions.
        let crossing = start_above != end_above;
        let horizontal = query_y_start.equal && query_y_end.equal;
        let horizontal_on_segment = horizontal
            && if start.x <= end.x {
                query.compare_axis(QueryAxis::X, start.x)? != Ordering::Less
                    && query.compare_axis(QueryAxis::X, end.x)? != Ordering::Greater
            } else {
                query.compare_axis(QueryAxis::X, end.x)? != Ordering::Less
                    && query.compare_axis(QueryAxis::X, start.x)? != Ordering::Greater
            };
        let orientation = if at_start || at_end {
            Some(ExactSign::Zero)
        } else if crossing {
            Some(query.orientation(start, end)?)
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
        Ok::<_, crate::error::Error>(EdgeProbe {
            on_segment,
            winding,
            formal_winding,
        })
    }};
}

/// Keep per-edge state updates in the scan loop; a helper call costs once per
/// source edge.
macro_rules! scan_ring_edge {
    ($location:expr, $state:expr, $infer_kind:expr, $start:expr, $end:expr) => {{
        let location = &mut *$location;
        let state = $state;
        let infer_kind = $infer_kind;
        let start = $start;
        let end = $end;
        if infer_kind
            && !state.non_collinear
            && let Some(first) = state.first
            && (end.x != first.x || end.y != first.y)
        {
            if let Some(second) = state.distinct {
                state.non_collinear =
                    stored_orientation_sign(first.x, first.y, second.x, second.y, end.x, end.y)
                        != ExactSign::Zero;
            } else {
                state.distinct = Some(end);
            }
        }
        if !location.query.misses_height(start.y, end.y) {
            if same_point(start, end) {
                state.collapsed_point |= location.query.same_point(start)?;
            } else {
                let edge = edge_probe!(start, end, location.query)?;
                if edge.on_segment {
                    if !location.query.same_point(end)? {
                        location.rays.push(Ray {
                            end,
                            ring: None,
                            curve: false,
                            collapsed: false,
                            reverse: false,
                        });
                    }
                    if !location.query.same_point(start)? {
                        location.rays.push(Ray {
                            end: start,
                            ring: None,
                            curve: false,
                            collapsed: false,
                            reverse: true,
                        });
                    }
                } else {
                    state.winding = state.winding.wrapping_add(edge.winding);
                }
                state.formal_winding = state.formal_winding.wrapping_add(edge.formal_winding);
            }
        }
        Ok::<(), crate::error::Error>(())
    }};
}

/// Query-local exact point location. Ordinary probes keep only scalar
/// winding and occupancy state; rays and per-ring state are retained only
/// when the query lies on source linework.
pub(in crate::geometry) struct StreamingLocation<Q: PointQuery> {
    query: Q,
    mode: LocationMode,
    boundary_rings: Vec<BoundaryRing>,
    boundary_polygons: Vec<BoundaryPolygon>,
    rays: Vec<Ray>,
    area_without_contact: bool,
    point_support: bool,
    curve_endpoint_parity: bool,
}

impl StreamingLocation<StoredQuery> {
    pub(in crate::geometry) const fn for_stored_point(point: Point) -> Self {
        Self::new(StoredQuery(point.xy()), LocationMode::Full)
    }
}

impl<'a> StreamingLocation<ExactQuery<'a>> {
    pub(in crate::geometry) fn for_exact_point(point: &'a [ExactDyadic; 3]) -> Result<Self> {
        Ok(Self::new(ExactQuery::new(point)?, LocationMode::Full))
    }
}

impl<'a> StreamingLocation<InfinitesimalQuery<'a>> {
    pub(in crate::geometry) fn for_exact_offset(
        point: &'a [ExactDyadic; 3],
        direction: &'a [ExactDyadic; 2],
    ) -> Result<Self> {
        Ok(Self::new(
            InfinitesimalQuery {
                point: ExactQuery::new(point)?,
                direction,
            },
            LocationMode::Full,
        ))
    }
}

impl<Q: PointQuery> StreamingLocation<Q> {
    const fn new(query: Q, mode: LocationMode) -> Self {
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

    pub(in crate::geometry) fn visit_indexed_polygon_views<'a, R: RingEdgeCandidates + 'a>(
        &mut self,
        rings: impl IntoIterator<Item = &'a R>,
    ) -> Result<()> {
        let y = self.query.index_y()?;
        let mut summary = PolygonSummary::new(self.boundary_rings.len());
        for (index, ring) in rings.into_iter().enumerate() {
            let view = ring.ring_view();
            let scan = self.scan_ring_with_edges(
                view.xs,
                view.ys,
                Some(view.kind),
                index == 0,
                |visit| ring.visit_stabbed_edges(y, visit),
            )?;
            summary.add_ring(index == 0, scan);
        }
        self.finish_polygon(summary);
        Ok(())
    }

    pub(in crate::geometry) fn finish(self) -> Result<SourcePointLocation> {
        if self.rays.is_empty() {
            let area = self.area_without_contact;
            return Ok(SourcePointLocation {
                location: self.location_without_contact(),
                area,
            });
        }
        self.boundary_location()
    }

    fn finish_location(self) -> Result<Loc> {
        if self.rays.is_empty() {
            return Ok(self.location_without_contact());
        }
        self.boundary_location().map(|location| location.location)
    }

    fn location_without_contact(&self) -> Loc {
        if (self.mode == LocationMode::Full && self.point_support) || self.area_without_contact {
            Loc::Interior
        } else {
            Loc::Exterior
        }
    }

    fn visit_row(
        &mut self,
        row: storage::ShapeRow<'_>,
        owners: &mut Option<&mut SharedShapeOwners>,
    ) -> Result<()> {
        use crate::geometry::storage::{PackedRow, ShapeRow};
        match row {
            ShapeRow::Handle(plan) => self.visit_shape(plan.shape(), owners)?,
            ShapeRow::Shape(shape) => self.visit_shape(shape, owners)?,
            ShapeRow::Point(point) => {
                if self.mode == LocationMode::Full && self.query.same_point(point.xy())? {
                    self.point_support = true;
                }
            },
            ShapeRow::Packed(PackedRow::Points {
                coords, physical, ..
            }) => {
                self.visit_row(ShapeRow::Point(coords.point_at(physical)), owners)?;
            },
            ShapeRow::Packed(row @ PackedRow::Lines { coords, .. }) => {
                if self.mode == LocationMode::Full {
                    let window = row.vertex_window().expect("line row has a vertex window");
                    self.visit_curve_columns(
                        coords
                            .xs()
                            .get(window.clone())
                            .expect("admitted line x window"),
                        coords.ys().get(window).expect("admitted line y window"),
                    )?;
                }
            },
            ShapeRow::Packed(row @ PackedRow::Polygons { coords, .. }) => {
                let mut summary = PolygonSummary::new(self.boundary_rings.len());
                for (index, ring) in row
                    .polygon_rings()
                    .expect("polygon row has rings")
                    .enumerate()
                {
                    let window = row
                        .polygon_ring_window(ring)
                        .expect("admitted polygon ring");
                    let scan = self.scan_ring(
                        coords
                            .xs()
                            .get(window.clone())
                            .expect("admitted ring x window"),
                        coords.ys().get(window).expect("admitted ring y window"),
                        None,
                        index == 0,
                    )?;
                    summary.add_ring(index == 0, scan);
                }
                self.finish_polygon(summary);
            },
        }
        Ok(())
    }

    fn visit_shape(
        &mut self,
        shape: &Shape,
        owners: &mut Option<&mut SharedShapeOwners>,
    ) -> Result<()> {
        match shape {
            Shape::Point(point) => {
                if self.mode == LocationMode::Full && self.query.same_point(point.xy())? {
                    self.point_support = true;
                }
            },
            Shape::MultiPoint(points) if self.mode == LocationMode::Full => {
                for point in points.points() {
                    if self.query.same_point(point.xy())? {
                        self.point_support = true;
                        break;
                    }
                }
            },
            Shape::LineString(line) if self.mode == LocationMode::Full => {
                self.visit_curve(line.as_coords())?;
            },
            Shape::MultiLineString(lines) if self.mode == LocationMode::Full => {
                for line in lines {
                    self.visit_curve(line.as_coords())?;
                }
            },
            Shape::Polygon(polygon) => self.visit_polygon(polygon)?,
            Shape::MultiPolygon(polygons) => {
                for polygon in polygons {
                    self.visit_polygon(polygon)?;
                }
            },
            Shape::GeometryCollection(geometries) => {
                if owners.as_mut().is_none_or(|owners| {
                    owners.first_collection(geometries.as_slice(), geometries.members_are_shared())
                }) {
                    for geometry in geometries {
                        self.visit_shape(geometry, owners)?;
                    }
                }
            },
            Shape::MultiPoint(_)
            | Shape::LineString(_)
            | Shape::MultiLineString(_)
            | Shape::Empty(..) => {},
        }
        Ok(())
    }

    fn visit_polygon(&mut self, polygon: &Polygon) -> Result<()> {
        let mut summary = PolygonSummary::new(self.boundary_rings.len());
        for (index, ring) in polygon.rings().enumerate() {
            let scan = self.scan_ring(ring.xs(), ring.ys(), None, index == 0)?;
            summary.add_ring(index == 0, scan);
        }
        self.finish_polygon(summary);
        Ok(())
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

    fn scan_ring(
        &mut self,
        xs: &[f64],
        ys: &[f64],
        kind_hint: Option<RingKind>,
        shell: bool,
    ) -> Result<RingScan> {
        let infer_kind = kind_hint.is_none();
        let mut state = RingScanState {
            first: xs.first().zip(ys.first()).map(|(&x, &y)| XY::new(x, y)),
            distinct: None,
            non_collinear: false,
            winding: 0,
            formal_winding: 0,
            collapsed_point: false,
            ray_start: self.rays.len(),
        };
        if xs.len() >= 2 && ys.len() >= 2 {
            for ([ax, bx], [ay, by]) in xs.array_windows::<2>().zip(ys.array_windows::<2>()) {
                scan_ring_edge!(
                    self,
                    &mut state,
                    infer_kind,
                    XY::new(*ax, *ay),
                    XY::new(*bx, *by)
                )?;
            }
            if let (Some(first), Some(last)) = (
                xs.first().zip(ys.first()).map(|(&x, &y)| XY::new(x, y)),
                xs.last().zip(ys.last()).map(|(&x, &y)| XY::new(x, y)),
            ) && !same_point(first, last)
            {
                scan_ring_edge!(self, &mut state, infer_kind, last, first)?;
            }
        }
        Ok(self.finish_ring(&state, kind_hint, shell))
    }

    fn scan_ring_with_edges(
        &mut self,
        xs: &[f64],
        ys: &[f64],
        kind_hint: Option<RingKind>,
        shell: bool,
        visit_edges: impl FnOnce(&mut dyn FnMut(XY, XY) -> Result<()>) -> Result<()>,
    ) -> Result<RingScan> {
        let mut state = RingScanState {
            first: xs.first().zip(ys.first()).map(|(&x, &y)| XY::new(x, y)),
            distinct: None,
            non_collinear: false,
            winding: 0,
            formal_winding: 0,
            collapsed_point: false,
            ray_start: self.rays.len(),
        };
        {
            let infer_kind = kind_hint.is_none();
            let mut visit_edge =
                |start: XY, end: XY| scan_ring_edge!(self, &mut state, infer_kind, start, end);
            visit_edges(&mut visit_edge)?;
        }
        Ok(self.finish_ring(&state, kind_hint, shell))
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "ray_start is the prior ray length and scanning only appends rays before rewriting that suffix"
    )]
    fn finish_ring(
        &mut self,
        state: &RingScanState,
        kind_hint: Option<RingKind>,
        shell: bool,
    ) -> RingScan {
        let kind = kind_hint.unwrap_or(if state.non_collinear {
            RingKind::Winding
        } else {
            RingKind::Collapsed
        });
        let touched = self.rays.len() != state.ray_start;
        if self.mode == LocationMode::Area && kind == RingKind::Collapsed {
            self.rays.truncate(state.ray_start);
        }
        let boundary_ring =
            if touched && (self.mode == LocationMode::Full || kind == RingKind::Winding) {
                let index = self.boundary_rings.len();
                for ray in &mut self.rays[state.ray_start..] {
                    ray.ring = Some((index, ring_ray_delta(kind, ray.reverse)));
                    ray.collapsed = kind == RingKind::Collapsed;
                }
                self.boundary_rings.push(BoundaryRing {
                    kind,
                    winding: if kind == RingKind::Winding {
                        state.formal_winding
                    } else {
                        0
                    },
                    shell,
                });
                Some(index)
            } else {
                if self.mode == LocationMode::Area {
                    self.rays.truncate(state.ray_start);
                }
                None
            };

        RingScan {
            kind,
            winding: if kind == RingKind::Winding {
                if boundary_ring.is_some() {
                    state.formal_winding
                } else {
                    state.winding
                }
            } else {
                0
            },
            boundary_ring,
            collapsed_point: state.collapsed_point,
        }
    }

    fn visit_curve(&mut self, curve: &crate::geometry::CoordSeq) -> Result<()> {
        self.visit_curve_columns(curve.xs(), curve.ys())
    }

    fn visit_curve_columns(&mut self, xs: &[f64], ys: &[f64]) -> Result<()> {
        let Some((&first_x, &first_y)) = xs.first().zip(ys.first()) else {
            return Ok(());
        };
        let Some((&last_x, &last_y)) = xs.last().zip(ys.last()) else {
            return Ok(());
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
            if edge_probe!(start, end, query)?.on_segment {
                if !query.same_point(end)? {
                    self.rays.push(Ray {
                        end,
                        ring: None,
                        curve: true,
                        collapsed: false,
                        reverse: false,
                    });
                }
                if !query.same_point(start)? {
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
            self.curve_endpoint_parity ^= query.same_point(XY::new(first_x, first_y))?
                ^ query.same_point(XY::new(last_x, last_y))?;
        } else if self.mode == LocationMode::Full {
            for (&x, &y) in xs.iter().zip(ys) {
                if query.same_point(XY::new(x, y))? {
                    self.point_support = true;
                    break;
                }
            }
        }
        Ok(())
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
    fn boundary_location(mut self) -> Result<SourcePointLocation> {
        let query = self.query;
        try_sort_by(&mut self.rays, |left, right| {
            compare_query_rays(query, left.end, right.end)
        })?;
        let groups = query_ray_groups(query, &self.rays)?;
        let mut seed_group = None;
        for (index, group) in groups.iter().enumerate() {
            let ray = &self.rays[group.start];
            if query.compare_axis(QueryAxis::Y, ray.end.y)? == Ordering::Equal
                && query.compare_axis(QueryAxis::X, ray.end.x)? == Ordering::Less
            {
                seed_group = Some(index);
                break;
            }
        }
        let first = if let Some(index) = seed_group {
            index.wrapping_add(1) % groups.len()
        } else {
            let mut first = 0;
            for (index, group) in groups.iter().enumerate() {
                let ray = &self.rays[group.start];
                let y = query.compare_axis(QueryAxis::Y, ray.end.y)?;
                if y == Ordering::Less
                    || (y == Ordering::Equal
                        && query.compare_axis(QueryAxis::X, ray.end.x)? == Ordering::Less)
                {
                    first = index;
                    break;
                }
            }
            first
        };

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
            return Ok(SourcePointLocation {
                location: if any_area {
                    Loc::Interior
                } else {
                    Loc::Exterior
                },
                area: any_area,
            });
        }
        if any_area {
            return Ok(SourcePointLocation {
                location: if all_area {
                    Loc::Interior
                } else {
                    Loc::Boundary
                },
                area: true,
            });
        }
        if line {
            return Ok(SourcePointLocation {
                location: line_location(self.curve_endpoint_parity, residual_collapsed_degree_odd),
                area: false,
            });
        }
        let point_support = self.point_support
            || self.boundary_polygons.iter().any(|polygon| {
                polygon.shell.is_some_and(|shell| shell.collapsed_point)
                    && !polygon.holes.point_support
            });
        if point_support {
            Ok(SourcePointLocation {
                location: Loc::Interior,
                area: false,
            })
        } else {
            Ok(SourcePointLocation {
                location: Loc::Exterior,
                area: false,
            })
        }
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
    reason = "the exact ray events are finite and the following group bounds partition their sorted slice"
)]
fn query_ray_groups<Q: PointQuery>(query: Q, rays: &[Ray]) -> Result<Vec<RayGroup>> {
    let mut groups = Vec::new();
    let mut start = 0_usize;
    while start < rays.len() {
        let mut end = start.wrapping_add(1);
        while end < rays.len()
            && compare_query_rays(query, rays[start].end, rays[end].end)? == Ordering::Equal
        {
            end = end.wrapping_add(1);
        }
        groups.push(RayGroup { start, end });
        start = end;
    }
    Ok(groups)
}

fn compare_query_rays<Q: PointQuery>(query: Q, left: XY, right: XY) -> Result<Ordering> {
    let left_y = query.compare_axis(QueryAxis::Y, left.y)?;
    let right_y = query.compare_axis(QueryAxis::Y, right.y)?;
    let left_upper = left_y == Ordering::Less
        || (left_y == Ordering::Equal
            && query.compare_axis(QueryAxis::X, left.x)? == Ordering::Less);
    let right_upper = right_y == Ordering::Less
        || (right_y == Ordering::Equal
            && query.compare_axis(QueryAxis::X, right.x)? == Ordering::Less);
    if left_upper != right_upper {
        return Ok(right_upper.cmp(&left_upper));
    }
    Ok(match query.orientation(left, right)? {
        ExactSign::Positive => Ordering::Less,
        ExactSign::Negative => Ordering::Greater,
        ExactSign::Zero => Ordering::Equal,
    })
}

#[cfg(test)]
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

pub(in crate::geometry) fn exact_point_in_bounds(
    bounds: Bounds,
    point: &[ExactDyadic; 3],
) -> Result<bool> {
    point_in_bounds(ExactQuery::new(point)?, bounds)
}

pub(in crate::geometry) fn exact_offset_point_in_bounds(
    bounds: Bounds,
    point: &[ExactDyadic; 3],
    direction: &[ExactDyadic; 2],
) -> Result<bool> {
    point_in_bounds(
        InfinitesimalQuery {
            point: ExactQuery::new(point)?,
            direction,
        },
        bounds,
    )
}

fn point_in_bounds(query: impl PointQuery, bounds: Bounds) -> Result<bool> {
    Ok(
        query.compare_axis(QueryAxis::X, bounds.minx())? != Ordering::Less
            && query.compare_axis(QueryAxis::X, bounds.maxx())? != Ordering::Greater
            && query.compare_axis(QueryAxis::Y, bounds.miny())? != Ordering::Less
            && query.compare_axis(QueryAxis::Y, bounds.maxy())? != Ordering::Greater,
    )
}

pub(in crate::geometry) fn exact_edge_winding(
    start: XY,
    end: XY,
    point: &[ExactDyadic; 3],
) -> Result<Option<i64>> {
    edge_winding_for_query(start, end, ExactQuery::new(point)?)
}

pub(in crate::geometry) fn exact_offset_edge_winding(
    start: XY,
    end: XY,
    point: &[ExactDyadic; 3],
    direction: &[ExactDyadic; 2],
) -> Result<Option<i64>> {
    edge_winding_for_query(start, end, InfinitesimalQuery {
        point: ExactQuery::new(point)?,
        direction,
    })
}

fn edge_winding_for_query(start: XY, end: XY, query: impl PointQuery) -> Result<Option<i64>> {
    if same_point(start, end) {
        return Ok(Some(0));
    }
    let (x_start, x_end) = (
        query.compare_axis(QueryAxis::X, start.x)?,
        query.compare_axis(QueryAxis::X, end.x)?,
    );
    let (y_start, y_end) = (
        query.compare_axis(QueryAxis::Y, start.y)?,
        query.compare_axis(QueryAxis::Y, end.y)?,
    );
    let outside_x = (x_start == Ordering::Less && x_end == Ordering::Less)
        || (x_start == Ordering::Greater && x_end == Ordering::Greater);
    let outside_y = (y_start == Ordering::Less && y_end == Ordering::Less)
        || (y_start == Ordering::Greater && y_end == Ordering::Greater);
    let orientation = if !outside_x && !outside_y {
        let sign = query.orientation(start, end)?;
        if sign == ExactSign::Zero {
            return Ok(None);
        }
        Some(sign)
    } else {
        None
    };
    let direction = if y_start != Ordering::Less && y_end == Ordering::Less {
        1
    } else if y_end != Ordering::Less && y_start == Ordering::Less {
        -1
    } else {
        return Ok(Some(0));
    };
    let orientation = match orientation {
        Some(sign) => sign,
        None => query.orientation(start, end)?,
    };
    let expected = if direction > 0 {
        ExactSign::Positive
    } else {
        ExactSign::Negative
    };
    Ok(Some(if orientation == expected {
        direction
    } else {
        0
    }))
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
    let mut location = StreamingLocation::for_stored_point(point);
    let mut owners = None;
    location
        .visit_shape(shape, &mut owners)
        .expect("stored point location does not construct exact values");
    location
        .finish_location()
        .expect("stored point location does not construct exact values")
}

pub(in crate::geometry) fn cartesian_point_location_and_area(
    shape: &Shape,
    point: Point,
) -> SourcePointLocation {
    let mut location = StreamingLocation::for_stored_point(point);
    let mut owners = None;
    location
        .visit_shape(shape, &mut owners)
        .expect("stored point location does not construct exact values");
    location
        .finish()
        .expect("stored point location does not construct exact values")
}

pub(crate) fn cartesian_row_point_location(
    row: storage::ShapeRow<'_>,
    point: Point,
    coverage: bool,
) -> Loc {
    let mut location = StreamingLocation::for_stored_point(point);
    let mut shared = SharedShapeOwners::for_collections();
    let mut owners = coverage.then_some(&mut shared);
    location
        .visit_row(row, &mut owners)
        .expect("stored point location does not construct exact values");
    location
        .finish_location()
        .expect("stored point location does not construct exact values")
}

pub(in crate::geometry) fn cartesian_row_point_location_and_area(
    row: storage::ShapeRow<'_>,
    point: Point,
) -> SourcePointLocation {
    let mut location = StreamingLocation::for_stored_point(point);
    let mut owners = None;
    location
        .visit_row(row, &mut owners)
        .expect("stored point location does not construct exact values");
    location
        .finish()
        .expect("stored point location does not construct exact values")
}

/// Boolean point coverage is a set-membership query: duplicate components can
/// change a line point's boundary/interior label, but not whether it is present.
pub(crate) fn cartesian_point_covers(shape: &Shape, point: Point) -> bool {
    cartesian_row_point_location(shape.into(), point, true) != Loc::Exterior
}

pub(crate) fn cartesian_area_covers_point(shape: &Shape, point: Point) -> bool {
    cartesian_row_area_covers_point(shape.into(), point)
}

pub(crate) fn cartesian_row_area_covers_point(row: storage::ShapeRow<'_>, point: Point) -> bool {
    let mut location = StreamingLocation::new(StoredQuery(point.xy()), LocationMode::Area);
    let mut shared_owners = SharedShapeOwners::for_collections();
    let mut owners = Some(&mut shared_owners);
    location
        .visit_row(row, &mut owners)
        .expect("stored point location does not construct exact values");
    location
        .finish()
        .expect("stored point location does not construct exact values")
        .area
}

pub(in crate::geometry) fn cartesian_exact_point_location_and_area(
    shape: &Shape,
    point: &[ExactDyadic; 3],
) -> Result<SourcePointLocation> {
    if let Some(point) = exact_stored_point(point) {
        return Ok(cartesian_point_location_and_area(shape, point));
    }
    let mut location = StreamingLocation::for_exact_point(point)?;
    let mut owners = None;
    location.visit_shape(shape, &mut owners)?;
    location.finish()
}

#[cfg(test)]
pub(in crate::geometry) fn cartesian_exact_point_location(
    shape: &Shape,
    point: &[ExactDyadic; 3],
) -> Result<Loc> {
    Ok(cartesian_exact_point_location_and_area(shape, point)?.location)
}

pub(in crate::geometry) fn cartesian_exact_offset_location_and_area(
    shape: &Shape,
    point: &[ExactDyadic; 3],
    direction: &[ExactDyadic; 2],
) -> Result<SourcePointLocation> {
    let mut location = StreamingLocation::for_exact_offset(point, direction)?;
    let mut owners = None;
    location.visit_shape(shape, &mut owners)?;
    location.finish()
}

#[cfg(test)]
pub(in crate::geometry) fn cartesian_exact_offset_location(
    shape: &Shape,
    point: &[ExactDyadic; 3],
    direction: &[ExactDyadic; 2],
) -> Result<Loc> {
    Ok(cartesian_exact_offset_location_and_area(shape, point, direction)?.location)
}

pub(in crate::geometry) fn cartesian_row_exact_point_location_and_area(
    row: storage::ShapeRow<'_>,
    point: &[ExactDyadic; 3],
) -> Result<SourcePointLocation> {
    if let Some(point) = exact_stored_point(point) {
        return Ok(cartesian_row_point_location_and_area(row, point));
    }
    let mut location = StreamingLocation::for_exact_point(point)?;
    let mut owners = None;
    location.visit_row(row, &mut owners)?;
    location.finish()
}

pub(in crate::geometry) fn cartesian_row_exact_offset_location_and_area(
    row: storage::ShapeRow<'_>,
    point: &[ExactDyadic; 3],
    direction: &[ExactDyadic; 2],
) -> Result<SourcePointLocation> {
    let mut location = StreamingLocation::for_exact_offset(point, direction)?;
    let mut owners = None;
    location.visit_row(row, &mut owners)?;
    location.finish()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::geometry) enum OccupiedToward {
    Yes,
    No,
    Tangent,
}

pub(in crate::geometry) const fn occupied_toward(location: SourcePointLocation) -> OccupiedToward {
    match (location.location, location.area) {
        (Loc::Interior, true) => OccupiedToward::Yes,
        (Loc::Exterior, _) => OccupiedToward::No,
        _ => OccupiedToward::Tangent,
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::*;

    #[derive(Clone, Copy)]
    struct CountedStoredQuery<'a> {
        query: StoredQuery,
        axis_probes: &'a Cell<usize>,
    }

    impl PointQuery for CountedStoredQuery<'_> {
        fn compare_axis(self, axis: QueryAxis, value: f64) -> Result<Ordering> {
            self.query.compare_axis(axis, value)
        }

        fn axis_relation(self, axis: QueryAxis, value: f64) -> Result<AxisRelation> {
            self.axis_probes.set(self.axis_probes.get().wrapping_add(1));
            self.query.axis_relation(axis, value)
        }

        fn orientation(self, start: XY, end: XY) -> Result<ExactSign> {
            self.query.orientation(start, end)
        }

        fn index_y(self) -> Result<f64> {
            self.query.index_y()
        }

        fn misses_height(self, start: f64, end: f64) -> bool {
            self.query.misses_height(start, end)
        }
    }

    fn shape(wkt: &str) -> Shape {
        crate::io::parse_wkt(wkt).expect("test WKT admits")
    }

    fn location(wkt: &str, x: f64, y: f64) -> Loc {
        cartesian_point_location(&shape(wkt), Point::new_raw_xy(x, y))
    }

    #[test]
    fn stored_ring_scans_skip_edges_outside_the_probe_height() {
        let Shape::Polygon(polygon) = shape(
            "POLYGON ((0 0,1 0,2 0,3 0,4 0,4 1,4 2,4 3,4 4,\
             3 4,2 4,1 4,0 4,0 3,0 2,0 1,0 0))",
        ) else {
            panic!("fixture is a polygon");
        };
        let probes = Cell::new(0);
        let query = CountedStoredQuery {
            query: StoredQuery(XY::new(2.0, 1.5)),
            axis_probes: &probes,
        };
        let mut location = StreamingLocation::new(query, LocationMode::Full);
        location.visit_polygon(&polygon).unwrap();
        assert_eq!(location.finish().unwrap(), SourcePointLocation {
            location: Loc::Interior,
            area: true,
        });
        assert!(probes.get() <= 4, "{} endpoint probes", probes.get());
    }

    #[test]
    fn boundary_tangent_probe_remains_unresolved() {
        let polygon = shape("POLYGON ((0 0,2 0,2 2,0 2,0 0))");
        let plan = crate::geometry::PreparedPlan::transient(&polygon);
        let point = [
            ExactDyadic::from_f64(1.0),
            ExactDyadic::from_f64(0.0),
            ExactDyadic::one(),
        ];
        let tangent = [ExactDyadic::one(), ExactDyadic::from_f64(0.0)];
        let location = plan.locate_exact_offset(&point, &tangent).unwrap();
        assert_eq!(location, SourcePointLocation {
            location: Loc::Boundary,
            area: true,
        });
        assert_eq!(occupied_toward(location), OccupiedToward::Tangent);
    }

    #[test]
    fn edge_atom_label_drops_isolated_point_support_at_an_external_hole_vertex() {
        let source = shape(
            "GEOMETRYCOLLECTION (\
             POLYGON ((0 0,2 0,2 2,0 2,0 0),\
                      (10 10,12 10,12 12,10 12,10 10)),\
             POINT (10 10))",
        );
        let node = cartesian_point_location_and_area(&source, Point::new_raw_xy(10.0, 10.0));
        assert_eq!(node.location, Loc::Interior);
        assert!(!node.area);
        assert_eq!(node.edge_atom_location(), Loc::Exterior);

        let filled = shape(
            "GEOMETRYCOLLECTION (\
             POLYGON ((0 0,20 0,20 20,0 20,0 0)),\
             POINT (5 5))",
        );
        let node = cartesian_point_location_and_area(&filled, Point::new_raw_xy(5.0, 5.0));
        assert_eq!(node.location, Loc::Interior);
        assert!(node.area);
        assert_eq!(node.edge_atom_location(), Loc::Interior);
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

    #[test]
    fn line_interior_in_a_collection_does_not_claim_area() {
        let source =
            shape("GEOMETRYCOLLECTION (POLYGON ((0 0,4 0,4 4,0 4,0 0)),LINESTRING (10 0,14 4))");
        let location = cartesian_point_location_and_area(&source, Point::new_raw_xy(12.0, 2.0));
        assert_eq!(location.location, Loc::Interior);
        assert!(!location.area);

        let point = [
            ExactDyadic::from_f64(12.0),
            ExactDyadic::from_f64(2.0),
            ExactDyadic::one(),
        ];
        let exact = cartesian_exact_point_location_and_area(&source, &point).unwrap();
        assert_eq!(exact, location);
        assert_eq!(
            crate::geometry::PreparedPlan::transient(&source)
                .locate_exact(&point)
                .unwrap(),
            exact
        );
    }

    #[test]
    fn stored_query_axis_order_preserves_topological_identity() {
        let first_nan = f64::from_bits(0x7FF8_0000_0000_0001);
        let second_nan = f64::from_bits(0x7FF8_0000_0000_0002);
        let query = StoredQuery(XY::new(first_nan, 0.0));
        assert_eq!(
            query.compare_axis(QueryAxis::X, first_nan).unwrap(),
            Ordering::Equal
        );
        assert_ne!(
            query.compare_axis(QueryAxis::X, second_nan).unwrap(),
            Ordering::Equal
        );
        assert_eq!(
            query.compare_axis(QueryAxis::Y, -0.0).unwrap(),
            Ordering::Equal
        );

        let finite = StoredQuery(XY::new(1.0, -0.0));
        assert!(finite.axis_relation(QueryAxis::X, 2.0).unwrap().query_below);
        assert!(!finite.axis_relation(QueryAxis::X, 0.0).unwrap().query_below);
        assert!(finite.axis_relation(QueryAxis::Y, 0.0).unwrap().equal);
    }
}
