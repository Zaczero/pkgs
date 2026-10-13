use std::cmp::Ordering;

use super::{
    ExactDyadic, ExactQuery, ExactSign, InfinitesimalQuery, Loc, Point, PointQuery, Polygon,
    QueryAxis, Range, Ray, Result, RingEdgeCandidates, RingKind, Shape, SharedShapeOwners,
    SourcePointLocation, StoredQuery, XY, compare_query_rays, line_location, query_ray_groups,
    ring_ray_delta, same_point, storage, stored_orientation_sign, symbolic_orientation_from_sign,
    try_sort_by,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum LocationMode {
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
            && !same_point(end, first)
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
    pub(super) const fn new(query: Q, mode: LocationMode) -> Self {
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

    pub(super) fn finish_location(self) -> Result<Loc> {
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

    pub(super) fn visit_row(
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

    pub(super) fn visit_shape(
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

    pub(super) fn visit_polygon(&mut self, polygon: &Polygon) -> Result<()> {
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

    #[expect(
        clippy::float_cmp,
        reason = "exact source-coordinate equality distinguishes contact from an open ray crossing"
    )]
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
