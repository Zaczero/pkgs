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
