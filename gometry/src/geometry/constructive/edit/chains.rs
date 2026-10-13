use super::*;

/// Length-preserving lifts of the [`CoordSeq`] editors above. Each rebuilds
/// every column with the same vertex count, so a `LineSeq`'s "at least two
/// vertices" invariant carries through and no call site re-checks it. A
/// transform that can drop vertices is not here — it goes through
/// [`LineSeq::try_new`].
impl LineSeq {
    pub(crate) fn try_map_xy_with<E>(
        &self,
        op: impl FnMut(f64, f64) -> Result<(f64, f64), E>,
    ) -> Result<Self, E>
    where
        E: From<crate::error::Error>,
    {
        self.as_coords().try_map_xy_with(op).map(Self::from_proven)
    }

    pub(crate) fn quantize(&self, precision: i32) -> Result<Self> {
        Ok(Self::from_proven(self.as_coords().quantize(precision)?))
    }

    pub(crate) fn force_2d(&self) -> Result<Self> {
        Ok(Self::from_proven(self.as_coords().force_2d()?))
    }

    pub(crate) fn reversed(&self) -> Result<Self> {
        Ok(Self::from_proven(self.as_coords().reversed()?))
    }

    /// The canonical presentation of this line (see
    /// [`crate::geometry::constructive::ring::normalized_line`]) — a rotation
    /// and/or reversal, so the vertex count is unchanged.
    pub(crate) fn normalized(&self) -> Result<Self> {
        Ok(Self::from_proven(normalized_line(self.as_coords())?))
    }

    /// Repeated-vertex removal. The underlying pass keeps the first and last
    /// vertex when a line would otherwise collapse to a point, so a line stays
    /// a line.
    pub(crate) fn remove_repeated_points(&self, tolerance: f64) -> Result<Self> {
        Ok(Self::from_proven(remove_repeated_line_points(
            self, tolerance,
        )?))
    }

    /// Densification and segmentization only insert vertices between existing
    /// ones, so the result is at least as long as the input.
    pub(crate) fn densify(
        &self,
        fraction: f64,
        placement: &SegmentPlacement<'_>,
        budget: &mut ExpansionBudget,
    ) -> Result<Self> {
        crate::geometry::densify_points_budgeted_with_placement(self, fraction, placement, budget)
            .map(Self::from_proven)
    }

    pub(crate) fn segmentize(
        &self,
        max_segment_length: f64,
        placement: &SegmentPlacement<'_>,
        budget: &mut ExpansionBudget,
    ) -> Result<Self> {
        segmentize_points_budgeted(self, max_segment_length, placement, budget)
            .map(Self::from_proven)
    }
}

impl Shape {
    pub fn map_points<F>(&self, transform: &F) -> Result<Self>
    where
        F: Fn(Point) -> Result<Point> + ?Sized,
    {
        match self {
            Self::Point(point) => Ok(Self::Point(transform(*point)?)),
            Self::MultiPoint(points) => Ok(Self::MultiPoint(points.try_map_values(transform)?)),
            Self::LineString(points) => Ok(Self::LineString(LineSeq::from_proven(
                points.as_coords().try_map_points(transform)?,
            ))),
            Self::MultiLineString(lines) => {
                let values = lines
                    .as_slice()
                    .iter()
                    .map(|line| {
                        line.as_coords()
                            .try_map_points(transform)
                            .map(LineSeq::from_proven)
                    })
                    .collect::<Result<Vec<_>, crate::error::Error>>()?;
                let axes = values
                    .first()
                    .map_or_else(|| lines.axes(), |line| line.axes());
                Ok(Self::MultiLineString(lines.try_with_values(axes, values)?))
            },
            Self::Polygon(polygon) => Ok(Self::Polygon(polygon.map_points(transform)?)),
            Self::MultiPolygon(polygons) => {
                let values = polygons
                    .as_slice()
                    .iter()
                    .map(|polygon| polygon.map_points(transform))
                    .collect::<Result<Vec<_>, _>>()?;
                let axes = values
                    .first()
                    .map_or_else(|| polygons.axes(), Polygon::axes);
                Ok(Self::MultiPolygon(polygons.try_with_values(axes, values)?))
            },
            Self::GeometryCollection(geometries) => Ok(Self::GeometryCollection(
                geometries.try_map_members(|geometry| geometry.map_points(transform))?,
            )),
            Self::Empty(..) => Ok(self.clone()),
        }
    }
}

impl Shape {
    /// Apply `transform` to every coordinate chain (lines and rings),
    /// keeping the structure; `None` from the transform reuses the
    /// original chain (cheap column clone, no re-materialization).
    pub(crate) fn map_chains(
        &self,
        transform: &mut impl FnMut(&CoordSeq) -> Result<Option<CoordSeq>>,
    ) -> Result<Self> {
        Ok(match self {
            Self::Point(_) | Self::MultiPoint(_) | Self::Empty(..) => self.clone(),
            Self::LineString(points) => Self::LineString(
                transform(points)?.map_or_else(|| points.clone(), LineSeq::from_proven),
            ),
            Self::MultiLineString(lines) => {
                Self::MultiLineString(lines.try_map_values(lines.axes(), |line| {
                    Ok::<_, crate::error::Error>(
                        transform(line)?.map_or_else(|| line.clone(), LineSeq::from_proven),
                    )
                })?)
            },
            Self::Polygon(polygon) => Self::Polygon(
                polygon
                    .map_ring_seqs(|ring| Ok(transform(ring)?.unwrap_or_else(|| ring.clone())))?,
            ),
            Self::MultiPolygon(polygons) => {
                Self::MultiPolygon(polygons.try_map_values(polygons.axes(), |polygon| {
                    polygon
                        .map_ring_seqs(|ring| Ok(transform(ring)?.unwrap_or_else(|| ring.clone())))
                })?)
            },
            Self::GeometryCollection(geometries) => Self::GeometryCollection(
                geometries.try_map_members(|geometry| geometry.map_chains(transform))?,
            ),
        })
    }

    /// Visit corresponding line and ring chains from a geometry and its
    /// `map_chains` result with each chain's global site-coordinate offset.
    pub(crate) fn any_paired_chain(
        &self,
        support: &Self,
        visitor: &mut impl FnMut(usize, usize, &CoordSeq, &CoordSeq) -> Result<bool>,
    ) -> Result<bool> {
        #[derive(Default)]
        struct Offsets {
            sites: usize,
            chain: usize,
        }

        fn advance(offset: &mut usize, amount: usize) -> Result<()> {
            *offset = offset
                .checked_add(amount)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            Ok(())
        }

        fn visit_chain(
            sites: &CoordSeq,
            support: &CoordSeq,
            offsets: &mut Offsets,
            visitor: &mut impl FnMut(usize, usize, &CoordSeq, &CoordSeq) -> Result<bool>,
        ) -> Result<bool> {
            let chain = offsets.chain;
            offsets.chain = (offsets.chain)
                .checked_add(1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            let stop = visitor(chain, offsets.sites, sites, support)?;
            advance(&mut offsets.sites, sites.len())?;
            Ok(stop)
        }

        fn visit_shape(
            sites: &Shape,
            support: &Shape,
            offsets: &mut Offsets,
            visitor: &mut impl FnMut(usize, usize, &CoordSeq, &CoordSeq) -> Result<bool>,
        ) -> Result<bool> {
            match (sites, support) {
                (Shape::Point(_), Shape::Point(_)) => advance(&mut offsets.sites, 1)?,
                (Shape::MultiPoint(sites), Shape::MultiPoint(_)) => {
                    advance(&mut offsets.sites, sites.len())?;
                },
                (Shape::LineString(sites), Shape::LineString(support)) => {
                    if visit_chain(sites, support, offsets, visitor)? {
                        return Ok(true);
                    }
                },
                (Shape::MultiLineString(sites), Shape::MultiLineString(support)) => {
                    if sites.member_count() != support.member_count() {
                        return Err(GeometryErrorKind::message(
                            "paired geometries have different line member counts",
                        ));
                    }
                    for (site, working) in sites
                        .members_with_values()
                        .zip(support.members_with_values())
                    {
                        match (site, working) {
                            (
                                crate::geometry::MultiMember::Value(site),
                                crate::geometry::MultiMember::Value(working),
                            ) => {
                                if visit_chain(site, working, offsets, visitor)? {
                                    return Ok(true);
                                }
                            },
                            (
                                crate::geometry::MultiMember::Empty,
                                crate::geometry::MultiMember::Empty,
                            ) => {},
                            _ => {
                                return Err(GeometryErrorKind::message(
                                    "paired geometries have different line member layouts",
                                ));
                            },
                        }
                    }
                },
                (Shape::Polygon(sites), Shape::Polygon(support)) => {
                    if sites.rings().count() != support.rings().count() {
                        return Err(GeometryErrorKind::message(
                            "paired geometries have different polygon ring counts",
                        ));
                    }
                    for (site, working) in sites.rings().zip(support.rings()) {
                        if visit_chain(site.coords(), working.coords(), offsets, visitor)? {
                            return Ok(true);
                        }
                    }
                },
                (Shape::MultiPolygon(sites), Shape::MultiPolygon(support)) => {
                    if sites.member_count() != support.member_count() {
                        return Err(GeometryErrorKind::message(
                            "paired geometries have different polygon member counts",
                        ));
                    }
                    for (site, working) in sites
                        .members_with_values()
                        .zip(support.members_with_values())
                    {
                        match (site, working) {
                            (
                                crate::geometry::MultiMember::Value(site),
                                crate::geometry::MultiMember::Value(working),
                            ) => {
                                if site.rings().count() != working.rings().count() {
                                    return Err(GeometryErrorKind::message(
                                        "paired geometries have different polygon ring counts",
                                    ));
                                }
                                for (site, working) in site.rings().zip(working.rings()) {
                                    if visit_chain(
                                        site.coords(),
                                        working.coords(),
                                        offsets,
                                        visitor,
                                    )? {
                                        return Ok(true);
                                    }
                                }
                            },
                            (
                                crate::geometry::MultiMember::Empty,
                                crate::geometry::MultiMember::Empty,
                            ) => {},
                            _ => {
                                return Err(GeometryErrorKind::message(
                                    "paired geometries have different polygon member layouts",
                                ));
                            },
                        }
                    }
                },
                (Shape::GeometryCollection(sites), Shape::GeometryCollection(support)) => {
                    if sites.len() != support.len() {
                        return Err(GeometryErrorKind::message(
                            "paired geometries have different collection member counts",
                        ));
                    }
                    for (site, working) in sites.iter().zip(support.iter()) {
                        if visit_shape(site, working, offsets, visitor)? {
                            return Ok(true);
                        }
                    }
                },
                (Shape::Empty(site_kind, _), Shape::Empty(support_kind, _))
                    if site_kind == support_kind => {},
                _ => {
                    return Err(GeometryErrorKind::message(
                        "paired geometries have different structures",
                    ));
                },
            }
            Ok(false)
        }

        visit_shape(self, support, &mut Offsets::default(), visitor)
    }
}
