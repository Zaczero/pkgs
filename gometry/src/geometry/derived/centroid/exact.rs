use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ExactAtRoot;

pub(super) enum CentroidAttempt<T> {
    Ready(T),
    ExactAtRoot(ExactAtRoot),
}

#[derive(Clone)]
pub(super) struct GrowableCentroidSums {
    pub(super) measure: ExactDyadic,
    pub(super) moment_x: ExactDyadic,
    pub(super) moment_y: ExactDyadic,
}

impl GrowableCentroidSums {
    pub(super) fn zero() -> Self {
        Self {
            measure: ExactDyadic::zero(),
            moment_x: ExactDyadic::zero(),
            moment_y: ExactDyadic::zero(),
        }
    }

    pub(super) fn add(self, other: Self) -> CoreResult<Self> {
        Ok(Self {
            measure: self.measure.try_add(other.measure)?,
            moment_x: self.moment_x.try_add(other.moment_x)?,
            moment_y: self.moment_y.try_add(other.moment_y)?,
        })
    }

    pub(super) fn scaled(self, occurrences: usize) -> CoreResult<Self> {
        if occurrences == 1 {
            return Ok(self);
        }
        let factor = ExactDyadic::try_from_u128(
            false,
            u128::try_from(occurrences).expect("a usize occurrence count fits in u128"),
            0,
        )?;
        Ok(Self {
            measure: self.measure.try_product(&factor)?,
            moment_x: self.moment_x.try_product(&factor)?,
            moment_y: self.moment_y.try_product(&factor)?,
        })
    }

    pub(super) fn negate(self) -> Self {
        Self {
            measure: self.measure.neg(),
            moment_x: self.moment_x.neg(),
            moment_y: self.moment_y.neg(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) enum RoundedExactRatio {
    Value(f64),
    OutsideBinary64,
}

pub(super) fn rounded_exact_ratio(
    numerator: &ExactDyadic,
    denominator: &ExactDyadic,
) -> CoreResult<RoundedExactRatio> {
    match round_ratio(numerator, denominator) {
        Ok(value) => Ok(RoundedExactRatio::Value(value)),
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::Geometry(GeometryErrorKind::ExactCoordinateOutOfRange)
            ) =>
        {
            Ok(RoundedExactRatio::OutsideBinary64)
        },
        Err(error) => Err(error),
    }
}

pub(super) fn exact_product(left: f64, right: f64) -> CoreResult<ExactDyadic> {
    ExactDyadic::from_f64(left).try_product(&ExactDyadic::from_f64(right))
}

pub(super) fn exact_cross(x0: f64, y0: f64, x1: f64, y1: f64) -> CoreResult<ExactDyadic> {
    exact_product(x0, y1)?.try_subtract(exact_product(x1, y0)?)
}

pub(super) const fn centroid_coordinate_outside() -> f64 {
    f64::INFINITY
}

pub(super) fn exact_summary_centroid(
    summary: GrowableCentroidSums,
    tier: CentroidDimension,
) -> CoreResult<Option<(f64, f64)>> {
    if summary.measure.is_zero() {
        return Ok(None);
    }
    let denominator = match tier {
        CentroidDimension::Areal => summary.measure.try_product(&ExactDyadic::from_f64(3.0))?,
        CentroidDimension::Lineal | CentroidDimension::Puntal => summary.measure,
    };
    let rounded_x = rounded_exact_ratio(&summary.moment_x, &denominator)?;
    let rounded_y = rounded_exact_ratio(&summary.moment_y, &denominator)?;
    match tier {
        CentroidDimension::Areal => {
            let (RoundedExactRatio::Value(x), RoundedExactRatio::Value(y)) = (rounded_x, rounded_y)
            else {
                return Ok(None);
            };
            Ok(Some((canonicalize_zero(x), canonicalize_zero(y))))
        },
        CentroidDimension::Lineal | CentroidDimension::Puntal => {
            let x = match rounded_x {
                RoundedExactRatio::Value(value) => value,
                RoundedExactRatio::OutsideBinary64 => centroid_coordinate_outside(),
            };
            let y = match rounded_y {
                RoundedExactRatio::Value(value) => value,
                RoundedExactRatio::OutsideBinary64 => centroid_coordinate_outside(),
            };
            let point = (x, y);
            Ok(Some(if tier == CentroidDimension::Lineal {
                (canonicalize_zero(point.0), canonicalize_zero(point.1))
            } else {
                point
            }))
        },
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct CentroidWorkStats {
    pub(super) owner_visits: usize,
    pub(super) member_slots: usize,
    pub(super) source_xy: usize,
    pub(super) cache_hits: usize,
}

pub(super) struct ExactOwnerFold<'owner> {
    tier: CentroidDimension,
    owners: CentroidOwnerMap<ShapeOwnerKey, GrowableCentroidSums>,
    point_chart: Option<crate::geographic::PointSelectionChart>,
    _owner: PhantomData<&'owner Shape>,
    #[cfg(test)]
    work: CentroidWorkStats,
}

impl<'owner> ExactOwnerFold<'owner> {
    pub(super) fn new(tier: CentroidDimension) -> Self {
        Self {
            tier,
            owners: CentroidOwnerMap::default(),
            point_chart: None,
            _owner: PhantomData,
            #[cfg(test)]
            work: CentroidWorkStats::default(),
        }
    }

    pub(super) fn for_point_chart(chart: &crate::geographic::PointSelectionChart) -> Self {
        Self {
            tier: CentroidDimension::Puntal,
            owners: CentroidOwnerMap::default(),
            point_chart: Some(*chart),
            _owner: PhantomData,
            #[cfg(test)]
            work: CentroidWorkStats::default(),
        }
    }

    pub(super) fn point_summary(&self, point: Point) -> CoreResult<GrowableCentroidSums> {
        let Some(chart) = self.point_chart.as_ref() else {
            return Ok(exact_point_summary(point));
        };
        let longitude = chart.longitude_site(point.x)?;
        let x = retained_site_longitude(chart.frame().longitude(), longitude)?;
        Ok(GrowableCentroidSums {
            measure: ExactDyadic::one(),
            moment_x: x,
            moment_y: ExactDyadic::from_f64(point.y),
        })
    }

    pub(super) fn shape_summary(
        &mut self,
        shape: &'owner Shape,
    ) -> CoreResult<GrowableCentroidSums> {
        let owner = if let Shape::GeometryCollection(collection) = shape
            && collection.members_are_shared()
        {
            Some(ShapeOwnerKey::new(collection.as_slice()))
        } else {
            None
        };
        if let Some(key) = owner
            && let Some(summary) = self.owners.get(&key)
        {
            #[cfg(test)]
            {
                self.work.cache_hits = self.work.cache_hits.wrapping_add(1);
            }
            return Ok(summary.clone());
        }

        #[cfg(test)]
        {
            self.work.owner_visits = self.work.owner_visits.wrapping_add(1);
        }
        let summary = match shape {
            Shape::Point(point) if self.tier == CentroidDimension::Puntal => {
                self.point_summary(*point)?
            },
            Shape::MultiPoint(points) if self.tier == CentroidDimension::Puntal => {
                let mut summary = GrowableCentroidSums::zero();
                #[cfg(test)]
                {
                    self.work.member_slots =
                        self.work.member_slots.wrapping_add(points.member_count());
                    self.work.source_xy = self.work.source_xy.wrapping_add(points.len());
                }
                for point in points {
                    summary = summary.add(self.point_summary(point)?)?;
                }
                summary
            },
            Shape::LineString(line)
                if matches!(
                    self.tier,
                    CentroidDimension::Lineal | CentroidDimension::Puntal
                ) =>
            {
                self.sequence_summary(line.as_coords())?
            },
            Shape::MultiLineString(lines)
                if matches!(
                    self.tier,
                    CentroidDimension::Lineal | CentroidDimension::Puntal
                ) =>
            {
                let mut summary = GrowableCentroidSums::zero();
                #[cfg(test)]
                {
                    self.work.member_slots =
                        self.work.member_slots.wrapping_add(lines.member_count());
                }
                for line in lines {
                    summary = summary.add(self.sequence_summary(line.as_coords())?)?;
                }
                summary
            },
            Shape::Polygon(polygon) => self.polygon_summary(polygon)?,
            Shape::MultiPolygon(polygons) => {
                let mut summary = GrowableCentroidSums::zero();
                #[cfg(test)]
                {
                    self.work.member_slots =
                        self.work.member_slots.wrapping_add(polygons.member_count());
                }
                for polygon in polygons {
                    summary = summary.add(self.polygon_summary(polygon)?)?;
                }
                summary
            },
            Shape::GeometryCollection(collection) => {
                #[cfg(test)]
                {
                    self.work.member_slots = self.work.member_slots.wrapping_add(collection.len());
                }
                self.shape_children(collection.as_slice())?
            },
            Shape::Point(_)
            | Shape::MultiPoint(_)
            | Shape::LineString(_)
            | Shape::MultiLineString(_)
            | Shape::Empty(..) => GrowableCentroidSums::zero(),
        };
        if let Some(key) = owner {
            self.owners.insert_new(&key, summary.clone());
        }
        Ok(summary)
    }

    pub(super) fn shape_children(
        &mut self,
        children: &'owner [Shape],
    ) -> CoreResult<GrowableCentroidSums> {
        let mut first_shared: Option<(ShapeOwnerKey, &'owner Shape, usize)> = None;
        let mut indices: Option<HashMap<ShapeOwnerKey, usize>> = None;
        let mut shared = Vec::<(&'owner Shape, usize)>::new();
        let mut summary = GrowableCentroidSums::zero();
        for child in children {
            if let Shape::GeometryCollection(collection) = child
                && collection.members_are_shared()
            {
                let key = ShapeOwnerKey::new(collection.as_slice());
                let occurrences = match &mut first_shared {
                    None => {
                        first_shared = Some((key, child, 1));
                        continue;
                    },
                    Some((first, _, count)) if *first == key => count,
                    Some(_) => match indices
                        .get_or_insert_with(|| HashMap::with_capacity(1))
                        .entry(key)
                    {
                        std::collections::hash_map::Entry::Occupied(entry) => {
                            &mut shared
                                .get_mut(*entry.get())
                                .expect("shared-owner entry ordinal was inserted for this vector")
                                .1
                        },
                        std::collections::hash_map::Entry::Vacant(entry) => {
                            entry.insert(shared.len());
                            shared.push((child, 1));
                            continue;
                        },
                    },
                };
                *occurrences = occurrences
                    .checked_add(1)
                    .expect("one collection's repeated slots fit its slice length");
            } else {
                summary = summary.add(self.shape_summary(child)?)?;
            }
        }
        for (child, occurrences) in first_shared
            .into_iter()
            .map(|(_, child, occurrences)| (child, occurrences))
            .chain(shared)
        {
            let child = self.shape_summary(child)?.scaled(occurrences)?;
            summary = summary.add(child)?;
        }
        Ok(summary)
    }

    pub(super) fn polygon_summary(
        &mut self,
        polygon: &Polygon,
    ) -> CoreResult<GrowableCentroidSums> {
        let mut summary = GrowableCentroidSums::zero();
        let rings = std::iter::once(&polygon.shell).chain(polygon.holes.iter());
        for (index, ring) in rings.enumerate() {
            #[cfg(test)]
            {
                self.work.member_slots = self.work.member_slots.wrapping_add(1);
            }
            let mut ring_summary = self.sequence_summary(ring.coords())?;
            if self.tier == CentroidDimension::Areal {
                if ring_summary.measure.is_zero() {
                    continue;
                }
                let hole = index != 0;
                let reverse = (ring_summary.measure.sign() == ExactSign::Negative) ^ hole;
                if reverse {
                    ring_summary = ring_summary.negate();
                }
            }
            summary = summary.add(ring_summary)?;
        }
        Ok(summary)
    }

    #[cfg_attr(
        not(test),
        expect(
            clippy::needless_pass_by_ref_mut,
            reason = "test-only traversal statistics record the visited coordinates"
        )
    )]
    pub(super) fn sequence_summary(
        &mut self,
        coords: &CoordSeq,
    ) -> CoreResult<GrowableCentroidSums> {
        #[cfg(test)]
        {
            self.work.source_xy = self.work.source_xy.wrapping_add(coords.len());
        }
        let xs = coords.xs();
        let ys = coords.ys();
        debug_assert_eq!(xs.len(), ys.len());
        let mut summary = GrowableCentroidSums::zero();
        match self.tier {
            CentroidDimension::Areal => {
                for ((&x0, &x1), (&y0, &y1)) in xs
                    .iter()
                    .zip(xs.iter().skip(1))
                    .zip(ys.iter().zip(ys.iter().skip(1)))
                {
                    let cross = exact_cross(x0, y0, x1, y1)?;
                    let x_pair = ExactDyadic::from_f64(x0).try_add(ExactDyadic::from_f64(x1))?;
                    let y_pair = ExactDyadic::from_f64(y0).try_add(ExactDyadic::from_f64(y1))?;
                    summary = summary.add(GrowableCentroidSums {
                        measure: cross.clone(),
                        moment_x: cross.try_product(&x_pair)?,
                        moment_y: cross.try_product(&y_pair)?,
                    })?;
                }
            },
            CentroidDimension::Lineal => {
                for ((&x0, &x1), (&y0, &y1)) in xs
                    .iter()
                    .zip(xs.iter().skip(1))
                    .zip(ys.iter().zip(ys.iter().skip(1)))
                {
                    let Some(length) = exact_segment_length(x0, y0, x1, y1)? else {
                        continue;
                    };
                    let midpoint_x = ExactDyadic::from_f64(f64::midpoint(x0, x1));
                    let midpoint_y = ExactDyadic::from_f64(f64::midpoint(y0, y1));
                    summary = summary.add(GrowableCentroidSums {
                        measure: length.clone(),
                        moment_x: length.try_product(&midpoint_x)?,
                        moment_y: length.try_product(&midpoint_y)?,
                    })?;
                }
            },
            CentroidDimension::Puntal => {
                if let Some((&x, &y)) = xs.first().zip(ys.first()) {
                    summary = exact_xy_point_summary(x, y);
                }
            },
        }
        Ok(summary)
    }
}

#[cfg(test)]
pub(super) fn exact_owner_centroid_with_work(
    shape: &Shape,
    tier: CentroidDimension,
) -> CoreResult<(Option<(f64, f64)>, CentroidWorkStats)> {
    let mut fold = ExactOwnerFold::new(tier);
    let summary = fold.shape_summary(shape)?;
    let centroid = exact_summary_centroid(summary, tier)?;
    Ok((centroid, fold.work))
}

pub(super) fn exact_xy_point_summary(x: f64, y: f64) -> GrowableCentroidSums {
    GrowableCentroidSums {
        measure: ExactDyadic::one(),
        moment_x: ExactDyadic::from_f64(x),
        moment_y: ExactDyadic::from_f64(y),
    }
}

pub(super) fn exact_point_summary(point: Point) -> GrowableCentroidSums {
    exact_xy_point_summary(point.x, point.y)
}

pub(super) fn exact_owner_centroid(
    shape: &Shape,
    tier: CentroidDimension,
) -> CoreResult<Option<(f64, f64)>> {
    let mut fold = ExactOwnerFold::new(tier);
    exact_summary_centroid(fold.shape_summary(shape)?, tier)
}

pub(crate) fn exact_point_chart_centroid(
    shape: &Shape,
    chart: &crate::geographic::PointSelectionChart,
) -> CoreResult<Option<(f64, f64)>> {
    let mut fold = ExactOwnerFold::for_point_chart(chart);
    exact_summary_centroid(fold.shape_summary(shape)?, CentroidDimension::Puntal)
}
