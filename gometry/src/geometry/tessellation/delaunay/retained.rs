use super::*;

/// Borrowed exact coordinates for one already admitted geographic point chart.
/// Mesh ordinals index `sites`; source ordinals remain untouched in each site.
pub(in crate::geometry::tessellation) struct RetainedPointCoordinates<'a> {
    frame: GeographicFrame,
    sites: &'a [ChartPointSite],
}

impl<'a> RetainedPointCoordinates<'a> {
    pub(in crate::geometry::tessellation) const fn new(
        frame: GeographicFrame,
        sites: &'a [ChartPointSite],
    ) -> Self {
        Self { frame, sites }
    }

    fn site(&self, query: usize) -> ChartPointSite {
        self.sites
            .get(query)
            .copied()
            .expect("mesh site ordinals come from the admitted retained site rows")
    }

    fn exact_x(&self, query: usize) -> Result<exact::ExactDyadic> {
        let site = self.site(query);
        retained_site_longitude(self.frame.longitude(), site.longitude())
    }

    fn exact_y(&self, query: usize) -> exact::ExactDyadic {
        exact::ExactDyadic::from_f64(self.site(query).source_y())
    }

    fn exact_xy(&self, query: usize) -> Result<[exact::ExactDyadic; 2]> {
        Ok([self.exact_x(query)?, self.exact_y(query)])
    }
}

impl exact_delaunay::MeshCoordinates for RetainedPointCoordinates<'_> {
    type Query = usize;
    type Key = (i64, u64, u64);

    fn query(&self, site: usize) -> Self::Query {
        site
    }

    fn key(&self, query: Self::Query) -> Self::Key {
        let site = self.site(query);
        (
            site.longitude().periods(),
            crate::geometry::canonical_f64_bits(site.longitude().native().get()),
            crate::geometry::canonical_f64_bits(site.source_y()),
        )
    }

    fn compare(&self, left: Self::Query, right: Self::Query) -> Ordering {
        self.site(left).compare_chart_xy(self.site(right))
    }

    fn compare_x(&self, left: Self::Query, right: Self::Query) -> Ordering {
        let left = self.site(left);
        let right = self.site(right);
        left.longitude()
            .periods()
            .cmp(&right.longitude().periods())
            .then_with(|| {
                left.longitude()
                    .native()
                    .get()
                    .total_cmp(&right.longitude().native().get())
            })
    }

    fn compare_y(&self, left: Self::Query, right: Self::Query) -> Ordering {
        self.site(left)
            .source_y()
            .total_cmp(&self.site(right).source_y())
    }

    fn orient(&self, [a, b, c]: [Self::Query; 3]) -> Result<ExactSign> {
        let a = self.site(a);
        let b = self.site(b);
        let c = self.site(c);
        retained_site_orientation(self.frame.longitude(), &[
            (a.longitude(), a.source_y()),
            (b.longitude(), b.source_y()),
            (c.longitude(), c.source_y()),
        ])
    }

    fn incircle(&self, [a, b, c, d]: [Self::Query; 4]) -> Result<ExactSign> {
        exact::incircle_dyadic_points(
            self.exact_xy(a)?,
            self.exact_xy(b)?,
            self.exact_xy(c)?,
            self.exact_xy(d)?,
        )
    }

    fn compare_axis_spans(
        &self,
        [min_x, max_x]: [Self::Query; 2],
        [min_y, max_y]: [Self::Query; 2],
    ) -> Result<Ordering> {
        let min_chart_x = self.exact_x(min_x)?;
        let max_chart_x = self.exact_x(max_x)?;
        let min_chart_y = self.exact_y(min_y);
        let max_chart_y = self.exact_y(max_y);
        let x = max_chart_x.try_subtract(min_chart_x)?.abs();
        let y = max_chart_y.try_subtract(min_chart_y)?.abs();
        Ok(x.cmp(&y))
    }

    fn locality_xy(&self, site: usize) -> XY {
        let site = self.site(site);
        // This is only a deterministic BRIO/Hilbert proposal. Exact identity,
        // order, orientation and in-circle never consume these rounded values.
        XY::new(site.longitude().native().get(), site.source_y())
    }
}

/// Build the exact primal mesh from retained chart sites without publishing it.
pub(in crate::geometry::tessellation) fn certified_delaunay_for_retained_points(
    frame: GeographicFrame,
    sites: &[ChartPointSite],
) -> Result<CertifiedDelaunay> {
    let coordinates = RetainedPointCoordinates::new(frame, sites);
    certified_delaunay_with_coordinates(&coordinates, sites.len())
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(super) struct RetainedSiteKey {
    pub(super) periods: i64,
    pub(super) native_x: u64,
    pub(super) y: u64,
}

pub(super) fn retained_site_key(site: ChartPointSite) -> RetainedSiteKey {
    RetainedSiteKey {
        periods: site.longitude().periods(),
        native_x: crate::geometry::canonical_f64_bits(site.longitude().native().get()),
        y: crate::geometry::canonical_f64_bits(site.source_y()),
    }
}

pub(super) fn compare_retained_x(left: ChartPointSite, right: ChartPointSite) -> Ordering {
    left.longitude()
        .periods()
        .cmp(&right.longitude().periods())
        .then_with(|| {
            left.longitude()
                .native()
                .get()
                .total_cmp(&right.longitude().native().get())
        })
}

pub(super) fn compare_retained_y(left: ChartPointSite, right: ChartPointSite) -> Ordering {
    left.source_y().total_cmp(&right.source_y())
}

pub(super) fn retained_between_axis(
    start: ChartPointSite,
    end: ChartPointSite,
    point: ChartPointSite,
    compare: impl Fn(ChartPointSite, ChartPointSite) -> Ordering,
) -> bool {
    let (low, high) = if compare(start, end).is_le() {
        (start, end)
    } else {
        (end, start)
    };
    compare(low, point).is_le() && compare(point, high).is_le()
}

pub(super) fn retained_on_segment(
    frame: GeographicFrame,
    start: ChartPointSite,
    end: ChartPointSite,
    point: ChartPointSite,
) -> Result<bool> {
    Ok(retained_site_orientation(frame.longitude(), &[
        (start.longitude(), start.source_y()),
        (end.longitude(), end.source_y()),
        (point.longitude(), point.source_y()),
    ])? == ExactSign::Zero
        && retained_between_axis(start, end, point, compare_retained_x)
        && retained_between_axis(start, end, point, compare_retained_y))
}

pub(super) fn retained_point_in_cycle(
    frame: GeographicFrame,
    cycle: &[ChartPointSite],
    point: ChartPointSite,
) -> Result<cdt::CdtPointLocation> {
    let mut inside = false;
    for pair in cycle.windows(2) {
        let (Some(&start), Some(&end)) = (pair.first(), pair.get(1)) else {
            continue;
        };
        if retained_on_segment(frame, start, end, point)? {
            return Ok(cdt::CdtPointLocation::Boundary);
        }
        let start_y = compare_retained_y(start, point);
        let end_y = compare_retained_y(end, point);
        let orientation = retained_site_orientation(frame.longitude(), &[
            (start.longitude(), start.source_y()),
            (end.longitude(), end.source_y()),
            (point.longitude(), point.source_y()),
        ])?;
        let crosses = (start_y != Ordering::Greater
            && end_y == Ordering::Greater
            && orientation == ExactSign::Positive)
            || (end_y != Ordering::Greater
                && start_y == Ordering::Greater
                && orientation == ExactSign::Negative);
        if crosses {
            inside = !inside;
        }
    }
    Ok(if inside {
        cdt::CdtPointLocation::Inside
    } else {
        cdt::CdtPointLocation::Outside
    })
}

pub(super) fn compare_retained_coordinate_x(
    left: RetainedLongitudeSite,
    right: RetainedLongitudeSite,
) -> Ordering {
    left.periods()
        .cmp(&right.periods())
        .then_with(|| left.native().get().total_cmp(&right.native().get()))
}

pub(super) fn retained_point_in_coordinate_cycle(
    frame: GeographicFrame,
    cycle: &[(RetainedLongitudeSite, f64)],
    point: ChartPointSite,
) -> Result<cdt::CdtPointLocation> {
    let point_xy = (point.longitude(), point.source_y());
    let mut inside = false;
    for pair in cycle.windows(2) {
        let (Some(&start), Some(&end)) = (pair.first(), pair.get(1)) else {
            continue;
        };
        let start_y = start.1.total_cmp(&point_xy.1);
        let end_y = end.1.total_cmp(&point_xy.1);
        let orientation = retained_site_orientation(frame.longitude(), &[
            (start.0, start.1),
            (end.0, end.1),
            point_xy,
        ])?;
        if orientation == ExactSign::Zero {
            let between_x = {
                let (low, high) = if compare_retained_coordinate_x(start.0, end.0).is_le() {
                    (start.0, end.0)
                } else {
                    (end.0, start.0)
                };
                compare_retained_coordinate_x(low, point_xy.0).is_le()
                    && compare_retained_coordinate_x(point_xy.0, high).is_le()
            };
            let (low, high) = if start.1.total_cmp(&end.1).is_le() {
                (start.1, end.1)
            } else {
                (end.1, start.1)
            };
            if between_x
                && low.total_cmp(&point_xy.1).is_le()
                && point_xy.1.total_cmp(&high).is_le()
            {
                return Ok(cdt::CdtPointLocation::Boundary);
            }
        }
        let crosses = (start_y != Ordering::Greater
            && end_y == Ordering::Greater
            && orientation == ExactSign::Positive)
            || (end_y != Ordering::Greater
                && start_y == Ordering::Greater
                && orientation == ExactSign::Negative);
        if crosses {
            inside = !inside;
        }
    }
    Ok(if inside {
        cdt::CdtPointLocation::Inside
    } else {
        cdt::CdtPointLocation::Outside
    })
}

#[derive(Clone, Copy)]
pub(super) struct RetainedPolarClosure {
    pub(super) owner: RingOwner,
    pub(super) pole: ChartPole,
    pub(super) first: ChartPointSite,
    pub(super) last: ChartPointSite,
}

pub(super) fn retained_chart_area(
    frame: GeographicFrame,
    rings: &[(RingOwner, Vec<ChartPointSite>)],
    polar_closures: &[RetainedPolarClosure],
) -> Result<exact::ExactDyadic> {
    let mut total = exact::ExactDyadic::zero();
    let mut at = 0;
    while let Some((first_owner, _)) = rings.get(at) {
        let polygon = first_owner.polygon();
        let mut polygon_area = exact::ExactDyadic::zero();
        while let Some((owner, sites)) = rings.get(at) {
            if owner.polygon() != polygon {
                break;
            }
            let mut twice_area = exact::ExactDyadic::zero();
            for pair in sites.windows(2) {
                let (Some(&left), Some(&right)) = (pair.first(), pair.get(1)) else {
                    continue;
                };
                let left_x = retained_site_longitude(frame.longitude(), left.longitude())?;
                let right_x = retained_site_longitude(frame.longitude(), right.longitude())?;
                let left_y = exact::ExactDyadic::from_f64(left.source_y());
                let right_y = exact::ExactDyadic::from_f64(right.source_y());
                let cross = left_x
                    .try_product(&right_y)?
                    .try_subtract(left_y.try_product(&right_x)?)?;
                twice_area = twice_area.try_add(cross)?;
            }
            if let Some(closure) = polar_closures.iter().find(|item| item.owner == *owner) {
                let pole_y = exact::ExactDyadic::from_f64(frame.chart_pole_latitude(closure.pole));
                let start_x =
                    retained_site_longitude(frame.longitude(), closure.first.longitude())?;
                let end_x = retained_site_longitude(frame.longitude(), closure.last.longitude())?;
                let start_y = exact::ExactDyadic::from_f64(closure.first.source_y());
                let end_y = exact::ExactDyadic::from_f64(closure.last.source_y());
                let first_cap = end_x
                    .try_product(&pole_y)?
                    .try_subtract(end_y.try_product(&end_x)?)?;
                let pole_edge = end_x
                    .try_product(&pole_y)?
                    .try_subtract(pole_y.try_product(&start_x)?)?;
                let last_cap = start_x
                    .try_product(&start_y)?
                    .try_subtract(pole_y.try_product(&start_x)?)?;
                twice_area = twice_area
                    .try_add(first_cap)?
                    .try_add(pole_edge)?
                    .try_add(last_cap)?;
            }
            let area = twice_area.abs();
            if owner.ring() == 0 {
                polygon_area = polygon_area.try_add(area)?;
            } else {
                polygon_area = polygon_area.try_subtract(area)?;
            }
            at = at
                .checked_add(1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
        }
        total = total.try_add(polygon_area)?;
    }
    total.abs().try_product(&exact::ExactDyadic::from_f64(0.5))
}
