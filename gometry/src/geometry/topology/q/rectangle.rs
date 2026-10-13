use super::*;

/// Spatial relation between a source and a supported, nondegenerate rectangle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ExactGeographicRectangleLocation {
    Outside,
    Boundary,
    Interior,
}

pub(in crate::geometry::topology::q) struct ExactQRectangleWindows {
    pub(in crate::geometry::topology::q) first: ExactQRectangle,
    pub(in crate::geometry::topology::q) second: Option<ExactQRectangle>,
}

impl ExactQRectangleWindows {
    pub(in crate::geometry::topology::q) fn new(
        frame: GeographicFrame,
        cut: &ExactRational,
        bounds: Bounds,
    ) -> Result<Option<Self>> {
        let [min_x, min_y, max_x, max_y] =
            [bounds.minx(), bounds.miny(), bounds.maxx(), bounds.maxy()];
        if ![min_x, min_y, max_x, max_y].into_iter().all(f64::is_finite)
            || min_x >= max_x
            || min_y >= max_y
            || min_x <= -frame.longitude().half_period()
            || max_x >= frame.longitude().half_period()
            || min_y <= -frame.pole()
            || max_y >= frame.pole()
        {
            return Ok(None);
        }
        let axis = frame.longitude();
        let (Ok(canonical_min), Ok(canonical_max)) =
            (axis.canonicalize(min_x), axis.canonicalize(max_x))
        else {
            return Ok(None);
        };
        if canonical_min.get().to_bits() != min_x.to_bits()
            || canonical_max.get().to_bits() != max_x.to_bits()
        {
            return Ok(None);
        }
        let center_x = f64::midpoint(min_x, max_x);
        let center_y = f64::midpoint(min_y, max_y);
        if !(min_x < center_x && center_x < max_x && min_y < center_y && center_y < max_y) {
            return Ok(None);
        }

        let min_x = ExactRational::from_f64(min_x);
        let max_x = ExactRational::from_f64(max_x);
        let min_y = ExactRational::from_f64(min_y);
        let max_y = ExactRational::from_f64(max_y);
        let period = ExactRational::from_f64(axis.period());
        if !max_x.try_subtract(&min_x)?.try_cmp(&period)?.is_lt() {
            return Ok(None);
        }
        // A rectangle crossing Q's artificial cut is split into its two
        // chart windows; seam and pole rectangles above decline to pair-Q.
        let crosses_cut = min_x.try_cmp(cut)?.is_lt() && max_x.try_cmp(cut)?.is_gt();
        let (first, second) = if crosses_cut {
            let cut_east = cut.try_add(&period)?;
            (
                ExactQRectangle::new(cut.clone(), max_x, min_y.clone(), max_y.clone()),
                Some(ExactQRectangle::new(
                    min_x.try_add(&period)?,
                    cut_east,
                    min_y,
                    max_y,
                )),
            )
        } else if max_x.try_cmp(cut)?.is_le() {
            (
                ExactQRectangle::new(
                    min_x.try_add(&period)?,
                    max_x.try_add(&period)?,
                    min_y,
                    max_y,
                ),
                None,
            )
        } else {
            (ExactQRectangle::new(min_x, max_x, min_y, max_y), None)
        };
        Ok(Some(Self { first, second }))
    }

    pub(in crate::geometry::topology::q) fn iter(&self) -> impl Iterator<Item = &ExactQRectangle> {
        std::iter::once(&self.first).chain(self.second.iter())
    }
}

pub(in crate::geometry::topology::q) struct ExactQRectangle {
    pub(in crate::geometry::topology::q) min_x: ExactRational,
    pub(in crate::geometry::topology::q) min_y: ExactRational,
    pub(in crate::geometry::topology::q) max_x: ExactRational,
    pub(in crate::geometry::topology::q) max_y: ExactRational,
    pub(in crate::geometry::topology::q) corners: [ExactChartPoint; 4],
}

impl ExactQRectangle {
    pub(in crate::geometry::topology::q) fn new(
        min_x: ExactRational,
        max_x: ExactRational,
        min_y: ExactRational,
        max_y: ExactRational,
    ) -> Self {
        let corners = [
            ExactChartPoint::from_rationals(min_x.clone(), min_y.clone()),
            ExactChartPoint::from_rationals(max_x.clone(), min_y.clone()),
            ExactChartPoint::from_rationals(max_x.clone(), max_y.clone()),
            ExactChartPoint::from_rationals(min_x.clone(), max_y.clone()),
        ];
        Self {
            min_x,
            min_y,
            max_x,
            max_y,
            corners,
        }
    }

    pub(in crate::geometry::topology::q) fn outward_query_segment(
        &self,
    ) -> Result<Option<Segment>> {
        let (min_x, _) = self.min_x.outward_bounds()?;
        let (min_y, _) = self.min_y.outward_bounds()?;
        let (_, max_x) = self.max_x.outward_bounds()?;
        let (_, max_y) = self.max_y.outward_bounds()?;
        if ![min_x, min_y, max_x, max_y].into_iter().all(f64::is_finite) {
            return Ok(None);
        }
        Ok(Some(Segment {
            start: crate::geometry::XY::new(min_x, min_y),
            end: crate::geometry::XY::new(max_x, max_y),
        }))
    }

    pub(in crate::geometry::topology::q) fn contains_closed(
        &self,
        point: &ExactChartPoint,
    ) -> Result<bool> {
        Ok(point.x.try_cmp(&self.min_x)?.is_ge()
            && point.x.try_cmp(&self.max_x)?.is_le()
            && point.y.try_cmp(&self.min_y)?.is_ge()
            && point.y.try_cmp(&self.max_y)?.is_le())
    }

    pub(in crate::geometry::topology::q) fn segment_intersects_closed(
        &self,
        start: &ExactChartPoint,
        end: &ExactChartPoint,
    ) -> Result<bool> {
        if self.contains_closed(start)? || self.contains_closed(end)? {
            return Ok(true);
        }
        let [southwest, southeast, northeast, northwest] = &self.corners;
        for (left, right) in [
            (southwest, southeast),
            (southeast, northeast),
            (northeast, northwest),
            (northwest, southwest),
        ] {
            if exact_segments_intersect(start, end, left, right)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub(in crate::geometry::topology::q) fn segment_intersects_open(
        &self,
        start: &ExactChartPoint,
        end: &ExactChartPoint,
    ) -> Result<bool> {
        let mut low = ExactRational::zero();
        let mut high = ExactRational::one();
        let delta_x = end.x.try_subtract(&start.x)?;
        if !clip_open_parameter(
            &start.x,
            &delta_x,
            &self.min_x,
            &self.max_x,
            &mut low,
            &mut high,
        )? {
            return Ok(false);
        }
        let delta_y = end.y.try_subtract(&start.y)?;
        clip_open_parameter(
            &start.y,
            &delta_y,
            &self.min_y,
            &self.max_y,
            &mut low,
            &mut high,
        )
    }
}

pub(in crate::geometry::topology::q) fn clip_open_parameter(
    origin: &ExactRational,
    delta: &ExactRational,
    minimum: &ExactRational,
    maximum: &ExactRational,
    low: &mut ExactRational,
    high: &mut ExactRational,
) -> Result<bool> {
    if delta.is_zero() {
        return Ok(origin.try_cmp(minimum)?.is_gt() && origin.try_cmp(maximum)?.is_lt());
    }
    let at_minimum = minimum.try_subtract(origin)?.try_quotient(delta)?;
    let at_maximum = maximum.try_subtract(origin)?.try_quotient(delta)?;
    let (lower, upper) = if at_minimum.try_cmp(&at_maximum)?.is_lt() {
        (at_minimum, at_maximum)
    } else {
        (at_maximum, at_minimum)
    };
    if lower.try_cmp(low)?.is_gt() {
        *low = lower;
    }
    if upper.try_cmp(high)?.is_lt() {
        *high = upper;
    }
    Ok(low.try_cmp(high)?.is_lt())
}
