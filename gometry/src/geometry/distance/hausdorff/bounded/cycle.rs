//! Exact cycle location and candidate-boundary intersection.

use super::{
    BoundaryEdge, DdInterval, ExactPoint, ExactSign, FloatBox, Ordering, Rational, Result, XY,
};

pub(super) enum CycleLocation {
    Boundary,
    /// This edge contributes one crossing to a rightward-ray fold.
    Crosses,
    Clear,
}

pub(super) fn edge_location(point: &ExactPoint, edge: &BoundaryEdge) -> Result<CycleLocation> {
    if let (Some(point), Some(left), Some(right)) = (
        point.stored_xy(),
        edge.start.stored_xy(),
        edge.end.stored_xy(),
    ) {
        return ray_edge_location(point, left, right);
    }
    ray_edge_location(point, &edge.start, &edge.end)
}

pub(super) trait RayPoint: Copy {
    fn compare_y(self, other: Self) -> Result<Ordering>;
    fn between_x(self, left: Self, right: Self) -> Result<bool>;
    fn orientation(self, left: Self, right: Self) -> Result<ExactSign>;
}

impl RayPoint for XY {
    fn compare_y(self, other: Self) -> Result<Ordering> {
        Ok(self
            .y
            .partial_cmp(&other.y)
            .expect("prepared coordinates are finite"))
    }

    fn between_x(self, left: Self, right: Self) -> Result<bool> {
        Ok((left.x <= self.x && self.x <= right.x) || (right.x <= self.x && self.x <= left.x))
    }

    fn orientation(self, left: Self, right: Self) -> Result<ExactSign> {
        Ok(match crate::geometry::orientation(left, right, self) {
            crate::geometry::Orientation::Collinear => ExactSign::Zero,
            crate::geometry::Orientation::Clockwise => ExactSign::Negative,
            crate::geometry::Orientation::CounterClockwise => ExactSign::Positive,
        })
    }
}

impl RayPoint for &ExactPoint {
    fn compare_y(self, other: Self) -> Result<Ordering> {
        self.y().try_cmp(other.y())
    }

    fn between_x(self, left: Self, right: Self) -> Result<bool> {
        between(self.x(), left.x(), right.x())
    }

    fn orientation(self, left: Self, right: Self) -> Result<ExactSign> {
        let dx = right.x().try_subtract(left.x())?;
        let dy = right.y().try_subtract(left.y())?;
        let px = self.x().try_subtract(left.x())?;
        let py = self.y().try_subtract(left.y())?;
        Ok(dx
            .try_product(&py)?
            .try_subtract(&dy.try_product(&px)?)?
            .sign())
    }
}

pub(super) fn ray_edge_location<P: RayPoint>(point: P, left: P, right: P) -> Result<CycleLocation> {
    let left_order = left.compare_y(point)?;
    let right_order = right.compare_y(point)?;
    let straddles = (left_order == Ordering::Greater) != (right_order == Ordering::Greater);
    let spans_y = left_order != right_order || left_order == Ordering::Equal;
    let holds_point = spans_y && point.between_x(left, right)?;
    if !straddles && !holds_point {
        return Ok(CycleLocation::Clear);
    }
    let cross = point.orientation(left, right)?;
    if holds_point && cross == ExactSign::Zero {
        return Ok(CycleLocation::Boundary);
    }
    let rising = right_order == Ordering::Greater;
    Ok(
        if straddles
            && ((rising && cross == ExactSign::Positive)
                || (!rising && cross == ExactSign::Negative))
        {
            CycleLocation::Crosses
        } else {
            CycleLocation::Clear
        },
    )
}

pub(super) fn between(value: &Rational, left: &Rational, right: &Rational) -> Result<bool> {
    Ok(
        (value.try_cmp(left)? != Ordering::Less && value.try_cmp(right)? != Ordering::Greater)
            || (value.try_cmp(left)? != Ordering::Greater
                && value.try_cmp(right)? != Ordering::Less),
    )
}

pub(super) fn segment_box_intersection(
    edge: &BoundaryEdge,
    bounds: FloatBox,
) -> Result<Option<ExactPoint>> {
    if point_in_box(&edge.start, bounds)? {
        return Ok(Some(edge.start.clone()));
    }
    if point_in_box(&edge.end, bounds)? {
        return Ok(Some(edge.end.clone()));
    }
    let dx = edge.end.x().try_subtract(edge.start.x())?;
    let dy = edge.end.y().try_subtract(edge.start.y())?;
    for x in [bounds.min_x, bounds.max_x] {
        if dx.is_zero() {
            continue;
        }
        let x = Rational::from_f64(x);
        let parameter = x.try_subtract(edge.start.x())?.try_quotient(&dx)?;
        if parameter.try_cmp(&Rational::zero())? == Ordering::Less
            || parameter.try_cmp(&Rational::one())? == Ordering::Greater
        {
            continue;
        }
        let y = edge.start.y().try_add(&dy.try_product(&parameter)?)?;
        if y.try_cmp(&Rational::from_f64(bounds.min_y))? != Ordering::Less
            && y.try_cmp(&Rational::from_f64(bounds.max_y))? != Ordering::Greater
        {
            return Ok(Some(ExactPoint::new(x, y)));
        }
    }
    for y in [bounds.min_y, bounds.max_y] {
        if dy.is_zero() {
            continue;
        }
        let y = Rational::from_f64(y);
        let parameter = y.try_subtract(edge.start.y())?.try_quotient(&dy)?;
        if parameter.try_cmp(&Rational::zero())? == Ordering::Less
            || parameter.try_cmp(&Rational::one())? == Ordering::Greater
        {
            continue;
        }
        let x = edge.start.x().try_add(&dx.try_product(&parameter)?)?;
        if x.try_cmp(&Rational::from_f64(bounds.min_x))? != Ordering::Less
            && x.try_cmp(&Rational::from_f64(bounds.max_x))? != Ordering::Greater
        {
            return Ok(Some(ExactPoint::new(x, y)));
        }
    }
    Ok(None)
}

pub(super) fn point_in_box(point: &ExactPoint, bounds: FloatBox) -> Result<bool> {
    let min_x = Rational::from_f64(bounds.min_x);
    let max_x = Rational::from_f64(bounds.max_x);
    let min_y = Rational::from_f64(bounds.min_y);
    let max_y = Rational::from_f64(bounds.max_y);
    Ok(point.x().try_cmp(&min_x)? != Ordering::Less
        && point.x().try_cmp(&max_x)? != Ordering::Greater
        && point.y().try_cmp(&min_y)? != Ordering::Less
        && point.y().try_cmp(&max_y)? != Ordering::Greater)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum BoxSplitStrategy {
    FourWay,
    LongestAxis,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum BoxSplitAxis {
    X,
    Y,
}

pub(super) fn normalized_box_extent(min: f64, max: f64, scale: f64) -> Option<(f64, f64)> {
    let min = min / scale;
    let max = max / scale;
    if !(min.is_finite() && max.is_finite()) {
        return None;
    }
    let lower = (max.next_down() - min.next_up()).next_down().max(0.0);
    let upper = (max.next_up() - min.next_down()).next_up();
    (lower.is_finite() && upper.is_finite() && lower <= upper).then_some((lower, upper))
}

pub(super) fn compare_box_extents(bounds: FloatBox) -> Option<Ordering> {
    let x = DdInterval::difference(bounds.max_x, bounds.min_x).map(DdInterval::outward_bounds);
    let y = DdInterval::difference(bounds.max_y, bounds.min_y).map(DdInterval::outward_bounds);
    if let (Some(x), Some(y)) = (x, y) {
        if x.0 > y.1 {
            return Some(Ordering::Greater);
        }
        if y.0 > x.1 {
            return Some(Ordering::Less);
        }
        if x.0.to_bits() == x.1.to_bits()
            && y.0.to_bits() == y.1.to_bits()
            && x.0.to_bits() == y.0.to_bits()
        {
            return Some(Ordering::Equal);
        }
    }

    let scale = bounds
        .min_x
        .abs()
        .max(bounds.max_x.abs())
        .max(bounds.min_y.abs())
        .max(bounds.max_y.abs());
    if scale == 0.0 {
        return Some(Ordering::Equal);
    }
    let x = normalized_box_extent(bounds.min_x, bounds.max_x, scale)?;
    let y = normalized_box_extent(bounds.min_y, bounds.max_y, scale)?;
    if x.0 > y.1 {
        Some(Ordering::Greater)
    } else if y.0 > x.1 {
        Some(Ordering::Less)
    } else if x.0.to_bits() == x.1.to_bits()
        && y.0.to_bits() == y.1.to_bits()
        && x.0.to_bits() == y.0.to_bits()
    {
        Some(Ordering::Equal)
    } else {
        None
    }
}
