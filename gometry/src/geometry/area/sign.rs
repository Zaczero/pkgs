use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AreaSign {
    Negative,
    Zero,
    Positive,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RingWinding {
    Clockwise,
    CounterClockwise,
    Degenerate,
}

impl RingWinding {
    pub(crate) const fn is_ccw(self) -> bool {
        matches!(self, Self::CounterClockwise)
    }

    pub(crate) const fn is_degenerate(self) -> bool {
        matches!(self, Self::Degenerate)
    }

    /// Reverse the ring when its winding does not match `clockwise`.
    pub(crate) const fn reverse_for_clockwise(self, clockwise: bool) -> bool {
        matches!(self, Self::CounterClockwise) == clockwise
    }
}

pub(super) const fn area_sign_to_winding(sign: AreaSign) -> RingWinding {
    match sign {
        AreaSign::Negative => RingWinding::Clockwise,
        AreaSign::Zero => RingWinding::Degenerate,
        AreaSign::Positive => RingWinding::CounterClockwise,
    }
}

pub(super) const EPSILON: f64 = f64::EPSILON;

/// The exact area sign of a CLOSED ring stored as columns, whose edges are the
/// contiguous pairs `(i, i + 1)`.
///
/// Staging, validity, canonicalization and areal topology share this
/// original-coordinate predicate, independently of rounded area measurement.
pub(crate) fn exact_ring_area_sign_columns(xs: &[f64], ys: &[f64], pairs: usize) -> AreaSign {
    if pairs == 0 {
        return AreaSign::Zero;
    }
    if let Some(sign) = rounded_translated_cycle_sign_columns(xs, ys, pairs) {
        return sign;
    }

    let mut sum = ExactSum::zero();
    for (&[x0, x1], &[y0, y1]) in xs
        .array_windows::<2>()
        .zip(ys.array_windows::<2>())
        .take(pairs)
    {
        sum.add_product(x0, y1);
        sum.subtract_product(x1, y0);
    }
    area_sign_from_ordering(sum.ordering())
}

pub(super) fn cycle_sign(
    edge_count: usize,
    edges: impl Iterator<Item = (XY, XY)> + Clone,
) -> AreaSign {
    if edge_count == 0 {
        return AreaSign::Zero;
    }
    if let Some(sign) = rounded_translated_cycle_sign(edge_count, edges.clone()) {
        return sign;
    }
    exact_cycle_sign(edges)
}

pub(super) fn exact_cycle_sign(edges: impl Iterator<Item = (XY, XY)>) -> AreaSign {
    let mut sum = ExactSum::zero();
    for (a, b) in edges {
        sum.add_product(a.x, b.y);
        sum.subtract_product(b.x, a.y);
    }
    area_sign_from_ordering(sum.ordering())
}

pub(super) const fn area_sign_from_ordering(ordering: std::cmp::Ordering) -> AreaSign {
    match ordering {
        std::cmp::Ordering::Less => AreaSign::Negative,
        std::cmp::Ordering::Equal => AreaSign::Zero,
        std::cmp::Ordering::Greater => AreaSign::Positive,
    }
}

/// Running filter over a cycle whose vertices are exposed by an edge reader.
pub(super) fn rounded_translated_cycle_sign(
    edge_count: usize,
    edges: impl Iterator<Item = (XY, XY)> + Clone,
) -> Option<AreaSign> {
    if edge_count > 1 << 26 {
        return None;
    }
    let (origin, _) = edges.clone().next()?;
    let mut filter = CycleSignFilter::new(origin, edge_count)?;
    for (_, destination) in edges {
        filter.push(destination);
    }
    filter.finish()
}

/// Column-specialized filter: the contiguous slices make each vertex pair
/// bounds-free in the loop, and the running product magnitude removes the
/// NaN-aware maxima from the prior per-edge error estimate.
pub(super) fn rounded_translated_cycle_sign_columns(
    xs: &[f64],
    ys: &[f64],
    edge_count: usize,
) -> Option<AreaSign> {
    if edge_count > 1 << 26 {
        return None;
    }
    let (&origin_x, &origin_y) = xs.first().zip(ys.first())?;
    let origin = XY::new(origin_x, origin_y);
    let mut filter = CycleSignFilter::new(origin, edge_count)?;
    for (&x, &y) in xs.iter().skip(1).zip(ys.iter().skip(1)).take(edge_count) {
        filter.push(XY::new(x, y));
    }
    filter.finish()
}

pub(super) struct CycleSignFilter {
    origin: XY,
    previous: XY,
    edge_count: usize,
    sum: f64,
    product_magnitude: f64,
}

impl CycleSignFilter {
    pub(super) fn new(origin: XY, edge_count: usize) -> Option<Self> {
        (edge_count <= 1 << 26 && origin.x.is_finite() && origin.y.is_finite()).then_some(Self {
            origin,
            previous: XY::new(0.0, 0.0),
            edge_count,
            sum: 0.0,
            product_magnitude: 0.0,
        })
    }

    pub(super) fn push(&mut self, destination: XY) {
        let next = XY::new(destination.x - self.origin.x, destination.y - self.origin.y);
        let left = self.previous.x * next.y;
        let right = next.x * self.previous.y;
        let difference = left - right;
        let sum = self.sum + difference;
        let product_magnitude = left.abs() + right.abs();
        let product_magnitude = self.product_magnitude + product_magnitude;

        self.previous = next;
        self.sum = sum;
        self.product_magnitude = product_magnitude;
    }

    pub(super) fn finish(self) -> Option<AreaSign> {
        if !(self.sum.is_finite() && self.product_magnitude.is_finite()) {
            return None;
        }
        // A finite binary64 subtraction is exact when its result is subnormal
        // (Sterbenz or the shared subnormal grid); normal residuals have the
        // usual relative bound. For n <= 2^26, the relative term covers
        // translated residuals, rounded products and differences, the
        // sequential sum, and rounding of the positive magnitude fold. The
        // absolute term covers underflow in products and accumulations.
        let count = self.edge_count as f64;
        let relative = (32.0 * count * EPSILON) * self.product_magnitude;
        let underflow = (8.0 * f64::MIN_POSITIVE * count) * count;
        let bound = (relative + underflow).next_up();
        if !bound.is_finite() {
            return None;
        }
        if self.sum > bound {
            Some(AreaSign::Positive)
        } else if self.sum < -bound {
            Some(AreaSign::Negative)
        } else {
            None
        }
    }
}

pub(super) fn exact_open_xy_sign(points: &[XY]) -> AreaSign {
    let count = points.len();
    if count < 2 {
        return AreaSign::Zero;
    }
    cycle_sign(
        count,
        points
            .iter()
            .copied()
            .zip(points.iter().copied().cycle().skip(1))
            .take(count),
    )
}

pub(super) fn exact_open_point_sign(points: &[Point]) -> AreaSign {
    let count = points.len();
    if count < 2 {
        return AreaSign::Zero;
    }
    cycle_sign(
        count,
        points
            .iter()
            .map(Point::xy)
            .zip(points.iter().map(Point::xy).cycle().skip(1))
            .take(count),
    )
}

/// The exact sign of a closed ring's area, with none of the magnitude work.
pub(crate) fn ring_area_sign<C: Coordinates + ?Sized>(points: &C) -> AreaSign {
    let pairs = points.coord_count().saturating_sub(1);
    if pairs == 0 {
        return AreaSign::Zero;
    }
    if let Some((xs, ys)) = points.xy_columns() {
        exact_ring_area_sign_columns(xs, ys, pairs)
    } else {
        let count = points.coord_count();
        let mut xs = Vec::with_capacity(count);
        let mut ys = Vec::with_capacity(count);
        for point in points.iter_coords() {
            xs.push(point.x);
            ys.push(point.y);
        }
        exact_ring_area_sign_columns(&xs, &ys, pairs)
    }
}

pub(crate) fn ring_winding<C: Coordinates + ?Sized>(points: &C) -> RingWinding {
    area_sign_to_winding(ring_area_sign(points))
}

pub(crate) fn closed_columns_winding(xs: &[f64], ys: &[f64], pairs: usize) -> RingWinding {
    area_sign_to_winding(exact_ring_area_sign_columns(xs, ys, pairs))
}

pub(crate) fn open_xy_cycle_winding(points: &[XY]) -> RingWinding {
    area_sign_to_winding(exact_open_xy_sign(points))
}

#[cfg(test)]
pub(crate) fn open_xy_cycle_sign(points: &[XY]) -> AreaSign {
    exact_open_xy_sign(points)
}

pub(crate) fn open_point_cycle_winding(points: &[Point]) -> RingWinding {
    area_sign_to_winding(exact_open_point_sign(points))
}
