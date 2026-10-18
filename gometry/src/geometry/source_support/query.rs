use std::cell::OnceCell;
use std::cmp::Ordering;

use super::{
    Bounds, ExactDyadic, ExactSign, Point, Result, XY, compare_ratios, outward_ratio_bounds,
    same_topological_coordinate, stored_orientation_sign,
};

#[derive(Clone, Copy)]
pub(in crate::geometry) enum QueryAxis {
    X,
    Y,
}

#[derive(Clone, Copy)]
pub(in crate::geometry) struct AxisRelation {
    pub(super) query_below: bool,
    pub(super) equal: bool,
}

pub(in crate::geometry) trait PointQuery: Copy {
    fn compare_axis(self, axis: QueryAxis, value: f64) -> Result<Ordering>;
    fn orientation(self, start: XY, end: XY) -> Result<ExactSign>;
    /// Lower-sided candidate key, not a rounded replacement for exact predicates.
    fn index_coordinate(self, axis: QueryAxis) -> f64;

    fn index_xy(self) -> XY {
        XY::new(
            self.index_coordinate(QueryAxis::X),
            self.index_coordinate(QueryAxis::Y),
        )
    }

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
pub(in crate::geometry) struct StoredQuery(pub(super) XY);

impl PointQuery for StoredQuery {
    fn misses_height(self, start: f64, end: f64) -> bool {
        (start < self.0.y && end < self.0.y) || (start > self.0.y && end > self.0.y)
    }

    #[expect(
        clippy::float_cmp,
        reason = "exact source-coordinate equality distinguishes contact from an open ray crossing"
    )]
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

    fn index_coordinate(self, axis: QueryAxis) -> f64 {
        match axis {
            QueryAxis::X => self.0.x,
            QueryAxis::Y => self.0.y,
        }
    }
}

#[derive(Clone, Copy)]
pub(in crate::geometry) struct ExactQuery<'a> {
    point: &'a [ExactDyadic; 3],
    x_bounds: (f64, f64),
    y_bounds: (f64, f64),
}

pub(in crate::geometry) struct ExactProbe<'a> {
    point: &'a [ExactDyadic; 3],
    stored: Option<Point>,
    source: OnceCell<ExactQuery<'a>>,
}

impl<'a> ExactProbe<'a> {
    pub(in crate::geometry) fn new(point: &'a [ExactDyadic; 3]) -> Self {
        Self {
            point,
            stored: exact_stored_point(point),
            source: OnceCell::new(),
        }
    }

    pub(in crate::geometry) const fn homogeneous(&self) -> &'a [ExactDyadic; 3] {
        self.point
    }

    pub(in crate::geometry) const fn stored(&self) -> Option<Point> {
        self.stored
    }

    pub(in crate::geometry) fn source_query(&self) -> Result<ExactQuery<'a>> {
        Ok(*self
            .source
            .get_or_try_init(|| ExactQuery::new(self.point))?)
    }
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

    pub(in crate::geometry) const fn offset(
        self,
        direction: &'a [ExactDyadic; 2],
    ) -> InfinitesimalQuery<'a> {
        InfinitesimalQuery {
            point: self,
            direction,
        }
    }
}

impl PointQuery for ExactQuery<'_> {
    #[expect(
        clippy::float_cmp,
        reason = "exact source-coordinate equality distinguishes contact from an open ray crossing"
    )]
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

    fn index_coordinate(self, axis: QueryAxis) -> f64 {
        match axis {
            QueryAxis::X => self.x_bounds.0,
            QueryAxis::Y => self.y_bounds.0,
        }
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

    #[expect(
        clippy::float_cmp,
        reason = "equal bracket endpoints prove exact representability"
    )]
    fn index_coordinate(self, axis: QueryAxis) -> f64 {
        let (bounds, direction) = match axis {
            QueryAxis::X => (self.point.x_bounds, &self.direction[0]),
            QueryAxis::Y => (self.point.y_bounds, &self.direction[1]),
        };
        if bounds.0 == bounds.1 && direction.sign() == ExactSign::Negative {
            bounds.0.next_down()
        } else {
            bounds.0
        }
    }

    fn misses_height(self, start: f64, end: f64) -> bool {
        self.point.misses_height(start, end)
    }
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

pub(in crate::geometry) fn point_in_bounds(query: impl PointQuery, bounds: Bounds) -> Result<bool> {
    Ok(
        query.compare_axis(QueryAxis::X, bounds.minx())? != Ordering::Less
            && query.compare_axis(QueryAxis::X, bounds.maxx())? != Ordering::Greater
            && query.compare_axis(QueryAxis::Y, bounds.miny())? != Ordering::Less
            && query.compare_axis(QueryAxis::Y, bounds.maxy())? != Ordering::Greater,
    )
}
