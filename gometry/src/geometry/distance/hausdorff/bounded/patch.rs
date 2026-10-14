//! Source-patch representation and subdivision.

#[cfg(any(test, debug_assertions))]
use super::cycle::point_in_box;
use super::cycle::{BoxSplitAxis, BoxSplitStrategy, compare_box_extents};
use super::support::span_point;
#[cfg(any(test, debug_assertions))]
use super::support::span_projection_parameter;
use super::{
    ExactPoint, ExpansionBudget, FloatBox, Ordering, PreparedSupport, Rational, Result,
    SourcePatch, SupportSpan, XY,
};

impl SourcePatch {
    #[expect(
        clippy::indexing_slicing,
        reason = "patch span and point ordinals are created from this immutable PreparedSupport"
    )]
    pub(super) fn witness(
        &self,
        source: &PreparedSupport,
        center_first: bool,
    ) -> Result<Option<ExactPoint>> {
        match self {
            Self::Span { span, lo, hi } => {
                let midpoint = lo.try_midpoint(hi)?;
                Ok(Some(span_point(&source.spans[*span], &midpoint)?))
            },
            Self::Box(bounds) => source.source_box_witness(*bounds, center_first),
            Self::Point(point) => Ok(Some(source.points[*point].clone())),
        }
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "patch span and point ordinals are created from this immutable PreparedSupport"
    )]
    pub(super) fn center_and_radius_squared(
        &self,
        source: &PreparedSupport,
    ) -> Result<(ExactPoint, Rational)> {
        match self {
            Self::Span { span, lo, hi } => {
                let midpoint = lo.try_midpoint(hi)?;
                let center = span_point(&source.spans[*span], &midpoint)?;
                let radius = span_radius_squared(&source.spans[*span], lo, hi)?;
                Ok((center, radius))
            },
            Self::Box(bounds) => {
                let min_x = Rational::from_f64(bounds.min_x);
                let max_x = Rational::from_f64(bounds.max_x);
                let min_y = Rational::from_f64(bounds.min_y);
                let max_y = Rational::from_f64(bounds.max_y);
                let center =
                    ExactPoint::new(min_x.try_midpoint(&max_x)?, min_y.try_midpoint(&max_y)?);
                let half_x = max_x
                    .try_subtract(&min_x)?
                    .try_quotient(&Rational::from_f64(2.0))?;
                let half_y = max_y
                    .try_subtract(&min_y)?
                    .try_quotient(&Rational::from_f64(2.0))?;
                let radius = half_x.try_square()?.try_add(&half_y.try_square()?)?;
                Ok((center, radius))
            },
            Self::Point(point) => Ok((source.points[*point].clone(), Rational::zero())),
        }
    }

    #[cfg(any(test, debug_assertions))]
    #[expect(
        clippy::indexing_slicing,
        reason = "patch span and point ordinals are created from this immutable PreparedSupport"
    )]
    pub(super) fn witness_is_valid(
        &self,
        source: &PreparedSupport,
        witness: &ExactPoint,
    ) -> Result<bool> {
        match self {
            Self::Span { span, lo, hi } => {
                let source_span = &source.spans[*span];
                let parameter = span_projection_parameter(witness, source_span)?;
                if parameter.try_cmp(lo)? == Ordering::Less
                    || parameter.try_cmp(hi)? == Ordering::Greater
                {
                    return Ok(false);
                }
                let reconstructed = span_point(source_span, &parameter)?;
                Ok(reconstructed.same_position(witness)?)
            },
            Self::Box(bounds) => {
                Ok(point_in_box(witness, *bounds)? && source.contains_area(witness)?)
            },
            Self::Point(point) => source.points[*point].same_position(witness),
        }
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "patch span and point ordinals are created from this immutable PreparedSupport"
    )]
    pub(super) fn corners(&self, source: &PreparedSupport) -> Result<Vec<ExactPoint>> {
        Ok(match self {
            Self::Span { span, lo, hi } => {
                vec![
                    span_point(&source.spans[*span], lo)?,
                    span_point(&source.spans[*span], hi)?,
                ]
            },
            Self::Box(bounds) => {
                let mut corners = Vec::with_capacity(4);
                corners.extend(
                    bounds
                        .corners()
                        .map(|(x, y)| ExactPoint::from_xy(XY::new(x, y))),
                );
                corners
            },
            Self::Point(point) => vec![source.points[*point].clone()],
        })
    }

    pub(super) fn split(self, budget: &mut ExpansionBudget) -> Result<Option<Vec<Self>>> {
        self.split_with_strategy(budget, BoxSplitStrategy::FourWay)
    }

    pub(super) fn split_with_strategy(
        self,
        budget: &mut ExpansionBudget,
        strategy: BoxSplitStrategy,
    ) -> Result<Option<Vec<Self>>> {
        match self {
            Self::Span { span, lo, hi } => {
                budget.add(2)?;
                let midpoint = lo.try_midpoint(&hi)?;
                Ok(Some(vec![
                    Self::Span {
                        span,
                        lo,
                        hi: midpoint.clone(),
                    },
                    Self::Span {
                        span,
                        lo: midpoint,
                        hi,
                    },
                ]))
            },
            Self::Box(bounds) => match strategy {
                BoxSplitStrategy::FourWay => Self::split_box_four_way(bounds, budget),
                BoxSplitStrategy::LongestAxis => Self::split_box_longest_axis(bounds, budget),
            },
            Self::Point(_) => Ok(None),
        }
    }

    pub(super) fn split_box_four_way(
        bounds: FloatBox,
        budget: &mut ExpansionBudget,
    ) -> Result<Option<Vec<Self>>> {
        let mid_x = f64::midpoint(bounds.min_x, bounds.max_x);
        let mid_y = f64::midpoint(bounds.min_y, bounds.max_y);
        let split_x = bounds.min_x < mid_x && mid_x < bounds.max_x;
        let split_y = bounds.min_y < mid_y && mid_y < bounds.max_y;
        if !split_x && !split_y {
            return Ok(None);
        }
        let count = if split_x && split_y { 4 } else { 2 };
        budget.add(count)?;
        let mut children = Vec::with_capacity(count);
        let xs = if split_x {
            [(bounds.min_x, mid_x), (mid_x, bounds.max_x)]
        } else {
            [(bounds.min_x, bounds.max_x); 2]
        };
        let ys = if split_y {
            [(bounds.min_y, mid_y), (mid_y, bounds.max_y)]
        } else {
            [(bounds.min_y, bounds.max_y); 2]
        };
        for (min_x, max_x) in xs.into_iter().take(if split_x { 2 } else { 1 }) {
            for (min_y, max_y) in ys.into_iter().take(if split_y { 2 } else { 1 }) {
                children.push(Self::Box(FloatBox {
                    min_x,
                    max_x,
                    min_y,
                    max_y,
                }));
            }
        }
        Ok(Some(children))
    }

    pub(super) fn split_box_longest_axis(
        bounds: FloatBox,
        budget: &mut ExpansionBudget,
    ) -> Result<Option<Vec<Self>>> {
        let mid_x = f64::midpoint(bounds.min_x, bounds.max_x);
        let mid_y = f64::midpoint(bounds.min_y, bounds.max_y);
        let split_x = bounds.min_x < mid_x && mid_x < bounds.max_x;
        let split_y = bounds.min_y < mid_y && mid_y < bounds.max_y;
        if !split_x && !split_y {
            return Ok(None);
        }
        let preferred = match compare_box_extents(bounds) {
            Some(Ordering::Less) => BoxSplitAxis::Y,
            Some(Ordering::Equal | Ordering::Greater) => BoxSplitAxis::X,
            None => return Self::split_box_four_way(bounds, budget),
        };
        // At least one axis splits, so the preferred axis yields only when it cannot.
        let axis = match preferred {
            BoxSplitAxis::X if split_x => BoxSplitAxis::X,
            BoxSplitAxis::Y if split_y => BoxSplitAxis::Y,
            BoxSplitAxis::X => BoxSplitAxis::Y,
            BoxSplitAxis::Y => BoxSplitAxis::X,
        };
        budget.add(2)?;
        let (first, second) = match axis {
            BoxSplitAxis::X => (
                FloatBox {
                    max_x: mid_x,
                    ..bounds
                },
                FloatBox {
                    min_x: mid_x,
                    ..bounds
                },
            ),
            BoxSplitAxis::Y => (
                FloatBox {
                    max_y: mid_y,
                    ..bounds
                },
                FloatBox {
                    min_y: mid_y,
                    ..bounds
                },
            ),
        };
        Ok(Some(vec![Self::Box(first), Self::Box(second)]))
    }
}

pub(super) fn span_radius_squared(
    span: &SupportSpan,
    lo: &Rational,
    hi: &Rational,
) -> Result<Rational> {
    let delta = hi.try_subtract(lo)?;
    let half = delta.try_quotient(&Rational::from_f64(2.0))?;
    span.carrier()?
        .norm_squared
        .try_product(&half.try_square()?)
}
