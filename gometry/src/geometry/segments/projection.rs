//! Clamped segment projection and its source-distance certificates.

#[cfg(test)]
use super::distance::point_segment_selection;
use super::distance::{
    exact_witness_cmp_with_identity, point_distance, squared_norm_is_trustworthy,
    try_point_distance_squared,
};
use super::interpolation::interpolate_segment_point;
use super::{
    CertifiedSegmentProjection, ClassifiedSegmentProjection, DdInterval,
    ExactClassifiedSegmentProjection, ExactDistanceWitness, ExactProjectionTerms,
    ExactSegmentProjection, ExactSquaredDistance, FilteredInteriorProjection, PointSegmentMetric,
    PointSegmentSelection, PointSegmentWitnessKind, SegmentProjection, SelectionProjectionSource,
    SourceInteriorProjection, SourcePair, SquaredDistanceFilter, SquaredDistanceKey, WitnessRole,
    certified_projection,
};
use crate::error::{ErrorKind, Result};
use crate::geometry::numerics::product::ExactSign;
use crate::geometry::tessellation::exact::{
    ExactDyadic, ExactRational, compare_ratios, round_ratio, round_sqrt_ratio,
};
use crate::geometry::{MOrdinate, Ordering, Point, Segment, XY, ZOrdinate, same_point};

pub(super) fn projection_difference(left: f64, right: f64) -> Result<ExactDyadic> {
    ExactDyadic::from_f64(left).try_subtract(ExactDyadic::from_f64(right))
}

impl SourcePair {
    pub(super) fn new(left: f64, right: f64, subtract: bool) -> Option<Self> {
        if !(left.is_normal() || left == 0.0) || !(right.is_normal() || right == 0.0) {
            return None;
        }
        let value = if subtract { left - right } else { left + right };
        let magnitude = left.abs() + right.abs();
        let radius = (magnitude * (8.0 * (f64::EPSILON / 2.0))).next_up();
        (value.is_finite() && radius.is_finite()).then_some(Self {
            value,
            radius,
            left,
            right,
            subtract,
        })
    }

    pub(super) fn lower(self) -> f64 {
        (self.value - self.radius).next_down()
    }

    pub(super) fn upper(self) -> f64 {
        (self.value + self.radius).next_up()
    }

    pub(super) const fn terms_do_not_cancel(self) -> bool {
        self.left == 0.0
            || self.right == 0.0
            || if self.subtract {
                self.left.is_sign_positive() != self.right.is_sign_positive()
            } else {
                self.left.is_sign_positive() == self.right.is_sign_positive()
            }
    }
}

pub(super) fn source_difference(left: f64, right: f64) -> Option<f64> {
    let difference = left - right;
    (difference.is_normal()
        || left.to_bits() == right.to_bits()
        || (left.to_bits() << 1 == 0 && right.to_bits() << 1 == 0))
        .then_some(difference)
}

pub(super) fn source_product(left: f64, right: f64) -> Option<f64> {
    let product = left * right;
    (product.is_normal() || (product == 0.0 && (left == 0.0 || right == 0.0))).then_some(product)
}

impl CertifiedSegmentProjection {
    fn exact(self) -> Result<ExactSegmentProjection> {
        let mut exact = exact_interior_projection(self.point, self.segment)?;
        exact.emitted_foot = Some(self.foot);
        Ok(exact)
    }
}

impl ExactSegmentProjection {
    fn interpolate(&self, start: f64, end: f64) -> Result<f64> {
        let numerator = ExactDyadic::from_f64(start)
            .try_product(&self.denominator)?
            .try_add(projection_difference(end, start)?.try_product(&self.numerator)?)?;
        round_ratio(&numerator, &self.denominator)
    }

    fn scale(&self, value: f64) -> Result<f64> {
        let numerator = ExactDyadic::from_f64(value).try_product(&self.numerator)?;
        round_ratio(&numerator, &self.denominator)
    }

    fn squared_distance(&self, point: XY, segment: Segment) -> Result<ExactSquaredDistance> {
        ExactSquaredDistance::from_projection(point, segment, self)
    }

    fn distance(&self, point: XY, segment: Segment) -> Result<f64> {
        let squared = self.squared_distance(point, segment)?;
        round_sqrt_ratio(&squared.numerator, &squared.denominator)
    }

    #[cfg(test)]
    fn emitted_xy(&self, segment: Segment) -> Result<XY> {
        Ok(match self.emitted_foot {
            Some(foot) => foot,
            None => XY::new(
                self.interpolate(segment.start.x, segment.end.x)?,
                self.interpolate(segment.start.y, segment.end.y)?,
            ),
        })
    }
}

impl WitnessRole {
    pub(crate) fn cmp_tie(
        self,
        incumbent: Self,
        source_order: impl FnOnce() -> Ordering,
    ) -> Ordering {
        self.cmp(&incumbent).then_with(source_order)
    }
}

impl SegmentProjection {
    pub(in crate::geometry) const fn is_start(&self) -> bool {
        matches!(self, Self::Start)
    }

    pub(in crate::geometry) const fn is_end(&self) -> bool {
        matches!(self, Self::End)
    }

    #[cfg(test)]
    pub(in crate::geometry) const fn uses_exact_ratio(&self) -> bool {
        matches!(self, Self::CertifiedInterior(_) | Self::ExactInterior(_))
    }

    pub(crate) fn fraction(&self) -> f64 {
        match self {
            Self::Start => 0.0,
            Self::Interior(projection) => projection.fraction,
            Self::CertifiedInterior(projection) => projection.fraction,
            Self::ExactInterior(projection) => projection.fraction,
            Self::End => 1.0,
        }
    }

    pub(in crate::geometry) fn exact_fraction(&self) -> Result<ExactRational> {
        match self {
            Self::Start => Ok(ExactRational::zero()),
            Self::End => Ok(ExactRational::one()),
            Self::Interior(_) | Self::CertifiedInterior(_) | Self::ExactInterior(_) => {
                let (numerator, denominator) = self.exact_ratio()?;
                ExactRational::new(numerator, denominator)
            },
        }
    }

    pub(super) fn continuum_distance(&self, point: XY, segment: Segment) -> Result<f64> {
        match self {
            Self::Start => Ok(point_distance(point, segment.start)),
            Self::Interior(projection) => {
                source_projection_distance(projection.point, projection.segment)
            },
            Self::CertifiedInterior(projection) => projection.exact()?.distance(point, segment),
            Self::ExactInterior(projection) => projection.distance(point, segment),
            Self::End => Ok(point_distance(point, segment.end)),
        }
    }

    pub(super) fn continuum_squared(&self, point: XY, segment: Segment) -> Result<f64> {
        match self {
            Self::Start => Ok(point_distance(point, segment.start).powi(2)),
            Self::Interior(projection) => {
                source_projection_squared(projection.point, projection.segment)
            },
            Self::CertifiedInterior(projection) => projection
                .exact()?
                .squared_distance(point, segment)?
                .rounded(),
            Self::ExactInterior(projection) => {
                projection.squared_distance(point, segment)?.rounded()
            },
            Self::End => Ok(point_distance(point, segment.end).powi(2)),
        }
    }

    pub(in crate::geometry) fn cmp_along(&self, other: &Self) -> Result<Ordering> {
        match (self, other) {
            (Self::Start, Self::Start) | (Self::End, Self::End) => Ok(Ordering::Equal),
            (Self::Start, _) | (_, Self::End) => Ok(Ordering::Less),
            (Self::End, _) | (_, Self::Start) => Ok(Ordering::Greater),
            _ => {
                let (left_numerator, left_denominator) = self.exact_ratio()?;
                let (right_numerator, right_denominator) = other.exact_ratio()?;
                compare_ratios(
                    &left_numerator,
                    &left_denominator,
                    &right_numerator,
                    &right_denominator,
                )
            },
        }
    }

    #[cfg(test)]
    pub(in crate::geometry) fn interpolate_xy(&self, segment: Segment) -> Result<XY> {
        Ok(match self {
            Self::Start => segment.start,
            Self::End => segment.end,
            Self::Interior(projection) => {
                exact_interior_projection(projection.point, projection.segment)?
                    .emitted_xy(segment)?
            },
            Self::CertifiedInterior(projection) => projection.foot,
            Self::ExactInterior(projection) => projection.emitted_xy(segment)?,
        })
    }

    fn exact_ratio(&self) -> Result<(ExactDyadic, ExactDyadic)> {
        let projection = match self {
            Self::Interior(projection) => {
                return exact_interior_projection(projection.point, projection.segment)
                    .map(|projection| (projection.numerator, projection.denominator));
            },
            Self::CertifiedInterior(projection) => projection.exact()?,
            Self::ExactInterior(projection) => {
                return Ok((projection.numerator.clone(), projection.denominator.clone()));
            },
            Self::Start | Self::End => unreachable!("endpoint ratio has no interior recipe"),
        };
        Ok((projection.numerator, projection.denominator))
    }

    pub(in crate::geometry) fn interpolate_point(&self, start: Point, end: Point) -> Result<Point> {
        let exact;
        let projection = match self {
            Self::Start => return Ok(start),
            Self::End => return Ok(end),
            Self::Interior(projection) => {
                exact = exact_interior_projection(projection.point, projection.segment)?;
                &exact
            },
            Self::ExactInterior(projection) => projection.as_ref(),
            Self::CertifiedInterior(certified) => {
                exact = certified.exact()?;
                &exact
            },
        };
        let interpolate_optional = |start: Option<f64>, end: Option<f64>| -> Result<Option<f64>> {
            match (start, end) {
                (Some(start), Some(end)) => Ok(Some(projection.interpolate(start, end)?)),
                _ => Ok(None),
            }
        };
        Ok(Point::new_axes(
            projection.interpolate(start.x, end.x)?,
            projection.interpolate(start.y, end.y)?,
            ZOrdinate(interpolate_optional(start.z(), end.z())?),
            MOrdinate(interpolate_optional(start.m(), end.m())?),
        )
        .unwrap_or(if projection.fraction <= 0.5 {
            start
        } else {
            end
        }))
    }

    pub(in crate::geometry) fn scale(&self, value: f64) -> Result<f64> {
        Ok(match self {
            Self::Start => 0.0,
            Self::Interior(projection) => {
                exact_interior_projection(projection.point, projection.segment)?.scale(value)?
            },
            Self::CertifiedInterior(projection) => projection.exact()?.scale(value)?,
            Self::ExactInterior(projection) => projection.scale(value)?,
            Self::End => value,
        })
    }
}

/// Certified clamped point-to-segment projection over the complete finite
/// binary64 domain. The outward interval is an allocation-free proof for the
/// common case. Any unresolved overlap, underflow, overflow, or cancellation
/// falls through to the exact stored-dyadic numerator/denominator owner.
pub(crate) fn segment_projection(
    point: impl Into<XY>,
    segment: Segment,
) -> Result<SegmentProjection> {
    let point = point.into();
    projection_from_classification(point, segment, classify_segment_projection(point, segment))
}

// The compact decide/decline filter handles ordinary projections; unresolved
// cases continue through the exact fallback.
fn classify_segment_projection(point: XY, segment: Segment) -> ClassifiedSegmentProjection {
    let Some(dx) = source_difference(segment.end.x, segment.start.x) else {
        return ClassifiedSegmentProjection::Declined;
    };
    let Some(dy) = source_difference(segment.end.y, segment.start.y) else {
        return ClassifiedSegmentProjection::Declined;
    };
    if dx == 0.0 && dy == 0.0 {
        return ClassifiedSegmentProjection::Start;
    }
    let Some(qx) = source_difference(point.x, segment.start.x) else {
        return ClassifiedSegmentProjection::Declined;
    };
    let Some(qy) = source_difference(point.y, segment.start.y) else {
        return ClassifiedSegmentProjection::Declined;
    };
    let Some(numerator_left) = source_product(qx, dx) else {
        return ClassifiedSegmentProjection::Declined;
    };
    let Some(numerator_right) = source_product(qy, dy) else {
        return ClassifiedSegmentProjection::Declined;
    };
    let Some(denominator_left) = source_product(dx, dx) else {
        return ClassifiedSegmentProjection::Declined;
    };
    let Some(denominator_right) = source_product(dy, dy) else {
        return ClassifiedSegmentProjection::Declined;
    };
    let Some(numerator) = SourcePair::new(numerator_left, numerator_right, false) else {
        return ClassifiedSegmentProjection::Declined;
    };
    let Some(denominator) = SourcePair::new(denominator_left, denominator_right, false) else {
        return ClassifiedSegmentProjection::Declined;
    };

    if numerator.upper() < 0.0 {
        return ClassifiedSegmentProjection::Start;
    }
    if numerator.lower() > denominator.upper() {
        return ClassifiedSegmentProjection::End;
    }
    if numerator.lower() > 0.0 && numerator.upper() < denominator.lower() {
        return classify_filtered_interior(segment, numerator, denominator);
    }
    ClassifiedSegmentProjection::Declined
}

/// Ratio and witness certification is reached only after both endpoint signs
/// have been decided as strictly interior.
fn classify_filtered_interior(
    segment: Segment,
    numerator: SourcePair,
    denominator: SourcePair,
) -> ClassifiedSegmentProjection {
    let fraction = numerator.value / denominator.value;
    if fraction.is_finite() && fraction > 0.0 && fraction < 1.0 && fraction.is_normal() {
        let foot = interpolate_segment_point(segment.start, segment.end, fraction);
        if !same_point(foot, segment.start) && !same_point(foot, segment.end) {
            return ClassifiedSegmentProjection::FilteredInterior(FilteredInteriorProjection {
                fraction,
                terms_do_not_cancel: numerator.terms_do_not_cancel(),
            });
        }
    }
    ClassifiedSegmentProjection::Declined
}

/// The interval-filter decline uses exact stored-dyadic arithmetic.
pub(super) fn classify_segment_projection_exact(
    point: XY,
    segment: Segment,
) -> Result<ExactClassifiedSegmentProjection> {
    let terms = exact_projection_terms(point, segment)?;
    if terms.denominator.is_zero() {
        return Ok(ExactClassifiedSegmentProjection::Start);
    }
    if terms.numerator.sign() != ExactSign::Positive {
        return Ok(ExactClassifiedSegmentProjection::Start);
    }
    if terms.numerator.cmp(&terms.denominator) != Ordering::Less {
        return Ok(ExactClassifiedSegmentProjection::End);
    }
    Ok(ExactClassifiedSegmentProjection::Interior(terms))
}

fn projection_from_classification(
    point: XY,
    segment: Segment,
    classification: ClassifiedSegmentProjection,
) -> Result<SegmentProjection> {
    let (terms, historical_fraction) = match classification {
        ClassifiedSegmentProjection::Start => return Ok(SegmentProjection::Start),
        ClassifiedSegmentProjection::End => return Ok(SegmentProjection::End),
        ClassifiedSegmentProjection::FilteredInterior(filtered) if filtered.terms_do_not_cancel => {
            return Ok(SegmentProjection::Interior(SourceInteriorProjection {
                point,
                segment,
                fraction: filtered.fraction,
            }));
        },
        ClassifiedSegmentProjection::FilteredInterior(filtered) => {
            if let Some((fraction, foot)) = certified_projection(point, segment, filtered.fraction)
            {
                return Ok(SegmentProjection::CertifiedInterior(
                    CertifiedSegmentProjection {
                        point,
                        segment,
                        fraction,
                        foot,
                    },
                ));
            }
            (
                exact_projection_terms(point, segment)?,
                Some(filtered.fraction),
            )
        },
        ClassifiedSegmentProjection::Declined => {
            match classify_segment_projection_exact(point, segment)? {
                ExactClassifiedSegmentProjection::Start => return Ok(SegmentProjection::Start),
                ExactClassifiedSegmentProjection::End => return Ok(SegmentProjection::End),
                ExactClassifiedSegmentProjection::Interior(terms) => (terms, None),
            }
        },
    };
    let fraction = round_ratio(&terms.numerator, &terms.denominator)?;
    let emitted_foot = historical_fraction
        .filter(|historical| historical.to_bits() == fraction.to_bits())
        .map(|historical| interpolate_segment_point(segment.start, segment.end, historical));
    Ok(SegmentProjection::ExactInterior(Box::new(
        ExactSegmentProjection {
            numerator: terms.numerator,
            denominator: terms.denominator,
            fraction,
            emitted_foot,
        },
    )))
}

fn exact_interior_projection(point: XY, segment: Segment) -> Result<ExactSegmentProjection> {
    let terms = exact_projection_terms(point, segment)?;
    debug_assert!(!terms.denominator.is_zero());
    debug_assert_eq!(terms.numerator.sign(), ExactSign::Positive);
    debug_assert!(terms.numerator.cmp(&terms.denominator).is_lt());
    let fraction = round_ratio(&terms.numerator, &terms.denominator)?;
    Ok(ExactSegmentProjection {
        numerator: terms.numerator,
        denominator: terms.denominator,
        fraction,
        emitted_foot: None,
    })
}

pub(super) fn exact_projection_terms(point: XY, segment: Segment) -> Result<ExactProjectionTerms> {
    let dx = projection_difference(segment.end.x, segment.start.x)?;
    let dy = projection_difference(segment.end.y, segment.start.y)?;
    let qx = projection_difference(point.x, segment.start.x)?;
    let qy = projection_difference(point.y, segment.start.y)?;
    let numerator = qx.try_product(&dx)?.try_add(qy.try_product(&dy)?)?;
    let denominator = dx.try_product(&dx)?.try_add(dy.try_product(&dy)?)?;
    let determinant = dx.try_product(&qy)?.try_subtract(dy.try_product(&qx)?)?;
    Ok(ExactProjectionTerms {
        numerator,
        denominator,
        determinant,
    })
}

impl ExactSquaredDistance {
    fn from_operands(determinant: &ExactDyadic, denominator: &ExactDyadic) -> Result<Self> {
        Ok(Self {
            numerator: determinant.try_product(determinant)?,
            denominator: denominator.clone(),
        })
    }

    pub(super) fn interior(terms: &ExactProjectionTerms) -> Result<Self> {
        Self::from_operands(&terms.determinant, &terms.denominator)
    }

    fn from_projection(
        point: XY,
        segment: Segment,
        _projection: &ExactSegmentProjection,
    ) -> Result<Self> {
        let terms = exact_projection_terms(point, segment)?;
        Self::from_operands(&terms.determinant, &terms.denominator)
    }

    pub(super) fn rounded(&self) -> Result<f64> {
        match round_ratio(&self.numerator, &self.denominator) {
            Ok(rounded) => Ok(rounded),
            Err(error) if matches!(error.kind(), ErrorKind::Resource(_)) => Err(error),
            Err(_) => Ok(f64::INFINITY),
        }
    }

    pub(super) fn point_pair(left: XY, right: XY) -> Result<Self> {
        let dx = projection_difference(left.x, right.x)?;
        let dy = projection_difference(left.y, right.y)?;
        Ok(Self {
            numerator: dx.try_product(&dx)?.try_add(dy.try_product(&dy)?)?,
            denominator: ExactDyadic::from_f64(1.0),
        })
    }

    pub(super) fn cmp(&self, other: &Self) -> Result<Ordering> {
        compare_ratios(
            &self.numerator,
            &self.denominator,
            &other.numerator,
            &other.denominator,
        )
    }

    pub(super) fn rounded_trustworthy(&self) -> Result<Option<f64>> {
        match round_ratio(&self.numerator, &self.denominator) {
            Ok(rounded) => Ok(if self.numerator.is_zero() {
                Some(0.0)
            } else {
                rounded.is_normal().then_some(rounded)
            }),
            Err(error) if matches!(error.kind(), ErrorKind::Resource(_)) => Err(error),
            Err(_) => Ok(None),
        }
    }
}

fn source_dd_terms(point: XY, segment: Segment) -> Option<(DdInterval, DdInterval, DdInterval)> {
    let dx = DdInterval::difference(segment.end.x, segment.start.x)?;
    let dy = DdInterval::difference(segment.end.y, segment.start.y)?;
    let qx = DdInterval::difference(point.x, segment.start.x)?;
    let qy = DdInterval::difference(point.y, segment.start.y)?;
    let denominator = dx.square()?.add(dy.square()?)?;
    let numerator = qx.mul(dx)?.add(qy.mul(dy)?)?;
    let determinant = dx.mul(qy)?.sub(dy.mul(qx)?)?;
    Some((numerator, denominator, determinant))
}

pub(super) fn exact_source_squared(point: XY, segment: Segment) -> Result<ExactSquaredDistance> {
    ExactSquaredDistance::interior(&exact_projection_terms(point, segment)?)
}

fn source_fast_interior_measurement(
    point: XY,
    segment: Segment,
) -> Option<(f64, SquaredDistanceFilter)> {
    let dx = source_difference(segment.end.x, segment.start.x)?;
    let dy = source_difference(segment.end.y, segment.start.y)?;
    let qx = source_difference(point.x, segment.start.x)?;
    let qy = source_difference(point.y, segment.start.y)?;
    let determinant_left = source_product(dx, qy)?;
    let determinant_right = source_product(dy, qx)?;
    let denominator_left = source_product(dx, dx)?;
    let denominator_right = source_product(dy, dy)?;
    let determinant = SourcePair::new(determinant_left, determinant_right, true)?;
    let denominator = SourcePair::new(denominator_left, denominator_right, false)?;
    // Equal-sign determinant products cancel. Let the DD tier retain their
    // original subtraction tails; opposite signs and exact zero terms are
    // the cheap source-aware path.
    if !(determinant.terms_do_not_cancel() || determinant.left == 0.0 && determinant.right == 0.0) {
        return None;
    }
    if determinant.value == 0.0 && !(determinant.left == 0.0 && determinant.right == 0.0) {
        return None;
    }
    if denominator.lower() <= 0.0 {
        return None;
    }
    let distance = determinant.value.abs() / denominator.value.sqrt();
    let squared = distance * distance;
    squared_norm_is_trustworthy(squared, determinant.value == 0.0).then_some((
        squared,
        SquaredDistanceFilter::source_squared(squared, 16.0 * (f64::EPSILON / 2.0)),
    ))
}

fn source_interior_measurement(
    point: XY,
    segment: Segment,
) -> Option<(f64, SquaredDistanceFilter)> {
    if let Some(measurement) = source_fast_interior_measurement(point, segment) {
        return Some(measurement);
    }
    let (_numerator, denominator, determinant) = source_dd_terms(point, segment)?;
    if denominator.is_exact_zero() {
        return None;
    }
    if determinant.is_exact_zero() {
        return Some((
            0.0,
            SquaredDistanceFilter::source_squared(0.0, 16.0 * (f64::EPSILON / 2.0)),
        ));
    }
    let determinant = determinant.rounded_common()?;
    let denominator = denominator.rounded_common()?;
    if determinant == 0.0 || !denominator.is_sign_positive() {
        return None;
    }
    let distance = determinant.abs() / denominator.sqrt();
    let squared = distance * distance;
    if !squared_norm_is_trustworthy(squared, false) {
        return None;
    }
    Some((
        squared,
        SquaredDistanceFilter::source_squared(squared, 8.0 * (f64::EPSILON / 2.0)),
    ))
}

fn source_projection_distance(point: XY, segment: Segment) -> Result<f64> {
    if let Some((squared, _)) = source_interior_measurement(point, segment) {
        return Ok(squared.sqrt());
    }
    let squared = exact_source_squared(point, segment)?;
    round_sqrt_ratio(&squared.numerator, &squared.denominator)
}

pub(super) fn source_projection_squared(point: XY, segment: Segment) -> Result<f64> {
    if let Some((squared, _)) = source_interior_measurement(point, segment) {
        return Ok(squared);
    }
    exact_source_squared(point, segment)?.rounded()
}

impl PointSegmentSelection {
    pub(crate) const fn distance(&self) -> f64 {
        self.distance
    }

    pub(crate) fn projection(&self, point: XY, segment: Segment) -> Result<SegmentProjection> {
        match &self.projection {
            SelectionProjectionSource::Classified(classification) => {
                projection_from_classification(point, segment, *classification)
            },
            SelectionProjectionSource::Ready(projection) => match projection {
                SegmentProjection::ExactInterior(inner) => Ok(SegmentProjection::ExactInterior(
                    Box::new(ExactSegmentProjection {
                        numerator: inner.numerator.clone(),
                        denominator: inner.denominator.clone(),
                        fraction: inner.fraction,
                        emitted_foot: inner.emitted_foot,
                    }),
                )),
                other @ (SegmentProjection::Start
                | SegmentProjection::Interior(_)
                | SegmentProjection::CertifiedInterior(_)
                | SegmentProjection::End) => Ok(other.clone()),
            },
        }
    }

    const fn witness(&self, point: XY, segment: Segment) -> ExactDistanceWitness {
        match self.witness_kind {
            PointSegmentWitnessKind::Start => ExactDistanceWitness::Points(point, segment.start),
            PointSegmentWitnessKind::Interior => ExactDistanceWitness::Projected(point, segment),
            PointSegmentWitnessKind::End => ExactDistanceWitness::Points(point, segment.end),
        }
    }

    pub(crate) const fn distance_key(&self, point: XY, segment: Segment) -> SquaredDistanceKey {
        self.filter.bind(self.witness(point, segment))
    }

    pub(crate) fn precedes(
        &self,
        point: XY,
        segment: Segment,
        incumbent: &SquaredDistanceKey,
        tie: impl FnOnce() -> bool,
    ) -> Result<bool> {
        self.filter.disjoint_cmp(incumbent.filter).map_or_else(
            || {
                let ordering = exact_witness_cmp_with_identity(
                    self.witness(point, segment),
                    incumbent.witness,
                )?;
                Ok(ordering.is_lt() || ordering.is_eq() && tie())
            },
            |ordering| Ok(ordering.is_lt()),
        )
    }
}

/// The single projection owner classifies every clamped parameter before a
/// metric consumes it. The measurement and selection key both use the source
/// determinant; projection emission is a later witness operation.
pub(super) fn point_segment_metric(point: XY, segment: Segment) -> Result<PointSegmentMetric> {
    let classification = classify_segment_projection(point, segment);
    if matches!(&classification, ClassifiedSegmentProjection::Start) {
        return Ok(try_point_distance_squared(point, segment.start).map_or(
            PointSegmentMetric::Projection(SegmentProjection::Start),
            |squared| PointSegmentMetric::Squared {
                measurement_squared: squared,
                filter: SquaredDistanceFilter::point_pair(squared),
                witness_kind: PointSegmentWitnessKind::Start,
                projection: SelectionProjectionSource::Classified(classification),
            },
        ));
    }
    if matches!(&classification, ClassifiedSegmentProjection::End) {
        return Ok(try_point_distance_squared(point, segment.end).map_or(
            PointSegmentMetric::Projection(SegmentProjection::End),
            |squared| PointSegmentMetric::Squared {
                measurement_squared: squared,
                filter: SquaredDistanceFilter::point_pair(squared),
                witness_kind: PointSegmentWitnessKind::End,
                projection: SelectionProjectionSource::Classified(classification),
            },
        ));
    }
    Ok(match classification {
        ClassifiedSegmentProjection::FilteredInterior(filtered) => {
            let projection = projection_from_classification(
                point,
                segment,
                ClassifiedSegmentProjection::FilteredInterior(filtered),
            )?;
            if let Some((measurement_squared, filter)) = source_interior_measurement(point, segment)
            {
                PointSegmentMetric::Squared {
                    measurement_squared,
                    filter,
                    witness_kind: PointSegmentWitnessKind::Interior,
                    projection: SelectionProjectionSource::Ready(projection),
                }
            } else {
                PointSegmentMetric::Projection(projection)
            }
        },
        ClassifiedSegmentProjection::Declined => return point_segment_metric_exact(point, segment),
        ClassifiedSegmentProjection::Start | ClassifiedSegmentProjection::End => unreachable!(),
    })
}

pub(in crate::geometry) fn point_segment_distance_and_projection(
    point: XY,
    segment: Segment,
) -> Result<(f64, SegmentProjection)> {
    match point_segment_metric(point, segment)? {
        PointSegmentMetric::Squared {
            measurement_squared,
            projection,
            ..
        } => {
            let projection = match projection {
                SelectionProjectionSource::Classified(classification) => {
                    projection_from_classification(point, segment, classification)?
                },
                SelectionProjectionSource::Ready(projection) => projection,
            };
            Ok((measurement_squared.sqrt(), projection))
        },
        PointSegmentMetric::Projection(projection) => {
            let distance = projection.continuum_distance(point, segment)?;
            Ok((distance, projection))
        },
    }
}

/// Exact classification and squared-ratio rounding handle filter declines.
fn point_segment_metric_exact(point: XY, segment: Segment) -> Result<PointSegmentMetric> {
    Ok(match classify_segment_projection_exact(point, segment)? {
        ExactClassifiedSegmentProjection::Start => try_point_distance_squared(point, segment.start)
            .map_or(
                PointSegmentMetric::Projection(SegmentProjection::Start),
                |squared| PointSegmentMetric::Squared {
                    measurement_squared: squared,
                    filter: SquaredDistanceFilter::unbounded(),
                    witness_kind: PointSegmentWitnessKind::Start,
                    projection: SelectionProjectionSource::Ready(SegmentProjection::Start),
                },
            ),
        ExactClassifiedSegmentProjection::End => try_point_distance_squared(point, segment.end)
            .map_or(
                PointSegmentMetric::Projection(SegmentProjection::End),
                |squared| PointSegmentMetric::Squared {
                    measurement_squared: squared,
                    filter: SquaredDistanceFilter::unbounded(),
                    witness_kind: PointSegmentWitnessKind::End,
                    projection: SelectionProjectionSource::Ready(SegmentProjection::End),
                },
            ),
        ExactClassifiedSegmentProjection::Interior(terms) => {
            let squared = ExactSquaredDistance::interior(&terms)?;
            let rounded = squared.rounded_trustworthy()?;
            let fraction = round_ratio(&terms.numerator, &terms.denominator)?;
            let projection = SegmentProjection::ExactInterior(Box::new(ExactSegmentProjection {
                numerator: terms.numerator,
                denominator: terms.denominator,
                fraction,
                emitted_foot: None,
            }));
            match rounded {
                Some(measurement_squared) => PointSegmentMetric::Squared {
                    measurement_squared,
                    filter: SquaredDistanceFilter::source_squared(
                        measurement_squared,
                        2.0 * (f64::EPSILON / 2.0),
                    ),
                    witness_kind: PointSegmentWitnessKind::Interior,
                    projection: SelectionProjectionSource::Ready(projection),
                },
                None => PointSegmentMetric::Projection(projection),
            }
        },
    })
}

#[cfg(test)]
mod tests;
