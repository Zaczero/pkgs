use std::cmp::Ordering;

use super::GeographicError;
use crate::geometry::exact_arith::product_sum::ExactSum;
use crate::geometry::{Bounds, Point};

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct AngularFactor(f64);

impl AngularFactor {
    pub(crate) fn try_new(
        name: &'static str,
        radians_per_unit: f64,
    ) -> Result<Self, GeographicError> {
        if radians_per_unit.is_finite() && radians_per_unit > 0.0 {
            Ok(Self(radians_per_unit))
        } else {
            Err(GeographicError::InvalidAngularFactor {
                name,
                value: radians_per_unit,
            })
        }
    }

    pub(crate) const fn radians_per_unit(self) -> f64 {
        self.0
    }

    pub(crate) fn recognized_period(self) -> Option<f64> {
        proj_sys::gometry_conventional_period(self.0)
    }

    pub(crate) fn effective_radians_per_unit(self) -> f64 {
        self.recognized_period()
            .map_or(self.0, |period| std::f64::consts::TAU / period)
    }

    pub(crate) fn to_radians(self, value: f64) -> f64 {
        self.recognized_period().map_or(value * self.0, |period| {
            value / period * std::f64::consts::TAU
        })
    }

    pub(crate) fn degrees_to_native(self, degrees: f64, period: f64) -> f64 {
        if period == 360.0 {
            return degrees;
        }
        self.recognized_period().map_or_else(
            || degrees.to_radians() / self.0,
            |selected| degrees / 360.0 * selected,
        )
    }

    pub(crate) fn native_to_degrees(self, value: f64, period: f64) -> f64 {
        if period == 360.0 {
            return value;
        }
        self.recognized_period().map_or_else(
            || (value * self.0).to_degrees(),
            |selected| value / (selected / 4.0) * 90.0,
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct PeriodicAxis {
    unit: AngularFactor,
    period: f64,
    half_period: f64,
}

pub(crate) mod retained_native_private {
    pub(crate) trait Sealed {}
}

pub(crate) trait RetainedNativeLongitude:
    retained_native_private::Sealed + Clone + std::fmt::Debug + PartialEq + Sized
{
}

impl retained_native_private::Sealed for CanonicalLongitude {}
impl RetainedNativeLongitude for CanonicalLongitude {}

/// One native longitude together with its integral lift from the native window.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct RetainedLongitudeSite<L: RetainedNativeLongitude = CanonicalLongitude> {
    native: L,
    periods: i64,
}

impl<L: RetainedNativeLongitude> RetainedLongitudeSite<L> {
    pub(crate) fn new(native: L, periods: i64) -> Self {
        Self { native, periods }
    }

    pub(crate) fn native(&self) -> &L {
        &self.native
    }

    #[cfg(test)]
    pub(crate) const fn periods(&self) -> i64 {
        self.periods
    }
}

/// One directed chart edge whose endpoints and integral branch survive
/// canonical publication.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct RetainedChartDisplacement<L: RetainedNativeLongitude = CanonicalLongitude> {
    endpoints: [L; 2],
    periods: i64,
}

impl<L: RetainedNativeLongitude> RetainedChartDisplacement<L> {
    pub(crate) fn from_native_endpoints(endpoints: [L; 2], periods: i64) -> Self {
        Self { endpoints, periods }
    }

    #[cfg(test)]
    pub(crate) fn between_sites(
        sites: [RetainedLongitudeSite<L>; 2],
    ) -> crate::error::Result<Self> {
        let [start, end] = sites;
        let periods = end
            .periods()
            .checked_sub(start.periods())
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        Ok(Self {
            endpoints: [start.native, end.native],
            periods,
        })
    }

    pub(crate) fn endpoints(&self) -> &[L; 2] {
        &self.endpoints
    }

    pub(crate) const fn periods(&self) -> i64 {
        self.periods
    }
}

impl RetainedChartDisplacement<CanonicalLongitude> {
    pub(crate) fn delta_native(&self, axis: PeriodicAxis) -> f64 {
        crate::geometry::exact_arith::retained_delta_native(
            self.endpoints[0].get(),
            self.endpoints[1].get(),
            axis.period(),
            self.periods,
        )
    }
}

impl PeriodicAxis {
    pub(crate) fn try_new(
        name: &'static str,
        unit: AngularFactor,
    ) -> Result<Self, GeographicError> {
        let period = unit
            .recognized_period()
            .unwrap_or(std::f64::consts::TAU / unit.0);
        let half_period = period / 2.0;
        if period.is_finite()
            && period > 0.0
            && half_period > 0.0
            && (half_period * 2.0).to_bits() == period.to_bits()
        {
            Ok(Self {
                unit,
                period,
                half_period,
            })
        } else {
            Err(GeographicError::InvalidAngularFactor {
                name,
                value: unit.0,
            })
        }
    }

    #[cfg(test)]
    pub(crate) const fn unit(self) -> AngularFactor {
        self.unit
    }
    pub(crate) const fn period(self) -> f64 {
        self.period
    }
    pub(crate) const fn half_period(self) -> f64 {
        self.half_period
    }
    pub(crate) fn radians_per_unit(self) -> f64 {
        self.unit.effective_radians_per_unit()
    }
    pub(crate) fn to_degrees(self, value: f64) -> f64 {
        if self.period == 360.0 {
            value
        } else if let Some(period) = self.conventional_integer_period() {
            value / period * 360.0
        } else {
            (value * self.unit.radians_per_unit()).to_degrees()
        }
    }
    pub(crate) fn from_degrees(self, value: f64) -> f64 {
        if self.period == 360.0 {
            value
        } else if let Some(period) = self.conventional_integer_period() {
            value / 360.0 * period
        } else {
            value.to_radians() / self.unit.radians_per_unit()
        }
    }

    /// Whether a newly-created coordinate edge would be read across the
    /// periodic seam. A full-turn edge retains its existing interpretation.
    pub(crate) fn edge_exceeds_half_period(self, from: f64, to: f64) -> bool {
        // Canonical interior longitudes need only one subtraction; aliases
        // use the same seam spelling rule as geographic topology.
        if from.abs() < self.half_period && to.abs() < self.half_period {
            return (to - from).abs() > self.half_period;
        }
        if matches!(self.chart_step(from, to), Ok(ChartStep::Circle(_))) {
            return false;
        }
        let raw_delta = (to - from).abs();
        let (Ok(from), Ok(to)) = (self.canonicalize(from), self.canonicalize(to)) else {
            return false;
        };
        if from.identity(self) == SeamIdentity::Seam || to.identity(self) == SeamIdentity::Seam {
            raw_delta > self.half_period && raw_delta < self.period.next_up()
        } else {
            (to.get() - from.get()).abs() > self.half_period
        }
    }

    pub(crate) fn full_turn_delta(self, from: f64, to: f64) -> Option<f64> {
        self.full_turn_connection(from, to).map(|(delta, ..)| delta)
    }

    fn full_turn_connection(
        self,
        from: f64,
        to: f64,
    ) -> Option<(f64, CanonicalLongitude, CanonicalLongitude)> {
        let delta = to - from;
        if !delta.is_finite() || delta.abs().to_bits() != self.period.to_bits() {
            return None;
        }
        let signed_period = self.period.copysign(delta);
        let mut exact_residual = ExactSum::zero();
        exact_residual.add_product(to, 1.0);
        exact_residual.subtract_product(from, 1.0);
        exact_residual.subtract_product(signed_period, 1.0);
        if exact_residual.ordering() != Ordering::Equal {
            return None;
        }
        // The two chart spellings of the seam are one source position. A
        // stored edge between them is not a full turn; a full-turn path must
        // carry an ordinary longitude or intermediate source vertices.
        let from = self.wrap_admitted(from);
        let to = self.wrap_admitted(to);
        if from.identity(self) == SeamIdentity::Seam && to.identity(self) == SeamIdentity::Seam {
            return None;
        }
        Some((delta.signum() * self.period, from, to))
    }

    /// The chart's single interpretation of one stored longitude edge.
    /// Explicit non-seam period edges are circles; the two seam spellings
    /// remain the same quotient point.
    pub(crate) fn chart_step(self, from: f64, to: f64) -> Result<ChartStep, GeographicError> {
        Ok(self.chart_connection(from, to)?.step)
    }

    /// Return one admitted chart step together with its authentic canonical
    /// endpoint relation and integral source lift.
    pub(crate) fn chart_connection(
        self,
        from: f64,
        to: f64,
    ) -> Result<ChartConnection, GeographicError> {
        #[cfg(test)]
        source_admission_counts::connection();
        if let Some((delta, start, end)) = self.full_turn_connection(from, to) {
            let direction = if delta.is_sign_negative() {
                TurnDirection::Negative
            } else {
                TurnDirection::Positive
            };
            return Ok(ChartConnection {
                step: ChartStep::Circle(direction),
                start,
                end,
                source_lift: if direction == TurnDirection::Negative {
                    -1
                } else {
                    1
                },
            });
        }
        let connection = self.shortest_edge_after_circle_check(from, to)?;
        Ok(ChartConnection {
            step: ChartStep::Shortest(connection.direction),
            start: connection.start,
            end: connection.end,
            source_lift: connection.source_lift,
        })
    }

    /// Admit explicit producer endpoint spellings and their directed lift.
    /// The integer is branch authority; it is never inferred from a rounded
    /// displacement.
    #[cfg(test)]
    pub(crate) fn retained_native_endpoints(
        self,
        from: f64,
        to: f64,
        periods: i64,
    ) -> Result<RetainedChartDisplacement, GeographicError> {
        let start = self.canonicalize(from)?;
        let end = self.canonicalize(to)?;
        Ok(RetainedChartDisplacement::from_native_endpoints(
            [start, end],
            periods,
        ))
    }

    pub(crate) fn native_site_at(
        self,
        edge: &RetainedChartDisplacement,
        parameter: &f64,
    ) -> crate::error::Result<RetainedLongitudeSite> {
        let (native, periods) = crate::geometry::exact_arith::retained_native_site(
            edge.endpoints[0].get(),
            edge.endpoints[1].get(),
            self.period(),
            self.half_period(),
            edge.periods,
            *parameter,
        )?;
        Ok(RetainedLongitudeSite::new(
            CanonicalLongitude(native),
            periods,
        ))
    }

    pub(crate) fn lifted_site_longitude(
        self,
        site: &RetainedLongitudeSite,
    ) -> crate::error::Result<f64> {
        crate::geometry::exact_arith::retained_lifted_native(
            site.native.get(),
            self.period,
            site.periods,
        )
    }

    /// The strict-interior certificate for an edge with zero quotient-seam
    /// transit under the shared shortest-edge rule.
    pub(crate) fn ordinary_chart_edge(self, from: f64, to: f64) -> bool {
        from.abs() < self.half_period
            && to.abs() < self.half_period
            && (to - from).abs() < self.half_period
    }

    /// Signed seam crossing of the chart step; ordinary canonical edges avoid
    /// canonicalization when their represented delta cannot reach the seam.
    pub(crate) fn chart_transit(self, from: f64, to: f64) -> Result<i8, GeographicError> {
        if self.ordinary_chart_edge(from, to) {
            return Ok(0);
        }
        Ok(self.chart_connection(from, to)?.seam_transit())
    }

    pub(crate) fn add_chart_transit(
        self,
        chart_turn: i64,
        from: f64,
        to: f64,
    ) -> Result<i64, GeographicError> {
        chart_turn
            .checked_add(i64::from(self.chart_transit(from, to)?))
            .ok_or(GeographicError::ChartTurnOverflow)
    }

    fn conventional_integer_period(self) -> Option<f64> {
        (self.period.fract() == 0.0).then_some(self.period)
    }

    pub(crate) fn ordinary_longitude_bound(self) -> f64 {
        self.conventional_integer_period()
            .filter(|period| (period / 2.0).fract() == 0.0)
            .map_or(self.half_period, |period| {
                self.half_period.min(period / 2.0)
            })
    }

    pub(crate) fn canonicalize(self, value: f64) -> Result<CanonicalLongitude, GeographicError> {
        #[cfg(test)]
        source_admission_counts::canonicalization();
        if !value.is_finite() {
            return Err(GeographicError::NonFinite {
                name: "longitude",
                value,
            });
        }
        if let Some(longitude) = self.ordinary_longitude(value) {
            return Ok(longitude);
        }
        Ok(self.wrap_admitted(value))
    }

    #[expect(
        clippy::float_cmp,
        reason = "the nominal half-period alias is an exact seam spelling"
    )]
    pub(crate) fn ordinary_longitude(self, value: f64) -> Option<CanonicalLongitude> {
        if value.abs().partial_cmp(&self.half_period) != Some(Ordering::Less) {
            return None;
        }
        // A rounded nominal half-period can lie just inside the derived half,
        // while wrap_admitted still treats that integer alias as the seam.
        if value.fract() == 0.0
            && self
                .conventional_integer_period()
                .is_some_and(|period| value.abs() == period / 2.0)
        {
            return None;
        }
        Some(CanonicalLongitude(if value == 0.0 { 0.0 } else { value }))
    }

    /// Preserve a source longitude while selecting its nearest periodic lift.
    #[expect(
        clippy::float_cmp,
        clippy::arithmetic_side_effects,
        reason = "finite subtraction may overflow into the exact lane; the strict half-period proof preserves source bits for k=0"
    )]
    pub(crate) fn nearest_longitude_alias(
        self,
        lifted_reference: f64,
        longitude: f64,
    ) -> Result<f64, GeographicError> {
        if !lifted_reference.is_finite() {
            return Err(GeographicError::NonFinite {
                name: "longitude",
                value: lifted_reference,
            });
        }
        if !longitude.is_finite() {
            return Err(GeographicError::NonFinite {
                name: "longitude",
                value: longitude,
            });
        }

        let difference = lifted_reference - longitude;
        if difference.is_finite() && difference.abs() < self.half_period {
            return Ok(longitude);
        }

        let alias = self
            .nearest_longitude_alias_with_certified_quotient(
                lifted_reference,
                longitude,
                difference,
            )
            .unwrap_or_else(|| {
                crate::geometry::exact_arith::nearest_periodic_alias(
                    longitude,
                    lifted_reference,
                    self.period,
                )
            });
        if alias.is_finite() {
            Ok(alias)
        } else {
            Err(GeographicError::NonFinite {
                name: "longitude",
                value: alias,
            })
        }
    }

    #[expect(
        clippy::float_cmp,
        clippy::arithmetic_side_effects,
        reason = "the small quotient is only a candidate until exact residual checks certify it; the one integer increment stays below 2^52"
    )]
    fn nearest_longitude_alias_with_certified_quotient(
        self,
        lifted_reference: f64,
        longitude: f64,
        difference: f64,
    ) -> Option<f64> {
        const INTEGER_COUNT_LIMIT: f64 = 4_503_599_627_370_496.0;

        if !difference.is_finite() {
            return None;
        }
        let approximate_count = difference.abs() / self.period;
        if !approximate_count.is_finite() || approximate_count >= INTEGER_COUNT_LIMIT {
            return None;
        }
        let count = approximate_count.floor();
        let mut remainder = ExactSum::zero();
        if difference.is_sign_negative() {
            remainder.add_product(longitude, 1.0);
            remainder.subtract_product(lifted_reference, 1.0);
        } else {
            remainder.add_product(lifted_reference, 1.0);
            remainder.subtract_product(longitude, 1.0);
        }
        remainder.subtract_product(self.period, count);
        if remainder.ordering() == Ordering::Less {
            return None;
        }
        let mut after_period = remainder.clone();
        after_period.subtract_product(self.period, 1.0);
        if after_period.ordering() == Ordering::Greater {
            return None;
        }
        let mut half_period = remainder;
        half_period.subtract_product(self.period, 0.5);
        let count = if half_period.ordering() == Ordering::Greater {
            count + 1.0
        } else {
            count
        };
        if count == 0.0 {
            return Some(longitude);
        }
        let periods = count.copysign(difference);
        let mut lifted = ExactSum::zero();
        lifted.add_product(longitude, 1.0);
        lifted.add_product(self.period, periods);
        Some(lifted.round_to_binary64())
    }

    /// Wrap a finite longitude with the canonical remainder, integer-period
    /// aliases, and signed-zero spelling. Ingress must already have rejected
    /// non-finite values.
    #[expect(
        clippy::float_cmp,
        reason = "exact zero and integer seam aliases are part of the coordinate contract"
    )]
    pub(super) fn wrap_admitted(self, value: f64) -> CanonicalLongitude {
        debug_assert!(
            value.is_finite(),
            "wrap_admitted requires a finite longitude"
        );
        // `value + half_period` can overflow. The remainder first keeps every
        // finite binary64 input admissible.
        let mut reduced = value % self.period;
        // PROJ's grad factor is a rounded representation of pi/200, so its
        // derived period is one ULP below 400. Preserve the conventional
        // integer aliases (400 and 200 grads) without widening ordinary
        // coordinates near zero or the seam.
        if value.fract() == 0.0
            && let Some(nominal_period) = self.conventional_integer_period()
        {
            let nominal_remainder = value % nominal_period;
            if nominal_remainder == 0.0 {
                reduced = 0.0;
            } else if nominal_remainder.abs() == nominal_period / 2.0 {
                reduced = -self.half_period;
            }
        }
        if reduced < -self.half_period {
            reduced += self.period;
        } else if reduced >= self.half_period {
            reduced -= self.period;
        }
        if reduced == 0.0 {
            reduced = 0.0;
        }
        CanonicalLongitude(reduced)
    }

    /// Shortest signed step between longitude aliases; a whole-period
    /// difference is zero. Stored chart edges use [`Self::chart_step`] so an
    /// explicit non-seam period edge retains its circle.
    pub(crate) fn shortest_alias_step(
        self,
        from: f64,
        to: f64,
    ) -> Result<EdgeDirection, GeographicError> {
        Ok(self.shortest_alias(from, to)?.direction)
    }

    fn shortest_alias(self, from: f64, to: f64) -> Result<ShortestConnection, GeographicError> {
        if self.full_turn_delta(from, to).is_some() {
            return Ok(ShortestConnection {
                direction: EdgeDirection::Minor(0.0),
                start: self.canonicalize(from)?,
                end: self.canonicalize(to)?,
                source_lift: 0,
            });
        }
        self.shortest_edge_after_circle_check(from, to)
    }

    #[expect(
        clippy::float_cmp,
        reason = "only the exact represented half-period is a directional tie"
    )]
    fn shortest_edge_after_circle_check(
        self,
        from: f64,
        to: f64,
    ) -> Result<ShortestConnection, GeographicError> {
        if !from.is_finite() {
            return Err(GeographicError::NonFinite {
                name: "longitude",
                value: from,
            });
        }
        if !to.is_finite() {
            return Err(GeographicError::NonFinite {
                name: "longitude",
                value: to,
            });
        }
        let negative_direction = to < from;
        let raw = to - from;
        if from.abs() < self.half_period
            && to.abs() < self.half_period
            && raw.is_finite()
            && raw.abs() < self.half_period
        {
            let canonical = |value| CanonicalLongitude(if value == 0.0 { 0.0 } else { value });
            return Ok(ShortestConnection {
                direction: EdgeDirection::Minor(if raw == 0.0 { 0.0 } else { raw }),
                start: canonical(from),
                end: canonical(to),
                source_lift: 0,
            });
        }

        let from = self.canonicalize(from)?;
        let to = self.canonicalize(to)?;
        let mut exact = ExactSum::zero();
        exact.add_product(to.get(), 1.0);
        exact.subtract_product(from.get(), 1.0);

        let mut positive_half_comparison = exact.clone();
        positive_half_comparison.subtract_product(self.half_period, 1.0);
        let mut negative_half_comparison = exact.clone();
        negative_half_comparison.add_product(self.half_period, 1.0);
        if positive_half_comparison.ordering() == Ordering::Equal
            || negative_half_comparison.ordering() == Ordering::Equal
        {
            let source_lift = match (exact.ordering(), negative_direction) {
                (Ordering::Greater, true) => -1,
                (Ordering::Less, false) => 1,
                _ => 0,
            };
            return Ok(ShortestConnection {
                direction: EdgeDirection::HalfTurn(if negative_direction {
                    TurnDirection::Negative
                } else {
                    TurnDirection::Positive
                }),
                start: from,
                end: to,
                source_lift,
            });
        }

        let source_lift = if positive_half_comparison.ordering() == Ordering::Greater {
            exact.subtract_product(self.period, 1.0);
            -1
        } else if negative_half_comparison.ordering() == Ordering::Less {
            exact.add_product(self.period, 1.0);
            1
        } else {
            0
        };
        let delta = exact.round_to_binary64();
        Ok(ShortestConnection {
            direction: EdgeDirection::Minor(delta),
            start: from,
            end: to,
            source_lift,
        })
    }

    /// Return the signed number of canonical seam transits made by the
    /// shortest alias step. The endpoints are admitted once here, and the
    /// direction comes from [`Self::shortest_alias_step`], including its exact
    /// source-directed half-turn tie.
    #[cfg(test)]
    pub(crate) fn seam_transit(self, from: f64, to: f64) -> Result<i8, GeographicError> {
        self.seam_transit_with_direction(from, to, self.shortest_alias_step(from, to)?)
    }

    fn seam_transit_with_direction(
        self,
        from: f64,
        to: f64,
        edge: EdgeDirection,
    ) -> Result<i8, GeographicError> {
        Ok(ChartStep::Shortest(edge)
            .transit_from_canonical(self.canonicalize(from)?, self.canonicalize(to)?))
    }
}

impl ChartStep {
    pub(crate) fn transit_from_canonical(
        self,
        from: CanonicalLongitude,
        to: CanonicalLongitude,
    ) -> i8 {
        let edge = match self {
            Self::Circle(TurnDirection::Positive) => return 1,
            Self::Circle(TurnDirection::Negative) => return -1,
            Self::Shortest(edge) => edge,
        };
        let direction = match edge {
            EdgeDirection::Minor(delta) if delta > 0.0 => Some(TurnDirection::Positive),
            EdgeDirection::Minor(delta) if delta < 0.0 => Some(TurnDirection::Negative),
            EdgeDirection::HalfTurn(direction) => Some(direction),
            EdgeDirection::Minor(_) => None,
        };
        match direction {
            Some(TurnDirection::Positive) if to.get() < from.get() => 1,
            Some(TurnDirection::Negative) if to.get() > from.get() => -1,
            _ => 0,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct GeographicFrame {
    longitude: PeriodicAxis,
    latitude_unit: AngularFactor,
    pole: f64,
    prime_meridian_radians: f64,
    orientation: GeographicAxisOrientation,
}

impl PartialEq for GeographicFrame {
    fn eq(&self, other: &Self) -> bool {
        self.longitude.period() == other.longitude.period()
            && self.longitude.half_period() == other.longitude.half_period()
            && self.longitude.radians_per_unit() == other.longitude.radians_per_unit()
            && self.latitude_unit.effective_radians_per_unit()
                == other.latitude_unit.effective_radians_per_unit()
            && self.pole == other.pole
            && self.prime_meridian_radians == other.prime_meridian_radians
            && self.orientation == other.orientation
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct GeographicAxisOrientation {
    longitude_east: bool,
    latitude_north: bool,
}

impl GeographicAxisOrientation {
    pub(crate) const fn from_axis_directions(longitude_east: bool, latitude_north: bool) -> Self {
        Self {
            longitude_east,
            latitude_north,
        }
    }

    pub(crate) const fn longitude_east(self) -> bool {
        self.longitude_east
    }

    pub(crate) const fn latitude_north(self) -> bool {
        self.latitude_north
    }
}

impl GeographicFrame {
    pub(crate) const fn crs84() -> Self {
        let radians_per_degree = std::f64::consts::PI / 180.0;
        Self {
            longitude: PeriodicAxis {
                unit: AngularFactor(radians_per_degree),
                period: 360.0,
                half_period: 180.0,
            },
            latitude_unit: AngularFactor(radians_per_degree),
            pole: 90.0,
            prime_meridian_radians: 0.0,
            orientation: GeographicAxisOrientation {
                longitude_east: true,
                latitude_north: true,
            },
        }
    }

    #[cfg(test)]
    pub(crate) fn try_new(
        longitude_unit: AngularFactor,
        latitude_unit: AngularFactor,
        prime_meridian_radians: f64,
    ) -> Result<Self, GeographicError> {
        Self::try_new_with_orientation(
            longitude_unit,
            latitude_unit,
            prime_meridian_radians,
            GeographicAxisOrientation {
                longitude_east: true,
                latitude_north: true,
            },
        )
    }

    pub(crate) fn try_new_with_orientation(
        longitude_unit: AngularFactor,
        latitude_unit: AngularFactor,
        prime_meridian_radians: f64,
        orientation: GeographicAxisOrientation,
    ) -> Result<Self, GeographicError> {
        if !prime_meridian_radians.is_finite() {
            return Err(GeographicError::NonFinite {
                name: "prime meridian",
                value: prime_meridian_radians,
            });
        }
        let pole = latitude_unit
            .recognized_period()
            .map_or(std::f64::consts::FRAC_PI_2 / latitude_unit.0, |period| {
                period / 4.0
            });
        if !pole.is_finite() || pole <= 0.0 {
            return Err(GeographicError::InvalidAngularFactor {
                name: "latitude unit",
                value: latitude_unit.0,
            });
        }
        Ok(Self {
            longitude: PeriodicAxis::try_new("longitude unit", longitude_unit)?,
            latitude_unit,
            pole,
            prime_meridian_radians: if prime_meridian_radians == 0.0 {
                0.0
            } else {
                prime_meridian_radians
            },
            orientation,
        })
    }

    pub(crate) const fn longitude(self) -> PeriodicAxis {
        self.longitude
    }
    pub(crate) const fn latitude_unit(self) -> AngularFactor {
        self.latitude_unit
    }
    pub(crate) const fn prime_meridian_radians(self) -> f64 {
        self.prime_meridian_radians
    }
    pub(crate) const fn pole(self) -> f64 {
        self.pole
    }
    pub(crate) const fn orientation(self) -> GeographicAxisOrientation {
        self.orientation
    }
    pub(crate) const fn longitude_orientation_sign(self) -> f64 {
        if self.orientation.longitude_east {
            1.0
        } else {
            -1.0
        }
    }
    pub(crate) const fn latitude_orientation_sign(self) -> f64 {
        if self.orientation.latitude_north {
            1.0
        } else {
            -1.0
        }
    }
    /// Native latitude coordinate of a physical geodetic pole.
    pub(crate) fn native_latitude_pole(self, pole: GeodeticPole) -> f64 {
        let direction = match pole {
            GeodeticPole::North => 1.0,
            GeodeticPole::South => -1.0,
        };
        direction * self.pole * self.latitude_orientation_sign()
    }

    /// Native latitude coordinate at the selected side of this chart.
    pub(crate) const fn chart_pole_latitude(self, pole: ChartPole) -> f64 {
        match pole {
            ChartPole::North => self.pole,
            ChartPole::South => -self.pole,
        }
    }
    pub(crate) fn latitude_radians_per_unit(self) -> f64 {
        self.latitude_unit.effective_radians_per_unit()
    }

    pub(crate) fn longitude_bounds_certify_no_seam(self, minx: f64, maxx: f64) -> bool {
        let axis = self.longitude;
        let bound = axis.ordinary_longitude_bound();
        minx > -bound && maxx < bound && maxx - minx < axis.half_period()
    }

    /// Whether raw extrema prove a geometry stays in one open, non-polar
    /// chart. Every edge is then an ordinary chart edge, so no seam scan or
    /// pole-winding check is needed.
    pub(crate) fn bounds_certify_ordinary_support(self, bounds: Bounds) -> bool {
        self.longitude_bounds_certify_no_seam(bounds.minx(), bounds.maxx())
            && bounds.miny() > -self.pole
            && bounds.maxy() < self.pole
    }

    /// Identity of one stored vertex in this frame's geographic quotient.
    ///
    /// The coordinate columns retain their source spelling; this key folds
    /// longitude aliases, the two seam spellings, and each pole to the
    /// identities used by exact geographic topology.
    pub(crate) fn quotient_vertex(self, x: f64, y: f64) -> QuotientVertex {
        let longitude = self.longitude.wrap_admitted(x);
        let latitude = Latitude::from_admitted(self, y);
        if let Some(pole) = latitude.chart_pole(self) {
            return QuotientVertex::ChartPole(pole);
        }
        if longitude.identity(self.longitude) == SeamIdentity::Seam {
            return QuotientVertex::Seam(latitude.get().to_bits());
        }
        QuotientVertex::Ordinary(longitude.get().to_bits(), latitude.get().to_bits())
    }

    /// Convert one admitted native-frame point to the canonical
    /// degree/Greenwich lanes consumed by geodesic and in-core kernels.
    pub(crate) fn to_geodesic_point(self, point: Point) -> Result<Point, GeographicError> {
        let (longitude, latitude) = self.to_geodesic_xy(point.x, point.y)?;
        Ok(point.with_xy_raw(longitude, latitude))
    }

    /// Convert a finite degree/Greenwich result back to this CRS's native
    /// angular axes, retaining any Z/M ordinates.
    pub(crate) fn point_from_geodesic(self, point: Point) -> Result<Point, GeographicError> {
        let (longitude, latitude) = self.native_geodesic_result(point.x, point.y)?;
        self.canonical_computed_point(point.with_xy_raw(longitude, latitude))
    }

    /// Canonicalize one newly computed point already expressed in this frame.
    /// Source points bypass this boundary so their stored coordinate bytes stay
    /// intact.
    pub(crate) fn canonical_computed_point(self, point: Point) -> Result<Point, GeographicError> {
        let (longitude, latitude) = self.canonical_computed_xy(point.x, point.y)?;
        Ok(point.with_xy_raw(longitude, latitude))
    }

    /// Canonicalize native target-geographic output lanes while retaining
    /// latitude bits and reporting the first invalid coordinate.
    pub(crate) fn canonicalize_computed_longitude_lanes_indexed(
        self,
        xs: &mut [f64],
        ys: &[f64],
    ) -> Result<(), (usize, GeographicError)> {
        for (index, (longitude, &latitude)) in xs.iter_mut().zip(ys).enumerate() {
            let (canonical_longitude, _) = self
                .canonical_computed_xy(*longitude, latitude)
                .map_err(|error| (index, error))?;
            *longitude = canonical_longitude;
        }
        Ok(())
    }

    /// Convert a route point without wrapping its longitude, then choose the
    /// alias nearest a caller-supplied lifted chart position.
    pub(crate) fn point_from_geodesic_in_lifted_chart(
        self,
        point: Point,
        lifted_reference: f64,
    ) -> Result<Point, GeographicError> {
        let (longitude, latitude) = self.xy_from_geodesic_unwrapped(point.x, point.y)?;
        let longitude = self
            .longitude
            .nearest_longitude_alias(lifted_reference, longitude)?;
        if !latitude.is_finite() {
            return Err(GeographicError::NonFinite {
                name: "native coordinate",
                value: latitude,
            });
        }
        Ok(point.with_xy_raw(longitude, latitude))
    }

    /// Convert mutable XY lanes to canonical Greenwich degrees. Z/T lanes are
    /// deliberately outside this owner and remain untouched by callers.
    pub(crate) fn to_geodesic_lanes(
        self,
        xs: &mut [f64],
        ys: &mut [f64],
    ) -> Result<(), GeographicError> {
        self.to_geodesic_lanes_indexed(xs, ys)
            .map_err(|(_, error)| error)
    }

    pub(crate) fn to_geodesic_lanes_indexed(
        self,
        xs: &mut [f64],
        ys: &mut [f64],
    ) -> Result<(), (usize, GeographicError)> {
        for (index, (longitude, latitude)) in std::iter::zip(xs, ys).enumerate() {
            (*longitude, *latitude) = self
                .to_geodesic_xy(*longitude, *latitude)
                .map_err(|error| (index, error))?;
        }
        Ok(())
    }

    /// Convert mutable canonical Greenwich-degree XY lanes to this CRS's
    /// native axes. Z/T lanes are not part of geographic angular conversion.
    pub(crate) fn lanes_from_geodesic(
        self,
        xs: &mut [f64],
        ys: &mut [f64],
    ) -> Result<(), GeographicError> {
        self.lanes_indexed_from_geodesic(xs, ys)
            .map_err(|(_, error)| error)
    }

    pub(crate) fn lanes_indexed_from_geodesic(
        self,
        xs: &mut [f64],
        ys: &mut [f64],
    ) -> Result<(), (usize, GeographicError)> {
        for (index, (longitude, latitude)) in std::iter::zip(xs, ys).enumerate() {
            (*longitude, *latitude) = self
                .xy_from_geodesic(*longitude, *latitude)
                .map_err(|error| (index, error))?;
        }
        Ok(())
    }

    /// Convert a transform's target-geographic lanes into the native angular
    /// units without reducing longitude. Shape publication retains these chart
    /// positions until computed edges have captured their intended steps.
    pub(crate) fn lanes_unwrapped_from_geodesic_indexed(
        self,
        xs: &mut [f64],
        ys: &mut [f64],
    ) -> Result<(), (usize, GeographicError)> {
        for (index, (longitude, latitude)) in std::iter::zip(xs, ys).enumerate() {
            (*longitude, *latitude) = self
                .xy_from_geodesic_unwrapped(*longitude, *latitude)
                .map_err(|error| (index, error))?;
        }
        Ok(())
    }

    pub(crate) fn to_geodesic_xy(
        self,
        longitude: f64,
        latitude: f64,
    ) -> Result<(f64, f64), GeographicError> {
        let longitude = self.longitude.canonicalize(longitude)?.get();
        let longitude = if self == Self::crs84() {
            longitude
        } else if self.prime_meridian_radians == 0.0 {
            self.longitude.to_degrees(longitude) * self.longitude_orientation_sign()
        } else {
            self.longitude.to_degrees(longitude) * self.longitude_orientation_sign()
                + self.prime_meridian_radians.to_degrees()
        };
        let longitude = Self::crs84().longitude().canonicalize(longitude)?.get();
        let latitude = self.to_geodesic_latitude(latitude)?;
        Ok((longitude, latitude))
    }

    pub(crate) fn to_geodesic_latitude(self, latitude: f64) -> Result<f64, GeographicError> {
        let admitted_latitude = Latitude::try_new(self, latitude)?.get();
        Ok(if self == Self::crs84() {
            // The canonical CRS84 lane is already in kernel degrees. Keep
            // its signed zero while still applying the admission check.
            if latitude == 0.0 {
                latitude
            } else {
                admitted_latitude
            }
        } else {
            self.latitude_unit
                .native_to_degrees(admitted_latitude, self.pole)
                * self.latitude_orientation_sign()
        })
    }

    fn xy_from_geodesic(
        self,
        longitude: f64,
        latitude: f64,
    ) -> Result<(f64, f64), GeographicError> {
        let (longitude, latitude) = self.native_geodesic_result(longitude, latitude)?;
        self.canonical_computed_xy(longitude, latitude)
    }

    fn native_geodesic_result(
        self,
        longitude: f64,
        latitude: f64,
    ) -> Result<(f64, f64), GeographicError> {
        ensure_geodesic_coordinates(longitude, latitude)?;
        let longitude = Self::crs84().longitude().canonicalize(longitude)?.get();
        Ok(self.native_xy_from_geodesic(longitude, latitude))
    }

    fn canonical_computed_xy(
        self,
        longitude: f64,
        latitude: f64,
    ) -> Result<(f64, f64), GeographicError> {
        let longitude = self.longitude.canonicalize(longitude)?.get();
        if !latitude.is_finite() {
            return Err(GeographicError::NonFinite {
                name: "native coordinate",
                value: latitude,
            });
        }
        Ok((longitude, latitude))
    }

    fn xy_from_geodesic_unwrapped(
        self,
        longitude: f64,
        latitude: f64,
    ) -> Result<(f64, f64), GeographicError> {
        ensure_geodesic_coordinates(longitude, latitude)?;
        Ok(self.native_xy_from_geodesic(longitude, latitude))
    }

    fn native_xy_from_geodesic(self, longitude: f64, latitude: f64) -> (f64, f64) {
        let longitude = if self == Self::crs84() {
            longitude
        } else {
            let relative = if self.prime_meridian_radians == 0.0 {
                longitude
            } else {
                longitude - self.prime_meridian_radians.to_degrees()
            } * self.longitude_orientation_sign();
            self.longitude.from_degrees(relative)
        };
        let latitude = if self == Self::crs84() {
            latitude
        } else {
            self.latitude_unit
                .degrees_to_native(latitude * self.latitude_orientation_sign(), self.pole)
        };
        (longitude, latitude)
    }
}

const fn ensure_geodesic_coordinates(longitude: f64, latitude: f64) -> Result<(), GeographicError> {
    if !longitude.is_finite() || !latitude.is_finite() {
        return Err(GeographicError::NonFinite {
            name: "geodesic coordinate",
            value: if longitude.is_finite() {
                latitude
            } else {
                longitude
            },
        });
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct CanonicalLongitude(f64);
impl CanonicalLongitude {
    pub(crate) const fn get(self) -> f64 {
        self.0
    }
    #[expect(
        clippy::float_cmp,
        reason = "seam identity has no epsilon neighborhood"
    )]
    pub(crate) fn identity(self, axis: PeriodicAxis) -> SeamIdentity {
        if self.0 == -axis.half_period() {
            SeamIdentity::Seam
        } else {
            SeamIdentity::Ordinary
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct Latitude(f64);
impl Latitude {
    pub(crate) fn try_new(frame: GeographicFrame, value: f64) -> Result<Self, GeographicError> {
        if !value.is_finite() {
            return Err(GeographicError::NonFinite {
                name: "latitude",
                value,
            });
        }
        let admitted = Self::from_admitted(frame, value);
        let pole = frame.pole();
        if admitted.get() < -pole || admitted.get() > pole {
            return Err(GeographicError::LatitudeOutOfRange {
                value: admitted.get(),
            });
        }
        Ok(admitted)
    }

    /// Fold the one-exterior-ULP pole neighbor and signed zero after ingress
    /// has validated the value. Geographic topology uses this admitted latitude
    /// while the native point retains its original ordinate.
    #[expect(
        clippy::float_cmp,
        reason = "admission matches exactly the measured adjacent binary64 values"
    )]
    pub(crate) fn from_admitted(frame: GeographicFrame, value: f64) -> Self {
        debug_assert!(
            value.is_finite(),
            "from_admitted requires a finite latitude"
        );
        let pole = frame.pole();
        // PROJ WKT angular-factor round trips can place the nominal pole one
        // native-unit ULP outside the derived pole. Admit exactly that measured
        // exterior neighbor; interior values remain bit-preserved.
        let value = if value == pole.next_up() {
            pole
        } else if value == (-pole).next_down() {
            -pole
        } else {
            value
        };
        let value = if value == 0.0 { 0.0 } else { value };
        Self(value)
    }
    pub(crate) const fn get(self) -> f64 {
        self.0
    }
    #[expect(
        clippy::float_cmp,
        reason = "pole identity has no epsilon neighborhood"
    )]
    pub(crate) fn identity(self, frame: GeographicFrame) -> SeamIdentity {
        if self.0.abs() == frame.pole() {
            SeamIdentity::Pole
        } else {
            SeamIdentity::Ordinary
        }
    }

    pub(crate) fn chart_pole(self, frame: GeographicFrame) -> Option<ChartPole> {
        (self.identity(frame) == SeamIdentity::Pole).then(|| {
            if self.0.is_sign_negative() {
                ChartPole::South
            } else {
                ChartPole::North
            }
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SeamIdentity {
    Ordinary,
    Seam,
    Pole,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum QuotientVertex {
    Ordinary(u64, u64),
    Seam(u64),
    ChartPole(ChartPole),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TurnDirection {
    Negative,
    Positive,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
/// North or south by the sign of the native chart latitude, independent of axis orientation.
pub(crate) enum ChartPole {
    North,
    South,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// Physical north or south on the geodetic ellipsoid.
pub(crate) enum GeodeticPole {
    North,
    South,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// A nonzero chart turn's bounded side in the sign of native latitude.
pub(crate) enum FullTurnPole {
    Bounded(ChartPole),
    Ambiguous,
}

impl FullTurnPole {
    /// Classify a nonzero chart turn by its native-latitude hemisphere.
    pub(crate) fn of_latitude_span(south: f64, north: f64) -> Self {
        if north > 0.0 && south >= 0.0 {
            Self::Bounded(ChartPole::North)
        } else if south < 0.0 && north <= 0.0 {
            Self::Bounded(ChartPole::South)
        } else {
            Self::Ambiguous
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum ChartStep {
    Circle(TurnDirection),
    Shortest(EdgeDirection),
}

impl ChartStep {
    pub(crate) fn delta(self, axis: PeriodicAxis) -> f64 {
        match self {
            Self::Circle(TurnDirection::Negative) => -axis.period(),
            Self::Circle(TurnDirection::Positive) => axis.period(),
            Self::Shortest(direction) => direction.delta(axis),
        }
    }

    pub(crate) const fn is_circle(self) -> bool {
        matches!(self, Self::Circle(_))
    }

    pub(crate) fn transit(
        self,
        axis: PeriodicAxis,
        from: f64,
        to: f64,
    ) -> Result<i8, GeographicError> {
        match self {
            Self::Circle(TurnDirection::Negative) => Ok(-1),
            Self::Circle(TurnDirection::Positive) => Ok(1),
            Self::Shortest(direction) => axis.seam_transit_with_direction(from, to, direction),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum EdgeDirection {
    Minor(f64),
    HalfTurn(TurnDirection),
}
impl EdgeDirection {
    pub(crate) fn delta(self, axis: PeriodicAxis) -> f64 {
        match self {
            Self::Minor(delta) => delta,
            Self::HalfTurn(TurnDirection::Negative) => -axis.half_period(),
            Self::HalfTurn(TurnDirection::Positive) => axis.half_period(),
        }
    }
}

#[cfg(test)]
pub(crate) mod source_admission_counts {
    use std::cell::Cell;

    thread_local! {
        static COUNTS: Cell<(usize, usize)> = const { Cell::new((0, 0)) };
    }

    pub(crate) fn snapshot() -> (usize, usize) {
        COUNTS.get()
    }

    pub(super) fn connection() {
        let (connections, canonicalizations) = COUNTS.get();
        COUNTS.set((connections + 1, canonicalizations));
    }

    pub(super) fn canonicalization() {
        let (connections, canonicalizations) = COUNTS.get();
        COUNTS.set((connections, canonicalizations + 1));
    }
}

/// One admitted edge decision with authentic canonical endpoints and the
/// integral lift that selects its directed source branch.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct ChartConnection {
    pub(crate) step: ChartStep,
    pub(crate) start: CanonicalLongitude,
    pub(crate) end: CanonicalLongitude,
    pub(crate) source_lift: i8,
}

impl ChartConnection {
    pub(crate) fn retained(self) -> RetainedChartDisplacement {
        RetainedChartDisplacement::from_native_endpoints(
            [self.start, self.end],
            i64::from(self.source_lift),
        )
    }

    pub(crate) fn seam_transit(self) -> i8 {
        self.step.transit_from_canonical(self.start, self.end)
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct ShortestConnection {
    direction: EdgeDirection,
    start: CanonicalLongitude,
    end: CanonicalLongitude,
    source_lift: i8,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RingTurn {
    None,
    Full(TurnDirection),
    Other,
}

impl RingTurn {
    pub(crate) const fn from_chart_turn(chart_turn: i64) -> Self {
        match chart_turn {
            0 => Self::None,
            1 => Self::Full(TurnDirection::Positive),
            -1 => Self::Full(TurnDirection::Negative),
            _ => Self::Other,
        }
    }
}
