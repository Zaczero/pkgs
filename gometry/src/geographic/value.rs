use super::GeographicError;
use crate::geometry::Point;

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
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct PeriodicAxis {
    unit: AngularFactor,
    period: f64,
    half_period: f64,
}

impl PeriodicAxis {
    pub(crate) fn try_new(
        name: &'static str,
        unit: AngularFactor,
    ) -> Result<Self, GeographicError> {
        let period = std::f64::consts::TAU / unit.0;
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

    pub(crate) const fn unit(self) -> AngularFactor {
        self.unit
    }
    pub(crate) const fn period(self) -> f64 {
        self.period
    }
    pub(crate) const fn half_period(self) -> f64 {
        self.half_period
    }

    pub(crate) fn full_turn_delta(self, from: f64, to: f64) -> Option<f64> {
        let delta = to - from;
        if !delta.is_finite() {
            return None;
        }
        let magnitude = delta.abs();
        let is_full_turn = magnitude.to_bits() == self.period.to_bits()
            || self
                .conventional_integer_period()
                .is_some_and(|period| magnitude.to_bits() == period.to_bits());
        is_full_turn.then_some(delta.signum() * self.period)
    }

    fn conventional_integer_period(self) -> Option<f64> {
        let period = self.period.round();
        (period > 0.0 && (self.period - period).abs() <= 8.0 * f64::EPSILON * period.abs())
            .then_some(period)
    }

    #[expect(
        clippy::float_cmp,
        reason = "exact zero and integer seam aliases are part of the coordinate contract"
    )]
    pub(crate) fn canonicalize(self, value: f64) -> Result<CanonicalLongitude, GeographicError> {
        if !value.is_finite() {
            return Err(GeographicError::NonFinite {
                name: "longitude",
                value,
            });
        }
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
        Ok(CanonicalLongitude(reduced))
    }

    #[expect(
        clippy::float_cmp,
        reason = "only the exact represented half-period is a directional tie"
    )]
    pub(crate) fn shortest_edge(
        self,
        from: f64,
        to: f64,
    ) -> Result<EdgeDirection, GeographicError> {
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
        // Preserve finite source deltas so an exact winding such as three
        // 120-degree edges remains exact. Reduce only after recognizing a
        // complete periodic alias; canonical endpoints handle subtraction
        // overflow and the remaining non-finite cases.
        let mut delta = if self.full_turn_delta(from, to).is_some() {
            0.0
        } else if let Some(raw) = (to - from).is_finite().then_some(to - from) {
            raw % self.period
        } else {
            self.canonicalize(to)?.get() - self.canonicalize(from)?.get()
        };
        if delta.abs() == self.half_period
            || self
                .conventional_integer_period()
                .is_some_and(|period| delta.abs() == period / 2.0)
        {
            return Ok(EdgeDirection::HalfTurn(if negative_direction {
                TurnDirection::Negative
            } else {
                TurnDirection::Positive
            }));
        } else if delta > self.half_period {
            delta -= self.period;
        } else if delta < -self.half_period {
            delta += self.period;
        }
        if delta == 0.0 {
            delta = 0.0;
        }
        Ok(EdgeDirection::Minor(delta))
    }

    /// Return the signed number of canonical seam transits made by the
    /// shortest directed edge. The endpoints are admitted once here, and the
    /// direction comes from [`Self::shortest_edge`], including its exact
    /// source-directed half-turn tie.
    #[expect(
        clippy::float_cmp,
        reason = "exact zero is the only directionless shortest edge"
    )]
    pub(crate) fn seam_transit(self, from: f64, to: f64) -> Result<i8, GeographicError> {
        let canonical_from = self.canonicalize(from)?.get();
        let canonical_to = self.canonicalize(to)?.get();
        let direction = match self.shortest_edge(from, to)? {
            EdgeDirection::Minor(delta) if delta > 0.0 => Some(TurnDirection::Positive),
            EdgeDirection::Minor(delta) if delta < 0.0 => Some(TurnDirection::Negative),
            EdgeDirection::HalfTurn(direction) => Some(direction),
            EdgeDirection::Minor(_) => None,
        };
        Ok(match direction {
            Some(TurnDirection::Positive) if canonical_to < canonical_from => 1,
            Some(TurnDirection::Negative) if canonical_to > canonical_from => -1,
            _ => 0,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct GeographicFrame {
    longitude: PeriodicAxis,
    latitude_unit: AngularFactor,
    pole: f64,
    prime_meridian_radians: f64,
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
        }
    }

    pub(crate) fn try_new(
        longitude_unit: AngularFactor,
        latitude_unit: AngularFactor,
        prime_meridian_radians: f64,
    ) -> Result<Self, GeographicError> {
        if !prime_meridian_radians.is_finite() {
            return Err(GeographicError::NonFinite {
                name: "prime meridian",
                value: prime_meridian_radians,
            });
        }
        let pole = std::f64::consts::FRAC_PI_2 / latitude_unit.0;
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

    /// Convert one admitted native-frame point to the canonical
    /// degree/Greenwich lanes consumed by geodesic and in-core kernels.
    pub(crate) fn to_geodesic_point(self, point: Point) -> Result<Point, GeographicError> {
        let (longitude, latitude) = self.to_geodesic_xy(point.x, point.y)?;
        Ok(point.with_xy_raw(longitude, latitude))
    }

    /// Convert a finite degree/Greenwich result back to this CRS's native
    /// angular axes, retaining any Z/M ordinates.
    pub(crate) fn from_geodesic_point(self, point: Point) -> Result<Point, GeographicError> {
        let (longitude, latitude) = self.from_geodesic_xy(point.x, point.y)?;
        Ok(point.with_xy_raw(longitude, latitude))
    }

    /// Convert mutable XY lanes to canonical Greenwich degrees. Z/T lanes are
    /// deliberately outside this owner and remain untouched by callers.
    pub(crate) fn to_geodesic_lanes(
        self,
        xs: &mut [f64],
        ys: &mut [f64],
    ) -> Result<(), GeographicError> {
        for (longitude, latitude) in std::iter::zip(xs, ys) {
            (*longitude, *latitude) = self.to_geodesic_xy(*longitude, *latitude)?;
        }
        Ok(())
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
    pub(crate) fn from_geodesic_lanes(
        self,
        xs: &mut [f64],
        ys: &mut [f64],
    ) -> Result<(), GeographicError> {
        for (longitude, latitude) in std::iter::zip(xs, ys) {
            (*longitude, *latitude) = self.from_geodesic_xy(*longitude, *latitude)?;
        }
        Ok(())
    }

    #[expect(
        clippy::wrong_self_convention,
        reason = "the frame value selects the native angular axes for this conversion"
    )]
    pub(crate) fn from_geodesic_lanes_indexed(
        self,
        xs: &mut [f64],
        ys: &mut [f64],
    ) -> Result<(), (usize, GeographicError)> {
        for (index, (longitude, latitude)) in std::iter::zip(xs, ys).enumerate() {
            (*longitude, *latitude) = self
                .from_geodesic_xy(*longitude, *latitude)
                .map_err(|error| (index, error))?;
        }
        Ok(())
    }

    #[expect(
        clippy::float_cmp,
        reason = "signed zero and the exact admitted pole are frame identities"
    )]
    fn to_geodesic_xy(self, longitude: f64, latitude: f64) -> Result<(f64, f64), GeographicError> {
        let longitude = self.longitude.canonicalize(longitude)?.get();
        let longitude = if self == Self::crs84() {
            longitude
        } else {
            longitude
                .mul_add(
                    self.longitude.unit().radians_per_unit(),
                    self.prime_meridian_radians,
                )
                .to_degrees()
        };
        let longitude = Self::crs84().longitude().canonicalize(longitude)?.get();
        let admitted_latitude = Latitude::try_new(self, latitude)?.get();
        let latitude = if self == Self::crs84() {
            // The canonical CRS84 lane is already in kernel degrees. Keep
            // its signed zero while still applying the admission check.
            if latitude == 0.0 {
                latitude
            } else {
                admitted_latitude
            }
        } else {
            (admitted_latitude * self.latitude_unit.radians_per_unit()).to_degrees()
        };
        Ok((longitude, latitude))
    }

    fn from_geodesic_xy(
        self,
        longitude: f64,
        latitude: f64,
    ) -> Result<(f64, f64), GeographicError> {
        if !longitude.is_finite() || !latitude.is_finite() {
            return Err(GeographicError::NonFinite {
                name: "geodesic coordinate",
                value: if !longitude.is_finite() {
                    longitude
                } else {
                    latitude
                },
            });
        }
        let longitude = Self::crs84().longitude().canonicalize(longitude)?.get();
        let longitude = if self == Self::crs84() {
            longitude
        } else {
            let longitude = (longitude.to_radians() - self.prime_meridian_radians)
                / self.longitude.unit().radians_per_unit();
            self.longitude.canonicalize(longitude)?.get()
        };
        let latitude = if self == Self::crs84() {
            latitude
        } else {
            latitude.to_radians() / self.latitude_unit.radians_per_unit()
        };
        if !longitude.is_finite() || !latitude.is_finite() {
            return Err(GeographicError::NonFinite {
                name: "native coordinate",
                value: if !longitude.is_finite() {
                    longitude
                } else {
                    latitude
                },
            });
        }
        Ok((longitude, latitude))
    }
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
    #[expect(
        clippy::float_cmp,
        reason = "admission matches exactly the measured adjacent binary64 values"
    )]
    pub(crate) fn try_new(frame: GeographicFrame, value: f64) -> Result<Self, GeographicError> {
        let pole = frame.pole();
        if !value.is_finite() {
            return Err(GeographicError::NonFinite {
                name: "latitude",
                value,
            });
        }
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
        if value < -pole || value > pole {
            return Err(GeographicError::LatitudeOutOfRange { value });
        }
        let value = if value == 0.0 { 0.0 } else { value };
        Ok(Self(value))
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
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SeamIdentity {
    Ordinary,
    Seam,
    Pole,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TurnDirection {
    Negative,
    Positive,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RingTurn {
    None,
    Full(TurnDirection),
    Other,
}
