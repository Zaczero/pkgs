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

    /// Whether a newly-created coordinate edge would be read across the
    /// periodic seam. A full-turn edge retains its existing interpretation.
    pub(crate) fn edge_exceeds_half_period(self, from: f64, to: f64) -> bool {
        // Canonical interior longitudes need only one subtraction; aliases
        // use the same seam spelling rule as geographic topology.
        if from.abs() < self.half_period && to.abs() < self.half_period {
            return (to - from).abs() > self.half_period;
        }
        let raw_delta = (to - from).abs();
        if self.full_turn_delta(from, to).is_some() {
            return false;
        }
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
        let delta = to - from;
        if !delta.is_finite() {
            return None;
        }
        let magnitude = delta.abs();
        let is_full_turn = magnitude.to_bits() == self.period.to_bits()
            || self
                .conventional_integer_period()
                .is_some_and(|period| magnitude.to_bits() == period.to_bits());
        if !is_full_turn {
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
        Some(delta.signum() * self.period)
    }

    fn conventional_integer_period(self) -> Option<f64> {
        let period = self.period.round();
        (period > 0.0 && (self.period - period).abs() <= 8.0 * f64::EPSILON * period.abs())
            .then_some(period)
    }

    pub(crate) fn canonicalize(self, value: f64) -> Result<CanonicalLongitude, GeographicError> {
        if !value.is_finite() {
            return Err(GeographicError::NonFinite {
                name: "longitude",
                value,
            });
        }
        Ok(self.wrap_admitted(value))
    }

    /// Choose the longitude alias nearest a position already lifted in this
    /// axis. Reduce the displacement before combining it with the reference,
    /// so adding several periods cannot erase the longitude's low bits.
    #[expect(
        clippy::float_cmp,
        reason = "exact zero, whole-period aliases, and half-period ties define longitude spelling"
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
        if difference.is_finite() && difference.abs() <= self.half_period {
            return Ok(if longitude == 0.0 { 0.0 } else { longitude });
        }

        let delta = if difference.is_finite()
            && difference.fract() == 0.0
            && let Some(period) = self.conventional_integer_period()
        {
            let remainder = difference % period;
            if remainder == 0.0 {
                return Ok(if lifted_reference == 0.0 {
                    0.0
                } else {
                    lifted_reference
                });
            }
            if remainder.abs() == period / 2.0 {
                (period / 2.0).copysign(difference)
            } else {
                self.wrap_admitted(difference).get()
            }
        } else {
            self.shortest_edge(longitude, lifted_reference)?.delta(self)
        };
        let alias = lifted_reference - delta;
        if alias.is_finite() {
            Ok(if alias == 0.0 { 0.0 } else { alias })
        } else {
            Err(GeographicError::NonFinite {
                name: "longitude",
                value: alias,
            })
        }
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
    pub(crate) fn point_from_geodesic(self, point: Point) -> Result<Point, GeographicError> {
        let (longitude, latitude) = self.xy_from_geodesic(point.x, point.y)?;
        Ok(point.with_xy_raw(longitude, latitude))
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

    pub(crate) fn to_geodesic_xy(
        self,
        longitude: f64,
        latitude: f64,
    ) -> Result<(f64, f64), GeographicError> {
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

    fn xy_from_geodesic(
        self,
        longitude: f64,
        latitude: f64,
    ) -> Result<(f64, f64), GeographicError> {
        ensure_geodesic_coordinates(longitude, latitude)?;
        let longitude = Self::crs84().longitude().canonicalize(longitude)?.get();
        let (longitude, latitude) = self.native_xy_from_geodesic(longitude, latitude);
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
            (longitude.to_radians() - self.prime_meridian_radians)
                / self.longitude.unit().radians_per_unit()
        };
        let latitude = if self == Self::crs84() {
            latitude
        } else {
            latitude.to_radians() / self.latitude_unit.radians_per_unit()
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
