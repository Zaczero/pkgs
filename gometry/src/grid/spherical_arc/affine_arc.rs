use super::trig::sin_cos;
use super::{
    AffineEndpointIdentity, AffineParentMap, AffineSourceArc, AffineStructure, AxisLatitude,
    AxisMeridian, Ball, Bound, LongitudeSpan, OrderedMinorArc, PhysicalEndpointKey,
    SourceEndpointRole, Vec3Ball, Vec3Bound,
};

impl Vec3Ball {
    pub(super) fn dot(self, vector: Vec3Bound) -> Option<Ball> {
        self.x
            .mul(Ball::from_bound(vector.x)?)?
            .add(self.y.mul(Ball::from_bound(vector.y)?)?)?
            .add(self.z.mul(Ball::from_bound(vector.z)?)?)
    }

    pub(super) fn scale_sign(self, sign: i8) -> Option<Self> {
        match sign {
            1 => Some(self),
            -1 => Some(Self {
                x: self.x.neg()?,
                y: self.y.neg()?,
                z: self.z.neg()?,
            }),
            _ => None,
        }
    }
}

impl AffineSourceArc {
    pub(in crate::grid) fn from_parent_map(
        map: &AffineParentMap,
        domain: Bound,
        endpoint_roles: [SourceEndpointRole; 2],
        parent_poles: [Option<PhysicalEndpointKey>; 2],
        endpoint_identities: [AffineEndpointIdentity; 2],
    ) -> Option<Self> {
        let domain = domain.intersection(Bound::new(0.0, 1.0)?)?;
        match map.structure {
            AffineStructure::Point if !map.dlambda.is_exact_zero() || !map.dphi.is_exact_zero() => {
                return None;
            },
            AffineStructure::ConstantLongitude { .. } if !map.dlambda.is_exact_zero() => {
                return None;
            },
            AffineStructure::Equator if !map.phi0.is_exact_zero() || !map.dphi.is_exact_zero() => {
                return None;
            },
            AffineStructure::General
            | AffineStructure::Point
            | AffineStructure::ConstantLongitude { .. }
            | AffineStructure::Equator => {},
        }
        for (role, pole) in endpoint_roles.into_iter().zip(parent_poles) {
            if matches!(role, SourceEndpointRole::Partition) && pole.is_some() {
                return None;
            }
        }
        Some(Self {
            map: *map,
            domain,
            endpoint_roles,
            endpoint_poles: parent_poles,
            endpoint_identities,
        })
    }

    pub(super) const fn is_zero_length(self) -> bool {
        matches!(self.map.structure, AffineStructure::Point)
    }

    pub(super) const fn is_vertical(self) -> bool {
        matches!(
            self.map.structure,
            AffineStructure::ConstantLongitude { .. }
        )
    }

    #[cfg(test)]
    fn is_axis(self, axis: AxisMeridian) -> bool {
        matches!(self.map.structure, AffineStructure::ConstantLongitude { axis: Some(value) } if value == axis)
    }

    pub(super) fn lambda_at(self, t: Bound) -> Option<Bound> {
        self.map.lambda0.add(self.map.dlambda.mul(t)?)
    }

    fn phi_at(self, t: Bound) -> Option<Bound> {
        self.map.phi0.add(self.map.dphi.mul(t)?)
    }

    pub(super) fn longitude_span(self) -> Option<LongitudeSpan> {
        if self.is_vertical() {
            return Some(LongitudeSpan {
                lower: self.map.lambda0,
                upper: self.map.lambda0,
            });
        }
        let lower = self.lambda_at(Bound::exact(self.domain.lo)?)?;
        let upper = self.lambda_at(Bound::exact(self.domain.hi)?)?;
        LongitudeSpan::ordered(lower, upper)
    }

    pub(super) fn latitude_span(self) -> Option<LongitudeSpan> {
        let lower = self.phi_at(Bound::exact(self.domain.lo)?)?;
        let upper = self.phi_at(Bound::exact(self.domain.hi)?)?;
        LongitudeSpan::ordered(lower, upper)
    }

    /// Whether this piece can reach the closed latitude band, in degrees.
    /// A piece whose certified latitudes miss the band cannot meet an arc
    /// that lies inside it.
    pub(in crate::grid) fn may_reach_latitudes(self, band: Bound) -> bool {
        let Some(span) = self.latitude_span() else {
            return true;
        };
        // Outward conversion: to_radians rounds once.
        let south = band.lo.to_radians().next_down().next_down();
        let north = band.hi.to_radians().next_up().next_up();
        !(span.upper.hi < south || span.lower.lo > north)
    }

    pub(super) fn parameter_at_longitude(self, longitude: Bound) -> Option<Bound> {
        longitude.sub(self.map.lambda0)?.div(self.map.dlambda)
    }

    pub(super) fn parameter_at_latitude(self, latitude: Bound) -> Option<Bound> {
        latitude.sub(self.map.phi0)?.div(self.map.dphi)
    }

    pub(super) fn endpoint_pole(self, endpoint: usize) -> Option<PhysicalEndpointKey> {
        match self.endpoint_roles.get(endpoint)? {
            SourceEndpointRole::ParentStart | SourceEndpointRole::ParentEnd => {
                self.endpoint_poles.get(endpoint).copied().flatten()
            },
            SourceEndpointRole::Partition => None,
        }
    }

    pub(super) fn endpoint_identity_at(self, t: Bound) -> Option<AffineEndpointIdentity> {
        let start = Bound::exact(self.domain.lo)?;
        if t == start {
            return Some(self.endpoint_identities[0]);
        }
        let end = Bound::exact(self.domain.hi)?;
        if t == end {
            return Some(self.endpoint_identities[1]);
        }
        None
    }

    pub(super) fn vector_at(self, t: Bound) -> Option<Vec3Bound> {
        let lambda = self.lambda_at(t)?;
        let phi = self.phi_at(t)?;
        let (sin_lambda, cos_lambda) = self.sin_cos_lambda_at(t, lambda)?;
        let (sin_phi, cos_phi) = self.sin_cos_phi_at(t, phi)?;
        let sin_lambda = sin_lambda.bound()?;
        let cos_lambda = cos_lambda.bound()?;
        let sin_phi = sin_phi.bound()?;
        let cos_phi = cos_phi.bound()?;
        Some(Vec3Bound {
            x: cos_phi.mul(cos_lambda)?,
            y: cos_phi.mul(sin_lambda)?,
            z: sin_phi,
        })
    }

    pub(super) fn plane_value(self, arc: &OrderedMinorArc, t: Bound) -> Option<Bound> {
        arc.normal.dot(self.vector_at(t)?)
    }

    pub(super) fn tangent_at(self, endpoint: usize) -> Option<Vec3Ball> {
        let t = Bound::exact(if endpoint == 0 {
            self.domain.lo
        } else {
            self.domain.hi
        })?;
        let away_sign = if endpoint == 0 { 1 } else { -1 };
        let lambda = self.lambda_at(t)?;
        let dphi = Ball::from_bound(self.map.dphi)?;
        let (sin_lambda, cos_lambda) = self.sin_cos_lambda_at(t, lambda)?;
        if let Some(pole) = self.endpoint_pole(endpoint) {
            let sign = match pole {
                PhysicalEndpointKey::NorthPole => -1,
                PhysicalEndpointKey::SouthPole => 1,
                PhysicalEndpointKey::CellVertex(_) => return None,
            };
            return Vec3Ball {
                x: dphi.mul(cos_lambda)?.scale_sign(sign)?,
                y: dphi.mul(sin_lambda)?.scale_sign(sign)?,
                z: Ball::exact(0.0)?,
            }
            .scale_sign(away_sign);
        }
        self.tangent_at_parameter(t, away_sign)
    }

    fn tangent_at_parameter(self, t: Bound, away_sign: i8) -> Option<Vec3Ball> {
        let dlambda = Ball::from_bound(self.map.dlambda)?;
        let dphi = Ball::from_bound(self.map.dphi)?;
        let phi = self.phi_at(t)?;
        let (sin_phi, cos_phi) = self.sin_cos_phi_at(t, phi)?;
        let lambda = self.lambda_at(t)?;
        let (sin_lambda, cos_lambda) = self.sin_cos_lambda_at(t, lambda)?;
        let x = dlambda
            .mul(cos_phi.mul(sin_lambda)?.neg()?)?
            .add(dphi.mul(sin_phi.mul(cos_lambda)?.neg()?)?)?;
        let y = dlambda
            .mul(cos_phi.mul(cos_lambda)?)?
            .add(dphi.mul(sin_phi.mul(sin_lambda)?.neg()?)?)?;
        let z = dphi.mul(cos_phi)?;
        Vec3Ball { x, y, z }.scale_sign(away_sign)
    }

    pub(super) fn plane_derivative(self, arc: &OrderedMinorArc, lambda: Bound) -> Option<Bound> {
        let (sin_lambda, cos_lambda) = sin_cos(lambda)?;
        let sin_lambda = sin_lambda.bound()?;
        let cos_lambda = cos_lambda.bound()?;
        let a = arc
            .normal
            .x
            .mul(cos_lambda)?
            .add(arc.normal.y.mul(sin_lambda)?)?;
        let w = arc
            .normal
            .x
            .mul(sin_lambda)?
            .sub(arc.normal.y.mul(cos_lambda)?)?;
        let nz_squared = arc.normal.z.mul(arc.normal.z)?;
        self.map
            .dphi
            .mul(nz_squared.add(a.mul(a)?)?)?
            .sub(self.map.dlambda.mul(arc.normal.z.mul(w)?)?)
    }

    pub(super) fn derivative_lipschitz(self, arc: &OrderedMinorArc) -> Option<Bound> {
        let radial = arc.normal.x.abs().add(arc.normal.y.abs())?;
        let twice_dphi_radial = Bound::exact(2.0)?.mul(self.map.dphi.abs())?.mul(radial)?;
        let dlambda_nz = self.map.dlambda.abs().mul(arc.normal.z.abs())?;
        radial.mul(twice_dphi_radial.add(dlambda_nz)?)
    }

    pub(super) fn vertical_derivative(self, arc: &OrderedMinorArc, phi: Bound) -> Option<Bound> {
        // `is_vertical()` is an exact stored-degree structural fact.  Use the
        // local piece's literal longitude rather than widening it over a
        // parent domain interval.
        let (sin_lambda, cos_lambda) = self.sin_cos_lambda(self.map.lambda0)?;
        let a = arc
            .normal
            .x
            .mul(cos_lambda.bound()?)?
            .add(arc.normal.y.mul(sin_lambda.bound()?)?)?;
        let (sin_phi, cos_phi) = sin_cos(phi)?;
        a.mul(sin_phi.bound()?)?
            .neg()
            .add(arc.normal.z.mul(cos_phi.bound()?)?)
    }

    fn sin_cos_lambda(self, lambda: Bound) -> Option<(Ball, Ball)> {
        if let AffineStructure::ConstantLongitude { axis: Some(axis) } = self.map.structure {
            return axis_sin_cos(axis);
        }
        sin_cos(lambda)
    }

    fn sin_cos_lambda_at(self, t: Bound, lambda: Bound) -> Option<(Ball, Ball)> {
        if let Some(axis) = self
            .endpoint_identity_at(t)
            .and_then(|identity| identity.longitude)
        {
            return axis_sin_cos(axis);
        }
        self.sin_cos_lambda(lambda)
    }

    fn sin_cos_phi_at(self, t: Bound, phi: Bound) -> Option<(Ball, Ball)> {
        if matches!(
            self.endpoint_identity_at(t)
                .and_then(|identity| identity.latitude),
            Some(AxisLatitude::Equator)
        ) {
            return Some((Ball::exact(0.0)?, Ball::exact(1.0)?));
        }
        sin_cos(phi)
    }
}
/// Exact source-axis identities are structural facts from the stored degree
/// endpoint, not a choice made by reducing an interval around pi/2 or pi.
fn axis_sin_cos(axis: AxisMeridian) -> Option<(Ball, Ball)> {
    let (sin, cos) = match axis {
        AxisMeridian::Prime => (0.0, 1.0),
        AxisMeridian::EastQuarter => (1.0, 0.0),
        AxisMeridian::Antimeridian => (0.0, -1.0),
        AxisMeridian::WestQuarter => (-1.0, 0.0),
    };
    Some((Ball::exact(sin)?, Ball::exact(cos)?))
}

#[cfg(test)]
mod tests;
