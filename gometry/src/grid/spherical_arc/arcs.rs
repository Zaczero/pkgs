use super::vectors::certified_vector_latitude;
#[cfg(test)]
use super::vectors::directed_arc_longitude;
use super::{
    Bound, CellVertexKey, DirectedLongitudeSpan, ExactArcKind, HALF_PI_HI, LongitudeSpan,
    OrderedMinorArc, PI_HI, PI_LO, PhysicalEndpointKey, Vec3Bound,
};

#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) enum LongitudeOverlap {
    Disjoint,
    Contact { outer: Bound, inner: Option<Bound> },
}
impl LongitudeSpan {
    pub(super) fn ordered(first: Bound, second: Bound) -> Option<Self> {
        if first == second {
            return Some(Self {
                lower: first,
                upper: second,
            });
        }
        if first.hi < second.lo {
            return Some(Self {
                lower: first,
                upper: second,
            });
        }
        if second.hi < first.lo {
            return Some(Self {
                lower: second,
                upper: first,
            });
        }
        None
    }

    pub(super) fn overlap(self, rhs: Self) -> LongitudeOverlap {
        let outer_lo = self.lower.lo.max(rhs.lower.lo);
        let outer_hi = self.upper.hi.min(rhs.upper.hi);
        let Some(outer) = Bound::new(outer_lo, outer_hi) else {
            return LongitudeOverlap::Disjoint;
        };
        let inner_lo = self.lower.hi.max(rhs.lower.hi);
        let inner_hi = self.upper.lo.min(rhs.upper.lo);
        let inner = Bound::new(inner_lo, inner_hi).filter(|value| value.lo < value.hi);
        LongitudeOverlap::Contact { outer, inner }
    }

    pub(super) fn strictly_contains(self, point: Bound) -> bool {
        self.lower.hi < point.lo && point.hi < self.upper.lo
    }
}
impl DirectedLongitudeSpan {
    pub(super) fn envelope(self) -> Option<LongitudeSpan> {
        LongitudeSpan::ordered(self.start, self.end)
    }

    fn shifted(self, turns: i64) -> Option<Self> {
        let tau = Bound::new(2.0 * PI_LO, 2.0 * PI_HI)?;
        // This multiplication proposes a periodic copy only. The enclosing
        // interval is widened and every later contact test remains certified,
        // so a non-exact binary64 representation of a large integer cannot
        // establish a negative.
        let offset = tau.mul(Bound::exact(turns as f64)?)?;
        Some(Self {
            start: self.start.add(offset)?,
            end: self.end.add(offset)?,
            turn: self.turn.checked_add(turns)?,
        })
    }
}
/// Position against the closed ordered minor arc.  Boundary and uncertainty
/// are distinct internally, but neither may establish a negative.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ArcPosition {
    Outside,
    Open,
    Boundary,
    Uncertain,
}
impl OrderedMinorArc {
    #[cfg(test)]
    pub(super) fn from_h3_points(
        cell: h3o::CellIndex,
        start_ordinal: u8,
        end_ordinal: u8,
        start: Vec3Bound,
        end: Vec3Bound,
    ) -> Option<Self> {
        let normal = start.cross(end)?;
        let normal_norm = normal.dot(normal)?.sqrt()?;
        if normal_norm.contains_zero() {
            return None;
        }
        // `dot > -1` is the strict non-antipodal minor-order precondition.
        if start.dot(end)?.lo <= -1.0 {
            return None;
        }
        // H3 supplies raw vectors but no exact equator/axis tag.  A
        // zero-containing component is only a possibility, never structural
        // authority for a negative-producing special branch.
        let kind = if normal.z.strictly_positive() || normal.z.strictly_negative() {
            ExactArcKind::General
        } else {
            ExactArcKind::UnresolvedMeridian
        };
        let longitude = if kind == ExactArcKind::General {
            Some(directed_arc_longitude(start, end)?)
        } else {
            None
        };
        let vertex_keys = [
            CellVertexKey::H3 {
                cell,
                ordinal: start_ordinal,
            },
            CellVertexKey::H3 {
                cell,
                ordinal: end_ordinal,
            },
        ];
        Some(Self::from_keyed_points(
            cell,
            vertex_keys,
            vertex_keys.map(PhysicalEndpointKey::CellVertex),
            start,
            end,
            kind,
            normal,
            longitude,
        ))
    }

    pub(super) const fn from_keyed_points(
        supplier: h3o::CellIndex,
        vertex_keys: [CellVertexKey; 2],
        endpoints: [PhysicalEndpointKey; 2],
        start: Vec3Bound,
        end: Vec3Bound,
        kind: ExactArcKind,
        normal: Vec3Bound,
        longitude: Option<DirectedLongitudeSpan>,
    ) -> Self {
        Self {
            supplier,
            vertex_keys,
            endpoints,
            start,
            end,
            normal,
            longitude,
            kind,
        }
    }

    #[cfg(test)]
    pub(super) fn from_test_points(start: [f64; 3], end: [f64; 3]) -> Self {
        Self::from_h3_points(
            h3o::CellIndex::try_from(0x0800_1FFF_FFFF_FFFF_u64).expect("known H3 cell"),
            0,
            1,
            Vec3Bound::exact(start).expect("finite test ray"),
            Vec3Bound::exact(end).expect("finite test ray"),
        )
        .expect("valid test minor arc")
    }

    pub(super) fn position(self, point: Vec3Bound) -> ArcPosition {
        if self.kind == ExactArcKind::UnresolvedMeridian {
            return ArcPosition::Uncertain;
        }
        let Some(start_cross) = self.start.cross(point) else {
            return ArcPosition::Uncertain;
        };
        let Some(end_cross) = point.cross(self.end) else {
            return ArcPosition::Uncertain;
        };
        let Some(left) = self.normal.dot(start_cross) else {
            return ArcPosition::Uncertain;
        };
        let Some(right) = self.normal.dot(end_cross) else {
            return ArcPosition::Uncertain;
        };
        if left.strictly_negative() || right.strictly_negative() {
            return ArcPosition::Outside;
        }
        if left.strictly_positive() && right.strictly_positive() {
            ArcPosition::Open
        } else if !left.contains_zero() && !right.contains_zero() {
            ArcPosition::Boundary
        } else {
            ArcPosition::Uncertain
        }
    }

    pub(super) fn with_longitude_shift(mut self, turns: i64) -> Option<Self> {
        self.longitude = Some(self.longitude?.shifted(turns)?);
        Some(self)
    }
}
/// Outward latitude bounds for one ordered minor great-circle piece.  The
/// extrema are the two projections of the north axis onto the arc plane; a
/// candidate may be used only after the ordered-arc certificate places it on
/// the piece.  Ambiguity widens to the world latitude interval.
pub(super) fn certified_arc_latitude_bounds_from_endpoints(
    arc: &OrderedMinorArc,
    start_latitude: Bound,
    endpoint_latitude: Bound,
) -> Option<Bound> {
    let mut latitude = Bound::new(start_latitude.lo, start_latitude.hi)?;
    latitude = Bound::new(
        latitude.lo.min(endpoint_latitude.lo),
        latitude.hi.max(endpoint_latitude.hi),
    )?;
    let normal_squared = arc.normal.dot(arc.normal)?;
    if normal_squared.contains_zero() {
        return Bound::new(-HALF_PI_HI, HALF_PI_HI);
    }
    let north = Vec3Bound::exact([0.0, 0.0, 1.0])?;
    let projection = arc
        .normal
        .scale(north.dot(arc.normal)?.div(normal_squared)?)?;
    let candidate = north.sub(projection)?.normalized()?;
    for candidate in [candidate, candidate.neg()] {
        match arc.position(candidate) {
            ArcPosition::Outside => {},
            ArcPosition::Open | ArcPosition::Boundary => {
                let candidate_latitude = certified_vector_latitude(candidate)?;
                latitude = Bound::new(
                    latitude.lo.min(candidate_latitude.lo),
                    latitude.hi.max(candidate_latitude.hi),
                )?;
            },
            ArcPosition::Uncertain => return Bound::new(-HALF_PI_HI, HALF_PI_HI),
        }
    }
    Some(latitude)
}
#[cfg(test)]
pub(super) fn certified_arc_latitude_bounds(arc: &OrderedMinorArc) -> Option<Bound> {
    certified_arc_latitude_bounds_from_endpoints(
        arc,
        certified_vector_latitude(arc.start)?,
        certified_vector_latitude(arc.end)?,
    )
}

#[cfg(test)]
mod tests;
