use super::GeometryKind;
use crate::geometry::source_support::PointMembership;

/// Historical amortize threshold for prepared/array batch entry points.
///
/// Answer-producing algorithm selection must NOT depend on this constant:
/// `scalar_vs_shapes` uses one pipeline for every batch length. Call sites may
/// still use it only as a layout/scheduling hint (e.g. detach vs in-GIL), never
/// to pick a different predicate kernel.
/// A named planar topological predicate with one canonical public token.
///
/// `dwithin` is not listed: it is a metric test that needs a distance model,
/// not a DE-9IM relation (the spatial index handles it separately).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Predicate {
    Intersects,
    Disjoint,
    Contains,
    ContainsProperly,
    Within,
    Covers,
    CoveredBy,
    Touches,
    Crosses,
    Overlaps,
    Equals,
}

/// How the R-tree narrows candidates for `query OP item`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IndexEnvelope {
    /// Candidates whose envelope intersects the query envelope.
    Intersecting,
    /// Use the query's whole outer envelope for containment predicates. The
    /// candidate walk is still intersecting and exact refinement owns
    /// containment; a raw item envelope is not a support certificate.
    ContainedInQuery,
}

/// All dispatch facts for one predicate, read from [`Predicate::spec`].
#[derive(Clone, Copy)]
pub(crate) struct PredicateSpec {
    /// The predicate these facts describe.
    pub predicate: Predicate,
    /// The public token (`'intersects'`, `'covers'`, …).
    pub token: &'static str,
    /// Whether `p(a, b) == p(b, a)`.
    pub symmetric: bool,
    /// R-tree candidate strategy, or `None` when the predicate cannot be
    /// index-accelerated (`disjoint` would enumerate the complement).
    pub index_envelope: Option<IndexEnvelope>,
}

/// Declared-kind class for the kind×kind constant gate (not topological dim).
#[derive(Clone, Copy, PartialEq, Eq)]
enum KindClass {
    /// `Shape::Point` / empty point — never MultiPoint.
    Point,
    MultiPoint,
    Curve,
    Surface,
    Collection,
}

const fn kind_class(kind: GeometryKind) -> KindClass {
    match kind {
        GeometryKind::Point => KindClass::Point,
        GeometryKind::MultiPoint => KindClass::MultiPoint,
        GeometryKind::LineString | GeometryKind::MultiLineString => KindClass::Curve,
        GeometryKind::Polygon | GeometryKind::MultiPolygon => KindClass::Surface,
        GeometryKind::GeometryCollection => KindClass::Collection,
    }
}

impl Predicate {
    pub(crate) const fn point_membership(self, point_is_left: bool) -> Option<PointMembership> {
        match (self, point_is_left) {
            (Self::Contains | Self::ContainsProperly, false) | (Self::Within, true) => {
                Some(PointMembership::Strict)
            },
            (Self::Covers, false) | (Self::CoveredBy, true) => {
                Some(PointMembership::BoundaryInclusive)
            },
            _ => None,
        }
    }

    /// Every predicate, in public-documentation order.
    pub(crate) const ALL: [Self; 11] = [
        Self::Intersects,
        Self::Disjoint,
        Self::Contains,
        Self::ContainsProperly,
        Self::Within,
        Self::Covers,
        Self::CoveredBy,
        Self::Touches,
        Self::Crosses,
        Self::Overlaps,
        Self::Equals,
    ];

    /// Parse a public token; `None` for unknown values (callers shape the
    /// error).
    pub(crate) fn parse(token: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|predicate| predicate.spec().token == token)
    }

    /// The canonical token (inverse of [`parse`](Self::parse)).
    pub(crate) const fn token(self) -> &'static str {
        self.spec().token
    }

    /// The dispatch facts for this predicate — the only place they live.
    pub(crate) const fn spec(self) -> PredicateSpec {
        match self {
            Self::Intersects => {
                self.symmetric_spec("intersects", Some(IndexEnvelope::Intersecting))
            },
            Self::Disjoint => self.symmetric_spec("disjoint", None),
            Self::Contains => self.directed_spec("contains"),
            Self::ContainsProperly => self.directed_spec("contains_properly"),
            Self::Within => self.directed_spec("within"),
            Self::Covers => self.directed_spec("covers"),
            Self::CoveredBy => self.directed_spec("covered_by"),
            Self::Touches => self.symmetric_spec("touches", Some(IndexEnvelope::Intersecting)),
            Self::Crosses => self.symmetric_spec("crosses", Some(IndexEnvelope::Intersecting)),
            Self::Overlaps => self.symmetric_spec("overlaps", Some(IndexEnvelope::Intersecting)),
            Self::Equals => self.symmetric_spec("equals", Some(IndexEnvelope::Intersecting)),
        }
    }

    /// Structural refutations that do not depend on a carrier's effective
    /// support. A single Point cannot cross or overlap another carrier;
    /// declared polygon and line kinds are not support-dimension proofs.
    pub(crate) const fn kind_gate(
        self,
        left_kind: GeometryKind,
        right_kind: GeometryKind,
    ) -> Option<bool> {
        let left = kind_class(left_kind);
        let right = kind_class(right_kind);
        if matches!(left, KindClass::Collection) || matches!(right, KindClass::Collection) {
            return None;
        }
        match self {
            Self::Crosses => {
                // A single Point has no crossing locus in either order.
                if matches!(left, KindClass::Point) || matches!(right, KindClass::Point) {
                    return Some(false);
                }
                None
            },
            Self::Overlaps => {
                // A single Point never overlaps another carrier. Other
                // declarations do not prove their effective support
                // dimension: polygons can collapse and lines can be point-only.
                if matches!(left, KindClass::Point) || matches!(right, KindClass::Point) {
                    return Some(false);
                }
                None
            },
            // Every other predicate uses the complete relation matrix.
            Self::Equals
            | Self::Intersects
            | Self::Disjoint
            | Self::Contains
            | Self::ContainsProperly
            | Self::Within
            | Self::Covers
            | Self::CoveredBy
            | Self::Touches => None,
        }
    }

    const fn symmetric_spec(
        self,
        token: &'static str,
        index_envelope: Option<IndexEnvelope>,
    ) -> PredicateSpec {
        PredicateSpec {
            predicate: self,
            token,
            symmetric: true,
            index_envelope,
        }
    }

    const fn directed_spec(self, token: &'static str) -> PredicateSpec {
        // Directed containment predicates use the query's whole outer window;
        // candidate traversal remains intersecting and exact refinement decides
        // whether the effective support is contained.
        let index_envelope = Some(match self {
            Self::Contains | Self::ContainsProperly | Self::Covers => {
                IndexEnvelope::ContainedInQuery
            },
            Self::Within
            | Self::CoveredBy
            | Self::Intersects
            | Self::Disjoint
            | Self::Touches
            | Self::Crosses
            | Self::Overlaps
            | Self::Equals => IndexEnvelope::Intersecting,
        });
        PredicateSpec {
            predicate: self,
            token,
            symmetric: false,
            index_envelope,
        }
    }
}

/// A named predicate read off a fully decided matrix.
pub(crate) fn predicate_matrix_value(spec: &PredicateSpec, matrix: crate::geometry::De9im) -> bool {
    match spec.predicate {
        Predicate::Intersects => matrix.is_intersects(),
        Predicate::Disjoint => matrix.is_disjoint(),
        Predicate::Contains => matrix.is_contains(),
        Predicate::Within => matrix.transpose().is_contains(),
        Predicate::Covers => matrix.is_covers(),
        Predicate::CoveredBy => matrix.transpose().is_covers(),
        Predicate::Equals => matrix.is_equal_topo(),
        Predicate::Touches => matrix.is_touches(),
        Predicate::Crosses => matrix.is_crosses_by_dimension(),
        Predicate::Overlaps => matrix.is_overlaps_by_dimension(),
        Predicate::ContainsProperly => matrix.is_contains_properly(),
    }
}
