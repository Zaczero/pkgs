//! The one source of truth for boolean spatial predicates.
//!
//! Every surface that evaluates a named predicate — free functions, `Geometry`
//! and `GeometryArray` methods, `PreparedGeometry`, the spatial index refine
//! loop, and `join` — reads its facts from [`Predicate::spec`] and projects the
//! complete matrix produced by the graph relation owner. Batch and scalar
//! surfaces therefore cannot drift in predicate semantics.

use crate::geometry::{Bounds, Shape, ShapeData, graph_relate_data, point_is_geographic_pole};
use crate::{GeometryKind, PyGeometryArray, ShapeRow};

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
    /// Candidates whose envelope lies inside the query envelope — the tighter
    /// lookup containment predicates allow.
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

const fn kind_class_dim(class: KindClass) -> Option<u8> {
    match class {
        KindClass::Point | KindClass::MultiPoint => Some(0),
        KindClass::Curve => Some(1),
        KindClass::Surface => Some(2),
        KindClass::Collection => None,
    }
}

impl Predicate {
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
    pub(crate) fn token(self) -> &'static str {
        self.spec().token
    }

    /// The dispatch facts for this predicate — the only place they live.
    pub(crate) fn spec(self) -> PredicateSpec {
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

    /// Compile-time kind×kind refutations for Crosses and Overlaps.
    ///
    /// Keys off **declared** [`GeometryKind`] (`Point` vs `MultiPoint`), never
    /// topological dimension alone — a MultiPoint can cross an area while a
    /// single Point never can. `GeometryCollection` on either side returns
    /// `None` (fall through). All non-refuted pairs, including empties and
    /// equality, use the matrix path.
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
                // Point × anything = F (both orders). MultiPoint is NOT Point.
                if matches!(left, KindClass::Point) || matches!(right, KindClass::Point) {
                    return Some(false);
                }
                // Area × Area = F. MultiPoint × MultiPoint = F.
                if (matches!(left, KindClass::Surface) && matches!(right, KindClass::Surface))
                    || (matches!(left, KindClass::MultiPoint)
                        && matches!(right, KindClass::MultiPoint))
                {
                    return Some(false);
                }
                // MultiPoint×{Line,Area}, Line×Line, Line×Area = REAL.
                None
            },
            Self::Overlaps => {
                // Point × anything = F (both orders).
                if matches!(left, KindClass::Point) || matches!(right, KindClass::Point) {
                    return Some(false);
                }
                // Unequal dimension = F; same-dim MultiPoint/Line/Area = REAL.
                match (kind_class_dim(left), kind_class_dim(right)) {
                    (Some(a), Some(b)) if a != b => Some(false),
                    _ => None,
                }
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

    fn symmetric_spec(
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

    fn directed_spec(self, token: &'static str) -> PredicateSpec {
        // Directed containment predicates share one envelope rule: the
        // container's envelope must contain the candidate's, so the index can
        // use the tighter `locate_in_envelope` lookup when the query is the
        // container (`contains`/`contains_properly`/`covers`), and the generic
        // intersecting lookup otherwise.
        let index_envelope = Some(match self {
            Self::Contains | Self::ContainsProperly | Self::Covers => {
                IndexEnvelope::ContainedInQuery
            },
            _ => IndexEnvelope::Intersecting,
        });
        PredicateSpec {
            predicate: self,
            token,
            symmetric: false,
            index_envelope,
        }
    }
}

/// Axis-aligned bounds gates are unsound for seam-crossing or polar operands
/// in a geographic frame, so those rows must reach the quotient-aware graph.
pub(crate) fn skip_antimeridian_bounds_gate_row(geographic: bool, row: ShapeRow<'_>) -> bool {
    geographic && row.with_shape(Shape::crosses_antimeridian)
}

impl PredicateSpec {
    /// A definite answer from operand bounds alone, before any exact topology.
    ///
    /// Strictly disjoint bounds settle every predicate (`disjoint` → `true`,
    /// everything else → `false`); containment and equality predicates also
    /// reject when the required bounds nesting fails. `None` means the exact
    /// kernel must decide.
    pub(crate) fn bounds_gate(&self, left: Bounds, right: Bounds) -> Option<bool> {
        if !left.intersects(right) {
            return Some(self.predicate == Predicate::Disjoint);
        }
        match self.predicate {
            Predicate::Contains | Predicate::ContainsProperly | Predicate::Covers
                if !left.contains(right) =>
            {
                Some(false)
            },
            Predicate::Within | Predicate::CoveredBy if !right.contains(left) => Some(false),
            // Topologically equal point sets have identical extremes.
            Predicate::Equals if left != right => Some(false),
            _ => None,
        }
    }
}

/// Evaluate `spec` between one fixed `scalar` operand and every row of
/// `elements`, building the scalar's cached/prepared state at most once.
///
/// `scalar_is_left` names the operand order: `true` evaluates
/// `scalar OP element`, `false` evaluates `element OP scalar`. This is the
/// single batch engine behind the free functions, the array methods,
/// `PreparedGeometry`, and the spatial-index refine loop. Rows come straight
/// off the storage: `Mixed` rows keep their persistent handles (prepared
/// state accumulates), packed point rows never synthesize one.
#[expect(
    clippy::too_many_lines,
    reason = "cohesive kernel; splitting obscures the algorithm"
)]
pub(crate) fn scalar_vs_shapes<'a>(
    spec: &PredicateSpec,
    scalar: &ShapeData,
    elements: impl ExactSizeIterator<Item = (usize, ShapeRow<'a>)>,
    scalar_is_left: bool,
    array: Option<&PyGeometryArray>,
    geographic: bool,
) -> Vec<bool> {
    let scalar_orig = scalar;
    let scalar_bounds = scalar.bounds();
    let element_bounds = array.and_then(PyGeometryArray::cached_element_bounds);
    let row_is_missing = |index: usize| array.is_some_and(|array| array.is_row_missing(index));
    // Declared geometry kinds provide sound refutations for predicates whose
    // OGC definition excludes the pair outright.
    let kind_reject = |row: ShapeRow<'_>| -> Option<bool> {
        let (row_kind, _) = row_kind_empty(row);
        let (left_kind, right_kind) = if scalar_is_left {
            (GeometryKind::of(scalar.shape()), row_kind)
        } else {
            (row_kind, GeometryKind::of(scalar.shape()))
        };
        spec.predicate.kind_gate(left_kind, right_kind)
    };
    // Axis-aligned bounds are unsound when either operand crosses the
    // antimeridian (including a fixed scalar against simple point rows).  A
    // physical pole has the same property: longitude is merely a spelling,
    // so its planar X cannot certify a negative against a geographic row.
    let scalar_crosses = geographic && scalar_orig.shape().crosses_antimeridian();
    let scalar_reaches_pole =
        geographic && crate::geometry::shape_reaches_geographic_pole(scalar_orig.shape());
    let scalar_is_pole = geographic
        && matches!(
            scalar_orig.shape(),
            Shape::Point(point) if point_is_geographic_pole(*point).is_some()
        );
    let bounds_reject = |index: usize, row: ShapeRow<'_>| -> Option<bool> {
        if scalar_crosses
            || scalar_reaches_pole
            || scalar_is_pole
            || skip_antimeridian_bounds_gate_row(geographic, row)
            || row.with_shape(|shape| crate::geometry::shape_reaches_geographic_pole(shape))
        {
            return None;
        }
        let row_bounds = element_bounds
            .as_ref()
            .and_then(|bounds| bounds.get(index).copied().flatten())
            .or_else(|| row.quick_bounds())?;
        let scalar_bounds = scalar_bounds?;
        if scalar_is_left {
            spec.bounds_gate(scalar_bounds, row_bounds)
        } else {
            spec.bounds_gate(row_bounds, scalar_bounds)
        }
    };
    let rectangle_intersects = |_: usize, row: ShapeRow<'_>| -> Option<bool> {
        if spec.predicate != Predicate::Intersects {
            return None;
        }
        if geographic
            && row.with_shape(|shape| {
                shape.crosses_antimeridian()
                    || crate::geometry::shape_reaches_geographic_pole(shape)
            })
        {
            return None;
        }
        if let Some(intersects) = row.with_shape(|shape| {
            crate::geometry::graph_intersects_rectangle_data(scalar_orig, shape, geographic)
        }) {
            return Some(intersects);
        }
        None
    };
    elements
        .map(|(index, row)| {
            if row_is_missing(index) {
                false
            } else {
                kind_reject(row)
                    .or_else(|| bounds_reject(index, row))
                    .or_else(|| rectangle_intersects(index, row))
                    .unwrap_or_else(|| {
                        let element = array.map_or_else(
                            || crate::array::PreparedRow::transient(row),
                            |array| array.prepared_row(index, row),
                        );
                        if scalar_is_left {
                            topology_scalar_pair(spec, scalar, &element, geographic)
                        } else {
                            topology_scalar_pair(spec, &element, scalar, geographic)
                        }
                    })
            }
        })
        .collect()
}

/// Declared kind + emptiness for one storage row (no materialize for packed).
fn row_kind_empty(row: ShapeRow<'_>) -> (GeometryKind, bool) {
    match row {
        ShapeRow::Point(_) => (GeometryKind::Point, false),
        ShapeRow::Line(_, start, end) => (GeometryKind::LineString, start == end),
        ShapeRow::Rings(_, _, start, end) => (GeometryKind::Polygon, start == end),
        ShapeRow::Handle(handle) => (GeometryKind::of(handle.shape()), handle.shape().is_empty()),
        ShapeRow::Shape(shape) => (GeometryKind::of(shape), shape.is_empty()),
    }
}

pub(crate) fn topology_scalar_pair(
    spec: &PredicateSpec,
    left: &ShapeData,
    right: &ShapeData,
    geographic: bool,
) -> bool {
    if geographic {
        predicate_matrix_value(
            spec,
            crate::geometry::graph_relate_data_geographic(left, right),
        )
    } else {
        predicate_matrix_value(spec, graph_relate_data(left, right))
    }
}

fn predicate_matrix_value(spec: &PredicateSpec, matrix: crate::geometry::De9im) -> bool {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geometry::{CoordSeq, Point, Polygon, Ring};

    fn rectangle(minx: f64, miny: f64, maxx: f64, maxy: f64) -> Shape {
        Shape::Polygon(Polygon::new(
            Ring::from_trusted_closed(vec![
                Point::new_unchecked_xy(minx, miny),
                Point::new_unchecked_xy(maxx, miny),
                Point::new_unchecked_xy(maxx, maxy),
                Point::new_unchecked_xy(minx, maxy),
                Point::new_unchecked_xy(minx, miny),
            ]),
            Vec::new(),
        ))
    }

    #[test]
    fn geographic_crossing_rows_fail_open_the_bounds_gate() {
        let query = ShapeData::from(rectangle(172.0, 42.0, 178.0, 48.0));
        let crossing = rectangle(170.0, 40.0, -170.0, 50.0);
        let actual = scalar_vs_shapes(
            &Predicate::Intersects.spec(),
            &query,
            [(0, ShapeRow::Shape(&crossing))].into_iter(),
            true,
            None,
            true,
        );
        assert_eq!(actual, [true]);
    }

    #[test]
    fn geographic_crossing_packed_rows_fail_open_the_bounds_gate() {
        let query = ShapeData::from(rectangle(172.0, 42.0, 178.0, 48.0));
        let coords = CoordSeq::from(vec![
            Point::new_unchecked_xy(170.0, 40.0),
            Point::new_unchecked_xy(-170.0, 40.0),
            Point::new_unchecked_xy(-170.0, 50.0),
            Point::new_unchecked_xy(170.0, 50.0),
            Point::new_unchecked_xy(170.0, 40.0),
        ]);
        let offsets = [0, 5];
        let actual = scalar_vs_shapes(
            &Predicate::Intersects.spec(),
            &query,
            [(0, ShapeRow::Rings(&coords, &offsets, 0, 1))].into_iter(),
            true,
            None,
            true,
        );
        assert_eq!(actual, [true]);
    }
}
