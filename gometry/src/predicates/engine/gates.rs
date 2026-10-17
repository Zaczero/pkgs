use super::{
    Bounds, GeographicPrepared, GeometryKind, PackedRow, Predicate, PredicateSpec, Shape, ShapeRow,
    SharedShapeOwners, ring_winding,
};

/// One operand as the pre-topology gates see it: a storage row, its declared
/// kind, its coordinate bounds, and whether those bounds mean what they say.
///
/// Deliberately not a [`PreparedPlan`] — a packed row would have to be
/// materialized to build one, and the point of these gates is to answer
/// before anything is materialized at all. An operand fixed for a whole call
/// is built once; operands that vary per row are built per row.
#[derive(Clone, Copy)]
pub(crate) struct GateOperand<'a> {
    pub(super) row: ShapeRow<'a>,
    pub(super) kind: GeometryKind,
    pub(super) bounds: Option<Bounds>,
    pub(super) bounds_sound: bool,
    /// Whether the operand is an axis-aligned rectangle, once settled for an
    /// operand that stays fixed across a call.
    pub(super) rectangle: Option<bool>,
}

impl<'a> GateOperand<'a> {
    pub(crate) const fn bounds(&self) -> Option<Bounds> {
        self.bounds
    }
    /// Read Cartesian gate facts off `row`, preferring `bounds` when the caller
    /// already holds them cached. Packed rows stay unmaterialized.
    pub(crate) fn cartesian(row: ShapeRow<'a>, bounds: Option<Bounds>) -> Self {
        let bounds = bounds.or_else(|| row.quick_bounds());
        Self {
            row,
            kind: row.kind(),
            bounds,
            bounds_sound: true,
            rectangle: None,
        }
    }

    /// Geographic gate facts derived from one admitted operand. There is no
    /// geographic operand except from a live [`GeographicPrepared`].
    pub(crate) fn from_prepared(prepared: &GeographicPrepared<'a>) -> Self {
        let plan = prepared.plan();
        let row = ShapeRow::Shape(plan.shape());
        Self {
            row,
            kind: row.kind(),
            bounds: plan.bounds(),
            bounds_sound: prepared.bounds_sound(),
            rectangle: None,
        }
    }

    /// Gate facts of an operand every pair of a call shares, with its
    /// rectangle fact settled once.
    pub(crate) fn fixed(shape: &'a Shape, bounds: Option<Bounds>) -> Self {
        let mut operand = Self::cartesian(ShapeRow::Shape(shape), bounds);
        operand.rectangle = Some(
            operand
                .bounds
                .is_some_and(|bounds| crate::geometry::is_axis_aligned_rectangle(shape, bounds)),
        );
        operand
    }

    fn is_rectangle(&self, bounds: Bounds) -> bool {
        if let Some(known) = self.rectangle {
            return known;
        }
        if self.kind != GeometryKind::Polygon {
            return false;
        }
        crate::geometry::is_axis_aligned_rectangle_row(self.row, bounds)
    }

    /// Positive-only proof for the empty-support equality case. A hole-free
    /// ring with nonzero exact area owns a filled face; all other areal
    /// support remains unknown rather than guessed from declared kind.
    fn has_known_nonempty_support(&self) -> bool {
        match self.row {
            ShapeRow::Handle(plan) => shape_has_known_nonempty_support(
                plan.shape(),
                &mut SharedShapeOwners::for_collections(),
            ),
            ShapeRow::Shape(shape) => {
                shape_has_known_nonempty_support(shape, &mut SharedShapeOwners::for_collections())
            },
            ShapeRow::Point(_) => true,
            ShapeRow::Packed(row @ PackedRow::Polygons { .. }) => {
                let Some(mut rings) = ShapeRow::Packed(row).polygon_rings() else {
                    return false;
                };
                let Some(shell) = rings.next() else {
                    return false;
                };
                rings.next().is_none() && !ring_winding(&shell).is_degenerate()
            },
            ShapeRow::Packed(row) => !row.is_empty(),
        }
    }

    /// Whether this operand's coordinate box means what it says, so a caller
    /// may compare it as a plain rectangle. False across the seam, at a pole,
    /// and at a longitude the frame would move.
    pub(crate) const fn bounds_are_sound(&self) -> bool {
        self.bounds_sound
    }
}

fn shape_has_known_nonempty_support(shape: &Shape, owners: &mut SharedShapeOwners) -> bool {
    match shape {
        Shape::Point(_) => true,
        Shape::MultiPoint(points) => !points.is_empty(),
        Shape::LineString(line) => !line.is_empty(),
        Shape::MultiLineString(lines) => lines.iter().any(|line| !line.is_empty()),
        Shape::Polygon(polygon) => {
            polygon.holes.is_empty() && !ring_winding(&polygon.shell).is_degenerate()
        },
        Shape::MultiPolygon(polygons) => polygons.iter().any(|polygon| {
            polygon.holes.is_empty() && !ring_winding(&polygon.shell).is_degenerate()
        }),
        Shape::GeometryCollection(parts) => {
            owners.first_collection(parts.as_slice(), parts.members_are_shared())
                && parts
                    .iter()
                    .any(|part| shape_has_known_nonempty_support(part, owners))
        },
        Shape::Empty(..) => false,
    }
}

#[cfg(all(test, debug_assertions))]
thread_local! {
    pub(super) static PRE_TOPOLOGY_CALLS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
}

impl PredicateSpec {
    /// A definite answer from pre-topology facts, before the pair is related.
    ///
    /// Disjoint boxes refute every predicate whose matrix requires an
    /// intersection. Equality also refutes when both operands have a
    /// certificate of non-empty support; raw bounds alone cannot distinguish
    /// two empty supports, which are equal.
    pub(crate) fn bounds_gate(
        &self,
        left: &GateOperand<'_>,
        right: &GateOperand<'_>,
    ) -> Option<bool> {
        let (left_bounds, right_bounds) = (left.bounds?, right.bounds?);
        if !left_bounds.intersects(right_bounds) {
            return match self.predicate {
                Predicate::Disjoint => Some(true),
                Predicate::Intersects
                | Predicate::Contains
                | Predicate::ContainsProperly
                | Predicate::Within
                | Predicate::Covers
                | Predicate::CoveredBy
                | Predicate::Touches
                | Predicate::Crosses
                | Predicate::Overlaps => Some(false),
                Predicate::Equals
                    if left.has_known_nonempty_support() && right.has_known_nonempty_support() =>
                {
                    Some(false)
                },
                Predicate::Equals => None,
            };
        }
        None
    }

    /// Every answer available before either operand is prepared.
    ///
    /// Ordered by what each stage has to touch: declared kinds refute without
    /// reading coordinates, disjoint bounds refute shared-support predicates,
    /// and two rectangles settle every predicate from their bounds. `None`
    /// hands the pair to the exact kernel.
    pub(crate) fn pre_topology(
        &self,
        left: &GateOperand<'_>,
        right: &GateOperand<'_>,
    ) -> Option<bool> {
        #[cfg(all(test, debug_assertions))]
        PRE_TOPOLOGY_CALLS.with(|calls| calls.set(calls.get().wrapping_add(1)));
        if let Some(verdict) = self.predicate.kind_gate(left.kind, right.kind) {
            return Some(verdict);
        }
        if !left.bounds_sound || !right.bounds_sound {
            return None;
        }
        if let Some(verdict) = self.bounds_gate(left, right) {
            return Some(verdict);
        }
        let (left_bounds, right_bounds) = (left.bounds?, right.bounds?);
        self.rectangle_pair(left, left_bounds, right, right_bounds)
    }

    /// Two filled axis-aligned rectangles each cover their bounding box
    /// exactly, so interval comparisons of the boxes decide every predicate
    /// and no graph is needed.
    ///
    /// Geographic operands included, because this library charts a geographic
    /// edge as a straight segment between canonical endpoints rather than a
    /// great-circle arc, so a rectangle really is its box here. Only the seam,
    /// the poles and a non-canonical longitude break that, and reaching this
    /// point means both operands already passed all three.
    fn rectangle_pair(
        &self,
        left: &GateOperand<'_>,
        left_bounds: Bounds,
        right: &GateOperand<'_>,
        right_bounds: Bounds,
    ) -> Option<bool> {
        if left.rectangle == Some(false) || right.rectangle == Some(false) {
            return None;
        }
        (left.is_rectangle(left_bounds) && right.is_rectangle(right_bounds))
            .then(|| rectangle_predicate(self.predicate, left_bounds, right_bounds))
    }
}

/// A predicate between two nondegenerate filled boxes. Closed boxes meet
/// when their closed intervals overlap on both axes; their interiors meet
/// when the open intervals do. A nondegenerate box inside another shares its
/// interior, so containment and coverage coincide.
fn rectangle_predicate(predicate: Predicate, left: Bounds, right: Bounds) -> bool {
    let covers = |outer: Bounds, inner: Bounds| {
        outer.minx() <= inner.minx()
            && inner.maxx() <= outer.maxx()
            && outer.miny() <= inner.miny()
            && inner.maxy() <= outer.maxy()
    };
    let closed = left.intersects(right);
    let open = left.minx() < right.maxx()
        && right.minx() < left.maxx()
        && left.miny() < right.maxy()
        && right.miny() < left.maxy();
    match predicate {
        Predicate::Intersects => closed,
        Predicate::Disjoint => !closed,
        Predicate::Contains | Predicate::Covers => covers(left, right),
        Predicate::Within | Predicate::CoveredBy => covers(right, left),
        Predicate::ContainsProperly => {
            left.minx() < right.minx()
                && right.maxx() < left.maxx()
                && left.miny() < right.miny()
                && right.maxy() < left.maxy()
        },
        Predicate::Touches => closed && !open,
        Predicate::Crosses => false,
        Predicate::Overlaps => open && !covers(left, right) && !covers(right, left),
        Predicate::Equals => covers(left, right) && covers(right, left),
    }
}
