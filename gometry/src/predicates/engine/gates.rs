use super::{
    Bounds, GeographicPrepared, GeometryKind, PackedRow, Predicate, PredicateSpec, Shape, ShapeRow,
    SharedShapeOwners, ring_winding, row_kind_empty,
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
        let (kind, _) = row_kind_empty(row);
        let bounds = bounds.or_else(|| row.quick_bounds());
        Self {
            row,
            kind,
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
        let (kind, _) = row_kind_empty(row);
        Self {
            row,
            kind,
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

    fn is_rectangle(&self, bounds: Bounds) -> crate::error::Result<bool> {
        if let Some(known) = self.rectangle {
            return Ok(known);
        }
        if self.kind != GeometryKind::Polygon {
            return Ok(false);
        }
        self.row
            .try_with_shape(|shape| Ok(crate::geometry::is_axis_aligned_rectangle(shape, bounds)))
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
    /// and two rectangles that meet settle `intersects` outright. `None` hands
    /// the pair to the exact kernel.
    pub(crate) fn pre_topology(
        &self,
        left: &GateOperand<'_>,
        right: &GateOperand<'_>,
    ) -> crate::error::Result<Option<bool>> {
        #[cfg(all(test, debug_assertions))]
        PRE_TOPOLOGY_CALLS.with(|calls| calls.set(calls.get().wrapping_add(1)));
        if let Some(verdict) = self.predicate.kind_gate(left.kind, right.kind) {
            return Ok(Some(verdict));
        }
        if !left.bounds_sound || !right.bounds_sound {
            return Ok(None);
        }
        if let Some(verdict) = self.bounds_gate(left, right) {
            return Ok(Some(verdict));
        }
        let (Some(left_bounds), Some(right_bounds)) = (left.bounds, right.bounds) else {
            return Ok(None);
        };
        self.rectangles_meet(left, left_bounds, right, right_bounds)
    }

    /// Two axis-aligned rectangles whose bounds overlap share ground, because
    /// each covers its own bounding box exactly. Reaching here means the
    /// bounds gate already found the boxes meet, so no graph decides this.
    ///
    /// Geographic operands included, because this library charts a geographic
    /// edge as a straight segment between canonical endpoints rather than a
    /// great-circle arc, so a rectangle really is its box here. Only the seam,
    /// the poles and a non-canonical longitude break that, and reaching this
    /// point means both operands already passed all three.
    fn rectangles_meet(
        &self,
        left: &GateOperand<'_>,
        left_bounds: Bounds,
        right: &GateOperand<'_>,
        right_bounds: Bounds,
    ) -> crate::error::Result<Option<bool>> {
        let meeting = match self.predicate {
            Predicate::Intersects => true,
            Predicate::Disjoint => false,
            Predicate::Contains
            | Predicate::ContainsProperly
            | Predicate::Within
            | Predicate::Covers
            | Predicate::CoveredBy
            | Predicate::Touches
            | Predicate::Crosses
            | Predicate::Overlaps
            | Predicate::Equals => return Ok(None),
        };
        if left.rectangle == Some(false) || right.rectangle == Some(false) {
            return Ok(None);
        }
        Ok(
            (left.is_rectangle(left_bounds)? && right.is_rectangle(right_bounds)?)
                .then_some(meeting),
        )
    }
}
