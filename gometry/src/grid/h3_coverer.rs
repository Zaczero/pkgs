//! Certified H3 traversal and private overlap relation.
//!
//! Bbox and overlap enumeration share the certified descendant cap.  Target
//! overlap leaves use the ordered native boundary/fan relation; no planar
//! chord proxy can establish a negative.
//!
//! The certified carrier owns unsplit source authority.  Historical
//! antimeridian working-shape splitting is intentionally absent here: it is a
//! planar-tile implementation detail, never authority for an exact H3
//! negative.

use h3o::{CellIndex, LatLng, Resolution};

use crate::collections::HashMap;
use crate::geometry::Shape;
use crate::grid::affine_source::{
    GeographicCoverageSource, GridDegreePoint, GridPointClass, RectClass, SphericalGridTarget,
    visit_grid_cover_components,
};
use crate::grid::coverer::dissolved_polygonal_area;
use crate::grid::spherical_arc::{
    ArcContact, Bound, CertifiedH3DescendantCap, H3ArcSet, H3CellPlan, H3FanPlan, H3FanPointClass,
    classify_h3_arc_contact, h3_bbox_positive_witnesses, h3_cell_plan,
};
use crate::grid::{CoverBudgetExceeded, ensure_cover_budget};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum H3TraversalRule {
    Within,
    Bbox,
    Overlap,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct H3CoveredCell {
    pub(crate) cell: CellIndex,
    pub(crate) interior: bool,
}

#[derive(Debug)]
pub(crate) enum H3CoverError {
    Budget(CoverBudgetExceeded),
    Native(crate::error::Error),
    CapacityOverflow,
}

fn try_reserve_h3<T>(values: &mut Vec<T>, additional: usize) -> Result<(), H3CoverError> {
    let required = values
        .len()
        .checked_add(additional)
        .ok_or(H3CoverError::CapacityOverflow)?;
    let element_size = std::mem::size_of::<T>();
    if element_size != 0 && required > (isize::MAX as usize) / element_size {
        return Err(H3CoverError::CapacityOverflow);
    }
    crate::try_reserve_vec(values, additional).map_err(H3CoverError::Native)
}

/// Keep the first atomic cover in its already sorted vector.  Once a second
/// component contributes, promote exactly once and retain only unique cells.
#[derive(Debug)]
enum H3CoverAccumulatorState {
    Empty,
    Single(Vec<H3CoveredCell>),
    Many(H3UniqueCells),
}

#[derive(Debug)]
struct H3UniqueCells {
    cells: HashMap<CellIndex, bool>,
}

impl H3UniqueCells {
    fn with_capacity(
        first_len: usize,
        second_len: usize,
        max_cells: Option<usize>,
    ) -> Result<Self, H3CoverError> {
        Self::with_capacity_using(
            first_len,
            second_len,
            max_cells,
            crate::collections::collection_hash_state,
        )
    }

    fn with_capacity_using(
        first_len: usize,
        second_len: usize,
        max_cells: Option<usize>,
        hash_state: impl FnOnce() -> crate::error::Result<&'static ahash::RandomState>,
    ) -> Result<Self, H3CoverError> {
        let occurrences = first_len
            .checked_add(second_len)
            .ok_or(H3CoverError::CapacityOverflow)?;
        let reservation = occurrences.min(max_cells.unwrap_or(usize::MAX));
        let mut cells = HashMap::with_hasher(hash_state().map_err(H3CoverError::Native)?.clone());
        cells
            .try_reserve(reservation)
            .map_err(|error| H3CoverError::Native(error.into()))?;
        Ok(Self { cells })
    }

    fn reserve_for_component(
        &mut self,
        component_len: usize,
        max_cells: Option<usize>,
    ) -> Result<(), H3CoverError> {
        // The component length is an upper bound on new keys; reserving only
        // the remaining budget avoids allocating beyond a caller's ceiling
        // while duplicates still remain admissible.
        let remaining =
            max_cells.map_or(usize::MAX, |limit| limit.saturating_sub(self.cells.len()));
        self.try_reserve(component_len.min(remaining))
    }

    fn try_reserve(&mut self, additional: usize) -> Result<(), H3CoverError> {
        self.cells
            .len()
            .checked_add(additional)
            .ok_or(H3CoverError::CapacityOverflow)?;
        self.cells
            .try_reserve(additional)
            .map_err(|error| H3CoverError::Native(error.into()))
    }

    fn insert(
        &mut self,
        entry: H3CoveredCell,
        max_cells: Option<usize>,
    ) -> Result<bool, H3CoverError> {
        let next_len = self
            .cells
            .len()
            .checked_add(1)
            .ok_or(H3CoverError::CapacityOverflow)?;
        match self.cells.entry(entry.cell) {
            std::collections::hash_map::Entry::Occupied(mut existing) => {
                *existing.get_mut() |= entry.interior;
                Ok(true)
            },
            std::collections::hash_map::Entry::Vacant(slot) => {
                ensure_cover_budget(next_len, max_cells).map_err(H3CoverError::Budget)?;
                slot.insert(entry.interior);
                Ok(false)
            },
        }
    }

    fn finish(self) -> Result<Vec<H3CoveredCell>, H3CoverError> {
        let mut output = Vec::new();
        try_reserve_h3(&mut output, self.cells.len())?;
        output.extend(
            self.cells
                .into_iter()
                .map(|(cell, interior)| H3CoveredCell { cell, interior }),
        );
        output.sort_unstable_by_key(|entry| u64::from(entry.cell));
        Ok(output)
    }
}

#[derive(Debug)]
struct H3CoverAccumulator {
    state: H3CoverAccumulatorState,
    #[cfg(test)]
    stats: H3AggregationStats,
}

impl Default for H3CoverAccumulator {
    fn default() -> Self {
        Self {
            state: H3CoverAccumulatorState::Empty,
            #[cfg(test)]
            stats: H3AggregationStats::default(),
        }
    }
}

impl H3CoverAccumulator {
    fn add_component(
        &mut self,
        component: Vec<H3CoveredCell>,
        max_cells: Option<usize>,
    ) -> Result<(), H3CoverError> {
        if component.is_empty() {
            return Ok(());
        }
        #[cfg(test)]
        {
            self.stats.submitted_occurrences += component.len();
        }
        let state = std::mem::replace(&mut self.state, H3CoverAccumulatorState::Empty);
        match state {
            H3CoverAccumulatorState::Empty => {
                ensure_cover_budget(component.len(), max_cells).map_err(H3CoverError::Budget)?;
                #[cfg(test)]
                {
                    self.stats.unique_cells = component.len();
                    self.stats.accumulator_peak_entries = component.len();
                }
                self.state = H3CoverAccumulatorState::Single(component);
            },
            H3CoverAccumulatorState::Single(previous) => {
                #[cfg(test)]
                {
                    self.stats.old_equivalent_prefix_entries_copied += previous.len();
                }
                let mut unique =
                    H3UniqueCells::with_capacity(previous.len(), component.len(), max_cells)?;
                self.insert_entries(&mut unique, previous, max_cells)?;
                self.insert_entries(&mut unique, component, max_cells)?;
                self.state = H3CoverAccumulatorState::Many(unique);
            },
            H3CoverAccumulatorState::Many(mut unique) => {
                #[cfg(test)]
                {
                    self.stats.old_equivalent_prefix_entries_copied += unique.cells.len();
                }
                unique.reserve_for_component(component.len(), max_cells)?;
                self.insert_entries(&mut unique, component, max_cells)?;
                self.state = H3CoverAccumulatorState::Many(unique);
            },
        }
        Ok(())
    }

    #[cfg_attr(
        not(test),
        expect(
            clippy::needless_pass_by_ref_mut,
            reason = "test-only movement statistics share the production insertion path"
        )
    )]
    fn insert_entries(
        &mut self,
        unique: &mut H3UniqueCells,
        entries: impl IntoIterator<Item = H3CoveredCell>,
        max_cells: Option<usize>,
    ) -> Result<(), H3CoverError> {
        for entry in entries {
            #[cfg(test)]
            {
                let duplicate = unique.insert(entry, max_cells)?;
                self.stats.accumulator_entries_moved += 1;
                if duplicate {
                    self.stats.duplicate_merges += 1;
                }
                self.stats.unique_cells = unique.cells.len();
                self.stats.accumulator_peak_entries =
                    self.stats.accumulator_peak_entries.max(unique.cells.len());
            }
            #[cfg(not(test))]
            unique.insert(entry, max_cells)?;
        }
        Ok(())
    }

    fn finish(self) -> Result<Vec<H3CoveredCell>, H3CoverError> {
        match self.state {
            H3CoverAccumulatorState::Empty => Ok(Vec::new()),
            H3CoverAccumulatorState::Single(cells) => Ok(cells),
            H3CoverAccumulatorState::Many(cells) => cells.finish(),
        }
    }

    #[cfg(test)]
    const fn stats(&self) -> H3AggregationStats {
        self.stats
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct H3AggregationStats {
    pub(crate) submitted_occurrences: usize,
    pub(crate) unique_cells: usize,
    pub(crate) duplicate_merges: usize,
    pub(crate) old_equivalent_prefix_entries_copied: usize,
    pub(crate) accumulator_entries_moved: usize,
    pub(crate) accumulator_peak_entries: usize,
}

/// Target-wide cap state.  A cap that cannot certify is never approximated:
/// traversal simply retains the subtree as a boundary candidate.
#[derive(Clone, Debug)]
pub(crate) struct H3CoverPlan {
    target: Resolution,
    cap: Option<CertifiedH3DescendantCap>,
}

impl H3CoverPlan {
    pub(crate) fn new(target: Resolution) -> Self {
        Self {
            target,
            cap: CertifiedH3DescendantCap::for_target(target),
        }
    }
}

/// Build an H3 cover from its unsplit source shape.
///
/// Every aggregate spelling is the sorted union of its atomic covers.  This
/// is deliberately unconditional: a source can become pole-sensitive only
/// after the exact carrier sees a stored-double endpoint, so choosing the
/// aggregate route from a preliminary shape predicate is representation-
/// dependent and can manufacture a leaf.
pub(crate) fn h3_cover_shape(
    shape: &Shape,
    plan: &H3CoverPlan,
    rule: H3TraversalRule,
    max_cells: Option<usize>,
) -> Result<Vec<H3CoveredCell>, H3CoverError> {
    // `within` is a claim about the complete source union. Decomposing an
    // aggregate would miss a cell jointly covered by touching components even
    // though neither component contains it alone.
    if rule == H3TraversalRule::Within {
        let Some(dissolved) = dissolved_polygonal_area(shape).map_err(H3CoverError::Native)? else {
            return Ok(Vec::new());
        };
        let source = GeographicCoverageSource::from_validated(
            &dissolved,
            SphericalGridTarget::H3(plan.target),
        )
        .map_err(H3CoverError::Native)?;
        return h3_cover(&source, plan, rule, max_cells);
    }
    let mut merged = H3CoverAccumulator::default();
    visit_grid_cover_components(shape, &mut |component| {
        let source = GeographicCoverageSource::from_validated(
            component,
            SphericalGridTarget::H3(plan.target),
        )
        .map_err(H3CoverError::Native)?;
        // A component is a subset of the union, so it may use the same limit.
        // Merge immediately and enforce the budget again against the realized
        // deduplicated aggregate.
        let covered = h3_cover(&source, plan, rule, max_cells)?;
        if covered.is_empty() {
            return Ok(());
        }
        merged.add_component(covered, max_cells)
    })?;
    merged.finish()
}

/// All-root H3 bbox enumeration.  Leaf negatives require the exact logical
/// bbox; parent negatives require the certified descendant cap.  The 122 H3
/// roots form a partition, so sorting is verification rather than dedup.
pub(crate) fn h3_cover(
    source: &GeographicCoverageSource,
    plan: &H3CoverPlan,
    rule: H3TraversalRule,
    max_cells: Option<usize>,
) -> Result<Vec<H3CoveredCell>, H3CoverError> {
    const MAX_DFS_FRONTIER: usize = 212; // 122 roots + 6 * 15 depth-first growth.
    let mut stack = Vec::new();
    try_reserve_h3(&mut stack, MAX_DFS_FRONTIER)?;
    stack.extend(CellIndex::base_cells());
    debug_assert_eq!(stack.len(), 122, "vendor base-cell partition changed");

    let mut covered = Vec::new();
    while let Some(cell) = stack.pop() {
        let depth = cell.resolution();
        let relation = match plan.cap.as_ref() {
            Some(cap) => source
                .classify_rect(&source.admit_rectangular_candidate(cap.descendant_windows(cell)))
                .map_err(H3CoverError::Native)?,
            None => RectClass::Boundary,
        };
        match relation {
            RectClass::Outside => {},
            RectClass::Interior => {
                emit_interior_descendants(cell, plan.target, max_cells, &mut covered)?;
            },
            RectClass::Boundary if depth == plan.target => match rule {
                H3TraversalRule::Within => {
                    emit_within_leaf(source, plan, cell, max_cells, &mut covered)?;
                },
                H3TraversalRule::Bbox => {
                    emit_bbox_leaf(source, cell, max_cells, &mut covered)?;
                },
                H3TraversalRule::Overlap => {
                    emit_overlap_leaf(source, plan, cell, max_cells, &mut covered)?;
                },
            },
            RectClass::Boundary => {
                let next = Resolution::try_from(u8::from(depth) + 1)
                    .map_err(|_| H3CoverError::CapacityOverflow)?;
                let count = usize::try_from(cell.children_count(next))
                    .map_err(|_| H3CoverError::CapacityOverflow)?;
                try_reserve_h3(&mut stack, count)?;
                stack.extend(cell.children(next));
            },
        }
    }
    covered.sort_unstable_by_key(|entry| u64::from(entry.cell));
    debug_assert!(
        covered
            .array_windows::<2>()
            .all(|pair| u64::from(pair[0].cell) < u64::from(pair[1].cell)),
        "all root branches must remain disjoint"
    );
    Ok(covered)
}

fn emit_within_leaf(
    source: &GeographicCoverageSource,
    plan: &H3CoverPlan,
    cell: CellIndex,
    max_cells: Option<usize>,
    covered: &mut Vec<H3CoveredCell>,
) -> Result<(), H3CoverError> {
    let cell_plan = h3_cell_plan(cell, plan.target);
    match source
        .classify_rect(&source.admit_rectangular_candidate(cell_plan.bbox()))
        .map_err(H3CoverError::Native)?
    {
        RectClass::Interior => return emit_leaf(cell, true, max_cells, covered),
        RectClass::Outside => return Ok(()),
        RectClass::Boundary => {},
    }
    if source.is_unknown() {
        return Ok(());
    }
    if classify_h3_overlap(source, &cell_plan)? == H3LeafClass::Interior {
        emit_leaf(cell, true, max_cells, covered)?;
    }
    Ok(())
}

/// An unknown source can only fail open after exact-bbox exclusion, so it
/// shares the bbox leaf dispatch.  Otherwise every non-negative bbox relation
/// continues to the ordered H3 arc/fan classifier; the old chord polygon has
/// no role in this path.
fn emit_overlap_leaf(
    source: &GeographicCoverageSource,
    plan: &H3CoverPlan,
    cell: CellIndex,
    max_cells: Option<usize>,
    covered: &mut Vec<H3CoveredCell>,
) -> Result<(), H3CoverError> {
    if source.is_unknown() {
        return emit_bbox_leaf(source, cell, max_cells, covered);
    }
    let cell_plan = h3_cell_plan(cell, plan.target);
    if source
        .classify_rect(&source.admit_rectangular_candidate(cell_plan.bbox()))
        .map_err(H3CoverError::Native)?
        == RectClass::Outside
    {
        return Ok(());
    }
    let relation = classify_h3_overlap(source, &cell_plan)?;
    if relation == H3LeafClass::Outside {
        return Ok(());
    }
    emit_leaf(cell, relation == H3LeafClass::Interior, max_cells, covered)
}

/// A target cap boundary is first offered strictly positive point witnesses;
/// witness failure is never a negative.  The independent ordered-boundary
/// bbox is the sole fallback, so this route neither constructs nor depends on
/// the fan used by overlap classification.
fn emit_bbox_leaf(
    source: &GeographicCoverageSource,
    cell: CellIndex,
    max_cells: Option<usize>,
    covered: &mut Vec<H3CoveredCell>,
) -> Result<(), H3CoverError> {
    // A point or line source has no area, so it cannot contain a cell rectangle.
    // Skipping the positive-only witness pass here is exact: its only possible
    // outcome for these sources is `Boundary`, after which we run the certified
    // classifier below anyway.
    let witness_is_interior = if source.has_polygon() {
        match h3_bbox_positive_witnesses(cell) {
            Some(witnesses) => {
                let mut interior = false;
                for window in witnesses.iter() {
                    if source
                        .classify_rect(&source.admit_rectangular_candidate(window))
                        .map_err(H3CoverError::Native)?
                        == RectClass::Interior
                    {
                        interior = true;
                        break;
                    }
                }
                interior
            },
            None => false,
        }
    } else {
        false
    };
    if witness_is_interior {
        return emit_leaf(cell, false, max_cells, covered);
    }
    let cell_plan = h3_cell_plan(cell, cell.resolution());
    let relation = source
        .classify_rect(&source.admit_rectangular_candidate(cell_plan.bbox()))
        .map_err(H3CoverError::Native)?;
    if relation == RectClass::Outside {
        return Ok(());
    }
    emit_leaf(cell, false, max_cells, covered)
}

fn emit_leaf(
    cell: CellIndex,
    interior: bool,
    max_cells: Option<usize>,
    covered: &mut Vec<H3CoveredCell>,
) -> Result<(), H3CoverError> {
    let next_len = covered
        .len()
        .checked_add(1)
        .ok_or(H3CoverError::CapacityOverflow)?;
    ensure_cover_budget(next_len, max_cells).map_err(H3CoverError::Budget)?;
    try_reserve_h3(covered, 1)?;
    covered.push(H3CoveredCell { cell, interior });
    Ok(())
}

fn emit_interior_descendants(
    cell: CellIndex,
    target: Resolution,
    max_cells: Option<usize>,
    covered: &mut Vec<H3CoveredCell>,
) -> Result<(), H3CoverError> {
    let count =
        usize::try_from(cell.children_count(target)).map_err(|_| H3CoverError::CapacityOverflow)?;
    let total = covered
        .len()
        .checked_add(count)
        .ok_or(H3CoverError::CapacityOverflow)?;
    ensure_cover_budget(total, max_cells).map_err(H3CoverError::Budget)?;
    try_reserve_h3(covered, count)?;
    let start = covered.len();
    covered.extend(cell.children(target).map(|cell| H3CoveredCell {
        cell,
        interior: true,
    }));
    debug_assert_eq!(covered.len() - start, count, "vendor child count changed");
    Ok(())
}

/// Certified target-leaf classification.  `Boundary` includes every failed
/// proof and every closed contact; no caller may turn it into a negative.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum H3LeafClass {
    Outside,
    Boundary,
    Interior,
}

/// The private ordered H3 relation.  Its ordering is deliberately explicit:
/// source/cell boundary contact and source-in-cell witnesses veto before a
/// cell-center PIP can establish area containment; lower-dimensional contact
/// can retain a leaf but cannot downgrade an areal interior proof.
pub(crate) fn classify_h3_overlap(
    source: &GeographicCoverageSource,
    cell: &H3CellPlan,
) -> Result<H3LeafClass, H3CoverError> {
    if source.is_unknown() {
        return Ok(H3LeafClass::Boundary);
    }
    let Some(arcs) = cell.arcs() else {
        return Ok(H3LeafClass::Boundary);
    };
    let fan = cell.fan();
    classify_h3_overlap_parts(source, cell.cell(), arcs, &fan)
}

/// Relation body split from the plan accessor so the entry guard is directly
/// testable with an uncertified fan.  An `Uncertain` fan may never reach any
/// source-derived negative, even when the source itself is empty.
fn classify_h3_overlap_parts(
    source: &GeographicCoverageSource,
    cell: h3o::CellIndex,
    arcs: &H3ArcSet,
    fan: &H3FanPlan,
) -> Result<H3LeafClass, H3CoverError> {
    if source.is_unknown() || !matches!(fan, H3FanPlan::Certified(_)) {
        return Ok(H3LeafClass::Boundary);
    }

    let Some(authority) = source.authority_pieces() else {
        return Ok(H3LeafClass::Boundary);
    };
    for piece in authority.filter(|piece| piece.is_polygon()) {
        if arcs
            .iter()
            .any(|arc| classify_h3_arc_contact(&piece.arc(), arc, fan) != ArcContact::None)
        {
            return Ok(H3LeafClass::Boundary);
        }
    }

    let Some(selection) = source.selection_pieces() else {
        return Ok(H3LeafClass::Boundary);
    };
    for piece in selection.filter(|piece| piece.is_polygon() && piece.is_positive_only()) {
        if arcs
            .iter()
            .any(|arc| classify_h3_arc_contact(&piece.arc(), arc, fan) != ArcContact::None)
        {
            return Ok(H3LeafClass::Boundary);
        }
    }

    // A witness for every shell and every hole is required: collapsing them
    // into one polygon witness can certify a cell that contains only a hole.
    if source
        .polygon_authority_witnesses()
        .any(|(component, ring, point)| {
            point.map_or_else(
                || !source.selection_ring_is_represented(component, ring),
                |point| authority_witness_retains_point(fan, Some(point)),
            )
        })
    {
        return Ok(H3LeafClass::Boundary);
    }

    // Every physical pole is a structural sphere point, not an arbitrary
    // `(+/-180, +/-90)` spelling.  The carrier retains exact split and vertex
    // witnesses so a pole path cannot disappear before the fan sees it.
    if source
        .selection_pole_witnesses()
        .any(|point| fan_retains_point(fan, Some(point)))
    {
        return Ok(H3LeafClass::Boundary);
    }

    let mut lower_contact = false;
    let Some(authority) = source.authority_pieces() else {
        return Ok(H3LeafClass::Boundary);
    };
    for piece in authority.filter(|piece| !piece.is_polygon()) {
        lower_contact |= arcs
            .iter()
            .any(|arc| classify_h3_arc_contact(&piece.arc(), arc, fan) != ArcContact::None);
    }
    let Some(selection) = source.selection_pieces() else {
        return Ok(H3LeafClass::Boundary);
    };
    for piece in selection.filter(|piece| !piece.is_polygon() && piece.is_positive_only()) {
        lower_contact |= arcs
            .iter()
            .any(|arc| classify_h3_arc_contact(&piece.arc(), arc, fan) != ArcContact::None);
    }
    lower_contact |= source
        .lower_authority_witnesses()
        .any(|point| authority_witness_retains_point(fan, point));
    lower_contact |= source
        .authority_points()
        .any(|point| authority_witness_retains_point(fan, point));
    lower_contact |= source
        .positive_selection_points()
        .any(|point| fan_retains_point(fan, point));
    lower_contact |= source.has_positive_selection_lower();

    if !source.has_polygon() {
        return Ok(if lower_contact {
            H3LeafClass::Boundary
        } else {
            H3LeafClass::Outside
        });
    }

    let center = LatLng::from(cell);
    let (Some(longitude), Some(latitude)) =
        (Bound::exact(center.lng()), Bound::exact(center.lat()))
    else {
        return Ok(H3LeafClass::Boundary);
    };
    if fan.kernel_point_class(longitude, latitude) != H3FanPointClass::Open {
        return Ok(H3LeafClass::Boundary);
    }

    // A reflected selection image can only veto a negative.  Crucially this
    // is candidate-local exact PIP, not the former global `has_positive...`
    // veto that widened every distant leaf when one source edge crossed a
    // pole.
    if source
        .positive_selection_point_class(center.lng(), center.lat())
        .map_err(H3CoverError::Native)?
        != GridPointClass::Exterior
    {
        return Ok(H3LeafClass::Boundary);
    }

    Ok(
        match source
            .authority_point_class(center.lng(), center.lat())
            .map_err(H3CoverError::Native)?
        {
            GridPointClass::Interior => H3LeafClass::Interior,
            GridPointClass::Exterior if lower_contact => H3LeafClass::Boundary,
            GridPointClass::Exterior => H3LeafClass::Outside,
            GridPointClass::Boundary | GridPointClass::Unknown => H3LeafClass::Boundary,
        },
    )
}

fn fan_retains_point(fan: &H3FanPlan, point: Option<GridDegreePoint>) -> bool {
    let Some(point) = point else {
        return true;
    };
    !point.is_proven_physical_latitude()
        || fan.point_class(point.longitude, point.latitude) != H3FanPointClass::Outside
}

/// Retained authority is allowed to locate only physical source points in a
/// native H3 fan.  An out-of-strip raw witness has no unit-sphere location of
/// its own; its exact reflected selection image is checked separately at the
/// candidate center.  Treating it as a universal witness would turn every
/// leaf into `Boundary` whenever a polygon crosses a pole.
fn authority_witness_retains_point(fan: &H3FanPlan, point: Option<GridDegreePoint>) -> bool {
    match point {
        None => true,
        Some(point) if point.is_proven_physical_latitude() => fan_retains_point(fan, Some(point)),
        Some(_) => false,
    }
}

#[cfg(test)]
#[path = "h3_coverer_tests.rs"]
mod tests;
