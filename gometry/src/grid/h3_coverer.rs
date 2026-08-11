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

use std::sync::atomic::{AtomicUsize, Ordering};

use h3o::{CellIndex, LatLng, Resolution};

use crate::geometry::Shape;
use crate::grid::affine_source::{
    GridAffineSource, GridDegreePoint, GridPointClass, RectClass, SphericalGridTarget,
    normalize_grid_source, visit_grid_cover_components,
};
use crate::grid::coverer::dissolved_polygonal_area;
use crate::grid::spherical_arc::{
    ArcContact, Bound, CertifiedH3DescendantCap, DegreeWindowResult, H3ArcSet, H3CellPlan,
    H3FanPlan, H3FanPointClass, H3PoleOwners, classify_h3_arc_contact, exact_h3_bbox_for_cell,
    h3_bbox_positive_witnesses, h3_cell_plan,
};
use crate::grid::{CoverBudgetExceeded, ensure_cover_budget};

static H3_DIAGNOSTIC_NODES: AtomicUsize = AtomicUsize::new(0);
static H3_DIAGNOSTIC_BBOX_LEAVES: AtomicUsize = AtomicUsize::new(0);

// The unknown-source dispatch is a performance property, not a semantic one:
// source.is_unknown() would make the relation fail open after the same bbox
// exclusion.  Keep its preparation-free shape observable without adding any
// production state.
#[cfg(test)]
std::thread_local! {
    static OVERLAP_CELL_PLAN_PREPARATIONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static TEST_DIAGNOSTIC_NODES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
fn reset_overlap_cell_plan_preparations() {
    OVERLAP_CELL_PLAN_PREPARATIONS.set(0);
}

#[cfg(test)]
fn overlap_cell_plan_preparations() -> usize {
    OVERLAP_CELL_PLAN_PREPARATIONS.get()
}

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
    Allocation,
    CapacityOverflow,
    Geometry(crate::error::Error),
}

/// Target-wide cap state.  A cap that cannot certify is never approximated:
/// traversal simply retains the subtree as a boundary candidate.
#[derive(Clone, Debug)]
pub(crate) struct H3CoverPlan {
    target: Resolution,
    cap: Option<CertifiedH3DescendantCap>,
    poles: H3PoleOwners,
}

impl H3CoverPlan {
    pub(crate) fn new(target: Resolution) -> Self {
        Self {
            target,
            cap: CertifiedH3DescendantCap::for_target(target),
            poles: H3PoleOwners::for_target(target),
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
    let diagnostic = std::env::var_os("GOMETRY_H3_DIAGNOSTIC").is_some();
    if diagnostic {
        H3_DIAGNOSTIC_NODES.store(0, Ordering::Relaxed);
        H3_DIAGNOSTIC_BBOX_LEAVES.store(0, Ordering::Relaxed);
    }
    #[cfg(test)]
    TEST_DIAGNOSTIC_NODES.set(0);
    let normalized = normalize_grid_source(shape);
    // `within` is a claim about the complete source union. Decomposing an
    // aggregate would miss a cell jointly covered by touching components even
    // though neither component contains it alone.
    if rule == H3TraversalRule::Within {
        let Some(dissolved) =
            dissolved_polygonal_area(&normalized).map_err(H3CoverError::Geometry)?
        else {
            return Ok(Vec::new());
        };
        let source = GridAffineSource::new(&dissolved, SphericalGridTarget::H3(plan.target))
            .map_err(|_| H3CoverError::Allocation)?;
        return h3_cover(&source, plan, rule, max_cells);
    }
    let mut merged = Vec::new();
    visit_grid_cover_components(&normalized, &mut |component| {
        let source = GridAffineSource::new(component, SphericalGridTarget::H3(plan.target))
            .map_err(|_| H3CoverError::Allocation)?;
        // A component is a subset of the union, so it may use the same limit.
        // Merge immediately and enforce the budget again against the realized
        // deduplicated aggregate.
        let covered = h3_cover(&source, plan, rule, max_cells)?;
        merge_h3_component(&mut merged, &covered, max_cells)
    })?;
    Ok(merged)
}

fn merge_h3_component(
    merged: &mut Vec<H3CoveredCell>,
    component: &[H3CoveredCell],
    max_cells: Option<usize>,
) -> Result<(), H3CoverError> {
    let previous = std::mem::take(merged);
    let mut result = Vec::new();
    result
        .try_reserve(previous.len().saturating_add(component.len()))
        .map_err(|_| H3CoverError::Allocation)?;
    let (mut left, mut right) = (0, 0);
    while left < previous.len() || right < component.len() {
        let entry = match (previous.get(left), component.get(right)) {
            (Some(a), Some(b)) if a.cell < b.cell => {
                left += 1;
                *a
            },
            (Some(a), Some(b)) if b.cell < a.cell => {
                right += 1;
                *b
            },
            (Some(a), Some(b)) => {
                left += 1;
                right += 1;
                H3CoveredCell {
                    cell: a.cell,
                    interior: a.interior || b.interior,
                }
            },
            (Some(a), None) => {
                left += 1;
                *a
            },
            (None, Some(b)) => {
                right += 1;
                *b
            },
            (None, None) => break,
        };
        if result
            .last()
            .is_some_and(|last: &H3CoveredCell| last.cell == entry.cell)
        {
            result.last_mut().expect("just checked").interior |= entry.interior;
        } else {
            ensure_cover_budget(result.len().saturating_add(1), max_cells)
                .map_err(H3CoverError::Budget)?;
            result.push(entry);
        }
    }
    *merged = result;
    Ok(())
}

/// All-root H3 bbox enumeration.  Leaf negatives require the exact logical
/// bbox; parent negatives require the certified descendant cap.  The 122 H3
/// roots form a partition, so sorting is verification rather than dedup.
pub(crate) fn h3_cover(
    source: &GridAffineSource,
    plan: &H3CoverPlan,
    rule: H3TraversalRule,
    max_cells: Option<usize>,
) -> Result<Vec<H3CoveredCell>, H3CoverError> {
    const MAX_DFS_FRONTIER: usize = 212; // 122 roots + 6 * 15 depth-first growth.
    let diagnostic = std::env::var_os("GOMETRY_H3_DIAGNOSTIC").is_some();
    let mut stack = Vec::new();
    stack
        .try_reserve_exact(MAX_DFS_FRONTIER)
        .map_err(|_| H3CoverError::Allocation)?;
    stack.extend(CellIndex::base_cells());
    debug_assert_eq!(stack.len(), 122, "vendor base-cell partition changed");

    let mut covered = Vec::new();
    while let Some(cell) = stack.pop() {
        if diagnostic {
            H3_DIAGNOSTIC_NODES.fetch_add(1, Ordering::Relaxed);
        }
        #[cfg(test)]
        TEST_DIAGNOSTIC_NODES.set(TEST_DIAGNOSTIC_NODES.get() + 1);
        let depth = cell.resolution();
        let relation = plan.cap.as_ref().map_or(RectClass::Boundary, |cap| {
            source.classify_rect(cap.descendant_windows(cell, plan.poles))
        });
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
                stack
                    .try_reserve(count)
                    .map_err(|_| H3CoverError::Allocation)?;
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
    if diagnostic {
        eprintln!(
            "h3 diagnostic: nodes={}, bbox_leaves={}, emitted={}",
            H3_DIAGNOSTIC_NODES.load(Ordering::Relaxed),
            H3_DIAGNOSTIC_BBOX_LEAVES.load(Ordering::Relaxed),
            covered.len(),
        );
    }
    Ok(covered)
}

fn emit_within_leaf(
    source: &GridAffineSource,
    plan: &H3CoverPlan,
    cell: CellIndex,
    max_cells: Option<usize>,
    covered: &mut Vec<H3CoveredCell>,
) -> Result<(), H3CoverError> {
    let cell_plan = h3_cell_plan(cell, plan.poles);
    match source.classify_rect(cell_plan.bbox()) {
        RectClass::Interior => return emit_leaf(cell, true, max_cells, covered),
        RectClass::Outside => return Ok(()),
        RectClass::Boundary => {},
    }
    if source.is_unknown() {
        return Ok(());
    }
    if classify_h3_overlap(source, &cell_plan) == H3LeafClass::Interior {
        emit_leaf(cell, true, max_cells, covered)?;
    }
    Ok(())
}

/// An unknown source can only fail open after exact-bbox exclusion, so it
/// shares the bbox leaf dispatch.  Otherwise every non-negative bbox relation
/// continues to the ordered H3 arc/fan classifier; the old chord polygon has
/// no role in this path.
fn emit_overlap_leaf(
    source: &GridAffineSource,
    plan: &H3CoverPlan,
    cell: CellIndex,
    max_cells: Option<usize>,
    covered: &mut Vec<H3CoveredCell>,
) -> Result<(), H3CoverError> {
    if source.is_unknown() {
        return emit_bbox_leaf(source, cell, max_cells, covered);
    }
    #[cfg(test)]
    OVERLAP_CELL_PLAN_PREPARATIONS.set(OVERLAP_CELL_PLAN_PREPARATIONS.get() + 1);
    let cell_plan = h3_cell_plan(cell, plan.poles);
    if source.classify_rect(cell_plan.bbox()) == RectClass::Outside {
        return Ok(());
    }
    let relation = classify_h3_overlap(source, &cell_plan);
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
    source: &GridAffineSource,
    cell: CellIndex,
    max_cells: Option<usize>,
    covered: &mut Vec<H3CoveredCell>,
) -> Result<(), H3CoverError> {
    if std::env::var_os("GOMETRY_H3_DIAGNOSTIC").is_some() {
        H3_DIAGNOSTIC_BBOX_LEAVES.fetch_add(1, Ordering::Relaxed);
    }
    // A point or line source has no area, so it cannot contain a cell rectangle.
    // Skipping the positive-only witness pass here is exact: its only possible
    // outcome for these sources is `Boundary`, after which we run the certified
    // classifier below anyway.
    let witness_is_interior = source.has_polygon()
        && h3_bbox_positive_witnesses(cell).is_some_and(|witnesses| {
            witnesses.iter().any(|window| {
                source.classify_rect(DegreeWindowResult::Windows(window)) == RectClass::Interior
            })
        });
    let relation = if witness_is_interior {
        RectClass::Interior
    } else {
        source.classify_rect(exact_h3_bbox_for_cell(cell))
    };
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
    covered
        .try_reserve(1)
        .map_err(|_| H3CoverError::Allocation)?;
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
    covered
        .try_reserve_exact(count)
        .map_err(|_| H3CoverError::Allocation)?;
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
pub(crate) fn classify_h3_overlap(source: &GridAffineSource, cell: &H3CellPlan) -> H3LeafClass {
    let (Some(arcs), fan) = (cell.arcs(), cell.fan()) else {
        return H3LeafClass::Boundary;
    };
    classify_h3_overlap_parts(source, cell.cell(), arcs, fan)
}

/// Relation body split from the plan accessor so the entry guard is directly
/// testable with an uncertified fan.  An `Uncertain` fan may never reach any
/// source-derived negative, even when the source itself is empty.
fn classify_h3_overlap_parts(
    source: &GridAffineSource,
    cell: h3o::CellIndex,
    arcs: &H3ArcSet,
    fan: &H3FanPlan,
) -> H3LeafClass {
    if source.is_unknown() || !matches!(fan, H3FanPlan::Certified(_)) {
        return H3LeafClass::Boundary;
    }

    let Some(authority) = source.authority_pieces() else {
        return H3LeafClass::Boundary;
    };
    for piece in authority.filter(|piece| piece.is_polygon()) {
        if arcs
            .iter()
            .any(|arc| classify_h3_arc_contact(piece.arc(), arc, fan) != ArcContact::None)
        {
            return H3LeafClass::Boundary;
        }
    }

    let Some(selection) = source.selection_pieces() else {
        return H3LeafClass::Boundary;
    };
    for piece in selection.filter(|piece| piece.is_polygon() && piece.is_positive_only()) {
        if arcs
            .iter()
            .any(|arc| classify_h3_arc_contact(piece.arc(), arc, fan) != ArcContact::None)
        {
            return H3LeafClass::Boundary;
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
        return H3LeafClass::Boundary;
    }

    // Every physical pole is a structural sphere point, not an arbitrary
    // `(+/-180, +/-90)` spelling.  The carrier retains exact split and vertex
    // witnesses so a pole path cannot disappear before the fan sees it.
    if source
        .selection_pole_witnesses()
        .any(|point| fan_retains_point(fan, Some(point)))
    {
        return H3LeafClass::Boundary;
    }

    let mut lower_contact = false;
    let Some(authority) = source.authority_pieces() else {
        return H3LeafClass::Boundary;
    };
    for piece in authority.filter(|piece| !piece.is_polygon()) {
        lower_contact |= arcs
            .iter()
            .any(|arc| classify_h3_arc_contact(piece.arc(), arc, fan) != ArcContact::None);
    }
    let Some(selection) = source.selection_pieces() else {
        return H3LeafClass::Boundary;
    };
    for piece in selection.filter(|piece| !piece.is_polygon() && piece.is_positive_only()) {
        lower_contact |= arcs
            .iter()
            .any(|arc| classify_h3_arc_contact(piece.arc(), arc, fan) != ArcContact::None);
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
        return if lower_contact {
            H3LeafClass::Boundary
        } else {
            H3LeafClass::Outside
        };
    }

    let center = LatLng::from(cell);
    let (Some(longitude), Some(latitude)) =
        (Bound::exact(center.lng()), Bound::exact(center.lat()))
    else {
        return H3LeafClass::Boundary;
    };
    if fan.kernel_point_class(longitude, latitude) != H3FanPointClass::Open {
        return H3LeafClass::Boundary;
    }

    // A reflected selection image can only veto a negative.  Crucially this
    // is candidate-local exact PIP, not the former global `has_positive...`
    // veto that widened every distant leaf when one source edge crossed a
    // pole.
    if source.positive_selection_point_class(center.lng(), center.lat()) != GridPointClass::Exterior
    {
        return H3LeafClass::Boundary;
    }

    match source.authority_point_class(center.lng(), center.lat()) {
        GridPointClass::Interior => H3LeafClass::Interior,
        GridPointClass::Exterior if lower_contact => H3LeafClass::Boundary,
        GridPointClass::Exterior => H3LeafClass::Outside,
        GridPointClass::Boundary | GridPointClass::Unknown => H3LeafClass::Boundary,
    }
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
