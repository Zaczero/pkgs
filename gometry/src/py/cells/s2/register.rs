use std::collections::BTreeMap;

use rstar::{AABB, RTree, RTreeObject};

use crate::error::ResourceError;
use crate::geometry::{Component, Coordinates as _, Shape};
use crate::grid::affine_source::{
    GeographicCoverageSource, SphericalGridTarget, WorkingShapeRelation,
};
use crate::grid::s2::cellid::CellId;
use crate::grid::s2::coverer::{CoverRule, Coverer, Covering};
use crate::py::cells::coverage_ops::{
    GridCoverError, coverage_factory_shapes, envelope_window_shapes,
};
use crate::py::cells::s2::PyS2Cell;
use crate::py::cells::s2::functions::{
    _unpickle_s2_cell, s2_bounding_cell, s2_cells, s2_cover, s2_difference, s2_intersection,
    s2_union,
};
use crate::py::cells::s2::parse::S2LevelBudget;
use crate::py::cells::{Bound, CellRule, PyGeometry, PyModule, PyModuleMethods as _, PyResult};

impl From<S2ComponentCoverError> for GridCoverError {
    fn from(error: S2ComponentCoverError) -> Self {
        match error {
            S2ComponentCoverError::Budget(error) => Self::Budget(error),
            S2ComponentCoverError::Preparation(error) => Self::Native(error),
            S2ComponentCoverError::Allocation => Self::Native(ResourceError::Allocation.into()),
        }
    }
}

/// Failure that can occur while building a union of atomic S2 covers.
///
/// Allocation is transient and must remain a Python memory error rather than a
/// poisoned partition.
#[derive(Debug)]
pub(super) enum S2ComponentCoverError {
    Budget(crate::grid::CoverBudgetExceeded),
    Preparation(crate::error::Error),
    Allocation,
}

/// Cover one atomic source component.  The caller owns the aggregate target;
/// this helper keeps antimeridian working topology and the retained affine
/// authority identical for both its floor probe and final adaptive cover.
fn cover_s2_component(
    component: &Shape,
    min_level: u8,
    max_level: u8,
    level_mod: u8,
    max_cells: Option<usize>,
    target_cells: usize,
    rule: CoverRule,
) -> Result<Covering, S2ComponentCoverError> {
    let (working, relation) = if component.crosses_antimeridian() {
        (
            component
                .split_antimeridian()
                .map_err(S2ComponentCoverError::Preparation)?,
            WorkingShapeRelation::AntimeridianSplit,
        )
    } else {
        (component.clone(), WorkingShapeRelation::Identity)
    };
    let affine = GeographicCoverageSource::from_validated(component, SphericalGridTarget::S2)
        .map_err(S2ComponentCoverError::Preparation)?;
    Coverer {
        min_level,
        max_level,
        level_mod,
        max_cells,
        target_cells,
    }
    .cover_for_rule(&working, &affine, relation, rule)
    .map_err(|error| match error {
        crate::grid::CoverError::Preparation(error) => S2ComponentCoverError::Preparation(error),
        crate::grid::CoverError::Budget(error) => S2ComponentCoverError::Budget(error),
    })
}

/// S2's covering target is global, so aggregate syntax cannot decide either
/// the component ordering or the share of an adaptive target.  The key is the
/// full XY topology that reaches the grid (Z/M are deliberately absent): it is
/// an in-process ordering key, never a serialized geometry identity.
fn s2_component_key(component: Component<'_>) -> Result<Vec<u64>, S2ComponentCoverError> {
    fn append_points(
        key: &mut Vec<u64>,
        points: impl IntoIterator<Item = crate::geometry::Point>,
    ) -> Result<(), S2ComponentCoverError> {
        for point in points {
            crate::try_push_vec(key, point.x.to_bits())
                .map_err(|_| S2ComponentCoverError::Allocation)?;
            crate::try_push_vec(key, point.y.to_bits())
                .map_err(|_| S2ComponentCoverError::Allocation)?;
        }
        Ok(())
    }
    let mut key = Vec::new();
    match component {
        Component::Point(point) => {
            crate::try_push_vec(&mut key, 0).map_err(|_| S2ComponentCoverError::Allocation)?;
            crate::try_push_vec(&mut key, point.x.to_bits())
                .map_err(|_| S2ComponentCoverError::Allocation)?;
            crate::try_push_vec(&mut key, point.y.to_bits())
                .map_err(|_| S2ComponentCoverError::Allocation)?;
        },
        Component::LineString(line) => {
            crate::try_push_vec(&mut key, 1).map_err(|_| S2ComponentCoverError::Allocation)?;
            crate::try_push_vec(&mut key, line.len() as u64)
                .map_err(|_| S2ComponentCoverError::Allocation)?;
            append_points(&mut key, line.iter())?;
        },
        Component::Polygon(polygon) => {
            crate::try_push_vec(&mut key, 2).map_err(|_| S2ComponentCoverError::Allocation)?;
            crate::try_push_vec(&mut key, polygon.holes.len() as u64)
                .map_err(|_| S2ComponentCoverError::Allocation)?;
            crate::try_push_vec(&mut key, polygon.shell.coord_count() as u64)
                .map_err(|_| S2ComponentCoverError::Allocation)?;
            append_points(&mut key, polygon.shell.iter())?;
            for hole in polygon.holes.iter() {
                crate::try_push_vec(&mut key, hole.coord_count() as u64)
                    .map_err(|_| S2ComponentCoverError::Allocation)?;
                append_points(&mut key, hole.iter())?;
            }
        },
    }
    Ok(key)
}

/// An atomic source component in the canonicalization broad phase.
///
/// Exact `Shape::covers` decides semantic containment; this index only avoids
/// asking it about components whose planar envelopes cannot possibly contain
/// one another.  In particular, a large ordinary MultiPoint has one R-tree
/// candidate (itself) per point instead of a quadratic all-pairs scan.
struct IndexedS2Component {
    index: usize,
    envelope: AABB<[f64; 2]>,
}

impl RTreeObject for IndexedS2Component {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        self.envelope
    }
}

fn bounds_contain(outer: crate::geometry::Bounds, inner: crate::geometry::Bounds) -> bool {
    outer.minx() <= inner.minx()
        && outer.maxx() >= inner.maxx()
        && outer.miny() <= inner.miny()
        && outer.maxy() >= inner.maxy()
}

/// Canonical components of one grid source.
///
/// A component that is already covered by another component adds no source
/// support, so it cannot receive a private share of one adaptive target. This
/// uses the source topology itself as a positive subset proof: ordering, ring
/// start/direction, nested one-member collections, and contained points,
/// lines, or polygons all describe the same region and therefore collapse.
/// No grid proxy is allowed to establish that negative decision.
fn canonical_s2_components(source: &Shape) -> Result<Vec<Shape>, S2ComponentCoverError> {
    let mut components = Vec::new();
    source.try_for_each_component(&mut |component| {
        crate::try_push_vec(
            &mut components,
            (s2_component_key(component)?, component.to_shape()),
        )
        .map_err(|_| S2ComponentCoverError::Allocation)?;
        Ok(())
    })?;
    components.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    components.dedup_by(|left, right| left.0 == right.0);

    let mut bounds = Vec::new();
    crate::try_reserve_vec(&mut bounds, components.len())
        .map_err(|_| S2ComponentCoverError::Allocation)?;
    for (_, shape) in &components {
        crate::try_push_vec(&mut bounds, shape.bounds())
            .map_err(|_| S2ComponentCoverError::Allocation)?;
    }
    let mut indexed_bounds = Vec::new();
    for (entry, bounds) in bounds.iter().enumerate() {
        if let Some(bounds) = bounds {
            crate::try_push_vec(&mut indexed_bounds, IndexedS2Component {
                index: entry,
                envelope: AABB::from_corners([bounds.minx(), bounds.miny()], [
                    bounds.maxx(),
                    bounds.maxy(),
                ]),
            })
            .map_err(|_| S2ComponentCoverError::Allocation)?;
        }
    }
    let index = RTree::bulk_load(indexed_bounds);

    // Deterministic raw ordering makes equal supports choose one stable
    // representative. Keep only maximal source supports. The R-tree is a
    // broad phase only: `Shape::covers` is still the exact source predicate,
    // never a cell or chord proxy. A component can be discarded only when an
    // exact larger/equal support covers it; the lower key wins an equal pair.
    let mut canonical = Vec::new();
    crate::try_reserve_vec(&mut canonical, components.len())
        .map_err(|_| S2ComponentCoverError::Allocation)?;
    for (candidate_index, (_, candidate)) in components.iter().enumerate() {
        let Some(candidate_bounds) = bounds[candidate_index] else {
            crate::try_push_vec(&mut canonical, candidate.clone())
                .map_err(|_| S2ComponentCoverError::Allocation)?;
            continue;
        };
        let candidate_envelope =
            AABB::from_corners([candidate_bounds.minx(), candidate_bounds.miny()], [
                candidate_bounds.maxx(),
                candidate_bounds.maxy(),
            ]);
        let mut subsumed = false;
        for support in index.locate_in_envelope_intersecting(candidate_envelope) {
            if support.index == candidate_index {
                continue;
            }
            let Some(support_bounds) = bounds[support.index] else {
                continue;
            };
            if !bounds_contain(support_bounds, candidate_bounds)
                || !components[support.index]
                    .1
                    .covers(candidate)
                    .map_err(S2ComponentCoverError::Preparation)?
            {
                continue;
            }
            // Topologically equal supports cover each other; retain only
            // the deterministically earlier raw key. A strict superset
            // always wins regardless of its raw ordering.
            if support.index < candidate_index
                || !candidate
                    .covers(&components[support.index].1)
                    .map_err(S2ComponentCoverError::Preparation)?
            {
                subsumed = true;
                break;
            }
        }
        if !subsumed {
            crate::try_push_vec(&mut canonical, candidate.clone())
                .map_err(|_| S2ComponentCoverError::Allocation)?;
        }
    }
    Ok(canonical)
}

/// Cover already-canonical atomic shapes and merge one global cell union. This
/// is shared by source covering and the per-component bbox representation, so
/// neither may hand every syntactic window the full adaptive target.
fn cover_s2_shapes(
    components: Vec<Shape>,
    min_level: u8,
    max_level: u8,
    level_mod: u8,
    max_cells: Option<usize>,
    target_cells: usize,
    rule: CoverRule,
) -> Result<Vec<(CellId, bool)>, S2ComponentCoverError> {
    if components.is_empty() {
        return Ok(Vec::new());
    }

    // A finite hard cap also bounds the aggregate adaptive target. Otherwise
    // each independent bbox window can spend the full quality target before
    // the deduplicated aggregate sink sees its cells.
    let target_cells = max_cells.map_or(target_cells, |limit| target_cells.min(limit));

    // `target_cells` describes the returned coverage, not every syntactic
    // member of a Multi* carrier.  First measure each component's irreducible
    // min-level emission, then apportion only the remaining global budget.
    // An adaptive target below that floor remains a guide, not a rejection.
    let component_targets = if min_level == max_level {
        let mut targets = Vec::new();
        crate::try_reserve_vec(&mut targets, components.len())
            .map_err(|_| S2ComponentCoverError::Allocation)?;
        for _ in 0..components.len() {
            crate::try_push_vec(&mut targets, target_cells)
                .map_err(|_| S2ComponentCoverError::Allocation)?;
        }
        targets
    } else {
        let mut floors = Vec::new();
        crate::try_reserve_vec(&mut floors, components.len())
            .map_err(|_| S2ComponentCoverError::Allocation)?;
        for component in &components {
            crate::try_push_vec(
                &mut floors,
                cover_s2_component(
                    component, min_level, max_level, level_mod, max_cells, 1, rule,
                )?
                .into_cells()
                .len(),
            )
            .map_err(|_| S2ComponentCoverError::Allocation)?;
        }
        let floor_total: usize = floors.iter().sum();
        let remaining = target_cells.saturating_sub(floor_total);
        let per_component = remaining / components.len();
        let remainder = remaining % components.len();
        let mut targets = Vec::new();
        crate::try_reserve_vec(&mut targets, floors.len())
            .map_err(|_| S2ComponentCoverError::Allocation)?;
        for (index, floor) in floors.into_iter().enumerate() {
            crate::try_push_vec(
                &mut targets,
                floor + per_component + usize::from(index < remainder),
            )
            .map_err(|_| S2ComponentCoverError::Allocation)?;
        }
        targets
    };
    let mut tagged = Vec::new();
    let mut unique = max_cells.map(|_| BTreeMap::<CellId, bool>::new());
    for (component, component_target) in components.iter().zip(component_targets) {
        let covered = cover_s2_component(
            component,
            min_level,
            max_level,
            level_mod,
            max_cells,
            component_target,
            rule,
        )?;
        for (cell, interior) in covered.into_cells() {
            if let Some(unique) = &mut unique {
                if let Some(existing) = unique.get_mut(&cell) {
                    *existing |= interior;
                    continue;
                }
                let limit = max_cells.expect("unique map has a finite budget");
                crate::grid::ensure_cover_budget(unique.len().saturating_add(1), Some(limit))
                    .map_err(S2ComponentCoverError::Budget)?;
                unique.insert(cell, interior);
            } else {
                crate::try_push_vec(&mut tagged, (cell, interior))
                    .map_err(|_| S2ComponentCoverError::Allocation)?;
            }
        }
    }

    if let Some(unique) = unique {
        tagged = unique
            .into_iter()
            .try_fold(Vec::new(), |mut values, item| {
                crate::try_push_vec(&mut values, item)
                    .map_err(|_| S2ComponentCoverError::Allocation)?;
                Ok::<_, S2ComponentCoverError>(values)
            })?;
    } else {
        tagged.sort_unstable_by_key(|(cell, _)| *cell);
    }
    let mut merged = Vec::new();
    crate::try_reserve_vec(&mut merged, tagged.len())
        .map_err(|_| S2ComponentCoverError::Allocation)?;
    for (cell, interior) in tagged {
        if let Some((last, last_interior)) = merged.last_mut()
            && *last == cell
        {
            *last_interior |= interior;
        } else {
            crate::try_push_vec(&mut merged, (cell, interior))
                .map_err(|_| S2ComponentCoverError::Allocation)?;
        }
    }
    Ok(merged)
}

/// Cover the semantic source in one adaptive traversal.
///
/// `target_cells` is a quality target for the covered region, not an amount to
/// divide between syntactic members. Passing a collection wholesale preserves
/// the same source predicate for a bare polygon, a one-member wrapper, that
/// polygon plus a contained line, and equivalent ring traversals. The bbox
/// spelling still canonically decomposes disjoint longitude windows at its
/// own owner below; it is a request for envelopes rather than this source set.
pub(super) fn cover_s2_components(
    source: &Shape,
    min_level: u8,
    max_level: u8,
    level_mod: u8,
    max_cells: Option<usize>,
    target_cells: usize,
    rule: CoverRule,
) -> Result<Vec<(CellId, bool)>, S2ComponentCoverError> {
    let mut components = canonical_s2_components(source)?;
    let canonical_source = match components.len() {
        0 => return Ok(Vec::new()),
        1 => components.pop().expect("one canonical S2 component"),
        _ => Shape::geometry_collection(components).map_err(S2ComponentCoverError::Preparation)?,
    };
    cover_s2_component(
        &canonical_source,
        min_level,
        max_level,
        level_mod,
        max_cells,
        target_cells,
        rule,
    )
    .map(Covering::into_cells)
}

/// Select visible cells from a tagged coverer emission.
fn membership_from_tagged(
    tagged: Vec<(CellId, bool)>,
    cell_rule: CellRule,
    bbox_visible: Option<Vec<CellId>>,
) -> Result<Vec<CellId>, GridCoverError> {
    if cell_rule == CellRule::Bbox {
        return Ok(bbox_visible.expect("bbox cells were built"));
    }
    let mut cells = Vec::new();
    if cell_rule == CellRule::Overlap {
        crate::try_reserve_vec(&mut cells, tagged.len())?;
    }
    match cell_rule {
        CellRule::Bbox => unreachable!("bbox membership returned before allocation"),
        // Rule-specific visibility was decided by the cover traversal. The
        // tagged stream therefore contains only retained, budgeted result
        // cells.
        CellRule::Overlap | CellRule::Within | CellRule::Center => {
            for (cell, _) in tagged {
                crate::try_push_vec(&mut cells, cell)?;
            }
        },
    }
    Ok(cells)
}

/// Build an exact-classified S2 coverage of `geometry` (the
/// ``s2_cover(...)`` backend).
///
/// Visible cells come from the coverer for `cell_rule` only; the tagged
/// emission has already applied its visibility and finite-budget policy.
pub(crate) fn build_coverage(
    geometry: &PyGeometry,
    budget: S2LevelBudget,
    cell_rule: CellRule,
) -> Result<Vec<CellId>, GridCoverError> {
    let cover_shape = coverage_factory_shapes(geometry, "S2")?;
    let (tagged, bbox_visible) = if cell_rule == CellRule::Bbox {
        let windows = envelope_window_shapes(&cover_shape)?;
        let mut cells = Vec::new();
        for (cell, _) in cover_s2_shapes(
            windows,
            budget.min_level,
            budget.max_level,
            budget.level_mod,
            budget.max_cells,
            budget.target_cells,
            CoverRule::Overlap,
        )? {
            crate::try_push_vec(&mut cells, cell)?;
        }
        (Vec::new(), Some(cells))
    } else {
        let rule = match cell_rule {
            CellRule::Overlap => CoverRule::Overlap,
            CellRule::Within => CoverRule::Within,
            CellRule::Center => CoverRule::Center,
            CellRule::Bbox => unreachable!("bbox uses window covering"),
        };
        let tagged = cover_s2_components(
            geometry.shape.as_ref(),
            budget.min_level,
            budget.max_level,
            budget.level_mod,
            budget.max_cells,
            budget.target_cells,
            rule,
        )?;
        (tagged, None)
    };

    let cells = membership_from_tagged(tagged, cell_rule, bbox_visible)?;
    Ok(cells)
}

/// Register the S2 classes, flat functions, and pickle rebuilder.
pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    crate::add_functions!(m;
        s2_cells, s2_cover, s2_union,
        s2_intersection, s2_difference, s2_bounding_cell,
        _unpickle_s2_cell,
    );
    crate::add_classes!(m; PyS2Cell);
    Ok(())
}
