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
        }
    }
}

/// Failure that can occur while building a union of atomic S2 covers.
///
#[derive(Debug)]
pub(super) enum S2ComponentCoverError {
    Budget(crate::grid::CoverBudgetExceeded),
    Preparation(crate::error::Error),
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
fn s2_component_key(component: Component<'_>) -> Vec<u64> {
    fn append_points(key: &mut Vec<u64>, points: impl IntoIterator<Item = crate::geometry::Point>) {
        for point in points {
            key.push(point.x.to_bits());
            key.push(point.y.to_bits());
        }
    }
    let mut key = Vec::new();
    match component {
        Component::Point(point) => {
            key.reserve(3);
            key.push(0);
            key.push(point.x.to_bits());
            key.push(point.y.to_bits());
        },
        Component::LineString(line) => {
            key.reserve(
                line.len()
                    .checked_mul(2)
                    .and_then(|words| words.checked_add(2))
                    .expect("an existing coordinate sequence has a representable key length"),
            );
            key.push(1);
            key.push(line.len() as u64);
            append_points(&mut key, line.iter());
        },
        Component::Polygon(polygon) => {
            key.reserve(
                polygon
                    .shell
                    .coord_count()
                    .checked_mul(2)
                    .and_then(|words| words.checked_add(3))
                    .expect("an existing polygon shell has a representable key length"),
            );
            key.push(2);
            key.push(polygon.holes.len() as u64);
            key.push(polygon.shell.coord_count() as u64);
            append_points(&mut key, polygon.shell.iter());
            for hole in polygon.holes.iter() {
                key.reserve(
                    hole.coord_count()
                        .checked_mul(2)
                        .and_then(|words| words.checked_add(1))
                        .expect("an existing polygon hole has a representable key length"),
                );
                key.push(hole.coord_count() as u64);
                append_points(&mut key, hole.iter());
            }
        },
    }
    key
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
    source.for_each_component(&mut |component| {
        components.push((s2_component_key(component), component.to_shape()));
    });
    components.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    components.dedup_by(|left, right| left.0 == right.0);

    let bounds: Vec<_> = components.iter().map(|(_, shape)| shape.bounds()).collect();
    let indexed_bounds = bounds
        .iter()
        .enumerate()
        .filter_map(|(entry, bounds)| {
            bounds.map(|bounds| IndexedS2Component {
                index: entry,
                envelope: AABB::from_corners([bounds.minx(), bounds.miny()], [
                    bounds.maxx(),
                    bounds.maxy(),
                ]),
            })
        })
        .collect();
    let index = RTree::bulk_load(indexed_bounds);

    // Deterministic raw ordering makes equal supports choose one stable
    // representative. Keep only maximal source supports. The R-tree is a
    // broad phase only: `Shape::covers` is still the exact source predicate,
    // never a cell or chord proxy. A component can be discarded only when an
    // exact larger/equal support covers it; the lower key wins an equal pair.
    let mut canonical = Vec::with_capacity(components.len());
    for (candidate_index, (_, candidate)) in components.iter().enumerate() {
        let Some(candidate_bounds) = bounds[candidate_index] else {
            canonical.push(candidate.clone());
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
            canonical.push(candidate.clone());
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
        vec![target_cells; components.len()]
    } else {
        let floors = components
            .iter()
            .map(|component| -> Result<usize, S2ComponentCoverError> {
                Ok(cover_s2_component(
                    component, min_level, max_level, level_mod, max_cells, 1, rule,
                )?
                .into_cells()
                .len())
            })
            .collect::<Result<Vec<_>, S2ComponentCoverError>>()?;
        let floor_total = floors
            .iter()
            .try_fold(0_usize, |total, &floor| total.checked_add(floor))
            .ok_or_else(|| {
                S2ComponentCoverError::Preparation(ResourceError::SizeOverflow.into())
            })?;
        let remaining = target_cells.saturating_sub(floor_total);
        let per_component = remaining / components.len();
        let remainder = remaining % components.len();
        floors
            .into_iter()
            .enumerate()
            .map(|(index, floor)| floor + per_component + usize::from(index < remainder))
            .collect()
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
        )?
        .into_cells();
        if let Some(unique) = &mut unique {
            for (cell, interior) in covered {
                if let Some(existing) = unique.get_mut(&cell) {
                    *existing |= interior;
                    continue;
                }
                let limit = max_cells.expect("unique map has a finite budget");
                crate::grid::ensure_cover_budget(unique.len().saturating_add(1), Some(limit))
                    .map_err(S2ComponentCoverError::Budget)?;
                unique.insert(cell, interior);
            }
        } else {
            tagged.reserve(covered.len());
            tagged.extend(covered);
        }
    }

    if let Some(unique) = unique {
        tagged = unique.into_iter().collect();
    } else {
        tagged.sort_unstable_by_key(|(cell, _)| *cell);
    }
    let mut merged = Vec::with_capacity(tagged.len());
    for (cell, interior) in tagged {
        if let Some((last, last_interior)) = merged.last_mut()
            && *last == cell
        {
            *last_interior |= interior;
        } else {
            merged.push((cell, interior));
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
) -> Vec<CellId> {
    if cell_rule == CellRule::Bbox {
        return bbox_visible.expect("bbox cells were built");
    }
    debug_assert!(matches!(
        cell_rule,
        CellRule::Overlap | CellRule::Within | CellRule::Center
    ));
    // Rule-specific visibility was decided by the cover traversal. The tagged
    // stream therefore contains only retained, budgeted result cells.
    tagged.into_iter().map(|(cell, _)| cell).collect()
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
        let cells = cover_s2_shapes(
            windows,
            budget.min_level,
            budget.max_level,
            budget.level_mod,
            budget.max_cells,
            budget.target_cells,
            CoverRule::Overlap,
        )?
        .into_iter()
        .map(|(cell, _)| cell)
        .collect();
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

    Ok(membership_from_tagged(tagged, cell_rule, bbox_visible))
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
