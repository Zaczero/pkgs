use std::sync::Arc;

use pyo3::exceptions::PyMemoryError;
use rstar::{AABB, RTree, RTreeObject};

use crate::geometry::{Coordinates as _, Shape};
use crate::grid::affine_source::{
    GridAffineSource, SphericalGridTarget, WorkingShapeRelation, visit_grid_cover_components,
};
use crate::grid::s2::cell::Cell as S2GeomCell;
use crate::grid::s2::cellid::CellId;
use crate::grid::s2::coverer::{Coverer, Covering};
use crate::py::cells::coverage_ops::{self, CoverageCells, coverage_factory_shapes};
use crate::py::cells::s2::functions::{
    _unpickle_s2_cell, _unpickle_s2_coverage, s2_bounding_cell, s2_cells, s2_cover, s2_difference,
    s2_intersection, s2_union,
};
use crate::py::cells::s2::parse::{bounds_query_shape, parse_s2_level_budget};
use crate::py::cells::s2::{PyS2Cell, PyS2Coverage, PyS2CoverageIter, S2Membership};
use crate::py::cells::{
    Bound, CellRule, PyAny, PyGeometry, PyModule, PyModuleMethods as _, PyResult,
};

/// Level-budget knobs shared by cover construction and lazy membership.
struct S2Budget {
    min_level: u8,
    max_level: u8,
    level_mod: u8,
    max_cells: Option<usize>,
    target_cells: usize,
}

/// Failure that can occur while building a union of atomic S2 covers.
///
/// The cover budget is cacheable by lazy membership; allocation is transient
/// and must remain a Python memory error rather than a poisoned partition.
#[derive(Clone, Copy, Debug)]
pub(super) enum S2ComponentCoverError {
    Budget(crate::grid::CoverBudgetExceeded),
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
) -> Result<Covering, S2ComponentCoverError> {
    let (working, relation) = if component.crosses_antimeridian() {
        (
            component
                .split_antimeridian()
                .map_err(|_| S2ComponentCoverError::Allocation)?,
            WorkingShapeRelation::AntimeridianSplit,
        )
    } else {
        (component.clone(), WorkingShapeRelation::Identity)
    };
    let affine = GridAffineSource::new(component, SphericalGridTarget::S2)
        .map_err(|_| S2ComponentCoverError::Allocation)?;
    Coverer {
        min_level,
        max_level,
        level_mod,
        max_cells,
        target_cells,
    }
    .cover(&working, &affine, relation)
    .map_err(S2ComponentCoverError::Budget)
}

/// S2's covering target is global, so aggregate syntax cannot decide either
/// the component ordering or the share of an adaptive target.  The key is the
/// full XY topology that reaches the grid (Z/M are deliberately absent): it is
/// an in-process ordering key, never a serialized geometry identity.
fn s2_component_key(shape: &Shape) -> Vec<u64> {
    fn append_points(key: &mut Vec<u64>, points: impl IntoIterator<Item = crate::geometry::Point>) {
        for point in points {
            key.extend([point.x.to_bits(), point.y.to_bits()]);
        }
    }
    match shape {
        Shape::Point(point) => vec![0, point.x.to_bits(), point.y.to_bits()],
        Shape::LineString(line) => {
            let mut key = vec![1, line.len() as u64];
            append_points(&mut key, line.iter());
            key
        },
        Shape::Polygon(polygon) => {
            let mut key = vec![
                2,
                polygon.holes.len() as u64,
                polygon.shell.coord_count() as u64,
            ];
            append_points(&mut key, polygon.shell.iter());
            for hole in polygon.holes.iter() {
                key.push(hole.coord_count() as u64);
                append_points(&mut key, hole.iter());
            }
            key
        },
        Shape::MultiPoint(..)
        | Shape::MultiLineString(..)
        | Shape::MultiPolygon(..)
        | Shape::GeometryCollection(..)
        | Shape::Empty(..) => unreachable!("S2 cover components must be atomic"),
    }
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
    visit_grid_cover_components(source, &mut |component| {
        components.push((s2_component_key(component), component.clone()));
        Ok(())
    })?;
    components.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    components.dedup_by(|left, right| left.0 == right.0);

    let bounds: Vec<_> = components.iter().map(|(_, shape)| shape.bounds()).collect();
    let index = RTree::bulk_load(
        bounds
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
            .collect(),
    );

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
        let subsumed = index
            .locate_in_envelope_intersecting(candidate_envelope)
            .any(|support| {
                if support.index == candidate_index {
                    return false;
                }
                let Some(support_bounds) = bounds[support.index] else {
                    return false;
                };
                if !bounds_contain(support_bounds, candidate_bounds)
                    || !components[support.index].1.covers(candidate)
                {
                    return false;
                }
                // Topologically equal supports cover each other; retain only
                // the deterministically earlier raw key. A strict superset
                // always wins regardless of its raw ordering.
                support.index < candidate_index || !candidate.covers(&components[support.index].1)
            });
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
) -> Result<Vec<(CellId, bool)>, S2ComponentCoverError> {
    if components.is_empty() {
        return Ok(Vec::new());
    }

    // `target_cells` describes the returned coverage, not every syntactic
    // member of a Multi* carrier.  First measure each component's irreducible
    // min-level emission, then apportion only the remaining global budget.
    // An adaptive target below that floor remains a guide, not a rejection.
    let component_targets = if min_level == max_level {
        vec![target_cells; components.len()]
    } else {
        let mut floors = Vec::with_capacity(components.len());
        for component in &components {
            floors.push(
                cover_s2_component(component, min_level, max_level, level_mod, max_cells, 1)?
                    .into_cells()
                    .len(),
            );
        }
        let floor_total: usize = floors.iter().sum();
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
    for (component, component_target) in components.iter().zip(component_targets) {
        let covered = cover_s2_component(
            component,
            min_level,
            max_level,
            level_mod,
            max_cells,
            component_target,
        )?;
        tagged.extend(covered.into_cells());
    }

    tagged.sort_unstable_by_key(|(cell, _)| *cell);
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
    if min_level == max_level
        && let Some(limit) = max_cells
        && merged.len() > limit
    {
        return Err(S2ComponentCoverError::Budget(
            crate::grid::CoverBudgetExceeded::new(limit),
        ));
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
) -> Result<Vec<(CellId, bool)>, S2ComponentCoverError> {
    let mut components = canonical_s2_components(source)?;
    let canonical_source = match components.len() {
        0 => return Ok(Vec::new()),
        1 => components.pop().expect("one canonical S2 component"),
        _ => Shape::GeometryCollection(components),
    };
    cover_s2_component(
        &canonical_source,
        min_level,
        max_level,
        level_mod,
        max_cells,
        target_cells,
    )
    .map(Covering::into_cells)
}

/// Select visible cells + lazy/seeded membership from a tagged coverer emission.
fn membership_from_tagged(
    tagged: Vec<(CellId, bool)>,
    cover_shape: Shape,
    cover_is_split: bool,
    budget: &S2Budget,
    cell_rule: CellRule,
    bbox_visible: Option<Vec<CellId>>,
) -> (Arc<S2Membership>, CoverageCells<PyS2Cell>) {
    let make_lazy = |shape: Shape| {
        S2Membership::lazy(
            shape,
            cover_is_split,
            budget.min_level,
            budget.max_level,
            budget.level_mod,
            budget.target_cells,
        )
    };
    match cell_rule {
        // Overlap already builds the annotated product — seed the inspection
        // partition so interior/boundary do not re-cover.
        CellRule::Overlap => {
            let partition = S2Membership::partition_from_covering(tagged);
            let cells = partition.all();
            (
                S2Membership::seeded(
                    partition,
                    cover_shape,
                    cover_is_split,
                    budget.min_level,
                    budget.max_level,
                    budget.level_mod,
                    budget.target_cells,
                ),
                cells,
            )
        },
        CellRule::Bbox => {
            let cells = CoverageCells::from_cells(
                bbox_visible
                    .expect("bbox cells were built")
                    .into_iter()
                    .map(|cell| PyS2Cell { cell })
                    .collect(),
            );
            (make_lazy(cover_shape), cells)
        },
        CellRule::Within => {
            let cells = CoverageCells::from_cells(
                tagged
                    .into_iter()
                    .filter_map(|(cell, interior)| interior.then_some(PyS2Cell { cell }))
                    .collect(),
            );
            (make_lazy(cover_shape), cells)
        },
        // Center: interior is a full-cover certificate (source.covers(rect)),
        // so the cell center is covered; only boundary cells probe the cover
        // working shape (split-normalized). Partition stays cold.
        CellRule::Center => {
            let cells = CoverageCells::from_cells(
                tagged
                    .into_iter()
                    .filter_map(|(cell, interior)| {
                        (interior
                            || cover_shape.covers_point(S2GeomCell::from_id(cell).center_lonlat()))
                        .then_some(PyS2Cell { cell })
                    })
                    .collect(),
            );
            (make_lazy(cover_shape), cells)
        },
    }
}

/// Build an exact-classified S2 coverage of `geometry` (the
/// ``s2_cover(...)`` backend).
///
/// Visible cells come from the coverer for `cell_rule` only. The inspection
/// partition is lazy across all grids (seeded immediately only for the
/// overlap rule, which already produces the annotated product).
pub(crate) fn build_coverage(
    geometry: &PyGeometry,
    level: Option<&Bound<'_, PyAny>>,
    max_cells: Option<i64>,
    target_cells: i64,
    min_level: Option<&Bound<'_, PyAny>>,
    max_level: Option<&Bound<'_, PyAny>>,
    level_mod: i64,
    cell_rule: CellRule,
) -> PyResult<PyS2Coverage> {
    let parsed = parse_s2_level_budget(
        level,
        max_cells,
        target_cells,
        min_level,
        max_level,
        level_mod,
    )?;
    let budget = S2Budget {
        min_level: parsed.min_level,
        max_level: parsed.max_level,
        level_mod: parsed.level_mod,
        max_cells: parsed.max_cells,
        target_cells: parsed.target_cells,
    };
    let (membership_geometry, cover_shape, cover_is_split) =
        coverage_factory_shapes(geometry, "S2")?;
    let (tagged, bbox_visible) = if cell_rule == CellRule::Bbox {
        // Cover each non-wrapped lon/lat window separately and deduplicate.
        // A multipolygon / multipartite seam band has full-world merged bounds
        // (-180..180) even when each part is a narrow half — covering that
        // merged envelope floods the equator with unrelated cells.
        // Eliminate semantically redundant *source* components before turning
        // them into longitude windows.  Windows are implementation pieces of
        // one bounds request, not source supports themselves: applying
        // `covers` after the split can incorrectly discard distinct polar
        // pieces (and changes a one-member MultiPolygon's result).
        let source_components = canonical_s2_components(&cover_shape)
            .map_err(|_| PyMemoryError::new_err("S2 coverage allocation failed"))?;
        let mut windows = Vec::new();
        for component in &source_components {
            windows.extend(bbox_cover_windows(component)?);
        }
        let cells = cover_s2_shapes(
            windows,
            budget.min_level,
            budget.max_level,
            budget.level_mod,
            budget.max_cells,
            budget.target_cells,
        )
        .map_err(|error| match error {
            S2ComponentCoverError::Budget(error) => coverage_ops::cover_budget_err(error),
            S2ComponentCoverError::Allocation => {
                PyMemoryError::new_err("S2 coverage allocation failed")
            },
        })?
        .into_iter()
        .map(|(cell, _)| cell)
        .collect();
        (Vec::new(), Some(cells))
    } else {
        let tagged = cover_s2_components(
            membership_geometry.shape.as_ref(),
            budget.min_level,
            budget.max_level,
            budget.level_mod,
            budget.max_cells,
            budget.target_cells,
        )
        .map_err(|error| match error {
            S2ComponentCoverError::Budget(error) => coverage_ops::cover_budget_err(error),
            S2ComponentCoverError::Allocation => {
                PyMemoryError::new_err("S2 coverage allocation failed")
            },
        })?;
        (tagged, None)
    };

    let (membership, cells) = membership_from_tagged(
        tagged,
        cover_shape,
        cover_is_split,
        &budget,
        cell_rule,
        bbox_visible,
    );
    Ok(PyS2Coverage {
        geometry: membership_geometry,
        cells,
        cell_rule,
        min_level: budget.min_level,
        max_level: budget.max_level,
        level_mod: budget.level_mod,
        max_cells: budget.max_cells,
        target_cells: budget.target_cells,
        membership,
    })
}

/// Non-wrapped lon/lat envelopes for the bbox cover rule.
///
/// Every canonical atomic component contributes its own windows, so bbox
/// follows the same aggregate-decomposition owner as the other S2 rules. A
/// single envelope with west > east (geographic wrap convention) splits into
/// two boxes at ±180. Ordinary ordered bounds stay one window.
fn bbox_cover_windows(shape: &Shape) -> PyResult<Vec<Shape>> {
    let mut windows = Vec::new();
    visit_grid_cover_components(shape, &mut |component| {
        push_bounds_windows(component.bounds(), &mut windows)
    })?;
    if windows.is_empty() {
        Err(coverage_ops::empty_coverage_err("S2"))
    } else {
        Ok(windows)
    }
}

fn push_bounds_windows(
    bounds: Option<crate::geometry::Bounds>,
    out: &mut Vec<Shape>,
) -> PyResult<()> {
    let Some(bounds) = bounds else {
        return Ok(());
    };
    let west = bounds.minx();
    let south = bounds.miny();
    let east = bounds.maxx();
    let north = bounds.maxy();
    if west > east {
        // Antimeridian wrap: cover [west, 180] and [-180, east] separately.
        out.push(bounds_query_shape(crate::geometry::Bounds::new_unchecked(
            west, south, 180.0, north,
        ))?);
        out.push(bounds_query_shape(crate::geometry::Bounds::new_unchecked(
            -180.0, south, east, north,
        ))?);
    } else {
        out.push(bounds_query_shape(bounds)?);
    }
    Ok(())
}

/// Register the S2 classes, flat functions, and pickle rebuilder.
pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    crate::add_functions!(m;
        s2_cells, s2_cover, s2_union,
        s2_intersection, s2_difference, s2_bounding_cell,
        _unpickle_s2_cell, _unpickle_s2_coverage,
    );
    crate::add_classes!(m; PyS2Coverage, PyS2CoverageIter, PyS2Cell);
    Ok(())
}
