//! Spatial index `PyO3` surface — `SpatialIndex` methods, CRS-aware metric
//! model, nearest-neighbor search, and the free `index`/`nearest`/`join`
//! functions.
//!
//! Predicate facts come from the unified [`Predicate`] table
//! (`crate::predicates::engine`), and exact refinement runs through the shared
//! [`scalar_vs_shapes`] batch engine, so the index cannot drift from the
//! broadcast/prepared surfaces. Geographic nearest/`dwithin` queries prune the
//! R-tree with the sound geodesic lower bound in [`geodesic`].

mod geodesic;
mod str_tree;

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;

use geodesic::{GeodesicPruner, GeodesicRowCaps};
use pyo3::prelude::*;
use pyo3::types::PyModule;
use rstar::{AABB, ParentNode, PointDistance, RTree, RTreeNode, RTreeObject};
use str_tree::StaticStrTree;

use crate::collections::sort_row_ids;
use crate::geometry::{
    Bounds, Point, PointBatchTester, Shape, ShapeData, XY, convex_box_strictly_inside,
};
use crate::py::errors::{GeometryError, parameter_error};
use crate::py::numpy::{float64_array, int64_array, usize_array};
use crate::py::vectors::Groups;
use crate::{
    Crs, DistanceUnit, Frame, GeometryArrayStorage, HeapSize, IndexEnvelope, NonNegative,
    PointRows, Predicate, PyGeometry, PyGeometryArray, ShapeRow, bounds_envelope, crs, crs_label,
    epoch_label, exact_geometry, exact_geometry_array, geometry_items,
    global_geographic_candidate_envelope, index_bounds, index_envelope, pair_distance_resolved,
    pair_dwithin_resolved, point_from_bounds, point_index_envelope, resolve_metric,
    scalar_vs_shapes, topology_scalar_pair, validate_distance_arg, validate_nearest_k,
};

mod build;
mod free_functions;
mod impl_mutate;
mod impl_nearest;
mod impl_query;
mod nearest_helpers;
mod pymethods;

pub(crate) use build::{build_spatial_index, index_metadata, restore_spatial_index, spatial_index};
pub(crate) use free_functions::{join, join_indexed, nearest};
pub(crate) use nearest_helpers::{
    CapBound, Frontier, FrontierNode, NearestCandidate, RowMatches, aabb_box_distance_2,
    collect_subtree_ids, format_nearest, format_nearest_rows, nearest_candidates_from_heap,
    parse_max_distance, push_nearest_candidate, retain_row,
};
pub(crate) use pymethods::{_unpickle_spatial_index, PySpatialIndexIter};
/// metric `dwithin`.
///
/// One parser owns the token vocabulary for
/// `query`/`query_pairs`/`explain`/`join`, and the topological side reads its
/// facts from [`Predicate::spec`], so the index cannot drift from the
/// broadcast/prepared surfaces in which predicates it accepts or how they
/// refine.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum IndexPredicate {
    Topological(Predicate),
    Dwithin,
}

impl IndexPredicate {
    /// Whether the relation is symmetric (`p(a, b) == p(b, a)`). Directional
    /// predicates cannot be reported as unordered `i < j` pairs without
    /// silently dropping the reverse direction.
    pub(crate) fn is_symmetric(self) -> bool {
        match self {
            Self::Topological(predicate) => predicate.spec().symmetric,
            Self::Dwithin => true,
        }
    }

    /// Parse a predicate token, rejecting unknown and non-indexable values.
    pub(crate) fn parse(token: &str) -> PyResult<Self> {
        if token == "dwithin" {
            return Ok(Self::Dwithin);
        }
        let Some(predicate) = Predicate::parse(token) else {
            return Err(parameter_error(
                crate::tokens::unknown_token_message("predicate", token, &[
                    "intersects",
                    "contains",
                    "contains_properly",
                    "covers",
                    "within",
                    "covered_by",
                    "equals",
                    "dwithin",
                    "touches",
                    "crosses",
                    "overlaps",
                ]),
                "predicate",
            ));
        };
        if predicate.spec().index_envelope.is_none() {
            return Err(GeometryError::new_err(format!(
                "predicate {token:?} cannot be index-accelerated (it matches everything far \
                 away); use the top-level gm.{token} predicate instead"
            )));
        }
        Ok(Self::Topological(predicate))
    }

    /// Parse an optional token (`None` = candidate-only / index default).
    pub(crate) fn parse_opt(token: Option<&str>) -> PyResult<Option<Self>> {
        token.map(Self::parse).transpose()
    }

    /// The canonical token (inverse of [`parse`](Self::parse)).
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Topological(predicate) => predicate.token(),
            Self::Dwithin => "dwithin",
        }
    }

    /// `dwithin` is the only predicate that consumes a `distance` argument.
    pub(crate) fn requires_distance(self) -> bool {
        self == Self::Dwithin
    }
}

pub(crate) fn dwithin_candidate_step(
    index: &PySpatialIndex,
    metric: &crs::MetricModel,
    query: Option<&Shape>,
    distance: NonNegative,
) -> String {
    let distance = distance.get();
    if metric.coordinate_radius(distance).is_some() {
        return format!("bounds envelope candidate filter expanded by {distance}");
    }
    if query.is_some_and(|shape| {
        index
            .geodesic_pruner(metric, shape_pruner_point(shape))
            .is_some()
    }) {
        return "geodesic lower-bound candidate window".to_owned();
    }
    if index.non_prunable_live == 0 {
        return "geodesic lower-bound candidate window (point queries; global scan otherwise)"
            .to_owned();
    }
    "global candidate scan (exact geodesic distances)".to_owned()
}

/// Resolve the `distance` argument for a query: `dwithin` requires one,
/// topological predicates reject one (exact refinement has no tolerance — a
/// stray `distance` would silently read as one), and candidate-only calls
/// (`predicate=None`) accept it as an envelope expansion.
pub(crate) fn query_distance(
    predicate: Option<IndexPredicate>,
    distance: Option<&Bound<'_, PyAny>>,
) -> PyResult<Option<NonNegative>> {
    let Some(distance) = distance else {
        if predicate.is_some_and(IndexPredicate::requires_distance) {
            return Err(GeometryError::new_err(
                "predicate 'dwithin' requires a non-negative distance",
            ));
        }
        return Ok(None);
    };
    if let Some(IndexPredicate::Topological(predicate)) = predicate {
        return Err(GeometryError::new_err(format!(
            "distance is only valid with predicate='dwithin'; '{}' is an exact relation with \
             no distance tolerance (use candidates(...) for an expanded envelope prefilter)",
            predicate.token(),
        )));
    }
    Ok(Some(validate_distance_arg(distance)?))
}

/// Validated, frame-level query state shared by scalar, bulk, join, self-join,
/// candidate-only, and explain lanes. Geometry-specific bounds/pruner state is
/// prepared separately per row so hot bulk loops reuse predicate and metric
/// resolution without allocating a `PyGeometry` wrapper.
pub(crate) enum PreparedIndexQuery {
    /// Candidate-only query without a distance expansion.
    Candidates,
    /// Candidate-only query with a resolved metric distance expansion.
    CandidatesDwithin {
        distance: NonNegative,
        metric: crs::MetricModel,
    },
    /// Exact topology refinement needs no metric state.
    Topological { predicate: Predicate },
    /// Exact distance refinement always carries every metric prerequisite.
    Dwithin {
        distance: NonNegative,
        metric: crs::MetricModel,
        resolved: crs::ResolvedMetric,
    },
}

#[derive(Clone, Copy)]
pub(crate) struct NearestOptions {
    pub(crate) exclude_equal: bool,
    pub(crate) include_ties: bool,
}

pub(crate) struct PreparedIndexRow {
    pub(crate) bounds: Option<Bounds>,
    pub(crate) pruner_point: Option<Point>,
    pub(crate) cap: Option<(Point, f64)>,
}

impl PreparedIndexQuery {
    pub(crate) const fn candidate_parts(
        &self,
    ) -> (
        Option<IndexPredicate>,
        Option<NonNegative>,
        Option<&crs::MetricModel>,
    ) {
        match self {
            Self::Candidates => (None, None, None),
            Self::CandidatesDwithin { distance, metric } => (None, Some(*distance), Some(metric)),
            Self::Topological { predicate } => {
                (Some(IndexPredicate::Topological(*predicate)), None, None)
            },
            Self::Dwithin {
                distance, metric, ..
            } => (Some(IndexPredicate::Dwithin), Some(*distance), Some(metric)),
        }
    }

    pub(crate) fn for_index(
        index: &PySpatialIndex,
        predicate: Option<IndexPredicate>,
        distance: Option<NonNegative>,
        unit: Option<DistanceUnit>,
    ) -> PyResult<Self> {
        let crs = index.metadata.as_ref().and_then(Frame::crs_ref);
        let epoch = index.metadata.as_ref().and_then(Frame::epoch);
        Self::for_frame(
            index,
            crs,
            epoch,
            index.metadata.as_ref().and_then(Frame::crs_str),
            predicate,
            distance,
            unit,
        )
    }

    pub(crate) fn for_geometry(
        index: &PySpatialIndex,
        geometry: &PyGeometry,
        predicate: Option<IndexPredicate>,
        distance: Option<NonNegative>,
        unit: Option<DistanceUnit>,
    ) -> PyResult<Self> {
        index.ensure_query_compatible(geometry, "spatial index query")?;
        Self::for_frame(
            index,
            geometry.crs_ref(),
            geometry.epoch(),
            index.metric_crs_str(geometry.crs_str()),
            predicate,
            distance,
            unit,
        )
    }

    pub(crate) fn for_array(
        index: &PySpatialIndex,
        array: &PyGeometryArray,
        predicate: Option<IndexPredicate>,
        distance: Option<NonNegative>,
        unit: Option<DistanceUnit>,
    ) -> PyResult<Self> {
        Self::for_frame(
            index,
            array.crs_ref(),
            array.epoch(),
            index.metric_crs_str(array.crs_str()),
            predicate,
            distance,
            unit,
        )
    }

    fn for_frame(
        index: &PySpatialIndex,
        crs: Option<&Crs>,
        epoch: Option<f64>,
        metric_crs: Option<&str>,
        predicate: Option<IndexPredicate>,
        distance: Option<NonNegative>,
        unit: Option<DistanceUnit>,
    ) -> PyResult<Self> {
        index.ensure_frame_compatible(crs, epoch, "spatial index query")?;
        match (predicate, distance) {
            (None, None) => Ok(Self::Candidates),
            (None, Some(distance)) => Ok(Self::CandidatesDwithin {
                distance,
                metric: resolve_metric(metric_crs, unit, "spatial index distance query")?,
            }),
            (Some(IndexPredicate::Topological(predicate)), None) => {
                Ok(Self::Topological { predicate })
            },
            (Some(IndexPredicate::Dwithin), Some(distance)) => {
                let metric = resolve_metric(metric_crs, unit, "spatial index distance query")?;
                let resolved = crs::ResolvedMetric::from_model(&metric)?;
                Ok(Self::Dwithin {
                    distance,
                    metric,
                    resolved,
                })
            },
            // `query_distance` owns both public argument invariants. Keep the
            // internal boundary fallible too, rather than representing either
            // impossible state with four independent Options.
            (Some(IndexPredicate::Dwithin), None) => Err(GeometryError::new_err(
                "predicate 'dwithin' requires a non-negative distance",
            )),
            (Some(IndexPredicate::Topological(predicate)), Some(_)) => {
                Err(GeometryError::new_err(format!(
                    "distance is only valid with predicate='dwithin'; '{}' is an exact relation",
                    predicate.token()
                )))
            },
        }
    }

    /// Prepare one query row. `seeded_bounds` is the packed-array element
    /// bounds cache when available — avoids re-scanning shells that
    /// [`cached_element_bounds`] already computed.
    pub(crate) fn row(
        &self,
        index: &PySpatialIndex,
        row: ShapeRow<'_>,
        seeded_bounds: Option<Bounds>,
    ) -> PreparedIndexRow {
        let pruner_point = match row {
            ShapeRow::Point(point) => Some(point),
            _ => None,
        };
        // Gate shape materialization: only pay `with_shape` when a geodesic
        // distance cap is actually needed, or when geographic antimeridian
        // correction may apply. Candidates with `distance=None` on a
        // projected/CRS-free index never build a temporary polygon.
        let geodesic_metric = match self {
            Self::CandidatesDwithin {
                metric: crs::MetricModel::Geodesic(crs),
                ..
            }
            | Self::Dwithin {
                metric: crs::MetricModel::Geodesic(crs),
                ..
            } => Some(crs),
            _ => None,
        };
        let need_geodesic_cap = pruner_point.is_none() && geodesic_metric.is_some();
        let cap = match pruner_point {
            Some(point) => Some((point, 0.0)),
            None if need_geodesic_cap => row.with_shape(|shape| {
                geodesic_metric.and_then(|crs| {
                    crs::with_ellipsoid_metric(crs, &[shape], |ellipsoid| {
                        Ok(shape.geodesic_cap(ellipsoid))
                    })
                    .ok()
                    .flatten()
                })
            }),
            None => None,
        };
        let bounds = seeded_bounds.or_else(|| row.quick_bounds());
        let bounds = bounds
            .map(|bounds| row.with_shape(|shape| index_bounds(shape, bounds, index.geographic())));
        PreparedIndexRow {
            bounds,
            pruner_point,
            cap,
        }
    }

    pub(crate) fn candidate_step(&self, index: &PySpatialIndex, query: Option<&Shape>) -> String {
        match self {
            Self::CandidatesDwithin { distance, metric }
            | Self::Dwithin {
                distance, metric, ..
            } => dwithin_candidate_step(index, metric, query, *distance),
            Self::Topological { predicate }
                if predicate.spec().index_envelope == Some(IndexEnvelope::ContainedInQuery) =>
            {
                "bounds envelope containment filter".to_owned()
            },
            Self::Candidates | Self::Topological { .. } => {
                "bounds envelope candidate filter".to_owned()
            },
        }
    }
}
// ---- SpatialIndex pyclass + RTree entry + builders ----

#[derive(Debug, Default)]
pub(crate) struct GeodesicCapsCache {
    pub(crate) row_count: usize,
    /// Index mutation generation (insert/remove).
    pub(crate) generation: u64,
    /// CRS runtime config generation — ellipsoid/search-path reconfiguration
    /// must invalidate caps so a rebuilt table cannot mix the old ellipsoid
    /// with a new metric configuration.
    pub(crate) runtime_generation: u64,
    pub(crate) caps: Option<Arc<GeodesicRowCaps>>,
}

/// A packed STR-tree over geometries sharing one CRS/epoch frame.
///
/// Built by ``SpatialIndex(geoms)``: ask set questions against the indexed
/// geometries — exact predicate matches (``idx.query(geom)``), bounding-box
/// candidates (``idx.candidates(geom)``), proximity (``idx.nearest(geom)``),
/// self-joins (``idx.query_pairs()``) — and mutate it incrementally with
/// ``insert``/``remove``. Distances follow the indexed CRS: meters on a
/// geographic frame, native linear units on a projected frame, coordinate
/// units when CRS-free.
#[pyclass(
    name = "SpatialIndex",
    module = "gometry",
    immutable_type,
    skip_from_py_object
)]
#[derive(Debug)]
pub(crate) struct PySpatialIndex {
    // `rows` is append-only so a handle (its index) stays stable across mutation;
    // `remove` tombstones bulk handles (overflow entries delete), which
    // excludes them from every query.
    rows: IndexRows,
    /// The immutable STR-packed bulk (build-time rows; tombstone removal).
    bulk: StaticStrTree,
    /// Post-build inserts — a small dynamic R-tree (usually empty).
    overflow: RTree<IndexEntry>,
    /// Lazy per-row geodesic caps (anchor + proven aux reach), built on the
    /// first geodesic nearest over non-point rows. Each mutable index owns its
    /// cache: clones may subsequently diverge while retaining equal row counts
    /// and mutation generations, so sharing would make those values an
    /// insufficient cache identity.
    /// keyed by `(row count, mutation generation)` so inserts/removes
    /// invalidate. `(len, gen, None)` caches a known-unsound frame
    /// (out-of-domain rows / non-oblate ellipsoid).
    geodesic_caps: std::sync::Mutex<GeodesicCapsCache>,
    /// Bumped on every successful insert/remove so the geodesic cap cache
    /// invalidates when the live row set changes without a row-count change.
    mutation_gen: u64,
    // The CRS + coordinate epoch shared by every indexed geometry, learned from
    // the first item. Queries must match it (predicate/distance refinement
    // compares coordinates directly, so mixed frames would silently mis-match).
    // `None` until the first geometry is indexed.
    metadata: Option<Frame>,
    // Count of live geometries that are not points inside the lon/lat domain.
    // Geodesic lower-bound pruning is sound exactly when this is zero. Keeping
    // the count incrementally makes removal O(tree mutation), not O(live rows).
    non_prunable_live: usize,
}

impl Clone for PySpatialIndex {
    fn clone(&self) -> Self {
        Self {
            rows: self.rows.clone(),
            bulk: self.bulk.clone(),
            overflow: self.overflow.clone(),
            // A clone is independently mutable. Rebuilding this lazy cache is
            // cheap relative to risking caps derived from a divergent clone.
            geodesic_caps: std::sync::Mutex::default(),
            mutation_gen: self.mutation_gen,
            metadata: self.metadata.clone(),
            non_prunable_live: self.non_prunable_live,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct IndexEntry {
    idx: usize,
    envelope: AABB<[f64; 2]>,
}

/// Column-shared storage retained by an index built from a packed array.
/// The array remains the sole owner of packed row interpretation; the index
/// borrows rows through `GeometryArrayStorage::row` instead of cloning every
/// component and duplicating CSR/selection logic.
#[derive(Clone, Debug)]
pub(crate) struct PackedStore {
    pub(crate) rows: Arc<GeometryArrayStorage>,
    pub(crate) frame_caches: crate::array::FrameCacheRows,
}

/// The indexed rows. Building from a packed point or line array shares its
/// coordinate columns zero-copy (no per-row boxing); Mixed-array rows,
/// iterable rows, and every later `insert` are boxed handles. A handle is
/// its build/insert position: packed rows first, then boxed rows.
#[derive(Clone, Debug, Default)]
pub(crate) struct IndexRows {
    pub(crate) packed: Option<PackedStore>,
    boxed: Vec<PyGeometry>,
    /// O(1) liveness by stable handle. This mirrors the two physical index
    /// stores so lazy mapping iteration never searches the overflow R-tree.
    live: Vec<bool>,
}

impl IndexRows {
    fn len(&self) -> usize {
        let len = self.packed.as_ref().map_or(0, |packed| packed.rows.len()) + self.boxed.len();
        debug_assert_eq!(len, self.live.len());
        len
    }

    /// The row behind `handle` — a stack point / column-window line for
    /// packed rows, the persistent handle (prepared caches intact) for
    /// boxed rows.
    fn get(&self, handle: usize) -> Option<ShapeRow<'_>> {
        let packed_len = self.packed.as_ref().map_or(0, |packed| packed.rows.len());
        if let Some(packed) = &self.packed
            && handle < packed.rows.len()
        {
            return Some(packed.rows.row(handle));
        }
        self.boxed
            .get(handle - packed_len)
            .map(|item| ShapeRow::Handle(&item.shape))
    }

    fn row(&self, handle: usize) -> ShapeRow<'_> {
        let packed_len = self.packed.as_ref().map_or(0, |packed| packed.rows.len());
        if let Some(packed) = &self.packed
            && handle < packed.rows.len()
        {
            return packed.rows.row(handle);
        }
        ShapeRow::Handle(&self.boxed[handle - packed_len].shape)
    }

    fn frame_cache(&self, handle: usize) -> Arc<crate::geometry::FrameDependentCaches> {
        let packed_len = self.packed.as_ref().map_or(0, |packed| packed.rows.len());
        if let Some(packed) = &self.packed
            && handle < packed.rows.len()
        {
            return packed.frame_caches.cache(handle);
        }
        Arc::clone(&self.boxed[handle - packed_len].frame_cache)
    }

    fn push(&mut self, geometry: PyGeometry) -> usize {
        let handle = self.len();
        self.boxed.push(geometry);
        self.live.push(true);
        handle
    }

    fn is_live(&self, handle: usize) -> bool {
        self.live.get(handle).copied().unwrap_or(false)
    }

    fn mark_removed(&mut self, handle: usize) {
        debug_assert!(self.live[handle]);
        self.live[handle] = false;
    }

    #[expect(
        clippy::same_name_method,
        reason = "the inherent operation deliberately shares the domain vocabulary of its trait contract"
    )]
    fn heap_bytes(&self) -> usize {
        HeapSize::heap_bytes(self)
    }
}

impl PySpatialIndex {
    fn overflow_heap_bytes(&self) -> usize {
        fn node_bytes(node: &RTreeNode<IndexEntry>) -> usize {
            match node {
                RTreeNode::Leaf(_) => std::mem::size_of::<RTreeNode<IndexEntry>>(),
                RTreeNode::Parent(parent) => {
                    std::mem::size_of::<RTreeNode<IndexEntry>>()
                        + parent.children().iter().map(node_bytes).sum::<usize>()
                },
            }
        }

        self.overflow.root().children().iter().map(node_bytes).sum()
    }

    fn metadata_heap_bytes(&self) -> usize {
        self.metadata
            .as_ref()
            .and_then(Frame::crs_ref)
            .map_or(0, HeapSize::heap_bytes)
    }

    fn geodesic_caps_heap_bytes(&self) -> usize {
        self.geodesic_caps.heap_bytes()
    }

    fn retained_heap_bytes(&self) -> usize {
        self.rows.heap_bytes()
            + self.bulk.heap_bytes()
            + self.overflow_heap_bytes()
            + self.metadata_heap_bytes()
            + self.geodesic_caps_heap_bytes()
    }
}

impl HeapSize for GeodesicCapsCache {
    fn heap_bytes(&self) -> usize {
        self.caps.as_ref().map_or(0, HeapSize::heap_bytes)
    }
}

impl HeapSize for IndexRows {
    fn heap_bytes(&self) -> usize {
        self.packed
            .as_ref()
            .map_or(0, |packed| packed.rows.logical_heap_bytes())
            + self.boxed.heap_bytes()
            + self.live.heap_bytes()
    }
}

impl HeapSize for PySpatialIndex {
    fn heap_bytes(&self) -> usize {
        self.retained_heap_bytes()
    }
}
/// Register the spatial-index free functions on the module. The `SpatialIndex`
/// pyclass itself is registered with the core classes in the crate root.
pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    crate::add_functions!(m; nearest, join, _unpickle_spatial_index);
    crate::add_classes!(m; PySpatialIndex, PySpatialIndexIter);
    m.py()
        .import("collections.abc")?
        .getattr("Mapping")?
        .call_method1("register", (m.getattr("SpatialIndex")?,))?;
    Ok(())
}
/// Whether a shape is a point inside the lon/lat domain — see
/// `PySpatialIndex::non_prunable_live`.
pub(crate) fn geodesic_prunable_point(shape: &Shape) -> bool {
    matches!(shape, Shape::Point(point)
        if (-180.0..=180.0).contains(&point.x) && (-90.0..=90.0).contains(&point.y))
}

pub(crate) const fn shape_pruner_point(shape: &Shape) -> Option<Point> {
    match shape {
        Shape::Point(point) => Some(*point),
        _ => None,
    }
}
