//! Spatial index `PyO3` surface — `SpatialIndex` methods, CRS-aware metric
//! model, nearest-neighbor search, and the free `index`/`nearest`/`join`
//! functions.
//!
//! Predicate facts come from the unified [`Predicate`] table
//! (`crate::predicates::engine`), and exact refinement runs through the shared
//! the shared predicate matrix owner, so the index cannot drift from the
//! broadcast/prepared surfaces. Geographic nearest/`dwithin` queries prune the
//! immutable runs with the sound geodesic lower bound in [`geodesic`].

mod geodesic;
mod live_handles;
mod str_tree;

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;

use geodesic::{GeodesicPruner, GeodesicRowCaps};
use pyo3::prelude::*;
use rstar::AABB;
use str_tree::{StaticStrForest, StaticStrTree};

use crate::array::{ArrayRowOperation, BoundsSeed};
use crate::collections::sort_unique_row_ids;
use crate::geometry::{Bounds, Point, Shape, ShapeData, TopologyDomain};
use crate::py::errors::{GeometryError, parameter_error};
use crate::py::numpy::{float64_array, int64_array, usize_array};
use crate::py::vectors::Groups;
use crate::{
    Crs, DistanceUnit, DwithinDistance, Frame, GeometryArrayStorage, HeapSize, IndexEnvelope,
    NonNegative, PointRows, Predicate, PyGeometry, PyGeometryArray, ShapeRow, bounds_envelope, crs,
    crs_label, epoch_label, exact_geometry, exact_geometry_array, geometry_items,
    global_geographic_candidate_envelope, index_envelopes, pair_distance_geodesic,
    pair_distance_planar, pair_dwithin_geodesic, pair_dwithin_planar, point_from_bounds,
    resolve_metric, topology_pair_after_gates, topology_scalar_pair, validate_dwithin_distance_arg,
    validate_nearest_k,
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
    CapBound, Frontier, FrontierNode, NearestCandidate, RowMatches, aabb_box_distance_2, csr_rows,
    format_nearest, format_nearest_rows, nearest_candidates_from_heap, parse_max_distance,
    push_nearest_candidate, retain_row,
};
pub(crate) use pymethods::{_unpickle_spatial_index_v1, PySpatialIndexIter};
/// metric `dwithin`.
///
/// One parser owns the token vocabulary for
/// `query`/`self_join`/`join`, and the topological side reads its
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
    pub(crate) const fn is_symmetric(self) -> bool {
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

    /// The canonical token (inverse of [`parse`](Self::parse)).
    pub(crate) const fn label(self) -> &'static str {
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

/// Resolve the `distance` argument for a query: `dwithin` requires one,
/// topological predicates reject one (exact refinement has no tolerance — a
/// stray `distance` would silently read as one), and candidate-only calls
/// (`predicate=None`) accept it as an envelope expansion.
pub(crate) fn query_distance(
    predicate: Option<IndexPredicate>,
    distance: Option<&Bound<'_, PyAny>>,
) -> PyResult<Option<DwithinDistance>> {
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
    Ok(Some(validate_dwithin_distance_arg(distance)?))
}

/// Query domain needed only when an unframed empty index has no stored frame
/// whose periodic window construction would admit a geographic query.
#[derive(Clone, Copy, Default)]
pub(crate) struct QueryDomain(Option<crate::geographic::GeographicFrame>);

impl QueryDomain {
    pub(crate) fn for_query_frame(
        index: &PySpatialIndex,
        query_frame: Option<&Frame>,
    ) -> PyResult<Self> {
        if index.metadata.is_some() {
            return Ok(Self(None));
        }
        let Some(query_frame) = query_frame else {
            return Ok(Self(None));
        };
        let TopologyDomain::Geographic(frame) = crate::geometry::topology_domain(query_frame)?
        else {
            return Ok(Self(None));
        };
        Ok(Self(Some(frame)))
    }

    pub(crate) const fn needs_admission(self) -> bool {
        self.0.is_some()
    }

    pub(crate) fn admit_shape(
        self,
        shape: &Shape,
        bounds: Option<Bounds>,
        logical_row: Option<usize>,
    ) -> PyResult<()> {
        let Some(frame) = self.0 else {
            return Ok(());
        };
        let result = bounds.map_or(Ok(()), |bounds| {
            index_envelopes(shape, bounds, TopologyDomain::Geographic(frame)).map(|_| ())
        });
        match logical_row {
            Some(row) => result.map_err(|error| crate::note_array_row(error, row)),
            None => result,
        }
    }

    pub(crate) fn admit_row(
        self,
        row: ShapeRow<'_>,
        seeded_bounds: BoundsSeed,
        logical_row: Option<usize>,
    ) -> PyResult<Option<Bounds>> {
        if !self.needs_admission() {
            return Ok(None);
        }
        let bounds = match seeded_bounds {
            BoundsSeed::Unset => row.quick_bounds(),
            BoundsSeed::Value(bounds) => bounds,
        };
        let Some(bounds) = bounds else {
            return Ok(None);
        };
        let result = row.with_shape(|shape| self.admit_shape(shape, Some(bounds), None))?;
        match logical_row {
            Some(row) => result.map_err(|error| crate::note_array_row(error, row))?,
            None => result?,
        }
        Ok(Some(bounds))
    }
}

/// Validated, frame-level query state shared by scalar, bulk, join, self-join,
/// and candidate-only lanes. Geometry-specific bounds/pruner state is
/// prepared separately per row so hot bulk loops reuse predicate and metric
/// resolution without allocating a `PyGeometry` wrapper.
pub(crate) enum PreparedIndexQuery {
    /// Candidate-only query without a distance expansion.
    Candidates { query_domain: QueryDomain },
    /// Candidate-only query with a resolved metric distance expansion.
    CandidatesDwithin {
        query_domain: QueryDomain,
        distance: DwithinDistance,
        metric: crs::MetricModel,
    },
    /// Exact topology refinement needs no metric state.
    Topological {
        query_domain: QueryDomain,
        predicate: Predicate,
    },
    /// Exact distance refinement always carries every metric prerequisite.
    Dwithin {
        query_domain: QueryDomain,
        distance: DwithinDistance,
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
        Option<DwithinDistance>,
        Option<&crs::MetricModel>,
    ) {
        match self {
            Self::Candidates { .. } => (None, None, None),
            Self::CandidatesDwithin {
                distance, metric, ..
            } => (None, Some(*distance), Some(metric)),
            Self::Topological { predicate, .. } => {
                (Some(IndexPredicate::Topological(*predicate)), None, None)
            },
            Self::Dwithin {
                distance, metric, ..
            } => (Some(IndexPredicate::Dwithin), Some(*distance), Some(metric)),
        }
    }

    pub(crate) const fn query_domain(&self) -> QueryDomain {
        match self {
            Self::Candidates { query_domain }
            | Self::CandidatesDwithin { query_domain, .. }
            | Self::Topological { query_domain, .. }
            | Self::Dwithin { query_domain, .. } => *query_domain,
        }
    }

    pub(crate) fn for_index(
        index: &PySpatialIndex,
        predicate: Option<IndexPredicate>,
        distance: Option<DwithinDistance>,
        unit: Option<DistanceUnit>,
    ) -> PyResult<Self> {
        Self::for_frame(
            index,
            index.metadata.as_ref(),
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
        distance: Option<DwithinDistance>,
        unit: Option<DistanceUnit>,
    ) -> PyResult<Self> {
        Self::for_frame(
            index,
            Some(&geometry.frame),
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
        distance: Option<DwithinDistance>,
        unit: Option<DistanceUnit>,
    ) -> PyResult<Self> {
        Self::for_frame(
            index,
            Some(&array.frame),
            index.metric_crs_str(array.crs_str()),
            predicate,
            distance,
            unit,
        )
    }

    fn for_frame(
        index: &PySpatialIndex,
        query_frame: Option<&Frame>,
        metric_crs: Option<&str>,
        predicate: Option<IndexPredicate>,
        distance: Option<DwithinDistance>,
        unit: Option<DistanceUnit>,
    ) -> PyResult<Self> {
        let crs = query_frame.and_then(Frame::crs_ref);
        let epoch = query_frame.and_then(Frame::epoch);
        index.ensure_frame_compatible(crs, epoch, "spatial index query")?;
        match (predicate, distance) {
            (None, None) => Ok(Self::Candidates {
                query_domain: QueryDomain::for_query_frame(index, query_frame)?,
            }),
            (None, Some(distance)) => {
                let metric = resolve_metric(
                    metric_crs,
                    unit,
                    crate::dispatch::MetricOp::SpatialIndexDistanceQuery,
                )?;
                Ok(Self::CandidatesDwithin {
                    query_domain: QueryDomain::for_query_frame(index, query_frame)?,
                    distance,
                    metric,
                })
            },
            (Some(IndexPredicate::Topological(predicate)), None) => Ok(Self::Topological {
                query_domain: QueryDomain::for_query_frame(index, query_frame)?,
                predicate,
            }),
            (Some(IndexPredicate::Dwithin), Some(distance)) => {
                let metric = resolve_metric(
                    metric_crs,
                    unit,
                    crate::dispatch::MetricOp::SpatialIndexDistanceQuery,
                )?;
                let resolved = crs::ResolvedMetric::from_model(&metric)?;
                Ok(Self::Dwithin {
                    query_domain: QueryDomain::for_query_frame(index, query_frame)?,
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
    /// [`existing_element_bounds`] already computed.
    pub(crate) fn row(
        &self,
        _index: &PySpatialIndex,
        row: ShapeRow<'_>,
        seeded_bounds: BoundsSeed,
        logical_row: Option<usize>,
    ) -> PyResult<PreparedIndexRow> {
        let admitted_bounds = self
            .query_domain()
            .admit_row(row, seeded_bounds, logical_row)?;
        let pruner_point = row.point();
        // Gate shape materialization: only pay `with_shape` when a geodesic
        // distance cap is actually needed, or when geographic antimeridian
        // correction may apply. Candidates with `distance=None` on a
        // projected/CRS-free index never build a temporary polygon.
        let geodesic_metric = if let Self::CandidatesDwithin {
            metric: crs::MetricModel::Geodesic { crs, state, .. },
            ..
        }
        | Self::Dwithin {
            metric: crs::MetricModel::Geodesic { crs, state, .. },
            ..
        } = self
        {
            Some((crs, state))
        } else {
            None
        };
        let need_geodesic_cap = pruner_point.is_none() && geodesic_metric.is_some();
        let cap = match pruner_point {
            Some(point) => Some((point, 0.0)),
            None if need_geodesic_cap => row
                .try_with_shape(|shape| {
                    Ok::<Option<(Point, f64)>, crate::error::Error>(geodesic_metric.and_then(
                        |(crs, state)| {
                            crs::with_ellipsoid_metric_at(crs, &[shape], state, |ellipsoid| {
                                shape.geodesic_cap(ellipsoid)
                            })
                            .ok()
                            .flatten()
                        },
                    ))
                })
                .map_err(pyo3::PyErr::from)?,
            None => None,
        };
        let bounds = admitted_bounds.or_else(|| match seeded_bounds {
            BoundsSeed::Unset => row.quick_bounds(),
            BoundsSeed::Value(bounds) => bounds,
        });
        Ok(PreparedIndexRow {
            bounds,
            pruner_point,
            cap,
        })
    }
}
// ---- SpatialIndex pyclass + STR entry + builders ----

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
/// self-joins (``idx.self_join()``) — and mutate it incrementally with
/// ``insert``/``remove``. Distances follow the indexed CRS: meters on a
/// geographic frame, native linear units on a projected frame, raw coordinate
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
    // `remove` tombstones handles; overflow entries stay in their immutable
    // runs until the dead-entry invariant triggers compaction.
    rows: IndexRows,
    /// Build-time rows, repacked once tombstones outnumber live cover entries.
    bulk: Arc<StaticStrTree>,
    bulk_dead_entries: usize,
    /// Periodic covers may contribute several entries for one handle.
    /// Single-entry covers need no count column.
    bulk_entry_counts: Option<Arc<Vec<usize>>>,
    /// Post-build inserts — immutable STR runs maintained by binary carries.
    overflow: StaticStrForest,
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
    // Count of live geographic geometries that are not points inside the lon/lat domain.
    // Geodesic lower-bound pruning is sound exactly when this is zero. Keeping
    // the count incrementally makes removal O(tree mutation), not O(live rows).
    non_prunable_live: usize,
}

impl Clone for PySpatialIndex {
    fn clone(&self) -> Self {
        Self {
            rows: self.rows.clone(),
            bulk: Arc::clone(&self.bulk),
            bulk_dead_entries: self.bulk_dead_entries,
            bulk_entry_counts: self.bulk_entry_counts.clone(),
            overflow: self.overflow.clone(),
            // A copied index can diverge on its next mutation.
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
/// resolves rows through its owner-bound operation instead of cloning every
/// component and duplicating CSR/selection logic.
#[derive(Clone, Debug)]
pub(crate) struct PackedStore {
    pub(crate) source: PyGeometryArray,
}

/// The indexed rows. Building from a packed point or line array shares its
/// coordinate columns zero-copy (no per-row boxing); Mixed-array rows,
/// iterable rows, and every later `insert` are boxed handles. A handle is
/// its build/insert position: packed rows first, then boxed rows.
#[derive(Clone, Debug, Default)]
pub(crate) struct IndexRows {
    pub(crate) packed: Option<PackedStore>,
    boxed: Vec<PyGeometry>,
    /// O(1) liveness by stable handle. This is the sole liveness authority for
    /// both immutable bulk entries and mutable overflow entries.
    live: Vec<u8>,
    live_count: usize,
    live_handles: live_handles::LiveHandles,
}

/// Operation-scoped access to the index's arbitrary live handles.
///
/// Candidate refinement visits handles in tree/order-dependent sequences and
/// also serves boxed overflow rows. The packed side delegates arbitrary
/// logical-row resolution to one owner-bound array operation, including the
/// compact-point validity rank directory.
pub(crate) struct IndexRowOperation<'a> {
    rows: &'a IndexRows,
    packed: Option<ArrayRowOperation<'a>>,
}

enum IndexRowSource<'operation, 'data> {
    Packed {
        operation: &'operation mut ArrayRowOperation<'data>,
        logical: usize,
        row: ShapeRow<'data>,
    },
    Boxed(&'data PyGeometry),
}

impl<'a> IndexRowOperation<'a> {
    fn new(rows: &'a IndexRows) -> Self {
        Self {
            rows,
            packed: rows
                .packed
                .as_ref()
                .map(|packed| packed.source.row_operation()),
        }
    }

    fn missing_handle(handle: usize) -> crate::error::Error {
        crate::geometry::GeometryErrorKind::message(format!(
            "spatial index handle {handle} does not contain a present geometry"
        ))
    }

    fn resolve<'operation>(
        &'operation mut self,
        handle: usize,
    ) -> crate::error::Result<IndexRowSource<'operation, 'a>> {
        let packed_len = self.rows.packed_len();
        if handle < packed_len {
            let packed = self
                .packed
                .as_mut()
                .expect("packed length implies an operation-owned packed row view");
            let row = packed
                .present_row_at(handle)
                .ok_or_else(|| Self::missing_handle(handle));
            return row.map(|row| IndexRowSource::Packed {
                operation: packed,
                logical: row.logical(),
                row: row.row(),
            });
        }
        let boxed_index = handle.wrapping_sub(packed_len);
        let rows: &'a IndexRows = self.rows;
        rows.boxed
            .get(boxed_index)
            .map(IndexRowSource::Boxed)
            .ok_or_else(|| Self::missing_handle(handle))
    }

    pub(crate) fn row(&mut self, handle: usize) -> crate::error::Result<ShapeRow<'a>> {
        match self.resolve(handle)? {
            IndexRowSource::Packed { row, .. } => Ok(row),
            IndexRowSource::Boxed(geometry) => Ok(ShapeRow::Shape(geometry.shape.shape())),
        }
    }

    pub(crate) fn prepared(
        &mut self,
        handle: usize,
    ) -> crate::error::Result<crate::array::PreparedRow<'a>> {
        match self.resolve(handle)? {
            IndexRowSource::Packed {
                operation,
                logical,
                row,
            } => operation.prepared_at(logical, row),
            IndexRowSource::Boxed(geometry) => {
                Ok(crate::array::PreparedRow::Owned(geometry.prepared_plan()))
            },
        }
    }

    pub(crate) fn frame_cache(
        &mut self,
        handle: usize,
    ) -> crate::error::Result<Arc<crate::geometry::FrameDependentCaches>> {
        match self.resolve(handle)? {
            IndexRowSource::Packed {
                operation, logical, ..
            } => Ok(operation.frame_cache(logical)),
            IndexRowSource::Boxed(geometry) => Ok(geometry.frame_cache()),
        }
    }
}

impl IndexRows {
    pub(crate) fn operation(&self) -> IndexRowOperation<'_> {
        IndexRowOperation::new(self)
    }

    const fn live_count(&self) -> usize {
        self.live_count
    }
    fn len(&self) -> usize {
        debug_assert_eq!(
            self.packed
                .as_ref()
                .map_or(0, |packed| packed.source.len())
                .checked_add(self.boxed.len()),
            Some(self.live.len())
        );
        self.live.len()
    }

    fn packed_len(&self) -> usize {
        self.live.len().wrapping_sub(self.boxed.len())
    }

    fn push(&mut self, geometry: PyGeometry) -> usize {
        let handle = self.len();
        self.boxed.push(geometry);
        self.live.push(1);
        self.live_count = self.live_count.wrapping_add(1);
        self.live_handles.insert(handle);
        handle
    }

    /// Materialize the sparse row table used by pickle.
    ///
    /// The liveness bitmap is the handle owner, so removed and originally
    /// missing rows become missing rows in the serialized array. Replaying
    /// the source array and applying tombstones afterward is not equivalent
    /// for compact nullable storage: its physical payload contains only
    /// present rows and can otherwise make a removed logical row live again.
    fn pickle_array(&self, frame: Frame) -> PyResult<PyGeometryArray> {
        let mut shapes = Vec::with_capacity(self.live_count);
        let mut rows = self.operation();
        for (handle, &live) in self.live.iter().enumerate() {
            if live == 0 {
                continue;
            }
            let shape = rows
                .row(handle)?
                .with_shape(std::clone::Clone::clone)
                .map_err(pyo3::PyErr::from)?;
            shapes.push(shape);
        }
        let present = PyGeometryArray::from_shapes(shapes, frame)?;
        let Some(missing) = crate::array::ValidityMask::from_missing_iter(
            self.live.len(),
            self.live.iter().map(|&live| live == 0),
        ) else {
            return Ok(present);
        };
        Ok(PyGeometryArray::scatter_present_rows(&present, missing)?)
    }

    fn reserve_insert(&mut self, additional: usize) {
        self.boxed.reserve(additional);
        self.live.reserve(additional);
    }

    fn is_live(&self, handle: usize) -> bool {
        self.live.get(handle).copied().unwrap_or(0) != 0
    }

    fn mark_removed(&mut self, handle: usize) -> bool {
        let Some(live) = self.live.get_mut(handle) else {
            return false;
        };
        if *live == 0 {
            return false;
        }
        *live = 0;
        self.live_count = self.live_count.wrapping_sub(1);
        self.live_handles.remove(handle);
        true
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
        self.rows
            .heap_bytes()
            .saturating_add(self.bulk.heap_bytes())
            .saturating_add(self.bulk_entry_counts.heap_bytes())
            .saturating_add(self.overflow.heap_bytes())
            .saturating_add(self.metadata_heap_bytes())
            .saturating_add(self.geodesic_caps_heap_bytes())
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
            .map_or(0, |packed| HeapSize::heap_bytes(&packed.source))
            .saturating_add(self.boxed.heap_bytes())
            .saturating_add(self.live.heap_bytes())
            .saturating_add(self.live_handles.heap_bytes())
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
    crate::add_functions!(m; nearest, join, _unpickle_spatial_index_v1);
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
        Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::Polygon(_)
        | Shape::MultiPolygon(_)
        | Shape::GeometryCollection(_)
        | Shape::Empty(..) => None,
    }
}
