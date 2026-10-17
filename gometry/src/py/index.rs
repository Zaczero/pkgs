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
use std::collections::{BTreeMap, BinaryHeap};
use std::sync::Arc;

use geodesic::{GeodesicBoxPruner, GeodesicIndexPruner, GeodesicPruner};
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
    resolve_metric, validate_dwithin_distance_arg, validate_nearest_k,
};

pub(crate) fn checked_index_handle_end(start: usize, count: usize) -> crate::error::Result<usize> {
    let end = start
        .checked_add(count)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    if end > isize::MAX as usize {
        return Err(crate::error::ResourceError::SizeOverflow.into());
    }
    Ok(end)
}

fn cover_count_exceptions(entries: &[IndexEntry]) -> BTreeMap<usize, usize> {
    debug_assert!(
        entries
            .array_windows::<2>()
            .all(|[left, right]| left.idx <= right.idx)
    );
    let mut counts = BTreeMap::new();
    let mut start = 0;
    while let Some(entry) = entries.get(start) {
        let handle = entry.idx;
        let mut end = start.wrapping_add(1);
        while entries.get(end).is_some_and(|entry| entry.idx == handle) {
            end = end.wrapping_add(1);
        }
        let count = end.wrapping_sub(start);
        if count > 1 {
            counts.insert(handle, count);
        }
        start = end;
    }
    counts
}

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
    Frontier, FrontierNode, NearestCandidate, RowMatches, aabb_box_distance_2, csr_rows,
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
    pub(crate) box_pruner: Option<GeodesicBoxPruner>,
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
        index: &PySpatialIndex,
        row: ShapeRow<'_>,
        seeded_bounds: BoundsSeed,
        logical_row: Option<usize>,
    ) -> PyResult<PreparedIndexRow> {
        let admitted_bounds = self
            .query_domain()
            .admit_row(row, seeded_bounds, logical_row)?;
        let pruner_point = row.point();
        let bounds = admitted_bounds.or_else(|| match seeded_bounds {
            BoundsSeed::Unset => row.quick_bounds(),
            BoundsSeed::Value(bounds) => bounds,
        });
        // Shape-query pruning uses the shared chart cover, not a geodesic
        // anchor/reach cap. Admit the metric coordinates before the cover can
        // exclude an invalid query row.
        let box_pruner = match (self, pruner_point, bounds) {
            (
                Self::CandidatesDwithin {
                    metric: crs::MetricModel::Geodesic { crs, frame, state },
                    ..
                }
                | Self::Dwithin {
                    metric: crs::MetricModel::Geodesic { crs, frame, state },
                    ..
                },
                None,
                Some(bounds),
            ) => Some(row.try_with_shape(|shape| {
                crs::ensure_geographic_domain_in_frame(*frame, shape).map_err(pyo3::PyErr::from)?;
                if *frame != crate::geographic::GeographicFrame::crs84()
                    || index.non_prunable_live != 0
                {
                    return Ok(None);
                }
                let cover = index_envelopes(shape, bounds, TopologyDomain::Geographic(*frame))?;
                crs::with_ellipsoid_metric_at(crs, &[shape], state, |metric| {
                    let (semi_major, flattening) = metric.ellipsoid_parameters();
                    Ok(GeodesicBoxPruner::new(
                        semi_major,
                        flattening,
                        cover.entries(),
                    ))
                })
                .map(Some)
                .map_err(pyo3::PyErr::from)
            })?),
            _ => None,
        }
        .flatten();
        Ok(PreparedIndexRow {
            bounds,
            pruner_point,
            box_pruner,
        })
    }
}
// ---- SpatialIndex pyclass + STR entry + builders ----

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
    // Handles are never reused. The row owner retains the next identity even
    // after removals; overflow entries stay in immutable runs until compaction.
    rows: IndexRows,
    /// Build-time rows, repacked once tombstones outnumber live cover entries.
    bulk: Arc<StaticStrTree>,
    bulk_dead_entries: usize,
    /// Periodic covers may contribute several entries for one live handle.
    /// Single-entry covers need no count entry.
    cover_counts: BTreeMap<usize, usize>,
    /// Post-build inserts — immutable STR runs maintained by binary carries.
    overflow: StaticStrForest,
    /// Insert/remove generation used by live index iterators.
    mutation_gen: u64,
    // The CRS + coordinate epoch shared by every indexed geometry, learned from
    // the first item. Queries must match it (predicate/distance refinement
    // compares coordinates directly, so mixed frames would silently mis-match).
    // `None` until the first geometry is indexed.
    metadata: Option<Frame>,
    // Count of live CRS84 points outside the raw point-metric domain. Shape
    // metrics normalize periodic longitude aliases at their chart boundary.
    non_prunable_live: usize,
}

impl Clone for PySpatialIndex {
    fn clone(&self) -> Self {
        Self {
            rows: self.rows.clone(),
            bulk: Arc::clone(&self.bulk),
            bulk_dead_entries: self.bulk_dead_entries,
            cover_counts: self.cover_counts.clone(),
            overflow: self.overflow.clone(),
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
/// The array remains the coordinate owner; compact nullable points additionally
/// need original-present IDs to address its physical point ordinals.
#[derive(Clone, Debug)]
enum PackedRowAddressing {
    /// A live handle is the source's logical row.
    Logical,
    /// Original logical ID of each physical row, in source storage order.
    PresentOrdinals(Arc<[usize]>),
}

#[derive(Clone, Debug)]
pub(crate) struct PackedStore {
    pub(crate) source: PyGeometryArray,
    addressing: PackedRowAddressing,
}

impl HeapSize for PackedStore {
    fn heap_bytes(&self) -> usize {
        let addressing = match &self.addressing {
            PackedRowAddressing::Logical => 0,
            PackedRowAddressing::PresentOrdinals(handles) => crate::heap_size::ARC_CONTROL_BYTES
                .saturating_add(handles.len().saturating_mul(size_of::<usize>())),
        };
        self.source.heap_bytes().saturating_add(addressing)
    }
}

/// The indexed rows. Building from a packed array shares its coordinate
/// columns zero-copy (no per-row boxing); Mixed-array rows, iterable rows, and
/// every later `insert` are boxed handles. A handle is its build/insert
/// position: packed rows first, then boxed rows.
#[derive(Clone, Debug, Default)]
pub(crate) struct IndexRows {
    pub(crate) packed: Option<PackedStore>,
    boxed: BoxedRows,
    /// Never-reused identity for the next successful insertion.
    next_handle: usize,
    /// Sole membership, cardinality and iteration-order authority.
    live_handles: live_handles::LiveHandles,
}

#[derive(Clone, Debug)]
enum BoxedRows {
    Dense(Vec<PyGeometry>),
    Sparse(BTreeMap<usize, PyGeometry>),
}

impl Default for BoxedRows {
    fn default() -> Self {
        Self::Dense(Vec::new())
    }
}

impl BoxedRows {
    fn from_dense(rows: Vec<PyGeometry>, live: &live_handles::LiveHandles) -> Self {
        if live.len() == rows.len() {
            return Self::Dense(rows);
        }
        Self::Sparse(
            rows.into_iter()
                .enumerate()
                .filter_map(|(handle, geometry)| {
                    live.contains(handle).then_some((handle, geometry))
                })
                .collect(),
        )
    }

    fn from_optional(
        rows: Vec<Option<PyGeometry>>,
        live: &live_handles::LiveHandles,
        total_rows: usize,
    ) -> Self {
        if live.len() == total_rows {
            return Self::Dense(
                rows.into_iter()
                    .map(|row| row.expect("every input row is live"))
                    .collect(),
            );
        }
        Self::Sparse(
            rows.into_iter()
                .enumerate()
                .filter_map(|(handle, geometry)| {
                    live.contains(handle).then(|| {
                        (
                            handle,
                            geometry.expect("live input row retains its geometry"),
                        )
                    })
                })
                .collect(),
        )
    }

    fn from_pairs(handles: &[usize], rows: Vec<PyGeometry>, next_handle: usize) -> Self {
        if handles.len() == next_handle {
            return Self::Dense(rows);
        }
        Self::Sparse(handles.iter().copied().zip(rows).collect())
    }

    fn get(&self, handle: usize, packed_len: usize) -> Option<&PyGeometry> {
        match self {
            Self::Dense(rows) => rows.get(handle.wrapping_sub(packed_len)),
            Self::Sparse(rows) => rows.get(&handle),
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        match self {
            Self::Dense(rows) => rows.len(),
            Self::Sparse(rows) => rows.len(),
        }
    }

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn reserve_insert(&mut self, additional: usize) {
        if let Self::Dense(rows) = self {
            rows.reserve(additional);
        }
    }

    fn push(&mut self, handle: usize, packed_len: usize, geometry: PyGeometry) {
        match self {
            Self::Dense(rows) if handle == packed_len.wrapping_add(rows.len()) => {
                rows.push(geometry);
            },
            Self::Dense(rows) => {
                let old = std::mem::take(rows);
                let mut sparse: BTreeMap<_, _> = old
                    .into_iter()
                    .enumerate()
                    .map(|(offset, geometry)| (packed_len.wrapping_add(offset), geometry))
                    .collect();
                sparse.insert(handle, geometry);
                *self = Self::Sparse(sparse);
            },
            Self::Sparse(rows) => {
                let previous = rows.insert(handle, geometry);
                debug_assert!(previous.is_none());
            },
        }
    }

    fn remove(&mut self, handle: usize, packed_len: usize) {
        match self {
            Self::Dense(rows) => {
                let removed = handle.wrapping_sub(packed_len);
                let old = std::mem::take(rows);
                let sparse = old
                    .into_iter()
                    .enumerate()
                    .filter_map(|(offset, geometry)| {
                        (offset != removed).then_some((packed_len.wrapping_add(offset), geometry))
                    })
                    .collect();
                *self = Self::Sparse(sparse);
            },
            Self::Sparse(rows) => {
                rows.remove(&handle);
            },
        }
    }

    fn heap_bytes(&self) -> usize {
        match self {
            Self::Dense(rows) => rows.heap_bytes(),
            Self::Sparse(rows) => rows.iter().fold(0_usize, |bytes, (_, geometry)| {
                bytes
                    .saturating_add(size_of::<(usize, PyGeometry)>())
                    .saturating_add(size_of::<[usize; 2]>())
                    .saturating_add(geometry.heap_bytes())
            }),
        }
    }
}

/// Operation-scoped access to the index's arbitrary live handles.
///
/// Candidate refinement visits handles in tree/order-dependent sequences and
/// also serves boxed overflow rows. Packed rows resolve through their retained
/// owner and admitted row-addressing mode.
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
        if !self.rows.live_handles.contains(handle) {
            return Err(Self::missing_handle(handle));
        }
        let packed_len = self.rows.packed_len();
        if handle < packed_len {
            let packed_store = self
                .rows
                .packed
                .as_ref()
                .expect("packed length implies an index-owned packed source");
            let packed = self
                .packed
                .as_mut()
                .expect("packed length implies an operation-owned packed row view");
            let (logical, row) = match &packed_store.addressing {
                PackedRowAddressing::Logical => {
                    let row = packed
                        .present_row_at(handle)
                        .ok_or_else(|| Self::missing_handle(handle))?;
                    (row.logical(), row.row())
                },
                PackedRowAddressing::PresentOrdinals(handles) => {
                    let ordinal = handles
                        .binary_search(&handle)
                        .map_err(|_| Self::missing_handle(handle))?;
                    (handle, packed_store.source.storage().row(ordinal))
                },
            };
            return Ok(IndexRowSource::Packed {
                operation: packed,
                logical,
                row,
            });
        }
        let rows: &'a IndexRows = self.rows;
        rows.boxed
            .get(handle, packed_len)
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

    fn packed_len(&self) -> usize {
        self.packed.as_ref().map_or(0, |packed| packed.source.len())
    }

    fn push(&mut self, geometry: PyGeometry) -> usize {
        let handle = self.next_handle;
        self.boxed.push(handle, self.packed_len(), geometry);
        self.live_handles.insert(handle);
        self.next_handle = self.next_handle.wrapping_add(1);
        handle
    }

    /// Materialize only live geometries and their stable handles for pickle.
    fn pickle_live_rows(&self, frame: Frame) -> PyResult<(PyGeometryArray, Vec<usize>)> {
        let mut shapes = Vec::with_capacity(self.live_handles.len());
        let mut handles = Vec::with_capacity(self.live_handles.len());
        let mut rows = self.operation();
        for handle in self.live_handles.iter() {
            let shape = rows
                .row(handle)?
                .with_shape(std::clone::Clone::clone)
                .map_err(pyo3::PyErr::from)?;
            shapes.push(shape);
            handles.push(handle);
        }
        Ok((PyGeometryArray::from_shapes(shapes, frame)?, handles))
    }

    fn reserve_insert(&mut self, additional: usize) {
        self.boxed.reserve_insert(additional);
    }

    fn is_live(&self, handle: usize) -> bool {
        self.live_handles.contains(handle)
    }

    fn mark_removed(&mut self, handle: usize) -> bool {
        if !self.live_handles.contains(handle) {
            return false;
        }
        let packed_len = self.packed_len();
        if handle >= packed_len {
            self.boxed.remove(handle, packed_len);
        }
        self.live_handles.remove(handle)
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

    fn retained_heap_bytes(&self) -> usize {
        self.rows
            .heap_bytes()
            .saturating_add(self.bulk.heap_bytes())
            .saturating_add(self.cover_counts.len().saturating_mul(
                size_of::<(usize, usize)>().saturating_add(size_of::<[usize; 2]>()),
            ))
            .saturating_add(self.overflow.heap_bytes())
            .saturating_add(self.metadata_heap_bytes())
    }
}

impl HeapSize for IndexRows {
    fn heap_bytes(&self) -> usize {
        self.packed
            .as_ref()
            .map_or(0, HeapSize::heap_bytes)
            .saturating_add(self.boxed.heap_bytes())
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
    crate::add_functions!(
        m;
        nearest,
        join,
        _unpickle_spatial_index_v1
    );
    crate::add_classes!(m; PySpatialIndex, PySpatialIndexIter);
    m.py()
        .import("collections.abc")?
        .getattr("Mapping")?
        .call_method1("register", (m.getattr("SpatialIndex")?,))?;
    Ok(())
}
/// Whether a CRS84 point must reach exact metric refinement before an
/// envelope can exclude its row. Shape metrics normalize periodic longitude
/// aliases when they admit their chart edges; point-pair metrics use raw lon/lat.
pub(crate) fn geodesic_domain_invalid(shape: &Shape, domain: TopologyDomain) -> bool {
    let TopologyDomain::Geographic(frame) = domain else {
        return false;
    };
    if frame != crate::geographic::GeographicFrame::crs84() {
        return false;
    }
    matches!(
        shape,
        Shape::Point(point)
            if !((-180.0..=180.0).contains(&point.x) && (-90.0..=90.0).contains(&point.y))
    )
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
