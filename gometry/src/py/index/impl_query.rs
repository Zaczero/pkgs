use super::shape_pruner_point;
use crate::CsrBuilder;
use crate::geometry::storage::ValidityMask;
use crate::geometry::{PreparedPlan, TopologyDomain};
use crate::predicates::engine::PredicateSpec;
use crate::py::errors::{crs_mismatch_error, epoch_mismatch_error};
use crate::py::index::{
    AABB, Arc, Bounds, Crs, DistanceUnit, DwithinDistance, Frame, GeodesicBoxPruner,
    GeodesicIndexPruner, GeodesicPruner, IndexEntry, IndexEnvelope, IndexPredicate,
    IndexRowOperation, Point, PointRows, Predicate, PreparedIndexQuery, PyGeometry,
    PyGeometryArray, PyResult, PySpatialIndex, QueryDomain, RowMatches, Shape, ShapeData, ShapeRow,
    bounds_envelope, crs, crs_label, csr_rows, epoch_label, global_geographic_candidate_envelope,
    index_envelopes, pair_dwithin_geodesic, pair_dwithin_planar, point_from_bounds, retain_row,
    sort_unique_row_ids,
};

mod candidates;
mod domain;
mod dwithin;
mod join;
mod points;
mod refine;
#[cfg(test)]
mod tests;

use candidates::candidate_policy;
use domain::admitted_query;
