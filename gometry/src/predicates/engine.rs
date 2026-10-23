//! The one source of truth for boolean spatial predicates.
//!
//! Every surface that evaluates a named predicate — free functions, `Geometry`
//! and `GeometryArray` methods, the spatial index refine
//! loop, and `join` — reads dispatch facts from [`Predicate::spec`] and asks
//! the domain's topology owner for an exact answer. Cartesian predicates use
//! the witness evaluator; geographic predicates share it on ordinary charts
//! and retain the exact quotient relation at seams and poles.

use std::cell::OnceCell;

use crate::geometry::source_support::{PointMembership, cartesian_row_point_location};
use crate::geometry::storage::PackedRow;
use crate::geometry::{
    Bounds, CompiledPattern, GeographicPrepared, GeographicTopologyRoute, Loc, PreparedPlan, Shape,
    SharedShapeOwners, TopologyDomain, graph_relate_data, ring_winding,
};
#[cfg(all(test, debug_assertions))]
use crate::test_support::binary_dispatch_counters::point_predicate_dispatch;
use crate::{GeometryKind, PyGeometryArray, ShapeRow};

mod batch;
mod fixed;
mod gates;
mod points;
#[cfg(test)]
mod tests;
mod topology;

pub(crate) use batch::scalar_vs_shapes;
pub(crate) use fixed::FixedPredicate;
pub(crate) use gates::GateOperand;
#[cfg(all(test, debug_assertions))]
use gates::PRE_TOPOLOGY_CALLS;
use points::cartesian_point_predicate;
pub(crate) use points::row_point_intersection;
#[cfg(test)]
use topology::pattern_bounds_gate;
pub(crate) use topology::{
    topology_pair_after_gates, topology_scalar_pair, topology_scalar_pattern,
    topology_shapes_predicate,
};

pub(crate) use crate::geometry::predicate_spec::{
    IndexEnvelope, Predicate, PredicateSpec, predicate_matrix_value,
};

#[derive(Clone, Copy)]
pub(crate) enum PreparedTopologyPair<'a> {
    Cartesian {
        left: &'a PreparedPlan,
        right: &'a PreparedPlan,
    },
    Geographic {
        left: GeographicPrepared<'a>,
        right: GeographicPrepared<'a>,
    },
}

impl<'a> PreparedTopologyPair<'a> {
    pub(crate) fn try_for_domain(
        left: &'a PreparedPlan,
        left_cache: &'a crate::geometry::FrameDependentCaches,
        right: &'a PreparedPlan,
        right_cache: &'a crate::geometry::FrameDependentCaches,
        domain: TopologyDomain,
    ) -> crate::error::Result<Self> {
        match domain {
            TopologyDomain::Cartesian => Ok(Self::Cartesian { left, right }),
            TopologyDomain::Geographic(frame) => {
                Self::try_geographic(left, left_cache, right, right_cache, frame)
            },
        }
    }

    /// The planar pair. Cartesian topology reads no frame products, so this
    /// constructor takes none — resolving them would allocate a row-product
    /// chunk per operand for this arm to ignore.
    pub(crate) const fn cartesian(left: &'a PreparedPlan, right: &'a PreparedPlan) -> Self {
        Self::Cartesian { left, right }
    }

    pub(crate) fn try_geographic(
        left: &'a PreparedPlan,
        left_cache: &'a crate::geometry::FrameDependentCaches,
        right: &'a PreparedPlan,
        right_cache: &'a crate::geometry::FrameDependentCaches,
        frame: crate::geographic::GeographicFrame,
    ) -> crate::error::Result<Self> {
        Ok(Self::from_geographic(
            GeographicPrepared::try_new(left, left_cache, frame)?,
            GeographicPrepared::try_new(right, right_cache, frame)?,
        ))
    }

    pub(crate) const fn from_geographic(
        left: GeographicPrepared<'a>,
        right: GeographicPrepared<'a>,
    ) -> Self {
        match left.chart_route().combine(right.chart_route()) {
            GeographicTopologyRoute::Cartesian => Self::Cartesian {
                left: left.plan(),
                right: right.plan(),
            },
            GeographicTopologyRoute::Quotient => Self::Geographic { left, right },
        }
    }

    pub(crate) fn relate(self) -> crate::error::Result<crate::geometry::De9im> {
        match self {
            Self::Cartesian { left, right } => graph_relate_data(left, right),
            Self::Geographic { left, right } => {
                crate::geometry::graph_relate_data_geographic(left, right)
            },
        }
    }
}
