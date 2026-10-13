use std::fmt::Write as _;

use super::*;
use crate::geometry::embedding_search::{
    ClassAssignment, GlobalCheck, Problem, Search, SearchAccount,
};
use crate::geometry::generated_budget::PAIRWISE_WORK_LIMIT;
use crate::geometry::graph::{
    GlobalFaces, RayAxis as Axis, RayPoints, prefix_contact_offsets, shoot_left,
    shoot_left_vector_reference,
};
use crate::geometry::numerics::product::ExactSign;
use crate::geometry::overlay::Strictness;
use crate::geometry::overlay::rect::rect_polygon;
use crate::geometry::segments::contact::{
    SegmentEndpoint, SharedSpan, TouchEndpoint, TouchLocation,
};
use crate::geometry::segments::ordering::{try_sort_by, try_sort_by_with_work};
use crate::work::{Unmetered, Work};

#[test]
fn csr_prefix_offsets_use_the_native_index_domain_past_u32_max() {
    let u32_max = usize::try_from(u32::MAX).expect("u32 count fits this supported target");
    let mut over_u32 = [0, u32_max, 1];
    prefix_contact_offsets(&mut over_u32);
    assert_eq!(over_u32, [0, u32_max, u32_max.wrapping_add(1)]);

    let mut at_u32 = [0, u32_max.wrapping_sub(1), 1];
    prefix_contact_offsets(&mut at_u32);
    assert_eq!(at_u32, [0, u32_max.wrapping_sub(1), u32_max]);
}

#[test]
fn ray_status_matches_vector_at_crossing_height_and_preserves_query_order() {
    let case = run(&[
        seg(0.0, 0.0, 4.0, 4.0),
        seg(4.0, 4.0, 0.0, 4.0),
        seg(0.0, 4.0, 4.0, 0.0),
        seg(4.0, 0.0, 0.0, 0.0),
        seg(6.0, 2.0, 6.0, 2.0),
    ]);
    let partition = case.partition();
    let origin = partition.stored_node(XY::new(0.0, 0.0)).unwrap();
    let isolated = partition.stored_node(XY::new(6.0, 2.0)).unwrap();
    let (topology, _) = partition.local_topology().unwrap();
    let minima = partition.component_minima(&topology).unwrap();
    assert_eq!(minima.len(), 2);
    assert!(minima.contains(&origin));
    assert!(minima.contains(&isolated));
    let crossing = partition
        .crossings()
        .into_iter()
        .next()
        .expect("crossing creates one exact node");
    let crossing_point = partition
        .points
        .rounded
        .get(crossing.0 as usize)
        .copied()
        .expect("crossing node is in the point arena");
    assert_eq!(crossing_point, XY::new(2.0, 2.0));
    let top_right = partition.stored_node(XY::new(4.0, 4.0)).unwrap();
    let expected = partition
        .spans
        .iter()
        .position(|span| {
            (span.start == crossing && span.end == top_right)
                || (span.start == top_right && span.end == crossing)
        })
        .unwrap();

    // Both queries are component minima. The first shares its y with the
    // crossing event; the second verifies results retain caller query order.
    let queries = [isolated.0, origin.0];
    let edge = |index: usize| {
        let span = partition
            .spans
            .get(index)
            .expect("edge index comes from the sweep input");
        [span.start.0, span.end.0]
    };
    let tree = shoot_left(&partition.points, partition.spans.len(), edge, &queries).unwrap();
    let vector =
        shoot_left_vector_reference(&partition.points, partition.spans.len(), edge, &queries)
            .unwrap();
    assert_eq!(tree, vector);
    assert_eq!(tree, [Some(expected), None]);
}
include!("tests/support.rs");
include!("tests/domains_subdivision.rs");
include!("tests/boundary_ordinates.rs");
include!("tests/admission_provenance.rs");
include!("tests/contacts_partition.rs");
include!("tests/metric_support.rs");
include!("tests/operations.rs");
include!("tests/support_queries.rs");
include!("tests/linework.rs");
include!("tests/generated_budget.rs");
include!("tests/incidence.rs");
include!("tests/topology.rs");
include!("tests/embedding.rs");
