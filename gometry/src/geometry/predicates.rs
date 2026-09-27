//! Topological predicates and validity on `Shape`: `is_empty`/`is_closed`/
//! `is_ring`/`is_simple`, antimeridian, min-clearance, validate/repair, and the
//! binary relations (contains/within/covers/intersects/touches/…/equals).

mod membership;
mod pole;
mod properties;
mod relate;
mod repair;
mod shape;
mod shape_data;
mod simplicity;
mod validate;
mod validity_helpers;

pub(in crate::geometry) use membership::{
    PolygonContact, multi_polygon_members_issue, polygon_rings_issue_with_contacts,
};
pub(crate) use membership::{
    RingClass, TouchDirections, ring_classify_point, ring_contains_interior, ring_label,
};
pub(crate) use pole::{
    ring_encloses_pole, shape_encloses_pole_in_frame, shape_has_polar_ring_in_frame,
    shape_reaches_geographic_pole_in_frame,
};
pub(in crate::geometry) use relate::LineworkChains;
pub(crate) use repair::polygonal_repair;
pub(in crate::geometry) use shape_data::classify_ring_pair_with_contacts;
pub(crate) use shape_data::{
    classify_ring_pair, ring_probe_point, settle_touches, visit_interacting_pairs,
};
pub(in crate::geometry) use simplicity::simplified_polygon_delta_is_simple;
pub(crate) use simplicity::{
    collect_duplicate_points, collect_offending_pair, indexed_segments_are_simple,
    segment_intersection_is_simple, segments_are_adjacent,
};
pub(in crate::geometry) use validate::{shell_is_convex, shell_is_strictly_convex};
pub(crate) use validate::{validate_line, validate_point, validate_points, validate_ring};
pub(in crate::geometry) use validity_helpers::face_interior_point;
pub(crate) use validity_helpers::{
    MinimumClearanceWitness, line_is_ccw, line_is_closed, line_is_simple, line_is_valid,
    minimum_clearance_witness, multiline_is_simple, polygon_is_valid,
};

#[cfg(test)]
mod tests;
