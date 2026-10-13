//! Shared graph traversal and face-linking primitives.

mod faces;
mod rays;

pub(in crate::geometry) use faces::{
    GlobalFaces, graph_global_faces_with_work, prefix_contact_offsets, visit_component_faces,
};
#[cfg(test)]
pub(in crate::geometry) use rays::shoot_left_vector_reference;
pub(in crate::geometry) use rays::{
    RayAxis, RayPoints, RayPointsWithWork, compare_departures_with_work, shoot_left,
    shoot_left_with_work,
};
#[cfg(test)]
pub(in crate::geometry) use rays::{graph_ray_orientations, reset_graph_ray_orientations};
