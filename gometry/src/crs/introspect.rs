//! PROJ FFI introspection — raw-pointer metadata readers that copy PROJ
//! object descriptions (authority, datum, ellipsoid, prime meridian, area of
//! use, axes, methods, parameters, grids, operation steps) into the owned
//! `types.rs` DTOs. Reached via `use super::*`.

mod metadata;
mod objects;
mod operations;

pub(super) use metadata::{
    area_of_use, axes, axis_role, compound_axis_metadata, coordinate_system_type, domain_infos,
};
pub(crate) use objects::Confidence;
pub(super) use objects::{
    authority_object_info, create_crs_transform_object, crs_coordinate_operation_info_from_pj,
    datum_info, ellipsoid_info, exported_owned_crs, id_authority, operation_info_from_pj,
    owned_authority_object_info, owned_crs_coordinate_operation_info, prime_meridian_info,
    split_authority, sub_crs_infos, validate_min_confidence,
};
pub(super) use operations::{
    crs_type_name, grids, method_info, operation_parameters, operation_steps,
};
