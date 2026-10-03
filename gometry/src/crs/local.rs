//! Local-frame selection, projection certificates, prepared inputs, and metric operations.

mod certificate;
mod frame;
mod input;
mod operations;

pub(crate) use frame::{UtmZone, bearing_from_azimuth, estimate_local_crs, normalize_longitude};
pub(crate) use input::{LocalInputPurpose, PreparedLocalInput};
pub(crate) use operations::{
    geodesic_by_identity, metric_concave_hull, metric_constructive_shape_budgeted,
    metric_maximum_inscribed_circle, metric_minimum_bounding_circle,
    metric_minimum_bounding_radius, metric_optional_constructive_shape,
};
