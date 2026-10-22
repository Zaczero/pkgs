//! Local-frame selection, projection certificates, prepared inputs, and metric operations.

mod certificate;
mod circle;
mod frame;
mod input;
mod operations;
mod output;

pub(crate) use circle::FramedCircle;
pub(crate) use frame::{UtmZone, bearing_from_azimuth, estimate_local_crs, normalize_longitude};
pub(crate) use input::{LocalInputPurpose, PreparedLocalInput};
pub(crate) use operations::{
    geodesic_by_identity, metric_concave_hull, metric_constructive_shape_budgeted,
    metric_optional_constructive_shape,
};
