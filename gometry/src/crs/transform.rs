#![expect(
    clippy::similar_names,
    reason = "axis and formula symbols name paired quantities here (minx/miny, xs/ys, lat_rad/lam_rad, step/stop), which this lint reads as typos"
)]
//! The public coordinate-transform API (`transform`/`transform_coordinates`/
//! `transform_bounds`/`transform_bounds_3d`).
//!
//! These take `&Shape`/`&str`/`&[f64]` and drive the FFI pipeline engine and
//! raw-pointer helpers (which stay private in the parent `crs` module) via
//! `use super::*`; re-exported at `crs` so `crs::transform` callers resolve.

use std::ops::Range;

use crate::crs::{
    CoordSeq, CrsError, GeometryErrorKind, Point, ProjDirection, ProjPipeline, RuntimeState, Shape,
    TRANSFORM_BOUNDS_MAX_DENSIFY, TransformExecution, TransformFailure, TransformOptions,
    TransformPlan, info_with_state, proj_error_message, runtime_state_snapshot,
    transform_in_core_xy_batch, transform_in_core_xy_batch_indexed, with_proj_operation_at,
};
use crate::error::Result;
use crate::geographic::{
    CircularInterval, GeodeticPole, GeographicError, GeographicFrame, Latitude, LongitudeWindows,
};
use crate::geometry::numerics::product_sum::ExactSum;
use crate::geometry::{
    CoordRun, CoordWindow, LineSeq, MColumn, MOrdinate, SharedSlice, TopologyDomain, ZOrdinate,
    types,
};

mod bounds;
mod domains;
mod output;

pub(crate) use bounds::{
    BOUNDS_2D_ERROR, BOUNDS_2D_GEO_ERROR, BOUNDS_3D_ERROR, BOUNDS_3D_GEO_ERROR, TransformedBounds,
    TransformedBounds3d, all_finite, transform_bounds, transform_bounds_3d,
    transform_bounds_3d_many, transform_bounds_many, validate_bounds, validate_bounds_3d,
};
pub(super) use bounds::{
    BoundsInterpretation, BoundsSamplePlan, canonical_identity_latitudes,
    canonical_identity_longitudes, identity_bounds_2d, identity_bounds_3d,
};
pub(crate) use domains::{
    coordinates_are_finite, validate_coordinate_lanes, validate_coordinate_slices,
};
pub(super) use domains::{
    map_geographic_conversion_error, transform_in_core_xy_with_domains,
    transform_in_core_xy_with_domains_indexed, transform_in_core_xy_with_domains_indexed_for_shape,
};
pub(crate) use output::TransformOutputPlan;

pub(crate) fn transform(shape: &Shape, source: &str, target: &str) -> Result<Shape> {
    let state = runtime_state_snapshot()?;
    transform_at(shape, source, target, &state)
}

pub(crate) fn transform_at(
    shape: &Shape,
    source: &str,
    target: &str,
    state: &RuntimeState,
) -> Result<Shape> {
    TransformPlan::new_with_options_at(
        source,
        target,
        TransformOptions::default(),
        std::sync::Arc::new(state.clone()),
    )?
    .transform_shape(shape)
}

pub(crate) fn transform_coordinates(
    source: &str,
    target: &str,
    x: &mut [f64],
    y: &mut [f64],
    zt: crate::ZtLanes<'_>,
    options: TransformOptions,
) -> Result<TransformExecution> {
    let state = runtime_state_snapshot()?;
    let plan = TransformPlan::new_with_options_at(source, target, options, state)?;
    plan.transform_coordinates(x, y, zt)?;
    Ok(plan.execution())
}

pub(crate) fn apply_operation(
    definition: &str,
    direction: ProjDirection,
    x: &mut [f64],
    y: &mut [f64],
    zt: crate::ZtLanes<'_>,
) -> Result<TransformExecution> {
    let definition = definition.trim();
    if definition.is_empty() {
        return Err(CrsError::invalid(
            "operation definition is required".to_owned(),
        ));
    }
    let state = runtime_state_snapshot()?;
    let execution = TransformExecution::default();
    with_proj_operation_at(definition, &state, &execution, |operation| {
        transform_coordinates_with_pipeline(operation, direction, x, y, zt, &execution)
    })?;
    Ok(execution)
}

pub(crate) fn parse_epsg(value: &str) -> Option<u32> {
    let (authority, code) = value.split_once(':')?;
    authority
        .eq_ignore_ascii_case("EPSG")
        .then(|| code.parse().ok())
        .flatten()
}

pub(crate) fn is_wgs84_lonlat(value: &str) -> bool {
    matches!(parse_epsg(value), Some(4326 | 4979))
}

pub(crate) fn proj_transform_error(error: i32) -> Box<str> {
    if error == 0 {
        "projection returned a non-finite coordinate".into()
    } else {
        proj_error_message(error).into()
    }
}

pub(crate) fn transform_coordinates_with_pipeline(
    transformer: &ProjPipeline,
    direction: ProjDirection,
    x: &mut [f64],
    y: &mut [f64],
    zt: crate::ZtLanes<'_>,
    execution: &crate::crs::TransformExecution,
) -> Result<()> {
    match zt {
        crate::Zt::None => {
            validate_coordinate_lanes(x, y, crate::Zt::None)?;
            transformer.transform_lanes(x, y, None, None, direction, execution)
        },
        crate::Zt::Z(z) => {
            validate_coordinate_lanes(x, y, crate::Zt::Z(&*z))?;
            transformer.transform_lanes(x, y, Some(z), None, direction, execution)
        },
        crate::Zt::T(t) => {
            validate_coordinate_lanes(x, y, crate::Zt::T(&*t))?;
            transformer.transform_lanes(x, y, None, Some(t), direction, execution)
        },
        crate::Zt::Zt { z, t } => {
            validate_coordinate_lanes(x, y, crate::Zt::Zt { z: &*z, t: &*t })?;
            transformer.transform_lanes(x, y, Some(z), Some(t), direction, execution)
        },
    }
}

pub(crate) fn transform_proj_shapes_indexed(
    transformer: &ProjPipeline,
    shapes: &[&Shape],
    source_epoch: Option<f64>,
    source_frame: Option<GeographicFrame>,
    execution: &crate::crs::TransformExecution,
) -> std::result::Result<Vec<Shape>, TransformFailure> {
    let mut output = TransformOutputPlan::try_new(shapes)?;
    if source_epoch.is_none() {
        transformer
            .require_epoch_lane()
            .map_err(TransformFailure::from)?;
    }
    output.execute_indexed(|x, y, z| {
        let mut epoch = source_epoch.map(|epoch| [epoch]);
        transformer.transform_default_source_lanes_indexed(
            source_frame,
            x,
            y,
            z,
            epoch.as_mut().map(<[f64; 1]>::as_mut_slice),
            execution,
        )
    })?;
    Ok(output.finish(shapes)?)
}
