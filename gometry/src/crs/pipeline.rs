#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! The libPROJ transform-pipeline engine (`impl ProjPipeline`).
//!
//! Wraps a cached `PJ` transformation object and runs coordinate transforms
//! (XY/XYZ/XYZT lanes). Reaches the FFI primitives, thread-local caches, and
//! error helpers in the parent `crs` module via `use super::*`.

use crate::crs::{
    CrsError, GeometryErrorKind, OperationInfo, OwnedPj, ProjArea, ProjContext, ProjDirection,
    ProjPipeline, ProjTransformOptions, RuntimeState, TransformExecution, TransformFailure,
    TransformOptions, coordinates_are_finite, create_crs_transform_object, cstring,
    operation_info_from_pj, preserve_resource_error, proj_context_error_message,
    proj_transform_error, ptr,
};
use crate::error::Result;

/// Preserve PROJ's structured coordinate-domain errno while retaining its
/// existing rendered error message for every other transform failure.
fn map_proj_transform_error(error: i32) -> crate::error::Error {
    if error == proj_sys::PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN as i32 {
        GeometryErrorKind::projection_domain(proj_transform_error(error))
    } else {
        GeometryErrorKind::projection(proj_transform_error(error))
    }
}

impl ProjPipeline {
    pub(crate) fn new_with_state(
        source: &str,
        target: &str,
        options: &TransformOptions,
        state: &RuntimeState,
    ) -> Result<Self> {
        let source_crs = cstring(source)?;
        let target_crs = cstring(target)?;
        let area = options.area_of_interest.map(ProjArea::new).transpose()?;
        let transform_options = ProjTransformOptions::new(options)?;
        let context = ProjContext::new_with_state(state).map_err(|error| {
            preserve_resource_error(error, |error| {
                CrsError::transform_create(source, target, error.to_string())
            })
        })?;
        let source_transform_object =
            create_crs_transform_object(&context, &source_crs, source, options.source_epoch)?;
        let target_transform_object =
            create_crs_transform_object(&context, &target_crs, target, options.target_epoch)?;
        // SAFETY: DOC-H. Typed live context/objects; area/options valid-or-null
        // per PROJ C API; returns uniquely owned PJ or null.
        let raw = unsafe {
            proj_sys::proj_create_crs_to_crs_from_pj(
                context.as_ptr(),
                source_transform_object.as_ptr(),
                target_transform_object.as_ptr(),
                area.as_ref().map_or(ptr::null_mut(), ProjArea::as_ptr),
                transform_options.as_ptr(),
            )
        };
        // SAFETY: non-null return is uniquely owned by the caller.
        let Some(raw) = (unsafe { OwnedPj::try_from_owned(raw) }) else {
            let message = proj_context_error_message(&context);
            return Err(CrsError::transform_create(source, target, message));
        };
        // SAFETY: DOC-H. Typed context + owned raw; returns uniquely owned
        // normalized transform or null.
        let transform =
            unsafe { proj_sys::proj_normalize_for_visualization(context.as_ptr(), raw.as_ptr()) };
        // SAFETY: non-null return is uniquely owned.
        let Some(transform) = (unsafe { OwnedPj::try_from_owned(transform) }) else {
            return Err(CrsError::transform_create(
                source,
                target,
                "PROJ could not normalize the CRS operation for X/Y order",
            ));
        };
        Ok(Self { transform, context })
    }

    pub(crate) fn requires_coordinate_epoch(&self) -> bool {
        // SAFETY: DOC-H. Typed self owners on creating thread.
        unsafe {
            proj_sys::proj_coordoperation_requires_per_coordinate_input_time(
                self.context.as_ptr(),
                self.transform.as_ptr(),
            ) != 0
        }
    }

    pub(crate) fn require_epoch_lane(&self) -> Result<()> {
        if self.requires_coordinate_epoch() {
            return Err(CrsError::message(
                "transform requires a coordinate epoch; pass epoch= or source_epoch=",
            ));
        }
        Ok(())
    }

    /// Transform one target-pole probe in the inverse direction. A projection
    /// domain miss means that this physical pole has no inverse in the
    /// operation; every other failure remains an error.
    pub(crate) fn transform_xy_probe(
        &self,
        x: f64,
        y: f64,
        epoch: Option<f64>,
        direction: ProjDirection,
        execution: &TransformExecution,
    ) -> Result<Option<(f64, f64)>> {
        if !x.is_finite() || !y.is_finite() {
            return Err(GeometryErrorKind::NonFiniteCoordinate.into());
        }
        super::record_transform_engine(false);
        self.context.clear_diagnostic();
        let mut x = x;
        let mut y = y;
        let mut epoch = epoch;
        // SAFETY: DOC-LANE. The one-element x/y lanes and optional one-element
        // time lane are Rust-owned and live for this synchronous call; absent
        // time is represented by a null pointer and zero count.
        unsafe {
            let transform = self.transform.as_ptr();
            proj_sys::proj_errno_reset(transform);
            let stride = std::mem::size_of::<f64>();
            let (t_ptr, t_stride, t_count) =
                epoch.as_mut().map_or((ptr::null_mut(), 0, 0), |epoch| {
                    (std::ptr::from_mut(epoch), stride, 1)
                });
            let transformed = proj_sys::proj_trans_generic(
                transform,
                direction.to_proj(),
                &raw mut x,
                stride,
                1,
                &raw mut y,
                stride,
                1,
                ptr::null_mut(),
                0,
                0,
                t_ptr,
                t_stride,
                t_count,
            );
            let error = proj_sys::proj_errno(transform);
            if let Some(message) = self.context.take_diagnostic() {
                execution.record_diagnostic(message);
            }
            if transformed == 1
                && x.is_finite()
                && y.is_finite()
                && epoch.is_none_or(f64::is_finite)
            {
                return Ok(Some((x, y)));
            }
            if error == proj_sys::PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN as i32 {
                return Ok(None);
            }
            Err(GeometryErrorKind::projection(proj_transform_error(error)))
        }
    }

    pub(crate) fn from_definition_with_state(
        definition: &str,
        state: &RuntimeState,
    ) -> Result<Self> {
        let definition = cstring(definition)?;
        let context = ProjContext::new_with_state(state).map_err(|error| {
            preserve_resource_error(error, |error| {
                CrsError::crs_create(definition.to_string_lossy().into_owned(), error.to_string())
            })
        })?;
        // SAFETY: DOC-H. Typed live context; definition is a live CString;
        // returns uniquely owned PJ or null.
        let transform = unsafe { proj_sys::proj_create(context.as_ptr(), definition.as_ptr()) };
        // SAFETY: non-null return is uniquely owned.
        let Some(transform) = (unsafe { OwnedPj::try_from_owned(transform) }) else {
            let message = proj_context_error_message(&context);
            return Err(CrsError::crs_create(
                definition.to_string_lossy().into_owned(),
                message,
            ));
        };
        Ok(Self { transform, context })
    }

    /// Run the PROJ transform over the x/y columns plus optional z/t lanes in a
    /// single `proj_trans_generic` call — the one FFI kernel behind the
    /// `transform_xy`/`xyz`/`xyzt` entry points (a missing lane passes a null
    /// pointer with zero stride/count, so PROJ ignores it).
    /// The single PROJ batch-transform entry: equal-length x/y lanes plus
    /// optional z/t lanes (absent lanes pass as null PROJ lanes).
    pub(crate) fn transform_lanes(
        &self,
        x: &mut [f64],
        y: &mut [f64],
        z: Option<&mut [f64]>,
        t: Option<&mut [f64]>,
        direction: ProjDirection,
        execution: &TransformExecution,
    ) -> Result<()> {
        self.transform_lanes_indexed(x, y, z, t, direction, execution)
            .map_err(|failure| failure.error)
    }

    pub(crate) fn transform_lanes_indexed(
        &self,
        x: &mut [f64],
        y: &mut [f64],
        mut z: Option<&mut [f64]>,
        mut t: Option<&mut [f64]>,
        direction: ProjDirection,
        execution: &TransformExecution,
    ) -> std::result::Result<(), TransformFailure> {
        // The `unsafe` `proj_trans_generic` call below reads/writes each lane
        // up to `x.len()` elements, so a length mismatch would be an
        // out-of-bounds FFI access (UB). Keep those safety preconditions
        // fallible across the public path; callers pass equal-length CoordSeq
        // columns by construction, so these do not fire in normal use.
        if x.len() != y.len() {
            return Err(TransformFailure::from(GeometryErrorKind::projection(
                "transform lane length mismatch",
            )));
        }
        if z.as_deref().is_some_and(|z| z.len() != x.len()) {
            return Err(TransformFailure::from(GeometryErrorKind::projection(
                "transform z lane length mismatch",
            )));
        }
        // A length-1 time lane is a valid PROJ broadcast constant: PROJ reads
        // (and writes) only its single element, never advancing the pointer,
        // so it stays in bounds regardless of `x.len()`. Any other length must
        // match `x.len()` for the same OOB reason as x/y/z.
        if t.as_deref()
            .is_some_and(|t| t.len() != x.len() && t.len() != 1)
        {
            return Err(TransformFailure::from(GeometryErrorKind::projection(
                "transform t lane length mismatch",
            )));
        }
        if x.is_empty() {
            return Ok(());
        }
        super::record_transform_engine(false);
        self.context.clear_diagnostic();
        // SAFETY: DOC-LANE. The fallible checks above prove x/y/z equality and a
        // t lane of len x or 1. All pointers come from exclusive Rust-owned f64
        // slices and remain live for this synchronous call; null/count-zero
        // lanes are absent. The PJ is thread-confined and PROJ cannot suspend
        // into Python.
        unsafe {
            let transform = self.transform.as_ptr();
            proj_sys::proj_errno_reset(transform);
            let stride = std::mem::size_of::<f64>();
            let (z_ptr, z_stride, z_count) =
                z.as_deref_mut().map_or((ptr::null_mut(), 0, 0), |z| {
                    (z.as_mut_ptr(), stride, z.len())
                });
            let (t_ptr, t_stride, t_count) =
                t.as_deref_mut().map_or((ptr::null_mut(), 0, 0), |t| {
                    (t.as_mut_ptr(), stride, t.len())
                });
            let transformed = proj_sys::proj_trans_generic(
                transform,
                direction.to_proj(),
                x.as_mut_ptr(),
                stride,
                x.len(),
                y.as_mut_ptr(),
                stride,
                y.len(),
                z_ptr,
                z_stride,
                z_count,
                t_ptr,
                t_stride,
                t_count,
            );
            let result = if transformed == x.len()
                && coordinates_are_finite(x, y, z.as_deref(), t.as_deref())
            {
                Ok(())
            } else {
                let error = proj_sys::proj_errno(transform);
                let coordinate = first_nonfinite_coordinate(x, y, z.as_deref(), t.as_deref());
                Err(TransformFailure {
                    error: map_proj_transform_error(error),
                    coordinate,
                })
            };
            if let Some(message) = self.context.take_diagnostic() {
                execution.record_diagnostic(message);
            }
            result
        }
    }

    pub(crate) fn operation_info_at(
        &self,
        source: String,
        target: String,
        source_epoch: Option<f64>,
        target_epoch: Option<f64>,
        x: f64,
        y: f64,
        z: Option<f64>,
        t: Option<f64>,
        execution: &TransformExecution,
    ) -> Result<OperationInfo> {
        self.context.clear_diagnostic();
        let transform = self.transform.as_ptr();
        // SAFETY: DOC-H. Typed self owners; proj_trans takes coordinates by
        // value; returned last-used operation is uniquely owned when non-null.
        // No Python callback.
        unsafe {
            proj_sys::proj_errno_reset(transform);
            let output = proj_sys::proj_trans(
                transform,
                ProjDirection::Forward.to_proj(),
                proj_sys::proj_coord(x, y, z.unwrap_or(0.0), t.unwrap_or(0.0)),
            );
            let error = proj_sys::proj_errno(transform);
            if error != 0 || !output.v[0].is_finite() || !output.v[1].is_finite() {
                if let Some(message) = self.context.take_diagnostic() {
                    execution.record_diagnostic(message);
                }
                return Err(map_proj_transform_error(error));
            }
        }
        if let Some(message) = self.context.take_diagnostic() {
            execution.record_diagnostic(message);
        }
        // SAFETY: DOC-H. Live transform; returns uniquely owned PJ or null.
        let operation = unsafe { proj_sys::proj_trans_get_last_used_operation(transform) };
        // SAFETY: non-null is uniquely owned.
        let Some(operation) = (unsafe { OwnedPj::try_from_owned(operation) }) else {
            let info = self.operation_info(source, target, source_epoch, target_epoch);
            execution.record_operation(info.clone());
            return Ok(info);
        };
        // SAFETY: DOC-H. Typed context + owned operation.
        let normalized = unsafe {
            proj_sys::proj_normalize_for_visualization(self.context.as_ptr(), operation.as_ptr())
        };
        // SAFETY: non-null normalized is uniquely owned.
        let normalized = unsafe { OwnedPj::try_from_owned(normalized) };
        let operation_for_info = normalized.as_ref().unwrap_or(&operation);
        let info = operation_info_from_pj(
            &self.context,
            operation_for_info,
            source,
            target,
            source_epoch,
            target_epoch,
        );
        execution.record_operation(info.clone());
        Ok(info)
    }

    pub(crate) fn operation_info(
        &self,
        source: String,
        target: String,
        source_epoch: Option<f64>,
        target_epoch: Option<f64>,
    ) -> OperationInfo {
        operation_info_from_pj(
            &self.context,
            &self.transform,
            source,
            target,
            source_epoch,
            target_epoch,
        )
    }

    pub(crate) fn roundtrip_errors(
        &self,
        direction: ProjDirection,
        iterations: i32,
        x: &[f64],
        y: &[f64],
        z: Option<&[f64]>,
        t: Option<&[f64]>,
        source_epoch: Option<f64>,
        execution: &TransformExecution,
    ) -> Result<Vec<f64>> {
        let mut roundtrip_x = x.to_vec();
        let mut roundtrip_y = y.to_vec();
        let mut roundtrip_z = z.map(<[f64]>::to_vec);
        let mut roundtrip_t = t.map(<[f64]>::to_vec);
        let mut epoch = source_epoch.map(|value| [value]);
        for _ in 0..iterations {
            // Out and back through the optional-lane kernel; absent z/t lanes
            // pass straight through as null PROJ lanes.
            for leg in [direction, direction.reversed()] {
                self.transform_lanes(
                    &mut roundtrip_x,
                    &mut roundtrip_y,
                    roundtrip_z.as_deref_mut(),
                    roundtrip_t
                        .as_deref_mut()
                        .or_else(|| epoch.as_mut().map(<[f64; 1]>::as_mut_slice)),
                    leg,
                    execution,
                )?;
            }
        }
        Ok(super::operations::roundtrip_error_distances(
            &roundtrip_x,
            &roundtrip_y,
            x,
            y,
            z,
            t,
            roundtrip_z.as_deref(),
            roundtrip_t.as_deref(),
        ))
    }
}

fn first_nonfinite_coordinate(
    x: &[f64],
    y: &[f64],
    z: Option<&[f64]>,
    t: Option<&[f64]>,
) -> Option<usize> {
    (0..x.len()).find(|&index| {
        !x[index].is_finite()
            || !y[index].is_finite()
            || z.is_some_and(|z| !z[index].is_finite())
            || t.is_some_and(|t| !t[if t.len() == 1 { 0 } else { index }].is_finite())
    })
}
