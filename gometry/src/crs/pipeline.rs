#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! The libPROJ transform-pipeline engine (`impl ProjPipeline`).
//!
//! Wraps a cached `PJ` transformation object and runs coordinate transforms
//! (XY/XYZ/XYZT lanes). Reaches the FFI primitives, thread-local caches, and
//! error helpers in the parent `crs` module via `use super::*`.

use super::*;
use crate::error::Result;

impl ProjPipeline {
    pub(crate) fn new(source: &str, target: &str, options: &TransformOptions) -> Result<Self> {
        let source_crs = cstring(source)?;
        let target_crs = cstring(target)?;
        let area = options.area_of_interest.map(ProjArea::new).transpose()?;
        let transform_options = ProjTransformOptions::new(options)?;
        let context = ProjContext::new()
            .map_err(|error| CrsError::transform_create(source, target, error.to_string()))?;
        let source_transform_object = create_crs_transform_object(
            context.as_ptr(),
            source_crs.as_ptr(),
            source,
            options.source_epoch,
        )?;
        let target_transform_object = create_crs_transform_object(
            context.as_ptr(),
            target_crs.as_ptr(),
            target,
            options.target_epoch,
        )?;
        // SAFETY: `context`, `source_transform_object`, and `target_transform_object`
        // are valid, non-null PJ pointers owned by live RAII guards for this call;
        // `area` and `transform_options` are valid-or-null per PROJ's C API.
        let raw = unsafe {
            proj_sys::proj_create_crs_to_crs_from_pj(
                context.as_ptr(),
                source_transform_object.as_ptr(),
                target_transform_object.as_ptr(),
                area.as_ref().map_or(ptr::null_mut(), ProjArea::as_ptr),
                transform_options.as_ptr(),
            )
        };
        if raw.is_null() {
            let message = proj_context_error_message(context.as_ptr());
            return Err(CrsError::transform_create(source, target, message));
        }
        // SAFETY: `raw` was just checked non-null and is owned by this guard.
        let raw = unsafe { OwnedPj::from_owned(raw) };
        // SAFETY: `context` and `raw` are valid, non-null PJ pointers (`raw` was just
        // null-checked) owned by live guards for the duration of the call.
        let transform =
            unsafe { proj_sys::proj_normalize_for_visualization(context.as_ptr(), raw.as_ptr()) };
        if transform.is_null() {
            return Err(CrsError::transform_create(
                source,
                target,
                "PROJ could not normalize the CRS operation for X/Y order",
            ));
        }
        // SAFETY: `transform` was just checked non-null and is owned by this guard.
        let transform = unsafe { OwnedPj::from_owned(transform) };
        Ok(Self { transform, context })
    }

    pub(crate) fn requires_coordinate_epoch(&self) -> bool {
        // SAFETY: self.context and self.transform are valid PROJ handles.
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

    pub(crate) fn from_definition(definition: &str) -> Result<Self> {
        let definition = cstring(definition)?;
        let context = ProjContext::new().map_err(|error| {
            CrsError::crs_create(definition.to_string_lossy().into_owned(), error.to_string())
        })?;
        // SAFETY: `context` is a valid live PJ_CONTEXT and `definition` is a valid
        // NUL-terminated C string that outlives this call.
        let transform = unsafe { proj_sys::proj_create(context.as_ptr(), definition.as_ptr()) };
        if transform.is_null() {
            let message = proj_context_error_message(context.as_ptr());
            return Err(CrsError::crs_create(
                definition.to_string_lossy().into_owned(),
                message,
            ));
        }
        // SAFETY: `transform` was just checked non-null and is owned by this guard.
        let transform = unsafe { OwnedPj::from_owned(transform) };
        Ok(Self { transform, context })
    }

    pub(crate) fn transform_bounds(
        &self,
        bounds: (f64, f64, f64, f64),
        densify: u32,
    ) -> Result<(f64, f64, f64, f64)> {
        let densify = densify as i32;
        let mut out_xmin = f64::NAN;
        let mut out_ymin = f64::NAN;
        let mut out_xmax = f64::NAN;
        let mut out_ymax = f64::NAN;
        let context = self.context.as_ptr();
        let transform = self.transform.as_ptr();
        // SAFETY: self owns a valid PROJ context/transform. Output pointers
        // reference local f64 storage for PROJ to fill.
        let ok = unsafe {
            proj_sys::proj_errno_reset(transform);
            proj_sys::proj_trans_bounds(
                context,
                transform,
                ProjDirection::Forward.to_proj(),
                bounds.0,
                bounds.1,
                bounds.2,
                bounds.3,
                &raw mut out_xmin,
                &raw mut out_ymin,
                &raw mut out_xmax,
                &raw mut out_ymax,
                densify,
            )
        };
        if ok != 0
            && [out_xmin, out_ymin, out_xmax, out_ymax]
                .iter()
                .all(|value| value.is_finite())
        {
            Ok((out_xmin, out_ymin, out_xmax, out_ymax))
        } else {
            // SAFETY: self owns the live PROJ transform queried immediately after
            // the failed bounds transform on the same object.
            let error = unsafe { proj_sys::proj_errno(transform) };
            Err(GeometryErrorKind::projection(proj_transform_error(error)))
        }
    }

    pub(crate) fn transform_bounds_3d(
        &self,
        bounds: (f64, f64, f64, f64, f64, f64),
        densify: u32,
    ) -> Result<(f64, f64, f64, f64, f64, f64)> {
        if densify == 0 {
            return self.transform_bounds_3d_corners(bounds);
        }
        let densify = densify as i32;
        let mut out_xmin = f64::NAN;
        let mut out_ymin = f64::NAN;
        let mut out_zmin = f64::NAN;
        let mut out_xmax = f64::NAN;
        let mut out_ymax = f64::NAN;
        let mut out_zmax = f64::NAN;
        let context = self.context.as_ptr();
        let transform = self.transform.as_ptr();
        // SAFETY: self owns a valid PROJ context/transform. Output pointers
        // reference local f64 storage for PROJ to fill.
        let ok = unsafe {
            proj_sys::proj_errno_reset(transform);
            proj_sys::proj_trans_bounds_3D(
                context,
                transform,
                ProjDirection::Forward.to_proj(),
                bounds.0,
                bounds.1,
                bounds.2,
                bounds.3,
                bounds.4,
                bounds.5,
                &raw mut out_xmin,
                &raw mut out_ymin,
                &raw mut out_zmin,
                &raw mut out_xmax,
                &raw mut out_ymax,
                &raw mut out_zmax,
                densify,
            )
        };
        if ok != 0
            && [out_xmin, out_ymin, out_zmin, out_xmax, out_ymax, out_zmax]
                .iter()
                .all(|value| value.is_finite())
        {
            Ok((out_xmin, out_ymin, out_zmin, out_xmax, out_ymax, out_zmax))
        } else {
            // SAFETY: self owns the live PROJ transform queried immediately after
            // the failed bounds transform on the same object.
            let error = unsafe { proj_sys::proj_errno(transform) };
            Err(GeometryErrorKind::projection(proj_transform_error(error)))
        }
    }

    pub(crate) fn transform_bounds_3d_corners(
        &self,
        bounds: (f64, f64, f64, f64, f64, f64),
    ) -> Result<(f64, f64, f64, f64, f64, f64)> {
        let mut x = [
            bounds.0, bounds.0, bounds.0, bounds.0, bounds.3, bounds.3, bounds.3, bounds.3,
        ];
        let mut y = [
            bounds.1, bounds.1, bounds.4, bounds.4, bounds.1, bounds.1, bounds.4, bounds.4,
        ];
        let mut z = [
            bounds.2, bounds.5, bounds.2, bounds.5, bounds.2, bounds.5, bounds.2, bounds.5,
        ];
        self.transform_lanes(&mut x, &mut y, Some(&mut z), None, ProjDirection::Forward)?;
        let mut xmin = x[0];
        let mut ymin = y[0];
        let mut zmin = z[0];
        let mut xmax = x[0];
        let mut ymax = y[0];
        let mut zmax = z[0];
        for index in 1..x.len() {
            xmin = xmin.min(x[index]);
            ymin = ymin.min(y[index]);
            zmin = zmin.min(z[index]);
            xmax = xmax.max(x[index]);
            ymax = ymax.max(y[index]);
            zmax = zmax.max(z[index]);
        }
        Ok((xmin, ymin, zmin, xmax, ymax, zmax))
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
        mut z: Option<&mut [f64]>,
        mut t: Option<&mut [f64]>,
        direction: ProjDirection,
    ) -> Result<()> {
        // The `unsafe` `proj_trans_generic` call below reads/writes each lane
        // up to `x.len()` elements, so a length mismatch would be an
        // out-of-bounds FFI access (UB). Keep those safety preconditions
        // fallible across the public path; callers pass equal-length CoordSeq
        // columns by construction, so these do not fire in normal use.
        if x.len() != y.len() {
            return Err(GeometryErrorKind::projection(
                "transform lane length mismatch",
            ));
        }
        if z.as_deref().is_some_and(|z| z.len() != x.len()) {
            return Err(GeometryErrorKind::projection(
                "transform z lane length mismatch",
            ));
        }
        // A length-1 time lane is a valid PROJ broadcast constant: PROJ reads
        // (and writes) only its single element, never advancing the pointer,
        // so it stays in bounds regardless of `x.len()`. Any other length must
        // match `x.len()` for the same OOB reason as x/y/z.
        if t.as_deref()
            .is_some_and(|t| t.len() != x.len() && t.len() != 1)
        {
            return Err(GeometryErrorKind::projection(
                "transform t lane length mismatch",
            ));
        }
        if x.is_empty() {
            return Ok(());
        }
        // SAFETY: x/y (and any present z/t) are same-length mutable f64 slices;
        // strides are expressed in bytes as PROJ requires; absent lanes pass a
        // null pointer with zero stride/count so PROJ neither reads nor writes
        // them. The raw lane pointers alias the slices only for the duration of
        // the FFI call, after which the slices are re-borrowed for validation.
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
            if transformed == x.len() && coordinates_are_finite(x, y, z.as_deref(), t.as_deref()) {
                Ok(())
            } else {
                let error = proj_sys::proj_errno(transform);
                Err(GeometryErrorKind::projection(proj_transform_error(error)))
            }
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
    ) -> Result<OperationInfo> {
        // SAFETY: self owns a valid PROJ context/transform. proj_trans operates
        // on a value coordinate, and the last-used operation returned by PROJ is
        // owned by the caller and destroyed below.
        unsafe {
            let context = self.context.as_ptr();
            let transform = self.transform.as_ptr();
            proj_sys::proj_errno_reset(transform);
            let output = proj_sys::proj_trans(
                transform,
                ProjDirection::Forward.to_proj(),
                proj_sys::proj_coord(x, y, z.unwrap_or(0.0), t.unwrap_or(0.0)),
            );
            let error = proj_sys::proj_errno(transform);
            if error != 0 || !output.v[0].is_finite() || !output.v[1].is_finite() {
                return Err(GeometryErrorKind::projection(proj_transform_error(error)));
            }
            let operation = proj_sys::proj_trans_get_last_used_operation(transform);
            if operation.is_null() {
                return Ok(self.operation_info(source, target, source_epoch, target_epoch));
            }
            // SAFETY: `operation` was just checked non-null and is owned by this guard.
            let operation = OwnedPj::from_owned(operation);
            let normalized =
                proj_sys::proj_normalize_for_visualization(context, operation.as_ptr());
            let normalized = if normalized.is_null() {
                None
            } else {
                // SAFETY: `normalized` was just checked non-null and is owned by this guard.
                Some(OwnedPj::from_owned(normalized))
            };
            let operation_for_info = normalized
                .as_ref()
                .map_or(operation.as_ptr(), OwnedPj::as_ptr);
            let operation_for_info = BorrowedPj::from_borrowed(operation_for_info)
                .expect("operation_for_info is non-null");
            let info = operation_info_from_pj(
                context,
                operation_for_info.as_ptr(),
                source,
                target,
                source_epoch,
                target_epoch,
            );
            Ok(info)
        }
    }

    pub(crate) fn operation_info(
        &self,
        source: String,
        target: String,
        source_epoch: Option<f64>,
        target_epoch: Option<f64>,
    ) -> OperationInfo {
        operation_info_from_pj(
            self.context.as_ptr(),
            self.transform.as_ptr(),
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
    ) -> Result<Vec<f64>> {
        let mut roundtrip_x = x.to_vec();
        let mut roundtrip_y = y.to_vec();
        let mut roundtrip_z = z.map(<[f64]>::to_vec);
        let mut roundtrip_t = t.map(<[f64]>::to_vec);
        for _ in 0..iterations {
            // Out and back through the optional-lane kernel; absent z/t lanes
            // pass straight through as null PROJ lanes.
            for leg in [direction, direction.reversed()] {
                self.transform_lanes(
                    &mut roundtrip_x,
                    &mut roundtrip_y,
                    roundtrip_z.as_deref_mut(),
                    roundtrip_t.as_deref_mut(),
                    leg,
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
