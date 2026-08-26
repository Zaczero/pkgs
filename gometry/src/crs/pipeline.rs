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
    ProjPipeline, ProjTransformOptions, RuntimeState, TransformExecution, TransformOptions,
    coordinates_are_finite, create_crs_transform_object, cstring, operation_info_from_pj,
    proj_context_error_message, proj_transform_error, ptr,
};
use crate::error::Result;

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
        let context = ProjContext::new_with_state(state)
            .map_err(|error| CrsError::transform_create(source, target, error.to_string()))?;
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

    pub(crate) fn from_definition_with_state(
        definition: &str,
        state: &RuntimeState,
    ) -> Result<Self> {
        let definition = cstring(definition)?;
        let context = ProjContext::new_with_state(state).map_err(|error| {
            CrsError::crs_create(definition.to_string_lossy().into_owned(), error.to_string())
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

    pub(crate) fn transform_bounds(
        &self,
        bounds: (f64, f64, f64, f64),
        densify: u32,
        source_epoch: Option<f64>,
        execution: &TransformExecution,
    ) -> Result<(f64, f64, f64, f64)> {
        if source_epoch.is_some() {
            return self.transform_bounds_with_epoch(bounds, densify, source_epoch, execution);
        }
        super::record_transform_engine(false);
        self.context.clear_diagnostic();
        let densify = densify as i32;
        let mut out_xmin = f64::NAN;
        let mut out_ymin = f64::NAN;
        let mut out_xmax = f64::NAN;
        let mut out_ymax = f64::NAN;
        let context = self.context.as_ptr();
        let transform = self.transform.as_ptr();
        // SAFETY: DOC-H + OUT. Typed self owners; six initialized local f64
        // slots exclusive for the call; no Python callback.
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
            if let Some(message) = self.context.take_diagnostic() {
                execution.record_diagnostic(message);
            }
            Ok((out_xmin, out_ymin, out_xmax, out_ymax))
        } else {
            if let Some(message) = self.context.take_diagnostic() {
                execution.record_diagnostic(message);
            }
            // SAFETY: DOC-H. Live transform queried immediately after the failed call.
            let error = unsafe { proj_sys::proj_errno(transform) };
            Err(GeometryErrorKind::projection(proj_transform_error(error)))
        }
    }

    fn transform_bounds_with_epoch(
        &self,
        bounds: (f64, f64, f64, f64),
        densify: u32,
        source_epoch: Option<f64>,
        execution: &TransformExecution,
    ) -> Result<(f64, f64, f64, f64)> {
        let Some(epoch) = source_epoch else {
            unreachable!("epoch bounds path requires a source epoch");
        };
        let segments = densify as usize + 1;
        let edge_points = segments + 1;
        let mut x = Vec::with_capacity(edge_points * 4);
        let mut y = Vec::with_capacity(edge_points * 4);
        let mut edge = |start: (f64, f64), end: (f64, f64)| {
            for index in 0..=segments {
                let fraction = index as f64 / segments as f64;
                x.push(start.0 + (end.0 - start.0) * fraction);
                y.push(start.1 + (end.1 - start.1) * fraction);
            }
        };
        edge((bounds.0, bounds.1), (bounds.2, bounds.1));
        edge((bounds.2, bounds.1), (bounds.2, bounds.3));
        edge((bounds.2, bounds.3), (bounds.0, bounds.3));
        edge((bounds.0, bounds.3), (bounds.0, bounds.1));
        let mut epoch_lane = [epoch];
        self.transform_lanes(
            &mut x,
            &mut y,
            None,
            Some(&mut epoch_lane),
            ProjDirection::Forward,
            execution,
        )?;
        let (mut minx, mut miny) = (x[0], y[0]);
        let (mut maxx, mut maxy) = (x[0], y[0]);
        for (&x, &y) in x.iter().zip(&y).skip(1) {
            minx = minx.min(x);
            miny = miny.min(y);
            maxx = maxx.max(x);
            maxy = maxy.max(y);
        }
        Ok((minx, miny, maxx, maxy))
    }

    fn transform_bounds_3d_with_epoch(
        &self,
        bounds: (f64, f64, f64, f64, f64, f64),
        densify: u32,
        source_epoch: Option<f64>,
        execution: &TransformExecution,
    ) -> Result<(f64, f64, f64, f64, f64, f64)> {
        let Some(epoch) = source_epoch else {
            unreachable!("epoch bounds path requires a source epoch");
        };
        let segments = densify as usize + 1;
        let edge_points = segments + 1;
        let mut x = Vec::with_capacity(edge_points * 12);
        let mut y = Vec::with_capacity(edge_points * 12);
        let mut z = Vec::with_capacity(edge_points * 12);
        let mut edge = |start: (f64, f64, f64), end: (f64, f64, f64)| {
            for index in 0..=segments {
                let fraction = index as f64 / segments as f64;
                x.push(start.0 + (end.0 - start.0) * fraction);
                y.push(start.1 + (end.1 - start.1) * fraction);
                z.push(start.2 + (end.2 - start.2) * fraction);
            }
        };
        let (minx, miny, minz, maxx, maxy, maxz) = bounds;
        for (x0, x1, y0, y1, z0, z1) in [
            (minx, maxx, miny, miny, minz, minz),
            (minx, maxx, miny, miny, maxz, maxz),
            (minx, maxx, maxy, maxy, minz, minz),
            (minx, maxx, maxy, maxy, maxz, maxz),
            (minx, minx, miny, maxy, minz, minz),
            (minx, minx, miny, maxy, maxz, maxz),
            (maxx, maxx, miny, maxy, minz, minz),
            (maxx, maxx, miny, maxy, maxz, maxz),
            (minx, minx, miny, miny, minz, maxz),
            (minx, minx, maxy, maxy, minz, maxz),
            (maxx, maxx, miny, miny, minz, maxz),
            (maxx, maxx, maxy, maxy, minz, maxz),
        ] {
            edge((x0, y0, z0), (x1, y1, z1));
        }
        let mut epoch_lane = [epoch];
        self.transform_lanes(
            &mut x,
            &mut y,
            Some(&mut z),
            Some(&mut epoch_lane),
            ProjDirection::Forward,
            execution,
        )?;
        let (mut minx, mut miny, mut minz) = (x[0], y[0], z[0]);
        let (mut maxx, mut maxy, mut maxz) = (x[0], y[0], z[0]);
        for ((&x, &y), &z) in x.iter().zip(&y).zip(&z).skip(1) {
            minx = minx.min(x);
            miny = miny.min(y);
            minz = minz.min(z);
            maxx = maxx.max(x);
            maxy = maxy.max(y);
            maxz = maxz.max(z);
        }
        Ok((minx, miny, minz, maxx, maxy, maxz))
    }

    pub(crate) fn transform_bounds_3d(
        &self,
        bounds: (f64, f64, f64, f64, f64, f64),
        densify: u32,
        source_epoch: Option<f64>,
        execution: &TransformExecution,
    ) -> Result<(f64, f64, f64, f64, f64, f64)> {
        if densify == 0 {
            return self.transform_bounds_3d_corners(bounds, source_epoch, execution);
        }
        if source_epoch.is_some() {
            return self.transform_bounds_3d_with_epoch(bounds, densify, source_epoch, execution);
        }
        super::record_transform_engine(false);
        self.context.clear_diagnostic();
        let densify = densify as i32;
        let mut out_xmin = f64::NAN;
        let mut out_ymin = f64::NAN;
        let mut out_zmin = f64::NAN;
        let mut out_xmax = f64::NAN;
        let mut out_ymax = f64::NAN;
        let mut out_zmax = f64::NAN;
        let context = self.context.as_ptr();
        let transform = self.transform.as_ptr();
        // SAFETY: DOC-H + OUT. Typed self owners; six initialized local f64
        // slots exclusive for the call; no Python callback.
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
            if let Some(message) = self.context.take_diagnostic() {
                execution.record_diagnostic(message);
            }
            Ok((out_xmin, out_ymin, out_zmin, out_xmax, out_ymax, out_zmax))
        } else {
            if let Some(message) = self.context.take_diagnostic() {
                execution.record_diagnostic(message);
            }
            // SAFETY: DOC-H. Live transform queried immediately after the failed call.
            let error = unsafe { proj_sys::proj_errno(transform) };
            Err(GeometryErrorKind::projection(proj_transform_error(error)))
        }
    }

    pub(crate) fn transform_bounds_3d_corners(
        &self,
        bounds: (f64, f64, f64, f64, f64, f64),
        source_epoch: Option<f64>,
        execution: &TransformExecution,
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
        let mut epoch = source_epoch.map(|value| [value]);
        self.transform_lanes(
            &mut x,
            &mut y,
            Some(&mut z),
            epoch.as_mut().map(<[f64; 1]>::as_mut_slice),
            ProjDirection::Forward,
            execution,
        )?;
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
        execution: &TransformExecution,
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
                Err(GeometryErrorKind::projection(proj_transform_error(error)))
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
                return Err(GeometryErrorKind::projection(proj_transform_error(error)));
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
