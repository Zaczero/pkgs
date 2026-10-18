//! The libPROJ transform-pipeline engine (`impl ProjPipeline`).
//!
//! Wraps a cached `PJ` transformation object and runs coordinate transforms
//! (XY/XYZ/XYZT lanes). Reaches the FFI primitives, thread-local caches, and
//! error helpers in the parent `crs` module via `use super::*`.

use super::transform::map_geographic_conversion_error;
use crate::crs::{
    CrsError, GeometryErrorKind, OperationInfo, OwnedPj, ProjArea, ProjContext, ProjDirection,
    ProjPipeline, ProjTransformOptions, RuntimeState, TransformExecution, TransformFailure,
    TransformOptions, coordinates_are_finite, create_crs_transform_object, cstring,
    operation_info_from_pj, preserve_resource_error, proj_context_error_message,
    proj_transform_error, ptr,
};
use crate::error::{ErrorKind, Result};
use crate::geographic::{GeographicFrame, Latitude};

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
        if !options.force_over {
            super::proj_env::enable_default_angular_units(&context).map_err(|error| {
                preserve_resource_error(error, |error| {
                    CrsError::transform_create(source, target, error.to_string())
                })
            })?;
        }
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
        if !options.force_over {
            // SAFETY: the normalized operation and original endpoint CRS
            // objects are live, uniquely owned by this constructor, and share
            // the creating context until it is moved into `Self`.
            unsafe {
                proj_sys::gometry_proj_configure_default_noop_phase(
                    context.as_ptr(),
                    transform.as_ptr(),
                    source_transform_object.as_ptr(),
                    target_transform_object.as_ptr(),
                );
            }
        }
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
        let mut x_lane = [x];
        let mut y_lane = [y];
        let mut t_lane = epoch.map(|epoch| [epoch]);
        match self.transform_lanes(
            &mut x_lane,
            &mut y_lane,
            None,
            t_lane.as_mut().map(<[f64; 1]>::as_mut_slice),
            direction,
            execution,
        ) {
            Ok(()) => Ok(Some((x_lane[0], y_lane[0]))),
            Err(error)
                if matches!(
                    error.kind(),
                    ErrorKind::Geometry(GeometryErrorKind::ProjectionDomain(_))
                ) =>
            {
                Ok(None)
            },
            Err(error) => Err(error),
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
        z: Option<&mut [f64]>,
        t: Option<&mut [f64]>,
        direction: ProjDirection,
        execution: &TransformExecution,
    ) -> std::result::Result<(), TransformFailure> {
        validate_lane_lengths(x, y, z.as_deref(), t.as_deref())?;
        self.transform_lanes_validated_indexed(x, y, z, t, direction, execution)
    }

    /// Validate default CRS-plan source latitudes before PROJ overwrites their
    /// lanes. If a later latitude is invalid, run only its already-admitted
    /// prefix so an earlier PROJ failure keeps precedence.
    pub(crate) fn transform_default_source_lanes_indexed(
        &self,
        source_frame: Option<GeographicFrame>,
        x: &mut [f64],
        y: &mut [f64],
        mut z: Option<&mut [f64]>,
        mut t: Option<&mut [f64]>,
        execution: &TransformExecution,
    ) -> std::result::Result<(), TransformFailure> {
        validate_lane_lengths(x, y, z.as_deref(), t.as_deref())?;

        if let Some(frame) = source_frame
            && let Some((coordinate, error)) = Latitude::first_invalid(frame, y)
        {
            if coordinate != 0 {
                let prefix_t = t.as_deref_mut().map(|lane| {
                    if lane.len() == 1 {
                        lane
                    } else {
                        lane.get_mut(..coordinate)
                            .expect("the validated T lane contains the source prefix")
                    }
                });
                self.transform_lanes_validated_indexed(
                    x.get_mut(..coordinate)
                        .expect("the validated X lane contains the source prefix"),
                    y.get_mut(..coordinate)
                        .expect("the validated Y lane contains the source prefix"),
                    z.as_deref_mut().map(|lane| {
                        lane.get_mut(..coordinate)
                            .expect("the validated Z lane contains the source prefix")
                    }),
                    prefix_t,
                    ProjDirection::Forward,
                    execution,
                )?;
            }
            return Err(TransformFailure::at(
                map_geographic_conversion_error(error),
                coordinate,
            ));
        }

        self.transform_lanes_validated_indexed(x, y, z, t, ProjDirection::Forward, execution)
    }

    fn transform_lanes_validated_indexed(
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
        // out-of-bounds FFI access (UB). The caller validated these
        // preconditions before entering this helper.
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
        let mut x_lane = [x];
        let mut y_lane = [y];
        let mut z_lane = z.map(|z| [z]);
        let mut t_lane = t.or(source_epoch).map(|t| [t]);
        if t_lane.is_none() {
            self.require_epoch_lane()?;
        }
        self.transform_lanes(
            &mut x_lane,
            &mut y_lane,
            z_lane.as_mut().map(<[f64; 1]>::as_mut_slice),
            t_lane.as_mut().map(<[f64; 1]>::as_mut_slice),
            ProjDirection::Forward,
            execution,
        )?;
        let transform = self.transform.as_ptr();
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

fn validate_lane_lengths(
    x: &[f64],
    y: &[f64],
    z: Option<&[f64]>,
    t: Option<&[f64]>,
) -> std::result::Result<(), TransformFailure> {
    if x.len() != y.len() {
        return Err(TransformFailure::from(GeometryErrorKind::projection(
            "transform lane length mismatch",
        )));
    }
    if z.is_some_and(|z| z.len() != x.len()) {
        return Err(TransformFailure::from(GeometryErrorKind::projection(
            "transform z lane length mismatch",
        )));
    }
    // A length-1 time lane is a valid PROJ broadcast constant: PROJ reads
    // (and writes) only its single element, never advancing the pointer.
    if t.is_some_and(|t| t.len() != x.len() && t.len() != 1) {
        return Err(TransformFailure::from(GeometryErrorKind::projection(
            "transform t lane length mismatch",
        )));
    }
    Ok(())
}

#[expect(
    clippy::indexing_slicing,
    reason = "the FFI entry checks equal X/Y/Z lengths and T is either equal-length or explicitly broadcast from its first element"
)]
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

#[cfg(test)]
mod tests {
    use super::*;

    const SPHERICAL_TM_RADIUS: f64 = 6_371_007.0;
    const SPHERICAL_TM_DEFINITION: &str = "+proj=tmerc +R=6371007 +lon_0=0 +lat_0=0.05 +k=1";

    fn spherical_tm_pipeline(state: &RuntimeState) -> ProjPipeline {
        ProjPipeline::from_definition_with_state(SPHERICAL_TM_DEFINITION, state)
            .expect("spherical transverse Mercator pipeline")
    }

    fn transform_xy(
        pipeline: &ProjPipeline,
        x: &mut [f64],
        y: &mut [f64],
        direction: ProjDirection,
    ) {
        pipeline
            .transform_lanes(x, y, None, None, direction, &TransformExecution::default())
            .expect("spherical transverse Mercator transform");
    }

    fn spherical_tm_scale(pipeline: &ProjPipeline, longitude: f64, latitude: f64) -> f64 {
        // SAFETY: The live projection is owned by `pipeline` on this test
        // thread; the coordinate is passed by value and PROJ retains no pointer.
        let factors = unsafe {
            proj_sys::proj_factors(
                pipeline.transform.as_ptr(),
                proj_sys::proj_coord(longitude, latitude, 0.0, 0.0),
            )
        };
        factors.meridional_scale.max(factors.parallel_scale)
    }

    fn spherical_witness_position(parameter: f64) -> (f64, f64) {
        let longitude =
            -0.003_894_176_784_489_192_4 + parameter * (2.0 * 0.003_894_176_784_489_192_4);
        let latitude = -0.002_248_304_014_796_826 + parameter * (2.0 * 0.002_248_304_014_796_826);
        (longitude.to_radians(), latitude.to_radians())
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
    fn operation_info_at_rejects_nonfinite_optional_output() {
        let _guard = crate::crs::RUNTIME_STATE_TEST_LOCK.lock().unwrap();
        let state = crate::crs::runtime_state_snapshot().unwrap();
        let pipeline = ProjPipeline::from_definition_with_state(
            "+proj=pipeline +step +proj=affine +s33=1e308",
            &state,
        )
        .unwrap();
        let execution = TransformExecution::default();

        let result = pipeline.operation_info_at(
            "source".to_owned(),
            "target".to_owned(),
            None,
            None,
            1.0,
            2.0,
            Some(10.0),
            None,
            &execution,
        );

        assert!(matches!(
            result,
            Err(error)
                if matches!(
                    error.kind(),
                    ErrorKind::Geometry(
                        GeometryErrorKind::Projection(_)
                            | GeometryErrorKind::ProjectionDomain(_),
                    )
            )
        ));
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
    fn transform_lanes_indexed_rejects_mismatches_and_accepts_singleton_time() {
        let _guard = crate::crs::RUNTIME_STATE_TEST_LOCK.lock().unwrap();
        let state = crate::crs::runtime_state_snapshot().unwrap();
        let pipeline = ProjPipeline::from_definition_with_state(
            "+proj=pipeline +step +proj=affine +xoff=1 +yoff=2",
            &state,
        )
        .unwrap();
        let execution = TransformExecution::default();

        let mut empty_x = [];
        let mut one_y = [0.0];
        let xy_mismatch = pipeline
            .transform_lanes_indexed(
                &mut empty_x,
                &mut one_y,
                None,
                None,
                ProjDirection::Forward,
                &execution,
            )
            .unwrap_err();
        assert!(
            xy_mismatch
                .error
                .to_string()
                .contains("transform lane length mismatch")
        );

        let mut empty_x = [];
        let mut empty_y = [];
        let mut one_z = [0.0];
        let z_mismatch = pipeline
            .transform_lanes_indexed(
                &mut empty_x,
                &mut empty_y,
                Some(&mut one_z),
                None,
                ProjDirection::Forward,
                &execution,
            )
            .unwrap_err();
        assert!(
            z_mismatch
                .error
                .to_string()
                .contains("transform z lane length mismatch")
        );

        let mut empty_x = [];
        let mut empty_y = [];
        let mut two_t = [2020.0, 2021.0];
        let t_mismatch = pipeline
            .transform_lanes_indexed(
                &mut empty_x,
                &mut empty_y,
                None,
                Some(&mut two_t),
                ProjDirection::Forward,
                &execution,
            )
            .unwrap_err();
        assert!(
            t_mismatch
                .error
                .to_string()
                .contains("transform t lane length mismatch")
        );

        let mut x = [0.0, 1.0];
        let mut y = [0.0, 1.0];
        let mut singleton_t = [2020.0];
        pipeline
            .transform_lanes_indexed(
                &mut x,
                &mut y,
                None,
                Some(&mut singleton_t),
                ProjDirection::Forward,
                &execution,
            )
            .unwrap();
        assert_eq!(x, [1.0, 2.0]);
        assert_eq!(y, [2.0, 3.0]);
        assert_eq!(singleton_t, [2020.0]);
    }

    #[rstest::rstest]
    #[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
    #[case::half_width_1_0e_5(1.0e-5)]
    #[case::half_width_1_0e_9(1.0e-9)]
    fn spherical_tmerc_forward_scales_through_the_equatorial_acos_band(#[case] half_width: f64) {
        let _guard = crate::crs::RUNTIME_STATE_TEST_LOCK.lock().unwrap();
        let state = crate::crs::runtime_state_snapshot().unwrap();
        let pipeline = spherical_tm_pipeline(&state);
        let center = 0.500_134_258_785_931_5;

        let start = spherical_witness_position(center - half_width);
        let end = spherical_witness_position(center + half_width);
        let middle = spherical_witness_position(center);
        let delta_longitude = end.0 - start.0;
        let delta_latitude = end.1 - start.1;
        let middle_latitude = f64::midpoint(start.1, end.1);
        let source_length =
            SPHERICAL_TM_RADIUS * (delta_longitude * middle_latitude.cos()).hypot(delta_latitude);
        let local_scale = [start, middle, end]
            .into_iter()
            .map(|(longitude, latitude)| spherical_tm_scale(&pipeline, longitude, latitude))
            .fold(0.0_f64, f64::max);

        let mut x = [start.0, end.0];
        let mut y = [start.1, end.1];
        transform_xy(&pipeline, &mut x, &mut y, ProjDirection::Forward);
        let [x0, x1] = x;
        let [y0, y1] = y;
        let image_separation = (x1 - x0).hypot(y1 - y0);

        assert!(
            image_separation <= 2.0 * source_length * local_scale,
            "half-width={half_width:e}, source={source_length:e} m, \
             image={image_separation:e} m, local scale={local_scale:e}"
        );
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
    fn spherical_tmerc_zero_latitude_backside_is_positive_pi() {
        let _guard = crate::crs::RUNTIME_STATE_TEST_LOCK.lock().unwrap();
        let state = crate::crs::runtime_state_snapshot().unwrap();
        let pipeline = spherical_tm_pipeline(&state);
        let mut x = [2.0, -2.0, 2.0, -2.0];
        let mut y = [0.0, 0.0, -0.0, -0.0];
        transform_xy(&pipeline, &mut x, &mut y, ProjDirection::Forward);

        let expected = SPHERICAL_TM_RADIUS * (std::f64::consts::PI - 0.05_f64.to_radians());
        assert_eq!(y, [expected; 4]);
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
    fn spherical_tmerc_inverse_scales_near_the_pole_and_roundtrips() {
        let _guard = crate::crs::RUNTIME_STATE_TEST_LOCK.lock().unwrap();
        let state = crate::crs::runtime_state_snapshot().unwrap();
        let pipeline = spherical_tm_pipeline(&state);
        let latitude_of_origin = 0.05_f64.to_radians();

        for interval_length in [0.02, 2.0e-6] {
            let center_northing =
                SPHERICAL_TM_RADIUS * (std::f64::consts::FRAC_PI_2 - 5.0e-9 - latitude_of_origin);
            let expected_y = [
                center_northing - interval_length * 0.5,
                center_northing + interval_length * 0.5,
            ];
            let mut x = [0.0, 0.0];
            let mut y = expected_y;
            transform_xy(&pipeline, &mut x, &mut y, ProjDirection::Inverse);

            let [longitude0, longitude1] = x;
            let [latitude0, latitude1] = y;
            let mean_latitude = f64::midpoint(latitude0, latitude1);
            let image_separation = SPHERICAL_TM_RADIUS
                * ((longitude1 - longitude0) * mean_latitude.cos()).hypot(latitude1 - latitude0);
            assert!(
                image_separation >= interval_length * 0.5,
                "interval={interval_length:e} m, inverse image={image_separation:e} m"
            );

            transform_xy(&pipeline, &mut x, &mut y, ProjDirection::Forward);
            let [x0, x1] = x;
            assert!(
                x0.abs() <= 1.0e-6 && x1.abs() <= 1.0e-6,
                "inverse/forward easting errors: {x0:e}, {x1:e}"
            );
            for (actual, expected) in y.into_iter().zip(expected_y) {
                assert!(
                    (actual - expected).abs() <= 1.0e-6,
                    "inverse/forward northing error: actual={actual:e}, expected={expected:e}"
                );
            }
        }

        let latitudes = [
            -std::f64::consts::FRAC_PI_2,
            -std::f64::consts::FRAC_PI_2 + 1.0e-8,
            -1.05e-8,
            -0.0,
            0.0,
            1.05e-8,
            std::f64::consts::FRAC_PI_2 - 1.0e-8,
            std::f64::consts::FRAC_PI_2,
        ];
        let longitudes = [-2.0, -0.2, 0.0, 0.2, 2.0];
        let mut x = Vec::with_capacity(latitudes.len() * longitudes.len());
        let mut y = Vec::with_capacity(latitudes.len() * longitudes.len());
        for latitude in latitudes {
            for longitude in longitudes {
                x.push(longitude);
                y.push(latitude);
            }
        }
        transform_xy(&pipeline, &mut x, &mut y, ProjDirection::Forward);
        let expected_x = x.clone();
        let expected_y = y.clone();
        transform_xy(&pipeline, &mut x, &mut y, ProjDirection::Inverse);
        transform_xy(&pipeline, &mut x, &mut y, ProjDirection::Forward);

        for ((actual_x, expected_x), (actual_y, expected_y)) in x
            .into_iter()
            .zip(expected_x)
            .zip(y.into_iter().zip(expected_y))
        {
            assert!(
                (actual_x - expected_x).abs() <= 1.0e-6 && (actual_y - expected_y).abs() <= 1.0e-6,
                "forward/inverse/forward error: ({actual_x:e}, {actual_y:e}) \
                 vs ({expected_x:e}, {expected_y:e})"
            );
        }
    }
}
