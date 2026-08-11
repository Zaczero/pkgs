//! CRS transform roundtrip-error metrics and operation enumeration
//! (`roundtrip_errors`/`operations_info`).
//!
//! `&str`-keyed; the raw-pointer PROJ-list helpers stay private in the parent
//! `crs` module and are reached via `use super::*`; re-exported at `crs`.

use std::simd::StdFloat as _;
use std::simd::cmp::SimdPartialEq as _;
use std::simd::num::SimdFloat as _;

use crate::crs::{
    CRS_OPERATIONS_CACHE, CRS_OPERATIONS_CACHE_CAPACITY, CachedCrsOperations, CrsError,
    OperationInfo, OwnedPj, ProjContext, ProjDirection, ProjObjList, ProjOperationFactoryContext,
    TransformOptions, Transformer, create_crs_transform_object, cstring,
    ensure_thread_caches_current, is_wgs84_lonlat, lonlat_to_web_mercator_xy, lru_resolve,
    normalize_pair, operation_info, operation_info_from_pj, proj_context_error_message,
    validate_coordinate_lanes, web_mercator_to_lonlat_xy, with_proj_diagnostic_pipeline,
};
use crate::error::Result;
use crate::geometry::{REDUCE_LANES, REDUCE_SIMD_MIN, ReduceSimd, simd_indexed_map_f64};

pub(crate) fn roundtrip_errors(
    source: &str,
    target: &str,
    x: &[f64],
    y: &[f64],
    zt: crate::ZtLaneRefs<'_>,
    iterations: i32,
    direction: ProjDirection,
    options: &TransformOptions,
) -> Result<Vec<f64>> {
    if iterations <= 0 {
        return Err(CrsError::invalid(format!(
            "roundtrip iterations must be a positive integer, got {iterations}"
        )));
    }
    validate_coordinate_lanes(x, y, zt)?;
    let (z, t) = match zt {
        crate::Zt::None => (None, None),
        crate::Zt::Z(z) => (Some(z), None),
        crate::Zt::T(t) => (None, Some(t)),
        crate::Zt::Zt { z, t } => (Some(z), Some(t)),
    };
    options.validate()?;
    let (source, target) = normalize_pair(source, target)?;
    if options.allows_in_core() && z.is_none() && t.is_none() {
        if is_wgs84_lonlat(&source) && target == "EPSG:3857" {
            return roundtrip_web_mercator_errors(x, y, direction, iterations, true)
                .map_err(|error| CrsError::transform(source, target, error.to_string()));
        }
        if source == "EPSG:3857" && is_wgs84_lonlat(&target) {
            return roundtrip_web_mercator_errors(x, y, direction, iterations, false)
                .map_err(|error| CrsError::transform(source, target, error.to_string()));
        }
    }
    if options.allows_in_core() {
        let forward = Transformer::new_with_options(&source, &target, options.clone());
        let reverse = Transformer::new(&target, &source);
        if forward.is_in_core() && reverse.is_in_core() {
            return roundtrip_errors_with_transformers(
                &forward, &reverse, direction, iterations, x, y, zt,
            )
            .map_err(|error| CrsError::transform(source, target, error.to_string()));
        }
    }
    with_proj_diagnostic_pipeline(&source, &target, options, |pipeline| {
        pipeline.roundtrip_errors(direction, iterations, x, y, z, t)
    })
}

enum RoundtripZt<'a> {
    None,
    Z {
        original: &'a [f64],
        values: Vec<f64>,
    },
    T {
        original: &'a [f64],
        values: Vec<f64>,
    },
    Zt {
        original_z: &'a [f64],
        original_t: &'a [f64],
        z: Vec<f64>,
        t: Vec<f64>,
    },
}

impl<'a> RoundtripZt<'a> {
    fn from_lanes(zt: crate::ZtLaneRefs<'a>) -> Self {
        match zt {
            crate::Zt::None => Self::None,
            crate::Zt::Z(z) => Self::Z {
                original: z,
                values: z.to_vec(),
            },
            crate::Zt::T(t) => Self::T {
                original: t,
                values: t.to_vec(),
            },
            crate::Zt::Zt { z, t } => Self::Zt {
                original_z: z,
                original_t: t,
                z: z.to_vec(),
                t: t.to_vec(),
            },
        }
    }

    fn transform_with(
        &mut self,
        transformer: &Transformer,
        x: &mut [f64],
        y: &mut [f64],
    ) -> Result<()> {
        match self {
            Self::None => transformer.transform_coordinates(x, y, crate::Zt::None),
            Self::Z { values, .. } => {
                transformer.transform_coordinates(x, y, crate::Zt::Z(values.as_mut_slice()))
            },
            Self::T { values, .. } => {
                transformer.transform_coordinates(x, y, crate::Zt::T(values.as_mut_slice()))
            },
            Self::Zt { z, t, .. } => transformer.transform_coordinates(x, y, crate::Zt::Zt {
                z: z.as_mut_slice(),
                t: t.as_mut_slice(),
            }),
        }
    }

    const fn error_lanes(&self) -> crate::Zt<(&[f64], &[f64])> {
        match self {
            Self::None => crate::Zt::None,
            Self::Z { original, values } => crate::Zt::Z((*original, values.as_slice())),
            Self::T { original, values } => crate::Zt::T((*original, values.as_slice())),
            Self::Zt {
                original_z,
                original_t,
                z,
                t,
            } => crate::Zt::Zt {
                z: (*original_z, z.as_slice()),
                t: (*original_t, t.as_slice()),
            },
        }
    }
}

fn roundtrip_errors_with_transformers(
    forward: &Transformer,
    reverse: &Transformer,
    direction: ProjDirection,
    iterations: i32,
    x: &[f64],
    y: &[f64],
    zt: crate::ZtLaneRefs<'_>,
) -> Result<Vec<f64>> {
    let mut roundtrip_x = x.to_vec();
    let mut roundtrip_y = y.to_vec();
    let mut roundtrip_zt = RoundtripZt::from_lanes(zt);
    let (first, second) = match direction {
        ProjDirection::Forward => (forward, reverse),
        ProjDirection::Inverse => (reverse, forward),
    };
    for _ in 0..iterations {
        roundtrip_zt.transform_with(first, &mut roundtrip_x, &mut roundtrip_y)?;
        roundtrip_zt.transform_with(second, &mut roundtrip_x, &mut roundtrip_y)?;
    }
    Ok(roundtrip_error_distances_zt(
        &roundtrip_x,
        &roundtrip_y,
        x,
        y,
        roundtrip_zt.error_lanes(),
    ))
}

fn roundtrip_web_mercator_errors(
    x: &[f64],
    y: &[f64],
    direction: ProjDirection,
    iterations: i32,
    forward_is_lonlat_to_web: bool,
) -> Result<Vec<f64>> {
    let mut roundtrip_x = x.to_vec();
    let mut roundtrip_y = y.to_vec();
    for _ in 0..iterations {
        let first_is_lonlat_to_web = match direction {
            ProjDirection::Forward => forward_is_lonlat_to_web,
            ProjDirection::Inverse => !forward_is_lonlat_to_web,
        };
        transform_web_mercator_coordinates(
            &mut roundtrip_x,
            &mut roundtrip_y,
            first_is_lonlat_to_web,
        )?;
        transform_web_mercator_coordinates(
            &mut roundtrip_x,
            &mut roundtrip_y,
            !first_is_lonlat_to_web,
        )?;
    }
    let count = roundtrip_x.len();
    let mut errors = vec![0.0; count];
    let (rtx, _) = roundtrip_x.as_chunks::<REDUCE_LANES>();
    let (rty, _) = roundtrip_y.as_chunks::<REDUCE_LANES>();
    let (ox, _) = x.as_chunks::<REDUCE_LANES>();
    let (oy, _) = y.as_chunks::<REDUCE_LANES>();
    simd_indexed_map_f64(
        count,
        &mut errors,
        |index| {
            let dx = roundtrip_x[index] - x[index];
            let dy = roundtrip_y[index] - y[index];
            roundtrip_norm(dx, dy, 0.0, 0.0)
        },
        |start| {
            let chunk = start / REDUCE_LANES;
            let dx = ReduceSimd::from_array(rtx[chunk]) - ReduceSimd::from_array(ox[chunk]);
            let dy = ReduceSimd::from_array(rty[chunk]) - ReduceSimd::from_array(oy[chunk]);
            roundtrip_norm_lanes(dx, dy, ReduceSimd::splat(0.0), ReduceSimd::splat(0.0))
        },
    );
    Ok(errors)
}

pub(crate) fn roundtrip_error_distances(
    roundtrip_x: &[f64],
    roundtrip_y: &[f64],
    original_x: &[f64],
    original_y: &[f64],
    original_z: Option<&[f64]>,
    original_t: Option<&[f64]>,
    roundtrip_z: Option<&[f64]>,
    roundtrip_t: Option<&[f64]>,
) -> Vec<f64> {
    let count = roundtrip_x.len();
    let mut errors = vec![0.0; count];
    simd_indexed_map_f64(
        count,
        &mut errors,
        |index| {
            roundtrip_error_at(
                index,
                roundtrip_x,
                roundtrip_y,
                original_x,
                original_y,
                original_z,
                original_t,
                roundtrip_z,
                roundtrip_t,
            )
        },
        |start| {
            let chunk = start / REDUCE_LANES;
            let dx = roundtrip_lane_delta(roundtrip_x, original_x, chunk);
            let dy = roundtrip_lane_delta(roundtrip_y, original_y, chunk);
            let dz = roundtrip_optional_lane_delta(original_z, roundtrip_z, chunk);
            let dt = roundtrip_optional_lane_delta(original_t, roundtrip_t, chunk);
            roundtrip_norm_lanes(dx, dy, dz, dt)
        },
    );
    errors
}

#[expect(
    clippy::large_types_passed_by_value,
    reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow"
)]
pub(crate) fn roundtrip_error_distances_zt(
    roundtrip_x: &[f64],
    roundtrip_y: &[f64],
    original_x: &[f64],
    original_y: &[f64],
    zt: crate::Zt<(&[f64], &[f64])>,
) -> Vec<f64> {
    let count = roundtrip_x.len();
    // Length-exact reborrows: every column is count-long so zip/as_chunks
    // paths carry no free-index bounds checks (elision).
    let roundtrip_x = &roundtrip_x[..count];
    let roundtrip_y = &roundtrip_y[..count];
    let original_x = &original_x[..count];
    let original_y = &original_y[..count];
    let zt = match zt {
        crate::Zt::None => crate::Zt::None,
        crate::Zt::Z((original_z, roundtrip_z)) => {
            crate::Zt::Z((&original_z[..count], &roundtrip_z[..count]))
        },
        crate::Zt::T((original_t, roundtrip_t)) => {
            crate::Zt::T((&original_t[..count], &roundtrip_t[..count]))
        },
        crate::Zt::Zt {
            z: (original_z, roundtrip_z),
            t: (original_t, roundtrip_t),
        } => crate::Zt::Zt {
            z: (&original_z[..count], &roundtrip_z[..count]),
            t: (&original_t[..count], &roundtrip_t[..count]),
        },
    };
    let mut errors = vec![0.0; count];
    if count < REDUCE_SIMD_MIN {
        roundtrip_error_zt_scalar(
            &mut errors,
            roundtrip_x,
            roundtrip_y,
            original_x,
            original_y,
            zt,
        );
        return errors;
    }
    let chunks = count / REDUCE_LANES;
    {
        let (out_chunks, _) = errors.as_chunks_mut::<REDUCE_LANES>();
        for (chunk, out_chunk) in out_chunks.iter_mut().enumerate().take(chunks) {
            let dx = roundtrip_lane_delta(roundtrip_x, original_x, chunk);
            let dy = roundtrip_lane_delta(roundtrip_y, original_y, chunk);
            let (dz, dt) = match zt {
                crate::Zt::None => (ReduceSimd::splat(0.0), ReduceSimd::splat(0.0)),
                crate::Zt::Z((original_z, roundtrip_z)) => (
                    roundtrip_lane_delta(roundtrip_z, original_z, chunk),
                    ReduceSimd::splat(0.0),
                ),
                crate::Zt::T((original_t, roundtrip_t)) => (
                    ReduceSimd::splat(0.0),
                    roundtrip_lane_delta(roundtrip_t, original_t, chunk),
                ),
                crate::Zt::Zt {
                    z: (original_z, roundtrip_z),
                    t: (original_t, roundtrip_t),
                } => (
                    roundtrip_lane_delta(roundtrip_z, original_z, chunk),
                    roundtrip_lane_delta(roundtrip_t, original_t, chunk),
                ),
            };
            roundtrip_norm_lanes(dx, dy, dz, dt).copy_to_slice(out_chunk);
        }
    }
    let lanes = chunks * REDUCE_LANES;
    if lanes < count {
        roundtrip_error_zt_scalar(
            &mut errors[lanes..],
            &roundtrip_x[lanes..],
            &roundtrip_y[lanes..],
            &original_x[lanes..],
            &original_y[lanes..],
            match zt {
                crate::Zt::None => crate::Zt::None,
                crate::Zt::Z((original_z, roundtrip_z)) => {
                    crate::Zt::Z((&original_z[lanes..], &roundtrip_z[lanes..]))
                },
                crate::Zt::T((original_t, roundtrip_t)) => {
                    crate::Zt::T((&original_t[lanes..], &roundtrip_t[lanes..]))
                },
                crate::Zt::Zt {
                    z: (original_z, roundtrip_z),
                    t: (original_t, roundtrip_t),
                } => crate::Zt::Zt {
                    z: (&original_z[lanes..], &roundtrip_z[lanes..]),
                    t: (&original_t[lanes..], &roundtrip_t[lanes..]),
                },
            },
        );
    }
    errors
}

/// Scalar zip fill for roundtrip error (length-exact column zips — no free
/// index bounds checks).
#[expect(
    clippy::large_types_passed_by_value,
    reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow"
)]
fn roundtrip_error_zt_scalar(
    out: &mut [f64],
    roundtrip_x: &[f64],
    roundtrip_y: &[f64],
    original_x: &[f64],
    original_y: &[f64],
    zt: crate::Zt<(&[f64], &[f64])>,
) {
    match zt {
        crate::Zt::None => {
            for (slot, ((&rx, &ry), (&ox, &oy))) in out.iter_mut().zip(
                std::iter::zip(roundtrip_x, roundtrip_y)
                    .zip(std::iter::zip(original_x, original_y)),
            ) {
                let dx = rx - ox;
                let dy = ry - oy;
                *slot = roundtrip_norm(dx, dy, 0.0, 0.0);
            }
        },
        crate::Zt::Z((original_z, roundtrip_z)) => {
            for (slot, (((&rx, &ry), (&ox, &oy)), (&oz, &rz))) in out.iter_mut().zip(
                std::iter::zip(roundtrip_x, roundtrip_y)
                    .zip(std::iter::zip(original_x, original_y))
                    .zip(std::iter::zip(original_z, roundtrip_z)),
            ) {
                let dx = rx - ox;
                let dy = ry - oy;
                let dz = rz - oz;
                *slot = roundtrip_norm(dx, dy, dz, 0.0);
            }
        },
        crate::Zt::T((original_t, roundtrip_t)) => {
            for (slot, (((&rx, &ry), (&ox, &oy)), (&ot, &rt))) in out.iter_mut().zip(
                std::iter::zip(roundtrip_x, roundtrip_y)
                    .zip(std::iter::zip(original_x, original_y))
                    .zip(std::iter::zip(original_t, roundtrip_t)),
            ) {
                let dx = rx - ox;
                let dy = ry - oy;
                let dt = rt - ot;
                *slot = roundtrip_norm(dx, dy, 0.0, dt);
            }
        },
        crate::Zt::Zt {
            z: (original_z, roundtrip_z),
            t: (original_t, roundtrip_t),
        } => {
            for (slot, ((((&rx, &ry), (&ox, &oy)), (&oz, &rz)), (&ot, &rt))) in out.iter_mut().zip(
                std::iter::zip(roundtrip_x, roundtrip_y)
                    .zip(std::iter::zip(original_x, original_y))
                    .zip(std::iter::zip(original_z, roundtrip_z))
                    .zip(std::iter::zip(original_t, roundtrip_t)),
            ) {
                let dx = rx - ox;
                let dy = ry - oy;
                let dz = rz - oz;
                let dt = rt - ot;
                *slot = roundtrip_norm(dx, dy, dz, dt);
            }
        },
    }
}

fn roundtrip_lane_delta(values: &[f64], original: &[f64], chunk: usize) -> ReduceSimd {
    let (values, _) = values.as_chunks::<REDUCE_LANES>();
    let (original, _) = original.as_chunks::<REDUCE_LANES>();
    ReduceSimd::from_array(values[chunk]) - ReduceSimd::from_array(original[chunk])
}

fn roundtrip_optional_lane_delta(
    original: Option<&[f64]>,
    roundtrip: Option<&[f64]>,
    chunk: usize,
) -> ReduceSimd {
    match (original, roundtrip) {
        (Some(original), Some(roundtrip)) => roundtrip_lane_delta(roundtrip, original, chunk),
        _ => ReduceSimd::splat(0.0),
    }
}

fn roundtrip_error_at(
    index: usize,
    roundtrip_x: &[f64],
    roundtrip_y: &[f64],
    original_x: &[f64],
    original_y: &[f64],
    original_z: Option<&[f64]>,
    original_t: Option<&[f64]>,
    roundtrip_z: Option<&[f64]>,
    roundtrip_t: Option<&[f64]>,
) -> f64 {
    let dx = roundtrip_x[index] - original_x[index];
    let dy = roundtrip_y[index] - original_y[index];
    let dz = original_z
        .zip(roundtrip_z)
        .map_or(0.0, |(original, result)| result[index] - original[index]);
    let dt = original_t
        .zip(roundtrip_t)
        .map_or(0.0, |(original, result)| result[index] - original[index]);
    roundtrip_norm(dx, dy, dz, dt)
}

fn transform_web_mercator_coordinates(
    x: &mut [f64],
    y: &mut [f64],
    lonlat_to_web: bool,
) -> Result<()> {
    for (x, y) in x.iter_mut().zip(y.iter_mut()) {
        let (next_x, next_y) = if lonlat_to_web {
            lonlat_to_web_mercator_xy(*x, *y)?
        } else {
            web_mercator_to_lonlat_xy(*x, *y)
        };
        *x = next_x;
        *y = next_y;
    }
    Ok(())
}

pub(crate) fn operations_info(
    source: &str,
    target: &str,
    options: &TransformOptions,
) -> Result<Vec<OperationInfo>> {
    ensure_thread_caches_current();
    options.validate()?;
    if options.only_best == Some(true) || options.force_over {
        return operation_info(source, target, options).map(|operation| vec![operation]);
    }
    let (source, target) = normalize_pair(source, target)?;
    CRS_OPERATIONS_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        let index = lru_resolve(
            &mut cache,
            CRS_OPERATIONS_CACHE_CAPACITY,
            |item| item.source == source && item.target == target && item.options == *options,
            || {
                let items = operations_info_uncached(&source, &target, options)?;
                Ok(CachedCrsOperations {
                    source: source.clone(),
                    target: target.clone(),
                    options: options.clone(),
                    items,
                })
            },
        )?;
        Ok(cache[index].items.clone())
    })
}

pub(crate) fn operations_info_uncached(
    source: &str,
    target: &str,
    options: &TransformOptions,
) -> Result<Vec<OperationInfo>> {
    let source_crs = cstring(source)?;
    let target_crs = cstring(target)?;
    let context = ProjContext::new()
        .map_err(|error| CrsError::transform_create(source, target, error.to_string()))?;
    let source_object =
        create_crs_transform_object(&context, &source_crs, source, options.source_epoch)?;
    let target_object =
        create_crs_transform_object(&context, &target_crs, target, options.target_epoch)?;
    let factory = ProjOperationFactoryContext::new(&context, options)?;
    // SAFETY: DOC-H. Typed live context/objects/factory on creating thread;
    // returns uniquely owned object list or null.
    let list = unsafe {
        proj_sys::proj_create_operations(
            context.as_ptr(),
            source_object.as_ptr(),
            target_object.as_ptr(),
            factory.as_ptr(),
        )
    };
    // SAFETY: non-null returns are uniquely owned by the caller.
    let Some(list) = (unsafe { ProjObjList::try_from_owned(list) }) else {
        let message = proj_context_error_message(&context);
        return Err(CrsError::transform_create(source, target, message));
    };
    let operations = operation_infos_from_list(&context, &list, source, target, options);
    Ok(operations)
}

pub(super) fn operation_infos_from_list(
    context: &ProjContext,
    list: &ProjObjList,
    source: &str,
    target: &str,
    options: &TransformOptions,
) -> Vec<OperationInfo> {
    let count = list.count();
    let mut operations = Vec::with_capacity(count.max(0) as usize);
    for index in 0..count {
        let Some(operation) = list.get(context, index) else {
            continue;
        };
        // SAFETY: DOC-H. Typed context + owned operation; returns uniquely owned
        // normalized PJ or null.
        let normalized = unsafe {
            proj_sys::proj_normalize_for_visualization(context.as_ptr(), operation.as_ptr())
        };
        // SAFETY: non-null normalized is uniquely owned; bind immediately.
        let normalized = unsafe { OwnedPj::try_from_owned(normalized) };
        let operation_for_info = normalized.as_ref().unwrap_or(&operation);
        operations.push(operation_info_from_pj(
            context,
            operation_for_info,
            source.to_owned(),
            target.to_owned(),
            options.source_epoch,
            options.target_epoch,
        ));
    }
    operations
}

fn roundtrip_norm(dx: f64, dy: f64, dz: f64, dt: f64) -> f64 {
    let squared = dx * dx + dy * dy + dz * dz + dt * dt;
    if squared.is_finite() && (squared != 0.0 || (dx == 0.0 && dy == 0.0 && dz == 0.0 && dt == 0.0))
    {
        squared.sqrt()
    } else {
        dx.hypot(dy).hypot(dz).hypot(dt)
    }
}

fn roundtrip_norm_lanes(
    dx: ReduceSimd,
    dy: ReduceSimd,
    dz: ReduceSimd,
    dt: ReduceSimd,
) -> ReduceSimd {
    let squared = dx * dx + dy * dy + dz * dz + dt * dt;
    let zero_delta = dx.simd_eq(ReduceSimd::splat(0.0))
        & dy.simd_eq(ReduceSimd::splat(0.0))
        & dz.simd_eq(ReduceSimd::splat(0.0))
        & dt.simd_eq(ReduceSimd::splat(0.0));
    let bad = !squared.is_finite() | (squared.simd_eq(ReduceSimd::splat(0.0)) & !zero_delta);
    let result = squared.sqrt();
    if !bad.any() {
        return result;
    }
    let bits = bad.to_bitmask();
    let mut out = result.to_array();
    let xs = dx.to_array();
    let ys = dy.to_array();
    let zs = dz.to_array();
    let ts = dt.to_array();
    for lane in 0..REDUCE_LANES {
        if bits & (1 << lane) != 0 {
            out[lane] = roundtrip_norm(xs[lane], ys[lane], zs[lane], ts[lane]);
        }
    }
    ReduceSimd::from_array(out)
}
