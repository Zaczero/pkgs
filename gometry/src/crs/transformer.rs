//! The public `Transformer` handle (`impl Transformer`).
//!
//! Wraps a cached pipeline for repeated coordinate transforms; reaches the FFI
//! pipeline engine and helpers in the parent `crs` module via `use super::*`.

use crate::crs::transform::TransformedBounds3d;
use crate::crs::{
    InCoreTransform, ProjDirection, Shape, TransformOptions, begin_transform_observation,
    coordinate_identity_crs, ensure_geographic_domain, ensure_geographic_lonlat, in_core_xy_op,
    is_geographic_crs, record_transform_engine, take_accuracy_diagnostic, transform_in_core_bounds,
    transform_in_core_bounds_3d, transform_in_core_xy_batch, transform_proj_shapes,
    try_in_core_transform, validate_coordinate_lanes, with_proj_pipeline,
};
use crate::error::Result;
use crate::geometry::{Bounds, Bounds3D};

impl Transformer {
    pub(crate) fn new(source: &str, target: &str) -> Self {
        Self::new_with_options(source, target, TransformOptions::default())
    }

    pub(crate) fn new_with_options(source: &str, target: &str, options: TransformOptions) -> Self {
        // A motion between unequal coordinate epochs is a real PROJ
        // operation, even when the CRS labels are otherwise identical.  Do
        // not let the same-CRS identity shortcut erase that time-dependent
        // transformation.
        let operation = if coordinate_identity_crs(source, target)
            && options.source_epoch == options.target_epoch
        {
            // Same-CRS identity still validates geographic domain (same class
            // as geodesic equality shortcuts) — never a silent pass for
            // out-of-domain latitudes.
            let source_geographic = is_geographic_crs(source).unwrap_or(false);
            TransformOperation::Identity { source_geographic }
        } else if let Some(transform) = try_in_core_transform(source, target, &options) {
            TransformOperation::InCore(transform)
        } else {
            TransformOperation::Proj {
                source: source.to_owned(),
                target: target.to_owned(),
                options,
            }
        };
        Self { operation }
    }

    pub(crate) const fn is_in_core(&self) -> bool {
        matches!(
            self.operation,
            TransformOperation::Identity { .. } | TransformOperation::InCore(_)
        )
    }

    fn begin_execution(&self) {
        begin_transform_observation();
        // Internal consumers do not have a Python warning boundary. Discard
        // anything they left behind before this execution; the PROJ FFI seam
        // starts diagnostics when this transform actually reaches it.
        let _ = take_accuracy_diagnostic();
    }

    fn finish_execution<T>(&self, result: Result<T>, executed: bool) -> Result<T> {
        if executed && result.is_ok() && self.is_in_core() {
            record_transform_engine(true);
        }
        result
    }

    pub(crate) fn transform_shape(&self, shape: &Shape) -> Result<Shape> {
        Ok(self
            .transform_shapes(&[shape])?
            .into_iter()
            .next()
            .expect("single-shape transform returns one shape"))
    }

    /// Transform a batch of geometries. Takes `&[&Shape]` (a cheap pointer
    /// slice) so callers — notably array `to_crs` over `Arc<ShapeData>`
    /// elements — need not deep-clone their inputs into an owned
    /// `Vec<Shape>`.
    pub(crate) fn transform_shapes(&self, shapes: &[&Shape]) -> Result<Vec<Shape>> {
        self.begin_execution();
        let result = match &self.operation {
            TransformOperation::Identity { source_geographic } => {
                if *source_geographic {
                    for shape in shapes {
                        ensure_geographic_domain(shape)?;
                    }
                }
                Ok(shapes.iter().map(|shape| (**shape).clone()).collect())
            },
            TransformOperation::InCore(operation) => {
                let op = in_core_xy_op(operation);
                shapes
                    .iter()
                    .map(|shape| {
                        // Columnar in-core projection: no per-vertex Point gather
                        // or axes re-derivation; Z/M columns carry through exactly
                        // like the per-point `with_xy` form did. The operation
                        // variant is resolved once per batch via `in_core_xy_op`.
                        shape.map_xy_with(op)
                    })
                    .collect::<Result<_, _>>()
            },
            TransformOperation::Proj {
                source,
                target,
                options,
            } => with_proj_pipeline(source, target, options, |transformer| {
                transform_proj_shapes(transformer, shapes, options.source_epoch)
            }),
        };
        self.finish_execution(result, !shapes.is_empty())
    }

    pub(crate) fn transform_coordinates(
        &self,
        x: &mut [f64],
        y: &mut [f64],
        zt: crate::ZtLanes<'_>,
    ) -> Result<()> {
        self.begin_execution();
        match &zt {
            crate::Zt::None => validate_coordinate_lanes(x, y, crate::Zt::None)?,
            crate::Zt::Z(z) => validate_coordinate_lanes(x, y, crate::Zt::Z(&**z))?,
            crate::Zt::T(t) => validate_coordinate_lanes(x, y, crate::Zt::T(&**t))?,
            crate::Zt::Zt { z, t } => {
                validate_coordinate_lanes(x, y, crate::Zt::Zt { z: &**z, t: &**t })?;
            },
        }
        let result = match &self.operation {
            TransformOperation::Identity { source_geographic } => {
                if *source_geographic {
                    for (&lon, &lat) in x.iter().zip(y.iter()) {
                        ensure_geographic_lonlat(lon, lat)?;
                    }
                }
                Ok(())
            },
            TransformOperation::InCore(operation) => {
                // Lane-direct: the planar formulas never touch Z/T, and the
                // slices were already boundary-validated finite — transform
                // XY in place, leave the other lanes untouched. The operation
                // variant is resolved once per batch via `transform_in_core_xy_batch`.
                transform_in_core_xy_batch(operation, x, y)
            },
            TransformOperation::Proj {
                source,
                target,
                options,
            } => with_proj_pipeline(source, target, options, |transformer| match zt {
                crate::Zt::None => {
                    if let Some(epoch) = options.source_epoch {
                        // PROJ broadcasts a length-1 time lane as a constant
                        // across every coordinate — no per-vertex epoch buffer.
                        let mut t = [epoch];
                        transformer.transform_lanes(
                            x,
                            y,
                            None,
                            Some(t.as_mut_slice()),
                            ProjDirection::Forward,
                        )
                    } else {
                        transformer.require_epoch_lane()?;
                        transformer.transform_lanes(x, y, None, None, ProjDirection::Forward)
                    }
                },
                crate::Zt::Z(z) => {
                    if let Some(epoch) = options.source_epoch {
                        let mut t = [epoch];
                        transformer.transform_lanes(
                            x,
                            y,
                            Some(z),
                            Some(t.as_mut_slice()),
                            ProjDirection::Forward,
                        )
                    } else {
                        transformer.require_epoch_lane()?;
                        transformer.transform_lanes(x, y, Some(z), None, ProjDirection::Forward)
                    }
                },
                crate::Zt::T(t) => {
                    transformer.transform_lanes(x, y, None, Some(t), ProjDirection::Forward)
                },
                crate::Zt::Zt { z, t } => {
                    transformer.transform_lanes(x, y, Some(z), Some(t), ProjDirection::Forward)
                },
            }),
        };
        self.finish_execution(result, !x.is_empty())
    }

    pub(crate) fn transform_bounds(
        &self,
        bounds: (f64, f64, f64, f64),
        densify: u32,
    ) -> Result<(f64, f64, f64, f64)> {
        self.begin_execution();
        let result = match &self.operation {
            TransformOperation::Identity { source_geographic } => {
                if *source_geographic {
                    ensure_geographic_lonlat(bounds.0, bounds.1)?;
                    ensure_geographic_lonlat(bounds.2, bounds.3)?;
                }
                Ok(bounds)
            },
            TransformOperation::InCore(operation) => {
                let bounds = Bounds::new_unchecked(bounds.0, bounds.1, bounds.2, bounds.3);
                transform_in_core_bounds(operation, bounds, densify).map(Bounds::into_tuple)
            },
            TransformOperation::Proj {
                source,
                target,
                options,
            } => with_proj_pipeline(source, target, options, |transformer| {
                if transformer.requires_coordinate_epoch() {
                    return Err(crate::crs::CrsError::message(
                        "bounds transform requires a coordinate epoch; time-aware bounds are not implemented",
                    ));
                }
                transformer.transform_bounds(bounds, densify)
            }),
        };
        self.finish_execution(result, true)
    }

    pub(crate) fn transform_bounds_many(
        &self,
        rows: &[(f64, f64, f64, f64)],
        densify: u32,
    ) -> Result<Vec<(f64, f64, f64, f64)>> {
        self.begin_execution();
        let result = match &self.operation {
            TransformOperation::Identity { source_geographic } => {
                if *source_geographic {
                    for bounds in rows {
                        ensure_geographic_lonlat(bounds.0, bounds.1)?;
                        ensure_geographic_lonlat(bounds.2, bounds.3)?;
                    }
                }
                Ok(rows.to_vec())
            },
            TransformOperation::InCore(operation) => rows
                .iter()
                .map(|bounds| {
                    transform_in_core_bounds(
                        operation,
                        Bounds::new_unchecked(bounds.0, bounds.1, bounds.2, bounds.3),
                        densify,
                    )
                    .map(Bounds::into_tuple)
                })
                .collect(),
            TransformOperation::Proj {
                source,
                target,
                options,
            } => with_proj_pipeline(source, target, options, |transformer| {
                if transformer.requires_coordinate_epoch() {
                    return Err(crate::crs::CrsError::message(
                        "bounds transform requires a coordinate epoch; time-aware bounds are not implemented",
                    ));
                }
                rows.iter()
                    .map(|bounds| transformer.transform_bounds(*bounds, densify))
                    .collect()
            }),
        };
        self.finish_execution(result, !rows.is_empty())
    }

    pub(crate) fn transform_bounds_3d(
        &self,
        bounds: (f64, f64, f64, f64, f64, f64),
        densify: u32,
    ) -> Result<(f64, f64, f64, f64, f64, f64)> {
        self.begin_execution();
        let result = match &self.operation {
            TransformOperation::Identity { source_geographic } => {
                if *source_geographic {
                    ensure_geographic_lonlat(bounds.0, bounds.1)?;
                    ensure_geographic_lonlat(bounds.3, bounds.4)?;
                }
                Ok(bounds)
            },
            TransformOperation::InCore(operation) => {
                transform_in_core_bounds_3d(operation, Bounds3D::from(bounds), densify)
                    .map(Bounds3D::into_tuple)
            },
            TransformOperation::Proj {
                source,
                target,
                options,
            } => with_proj_pipeline(source, target, options, |transformer| {
                if transformer.requires_coordinate_epoch() {
                    return Err(crate::crs::CrsError::message(
                        "bounds transform requires a coordinate epoch; time-aware bounds are not implemented",
                    ));
                }
                transformer.transform_bounds_3d(bounds, densify)
            }),
        };
        self.finish_execution(result, true)
    }

    pub(crate) fn transform_bounds_3d_many(
        &self,
        rows: &[(f64, f64, f64, f64, f64, f64)],
        densify: u32,
    ) -> Result<Vec<TransformedBounds3d>> {
        self.begin_execution();
        let result = match &self.operation {
            TransformOperation::Identity { source_geographic } => {
                if *source_geographic {
                    for bounds in rows {
                        ensure_geographic_lonlat(bounds.0, bounds.1)?;
                        ensure_geographic_lonlat(bounds.3, bounds.4)?;
                    }
                }
                Ok(rows.to_vec())
            },
            TransformOperation::InCore(operation) => rows
                .iter()
                .map(|bounds| {
                    transform_in_core_bounds_3d(operation, Bounds3D::from(*bounds), densify)
                        .map(Bounds3D::into_tuple)
                })
                .collect(),
            TransformOperation::Proj {
                source,
                target,
                options,
            } => with_proj_pipeline(source, target, options, |transformer| {
                if transformer.requires_coordinate_epoch() {
                    return Err(crate::crs::CrsError::message(
                        "bounds transform requires a coordinate epoch; time-aware bounds are not implemented",
                    ));
                }
                rows.iter()
                    .map(|bounds| transformer.transform_bounds_3d(*bounds, densify))
                    .collect()
            }),
        };
        self.finish_execution(result, !rows.is_empty())
    }
}

pub(crate) struct Transformer {
    operation: TransformOperation,
}

#[expect(
    clippy::large_enum_variant,
    reason = "the operation is an owned, immutable transform plan; boxing its hot in-core variant would add an allocation and indirection"
)]
pub(super) enum TransformOperation {
    /// Same source/target CRS. `source_geographic` gates domain validation so
    /// identity never silently accepts out-of-domain geographic coordinates.
    Identity {
        source_geographic: bool,
    },
    InCore(InCoreTransform),
    Proj {
        source: String,
        target: String,
        options: TransformOptions,
    },
}
