//! The public `Transformer` handle (`impl Transformer`).
//!
//! Wraps a cached pipeline for repeated coordinate transforms; reaches the FFI
//! pipeline engine and helpers in the parent `crs` module via `use super::*`.

use super::*;
use crate::crs::transform::TransformedBounds3d;
use crate::error::Result;
use crate::geometry::Bounds;
use crate::py::support::Bounds3D;

impl Transformer {
    pub(crate) fn new(source: &str, target: &str) -> Self {
        Self::new_with_options(source, target, TransformOptions::default())
    }

    pub(crate) fn new_with_options(source: &str, target: &str, options: TransformOptions) -> Self {
        let operation = if coordinate_identity_crs(source, target) {
            TransformOperation::Identity
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
            TransformOperation::Identity | TransformOperation::InCore(_)
        )
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
        match &self.operation {
            TransformOperation::Identity => {
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
        }
    }

    pub(crate) fn transform_coordinates(
        &self,
        x: &mut [f64],
        y: &mut [f64],
        zt: crate::ZtLanes<'_>,
    ) -> Result<()> {
        match &zt {
            crate::Zt::None => validate_coordinate_lanes(x, y, crate::Zt::None)?,
            crate::Zt::Z(z) => validate_coordinate_lanes(x, y, crate::Zt::Z(&**z))?,
            crate::Zt::T(t) => validate_coordinate_lanes(x, y, crate::Zt::T(&**t))?,
            crate::Zt::Zt { z, t } => {
                validate_coordinate_lanes(x, y, crate::Zt::Zt { z: &**z, t: &**t })?;
            },
        }
        match &self.operation {
            TransformOperation::Identity => Ok(()),
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
        }
    }

    pub(crate) fn transform_bounds(
        &self,
        bounds: (f64, f64, f64, f64),
        densify: u32,
    ) -> Result<(f64, f64, f64, f64)> {
        match &self.operation {
            TransformOperation::Identity => Ok(bounds),
            TransformOperation::InCore(operation) => {
                let bounds = Bounds::new_unchecked(bounds.0, bounds.1, bounds.2, bounds.3);
                transform_in_core_bounds(operation, bounds, densify).map(Bounds::into_tuple)
            },
            TransformOperation::Proj {
                source,
                target,
                options,
            } => with_proj_pipeline(source, target, options, |transformer| {
                transformer.transform_bounds(bounds, densify)
            }),
        }
    }

    pub(crate) fn transform_bounds_many(
        &self,
        rows: &[(f64, f64, f64, f64)],
        densify: u32,
    ) -> Result<Vec<(f64, f64, f64, f64)>> {
        match &self.operation {
            TransformOperation::Identity => Ok(rows.to_vec()),
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
                rows.iter()
                    .map(|bounds| transformer.transform_bounds(*bounds, densify))
                    .collect()
            }),
        }
    }

    pub(crate) fn transform_bounds_3d(
        &self,
        bounds: (f64, f64, f64, f64, f64, f64),
        densify: u32,
    ) -> Result<(f64, f64, f64, f64, f64, f64)> {
        match &self.operation {
            TransformOperation::Identity => Ok(bounds),
            TransformOperation::InCore(operation) => {
                transform_in_core_bounds_3d(operation, Bounds3D::from(bounds), densify)
                    .map(Bounds3D::into_tuple)
            },
            TransformOperation::Proj {
                source,
                target,
                options,
            } => with_proj_pipeline(source, target, options, |transformer| {
                transformer.transform_bounds_3d(bounds, densify)
            }),
        }
    }

    pub(crate) fn transform_bounds_3d_many(
        &self,
        rows: &[(f64, f64, f64, f64, f64, f64)],
        densify: u32,
    ) -> Result<Vec<TransformedBounds3d>> {
        match &self.operation {
            TransformOperation::Identity => Ok(rows.to_vec()),
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
                rows.iter()
                    .map(|bounds| transformer.transform_bounds_3d(*bounds, densify))
                    .collect()
            }),
        }
    }
}

pub(crate) struct Transformer {
    operation: TransformOperation,
}

#[expect(clippy::large_enum_variant)]
pub(super) enum TransformOperation {
    Identity,
    InCore(InCoreTransform),
    Proj {
        source: String,
        target: String,
        options: TransformOptions,
    },
}
