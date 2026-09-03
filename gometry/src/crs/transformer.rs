//! The public `TransformPlan` handle (`impl TransformPlan`).
//!
//! Wraps a cached pipeline for repeated coordinate transforms; reaches the FFI
//! pipeline engine and helpers in the parent `crs` module via `use super::*`.

use std::sync::Arc;

use crate::crs::transform::{
    BoundsSamplePlan, TransformOutputPlan, TransformedBounds3d, canonical_identity_longitudes,
    transform_in_core_xy_with_domains, transform_in_core_xy_with_domains_indexed,
    transform_proj_shapes_indexed,
};
use crate::crs::{
    InCoreTransform, ProjDirection, ProjPipeline, RuntimeState, Shape, TransformExecution,
    TransformFailure, TransformOptions, begin_transform_observation, coordinate_identity_crs,
    ensure_geographic_domain_in_frame, record_transform_engine, runtime_state_snapshot,
    transform_in_core_xy_batch, transform_proj_shapes, try_in_core_transform_with_state,
    validate_coordinate_lanes, with_proj_pipeline_at, with_proj_pipeline_indexed_at,
};
use crate::error::{ErrorKind, Result};
use crate::geometry::{GeometryErrorKind, TopologyDomain};

impl TransformPlan {
    pub(crate) fn new(source: &str, target: &str) -> Self {
        Self::new_with_options(source, target, TransformOptions::default())
    }

    pub(crate) fn new_with_options(source: &str, target: &str, options: TransformOptions) -> Self {
        let state = runtime_state_snapshot().expect("CRS runtime state must remain readable");
        Self::new_with_options_at(source, target, options, state)
            .expect("CRS transform plan must be valid")
    }

    pub(crate) fn new_with_options_at(
        source: &str,
        target: &str,
        options: TransformOptions,
        state: Arc<RuntimeState>,
    ) -> Result<Self> {
        options.validate()?;
        let plan_options = options.clone();
        let source_domain = crate::crs::info_with_state(source, &state)?
            .geographic_frame()
            .map_or(TopologyDomain::Cartesian, TopologyDomain::Geographic);
        let target_domain = crate::crs::info_with_state(target, &state)?
            .geographic_frame()
            .map_or(TopologyDomain::Cartesian, TopologyDomain::Geographic);
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
            TransformOperation::Identity { source_domain }
        } else if let Some(transform) =
            try_in_core_transform_with_state(source, target, &options, &state)
        {
            TransformOperation::InCore(transform)
        } else {
            TransformOperation::Proj {
                source: source.to_owned(),
                target: target.to_owned(),
                options,
            }
        };
        begin_transform_observation();
        Ok(Self {
            operation,
            state,
            execution: TransformExecution::default(),
            options: plan_options,
            source_domain,
            target_domain,
        })
    }

    pub(crate) fn execution(&self) -> TransformExecution {
        self.execution.clone()
    }

    pub(crate) const fn is_identity(&self) -> bool {
        matches!(self.operation, TransformOperation::Identity { .. })
    }

    pub(crate) const fn is_in_core(&self) -> bool {
        matches!(
            self.operation,
            TransformOperation::Identity { .. } | TransformOperation::InCore(_)
        )
    }

    fn begin_execution(&self) {
        begin_transform_observation();
    }

    fn finish_execution<T>(&self, result: Result<T>, executed: bool) -> Result<T> {
        if executed && result.is_ok() && self.is_in_core() {
            record_transform_engine(true);
        }
        result
    }

    fn finish_execution_indexed<T>(
        &self,
        result: std::result::Result<T, TransformFailure>,
        executed: bool,
    ) -> std::result::Result<T, TransformFailure> {
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
            TransformOperation::Identity { source_domain } => {
                if let TopologyDomain::Geographic(frame) = source_domain {
                    for shape in shapes {
                        ensure_geographic_domain_in_frame(*frame, shape)?;
                    }
                }
                Ok(shapes.iter().map(|shape| (**shape).clone()).collect())
            },
            TransformOperation::InCore(operation) => {
                let mut output = TransformOutputPlan::try_new(shapes)?;
                output.execute(|x, y, _| {
                    transform_in_core_xy_with_domains(
                        operation,
                        self.source_domain,
                        self.target_domain,
                        x,
                        y,
                    )
                })?;
                output.finish(shapes)
            },
            TransformOperation::Proj {
                source,
                target,
                options,
            } => with_proj_pipeline_at(
                source,
                target,
                options,
                &self.state,
                &self.execution,
                |transformer| {
                    transform_proj_shapes(
                        transformer,
                        shapes,
                        options.source_epoch,
                        &self.execution,
                    )
                },
            ),
        };
        self.finish_execution(result, !shapes.is_empty())
    }

    pub(crate) fn transform_shapes_indexed(
        &self,
        shapes: &[&Shape],
    ) -> std::result::Result<Vec<Shape>, TransformFailure> {
        self.begin_execution();
        let result: std::result::Result<Vec<Shape>, TransformFailure> = match &self.operation {
            TransformOperation::Identity { source_domain } => {
                if let TopologyDomain::Geographic(frame) = source_domain {
                    for (index, shape) in shapes.iter().enumerate() {
                        ensure_geographic_domain_in_frame(*frame, shape)
                            .map_err(|error| TransformFailure::at(error, index))?;
                    }
                }
                Ok(shapes.iter().map(|shape| (**shape).clone()).collect())
            },
            TransformOperation::InCore(operation) => {
                let mut output = TransformOutputPlan::try_new(shapes)?;
                output.execute_indexed(|x, y, _| {
                    transform_in_core_xy_with_domains_indexed(
                        operation,
                        self.source_domain,
                        self.target_domain,
                        x,
                        y,
                    )
                })?;
                Ok(output.finish(shapes)?)
            },
            TransformOperation::Proj {
                source,
                target,
                options,
            } => with_proj_pipeline_indexed_at(
                source,
                target,
                options,
                &self.state,
                &self.execution,
                |transformer| {
                    transform_proj_shapes_indexed(
                        transformer,
                        shapes,
                        options.source_epoch,
                        &self.execution,
                    )
                },
            ),
        };
        self.finish_execution_indexed(result, !shapes.is_empty())
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
            TransformOperation::Identity { source_domain } => {
                if let TopologyDomain::Geographic(frame) = source_domain {
                    for (&lon, &lat) in x.iter().zip(y.iter()) {
                        crate::crs::ensure_geographic_lonlat_in_frame(*frame, lon, lat)?;
                    }
                }
                Ok(())
            },
            TransformOperation::InCore(operation) => {
                // Lane-direct: the planar formulas never touch Z/T, and the
                // slices were already boundary-validated finite — transform
                // XY in place, leave the other lanes untouched. The operation
                // variant is resolved once per batch via `transform_in_core_xy_batch`.
                transform_in_core_xy_with_domains(
                    operation,
                    self.source_domain,
                    self.target_domain,
                    x,
                    y,
                )
            },
            TransformOperation::Proj {
                source,
                target,
                options,
            } => with_proj_pipeline_at(
                source,
                target,
                options,
                &self.state,
                &self.execution,
                |transformer| match zt {
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
                                &self.execution,
                            )
                        } else {
                            transformer.require_epoch_lane()?;
                            transformer.transform_lanes(
                                x,
                                y,
                                None,
                                None,
                                ProjDirection::Forward,
                                &self.execution,
                            )
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
                                &self.execution,
                            )
                        } else {
                            transformer.require_epoch_lane()?;
                            transformer.transform_lanes(
                                x,
                                y,
                                Some(z),
                                None,
                                ProjDirection::Forward,
                                &self.execution,
                            )
                        }
                    },
                    crate::Zt::T(t) => transformer.transform_lanes(
                        x,
                        y,
                        None,
                        Some(t),
                        ProjDirection::Forward,
                        &self.execution,
                    ),
                    crate::Zt::Zt { z, t } => transformer.transform_lanes(
                        x,
                        y,
                        Some(z),
                        Some(t),
                        ProjDirection::Forward,
                        &self.execution,
                    ),
                },
            ),
        };
        self.finish_execution(result, !x.is_empty())
    }

    #[expect(
        clippy::too_many_lines,
        reason = "indexed execution mirrors the owned lane and domain dispatch in one boundary"
    )]
    pub(crate) fn transform_coordinates_indexed(
        &self,
        x: &mut [f64],
        y: &mut [f64],
        zt: crate::ZtLanes<'_>,
    ) -> std::result::Result<(), TransformFailure> {
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
            TransformOperation::Identity { source_domain } => {
                if let TopologyDomain::Geographic(frame) = source_domain {
                    for (index, (&lon, &lat)) in x.iter().zip(y.iter()).enumerate() {
                        crate::crs::ensure_geographic_lonlat_in_frame(*frame, lon, lat)
                            .map_err(|error| TransformFailure::at(error, index))?;
                    }
                }
                Ok(())
            },
            TransformOperation::InCore(operation) => {
                // Lane-direct: the planar formulas never touch Z/T, and the
                // slices were already boundary-validated finite — transform
                // XY in place, leave the other lanes untouched. The operation
                // variant is resolved once per batch via `transform_in_core_xy_batch`.
                transform_in_core_xy_with_domains_indexed(
                    operation,
                    self.source_domain,
                    self.target_domain,
                    x,
                    y,
                )
            },
            TransformOperation::Proj {
                source,
                target,
                options,
            } => with_proj_pipeline_indexed_at(
                source,
                target,
                options,
                &self.state,
                &self.execution,
                |transformer| match zt {
                    crate::Zt::None => {
                        if let Some(epoch) = options.source_epoch {
                            // PROJ broadcasts a length-1 time lane as a constant
                            // across every coordinate — no per-vertex epoch buffer.
                            let mut t = [epoch];
                            transformer.transform_lanes_indexed(
                                x,
                                y,
                                None,
                                Some(t.as_mut_slice()),
                                ProjDirection::Forward,
                                &self.execution,
                            )
                        } else {
                            transformer
                                .require_epoch_lane()
                                .map_err(TransformFailure::from)?;
                            transformer.transform_lanes_indexed(
                                x,
                                y,
                                None,
                                None,
                                ProjDirection::Forward,
                                &self.execution,
                            )
                        }
                    },
                    crate::Zt::Z(z) => {
                        if let Some(epoch) = options.source_epoch {
                            let mut t = [epoch];
                            transformer.transform_lanes_indexed(
                                x,
                                y,
                                Some(z),
                                Some(t.as_mut_slice()),
                                ProjDirection::Forward,
                                &self.execution,
                            )
                        } else {
                            transformer
                                .require_epoch_lane()
                                .map_err(TransformFailure::from)?;
                            transformer.transform_lanes_indexed(
                                x,
                                y,
                                Some(z),
                                None,
                                ProjDirection::Forward,
                                &self.execution,
                            )
                        }
                    },
                    crate::Zt::T(t) => transformer.transform_lanes_indexed(
                        x,
                        y,
                        None,
                        Some(t),
                        ProjDirection::Forward,
                        &self.execution,
                    ),
                    crate::Zt::Zt { z, t } => transformer.transform_lanes_indexed(
                        x,
                        y,
                        Some(z),
                        Some(t),
                        ProjDirection::Forward,
                        &self.execution,
                    ),
                },
            ),
        };
        self.finish_execution_indexed(result, !x.is_empty())
    }

    pub(crate) fn transform_bounds(
        &self,
        bounds: (f64, f64, f64, f64),
        densify: u32,
    ) -> Result<(f64, f64, f64, f64)> {
        self.begin_execution();
        let result = if self.is_identity() {
            if let TopologyDomain::Geographic(frame) = self.target_domain {
                let (west, east) = canonical_identity_longitudes(bounds.0, bounds.2, frame)?;
                let [south, north] =
                    crate::crs::transform::canonical_identity_latitudes(bounds.1, bounds.3, frame)?;
                Ok((west, south, east, north))
            } else {
                Ok(bounds)
            }
        } else {
            let mut samples = BoundsSamplePlan::new_2d(
                bounds,
                densify,
                self.source_domain,
                self.target_domain,
                self.options.source_epoch,
            )?;
            self.execute_bounds_samples(&mut samples)?;
            samples.finish_2d()
        };
        self.finish_execution(result, true)
    }

    pub(crate) fn transform_bounds_many(
        &self,
        rows: &[(f64, f64, f64, f64)],
        densify: u32,
    ) -> Result<Vec<(f64, f64, f64, f64)>> {
        self.begin_execution();
        let result = self.execute_bounds_many(rows, |bounds, executor| {
            let densify = if matches!(executor, BoundsExecutor::Identity) {
                0
            } else {
                densify
            };
            let mut samples = BoundsSamplePlan::new_2d(
                bounds,
                densify,
                self.source_domain,
                self.target_domain,
                self.options.source_epoch,
            )?;
            self.execute_bounds_with_executor(&mut samples, executor)?;
            samples.finish_2d()
        });
        self.finish_execution(result, !rows.is_empty())
    }

    pub(crate) fn transform_bounds_3d(
        &self,
        bounds: (f64, f64, f64, f64, f64, f64),
        densify: u32,
    ) -> Result<(f64, f64, f64, f64, f64, f64)> {
        self.begin_execution();
        let result = if self.is_identity() {
            if let TopologyDomain::Geographic(frame) = self.target_domain {
                let (west, east) = canonical_identity_longitudes(bounds.0, bounds.3, frame)?;
                let [south, north] =
                    crate::crs::transform::canonical_identity_latitudes(bounds.1, bounds.4, frame)?;
                Ok((west, south, bounds.2, east, north, bounds.5))
            } else {
                Ok(bounds)
            }
        } else {
            let mut samples = BoundsSamplePlan::new_3d(
                bounds,
                densify,
                self.source_domain,
                self.target_domain,
                self.options.source_epoch,
            )?;
            self.execute_bounds_samples(&mut samples)?;
            samples.finish_3d()
        };
        self.finish_execution(result, true)
    }

    pub(crate) fn transform_bounds_3d_many(
        &self,
        rows: &[(f64, f64, f64, f64, f64, f64)],
        densify: u32,
    ) -> Result<Vec<TransformedBounds3d>> {
        self.begin_execution();
        let result = self.execute_bounds_many(rows, |bounds, executor| {
            let densify = if matches!(executor, BoundsExecutor::Identity) {
                0
            } else {
                densify
            };
            let mut samples = BoundsSamplePlan::new_3d(
                bounds,
                densify,
                self.source_domain,
                self.target_domain,
                self.options.source_epoch,
            )?;
            self.execute_bounds_with_executor(&mut samples, executor)?;
            samples.finish_3d()
        });
        self.finish_execution(result, !rows.is_empty())
    }

    fn execute_bounds_many<B: Copy, O: From<B>>(
        &self,
        rows: &[B],
        mut transform: impl FnMut(B, BoundsExecutor<'_>) -> Result<O>,
    ) -> Result<Vec<O>> {
        let mut output = Vec::new();
        crate::try_reserve_vec(&mut output, rows.len())?;
        if rows.is_empty() {
            return Ok(output);
        }
        match &self.operation {
            TransformOperation::Identity { .. } => {
                if self.source_domain.is_geographic() {
                    for &bounds in rows {
                        output.push(transform(bounds, BoundsExecutor::Identity)?);
                    }
                } else {
                    output.extend(rows.iter().copied().map(Into::into));
                }
                Ok(output)
            },
            TransformOperation::InCore(operation) => {
                for &bounds in rows {
                    output.push(transform(bounds, BoundsExecutor::InCore(operation))?);
                }
                Ok(output)
            },
            TransformOperation::Proj {
                source,
                target,
                options,
            } => with_proj_pipeline_at(
                source,
                target,
                options,
                &self.state,
                &self.execution,
                |pipeline| {
                    if pipeline.requires_coordinate_epoch() && self.options.source_epoch.is_none() {
                        return Err(crate::crs::CrsError::message(
                            "transform requires a coordinate epoch; pass epoch= or source_epoch=",
                        ));
                    }
                    for &bounds in rows {
                        output.push(transform(bounds, BoundsExecutor::Proj(pipeline))?);
                    }
                    Ok(output)
                },
            ),
        }
    }

    fn execute_bounds_samples(&self, samples: &mut BoundsSamplePlan) -> Result<()> {
        match &self.operation {
            TransformOperation::Identity { .. } => Ok(()),
            TransformOperation::InCore(operation) => {
                self.execute_bounds_with_executor(samples, BoundsExecutor::InCore(operation))
            },
            TransformOperation::Proj {
                source,
                target,
                options,
            } => with_proj_pipeline_at(
                source,
                target,
                options,
                &self.state,
                &self.execution,
                |transformer| {
                    if transformer.requires_coordinate_epoch() && samples.epoch.is_none() {
                        return Err(crate::crs::CrsError::message(
                            "transform requires a coordinate epoch; pass epoch= or source_epoch=",
                        ));
                    }
                    self.execute_bounds_with_executor(samples, BoundsExecutor::Proj(transformer))
                },
            ),
        }
    }

    fn execute_bounds_with_executor(
        &self,
        samples: &mut BoundsSamplePlan,
        executor: BoundsExecutor<'_>,
    ) -> Result<()> {
        match executor {
            BoundsExecutor::Identity => Ok(()),
            BoundsExecutor::InCore(operation) => {
                executor.execute(samples, &self.execution)?;
                let reverse = operation.reversed();
                samples.include_target_poles(|x, y| {
                    let mut x = [x];
                    let mut y = [y];
                    let result = transform_in_core_xy_with_domains(
                        &reverse,
                        self.target_domain,
                        self.source_domain,
                        &mut x,
                        &mut y,
                    );
                    match result {
                        Ok(()) => Ok(Some((x[0], y[0]))),
                        Err(error) if in_core_pole_domain_miss(&error) => Ok(None),
                        Err(error) => Err(error),
                    }
                })
            },
            BoundsExecutor::Proj(transformer) => {
                executor.execute(samples, &self.execution)?;
                samples.include_target_poles(|x, y| {
                    let epoch = self.options.target_epoch.or(self.options.source_epoch);
                    transformer.transform_xy_probe(
                        x,
                        y,
                        epoch,
                        ProjDirection::Inverse,
                        &self.execution,
                    )
                })
            },
        }
    }
}

fn in_core_pole_domain_miss(error: &crate::error::Error) -> bool {
    matches!(
        error.kind(),
        ErrorKind::Geometry(
            GeometryErrorKind::WebMercatorLatitude(_) | GeometryErrorKind::ProjectionDomain(_),
        )
    )
}

#[derive(Clone, Copy)]
enum BoundsExecutor<'a> {
    Identity,
    InCore(&'a InCoreTransform),
    Proj(&'a ProjPipeline),
}

impl BoundsExecutor<'_> {
    fn execute(self, samples: &mut BoundsSamplePlan, execution: &TransformExecution) -> Result<()> {
        match self {
            Self::Identity => Ok(()),
            Self::InCore(operation) => {
                samples.enter_in_core()?;
                transform_in_core_xy_batch(operation, &mut samples.x, &mut samples.y)?;
                samples.leave_in_core()?;
                Ok(())
            },
            Self::Proj(pipeline) => pipeline.transform_lanes(
                &mut samples.x,
                &mut samples.y,
                samples.z.as_deref_mut(),
                samples.epoch.as_mut().map(<[f64; 1]>::as_mut_slice),
                ProjDirection::Forward,
                execution,
            ),
        }
    }
}

pub(crate) struct TransformPlan {
    operation: TransformOperation,
    state: Arc<RuntimeState>,
    execution: TransformExecution,
    options: TransformOptions,
    source_domain: TopologyDomain,
    target_domain: TopologyDomain,
}

#[expect(
    clippy::large_enum_variant,
    reason = "the operation is an owned, immutable transform plan; boxing its hot in-core variant would add an allocation and indirection"
)]
pub(super) enum TransformOperation {
    /// Same source/target CRS. The source domain gates validation so
    /// identity never silently accepts out-of-domain geographic coordinates.
    Identity {
        source_domain: TopologyDomain,
    },
    InCore(InCoreTransform),
    Proj {
        source: String,
        target: String,
        options: TransformOptions,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_bulk_bounds_do_not_acquire_proj_pipeline() {
        crate::crs::clear_cache();
        let plan = TransformPlan::new_with_options("EPSG:4326", "EPSG:3857", TransformOptions {
            force_over: true,
            ..TransformOptions::default()
        });

        assert!(plan.transform_bounds_many(&[], 2).unwrap().is_empty());
        let info = crate::crs::cache_info();
        assert_eq!(info.last_transform_engine, None);
        assert_eq!(
            info.buckets
                .iter()
                .find(|bucket| bucket.name == "proj_pipeline")
                .unwrap()
                .entries,
            0
        );
    }

    #[test]
    fn pole_domain_classifier_uses_typed_projection_domain() {
        let message = "coordinate is outside the projection domain";
        let domain = GeometryErrorKind::projection_domain(message);
        let generic = GeometryErrorKind::projection(message);

        assert_eq!(domain.to_string(), format!("projection failed: {message}"));
        assert!(in_core_pole_domain_miss(&domain));
        assert!(!in_core_pole_domain_miss(&generic));
    }
}
