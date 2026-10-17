//! The public `TransformPlan` handle (`impl TransformPlan`).
//!
//! Wraps a cached pipeline for repeated coordinate transforms; reaches the FFI
//! pipeline engine and helpers in the parent `crs` module via `use super::*`.

use std::sync::Arc;

use smol_str::SmolStr;

#[cfg(test)]
use crate::crs::frame::crs_arc_static;
use crate::crs::frame::{Frame, GeometryTransformFrame};
use crate::crs::transform::{
    BoundsInterpretation, BoundsSamplePlan, TransformOutputPlan, TransformedBounds3d,
    identity_bounds_2d, identity_bounds_3d, transform_in_core_xy_with_domains,
    transform_in_core_xy_with_domains_indexed, transform_in_core_xy_with_domains_indexed_for_shape,
    transform_proj_shapes_indexed,
};
use crate::crs::{
    InCoreTransform, ProjDirection, ProjPipeline, RuntimeState, Shape, TransformExecution,
    TransformFailure, TransformOptions, begin_transform_observation, coordinate_identity_crs,
    ensure_geographic_domain_in_frame, ensure_geographic_lonlat_in_frame,
    ensure_geographic_row_in_frame, record_transform_engine, runtime_state_snapshot,
    transform_in_core_xy_batch, try_in_core_transform_with_state, validate_coordinate_lanes,
    with_proj_pipeline_at, with_proj_pipeline_indexed_at,
};
use crate::error::{ErrorKind, Result};
use crate::geometry::storage::ShapeRow;
use crate::geometry::{GeometryErrorKind, TopologyDomain};

impl TransformPlan {
    pub(crate) fn new(source: &str, target: &str) -> Result<Self> {
        Self::new_with_options(source, target, TransformOptions::default())
    }

    pub(crate) fn new_with_options(
        source: &str,
        target: &str,
        options: TransformOptions,
    ) -> Result<Self> {
        Self::new_with_options_at(source, target, options, runtime_state_snapshot()?)
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
                source: source.into(),
                target: target.into(),
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
            geometry_frame: None,
        })
    }

    pub(crate) fn from_geometry_frame(frame: GeometryTransformFrame) -> Result<Self> {
        let mut plan = Self::new_with_options_at(
            frame.source.as_str(),
            frame.target.as_str(),
            frame.options.clone(),
            Arc::clone(&frame.state),
        )?;
        plan.geometry_frame = Some(frame);
        Ok(plan)
    }

    pub(crate) const fn output_frame(&self) -> &Frame {
        &self
            .geometry_frame
            .as_ref()
            .expect("geometry transform plans retain their admitted output frame")
            .output
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

    pub(crate) const fn source_geographic_frame(
        &self,
    ) -> Option<crate::geographic::GeographicFrame> {
        match self.source_domain {
            TopologyDomain::Geographic(frame) => Some(frame),
            TopologyDomain::Cartesian => None,
        }
    }

    const fn default_proj_source_frame(&self) -> Option<crate::geographic::GeographicFrame> {
        if self.options.force_over {
            None
        } else {
            self.source_geographic_frame()
        }
    }

    pub(crate) const fn computed_geographic_frame(
        &self,
    ) -> Option<crate::geographic::GeographicFrame> {
        if self.is_identity() || self.options.force_over {
            return None;
        }
        match self.target_domain {
            crate::geometry::TopologyDomain::Geographic(frame) => Some(frame),
            crate::geometry::TopologyDomain::Cartesian => None,
        }
    }

    /// Lazily resolve the output CRS ellipsoid against this plan's runtime snapshot.
    pub(crate) const fn target_metric_scratch(&self) -> TransformMetricScratch<'_> {
        TransformMetricScratch {
            frame: self
                .geometry_frame
                .as_ref()
                .expect("computed geometry publication retains its admitted target frame"),
            metric: None,
        }
    }

    const fn identity_geographic_frame(&self) -> Option<crate::geographic::GeographicFrame> {
        match &self.operation {
            TransformOperation::Identity {
                source_domain: TopologyDomain::Geographic(frame),
            } => Some(*frame),
            TransformOperation::Identity {
                source_domain: TopologyDomain::Cartesian,
            }
            | TransformOperation::InCore(_)
            | TransformOperation::Proj { .. } => None,
        }
    }

    /// Admit a source geometry for an identity `to_crs` result without
    /// materializing or executing another transform.
    pub(crate) fn admit_identity_shape(&self, shape: &Shape) -> Result<()> {
        self.admit_identity_shape_at(0, shape)
            .map_err(|failure| failure.error)
    }

    pub(crate) fn admit_identity_shape_at(
        &self,
        row: usize,
        shape: &Shape,
    ) -> std::result::Result<(), TransformFailure> {
        if let Some(frame) = self.identity_geographic_frame() {
            ensure_geographic_domain_in_frame(frame, shape)
                .map_err(|error| TransformFailure::at(error, row))?;
        }
        Ok(())
    }

    pub(crate) fn admit_identity_row_at(
        &self,
        row: usize,
        shape: ShapeRow<'_>,
    ) -> std::result::Result<(), TransformFailure> {
        if let Some(frame) = self.identity_geographic_frame() {
            ensure_geographic_row_in_frame(frame, shape)
                .map_err(|error| TransformFailure::at(error, row))?;
        }
        Ok(())
    }

    pub(crate) fn transform_shape_with_computed_edges(&self, shape: &Shape) -> Result<Shape> {
        self.transform_shapes_with_computed_edges_indexed(&[shape])
            .map_err(|failure| failure.error)?
            .into_iter()
            .next()
            .ok_or_else(|| {
                crate::geometry::GeometryErrorKind::Projection(
                    "single-shape transform returned no geometry".into(),
                )
                .into()
            })
    }

    pub(crate) fn transform_shapes_with_computed_edges_indexed(
        &self,
        shapes: &[&Shape],
    ) -> std::result::Result<Vec<Shape>, TransformFailure> {
        let mut output = self.transform_shapes_indexed_mode(shapes, true)?;
        if self.is_identity() {
            return Ok(output);
        }
        let Some(frame) = self.computed_geographic_frame() else {
            return Ok(output);
        };
        let mut metric = self.target_metric_scratch();
        for (index, (source, transformed)) in shapes.iter().zip(&mut output).enumerate() {
            *transformed = crate::geometry::publish_transformed_shape(
                source,
                transformed,
                self.source_geographic_frame(),
                frame,
                "to_crs",
                &mut metric,
            )
            .map_err(|error| TransformFailure::at(error, index))?;
        }
        Ok(output)
    }

    fn begin_execution(&self) {
        begin_transform_observation();
    }

    fn finish_execution<T, E>(&self, result: Result<T, E>, executed: bool) -> Result<T, E> {
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
        self.transform_shapes_indexed(shapes)
            .map_err(|failure| failure.error)
    }

    pub(crate) fn transform_shapes_indexed(
        &self,
        shapes: &[&Shape],
    ) -> std::result::Result<Vec<Shape>, TransformFailure> {
        self.transform_shapes_indexed_mode(shapes, false)
    }

    fn transform_shapes_indexed_mode(
        &self,
        shapes: &[&Shape],
        keep_target_chart: bool,
    ) -> std::result::Result<Vec<Shape>, TransformFailure> {
        self.begin_execution();
        let result: std::result::Result<Vec<Shape>, TransformFailure> = match &self.operation {
            TransformOperation::Identity { .. } => {
                for (index, shape) in shapes.iter().enumerate() {
                    self.admit_identity_shape_at(index, shape)?;
                }
                Ok(shapes.iter().map(|shape| (**shape).clone()).collect())
            },
            TransformOperation::InCore(operation) => {
                let mut output = TransformOutputPlan::try_new(shapes)?;
                output.execute_indexed(|x, y, _| {
                    let transform = if keep_target_chart {
                        transform_in_core_xy_with_domains_indexed_for_shape
                    } else {
                        transform_in_core_xy_with_domains_indexed
                    };
                    transform(operation, self.source_domain, self.target_domain, x, y)
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
                        self.default_proj_source_frame(),
                        &self.execution,
                    )
                },
            ),
        };
        self.finish_execution(result, !shapes.is_empty())
    }

    pub(crate) fn transform_coordinates(
        &self,
        x: &mut [f64],
        y: &mut [f64],
        zt: crate::ZtLanes<'_>,
    ) -> Result<()> {
        self.transform_coordinates_indexed(x, y, zt)
            .map_err(|failure| failure.error)
    }

    pub(crate) fn transform_coordinates_indexed(
        &self,
        x: &mut [f64],
        y: &mut [f64],
        zt: crate::ZtLanes<'_>,
    ) -> std::result::Result<(), TransformFailure> {
        self.transform_coordinates_indexed_mode(x, y, zt, false)
    }

    pub(crate) fn transform_coordinates_indexed_for_shape(
        &self,
        x: &mut [f64],
        y: &mut [f64],
        zt: crate::ZtLanes<'_>,
    ) -> std::result::Result<(), TransformFailure> {
        self.transform_coordinates_indexed_mode(x, y, zt, true)
    }

    fn transform_coordinates_indexed_mode(
        &self,
        x: &mut [f64],
        y: &mut [f64],
        zt: crate::ZtLanes<'_>,
        keep_target_chart: bool,
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
            TransformOperation::Identity { .. } => {
                if let Some(frame) = self.identity_geographic_frame() {
                    for (index, (&lon, &lat)) in x.iter().zip(y.iter()).enumerate() {
                        ensure_geographic_lonlat_in_frame(frame, lon, lat)
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
                let transform = if keep_target_chart {
                    transform_in_core_xy_with_domains_indexed_for_shape
                } else {
                    transform_in_core_xy_with_domains_indexed
                };
                transform(operation, self.source_domain, self.target_domain, x, y)
            },
            TransformOperation::Proj {
                source,
                target,
                options,
            } => {
                let source_frame = self.default_proj_source_frame();
                with_proj_pipeline_indexed_at(
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
                                transformer.transform_default_source_lanes_indexed(
                                    source_frame,
                                    x,
                                    y,
                                    None,
                                    Some(t.as_mut_slice()),
                                    &self.execution,
                                )
                            } else {
                                transformer
                                    .require_epoch_lane()
                                    .map_err(TransformFailure::from)?;
                                transformer.transform_default_source_lanes_indexed(
                                    source_frame,
                                    x,
                                    y,
                                    None,
                                    None,
                                    &self.execution,
                                )
                            }
                        },
                        crate::Zt::Z(z) => {
                            if let Some(epoch) = options.source_epoch {
                                let mut t = [epoch];
                                transformer.transform_default_source_lanes_indexed(
                                    source_frame,
                                    x,
                                    y,
                                    Some(z),
                                    Some(t.as_mut_slice()),
                                    &self.execution,
                                )
                            } else {
                                transformer
                                    .require_epoch_lane()
                                    .map_err(TransformFailure::from)?;
                                transformer.transform_default_source_lanes_indexed(
                                    source_frame,
                                    x,
                                    y,
                                    Some(z),
                                    None,
                                    &self.execution,
                                )
                            }
                        },
                        crate::Zt::T(t) => transformer.transform_default_source_lanes_indexed(
                            source_frame,
                            x,
                            y,
                            None,
                            Some(t),
                            &self.execution,
                        ),
                        crate::Zt::Zt { z, t } => transformer
                            .transform_default_source_lanes_indexed(
                                source_frame,
                                x,
                                y,
                                Some(z),
                                Some(t),
                                &self.execution,
                            ),
                    },
                )?;
                if !keep_target_chart && let Some(frame) = self.computed_geographic_frame() {
                    frame
                        .canonicalize_computed_longitude_lanes_indexed(x, y)
                        .map_err(|(index, error)| {
                            TransformFailure::at(
                                crate::crs::CrsError::invalid(error.to_string()),
                                index,
                            )
                        })?;
                }
                Ok(())
            },
        };
        self.finish_execution(result, !x.is_empty())
    }

    pub(crate) fn transform_bounds(
        &self,
        bounds: (f64, f64, f64, f64),
        densify: u32,
    ) -> Result<(f64, f64, f64, f64)> {
        self.begin_execution();
        let interpretation = BoundsInterpretation::from_force_over(self.options.force_over);
        let result = if self.is_identity() {
            identity_bounds_2d(bounds, self.target_domain, interpretation)
        } else {
            let mut samples = BoundsSamplePlan::new_2d(
                bounds,
                densify,
                interpretation,
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
        let interpretation = BoundsInterpretation::from_force_over(self.options.force_over);
        let result = self.execute_bounds_many(rows, |bounds, executor| {
            if matches!(executor, BoundsExecutor::Identity) {
                return identity_bounds_2d(bounds, self.target_domain, interpretation);
            }
            let mut samples = BoundsSamplePlan::new_2d(
                bounds,
                densify,
                interpretation,
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
        let interpretation = BoundsInterpretation::from_force_over(self.options.force_over);
        let result = if self.is_identity() {
            identity_bounds_3d(bounds, self.target_domain, interpretation)
        } else {
            let mut samples = BoundsSamplePlan::new_3d(
                bounds,
                densify,
                interpretation,
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
        let interpretation = BoundsInterpretation::from_force_over(self.options.force_over);
        let result = self.execute_bounds_many(rows, |bounds, executor| {
            if matches!(executor, BoundsExecutor::Identity) {
                return identity_bounds_3d(bounds, self.target_domain, interpretation);
            }
            let mut samples = BoundsSamplePlan::new_3d(
                bounds,
                densify,
                interpretation,
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
        let mut output = Vec::with_capacity(rows.len());
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
        ErrorKind::Geometry(GeometryErrorKind::ProjectionDomain(_),)
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
    geometry_frame: Option<GeometryTransformFrame>,
}

/// One transform's target metric, materialized only when a generated sample
/// carries an ordinate.
pub(crate) struct TransformMetricScratch<'a> {
    frame: &'a GeometryTransformFrame,
    metric: Option<crate::crs::GeodesicMetric>,
}

impl TransformMetricScratch<'_> {
    pub(crate) fn geodesic_metric(&mut self) -> Result<&crate::crs::GeodesicMetric> {
        if self.metric.is_none() {
            let model =
                crate::crs::metric_model_at(Some(self.frame.target.as_str()), &self.frame.state)?;
            let metric = match crate::crs::ResolvedMetric::from_model(&model)? {
                crate::crs::ResolvedMetric::Geodesic(metric) => metric,
                crate::crs::ResolvedMetric::Planar { .. } => {
                    return Err(crate::crs::CrsError::invalid(
                        "geographic transform target has no ellipsoid metric",
                    ));
                },
            };
            self.metric = Some(metric);
        }
        Ok(self
            .metric
            .as_ref()
            .expect("resolved target ellipsoid is retained in its operation scratch"))
    }
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
        source: SmolStr,
        target: SmolStr,
        options: TransformOptions,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn computed_transform_plan(
        source: &'static str,
        target: &'static str,
        options: TransformOptions,
    ) -> TransformPlan {
        let source_frame = Frame::Crs(crs_arc_static(source));
        let frame =
            GeometryTransformFrame::new(&source_frame, crs_arc_static(target), None, options)
                .unwrap();
        TransformPlan::from_geometry_frame(frame).unwrap()
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
    fn target_metric_comes_from_the_output_frame_owned_by_the_plan() {
        let plan = computed_transform_plan("EPSG:3857", "EPSG:4326", TransformOptions::default());
        assert_eq!(plan.output_frame().crs_str(), Some("EPSG:4326"));

        let mut metric = plan.target_metric_scratch();
        let target_metric = metric.geodesic_metric().unwrap();
        assert_eq!(
            target_metric.frame,
            plan.computed_geographic_frame().unwrap(),
        );
        assert_eq!(
            target_metric.state.generation,
            plan.geometry_frame.as_ref().unwrap().state.generation,
        );
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
    fn proj_numeric_target_geographic_output_canonicalizes_only_longitude() {
        let plan = TransformPlan::new_with_options("EPSG:3857", "EPSG:4326", TransformOptions {
            authority: Some("EPSG".into()),
            ..TransformOptions::default()
        })
        .unwrap();
        let radius = crate::crs::WEB_MERCATOR_RADIUS;
        let mut x = [
            radius * std::f64::consts::PI,
            radius * std::f64::consts::TAU,
        ];
        let mut y = [-0.0, -0.0];
        let mut z = [3.0, 5.0];
        let mut t = [2020.0, 2021.0];
        plan.transform_coordinates(&mut x, &mut y, crate::Zt::Zt {
            z: &mut z,
            t: &mut t,
        })
        .unwrap();
        assert_eq!(x, [-180.0, 0.0]);
        assert_eq!(y.map(f64::to_bits), [(-0.0_f64).to_bits(); 2]);
        assert_eq!(z, [3.0, 5.0]);
        assert_eq!(t, [2020.0, 2021.0]);

        let forced = TransformPlan::new_with_options("EPSG:3857", "EPSG:4326", TransformOptions {
            authority: Some("EPSG".into()),
            force_over: true,
            ..TransformOptions::default()
        })
        .unwrap();
        let mut x = [
            radius * std::f64::consts::PI,
            radius * std::f64::consts::TAU,
        ];
        let mut y = [0.0, 0.0];
        forced
            .transform_coordinates(&mut x, &mut y, crate::Zt::None)
            .unwrap();
        assert_eq!(x, [180.0, 360.0]);
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
    fn default_proj_source_latitude_failure_transforms_only_its_valid_prefix() {
        let _guard = crate::crs::RUNTIME_STATE_TEST_LOCK.lock().unwrap();
        crate::crs::clear_cache();
        let radians = concat!(
            "GEOGCRS[\"WGS 84 radians\",DATUM[\"World Geodetic System 1984\",",
            "ELLIPSOID[\"WGS 84\",6378137,298.257223563]],",
            "PRIMEM[\"Greenwich\",0,ANGLEUNIT[\"degree\",0.017453292519943295]],",
            "CS[ellipsoidal,2],AXIS[\"Longitude\",east,ORDER[1]],",
            "AXIS[\"Latitude\",north,ORDER[2]],ANGLEUNIT[\"radian\",1]]",
        );
        let plan = TransformPlan::new_with_options("EPSG:4326", radians, TransformOptions {
            source_epoch: Some(2020.0),
            authority: Some("EPSG".into()),
            ..TransformOptions::default()
        })
        .unwrap();
        let mut x = [10.0, 20.0, 30.0];
        let mut y = [1.0, 91.0, 2.0];
        let mut z = [3.0, 4.0, 5.0];

        let failure = plan
            .transform_coordinates_indexed(&mut x, &mut y, crate::Zt::Z(&mut z))
            .unwrap_err();

        assert_eq!(failure.coordinate, Some(1));
        assert!(
            failure
                .error
                .to_string()
                .contains("latitude 9.1e1 is outside the geographic coordinate domain")
        );
        assert_eq!(x[0], 10.0_f64.to_radians());
        assert_eq!(y[0], 1.0_f64.to_radians());
        assert_eq!(&x[1..], &[20.0, 30.0]);
        assert_eq!(&y[1..], &[91.0, 2.0]);
        assert_eq!(z, [3.0, 4.0, 5.0]);

        let observation = crate::crs::cache_info();
        assert_eq!(observation.last_transform_engine, Some("proj"));
        assert_eq!(observation.transform_invocations, 1);
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
    fn default_proj_source_latitude_precedes_later_backend_failure() {
        let _guard = crate::crs::RUNTIME_STATE_TEST_LOCK.lock().unwrap();
        crate::crs::clear_cache();
        let plan = TransformPlan::new_with_options("EPSG:4326", "EPSG:3857", TransformOptions {
            authority: Some("EPSG".into()),
            ..TransformOptions::default()
        })
        .unwrap();
        let mut x = [0.0, 0.0];
        let mut y = [91.0, 90.0];

        let failure = plan
            .transform_coordinates_indexed(&mut x, &mut y, crate::Zt::None)
            .unwrap_err();

        assert_eq!(failure.coordinate, Some(0));
        assert!(
            failure
                .error
                .to_string()
                .contains("latitude 9.1e1 is outside the geographic coordinate domain")
        );
        assert_eq!(x, [0.0, 0.0]);
        assert_eq!(y, [91.0, 90.0]);
        assert_eq!(crate::crs::cache_info().transform_invocations, 0);
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
    fn earlier_proj_domain_failure_precedes_later_invalid_source_latitude() {
        let _guard = crate::crs::RUNTIME_STATE_TEST_LOCK.lock().unwrap();
        crate::crs::clear_cache();
        let plan = TransformPlan::new_with_options("EPSG:4326", "EPSG:32660", TransformOptions {
            authority: Some("EPSG".into()),
            ..TransformOptions::default()
        })
        .unwrap();
        let mut x = [-90.0, 0.0];
        let mut y = [0.0, 91.0];

        let failure = plan
            .transform_coordinates_indexed(&mut x, &mut y, crate::Zt::None)
            .unwrap_err();

        assert_eq!(failure.coordinate, Some(0));
        assert!(
            failure
                .error
                .to_string()
                .contains("Point outside of projection domain")
        );
        assert_eq!(crate::crs::cache_info().transform_invocations, 1);
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
    fn identity_geometry_admission_is_pure_and_uses_the_source_frame() {
        let _guard = crate::crs::RUNTIME_STATE_TEST_LOCK.lock().unwrap();
        crate::crs::clear_cache();
        let plan = TransformPlan::new("EPSG:4326", "OGC:CRS84").unwrap();
        assert!(plan.is_identity());
        let before = crate::crs::cache_info();
        let valid = crate::geometry::Shape::LineString(
            crate::geometry::LineSeq::try_new(
                crate::geometry::CoordSeq::try_from_points(&[
                    crate::geometry::Point::new_raw_xy(180.0, -0.0),
                    crate::geometry::Point::new_raw_xy(540.0, 0.0),
                ])
                .unwrap(),
            )
            .unwrap(),
        );
        plan.admit_identity_shape(&valid)
            .expect("valid aliases remain byte-preserved");
        let invalid = crate::geometry::Shape::Point(crate::geometry::Point::new_raw_xy(0.0, 91.0));
        assert!(plan.admit_identity_shape(&invalid).is_err());
        let after = crate::crs::cache_info();
        assert_eq!(after.last_transform_engine, before.last_transform_engine);
        assert_eq!(after.transform_invocations, before.transform_invocations);
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
    fn empty_bulk_bounds_do_not_acquire_proj_pipeline() {
        let _guard = crate::crs::RUNTIME_STATE_TEST_LOCK.lock().unwrap();
        crate::crs::clear_cache();
        let plan = TransformPlan::new_with_options("EPSG:4326", "EPSG:3857", TransformOptions {
            force_over: true,
            ..TransformOptions::default()
        })
        .unwrap();

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
    #[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
    fn identity_bounds_share_native_endpoint_intervals_for_scalar_and_bulk() {
        let plan = TransformPlan::new("EPSG:4326", "EPSG:4326").unwrap();
        let wide_2d = (-170.0, -1.0, 170.0, 1.0);
        assert_eq!(plan.transform_bounds(wide_2d, 21).unwrap(), wide_2d);
        assert_eq!(plan.transform_bounds_many(&[wide_2d], 21).unwrap(), [
            wide_2d
        ]);

        let wide_3d = (-170.0, -1.0, 3.0, 170.0, 1.0, 4.0);
        assert_eq!(plan.transform_bounds_3d(wide_3d, 21).unwrap(), wide_3d);
        assert_eq!(plan.transform_bounds_3d_many(&[wide_3d], 21).unwrap(), [
            wide_3d
        ]);

        let near_period = (-0.1, 0.0, 359.9, 1.0);
        let scalar = plan.transform_bounds(near_period, 21).unwrap();
        assert_ne!(scalar, (-180.0, 0.0, 180.0, 1.0));
        assert_eq!(plan.transform_bounds_many(&[near_period], 21).unwrap(), [
            scalar
        ]);
        assert_eq!(scalar.0, -0.1);
        assert_eq!(
            scalar.2,
            crate::geographic::GeographicFrame::crs84()
                .longitude()
                .canonicalize(359.9)
                .unwrap()
                .get()
        );
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
    fn force_over_identity_bounds_keep_literal_numeric_longitude_extrema() {
        let plan = TransformPlan::new_with_options("EPSG:4326", "EPSG:4326", TransformOptions {
            force_over: true,
            ..TransformOptions::default()
        })
        .unwrap();
        assert_eq!(
            plan.transform_bounds((540.0, -1.0, 550.0, 1.0), 21)
                .unwrap(),
            (540.0, -1.0, 550.0, 1.0)
        );
        let descending = (170.0, -1.0, -170.0, 1.0);
        assert_eq!(
            plan.transform_bounds(descending, 21).unwrap(),
            (-170.0, -1.0, 170.0, 1.0)
        );
        let descending_3d = (170.0, -1.0, 3.0, -170.0, 1.0, 4.0);
        assert_eq!(
            plan.transform_bounds_3d_many(&[descending_3d], 21).unwrap(),
            [(-170.0, -1.0, 3.0, 170.0, 1.0, 4.0)]
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

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
    fn target_geographic_transform_subdivides_its_produced_major_chart_edge() {
        let source = crate::geometry::Shape::LineString(
            crate::geometry::LineSeq::try_new(
                crate::geometry::CoordSeq::try_from_points(&[
                    crate::geometry::Point::new_raw_xy(20_000_000.0, 0.0),
                    crate::geometry::Point::new_raw_xy(-20_000_000.0, 0.0),
                ])
                .unwrap(),
            )
            .unwrap(),
        );
        let plan = computed_transform_plan("EPSG:3857", "EPSG:4326", TransformOptions::default());
        let output = plan.transform_shape_with_computed_edges(&source).unwrap();
        let crate::geometry::Shape::LineString(output) = output else {
            panic!("to_crs retains a linestring result");
        };
        let points = output.iter().collect::<Vec<_>>();
        assert_eq!(points.len(), 3);
        assert!(points[0].x > 170.0);
        assert_eq!(points[1].x, 0.0);
        assert!(points[2].x < -170.0);
        let axis = crate::geographic::GeographicFrame::crs84().longitude();
        for pair in points.array_windows::<2>() {
            assert!(
                axis.chart_step(pair[0].x, pair[1].x)
                    .unwrap()
                    .delta(axis)
                    .abs()
                    <= axis.half_period()
            );
        }
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
    fn force_over_shape_transform_keeps_unrolled_longitudes_and_edge_count() {
        let source = crate::geometry::Shape::LineString(
            crate::geometry::LineSeq::try_new(
                crate::geometry::CoordSeq::try_from_points(&[
                    crate::geometry::Point::new_raw_xy(0.0, 0.0),
                    crate::geometry::Point::new_raw_xy(41_000_000.0, 0.0),
                ])
                .unwrap(),
            )
            .unwrap(),
        );
        let plan = computed_transform_plan("EPSG:3857", "EPSG:4326", TransformOptions {
            force_over: true,
            ..TransformOptions::default()
        });
        let raw = plan.transform_shape(&source).unwrap();
        let output = plan.transform_shape_with_computed_edges(&source).unwrap();
        assert_eq!(output, raw);
        let crate::geometry::Shape::LineString(line) = output else {
            panic!("a raw transform retains its line");
        };
        assert_eq!(line.len(), 2);
        assert!(line.last().unwrap().x > 360.0);
    }
}
