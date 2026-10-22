use std::num::NonZeroU32;
use std::sync::Arc;

use super::frame::LocalCrsDefinition;
use super::{LocalInputPurpose, PreparedLocalInput};
use crate::crs::{MetricModel, RuntimeState};
use crate::error::Result;
use crate::geographic::GeographicFrame;
use crate::geometry::{
    BufferCapStyle, BufferJoinStyle, CircleSolution, ExpansionBudget, Point, Shape, TopologyDomain,
};

pub(crate) struct GeographicCircleFrame {
    #[expect(
        clippy::rc_buffer,
        reason = "shares the metric resolver's CRS owner without copying its text"
    )]
    crs: Arc<String>,
    geographic: GeographicFrame,
    state: Arc<RuntimeState>,
}

/// A disk in exactly one metric space. Polygon realization never changes the
/// solved disk; ellipsoidal point disks use the geographic buffer approximation.
pub(crate) enum FramedCircle {
    Planar {
        disk: CircleSolution,
        scale: f64,
        crs: Option<crate::Crs>,
        metres: bool,
        geographic: Option<GeographicFrame>,
    },
    Local {
        disk: CircleSolution,
        local: LocalCrsDefinition,
        frame: GeographicCircleFrame,
    },
    Ellipsoidal {
        center: Point,
        radius: f64,
        frame: GeographicCircleFrame,
    },
}

impl FramedCircle {
    pub(crate) fn same_disk(&self, other: &Self) -> Result<bool> {
        match (self, other) {
            (
                Self::Planar {
                    disk: left,
                    scale: a,
                    crs: ac,
                    metres: am,
                    ..
                },
                Self::Planar {
                    disk: right,
                    scale: b,
                    crs: bc,
                    metres: bm,
                    ..
                },
            ) => Ok(a.to_bits() == b.to_bits() && ac == bc && am == bm && left.same_disk(right)?),
            (
                Self::Local {
                    disk: left,
                    local: a,
                    frame: af,
                },
                Self::Local {
                    disk: right,
                    local: b,
                    frame: bf,
                },
            ) => Ok(a.definition == b.definition
                && af.crs == bf.crs
                && af.state.generation == bf.state.generation
                && left.same_disk(right)?),
            (
                Self::Ellipsoidal {
                    center: a,
                    radius: ar,
                    frame: af,
                },
                Self::Ellipsoidal {
                    center: b,
                    radius: br,
                    frame: bf,
                },
            ) => Ok(a == b
                && ar.to_bits() == br.to_bits()
                && af.crs == bf.crs
                && af.state.generation == bf.state.generation),
            (Self::Planar { .. }, Self::Local { .. } | Self::Ellipsoidal { .. })
            | (Self::Local { .. }, Self::Planar { .. } | Self::Ellipsoidal { .. })
            | (Self::Ellipsoidal { .. }, Self::Planar { .. } | Self::Local { .. }) => Ok(false),
        }
    }

    pub(crate) fn solve(
        model: &MetricModel,
        source: Option<&str>,
        shape: &Shape,
        tolerance: Option<f64>,
        inscribed: bool,
        unit: Option<crate::DistanceUnit>,
        domain: TopologyDomain,
    ) -> Result<Self> {
        let solve = |shape: &Shape, scale| {
            if inscribed {
                CircleSolution::inscribed(shape, tolerance, scale)
            } else {
                CircleSolution::bounding(shape)
            }
        };
        match model {
            MetricModel::Planar { to_metre } => Ok(Self::Planar {
                disk: solve(shape, to_metre.get())?,
                scale: to_metre.get(),
                crs: source.map(crate::Crs::new),
                metres: unit == Some(crate::DistanceUnit::Meters),
                geographic: match domain {
                    TopologyDomain::Cartesian => None,
                    TopologyDomain::Geographic(frame) => Some(frame),
                },
            }),
            MetricModel::Geodesic { crs, frame, state } => {
                let frame = GeographicCircleFrame {
                    crs: Arc::clone(crs),
                    geographic: *frame,
                    state: Arc::clone(state),
                };
                if !inscribed && shape.point_set_only() {
                    crate::crs::ensure_geographic_domain_in_frame(frame.geographic, shape)?;
                    let mut points = Vec::with_capacity(3);
                    let _scan_completion = shape.try_for_each_first_point_run(|point| {
                        let key = frame.geographic.quotient_vertex(point.x, point.y);
                        if points.iter().all(|(existing, _)| *existing != key) {
                            points.push((key, point));
                        }
                        if points.len() == 3 {
                            std::ops::ControlFlow::Break(())
                        } else {
                            std::ops::ControlFlow::Continue(())
                        }
                    });
                    match points.as_slice() {
                        [] => {},
                        [(_, point)] => {
                            return Ok(Self::Ellipsoidal {
                                center: *point,
                                radius: 0.0,
                                frame,
                            });
                        },
                        [(_, left), (_, right)] => {
                            let (center, radius) = two_point_disk(*left, *right, &frame)?;
                            return Ok(Self::Ellipsoidal {
                                center,
                                radius,
                                frame,
                            });
                        },
                        [_, _, _, ..] => {},
                    }
                }
                let mut budget = ExpansionBudget::new("circle", "input_refinement");
                let prepared = PreparedLocalInput::prepare(
                    shape,
                    &[shape],
                    &frame.crs,
                    None,
                    LocalInputPurpose::BoundarySupport,
                    &frame.state,
                    &mut budget,
                )?;
                let Some(prepared) = prepared else {
                    return Ok(Self::Planar {
                        disk: solve(shape, 1.0)?,
                        scale: 1.0,
                        crs: None,
                        metres: true,
                        geographic: None,
                    });
                };
                let disk = solve(prepared.projected_working(), 1.0)?;
                let disk = if let CircleSolution::SourcePoint(point) = disk {
                    CircleSolution::Zero(point.to_xy())
                } else {
                    disk
                };
                let local = prepared.local_crs().clone();
                Ok(Self::Local { disk, local, frame })
            },
        }
    }

    pub(crate) fn radius(&self) -> Result<f64> {
        match self {
            Self::Planar { disk, scale, .. } => disk.radius(*scale),
            Self::Local { disk, .. } => disk.radius(1.0),
            Self::Ellipsoidal { radius, .. } => Ok(*radius),
        }
    }

    pub(crate) fn center(&self) -> Result<Shape> {
        match self {
            Self::Planar {
                disk, geographic, ..
            } => {
                let center = disk.center()?;
                if matches!(disk, CircleSolution::SourcePoint(_)) {
                    return Ok(center);
                }
                match geographic {
                    Some(frame) => crate::geometry::publish_computed_point_or_raw_shape(
                        &center,
                        *frame,
                        "circle.center",
                    ),
                    None => Ok(center),
                }
            },
            Self::Local { disk, local, frame } => crate::crs::transform_at(
                &disk.center()?,
                &local.definition,
                &frame.crs,
                &frame.state,
            ),
            Self::Ellipsoidal { center, .. } => Ok(Shape::Point(*center)),
        }
    }

    pub(crate) fn working_crs(&self) -> Option<&str> {
        match self {
            Self::Planar { crs, .. } => crs.as_deref(),
            Self::Local { local, .. } => Some(&local.definition),
            Self::Ellipsoidal { .. } => None,
        }
    }

    pub(crate) const fn unit(&self) -> &'static str {
        match self {
            Self::Planar { metres: false, .. } => "planar",
            Self::Planar { metres: true, .. } | Self::Local { .. } | Self::Ellipsoidal { .. } => {
                "meters"
            },
        }
    }

    pub(crate) fn to_polygon(&self, quadrant_segments: NonZeroU32) -> Result<Shape> {
        let mut budget = ExpansionBudget::new("circle.to_polygon", "quadrant_segments");
        match self {
            Self::Planar {
                disk, geographic, ..
            } => {
                let polygon = disk.to_polygon(quadrant_segments, &mut budget)?;
                if matches!(disk, CircleSolution::SourcePoint(_)) {
                    return Ok(polygon);
                }
                match geographic {
                    Some(frame) => crate::geometry::publish_computed_point_or_raw_shape(
                        &polygon,
                        *frame,
                        "circle.to_polygon",
                    ),
                    None => Ok(polygon),
                }
            },
            Self::Local { disk, local, frame } => {
                let polygon = disk.to_polygon(quadrant_segments, &mut budget)?;
                let restored = crate::crs::transform_at(
                    &polygon,
                    &local.definition,
                    &frame.crs,
                    &frame.state,
                )?;
                super::output::publish(
                    &polygon,
                    &restored,
                    local,
                    frame.geographic,
                    "circle.to_polygon",
                    false,
                    &mut budget,
                )
            },
            Self::Ellipsoidal {
                center,
                radius,
                frame,
            } => {
                if *radius == 0.0 {
                    return Ok(Shape::Point(*center));
                }
                let model = MetricModel::Geodesic {
                    crs: Arc::clone(&frame.crs),
                    frame: frame.geographic,
                    state: Arc::clone(&frame.state),
                };
                super::operations::metric_constructive_shape_budgeted(
                    &model,
                    Some(&frame.crs),
                    &Shape::Point(*center),
                    *radius,
                    &mut budget,
                    |point, distance, budget| {
                        point.buffer_with_style_budgeted(
                            distance,
                            BufferCapStyle::Round,
                            BufferJoinStyle::Round,
                            quadrant_segments,
                            crate::Positive::try_new("miter_limit", 5.0)?,
                            budget,
                        )
                    },
                )
            },
        }
    }
}

fn two_point_disk(
    left: Point,
    right: Point,
    frame: &GeographicCircleFrame,
) -> Result<(Point, f64)> {
    let left_geodesic = frame
        .geographic
        .to_geodesic_point(left)
        .map_err(|error| crate::crs::CrsError::invalid(error.to_string()))?;
    let right_geodesic = frame
        .geographic
        .to_geodesic_point(right)
        .map_err(|error| crate::crs::CrsError::invalid(error.to_string()))?;
    crate::crs::with_geodesic_cache_at(
        &frame.crs,
        &frame.state,
        |error| error,
        |geodesic| {
            let (distance, azimuth, _) = crate::crs::geodesic::inverse_distance_azimuths(
                geodesic,
                left_geodesic.x,
                left_geodesic.y,
                right_geodesic.x,
                right_geodesic.y,
            );
            let radius =
                crate::crs::finite(distance, crate::crs::GeodesicCalculation::Distance)? / 2.0;
            if radius == 0.0 {
                return Ok((left, radius));
            }
            let (x, y) = crate::crs::geodesic_destination(
                geodesic,
                left_geodesic.x,
                left_geodesic.y,
                azimuth,
                radius,
            )?;
            let center = frame
                .geographic
                .point_from_geodesic(Point::new_raw_xy(x, y))
                .map_err(|error| crate::crs::CrsError::invalid(error.to_string()))?;
            Ok((center, radius))
        },
    )
}
