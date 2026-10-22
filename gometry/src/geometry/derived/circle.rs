use std::num::NonZeroU32;

use super::minimum_circle::{CertifiedDisk, next_realization_radius};
use super::{build_inscribed_circle_budgeted, emit_minimum_bounding_circle};
use crate::error::Result;
use crate::geometry::{ExactRational, ExpansionBudget, Point, Shape};

/// A solved disk, independent of its polygonal realization.
#[derive(Debug)]
pub(crate) enum CircleSolution {
    Empty,
    Zero(Point),
    Bounding(Box<CertifiedDisk>),
    Inscribed {
        center: Point,
        squared_radius: ExactRational,
    },
}

impl CircleSolution {
    fn exact_values(&self) -> Result<Option<(ExactRational, ExactRational, ExactRational)>> {
        Ok(match self {
            Self::Empty => None,
            Self::Zero(center) => Some((
                ExactRational::from_f64(center.x),
                ExactRational::from_f64(center.y),
                ExactRational::zero(),
            )),
            Self::Bounding(disk) => Some(disk.exact_values()?),
            Self::Inscribed {
                center,
                squared_radius,
            } => Some((
                ExactRational::from_f64(center.x),
                ExactRational::from_f64(center.y),
                squared_radius.clone(),
            )),
        })
    }

    pub(crate) fn same_disk(&self, other: &Self) -> Result<bool> {
        Ok(self.exact_values()? == other.exact_values()?)
    }

    pub(crate) fn bounding(shape: &Shape) -> Result<Self> {
        let points = shape.try_unique_xy_points()?;
        if points
            .iter()
            .any(|point| !point.x.is_finite() || !point.y.is_finite())
        {
            return Err(crate::geometry::GeometryErrorKind::unrepresentable_output(
                "minimum_bounding_circle",
            ));
        }
        if points.is_empty() {
            return Ok(Self::Empty);
        }
        Ok(Self::Bounding(Box::new(
            super::smallest_enclosing_circle(&points)?.certified_disk(&points)?,
        )))
    }

    pub(crate) fn inscribed(shape: &Shape, tolerance: Option<f64>, scale: f64) -> Result<Self> {
        shape
            .polygonal()
            .ok_or(crate::geometry::GeometryErrorKind::PolygonRequired)?;
        if shape.is_empty() {
            return Ok(Self::Empty);
        }
        Ok(shape
            .certified_inradius(tolerance, "maximum_inscribed_circle", scale)?
            .map_or_else(
                || {
                    Self::Zero(super::degenerate_polygon_center(
                        shape.bounds().expect("nonempty polygon has bounds"),
                    ))
                },
                |disk| Self::Inscribed {
                    center: disk.center,
                    squared_radius: disk.center_squared,
                },
            ))
    }

    pub(crate) fn center(&self) -> Result<Shape> {
        Ok(match self {
            Self::Empty => Shape::empty_point(),
            Self::Zero(point) => Shape::Point(*point),
            Self::Bounding(disk) => Shape::Point(disk.center()?),
            Self::Inscribed { center, .. } => Shape::Point(*center),
        })
    }

    pub(crate) fn radius(&self, scale: f64) -> Result<f64> {
        match self {
            Self::Empty => Ok(f64::NAN),
            Self::Zero(_) => Ok(0.0),
            Self::Bounding(disk) => disk.radius_in_scale(scale),
            Self::Inscribed { squared_radius, .. } => {
                let scale = ExactRational::from_f64(scale);
                let squared = squared_radius.try_product(&scale)?.try_product(&scale)?;
                let radius = squared.round_sqrt()?;
                if ExactRational::from_f64(radius)
                    .try_square()?
                    .try_cmp(&squared)?
                    .is_gt()
                {
                    Ok(radius.next_down())
                } else {
                    Ok(radius)
                }
            },
        }
    }

    pub(crate) fn to_polygon(
        &self,
        quadrant_segments: NonZeroU32,
        budget: &mut ExpansionBudget,
    ) -> Result<Shape> {
        match self {
            Self::Empty => Ok(Shape::empty_polygon()),
            Self::Zero(point) => Ok(Shape::Point(*point)),
            Self::Bounding(disk) if disk.is_exact_zero() => Ok(Shape::Point(disk.center()?)),
            Self::Bounding(disk) => emit_minimum_bounding_circle(disk, quadrant_segments, budget),
            Self::Inscribed {
                center,
                squared_radius,
            } => {
                let center = *center;
                let radius = self.radius(1.0)?;
                if radius < f64::MIN_POSITIVE {
                    return Ok(Shape::Point(center));
                }
                let mut candidate_radius = radius;
                let mut retries = 0_u32;
                loop {
                    let candidate = build_inscribed_circle_budgeted(
                        center,
                        candidate_radius,
                        quadrant_segments,
                        None,
                        budget,
                    )?;
                    let mut inside = true;
                    let mut failure = None;
                    candidate.for_each_point(|point| {
                        if !inside || failure.is_some() {
                            return;
                        }
                        let check = || -> Result<bool> {
                            let dx = ExactRational::from_f64(point.x)
                                .try_subtract(&ExactRational::from_f64(center.x))?;
                            let dy = ExactRational::from_f64(point.y)
                                .try_subtract(&ExactRational::from_f64(center.y))?;
                            Ok(dx
                                .try_square()?
                                .try_add(&dy.try_square()?)?
                                .try_cmp(squared_radius)?
                                .is_le())
                        };
                        match check() {
                            Ok(value) => inside = value,
                            Err(error) => failure = Some(error),
                        }
                    });
                    if let Some(error) = failure {
                        return Err(error);
                    }
                    if inside {
                        return Ok(candidate);
                    }
                    retries = retries.wrapping_add(1);
                    candidate_radius =
                        next_realization_radius(radius, candidate_radius, retries, false);
                    if !(candidate_radius > 0.0 && candidate_radius.is_finite()) {
                        return Err(crate::geometry::GeometryErrorKind::unrepresentable_output(
                            "maximum_inscribed_circle",
                        ));
                    }
                }
            },
        }
    }
}
