//! Extended-range planar stations for routes whose native lengths overflow.
//!
//! Each segment norm is measured in binary64, then retained as a dyadic value
//! with an extended exponent. Prefix addition keeps small earlier segments
//! visible even when a later segment makes the total exceed binary64.

use super::{
    CoordSeq, GeometryErrorKind, LineIndex, LineSeq, MeasureRange, OrdinateSegment, PlanarMetric,
    Point, Result, Shape, point_distance, push_distinct_active_position,
};
use crate::HeapSize as _;
use crate::geometry::numerics::product::exact_power_of_two;
use crate::geometry::segments::projection::segment_projection;
use crate::geometry::tessellation::exact::{ExactDyadic, ExactRational};
use crate::geometry::{Ordering, Segment, lerp_point};

pub(super) struct WidePrefix {
    values: Box<[ExactDyadic]>,
}

impl WidePrefix {
    pub(super) fn heap_bytes(&self) -> usize {
        use crate::geometry::tessellation::exact::CompactLimbs;

        self.values.iter().fold(
            self.values.len().saturating_mul(size_of::<ExactDyadic>()),
            |total, value| {
                total.saturating_add(match &value.limbs {
                    CompactLimbs::Inline { .. } => 0,
                    CompactLimbs::Heap(limbs) => limbs.heap_bytes(),
                })
            },
        )
    }

    pub(super) fn build(segments: &[OrdinateSegment]) -> Result<Self> {
        let mut values = Vec::with_capacity(segments.len().saturating_add(1));
        let mut total = ExactDyadic::zero();
        values.push(total.clone());
        for segment in segments {
            let length = point_distance(segment.start, segment.end);
            let length = if length.is_normal() || length == 0.0 {
                ExactDyadic::from_f64(length)
            } else if length.is_finite() {
                let scale = exact_power_of_two(1023);
                let dx = (segment.end.x - segment.start.x) * scale;
                let dy = (segment.end.y - segment.start.y) * scale;
                let mut length = ExactDyadic::from_f64(dx.hypot(dy));
                length.exponent = length.exponent.wrapping_sub(1023);
                length
            } else {
                // Quartering each endpoint bounds both residuals by MAX/2,
                // and their Euclidean norm by MAX/sqrt(2).
                let dx = segment.end.x * 0.25 - segment.start.x * 0.25;
                let dy = segment.end.y * 0.25 - segment.start.y * 0.25;
                let mut length = ExactDyadic::from_f64(dx.hypot(dy));
                length.exponent = length.exponent.wrapping_add(2);
                length
            };
            total = total.try_add(length)?;
            values.push(total.clone());
        }
        Ok(Self {
            values: values.into_boxed_slice(),
        })
    }

    pub(super) fn total(&self) -> ExactRational {
        ExactRational::from_dyadic(
            self.values
                .last()
                .expect("prefix contains its origin")
                .clone(),
        )
    }

    pub(super) fn target(
        &self,
        distance: f64,
        normalized: bool,
        scale: f64,
    ) -> Result<ExactRational> {
        let total = self.total();
        let negative = distance < 0.0;
        let distance = ExactRational::from_f64(distance);
        let value = if normalized {
            total.try_product(&distance)?
        } else {
            let distance = distance.try_quotient(&ExactRational::from_f64(scale))?;
            if negative {
                total.try_add(&distance)?
            } else {
                distance
            }
        };
        if value.try_cmp(&ExactRational::zero())?.is_lt() {
            Ok(ExactRational::zero())
        } else if value.try_cmp(&total)?.is_gt() {
            Ok(total)
        } else {
            Ok(value)
        }
    }

    pub(super) fn boundary(&self, index: usize) -> ExactRational {
        ExactRational::from_dyadic(
            self.values
                .get(index)
                .expect("station boundary belongs to the prefix")
                .clone(),
        )
    }

    pub(super) fn segment_at(&self, target: &ExactRational) -> Result<usize> {
        let mut low = 0;
        let mut high = self.values.len();
        while low < high {
            let middle = low.midpoint(high);
            if self.boundary(middle).try_cmp(target)?.is_lt() {
                low = middle.wrapping_add(1);
            } else {
                high = middle;
            }
        }
        Ok(low
            .saturating_sub(1)
            .min(self.values.len().saturating_sub(2)))
    }

    pub(super) fn interpolate(
        &self,
        segments: &[OrdinateSegment],
        target: &ExactRational,
    ) -> Result<Point> {
        let index = self.segment_at(target)?;
        let start = self.boundary(index);
        let end = self.boundary(index.wrapping_add(1));
        let segment = *segments
            .get(index)
            .expect("station segment belongs to the prefix");
        if target.try_cmp(&start)? == Ordering::Equal {
            return Ok(segment.start);
        }
        if target.try_cmp(&end)? == Ordering::Equal {
            return Ok(segment.end);
        }
        let fraction = target
            .try_subtract(&start)?
            .try_quotient(&end.try_subtract(&start)?)?
            .round()?;
        Ok(lerp_point(segment.start, segment.end, fraction))
    }

    pub(super) fn locate(
        &self,
        ordinal: usize,
        segment: OrdinateSegment,
        probe: Point,
        normalized: bool,
        scale: f64,
    ) -> Result<f64> {
        let start = self.boundary(ordinal);
        let end = self.boundary(ordinal.wrapping_add(1));
        let source = Segment {
            start: segment.start.xy(),
            end: segment.end.xy(),
        };
        let fraction = segment_projection(probe, source)?.exact_fraction()?;
        let station = start.try_add(&end.try_subtract(&start)?.try_product(&fraction)?)?;
        let value = if normalized {
            station.try_quotient(&self.total())?
        } else {
            station.try_product(&ExactRational::from_f64(scale))?
        };
        match value.round() {
            Ok(value) => Ok(value),
            Err(error)
                if matches!(
                    error.kind(),
                    crate::error::ErrorKind::Geometry(GeometryErrorKind::ExactCoordinateOutOfRange)
                ) =>
            {
                Ok(f64::INFINITY)
            },
            Err(error) => Err(error),
        }
    }
}

impl LineIndex {
    /// Interpolate in output units without rounding an extreme native station.
    pub(crate) fn interpolate_planar(
        &self,
        distance: f64,
        normalized: bool,
        scale: f64,
    ) -> Result<Point> {
        if self.wide.is_none() && self.total == 0.0 {
            return self.interpolate_route_position(distance, normalized, &PlanarMetric);
        }
        let native = if normalized {
            distance
        } else {
            distance / scale
        };
        let temporary =
            if self.wide.is_none() && !normalized && !native.is_normal() && distance != 0.0 {
                Some(WidePrefix::build(&self.segments)?)
            } else {
                None
            };
        if let Some(wide) = self.wide.as_deref().or(temporary.as_ref()) {
            let target = wide.target(distance, normalized, scale)?;
            if self.has_zero_segments && target.try_cmp(&wide.total())?.is_eq() {
                return Ok(self.last);
            }
            return wide.interpolate(&self.segments, &target);
        }
        self.interpolate_route_position(native, normalized, &PlanarMetric)
    }

    /// Extract a planar interval while keeping extreme stationing in range.
    pub(crate) fn substring_planar(
        &self,
        range: MeasureRange,
        normalized: bool,
        scale: f64,
    ) -> Result<Shape> {
        if self.wide.is_none() && self.total == 0.0 {
            return self.substring(range, normalized, &PlanarMetric);
        }
        let start_native = if normalized {
            range.start()
        } else {
            range.start() / scale
        };
        let end_native = if normalized {
            range.end()
        } else {
            range.end() / scale
        };
        let temporary = if self.wide.is_none()
            && !normalized
            && ((!start_native.is_normal() && range.start() != 0.0)
                || (!end_native.is_normal() && range.end() != 0.0))
        {
            Some(WidePrefix::build(&self.segments)?)
        } else {
            None
        };
        let Some(wide) = self.wide.as_deref().or(temporary.as_ref()) else {
            let native = if normalized {
                range
            } else {
                MeasureRange::substring_distance(start_native, end_native)?
            };
            return self.substring(native, normalized, &PlanarMetric);
        };
        let start = wide.target(range.start(), normalized, scale)?;
        let end = wide.target(range.end(), normalized, scale)?;
        if start.try_cmp(&end)?.is_gt() {
            return Err(GeometryErrorKind::SubstringOrder(range.start(), range.end()).into());
        }
        if start.try_cmp(&end)?.is_eq() {
            return Ok(Shape::Point(wide.interpolate(&self.segments, &start)?));
        }
        let mut points = Vec::new();
        let first = wide.segment_at(&start)?;
        for (index, segment) in self.segments.iter().enumerate().skip(first) {
            let left = wide.boundary(index);
            let right = wide.boundary(index.wrapping_add(1));
            if right.try_cmp(&start)?.is_lt() {
                continue;
            }
            if left.try_cmp(&end)?.is_gt() {
                break;
            }
            if left.try_cmp(&right)?.is_eq() {
                if left.try_cmp(&start)?.is_ge() && left.try_cmp(&end)?.is_le() {
                    push_distinct_active_position(&mut points, segment.start);
                    push_distinct_active_position(&mut points, segment.end);
                }
                continue;
            }
            let length = right.try_subtract(&left)?;
            let from = if start.try_cmp(&left)?.is_gt() {
                start.try_subtract(&left)?.try_quotient(&length)?.round()?
            } else {
                0.0
            };
            let to = if end.try_cmp(&right)?.is_lt() {
                end.try_subtract(&left)?.try_quotient(&length)?.round()?
            } else {
                1.0
            };
            push_distinct_active_position(
                &mut points,
                lerp_point(segment.start, segment.end, from),
            );
            push_distinct_active_position(&mut points, lerp_point(segment.start, segment.end, to));
        }
        Ok(match points.as_slice() {
            [] => Shape::Point(wide.interpolate(&self.segments, &start)?),
            [point] => Shape::Point(*point),
            _ => Shape::LineString(LineSeq::from_proven(CoordSeq::try_from_points(&points)?)),
        })
    }
}
