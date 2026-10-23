//! Version-one durable disk values, independent of live storage and caches.

use serde::{Deserialize, Serialize};

use super::{CertifiedDisk, CircleSolution, ExactRational, Point, Result};
use crate::geometry::tessellation::exact::{CompactLimbs, ExactDyadic};
use crate::geometry::{MOrdinate, ZOrdinate};

#[derive(Serialize, Deserialize)]
pub(crate) struct PointStateV1(u64, u64, Option<u64>, Option<u64>);

impl From<Point> for PointStateV1 {
    fn from(point: Point) -> Self {
        Self(
            point.x.to_bits(),
            point.y.to_bits(),
            point.z().map(f64::to_bits),
            point.m().map(f64::to_bits),
        )
    }
}

impl PointStateV1 {
    pub(crate) fn into_point(self) -> Point {
        Point::new_raw_axes(
            f64::from_bits(self.0),
            f64::from_bits(self.1),
            ZOrdinate(self.2.map(f64::from_bits)),
            MOrdinate(self.3.map(f64::from_bits)),
        )
    }
}

/// Signed integer coefficient in little-endian base-2^64 digits, times 2^exponent.
#[derive(Serialize, Deserialize)]
pub(crate) struct DyadicStateV1(bool, Vec<u64>, i64);

impl From<&ExactDyadic> for DyadicStateV1 {
    fn from(value: &ExactDyadic) -> Self {
        Self(
            value.negative,
            value.limbs.as_slice().to_vec(),
            value.exponent,
        )
    }
}

impl DyadicStateV1 {
    fn into_dyadic(self) -> ExactDyadic {
        ExactDyadic {
            negative: self.0,
            limbs: CompactLimbs::from_vec(self.1),
            exponent: self.2,
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum CircleStateV1 {
    Empty,
    SourcePoint {
        point: PointStateV1,
    },
    Zero {
        point: PointStateV1,
    },
    Bounding {
        components: [DyadicStateV1; 5],
        source_center: Option<PointStateV1>,
    },
    Inscribed {
        center: PointStateV1,
        squared_radius: [DyadicStateV1; 2],
    },
}

impl From<&CircleSolution> for CircleStateV1 {
    fn from(value: &CircleSolution) -> Self {
        match value {
            CircleSolution::Empty => Self::Empty,
            CircleSolution::SourcePoint(point) => Self::SourcePoint {
                point: (*point).into(),
            },
            CircleSolution::Zero(point) => Self::Zero {
                point: (*point).into(),
            },
            CircleSolution::Bounding(disk) => {
                let (components, source_center) = disk.components();
                Self::Bounding {
                    components: components.map(DyadicStateV1::from),
                    source_center: source_center.map(PointStateV1::from),
                }
            },
            CircleSolution::Inscribed {
                center,
                squared_radius,
            } => {
                let (numerator, denominator) = squared_radius.components();
                Self::Inscribed {
                    center: (*center).into(),
                    squared_radius: [numerator.into(), denominator.into()],
                }
            },
        }
    }
}

impl CircleStateV1 {
    pub(crate) fn restore(self) -> Result<CircleSolution> {
        Ok(match self {
            Self::Empty => CircleSolution::Empty,
            Self::SourcePoint { point } => CircleSolution::SourcePoint(point.into_point()),
            Self::Zero { point } => CircleSolution::Zero(point.into_point()),
            Self::Bounding {
                components,
                source_center,
            } => CircleSolution::Bounding(Box::new(CertifiedDisk::from_components(
                components.map(DyadicStateV1::into_dyadic),
                source_center.map(PointStateV1::into_point),
            )?)),
            Self::Inscribed {
                center,
                squared_radius,
            } => {
                let [numerator, denominator] = squared_radius;
                CircleSolution::Inscribed {
                    center: center.into_point(),
                    squared_radius: ExactRational::new(
                        numerator.into_dyadic(),
                        denominator.into_dyadic(),
                    )?,
                }
            },
        })
    }
}
