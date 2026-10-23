use serde::{Deserialize, Serialize};

use super::{FramedCircle, GeographicCircleFrame, LocalCrsDefinition, Result};
use crate::geometry::{CircleStateV1, PointStateV1};

#[derive(Serialize, Deserialize)]
#[serde(tag = "frame", rename_all = "snake_case")]
pub(crate) enum FramedCircleStateV1 {
    Planar {
        disk: CircleStateV1,
        scale: u64,
        crs: Option<String>,
        metres: bool,
        geographic: bool,
    },
    Local {
        disk: CircleStateV1,
        crs: String,
        working_crs: String,
        custom: bool,
        projected_pole: Option<[u64; 2]>,
    },
    Ellipsoidal {
        center: PointStateV1,
        radius: u64,
        crs: String,
    },
}

impl From<&FramedCircle> for FramedCircleStateV1 {
    fn from(value: &FramedCircle) -> Self {
        match value {
            FramedCircle::Planar {
                disk,
                scale,
                crs,
                metres,
                geographic,
            } => Self::Planar {
                disk: disk.into(),
                scale: scale.to_bits(),
                crs: crs.as_ref().map(ToString::to_string),
                metres: *metres,
                geographic: geographic.is_some(),
            },
            FramedCircle::Local { disk, local, frame } => Self::Local {
                disk: disk.into(),
                crs: (*frame.crs).clone(),
                working_crs: local.definition.clone(),
                custom: local.display_name.is_some(),
                projected_pole: local
                    .projected_pole
                    .map(|(x, y)| [x.to_bits(), y.to_bits()]),
            },
            FramedCircle::Ellipsoidal {
                center,
                radius,
                frame,
            } => Self::Ellipsoidal {
                center: (*center).into(),
                radius: radius.to_bits(),
                crs: (*frame.crs).clone(),
            },
        }
    }
}

impl GeographicCircleFrame {
    fn restore(crs: String) -> Result<Self> {
        let state = crate::crs::runtime_state_snapshot()?;
        let geographic = crate::crs::info_with_state(&crs, &state)?
            .geographic_frame()
            .ok_or_else(|| crate::crs::CrsError::invalid("circle requires a geographic CRS"))?;
        Ok(Self {
            crs: std::sync::Arc::new(crs),
            geographic,
            state,
        })
    }
}

impl FramedCircleStateV1 {
    pub(crate) fn restore(self) -> Result<FramedCircle> {
        Ok(match self {
            Self::Planar {
                disk,
                scale,
                crs,
                metres,
                geographic,
            } => {
                let geographic = if geographic {
                    Some(
                        GeographicCircleFrame::restore(crs.clone().ok_or_else(|| {
                            crate::crs::CrsError::invalid("geographic circle requires a CRS")
                        })?)?
                        .geographic,
                    )
                } else {
                    None
                };
                FramedCircle::Planar {
                    disk: disk.restore()?,
                    scale: f64::from_bits(scale),
                    crs: crs.map(crate::Crs::from),
                    metres,
                    geographic,
                }
            },
            Self::Local {
                disk,
                crs,
                working_crs,
                custom,
                projected_pole,
            } => FramedCircle::Local {
                disk: disk.restore()?,
                local: LocalCrsDefinition {
                    definition: working_crs,
                    display_name: custom.then_some(super::super::frame::LOCAL_CRS_DISPLAY_NAME),
                    projected_pole: projected_pole
                        .map(|[x, y]| (f64::from_bits(x), f64::from_bits(y))),
                },
                frame: GeographicCircleFrame::restore(crs)?,
            },
            Self::Ellipsoidal {
                center,
                radius,
                crs,
            } => FramedCircle::Ellipsoidal {
                center: center.into_point(),
                radius: f64::from_bits(radius),
                frame: GeographicCircleFrame::restore(crs)?,
            },
        })
    }
}
