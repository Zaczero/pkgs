use std::borrow::Cow;
use std::cmp::Ordering;

use crate::geometry::Result;
use crate::geometry::tessellation::exact::{ExactDyadic, ExactRational};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(in crate::geometry) enum GridCoordinate {
    Small(i64),
    Large(Box<ExactDyadic>),
}

pub(in crate::geometry) type GridPoint = [GridCoordinate; 2];

impl GridCoordinate {
    pub(super) fn from_exact(value: ExactDyadic) -> Self {
        let limbs = value.limbs.as_slice();
        if limbs.is_empty() {
            return Self::Small(0);
        }
        if let [limb] = limbs
            && (0..64).contains(&value.exponent)
        {
            let magnitude = u128::from(*limb) << value.exponent;
            if value.negative && magnitude == 1_u128 << 63 {
                return Self::Small(i64::MIN);
            }
            if let Ok(magnitude) = i64::try_from(magnitude) {
                return Self::Small(if value.negative {
                    magnitude.wrapping_neg()
                } else {
                    magnitude
                });
            }
        }
        Self::Large(Box::new(value))
    }

    pub(in crate::geometry) fn exact(&self) -> Cow<'_, ExactDyadic> {
        match self {
            Self::Small(value) => Cow::Owned(ExactDyadic::from_i64(*value)),
            Self::Large(value) => Cow::Borrowed(value),
        }
    }

    pub(super) fn rational(&self) -> ExactRational {
        ExactRational::from_dyadic(self.exact().into_owned())
    }

    pub(in crate::geometry) fn rounded(&self) -> Result<f64> {
        match self {
            Self::Small(value) => Ok(*value as f64),
            Self::Large(_) => {
                let value = self.rational();
                if value.try_cmp(&ExactRational::from_f64(f64::MAX))?.is_gt() {
                    Ok(f64::MAX)
                } else if value.try_cmp(&ExactRational::from_f64(-f64::MAX))?.is_lt() {
                    Ok(-f64::MAX)
                } else {
                    value.round()
                }
            },
        }
    }

    pub(super) fn exact_f64(&self) -> Option<f64> {
        self.exact().to_f64_exact_scaled(0)
    }
}

impl Ord for GridCoordinate {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::Small(left), Self::Small(right)) => left.cmp(right),
            _ => self.exact().as_ref().cmp(other.exact().as_ref()),
        }
    }
}

impl PartialOrd for GridCoordinate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
