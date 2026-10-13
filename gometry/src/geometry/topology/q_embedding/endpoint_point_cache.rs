use super::super::emission::sites::QCandidate;
use super::{ExactChartPoint, QPortId};
use crate::collections::HashMap;
use crate::error::Result;
use crate::geometry::Work;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(super) struct NativeEndpointPointKey {
    port: QPortId,
    native_x_bits: u64,
    native_y_bits: u64,
    periods: i64,
}

impl NativeEndpointPointKey {
    pub(super) const fn new(port: QPortId, candidate: QCandidate) -> Self {
        Self {
            port,
            native_x_bits: candidate.native.x.to_bits(),
            native_y_bits: candidate.native.y.to_bits(),
            periods: candidate.periods,
        }
    }
}

pub(super) struct NativeEndpointPointCache {
    points: HashMap<NativeEndpointPointKey, Option<ExactChartPoint>>,
}

impl NativeEndpointPointCache {
    pub(super) const fn new(hash_state: ahash::RandomState) -> Self {
        Self {
            points: HashMap::with_hasher(hash_state),
        }
    }

    pub(super) fn get_or_compute<'a, W, F>(
        &'a mut self,
        key: NativeEndpointPointKey,
        budget: &mut W,
        compute: F,
    ) -> Result<Option<&'a ExactChartPoint>>
    where
        W: Work,
        F: FnOnce(&mut W) -> Result<Option<ExactChartPoint>>,
    {
        budget.charge_into_error(1)?;
        match self.points.entry(key) {
            std::collections::hash_map::Entry::Occupied(entry) => Ok(entry.into_mut().as_ref()),
            std::collections::hash_map::Entry::Vacant(entry) => {
                let point = compute(budget)?;
                Ok(entry.insert(point).as_ref())
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::{ExactChartPoint, ExactRational, QPortId};
    use super::{NativeEndpointPointCache, NativeEndpointPointKey, QCandidate};
    use crate::error::{ResourceError, Result};
    use crate::geometry::{Work, XY};

    struct CountingWork(u64);

    impl Work for CountingWork {
        type Error = crate::error::Error;

        fn charge(&mut self, ticks: u64) -> Result<()> {
            self.0 = self
                .0
                .checked_add(ticks)
                .ok_or(ResourceError::SizeOverflow)?;
            Ok(())
        }

        fn charge_product(&mut self, count: usize, ticks_per_item: usize) -> Result<()> {
            let ticks = count
                .checked_mul(ticks_per_item)
                .ok_or(ResourceError::SizeOverflow)?;
            self.charge(u64::try_from(ticks).map_err(|_| ResourceError::SizeOverflow)?)
        }
    }

    #[test]
    fn repeated_candidate_identity_reuses_admitted_and_rejected_points() {
        let mut cache = NativeEndpointPointCache::new(ahash::RandomState::with_seeds(1, 2, 3, 4));
        let candidate = QCandidate {
            native: XY::new(12.5, 3.25),
            periods: 2,
        };
        let key = NativeEndpointPointKey::new(QPortId(7), candidate);
        let mut work = CountingWork(0);
        let mut computations = 0;
        let expected = ExactChartPoint::from_rationals(
            ExactRational::from_f64(12.5),
            ExactRational::from_f64(3.25),
        );

        let admitted = cache
            .get_or_compute(key, &mut work, |work| {
                computations += 1;
                work.charge_into_error(3)?;
                Ok(Some(expected.clone()))
            })
            .expect("first admitted point lookup should succeed");
        assert_eq!(admitted, Some(&expected));
        let admitted_again = cache
            .get_or_compute(key, &mut work, |work| {
                computations += 1;
                work.charge_into_error(3)?;
                Ok(Some(expected.clone()))
            })
            .expect("repeated admitted point lookup should succeed");
        assert_eq!(admitted_again, Some(&expected));

        let rejected_key = NativeEndpointPointKey { periods: 3, ..key };
        let rejected = cache
            .get_or_compute(rejected_key, &mut work, |work| {
                computations += 1;
                work.charge_into_error(2)?;
                Ok(None)
            })
            .expect("first rejected point lookup should succeed");
        assert_eq!(rejected, None);
        let rejected_again = cache
            .get_or_compute(rejected_key, &mut work, |work| {
                computations += 1;
                work.charge_into_error(2)?;
                Ok(None)
            })
            .expect("repeated rejected point lookup should succeed");
        assert_eq!(rejected_again, None);

        assert_eq!(computations, 2);
        assert_eq!(work.0, 9);
    }
}
