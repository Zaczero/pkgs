use std::collections::BTreeMap;

use super::receipt::{CheckedPublicationLiteral, ProducerLiteral, PublicationImage};
use crate::error::Result;
use crate::geometry::embedding_completion::literals::LiteralAddress;
use crate::geometry::embedding_completion::storage::{CheckedImages, LiteralChart};
use crate::geometry::topology::ExactChartPoint;
use crate::geometry::{CoordinateAxes, GeometryErrorKind, Point, Work};

pub(super) struct CheckedLiteralSnapshot {
    records: BTreeMap<(LiteralAddress, PublicationImage), (Point, ExactChartPoint)>,
}

impl CheckedLiteralSnapshot {
    pub(super) fn new(
        records: impl IntoIterator<Item = CheckedPublicationLiteral>,
    ) -> Result<Self> {
        let mut snapshot =
            BTreeMap::<(LiteralAddress, PublicationImage), (Point, ExactChartPoint)>::new();
        for literal in records {
            let key = (literal.address, literal.image);
            if let Some((record, chart)) = snapshot.get(&key)
                && (*record != literal.record || *chart != literal.chart)
            {
                return Err(GeometryErrorKind::Invalid(
                    "checked publication occurrences disagree on one literal identity".into(),
                )
                .into());
            }
            snapshot.insert(key, (literal.record, literal.chart));
        }
        Ok(Self { records: snapshot })
    }

    pub(super) fn contains(&self, address: LiteralAddress, image: PublicationImage) -> bool {
        self.records.contains_key(&(address, image))
    }

    pub(super) fn addresses_for_image(&self, image: PublicationImage) -> Vec<LiteralAddress> {
        self.records
            .keys()
            .filter_map(|(address, candidate)| (*candidate == image).then_some(*address))
            .collect()
    }

    pub(super) fn into_producer_literals(self) -> Result<Vec<ProducerLiteral>> {
        let mut literals = Vec::with_capacity(self.records.len());
        for ((address, image), (record, chart)) in self.records {
            let PublicationImage::ProducerDrawn(image) = image else {
                return Err(GeometryErrorKind::Invalid(
                    "producer receiver contains a different image family".into(),
                )
                .into());
            };
            literals.push(ProducerLiteral {
                address,
                image,
                record,
                chart,
            });
        }
        Ok(literals)
    }
}

impl CheckedImages<PublicationImage> for CheckedLiteralSnapshot {
    fn image(&self, address: LiteralAddress, image: PublicationImage) -> Result<&ExactChartPoint> {
        self.records
            .get(&(address, image))
            .map(|(_, chart)| chart)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "final storage literal has no checked publication identity".into(),
                )
                .into()
            })
    }
}

impl LiteralChart<PublicationImage> for CheckedLiteralSnapshot {
    fn read_point<W: Work>(
        &mut self,
        address: LiteralAddress,
        image: PublicationImage,
        record: Point,
        expected_axes: CoordinateAxes,
        stored_axes: CoordinateAxes,
        _work: &mut W,
    ) -> Result<Option<ExactChartPoint>> {
        let Some((expected, chart)) = self.records.get(&(address, image)) else {
            return Ok(None);
        };
        if expected_axes != stored_axes || record != *expected {
            return Ok(None);
        }
        Ok(Some(chart.clone()))
    }
}
