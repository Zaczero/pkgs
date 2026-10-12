//! Private contracts shared by geographic producer receipts and their sink.

use crate::Frame;
use crate::error::Result;
use crate::geographic::ChartPole;
use crate::geometry::embedding_completion::{FinalGroupRows, LiteralStorage, SealedOutput};
use crate::geometry::storage::ArrayRows;
use crate::geometry::topology::QSourceImage;
use crate::geometry::topology::q_embedding::{
    NativeCandidatePublication, NativePublicationCheck, SourceOnlyPublication,
    SourcePublicationCheck,
};
use crate::geometry::{GeometryErrorKind, Shape, Work};

/// Which copy of the longitude cut a synthetic chart vertex occupies.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) enum ChartCutSide {
    East,
    West,
}

/// A generated edge that restricts publication to a chart boundary.
///
/// Authentic source intervals remain in producer source-use receipts; these
/// roles describe only generated presentation geometry.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum ChartRestriction {
    CutChord,
    PolarCap { pole: ChartPole },
}

/// Synthetic vertices introduced to close a chart-cut or polar boundary.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum SyntheticChartVertex {
    Cut {
        /// Producer-local identity; the side retains the quotient spelling.
        node: u32,
        side: ChartCutSide,
    },
    Pole {
        /// Producer-local pole node identity.
        node: u32,
        pole: ChartPole,
        /// Pole vertices share a quotient node but retain their native spelling.
        native_longitude_bits: u64,
    },
}

/// One directed generated chart edge; `start` to `end` is its published use.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DirectedChartRestrictionUse {
    pub(crate) restriction: ChartRestriction,
    pub(crate) start: SyntheticChartVertex,
    pub(crate) end: SyntheticChartVertex,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[expect(
    dead_code,
    reason = "array and Groups producer receipts select these final storage targets"
)]
pub(in crate::geometry) enum StorageTarget {
    Scalar,
    Array,
    Groups,
}

pub(in crate::geometry) enum ProducerReceipt<'call, 'reference, 'topology> {
    QNative(NativeCandidatePublication<'call, 'reference, 'topology>),
    QSourceOnly(SourceOnlyPublication<'call>),
}

pub(in crate::geometry) struct PublicationCall<'call, 'reference, 'topology, W: Work> {
    receipt: ProducerReceipt<'call, 'reference, 'topology>,
    frame: Frame,
    work: &'call mut W,
}

impl<'call, 'reference, 'topology, W: Work> PublicationCall<'call, 'reference, 'topology, W> {
    pub(in crate::geometry) fn new(
        receipt: ProducerReceipt<'call, 'reference, 'topology>,
        frame: Frame,
        work: &'call mut W,
    ) -> Self {
        Self {
            receipt,
            frame,
            work,
        }
    }
}

pub(crate) struct FinishedGeometry {
    storage: LiteralStorage,
    frame: Frame,
}

impl FinishedGeometry {
    fn from_sealed<I>(sealed: SealedOutput<I>, frame: Frame) -> Self {
        Self {
            storage: sealed.into_storage(),
            frame,
        }
    }

    pub(crate) fn into_scalar(self) -> Result<(Shape, Frame)> {
        match self.storage {
            LiteralStorage::Scalar(shape) => Ok((shape, self.frame)),
            LiteralStorage::Rows(_) | LiteralStorage::Groups(_) => Err(GeometryErrorKind::Invalid(
                "non-scalar finished geometry cannot be adopted as a scalar".into(),
            )
            .into()),
        }
    }

    #[expect(
        dead_code,
        reason = "array producers adopt already-admitted rows through this sink result"
    )]
    pub(crate) fn into_array(self) -> Result<(ArrayRows, Frame)> {
        match self.storage {
            LiteralStorage::Rows(rows) => Ok((rows, self.frame)),
            LiteralStorage::Scalar(_) | LiteralStorage::Groups(_) => {
                Err(GeometryErrorKind::Invalid(
                    "finished geometry does not own admitted array rows".into(),
                )
                .into())
            },
        }
    }

    #[expect(
        dead_code,
        reason = "grouped producers adopt their sealed CSR owner through this sink result"
    )]
    pub(crate) fn into_groups(self) -> Result<(FinalGroupRows, Frame)> {
        match self.storage {
            LiteralStorage::Groups(groups) => Ok((groups, self.frame)),
            LiteralStorage::Scalar(_) | LiteralStorage::Rows(_) => Err(GeometryErrorKind::Invalid(
                "finished geometry does not own admitted Groups rows".into(),
            )
            .into()),
        }
    }
}

pub(in crate::geometry) enum PublicationRejection {
    Native,
    SourceOnly(Vec<QSourceImage>),
}

pub(in crate::geometry) enum PublicationOutcome {
    Pass(FinishedGeometry),
    Reject(PublicationRejection),
}

pub(in crate::geometry) fn finish_geographic<W: Work>(
    call: PublicationCall<'_, '_, '_, W>,
    target: StorageTarget,
) -> Result<PublicationOutcome> {
    let PublicationCall {
        receipt,
        frame,
        work,
    } = call;
    match receipt {
        ProducerReceipt::QNative(receipt) => {
            match crate::geometry::topology::q_embedding::bind_native_publication(
                receipt, target, work,
            )? {
                NativePublicationCheck::Pass(sealed) => Ok(PublicationOutcome::Pass(
                    FinishedGeometry::from_sealed(sealed, frame),
                )),
                NativePublicationCheck::Reject => {
                    Ok(PublicationOutcome::Reject(PublicationRejection::Native))
                },
            }
        },
        ProducerReceipt::QSourceOnly(receipt) => {
            match crate::geometry::topology::q_embedding::bind_source_publication(
                receipt, target, work,
            )? {
                SourcePublicationCheck::Pass(sealed) => Ok(PublicationOutcome::Pass(
                    FinishedGeometry::from_sealed(sealed, frame),
                )),
                SourcePublicationCheck::Reject(images) => Ok(PublicationOutcome::Reject(
                    PublicationRejection::SourceOnly(images),
                )),
            }
        },
    }
}
