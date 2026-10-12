//! Receipt admission for Q's checked Native and SourceOnly publications.

use super::{NativeCandidateAssignment, FrozenLiteralBuffers, EmissionPortId, AssignedSite, NodeOrdinates, Point, SealedOutput, CheckedImages, Result, ExactChartPoint, GeometryErrorKind, LiteralChart, Work, validate_native_materialized_port, ExactRational, StorageTarget, bind_literals, LiteralCheck, validate_native_sealed_layout, SourceFrozenDrawing, ExactGeographicTopology, PublicationInventory, XY, QSourceImage, SourceOnlyReceipt, QSourceImages, AssignedSourceChart};

pub(in crate::geometry) struct NativeCandidatePublication<'call, 'reference, 'topology> {
    pub(super) assignment: &'call NativeCandidateAssignment<'reference, 'topology>,
    pub(super) buffers: FrozenLiteralBuffers<EmissionPortId>,
    pub(super) sites: &'call [AssignedSite],
    pub(super) ordinates: &'call [NodeOrdinates],
    pub(super) port_records: &'call mut [Option<Point>],
}

pub(in crate::geometry) enum NativePublicationCheck {
    Pass(SealedOutput<EmissionPortId>),
    Reject,
}

struct NativeLiteralReceiver<'assignment, 'reference, 'topology> {
    assignment: &'assignment NativeCandidateAssignment<'reference, 'topology>,
    sites: &'assignment [AssignedSite],
    ordinates: &'assignment [NodeOrdinates],
    port_records: &'assignment mut [Option<Point>],
}

impl CheckedImages<EmissionPortId> for NativeLiteralReceiver<'_, '_, '_> {
    fn image(
        &self,
        _address: crate::geometry::embedding_completion::LiteralAddress,
        image: EmissionPortId,
    ) -> Result<&ExactChartPoint> {
        let vertex = *self
            .assignment
            .reference
            .port_vertices
            .get(image.0 as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("native literal image has no graph vertex".into())
            })?;
        self.assignment
            .points()
            .exact
            .get(vertex as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "native literal image vertex is outside the graph".into(),
                )
                .into()
            })
    }
}

impl LiteralChart<EmissionPortId> for NativeLiteralReceiver<'_, '_, '_> {
    fn read_point<W: Work>(
        &mut self,
        _address: crate::geometry::embedding_completion::LiteralAddress,
        image: EmissionPortId,
        record: Point,
        _expected_axes: crate::geometry::CoordinateAxes,
        stored_axes: crate::geometry::CoordinateAxes,
        work: &mut W,
    ) -> Result<Option<ExactChartPoint>> {
        if !validate_native_materialized_port(
            self.assignment.reference,
            image,
            record,
            self.sites,
            self.ordinates,
            stored_axes,
            self.port_records,
            &self.assignment.points().exact,
            work,
        )? {
            return Ok(None);
        }
        Ok(Some(ExactChartPoint::from_rationals(
            ExactRational::from_f64(record.x),
            ExactRational::from_f64(record.y),
        )))
    }
}

pub(in crate::geometry) fn bind_native_publication<W: Work>(
    mut receipt: NativeCandidatePublication<'_, '_, '_>,
    target: StorageTarget,
    work: &mut W,
) -> Result<NativePublicationCheck> {
    receipt.buffers = receipt.buffers.admit_target(target)?;
    let NativeCandidatePublication {
        assignment,
        buffers,
        sites,
        ordinates,
        port_records,
    } = receipt;
    let mut receiver = NativeLiteralReceiver {
        assignment,
        sites,
        ordinates,
        port_records,
    };
    let sealed = match bind_literals(buffers, &mut receiver, work)? {
        LiteralCheck::Pass(sealed) => sealed,
        LiteralCheck::Reject(_) => return Ok(NativePublicationCheck::Reject),
    };
    let layout = sealed.shape_layout().ok_or_else(|| {
        GeometryErrorKind::Invalid("native Q candidate did not bind scalar final storage".into())
    })?;
    if !validate_native_sealed_layout(
        assignment.reference,
        layout,
        receiver.port_records,
        assignment.points(),
        work,
    )? {
        return Ok(NativePublicationCheck::Reject);
    }
    drop(receiver);
    Ok(NativePublicationCheck::Pass(sealed))
}

pub(in crate::geometry) struct SourceOnlyPublication<'a> {
    pub(super) source: SourceFrozenDrawing,
    pub(super) topology: &'a ExactGeographicTopology,
    pub(super) source_ports: Option<&'a [Option<ExactChartPoint>]>,
    pub(super) source_ordinates: &'a [NodeOrdinates],
    pub(super) publication: Option<&'a PublicationInventory>,
    pub(super) publication_images: &'a [ExactChartPoint],
    pub(super) publication_points: &'a [XY],
    pub(super) publication_ordinates: &'a [NodeOrdinates],
}

impl<'a> SourceOnlyPublication<'a> {
    pub(in crate::geometry::topology) const fn new(
        source: SourceFrozenDrawing,
        topology: &'a ExactGeographicTopology,
        source_ports: Option<&'a [Option<ExactChartPoint>]>,
        source_ordinates: &'a [NodeOrdinates],
        publication: Option<&'a PublicationInventory>,
        publication_images: &'a [ExactChartPoint],
        publication_points: &'a [XY],
        publication_ordinates: &'a [NodeOrdinates],
    ) -> Self {
        Self {
            source,
            topology,
            source_ports,
            source_ordinates,
            publication,
            publication_images,
            publication_points,
            publication_ordinates,
        }
    }
}

pub(in crate::geometry) enum SourcePublicationCheck {
    Pass(SealedOutput<QSourceImage>),
    Reject(Vec<QSourceImage>),
}

pub(in crate::geometry) fn bind_source_publication<W: Work>(
    mut receipt: SourceOnlyPublication<'_>,
    target: StorageTarget,
    work: &mut W,
) -> Result<SourcePublicationCheck> {
    receipt.source.buffers = receipt.source.buffers.admit_target(target)?;
    let SourceOnlyPublication {
        source,
        topology,
        source_ports,
        source_ordinates,
        publication,
        publication_images,
        publication_points,
        publication_ordinates,
    } = receipt;
    let mut receiver = SourceOnlyReceipt {
        images: QSourceImages {
            topology,
            source_ports,
            publication: publication_images,
        },
        chart: AssignedSourceChart {
            topology,
            source_ports,
            source_ordinates,
            publication_points,
            publication_ordinates,
        },
        publication,
    };
    Ok(match bind_literals(source.buffers, &mut receiver, work)? {
        LiteralCheck::Pass(sealed) => SourcePublicationCheck::Pass(sealed),
        LiteralCheck::Reject(images) => SourcePublicationCheck::Reject(images),
    })
}
