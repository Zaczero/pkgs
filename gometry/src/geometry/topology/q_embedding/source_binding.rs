use super::*;

pub(in crate::geometry::topology::q_embedding) fn q_source_edge(
    topology: &ExactGeographicTopology,
    edge: SourceEdgeId,
) -> Result<&ExactSourceEdge> {
    let source = topology.sources.edges.get(edge.0 as usize).ok_or_else(|| {
        GeometryErrorKind::Invalid("source literal names an unknown source edge".into())
    })?;
    if source.id != edge {
        return Err(GeometryErrorKind::Invalid(
            "source literal edge ID does not match its source row".into(),
        )
        .into());
    }
    Ok(source)
}
pub(in crate::geometry::topology::q_embedding) fn q_source_image_port(
    topology: &ExactGeographicTopology,
    image: QSourceImage,
) -> Result<QPortId> {
    let port = match image {
        QSourceImage::Port(port) => return Ok(port),
        QSourceImage::SourcePoint(index) => {
            topology.q.source_point_ports.get(index as usize).copied()
        },
        QSourceImage::SourceRingStart(edge) | QSourceImage::CollapsedStart(edge) => {
            let _ = q_source_edge(topology, edge)?;
            topology
                .q
                .source_edge_ports
                .get(edge.0 as usize)
                .and_then(|ports| ports.first())
                .copied()
        },
        QSourceImage::SourceRingEnd(edge) | QSourceImage::CollapsedEnd(edge) => {
            let _ = q_source_edge(topology, edge)?;
            topology
                .q
                .source_edge_ports
                .get(edge.0 as usize)
                .and_then(|ports| ports.get(1))
                .copied()
        },
        QSourceImage::PublicationSample(_) => None,
    };
    port.ok_or_else(|| {
        GeometryErrorKind::Invalid("source literal has no admitted Q port".into()).into()
    })
}
pub(in crate::geometry::topology::q_embedding) fn q_source_image_xy_bits(
    topology: &ExactGeographicTopology,
    image: QSourceImage,
) -> Result<Option<[u64; 2]>> {
    let record = match image {
        QSourceImage::SourcePoint(index) => topology
            .sources
            .points
            .get(index as usize)
            .map(|source| source.point),
        QSourceImage::SourceRingStart(edge) | QSourceImage::CollapsedStart(edge) => Some(
            q_source_edge(topology, edge)?
                .source
                .first()
                .copied()
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("source edge has no retained start point".into())
                })?,
        ),
        QSourceImage::SourceRingEnd(edge) | QSourceImage::CollapsedEnd(edge) => Some(
            q_source_edge(topology, edge)?
                .source
                .get(1)
                .copied()
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("source edge has no retained end point".into())
                })?,
        ),
        QSourceImage::Port(_) | QSourceImage::PublicationSample(_) => return Ok(None),
    };
    let record = record.ok_or_else(|| {
        GeometryErrorKind::Invalid("source literal has no retained source record".into())
    })?;
    Ok(Some([record.x.to_bits(), record.y.to_bits()]))
}
pub(in crate::geometry::topology::q_embedding) struct QSourceImages<'a> {
    pub(in crate::geometry::topology::q_embedding) topology: &'a ExactGeographicTopology,
    pub(in crate::geometry::topology::q_embedding) source_ports:
        Option<&'a [Option<ExactChartPoint>]>,
    pub(in crate::geometry::topology::q_embedding) publication: &'a [ExactChartPoint],
}
impl QSourceImages<'_> {
    pub(in crate::geometry::topology::q_embedding) fn source_port(
        &self,
        port: QPortId,
    ) -> Result<&ExactChartPoint> {
        if let Some(source_ports) = self.source_ports {
            return source_ports
                .get(port.0 as usize)
                .and_then(Option::as_ref)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "source literal port has no checked assigned image".into(),
                    )
                    .into()
                });
        }
        self.topology
            .q
            .ports
            .get(port.0 as usize)
            .map(|port| &port.point)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("source literal port is unknown".into()).into()
            })
    }

    pub(in crate::geometry::topology::q_embedding) fn image_for_port(
        &self,
        image: QSourceImage,
    ) -> Result<&ExactChartPoint> {
        self.source_port(q_source_image_port(self.topology, image)?)
    }
}
impl CheckedImages<QSourceImage> for QSourceImages<'_> {
    fn image(
        &self,
        _address: super::super::super::super::embedding_completion::LiteralAddress,
        image: QSourceImage,
    ) -> Result<&ExactChartPoint> {
        match image {
            QSourceImage::Port(_)
            | QSourceImage::SourcePoint(_)
            | QSourceImage::SourceRingStart(_)
            | QSourceImage::SourceRingEnd(_)
            | QSourceImage::CollapsedStart(_)
            | QSourceImage::CollapsedEnd(_) => self.image_for_port(image),
            QSourceImage::PublicationSample(sample) => {
                self.publication.get(sample as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "source publication literal has no checked assigned image".into(),
                    )
                    .into()
                })
            },
        }
    }
}
pub(in crate::geometry::topology::q_embedding) struct AssignedSourceChart<'a> {
    pub(in crate::geometry::topology::q_embedding) topology: &'a ExactGeographicTopology,
    pub(in crate::geometry::topology::q_embedding) source_ports:
        Option<&'a [Option<ExactChartPoint>]>,
    pub(in crate::geometry::topology::q_embedding) source_ordinates: &'a [NodeOrdinates],
    pub(in crate::geometry::topology::q_embedding) publication_points: &'a [XY],
    pub(in crate::geometry::topology::q_embedding) publication_ordinates: &'a [NodeOrdinates],
}
impl LiteralChart<QSourceImage> for AssignedSourceChart<'_> {
    fn read_point<W: Work>(
        &mut self,
        _address: super::super::super::super::embedding_completion::LiteralAddress,
        image: QSourceImage,
        record: Point,
        _expected_axes: crate::geometry::CoordinateAxes,
        stored_axes: crate::geometry::CoordinateAxes,
        work: &mut W,
    ) -> Result<Option<ExactChartPoint>> {
        match image {
            image @ (QSourceImage::Port(_)
            | QSourceImage::SourcePoint(_)
            | QSourceImage::SourceRingStart(_)
            | QSourceImage::SourceRingEnd(_)
            | QSourceImage::CollapsedStart(_)
            | QSourceImage::CollapsedEnd(_)) => {
                let source_record_authority = !matches!(image, QSourceImage::Port(_));
                if let Some([expected_x, expected_y]) =
                    q_source_image_xy_bits(self.topology, image)?
                {
                    work.charge_into_error(1)?;
                    if record.x.to_bits() != expected_x || record.y.to_bits() != expected_y {
                        return Ok(None);
                    }
                }
                let chart_record_y = if source_record_authority {
                    Latitude::from_admitted(self.topology.sources.frame, record.y).get()
                } else {
                    record.y
                };
                let port_id = q_source_image_port(self.topology, image)?;
                let port = self
                    .topology
                    .q
                    .ports
                    .get(port_id.0 as usize)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid("source literal names an unknown Q port".into())
                    })?;
                let expected = if let Some(source_ports) = self.source_ports {
                    let Some(expected) = source_ports
                        .get(port_id.0 as usize)
                        .and_then(Option::as_ref)
                    else {
                        return Ok(None);
                    };
                    expected
                } else {
                    &port.point
                };
                let source_location = if port.cut && self.source_ports.is_some() {
                    let rounded = rounded_chart_point_with_work(expected, work)?;
                    if record.x.partial_cmp(&rounded.x) != Some(Ordering::Equal)
                        || chart_record_y.partial_cmp(&rounded.y) != Some(Ordering::Equal)
                    {
                        return Ok(None);
                    }
                    ExactChartPoint::from_rationals(
                        ExactRational::from_f64(record.x),
                        ExactRational::from_f64(record.y),
                    )
                } else if matches!(
                    quotient_key_for_chart_position_with_work(
                        self.topology.sources.frame,
                        &port.point,
                        work,
                    )?,
                    ExactQuotientKey::NorthPole | ExactQuotientKey::SouthPole
                ) {
                    if !exact_coordinate_matches_with_work(&expected.y, chart_record_y, work)?
                        || (!source_record_authority
                            && record.x.partial_cmp(&port.point.x.round_with_work(work)?)
                                != Some(Ordering::Equal))
                    {
                        return Ok(None);
                    }
                    expected.try_clone_with_work(work)?
                } else {
                    if !exact_coordinate_matches_with_work(&expected.y, chart_record_y, work)? {
                        return Ok(None);
                    }
                    let period =
                        ExactRational::from_f64(self.topology.sources.frame.longitude().period());
                    let sheet = expected
                        .x
                        .try_subtract_with_work(&ExactRational::from_f64(record.x), work)?
                        .try_quotient_with_work(&period, work)?;
                    if !sheet.is_integer() {
                        return Ok(None);
                    }
                    expected.try_clone_with_work(work)?
                };
                if self.source_ordinates.get(port.node.0 as usize).is_none() {
                    return Err(GeometryErrorKind::Invalid(
                        "source literal port has no resolved ordinate row".into(),
                    )
                    .into());
                }
                let expected_record = super::super::overlay::with_vertex_axes(
                    EmittedVertex {
                        port,
                        point: record,
                        literal_image: image,
                    },
                    self.source_ordinates,
                    stored_axes,
                )?;
                work.charge_into_error(1)?;
                if expected_record != record {
                    return Ok(None);
                }
                Ok(Some(source_location))
            },
            QSourceImage::PublicationSample(sample) => {
                let index = sample as usize;
                let point = *self.publication_points.get(index).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "source publication slot has no assigned point".into(),
                    )
                })?;
                let ordinates = *self.publication_ordinates.get(index).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "source publication slot has no resolved ordinates".into(),
                    )
                })?;
                if record.x.partial_cmp(&point.x) != Some(Ordering::Equal)
                    || record.y.partial_cmp(&point.y) != Some(Ordering::Equal)
                {
                    return Ok(None);
                }
                let expected =
                    super::super::overlay::with_publication_axes(point, ordinates, stored_axes)?;
                work.charge_into_error(1)?;
                if expected != record {
                    return Ok(None);
                }
                Ok(Some(ExactChartPoint::from_rationals(
                    ExactRational::from_f64(record.x),
                    ExactRational::from_f64(record.y),
                )))
            },
        }
    }
}
pub(in crate::geometry::topology::q_embedding) struct SourceOnlyReceipt<'a> {
    pub(in crate::geometry::topology::q_embedding) images: QSourceImages<'a>,
    pub(in crate::geometry::topology::q_embedding) chart: AssignedSourceChart<'a>,
    pub(in crate::geometry::topology::q_embedding) publication: Option<&'a PublicationInventory>,
}
impl CheckedImages<QSourceImage> for SourceOnlyReceipt<'_> {
    fn image(
        &self,
        address: super::super::super::super::embedding_completion::LiteralAddress,
        image: QSourceImage,
    ) -> Result<&ExactChartPoint> {
        if let QSourceImage::PublicationSample(sample) = image
            && self
                .publication
                .and_then(|publication| publication.samples.get(sample as usize))
                .is_none()
        {
            return Err(GeometryErrorKind::Invalid(
                "source-only literal sample is outside its publication context".into(),
            )
            .into());
        }
        self.images.image(address, image)
    }
}
impl LiteralChart<QSourceImage> for SourceOnlyReceipt<'_> {
    fn read_point<W: Work>(
        &mut self,
        address: super::super::super::super::embedding_completion::LiteralAddress,
        image: QSourceImage,
        record: Point,
        expected_axes: crate::geometry::CoordinateAxes,
        stored_axes: crate::geometry::CoordinateAxes,
        work: &mut W,
    ) -> Result<Option<ExactChartPoint>> {
        self.chart
            .read_point(address, image, record, expected_axes, stored_axes, work)
    }
}
#[cfg(test)]
pub(in crate::geometry::topology::q_embedding) fn bind_source_only(
    source: SourceFrozenDrawing,
    topology: &ExactGeographicTopology,
    source_ports: Option<&[Option<ExactChartPoint>]>,
    source_ordinates: &[NodeOrdinates],
    publication: Option<&PublicationInventory>,
    publication_images: &[ExactChartPoint],
    publication_points: &[XY],
    publication_ordinates: &[NodeOrdinates],
    work: &mut impl Work,
) -> Result<LiteralCheck<QSourceImage>> {
    if publication_images.len() != publication_points.len()
        || publication_images.len() != publication_ordinates.len()
        || publication
            .is_some_and(|publication| publication.samples.len() != publication_images.len())
        || (publication.is_none() && !publication_images.is_empty())
    {
        return Err(GeometryErrorKind::Invalid(
            "source-only publication images do not match their operation context".into(),
        )
        .into());
    }
    let mut receipt = SourceOnlyReceipt {
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
    bind_literals(source.buffers, &mut receipt, work)
}
