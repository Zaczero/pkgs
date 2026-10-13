use super::super::*;

impl<'topology, 'emission, 'access, 'metric, M: ChartMetric>
    QEmbeddingProblem<'topology, 'emission, 'access, 'metric, M>
{
    fn complete_native_drawing(
        &self,
        reference: &NativeReference<'topology>,
        features: &[NativeCutFeature],
        feature_candidates: &[QCandidate],
        budget: &mut EvaluationBudget,
    ) -> Result<Option<super::super::emission::CompletedNativeDrawing>> {
        if reference.feature_nodes.len() != feature_candidates.len()
            || features.len() != feature_candidates.len()
        {
            return Err(GeometryErrorKind::Invalid(
                "native cut feature nodes and candidates are misaligned".into(),
            )
            .into());
        }
        budget.charge_into_error(
            u64::try_from(reference.sites.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut sites = reference.sites.clone();
        let mut assigned_by_node = vec![None; reference.sites.len()];
        for (&node, &candidate) in reference.feature_nodes.iter().zip(feature_candidates) {
            budget.charge_into_error(1)?;
            let slot = assigned_by_node.get_mut(node.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("native cut node is outside its reference".into())
            })?;
            if slot.is_some_and(|previous| previous != candidate) {
                return Err(GeometryErrorKind::Invalid(
                    "native cut aliases have conflicting assigned sites".into(),
                )
                .into());
            }
            *slot = Some(candidate);
        }
        for (node_index, candidate) in assigned_by_node.into_iter().enumerate() {
            let Some(candidate) = candidate else {
                continue;
            };
            let site = assigned_emission_site(self.topology.sources.frame, candidate)?;
            let reference_site = sites.get_mut(node_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("native cut node site is unknown".into())
            })?;
            if let Some(previous) = reference_site
                && *previous != site
            {
                return Err(GeometryErrorKind::Invalid(
                    "native cut assignment conflicts with its fixed reference node".into(),
                )
                .into());
            }
            *reference_site = Some(site);
        }
        let mut assigned_sites = Vec::with_capacity(sites.len());
        for site in sites {
            budget.charge_into_error(1)?;
            assigned_sites.push(site.ok_or_else(|| {
                GeometryErrorKind::Invalid("native reference node has no assigned site".into())
            })?);
        }
        let (ordinates, axes) = super::super::emission::resolve_emission_ordinates(
            &reference.plan,
            self.emission.publication,
            &self.ordinates,
            self.topology,
            self.declared_axes,
            self.strict_axes,
            self.operation,
            self.metric_access,
            budget,
        )?;
        let parts = super::super::emission::materialize_native_parts(
            &reference.plan,
            &assigned_sites,
            &ordinates,
            axes,
            &reference.line_traces,
            budget,
        )?;
        let Some(mut candidate) = validate_native_materialized_candidate(
            reference,
            &parts,
            &assigned_sites,
            &ordinates,
            budget,
        )?
        else {
            return Ok(None);
        };
        let frozen = self
            .emission
            .plan
            .seal_native_parts(parts, self.declared_axes, budget)?;
        if !std::ptr::eq(candidate.assignment.reference, reference) {
            return Err(GeometryErrorKind::Invalid(
                "native completion assignment belongs to another reference".into(),
            )
            .into());
        }
        let line_traces = frozen.line_traces;
        let receipt = NativeCandidatePublication {
            assignment: &candidate.assignment,
            buffers: frozen.buffers,
            sites: &assigned_sites,
            ordinates: &ordinates,
            port_records: &mut candidate.port_records,
        };
        match finish_geographic(
            PublicationCall::new(
                ProducerReceipt::QNative(receipt),
                self.topology.publication_frame().clone(),
                budget,
            ),
            StorageTarget::Scalar,
        )? {
            PublicationOutcome::Pass(sealed) => {
                Ok(Some(super::super::emission::CompletedNativeDrawing {
                    sealed,
                    line_traces,
                }))
            },
            PublicationOutcome::Reject(PublicationRejection::Native) => Ok(None),
            PublicationOutcome::Reject(
                PublicationRejection::SourceOnly(_)
                | PublicationRejection::Batch(_)
                | PublicationRejection::ProducerDrawing(_),
            ) => Err(GeometryErrorKind::Invalid(
                "native Q finisher returned a non-native rejection".into(),
            )
            .into()),
        }
    }
    fn assigned_publication_chart_points(
        &self,
        producer: &AssignedProducerProjection,
        budget: &mut impl Work,
    ) -> Result<Vec<ExactChartPoint>> {
        self.checked_publication_sample_images(producer, budget)
    }
    fn checked_publication_sample_images(
        &self,
        producer: &AssignedProducerProjection,
        work: &mut impl Work,
    ) -> Result<Vec<ExactChartPoint>> {
        let sample_count = self.emission.publication.samples.len();
        if self.publication_sample_nodes.len() != sample_count
            || self.publication_sample_source_ports.len() != sample_count
            || producer.leaves.len() != self.publication_chords.len()
        {
            return Err(GeometryErrorKind::Invalid(
                "publication sample receipts do not match their checked producer leaves".into(),
            )
            .into());
        }
        work.charge_into_error(
            u64::try_from(sample_count).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut images = vec![None; sample_count];
        for (chord, leaf) in self.publication_chords.iter().zip(&producer.leaves) {
            work.charge_into_error(1)?;
            if chord.recipe != leaf.recipe || chord.trace_edge != leaf.trace_edge {
                return Err(GeometryErrorKind::Invalid(
                    "publication leaf projection lost its recipe/trace-edge identity".into(),
                )
                .into());
            }
            for side in 0..2 {
                work.charge_into_error(1)?;
                let endpoint = chord.endpoints.get(side).ok_or_else(|| {
                    GeometryErrorKind::Invalid("checked publication endpoint is unknown".into())
                })?;
                let Some(sample_index) = publication_sample_index(endpoint) else {
                    continue;
                };
                let sample = self
                    .emission
                    .publication
                    .samples
                    .get(sample_index)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid("checked publication sample is unknown".into())
                    })?;
                let reference = chord.exact.get(side).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "checked publication endpoint lost its exact reference".into(),
                    )
                })?;
                if !reference.try_eq_with_work(&sample.exact_point, work)? {
                    return Err(GeometryErrorKind::Invalid(
                        "checked publication endpoint changed its frozen sample reference".into(),
                    )
                    .into());
                }
                let point = leaf.points.get(side).cloned().ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "checked publication leaf has no endpoint image".into(),
                    )
                })?;
                let slot = images.get_mut(sample_index).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "publication sample maps outside the image table".into(),
                    )
                })?;
                if slot.as_ref().is_some_and(|previous| previous != &point) {
                    return Err(GeometryErrorKind::Invalid(
                        "adjacent checked leaves disagree on one sample image".into(),
                    )
                    .into());
                }
                *slot = Some(point);
            }
        }
        let mut quotient_images = BTreeMap::<EmissionNodeId, ExactQuotientKey>::new();
        let mut checked = Vec::with_capacity(sample_count);
        for (sample_index, image) in images.into_iter().enumerate() {
            work.charge_into_error(1)?;
            let image = image.ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "publication sample has no checked source-port or leaf image".into(),
                )
            })?;
            let node = *self
                .publication_sample_nodes
                .get(sample_index)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "publication sample has no checked node identity".into(),
                    )
                })?;
            let key = quotient_key_for_chart_position_with_work(
                self.topology.sources.frame,
                &image,
                work,
            )?;
            if let Some(previous) = quotient_images.get(&node) {
                if !compare_quotient_keys_with_work(previous, &key, work)?.is_eq() {
                    return Err(GeometryErrorKind::Invalid(
                        "publication occurrences of one node have different assigned quotient images"
                            .into(),
                    )
                    .into());
                }
            } else {
                quotient_images.insert(node, key);
            }
            checked.push(image);
        }
        Ok(checked)
    }
    fn check_global_candidate(
        &mut self,
        coordinates: &QCoordinates,
        publication_coordinates: &[Option<QCandidate>],
        drawing: QEmissionDrawing<'topology>,
        budget: &mut impl SearchAccount,
    ) -> Result<GlobalCheck<QEmbeddingOutput>> {
        let live_keys = drawing.live_node_keys(self.topology.sources.frame, budget)?;
        let (identity_conflicts, identity_unrepresentable) = self.publication_identity_conflicts(
            coordinates,
            &live_keys,
            publication_coordinates,
            budget,
        )?;
        if identity_unrepresentable {
            return Ok(GlobalCheck::Reject(Vec::new()));
        }
        if !identity_conflicts.is_empty() {
            return Ok(GlobalCheck::Reject(
                self.classes_for_variables(identity_conflicts, budget)?,
            ));
        }
        let source_faces = if let Some(exact_faces) = &self.exact_faces {
            let chart = subgraph_chart(
                self.topology,
                self.emission,
                &self.nearest,
                Some(coordinates),
                budget,
            )?;
            let emitted_faces = QFaceStructure::build(self.topology, self.emission, chart)?;
            if let Some(nodes) = face_structure_mismatch_nodes(
                exact_faces,
                &emitted_faces,
                &self.emission.selected_nodes,
            ) {
                return Ok(GlobalCheck::Reject(self.classes_for_nodes(nodes, budget)?));
            }
            Some(emitted_faces)
        } else {
            None
        };
        let mut producer_projection = {
            let source_faces_for_certification = source_faces;
            self.certify_assigned_producer(
                coordinates,
                publication_coordinates,
                source_faces_for_certification.as_ref(),
                budget,
            )?
        };
        let publication_rejections =
            self.publication_graph_rejections(&producer_projection, budget)?;
        if !publication_rejections.is_empty() {
            return Ok(GlobalCheck::Reject(
                self.classes_for_variables(publication_rejections, budget)?,
            ));
        }
        match check_q_ring_roles(self.topology, &drawing, budget)? {
            QRingRoleCheck::Pass(ring_roles) => {
                if !producer_projection.rejections.is_empty() {
                    let rejections = std::mem::take(&mut producer_projection.rejections);
                    return Ok(GlobalCheck::Reject(
                        self.classes_for_variables(rejections, budget)?,
                    ));
                }
                let native_required = self.actual_native_demand(
                    &drawing,
                    &ring_roles,
                    &producer_projection,
                    coordinates,
                    publication_coordinates,
                    budget,
                )?;
                let sealed = if native_required {
                    let budget = budget.budget_mut();
                    self.freeze_native_families(budget)?;
                    let native_cut_features =
                        self.materialize_native_cut_features(&producer_projection, budget)?;
                    let prepared_reference = self.prepare_native_reference(
                        &drawing,
                        &ring_roles,
                        &producer_projection,
                        coordinates,
                        publication_coordinates,
                        &native_cut_features,
                        budget,
                    );
                    let reference = match native_reference_global_check(prepared_reference, || {
                        self.all_outer_repair_classes(budget)
                    })? {
                        GlobalCheck::Pass(reference) => reference,
                        GlobalCheck::Reject(classes) => {
                            return Ok(GlobalCheck::Reject(classes));
                        },
                    };
                    let native = match self.search_native_cut_candidates(
                        &reference,
                        &native_cut_features,
                        coordinates,
                        publication_coordinates,
                        budget,
                    )? {
                        NativeCutSearchOutcome::Pass(drawing) => drawing,
                        NativeCutSearchOutcome::Reject(classes) => {
                            return Ok(GlobalCheck::Reject(classes));
                        },
                    };
                    super::super::emission::SealedQDrawing::Native(native)
                } else {
                    let live_publication_samples =
                        super::super::emission::source_publication::source_live_publication_samples(
                            self.emission.plan,
                            self.emission.publication,
                            budget,
                        )?;
                    let publication_chart_points =
                        self.assigned_publication_chart_points(&producer_projection, budget)?;
                    let publication_points = publication_chart_points
                        .iter()
                        .map(rounded_chart_point)
                        .collect::<Result<Vec<_>>>()?;
                    let publication_ordinates =
                        super::super::emission::resolve_publication_ordinates(
                            self.topology,
                            self.emission.publication,
                            &live_publication_samples,
                            self.declared_axes,
                            self.metric_access,
                            budget,
                        )?;
                    let source_publication =
                        super::super::emission::source_publication::SourcePublication {
                            inventory: self.emission.publication,
                            points: &publication_points,
                            ordinates: &publication_ordinates,
                            live_samples: &live_publication_samples,
                        };
                    let source = self.emission.plan.seal_source_drawing(
                        drawing,
                        &ring_roles,
                        &self.ordinates,
                        Some(source_publication),
                        Some(&producer_projection.source_ports),
                        self.declared_axes,
                        self.strict_axes,
                        self.operation,
                        budget,
                    )?;
                    let receipt = SourceOnlyPublication {
                        source,
                        topology: self.topology,
                        source_ports: Some(&producer_projection.source_ports),
                        source_ordinates: &self.ordinates,
                        publication: Some(self.emission.publication),
                        publication_images: &publication_chart_points,
                        publication_points: &publication_points,
                        publication_ordinates: &publication_ordinates,
                    };
                    let sealed = match finish_geographic(
                        PublicationCall::new(
                            ProducerReceipt::QSourceOnly(receipt),
                            self.topology.publication_frame().clone(),
                            budget,
                        ),
                        StorageTarget::Scalar,
                    )? {
                        PublicationOutcome::Pass(finished) => finished,
                        PublicationOutcome::Reject(PublicationRejection::SourceOnly(images)) => {
                            let mut variables = Vec::with_capacity(images.len());
                            for image in images {
                                budget.charge_into_error(1)?;
                                variables.push(match image {
                                    image @ (QSourceImage::Port(_)
                                    | QSourceImage::SourcePoint(_)
                                    | QSourceImage::SourceRingStart(_)
                                    | QSourceImage::SourceRingEnd(_)
                                    | QSourceImage::CollapsedStart(_)
                                    | QSourceImage::CollapsedEnd(_)) => {
                                        let port = q_source_image_port(self.topology, image)?;
                                        let node = self
                                            .topology
                                            .q
                                            .ports
                                            .get(port.0 as usize)
                                            .map(|port| port.node)
                                            .ok_or_else(|| {
                                                GeometryErrorKind::Invalid(
                                                    "rejected source literal port is unknown"
                                                        .into(),
                                                )
                                            })?;
                                        QVariableNode::Source(node)
                                    },
                                    QSourceImage::PublicationSample(sample) => {
                                        let node = self
                                            .publication_sample_nodes
                                            .get(sample as usize)
                                            .copied()
                                            .ok_or_else(|| {
                                                GeometryErrorKind::Invalid(
                                                    "rejected publication literal has no node"
                                                        .into(),
                                                )
                                            })?;
                                        QVariableNode::Published(node)
                                    },
                                });
                            }
                            return Ok(GlobalCheck::Reject(
                                self.classes_for_variables(variables, budget)?,
                            ));
                        },
                        PublicationOutcome::Reject(PublicationRejection::Native) => {
                            return Err(GeometryErrorKind::Invalid(
                                "source-only Q finisher returned native rejection".into(),
                            )
                            .into());
                        },
                        PublicationOutcome::Reject(PublicationRejection::Batch(_)) => {
                            return Err(GeometryErrorKind::Invalid(
                                "source-only Q finisher returned array batch rejection".into(),
                            )
                            .into());
                        },
                        PublicationOutcome::Reject(PublicationRejection::ProducerDrawing(_)) => {
                            return Err(GeometryErrorKind::Invalid(
                                "source-only Q finisher returned producer-drawn rejection".into(),
                            )
                            .into());
                        },
                    };
                    super::super::emission::SealedQDrawing::Source(SealedSourceDrawing {
                        sealed,
                        line_traces: Vec::new(),
                    })
                };
                Ok(GlobalCheck::Pass(QEmbeddingOutput {
                    sealed,
                    family_state: None,
                }))
            },
            QRingRoleCheck::Reject { nodes, .. } => {
                Ok(GlobalCheck::Reject(self.classes_for_nodes(nodes, budget)?))
            },
        }
    }
}

pub(in crate::geometry::topology::q_embedding) fn native_reference_global_check<T>(
    result: Result<T>,
    mut reject_classes: impl FnMut() -> Result<Vec<usize>>,
) -> Result<GlobalCheck<T>> {
    match result {
        Ok(reference) => Ok(GlobalCheck::Pass(reference)),
        Err(error)
            if matches!(
                error.kind(),
                crate::error::ErrorKind::Geometry(GeometryErrorKind::NativeRefinementMismatch)
            ) =>
        {
            Ok(GlobalCheck::Reject(reject_classes()?))
        },
        Err(error) => Err(error),
    }
}
