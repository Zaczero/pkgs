use super::{
    AxisValue, ChartMetric, CoordinateAxes, ExactLineEmission, ExactPartition, ExactRational,
    ExactSourceInventory, GeometryErrorKind, MOrdinate, NodeId, Point, Result, SourceArcContext,
    SourceArcRequest, XY, ZOrdinate, embedding, interpolate_ordinate, parameter_along,
    source_arc_ordinates,
};
use crate::work::Unmetered;

pub(in crate::geometry) struct ResolvedNodePoints<'e, 'p> {
    embedding: &'e embedding::Embedding<'p>,
    by_node: Option<Vec<Option<(ZOrdinate, MOrdinate)>>>,
    by_xy: Option<Vec<Option<XY>>>,
}

impl ResolvedNodePoints<'_, '_> {
    pub(in crate::geometry) fn coordinates(&self) -> &[XY] {
        self.embedding.coordinates()
    }

    pub(in crate::geometry) fn get(&self, node: NodeId, xy: XY) -> Option<Point> {
        let xy = self
            .by_xy
            .as_ref()
            .and_then(|points| points.get(node.0 as usize))
            .copied()
            .flatten()
            .unwrap_or(xy);
        let Some(by_node) = &self.by_node else {
            return Some(Point::new_raw_xy(xy.x, xy.y));
        };
        let (z, m) = by_node.get(node.0 as usize).copied().flatten()?;
        Some(Point::new_raw_axes(xy.x, xy.y, z, m))
    }

    pub(in crate::geometry) fn ordinates(&self, node: NodeId) -> Option<(ZOrdinate, MOrdinate)> {
        self.by_node.as_ref().map_or_else(
            || Some((ZOrdinate(None), MOrdinate(None))),
            |by_node| by_node.get(node.0 as usize).copied().flatten(),
        )
    }

    pub(in crate::geometry) fn point(&self, node: NodeId, xy: XY) -> Point {
        self.get(node, xy)
            .expect("emitted node has a resolved point")
    }
}

impl<'e, 'p> ResolvedNodePoints<'e, 'p> {
    pub(super) const fn from_embedding(
        embedding: &'e embedding::Embedding<'p>,
        by_node: Option<Vec<Option<(ZOrdinate, MOrdinate)>>>,
    ) -> Self {
        Self {
            embedding,
            by_node,
            by_xy: None,
        }
    }

    pub(super) const fn from_embedding_with_xy(
        embedding: &'e embedding::Embedding<'p>,
        by_node: Option<Vec<Option<(ZOrdinate, MOrdinate)>>>,
        by_xy: Vec<Option<XY>>,
    ) -> Self {
        Self {
            embedding,
            by_node,
            by_xy: Some(by_xy),
        }
    }
}

impl ExactSourceInventory {
    #[expect(
        clippy::indexing_slicing,
        reason = "node_bindings is a CSR row lookup for a NodeId admitted by the same ExactPartition"
    )]
    pub(super) fn node_bindings<'a>(
        &self,
        node: NodeId,
        partition: &'a ExactPartition<'_>,
    ) -> &'a [u32] {
        let Some(bindings) = &partition.ordinate_bindings else {
            return &[];
        };
        let index = node.0 as usize;
        let start = bindings.starts[index] as usize;
        let end = bindings.starts[index + 1] as usize;
        &bindings.unique[start..end]
    }

    pub(super) fn source_ordinate_points(&self, source: u32) -> Result<Option<&[Point; 2]>> {
        self.source_points
            .as_ref()
            .map(|points| {
                points
                    .get(source as usize)
                    .map(|record| &record.points)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "exact partition ordinate binding references an unknown source".into(),
                        )
                        .into()
                    })
            })
            .transpose()
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "unique, source, and point IDs come from this inventory's checked provenance and partition"
    )]
    pub(super) fn resolve_node_ordinates<M: ChartMetric>(
        &self,
        partition: &ExactPartition<'_>,
        node: NodeId,
        axes: crate::geometry::CoordinateAxes,
        context: &SourceArcContext<'_, '_, M>,
        source_scope: Option<u32>,
    ) -> Result<(ZOrdinate, MOrdinate)> {
        if !axes.has_z() && !axes.has_m() {
            return Ok((ZOrdinate(None), MOrdinate(None)));
        }
        let mut z = AxisValue::default();
        let mut m = AxisValue::default();
        let source_frame = match context.domain() {
            crate::geometry::TopologyDomain::Geographic(frame) => Some(frame),
            crate::geometry::TopologyDomain::Cartesian => None,
        };
        let bindings = self.node_bindings(node, partition);
        if !bindings.is_empty() {
            let point = partition.exact(node)?;
            for &unique in bindings {
                // The parameter from the unique segment's start, and from its
                // end for a source stored in reverse.
                let [num, den] = parameter_along(partition.unique[unique as usize], &point)?;
                let from_end = den.clone().try_subtract(num.clone())?;
                for &(source, reversed) in self.provenance.owners(unique as usize) {
                    if source_scope.is_some_and(|operand| {
                        self.source_operands.get(source as usize).copied() != Some(operand)
                    }) {
                        continue;
                    }
                    let Some(records) = self.source_points.as_ref() else {
                        continue;
                    };
                    let along = if reversed { &from_end } else { &num };
                    let record = records.get(source as usize).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "exact partition ordinate binding references an unknown source".into(),
                        )
                    })?;
                    let points = &record.points;
                    let [start, end] = *points;
                    if let Some(source_frame) = source_frame {
                        let parameter = ExactRational::new(along.clone(), den.clone())?;
                        let request =
                            SourceArcRequest::new(source_frame, points, axes, &parameter, || {
                                source_frame
                                    .longitude()
                                    .chart_connection(start.x, end.x)
                                    .map(crate::geographic::ChartConnection::retained)
                                    .map_err(|error| {
                                        GeometryErrorKind::Invalid(error.to_string().into()).into()
                                    })
                            })?;
                        let mut fraction = None;
                        let mut work = Unmetered;
                        let mut metric_state = record.metric.get();
                        let ordinates = source_arc_ordinates(
                            &request,
                            || {
                                let access = context.metric_access().ok_or_else(|| {
                                crate::crs::CrsError::invalid(
                                    "geographic source-ordinate carry requires the operation's CRS metric",
                                )
                            })?;
                                access()
                            },
                            &mut metric_state,
                            &mut fraction,
                            &mut work,
                        );
                        record.metric.set(metric_state);
                        let ordinates = ordinates?;
                        if axes.has_z() {
                            z.observe(ordinates.z.value());
                        }
                        if axes.has_m() {
                            m.observe(ordinates.m.value());
                        }
                    } else {
                        if axes.has_z() {
                            z.observe(interpolate_ordinate(start.z(), end.z(), along, &den)?);
                        }
                        if axes.has_m() {
                            m.observe(interpolate_ordinate(start.m(), end.m(), along, &den)?);
                        }
                    }
                }
            }
        }
        for &(bound, source) in &partition.point_sources {
            if bound != node
                || source_scope.is_some_and(|operand| {
                    self.source_operands.get(source as usize).copied() != Some(operand)
                })
            {
                continue;
            }
            let Some(points) = self.source_ordinate_points(source)? else {
                continue;
            };
            for point in points {
                if axes.has_z() {
                    z.observe(point.z());
                }
                if axes.has_m() {
                    m.observe(point.m());
                }
            }
        }
        for (bound, point, operand) in self.isolated_point_occurrences(partition) {
            if bound != node || source_scope.is_some_and(|source| operand != source) {
                continue;
            }
            if axes.has_z() {
                z.observe(point.z());
            }
            if axes.has_m() {
                m.observe(point.m());
            }
        }
        Ok((ZOrdinate(z.value()), MOrdinate(m.value())))
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "the binding-start windows and node-aligned output are built from the same ExactPartition"
    )]
    pub(super) fn ordinate_protected_nodes(
        &self,
        partition: &ExactPartition<'_>,
        axes: crate::geometry::CoordinateAxes,
    ) -> Result<Option<Vec<bool>>> {
        if !axes.has_z() && !axes.has_m() {
            return Ok(None);
        }
        let Some(bindings) = &partition.ordinate_bindings else {
            return Ok(None);
        };
        let mut protected = Vec::with_capacity(partition.node_count());
        protected.resize(partition.node_count(), false);
        for (node, &[start, end]) in bindings.starts.array_windows::<2>().enumerate() {
            for &unique in &bindings.unique[start as usize..end as usize] {
                let mut source_has_ordinate = false;
                for &(source, _) in self.provenance.owners(unique as usize) {
                    let Some(points) = self.source_ordinate_points(source)? else {
                        continue;
                    };
                    source_has_ordinate |= (axes.has_z()
                        && (points[0].z().is_some() || points[1].z().is_some()))
                        || (axes.has_m() && (points[0].m().is_some() || points[1].m().is_some()));
                }
                if source_has_ordinate {
                    protected[node] = true;
                    break;
                }
            }
        }
        Ok(Some(protected))
    }

    pub(in crate::geometry) fn resolve_line_emission_points<
        'e,
        'p,
        M: ChartMetric,
        I: IntoIterator<Item = NodeId>,
    >(
        &self,
        partition: &ExactPartition<'_>,
        emission: &'e ExactLineEmission<'p>,
        nodes: I,
        declared_axes: crate::geometry::CoordinateAxes,
        context: &SourceArcContext<'_, '_, M>,
        source_scope: Option<u32>,
    ) -> Result<ResolvedNodePoints<'e, 'p>> {
        self.resolve_node_points(
            partition,
            &emission.embedding,
            nodes,
            declared_axes,
            context,
            source_scope,
        )
    }

    pub(super) fn resolve_node_points<'e, 'p, M: ChartMetric, I: IntoIterator<Item = NodeId>>(
        &self,
        partition: &ExactPartition<'_>,
        embedding: &'e embedding::Embedding<'p>,
        nodes: I,
        declared_axes: crate::geometry::CoordinateAxes,
        context: &SourceArcContext<'_, '_, M>,
        source_scope: Option<u32>,
    ) -> Result<ResolvedNodePoints<'e, 'p>> {
        if !declared_axes.has_z() && !declared_axes.has_m() {
            return Ok(ResolvedNodePoints::from_embedding(embedding, None));
        }
        let mut nodes = nodes.into_iter().peekable();
        if nodes.peek().is_none() {
            return Ok(ResolvedNodePoints::from_embedding(embedding, None));
        }
        let mut by_node = vec![None; partition.node_count()];
        self.resolve_node_points_into(
            partition,
            nodes,
            declared_axes,
            context,
            source_scope,
            &mut by_node,
        )?;
        Ok(ResolvedNodePoints::from_embedding(embedding, Some(by_node)))
    }

    pub(super) fn resolve_node_points_into<M: ChartMetric, I: IntoIterator<Item = NodeId>>(
        &self,
        partition: &ExactPartition<'_>,
        nodes: I,
        declared_axes: CoordinateAxes,
        context: &SourceArcContext<'_, '_, M>,
        source_scope: Option<u32>,
        by_node: &mut [Option<(ZOrdinate, MOrdinate)>],
    ) -> Result<()> {
        if !declared_axes.has_z() && !declared_axes.has_m() {
            return Ok(());
        }
        debug_assert_eq!(by_node.len(), partition.node_count());
        for node in nodes {
            let ordinates = by_node.get_mut(node.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("exact overlay output references an unknown node".into())
            })?;
            if ordinates.is_none() {
                *ordinates = Some(self.resolve_node_ordinates(
                    partition,
                    node,
                    declared_axes,
                    context,
                    source_scope,
                )?);
            }
        }
        Ok(())
    }
}
