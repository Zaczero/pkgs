use super::*;

/// Pair each original and transformed coordinate chain without staging edge
/// relations; callers request each retained target connection from its
/// original endpoint pair while publishing that chain. Use this route only
/// when the source frame is geographic; projected sources retain their
/// producer-chord interpretation.
pub(crate) fn map_transformed_shape_chains(
    source: &Shape,
    output: &Shape,
    source_frame: GeographicFrame,
    target_frame: GeographicFrame,
    mut map: impl FnMut(
        &TransformedChartEdgeProvider<'_>,
        bool,
    ) -> Result<Option<Vec<crate::geometry::Point>>>,
) -> Result<Shape> {
    map_computed_shape_chains(source, output, |source, output, closed| {
        let edges =
            TransformedChartEdgeProvider::try_new(source, output, source_frame, target_frame)?;
        map(&edges, closed)
    })
}

/// Borrow one original chain and its one-for-one transformed output.
///
/// Each edge connection is derived on request, so scalar chains and selected
/// packed windows retain their caller-owned endpoint pairs without staging a
/// parallel displacement column.
pub(crate) struct TransformedChartEdgeProvider<'a> {
    source: &'a CoordSeq,
    output: &'a CoordSeq,
    source_frame: GeographicFrame,
    target_frame: GeographicFrame,
}

impl<'a> TransformedChartEdgeProvider<'a> {
    pub(crate) fn try_new(
        source: &'a CoordSeq,
        output: &'a CoordSeq,
        source_frame: GeographicFrame,
        target_frame: GeographicFrame,
    ) -> Result<Self> {
        if source.coord_count() != output.coord_count() {
            return Err(GeometryErrorKind::message(
                "transformed coordinate chain changed endpoint correspondence",
            ));
        }
        Ok(Self {
            source,
            output,
            source_frame,
            target_frame,
        })
    }

    pub(crate) const fn source(&self) -> &'a CoordSeq {
        self.source
    }

    pub(crate) const fn output(&self) -> &'a CoordSeq {
        self.output
    }

    pub(crate) fn edge_count(&self) -> usize {
        self.source.coord_count().saturating_sub(1)
    }

    pub(crate) fn retained_connection(&self, edge: usize) -> Result<RetainedChartDisplacement> {
        if edge >= self.edge_count() {
            return Err(GeometryErrorKind::message(
                "transformed edge index is outside its paired coordinate chain",
            ));
        }
        let source_start = self.source.nth_coord(edge);
        let source_end = self.source.nth_coord(edge.wrapping_add(1));
        let output_start = self.output.nth_coord(edge);
        let output_end = self.output.nth_coord(edge.wrapping_add(1));
        select_transformed_chart_displacement(
            self.source_frame,
            [source_start.x, source_end.x],
            self.target_frame,
            [output_start.x, output_end.x],
        )
    }
}

fn publish_transformed_chain_with_edges(
    source: &CoordSeq,
    output: &CoordSeq,
    closed_ring: bool,
    edges: Option<&TransformedChartEdgeProvider<'_>>,
    frame: GeographicFrame,
    budget: &mut ExpansionBudget,
    metric: &mut TransformMetricScratch<'_>,
) -> Result<Option<Vec<crate::geometry::Point>>> {
    if edges.is_none() && source.coord_count() != output.coord_count() {
        return Err(GeometryErrorKind::message(
            "transformed coordinate chain changed endpoint correspondence",
        ));
    }

    let mut current_root = None;
    let mut root_metric = RootMetricState::default();
    let mut work = Unmetered;
    publish_computed_chain_with_vertex_intents_and_samples(
        frame,
        output,
        closed_ring,
        budget,
        |index, start, end| {
            if let Some(edges) = edges {
                Ok(ComputedEdgeIntent::retained_native(
                    edges.retained_connection(index)?,
                ))
            } else {
                ComputedEdgeIntent::retained_binary64_pair(frame.longitude(), [start.x, end.x])
            }
        },
        |_, _| ComputedVertexIntent::Canonical,
        |index, endpoints, relation, parameter, _retained| {
            if current_root != Some(index) {
                current_root = Some(index);
                root_metric = RootMetricState::default();
            }
            let ComputedLongitudeRelation::Native(longitude) = *relation else {
                unreachable!("transform publication supplies native retained relations");
            };
            let source_metric = if output.axes().has_z() || output.axes().has_m() {
                Some(metric.geodesic_metric()?)
            } else {
                None
            };
            let metric_access = || {
                source_metric.ok_or_else(|| {
                    crate::crs::CrsError::invalid(
                        "geographic transform samples with Z/M require the target CRS metric",
                    )
                })
            };
            crate::geometry::computed_native_sample_ordinates(
                frame,
                endpoints,
                longitude,
                parameter,
                output.axes(),
                Some(&metric_access),
                &mut root_metric,
                &mut work,
            )
        },
        |_, _, _, _, _, _| Ok(()),
    )
}

/// Publish one provisional output chain using its paired original source edges.
///
/// Geographic sources select a target lift from the original endpoints;
/// projected sources retain the chord the executor actually produced.
pub(crate) fn publish_transformed_chain(
    source: &CoordSeq,
    output: &CoordSeq,
    closed_ring: bool,
    source_frame: Option<GeographicFrame>,
    frame: GeographicFrame,
    budget: &mut ExpansionBudget,
    metric: &mut TransformMetricScratch<'_>,
) -> Result<Option<Vec<crate::geometry::Point>>> {
    let edges = source_frame
        .map(|source_frame| {
            TransformedChartEdgeProvider::try_new(source, output, source_frame, frame)
        })
        .transpose()?;
    publish_transformed_chain_with_edges(
        source,
        output,
        closed_ring,
        edges.as_ref(),
        frame,
        budget,
        metric,
    )
}

pub(crate) fn publish_transformed_shape(
    source: &Shape,
    output: &Shape,
    source_frame: Option<GeographicFrame>,
    frame: GeographicFrame,
    operation: &'static str,
    metric: &mut TransformMetricScratch<'_>,
) -> Result<Shape> {
    let canonical_point = |point: crate::geometry::Point| {
        frame
            .canonical_computed_point(point)
            .map_err(|error| crate::crs::CrsError::invalid(error.to_string()))
    };
    match (source, output) {
        (Shape::Point(_), Shape::Point(point)) => Ok(Shape::Point(canonical_point(*point)?)),
        (Shape::MultiPoint(_), Shape::MultiPoint(points)) => {
            Ok(Shape::MultiPoint(points.try_map_values(canonical_point)?))
        },
        (Shape::GeometryCollection(source), Shape::GeometryCollection(output)) => {
            let mut source_members = source.iter();
            let mapped = output.try_map_members(|member| {
                let source = source_members.next().ok_or_else(|| {
                    GeometryErrorKind::message(
                        "computed output changed a geometry-collection structure",
                    )
                })?;
                publish_transformed_shape(source, member, source_frame, frame, operation, metric)
            })?;
            if source_members.next().is_some() {
                return Err(GeometryErrorKind::message(
                    "computed output changed a geometry-collection structure",
                ));
            }
            Ok(Shape::GeometryCollection(mapped))
        },
        (Shape::LineString(_), Shape::LineString(_))
        | (Shape::MultiLineString(_), Shape::MultiLineString(_))
        | (Shape::Polygon(_), Shape::Polygon(_))
        | (Shape::MultiPolygon(_), Shape::MultiPolygon(_)) => {
            let mut budget = ExpansionBudget::new(operation, "computed edges");
            budget.add(ShapeCountQuery::default().coordinates(output)?)?;
            match source_frame {
                Some(source_frame) => map_transformed_shape_chains(
                    source,
                    output,
                    source_frame,
                    frame,
                    |edges, closed| {
                        publish_transformed_chain_with_edges(
                            edges.source(),
                            edges.output(),
                            closed,
                            Some(edges),
                            frame,
                            &mut budget,
                            metric,
                        )
                    },
                ),
                None => map_computed_shape_chains(source, output, |source, output, closed| {
                    publish_transformed_chain(
                        source,
                        output,
                        closed,
                        None,
                        frame,
                        &mut budget,
                        metric,
                    )
                }),
            }
        },
        (Shape::Empty(..), Shape::Empty(..)) => Ok(output.clone()),
        _ => Err(GeometryErrorKind::message(
            "computed output changed geometry structure",
        )),
    }
}
