use crate::error::Result;
use crate::geographic::{
    ComputedEdgeIntent, ComputedVertexIntent, GeographicFrame, QuotientVertex,
};
use crate::geometry::{
    CoordSeq, ExpansionBudget, GeometryErrorKind, GeometryKind, LineSeq, MultiLineString, Point,
    PointKey, Shape, SourceArcContext, publish_native_computed_chain,
};

pub(super) fn directed_geographic_line_merge(
    input: &Shape,
    lines: &MultiLineString,
    frame: GeographicFrame,
    context: &SourceArcContext<'_, '_>,
) -> Result<Shape> {
    for line in lines {
        crate::crs::ensure_geographic_coordseq_in_frame(frame, line)?;
    }
    let (chains, origins) = super::merge_directed_geographic_chains(lines, frame)?;
    if chains.is_empty() {
        return Ok(Shape::typed_empty(
            GeometryKind::MultiLineString,
            input.axes(),
        ));
    }
    let mut output = Vec::with_capacity(chains.len());
    let mut budget = ExpansionBudget::for_input("line_merge", "retained chart edges");
    for (chain, origins) in chains.into_iter().zip(origins) {
        let aliases_join = origins.windows(2).enumerate().any(|(edge, pair)| {
            let &[_, (_, vertex)] = pair else {
                unreachable!("adjacent origin window has two occurrences");
            };
            vertex == 0
                && PointKey::new(chain.point_at(edge))
                    != PointKey::new(chain.point_at(edge.wrapping_add(1)))
        });
        if !aliases_join {
            output.push(LineSeq::from_proven(chain));
            continue;
        }
        let published = publish_native_computed_chain(
            frame,
            &chain,
            false,
            &mut budget,
            context,
            |edge, start, end| {
                let &(line, vertex) = origins
                    .get(edge.wrapping_add(1))
                    .expect("every emitted edge ends at its recorded source occurrence");
                if vertex != 0 {
                    let source = lines.get(line).expect("origin selects its source line");
                    let source_start = source.point_at(vertex.wrapping_sub(1));
                    let site = frame.quotient_vertex(source_start.x, source_start.y);
                    if matches!(site, QuotientVertex::ChartPole(_))
                        && site == frame.quotient_vertex(end.x, end.y)
                    {
                        // A pole-only edge has point support. Keep its endpoint
                        // ordinates without sampling a zero-arclength interior.
                        return coincident_site_intent(frame, source_start, end);
                    }
                    return ComputedEdgeIntent::retained_chart_connection(frame, [
                        source_start.x,
                        end.x,
                    ]);
                }
                coincident_site_intent(frame, start, end)
            },
            |_, _| ComputedVertexIntent::Sampled,
        )?;
        output.push(LineSeq::try_new(match published {
            Some(points) => CoordSeq::try_from_points(&points)?,
            None => chain,
        })?);
    }
    Ok(Shape::narrow_lines(input.axes(), output))
}

fn coincident_site_intent(
    frame: GeographicFrame,
    start: Point,
    end: Point,
) -> Result<ComputedEdgeIntent<'static>> {
    debug_assert_eq!(
        frame.quotient_vertex(start.x, start.y),
        frame.quotient_vertex(end.x, end.y),
        "joined source occurrences denote the same geographic endpoint",
    );
    let longitudes = if matches!(
        frame.quotient_vertex(start.x, start.y),
        QuotientVertex::ChartPole(_)
    ) {
        let axis = frame.longitude();
        [
            axis.canonicalize(start.x)
                .map_err(|error| GeometryErrorKind::message(error.to_string()))?
                .get(),
            axis.canonicalize(end.x)
                .map_err(|error| GeometryErrorKind::message(error.to_string()))?
                .get(),
        ]
    } else {
        // Aliased endpoint records are one site, not an authored full-period edge.
        [start.x, start.x]
    };
    ComputedEdgeIntent::retained_chart_connection(frame, longitudes)
}
