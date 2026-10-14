use super::super::{
    FragmentRecipe, NativeAreaEdgeKind, NativeLiteralChordAuthority, NativeReference,
};
use crate::error::Result;
use crate::geometry::ordinates::{RequestRowInput, RootId, RootRange, RootRequest};
use crate::geometry::tessellation::exact::ExactRational;
use crate::geometry::{CoordinateAxes, GeometryErrorKind};
use crate::work::Work;

impl NativeReference<'_> {
    pub(in crate::geometry::topology::q_embedding) fn coverage_request_rows(
        &self,
        recipes: &[FragmentRecipe],
        axes: CoordinateAxes,
        work: &mut impl Work,
    ) -> Result<Vec<RequestRowInput>> {
        let mut rows = std::iter::repeat_with(|| RequestRowInput {
            axes,
            requests: Vec::new(),
        })
        .take(self.plan.ports.len())
        .collect::<Vec<_>>();
        for use_ in &self.literal_chord_uses {
            work.charge_into_error(1)?;
            let chord = self.plan.chords.get(use_.chord.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("coverage chord has no native writer authority".into())
            })?;
            let NativeLiteralChordAuthority::ReferenceAtom {
                reversed,
                writer_edges,
                ..
            } = &use_.authority
            else {
                return Err(GeometryErrorKind::Invalid(
                    "coverage native drawing contains a line chord".into(),
                )
                .into());
            };
            for &writer in writer_edges {
                work.charge_into_error(1)?;
                let written = self.graph.written_edges.get(writer).ok_or_else(|| {
                    GeometryErrorKind::Invalid("coverage atom writer is unknown".into())
                })?;
                let edge = self.area_edges.get(writer).ok_or_else(|| {
                    GeometryErrorKind::Invalid("coverage writer has no areal source edge".into())
                })?;
                let NativeAreaEdgeKind::Producer {
                    recipe,
                    trace_ranges,
                    ..
                } = &edge.kind
                else {
                    continue;
                };
                let recipe = recipes.get(recipe.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("coverage writer source recipe is unknown".into())
                })?;
                let parameters = if written.reversed == *reversed {
                    [ExactRational::zero(), ExactRational::one()]
                } else {
                    [ExactRational::one(), ExactRational::zero()]
                };
                for (port, local) in chord.ports.into_iter().zip(parameters) {
                    let row = rows.get_mut(port.0 as usize).ok_or_else(|| {
                        GeometryErrorKind::Invalid("coverage writer output port is unknown".into())
                    })?;
                    for (_, range) in trace_ranges {
                        work.charge_into_error(1)?;
                        if let Some(parameter) = range.parameter_at_with_work(&local, work)?
                            && selected_source_parameter(recipe, range, &parameter, work)?
                        {
                            row.requests.push(RootRequest::new_with_work(
                                range.root(),
                                parameter,
                                work,
                            )?);
                        }
                    }
                }
            }
        }
        Ok(rows)
    }
}

fn selected_source_parameter(
    recipe: &FragmentRecipe,
    range: &RootRange,
    parameter: &ExactRational,
    work: &mut impl Work,
) -> Result<bool> {
    for selected in &recipe.ranges {
        work.charge_into_error(1)?;
        if !selected.emitted || RootId::from_index(selected.source_edge.0) != range.root() {
            continue;
        }
        let (low, high) = if selected
            .range
            .start
            .try_cmp_with_work(&selected.range.end, work)?
            .is_le()
        {
            (&selected.range.start, &selected.range.end)
        } else {
            (&selected.range.end, &selected.range.start)
        };
        if parameter.try_cmp_with_work(low, work)?.is_ge()
            && parameter.try_cmp_with_work(high, work)?.is_le()
        {
            return Ok(true);
        }
    }
    Ok(false)
}
