use super::{
    ChartPole, ChartRestriction, ChartSide, ChartSpanId, DirectedChartRestrictionUse,
    EmissionNodeId, EvaluationBudget, ExactRational, ExactSourceRange, FragmentRecipeId,
    GeographicFrame, GeometryErrorKind, GlobalFaceId, NativePortData, OccurrenceSource,
    PortSpelling, QDartId, QNodeId, QPortId, ResourceError, Result, RootRange, SourceEdgeId,
    SyntheticChartVertex, VertexOrigin, try_sort_by_with_work,
};
use crate::work::Work;

pub(in crate::geometry::topology) struct NativeAreaEdge {
    pub(in crate::geometry::topology) vertices: [NativePortData; 2],
    pub(in crate::geometry::topology) owner: NativeGraphEdgeOwner,
    pub(in crate::geometry::topology) kind: NativeAreaEdgeKind,
}

pub(super) fn directed_chart_restriction_use(
    restriction: ChartRestriction,
    vertices: &[NativePortData; 2],
) -> Result<DirectedChartRestrictionUse> {
    let synthetic_vertex = |port: &NativePortData| -> Result<SyntheticChartVertex> {
        match (&restriction, &port.spelling) {
            (
                ChartRestriction::CutChord | ChartRestriction::PolarCap { .. },
                PortSpelling::Seam(side),
            ) => Ok(SyntheticChartVertex::Cut {
                node: port.node.0,
                side: *side,
            }),
            (ChartRestriction::PolarCap { pole }, PortSpelling::Pole { native_longitude }) => {
                Ok(SyntheticChartVertex::Pole {
                    node: port.node.0,
                    pole: *pole,
                    native_longitude_bits: native_longitude.to_bits(),
                })
            },
            _ => Err(GeometryErrorKind::Invalid(
                "generated chart restriction has a nonsynthetic endpoint".into(),
            )
            .into()),
        }
    };
    Ok(DirectedChartRestrictionUse {
        restriction,
        start: synthetic_vertex(&vertices[0])?,
        end: synthetic_vertex(&vertices[1])?,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::geometry::topology) enum NativeGraphEdgeOwner {
    Area { region: usize },
    Line,
}

pub(in crate::geometry::topology) enum NativeAreaEdgeKind {
    Producer {
        recipe: FragmentRecipeId,
        trace_edge: usize,
        producer_start: Box<ExactRational>,
        producer_end: ExactRational,
        trace_ranges: Vec<(usize, RootRange)>,
        known_spans: Vec<NativeSourceSpanUse>,
    },
    Restriction(DirectedChartRestrictionUse),
    WholeSphereCover {
        cell: u8,
    },
}

#[derive(Clone)]
pub(in crate::geometry::topology) struct NativeSourceSpanUse {
    pub(in crate::geometry::topology) span: ChartSpanId,
    pub(in crate::geometry::topology) owner: usize,
    pub(in crate::geometry::topology) source_edge: SourceEdgeId,
    pub(in crate::geometry::topology) source_lift: i64,
    pub(in crate::geometry::topology) source_range: ExactSourceRange,
    pub(in crate::geometry::topology) producer_range: ExactSourceRange,
    pub(in crate::geometry::topology) span_range: ExactSourceRange,
    pub(in crate::geometry::topology) nodes: [QNodeId; 2],
    pub(in crate::geometry::topology) ports: [QPortId; 2],
    pub(in crate::geometry::topology) face_germs: Option<[NativeSourceFaceGerm; 2]>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::geometry::topology) struct NativeSourceFaceGerm {
    pub(in crate::geometry::topology) dart: QDartId,
    pub(in crate::geometry::topology) face: GlobalFaceId,
    pub(in crate::geometry::topology) selected_root: Option<usize>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(in crate::geometry::topology) struct AssignedSite {
    pub(in crate::geometry::topology) longitude: crate::geographic::RetainedLongitudeSite,
    pub(in crate::geometry::topology) latitude: f64,
}

pub(in crate::geometry::topology) struct SelectedNativeSeamBand {
    pub(in crate::geometry::topology) low: ExactRational,
    pub(in crate::geometry::topology) high: ExactRational,
    pub(in crate::geometry::topology) west_region: Option<usize>,
    pub(in crate::geometry::topology) east_region: Option<usize>,
}

pub(in crate::geometry::topology) struct NativeSeamVertices {
    pub(in crate::geometry::topology) latitude: ExactRational,
    pub(in crate::geometry::topology) east: NativePortData,
    pub(in crate::geometry::topology) west: NativePortData,
}

#[derive(Clone)]
pub(in crate::geometry::topology) struct NativeSeamEvent {
    pub(in crate::geometry::topology) latitude: ExactRational,
    pub(in crate::geometry::topology) output_vertex: bool,
}

pub(super) fn native_seam_vertex_index(
    events: &[NativeSeamVertices],
    latitude: &ExactRational,
    work: &mut impl Work,
) -> Result<usize> {
    let mut low = 0_usize;
    let mut high = events.len();
    while low < high {
        let middle = low.midpoint(high);
        let event = events.get(middle).ok_or_else(|| {
            GeometryErrorKind::Invalid("native seam event index is unknown".into())
        })?;
        if event.latitude.try_cmp_with_work(latitude, work)?.is_lt() {
            low = middle.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
        } else {
            high = middle;
        }
    }
    let event = events.get(low).ok_or_else(|| {
        GeometryErrorKind::Invalid("native seam band endpoint has no output vertex".into())
    })?;
    if !event.latitude.try_cmp_with_work(latitude, work)?.is_eq() {
        return Err(GeometryErrorKind::Invalid(
            "native seam band endpoint lost its output vertex".into(),
        )
        .into());
    }
    Ok(low)
}

pub(in crate::geometry::topology) fn native_seam_edges(
    bands: &[SelectedNativeSeamBand],
    events: &[NativeSeamVertices],
    has_existing_area_edge: &mut impl FnMut(
        usize,
        &NativePortData,
        &NativePortData,
        &mut EvaluationBudget,
    ) -> Result<bool>,
    budget: &mut EvaluationBudget,
) -> Result<Vec<NativeAreaEdge>> {
    let band_work = bands
        .len()
        .checked_mul(2)
        .ok_or(ResourceError::SizeOverflow)?;
    let work = events
        .len()
        .checked_add(band_work)
        .ok_or(ResourceError::SizeOverflow)?;
    budget.charge_into_error(u64::try_from(work).map_err(|_| ResourceError::SizeOverflow)?)?;
    for pair in events.array_windows::<2>() {
        let [low, high] = pair;
        if !low
            .latitude
            .try_cmp_with_work(&high.latitude, budget)?
            .is_lt()
        {
            return Err(GeometryErrorKind::Invalid(
                "native seam output vertices are not in strict latitude order".into(),
            )
            .into());
        }
    }
    let mut edges = Vec::with_capacity(band_work);
    for band in bands {
        if band.west_region.is_none() && band.east_region.is_none() {
            continue;
        }
        let low = events
            .get(native_seam_vertex_index(events, &band.low, budget)?)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("native seam lower endpoint is unknown".into())
            })?;
        let high = events
            .get(native_seam_vertex_index(events, &band.high, budget)?)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("native seam upper endpoint is unknown".into())
            })?;
        if let Some(region) = band.east_region
            && !has_existing_area_edge(region, &low.east, &high.east, budget)?
        {
            let vertices = [
                low.east.try_clone_with_work(budget)?,
                high.east.try_clone_with_work(budget)?,
            ];
            let kind = NativeAreaEdgeKind::Restriction(directed_chart_restriction_use(
                ChartRestriction::CutChord,
                &vertices,
            )?);
            edges.push(NativeAreaEdge {
                vertices,
                owner: NativeGraphEdgeOwner::Area { region },
                kind,
            });
        }
        if let Some(region) = band.west_region
            && !has_existing_area_edge(region, &high.west, &low.west, budget)?
        {
            let vertices = [
                high.west.try_clone_with_work(budget)?,
                low.west.try_clone_with_work(budget)?,
            ];
            let kind = NativeAreaEdgeKind::Restriction(directed_chart_restriction_use(
                ChartRestriction::CutChord,
                &vertices,
            )?);
            edges.push(NativeAreaEdge {
                vertices,
                owner: NativeGraphEdgeOwner::Area { region },
                kind,
            });
        }
    }
    Ok(edges)
}

pub(super) fn native_pole_presentation_occurrence(
    source: &NativePortData,
    node: EmissionNodeId,
    latitude: &ExactRational,
    west: &ExactRational,
    half: &ExactRational,
    budget: &mut EvaluationBudget,
) -> Result<Option<NativePortData>> {
    budget.charge_into_error(1)?;
    if source.node != node
        || !matches!(&source.spelling, PortSpelling::Pole { .. })
        || !source.point.y.try_cmp_with_work(latitude, budget)?.is_eq()
        || !source.point.x.try_cmp_with_work(west, budget)?.is_gt()
        || !source.point.x.try_cmp_with_work(half, budget)?.is_lt()
    {
        return Ok(None);
    }
    let PortSpelling::Pole { native_longitude } = &source.spelling else {
        return Ok(None);
    };
    if !ExactRational::from_f64(*native_longitude)
        .try_cmp_with_work(&source.point.x, budget)?
        .is_eq()
    {
        return Err(GeometryErrorKind::Invalid(
            "native pole occurrence has an inexact longitude spelling".into(),
        )
        .into());
    }
    budget.charge_into_error(1)?;
    Ok(Some(NativePortData {
        node: source.node,
        point: source.point.try_clone_with_work(budget)?,
        spelling: source.spelling.clone(),
        source_record: None,
        occurrences: vec![OccurrenceSource {
            origin: VertexOrigin::Presentation,
            source_trace: None,
        }],
    }))
}

pub(in crate::geometry::topology) fn native_pole_edges(
    frame: GeographicFrame,
    events: &[NativeSeamVertices],
    closures: &[(usize, ChartPole)],
    area_edges: &[NativeAreaEdge],
    line_edges: &[NativeAreaEdge],
    reference_ports: &[NativePortData],
    has_existing_area_edge: &mut impl FnMut(
        usize,
        &NativePortData,
        &NativePortData,
        &mut EvaluationBudget,
    ) -> Result<bool>,
    budget: &mut EvaluationBudget,
) -> Result<Vec<NativeAreaEdge>> {
    if closures.is_empty() {
        return Ok(Vec::new());
    }
    budget.charge_into_error(
        u64::try_from(closures.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut edges = Vec::new();
    let half = ExactRational::from_f64(frame.longitude().half_period());
    let west = ExactRational::zero().try_subtract_with_work(&half, budget)?;
    let mut known_poles = Vec::<(
        ChartPole,
        EmissionNodeId,
        ExactRational,
        Vec<NativePortData>,
    )>::new();
    for pole in [ChartPole::South, ChartPole::North] {
        budget.charge_into_error(
            u64::try_from(closures.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        if !closures
            .iter()
            .any(|(_, closure_pole)| *closure_pole == pole)
        {
            continue;
        }
        let latitude = ExactRational::from_f64(frame.chart_pole_latitude(pole));
        let event = events
            .get(native_seam_vertex_index(events, &latitude, budget)?)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("native pole closure has no seam event".into())
            })?;
        if event.east.node != event.west.node
            || !event.latitude.try_cmp_with_work(&latitude, budget)?.is_eq()
        {
            return Err(GeometryErrorKind::Invalid(
                "native pole banks do not share their typed pole identity".into(),
            )
            .into());
        }
        known_poles.push((pole, event.east.node, latitude, Vec::new()));
    }
    for edge in area_edges.iter().chain(line_edges) {
        budget.charge_into_error(1)?;
        if !matches!(&edge.kind, NativeAreaEdgeKind::Producer { .. }) {
            continue;
        }
        for source in &edge.vertices {
            for (_, node, latitude, known) in &mut known_poles {
                if let Some(presentation) = native_pole_presentation_occurrence(
                    source, *node, latitude, &west, &half, budget,
                )? {
                    known.push(presentation);
                }
            }
        }
    }
    for source in reference_ports {
        for (_, node, latitude, known) in &mut known_poles {
            if let Some(presentation) =
                native_pole_presentation_occurrence(source, *node, latitude, &west, &half, budget)?
            {
                known.push(presentation);
            }
        }
    }
    for (_, _, _, known) in &mut known_poles {
        try_sort_by_with_work(known, budget, |left, right, budget| {
            budget.charge_into_error(1)?;
            left.point.x.try_cmp_with_work(&right.point.x, budget)
        })?;
        let known_count = known.len();
        budget.charge_into_error(
            u64::try_from(known_count).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut unique = Vec::<NativePortData>::with_capacity(known_count);
        for port in std::mem::take(known) {
            budget.charge_into_error(1)?;
            if let Some(previous) = unique.last()
                && previous
                    .point
                    .x
                    .try_cmp_with_work(&port.point.x, budget)?
                    .is_eq()
            {
                let same_spelling = match (&previous.spelling, &port.spelling) {
                    (
                        PortSpelling::Pole {
                            native_longitude: previous,
                        },
                        PortSpelling::Pole {
                            native_longitude: current,
                        },
                    ) => previous.to_bits() == current.to_bits(),
                    _ => false,
                };
                if !same_spelling || !previous.point.try_eq_with_work(&port.point, budget)? {
                    return Err(GeometryErrorKind::Invalid(
                        "native producer pole ports disagree on one chart occurrence".into(),
                    )
                    .into());
                }
                continue;
            }
            unique.push(port);
        }
        *known = unique;
    }
    for &(region, pole) in closures {
        let latitude = ExactRational::from_f64(frame.chart_pole_latitude(pole));
        let event = events
            .get(native_seam_vertex_index(events, &latitude, budget)?)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("native pole closure has no seam event".into())
            })?;
        if event.east.node != event.west.node
            || !event.latitude.try_cmp_with_work(&latitude, budget)?.is_eq()
        {
            return Err(GeometryErrorKind::Invalid(
                "native pole banks do not share their typed pole identity".into(),
            )
            .into());
        }
        let known = known_poles
            .iter()
            .find(|(known_pole, node, ..)| *known_pole == pole && *node == event.east.node)
            .map(|(_, _, _, ports)| ports)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("native pole closure has no incidence inventory".into())
            })?;
        let path_len = known
            .len()
            .checked_add(2)
            .ok_or(ResourceError::SizeOverflow)?;
        budget
            .charge_into_error(u64::try_from(path_len).map_err(|_| ResourceError::SizeOverflow)?)?;
        let mut path = Vec::with_capacity(path_len);
        let east = NativePortData {
            node: event.east.node,
            point: event.east.point.try_clone_with_work(budget)?,
            spelling: PortSpelling::Seam(ChartSide::East),
            source_record: None,
            occurrences: {
                budget.charge_into_error(1)?;
                vec![OccurrenceSource {
                    origin: VertexOrigin::Presentation,
                    source_trace: None,
                }]
            },
        };
        let west = NativePortData {
            node: event.west.node,
            point: event.west.point.try_clone_with_work(budget)?,
            spelling: PortSpelling::Seam(ChartSide::West),
            source_record: None,
            occurrences: {
                budget.charge_into_error(1)?;
                vec![OccurrenceSource {
                    origin: VertexOrigin::Presentation,
                    source_trace: None,
                }]
            },
        };
        match pole {
            ChartPole::North => {
                path.push(east);
                for port in known.iter().rev() {
                    path.push(port.try_clone_with_work(budget)?);
                }
                path.push(west);
            },
            ChartPole::South => {
                path.push(west);
                for port in known {
                    path.push(port.try_clone_with_work(budget)?);
                }
                path.push(east);
            },
        }
        let segment_count = path
            .len()
            .checked_sub(1)
            .ok_or(ResourceError::SizeOverflow)?;
        budget.charge_into_error(
            u64::try_from(segment_count).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        edges.reserve(segment_count);
        for pair in path.array_windows::<2>() {
            budget.charge_into_error(1)?;
            let mut endpoints = pair.iter();
            let start = endpoints.next().ok_or_else(|| {
                GeometryErrorKind::Invalid("native pole arc has no start port".into())
            })?;
            let end = endpoints.next().ok_or_else(|| {
                GeometryErrorKind::Invalid("native pole arc has no end port".into())
            })?;
            if start
                .point
                .x
                .try_cmp_with_work(&end.point.x, budget)?
                .is_eq()
            {
                return Err(GeometryErrorKind::Invalid(
                    "native pole presentation contains a zero-length chart arc".into(),
                )
                .into());
            }
            if has_existing_area_edge(region, start, end, budget)? {
                continue;
            }
            let vertices = [
                start.try_clone_with_work(budget)?,
                end.try_clone_with_work(budget)?,
            ];
            let kind = NativeAreaEdgeKind::Restriction(directed_chart_restriction_use(
                ChartRestriction::PolarCap { pole },
                &vertices,
            )?);
            edges.push(NativeAreaEdge {
                vertices,
                owner: NativeGraphEdgeOwner::Area { region },
                kind,
            });
        }
    }
    Ok(edges)
}
