use std::collections::BTreeMap;
use std::ops::Range;

use super::exact::{
    ExactChartPoint, ExactQuotientKey, ExactSourceRange, FallibleAvl, SourceEdgeId,
    compare_quotient_keys, compare_quotient_keys_with_work, quotient_key_for_chart_position,
    quotient_key_for_chart_position_with_work,
};
use super::overlay::QEmissionPlan;
use super::q::{
    CanonicalQNodeKey, ChartSpan, ChartSpanId, ExactGeographicTopology, GlobalFaceId, QDartId,
    QNodeId, QPortId,
};
use crate::error::{ResourceError, Result};
use crate::geographic::{
    ChartPole, ComputedEdgeIntent, ComputedSampleOrdinates, ComputedVertexIntent, GeographicFrame,
    Latitude, publish_computed_chain_with_vertex_intents_and_samples,
};
use crate::geometry::embedding_completion::{
    FrozenLiteralBuffers, SealedOutput, ShapeLiteralLayout,
};
pub(super) use crate::geometry::embedding_completion::{
    LiteralPoint, LiteralPolygon, LiteralSequence,
};
use crate::geometry::embedding_search::{EvaluationBudget, SearchAccount};
use crate::geometry::exact_arith::{
    retained_exact_native_site_with_work, split_rational_native_site_with_work,
};
#[cfg(test)]
use crate::geometry::ordinates::AxisValue;
use crate::geometry::ordinates::{
    NodeOrdinates, RequestRowId, RequestRowInput, RootCatalog, RootId, RootRange, RootRequest,
    SourceArcMetricAccess, TraceRows, TraceRowsBuilder, resolve_request_rows,
};
use crate::geometry::overlay::CertifiedOverlayOrder;
use crate::geometry::segments::try_sort_by_with_work;
use crate::geometry::tessellation::exact::{ExactDyadic, ExactRational};
use crate::geometry::{
    ChartMetric, CoordSeq, CoordinateAxes, ExpansionBudget, GeometryErrorKind, MOrdinate, Point,
    Polygon, Ring, Shape, Work, ZOrdinate,
};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) struct EmissionNodeId(pub u32);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) struct EmissionPortId(pub u32);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) struct EmissionChordId(pub u32);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) struct ChartPartId(pub u32);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) struct NativeFaceId(pub u32);

#[derive(Clone, Debug)]
pub(super) enum NodeAuthority {
    Source(QNodeId),
    Computed(ExactQuotientKey),
}

#[derive(Clone, Debug)]
pub(super) enum PortSpelling {
    Interior { periods: i64 },
    Seam(ChartSide),
    Pole { native_longitude: f64 },
}

#[derive(Clone, Debug)]
pub(super) enum VertexOrigin {
    SourceNode(QNodeId),
    SourceSample,
    Presentation,
}

#[derive(Clone, Debug)]
pub(super) struct EmissionNode {
    pub(super) authority: NodeAuthority,
}

#[derive(Clone)]
struct IndexedEmissionNode {
    key: ExactQuotientKey,
    node: EmissionNodeId,
}

pub(super) struct EmissionNodeRegistry {
    nodes: Vec<EmissionNode>,
    source_nodes: Vec<Option<EmissionNodeId>>,
    by_exact_key: FallibleAvl<IndexedEmissionNode>,
}

impl EmissionNodeRegistry {
    pub(super) fn with_sources(
        topology: &ExactGeographicTopology,
        live_nodes: &[QNodeId],
        work: &mut impl Work,
    ) -> Result<Self> {
        work.charge(
            u64::try_from(topology.q.nodes.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut registry = Self {
            nodes: Vec::with_capacity(live_nodes.len()),
            source_nodes: vec![None; topology.q.nodes.len()],
            by_exact_key: FallibleAvl::with_capacity(live_nodes.len()),
        };
        for &source in live_nodes {
            work.charge(1)?;
            let key = match topology.canonical_node_key(source)? {
                CanonicalQNodeKey::Chart(key) => key.clone(),
                CanonicalQNodeKey::Cut {
                    longitude,
                    latitude,
                } => quotient_key_for_chart_position(
                    topology.sources.frame,
                    &ExactChartPoint::from_rationals(longitude.clone(), latitude.clone()),
                )?,
            };
            let node = registry.intern(key, NodeAuthority::Source(source), work)?;
            let slot = registry
                .source_nodes
                .get_mut(source.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("source emission node is unknown".into())
                })?;
            if slot.replace(node).is_some() {
                return Err(GeometryErrorKind::Invalid(
                    "source emission node is registered twice".into(),
                )
                .into());
            }
        }
        Ok(registry)
    }

    pub(super) fn source_node(&self, node: QNodeId) -> Result<EmissionNodeId> {
        let source = self.source_nodes.get(node.0 as usize).copied().flatten();
        source.ok_or_else(|| {
            GeometryErrorKind::Invalid("Q node is outside the emitted source inventory".into())
                .into()
        })
    }

    pub(super) fn intern_computed(
        &mut self,
        frame: GeographicFrame,
        point: ExactChartPoint,
        work: &mut impl Work,
    ) -> Result<EmissionNodeId> {
        work.charge(1)?;
        let key = quotient_key_for_chart_position(frame, &point)?;
        self.intern(key.clone(), NodeAuthority::Computed(key), work)
    }

    pub(super) fn nodes(&self) -> &[EmissionNode] {
        &self.nodes
    }

    fn intern(
        &mut self,
        key: ExactQuotientKey,
        authority: NodeAuthority,
        work: &mut impl Work,
    ) -> Result<EmissionNodeId> {
        let probe = IndexedEmissionNode {
            key: key.clone(),
            node: EmissionNodeId(0),
        };
        if let Some(previous) = self.by_exact_key.get(&probe, &mut |left, right| {
            work.charge(1)?;
            compare_quotient_keys(&left.key, &right.key)
        })? {
            return Ok(previous.node);
        }
        let node = EmissionNodeId(
            u32::try_from(self.nodes.len()).map_err(|_| ResourceError::SizeOverflow)?,
        );
        self.nodes.push(EmissionNode { authority });
        self.by_exact_key
            .insert(IndexedEmissionNode { key, node }, &mut |left, right| {
                work.charge(1)?;
                compare_quotient_keys(&left.key, &right.key)
            })?;
        Ok(node)
    }
}

#[derive(Clone, Debug)]
pub(super) enum PortPosition {
    Source { spelling: PortSpelling },
    Projected { spelling: PortSpelling },
}

#[derive(Clone, Debug)]
pub(super) struct EmissionPort {
    pub(super) node: EmissionNodeId,
    pub(super) position: PortPosition,
    pub(super) source_record: Option<Point>,
    occurrence_range: Range<usize>,
}

#[derive(Clone, Debug)]
pub(super) struct OccurrenceSource {
    pub(super) origin: VertexOrigin,
    pub(super) source_trace: Option<TraceRequestLocation>,
}

#[derive(Clone, Debug)]
pub(super) struct WrittenOccurrence {
    pub(super) origin: VertexOrigin,
    pub(super) source_trace: Option<TraceRequestLocation>,
    pub(super) request_row: RequestRowId,
}

#[derive(Clone, Debug)]
pub(super) struct EmissionChord {
    pub(super) ports: [EmissionPortId; 2],
    pub(super) recipe: Option<FragmentRecipeId>,
}

#[derive(Clone, Debug)]
pub(super) struct EmissionPart {
    pub(super) region: usize,
    pub(super) face: NativeFaceId,
    pub(super) shell: Vec<EmissionChordId>,
    pub(super) holes: Vec<Vec<EmissionChordId>>,
}

#[derive(Clone)]
pub(super) struct NativePortData {
    pub(super) node: EmissionNodeId,
    pub(super) point: ExactChartPoint,
    pub(super) spelling: PortSpelling,
    pub(super) source_record: Option<Point>,
    pub(super) occurrences: Vec<OccurrenceSource>,
}

fn exact_chart_points_equal_with_work(
    left: &ExactChartPoint,
    right: &ExactChartPoint,
    work: &mut impl Work,
) -> Result<bool> {
    Ok(left.x.try_cmp_with_work(&right.x, work)?.is_eq()
        && left.y.try_cmp_with_work(&right.y, work)?.is_eq())
}

fn validate_native_port_image(
    frame: GeographicFrame,
    left: &NativePortData,
    right: &NativePortData,
    work: &mut impl Work,
) -> Result<()> {
    let left_key = quotient_key_for_chart_position_with_work(frame, &left.point, work)?;
    let right_key = quotient_key_for_chart_position_with_work(frame, &right.point, work)?;
    if !compare_quotient_keys_with_work(&left_key, &right_key, work)?.is_eq() {
        return Err(GeometryErrorKind::Invalid(
            "native output vertex aliases disagree on exact quotient image".into(),
        )
        .into());
    }
    match (&left.spelling, &right.spelling, &left_key, &right_key) {
        (
            PortSpelling::Interior {
                periods: left_periods,
            },
            PortSpelling::Interior {
                periods: right_periods,
            },
            ExactQuotientKey::Coordinate(_),
            ExactQuotientKey::Coordinate(_),
        ) => {
            let longitude = frame.longitude();
            let (left_native, left_lift) = split_rational_native_site_with_work(
                &left.point.x,
                longitude.period(),
                longitude.half_period(),
                work,
            )?;
            let (right_native, right_lift) = split_rational_native_site_with_work(
                &right.point.x,
                longitude.period(),
                longitude.half_period(),
                work,
            )?;
            if !left_native
                .as_ref()
                .try_cmp_with_work(right_native.as_ref(), work)?
                .is_eq()
                || !left_lift
                    .cmp_with_work(&ExactDyadic::from_i64(*left_periods), work)?
                    .is_eq()
                || !right_lift
                    .cmp_with_work(&ExactDyadic::from_i64(*right_periods), work)?
                    .is_eq()
                || (left_periods == right_periods
                    && !exact_chart_points_equal_with_work(&left.point, &right.point, work)?)
            {
                return Err(GeometryErrorKind::Invalid(
                    "native interior aliases disagree on their exact periodic image".into(),
                )
                .into());
            }
        },
        (
            PortSpelling::Seam(left_side),
            PortSpelling::Seam(right_side),
            ExactQuotientKey::Seam(_),
            ExactQuotientKey::Seam(_),
        ) if left_side == right_side => {
            let longitude = ExactRational::from_f64(match left_side {
                ChartSide::East => frame.longitude().half_period(),
                ChartSide::West => -frame.longitude().half_period(),
            });
            if !left.point.x.try_cmp_with_work(&longitude, work)?.is_eq()
                || !right.point.x.try_cmp_with_work(&longitude, work)?.is_eq()
            {
                return Err(GeometryErrorKind::Invalid(
                    "native seam aliases disagree on their assigned bank image".into(),
                )
                .into());
            }
        },
        (
            PortSpelling::Seam(left_side),
            PortSpelling::Seam(right_side),
            left_pole @ (ExactQuotientKey::NorthPole | ExactQuotientKey::SouthPole),
            right_pole @ (ExactQuotientKey::NorthPole | ExactQuotientKey::SouthPole),
        ) if left_side == right_side
            && compare_quotient_keys_with_work(left_pole, right_pole, work)?.is_eq() =>
        {
            let longitude = ExactRational::from_f64(match left_side {
                ChartSide::East => frame.longitude().half_period(),
                ChartSide::West => -frame.longitude().half_period(),
            });
            if !left.point.x.try_cmp_with_work(&longitude, work)?.is_eq()
                || !right.point.x.try_cmp_with_work(&longitude, work)?.is_eq()
                || !exact_chart_points_equal_with_work(&left.point, &right.point, work)?
            {
                return Err(GeometryErrorKind::Invalid(
                    "native pole-bank aliases disagree on their exact chart occurrence".into(),
                )
                .into());
            }
        },
        (
            PortSpelling::Pole {
                native_longitude: left_longitude,
            },
            PortSpelling::Pole {
                native_longitude: right_longitude,
            },
            left_pole @ (ExactQuotientKey::NorthPole | ExactQuotientKey::SouthPole),
            right_pole @ (ExactQuotientKey::NorthPole | ExactQuotientKey::SouthPole),
        ) if compare_quotient_keys_with_work(left_pole, right_pole, work)?.is_eq() => {
            let same_spelling = left_longitude.to_bits() == right_longitude.to_bits();
            let left_longitude = ExactRational::from_f64(*left_longitude);
            let right_longitude = ExactRational::from_f64(*right_longitude);
            let (left_native, _) = split_rational_native_site_with_work(
                &left_longitude,
                frame.longitude().period(),
                frame.longitude().half_period(),
                work,
            )?;
            let (right_native, _) = split_rational_native_site_with_work(
                &right_longitude,
                frame.longitude().period(),
                frame.longitude().half_period(),
                work,
            )?;
            let (left_point_native, _) = split_rational_native_site_with_work(
                &left.point.x,
                frame.longitude().period(),
                frame.longitude().half_period(),
                work,
            )?;
            let (right_point_native, _) = split_rational_native_site_with_work(
                &right.point.x,
                frame.longitude().period(),
                frame.longitude().half_period(),
                work,
            )?;
            if !same_spelling
                || !left_native
                    .as_ref()
                    .try_cmp_with_work(left_point_native.as_ref(), work)?
                    .is_eq()
                || !right_native
                    .as_ref()
                    .try_cmp_with_work(right_point_native.as_ref(), work)?
                    .is_eq()
            {
                return Err(GeometryErrorKind::Invalid(
                    "native pole aliases disagree on their typed identity or explicit spelling"
                        .into(),
                )
                .into());
            }
        },
        _ => {
            return Err(GeometryErrorKind::Invalid(
                "native output aliases use incompatible chart spellings".into(),
            )
            .into());
        },
    }
    Ok(())
}

pub(super) fn native_source_record_quotient_key(
    frame: GeographicFrame,
    record: Point,
    assigned: &ExactChartPoint,
    work: &mut impl Work,
) -> Result<ExactQuotientKey> {
    let record_point = ExactChartPoint::from_rationals(
        ExactRational::from_f64(record.x),
        ExactRational::from_f64(Latitude::from_admitted(frame, record.y).get()),
    );
    let record_key = quotient_key_for_chart_position_with_work(frame, &record_point, work)?;
    let assigned_key = quotient_key_for_chart_position_with_work(frame, assigned, work)?;
    let longitude = frame.longitude();
    let (record_native, _) = split_rational_native_site_with_work(
        &record_point.x,
        longitude.period(),
        longitude.half_period(),
        work,
    )?;
    let (assigned_native, _) = split_rational_native_site_with_work(
        &assigned.x,
        longitude.period(),
        longitude.half_period(),
        work,
    )?;
    if !compare_quotient_keys_with_work(&record_key, &assigned_key, work)?.is_eq()
        || !record_native
            .as_ref()
            .try_cmp_with_work(assigned_native.as_ref(), work)?
            .is_eq()
    {
        return Err(GeometryErrorKind::Invalid(
            "authentic native source record differs from its assigned endpoint image".into(),
        )
        .into());
    }
    Ok(record_key)
}

fn validate_native_source_record(
    frame: GeographicFrame,
    port: &NativePortData,
    record: Point,
    work: &mut impl Work,
) -> Result<()> {
    let record_key = native_source_record_quotient_key(frame, record, &port.point, work)?;
    let longitude = frame.longitude();
    match &port.spelling {
        PortSpelling::Seam(side) => {
            let expected = match side {
                ChartSide::East => longitude.half_period(),
                ChartSide::West => -longitude.half_period(),
            };
            if record.x.to_bits() != expected.to_bits() {
                return Err(GeometryErrorKind::Invalid(
                    "authentic native source record changes its seam-bank spelling".into(),
                )
                .into());
            }
        },
        PortSpelling::Pole { native_longitude } => {
            if record.x.to_bits() != native_longitude.to_bits()
                || !matches!(
                    record_key,
                    ExactQuotientKey::NorthPole | ExactQuotientKey::SouthPole
                )
            {
                return Err(GeometryErrorKind::Invalid(
                    "authentic native pole record changes its explicit spelling".into(),
                )
                .into());
            }
        },
        PortSpelling::Interior { .. } => {
            if !matches!(record_key, ExactQuotientKey::Coordinate(_)) {
                return Err(GeometryErrorKind::Invalid(
                    "authentic native interior record is not a coordinate image".into(),
                )
                .into());
            }
        },
    }
    Ok(())
}

fn native_source_record_spells_chart_point(
    frame: GeographicFrame,
    record: Point,
    point: &ExactChartPoint,
    work: &mut impl Work,
) -> Result<bool> {
    let admitted_y = Latitude::from_admitted(frame, record.y).get();
    Ok(ExactRational::from_f64(record.x)
        .try_cmp_with_work(&point.x, work)?
        .is_eq()
        && ExactRational::from_f64(admitted_y)
            .try_cmp_with_work(&point.y, work)?
            .is_eq())
}

pub(super) fn merge_native_port_data(
    frame: GeographicFrame,
    left: &NativePortData,
    right: &NativePortData,
    work: &mut impl Work,
) -> Result<NativePortData> {
    if left.node != right.node {
        return Err(GeometryErrorKind::Invalid(
            "native output vertex aliases disagree on quotient identity".into(),
        )
        .into());
    }
    validate_native_port_image(frame, left, right, work)?;
    let spelling = match (&left.spelling, &right.spelling) {
        (left, _) => left.clone(),
    };
    for port in [left, right] {
        if let Some(record) = port.source_record {
            validate_native_source_record(frame, port, record, work)?;
        }
    }
    // Retain the authentic record for the chosen chart lift; ambiguous raw bits
    // are not resolved by the order in which graph contributors were visited.
    let source_record = match (left.source_record, right.source_record) {
        (Some(left), Some(right))
            if left.x.to_bits() == right.x.to_bits() && left.y.to_bits() == right.y.to_bits() =>
        {
            Some(left)
        },
        (Some(left_record), Some(right_record)) => {
            let left_spelling =
                native_source_record_spells_chart_point(frame, left_record, &left.point, work)?;
            let right_spelling =
                native_source_record_spells_chart_point(frame, right_record, &left.point, work)?;
            match (left_spelling, right_spelling) {
                (true, false) => Some(left_record),
                (false, true) => Some(right_record),
                (false, false) => None,
                (true, true) => {
                    return Err(GeometryErrorKind::Invalid(
                        "native aliases carry conflicting exact source spellings".into(),
                    )
                    .into());
                },
            }
        },
        (Some(record), None) | (None, Some(record)) => Some(record),
        (None, None) => None,
    };
    let occurrence_count = left
        .occurrences
        .len()
        .checked_add(right.occurrences.len())
        .ok_or(ResourceError::SizeOverflow)?;
    work.charge(u64::try_from(occurrence_count).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut occurrences = Vec::with_capacity(occurrence_count);
    occurrences.extend_from_slice(&left.occurrences);
    occurrences.extend_from_slice(&right.occurrences);
    Ok(NativePortData {
        node: left.node,
        point: left.point.clone(),
        spelling,
        source_record,
        occurrences,
    })
}

pub(super) struct NativeAreaEdge {
    pub(super) vertices: [NativePortData; 2],
    pub(super) owner: NativeGraphEdgeOwner,
    pub(super) kind: NativeAreaEdgeKind,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum NativeGraphEdgeOwner {
    Area { region: usize },
    Line,
}

pub(super) enum NativeAreaEdgeKind {
    Producer {
        recipe: FragmentRecipeId,
        trace_edge: usize,
        producer_start: ExactRational,
        producer_end: ExactRational,
        trace_ranges: Vec<(usize, RootRange)>,
        known_spans: Vec<NativeSourceSpanUse>,
    },
    SeamConnector,
    PolePresentation {
        pole: ChartPole,
    },
    WholeSphereCover {
        cell: u8,
    },
}

#[derive(Clone)]
pub(super) struct NativeSourceSpanUse {
    pub(super) span: ChartSpanId,
    pub(super) owner: usize,
    pub(super) source_edge: SourceEdgeId,
    pub(super) source_lift: i64,
    pub(super) source_range: ExactSourceRange,
    pub(super) producer_range: ExactSourceRange,
    pub(super) span_range: ExactSourceRange,
    pub(super) nodes: [QNodeId; 2],
    pub(super) ports: [QPortId; 2],
    pub(super) face_germs: Option<[NativeSourceFaceGerm; 2]>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct NativeSourceFaceGerm {
    pub(super) dart: QDartId,
    pub(super) face: GlobalFaceId,
    pub(super) selected_root: Option<usize>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) struct AssignedSite {
    pub(super) longitude: crate::geographic::RetainedLongitudeSite,
    pub(super) latitude: f64,
}

pub(super) struct SelectedNativeSeamBand {
    pub(super) low: ExactRational,
    pub(super) high: ExactRational,
    pub(super) west_region: Option<usize>,
    pub(super) east_region: Option<usize>,
}

pub(super) struct NativeSeamVertices {
    pub(super) latitude: ExactRational,
    pub(super) east: NativePortData,
    pub(super) west: NativePortData,
}

#[derive(Clone)]
pub(super) struct NativeSeamEvent {
    pub(super) latitude: ExactRational,
    pub(super) output_vertex: bool,
}

fn native_seam_vertex_index(
    events: &[NativeSeamVertices],
    latitude: &ExactRational,
    work: &mut impl Work,
) -> Result<usize> {
    let mut low = 0_usize;
    let mut high = events.len();
    while low < high {
        let middle = low + (high - low) / 2;
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

pub(super) fn native_seam_edges(
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
    budget.charge(u64::try_from(work).map_err(|_| ResourceError::SizeOverflow)?)?;
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
            edges.push(NativeAreaEdge {
                vertices: [low.east.clone(), high.east.clone()],
                owner: NativeGraphEdgeOwner::Area { region },
                kind: NativeAreaEdgeKind::SeamConnector,
            });
        }
        if let Some(region) = band.west_region
            && !has_existing_area_edge(region, &high.west, &low.west, budget)?
        {
            edges.push(NativeAreaEdge {
                vertices: [high.west.clone(), low.west.clone()],
                owner: NativeGraphEdgeOwner::Area { region },
                kind: NativeAreaEdgeKind::SeamConnector,
            });
        }
    }
    Ok(edges)
}

fn native_pole_presentation_occurrence(
    source: &NativePortData,
    node: EmissionNodeId,
    latitude: &ExactRational,
    west: &ExactRational,
    half: &ExactRational,
    budget: &mut EvaluationBudget,
) -> Result<Option<NativePortData>> {
    budget.charge(1)?;
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
    budget.charge(1)?;
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

pub(super) fn native_pole_edges(
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
    budget.charge(u64::try_from(closures.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
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
        budget.charge(u64::try_from(closures.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
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
        budget.charge(1)?;
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
            budget.charge(1)?;
            left.point.x.try_cmp_with_work(&right.point.x, budget)
        })?;
        let known_count = known.len();
        budget.charge(u64::try_from(known_count).map_err(|_| ResourceError::SizeOverflow)?)?;
        let mut unique = Vec::<NativePortData>::with_capacity(known_count);
        for port in std::mem::take(known) {
            budget.charge(1)?;
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
        budget.charge(u64::try_from(path_len).map_err(|_| ResourceError::SizeOverflow)?)?;
        let mut path = Vec::with_capacity(path_len);
        let east = NativePortData {
            node: event.east.node,
            point: event.east.point.try_clone_with_work(budget)?,
            spelling: PortSpelling::Seam(ChartSide::East),
            source_record: None,
            occurrences: vec![OccurrenceSource {
                origin: VertexOrigin::Presentation,
                source_trace: None,
            }],
        };
        let west = NativePortData {
            node: event.west.node,
            point: event.west.point.try_clone_with_work(budget)?,
            spelling: PortSpelling::Seam(ChartSide::West),
            source_record: None,
            occurrences: vec![OccurrenceSource {
                origin: VertexOrigin::Presentation,
                source_trace: None,
            }],
        };
        match pole {
            ChartPole::North => {
                path.push(east);
                path.extend(known.iter().rev().cloned());
                path.push(west);
            },
            ChartPole::South => {
                path.push(west);
                path.extend(known.iter().cloned());
                path.push(east);
            },
        }
        let segment_count = path
            .len()
            .checked_sub(1)
            .ok_or(ResourceError::SizeOverflow)?;
        budget.charge(u64::try_from(segment_count).map_err(|_| ResourceError::SizeOverflow)?)?;
        edges.reserve(segment_count);
        for pair in path.array_windows::<2>() {
            budget.charge(1)?;
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
            edges.push(NativeAreaEdge {
                vertices: [start.clone(), end.clone()],
                owner: NativeGraphEdgeOwner::Area { region },
                kind: NativeAreaEdgeKind::PolePresentation { pole },
            });
        }
    }
    Ok(edges)
}

pub(super) struct ExpandedEmissionPlan<'topology> {
    pub(super) topology: &'topology ExactGeographicTopology,
    pub(super) selected_face_roots: Vec<Option<usize>>,
    pub(super) node_registry: EmissionNodeRegistry,
    pub(super) ports: Vec<EmissionPort>,
    pub(super) occurrences: Vec<WrittenOccurrence>,
    pub(super) chords: Vec<EmissionChord>,
    pub(super) parts: Vec<EmissionPart>,
    pub(super) lines: Vec<Vec<EmissionChordId>>,
    pub(super) points: Vec<EmissionPortId>,
    pub(super) whole_sphere: bool,
}

pub(super) struct EmissionBuilder<'topology> {
    topology: &'topology ExactGeographicTopology,
    selected_face_roots: Vec<Option<usize>>,
    nodes: EmissionNodeRegistry,
    ports: Vec<EmissionPort>,
    occurrences: Vec<WrittenOccurrence>,
    chords: Vec<EmissionChord>,
    parts: Vec<EmissionPart>,
    lines: Vec<Vec<EmissionChordId>>,
    points: Vec<EmissionPortId>,
    whole_sphere: bool,
}

impl<'topology> EmissionBuilder<'topology> {
    pub(super) fn new(
        topology: &'topology ExactGeographicTopology,
        selected: &QEmissionPlan,
        live_nodes: &[QNodeId],
        whole_sphere: bool,
        work: &mut impl Work,
    ) -> Result<Self> {
        Ok(Self {
            topology,
            selected_face_roots: selected.selected_face_roots.clone(),
            nodes: EmissionNodeRegistry::with_sources(topology, live_nodes, work)?,
            ports: Vec::new(),
            occurrences: Vec::new(),
            chords: Vec::new(),
            parts: Vec::new(),
            lines: Vec::new(),
            points: Vec::new(),
            whole_sphere,
        })
    }

    pub(super) fn source_port(
        &mut self,
        port: QPortId,
        origin: VertexOrigin,
        source_record: Option<Point>,
        spelling: PortSpelling,
    ) -> Result<EmissionPortId> {
        let source =
            self.topology.q.ports.get(port.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("source emission port is unknown".into())
            })?;
        if !matches!(&origin, VertexOrigin::SourceNode(node) if *node == source.node) {
            return Err(GeometryErrorKind::Invalid(
                "borrowed source port must retain its source-node origin".into(),
            )
            .into());
        }
        let node = self.nodes.source_node(source.node)?;
        self.validate_port_origin(node, &origin)?;
        self.push_port(
            EmissionPort {
                node,
                position: PortPosition::Source { spelling },
                source_record,
                occurrence_range: 0..0,
            },
            vec![OccurrenceSource {
                origin,
                source_trace: None,
            }],
        )
    }

    pub(super) fn source_node(&self, node: QNodeId) -> Result<EmissionNodeId> {
        self.nodes.source_node(node)
    }

    pub(super) fn reserve_publication_nodes(
        &mut self,
        samples: &[PublishedSample],
        work: &mut impl Work,
    ) -> Result<Vec<EmissionNodeId>> {
        let mut nodes = Vec::with_capacity(samples.len());
        for sample in samples {
            work.charge(1)?;
            nodes.push(self.nodes.intern_computed(
                self.topology.sources.frame,
                sample.exact_point.try_clone_with_work(work)?,
                work,
            )?);
        }
        Ok(nodes)
    }

    pub(super) fn intern_point(
        &mut self,
        point: &ExactChartPoint,
        work: &mut impl Work,
    ) -> Result<EmissionNodeId> {
        self.nodes.intern_computed(
            self.topology.sources.frame,
            point.try_clone_with_work(work)?,
            work,
        )
    }

    pub(super) fn chord(
        &mut self,
        ports: [EmissionPortId; 2],
        recipe: Option<FragmentRecipeId>,
    ) -> Result<EmissionChordId> {
        for port in ports {
            self.ports.get(port.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("emission chord port is unknown".into())
            })?;
        }
        let id = EmissionChordId(
            u32::try_from(self.chords.len()).map_err(|_| ResourceError::SizeOverflow)?,
        );
        self.chords.push(EmissionChord { ports, recipe });
        Ok(id)
    }

    pub(super) fn part(
        &mut self,
        face: NativeFaceId,
        region: usize,
        shell: Vec<EmissionChordId>,
        holes: Vec<Vec<EmissionChordId>>,
    ) -> Result<ChartPartId> {
        for chord in shell.iter().chain(holes.iter().flatten()) {
            self.chords.get(chord.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("emission part chord is unknown".into())
            })?;
        }
        let id =
            ChartPartId(u32::try_from(self.parts.len()).map_err(|_| ResourceError::SizeOverflow)?);
        self.parts.push(EmissionPart {
            region,
            face,
            shell,
            holes,
        });
        Ok(id)
    }

    pub(super) fn line(&mut self, chords: Vec<EmissionChordId>) -> Result<()> {
        for chord in &chords {
            self.chords.get(chord.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("emission line chord is unknown".into())
            })?;
        }
        self.lines.push(chords);
        Ok(())
    }

    pub(super) fn point(&mut self, port: EmissionPortId) -> Result<()> {
        self.ports
            .get(port.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("emission point port is unknown".into()))?;
        self.points.push(port);
        Ok(())
    }

    pub(super) fn port(&mut self, data: NativePortData) -> Result<EmissionPortId> {
        for occurrence in &data.occurrences {
            self.validate_port_origin(data.node, &occurrence.origin)?;
        }
        self.push_port(
            EmissionPort {
                node: data.node,
                position: PortPosition::Projected {
                    spelling: data.spelling,
                },
                source_record: data.source_record,
                occurrence_range: 0..0,
            },
            data.occurrences,
        )
    }

    pub(super) const fn nodes(&self) -> &EmissionNodeRegistry {
        &self.nodes
    }

    pub(super) fn finish(self) -> ExpandedEmissionPlan<'topology> {
        ExpandedEmissionPlan {
            topology: self.topology,
            selected_face_roots: self.selected_face_roots,
            node_registry: self.nodes,
            ports: self.ports,
            occurrences: self.occurrences,
            chords: self.chords,
            parts: self.parts,
            lines: self.lines,
            points: self.points,
            whole_sphere: self.whole_sphere,
        }
    }

    fn push_port(
        &mut self,
        mut port: EmissionPort,
        occurrences: Vec<OccurrenceSource>,
    ) -> Result<EmissionPortId> {
        let start = self.occurrences.len();
        let end = start
            .checked_add(occurrences.len())
            .ok_or(ResourceError::SizeOverflow)?;
        for (offset, occurrence) in occurrences.into_iter().enumerate() {
            let request_row = start
                .checked_add(offset)
                .ok_or(ResourceError::SizeOverflow)?;
            self.occurrences.push(WrittenOccurrence {
                origin: occurrence.origin,
                source_trace: occurrence.source_trace,
                request_row: RequestRowId::from_index(request_row)?,
            });
        }
        port.occurrence_range = start..end;
        let id = EmissionPortId(
            u32::try_from(self.ports.len()).map_err(|_| ResourceError::SizeOverflow)?,
        );
        self.ports.push(port);
        Ok(id)
    }

    fn validate_port_origin(&self, node: EmissionNodeId, origin: &VertexOrigin) -> Result<()> {
        let VertexOrigin::SourceNode(source) = origin else {
            return Ok(());
        };
        let Some(EmissionNode {
            authority: NodeAuthority::Source(authority),
        }) = self.nodes.nodes().get(node.0 as usize)
        else {
            return Err(GeometryErrorKind::Invalid(
                "source-origin port references a computed emission node".into(),
            )
            .into());
        };
        if authority != source {
            return Err(GeometryErrorKind::Invalid(
                "source-origin port disagrees with its emission node authority".into(),
            )
            .into());
        }
        Ok(())
    }
}

pub(super) struct CompletedNativeDrawing {
    pub(super) sealed: SealedOutput<EmissionPortId>,
    pub(super) line_traces: Vec<TracedLine>,
}

pub(super) struct NativeFrozenDrawing {
    pub(super) buffers: FrozenLiteralBuffers<EmissionPortId>,
    pub(super) line_traces: Vec<TracedLine>,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) enum QSourceImage {
    /// A computed or respelled port, authenticated by its checked chart image.
    Port(QPortId),
    /// Retained source records. Their locator lets the final sink recheck the
    /// writer's XY bit law without deriving an expectation from that sink.
    SourcePoint(u32),
    SourceRingStart(SourceEdgeId),
    SourceRingEnd(SourceEdgeId),
    CollapsedStart(SourceEdgeId),
    CollapsedEnd(SourceEdgeId),
    PublicationSample(u32),
}

pub(super) struct SourceFrozenDrawing {
    pub(super) buffers: FrozenLiteralBuffers<QSourceImage>,
}

#[cfg(test)]
pub(super) type NativePolygonLiteralUses = LiteralPolygon<EmissionPortId>;
pub(super) type NativeLiteralUses = ShapeLiteralLayout<EmissionPortId>;

pub(super) fn native_literal_uses_in_output_order(
    literal_uses: NativeLiteralUses,
    line_traces: Vec<TracedLine>,
    order: CertifiedOverlayOrder,
    work: &mut impl Work,
) -> Result<(NativeLiteralUses, Vec<TracedLine>)> {
    if literal_uses.lines.len() != line_traces.len() {
        return Err(GeometryErrorKind::Invalid(
            "native line traces and literal line rows lost their shared order".into(),
        )
        .into());
    }
    let line_traces = reorder_literal_rows(line_traces, &order.lines, work)?;
    let literal_uses = literal_uses.in_output_order(order, work)?;
    Ok((literal_uses, line_traces))
}

fn reorder_literal_rows<T>(rows: Vec<T>, order: &[usize], work: &mut impl Work) -> Result<Vec<T>> {
    if rows.len() != order.len() {
        return Err(GeometryErrorKind::Invalid("literal output order omits a row".into()).into());
    }
    work.charge(u64::try_from(rows.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut rows = rows.into_iter().map(Some).collect::<Vec<_>>();
    work.charge(u64::try_from(order.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut output = Vec::with_capacity(order.len());
    for &index in order {
        work.charge(1)?;
        let slot = rows.get_mut(index).ok_or_else(|| {
            GeometryErrorKind::Invalid("literal output order references an unknown row".into())
        })?;
        output.push(slot.take().ok_or_else(|| {
            GeometryErrorKind::Invalid("literal output order repeats a row".into())
        })?);
    }
    Ok(output)
}

pub(super) struct NativeMaterializedParts {
    pub(super) axes: CoordinateAxes,
    pub(super) points: Vec<Point>,
    pub(super) lines: Vec<CoordSeq>,
    pub(super) polygons: Vec<Polygon>,
    pub(super) line_traces: Vec<TracedLine>,
    pub(super) literal_uses: NativeLiteralUses,
}

impl NativeMaterializedParts {
    #[expect(
        clippy::suspicious_operation_groupings,
        reason = "LiteralSequence::images is the one-to-one source map for each ring's coordinates"
    )]
    pub(super) fn validate_literal_uses(&self) -> Result<()> {
        if self.literal_uses.points.len() != self.points.len()
            || self.literal_uses.lines.len() != self.lines.len()
            || self.literal_uses.polygons.len() != self.polygons.len()
        {
            return Err(GeometryErrorKind::Invalid(
                "native literal-use rows do not match their output parts".into(),
            )
            .into());
        }
        for (point, uses) in self.points.iter().zip(&self.literal_uses.points) {
            if CoordinateAxes::from_point(*point) != uses.axes {
                return Err(GeometryErrorKind::Invalid(
                    "native point axes differ from its logical output policy".into(),
                )
                .into());
            }
        }
        for (line, uses) in self.lines.iter().zip(&self.literal_uses.lines) {
            if line.len() != uses.images.len() || line.axes() != uses.axes {
                return Err(GeometryErrorKind::Invalid(
                    "native line literal-use rows do not match their coordinates".into(),
                )
                .into());
            }
        }
        for (polygon, uses) in self.polygons.iter().zip(&self.literal_uses.polygons) {
            if polygon.shell.coords().len() != uses.shell.images.len()
                || polygon.holes.len() != uses.holes.len()
                || polygon.shell.coords().axes() != uses.axes
                || uses.shell.axes != uses.axes
                || uses.holes.iter().any(|hole| hole.axes != uses.axes)
            {
                return Err(GeometryErrorKind::Invalid(
                    "native polygon literal-use rows do not match their rings".into(),
                )
                .into());
            }
            for (hole, uses) in polygon.holes.iter().zip(&uses.holes) {
                if hole.coords().len() != uses.images.len() {
                    return Err(GeometryErrorKind::Invalid(
                        "native hole literal-use rows do not match their coordinates".into(),
                    )
                    .into());
                }
            }
        }
        Ok(())
    }
}

struct NativeChordPoints {
    points: Vec<Point>,
    ports: Vec<EmissionPortId>,
}

pub(super) fn materialize_native_parts(
    plan: &ExpandedEmissionPlan<'_>,
    sites: &[AssignedSite],
    ordinates: &[NodeOrdinates],
    axes: CoordinateAxes,
    line_traces: &[TracedLine],
    budget: &mut EvaluationBudget,
) -> Result<NativeMaterializedParts> {
    if line_traces.len() != plan.lines.len() {
        return Err(GeometryErrorKind::Invalid(
            "native line traces do not match the written line table".into(),
        )
        .into());
    }
    budget.charge(u64::try_from(line_traces.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
    let line_traces = line_traces.to_vec();
    budget.charge(u64::try_from(plan.points.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut points = Vec::with_capacity(plan.points.len());
    budget.charge(u64::try_from(plan.points.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut point_uses = Vec::with_capacity(plan.points.len());
    for (index, &port) in plan.points.iter().enumerate() {
        budget.charge(1)?;
        points.push(native_port_point(plan, sites, ordinates, port, axes)?);
        point_uses.push(LiteralPoint {
            output: u32::try_from(index).map_err(|_| ResourceError::SizeOverflow)?,
            axes,
            image: port,
        });
    }
    budget.charge(u64::try_from(plan.lines.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut lines = Vec::with_capacity(plan.lines.len());
    budget.charge(u64::try_from(plan.lines.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut line_uses = Vec::with_capacity(plan.lines.len());
    for (index, chords) in plan.lines.iter().enumerate() {
        let walk = native_chord_points(plan, sites, ordinates, chords, axes, false, budget)?;
        budget
            .charge(u64::try_from(walk.points.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
        lines.push(CoordSeq::try_from_points(&walk.points)?);
        line_uses.push(LiteralSequence {
            output: u32::try_from(index).map_err(|_| ResourceError::SizeOverflow)?,
            axes,
            images: walk.ports,
        });
    }
    budget.charge(u64::try_from(plan.parts.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut polygons = Vec::with_capacity(plan.parts.len());
    budget.charge(u64::try_from(plan.parts.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut polygon_uses = Vec::with_capacity(plan.parts.len());
    for (part_index, part) in plan.parts.iter().enumerate() {
        let output = u32::try_from(part_index).map_err(|_| ResourceError::SizeOverflow)?;
        let shell_walk =
            native_chord_points(plan, sites, ordinates, &part.shell, axes, true, budget)?;
        budget.charge(
            u64::try_from(shell_walk.points.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let shell = Ring::try_from_closed(CoordSeq::try_from_points(&shell_walk.points)?)?;
        budget.charge(u64::try_from(part.holes.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
        let mut holes = Vec::with_capacity(part.holes.len());
        budget.charge(u64::try_from(part.holes.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
        let mut hole_uses = Vec::with_capacity(part.holes.len());
        for hole in &part.holes {
            let walk = native_chord_points(plan, sites, ordinates, hole, axes, true, budget)?;
            budget.charge(
                u64::try_from(walk.points.len()).map_err(|_| ResourceError::SizeOverflow)?,
            )?;
            holes.push(Ring::try_from_closed(CoordSeq::try_from_points(
                &walk.points,
            )?)?);
            hole_uses.push(LiteralSequence {
                output,
                axes,
                images: walk.ports,
            });
        }
        polygons.push(Polygon::new(shell, holes));
        polygon_uses.push(LiteralPolygon {
            output,
            axes,
            shell: LiteralSequence {
                output,
                axes,
                images: shell_walk.ports,
            },
            holes: hole_uses,
        });
    }
    Ok(NativeMaterializedParts {
        axes,
        points,
        lines,
        polygons,
        line_traces,
        literal_uses: NativeLiteralUses {
            points: point_uses,
            lines: line_uses,
            polygons: polygon_uses,
        },
    })
}

pub(super) fn write_completed_native(
    drawing: CompletedNativeDrawing,
) -> Result<WrittenNativeDrawing> {
    let CompletedNativeDrawing {
        sealed,
        line_traces,
    } = drawing;
    Ok(WrittenNativeDrawing {
        shape: sealed.into_shape(),
        line_traces,
    })
}

pub(super) struct WrittenNativeDrawing {
    pub(super) shape: Shape,
    pub(super) line_traces: Vec<TracedLine>,
}

pub(super) fn resolve_emission_ordinates<M: ChartMetric, C: RootCatalog>(
    plan: &ExpandedEmissionPlan<'_>,
    publication: &PublicationInventory,
    source_ordinates: &[NodeOrdinates],
    roots: &C,
    declared_axes: CoordinateAxes,
    strict: bool,
    operation: &'static str,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
    work: &mut impl Work,
) -> Result<(Vec<NodeOrdinates>, CoordinateAxes)> {
    if !declared_axes.has_z() && !declared_axes.has_m() {
        work.charge(u64::try_from(plan.ports.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
        return Ok((
            vec![NodeOrdinates::default(); plan.ports.len()],
            CoordinateAxes::XY,
        ));
    }
    let mut request_rows = Vec::with_capacity(plan.occurrences.len());
    for occurrence in &plan.occurrences {
        work.charge(1)?;
        let requests = match &occurrence.source_trace {
            Some(location) => source_requests_at_location(publication, location, work)?,
            None => Vec::new(),
        };
        request_rows.push(RequestRowInput {
            axes: declared_axes,
            requests,
        });
    }
    let resolved_requests = resolve_request_rows(&request_rows, roots, metric_access, work)?;

    let mut output = Vec::with_capacity(plan.ports.len());
    let mut has_z = declared_axes.has_z();
    let mut has_m = declared_axes.has_m();
    for port in &plan.ports {
        work.charge(1)?;
        let mut ordinates = NodeOrdinates::default();
        let mut has_live_presentation = false;
        for occurrence_index in port.occurrence_range.clone() {
            work.charge(1)?;
            let occurrence = plan.occurrences.get(occurrence_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("native port occurrence row is unknown".into())
            })?;
            if occurrence.request_row.index() != occurrence_index {
                return Err(GeometryErrorKind::Invalid(
                    "native port occurrence request row is misaligned".into(),
                )
                .into());
            }
            let mut occurrence_ordinates = match &occurrence.origin {
                VertexOrigin::SourceNode(node) => {
                    *source_ordinates.get(node.0 as usize).ok_or_else(|| {
                        GeometryErrorKind::Invalid("native source ordinate node is unknown".into())
                    })?
                },
                VertexOrigin::SourceSample => NodeOrdinates::default(),
                VertexOrigin::Presentation => {
                    has_live_presentation = true;
                    NodeOrdinates::default()
                },
            };
            occurrence_ordinates.merge(*resolved_requests.get(occurrence_index).ok_or_else(
                || {
                    GeometryErrorKind::Invalid(
                        "native occurrence has no resolved source request row".into(),
                    )
                },
            )?);
            ordinates.merge(occurrence_ordinates);
        }
        has_z &= !has_live_presentation && ordinates.z.value().is_some();
        has_m &= !has_live_presentation && ordinates.m.value().is_some();
        output.push(ordinates);
    }
    let axes = CoordinateAxes::new(crate::geometry::HasZ(has_z), crate::geometry::HasM(has_m));
    if strict && axes != declared_axes {
        return Err(GeometryErrorKind::ordinate_dropped(operation));
    }
    Ok((output, axes))
}

fn source_requests_at_location(
    publication: &PublicationInventory,
    location: &TraceRequestLocation,
    work: &mut impl Work,
) -> Result<Vec<RootRequest>> {
    match location {
        TraceRequestLocation::Root { root, parameter } => Ok(vec![RootRequest::new_with_work(
            *root,
            parameter.try_clone_with_work(work)?,
            work,
        )?]),
        TraceRequestLocation::Edge {
            recipe,
            edge,
            parameter,
        } => {
            let fragment = source_trace_fragment(publication, *recipe)?;
            fragment.trace.rows.requests_at(*edge, parameter, work)
        },
        TraceRequestLocation::Sample { recipe, row } => {
            let fragment = source_trace_fragment(publication, *recipe)?;
            fragment.trace.rows.sample_requests_at(row.index(), work)
        },
    }
}

fn source_trace_fragment(
    publication: &PublicationInventory,
    recipe: FragmentRecipeId,
) -> Result<&PublishedFragment> {
    publication
        .fragments
        .get(recipe.0 as usize)
        .filter(|fragment| fragment.recipe == recipe)
        .ok_or_else(|| {
            GeometryErrorKind::Invalid(
                "written occurrence references an unknown source trace".into(),
            )
            .into()
        })
}

fn native_chord_points(
    plan: &ExpandedEmissionPlan<'_>,
    sites: &[AssignedSite],
    ordinates: &[NodeOrdinates],
    chords: &[EmissionChordId],
    axes: CoordinateAxes,
    closed: bool,
    budget: &mut EvaluationBudget,
) -> Result<NativeChordPoints> {
    let Some(&first_id) = chords.first() else {
        return Err(GeometryErrorKind::Invalid("native emission walk is empty".into()).into());
    };
    let first_chord = plan.chords.get(first_id.0 as usize).ok_or_else(|| {
        GeometryErrorKind::Invalid("native emission walk starts with an unknown chord".into())
    })?;
    let first_port = first_chord.ports[0];
    let point_count = chords
        .len()
        .checked_add(1)
        .ok_or(ResourceError::SizeOverflow)?;
    budget.charge(
        u64::try_from(
            point_count
                .checked_mul(2)
                .ok_or(ResourceError::SizeOverflow)?,
        )
        .map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut points = Vec::with_capacity(point_count);
    let mut ports = Vec::with_capacity(point_count);
    budget.charge(1)?;
    points.push(native_port_point(plan, sites, ordinates, first_port, axes)?);
    ports.push(first_port);
    let mut previous = first_port;
    for &chord_id in chords {
        budget.charge(1)?;
        let chord = plan.chords.get(chord_id.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("native emission walk references an unknown chord".into())
        })?;
        if chord.ports[0] != previous {
            return Err(GeometryErrorKind::Invalid(
                "native emission chords are not directed-contiguous".into(),
            )
            .into());
        }
        let end = chord.ports[1];
        points.push(native_port_point(plan, sites, ordinates, end, axes)?);
        ports.push(end);
        previous = end;
    }
    if closed {
        if previous != first_port {
            return Err(GeometryErrorKind::Invalid(
                "native polygon part does not close through its indexed ports".into(),
            )
            .into());
        }
        let first = *points.first().ok_or_else(|| {
            GeometryErrorKind::Invalid("native polygon part has no first record".into())
        })?;
        let last = points.last_mut().ok_or_else(|| {
            GeometryErrorKind::Invalid("native polygon part has no closing record".into())
        })?;
        *last = first;
    }
    Ok(NativeChordPoints { points, ports })
}

pub(super) fn native_port_point(
    plan: &ExpandedEmissionPlan<'_>,
    sites: &[AssignedSite],
    ordinates: &[NodeOrdinates],
    port_id: EmissionPortId,
    axes: CoordinateAxes,
) -> Result<Point> {
    let port = plan.ports.get(port_id.0 as usize).ok_or_else(|| {
        GeometryErrorKind::Invalid("native drawing references an unknown port".into())
    })?;
    let site = sites.get(port.node.0 as usize).ok_or_else(|| {
        GeometryErrorKind::Invalid("native drawing node has no assigned site".into())
    })?;
    let (longitude, latitude) = port.source_record.map_or_else(
        || {
            let spelling = match &port.position {
                PortPosition::Source { spelling } | PortPosition::Projected { spelling } => {
                    spelling
                },
            };
            let longitude = match spelling {
                PortSpelling::Interior { .. } => site.longitude.native().get(),
                PortSpelling::Seam(ChartSide::East) => {
                    plan.topology.sources.frame.longitude().half_period()
                },
                PortSpelling::Seam(ChartSide::West) => {
                    -plan.topology.sources.frame.longitude().half_period()
                },
                PortSpelling::Pole { native_longitude } => *native_longitude,
            };
            (longitude, site.latitude)
        },
        |record| (record.x, record.y),
    );
    let ordinates = ordinates.get(port_id.0 as usize).copied().ok_or_else(|| {
        GeometryErrorKind::Invalid("native drawing port has no resolved ordinates".into())
    })?;
    let z = if axes.has_z() {
        Some(ordinates.z.value().ok_or_else(|| {
            GeometryErrorKind::Invalid("sealed native Z ordinate is missing".into())
        })?)
    } else {
        None
    };
    let m = if axes.has_m() {
        Some(ordinates.m.value().ok_or_else(|| {
            GeometryErrorKind::Invalid("sealed native M ordinate is missing".into())
        })?)
    } else {
        None
    };
    Ok(Point::new_raw_axes(
        longitude,
        latitude,
        ZOrdinate(z),
        MOrdinate(m),
    ))
}

pub(super) struct SealedSourceDrawing {
    pub(super) sealed: SealedOutput<QSourceImage>,
    pub(super) line_traces: Vec<TracedLine>,
}

pub(super) enum SealedQDrawing {
    Source(SealedSourceDrawing),
    Native(CompletedNativeDrawing),
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) struct FragmentRecipeId(pub u32);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SelectedUse {
    AreaBoundary,
    ArealBoundaryLine,
    Curve,
}

#[derive(Clone, Debug)]
pub(super) struct DirectedSourceRange {
    pub(super) source_edge: SourceEdgeId,
    pub(super) range: ExactSourceRange,
    pub(super) emitted: bool,
    pub(super) producer_start: ExactRational,
    pub(super) producer_end: ExactRational,
    pub(super) endpoint_only: bool,
}

struct PendingSourceRange {
    source_edge: SourceEdgeId,
    range: ExactSourceRange,
    emitted: bool,
    longitude_start: ExactRational,
    longitude_end: ExactRational,
    latitude_start: ExactRational,
    latitude_end: ExactRational,
}

fn pending_source_ranges(
    span: &ChartSpan,
    directed: DirectedSpan,
    longitude_start: &ExactRational,
    longitude_end: &ExactRational,
    latitude_start: &ExactRational,
    latitude_end: &ExactRational,
    work: &mut impl Work,
) -> Result<Vec<PendingSourceRange>> {
    let mut output = Vec::with_capacity(span.owners.len());
    for (owner_index, owner) in span.owners.iter().enumerate() {
        work.charge(1)?;
        let forward = (directed.from == span.ports[0]) != owner.reversed;
        let range = if forward {
            ExactSourceRange {
                start: owner.source_range.start.try_clone_with_work(work)?,
                end: owner.source_range.end.try_clone_with_work(work)?,
            }
        } else {
            ExactSourceRange {
                start: owner.source_range.end.try_clone_with_work(work)?,
                end: owner.source_range.start.try_clone_with_work(work)?,
            }
        };
        output.push(PendingSourceRange {
            source_edge: owner.source_edge,
            range,
            emitted: owner_index == directed.owner_index,
            longitude_start: longitude_start.try_clone_with_work(work)?,
            longitude_end: longitude_end.try_clone_with_work(work)?,
            latitude_start: latitude_start.try_clone_with_work(work)?,
            latitude_end: latitude_end.try_clone_with_work(work)?,
        });
    }
    Ok(output)
}

fn producer_mapping_is_affine(
    ranges: &[PendingSourceRange],
    longitude_displacement: &ExactRational,
    latitude_displacement: &ExactRational,
    support_allowed: bool,
    work: &mut impl Work,
) -> Result<bool> {
    let axis = if !longitude_displacement.is_zero() {
        Some((longitude_displacement, true))
    } else if !latitude_displacement.is_zero() {
        Some((latitude_displacement, false))
    } else {
        None
    };
    let Some((displacement, longitude)) = axis.filter(|_| support_allowed) else {
        return Ok(false);
    };
    {
        let zero = ExactRational::zero();
        let displacement_order = displacement.try_cmp_with_work(&zero, work)?;
        let mut supported = true;
        for range in ranges {
            work.charge(1)?;
            let longitude_step = range
                .longitude_end
                .try_subtract_with_work(&range.longitude_start, work)?;
            let latitude_step = range
                .latitude_end
                .try_subtract_with_work(&range.latitude_start, work)?;
            if !longitude_step
                .try_product_with_work(latitude_displacement, work)?
                .try_eq_with_work(
                    &latitude_step.try_product_with_work(longitude_displacement, work)?,
                    work,
                )?
            {
                supported = false;
                break;
            }
            let step = if longitude {
                &longitude_step
            } else {
                &latitude_step
            };
            let step_order = step.try_cmp_with_work(&zero, work)?;
            if (displacement_order.is_gt() && step_order.is_lt())
                || (displacement_order.is_lt() && step_order.is_gt())
            {
                supported = false;
                break;
            }
        }
        Ok(supported)
    }
}

fn producer_step_extends_run(
    longitude_displacement: &ExactRational,
    latitude_displacement: &ExactRational,
    longitude_step: &ExactRational,
    latitude_step: &ExactRational,
    support_allowed: bool,
    point_support: bool,
    work: &mut impl Work,
) -> Result<bool> {
    work.charge(1)?;
    if !support_allowed
        || point_support
        || (longitude_displacement.is_zero() && latitude_displacement.is_zero())
    {
        return Ok(false);
    }
    // The admitted prefix is already affine to its cumulative displacement;
    // collinearity and a matching primary-axis sign certify only this step.
    let cross = longitude_displacement
        .try_product_with_work(latitude_step, work)?
        .try_subtract_with_work(
            &latitude_displacement.try_product_with_work(longitude_step, work)?,
            work,
        )?;
    if !cross.is_zero() {
        return Ok(false);
    }
    let zero = ExactRational::zero();
    let (displacement_primary, step_primary) = if longitude_displacement.is_zero() {
        (latitude_displacement, latitude_step)
    } else {
        (longitude_displacement, longitude_step)
    };
    let displacement_order = displacement_primary.try_cmp_with_work(&zero, work)?;
    let step_order = step_primary.try_cmp_with_work(&zero, work)?;
    Ok(!displacement_order.is_eq() && displacement_order == step_order)
}

fn producer_ranges(
    ranges: Vec<PendingSourceRange>,
    longitude_displacement: &ExactRational,
    latitude_displacement: &ExactRational,
    support_allowed: bool,
    work: &mut impl Work,
) -> Result<Vec<DirectedSourceRange>> {
    let has_displacement = !longitude_displacement.is_zero() || !latitude_displacement.is_zero();
    let endpoint_only = !support_allowed || !has_displacement;
    let affine = producer_mapping_is_affine(
        &ranges,
        longitude_displacement,
        latitude_displacement,
        support_allowed,
        work,
    )?;
    if !endpoint_only && !affine {
        return Err(GeometryErrorKind::Invalid(
            "producer fragment changes direction before a written Q port".into(),
        )
        .into());
    }
    let mut output = Vec::with_capacity(ranges.len());
    for range in ranges {
        work.charge(1)?;
        let (producer_start, producer_end) = if endpoint_only {
            (ExactRational::zero(), ExactRational::one())
        } else if !longitude_displacement.is_zero() {
            let (start, end) = (&range.longitude_start, &range.longitude_end);
            (
                start.try_quotient_with_work(longitude_displacement, work)?,
                end.try_quotient_with_work(longitude_displacement, work)?,
            )
        } else if !latitude_displacement.is_zero() {
            let (start, end) = (&range.latitude_start, &range.latitude_end);
            (
                start.try_quotient_with_work(latitude_displacement, work)?,
                end.try_quotient_with_work(latitude_displacement, work)?,
            )
        } else {
            (ExactRational::zero(), ExactRational::one())
        };
        output.push(DirectedSourceRange {
            source_edge: range.source_edge,
            range: range.range,
            emitted: range.emitted,
            producer_start,
            producer_end,
            endpoint_only,
        });
    }
    Ok(output)
}

#[derive(Clone, Debug)]
pub(super) struct FragmentRecipe {
    pub(super) id: FragmentRecipeId,
    pub(super) endpoints: [QPortId; 2],
    pub(super) ranges: Vec<DirectedSourceRange>,
    pub(super) exact_displacement: ExactRational,
    pub(super) use_kind: SelectedUse,
}

#[derive(Clone, Copy)]
enum EndpointSide {
    Start,
    End,
}

impl EndpointSide {
    const fn index(self) -> usize {
        match self {
            Self::Start => 0,
            Self::End => 1,
        }
    }

    fn parameter(self) -> ExactRational {
        match self {
            Self::Start => ExactRational::zero(),
            Self::End => ExactRational::one(),
        }
    }
}

fn interpolate_exact_reference_coordinate(
    start: &ExactRational,
    end: &ExactRational,
    parameter: &ExactRational,
    work: &mut impl Work,
) -> Result<ExactRational> {
    let displacement = end.try_subtract_with_work(start, work)?;
    start.try_add_with_work(&displacement.try_product_with_work(parameter, work)?, work)
}

fn interpolate_exact_reference_point(
    endpoints: &[ExactChartPoint; 2],
    parameter: &ExactRational,
    work: &mut impl Work,
) -> Result<ExactChartPoint> {
    Ok(ExactChartPoint::from_rationals(
        interpolate_exact_reference_coordinate(&endpoints[0].x, &endpoints[1].x, parameter, work)?,
        interpolate_exact_reference_coordinate(&endpoints[0].y, &endpoints[1].y, parameter, work)?,
    ))
}

pub(super) struct PublishedSample {
    pub(super) request_row: RequestRowId,
    pub(super) family: FeatureFamily,
    pub(super) parameter: ExactRational,
    pub(super) written_vertex: usize,
    pub(super) point: Point,
    pub(super) exact_point: ExactChartPoint,
    pub(super) published_chart_point: ExactChartPoint,
    pub(super) source_trace: Option<TraceRequestLocation>,
    pub(super) endpoint_only: bool,
}

pub(super) struct PublishedFragment {
    pub(super) recipe: FragmentRecipeId,
    pub(super) points: Vec<Point>,
    pub(super) trace: SourceTrace,
    pub(super) sample_range: Range<usize>,
    pub(super) source_records: [Option<Point>; 2],
    pub(super) exact_endpoints: [ExactChartPoint; 2],
}

pub(super) struct PublicationInventory {
    pub(super) fragments: Vec<PublishedFragment>,
    pub(super) samples: Vec<PublishedSample>,
    pub(super) counts: Vec<usize>,
    pub(super) ordinates: Vec<NodeOrdinates>,
}

impl FragmentRecipe {
    pub(super) fn reference_endpoints(
        &self,
        topology: &ExactGeographicTopology,
        work: &mut impl Work,
    ) -> Result<[ExactChartPoint; 2]> {
        let start_port = topology
            .q
            .ports
            .get(self.endpoints[0].0 as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("publication reference start port is unknown".into())
            })?;
        let end_port = topology
            .q
            .ports
            .get(self.endpoints[1].0 as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("publication reference end port is unknown".into())
            })?;
        let start = start_port.point.try_clone_with_work(work)?;
        let end = ExactChartPoint::from_rationals(
            start.x.try_add_with_work(&self.exact_displacement, work)?,
            end_port.point.y.try_clone_with_work(work)?,
        );
        let period = ExactRational::from_f64(topology.sources.frame.longitude().period());
        let end_offset = end
            .x
            .try_subtract_with_work(&end_port.point.x, work)?
            .try_quotient_with_work(&period, work)?;
        if !end_offset.is_integer() {
            return Err(GeometryErrorKind::Invalid(
                "publication reference end is not an integral-period Q-port alias".into(),
            )
            .into());
        }
        if !quotient_key_for_chart_position_with_work(topology.sources.frame, &end, work)
            .and_then(|end_key| {
                let port_key = quotient_key_for_chart_position_with_work(
                    topology.sources.frame,
                    &end_port.point,
                    work,
                )?;
                compare_quotient_keys_with_work(&end_key, &port_key, work)
            })?
            .is_eq()
        {
            return Err(GeometryErrorKind::Invalid(
                "publication reference end disagrees with its Q-port identity".into(),
            )
            .into());
        }
        Ok([start, end])
    }

    fn source_endpoint_record(
        &self,
        topology: &ExactGeographicTopology,
        side: EndpointSide,
        work: &mut impl Work,
    ) -> Result<Option<Point>> {
        let local_parameter = side.parameter();
        for range in &self.ranges {
            if !range.emitted {
                continue;
            }
            if range.endpoint_only
                && !local_parameter
                    .try_cmp_with_work(&range.producer_start, work)?
                    .is_eq()
                && !local_parameter
                    .try_cmp_with_work(&range.producer_end, work)?
                    .is_eq()
            {
                continue;
            }
            let producer_order = range
                .producer_start
                .try_cmp_with_work(&range.producer_end, work)?;
            let lower = if producer_order.is_le() {
                &range.producer_start
            } else {
                &range.producer_end
            };
            let upper = if producer_order.is_le() {
                &range.producer_end
            } else {
                &range.producer_start
            };
            if local_parameter.try_cmp_with_work(lower, work)?.is_lt()
                || local_parameter.try_cmp_with_work(upper, work)?.is_gt()
            {
                continue;
            }
            let width = range
                .producer_end
                .try_subtract_with_work(&range.producer_start, work)?;
            if width.is_zero() {
                continue;
            }
            let within = local_parameter
                .try_subtract_with_work(&range.producer_start, work)?
                .try_quotient_with_work(&width, work)?;
            if within
                .try_cmp_with_work(&ExactRational::zero(), work)?
                .is_lt()
                || within
                    .try_cmp_with_work(&ExactRational::one(), work)?
                    .is_gt()
            {
                continue;
            }
            let parameter = range.range.compose_with_work(&within, &within, work)?.start;
            let root_side = if parameter.is_zero() {
                Some(EndpointSide::Start)
            } else if parameter.is_one() {
                Some(EndpointSide::End)
            } else {
                None
            };
            let Some(root_side) = root_side else {
                continue;
            };
            let edge = topology
                .sources
                .edges
                .get(range.source_edge.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("publication endpoint source edge is unknown".into())
                })?;
            let Some(record) = edge.source.get(root_side.index()).copied() else {
                continue;
            };
            let port_id = *self.endpoints.get(side.index()).ok_or_else(|| {
                GeometryErrorKind::Invalid("publication endpoint port is unknown".into())
            })?;
            let port = topology.q.ports.get(port_id.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("publication endpoint Q port is unknown".into())
            })?;
            let admitted_y = Latitude::from_admitted(topology.sources.frame, record.y).get();
            let record_point = ExactChartPoint::from_rationals(
                ExactRational::from_f64(record.x),
                ExactRational::from_f64(admitted_y),
            );
            let record_key = quotient_key_for_chart_position_with_work(
                topology.sources.frame,
                &record_point,
                work,
            )?;
            let port_key = quotient_key_for_chart_position_with_work(
                topology.sources.frame,
                &port.point,
                work,
            )?;
            if !compare_quotient_keys_with_work(&record_key, &port_key, work)?.is_eq() {
                continue;
            }
            if matches!(
                &record_key,
                ExactQuotientKey::NorthPole | ExactQuotientKey::SouthPole
            ) {
                let longitude = topology.sources.frame.longitude();
                let (record_longitude, _) = split_rational_native_site_with_work(
                    &record_point.x,
                    longitude.period(),
                    longitude.half_period(),
                    work,
                )?;
                let (port_longitude, _) = split_rational_native_site_with_work(
                    &port.point.x,
                    longitude.period(),
                    longitude.half_period(),
                    work,
                )?;
                if !record_longitude
                    .as_ref()
                    .try_cmp_with_work(port_longitude.as_ref(), work)?
                    .is_eq()
                {
                    continue;
                }
            }
            return Ok(Some(record));
        }
        Ok(None)
    }
}

impl SourceTrace {
    fn split_at_samples(
        &self,
        samples: &[SourceTraceSample],
        work: &mut impl Work,
    ) -> Result<Self> {
        let edge_count = self.rows.edge_count();
        let input_vertex_count = edge_count
            .checked_add(1)
            .ok_or(ResourceError::SizeOverflow)?;
        let mut new_samples_by_edge = vec![0_usize; edge_count];
        for sample in samples {
            let count = new_samples_by_edge
                .get_mut(sample.edge_index)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "publication sample references an unknown source edge".into(),
                    )
                })?;
            *count = count.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
        }
        let mut new_samples_before_vertex = vec![0_usize; input_vertex_count];
        for vertex in 1..input_vertex_count {
            work.charge(1)?;
            let previous = *new_samples_before_vertex.get(vertex - 1).ok_or_else(|| {
                GeometryErrorKind::Invalid("publication sample prefix is incomplete".into())
            })?;
            let edge_samples = *new_samples_by_edge.get(vertex - 1).ok_or_else(|| {
                GeometryErrorKind::Invalid("publication sample edge count is incomplete".into())
            })?;
            *new_samples_before_vertex.get_mut(vertex).ok_or_else(|| {
                GeometryErrorKind::Invalid("publication sample prefix is incomplete".into())
            })? = previous
                .checked_add(edge_samples)
                .ok_or(ResourceError::SizeOverflow)?;
        }
        let output_vertex_count = input_vertex_count
            .checked_add(samples.len())
            .ok_or(ResourceError::SizeOverflow)?;
        let mut output_rows = TraceRowsBuilder::new();
        let mut output_samples = Vec::with_capacity(samples.len());
        let mut request_row_index = 0_usize;
        for sample in &self.samples {
            work.charge(1)?;
            if sample.written_vertex > edge_count {
                return Err(GeometryErrorKind::Invalid(
                    "previous source sample is outside the input vertex sequence".into(),
                )
                .into());
            }
            let inserted_before = *new_samples_before_vertex
                .get(sample.written_vertex)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("previous source sample index is unknown".into())
                })?;
            let written_vertex = sample
                .written_vertex
                .checked_add(inserted_before)
                .ok_or(ResourceError::SizeOverflow)?;
            if written_vertex >= output_vertex_count {
                return Err(GeometryErrorKind::Invalid(
                    "previous source sample exceeds the rewritten point chain".into(),
                )
                .into());
            }
            let requests = self
                .rows
                .sample_requests_at(sample.request_row.index(), work)?;
            work.charge(u64::try_from(requests.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
            let point_ranges = requests
                .iter()
                .map(|request| {
                    RootRange::point_with_work(
                        request.root(),
                        request.parameter().try_clone_with_work(work)?,
                        ExactRational::zero(),
                        work,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            output_rows.push_sample_row(point_ranges)?;
            output_samples.push(TracedSourceSample {
                written_vertex,
                edge: sample.edge,
                parameter: sample.parameter.try_clone_with_work(work)?,
                request_row: RequestRowId::from_index(request_row_index)?,
                has_source: sample.has_source,
                endpoint_only: sample.endpoint_only,
            });
            request_row_index = request_row_index
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?;
        }
        let zero = ExactRational::zero();
        let one = ExactRational::one();
        let mut sample_index = 0_usize;
        let mut previous_new_written_vertex = None;
        for edge_index in 0..edge_count {
            work.charge(1)?;
            let mut boundaries = vec![zero.try_clone_with_work(work)?];
            let mut previous_parameter = zero.try_clone_with_work(work)?;
            while let Some(sample) = samples.get(sample_index)
                && sample.edge_index == edge_index
            {
                work.charge(1)?;
                if sample
                    .local_parameter
                    .try_cmp_with_work(&previous_parameter, work)?
                    .is_le()
                    || sample
                        .local_parameter
                        .try_cmp_with_work(&one, work)?
                        .is_ge()
                    || previous_new_written_vertex
                        .is_some_and(|previous| previous >= sample.written_vertex)
                {
                    return Err(GeometryErrorKind::Invalid(
                        "accepted publication samples are not in written order".into(),
                    )
                    .into());
                }
                let (requests, endpoint_only) = self.rows.requests_with_support_at(
                    edge_index,
                    &sample.local_parameter,
                    work,
                )?;
                let has_source = !requests.is_empty();
                work.charge(
                    u64::try_from(requests.len()).map_err(|_| ResourceError::SizeOverflow)?,
                )?;
                let point_ranges = requests
                    .iter()
                    .map(|request| {
                        RootRange::point_with_work(
                            request.root(),
                            request.parameter().try_clone_with_work(work)?,
                            zero.try_clone_with_work(work)?,
                            work,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;
                output_rows.push_sample_row(point_ranges)?;
                output_samples.push(TracedSourceSample {
                    written_vertex: sample.written_vertex,
                    edge: edge_index,
                    parameter: sample.local_parameter.try_clone_with_work(work)?,
                    request_row: RequestRowId::from_index(request_row_index)?,
                    has_source,
                    endpoint_only,
                });
                request_row_index = request_row_index
                    .checked_add(1)
                    .ok_or(ResourceError::SizeOverflow)?;
                boundaries.push(sample.local_parameter.try_clone_with_work(work)?);
                previous_parameter = sample.local_parameter.try_clone_with_work(work)?;
                previous_new_written_vertex = Some(sample.written_vertex);
                sample_index = sample_index
                    .checked_add(1)
                    .ok_or(ResourceError::SizeOverflow)?;
            }
            boundaries.push(one.try_clone_with_work(work)?);

            for pair in boundaries.array_windows::<2>() {
                let [from, to] = pair;
                work.charge(1)?;
                output_rows.push_composed_row(self.rows.row(edge_index)?, from, to, work)?;
            }
        }
        if sample_index != samples.len() {
            return Err(GeometryErrorKind::Invalid(
                "publication sample references an unknown source edge".into(),
            )
            .into());
        }
        try_sort_by_with_work(&mut output_samples, work, |left, right, _| {
            Ok(left.written_vertex.cmp(&right.written_vertex))
        })?;
        for pair in output_samples.array_windows::<2>() {
            let [left, right] = pair;
            work.charge(1)?;
            if left.written_vertex >= right.written_vertex {
                return Err(GeometryErrorKind::Invalid(
                    "written source sample vertices are not strictly ordered".into(),
                )
                .into());
            }
        }
        Ok(Self {
            rows: output_rows.finish(work)?,
            samples: output_samples,
        })
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct DirectedSpan {
    pub(super) span: ChartSpanId,
    pub(super) from: QPortId,
    pub(super) to: QPortId,
    pub(super) owner_index: usize,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) enum RecipeWalkOwner {
    Cycle(usize),
    Line(usize),
    SourceRing(usize),
}

#[derive(Clone, Debug)]
pub(super) struct RecipeWalk {
    pub(super) owner: RecipeWalkOwner,
    pub(super) fragments: Vec<FragmentRecipeId>,
}

pub(super) type RecipeWalkIndex<'a> = BTreeMap<RecipeWalkOwner, Vec<&'a RecipeWalk>>;

pub(super) fn index_recipe_walks<'walks>(
    walks: &'walks [RecipeWalk],
    work: &mut impl Work,
) -> Result<RecipeWalkIndex<'walks>> {
    work.charge(u64::try_from(walks.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut index = BTreeMap::new();
    for walk in walks {
        work.charge(1)?;
        index.entry(walk.owner).or_insert_with(Vec::new).push(walk);
    }
    Ok(index)
}

pub(super) fn next_recipe_walk<'walk>(
    index: &RecipeWalkIndex<'walk>,
    cursors: &mut BTreeMap<RecipeWalkOwner, usize>,
    owner: RecipeWalkOwner,
    work: &mut impl Work,
) -> Result<&'walk RecipeWalk> {
    work.charge(1)?;
    let cursor = cursors.entry(owner).or_default();
    let walk = index
        .get(&owner)
        .and_then(|walks| walks.get(*cursor))
        .copied()
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("producer recipe walk is missing for its owner".into())
        })?;
    *cursor = cursor.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
    Ok(walk)
}

#[derive(Clone)]
pub(in crate::geometry) struct SourceTrace {
    pub(super) rows: TraceRows,
    pub(super) samples: Vec<TracedSourceSample>,
}

struct SourceTraceSample {
    edge_index: usize,
    local_parameter: ExactRational,
    written_vertex: usize,
}

#[derive(Clone)]
pub(in crate::geometry) struct TracedLine {
    pub(in crate::geometry) member: usize,
    pub(in crate::geometry) trace: SourceTrace,
}

pub(in crate::geometry) struct PreparedNativeSource {
    pub(in crate::geometry) shape: Shape,
    pub(in crate::geometry) line_traces: Vec<TracedLine>,
    pub(in crate::geometry) source_members: Vec<usize>,
    pub(in crate::geometry) family_state: Option<FeatureFamilyRegistry>,
}

impl SourceTrace {
    pub(in crate::geometry) fn from_edge_rows(
        edge_rows: impl IntoIterator<Item = Vec<RootRange>>,
        work: &mut impl Work,
    ) -> Result<Self> {
        let mut rows = TraceRowsBuilder::new();
        for row in edge_rows {
            work.charge(1)?;
            rows.push_row(row)?;
        }
        Ok(Self {
            rows: rows.finish(work)?,
            samples: Vec::new(),
        })
    }

    pub(in crate::geometry) const fn edge_count(&self) -> usize {
        self.rows.edge_count()
    }

    pub(in crate::geometry) fn requests_at(
        &self,
        edge: usize,
        local: &ExactRational,
        work: &mut impl Work,
    ) -> Result<Vec<RootRequest>> {
        self.rows.requests_at(edge, local, work)
    }

    pub(in crate::geometry) fn compose_edges(
        &self,
        slices: &[(usize, ExactRational, ExactRational)],
        work: &mut impl Work,
    ) -> Result<Self> {
        let edge_count = self.rows.edge_count();
        let vertex_count = edge_count
            .checked_add(1)
            .ok_or(ResourceError::SizeOverflow)?;
        work.charge(u64::try_from(vertex_count).map_err(|_| ResourceError::SizeOverflow)?)?;
        let mut requests_by_vertex = vec![Vec::<RootRequest>::new(); vertex_count];
        for sample in &self.samples {
            work.charge(1)?;
            let requests = self
                .rows
                .sample_requests_at(sample.request_row.index(), work)?;
            let vertex_requests = requests_by_vertex
                .get_mut(sample.written_vertex)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "source sample is outside its retained line trace".into(),
                    )
                })?;
            work.charge(u64::try_from(requests.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
            vertex_requests.extend(requests);
        }

        let zero = ExactRational::zero();
        let one = ExactRational::one();
        let mut rows = TraceRowsBuilder::new();
        for &(edge, ref start, ref end) in slices {
            work.charge(1)?;
            if edge >= edge_count {
                return Err(GeometryErrorKind::Invalid(
                    "source trace slice references an unknown edge".into(),
                )
                .into());
            }
            let mut row = Vec::new();
            for parent in self.rows.row(edge)? {
                work.charge(1)?;
                if let Some(child) = parent.compose_with_work(start, end, work)? {
                    row.push(child);
                }
            }
            if start.try_cmp_with_work(&zero, work)?.is_eq() {
                for request in requests_by_vertex.get(edge).ok_or_else(|| {
                    GeometryErrorKind::Invalid("source trace start vertex is unknown".into())
                })? {
                    work.charge(1)?;
                    row.push(RootRange::point_with_work(
                        request.root(),
                        request.parameter().try_clone_with_work(work)?,
                        zero.try_clone_with_work(work)?,
                        work,
                    )?);
                }
            }
            if end.try_cmp_with_work(&one, work)?.is_eq() {
                let vertex = edge.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
                for request in requests_by_vertex.get(vertex).ok_or_else(|| {
                    GeometryErrorKind::Invalid("source trace end vertex is unknown".into())
                })? {
                    work.charge(1)?;
                    row.push(RootRange::point_with_work(
                        request.root(),
                        request.parameter().try_clone_with_work(work)?,
                        one.try_clone_with_work(work)?,
                        work,
                    )?);
                }
            }
            rows.push_row(row)?;
        }
        Ok(Self {
            rows: rows.finish(work)?,
            samples: Vec::new(),
        })
    }
}

#[derive(Clone, Debug)]
pub(super) enum TraceRequestLocation {
    Edge {
        recipe: FragmentRecipeId,
        edge: usize,
        parameter: ExactRational,
    },
    Sample {
        recipe: FragmentRecipeId,
        row: RequestRowId,
    },
    Root {
        root: RootId,
        parameter: ExactRational,
    },
}

pub(super) fn native_source_sample_occurrences(
    source_trace: Option<TraceRequestLocation>,
    source_requests: &[RootRequest],
    work: &mut impl Work,
) -> Result<Vec<OccurrenceSource>> {
    let capacity = source_requests
        .len()
        .checked_add(usize::from(source_trace.is_some()))
        .ok_or(ResourceError::SizeOverflow)?;
    let mut occurrences = Vec::with_capacity(capacity);
    if let Some(source_trace) = source_trace {
        work.charge(1)?;
        occurrences.push(OccurrenceSource {
            origin: VertexOrigin::SourceSample,
            source_trace: Some(source_trace),
        });
    }
    for request in source_requests {
        work.charge(1)?;
        occurrences.push(OccurrenceSource {
            origin: VertexOrigin::SourceSample,
            source_trace: Some(TraceRequestLocation::Root {
                root: request.root(),
                parameter: request.parameter().try_clone_with_work(work)?,
            }),
        });
    }
    if occurrences.is_empty() {
        work.charge(1)?;
        occurrences.push(OccurrenceSource {
            origin: VertexOrigin::Presentation,
            source_trace: None,
        });
    }
    Ok(occurrences)
}

#[derive(Clone, Debug)]
pub(super) struct SourceSampleOccurrence {
    pub(super) trace: Option<TraceRequestLocation>,
    pub(super) endpoint_only: bool,
    pub(super) publication_sample: Option<usize>,
}

#[derive(Clone, Debug)]
pub(super) struct TracedSourceSample {
    pub(super) written_vertex: usize,
    pub(super) edge: usize,
    pub(super) parameter: ExactRational,
    pub(super) request_row: RequestRowId,
    pub(super) has_source: bool,
    pub(super) endpoint_only: bool,
}

pub(super) fn resolve_publication_ordinates<M: ChartMetric>(
    topology: &ExactGeographicTopology,
    publication: &PublicationInventory,
    live_samples: &[bool],
    axes: CoordinateAxes,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
    work: &mut impl Work,
) -> Result<Vec<NodeOrdinates>> {
    if live_samples.len() != publication.samples.len() {
        return Err(GeometryErrorKind::Invalid(
            "publication liveness rows do not match their sample inventory".into(),
        )
        .into());
    }
    if publication.samples.is_empty() {
        return Ok(Vec::new());
    }
    if !axes.has_z() && !axes.has_m() {
        work.charge(
            u64::try_from(publication.samples.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        return Ok(vec![NodeOrdinates::default(); publication.samples.len()]);
    }
    let mut request_rows = Vec::with_capacity(publication.samples.len());
    for fragment in &publication.fragments {
        if fragment.trace.samples.len() != fragment.sample_range.len() {
            return Err(GeometryErrorKind::Invalid(
                "publication source ordinates do not match their sample rows".into(),
            )
            .into());
        }
        for (offset, trace_sample) in fragment.trace.samples.iter().enumerate() {
            work.charge(1)?;
            let sample_index = fragment
                .sample_range
                .start
                .checked_add(offset)
                .ok_or(ResourceError::SizeOverflow)?;
            let sample = publication.samples.get(sample_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("publication sample row is unknown".into())
            })?;
            if sample.request_row.index() != sample_index
                || sample.written_vertex != trace_sample.written_vertex
                || sample.source_trace.is_some() != trace_sample.has_source
                || sample.endpoint_only != trace_sample.endpoint_only
            {
                return Err(GeometryErrorKind::Invalid(
                    "publication sample source rows are misaligned".into(),
                )
                .into());
            }
            let source_trace = if let Some(source_trace) = &sample.source_trace {
                let TraceRequestLocation::Sample { recipe, row } = source_trace else {
                    return Err(GeometryErrorKind::Invalid(
                        "publication sample does not own a sample request row".into(),
                    )
                    .into());
                };
                if *recipe != fragment.recipe || *row != trace_sample.request_row {
                    return Err(GeometryErrorKind::Invalid(
                        "publication sample source trace is misaligned".into(),
                    )
                    .into());
                }
                Some(source_trace)
            } else {
                None
            };
            let live = *live_samples.get(sample_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("publication sample liveness row is unknown".into())
            })?;
            let requests = if live {
                source_trace
                    .map(|location| source_requests_at_location(publication, location, work))
                    .transpose()?
                    .unwrap_or_default()
            } else {
                Vec::new()
            };
            request_rows.push(RequestRowInput {
                axes: if live { axes } else { CoordinateAxes::XY },
                requests,
            });
        }
    }
    if request_rows.len() != publication.samples.len() {
        return Err(GeometryErrorKind::Invalid(
            "publication source ordinate rows are incomplete".into(),
        )
        .into());
    }
    resolve_request_rows(&request_rows, topology, metric_access, work)
}

impl SourceTrace {
    pub(super) fn owners_for_edge(&self, edge_index: usize) -> Result<&[RootRange]> {
        self.rows.row(edge_index)
    }
}

#[derive(Default)]
pub(super) struct FragmentRecipes {
    values: Vec<FragmentRecipe>,
    walks: Vec<RecipeWalk>,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) enum FeatureFamilyParent {
    Fragment(FragmentRecipeId),
    Region(usize),
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) enum SampleProducerKind {
    Publication,
    NativeCut,
    PolePresentation(ChartPole),
    WholeSphereCover,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct FeatureFamily {
    pub(super) parent: FeatureFamilyParent,
    pub(super) producer: SampleProducerKind,
    pub(super) occurrence: u32,
}

impl FeatureFamily {
    pub(super) const fn fragment_recipe(self) -> Option<FragmentRecipeId> {
        match self.parent {
            FeatureFamilyParent::Fragment(recipe) => Some(recipe),
            FeatureFamilyParent::Region(_) => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ChartSide {
    East,
    West,
}

#[derive(Clone, Debug)]
pub(super) struct NativeCutFeature {
    pub(super) family: FeatureFamily,
    pub(super) seam_period_index: i64,
    pub(super) trace_edge: usize,
    pub(super) edge_parameter: ExactRational,
    pub(super) point: ExactChartPoint,
    pub(super) source_trace: Option<TraceRequestLocation>,
}

pub(super) fn native_cut_features(
    topology: &ExactGeographicTopology,
    recipe: &FragmentRecipe,
    trace: &SourceTrace,
    trace_edge: usize,
    start: &ExactChartPoint,
    end: &ExactChartPoint,
    assigned_displacement: &ExactRational,
    work: &mut impl Work,
) -> Result<Vec<NativeCutFeature>> {
    let axis = topology.sources.frame.longitude();
    let start_site = retained_exact_native_site_with_work(axis, &start.x, work)?;
    let producer_end_x = start.x.try_add_with_work(assigned_displacement, work)?;
    let end_site = retained_exact_native_site_with_work(axis, &producer_end_x, work)?;
    let mut features = Vec::new();
    if start_site.periods() < end_site.periods() {
        let mut period_index = start_site.periods();
        while period_index < end_site.periods() {
            work.charge(1)?;
            append_native_cut_feature(
                topology,
                recipe,
                trace,
                trace_edge,
                start,
                end,
                assigned_displacement,
                period_index,
                &mut features,
                work,
            )?;
            period_index = period_index
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?;
        }
    } else {
        let mut period_index = start_site.periods();
        while period_index > end_site.periods() {
            work.charge(1)?;
            period_index = period_index
                .checked_sub(1)
                .ok_or(ResourceError::SizeOverflow)?;
            append_native_cut_feature(
                topology,
                recipe,
                trace,
                trace_edge,
                start,
                end,
                assigned_displacement,
                period_index,
                &mut features,
                work,
            )?;
        }
    }
    Ok(features)
}

fn append_native_cut_feature(
    topology: &ExactGeographicTopology,
    recipe: &FragmentRecipe,
    trace: &SourceTrace,
    trace_edge: usize,
    start: &ExactChartPoint,
    end: &ExactChartPoint,
    assigned_displacement: &ExactRational,
    period_index: i64,
    output: &mut Vec<NativeCutFeature>,
    work: &mut impl Work,
) -> Result<()> {
    work.charge(1)?;
    let axis = topology.sources.frame.longitude();
    let period = ExactRational::from_f64(axis.period());
    let half = ExactRational::from_f64(axis.half_period());
    let seam = half.try_add_with_work(
        &period.try_product_with_work(
            &ExactRational::from_dyadic(ExactDyadic::from_i64(period_index)),
            work,
        )?,
        work,
    )?;
    let delta_x = assigned_displacement.try_clone_with_work(work)?;
    if delta_x.is_zero() {
        return Ok(());
    }
    let parameter = seam
        .try_subtract_with_work(&start.x, work)?
        .try_quotient_with_work(&delta_x, work)?;
    if parameter
        .try_cmp_with_work(&ExactRational::zero(), work)?
        .is_lt()
        || parameter
            .try_cmp_with_work(&ExactRational::one(), work)?
            .is_gt()
        || parameter.is_zero()
        || parameter.is_one()
    {
        return Ok(());
    }
    let delta_y = end.y.try_subtract_with_work(&start.y, work)?;
    let latitude = start
        .y
        .try_add_with_work(&delta_y.try_product_with_work(&parameter, work)?, work)?;
    let (has_source, _) = trace
        .rows
        .request_support_at(trace_edge, &parameter, work)?;
    let point = ExactChartPoint::from_rationals(seam, latitude);
    output.push(NativeCutFeature {
        family: FeatureFamily {
            parent: FeatureFamilyParent::Fragment(recipe.id),
            producer: SampleProducerKind::NativeCut,
            occurrence: 0,
        },
        seam_period_index: period_index,
        trace_edge,
        edge_parameter: parameter.try_clone_with_work(work)?,
        point,
        source_trace: has_source.then_some(TraceRequestLocation::Edge {
            recipe: recipe.id,
            edge: trace_edge,
            parameter,
        }),
    });
    Ok(())
}

#[derive(Clone, Debug)]
pub(super) struct FeatureFamilyBound {
    publication: usize,
    native_cuts: usize,
    native_cut_ranges: Vec<NativeCutOccurrenceRange>,
}

#[derive(Clone, Debug)]
pub(super) struct NativeCutOccurrenceRange {
    intervals: Vec<NativeCutOccurrenceInterval>,
}

#[derive(Clone, Copy, Debug)]
struct NativeCutOccurrenceInterval {
    low_period: i64,
    high_period: i64,
    family_base: u32,
}

pub(super) struct FeatureFamilyBounds {
    bounds: Vec<FeatureFamilyBound>,
    region_families: Vec<FeatureFamily>,
}

pub(in crate::geometry) struct FeatureFamilyRegistry {
    bounds: Vec<FeatureFamilyBound>,
    region_families: Vec<FeatureFamily>,
    families: FallibleAvl<FeatureFamily>,
    source_base: usize,
}

impl FeatureFamilyBounds {
    pub(super) fn from_prepared_domains(
        recipes: &[FragmentRecipe],
        publication: &PublicationInventory,
        native_cut_ranges: &[Vec<Vec<(i64, i64)>>],
        mut region_families: Vec<FeatureFamily>,
        work: &mut impl Work,
    ) -> Result<Self> {
        if publication.fragments.len() != recipes.len()
            || publication.counts.len() != recipes.len()
            || native_cut_ranges.len() != recipes.len()
        {
            return Err(GeometryErrorKind::Invalid(
                "native family bounds do not match the prepared producer table".into(),
            )
            .into());
        }
        work.charge(u64::try_from(recipes.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
        let mut bounds = Vec::with_capacity(recipes.len());
        for (recipe_index, recipe) in recipes.iter().enumerate() {
            work.charge(1)?;
            let fragment = publication.fragments.get(recipe_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("native family publication fragment is unknown".into())
            })?;
            let ranges = native_cut_ranges.get(recipe_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("native family leaf ranges are unknown".into())
            })?;
            if recipe.id.0 as usize != recipe_index
                || fragment.recipe != recipe.id
                || fragment.trace.rows.edge_count() != ranges.len()
            {
                return Err(GeometryErrorKind::Invalid(
                    "native family rows are not aligned with their producer leaves".into(),
                )
                .into());
            }
            let publication_count = *publication.counts.get(recipe_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("publication family count is unknown".into())
            })?;
            if fragment.sample_range.len() != publication_count {
                return Err(GeometryErrorKind::Invalid(
                    "publication family count differs from its sample range".into(),
                )
                .into());
            }
            let mut native_cut_ranges = Vec::with_capacity(ranges.len());
            let mut native_cuts = 0_usize;
            work.charge(u64::try_from(ranges.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
            for possible_intervals in ranges {
                work.charge(
                    u64::try_from(possible_intervals.len())
                        .map_err(|_| ResourceError::SizeOverflow)?,
                )?;
                let mut sorted = possible_intervals.clone();
                try_sort_by_with_work(&mut sorted, work, |left, right, _work| Ok(left.cmp(right)))?;
                let mut merged = Vec::<(i64, i64)>::new();
                for (low, high) in sorted {
                    work.charge(1)?;
                    if low > high {
                        return Err(GeometryErrorKind::Invalid(
                            "native family period range is reversed".into(),
                        )
                        .into());
                    }
                    if let Some(previous) = merged.last_mut() {
                        let adjacent = previous.1.checked_add(1).is_some_and(|next| low <= next);
                        if low <= previous.1 || adjacent {
                            previous.1 = previous.1.max(high);
                            continue;
                        }
                    }
                    merged.push((low, high));
                }
                work.charge(u64::try_from(merged.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
                let mut intervals = Vec::with_capacity(merged.len());
                for (low_period, high_period) in merged {
                    work.charge(1)?;
                    let width = usize::try_from(
                        high_period
                            .checked_sub(low_period)
                            .ok_or(ResourceError::SizeOverflow)?,
                    )
                    .map_err(|_| ResourceError::SizeOverflow)?
                    .checked_add(1)
                    .ok_or(ResourceError::SizeOverflow)?;
                    intervals.push(NativeCutOccurrenceInterval {
                        low_period,
                        high_period,
                        family_base: u32::try_from(native_cuts)
                            .map_err(|_| ResourceError::SizeOverflow)?,
                    });
                    native_cuts = native_cuts
                        .checked_add(width)
                        .ok_or(ResourceError::SizeOverflow)?;
                }
                native_cut_ranges.push(NativeCutOccurrenceRange { intervals });
            }
            bounds.push(FeatureFamilyBound {
                publication: publication_count,
                native_cuts,
                native_cut_ranges,
            });
        }
        try_sort_by_with_work(&mut region_families, work, |left, right, _work| {
            Ok(left.cmp(right))
        })?;
        work.charge(
            u64::try_from(region_families.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        region_families.dedup();
        Ok(Self {
            bounds,
            region_families,
        })
    }

    pub(super) fn into_registry(self, source_base: usize) -> FeatureFamilyRegistry {
        FeatureFamilyRegistry {
            bounds: self.bounds,
            region_families: self.region_families,
            families: FallibleAvl::with_capacity(0),
            source_base,
        }
    }
}

impl FeatureFamilyRegistry {
    pub(super) fn native_cut_occurrence(
        &self,
        parent: FragmentRecipeId,
        trace_edge: usize,
        seam_period_index: i64,
        work: &mut impl Work,
    ) -> Result<u32> {
        work.charge(1)?;
        let bound = self.bounds.get(parent.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("native cut family parent is unknown".into())
        })?;
        work.charge(1)?;
        let range = bound.native_cut_ranges.get(trace_edge).ok_or_else(|| {
            GeometryErrorKind::Invalid("native cut family leaf is unknown".into())
        })?;
        let mut low = 0_usize;
        let mut high = range.intervals.len();
        let mut found = None;
        while low < high {
            work.charge(1)?;
            let middle = low
                .checked_add(high.checked_sub(low).ok_or(ResourceError::SizeOverflow)? / 2)
                .ok_or(ResourceError::SizeOverflow)?;
            let interval = range.intervals.get(middle).ok_or_else(|| {
                GeometryErrorKind::Invalid("native cut period interval is unknown".into())
            })?;
            if seam_period_index < interval.low_period {
                high = middle;
            } else if seam_period_index > interval.high_period {
                low = middle.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
            } else {
                found = Some(interval);
                break;
            }
        }
        let interval = found.ok_or_else(|| {
            GeometryErrorKind::Invalid(
                "native cut seam lies outside its retained occurrence ranges".into(),
            )
        })?;
        let within = u32::try_from(
            seam_period_index
                .checked_sub(interval.low_period)
                .ok_or(ResourceError::SizeOverflow)?,
        )
        .map_err(|_| ResourceError::SizeOverflow)?;
        interval
            .family_base
            .checked_add(within)
            .ok_or_else(|| ResourceError::SizeOverflow.into())
    }

    pub(super) fn register(&mut self, family: FeatureFamily, work: &mut impl Work) -> Result<bool> {
        work.charge(1)?;
        let bound = match family.parent {
            FeatureFamilyParent::Fragment(recipe) => {
                self.bounds.get(recipe.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "feature family references an unknown producer".into(),
                    )
                })?
            },
            FeatureFamilyParent::Region(_) => {
                return self.register_region_family(family, work);
            },
        };
        let occurrence =
            usize::try_from(family.occurrence).map_err(|_| ResourceError::SizeOverflow)?;
        let limit = match family.producer {
            SampleProducerKind::Publication => bound.publication,
            SampleProducerKind::NativeCut => bound.native_cuts,
            SampleProducerKind::PolePresentation(_) | SampleProducerKind::WholeSphereCover => {
                return Err(GeometryErrorKind::Invalid(
                    "region presentation family has a fragment parent".into(),
                )
                .into());
            },
        };
        if occurrence >= limit {
            return Err(GeometryErrorKind::Invalid(
                "materialized feature exceeds its retained producer-family bound".into(),
            )
            .into());
        }
        self.families
            .insert_with_work(family, work, &mut |left, right, _work| Ok(left.cmp(right)))
    }

    fn register_region_family(
        &mut self,
        family: FeatureFamily,
        work: &mut impl Work,
    ) -> Result<bool> {
        if !matches!(
            family.producer,
            SampleProducerKind::PolePresentation(_) | SampleProducerKind::WholeSphereCover
        ) {
            return Err(GeometryErrorKind::Invalid(
                "source feature family has a region parent".into(),
            )
            .into());
        }
        let mut low = 0_usize;
        let mut high = self.region_families.len();
        let mut found = false;
        while low < high {
            work.charge(1)?;
            let middle = low
                .checked_add(high.checked_sub(low).ok_or(ResourceError::SizeOverflow)? / 2)
                .ok_or(ResourceError::SizeOverflow)?;
            let candidate = self.region_families.get(middle).ok_or_else(|| {
                GeometryErrorKind::Invalid("native region family index is unknown".into())
            })?;
            match candidate.cmp(&family) {
                std::cmp::Ordering::Less => {
                    low = middle.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
                },
                std::cmp::Ordering::Greater => high = middle,
                std::cmp::Ordering::Equal => {
                    found = true;
                    break;
                },
            }
        }
        if !found {
            return Err(GeometryErrorKind::Invalid(
                "materialized region family is outside the frozen template universe".into(),
            )
            .into());
        }
        self.families
            .insert_with_work(family, work, &mut |left, right, _work| Ok(left.cmp(right)))
    }

    pub(super) const fn additional_constructed(&self) -> usize {
        self.families.len()
    }

    pub(super) fn synchronize_budget(&self, budget: &mut impl SearchAccount) -> Result<()> {
        budget.charge(1)?;
        let constructed = self
            .source_base
            .checked_add(self.additional_constructed())
            .ok_or(ResourceError::SizeOverflow)?;
        budget.budget_mut().include_constructed(constructed)
    }
}

impl FragmentRecipes {
    pub(super) fn as_slice(&self) -> &[FragmentRecipe] {
        &self.values
    }

    pub(super) fn record_walk(&mut self, owner: RecipeWalkOwner, fragments: Vec<FragmentRecipeId>) {
        self.walks.push(RecipeWalk { owner, fragments });
    }

    pub(super) fn walks(&self) -> &[RecipeWalk] {
        &self.walks
    }

    #[cfg(test)]
    pub(super) fn source_trace(
        &self,
        walk: &RecipeWalk,
        work: &mut impl Work,
    ) -> Result<SourceTrace> {
        self.source_trace_fragments(&walk.fragments, work)
    }

    pub(super) fn source_trace_fragment(
        &self,
        recipe: FragmentRecipeId,
        work: &mut impl Work,
    ) -> Result<SourceTrace> {
        self.source_trace_fragments(std::slice::from_ref(&recipe), work)
    }

    fn source_trace_fragments(
        &self,
        fragments: &[FragmentRecipeId],
        work: &mut impl Work,
    ) -> Result<SourceTrace> {
        let mut rows = TraceRowsBuilder::new();
        for &fragment_id in fragments {
            work.charge(1)?;
            let fragment = self.values.get(fragment_id.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("source trace fragment is unknown".into())
            })?;
            let mut row = Vec::new();
            for range in &fragment.ranges {
                work.charge(1)?;
                let root = RootId::from_index(range.source_edge.0);
                if range.endpoint_only {
                    row.push(RootRange::point_with_work(
                        root,
                        range.range.start.try_clone_with_work(work)?,
                        range.producer_start.try_clone_with_work(work)?,
                        work,
                    )?);
                    row.push(RootRange::point_with_work(
                        root,
                        range.range.end.try_clone_with_work(work)?,
                        range.producer_end.try_clone_with_work(work)?,
                        work,
                    )?);
                } else {
                    row.push(RootRange::from_mapped_with_work(
                        root,
                        range.range.start.try_clone_with_work(work)?,
                        range.range.end.try_clone_with_work(work)?,
                        range.producer_start.try_clone_with_work(work)?,
                        range.producer_end.try_clone_with_work(work)?,
                        work,
                    )?);
                }
            }
            rows.push_row(row)?;
        }
        Ok(SourceTrace {
            rows: rows.finish(work)?,
            samples: Vec::new(),
        })
    }

    #[cfg(test)]
    pub(super) fn publish(
        &self,
        topology: &ExactGeographicTopology,
        coordinates: &super::q_embedding::QCoordinates,
        operation: &'static str,
        work: &mut impl Work,
    ) -> Result<PublicationInventory> {
        let mut budget = ExpansionBudget::new(operation, "computed edges");
        self.publish_with_expansion_budget(topology, coordinates, &mut budget, work)
    }

    pub(super) fn publish_with_expansion_budget(
        &self,
        topology: &ExactGeographicTopology,
        coordinates: &super::q_embedding::QCoordinates,
        budget: &mut ExpansionBudget,
        work: &mut impl Work,
    ) -> Result<PublicationInventory> {
        let mut fragments = Vec::with_capacity(self.values.len());
        let mut samples = Vec::new();
        let mut counts = vec![0_usize; self.values.len()];
        for recipe in &self.values {
            work.charge(1)?;
            let exact_endpoints = recipe.reference_endpoints(topology, work)?;
            let source_start_record =
                recipe.source_endpoint_record(topology, EndpointSide::Start, work)?;
            let source_end_record =
                recipe.source_endpoint_record(topology, EndpointSide::End, work)?;
            let start = match source_start_record {
                Some(point) => point,
                None => coordinates.point(topology, recipe.endpoints[0])?,
            };
            let end = match source_end_record {
                Some(point) => point,
                None => coordinates.point(topology, recipe.endpoints[1])?,
            };
            budget.add(2)?;
            let publisher_start = Point::new_raw_xy(start.x, start.y);
            let publisher_end = Point::new_raw_xy(end.x, end.y);
            let source = CoordSeq::try_from_points(&[publisher_start, publisher_end])?;
            let mut publication_samples = Vec::new();
            let publication = publish_computed_chain_with_vertex_intents_and_samples(
                topology.sources.frame,
                &source,
                false,
                &mut *budget,
                |_, _, _| {
                    Ok(ComputedEdgeIntent::exact_lifted([
                        &exact_endpoints[0].x,
                        &exact_endpoints[1].x,
                    ]))
                },
                |index, _| {
                    if (index == 0 && source_start_record.is_some())
                        || (index == 1 && source_end_record.is_some())
                    {
                        ComputedVertexIntent::Preserved
                    } else {
                        ComputedVertexIntent::Sampled
                    }
                },
                |_, _, _, parameter, _| {
                    let parameter = ExactRational::from_f64(parameter);
                    let y = interpolate_exact_reference_coordinate(
                        &exact_endpoints[0].y,
                        &exact_endpoints[1].y,
                        &parameter,
                        work,
                    )?
                    .round_with_work(work)?;
                    Ok(ComputedSampleOrdinates {
                        y,
                        z: ZOrdinate(None),
                        m: MOrdinate(None),
                    })
                },
                |edge_index, _, _, parameter, _, point| {
                    if parameter > 0.0 && parameter < 1.0 {
                        publication_samples.push((edge_index, parameter, point));
                    }
                    Ok(())
                },
            )?;
            let published_points =
                publication.unwrap_or_else(|| vec![publisher_start, publisher_end]);
            if publication_samples
                .iter()
                .any(|(edge_index, ..)| *edge_index != 0)
            {
                return Err(GeometryErrorKind::Invalid(
                    "single-fragment publication returned another source edge".into(),
                )
                .into());
            }
            let original_trace = self.source_trace_fragment(recipe.id, work)?;
            work.charge(
                u64::try_from(publication_samples.len())
                    .map_err(|_| ResourceError::SizeOverflow)?,
            )?;
            let mut trace_samples = Vec::with_capacity(publication_samples.len());
            for (sample_index, &(edge_index, parameter, _)) in
                publication_samples.iter().enumerate()
            {
                let written_vertex = sample_index
                    .checked_add(1)
                    .ok_or(ResourceError::SizeOverflow)?;
                trace_samples.push(SourceTraceSample {
                    edge_index,
                    local_parameter: ExactRational::from_f64(parameter),
                    written_vertex,
                });
            }
            let trace = original_trace.split_at_samples(&trace_samples, work)?;
            if trace.rows.edge_count().saturating_add(1) != published_points.len()
                || trace.samples.len() != publication_samples.len()
            {
                return Err(GeometryErrorKind::Invalid(
                    "publication points and source trace lost ordinal alignment".into(),
                )
                .into());
            }
            let mut published_points = published_points;
            if let Some(record) = source_start_record {
                *published_points.first_mut().ok_or_else(|| {
                    GeometryErrorKind::Invalid("publication chain has no start record".into())
                })? = record;
            }
            if let Some(record) = source_end_record {
                *published_points.last_mut().ok_or_else(|| {
                    GeometryErrorKind::Invalid("publication chain has no end record".into())
                })? = record;
            }
            let sample_start = samples.len();
            for (occurrence, (edge_index, parameter, point)) in
                publication_samples.into_iter().enumerate()
            {
                work.charge(1)?;
                let trace_sample = trace.samples.get(occurrence).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "publication trace lost an accepted sample row".into(),
                    )
                })?;
                let parameter = ExactRational::from_f64(parameter);
                let exact_point =
                    interpolate_exact_reference_point(&exact_endpoints, &parameter, work)?;
                let retained_site = retained_exact_native_site_with_work(
                    topology.sources.frame.longitude(),
                    &exact_point.x,
                    work,
                )?;
                let period_offset =
                    ExactRational::from_dyadic(ExactDyadic::from_i64(retained_site.periods()));
                let published_x = ExactRational::from_f64(point.x).try_add_with_work(
                    &ExactRational::from_f64(topology.sources.frame.longitude().period())
                        .try_product_with_work(&period_offset, work)?,
                    work,
                )?;
                if trace_sample.written_vertex != occurrence.saturating_add(1)
                    || trace_sample.edge != edge_index
                    || !trace_sample.parameter.try_eq_with_work(&parameter, work)?
                    || published_points.get(trace_sample.written_vertex) != Some(&point)
                {
                    return Err(GeometryErrorKind::Invalid(
                        "accepted publication sample does not match its written point".into(),
                    )
                    .into());
                }
                let request_row = RequestRowId::from_index(samples.len())?;
                samples.push(PublishedSample {
                    request_row,
                    family: FeatureFamily {
                        parent: FeatureFamilyParent::Fragment(recipe.id),
                        producer: SampleProducerKind::Publication,
                        occurrence: u32::try_from(occurrence)
                            .map_err(|_| ResourceError::SizeOverflow)?,
                    },
                    parameter,
                    written_vertex: trace_sample.written_vertex,
                    point,
                    exact_point,
                    published_chart_point: ExactChartPoint::from_rationals(
                        published_x,
                        ExactRational::from_f64(point.y),
                    ),
                    source_trace: trace_sample
                        .has_source
                        .then_some(TraceRequestLocation::Sample {
                            recipe: recipe.id,
                            row: trace_sample.request_row,
                        }),
                    endpoint_only: trace_sample.endpoint_only,
                });
            }
            let sample_end = samples.len();
            *counts.get_mut(recipe.id.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("publication recipe count is unknown".into())
            })? = trace.samples.len();
            fragments.push(PublishedFragment {
                recipe: recipe.id,
                points: published_points,
                trace,
                sample_range: sample_start..sample_end,
                source_records: [source_start_record, source_end_record],
                exact_endpoints,
            });
        }
        let ordinates = vec![NodeOrdinates::default(); samples.len()];
        Ok(PublicationInventory {
            fragments,
            samples,
            counts,
            ordinates,
        })
    }

    pub(super) fn push_source_edge(
        &mut self,
        endpoints: [QPortId; 2],
        source_edge: SourceEdgeId,
        range: ExactSourceRange,
        exact_displacement: ExactRational,
        use_kind: SelectedUse,
        work: &mut impl Work,
    ) -> Result<FragmentRecipeId> {
        work.charge(1)?;
        let id = FragmentRecipeId(
            u32::try_from(self.values.len()).map_err(|_| ResourceError::SizeOverflow)?,
        );
        self.values.push(FragmentRecipe {
            id,
            endpoints,
            ranges: vec![DirectedSourceRange {
                source_edge,
                range,
                emitted: true,
                producer_start: ExactRational::zero(),
                producer_end: ExactRational::one(),
                endpoint_only: false,
            }],
            exact_displacement,
            use_kind,
        });
        Ok(id)
    }

    pub(super) fn push_walk(
        &mut self,
        topology: &ExactGeographicTopology,
        spans: &[DirectedSpan],
        closed: bool,
        use_kind: SelectedUse,
        work: &mut impl Work,
    ) -> Result<Vec<FragmentRecipeId>> {
        let mut emitted = Vec::new();
        let mut first_port = None;
        let mut last_port = None;
        let mut fragment_start = spans.first().map(|span| span.from);
        let mut ranges = Vec::new();
        let mut longitude_displacement = ExactRational::zero();
        let mut latitude_displacement = ExactRational::zero();
        let mut producer_supported = true;
        let mut previous_to: Option<QPortId> = None;

        for directed in spans {
            work.charge(1)?;
            let span = topology
                .q
                .spans
                .get(directed.span.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("emission recipe span is unknown".into())
                })?;
            if !span.ports.contains(&directed.from) || !span.ports.contains(&directed.to) {
                return Err(GeometryErrorKind::Invalid(
                    "emission recipe endpoints do not belong to their span".into(),
                )
                .into());
            }
            if directed.owner_index >= span.owners.len() {
                return Err(GeometryErrorKind::Invalid(
                    "emission recipe owner is outside its source span".into(),
                )
                .into());
            }
            let from = topology
                .q
                .ports
                .get(directed.from.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("emission recipe start port is unknown".into())
                })?;
            let to = topology
                .q
                .ports
                .get(directed.to.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("emission recipe end port is unknown".into())
                })?;
            if let Some(previous) = previous_to {
                let previous = topology.q.ports.get(previous.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("emission recipe previous port is unknown".into())
                })?;
                if previous.node != from.node {
                    return Err(GeometryErrorKind::Invalid(
                        "emission recipe walk contains disconnected spans".into(),
                    )
                    .into());
                }
            }
            if !from.cut {
                match last_port {
                    None => {
                        if !ranges.is_empty() {
                            let start = fragment_start.ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "emission recipe has no current start port".into(),
                                )
                            })?;
                            self.flush_pending_fragment(
                                start,
                                directed.from,
                                &mut ranges,
                                &mut longitude_displacement,
                                &mut latitude_displacement,
                                producer_supported,
                                use_kind,
                                &mut emitted,
                                work,
                            )?;
                        }
                        first_port = Some(directed.from);
                        last_port = Some(directed.from);
                        fragment_start = Some(directed.from);
                        producer_supported = true;
                    },
                    Some(previous)
                        if previous != directed.from || fragment_start != Some(directed.from) =>
                    {
                        if !ranges.is_empty() {
                            let start = fragment_start.ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "emission recipe has no current start port".into(),
                                )
                            })?;
                            self.flush_pending_fragment(
                                start,
                                directed.from,
                                &mut ranges,
                                &mut longitude_displacement,
                                &mut latitude_displacement,
                                producer_supported,
                                use_kind,
                                &mut emitted,
                                work,
                            )?;
                        }
                        last_port = Some(directed.from);
                        fragment_start = Some(directed.from);
                        producer_supported = true;
                    },
                    Some(_) => {},
                }
            } else if fragment_start.is_none() {
                fragment_start = Some(directed.from);
            }

            let longitude_step = to.point.x.try_subtract_with_work(&from.point.x, work)?;
            let latitude_step = to.point.y.try_subtract_with_work(&from.point.y, work)?;
            let collapsed_pole = topology
                .q
                .span_is_collapsed_pole(directed.span.0 as usize)?;
            let point_support =
                collapsed_pole || (longitude_step.is_zero() && latitude_step.is_zero());
            let mut producer_longitude_end =
                longitude_displacement.try_add_with_work(&longitude_step, work)?;
            let mut producer_latitude_end =
                latitude_displacement.try_add_with_work(&latitude_step, work)?;
            if !ranges.is_empty() {
                let extends_affine_run = producer_step_extends_run(
                    &longitude_displacement,
                    &latitude_displacement,
                    &longitude_step,
                    &latitude_step,
                    producer_supported,
                    point_support,
                    work,
                )?;
                if !extends_affine_run {
                    let previous = previous_to.ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "non-affine producer transition has no preceding Q port".into(),
                        )
                    })?;
                    let start = fragment_start.ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "non-affine producer transition has no recipe start port".into(),
                        )
                    })?;
                    self.flush_pending_fragment(
                        start,
                        previous,
                        &mut ranges,
                        &mut longitude_displacement,
                        &mut latitude_displacement,
                        producer_supported,
                        use_kind,
                        &mut emitted,
                        work,
                    )?;
                    fragment_start = Some(directed.from);
                    producer_supported = true;
                    producer_longitude_end = longitude_step.try_clone_with_work(work)?;
                    producer_latitude_end = latitude_step.try_clone_with_work(work)?;
                }
            }
            let pending = pending_source_ranges(
                span,
                *directed,
                &longitude_displacement,
                &producer_longitude_end,
                &latitude_displacement,
                &producer_latitude_end,
                work,
            )?;
            producer_supported &= !point_support;
            ranges.extend(pending);
            longitude_displacement = producer_longitude_end;
            latitude_displacement = producer_latitude_end;
            previous_to = Some(directed.to);

            if point_support {
                let start = fragment_start.ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "point producer support has no recipe start port".into(),
                    )
                })?;
                self.flush_pending_fragment(
                    start,
                    directed.to,
                    &mut ranges,
                    &mut longitude_displacement,
                    &mut latitude_displacement,
                    false,
                    use_kind,
                    &mut emitted,
                    work,
                )?;
                fragment_start = Some(directed.to);
                producer_supported = true;
                if !to.cut {
                    first_port.get_or_insert(directed.to);
                    last_port = Some(directed.to);
                }
            } else if !to.cut {
                let start = fragment_start.ok_or_else(|| {
                    GeometryErrorKind::Invalid("emission recipe has no current start port".into())
                })?;
                self.flush_pending_fragment(
                    start,
                    directed.to,
                    &mut ranges,
                    &mut longitude_displacement,
                    &mut latitude_displacement,
                    producer_supported,
                    use_kind,
                    &mut emitted,
                    work,
                )?;
                fragment_start = Some(directed.to);
                producer_supported = true;
                first_port.get_or_insert(directed.to);
                last_port = Some(directed.to);
            }
        }

        if !ranges.is_empty() {
            let start = fragment_start.ok_or_else(|| {
                GeometryErrorKind::Invalid("emission recipe has no final start port".into())
            })?;
            let end = spans.last().map_or(start, |span| span.to);
            self.flush_pending_fragment(
                start,
                end,
                &mut ranges,
                &mut longitude_displacement,
                &mut latitude_displacement,
                producer_supported,
                use_kind,
                &mut emitted,
                work,
            )?;
        } else if closed
            && let (Some(first), Some(last)) = (first_port, last_port)
            && first != last
        {
            self.push_fragment(
                last,
                first,
                Vec::new(),
                ExactRational::zero(),
                ExactRational::zero(),
                true,
                use_kind,
                &mut emitted,
                work,
            )?;
        }
        Ok(emitted)
    }

    fn flush_pending_fragment(
        &mut self,
        from: QPortId,
        to: QPortId,
        ranges: &mut Vec<PendingSourceRange>,
        longitude_displacement: &mut ExactRational,
        latitude_displacement: &mut ExactRational,
        support_allowed: bool,
        use_kind: SelectedUse,
        emitted: &mut Vec<FragmentRecipeId>,
        work: &mut impl Work,
    ) -> Result<()> {
        if ranges.is_empty() {
            return Ok(());
        }
        self.push_fragment(
            from,
            to,
            std::mem::take(ranges),
            std::mem::replace(longitude_displacement, ExactRational::zero()),
            std::mem::replace(latitude_displacement, ExactRational::zero()),
            support_allowed,
            use_kind,
            emitted,
            work,
        )
    }

    fn push_fragment(
        &mut self,
        from: QPortId,
        to: QPortId,
        ranges: Vec<PendingSourceRange>,
        longitude_displacement: ExactRational,
        latitude_displacement: ExactRational,
        support_allowed: bool,
        use_kind: SelectedUse,
        emitted: &mut Vec<FragmentRecipeId>,
        work: &mut impl Work,
    ) -> Result<()> {
        work.charge(1)?;
        let ranges = producer_ranges(
            ranges,
            &longitude_displacement,
            &latitude_displacement,
            support_allowed,
            work,
        )?;
        let id = FragmentRecipeId(
            u32::try_from(self.values.len()).map_err(|_| ResourceError::SizeOverflow)?,
        );
        self.values.push(FragmentRecipe {
            id,
            endpoints: [from, to],
            ranges,
            exact_displacement: longitude_displacement,
            use_kind,
        });
        emitted.push(id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geometry::Unmetered;
    use crate::geometry::embedding_completion::{
        ExactPoints, admit_known_graph, visit_shape_literals,
    };
    use crate::geometry::ordinates::source_ordinate_values_at_fraction;

    fn pending_source_range(
        source_edge: u32,
        longitude_start: f64,
        longitude_end: f64,
        latitude_start: f64,
        latitude_end: f64,
    ) -> PendingSourceRange {
        PendingSourceRange {
            source_edge: SourceEdgeId(source_edge),
            range: ExactSourceRange {
                start: ExactRational::zero(),
                end: ExactRational::one(),
            },
            emitted: true,
            longitude_start: ExactRational::from_f64(longitude_start),
            longitude_end: ExactRational::from_f64(longitude_end),
            latitude_start: ExactRational::from_f64(latitude_start),
            latitude_end: ExactRational::from_f64(latitude_end),
        }
    }

    fn endpoint_test_line(start: Point, end: Point) -> Result<Shape> {
        Ok(Shape::LineString(crate::geometry::LineSeq::try_new(
            CoordSeq::try_from_points(&[start, end])?,
        )?))
    }

    fn endpoint_test_topology(shapes: &[Shape]) -> Result<ExactGeographicTopology> {
        let frame = GeographicFrame::crs84();
        let inputs = crate::geographic::GeographicInputs::try_from_shapes(frame, shapes.iter())
            .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
        ExactGeographicTopology::from_views(frame, inputs.views().iter().copied())
    }

    fn resolve_native_root_occurrences<M: ChartMetric>(
        topology: &ExactGeographicTopology,
        port_id: QPortId,
        root_requests: &[(RootId, ExactRational)],
        metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
        strict: bool,
    ) -> Result<(Vec<NodeOrdinates>, CoordinateAxes)> {
        let mut work = Unmetered;
        let q_port = topology.q.ports.get(port_id.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("source-root test Q port is unknown".into())
        })?;
        let node_registry =
            EmissionNodeRegistry::with_sources(topology, &[q_port.node], &mut work)?;
        let node = node_registry.source_node(q_port.node)?;
        let mut requests = Vec::with_capacity(root_requests.len());
        for (root, parameter) in root_requests {
            requests.push(RootRequest::new_with_work(
                *root,
                parameter.try_clone_with_work(&mut work)?,
                &mut work,
            )?);
        }
        let occurrences = native_source_sample_occurrences(None, &requests, &mut work)?;
        let occurrence_count = occurrences.len();
        let mut written_occurrences = Vec::with_capacity(occurrence_count);
        for (index, occurrence) in occurrences.into_iter().enumerate() {
            written_occurrences.push(WrittenOccurrence {
                origin: occurrence.origin,
                source_trace: occurrence.source_trace,
                request_row: RequestRowId::from_index(index)?,
            });
        }
        let plan = ExpandedEmissionPlan {
            topology,
            selected_face_roots: Vec::new(),
            node_registry,
            ports: vec![EmissionPort {
                node,
                position: PortPosition::Projected {
                    spelling: PortSpelling::Interior { periods: 0 },
                },
                source_record: None,
                occurrence_range: 0..occurrence_count,
            }],
            occurrences: written_occurrences,
            chords: Vec::new(),
            parts: Vec::new(),
            lines: Vec::new(),
            points: Vec::new(),
            whole_sphere: false,
        };
        let publication = PublicationInventory {
            fragments: Vec::new(),
            samples: Vec::new(),
            counts: Vec::new(),
            ordinates: Vec::new(),
        };
        let source_ordinates = vec![NodeOrdinates::default(); topology.q.nodes.len()];
        resolve_emission_ordinates(
            &plan,
            &publication,
            &source_ordinates,
            topology,
            CoordinateAxes::XYZM,
            strict,
            "difference",
            metric_access,
            &mut work,
        )
    }

    #[expect(
        clippy::float_cmp,
        reason = "source-root coordinates are exact stored values, and the fixture rejects horizontal edges"
    )]
    fn source_edge_varies_in_latitude_and_ordinates(source: &[Point; 2]) -> bool {
        source[0].y != source[1].y
            && (source[0].z() != source[1].z() || source[0].m() != source[1].m())
    }

    fn source_requests_at_shared_port(
        topology: &ExactGeographicTopology,
        minimum_roots: usize,
        varying_source_edge: bool,
    ) -> Result<(QPortId, Vec<(RootId, ExactRational)>)> {
        for port in &topology.q.ports {
            let mut requests = Vec::new();
            for span in &topology.q.spans {
                for (endpoint, &span_port) in span.ports.iter().enumerate() {
                    if span_port != port.id {
                        continue;
                    }
                    for owner in &span.owners {
                        let parameter = if endpoint == 0 {
                            if owner.reversed {
                                &owner.source_range.end
                            } else {
                                &owner.source_range.start
                            }
                        } else if owner.reversed {
                            &owner.source_range.start
                        } else {
                            &owner.source_range.end
                        };
                        let request = (RootId::from_index(owner.source_edge.0), parameter.clone());
                        if !requests.contains(&request) {
                            requests.push(request);
                        }
                    }
                }
            }
            let distinct_roots = requests
                .iter()
                .map(|(root, _)| *root)
                .collect::<std::collections::BTreeSet<_>>();
            let has_varying_source_edge = requests.iter().any(|(root, parameter)| {
                topology
                    .sources
                    .edges
                    .get(root.index())
                    .is_some_and(|edge| {
                        source_edge_varies_in_latitude_and_ordinates(&edge.source)
                            && !parameter.is_zero()
                            && !parameter.is_one()
                    })
            });
            if distinct_roots.len() >= minimum_roots
                && (!varying_source_edge || has_varying_source_edge)
            {
                return Ok((port.id, requests));
            }
        }
        Err(GeometryErrorKind::Invalid(
            "source-root test has no shared port with enough distinct source roots".into(),
        )
        .into())
    }

    fn endpoint_test_recipe(
        endpoints: [QPortId; 2],
        source_edge: SourceEdgeId,
        source_start: ExactRational,
        source_end: ExactRational,
    ) -> FragmentRecipe {
        FragmentRecipe {
            id: FragmentRecipeId(0),
            endpoints,
            ranges: vec![DirectedSourceRange {
                source_edge,
                range: ExactSourceRange {
                    start: source_start,
                    end: source_end,
                },
                emitted: true,
                producer_start: ExactRational::zero(),
                producer_end: ExactRational::one(),
                endpoint_only: false,
            }],
            exact_displacement: ExactRational::from_f64(1.0),
            use_kind: SelectedUse::Curve,
        }
    }

    fn assert_source_record_bits(actual: Option<Point>, expected: Point) {
        let actual = actual.expect("authenticated source endpoint must be retained");
        assert_eq!(actual.x.to_bits(), expected.x.to_bits());
        assert_eq!(actual.y.to_bits(), expected.y.to_bits());
        assert_eq!(actual.z().map(f64::to_bits), expected.z().map(f64::to_bits));
        assert_eq!(actual.m().map(f64::to_bits), expected.m().map(f64::to_bits));
    }

    #[test]
    fn source_endpoint_record_uses_local_side_and_composed_root_side() -> Result<()> {
        let start = Point::new_raw_axes(-0.0, -0.0, ZOrdinate(Some(-0.0)), MOrdinate(Some(17.0)));
        let end = Point::new_raw_axes(10.0, 0.0, ZOrdinate(Some(23.0)), MOrdinate(Some(29.0)));
        let topology = endpoint_test_topology(&[endpoint_test_line(start, end)?])?;
        let edge =
            topology.sources.edges.first().ok_or_else(|| {
                GeometryErrorKind::Invalid("endpoint test edge is missing".into())
            })?;
        let ports = *topology
            .q
            .source_edge_ports
            .get(edge.id.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("endpoint test ports are missing".into()))?;
        let zero = ExactRational::zero();
        let one = ExactRational::one();
        let mut work = Unmetered;

        let forward = endpoint_test_recipe(ports, edge.id, zero.clone(), one.clone());
        assert_source_record_bits(
            forward.source_endpoint_record(&topology, EndpointSide::Start, &mut work)?,
            edge.source[0],
        );
        assert_source_record_bits(
            forward.source_endpoint_record(&topology, EndpointSide::End, &mut work)?,
            edge.source[1],
        );

        let reverse = endpoint_test_recipe([ports[1], ports[0]], edge.id, one, zero);
        assert_source_record_bits(
            reverse.source_endpoint_record(&topology, EndpointSide::Start, &mut work)?,
            edge.source[1],
        );
        assert_source_record_bits(
            reverse.source_endpoint_record(&topology, EndpointSide::End, &mut work)?,
            edge.source[0],
        );
        Ok(())
    }

    #[test]
    fn partial_root_endpoint_does_not_claim_an_authentic_source_record() {
        let topology = endpoint_test_topology(&[endpoint_test_line(
            Point::new_raw_xy(0.0, 0.0),
            Point::new_raw_xy(10.0, 0.0),
        )
        .unwrap()])
        .unwrap();
        let edge = topology
            .sources
            .edges
            .first()
            .ok_or_else(|| GeometryErrorKind::Invalid("endpoint test edge is missing".into()))
            .unwrap();
        let ports = *topology
            .q
            .source_edge_ports
            .get(edge.id.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("endpoint test ports are missing".into()))
            .unwrap();
        let recipe = endpoint_test_recipe(
            ports,
            edge.id,
            ExactRational::from_f64(0.25),
            ExactRational::from_f64(0.75),
        );
        let mut work = Unmetered;

        assert!(
            recipe
                .source_endpoint_record(&topology, EndpointSide::Start, &mut work)
                .unwrap()
                .is_none()
        );
        assert!(
            recipe
                .source_endpoint_record(&topology, EndpointSide::End, &mut work)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn source_endpoint_record_accepts_each_matching_owner_orientation() {
        let first = crate::geometry::LineSeq::try_new(
            CoordSeq::try_from_points(&[
                Point::new_raw_axes(0.0, 0.0, ZOrdinate(Some(11.0)), MOrdinate(Some(101.0))),
                Point::new_raw_axes(10.0, 0.0, ZOrdinate(Some(12.0)), MOrdinate(Some(102.0))),
            ])
            .unwrap(),
        )
        .unwrap();
        let second = crate::geometry::LineSeq::try_new(
            CoordSeq::try_from_points(&[
                Point::new_raw_axes(10.0, 0.0, ZOrdinate(Some(21.0)), MOrdinate(Some(201.0))),
                Point::new_raw_axes(0.0, 0.0, ZOrdinate(Some(22.0)), MOrdinate(Some(202.0))),
            ])
            .unwrap(),
        )
        .unwrap();
        let shape = Shape::multi_line_string(
            crate::geometry::CoordinateAxes::new(
                crate::geometry::HasZ(true),
                crate::geometry::HasM(true),
            ),
            vec![first, second],
        );
        let topology = endpoint_test_topology(&[shape]).unwrap();
        assert_eq!(topology.sources.edges.len(), 2);
        let first_edge = topology
            .sources
            .edges
            .first()
            .ok_or_else(|| GeometryErrorKind::Invalid("first endpoint owner is missing".into()))
            .unwrap();
        let second_edge = topology
            .sources
            .edges
            .get(1)
            .ok_or_else(|| GeometryErrorKind::Invalid("second endpoint owner is missing".into()))
            .unwrap();
        let endpoints = *topology
            .q
            .source_edge_ports
            .get(first_edge.id.0 as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("first endpoint owner ports are missing".into())
            })
            .unwrap();
        let second_endpoints = *topology
            .q
            .source_edge_ports
            .get(second_edge.id.0 as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("second endpoint owner ports are missing".into())
            })
            .unwrap();
        assert_eq!(endpoints, [second_endpoints[1], second_endpoints[0]]);
        let zero = ExactRational::zero();
        let one = ExactRational::one();
        let mut recipe = endpoint_test_recipe(endpoints, first_edge.id, zero.clone(), one.clone());
        recipe.ranges.push(DirectedSourceRange {
            source_edge: second_edge.id,
            range: ExactSourceRange {
                start: one,
                end: zero,
            },
            emitted: true,
            producer_start: ExactRational::zero(),
            producer_end: ExactRational::one(),
            endpoint_only: false,
        });
        let mut work = Unmetered;

        assert_source_record_bits(
            recipe
                .source_endpoint_record(&topology, EndpointSide::Start, &mut work)
                .unwrap(),
            first_edge.source[0],
        );
        assert_source_record_bits(
            recipe
                .source_endpoint_record(&topology, EndpointSide::End, &mut work)
                .unwrap(),
            first_edge.source[1],
        );

        recipe.ranges.swap(0, 1);
        assert_source_record_bits(
            recipe
                .source_endpoint_record(&topology, EndpointSide::Start, &mut work)
                .unwrap(),
            second_edge.source[1],
        );
        assert_source_record_bits(
            recipe
                .source_endpoint_record(&topology, EndpointSide::End, &mut work)
                .unwrap(),
            second_edge.source[0],
        );
    }

    #[test]
    fn publication_preserves_authentic_start_and_samples_constructed_end() {
        let horizontal = endpoint_test_line(
            Point::new_raw_axes(0.0, 0.0, ZOrdinate(Some(7.0)), MOrdinate(Some(11.0))),
            Point::new_raw_axes(10.0, 0.0, ZOrdinate(Some(13.0)), MOrdinate(Some(17.0))),
        )
        .unwrap();
        let vertical =
            endpoint_test_line(Point::new_raw_xy(5.0, -1.0), Point::new_raw_xy(5.0, 1.0)).unwrap();
        let source = Shape::geometry_collection(vec![horizontal, vertical]).unwrap();
        let topology = endpoint_test_topology(&[source]).unwrap();
        let edge = topology
            .sources
            .edges
            .iter()
            .find(|edge| {
                edge.source[0].x == 0.0
                    && edge.source[0].y == 0.0
                    && edge.source[1].x.to_bits() == 10.0_f64.to_bits()
                    && edge.source[1].y == 0.0
            })
            .ok_or_else(|| GeometryErrorKind::Invalid("horizontal source edge is missing".into()))
            .unwrap();
        let [start_port, _] = *topology
            .q
            .source_edge_ports
            .get(edge.id.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("horizontal source ports are missing".into()))
            .unwrap();
        let constructed_end = topology
            .q
            .ports
            .iter()
            .find(|port| {
                port.point.x == ExactRational::from_f64(5.0)
                    && port.point.y == ExactRational::zero()
            })
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("constructed crossing port is missing".into())
            })
            .unwrap();
        let mut work = Unmetered;
        let displacement = constructed_end
            .point
            .x
            .try_subtract_with_work(
                &topology
                    .q
                    .ports
                    .get(start_port.0 as usize)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid("horizontal start port is unknown".into())
                    })
                    .unwrap()
                    .point
                    .x,
                &mut work,
            )
            .unwrap();
        let recipe = FragmentRecipe {
            id: FragmentRecipeId(0),
            endpoints: [start_port, constructed_end.id],
            ranges: vec![DirectedSourceRange {
                source_edge: edge.id,
                range: ExactSourceRange {
                    start: ExactRational::zero(),
                    end: ExactRational::from_f64(0.5),
                },
                emitted: true,
                producer_start: ExactRational::zero(),
                producer_end: ExactRational::one(),
                endpoint_only: false,
            }],
            exact_displacement: displacement,
            use_kind: SelectedUse::Curve,
        };
        let coordinates = super::super::q_embedding::QCoordinates::build(&topology).unwrap();
        let recipes = FragmentRecipes {
            values: vec![recipe],
            walks: Vec::new(),
        };
        let publication = recipes
            .publish(&topology, &coordinates, "endpoint intent", &mut work)
            .unwrap();
        let fragment = publication
            .fragments
            .first()
            .ok_or_else(|| GeometryErrorKind::Invalid("endpoint intent fragment is missing".into()))
            .unwrap();
        assert_source_record_bits(fragment.source_records[0], edge.source[0]);
        assert!(fragment.source_records[1].is_none());
        assert_eq!(
            fragment.exact_endpoints[0],
            topology.q.ports[start_port.0 as usize].point
        );
        assert_eq!(fragment.exact_endpoints[1], constructed_end.point);
        let end = fragment
            .points
            .last()
            .ok_or_else(|| GeometryErrorKind::Invalid("endpoint intent has no end".into()))
            .unwrap();
        assert_eq!(
            end.x.to_bits(),
            fragment.exact_endpoints[1]
                .x
                .round_with_work(&mut work)
                .unwrap()
                .to_bits()
        );
        assert_eq!(
            end.y.to_bits(),
            fragment.exact_endpoints[1]
                .y
                .round_with_work(&mut work)
                .unwrap()
                .to_bits()
        );
    }

    #[test]
    fn source_endpoint_record_admits_pole_latitude_but_keeps_its_spelling() {
        let frame = GeographicFrame::crs84();
        let pole = frame.pole();
        let first = crate::geometry::LineSeq::try_new(
            CoordSeq::try_from_points(&[
                Point::new_raw_axes(
                    17.0,
                    pole.next_up(),
                    ZOrdinate(Some(31.0)),
                    MOrdinate(Some(37.0)),
                ),
                Point::new_raw_axes(18.0, 80.0, ZOrdinate(Some(41.0)), MOrdinate(Some(43.0))),
            ])
            .unwrap(),
        )
        .unwrap();
        let second = crate::geometry::LineSeq::try_new(
            CoordSeq::try_from_points(&[
                Point::new_raw_axes(19.0, pole, ZOrdinate(Some(47.0)), MOrdinate(Some(53.0))),
                Point::new_raw_axes(20.0, 80.0, ZOrdinate(Some(59.0)), MOrdinate(Some(61.0))),
            ])
            .unwrap(),
        )
        .unwrap();
        let shape = Shape::multi_line_string(
            crate::geometry::CoordinateAxes::new(
                crate::geometry::HasZ(true),
                crate::geometry::HasM(true),
            ),
            vec![first, second],
        );
        let topology = endpoint_test_topology(&[shape]).unwrap();
        let first_edge = topology
            .sources
            .edges
            .first()
            .ok_or_else(|| GeometryErrorKind::Invalid("first pole edge is missing".into()))
            .unwrap();
        let second_edge = topology
            .sources
            .edges
            .get(1)
            .ok_or_else(|| GeometryErrorKind::Invalid("second pole edge is missing".into()))
            .unwrap();
        let first_ports = *topology
            .q
            .source_edge_ports
            .get(first_edge.id.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("first pole ports are missing".into()))
            .unwrap();
        let second_ports = *topology
            .q
            .source_edge_ports
            .get(second_edge.id.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("second pole ports are missing".into()))
            .unwrap();
        let first_pole = topology
            .q
            .ports
            .get(first_ports[0].0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("first pole port is missing".into()))
            .unwrap();
        let second_pole = topology
            .q
            .ports
            .get(second_ports[0].0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("second pole port is missing".into()))
            .unwrap();
        assert_eq!(first_pole.node, second_pole.node);
        assert_ne!(first_ports[0], second_ports[0]);
        let mut work = Unmetered;
        let matching = endpoint_test_recipe(
            first_ports,
            first_edge.id,
            ExactRational::zero(),
            ExactRational::one(),
        );
        assert_source_record_bits(
            matching
                .source_endpoint_record(&topology, EndpointSide::Start, &mut work)
                .unwrap(),
            first_edge.source[0],
        );
        let wrong_pole_spelling = endpoint_test_recipe(
            [second_ports[0], first_ports[1]],
            first_edge.id,
            ExactRational::zero(),
            ExactRational::one(),
        );
        assert!(
            wrong_pole_spelling
                .source_endpoint_record(&topology, EndpointSide::Start, &mut work)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn bent_and_backtracking_producers_are_not_flattened() -> Result<()> {
        let unsupported = [
            (
                vec![
                    pending_source_range(0, 0.0, 1.0, 0.0, 0.0),
                    pending_source_range(1, 1.0, 2.0, 0.0, 1.0),
                ],
                2.0,
                1.0,
            ),
            (
                vec![
                    pending_source_range(0, 0.0, 2.0, 0.0, 0.0),
                    pending_source_range(1, 2.0, 1.0, 0.0, 0.0),
                ],
                1.0,
                0.0,
            ),
        ];
        let mut work = Unmetered;
        for (ranges, longitude, latitude) in unsupported {
            producer_ranges(
                ranges,
                &ExactRational::from_f64(longitude),
                &ExactRational::from_f64(latitude),
                true,
                &mut work,
            )
            .unwrap_err();
        }
        Ok(())
    }

    #[test]
    fn endpoint_only_producer_keeps_two_endpoint_requests() {
        let quarter = ExactRational::from_f64(0.25);
        let three_quarters = ExactRational::from_f64(0.75);
        let mut work = Unmetered;
        let mapped = producer_ranges(
            vec![pending_source_range(4, 0.0, 10.0, 90.0, 90.0)],
            &ExactRational::from_f64(10.0),
            &ExactRational::zero(),
            false,
            &mut work,
        )
        .unwrap();
        let range = mapped
            .into_iter()
            .next()
            .ok_or_else(|| GeometryErrorKind::Invalid("point support range is missing".into()))
            .unwrap();
        assert!(range.endpoint_only);
        assert_eq!(range.producer_start, ExactRational::zero());
        assert_eq!(range.producer_end, ExactRational::one());

        let recipe = FragmentRecipe {
            id: FragmentRecipeId(0),
            endpoints: [QPortId(1), QPortId(2)],
            ranges: vec![DirectedSourceRange {
                source_edge: range.source_edge,
                range: ExactSourceRange {
                    start: quarter.clone(),
                    end: three_quarters.clone(),
                },
                emitted: range.emitted,
                producer_start: range.producer_start,
                producer_end: range.producer_end,
                endpoint_only: true,
            }],
            exact_displacement: ExactRational::from_f64(10.0),
            use_kind: SelectedUse::Curve,
        };
        let recipes = FragmentRecipes {
            values: vec![recipe],
            walks: Vec::new(),
        };
        let trace = recipes
            .source_trace_fragment(FragmentRecipeId(0), &mut work)
            .unwrap();
        assert_eq!(
            trace
                .rows
                .requests_at(0, &ExactRational::zero(), &mut work)
                .unwrap()
                .first()
                .ok_or_else(|| GeometryErrorKind::Invalid("point support lost its start".into()))
                .unwrap()
                .parameter(),
            &quarter
        );
        assert!(
            trace
                .rows
                .requests_at(0, &ExactRational::from_f64(0.5), &mut work)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            trace
                .rows
                .requests_at(0, &ExactRational::one(), &mut work)
                .unwrap()
                .first()
                .ok_or_else(|| GeometryErrorKind::Invalid("point support lost its end".into()))
                .unwrap()
                .parameter(),
            &three_quarters
        );
    }

    #[test]
    fn merging_vertex_aliases_keeps_presentation_and_source_rows_separate() {
        let point = ExactChartPoint::from_rationals(
            ExactRational::from_f64(12.0),
            ExactRational::from_f64(34.0),
        );
        let presentation = NativePortData {
            node: EmissionNodeId(3),
            point: point.clone(),
            spelling: PortSpelling::Interior { periods: 0 },
            source_record: None,
            occurrences: vec![OccurrenceSource {
                origin: VertexOrigin::Presentation,
                source_trace: None,
            }],
        };
        let source_sample = NativePortData {
            node: EmissionNodeId(3),
            point,
            spelling: PortSpelling::Interior { periods: 0 },
            source_record: None,
            occurrences: vec![OccurrenceSource {
                origin: VertexOrigin::SourceSample,
                source_trace: Some(TraceRequestLocation::Edge {
                    recipe: FragmentRecipeId(5),
                    edge: 0,
                    parameter: ExactRational::from_f64(0.5),
                }),
            }],
        };
        let mut work = Unmetered;
        let merged = merge_native_port_data(
            GeographicFrame::crs84(),
            &presentation,
            &source_sample,
            &mut work,
        )
        .unwrap();
        assert_eq!(merged.occurrences.len(), 2);
        assert!(matches!(
            merged.occurrences.first(),
            Some(OccurrenceSource {
                origin: VertexOrigin::Presentation,
                source_trace: None,
            })
        ));
        assert!(matches!(
            merged.occurrences.get(1),
            Some(OccurrenceSource {
                origin: VertexOrigin::SourceSample,
                source_trace: Some(_),
            })
        ));
    }

    #[test]
    fn merging_periodic_vertex_aliases_keeps_the_existing_chart_lift() {
        let y = ExactRational::from_f64(20.0);
        let left_point =
            ExactChartPoint::from_rationals(ExactRational::from_f64(-170.0), y.clone());
        let right_point = ExactChartPoint::from_rationals(ExactRational::from_f64(190.0), y);
        let alias = |point, periods| NativePortData {
            node: EmissionNodeId(3),
            point,
            spelling: PortSpelling::Interior { periods },
            source_record: None,
            occurrences: Vec::new(),
        };
        let left = alias(left_point.clone(), 0);
        let right = alias(right_point, 1);
        let mut work = Unmetered;

        let merged =
            merge_native_port_data(GeographicFrame::crs84(), &left, &right, &mut work).unwrap();

        assert_eq!(merged.point, left_point);
        assert!(matches!(merged.spelling, PortSpelling::Interior {
            periods: 0
        }));
    }

    #[test]
    fn native_vertex_merge_rejects_nonperiodic_longitude_aliases() {
        let frame = GeographicFrame::crs84();
        let point = |x, y| {
            ExactChartPoint::from_rationals(ExactRational::from_f64(x), ExactRational::from_f64(y))
        };
        let alias = |point, periods| NativePortData {
            node: EmissionNodeId(3),
            point,
            spelling: PortSpelling::Interior { periods },
            source_record: None,
            occurrences: Vec::new(),
        };
        let left = alias(point(-170.0, 20.0), 0);
        let mut work = Unmetered;

        assert!(
            merge_native_port_data(frame, &left, &alias(point(190.25, 20.0), 1), &mut work)
                .is_err(),
            "equal node IDs do not admit nonperiodic longitude differences"
        );
        assert!(
            merge_native_port_data(frame, &left, &alias(point(190.0, 20.0), 0), &mut work).is_err(),
            "one chart spelling cannot name two exact periodic lifts"
        );
    }

    #[test]
    fn native_vertex_merge_rejects_mismatched_admitted_latitudes() {
        let frame = GeographicFrame::crs84();
        let point = |x, y| {
            ExactChartPoint::from_rationals(ExactRational::from_f64(x), ExactRational::from_f64(y))
        };
        let alias = |point, periods| NativePortData {
            node: EmissionNodeId(3),
            point,
            spelling: PortSpelling::Interior { periods },
            source_record: None,
            occurrences: Vec::new(),
        };
        let left = alias(point(-170.0, 20.0), 0);
        let right = alias(point(190.0, 20.0_f64.next_up()), 1);
        let mut work = Unmetered;

        assert!(
            merge_native_port_data(frame, &left, &right, &mut work).is_err(),
            "one-ULP admitted latitude differences are not one assigned source image"
        );
    }

    #[test]
    fn native_vertex_merge_preserves_one_authenticated_periodic_source_record() {
        let frame = GeographicFrame::crs84();
        let y = ExactRational::from_f64(20.0);
        let left = NativePortData {
            node: EmissionNodeId(3),
            point: ExactChartPoint::from_rationals(ExactRational::from_f64(-170.0), y.clone()),
            spelling: PortSpelling::Interior { periods: 0 },
            source_record: Some(Point::new_raw_axes(
                190.0,
                20.0,
                ZOrdinate(Some(7.0)),
                MOrdinate(None),
            )),
            occurrences: Vec::new(),
        };
        let right = NativePortData {
            node: EmissionNodeId(3),
            point: ExactChartPoint::from_rationals(ExactRational::from_f64(190.0), y),
            spelling: PortSpelling::Interior { periods: 1 },
            source_record: None,
            occurrences: Vec::new(),
        };
        let mut work = Unmetered;

        let merged = merge_native_port_data(frame, &left, &right, &mut work).unwrap();

        assert_eq!(
            merged.source_record.map(|record| record.x.to_bits()),
            Some(190.0_f64.to_bits()),
            "the authenticated raw record is retained even when it uses another exact lift"
        );
        assert_eq!(merged.point, left.point);
    }

    #[test]
    fn native_vertex_merge_selects_the_record_for_its_retained_chart_lift() {
        let frame = GeographicFrame::crs84();
        let make_port = |longitude, periods, source_record| NativePortData {
            node: EmissionNodeId(3),
            point: ExactChartPoint::from_rationals(
                ExactRational::from_f64(longitude),
                ExactRational::from_f64(20.0),
            ),
            spelling: PortSpelling::Interior { periods },
            source_record: Some(Point::new_raw_xy(source_record, 20.0)),
            occurrences: Vec::new(),
        };
        let mut work = Unmetered;
        for (left_record, right_record) in [(190.0, -170.0), (-170.0, 190.0)] {
            let left = make_port(-170.0, 0, left_record);
            let right = make_port(190.0, 1, right_record);
            let merged = merge_native_port_data(frame, &left, &right, &mut work).unwrap();
            assert_eq!(
                merged.source_record.map(|record| record.x.to_bits()),
                Some((-170.0_f64).to_bits()),
                "the retained chart spelling, not contributor arrival order, selects the literal record"
            );
        }
    }

    #[test]
    fn native_vertex_merge_keeps_seam_banks_and_pole_spellings_distinct() {
        let frame = GeographicFrame::crs84();
        let point = |x, y| {
            ExactChartPoint::from_rationals(ExactRational::from_f64(x), ExactRational::from_f64(y))
        };
        let seam = |x, side| NativePortData {
            node: EmissionNodeId(3),
            point: point(x, 20.0),
            spelling: PortSpelling::Seam(side),
            source_record: None,
            occurrences: Vec::new(),
        };
        let mut work = Unmetered;
        assert!(
            merge_native_port_data(
                frame,
                &seam(180.0, ChartSide::East),
                &seam(-180.0, ChartSide::West),
                &mut work,
            )
            .is_err(),
            "opposite seam banks remain distinct ports"
        );

        let pole = |x, longitude| NativePortData {
            node: EmissionNodeId(4),
            point: point(x, 90.0),
            spelling: PortSpelling::Pole {
                native_longitude: longitude,
            },
            source_record: None,
            occurrences: Vec::new(),
        };
        assert!(
            merge_native_port_data(frame, &pole(10.0, 10.0), &pole(370.0, 10.0), &mut work,)
                .is_ok(),
            "same-pole coordinates may differ by an exact period when spelling is retained"
        );
        assert!(
            merge_native_port_data(frame, &pole(10.0, 10.0), &pole(11.0, 11.0), &mut work).is_err(),
            "same-pole identity does not erase the explicit longitude spelling"
        );
        assert!(
            merge_native_port_data(frame, &pole(10.0, 11.0), &pole(10.0, 11.0), &mut work).is_err(),
            "the pole spelling must remain an exact periodic image of the assigned point"
        );
    }

    #[test]
    fn same_seam_bank_occurrence_merges_at_a_pole() {
        let frame = GeographicFrame::crs84();
        let make_port = |side, origin| NativePortData {
            node: EmissionNodeId(4),
            point: ExactChartPoint::from_rationals(
                ExactRational::from_f64(match side {
                    ChartSide::East => frame.longitude().half_period(),
                    ChartSide::West => -frame.longitude().half_period(),
                }),
                ExactRational::from_f64(frame.chart_pole_latitude(ChartPole::North)),
            ),
            spelling: PortSpelling::Seam(side),
            source_record: None,
            occurrences: vec![OccurrenceSource {
                origin,
                source_trace: None,
            }],
        };
        let east_presentation = make_port(ChartSide::East, VertexOrigin::Presentation);
        let east_source = make_port(ChartSide::East, VertexOrigin::SourceNode(QNodeId(3)));
        let mut work = Unmetered;

        let merged =
            merge_native_port_data(frame, &east_presentation, &east_source, &mut work).unwrap();

        assert_eq!(merged.point, east_presentation.point);
        assert!(matches!(
            merged.spelling,
            PortSpelling::Seam(ChartSide::East)
        ));
        assert_eq!(merged.occurrences.len(), 2);
        let west = make_port(ChartSide::West, VertexOrigin::Presentation);
        assert!(
            merge_native_port_data(frame, &east_presentation, &west, &mut work).is_err(),
            "East and West seam-bank copies remain distinct at a pole"
        );
    }

    #[test]
    fn native_pole_presentation_splits_at_a_known_source_occurrence() {
        let frame = GeographicFrame::crs84();
        let pole_latitude = frame.chart_pole_latitude(ChartPole::North);
        let pole = NativePortData {
            node: EmissionNodeId(4),
            point: ExactChartPoint::from_rationals(
                ExactRational::from_f64(120.0),
                ExactRational::from_f64(pole_latitude),
            ),
            spelling: PortSpelling::Pole {
                native_longitude: 120.0,
            },
            source_record: Some(Point::new_raw_xy(120.0, pole_latitude)),
            occurrences: vec![OccurrenceSource {
                origin: VertexOrigin::SourceNode(QNodeId(7)),
                source_trace: None,
            }],
        };
        let lower = NativePortData {
            node: EmissionNodeId(5),
            point: ExactChartPoint::from_rationals(
                ExactRational::from_f64(110.0),
                ExactRational::from_f64(75.0),
            ),
            spelling: PortSpelling::Interior { periods: 0 },
            source_record: None,
            occurrences: Vec::new(),
        };
        let producer = NativeAreaEdge {
            vertices: [pole, lower],
            owner: NativeGraphEdgeOwner::Area { region: 0 },
            kind: NativeAreaEdgeKind::Producer {
                recipe: FragmentRecipeId(0),
                trace_edge: 0,
                producer_start: ExactRational::zero(),
                producer_end: ExactRational::one(),
                trace_ranges: Vec::new(),
                known_spans: Vec::new(),
            },
        };
        let isolated_pole = NativePortData {
            node: EmissionNodeId(4),
            point: ExactChartPoint::from_rationals(
                ExactRational::from_f64(100.0),
                ExactRational::from_f64(pole_latitude),
            ),
            spelling: PortSpelling::Pole {
                native_longitude: 100.0,
            },
            source_record: Some(Point::new_raw_xy(100.0, pole_latitude)),
            occurrences: vec![OccurrenceSource {
                origin: VertexOrigin::SourceNode(QNodeId(8)),
                source_trace: None,
            }],
        };
        let seam_vertex = |longitude, spelling| NativePortData {
            node: EmissionNodeId(4),
            point: ExactChartPoint::from_rationals(
                ExactRational::from_f64(longitude),
                ExactRational::from_f64(pole_latitude),
            ),
            spelling: PortSpelling::Seam(spelling),
            source_record: None,
            occurrences: vec![OccurrenceSource {
                origin: VertexOrigin::Presentation,
                source_trace: None,
            }],
        };
        let event = NativeSeamVertices {
            latitude: ExactRational::from_f64(pole_latitude),
            east: seam_vertex(180.0, ChartSide::East),
            west: seam_vertex(-180.0, ChartSide::West),
        };
        let mut budget = EvaluationBudget::new("pole presentation incidence", 0).unwrap();
        let edges = native_pole_edges(
            frame,
            &[event],
            &[(0, ChartPole::North)],
            &[producer],
            &[],
            &[isolated_pole],
            &mut |_, _, _, _| Ok(false),
            &mut budget,
        )
        .unwrap();

        assert_eq!(edges.len(), 3);
        let first = &edges[0];
        let second = &edges[1];
        let third = &edges[2];
        assert_eq!(first.vertices[0].point.x, ExactRational::from_f64(180.0));
        assert_eq!(first.vertices[1].point.x, ExactRational::from_f64(120.0));
        assert_eq!(second.vertices[0].point.x, ExactRational::from_f64(120.0));
        assert_eq!(second.vertices[1].point.x, ExactRational::from_f64(100.0));
        assert_eq!(third.vertices[0].point.x, ExactRational::from_f64(100.0));
        assert_eq!(third.vertices[1].point.x, ExactRational::from_f64(-180.0));
        assert!(matches!(
            &first.vertices[0].spelling,
            PortSpelling::Seam(ChartSide::East)
        ));
        assert!(matches!(
            &third.vertices[1].spelling,
            PortSpelling::Seam(ChartSide::West)
        ));
        for presentation_port in [
            &first.vertices[1],
            &second.vertices[0],
            &second.vertices[1],
            &third.vertices[0],
        ] {
            assert!(matches!(
                &presentation_port.spelling,
                PortSpelling::Pole { .. }
            ));
            assert!(presentation_port.source_record.is_none());
            assert!(matches!(presentation_port.occurrences.as_slice(), [
                OccurrenceSource {
                    origin: VertexOrigin::Presentation,
                    source_trace: None,
                }
            ]));
        }
    }

    #[test]
    fn overlapping_pole_presentation_arcs_still_fail_known_graph_admission() {
        let frame = GeographicFrame::crs84();
        let pole_latitude = frame.chart_pole_latitude(ChartPole::North);
        let pole_port = |longitude| NativePortData {
            node: EmissionNodeId(4),
            point: ExactChartPoint::from_rationals(
                ExactRational::from_f64(longitude),
                ExactRational::from_f64(pole_latitude),
            ),
            spelling: PortSpelling::Pole {
                native_longitude: longitude,
            },
            source_record: None,
            occurrences: vec![OccurrenceSource {
                origin: VertexOrigin::Presentation,
                source_trace: None,
            }],
        };
        let existing = NativeAreaEdge {
            vertices: [pole_port(80.0), pole_port(140.0)],
            owner: NativeGraphEdgeOwner::Area { region: 0 },
            kind: NativeAreaEdgeKind::PolePresentation {
                pole: ChartPole::North,
            },
        };
        let seam_vertex = |longitude, spelling| NativePortData {
            node: EmissionNodeId(4),
            point: ExactChartPoint::from_rationals(
                ExactRational::from_f64(longitude),
                ExactRational::from_f64(pole_latitude),
            ),
            spelling: PortSpelling::Seam(spelling),
            source_record: None,
            occurrences: vec![OccurrenceSource {
                origin: VertexOrigin::Presentation,
                source_trace: None,
            }],
        };
        let event = NativeSeamVertices {
            latitude: ExactRational::from_f64(pole_latitude),
            east: seam_vertex(180.0, ChartSide::East),
            west: seam_vertex(-180.0, ChartSide::West),
        };
        let mut budget = EvaluationBudget::new("overlapping pole presentations", 0).unwrap();
        let generated = native_pole_edges(
            frame,
            &[event],
            &[(0, ChartPole::North)],
            std::slice::from_ref(&existing),
            &[],
            &[],
            &mut |_, _, _, _| Ok(false),
            &mut budget,
        )
        .unwrap();
        assert_eq!(generated.len(), 1);
        let presentation = generated
            .first()
            .ok_or_else(|| GeometryErrorKind::Invalid("pole arc was not generated".into()))
            .unwrap();
        let exact = vec![
            existing.vertices[0].point.clone(),
            existing.vertices[1].point.clone(),
            presentation.vertices[0].point.clone(),
            presentation.vertices[1].point.clone(),
        ];
        let mut work = Unmetered;
        let points = ExactPoints::from_exact(exact, &mut work).unwrap();

        assert!(
            admit_known_graph(points, vec![[0, 1], [2, 3]], vec![4; 4], &mut work).is_err(),
            "a partial overlap between known presentation arcs remains inadmissible"
        );
    }

    #[test]
    fn live_presentation_use_cannot_inherit_colocated_source_axes() {
        let frame = GeographicFrame::crs84();
        let source = crate::io::parse_wkt("LINESTRING Z (0 0 7, 0 10 9)").unwrap();
        let inputs =
            crate::geographic::GeographicInputs::try_from_shapes(frame, std::iter::once(&source))
                .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))
                .unwrap();
        let topology =
            ExactGeographicTopology::from_views(frame, inputs.views().iter().copied()).unwrap();
        let port_id = *topology
            .q
            .source_edge_ports
            .first()
            .and_then(|ports| ports.first())
            .ok_or_else(|| GeometryErrorKind::Invalid("source port is missing".into()))
            .unwrap();
        let source_node = topology
            .q
            .ports
            .get(port_id.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("source port is unknown".into()))
            .unwrap()
            .node;
        let mut work = Unmetered;
        let node_registry =
            EmissionNodeRegistry::with_sources(&topology, &[source_node], &mut work).unwrap();
        let emission_node = node_registry.source_node(source_node).unwrap();
        let source_record = Point::new_raw_axes(0.0, 0.0, ZOrdinate(Some(7.0)), MOrdinate(None));
        let plan = ExpandedEmissionPlan {
            topology: &topology,
            selected_face_roots: Vec::new(),
            node_registry,
            ports: vec![EmissionPort {
                node: emission_node,
                position: PortPosition::Source {
                    spelling: PortSpelling::Interior { periods: 0 },
                },
                source_record: Some(source_record),
                occurrence_range: 0..2,
            }],
            occurrences: vec![
                WrittenOccurrence {
                    origin: VertexOrigin::SourceNode(source_node),
                    source_trace: None,
                    request_row: RequestRowId::from_index(0).unwrap(),
                },
                WrittenOccurrence {
                    origin: VertexOrigin::Presentation,
                    source_trace: None,
                    request_row: RequestRowId::from_index(1).unwrap(),
                },
            ],
            chords: Vec::new(),
            parts: Vec::new(),
            lines: Vec::new(),
            points: Vec::new(),
            whole_sphere: false,
        };
        let publication = PublicationInventory {
            fragments: Vec::new(),
            samples: Vec::new(),
            counts: Vec::new(),
            ordinates: Vec::new(),
        };
        let mut source_ordinates = vec![NodeOrdinates::default(); topology.q.nodes.len()];
        let mut source_z = AxisValue::default();
        source_z.observe(Some(7.0));
        source_ordinates
            .get_mut(source_node.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("source ordinate row is missing".into()))
            .unwrap()
            .z = source_z;
        let metric_access: Option<&SourceArcMetricAccess<'_, '_, crate::crs::GeodesicMetric>> =
            None;
        let mut work = Unmetered;
        let (ordinates, axes) = resolve_emission_ordinates(
            &plan,
            &publication,
            &source_ordinates,
            &topology,
            CoordinateAxes::XYZ,
            false,
            "overlay",
            metric_access,
            &mut work,
        )
        .unwrap();
        assert_eq!(axes, CoordinateAxes::XY);
        assert_eq!(ordinates.len(), 1);
        assert_eq!(
            ordinates.first().map(|value| value.z.value()),
            Some(Some(7.0)),
            "the source use retains its value even though the shared output slot cannot keep Z"
        );
        let longitude = frame
            .longitude()
            .canonicalize(0.0)
            .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))
            .unwrap();
        let sites = [AssignedSite {
            longitude: crate::geographic::RetainedLongitudeSite::new(longitude, 0),
            latitude: 0.0,
        }];
        let written =
            native_port_point(&plan, &sites, &ordinates, EmissionPortId(0), axes).unwrap();
        assert_eq!(written.z(), None);
        assert_eq!(written.m(), None);
        assert_eq!(
            written.x.to_bits(),
            source_record.x.to_bits(),
            "the actual source XY record remains literal while presentation Z is dropped"
        );

        let mut work = Unmetered;
        assert!(
            resolve_emission_ordinates(
                &plan,
                &publication,
                &source_ordinates,
                &topology,
                CoordinateAxes::XYZ,
                true,
                "overlay",
                metric_access,
                &mut work,
            )
            .is_err()
        );
    }

    #[test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "assertions are the exact ordinate and strict-axis regression oracle"
    )]
    fn native_source_root_occurrences_preserve_constant_zm_and_resolve_varying_seam_values()
    -> Result<()> {
        const CRS: &str = "+proj=longlat +a=7000000 +rf=150 +type=crs";
        let state = crate::crs::runtime_state_snapshot()?;
        crate::crs::with_ellipsoid_metric_at(CRS, &[], &state, |metric| {
            let frame = metric.chart_frame();
            let metric_access = || Ok(metric);
            let metric_access: &SourceArcMetricAccess<'_, '_, crate::crs::EllipsoidMetric<'_>> =
                &metric_access;
            let mut request_work = Unmetered;

            let constant = [
                crate::io::parse_wkt("LINESTRING ZM (170 -30 5 50, -170 70 5 50)")?,
                crate::io::parse_wkt("LINESTRING ZM (170 70 5 50, -170 -30 5 50)")?,
            ];
            let inputs =
                crate::geographic::GeographicInputs::try_from_shapes(frame, constant.iter())
                    .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
            let topology =
                ExactGeographicTopology::from_views(frame, inputs.views().iter().copied())?;
            let (cut_port, requests) = source_requests_at_shared_port(&topology, 2, false)?;
            let (ordinates, axes) = resolve_native_root_occurrences(
                &topology,
                cut_port,
                &requests,
                Some(metric_access),
                true,
            )?;
            assert_eq!(axes, CoordinateAxes::XYZM);
            let ordinates = ordinates.first().ok_or_else(|| {
                GeometryErrorKind::Invalid("constant Z/M source port is missing".into())
            })?;
            assert_eq!(ordinates.z.value(), Some(5.0));
            assert_eq!(ordinates.m.value(), Some(50.0));

            let varying = crate::io::parse_wkt("LINESTRING ZM (170 -30 1 5, -170 70 9 45)")?;
            let inputs = crate::geographic::GeographicInputs::try_from_shapes(
                frame,
                std::iter::once(&varying),
            )
            .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
            let topology =
                ExactGeographicTopology::from_views(frame, inputs.views().iter().copied())?;
            let (cut_port, requests) = source_requests_at_shared_port(&topology, 1, true)?;
            let (root, parameter) = requests
                .iter()
                .find(|(root, parameter)| {
                    topology
                        .sources
                        .edges
                        .get(root.index())
                        .is_some_and(|edge| {
                            source_edge_varies_in_latitude_and_ordinates(&edge.source)
                                && !parameter.is_zero()
                                && !parameter.is_one()
                        })
                })
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "varying shared port has no interior source-root request".into(),
                    )
                })?;
            let edge = topology.sources.edges.get(root.index()).ok_or_else(|| {
                GeometryErrorKind::Invalid("varying source root is unknown".into())
            })?;
            let chart_parameter = parameter.round_with_work(&mut request_work)?;
            let chart_edge = crate::crs::ChartEdge::from_retained(
                edge.source[0],
                edge.source[1],
                frame,
                edge.retained_longitude(),
            )?;
            let fraction =
                metric.chart_edge_arclength_fraction(&chart_edge, chart_parameter, None)?;
            assert_ne!(
                fraction, chart_parameter,
                "the source sample distinguishes ellipsoidal arclength from chart t"
            );
            let expected = source_ordinate_values_at_fraction(
                edge.source[0],
                edge.source[1],
                CoordinateAxes::XYZM,
                fraction,
            );
            let (ordinates, axes) = resolve_native_root_occurrences(
                &topology,
                cut_port,
                &requests,
                Some(metric_access),
                true,
            )?;
            assert_eq!(axes, CoordinateAxes::XYZM);
            let ordinates = ordinates.first().ok_or_else(|| {
                GeometryErrorKind::Invalid("varying source request lost its output port".into())
            })?;
            assert_eq!(
                ordinates.z.value().map(f64::to_bits),
                expected.0.0.map(f64::to_bits)
            );
            assert_eq!(
                ordinates.m.value().map(f64::to_bits),
                expected.1.0.map(f64::to_bits)
            );

            let conflicting = [
                crate::io::parse_wkt("LINESTRING ZM (170 -30 5 50, -170 70 5 50)")?,
                crate::io::parse_wkt("LINESTRING ZM (170 70 7 70, -170 -30 7 70)")?,
            ];
            let inputs =
                crate::geographic::GeographicInputs::try_from_shapes(frame, conflicting.iter())
                    .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
            let topology =
                ExactGeographicTopology::from_views(frame, inputs.views().iter().copied())?;
            let (cut_port, requests) = source_requests_at_shared_port(&topology, 2, false)?;
            let Err(error) = resolve_native_root_occurrences(
                &topology,
                cut_port,
                &requests,
                Some(metric_access),
                true,
            ) else {
                return Err(GeometryErrorKind::Invalid(
                    "conflicting source ordinates were merged into one value".into(),
                )
                .into());
            };
            assert!(
                error
                    .to_string()
                    .contains("difference cannot represent Z/M")
            );
            Ok(())
        })
    }

    #[test]
    fn one_materialized_feature_family_counts_once_across_retries() {
        let mut registry = FeatureFamilyBounds {
            bounds: vec![FeatureFamilyBound {
                publication: 1,
                native_cuts: 2,
                native_cut_ranges: Vec::new(),
            }],
            region_families: Vec::new(),
        }
        .into_registry(2);
        let family = FeatureFamily {
            parent: FeatureFamilyParent::Fragment(FragmentRecipeId(0)),
            producer: SampleProducerKind::NativeCut,
            occurrence: 0,
        };

        let mut work = Unmetered;
        for retry in 0..100 {
            assert_eq!(registry.register(family, &mut work).unwrap(), retry == 0);
        }
        assert_eq!(registry.additional_constructed(), 1);

        let mut budget = EvaluationBudget::new("native completion", 2).unwrap();
        budget.charge(7).unwrap();
        registry.synchronize_budget(&mut budget).unwrap();
        for _ in 0..100 {
            registry.register(family, &mut budget).unwrap();
            registry.synchronize_budget(&mut budget).unwrap();
        }
        assert_eq!(budget.constructed(), 3);
    }

    #[test]
    fn native_cut_occurrences_follow_disjoint_prepared_period_ranges() {
        let registry = FeatureFamilyRegistry {
            bounds: vec![FeatureFamilyBound {
                publication: 0,
                native_cuts: 4,
                native_cut_ranges: vec![NativeCutOccurrenceRange {
                    intervals: vec![
                        NativeCutOccurrenceInterval {
                            low_period: 0,
                            high_period: 1,
                            family_base: 0,
                        },
                        NativeCutOccurrenceInterval {
                            low_period: 4,
                            high_period: 5,
                            family_base: 2,
                        },
                    ],
                }],
            }],
            region_families: Vec::new(),
            families: FallibleAvl::with_capacity(0),
            source_base: 0,
        };
        let mut work = Unmetered;
        for (period, occurrence) in [(0, 0), (1, 1), (4, 2), (5, 3)] {
            assert_eq!(
                registry
                    .native_cut_occurrence(FragmentRecipeId(0), 0, period, &mut work,)
                    .unwrap(),
                occurrence,
            );
        }
        assert!(
            registry
                .native_cut_occurrence(FragmentRecipeId(0), 0, 2, &mut work)
                .is_err(),
            "a period not admitted by any prepared endpoint pair has no family identity",
        );
    }

    #[test]
    fn region_feature_registration_is_limited_to_the_frozen_template_universe() {
        let admitted = FeatureFamily {
            parent: FeatureFamilyParent::Region(7),
            producer: SampleProducerKind::WholeSphereCover,
            occurrence: 0,
        };
        let mut registry = FeatureFamilyRegistry {
            bounds: Vec::new(),
            region_families: vec![admitted],
            families: FallibleAvl::with_capacity(0),
            source_base: 0,
        };
        let mut work = Unmetered;
        assert!(registry.register(admitted, &mut work).unwrap());
        assert!(!registry.register(admitted, &mut work).unwrap());
        registry
            .register(
                FeatureFamily {
                    parent: FeatureFamilyParent::Region(8),
                    producer: SampleProducerKind::WholeSphereCover,
                    occurrence: 0,
                },
                &mut work,
            )
            .unwrap_err();
        registry
            .register(
                FeatureFamily {
                    occurrence: 10,
                    ..admitted
                },
                &mut work,
            )
            .unwrap_err();
    }

    #[test]
    fn native_literal_port_map_follows_final_shape_order() {
        let uses = NativeLiteralUses {
            points: vec![
                LiteralPoint {
                    output: 0,
                    axes: CoordinateAxes::XY,
                    image: EmissionPortId(10),
                },
                LiteralPoint {
                    output: 1,
                    axes: CoordinateAxes::XY,
                    image: EmissionPortId(11),
                },
            ],
            lines: vec![
                LiteralSequence {
                    output: 0,
                    axes: CoordinateAxes::XY,
                    images: vec![EmissionPortId(20), EmissionPortId(21)],
                },
                LiteralSequence {
                    output: 1,
                    axes: CoordinateAxes::XY,
                    images: vec![EmissionPortId(30), EmissionPortId(31)],
                },
            ],
            polygons: Vec::new(),
        };
        let order = CertifiedOverlayOrder {
            points: vec![1, 0],
            lines: vec![1, 0],
            polygons: Vec::new(),
        };
        let mut work = Unmetered;
        let trace = SourceTrace::from_edge_rows([Vec::new()], &mut work).unwrap();
        let line_traces = vec![
            TracedLine {
                member: 20,
                trace: trace.clone(),
            },
            TracedLine { member: 30, trace },
        ];
        let (ordered, ordered_traces) =
            native_literal_uses_in_output_order(uses, line_traces, order, &mut work).unwrap();
        assert_eq!(
            ordered
                .points
                .iter()
                .map(|point| point.image)
                .collect::<Vec<_>>(),
            [EmissionPortId(11), EmissionPortId(10)],
        );
        assert_eq!(
            ordered
                .points
                .iter()
                .map(|point| point.output)
                .collect::<Vec<_>>(),
            [1, 0],
        );
        assert_eq!(ordered.lines, [
            LiteralSequence {
                output: 1,
                axes: CoordinateAxes::XY,
                images: vec![EmissionPortId(30), EmissionPortId(31)],
            },
            LiteralSequence {
                output: 0,
                axes: CoordinateAxes::XY,
                images: vec![EmissionPortId(20), EmissionPortId(21)],
            },
        ],);
        assert_eq!(
            ordered_traces
                .iter()
                .map(|line| line.member)
                .collect::<Vec<_>>(),
            [30, 20],
        );
        assert!(ordered.polygons.is_empty());
    }

    #[test]
    fn completed_native_drawing_pairs_literal_points_with_port_uses() {
        let shape = Shape::multi_point(
            CoordinateAxes::XY,
            CoordSeq::try_from_points(&[Point::new_raw_xy(3.0, 1.0), Point::new_raw_xy(8.0, 2.0)])
                .unwrap(),
        );
        let layout = NativeLiteralUses {
            points: vec![
                LiteralPoint {
                    output: 0,
                    axes: CoordinateAxes::XY,
                    image: EmissionPortId(4),
                },
                LiteralPoint {
                    output: 1,
                    axes: CoordinateAxes::XY,
                    image: EmissionPortId(9),
                },
            ],
            lines: Vec::new(),
            polygons: Vec::new(),
        };
        let mut visited = Vec::new();
        let mut work = Unmetered;
        visit_shape_literals(
            &shape,
            &layout,
            &mut work,
            &mut |_, port, point, _, _, _| {
                visited.push((port, point.x, point.y));
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(visited, [
            (EmissionPortId(4), 3.0, 1.0),
            (EmissionPortId(9), 8.0, 2.0),
        ],);
    }

    #[test]
    fn completed_native_drawing_keeps_the_full_ring_closure_binding() {
        let ring = Ring::try_from_closed(
            CoordSeq::try_from_points(&[
                Point::new_raw_xy(0.0, 0.0),
                Point::new_raw_xy(1.0, 0.0),
                Point::new_raw_xy(0.0, 1.0),
                Point::new_raw_xy(0.0, 0.0),
            ])
            .unwrap(),
        )
        .unwrap();
        let shape = Shape::Polygon(Polygon::new(ring, Vec::new()));
        let layout = NativeLiteralUses {
            points: Vec::new(),
            lines: Vec::new(),
            polygons: vec![NativePolygonLiteralUses {
                output: 0,
                axes: CoordinateAxes::XY,
                shell: LiteralSequence {
                    output: 0,
                    axes: CoordinateAxes::XY,
                    images: vec![
                        EmissionPortId(4),
                        EmissionPortId(5),
                        EmissionPortId(6),
                        EmissionPortId(4),
                    ],
                },
                holes: Vec::new(),
            }],
        };
        let mut visited = Vec::new();
        let mut work = Unmetered;
        visit_shape_literals(
            &shape,
            &layout,
            &mut work,
            &mut |_, port, point, _, _, _| {
                visited.push((port, point.x, point.y));
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(visited, [
            (EmissionPortId(4), 0.0, 0.0),
            (EmissionPortId(5), 1.0, 0.0),
            (EmissionPortId(6), 0.0, 1.0),
            (EmissionPortId(4), 0.0, 0.0),
        ],);
    }

    #[test]
    fn written_sample_splits_the_original_root_parameter_trace() {
        let one = ExactRational::one();
        let zero = ExactRational::zero();
        let half = ExactRational::from_f64(0.5);
        let quarter = ExactRational::from_f64(0.25);
        let three_quarters = ExactRational::from_f64(0.75);
        let recipe = FragmentRecipe {
            id: FragmentRecipeId(0),
            endpoints: [QPortId(4), QPortId(9)],
            ranges: vec![DirectedSourceRange {
                source_edge: SourceEdgeId(3),
                range: ExactSourceRange {
                    start: quarter,
                    end: three_quarters,
                },
                emitted: true,
                producer_start: zero,
                producer_end: one,
                endpoint_only: false,
            }],
            exact_displacement: ExactRational::from_f64(10.0),
            use_kind: SelectedUse::Curve,
        };
        let recipes = FragmentRecipes {
            values: vec![recipe],
            walks: Vec::new(),
        };
        let mut work = Unmetered;
        let original = recipes
            .source_trace(
                &RecipeWalk {
                    owner: RecipeWalkOwner::Line(0),
                    fragments: vec![FragmentRecipeId(0)],
                },
                &mut work,
            )
            .unwrap();
        let split = original
            .split_at_samples(
                &[SourceTraceSample {
                    edge_index: 0,
                    local_parameter: half.clone(),
                    written_vertex: 1,
                }],
                &mut work,
            )
            .unwrap();

        assert_eq!(split.rows.edge_count(), 2);
        assert_eq!(split.rows.owner_count(), 2);
        assert_eq!(split.samples.len(), 1);
        assert_eq!(split.samples[0].written_vertex, 1);
        assert!(split.samples[0].has_source);
        assert!(!split.samples[0].endpoint_only);
        assert_eq!(split.samples[0].request_row.index(), 0);
        let sample_request = split
            .rows
            .sample_requests_at(0, &mut work)
            .unwrap()
            .into_iter()
            .next()
            .ok_or_else(|| GeometryErrorKind::Invalid("sample lost its root request".into()))
            .unwrap();
        assert_eq!(sample_request.parameter(), &half);
        let left_owner = split
            .owners_for_edge(0)
            .unwrap()
            .first()
            .ok_or_else(|| GeometryErrorKind::Invalid("left child lost its root range".into()))
            .unwrap();
        let right_owner = split
            .owners_for_edge(1)
            .unwrap()
            .first()
            .ok_or_else(|| GeometryErrorKind::Invalid("right child lost its root range".into()))
            .unwrap();
        assert!(left_owner.source_end().try_cmp(&half).unwrap().is_eq());
        assert!(right_owner.source_start().try_cmp(&half).unwrap().is_eq());
        assert_eq!(left_owner.producer_start(), &ExactRational::zero());
        assert_eq!(left_owner.producer_end(), &ExactRational::one());
        assert_eq!(right_owner.producer_start(), &ExactRational::zero());
        assert_eq!(right_owner.producer_end(), &ExactRational::one());
        assert_eq!(
            split
                .rows
                .requests_at(0, &half, &mut work)
                .unwrap()
                .first()
                .ok_or_else(|| GeometryErrorKind::Invalid(
                    "left child lost its source request".into()
                ))
                .unwrap()
                .parameter(),
            &ExactRational::from_f64(0.375)
        );
        assert_eq!(
            split
                .rows
                .requests_at(1, &half, &mut work)
                .unwrap()
                .first()
                .ok_or_else(|| GeometryErrorKind::Invalid(
                    "right child lost its source request".into()
                ))
                .unwrap()
                .parameter(),
            &ExactRational::from_f64(0.625)
        );
    }

    #[test]
    fn recursive_source_trace_split_retains_prior_sample_rows() {
        let zero = ExactRational::zero();
        let one = ExactRational::one();
        let recipe = FragmentRecipe {
            id: FragmentRecipeId(0),
            endpoints: [QPortId(7), QPortId(8)],
            ranges: vec![DirectedSourceRange {
                source_edge: SourceEdgeId(2),
                range: ExactSourceRange {
                    start: zero.clone(),
                    end: one.clone(),
                },
                emitted: true,
                producer_start: zero,
                producer_end: one,
                endpoint_only: false,
            }],
            exact_displacement: ExactRational::from_f64(16.0),
            use_kind: SelectedUse::Curve,
        };
        let recipes = FragmentRecipes {
            values: vec![recipe],
            walks: Vec::new(),
        };
        let mut work = Unmetered;
        let original = recipes
            .source_trace_fragment(FragmentRecipeId(0), &mut work)
            .unwrap();
        let first = original
            .split_at_samples(
                &[SourceTraceSample {
                    edge_index: 0,
                    local_parameter: ExactRational::from_f64(0.5),
                    written_vertex: 1,
                }],
                &mut work,
            )
            .unwrap();
        let second = first
            .split_at_samples(
                &[SourceTraceSample {
                    edge_index: 0,
                    local_parameter: ExactRational::from_f64(0.5),
                    written_vertex: 1,
                }],
                &mut work,
            )
            .unwrap();

        assert_eq!(second.rows.edge_count(), 3);
        assert_eq!(second.samples.len(), 2);
        assert_eq!(second.samples[0].written_vertex, 1);
        assert_eq!(second.samples[1].written_vertex, 2);
        for (sample, expected) in [
            (&second.samples[0], ExactRational::from_f64(0.25)),
            (&second.samples[1], ExactRational::from_f64(0.5)),
        ] {
            let requests = second
                .rows
                .sample_requests_at(sample.request_row.index(), &mut work)
                .unwrap();
            assert_eq!(requests.len(), 1);
            assert_eq!(requests[0].parameter(), &expected);
        }
    }

    #[test]
    fn recursive_source_trace_split_retains_a_prior_child_endpoint() {
        let root = RootId::from_index(12);
        let old_parameter = ExactRational::from_f64(0.75);
        let mut rows = TraceRowsBuilder::new();
        rows.push_row([RootRange::whole(root)]).unwrap();
        rows.push_sample_row([RootRange::point(
            root,
            old_parameter.clone(),
            ExactRational::zero(),
        )
        .unwrap()])
            .unwrap();
        let mut work = Unmetered;
        let trace = SourceTrace {
            rows: rows.finish(&mut work).unwrap(),
            samples: vec![TracedSourceSample {
                written_vertex: 1,
                edge: 0,
                parameter: old_parameter.clone(),
                request_row: RequestRowId::from_index(0).unwrap(),
                has_source: true,
                endpoint_only: true,
            }],
        };
        let split = trace
            .split_at_samples(
                &[SourceTraceSample {
                    edge_index: 0,
                    local_parameter: ExactRational::from_f64(0.5),
                    written_vertex: 1,
                }],
                &mut work,
            )
            .unwrap();

        assert_eq!(split.samples.len(), 2);
        assert_eq!(split.samples[0].written_vertex, 1);
        assert_eq!(split.samples[1].written_vertex, 2);
        let requests = split
            .rows
            .sample_requests_at(split.samples[1].request_row.index(), &mut work)
            .unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].root(), root);
        assert_eq!(requests[0].parameter(), &old_parameter);
    }

    #[test]
    fn publisher_samples_resolve_through_the_original_source_edge() {
        let frame = GeographicFrame::crs84();
        let shape = crate::io::parse_wkt("LINESTRING ZM (0 0 1 100, 0 10 3 300)").unwrap();
        let inputs =
            crate::geographic::GeographicInputs::try_from_shapes(frame, std::iter::once(&shape))
                .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))
                .unwrap();
        let topology =
            ExactGeographicTopology::from_views(frame, inputs.views().iter().copied()).unwrap();
        let edge = topology
            .sources
            .edges
            .first()
            .ok_or_else(|| GeometryErrorKind::Invalid("source line edge is missing".into()))
            .unwrap();
        let endpoints = *topology
            .q
            .source_edge_ports
            .get(edge.id.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("source line ports are missing".into()))
            .unwrap();
        let zero = ExactRational::zero();
        let one = ExactRational::one();
        let recipe = FragmentRecipe {
            id: FragmentRecipeId(0),
            endpoints,
            ranges: vec![DirectedSourceRange {
                source_edge: edge.id,
                range: ExactSourceRange {
                    start: zero.clone(),
                    end: one.clone(),
                },
                emitted: true,
                producer_start: zero,
                producer_end: one,
                endpoint_only: false,
            }],
            exact_displacement: ExactRational::from_f64(frame.longitude().period()),
            use_kind: SelectedUse::Curve,
        };
        let recipes = FragmentRecipes {
            values: vec![recipe],
            walks: Vec::new(),
        };
        let coordinates = super::super::q_embedding::QCoordinates::build(&topology).unwrap();
        let mut work = Unmetered;
        let publication = recipes
            .publish(&topology, &coordinates, "source trace test", &mut work)
            .unwrap();
        let fragment = publication
            .fragments
            .first()
            .ok_or_else(|| GeometryErrorKind::Invalid("published fragment is missing".into()))
            .unwrap();

        assert_eq!(publication.samples.len(), 3);
        assert_eq!(fragment.points.len(), 5);
        assert_eq!(
            fragment.trace.rows.edge_count().checked_add(1),
            Some(fragment.points.len())
        );
        assert_eq!(
            fragment.points.first().and_then(|point| point.z()),
            Some(1.0)
        );
        assert_eq!(
            fragment.points.last().and_then(|point| point.m()),
            Some(300.0)
        );
        assert!(
            publication
                .samples
                .iter()
                .all(|sample| { sample.point.z().is_none() && sample.point.m().is_none() })
        );
        for (index, expected) in [
            ExactRational::from_f64(0.25),
            ExactRational::from_f64(0.5),
            ExactRational::from_f64(0.75),
        ]
        .into_iter()
        .enumerate()
        {
            let sample = publication
                .samples
                .get(index)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("accepted publisher sample is missing".into())
                })
                .unwrap();
            let source_trace = sample
                .source_trace
                .as_ref()
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("accepted publisher sample lost its source".into())
                })
                .unwrap();
            let TraceRequestLocation::Sample {
                recipe: trace_recipe,
                row,
            } = source_trace
            else {
                panic!("publisher sample does not reference its sample request row");
            };
            assert_eq!(
                *trace_recipe,
                sample
                    .family
                    .fragment_recipe()
                    .expect("publication has a recipe parent")
            );
            let request = fragment
                .trace
                .rows
                .sample_requests_at(row.index(), &mut work)
                .unwrap()
                .into_iter()
                .next()
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("accepted publisher source row is empty".into())
                })
                .unwrap();
            assert_eq!(sample.parameter, expected);
            assert_eq!(request.parameter(), &expected);
            assert_eq!(request.root().index(), edge.id.0 as usize);
        }
    }
}
