use std::cell::Cell;
use std::rc::Rc;

use super::*;
use crate::geometry::embedding_completion::admission::admit_known_graph;
use crate::geometry::embedding_completion::literals::{LiteralRowAddress, visit_shape_literals};
use crate::geometry::embedding_completion::points::ExactPoints;
use crate::geometry::ordinates::{RootSourceData, source_ordinate_values_at_fraction};
use crate::work::{Unmetered, Work};

const EMISSION_STORAGE_OPERATION: &str = "emission_storage_test";

#[derive(Clone, Copy)]
struct RecordedWorkCharge {
    spent_before: u128,
    ticks: u128,
    progress: usize,
    accepted: bool,
}

struct RecordedWork<'a> {
    budget: &'a mut EvaluationBudget,
    progress: Rc<Cell<usize>>,
    spent: u128,
    charges: Vec<RecordedWorkCharge>,
}

impl Work for RecordedWork<'_> {
    type Error = crate::error::Error;

    fn charge(&mut self, ticks: u64) -> Result<()> {
        let spent_before = self.spent;
        let result = <EvaluationBudget as Work>::charge(self.budget, ticks);
        self.charges.push(RecordedWorkCharge {
            spent_before,
            ticks: u128::from(ticks),
            progress: self.progress.get(),
            accepted: result.is_ok(),
        });
        if result.is_ok() {
            self.spent = self
                .spent
                .checked_add(u128::from(ticks))
                .expect("recorded work total fits u128");
        }
        result
    }

    fn charge_product(&mut self, count: usize, ticks_per_item: usize) -> Result<()> {
        let ticks = u128::try_from(count)
            .expect("usize fits u128")
            .checked_mul(u128::try_from(ticks_per_item).expect("usize fits u128"))
            .expect("recorded work product fits u128");
        let spent_before = self.spent;
        let result = <EvaluationBudget as Work>::charge_product(self.budget, count, ticks_per_item);
        self.charges.push(RecordedWorkCharge {
            spent_before,
            ticks,
            progress: self.progress.get(),
            accepted: result.is_ok(),
        });
        if result.is_ok() {
            self.spent = self
                .spent
                .checked_add(ticks)
                .expect("recorded work total fits u128");
        }
        result
    }
}

fn emission_budget_refusing_before(charge: RecordedWorkCharge) -> EvaluationBudget {
    assert!(charge.ticks > 0);
    EvaluationBudget::with_remaining_ticks_for_test(
        EMISSION_STORAGE_OPERATION,
        0,
        charge
            .spent_before
            .checked_add(
                charge
                    .ticks
                    .checked_sub(1)
                    .expect("recorded work refusal has positive cost"),
            )
            .expect("the calibrated allowance is representable"),
    )
    .expect("the calibrated allowance fits the actual EvaluationBudget")
}

fn assert_emission_work_limit(error: &crate::error::Error) {
    assert!(matches!(
        error.kind(),
        crate::error::ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
            operation: EMISSION_STORAGE_OPERATION,
            ..
        })
    ));
}

struct ProgressRootCatalog(Rc<Cell<usize>>);

impl RootCatalog for ProgressRootCatalog {
    fn frame(&self) -> crate::geographic::GeographicFrame {
        crate::geographic::GeographicFrame::crs84()
    }

    fn source_root(&self, _root: RootId) -> Option<RootSourceData<'_>> {
        self.0.set(
            self.0
                .get()
                .checked_add(1)
                .expect("test source lookup count fits usize"),
        );
        Some(RootSourceData::Point(crate::geometry::Point::new_raw_axes(
            0.0,
            0.0,
            crate::geometry::ZOrdinate(Some(10.0)),
            crate::geometry::MOrdinate(Some(20.0)),
        )))
    }
}

struct RecordingWork {
    budget: EvaluationBudget,
    attempted: Vec<u64>,
    accepted: Vec<u64>,
}

impl RecordingWork {
    fn with_remaining(operation: &'static str, remaining_ticks: u128) -> Result<Self> {
        Ok(Self {
            budget: EvaluationBudget::with_remaining_ticks_for_test(operation, 1, remaining_ticks)?,
            attempted: Vec::new(),
            accepted: Vec::new(),
        })
    }

    fn accepted_ticks(&self) -> u128 {
        self.accepted.iter().map(|&ticks| u128::from(ticks)).sum()
    }
}

impl Work for RecordingWork {
    type Error = crate::error::Error;

    fn charge(&mut self, ticks: u64) -> Result<()> {
        self.attempted.push(ticks);
        self.budget.charge(ticks)?;
        self.accepted.push(ticks);
        Ok(())
    }

    fn charge_product(&mut self, count: usize, ticks_per_item: usize) -> Result<()> {
        self.charge(crate::work::work_product_ticks(count, ticks_per_item)?)
    }
}

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

fn materialize_test_line(
    axes: CoordinateAxes,
    work: &mut impl Work,
) -> Result<NativeMaterializedParts> {
    materialize_test_drawing(axes, false, false, work)
}

fn materialize_test_ring(
    axes: CoordinateAxes,
    work: &mut impl Work,
) -> Result<NativeMaterializedParts> {
    materialize_test_drawing(axes, true, false, work)
}

fn materialize_test_ring_with_hole(
    axes: CoordinateAxes,
    work: &mut impl Work,
) -> Result<NativeMaterializedParts> {
    materialize_test_drawing(axes, true, true, work)
}

fn materialize_test_drawing(
    axes: CoordinateAxes,
    closed: bool,
    with_hole: bool,
    work: &mut impl Work,
) -> Result<NativeMaterializedParts> {
    let source = crate::io::parse_wkt("LINESTRING (0 0, 1 0)")?;
    let topology = endpoint_test_topology(std::slice::from_ref(&source))?;
    let [start_port, end_port] = topology
        .q
        .source_edge_ports
        .first()
        .copied()
        .ok_or_else(|| GeometryErrorKind::Invalid("test source edge is missing".into()))?;
    let start_node = topology
        .q
        .ports
        .get(start_port.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("test start port is missing".into()))?
        .node;
    let end_node = topology
        .q
        .ports
        .get(end_port.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("test end port is missing".into()))?
        .node;
    let mut setup_work = Unmetered;
    let node_registry =
        EmissionNodeRegistry::with_sources(&topology, &[start_node, end_node], &mut setup_work)?;
    let start_emission_node = node_registry.source_node(start_node)?;
    let end_emission_node = node_registry.source_node(end_node)?;
    let frame = topology.sources.frame;
    let make_site = |longitude: f64, latitude: f64| -> Result<AssignedSite> {
        let longitude = frame
            .longitude()
            .canonicalize(longitude)
            .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
        Ok(AssignedSite {
            longitude: crate::geographic::RetainedLongitudeSite::new(longitude, 0),
            latitude,
        })
    };
    let mut sites = vec![None; node_registry.nodes().len()];
    *sites
        .get_mut(start_emission_node.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("test start site is missing".into()))? =
        Some(make_site(0.0, 0.0)?);
    *sites
        .get_mut(end_emission_node.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("test end site is missing".into()))? =
        Some(make_site(1.0, 0.0)?);
    let sites = sites
        .into_iter()
        .map(|site| {
            site.ok_or_else(|| GeometryErrorKind::Invalid("test site is missing".into()).into())
        })
        .collect::<Result<Vec<_>>>()?;
    let ports = vec![
        EmissionPort {
            node: start_emission_node,
            position: PortPosition::Source {
                spelling: PortSpelling::Interior { periods: 0 },
            },
            source_record: Some(Point::new_raw_xy(0.0, 0.0)),
            occurrence_range: 0..0,
        },
        EmissionPort {
            node: end_emission_node,
            position: PortPosition::Source {
                spelling: PortSpelling::Interior { periods: 0 },
            },
            source_record: Some(Point::new_raw_xy(1.0, 0.0)),
            occurrence_range: 0..0,
        },
    ];
    let mut ordinates = vec![NodeOrdinates::default(); ports.len()];
    for (index, (z, m)) in [(1.25, 2.5), (3.75, 4.5)].into_iter().enumerate() {
        let row = ordinates
            .get_mut(index)
            .ok_or_else(|| GeometryErrorKind::Invalid("test ordinate row is missing".into()))?;
        row.z.observe(Some(z));
        row.m.observe(Some(m));
    }
    let chords = if closed {
        vec![
            EmissionChord {
                ports: [EmissionPortId(0), EmissionPortId(1)],
                recipe: None,
            },
            EmissionChord {
                ports: [EmissionPortId(1), EmissionPortId(0)],
                recipe: None,
            },
        ]
    } else {
        vec![EmissionChord {
            ports: [EmissionPortId(0), EmissionPortId(1)],
            recipe: None,
        }]
    };
    let parts = if closed {
        vec![EmissionPart {
            region: 0,
            face: NativeFaceId(0),
            shell: vec![
                EmissionChordId(0),
                EmissionChordId(1),
                EmissionChordId(0),
                EmissionChordId(1),
            ],
            holes: if with_hole {
                vec![vec![
                    EmissionChordId(0),
                    EmissionChordId(1),
                    EmissionChordId(0),
                    EmissionChordId(1),
                ]]
            } else {
                Vec::new()
            },
        }]
    } else {
        Vec::new()
    };
    let plan = ExpandedEmissionPlan {
        topology: &topology,
        selected_face_roots: Vec::new(),
        node_registry,
        ports,
        occurrences: Vec::new(),
        chords,
        parts,
        lines: if closed {
            Vec::new()
        } else {
            vec![vec![EmissionChordId(0)]]
        },
        points: Vec::new(),
        whole_sphere: false,
    };
    materialize_native_parts(&plan, &sites, &ordinates, axes, work)
}

fn literal_test_sequence(count: usize) -> LiteralSequence<EmissionPortId> {
    LiteralSequence {
        output: 0,
        axes: CoordinateAxes::XY,
        images: (0..count)
            .map(|index| EmissionPortId(u32::try_from(index).expect("test image fits u32")))
            .collect(),
    }
}

fn empty_literal_test_parts() -> NativeMaterializedParts {
    NativeMaterializedParts {
        axes: CoordinateAxes::XY,
        points: Vec::new(),
        lines: Vec::new(),
        polygons: Vec::new(),
        literal_uses: NativeLiteralUses {
            points: Vec::new(),
            lines: Vec::new(),
            polygons: Vec::new(),
        },
        diagnostic_lines: Vec::new(),
    }
}

fn point_literal_test_parts() -> NativeMaterializedParts {
    let point = Point::new_raw_xy(3.0, 4.0);
    NativeMaterializedParts {
        axes: CoordinateAxes::XY,
        points: vec![point],
        literal_uses: NativeLiteralUses {
            points: vec![LiteralPoint {
                output: 0,
                axes: CoordinateAxes::XY,
                image: EmissionPortId(0),
            }],
            ..empty_literal_test_parts().literal_uses
        },
        ..empty_literal_test_parts()
    }
}

fn line_literal_test_parts() -> Result<NativeMaterializedParts> {
    let mut parts = empty_literal_test_parts();
    parts.lines.push(CoordSeq::try_from_points(&[
        Point::new_raw_xy(0.0, 0.0),
        Point::new_raw_xy(1.0, 0.0),
    ])?);
    parts.literal_uses.lines.push(literal_test_sequence(2));
    Ok(parts)
}

fn polygon_literal_test_parts(with_hole: bool) -> Result<NativeMaterializedParts> {
    let shell = Ring::try_from_closed(CoordSeq::try_from_points(&[
        Point::new_raw_xy(0.0, 0.0),
        Point::new_raw_xy(2.0, 0.0),
        Point::new_raw_xy(0.0, 2.0),
        Point::new_raw_xy(0.0, 0.0),
    ])?)?;
    let holes = if with_hole {
        vec![Ring::try_from_closed(CoordSeq::try_from_points(&[
            Point::new_raw_xy(0.25, 0.25),
            Point::new_raw_xy(0.5, 0.25),
            Point::new_raw_xy(0.25, 0.5),
            Point::new_raw_xy(0.25, 0.25),
        ])?)?]
    } else {
        Vec::new()
    };
    let uses = LiteralPolygon {
        output: 0,
        axes: CoordinateAxes::XY,
        shell: literal_test_sequence(4),
        holes: with_hole
            .then(|| literal_test_sequence(4))
            .into_iter()
            .collect(),
    };
    let mut parts = empty_literal_test_parts();
    parts.polygons.push(Polygon::new(shell, holes));
    parts.literal_uses.polygons.push(uses);
    Ok(parts)
}

fn resolve_native_root_occurrences<M: ChartMetric>(
    topology: &ExactGeographicTopology,
    port_id: QPortId,
    root_requests: &[(RootId, ExactRational)],
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
    strict: bool,
) -> Result<(Vec<NodeOrdinates>, CoordinateAxes)> {
    let mut work = Unmetered;
    let q_port =
        topology.q.ports.get(port_id.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("source-root test Q port is unknown".into())
        })?;
    let node_registry = EmissionNodeRegistry::with_sources(topology, &[q_port.node], &mut work)?;
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
    source[0].y != source[1].y && (source[0].z() != source[1].z() || source[0].m() != source[1].m())
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
