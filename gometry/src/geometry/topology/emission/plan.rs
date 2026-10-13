use super::super::exact::SourceRowId;
use super::cut_ports::CutNodePorts;
use super::sites::{EmittedNodeKey, EmittedVertex, record_emitted_node};
use super::{
    ChartPole, CoordinateAxes, EmissionNodeRegistry, FragmentRecipe, FragmentRecipes,
    GeometryErrorKind, NodeOrdinates, PublicationInventory, QDartId, QNodeId, QPortId, RecipeWalk,
    RecipeWalkOwner, Result,
};
use crate::geometry::{CoordSeq, HasM, HasZ};
use crate::work::Work;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::geometry::topology) enum DiagnosticKind {
    Cut,
    Dangle,
    InvalidRing,
}

#[derive(Clone, Copy)]
pub(in crate::geometry::topology) struct LineStep {
    pub(in crate::geometry::topology) span: usize,
    pub(in crate::geometry::topology) from_endpoint: SpanEndpoint,
    pub(in crate::geometry::topology) owner: usize,
}

#[derive(Clone, Copy)]
pub(in crate::geometry::topology) struct SelectedLineSpan {
    pub(in crate::geometry::topology) owner: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::geometry::topology) enum QLineOrigin {
    Curve,
    ArealBoundary,
    Noded,
    Diagnostic(DiagnosticKind),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::geometry::topology) struct QDiagnosticLineTag {
    pub(in crate::geometry::topology) kind: DiagnosticKind,
    pub(in crate::geometry::topology) source_row: SourceRowId,
    pub(in crate::geometry::topology) axes: CoordinateAxes,
}

#[derive(Clone)]
pub(in crate::geometry::topology) struct QDiagnosticLine {
    pub(in crate::geometry::topology) tag: QDiagnosticLineTag,
    pub(in crate::geometry::topology) coordinates: CoordSeq,
}

pub(in crate::geometry::topology) struct QLinePrimitive<T> {
    pub(in crate::geometry::topology) origin: QLineOrigin,
    pub(in crate::geometry::topology) recipe_walk: RecipeWalkOwner,
    pub(in crate::geometry::topology) source_row: Option<SourceRowId>,
    pub(in crate::geometry::topology) close: bool,
    pub(in crate::geometry::topology) path: T,
}

pub(in crate::geometry::topology) type PlannedQLine = QLinePrimitive<Vec<LineStep>>;
pub(in crate::geometry::topology) type DrawnQLine<'topology> =
    QLinePrimitive<Vec<EmittedVertex<'topology>>>;

#[derive(Clone, Copy)]
pub(in crate::geometry::topology) struct BoundarySupport {
    pub(in crate::geometry::topology) has_area: bool,
    pub(in crate::geometry::topology) has_curve: bool,
    pub(in crate::geometry::topology) has_point: bool,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(in crate::geometry::topology) enum SpanEndpoint {
    Start,
    End,
}

impl SpanEndpoint {
    pub(in crate::geometry::topology) const fn opposite(self) -> Self {
        match self {
            Self::Start => Self::End,
            Self::End => Self::Start,
        }
    }
}

pub(in crate::geometry::topology) struct BoundaryCycle {
    pub(in crate::geometry::topology) darts: Vec<QDartId>,
    pub(in crate::geometry::topology) region: usize,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(in crate::geometry::topology) struct SelectedRegionPoles {
    pub(in crate::geometry::topology) north: bool,
    pub(in crate::geometry::topology) south: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::geometry::topology) struct SelectedRegionFact {
    pub(in crate::geometry::topology) root: usize,
    pub(in crate::geometry::topology) poles: SelectedRegionPoles,
}

impl SelectedRegionPoles {
    pub(in crate::geometry::topology) const fn includes(self, pole: ChartPole) -> bool {
        match pole {
            ChartPole::North => self.north,
            ChartPole::South => self.south,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::geometry) enum QProducer {
    Overlay,
    Boundary,
    Node,
    Polygonize,
    BuildArea,
}

impl QProducer {
    pub(in crate::geometry::topology) const fn is_pooled_linework(self) -> bool {
        matches!(self, Self::Node | Self::Polygonize | Self::BuildArea)
    }

    pub(in crate::geometry::topology) const fn operation(self) -> &'static str {
        match self {
            Self::Node => "node",
            Self::Polygonize => "polygonize",
            Self::BuildArea => "build_area",
            Self::Overlay => "overlay",
            Self::Boundary => "boundary",
        }
    }
}

pub(in crate::geometry::topology) struct QEmissionPlan {
    pub(in crate::geometry::topology) whole_sphere: bool,
    pub(in crate::geometry::topology) force_native: bool,
    pub(in crate::geometry::topology) boundary_support: Option<BoundarySupport>,
    pub(in crate::geometry::topology) producer: QProducer,
    pub(in crate::geometry::topology) retain_literals_for_republication: bool,
    pub(in crate::geometry::topology) polygons: PolygonPlan,
    pub(in crate::geometry::topology) cycles: Vec<BoundaryCycle>,
    pub(in crate::geometry::topology) selected_regions: Vec<SelectedRegionFact>,
    pub(in crate::geometry::topology) selected_face_roots: Vec<Option<usize>>,
    pub(in crate::geometry::topology) fragment_recipes: FragmentRecipes,
    pub(in crate::geometry::topology) line_paths: Vec<PlannedQLine>,
    pub(in crate::geometry::topology) point_nodes: Vec<QNodeId>,
}

pub(in crate::geometry::topology) enum PolygonPlan {
    WholeSphere(Vec<usize>),
    Regions {
        rings: Vec<PlannedAreaRing>,
        region_count: usize,
    },
}

pub(in crate::geometry::topology) enum DrawnPolygons<'topology> {
    WholeSphere(Vec<Vec<DrawnSourceRing<'topology>>>),
    Regions {
        rings: Vec<DrawnAreaRing<'topology>>,
        region_count: usize,
    },
}

pub(in crate::geometry::topology) struct DrawnSourceRing<'topology> {
    pub(in crate::geometry::topology) hole: bool,
    pub(in crate::geometry::topology) recipe_walk: RecipeWalkOwner,
    pub(in crate::geometry::topology) vertices: Vec<EmittedVertex<'topology>>,
}

pub(in crate::geometry::topology) struct DrawnAreaRing<'topology> {
    pub(in crate::geometry::topology) region: usize,
    pub(in crate::geometry::topology) origin: DrawnAreaOrigin,
    pub(in crate::geometry::topology) recipe_walk: RecipeWalkOwner,
    pub(in crate::geometry::topology) native_start_port: Option<QPortId>,
    pub(in crate::geometry::topology) vertices: Vec<EmittedVertex<'topology>>,
}

#[derive(Clone, Copy)]
pub(in crate::geometry::topology) enum DrawnAreaOrigin {
    Source,
    Cycle,
}

pub(in crate::geometry::topology) struct QEmissionDrawing<'topology> {
    pub(in crate::geometry::topology) polygons: DrawnPolygons<'topology>,
    pub(in crate::geometry::topology) lines: Vec<DrawnQLine<'topology>>,
    pub(in crate::geometry::topology) diagnostic_line_tags: Vec<(usize, QDiagnosticLineTag)>,
    pub(in crate::geometry::topology) points: Vec<EmittedVertex<'topology>>,
}

impl QEmissionDrawing<'_> {
    pub(in crate::geometry::topology) fn live_node_keys(
        &self,
        frame: crate::geographic::GeographicFrame,
        work: &mut impl Work,
    ) -> Result<Vec<(EmittedNodeKey, QNodeId)>> {
        let mut live = Vec::new();
        self.for_each_vertex(
            |vertex| record_emitted_node(frame, vertex.port.node, vertex.point, &mut live),
            work,
        )?;
        Ok(live)
    }

    pub(in crate::geometry::topology) fn axes(
        &self,
        declared_axes: CoordinateAxes,
        ordinates: &[NodeOrdinates],
        work: &mut impl Work,
    ) -> Result<CoordinateAxes> {
        let mut has_z = declared_axes.has_z();
        let mut has_m = declared_axes.has_m();
        self.for_each_vertex(
            |vertex| {
                let node_ordinates =
                    ordinates.get(vertex.port.node.0 as usize).ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q emitted an unknown node".into())
                    })?;
                has_z &= node_ordinates.z.value().is_some();
                has_m &= node_ordinates.m.value().is_some();
                Ok(())
            },
            work,
        )?;
        Ok(CoordinateAxes::new(HasZ(has_z), HasM(has_m)))
    }

    fn for_each_vertex(
        &self,
        mut visit: impl FnMut(&EmittedVertex<'_>) -> Result<()>,
        work: &mut impl Work,
    ) -> Result<()> {
        match &self.polygons {
            DrawnPolygons::WholeSphere(polygons) => {
                for polygon in polygons {
                    for ring in polygon {
                        for vertex in &ring.vertices {
                            work.charge_into_error(1)?;
                            visit(vertex)?;
                        }
                    }
                }
            },
            DrawnPolygons::Regions { rings, .. } => {
                for ring in rings {
                    for vertex in &ring.vertices {
                        work.charge_into_error(1)?;
                        visit(vertex)?;
                    }
                }
            },
        }
        for line in &self.lines {
            for vertex in &line.path {
                work.charge_into_error(1)?;
                visit(vertex)?;
            }
        }
        for vertex in &self.points {
            work.charge_into_error(1)?;
            visit(vertex)?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
pub(in crate::geometry::topology) enum PlannedAreaRing {
    Source {
        ring: usize,
        region: usize,
        reverse: bool,
        cycle_start: Option<QPortId>,
    },
    Cycle {
        cycle: usize,
        region: usize,
    },
}

pub(in crate::geometry::topology) struct QEmbeddingEmission<'topology, 'emission> {
    pub(in crate::geometry::topology) plan: &'emission QEmissionPlan,
    pub(in crate::geometry::topology) force_native: bool,
    pub(in crate::geometry::topology) source_points:
        &'emission [Option<EmittedVertex<'topology>>],
    pub(in crate::geometry::topology) selected_regions: &'emission [SelectedRegionFact],
    pub(in crate::geometry::topology) selected_face_roots: &'emission [Option<usize>],
    pub(in crate::geometry::topology) fragment_recipes: &'emission [FragmentRecipe],
    pub(in crate::geometry::topology) fragment_walks: &'emission [RecipeWalk],
    pub(in crate::geometry::topology) publication: &'emission PublicationInventory,
    pub(in crate::geometry::topology) source_nodes: EmissionNodeRegistry,
    pub(in crate::geometry::topology) spans: Vec<usize>,
    pub(in crate::geometry::topology) areal_spans: Vec<usize>,
    pub(in crate::geometry::topology) point_nodes: &'emission [QNodeId],
    pub(in crate::geometry::topology) live_nodes: Vec<QNodeId>,
    pub(in crate::geometry::topology) selected_ports: Vec<(QNodeId, QPortId)>,
    pub(in crate::geometry::topology) selected_nodes: Vec<QNodeId>,
    pub(in crate::geometry::topology) cut_nodes: Vec<CutNodePorts>,
}

impl QEmissionPlan {
    pub(in crate::geometry::topology) const fn is_whole_sphere(&self) -> bool {
        self.whole_sphere
    }
}
