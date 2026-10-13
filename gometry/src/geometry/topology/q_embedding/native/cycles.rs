use super::super::*;

pub(in crate::geometry::topology::q_embedding) struct NativeGraphCycleEdge {
    pub(in crate::geometry::topology::q_embedding) atom: u32,
    pub(in crate::geometry::topology::q_embedding) reversed: bool,
    pub(in crate::geometry::topology::q_embedding) contributors: Vec<usize>,
}

pub(in crate::geometry::topology::q_embedding) struct NativeGraphCycle {
    pub(in crate::geometry::topology::q_embedding) face: u32,
    pub(in crate::geometry::topology::q_embedding) region: usize,
    pub(in crate::geometry::topology::q_embedding) edges: Vec<NativeGraphCycleEdge>,
    pub(in crate::geometry::topology::q_embedding) shell: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::geometry::topology::q_embedding) struct NativeCycleStartQuery {
    pub(in crate::geometry::topology::q_embedding) region: usize,
    pub(in crate::geometry::topology::q_embedding) shell: bool,
    pub(in crate::geometry::topology::q_embedding) node: EmissionNodeId,
    pub(in crate::geometry::topology::q_embedding) spelling: Option<NativeChartSpellingKey>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::geometry::topology::q_embedding) struct NativeCycleStartCandidate {
    pub(in crate::geometry::topology::q_embedding) cycle_index: usize,
    pub(in crate::geometry::topology::q_embedding) start: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::geometry::topology::q_embedding) enum NativeCycleStartLookup {
    None,
    Unique(NativeCycleStartCandidate),
    Ambiguous,
}

#[derive(Clone, Copy, Default)]
enum NativeCycleStartCandidates {
    #[default]
    None,
    One(NativeCycleStartCandidate),
    Multiple,
}

impl NativeCycleStartCandidates {
    const fn insert(&mut self, candidate: NativeCycleStartCandidate) {
        *self = match *self {
            Self::None => Self::One(candidate),
            Self::One(_) | Self::Multiple => Self::Multiple,
        };
    }

    const fn lookup(self) -> NativeCycleStartLookup {
        match self {
            Self::None => NativeCycleStartLookup::None,
            Self::One(candidate) => NativeCycleStartLookup::Unique(candidate),
            Self::Multiple => NativeCycleStartLookup::Ambiguous,
        }
    }
}

#[derive(Default)]
struct NativeCycleStartSpellingIndex {
    exact: NativeCycleStartCandidates,
    singleton_count: usize,
    last_cycle_index: Option<usize>,
}

#[derive(Default)]
struct NativeCycleStartBucket {
    any_spelling: NativeCycleStartCandidates,
    singleton_count: usize,
    // These two candidates answer a unique-node fallback after excluding one
    // exact spelling, without walking the remaining singleton cycles.
    first_singleton: Option<(NativeChartSpellingKey, NativeCycleStartCandidate)>,
    first_other_singleton: Option<(NativeChartSpellingKey, NativeCycleStartCandidate)>,
}

impl NativeCycleStartBucket {
    fn insert_singleton(
        &mut self,
        spelling: NativeChartSpellingKey,
        candidate: NativeCycleStartCandidate,
    ) -> Result<()> {
        self.singleton_count = self
            .singleton_count
            .checked_add(1)
            .ok_or(ResourceError::SizeOverflow)?;
        match self.first_singleton {
            None => self.first_singleton = Some((spelling, candidate)),
            Some((first_spelling, _))
                if spelling != first_spelling && self.first_other_singleton.is_none() =>
            {
                self.first_other_singleton = Some((spelling, candidate));
            },
            Some(_) => {},
        }
        Ok(())
    }

    fn unique_singleton_other_than(
        &self,
        spelling: NativeChartSpellingKey,
    ) -> Result<NativeCycleStartCandidate> {
        let candidate = self
            .first_singleton
            .filter(|(first_spelling, _)| *first_spelling != spelling)
            .or_else(|| {
                self.first_other_singleton
                    .filter(|(other_spelling, _)| *other_spelling != spelling)
            });
        candidate.map(|(_, candidate)| candidate).ok_or_else(|| {
            GeometryErrorKind::Invalid(
                "Native cycle-start singleton summary has no unique candidate".into(),
            )
            .into()
        })
    }
}

pub(in crate::geometry::topology::q_embedding) struct NativeCycleStartIndex {
    by_region_role_node: HashMap<(usize, bool, EmissionNodeId), NativeCycleStartBucket>,
    by_region_role_node_spelling: HashMap<
        (usize, bool, EmissionNodeId, NativeChartSpellingKey),
        NativeCycleStartSpellingIndex,
    >,
}

struct NativeCycleLocalNodeStart {
    first: usize,
    spelling: NativeChartSpellingKey,
    count: u8,
}

pub(in crate::geometry::topology::q_embedding) struct NativeGraphPart {
    pub(in crate::geometry::topology::q_embedding) face: NativeFaceId,
    pub(in crate::geometry::topology::q_embedding) region: usize,
    pub(in crate::geometry::topology::q_embedding) shell: usize,
    pub(in crate::geometry::topology::q_embedding) holes: Vec<usize>,
}

pub(in crate::geometry::topology::q_embedding) fn native_reference_cycles_and_parts(
    graph: &NativeReferenceGraph,
    area_edges: &[NativeAreaEdge],
    line_edges: &[NativeAreaEdge],
    whole_sphere: bool,
    materialize_areas: bool,
    budget: &mut impl Work,
) -> Result<NativeReferenceGraphResult> {
    let face_count = usize::try_from(graph.faces.count).map_err(|_| ResourceError::SizeOverflow)?;
    budget.charge_into_error(
        u64::try_from(face_count)
            .map_err(|_| ResourceError::SizeOverflow)?
            .checked_mul(4)
            .ok_or(ResourceError::SizeOverflow)?,
    )?;
    let mut face_regions = vec![None; face_count];
    let mut face_regions_known = vec![false; face_count];
    let mut face_cells = vec![None; face_count];
    let mut face_cells_known = vec![false; face_count];
    let written_edge_count = area_edges
        .len()
        .checked_add(line_edges.len())
        .ok_or(ResourceError::SizeOverflow)?;
    if graph.written_edges.len() != written_edge_count {
        return Err(GeometryErrorKind::Invalid(
            "native written edges do not match their graph back-map".into(),
        )
        .into());
    }
    for (face, offsets) in graph
        .face_source_uses
        .offsets
        .array_windows::<2>()
        .enumerate()
    {
        let [start, end] = offsets;
        let rows = graph
            .face_source_uses
            .uses
            .get(*start as usize..*end as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("native face-source range is incomplete".into())
            })?;
        for use_row in rows {
            let edge = native_graph_writer_edge(area_edges, line_edges, use_row.written_edge)?;
            if matches!(edge.owner, NativeGraphEdgeOwner::Line) {
                // A line's two Q germs can belong to different source faces;
                // they are side evidence, not one label for its planar host.
                continue;
            }
            let NativeAreaEdgeKind::Producer { known_spans, .. } = &edge.kind else {
                return Err(GeometryErrorKind::Invalid(
                    "native face-source row references a presentation edge".into(),
                )
                .into());
            };
            let source = known_spans.get(use_row.source_span).ok_or_else(|| {
                GeometryErrorKind::Invalid("native face-source row has no Q-span use".into())
            })?;
            let germ = source
                .face_germs
                .and_then(|germs| germs.get(use_row.side.index()).copied())
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("native face-source row has no Q face germ".into())
                })?;
            // Another input can select an output face beyond this source germ.
            if let Some(region) = germ.selected_root {
                assign_native_face_region(
                    &mut face_regions,
                    &mut face_regions_known,
                    u32::try_from(face).map_err(|_| ResourceError::SizeOverflow)?,
                    Some(region),
                    budget,
                )?;
            }
        }
    }
    for (atom_index, atom) in graph.atoms.iter().enumerate() {
        budget.charge_into_error(1)?;
        for contributor in &atom.contributors {
            budget.charge_into_error(1)?;
            let Some(region) = contributor.region else {
                continue;
            };
            let written = *graph
                .written_edges
                .get(contributor.written_edge)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("native graph contributor is unknown".into())
                })?;
            let area_edge = area_edges.get(contributor.written_edge).ok_or_else(|| {
                GeometryErrorKind::Invalid("native graph contributor has no written edge".into())
            })?;
            let NativeGraphEdgeOwner::Area {
                region: edge_region,
            } = area_edge.owner
            else {
                return Err(GeometryErrorKind::Invalid(
                    "native area contributor maps to a line producer".into(),
                )
                .into());
            };
            if written.atom as usize != atom_index
                || written.reversed != contributor.reversed
                || edge_region != region
            {
                return Err(GeometryErrorKind::Invalid(
                    "native graph contributor changed its written-edge authority".into(),
                )
                .into());
            }
            let slot = native_graph_slot(graph, written.atom, !written.reversed, budget)?;
            let local_face = *graph.topology.face_of.get(slot).ok_or_else(|| {
                GeometryErrorKind::Invalid("native graph contributor has no face walk".into())
            })?;
            let face = *graph
                .faces
                .walk_faces
                .get(local_face as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("native graph walk has no global face".into())
                })?;
            assign_native_face_region(
                &mut face_regions,
                &mut face_regions_known,
                face,
                Some(region),
                budget,
            )?;
            if let NativeAreaEdgeKind::WholeSphereCover { cell } = &area_edge.kind {
                let cell_known = face_cells_known.get_mut(face as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "native graph face has no chart-cell known bit".into(),
                    )
                })?;
                let assigned_cell = face_cells.get_mut(face as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("native graph face has no chart-cell row".into())
                })?;
                if *cell_known && *assigned_cell != Some(*cell) {
                    return Err(GeometryErrorKind::Invalid(
                        "native graph face has conflicting whole-sphere cell owners".into(),
                    )
                    .into());
                }
                *cell_known = true;
                *assigned_cell = Some(*cell);
            }
        }
    }
    budget.charge_into_error(
        u64::try_from(face_count)
            .map_err(|_| ResourceError::SizeOverflow)?
            .checked_mul(2)
            .ok_or(ResourceError::SizeOverflow)?,
    )?;
    let mut face_union = UnionFind::new(face_count);
    for (atom_index, atom) in graph.atoms.iter().enumerate() {
        let mut area_boundary = false;
        for contributor in &atom.contributors {
            budget.charge_into_error(1)?;
            area_boundary |= contributor.region.is_some();
        }
        if area_boundary {
            continue;
        }
        let atom_id = u32::try_from(atom_index).map_err(|_| ResourceError::SizeOverflow)?;
        let mut global_faces = [0_usize; 2];
        for (direction, face_slot) in global_faces.iter_mut().enumerate() {
            budget.charge_into_error(1)?;
            let slot = native_graph_slot(graph, atom_id, direction == 0, budget)?;
            let local_face = *graph.topology.face_of.get(slot).ok_or_else(|| {
                GeometryErrorKind::Invalid("native line support slot has no local face".into())
            })?;
            *face_slot = *graph
                .faces
                .walk_faces
                .get(local_face as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "native line support local face has no global face".into(),
                    )
                })? as usize;
        }
        let [left_face, right_face] = global_faces;
        let left_known = *face_regions_known.get(left_face).ok_or_else(|| {
            GeometryErrorKind::Invalid("native left line-support face has no region state".into())
        })?;
        let right_known = *face_regions_known.get(right_face).ok_or_else(|| {
            GeometryErrorKind::Invalid("native right line-support face has no region state".into())
        })?;
        let left_region = *face_regions.get(left_face).ok_or_else(|| {
            GeometryErrorKind::Invalid("native left line-support face has no region".into())
        })?;
        let right_region = *face_regions.get(right_face).ok_or_else(|| {
            GeometryErrorKind::Invalid("native right line-support face has no region".into())
        })?;
        let left_cell_known = *face_cells_known.get(left_face).ok_or_else(|| {
            GeometryErrorKind::Invalid("native left line-support face has no cell state".into())
        })?;
        let right_cell_known = *face_cells_known.get(right_face).ok_or_else(|| {
            GeometryErrorKind::Invalid("native right line-support face has no cell state".into())
        })?;
        let left_cell = *face_cells.get(left_face).ok_or_else(|| {
            GeometryErrorKind::Invalid("native left line-support face has no cell".into())
        })?;
        let right_cell = *face_cells.get(right_face).ok_or_else(|| {
            GeometryErrorKind::Invalid("native right line-support face has no cell".into())
        })?;
        let region_compatible = !left_known || !right_known || left_region == right_region;
        let cell_compatible =
            !whole_sphere || !left_cell_known || !right_cell_known || left_cell == right_cell;
        let compatible = region_compatible && cell_compatible;
        if left_face != right_face && compatible {
            face_union.union_with_work(left_face, right_face, budget)?;
        }
    }
    budget.charge_into_error(
        u64::try_from(face_count)
            .map_err(|_| ResourceError::SizeOverflow)?
            .checked_mul(6)
            .ok_or(ResourceError::SizeOverflow)?,
    )?;
    let mut minimum_by_root = vec![usize::MAX; face_count];
    for face in 0..face_count {
        let root = face_union.find_with_work(face, budget)?;
        let minimum = minimum_by_root.get_mut(root).ok_or_else(|| {
            GeometryErrorKind::Invalid("native line-face component is unknown".into())
        })?;
        *minimum = (*minimum).min(face);
    }
    let mut face_groups = Vec::with_capacity(face_count);
    let mut group_regions = vec![None; face_count];
    let mut group_regions_known = vec![false; face_count];
    let mut group_cells = vec![None; face_count];
    let mut group_cells_known = vec![false; face_count];
    for face in 0..face_count {
        let root = face_union.find_with_work(face, budget)?;
        let group = *minimum_by_root.get(root).ok_or_else(|| {
            GeometryErrorKind::Invalid("native line-face component has no canonical face".into())
        })?;
        face_groups.push(u32::try_from(group).map_err(|_| ResourceError::SizeOverflow)?);
        let selected_region = *face_regions.get(face).ok_or_else(|| {
            GeometryErrorKind::Invalid("native global face has no selected-region row".into())
        })?;
        let selected_region_known = *face_regions_known.get(face).ok_or_else(|| {
            GeometryErrorKind::Invalid("native global face has no region-known row".into())
        })?;
        if selected_region_known {
            let group_region = group_regions.get_mut(group).ok_or_else(|| {
                GeometryErrorKind::Invalid("native area-face group is unknown".into())
            })?;
            let group_region_known = group_regions_known.get_mut(group).ok_or_else(|| {
                GeometryErrorKind::Invalid("native area-face group has no region-known row".into())
            })?;
            if *group_region_known && *group_region != selected_region {
                return Err(GeometryErrorKind::Invalid(
                    "native line support joins conflicting selected regions".into(),
                )
                .into());
            }
            *group_region = selected_region;
            *group_region_known = true;
        }
        let selected_cell = *face_cells.get(face).ok_or_else(|| {
            GeometryErrorKind::Invalid("native global face has no chart-cell row".into())
        })?;
        let selected_cell_known = *face_cells_known.get(face).ok_or_else(|| {
            GeometryErrorKind::Invalid("native global face has no cell-known row".into())
        })?;
        if selected_cell_known {
            let group_cell = group_cells.get_mut(group).ok_or_else(|| {
                GeometryErrorKind::Invalid("native chart-cell group is unknown".into())
            })?;
            let group_cell_known = group_cells_known.get_mut(group).ok_or_else(|| {
                GeometryErrorKind::Invalid("native chart-cell group has no cell-known row".into())
            })?;
            if *group_cell_known && *group_cell != selected_cell {
                return Err(GeometryErrorKind::Invalid(
                    "native line support joins distinct whole-sphere chart cells".into(),
                )
                .into());
            }
            *group_cell = selected_cell;
            *group_cell_known = true;
        }
    }
    for face in 0..face_count {
        budget.charge_into_error(1)?;
        let group = *face_groups.get(face).ok_or_else(|| {
            GeometryErrorKind::Invalid("native global face has no area-group mapping".into())
        })? as usize;
        let region_known = *group_regions_known.get(group).ok_or_else(|| {
            GeometryErrorKind::Invalid("native area-face group has no region-known slot".into())
        })?;
        *face_regions.get_mut(face).ok_or_else(|| {
            GeometryErrorKind::Invalid("native global face has no region slot".into())
        })? = if region_known {
            *group_regions.get(group).ok_or_else(|| {
                GeometryErrorKind::Invalid("native area-face group has no region slot".into())
            })?
        } else {
            None
        };
        *face_regions_known.get_mut(face).ok_or_else(|| {
            GeometryErrorKind::Invalid("native global face has no region-known slot".into())
        })? = region_known;
        let cell_known = *group_cells_known.get(group).ok_or_else(|| {
            GeometryErrorKind::Invalid("native chart-cell group has no cell-known slot".into())
        })?;
        *face_cells.get_mut(face).ok_or_else(|| {
            GeometryErrorKind::Invalid("native global face has no chart-cell slot".into())
        })? = if cell_known {
            *group_cells.get(group).ok_or_else(|| {
                GeometryErrorKind::Invalid("native chart-cell group has no cell slot".into())
            })?
        } else {
            None
        };
        *face_cells_known.get_mut(face).ok_or_else(|| {
            GeometryErrorKind::Invalid("native global face has no cell-known slot".into())
        })? = cell_known;
    }
    if face_regions.first().is_some_and(Option::is_some) {
        return Err(GeometryErrorKind::Invalid(
            "native selected area boundary labels the unbounded chart face".into(),
        )
        .into());
    }
    if face_cells.first().is_some_and(Option::is_some) {
        return Err(GeometryErrorKind::Invalid(
            "native whole-sphere cover labels the unbounded chart face".into(),
        )
        .into());
    }
    if !materialize_areas {
        return Ok(NativeReferenceGraphResult {
            cycles: Vec::new(),
            parts: Vec::new(),
            face_regions,
            face_groups,
            face_cells,
        });
    }

    let boundary_selects = |face, other| {
        let selected = face_regions.get(face).copied().flatten().is_some();
        let adjacent_selected = face_regions.get(other).copied().flatten().is_some();
        let same_area_face = face_groups.get(face) == face_groups.get(other);
        let different_cell = whole_sphere
            && face_cells.get(face).copied().flatten() != face_cells.get(other).copied().flatten();
        selected && (!adjacent_selected || (!same_area_face && (!whole_sphere || different_cell)))
    };
    let dart_count = graph.topology.targets.len();
    budget
        .charge_into_error(u64::try_from(dart_count).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut selected_boundary = Vec::with_capacity(dart_count);
    for (slot, &local_face) in graph.topology.face_of.iter().enumerate() {
        budget.charge_into_error(1)?;
        let face = *graph
            .faces
            .walk_faces
            .get(local_face as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("native boundary dart has no global face".into())
            })? as usize;
        let twin =
            *graph.topology.twins.get(slot).ok_or_else(|| {
                GeometryErrorKind::Invalid("native boundary dart has no twin".into())
            })? as usize;
        let other_local = *graph.topology.face_of.get(twin).ok_or_else(|| {
            GeometryErrorKind::Invalid("native boundary twin has no local face".into())
        })?;
        let other = *graph
            .faces
            .walk_faces
            .get(other_local as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("native boundary twin has no global face".into())
            })? as usize;
        selected_boundary.push(boundary_selects(face, other));
    }
    let walks = graph
        .faces
        .boundary_walks_with_work(&graph.topology, boundary_selects, budget)?;
    validate_native_boundary_walks(graph, &selected_boundary, &walks, budget)?;
    budget
        .charge_into_error(u64::try_from(walks.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut cycles = Vec::new();
    for slots in walks {
        budget.charge_into_error(1)?;
        let first_slot = *slots.first().ok_or_else(|| {
            GeometryErrorKind::Invalid("native graph boundary walk is empty".into())
        })?;
        let local_face = *graph
            .topology
            .face_of
            .get(first_slot as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("native graph boundary slot has no local face".into())
            })?;
        let face = *graph
            .faces
            .walk_faces
            .get(local_face as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("native graph boundary walk has no global face".into())
            })?;
        let area_face = *face_groups.get(face as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("native selected boundary has no area-face group".into())
        })?;
        let region = face_regions
            .get(face as usize)
            .copied()
            .flatten()
            .ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "native graph selected boundary has no selected region".into(),
                )
            })?;
        if slots.len() < 3 {
            return Err(GeometryErrorKind::Invalid(
                "native selected chart boundary has fewer than three edges".into(),
            )
            .into());
        }
        budget.charge_into_error(
            u64::try_from(slots.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut edges = Vec::with_capacity(slots.len());
        for &slot in &slots {
            budget.charge_into_error(1)?;
            let slot_index = slot as usize;
            let from = *graph.topology.owners.get(slot_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("native graph walk slot has no origin".into())
            })?;
            let to = *graph.topology.targets.get(slot_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("native graph walk slot has no target".into())
            })?;
            let edge_face = *graph
                .faces
                .walk_faces
                .get(*graph.topology.face_of.get(slot_index).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "native graph boundary slot has no local face".into(),
                    )
                })? as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "native graph boundary slot has no global face".into(),
                    )
                })?;
            if face_groups.get(edge_face as usize).copied() != Some(area_face) {
                return Err(GeometryErrorKind::Invalid(
                    "native graph boundary walk changes its selected area face".into(),
                )
                .into());
            }
            let atom = *graph.edge_of_slot.get(slot_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("native graph boundary slot has no atom".into())
            })?;
            let atom_endpoints = graph.edge_nodes.get(atom as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("native graph boundary atom is unknown".into())
            })?;
            let slot_forward = [from, to] == *atom_endpoints;
            if !slot_forward && [to, from] != *atom_endpoints {
                return Err(GeometryErrorKind::Invalid(
                    "native graph boundary slot does not match its atom endpoints".into(),
                )
                .into());
            }
            let atom_data = graph.atoms.get(atom as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("native graph boundary atom has no contributors".into())
            })?;
            let mut contributors = Vec::new();
            for contributor in &atom_data.contributors {
                budget.charge_into_error(1)?;
                if contributor.region == Some(region) && contributor.reversed != slot_forward {
                    contributors.push(contributor.written_edge);
                }
            }
            if contributors.is_empty() {
                return Err(GeometryErrorKind::Invalid(
                    "native selected boundary edge has no directed written contributor".into(),
                )
                .into());
            }
            edges.push(NativeGraphCycleEdge {
                atom,
                reversed: !slot_forward,
                contributors,
            });
        }
        let slot_at = |step| {
            slots
                .get(step)
                .copied()
                .expect("walk steps are bounded by the admitted face slots")
        };
        let shell = walk_is_shell_with_work(
            &graph.points,
            &graph.topology,
            slots.len(),
            slot_at,
            slot_at,
            budget,
        )?;
        cycles.push(NativeGraphCycle {
            face: area_face,
            region,
            edges,
            shell,
        });
    }

    budget
        .charge_into_error(u64::try_from(face_count).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut cycles_by_face = vec![Vec::<usize>::new(); face_count];
    for (cycle_index, cycle) in cycles.iter().enumerate() {
        budget.charge_into_error(1)?;
        cycles_by_face
            .get_mut(cycle.face as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("native chart cycle has an unknown face".into())
            })?
            .push(cycle_index);
    }
    let mut parts = Vec::new();
    for face in 1..face_count {
        budget.charge_into_error(1)?;
        if face_groups.get(face).copied()
            != Some(u32::try_from(face).map_err(|_| ResourceError::SizeOverflow)?)
        {
            continue;
        }
        let Some(region) = face_regions.get(face).copied().flatten() else {
            continue;
        };
        let cycle_indices = cycles_by_face.get(face).ok_or_else(|| {
            GeometryErrorKind::Invalid("native selected face has no cycle row".into())
        })?;
        if cycle_indices.is_empty() {
            return Err(GeometryErrorKind::Invalid(
                "native selected face has no closed boundary walk".into(),
            )
            .into());
        }
        let mut shell = None;
        let mut holes = Vec::new();
        for &cycle_index in cycle_indices {
            budget.charge_into_error(1)?;
            let cycle = cycles.get(cycle_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("native selected face cycle is unknown".into())
            })?;
            if cycle.region != region {
                return Err(GeometryErrorKind::Invalid(
                    "native selected face cycles disagree on their region".into(),
                )
                .into());
            }
            if cycle.shell {
                if shell.replace(cycle_index).is_some() {
                    return Err(GeometryErrorKind::Invalid(
                        "native selected face has multiple shell walks".into(),
                    )
                    .into());
                }
            } else {
                holes.push(cycle_index);
            }
        }
        let shell = shell.ok_or_else(|| {
            GeometryErrorKind::Invalid("native selected face has no shell walk".into())
        })?;
        parts.push(NativeGraphPart {
            face: NativeFaceId(u32::try_from(face).map_err(|_| ResourceError::SizeOverflow)?),
            region,
            shell,
            holes,
        });
    }
    budget
        .charge_into_error(u64::try_from(cycles.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut cycle_membership = vec![false; cycles.len()];
    for part in &parts {
        for &cycle_index in std::iter::once(&part.shell).chain(&part.holes) {
            budget.charge_into_error(1)?;
            let seen = cycle_membership.get_mut(cycle_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("native area part references an unknown cycle".into())
            })?;
            if std::mem::replace(seen, true) {
                return Err(GeometryErrorKind::Invalid(
                    "native area cycle belongs to more than one part".into(),
                )
                .into());
            }
        }
    }
    for &seen in &cycle_membership {
        budget.charge_into_error(1)?;
        if !seen {
            return Err(GeometryErrorKind::Invalid(
                "native selected boundary cycle has no area part".into(),
            )
            .into());
        }
    }
    Ok(NativeReferenceGraphResult {
        cycles,
        parts,
        face_regions,
        face_groups,
        face_cells,
    })
}

#[cfg(test)]
fn exhaustive_native_cycle_start_vertex_index(
    edge_nodes: &[[u32; 2]],
    vertex_keys: &[NativeChartVertexKey],
    cycle: &NativeGraphCycle,
    target: EmissionNodeId,
    spelling: Option<NativeChartSpellingKey>,
    budget: &mut impl Work,
) -> Result<Option<usize>> {
    let mut exact_match = None;
    let mut node_match = None;
    let mut node_match_count = 0_usize;
    for (edge_index, edge) in cycle.edges.iter().enumerate() {
        budget.charge_into_error(1)?;
        let endpoints = edge_nodes.get(edge.atom as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("native cycle start edge has no graph endpoints".into())
        })?;
        let start_vertex = if edge.reversed {
            endpoints[1]
        } else {
            endpoints[0]
        };
        let key = vertex_keys.get(start_vertex as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("native cycle start vertex has no chart key".into())
        })?;
        if key.node != target {
            continue;
        }
        node_match.get_or_insert(edge_index);
        node_match_count = node_match_count
            .checked_add(1)
            .ok_or(ResourceError::SizeOverflow)?;
        if spelling.is_none_or(|spelling| key.spelling == spelling) {
            exact_match.get_or_insert(edge_index);
        }
    }
    Ok(exact_match.or_else(|| (node_match_count == 1).then_some(node_match).flatten()))
}

fn native_drawing_start_spelling(
    frame: GeographicFrame,
    vertex: EmittedVertex<'_>,
    budget: &mut impl Work,
) -> Result<Option<NativeChartSpellingKey>> {
    let key = quotient_key_for_chart_position_with_work(frame, &vertex.port.point, budget)?;
    Ok(match key {
        ExactQuotientKey::Coordinate(_) => Some(NativeChartSpellingKey::Interior(0)),
        ExactQuotientKey::Seam(_) => {
            let half = ExactRational::from_f64(frame.longitude().half_period());
            let negative_half = ExactRational::zero().try_subtract_with_work(&half, budget)?;
            let longitude = ExactRational::from_f64(vertex.point.x);
            if longitude.try_cmp_with_work(&half, budget)?.is_eq() {
                Some(NativeChartSpellingKey::SeamEast)
            } else if longitude.try_cmp_with_work(&negative_half, budget)?.is_eq() {
                Some(NativeChartSpellingKey::SeamWest)
            } else {
                None
            }
        },
        ExactQuotientKey::NorthPole | ExactQuotientKey::SouthPole => {
            Some(NativeChartSpellingKey::Pole(vertex.point.x.to_bits()))
        },
    })
}

impl NativeCycleStartIndex {
    pub(in crate::geometry::topology::q_embedding) fn build(
        cycles: &[NativeGraphCycle],
        edge_nodes: &[[u32; 2]],
        vertex_keys: &[NativeChartVertexKey],
        eligible_roles: &HashSet<(usize, bool)>,
        hash_state: &ahash::RandomState,
        budget: &mut impl Work,
    ) -> Result<Self> {
        budget.charge_into_error(1)?;
        let mut by_region_role_node: HashMap<
            (usize, bool, EmissionNodeId),
            NativeCycleStartBucket,
        > = HashMap::with_hasher(hash_state.clone());
        let mut by_region_role_node_spelling: HashMap<
            (usize, bool, EmissionNodeId, NativeChartSpellingKey),
            NativeCycleStartSpellingIndex,
        > = HashMap::with_hasher(hash_state.clone());
        let mut local_nodes: HashMap<EmissionNodeId, NativeCycleLocalNodeStart> =
            HashMap::with_hasher(hash_state.clone());
        let mut local_node_order = Vec::new();

        for (cycle_index, cycle) in cycles.iter().enumerate() {
            budget.charge_into_error(1)?;
            if !eligible_roles.contains(&(cycle.region, cycle.shell)) {
                continue;
            }
            local_nodes.clear();
            local_node_order.clear();
            for (edge_index, edge) in cycle.edges.iter().enumerate() {
                budget.charge_into_error(1)?;
                let endpoints = edge_nodes.get(edge.atom as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "native cycle start edge has no graph endpoints".into(),
                    )
                })?;
                let start_vertex = if edge.reversed {
                    endpoints[1]
                } else {
                    endpoints[0]
                };
                let key = vertex_keys.get(start_vertex as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("native cycle start vertex has no chart key".into())
                })?;
                if let Some(node_start) = local_nodes.get_mut(&key.node) {
                    node_start.count = node_start.count.saturating_add(1);
                } else {
                    local_nodes.insert(key.node, NativeCycleLocalNodeStart {
                        first: edge_index,
                        spelling: key.spelling,
                        count: 1,
                    });
                    local_node_order.push(key.node);
                    let bucket = by_region_role_node
                        .entry((cycle.region, cycle.shell, key.node))
                        .or_default();
                    bucket.any_spelling.insert(NativeCycleStartCandidate {
                        cycle_index,
                        start: edge_index,
                    });
                }
                let spelling_index = by_region_role_node_spelling
                    .entry((cycle.region, cycle.shell, key.node, key.spelling))
                    .or_default();
                if spelling_index.last_cycle_index != Some(cycle_index) {
                    budget.charge_into_error(1)?;
                    spelling_index.exact.insert(NativeCycleStartCandidate {
                        cycle_index,
                        start: edge_index,
                    });
                    spelling_index.last_cycle_index = Some(cycle_index);
                }
            }

            for node in local_node_order.iter().copied() {
                budget.charge_into_error(1)?;
                let node_start = local_nodes.get(&node).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Native cycle-start node summary is missing".into())
                })?;
                if node_start.count == 1 {
                    let bucket = by_region_role_node
                        .get_mut(&(cycle.region, cycle.shell, node))
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "Native cycle-start bucket is missing".into(),
                            )
                        })?;
                    bucket.insert_singleton(node_start.spelling, NativeCycleStartCandidate {
                        cycle_index,
                        start: node_start.first,
                    })?;
                    let spelling_index = by_region_role_node_spelling
                        .get_mut(&(cycle.region, cycle.shell, node, node_start.spelling))
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "Native cycle-start spelling summary is missing".into(),
                            )
                        })?;
                    spelling_index.singleton_count = spelling_index
                        .singleton_count
                        .checked_add(1)
                        .ok_or(ResourceError::SizeOverflow)?;
                }
            }
        }

        Ok(Self {
            by_region_role_node,
            by_region_role_node_spelling,
        })
    }

    pub(in crate::geometry::topology::q_embedding) fn lookup(
        &self,
        query: NativeCycleStartQuery,
    ) -> Result<NativeCycleStartLookup> {
        let Some(bucket) = self
            .by_region_role_node
            .get(&(query.region, query.shell, query.node))
        else {
            return Ok(NativeCycleStartLookup::None);
        };
        let Some(spelling) = query.spelling else {
            return Ok(bucket.any_spelling.lookup());
        };

        let spelling_index = self.by_region_role_node_spelling.get(&(
            query.region,
            query.shell,
            query.node,
            spelling,
        ));
        let singleton_same = spelling_index.map_or(0, |index| index.singleton_count);
        let singleton_other = bucket
            .singleton_count
            .checked_sub(singleton_same)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "Native cycle-start singleton counts are inconsistent".into(),
                )
            })?;
        match spelling_index.map_or(NativeCycleStartCandidates::None, |index| index.exact) {
            NativeCycleStartCandidates::Multiple => Ok(NativeCycleStartLookup::Ambiguous),
            NativeCycleStartCandidates::One(candidate) => Ok(if singleton_other == 0 {
                NativeCycleStartLookup::Unique(candidate)
            } else {
                NativeCycleStartLookup::Ambiguous
            }),
            NativeCycleStartCandidates::None => match singleton_other {
                0 => Ok(NativeCycleStartLookup::None),
                1 => Ok(NativeCycleStartLookup::Unique(
                    bucket.unique_singleton_other_than(spelling)?,
                )),
                _ => Ok(NativeCycleStartLookup::Ambiguous),
            },
        }
    }
}

fn native_cycle_rotations_for_queries(
    queries: &[NativeCycleStartQuery],
    cycles: &[NativeGraphCycle],
    edge_nodes: &[[u32; 2]],
    vertex_keys: &[NativeChartVertexKey],
    budget: &mut impl Work,
) -> Result<Vec<Option<usize>>> {
    let mut rotations = vec![None; cycles.len()];
    if queries.is_empty() {
        return Ok(rotations);
    }

    let hash_state = collection_hash_state()?.clone();
    let mut eligible_roles = HashSet::with_hasher(hash_state.clone());
    for query in queries {
        eligible_roles.insert((query.region, query.shell));
    }
    let index = NativeCycleStartIndex::build(
        cycles,
        edge_nodes,
        vertex_keys,
        &eligible_roles,
        &hash_state,
        budget,
    )?;
    for &query in queries {
        budget.charge_into_error(1)?;
        let NativeCycleStartLookup::Unique(candidate) = index.lookup(query)? else {
            continue;
        };
        let slot = rotations.get_mut(candidate.cycle_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("Native cycle rotation row is unknown".into())
        })?;
        if slot.is_some_and(|previous| previous != candidate.start) {
            return Err(GeometryErrorKind::Invalid(
                "Native Q rings request conflicting starts on one graph cycle".into(),
            )
            .into());
        }
        *slot = Some(candidate.start);
    }
    Ok(rotations)
}

pub(in crate::geometry::topology::q_embedding) fn align_native_cycle_starts(
    frame: GeographicFrame,
    topology: &ExactGeographicTopology,
    drawing: &QEmissionDrawing<'_>,
    ring_roles: &[bool],
    nodes: &EmissionNodeRegistry,
    graph: &NativeReferenceGraph,
    cycles: &mut [NativeGraphCycle],
    budget: &mut impl Work,
) -> Result<()> {
    // Native graph walks can choose another first dart; keep the Q ring's literal start.
    let DrawnPolygons::Regions { rings, .. } = &drawing.polygons else {
        return Ok(());
    };
    if rings.len() != ring_roles.len() {
        return Err(GeometryErrorKind::Invalid(
            "Native cycle starts do not match the certified Q ring roles".into(),
        )
        .into());
    }
    let mut queries = Vec::with_capacity(rings.len());
    for (ring_index, ring) in rings.iter().enumerate() {
        budget.charge_into_error(1)?;
        let Some(port_id) = ring.native_start_port else {
            continue;
        };
        let vertex = ring
            .vertices
            .iter()
            .find(|vertex| vertex.port.id == port_id)
            .copied()
            .ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "Native ring-order anchor is absent from its retained Q ring".into(),
                )
            })?;
        let q_port = topology.q.ports.get(port_id.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("Native ring-order anchor has no Q port".into())
        })?;
        let target = nodes.source_node(q_port.node)?;
        let spelling = native_drawing_start_spelling(frame, vertex, budget)?;
        let is_hole = *ring_roles.get(ring_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("Native ring-order role is unknown".into())
        })?;
        queries.push(NativeCycleStartQuery {
            region: ring.region,
            shell: !is_hole,
            node: target,
            spelling,
        });
    }
    let rotations = native_cycle_rotations_for_queries(
        &queries,
        cycles,
        &graph.edge_nodes,
        &graph.vertex_keys,
        budget,
    )?;
    for (cycle, rotation) in cycles.iter_mut().zip(rotations) {
        if let Some(rotation) = rotation {
            budget.charge_into_error(
                u64::try_from(cycle.edges.len()).map_err(|_| ResourceError::SizeOverflow)?,
            )?;
            cycle.edges.rotate_left(rotation);
        }
    }
    Ok(())
}

pub(in crate::geometry::topology::q_embedding) fn native_graph_cycle_vertex(
    frame: GeographicFrame,
    graph: &NativeReferenceGraph,
    cycle_edge: &NativeGraphCycleEdge,
    endpoint: usize,
    area_edges: &[NativeAreaEdge],
    budget: &mut impl Work,
) -> Result<NativePortData> {
    let first = *cycle_edge.contributors.first().ok_or_else(|| {
        GeometryErrorKind::Invalid("native graph cycle edge has no written contributor".into())
    })?;
    let first_edge = area_edges.get(first).ok_or_else(|| {
        GeometryErrorKind::Invalid("native graph cycle contributor is unknown".into())
    })?;
    if graph
        .written_edges
        .get(first)
        .is_none_or(|written| written.atom != cycle_edge.atom)
    {
        return Err(GeometryErrorKind::Invalid(
            "native graph cycle contributor maps to another atom".into(),
        )
        .into());
    }
    let mut point = first_edge
        .vertices
        .get(endpoint)
        .ok_or_else(|| GeometryErrorKind::Invalid("native graph cycle endpoint is unknown".into()))?
        .try_clone_with_work(budget)?;
    for &written_edge in cycle_edge.contributors.iter().skip(1) {
        budget.charge_into_error(1)?;
        if graph
            .written_edges
            .get(written_edge)
            .is_none_or(|written| written.atom != cycle_edge.atom)
        {
            return Err(GeometryErrorKind::Invalid(
                "native graph cycle contributor maps to another atom".into(),
            )
            .into());
        }
        let edge = area_edges.get(written_edge).ok_or_else(|| {
            GeometryErrorKind::Invalid("native graph cycle contributor is unknown".into())
        })?;
        let contributor = edge.vertices.get(endpoint).ok_or_else(|| {
            GeometryErrorKind::Invalid("native graph cycle endpoint is unknown".into())
        })?;
        point = merge_native_port_data(frame, &point, contributor, budget)?;
    }
    Ok(point)
}

pub(in crate::geometry::topology::q_embedding) fn native_graph_cycle_recipe(
    graph: &NativeReferenceGraph,
    cycle_edge: &NativeGraphCycleEdge,
    area_edges: &[NativeAreaEdge],
) -> Result<Option<FragmentRecipeId>> {
    let mut recipe = None;
    let mut seen = false;
    for &written_edge in &cycle_edge.contributors {
        let mapping = graph.written_edges.get(written_edge).ok_or_else(|| {
            GeometryErrorKind::Invalid("native graph cycle contributor is unknown".into())
        })?;
        if mapping.atom != cycle_edge.atom {
            return Err(GeometryErrorKind::Invalid(
                "native graph cycle contributor maps to another atom".into(),
            )
            .into());
        }
        let edge = area_edges.get(written_edge).ok_or_else(|| {
            GeometryErrorKind::Invalid("native graph cycle contributor is unknown".into())
        })?;
        let next = match &edge.kind {
            NativeAreaEdgeKind::Producer { recipe, .. } => Some(*recipe),
            NativeAreaEdgeKind::Restriction(_) | NativeAreaEdgeKind::WholeSphereCover { .. } => {
                None
            },
        };
        if seen && recipe != next {
            return Ok(None);
        }
        recipe = next;
        seen = true;
    }
    Ok(recipe)
}
