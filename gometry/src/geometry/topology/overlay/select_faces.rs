use super::*;

/// Incident faces can differ only on rows owning an incident source edge.
/// Count their union from one face and those rows, without expanding ancestors.
#[derive(Default)]
pub(in crate::geometry::topology) struct AreaSupport {
    pub(in crate::geometry::topology) faces: Vec<GlobalFaceId>,
    pub(in crate::geometry::topology) rows: Vec<SourceRowId>,
}
impl AreaSupport {
    pub(in crate::geometry::topology) fn clear(&mut self) {
        self.faces.clear();
        self.rows.clear();
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "span indices come from this topology's span table or admitted darts"
    )]
    pub(in crate::geometry::topology) fn append_span(
        &mut self,
        topology: &ExactGeographicTopology,
        span: usize,
        work: &mut impl Work,
    ) -> Result<()> {
        for owner in &topology.q.spans[span].owners {
            work.charge_into_error(1)?;
            let edge = topology
                .sources
                .edges
                .get(owner.source_edge.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q support references an unknown source edge".into())
                })?;
            self.rows.push(edge.row);
        }
        Ok(())
    }

    pub(in crate::geometry::topology) fn contains(
        &self,
        q: &super::super::q::ExactGeographicQ,
        row: SourceRowId,
        work: &mut impl Work,
    ) -> Result<bool> {
        for &face in &self.faces {
            work.charge_into_error(1)?;
            if q.face_rows.contains(face, row) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub(in crate::geometry::topology) fn count(
        &mut self,
        q: &super::super::q::ExactGeographicQ,
        work: &mut impl Work,
    ) -> Result<usize> {
        let Some(&first) = self.faces.first() else {
            return Ok(0);
        };
        try_sort_by_with_work(&mut self.rows, work, |left, right, _work| {
            Ok(left.cmp(right))
        })?;
        work.charge_into_error(
            u64::try_from(self.rows.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        self.rows.dedup();
        let mut count = q.face_rows.count(first);
        for &row in &self.rows {
            work.charge_into_error(1)?;
            if !q.face_rows.contains(first, row) && self.contains(q, row, work)? {
                count = count.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
            }
        }
        Ok(count)
    }

    pub(in crate::geometry::topology) fn with_rows(
        &mut self,
        q: &super::super::q::ExactGeographicQ,
        rows: &[SourceRowId],
        work: &mut impl Work,
    ) -> Result<(usize, bool)> {
        let mut count = self.count(q, work)?;
        for &row in rows {
            work.charge_into_error(1)?;
            if !self.contains(q, row, work)? {
                count = count.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
            }
        }
        work.charge_into_error(1)?;
        let first = rows.binary_search(&SourceRowId(0)).is_ok()
            || self.contains(q, SourceRowId(0), work)?;
        Ok((count, first))
    }
}
pub(in crate::geometry::topology) fn append_unique_rows(
    output: &mut Vec<SourceRowId>,
    left: &[SourceRowId],
    right: &[SourceRowId],
    work: &mut impl Work,
) -> Result<()> {
    output.clear();
    work.charge_into_error(
        u64::try_from(
            left.len()
                .checked_add(right.len())
                .ok_or(ResourceError::SizeOverflow)?,
        )
        .map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    output.extend_from_slice(left);
    output.extend_from_slice(right);
    try_sort_by_with_work(output, work, |left, right, _work| Ok(left.cmp(right)))?;
    work.charge_into_error(u64::try_from(output.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
    output.dedup();
    Ok(())
}
pub(in crate::geometry::topology) fn select_faces(
    q: &super::super::q::ExactGeographicQ,
    row_count: usize,
    operation: OverlayOp,
    work: &mut impl Work,
) -> Result<Vec<bool>> {
    let mut selected = Vec::with_capacity(q.global_face_count);
    for face in 0..q.global_face_count {
        work.charge_into_error(1)?;
        let face = GlobalFaceId(u32::try_from(face).map_err(|_| ResourceError::SizeOverflow)?);
        selected.push(selects_support(
            q.face_rows.count(face),
            q.face_rows.contains(face, SourceRowId(0)),
            row_count,
            operation,
        ));
    }
    Ok(selected)
}
pub(in crate::geometry::topology) struct FaceUnion {
    pub(in crate::geometry::topology) parents: Vec<usize>,
    pub(in crate::geometry::topology) selected: Vec<bool>,
}
impl FaceUnion {
    pub(in crate::geometry::topology) fn new(
        selected: &[bool],
        work: &mut impl Work,
    ) -> Result<Self> {
        work.charge_into_error(
            u64::try_from(
                selected
                    .len()
                    .checked_mul(2)
                    .ok_or(ResourceError::SizeOverflow)?,
            )
            .map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        Ok(Self {
            parents: (0..selected.len()).collect(),
            selected: selected.to_vec(),
        })
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "Face IDs enter the union only from its selected-face table"
    )]
    pub(in crate::geometry::topology) fn find(
        &mut self,
        mut face: usize,
        work: &mut impl Work,
    ) -> Result<usize> {
        while self.parents[face] != face {
            work.charge_into_error(1)?;
            self.parents[face] = self.parents[self.parents[face]];
            face = self.parents[face];
        }
        Ok(face)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "Both joined face IDs were admitted against the selected-face table"
    )]
    pub(in crate::geometry::topology) fn join(
        &mut self,
        left: usize,
        right: usize,
        work: &mut impl Work,
    ) -> Result<()> {
        let (left, right) = (self.find(left, work)?, self.find(right, work)?);
        if left != right {
            self.parents[left.max(right)] = left.min(right);
        }
        Ok(())
    }

    pub(in crate::geometry::topology) fn selected_root(
        &mut self,
        face: GlobalFaceId,
        work: &mut impl Work,
    ) -> Result<usize> {
        let index = face.0 as usize;
        let selected = self.selected.get(index).copied().ok_or_else(|| {
            GeometryErrorKind::Invalid("Q boundary references an unknown global face".into())
        })?;
        if !selected {
            return Err(GeometryErrorKind::Invalid(
                "Q boundary references an unselected global face".into(),
            )
            .into());
        }
        self.find(index, work)
    }
}
pub(in crate::geometry::topology) fn face_regions(
    q: &super::super::q::ExactGeographicQ,
    selected: &[bool],
    work: &mut impl Work,
) -> Result<FaceUnion> {
    face_regions_with_owner_labels(q, selected, None, work)
}
pub(in crate::geometry::topology) fn coverage_face_regions(
    q: &super::super::q::ExactGeographicQ,
    face_owners: &[Option<usize>],
    work: &mut impl Work,
) -> Result<FaceUnion> {
    if face_owners.len() != q.global_face_count {
        return Err(GeometryErrorKind::Invalid(
            "coverage face owners do not match Q's global face table".into(),
        )
        .into());
    }
    work.charge_into_error(
        u64::try_from(face_owners.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let selected = face_owners.iter().map(Option::is_some).collect::<Vec<_>>();
    face_regions_with_owner_labels(q, &selected, Some(face_owners), work)
}
pub(in crate::geometry::topology) fn face_regions_with_owner_labels(
    q: &super::super::q::ExactGeographicQ,
    selected: &[bool],
    owner_labels: Option<&[Option<usize>]>,
    work: &mut impl Work,
) -> Result<FaceUnion> {
    if selected.len() != q.global_face_count {
        return Err(GeometryErrorKind::Invalid(
            "Q face selection does not match its global face table".into(),
        )
        .into());
    }
    if owner_labels.is_some_and(|owners| owners.len() != selected.len()) {
        return Err(GeometryErrorKind::Invalid(
            "Q face owner labels do not match its global face table".into(),
        )
        .into());
    }
    let mut regions = FaceUnion::new(selected, work)?;
    for faces in &q.span_global_faces {
        work.charge_into_error(1)?;
        let Some([left, right]) = faces else {
            continue;
        };
        let left = left.0 as usize;
        let right = right.0 as usize;
        let left_selected = selected.get(left).copied().ok_or_else(|| {
            GeometryErrorKind::Invalid("Q span references an unknown global face".into())
        })?;
        let right_selected = selected.get(right).copied().ok_or_else(|| {
            GeometryErrorKind::Invalid("Q span references an unknown global face".into())
        })?;
        let same_owner = if let Some(owners) = owner_labels {
            let left_owner = owners.get(left).copied().ok_or_else(|| {
                GeometryErrorKind::Invalid("Q span has no left face owner label".into())
            })?;
            let right_owner = owners.get(right).copied().ok_or_else(|| {
                GeometryErrorKind::Invalid("Q span has no right face owner label".into())
            })?;
            left_owner.is_some() && left_owner == right_owner
        } else {
            true
        };
        if left_selected && right_selected && same_owner {
            regions.join(left, right, work)?;
        }
    }
    Ok(regions)
}
pub(in crate::geometry::topology) fn selected_region_facts(
    q: &super::super::q::ExactGeographicQ,
    selected: &[bool],
    regions: &mut FaceUnion,
    region_roots: &[usize],
    work: &mut impl Work,
) -> Result<Vec<SelectedRegionFact>> {
    if selected.len() != q.global_face_count {
        return Err(GeometryErrorKind::Invalid(
            "Q face selection does not match its global face table".into(),
        )
        .into());
    }
    work.charge_into_error(
        u64::try_from(region_roots.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut roots = region_roots.to_vec();
    for &root in &roots {
        work.charge_into_error(1)?;
        if !selected.get(root).copied().unwrap_or(false) || regions.find(root, work)? != root {
            return Err(GeometryErrorKind::Invalid(
                "emitted region is not a selected FaceUnion root".into(),
            )
            .into());
        }
    }
    try_sort_by_with_work(&mut roots, work, |left, right, _work| Ok(left.cmp(right)))?;
    work.charge_into_error(u64::try_from(roots.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
    roots.dedup();
    work.charge_into_error(u64::try_from(roots.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut facts = roots
        .into_iter()
        .map(|root| SelectedRegionFact {
            root,
            poles: SelectedRegionPoles::default(),
        })
        .collect::<Vec<_>>();
    if facts.is_empty() {
        return Ok(facts);
    }

    for pole in [ChartPole::North, ChartPole::South] {
        q.for_each_pole_sector_with_work(pole, work, |sector, work| {
            work.charge_into_error(1)?;
            let face = sector.face();
            if !selected.get(face.0 as usize).copied().ok_or_else(|| {
                GeometryErrorKind::Invalid("Q pole sector references an unknown face".into())
            })? {
                return Ok(());
            }
            let root = regions.selected_root(face, work)?;
            work.charge_into_error(1)?;
            let index = facts
                .binary_search_by_key(&root, |fact| fact.root)
                .map_err(|_| {
                    GeometryErrorKind::Invalid(
                        "selected Q pole sector has no retained region root".into(),
                    )
                })?;
            let poles = &mut facts
                .get_mut(index)
                .expect("binary search returned a selected-region fact")
                .poles;
            match pole {
                ChartPole::North => poles.north = true,
                ChartPole::South => poles.south = true,
            }
            Ok(())
        })?;
    }
    Ok(facts)
}
pub(in crate::geometry::topology) fn selected_face_roots(
    q: &super::super::q::ExactGeographicQ,
    selected: &[bool],
    regions: &mut FaceUnion,
    work: &mut impl Work,
) -> Result<Vec<Option<usize>>> {
    if selected.len() != q.global_face_count {
        return Err(GeometryErrorKind::Invalid(
            "Q face selection does not match its global face table".into(),
        )
        .into());
    }
    let mut roots = Vec::with_capacity(selected.len());
    for (index, &is_selected) in selected.iter().enumerate() {
        work.charge_into_error(1)?;
        if !is_selected {
            roots.push(None);
            continue;
        }
        let face = GlobalFaceId(u32::try_from(index).map_err(|_| ResourceError::SizeOverflow)?);
        roots.push(Some(regions.selected_root(face, work)?));
    }
    Ok(roots)
}
