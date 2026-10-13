//! Source interval storage and checked constrained-mesh publication.

use std::cmp::Ordering;

use super::super::exact_delaunay::{EMPTY, FaceSide, HalfedgeId, MeshCoordinates as _};
use super::refinement::triangle_min_angle_degrees;
#[cfg(test)]
use super::update_work;
use super::{
    CdtCoordinates, CdtFaceUse, CdtQuery, CdtSideBoundaryUse, CdtSideUse, CdtSourceUse,
    CertifiedBoundarySplit, ConstrainedMesh, ConstrainedMeshPlan, ConstraintPiece,
    ConstraintPieces, NodeProvenance, PieceCursor, PieceNode, RefinementConstruction,
    RestrictionConstraint, RestrictionPiece, SourceConstraint, SourcePiece, SourceRingStart,
    next_triangle_side, ordered, source_ring_owner,
};
use crate::collections::HashSet;
use crate::error::{ErrorKind, Result};
use crate::geographic::ChartPointSite;
use crate::geometry::numerics::product::ExactSign;
use crate::geometry::publication::{
    ChartRestriction, DirectedChartRestrictionUse, SyntheticChartVertex,
};
#[cfg(test)]
use crate::geometry::segments::ordering::try_sort_by;
use crate::geometry::tessellation::exact;
use crate::geometry::{GeometryErrorKind, Point, XY};

impl ConstrainedMeshPlan {
    pub(crate) fn site_chart_location(&self, site: usize) -> Option<ChartPointSite> {
        self.coordinates.retained_site_location(site)
    }

    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "the parent-owned final writer resolves source-occurrence coordinates for boundary output"
        )
    )]
    pub(crate) fn source_chart_location(&self, occurrence: usize) -> Option<ChartPointSite> {
        self.coordinates.retained_source_location(occurrence)
    }

    pub(crate) fn chart_proposal(&self, site: usize) -> XY {
        self.coordinates.site_xy(site)
    }

    pub(in crate::geometry::tessellation::delaunay) fn triangle_orientation(
        &self,
        sites: [usize; 3],
    ) -> Result<ExactSign> {
        self.coordinates
            .orient(sites.map(|site| self.coordinates.site_query(site)))
    }

    pub(in crate::geometry::tessellation::delaunay) fn validate_refined_triangle(
        &self,
        sites: [usize; 3],
        refinement: super::CdtRefinement,
    ) -> Result<()> {
        let super::CdtRefinement::On {
            min_angle,
            max_area,
        } = refinement
        else {
            return Ok(());
        };
        let [a, b, c] = self.coordinates.metric_frame(sites)?;
        if let Some(limit) = min_angle
            && triangle_min_angle_degrees(a, b, c) + 1.0e-9 < limit.get()
        {
            return Err(GeometryErrorKind::triangulation(format!(
                "constrained refinement did not satisfy min_angle={} degrees",
                limit.get()
            )));
        }
        if let Some(limit) = max_area
            && self.coordinates.triangle_exceeds_area(sites, limit.get())?
        {
            return Err(GeometryErrorKind::triangulation(format!(
                "constrained refinement did not satisfy max_area={}",
                limit.get()
            )));
        }
        Ok(())
    }

    /// Consume the retained source certificate immediately before publication.
    ///
    /// The topology builder performs these checks while the live mesh is
    /// available. Rechecking the detached product prevents a publisher from
    /// silently dropping source intervals or split proofs.
    pub(crate) fn validate_certificate(&self) -> Result<()> {
        if self.provenance.len() != self.vertices.len()
            || self.coordinates.sites.len() != self.vertices.len()
            || self.face_uses.len() != self.faces.len()
        {
            return Err(GeometryErrorKind::triangulation(
                "constrained mesh certificate has inconsistent node or edge state",
            ));
        }
        let zero = exact::ExactRational::zero();
        let one = exact::ExactRational::one();
        for (site, provenance) in self.provenance.iter().enumerate() {
            match provenance {
                NodeProvenance::SourceSite | NodeProvenance::InteriorRefinement { .. } => {},
                NodeProvenance::ChartRestrictionSite => {
                    if self.coordinates.retained_site_location(site).is_some()
                        || !self.restrictions.iter().any(|restriction| {
                            restriction.start_site == site || restriction.end_site == site
                        })
                    {
                        return Err(GeometryErrorKind::triangulation(
                            "synthetic restriction site has no matching source-less chart use",
                        ));
                    }
                },
                NodeProvenance::ConstraintSplit { certificate } => {
                    let Some(certificate) = self.split_certificates.get(*certificate) else {
                        return Err(GeometryErrorKind::triangulation(
                            "constrained mesh certificate has a missing split proof",
                        ));
                    };
                    let constraint_count = self
                        .source_constraints
                        .len()
                        .checked_add(self.restrictions.len())
                        .ok_or(crate::error::ResourceError::SizeOverflow)?;
                    if certificate.constraint >= constraint_count
                        || certificate.parameter.try_cmp(&zero)? != std::cmp::Ordering::Greater
                        || certificate.parameter.try_cmp(&one)? != std::cmp::Ordering::Less
                    {
                        return Err(GeometryErrorKind::triangulation(
                            "constrained mesh certificate has an invalid split provenance",
                        ));
                    }
                    let point = self.vertices.get(site).ok_or_else(|| {
                        GeometryErrorKind::triangulation(
                            "constrained mesh certificate has a missing split vertex",
                        )
                    })?;
                    let expected_half_ulp = split_half_ulp_bounds(point.xy())?;
                    if point.xy() != certificate.point
                        || expected_half_ulp != certificate.half_ulp
                        || self.coordinates.site_xy(site) != certificate.point
                    {
                        return Err(GeometryErrorKind::triangulation(
                            "constrained mesh certificate has a mismatched split proof",
                        ));
                    }
                },
            }
            if let NodeProvenance::InteriorRefinement {
                parent_face,
                parent_sites,
                construction,
            } = provenance
            {
                if *parent_face >= self.face_slot_count
                    || parent_sites.iter().any(|&parent| parent >= site)
                {
                    return Err(GeometryErrorKind::triangulation(
                        "generated mesh site has an invalid parent recipe",
                    ));
                }
                let valid_construction = match construction {
                    RefinementConstruction::Circumcenter => true,
                    RefinementConstruction::EdgeMidpoint(edge)
                    | RefinementConstruction::LongestEdgeMidpoint(edge) => {
                        let [edge_start, edge_end] = edge;
                        edge_start != edge_end
                            && parent_sites.contains(edge_start)
                            && parent_sites.contains(edge_end)
                    },
                };
                if !valid_construction
                    || self
                        .vertices
                        .get(site)
                        .is_none_or(|point| point.xy() != self.coordinates.site_xy(site))
                {
                    return Err(GeometryErrorKind::triangulation(
                        "generated mesh site has an invalid construction recipe",
                    ));
                }
            }
        }
        let mut previous_source_root = None;
        for (id, source) in self.source_constraints.iter().enumerate() {
            let source_root_is_ordered =
                previous_source_root.is_none_or(|previous| source.source_root > previous);
            previous_source_root = Some(source.source_root);
            if source.start_occurrence >= self.coordinates.source_occurrences.len()
                || source.end_occurrence >= self.coordinates.source_occurrences.len()
            {
                return Err(GeometryErrorKind::triangulation(
                    "constrained mesh certificate has an absent source occurrence",
                ));
            }
            let source_start = self.coordinates.source_query(source.start_occurrence);
            let source_end = self.coordinates.source_query(source.end_occurrence);
            if source.id != id
                || !source_root_is_ordered
                || source.start_site == source.end_site
                || !source.start.xy().x.is_finite()
                || !source.start.xy().y.is_finite()
                || !source.end.xy().x.is_finite()
                || !source.end.xy().y.is_finite()
                || self.vertices.get(source.start_site).is_none()
                || self.vertices.get(source.end_site).is_none()
                || self.coordinates.key(source_start)
                    != self
                        .coordinates
                        .key(self.coordinates.site_query(source.start_site))
                || self.coordinates.key(source_end)
                    != self
                        .coordinates
                        .key(self.coordinates.site_query(source.end_site))
            {
                return Err(GeometryErrorKind::triangulation(
                    "constrained mesh certificate has an invalid source site or root",
                ));
            }
        }
        for (id, restriction) in self.restrictions.iter().enumerate() {
            let valid_role = match restriction.use_.restriction {
                ChartRestriction::CutChord => matches!(
                    (restriction.use_.start, restriction.use_.end),
                    (
                        SyntheticChartVertex::Cut {
                            side: start_side, ..
                        },
                        SyntheticChartVertex::Cut {
                            side: end_side, ..
                        }
                    ) if start_side == end_side
                ),
                ChartRestriction::PolarCap { pole } => matches!(
                    (restriction.use_.start, restriction.use_.end),
                    (
                        SyntheticChartVertex::Pole {
                            pole: start_pole, ..
                        },
                        SyntheticChartVertex::Pole {
                            pole: end_pole, ..
                        }
                    ) if start_pole == pole && end_pole == pole
                ),
            };
            if restriction.id != id
                || restriction.start_site == restriction.end_site
                || self.vertices.get(restriction.start_site).is_none()
                || self.vertices.get(restriction.end_site).is_none()
                || !valid_role
            {
                return Err(GeometryErrorKind::triangulation(
                    "constrained mesh certificate has an invalid chart restriction",
                ));
            }
        }
        let mut source_id = 0;
        let mut cursor = exact::ExactRational::zero();
        for piece in &self.constraint_pieces {
            if self.source_constraints.get(piece.source).is_none()
                || self.vertices.get(piece.start_site).is_none()
                || self.vertices.get(piece.end_site).is_none()
            {
                return Err(GeometryErrorKind::triangulation(
                    "constrained mesh certificate has an invalid source piece",
                ));
            }
            if piece.start_site == piece.end_site
                || piece.lo.try_cmp(&piece.hi)? != std::cmp::Ordering::Less
            {
                return Err(GeometryErrorKind::triangulation(
                    "constrained mesh certificate has an invalid source piece",
                ));
            }
            if piece.source != source_id {
                if source_id.checked_add(1) != Some(piece.source)
                    || cursor.try_cmp(&one)? != std::cmp::Ordering::Equal
                {
                    return Err(GeometryErrorKind::triangulation(
                        "constrained mesh certificate has a discontinuous source chain",
                    ));
                }
                source_id = piece.source;
                cursor = exact::ExactRational::zero();
            }
            if piece.lo.try_cmp(&cursor)? != std::cmp::Ordering::Equal {
                return Err(GeometryErrorKind::triangulation(
                    "constrained mesh certificate has an unordered source chain",
                ));
            }
            cursor = piece.hi.clone();
        }
        let source_count_matches = if self.constraint_pieces.is_empty() {
            self.source_constraints.is_empty()
        } else {
            source_id.checked_add(1) == Some(self.source_constraints.len())
        };
        if !source_count_matches
            || (!self.constraint_pieces.is_empty()
                && cursor.try_cmp(&one)? != std::cmp::Ordering::Equal)
        {
            return Err(GeometryErrorKind::triangulation(
                "constrained mesh certificate does not cover all sources",
            ));
        }
        let mut remaining_restrictions = self.restriction_pieces.as_slice();
        for restriction in &self.restrictions {
            let count = remaining_restrictions
                .iter()
                .take_while(|piece| piece.restriction == restriction.id)
                .count();
            let (pieces, rest) = remaining_restrictions.split_at(count);
            remaining_restrictions = rest;
            if pieces.is_empty() {
                return Err(GeometryErrorKind::triangulation(
                    "chart restriction has no retained directed pieces",
                ));
            }
            let mut cursor = exact::ExactRational::zero();
            let mut previous_vertex = restriction.use_.start;
            for piece in pieces {
                if self.vertices.get(piece.start_site).is_none()
                    || self.vertices.get(piece.end_site).is_none()
                    || piece.start_site == piece.end_site
                    || piece.start.try_cmp(&cursor)? != std::cmp::Ordering::Equal
                    || piece.start.try_cmp(&piece.end)? != std::cmp::Ordering::Less
                    || piece.use_.restriction != restriction.use_.restriction
                    || piece.use_.start != previous_vertex
                {
                    return Err(GeometryErrorKind::triangulation(
                        "constrained mesh certificate has a broken directed restriction chain",
                    ));
                }
                let valid_role = match piece.use_.restriction {
                    ChartRestriction::CutChord => matches!(
                        (piece.use_.start, piece.use_.end),
                        (
                            SyntheticChartVertex::Cut { side: start, .. },
                            SyntheticChartVertex::Cut { side: end, .. }
                        ) if start == end
                    ),
                    ChartRestriction::PolarCap { pole } => matches!(
                        (piece.use_.start, piece.use_.end),
                        (
                            SyntheticChartVertex::Pole { pole: start, .. },
                            SyntheticChartVertex::Pole { pole: end, .. }
                        ) if start == pole && end == pole
                    ),
                };
                if !valid_role {
                    return Err(GeometryErrorKind::triangulation(
                        "restriction piece has incompatible synthetic endpoint roles",
                    ));
                }
                cursor = piece.end.clone();
                previous_vertex = piece.use_.end;
            }
            if cursor.try_cmp(&one)? != std::cmp::Ordering::Equal
                || previous_vertex != restriction.use_.end
            {
                return Err(GeometryErrorKind::triangulation(
                    "constrained mesh certificate does not cover a chart restriction",
                ));
            }
        }
        if !remaining_restrictions.is_empty() {
            return Err(GeometryErrorKind::triangulation(
                "constrained mesh certificate contains an unknown chart restriction",
            ));
        }
        let mut constrained_piece_use_count =
            crate::collections::try_hash_map_with_capacity(self.constraint_pieces.len())?;
        let mut source_use_count = vec![0_usize; self.source_uses.len()];
        let mut restriction_piece_use_count =
            crate::collections::try_hash_map_with_capacity(self.restriction_pieces.len())?;
        for (sites, uses) in self.faces.iter().zip(&self.face_uses) {
            if uses.sides.len() != 3 {
                return Err(GeometryErrorKind::triangulation(
                    "selected constrained face has an incomplete side-use table",
                ));
            }
            for output_vertex in 0..3 {
                let selected_vertex = uses
                    .output_to_selected
                    .get(output_vertex)
                    .copied()
                    .map(usize::from);
                let output_site = sites.get(output_vertex).copied();
                if selected_vertex.and_then(|selected| uses.selected_sites.get(selected).copied())
                    != output_site
                {
                    return Err(GeometryErrorKind::triangulation(
                        "canonical constrained face lost its selected-site map",
                    ));
                }
            }
            for output_side in 0..3 {
                let Some(side) = uses.sides.get(output_side) else {
                    return Err(GeometryErrorKind::triangulation(
                        "selected constrained face has a missing directed side",
                    ));
                };
                let start = sites.get(output_side).copied();
                let end = sites.get(next_triangle_side(output_side)).copied();
                let live_edge = HalfedgeId::from_index(side.halfedge);
                let twin_in_range = side
                    .twin
                    .is_none_or(|twin| twin < self.face_slot_count.saturating_mul(3));
                if start != Some(side.start_site)
                    || end != Some(side.end_site)
                    || uses.live_face >= self.face_slot_count
                    || live_edge.face().index() != uses.live_face
                    || side.twin_selected && side.twin.is_none()
                    || !twin_in_range
                {
                    return Err(GeometryErrorKind::triangulation(
                        "selected constrained side has an invalid live-use receipt",
                    ));
                }
                match &side.boundary {
                    Some(CdtSideBoundaryUse::Source(source_use_index)) => {
                        let Some(source_use) = self.source_uses.get(*source_use_index) else {
                            return Err(GeometryErrorKind::triangulation(
                                "constrained side references an absent source-use record",
                            ));
                        };
                        let Some(source) = self.source_constraints.get(source_use.source) else {
                            return Err(GeometryErrorKind::triangulation(
                                "constrained side references an absent source root",
                            ));
                        };
                        if source_ring_owner(source.id, &self.source_rings)
                            != Some(source_use.owner)
                        {
                            return Err(GeometryErrorKind::triangulation(
                                "constrained side has a mismatched polygon-ring owner",
                            ));
                        }
                        let matched_piece =
                            self.constraint_pieces
                                .iter()
                                .enumerate()
                                .find(|(_, piece)| {
                                    piece.source == source_use.source
                                        && ordered(piece.start_site, piece.end_site)
                                            == ordered(side.start_site, side.end_site)
                                });
                        let Some((piece_index, piece)) = matched_piece else {
                            return Err(GeometryErrorKind::triangulation(
                                "constrained side has no matching directed source interval",
                            ));
                        };
                        let (expected_start, expected_end) = if source_use.forward {
                            (&piece.lo, &piece.hi)
                        } else {
                            (&piece.hi, &piece.lo)
                        };
                        if source_use.forward
                            != ((piece.start_site, piece.end_site)
                                == (side.start_site, side.end_site))
                            || source_use.start.try_cmp(expected_start)? != Ordering::Equal
                            || source_use.end.try_cmp(expected_end)? != Ordering::Equal
                        {
                            return Err(GeometryErrorKind::triangulation(
                                "constrained side has a reversed or crosswired source interval",
                            ));
                        }
                        let count = constrained_piece_use_count
                            .entry(piece_index)
                            .or_insert(0_usize);
                        *count = count
                            .checked_add(1)
                            .ok_or(crate::error::ResourceError::SizeOverflow)?;
                        let count =
                            source_use_count.get_mut(*source_use_index).ok_or_else(|| {
                                GeometryErrorKind::triangulation(
                                    "source-use index is outside its prepared table",
                                )
                            })?;
                        *count = count
                            .checked_add(1)
                            .ok_or(crate::error::ResourceError::SizeOverflow)?;
                    },
                    Some(CdtSideBoundaryUse::Restriction(piece_index)) => {
                        let Some(piece) = self.restriction_pieces.get(*piece_index) else {
                            return Err(GeometryErrorKind::triangulation(
                                "constrained side references an absent chart-restriction piece",
                            ));
                        };
                        let Some(restriction) = self.restrictions.get(piece.restriction) else {
                            return Err(GeometryErrorKind::triangulation(
                                "chart-restriction piece references an absent root",
                            ));
                        };
                        if piece.owner != restriction.owner
                            || ordered(piece.start_site, piece.end_site)
                                != ordered(side.start_site, side.end_site)
                            || piece.use_.restriction != restriction.use_.restriction
                        {
                            return Err(GeometryErrorKind::triangulation(
                                "constrained side has a crosswired chart-restriction use",
                            ));
                        }
                        let count = restriction_piece_use_count
                            .entry(*piece_index)
                            .or_insert(0_usize);
                        *count = count
                            .checked_add(1)
                            .ok_or(crate::error::ResourceError::SizeOverflow)?;
                    },
                    None => {},
                }
            }
        }
        if constrained_piece_use_count.len() != self.constraint_pieces.len()
            || constrained_piece_use_count
                .values()
                .any(|&count| count != 1)
        {
            return Err(GeometryErrorKind::triangulation(
                "source constraint piece does not have exactly one selected face use",
            ));
        }
        if source_use_count.len() != self.constraint_pieces.len()
            || source_use_count.iter().any(|&count| count != 1)
        {
            return Err(GeometryErrorKind::triangulation(
                "source-use record does not have exactly one selected face use",
            ));
        }
        if restriction_piece_use_count.len() != self.restriction_pieces.len()
            || restriction_piece_use_count
                .values()
                .any(|&count| count != 1)
        {
            return Err(GeometryErrorKind::triangulation(
                "chart restriction piece does not have exactly one selected face use",
            ));
        }
        let live_edge_count = self
            .face_slot_count
            .checked_mul(3)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        let side_capacity = self
            .face_uses
            .len()
            .checked_mul(3)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        let mut selected_uses = crate::collections::try_hash_map_with_capacity(side_capacity)?;
        for (face_index, uses) in self.face_uses.iter().enumerate() {
            for (side_index, side) in uses.sides.iter().enumerate() {
                if side.halfedge >= live_edge_count
                    || selected_uses
                        .insert(side.halfedge, (face_index, side_index))
                        .is_some()
                {
                    return Err(GeometryErrorKind::triangulation(
                        "selected constrained face repeats or misidentifies a live halfedge",
                    ));
                }
            }
        }
        for uses in &self.face_uses {
            for side in &uses.sides {
                let expected_twin_selected = side
                    .twin
                    .is_some_and(|twin| selected_uses.contains_key(&twin));
                if side.twin_selected != expected_twin_selected {
                    return Err(GeometryErrorKind::triangulation(
                        "selected constrained side has a mismatched twin-selection fact",
                    ));
                }
                if side.twin_selected {
                    let twin = side.twin.expect("selected twins were checked above");
                    let reciprocal = selected_uses
                        .get(&twin)
                        .and_then(|&(face_index, side_index)| {
                            self.face_uses
                                .get(face_index)
                                .and_then(|face| face.sides.get(side_index))
                        })
                        .is_some_and(|other| {
                            other.twin == Some(side.halfedge)
                                && other.start_site == side.end_site
                                && other.end_site == side.start_site
                                && other.twin_selected
                        });
                    if !reciprocal {
                        return Err(GeometryErrorKind::triangulation(
                            "selected twin uses do not identify each other",
                        ));
                    }
                }
            }
        }
        Ok(())
    }
}

impl ConstraintPieces {
    pub(super) fn new(constraint_count: usize) -> Result<Self> {
        Ok(Self {
            edges: crate::collections::try_hash_map_with_capacity(constraint_count)?,
            nodes: std::iter::repeat_with(|| PieceNode {
                piece: None,
                next: None,
            })
            .take(constraint_count)
            .collect(),
        })
    }

    pub(super) fn piece(&self, index: usize) -> &ConstraintPiece {
        self.nodes
            .get(index)
            .and_then(|node| node.piece.as_ref())
            .expect("the edge index names a recorded constraint piece")
    }

    pub(super) fn get(&self, edge: (usize, usize)) -> Option<&ConstraintPiece> {
        self.edges.get(&edge).map(|&index| self.piece(index))
    }

    pub(super) fn values(&self) -> impl Iterator<Item = &ConstraintPiece> {
        self.edges.values().map(|&index| self.piece(index))
    }

    pub(super) fn reserve(&mut self, additional: usize) {
        self.edges.reserve(additional);
        self.nodes.reserve(additional);
    }

    pub(super) fn remove(&mut self, edge: (usize, usize)) -> Option<PieceCursor> {
        self.edges.remove(&edge).map(PieceCursor::Replace)
    }

    pub(super) fn record(&mut self, piece: ConstraintPiece, cursor: &mut PieceCursor) {
        let entry = self.edges.entry(ordered(piece.start_site, piece.end_site));
        if let std::collections::hash_map::Entry::Vacant(entry) = entry {
            let index = match *cursor {
                PieceCursor::Replace(index) => {
                    self.nodes
                        .get_mut(index)
                        .expect("replacement uses an admitted constraint slot")
                        .piece = Some(piece);
                    index
                },
                PieceCursor::After(previous) => {
                    let next = self
                        .nodes
                        .get(previous)
                        .expect("the insertion cursor names an admitted constraint slot")
                        .next;
                    let index = self.nodes.len();
                    self.nodes.push(PieceNode {
                        piece: Some(piece),
                        next,
                    });
                    self.nodes
                        .get_mut(previous)
                        .expect("the insertion cursor names an admitted constraint slot")
                        .next = Some(index);
                    index
                },
            };
            entry.insert(index);
            *cursor = PieceCursor::After(index);
        }
    }

    #[cfg_attr(
        test,
        expect(
            clippy::panic_in_result_fn,
            reason = "test-only assertions characterize source-chain order against the sorted oracle"
        )
    )]
    pub(super) fn take_ordered(
        &mut self,
        sources: &[SourceConstraint],
        restrictions: &[RestrictionConstraint],
        source_rings: &[SourceRingStart],
        coordinates: &CdtCoordinates,
    ) -> Result<Vec<ConstraintPiece>> {
        let root_count = sources
            .len()
            .checked_add(restrictions.len())
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        #[cfg(test)]
        let expected = {
            let mut pieces: Vec<_> = self.values().cloned().collect();
            try_sort_by(&mut pieces, compare_constraint_pieces)?;
            pieces
        };
        let mut pieces = Vec::with_capacity(self.edges.len());
        for constraint in 0..root_count {
            let mut at = Some(constraint);
            while let Some(index) = at {
                let node = self
                    .nodes
                    .get_mut(index)
                    .expect("constraint successors name admitted piece slots");
                let Some(piece) = node.piece.take() else {
                    if constraint >= sources.len() {
                        let restriction = constraint
                            .checked_sub(sources.len())
                            .ok_or(crate::error::ResourceError::SizeOverflow)?;
                        return Err(GeometryErrorKind::triangulation(format!(
                            "chart restriction {restriction} has no emitted boundary chain"
                        )));
                    }
                    let missing = sources.get(constraint).expect("source IDs are ordered");
                    let mut overlap = None;
                    for other in sources {
                        if other.id != missing.id
                            && source_constraints_overlap(coordinates, missing, other)?
                        {
                            overlap = Some(other);
                            break;
                        }
                    }
                    if let Some(overlap) = overlap {
                        let left = source_ring_owner(missing.id, source_rings)
                            .expect("a source constraint belongs to a source ring");
                        let right = source_ring_owner(overlap.id, source_rings)
                            .expect("an overlapping source constraint belongs to a source ring");
                        return Err(super::super::overlapping_rings_error(left, right));
                    }
                    return Err(GeometryErrorKind::triangulation(format!(
                        "source constraint {constraint} has no emitted boundary chain"
                    )));
                };
                pieces.push(piece);
                at = node.next;
            }
        }
        debug_assert_eq!(pieces.len(), self.edges.len());
        #[cfg(test)]
        {
            assert_eq!(pieces.len(), expected.len());
            for (actual, expected) in pieces.iter().zip(&expected) {
                assert_eq!(
                    compare_constraint_pieces(actual, expected)?,
                    std::cmp::Ordering::Equal
                );
            }
        }
        self.edges.clear();
        self.nodes.clear();
        Ok(pieces)
    }
}

pub(super) fn exact_source_coordinate(
    start: &exact::ExactDyadic,
    end: &exact::ExactDyadic,
    parameter: &exact::ExactRational,
) -> Result<exact::ExactRational> {
    let start = exact::ExactRational::from_dyadic(start.clone());
    let delta = exact::ExactRational::from_dyadic(end.clone()).try_subtract(&start)?;
    start.try_add(&delta.try_product(parameter)?)
}

pub(super) fn round_source_coordinate(value: &exact::ExactRational) -> Result<f64> {
    match value.round() {
        Ok(value) => Ok(value),
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::Geometry(GeometryErrorKind::ExactCoordinateOutOfRange)
            ) =>
        {
            Err(GeometryErrorKind::unrepresentable_output("triangulate"))
        },
        Err(error) => Err(error),
    }
}

fn split_half_ulp(value: f64) -> Result<exact::ExactDyadic> {
    if !value.is_finite() {
        return Err(GeometryErrorKind::unrepresentable_output("triangulate"));
    }
    let next = value.next_up();
    let previous = value.next_down();
    let up_gap = if next.is_finite() { next - value } else { 0.0 };
    let down_gap = if previous.is_finite() {
        value - previous
    } else {
        0.0
    };
    let gap = up_gap.max(down_gap);
    exact::ExactDyadic::from_f64(gap).try_product(&exact::ExactDyadic::from_f64(0.5))
}

pub(super) fn split_half_ulp_bounds(point: XY) -> Result<[exact::ExactDyadic; 2]> {
    Ok([split_half_ulp(point.x)?, split_half_ulp(point.y)?])
}

fn restriction_use_for_piece(
    restriction: &RestrictionConstraint,
    start_site: usize,
    end_site: usize,
    coordinates: &CdtCoordinates,
) -> Result<DirectedChartRestrictionUse> {
    let synthetic_vertex = |site| -> Result<SyntheticChartVertex> {
        if site == restriction.start_site {
            return Ok(restriction.use_.start);
        }
        if site == restriction.end_site {
            return Ok(restriction.use_.end);
        }
        Ok(match restriction.use_.restriction {
            ChartRestriction::CutChord => {
                let side = match (restriction.use_.start, restriction.use_.end) {
                    (
                        SyntheticChartVertex::Cut { side: start, .. },
                        SyntheticChartVertex::Cut { side: end, .. },
                    ) if start == end => start,
                    _ => {
                        return Err(GeometryErrorKind::triangulation(
                            "cut restriction has incompatible endpoint aliases",
                        ));
                    },
                };
                SyntheticChartVertex::Cut {
                    node: u32::try_from(site)
                        .map_err(|_| crate::error::ResourceError::SizeOverflow)?,
                    side,
                }
            },
            ChartRestriction::PolarCap { pole } => SyntheticChartVertex::Pole {
                node: u32::try_from(site).map_err(|_| crate::error::ResourceError::SizeOverflow)?,
                pole,
                native_longitude_bits: coordinates.site_xy(site).x.to_bits(),
            },
        })
    };
    Ok(DirectedChartRestrictionUse {
        restriction: restriction.use_.restriction,
        start: synthetic_vertex(start_site)?,
        end: synthetic_vertex(end_site)?,
    })
}

pub(super) fn split_deviation_within(
    coordinates: &CdtCoordinates,
    start: CdtQuery,
    end: CdtQuery,
    parameter: &exact::ExactRational,
    point: CdtQuery,
    half_ulp: &[exact::ExactDyadic; 2],
) -> Result<bool> {
    let [half_ulp_x, half_ulp_y] = half_ulp;
    let [start_x, start_y] = coordinates.exact_xy(start);
    let [end_x, end_y] = coordinates.exact_xy(end);
    let [actual_x, actual_y] = coordinates.exact_xy(point);
    let exact_x = exact_source_coordinate(&start_x, &end_x, parameter)?;
    let exact_y = exact_source_coordinate(&start_y, &end_y, parameter)?;
    let error_x = exact::ExactRational::from_dyadic(actual_x).try_subtract(&exact_x)?;
    let error_y = exact::ExactRational::from_dyadic(actual_y).try_subtract(&exact_y)?;
    let zero = exact::ExactRational::zero();
    let abs_x = if error_x.sign() == ExactSign::Negative {
        zero.try_subtract(&error_x)?
    } else {
        error_x
    };
    let abs_y = if error_y.sign() == ExactSign::Negative {
        zero.try_subtract(&error_y)?
    } else {
        error_y
    };
    Ok(
        abs_x.try_cmp(&exact::ExactRational::from_dyadic(half_ulp_x.clone()))?
            != std::cmp::Ordering::Greater
            && abs_y.try_cmp(&exact::ExactRational::from_dyadic(half_ulp_y.clone()))?
                != std::cmp::Ordering::Greater,
    )
}

fn validate_piece_endpoint(
    coordinates: &CdtCoordinates,
    constraint: usize,
    start_site: usize,
    end_site: usize,
    parameter: &exact::ExactRational,
    point: CdtQuery,
    site: usize,
    provenance: &[NodeProvenance],
    certificates: &[CertifiedBoundarySplit],
) -> Result<()> {
    let point_xy = coordinates.proposal_xy(point);
    match provenance.get(site) {
        Some(NodeProvenance::ConstraintSplit { certificate }) => {
            let Some(certificate) = certificates.get(*certificate) else {
                return Err(GeometryErrorKind::triangulation(
                    "constraint references a missing rounded split proof",
                ));
            };
            if certificate.constraint != constraint
                || certificate.parameter.try_cmp(parameter)? != std::cmp::Ordering::Equal
                || certificate.point != point_xy
            {
                return Err(GeometryErrorKind::triangulation(
                    "constraint interval does not match its rounded split",
                ));
            }
        },
        Some(
            NodeProvenance::SourceSite
            | NodeProvenance::ChartRestrictionSite
            | NodeProvenance::InteriorRefinement { .. },
        ) => {
            let start = coordinates.site_query(start_site);
            let end = coordinates.site_query(end_site);
            if coordinates.orient([start, end, point])? != ExactSign::Zero {
                return Err(GeometryErrorKind::triangulation(
                    "constraint has an off-line emitted vertex",
                ));
            }
            let actual = coordinates.interpolate_source_parameter(
                &exact::ExactRational::zero(),
                &exact::ExactRational::one(),
                start,
                end,
                point,
            )?;
            if actual.try_cmp(parameter)? != std::cmp::Ordering::Equal {
                return Err(GeometryErrorKind::triangulation(
                    "source constraint interval does not match its emitted vertex",
                ));
            }
        },
        None => {
            return Err(GeometryErrorKind::triangulation(
                "source constraint references missing provenance",
            ));
        },
    }
    Ok(())
}

#[cfg(test)]
fn compare_constraint_pieces(
    left: &ConstraintPiece,
    right: &ConstraintPiece,
) -> Result<std::cmp::Ordering> {
    let constraint = left.constraint.cmp(&right.constraint);
    if constraint.is_ne() {
        return Ok(constraint);
    }
    let lo = left.lo.try_cmp(&right.lo)?;
    if lo.is_ne() {
        return Ok(lo);
    }
    let hi = left.hi.try_cmp(&right.hi)?;
    if hi.is_ne() {
        return Ok(hi);
    }
    Ok(left
        .start_site
        .cmp(&right.start_site)
        .then_with(|| left.end_site.cmp(&right.end_site)))
}

fn source_constraints_overlap(
    coordinates: &CdtCoordinates,
    left: &SourceConstraint,
    right: &SourceConstraint,
) -> Result<bool> {
    let (a, b, c, d) = (
        coordinates.source_query(left.start_occurrence),
        coordinates.source_query(left.end_occurrence),
        coordinates.source_query(right.start_occurrence),
        coordinates.source_query(right.end_occurrence),
    );
    Ok(coordinates.orient([a, b, c])? == ExactSign::Zero
        && coordinates.orient([a, b, d])? == ExactSign::Zero
        && (coordinates.between(a, b, c)
            || coordinates.between(a, b, d)
            || coordinates.between(c, d, a)
            || coordinates.between(c, d, b)))
}

impl ConstrainedMesh {
    pub(in crate::geometry::tessellation::delaunay) fn constraint_sites(
        &self,
        constraint: usize,
    ) -> Result<(usize, usize)> {
        let source_count = self.source_constraints.len();
        if let Some(source) = self.source_constraints.get(constraint) {
            return Ok((source.start_site, source.end_site));
        }
        self.restrictions
            .get(constraint.checked_sub(source_count).ok_or_else(|| {
                GeometryErrorKind::triangulation("constraint ID is outside its root table")
            })?)
            .map(|restriction| (restriction.start_site, restriction.end_site))
            .ok_or_else(|| {
                GeometryErrorKind::triangulation("constraint ID is outside its root table")
            })
    }

    fn constrained_pairs(&self) -> Result<HashSet<(usize, usize)>> {
        #[cfg(test)]
        update_work(|counters| {
            counters.constraint_reconstruction_passes =
                counters.constraint_reconstruction_passes.saturating_add(1);
        });
        let mut pairs = crate::collections::try_hash_set_with_capacity(self.mesh.edge_count())?;
        for edge in self.mesh.edge_ids() {
            if self.is_constrained(edge) && self.mesh.alive(edge.face()) {
                let (start, end) = self.mesh.edge_vertices(edge);
                pairs.insert(ordered(start, end));
            }
        }
        Ok(pairs)
    }

    fn validate_constraint_chains(
        &self,
        points: &[Point],
        pieces: &[ConstraintPiece],
    ) -> Result<()> {
        let one = exact::ExactRational::one();
        let constrained = self.constrained_pairs()?;
        let mut remaining = pieces;
        let root_count = self
            .source_constraints
            .len()
            .checked_add(self.restrictions.len())
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        for constraint in 0..root_count {
            let count = remaining
                .iter()
                .take_while(|piece| {
                    #[cfg(test)]
                    update_work(|work| {
                        work.source_piece_visits = work.source_piece_visits.saturating_add(1);
                    });
                    piece.constraint == constraint
                })
                .count();
            let (pieces, rest) = remaining.split_at(count);
            remaining = rest;
            if pieces.is_empty() {
                let label = if constraint < self.source_constraints.len() {
                    format!("source constraint {constraint}")
                } else {
                    let restriction = constraint
                        .checked_sub(self.source_constraints.len())
                        .ok_or(crate::error::ResourceError::SizeOverflow)?;
                    format!("chart restriction {restriction}")
                };
                return Err(GeometryErrorKind::triangulation(format!(
                    "{label} has no ordered boundary pieces"
                )));
            }
            let (root_start, root_end) = self.constraint_sites(constraint)?;
            let mut cursor = exact::ExactRational::zero();
            for piece in pieces {
                if piece.lo.try_cmp(&cursor)? != std::cmp::Ordering::Equal
                    || piece.lo.try_cmp(&piece.hi)? != std::cmp::Ordering::Less
                {
                    return Err(GeometryErrorKind::triangulation(format!(
                        "constraint root {constraint} has a non-ordered parameter chain"
                    )));
                }
                if points.get(piece.start_site).is_none() {
                    return Err(GeometryErrorKind::triangulation(
                        "source constraint references a missing start site",
                    ));
                }
                if points.get(piece.end_site).is_none() {
                    return Err(GeometryErrorKind::triangulation(
                        "source constraint references a missing end site",
                    ));
                }
                validate_piece_endpoint(
                    &self.coordinates,
                    constraint,
                    root_start,
                    root_end,
                    &piece.lo,
                    self.coordinates.site_query(piece.start_site),
                    piece.start_site,
                    &self.provenance,
                    &self.split_certificates,
                )?;
                validate_piece_endpoint(
                    &self.coordinates,
                    constraint,
                    root_start,
                    root_end,
                    &piece.hi,
                    self.coordinates.site_query(piece.end_site),
                    piece.end_site,
                    &self.provenance,
                    &self.split_certificates,
                )?;
                if !constrained.contains(&ordered(piece.start_site, piece.end_site)) {
                    return Err(GeometryErrorKind::triangulation(format!(
                        "constraint root {constraint} does not match its emitted edge"
                    )));
                }
                cursor = piece.hi.clone();
            }
            if cursor.try_cmp(&one)? != std::cmp::Ordering::Equal {
                return Err(GeometryErrorKind::triangulation(format!(
                    "constraint root {constraint} does not cover its parameter interval"
                )));
            }
        }
        if !remaining.is_empty() {
            return Err(GeometryErrorKind::triangulation(
                "constraint pieces contain an unknown root",
            ));
        }
        Ok(())
    }

    pub(in crate::geometry::tessellation::delaunay) fn finish_plan(
        mut self,
        points: Vec<Point>,
        refinement: super::CdtRefinement,
        force_xy: bool,
    ) -> Result<ConstrainedMeshPlan> {
        let constraint_pieces = self.constraint_pieces.take_ordered(
            &self.source_constraints,
            &self.restrictions,
            &self.source_rings,
            &self.coordinates,
        )?;
        self.validate_constraint_chains(&points, &constraint_pieces)?;
        let inside = self.inside_faces();
        let face_slot_count = self.mesh.triangle_count();
        let mut selected = Vec::with_capacity(self.mesh.triangle_count());
        for (face, &kept) in self.mesh.face_ids().zip(&inside) {
            if kept && self.mesh.alive(face) {
                selected.push((face, self.mesh.triangle(face)));
            }
        }
        let triangles = selected.iter().map(|(_, sites)| *sites).collect::<Vec<_>>();
        let canonical =
            super::exact_delaunay::canonical_triangle_order(&self.coordinates, &triangles);
        let mut faces = Vec::with_capacity(canonical.len());
        let mut face_uses = Vec::with_capacity(canonical.len());
        let mut source_pieces = Vec::with_capacity(self.source_constraints.len());
        let mut source_uses = Vec::with_capacity(self.source_constraints.len());
        let mut restriction_pieces = Vec::with_capacity(self.restrictions.len());
        let mut piece_by_edge =
            crate::collections::try_hash_map_with_capacity(constraint_pieces.len())?;
        for (piece_index, piece) in constraint_pieces.iter().enumerate() {
            if piece_by_edge
                .insert(ordered(piece.start_site, piece.end_site), piece_index)
                .is_some()
            {
                return Err(GeometryErrorKind::triangulation(
                    "two constraint roots claim one mesh edge",
                ));
            }
            if piece.constraint < self.source_constraints.len() {
                source_pieces.push(SourcePiece {
                    source: piece.constraint,
                    start_site: piece.start_site,
                    end_site: piece.end_site,
                    lo: piece.lo.clone(),
                    hi: piece.hi.clone(),
                });
            } else {
                let restriction_id = piece
                    .constraint
                    .checked_sub(self.source_constraints.len())
                    .ok_or_else(|| {
                        GeometryErrorKind::triangulation(
                            "constraint piece has an invalid restriction root",
                        )
                    })?;
                let restriction = self.restrictions.get(restriction_id).ok_or_else(|| {
                    GeometryErrorKind::triangulation(
                        "constraint piece references a missing chart restriction",
                    )
                })?;
                restriction_pieces.push(RestrictionPiece {
                    restriction: restriction_id,
                    owner: restriction.owner,
                    start_site: piece.start_site,
                    end_site: piece.end_site,
                    start: piece.lo.clone(),
                    end: piece.hi.clone(),
                    use_: restriction_use_for_piece(
                        restriction,
                        piece.start_site,
                        piece.end_site,
                        &self.coordinates,
                    )?,
                });
            }
        }
        let source_piece_count = source_pieces.len();
        for canonical_face in canonical {
            let (live_face, selected_sites) =
                *selected.get(canonical_face.selected_face).ok_or_else(|| {
                    GeometryErrorKind::triangulation(
                        "canonical constrained face lost its live mesh identity",
                    )
                })?;
            let output_sites = canonical_face.output_sites;
            let mut sides = Vec::with_capacity(3);
            for output_side in 0..3 {
                let selected_site_index = canonical_face
                    .output_to_selected
                    .get(output_side)
                    .copied()
                    .ok_or_else(|| {
                        GeometryErrorKind::triangulation(
                            "canonical constrained side has no source map",
                        )
                    })?;
                let selected_side = usize::from(selected_site_index);
                let side = *FaceSide::ALL.get(selected_side).ok_or_else(|| {
                    GeometryErrorKind::triangulation(
                        "canonical constrained side has an invalid source map",
                    )
                })?;
                let halfedge = HalfedgeId::in_face(live_face, side);
                let (start_site, end_site) = self.mesh.edge_vertices(halfedge);
                let expected_start = output_sites.get(output_side).copied().ok_or_else(|| {
                    GeometryErrorKind::triangulation(
                        "canonical constrained face has a missing output site",
                    )
                })?;
                let expected_end = output_sites
                    .get(next_triangle_side(output_side))
                    .copied()
                    .ok_or_else(|| {
                        GeometryErrorKind::triangulation(
                            "canonical constrained face has a missing output side",
                        )
                    })?;
                if (start_site, end_site) != (expected_start, expected_end) {
                    return Err(GeometryErrorKind::triangulation(
                        "canonical constrained side does not match its live halfedge",
                    ));
                }
                let twin_index = self.mesh.twin(halfedge);
                let twin = (twin_index != EMPTY).then(|| HalfedgeId::from_index(twin_index));
                let twin_selected = twin.is_some_and(|twin| {
                    self.mesh.alive(twin.face())
                        && inside.get(twin.face().index()).copied().unwrap_or(false)
                });
                let constrained = self.is_constrained(halfedge);
                let constraint_piece_index =
                    piece_by_edge.get(&ordered(start_site, end_site)).copied();
                let boundary = if let Some(piece_index) = constraint_piece_index {
                    let piece = constraint_pieces.get(piece_index).ok_or_else(|| {
                        GeometryErrorKind::triangulation(
                            "live constrained side references a missing root piece",
                        )
                    })?;
                    if piece.constraint < self.source_constraints.len() {
                        let source = piece.constraint;
                        let forward = (piece.start_site, piece.end_site) == (start_site, end_site);
                        let owner =
                            source_ring_owner(source, &self.source_rings).ok_or_else(|| {
                                GeometryErrorKind::triangulation(
                                    "source piece has no polygon-ring owner",
                                )
                            })?;
                        let (start, end) = if forward {
                            (piece.lo.clone(), piece.hi.clone())
                        } else {
                            (piece.hi.clone(), piece.lo.clone())
                        };
                        let source_use_index = source_uses.len();
                        source_uses.push(CdtSourceUse {
                            source,
                            owner,
                            start,
                            end,
                            forward,
                        });
                        Some(CdtSideBoundaryUse::Source(source_use_index))
                    } else {
                        let restriction_piece = piece_index
                            .checked_sub(source_piece_count)
                            .filter(|&index| index < restriction_pieces.len())
                            .ok_or_else(|| {
                                GeometryErrorKind::triangulation(
                                    "restricted side has no prepared directed use",
                                )
                            })?;
                        Some(CdtSideBoundaryUse::Restriction(restriction_piece))
                    }
                } else {
                    None
                };
                if constrained != boundary.is_some() {
                    return Err(GeometryErrorKind::triangulation(
                        "live constrained side has no matching source or chart-restriction use",
                    ));
                }
                sides.push(CdtSideUse {
                    halfedge: halfedge.index(),
                    start_site,
                    end_site,
                    twin: twin.map(HalfedgeId::index),
                    twin_selected,
                    boundary,
                });
            }
            faces.push(output_sites);
            face_uses.push(CdtFaceUse {
                live_face: live_face.index(),
                selected_sites,
                output_to_selected: canonical_face.output_to_selected,
                sides,
            });
        }
        Ok(ConstrainedMeshPlan {
            vertices: points,
            faces,
            face_uses,
            coordinates: self.coordinates,
            face_slot_count,
            provenance: self.provenance,
            source_constraints: self.source_constraints,
            source_uses,
            restrictions: self.restrictions,
            source_rings: self.source_rings,
            constraint_pieces: source_pieces,
            restriction_pieces,
            split_certificates: self.split_certificates,
            refinement,
            force_xy,
        })
    }
}
