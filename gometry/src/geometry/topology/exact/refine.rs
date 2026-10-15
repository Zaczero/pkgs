use std::cmp::Ordering;

use super::events::{ContactPoint, exact_piece_contacts, insert_cut_event};
use super::{
    CutEvent, ExactChartPiece, ExactChartPoint, ExactGeographicChartInventory, ExactQuotientKey,
    FallibleAvl, parameter_on_exact_segment, point_on_exact_segment, quotient_key,
    source_chart_point,
};
use crate::error::{ResourceError, Result};
use crate::geometry::segments::ordering::try_sort_by_with_work;
use crate::geometry::tessellation::exact::ExactRational;
use crate::geometry::{
    Bounds, ExpansionBudget, GeometryErrorKind, Segment, for_each_overlapping_bounds_cross_pair,
};
use crate::work::Work;

impl ExactGeographicChartInventory {
    /// Noding is planar inside each chart strip; only pieces incident to
    /// opposite seam lifts can acquire new contacts after quotienting.
    #[expect(
        clippy::indexing_slicing,
        reason = "seam candidate ordinals index the parallel proxy-ID columns built by this sweep"
    )]
    pub(in crate::geometry::topology::exact) fn refine_seam_aliases<W: Work>(
        &self,
        cuts: &mut FallibleAvl<CutEvent>,
        constructed_nodes: &mut FallibleAvl<ExactQuotientKey>,
        compare_cuts: &mut impl FnMut(&CutEvent, &CutEvent, &mut W) -> Result<Ordering>,
        compare_keys: &mut impl FnMut(&ExactQuotientKey, &ExactQuotientKey, &mut W) -> Result<Ordering>,
        budget: &mut ExpansionBudget,
        work: &mut W,
    ) -> Result<()> {
        let axis = self.frame.longitude();
        let half = ExactRational::from_f64(axis.half_period());
        let negative_half = ExactRational::from_f64(-axis.half_period());
        let period = ExactRational::from_f64(axis.period());
        let touches = |piece: &ExactChartPiece, seam: &ExactRational| -> Result<bool> {
            Ok(piece.start.x.try_cmp(seam)?.is_eq() || piece.end.x.try_cmp(seam)?.is_eq())
        };
        let mut positive_proxy_ids = Vec::new();
        let mut positive_bounds = Vec::new();
        let mut positive_unproxyable = Vec::new();
        let mut negative_ids = Vec::new();
        let mut negative_proxy_ids = Vec::new();
        let mut negative_bounds = Vec::new();
        let mut negative_unproxyable = Vec::new();
        for (index, piece) in self.pieces.iter().enumerate() {
            work.charge_into_error(1)?;
            let positive = touches(piece, &half)?;
            let negative = touches(piece, &negative_half)?;
            if positive {
                if let Some(bounds) = piece.outward_bounds()? {
                    positive_proxy_ids.push(index);
                    positive_bounds.push(bounds);
                } else {
                    positive_unproxyable.push(index);
                }
            }
            if negative {
                negative_ids.push(index);
                if let Some(bounds) = piece.shifted_x(&period)?.outward_bounds()? {
                    negative_proxy_ids.push(index);
                    negative_bounds.push(bounds);
                } else {
                    negative_unproxyable.push(index);
                }
            }
        }
        if !positive_bounds.is_empty() && !negative_bounds.is_empty() {
            let mut failure = None;
            let candidates = for_each_overlapping_bounds_cross_pair(
                &positive_bounds,
                &negative_bounds,
                |left, right| {
                    if let Err(error) = work.charge_into_error(1) {
                        failure = Some(error);
                        return std::ops::ControlFlow::Break(());
                    }
                    let left_piece = positive_proxy_ids[left];
                    let right_piece = negative_proxy_ids[right];
                    if left_piece == right_piece {
                        return std::ops::ControlFlow::Continue(());
                    }
                    match self.refine_piece_pair(
                        [left_piece, right_piece],
                        Some(&period),
                        cuts,
                        constructed_nodes,
                        compare_cuts,
                        compare_keys,
                        budget,
                        work,
                    ) {
                        Ok(()) => std::ops::ControlFlow::Continue(()),
                        Err(error) => {
                            failure = Some(error);
                            std::ops::ControlFlow::Break(())
                        },
                    }
                },
            )?;
            if candidates.is_break() {
                return Err(failure.expect("a failed seam pair stores its first error"));
            }
        }
        for &left in &positive_unproxyable {
            for &right in &negative_ids {
                work.charge_into_error(1)?;
                if left != right {
                    self.refine_piece_pair(
                        [left, right],
                        Some(&period),
                        cuts,
                        constructed_nodes,
                        compare_cuts,
                        compare_keys,
                        budget,
                        work,
                    )?;
                }
            }
        }
        for &left in &positive_proxy_ids {
            for &right in &negative_unproxyable {
                work.charge_into_error(1)?;
                if left != right {
                    self.refine_piece_pair(
                        [left, right],
                        Some(&period),
                        cuts,
                        constructed_nodes,
                        compare_cuts,
                        compare_keys,
                        budget,
                        work,
                    )?;
                }
            }
        }
        Ok(())
    }

    pub(in crate::geometry::topology::exact) fn refine_source_points<W: Work>(
        &self,
        cuts: &mut FallibleAvl<CutEvent>,
        compare_cuts: &mut impl FnMut(&CutEvent, &CutEvent, &mut W) -> Result<Ordering>,
        proxy_piece_ids: &[usize],
        proxies: &[Segment],
        proxyable: &[bool],
        work: &mut W,
    ) -> Result<()> {
        if self.points.is_empty() || self.pieces.is_empty() {
            return Ok(());
        }
        let axis = self.frame.longitude();
        let half = ExactRational::from_f64(axis.half_period());
        let negative_half = ExactRational::from_f64(-axis.half_period());
        let point_capacity = self
            .points
            .len()
            .checked_mul(2)
            .ok_or(ResourceError::SizeOverflow)?;
        work.charge_into_error(
            u64::try_from(point_capacity).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut point_coordinates = Vec::with_capacity(point_capacity);
        for source in &self.points {
            work.charge_into_error(1)?;
            let point = source_chart_point(self.frame, source.point);
            if point.x.try_cmp(&half)?.is_eq() || point.x.try_cmp(&negative_half)?.is_eq() {
                work.charge_into_error(2)?;
                let y = point.y;
                point_coordinates.push(ExactChartPoint::from_rationals(
                    negative_half.clone(),
                    y.clone(),
                ));
                point_coordinates.push(ExactChartPoint::from_rationals(half.clone(), y));
            } else {
                point_coordinates.push(point);
            }
        }
        try_sort_by_with_work(&mut point_coordinates, work, |left, right, work| {
            work.charge_into_error(1)?;
            left.compare_lex(right)
        })?;
        work.charge_into_error(
            u64::try_from(point_coordinates.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut unique_points: Vec<ExactChartPoint> = Vec::with_capacity(point_coordinates.len());
        for point in point_coordinates {
            work.charge_into_error(1)?;
            if let Some(previous) = unique_points.last()
                && previous.same_position(&point)?
            {
                continue;
            }
            unique_points.push(point);
        }

        work.charge_into_error(
            u64::try_from(
                unique_points
                    .len()
                    .checked_mul(2)
                    .ok_or(ResourceError::SizeOverflow)?,
            )
            .map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut point_bounds = Vec::with_capacity(unique_points.len());
        let mut proxy_points = Vec::with_capacity(unique_points.len());
        let mut unproxyable_points = Vec::new();
        for point in unique_points {
            work.charge_into_error(1)?;
            let [min_x, min_y, max_x, max_y] = point.outward_bounds()?;
            if [min_x, min_y, max_x, max_y].into_iter().all(f64::is_finite) {
                point_bounds.push(Bounds::new_raw(min_x, min_y, max_x, max_y));
                proxy_points.push(point);
            } else {
                unproxyable_points.push(point);
            }
        }

        work.charge_into_error(
            u64::try_from(proxies.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut piece_bounds = Vec::with_capacity(proxies.len());
        for proxy in proxies {
            work.charge_into_error(1)?;
            piece_bounds.push(Bounds::new_raw(
                proxy.start.x,
                proxy.start.y,
                proxy.end.x,
                proxy.end.y,
            ));
        }
        if !piece_bounds.is_empty() && !point_bounds.is_empty() {
            let mut failure = None;
            let candidates = for_each_overlapping_bounds_cross_pair(
                &piece_bounds,
                &point_bounds,
                |piece_index, point_index| {
                    if let Err(error) = work.charge_into_error(1) {
                        failure = Some(error);
                        return std::ops::ControlFlow::Break(());
                    }
                    let Some(&piece_id) = proxy_piece_ids.get(piece_index) else {
                        failure = Some(
                            GeometryErrorKind::Invalid(
                                "Q point sweep has an unknown chart piece".into(),
                            )
                            .into(),
                        );
                        return std::ops::ControlFlow::Break(());
                    };
                    let Some(point) = proxy_points.get(point_index) else {
                        failure = Some(
                            GeometryErrorKind::Invalid(
                                "Q point sweep has an unknown source point".into(),
                            )
                            .into(),
                        );
                        return std::ops::ControlFlow::Break(());
                    };
                    match self.refine_piece_point(piece_id, point, cuts, compare_cuts, work) {
                        Ok(()) => std::ops::ControlFlow::Continue(()),
                        Err(error) => {
                            failure = Some(error);
                            std::ops::ControlFlow::Break(())
                        },
                    }
                },
            )?;
            if candidates.is_break() {
                return Err(failure.expect("a failed exact point contact stores its error"));
            }
        }
        for point in &unproxyable_points {
            for piece_index in 0..self.pieces.len() {
                work.charge_into_error(1)?;
                self.refine_piece_point(piece_index, point, cuts, compare_cuts, work)?;
            }
        }
        for (piece_index, &is_proxyable) in proxyable.iter().enumerate() {
            if !is_proxyable {
                for point in &proxy_points {
                    work.charge_into_error(1)?;
                    self.refine_piece_point(piece_index, point, cuts, compare_cuts, work)?;
                }
            }
        }
        Ok(())
    }

    fn refine_piece_point<W: Work>(
        &self,
        piece_index: usize,
        point: &ExactChartPoint,
        cuts: &mut FallibleAvl<CutEvent>,
        compare_cuts: &mut impl FnMut(&CutEvent, &CutEvent, &mut W) -> Result<Ordering>,
        work: &mut W,
    ) -> Result<()> {
        work.charge_into_error(1)?;
        let piece = self.pieces.get(piece_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q point contact has an unknown chart piece".into())
        })?;
        if point_on_exact_segment(point, &piece.start, &piece.end)? {
            work.charge_into_error(1)?;
            let parameter = parameter_on_exact_segment(point, &piece.start, &piece.end)?;
            insert_cut_event(cuts, piece, parameter, point.clone(), work, compare_cuts)?;
        }
        Ok(())
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "piece indices are generated from the admitted exact chart-piece slice"
    )]
    pub(in crate::geometry::topology::exact) fn refine_piece_pair<W: Work>(
        &self,
        pair: [usize; 2],
        right_shift: Option<&ExactRational>,
        cuts: &mut FallibleAvl<CutEvent>,
        constructed_nodes: &mut FallibleAvl<ExactQuotientKey>,
        compare_cuts: &mut impl FnMut(&CutEvent, &CutEvent, &mut W) -> Result<Ordering>,
        compare_keys: &mut impl FnMut(&ExactQuotientKey, &ExactQuotientKey, &mut W) -> Result<Ordering>,
        budget: &mut ExpansionBudget,
        work: &mut W,
    ) -> Result<()> {
        let [left, right] = pair;
        let a = &self.pieces[left];
        let b = &self.pieces[right];
        let shifted = right_shift.map(|shift| b.shifted_x(shift)).transpose()?;
        let right = shifted.as_ref().unwrap_or(b);
        work.charge_into_error(1)?;
        exact_piece_contacts(a, right, work, |point, left_t, right_t, work| {
            let point = match point {
                // Original and chart-split endpoints were seeded before pair refinement.
                ContactPoint::Endpoint(point) => point.clone(),
                ContactPoint::Constructed(point) => {
                    let key = quotient_key(self.frame, &point)?;
                    if constructed_nodes.insert_with_work(key, work, compare_keys)? {
                        budget.add(1)?;
                    }
                    point
                },
            };
            let right_point = match right_shift {
                Some(shift) => {
                    ExactChartPoint::from_rationals(point.x.try_subtract(shift)?, point.y.clone())
                },
                None => point.clone(),
            };
            insert_cut_event(cuts, a, left_t, point, work, compare_cuts)?;
            insert_cut_event(cuts, b, right_t, right_point, work, compare_cuts)?;
            Ok(())
        })
    }
}
