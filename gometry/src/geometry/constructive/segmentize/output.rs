use super::plan::{Plan, Route};
use super::*;

/// How interior vertices are placed along a segment.
///
/// Z and M stay per-column under every placement — they are attribute ramps,
/// not positions.
#[derive(Clone, Copy)]
pub(crate) enum SegmentPlacement<'a> {
    /// Straight line in coordinate space (projected or CRS-free input).
    Planar,
    /// Along the straight coordinate-chart edge, at equal ellipsoidal
    /// arclength, for geographic coordinates in the typed native frame.
    Chart(&'a geographiclib_rs::Geodesic, GeographicFrame),
    /// Along the shortest ellipsoidal path between geographic endpoints.
    Geodesic(&'a geographiclib_rs::Geodesic, GeographicFrame),
    /// Along the constant-bearing route between geographic endpoints.
    Rhumb {
        crs: &'a str,
        state: &'a crate::crs::RuntimeState,
        frame: GeographicFrame,
    },
}

#[derive(Clone)]
pub(crate) enum SegmentPlacementOwned {
    Planar,
    Chart(geographiclib_rs::Geodesic, GeographicFrame),
    Geodesic(geographiclib_rs::Geodesic, GeographicFrame),
    Rhumb {
        #[expect(
            clippy::rc_buffer,
            reason = "reuse the operation metric's canonical CRS allocation without copying the label"
        )]
        crs: Arc<String>,
        state: Arc<crate::crs::RuntimeState>,
        frame: GeographicFrame,
    },
}

impl SegmentPlacementOwned {
    pub(crate) fn as_borrowed(&self) -> SegmentPlacement<'_> {
        match self {
            Self::Planar => SegmentPlacement::Planar,
            Self::Chart(geodesic, frame) => SegmentPlacement::Chart(geodesic, *frame),
            Self::Geodesic(geodesic, frame) => SegmentPlacement::Geodesic(geodesic, *frame),
            Self::Rhumb { crs, state, frame } => SegmentPlacement::Rhumb {
                crs,
                state,
                frame: *frame,
            },
        }
    }

    pub(crate) const fn frame(&self) -> Option<GeographicFrame> {
        match self {
            Self::Planar => None,
            Self::Chart(_, frame) | Self::Geodesic(_, frame) | Self::Rhumb { frame, .. } => {
                Some(*frame)
            },
        }
    }
}

pub(super) struct OutputColumns {
    pub(super) xs: Vec<f64>,
    pub(super) ys: Vec<f64>,
    pub(super) zs: Option<Vec<f64>>,
    pub(super) ms: Option<Vec<f64>>,
}

impl OutputColumns {
    fn with_capacity(points: &CoordSeq, total: usize) -> Result<Self> {
        ensure_coordseq_vertex_capacity(total)?;
        Ok(Self {
            xs: Vec::with_capacity(total),
            ys: Vec::with_capacity(total),
            zs: points.zs().map(|_| Vec::with_capacity(total)),
            ms: points.ms().map(|_| Vec::with_capacity(total)),
        })
    }

    fn extend_sources(&mut self, points: &CoordSeq, range: Range<usize>) {
        self.xs.extend_from_slice(
            points
                .xs()
                .get(range.clone())
                .expect("subdivision source run belongs to the X column"),
        );
        self.ys.extend_from_slice(
            points
                .ys()
                .get(range.clone())
                .expect("subdivision source run belongs to the Y column"),
        );
        if let (Some(output), Some(column)) = (&mut self.zs, points.zs()) {
            output.extend_from_slice(
                column
                    .get(range.clone())
                    .expect("subdivision source run belongs to the Z column"),
            );
        }
        if let (Some(output), Some(column)) = (&mut self.ms, points.ms()) {
            output.extend_from_slice(
                column
                    .get(range)
                    .expect("subdivision source run belongs to the M column"),
            );
        }
    }

    pub(super) fn push_ordinates(&mut self, points: &CoordSeq, segment: usize, pieces: usize) {
        if let (Some(output), Some(column)) = (&mut self.zs, points.zs()) {
            let start = *column
                .get(segment)
                .expect("subdivision Z segment start belongs to its column");
            let end = *column
                .get(segment.wrapping_add(1))
                .expect("subdivision Z segment end belongs to its column");
            push_interpolated(output, start, end, pieces);
        }
        if let (Some(output), Some(column)) = (&mut self.ms, points.ms()) {
            let start = *column
                .get(segment)
                .expect("subdivision M segment start belongs to its column");
            let end = *column
                .get(segment.wrapping_add(1))
                .expect("subdivision M segment end belongs to its column");
            push_interpolated(output, start, end, pieces);
        }
    }

    pub(super) fn push_chart_point(
        &mut self,
        points: &CoordSeq,
        segment: usize,
        point: Point,
        fraction: f64,
    ) -> Result<()> {
        let length = self
            .xs
            .len()
            .checked_add(1)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        ensure_coordseq_vertex_capacity(length)?;
        self.xs.push(point.x);
        self.ys.push(point.y);
        if let (Some(output), Some(column)) = (&mut self.zs, points.zs()) {
            let start = *column
                .get(segment)
                .expect("chart subdivision Z start belongs to its column");
            let end = *column
                .get(segment.wrapping_add(1))
                .expect("chart subdivision Z end belongs to its column");
            let value = interpolate_subdivision_ordinate(start, end, fraction);
            output.push(if value.is_finite() {
                value
            } else if fraction <= 0.5 {
                start
            } else {
                end
            });
        }
        if let (Some(output), Some(column)) = (&mut self.ms, points.ms()) {
            let start = *column
                .get(segment)
                .expect("chart subdivision M start belongs to its column");
            let end = *column
                .get(segment.wrapping_add(1))
                .expect("chart subdivision M end belongs to its column");
            let value = interpolate_subdivision_ordinate(start, end, fraction);
            output.push(if value.is_finite() {
                value
            } else if fraction <= 0.5 {
                start
            } else {
                end
            });
        }
        Ok(())
    }

    fn finish(self) -> Result<CoordSeq> {
        CoordSeq::try_from_vecs(self.xs, self.ys, self.zs, self.ms)
    }
}

pub(super) fn push_interpolated(output: &mut Vec<f64>, start: f64, end: f64, pieces: usize) {
    for step in 1..pieces {
        let value = interpolate_subdivision_ordinate(start, end, step as f64 / pieces as f64);
        output.push(if value.is_finite() {
            value
        } else if step <= pieces / 2 {
            start
        } else {
            end
        });
    }
}

pub(super) fn materialize<R: Route>(
    route: &mut R,
    points: &CoordSeq,
    plan: &Plan<R::Solution>,
    budget: &ExpansionBudget,
) -> Result<(CoordSeq, usize)> {
    let total = points
        .len()
        .checked_add(plan.generated)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    if plan.generated == 0 {
        return Ok((points.clone(), 0));
    }

    let mut output = OutputColumns::with_capacity(points, total)?;
    let mut copied = 0;
    let mut refined_generated = 0;
    let mut solutions = plan.solutions.iter();
    for (edge, (segment, &pieces)) in line_segments(points).zip(plan.steps.iter()).enumerate() {
        if pieces < 2 {
            continue;
        }
        output.extend_sources(points, copied..edge.wrapping_add(1));
        copied = edge.wrapping_add(1);
        let solution = *solutions
            .next()
            .expect("every split edge has one route solution");
        route.interior(
            segment,
            solution,
            pieces,
            points,
            edge,
            &mut output,
            budget,
            plan.generated,
            &mut refined_generated,
        )?;
    }
    output.extend_sources(points, copied..points.len());
    debug_assert_eq!(solutions.len(), 0);
    let generated = plan
        .generated
        .checked_add(refined_generated)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    let expected_total = points
        .len()
        .checked_add(generated)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    debug_assert_eq!(output.xs.len(), expected_total);
    debug_assert_eq!(output.ys.len(), expected_total);
    Ok((output.finish()?, generated))
}
