use super::{
    CoordSeq, DistanceParts, GeometryErrorKind, LineIndex, LineSegments as _, NearestCandidate,
    Point, Result, Segment, XY, coordinate_squared_safe, point_distance, point_segment_selection,
    segment_projection, squared_space_safe, update_nearest_candidate,
};
use crate::Coordinates as _;

/// Locate one query while measuring its route, without a reusable prefix index.
pub(crate) fn locate_planar_route<'a>(
    lines: impl Iterator<Item = &'a CoordSeq> + Clone,
    probe: Point,
    normalized: bool,
    scale: f64,
) -> Result<f64> {
    let source_lines = lines.clone();
    let mut has_vertex = false;
    let mut total = 0.0;
    let mut best_distance = f64::INFINITY;
    let mut search: Result<Option<NearestCandidate>> = Ok(None);
    let mut stationing = None;
    let mut base = 0_usize;
    for line in lines {
        has_vertex |= !line.is_empty();
        for (local, (xs, ys)) in line
            .xs()
            .array_windows::<2>()
            .zip(line.ys().array_windows::<2>())
            .enumerate()
        {
            let ordinal = base.wrapping_add(local);
            let [ax, bx] = *xs;
            let [ay, by] = *ys;
            let segment = Segment {
                start: XY::new(ax, ay),
                end: XY::new(bx, by),
            };
            let measured = point_distance(segment.start, segment.end);
            if !measured.is_finite() || (measured != 0.0 && !measured.is_normal() && scale > 1.0) {
                return locate_wide_route(source_lines, probe, normalized, scale);
            }
            let offset = total;
            if measured != 0.0 {
                total += measured;
            }
            if !total.is_finite() {
                return locate_wide_route(source_lines, probe, normalized, scale);
            }
            if measured == 0.0 || probe.x.is_nan() {
                continue;
            }
            let gap = (probe.x - segment.start.x.max(segment.end.x))
                .max(segment.start.x.min(segment.end.x) - probe.x)
                .max(probe.y - segment.start.y.max(segment.end.y))
                .max(segment.start.y.min(segment.end.y) - probe.y);
            if gap.is_normal() && gap > best_distance * 2.0 {
                continue;
            }
            if let Ok(best) = &mut search {
                let candidate =
                    point_segment_selection(probe.xy(), segment).and_then(|selection| {
                        let distance = selection.distance();
                        update_nearest_candidate!(&selection, probe, segment, || ordinal, best)
                            .map(|changed| (changed, distance))
                    });
                match candidate {
                    Ok((true, distance)) => {
                        best_distance = distance;
                        // Subtract accumulated prefixes, as on the prepared path:
                        // at a large station the stored segment length can round.
                        stationing = Some((offset, total - offset, segment));
                    },
                    Ok((false, _)) => {},
                    // Finish source admission before reporting a projection error.
                    Err(error) => search = Err(error),
                }
            }
        }
        base = base.wrapping_add(line.len().saturating_sub(1));
    }
    if !has_vertex {
        return Err(GeometryErrorKind::EmptyLinework.into());
    }
    if probe.x.is_nan() {
        return Ok(f64::NAN);
    }
    if total == 0.0 {
        return Ok(0.0);
    }
    search?;
    let Some((offset, length, segment)) = stationing else {
        return Ok(0.0);
    };
    let projection = segment_projection(probe, segment)?;
    let station = offset + projection.scale(length)?;
    Ok(if normalized {
        station / total
    } else {
        station * scale
    })
}

fn locate_wide_route<'a>(
    lines: impl Iterator<Item = &'a CoordSeq>,
    probe: Point,
    normalized: bool,
    scale: f64,
) -> Result<f64> {
    use super::OrdinateSegment;

    let mut segments = Vec::new();
    for line in lines {
        segments.extend(
            line.segment_pairs()
                .map(|[start, end]| OrdinateSegment { start, end }),
        );
    }
    if probe.x.is_nan() {
        return Ok(f64::NAN);
    }
    let prefix = super::wide::WidePrefix::build(&segments)?;
    let mut best: Option<NearestCandidate> = None;
    for (ordinal, source) in segments.iter().enumerate() {
        let segment = Segment {
            start: source.start.xy(),
            end: source.end.xy(),
        };
        if segment.start == segment.end {
            continue;
        }
        let selection = point_segment_selection(probe.xy(), segment)?;
        update_nearest_candidate!(&selection, probe, segment, || ordinal, &mut best)?;
    }
    let Some(candidate) = best else {
        return Ok(0.0);
    };
    let ordinal = candidate
        .segment_ordinal()
        .expect("locate selects a linework segment");
    let segment = *segments
        .get(ordinal)
        .expect("nearest ordinal belongs to the source route");
    prefix.locate(ordinal, segment, probe, normalized, scale)
}

impl LineIndex {
    /// The along-line offset of `probe`'s nearest projection (the engine
    /// behind `line_locate_point`). Selection shares proximity's exact key:
    /// source distance, projection provenance, then segment ordinal.
    pub(crate) fn locate_point_prepared<'a>(
        &self,
        probe: Point,
        normalized: bool,
        scale: f64,
        distance_parts: impl FnOnce() -> Result<&'a DistanceParts>,
    ) -> Result<f64> {
        self.locate_point_with_search(probe, normalized, scale, || {
            self.nearest_planar_prepared(probe, distance_parts()?)
        })
    }

    fn locate_point_with_search(
        &self,
        probe: Point,
        normalized: bool,
        scale: f64,
        search: impl FnOnce() -> Result<Option<NearestCandidate>>,
    ) -> Result<f64> {
        // POINT EMPTY reaches a locate as the NaN point of a point column.
        if probe.x.is_nan() {
            return Ok(f64::NAN);
        }
        if self.wide.is_none() && self.total == 0.0 {
            return Ok(0.0);
        }
        let Some(candidate) = search()? else {
            return Ok(0.0);
        };
        let ordinal = candidate
            .segment_ordinal()
            .expect("locate selects a linework segment");
        let segment = self.segments.segment(ordinal);
        if let Some(wide) = &self.wide {
            return wide.locate(ordinal, segment, probe, normalized, scale);
        }
        let offset = *self
            .prefix
            .get(ordinal)
            .expect("nearest ordinal has a station prefix");
        let next = *self
            .prefix
            .get(ordinal.wrapping_add(1))
            .expect("nearest ordinal has a trailing station prefix");
        let length = next - offset;
        let source_segment = Segment {
            start: segment.start.xy(),
            end: segment.end.xy(),
        };
        let projection = segment_projection(probe, source_segment)?;
        let station = offset + projection.scale(length)?;
        let result = if normalized {
            station / self.total
        } else {
            station
        };
        Ok(if normalized { result } else { result * scale })
    }

    fn nearest_planar_prepared(
        &self,
        probe: Point,
        parts: &DistanceParts,
    ) -> Result<Option<NearestCandidate>> {
        let squared = squared_space_safe(parts) && coordinate_squared_safe(probe);
        let keep = |_: Point, segment: Segment| {
            !self.has_zero_segments || point_distance(segment.start, segment.end) != 0.0
        };
        let mut best = None;
        match (squared, parts.bvh()) {
            (true, Some(bvh)) => {
                bvh.nearest_to_points::<true>(
                    &parts.linework,
                    std::iter::once(probe),
                    &keep,
                    &mut best,
                )?;
            },
            (false, Some(bvh)) => {
                bvh.nearest_to_points::<false>(
                    &parts.linework,
                    std::iter::once(probe),
                    &keep,
                    &mut best,
                )?;
            },
            (true, None) => {
                parts.linework.nearest_to_points::<true>(
                    std::iter::once(probe),
                    &keep,
                    &mut best,
                )?;
            },
            (false, None) => {
                parts.linework.nearest_to_points::<false>(
                    std::iter::once(probe),
                    &keep,
                    &mut best,
                )?;
            },
        }
        Ok(best)
    }
}
